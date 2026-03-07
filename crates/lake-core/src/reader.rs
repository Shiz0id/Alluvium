use std::collections::HashMap;
use std::io;
use std::sync::Arc;

use crate::store::ObjectStore;
use crate::types::{B3Hash, ChunkRef, Manifest};

// ─── Chunk Index ─────────────────────────────────────────────────

/// Precomputed byte-offset-to-chunk mapping for a manifest.
/// Enables O(log n) lookup of which chunk contains a given byte offset
/// without reading any chunk data.
#[derive(Debug, Clone)]
pub struct ChunkIndex {
    /// Each entry: (start_offset, chunk_ref)
    /// Sorted by start_offset for binary search.
    entries: Vec<(u64, ChunkRef)>,
    pub total_size: u64,
}

/// A resolved range: which chunks to read for a given (offset, size) request.
#[derive(Debug)]
pub struct ResolvedRead {
    pub segments: Vec<ReadSegment>,
}

/// A single segment of a resolved read — one chunk's contribution.
#[derive(Debug)]
pub struct ReadSegment {
    pub chunk_hash: B3Hash,
    pub chunk_offset: usize,   // offset within the chunk to start reading
    pub length: usize,         // how many bytes to take from this chunk
}

impl ChunkIndex {
    /// Build an index from a manifest.
    pub fn from_manifest(manifest: &Manifest) -> Self {
        let mut entries = Vec::with_capacity(manifest.chunks.len());
        let mut offset = 0u64;
        for chunk_ref in &manifest.chunks {
            entries.push((offset, *chunk_ref));
            offset += chunk_ref.size;
        }
        ChunkIndex {
            entries,
            total_size: offset,
        }
    }

    /// Resolve a read(offset, size) into the specific chunks and byte
    /// ranges needed. Returns None if offset is past end of file.
    pub fn resolve(&self, offset: u64, size: u32) -> Option<ResolvedRead> {
        if offset >= self.total_size || self.entries.is_empty() {
            return None;
        }

        let size = size as u64;
        let end = (offset + size).min(self.total_size);
        let mut remaining_offset = offset;
        let mut remaining = (end - offset) as usize;
        let mut segments = Vec::new();

        // Binary search for the first chunk containing our offset
        let start_idx = match self.entries.binary_search_by_key(&offset, |(o, _)| *o) {
            Ok(i) => i,
            Err(i) => i.saturating_sub(1),
        };

        for i in start_idx..self.entries.len() {
            if remaining == 0 {
                break;
            }

            let (chunk_start, chunk_ref) = &self.entries[i];
            let chunk_end = chunk_start + chunk_ref.size;

            // Skip if this chunk ends before our read starts
            if chunk_end <= remaining_offset {
                continue;
            }

            let offset_in_chunk = if remaining_offset > *chunk_start {
                (remaining_offset - chunk_start) as usize
            } else {
                0
            };

            let available = chunk_ref.size as usize - offset_in_chunk;
            let take = remaining.min(available);

            segments.push(ReadSegment {
                chunk_hash: chunk_ref.hash,
                chunk_offset: offset_in_chunk,
                length: take,
            });

            remaining_offset = chunk_end;
            remaining -= take;
        }

        if segments.is_empty() {
            None
        } else {
            Some(ResolvedRead { segments })
        }
    }
}

// ─── LRU Chunk Cache ─────────────────────────────────────────────

/// Simple LRU cache for chunk data. Keeps recently accessed chunks
/// in memory so sequential reads don't hit disk for every call.
pub struct ChunkCache {
    /// hash → chunk bytes
    entries: HashMap<B3Hash, Vec<u8>>,
    /// Access order: most recent at the back
    order: Vec<B3Hash>,
    capacity: usize,
}

impl ChunkCache {
    pub fn new(capacity: usize) -> Self {
        ChunkCache {
            entries: HashMap::with_capacity(capacity),
            order: Vec::with_capacity(capacity),
            capacity,
        }
    }

    /// Get a chunk from cache if present. Updates access order.
    pub fn get(&mut self, hash: &B3Hash) -> Option<&[u8]> {
        if self.entries.contains_key(hash) {
            // Move to back (most recent)
            self.order.retain(|h| h != hash);
            self.order.push(*hash);
            self.entries.get(hash).map(|v| v.as_slice())
        } else {
            None
        }
    }

    /// Insert a chunk into the cache. Evicts oldest if at capacity.
    pub fn put(&mut self, hash: B3Hash, data: Vec<u8>) {
        if self.entries.contains_key(&hash) {
            self.order.retain(|h| *h != hash);
            self.order.push(hash);
            return;
        }

        while self.entries.len() >= self.capacity {
            if let Some(oldest) = self.order.first().copied() {
                self.order.remove(0);
                self.entries.remove(&oldest);
            } else {
                break;
            }
        }

        self.order.push(hash);
        self.entries.insert(hash, data);
    }

    pub fn clear(&mut self) {
        self.entries.clear();
        self.order.clear();
    }
}

// ─── File Reader ─────────────────────────────────────────────────

/// Efficient file reader that resolves byte ranges to specific chunks
/// and caches recently accessed chunks. This is what the FUSE and
/// WinFSP frontends should use instead of ObjectStore::read_file().
pub struct FileReader {
    store: Arc<ObjectStore>,
    cache: ChunkCache,
}

impl FileReader {
    pub fn new(store: Arc<ObjectStore>, cache_capacity: usize) -> Self {
        FileReader {
            store,
            cache: ChunkCache::new(cache_capacity),
        }
    }

    /// Build a chunk index for a manifest. Cheap — just walks the
    /// chunk ref list and computes cumulative offsets.
    pub fn index(&self, manifest: &Manifest) -> ChunkIndex {
        ChunkIndex::from_manifest(manifest)
    }

    /// Read a byte range from a file using its chunk index.
    /// Only fetches the chunks that overlap the requested range.
    /// Serves from cache when possible.
    pub fn read(
        &mut self,
        index: &ChunkIndex,
        offset: u64,
        size: u32,
    ) -> io::Result<Vec<u8>> {
        let resolved = match index.resolve(offset, size) {
            Some(r) => r,
            None => return Ok(Vec::new()),
        };

        let total: usize = resolved.segments.iter().map(|s| s.length).sum();
        let mut buf = Vec::with_capacity(total);

        for segment in &resolved.segments {
            let chunk_data = self.fetch_chunk(&segment.chunk_hash)?;
            let end = segment.chunk_offset + segment.length;
            buf.extend_from_slice(&chunk_data[segment.chunk_offset..end]);
        }

        Ok(buf)
    }

    /// Read an entire file using the chunk index. Same as read_file
    /// but uses the cache.
    pub fn read_all(&mut self, index: &ChunkIndex) -> io::Result<Vec<u8>> {
        if index.total_size == 0 {
            return Ok(Vec::new());
        }
        // Cap at u32::MAX per read, but for files under 4GB this is one call
        self.read(index, 0, index.total_size.min(u32::MAX as u64) as u32)
    }

    /// Fetch a chunk from cache or disk.
    fn fetch_chunk(&mut self, hash: &B3Hash) -> io::Result<Vec<u8>> {
        // Check cache first
        if let Some(data) = self.cache.get(hash) {
            return Ok(data.to_vec());
        }

        // Cache miss — read from store
        let data = self.store.get_chunk(hash)?;
        self.cache.put(*hash, data.clone());
        Ok(data)
    }

    /// Clear the chunk cache.
    pub fn clear_cache(&mut self) {
        self.cache.clear();
    }
}

// ─── Tests ───────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::Ingester;
    use tempfile::tempdir;

    fn make_test_store() -> (tempfile::TempDir, Arc<ObjectStore>) {
        let dir = tempdir().unwrap();
        let store = Arc::new(ObjectStore::open(dir.path().join("lake")).unwrap());
        (dir, store)
    }

    #[test]
    fn chunk_index_resolves_offsets() {
        let (_dir, store) = make_test_store();
        let ingester = Ingester::new(&*store);

        // Large enough for multiple chunks
        let data: Vec<u8> = (0u64..256 * 1024)
            .map(|i| (i.wrapping_mul(31).wrapping_add(17)) as u8)
            .collect();

        let manifest = ingester.ingest(&data).unwrap();
        let index = ChunkIndex::from_manifest(&manifest);

        assert!(manifest.chunks.len() > 1);
        assert_eq!(index.total_size, data.len() as u64);

        // Read from the beginning
        let resolved = index.resolve(0, 100).unwrap();
        assert_eq!(resolved.segments.len(), 1);
        assert_eq!(resolved.segments[0].chunk_offset, 0);
        assert_eq!(resolved.segments[0].length, 100);
    }

    #[test]
    fn chunk_index_cross_boundary_read() {
        let (_dir, store) = make_test_store();
        let ingester = Ingester::new(&*store);

        let data: Vec<u8> = (0u64..256 * 1024)
            .map(|i| (i.wrapping_mul(31).wrapping_add(17)) as u8)
            .collect();

        let manifest = ingester.ingest(&data).unwrap();
        let index = ChunkIndex::from_manifest(&manifest);

        // Find the boundary between chunk 0 and chunk 1
        let first_chunk_size = manifest.chunks[0].size;
        let boundary = first_chunk_size - 50;

        // Read 100 bytes spanning the boundary
        let resolved = index.resolve(boundary, 100).unwrap();
        assert_eq!(resolved.segments.len(), 2);
        assert_eq!(resolved.segments[0].length + resolved.segments[1].length, 100);
    }

    #[test]
    fn file_reader_matches_full_read() {
        let (_dir, store) = make_test_store();
        let ingester = Ingester::new(&*store);

        let data: Vec<u8> = (0u64..256 * 1024)
            .map(|i| (i.wrapping_mul(31).wrapping_add(17)) as u8)
            .collect();

        let manifest = ingester.ingest(&data).unwrap();

        // Full read via store
        let full = store.read_file(&manifest.hash).unwrap();

        // Chunked reads via reader
        let index = ChunkIndex::from_manifest(&manifest);
        let mut reader = FileReader::new(store.clone(), 32);

        // Read in 4KB blocks like a real app would
        let mut assembled = Vec::new();
        let mut offset = 0u64;
        while offset < index.total_size {
            let chunk = reader.read(&index, offset, 4096).unwrap();
            if chunk.is_empty() {
                break;
            }
            assembled.extend_from_slice(&chunk);
            offset += chunk.len() as u64;
        }

        assert_eq!(assembled, full);
        assert_eq!(assembled, data);
    }

    #[test]
    fn file_reader_past_eof() {
        let (_dir, store) = make_test_store();
        let ingester = Ingester::new(&*store);

        let manifest = ingester.ingest(b"small file").unwrap();
        let index = ChunkIndex::from_manifest(&manifest);
        let mut reader = FileReader::new(store, 32);

        let result = reader.read(&index, 99999, 100).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn cache_serves_repeated_reads() {
        let (_dir, store) = make_test_store();
        let ingester = Ingester::new(&*store);

        let data: Vec<u8> = (0u64..256 * 1024)
            .map(|i| (i.wrapping_mul(31).wrapping_add(17)) as u8)
            .collect();

        let manifest = ingester.ingest(&data).unwrap();
        let index = ChunkIndex::from_manifest(&manifest);
        let mut reader = FileReader::new(store, 32);

        // Read the same range twice — second should hit cache
        let r1 = reader.read(&index, 0, 4096).unwrap();
        let r2 = reader.read(&index, 0, 4096).unwrap();
        assert_eq!(r1, r2);
    }
}
