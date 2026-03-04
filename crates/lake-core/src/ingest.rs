use std::io;

use fastcdc::v2020::FastCDC;

use crate::store::ObjectStore;
use crate::types::{Chunk, Manifest};

// ─── Chunking Parameters ─────────────────────────────────────────

/// CDC chunking configuration.
/// Defaults: 8KB min, 16KB avg, 64KB max.
/// Future: per-file or per-type policies from the metadata layer.
#[derive(Debug, Clone, Copy)]
pub struct ChunkParams {
    pub min_size: u32,
    pub avg_size: u32,
    pub max_size: u32,
}

impl Default for ChunkParams {
    fn default() -> Self {
        ChunkParams {
            min_size: 8 * 1024,      // 8 KB
            avg_size: 16 * 1024,     // 16 KB
            max_size: 64 * 1024,     // 64 KB
        }
    }
}

// ─── Ingestion Engine ────────────────────────────────────────────

/// Takes raw file bytes, chunks them via FastCDC, stores chunks in the lake,
/// builds and stores a manifest. The bridge between "user wrote a file" and
/// "lake has content-addressed objects."
pub struct Ingester<'a> {
    store: &'a ObjectStore,
    params: ChunkParams,
}

impl<'a> Ingester<'a> {
    pub fn new(store: &'a ObjectStore) -> Self {
        Ingester {
            store,
            params: ChunkParams::default(),
        }
    }

    pub fn with_params(store: &'a ObjectStore, params: ChunkParams) -> Self {
        Ingester { store, params }
    }

    /// Ingest raw bytes into the lake.
    /// Chunks the data, stores each chunk, builds and stores the manifest.
    /// Returns the manifest describing the file's state.
    ///
    /// If every chunk already exists (identical content previously stored),
    /// this is essentially free — dedup at the storage layer.
    pub fn ingest(&self, data: &[u8]) -> io::Result<Manifest> {
        let chunks = self.chunk_and_store(data)?;
        let manifest = Manifest::from_chunks(&chunks);
        self.store.put_manifest(&manifest)?;
        Ok(manifest)
    }

    /// Run FastCDC over the data and store each chunk.
    fn chunk_and_store(&self, data: &[u8]) -> io::Result<Vec<Chunk>> {
        let chunker = FastCDC::new(
            data,
            self.params.min_size,
            self.params.avg_size,
            self.params.max_size,
        );

        let mut chunks = Vec::new();
        for entry in chunker {
            let chunk_data = &data[entry.offset..entry.offset + entry.length];
            let chunk = self.store.put_chunk(chunk_data)?;
            chunks.push(chunk);
        }

        Ok(chunks)
    }
}

// ─── Tests ───────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn ingest_small_file() {
        let dir = tempdir().unwrap();
        let store = ObjectStore::open(dir.path().join("lake")).unwrap();
        let ingester = Ingester::new(&store);

        let data = b"small file content";
        let manifest = ingester.ingest(data).unwrap();

        // Small file → single chunk
        assert_eq!(manifest.chunks.len(), 1);
        assert_eq!(manifest.total_size, data.len() as u64);

        // Round-trip: reassemble from lake
        let retrieved = store.read_file(&manifest.hash).unwrap();
        assert_eq!(retrieved, data);
    }

    #[test]
    fn ingest_large_file_produces_multiple_chunks() {
        let dir = tempdir().unwrap();
        let store = ObjectStore::open(dir.path().join("lake")).unwrap();
        let ingester = Ingester::new(&store);

        // 256KB of pseudo-random data to trigger multiple CDC boundaries
        let data: Vec<u8> = (0u64..256 * 1024)
            .map(|i| (i.wrapping_mul(31).wrapping_add(17)) as u8)
            .collect();

        let manifest = ingester.ingest(&data).unwrap();
        assert!(manifest.chunks.len() > 1);
        assert_eq!(manifest.total_size, data.len() as u64);

        let retrieved = store.read_file(&manifest.hash).unwrap();
        assert_eq!(retrieved, data);
    }

    #[test]
    fn ingest_identical_content_deduplicates() {
        let dir = tempdir().unwrap();
        let store = ObjectStore::open(dir.path().join("lake")).unwrap();
        let ingester = Ingester::new(&store);

        let data = b"deduplicate me please";
        let m1 = ingester.ingest(data).unwrap();
        let m2 = ingester.ingest(data).unwrap();

        // Same content → same manifest hash
        assert_eq!(m1.hash, m2.hash);
    }

    #[test]
    fn ingest_similar_content_shares_chunks() {
        let dir = tempdir().unwrap();
        let store = ObjectStore::open(dir.path().join("lake")).unwrap();
        let ingester = Ingester::new(&store);

        // Two large files that differ only in one region.
        // CDC should produce mostly identical chunks.
        let mut data1: Vec<u8> = (0u64..256 * 1024)
            .map(|i| (i.wrapping_mul(31).wrapping_add(17)) as u8)
            .collect();
        let mut data2 = data1.clone();

        // Modify a small region in the middle
        for i in 100_000..100_500 {
            data2[i] = 0xFF;
        }

        let m1 = ingester.ingest(&data1).unwrap();
        let m2 = ingester.ingest(&data2).unwrap();

        // Different manifests
        assert_ne!(m1.hash, m2.hash);

        // But most chunks should be shared
        let shared = m1
            .chunks
            .iter()
            .filter(|h| m2.chunks.contains(h))
            .count();

        assert!(
            shared > 0,
            "similar files should share at least some chunks"
        );
        assert!(
            shared as f64 / m1.chunks.len() as f64 > 0.5,
            "majority of chunks should be shared for a small edit"
        );
    }
}
