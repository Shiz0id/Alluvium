use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use crate::types::{B3Hash, Chunk, Manifest};

// ─── Object Type Prefix ──────────────────────────────────────────

/// Single-byte prefix stored on disk to distinguish object types.
/// Not included in hash computation — purely a storage concern.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ObjectType {
    Chunk = 0x01,
    Manifest = 0x02,
}

impl ObjectType {
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            0x01 => Some(ObjectType::Chunk),
            0x02 => Some(ObjectType::Manifest),
            _ => None,
        }
    }
}

/// What you get back when you read an object from the lake.
#[derive(Debug)]
pub enum Object {
    Chunk(Vec<u8>),
    Manifest(Manifest),
}

// ─── Object Store ────────────────────────────────────────────────

/// The lake's content-addressed object store.
/// Stores raw bytes keyed by BLAKE3 hash in Git-style sharded directories.
///
/// On-disk layout:
///   {root}/
///     7f/
///       2a91c3e4... → [0x01][raw chunk bytes]
///     a8/
///       3bf20d11... → [0x02][manifest JSON]
pub struct ObjectStore {
    root: PathBuf,
}

impl ObjectStore {
    /// Open or create an object store at the given root path.
    pub fn open(root: impl Into<PathBuf>) -> io::Result<Self> {
        let root = root.into();
        fs::create_dir_all(&root)?;
        Ok(ObjectStore { root })
    }

    /// Shard path: first 2 hex chars as directory, rest as filename.
    fn object_path(&self, hash: &B3Hash) -> PathBuf {
        let hex = hash.to_hex();
        self.root.join(&hex[..2]).join(&hex[2..])
    }

    /// Check if an object exists in the store.
    pub fn exists(&self, hash: &B3Hash) -> bool {
        self.object_path(hash).exists()
    }

    // ─── Write Operations ────────────────────────────────────────

    /// Store raw bytes as a chunk. Returns the chunk descriptor.
    /// If the hash already exists, this is a no-op (dedup).
    pub fn put_chunk(&self, data: &[u8]) -> io::Result<Chunk> {
        let hash = B3Hash::from_bytes(data);

        if !self.exists(&hash) {
            let path = self.object_path(&hash);
            fs::create_dir_all(path.parent().unwrap())?;

            let mut stored = Vec::with_capacity(1 + data.len());
            stored.push(ObjectType::Chunk as u8);
            stored.extend_from_slice(data);
            fs::write(&path, &stored)?;
        }

        Ok(Chunk {
            hash,
            size: data.len() as u64,
        })
    }

    /// Store a manifest. Serializes to JSON, hashes it, writes with prefix.
    /// The manifest's hash is computed from its chunk list, not the JSON —
    /// so we use the hash already computed in Manifest::from_chunks.
    pub fn put_manifest(&self, manifest: &Manifest) -> io::Result<()> {
        if !self.exists(&manifest.hash) {
            let path = self.object_path(&manifest.hash);
            fs::create_dir_all(path.parent().unwrap())?;

            let json = serde_json::to_vec(manifest).map_err(|e| {
                io::Error::new(io::ErrorKind::InvalidData, e)
            })?;

            let mut stored = Vec::with_capacity(1 + json.len());
            stored.push(ObjectType::Manifest as u8);
            stored.extend_from_slice(&json);
            fs::write(&path, &stored)?;
        }
        Ok(())
    }

    // ─── Read Operations ─────────────────────────────────────────

    /// Read an object by hash. Returns the typed object with verified integrity.
    pub fn get(&self, hash: &B3Hash) -> io::Result<Object> {
        let path = self.object_path(hash);
        let stored = fs::read(&path)?;

        if stored.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "empty object file",
            ));
        }

        let type_byte = stored[0];
        let data = &stored[1..];

        let obj_type = ObjectType::from_byte(type_byte).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown object type prefix: 0x{:02x}", type_byte),
            )
        })?;

        match obj_type {
            ObjectType::Chunk => {
                // Verify integrity: hash the content, compare to expected hash
                let actual = B3Hash::from_bytes(data);
                if actual != *hash {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "integrity check failed: expected {}, got {}",
                            hash, actual
                        ),
                    ));
                }
                Ok(Object::Chunk(data.to_vec()))
            }
            ObjectType::Manifest => {
                let manifest: Manifest =
                    serde_json::from_slice(data).map_err(|e| {
                        io::Error::new(io::ErrorKind::InvalidData, e)
                    })?;
                // Verify the stored hash matches
                if manifest.hash != *hash {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "manifest hash mismatch: expected {}, got {}",
                            hash, manifest.hash
                        ),
                    ));
                }
                Ok(Object::Manifest(manifest))
            }
        }
    }

    /// Convenience: get raw chunk bytes by hash.
    /// Returns error if the hash points to a manifest.
    pub fn get_chunk(&self, hash: &B3Hash) -> io::Result<Vec<u8>> {
        match self.get(hash)? {
            Object::Chunk(data) => Ok(data),
            Object::Manifest(_) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "expected chunk, found manifest",
            )),
        }
    }

    /// Convenience: get a manifest by hash.
    /// Returns error if the hash points to a chunk.
    pub fn get_manifest(&self, hash: &B3Hash) -> io::Result<Manifest> {
        match self.get(hash)? {
            Object::Manifest(m) => Ok(m),
            Object::Chunk(_) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "expected manifest, found chunk",
            )),
        }
    }

    /// Reassemble a complete file from a manifest hash.
    /// Reads the manifest, then reads and concatenates all chunks in order.
    pub fn read_file(&self, manifest_hash: &B3Hash) -> io::Result<Vec<u8>> {
        let manifest = self.get_manifest(manifest_hash)?;
        let mut buf = Vec::with_capacity(manifest.total_size as usize);
        for chunk_ref in &manifest.chunks {
            let chunk_data = self.get_chunk(&chunk_ref.hash)?;
            buf.extend_from_slice(&chunk_data);
        }
        Ok(buf)
    }
}

// ─── Tests ───────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn roundtrip_chunk() {
        let dir = tempdir().unwrap();
        let store = ObjectStore::open(dir.path().join("lake")).unwrap();

        let data = b"hello deeplake";
        let chunk = store.put_chunk(data).unwrap();

        assert!(store.exists(&chunk.hash));
        let retrieved = store.get_chunk(&chunk.hash).unwrap();
        assert_eq!(retrieved, data);
    }

    #[test]
    fn dedup_identical_chunks() {
        let dir = tempdir().unwrap();
        let store = ObjectStore::open(dir.path().join("lake")).unwrap();

        let data = b"duplicate content";
        let c1 = store.put_chunk(data).unwrap();
        let c2 = store.put_chunk(data).unwrap();

        assert_eq!(c1.hash, c2.hash);
        // Only one file on disk
        let path = store.object_path(&c1.hash);
        assert!(path.exists());
    }

    #[test]
    fn roundtrip_manifest() {
        let dir = tempdir().unwrap();
        let store = ObjectStore::open(dir.path().join("lake")).unwrap();

        let c1 = store.put_chunk(b"chunk one").unwrap();
        let c2 = store.put_chunk(b"chunk two").unwrap();
        let c3 = store.put_chunk(b"chunk three").unwrap();

        let manifest = Manifest::from_chunks(&[c1, c2, c3]);
        store.put_manifest(&manifest).unwrap();

        let retrieved = store.get_manifest(&manifest.hash).unwrap();
        assert_eq!(retrieved.chunks.len(), 3);
        assert_eq!(retrieved.total_size, manifest.total_size);
    }

    #[test]
    fn read_file_from_manifest() {
        let dir = tempdir().unwrap();
        let store = ObjectStore::open(dir.path().join("lake")).unwrap();

        let c1 = store.put_chunk(b"hello ").unwrap();
        let c2 = store.put_chunk(b"deep").unwrap();
        let c3 = store.put_chunk(b"lake").unwrap();

        let manifest = Manifest::from_chunks(&[c1, c2, c3]);
        store.put_manifest(&manifest).unwrap();

        let file = store.read_file(&manifest.hash).unwrap();
        assert_eq!(file, b"hello deeplake");
    }

    #[test]
    fn integrity_check_catches_corruption() {
        let dir = tempdir().unwrap();
        let store = ObjectStore::open(dir.path().join("lake")).unwrap();

        let chunk = store.put_chunk(b"original").unwrap();

        // Corrupt the file on disk
        let path = store.object_path(&chunk.hash);
        let mut stored = fs::read(&path).unwrap();
        // Flip a byte in the content (after the type prefix)
        stored[5] ^= 0xFF;
        fs::write(&path, &stored).unwrap();

        // Read should fail integrity check
        let result = store.get_chunk(&chunk.hash);
        assert!(result.is_err());
    }
}
