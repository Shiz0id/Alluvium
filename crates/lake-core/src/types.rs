use std::fmt;
use std::path::PathBuf;
use serde::{Serialize, Deserialize};

// ─── Hash Identity ───────────────────────────────────────────────

/// A BLAKE3 hash. The universal identity for everything in the lake.
/// Chunks, manifests, and any future object types are all addressed by this.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct B3Hash(pub [u8; 32]);

impl B3Hash {
    pub fn from_bytes(data: &[u8]) -> Self {
        let hash = blake3::hash(data);
        B3Hash(*hash.as_bytes())
    }

    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    pub fn from_hex(s: &str) -> Result<Self, hex::FromHexError> {
        let mut bytes = [0u8; 32];
        hex::decode_to_slice(s, &mut bytes)?;
        Ok(B3Hash(bytes))
    }
}

impl fmt::Debug for B3Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "b3:{}", &self.to_hex()[..12])
    }
}

impl fmt::Display for B3Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "b3:{}", self.to_hex())
    }
}

// ─── Chunk ───────────────────────────────────────────────────────

/// A chunk is a raw byte range produced by FastCDC.
/// It is the smallest unit in the lake. Never interpreted, just bytes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Chunk {
    pub hash: B3Hash,
    pub size: u64,
}

// ─── Manifest ────────────────────────────────────────────────────

/// A single entry in a manifest: a chunk's hash and its size in bytes.
/// Keeping size alongside hash makes the manifest self-contained for
/// offset calculation, diffing, and reassembly without hitting the store.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkRef {
    pub hash: B3Hash,
    pub size: u64,
}

/// A manifest is an ordered list of chunk references that reconstitutes a file.
/// The manifest's own identity is the BLAKE3 hash of its serialized content.
/// This is what "a file" means in the lake.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub hash: B3Hash,               // hash of this manifest's chunk list
    pub chunks: Vec<ChunkRef>,      // ordered chunk refs
    pub total_size: u64,            // sum of chunk sizes
}

impl Manifest {
    /// Build a manifest from an ordered list of chunks.
    /// Computes the manifest's own hash from the chunk list.
    pub fn from_chunks(chunks: &[Chunk]) -> Self {
        let chunk_refs: Vec<ChunkRef> = chunks
            .iter()
            .map(|c| ChunkRef { hash: c.hash, size: c.size })
            .collect();
        let total_size: u64 = chunks.iter().map(|c| c.size).sum();

        // Manifest hash = BLAKE3 of the concatenated chunk hashes
        let mut hasher = blake3::Hasher::new();
        for c in &chunk_refs {
            hasher.update(&c.hash.0);
        }
        let hash = B3Hash(*hasher.finalize().as_bytes());

        Manifest {
            hash,
            chunks: chunk_refs,
            total_size,
        }
    }
}

// ─── Version ─────────────────────────────────────────────────────

/// Why this version exists.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum VersionTrigger {
    /// File was closed after writing
    Close,
    /// User explicitly committed via lakectl
    Explicit,
    /// Periodic micro-hash snapshot (future)
    MicroHash,
}

/// A single version in a file's history.
/// Points at the manifest that represents the file's state at this point.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Version {
    pub manifest_hash: B3Hash,
    pub version_num: u64,           // sequential per-file counter (v1, v2, ...)
    pub timestamp: u64,             // unix epoch millis
    pub trigger: VersionTrigger,
    pub message: Option<String>,    // optional commit message
}

// ─── Path Entry ──────────────────────────────────────────────────

/// The mapping from a POSIX path to its lake identity.
/// This is the only mutable concept in the system.
/// Everything else is append-only.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PathEntry {
    pub path: PathBuf,
    pub head: B3Hash,               // current manifest hash
    pub history: Vec<Version>,      // newest first
    pub created_at: u64,
    pub modified_at: u64,
}
