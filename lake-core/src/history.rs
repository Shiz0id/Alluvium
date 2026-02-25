use std::collections::HashSet;
use std::io;
use std::path::Path;

use crate::diff::{self, DiffResult};
use crate::path_table::{PathTable, PathTableError};
use crate::store::ObjectStore;
use crate::types::{B3Hash, Manifest, Version, VersionTrigger};

// ─── Error Type ──────────────────────────────────────────────────

#[derive(Debug)]
pub enum HistoryError {
    Store(io::Error),
    PathTable(PathTableError),
}

impl From<io::Error> for HistoryError {
    fn from(e: io::Error) -> Self {
        HistoryError::Store(e)
    }
}

impl From<PathTableError> for HistoryError {
    fn from(e: PathTableError) -> Self {
        HistoryError::PathTable(e)
    }
}

impl std::fmt::Display for HistoryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HistoryError::Store(e) => write!(f, "store error: {}", e),
            HistoryError::PathTable(e) => write!(f, "path table error: {}", e),
        }
    }
}

impl std::error::Error for HistoryError {}

type Result<T> = std::result::Result<T, HistoryError>;

// ─── Storage Stats ───────────────────────────────────────────────

/// Storage statistics for a file's entire version history.
#[derive(Debug, Clone)]
pub struct StorageStats {
    pub version_count: usize,
    /// Total bytes if every version were stored independently (naive cost).
    pub naive_total_bytes: u64,
    /// Actual unique bytes stored in the lake (deduped across all versions).
    pub actual_bytes: u64,
    /// Total unique chunks across all versions.
    pub unique_chunks: usize,
    /// Total chunk references across all manifests (includes duplicates).
    pub total_chunk_refs: usize,
}

impl StorageStats {
    /// Space savings ratio: 0.0 = no savings, 1.0 = 100% dedup.
    pub fn dedup_ratio(&self) -> f64 {
        if self.naive_total_bytes == 0 {
            return 0.0;
        }
        1.0 - (self.actual_bytes as f64 / self.naive_total_bytes as f64)
    }
}

// ─── History Engine ──────────────────────────────────────────────

/// Coordinates between the path table, object store, and diff engine
/// to provide unified version history operations.
pub struct History<'a> {
    store: &'a ObjectStore,
    paths: &'a PathTable,
}

impl<'a> History<'a> {
    pub fn new(store: &'a ObjectStore, paths: &'a PathTable) -> Self {
        History { store, paths }
    }

    /// Get the full version list for a file, newest first.
    pub fn list(&self, path: impl AsRef<Path>) -> Result<Vec<Version>> {
        Ok(self.paths.get_history(&path)?)
    }

    /// Diff two specific versions of a file by version number.
    /// diff_versions("report.txt", 2, 5) → what changed from v2 to v5.
    pub fn diff_versions(
        &self,
        path: impl AsRef<Path>,
        from_version: u64,
        to_version: u64,
    ) -> Result<DiffResult> {
        let hash_a = self.paths.get_version(&path, from_version)?;
        let hash_b = self.paths.get_version(&path, to_version)?;
        let manifest_a = self.store.get_manifest(&hash_a)?;
        let manifest_b = self.store.get_manifest(&hash_b)?;
        Ok(diff::diff(&manifest_a, &manifest_b))
    }

    /// Diff a specific version against HEAD.
    pub fn diff_against_head(
        &self,
        path: impl AsRef<Path>,
        from_version: u64,
    ) -> Result<DiffResult> {
        let hash_a = self.paths.get_version(&path, from_version)?;
        let hash_b = self.paths.get_head(&path)?;
        let manifest_a = self.store.get_manifest(&hash_a)?;
        let manifest_b = self.store.get_manifest(&hash_b)?;
        Ok(diff::diff(&manifest_a, &manifest_b))
    }

    /// Diff two adjacent versions: vN vs vN+1.
    pub fn diff_adjacent(
        &self,
        path: impl AsRef<Path>,
        version: u64,
    ) -> Result<DiffResult> {
        self.diff_versions(&path, version, version + 1)
    }

    /// Read the full file content at a specific version number.
    pub fn read_at_version(
        &self,
        path: impl AsRef<Path>,
        version_num: u64,
    ) -> Result<Vec<u8>> {
        let hash = self.paths.get_version(&path, version_num)?;
        Ok(self.store.read_file(&hash)?)
    }

    /// Read the full file content at HEAD.
    pub fn read_head(&self, path: impl AsRef<Path>) -> Result<Vec<u8>> {
        let hash = self.paths.get_head(&path)?;
        Ok(self.store.read_file(&hash)?)
    }

    /// Rewind a file to a specific version number.
    /// This doesn't destroy history — it creates a new version entry
    /// whose manifest points at the old state.
    pub fn rewind(
        &self,
        path: impl AsRef<Path>,
        target_version: u64,
    ) -> Result<()> {
        let hash = self.paths.get_version(&path, target_version)?;
        self.paths.rewind(&path, &hash)?;
        Ok(())
    }

    /// Compute storage statistics for a file's entire version history.
    /// Walks all manifests and counts unique vs total chunks.
    pub fn storage_stats(&self, path: impl AsRef<Path>) -> Result<StorageStats> {
        let history = self.paths.get_history(&path)?;
        let mut unique_chunks: HashSet<B3Hash> = HashSet::new();
        let mut unique_bytes: u64 = 0;
        let mut naive_total: u64 = 0;
        let mut total_refs: usize = 0;

        for version in &history {
            let manifest = self.store.get_manifest(&version.manifest_hash)?;
            naive_total += manifest.total_size;
            total_refs += manifest.chunks.len();

            for chunk_ref in &manifest.chunks {
                if unique_chunks.insert(chunk_ref.hash) {
                    unique_bytes += chunk_ref.size;
                }
            }
        }

        Ok(StorageStats {
            version_count: history.len(),
            naive_total_bytes: naive_total,
            actual_bytes: unique_bytes,
            unique_chunks: unique_chunks.len(),
            total_chunk_refs: total_refs,
        })
    }
}

// ─── Tests ───────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::Ingester;
    use tempfile::tempdir;

    /// Helper: set up a store, path table, ingester, and history engine.
    struct TestHarness {
        store: ObjectStore,
        paths: PathTable,
    }

    impl TestHarness {
        fn new() -> Self {
            let dir = tempdir().unwrap();
            let store = ObjectStore::open(dir.into_path().join("lake")).unwrap();
            let paths = PathTable::in_memory().unwrap();
            TestHarness { store, paths }
        }

        fn ingester(&self) -> Ingester {
            Ingester::new(&self.store)
        }

        fn history(&self) -> History {
            History::new(&self.store, &self.paths)
        }

        /// Ingest data, commit as a new version of the given path.
        fn write_version(&self, path: &str, data: &[u8]) -> B3Hash {
            let manifest = self.ingester().ingest(data).unwrap();
            self.paths
                .put(path, &manifest.hash, VersionTrigger::Close, None)
                .unwrap();
            manifest.hash
        }
    }

    #[test]
    fn full_lifecycle() {
        let h = TestHarness::new();

        h.write_version("/doc.txt", b"version one");
        h.write_version("/doc.txt", b"version two");
        h.write_version("/doc.txt", b"version three");

        let history = h.history();
        let versions = history.list("/doc.txt").unwrap();
        assert_eq!(versions.len(), 3);
        assert_eq!(versions[0].version_num, 3);
        assert_eq!(versions[2].version_num, 1);
    }

    #[test]
    fn read_at_version() {
        let h = TestHarness::new();

        h.write_version("/doc.txt", b"first");
        h.write_version("/doc.txt", b"second");
        h.write_version("/doc.txt", b"third");

        let history = h.history();
        assert_eq!(history.read_at_version("/doc.txt", 1).unwrap(), b"first");
        assert_eq!(history.read_at_version("/doc.txt", 2).unwrap(), b"second");
        assert_eq!(history.read_head("/doc.txt").unwrap(), b"third");
    }

    #[test]
    fn diff_between_versions() {
        let h = TestHarness::new();

        h.write_version("/doc.txt", b"hello world");
        h.write_version("/doc.txt", b"hello deeplake");

        let history = h.history();
        let result = history.diff_versions("/doc.txt", 1, 2).unwrap();

        // Small files → single chunk each → complete replacement
        assert_eq!(result.summary.chunks_deleted, 1);
        assert_eq!(result.summary.chunks_inserted, 1);
    }

    #[test]
    fn rewind_and_verify() {
        let h = TestHarness::new();

        h.write_version("/doc.txt", b"original");
        h.write_version("/doc.txt", b"modified");
        h.write_version("/doc.txt", b"broken");

        let history = h.history();
        history.rewind("/doc.txt", 1).unwrap();

        assert_eq!(history.read_head("/doc.txt").unwrap(), b"original");

        // History now has 4 entries: 3 original + 1 rewind
        let versions = history.list("/doc.txt").unwrap();
        assert_eq!(versions.len(), 4);
        assert_eq!(versions[0].version_num, 4);
        assert_eq!(versions[0].message.as_deref(), Some("rewind"));
    }

    #[test]
    fn storage_stats_show_dedup() {
        let h = TestHarness::new();

        // Write the same content multiple times
        h.write_version("/doc.txt", b"same content every time");
        h.write_version("/doc.txt", b"same content every time");
        h.write_version("/doc.txt", b"same content every time");

        let stats = h.history().storage_stats("/doc.txt").unwrap();
        assert_eq!(stats.version_count, 3);
        // Naive: 3x the file size. Actual: 1x (100% dedup across versions)
        assert!(stats.dedup_ratio() > 0.6);
        assert_eq!(stats.unique_chunks, 1);
        assert_eq!(stats.total_chunk_refs, 3);
    }

    #[test]
    fn storage_stats_partial_dedup() {
        let h = TestHarness::new();

        // Large enough to chunk, with small differences between versions
        let mut data: Vec<u8> = (0..256 * 1024)
            .map(|i| (i.wrapping_mul(31).wrapping_add(17)) as u8)
            .collect();

        h.write_version("/big.bin", &data);

        // Small edit
        for i in 100_000..100_500 {
            data[i] = 0xFF;
        }
        h.write_version("/big.bin", &data);

        let stats = h.history().storage_stats("/big.bin").unwrap();
        assert_eq!(stats.version_count, 2);
        assert!(stats.dedup_ratio() > 0.3, "should see meaningful dedup");
        assert!(
            stats.actual_bytes < stats.naive_total_bytes,
            "actual storage should be less than naive"
        );
    }
}
