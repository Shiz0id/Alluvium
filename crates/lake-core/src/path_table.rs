use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::{params, Connection, OptionalExtension};

use crate::types::{B3Hash, Version, VersionTrigger};

// ─── Error Type ──────────────────────────────────────────────────

#[derive(Debug)]
pub enum PathTableError {
    Sqlite(rusqlite::Error),
    NotFound(PathBuf),
    Deleted(PathBuf),
}

impl From<rusqlite::Error> for PathTableError {
    fn from(e: rusqlite::Error) -> Self {
        PathTableError::Sqlite(e)
    }
}

impl std::fmt::Display for PathTableError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PathTableError::Sqlite(e) => write!(f, "database error: {}", e),
            PathTableError::NotFound(p) => write!(f, "not found: {}", p.display()),
            PathTableError::Deleted(p) => write!(f, "deleted: {}", p.display()),
        }
    }
}

impl std::error::Error for PathTableError {}

type Result<T> = std::result::Result<T, PathTableError>;

// ─── Path Table ──────────────────────────────────────────────────

/// The thin mutable layer between POSIX paths and the immutable lake.
/// Everything else in the system is append-only. This is the one place
/// where state changes: HEAD pointers move, files get created and deleted.
pub struct PathTable {
    conn: Connection,
}

impl PathTable {
    /// Open or create a path table database at the given path.
    pub fn open(db_path: impl AsRef<Path>) -> Result<Self> {
        let conn = Connection::open(db_path)?;
        let table = PathTable { conn };
        table.init_schema()?;
        Ok(table)
    }

    /// In-memory path table for testing.
    pub fn in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        let table = PathTable { conn };
        table.init_schema()?;
        Ok(table)
    }

    fn init_schema(&self) -> Result<()> {
        self.conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS paths (
                path        TEXT PRIMARY KEY,
                head        TEXT NOT NULL,
                created_at  INTEGER NOT NULL,
                modified_at INTEGER NOT NULL,
                deleted     INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS versions (
                path        TEXT NOT NULL,
                manifest    TEXT NOT NULL,
                version_num INTEGER NOT NULL,
                timestamp   INTEGER NOT NULL,
                trigger_type TEXT NOT NULL,
                message     TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_versions_path
                ON versions(path, version_num DESC);

            CREATE UNIQUE INDEX IF NOT EXISTS idx_versions_path_num
                ON versions(path, version_num);

            CREATE INDEX IF NOT EXISTS idx_paths_prefix
                ON paths(path);
            ",
        )?;
        Ok(())
    }

    // ─── Write Operations ────────────────────────────────────────

    /// Create or update a file at the given path with a new manifest hash.
    /// Appends a version record and updates the HEAD pointer.
    /// If the path was soft-deleted, it gets resurrected.
    pub fn put(
        &self,
        path: impl AsRef<Path>,
        manifest_hash: &B3Hash,
        trigger: VersionTrigger,
        message: Option<&str>,
    ) -> Result<()> {
        let path_str = path.as_ref().to_string_lossy().to_string();
        let hash_hex = manifest_hash.to_hex();
        let now = now_millis();
        let trigger_str = trigger_to_str(&trigger);

        let tx = self.conn.unchecked_transaction()?;

        // Upsert the path entry
        tx.execute(
            "INSERT INTO paths (path, head, created_at, modified_at, deleted)
             VALUES (?1, ?2, ?3, ?3, 0)
             ON CONFLICT(path) DO UPDATE SET
                head = ?2,
                modified_at = ?3,
                deleted = 0",
            params![path_str, hash_hex, now],
        )?;

        // Get next version number for this path
        let next_version: u64 = tx
            .query_row(
                "SELECT COALESCE(MAX(version_num), 0) + 1 FROM versions WHERE path = ?1",
                params![path_str],
                |row| row.get(0),
            )?;

        // Append version record
        tx.execute(
            "INSERT INTO versions (path, manifest, version_num, timestamp, trigger_type, message)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![path_str, hash_hex, next_version, now, trigger_str, message],
        )?;

        tx.commit()?;
        Ok(())
    }

    /// Soft-delete a file. Path entry remains with history intact.
    pub fn delete(&self, path: impl AsRef<Path>) -> Result<()> {
        let path_str = path.as_ref().to_string_lossy().to_string();
        let affected = self.conn.execute(
            "UPDATE paths SET deleted = 1, modified_at = ?1 WHERE path = ?2 AND deleted = 0",
            params![now_millis(), path_str],
        )?;
        if affected == 0 {
            return Err(PathTableError::NotFound(path.as_ref().to_path_buf()));
        }
        Ok(())
    }

    /// Restore a soft-deleted file.
    pub fn restore(&self, path: impl AsRef<Path>) -> Result<()> {
        let path_str = path.as_ref().to_string_lossy().to_string();
        let affected = self.conn.execute(
            "UPDATE paths SET deleted = 0, modified_at = ?1 WHERE path = ?2 AND deleted = 1",
            params![now_millis(), path_str],
        )?;
        if affected == 0 {
            return Err(PathTableError::NotFound(path.as_ref().to_path_buf()));
        }
        Ok(())
    }

    /// Rename/move a file. Metadata-only — the lake objects don't change.
    pub fn rename(
        &self,
        from: impl AsRef<Path>,
        to: impl AsRef<Path>,
    ) -> Result<()> {
        let from_str = from.as_ref().to_string_lossy().to_string();
        let to_str = to.as_ref().to_string_lossy().to_string();
        let now = now_millis();

        let tx = self.conn.unchecked_transaction()?;

        // Get the current entry
        let (head, created_at): (String, i64) = tx
            .query_row(
                "SELECT head, created_at FROM paths WHERE path = ?1 AND deleted = 0",
                params![from_str],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?
            .ok_or_else(|| PathTableError::NotFound(from.as_ref().to_path_buf()))?;

        // Create the new path entry
        tx.execute(
            "INSERT INTO paths (path, head, created_at, modified_at, deleted)
             VALUES (?1, ?2, ?3, ?4, 0)
             ON CONFLICT(path) DO UPDATE SET
                head = ?2, modified_at = ?4, deleted = 0",
            params![to_str, head, created_at, now],
        )?;

        // Move version history to the new path
        tx.execute(
            "UPDATE versions SET path = ?1 WHERE path = ?2",
            params![to_str, from_str],
        )?;

        // Remove old path entry
        tx.execute("DELETE FROM paths WHERE path = ?1", params![from_str])?;

        tx.commit()?;
        Ok(())
    }

    // ─── Read Operations ─────────────────────────────────────────

    /// Get the current HEAD manifest hash for a path.
    pub fn get_head(&self, path: impl AsRef<Path>) -> Result<B3Hash> {
        let path_str = path.as_ref().to_string_lossy().to_string();

        let row: Option<(String, bool)> = self
            .conn
            .query_row(
                "SELECT head, deleted FROM paths WHERE path = ?1",
                params![path_str],
                |row| Ok((row.get(0)?, row.get::<_, i32>(1)? != 0)),
            )
            .optional()?;

        match row {
            Some((_, true)) => Err(PathTableError::Deleted(path.as_ref().to_path_buf())),
            Some((hex, false)) => B3Hash::from_hex(&hex)
                .map_err(|_| PathTableError::NotFound(path.as_ref().to_path_buf())),
            None => Err(PathTableError::NotFound(path.as_ref().to_path_buf())),
        }
    }

    /// Get the full version history for a path, newest first.
    pub fn get_history(&self, path: impl AsRef<Path>) -> Result<Vec<Version>> {
        let path_str = path.as_ref().to_string_lossy().to_string();

        let mut stmt = self.conn.prepare(
            "SELECT manifest, version_num, timestamp, trigger_type, message
             FROM versions
             WHERE path = ?1
             ORDER BY version_num DESC",
        )?;

        let versions = stmt
            .query_map(params![path_str], |row| {
                let hash_hex: String = row.get(0)?;
                let version_num: u64 = row.get(1)?;
                let timestamp: u64 = row.get(2)?;
                let trigger_str: String = row.get(3)?;
                let message: Option<String> = row.get(4)?;
                Ok((hash_hex, version_num, timestamp, trigger_str, message))
            })?
            .filter_map(|r| r.ok())
            .filter_map(|(hex, vn, ts, trig, msg)| {
                let hash = B3Hash::from_hex(&hex).ok()?;
                let trigger = str_to_trigger(&trig)?;
                Some(Version {
                    manifest_hash: hash,
                    version_num: vn,
                    timestamp: ts,
                    trigger,
                    message: msg,
                })
            })
            .collect();

        Ok(versions)
    }

    /// Get the manifest hash for a specific version number.
    pub fn get_version(
        &self,
        path: impl AsRef<Path>,
        version_num: u64,
    ) -> Result<B3Hash> {
        let path_str = path.as_ref().to_string_lossy().to_string();

        let hex: String = self
            .conn
            .query_row(
                "SELECT manifest FROM versions
                 WHERE path = ?1 AND version_num = ?2",
                params![path_str, version_num],
                |row| row.get(0),
            )
            .optional()?
            .ok_or_else(|| PathTableError::NotFound(path.as_ref().to_path_buf()))?;

        B3Hash::from_hex(&hex)
            .map_err(|_| PathTableError::NotFound(path.as_ref().to_path_buf()))
    }

    /// Get the latest version number for a path.
    pub fn get_current_version_num(&self, path: impl AsRef<Path>) -> Result<u64> {
        let path_str = path.as_ref().to_string_lossy().to_string();

        self.conn
            .query_row(
                "SELECT COALESCE(MAX(version_num), 0) FROM versions WHERE path = ?1",
                params![path_str],
                |row| row.get(0),
            )
            .map_err(PathTableError::from)
    }

    /// Rewind a file to a specific version by manifest hash.
    /// The old versions remain — this just moves HEAD backward.
    pub fn rewind(
        &self,
        path: impl AsRef<Path>,
        target_manifest: &B3Hash,
    ) -> Result<()> {
        let path_str = path.as_ref().to_string_lossy().to_string();
        let hash_hex = target_manifest.to_hex();

        // Verify the target manifest exists in this file's history
        let exists: bool = self
            .conn
            .query_row(
                "SELECT COUNT(*) > 0 FROM versions
                 WHERE path = ?1 AND manifest = ?2",
                params![path_str, hash_hex],
                |row| row.get(0),
            )?;

        if !exists {
            return Err(PathTableError::NotFound(path.as_ref().to_path_buf()));
        }

        // Move HEAD to the target, record as an explicit version
        self.put(
            path,
            target_manifest,
            VersionTrigger::Explicit,
            Some("rewind"),
        )
    }

    /// List files in a directory (implicit directories via prefix query).
    /// Returns immediate children only — not recursive.
    pub fn list_dir(&self, dir: impl AsRef<Path>) -> Result<Vec<DirEntry>> {
        let mut prefix = dir.as_ref().to_string_lossy().to_string();
        if !prefix.ends_with('/') {
            prefix.push('/');
        }

        // SQLite prefix query: path >= prefix AND path < prefix with last char incremented
        let mut upper = prefix.clone();
        let last = upper.pop().unwrap();
        upper.push((last as u8 + 1) as char);

        let mut stmt = self.conn.prepare(
            "SELECT path FROM paths
             WHERE path >= ?1 AND path < ?2 AND deleted = 0
             ORDER BY path",
        )?;

        let mut entries = Vec::new();
        let mut seen_dirs = std::collections::HashSet::new();

        let rows: Vec<String> = stmt
            .query_map(params![prefix, upper], |row| row.get(0))?
            .filter_map(|r| r.ok())
            .collect();

        for full_path in rows {
            let relative = &full_path[prefix.len()..];
            if let Some(slash_pos) = relative.find('/') {
                // This is a file in a subdirectory — the subdirectory is an implicit dir
                let dir_name = &relative[..slash_pos];
                if seen_dirs.insert(dir_name.to_string()) {
                    entries.push(DirEntry {
                        name: dir_name.to_string(),
                        is_dir: true,
                    });
                }
            } else {
                // Direct child file
                entries.push(DirEntry {
                    name: relative.to_string(),
                    is_dir: false,
                });
            }
        }

        Ok(entries)
    }

    /// Check if a path exists (not deleted).
    pub fn exists(&self, path: impl AsRef<Path>) -> bool {
        self.get_head(path).is_ok()
    }

    /// List all soft-deleted file paths.
    pub fn list_deleted(&self) -> Result<Vec<String>> {
        let mut stmt = self.conn.prepare(
            "SELECT path FROM paths WHERE deleted = 1 ORDER BY path",
        )?;

        let paths = stmt
            .query_map([], |row| row.get(0))?
            .filter_map(|r| r.ok())
            .collect();

        Ok(paths)
    }

    /// List all active (non-deleted) paths in the table.
    /// Returns every path regardless of hierarchy — works for both
    /// slash-delimited paths and flat UUID identifiers.
    pub fn list_all(&self) -> Result<Vec<String>> {
        let mut stmt = self.conn.prepare(
            "SELECT path FROM paths WHERE deleted = 0 ORDER BY path",
        )?;

        let paths = stmt
            .query_map([], |row| row.get(0))?
            .filter_map(|r| r.ok())
            .collect();

        Ok(paths)
    }
}

// ─── Directory Entry ─────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct DirEntry {
    pub name: String,
    pub is_dir: bool,
}

// ─── Helpers ─────────────────────────────────────────────────────

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn trigger_to_str(t: &VersionTrigger) -> &'static str {
    match t {
        VersionTrigger::Close => "close",
        VersionTrigger::Explicit => "explicit",
        VersionTrigger::MicroHash => "micro_hash",
    }
}

fn str_to_trigger(s: &str) -> Option<VersionTrigger> {
    match s {
        "close" => Some(VersionTrigger::Close),
        "explicit" => Some(VersionTrigger::Explicit),
        "micro_hash" => Some(VersionTrigger::MicroHash),
        _ => None,
    }
}

// ─── Tests ───────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn hash(s: &str) -> B3Hash {
        B3Hash::from_bytes(s.as_bytes())
    }

    #[test]
    fn create_and_read() {
        let pt = PathTable::in_memory().unwrap();
        let h = hash("v1");

        pt.put("/docs/a.txt", &h, VersionTrigger::Close, None).unwrap();

        assert_eq!(pt.get_head("/docs/a.txt").unwrap(), h);
    }

    #[test]
    fn update_moves_head() {
        let pt = PathTable::in_memory().unwrap();
        let h1 = hash("v1");
        let h2 = hash("v2");

        pt.put("/a.txt", &h1, VersionTrigger::Close, None).unwrap();
        pt.put("/a.txt", &h2, VersionTrigger::Close, None).unwrap();

        assert_eq!(pt.get_head("/a.txt").unwrap(), h2);
    }

    #[test]
    fn history_newest_first() {
        let pt = PathTable::in_memory().unwrap();
        let h1 = hash("v1");
        let h2 = hash("v2");
        let h3 = hash("v3");

        pt.put("/a.txt", &h1, VersionTrigger::Close, None).unwrap();
        pt.put("/a.txt", &h2, VersionTrigger::Close, None).unwrap();
        pt.put("/a.txt", &h3, VersionTrigger::Explicit, Some("final"))
            .unwrap();

        let hist = pt.get_history("/a.txt").unwrap();
        assert_eq!(hist.len(), 3);
        assert_eq!(hist[0].manifest_hash, h3);
        assert_eq!(hist[0].message.as_deref(), Some("final"));
        assert_eq!(hist[2].manifest_hash, h1);
    }

    #[test]
    fn soft_delete_and_restore() {
        let pt = PathTable::in_memory().unwrap();
        let h = hash("v1");

        pt.put("/a.txt", &h, VersionTrigger::Close, None).unwrap();
        pt.delete("/a.txt").unwrap();

        assert!(matches!(
            pt.get_head("/a.txt"),
            Err(PathTableError::Deleted(_))
        ));

        pt.restore("/a.txt").unwrap();
        assert_eq!(pt.get_head("/a.txt").unwrap(), h);
    }

    #[test]
    fn rewind_to_previous_version() {
        let pt = PathTable::in_memory().unwrap();
        let h1 = hash("v1");
        let h2 = hash("v2");
        let h3 = hash("v3");

        pt.put("/a.txt", &h1, VersionTrigger::Close, None).unwrap();
        pt.put("/a.txt", &h2, VersionTrigger::Close, None).unwrap();
        pt.put("/a.txt", &h3, VersionTrigger::Close, None).unwrap();

        pt.rewind("/a.txt", &h1).unwrap();
        assert_eq!(pt.get_head("/a.txt").unwrap(), h1);

        // Rewind creates a new version entry, doesn't destroy history
        let hist = pt.get_history("/a.txt").unwrap();
        assert_eq!(hist.len(), 4); // 3 original + 1 rewind
    }

    #[test]
    fn rename_preserves_history() {
        let pt = PathTable::in_memory().unwrap();
        let h1 = hash("v1");
        let h2 = hash("v2");

        pt.put("/old.txt", &h1, VersionTrigger::Close, None).unwrap();
        pt.put("/old.txt", &h2, VersionTrigger::Close, None).unwrap();
        pt.rename("/old.txt", "/new.txt").unwrap();

        assert!(!pt.exists("/old.txt"));
        assert_eq!(pt.get_head("/new.txt").unwrap(), h2);

        let hist = pt.get_history("/new.txt").unwrap();
        assert_eq!(hist.len(), 2);
    }

    #[test]
    fn list_dir_immediate_children() {
        let pt = PathTable::in_memory().unwrap();

        pt.put("/docs/a.txt", &hash("a"), VersionTrigger::Close, None).unwrap();
        pt.put("/docs/b.txt", &hash("b"), VersionTrigger::Close, None).unwrap();
        pt.put("/docs/sub/c.txt", &hash("c"), VersionTrigger::Close, None).unwrap();
        pt.put("/docs/sub/deep/d.txt", &hash("d"), VersionTrigger::Close, None).unwrap();
        pt.put("/other/e.txt", &hash("e"), VersionTrigger::Close, None).unwrap();

        let entries = pt.list_dir("/docs").unwrap();
        assert_eq!(entries.len(), 3); // a.txt, b.txt, sub/

        let files: Vec<_> = entries.iter().filter(|e| !e.is_dir).collect();
        let dirs: Vec<_> = entries.iter().filter(|e| e.is_dir).collect();

        assert_eq!(files.len(), 2);
        assert_eq!(dirs.len(), 1);
        assert_eq!(dirs[0].name, "sub");
    }

    #[test]
    fn deleted_files_hidden_from_listing() {
        let pt = PathTable::in_memory().unwrap();

        pt.put("/docs/a.txt", &hash("a"), VersionTrigger::Close, None).unwrap();
        pt.put("/docs/b.txt", &hash("b"), VersionTrigger::Close, None).unwrap();
        pt.delete("/docs/b.txt").unwrap();

        let entries = pt.list_dir("/docs").unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "a.txt");
    }
}
