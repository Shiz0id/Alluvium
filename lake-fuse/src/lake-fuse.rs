use std::collections::HashMap;
use std::ffi::OsStr;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use fuser::{
    FileAttr, FileType, Filesystem, MountOption, ReplyAttr, ReplyCreate,
    ReplyData, ReplyDirectory, ReplyEntry, ReplyWrite, Request,
};
use rusqlite::{params, Connection, OptionalExtension};

use lake_core::history::History;
use lake_core::ingest::Ingester;
use lake_core::path_table::PathTable;
use lake_core::store::ObjectStore;
use lake_core::types::{B3Hash, VersionTrigger};

// ─── Constants ───────────────────────────────────────────────────

const ROOT_INO: u64 = 1;
const TTL: Duration = Duration::from_secs(1);
const BLOCK_SIZE: u32 = 512;

/// Virtual inodes live in the high range to never collide with real inodes.
const VIRTUAL_INO_BASE: u64 = 0x8000_0000_0000_0000;
const DEEPLAKE_DIR: &str = ".deeplake";

// ─── Virtual Path Resolution ─────────────────────────────────────

/// Parsed result of a .deeplake/* path.
#[derive(Debug, Clone)]
enum VirtualPath {
    /// /.deeplake itself
    Root,
    /// /.deeplake/history
    HistoryRoot,
    /// /.deeplake/history/{file_path} — list versions of a file
    HistoryFile { file_path: String },
    /// /.deeplake/history/{file_path}/vN — content at version N
    HistoryVersion { file_path: String, version: u64 },
    /// /.deeplake/timeline
    TimelineRoot,
    /// /.deeplake/timeline/YYYY-MM-DD — list versions created on this date
    TimelineDate { date: String },
    /// /.deeplake/stats
    StatsRoot,
    /// /.deeplake/stats/{file_path} — storage stats for a file
    StatsFile { file_path: String },
    /// /.deeplake/deleted
    DeletedRoot,
}

/// Parse a full path into a VirtualPath if it starts with /.deeplake.
fn parse_virtual(path: &str) -> Option<VirtualPath> {
    let trimmed = path.strip_prefix("/.deeplake")?;
    if trimmed.is_empty() || trimmed == "/" {
        return Some(VirtualPath::Root);
    }
    let rest = trimmed.strip_prefix('/')?;

    if rest == "history" || rest == "history/" {
        return Some(VirtualPath::HistoryRoot);
    }
    if let Some(inner) = rest.strip_prefix("history/") {
        // Check if last segment is vN
        if let Some(slash_pos) = inner.rfind('/') {
            let (file_part, version_part) = inner.split_at(slash_pos);
            let version_str = &version_part[1..]; // skip the /
            if let Some(num_str) = version_str.strip_prefix('v') {
                if let Ok(n) = num_str.parse::<u64>() {
                    return Some(VirtualPath::HistoryVersion {
                        file_path: format!("/{}", file_part),
                        version: n,
                    });
                }
            }
            // Not a version — treat entire inner as a directory in the file path
            return Some(VirtualPath::HistoryFile {
                file_path: format!("/{}", inner.trim_end_matches('/')),
            });
        }
        // No slash — could be a top-level file
        return Some(VirtualPath::HistoryFile {
            file_path: format!("/{}", inner.trim_end_matches('/')),
        });
    }

    if rest == "timeline" || rest == "timeline/" {
        return Some(VirtualPath::TimelineRoot);
    }
    if let Some(date) = rest.strip_prefix("timeline/") {
        return Some(VirtualPath::TimelineDate {
            date: date.trim_end_matches('/').to_string(),
        });
    }

    if rest == "stats" || rest == "stats/" {
        return Some(VirtualPath::StatsRoot);
    }
    if let Some(inner) = rest.strip_prefix("stats/") {
        return Some(VirtualPath::StatsFile {
            file_path: format!("/{}", inner.trim_end_matches('/')),
        });
    }

    if rest == "deleted" || rest == "deleted/" {
        return Some(VirtualPath::DeletedRoot);
    }

    None
}

// ─── Inode Store ─────────────────────────────────────────────────

struct InodeStore {
    conn: Connection,
}

impl InodeStore {
    fn open(db_path: impl AsRef<Path>) -> io::Result<Self> {
        let conn = Connection::open(db_path)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS inodes (
                ino     INTEGER PRIMARY KEY,
                path    TEXT NOT NULL UNIQUE,
                is_dir  INTEGER NOT NULL DEFAULT 0
            );
            INSERT OR IGNORE INTO inodes (ino, path, is_dir) VALUES (1, '/', 1);
            ",
        )
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        Ok(InodeStore { conn })
    }

    fn get_or_assign(&self, path: &str, is_dir: bool) -> io::Result<u64> {
        let existing: Option<u64> = self
            .conn
            .query_row(
                "SELECT ino FROM inodes WHERE path = ?1",
                params![path],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        if let Some(ino) = existing {
            return Ok(ino);
        }

        self.conn
            .execute(
                "INSERT INTO inodes (path, is_dir) VALUES (?1, ?2)",
                params![path, is_dir as i32],
            )
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        Ok(self.conn.last_insert_rowid() as u64)
    }

    fn get_path(&self, ino: u64) -> io::Result<Option<(String, bool)>> {
        self.conn
            .query_row(
                "SELECT path, is_dir FROM inodes WHERE ino = ?1",
                params![ino],
                |row| Ok((row.get(0)?, row.get::<_, i32>(1)? != 0)),
            )
            .optional()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
    }

    fn remove(&self, ino: u64) -> io::Result<()> {
        self.conn
            .execute("DELETE FROM inodes WHERE ino = ?1", params![ino])
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        Ok(())
    }

    fn rename(&self, old_path: &str, new_path: &str) -> io::Result<()> {
        self.conn
            .execute(
                "UPDATE inodes SET path = ?1 WHERE path = ?2",
                params![new_path, old_path],
            )
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        Ok(())
    }
}

// ─── Virtual Inode Mapping ───────────────────────────────────────

/// Maps virtual paths to stable virtual inodes.
/// These are ephemeral — recomputed each session — but stable within a session.
struct VirtualInodes {
    map: HashMap<String, u64>,
    next: u64,
}

impl VirtualInodes {
    fn new() -> Self {
        VirtualInodes {
            map: HashMap::new(),
            next: VIRTUAL_INO_BASE + 1,
        }
    }

    fn get_or_assign(&mut self, vpath: &str) -> u64 {
        if let Some(&ino) = self.map.get(vpath) {
            return ino;
        }
        let ino = self.next;
        self.next += 1;
        self.map.insert(vpath.to_string(), ino);
        ino
    }

    fn get_path(&self, ino: u64) -> Option<&str> {
        self.map
            .iter()
            .find(|(_, &v)| v == ino)
            .map(|(k, _)| k.as_str())
    }
}

fn is_virtual_ino(ino: u64) -> bool {
    ino >= VIRTUAL_INO_BASE
}

// ─── Write Buffer ────────────────────────────────────────────────

struct WriteBuffer {
    path: PathBuf,
    data: Vec<u8>,
    dirty: bool,
}

// ─── LakeFS FUSE ─────────────────────────────────────────────────

pub struct LakeFS {
    store: ObjectStore,
    paths: PathTable,
    inodes: InodeStore,
    virtual_inodes: Mutex<VirtualInodes>,
    open_files: Mutex<HashMap<u64, WriteBuffer>>,
    next_fh: Mutex<u64>,
}

impl LakeFS {
    pub fn new(store: ObjectStore, paths: PathTable, inodes: InodeStore) -> Self {
        // Pre-assign .deeplake root
        let mut vi = VirtualInodes::new();
        vi.get_or_assign("/.deeplake");
        vi.get_or_assign("/.deeplake/history");
        vi.get_or_assign("/.deeplake/timeline");
        vi.get_or_assign("/.deeplake/stats");
        vi.get_or_assign("/.deeplake/deleted");

        LakeFS {
            store,
            paths,
            inodes,
            virtual_inodes: Mutex::new(vi),
            open_files: Mutex::new(HashMap::new()),
            next_fh: Mutex::new(1),
        }
    }

    fn alloc_fh(&self) -> u64 {
        let mut fh = self.next_fh.lock().unwrap();
        let id = *fh;
        *fh += 1;
        id
    }

    fn history(&self) -> History {
        History::new(&self.store, &self.paths)
    }

    fn resolve_child(&self, parent: u64, name: &OsStr) -> io::Result<String> {
        let name_str = name.to_string_lossy();

        // Virtual parent
        if is_virtual_ino(parent) {
            let vi = self.virtual_inodes.lock().unwrap();
            if let Some(parent_path) = vi.get_path(parent) {
                let parent_path = parent_path.to_string();
                drop(vi);
                if parent_path == "/" {
                    return Ok(format!("/{}", name_str));
                }
                return Ok(format!("{}/{}", parent_path, name_str));
            }
        }

        if parent == ROOT_INO {
            Ok(format!("/{}", name_str))
        } else {
            let (parent_path, _) = self
                .inodes
                .get_path(parent)?
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "parent not found"))?;
            if parent_path == "/" {
                Ok(format!("/{}", name_str))
            } else {
                Ok(format!("{}/{}", parent_path, name_str))
            }
        }
    }

    fn file_attr(&self, ino: u64, path: &str) -> io::Result<FileAttr> {
        let head = self
            .paths
            .get_head(path)
            .map_err(|_| io::Error::new(io::ErrorKind::NotFound, "not found"))?;
        let manifest = self.store.get_manifest(&head)?;
        let size = manifest.total_size;

        Ok(FileAttr {
            ino,
            size,
            blocks: (size + BLOCK_SIZE as u64 - 1) / BLOCK_SIZE as u64,
            atime: SystemTime::now(),
            mtime: SystemTime::now(),
            ctime: SystemTime::now(),
            crtime: SystemTime::now(),
            kind: FileType::RegularFile,
            perm: 0o644,
            nlink: 1,
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
            rdev: 0,
            blksize: BLOCK_SIZE,
            flags: 0,
        })
    }

    fn dir_attr(&self, ino: u64) -> FileAttr {
        FileAttr {
            ino,
            size: 0,
            blocks: 0,
            atime: SystemTime::now(),
            mtime: SystemTime::now(),
            ctime: SystemTime::now(),
            crtime: SystemTime::now(),
            kind: FileType::Directory,
            perm: 0o755,
            nlink: 2,
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
            rdev: 0,
            blksize: BLOCK_SIZE,
            flags: 0,
        }
    }

    fn virtual_file_attr(&self, ino: u64, size: u64) -> FileAttr {
        FileAttr {
            ino,
            size,
            blocks: (size + BLOCK_SIZE as u64 - 1) / BLOCK_SIZE as u64,
            atime: SystemTime::now(),
            mtime: SystemTime::now(),
            ctime: SystemTime::now(),
            crtime: SystemTime::now(),
            kind: FileType::RegularFile,
            perm: 0o444, // read-only
            nlink: 1,
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
            rdev: 0,
            blksize: BLOCK_SIZE,
            flags: 0,
        }
    }

    // ─── Virtual Namespace Handlers ──────────────────────────────

    fn virtual_lookup(&self, full_path: &str, reply: ReplyEntry) {
        let vpath = match parse_virtual(full_path) {
            Some(v) => v,
            None => return reply.error(libc::ENOENT),
        };

        match vpath {
            VirtualPath::Root
            | VirtualPath::HistoryRoot
            | VirtualPath::TimelineRoot
            | VirtualPath::StatsRoot
            | VirtualPath::DeletedRoot => {
                let mut vi = self.virtual_inodes.lock().unwrap();
                let ino = vi.get_or_assign(full_path);
                reply.entry(&TTL, &self.dir_attr(ino), 0);
            }
            VirtualPath::HistoryFile { ref file_path } => {
                // Check if this file exists in the path table (including deleted)
                if let Ok(history) = self.paths.get_history(file_path) {
                    if !history.is_empty() {
                        let mut vi = self.virtual_inodes.lock().unwrap();
                        let ino = vi.get_or_assign(full_path);
                        reply.entry(&TTL, &self.dir_attr(ino), 0);
                        return;
                    }
                }
                // Maybe it's an intermediate directory in the history view
                // Check if any files exist under this prefix
                if let Ok(entries) = self.paths.list_dir(file_path) {
                    if !entries.is_empty() {
                        let mut vi = self.virtual_inodes.lock().unwrap();
                        let ino = vi.get_or_assign(full_path);
                        reply.entry(&TTL, &self.dir_attr(ino), 0);
                        return;
                    }
                }
                reply.error(libc::ENOENT);
            }
            VirtualPath::HistoryVersion {
                ref file_path,
                version,
            } => {
                let history = self.history();
                match history.read_at_version(file_path, version) {
                    Ok(data) => {
                        let mut vi = self.virtual_inodes.lock().unwrap();
                        let ino = vi.get_or_assign(full_path);
                        reply.entry(
                            &TTL,
                            &self.virtual_file_attr(ino, data.len() as u64),
                            0,
                        );
                    }
                    Err(_) => reply.error(libc::ENOENT),
                }
            }
            VirtualPath::TimelineDate { ref date } => {
                let mut vi = self.virtual_inodes.lock().unwrap();
                let ino = vi.get_or_assign(full_path);
                reply.entry(&TTL, &self.dir_attr(ino), 0);
            }
            VirtualPath::StatsFile { ref file_path } => {
                let history = self.history();
                match history.storage_stats(file_path) {
                    Ok(stats) => {
                        let text = format_stats(&stats, file_path);
                        let mut vi = self.virtual_inodes.lock().unwrap();
                        let ino = vi.get_or_assign(full_path);
                        reply.entry(
                            &TTL,
                            &self.virtual_file_attr(ino, text.len() as u64),
                            0,
                        );
                    }
                    Err(_) => reply.error(libc::ENOENT),
                }
            }
        }
    }

    fn virtual_readdir(
        &self,
        full_path: &str,
        ino: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        let vpath = match parse_virtual(full_path) {
            Some(v) => v,
            None => return reply.error(libc::ENOENT),
        };

        let mut entries: Vec<(FileType, String)> = Vec::new();

        match vpath {
            VirtualPath::Root => {
                entries.push((FileType::Directory, "history".to_string()));
                entries.push((FileType::Directory, "timeline".to_string()));
                entries.push((FileType::Directory, "stats".to_string()));
                entries.push((FileType::Directory, "deleted".to_string()));
            }
            VirtualPath::HistoryRoot => {
                // Show top-level files and directories
                if let Ok(dir_entries) = self.paths.list_dir("/") {
                    for e in dir_entries {
                        let ft = if e.is_dir {
                            FileType::Directory
                        } else {
                            FileType::Directory // files are dirs in history (they contain versions)
                        };
                        entries.push((ft, e.name));
                    }
                }
            }
            VirtualPath::HistoryFile { ref file_path } => {
                // If it's a file with history, show versions
                if let Ok(history) = self.paths.get_history(file_path) {
                    if !history.is_empty() {
                        for v in &history {
                            entries.push((
                                FileType::RegularFile,
                                format!("v{}", v.version_num),
                            ));
                        }
                    } else {
                        // It's an intermediate directory — show children
                        if let Ok(dir_entries) = self.paths.list_dir(file_path) {
                            for e in dir_entries {
                                entries.push((FileType::Directory, e.name));
                            }
                        }
                    }
                } else {
                    // Intermediate directory
                    if let Ok(dir_entries) = self.paths.list_dir(file_path) {
                        for e in dir_entries {
                            entries.push((FileType::Directory, e.name));
                        }
                    }
                }
            }
            VirtualPath::TimelineRoot => {
                // List distinct dates from version history
                if let Ok(dates) = self.get_timeline_dates() {
                    for d in dates {
                        entries.push((FileType::Directory, d));
                    }
                }
            }
            VirtualPath::TimelineDate { ref date } => {
                if let Ok(items) = self.get_timeline_entries(date) {
                    for (time_str, filename) in items {
                        entries.push((
                            FileType::RegularFile,
                            format!("{}_{}", time_str, filename),
                        ));
                    }
                }
            }
            VirtualPath::StatsRoot => {
                // Mirror the real directory structure
                if let Ok(dir_entries) = self.paths.list_dir("/") {
                    for e in dir_entries {
                        let ft = if e.is_dir {
                            FileType::Directory
                        } else {
                            FileType::RegularFile
                        };
                        entries.push((ft, e.name));
                    }
                }
            }
            VirtualPath::DeletedRoot => {
                if let Ok(deleted) = self.get_deleted_files() {
                    for name in deleted {
                        entries.push((FileType::RegularFile, name));
                    }
                }
            }
            _ => return reply.error(libc::ENOENT),
        }

        // Build full reply with . and ..
        let mut full: Vec<(u64, FileType, String)> = Vec::new();
        full.push((ino, FileType::Directory, ".".to_string()));
        full.push((ino, FileType::Directory, "..".to_string()));

        let mut vi = self.virtual_inodes.lock().unwrap();
        for (ft, name) in entries {
            let child_vpath = if full_path == "/.deeplake" {
                format!("/.deeplake/{}", name)
            } else {
                format!("{}/{}", full_path, name)
            };
            let child_ino = vi.get_or_assign(&child_vpath);
            full.push((child_ino, ft, name));
        }
        drop(vi);

        for (i, (child_ino, ft, name)) in full.iter().enumerate().skip(offset as usize) {
            if reply.add(*child_ino, (i + 1) as i64, *ft, name) {
                break;
            }
        }
        reply.ok();
    }

    fn virtual_read(&self, full_path: &str, offset: i64, size: u32, reply: ReplyData) {
        let vpath = match parse_virtual(full_path) {
            Some(v) => v,
            None => return reply.error(libc::ENOENT),
        };

        let data = match vpath {
            VirtualPath::HistoryVersion { file_path, version } => {
                match self.history().read_at_version(&file_path, version) {
                    Ok(d) => d,
                    Err(_) => return reply.error(libc::ENOENT),
                }
            }
            VirtualPath::StatsFile { file_path } => {
                match self.history().storage_stats(&file_path) {
                    Ok(stats) => format_stats(&stats, &file_path).into_bytes(),
                    Err(_) => return reply.error(libc::ENOENT),
                }
            }
            _ => return reply.error(libc::EISDIR),
        };

        let start = offset as usize;
        let end = (start + size as usize).min(data.len());
        if start >= data.len() {
            reply.data(&[]);
        } else {
            reply.data(&data[start..end]);
        }
    }

    // ─── Timeline Queries ────────────────────────────────────────

    fn get_timeline_dates(&self) -> Result<Vec<String>, io::Error> {
        // Query distinct dates from the versions table via path table's connection
        // For prototype: scan all history and extract dates
        // This is a bit of a reach into the path table's internals.
        // In production you'd add a proper method to PathTable.
        //
        // For now, get all files and walk their histories.
        let all_files = self.paths.list_dir("/").map_err(|e| {
            io::Error::new(io::ErrorKind::Other, e)
        })?;

        let mut dates = std::collections::BTreeSet::new();
        self.collect_dates("/", &mut dates);
        Ok(dates.into_iter().collect())
    }

    fn collect_dates(&self, dir: &str, dates: &mut std::collections::BTreeSet<String>) {
        if let Ok(entries) = self.paths.list_dir(dir) {
            for e in entries {
                let child = if dir == "/" {
                    format!("/{}", e.name)
                } else {
                    format!("{}/{}", dir, e.name)
                };
                if e.is_dir {
                    self.collect_dates(&child, dates);
                } else if let Ok(history) = self.paths.get_history(&child) {
                    for v in history {
                        let secs = v.timestamp / 1000;
                        let dt = UNIX_EPOCH + Duration::from_secs(secs);
                        if let Ok(elapsed) = dt.duration_since(UNIX_EPOCH) {
                            let days = elapsed.as_secs() / 86400;
                            // Simple date calculation
                            let date = epoch_days_to_date(days);
                            dates.insert(date);
                        }
                    }
                }
            }
        }
    }

    fn get_timeline_entries(
        &self,
        date: &str,
    ) -> Result<Vec<(String, String)>, io::Error> {
        let mut results = Vec::new();
        self.collect_timeline_entries("/", date, &mut results);
        results.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(results)
    }

    fn collect_timeline_entries(
        &self,
        dir: &str,
        target_date: &str,
        results: &mut Vec<(String, String)>,
    ) {
        if let Ok(entries) = self.paths.list_dir(dir) {
            for e in entries {
                let child = if dir == "/" {
                    format!("/{}", e.name)
                } else {
                    format!("{}/{}", dir, e.name)
                };
                if e.is_dir {
                    self.collect_timeline_entries(&child, target_date, results);
                } else if let Ok(history) = self.paths.get_history(&child) {
                    for v in history {
                        let secs = v.timestamp / 1000;
                        let dt = UNIX_EPOCH + Duration::from_secs(secs);
                        if let Ok(elapsed) = dt.duration_since(UNIX_EPOCH) {
                            let days = elapsed.as_secs() / 86400;
                            let date = epoch_days_to_date(days);
                            if date == target_date {
                                let day_secs = (v.timestamp / 1000) % 86400;
                                let h = day_secs / 3600;
                                let m = (day_secs % 3600) / 60;
                                let s = day_secs % 60;
                                let time_str =
                                    format!("{:02}:{:02}:{:02}", h, m, s);
                                let filename = child
                                    .rsplit('/')
                                    .next()
                                    .unwrap_or(&child)
                                    .to_string();
                                results.push((time_str, filename));
                            }
                        }
                    }
                }
            }
        }
    }

    fn get_deleted_files(&self) -> Result<Vec<String>, io::Error> {
        let paths = self
            .paths
            .list_deleted()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        // Extract just the filename from each path for display
        Ok(paths
            .iter()
            .map(|p| {
                p.rsplit('/')
                    .next()
                    .unwrap_or(p)
                    .to_string()
            })
            .collect())
    }
}

// ─── Filesystem Implementation ───────────────────────────────────

impl Filesystem for LakeFS {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let full_path = match self.resolve_child(parent, name) {
            Ok(p) => p,
            Err(_) => return reply.error(libc::ENOENT),
        };

        // Route to virtual namespace
        if full_path.starts_with("/.deeplake") {
            return self.virtual_lookup(&full_path, reply);
        }

        // Check if it's a file
        if self.paths.exists(&full_path) {
            let ino = match self.inodes.get_or_assign(&full_path, false) {
                Ok(i) => i,
                Err(_) => return reply.error(libc::EIO),
            };
            match self.file_attr(ino, &full_path) {
                Ok(attr) => reply.entry(&TTL, &attr, 0),
                Err(_) => reply.error(libc::EIO),
            }
            return;
        }

        // Check if it's an implicit directory
        if let Ok(entries) = self.paths.list_dir(&full_path) {
            if !entries.is_empty() {
                let ino = match self.inodes.get_or_assign(&full_path, true) {
                    Ok(i) => i,
                    Err(_) => return reply.error(libc::EIO),
                };
                reply.entry(&TTL, &self.dir_attr(ino), 0);
                return;
            }
        }

        reply.error(libc::ENOENT);
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        if ino == ROOT_INO {
            return reply.attr(&TTL, &self.dir_attr(ROOT_INO));
        }

        // Virtual inode
        if is_virtual_ino(ino) {
            let vi = self.virtual_inodes.lock().unwrap();
            if let Some(vpath_str) = vi.get_path(ino) {
                let vpath_str = vpath_str.to_string();
                drop(vi);

                if let Some(vpath) = parse_virtual(&vpath_str) {
                    match vpath {
                        VirtualPath::HistoryVersion { file_path, version } => {
                            if let Ok(data) =
                                self.history().read_at_version(&file_path, version)
                            {
                                return reply.attr(
                                    &TTL,
                                    &self.virtual_file_attr(ino, data.len() as u64),
                                );
                            }
                        }
                        VirtualPath::StatsFile { file_path } => {
                            if let Ok(stats) =
                                self.history().storage_stats(&file_path)
                            {
                                let text = format_stats(&stats, &file_path);
                                return reply.attr(
                                    &TTL,
                                    &self.virtual_file_attr(ino, text.len() as u64),
                                );
                            }
                        }
                        _ => return reply.attr(&TTL, &self.dir_attr(ino)),
                    }
                }
            }
            return reply.error(libc::ENOENT);
        }

        // Real inode
        let (path, is_dir) = match self.inodes.get_path(ino) {
            Ok(Some(p)) => p,
            _ => return reply.error(libc::ENOENT),
        };

        if is_dir {
            reply.attr(&TTL, &self.dir_attr(ino));
        } else {
            match self.file_attr(ino, &path) {
                Ok(attr) => reply.attr(&TTL, &attr),
                Err(_) => reply.error(libc::ENOENT),
            }
        }
    }

    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        // Virtual directory
        if is_virtual_ino(ino) {
            let vi = self.virtual_inodes.lock().unwrap();
            if let Some(vpath_str) = vi.get_path(ino) {
                let vpath_str = vpath_str.to_string();
                drop(vi);
                return self.virtual_readdir(&vpath_str, ino, offset, reply);
            }
            return reply.error(libc::ENOENT);
        }

        // Root includes .deeplake
        let dir_path = if ino == ROOT_INO {
            "/".to_string()
        } else {
            match self.inodes.get_path(ino) {
                Ok(Some((p, true))) => p,
                _ => return reply.error(libc::ENOENT),
            }
        };

        let entries = match self.paths.list_dir(&dir_path) {
            Ok(e) => e,
            Err(_) => return reply.error(libc::EIO),
        };

        let mut full: Vec<(u64, FileType, String)> = Vec::new();
        full.push((ino, FileType::Directory, ".".to_string()));
        full.push((ino, FileType::Directory, "..".to_string()));

        // Inject .deeplake at root
        if ino == ROOT_INO {
            let mut vi = self.virtual_inodes.lock().unwrap();
            let dl_ino = vi.get_or_assign("/.deeplake");
            full.push((dl_ino, FileType::Directory, DEEPLAKE_DIR.to_string()));
        }

        for entry in entries {
            let child_path = if dir_path == "/" {
                format!("/{}", entry.name)
            } else {
                format!("{}/{}", dir_path, entry.name)
            };

            let ft = if entry.is_dir {
                FileType::Directory
            } else {
                FileType::RegularFile
            };

            let child_ino = match self.inodes.get_or_assign(&child_path, entry.is_dir) {
                Ok(i) => i,
                Err(_) => continue,
            };

            full.push((child_ino, ft, entry.name));
        }

        for (i, (child_ino, ft, name)) in full.iter().enumerate().skip(offset as usize) {
            if reply.add(*child_ino, (i + 1) as i64, *ft, name) {
                break;
            }
        }
        reply.ok();
    }

    fn read(
        &mut self,
        _req: &Request,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        // Virtual file
        if is_virtual_ino(ino) {
            let vi = self.virtual_inodes.lock().unwrap();
            if let Some(vpath_str) = vi.get_path(ino) {
                let vpath_str = vpath_str.to_string();
                drop(vi);
                return self.virtual_read(&vpath_str, offset, size, reply);
            }
            return reply.error(libc::ENOENT);
        }

        // Check write buffer first
        {
            let open = self.open_files.lock().unwrap();
            if let Some(buf) = open.get(&fh) {
                let start = offset as usize;
                let end = (start + size as usize).min(buf.data.len());
                if start >= buf.data.len() {
                    return reply.data(&[]);
                }
                return reply.data(&buf.data[start..end]);
            }
        }

        // Read from lake
        let (path, _) = match self.inodes.get_path(ino) {
            Ok(Some(p)) => p,
            _ => return reply.error(libc::ENOENT),
        };

        let head = match self.paths.get_head(&path) {
            Ok(h) => h,
            Err(_) => return reply.error(libc::ENOENT),
        };

        let data = match self.store.read_file(&head) {
            Ok(d) => d,
            Err(_) => return reply.error(libc::EIO),
        };

        let start = offset as usize;
        let end = (start + size as usize).min(data.len());
        if start >= data.len() {
            reply.data(&[]);
        } else {
            reply.data(&data[start..end]);
        }
    }

    fn open(&mut self, _req: &Request, ino: u64, flags: i32, reply: fuser::ReplyOpen) {
        // Virtual files are read-only
        if is_virtual_ino(ino) {
            let writable = (flags & libc::O_WRONLY != 0)
                || (flags & libc::O_RDWR != 0);
            if writable {
                return reply.error(libc::EROFS);
            }
            let fh = self.alloc_fh();
            return reply.opened(fh, 0);
        }

        let fh = self.alloc_fh();
        let writable = (flags & libc::O_WRONLY != 0)
            || (flags & libc::O_RDWR != 0)
            || (flags & libc::O_APPEND != 0);

        if writable {
            let (path, _) = match self.inodes.get_path(ino) {
                Ok(Some(p)) => p,
                _ => return reply.error(libc::ENOENT),
            };

            let data = match self.paths.get_head(&path) {
                Ok(head) => self.store.read_file(&head).unwrap_or_default(),
                Err(_) => Vec::new(),
            };

            let mut open = self.open_files.lock().unwrap();
            open.insert(
                fh,
                WriteBuffer {
                    path: PathBuf::from(&path),
                    data,
                    dirty: false,
                },
            );
        }

        reply.opened(fh, 0);
    }

    fn write(
        &mut self,
        _req: &Request,
        _ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        let mut open = self.open_files.lock().unwrap();
        let buf = match open.get_mut(&fh) {
            Some(b) => b,
            None => return reply.error(libc::EBADF),
        };

        let off = offset as usize;
        let end = off + data.len();

        if end > buf.data.len() {
            buf.data.resize(end, 0);
        }

        buf.data[off..end].copy_from_slice(data);
        buf.dirty = true;
        reply.written(data.len() as u32);
    }

    fn flush(
        &mut self,
        _req: &Request,
        _ino: u64,
        fh: u64,
        _lock_owner: u64,
        reply: fuser::ReplyEmpty,
    ) {
        let mut open = self.open_files.lock().unwrap();
        if let Some(buf) = open.get_mut(&fh) {
            if buf.dirty {
                let ingester = Ingester::new(&self.store);
                match ingester.ingest(&buf.data) {
                    Ok(manifest) => {
                        let _ = self.paths.put(
                            &buf.path,
                            &manifest.hash,
                            VersionTrigger::Close,
                            None,
                        );
                        buf.dirty = false;
                    }
                    Err(_) => return reply.error(libc::EIO),
                }
            }
        }
        reply.ok();
    }

    fn release(
        &mut self,
        _req: &Request,
        _ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: fuser::ReplyEmpty,
    ) {
        let buf = {
            let mut open = self.open_files.lock().unwrap();
            open.remove(&fh)
        };

        if let Some(buf) = buf {
            if buf.dirty {
                let ingester = Ingester::new(&self.store);
                if let Ok(manifest) = ingester.ingest(&buf.data) {
                    let _ = self.paths.put(
                        &buf.path,
                        &manifest.hash,
                        VersionTrigger::Close,
                        None,
                    );
                }
            }
        }
        reply.ok();
    }

    fn create(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        flags: i32,
        reply: ReplyCreate,
    ) {
        let path = match self.resolve_child(parent, name) {
            Ok(p) => p,
            Err(_) => return reply.error(libc::EIO),
        };

        // Can't create in virtual namespace
        if path.starts_with("/.deeplake") {
            return reply.error(libc::EROFS);
        }

        let ingester = Ingester::new(&self.store);
        let manifest = match ingester.ingest(&[]) {
            Ok(m) => m,
            Err(_) => return reply.error(libc::EIO),
        };

        if self
            .paths
            .put(&path, &manifest.hash, VersionTrigger::Close, Some("created"))
            .is_err()
        {
            return reply.error(libc::EIO);
        }

        let ino = match self.inodes.get_or_assign(&path, false) {
            Ok(i) => i,
            Err(_) => return reply.error(libc::EIO),
        };

        let fh = self.alloc_fh();
        let mut open = self.open_files.lock().unwrap();
        open.insert(
            fh,
            WriteBuffer {
                path: PathBuf::from(&path),
                data: Vec::new(),
                dirty: false,
            },
        );

        match self.file_attr(ino, &path) {
            Ok(attr) => reply.created(&TTL, &attr, 0, fh, 0),
            Err(_) => reply.error(libc::EIO),
        }
    }

    fn unlink(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: fuser::ReplyEmpty) {
        let path = match self.resolve_child(parent, name) {
            Ok(p) => p,
            Err(_) => return reply.error(libc::ENOENT),
        };

        if path.starts_with("/.deeplake") {
            return reply.error(libc::EROFS);
        }

        match self.paths.delete(&path) {
            Ok(()) => reply.ok(),
            Err(_) => reply.error(libc::ENOENT),
        }
    }

    fn rename(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        _flags: u32,
        reply: fuser::ReplyEmpty,
    ) {
        let old_path = match self.resolve_child(parent, name) {
            Ok(p) => p,
            Err(_) => return reply.error(libc::ENOENT),
        };
        let new_path = match self.resolve_child(newparent, newname) {
            Ok(p) => p,
            Err(_) => return reply.error(libc::EIO),
        };

        if old_path.starts_with("/.deeplake") || new_path.starts_with("/.deeplake") {
            return reply.error(libc::EROFS);
        }

        if self.paths.rename(&old_path, &new_path).is_err() {
            return reply.error(libc::ENOENT);
        }

        let _ = self.inodes.rename(&old_path, &new_path);
        reply.ok();
    }

    fn setattr(
        &mut self,
        _req: &Request,
        ino: u64,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<fuser::TimeOrNow>,
        _mtime: Option<fuser::TimeOrNow>,
        _ctime: Option<SystemTime>,
        fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        if let Some(new_size) = size {
            if let Some(fh) = fh {
                let mut open = self.open_files.lock().unwrap();
                if let Some(buf) = open.get_mut(&fh) {
                    buf.data.truncate(new_size as usize);
                    buf.data.resize(new_size as usize, 0);
                    buf.dirty = true;
                }
            }
        }

        if ino == ROOT_INO {
            return reply.attr(&TTL, &self.dir_attr(ROOT_INO));
        }

        if is_virtual_ino(ino) {
            return reply.attr(&TTL, &self.dir_attr(ino));
        }

        let (path, is_dir) = match self.inodes.get_path(ino) {
            Ok(Some(p)) => p,
            _ => return reply.error(libc::ENOENT),
        };

        if is_dir {
            reply.attr(&TTL, &self.dir_attr(ino));
        } else {
            match self.file_attr(ino, &path) {
                Ok(attr) => reply.attr(&TTL, &attr),
                Err(_) => reply.error(libc::EIO),
            }
        }
    }
}

// ─── Helpers ─────────────────────────────────────────────────────

fn format_stats(
    stats: &lake_core::history::StorageStats,
    file_path: &str,
) -> String {
    format!(
        "Storage Stats: {}\n\
         ─────────────────────────────\n\
         Versions:        {}\n\
         Unique chunks:   {}\n\
         Chunk refs:      {}\n\
         Naive storage:   {}\n\
         Actual storage:  {}\n\
         Dedup ratio:     {:.1}%\n\
         Space saved:     {}\n",
        file_path,
        stats.version_count,
        stats.unique_chunks,
        stats.total_chunk_refs,
        format_bytes(stats.naive_total_bytes),
        format_bytes(stats.actual_bytes),
        stats.dedup_ratio() * 100.0,
        format_bytes(stats.naive_total_bytes.saturating_sub(stats.actual_bytes)),
    )
}

fn format_bytes(b: u64) -> String {
    if b < 1024 {
        format!("{} B", b)
    } else if b < 1024 * 1024 {
        format!("{:.1} KB", b as f64 / 1024.0)
    } else if b < 1024 * 1024 * 1024 {
        format!("{:.1} MB", b as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.2} GB", b as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}

/// Convert days since Unix epoch to YYYY-MM-DD string.
/// Simple civil date calculation — no timezone handling for prototype.
fn epoch_days_to_date(days: u64) -> String {
    // Algorithm from http://howardhinnant.github.io/date_algorithms.html
    let z = days + 719468;
    let era = z / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{:04}-{:02}-{:02}", y, m, d)
}

// ─── Main ────────────────────────────────────────────────────────

fn main() -> io::Result<()> {
    let lake_dir = std::env::args()
        .nth(1)
        .unwrap_or_else(|| ".lake".to_string());
    let mount_point = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "mnt".to_string());

    std::fs::create_dir_all(&mount_point)?;

    let store = ObjectStore::open(format!("{}/objects", lake_dir))?;
    let paths = PathTable::open(format!("{}/paths.db", lake_dir))
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    let inodes = InodeStore::open(format!("{}/inodes.db", lake_dir))?;

    let fs = LakeFS::new(store, paths, inodes);

    println!("DeepLake mounted at {}", mount_point);
    println!("Lake storage at {}", lake_dir);
    println!("  .deeplake/history/   — browse version history");
    println!("  .deeplake/timeline/  — versions by date");
    println!("  .deeplake/stats/     — storage statistics");
    println!("  .deeplake/deleted/   — soft-deleted files");
    println!("Ctrl+C to unmount");

    fuser::mount2(
        fs,
        &mount_point,
        &[
            MountOption::FSName("deeplake".to_string()),
            MountOption::AutoUnmount,
            MountOption::AllowOther,
        ],
    )?;

    Ok(())
}
