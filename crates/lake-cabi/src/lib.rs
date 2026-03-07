//! C-compatible API for lake-core.

use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::ptr;
use std::slice;
use std::sync::{Arc, Mutex};

use lake_core::history::History;
use lake_core::ingest::Ingester;
use lake_core::path_table::PathTable;
use lake_core::reader::{ChunkIndex, FileReader};
use lake_core::store::ObjectStore;
use lake_core::types::{B3Hash, VersionTrigger};

// ─── Error Codes ─────────────────────────────────────────────────

pub const LAKE_OK: i32 = 0;
pub const LAKE_ERR_NULL_PTR: i32 = -1;
pub const LAKE_ERR_INVALID_UTF8: i32 = -2;
pub const LAKE_ERR_STORE: i32 = -3;
pub const LAKE_ERR_PATH_TABLE: i32 = -4;
pub const LAKE_ERR_NOT_FOUND: i32 = -5;
pub const LAKE_ERR_DELETED: i32 = -6;
pub const LAKE_ERR_HASH_PARSE: i32 = -7;
pub const LAKE_ERR_HISTORY: i32 = -8;

// ─── Opaque Handle Types ─────────────────────────────────────────

pub struct LakeHandle {
    store: Arc<ObjectStore>,
    paths: PathTable,
    reader: Mutex<FileReader>,
}

/// A version record in C-friendly form.
#[repr(C)]
pub struct LakeVersion {
    pub manifest_hash: *mut c_char,
    pub version_num: u64,
    pub timestamp: u64,
    pub trigger: *mut c_char,
    pub message: *mut c_char,
}

/// A diff operation in C-friendly form.
#[repr(C)]
pub struct LakeDiffOp {
    pub op_type: i32,
    pub hash: *mut c_char,
    pub offset_a: u64,
    pub offset_b: u64,
    pub size: u64,
}

/// Diff result in C-friendly form.
#[repr(C)]
pub struct LakeDiffResult {
    pub ops: *mut LakeDiffOp,
    pub ops_count: usize,
    pub chunks_kept: usize,
    pub chunks_deleted: usize,
    pub chunks_inserted: usize,
    pub bytes_kept: u64,
    pub bytes_deleted: u64,
    pub bytes_inserted: u64,
}

/// Storage stats in C-friendly form.
#[repr(C)]
pub struct LakeStorageStats {
    pub version_count: usize,
    pub naive_total_bytes: u64,
    pub actual_bytes: u64,
    pub unique_chunks: usize,
    pub total_chunk_refs: usize,
}

/// A directory entry in C-friendly form.
#[repr(C)]
pub struct LakeDirEntry {
    pub name: *mut c_char,
    pub is_dir: i32,
}

// ─── Helper Macros ───────────────────────────────────────────────

macro_rules! cstr_to_str {
    ($ptr:expr, $err_ret:expr) => {{
        if $ptr.is_null() {
            return $err_ret;
        }
        match unsafe { CStr::from_ptr($ptr) }.to_str() {
            Ok(s) => s,
            Err(_) => return LAKE_ERR_INVALID_UTF8,
        }
    }};
}

macro_rules! handle_ref {
    ($ptr:expr, $err_ret:expr) => {{
        if $ptr.is_null() {
            return $err_ret;
        }
        unsafe { &*$ptr }
    }};
}

fn to_c_string(s: &str) -> *mut c_char {
    CString::new(s)
        .map(|cs| cs.into_raw())
        .unwrap_or(ptr::null_mut())
}

fn to_c_string_opt(s: &Option<String>) -> *mut c_char {
    match s {
        Some(ref val) => to_c_string(val),
        None => ptr::null_mut(),
    }
}

// ─── Lifecycle ───────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn lake_open(lake_dir: *const c_char) -> *mut LakeHandle {
    let dir = match unsafe { CStr::from_ptr(lake_dir) }.to_str() {
        Ok(s) => s,
        Err(_) => return ptr::null_mut(),
    };

    let store = match ObjectStore::open(format!("{}/objects", dir)) {
        Ok(s) => Arc::new(s),
        Err(_) => return ptr::null_mut(),
    };

    let paths = match PathTable::open(format!("{}/paths.db", dir)) {
        Ok(p) => p,
        Err(_) => return ptr::null_mut(),
    };

    let reader = Mutex::new(FileReader::new(store.clone(), 64));

    Box::into_raw(Box::new(LakeHandle { store, paths, reader }))
}

#[no_mangle]
pub extern "C" fn lake_close(handle: *mut LakeHandle) {
    if !handle.is_null() {
        unsafe { drop(Box::from_raw(handle)); }
    }
}

// ─── Ingest ──────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn lake_ingest(
    handle: *const LakeHandle,
    data: *const u8,
    data_len: usize,
    manifest_hash_out: *mut c_char,
) -> i32 {
    let h = handle_ref!(handle, LAKE_ERR_NULL_PTR);
    if data.is_null() && data_len > 0 {
        return LAKE_ERR_NULL_PTR;
    }
    if manifest_hash_out.is_null() {
        return LAKE_ERR_NULL_PTR;
    }

    let bytes = if data_len == 0 {
        &[]
    } else {
        unsafe { slice::from_raw_parts(data, data_len) }
    };

    let ingester = Ingester::new(&h.store);
    let manifest = match ingester.ingest(bytes) {
        Ok(m) => m,
        Err(_) => return LAKE_ERR_STORE,
    };

    let hex = manifest.hash.to_hex();
    let hex_bytes = hex.as_bytes();
    unsafe {
        ptr::copy_nonoverlapping(hex_bytes.as_ptr(), manifest_hash_out as *mut u8, 64);
        *manifest_hash_out.add(64) = 0;
    }

    LAKE_OK
}

// ─── Read ────────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn lake_read_file(
    handle: *const LakeHandle,
    manifest_hash: *const c_char,
    data_out: *mut *mut u8,
    len_out: *mut usize,
) -> i32 {
    let h = handle_ref!(handle, LAKE_ERR_NULL_PTR);
    let hex = cstr_to_str!(manifest_hash, LAKE_ERR_NULL_PTR);

    let hash = match B3Hash::from_hex(hex) {
        Ok(h) => h,
        Err(_) => return LAKE_ERR_HASH_PARSE,
    };

    let data = match h.store.read_file(&hash) {
        Ok(d) => d,
        Err(_) => return LAKE_ERR_STORE,
    };

    let len = data.len();
    let buf = data.into_boxed_slice();
    let ptr = Box::into_raw(buf) as *mut u8;

    unsafe {
        *data_out = ptr;
        *len_out = len;
    }

    LAKE_OK
}

#[no_mangle]
pub extern "C" fn lake_bytes_free(data: *mut u8, len: usize) {
    if !data.is_null() {
        unsafe { drop(Box::from_raw(slice::from_raw_parts_mut(data, len))); }
    }
}

#[no_mangle]
pub extern "C" fn lake_string_free(s: *mut c_char) {
    if !s.is_null() {
        unsafe { drop(CString::from_raw(s)); }
    }
}

// ─── Path Table Operations ───────────────────────────────────────

#[no_mangle]
pub extern "C" fn lake_put(
    handle: *const LakeHandle,
    path: *const c_char,
    manifest_hash: *const c_char,
    trigger: *const c_char,
    message: *const c_char,
) -> i32 {
    let h = handle_ref!(handle, LAKE_ERR_NULL_PTR);
    let path_str = cstr_to_str!(path, LAKE_ERR_NULL_PTR);
    let hash_hex = cstr_to_str!(manifest_hash, LAKE_ERR_NULL_PTR);
    let trigger_str = cstr_to_str!(trigger, LAKE_ERR_NULL_PTR);

    let hash = match B3Hash::from_hex(hash_hex) {
        Ok(h) => h,
        Err(_) => return LAKE_ERR_HASH_PARSE,
    };

    let trig = match trigger_str {
        "close" => VersionTrigger::Close,
        "explicit" => VersionTrigger::Explicit,
        "micro_hash" => VersionTrigger::MicroHash,
        _ => VersionTrigger::Close,
    };

    let msg = if message.is_null() {
        None
    } else {
        match unsafe { CStr::from_ptr(message) }.to_str() {
            Ok(s) => Some(s),
            Err(_) => None,
        }
    };

    match h.paths.put(path_str, &hash, trig, msg) {
        Ok(()) => LAKE_OK,
        Err(_) => LAKE_ERR_PATH_TABLE,
    }
}

#[no_mangle]
pub extern "C" fn lake_get_head(
    handle: *const LakeHandle,
    path: *const c_char,
    hash_out: *mut c_char,
) -> i32 {
    let h = handle_ref!(handle, LAKE_ERR_NULL_PTR);
    let path_str = cstr_to_str!(path, LAKE_ERR_NULL_PTR);
    if hash_out.is_null() {
        return LAKE_ERR_NULL_PTR;
    }

    let hash = match h.paths.get_head(path_str) {
        Ok(h) => h,
        Err(lake_core::path_table::PathTableError::NotFound(_)) => return LAKE_ERR_NOT_FOUND,
        Err(lake_core::path_table::PathTableError::Deleted(_)) => return LAKE_ERR_DELETED,
        Err(_) => return LAKE_ERR_PATH_TABLE,
    };

    let hex = hash.to_hex();
    unsafe {
        ptr::copy_nonoverlapping(hex.as_bytes().as_ptr(), hash_out as *mut u8, 64);
        *hash_out.add(64) = 0;
    }

    LAKE_OK
}

#[no_mangle]
pub extern "C" fn lake_delete(
    handle: *const LakeHandle,
    path: *const c_char,
) -> i32 {
    let h = handle_ref!(handle, LAKE_ERR_NULL_PTR);
    let path_str = cstr_to_str!(path, LAKE_ERR_NULL_PTR);
    match h.paths.delete(path_str) {
        Ok(()) => LAKE_OK,
        Err(_) => LAKE_ERR_NOT_FOUND,
    }
}

#[no_mangle]
pub extern "C" fn lake_restore(
    handle: *const LakeHandle,
    path: *const c_char,
) -> i32 {
    let h = handle_ref!(handle, LAKE_ERR_NULL_PTR);
    let path_str = cstr_to_str!(path, LAKE_ERR_NULL_PTR);
    match h.paths.restore(path_str) {
        Ok(()) => LAKE_OK,
        Err(_) => LAKE_ERR_NOT_FOUND,
    }
}

#[no_mangle]
pub extern "C" fn lake_rename(
    handle: *const LakeHandle,
    old_path: *const c_char,
    new_path: *const c_char,
) -> i32 {
    let h = handle_ref!(handle, LAKE_ERR_NULL_PTR);
    let old = cstr_to_str!(old_path, LAKE_ERR_NULL_PTR);
    let new = cstr_to_str!(new_path, LAKE_ERR_NULL_PTR);
    match h.paths.rename(old, new) {
        Ok(()) => LAKE_OK,
        Err(_) => LAKE_ERR_NOT_FOUND,
    }
}

// ─── History Operations ──────────────────────────────────────────

#[no_mangle]
pub extern "C" fn lake_history_list(
    handle: *const LakeHandle,
    path: *const c_char,
    versions_out: *mut *mut LakeVersion,
    count_out: *mut usize,
) -> i32 {
    let h = handle_ref!(handle, LAKE_ERR_NULL_PTR);
    let path_str = cstr_to_str!(path, LAKE_ERR_NULL_PTR);
    if versions_out.is_null() || count_out.is_null() {
        return LAKE_ERR_NULL_PTR;
    }

    let history = History::new(&h.store, &h.paths);
    let versions = match history.list(path_str) {
        Ok(v) => v,
        Err(_) => return LAKE_ERR_HISTORY,
    };

    let count = versions.len();
    let mut c_versions: Vec<LakeVersion> = versions
        .iter()
        .map(|v| {
            let trigger_str = match v.trigger {
                VersionTrigger::Close => "close",
                VersionTrigger::Explicit => "explicit",
                VersionTrigger::MicroHash => "micro_hash",
            };
            LakeVersion {
                manifest_hash: to_c_string(&v.manifest_hash.to_hex()),
                version_num: v.version_num,
                timestamp: v.timestamp,
                trigger: to_c_string(trigger_str),
                message: to_c_string_opt(&v.message),
            }
        })
        .collect();

    let ptr = c_versions.as_mut_ptr();
    std::mem::forget(c_versions);

    unsafe {
        *versions_out = ptr;
        *count_out = count;
    }

    LAKE_OK
}

#[no_mangle]
pub extern "C" fn lake_versions_free(versions: *mut LakeVersion, count: usize) {
    if versions.is_null() { return; }
    unsafe {
        let slice = slice::from_raw_parts_mut(versions, count);
        for v in slice.iter_mut() {
            lake_string_free(v.manifest_hash);
            lake_string_free(v.trigger);
            if !v.message.is_null() {
                lake_string_free(v.message);
            }
        }
        drop(Vec::from_raw_parts(versions, count, count));
    }
}

#[no_mangle]
pub extern "C" fn lake_read_version(
    handle: *const LakeHandle,
    path: *const c_char,
    version_num: u64,
    data_out: *mut *mut u8,
    len_out: *mut usize,
) -> i32 {
    let h = handle_ref!(handle, LAKE_ERR_NULL_PTR);
    let path_str = cstr_to_str!(path, LAKE_ERR_NULL_PTR);
    if data_out.is_null() || len_out.is_null() {
        return LAKE_ERR_NULL_PTR;
    }

    let history = History::new(&h.store, &h.paths);
    let data = match history.read_at_version(path_str, version_num) {
        Ok(d) => d,
        Err(_) => return LAKE_ERR_NOT_FOUND,
    };

    let len = data.len();
    let buf = data.into_boxed_slice();
    let ptr = Box::into_raw(buf) as *mut u8;

    unsafe {
        *data_out = ptr;
        *len_out = len;
    }

    LAKE_OK
}

#[no_mangle]
pub extern "C" fn lake_rewind(
    handle: *const LakeHandle,
    path: *const c_char,
    version_num: u64,
) -> i32 {
    let h = handle_ref!(handle, LAKE_ERR_NULL_PTR);
    let path_str = cstr_to_str!(path, LAKE_ERR_NULL_PTR);

    let history = History::new(&h.store, &h.paths);
    match history.rewind(path_str, version_num) {
        Ok(()) => LAKE_OK,
        Err(_) => LAKE_ERR_NOT_FOUND,
    }
}

// ─── Diff ────────────────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn lake_diff(
    handle: *const LakeHandle,
    path: *const c_char,
    v1: u64,
    v2: u64,
    result_out: *mut LakeDiffResult,
) -> i32 {
    let h = handle_ref!(handle, LAKE_ERR_NULL_PTR);
    let path_str = cstr_to_str!(path, LAKE_ERR_NULL_PTR);
    if result_out.is_null() {
        return LAKE_ERR_NULL_PTR;
    }

    let history = History::new(&h.store, &h.paths);
    let diff = match history.diff_versions(path_str, v1, v2) {
        Ok(d) => d,
        Err(_) => return LAKE_ERR_HISTORY,
    };

    let mut c_ops: Vec<LakeDiffOp> = diff
        .ops
        .iter()
        .map(|op| match op {
            lake_core::diff::DiffOp::Keep { hash, offset_a, offset_b, size } => {
                LakeDiffOp {
                    op_type: 0,
                    hash: to_c_string(&hash.to_hex()),
                    offset_a: *offset_a,
                    offset_b: *offset_b,
                    size: *size,
                }
            }
            lake_core::diff::DiffOp::Delete { hash, offset_a, size } => {
                LakeDiffOp {
                    op_type: 1,
                    hash: to_c_string(&hash.to_hex()),
                    offset_a: *offset_a,
                    offset_b: 0,
                    size: *size,
                }
            }
            lake_core::diff::DiffOp::Insert { hash, offset_b, size } => {
                LakeDiffOp {
                    op_type: 2,
                    hash: to_c_string(&hash.to_hex()),
                    offset_a: 0,
                    offset_b: *offset_b,
                    size: *size,
                }
            }
        })
        .collect();

    let ops_count = c_ops.len();
    let ops_ptr = c_ops.as_mut_ptr();
    std::mem::forget(c_ops);

    unsafe {
        *result_out = LakeDiffResult {
            ops: ops_ptr,
            ops_count,
            chunks_kept: diff.summary.chunks_kept,
            chunks_deleted: diff.summary.chunks_deleted,
            chunks_inserted: diff.summary.chunks_inserted,
            bytes_kept: diff.summary.bytes_kept,
            bytes_deleted: diff.summary.bytes_deleted,
            bytes_inserted: diff.summary.bytes_inserted,
        };
    }

    LAKE_OK
}

#[no_mangle]
pub extern "C" fn lake_diff_free(result: *mut LakeDiffResult) {
    if result.is_null() { return; }
    unsafe {
        let r = &*result;
        if !r.ops.is_null() {
            let slice = slice::from_raw_parts_mut(r.ops, r.ops_count);
            for op in slice.iter_mut() {
                lake_string_free(op.hash);
            }
            drop(Vec::from_raw_parts(r.ops, r.ops_count, r.ops_count));
        }
    }
}

// ─── Ranged Read ─────────────────────────────────────────────────

/// Get the total file size for a path without reading any chunk data.
#[no_mangle]
pub extern "C" fn lake_get_file_size(
    handle: *const LakeHandle,
    path: *const c_char,
    size_out: *mut u64,
) -> i32 {
    let h = handle_ref!(handle, LAKE_ERR_NULL_PTR);
    let path_str = cstr_to_str!(path, LAKE_ERR_NULL_PTR);
    if size_out.is_null() {
        return LAKE_ERR_NULL_PTR;
    }

    let hash = match h.paths.get_head(path_str) {
        Ok(h) => h,
        Err(lake_core::path_table::PathTableError::NotFound(_)) => return LAKE_ERR_NOT_FOUND,
        Err(lake_core::path_table::PathTableError::Deleted(_)) => return LAKE_ERR_DELETED,
        Err(_) => return LAKE_ERR_PATH_TABLE,
    };

    let manifest = match h.store.get_manifest(&hash) {
        Ok(m) => m,
        Err(_) => return LAKE_ERR_STORE,
    };

    unsafe { *size_out = manifest.total_size; }
    LAKE_OK
}

/// Read a byte range from a file. Only fetches the chunks that overlap
/// the requested range. Uses an LRU cache for sequential read patterns.
#[no_mangle]
pub extern "C" fn lake_read_range(
    handle: *const LakeHandle,
    path: *const c_char,
    offset: u64,
    size: u32,
    data_out: *mut *mut u8,
    len_out: *mut usize,
) -> i32 {
    let h = handle_ref!(handle, LAKE_ERR_NULL_PTR);
    let path_str = cstr_to_str!(path, LAKE_ERR_NULL_PTR);
    if data_out.is_null() || len_out.is_null() {
        return LAKE_ERR_NULL_PTR;
    }

    let hash = match h.paths.get_head(path_str) {
        Ok(h) => h,
        Err(_) => return LAKE_ERR_NOT_FOUND,
    };

    let manifest = match h.store.get_manifest(&hash) {
        Ok(m) => m,
        Err(_) => return LAKE_ERR_STORE,
    };

    let index = ChunkIndex::from_manifest(&manifest);
    let mut reader = match h.reader.lock() {
        Ok(r) => r,
        Err(_) => return LAKE_ERR_STORE,
    };

    let data: Vec<u8> = match reader.read(&index, offset, size) {
        Ok(d) => d,
        Err(_) => return LAKE_ERR_STORE,
    };

    let len = data.len();
    let buf = data.into_boxed_slice();
    let ptr = Box::into_raw(buf) as *mut u8;

    unsafe {
        *data_out = ptr;
        *len_out = len;
    }

    LAKE_OK
}

// ─── Directory Listing ───────────────────────────────────────────

#[no_mangle]
pub extern "C" fn lake_list_dir(
    handle: *const LakeHandle,
    path: *const c_char,
    entries_out: *mut *mut LakeDirEntry,
    count_out: *mut usize,
) -> i32 {
    let h = handle_ref!(handle, LAKE_ERR_NULL_PTR);
    let path_str = cstr_to_str!(path, LAKE_ERR_NULL_PTR);
    if entries_out.is_null() || count_out.is_null() {
        return LAKE_ERR_NULL_PTR;
    }

    let entries = match h.paths.list_dir(path_str) {
        Ok(e) => e,
        Err(_) => return LAKE_ERR_PATH_TABLE,
    };

    let count = entries.len();
    let mut c_entries: Vec<LakeDirEntry> = entries
        .iter()
        .map(|e| LakeDirEntry {
            name: to_c_string(&e.name),
            is_dir: if e.is_dir { 1 } else { 0 },
        })
        .collect();

    let ptr = c_entries.as_mut_ptr();
    std::mem::forget(c_entries);

    unsafe {
        *entries_out = ptr;
        *count_out = count;
    }

    LAKE_OK
}

#[no_mangle]
pub extern "C" fn lake_dir_entries_free(entries: *mut LakeDirEntry, count: usize) {
    if entries.is_null() { return; }
    unsafe {
        let slice = slice::from_raw_parts_mut(entries, count);
        for e in slice.iter_mut() {
            lake_string_free(e.name);
        }
        drop(Vec::from_raw_parts(entries, count, count));
    }
}

#[no_mangle]
pub extern "C" fn lake_list_deleted(
    handle: *const LakeHandle,
    paths_out: *mut *mut *mut c_char,
    count_out: *mut usize,
) -> i32 {
    let h = handle_ref!(handle, LAKE_ERR_NULL_PTR);
    if paths_out.is_null() || count_out.is_null() {
        return LAKE_ERR_NULL_PTR;
    }

    let deleted = match h.paths.list_deleted() {
        Ok(d) => d,
        Err(_) => return LAKE_ERR_PATH_TABLE,
    };

    let count = deleted.len();
    let mut c_paths: Vec<*mut c_char> = deleted
        .iter()
        .map(|p| to_c_string(p))
        .collect();

    let ptr = c_paths.as_mut_ptr();
    std::mem::forget(c_paths);

    unsafe {
        *paths_out = ptr;
        *count_out = count;
    }

    LAKE_OK
}

#[no_mangle]
pub extern "C" fn lake_string_array_free(arr: *mut *mut c_char, count: usize) {
    if arr.is_null() { return; }
    unsafe {
        let slice = slice::from_raw_parts_mut(arr, count);
        for s in slice.iter_mut() {
            lake_string_free(*s);
        }
        drop(Vec::from_raw_parts(arr, count, count));
    }
}

// ─── Storage Stats ───────────────────────────────────────────────

#[no_mangle]
pub extern "C" fn lake_storage_stats(
    handle: *const LakeHandle,
    path: *const c_char,
    stats_out: *mut LakeStorageStats,
) -> i32 {
    let h = handle_ref!(handle, LAKE_ERR_NULL_PTR);
    let path_str = cstr_to_str!(path, LAKE_ERR_NULL_PTR);
    if stats_out.is_null() {
        return LAKE_ERR_NULL_PTR;
    }

    let history = History::new(&h.store, &h.paths);
    let stats = match history.storage_stats(path_str) {
        Ok(s) => s,
        Err(_) => return LAKE_ERR_HISTORY,
    };

    unsafe {
        *stats_out = LakeStorageStats {
            version_count: stats.version_count,
            naive_total_bytes: stats.naive_total_bytes,
            actual_bytes: stats.actual_bytes,
            unique_chunks: stats.unique_chunks,
            total_chunk_refs: stats.total_chunk_refs,
        };
    }

    LAKE_OK
}
