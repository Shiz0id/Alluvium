/*
 * lake.h — C API for lake-core (Alluvium / Varve FS)
 *
 * This is the cross-platform interface to the content-addressed
 * hash lake. It powers filesystem frontends (FUSE, WinFSP) and
 * language bindings without exposing any Rust types.
 *
 * Conventions:
 *   - All functions return int error codes (LAKE_OK = 0 on success)
 *   - Opaque LakeHandle* must be opened with lake_open(), freed with lake_close()
 *   - Hash strings are 64-char hex + null terminator (65 bytes)
 *   - Byte buffers from read functions must be freed with lake_bytes_free()
 *   - String fields in structs must be freed with lake_string_free()
 *   - Struct arrays must be freed with their corresponding _free() function
 */

#ifndef LAKE_H
#define LAKE_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ── Error Codes ────────────────────────────────────────────── */

#define LAKE_OK              0
#define LAKE_ERR_NULL_PTR   -1
#define LAKE_ERR_INVALID_UTF8 -2
#define LAKE_ERR_STORE      -3
#define LAKE_ERR_PATH_TABLE -4
#define LAKE_ERR_NOT_FOUND  -5
#define LAKE_ERR_DELETED    -6
#define LAKE_ERR_HASH_PARSE -7
#define LAKE_ERR_HISTORY    -8

/* ── Opaque Handle ──────────────────────────────────────────── */

typedef struct LakeHandle LakeHandle;

/* ── Data Structures ────────────────────────────────────────── */

typedef struct {
    char*    manifest_hash;   /* hex string — free with lake_string_free */
    uint64_t version_num;
    uint64_t timestamp;       /* unix epoch millis */
    char*    trigger;         /* "close", "explicit", or "micro_hash" */
    char*    message;         /* nullable */
} LakeVersion;

typedef struct {
    int      op_type;         /* 0 = keep, 1 = delete, 2 = insert */
    char*    hash;            /* hex string — free with lake_string_free */
    uint64_t offset_a;
    uint64_t offset_b;
    uint64_t size;
} LakeDiffOp;

typedef struct {
    LakeDiffOp* ops;
    size_t      ops_count;
    size_t      chunks_kept;
    size_t      chunks_deleted;
    size_t      chunks_inserted;
    uint64_t    bytes_kept;
    uint64_t    bytes_deleted;
    uint64_t    bytes_inserted;
} LakeDiffResult;

typedef struct {
    size_t   version_count;
    uint64_t naive_total_bytes;
    uint64_t actual_bytes;
    size_t   unique_chunks;
    size_t   total_chunk_refs;
} LakeStorageStats;

/* ── Lifecycle ──────────────────────────────────────────────── */

/**
 * Open or create a lake at the given directory path.
 * Returns an opaque handle, or NULL on failure.
 * Must be freed with lake_close().
 */
LakeHandle* lake_open(const char* lake_dir);

/**
 * Close a lake handle and free all resources.
 */
void lake_close(LakeHandle* handle);

/* ── Ingest ─────────────────────────────────────────────────── */

/**
 * Ingest raw bytes into the lake.
 *
 * Chunks the data via FastCDC, stores each chunk, builds and
 * stores a manifest. On success, writes the manifest hash as
 * a 64-char hex string into manifest_hash_out.
 *
 * @param handle          Lake handle from lake_open()
 * @param data            Pointer to raw bytes
 * @param data_len        Number of bytes
 * @param manifest_hash_out  Output buffer, at least 65 bytes
 * @return LAKE_OK on success
 */
int lake_ingest(const LakeHandle* handle,
                const uint8_t* data, size_t data_len,
                char* manifest_hash_out);

/* ── Read ───────────────────────────────────────────────────── */

/**
 * Read a complete file from the lake by manifest hash.
 *
 * On success, allocates a buffer and sets *data_out and *len_out.
 * Caller must free with lake_bytes_free(*data_out, *len_out).
 */
int lake_read_file(const LakeHandle* handle,
                   const char* manifest_hash,
                   uint8_t** data_out, size_t* len_out);

/**
 * Read file content at a specific version number.
 * Caller must free with lake_bytes_free().
 */
int lake_read_version(const LakeHandle* handle,
                      const char* path, uint64_t version_num,
                      uint8_t** data_out, size_t* len_out);

/* ── Path Table ─────────────────────────────────────────────── */

/**
 * Register a new version of a file.
 *
 * @param trigger  "close", "explicit", or "micro_hash"
 * @param message  Optional commit message (NULL for none)
 */
int lake_put(const LakeHandle* handle,
             const char* path, const char* manifest_hash,
             const char* trigger, const char* message);

/**
 * Get the HEAD manifest hash for a path.
 * hash_out must be at least 65 bytes.
 *
 * Returns LAKE_ERR_NOT_FOUND or LAKE_ERR_DELETED if applicable.
 */
int lake_get_head(const LakeHandle* handle,
                  const char* path, char* hash_out);

/**
 * Soft-delete a file. History is preserved.
 */
int lake_delete(const LakeHandle* handle, const char* path);

/**
 * Restore a soft-deleted file.
 */
int lake_restore(const LakeHandle* handle, const char* path);

/**
 * Rename/move a file. Metadata-only, instant regardless of size.
 */
int lake_rename(const LakeHandle* handle,
                const char* old_path, const char* new_path);

/* ── History ────────────────────────────────────────────────── */

/**
 * Get version history for a file, newest first.
 * Caller must free with lake_versions_free().
 */
int lake_history_list(const LakeHandle* handle,
                      const char* path,
                      LakeVersion** versions_out, size_t* count_out);

/**
 * Rewind a file to a previous version. Non-destructive.
 */
int lake_rewind(const LakeHandle* handle,
                const char* path, uint64_t version_num);

/* ── Diff ───────────────────────────────────────────────────── */

/**
 * Diff two versions of a file by version number.
 * Caller must free with lake_diff_free().
 */
int lake_diff(const LakeHandle* handle,
              const char* path, uint64_t v1, uint64_t v2,
              LakeDiffResult* result_out);

/* ── Stats ──────────────────────────────────────────────────── */

/**
 * Get storage statistics for a file's version history.
 */
int lake_storage_stats(const LakeHandle* handle,
                       const char* path,
                       LakeStorageStats* stats_out);

/* ── Directory Listing ───────────────────────────────────────── */

typedef struct {
    char* name;               /* entry name — free with lake_string_free */
    int   is_dir;             /* 0 = file, 1 = directory */
} LakeDirEntry;

/**
 * List immediate children of a directory.
 * Caller must free with lake_dir_entries_free().
 */
int lake_list_dir(const LakeHandle* handle,
                  const char* path,
                  LakeDirEntry** entries_out, size_t* count_out);

/**
 * List all soft-deleted file paths.
 * Caller must free each string with lake_string_free(),
 * then the array with lake_string_array_free().
 */
int lake_list_deleted(const LakeHandle* handle,
                      char*** paths_out, size_t* count_out);

/* ── Free Functions ─────────────────────────────────────────── */

void lake_bytes_free(uint8_t* data, size_t len);
void lake_string_free(char* s);
void lake_versions_free(LakeVersion* versions, size_t count);
void lake_diff_free(LakeDiffResult* result);
void lake_dir_entries_free(LakeDirEntry* entries, size_t count);
void lake_string_array_free(char** arr, size_t count);

#ifdef __cplusplus
}
#endif

#endif /* LAKE_H */
