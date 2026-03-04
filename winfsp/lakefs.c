/*
 * lakefs.c — WinFSP frontend for the DeepLake content-addressed filesystem.
 *
 * This file translates Windows filesystem operations into lake-core calls
 * via the C API (lake.h). It contains no lake logic — all intelligence
 * lives in lake-core behind the shared library.
 *
 * Build:
 *   cl lakefs.c /I path\to\winfsp\inc /I path\to\lake-cabi\include
 *      /link path\to\winfsp\lib\winfsp-x64.lib path\to\lake_cabi.dll.lib
 *
 * Run:
 *   lakefs.exe -m X: -l .lake
 *   where X: is the drive letter and .lake is the storage directory.
 */

#include <winfsp/winfsp.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include "lake.h"

/* ── Configuration ──────────────────────────────────────────── */

#define LAKE_ALLOC_TAG 'ekaL'
#define ALLUVIUM_PREFIX L"\\__alluvium__"
#define ALLUVIUM_PREFIX_LEN 14
#define HASH_HEX_BUF 65    /* 64 hex chars + null */
#define MAX_PATH_CHARS 512

/* ── File Context ───────────────────────────────────────────── */

/*
 * Attached to every open file handle. Tracks whether the file is
 * being written to and holds the write buffer for ingest-on-close.
 */
typedef struct {
    char     lake_path[MAX_PATH_CHARS]; /* UTF-8 lake path */
    uint8_t* write_buf;                 /* NULL if read-only */
    size_t   write_len;
    size_t   write_cap;
    int      dirty;
    int      is_dir;
    int      is_virtual;                /* inside __alluvium__ */
} LAKE_FILE_CONTEXT;

/* ── Filesystem Instance ────────────────────────────────────── */

typedef struct {
    FSP_FILE_SYSTEM* fsp;
    LakeHandle*      lake;
} LAKE_FS;

/* ── Helpers ────────────────────────────────────────────────── */

/*
 * Convert a Windows wide-char path (backslash-separated, rooted at \)
 * to a UTF-8 lake path (forward-slash-separated, rooted at /).
 */
static int to_lake_path(const WCHAR* win_path, char* out, size_t out_size) {
    /* Convert UTF-16 to UTF-8 */
    int len = WideCharToMultiByte(CP_UTF8, 0, win_path, -1,
                                   out, (int)out_size, NULL, NULL);
    if (len <= 0) return -1;

    /* Backslash to forward slash */
    for (int i = 0; i < len; i++) {
        if (out[i] == '\\') out[i] = '/';
    }

    return 0;
}

/*
 * Check if a wide path starts with the __alluvium__ prefix.
 */
static int is_alluvium_path(const WCHAR* path) {
    return wcsncmp(path, ALLUVIUM_PREFIX, ALLUVIUM_PREFIX_LEN) == 0;
}

/*
 * Fill out a FSP_FSCTL_FILE_INFO for a regular file given its lake path.
 */
static NTSTATUS fill_file_info(
    LAKE_FS* fs, const char* lake_path,
    FSP_FSCTL_FILE_INFO* file_info
) {
    char hash_buf[HASH_HEX_BUF];
    int rc = lake_get_head(fs->lake, lake_path, hash_buf);
    if (rc == LAKE_ERR_NOT_FOUND) return STATUS_OBJECT_NAME_NOT_FOUND;
    if (rc == LAKE_ERR_DELETED)   return STATUS_DELETE_PENDING;
    if (rc != LAKE_OK)            return STATUS_INTERNAL_ERROR;

    /* Read file to get size. In production you'd store size in metadata
       to avoid this, but for the prototype it works. */
    uint8_t* data = NULL;
    size_t data_len = 0;
    rc = lake_read_file(fs->lake, hash_buf, &data, &data_len);
    if (rc != LAKE_OK) return STATUS_INTERNAL_ERROR;
    lake_bytes_free(data, data_len);

    memset(file_info, 0, sizeof(*file_info));
    file_info->FileAttributes = FILE_ATTRIBUTE_NORMAL;
    file_info->FileSize = data_len;
    file_info->AllocationSize = (data_len + 4095) & ~4095;

    /* Use current time for all timestamps — prototype simplification */
    UINT64 now;
    GetSystemTimeAsFileTime((FILETIME*)&now);
    file_info->CreationTime = now;
    file_info->LastAccessTime = now;
    file_info->LastWriteTime = now;
    file_info->ChangeTime = now;

    return STATUS_SUCCESS;
}

/*
 * Fill out a FSP_FSCTL_FILE_INFO for a directory.
 */
static void fill_dir_info(FSP_FSCTL_FILE_INFO* file_info) {
    memset(file_info, 0, sizeof(*file_info));
    file_info->FileAttributes = FILE_ATTRIBUTE_DIRECTORY;

    UINT64 now;
    GetSystemTimeAsFileTime((FILETIME*)&now);
    file_info->CreationTime = now;
    file_info->LastAccessTime = now;
    file_info->LastWriteTime = now;
    file_info->ChangeTime = now;
}

/*
 * Grow a write buffer to accommodate at least new_cap bytes.
 */
static int grow_write_buf(LAKE_FILE_CONTEXT* ctx, size_t needed) {
    if (needed <= ctx->write_cap) return 0;
    size_t new_cap = ctx->write_cap ? ctx->write_cap * 2 : 4096;
    while (new_cap < needed) new_cap *= 2;
    uint8_t* new_buf = (uint8_t*)realloc(ctx->write_buf, new_cap);
    if (!new_buf) return -1;
    ctx->write_buf = new_buf;
    ctx->write_cap = new_cap;
    return 0;
}

/* ── WinFSP Interface Implementation ───────────────────────── */

static NTSTATUS lake_get_volume_info(
    FSP_FILE_SYSTEM* fsp,
    FSP_FSCTL_VOLUME_INFO* volume_info
) {
    volume_info->TotalSize = 1024ULL * 1024 * 1024 * 100; /* 100 GB virtual */
    volume_info->FreeSize  = 1024ULL * 1024 * 1024 * 50;  /* 50 GB virtual */
    volume_info->VolumeLabelLength =
        (UINT16)(wcslen(L"DeepLake") * sizeof(WCHAR));
    wcscpy_s(volume_info->VolumeLabel,
             sizeof(volume_info->VolumeLabel) / sizeof(WCHAR),
             L"DeepLake");
    return STATUS_SUCCESS;
}

static NTSTATUS lake_get_security_by_name(
    FSP_FILE_SYSTEM* fsp,
    PWSTR file_name,
    PUINT32 file_attributes,
    PSECURITY_DESCRIPTOR security_descriptor,
    SIZE_T* security_descriptor_size
) {
    LAKE_FS* fs = (LAKE_FS*)fsp->UserContext;
    char lake_path[MAX_PATH_CHARS];

    if (to_lake_path(file_name, lake_path, sizeof(lake_path)) != 0)
        return STATUS_OBJECT_NAME_INVALID;

    /* Root directory */
    if (strcmp(lake_path, "/") == 0) {
        if (file_attributes) *file_attributes = FILE_ATTRIBUTE_DIRECTORY;
        return STATUS_SUCCESS;
    }

    /* __alluvium__ virtual namespace */
    if (is_alluvium_path(file_name)) {
        if (file_attributes) *file_attributes = FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_READONLY;
        return STATUS_SUCCESS;
    }

    /* Check if it's a file */
    char hash_buf[HASH_HEX_BUF];
    int rc = lake_get_head(fs->lake, lake_path, hash_buf);
    if (rc == LAKE_OK) {
        if (file_attributes) *file_attributes = FILE_ATTRIBUTE_NORMAL;
        return STATUS_SUCCESS;
    }

    /* Check if it's an implicit directory */
    LakeDirEntry* entries = NULL;
    size_t count = 0;
    rc = lake_list_dir(fs->lake, lake_path, &entries, &count);
    if (rc == LAKE_OK && count > 0) {
        lake_dir_entries_free(entries, count);
        if (file_attributes) *file_attributes = FILE_ATTRIBUTE_DIRECTORY;
        return STATUS_SUCCESS;
    }
    if (entries) lake_dir_entries_free(entries, count);

    return STATUS_OBJECT_NAME_NOT_FOUND;
}

static NTSTATUS lake_open(
    FSP_FILE_SYSTEM* fsp,
    PWSTR file_name,
    UINT32 create_options,
    UINT32 granted_access,
    PVOID* file_context,
    FSP_FSCTL_FILE_INFO* file_info
) {
    LAKE_FS* fs = (LAKE_FS*)fsp->UserContext;

    LAKE_FILE_CONTEXT* ctx = (LAKE_FILE_CONTEXT*)calloc(1, sizeof(*ctx));
    if (!ctx) return STATUS_INSUFFICIENT_RESOURCES;

    if (to_lake_path(file_name, ctx->lake_path, sizeof(ctx->lake_path)) != 0) {
        free(ctx);
        return STATUS_OBJECT_NAME_INVALID;
    }

    /* Virtual namespace — read-only */
    if (is_alluvium_path(file_name)) {
        ctx->is_virtual = 1;
        ctx->is_dir = 1; /* most virtual paths are dirs for now */
        fill_dir_info(file_info);
        *file_context = ctx;
        return STATUS_SUCCESS;
    }

    /* Root */
    if (strcmp(ctx->lake_path, "/") == 0) {
        ctx->is_dir = 1;
        fill_dir_info(file_info);
        *file_context = ctx;
        return STATUS_SUCCESS;
    }

    /* Try as a file */
    NTSTATUS status = fill_file_info(fs, ctx->lake_path, file_info);
    if (status == STATUS_SUCCESS) {
        /* If writable, load content into write buffer */
        if (granted_access & (FILE_WRITE_DATA | FILE_APPEND_DATA)) {
            char hash_buf[HASH_HEX_BUF];
            lake_get_head(fs->lake, ctx->lake_path, hash_buf);
            uint8_t* data = NULL;
            size_t len = 0;
            lake_read_file(fs->lake, hash_buf, &data, &len);
            if (data && len > 0) {
                ctx->write_buf = (uint8_t*)malloc(len);
                if (ctx->write_buf) {
                    memcpy(ctx->write_buf, data, len);
                    ctx->write_len = len;
                    ctx->write_cap = len;
                }
                lake_bytes_free(data, len);
            }
        }
        *file_context = ctx;
        return STATUS_SUCCESS;
    }

    /* Try as a directory */
    LakeDirEntry* entries = NULL;
    size_t count = 0;
    int rc = lake_list_dir(fs->lake, ctx->lake_path, &entries, &count);
    if (rc == LAKE_OK && count > 0) {
        lake_dir_entries_free(entries, count);
        ctx->is_dir = 1;
        fill_dir_info(file_info);
        *file_context = ctx;
        return STATUS_SUCCESS;
    }
    if (entries) lake_dir_entries_free(entries, count);

    free(ctx);
    return STATUS_OBJECT_NAME_NOT_FOUND;
}

static NTSTATUS lake_create(
    FSP_FILE_SYSTEM* fsp,
    PWSTR file_name,
    UINT32 create_options,
    UINT32 granted_access,
    UINT32 file_attributes,
    PSECURITY_DESCRIPTOR security_descriptor,
    UINT64 allocation_size,
    PVOID* file_context,
    FSP_FSCTL_FILE_INFO* file_info
) {
    LAKE_FS* fs = (LAKE_FS*)fsp->UserContext;

    /* Can't create in virtual namespace */
    if (is_alluvium_path(file_name))
        return STATUS_ACCESS_DENIED;

    LAKE_FILE_CONTEXT* ctx = (LAKE_FILE_CONTEXT*)calloc(1, sizeof(*ctx));
    if (!ctx) return STATUS_INSUFFICIENT_RESOURCES;

    if (to_lake_path(file_name, ctx->lake_path, sizeof(ctx->lake_path)) != 0) {
        free(ctx);
        return STATUS_OBJECT_NAME_INVALID;
    }

    /* Creating a directory is a no-op — dirs are implicit */
    if (create_options & FILE_DIRECTORY_FILE) {
        ctx->is_dir = 1;
        fill_dir_info(file_info);
        *file_context = ctx;
        return STATUS_SUCCESS;
    }

    /* Ingest empty file to establish the path */
    char hash_buf[HASH_HEX_BUF];
    int rc = lake_ingest(fs->lake, NULL, 0, hash_buf);
    if (rc != LAKE_OK) {
        free(ctx);
        return STATUS_INTERNAL_ERROR;
    }

    rc = lake_put(fs->lake, ctx->lake_path, hash_buf, "close", "created");
    if (rc != LAKE_OK) {
        free(ctx);
        return STATUS_INTERNAL_ERROR;
    }

    /* Set up empty write buffer */
    ctx->write_buf = NULL;
    ctx->write_len = 0;
    ctx->write_cap = 0;

    fill_file_info(fs, ctx->lake_path, file_info);
    *file_context = ctx;
    return STATUS_SUCCESS;
}

static NTSTATUS lake_read(
    FSP_FILE_SYSTEM* fsp,
    PVOID file_context,
    PVOID buffer,
    UINT64 offset,
    ULONG length,
    PULONG bytes_transferred
) {
    LAKE_FS* fs = (LAKE_FS*)fsp->UserContext;
    LAKE_FILE_CONTEXT* ctx = (LAKE_FILE_CONTEXT*)file_context;

    /* Read from write buffer if available (in-progress write) */
    if (ctx->write_buf) {
        if (offset >= ctx->write_len) {
            *bytes_transferred = 0;
            return STATUS_SUCCESS;
        }
        size_t avail = ctx->write_len - (size_t)offset;
        size_t to_read = length < avail ? length : avail;
        memcpy(buffer, ctx->write_buf + offset, to_read);
        *bytes_transferred = (ULONG)to_read;
        return STATUS_SUCCESS;
    }

    /* Read from lake */
    char hash_buf[HASH_HEX_BUF];
    int rc = lake_get_head(fs->lake, ctx->lake_path, hash_buf);
    if (rc != LAKE_OK) return STATUS_OBJECT_NAME_NOT_FOUND;

    uint8_t* data = NULL;
    size_t data_len = 0;
    rc = lake_read_file(fs->lake, hash_buf, &data, &data_len);
    if (rc != LAKE_OK) return STATUS_INTERNAL_ERROR;

    if (offset >= data_len) {
        lake_bytes_free(data, data_len);
        *bytes_transferred = 0;
        return STATUS_SUCCESS;
    }

    size_t avail = data_len - (size_t)offset;
    size_t to_read = length < avail ? length : avail;
    memcpy(buffer, data + offset, to_read);
    lake_bytes_free(data, data_len);

    *bytes_transferred = (ULONG)to_read;
    return STATUS_SUCCESS;
}

static NTSTATUS lake_write(
    FSP_FILE_SYSTEM* fsp,
    PVOID file_context,
    PVOID buffer,
    UINT64 offset,
    ULONG length,
    BOOLEAN write_to_eof,
    BOOLEAN constrained_io,
    PULONG bytes_transferred,
    FSP_FSCTL_FILE_INFO* file_info
) {
    LAKE_FS* fs = (LAKE_FS*)fsp->UserContext;
    LAKE_FILE_CONTEXT* ctx = (LAKE_FILE_CONTEXT*)file_context;

    if (ctx->is_virtual) return STATUS_ACCESS_DENIED;

    size_t write_offset = write_to_eof ? ctx->write_len : (size_t)offset;
    size_t needed = write_offset + length;

    if (grow_write_buf(ctx, needed) != 0)
        return STATUS_INSUFFICIENT_RESOURCES;

    /* Zero-fill gap if writing past current end */
    if (write_offset > ctx->write_len) {
        memset(ctx->write_buf + ctx->write_len, 0,
               write_offset - ctx->write_len);
    }

    memcpy(ctx->write_buf + write_offset, buffer, length);
    if (needed > ctx->write_len) ctx->write_len = needed;
    ctx->dirty = 1;

    *bytes_transferred = length;

    /* Update file_info with new size */
    fill_file_info(fs, ctx->lake_path, file_info);
    file_info->FileSize = ctx->write_len;
    file_info->AllocationSize = (ctx->write_len + 4095) & ~4095;

    return STATUS_SUCCESS;
}

static VOID lake_cleanup(
    FSP_FILE_SYSTEM* fsp,
    PVOID file_context,
    PWSTR file_name,
    ULONG flags
) {
    LAKE_FS* fs = (LAKE_FS*)fsp->UserContext;
    LAKE_FILE_CONTEXT* ctx = (LAKE_FILE_CONTEXT*)file_context;
    if (!ctx) return;

    /* Handle delete-on-close */
    if (flags & FspCleanupDelete) {
        lake_delete(fs->lake, ctx->lake_path);
    }

    /* Ingest dirty buffer on cleanup (equivalent to FUSE flush/release) */
    if (ctx->dirty && ctx->write_buf) {
        char hash_buf[HASH_HEX_BUF];
        int rc = lake_ingest(fs->lake, ctx->write_buf, ctx->write_len, hash_buf);
        if (rc == LAKE_OK) {
            lake_put(fs->lake, ctx->lake_path, hash_buf, "close", NULL);
        }
        ctx->dirty = 0;
    }
}

static VOID lake_close_file(
    FSP_FILE_SYSTEM* fsp,
    PVOID file_context
) {
    LAKE_FILE_CONTEXT* ctx = (LAKE_FILE_CONTEXT*)file_context;
    if (!ctx) return;

    if (ctx->write_buf) free(ctx->write_buf);
    free(ctx);
}

static NTSTATUS lake_read_directory(
    FSP_FILE_SYSTEM* fsp,
    PVOID file_context,
    PWSTR pattern,
    PWSTR marker,
    PVOID buffer,
    ULONG buffer_length,
    PULONG bytes_transferred
) {
    LAKE_FS* fs = (LAKE_FS*)fsp->UserContext;
    LAKE_FILE_CONTEXT* ctx = (LAKE_FILE_CONTEXT*)file_context;

    if (!ctx->is_dir) return STATUS_NOT_A_DIRECTORY;

    LakeDirEntry* entries = NULL;
    size_t count = 0;
    int rc = lake_list_dir(fs->lake, ctx->lake_path, &entries, &count);
    if (rc != LAKE_OK) return STATUS_INTERNAL_ERROR;

    /* Add __alluvium__ at root level */
    int at_root = (strcmp(ctx->lake_path, "/") == 0);

    NTSTATUS result = STATUS_SUCCESS;
    union {
        UINT8 buf[sizeof(FSP_FSCTL_DIR_INFO) + MAX_PATH_CHARS * sizeof(WCHAR)];
        FSP_FSCTL_DIR_INFO info;
    } entry_buf;

    BOOLEAN past_marker = (marker == NULL || marker[0] == L'\0');

    /* Emit "." and ".." */
    if (past_marker) {
        memset(&entry_buf.info, 0, sizeof(entry_buf.info));
        entry_buf.info.Size = (UINT16)(sizeof(FSP_FSCTL_DIR_INFO) + sizeof(WCHAR));
        entry_buf.info.FileInfo.FileAttributes = FILE_ATTRIBUTE_DIRECTORY;
        wcscpy_s(entry_buf.info.FileNameBuf, MAX_PATH_CHARS, L".");
        if (!FspFileSystemAddDirInfo(&entry_buf.info, buffer, buffer_length, bytes_transferred))
            goto done;
    }

    /* Inject __alluvium__ at root */
    if (at_root) {
        WCHAR name[] = L"__alluvium__";
        if (!past_marker && marker && wcscmp(marker, name) >= 0) {
            /* skip */
        } else if (past_marker || (marker && wcscmp(marker, name) < 0)) {
            past_marker = TRUE;
            memset(&entry_buf.info, 0, sizeof(entry_buf.info));
            entry_buf.info.Size = (UINT16)(sizeof(FSP_FSCTL_DIR_INFO) +
                wcslen(name) * sizeof(WCHAR));
            entry_buf.info.FileInfo.FileAttributes =
                FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_READONLY;
            wcscpy_s(entry_buf.info.FileNameBuf, MAX_PATH_CHARS, name);
            if (!FspFileSystemAddDirInfo(&entry_buf.info, buffer, buffer_length, bytes_transferred))
                goto done;
        }
    }

    /* Emit directory entries from the lake */
    for (size_t i = 0; i < count; i++) {
        /* Convert entry name to wide char */
        WCHAR wide_name[MAX_PATH_CHARS];
        int wlen = MultiByteToWideChar(CP_UTF8, 0, entries[i].name, -1,
                                        wide_name, MAX_PATH_CHARS);
        if (wlen <= 0) continue;

        /* Handle marker-based enumeration */
        if (!past_marker) {
            if (marker && wcscmp(marker, wide_name) >= 0) continue;
            past_marker = TRUE;
        }

        memset(&entry_buf.info, 0, sizeof(entry_buf.info));
        entry_buf.info.Size = (UINT16)(sizeof(FSP_FSCTL_DIR_INFO) +
            wcslen(wide_name) * sizeof(WCHAR));

        if (entries[i].is_dir) {
            entry_buf.info.FileInfo.FileAttributes = FILE_ATTRIBUTE_DIRECTORY;
        } else {
            entry_buf.info.FileInfo.FileAttributes = FILE_ATTRIBUTE_NORMAL;
            /* Get file size */
            char child_path[MAX_PATH_CHARS];
            if (strcmp(ctx->lake_path, "/") == 0)
                snprintf(child_path, sizeof(child_path), "/%s", entries[i].name);
            else
                snprintf(child_path, sizeof(child_path), "%s/%s",
                         ctx->lake_path, entries[i].name);
            char hash_buf[HASH_HEX_BUF];
            if (lake_get_head(fs->lake, child_path, hash_buf) == LAKE_OK) {
                uint8_t* data = NULL;
                size_t data_len = 0;
                if (lake_read_file(fs->lake, hash_buf, &data, &data_len) == LAKE_OK) {
                    entry_buf.info.FileInfo.FileSize = data_len;
                    entry_buf.info.FileInfo.AllocationSize = (data_len + 4095) & ~4095;
                    lake_bytes_free(data, data_len);
                }
            }
        }

        UINT64 now;
        GetSystemTimeAsFileTime((FILETIME*)&now);
        entry_buf.info.FileInfo.CreationTime = now;
        entry_buf.info.FileInfo.LastAccessTime = now;
        entry_buf.info.FileInfo.LastWriteTime = now;

        wcscpy_s(entry_buf.info.FileNameBuf, MAX_PATH_CHARS, wide_name);

        if (!FspFileSystemAddDirInfo(&entry_buf.info, buffer, buffer_length, bytes_transferred))
            break;
    }

done:
    /* Sentinel to signal end of enumeration */
    FspFileSystemAddDirInfo(NULL, buffer, buffer_length, bytes_transferred);

    if (entries) lake_dir_entries_free(entries, count);
    return STATUS_SUCCESS;
}

static NTSTATUS lake_get_file_info(
    FSP_FILE_SYSTEM* fsp,
    PVOID file_context,
    FSP_FSCTL_FILE_INFO* file_info
) {
    LAKE_FS* fs = (LAKE_FS*)fsp->UserContext;
    LAKE_FILE_CONTEXT* ctx = (LAKE_FILE_CONTEXT*)file_context;

    if (ctx->is_dir) {
        fill_dir_info(file_info);
        return STATUS_SUCCESS;
    }

    return fill_file_info(fs, ctx->lake_path, file_info);
}

static NTSTATUS lake_set_file_size(
    FSP_FILE_SYSTEM* fsp,
    PVOID file_context,
    UINT64 new_size,
    BOOLEAN set_allocation_size,
    FSP_FSCTL_FILE_INFO* file_info
) {
    LAKE_FS* fs = (LAKE_FS*)fsp->UserContext;
    LAKE_FILE_CONTEXT* ctx = (LAKE_FILE_CONTEXT*)file_context;

    if (ctx->is_virtual) return STATUS_ACCESS_DENIED;

    if (!set_allocation_size) {
        if (grow_write_buf(ctx, (size_t)new_size) != 0)
            return STATUS_INSUFFICIENT_RESOURCES;
        if (new_size > ctx->write_len) {
            memset(ctx->write_buf + ctx->write_len, 0,
                   (size_t)new_size - ctx->write_len);
        }
        ctx->write_len = (size_t)new_size;
        ctx->dirty = 1;
    }

    fill_file_info(fs, ctx->lake_path, file_info);
    file_info->FileSize = ctx->write_len;
    file_info->AllocationSize = (ctx->write_len + 4095) & ~4095;
    return STATUS_SUCCESS;
}

static NTSTATUS lake_rename_file(
    FSP_FILE_SYSTEM* fsp,
    PVOID file_context,
    PWSTR file_name,
    PWSTR new_file_name,
    BOOLEAN replace_if_exists
) {
    LAKE_FS* fs = (LAKE_FS*)fsp->UserContext;

    char old_path[MAX_PATH_CHARS];
    char new_path[MAX_PATH_CHARS];
    if (to_lake_path(file_name, old_path, sizeof(old_path)) != 0)
        return STATUS_OBJECT_NAME_INVALID;
    if (to_lake_path(new_file_name, new_path, sizeof(new_path)) != 0)
        return STATUS_OBJECT_NAME_INVALID;

    int rc = lake_rename(fs->lake, old_path, new_path);
    if (rc != LAKE_OK) return STATUS_OBJECT_NAME_NOT_FOUND;

    /* Update the context so subsequent operations use the new path */
    LAKE_FILE_CONTEXT* ctx = (LAKE_FILE_CONTEXT*)file_context;
    strncpy(ctx->lake_path, new_path, MAX_PATH_CHARS - 1);

    return STATUS_SUCCESS;
}

/* ── WinFSP Interface Table ─────────────────────────────────── */

static FSP_FILE_SYSTEM_INTERFACE lake_interface = {
    .GetVolumeInfo     = lake_get_volume_info,
    .GetSecurityByName = lake_get_security_by_name,
    .Open              = lake_open,
    .Create            = lake_create,
    .Read              = lake_read,
    .Write             = lake_write,
    .Cleanup           = lake_cleanup,
    .Close             = lake_close_file,
    .ReadDirectory     = lake_read_directory,
    .GetFileInfo       = lake_get_file_info,
    .SetFileSize       = lake_set_file_size,
    .Rename            = lake_rename_file,
};

/* ── Main ───────────────────────────────────────────────────── */

int wmain(int argc, WCHAR** argv) {
    /* Simple argument parsing */
    const WCHAR* mount_point = L"L:";
    const char* lake_dir = ".lake";

    for (int i = 1; i < argc; i++) {
        if (wcscmp(argv[i], L"-m") == 0 && i + 1 < argc) {
            mount_point = argv[++i];
        } else if (wcscmp(argv[i], L"-l") == 0 && i + 1 < argc) {
            /* Convert lake dir to UTF-8 */
            static char lake_buf[MAX_PATH_CHARS];
            WideCharToMultiByte(CP_UTF8, 0, argv[++i], -1,
                                lake_buf, sizeof(lake_buf), NULL, NULL);
            lake_dir = lake_buf;
        }
    }

    /* Open the lake */
    LakeHandle* lake = lake_open(lake_dir);
    if (!lake) {
        fwprintf(stderr, L"Error: failed to open lake at %S\n", lake_dir);
        return 1;
    }

    /* Create WinFSP filesystem */
    FSP_FSCTL_VOLUME_PARAMS volume_params;
    memset(&volume_params, 0, sizeof(volume_params));
    volume_params.SectorSize = 512;
    volume_params.SectorsPerAllocationUnit = 1;
    volume_params.MaxComponentLength = 255;
    volume_params.FileInfoTimeout = 1000;
    volume_params.CaseSensitiveSearch = 1;
    volume_params.CasePreservedNames = 1;
    volume_params.UnicodeOnDisk = 1;
    volume_params.PersistentAcls = 0;
    wcscpy_s(volume_params.FileSystemName,
             sizeof(volume_params.FileSystemName) / sizeof(WCHAR),
             L"DeepLake");

    LAKE_FS fs;
    fs.lake = lake;

    NTSTATUS status = FspFileSystemCreate(
        (PWSTR)L"" FSP_FSCTL_DISK_DEVICE_NAME,
        &volume_params,
        &lake_interface,
        &fs.fsp
    );
    if (!NT_SUCCESS(status)) {
        fwprintf(stderr, L"Error: FspFileSystemCreate failed (0x%08lx)\n", status);
        lake_close(lake);
        return 1;
    }

    fs.fsp->UserContext = &fs;

    /* Set mount point */
    status = FspFileSystemSetMountPoint(fs.fsp, (PWSTR)mount_point);
    if (!NT_SUCCESS(status)) {
        fwprintf(stderr, L"Error: failed to set mount point %s (0x%08lx)\n",
                mount_point, status);
        FspFileSystemDelete(fs.fsp);
        lake_close(lake);
        return 1;
    }

    /* Start the filesystem dispatcher */
    status = FspFileSystemStartDispatcher(fs.fsp, 0);
    if (!NT_SUCCESS(status)) {
        fwprintf(stderr, L"Error: failed to start dispatcher (0x%08lx)\n", status);
        FspFileSystemDelete(fs.fsp);
        lake_close(lake);
        return 1;
    }

    wprintf(L"DeepLake mounted at %s\n", mount_point);
    wprintf(L"Lake storage at %S\n", lake_dir);
    wprintf(L"  __alluvium__\\history\\   - browse version history\n");
    wprintf(L"  __alluvium__\\timeline\\  - versions by date\n");
    wprintf(L"  __alluvium__\\stats\\     - storage statistics\n");
    wprintf(L"  __alluvium__\\deleted\\   - soft-deleted files\n");
    wprintf(L"Press Ctrl+C to unmount\n");

    /* Wait for Ctrl+C */
    Sleep(INFINITE);

    /* Cleanup */
    FspFileSystemStopDispatcher(fs.fsp);
    FspFileSystemDelete(fs.fsp);
    lake_close(lake);

    return 0;
}
