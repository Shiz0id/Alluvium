# Alluvium

**A FUSE PoC for Varvefs — the content-addressed, version-native filesystem.**

Alluvium is a working proof of concept for a filesystem that treats files as immutable, content-addressed objects in a hash lake. The familiar POSIX directory tree is a computed illusion projected from the lake. Every edit creates a new version. Nothing is ever overwritten. Nothing is ever lost.

This is the foundation layer for Varvefs, a filesystem that combines content-addressed storage, relational semantic metadata, and full version history as first-class features.

---

## What It Does

**Content-addressed storage.** Every file is split into chunks via FastCDC, each chunk hashed with BLAKE3 and stored once. Identical content across files or across versions is never duplicated.

**Automatic version history.** Every file close creates a new version. No commands, no configuration, no prior setup. The history is just there.

**Structural diffing.** Compare any two versions of a file at the chunk level. The filesystem knows exactly which byte ranges changed without reading or interpreting the file contents.

**Sub-file deduplication.** Change one paragraph in a large document — only the affected chunks are new objects. The rest are shared with previous versions.

**Integrity by default.** Every chunk is verified against its BLAKE3 hash on read. Silent data corruption is detected automatically.

**Instant rename and move.** File moves are metadata-only operations regardless of file size.

**Soft delete.** Deleted files are hidden, not destroyed. Full history is preserved. Restore with one command.

**Rewind.** Roll any file back to any previous version. Non-destructive — rewinding creates a new version entry, preserving the full timeline.

---

## The `.alluvium` Virtual Namespace

Alluvium exposes a read-only virtual directory at the mount root that lets you browse version history, storage statistics, and timeline data using standard Unix tools. No special software required.

```
mnt/
├── Documents/
│   ├── report.txt              ← normal file (HEAD version)
│   └── notes.txt
└── .alluvium/
    ├── history/
    │   └── Documents/
    │       └── report.txt/
    │           ├── v1          ← full content at version 1
    │           ├── v2
    │           └── v3
    ├── timeline/
    │   ├── 2026-02-24/
    │   │   └── 14:05:33_report.txt
    │   └── 2026-02-25/
    │       ├── 11:02:45_report.txt
    │       └── 15:45:01_notes.txt
    ├── stats/
    │   └── Documents/
    │       └── report.txt      ← storage stats as readable text
    └── deleted/
        └── old_draft.txt       ← soft-deleted files
```

### Browse history with `ls` and `cat`

```bash
$ ls mnt/.alluvium/history/Documents/report.txt/
v1  v2  v3  v4  v5

$ cat mnt/.alluvium/history/Documents/report.txt/v2
```

### Diff versions with standard `diff`

```bash
$ diff mnt/.alluvium/history/Documents/report.txt/v1 \
       mnt/.alluvium/history/Documents/report.txt/v3
```

### View storage efficiency

```bash
$ cat mnt/.alluvium/stats/Documents/report.txt
Storage Stats: /Documents/report.txt
─────────────────────────────────
Versions:        5
Unique chunks:   12
Chunk refs:      47
Naive storage:   2.4 MB
Actual storage:  890 KB
Dedup ratio:     62.9%
Space saved:     1.5 MB
```

---

## CLI: `lakectl`

```bash
lakectl history <path>              # list all versions
lakectl diff <path> <v1> <v2>       # structural diff between two versions
lakectl cat <path> <version>        # print file content at a version
lakectl rewind <path> <version>     # rewind file to a previous version
lakectl stats <path>                # storage statistics
lakectl restore <path>              # restore a soft-deleted file
```

The CLI talks directly to the lake and path table, bypassing FUSE. Use `--lake <path>` to specify the storage directory (defaults to `.lake/`).

---

## Architecture

```
┌──────────────────────────────────────────────────┐
│                   FUSE Layer                      │
│         POSIX translation + .alluvium views       │
├──────────────────────────────────────────────────┤
│                   lake-core                       │
│  ┌────────────┐ ┌──────────┐ ┌────────────────┐  │
│  │  Ingester   │ │  Differ  │ │    History      │  │
│  │  (FastCDC)  │ │  (LCS)   │ │  (coordination)│  │
│  └──────┬──────┘ └────┬─────┘ └───────┬────────┘  │
│         │             │               │            │
│  ┌──────▼─────────────▼───────────────▼────────┐  │
│  │            Object Store                      │  │
│  │     BLAKE3 content-addressed hash lake       │  │
│  └──────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────┐  │
│  │            Path Table (SQLite)                │  │
│  │     path → HEAD manifest + version history    │  │
│  └──────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────┘
```

### Source Layout

```
alluvium/
├── Cargo.toml                  # workspace
└── crates/
    ├── lake-core/
    │   └── src/
    │       ├── lib.rs          # module exports
    │       ├── types.rs        # B3Hash, Chunk, ChunkRef, Manifest, Version
    │       ├── store.rs        # content-addressed object store
    │       ├── ingest.rs       # FastCDC chunking pipeline
    │       ├── diff.rs         # positional diff engine
    │       ├── path_table.rs   # SQLite path → manifest mapping
    │       └── history.rs      # version chain coordination
    ├── lake-fuse/
    │   └── src/
    │       └── main.rs         # FUSE mount + .alluvium virtual namespace
    └── lakectl/
        └── src/
            └── main.rs         # CLI tool
```

---

## Building

Requires Linux (or WSL2) with FUSE support and a Rust toolchain.

```bash
# Install FUSE development headers (Ubuntu/Debian)
sudo apt install fuse3 libfuse3-dev pkg-config

# Build everything
cargo build --release

# Mount the filesystem
./target/release/lake-fuse .lake mnt

# In another terminal
echo "hello varve" > mnt/test.txt
echo "hello world" > mnt/test.txt
cat mnt/.alluvium/history/test.txt/
# v1  v2

./target/release/lakectl history /test.txt
./target/release/lakectl diff /test.txt 1 2
```

---

## Design Principles

**The lake is immutable.** Content goes in, gets a hash, and never changes. New content gets new hashes.

**The path table is the only mutable state.** HEAD pointers move. Everything else is append-only.

**POSIX is a view.** The directory hierarchy is computed from the path table. It's one possible projection of the lake. The `.alluvium` namespace is another.

**Format agnostic.** The lake stores bytes. It doesn't know or care whether something is a text file, an image, or a database. Chunking, hashing, versioning, and diffing all operate on raw bytes.

**Nothing is lost unless you explicitly destroy it.** Edits create new versions. Deletes are soft. Rewinds are non-destructive. Garbage collection is a deliberate policy decision, not a side effect.

---

## What This Doesn't Do (Yet)

Alluvium is a prototype. It demonstrates the storage and versioning layer. What comes next in Varve FS:

- **Relational semantic metadata** — typed objects, properties, relationships, computed views
- **AI enrichment** — automatic tagging, classification, summarization, knowledge graph construction
- **Branching and merging** — filesystem-level branches for projects, experiments, drafts
- **Per-process view virtualization** — different apps see different projections of the same lake
- **Distributed sync** — push and pull between machines at the lake level
- **Zone architecture** — POSIX-speed zones for system files, full lake semantics for user files
- **Kernel module** — replacing FUSE with a native filesystem for production performance

---

## Origins

The POSIX filesystem was born on the PDP-11 — a machine with 256KB of RAM. Its constraints became Unix's design, Unix's design became a standard, and the standard went unchallenged for over fifty years. Every computer on earth still organizes files the way a minicomputer had to in 1971.

Alluvium asks: what if we kept the interface but replaced the assumption?

---

*Alluvium: the sediment that builds new ground. Varve: the layered sediment record of what came before.*
