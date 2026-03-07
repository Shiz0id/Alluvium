use std::process;

use clap::{Parser, Subcommand};
use colored::*;

use lake_core::history::History;
use lake_core::ingest::Ingester;
use lake_core::path_table::PathTable;
use lake_core::store::ObjectStore;
use lake_core::types::VersionTrigger;

// ─── CLI Definition ──────────────────────────────────────────────

#[derive(Parser)]
#[command(name = "lakectl")]
#[command(about = "DeepLake filesystem control tool")]
#[command(version)]
#[command(allow_hyphen_values = true)]
struct Cli {
    /// Path to the lake storage directory
    #[arg(long, default_value = ".lake", global = true)]
    lake: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Ingest a file from disk into the lake
    Ingest {
        /// Lake path (e.g. /Documents/report.txt)
        lake_path: String,
        /// Path to the file on disk
        #[arg(long)]
        file: String,
        /// Optional commit message
        #[arg(long, short)]
        message: Option<String>,
        /// Minimum chunk size in bytes (default: 8192)
        #[arg(long)]
        chunk_min: Option<u32>,
        /// Average chunk size in bytes (default: 16384)
        #[arg(long)]
        chunk_avg: Option<u32>,
        /// Maximum chunk size in bytes (default: 65536)
        #[arg(long)]
        chunk_max: Option<u32>,
    },
    /// Show version history for a file
    History {
        /// Lake path (e.g. /Documents/report.txt)
        path: String,
    },
    /// Diff two versions of a file
    Diff {
        /// Lake path
        path: String,
        /// First version number
        v1: u64,
        /// Second version number
        v2: u64,
    },
    /// Rewind a file to a previous version
    Rewind {
        /// Lake path
        path: String,
        /// Version number to rewind to
        version: u64,
    },
    /// Show storage statistics for a file
    Stats {
        /// Lake path
        path: String,
    },
    /// Restore a soft-deleted file
    Restore {
        /// Lake path
        path: String,
    },
    /// Print file content at a specific version
    Cat {
        /// Lake path
        path: String,
        /// Version number
        version: u64,
    },
    /// List files and directories at a path
    Ls {
        /// Lake path (e.g. / or /Documents)
        #[arg(default_value = "/", allow_hyphen_values = true)]
        path: String,
    },
    /// List all paths in the lake (flat and hierarchical)
    Paths,
    /// List soft-deleted files
    Deleted,
    /// Seed the lake with demo data to showcase features
    Seed,
}

// ─── Main ────────────────────────────────────────────────────────

fn main() {
    // Enable ANSI color support on Windows
    #[cfg(windows)]
    let _ = colored::control::set_virtual_terminal(true);

    let cli = Cli::parse();

    // Ensure lake directory exists
    if let Err(e) = std::fs::create_dir_all(&cli.lake) {
        eprintln!("{} failed to create lake dir: {}", "error:".red().bold(), e);
        process::exit(1);
    }

    let store = match ObjectStore::open(format!("{}/objects", cli.lake)) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("{} failed to open lake at {}: {}", "error:".red().bold(), cli.lake, e);
            process::exit(1);
        }
    };

    let paths = match PathTable::open(format!("{}/paths.db", cli.lake)) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("{} failed to open path table: {}", "error:".red().bold(), e);
            process::exit(1);
        }
    };

    let history = History::new(&store, &paths);

    match cli.command {
        Commands::Ingest { lake_path, file, message, chunk_min, chunk_avg, chunk_max } => {
            cmd_ingest(&store, &paths, &lake_path, &file, message.as_deref(),
                       chunk_min, chunk_avg, chunk_max)
        }
        Commands::History { path } => cmd_history(&history, &path),
        Commands::Diff { path, v1, v2 } => cmd_diff(&history, &path, v1, v2),
        Commands::Rewind { path, version } => cmd_rewind(&history, &path, version),
        Commands::Stats { path } => cmd_stats(&history, &path),
        Commands::Restore { path } => cmd_restore(&paths, &path),
        Commands::Cat { path, version } => cmd_cat(&history, &path, version),
        Commands::Ls { path } => cmd_ls(&paths, &path),
        Commands::Paths => cmd_paths(&paths),
        Commands::Deleted => cmd_deleted(&paths),
        Commands::Seed => cmd_seed(&store, &paths),
    }
}

// ─── Commands ────────────────────────────────────────────────────

fn cmd_ingest(
    store: &ObjectStore,
    paths: &PathTable,
    lake_path: &str,
    file_path: &str,
    message: Option<&str>,
    chunk_min: Option<u32>,
    chunk_avg: Option<u32>,
    chunk_max: Option<u32>,
) {
    let data = match std::fs::read(file_path) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("{} failed to read {}: {}", "error:".red().bold(), file_path, e);
            process::exit(1);
        }
    };

    let ingester = if chunk_min.is_some() || chunk_avg.is_some() || chunk_max.is_some() {
        use lake_core::ingest::ChunkParams;
        let avg = chunk_avg.unwrap_or(16 * 1024);
        let params = ChunkParams {
            min_size: chunk_min.unwrap_or(avg / 4),
            avg_size: avg,
            max_size: chunk_max.unwrap_or(avg * 4),
        };
        Ingester::with_params(store, params)
    } else {
        Ingester::new(store)
    };
    let manifest = match ingester.ingest(&data) {
        Ok(m) => m,
        Err(e) => {
            eprintln!("{} ingestion failed: {}", "error:".red().bold(), e);
            process::exit(1);
        }
    };

    let trigger = if message.is_some() {
        VersionTrigger::Explicit
    } else {
        VersionTrigger::Close
    };

    match paths.put(lake_path, &manifest.hash, trigger, message) {
        Ok(()) => {
            println!(
                "{} {} ingested as {}",
                "✓".green().bold(),
                file_path,
                lake_path,
            );
            println!(
                "  manifest: b3:{}",
                &manifest.hash.to_hex()[..12].dimmed(),
            );
            println!(
                "  chunks: {}, size: {}",
                manifest.chunks.len(),
                format_bytes(manifest.total_size),
            );
        }
        Err(e) => {
            eprintln!("{} failed to register path: {}", "error:".red().bold(), e);
            process::exit(1);
        }
    }
}

fn cmd_ls(paths: &PathTable, path: &str) {
    let entries = match paths.list_dir(path) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("{} {}", "error:".red().bold(), e);
            process::exit(1);
        }
    };

    if entries.is_empty() {
        println!("{}", "Empty directory.".dimmed());
        return;
    }

    for entry in &entries {
        if entry.is_dir {
            println!("  {}  {}", "dir ".blue().bold(), entry.name.blue());
        } else {
            println!("  {}  {}", "file".white(), entry.name);
        }
    }
}

fn cmd_paths(paths: &PathTable) {
    let all = match paths.list_all() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("{} {}", "error:".red().bold(), e);
            process::exit(1);
        }
    };

    if all.is_empty() {
        println!("{}", "Lake is empty.".dimmed());
        return;
    }

    println!("{} ({} entries)", "All paths:".bold(), all.len());
    for path in &all {
        // Detect whether it's a UUID or hierarchical path
        if path.starts_with('/') {
            println!("  {}  {}", "path".white(), path);
        } else {
            println!("  {}  {}", " uid".yellow(), path);
        }
    }
}

fn cmd_deleted(paths: &PathTable) {
    let deleted = match paths.list_deleted() {
        Ok(d) => d,
        Err(e) => {
            eprintln!("{} {}", "error:".red().bold(), e);
            process::exit(1);
        }
    };

    if deleted.is_empty() {
        println!("{}", "No deleted files.".dimmed());
        return;
    }

    println!("{}", "Deleted files:".bold());
    for path in &deleted {
        println!("  {}  {}", "✗".red(), path);
    }
}

fn cmd_seed(store: &ObjectStore, paths: &PathTable) {
    println!("{}", "Seeding lake with demo data...".bold());
    println!();

    let ingester = Ingester::new(store);

    // ── Document with evolving versions ──────────────────────────
    // Each version must be large enough (>16KB) to produce multiple
    // FastCDC chunks so shared sections survive across edits.

    // Build a substantial base document
    let base_preamble = "Project Proposal: DeepLake OS\n\
        \n\
        Abstract:\n\
        We propose a novel operating system built around a content-addressed\n\
        filesystem that unifies storage, versioning, and semantic metadata\n\
        into a single coherent layer.\n\
        \n\
        The current filesystem paradigm dates back to 1973 and the PDP-11.\n\
        A machine with 256KB of RAM whose hardware constraints directly shaped\n\
        the POSIX filesystem model. Fixed inodes, flat byte streams, hierarchical\n\
        directories as the only organizational primitive. Those were not design\n\
        ideals — they were survival strategies for a machine that could not afford\n\
        anything richer. And then Unix won, and the constraints got frozen into\n\
        an API, and the API got frozen into a standard, and the standard got\n\
        frozen into every operating system for half a century.\n\
        \n\
        Every modern feature — versioning, search, metadata, cloud sync,\n\
        sandboxing, backup, deduplication, integrity checking — has been\n\
        bolted onto a model designed for single users on PDP-11 hardware.\n\
        DeepLake inverts this: POSIX is a compatibility shim projected from\n\
        a modern foundation, not the foundation itself.\n\
        \n";

    // Padding that remains constant across versions to ensure shared chunks
    let stable_body = "Background and Motivation:\n\
        \n\
        The filesystem is the most fundamental abstraction in computing.\n\
        Every application, every document, every photograph, every email,\n\
        every configuration file, every log entry, every database — all of\n\
        it ultimately lives as bytes on a storage device, mediated by the\n\
        filesystem. And yet the filesystem abstraction has barely changed\n\
        since its invention. We still organize files into hierarchical\n\
        directories. We still identify files by path. We still treat every\n\
        file as an opaque sequence of bytes with no inherent structure or\n\
        meaning. We still lose files when we forget where we put them.\n\
        We still lose versions when we overwrite without saving. We still\n\
        lose everything when a disk fails without backup.\n\
        \n\
        These are not unsolvable problems. They are symptoms of building\n\
        every solution on top of an abstraction that was never designed to\n\
        support them. Version control systems solve versioning for code\n\
        but not for documents. Search engines solve finding things on the\n\
        web but not on your own computer. Cloud sync solves backup but\n\
        introduces complexity and privacy concerns. Each solution operates\n\
        in its own silo, unaware of the others, bolted onto a filesystem\n\
        that treats all files as anonymous byte sequences.\n\
        \n\
        DeepLake proposes a different approach: solve these problems at the\n\
        filesystem layer itself, where every application can benefit from\n\
        the solution without modification. Content-addressed storage gives\n\
        us deduplication and integrity. Chunking gives us efficient diffs\n\
        and sub-file deduplication. Version history gives us the ability\n\
        to recover any previous state. And semantic metadata — added later\n\
        by an enrichment layer — gives us the ability to find files by\n\
        what they contain rather than where we put them.\n\
        \n\
        The key insight is that these capabilities emerge naturally from\n\
        the right storage primitive. Content addressing means identity is\n\
        derived from content, not location. The moment that choice is made,\n\
        duplication becomes impossible. Versioning becomes append-only.\n\
        Integrity verification becomes free. And the hierarchical directory\n\
        tree becomes just one possible view of the underlying data — a view\n\
        that can be computed, rearranged, and personalized without moving\n\
        a single byte on disk.\n\
        \n\
        This is not a theoretical exercise. The Alluvium prototype\n\
        demonstrates every one of these properties through a working FUSE\n\
        filesystem that mounts on any Linux machine. Applications see a\n\
        normal directory tree. They read and write files normally. But\n\
        underneath, every write creates a new immutable version, every\n\
        file is verified on every read, and every byte is stored exactly\n\
        once regardless of how many files or versions reference it.\n\
        \n";

    let versions = [
        (
            "/Documents/proposal.txt",
            format!(
                "{}{}",
                base_preamble,
                stable_body,
            ),
            "initial draft",
        ),
        (
            "/Documents/proposal.txt",
            format!(
                "{}{}\
                 Key Innovation:\n\
                 \n\
                 Files are stored as BLAKE3 hashes in a flat content-addressed lake.\n\
                 The familiar directory hierarchy is a computed view, not the reality.\n\
                 Every write creates a new immutable version. Nothing is ever lost.\n\
                 BLAKE3 provides verification, not immutability — the append-only\n\
                 write policy of the object store provides immutability. Together\n\
                 they guarantee that stored data cannot be silently corrupted or\n\
                 tampered with.\n\
                 \n",
                base_preamble,
                stable_body,
            ),
            "added key innovation section",
        ),
        (
            "/Documents/proposal.txt",
            format!(
                "{}{}\
                 Key Innovation:\n\
                 \n\
                 Files are stored as BLAKE3 hashes in a flat content-addressed lake.\n\
                 The familiar directory hierarchy is a computed view, not the reality.\n\
                 Every write creates a new immutable version. Nothing is ever lost.\n\
                 BLAKE3 provides verification, not immutability — the append-only\n\
                 write policy of the object store provides immutability. Together\n\
                 they guarantee that stored data cannot be silently corrupted or\n\
                 tampered with.\n\
                 \n\
                 Technical Foundation:\n\
                 \n\
                 - Content-defined chunking via FastCDC for sub-file deduplication\n\
                 - Per-file version history with structural diffing at chunk level\n\
                 - WORM storage properties with a thin mutable index layer\n\
                 - Automatic integrity verification on every read via BLAKE3\n\
                 - Soft delete preserving full history for recovery\n\
                 - Instant rename/move as metadata-only operations\n\
                 \n\
                 The prototype (Alluvium) demonstrates these properties through a\n\
                 FUSE filesystem that is fully transparent to applications. No app\n\
                 needs modification. No user needs training. The filesystem simply\n\
                 works better than what came before.\n\
                 \n",
                base_preamble,
                stable_body,
            ),
            "added technical foundation section",
        ),
        (
            "/Documents/proposal.txt",
            format!(
                "Project Proposal: DeepLake OS\n\
                 Version: Final Draft\n\
                 \n\
                 Abstract:\n\
                 We propose a novel operating system built around a content-addressed\n\
                 filesystem that unifies storage, versioning, and semantic metadata\n\
                 into a single coherent layer. This document outlines the technical\n\
                 architecture, the phased development approach, and the business model\n\
                 for commercialization.\n\
                 \n\
                 The current filesystem paradigm dates back to 1973 and the PDP-11.\n\
                 A machine with 256KB of RAM whose hardware constraints directly shaped\n\
                 the POSIX filesystem model. Fixed inodes, flat byte streams, hierarchical\n\
                 directories as the only organizational primitive. Those were not design\n\
                 ideals — they were survival strategies for a machine that could not afford\n\
                 anything richer. And then Unix won, and the constraints got frozen into\n\
                 an API, and the API got frozen into a standard, and the standard got\n\
                 frozen into every operating system for half a century.\n\
                 \n\
                 Every modern feature — versioning, search, metadata, cloud sync,\n\
                 sandboxing, backup, deduplication, integrity checking — has been\n\
                 bolted onto a model designed for single users on PDP-11 hardware.\n\
                 DeepLake inverts this: POSIX is a compatibility shim projected from\n\
                 a modern foundation, not the foundation itself.\n\
                 \n\
                 {}\
                 Key Innovation:\n\
                 \n\
                 Files are stored as BLAKE3 hashes in a flat content-addressed lake.\n\
                 The familiar directory hierarchy is a computed view, not the reality.\n\
                 Every write creates a new immutable version. Nothing is ever lost.\n\
                 BLAKE3 provides verification, not immutability — the append-only\n\
                 write policy of the object store provides immutability. Together\n\
                 they guarantee that stored data cannot be silently corrupted or\n\
                 tampered with.\n\
                 \n\
                 Technical Foundation:\n\
                 \n\
                 - Content-defined chunking via FastCDC for sub-file deduplication\n\
                 - Per-file version history with structural diffing at chunk level\n\
                 - WORM storage properties with a thin mutable index layer\n\
                 - Automatic integrity verification on every read via BLAKE3\n\
                 - Soft delete preserving full history for recovery\n\
                 - Instant rename/move as metadata-only operations\n\
                 \n\
                 The prototype (Alluvium) demonstrates these properties through a\n\
                 FUSE filesystem that is fully transparent to applications. No app\n\
                 needs modification. No user needs training. The filesystem simply\n\
                 works better than what came before.\n\
                 \n\
                 Business Model:\n\
                 \n\
                 Open source core with paid AI enrichment cloud and enterprise\n\
                 collaboration tier. The filesystem, kernel module, and all tooling\n\
                 are open. Revenue comes from managed cloud services that provide\n\
                 AI-powered semantic tagging, organization-wide search, and\n\
                 compliance features built on top of the open storage layer.\n\
                 \n",
                stable_body,
            ),
            "final draft with business model",
        ),
    ];

    print_section("Document with 4 evolving versions");
    for (lake_path, content, message) in &versions {
        let manifest = ingester.ingest(content.as_bytes()).unwrap();
        paths
            .put(lake_path, &manifest.hash, VersionTrigger::Explicit, Some(message))
            .unwrap();
        println!(
            "  {} v{}: {} ({}, {} chunks)",
            "✓".green(),
            paths.get_current_version_num(lake_path).unwrap(),
            message.dimmed(),
            format_bytes(content.len() as u64),
            manifest.chunks.len(),
        );
    }

    // ── Deduplication demo: similar files ────────────────────────

    print_section("Deduplication demo: three similar config files");

    let configs = [
        (
            "/Config/app.toml",
            "[database]\nhost = \"localhost\"\nport = 5432\nname = \"deeplake_dev\"\n\n\
             [server]\nhost = \"0.0.0.0\"\nport = 8080\nworkers = 4\n\n\
             [logging]\nlevel = \"info\"\nformat = \"json\"\n",
            "dev config",
        ),
        (
            "/Config/app.staging.toml",
            "[database]\nhost = \"staging-db.internal\"\nport = 5432\nname = \"deeplake_staging\"\n\n\
             [server]\nhost = \"0.0.0.0\"\nport = 8080\nworkers = 8\n\n\
             [logging]\nlevel = \"info\"\nformat = \"json\"\n",
            "staging config — mostly identical to dev",
        ),
        (
            "/Config/app.prod.toml",
            "[database]\nhost = \"prod-db.internal\"\nport = 5432\nname = \"deeplake_prod\"\n\n\
             [server]\nhost = \"0.0.0.0\"\nport = 8080\nworkers = 32\n\n\
             [logging]\nlevel = \"warn\"\nformat = \"json\"\n",
            "prod config — mostly identical to dev",
        ),
    ];

    for (lake_path, content, message) in &configs {
        let manifest = ingester.ingest(content.as_bytes()).unwrap();
        paths
            .put(lake_path, &manifest.hash, VersionTrigger::Close, Some(message))
            .unwrap();
        println!("  {} {}: {}", "✓".green(), lake_path, message.dimmed());
    }

    // ── Soft delete demo ─────────────────────────────────────────

    print_section("Soft delete demo");

    let manifest = ingester
        .ingest(b"This file will be deleted but never truly lost.\nThe lake remembers everything.\n")
        .unwrap();
    paths
        .put(
            "/Documents/deleted_memo.txt",
            &manifest.hash,
            VersionTrigger::Close,
            Some("memo that gets deleted"),
        )
        .unwrap();
    println!("  {} created /Documents/deleted_memo.txt", "✓".green());

    paths.delete("/Documents/deleted_memo.txt").unwrap();
    println!("  {} soft-deleted /Documents/deleted_memo.txt", "✗".red());
    println!(
        "  {}",
        "  (still in the lake, recoverable with: lakectl restore /Documents/deleted_memo.txt)"
            .dimmed()
    );

    // ── Large-ish file with binary-like data ─────────────────────

    print_section("Larger file with two versions (shows chunk-level dedup)");

    // 256KB — large enough for many chunks at 16KB average
    let large_v1: Vec<u8> = (0u64..256 * 1024)
        .map(|i| (i.wrapping_mul(31).wrapping_add(17)) as u8)
        .collect();
    let manifest = ingester.ingest(&large_v1).unwrap();
    let chunks_v1 = manifest.chunks.len();
    paths
        .put(
            "/Data/measurements.bin",
            &manifest.hash,
            VersionTrigger::Close,
            Some("initial dataset"),
        )
        .unwrap();
    println!(
        "  {} v1: {} in {} chunks",
        "✓".green(),
        format_bytes(large_v1.len() as u64),
        chunks_v1,
    );

    let mut large_v2 = large_v1.clone();
    // Modify a small region — only affected chunks should differ
    for i in 100_000..100_500 {
        large_v2[i] = 0xFF;
    }
    let manifest = ingester.ingest(&large_v2).unwrap();
    let chunks_v2 = manifest.chunks.len();
    paths
        .put(
            "/Data/measurements.bin",
            &manifest.hash,
            VersionTrigger::Close,
            Some("corrected readings in middle section"),
        )
        .unwrap();
    println!(
        "  {} v2: {} in {} chunks (small edit in the middle)",
        "✓".green(),
        format_bytes(large_v2.len() as u64),
        chunks_v2,
    );

    // ── Summary ──────────────────────────────────────────────────

    println!();
    println!("{}", "Lake seeded. Try these commands:".bold());
    println!();
    println!("  {} — version history with commit messages",
        "lakectl history /Documents/proposal.txt".cyan());
    println!("  {} — what changed between drafts",
        "lakectl diff /Documents/proposal.txt 1 4".cyan());
    println!("  {} — dedup savings across versions",
        "lakectl stats /Documents/proposal.txt".cyan());
    println!("  {} — chunk-level dedup on binary data",
        "lakectl stats /Data/measurements.bin".cyan());
    println!("  {} — diff shows exactly which chunks changed",
        "lakectl diff /Data/measurements.bin 1 2".cyan());
    println!("  {} — read an old version",
        "lakectl cat /Documents/proposal.txt 1".cyan());
    println!("  {} — rewind to first draft",
        "lakectl rewind /Documents/proposal.txt 1".cyan());
    println!("  {} — bring back the deleted memo",
        "lakectl restore /Documents/deleted_memo.txt".cyan());
}

fn print_section(title: &str) {
    println!();
    println!("  {}", title.white().bold());
    println!("  {}", "─".repeat(title.len()));
}

fn cmd_history(history: &History, path: &str) {
    let versions = match history.list(path) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("{} {}", "error:".red().bold(), e);
            process::exit(1);
        }
    };

    if versions.is_empty() {
        println!("{}", "No history found.".dimmed());
        return;
    }

    println!("{} {}", "History:".bold(), path);
    println!("{}", "─".repeat(60));

    for v in &versions {
        let trigger = match v.trigger {
            VersionTrigger::Close => "close".dimmed(),
            VersionTrigger::Explicit => "explicit".cyan(),
            VersionTrigger::MicroHash => "micro".yellow(),
        };

        let hash_short = &v.manifest_hash.to_hex()[..12];
        let msg = v
            .message
            .as_deref()
            .unwrap_or("")
            .dimmed();

        let ts = format_timestamp(v.timestamp);

        let version_label = format!("v{}", v.version_num);
        let is_head = v.version_num
            == versions.iter().map(|x| x.version_num).max().unwrap_or(0);

        if is_head {
            println!(
                "  {} {}  {}  b3:{}  [{}] {}",
                version_label.green().bold(),
                "HEAD".green().bold(),
                ts.dimmed(),
                hash_short.dimmed(),
                trigger,
                msg,
            );
        } else {
            println!(
                "  {}       {}  b3:{}  [{}] {}",
                version_label.white().bold(),
                ts.dimmed(),
                hash_short.dimmed(),
                trigger,
                msg,
            );
        }
    }
}

fn cmd_diff(history: &History, path: &str, v1: u64, v2: u64) {
    let result = match history.diff_versions(path, v1, v2) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("{} {}", "error:".red().bold(), e);
            process::exit(1);
        }
    };

    println!(
        "{} {} v{} → v{}",
        "Diff:".bold(),
        path,
        v1,
        v2,
    );
    println!("{}", "─".repeat(60));

    for op in &result.ops {
        match op {
            lake_core::diff::DiffOp::Keep {
                hash,
                offset_a,
                size,
                ..
            } => {
                println!(
                    "  {} {:>10} {:>10}  b3:{}",
                    "KEEP".green(),
                    format_bytes(*size),
                    format!("@{}", offset_a).dimmed(),
                    &hash.to_hex()[..12].to_string().dimmed(),
                );
            }
            lake_core::diff::DiffOp::Delete {
                hash,
                offset_a,
                size,
            } => {
                println!(
                    "  {} {:>10} {:>10}  b3:{}",
                    " DEL".red(),
                    format_bytes(*size),
                    format!("@{}", offset_a).dimmed(),
                    &hash.to_hex()[..12].to_string().dimmed(),
                );
            }
            lake_core::diff::DiffOp::Insert {
                hash,
                offset_b,
                size,
            } => {
                println!(
                    "  {} {:>10} {:>10}  b3:{}",
                    " INS".yellow(),
                    format_bytes(*size),
                    format!("@{}", offset_b).dimmed(),
                    &hash.to_hex()[..12].to_string().dimmed(),
                );
            }
        }
    }

    println!("{}", "─".repeat(60));
    println!(
        "  Kept:     {} chunks ({})",
        result.summary.chunks_kept.to_string().green(),
        format_bytes(result.summary.bytes_kept).green(),
    );
    println!(
        "  Deleted:  {} chunks ({})",
        result.summary.chunks_deleted.to_string().red(),
        format_bytes(result.summary.bytes_deleted).red(),
    );
    println!(
        "  Inserted: {} chunks ({})",
        result.summary.chunks_inserted.to_string().yellow(),
        format_bytes(result.summary.bytes_inserted).yellow(),
    );
    println!(
        "  Similarity: {:.1}%",
        result.summary.similarity() * 100.0,
    );
}

fn cmd_rewind(history: &History, path: &str, version: u64) {
    match history.rewind(path, version) {
        Ok(()) => {
            println!(
                "{} {} rewound to v{}",
                "✓".green().bold(),
                path,
                version,
            );
        }
        Err(e) => {
            eprintln!("{} {}", "error:".red().bold(), e);
            process::exit(1);
        }
    }
}

fn cmd_stats(history: &History, path: &str) {
    let stats = match history.storage_stats(path) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("{} {}", "error:".red().bold(), e);
            process::exit(1);
        }
    };

    println!("{} {}", "Storage Stats:".bold(), path);
    println!("{}", "─".repeat(40));
    println!("  Versions:        {}", stats.version_count.to_string().white().bold());
    println!("  Unique chunks:   {}", stats.unique_chunks);
    println!("  Chunk refs:      {}", stats.total_chunk_refs);
    println!(
        "  Naive storage:   {}",
        format_bytes(stats.naive_total_bytes).dimmed(),
    );
    println!(
        "  Actual storage:  {}",
        format_bytes(stats.actual_bytes).green().bold(),
    );
    println!(
        "  Dedup ratio:     {}",
        format!("{:.1}%", stats.dedup_ratio() * 100.0).cyan().bold(),
    );
    println!(
        "  Space saved:     {}",
        format_bytes(
            stats.naive_total_bytes.saturating_sub(stats.actual_bytes)
        )
        .green(),
    );
}

fn cmd_restore(paths: &PathTable, path: &str) {
    match paths.restore(path) {
        Ok(()) => {
            println!("{} {} restored", "✓".green().bold(), path);
        }
        Err(e) => {
            eprintln!("{} {}", "error:".red().bold(), e);
            process::exit(1);
        }
    }
}

fn cmd_cat(history: &History, path: &str, version: u64) {
    let data = match history.read_at_version(path, version) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("{} {}", "error:".red().bold(), e);
            process::exit(1);
        }
    };

    // Write raw bytes to stdout — works for text and binary
    use std::io::Write;
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    if let Err(e) = out.write_all(&data) {
        eprintln!("{} {}", "error:".red().bold(), e);
        process::exit(1);
    }
}

// ─── Helpers ─────────────────────────────────────────────────────

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

fn format_timestamp(millis: u64) -> String {
    let secs = millis / 1000;
    let days = secs / 86400;
    let day_secs = secs % 86400;
    let h = day_secs / 3600;
    let m = (day_secs % 3600) / 60;
    let s = day_secs % 60;
    let date = epoch_days_to_date(days);
    format!("{} {:02}:{:02}:{:02}", date, h, m, s)
}

fn epoch_days_to_date(days: u64) -> String {
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
