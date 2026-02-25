use std::process;

use clap::{Parser, Subcommand};
use colored::*;

use lake_core::history::History;
use lake_core::path_table::PathTable;
use lake_core::store::ObjectStore;
use lake_core::types::VersionTrigger;

// ─── CLI Definition ──────────────────────────────────────────────

#[derive(Parser)]
#[command(name = "lakectl")]
#[command(about = "DeepLake filesystem control tool")]
#[command(version)]
struct Cli {
    /// Path to the lake storage directory
    #[arg(long, default_value = ".lake", global = true)]
    lake: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
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
}

// ─── Main ────────────────────────────────────────────────────────

fn main() {
    let cli = Cli::parse();

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
        Commands::History { path } => cmd_history(&history, &path),
        Commands::Diff { path, v1, v2 } => cmd_diff(&history, &path, v1, v2),
        Commands::Rewind { path, version } => cmd_rewind(&history, &path, version),
        Commands::Stats { path } => cmd_stats(&history, &path),
        Commands::Restore { path } => cmd_restore(&paths, &path),
        Commands::Cat { path, version } => cmd_cat(&history, &path, version),
    }
}

// ─── Commands ────────────────────────────────────────────────────

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
