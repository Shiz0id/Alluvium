use crate::types::{B3Hash, ChunkRef, Manifest};

// ─── Diff Operations ─────────────────────────────────────────────

/// A single operation in the positional diff between two manifests.
/// Describes what happened at each position in the file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiffOp {
    /// Chunk exists in both versions at this position. Unchanged.
    Keep {
        hash: B3Hash,
        offset_a: u64,
        offset_b: u64,
        size: u64,
    },
    /// Chunk exists only in version A. Removed in B.
    Delete {
        hash: B3Hash,
        offset_a: u64,
        size: u64,
    },
    /// Chunk exists only in version B. Added since A.
    Insert {
        hash: B3Hash,
        offset_b: u64,
        size: u64,
    },
}

// ─── Diff Summary ────────────────────────────────────────────────

/// High-level summary of what changed between two versions.
#[derive(Debug, Clone)]
pub struct DiffSummary {
    pub chunks_kept: usize,
    pub chunks_deleted: usize,
    pub chunks_inserted: usize,
    pub bytes_kept: u64,
    pub bytes_deleted: u64,
    pub bytes_inserted: u64,
}

impl DiffSummary {
    /// How much of version A was retained in version B, as a ratio 0.0–1.0.
    pub fn similarity(&self) -> f64 {
        let total = self.bytes_kept + self.bytes_deleted;
        if total == 0 {
            return 1.0; // both empty
        }
        self.bytes_kept as f64 / total as f64
    }
}

// ─── Diff Result ─────────────────────────────────────────────────

/// The full diff between two manifest versions.
#[derive(Debug)]
pub struct DiffResult {
    pub from: B3Hash,
    pub to: B3Hash,
    pub ops: Vec<DiffOp>,
    pub summary: DiffSummary,
}

// ─── Diff Engine ─────────────────────────────────────────────────

/// Compute a positional diff between two manifests.
///
/// Uses a longest common subsequence (LCS) over the chunk hash sequences
/// to determine which chunks are kept, deleted, and inserted, preserving
/// byte offset information for both versions.
///
/// Operates entirely on manifest data — never touches the object store.
pub fn diff(a: &Manifest, b: &Manifest) -> DiffResult {
    let lcs = lcs_table(&a.chunks, &b.chunks);
    let ops = build_ops(&a.chunks, &b.chunks, &lcs);

    let mut summary = DiffSummary {
        chunks_kept: 0,
        chunks_deleted: 0,
        chunks_inserted: 0,
        bytes_kept: 0,
        bytes_deleted: 0,
        bytes_inserted: 0,
    };

    for op in &ops {
        match op {
            DiffOp::Keep { size, .. } => {
                summary.chunks_kept += 1;
                summary.bytes_kept += size;
            }
            DiffOp::Delete { size, .. } => {
                summary.chunks_deleted += 1;
                summary.bytes_deleted += size;
            }
            DiffOp::Insert { size, .. } => {
                summary.chunks_inserted += 1;
                summary.bytes_inserted += size;
            }
        }
    }

    DiffResult {
        from: a.hash,
        to: b.hash,
        ops,
        summary,
    }
}

// ─── LCS (Longest Common Subsequence) ────────────────────────────

/// Standard LCS dynamic programming table over chunk ref sequences.
/// Compares by hash only — size is carried along for offset calculation.
fn lcs_table(a: &[ChunkRef], b: &[ChunkRef]) -> Vec<Vec<u16>> {
    let m = a.len();
    let n = b.len();
    let mut dp = vec![vec![0u16; n + 1]; m + 1];

    for i in 1..=m {
        for j in 1..=n {
            if a[i - 1].hash == b[j - 1].hash {
                dp[i][j] = dp[i - 1][j - 1] + 1;
            } else {
                dp[i][j] = dp[i - 1][j].max(dp[i][j - 1]);
            }
        }
    }

    dp
}

/// Walk the LCS table backwards to produce an ordered list of diff operations
/// with byte offsets into both versions.
fn build_ops(
    a: &[ChunkRef],
    b: &[ChunkRef],
    dp: &[Vec<u16>],
) -> Vec<DiffOp> {
    let mut ops = Vec::new();
    let mut i = a.len();
    let mut j = b.len();
    let mut offset_a: u64 = a.iter().map(|c| c.size).sum();
    let mut offset_b: u64 = b.iter().map(|c| c.size).sum();

    while i > 0 || j > 0 {
        if i > 0 && j > 0 && a[i - 1].hash == b[j - 1].hash {
            let size = a[i - 1].size;
            offset_a -= size;
            offset_b -= size;
            ops.push(DiffOp::Keep {
                hash: a[i - 1].hash,
                offset_a,
                offset_b,
                size,
            });
            i -= 1;
            j -= 1;
        } else if j > 0 && (i == 0 || dp[i][j - 1] >= dp[i - 1][j]) {
            let size = b[j - 1].size;
            offset_b -= size;
            ops.push(DiffOp::Insert {
                hash: b[j - 1].hash,
                offset_b,
                size,
            });
            j -= 1;
        } else if i > 0 {
            let size = a[i - 1].size;
            offset_a -= size;
            ops.push(DiffOp::Delete {
                hash: a[i - 1].hash,
                offset_a,
                size,
            });
            i -= 1;
        }
    }

    ops.reverse();
    ops
}

// ─── Tests ───────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Chunk;

    fn make_chunk(data: &[u8]) -> Chunk {
        Chunk {
            hash: B3Hash::from_bytes(data),
            size: data.len() as u64,
        }
    }

    #[test]
    fn identical_manifests() {
        let c1 = make_chunk(b"aaa");
        let c2 = make_chunk(b"bbb");
        let m = Manifest::from_chunks(&[c1, c2]);

        let result = diff(&m, &m);
        assert_eq!(result.summary.chunks_kept, 2);
        assert_eq!(result.summary.chunks_deleted, 0);
        assert_eq!(result.summary.chunks_inserted, 0);
        assert!((result.summary.similarity() - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn one_chunk_changed() {
        let c1 = make_chunk(b"aaa");
        let c2 = make_chunk(b"bbb");
        let c3 = make_chunk(b"ccc");

        let ma = Manifest::from_chunks(&[c1.clone(), c2]);
        let mb = Manifest::from_chunks(&[c1, c3]);

        let result = diff(&ma, &mb);
        assert_eq!(result.summary.chunks_kept, 1);
        assert_eq!(result.summary.chunks_deleted, 1);
        assert_eq!(result.summary.chunks_inserted, 1);
    }

    #[test]
    fn chunk_inserted_in_middle() {
        let c1 = make_chunk(b"aaa");
        let c2 = make_chunk(b"bbb");
        let c_new = make_chunk(b"new");

        let ma = Manifest::from_chunks(&[c1.clone(), c2.clone()]);
        let mb = Manifest::from_chunks(&[c1, c_new, c2]);

        let result = diff(&ma, &mb);
        assert_eq!(result.summary.chunks_kept, 2);
        assert_eq!(result.summary.chunks_deleted, 0);
        assert_eq!(result.summary.chunks_inserted, 1);
    }

    #[test]
    fn completely_different() {
        let c1 = make_chunk(b"aaa");
        let c2 = make_chunk(b"bbb");
        let c3 = make_chunk(b"ccc");
        let c4 = make_chunk(b"ddd");

        let ma = Manifest::from_chunks(&[c1, c2]);
        let mb = Manifest::from_chunks(&[c3, c4]);

        let result = diff(&ma, &mb);
        assert_eq!(result.summary.chunks_kept, 0);
        assert_eq!(result.summary.chunks_deleted, 2);
        assert_eq!(result.summary.chunks_inserted, 2);
        assert!(result.summary.similarity() < f64::EPSILON);
    }

    #[test]
    fn offsets_are_correct() {
        let c1 = make_chunk(b"aaaa");  // 4 bytes
        let c2 = make_chunk(b"bbbb");  // 4 bytes
        let c3 = make_chunk(b"cccc");  // 4 bytes

        let ma = Manifest::from_chunks(&[c1.clone(), c2.clone(), c3.clone()]);
        let mb = Manifest::from_chunks(&[c1, c3]);

        let result = diff(&ma, &mb);

        for op in &result.ops {
            match op {
                DiffOp::Keep { hash, offset_a, offset_b, .. } => {
                    if *hash == B3Hash::from_bytes(b"aaaa") {
                        assert_eq!(*offset_a, 0);
                        assert_eq!(*offset_b, 0);
                    }
                    if *hash == B3Hash::from_bytes(b"cccc") {
                        assert_eq!(*offset_a, 8);
                        assert_eq!(*offset_b, 4);
                    }
                }
                DiffOp::Delete { hash, offset_a, .. } => {
                    assert_eq!(*hash, B3Hash::from_bytes(b"bbbb"));
                    assert_eq!(*offset_a, 4);
                }
                _ => {}
            }
        }
    }
}
