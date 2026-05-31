//! Retrieval query input, result types, and fusion logic (plan §8).

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::kind::DocKind;

/// Optional filters applied in the SQL `WHERE` before fusion (plan §8).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct QueryFilters {
    /// Restrict to these document kinds (empty = any).
    pub kinds: Vec<DocKind>,
    /// Restrict to documents carrying all of these canonical tag names (empty = any).
    pub tags: Vec<String>,
    /// Only documents created at/after this time.
    pub created_after: Option<DateTime<Utc>>,
    /// Only documents created at/before this time.
    pub created_before: Option<DateTime<Utc>>,
}

/// A retrieval request: free text plus filters and a result cap.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Query {
    /// The user's natural-language query.
    pub text: String,
    /// Structured filters.
    pub filters: QueryFilters,
    /// Maximum number of documents to return after roll-up/rerank.
    pub top_k: usize,
}

/// A single retrieval result: one document, with the winning chunk's provenance so the UI can
/// deep-link to the exact page or moment (plan §8).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Hit {
    /// The matched document.
    pub document_id: i64,
    /// Fused/rerank relevance score (higher = better).
    pub score: f32,
    /// Document title, if set.
    pub title: Option<String>,
    /// A snippet from the best-matching chunk.
    pub snippet: String,
    /// File/page that produced the winning chunk (deep-link target).
    pub file_id: i64,
    /// Page number of the winning chunk, if applicable.
    pub page_no: Option<i32>,
    /// Seconds offset of the winning chunk for audio/video, if applicable.
    pub ts_offset: Option<f64>,
    /// Document [`DocKind`](crate::kind::DocKind) as the database string,
    /// if available from the search query.
    pub kind: Option<String>,
}

// ── ChunkHit — intermediate between DB search and document rollup ────────────

/// A chunk-level hit with full provenance, used as the intermediate representation between
/// database-ranked chunk lists and document rollup (plan §8 step 4).
///
/// Unlike [`Hit`], which represents one document after rollup, `ChunkHit` carries per-chunk
/// data including the chunk id and its fused relevance score.
#[derive(Debug, Clone, PartialEq)]
pub struct ChunkHit {
    /// Chunk primary key (`chunks.id`).
    pub chunk_id: i64,
    /// Parent document (rollup groups by this).
    pub document_id: i64,
    /// Fused relevance score — higher is better.
    pub score: f32,
    /// Source file (deep-link provenance).
    pub file_id: i64,
    /// Page number, if the chunk came from a paginated document.
    pub page_no: Option<i32>,
    /// Seconds offset for audio/video transcript chunks.
    pub ts_offset: Option<f64>,
    /// Chunk content used as the result snippet.
    pub snippet: String,
    /// Document title, if set.
    pub title: Option<String>,
    /// Document [`DocKind`](crate::kind::DocKind) as the database string,
    /// if available from the search query.
    pub kind: Option<String>,
}

// ── RRF fusion + document rollup ─────────────────────────────────────────────

/// Default RRF smoothing constant (plan §8: k≈60).
pub const RRF_K: u32 = 60;

/// Fuse two ranked lists of chunk IDs using Reciprocal Rank Fusion (plan §8 step 3).
///
/// The RRF formula is `Σ 1/(k + rank_i)` where `rank_i` is the **1-based** position of the
/// chunk in list *i* (vector or keyword). A chunk appearing in both lists receives
/// contributions from both, making it rank higher.
///
/// `vec_ranked` and `kw_ranked` are chunk IDs ordered by descending relevance (best first).
/// When one list is empty, results from the other list pass through with their RRF scores.
///
/// Returns `(chunk_id, fused_score)` pairs sorted by score descending. Ties are resolved by
/// chunk id (lower id first) for deterministic output.
pub fn rrf_fuse(vec_ranked: &[i64], kw_ranked: &[i64], k: u32) -> Vec<(i64, f32)> {
    let mut scores: HashMap<i64, f32> = HashMap::with_capacity(vec_ranked.len() + kw_ranked.len());
    let k = k as f32;

    // Vector list contribution.
    for (rank, &chunk_id) in vec_ranked.iter().enumerate() {
        let rrf_score = 1.0 / (k + (rank as u32 + 1) as f32);
        let entry = scores.entry(chunk_id).or_insert(0.0);
        *entry += rrf_score;
    }

    // Keyword list contribution.
    for (rank, &chunk_id) in kw_ranked.iter().enumerate() {
        let rrf_score = 1.0 / (k + (rank as u32 + 1) as f32);
        let entry = scores.entry(chunk_id).or_insert(0.0);
        *entry += rrf_score;
    }

    let mut result: Vec<(i64, f32)> = scores.into_iter().collect();
    // Sort by score descending, tie-break by chunk_id ascending for determinism.
    result.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });
    result
}

/// Roll chunk-level hits up to one [`Hit`] per document (plan §8 step 4).
///
/// Chunks belonging to the same document are merged into a single `Hit` that keeps the
/// **highest-scoring** chunk's provenance (`file_id`, `page_no`, `ts_offset`, `snippet`) as
/// the deep-link target. The document's `title` is taken from the winning chunk if available.
///
/// Results are sorted by score descending. An empty input yields an empty `Vec`.
pub fn document_rollup(chunk_hits: &[ChunkHit]) -> Vec<Hit> {
    let mut best: HashMap<i64, &ChunkHit> = HashMap::with_capacity(chunk_hits.len());

    for ch in chunk_hits {
        let entry = best.entry(ch.document_id).or_insert(ch);
        if ch.score > entry.score {
            *entry = ch;
        }
    }

    let mut hits: Vec<Hit> = best
        .values()
        .map(|ch| Hit {
            document_id: ch.document_id,
            score: ch.score,
            title: ch.title.clone(),
            snippet: ch.snippet.clone(),
            file_id: ch.file_id,
            page_no: ch.page_no,
            ts_offset: ch.ts_offset,
            kind: ch.kind.clone(),
        })
        .collect();

    hits.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.document_id.cmp(&b.document_id))
    });
    hits
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    // ── rrf_fuse ──────────────────────────────────────────────────────────

    #[test]
    fn rrf_two_equal_lists() {
        // Same chunks in same order → each gets contribution from both lists.
        let vec_list = [10, 20, 30];
        let kw_list = [10, 20, 30];
        let fused = rrf_fuse(&vec_list, &kw_list, RRF_K);

        // chunk 10: 1/(60+1) + 1/(60+1) = 2/61 ≈ 0.032787
        // chunk 20: 1/(60+2) + 1/(60+2) = 2/62 ≈ 0.032258
        // chunk 30: 1/(60+3) + 1/(60+3) = 2/63 ≈ 0.031746
        assert_eq!(fused.len(), 3);
        // Sorted by score descending.
        assert_eq!(fused[0].0, 10);
        assert_eq!(fused[1].0, 20);
        assert_eq!(fused[2].0, 30);
        assert!((fused[0].1 - 2.0 / 61.0).abs() < 1e-6);
        assert!((fused[1].1 - 2.0 / 62.0).abs() < 1e-6);
        assert!((fused[2].1 - 2.0 / 63.0).abs() < 1e-6);
    }

    #[test]
    fn rrf_empty_keyword_list_passes_through() {
        let vec_list = [5, 10, 15];
        let fused = rrf_fuse(&vec_list, &[], RRF_K);

        assert_eq!(fused.len(), 3);
        assert_eq!(fused[0].0, 5);
        assert_eq!(fused[1].0, 10);
        assert_eq!(fused[2].0, 15);
        // Scores descend because rank 1 > rank 2 > rank 3.
        assert!(fused[0].1 > fused[1].1);
        assert!(fused[1].1 > fused[2].1);
    }

    #[test]
    fn rrf_empty_vector_list_passes_through() {
        let kw_list = [100, 200];
        let fused = rrf_fuse(&[], &kw_list, RRF_K);

        assert_eq!(fused.len(), 2);
        assert_eq!(fused[0].0, 100);
        assert_eq!(fused[1].0, 200);
    }

    #[test]
    fn rrf_both_empty() {
        let fused: Vec<(i64, f32)> = rrf_fuse(&[], &[], RRF_K);
        assert!(fused.is_empty());
    }

    #[test]
    fn rrf_single_item_each() {
        let fused = rrf_fuse(&[42], &[42], RRF_K);
        assert_eq!(fused.len(), 1);
        assert_eq!(fused[0].0, 42);
        // 1/(60+1) + 1/(60+1) = 2/61
        let expected = 2.0 / 61.0_f32;
        assert!((fused[0].1 - expected).abs() < 1e-6);
    }

    #[test]
    fn rrf_overlapping_chunks_get_combined_score() {
        // Chunk 1 appears in vec only, chunk 2 in kw only, chunk 3 in both.
        let vec_list = [1, 3];
        let kw_list = [2, 3];
        let fused = rrf_fuse(&vec_list, &kw_list, RRF_K);

        assert_eq!(fused.len(), 3);

        // chunk 3 should have highest score (appears in both lists).
        assert_eq!(fused[0].0, 3);
        let expected_3 = 1.0 / 62.0_f32 + 1.0 / 62.0_f32; // rank 2 in both
        assert!((fused[0].1 - expected_3).abs() < 1e-6);

        // chunk 1: only vec, rank 1 → 1/(60+1)
        // chunk 2: only kw, rank 1 → 1/(60+1)
        // They tie → chunk_id tie-break (1 < 2).
        let matching: Vec<_> = fused
            .iter()
            .filter(|(id, _)| *id == 1 || *id == 2)
            .collect();
        assert_eq!(matching.len(), 2);
    }

    #[test]
    fn rrf_different_lengths() {
        let vec_list = [10, 20, 30, 40];
        let kw_list = [15, 25];
        let fused = rrf_fuse(&vec_list, &kw_list, RRF_K);

        // All 4 + 2 = 6 unique chunk ids should appear.
        assert_eq!(fused.len(), 6);
        // Every chunk id from both lists appears.
        for &id in &[10, 20, 30, 40, 15, 25] {
            assert!(
                fused.iter().any(|(fid, _)| *fid == id),
                "missing chunk {id}"
            );
        }
    }

    #[test]
    fn rrf_custom_k() {
        let vec_list = [1, 2];
        let kw_list = [1];
        let k_small = 0;
        let fused = rrf_fuse(&vec_list, &kw_list, k_small);

        // chunk 1: rank 1 vec + rank 1 kw = 1/1 + 1/1 = 2.0
        // chunk 2: rank 2 vec = 1/2 = 0.5
        assert_eq!(fused[0].0, 1);
        assert!((fused[0].1 - 2.0).abs() < 1e-6);
        assert_eq!(fused[1].0, 2);
        assert!((fused[1].1 - 0.5).abs() < 1e-6);
    }

    #[test]
    fn rrf_ties_broken_by_id() {
        // Two chunks that appear only in their respective lists at the same rank → same score.
        let vec_list = [200];
        let kw_list = [100];
        let fused = rrf_fuse(&vec_list, &kw_list, RRF_K);

        assert_eq!(fused.len(), 2);
        // Same score → lower chunk_id first.
        assert_eq!(fused[0].0, 100);
        assert_eq!(fused[1].0, 200);
        assert!((fused[0].1 - fused[1].1).abs() < 1e-6);
    }

    // ── document_rollup ────────────────────────────────────────────────────

    fn make_chunk(
        chunk_id: i64,
        document_id: i64,
        score: f32,
        file_id: i64,
        page_no: Option<i32>,
        snippet: &str,
    ) -> ChunkHit {
        ChunkHit {
            chunk_id,
            document_id,
            score,
            file_id,
            page_no,
            ts_offset: None,
            snippet: snippet.to_string(),
            title: Some(format!("Doc {document_id}")),
            kind: Some("document".to_string()),
        }
    }

    #[test]
    fn rollup_two_chunks_same_doc_best_wins() {
        let chunks = [
            make_chunk(1, 10, 0.8, 100, Some(1), "page one snippet"),
            make_chunk(2, 10, 0.9, 200, Some(2), "page two snippet"),
        ];
        let hits = document_rollup(&chunks);

        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].document_id, 10);
        assert_eq!(hits[0].score, 0.9);
        // Best chunk provenance: chunk 2.
        assert_eq!(hits[0].file_id, 200);
        assert_eq!(hits[0].page_no, Some(2));
        assert_eq!(hits[0].snippet, "page two snippet");
        assert_eq!(hits[0].title.as_deref(), Some("Doc 10"));
    }

    #[test]
    fn rollup_different_docs_one_hit_per_doc() {
        let chunks = [
            make_chunk(1, 10, 0.9, 100, Some(1), "doc10"),
            make_chunk(2, 20, 0.8, 200, None, "doc20"),
            make_chunk(3, 30, 0.7, 300, Some(3), "doc30"),
        ];
        let hits = document_rollup(&chunks);

        assert_eq!(hits.len(), 3);
        // Sorted by score descending.
        assert_eq!(hits[0].document_id, 10);
        assert_eq!(hits[1].document_id, 20);
        assert_eq!(hits[2].document_id, 30);
    }

    #[test]
    fn rollup_empty() {
        let hits = document_rollup(&[]);
        assert!(hits.is_empty());
    }

    #[test]
    fn rollup_single_chunk() {
        let chunks = [make_chunk(1, 10, 0.75, 100, Some(5), "snippet")];
        let hits = document_rollup(&chunks);

        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].document_id, 10);
        assert_eq!(hits[0].score, 0.75);
        assert_eq!(hits[0].file_id, 100);
        assert_eq!(hits[0].page_no, Some(5));
        assert_eq!(hits[0].snippet, "snippet");
    }

    #[test]
    fn rollup_multiple_chunks_multiple_docs_mixed() {
        let chunks = [
            make_chunk(1, 10, 0.5, 100, Some(1), "10-low"),
            make_chunk(2, 20, 0.9, 200, Some(1), "20-high"),
            make_chunk(3, 10, 0.8, 101, Some(2), "10-high"),
            make_chunk(4, 20, 0.3, 201, Some(2), "20-low"),
            make_chunk(5, 30, 0.7, 300, None, "30-only"),
        ];
        let hits = document_rollup(&chunks);

        assert_eq!(hits.len(), 3);
        // doc 20: best score 0.9 (chunk 2)
        assert_eq!(hits[0].document_id, 20);
        assert_eq!(hits[0].score, 0.9);
        assert_eq!(hits[0].file_id, 200);
        assert_eq!(hits[0].snippet, "20-high");
        // doc 10: best score 0.8 (chunk 3)
        assert_eq!(hits[1].document_id, 10);
        assert_eq!(hits[1].score, 0.8);
        assert_eq!(hits[1].file_id, 101);
        assert_eq!(hits[1].snippet, "10-high");
        // doc 30: 0.7
        assert_eq!(hits[2].document_id, 30);
        assert_eq!(hits[2].score, 0.7);
    }

    #[test]
    fn rollup_same_score_earlier_chunk_wins() {
        // Two chunks same doc, same score — first seen keeps provenance.
        let chunks = [
            make_chunk(10, 1, 0.5, 100, Some(1), "first"),
            make_chunk(20, 1, 0.5, 200, Some(2), "second"),
        ];
        let hits = document_rollup(&chunks);

        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].file_id, 100);
        assert_eq!(hits[0].snippet, "first");
    }

    #[test]
    fn rollup_ts_offset_preserved() {
        let chunks = [ChunkHit {
            chunk_id: 1,
            document_id: 10,
            score: 0.9,
            file_id: 100,
            page_no: None,
            ts_offset: Some(12.5),
            snippet: "audio snippet".into(),
            title: None,
            kind: Some("audio".into()),
        }];
        let hits = document_rollup(&chunks);

        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].ts_offset, Some(12.5));
    }

    #[test]
    fn rollup_title_none_when_chunk_has_none() {
        let chunks = [ChunkHit {
            chunk_id: 1,
            document_id: 10,
            score: 0.5,
            file_id: 100,
            page_no: None,
            ts_offset: None,
            snippet: "s".into(),
            title: None,
            kind: Some("document".into()),
        }];
        let hits = document_rollup(&chunks);
        assert_eq!(hits[0].title, None);
    }
}
