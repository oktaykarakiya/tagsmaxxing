//! Retrieval eval harness: labelled queries, metrics computation (recall@k, MRR),
//! and a regression-check baseline (plan §19, P6-T12).
//!
//! The integration test in `crates/pipeline/tests/eval_integration.rs` ingests
//! a deterministic fixture corpus into a real pgvector Postgres via testcontainers,
//! runs the full [`RetrievalPipeline`](crate::retrieval::RetrievalPipeline), and
//! asserts that recall@k / MRR do not regress below a committed baseline.
//!
//! # Baseline
//!
//! The baseline lives in `crates/pipeline/tests/fixtures/eval_baseline.json`.
//! When retrieval-quality changes are intentional (e.g. tuning RRF weights or
//! `TAG_MERGE_THRESHOLD`), update the baseline by editing that file and
//! documenting the reason in the `note` field.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

// ── Types ──────────────────────────────────────────────────────────────────────

/// A single labelled query: the query text and the set of document ids that are
/// considered relevant (the ground-truth label).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LabeledQuery {
    /// Natural-language query text.
    pub text: String,
    /// Document ids that should appear in the top results.
    pub expected_ids: Vec<i64>,
}

/// Aggregated evaluation metrics over a set of labelled queries.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EvalMetrics {
    /// Recall@1: fraction of relevant documents found in the very first result,
    /// averaged across all queries. 1.0 means every query's top result is relevant.
    pub recall_at_1: f64,
    /// Recall@3: fraction of relevant documents found in the top 3 results,
    /// averaged across all queries.
    pub recall_at_3: f64,
    /// Recall@5: fraction of relevant documents found in the top 5 results,
    /// averaged across all queries.
    pub recall_at_5: f64,
    /// Mean Reciprocal Rank: `1 / rank_of_first_relevant` averaged across all
    /// queries. Higher is better; 1.0 means every first result is relevant.
    pub mrr: f64,
}

/// A committed baseline used for regression detection in CI (plan §19).
///
/// The integration test loads this file (or an in-code constant) and fails
/// if any computed metric drops below the baseline value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EvalBaseline {
    /// The baseline metrics — the minimum acceptable values.
    pub metrics: EvalMetrics,
    /// Human-readable note describing when/why this baseline was set.
    pub note: String,
}

// ── Pure metric functions ──────────────────────────────────────────────────────

/// Compute recall@k for a single query.
///
/// Returns the fraction of `expected` ids that appear in the first `k`
/// ranked `retrieved` ids. If `expected` is empty, returns 1.0 (vacuous
/// truth — there are no relevant documents to miss).
///
/// # Examples
///
/// ```
/// use std::collections::HashSet;
/// use kb_pipeline::eval::recall_at_k;
///
/// let retrieved = [10, 20, 30];
/// let expected: HashSet<i64> = [10, 30].into();
/// assert!((recall_at_k(&retrieved, &expected, 2) - 0.5).abs() < 0.001);
/// assert!((recall_at_k(&retrieved, &expected, 3) - 1.0).abs() < 0.001);
/// ```
#[must_use]
pub fn recall_at_k(retrieved: &[i64], expected: &HashSet<i64>, k: usize) -> f64 {
    if expected.is_empty() {
        return 1.0;
    }
    let found = retrieved
        .iter()
        .take(k)
        .filter(|id| expected.contains(id))
        .count();
    found as f64 / expected.len() as f64
}

/// Compute the reciprocal rank for a single query: `1 / (1-based rank)` of
/// the first relevant result, or 0.0 if no relevant result is found.
///
/// # Examples
///
/// ```
/// use std::collections::HashSet;
/// use kb_pipeline::eval::reciprocal_rank;
///
/// let retrieved = [10, 20, 30];
/// let expected: HashSet<i64> = [20].into();
/// assert!((reciprocal_rank(&retrieved, &expected) - 0.5).abs() < 0.001);
/// ```
#[must_use]
pub fn reciprocal_rank(retrieved: &[i64], expected: &HashSet<i64>) -> f64 {
    for (rank, id) in retrieved.iter().enumerate() {
        if expected.contains(id) {
            return 1.0 / (rank as f64 + 1.0);
        }
    }
    0.0
}

/// Compute aggregated [`EvalMetrics`] from all labelled queries and their
/// corresponding retrieved document-id lists.
///
/// `queries` and `retrieved` must have the same length — each query maps to
/// one `Vec<i64>` of document ids in ranked order.
///
/// # Panics
///
/// Panics if `queries.len() != retrieved.len()`.
///
/// # Examples
///
/// ```
/// use kb_pipeline::eval::{LabeledQuery, EvalMetrics, compute_metrics};
///
/// let queries = vec![
///     LabeledQuery { text: "a".into(), expected_ids: vec![1, 2] },
///     LabeledQuery { text: "b".into(), expected_ids: vec![3] },
/// ];
/// let results = vec![
///     vec![1, 10, 2],  // first relevant at rank 1, both found in top 3
///     vec![4, 3],       // first relevant at rank 2
/// ];
/// let m = compute_metrics(&queries, &results);
/// assert!((m.recall_at_1 - 0.25).abs() < 0.001);  // avg of (0.5, 0.0)
/// assert!((m.recall_at_3 - 1.0).abs() < 0.001);   // avg of (1.0, 1.0)
/// assert!(m.mrr > 0.5);                            // avg of (1.0, 0.5) = 0.75
/// ```
#[must_use]
pub fn compute_metrics(queries: &[LabeledQuery], retrieved: &[Vec<i64>]) -> EvalMetrics {
    assert_eq!(
        queries.len(),
        retrieved.len(),
        "each query must have exactly one result list"
    );

    if queries.is_empty() {
        return EvalMetrics {
            recall_at_1: 1.0,
            recall_at_3: 1.0,
            recall_at_5: 1.0,
            mrr: 1.0,
        };
    }

    let n = queries.len() as f64;
    let mut sum_r1 = 0.0_f64;
    let mut sum_r3 = 0.0_f64;
    let mut sum_r5 = 0.0_f64;
    let mut sum_rr = 0.0_f64;

    for (q, ret) in queries.iter().zip(retrieved.iter()) {
        let expected: HashSet<i64> = q.expected_ids.iter().copied().collect();
        sum_r1 += recall_at_k(ret, &expected, 1);
        sum_r3 += recall_at_k(ret, &expected, 3);
        sum_r5 += recall_at_k(ret, &expected, 5);
        sum_rr += reciprocal_rank(ret, &expected);
    }

    EvalMetrics {
        recall_at_1: sum_r1 / n,
        recall_at_3: sum_r3 / n,
        recall_at_5: sum_r5 / n,
        mrr: sum_rr / n,
    }
}

/// Check whether computed metrics regress below a baseline, returning a
/// list of human-readable regression messages (one per metric that is
/// below the baseline). An empty `Vec` means all metrics meet or exceed
/// the baseline — no regression.
///
/// # Examples
///
/// ```
/// use kb_pipeline::eval::{EvalMetrics, EvalBaseline, check_regression};
///
/// let baseline = EvalBaseline {
///     metrics: EvalMetrics { recall_at_1: 0.5, recall_at_3: 0.5, recall_at_5: 0.5, mrr: 0.4 },
///     note: "example".into(),
/// };
/// let actual   = EvalMetrics { recall_at_1: 0.6, recall_at_3: 0.4, recall_at_5: 0.5, mrr: 0.5 };
/// let regressions = check_regression(&actual, &baseline);
/// assert_eq!(regressions.len(), 1); // recall_at_3 dropped
/// assert!(regressions[0].contains("recall@3"));
/// ```
#[must_use]
pub fn check_regression(actual: &EvalMetrics, baseline: &EvalBaseline) -> Vec<String> {
    let mut msgs = Vec::new();
    let b = &baseline.metrics;

    if actual.recall_at_1 < b.recall_at_1 - 0.001 {
        msgs.push(format!(
            "recall@1 regressed: {:.4} < baseline {:.4}",
            actual.recall_at_1, b.recall_at_1
        ));
    }
    if actual.recall_at_3 < b.recall_at_3 - 0.001 {
        msgs.push(format!(
            "recall@3 regressed: {:.4} < baseline {:.4}",
            actual.recall_at_3, b.recall_at_3
        ));
    }
    if actual.recall_at_5 < b.recall_at_5 - 0.001 {
        msgs.push(format!(
            "recall@5 regressed: {:.4} < baseline {:.4}",
            actual.recall_at_5, b.recall_at_5
        ));
    }
    if actual.mrr < b.mrr - 0.001 {
        msgs.push(format!(
            "MRR regressed: {:.4} < baseline {:.4}",
            actual.mrr, b.mrr
        ));
    }

    msgs
}

// ── Fixture corpus ─────────────────────────────────────────────────────────────

/// One fixture document: title, kind, chunk content, and an optional tag.
#[derive(Debug, Clone)]
pub struct FixtureDoc {
    /// Document title (also used as a keyword source).
    pub title: &'static str,
    /// Document kind (maps to PostgreSQL `documents.kind`).
    pub kind: &'static str,
    /// Chunk text content — the primary retrieval signal.
    pub content: &'static str,
    /// Optional canonical tag name.
    pub tag: Option<&'static str>,
}

/// The standard 8-document fixture corpus used by the eval harness.
///
/// Documents span four topics: Rust, machine learning, cooking, and history.
/// Each document has a distinctive keyword or phrase so keyword-based search
/// can differentiate them. All chunks share the same spike embedding (`spike(0)`)
/// so vector search returns a flat signal — the eval primarily measures keyword
/// search through the full retrieval pipeline (RRF fusion + document rollup).
///
/// The corpus is deliberately small so CI runs quickly while still providing
/// a meaningful regression signal.
#[must_use]
pub fn fixture_corpus() -> Vec<FixtureDoc> {
    vec![
        // ── Rust topic (docs 1–2) ──────────────────────────────────────────
        FixtureDoc {
            title: "Rust Ownership Guide",
            kind: "document",
            content: "Rust's ownership model ensures memory safety without garbage collection.",
            tag: Some("rust"),
        },
        FixtureDoc {
            title: "Async Programming in Rust",
            kind: "document",
            content: "Tokio runtime provides async I/O for Rust applications with high concurrency.",
            tag: Some("rust"),
        },
        // ── Machine learning topic (docs 3–4) ──────────────────────────────
        FixtureDoc {
            title: "Neural Network Basics",
            kind: "document",
            content: "Neural networks learn patterns through backpropagation and gradient descent.",
            tag: Some("machine-learning"),
        },
        FixtureDoc {
            title: "Deep Learning with PyTorch",
            kind: "document",
            content: "PyTorch provides tensor computation with strong GPU acceleration for deep learning.",
            tag: Some("machine-learning"),
        },
        // ── Cooking topic (docs 5–6) ───────────────────────────────────────
        FixtureDoc {
            title: "Italian Pasta Recipes",
            kind: "document",
            content: "Homemade pasta requires only flour, eggs, and olive oil for authentic Italian cuisine.",
            tag: Some("cooking"),
        },
        FixtureDoc {
            title: "Baking Sourdough Bread",
            kind: "document",
            content: "Sourdough bread fermentation develops complex flavors through natural yeast cultures.",
            tag: Some("cooking"),
        },
        // ── History topic (docs 7–8) ───────────────────────────────────────
        FixtureDoc {
            title: "Ancient Rome History",
            kind: "document",
            content: "The Roman Republic evolved into an empire spanning three continents over centuries.",
            tag: Some("history"),
        },
        FixtureDoc {
            title: "World War II Overview",
            kind: "document",
            content: "World War II involved global conflict between Allied and Axis powers from 1939 to 1945.",
            tag: Some("history"),
        },
    ]
}

/// The labelled eval queries (plan §19).
///
/// Each query targets one or more fixture documents by distinctive keywords.
/// The `expected_ids` field enumerates document ids (1-based, matching the
/// insertion order of [`fixture_corpus`]).
#[must_use]
pub fn eval_queries() -> Vec<LabeledQuery> {
    vec![
        LabeledQuery {
            text: "Rust memory safety".to_string(),
            expected_ids: vec![1, 2],
        },
        LabeledQuery {
            text: "neural network training".to_string(),
            expected_ids: vec![3, 4],
        },
        LabeledQuery {
            text: "homemade Italian food".to_string(),
            expected_ids: vec![5],
        },
        LabeledQuery {
            text: "sourdough fermentation".to_string(),
            expected_ids: vec![6],
        },
        LabeledQuery {
            text: "Roman empire history".to_string(),
            expected_ids: vec![7],
        },
        LabeledQuery {
            text: "global war 1939".to_string(),
            expected_ids: vec![8],
        },
        LabeledQuery {
            text: "deep learning GPU".to_string(),
            expected_ids: vec![4],
        },
    ]
}

// ── Helpers (public, used by integration tests) ───────────────────────────────

/// Produce a 1024-dim vector with a single 1.0 at `pos` (mod 1024),
/// matching the convention used in `p4_integration.rs`.
#[must_use]
pub fn spike_1024(pos: usize) -> Vec<f32> {
    let mut v = vec![0.0_f32; 1024];
    if !v.is_empty() {
        v[pos % 1024] = 1.0;
    }
    v
}

/// Format a `Vec<f32>` into the pgvector text representation `[x1,x2,…]`.
#[must_use]
pub fn format_vector(v: &[f32]) -> String {
    let mut buf = String::with_capacity(2 + v.len() * 16);
    buf.push('[');
    for (i, x) in v.iter().enumerate() {
        if i > 0 {
            buf.push(',');
        }
        use std::fmt::Write;
        let _ = write!(buf, "{x}");
    }
    buf.push(']');
    buf
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    // ── recall_at_k ─────────────────────────────────────────────────────────

    #[test]
    fn recall_empty_expected_returns_one() {
        let retrieved = [1, 2, 3];
        let expected = HashSet::new();
        assert!((recall_at_k(&retrieved, &expected, 3) - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn recall_all_found() {
        let retrieved = [10, 20, 30];
        let expected: HashSet<i64> = [10, 20].into();
        assert!((recall_at_k(&retrieved, &expected, 2) - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn recall_none_found() {
        let retrieved = [10, 20, 30];
        let expected: HashSet<i64> = [99].into();
        assert!((recall_at_k(&retrieved, &expected, 3) - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn recall_partial() {
        let retrieved = [10, 20, 30];
        let expected: HashSet<i64> = [10, 99].into();
        // Only 10 is found in top 2 → 0.5
        assert!((recall_at_k(&retrieved, &expected, 2) - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn recall_k_zero() {
        let retrieved = [10, 20];
        let expected: HashSet<i64> = [10].into();
        // k=0 means no items are considered → 0 found.
        assert!((recall_at_k(&retrieved, &expected, 0) - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn recall_k_exceeds_retrieved() {
        let retrieved = [1, 2];
        let expected: HashSet<i64> = [1, 2].into();
        // k=100 but only 2 items → both found.
        assert!((recall_at_k(&retrieved, &expected, 100) - 1.0).abs() < f64::EPSILON);
    }

    // ── reciprocal_rank ─────────────────────────────────────────────────────

    #[test]
    fn rr_first_is_relevant() {
        let retrieved = [10, 20, 30];
        let expected: HashSet<i64> = [10].into();
        assert!((reciprocal_rank(&retrieved, &expected) - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn rr_second_is_relevant() {
        let retrieved = [10, 20, 30];
        let expected: HashSet<i64> = [20].into();
        assert!((reciprocal_rank(&retrieved, &expected) - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn rr_third_is_relevant() {
        let retrieved = [10, 20, 30];
        let expected: HashSet<i64> = [30].into();
        let rr = reciprocal_rank(&retrieved, &expected);
        let expected_rr = 1.0 / 3.0;
        assert!((rr - expected_rr).abs() < 0.001);
    }

    #[test]
    fn rr_none_relevant() {
        let retrieved = [10, 20];
        let expected: HashSet<i64> = [99].into();
        assert!((reciprocal_rank(&retrieved, &expected) - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn rr_empty_retrieved() {
        let retrieved: [i64; 0] = [];
        let expected: HashSet<i64> = [1].into();
        assert!((reciprocal_rank(&retrieved, &expected) - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn rr_multiple_relevant_first_wins() {
        let retrieved = [10, 20, 30];
        let expected: HashSet<i64> = [10, 20].into();
        // First relevant is at rank 1 → RR = 1.0 (not average).
        assert!((reciprocal_rank(&retrieved, &expected) - 1.0).abs() < f64::EPSILON);
    }

    // ── compute_metrics ──────────────────────────────────────────────────────

    #[test]
    fn metrics_empty_queries() {
        let m = compute_metrics(&[], &[]);
        assert!((m.recall_at_1 - 1.0).abs() < f64::EPSILON);
        assert!((m.recall_at_3 - 1.0).abs() < f64::EPSILON);
        assert!((m.recall_at_5 - 1.0).abs() < f64::EPSILON);
        assert!((m.mrr - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn metrics_perfect() {
        let queries = vec![
            LabeledQuery {
                text: "a".into(),
                expected_ids: vec![1],
            },
            LabeledQuery {
                text: "b".into(),
                expected_ids: vec![2],
            },
        ];
        let results = vec![vec![1, 10], vec![2, 20]];
        let m = compute_metrics(&queries, &results);
        assert!((m.recall_at_1 - 1.0).abs() < f64::EPSILON);
        assert!((m.mrr - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn metrics_mixed() {
        let queries = vec![
            LabeledQuery {
                text: "a".into(),
                expected_ids: vec![1, 2],
            },
            LabeledQuery {
                text: "b".into(),
                expected_ids: vec![3],
            },
        ];
        // Query 0: retrieved [10, 1, 20, 2] → recall@1=0, recall@3=0.5, recall@5=1.0, RR=1/2=0.5
        // Query 1: retrieved [4, 5] → recall@1=0, recall@3=0, recall@5=0, RR=0
        let results = vec![vec![10, 1, 20, 2], vec![4, 5]];
        let m = compute_metrics(&queries, &results);
        assert!((m.recall_at_1 - 0.0).abs() < f64::EPSILON);
        // recall@3 avg: (0.5 + 0.0)/2 = 0.25
        assert!((m.recall_at_3 - 0.25).abs() < 0.001);
        // recall@5 avg: (1.0 + 0.0)/2 = 0.5
        assert!((m.recall_at_5 - 0.5).abs() < 0.001);
        // MRR: (0.5 + 0.0)/2 = 0.25
        assert!((m.mrr - 0.25).abs() < 0.001);
    }

    #[test]
    fn metrics_single_query_all_relevant() {
        let queries = vec![LabeledQuery {
            text: "x".into(),
            expected_ids: vec![1, 2, 3],
        }];
        let results = vec![vec![1, 2, 3]];
        let m = compute_metrics(&queries, &results);
        assert!((m.recall_at_1 - 1.0 / 3.0).abs() < 0.001);
        assert!((m.recall_at_3 - 1.0).abs() < f64::EPSILON);
        assert!((m.mrr - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    #[should_panic(expected = "each query must have exactly one result list")]
    fn mismatched_query_result_lengths_panic() {
        let queries = vec![LabeledQuery {
            text: "x".into(),
            expected_ids: vec![1],
        }];
        let results: Vec<Vec<i64>> = vec![];
        let _ = compute_metrics(&queries, &results);
    }

    // ── check_regression ────────────────────────────────────────────────────

    #[test]
    fn no_regression_when_all_above() {
        let baseline = EvalBaseline {
            metrics: EvalMetrics {
                recall_at_1: 0.5,
                recall_at_3: 0.5,
                recall_at_5: 0.5,
                mrr: 0.5,
            },
            note: "test".into(),
        };
        let actual = EvalMetrics {
            recall_at_1: 0.6,
            recall_at_3: 0.7,
            recall_at_5: 0.8,
            mrr: 0.9,
        };
        assert!(check_regression(&actual, &baseline).is_empty());
    }

    #[test]
    fn no_regression_when_equal() {
        let baseline = EvalBaseline {
            metrics: EvalMetrics {
                recall_at_1: 0.5,
                recall_at_3: 0.5,
                recall_at_5: 0.5,
                mrr: 0.5,
            },
            note: "test".into(),
        };
        let actual = EvalMetrics {
            recall_at_1: 0.5,
            recall_at_3: 0.5,
            recall_at_5: 0.5,
            mrr: 0.5,
        };
        assert!(check_regression(&actual, &baseline).is_empty());
    }

    #[test]
    fn regression_detected_single_metric() {
        let baseline = EvalBaseline {
            metrics: EvalMetrics {
                recall_at_1: 0.8,
                recall_at_3: 0.8,
                recall_at_5: 0.8,
                mrr: 0.8,
            },
            note: "test".into(),
        };
        let actual = EvalMetrics {
            recall_at_1: 0.9,
            recall_at_3: 0.7, // below baseline
            recall_at_5: 0.9,
            mrr: 0.9,
        };
        let regs = check_regression(&actual, &baseline);
        assert_eq!(regs.len(), 1);
        assert!(regs[0].contains("recall@3"));
    }

    #[test]
    fn regression_detected_multiple() {
        let baseline = EvalBaseline {
            metrics: EvalMetrics {
                recall_at_1: 0.8,
                recall_at_3: 0.8,
                recall_at_5: 0.8,
                mrr: 0.8,
            },
            note: "test".into(),
        };
        let actual = EvalMetrics {
            recall_at_1: 0.3,
            recall_at_3: 0.4,
            recall_at_5: 0.5,
            mrr: 0.2,
        };
        let regs = check_regression(&actual, &baseline);
        assert_eq!(regs.len(), 4);
    }

    #[test]
    fn regression_mrr_specific_message() {
        let baseline = EvalBaseline {
            metrics: EvalMetrics {
                recall_at_1: 0.5,
                recall_at_3: 0.5,
                recall_at_5: 0.5,
                mrr: 0.85,
            },
            note: "test".into(),
        };
        let actual = EvalMetrics {
            recall_at_1: 0.9,
            recall_at_3: 0.9,
            recall_at_5: 0.9,
            mrr: 0.3,
        };
        let regs = check_regression(&actual, &baseline);
        assert_eq!(regs.len(), 1);
        assert!(regs[0].contains("MRR"));
        assert!(regs[0].contains("0.3000"));
    }

    // ── fixture helpers ──────────────────────────────────────────────────────

    #[test]
    fn fixture_corpus_has_eight_docs() {
        let docs = fixture_corpus();
        assert_eq!(docs.len(), 8);
    }

    #[test]
    fn eval_queries_has_seven_entries() {
        let queries = eval_queries();
        assert_eq!(queries.len(), 7);
    }

    #[test]
    fn eval_queries_reference_valid_doc_ids() {
        let queries = eval_queries();
        for q in &queries {
            for &id in &q.expected_ids {
                assert!(
                    (1..=8).contains(&id),
                    "query '{}' references doc {id} which is outside 1..8",
                    q.text
                );
            }
            assert!(
                !q.expected_ids.is_empty(),
                "query '{}' has no expected ids",
                q.text
            );
        }
    }

    #[test]
    fn labeled_query_serde_roundtrip() {
        let q = LabeledQuery {
            text: "test query".into(),
            expected_ids: vec![1, 2, 3],
        };
        let json = serde_json::to_string(&q).unwrap();
        let q2: LabeledQuery = serde_json::from_str(&json).unwrap();
        assert_eq!(q, q2);
    }

    #[test]
    fn eval_metrics_serde_roundtrip() {
        let m = EvalMetrics {
            recall_at_1: 0.5,
            recall_at_3: 0.75,
            recall_at_5: 0.9,
            mrr: 0.6,
        };
        let json = serde_json::to_string(&m).unwrap();
        let m2: EvalMetrics = serde_json::from_str(&json).unwrap();
        assert_eq!(m.recall_at_1, m2.recall_at_1);
        assert_eq!(m.recall_at_3, m2.recall_at_3);
        assert_eq!(m.recall_at_5, m2.recall_at_5);
        assert_eq!(m.mrr, m2.mrr);
    }

    #[test]
    fn baseline_serde_roundtrip() {
        let b = EvalBaseline {
            metrics: EvalMetrics {
                recall_at_1: 0.5,
                recall_at_3: 0.6,
                recall_at_5: 0.7,
                mrr: 0.8,
            },
            note: "initial baseline".into(),
        };
        let json = serde_json::to_string(&b).unwrap();
        let b2: EvalBaseline = serde_json::from_str(&json).unwrap();
        assert_eq!(b.metrics.recall_at_1, b2.metrics.recall_at_1);
        assert_eq!(b.note, b2.note);
    }

    /// The eval baseline JSON fixture must parse as valid [`EvalBaseline`].
    #[test]
    fn baseline_fixture_is_valid() {
        let json_str = include_str!("../tests/fixtures/eval_baseline.json");
        let baseline: EvalBaseline =
            serde_json::from_str(json_str).expect("eval_baseline.json must parse as EvalBaseline");
        assert!(!baseline.note.is_empty(), "baseline note must not be empty");
        let m = &baseline.metrics;
        assert!((0.0..=1.0).contains(&m.recall_at_1));
        assert!((0.0..=1.0).contains(&m.recall_at_3));
        assert!((0.0..=1.0).contains(&m.recall_at_5));
        assert!((0.0..=1.0).contains(&m.mrr));
    }

    /// The cosine between identical spike vectors is 1.0; between distinct ones ≈ 0.
    /// Smoke-test that our spike-vector intuition is correct.
    #[test]
    fn spike_vector_cosine() {
        let spike0 = spike_1024(0);
        let spike1 = spike_1024(1);

        // Same spike → cosine ≈ 1.0.
        let dot_self: f32 = spike0.iter().zip(spike0.iter()).map(|(a, b)| a * b).sum();
        assert!((dot_self - 1.0).abs() < 1e-6);

        // Different spikes → orthogonal → cosine ≈ 0.0.
        let dot_other: f32 = spike0.iter().zip(spike1.iter()).map(|(a, b)| a * b).sum();
        assert!(dot_other.abs() < 1e-6);
    }
}
