//! Tag canonicalization: deduplicate raw LLM-proposed tags against a tenant's
//! existing tag set via alias lookup and cosine similarity (plan §6.5).
//!
//! # Algorithm
//!
//! 1. **Exact alias lookup** in `tag_aliases` → if found, use that canonical `tag_id`.
//! 2. Else **embed the raw tag name**, cosine-match against existing `tags.embedding`
//!    for the tenant: if best match ≥ `TAG_MERGE_THRESHOLD` (0.85), reuse the
//!    canonical tag and insert an alias row.
//! 3. Else **insert a new canonical tag** with its name embedding and return the new id.
//!
//! On return the caller receives `Vec<i64>` — one canonical tag id per raw input,
//! order-aligned with the input slice.

use std::sync::Arc;

use anyhow::Context;
use kb_core::provider::EmbedReq;
use kb_llm::LlamaClient;
use kb_store::PgStore;

/// Default cosine-similarity merge threshold (plan §6.5).
pub const TAG_MERGE_THRESHOLD: f32 = 0.85;

// ── Pure functions (no I/O, testable without mocks) ──────────────────────────

/// Cosine similarity between two equal-length vectors.
///
/// Returns `None` when either vector has zero magnitude (avoiding division by zero)
/// or when the slices have different lengths.
#[must_use]
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> Option<f32> {
    if a.len() != b.len() {
        return None;
    }

    let mut dot = 0.0f64;
    let mut mag_a = 0.0f64;
    let mut mag_b = 0.0f64;

    for i in 0..a.len() {
        let x = f64::from(a[i]);
        let y = f64::from(b[i]);
        dot += x * y;
        mag_a += x * x;
        mag_b += y * y;
    }

    let mag_a = mag_a.sqrt();
    let mag_b = mag_b.sqrt();

    if mag_a == 0.0 || mag_b == 0.0 {
        return None;
    }

    // Clamp to [-1, 1] to guard against float rounding.
    let cos = (dot / (mag_a * mag_b)) as f32;
    Some(cos.clamp(-1.0, 1.0))
}

/// Find the best cosine match for `tag_embedding` among `existing_tags`.
///
/// Returns `Some((tag_id, score))` if the best match meets or exceeds `threshold`,
/// or `None` if no match qualifies (including when `existing_tags` is empty).
#[must_use]
pub fn find_best_match(
    tag_embedding: &[f32],
    existing_tags: &[(i64, Vec<f32>)],
    threshold: f32,
) -> Option<(i64, f32)> {
    let mut best: Option<(i64, f32)> = None;

    for (tag_id, vec) in existing_tags {
        if let Some(sim) = cosine_similarity(tag_embedding, vec)
            && sim >= threshold
        {
            match best {
                Some((_, best_score)) if sim > best_score => {
                    best = Some((*tag_id, sim));
                }
                None => {
                    best = Some((*tag_id, sim));
                }
                _ => {} // sim <= best_score, keep current best
            }
        }
    }

    best
}

// ── TagCanonicalizer ─────────────────────────────────────────────────────────

/// Canonicalizes raw LLM-proposed tags against a tenant's existing tag set using
/// alias lookups and cosine similarity (plan §6.5, P3-T3).
///
/// The canonicalizer holds shared references to the Postgres store (for
/// alias/tag CRUD) and the LLM client (for embedding tag names).
///
/// # Example
///
/// ```no_run
/// # use std::sync::Arc;
/// # use kb_pipeline::tag_canonicalizer::{TagCanonicalizer, TAG_MERGE_THRESHOLD};
/// # use kb_store::PgStore;
/// # use kb_llm::LlamaClient;
/// # fn example(store: Arc<PgStore>, llm: Arc<LlamaClient>) {
/// let canon = TagCanonicalizer::new(store, llm, "bge-m3".into(), TAG_MERGE_THRESHOLD);
/// # }
/// ```
pub struct TagCanonicalizer {
    /// Postgres store for tag CRUD + alias lookups.
    store: Arc<PgStore>,
    /// LLM client for embedding tag names.
    llm: Arc<LlamaClient>,
    /// Embedding model id sent in the OpenAI-compatible request body.
    embed_model: String,
    /// Cosine-similarity merge threshold (plan §6.5).
    threshold: f32,
}

impl TagCanonicalizer {
    /// Create a new canonicalizer.
    pub fn new(
        store: Arc<PgStore>,
        llm: Arc<LlamaClient>,
        embed_model: String,
        threshold: f32,
    ) -> Self {
        Self {
            store,
            llm,
            embed_model,
            threshold,
        }
    }

    /// Canonicalize a batch of raw tag names, returning one canonical `tag_id` per
    /// input, order-aligned.
    ///
    /// Tags newly inserted during this call are appended to the local working set so
    /// that subsequent raw tags in the same batch can match against them — e.g.
    /// `["invoice", "bill"]` results in a single canonical tag for both.
    ///
    /// # Errors
    /// Returns an error if a database or embedding call fails.
    pub async fn canonicalize(
        &self,
        tenant_id: i64,
        raw_tags: &[String],
    ) -> anyhow::Result<Vec<i64>> {
        let mut tag_ids = Vec::with_capacity(raw_tags.len());

        // Pre-fetch all existing tags with embeddings for this tenant.
        let mut existing_tags = self.store.find_similar_tags(tenant_id).await?;

        for raw_tag in raw_tags {
            // 1. Exact alias lookup.
            if let Some(tag_id) = self.store.lookup_alias(tenant_id, raw_tag).await? {
                tag_ids.push(tag_id);
                continue;
            }

            // 2. Embed the raw tag name.
            let embedding = self.embed_tag_name(raw_tag).await?;

            // 3. Cosine-match against existing tags (including those just inserted).
            if let Some((best_id, _score)) =
                find_best_match(&embedding, &existing_tags, self.threshold)
            {
                // Merge: record the raw form as an alias.
                self.store
                    .insert_tag_alias(tenant_id, raw_tag, best_id)
                    .await?;
                tag_ids.push(best_id);
            } else {
                // 4. No match — create a new canonical tag.
                let new_id = self
                    .store
                    .upsert_tag(tenant_id, raw_tag, &embedding)
                    .await?;
                // Append to the local working set so subsequent raw tags can match.
                existing_tags.push((new_id, embedding));
                tag_ids.push(new_id);
            }
        }

        Ok(tag_ids)
    }

    /// Embed a single tag name, returning its vector.
    ///
    /// # Errors
    /// Returns an error if the LLM backend call fails or returns zero vectors.
    async fn embed_tag_name(&self, name: &str) -> anyhow::Result<Vec<f32>> {
        let req = EmbedReq {
            texts: vec![name.to_string()],
        };

        let resp = self
            .llm
            .embed(&self.embed_model, &req)
            .await
            .map_err(|e| anyhow::anyhow!("failed to embed tag name '{name}': {e}"))?;

        resp.vectors
            .into_iter()
            .next()
            .with_context(|| format!("embedder returned zero vectors for tag '{name}'"))
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    // ── cosine_similarity ─────────────────────────────────────────────────

    #[test]
    fn cosine_identical_vectors() {
        let v = vec![1.0, 2.0, 3.0];
        let sim = cosine_similarity(&v, &v).unwrap();
        assert!(
            (sim - 1.0).abs() < 1e-6,
            "identical vectors must have cosine 1.0, got {sim}"
        );
    }

    #[test]
    fn cosine_orthogonal_vectors() {
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![0.0, 1.0, 0.0];
        let sim = cosine_similarity(&a, &b).unwrap();
        assert!(
            sim.abs() < 1e-6,
            "orthogonal vectors must have cosine 0.0, got {sim}"
        );
    }

    #[test]
    fn cosine_opposite_vectors() {
        let a = vec![1.0, 2.0];
        let b = vec![-1.0, -2.0];
        let sim = cosine_similarity(&a, &b).unwrap();
        assert!(
            (sim - (-1.0)).abs() < 1e-6,
            "opposite vectors must have cosine -1.0, got {sim}"
        );
    }

    #[test]
    fn cosine_known_example() {
        // a = [1,0], b = [1,1] → dot=1, |a|=1, |b|=sqrt(2) → cos = 1/sqrt(2) ≈ 0.7071
        let a = vec![1.0, 0.0];
        let b = vec![1.0, 1.0];
        let sim = cosine_similarity(&a, &b).unwrap();
        let expected = 1.0_f32 / 2.0_f32.sqrt();
        assert!(
            (sim - expected).abs() < 1e-5,
            "expected ~{expected}, got {sim}"
        );
    }

    #[test]
    fn cosine_zero_vector_a_returns_none() {
        let a = vec![0.0, 0.0, 0.0];
        let b = vec![1.0, 2.0, 3.0];
        assert!(cosine_similarity(&a, &b).is_none());
    }

    #[test]
    fn cosine_zero_vector_b_returns_none() {
        let a = vec![1.0, 2.0, 3.0];
        let b = vec![0.0, 0.0, 0.0];
        assert!(cosine_similarity(&a, &b).is_none());
    }

    #[test]
    fn cosine_mismatched_lengths_returns_none() {
        assert!(cosine_similarity(&[1.0, 2.0], &[1.0, 2.0, 3.0]).is_none());
    }

    #[test]
    fn cosine_empty_vectors_returns_none() {
        // Empty vectors have zero magnitude → None.
        assert!(cosine_similarity(&[], &[]).is_none());
    }

    #[test]
    fn cosine_negative_values() {
        // Symmetrical negative values should give the same cos as positive ones
        // (scaling both by -1 preserves angle).
        let a = vec![1.0, 2.0, 3.0];
        let b = vec![-1.0, -2.0, -3.0];
        let sim = cosine_similarity(&a, &b).unwrap();
        assert!(
            (sim - (-1.0)).abs() < 1e-6,
            "negated vectors must have cosine -1.0, got {sim}"
        );
    }

    // ── find_best_match ───────────────────────────────────────────────────

    #[test]
    fn best_match_empty_existing_returns_none() {
        let embedding = vec![1.0, 0.0];
        assert!(find_best_match(&embedding, &[], 0.85).is_none());
    }

    #[test]
    fn best_match_single_above_threshold() {
        let embedding = vec![1.0, 0.0, 0.0];
        let existing = vec![(42, vec![0.999, 0.001, 0.0])];
        let result = find_best_match(&embedding, &existing, 0.85).unwrap();
        assert_eq!(result.0, 42);
        assert!(result.1 >= 0.85);
    }

    #[test]
    fn best_match_single_below_threshold_returns_none() {
        let embedding = vec![1.0, 0.0];
        let existing = vec![(1, vec![0.0, 1.0])]; // orthogonal → cos ≈ 0
        assert!(find_best_match(&embedding, &existing, 0.85).is_none());
    }

    #[test]
    fn best_match_picks_highest_among_many() {
        let embedding = vec![1.0, 0.0, 0.0];
        let existing = vec![
            (10, vec![0.6, 0.8, 0.0]),  // cos ≈ 0.6
            (20, vec![0.95, 0.3, 0.0]), // cos ≈ 0.95
            (30, vec![0.85, 0.5, 0.0]), // cos ≈ 0.86
        ];
        let result = find_best_match(&embedding, &existing, 0.85).unwrap();
        assert_eq!(result.0, 20, "must pick highest cosine (id=20)");
        assert!(result.1 > 0.94);
    }

    #[test]
    fn best_match_threshold_boundary_085_matches() {
        // cos similarity of exactly 0.85 should match.
        let embedding = vec![1.0, 0.0];
        // To get cos = 0.85, we need b = [0.85, sqrt(1-0.85^2)] = [0.85, ~0.527]
        let y = (1.0_f32 - 0.85_f32 * 0.85_f32).sqrt();
        let existing = vec![(1, vec![0.85, y])];
        let result = find_best_match(&embedding, &existing, 0.85);
        assert!(result.is_some(), "0.85 at threshold must match");
    }

    #[test]
    fn best_match_threshold_boundary_084_no_match() {
        // cos = 0.84 < 0.85 threshold → no match.
        let embedding = vec![1.0, 0.0];
        let existing = vec![(1, vec![0.84, 0.543])]; // cos ≈ 0.84
        let result = find_best_match(&embedding, &existing, 0.85);
        assert!(result.is_none(), "0.84 below threshold must NOT match");
    }

    #[test]
    fn best_match_skips_zero_vector_tags() {
        // A tag with a zero-magnitude embedding should be skipped (cosine returns
        // None), not crash. The other tag should still be considered.
        let embedding = vec![1.0, 0.0];
        let existing = vec![
            (1, vec![0.0, 0.0]),     // zero vector → skipped
            (2, vec![0.999, 0.001]), // cos ≈ 1.0
        ];
        let result = find_best_match(&embedding, &existing, 0.85).unwrap();
        assert_eq!(result.0, 2, "must skip zero-vector tag and match id=2");
    }

    // ── TagCanonicalizer construction ──────────────────────────────────────

    #[test]
    fn constructor_stores_threshold() {
        // The struct cannot be constructed without a real store+llm, but we can
        // test the constant value and the constructor pattern via the module.
        assert!(
            (TAG_MERGE_THRESHOLD - 0.85).abs() < f32::EPSILON,
            "TAG_MERGE_THRESHOLD must be 0.85"
        );
    }

    #[test]
    fn threshold_constant_is_in_expected_range() {
        // Guard: the threshold must be in (0.0, 1.0] and reasonable for cosine
        // matching. These are compile-time checks via const-eval so clippy stays
        // happy; the test body is intentionally empty.
        const {
            assert!(TAG_MERGE_THRESHOLD > 0.0);
            assert!(TAG_MERGE_THRESHOLD <= 1.0);
            assert!(TAG_MERGE_THRESHOLD >= 0.8);
            assert!(TAG_MERGE_THRESHOLD < 0.95);
        }
    }

    // ── Edge cases on pure logic ──────────────────────────────────────────

    #[test]
    fn best_match_first_candidate_wins_when_equal_scores() {
        // When two candidates have (effectively) equal cosine, the first one
        // encountered should be kept (no arbitrary reordering).
        let embedding = vec![1.0, 0.0];
        // Both have same direction (cos ≈ 1.0), but different magnitudes.
        let existing = vec![(100, vec![0.5, 0.0]), (200, vec![2.0, 0.0])];
        let result = find_best_match(&embedding, &existing, 0.85).unwrap();
        // Both have cos=1.0. First one (id=100) should win.
        assert_eq!(result.0, 100);
    }

    #[test]
    fn find_best_match_with_large_embeddings() {
        // 1024-dim vectors (BGE-M3) should work correctly.
        let mut a = vec![0.0f32; 1024];
        a[0] = 1.0;
        a[100] = 0.5;
        a[500] = -0.3;

        let mut b = a.clone();
        // Identical → cos = 1.0
        let sim = cosine_similarity(&a, &b).unwrap();
        assert!((sim - 1.0).abs() < 1e-5);

        // Slightly perturbed → still high cos.
        b[0] = 0.99;
        let sim2 = cosine_similarity(&a, &b).unwrap();
        assert!(
            sim2 > 0.99,
            "slight perturbation must give high cos, got {sim2}"
        );

        // find_best_match with 1024-dim vectors.
        let existing = vec![(1, b)];
        let result = find_best_match(&a, &existing, 0.85).unwrap();
        assert_eq!(result.0, 1);
    }
}
