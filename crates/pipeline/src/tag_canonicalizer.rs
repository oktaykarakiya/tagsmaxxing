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

use crate::tag_store::TagStore;

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
/// # use kb_pipeline::TagStore;
/// # use kb_llm::LlamaClient;
/// # fn example(store: Arc<dyn TagStore>, llm: Arc<LlamaClient>) {
/// let canon = TagCanonicalizer::new(store, llm, "bge-m3".into(), TAG_MERGE_THRESHOLD);
/// # }
/// ```
pub struct TagCanonicalizer {
    /// Postgres store for tag CRUD + alias lookups (trait-object for testability).
    store: Arc<dyn TagStore>,
    /// LLM client for embedding tag names.
    llm: Arc<LlamaClient>,
    /// Embedding model id sent in the OpenAI-compatible request body.
    embed_model: String,
    /// Cosine-similarity merge threshold (plan §6.5).
    threshold: f32,
}

impl TagCanonicalizer {
    /// Create a new canonicalizer.
    ///
    /// `store` may be a real [`PgStore`](kb_store::PgStore) in production or an
    /// in-memory mock in tests — anything implementing [`TagStore`].
    pub fn new(
        store: Arc<dyn TagStore>,
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
        local_only: bool,
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
            let embedding = self.embed_tag_name(raw_tag, local_only).await?;

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
    async fn embed_tag_name(&self, name: &str, local_only: bool) -> anyhow::Result<Vec<f32>> {
        let req = EmbedReq {
            texts: vec![name.to_string()],
        };

        let resp = self
            .llm
            .embed(&self.embed_model, &req, local_only, 0)
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
    use std::sync::Arc;
    use std::time::Duration;

    use kb_core::role::Role;
    use kb_mock_backend::MockBackend;
    use kb_scheduler::{Pool, test_backend};
    use reqwest::Client;

    use super::*;
    use crate::tag_store::mock::MockTagStore;

    // ── Test helpers ──────────────────────────────────────────────────────

    /// Build a `TagCanonicalizer` backed by a mock store + mock LLM backend.
    async fn canonicalizer_with_mock(store: Arc<MockTagStore>) -> (TagCanonicalizer, MockBackend) {
        let mock = MockBackend::start().await;
        let base_url = mock.url("/v1");
        let backend = Arc::new(test_backend(
            "mock-embed",
            base_url,
            vec![Role::Embed],
            0, /* priority */
            4, /* slots */
        ));
        let pool = Pool::new(vec![backend], Duration::from_secs(5));
        let llm = Arc::new(LlamaClient::new(
            pool,
            Client::new(),
            0, /* max_retries */
            0, /* circuit_threshold */
            Duration::from_millis(200),
        ));
        let canon = TagCanonicalizer::new(
            store as Arc<dyn TagStore>,
            llm,
            "test-model".into(),
            TAG_MERGE_THRESHOLD,
        );
        (canon, mock)
    }

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
        assert!(cosine_similarity(&[], &[]).is_none());
    }

    #[test]
    fn cosine_negative_values() {
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
        let existing = vec![(1, vec![0.0, 1.0])];
        assert!(find_best_match(&embedding, &existing, 0.85).is_none());
    }

    #[test]
    fn best_match_picks_highest_among_many() {
        let embedding = vec![1.0, 0.0, 0.0];
        let existing = vec![
            (10, vec![0.6, 0.8, 0.0]),
            (20, vec![0.95, 0.3, 0.0]),
            (30, vec![0.85, 0.5, 0.0]),
        ];
        let result = find_best_match(&embedding, &existing, 0.85).unwrap();
        assert_eq!(result.0, 20, "must pick highest cosine (id=20)");
        assert!(result.1 > 0.94);
    }

    #[test]
    fn best_match_threshold_boundary_085_matches() {
        let embedding = vec![1.0, 0.0];
        let y = (1.0_f32 - 0.85_f32 * 0.85_f32).sqrt();
        let existing = vec![(1, vec![0.85, y])];
        let result = find_best_match(&embedding, &existing, 0.85);
        assert!(result.is_some(), "0.85 at threshold must match");
    }

    #[test]
    fn best_match_threshold_boundary_084_no_match() {
        let embedding = vec![1.0, 0.0];
        let existing = vec![(1, vec![0.84, 0.543])];
        let result = find_best_match(&embedding, &existing, 0.85);
        assert!(result.is_none(), "0.84 below threshold must NOT match");
    }

    #[test]
    fn best_match_skips_zero_vector_tags() {
        let embedding = vec![1.0, 0.0];
        let existing = vec![(1, vec![0.0, 0.0]), (2, vec![0.999, 0.001])];
        let result = find_best_match(&embedding, &existing, 0.85).unwrap();
        assert_eq!(result.0, 2, "must skip zero-vector tag and match id=2");
    }

    // ── TagCanonicalizer construction ──────────────────────────────────────

    #[test]
    fn constructor_stores_threshold() {
        assert!(
            (TAG_MERGE_THRESHOLD - 0.85).abs() < f32::EPSILON,
            "TAG_MERGE_THRESHOLD must be 0.85"
        );
    }

    #[test]
    fn threshold_constant_is_in_expected_range() {
        const {
            assert!(TAG_MERGE_THRESHOLD > 0.0);
            assert!(TAG_MERGE_THRESHOLD <= 1.0);
            assert!(TAG_MERGE_THRESHOLD >= 0.8);
            assert!(TAG_MERGE_THRESHOLD < 0.95);
        }
    }

    #[test]
    fn best_match_first_candidate_wins_when_equal_scores() {
        let embedding = vec![1.0, 0.0];
        let existing = vec![(100, vec![0.5, 0.0]), (200, vec![2.0, 0.0])];
        let result = find_best_match(&embedding, &existing, 0.85).unwrap();
        assert_eq!(result.0, 100);
    }

    #[test]
    fn best_match_with_two_equal_scores_keeps_first() {
        // Both candidates have cos=1.0 (identical to embedding). First one wins.
        let embedding = vec![1.0, 2.0, 3.0];
        let existing = vec![
            (1, vec![1.0, 2.0, 3.0]),
            (2, vec![2.0, 4.0, 6.0]), // same direction, cos=1.0
        ];
        let result = find_best_match(&embedding, &existing, 0.85).unwrap();
        assert_eq!(result.0, 1, "first equal-score candidate should win");
    }

    #[test]
    fn cosine_near_but_not_quite_identical() {
        // Two vectors that are very similar but not identical.
        let a = vec![1.0, 0.0, 0.0];
        let b = vec![0.9999, 0.01414, 0.0]; // cos ≈ 0.9999
        let sim = cosine_similarity(&a, &b).unwrap();
        assert!(sim > 0.999, "expected > 0.999, got {sim}");
        assert!(sim < 1.0, "should not be exactly 1.0");
    }

    #[test]
    fn cosine_float_clamping_to_range() {
        // Due to floating-point rounding, cos might be slightly >1.0 or <-1.0 after
        // division. The clamp in cosine_similarity must correct this.
        let a = vec![1e20_f32, 1e20_f32];
        let b = vec![1e20_f32, 1e20_f32];
        let sim = cosine_similarity(&a, &b).unwrap();
        assert!(
            (-1.0..=1.0).contains(&sim),
            "cos must be in [-1,1], got {sim}"
        );
    }

    #[test]
    fn find_best_match_with_large_embeddings() {
        let mut a = vec![0.0f32; 1024];
        a[0] = 1.0;
        a[100] = 0.5;
        a[500] = -0.3;

        let mut b = a.clone();
        let sim = cosine_similarity(&a, &b).unwrap();
        assert!((sim - 1.0).abs() < 1e-5);

        b[0] = 0.99;
        let sim2 = cosine_similarity(&a, &b).unwrap();
        assert!(
            sim2 > 0.99,
            "slight perturbation must give high cos, got {sim2}"
        );

        let existing = vec![(1, b)];
        let result = find_best_match(&a, &existing, 0.85).unwrap();
        assert_eq!(result.0, 1);
    }

    // ── canonicalize() — happy path ───────────────────────────────────────

    #[tokio::test]
    async fn canonicalize_returns_exact_alias_match() {
        let store = Arc::new(MockTagStore::new());
        store.seed_tag(1, "rust", 10, vec![1.0, 0.0]);
        store.seed_alias(1, "rust-lang", 10);

        let (canon, mock) = canonicalizer_with_mock(store.clone()).await;

        let ids = canon
            .canonicalize(1, &["rust-lang".into()], false)
            .await
            .unwrap();
        assert_eq!(
            ids,
            vec![10],
            "alias should resolve to existing tag id without embedding call"
        );
        mock.shutdown().await;
    }

    #[tokio::test]
    async fn canonicalize_matches_existing_tag_above_threshold() {
        let store = Arc::new(MockTagStore::new());
        // Seed an existing "invoice" tag with the same embedding the mock returns.
        store.seed_tag(1, "invoice", 5, vec![1.0, 0.0]);

        let (canon, mock) = canonicalizer_with_mock(store.clone()).await;
        // The mock returns [1.0, 0.0] by default — cos=1.0 with the seeded tag.
        mock.scenario().lock().await.embed_content = Some(vec![vec![1.0, 0.0]]);

        let ids = canon
            .canonicalize(1, &["bill".into()], false)
            .await
            .unwrap();
        assert_eq!(
            ids,
            vec![5],
            "should merge with existing tag via cosine match"
        );
        // An alias should have been created.
        let alias = store.lookup_alias(1, "bill").await.unwrap();
        assert_eq!(alias, Some(5), "alias 'bill' → tag 5 should exist");
        mock.shutdown().await;
    }

    #[tokio::test]
    async fn canonicalize_creates_new_tag_when_no_match() {
        let store = Arc::new(MockTagStore::new());
        // Seed an unrelated tag with orthogonal embedding.
        store.seed_tag(1, "unrelated", 1, vec![0.0, 1.0]);

        let (canon, mock) = canonicalizer_with_mock(store.clone()).await;
        // The mock returns [1.0, 0.0] — cos ≈ 0.0 with [0.0, 1.0] (below threshold).
        mock.scenario().lock().await.embed_content = Some(vec![vec![1.0, 0.0]]);

        let ids = canon
            .canonicalize(1, &["newtopic".into()], false)
            .await
            .unwrap();
        assert_eq!(ids.len(), 1);
        assert!(ids[0] > 0, "new tag must get a positive id");
        // The new tag should exist in the store.
        let tags = store.find_similar_tags(1).await.unwrap();
        assert_eq!(tags.len(), 2, "should have 'unrelated' + 'newtopic'");
        mock.shutdown().await;
    }

    #[tokio::test]
    async fn canonicalize_empty_tags_returns_empty() {
        let store = Arc::new(MockTagStore::new());
        let (canon, mock) = canonicalizer_with_mock(store.clone()).await;

        let ids = canon.canonicalize(1, &[], false).await.unwrap();
        assert!(ids.is_empty());
        mock.shutdown().await;
    }

    #[tokio::test]
    async fn canonicalize_multiple_tags_order_preserved() {
        let store = Arc::new(MockTagStore::new());
        store.seed_tag(1, "alpha", 1, vec![1.0, 0.0]);
        store.seed_tag(1, "beta", 2, vec![0.0, 1.0]);
        store.seed_alias(1, "first", 1);

        let (canon, mock) = canonicalizer_with_mock(store.clone()).await;
        // Returns an embedding close to beta for "second".
        mock.scenario().lock().await.embed_content = Some(vec![vec![0.0, 1.0]]);

        // "first" → alias to 1, "second" → cos-match to beta(2).
        let ids = canon
            .canonicalize(1, &["first".into(), "second".into()], false)
            .await
            .unwrap();
        assert_eq!(ids.len(), 2, "2 inputs → 2 outputs");
        assert_eq!(ids[0], 1, "first → alpha via alias");
        assert_eq!(ids[1], 2, "second → beta via cosine match");
        mock.shutdown().await;
    }

    /// Two raw tags in the same batch that are semantically similar should
    /// converge to a single canonical tag (plan §6.5 batch behaviour).
    #[tokio::test]
    async fn canonicalize_batch_converges_similar_tags() {
        let store = Arc::new(MockTagStore::new());
        let (canon, mock) = canonicalizer_with_mock(store.clone()).await;
        // Mock returns the same embedding for both calls → both raw tags get same vector.
        mock.scenario().lock().await.embed_content = Some(vec![vec![0.7, 0.7]]);

        let ids = canon
            .canonicalize(1, &["invoice".into(), "bill".into()], false)
            .await
            .unwrap();
        assert_eq!(ids.len(), 2);
        // Both should map to the same canonical id: the second ("bill")
        // matches the first ("invoice") which was just inserted.
        assert_eq!(ids[0], ids[1], "invoice and bill must converge to same tag");
        // An alias should link bill → invoice.
        let alias = store.lookup_alias(1, "bill").await.unwrap();
        assert_eq!(alias, Some(ids[0]));
        mock.shutdown().await;
    }

    /// When an alias already exists, no embedding call should be made for that
    /// raw tag (the LLM is skipped entirely — plan §6.5 step 1).
    #[tokio::test]
    async fn canonicalize_alias_skips_embedding() {
        let store = Arc::new(MockTagStore::new());
        store.seed_tag(1, "known", 99, vec![1.0, 0.0]);
        store.seed_alias(1, "known-alias", 99);

        let (canon, mock) = canonicalizer_with_mock(store.clone()).await;
        // Set embed content — it should only be consumed once (for "new-tag", not the alias).
        mock.scenario().lock().await.embed_content = Some(vec![vec![0.5, 0.5]]);

        let ids = canon
            .canonicalize(1, &["known-alias".into(), "new-tag".into()], false)
            .await
            .unwrap();
        assert_eq!(ids.len(), 2);
        assert_eq!(ids[0], 99, "alias must resolve without LLM call");
        assert!(ids[1] > 0, "new tag must get an id");
        mock.shutdown().await;
    }

    /// Multiple identical raw tags in a single batch should all resolve to the
    /// same canonical id.
    #[tokio::test]
    async fn canonicalize_duplicate_raw_tags_same_id() {
        let store = Arc::new(MockTagStore::new());
        let (canon, mock) = canonicalizer_with_mock(store.clone()).await;
        mock.scenario().lock().await.embed_content = Some(vec![vec![0.3, 0.9]]);

        let ids = canon
            .canonicalize(1, &["dup".into(), "dup".into(), "dup".into()], false)
            .await
            .unwrap();
        assert_eq!(ids.len(), 3);
        assert!(
            ids.iter().all(|&id| id == ids[0]),
            "all duplicates must resolve to same canonical id"
        );
        mock.shutdown().await;
    }

    /// When the existing tag set is empty, every raw tag creates a new
    /// canonical tag.
    #[tokio::test]
    async fn canonicalize_empty_store_all_new() {
        let store = Arc::new(MockTagStore::new());
        let (canon, mock) = canonicalizer_with_mock(store.clone()).await;
        mock.scenario().lock().await.embed_content = Some(vec![vec![0.5, 0.5]]);

        let ids = canon
            .canonicalize(1, &["a".into(), "b".into(), "c".into()], false)
            .await
            .unwrap();
        assert_eq!(ids.len(), 3);
        // All three converge since mock returns the same vector.
        assert_eq!(ids[0], ids[1]);
        assert_eq!(ids[1], ids[2]);
        mock.shutdown().await;
    }

    /// Canonicalization operates per-tenant — tags in tenant 2 should not
    /// leak to tenant 1.
    #[tokio::test]
    async fn canonicalize_tenant_isolation() {
        let store = Arc::new(MockTagStore::new());
        // Seed tag for tenant 2 only.
        store.seed_tag(2, "shared-name", 500, vec![1.0, 0.0]);
        store.seed_alias(2, "alias-t2", 500);

        let (canon, mock) = canonicalizer_with_mock(store.clone()).await;
        mock.scenario().lock().await.embed_content = Some(vec![vec![0.3, 0.9]]);

        // Tenant 1 asks for the same names — must NOT see tenant 2's data.
        let ids = canon
            .canonicalize(1, &["shared-name".into(), "alias-t2".into()], false)
            .await
            .unwrap();
        assert_eq!(ids.len(), 2);
        // Both should be new ids, NOT 500.
        assert_ne!(ids[0], 500);
        assert_ne!(ids[1], 500);
        mock.shutdown().await;
    }

    /// When the embed backend returns an error, canonicalize must propagate it.
    #[tokio::test]
    async fn canonicalize_surfaces_embed_error() {
        let store = Arc::new(MockTagStore::new());
        let (canon, mock) = canonicalizer_with_mock(store.clone()).await;
        // Only an alias — which skips embedding — should work. Non-alias
        // tags need embedding and should fail when embed is set to ServerError.
        store.seed_tag(1, "known", 1, vec![1.0, 0.0]);
        store.seed_alias(1, "known-alias", 1);

        // Test that a tag requiring embedding fails.
        mock.scenario().lock().await.embed = kb_mock_backend::ResponseMode::ServerError;

        let err = canon
            .canonicalize(1, &["new-tag".into()], false)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("failed to embed"),
            "should surface embed error, got: {err}"
        );
        mock.shutdown().await;
    }

    /// A mix of aliases (no embed needed) and new tags should still work when
    /// only aliases are requested (verifies embed is skipped for aliases).
    #[tokio::test]
    async fn canonicalize_aliases_work_even_when_embed_fails() {
        let store = Arc::new(MockTagStore::new());
        store.seed_tag(1, "cat", 1, vec![1.0, 0.0]);
        store.seed_alias(1, "feline", 1);
        store.seed_alias(1, "kitty", 1);

        let (canon, mock) = canonicalizer_with_mock(store.clone()).await;
        // Set embed to error — should not be called for aliases.
        mock.scenario().lock().await.embed = kb_mock_backend::ResponseMode::ServerError;

        // Only aliases — no embedding needed.
        let ids = canon
            .canonicalize(1, &["feline".into(), "kitty".into()], false)
            .await
            .unwrap();
        assert_eq!(ids, vec![1, 1]);
        mock.shutdown().await;
    }

    /// Canonicalize returns an error when the LLM returns zero vectors for a
    /// tag name (edge case in `embed_tag_name`).
    #[tokio::test]
    async fn canonicalize_zero_vectors_from_embed_is_error() {
        let store = Arc::new(MockTagStore::new());
        let (canon, mock) = canonicalizer_with_mock(store.clone()).await;
        // Return empty data array from embed endpoint.
        mock.scenario().lock().await.embed_content = Some(vec![]);

        let err = canon
            .canonicalize(1, &["orphan".into()], false)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("zero vectors"),
            "expected zero vectors error, got: {err}"
        );
        mock.shutdown().await;
    }
}
