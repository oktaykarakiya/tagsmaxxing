//! [`RetrievalPipeline`] — top-level orchestrator wiring the complete §8 read path
//! from query text to ranked [`Hit`]s with deep-link provenance (plan §8, §10, P4-T5).
//!
//! The 4-step pipeline:
//! 1. Embed the query text via [`ChunkEmbedder`] with [`EmbedKind::Query`].
//! 2. Call [`Store::hybrid_search`] with the query + embedding.
//! 3. Rerank the top search results via the [`Reranker`] trait.
//! 4. Return top-`n` [`Hit`]s sorted by reranker score descending.
//!
//! Parallel to [`IngestPipeline`] — same pattern, same testability
//! (mock [`Store`] + mock [`Reranker`] for unit tests).

use std::sync::Arc;

use kb_core::query::{Hit, Query};
use kb_core::reranker::Reranker;
use kb_core::store::Store;

use crate::embedder::ChunkEmbedder;

/// The retrieval pipeline orchestrator (plan §8, §10).
///
/// Wires the complete §8 read path: embed query → hybrid search → rerank →
/// return ranked [`Hit`]s. Every heavy component is behind a trait object so
/// the pipeline can be tested with mock implementations (plan §31.3).
///
/// # Examples
///
/// ```no_run
/// # use std::sync::Arc;
/// # use kb_pipeline::retrieval::RetrievalPipeline;
/// # use kb_core::query::{Query, QueryFilters};
/// # fn example(
/// #     embedder: Arc<kb_pipeline::embedder::ChunkEmbedder>,
/// #     store: Arc<dyn kb_core::store::Store>,
/// #     reranker: Arc<dyn kb_core::reranker::Reranker>,
/// # ) {
/// let pipeline = RetrievalPipeline::new(embedder, store, reranker);
/// # }
/// ```
pub struct RetrievalPipeline {
    /// Batch embedder for encoding the query text.
    embedder: Arc<ChunkEmbedder>,
    /// Database store for hybrid (vector + keyword) search.
    store: Arc<dyn Store>,
    /// Cross-encoder reranker that rescores candidates against the query.
    reranker: Arc<dyn Reranker>,
}

impl RetrievalPipeline {
    /// Create a new pipeline. All components must be fully initialised
    /// before retrieval begins.
    #[must_use]
    pub fn new(
        embedder: Arc<ChunkEmbedder>,
        store: Arc<dyn Store>,
        reranker: Arc<dyn Reranker>,
    ) -> Self {
        Self {
            embedder,
            store,
            reranker,
        }
    }

    /// Run the full 4-step retrieval flow for a single query.
    ///
    /// `tenant_id` is passed through to [`Store::hybrid_search`] so the
    /// implementation can set the `app.current_tenant` Postgres GUC for
    /// Row-Level Security enforcement (plan §13, P5-T1).
    ///
    /// 1. Embeds `query.text` with [`EmbedKind::Query`].
    /// 2. Calls [`Store::hybrid_search`] with the query and embedding.
    /// 3. If results are non-empty, passes the snippet of each hit to the
    ///    [`Reranker`] and re-scores against the original query text.
    /// 4. Returns the top `query.top_k` hits ordered by reranker score
    ///    descending.
    ///
    /// # Errors
    ///
    /// Returns an error if embedding or search fails. An empty result set
    /// is not an error — it returns an empty `Vec<Hit>`.
    pub async fn retrieve(
        &self,
        tenant_id: i64,
        query: &Query,
        local_only: bool,
    ) -> anyhow::Result<Vec<Hit>> {
        // 1. Embed query text.
        let query_embedding = self.embedder.embed_query(&query.text, local_only).await?;

        // 2. Hybrid search (vector + keyword) with RRF fusion + document rollup.
        let hits = self
            .store
            .hybrid_search(tenant_id, query, &query_embedding)
            .await?;

        // 3. Rerank: if there are results, rescore them against the query.
        if hits.is_empty() {
            return Ok(Vec::new());
        }

        let doc_texts: Vec<String> = hits.iter().map(|h| h.snippet.clone()).collect();
        let rerank_scores = self
            .reranker
            .rerank(&query.text, &doc_texts, local_only, 0)
            .await?;

        anyhow::ensure!(
            rerank_scores.len() == hits.len(),
            "reranker returned {} scores for {} documents",
            rerank_scores.len(),
            hits.len()
        );

        // Blend RRF hybrid-search scores with reranker scores.
        // RRF encodes keyword (ts_rank) + vector (cosine) signal that the
        // cross-encoder may not fully capture on its own, especially with
        // quantised models. Blending preserves the exact-match term boost
        // while still deferring to the reranker for semantic relevance.
        const RRF_BLEND_WEIGHT: f32 = 0.2;

        // Normalise RRF scores to [0, 1] so they are range-compatible with
        // the reranker scores before blending.
        let max_rrf = hits.iter().map(|h| h.score).fold(0.0_f32, f32::max);
        let norm = if max_rrf > 0.0 { 1.0 / max_rrf } else { 1.0 };

        let mut rescored: Vec<(Hit, f32)> = hits
            .into_iter()
            .zip(rerank_scores)
            .map(|(mut hit, rerank_score)| {
                let norm_rrf = hit.score * norm;
                let blended = RRF_BLEND_WEIGHT * norm_rrf + (1.0 - RRF_BLEND_WEIGHT) * rerank_score;
                hit.score = blended;
                (hit, blended)
            })
            .collect();

        rescored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        // 4. Return top_k.
        let final_hits: Vec<Hit> = rescored
            .into_iter()
            .take(query.top_k)
            .map(|(hit, _)| hit)
            .collect();

        Ok(final_hits)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use kb_core::chunk::Chunk;
    use kb_core::file::FileRecord;
    use kb_core::query::QueryFilters;

    use super::*;

    // ── Mock Store ─────────────────────────────────────────────────────────

    /// An in-memory [`Store`] that returns pre-configured [`Hit`]s.
    ///
    /// Uses `Arc<[Hit]>` so the struct is `Sync` (required by `Store:
    /// Send + Sync`).
    struct MockStore {
        /// Hits to return from `hybrid_search`.
        hits: Arc<[Hit]>,
    }

    impl MockStore {
        fn new(hits: Vec<Hit>) -> Self {
            Self { hits: hits.into() }
        }
    }

    #[async_trait]
    impl Store for MockStore {
        async fn upsert_file(&self, _rec: &FileRecord) -> anyhow::Result<i64> {
            anyhow::bail!("mock: only hybrid_search is used by RetrievalPipeline")
        }

        async fn upsert_chunks(&self, _file_id: i64, _chunks: &[Chunk]) -> anyhow::Result<()> {
            anyhow::bail!("mock: only hybrid_search is used by RetrievalPipeline")
        }

        async fn hybrid_search(
            &self,
            _tenant_id: i64,
            _query: &Query,
            _query_embedding: &[f32],
        ) -> anyhow::Result<Vec<Hit>> {
            Ok(self.hits.to_vec())
        }
    }

    /// A [`Store`] that records the parameters passed to `hybrid_search` for
    /// later assertion.
    struct RecordingStore {
        /// Hits to return.
        hits: Arc<[Hit]>,
        /// Captured `(query_text, embedding)` from the last call.
        /// Using a `std::sync::Mutex` to allow interior mutability.
        last_call: std::sync::Mutex<Option<(String, Vec<f32>)>>,
    }

    impl RecordingStore {
        fn new(hits: Vec<Hit>) -> Self {
            Self {
                hits: hits.into(),
                last_call: std::sync::Mutex::new(None),
            }
        }

        fn last_query_text(&self) -> Option<String> {
            self.last_call
                .lock()
                .unwrap()
                .as_ref()
                .map(|(t, _)| t.clone())
        }

        fn last_embedding_len(&self) -> Option<usize> {
            self.last_call
                .lock()
                .unwrap()
                .as_ref()
                .map(|(_, e)| e.len())
        }
    }

    #[async_trait]
    impl Store for RecordingStore {
        async fn upsert_file(&self, _rec: &FileRecord) -> anyhow::Result<i64> {
            anyhow::bail!("mock: only hybrid_search is used by RetrievalPipeline")
        }

        async fn upsert_chunks(&self, _file_id: i64, _chunks: &[Chunk]) -> anyhow::Result<()> {
            anyhow::bail!("mock: only hybrid_search is used by RetrievalPipeline")
        }

        async fn hybrid_search(
            &self,
            _tenant_id: i64,
            query: &Query,
            query_embedding: &[f32],
        ) -> anyhow::Result<Vec<Hit>> {
            let mut guard = self.last_call.lock().unwrap();
            *guard = Some((query.text.clone(), query_embedding.to_vec()));
            Ok(self.hits.to_vec())
        }
    }

    // ── Mock Reranker ──────────────────────────────────────────────────────

    /// A [`Reranker`] that returns pre-configured scores.
    ///
    /// Uses `Arc<[f32]>` so the struct is `Sync` (required by `Reranker:
    /// Send + Sync`).
    struct MockReranker {
        scores: Arc<[f32]>,
    }

    impl MockReranker {
        fn new(scores: Vec<f32>) -> Self {
            Self {
                scores: scores.into(),
            }
        }
    }

    #[async_trait]
    impl Reranker for MockReranker {
        async fn rerank(
            &self,
            _query: &str,
            _docs: &[String],
            _local_only: bool,
            _priority: i32,
        ) -> anyhow::Result<Vec<f32>> {
            Ok(self.scores.to_vec())
        }
    }

    /// A [`Reranker`] that records the documents it receives.
    struct RecordingReranker {
        scores: Arc<[f32]>,
        last_docs: std::sync::Mutex<Vec<String>>,
    }

    impl RecordingReranker {
        fn new(scores: Vec<f32>) -> Self {
            Self {
                scores: scores.into(),
                last_docs: std::sync::Mutex::new(Vec::new()),
            }
        }

        fn last_docs(&self) -> Vec<String> {
            self.last_docs.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl Reranker for RecordingReranker {
        async fn rerank(
            &self,
            _query: &str,
            docs: &[String],
            _local_only: bool,
            _priority: i32,
        ) -> anyhow::Result<Vec<f32>> {
            *self.last_docs.lock().unwrap() = docs.to_vec();
            Ok(self.scores.to_vec())
        }
    }

    /// A [`Reranker`] that always returns an error.
    struct FailingReranker;

    #[async_trait]
    impl Reranker for FailingReranker {
        async fn rerank(
            &self,
            _query: &str,
            _docs: &[String],
            _local_only: bool,
            _priority: i32,
        ) -> anyhow::Result<Vec<f32>> {
            anyhow::bail!("simulated reranker failure")
        }
    }

    // ── Helpers ────────────────────────────────────────────────────────────

    /// Build a simple `Hit` for tests.
    fn hit(document_id: i64, score: f32, snippet: &str, file_id: i64, page_no: Option<i32>) -> Hit {
        Hit {
            document_id,
            score,
            title: Some(format!("Doc {document_id}")),
            snippet: snippet.to_string(),
            file_id,
            page_no,
            ts_offset: None,
            kind: None,
        }
    }

    /// Build a `Query` with default filters.
    fn query(text: &str, top_k: usize) -> Query {
        Query {
            text: text.to_string(),
            filters: QueryFilters::default(),
            top_k,
        }
    }

    /// Build a `RetrievalPipeline` with mock components for testing.
    ///
    /// Uses a real `ChunkEmbedder` backed by a mock HTTP server so that
    /// embedding the query exercises the full call path.
    async fn build_test_pipeline(
        store_hits: Vec<Hit>,
        rerank_scores: Vec<f32>,
    ) -> (RetrievalPipeline, kb_mock_backend::MockBackend) {
        use kb_core::role::Role;
        use kb_llm::LlamaClient;
        use kb_mock_backend::MockBackend;
        use kb_scheduler::{Pool, test_backend};
        use reqwest::Client;
        use std::time::Duration;

        let mock = MockBackend::start().await;
        let base_url = mock.url("/v1");
        let backend = Arc::new(test_backend(
            "mock-retrieval",
            base_url,
            vec![Role::Embed],
            0, /* priority */
            4, /* slots */
        ));
        let pool = Pool::new(vec![backend], Duration::from_secs(5));
        let llm = Arc::new(LlamaClient::new(
            pool,
            Client::new(),
            0,                          /* max_retries */
            0,                          /* circuit_threshold */
            Duration::from_millis(200), /* cooldown */
        ));

        // Mock backend returns [0.1, 0.2, 0.3] for every embed request (dim=3).
        let embedder = Arc::new(ChunkEmbedder::new(llm, "test-model".into(), 3));

        let store: Arc<dyn Store> = Arc::new(MockStore::new(store_hits));
        let reranker: Arc<dyn Reranker> = Arc::new(MockReranker::new(rerank_scores));

        let pipeline = RetrievalPipeline::new(embedder, store, reranker);
        (pipeline, mock)
    }

    // ── Construction ───────────────────────────────────────────────────────

    #[test]
    fn pipeline_construction() {
        // Construction is purely structural — verify it compiles and the
        // types align (no runtime components needed).
        let embedder = Arc::new(ChunkEmbedder::new(
            Arc::new(kb_llm::LlamaClient::new(
                kb_scheduler::Pool::new(vec![], std::time::Duration::from_secs(1)),
                reqwest::Client::new(),
                0,
                0,
                std::time::Duration::from_millis(200),
            )),
            "model".into(),
            1024,
        ));
        let store: Arc<dyn Store> = Arc::new(MockStore::new(vec![]));
        let reranker: Arc<dyn Reranker> = Arc::new(MockReranker::new(vec![]));

        let _pipeline = RetrievalPipeline::new(embedder, store, reranker);
    }

    // ── Happy-path: embed → search → rerank → return ─────────────────────

    #[tokio::test]
    async fn full_pipeline_returns_reranked_hits() {
        let store_hits = vec![
            hit(10, 0.8, "snippet for doc 10", 100, Some(1)),
            hit(20, 0.6, "snippet for doc 20", 200, Some(1)),
            hit(30, 0.9, "snippet for doc 30", 300, None),
        ];
        // Reranker: doc 20 becomes best, doc 10 second, doc 30 last.
        let rerank_scores = vec![0.5, 0.95, 0.3];

        let (pipeline, mock) = build_test_pipeline(store_hits, rerank_scores).await;
        let q = query("test query", 10);

        let results = pipeline.retrieve(1, &q, false).await.unwrap();

        assert_eq!(results.len(), 3);
        // Sorted by blended score descending.
        // With RRF_BLEND_WEIGHT=0.2 and max_rrf=0.9:
        //   doc 20: 0.2*(0.6/0.9) + 0.8*0.95 = 0.8933
        //   doc 10: 0.2*(0.8/0.9) + 0.8*0.50 = 0.5778
        //   doc 30: 0.2*(0.9/0.9) + 0.8*0.30 = 0.4400
        assert_eq!(results[0].document_id, 20);
        assert!((results[0].score - 0.8933).abs() < 0.001);
        assert_eq!(results[1].document_id, 10);
        assert!((results[1].score - 0.5778).abs() < 0.001);
        assert_eq!(results[2].document_id, 30);
        assert!((results[2].score - 0.4400).abs() < 0.001);

        // Deep-link provenance preserved.
        assert_eq!(results[0].file_id, 200);
        assert_eq!(results[0].snippet, "snippet for doc 20");

        mock.shutdown().await;
    }

    // ── top_k truncation ──────────────────────────────────────────────────

    #[tokio::test]
    async fn top_k_truncates_results() {
        let store_hits = vec![
            hit(1, 0.9, "a", 10, Some(1)),
            hit(2, 0.8, "b", 20, Some(1)),
            hit(3, 0.7, "c", 30, Some(1)),
            hit(4, 0.6, "d", 40, Some(1)),
            hit(5, 0.5, "e", 50, Some(1)),
        ];
        let rerank_scores = vec![0.95, 0.85, 0.75, 0.65, 0.55];

        let (pipeline, mock) = build_test_pipeline(store_hits, rerank_scores).await;

        // Request only top-2.
        let q = query("top 2 query", 2);
        let results = pipeline.retrieve(1, &q, false).await.unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].document_id, 1);
        assert_eq!(results[1].document_id, 2);

        mock.shutdown().await;
    }

    #[tokio::test]
    async fn top_k_zero_returns_empty() {
        // Store returns results but top_k=0 should yield no hits.
        let store_hits = vec![hit(1, 0.9, "a", 10, Some(1))];
        let rerank_scores = vec![0.5];

        let (pipeline, mock) = build_test_pipeline(store_hits, rerank_scores).await;
        let q = query("query", 0);

        let results = pipeline.retrieve(1, &q, false).await.unwrap();
        assert!(results.is_empty());

        mock.shutdown().await;
    }

    // ── Empty search results ──────────────────────────────────────────────

    #[tokio::test]
    async fn empty_search_results_handled_gracefully() {
        let (pipeline, mock) = build_test_pipeline(vec![], vec![]).await;
        let q = query("nothing matches", 10);

        let results = pipeline.retrieve(1, &q, false).await.unwrap();
        assert!(results.is_empty());
        // Reranker is never called when hits are empty (verified by the
        // MockStore returning empty — if reranker were called with empty docs,
        // the mock would return empty scores and the check above still passes).

        mock.shutdown().await;
    }

    // ── Reranker receives correct documents ───────────────────────────────

    #[tokio::test]
    async fn reranker_receives_snippets_as_documents() {
        let store_hits = vec![
            hit(10, 0.9, "first snippet here", 100, Some(1)),
            hit(20, 0.8, "second snippet", 200, Some(2)),
        ];
        let (pipeline, mock) =
            build_test_pipeline_with_recording_reranker(store_hits, vec![0.7, 0.3]).await;

        let q = query("find me", 10);
        let _results = pipeline.retrieve(1, &q, false).await.unwrap();

        let docs = pipeline.recording_reranker.last_docs();
        // Verify the reranker received the snippets.
        assert_eq!(docs.len(), 2);
        assert!(docs.iter().any(|d| d == "first snippet here"));
        assert!(docs.iter().any(|d| d == "second snippet"));

        mock.shutdown().await;
    }

    /// Build a pipeline with a recording reranker so we can inspect what
    /// documents were passed to it.
    async fn build_test_pipeline_with_recording_reranker(
        store_hits: Vec<Hit>,
        rerank_scores: Vec<f32>,
    ) -> (RetrievalPipelineWithRecording, kb_mock_backend::MockBackend) {
        use kb_core::role::Role;
        use kb_llm::LlamaClient;
        use kb_mock_backend::MockBackend;
        use kb_scheduler::{Pool, test_backend};
        use reqwest::Client;
        use std::time::Duration;

        let mock = MockBackend::start().await;
        let base_url = mock.url("/v1");
        let backend = Arc::new(test_backend("mock-rec", base_url, vec![Role::Embed], 0, 4));
        let pool = Pool::new(vec![backend], Duration::from_secs(5));
        let llm = Arc::new(LlamaClient::new(
            pool,
            Client::new(),
            0,
            0,
            Duration::from_millis(200),
        ));
        let embedder = Arc::new(ChunkEmbedder::new(llm, "test-model".into(), 3));
        let store: Arc<dyn Store> = Arc::new(MockStore::new(store_hits));
        let reranker = Arc::new(RecordingReranker::new(rerank_scores));

        let r: Arc<dyn Reranker> = Arc::clone(&reranker) as Arc<dyn Reranker>;
        let pipeline = RetrievalPipeline::new(Arc::clone(&embedder), store, r);

        // We need to keep the Arc<RecordingReranker> alive for post-assertion.
        // Wrap both in a struct to avoid needing a separate type.
        let wrapper = RetrievalPipelineWithRecording {
            pipeline,
            recording_reranker: reranker,
        };

        (wrapper, mock)
    }

    /// Helper wrapper so tests can inspect the recording reranker.
    struct RetrievalPipelineWithRecording {
        pipeline: RetrievalPipeline,
        recording_reranker: Arc<RecordingReranker>,
    }

    impl std::ops::Deref for RetrievalPipelineWithRecording {
        type Target = RetrievalPipeline;

        fn deref(&self) -> &Self::Target {
            &self.pipeline
        }
    }

    // ── Reranker error propagation ────────────────────────────────────────

    #[tokio::test]
    async fn reranker_error_propagates() {
        let store_hits = vec![hit(1, 0.9, "snippet", 10, None)];

        // Build pipeline with a failing reranker.
        let (pipeline, mock) = build_test_pipeline_with_failing_reranker(store_hits).await;
        let q = query("query", 10);

        let err = pipeline.retrieve(1, &q, false).await.unwrap_err();
        assert!(
            err.to_string().contains("simulated reranker failure"),
            "expected reranker failure error, got: {err}"
        );

        mock.shutdown().await;
    }

    async fn build_test_pipeline_with_failing_reranker(
        store_hits: Vec<Hit>,
    ) -> (RetrievalPipeline, kb_mock_backend::MockBackend) {
        use kb_core::role::Role;
        use kb_llm::LlamaClient;
        use kb_mock_backend::MockBackend;
        use kb_scheduler::{Pool, test_backend};
        use reqwest::Client;
        use std::time::Duration;

        let mock = MockBackend::start().await;
        let base_url = mock.url("/v1");
        let backend = Arc::new(test_backend("mock-fail", base_url, vec![Role::Embed], 0, 4));
        let pool = Pool::new(vec![backend], Duration::from_secs(5));
        let llm = Arc::new(LlamaClient::new(
            pool,
            Client::new(),
            0,
            0,
            Duration::from_millis(200),
        ));
        let embedder = Arc::new(ChunkEmbedder::new(llm, "test-model".into(), 3));
        let store: Arc<dyn Store> = Arc::new(MockStore::new(store_hits));
        let reranker: Arc<dyn Reranker> = Arc::new(FailingReranker);

        let pipeline = RetrievalPipeline::new(embedder, store, reranker);
        (pipeline, mock)
    }

    // ── Embedding passed through to store ─────────────────────────────────

    #[tokio::test]
    async fn query_text_and_embedding_passed_to_store() {
        use kb_core::role::Role;
        use kb_llm::LlamaClient;
        use kb_mock_backend::MockBackend;
        use kb_scheduler::{Pool, test_backend};
        use reqwest::Client;
        use std::time::Duration;

        let mock = MockBackend::start().await;
        let base_url = mock.url("/v1");
        let backend = Arc::new(test_backend("mock-pass", base_url, vec![Role::Embed], 0, 4));
        let pool = Pool::new(vec![backend], Duration::from_secs(5));
        let llm = Arc::new(LlamaClient::new(
            pool,
            Client::new(),
            0,
            0,
            Duration::from_millis(200),
        ));
        let embedder = Arc::new(ChunkEmbedder::new(llm, "test-model".into(), 3));

        let store = Arc::new(RecordingStore::new(vec![hit(1, 0.9, "results", 10, None)]));
        let reranker: Arc<dyn Reranker> = Arc::new(MockReranker::new(vec![0.8]));

        let pipeline =
            RetrievalPipeline::new(embedder, Arc::clone(&store) as Arc<dyn Store>, reranker);

        let q = query("find this text", 10);
        let _results = pipeline.retrieve(1, &q, false).await.unwrap();

        // Verify the store received the correct query text and embedding.
        assert_eq!(store.last_query_text().as_deref(), Some("find this text"));
        // Mock embedder returns 3-dim vectors ([0.1, 0.2, 0.3]).
        assert_eq!(store.last_embedding_len(), Some(3));

        mock.shutdown().await;
    }

    // ── Deep-link provenance preserved after rerank ───────────────────────

    #[tokio::test]
    async fn deep_link_provenance_preserved_after_rerank() {
        let store_hits = vec![Hit {
            document_id: 42,
            score: 0.9,
            title: Some("Important Doc".into()),
            snippet: "the key paragraph".into(),
            file_id: 777,
            page_no: Some(3),
            ts_offset: Some(15.0),
            kind: None,
        }];
        let rerank_scores = vec![0.75];

        let (pipeline, mock) = build_test_pipeline(store_hits, rerank_scores).await;
        let q = query("key", 10);

        let results = pipeline.retrieve(1, &q, false).await.unwrap();
        assert_eq!(results.len(), 1);

        let hit = &results[0];
        assert_eq!(hit.document_id, 42);
        assert_eq!(hit.title.as_deref(), Some("Important Doc"));
        assert_eq!(hit.snippet, "the key paragraph");
        assert_eq!(hit.file_id, 777);
        assert_eq!(hit.page_no, Some(3));
        assert_eq!(hit.ts_offset, Some(15.0));
        // Score is updated by blended RRF + reranker.
        // RRF_BLEND_WEIGHT=0.2, max_rrf=0.9 → blended = 0.2*(0.9/0.9) + 0.8*0.75 = 0.8
        assert!((hit.score - 0.8).abs() < 0.001);

        mock.shutdown().await;
    }

    // ── Reranker length mismatch error ────────────────────────────────────

    #[tokio::test]
    async fn reranker_length_mismatch_is_error() {
        let store_hits = vec![
            hit(1, 0.9, "a", 10, None),
            hit(2, 0.8, "b", 20, None),
            hit(3, 0.7, "c", 30, None),
        ];
        // Return only 2 scores for 3 documents.
        let rerank_scores = vec![0.5, 0.3];

        // We need a mock reranker that returns the wrong length.
        // Use the recording one but with mismatched counts.
        let (pipeline, mock) =
            build_test_pipeline_with_mismatched_reranker(store_hits, rerank_scores).await;
        let q = query("mismatch", 10);

        let err = pipeline.retrieve(1, &q, false).await.unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("reranker returned"),
            "expected length mismatch error, got: {msg}"
        );

        mock.shutdown().await;
    }

    async fn build_test_pipeline_with_mismatched_reranker(
        store_hits: Vec<Hit>,
        rerank_scores: Vec<f32>,
    ) -> (RetrievalPipeline, kb_mock_backend::MockBackend) {
        use kb_core::role::Role;
        use kb_llm::LlamaClient;
        use kb_mock_backend::MockBackend;
        use kb_scheduler::{Pool, test_backend};
        use reqwest::Client;
        use std::time::Duration;

        let mock = MockBackend::start().await;
        let base_url = mock.url("/v1");
        let backend = Arc::new(test_backend(
            "mock-mismatch",
            base_url,
            vec![Role::Embed],
            0,
            4,
        ));
        let pool = Pool::new(vec![backend], Duration::from_secs(5));
        let llm = Arc::new(LlamaClient::new(
            pool,
            Client::new(),
            0,
            0,
            Duration::from_millis(200),
        ));
        let embedder = Arc::new(ChunkEmbedder::new(llm, "test-model".into(), 3));
        let store: Arc<dyn Store> = Arc::new(MockStore::new(store_hits));
        let reranker: Arc<dyn Reranker> = Arc::new(MockReranker::new(rerank_scores));

        let pipeline = RetrievalPipeline::new(embedder, store, reranker);
        (pipeline, mock)
    }

    // ── Ties in reranker scores: RRF scores break ties ─────────────────────

    #[tokio::test]
    async fn tied_rerank_scores_rrf_breaks_tie() {
        let store_hits = vec![
            hit(10, 0.8, "first", 100, Some(1)),
            hit(20, 0.6, "second", 200, Some(1)),
            hit(30, 0.7, "third", 300, None),
        ];
        // All same reranker score → RRF scores determine order.
        let rerank_scores = vec![0.5, 0.5, 0.5];

        let (pipeline, mock) = build_test_pipeline(store_hits, rerank_scores).await;
        let q = query("tied", 10);

        let results = pipeline.retrieve(1, &q, false).await.unwrap();
        assert_eq!(results.len(), 3);
        // With RRF_BLEND_WEIGHT=0.2 and max_rrf=0.8 (norm=1.25):
        //   doc 10: 0.2*1.0   + 0.8*0.5 = 0.600
        //   doc 30: 0.2*0.875 + 0.8*0.5 = 0.575
        //   doc 20: 0.2*0.75  + 0.8*0.5 = 0.550
        assert!((results[0].score - 0.6).abs() < 0.001);
        assert!((results[1].score - 0.575).abs() < 0.001);
        assert!((results[2].score - 0.55).abs() < 0.001);
        // RRF scores break ties: highest RRF score (0.8) ranks first.
        assert_eq!(results[0].document_id, 10);
        assert_eq!(results[1].document_id, 30);
        assert_eq!(results[2].document_id, 20);

        mock.shutdown().await;
    }

    // ── EmbedQuery tests (on ChunkEmbedder, built here for colocation) ────

    /// Verify that [`ChunkEmbedder::embed_query`] returns a vector of the
    /// expected dimension.
    #[tokio::test]
    async fn embed_query_returns_correct_dim() {
        use kb_core::role::Role;
        use kb_llm::LlamaClient;
        use kb_mock_backend::MockBackend;
        use kb_scheduler::{Pool, test_backend};
        use reqwest::Client;
        use std::time::Duration;

        let mock = MockBackend::start().await;
        let base_url = mock.url("/v1");
        let backend = Arc::new(test_backend("mock-eq", base_url, vec![Role::Embed], 0, 4));
        let pool = Pool::new(vec![backend], Duration::from_secs(5));
        let llm = Arc::new(LlamaClient::new(
            pool,
            Client::new(),
            0,
            0,
            Duration::from_millis(200),
        ));
        // Mock returns [0.1, 0.2, 0.3] (dim=3).
        let embedder = ChunkEmbedder::new(llm, "model".into(), 3);

        let vec = embedder.embed_query("find me", false).await.unwrap();
        assert_eq!(vec.len(), 3);

        mock.shutdown().await;
    }

    /// Verify that `embed_query` with wrong expected_dim fails.
    #[tokio::test]
    async fn embed_query_dimension_mismatch_is_error() {
        use kb_core::role::Role;
        use kb_llm::LlamaClient;
        use kb_mock_backend::MockBackend;
        use kb_scheduler::{Pool, test_backend};
        use reqwest::Client;
        use std::time::Duration;

        let mock = MockBackend::start().await;
        let base_url = mock.url("/v1");
        let backend = Arc::new(test_backend(
            "mock-eq-dim",
            base_url,
            vec![Role::Embed],
            0,
            4,
        ));
        let pool = Pool::new(vec![backend], Duration::from_secs(5));
        let llm = Arc::new(LlamaClient::new(
            pool,
            Client::new(),
            0,
            0,
            Duration::from_millis(200),
        ));
        // expected_dim=512 but mock returns dim=3 → error.
        let embedder = ChunkEmbedder::new(llm, "model".into(), 512);

        let err = embedder.embed_query("find me", false).await.unwrap_err();
        assert!(
            err.to_string().contains("dimension mismatch"),
            "expected dimension mismatch, got: {err}"
        );

        mock.shutdown().await;
    }
}
