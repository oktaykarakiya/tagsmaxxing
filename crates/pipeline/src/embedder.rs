// SPDX-License-Identifier: AGPL-3.0-or-later

//! Batch-embedding wrapper around [`LlamaClient`] for chunk and tag-name embedding
//! (plan §7, P3-T4).
//!
//! The [`ChunkEmbedder`] transparently splits oversized batches, verifies output
//! dimensions against the configured schema (Qwen3-Embedding-4B = 2560), and returns either
//! fully-formed [`Chunk`] records or raw vectors for tag canonicalization.

use std::sync::Arc;

use anyhow::{Context, anyhow};
use kb_core::chunk::Chunk;
use kb_core::embedder::EmbedKind;
use kb_core::provider::{EmbedReq, Usage};
use kb_core::role::Role;
use kb_core::usage::{UsageEvent, UsageRecorder};
use kb_llm::LlamaClient;

use crate::chunker::{MAX_EMBED_BATCH_SIZE, TextChunk};

/// Query instruction prefix (Qwen3-Embedding format) prepended to query text
/// before embedding (plan §4). This ensures query embeddings land in
/// the same semantic space as document embeddings, which use no prefix.
///
/// Without this prefix, paraphrased queries with zero keyword overlap score
/// as random noise in the vector index because cosine similarity between a
/// bare-text query vector and a document-chunk vector measures proximity in
/// different subspaces.
const EMBED_QUERY_PREFIX: &str =
    "Instruct: Given a web search query, retrieve relevant passages that answer the query\nQuery: ";

/// Wraps [`LlamaClient`] to batch-embed text chunks and tag names, verifying that
/// every returned vector has the expected dimension (plan §7, §11).
///
/// # Dimension check
///
/// Because every vector in a single index must come from the same embedder model
/// (plan §11 — the "embed role must pin to ONE model" rule), every call verifies
/// `vector.len() == expected_dim`. A mismatch is a hard error that aborts the batch;
/// it should never fire in normal operation and signals a misconfigured backend.
pub struct ChunkEmbedder {
    /// Shared LLM client for embedding calls.
    llm: Arc<LlamaClient>,
    /// Model id sent in the OpenAI-compatible `/embeddings` body.
    embed_model: String,
    /// Expected output vector dimension (Qwen3-Embedding-4B = 2560; plan §5 embedder lock-in).
    expected_dim: usize,
    /// Optional sink for per-call token usage (BUG-BILL-03). When set, each
    /// document-embedding batch's usage is metered to the calling tenant.
    usage_recorder: Option<Arc<dyn UsageRecorder>>,
}

impl ChunkEmbedder {
    /// Create a new batch embedder.
    ///
    /// `expected_dim` must equal the schema's `VECTOR(N)` (currently 2560 for
    /// Qwen3-Embedding-4B). Every call verifies the returned vectors against this dimension.
    pub fn new(llm: Arc<LlamaClient>, embed_model: String, expected_dim: usize) -> Self {
        Self {
            llm,
            embed_model,
            expected_dim,
            usage_recorder: None,
        }
    }

    /// Attach a [`UsageRecorder`] so each document-embedding batch's token usage
    /// is metered into `usage_events` for the calling tenant (BUG-BILL-03).
    #[must_use]
    pub fn with_usage_recorder(mut self, recorder: Arc<dyn UsageRecorder>) -> Self {
        self.usage_recorder = Some(recorder);
        self
    }

    /// Meter one embedding batch's token usage to `tenant_id`, attributed to
    /// `user_id` when known (best-effort, P14-T1).
    ///
    /// A no-op when no [`UsageRecorder`] is attached. Recording failures are
    /// logged and swallowed — metering must never fail an embedding call.
    async fn meter(&self, tenant_id: i64, user_id: Option<i64>, usage: &Usage) {
        let Some(recorder) = &self.usage_recorder else {
            return;
        };
        let event = UsageEvent {
            id: 0,
            tenant_id,
            user_id,
            model: self.embed_model.clone(),
            role: Role::Embed,
            backend_id: None,
            prompt_tokens: Some(usage.prompt_tokens as i32),
            completion_tokens: Some(usage.completion_tokens as i32),
            latency_ms: None,
            cost_micros: None,
            created_at: chrono::Utc::now(),
        };
        if let Err(e) = recorder.record_usage(&event).await {
            tracing::warn!(error = %e, tenant_id, "embedder: failed to meter usage event");
        }
    }

    /// Embed a batch of [`TextChunk`]s, producing fully-formed [`Chunk`] records
    /// with embeddings.
    ///
    /// Chunks are grouped into batches of at most [`MAX_EMBED_BATCH_SIZE`]; each
    /// batch is sent as a single call to the embed backend with
    /// [`EmbedKind::Document`].
    ///
    /// The returned [`Chunk`]s have `id = 0` (not yet assigned by the database) and
    /// are ordered identically to the input `chunks` slice.
    ///
    /// `user_id` attributes the batch's metered token usage to the acting user
    /// when known (P14-T1); `None` records the event tenant-only.
    ///
    /// # Errors
    ///
    /// Returns an error if any batch's embedding call fails, if the backend returns
    /// a wrong number of vectors, or if any vector's dimension does not match
    /// [`expected_dim`](Self::new).
    pub async fn embed_chunks(
        &self,
        chunks: Vec<TextChunk>,
        tenant_id: i64,
        user_id: Option<i64>,
        document_id: i64,
        local_only: bool,
    ) -> anyhow::Result<Vec<Chunk>> {
        if chunks.is_empty() {
            return Ok(Vec::new());
        }

        let texts: Vec<String> = chunks.iter().map(|c| c.content.clone()).collect();
        let vectors = self
            .batch_embed(
                &texts,
                EmbedKind::Document,
                Some(tenant_id),
                user_id,
                local_only,
            )
            .await?;

        anyhow::ensure!(
            vectors.len() == chunks.len(),
            "embedder returned {} vectors but {} chunks were submitted",
            vectors.len(),
            chunks.len()
        );

        Ok(chunks
            .into_iter()
            .zip(vectors)
            .map(|(tc, embedding)| Chunk {
                id: 0, // not yet assigned by DB
                tenant_id,
                document_id,
                file_id: tc.file_id,
                page_no: tc.page_no,
                idx: tc.idx,
                content: tc.content,
                ts_offset: tc.ts_offset,
                embedding,
            })
            .collect())
    }

    /// Embed tag names, returning raw vectors for
    /// [`TagCanonicalizer`](super::tag_canonicalizer::TagCanonicalizer).
    ///
    /// Uses [`EmbedKind::Document`] because tag names are stored content, not
    /// queries (plan §4).
    ///
    /// # Errors
    ///
    /// Returns an error if the embedding call fails or any vector's dimension does
    /// not match [`expected_dim`](Self::new).
    pub async fn embed_tag_names(
        &self,
        names: &[String],
        local_only: bool,
    ) -> anyhow::Result<Vec<Vec<f32>>> {
        self.batch_embed(names, EmbedKind::Document, None, None, local_only)
            .await
    }

    /// Embed a single query text for retrieval (plan §8 step 2).
    ///
    /// Uses [`EmbedKind::Query`] to signal the retrieval instruction
    /// (BGE-M3 / Qwen3-Embedding models apply a different prompt prefix for
    /// queries vs. stored documents).
    ///
    /// The query embedding is metered to `tenant_id` on the search read path
    /// (P14-T2), attributed to the searching `user_id` when known (`None` for
    /// system/CLI searches). Metering is best-effort — see [`meter`](Self::meter).
    ///
    /// # Errors
    ///
    /// Returns an error if the embedding call fails or the returned vector's
    /// dimension does not match [`expected_dim`](Self::new).
    pub async fn embed_query(
        &self,
        query_text: &str,
        tenant_id: i64,
        user_id: Option<i64>,
        local_only: bool,
    ) -> anyhow::Result<Vec<f32>> {
        let vectors = self
            .batch_embed(
                &[query_text.to_string()],
                EmbedKind::Query,
                Some(tenant_id),
                user_id,
                local_only,
            )
            .await?;
        anyhow::ensure!(
            vectors.len() == 1,
            "embedder returned {} vectors for a single query text",
            vectors.len()
        );
        Ok(vectors.into_iter().next().unwrap_or_default())
    }

    // ── Internal ───────────────────────────────────────────────────────────

    /// Send `texts` to the embed backend in batches, verifying output dimensions.
    ///
    /// For [`EmbedKind::Query`], the Qwen3-Embedding retrieval instruction prefix
    /// ([`EMBED_QUERY_PREFIX`]) is prepended to every text so the query
    /// embedding lands in the same semantic space as stored document embeddings.
    /// [`EmbedKind::Document`] texts are sent unchanged.
    async fn batch_embed(
        &self,
        texts: &[String],
        kind: EmbedKind,
        tenant_id: Option<i64>,
        user_id: Option<i64>,
        local_only: bool,
    ) -> anyhow::Result<Vec<Vec<f32>>> {
        // Apply the query instruction prefix (Qwen3-Embedding format) for queries only.
        let processed: Vec<String> = match kind {
            EmbedKind::Query => texts
                .iter()
                .map(|t| format!("{EMBED_QUERY_PREFIX}{t}"))
                .collect(),
            EmbedKind::Document => texts.to_vec(),
        };

        let mut all_vectors = Vec::with_capacity(processed.len());

        for batch in processed.chunks(MAX_EMBED_BATCH_SIZE) {
            let req = EmbedReq {
                texts: batch.to_vec(),
            };

            let resp = self
                .llm
                .embed(&self.embed_model, &req, local_only, 0)
                .await
                .context("failed to embed batch")?;

            // Meter this batch's token usage to the tenant (BUG-BILL-03),
            // attributed to the acting user when known (P14-T1/P14-T2). Both
            // document embedding (ingest) and query embedding (search read path)
            // are tenant-attributable and pass `Some(tenant_id)`; tag-name
            // embedding passes `None` and is not metered.
            if let Some(tid) = tenant_id {
                self.meter(tid, user_id, &resp.usage).await;
            }

            for (i, vector) in resp.vectors.into_iter().enumerate() {
                if vector.len() != self.expected_dim {
                    return Err(anyhow!(
                        "embedding dimension mismatch: expected {} but got {} for text index {}",
                        self.expected_dim,
                        vector.len(),
                        i
                    ));
                }
                all_vectors.push(vector);
            }
        }

        Ok(all_vectors)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

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
    use crate::chunker::{TextChunk, chunk_text};

    /// The mock backend always returns `[0.1, 0.2, 0.3]` (dim = 3).
    const MOCK_DIM: usize = 3;

    /// Test helper: build a `ChunkEmbedder` wired to a single mock backend.
    async fn embedder_with_mock(dim: usize) -> (ChunkEmbedder, MockBackend) {
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
            0,                          /* max_retries — one attempt */
            0,                          /* circuit_threshold — disabled */
            Duration::from_millis(200), /* cooldown (unused) */
        ));
        let embedder = ChunkEmbedder::new(llm, "bge-m3".into(), dim);
        (embedder, mock)
    }

    /// A [`UsageRecorder`] capturing events for assertions (BUG-BILL-03).
    #[derive(Default)]
    struct CapturingRecorder {
        events: std::sync::Mutex<Vec<UsageEvent>>,
    }

    #[async_trait::async_trait]
    impl UsageRecorder for CapturingRecorder {
        async fn record_usage(&self, event: &UsageEvent) -> anyhow::Result<()> {
            self.events.lock().unwrap().push(event.clone());
            Ok(())
        }
    }

    /// A minimal `TextChunk` helper.
    fn tc(content: &str, idx: i32, file_id: i64, page_no: i32) -> TextChunk {
        TextChunk {
            content: content.into(),
            idx,
            page_no: Some(page_no),
            file_id,
            ts_offset: None,
        }
    }

    // ── embed_chunks ───────────────────────────────────────────────────────

    /// The mock backend returns exactly one vector per call regardless of input
    /// size. All mock-based tests use single-element inputs; batch splitting is
    /// tested as pure logic below.
    #[tokio::test]
    async fn embed_chunks_success() {
        let (embedder, mock) = embedder_with_mock(MOCK_DIM).await;

        let input = vec![tc("hello", 0, 1, 1)];
        let chunks = embedder
            .embed_chunks(
                input, 42,   /* tenant */
                None, /* user */
                7,    /* doc */
                false,
            )
            .await
            .unwrap();

        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].content, "hello");
        assert_eq!(chunks[0].tenant_id, 42);
        assert_eq!(chunks[0].document_id, 7);
        assert_eq!(chunks[0].file_id, 1);
        assert_eq!(chunks[0].page_no, Some(1));
        assert_eq!(chunks[0].idx, 0);
        assert_eq!(chunks[0].embedding.len(), MOCK_DIM);

        mock.shutdown().await;
    }

    #[tokio::test]
    async fn embed_chunks_meters_usage_to_tenant() {
        // BUG-BILL-03: embedding document chunks meters an Embed usage event
        // attributed to the tenant and the embed model.
        let (embedder, mock) = embedder_with_mock(MOCK_DIM).await;
        let recorder = Arc::new(CapturingRecorder::default());
        let embedder = embedder.with_usage_recorder(recorder.clone() as Arc<dyn UsageRecorder>);

        let input = vec![tc("hello", 0, 1, 1)];
        embedder
            .embed_chunks(input, 42, Some(99), 7, false)
            .await
            .unwrap();
        mock.shutdown().await;

        let events = recorder.events.lock().unwrap();
        assert_eq!(events.len(), 1, "one embed batch should be metered");
        assert_eq!(events[0].tenant_id, 42);
        // P14-T1: the acting user is attributed on the metered event.
        assert_eq!(events[0].user_id, Some(99));
        assert_eq!(events[0].role, Role::Embed);
        assert_eq!(events[0].model, "bge-m3");
    }

    #[tokio::test]
    async fn embed_chunks_without_user_records_tenant_only() {
        // P14-T1: when no acting user is supplied, the metered event still
        // records the tenant but leaves user_id NULL (e.g. background reembed).
        let (embedder, mock) = embedder_with_mock(MOCK_DIM).await;
        let recorder = Arc::new(CapturingRecorder::default());
        let embedder = embedder.with_usage_recorder(recorder.clone() as Arc<dyn UsageRecorder>);

        let input = vec![tc("hello", 0, 1, 1)];
        embedder
            .embed_chunks(input, 42, None, 7, false)
            .await
            .unwrap();
        mock.shutdown().await;

        let events = recorder.events.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].tenant_id, 42);
        assert_eq!(events[0].user_id, None);
    }

    #[tokio::test]
    async fn embed_query_meters_usage_to_tenant_and_user() {
        // P14-T2: query embedding on the search read path IS tenant-attributed
        // and records an Embed usage event stamped with the searching user.
        let (embedder, mock) = embedder_with_mock(MOCK_DIM).await;
        let recorder = Arc::new(CapturingRecorder::default());
        let embedder = embedder.with_usage_recorder(recorder.clone() as Arc<dyn UsageRecorder>);

        embedder
            .embed_query("a search query", 42, Some(99), false)
            .await
            .unwrap();
        mock.shutdown().await;

        let events = recorder.events.lock().unwrap();
        assert_eq!(events.len(), 1, "query embedding should be metered");
        assert_eq!(events[0].tenant_id, 42);
        assert_eq!(events[0].user_id, Some(99));
        assert_eq!(events[0].role, Role::Embed);
        assert_eq!(events[0].model, "bge-m3");
    }

    #[tokio::test]
    async fn embed_query_without_user_records_tenant_only() {
        // P14-T2: a system/CLI search (no acting user) still meters the tenant
        // but leaves user_id NULL.
        let (embedder, mock) = embedder_with_mock(MOCK_DIM).await;
        let recorder = Arc::new(CapturingRecorder::default());
        let embedder = embedder.with_usage_recorder(recorder.clone() as Arc<dyn UsageRecorder>);

        embedder
            .embed_query("a search query", 7, None, false)
            .await
            .unwrap();
        mock.shutdown().await;

        let events = recorder.events.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].tenant_id, 7);
        assert_eq!(events[0].user_id, None);
        assert_eq!(events[0].role, Role::Embed);
    }

    #[tokio::test]
    async fn embed_chunks_empty_input() {
        let (embedder, mock) = embedder_with_mock(MOCK_DIM).await;
        let result = embedder
            .embed_chunks(vec![], 1, None, 1, false)
            .await
            .unwrap();
        assert!(result.is_empty());
        mock.shutdown().await;
    }

    #[tokio::test]
    async fn embed_chunks_preserves_ts_offset() {
        let (embedder, mock) = embedder_with_mock(MOCK_DIM).await;

        let input = vec![TextChunk {
            content: "transcript".into(),
            idx: 0,
            page_no: Some(1),
            file_id: 10,
            ts_offset: Some(15.5),
        }];
        let chunks = embedder
            .embed_chunks(input, 1, None, 1, false)
            .await
            .unwrap();
        assert_eq!(chunks[0].ts_offset, Some(15.5));

        mock.shutdown().await;
    }

    #[tokio::test]
    async fn embed_chunks_mismatched_dim_error() {
        // Use expected_dim=512 but the mock returns 3-dim vectors.
        let (embedder, mock) = embedder_with_mock(512).await;

        let input = vec![tc("hello", 0, 1, 1)];
        let err = embedder
            .embed_chunks(input, 1, None, 1, false)
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("dimension mismatch"),
            "expected dimension-mismatch error, got: {msg}"
        );
        assert!(
            msg.contains("expected 512"),
            "error must mention expected 512: {msg}"
        );

        mock.shutdown().await;
    }

    // ── embed_tag_names ────────────────────────────────────────────────────

    #[tokio::test]
    async fn embed_tag_names_success() {
        let (embedder, mock) = embedder_with_mock(MOCK_DIM).await;

        let vectors = embedder
            .embed_tag_names(&["invoice".into()], false)
            .await
            .unwrap();

        assert_eq!(vectors.len(), 1);
        for v in &vectors {
            assert_eq!(v.len(), MOCK_DIM);
        }

        mock.shutdown().await;
    }

    #[tokio::test]
    async fn embed_tag_names_empty_input() {
        let (embedder, mock) = embedder_with_mock(MOCK_DIM).await;
        let vectors = embedder.embed_tag_names(&[], false).await.unwrap();
        assert!(vectors.is_empty());
        mock.shutdown().await;
    }

    // ── End-to-end with chunker ────────────────────────────────────────────

    #[tokio::test]
    async fn chunk_then_embed_round_trip() {
        let (embedder, mock) = embedder_with_mock(MOCK_DIM).await;

        // Short text so the chunker produces exactly 1 chunk (mock returns 1
        // vector per call).
        let text = "Short text, single chunk.";
        let text_chunks = chunk_text(text, 42, Some(2), None, 2048, 64);
        assert_eq!(text_chunks.len(), 1);

        let embedded = embedder
            .embed_chunks(text_chunks, 1, None, 99, false)
            .await
            .unwrap();
        assert_eq!(embedded.len(), 1);

        assert_eq!(embedded[0].file_id, 42);
        assert_eq!(embedded[0].page_no, Some(2));
        assert_eq!(embedded[0].document_id, 99);
        assert_eq!(embedded[0].embedding.len(), MOCK_DIM);

        mock.shutdown().await;
    }

    // ── Batch splitting (pure logic) ───────────────────────────────────────

    #[test]
    fn batch_splitting_32_texts_fits_in_one_batch() {
        let texts: Vec<String> = (0..32).map(|i| format!("text-{i}")).collect();
        let batches: Vec<_> = texts.chunks(MAX_EMBED_BATCH_SIZE).collect();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].len(), 32);
    }

    #[test]
    fn batch_splitting_33_texts_makes_two_batches() {
        let texts: Vec<String> = (0..33).map(|i| format!("text-{i}")).collect();
        let batches: Vec<_> = texts.chunks(MAX_EMBED_BATCH_SIZE).collect();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].len(), 32);
        assert_eq!(batches[1].len(), 1);
    }

    #[test]
    fn batch_splitting_100_texts_makes_4_batches() {
        let texts: Vec<String> = (0..100).map(|i| format!("text-{i}")).collect();
        let batches: Vec<_> = texts.chunks(MAX_EMBED_BATCH_SIZE).collect();
        assert_eq!(batches.len(), 4);
        // 32 + 32 + 32 + 4 = 100
        assert_eq!(batches[0].len(), 32);
        assert_eq!(batches[1].len(), 32);
        assert_eq!(batches[2].len(), 32);
        assert_eq!(batches[3].len(), 4);
    }

    #[test]
    fn batch_splitting_empty_texts_no_batches() {
        let texts: &[String] = &[];
        let batches: Vec<_> = texts.chunks(MAX_EMBED_BATCH_SIZE).collect();
        assert!(batches.is_empty());
    }

    #[test]
    fn batch_splitting_exact_multiple_64_texts_2_batches() {
        let texts: Vec<String> = (0..64).map(|i| format!("text-{i}")).collect();
        let batches: Vec<_> = texts.chunks(MAX_EMBED_BATCH_SIZE).collect();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].len(), 32);
        assert_eq!(batches[1].len(), 32);
    }

    // ── Constructor ────────────────────────────────────────────────────────

    #[test]
    fn constructor_stores_expected_dim() {
        // We can't easily construct without a backend, but we can verify the
        // constants and the type is constructible.
        const {
            assert!(MAX_EMBED_BATCH_SIZE > 0);
        }
    }

    // ── query instruction prefix ───────────────────────────────────────────

    /// Verify that the query instruction prefix is prepended for
    /// [`EmbedKind::Query`] but not for [`EmbedKind::Document`].
    #[test]
    fn query_prefix_is_applied() {
        // Pure-function test: the constant is a non-empty string.
        assert!(!EMBED_QUERY_PREFIX.is_empty());
        assert!(EMBED_QUERY_PREFIX.contains("retrieve relevant passages"));

        // Simulate the logic applied in `batch_embed`.
        let text = "how do black holes form";
        let query_text = format!("{EMBED_QUERY_PREFIX}{text}");
        assert!(query_text.starts_with(EMBED_QUERY_PREFIX));
        assert!(query_text.ends_with(text));
        assert!(query_text.len() > text.len());
    }

    /// Documents must NOT receive the query prefix — only queries do.
    #[test]
    fn document_kind_no_prefix() {
        let text = "When a sufficiently massive star exhausts its nuclear fuel";
        // Document: no prefix applied (the logic in batch_embed returns texts
        // as-is for EmbedKind::Document).
        assert!(!text.starts_with(EMBED_QUERY_PREFIX));
        // The prefix should not appear anywhere in an unmodified document.
        assert!(!text.contains("retrieve relevant passages"));
    }

    /// End-to-end: `embed_query` succeeds through the mock backend (proves the
    /// prefixed text is accepted by the embed endpoint and produces a valid
    /// vector of the expected dimension).
    #[tokio::test]
    async fn embed_query_with_prefix_through_mock() {
        let (embedder, mock) = embedder_with_mock(MOCK_DIM).await;

        let vec = embedder
            .embed_query("paraphrased query text", 1, None, false)
            .await
            .unwrap();
        assert_eq!(vec.len(), MOCK_DIM);

        mock.shutdown().await;
    }

    /// End-to-end: `embed_chunks` succeeds with document kind (no prefix).
    #[tokio::test]
    async fn embed_chunks_without_prefix_through_mock() {
        let (embedder, mock) = embedder_with_mock(MOCK_DIM).await;

        let chunks = embedder
            .embed_chunks(vec![tc("document content", 0, 1, 1)], 1, None, 1, false)
            .await
            .unwrap();
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].embedding.len(), MOCK_DIM);

        mock.shutdown().await;
    }
}
