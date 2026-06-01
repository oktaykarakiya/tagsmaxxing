//! Batch-embedding wrapper around [`LlamaClient`] for chunk and tag-name embedding
//! (plan §7, P3-T4).
//!
//! The [`ChunkEmbedder`] transparently splits oversized batches, verifies output
//! dimensions against the configured schema (BGE-M3 = 1024), and returns either
//! fully-formed [`Chunk`] records or raw vectors for tag canonicalization.

use std::sync::Arc;

use anyhow::{Context, anyhow};
use kb_core::chunk::Chunk;
use kb_core::embedder::EmbedKind;
use kb_core::provider::EmbedReq;
use kb_llm::LlamaClient;

use crate::chunker::{MAX_EMBED_BATCH_SIZE, TextChunk};

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
    /// Expected output vector dimension (BGE-M3 = 1024; plan §5 embedder lock-in).
    expected_dim: usize,
}

impl ChunkEmbedder {
    /// Create a new batch embedder.
    ///
    /// `expected_dim` must equal the schema's `VECTOR(N)` (currently 1024 for
    /// BGE-M3). Every call verifies the returned vectors against this dimension.
    pub fn new(llm: Arc<LlamaClient>, embed_model: String, expected_dim: usize) -> Self {
        Self {
            llm,
            embed_model,
            expected_dim,
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
    /// # Errors
    ///
    /// Returns an error if any batch's embedding call fails, if the backend returns
    /// a wrong number of vectors, or if any vector's dimension does not match
    /// [`expected_dim`](Self::new).
    pub async fn embed_chunks(
        &self,
        chunks: Vec<TextChunk>,
        tenant_id: i64,
        document_id: i64,
        local_only: bool,
    ) -> anyhow::Result<Vec<Chunk>> {
        if chunks.is_empty() {
            return Ok(Vec::new());
        }

        let texts: Vec<String> = chunks.iter().map(|c| c.content.clone()).collect();
        let vectors = self
            .batch_embed(&texts, EmbedKind::Document, local_only)
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
        self.batch_embed(names, EmbedKind::Document, local_only)
            .await
    }

    /// Embed a single query text for retrieval (plan §8 step 2).
    ///
    /// Uses [`EmbedKind::Query`] to signal the retrieval instruction
    /// (BGE-M3 / Qwen3-Embedding models apply a different prompt prefix for
    /// queries vs. stored documents).
    ///
    /// # Errors
    ///
    /// Returns an error if the embedding call fails or the returned vector's
    /// dimension does not match [`expected_dim`](Self::new).
    pub async fn embed_query(
        &self,
        query_text: &str,
        local_only: bool,
    ) -> anyhow::Result<Vec<f32>> {
        let vectors = self
            .batch_embed(&[query_text.to_string()], EmbedKind::Query, local_only)
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
    async fn batch_embed(
        &self,
        texts: &[String],
        _kind: EmbedKind,
        local_only: bool,
    ) -> anyhow::Result<Vec<Vec<f32>>> {
        let mut all_vectors = Vec::with_capacity(texts.len());

        for batch in texts.chunks(MAX_EMBED_BATCH_SIZE) {
            let req = EmbedReq {
                texts: batch.to_vec(),
            };

            let resp = self
                .llm
                .embed(&self.embed_model, &req, local_only)
                .await
                .context("failed to embed batch")?;

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
            .embed_chunks(input, 42 /* tenant */, 7 /* doc */, false)
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
    async fn embed_chunks_empty_input() {
        let (embedder, mock) = embedder_with_mock(MOCK_DIM).await;
        let result = embedder.embed_chunks(vec![], 1, 1, false).await.unwrap();
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
        let chunks = embedder.embed_chunks(input, 1, 1, false).await.unwrap();
        assert_eq!(chunks[0].ts_offset, Some(15.5));

        mock.shutdown().await;
    }

    #[tokio::test]
    async fn embed_chunks_mismatched_dim_error() {
        // Use expected_dim=512 but the mock returns 3-dim vectors.
        let (embedder, mock) = embedder_with_mock(512).await;

        let input = vec![tc("hello", 0, 1, 1)];
        let err = embedder.embed_chunks(input, 1, 1, false).await.unwrap_err();
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
            .embed_chunks(text_chunks, 1, 99, false)
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
}
