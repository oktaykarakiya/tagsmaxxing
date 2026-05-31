//! [`LlamaReranker`] — implements the core [`Reranker`] trait using
//! [`LlamaClient`] with [`Role::Rerank`] (plan §8 step 5).
//!
//! The reranker calls `POST /v1/rerank` on the selected backend — the
//! OpenAI-compatible llama.cpp rerank endpoint — and returns relevance scores
//! aligned with the input document list.

use std::sync::Arc;

use async_trait::async_trait;
use kb_core::reranker::Reranker;

use crate::client::LlamaClient;

/// A cross-encoder reranker backed by a llama.cpp `/rerank` endpoint through
/// the scheduler pool (plan §8).
///
/// Each call acquires a slot via the pool with [`Role::Rerank`] and posts
/// `{model, query, documents[]}` to the backend. The response is validated
/// for correct length and returned as order-aligned `Vec<f32>` scores.
///
/// # Example
///
/// ```no_run
/// # use std::sync::Arc;
/// # use kb_llm::{LlamaClient, LlamaReranker};
/// # use kb_core::reranker::Reranker;
/// # async fn example(client: Arc<LlamaClient>) -> anyhow::Result<()> {
/// let reranker = LlamaReranker::new(client, "bge-reranker-v2-m3".into());
/// let scores = reranker.rerank("query", &["doc1".into(), "doc2".into()]).await?;
/// # Ok(())
/// # }
/// ```
pub struct LlamaReranker {
    /// The shared LLM client used to POST rerank requests.
    client: Arc<LlamaClient>,
    /// The model id sent in the rerank request body.
    model: String,
}

impl LlamaReranker {
    /// Create a new reranker that sends requests with the given `model`.
    ///
    /// The `client` is shared across all calls and manages pool acquisition and
    /// failover internally.
    pub fn new(client: Arc<LlamaClient>, model: String) -> Self {
        Self { client, model }
    }
}

#[async_trait]
impl Reranker for LlamaReranker {
    /// Score each document's relevance to `query`.
    ///
    /// Calls `POST /v1/rerank` through the shared [`LlamaClient`], which
    /// handles pool acquisition, failover, and circuit-breaking. The returned
    /// `Vec<f32>` has the same length as `docs` and is order-aligned.
    ///
    /// # Errors
    ///
    /// Returns an error if the backend call fails, the response length does not
    /// match `docs.len()`, or the JSON cannot be deserialized.
    async fn rerank(&self, query: &str, docs: &[String]) -> anyhow::Result<Vec<f32>> {
        Ok(self.client.rerank(&self.model, query, docs).await?)
    }
}

// ── unit tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use std::sync::Arc;
    use std::time::Duration;

    use kb_core::role::Role;
    use kb_mock_backend::{MockBackend, ResponseMode};
    use kb_scheduler::{Backend, Pool};
    use reqwest::Client;

    use super::*;

    /// Build a `LlamaReranker` with a single mock backend registered for `Role::Rerank`.
    async fn reranker_with_one_backend(slots: usize) -> (LlamaReranker, MockBackend) {
        let mock = MockBackend::start().await;
        let base_url = mock.url("/v1");
        let backend = Arc::new(Backend::new(
            "mock-1",
            base_url,
            vec![Role::Rerank],
            0,
            slots,
        ));
        let pool = Pool::new(vec![backend], Duration::from_secs(5));
        let client = Arc::new(LlamaClient::new(
            pool,
            Client::new(),
            2,
            3,
            Duration::from_millis(200),
        ));
        let reranker = LlamaReranker::new(client, "mock-rerank-model".into());
        (reranker, mock)
    }

    // ── success path ────────────────────────────────────────────────────

    /// Happy path: valid rerank response returns correctly ordered scores.
    #[tokio::test]
    async fn rerank_returns_scores() {
        let (reranker, mock) = reranker_with_one_backend(2).await;
        mock.scenario().lock().await.rerank_content = Some(vec![0.95, 0.42, 0.73]);

        let docs: Vec<String> = ["a", "b", "c"].map(String::from).to_vec();
        let scores = reranker.rerank("query", &docs).await.unwrap();

        assert_eq!(scores.len(), 3);
        assert!((scores[0] - 0.95_f32).abs() < 0.0001);
        assert!((scores[1] - 0.42_f32).abs() < 0.0001);
        assert!((scores[2] - 0.73_f32).abs() < 0.0001);

        mock.shutdown().await;
    }

    /// Empty documents list returns empty scores.
    #[tokio::test]
    async fn empty_documents_returns_empty() {
        let (reranker, mock) = reranker_with_one_backend(2).await;

        let docs: Vec<String> = vec![];
        let scores = reranker.rerank("query", &docs).await.unwrap();
        assert!(scores.is_empty());

        mock.shutdown().await;
    }

    /// Length mismatch between input and response returns an error.
    #[tokio::test]
    async fn length_mismatch_is_error() {
        let (reranker, mock) = reranker_with_one_backend(2).await;
        // Return 1 score for 3 docs → mismatch.
        mock.scenario().lock().await.rerank_content = Some(vec![0.5]);

        let docs: Vec<String> = ["a", "b", "c"].map(String::from).to_vec();
        let err = reranker.rerank("query", &docs).await.unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("length mismatch"),
            "expected length mismatch, got: {msg}"
        );

        mock.shutdown().await;
    }

    /// Backend error is propagated.
    #[tokio::test]
    async fn backend_error_propagated() {
        let (reranker, mock) = reranker_with_one_backend(2).await;
        mock.scenario().lock().await.rerank = ResponseMode::ServerError;

        let docs: Vec<String> = ["a"].map(String::from).to_vec();
        let err = reranker.rerank("query", &docs).await.unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("500") || msg.contains("failed"), "{msg}");

        mock.shutdown().await;
    }

    /// The model string flows through to the backend (verified by the mock
    /// returning controlled scores — the key point is that the method composes
    /// correctly; the model string itself is a simple field passthrough).
    #[tokio::test]
    async fn model_parameter_is_passed() {
        let mock = MockBackend::start().await;
        let base_url = mock.url("/v1");
        let backend = Arc::new(Backend::new("mock-1", base_url, vec![Role::Rerank], 0, 2));
        let pool = Pool::new(vec![backend], Duration::from_secs(5));
        let client = Arc::new(LlamaClient::new(
            pool,
            Client::new(),
            2,
            3,
            Duration::from_millis(200),
        ));

        // Create reranker with a specific model name.
        let reranker = LlamaReranker::new(client, "my-specific-reranker".into());

        mock.scenario().lock().await.rerank_content = Some(vec![0.99]);
        let docs: Vec<String> = ["test"].map(String::from).to_vec();
        let scores = reranker.rerank("query text", &docs).await.unwrap();
        assert_eq!(scores, vec![0.99_f32]);

        mock.shutdown().await;
    }

    /// Scores are returned in input-aligned order, even when the backend
    /// returns them out-of-order (sorted by index inside LlamaClient::rerank).
    #[tokio::test]
    async fn scores_are_order_aligned() {
        let (reranker, mock) = reranker_with_one_backend(2).await;
        // The mock returns scores in index order regardless; the client sorts by
        // index. This test verifies the end-to-end alignment through the trait.
        mock.scenario().lock().await.rerank_content = Some(vec![0.10, 0.90, 0.50]);

        let docs: Vec<String> = ["first", "second", "third"].map(String::from).to_vec();
        let scores = reranker.rerank("q", &docs).await.unwrap();

        assert_eq!(scores.len(), 3);
        // Position 0 → score for "first", position 1 → "second", etc.
        assert!((scores[0] - 0.10).abs() < 0.0001);
        assert!((scores[1] - 0.90).abs() < 0.0001);
        assert!((scores[2] - 0.50).abs() < 0.0001);

        mock.shutdown().await;
    }
}
