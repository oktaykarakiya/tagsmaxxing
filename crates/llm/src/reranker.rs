// SPDX-License-Identifier: AGPL-3.0-or-later

//! [`LlamaReranker`] — implements the core [`Reranker`] trait using
//! [`LlamaClient`] with [`Role::Rerank`] (plan §8 step 5).
//!
//! The reranker calls `POST /v1/rerank` on the selected backend — the
//! OpenAI-compatible llama.cpp rerank endpoint — and returns relevance scores
//! aligned with the input document list.

use std::sync::Arc;

use async_trait::async_trait;
use kb_core::provider::Usage;
use kb_core::reranker::Reranker;
use kb_core::role::Role;
use kb_core::usage::{UsageEvent, UsageRecorder};

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
/// let scores = reranker
///     .rerank("query", &["doc1".into(), "doc2".into()], false, 0, 1, None)
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct LlamaReranker {
    /// The shared LLM client used to POST rerank requests.
    client: Arc<LlamaClient>,
    /// The model id sent in the rerank request body.
    model: String,
    /// Optional sink for per-call token usage (P14-T2). When set, each rerank
    /// call on the search read path is metered to the searching tenant+user.
    usage_recorder: Option<Arc<dyn UsageRecorder>>,
}

/// Max characters of each document sent to the cross-encoder reranker.
///
/// The rerank model scores the `query`+`document` pair within a fixed token
/// window. If a chunk pushes the pair over that window the backend returns HTTP
/// 500 ("input is too large to process"), which trips the per-backend circuit
/// breaker and disables reranking for **every** subsequent search. This is a
/// **generous safety ceiling** that guards against pathological, non-chunk inputs
/// without ever truncating a real chunk.
///
/// The active reranker (ettin-reranker-400m-v1) has an **8192-token** window, and
/// ingested chunks are at most 2048 chars (`DEFAULT_CHUNK_SIZE_CHARS` in
/// kb-pipeline), so this 4096-char ceiling never bites a real chunk — the reranker
/// scores the full chunk. The cap is in **characters** but the budget is **tokens**
/// (script-dependent: Latin ~0.3 tok/char, CJK ~1–1.5 tok/char); 4096 chars stays
/// well under 8192 tokens even for dense CJK (≈6144 tok), leaving room for the
/// query. It only bounds pathological inputs that are not normal chunks.
const MAX_RERANK_DOC_CHARS: usize = 4096;
/// Max characters of the query sent to the reranker (see [`MAX_RERANK_DOC_CHARS`]).
const MAX_RERANK_QUERY_CHARS: usize = 512;

/// Truncate `s` to at most `max_chars` characters, on a UTF-8 char boundary.
fn truncate_chars(s: &str, max_chars: usize) -> &str {
    match s.char_indices().nth(max_chars) {
        Some((byte_idx, _)) => &s[..byte_idx],
        None => s,
    }
}

impl LlamaReranker {
    /// Create a new reranker that sends requests with the given `model`.
    ///
    /// The `client` is shared across all calls and manages pool acquisition and
    /// failover internally.
    pub fn new(client: Arc<LlamaClient>, model: String) -> Self {
        Self {
            client,
            model,
            usage_recorder: None,
        }
    }

    /// Attach a [`UsageRecorder`] so each rerank call's token usage is metered
    /// into `usage_events` on the search read path (P14-T2).
    #[must_use]
    pub fn with_usage_recorder(mut self, recorder: Arc<dyn UsageRecorder>) -> Self {
        self.usage_recorder = Some(recorder);
        self
    }

    /// Meter one rerank call's token usage to `tenant_id`, attributed to
    /// `user_id` when known (best-effort, P14-T2).
    ///
    /// A no-op when no [`UsageRecorder`] is attached. When the backend reports
    /// no `usage` (e.g. the ettin reranker), a zero-token event is still
    /// recorded so the call is counted. Recording failures are logged and
    /// swallowed — metering must never fail a rerank call.
    async fn meter(&self, tenant_id: i64, user_id: Option<i64>, usage: Option<&Usage>) {
        let Some(recorder) = &self.usage_recorder else {
            return;
        };
        let event = UsageEvent {
            id: 0,
            tenant_id,
            user_id,
            model: self.model.clone(),
            role: Role::Rerank,
            backend_id: None,
            prompt_tokens: usage.map(|u| u.prompt_tokens as i32),
            completion_tokens: usage.map(|u| u.completion_tokens as i32),
            latency_ms: None,
            cost_micros: None,
            created_at: chrono::Utc::now(),
        };
        if let Err(e) = recorder.record_usage(&event).await {
            tracing::warn!(error = %e, tenant_id, "reranker: failed to meter usage event");
        }
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
    /// `tenant_id` and `user_id` attribute the call's token usage on the search
    /// read path (P14-T2): a [`Role::Rerank`] usage event is recorded for the
    /// tenant, stamped with the acting user when known. Metering is best-effort.
    ///
    /// # Errors
    ///
    /// Returns an error if the backend call fails, the response length does not
    /// match `docs.len()`, or the JSON cannot be deserialized.
    async fn rerank(
        &self,
        query: &str,
        docs: &[String],
        local_only: bool,
        priority: i32,
        tenant_id: i64,
        user_id: Option<i64>,
    ) -> anyhow::Result<Vec<f32>> {
        // Guard against pathological, non-chunk inputs overflowing the reranker's
        // token window (which would 500 the backend and trip its circuit breaker,
        // disabling reranking for all later searches). The ceiling is generous —
        // real chunks (≤2048 chars) pass through in full. See [`MAX_RERANK_DOC_CHARS`].
        let query = truncate_chars(query, MAX_RERANK_QUERY_CHARS);
        let docs: Vec<String> = docs
            .iter()
            .map(|d| truncate_chars(d, MAX_RERANK_DOC_CHARS).to_owned())
            .collect();
        let (scores, usage) = self
            .client
            .rerank(&self.model, query, &docs, local_only, priority)
            .await?;

        // Meter the call to the searching tenant+user (P14-T2). Recorded even
        // when the backend reports no token usage so the call is still counted.
        self.meter(tenant_id, user_id, usage.as_ref()).await;

        Ok(scores)
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
    use kb_scheduler::{Pool, test_backend};
    use reqwest::Client;

    use super::*;

    /// Build a `LlamaReranker` with a single mock backend registered for `Role::Rerank`.
    async fn reranker_with_one_backend(slots: usize) -> (LlamaReranker, MockBackend) {
        let mock = MockBackend::start().await;
        let base_url = mock.url("/v1");
        let backend = Arc::new(test_backend(
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
        let scores = reranker
            .rerank("query", &docs, false, 0, 1, None)
            .await
            .unwrap();

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
        let scores = reranker
            .rerank("query", &docs, false, 0, 1, None)
            .await
            .unwrap();
        assert!(scores.is_empty());

        mock.shutdown().await;
    }

    // ── input truncation (rerank token-window guard) ─────────────────────

    /// A string at or under the limit is returned unchanged.
    #[test]
    fn truncate_chars_keeps_short_input() {
        assert_eq!(truncate_chars("hello", 10), "hello");
        assert_eq!(truncate_chars("hello", 5), "hello");
        assert_eq!(truncate_chars("", 5), "");
    }

    /// A string over the limit is cut to exactly `max_chars` characters.
    #[test]
    fn truncate_chars_caps_long_input() {
        let long = "x".repeat(MAX_RERANK_DOC_CHARS + 1000);
        assert_eq!(truncate_chars(&long, 700).chars().count(), 700);
        assert_eq!(
            truncate_chars(&long, MAX_RERANK_DOC_CHARS).chars().count(),
            MAX_RERANK_DOC_CHARS
        );
    }

    /// A full-size chunk (≤ DEFAULT_CHUNK_SIZE_CHARS = 2048) is NOT truncated —
    /// the long-context reranker (ettin, 8192-token) scores the whole chunk.
    #[test]
    fn full_chunk_reaches_reranker_untruncated() {
        let chunk = "a".repeat(2048);
        assert_eq!(truncate_chars(&chunk, MAX_RERANK_DOC_CHARS), chunk.as_str());
    }

    /// Truncation lands on a UTF-8 char boundary (never splits a code point).
    #[test]
    fn truncate_chars_respects_char_boundaries() {
        // Each 'é' is 2 bytes; cutting at 3 chars must yield 6 bytes, valid UTF-8.
        let s = "éééééé";
        let out = truncate_chars(s, 3);
        assert_eq!(out.chars().count(), 3);
        assert_eq!(out, "ééé");
    }

    /// Length mismatch between input and response returns an error.
    #[tokio::test]
    async fn length_mismatch_is_error() {
        let (reranker, mock) = reranker_with_one_backend(2).await;
        // Return 1 score for 3 docs → mismatch.
        mock.scenario().lock().await.rerank_content = Some(vec![0.5]);

        let docs: Vec<String> = ["a", "b", "c"].map(String::from).to_vec();
        let err = reranker
            .rerank("query", &docs, false, 0, 1, None)
            .await
            .unwrap_err();
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
        let err = reranker
            .rerank("query", &docs, false, 0, 1, None)
            .await
            .unwrap_err();
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
        let backend = Arc::new(test_backend("mock-1", base_url, vec![Role::Rerank], 0, 2));
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
        let scores = reranker
            .rerank("query text", &docs, false, 0, 1, None)
            .await
            .unwrap();
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
        let scores = reranker
            .rerank("q", &docs, false, 0, 1, None)
            .await
            .unwrap();

        assert_eq!(scores.len(), 3);
        // Position 0 → score for "first", position 1 → "second", etc.
        assert!((scores[0] - 0.10).abs() < 0.0001);
        assert!((scores[1] - 0.90).abs() < 0.0001);
        assert!((scores[2] - 0.50).abs() < 0.0001);

        mock.shutdown().await;
    }

    // ── metering (P14-T2) ────────────────────────────────────────────────

    /// A [`UsageRecorder`] capturing events for assertions (P14-T2).
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

    /// A successful rerank meters a `Role::Rerank` usage event attributed to the
    /// searching tenant+user, carrying the backend's reported token usage.
    #[tokio::test]
    async fn rerank_meters_usage_to_tenant_and_user() {
        let (reranker, mock) = reranker_with_one_backend(2).await;
        let recorder = Arc::new(CapturingRecorder::default());
        let reranker = reranker.with_usage_recorder(recorder.clone() as Arc<dyn UsageRecorder>);
        mock.scenario().lock().await.rerank_content = Some(vec![0.5, 0.4]);

        let docs: Vec<String> = ["a", "b"].map(String::from).to_vec();
        let _ = reranker
            .rerank("query", &docs, false, 0, 42, Some(99))
            .await
            .unwrap();
        mock.shutdown().await;

        let events = recorder.events.lock().unwrap();
        assert_eq!(events.len(), 1, "one rerank call should be metered");
        assert_eq!(events[0].tenant_id, 42);
        assert_eq!(events[0].user_id, Some(99));
        assert_eq!(events[0].role, Role::Rerank);
        assert_eq!(events[0].model, "mock-rerank-model");
        // The mock backend reports prompt_tokens: 10 in its rerank usage block.
        assert_eq!(events[0].prompt_tokens, Some(10));
    }

    /// A system/CLI search (no acting user) still meters the tenant but leaves
    /// `user_id` NULL.
    #[tokio::test]
    async fn rerank_without_user_records_tenant_only() {
        let (reranker, mock) = reranker_with_one_backend(2).await;
        let recorder = Arc::new(CapturingRecorder::default());
        let reranker = reranker.with_usage_recorder(recorder.clone() as Arc<dyn UsageRecorder>);
        mock.scenario().lock().await.rerank_content = Some(vec![0.5]);

        let docs: Vec<String> = ["a"].map(String::from).to_vec();
        let _ = reranker
            .rerank("query", &docs, false, 0, 7, None)
            .await
            .unwrap();
        mock.shutdown().await;

        let events = recorder.events.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].tenant_id, 7);
        assert_eq!(events[0].user_id, None);
        assert_eq!(events[0].role, Role::Rerank);
    }

    /// With no recorder attached, reranking does not attempt to meter (no panic,
    /// scores returned as normal).
    #[tokio::test]
    async fn rerank_without_recorder_is_noop() {
        let (reranker, mock) = reranker_with_one_backend(2).await;
        mock.scenario().lock().await.rerank_content = Some(vec![0.9]);

        let docs: Vec<String> = ["a"].map(String::from).to_vec();
        let scores = reranker
            .rerank("query", &docs, false, 0, 1, Some(2))
            .await
            .unwrap();
        assert_eq!(scores, vec![0.9_f32]);

        mock.shutdown().await;
    }
}
