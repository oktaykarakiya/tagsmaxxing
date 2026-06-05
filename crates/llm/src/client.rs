//! [`LlamaClient`] — the OpenAI-compatible HTTP client that routes every call
//! through [`kb_scheduler::Pool::acquire`] with failover and circuit-breaking
//! (plan §6.4).

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use dashmap::DashMap;
use kb_core::provider::{ChatReq, ChatResp, EmbedReq, EmbedResp, Usage};
use kb_core::role::Role;
use kb_scheduler::{AcquireError, Pool};
use reqwest::Client;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::LlmError;

// ── circuit breaker ─────────────────────────────────────────────────────────

/// Per-backend circuit-breaker state (plan §6.4).
///
/// After `threshold` consecutive failures the backend enters a cooldown period.
/// While in cooldown the client refuses to use it even if the pool's health flag
/// says it is alive.  On the first success after cooldown the counter resets.
#[derive(Debug, Clone, Default)]
struct CircuitState {
    /// Consecutive failures since the last success or cooldown expiry.
    failures: u32,
    /// If `Some`, the backend is in cooldown until this instant.
    cooldown_until: Option<Instant>,
}

// ── OpenAI wire types ───────────────────────────────────────────────────────

/// OpenAI-compatible chat completion request body.
#[derive(Debug, Serialize)]
struct OpenAiChatBody<'a> {
    model: &'a str,
    messages: Vec<OpenAiMessage<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    response_format: Option<ResponseFormat>,
}

/// The `response_format` field sent to OpenAI-compatible backends when
/// `ChatReq.json_schema` is set (plan §9 grammar-constrained structured output).
#[derive(Debug, Serialize)]
struct ResponseFormat {
    #[serde(rename = "type")]
    type_: &'static str,
    json_schema: ResponseFormatSchema,
}

#[derive(Debug, Serialize)]
struct ResponseFormatSchema {
    name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    strict: Option<bool>,
    schema: serde_json::Value,
}

#[derive(Debug, Serialize)]
struct OpenAiMessage<'a> {
    role: &'a str,
    content: &'a str,
}

/// OpenAI-compatible chat completion response.
#[derive(Debug, serde::Deserialize)]
struct OpenAiChatResp {
    choices: Vec<OpenAiChoice>,
    #[serde(default)]
    usage: Option<OpenAiUsage>,
}

#[derive(Debug, serde::Deserialize)]
struct OpenAiChoice {
    message: OpenAiMsgContent,
}

#[derive(Debug, serde::Deserialize)]
struct OpenAiMsgContent {
    content: String,
}

/// OpenAI-compatible embedding request body.
#[derive(Debug, Serialize)]
struct OpenAiEmbedBody<'a> {
    model: &'a str,
    input: &'a [String],
}

/// OpenAI-compatible embedding response.
#[derive(Debug, serde::Deserialize)]
struct OpenAiEmbedResp {
    data: Vec<OpenAiEmbedData>,
    #[serde(default)]
    usage: Option<OpenAiUsage>,
}

#[derive(Debug, serde::Deserialize)]
struct OpenAiEmbedData {
    embedding: Vec<f32>,
}

/// Normalized token usage shared by chat and embed responses.
#[derive(Debug, serde::Deserialize)]
struct OpenAiUsage {
    prompt_tokens: u32,
    #[serde(default)]
    completion_tokens: u32,
}

/// OpenAI-compatible rerank request body (plan §8, llama.cpp `/rerank`).
#[derive(Debug, Serialize)]
struct OpenAiRerankBody<'a> {
    model: &'a str,
    query: &'a str,
    documents: &'a [String],
}

/// One rerank result.
#[derive(Debug, serde::Deserialize)]
struct OpenAiRerankResult {
    index: u32,
    relevance_score: f32,
}

/// OpenAI-compatible rerank response.
#[derive(Debug, serde::Deserialize)]
struct OpenAiRerankResp {
    results: Vec<OpenAiRerankResult>,
    #[serde(default)]
    #[allow(dead_code)]
    usage: Option<OpenAiUsage>,
}

// ── LlamaClient ─────────────────────────────────────────────────────────────

/// An OpenAI-compatible HTTP client that acquires a slot via the
/// [`Pool`](kb_scheduler::Pool), sends the request, and applies failover with a
/// bounded retry loop and per-backend circuit-breaking (plan §6.4).
///
/// The held [`kb_scheduler::Lease`] is dropped at the end of each attempt,
/// unconditionally freeing the slot (RAII — plan §6.2). On transport error or a
/// non-success HTTP status the backend is marked unhealthy so that the next
/// [`acquire`](Pool::acquire) picks a different candidate.
///
/// # Circuit breaker
///
/// Each backend tracks consecutive failures. After `circuit_threshold` failures
/// the backend enters a cooldown (`cooldown`); the client skips it even if the
/// health loop has re-marked it healthy. On the first success after cooldown
/// expires the counter resets.
///
/// # Embed role
///
/// The `embed` role is served through the same failover loop, but the caller
/// must ensure all backends serving `Role::Embed` use the **same** embedding
/// model — otherwise vectors from different models silently corrupt the index
/// (plan §11). The routing-table enforcement lands in P9.
pub struct LlamaClient {
    /// The scheduler pool, shared across all calls.
    pool: Pool,
    /// Shared HTTP client (connection pooling, timeouts set by the caller).
    http: Client,
    /// Maximum number of retry attempts (total calls = 1 + max_retries).
    max_retries: u32,
    /// Consecutive failures before the circuit trips.
    circuit_threshold: u32,
    /// How long a tripped circuit stays open.
    cooldown: Duration,
    /// Per-backend circuit-breaker state.
    circuits: DashMap<String, CircuitState>,
}

impl LlamaClient {
    /// Create a new client.
    ///
    /// `circuit_threshold` consecutive failures trip the breaker for `cooldown`.
    /// Set `circuit_threshold` to `0` to disable circuit-breaking (only the
    /// pool's health flag gates retries).
    pub fn new(
        pool: Pool,
        http: Client,
        max_retries: u32,
        circuit_threshold: u32,
        cooldown: Duration,
    ) -> Self {
        Self {
            pool,
            http,
            max_retries,
            circuit_threshold,
            cooldown,
            circuits: DashMap::new(),
        }
    }

    /// Number of tracked circuit-breaker entries (for inspection in tests).
    #[cfg(test)]
    pub(crate) fn circuit_count(&self) -> usize {
        self.circuits.len()
    }

    // ── public API ──────────────────────────────────────────────────────

    /// Send a chat-completion request through the pool with failover.
    ///
    /// `model` is sent in the OpenAI request body (ignored by `llama-server`
    /// but required for API compatibility). On success the response is
    /// normalized into the core [`ChatResp`] form.
    ///
    /// # Errors
    ///
    /// - [`LlmError::Scheduler`] — `pool.acquire` failed (no backend / timeout).
    /// - [`LlmError::AllFailed`] — every attempt exhausted retries.
    /// - [`LlmError::AllCooldown`] — every candidate is circuit-breaker-blocked.
    pub async fn chat(
        &self,
        role: Role,
        model: &str,
        req: &ChatReq,
        local_only: bool,
        priority: i32,
    ) -> Result<ChatResp, LlmError> {
        let response_format = req.json_schema.as_ref().map(|schema| ResponseFormat {
            type_: "json_schema",
            json_schema: ResponseFormatSchema {
                name: req
                    .json_schema_name
                    .as_deref()
                    .unwrap_or("output")
                    .to_string(),
                strict: Some(true),
                schema: schema.clone(),
            },
        });

        let raw: OpenAiChatResp = if req.images.is_empty() {
            // Text-only path: use the type-safe struct serialization.
            let body = OpenAiChatBody {
                model,
                messages: req
                    .messages
                    .iter()
                    .map(|m| OpenAiMessage {
                        role: m.role.as_str(),
                        content: &m.content,
                    })
                    .collect(),
                max_tokens: req.max_tokens,
                temperature: req.temperature,
                response_format,
            };
            self.call_with_failover(
                role,
                model,
                "/chat/completions",
                &body,
                local_only,
                priority,
            )
            .await?
        } else {
            // Multimodal path: build raw JSON with content-part arrays
            // for OpenAI vision API compatibility. The response shape is
            // identical to the text-only path, so we deserialize as
            // OpenAiChatResp either way.
            let messages: Vec<serde_json::Value> = req
                .messages
                .iter()
                .map(|m| {
                    let mut content_parts: Vec<serde_json::Value> = Vec::new();
                    if !m.content.is_empty() {
                        content_parts.push(serde_json::json!({
                            "type": "text",
                            "text": m.content,
                        }));
                    }
                    for img in &req.images {
                        use base64::Engine;
                        let b64 = base64::engine::general_purpose::STANDARD
                            .encode(&img.data);
                        content_parts.push(serde_json::json!({
                            "type": "image_url",
                            "image_url": {
                                "url": format!("data:{};base64,{}", img.mime, b64)
                            }
                        }));
                    }
                    serde_json::json!({
                        "role": m.role.as_str(),
                        "content": content_parts,
                    })
                })
                .collect();

            let mut body = serde_json::json!({
                "model": model,
                "messages": messages,
            });
            if let Some(mt) = req.max_tokens {
                body["max_tokens"] = serde_json::json!(mt);
            }
            if let Some(t) = req.temperature {
                body["temperature"] = serde_json::json!(t);
            }
            if let Some(ref rf) = response_format {
                body["response_format"] = serde_json::json!(rf);
            }

            self.call_with_failover(
                role,
                model,
                "/chat/completions",
                &body,
                local_only,
                priority,
            )
            .await?
        };

        Ok(ChatResp {
            text: raw
                .choices
                .into_iter()
                .next()
                .map(|c| c.message.content)
                .unwrap_or_default(),
            usage: raw.usage.map_or(Usage::default(), |u| Usage {
                prompt_tokens: u.prompt_tokens,
                completion_tokens: u.completion_tokens,
            }),
        })
    }

    /// Send an embedding request through the pool with failover.
    ///
    /// `model` is sent in the OpenAI request body. Every backend serving
    /// [`Role::Embed`] **must** use the same model+version, or retrieval
    /// correctness degrades (plan §11). This is enforced by configuration;
    /// the routing-table guard lands in P9.
    ///
    /// # Errors
    ///
    /// See [`chat`](Self::chat).
    pub async fn embed(
        &self,
        model: &str,
        req: &EmbedReq,
        local_only: bool,
        priority: i32,
    ) -> Result<EmbedResp, LlmError> {
        let body = OpenAiEmbedBody {
            model,
            input: &req.texts,
        };

        let raw: OpenAiEmbedResp = self
            .call_with_failover(
                Role::Embed,
                model,
                "/embeddings",
                &body,
                local_only,
                priority,
            )
            .await?;

        Ok(EmbedResp {
            vectors: raw.data.into_iter().map(|d| d.embedding).collect(),
            usage: raw.usage.map_or(Usage::default(), |u| Usage {
                prompt_tokens: u.prompt_tokens,
                completion_tokens: u.completion_tokens,
            }),
        })
    }

    /// Send a rerank request through the pool with failover.
    ///
    /// `model` is sent in the OpenAI request body. Calls `POST /rerank` on the
    /// selected backend (which is joined with the `/v1` base URL prefix — see
    /// [`MockBackend::url`]).
    ///
    /// The response results are sorted by `index` to guarantee output scores are
    /// aligned with input `documents` order. If the response contains fewer or
    /// more results than input documents, an error is returned — the backend must
    /// return exactly one score per document.
    ///
    /// # Errors
    ///
    /// - [`LlmError::Scheduler`] — `pool.acquire` failed (no backend / timeout).
    /// - [`LlmError::AllFailed`] — every attempt exhausted retries.
    /// - [`LlmError::AllCooldown`] — every candidate is circuit-breaker-blocked.
    /// - [`LlmError::Deserialize`] — response length ≠ input length, or bad JSON.
    pub async fn rerank(
        &self,
        model: &str,
        query: &str,
        documents: &[String],
        local_only: bool,
        priority: i32,
    ) -> Result<Vec<f32>, LlmError> {
        let body = OpenAiRerankBody {
            model,
            query,
            documents,
        };

        let raw: OpenAiRerankResp = self
            .call_with_failover(Role::Rerank, model, "/rerank", &body, local_only, priority)
            .await?;

        let n_docs = documents.len();
        if raw.results.len() != n_docs {
            return Err(LlmError::Deserialize(format!(
                "rerank response length mismatch: got {} results, expected {n_docs}",
                raw.results.len()
            )));
        }

        // Sort by index so output is aligned with input order.
        let mut scored: Vec<(u32, f32)> = raw
            .results
            .into_iter()
            .map(|r| (r.index, r.relevance_score))
            .collect();
        scored.sort_by_key(|(i, _)| *i);
        Ok(scored.into_iter().map(|(_, s)| s).collect())
    }

    // ── failover core ───────────────────────────────────────────────────

    /// Acquire → POST → parse, with retry + circuit-breaker loop (plan §6.4).
    ///
    /// On each attempt:
    /// 1. Acquire a lease from the pool (waits if all slots are busy).
    /// 2. Skip the backend if it is in circuit-breaker cooldown.
    /// 3. POST the body to `{base_url}{endpoint}`.
    /// 4. On success (2xx): reset circuit, return parsed response.
    /// 5. On failure: mark the backend unhealthy, advance circuit breaker,
    ///    drop the lease (frees the slot), and retry up to `max_retries`.
    async fn call_with_failover<Req: Serialize + std::fmt::Debug, Resp: DeserializeOwned>(
        &self,
        role: Role,
        model: &str,
        endpoint: &str,
        body: &Req,
        local_only: bool,
        priority: i32,
    ) -> Result<Resp, LlmError> {
        let mut last_error: Option<String> = None;
        let total_attempts = self.max_retries.saturating_add(1);
        // Label reused across attempts for the kb_requests_total / *_errors_total
        // / *_duration_seconds families (BUG-OBS-06).
        let role_label = role.to_string();

        for attempt in 0..total_attempts {
            // 1. Acquire a slot (fast path or wait path; plan §6.3).
            let lease = match self.pool.acquire(role, local_only, priority).await {
                Ok(l) => l,
                Err(AcquireError::NoBackend { .. }) => {
                    // Every known backend is either unhealthy or in cooldown.
                    let candidates = self.pool.backends_for(role);
                    let all_cooldown = !candidates.is_empty()
                        && candidates.iter().all(|b| self.in_cooldown(&b.id));
                    if all_cooldown {
                        return Err(LlmError::AllCooldown(self.cooldown));
                    }
                    // If we already burned attempts, surface as AllFailed
                    // (we exhausted every backend). If this is the very
                    // first acquire (attempt==0), the role genuinely has no
                    // healthy backends → propagate NoBackend.
                    if attempt > 0 {
                        return Err(LlmError::AllFailed {
                            retries: self.max_retries,
                            last_error: last_error
                                .unwrap_or_else(|| format!("no healthy backend for role `{role}`")),
                        });
                    }
                    return Err(LlmError::Scheduler(AcquireError::NoBackend { role }));
                }
                Err(e) => return Err(LlmError::Scheduler(e)),
            };

            // 2. Circuit-breaker gate — skip backends in cooldown.
            if self.in_cooldown(&lease.backend_id) {
                // Mark unhealthy so the pool gives us a different backend on
                // the next acquire attempt within this call.
                self.mark_unhealthy(&lease.backend_id);
                // Drop the lease (frees the slot) and retry.
                drop(lease);
                // If this was the last attempt, report cooldown.
                if attempt + 1 >= total_attempts {
                    return Err(LlmError::AllCooldown(self.cooldown));
                }
                continue;
            }

            let url = format!("{}{endpoint}", lease.endpoint);

            // 3. POST to the backend.
            let attempt_start = Instant::now();
            let result = self.http.post(&url).json(body).send().await;
            let elapsed = attempt_start.elapsed().as_secs_f64();

            match result {
                Ok(resp) if resp.status().is_success() => {
                    // 4. Success — parse body, reset circuit, return.
                    let parsed: Resp = resp.json().await.map_err(|e| {
                        LlmError::Deserialize(format!(
                            "failed to parse response from {}: {e}",
                            lease.backend_id
                        ))
                    })?;
                    self.record_success(&lease.backend_id);
                    // Per-backend-call RED metrics (kb_requests_total etc., BUG-OBS-06).
                    kb_metrics::record_request(
                        kb_metrics::RequestOutcome::Success,
                        elapsed,
                        &role_label,
                        model,
                    );
                    return Ok(parsed);
                }
                Ok(resp) => {
                    // Non-success (4xx, 5xx, …).
                    let status = resp.status();
                    let body_text = resp.text().await.unwrap_or_default();
                    self.mark_unhealthy(&lease.backend_id);
                    self.record_failure(&lease.backend_id);
                    kb_metrics::record_request(
                        kb_metrics::RequestOutcome::Error,
                        elapsed,
                        &role_label,
                        model,
                    );
                    let msg = format!("HTTP {status} from {}: {body_text}", lease.backend_id);
                    last_error = Some(msg);
                }
                Err(e) => {
                    // Transport error (connection refused, timeout, DNS, …).
                    self.mark_unhealthy(&lease.backend_id);
                    self.record_failure(&lease.backend_id);
                    kb_metrics::record_request(
                        kb_metrics::RequestOutcome::Error,
                        elapsed,
                        &role_label,
                        model,
                    );
                    let msg = format!("transport error to {}: {e}", lease.backend_id);
                    last_error = Some(msg);
                }
            }
            // Lease drops here → slot freed for the next attempt (plan §6.2).
        }

        Err(LlmError::AllFailed {
            retries: self.max_retries,
            last_error: last_error.unwrap_or_else(|| "unknown error".into()),
        })
    }

    // ── circuit breaker helpers ─────────────────────────────────────────

    /// Whether `backend_id` is currently in circuit-breaker cooldown.
    fn in_cooldown(&self, backend_id: &str) -> bool {
        if self.circuit_threshold == 0 {
            return false;
        }
        self.circuits
            .get(backend_id)
            .and_then(|entry| entry.cooldown_until)
            .is_some_and(|until| Instant::now() < until)
    }

    /// Reset circuit-breaker state for a backend that just succeeded.
    ///
    /// Also publishes the breaker's now-closed state (`0`) to the
    /// `kb_circuit_breaker_open` gauge so operators can alert on a tripped
    /// breaker (plan §6.4/§22). Recording on every success means a healthy
    /// backend that has served at least one request emits a live data line.
    fn record_success(&self, backend_id: &str) {
        self.circuits.remove(backend_id);
        kb_metrics::record_circuit_breaker(backend_id, false);
    }

    /// Record a failed call and trip the breaker if the threshold is reached.
    ///
    /// Publishes the breaker's resulting open/closed state to the
    /// `kb_circuit_breaker_open` gauge (plan §6.4/§22): `1` once the
    /// consecutive-failure threshold trips it into cooldown, `0` while it is
    /// still closed.
    fn record_failure(&self, backend_id: &str) {
        if self.circuit_threshold == 0 {
            // Circuit-breaking disabled — the breaker can never open.
            kb_metrics::record_circuit_breaker(backend_id, false);
            return;
        }
        // Mutate the breaker state under the shard guard, then drop the guard
        // before touching the metrics recorder.
        let open = {
            let mut entry = self.circuits.entry(backend_id.to_string()).or_default();
            entry.failures = entry.failures.saturating_add(1);
            if entry.failures >= self.circuit_threshold {
                entry.cooldown_until = Some(Instant::now() + self.cooldown);
                true
            } else {
                false
            }
        };
        kb_metrics::record_circuit_breaker(backend_id, open);
    }

    // ── health helpers ──────────────────────────────────────────────────

    /// Mark a backend unhealthy by its id so that subsequent
    /// [`Pool::acquire`] calls skip it (plan §6.4).
    ///
    /// Searches the pool's backend list by id; a non-existent id is a no-op.
    fn mark_unhealthy(&self, backend_id: &str) {
        for b in self.pool.all_backends() {
            if b.id == backend_id {
                b.healthy.store(false, Ordering::Release);
                return;
            }
        }
    }
}

// ── unit tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    use kb_core::extractor::PageImage;
    use kb_core::provider::{ChatMessage, ChatReq, EmbedReq};
    use kb_core::role::Role;
    use kb_mock_backend::{MockBackend, ResponseMode};
    use kb_scheduler::{Pool, test_backend};
    use reqwest::Client;

    use super::*;

    /// Build a `LlamaClient` with a single mock backend registered for `role`.
    async fn client_with_one_backend(role: Role, slots: usize) -> (LlamaClient, MockBackend) {
        let mock = MockBackend::start().await;
        let base_url = mock.url("/v1");
        let backend = Arc::new(test_backend(
            "mock-1",
            &base_url,
            vec![role],
            0, /* priority */
            slots,
        ));
        let pool = Pool::new(vec![backend], Duration::from_secs(5));
        let client = LlamaClient::new(
            pool,
            Client::new(),
            2,                          /* max_retries */
            3,                          /* circuit_threshold */
            Duration::from_millis(200), /* cooldown */
        );
        (client, mock)
    }

    /// Build a `LlamaClient` with TWO mock backends for the same role
    /// (used for failover tests).
    async fn client_with_two_backends(role: Role) -> (LlamaClient, MockBackend, MockBackend) {
        let mock1 = MockBackend::start().await;
        let mock2 = MockBackend::start().await;
        let b1 = Arc::new(test_backend(
            "mock-1",
            mock1.url("/v1"),
            vec![role],
            0, /* priority */
            2, /* slots */
        ));
        let b2 = Arc::new(test_backend(
            "mock-2",
            mock2.url("/v1"),
            vec![role],
            10, /* lower priority — preferred only when b1 is unhealthy */
            2,
        ));
        let pool = Pool::new(vec![b1, b2], Duration::from_secs(5));
        let client = LlamaClient::new(
            pool,
            Client::new(),
            2, /* max_retries */
            3, /* circuit_threshold */
            Duration::from_millis(200),
        );
        (client, mock1, mock2)
    }

    fn chat_req(content: &str) -> ChatReq {
        ChatReq {
            messages: vec![ChatMessage {
                role: kb_core::provider::ChatRole::User,
                content: content.into(),
            }],
            ..Default::default()
        }
    }

    // ── success path ────────────────────────────────────────────────────

    /// The happy path: a healthy mock returns a valid chat completion.
    #[tokio::test]
    async fn chat_success_path() {
        let (client, mock) = client_with_one_backend(Role::Text, 2).await;

        let resp = client
            .chat(Role::Text, "test-model", &chat_req("hi"), false, 0)
            .await
            .unwrap();
        assert_eq!(resp.text, "mock response");
        assert_eq!(resp.usage.prompt_tokens, 10);
        assert_eq!(resp.usage.completion_tokens, 5);

        mock.shutdown().await;
    }

    /// Multimodal chat: sending a request with images uses the vision role
    /// and returns the mock's chat content.
    #[tokio::test]
    async fn multimodal_chat_returns_caption() {
        let (client, mock) = client_with_one_backend(Role::Vision, 2).await;
        // Set a known caption that the mock will return for any chat request.
        let caption = "A photo of a sunset over mountains.";
        mock.scenario().lock().await.chat_content = Some(caption.to_string());

        let img = PageImage {
            data: bytes::Bytes::from_static(b"\xff\xd8\xff"),
            mime: "image/jpeg".into(),
        };
        let req = ChatReq {
            messages: vec![ChatMessage {
                role: kb_core::provider::ChatRole::User,
                content: "Describe this image.".into(),
            }],
            images: vec![img],
            ..Default::default()
        };

        let resp = client
            .chat(Role::Vision, "test-model", &req, false, 0)
            .await
            .unwrap();
        assert_eq!(resp.text, caption);

        mock.shutdown().await;
    }

    /// The happy path for embeddings.
    #[tokio::test]
    async fn embed_success_path() {
        let (client, mock) = client_with_one_backend(Role::Embed, 2).await;
        let req = EmbedReq {
            texts: vec!["hello".into(), "world".into()],
        };

        let resp = client
            .embed("test-embed-model", &req, false, 0)
            .await
            .unwrap();
        assert_eq!(resp.vectors.len(), 1); // mock returns 1 vector regardless of input
        assert!(!resp.vectors[0].is_empty());
        assert_eq!(resp.usage.prompt_tokens, 3);

        mock.shutdown().await;
    }

    // ── failover on 5xx ─────────────────────────────────────────────────

    /// When the first backend returns 500, the client fails over to the second
    /// healthy backend.
    #[tokio::test]
    async fn failover_on_5xx_to_healthy_backend() {
        let (client, mock1, mock2) = client_with_two_backends(Role::Text).await;

        // Make mock-1 return 500 on chat.
        mock1.scenario().lock().await.chat = ResponseMode::ServerError;

        // mock-2 stays healthy → failover should land there.
        let resp = client
            .chat(Role::Text, "model", &chat_req("hi"), false, 0)
            .await
            .unwrap();
        assert_eq!(resp.text, "mock response");

        // After the failure, mock-1 should be marked unhealthy.
        // (Prove by checking the local backend state.)
        // The pool's all_backends() includes mock-1.
        let backends = client.pool.all_backends();
        let b1 = backends.iter().find(|b| b.id == "mock-1").unwrap();
        assert!(
            !b1.healthy.load(Ordering::Acquire),
            "failed backend must be marked unhealthy"
        );

        // mock-2 should still be healthy.
        let b2 = backends.iter().find(|b| b.id == "mock-2").unwrap();
        assert!(b2.healthy.load(Ordering::Acquire));

        mock1.shutdown().await;
        mock2.shutdown().await;
    }

    /// Failover on transport error (mock is shut down mid-test).
    #[tokio::test]
    async fn failover_on_transport_error() {
        let (client, mock1, mock2) = client_with_two_backends(Role::Text).await;

        // Shut down mock-1 so its port is no longer listening.
        mock1.shutdown().await;

        // Transport error should trigger failover to mock-2.
        let resp = client
            .chat(Role::Text, "model", &chat_req("hi"), false, 0)
            .await
            .unwrap();
        assert_eq!(resp.text, "mock response");

        mock2.shutdown().await;
    }

    // ── bounded retries → error ─────────────────────────────────────────

    /// When ALL backends return 5xx, the client exhausts retries and returns
    /// `AllFailed`.
    #[tokio::test]
    async fn bounded_retries_all_failed() {
        let (client, mock1, mock2) = client_with_two_backends(Role::Text).await;

        // Both backends return 500.
        mock1.scenario().lock().await.chat = ResponseMode::ServerError;
        mock2.scenario().lock().await.chat = ResponseMode::ServerError;

        let err = client
            .chat(Role::Text, "model", &chat_req("hi"), false, 0)
            .await
            .unwrap_err();
        match err {
            LlmError::AllFailed {
                retries,
                last_error,
            } => {
                assert_eq!(retries, 2, "max_retries=2");
                assert!(
                    last_error.contains("no healthy backend") || last_error.contains("500"),
                    "{last_error}"
                );
            }
            other => panic!("expected AllFailed, got {other:?}"),
        }

        mock1.shutdown().await;
        mock2.shutdown().await;
    }

    /// When no backend serves the role, `NoBackend` is returned.
    #[tokio::test]
    async fn no_backend_for_role() {
        let pool = Pool::new(vec![], Duration::from_secs(5));
        let client = LlamaClient::new(pool, Client::new(), 2, 3, Duration::from_millis(200));

        let err = client
            .chat(Role::Rerank, "model", &chat_req("hi"), false, 0)
            .await
            .unwrap_err();
        match err {
            LlmError::Scheduler(AcquireError::NoBackend { role }) => {
                assert_eq!(role, Role::Rerank);
            }
            other => panic!("expected NoBackend, got {other:?}"),
        }
    }

    // ── circuit-break + cooldown ────────────────────────────────────────

    /// After `circuit_threshold` consecutive failures the backend enters
    /// cooldown and is skipped until the cooldown expires.
    ///
    /// Uses two backends so the circuit-breaker can accumulate failures on
    /// mock-1 across separate calls: between calls we manually re-mark it
    /// healthy (simulating the health loop's recovery), giving the pool a
    /// reason to hand it to us again. Once the circuit trips, mock-1 is
    /// skipped despite being healthy — the cooldown gate blocks it.
    #[tokio::test]
    async fn circuit_breaker_trips_and_blocks() {
        let mock1 = MockBackend::start().await;
        let mock2 = MockBackend::start().await;

        // mock-1 has lower priority → preferred when healthy.
        let b1 = Arc::new(test_backend(
            "mock-1",
            mock1.url("/v1"),
            vec![Role::Text],
            0, /* priority */
            2,
        ));
        let b2 = Arc::new(test_backend(
            "mock-2",
            mock2.url("/v1"),
            vec![Role::Text],
            10, /* priority */
            2,
        ));
        let pool = Pool::new(vec![Arc::clone(&b1), b2], Duration::from_secs(5));
        let client = LlamaClient::new(
            pool,
            Client::new(),
            1, /* max_retries — one retry, two total attempts */
            2, /* circuit_threshold — trip after 2 failures */
            Duration::from_millis(200),
        );

        // --- call 1: mock-1 fails → mock-2 succeeds (failover) --------
        mock1.scenario().lock().await.chat = ResponseMode::ServerError;
        // mock-2 stays healthy.
        let resp = client
            .chat(Role::Text, "model", &chat_req("a"), false, 0)
            .await
            .unwrap();
        assert_eq!(resp.text, "mock response");
        // mock-1 is now unhealthy (marked by the failover path).
        assert!(!b1.healthy.load(Ordering::Acquire));

        // Simulate health-loop recovery: re-mark mock-1 healthy.
        b1.healthy.store(true, Ordering::Release);

        // --- call 2: mock-1 fails again → circuit trips ----------------
        let resp = client
            .chat(Role::Text, "model", &chat_req("b"), false, 0)
            .await
            .unwrap();
        assert_eq!(resp.text, "mock response");
        // circuit: mock-1 failures=2 → tripped.

        // Re-mark healthy again (health loop).
        b1.healthy.store(true, Ordering::Release);

        // --- call 3: mock-1 in cooldown → skipped → mock-2 wins -------
        // Even though mock-1 is healthy (pool would pick it, priority 0),
        // the circuit breaker cooldown must refuse it. The cooldown path
        // also marks mock-1 unhealthy so the pool gives us mock-2 on retry.
        let resp = client
            .chat(Role::Text, "model", &chat_req("c"), false, 0)
            .await
            .unwrap();
        assert_eq!(resp.text, "mock response");
        // mock-1 is now unhealthy (marked by the cooldown path).
        assert!(!b1.healthy.load(Ordering::Acquire));

        // --- wait for cooldown to expire --------------------------------
        tokio::time::sleep(Duration::from_millis(250)).await;
        // Re-mark healthy (simulates health-loop recovery after cooldown).
        b1.healthy.store(true, Ordering::Release);
        // Make mock-1 return healthy responses again.
        mock1.scenario().lock().await.chat = ResponseMode::Healthy;

        let resp = client
            .chat(Role::Text, "model", &chat_req("d"), false, 0)
            .await
            .unwrap();
        assert_eq!(resp.text, "mock response");
        // Circuit breaker should be reset after the successful call.
        assert_eq!(client.circuit_count(), 0);

        mock1.shutdown().await;
        mock2.shutdown().await;
    }

    /// When `circuit_threshold` is 0, circuit-breaking is disabled.
    #[tokio::test]
    async fn circuit_breaker_disabled_with_threshold_zero() {
        let mock = MockBackend::start().await;
        let base_url = mock.url("/v1");
        let backend = Arc::new(test_backend("mock-1", base_url, vec![Role::Text], 0, 2));
        let pool = Pool::new(vec![backend], Duration::from_secs(5));
        let client = LlamaClient::new(
            pool,
            Client::new(),
            2,
            0, /* threshold=0 disables circuit breaker */
            Duration::from_millis(200),
        );

        // Fail multiple times.
        mock.scenario().lock().await.chat = ResponseMode::ServerError;
        let _ = client
            .chat(Role::Text, "model", &chat_req("a"), false, 0)
            .await;
        let _ = client
            .chat(Role::Text, "model", &chat_req("b"), false, 0)
            .await;

        // No circuits tracked.
        assert_eq!(client.circuit_count(), 0);

        // Recover the backend.
        mock.scenario().lock().await.chat = ResponseMode::Healthy;
        // Also re-mark healthy (the client marked it unhealthy on failure).
        for b in client.pool.all_backends() {
            b.healthy.store(true, Ordering::Release);
        }

        let resp = client
            .chat(Role::Text, "model", &chat_req("c"), false, 0)
            .await
            .unwrap();
        assert_eq!(resp.text, "mock response");

        mock.shutdown().await;
    }

    // ── slot released on drop ───────────────────────────────────────────

    /// After a successful call the slot is returned to the backend.
    #[tokio::test]
    async fn slot_freed_after_success() {
        let (client, mock) = client_with_one_backend(Role::Text, 1).await;

        // First call consumes the only slot.
        let resp = client
            .chat(Role::Text, "model", &chat_req("a"), false, 0)
            .await
            .unwrap();
        assert_eq!(resp.text, "mock response");

        // After the call, the slot should be free.
        let backends = client.pool.all_backends();
        assert_eq!(backends[0].free(), 1);

        mock.shutdown().await;
    }

    /// After a failed call the slot is still returned.
    #[tokio::test]
    async fn slot_freed_after_failure() {
        let (client, mock) = client_with_one_backend(Role::Text, 1).await;

        mock.scenario().lock().await.chat = ResponseMode::ServerError;
        let _ = client
            .chat(Role::Text, "model", &chat_req("a"), false, 0)
            .await;

        // Slot must be free even after failure.
        let backends = client.pool.all_backends();
        assert_eq!(backends[0].free(), 1);

        mock.shutdown().await;
    }

    // ── embed failover ──────────────────────────────────────────────────

    /// Embed failover works the same as chat failover.
    #[tokio::test]
    async fn embed_failover_on_5xx() {
        let (client, mock1, mock2) = client_with_two_backends(Role::Embed).await;

        mock1.scenario().lock().await.embed = ResponseMode::ServerError;

        let req = EmbedReq {
            texts: vec!["hi".into()],
        };
        let resp = client.embed("embed-model", &req, false, 0).await.unwrap();
        assert_eq!(resp.vectors.len(), 1);

        // mock-1 should be unhealthy now.
        let backends = client.pool.all_backends();
        let b1 = backends.iter().find(|b| b.id == "mock-1").unwrap();
        assert!(!b1.healthy.load(Ordering::Acquire));

        mock1.shutdown().await;
        mock2.shutdown().await;
    }

    // ── 429 (rate-limit) is treated as a failure ────────────────────────

    /// The client treats 429 as a failure (marks unhealthy, retries).
    #[tokio::test]
    async fn rate_limit_triggers_failover() {
        let (client, mock1, mock2) = client_with_two_backends(Role::Text).await;

        mock1.scenario().lock().await.chat = ResponseMode::RateLimited {
            retry_after_secs: 30,
        };

        let resp = client
            .chat(Role::Text, "model", &chat_req("hi"), false, 0)
            .await
            .unwrap();
        assert_eq!(resp.text, "mock response");

        // mock-1 should be unhealthy after 429.
        let backends = client.pool.all_backends();
        let b1 = backends.iter().find(|b| b.id == "mock-1").unwrap();
        assert!(!b1.healthy.load(Ordering::Acquire));

        mock1.shutdown().await;
        mock2.shutdown().await;
    }

    // ── AcquireError::NoBackend propagates correctly ────────────────────

    /// When every backend serving the role is unhealthy, `NoBackend` is
    /// surfaced through `LlmError::Scheduler` (not `AllCooldown`).
    #[tokio::test]
    async fn no_healthy_backend_is_not_cooldown() {
        let mock = MockBackend::start().await;
        let base_url = mock.url("/v1");
        let backend = Arc::new(test_backend("mock-1", base_url, vec![Role::Text], 0, 2));
        // Mark unhealthy before building the pool.
        backend.healthy.store(false, Ordering::Release);
        let pool = Pool::new(vec![backend], Duration::from_secs(5));
        let client = LlamaClient::new(pool, Client::new(), 2, 3, Duration::from_millis(200));

        let err = client
            .chat(Role::Text, "model", &chat_req("hi"), false, 0)
            .await
            .unwrap_err();
        match err {
            LlmError::Scheduler(AcquireError::NoBackend { role }) => {
                assert_eq!(role, Role::Text);
            }
            other => panic!("expected NoBackend, got {other:?}"),
        }

        mock.shutdown().await;
    }

    // ── circuit-breaker helpers ─────────────────────────────────────────

    #[tokio::test]
    async fn circuit_count_starts_at_zero() {
        let b = Arc::new(test_backend("b1", "http://x:8001", vec![Role::Text], 0, 2));
        let pool = Pool::new(vec![b], Duration::from_secs(1));
        let client = LlamaClient::new(pool, Client::new(), 2, 3, Duration::from_millis(200));
        assert_eq!(client.circuit_count(), 0);
    }

    #[tokio::test]
    async fn circuit_count_after_single_success() {
        let (client, mock) = client_with_one_backend(Role::Text, 2).await;
        let _ = client
            .chat(Role::Text, "model", &chat_req("hello"), false, 0)
            .await
            .unwrap();
        // After success, no circuit-breaker entry (backend not failed).
        assert_eq!(client.circuit_count(), 0);
        mock.shutdown().await;
    }

    /// Circuit-breaker state transitions publish the `kb_circuit_breaker_open`
    /// gauge so the metric is a live alerting signal, not a dead HELP/TYPE-only
    /// family (BUG-OBS-04). Drives the breaker directly to keep the test
    /// deterministic (no real backend / sleeps).
    #[tokio::test]
    async fn circuit_breaker_state_publishes_gauge() {
        // Install the global Prometheus recorder so the gauge is renderable.
        let _ = kb_metrics::init_metrics();

        let pool = Pool::new(vec![], Duration::from_secs(1));
        // threshold = 2 → the second failure trips the breaker.
        let client = LlamaClient::new(pool, Client::new(), 1, 2, Duration::from_millis(50));

        // Unique label so parallel tests sharing the global recorder don't clash.
        let backend = "bug-obs-04-backend";
        let closed = format!("kb_circuit_breaker_open{{dependency=\"{backend}\"}} 0");
        let opened = format!("kb_circuit_breaker_open{{dependency=\"{backend}\"}} 1");

        // First failure: below threshold → breaker closed (0).
        client.record_failure(backend);
        let text = kb_metrics::render();
        assert!(
            text.contains(&closed),
            "expected closed breaker, got: {text}"
        );

        // Second failure reaches the threshold → breaker open (1).
        client.record_failure(backend);
        let text = kb_metrics::render();
        assert!(text.contains(&opened), "expected open breaker, got: {text}");

        // A success resets the breaker → closed (0) again.
        client.record_success(backend);
        let text = kb_metrics::render();
        assert!(
            text.contains(&closed),
            "expected reset-to-closed breaker, got: {text}"
        );
    }

    /// With circuit-breaking disabled (`threshold == 0`) a failure still
    /// publishes a closed-breaker (`0`) data line — the breaker can never open.
    #[tokio::test]
    async fn circuit_breaker_disabled_publishes_closed_gauge() {
        let _ = kb_metrics::init_metrics();

        let pool = Pool::new(vec![], Duration::from_secs(1));
        let client = LlamaClient::new(pool, Client::new(), 1, 0, Duration::from_millis(50));

        let backend = "bug-obs-04-disabled";
        client.record_failure(backend);

        let text = kb_metrics::render();
        assert!(
            text.contains(&format!(
                "kb_circuit_breaker_open{{dependency=\"{backend}\"}} 0"
            )),
            "expected closed breaker with breaking disabled, got: {text}"
        );
        // No circuit entry is tracked when breaking is disabled.
        assert_eq!(client.circuit_count(), 0);
    }

    // ── rerank ───────────────────────────────────────────────────────────

    /// The happy path: a healthy mock returns valid rerank scores.
    #[tokio::test]
    async fn rerank_success_path() {
        let mock = MockBackend::start().await;
        let base_url = mock.url("/v1");
        let backend = Arc::new(test_backend("mock-1", base_url, vec![Role::Rerank], 0, 2));
        let pool = Pool::new(vec![backend], Duration::from_secs(5));
        let client = LlamaClient::new(pool, Client::new(), 2, 3, Duration::from_millis(200));

        let documents = vec![
            "doc a".to_string(),
            "doc b".to_string(),
            "doc c".to_string(),
        ];
        let scores = client
            .rerank("model", "query", &documents, false, 0)
            .await
            .unwrap();
        assert_eq!(scores.len(), 3);
        // Default mock scores: [1.0, 0.9, 0.8]
        assert!((scores[0] - 1.0_f32).abs() < 0.01);
        assert!((scores[1] - 0.9_f32).abs() < 0.01);
        assert!((scores[2] - 0.8_f32).abs() < 0.01);

        mock.shutdown().await;
    }

    /// Custom scores via `rerank_content`.
    #[tokio::test]
    async fn rerank_custom_scores() {
        let mock = MockBackend::start().await;
        mock.scenario().lock().await.rerank_content = Some(vec![0.12, 0.34, 0.56]);
        let base_url = mock.url("/v1");
        let backend = Arc::new(test_backend("mock-1", base_url, vec![Role::Rerank], 0, 2));
        let pool = Pool::new(vec![backend], Duration::from_secs(5));
        let client = LlamaClient::new(pool, Client::new(), 2, 3, Duration::from_millis(200));

        let documents = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let scores = client
            .rerank("model", "query", &documents, false, 0)
            .await
            .unwrap();
        assert_eq!(scores, vec![0.12_f32, 0.34_f32, 0.56_f32]);

        mock.shutdown().await;
    }

    /// Empty documents → empty scores.
    #[tokio::test]
    async fn rerank_empty_documents() {
        let mock = MockBackend::start().await;
        let base_url = mock.url("/v1");
        let backend = Arc::new(test_backend("mock-1", base_url, vec![Role::Rerank], 0, 2));
        let pool = Pool::new(vec![backend], Duration::from_secs(5));
        let client = LlamaClient::new(pool, Client::new(), 2, 3, Duration::from_millis(200));

        let documents: Vec<String> = vec![];
        let scores = client
            .rerank("model", "query", &documents, false, 0)
            .await
            .unwrap();
        assert!(scores.is_empty());

        mock.shutdown().await;
    }

    /// Length mismatch between input and response returns an error.
    #[tokio::test]
    async fn rerank_length_mismatch_error() {
        let mock = MockBackend::start().await;
        // Return only 1 score for 3 documents → mismatch.
        mock.scenario().lock().await.rerank_content = Some(vec![0.5]);
        let base_url = mock.url("/v1");
        let backend = Arc::new(test_backend("mock-1", base_url, vec![Role::Rerank], 0, 2));
        let pool = Pool::new(vec![backend], Duration::from_secs(5));
        let client = LlamaClient::new(pool, Client::new(), 2, 3, Duration::from_millis(200));

        let documents = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let err = client
            .rerank("model", "query", &documents, false, 0)
            .await
            .unwrap_err();
        match err {
            LlmError::Deserialize(msg) => {
                assert!(
                    msg.contains("length mismatch"),
                    "expected length mismatch, got: {msg}"
                );
                assert!(msg.contains("got 1"), "{msg}");
                assert!(msg.contains("expected 3"), "{msg}");
            }
            other => panic!("expected Deserialize error, got {other:?}"),
        }

        mock.shutdown().await;
    }

    /// Failover on 5xx for rerank.
    #[tokio::test]
    async fn rerank_failover_on_5xx() {
        let mock1 = MockBackend::start().await;
        let mock2 = MockBackend::start().await;
        let b1 = Arc::new(test_backend(
            "mock-1",
            mock1.url("/v1"),
            vec![Role::Rerank],
            0,
            2,
        ));
        let b2 = Arc::new(test_backend(
            "mock-2",
            mock2.url("/v1"),
            vec![Role::Rerank],
            10,
            2,
        ));
        let pool = Pool::new(vec![Arc::clone(&b1), b2], Duration::from_secs(5));
        let client = LlamaClient::new(pool, Client::new(), 2, 3, Duration::from_millis(200));

        mock1.scenario().lock().await.rerank = ResponseMode::ServerError;

        let documents = vec!["doc a".to_string()];
        let scores = client
            .rerank("model", "query", &documents, false, 0)
            .await
            .unwrap();
        assert_eq!(scores.len(), 1);
        assert!(!b1.healthy.load(Ordering::Acquire));

        mock1.shutdown().await;
        mock2.shutdown().await;
    }
}
