//! In-process mock HTTP backend test harness for deterministic scheduler/llm
//! tests (plan §31.3 mock-backend pattern).
//!
//! [`MockBackend`] starts an axum server on an ephemeral `127.0.0.1` port with
//! three routes — `/health`, `/v1/chat/completions`, `/v1/embeddings` — whose
//! behaviour is steered at runtime via a shared [`Scenario`] handle. Tests mutate
//! the scenario between calls to simulate healthy, unhealthy, slow, 5xx, and 429
//! responses without any real network or flakiness.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::extract::State;
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Json, Response};
use axum::routing::{get, post};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::Mutex;

// ── scenario types ────────────────────────────────────────────────────────

/// Per-endpoint response behaviour the mock server is configured to simulate.
///
/// The default is always [`ResponseMode::Healthy`], so a freshly-started mock
/// returns `200 OK` with a valid JSON body on every route.
#[derive(Debug, Clone)]
pub enum ResponseMode {
    /// `200 OK` with a realistic OpenAI-compatible JSON body.
    Healthy,
    /// `503 Service Unavailable` — simulates a downed backend.
    Unhealthy,
    /// `429 Too Many Requests` with a `Retry-After` header.
    RateLimited {
        /// Value of the `Retry-After` header (seconds).
        retry_after_secs: u64,
    },
    /// `500 Internal Server Error` — simulates a misbehaving backend.
    ServerError,
    /// `200 OK` after a fixed wall-clock delay (simulates a slow backend).
    Slow {
        /// Millis to sleep before responding.
        delay_ms: u64,
    },
}

/// The full set of endpoint behaviours the mock server obeys.
///
/// One [`Scenario`] is shared between the mock's axum handlers and the test code
/// via `Arc<Mutex<Scenario>>`; handlers snapshot the relevant field on every call.
#[derive(Debug, Clone)]
pub struct Scenario {
    /// Behaviour for `GET /health`.
    pub health: ResponseMode,
    /// Behaviour for `POST /v1/chat/completions`.
    pub chat: ResponseMode,
    /// Behaviour for `POST /v1/embeddings`.
    pub embed: ResponseMode,
    /// When `Some`, the chat endpoint returns this string as the `choices[0].message.content`
    /// instead of the default `"mock response"`. Useful for structured-output tests.
    pub chat_content: Option<String>,
    /// When `Some`, the embed endpoint returns these vectors as `data[i].embedding`
    /// instead of the default `[0.1, 0.2, 0.3]`. Useful for canonicalization tests.
    pub embed_content: Option<Vec<Vec<f32>>>,
    /// When `Some(dim)` (and `embed_content` is `None`), the embed endpoint returns **one
    /// deterministic `dim`-dimensional vector per input text**, derived from a hash of the
    /// text. Distinct texts get distinct (near-orthogonal) vectors and identical texts get
    /// identical ones — so a real embedder's "different words → different vectors" behaviour is
    /// modelled. Needed by full-pipeline canonicalization tests that must keep N distinct tags
    /// distinct against a `VECTOR(2560)` schema.
    pub embed_dim: Option<usize>,
    /// Behaviour for `POST /v1/rerank`.
    pub rerank: ResponseMode,
    /// When `Some`, the rerank endpoint returns these floats as `results[i].relevance_score`
    /// instead of default descending scores `[1.0, 0.9, 0.8, …]`. Useful for tests that
    /// need specific score values or length-mismatch scenarios.
    pub rerank_content: Option<Vec<f32>>,
}

impl Default for Scenario {
    fn default() -> Self {
        Self {
            health: ResponseMode::Healthy,
            chat: ResponseMode::Healthy,
            embed: ResponseMode::Healthy,
            chat_content: None,
            embed_content: None,
            embed_dim: None,
            rerank: ResponseMode::Healthy,
            rerank_content: None,
        }
    }
}

// ── shared axum state ─────────────────────────────────────────────────────

/// Internal state passed to every axum handler via `State`.
#[derive(Clone)]
struct AppState {
    scenario: Arc<Mutex<Scenario>>,
}

// ── the mock server ───────────────────────────────────────────────────────

/// A running in-process mock HTTP backend.
///
/// # Lifecycle
///
/// ```no_run
/// # async {
/// let mock = kb_mock_backend::MockBackend::start().await;
/// let addr = mock.addr();
/// // … run tests against addr …
/// mock.shutdown().await;
/// # };
/// ```
///
/// The server is bound to `127.0.0.1:0` (ephemeral port) and is fully
/// deterministic — no real network, no flakiness.
pub struct MockBackend {
    /// The `SocketAddr` the server is listening on.
    addr: SocketAddr,
    /// Shared scenario handle, for test-driven mutation at runtime.
    scenario: Arc<Mutex<Scenario>>,
    /// Send-side of the graceful-shutdown channel.
    shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
}

impl MockBackend {
    /// Start the mock server on an ephemeral `127.0.0.1` port.
    ///
    /// Returns as soon as the listener is bound; the axum serve loop runs in a
    /// background tokio task. The default [`Scenario`] answers every route with
    /// [`ResponseMode::Healthy`].
    pub async fn start() -> Self {
        let scenario = Arc::new(Mutex::new(Scenario::default()));
        let app_state = AppState {
            scenario: Arc::clone(&scenario),
        };

        let app = Router::new()
            .route("/health", get(handle_health))
            .route("/v1/health", get(handle_health))
            .route("/v1/chat/completions", post(handle_chat))
            .route("/v1/embeddings", post(handle_embed))
            .route("/v1/rerank", post(handle_rerank))
            .with_state(app_state);

        let listener = match TcpListener::bind("127.0.0.1:0").await {
            Ok(l) => l,
            Err(e) => panic!("ephemeral bind on 127.0.0.1:0 failed: {e}"),
        };
        let addr = match listener.local_addr() {
            Ok(a) => a,
            Err(e) => panic!("local_addr on freshly-bound listener failed: {e}"),
        };

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

        tokio::spawn(async move {
            if let Err(e) = axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    shutdown_rx.await.ok();
                })
                .await
            {
                tracing::error!(%e, "mock server stopped with an error");
            }
        });

        Self {
            addr,
            scenario,
            shutdown_tx: Some(shutdown_tx),
        }
    }

    /// The `SocketAddr` the server is listening on.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Convenience: build a full HTTP URL for a path.
    ///
    /// # Example
    ///
    /// ```
    /// # async {
    /// # let mock = kb_mock_backend::MockBackend::start().await;
    /// let health_url = mock.url("/health");
    /// assert!(health_url.starts_with("http://127.0.0.1:"));
    /// assert!(health_url.ends_with("/health"));
    /// # mock.shutdown().await;
    /// # };
    /// ```
    pub fn url(&self, path: &str) -> String {
        format!("http://{}{path}", self.addr)
    }

    /// Borrow the shared scenario handle for runtime mutation.
    ///
    /// Lock the mutex, mutate the scenario, drop the guard — the next request
    /// will observe the new behaviour.
    pub fn scenario(&self) -> &Arc<Mutex<Scenario>> {
        &self.scenario
    }

    /// Gracefully shut down the mock server.
    ///
    /// Sends the shutdown signal and yields until the axum serve loop returns.
    /// Idempotent: calling `shutdown` more than once is a no-op.
    pub async fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
            // Yield to let the background task observe the signal.
            tokio::task::yield_now().await;
        }
    }
}

impl Drop for MockBackend {
    fn drop(&mut self) {
        // Try to send the shutdown signal on drop so the background task does
        // not leak. The receiver end is consumed by the spawn, so send() may
        // fail if shutdown was already called — that's fine.
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

// ── axum handlers ──────────────────────────────────────────────────────────

/// Apply a [`ResponseMode`] to produce an HTTP response.
///
/// This is the single dispatch point that all route handlers route through;
/// testing it covers every simulated behaviour.
fn respond(mode: &ResponseMode) -> impl IntoResponse {
    match mode {
        ResponseMode::Healthy => {
            // The caller (handler) will set the body — this path is reached
            // but the body is determined per-endpoint. We return a default OK
            // here; callers override.
            (StatusCode::OK, Json(json!({"status": "ok"}))).into_response()
        }
        ResponseMode::Unhealthy => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "service unavailable"})),
        )
            .into_response(),
        ResponseMode::RateLimited { retry_after_secs } => {
            let body = Json(json!({"error": "rate limited"}));
            let mut resp = (StatusCode::TOO_MANY_REQUESTS, body).into_response();
            // u64 decimal representation is always a valid HeaderValue.
            let retry_val = match retry_after_secs.to_string().parse::<HeaderValue>() {
                Ok(v) => v,
                Err(_) => HeaderValue::from_static("0"),
            };
            resp.headers_mut().insert("Retry-After", retry_val);
            resp
        }
        ResponseMode::ServerError => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "internal error"})),
        )
            .into_response(),
        ResponseMode::Slow { delay_ms: _ } => {
            // apply_mode(…) pre-handles Slow with an async sleep; this arm is a
            // safety net if respond(…) is ever called directly with Slow.
            (StatusCode::OK, Json(json!({"status": "ok"}))).into_response()
        }
    }
}

/// Pre-inspect the mode, sleep if slow, then dispatch to [`respond`].
async fn apply_mode(mode: &ResponseMode, healthy_body: Value) -> Response {
    // Inject the delay BEFORE building the response (plan §6: slow backend).
    if let ResponseMode::Slow { delay_ms } = mode {
        tokio::time::sleep(std::time::Duration::from_millis(*delay_ms)).await;
        return (StatusCode::OK, Json(healthy_body)).into_response();
    }
    // For Healthy: use the endpoint-specific body.
    if matches!(mode, ResponseMode::Healthy) {
        return (StatusCode::OK, Json(healthy_body)).into_response();
    }
    respond(mode).into_response()
}

async fn handle_health(State(st): State<AppState>) -> Response {
    let mode = st.scenario.lock().await.health.clone();
    apply_mode(&mode, json!({"status": "ok"})).await
}

async fn handle_chat(State(st): State<AppState>) -> Response {
    let scenario = st.scenario.lock().await;
    let mode = scenario.chat.clone();
    let content = scenario
        .chat_content
        .clone()
        .unwrap_or_else(|| "mock response".to_string());
    drop(scenario);
    apply_mode(
        &mode,
        json!({
            "id": "chatcmpl-mock",
            "object": "chat.completion",
            "created": 1700000000u64,
            "model": "mock-model",
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": content
                },
                "finish_reason": "stop"
            }],
            "usage": {
                "prompt_tokens": 10,
                "completion_tokens": 5,
                "total_tokens": 15
            }
        }),
    )
    .await
}

/// A deterministic `dim`-dimensional unit "embedding" for `text`: a single 1.0 spike at
/// `hash(text) % dim`. Distinct texts map to (almost always) distinct indices → orthogonal
/// vectors (cosine 0); identical texts map to the same vector. Stable across runs because
/// `DefaultHasher` is seeded with fixed keys (§31.3 determinism).
fn hash_embedding(text: &str, dim: usize) -> Vec<f32> {
    use std::hash::{Hash, Hasher};
    let mut v = vec![0.0f32; dim];
    if dim > 0 {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        text.hash(&mut h);
        v[(h.finish() as usize) % dim] = 1.0;
    }
    v
}

async fn handle_embed(
    State(st): State<AppState>,
    body: Option<axum::extract::Json<Value>>,
) -> Response {
    let scenario = st.scenario.lock().await;
    let mode = scenario.embed.clone();
    let content = scenario.embed_content.clone();
    let embed_dim = scenario.embed_dim;
    drop(scenario);

    let data: Vec<Value> = if let Some(vectors) = content {
        vectors
            .into_iter()
            .enumerate()
            .map(|(i, emb)| json!({"object": "embedding", "index": i, "embedding": emb}))
            .collect()
    } else if let Some(dim) = embed_dim {
        // One deterministic vector per input text (a real embedder maps distinct text to
        // distinct vectors). Falls back to a single vector if the body has no `input` array.
        let inputs: Vec<String> = body
            .and_then(|axum::extract::Json(v)| v.get("input").and_then(|i| i.as_array().cloned()))
            .map(|arr| {
                arr.into_iter()
                    .map(|t| t.as_str().unwrap_or_default().to_string())
                    .collect()
            })
            .unwrap_or_else(|| vec![String::new()]);
        inputs
            .into_iter()
            .enumerate()
            .map(|(i, text)| {
                json!({"object": "embedding", "index": i, "embedding": hash_embedding(&text, dim)})
            })
            .collect()
    } else {
        vec![json!({
            "object": "embedding",
            "index": 0,
            "embedding": [0.1, 0.2, 0.3]
        })]
    };

    apply_mode(
        &mode,
        json!({
            "object": "list",
            "model": "mock-embed-model",
            "data": data,
            "usage": {
                "prompt_tokens": 3,
                "total_tokens": 3
            }
        }),
    )
    .await
}

async fn handle_rerank(
    State(st): State<AppState>,
    axum::extract::Json(body): axum::extract::Json<serde_json::Value>,
) -> Response {
    let scenario = st.scenario.lock().await;
    let mode = scenario.rerank.clone();
    let content = scenario.rerank_content.clone();
    let doc_count = body
        .get("documents")
        .and_then(|d| d.as_array())
        .map_or(0, |a| a.len());
    drop(scenario);

    let results: Vec<Value> = match content {
        Some(scores) => scores
            .into_iter()
            .enumerate()
            .map(|(i, score)| json!({"index": i, "relevance_score": score}))
            .collect(),
        None => (0..doc_count)
            .map(|i| {
                let score = 1.0_f32 - (i as f32 * 0.1);
                json!({"index": i, "relevance_score": score.max(0.0)})
            })
            .collect(),
    };

    apply_mode(
        &mode,
        json!({
            "object": "list",
            "model": "mock-rerank-model",
            "results": results,
            "usage": {
                "prompt_tokens": 10,
                "total_tokens": 10
            }
        }),
    )
    .await
}

// ── unit tests ─────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    /// Start → verify the server binds, serves health, and shuts down.
    #[tokio::test]
    async fn start_serves_health_and_shuts_down() {
        let mock = MockBackend::start().await;
        let status = reqwest::get(mock.url("/health")).await.unwrap().status();
        assert_eq!(status, 200);
        mock.shutdown().await;
    }

    /// Unhealthy mode → 503.
    #[tokio::test]
    async fn unhealthy_returns_503() {
        let mock = MockBackend::start().await;
        mock.scenario().lock().await.health = ResponseMode::Unhealthy;

        let resp = reqwest::get(mock.url("/health")).await.unwrap();
        assert_eq!(resp.status(), 503);

        mock.shutdown().await;
    }

    /// ServerError mode → 500.
    #[tokio::test]
    async fn server_error_returns_500() {
        let mock = MockBackend::start().await;
        mock.scenario().lock().await.chat = ResponseMode::ServerError;

        let client = reqwest::Client::new();
        let resp = client
            .post(mock.url("/v1/chat/completions"))
            .json(&json!({"model": "x", "messages": []}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 500);

        mock.shutdown().await;
    }

    /// RateLimited mode → 429 with Retry-After header.
    #[tokio::test]
    async fn rate_limited_returns_429_with_retry_after() {
        let mock = MockBackend::start().await;
        mock.scenario().lock().await.chat = ResponseMode::RateLimited {
            retry_after_secs: 42,
        };

        let client = reqwest::Client::new();
        let resp = client
            .post(mock.url("/v1/chat/completions"))
            .json(&json!({"model": "x", "messages": []}))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 429);
        assert_eq!(
            resp.headers().get("Retry-After").unwrap().to_str().unwrap(),
            "42"
        );

        mock.shutdown().await;
    }

    /// Slow mode eventually responds 200 (delay is real but short enough for
    /// a unit test).
    #[tokio::test]
    async fn slow_eventually_responds_200() {
        let mock = MockBackend::start().await;
        mock.scenario().lock().await.embed = ResponseMode::Slow { delay_ms: 50 };

        let client = reqwest::Client::new();
        let start = std::time::Instant::now();
        let resp = client
            .post(mock.url("/v1/embeddings"))
            .json(&json!({"model": "x", "input": "hi"}))
            .send()
            .await
            .unwrap();
        let elapsed = start.elapsed();

        assert_eq!(resp.status(), 200);
        assert!(
            elapsed >= std::time::Duration::from_millis(50),
            "slow mode: expected ≥50ms, got {elapsed:?}"
        );

        mock.shutdown().await;
    }

    /// Chat endpoint returns a realistic-looking completion body.
    #[tokio::test]
    async fn chat_returns_completion_shape() {
        let mock = MockBackend::start().await;

        let client = reqwest::Client::new();
        let body: Value = client
            .post(mock.url("/v1/chat/completions"))
            .json(&json!({"model": "x", "messages": [{"role": "user", "content": "hi"}]}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();

        assert_eq!(body["object"], "chat.completion");
        assert_eq!(body["choices"][0]["message"]["content"], "mock response");
        assert!(body["usage"]["total_tokens"].is_number());

        mock.shutdown().await;
    }

    /// Embed endpoint returns a realistic-looking embedding body.
    #[tokio::test]
    async fn embed_returns_embedding_shape() {
        let mock = MockBackend::start().await;

        let client = reqwest::Client::new();
        let body: Value = client
            .post(mock.url("/v1/embeddings"))
            .json(&json!({"model": "x", "input": "hi"}))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();

        assert_eq!(body["object"], "list");
        assert_eq!(body["data"][0]["embedding"][0], 0.1);
        assert!(body["usage"]["total_tokens"].is_number());

        mock.shutdown().await;
    }

    /// url() convenience method.
    #[tokio::test]
    async fn url_helper() {
        let mock = MockBackend::start().await;
        let u = mock.url("/health");
        assert!(u.starts_with("http://127.0.0.1:"));
        assert!(u.ends_with("/health"));
        mock.shutdown().await;
    }

    /// Rerank endpoint returns a realistic-looking rerank body with
    /// scores aligned to input document count.
    #[tokio::test]
    async fn rerank_returns_scores_shape() {
        let mock = MockBackend::start().await;

        let client = reqwest::Client::new();
        let body: Value = client
            .post(mock.url("/v1/rerank"))
            .json(&json!({
                "model": "mock-rerank",
                "query": "test query",
                "documents": ["doc a", "doc b", "doc c"]
            }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();

        assert_eq!(body["object"], "list");
        assert_eq!(body["model"], "mock-rerank-model");
        let results = body["results"].as_array().unwrap();
        assert_eq!(results.len(), 3);
        // Default scores descend from 1.0
        assert!((results[0]["relevance_score"].as_f64().unwrap() - 1.0_f64).abs() < 0.01);
        assert!((results[1]["relevance_score"].as_f64().unwrap() - 0.9_f64).abs() < 0.01);
        assert!((results[2]["relevance_score"].as_f64().unwrap() - 0.8_f64).abs() < 0.01);
        assert!(body["usage"]["total_tokens"].is_number());

        mock.shutdown().await;
    }

    /// Rerank with custom scores via `rerank_content`.
    #[tokio::test]
    async fn rerank_with_custom_content() {
        let mock = MockBackend::start().await;
        mock.scenario().lock().await.rerank_content = Some(vec![0.42, 0.73, 0.15]);

        let client = reqwest::Client::new();
        let body: Value = client
            .post(mock.url("/v1/rerank"))
            .json(&json!({
                "model": "mock-rerank",
                "query": "test",
                "documents": ["a", "b", "c"]
            }))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();

        let results = body["results"].as_array().unwrap();
        assert_eq!(results.len(), 3);
        assert!((results[0]["relevance_score"].as_f64().unwrap() - 0.42_f64).abs() < 0.01);
        assert!((results[1]["relevance_score"].as_f64().unwrap() - 0.73_f64).abs() < 0.01);
        assert!((results[2]["relevance_score"].as_f64().unwrap() - 0.15_f64).abs() < 0.01);

        mock.shutdown().await;
    }

    /// Rerank with ServerError mode.
    #[tokio::test]
    async fn rerank_returns_500() {
        let mock = MockBackend::start().await;
        mock.scenario().lock().await.rerank = ResponseMode::ServerError;

        let client = reqwest::Client::new();
        let resp = client
            .post(mock.url("/v1/rerank"))
            .json(&json!({
                "model": "x",
                "query": "q",
                "documents": ["a"]
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(resp.status(), 500);

        mock.shutdown().await;
    }

    /// Scenario::default() starts healthy on all endpoints.
    #[tokio::test]
    async fn default_scenario_all_healthy() {
        let mock = MockBackend::start().await;
        let client = reqwest::Client::new();

        assert_eq!(
            reqwest::get(mock.url("/health")).await.unwrap().status(),
            200
        );
        assert_eq!(
            client
                .post(mock.url("/v1/chat/completions"))
                .json(&json!({"model":"x","messages":[]}))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        assert_eq!(
            client
                .post(mock.url("/v1/embeddings"))
                .json(&json!({"model":"x","input":"x"}))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
        assert_eq!(
            client
                .post(mock.url("/v1/rerank"))
                .json(&json!({"model":"x","query":"q","documents":["a"]}))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );

        mock.shutdown().await;
    }
}
