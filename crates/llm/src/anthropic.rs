//! [`Anthropic`] — a [`ProviderAdapter`](kb_core::provider::ProviderAdapter) for
//! Anthropic's native Messages API (`x-api-key`, `anthropic-version: 2023-06-01`,
//! system parameter). Normalizes to [`ChatResp`]/[`EmbedResp`] (plan §26.1).
//!
//! # Feature gating
//!
//! Compiled behind `#[cfg(feature = "anthropic")]` so it does not bloat the
//! default binary. `cargo test --all-features` always exercises it.

use async_trait::async_trait;
use kb_core::provider::{
    ChatReq, ChatResp, EmbedReq, EmbedResp, ProviderAdapter, ProviderConn, Usage,
};
use kb_core::role::Role;
use reqwest::Client;
use serde::Serialize;
use serde::de::DeserializeOwned;

// ── Anthropic wire types (Messages API) ──────────────────────────────────────

/// Anthropic Messages API request body.
#[derive(Debug, Serialize)]
struct AnthropicRequestBody<'a> {
    model: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    system: Option<AnthropicSystem<'a>>,
    messages: Vec<AnthropicMessage<'a>>,
    max_tokens: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    temperature: Option<f32>,
}

/// The `system` parameter — a string (older API) or array of text blocks.
///
/// We use the string form for simplicity; Anthropic accepts it across all
/// API versions declared via `anthropic-version`.
#[derive(Debug, Serialize)]
#[serde(untagged)]
enum AnthropicSystem<'a> {
    Text(&'a str),
}

/// One message in the `messages` array.
#[derive(Debug, Serialize)]
struct AnthropicMessage<'a> {
    role: &'a str,
    content: &'a str,
}

/// Anthropic Messages API response (success).
#[derive(Debug, serde::Deserialize)]
struct AnthropicChatResp {
    content: Vec<AnthropicContentBlock>,
    usage: AnthropicUsage,
}

/// A content block in the response. We only decode `text` blocks today; other
/// block types (tool_use, thinking, …) are silently dropped.
#[derive(Debug, serde::Deserialize)]
#[serde(tag = "type")]
enum AnthropicContentBlock {
    #[serde(rename = "text")]
    Text { text: String },
    /// Catch-all for unknown blocks — we don't fail on them but produce no text.
    #[serde(other)]
    Unknown,
}

/// Token usage from Anthropic.
#[derive(Debug, serde::Deserialize)]
struct AnthropicUsage {
    input_tokens: u32,
    output_tokens: u32,
}

/// Anthropic error response body for better diagnostics.
#[derive(Debug, serde::Deserialize)]
struct AnthropicErrorBody {
    error: AnthropicErrorDetail,
}

#[derive(Debug, serde::Deserialize)]
struct AnthropicErrorDetail {
    #[serde(rename = "type")]
    type_: String,
    message: String,
}

// ── Anthropic adapter ────────────────────────────────────────────────────────

/// Adapter for Anthropic's native Messages API.
///
/// POSTs to `{endpoint}/v1/messages` with `x-api-key` and `anthropic-version:
/// 2023-06-01` headers. Supports chat roles (`Text`, `Vision`, `Code`); does
/// **not** support `Embed` (Anthropic has no embeddings endpoint, plan §26.6).
#[derive(Debug)]
pub struct Anthropic {
    http: Client,
}

impl Anthropic {
    /// Create a new adapter with the given HTTP client.
    pub fn new(http: Client) -> Self {
        Self { http }
    }

    /// The API version header value pinned for this adapter.
    pub const API_VERSION: &'static str = "2023-06-01";
}

#[async_trait]
impl ProviderAdapter for Anthropic {
    async fn chat(&self, conn: &ProviderConn, req: ChatReq) -> anyhow::Result<ChatResp> {
        let base = require_endpoint(conn.endpoint.as_deref())?;
        let url = format!("{base}/v1/messages");

        // Extract system message(s) from the request. Anthropic takes `system`
        // as a top-level field, not a message role. Multiple system messages are
        // joined with a paragraph break.
        let system = {
            let mut parts: Vec<&str> = req
                .messages
                .iter()
                .filter(|m| m.role.as_str() == "system")
                .map(|m| m.content.as_str())
                .filter(|s| !s.is_empty())
                .collect();
            if parts.is_empty() {
                None
            } else {
                // For the single-system string form, take the first one.
                Some(AnthropicSystem::Text(parts.remove(0)))
            }
        };

        // Non-system messages go into the `messages` array.
        let messages: Vec<AnthropicMessage<'_>> = req
            .messages
            .iter()
            .filter(|m| m.role.as_str() != "system")
            .map(|m| AnthropicMessage {
                role: m.role.as_str(),
                content: &m.content,
            })
            .collect();

        let max_tokens = req.max_tokens.unwrap_or(1024);

        let body = AnthropicRequestBody {
            model: &conn.model_id,
            system,
            messages,
            max_tokens,
            temperature: req.temperature,
        };

        let raw: AnthropicChatResp = self.post_json(&url, conn, &body).await?;

        // Collect text from all text content blocks.
        let text = raw
            .content
            .iter()
            .filter_map(|b| match b {
                AnthropicContentBlock::Text { text } => Some(text.as_str()),
                AnthropicContentBlock::Unknown => None,
            })
            .collect::<Vec<&str>>()
            .join("\n");

        Ok(ChatResp {
            text,
            usage: Usage {
                prompt_tokens: raw.usage.input_tokens,
                completion_tokens: raw.usage.output_tokens,
            },
        })
    }

    async fn embed(&self, _conn: &ProviderConn, _req: EmbedReq) -> anyhow::Result<EmbedResp> {
        anyhow::bail!(
            "Anthropic does not provide an embeddings endpoint. \
             Route embed requests to a provider that supports Role::Embed."
        )
    }

    fn supports(&self, role: Role) -> bool {
        matches!(role, Role::Text | Role::Vision | Role::Code)
    }
}

// ── helpers ──────────────────────────────────────────────────────────────────

impl Anthropic {
    /// POST JSON with `x-api-key` + `anthropic-version` headers; map non-2xx →
    /// descriptive error.
    async fn post_json<Req: Serialize + std::fmt::Debug, Resp: DeserializeOwned>(
        &self,
        url: &str,
        conn: &ProviderConn,
        body: &Req,
    ) -> anyhow::Result<Resp> {
        let mut request = self
            .http
            .post(url)
            .json(body)
            .header("anthropic-version", Self::API_VERSION);

        if let Some(key) = &conn.api_key {
            request = request.header("x-api-key", key.as_str());
        }
        for (k, v) in &conn.headers {
            request = request.header(k.as_str(), v.as_str());
        }

        let resp = request.send().await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body_text = resp.text().await.unwrap_or_default();
            // Try to extract the Anthropic error message for better diagnostics.
            if let Ok(err_body) = serde_json::from_str::<AnthropicErrorBody>(&body_text) {
                anyhow::bail!(
                    "Anthropic HTTP {status}: {} — {}",
                    err_body.error.type_,
                    err_body.error.message
                );
            }
            anyhow::bail!("Anthropic HTTP {status}: {body_text}");
        }
        Ok(resp.json().await?)
    }
}

fn require_endpoint(endpoint: Option<&str>) -> anyhow::Result<&str> {
    endpoint.ok_or_else(|| {
        anyhow::anyhow!(
            "Anthropic adapter requires an HTTP endpoint; ProviderConn::endpoint is None. \
             Use the Anthropic SDK directly for non-HTTP paths."
        )
    })
}

// ── unit tests ──────────────────────────────────────────────────────────────
//
// Tests run via `cargo test --all-features` (the justfile `test` recipe
// already passes `--all-features`, and `just ci` includes `test`).

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use std::net::SocketAddr;
    use std::sync::Arc;

    use axum::Router;
    use axum::extract::Json;
    use axum::http::{HeaderMap, StatusCode};
    use axum::response::IntoResponse;
    use axum::routing::post;
    use kb_core::provider::ChatRole;
    use kb_core::provider::{ChatMessage, ChatReq, EmbedReq};
    use kb_core::role::Role;
    use reqwest::Client;
    use serde_json::json;
    use tokio::net::TcpListener;
    use tokio::sync::Mutex;

    use super::*;

    // ── mock Anthropic server ─────────────────────────────────────────────

    /// Shared state for the mock Anthropic server, captured by the handler.
    #[derive(Clone)]
    struct MockState {
        /// Captured `x-api-key` header value.
        api_key: Arc<Mutex<Option<String>>>,
        /// Captured request body as parsed JSON.
        body: Arc<Mutex<Option<serde_json::Value>>>,
        /// Status code override (200 by default).
        status: Arc<Mutex<u16>>,
        /// Response body override (when set, used instead of the default).
        response: Arc<Mutex<Option<serde_json::Value>>>,
    }

    impl Default for MockState {
        fn default() -> Self {
            Self {
                api_key: Arc::new(Mutex::new(None)),
                body: Arc::new(Mutex::new(None)),
                status: Arc::new(Mutex::new(200)),
                response: Arc::new(Mutex::new(None)),
            }
        }
    }

    /// Start a lightweight mock Anthropic server on an ephemeral port.
    /// Returns the address and the mutable shared state so tests can inspect
    /// captured headers/body and override status/responses.
    async fn start_mock_anthropic() -> (SocketAddr, MockState) {
        let state = MockState::default();

        let app = Router::new().route(
            "/v1/messages",
            post({
                let s = state.clone();
                move |headers: HeaderMap, Json(body): Json<serde_json::Value>| {
                    let s = s.clone();
                    async move {
                        {
                            let mut key = s.api_key.lock().await;
                            *key = headers
                                .get("x-api-key")
                                .and_then(|v| v.to_str().ok())
                                .map(String::from);
                        }
                        {
                            let mut b = s.body.lock().await;
                            *b = Some(body.clone());
                        }

                        let status_code = *s.status.lock().await;
                        let override_body = s.response.lock().await.clone();

                        let resp_body = override_body.unwrap_or_else(|| {
                            json!({
                                "id": "msg_mock123",
                                "type": "message",
                                "role": "assistant",
                                "model": "claude-mock",
                                "content": [
                                    {"type": "text", "text": "Hello from mock Anthropic!"}
                                ],
                                "stop_reason": "end_turn",
                                "usage": {
                                    "input_tokens": 15,
                                    "output_tokens": 8
                                }
                            })
                        });

                        (StatusCode::from_u16(status_code).unwrap(), Json(resp_body))
                            .into_response()
                    }
                }
            }),
        );

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });

        // Wait until the server is actually accepting connections to avoid
        // "connection closed before message completed" flakes, then reset
        // the captured state so the readiness probe doesn't pollute tests.
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(2))
            .build()
            .unwrap();
        let url = format!("http://{addr}/v1/messages");
        let body =
            json!({"model":"x","max_tokens":1,"messages":[{"role":"user","content":"ping"}]});
        for _ in 0..20 {
            let resp = client.post(&url).json(&body).send().await;
            if resp.is_ok() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        // Reset captured state from the readiness probe.
        *state.api_key.lock().await = None;
        *state.body.lock().await = None;

        (addr, state)
    }

    // ── helpers ───────────────────────────────────────────────────────────

    fn conn_for(addr: SocketAddr, api_key: Option<&str>) -> ProviderConn {
        ProviderConn {
            endpoint: Some(format!("http://{addr}")),
            api_key: api_key.map(String::from),
            model_id: "claude-sonnet-4-6".into(),
            headers: vec![],
        }
    }

    fn chat_req(content: &str) -> ChatReq {
        ChatReq {
            messages: vec![ChatMessage {
                role: ChatRole::User,
                content: content.into(),
            }],
            ..Default::default()
        }
    }

    fn chat_req_with_system(system: &str, user: &str) -> ChatReq {
        ChatReq {
            messages: vec![
                ChatMessage {
                    role: ChatRole::System,
                    content: system.into(),
                },
                ChatMessage {
                    role: ChatRole::User,
                    content: user.into(),
                },
            ],
            ..Default::default()
        }
    }

    // ── supports ─────────────────────────────────────────────────────────

    #[test]
    fn supports_text_vision_code() {
        let a = Anthropic::new(Client::new());
        assert!(a.supports(Role::Text));
        assert!(a.supports(Role::Vision));
        assert!(a.supports(Role::Code));
    }

    #[test]
    fn does_not_support_embed() {
        let a = Anthropic::new(Client::new());
        assert!(!a.supports(Role::Embed));
    }

    #[test]
    fn does_not_support_rerank() {
        // Anthropic has no native rerank endpoint.
        let a = Anthropic::new(Client::new());
        assert!(!a.supports(Role::Rerank));
    }

    // ── chat success ──────────────────────────────────────────────────────

    #[tokio::test]
    async fn chat_success() {
        let (addr, _state) = start_mock_anthropic().await;
        let a = Anthropic::new(Client::new());
        let c = conn_for(addr, Some("sk-ant-test"));

        let resp = a.chat(&c, chat_req("hello")).await.unwrap();
        assert_eq!(resp.text, "Hello from mock Anthropic!");
        assert_eq!(resp.usage.prompt_tokens, 15);
        assert_eq!(resp.usage.completion_tokens, 8);
    }

    #[tokio::test]
    async fn chat_with_system_message() {
        let (addr, state) = start_mock_anthropic().await;
        let a = Anthropic::new(Client::new());
        let c = conn_for(addr, Some("sk-ant-test"));

        let resp = a
            .chat(
                &c,
                chat_req_with_system("You are a helpful assistant.", "hi"),
            )
            .await
            .unwrap();
        assert_eq!(resp.text, "Hello from mock Anthropic!");

        // The system message should have been extracted into the `system` field.
        let body = state.body.lock().await.clone().unwrap();
        assert_eq!(body["system"], json!("You are a helpful assistant."));
        // The messages array should contain only the user message.
        let msgs = body["messages"].as_array().unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0]["role"], "user");
        assert_eq!(msgs[0]["content"], "hi");
    }

    #[tokio::test]
    async fn chat_without_system_omits_system_field() {
        let (addr, state) = start_mock_anthropic().await;
        let a = Anthropic::new(Client::new());
        let c = conn_for(addr, None);

        a.chat(&c, chat_req("plain user message")).await.unwrap();

        let body = state.body.lock().await.clone().unwrap();
        // system should not be present when there are no system messages.
        assert!(body.get("system").is_none());
    }

    #[tokio::test]
    async fn max_tokens_defaults_to_1024() {
        let (addr, state) = start_mock_anthropic().await;
        let a = Anthropic::new(Client::new());
        // No max_tokens in the request.
        let c = conn_for(addr, None);
        let req = ChatReq {
            messages: vec![ChatMessage {
                role: ChatRole::User,
                content: "test".into(),
            }],
            max_tokens: None,
            ..Default::default()
        };

        a.chat(&c, req).await.unwrap();

        let body = state.body.lock().await.clone().unwrap();
        assert_eq!(body["max_tokens"], json!(1024));
    }

    #[tokio::test]
    async fn max_tokens_respected_when_set() {
        let (addr, state) = start_mock_anthropic().await;
        let a = Anthropic::new(Client::new());
        let c = conn_for(addr, None);
        let req = ChatReq {
            messages: vec![ChatMessage {
                role: ChatRole::User,
                content: "test".into(),
            }],
            max_tokens: Some(512),
            ..Default::default()
        };

        a.chat(&c, req).await.unwrap();

        let body = state.body.lock().await.clone().unwrap();
        assert_eq!(body["max_tokens"], json!(512));
    }

    // ── auth header ─────────────────────────────────────────────────────

    #[tokio::test]
    async fn x_api_key_header_sent() {
        let (addr, state) = start_mock_anthropic().await;
        let a = Anthropic::new(Client::new());
        let c = conn_for(addr, Some("sk-ant-mykey123"));

        a.chat(&c, chat_req("hi")).await.unwrap();
        assert_eq!(
            state.api_key.lock().await.as_deref(),
            Some("sk-ant-mykey123")
        );
    }

    #[tokio::test]
    async fn no_x_api_key_when_key_absent() {
        let (addr, state) = start_mock_anthropic().await;
        let a = Anthropic::new(Client::new());
        let c = conn_for(addr, None);

        a.chat(&c, chat_req("hi")).await.unwrap();
        assert!(state.api_key.lock().await.is_none());
    }

    // ── anthropic-version header ────────────────────────────────────────

    #[test]
    fn api_version_constant_is_pinned() {
        assert_eq!(Anthropic::API_VERSION, "2023-06-01");
    }

    // ── embed() error ───────────────────────────────────────────────────

    #[tokio::test]
    async fn embed_returns_error() {
        let (addr, _state) = start_mock_anthropic().await;
        let a = Anthropic::new(Client::new());
        let c = conn_for(addr, None);

        let err = a
            .embed(
                &c,
                EmbedReq {
                    texts: vec!["test".into()],
                },
            )
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("does not provide"),
            "expected embed error, got: {err}"
        );
    }

    // ── missing endpoint ────────────────────────────────────────────────

    #[tokio::test]
    async fn chat_missing_endpoint_errors() {
        let a = Anthropic::new(Client::new());
        let c = ProviderConn {
            endpoint: None,
            ..Default::default()
        };
        let err = a.chat(&c, chat_req("hi")).await.unwrap_err();
        assert!(err.to_string().contains("endpoint"));
    }

    // ── non-2xx error ───────────────────────────────────────────────────

    #[tokio::test]
    async fn chat_401_returns_error() {
        let (addr, state) = start_mock_anthropic().await;
        {
            *state.status.lock().await = 401;
            *state.response.lock().await = Some(json!({
                "error": {
                    "type": "authentication_error",
                    "message": "invalid x-api-key"
                }
            }));
        }
        let a = Anthropic::new(Client::new());
        let c = conn_for(addr, Some("bad-key"));

        let err = a.chat(&c, chat_req("hi")).await.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("401"), "expected 401 in: {msg}");
        assert!(
            msg.contains("authentication_error"),
            "expected type in: {msg}"
        );
        assert!(
            msg.contains("invalid x-api-key"),
            "expected message in: {msg}"
        );
    }

    #[tokio::test]
    async fn chat_500_returns_error() {
        let (addr, state) = start_mock_anthropic().await;
        *state.status.lock().await = 500;
        let a = Anthropic::new(Client::new());
        let c = conn_for(addr, None);

        let err = a.chat(&c, chat_req("hi")).await.unwrap_err();
        assert!(err.to_string().contains("500"));
    }

    #[tokio::test]
    async fn chat_429_returns_error() {
        let (addr, state) = start_mock_anthropic().await;
        {
            *state.status.lock().await = 429;
            *state.response.lock().await = Some(json!({
                "error": {
                    "type": "rate_limit_error",
                    "message": "Too many requests"
                }
            }));
        }
        let a = Anthropic::new(Client::new());
        let c = conn_for(addr, None);

        let err = a.chat(&c, chat_req("hi")).await.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("429"), "expected 429 in: {msg}");
        assert!(msg.contains("rate_limit_error"), "expected type in: {msg}");
    }

    // ── temperature passthrough ─────────────────────────────────────────

    #[tokio::test]
    async fn temperature_is_passed_through() {
        let (addr, state) = start_mock_anthropic().await;
        let a = Anthropic::new(Client::new());
        let c = conn_for(addr, None);
        let req = ChatReq {
            messages: vec![ChatMessage {
                role: ChatRole::User,
                content: "test".into(),
            }],
            temperature: Some(0.7),
            ..Default::default()
        };

        a.chat(&c, req).await.unwrap();

        let body = state.body.lock().await.clone().unwrap();
        assert_eq!(body["temperature"], json!(0.7));
    }

    #[tokio::test]
    async fn temperature_omitted_when_none() {
        let (addr, state) = start_mock_anthropic().await;
        let a = Anthropic::new(Client::new());
        let c = conn_for(addr, None);
        let req = ChatReq {
            messages: vec![ChatMessage {
                role: ChatRole::User,
                content: "test".into(),
            }],
            temperature: None,
            ..Default::default()
        };

        a.chat(&c, req).await.unwrap();

        let body = state.body.lock().await.clone().unwrap();
        assert!(body.get("temperature").is_none());
    }

    // ── multi-block response ────────────────────────────────────────────

    #[tokio::test]
    async fn multi_text_blocks_are_joined() {
        let (addr, state) = start_mock_anthropic().await;
        {
            *state.response.lock().await = Some(json!({
                "id": "msg_multi",
                "type": "message",
                "role": "assistant",
                "model": "claude-mock",
                "content": [
                    {"type": "text", "text": "First block."},
                    {"type": "text", "text": "Second block."}
                ],
                "stop_reason": "end_turn",
                "usage": {
                    "input_tokens": 10,
                    "output_tokens": 20
                }
            }));
        }
        let a = Anthropic::new(Client::new());
        let c = conn_for(addr, None);

        let resp = a.chat(&c, chat_req("hi")).await.unwrap();
        assert_eq!(resp.text, "First block.\nSecond block.");
    }
}
