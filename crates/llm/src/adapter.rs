//! [`OpenAiCompat`] — a [`ProviderAdapter`](kb_core::provider::ProviderAdapter)
//! for any OpenAI-compatible HTTP API (plan §26.1). Covers local
//! `llama.cpp`/vLLM, DeepSeek, Qwen, Gemini (OpenAI-compat), OpenAI, etc.
//! POSTs to `{endpoint}/v1/chat/completions` and `/v1/embeddings` with an
//! optional `Authorization: Bearer` header.

use async_trait::async_trait;
use kb_core::provider::{
    ChatReq, ChatResp, EmbedReq, EmbedResp, ProviderAdapter, ProviderConn, Usage,
};
use kb_core::role::Role;
use reqwest::Client;
use serde::Serialize;
use serde::de::DeserializeOwned;

// ── OpenAI wire types ────────────────────────────────────────────────────────

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

#[derive(Debug, Serialize)]
struct OpenAiEmbedBody<'a> {
    model: &'a str,
    input: &'a [String],
}

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

#[derive(Debug, serde::Deserialize)]
struct OpenAiUsage {
    prompt_tokens: u32,
    #[serde(default)]
    completion_tokens: u32,
}

// ── OpenAiCompat ─────────────────────────────────────────────────────────────

/// An OpenAI-compatible HTTP adapter implementing [`ProviderAdapter`].
///
/// POSTs to `{endpoint}/v1/chat/completions` and `/v1/embeddings` with optional
/// `Authorization: Bearer`. `supported_roles` gates [`supports`](ProviderAdapter::supports);
/// `endpoint` must be a host base URL — `None` returns an error.
pub struct OpenAiCompat {
    http: Client,
    supported_roles: Vec<Role>,
}

impl OpenAiCompat {
    /// Create a new adapter with the given HTTP client and role set.
    pub fn new(http: Client, supported_roles: Vec<Role>) -> Self {
        Self {
            http,
            supported_roles,
        }
    }
}

#[async_trait]
impl ProviderAdapter for OpenAiCompat {
    async fn chat(&self, conn: &ProviderConn, req: ChatReq) -> anyhow::Result<ChatResp> {
        let base = require_endpoint(conn.endpoint.as_deref())?;
        let url = format!("{base}/v1/chat/completions");

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

        let body = OpenAiChatBody {
            model: &conn.model_id,
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

        let raw: OpenAiChatResp = self.post_json(&url, conn, &body).await?;
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

    async fn embed(&self, conn: &ProviderConn, req: EmbedReq) -> anyhow::Result<EmbedResp> {
        let base = require_endpoint(conn.endpoint.as_deref())?;
        let url = format!("{base}/v1/embeddings");

        let body = OpenAiEmbedBody {
            model: &conn.model_id,
            input: &req.texts,
        };

        let raw: OpenAiEmbedResp = self.post_json(&url, conn, &body).await?;
        Ok(EmbedResp {
            vectors: raw.data.into_iter().map(|d| d.embedding).collect(),
            usage: raw.usage.map_or(Usage::default(), |u| Usage {
                prompt_tokens: u.prompt_tokens,
                completion_tokens: u.completion_tokens,
            }),
        })
    }

    fn supports(&self, role: Role) -> bool {
        self.supported_roles.contains(&role)
    }
}

// ── helpers ──────────────────────────────────────────────────────────────────

impl OpenAiCompat {
    /// POST JSON, attach auth/extra headers, map non-2xx to errors.
    async fn post_json<Req: Serialize + std::fmt::Debug, Resp: DeserializeOwned>(
        &self,
        url: &str,
        conn: &ProviderConn,
        body: &Req,
    ) -> anyhow::Result<Resp> {
        let mut request = self.http.post(url).json(body);
        if let Some(key) = &conn.api_key {
            request = request.header("Authorization", format!("Bearer {key}"));
        }
        for (k, v) in &conn.headers {
            request = request.header(k.as_str(), v.as_str());
        }

        let resp = request.send().await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body_text = resp.text().await.unwrap_or_default();
            anyhow::bail!("HTTP {status}: {body_text}");
        }
        Ok(resp.json().await?)
    }
}

fn require_endpoint(endpoint: Option<&str>) -> anyhow::Result<&str> {
    endpoint.ok_or_else(|| {
        anyhow::anyhow!(
            "OpenAiCompat requires an HTTP endpoint; ProviderConn::endpoint is None. \
             Use a native-SDK adapter for non-HTTP providers."
        )
    })
}

// ── unit tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use std::net::SocketAddr;
    use std::sync::Arc;

    use axum::Router;
    use axum::response::Json;
    use axum::routing::post;
    use kb_core::provider::{ChatMessage, ChatReq, EmbedReq};
    use kb_core::role::Role;
    use kb_mock_backend::{MockBackend, ResponseMode};
    use reqwest::Client;
    use serde_json::json;
    use tokio::net::TcpListener;
    use tokio::sync::Mutex;

    use super::*;

    // ── helpers ───────────────────────────────────────────────────────────

    fn all_roles() -> Vec<Role> {
        Role::all().to_vec()
    }

    fn conn_for(addr: SocketAddr, api_key: Option<&str>) -> ProviderConn {
        ProviderConn {
            endpoint: Some(format!("http://{addr}")),
            api_key: api_key.map(String::from),
            model_id: "test-model".into(),
            headers: vec![],
        }
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

    // ── supports ─────────────────────────────────────────────────────────

    #[test]
    fn supports_all_roles_when_configured() {
        let a = OpenAiCompat::new(Client::new(), all_roles());
        for r in Role::all() {
            assert!(a.supports(*r), "should support {r}");
        }
    }

    #[test]
    fn supports_only_configured_roles() {
        let a = OpenAiCompat::new(Client::new(), vec![Role::Text, Role::Embed]);
        assert!(a.supports(Role::Text));
        assert!(a.supports(Role::Embed));
        assert!(!a.supports(Role::Vision));
        assert!(!a.supports(Role::Code));
        assert!(!a.supports(Role::Rerank));
    }

    #[test]
    fn supports_empty_returns_false_for_all() {
        let a = OpenAiCompat::new(Client::new(), vec![]);
        for r in Role::all() {
            assert!(!a.supports(*r));
        }
    }

    // ── success paths ────────────────────────────────────────────────────

    #[tokio::test]
    async fn chat_success() {
        let mock = MockBackend::start().await;
        let a = OpenAiCompat::new(Client::new(), all_roles());
        let c = conn_for(mock.addr(), None);

        let resp = a.chat(&c, chat_req("hello")).await.unwrap();
        assert_eq!(resp.text, "mock response");
        assert_eq!(resp.usage.prompt_tokens, 10);
        assert_eq!(resp.usage.completion_tokens, 5);

        mock.shutdown().await;
    }

    #[tokio::test]
    async fn embed_success() {
        let mock = MockBackend::start().await;
        let a = OpenAiCompat::new(Client::new(), all_roles());
        let c = conn_for(mock.addr(), None);

        let req = EmbedReq {
            texts: vec!["hello".into(), "world".into()],
        };
        let resp = a.embed(&c, req).await.unwrap();
        assert_eq!(resp.vectors.len(), 1);
        assert!(!resp.vectors[0].is_empty());
        assert_eq!(resp.usage.prompt_tokens, 3);

        mock.shutdown().await;
    }

    // ── missing endpoint ─────────────────────────────────────────────────

    #[tokio::test]
    async fn chat_missing_endpoint_errors() {
        let a = OpenAiCompat::new(Client::new(), all_roles());
        let c = ProviderConn {
            endpoint: None,
            ..Default::default()
        };
        let err = a.chat(&c, chat_req("hi")).await.unwrap_err();
        assert!(err.to_string().contains("endpoint"));
    }

    #[tokio::test]
    async fn embed_missing_endpoint_errors() {
        let a = OpenAiCompat::new(Client::new(), all_roles());
        let c = ProviderConn {
            endpoint: None,
            ..Default::default()
        };
        let err = a
            .embed(
                &c,
                EmbedReq {
                    texts: vec!["x".into()],
                },
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("endpoint"));
    }

    // ── error responses ──────────────────────────────────────────────────

    #[tokio::test]
    async fn chat_500_returns_error_with_status() {
        let mock = MockBackend::start().await;
        mock.scenario().lock().await.chat = ResponseMode::ServerError;
        let a = OpenAiCompat::new(Client::new(), all_roles());
        let c = conn_for(mock.addr(), None);

        let err = a.chat(&c, chat_req("hi")).await.unwrap_err();
        assert!(err.to_string().contains("500"));
        mock.shutdown().await;
    }

    #[tokio::test]
    async fn embed_500_returns_error_with_status() {
        let mock = MockBackend::start().await;
        mock.scenario().lock().await.embed = ResponseMode::ServerError;
        let a = OpenAiCompat::new(Client::new(), all_roles());
        let c = conn_for(mock.addr(), None);

        let err = a
            .embed(
                &c,
                EmbedReq {
                    texts: vec!["x".into()],
                },
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("500"));
        mock.shutdown().await;
    }

    #[tokio::test]
    async fn chat_429_returns_error_with_status() {
        let mock = MockBackend::start().await;
        mock.scenario().lock().await.chat = ResponseMode::RateLimited {
            retry_after_secs: 30,
        };
        let a = OpenAiCompat::new(Client::new(), all_roles());
        let c = conn_for(mock.addr(), None);

        let err = a.chat(&c, chat_req("hi")).await.unwrap_err();
        assert!(err.to_string().contains("429"));
        mock.shutdown().await;
    }

    // ── authorization header ─────────────────────────────────────────────

    /// Start a tiny axum server that captures the `Authorization` header on
    /// `/v1/chat/completions` and `/v1/embeddings` for test inspection.
    async fn echo_auth_server() -> (SocketAddr, Arc<Mutex<Option<String>>>) {
        let captured = Arc::new(Mutex::new(None::<String>));
        let cap = Arc::clone(&captured);

        let app = Router::new()
            .route("/v1/chat/completions", post({
                let c = Arc::clone(&cap);
                move |h: axum::http::HeaderMap| {
                    let c = Arc::clone(&c);
                    async move {
                        *c.lock().await =
                            h.get("Authorization").and_then(|v| v.to_str().ok()).map(String::from);
                        Json(json!({"choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}],"usage":{"prompt_tokens":0,"completion_tokens":0}}))
                    }
                }
            }))
            .route("/v1/embeddings", post({
                let c = Arc::clone(&cap);
                move |h: axum::http::HeaderMap| {
                    let c = Arc::clone(&c);
                    async move {
                        *c.lock().await =
                            h.get("Authorization").and_then(|v| v.to_str().ok()).map(String::from);
                        Json(json!({"data":[{"object":"embedding","index":0,"embedding":[0.1]}],"usage":{"prompt_tokens":1,"total_tokens":1}}))
                    }
                }
            }));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        (addr, captured)
    }

    #[tokio::test]
    async fn auth_header_sent_when_key_present() {
        let (addr, captured) = echo_auth_server().await;
        let a = OpenAiCompat::new(Client::new(), all_roles());
        let c = ProviderConn {
            endpoint: Some(format!("http://{addr}")),
            api_key: Some("sk-test-key".into()),
            model_id: "m".into(),
            headers: vec![],
        };

        a.chat(&c, chat_req("hi")).await.unwrap();
        assert_eq!(captured.lock().await.as_deref(), Some("Bearer sk-test-key"));
    }

    #[tokio::test]
    async fn no_auth_header_when_key_absent() {
        let (addr, captured) = echo_auth_server().await;
        let a = OpenAiCompat::new(Client::new(), all_roles());
        let c = ProviderConn {
            endpoint: Some(format!("http://{addr}")),
            api_key: None,
            model_id: "m".into(),
            headers: vec![],
        };

        a.chat(&c, chat_req("hi")).await.unwrap();
        assert!(captured.lock().await.is_none());
    }

    #[tokio::test]
    async fn auth_header_sent_for_embed() {
        let (addr, captured) = echo_auth_server().await;
        let a = OpenAiCompat::new(Client::new(), all_roles());
        let c = ProviderConn {
            endpoint: Some(format!("http://{addr}")),
            api_key: Some("sk-embed".into()),
            model_id: "em".into(),
            headers: vec![],
        };

        a.embed(
            &c,
            EmbedReq {
                texts: vec!["t".into()],
            },
        )
        .await
        .unwrap();
        assert_eq!(captured.lock().await.as_deref(), Some("Bearer sk-embed"));
    }
}
