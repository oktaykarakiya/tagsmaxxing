// SPDX-License-Identifier: AGPL-3.0-or-later

//! Web UI chat page handlers (P18).
//!
//! * `GET /chat` — main chat page with conversation list.
//! * `GET /chat/{id}` — open a specific conversation.
//! * `POST /chat/new` — create a new conversation, redirect.
//! * `POST /chat/{id}/send` — SSE streaming chat response.
//! * `POST /chat/{id}/delete` — delete a conversation (CSRF).
//! * `GET /chat/{id}/messages` — HTMX partial for message list.

use std::sync::Arc;

use crate::AuthUser;
use askama::Template;
use axum::Extension;
use axum::Form;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response, Sse};
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::AppState;
use crate::web::csrf;

// ── Form types ────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct SendMessageForm {
    pub csrf_token: String,
    pub message: String,
}

#[derive(Debug, Deserialize)]
pub struct DeleteChatForm {
    pub csrf_token: String,
}

// ── Template types ────────────────────────────────────────────────────────

#[derive(Debug, Template)]
#[template(path = "chat.html")]
struct ChatPage {
    csrf_token: String,
    conv_id: i64,
    conversations: Vec<ChatConvEntry>,
    messages: Vec<ChatMsgEntry>,
}

#[derive(Debug, Clone)]
struct ChatConvEntry {
    id: i64,
    title: String,
    updated_at: String,
    message_count: i32,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct ChatMsgEntry {
    id: i64,
    role: String,
    content: String,
    created_at: String,
    search_results_json: Option<String>,
}

#[derive(Debug, Template)]
#[template(path = "chat_messages.html")]
struct ChatMessagesPartial {
    messages: Vec<ChatMsgEntry>,
}

// ── Helpers ────────────────────────────────────────────────────────────────

fn render_ok<T: Template>(t: &T) -> Response {
    match t.render() {
        Ok(html) => (StatusCode::OK, Html(html)).into_response(),
        Err(e) => {
            tracing::error!(error=%e, "chat template render failure");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Html(String::from("Internal error")),
            )
                .into_response()
        }
    }
}

fn format_time(t: chrono::DateTime<chrono::Utc>) -> String {
    t.format("%b %d, %H:%M").to_string()
}

/// Build a chat page from the database.
async fn build_chat_page(
    state: &AppState,
    tenant_id: i64,
    user_id: i64,
    conv_id: Option<i64>,
    csrf_token: &str,
    _error: &str,
) -> Response {
    let _model_ref = state
        .app_config
        .as_ref()
        .and_then(|c| {
            let m = &c.current().chat.model;
            if m.is_empty() { None } else { Some(m.clone()) }
        })
        .unwrap_or_else(|| "local/default".to_string());

    let _enabled = state
        .app_config
        .as_ref()
        .map(|c| c.current().chat.enabled)
        .unwrap_or(true);

    // Load conversation list.
    let convs = state
        .pg_store
        .list_conversations(tenant_id, user_id, 50)
        .await
        .unwrap_or_default();

    let conversations: Vec<ChatConvEntry> = convs
        .into_iter()
        .map(|c| ChatConvEntry {
            id: c.id,
            title: c.title.unwrap_or_else(|| "New conversation".into()),
            updated_at: format_time(c.updated_at),
            message_count: c.message_count,
        })
        .collect();

    // Load messages for the active conversation.
    let messages: Vec<ChatMsgEntry> = if let Some(cid) = conv_id {
        state
            .pg_store
            .get_messages(tenant_id, cid, i64::MAX, 100)
            .await
            .unwrap_or_default()
            .into_iter()
            .map(|m| ChatMsgEntry {
                id: m.id,
                role: m.role,
                content: m.content,
                created_at: format_time(m.created_at),
                search_results_json: m.search_results_json.map(|j| j.to_string()),
            })
            .collect()
    } else {
        vec![]
    };

    let page = ChatPage {
        csrf_token: csrf_token.to_string(),
        conv_id: conv_id.unwrap_or(0),
        conversations,
        messages,
    };

    // TODO: pass error to template for user feedback.
    render_ok(&page)
}

// ── Handlers ──────────────────────────────────────────────────────────────

/// `GET /chat` — main chat page (no active conversation).
pub async fn chat_page(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
) -> Response {
    let csrf = csrf::generate_csrf_token().unwrap_or_default();
    let mut resp = build_chat_page(
        &state,
        auth_user.tenant_id,
        auth_user.user_id,
        None,
        &csrf,
        "",
    )
    .await;
    resp.headers_mut().insert(
        axum::http::header::SET_COOKIE,
        csrf::csrf_cookie_value(&csrf, state.session_ttl, state.secure_cookies),
    );
    resp
}

/// `GET /chat/{id}` — main chat page with an active conversation.
pub async fn chat_page_with_id(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Path(conv_id): Path<i64>,
) -> Response {
    let csrf = csrf::generate_csrf_token().unwrap_or_default();
    let mut resp = build_chat_page(
        &state,
        auth_user.tenant_id,
        auth_user.user_id,
        Some(conv_id),
        &csrf,
        "",
    )
    .await;
    resp.headers_mut().insert(
        axum::http::header::SET_COOKIE,
        csrf::csrf_cookie_value(&csrf, state.session_ttl, state.secure_cookies),
    );
    resp
}

/// `POST /chat/new` — create a new conversation.
pub async fn chat_new(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    headers: axum::http::HeaderMap,
    Form(form): Form<NewChatForm>,
) -> Response {
    if csrf::validate_csrf(&headers, &form.csrf_token).is_err() {
        let csrf = csrf::generate_csrf_token().unwrap_or_default();
        let mut resp = (
            StatusCode::FORBIDDEN,
            Html(String::from("CSRF validation failed")),
        )
            .into_response();
        resp.headers_mut().insert(
            axum::http::header::SET_COOKIE,
            csrf::csrf_cookie_value(&csrf, state.session_ttl, state.secure_cookies),
        );
        return resp;
    }

    let model = state
        .app_config
        .as_ref()
        .and_then(|c| {
            let m = &c.current().chat.model;
            if m.is_empty() { None } else { Some(m.clone()) }
        })
        .unwrap_or_else(|| "local/default".to_string());

    match state
        .pg_store
        .create_conversation(auth_user.tenant_id, auth_user.user_id, &model)
        .await
    {
        Ok(conv) => {
            let csrf = csrf::generate_csrf_token().unwrap_or_default();
            let mut resp =
                axum::response::Redirect::to(&format!("/chat/{}", conv.id)).into_response();
            resp.headers_mut().insert(
                axum::http::header::SET_COOKIE,
                csrf::csrf_cookie_value(&csrf, state.session_ttl, state.secure_cookies),
            );
            resp
        }
        Err(e) => {
            tracing::error!(error=%e, "failed to create conversation");
            let csrf = csrf::generate_csrf_token().unwrap_or_default();
            let mut resp = build_chat_page(
                &state,
                auth_user.tenant_id,
                auth_user.user_id,
                None,
                &csrf,
                "Failed to create conversation",
            )
            .await;
            resp.headers_mut().insert(
                axum::http::header::SET_COOKIE,
                csrf::csrf_cookie_value(&csrf, state.session_ttl, state.secure_cookies),
            );
            resp
        }
    }
}

/// `POST /chat/{id}/send` — SSE streaming response.
pub async fn chat_send(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Path(conv_id): Path<i64>,
    headers: axum::http::HeaderMap,
    Form(form): Form<SendMessageForm>,
) -> Response {
    if csrf::validate_csrf(&headers, &form.csrf_token).is_err() {
        return (StatusCode::FORBIDDEN, "CSRF failed").into_response();
    }

    let message = form.message.trim().to_string();
    if message.is_empty() {
        return (StatusCode::BAD_REQUEST, "message empty").into_response();
    }

    let (tx, rx) = mpsc::channel::<Result<axum::response::sse::Event, std::convert::Infallible>>(8);

    let tenant_id = auth_user.tenant_id;
    let _user_id = auth_user.user_id;
    let pg = state.pg_store.clone();
    let retrieval = state.retrieval_pipeline.clone();
    let client = state.backend_pool.clone();
    let app_config = state.app_config.clone();

    let model = app_config
        .as_ref()
        .and_then(|c| {
            let m = &c.current().chat.model;
            if m.is_empty() { None } else { Some(m.clone()) }
        })
        .unwrap_or_else(|| "local/default".to_string());
    let max_history = app_config
        .as_ref()
        .map(|c| c.current().chat.max_history_messages)
        .unwrap_or(20);
    let max_rag = app_config
        .as_ref()
        .map(|c| c.current().chat.max_rag_docs)
        .unwrap_or(5);

    let local_only = crate::handlers::resolve_local_only(&state.pg_store, tenant_id).await;

    tokio::spawn(async move {
        use axum::response::sse::Event;

        // 1. Save user message.
        let _ = pg
            .insert_message(tenant_id, conv_id, "user", &message, None, None, None)
            .await;

        // Auto-title: set from first message if conversation has no title.
        let conv = pg.get_conversation(tenant_id, conv_id).await.ok().flatten();
        if let Some(ref c) = conv
            && c.title.is_none()
        {
            let title: String = message.chars().take(80).collect();
            let _ = pg
                .update_conversation_title(tenant_id, conv_id, &title)
                .await;
        }

        // 2. RAG + LLM: use retrieval pipeline + scheduler pool directly.
        if let (Some(retrieval), Some(pool)) = (&retrieval, &client) {
            // RAG retrieval.
            let query = kb_core::query::Query {
                text: message.clone(),
                filters: kb_core::query::QueryFilters {
                    kinds: vec![],
                    tags: vec![],
                    created_after: None,
                    created_before: None,
                },
                top_k: max_rag,
            };
            let search_results: Vec<kb_core::query::Hit> = retrieval
                .retrieve(tenant_id, None, &query, local_only, false)
                .await
                .map(|(hits, _mode)| hits)
                .unwrap_or_default();

            // Send sources event.
            if !search_results.is_empty() {
                let sources_json = serde_json::json!({
                    "count": search_results.len(),
                    "docs": search_results.iter().map(|h| serde_json::json!({
                        "title": h.title, "snippet": h.snippet, "document_id": h.document_id,
                    })).collect::<Vec<_>>(),
                });
                tx.send(Ok(Event::default()
                    .event("sources")
                    .data(sources_json.to_string())))
                    .await
                    .ok();
            }

            // Load history.
            let history = pg
                .get_recent_messages(tenant_id, conv_id, max_history as i64)
                .await
                .unwrap_or_default();

            // Build ChatReq.
            let mut messages: Vec<kb_core::provider::ChatMessage> = Vec::new();
            let system_content = if search_results.is_empty() {
                "You are a helpful knowledge-base assistant. You have access to the user's document library. Be concise and accurate.".to_string()
            } else {
                let docs: Vec<String> = search_results
                    .iter()
                    .map(|h| {
                        let title = h.title.as_deref().unwrap_or("Untitled");
                        format!("- {title}: {}", h.snippet)
                    })
                    .collect();
                format!(
                    "You are a helpful knowledge-base assistant. You have access to the user's document library. When answering, use the provided context if relevant. If not relevant, answer from general knowledge. Be concise and accurate.\n\nRELEVANT DOCUMENTS:\n{}",
                    docs.join("\n")
                )
            };
            messages.push(kb_core::provider::ChatMessage {
                role: kb_core::provider::ChatRole::System,
                content: system_content,
            });
            for msg in &history {
                let role = match msg.role.as_str() {
                    "user" => kb_core::provider::ChatRole::User,
                    "assistant" => kb_core::provider::ChatRole::Assistant,
                    _ => continue,
                };
                messages.push(kb_core::provider::ChatMessage {
                    role,
                    content: msg.content.clone(),
                });
            }
            messages.push(kb_core::provider::ChatMessage {
                role: kb_core::provider::ChatRole::User,
                content: message.clone(),
            });

            let req = kb_core::provider::ChatReq {
                messages,
                max_tokens: Some(1024),
                ..Default::default()
            };

            // LLM call: construct a temporary LlamaClient from the pool.
            // (In production, the LlamaClient should be shared via AppState
            // rather than created per-request.)
            let client = kb_llm::LlamaClient::new(
                (**pool).clone(),
                reqwest::Client::new(),
                2,
                5,
                std::time::Duration::from_secs(30),
            );
            match client
                .chat(kb_core::role::Role::Text, &model, &req, local_only, 0)
                .await
            {
                Ok(resp) => {
                    let response_json = serde_json::json!({"content": resp.text, "tokens": {"in": resp.usage.prompt_tokens, "out": resp.usage.completion_tokens}});
                    tx.send(Ok(Event::default()
                        .event("response")
                        .data(response_json.to_string())))
                        .await
                        .ok();

                    let _ = pg
                        .insert_message(
                            tenant_id,
                            conv_id,
                            "assistant",
                            &resp.text,
                            Some(resp.usage.prompt_tokens as i32),
                            Some(resp.usage.completion_tokens as i32),
                            Some(&serde_json::to_value(&search_results).unwrap_or_default()),
                        )
                        .await;

                    let done_json = serde_json::json!({"conversation_id": conv_id, "message_count": conv.map(|c| c.message_count + 2).unwrap_or(2)});
                    tx.send(Ok(Event::default()
                        .event("done")
                        .data(done_json.to_string())))
                        .await
                        .ok();
                }
                Err(e) => {
                    tracing::error!(error=%e, "chat LLM failed");
                    tx.send(Ok(Event::default()
                        .event("error")
                        .data(format!("{{\"message\":\"{}\"}}", e))))
                        .await
                        .ok();
                }
            }
        } else {
            tx.send(Ok(Event::default().event("error").data("{\"message\":\"Chat backend not configured — retrieval pipeline or backend pool missing\"}"))).await.ok();
        }
    });

    Sse::new(ReceiverStream::new(rx)).into_response()
}

/// `POST /chat/{id}/delete` — delete a conversation (CSRF-protected).
pub async fn chat_delete(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Path(conv_id): Path<i64>,
    headers: axum::http::HeaderMap,
    Form(form): Form<DeleteChatForm>,
) -> Response {
    if csrf::validate_csrf(&headers, &form.csrf_token).is_err() {
        let csrf = csrf::generate_csrf_token().unwrap_or_default();
        let mut resp = (StatusCode::FORBIDDEN, Html(String::from("CSRF failed"))).into_response();
        resp.headers_mut().insert(
            axum::http::header::SET_COOKIE,
            csrf::csrf_cookie_value(&csrf, state.session_ttl, state.secure_cookies),
        );
        return resp;
    }

    let _ = state
        .pg_store
        .delete_conversation(auth_user.tenant_id, conv_id)
        .await;

    let csrf = csrf::generate_csrf_token().unwrap_or_default();
    let mut resp = axum::response::Redirect::to("/chat").into_response();
    resp.headers_mut().insert(
        axum::http::header::SET_COOKIE,
        csrf::csrf_cookie_value(&csrf, state.session_ttl, state.secure_cookies),
    );
    resp
}

/// `GET /chat/{id}/messages` — HTMX partial for message list.
pub async fn chat_messages(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Path(conv_id): Path<i64>,
) -> Response {
    let messages: Vec<ChatMsgEntry> = state
        .pg_store
        .get_messages(auth_user.tenant_id, conv_id, i64::MAX, 200)
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|m| ChatMsgEntry {
            id: m.id,
            role: m.role,
            content: m.content,
            created_at: format_time(m.created_at),
            search_results_json: m.search_results_json.map(|j| j.to_string()),
        })
        .collect();

    let partial = ChatMessagesPartial { messages };
    match partial.render() {
        Ok(html) => (StatusCode::OK, Html(html)).into_response(),
        Err(e) => {
            tracing::error!(error=%e, "chat messages render failure");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Html(String::from("Error")),
            )
                .into_response()
        }
    }
}

/// A minimal form for creating a new chat.
#[derive(Debug, Deserialize)]
pub struct NewChatForm {
    /// CSRF token.
    pub csrf_token: String,
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    #[test]
    fn format_time_returns_string() {
        let now = chrono::Utc::now();
        let s = format_time(now);
        assert!(!s.is_empty());
    }

    #[test]
    fn chat_msg_entry_has_fields() {
        let e = ChatMsgEntry {
            id: 1,
            role: "user".into(),
            content: "hi".into(),
            created_at: "now".into(),
            search_results_json: None,
        };
        assert_eq!(e.role, "user");
    }
}
