// SPDX-License-Identifier: AGPL-3.0-or-later

//! JSON API handlers for chat conversations (P18-T7).

use std::sync::Arc;

use axum::Extension;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::Json;
use serde::Serialize;

use crate::AppState;
use crate::AuthUser;

/// Error response for chat API endpoints.
#[derive(Debug, Serialize)]
pub struct ChatError {
    /// Machine-readable error code.
    error: String,
    /// Human-readable error message.
    message: String,
}

#[derive(Debug, Serialize)]
struct ConversationResponse {
    id: i64,
    title: Option<String>,
    model_ref: String,
    message_count: i32,
    created_at: String,
    updated_at: String,
}

#[derive(Debug, Serialize)]
struct MessageResponse {
    id: i64,
    role: String,
    content: String,
    tokens_in: Option<i32>,
    tokens_out: Option<i32>,
    created_at: String,
}

fn internal_error(e: impl std::fmt::Display) -> (StatusCode, Json<ChatError>) {
    tracing::error!(error=%e, "chat API error");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ChatError {
            error: "internal_error".into(),
            message: "an unexpected error occurred".into(),
        }),
    )
}

fn not_found() -> (StatusCode, Json<ChatError>) {
    (
        StatusCode::NOT_FOUND,
        Json(ChatError {
            error: "not_found".into(),
            message: "conversation not found".into(),
        }),
    )
}

/// `GET /api/chat/conversations` — list user's conversations
pub async fn list_conversations(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ChatError>)> {
    let convs = state
        .pg_store
        .list_conversations(auth_user.tenant_id, auth_user.user_id, 50)
        .await
        .map_err(internal_error)?;

    let items: Vec<ConversationResponse> = convs
        .into_iter()
        .map(|c| ConversationResponse {
            id: c.id,
            title: c.title,
            model_ref: c.model_ref,
            message_count: c.message_count,
            created_at: c.created_at.to_rfc3339(),
            updated_at: c.updated_at.to_rfc3339(),
        })
        .collect();

    Ok(Json(serde_json::json!({ "conversations": items })))
}

/// `GET /api/chat/conversations/{id}` — get conversation with messages
pub async fn get_conversation(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Path(id): Path<i64>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ChatError>)> {
    let conv = state
        .pg_store
        .get_conversation(auth_user.tenant_id, id)
        .await
        .map_err(internal_error)?
        .ok_or_else(not_found)?;

    let msgs = state
        .pg_store
        .get_messages(auth_user.tenant_id, id, i64::MAX, 200)
        .await
        .map_err(internal_error)?;

    let messages: Vec<MessageResponse> = msgs
        .into_iter()
        .map(|m| MessageResponse {
            id: m.id,
            role: m.role,
            content: m.content,
            tokens_in: m.tokens_in,
            tokens_out: m.tokens_out,
            created_at: m.created_at.to_rfc3339(),
        })
        .collect();

    Ok(Json(serde_json::json!({
        "id": conv.id,
        "title": conv.title,
        "model_ref": conv.model_ref,
        "message_count": conv.message_count,
        "created_at": conv.created_at.to_rfc3339(),
        "updated_at": conv.updated_at.to_rfc3339(),
        "messages": messages,
    })))
}

/// `DELETE /api/chat/conversations/{id}` — delete a conversation
pub async fn delete_conversation(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Path(id): Path<i64>,
) -> Result<Json<serde_json::Value>, (StatusCode, Json<ChatError>)> {
    state
        .pg_store
        .delete_conversation(auth_user.tenant_id, id)
        .await
        .map_err(internal_error)?;

    Ok(Json(serde_json::json!({ "ok": true })))
}
