// SPDX-License-Identifier: AGPL-3.0-or-later

//! Prompt handler — SSE-streamed responses and page rendering.

use std::sync::Arc;

use askama::Template;
use axum::Extension;
use axum::Form;
use axum::response::sse::{Event, Sse};
use axum::response::{Html, IntoResponse};
use serde::Deserialize;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;

use super::AssistantState;

// ── SSE helpers ───────────────────────────────────────────────────────────────

/// Shorthand for a single-element SSE stream.
fn sse_single(msg: String) -> Sse<impl tokio_stream::Stream<Item = Result<Event, anyhow::Error>>> {
    Sse::new(tokio_stream::iter(vec![Ok(Event::default().data(msg))]))
}

// ── Templates ────────────────────────────────────────────────────────────────

/// Main assistant dashboard page.
#[derive(Template)]
#[template(path = "assistant.html")]
pub struct AssistantPage {
    #[allow(dead_code)]
    pub model_ref: String,
}

/// Session list fragment (HTMX partial).
#[derive(Template)]
#[template(path = "assistant_session.html")]
pub struct AssistantSessionsPage {
    pub session_count: usize,
}

// ── Request types ────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct PromptForm {
    pub text: String,
    #[serde(default)]
    pub department: Option<String>,
    #[serde(default)]
    pub session_key: Option<String>,
}

// ── Handlers ─────────────────────────────────────────────────────────────────

pub async fn assistant_page(Extension(state): Extension<Arc<AssistantState>>) -> impl IntoResponse {
    // Use the same template as the main dashboard
    let template = AssistantPage {
        model_ref: state.cfg.model_ref.clone(),
    };
    Html(
        template
            .render()
            .unwrap_or_else(|e| format!("Template error: {e}")),
    )
}

pub async fn assistant_prompt_handler(
    Extension(state): Extension<Arc<AssistantState>>,
    Form(form): Form<PromptForm>,
) -> impl IntoResponse {
    if state.executor.is_none() {
        return sse_single("Assistant is disabled: opencode_bin not configured".to_string())
            .into_response();
    }

    let executor = match &state.executor {
        Some(e) => e,
        None => {
            return sse_single("Assistant is disabled: opencode_bin not configured".to_string())
                .into_response();
        }
    };

    let session_key = form
        .session_key
        .unwrap_or_else(|| format!("web-{}", uuid::Uuid::new_v4()));
    let work_dir = std::env::temp_dir().join(format!("asst-{session_key}"));
    let _ = std::fs::create_dir_all(&work_dir);

    let mut env = std::collections::HashMap::new();
    if let Some(ref dept) = form.department {
        env.insert("OPC_DEPARTMENT".into(), dept.clone());
    }

    match executor
        .execute(&work_dir, &session_key, &form.text, env)
        .await
    {
        Ok(rx) => {
            let stream = ReceiverStream::new(rx)
                .map(|line| Ok::<_, anyhow::Error>(Event::default().data(line)));
            Sse::new(stream).into_response()
        }
        Err(e) => sse_single(format!("Failed to start agent: {e}")).into_response(),
    }
}

pub async fn assistant_sessions_handler(
    Extension(_state): Extension<Arc<AssistantState>>,
) -> impl IntoResponse {
    let template = AssistantSessionsPage { session_count: 0 };
    Html(
        template
            .render()
            .unwrap_or_else(|e| format!("Template error: {e}")),
    )
}
