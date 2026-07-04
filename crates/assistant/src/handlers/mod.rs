// SPDX-License-Identifier: AGPL-3.0-or-later

//! axum route handlers and router builder for the AI assistant.
//!
//! Exposes [`build_assistant_router`] which always returns a router.
//! When `opencode_bin` is configured, the full agent is available (SSE streaming).
//! When it is not configured, a basic Q&A interface is shown.

use std::sync::Arc;

use axum::Extension;
use axum::Router;
use axum::routing::{get, post};
use kb_pipeline::IngestPipeline;
use kb_pipeline::RetrievalPipeline;
use kb_store::PgStore;

use crate::config_ext::AssistantConfig;
use crate::executor::Executor;
use crate::prompt::PromptBuilder;
use crate::session::SessionManager;

mod prompt;
pub(crate) mod upload;

/// Shared state for all assistant routes.
#[derive(Clone)]
pub struct AssistantState {
    /// The parsed assistant config section.
    pub cfg: AssistantConfig,
    /// Postgres store for session, action item, and transcript persistence.
    pub store: Arc<PgStore>,
    /// Hybrid search pipeline for context-enriched prompts.
    pub pipeline: Arc<RetrievalPipeline>,
    /// Ingestion pipeline for file uploads.
    pub ingest: Option<Arc<IngestPipeline>>,
    /// OpenCode CLI subprocess executor (None if disabled).
    pub executor: Option<Executor>,
    /// Prompt builder for context augmentation.
    pub prompt_builder: PromptBuilder,
    /// Per-session mutual exclusion manager.
    pub sessions: SessionManager,
}

/// Build the assistant sub-router.
///
/// Always returns a router. When `opencode_bin` is unconfigured, the assistant
/// page shows a basic Q&A interface. When configured, full agent SSE streaming
/// is available at `POST /assistant/prompt`.
pub fn build_assistant_router(
    store: &Arc<PgStore>,
    pipeline: &Option<Arc<RetrievalPipeline>>,
    ingest: &Option<Arc<IngestPipeline>>,
    asst_cfg: AssistantConfig,
) -> Router {
    let executor = asst_cfg.opencode_bin.as_ref().map(|bin| {
        Executor::new(
            std::path::Path::new(bin),
            &asst_cfg.model_ref,
            asst_cfg.prompt_timeout_secs,
        )
    });

    let prompt_builder = PromptBuilder::new(asst_cfg.context_budget_pct);

    let pipeline = match pipeline.as_ref() {
        Some(p) => Arc::clone(p),
        None => {
            tracing::warn!("retrieval pipeline not configured — assistant disabled");
            return Router::new();
        }
    };

    let state = Arc::new(AssistantState {
        cfg: asst_cfg,
        store: Arc::clone(store),
        pipeline,
        ingest: ingest.clone(),
        executor,
        prompt_builder,
        sessions: SessionManager::new(),
    });

    Router::new()
        .route("/", get(prompt::assistant_page))
        .route("/prompt", post(prompt::assistant_prompt_handler))
        .route("/upload", post(upload::upload_handler))
        .route("/sessions", get(prompt::assistant_sessions_handler))
        .layer(Extension(state))
}
