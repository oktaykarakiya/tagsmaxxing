// SPDX-License-Identifier: AGPL-3.0-or-later

//! axum route handlers and router builder for the AI assistant.
//!
//! Exposes [`build_assistant_router`] which returns a unit-state `Router`
//! ready for `nest_service` mounting.  The [`AssistantState`] is injected
//! via an `Extension` layer so the callers extract it with
//! `Extension<Arc<AssistantState>>`.

use std::sync::Arc;

use axum::Extension;
use axum::Router;
use axum::routing::{get, post};
use kb_pipeline::RetrievalPipeline;
use kb_store::PgStore;

use crate::config_ext::AssistantConfig;
use crate::executor::Executor;
use crate::prompt::PromptBuilder;
use crate::session::SessionManager;

mod prompt;

/// Shared state for all assistant routes.
#[derive(Clone)]
pub struct AssistantState {
    /// The parsed assistant config section.
    pub cfg: AssistantConfig,
    /// Postgres store for session, action item, and transcript persistence.
    pub store: Arc<PgStore>,
    /// Hybrid search pipeline for context-enriched prompts.
    pub pipeline: Arc<RetrievalPipeline>,
    /// OpenCode CLI subprocess executor (None if disabled).
    pub executor: Option<Executor>,
    /// Prompt builder for context augmentation.
    pub prompt_builder: PromptBuilder,
    /// Per-session mutual exclusion manager.
    pub sessions: SessionManager,
}

/// Build the assistant sub-router.
///
/// Uses unit state `()` so it can be mounted via `nest_service`. State is
/// passed through an `Extension<Arc<AssistantState>>` layer.
///
/// Returns `None` when the assistant is disabled.
#[must_use]
pub fn build_assistant_router(
    store: &Arc<PgStore>,
    pipeline: &Option<Arc<RetrievalPipeline>>,
) -> Option<Router> {
    let pipeline = pipeline.as_ref()?;
    let asst_cfg = AssistantConfig::default();
    let opencode_bin = asst_cfg.opencode_bin.as_ref()?;

    let executor = Executor::new(
        std::path::Path::new(opencode_bin),
        &asst_cfg.model_ref,
        asst_cfg.prompt_timeout_secs,
    );
    let prompt_builder = PromptBuilder::new(asst_cfg.context_budget_pct);

    let state = Arc::new(AssistantState {
        cfg: asst_cfg,
        store: Arc::clone(store),
        pipeline: Arc::clone(pipeline),
        executor: Some(executor),
        prompt_builder,
        sessions: SessionManager::new(),
    });

    let router = Router::new()
        .route("/", get(prompt::assistant_page))
        .route("/prompt", post(prompt::assistant_prompt_handler))
        .route("/sessions", get(prompt::assistant_sessions_handler))
        .layer(Extension(state));

    Some(router)
}
