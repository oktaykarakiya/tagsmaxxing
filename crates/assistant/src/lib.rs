// SPDX-License-Identifier: AGPL-3.0-or-later

//! `kb-assistant`: AI assistant agent orchestration.
//!
//! Wraps the `opencode` CLI binary with sandboxed workspace isolation, streaming
//! output, action-item tracking, post-prompt validation, and scheduled background
//! maintenance (consolidation, pruning, stale-watch checks).
//!
//! # Architecture
//!
//! - [`Executor`] spawns `opencode run` as a subprocess and pipes stripped stdout
//!   through a tokio channel.
//! - [`Sandbox`] creates an isolated workspace under `/tmp`, copies safe files, and
//!   atomically syncs validated output back.
//! - [`PromptBuilder`] enriches user input with relevant past documents (via the
//!   existing hybrid search pipeline), pending action items, and thread continuity.
//! - [`ActionTracker`] detects explicit reminders/tasks/decisions from prompt text
//!   via regex and persists them to Postgres.
//! - [`Analyzer`] scans the agent's output for missed memory-block updates, naming
//!   violations, and quality signals.
//! - Background jobs (consolidation, pruning, stale-watch) plug into
//!   `kb_pipeline::job_queue`.
//!
//! # Feature gate
//!
//! When the `opencode_bin` config field is unset, the assistant is dormant.

pub mod action_tracker;
pub mod analyzer;
pub mod config_ext;
pub mod executor;
pub mod prompt;
pub mod sandbox;
pub mod scheduler_jobs;
pub mod session;
pub mod stale_watch;
pub mod taxonomy;

pub mod handlers;

/// Crate-level result.
pub type Result<T> = std::result::Result<T, anyhow::Error>;

/// Companion error type carrying a structured label.
#[allow(dead_code)]
#[derive(Debug, thiserror::Error)]
pub(crate) enum AsstError {
    #[error("assistant disabled: opencode_bin not configured")]
    Disabled,

    #[error("session: {0}")]
    Session(String),

    #[error("sandbox: {0}")]
    Sandbox(String),

    #[error("executor: {0}")]
    Executor(String),
}
