// SPDX-License-Identifier: AGPL-3.0-or-later

//! Session state machine for assistant sessions.
//!
//! Each session maps to an `opencode --session` key. A session lock ensures
//! only one prompt executes at a time per session.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Lifecycle status of an assistant session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SessionStatus {
    /// Ready for a prompt.
    Idle,
    /// A prompt is currently executing.
    Running,
    /// Completed normally.
    Done,
    /// Killed by user or timeout.
    Killed,
}

impl SessionStatus {
    /// Wire string for DB storage.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Running => "running",
            Self::Done => "done",
            Self::Killed => "killed",
        }
    }
}

/// A persisted assistant session (maps to `assistant_sessions` table).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssistantSession {
    /// Surrogate primary key.
    pub id: i64,
    /// Owning tenant.
    pub tenant_id: i64,
    /// Owning user.
    pub user_id: i64,
    /// The `opencode --session` key.
    pub session_key: String,
    /// Model reference string.
    pub model_ref: String,
    /// Optional department scope.
    pub department: Option<String>,
    /// Current lifecycle status.
    pub status: SessionStatus,
    /// Filesystem path to the sandbox workspace, if allocated.
    pub sandbox_path: Option<String>,
    /// Number of prompts executed in this session.
    pub prompt_count: i32,
    /// Cumulative token usage estimate.
    pub total_tokens: i64,
    /// When the session was created.
    pub created_at: DateTime<Utc>,
    /// When the session ended (status Done or Killed).
    pub finished_at: Option<DateTime<Utc>>,
}

/// Session manager — provides session-scoped mutual exclusion.
///
/// One [`SessionManager`] per app instance. Uses tokio Mutex per session key
/// to serialise prompts within a session while allowing concurrent execution
/// across sessions.
pub struct SessionManager {
    // TODO: tokio::sync::Mutex map by session_key
    _priv: (),
}

impl Clone for SessionManager {
    fn clone(&self) -> Self {
        Self { _priv: () }
    }
}

impl SessionManager {
    /// Create a new empty session manager.
    #[must_use]
    pub fn new() -> Self {
        Self { _priv: () }
    }
}

impl Default for SessionManager {
    fn default() -> Self {
        Self::new()
    }
}
