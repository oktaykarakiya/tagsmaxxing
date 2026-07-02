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

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use chrono::Utc;

    #[test]
    fn session_manager_new_succeeds() {
        let _sm = SessionManager::new();
    }

    #[test]
    fn session_manager_clone_independent() {
        let sm1 = SessionManager::new();
        let sm2 = sm1.clone();
        let _both = (sm1, sm2);
    }

    #[test]
    fn session_status_as_str() {
        assert_eq!(SessionStatus::Idle.as_str(), "idle");
        assert_eq!(SessionStatus::Running.as_str(), "running");
        assert_eq!(SessionStatus::Done.as_str(), "done");
        assert_eq!(SessionStatus::Killed.as_str(), "killed");
    }

    #[test]
    fn session_serialization_roundtrip() {
        let now = Utc::now();
        let session = AssistantSession {
            id: 42,
            tenant_id: 1,
            user_id: 7,
            session_key: "test-key".into(),
            model_ref: "local/qwen-35b".into(),
            department: Some("legal".into()),
            status: SessionStatus::Idle,
            sandbox_path: Some("/tmp/sandbox/test".into()),
            prompt_count: 3,
            total_tokens: 15000,
            created_at: now,
            finished_at: Some(now),
        };

        let json = serde_json::to_string(&session).expect("serialize");
        let roundtripped: AssistantSession = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(roundtripped.id, session.id);
        assert_eq!(roundtripped.tenant_id, session.tenant_id);
        assert_eq!(roundtripped.user_id, session.user_id);
        assert_eq!(roundtripped.session_key, session.session_key);
        assert_eq!(roundtripped.model_ref, session.model_ref);
        assert_eq!(roundtripped.department, session.department);
        assert_eq!(roundtripped.status, session.status);
        assert_eq!(roundtripped.sandbox_path, session.sandbox_path);
        assert_eq!(roundtripped.prompt_count, session.prompt_count);
        assert_eq!(roundtripped.total_tokens, session.total_tokens);
        assert_eq!(roundtripped.finished_at, session.finished_at);

        let json_lower = json.to_lowercase();
        assert!(json_lower.contains(r#""status":"idle""#));
    }
}
