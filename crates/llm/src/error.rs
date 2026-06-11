// SPDX-License-Identifier: AGPL-3.0-or-later

//! Error types for the `kb-llm` crate.

use std::time::Duration;

use kb_scheduler::AcquireError;
use thiserror::Error;

/// Errors the [`crate::LlamaClient`] can produce.
///
/// Covers scheduler acquisition failures (propagated from the pool), exhaustive
/// retry exhaustion, and HTTP-level errors.
#[derive(Debug, Error)]
pub enum LlmError {
    /// The scheduler could not provide a slot — no healthy backend serves the
    /// requested role, or the acquire timed out, or the pool is closed.
    #[error("scheduler: {0}")]
    Scheduler(#[from] AcquireError),

    /// Every candidate backend failed across all retry attempts.
    ///
    /// `last_error` carries the message from the final failed attempt.
    #[error("all {retries} backend(s) failed; last error: {last_error}")]
    AllFailed {
        /// How many retries were attempted (at least 1).
        retries: u32,
        /// Error message from the last failed attempt.
        last_error: String,
    },

    /// An HTTP transport error (connection refused, DNS, TLS, timeout, …).
    #[error("http: {0}")]
    Http(#[from] reqwest::Error),

    /// The backend returned a successful HTTP status but the JSON body could not
    /// be deserialized into the expected shape.
    #[error("deserialize: {0}")]
    Deserialize(String),

    /// Cooldown: every backend serving the role is currently in circuit-breaker
    /// cooldown and may not be used.
    #[error("all backends for this role are in cooldown (retry after {0:?})")]
    AllCooldown(Duration),
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use kb_core::role::Role;

    #[test]
    fn scheduler_error_displays_acquire_message() {
        let err = LlmError::Scheduler(AcquireError::NoBackend { role: Role::Embed });
        let msg = err.to_string();
        assert!(msg.contains("scheduler"), "{msg}");
        assert!(msg.contains("embed"), "{msg}");
    }

    #[test]
    fn all_failed_reports_retry_count_and_last_error() {
        let err = LlmError::AllFailed {
            retries: 3,
            last_error: "connection refused".into(),
        };
        let msg = err.to_string();
        assert!(msg.contains("3"), "{msg}");
        assert!(msg.contains("connection refused"), "{msg}");
    }

    #[test]
    fn all_cooldown_shows_duration() {
        let err = LlmError::AllCooldown(Duration::from_secs(30));
        let msg = err.to_string();
        assert!(msg.contains("cooldown"), "{msg}");
        assert!(msg.contains("30s"), "{msg}");
    }

    #[test]
    fn deserialize_error_displays_message() {
        let err = LlmError::Deserialize("missing field `title`".into());
        let msg = err.to_string();
        assert!(msg.contains("deserialize"), "{msg}");
        assert!(msg.contains("missing field"), "{msg}");
    }

    /// Verify `LlmError` impls `Send + Sync` (required by anyhow/async boundaries).
    #[test]
    fn error_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<LlmError>();
    }
}
