// SPDX-License-Identifier: AGPL-3.0-or-later

//! Scheduler acquisition errors (plan §6.3).

use std::time::Duration;

use kb_core::role::Role;
use thiserror::Error;

/// Why a slot could not be acquired from the [`crate::Pool`].
#[derive(Debug, Error)]
pub enum AcquireError {
    /// No healthy backend serves the requested role (plan §6.3, step 1).
    #[error("no healthy backend serves role `{role}`")]
    NoBackend {
        /// The role that had no healthy candidate.
        role: Role,
    },
    /// Every candidate slot stayed busy until the acquire timeout elapsed
    /// (plan §6.3, the bounded wait path).
    #[error("timed out after {0:?} waiting for a free slot")]
    Timeout(Duration),
    /// A backend semaphore was closed — the pool is shutting down.
    #[error("scheduler pool is closed")]
    Closed,
    /// All routing tiers were exhausted without acquiring capacity
    /// (plan §26.4, tiered failover final step).
    #[error("capacity exhausted for role `{role}` after {tiers_attempted} tiers")]
    CapacityExhausted {
        /// The role that had no available capacity.
        role: Role,
        /// How many tiers were attempted before giving up.
        tiers_attempted: usize,
    },
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    #[test]
    fn no_backend_names_the_role() {
        let msg = AcquireError::NoBackend { role: Role::Embed }.to_string();
        assert!(msg.contains("embed"), "{msg}");
        assert!(msg.contains("no healthy backend"), "{msg}");
    }

    #[test]
    fn timeout_shows_the_duration() {
        let msg = AcquireError::Timeout(Duration::from_secs(5)).to_string();
        assert!(msg.contains("timed out"), "{msg}");
        assert!(msg.contains("5s"), "{msg}");
    }

    #[test]
    fn closed_has_a_stable_message() {
        assert_eq!(AcquireError::Closed.to_string(), "scheduler pool is closed");
    }

    #[test]
    fn capacity_exhausted_names_role_and_tiers() {
        let msg = AcquireError::CapacityExhausted {
            role: Role::Text,
            tiers_attempted: 3,
        }
        .to_string();
        assert!(msg.contains("text"), "{msg}");
        assert!(msg.contains("3 tiers"), "{msg}");
        assert!(msg.contains("capacity exhausted"), "{msg}");
    }
}
