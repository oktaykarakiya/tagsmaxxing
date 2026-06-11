// SPDX-License-Identifier: AGPL-3.0-or-later

//! Aggregated degradation status for all subsystems (plan §22).
//!
//! Each flag is an [`AtomicBool`] for lock-free reads across axum worker tasks.
//! Components set flags when they detect a dependency is unhealthy; the API
//! layer reads them to decide whether to fast-fail, serve degraded, or inject
//! the `X-Degraded` response header.

use std::sync::atomic::{AtomicBool, Ordering};

/// Tracks which subsystems of the knowledge base are currently degraded.
///
/// All reads are lock-free via [`Ordering::Acquire`]; writes use
/// [`Ordering::Release`]. The struct is `Send + Sync` and cheap to
/// [`Clone`] via [`Arc`](std::sync::Arc).
///
/// # Lifecycle
///
/// - **Backend health** (P1-T4): the periodic health loop sets per-role
///   degraded flags when a role has zero healthy backends.
/// - **Blob store** (P8-T1): a periodic B2 health probe (or the circuit
///   breaker tripping) sets [`blob_store_degraded`](Self::blob_store_degraded).
/// - **API layer**: reads the flags to populate the `X-Degraded` header, the
///   `/health` readiness endpoint, and admin-panel banners.
#[derive(Debug, Default)]
pub struct DegradationState {
    /// `true` when the blob store (B2 / local) is unreachable.
    pub blob_store_degraded: AtomicBool,
    /// `true` when no healthy embed backends are available.
    pub embed_degraded: AtomicBool,
    /// `true` when no healthy text-generation backends are available.
    pub text_degraded: AtomicBool,
    /// `true` when no healthy vision (VLM) backends are available.
    pub vision_degraded: AtomicBool,
    /// `true` when no healthy code-generation backends are available.
    pub code_degraded: AtomicBool,
    /// `true` when no healthy reranker backends are available.
    pub rerank_degraded: AtomicBool,
}

impl DegradationState {
    /// Returns the list of degraded subsystem names as human-readable
    /// strings. Suitable for the `X-Degraded` header value or admin-panel
    /// banners.
    #[must_use]
    pub fn degraded_subsystems(&self) -> Vec<&'static str> {
        let mut out = Vec::new();
        if self.blob_store_degraded.load(Ordering::Acquire) {
            out.push("blob-store");
        }
        if self.embed_degraded.load(Ordering::Acquire) {
            out.push("embed");
        }
        if self.text_degraded.load(Ordering::Acquire) {
            out.push("text");
        }
        if self.vision_degraded.load(Ordering::Acquire) {
            out.push("vision");
        }
        if self.code_degraded.load(Ordering::Acquire) {
            out.push("code");
        }
        if self.rerank_degraded.load(Ordering::Acquire) {
            out.push("rerank");
        }
        out
    }

    /// Returns `true` when **any** subsystem is degraded.
    #[must_use]
    pub fn is_degraded(&self) -> bool {
        self.blob_store_degraded.load(Ordering::Acquire)
            || self.embed_degraded.load(Ordering::Acquire)
            || self.text_degraded.load(Ordering::Acquire)
            || self.vision_degraded.load(Ordering::Acquire)
            || self.code_degraded.load(Ordering::Acquire)
            || self.rerank_degraded.load(Ordering::Acquire)
    }

    /// Returns the `X-Degraded` header value: a comma-separated list of
    /// degraded subsystems, or `None` when everything is healthy.
    #[must_use]
    pub fn degraded_header_value(&self) -> Option<String> {
        let subs = self.degraded_subsystems();
        if subs.is_empty() {
            None
        } else {
            Some(subs.join(", "))
        }
    }

    /// Set the per-role degraded flag based on whether any healthy backend
    /// exists for that role.
    ///
    /// Called by the health loop / metrics collector.
    pub fn set_role_degraded(&self, role: &str, degraded: bool) {
        let flag = match role {
            "embed" => &self.embed_degraded,
            "text" => &self.text_degraded,
            "vision" => &self.vision_degraded,
            "code" => &self.code_degraded,
            "rerank" => &self.rerank_degraded,
            _ => return, // unknown role — nothing to set
        };
        flag.store(degraded, Ordering::Release);
    }

    /// Returns `true` when a specific role is degraded.
    #[must_use]
    pub fn is_role_degraded(&self, role: &str) -> bool {
        match role {
            "embed" => self.embed_degraded.load(Ordering::Acquire),
            "text" => self.text_degraded.load(Ordering::Acquire),
            "vision" => self.vision_degraded.load(Ordering::Acquire),
            "code" => self.code_degraded.load(Ordering::Acquire),
            "rerank" => self.rerank_degraded.load(Ordering::Acquire),
            _ => false,
        }
    }

    /// Returns `true` when the named subsystem is degraded.
    ///
    /// Unlike [`is_role_degraded`](Self::is_role_degraded) this also resolves the
    /// non-role `"blob-store"` subsystem, so the metrics collector can publish a
    /// `0`/`1` gauge for **every** name in
    /// [`degraded_subsystems`](Self::degraded_subsystems) uniformly. Unknown
    /// names return `false`.
    #[must_use]
    pub fn is_subsystem_degraded(&self, subsystem: &str) -> bool {
        match subsystem {
            "blob-store" => self.blob_store_degraded.load(Ordering::Acquire),
            other => self.is_role_degraded(other),
        }
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::thread;

    use super::*;

    #[test]
    fn default_all_healthy() {
        let ds = DegradationState::default();
        assert!(!ds.is_degraded());
        assert!(ds.degraded_subsystems().is_empty());
    }

    #[test]
    fn blob_store_degraded() {
        let ds = DegradationState::default();
        ds.blob_store_degraded.store(true, Ordering::Release);
        assert!(ds.is_degraded());
        assert_eq!(ds.degraded_subsystems(), vec!["blob-store"]);
        assert_eq!(ds.degraded_header_value(), Some("blob-store".to_string()));
    }

    #[test]
    fn multiple_degraded() {
        let ds = DegradationState::default();
        ds.blob_store_degraded.store(true, Ordering::Release);
        ds.embed_degraded.store(true, Ordering::Release);
        assert!(ds.is_degraded());
        let mut subs = ds.degraded_subsystems();
        subs.sort();
        assert_eq!(subs, vec!["blob-store", "embed"]);
        assert_eq!(
            ds.degraded_header_value(),
            Some("blob-store, embed".to_string())
        );
    }

    #[test]
    fn header_value_none_when_healthy() {
        let ds = DegradationState::default();
        assert_eq!(ds.degraded_header_value(), None);
    }

    #[test]
    fn set_role_degraded_updates_flag() {
        let ds = DegradationState::default();
        ds.set_role_degraded("embed", true);
        assert!(ds.is_role_degraded("embed"));
        ds.set_role_degraded("embed", false);
        assert!(!ds.is_role_degraded("embed"));
    }

    #[test]
    fn set_role_degraded_unknown_role_noop() {
        let ds = DegradationState::default();
        ds.set_role_degraded("unknown", true);
        assert!(!ds.is_degraded());
    }

    #[test]
    fn is_role_degraded_unknown_returns_false() {
        let ds = DegradationState::default();
        assert!(!ds.is_role_degraded("nonexistent"));
    }

    #[test]
    fn is_subsystem_degraded_covers_blob_store_and_roles() {
        let ds = DegradationState::default();
        // All healthy initially.
        for name in ["blob-store", "embed", "text", "vision", "code", "rerank"] {
            assert!(
                !ds.is_subsystem_degraded(name),
                "{name} should start healthy"
            );
        }
        // blob-store is NOT a role, so is_role_degraded can't see it but
        // is_subsystem_degraded must.
        ds.blob_store_degraded.store(true, Ordering::Release);
        assert!(ds.is_subsystem_degraded("blob-store"));
        assert!(!ds.is_role_degraded("blob-store"));
        // A role flag flows through the role path.
        ds.set_role_degraded("embed", true);
        assert!(ds.is_subsystem_degraded("embed"));
        // Unknown name is false.
        assert!(!ds.is_subsystem_degraded("nope"));
    }

    #[test]
    fn is_subsystem_degraded_matches_degraded_subsystems_list() {
        // Every name degraded_subsystems() can emit must be resolvable by
        // is_subsystem_degraded — guards the collector's static name list.
        let ds = DegradationState::default();
        ds.blob_store_degraded.store(true, Ordering::Release);
        ds.set_role_degraded("embed", true);
        ds.set_role_degraded("text", true);
        ds.set_role_degraded("vision", true);
        ds.set_role_degraded("code", true);
        ds.set_role_degraded("rerank", true);
        for name in ds.degraded_subsystems() {
            assert!(
                ds.is_subsystem_degraded(name),
                "degraded_subsystems() emitted {name} but is_subsystem_degraded says healthy"
            );
        }
    }

    #[test]
    fn all_roles_independent() {
        let ds = DegradationState::default();
        ds.set_role_degraded("text", true);
        assert!(ds.is_degraded());
        assert!(!ds.is_role_degraded("embed"));
        assert!(ds.is_role_degraded("text"));
        ds.set_role_degraded("vision", true);
        assert!(ds.is_role_degraded("vision"));
    }

    #[test]
    fn thread_safe_cloneable_via_arc() {
        let ds = Arc::new(DegradationState::default());
        let d2 = Arc::clone(&ds);

        let handle = thread::spawn(move || {
            d2.blob_store_degraded.store(true, Ordering::Release);
        });
        handle.join().unwrap();

        assert!(ds.blob_store_degraded.load(Ordering::Acquire));
        assert_eq!(ds.degraded_subsystems(), vec!["blob-store"]);
    }
}
