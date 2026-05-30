//! [`Backend`]: one inference server with a concurrency-limited slot pool
//! (plan §6.1–§6.2).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize};

use kb_config::Backend as BackendConfig;
use kb_core::role::Role;
use tokio::sync::Semaphore;

/// One OpenAI-compatible inference server the scheduler can route work to.
///
/// A backend advertises the [`Role`]s it can serve and caps concurrency with a
/// [`Semaphore`] of `slots` permits. The semaphore is the single source of truth
/// for in-flight load; [`Backend::healthy`] is liveness only (plan §6.5).
///
/// Cloning a `Backend` shares its live state (semaphore, health flag, in-flight
/// counter) through the inner `Arc`s, so the same backend registered under
/// several roles always observes one shared slot pool.
#[derive(Debug, Clone)]
pub struct Backend {
    /// Stable identifier (matches the config `id`).
    pub id: String,
    /// OpenAI-compatible base URL requests are sent to.
    pub base_url: String,
    /// Roles this backend can serve.
    pub roles: Vec<Role>,
    /// Routing priority; **lower is preferred** (plan §6.3).
    pub priority: u8,
    /// Concurrency permits; one is held per in-flight request via a [`crate::Lease`].
    pub slots: Arc<Semaphore>,
    /// Liveness flag set by the health loop (plan §6.5); `true` == eligible.
    pub healthy: Arc<AtomicBool>,
    /// Best-effort in-flight counter for metrics / least-loaded tie-breaks.
    pub in_flight: Arc<AtomicUsize>,
}

impl Backend {
    /// Build a backend with `slots` concurrency permits, healthy by default.
    pub fn new(
        id: impl Into<String>,
        base_url: impl Into<String>,
        roles: Vec<Role>,
        priority: u8,
        slots: usize,
    ) -> Self {
        Self {
            id: id.into(),
            base_url: base_url.into(),
            roles,
            priority,
            slots: Arc::new(Semaphore::new(slots)),
            healthy: Arc::new(AtomicBool::new(true)),
            in_flight: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Build a backend from its typed configuration entry (plan §6.6).
    ///
    /// The config `slots` count (a `u32`) becomes the semaphore's permit count.
    pub fn from_config(cfg: &BackendConfig) -> Self {
        Self::new(
            cfg.id.clone(),
            cfg.base_url.clone(),
            cfg.roles.clone(),
            cfg.priority,
            cfg.slots as usize,
        )
    }

    /// Free slots right now — `0` when the semaphore is exhausted (plan §6.2).
    pub fn free(&self) -> usize {
        self.slots.available_permits()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use std::sync::atomic::Ordering;

    #[test]
    fn new_sets_fields_and_starts_healthy() {
        let b = Backend::new("b1", "http://x:8001", vec![Role::Text, Role::Embed], 7, 3);
        assert_eq!(b.id, "b1");
        assert_eq!(b.base_url, "http://x:8001");
        assert_eq!(b.roles, vec![Role::Text, Role::Embed]);
        assert_eq!(b.priority, 7);
        assert_eq!(b.free(), 3);
        assert!(b.healthy.load(Ordering::Acquire));
        assert_eq!(b.in_flight.load(Ordering::Acquire), 0);
    }

    #[test]
    fn from_config_maps_every_field() {
        let cfg = BackendConfig {
            id: "gpu".into(),
            base_url: "http://h:9".into(),
            roles: vec![Role::Embed],
            slots: 2,
            priority: 10,
        };
        let b = Backend::from_config(&cfg);
        assert_eq!(b.id, "gpu");
        assert_eq!(b.base_url, "http://h:9");
        assert_eq!(b.roles, vec![Role::Embed]);
        assert_eq!(b.priority, 10);
        assert_eq!(b.free(), 2);
    }

    #[test]
    fn free_tracks_held_permits() {
        let b = Backend::new("b", "u", vec![Role::Text], 0, 2);
        assert_eq!(b.free(), 2);
        let permit = b.slots.clone().try_acquire_owned().unwrap();
        assert_eq!(b.free(), 1);
        drop(permit);
        assert_eq!(b.free(), 2);
    }

    #[test]
    fn clone_shares_one_slot_pool() {
        let b = Backend::new("b", "u", vec![Role::Text], 0, 1);
        let twin = b.clone();
        let _permit = b.slots.clone().try_acquire_owned().unwrap();
        // The clone observes the same now-exhausted semaphore.
        assert_eq!(twin.free(), 0);
    }
}
