//! [`Backend`]: one inference server with a pluggable capacity guard
//! (plan §6.1–§6.2, §26.2).

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::time::Instant;

use kb_config::Backend as BackendConfig;
use kb_core::role::Role;

use crate::capacity::Capacity;

/// One OpenAI-compatible inference server the scheduler can route work to.
///
/// A backend advertises the [`Role`]s it can serve and caps concurrency with a
/// [`Capacity`] guard (plan §26.2). The capacity guard is the single source of
/// truth for in-flight load; [`Backend::healthy`] is liveness only (plan §6.5).
///
/// Cloning a `Backend` shares its live state (capacity, health flag, in-flight
/// counter, cooldown) through the inner `Arc`s, so the same backend registered
/// under several roles always observes one shared capacity pool.
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
    /// Pluggable capacity guard (plan §26.2): `Slots`, `Concurrency`, or `Rated`.
    pub capacity: Capacity,
    /// Total permit count stored explicitly because `Semaphore` does not expose
    /// its max permits. Deprecated: prefer `capacity.free()` or other methods.
    /// Kept for backward compatibility in tests.
    pub max_slots: usize,
    /// Liveness flag set by the health loop (plan §6.5); `true` == eligible.
    pub healthy: Arc<AtomicBool>,
    /// Best-effort in-flight counter for metrics / least-loaded tie-breaks.
    pub in_flight: Arc<AtomicUsize>,
    /// When set, this backend is in a 429-induced cooldown period (plan §26.3).
    /// Any `now < cooldown_until` means the backend is skipped in acquire.
    pub cooldown_until: Arc<Mutex<Option<Instant>>>,
}

impl Backend {
    /// Build a backend with `slots` concurrency permits, healthy by default.
    ///
    /// The `slots` parameter creates a [`Capacity::Slots`] guard (local or
    /// operator-chosen concurrency). Use [`Self::with_concurrency`] or
    /// [`Self::with_rated`] for other capacity variants.
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
            capacity: Capacity::new_slots(slots),
            max_slots: slots,
            healthy: Arc::new(AtomicBool::new(true)),
            in_flight: Arc::new(AtomicUsize::new(0)),
            cooldown_until: Arc::new(Mutex::new(None)),
        }
    }

    /// Build a backend from its typed configuration entry (plan §6.6).
    ///
    /// The config `slots` count (a `u32`) becomes a [`Capacity::Slots`] guard.
    pub fn from_config(cfg: &BackendConfig) -> Self {
        Self::new(
            cfg.id.clone(),
            cfg.base_url.clone(),
            cfg.roles.clone(),
            cfg.priority,
            cfg.slots as usize,
        )
    }

    /// Free concurrency slots right now (plan §6.2).
    ///
    /// Delegates to [`Capacity::free`]. For `Slots`/`Concurrency`, this is
    /// `semaphore.available_permits()`. For `Rated`, it returns concurrency
    /// headroom; token-bucket headroom is separately queried via
    /// [`Capacity::is_usable`].
    pub fn free(&self) -> usize {
        self.capacity.free()
    }

    /// Whether this backend is in a cooldown period.
    ///
    /// A backend enters cooldown on a `429` response (plan §26.2) and is
    /// skipped in [`crate::Pool::acquire`] until the cooldown expires.
    pub fn cooldown_active(&self) -> bool {
        let guard = self
            .cooldown_until
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        guard.is_some_and(|until| Instant::now() < until)
    }

    /// Enter cooldown until `until` (typically `now + Retry-After`).
    pub fn set_cooldown(&self, until: Instant) {
        let mut guard = self
            .cooldown_until
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        *guard = Some(until);
    }

    /// Clear any active cooldown.
    pub fn clear_cooldown(&self) {
        let mut guard = self
            .cooldown_until
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        *guard = None;
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    #[test]
    fn new_sets_fields_and_starts_healthy() {
        let b = Backend::new("b1", "http://x:8001", vec![Role::Text, Role::Embed], 7, 3);
        assert_eq!(b.id, "b1");
        assert_eq!(b.base_url, "http://x:8001");
        assert_eq!(b.roles, vec![Role::Text, Role::Embed]);
        assert_eq!(b.priority, 7);
        assert_eq!(b.max_slots, 3);
        assert_eq!(b.free(), 3);
        assert!(b.healthy.load(Ordering::Acquire));
        assert_eq!(b.in_flight.load(Ordering::Acquire), 0);
        assert!(!b.cooldown_active());
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
        assert_eq!(b.max_slots, 2);
        assert_eq!(b.free(), 2);
    }

    #[test]
    fn free_tracks_held_permits() {
        let b = Backend::new("b", "u", vec![Role::Text], 0, 2);
        assert_eq!(b.free(), 2);
        let permit = b.capacity.try_acquire().unwrap();
        assert_eq!(b.free(), 1);
        drop(permit);
        assert_eq!(b.free(), 2);
    }

    #[test]
    fn clone_shares_one_slot_pool() {
        let b = Backend::new("b", "u", vec![Role::Text], 0, 1);
        let twin = b.clone();
        assert_eq!(twin.max_slots, 1);
        let _permit = b.capacity.try_acquire().unwrap();
        // The clone observes the same now-exhausted capacity.
        assert_eq!(twin.free(), 0);
    }

    #[test]
    fn cooldown_defaults_to_inactive() {
        let b = Backend::new("b", "u", vec![Role::Text], 0, 1);
        assert!(!b.cooldown_active());
    }

    #[test]
    fn cooldown_active_while_in_effect() {
        let b = Backend::new("b", "u", vec![Role::Text], 0, 1);
        let future = Instant::now() + Duration::from_secs(60);
        b.set_cooldown(future);
        assert!(b.cooldown_active());
    }

    #[test]
    fn cooldown_expired_returns_false() {
        let b = Backend::new("b", "u", vec![Role::Text], 0, 1);
        let past = Instant::now() - Duration::from_secs(10);
        b.set_cooldown(past);
        assert!(!b.cooldown_active());
    }

    #[test]
    fn clear_cooldown_resets() {
        let b = Backend::new("b", "u", vec![Role::Text], 0, 1);
        let future = Instant::now() + Duration::from_secs(60);
        b.set_cooldown(future);
        assert!(b.cooldown_active());
        b.clear_cooldown();
        assert!(!b.cooldown_active());
    }

    #[test]
    fn clone_shares_cooldown_state() {
        let b = Backend::new("b", "u", vec![Role::Text], 0, 1);
        let twin = b.clone();
        let future = Instant::now() + Duration::from_secs(60);
        b.set_cooldown(future);
        assert!(twin.cooldown_active());
        twin.clear_cooldown();
        assert!(!b.cooldown_active());
    }
}
