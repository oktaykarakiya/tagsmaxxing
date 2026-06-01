//! [`Backend`]: one inference server or provider endpoint with a pluggable
//! capacity guard and capability set (plan §6.1–§6.2, §26.2–§26.3).

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicBool;
use std::time::Instant;

use arc_swap::ArcSwap;
use kb_config::Backend as BackendConfig;
use kb_core::capability::CapSet;
use kb_core::data_class::DataClass;
use kb_core::pricing::Pricing;
use kb_core::provider::{NoopAdapter, ProviderAdapter};
use kb_core::role::Role;
use kb_core::secret::SecretRef;

use crate::bandwidth::BandwidthLimiter;
use crate::capacity::Capacity;
use crate::time_window::TimeWindow;

/// One inference provider the scheduler can route work to (plan §26.3).
///
/// A backend groups an adapter (the HTTP/SDK client for a specific provider
/// family), a capability set (which [`Role`]s it serves), a capacity guard
/// (concurrency + rate limits), and optional pricing metadata.
///
/// Cloning a `Backend` shares live state (capacity, health flag, cooldown,
/// time window, bandwidth limiter) through the inner `Arc`s, so the same
/// backend registered under several roles via [`CapSet`] always observes one
/// shared capacity pool.
#[derive(Debug, Clone)]
pub struct Backend {
    /// Stable identifier (matches config or DB row id).
    pub id: String,
    /// Provider-family adapter (OpenAI-compat, Anthropic, Gemini, …).
    /// Replaces direct HTTP calls from the legacy client (plan §26.1).
    pub adapter: Arc<dyn ProviderAdapter>,
    /// Base URL for HTTP providers; `None` for native-SDK providers.
    pub endpoint: Option<String>,
    /// Reference to an encrypted provider API key in the DB (§24).
    /// Decrypted at call time (P10-T3); `None` for local/unauthenticated.
    pub key: Option<SecretRef>,
    /// Model id / local alias to invoke.
    pub model_id: String,
    /// Which roles (capabilities) this backend can serve.
    pub caps: CapSet,
    /// Pluggable capacity guard (plan §26.2): `Slots`, `Concurrency`, or `Rated`.
    pub capacity: Capacity,
    /// Maximum concurrent requests this backend supports.
    /// Stored separately because [`Capacity`] does not expose max permits
    /// (Semaphore limitation). Used by metrics and observability only.
    pub max_concurrency: usize,
    /// Per-million-token pricing; `None` = free / local-only.
    pub pricing: Option<Pricing>,
    /// Whether inference runs locally or in a remote cloud.
    pub data_class: DataClass,
    /// Routing priority; **lower is preferred** (plan §6.3).
    pub priority: u8,
    /// Liveness flag set by the health loop (plan §6.5); `true` == eligible.
    pub healthy: Arc<AtomicBool>,
    /// When set, this backend is in a 429-induced cooldown period (plan §26.3).
    /// Any `now < cooldown_until` means the backend is skipped in acquire.
    pub cooldown_until: Arc<Mutex<Option<Instant>>>,
    /// Optional time-based access window (plan §26.2, P9-T13).
    /// `None` means the backend is always eligible by time.
    /// Wrapped in [`ArcSwap`] for hot-swappable hot-reload without restart.
    pub time_window: Arc<ArcSwap<Option<TimeWindow>>>,
    /// Optional per-backend bandwidth limiter (plan §26.2, P9-T13).
    /// `None` means no bandwidth restriction.
    /// Wrapped in [`ArcSwap`] for hot-swappable rate changes without restart.
    pub bandwidth_limiter: Arc<ArcSwap<Option<BandwidthLimiter>>>,
}

impl Backend {
    /// Build a fully-specified backend.
    ///
    /// Use [`Self::from_config`] when building from a TOML config entry
    /// (bootstrap only — the DB-driven routing table supersedes it in P9-T5).
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: impl Into<String>,
        adapter: Arc<dyn ProviderAdapter>,
        endpoint: impl Into<String>,
        key: Option<SecretRef>,
        model_id: impl Into<String>,
        caps: CapSet,
        capacity: Capacity,
        max_concurrency: usize,
        pricing: Option<Pricing>,
        data_class: DataClass,
        priority: u8,
    ) -> Self {
        Self {
            id: id.into(),
            adapter,
            endpoint: Some(endpoint.into()),
            key,
            model_id: model_id.into(),
            caps,
            capacity,
            max_concurrency,
            pricing,
            data_class,
            priority,
            healthy: Arc::new(AtomicBool::new(true)),
            cooldown_until: Arc::new(Mutex::new(None)),
            time_window: Arc::new(ArcSwap::new(Arc::new(None))),
            bandwidth_limiter: Arc::new(ArcSwap::new(Arc::new(None))),
        }
    }

    /// Build a backend from its typed configuration entry (plan §6.6).
    ///
    /// The adapter defaults to [`NoopAdapter`] — this is a bootstrap placeholder.
    /// The real adapter is wired when the DB-driven routing table lands (P9-T6).
    pub fn from_config(cfg: &BackendConfig) -> Self {
        let caps = CapSet::from(cfg.roles.as_slice());
        let slots = cfg.slots as usize;
        Self::new(
            cfg.id.clone(),
            Arc::new(NoopAdapter),
            cfg.base_url.clone(),
            None,           /* key — no encrypted key from config */
            cfg.id.clone(), /* model_id — placeholder; DB provides real value */
            caps,
            Capacity::new_slots(slots),
            slots, /* max_concurrency */
            None,  /* pricing — local by default */
            DataClass::Local,
            cfg.priority,
        )
    }

    /// Whether this backend supports `role` (capability gating; plan §26.3).
    ///
    /// Delegates to [`CapSet::supports`].
    pub fn supports(&self, role: Role) -> bool {
        self.caps.supports(role)
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

    // ── Time window (P9-T13) ───────────────────────────────────────────

    /// Whether the current wall-clock time falls within the backend's
    /// configured time window (plan §26.2, P9-T13).
    ///
    /// Returns `true` when no window is set (always eligible).
    pub fn is_in_time_window(&self) -> bool {
        let tw = self.time_window.load();
        match tw.as_ref() {
            None => true,
            Some(window) => window.is_active(&chrono::Local::now()),
        }
    }

    /// Replace the time window (hot-swappable — no restart required).
    ///
    /// Pass `None` to remove the restriction.
    pub fn set_time_window(&self, tw: Option<TimeWindow>) {
        self.time_window.store(Arc::new(tw));
    }

    // ── Bandwidth limiter (P9-T13) ─────────────────────────────────────

    /// Whether there are sufficient bandwidth tokens available.
    ///
    /// Returns `true` when no bandwidth limiter is set (unlimited bandwidth).
    /// Delegates to [`BandwidthLimiter::bandwidth_available`].
    pub fn bandwidth_available(&self) -> bool {
        let bl = self.bandwidth_limiter.load();
        match bl.as_ref() {
            None => true,
            Some(limiter) => limiter.bandwidth_available(),
        }
    }

    /// Attempt to consume `bytes` from the bandwidth limiter's token bucket.
    ///
    /// Returns `true` if the bytes were consumed (or no limiter is set).
    pub fn try_consume_bandwidth(&self, bytes: f64) -> bool {
        let bl = self.bandwidth_limiter.load();
        match bl.as_ref() {
            None => true,
            Some(limiter) => limiter.try_consume_bytes(bytes),
        }
    }

    /// Replace the bandwidth limiter (hot-swappable — no restart required).
    ///
    /// Pass `None` to remove the bandwidth restriction.
    pub fn set_bandwidth_limiter(&self, limiter: Option<BandwidthLimiter>) {
        self.bandwidth_limiter.store(Arc::new(limiter));
    }
}

/// Build a minimal backend for use in unit tests (public for cross-crate test usage).
///
/// Defaults: [`DataClass::Local`], no pricing, no key, [`NoopAdapter`].
/// The `endpoint` replaces the old `base_url`; `caps` are derived from `roles`.
///
/// This is a convenience shorthand for the common test pattern; production code
/// should use [`Backend::new`] or [`Backend::from_config`] instead.
pub fn test_backend(
    id: &str,
    endpoint: impl Into<String>,
    roles: Vec<Role>,
    priority: u8,
    slots: usize,
) -> Backend {
    let endpoint = endpoint.into();
    Backend::new(
        id,
        Arc::new(NoopAdapter),
        &endpoint,
        None,           /* key */
        id.to_string(), /* model_id — use id as placeholder */
        CapSet::from(roles.as_slice()),
        Capacity::new_slots(slots),
        slots, /* max_concurrency */
        None,  /* pricing */
        DataClass::Local,
        priority,
    )
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    fn test_adapter() -> Arc<dyn ProviderAdapter> {
        Arc::new(NoopAdapter)
    }

    fn caps_text() -> CapSet {
        CapSet::from(Role::Text)
    }

    #[test]
    fn new_sets_fields_and_starts_healthy() {
        let b = Backend::new(
            "b1",
            test_adapter(),
            "http://x:8001",
            None,
            "my-model",
            caps_text(),
            Capacity::new_slots(3),
            3, /* max_concurrency */
            None,
            DataClass::Local,
            7,
        );
        assert_eq!(b.id, "b1");
        assert_eq!(b.endpoint.as_deref(), Some("http://x:8001"));
        assert_eq!(b.model_id, "my-model");
        assert!(b.supports(Role::Text));
        assert!(!b.supports(Role::Embed));
        assert_eq!(b.priority, 7);
        assert_eq!(b.free(), 3);
        assert!(b.healthy.load(Ordering::Acquire));
        assert!(!b.cooldown_active());
        assert_eq!(b.data_class, DataClass::Local);
        assert!(b.pricing.is_none());
        assert!(b.key.is_none());
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
        assert_eq!(b.endpoint.as_deref(), Some("http://h:9"));
        assert!(b.supports(Role::Embed));
        assert!(!b.supports(Role::Text));
        assert_eq!(b.priority, 10);
        assert_eq!(b.free(), 2);
        assert_eq!(b.data_class, DataClass::Local);
    }

    #[test]
    fn from_config_multiple_roles() {
        let cfg = BackendConfig {
            id: "multi".into(),
            base_url: "http://x".into(),
            roles: vec![Role::Text, Role::Vision, Role::Code],
            slots: 4,
            priority: 5,
        };
        let b = Backend::from_config(&cfg);
        assert!(b.supports(Role::Text));
        assert!(b.supports(Role::Vision));
        assert!(b.supports(Role::Code));
        assert!(!b.supports(Role::Embed));
        assert!(!b.supports(Role::Rerank));
    }

    #[test]
    fn free_tracks_held_permits() {
        let b = Backend::new(
            "b",
            test_adapter(),
            "http://u",
            None,
            "model",
            caps_text(),
            Capacity::new_slots(2),
            2, /* max_concurrency */
            None,
            DataClass::Local,
            0,
        );
        assert_eq!(b.free(), 2);
        let permit = b.capacity.try_acquire().unwrap();
        assert_eq!(b.free(), 1);
        drop(permit);
        assert_eq!(b.free(), 2);
    }

    #[test]
    fn clone_shares_one_slot_pool() {
        let b = Backend::new(
            "b",
            test_adapter(),
            "http://u",
            None,
            "model",
            caps_text(),
            Capacity::new_slots(1),
            1, /* max_concurrency */
            None,
            DataClass::Local,
            0,
        );
        let twin = b.clone();
        assert_eq!(twin.free(), 1);
        let _permit = b.capacity.try_acquire().unwrap();
        // The clone observes the same now-exhausted capacity.
        assert_eq!(twin.free(), 0);
    }

    #[test]
    fn cooldown_defaults_to_inactive() {
        let b = test_backend("b", "http://u", vec![Role::Text], 0, 1);
        assert!(!b.cooldown_active());
    }

    #[test]
    fn cooldown_active_while_in_effect() {
        let b = test_backend("b", "http://u", vec![Role::Text], 0, 1);
        let future = Instant::now() + Duration::from_secs(60);
        b.set_cooldown(future);
        assert!(b.cooldown_active());
    }

    #[test]
    fn cooldown_expired_returns_false() {
        let b = test_backend("b", "http://u", vec![Role::Text], 0, 1);
        let past = Instant::now() - Duration::from_secs(10);
        b.set_cooldown(past);
        assert!(!b.cooldown_active());
    }

    #[test]
    fn clear_cooldown_resets() {
        let b = test_backend("b", "http://u", vec![Role::Text], 0, 1);
        let future = Instant::now() + Duration::from_secs(60);
        b.set_cooldown(future);
        assert!(b.cooldown_active());
        b.clear_cooldown();
        assert!(!b.cooldown_active());
    }

    #[test]
    fn clone_shares_cooldown_state() {
        let b = test_backend("b", "http://u", vec![Role::Text], 0, 1);
        let twin = b.clone();
        let future = Instant::now() + Duration::from_secs(60);
        b.set_cooldown(future);
        assert!(twin.cooldown_active());
        twin.clear_cooldown();
        assert!(!b.cooldown_active());
    }

    #[test]
    fn supports_uses_cap_set() {
        let caps = CapSet::from(&[Role::Text, Role::Embed][..]);
        let b = Backend::new(
            "multi",
            test_adapter(),
            "http://x",
            None,
            "model",
            caps,
            Capacity::new_slots(2),
            2, /* max_concurrency */
            None,
            DataClass::Local,
            0,
        );
        assert!(b.supports(Role::Text));
        assert!(b.supports(Role::Embed));
        assert!(!b.supports(Role::Vision));
        assert!(!b.supports(Role::Code));
        assert!(!b.supports(Role::Rerank));
    }

    #[test]
    fn test_backend_helper_creates_expected_backend() {
        let b = test_backend("t1", "http://t:1", vec![Role::Text, Role::Embed], 5, 3);
        assert_eq!(b.id, "t1");
        assert_eq!(b.endpoint.as_deref(), Some("http://t:1"));
        assert!(b.supports(Role::Text));
        assert!(b.supports(Role::Embed));
        assert!(!b.supports(Role::Vision));
        assert_eq!(b.priority, 5);
        assert_eq!(b.free(), 3);
        assert!(b.key.is_none());
        assert!(b.pricing.is_none());
        assert_eq!(b.data_class, DataClass::Local);
    }

    #[test]
    fn pricing_stored_and_accessible() {
        let p = Pricing::new(1.0, 2.0);
        let b = Backend::new(
            "paid",
            test_adapter(),
            "http://x",
            None,
            "gpt-4",
            caps_text(),
            Capacity::new_slots(5),
            5, /* max_concurrency */
            Some(p),
            DataClass::Remote,
            1,
        );
        assert_eq!(b.pricing.unwrap(), p);
        assert_eq!(b.data_class, DataClass::Remote);
        assert_eq!(b.model_id, "gpt-4");
    }

    #[test]
    fn secret_key_accessible() {
        let key = SecretRef::new("sk-abc123");
        let b = Backend::new(
            "remote",
            test_adapter(),
            "http://x",
            Some(key.clone()),
            "claude",
            caps_text(),
            Capacity::new_slots(5),
            5, /* max_concurrency */
            None,
            DataClass::Remote,
            1,
        );
        assert_eq!(b.key.as_ref().unwrap().id(), "sk-abc123");
    }
}
