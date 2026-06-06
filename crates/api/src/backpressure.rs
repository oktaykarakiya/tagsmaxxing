//! In-flight request limiter for backpressure (plan §22, P8-T9, P14-T7).
//!
//! Caps the number of concurrent ingest requests to prevent resource
//! exhaustion. When the limit is reached the caller receives `None` from
//! [`try_acquire`](InflightLimiter::try_acquire) — the API handler can then
//! return `429 Too Many Requests` with a `Retry-After` header.
//!
//! # Per-tenant fairness (P14-T7)
//!
//! The limiter enforces **two** ceilings simultaneously:
//!
//! 1. A **global** ceiling (`max`) — the overall system-wide cap on concurrent
//!    ingests, shared by every tenant.
//! 2. A **per-tenant** ceiling (`per_tenant_max`) — each tenant gets its own
//!    semaphore, so a single noisy tenant can hold at most `per_tenant_max`
//!    permits no matter how many requests it makes.
//!
//! A request must acquire **both** a per-tenant permit *and* a global permit;
//! the returned RAII [`Permit`] holds both and releases both on drop.
//!
//! **Fairness invariant:** because each tenant is bounded to `per_tenant_max`
//! in-flight requests, a single tenant can never consume more than that many of
//! the global permits. With `per_tenant_max < max`, at least `max -
//! per_tenant_max` global permits always remain reachable by *other* tenants —
//! so one tenant saturating its own share cannot `429` a different tenant. The
//! global ceiling still caps the *total* in-flight across all tenants.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// A held in-flight slot: keeps both the per-tenant and global permits alive.
///
/// Dropping a `Permit` releases **both** underlying semaphore permits, freeing
/// one unit of the tenant's per-tenant budget and one unit of the global pool.
/// The handler holds it for the lifetime of the request (RAII).
#[derive(Debug)]
pub struct Permit {
    /// Held for RAII: released on drop. Order is irrelevant — both are
    /// independent owned permits.
    _tenant: OwnedSemaphorePermit,
    /// Held for RAII: released on drop.
    _global: OwnedSemaphorePermit,
}

/// Caps in-flight ingest requests with a global + per-tenant [`Semaphore`] pair.
///
/// # Cloning
///
/// The struct is cheap to clone — the global semaphore and the per-tenant map
/// are both [`Arc`]-wrapped and shared across clones.
#[derive(Clone, Debug)]
pub struct InflightLimiter {
    /// System-wide ceiling shared by every tenant.
    global: Arc<Semaphore>,
    /// Per-tenant semaphores, created lazily on first use of each tenant id.
    tenants: Arc<Mutex<HashMap<i64, Arc<Semaphore>>>>,
    /// Configured global maximum (0 = disabled/unlimited).
    max: usize,
    /// Configured per-tenant maximum (always ≥ 1 when `max > 0`).
    per_tenant_max: usize,
}

/// Permit count handed to a [`Semaphore`] when the limiter is disabled.
///
/// `tokio::sync::Semaphore::MAX_PERMITS` is the hard upper bound; we halve it so
/// [`InflightLimiter::in_flight`] arithmetic cannot overflow.
fn unlimited_permits() -> usize {
    Semaphore::MAX_PERMITS / 2
}

/// Derive the default per-tenant ceiling from the global ceiling: `max(1,
/// global / 4)` so a single tenant is bounded to roughly a quarter of the pool
/// (leaving ≥ ¾ for everyone else) while always getting at least one slot.
#[must_use]
pub fn default_per_tenant_max(global_max: usize) -> usize {
    (global_max / 4).max(1)
}

impl InflightLimiter {
    /// Create a limiter with a `max` global ceiling and an **auto-derived**
    /// per-tenant ceiling of [`default_per_tenant_max`] (`max(1, max / 4)`).
    ///
    /// When `max` is `0` the limiter is disabled — [`try_acquire`](Self::try_acquire)
    /// always succeeds regardless of concurrency or tenant.
    #[must_use]
    pub fn new(max: usize) -> Self {
        Self::with_per_tenant(max, 0)
    }

    /// Create a limiter with an explicit global and per-tenant ceiling.
    ///
    /// * `max` — the global ceiling. `0` disables the limiter entirely.
    /// * `per_tenant_max` — the per-tenant ceiling. `0` means *derive* it as
    ///   [`default_per_tenant_max`] of `max`. When `max > 0` the effective
    ///   per-tenant ceiling is clamped to `1..=max` (a per-tenant cap above the
    ///   global cap is pointless; below 1 would wedge every tenant).
    #[must_use]
    pub fn with_per_tenant(max: usize, per_tenant_max: usize) -> Self {
        if max == 0 {
            // Disabled: both pools are effectively unbounded so try_acquire
            // never returns None and no per-tenant accounting is needed.
            return Self {
                global: Arc::new(Semaphore::new(unlimited_permits())),
                tenants: Arc::new(Mutex::new(HashMap::new())),
                max: 0,
                per_tenant_max: 0,
            };
        }
        let effective_per_tenant = if per_tenant_max == 0 {
            default_per_tenant_max(max)
        } else {
            // A per-tenant cap can never usefully exceed the global cap, and
            // must be ≥ 1 so a tenant can make progress.
            per_tenant_max.clamp(1, max)
        };
        Self {
            global: Arc::new(Semaphore::new(max)),
            tenants: Arc::new(Mutex::new(HashMap::new())),
            max,
            per_tenant_max: effective_per_tenant,
        }
    }

    /// Fetch (or lazily create) the semaphore for `tenant_id`.
    ///
    /// Each tenant's semaphore starts with `per_tenant_max` permits. The map is
    /// guarded by a `Mutex` held only for the brief insert/lookup.
    fn tenant_semaphore(&self, tenant_id: i64) -> Arc<Semaphore> {
        let mut map = self
            .tenants
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Arc::clone(
            map.entry(tenant_id)
                .or_insert_with(|| Arc::new(Semaphore::new(self.per_tenant_max))),
        )
    }

    /// Try to acquire an in-flight slot for `tenant_id` without waiting.
    ///
    /// Returns `Some(Permit)` only when **both** the tenant's per-tenant budget
    /// *and* the global pool have capacity. Returns `None` when either is
    /// exhausted — the caller should return `429 Too Many Requests`.
    ///
    /// A `None` caused by the tenant's own cap throttles **only that tenant**;
    /// other tenants are unaffected (the fairness invariant — see the module
    /// docs). The tenant permit is taken first, so a global permit is never held
    /// while the tenant is over its own cap.
    pub fn try_acquire(&self, tenant_id: i64) -> Option<Permit> {
        if self.max == 0 {
            // Disabled: per-tenant accounting is off (its semaphores start at 0
            // permits), so take two permits from the unbounded global pool and
            // keep the RAII Permit shape identical on every path.
            let g = Arc::clone(&self.global);
            let _tenant = g.try_acquire_owned().ok()?;
            let _global = Arc::clone(&self.global).try_acquire_owned().ok()?;
            return Some(Permit { _tenant, _global });
        }
        // Take the per-tenant permit first; if the tenant is at its cap we bail
        // without ever touching the global pool (so a saturated tenant cannot
        // even momentarily consume a shared permit).
        let tenant = self.tenant_semaphore(tenant_id);
        let _tenant = tenant.try_acquire_owned().ok()?;
        // Then the global permit. If the global ceiling is hit, dropping
        // `_tenant` here releases the per-tenant permit we just took.
        let _global = Arc::clone(&self.global).try_acquire_owned().ok()?;
        Some(Permit { _tenant, _global })
    }

    /// Number of currently in-flight requests across **all** tenants
    /// (approximate — read without locking, so concurrent acquires/releases may
    /// make it slightly stale). Returns `0` when disabled.
    #[must_use]
    pub fn in_flight(&self) -> usize {
        if self.max == 0 {
            return 0;
        }
        self.max.saturating_sub(self.global.available_permits())
    }

    /// Maximum concurrent requests this limiter allows globally.
    /// Returns `0` when disabled (unlimited).
    #[must_use]
    pub fn max(&self) -> usize {
        self.max
    }

    /// Per-tenant ceiling: the most concurrent in-flight requests a single
    /// tenant may hold. Returns `0` when the limiter is disabled.
    #[must_use]
    pub fn per_tenant_max(&self) -> usize {
        self.per_tenant_max
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::sync::Arc;

    use super::*;

    const T_A: i64 = 1;
    const T_B: i64 = 2;

    #[test]
    fn acquire_below_limit() {
        let limiter = InflightLimiter::with_per_tenant(2, 2);
        let p1 = limiter.try_acquire(T_A);
        assert!(p1.is_some());
        let p2 = limiter.try_acquire(T_A);
        assert!(p2.is_some());
    }

    #[test]
    fn none_when_global_saturated() {
        // Global cap of 1, per-tenant high enough not to bind.
        let limiter = InflightLimiter::with_per_tenant(1, 1);
        let p1 = limiter.try_acquire(T_A);
        assert!(p1.is_some());
        // Even a *different* tenant is blocked once the global pool is empty.
        let p2 = limiter.try_acquire(T_B);
        assert!(p2.is_none());
    }

    #[test]
    fn release_frees_both_permits() {
        let limiter = InflightLimiter::with_per_tenant(1, 1);
        let p = limiter.try_acquire(T_A).unwrap();
        // Saturated globally and per-tenant.
        assert!(limiter.try_acquire(T_A).is_none());
        drop(p);
        // After dropping, both the per-tenant and the global permit are back.
        assert!(limiter.try_acquire(T_A).is_some());
    }

    #[test]
    fn saturated_tenant_cannot_starve_another() {
        // FAIRNESS: global pool of 10, but each tenant capped at 2. Tenant A
        // grabs its full per-tenant share, then A's next request 429s — yet
        // tenant B can still acquire because A could never drain the global pool.
        let limiter = InflightLimiter::with_per_tenant(10, 2);
        let a1 = limiter.try_acquire(T_A);
        let a2 = limiter.try_acquire(T_A);
        assert!(a1.is_some() && a2.is_some(), "A gets its per-tenant share");

        // A is now at its per-tenant cap → throttled (429 for A only).
        assert!(
            limiter.try_acquire(T_A).is_none(),
            "A over its per-tenant cap must be throttled"
        );

        // B is completely unaffected: the global pool still has 8 free permits.
        let b1 = limiter.try_acquire(T_B);
        assert!(
            b1.is_some(),
            "a saturated tenant A must not 429 a different tenant B"
        );

        // Hold everything alive to the end of the test.
        drop((a1, a2, b1));
    }

    #[test]
    fn global_ceiling_caps_total_across_tenants() {
        // Global cap of 3; per-tenant cap of 2. Two tenants together can hold at
        // most 3 in-flight — the global ceiling, not 2+2=4.
        let limiter = InflightLimiter::with_per_tenant(3, 2);
        let a1 = limiter.try_acquire(T_A);
        let a2 = limiter.try_acquire(T_A);
        let b1 = limiter.try_acquire(T_B);
        assert!(a1.is_some() && a2.is_some() && b1.is_some());
        assert_eq!(limiter.in_flight(), 3);

        // The global pool is now empty: B's next request 429s even though B is
        // below its own per-tenant cap of 2.
        assert!(
            limiter.try_acquire(T_B).is_none(),
            "global ceiling caps the total in-flight across tenants"
        );
        drop((a1, a2, b1));
    }

    #[test]
    fn per_tenant_default_is_quarter_of_global() {
        // new(max) auto-derives per_tenant = max(1, max/4).
        assert_eq!(default_per_tenant_max(100), 25);
        assert_eq!(default_per_tenant_max(4), 1);
        assert_eq!(default_per_tenant_max(3), 1); // floor, then max(1, .)
        assert_eq!(default_per_tenant_max(1), 1); // never below 1
        assert_eq!(default_per_tenant_max(0), 1); // guarded by caller; still ≥1

        let limiter = InflightLimiter::new(100);
        assert_eq!(limiter.max(), 100);
        assert_eq!(limiter.per_tenant_max(), 25);
    }

    #[test]
    fn explicit_per_tenant_is_clamped_to_global() {
        // A per-tenant cap above the global cap is clamped down to the global.
        let limiter = InflightLimiter::with_per_tenant(4, 100);
        assert_eq!(limiter.per_tenant_max(), 4);
        // A per-tenant value of 0 means "derive".
        let derived = InflightLimiter::with_per_tenant(40, 0);
        assert_eq!(derived.per_tenant_max(), 10);
    }

    #[test]
    fn in_flight_counts_global_across_tenants() {
        let limiter = InflightLimiter::with_per_tenant(3, 3);
        assert_eq!(limiter.in_flight(), 0);
        let p1 = limiter.try_acquire(T_A).unwrap();
        assert_eq!(limiter.in_flight(), 1);
        let p2 = limiter.try_acquire(T_B).unwrap();
        assert_eq!(limiter.in_flight(), 2);
        drop(p1);
        assert_eq!(limiter.in_flight(), 1);
        drop(p2);
        assert_eq!(limiter.in_flight(), 0);
    }

    #[test]
    fn disabled_always_acquires_any_tenant() {
        let limiter = InflightLimiter::new(0);
        assert_eq!(limiter.max(), 0);
        assert_eq!(limiter.per_tenant_max(), 0);
        assert_eq!(limiter.in_flight(), 0);
        // Many concurrent acquires across many tenants all succeed.
        let permits: Vec<_> = (0..100)
            .map(|i| limiter.try_acquire(i64::from(i % 5)).unwrap())
            .collect();
        assert_eq!(permits.len(), 100);
        assert_eq!(limiter.in_flight(), 0, "disabled path reports 0");
        drop(permits);
    }

    #[test]
    fn arc_shared_across_clones() {
        let lim1 = Arc::new(InflightLimiter::with_per_tenant(1, 1));
        let lim2 = Arc::clone(&lim1);
        let p = lim1.try_acquire(T_A).unwrap();
        assert!(lim2.try_acquire(T_A).is_none());
        drop(p);
        assert!(lim2.try_acquire(T_A).is_some());
    }

    #[test]
    fn distinct_tenants_have_independent_budgets() {
        // With a generous global pool, each tenant exhausts only its own cap.
        let limiter = InflightLimiter::with_per_tenant(100, 1);
        let a = limiter.try_acquire(T_A).unwrap();
        assert!(limiter.try_acquire(T_A).is_none(), "A capped at 1");
        // A different tenant still has its full (fresh) budget.
        let b = limiter.try_acquire(T_B).unwrap();
        assert!(limiter.try_acquire(T_B).is_none(), "B independently capped");
        drop((a, b));
    }
}
