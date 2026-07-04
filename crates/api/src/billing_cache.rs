// SPDX-License-Identifier: AGPL-3.0-or-later

//! Short-lived in-memory cache for per-tenant billing lookups.
//!
//! Every authenticated request calls `resolve_rate_cap` (middleware) and every
//! ingest/search request also calls `is_remote_models_allowed` (handlers).
//! Both ultimately hit `get_tenant_billing()` — the same tenants + plans rows.
//! This cache (60 s TTL) eliminates the redundant second call and reduces the
//! per-request DB round-trip count by 1–2.
//!
//! A plan change (Stripe webhook, admin override) takes effect within 60 s at
//! most, which satisfies the existing hot-swap rule documented in the auth
//! middleware and `resolve_local_only`.

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

use kb_store::PgStore;

/// How long a cached billing row stays valid.
const TTL: Duration = Duration::from_secs(60);

struct Entry {
    rate_cap: Option<i64>,
    remote_models_allowed: Option<bool>,
    at: Instant,
}

struct Inner {
    map: Mutex<HashMap<i64, Entry>>,
}

impl Inner {
    /// Look up `tenant_id` as of `now` (clock injected for deterministic tests).
    fn get_at(&self, tenant_id: i64, now: Instant) -> Option<(Option<i64>, Option<bool>)> {
        let map = self.map.lock().unwrap_or_else(|e| e.into_inner());
        map.get(&tenant_id)
            .filter(|e| now.duration_since(e.at) < TTL)
            .map(|e| (e.rate_cap, e.remote_models_allowed))
    }

    fn get(&self, tenant_id: i64) -> Option<(Option<i64>, Option<bool>)> {
        self.get_at(tenant_id, Instant::now())
    }

    /// Insert an entry stamped `at` (clock injected for deterministic tests).
    fn set_at(
        &self,
        tenant_id: i64,
        rate_cap: Option<i64>,
        remote_models_allowed: Option<bool>,
        at: Instant,
    ) {
        let mut map = self.map.lock().unwrap_or_else(|e| e.into_inner());
        map.insert(
            tenant_id,
            Entry {
                rate_cap,
                remote_models_allowed,
                at,
            },
        );
    }

    fn set(&self, tenant_id: i64, rate_cap: Option<i64>, remote_models_allowed: Option<bool>) {
        self.set_at(tenant_id, rate_cap, remote_models_allowed, Instant::now());
    }

    /// Drop entries older than [`TTL`] as of `now`.
    fn prune_at(&self, now: Instant) {
        let mut map = self.map.lock().unwrap_or_else(|e| e.into_inner());
        map.retain(|_, e| now.duration_since(e.at) < TTL);
    }

    /// Drop entries older than TTL. Called periodically from [`billing_cache_cleanup`].
    fn prune(&self) {
        self.prune_at(Instant::now());
    }
}

static CACHE: LazyLock<Inner> = LazyLock::new(|| Inner {
    map: Mutex::new(HashMap::new()),
});

/// Resolve the per-minute rate cap for `tenant_id`, reading through the cache.
///
/// On a cache miss this populates **both** the rate cap and the remote-models
/// flag in a single `get_tenant_billing` call, so a subsequent
/// [`is_remote_models_allowed_cached`] hits the cache without a second DB
/// round-trip.
pub async fn resolve_rate_cap_cached(
    pg_store: &PgStore,
    tenant_id: i64,
) -> anyhow::Result<Option<i64>> {
    if let Some((rate_cap, _)) = CACHE.get(tenant_id) {
        return Ok(rate_cap);
    }
    // Fetch both values with one DB call chain so the handler's
    // is_remote_models_allowed_cached hits the cache.
    let billing = pg_store.get_tenant_billing(tenant_id).await?;
    let rate_cap = billing
        .as_ref()
        .and_then(|b| b.plan.as_ref())
        .and_then(|p| p.per_minute_rate_cap());
    let remote = billing
        .as_ref()
        .and_then(|b| b.plan.as_ref())
        .map(|p| p.remote_models_allowed());
    CACHE.set(tenant_id, rate_cap, remote);
    Ok(rate_cap)
}

/// Check whether the tenant's plan allows remote models, reading through the cache.
///
/// On a cache miss falls back to a direct DB call (typically the middleware already
/// populated the cache with [`resolve_rate_cap_cached`]).
pub async fn is_remote_models_allowed_cached(pg_store: &PgStore, tenant_id: i64) -> bool {
    if let Some((_, Some(allowed))) = CACHE.get(tenant_id) {
        return allowed;
    }
    pg_store
        .is_remote_models_allowed(tenant_id)
        .await
        .unwrap_or(false)
}

/// Prune expired entries from the billing cache. Called every 120 s by the
/// background cleanup task spawned in `start_cleanup`.
pub fn billing_cache_cleanup() {
    CACHE.prune();
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    // NOTE: tests share the global CACHE static, so every test uses its own
    // tenant-id range to stay independent under parallel execution.

    fn fresh_inner() -> Inner {
        Inner {
            map: Mutex::new(HashMap::new()),
        }
    }

    fn disconnected_store() -> PgStore {
        PgStore::new("postgres://user:pw@127.0.0.1:1/unreachable")
    }

    #[test]
    fn get_hits_within_ttl() {
        let inner = fresh_inner();
        let t0 = Instant::now();
        inner.set_at(1, Some(30), Some(true), t0);
        assert_eq!(inner.get_at(1, t0), Some((Some(30), Some(true))));
        // One second before expiry: still served.
        assert_eq!(
            inner.get_at(1, t0 + TTL - Duration::from_secs(1)),
            Some((Some(30), Some(true)))
        );
    }

    #[test]
    fn get_misses_after_ttl() {
        let inner = fresh_inner();
        let t0 = Instant::now();
        inner.set_at(1, Some(30), Some(false), t0);
        // Exactly at the TTL boundary the entry is stale (strict `<`).
        assert_eq!(inner.get_at(1, t0 + TTL), None);
        assert_eq!(inner.get_at(1, t0 + TTL + Duration::from_secs(1)), None);
    }

    #[test]
    fn get_unknown_tenant_is_none() {
        let inner = fresh_inner();
        assert_eq!(inner.get_at(999, Instant::now()), None);
    }

    #[test]
    fn set_overwrites_existing_entry() {
        let inner = fresh_inner();
        let t0 = Instant::now();
        inner.set_at(1, Some(10), Some(false), t0);
        inner.set_at(1, None, Some(true), t0 + Duration::from_secs(5));
        assert_eq!(
            inner.get_at(1, t0 + Duration::from_secs(6)),
            Some((None, Some(true)))
        );
    }

    #[test]
    fn prune_drops_stale_keeps_fresh() {
        let inner = fresh_inner();
        let t0 = Instant::now();
        inner.set_at(1, Some(10), None, t0);
        inner.set_at(2, Some(20), None, t0 + TTL);
        inner.prune_at(t0 + TTL + Duration::from_secs(1));
        // Tenant 1 is past TTL and pruned; tenant 2 is 1s old and kept.
        let map = inner.map.lock().unwrap();
        assert!(!map.contains_key(&1));
        assert!(map.contains_key(&2));
    }

    #[tokio::test]
    async fn resolve_rate_cap_cached_serves_from_cache_without_db() {
        // Pre-seed the global cache; the store is unreachable, so a hit on the
        // DB path would error — returning Ok proves the cache satisfied it.
        CACHE.set(9_000_001, Some(42), Some(true));
        let cap = resolve_rate_cap_cached(&disconnected_store(), 9_000_001)
            .await
            .expect("cache hit must not touch the DB");
        assert_eq!(cap, Some(42));
    }

    #[tokio::test]
    async fn resolve_rate_cap_cached_propagates_db_error_on_miss() {
        let err = resolve_rate_cap_cached(&disconnected_store(), 9_000_002).await;
        assert!(err.is_err(), "cache miss with unreachable DB must error");
    }

    #[tokio::test]
    async fn remote_models_cached_serves_from_cache_without_db() {
        CACHE.set(9_000_003, None, Some(true));
        assert!(is_remote_models_allowed_cached(&disconnected_store(), 9_000_003).await);
    }

    #[tokio::test]
    async fn remote_models_fails_closed_on_miss_with_unreachable_db() {
        // Miss + DB error → deny remote models (fail closed).
        assert!(!is_remote_models_allowed_cached(&disconnected_store(), 9_000_004).await);
    }

    #[tokio::test]
    async fn remote_models_cached_none_flag_falls_back_to_db() {
        // An entry whose remote flag is None (never resolved) must not be
        // treated as an answer — with an unreachable DB that means `false`.
        CACHE.set(9_000_005, Some(10), None);
        assert!(!is_remote_models_allowed_cached(&disconnected_store(), 9_000_005).await);
    }

    #[test]
    fn cleanup_smoke() {
        CACHE.set(9_000_006, Some(1), None);
        billing_cache_cleanup();
        // Freshly inserted entry survives a cleanup pass.
        assert_eq!(CACHE.get(9_000_006), Some((Some(1), None)));
    }
}
