// SPDX-License-Identifier: AGPL-3.0-or-later

//! Materialize live [`Backend`]s from DB routing entries (BUG-SCHED-03).
//!
//! The routing table (plan §26.5) references models by their numeric DB id,
//! while the pool's flat backend map is keyed by config string ids. This
//! module bridges the two: at routing-apply time every routed model is
//! upserted into the flat map under the canonical [`db_backend_key`], which
//! is exactly the key the tiered resolver looks up
//! ([`crate::tiered`]'s `resolve_and_filter`).
//!
//! Scope honesty: the live LLM client speaks raw OpenAI-compatible HTTP to
//! the lease endpoint and cannot attach provider API keys, so only
//! `Local`/`OpenAiCompat` providers **without** an encrypted key are
//! materialized. Routes to other providers resolve to nothing and fall back
//! to the legacy config pool per role (see [`crate::Pool::acquire`]).
//!
//! Capacity sharing: a DB provider pointing at the same physical server as a
//! config `[[backend]]` gets its **own** semaphore (no endpoint-equality
//! dedup) — concurrent legacy + tiered use can over-subscribe that server,
//! which inference servers tolerate by queueing. Recorded as a ledger note.

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use dashmap::DashMap;
use kb_core::capability::CapSet;
use kb_core::pricing::Pricing;
use kb_core::role::Role;
use kb_core::routing::{ModelRow, ProviderKind, RoutingEntry};

use crate::backend::Backend;
use crate::capacity::{Capacity, WallClock};

/// Concurrency for a DB model that declares no `max_conc` — one in-flight
/// request, the llama.cpp default (`--parallel 1`).
const DEFAULT_DB_SLOTS: usize = 1;

/// Stand-in per-minute limit when a model declares only one of RPM/TPM.
/// Large enough to never gate; finite so bucket math stays NaN-free.
const UNLIMITED_PER_MIN: f64 = 1e12;

/// Canonical flat-map key for a DB-materialized backend: `"db:{models.id}"`.
///
/// The prefix keeps DB-materialized backends disjoint from config backend
/// ids and makes pruning and log attribution unambiguous. The `db:` prefix is
/// **reserved**: `kb_config::load::validate` rejects any config `[[backend]]`
/// id that starts with it, so [`sync_backends`]/[`prune_all`] can safely
/// evict every `db:`-keyed entry without ever touching a config backend.
pub(crate) fn db_backend_key(model_id: i64) -> String {
    format!("db:{model_id}")
}

/// Whether `key` names a DB-materialized backend (see [`db_backend_key`]).
pub(crate) fn is_db_backend_key(key: &str) -> bool {
    key.starts_with("db:")
}

/// Capability set for a routed model: the model row's `caps` strings, with
/// unknown capability names skipped. An empty result falls back to the
/// route's own role so the backend is never capability-less.
fn capset_for(entry: &RoutingEntry) -> CapSet {
    let roles: Vec<Role> = entry
        .model
        .caps
        .iter()
        .filter_map(|c| Role::from_str(c).ok())
        .collect();
    if roles.is_empty() {
        CapSet::from(entry.role)
    } else {
        CapSet::from(roles)
    }
}

/// Concurrency for a model row (`max_conc`, defaulting to
/// [`DEFAULT_DB_SLOTS`]).
fn conc_for(model: &ModelRow) -> usize {
    model
        .max_conc
        .map_or(DEFAULT_DB_SLOTS, |c| usize::try_from(c).unwrap_or(1).max(1))
}

/// Capacity guard for a model row: `Rated` when the row documents RPM or TPM
/// limits, plain `Slots` otherwise.
fn capacity_for(model: &ModelRow) -> Capacity {
    let conc = conc_for(model);
    if model.rpm.is_some() || model.tpm.is_some() {
        Capacity::new_rated(
            conc,
            model.rpm.map_or(UNLIMITED_PER_MIN, f64::from),
            model.tpm.map_or(UNLIMITED_PER_MIN, f64::from),
            Arc::new(WallClock),
        )
    } else {
        Capacity::new_slots(conc)
    }
}

/// Pricing for a model row; `None` when neither price column is set.
fn pricing_for(model: &ModelRow) -> Option<Pricing> {
    if model.price_in.is_none() && model.price_out.is_none() {
        None
    } else {
        Some(Pricing::new(
            model.price_in.unwrap_or(0.0),
            model.price_out.unwrap_or(0.0),
        ))
    }
}

/// Whether the live client can serve this entry's provider at all:
/// `Local`/`OpenAiCompat` kind, an endpoint, and no API key (the raw
/// OpenAI-compat client attaches no auth).
fn servable(entry: &RoutingEntry) -> bool {
    matches!(
        entry.provider.kind,
        ProviderKind::Local | ProviderKind::OpenAiCompat
    ) && entry.provider.endpoint.is_some()
        && entry.provider.api_key_enc.is_none()
}

/// Build one live [`Backend`] from a routing entry, or `None` (with a
/// warning) when the live client cannot serve this provider — the
/// [`servable`] conditions, each with its own operator-facing warning.
pub(crate) fn materialize_backend(entry: &RoutingEntry) -> Option<Backend> {
    let provider = &entry.provider;
    let model = &entry.model;

    if !matches!(
        provider.kind,
        ProviderKind::Local | ProviderKind::OpenAiCompat
    ) {
        tracing::warn!(
            provider = %provider.name,
            kind = %provider.kind,
            model = %model.model_id,
            "routing entry skipped: provider kind is not served by the live \
             OpenAI-compat client; its routes fall back to legacy backends"
        );
        return None;
    }
    let Some(endpoint) = provider.endpoint.as_deref() else {
        tracing::warn!(
            provider = %provider.name,
            model = %model.model_id,
            "routing entry skipped: provider has no endpoint"
        );
        return None;
    };
    if provider.api_key_enc.is_some() {
        tracing::warn!(
            provider = %provider.name,
            model = %model.model_id,
            "routing entry skipped: provider requires an API key, which the \
             live client cannot attach yet; its routes fall back to legacy backends"
        );
        return None;
    }

    Some(Backend::new(
        db_backend_key(model.id),
        Arc::new(kb_core::provider::NoopAdapter),
        endpoint,
        None, /* key — keyed providers are skipped above */
        model.model_id.clone(),
        capset_for(entry),
        capacity_for(model),
        conc_for(model),
        pricing_for(model),
        model.data_class,
        0, /* priority — ordering comes from tiers/strategy, not this field */
        model.ctx_tokens.map(i64::from),
    ))
}

/// Whether an existing materialized backend still matches the entry's
/// definition, so its `Arc` (semaphore permits, in-flight state) can be
/// preserved across a routing reload.
fn definition_matches(existing: &Backend, entry: &RoutingEntry) -> bool {
    let model = &entry.model;
    // A provider that became unservable (e.g. it gained an API key) must not
    // keep its previously materialized backend alive.
    if !servable(entry) {
        return false;
    }
    if existing.endpoint.as_deref() != entry.provider.endpoint.as_deref()
        || existing.model_id != model.model_id
        || existing.data_class != model.data_class
        || existing.pricing != pricing_for(model)
        || existing.caps.bits() != capset_for(entry).bits()
        || existing.max_concurrency != conc_for(model)
        || existing.ctx_tokens != model.ctx_tokens.map(i64::from)
    {
        return false;
    }
    let wants_rated = model.rpm.is_some() || model.tpm.is_some();
    match &existing.capacity {
        Capacity::Slots { .. } => !wants_rated,
        Capacity::Rated { rpm, tpm, .. } => {
            wants_rated
                && rpm.max_tokens() == model.rpm.map_or(UNLIMITED_PER_MIN, f64::from)
                && tpm.max_tokens() == model.tpm.map_or(UNLIMITED_PER_MIN, f64::from)
        }
        Capacity::Concurrency { .. } => false, // never materialized here
    }
}

/// Upsert the flat backend map against a fresh routing snapshot.
///
/// - Unchanged definitions keep their existing `Arc<Backend>` (live
///   semaphore state survives the reload).
/// - Changed definitions are rebuilt (old permits drain safely on the old
///   `Arc`; a brief over-subscription window is possible and accepted).
/// - Every desired backend — preserved or rebuilt — has `healthy` reset to
///   `true`: an admin routing change (or the degraded-mode poll) is the
///   deliberate retry signal for backends the client marked dead, since the
///   health loop does not probe materialized backends.
/// - Stale `db:`-keyed entries are pruned. Config-keyed backends are never
///   touched.
pub(crate) fn sync_backends(map: &DashMap<String, Arc<Backend>>, entries: &[RoutingEntry]) {
    // Dedupe by model id — the same model may appear in several routes; all
    // such entries carry the same provider+model rows (one DB row each).
    let mut desired: HashMap<String, &RoutingEntry> = HashMap::new();
    for entry in entries {
        desired
            .entry(db_backend_key(entry.model.id))
            .or_insert(entry);
    }

    for (key, entry) in &desired {
        let preserved = map.get(key).is_some_and(|existing| {
            if definition_matches(existing.value(), entry) {
                existing
                    .value()
                    .healthy
                    .store(true, std::sync::atomic::Ordering::Release);
                true
            } else {
                false
            }
        });
        if !preserved {
            match materialize_backend(entry) {
                Some(backend) => {
                    map.insert(key.clone(), Arc::new(backend));
                }
                // Unservable provider — make sure no stale backend lingers
                // under this key (e.g. the provider just gained an API key).
                None => {
                    map.remove(key);
                }
            }
        }
    }

    map.retain(|key, _| !is_db_backend_key(key) || desired.contains_key(key));
}

/// Remove every DB-materialized backend (used when the routing table is
/// cleared). Config-keyed backends are never touched.
pub(crate) fn prune_all(map: &DashMap<String, Arc<Backend>>) {
    map.retain(|key, _| !is_db_backend_key(key));
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use std::sync::atomic::Ordering;

    use super::*;
    use kb_core::data_class::DataClass;
    use kb_core::routing::{ModelRow, ProviderRow, RoutingStrategy};

    /// A baseline OpenAI-compat, keyless, endpoint-bearing entry.
    fn entry(role: Role, model_id: i64, endpoint: &str) -> RoutingEntry {
        RoutingEntry {
            tenant_id: None,
            role,
            tier: 0,
            strategy: RoutingStrategy::LeastLoaded,
            spill_ms: 800,
            provider: ProviderRow {
                id: 1000 + model_id,
                name: format!("prov-{model_id}"),
                kind: ProviderKind::OpenAiCompat,
                endpoint: Some(endpoint.to_string()),
                headers: serde_json::Value::Object(Default::default()),
                enabled: true,
                api_key_enc: None,
            },
            model: ModelRow {
                id: model_id,
                provider_id: 1000 + model_id,
                model_id: format!("model-{model_id}"),
                caps: vec![role.as_str().to_string()],
                ctx_tokens: Some(8192),
                max_conc: Some(2),
                rpm: None,
                tpm: None,
                price_in: None,
                price_out: None,
                data_class: DataClass::Local,
                enabled: true,
            },
        }
    }

    #[test]
    fn db_backend_key_format_and_no_config_collision() {
        assert_eq!(db_backend_key(12), "db:12");
        assert!(is_db_backend_key("db:12"));
        // A bare numeric model id (the original buggy lookup) is NOT a db key,
        // and neither are config-style ids.
        assert!(!is_db_backend_key("12"));
        assert!(!is_db_backend_key("llama-text"));
    }

    #[test]
    fn capacity_for_max_conc_yields_slots() {
        let e = entry(Role::Text, 1, "http://x");
        let cap = capacity_for(&e.model);
        assert!(matches!(cap, Capacity::Slots { .. }));
        assert_eq!(cap.free(), 2, "slots == max_conc");
    }

    #[test]
    fn capacity_for_rpm_yields_rated() {
        let mut e = entry(Role::Text, 1, "http://x");
        e.model.rpm = Some(60);
        let cap = capacity_for(&e.model);
        match &cap {
            Capacity::Rated { rpm, tpm, .. } => {
                assert_eq!(rpm.max_tokens(), 60.0);
                assert_eq!(
                    tpm.max_tokens(),
                    UNLIMITED_PER_MIN,
                    "missing tpm is unlimited"
                );
            }
            other => panic!("expected Rated, got {other:?}"),
        }
        assert_eq!(cap.free(), 2, "concurrency still from max_conc");
    }

    #[test]
    fn capacity_for_default_is_one_slot() {
        let mut e = entry(Role::Text, 1, "http://x");
        e.model.max_conc = None;
        let cap = capacity_for(&e.model);
        assert!(matches!(cap, Capacity::Slots { .. }));
        assert_eq!(cap.free(), DEFAULT_DB_SLOTS);
    }

    #[test]
    fn capset_parses_caps_and_falls_back_to_route_role() {
        let mut e = entry(Role::Text, 1, "http://x");
        e.model.caps = vec!["text".into(), "embed".into(), "bogus".into()];
        let caps = capset_for(&e);
        assert!(caps.supports(Role::Text));
        assert!(caps.supports(Role::Embed));
        assert!(!caps.supports(Role::Vision));

        // All-unknown caps → the route's role.
        e.model.caps = vec!["bogus".into()];
        e.role = Role::Rerank;
        let caps = capset_for(&e);
        assert!(caps.supports(Role::Rerank));
        assert_eq!(caps.len(), 1);
    }

    #[test]
    fn materialize_maps_fields() {
        let mut e = entry(Role::Text, 7, "http://srv:9000/v1");
        e.model.price_in = Some(1.5);
        e.model.price_out = Some(3.0);
        e.model.data_class = DataClass::Remote;

        let b = materialize_backend(&e).expect("materializable entry");
        assert_eq!(b.id, "db:7");
        assert_eq!(b.endpoint.as_deref(), Some("http://srv:9000/v1"));
        assert_eq!(b.model_id, "model-7");
        assert_eq!(b.max_concurrency, 2);
        assert_eq!(b.pricing, Some(Pricing::new(1.5, 3.0)));
        assert_eq!(b.data_class, DataClass::Remote);
        assert!(b.healthy.load(Ordering::Acquire));
    }

    #[test]
    fn materialize_skips_keyed_and_native_providers() {
        // Native-SDK kinds are not served by the raw OpenAI-compat client.
        let mut e = entry(Role::Text, 1, "http://x");
        e.provider.kind = ProviderKind::Anthropic;
        assert!(materialize_backend(&e).is_none());

        let mut e = entry(Role::Text, 1, "http://x");
        e.provider.kind = ProviderKind::GeminiNative;
        assert!(materialize_backend(&e).is_none());

        // No endpoint.
        let mut e = entry(Role::Text, 1, "http://x");
        e.provider.endpoint = None;
        assert!(materialize_backend(&e).is_none());

        // Encrypted API key present — the client cannot attach auth.
        let mut e = entry(Role::Text, 1, "http://x");
        e.provider.api_key_enc = Some(vec![1, 2, 3]);
        assert!(materialize_backend(&e).is_none());
    }

    #[test]
    fn sync_preserves_unchanged_backend_arc() {
        let map: DashMap<String, Arc<Backend>> = DashMap::new();
        let e = entry(Role::Text, 3, "http://x:1");
        sync_backends(&map, std::slice::from_ref(&e));

        let first = Arc::clone(map.get("db:3").unwrap().value());
        // Hold a permit and mark unhealthy — live state that must survive.
        let permit = first.capacity.try_acquire().unwrap();
        first.healthy.store(false, Ordering::Release);

        sync_backends(&map, std::slice::from_ref(&e));
        let second = Arc::clone(map.get("db:3").unwrap().value());

        assert!(
            Arc::ptr_eq(&first, &second),
            "unchanged definition keeps the Arc"
        );
        assert_eq!(second.free(), 1, "held permit still accounted");
        assert!(
            second.healthy.load(Ordering::Acquire),
            "apply resets healthy as the deliberate retry signal"
        );
        drop(permit);
    }

    #[test]
    fn sync_rebuilds_on_endpoint_change() {
        let map: DashMap<String, Arc<Backend>> = DashMap::new();
        let e = entry(Role::Text, 3, "http://x:1");
        sync_backends(&map, std::slice::from_ref(&e));
        let first = Arc::clone(map.get("db:3").unwrap().value());

        let moved = entry(Role::Text, 3, "http://x:2");
        sync_backends(&map, std::slice::from_ref(&moved));
        let second = Arc::clone(map.get("db:3").unwrap().value());

        assert!(!Arc::ptr_eq(&first, &second), "changed endpoint rebuilds");
        assert_eq!(second.endpoint.as_deref(), Some("http://x:2"));
    }

    #[test]
    fn sync_rebuilds_on_ctx_tokens_change() {
        let map: DashMap<String, Arc<Backend>> = DashMap::new();
        let e = entry(Role::Text, 3, "http://x:1");
        sync_backends(&map, std::slice::from_ref(&e));
        let first = Arc::clone(map.get("db:3").unwrap().value());

        let mut bigger = e.clone();
        bigger.model.ctx_tokens = Some(131072);
        sync_backends(&map, std::slice::from_ref(&bigger));
        let second = Arc::clone(map.get("db:3").unwrap().value());

        assert!(!Arc::ptr_eq(&first, &second), "changed ctx_tokens rebuilds");
        assert_eq!(second.ctx_tokens, Some(131072));
    }

    #[test]
    fn sync_prunes_stale_db_keys_only() {
        let map: DashMap<String, Arc<Backend>> = DashMap::new();
        // A config backend that must never be touched.
        map.insert(
            "llama-text".into(),
            Arc::new(crate::backend::test_backend(
                "llama-text",
                "http://cfg",
                vec![Role::Text],
                0,
                2,
            )),
        );
        sync_backends(
            &map,
            &[
                entry(Role::Text, 1, "http://x:1"),
                entry(Role::Embed, 2, "http://x:2"),
            ],
        );
        assert!(map.contains_key("db:1") && map.contains_key("db:2"));

        // Next snapshot drops model 2.
        sync_backends(&map, &[entry(Role::Text, 1, "http://x:1")]);
        assert!(map.contains_key("db:1"));
        assert!(!map.contains_key("db:2"), "stale db backend pruned");
        assert!(map.contains_key("llama-text"), "config backend untouched");
    }

    #[test]
    fn sync_removes_backend_that_became_unservable() {
        let map: DashMap<String, Arc<Backend>> = DashMap::new();
        let e = entry(Role::Text, 4, "http://x:1");
        sync_backends(&map, std::slice::from_ref(&e));
        assert!(map.contains_key("db:4"));

        // The provider gains an API key — no longer servable.
        let mut keyed = e.clone();
        keyed.provider.api_key_enc = Some(vec![9]);
        sync_backends(&map, std::slice::from_ref(&keyed));
        assert!(!map.contains_key("db:4"), "unservable backend removed");
    }

    #[test]
    fn prune_all_removes_only_db_keys() {
        let map: DashMap<String, Arc<Backend>> = DashMap::new();
        map.insert(
            "cfg".into(),
            Arc::new(crate::backend::test_backend(
                "cfg",
                "http://cfg",
                vec![Role::Text],
                0,
                1,
            )),
        );
        sync_backends(&map, &[entry(Role::Text, 9, "http://x")]);
        assert!(map.contains_key("db:9"));

        prune_all(&map);
        assert!(!map.contains_key("db:9"));
        assert!(map.contains_key("cfg"));
    }
}
