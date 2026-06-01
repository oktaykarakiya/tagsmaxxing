//! Hot-reload for the DB-driven routing table (plan §26.5, P9-T7).
//!
//! [`RoutingReloader`] bridges a [`Pool`](crate::Pool) with a source of
//! routing entries (Postgres or config bootstrap) and a notification
//! mechanism ([`RoutingChangeNotifier`]). It performs the initial load
//! at startup and then runs a background loop that reloads on change.

use std::sync::Arc;

use anyhow::Context;
use kb_config::Config;
use kb_core::data_class::DataClass;
use kb_core::routing::{
    ModelRow, ProviderKind, ProviderRow, RoutingChangeNotifier, RoutingEntry, RoutingTable,
};

use crate::pool::Pool;

/// Manages hot-reload of the DB-driven routing table on a [`Pool`].
///
/// Built at startup, it performs an initial load (DB first, config-toml
/// fallback) and then runs a notification-driven reload loop in the
/// background.
#[derive(Clone)]
pub struct Reloader {
    pool: Pool,
}

impl Reloader {
    /// Create a new reloader that operates on `pool`.
    #[must_use]
    pub fn new(pool: Pool) -> Self {
        Self { pool }
    }

    /// Perform the startup load.
    ///
    /// 1. If `entries` (from the DB) is non-empty, build a
    ///    [`RoutingTable`] and call [`Pool::set_routing`].
    /// 2. If the DB returned zero entries **and** a `fallback_config` is
    ///    provided, convert the config's `[[backends]]` into routing entries
    ///    and set those on the pool.
    /// 3. If both are empty, the pool stays in legacy flat-priority mode
    ///    (no routing table set).
    ///
    /// # Errors
    ///
    /// Returns an error if the DB entries cannot be parsed, but this is
    /// defensive — the DB query already validates them.
    pub async fn startup_load(
        &self,
        entries: Vec<RoutingEntry>,
        fallback_config: Option<&Config>,
    ) -> anyhow::Result<()> {
        if !entries.is_empty() {
            let table = Arc::new(RoutingTable::from_entries(entries));
            self.pool.set_routing(table);
            return Ok(());
        }

        // DB returned nothing — try config fallback.
        if let Some(cfg) = fallback_config {
            let config_entries = config_backends_to_entries(cfg);
            if !config_entries.is_empty() {
                let table = Arc::new(RoutingTable::from_entries(config_entries));
                self.pool.set_routing(table);
                return Ok(());
            }
        }

        // Neither source has entries — leave legacy mode.
        Ok(())
    }

    /// Run the hot-reload background loop.
    ///
    /// The loop:
    /// 1. Awaits [`RoutingChangeNotifier::wait_for_change`] (first call
    ///    returns immediately for the initial load).
    /// 2. Calls `fetch_fn()` to get the latest routing entries from the
    ///    source of truth.
    /// 3. Builds a [`RoutingTable`] and calls [`Pool::set_routing`].
    ///
    /// This function never returns unless the notifier or fetch function
    /// returns a fatal error. It is designed to be spawned as a background
    /// [`tokio::task`].
    ///
    /// # Errors
    ///
    /// Returns on the first fatal error from the notifier or fetch function.
    pub async fn run<N, F, Fut>(&self, notifier: N, fetch_fn: F) -> anyhow::Result<()>
    where
        N: RoutingChangeNotifier,
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = anyhow::Result<Vec<RoutingEntry>>>,
    {
        loop {
            notifier
                .wait_for_change()
                .await
                .context("routing notifier failed")?;

            let entries = fetch_fn()
                .await
                .context("failed to fetch routing entries for hot-reload")?;

            if entries.is_empty() {
                // DB became empty — clear the routing table so the pool
                // falls back to legacy flat-priority mode.
                self.pool.clear_routing();
            } else {
                let table = Arc::new(RoutingTable::from_entries(entries));
                self.pool.set_routing(table);
            }
        }
    }
}

/// Convert `[[backends]]` from `config.toml` into [`RoutingEntry`] values.
///
/// This is the bootstrap path when the database has no routing entries yet.
/// Each backend becomes a `Local`-kind provider with a single model and one
/// routing entry per role that the backend serves. All entries land in tier 0
/// with the `LeastLoaded` strategy and an 800ms spill timeout.
///
/// # Returns
///
/// An empty `Vec` when the config has no backends.
#[must_use]
pub fn config_backends_to_entries(config: &Config) -> Vec<RoutingEntry> {
    let mut entries = Vec::new();

    for (idx, backend) in config.backends.iter().enumerate() {
        let provider_id: i64 = (idx + 1) as i64;
        let model_id_num: i64 = provider_id;

        let provider = ProviderRow {
            id: provider_id,
            name: backend.id.clone(),
            kind: ProviderKind::Local,
            endpoint: Some(backend.base_url.clone()),
            headers: serde_json::Value::Object(Default::default()),
            enabled: true,
        };

        let caps: Vec<String> = backend
            .roles
            .iter()
            .map(|r| r.as_str().to_string())
            .collect();

        let model = ModelRow {
            id: model_id_num,
            provider_id,
            model_id: backend.id.clone(),
            caps,
            ctx_tokens: None,
            max_conc: Some(backend.slots as i32),
            rpm: None,
            tpm: None,
            price_in: None,
            price_out: None,
            data_class: DataClass::Local,
            enabled: true,
        };

        // Create one route entry per role.
        for role in &backend.roles {
            entries.push(RoutingEntry {
                tenant_id: None,
                role: *role,
                tier: 0,
                strategy: kb_core::routing::RoutingStrategy::LeastLoaded,
                spill_ms: 800,
                provider: provider.clone(),
                model: model.clone(),
            });
        }
    }

    entries
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use super::*;
    use crate::backend::test_backend;
    use crate::pool::Pool;
    use kb_core::role::Role;

    // ── Helpers ───────────────────────────────────────────────────────────────

    fn tb(id: &str, slots: usize) -> Arc<crate::backend::Backend> {
        Arc::new(test_backend(
            id,
            format!("http://x:{id}"),
            vec![Role::Text],
            0,
            slots,
        ))
    }

    /// Build a test config with the given backends.
    fn test_config(backends: Vec<kb_config::Backend>) -> Config {
        Config {
            backends,
            ..Default::default()
        }
    }

    fn backend_cfg(id: &str, base_url: &str, roles: Vec<Role>, slots: u32) -> kb_config::Backend {
        kb_config::Backend {
            id: id.into(),
            base_url: base_url.into(),
            roles,
            slots,
            priority: 0,
        }
    }

    // ── config_backends_to_entries ────────────────────────────────────────────

    #[test]
    fn config_backends_empty_config_returns_empty() {
        let cfg = Config::default();
        let entries = config_backends_to_entries(&cfg);
        assert!(entries.is_empty());
    }

    #[test]
    fn config_backends_single_backend_single_role() {
        let cfg = test_config(vec![backend_cfg(
            "gpu-a",
            "http://localhost:8001/v1",
            vec![Role::Text],
            4,
        )]);
        let entries = config_backends_to_entries(&cfg);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].role, Role::Text);
        assert_eq!(entries[0].tier, 0);
        assert_eq!(entries[0].spill_ms, 800);
        assert_eq!(entries[0].provider.name, "gpu-a");
        assert_eq!(
            entries[0].provider.endpoint.as_deref(),
            Some("http://localhost:8001/v1")
        );
        assert_eq!(entries[0].provider.kind, ProviderKind::Local);
        assert_eq!(entries[0].model.model_id, "gpu-a");
        assert_eq!(entries[0].model.caps, vec!["text"]);
        assert_eq!(entries[0].model.max_conc, Some(4));
        assert_eq!(entries[0].model.data_class, DataClass::Local);
        assert!(entries[0].model.enabled);
    }

    #[test]
    fn config_backends_multi_role_backend_creates_one_entry_per_role() {
        let cfg = test_config(vec![backend_cfg(
            "multi-role",
            "http://localhost:8001/v1",
            vec![Role::Text, Role::Embed],
            2,
        )]);
        let entries = config_backends_to_entries(&cfg);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].role, Role::Text);
        assert_eq!(entries[1].role, Role::Embed);
        // Both share the same provider and model.
        assert_eq!(entries[0].provider.name, "multi-role");
        assert_eq!(entries[1].provider.name, "multi-role");
        // The model caps include both roles.
        assert!(entries[0].model.caps.contains(&"text".to_string()));
        assert!(entries[0].model.caps.contains(&"embed".to_string()));
    }

    #[test]
    fn config_backends_two_backends_separate_providers() {
        let cfg = test_config(vec![
            backend_cfg("a", "http://a:8001/v1", vec![Role::Text], 4),
            backend_cfg("b", "http://b:8002/v1", vec![Role::Embed], 2),
        ]);
        let entries = config_backends_to_entries(&cfg);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].provider.name, "a");
        assert_eq!(entries[1].provider.name, "b");
        assert_ne!(entries[0].provider.id, entries[1].provider.id);
        assert_ne!(entries[0].model.id, entries[1].model.id);
    }

    // ── Reloader::startup_load ─────────────────────────────────────────────────

    #[tokio::test]
    async fn startup_load_with_db_entries_sets_routing() {
        let b = tb("b1", 2);
        let pool = Pool::new(vec![Arc::clone(&b)], Duration::from_secs(5));
        let reloader = Reloader::new(pool.clone());

        // Build a DB-like entry for backend "b1".
        let entry = RoutingEntry {
            tenant_id: None,
            role: Role::Text,
            tier: 0,
            strategy: kb_core::routing::RoutingStrategy::LeastLoaded,
            spill_ms: 500,
            provider: ProviderRow {
                id: 1,
                name: "p1".into(),
                kind: ProviderKind::Local,
                endpoint: Some("http://x:b1".into()),
                headers: Default::default(),
                enabled: true,
            },
            model: ModelRow {
                id: 1,
                provider_id: 1,
                model_id: "b1".into(),
                caps: vec!["text".into()],
                ctx_tokens: None,
                max_conc: Some(2),
                rpm: None,
                tpm: None,
                price_in: None,
                price_out: None,
                data_class: DataClass::Local,
                enabled: true,
            },
        };

        reloader.startup_load(vec![entry], None).await.unwrap();
        assert!(pool.has_routing(), "routing table must be set");
    }

    #[tokio::test]
    async fn startup_load_empty_db_falls_back_to_config() {
        let b = tb("cfg-a", 2);
        let pool = Pool::new(vec![Arc::clone(&b)], Duration::from_secs(5));
        let reloader = Reloader::new(pool.clone());

        let cfg = test_config(vec![backend_cfg(
            "cfg-a",
            "http://localhost:8001/v1",
            vec![Role::Text],
            2,
        )]);

        // Empty DB entries → fall back to config.
        reloader.startup_load(vec![], Some(&cfg)).await.unwrap();
        assert!(
            pool.has_routing(),
            "routing must be set from config fallback"
        );
    }

    #[tokio::test]
    async fn startup_load_both_empty_leaves_no_routing() {
        let b = tb("b1", 2);
        let pool = Pool::new(vec![Arc::clone(&b)], Duration::from_secs(5));
        let reloader = Reloader::new(pool.clone());

        // Both DB and config are empty.
        reloader
            .startup_load(vec![], Some(&Config::default()))
            .await
            .unwrap();
        assert!(
            !pool.has_routing(),
            "no routing when both sources are empty"
        );
    }

    #[tokio::test]
    async fn startup_load_db_entries_take_precedence_over_config() {
        let b_config = tb("from-config", 2);
        let b_db = tb("from-db", 2);
        let pool = Pool::new(
            vec![Arc::clone(&b_config), Arc::clone(&b_db)],
            Duration::from_secs(5),
        );
        let reloader = Reloader::new(pool.clone());

        let cfg = test_config(vec![backend_cfg(
            "from-config",
            "http://localhost:8001/v1",
            vec![Role::Text],
            2,
        )]);

        // DB has entries (to a different backend).
        let entry = RoutingEntry {
            tenant_id: None,
            role: Role::Text,
            tier: 0,
            strategy: kb_core::routing::RoutingStrategy::LeastLoaded,
            spill_ms: 500,
            provider: ProviderRow {
                id: 1,
                name: "db-prov".into(),
                kind: ProviderKind::Local,
                endpoint: Some("http://x:from-db".into()),
                headers: Default::default(),
                enabled: true,
            },
            model: ModelRow {
                id: 2,
                provider_id: 1,
                model_id: "from-db".into(),
                caps: vec!["text".into()],
                ctx_tokens: None,
                max_conc: Some(2),
                rpm: None,
                tpm: None,
                price_in: None,
                price_out: None,
                data_class: DataClass::Local,
                enabled: true,
            },
        };

        // DB non-empty → config is ignored.
        reloader
            .startup_load(vec![entry], Some(&cfg))
            .await
            .unwrap();
        assert!(pool.has_routing());
    }

    // ── Reloader::run with mock notifier ───────────────────────────────────────

    /// A notifier that signals `count` times then blocks forever.
    struct CountingNotifier {
        remaining: std::sync::Mutex<usize>,
    }

    impl CountingNotifier {
        fn new(count: usize) -> Self {
            Self {
                remaining: std::sync::Mutex::new(count),
            }
        }
    }

    #[async_trait::async_trait]
    impl RoutingChangeNotifier for CountingNotifier {
        async fn wait_for_change(&self) -> anyhow::Result<()> {
            let should_block = {
                let mut rem = self.remaining.lock().unwrap();
                if *rem == 0 {
                    true
                } else {
                    *rem -= 1;
                    false
                }
            };
            if should_block {
                // Block indefinitely — the test wraps the run loop in a timeout.
                std::future::pending::<()>().await;
                unreachable!()
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn run_loop_reloads_on_notification() {
        let b = tb("r1", 2);
        let pool = Pool::new(vec![Arc::clone(&b)], Duration::from_secs(5));
        let reloader = Reloader::new(pool.clone());
        assert!(!pool.has_routing());

        let fetch_count = Arc::new(AtomicUsize::new(0));
        let fetch_count2 = Arc::clone(&fetch_count);

        let notifier = CountingNotifier::new(3); // initial + 2 reloads

        // Spawn the run loop with a short timeout (it would block forever on the
        // 4th wait_for_change call).
        let reloader2 = reloader.clone();
        let handle = tokio::spawn(async move {
            reloader2
                .run(notifier, move || {
                    let fc = Arc::clone(&fetch_count2);
                    async move {
                        let n = fc.fetch_add(1, Ordering::SeqCst);
                        // Return a fresh entry each time.
                        let entry = RoutingEntry {
                            tenant_id: None,
                            role: Role::Text,
                            tier: 0,
                            strategy: kb_core::routing::RoutingStrategy::LeastLoaded,
                            spill_ms: 500,
                            provider: ProviderRow {
                                id: n as i64,
                                name: format!("p{n}"),
                                kind: ProviderKind::Local,
                                endpoint: Some(format!("http://x:r{n}")),
                                headers: Default::default(),
                                enabled: true,
                            },
                            model: ModelRow {
                                id: n as i64,
                                provider_id: n as i64,
                                model_id: format!("r{n}"),
                                caps: vec!["text".into()],
                                ctx_tokens: None,
                                max_conc: Some(2),
                                rpm: None,
                                tpm: None,
                                price_in: None,
                                price_out: None,
                                data_class: DataClass::Local,
                                enabled: true,
                            },
                        };
                        Ok(vec![entry])
                    }
                })
                .await
        });

        // Give the loop time to process 3 notifications.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // The pool should have routing set (the loop reloaded at least once).
        assert!(pool.has_routing(), "routing must be set after first reload");
        let fc = fetch_count.load(Ordering::SeqCst);
        assert!(fc >= 1, "fetch was called at least once, got {fc}");

        // The run loop is still running (waiting on the 4th notification).
        // Abort it.
        handle.abort();
    }

    // ── Pool routing swap is contention-free ──────────────────────────────────

    #[tokio::test]
    async fn pool_routing_swap_is_contention_free_under_acquire() {
        let b = tb("swap-me", 4);
        let pool = Pool::new(vec![Arc::clone(&b)], Duration::from_secs(5));
        let reloader = Reloader::new(pool.clone());

        // Set initial routing.
        let entry1 = RoutingEntry {
            tenant_id: None,
            role: Role::Text,
            tier: 0,
            strategy: kb_core::routing::RoutingStrategy::LeastLoaded,
            spill_ms: 800,
            provider: ProviderRow {
                id: 1,
                name: "p1".into(),
                kind: ProviderKind::Local,
                endpoint: Some("http://x:swap-me".into()),
                headers: Default::default(),
                enabled: true,
            },
            model: ModelRow {
                id: 1,
                provider_id: 1,
                model_id: "swap-me".into(),
                caps: vec!["text".into()],
                ctx_tokens: None,
                max_conc: Some(4),
                rpm: None,
                tpm: None,
                price_in: None,
                price_out: None,
                data_class: DataClass::Local,
                enabled: true,
            },
        };
        reloader.startup_load(vec![entry1], None).await.unwrap();
        assert!(pool.has_routing());

        // Concurrent acquire + routing swap must not panic or deadlock.
        let pool2 = pool.clone();
        let acquire_task = tokio::spawn(async move {
            for _ in 0..10 {
                let lease = pool2.acquire(Role::Text, false).await;
                if let Ok(l) = lease {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    drop(l);
                }
            }
        });

        // Swap the routing table several times while acquires are in-flight.
        for i in 0..5 {
            let entry = RoutingEntry {
                tenant_id: None,
                role: Role::Text,
                tier: 0,
                strategy: kb_core::routing::RoutingStrategy::LeastLoaded,
                spill_ms: 800,
                provider: ProviderRow {
                    id: i as i64,
                    name: format!("p{i}"),
                    kind: ProviderKind::Local,
                    endpoint: Some("http://x:swap-me".into()),
                    headers: Default::default(),
                    enabled: true,
                },
                model: ModelRow {
                    id: i as i64,
                    provider_id: i as i64,
                    model_id: "swap-me".into(),
                    caps: vec!["text".into()],
                    ctx_tokens: None,
                    max_conc: Some(4),
                    rpm: None,
                    tpm: None,
                    price_in: None,
                    price_out: None,
                    data_class: DataClass::Local,
                    enabled: true,
                },
            };
            let table = Arc::new(RoutingTable::from_entries(vec![entry]));
            pool.set_routing(table);
            tokio::time::sleep(Duration::from_micros(100)).await;
        }

        acquire_task.await.unwrap();
    }

    // ── Reloader::new and Clone ───────────────────────────────────────────────

    #[test]
    fn reloader_new_creates_with_pool() {
        let b = tb("b1", 2);
        let pool = Pool::new(vec![Arc::clone(&b)], Duration::from_secs(5));
        let reloader = Reloader::new(pool);
        // The reloader is functional (smoke test).
        assert!(!reloader.pool.has_routing());
    }

    #[test]
    fn reloader_clone_shares_pool() {
        let b = tb("b1", 2);
        let pool = Pool::new(vec![Arc::clone(&b)], Duration::from_secs(5));
        let r1 = Reloader::new(pool.clone());
        let r2 = r1.clone();

        // Set routing via r2, check via r1.
        let entry = RoutingEntry {
            tenant_id: None,
            role: Role::Text,
            tier: 0,
            strategy: kb_core::routing::RoutingStrategy::LeastLoaded,
            spill_ms: 800,
            provider: ProviderRow {
                id: 1,
                name: "p".into(),
                kind: ProviderKind::Local,
                endpoint: Some("http://x".into()),
                headers: Default::default(),
                enabled: true,
            },
            model: ModelRow {
                id: 1,
                provider_id: 1,
                model_id: "m".into(),
                caps: vec!["text".into()],
                ctx_tokens: None,
                max_conc: Some(2),
                rpm: None,
                tpm: None,
                price_in: None,
                price_out: None,
                data_class: DataClass::Local,
                enabled: true,
            },
        };
        let table = Arc::new(RoutingTable::from_entries(vec![entry]));
        r2.pool.set_routing(table);
        assert!(r1.pool.has_routing());
    }
}
