// SPDX-License-Identifier: AGPL-3.0-or-later

//! Hot-reload for the DB-driven routing table (plan §26.5, P9-T7).
//!
//! [`Reloader`] bridges a [`Pool`](crate::Pool) with the source of routing
//! entries (Postgres) and a notification mechanism
//! ([`RoutingChangeNotifier`]). It performs the initial load at startup and
//! then runs a background loop that re-applies the table on change.
//!
//! When the database has no routing entries the pool simply stays in legacy
//! flat-priority mode, serving the config `[[backend]]`s directly. (The old
//! config→routing-entries bootstrap was removed with BUG-SCHED-03: its
//! synthetic numeric model ids could never resolve to pool backends, so it
//! only ever produced an unusable table that the first reload pass cleared.)

use anyhow::Context;
use kb_core::routing::{RoutingChangeNotifier, RoutingEntry};

use crate::pool::Pool;

/// Manages hot-reload of the DB-driven routing table on a [`Pool`].
///
/// Built at startup, it applies the initial DB snapshot and then runs a
/// notification-driven reload loop in the background. Every snapshot goes
/// through [`Pool::apply_routing`], which materializes the routed models
/// into live backends before activating the table.
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

    /// Apply the startup snapshot of DB routing entries.
    ///
    /// A non-empty snapshot activates tiered routing (with its models
    /// materialized as live backends); an empty one leaves the pool in
    /// legacy flat-priority mode.
    pub fn startup_load(&self, entries: Vec<RoutingEntry>) {
        self.pool.apply_routing(entries);
    }

    /// Run the hot-reload background loop.
    ///
    /// The loop:
    /// 1. Awaits [`RoutingChangeNotifier::wait_for_change`] (first call
    ///    returns immediately for the initial load).
    /// 2. Calls `fetch_fn()` to get the latest routing entries from the
    ///    source of truth.
    /// 3. Applies them via [`Pool::apply_routing`] (an empty snapshot
    ///    reverts the pool to legacy flat-priority mode).
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

            self.pool.apply_routing(entries);
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use super::*;
    use crate::backend::test_backend;
    use crate::pool::Pool;
    use kb_core::data_class::DataClass;
    use kb_core::role::Role;
    use kb_core::routing::{ModelRow, ProviderKind, ProviderRow, RoutingStrategy};

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

    /// A DB-shaped routing entry: numeric model id, OpenAI-compat keyless
    /// provider — exactly what `get_routing_table` returns.
    fn db_entry(role: Role, model_id: i64, endpoint: &str) -> RoutingEntry {
        RoutingEntry {
            tenant_id: None,
            role,
            tier: 0,
            strategy: RoutingStrategy::LeastLoaded,
            spill_ms: 500,
            provider: ProviderRow {
                id: 1000 + model_id,
                name: format!("p-{model_id}"),
                kind: ProviderKind::OpenAiCompat,
                endpoint: Some(endpoint.to_string()),
                headers: serde_json::Value::Object(Default::default()),
                enabled: true,
                api_key_enc: None,
            },
            model: ModelRow {
                id: model_id,
                provider_id: 1000 + model_id,
                model_id: format!("m-{model_id}"),
                caps: vec![role.as_str().to_string()],
                ctx_tokens: None,
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

    // ── Reloader::startup_load ─────────────────────────────────────────────────

    /// The startup snapshot must not only activate routing — its models must
    /// be **materialized** so acquire actually resolves them. (The assertion
    /// that would have caught BUG-SCHED-03: the old test only checked
    /// `has_routing()`.)
    #[tokio::test]
    async fn startup_load_with_db_entries_activates_and_materializes() {
        let b = tb("cfg-a", 2);
        let pool = Pool::new(vec![Arc::clone(&b)], Duration::from_secs(5));
        let reloader = Reloader::new(pool.clone());

        reloader.startup_load(vec![db_entry(Role::Text, 1, "http://db:1")]);
        assert!(pool.has_routing(), "routing table must be set");

        let lease = pool.acquire(Role::Text, false, 0).await.unwrap();
        assert_eq!(
            lease.backend_id, "db:1",
            "acquire must resolve the materialized DB backend, not fall back"
        );
        assert_eq!(lease.endpoint, "http://db:1");
    }

    /// Empty DB snapshot → explicit legacy mode, and the config backends keep
    /// serving (the config→entries bootstrap is gone).
    #[tokio::test]
    async fn startup_load_empty_db_stays_legacy_and_acquire_works() {
        let b = tb("cfg-a", 2);
        let pool = Pool::new(vec![Arc::clone(&b)], Duration::from_secs(5));
        let reloader = Reloader::new(pool.clone());

        reloader.startup_load(vec![]);
        assert!(!pool.has_routing(), "no routing when the DB is empty");

        let lease = pool.acquire(Role::Text, false, 0).await.unwrap();
        assert_eq!(lease.backend_id, "cfg-a", "legacy pool serves as before");
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
                        Ok(vec![db_entry(
                            Role::Text,
                            n as i64,
                            &format!("http://x:r{n}"),
                        )])
                    }
                })
                .await
        });

        // Give the loop time to process 3 notifications.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // The pool should have routing set (the loop reloaded at least once)
        // AND the routed model must actually serve acquires.
        assert!(pool.has_routing(), "routing must be set after first reload");
        let fc = fetch_count.load(Ordering::SeqCst);
        assert!(fc >= 1, "fetch was called at least once, got {fc}");

        let lease = pool.acquire(Role::Text, false, 0).await.unwrap();
        assert!(
            lease.backend_id.starts_with("db:"),
            "post-reload acquire must use a materialized DB backend, got {}",
            lease.backend_id
        );

        // The run loop is still running (waiting on the 4th notification).
        // Abort it.
        handle.abort();
    }

    /// When the routes table empties out, the loop reverts the pool to
    /// legacy mode and prunes the materialized backends.
    #[tokio::test]
    async fn run_loop_empty_fetch_reverts_to_legacy_and_prunes() {
        let b = tb("r2", 2);
        let pool = Pool::new(vec![Arc::clone(&b)], Duration::from_secs(5));
        let reloader = Reloader::new(pool.clone());

        let fetch_count = Arc::new(AtomicUsize::new(0));
        let fetch_count2 = Arc::clone(&fetch_count);
        let notifier = CountingNotifier::new(2); // entries, then empty

        let reloader2 = reloader.clone();
        let handle = tokio::spawn(async move {
            reloader2
                .run(notifier, move || {
                    let fc = Arc::clone(&fetch_count2);
                    async move {
                        let n = fc.fetch_add(1, Ordering::SeqCst);
                        if n == 0 {
                            Ok(vec![db_entry(Role::Text, 42, "http://x:r42")])
                        } else {
                            Ok(vec![]) // routes deleted
                        }
                    }
                })
                .await
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        assert!(!pool.has_routing(), "empty snapshot must clear routing");
        assert!(
            pool.find_backend("db:42").is_none(),
            "materialized backend must be pruned with the table"
        );
        let lease = pool.acquire(Role::Text, false, 0).await.unwrap();
        assert_eq!(lease.backend_id, "r2", "legacy pool serves again");

        handle.abort();
    }

    // ── Pool routing swap is contention-free ──────────────────────────────────

    #[tokio::test]
    async fn pool_routing_swap_is_contention_free_under_acquire() {
        let b = tb("swap-me", 4);
        let pool = Pool::new(vec![Arc::clone(&b)], Duration::from_secs(5));
        let reloader = Reloader::new(pool.clone());

        // Set initial routing.
        reloader.startup_load(vec![db_entry(Role::Text, 1, "http://x:swap-me")]);
        assert!(pool.has_routing());

        // Concurrent acquire + routing swap must not panic or deadlock.
        let pool2 = pool.clone();
        let acquire_task = tokio::spawn(async move {
            for _ in 0..10 {
                let lease = pool2.acquire(Role::Text, false, 0).await;
                if let Ok(l) = lease {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    drop(l);
                }
            }
        });

        // Swap the routing table several times while acquires are in-flight.
        for i in 0..5 {
            pool.apply_routing(vec![db_entry(Role::Text, i, "http://x:swap-me")]);
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

        // Apply routing via r2, check via r1.
        r2.startup_load(vec![db_entry(Role::Text, 1, "http://x")]);
        assert!(r1.pool.has_routing());
    }
}
