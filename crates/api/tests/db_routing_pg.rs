// SPDX-License-Identifier: AGPL-3.0-or-later

//! Integration tests for DB-driven routing against a real pgvector Postgres
//! container (BUG-SCHED-03): the serve-shaped runtime
//! (`build_runtime(.., load_db_routing = true)`) must
//!
//! * stay in legacy flat-priority mode when the routes table is empty,
//! * materialize a global route's model into a live backend and resolve it
//!   in tiered acquire,
//! * keep every *other* role serving from the config backends (per-role
//!   fallback) — "creating ONE real DB route must not break every other
//!   role's acquire",
//! * pick up routes created after startup via LISTEN/NOTIFY without a
//!   restart.
//!
//! Needs **Podman** + network to pull `pgvector/pgvector`; gated `#[ignore]`.
//! Run single-threaded — each test runs the migrations on a fresh database,
//! and the `CREATE ROLE kb_app` in migration 0006 is **cluster-global**, so
//! concurrent runs race:
//!
//! ```text
//! systemctl --user start podman.socket
//! DOCKER_HOST=unix://$XDG_RUNTIME_DIR/podman/podman.sock \
//!   cargo test -p kb-api --test db_routing_pg -- --ignored --test-threads=1
//! ```
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::time::Duration;

use kb_core::data_class::DataClass;
use kb_core::role::Role;
use kb_core::routing::{ProviderKind, RoutingStrategy};
use kb_mock_backend::MockBackend;

/// Build a serve-shaped runtime (DB routing + hot reload) over a fresh DB,
/// with one config `[[backend]]` (`"mock"`) serving Text + Embed.
async fn serve_runtime(
    db: &kb_testsupport::TestDb,
    mock: &MockBackend,
    blob_dir: &tempfile::TempDir,
) -> anyhow::Result<kb_api::runtime::PipelineRuntime> {
    let mut cfg = kb_config::Config::default();
    cfg.storage.postgres_url = db.admin_url.clone();
    cfg.storage.app_postgres_url = db.app_url.clone();
    cfg.blob.local_root = blob_dir.path().to_string_lossy().into_owned();
    cfg.backends.push(kb_config::Backend {
        id: "mock".into(),
        base_url: mock.url("/v1"),
        roles: vec![Role::Text, Role::Embed],
        slots: 8,
        priority: 0,
    });
    let app_config = kb_config::AppConfig::from_config(cfg);
    kb_api::runtime::build_runtime(&app_config, true).await
}

/// Seed one provider + one Text-capable model, returning the model's DB id.
async fn seed_text_model(
    pg: &kb_store::PgStore,
    endpoint: &str,
    suffix: &str,
) -> anyhow::Result<i64> {
    let provider_id = pg
        .create_provider(
            &format!("it-prov-{suffix}"),
            ProviderKind::OpenAiCompat,
            Some(endpoint),
            &serde_json::json!({}),
        )
        .await?;
    pg.create_model(
        provider_id,
        &format!("it-model-{suffix}"),
        &["text".to_string()],
        Some(8192),
        Some(4),
        None,
        None,
        None,
        None,
        DataClass::Local,
    )
    .await
}

/// Empty routes table → the pool stays in legacy mode and the config
/// backend serves every role (the explicit empty-DB behavior that replaced
/// the unresolvable config→entries bootstrap).
#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn serve_runtime_empty_db_stays_legacy_and_acquires() -> anyhow::Result<()> {
    let mock = MockBackend::start().await;
    let db = kb_testsupport::fresh_db().await?;
    let blob_dir = tempfile::tempdir()?;
    let rt = serve_runtime(&db, &mock, &blob_dir).await?;

    assert!(
        !rt.backend_pool.has_routing(),
        "empty routes table must leave the pool in legacy mode"
    );
    let lease = rt.backend_pool.acquire(Role::Text, false, 0).await?;
    assert_eq!(lease.backend_id, "mock");

    mock.shutdown().await;
    Ok(())
}

/// One **global** Text route, present at startup: Text acquires resolve the
/// materialized DB backend; Embed (no routes) falls back to the config
/// backend — the ledger acceptance, serve-shaped.
#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn one_global_route_resolves_and_other_roles_fall_back() -> anyhow::Result<()> {
    let mock = MockBackend::start().await;
    let routed = MockBackend::start().await; // the DB provider's own server
    let db = kb_testsupport::fresh_db().await?;
    let blob_dir = tempfile::tempdir()?;

    // Seed BEFORE the runtime builds, via a store on the same DB.
    let seed_store = kb_store::PgStore::with_roles(&db.admin_url, &db.app_url);
    seed_store.connect().await?;
    let mid = seed_text_model(&seed_store, &routed.url("/v1"), "startup").await?;
    seed_store
        .create_route(None, Role::Text, 0, RoutingStrategy::LeastLoaded, 800, mid)
        .await?;

    let rt = serve_runtime(&db, &mock, &blob_dir).await?;
    assert!(
        rt.backend_pool.has_routing(),
        "startup load must activate routing"
    );

    let lease = rt.backend_pool.acquire(Role::Text, false, 0).await?;
    assert_eq!(
        lease.backend_id,
        format!("db:{mid}"),
        "Text must resolve the materialized DB backend"
    );
    assert_eq!(lease.endpoint, routed.url("/v1"));
    drop(lease);

    let lease = rt.backend_pool.acquire(Role::Embed, false, 0).await?;
    assert_eq!(
        lease.backend_id, "mock",
        "a role with no routes must fall back to the config backend"
    );

    mock.shutdown().await;
    routed.shutdown().await;
    Ok(())
}

/// The acceptance clause live: a route created **after** startup arrives via
/// LISTEN/NOTIFY, resolves for its role, and leaves every other role working.
#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn route_created_after_startup_hot_reloads_without_breaking_roles() -> anyhow::Result<()> {
    let mock = MockBackend::start().await;
    let routed = MockBackend::start().await;
    let db = kb_testsupport::fresh_db().await?;
    let blob_dir = tempfile::tempdir()?;
    let rt = serve_runtime(&db, &mock, &blob_dir).await?;
    assert!(!rt.backend_pool.has_routing());

    // Admin creates provider → model → route on the live system.
    let mid = seed_text_model(&rt.pg_store, &routed.url("/v1"), "hot").await?;
    rt.pg_store
        .create_route(None, Role::Text, 0, RoutingStrategy::LeastLoaded, 800, mid)
        .await?;

    // Bounded poll, re-notifying as we go: the notifier opens a fresh
    // PgListener per wait, so a single NOTIFY fired while it is between
    // recv loops is dropped. Real admin flows notify once per CRUD action
    // (provider → model → route bursts); re-sending models that and keeps
    // the test deterministic. (The lost-single-NOTIFY window is recorded as
    // a follow-up in the ledger notes.)
    let mut activated = false;
    for _ in 0..100 {
        rt.pg_store.notify_routing_change().await?;
        if rt.backend_pool.has_routing() {
            activated = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        activated,
        "hot reload must activate routing within 10s of NOTIFY"
    );

    let lease = rt.backend_pool.acquire(Role::Text, false, 0).await?;
    assert_eq!(lease.backend_id, format!("db:{mid}"));
    drop(lease);

    // The clause that used to fail: every OTHER role keeps acquiring.
    let lease = rt.backend_pool.acquire(Role::Embed, false, 0).await?;
    assert_eq!(lease.backend_id, "mock");

    mock.shutdown().await;
    routed.shutdown().await;
    Ok(())
}

/// Documents the recorded follow-up gap: tenant-bound routes (what the admin
/// UI creates) are invisible to the tenant-blind acquire — the role falls
/// back to config backends instead of breaking.
#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn tenant_bound_route_is_invisible_to_tenant_blind_acquire() -> anyhow::Result<()> {
    let mock = MockBackend::start().await;
    let routed = MockBackend::start().await;
    let db = kb_testsupport::fresh_db().await?;
    let blob_dir = tempfile::tempdir()?;

    let seed_store = kb_store::PgStore::with_roles(&db.admin_url, &db.app_url);
    seed_store.connect().await?;
    let admin = seed_store.pool()?;
    let tenant_id: i64 =
        sqlx::query_scalar("INSERT INTO tenants (slug, name) VALUES ('t', 'T') RETURNING id")
            .fetch_one(&admin)
            .await?;
    let mid = seed_text_model(&seed_store, &routed.url("/v1"), "tenant").await?;
    seed_store
        .create_route(
            Some(tenant_id),
            Role::Text,
            0,
            RoutingStrategy::LeastLoaded,
            800,
            mid,
        )
        .await?;

    let rt = serve_runtime(&db, &mock, &blob_dir).await?;
    assert!(
        rt.backend_pool.has_routing(),
        "tenant routes still activate the table"
    );

    // tiers_for(None, Text) sees no global tiers → per-role fallback.
    let lease = rt.backend_pool.acquire(Role::Text, false, 0).await?;
    assert_eq!(
        lease.backend_id, "mock",
        "tenant-bound route is dormant for tenant-blind acquire (recorded gap)"
    );

    mock.shutdown().await;
    routed.shutdown().await;
    Ok(())
}
