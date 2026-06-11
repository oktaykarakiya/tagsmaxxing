// SPDX-License-Identifier: AGPL-3.0-or-later

//! Integration tests for the dashboard query methods on `PgStore` (P12-T4,
//! P14-T13) against a real pgvector Postgres container.
//!
//! These tests need **Podman** (this project targets Podman exclusively) and
//! network access to pull `pgvector/pgvector`. They are gated `#[ignore]` so
//! `just ci` stays green when no container runtime is available. Run them
//! explicitly against the Podman socket:
//!
//! ```text
//! systemctl --user start podman.socket
//! DOCKER_HOST=unix://$XDG_RUNTIME_DIR/podman/podman.sock \
//!   cargo test -p kb-store --test dashboard_pg -- --ignored
//! ```
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use chrono::{DateTime, Utc};
use kb_core::role::Role;
use kb_core::usage::UsageEvent;
use kb_store::PgStore;
use sqlx::PgPool;

struct Setup {
    store: Arc<PgStore>,
    pool: PgPool,
    tenant_id: i64,
}

/// Provision a fresh DB, run migrations (two-role), insert a tenant via the
/// privileged pool.
async fn setup() -> anyhow::Result<Setup> {
    let db = kb_testsupport::fresh_db().await?;
    let store = Arc::new(PgStore::with_roles(&db.admin_url, &db.app_url));
    store.connect().await?;
    let pool = store.pool()?;

    let tenant_id: i64 =
        sqlx::query_scalar("INSERT INTO tenants (slug, name) VALUES ('t', 'Test') RETURNING id")
            .fetch_one(&pool)
            .await?;

    Ok(Setup {
        store,
        pool,
        tenant_id,
    })
}

/// Insert a user via the privileged pool and return its id.
async fn insert_user(pool: &PgPool, tenant_id: i64, email: &str) -> anyhow::Result<i64> {
    let id: i64 = sqlx::query_scalar(
        "INSERT INTO users (tenant_id, email, password_hash, role) \
         VALUES ($1, $2, 'x', 'member') RETURNING id",
    )
    .bind(tenant_id)
    .bind(email)
    .fetch_one(pool)
    .await?;
    Ok(id)
}

fn usage_event(
    tenant_id: i64,
    user_id: Option<i64>,
    prompt: i32,
    completion: i32,
    created_at: DateTime<Utc>,
) -> UsageEvent {
    UsageEvent {
        id: 0,
        tenant_id,
        user_id,
        model: "test-model".into(),
        role: Role::Embed,
        backend_id: None,
        prompt_tokens: Some(prompt),
        completion_tokens: Some(completion),
        latency_ms: None,
        cost_micros: None,
        created_at,
    }
}

/// `get_monthly_token_usage_by_user` groups the current month's usage by user,
/// includes the NULL/system bucket, joins the email, and orders by usage desc.
#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn per_user_usage_groups_and_includes_system_bucket() -> anyhow::Result<()> {
    let s = setup().await?;
    let now = Utc::now();

    let alice = insert_user(&s.pool, s.tenant_id, "alice@example.com").await?;
    let bob = insert_user(&s.pool, s.tenant_id, "bob@example.com").await?;

    // Empty before any usage.
    let empty = s.store.get_monthly_token_usage_by_user(s.tenant_id).await?;
    assert!(empty.is_empty(), "no usage ⇒ no rows");

    // Alice: 10+5 then 20 = 35. Bob: 100. System (None): 7.
    for ev in [
        usage_event(s.tenant_id, Some(alice), 10, 5, now),
        usage_event(s.tenant_id, Some(alice), 20, 0, now),
        usage_event(s.tenant_id, Some(bob), 100, 0, now),
        usage_event(s.tenant_id, None, 7, 0, now),
    ] {
        s.store.insert_usage_event(&ev).await?;
    }

    let rows = s.store.get_monthly_token_usage_by_user(s.tenant_id).await?;

    // Three buckets: bob (100), alice (35), system (7) — ordered by usage desc.
    assert_eq!(rows.len(), 3, "alice + bob + system bucket");
    assert_eq!(rows[0].user_id, Some(bob));
    assert_eq!(rows[0].total_tokens, 100);
    assert_eq!(rows[1].user_id, Some(alice));
    assert_eq!(rows[1].email.as_deref(), Some("alice@example.com"));
    assert_eq!(rows[1].total_tokens, 35);
    // System bucket: NULL user, NULL email, smallest usage → last.
    assert_eq!(rows[2].user_id, None);
    assert!(rows[2].email.is_none());
    assert_eq!(rows[2].total_tokens, 7);

    Ok(())
}

/// Per-user usage is scoped to the current UTC month — prior-month events are
/// excluded.
#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn per_user_usage_excludes_other_months() -> anyhow::Result<()> {
    let s = setup().await?;
    let alice = insert_user(&s.pool, s.tenant_id, "alice@example.com").await?;

    let now = Utc::now();
    // A clearly prior month (60 days back), inserted directly so the month-trunc
    // filter is exercised regardless of the rollup.
    let old = now - chrono::Duration::days(60);
    sqlx::query(
        "INSERT INTO usage_events \
         (tenant_id, user_id, model, role, prompt_tokens, completion_tokens, created_at) \
         VALUES ($1, $2, 'seed', 'embed', 999, 0, $3)",
    )
    .bind(s.tenant_id)
    .bind(alice)
    .bind(old)
    .execute(&s.pool)
    .await?;

    s.store
        .insert_usage_event(&usage_event(s.tenant_id, Some(alice), 5, 0, now))
        .await?;

    let rows = s.store.get_monthly_token_usage_by_user(s.tenant_id).await?;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].user_id, Some(alice));
    assert_eq!(rows[0].total_tokens, 5, "prior-month 999 excluded");

    Ok(())
}

/// Insert one `usage_events` row with an explicit `cost_micros` (or NULL) and
/// `created_at`, via the privileged pool (RLS-bypassing, like the other direct
/// inserts in this file).
async fn insert_priced_event(
    pool: &PgPool,
    tenant_id: i64,
    cost_micros: Option<i64>,
    created_at: DateTime<Utc>,
) -> anyhow::Result<()> {
    sqlx::query(
        "INSERT INTO usage_events \
         (tenant_id, model, role, prompt_tokens, completion_tokens, cost_micros, created_at) \
         VALUES ($1, 'seed', 'embed', 1, 0, $2, $3)",
    )
    .bind(tenant_id)
    .bind(cost_micros)
    .bind(created_at)
    .execute(pool)
    .await?;
    Ok(())
}

/// Regression (F6): `get_monthly_cost` and `admin_monthly_spend` sum the current
/// month's non-NULL `cost_micros`. Postgres `SUM(BIGINT)` returns NUMERIC, which
/// sqlx 0.8 (no `bigdecimal`/`rust_decimal` feature) cannot decode as `i64` — so
/// without the `::bigint` cast in both queries this errored on every call (masked
/// by `.context`), breaking the monthly-spend gauge and the admin dashboard cost
/// figure. This test would have caught it: it asserts the queries succeed and
/// return the correct in-month, non-NULL sum.
#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn monthly_cost_sums_in_month_non_null_micros() -> anyhow::Result<()> {
    let s = setup().await?;
    let now = Utc::now();
    let prior_month = now - chrono::Duration::days(40);

    // No events yet → COALESCE(SUM,0)::bigint → 0 (and the query must not error).
    assert_eq!(s.store.get_monthly_cost(s.tenant_id).await?, 0);
    assert_eq!(s.store.admin_monthly_spend(s.tenant_id).await?, 0);

    // Two priced calls this month, one free (NULL) call this month, one priced
    // call in the prior month (excluded by the month filter).
    insert_priced_event(&s.pool, s.tenant_id, Some(1_500_000), now).await?;
    insert_priced_event(&s.pool, s.tenant_id, Some(2_500_000), now).await?;
    insert_priced_event(&s.pool, s.tenant_id, None, now).await?; // free/local
    insert_priced_event(&s.pool, s.tenant_id, Some(9_000_000), prior_month).await?;

    // Only the two in-month non-NULL rows count: 1.5M + 2.5M = 4.0M micros.
    assert_eq!(s.store.get_monthly_cost(s.tenant_id).await?, 4_000_000);
    assert_eq!(s.store.admin_monthly_spend(s.tenant_id).await?, 4_000_000);

    Ok(())
}
