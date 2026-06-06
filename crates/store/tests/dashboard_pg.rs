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
