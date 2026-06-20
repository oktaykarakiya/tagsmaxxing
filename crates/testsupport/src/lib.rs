// SPDX-License-Identifier: AGPL-3.0-or-later

//! Shared harness for the `#[ignore]` testcontainers integration suites (P6-T14).
//!
//! # Why this crate exists
//!
//! 1. **No per-test container churn (finding 06-B).** Each suite used to spin up its *own*
//!    pgvector container in `setup()`; running ~150 of them — even serially — hammered the
//!    Podman daemon hard enough that ~7% of starts / first connections timed out. Here, **one**
//!    container is started per test *binary* (a [`tokio::sync::OnceCell`], so per-process) and
//!    every test carves out a **fresh database** inside it. Container startup — the slow, flaky
//!    part — happens once; per-test isolation is a cheap `CREATE DATABASE`.
//!
//! 2. **The two RLS roles (P6-T14).** Tenant isolation is enforced by Row-Level Security, which
//!    is bypassed by superusers. So each [`TestDb`] exposes two URLs: an **admin** URL (the
//!    container superuser — runs migrations, seeds data, the cross-tenant job queue) and an
//!    **app** URL (the non-privileged `kb_app` role —
//!    RLS-enforced, for tenant-scoped assertions). Suites seed via admin and assert via app.
//!
//! This crate is `publish = false` and used only as a dev-dependency.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::Context;
use sqlx::postgres::PgPool;
use sqlx::{Connection, PgConnection, Postgres, Transaction};
use tokio::sync::OnceCell;

/// The non-privileged application role (`kb_app`).
pub const APP_ROLE: &str = "kb_app";
/// The development password for [`APP_ROLE`] (matches the migration; operators rotate it).
pub const APP_PASSWORD: &str = "kb_app";
/// The container superuser (matches `POSTGRES_USER`/`POSTGRES_PASSWORD` below).
pub const ADMIN_ROLE: &str = "kb";

/// A fresh, isolated database inside the per-binary shared pgvector container.
///
/// Both URLs point at the **same** database; they differ only in the connecting role.
#[derive(Debug, Clone)]
pub struct TestDb {
    /// Privileged superuser URL — migrations, seeding, the cross-tenant job queue, admin paths.
    pub admin_url: String,
    /// Non-privileged `kb_app` URL — RLS-enforced tenant-scoped data.
    pub app_url: String,
    /// The generated database name (unique within the container).
    pub db_name: String,
}

/// The per-process shared container handle + its host port.
///
/// Deliberately holds **no** sqlx pool/connection: each `#[tokio::test]` runs on its own tokio
/// runtime, and sqlx connections are bound to the runtime that created them. A pool cached here
/// (created by the first test) would be dead for every later test ("pool timed out"). So the
/// only shared state is the container (kept alive) + the port (a plain `u16`); every call opens
/// its own connection on the *current* runtime.
struct Shared {
    /// Held so the container stays up for the whole test binary (one container, not per test).
    _container: testcontainers::ContainerAsync<testcontainers::GenericImage>,
    /// Host port mapped to the container's 5432.
    host_port: u16,
}

static SHARED: OnceCell<Shared> = OnceCell::const_new();
static DB_SEQ: AtomicU64 = AtomicU64::new(0);

/// Build the admin (superuser) and app (`kb_app`) connection URLs for a database.
///
/// Factored out as a pure function so the URL shape is unit-testable without a container.
#[must_use]
pub fn role_urls(host_port: u16, db_name: &str) -> (String, String) {
    (
        format!(
            "postgres://{ADMIN_ROLE}:{ADMIN_ROLE}@127.0.0.1:{host_port}/{db_name}?sslmode=disable"
        ),
        format!(
            "postgres://{APP_ROLE}:{APP_PASSWORD}@127.0.0.1:{host_port}/{db_name}?sslmode=disable"
        ),
    )
}

/// Generate a database name unique to this process + call (valid SQL identifier: lowercase
/// letters, digits, underscores).
#[must_use]
pub fn unique_db_name(seq: u64) -> String {
    format!("kb_test_{}_{}", std::process::id(), seq)
}

/// Start (once per test binary) the shared pgvector container, returning its host port.
///
/// `max_connections` is raised well above the default 100 because the container is shared
/// across all tests in the binary: each test opens its own `PgStore` pools (admin + app), and
/// connections from a finished test may briefly linger during teardown. The generous ceiling
/// keeps the lane from spuriously hitting "too many clients" / pool timeouts (finding 06-B).
async fn shared() -> anyhow::Result<&'static Shared> {
    SHARED
        .get_or_try_init(|| async {
            use testcontainers::core::ports::IntoContainerPort;
            use testcontainers::core::wait::WaitFor;
            use testcontainers::runners::AsyncRunner;
            use testcontainers::{GenericImage, ImageExt};

            let container = GenericImage::new("pgvector/pgvector", "pg17")
                .with_exposed_port(5432u16.tcp())
                .with_wait_for(WaitFor::message_on_stderr(
                    "database system is ready to accept connections",
                ))
                .with_env_var("POSTGRES_USER", ADMIN_ROLE)
                .with_env_var("POSTGRES_PASSWORD", ADMIN_ROLE)
                .with_env_var("POSTGRES_DB", ADMIN_ROLE)
                // Run `postgres -c max_connections=500` (the entrypoint exec's our args).
                .with_cmd(["postgres", "-c", "max_connections=500"])
                .start()
                .await
                .context("failed to start shared pgvector container")?;

            let host_port = container.get_host_port_ipv4(5432u16.tcp()).await?;
            Ok::<_, anyhow::Error>(Shared {
                _container: container,
                host_port,
            })
        })
        .await
}

/// Open a single maintenance connection on the **current** runtime, retrying on the transient
/// "not ready yet" errors that follow the container's first readiness log line.
async fn connect_maint(host_port: u16) -> anyhow::Result<PgConnection> {
    let url = format!(
        "postgres://{ADMIN_ROLE}:{ADMIN_ROLE}@127.0.0.1:{host_port}/{ADMIN_ROLE}?sslmode=disable"
    );
    let mut last_err = None;
    for _ in 0..40 {
        match PgConnection::connect(&url).await {
            Ok(conn) => return Ok(conn),
            Err(e) => {
                last_err = Some(e);
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
        }
    }
    Err(anyhow::anyhow!(
        "could not connect to {url} after 40 attempts: {last_err:?}"
    ))
}

/// Provision a fresh, empty database in the per-binary shared container and return its two
/// role URLs. Run migrations against [`TestDb::admin_url`] (e.g. via `PgStore::connect`); they
/// create the `kb_app` role + grants for [`TestDb::app_url`].
///
/// # Errors
/// Returns an error if the container cannot start or the database cannot be created.
pub async fn fresh_db() -> anyhow::Result<TestDb> {
    let s = shared().await?;
    let seq = DB_SEQ.fetch_add(1, Ordering::Relaxed);
    let db_name = unique_db_name(seq);
    // A short-lived maintenance connection on THIS test's runtime (sqlx connections are
    // runtime-bound, so we can't reuse one across `#[tokio::test]` runtimes). CREATE DATABASE
    // cannot run inside a transaction; a bare `.execute` on a connection is autocommit.
    let mut conn = connect_maint(s.host_port).await?;
    let created = sqlx::query(&format!("CREATE DATABASE \"{db_name}\""))
        .execute(&mut conn)
        .await
        .with_context(|| format!("failed to create test database {db_name}"));
    // Always close the maintenance connection promptly, whether or not the create succeeded.
    let _ = conn.close().await;
    created?;
    let (admin_url, app_url) = role_urls(s.host_port, &db_name);
    Ok(TestDb {
        admin_url,
        app_url,
        db_name,
    })
}

/// Begin a transaction on `pool` with the `app.current_tenant` GUC set transaction-locally —
/// the raw-SQL counterpart of `kb_store`'s `begin_tenant_tx`, for RLS-scoped assertions in the
/// isolation suites. Run statements on `&mut *tx`, then `tx.commit().await`.
///
/// # Errors
/// Returns an error if the transaction cannot begin or the GUC cannot be set.
pub async fn tenant_tx(
    pool: &PgPool,
    tenant_id: i64,
) -> anyhow::Result<Transaction<'static, Postgres>> {
    let mut tx = pool
        .begin()
        .await
        .context("failed to begin tenant transaction")?;
    sqlx::query("SELECT app_set_current_tenant($1)")
        .bind(tenant_id)
        .execute(&mut *tx)
        .await
        .context("failed to set app.current_tenant")?;
    Ok(tx)
}

/// Convenience: count rows of `sql` (a `SELECT COUNT(*) …`) under `tenant_id`'s RLS context on
/// `pool`. The query and the tenant GUC share one transaction so RLS actually applies.
///
/// # Errors
/// Returns an error if any statement fails.
pub async fn count_as_tenant(pool: &PgPool, tenant_id: i64, sql: &str) -> anyhow::Result<i64> {
    let mut tx = tenant_tx(pool, tenant_id).await?;
    let n: i64 = sqlx::query_scalar(sql)
        .fetch_one(&mut *tx)
        .await
        .with_context(|| format!("tenant-scoped count failed: {sql}"))?;
    tx.commit()
        .await
        .context("failed to commit count_as_tenant")?;
    Ok(n)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    #[test]
    fn role_urls_use_distinct_roles_same_db() {
        let (admin, app) = role_urls(54321, "kb_test_9_3");
        assert!(admin.starts_with("postgres://kb:kb@127.0.0.1:54321/kb_test_9_3"));
        assert!(app.starts_with("postgres://kb_app:kb_app@127.0.0.1:54321/kb_test_9_3"));
        // Same host:port/database, different credentials → the two RLS roles on one DB.
        assert!(admin.ends_with("/kb_test_9_3?sslmode=disable"));
        assert!(app.ends_with("/kb_test_9_3?sslmode=disable"));
        assert_ne!(admin, app);
    }

    #[test]
    fn unique_db_name_is_a_valid_identifier() {
        let name = unique_db_name(7);
        assert!(name.starts_with("kb_test_"));
        assert!(name.ends_with("_7"));
        assert!(
            name.chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'),
            "db name must be a bare identifier: {name}"
        );
    }

    #[test]
    fn unique_db_names_differ_by_seq() {
        assert_ne!(unique_db_name(1), unique_db_name(2));
    }

    #[test]
    fn role_constants_match_migration_defaults() {
        assert_eq!(APP_ROLE, "kb_app");
        assert_eq!(APP_PASSWORD, "kb_app");
        assert_eq!(ADMIN_ROLE, "kb");
    }
}
