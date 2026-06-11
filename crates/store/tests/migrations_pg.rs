// SPDX-License-Identifier: AGPL-3.0-or-later

//! Integration test: the schema migrations apply cleanly on a *fresh* pgvector Postgres,
//! produce 2560-dim vector columns, enable RLS, and are idempotent on re-run (plan §5/§13/§16,
//! acceptance of P0-T6).
//!
//! This test needs **Podman** (this project targets Podman exclusively) and network access to
//! pull the `pgvector/pgvector` image, so it is gated `#[ignore]` and does NOT run in `just ci`
//! (pulls may be offline). Run it explicitly against the Podman socket testcontainers talks to:
//!
//! ```text
//! systemctl --user start podman.socket
//! DOCKER_HOST=unix://$XDG_RUNTIME_DIR/podman/podman.sock \
//!   cargo test -p kb-store --test migrations_pg -- --ignored
//! ```
//! (`DOCKER_HOST` is just the env var testcontainers reads; it points at the Podman socket.)
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::time::Duration;

use kb_store::MIGRATOR;
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};

type TestResult = Result<(), Box<dyn std::error::Error>>;

/// Connect with a bounded retry loop so the test does not depend on log-message timing
/// (Postgres logs "ready" once during initdb and again after the real start).
async fn connect_with_retry(url: &str) -> PgPool {
    for _ in 0..40 {
        match PgPoolOptions::new()
            .max_connections(4)
            .acquire_timeout(Duration::from_secs(3))
            .connect(url)
            .await
        {
            Ok(pool) => return pool,
            Err(_) => tokio::time::sleep(Duration::from_millis(500)).await,
        }
    }
    panic!("postgres at {url} did not become ready in time");
}

#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn migrations_apply_on_fresh_pgvector() -> TestResult {
    // A fresh, empty database in the shared pgvector container. Migrations run as the
    // privileged role (admin_url), exactly as `PgStore::connect` does.
    let db = kb_testsupport::fresh_db().await?;
    let pool = connect_with_retry(&db.admin_url).await;

    // Apply forward-only migrations against the empty database.
    MIGRATOR.run(&pool).await?;

    // pgvector extension is installed.
    let has_vector: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'vector')")
            .fetch_one(&pool)
            .await?;
    assert!(has_vector, "vector extension must be installed");

    // The embedder lock-in: chunks.embedding is exactly vector(2560) after the Qwen3-Embedding-4B
    // migration (0023) widens it from the original 1024 (plan §5).
    let chunks_dim: String = sqlx::query_scalar(
        "SELECT format_type(a.atttypid, a.atttypmod) FROM pg_attribute a \
         WHERE a.attrelid = 'chunks'::regclass AND a.attname = 'embedding'",
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(chunks_dim, "vector(2560)", "chunks.embedding dim");

    let tags_dim: String = sqlx::query_scalar(
        "SELECT format_type(a.atttypid, a.atttypmod) FROM pg_attribute a \
         WHERE a.attrelid = 'tags'::regclass AND a.attname = 'embedding'",
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(tags_dim, "vector(2560)", "tags.embedding dim");

    // RLS is enabled (and FORCEd) on the tenant-scoped tables (plan §13).
    for table in [
        "users",
        "documents",
        "files",
        "tags",
        "chunks",
        "jobs",
        "usage_events",
    ] {
        let row = sqlx::query(
            "SELECT relrowsecurity, relforcerowsecurity FROM pg_class WHERE relname = $1",
        )
        .bind(table)
        .fetch_one(&pool)
        .await?;
        let enabled: bool = row.try_get("relrowsecurity")?;
        let forced: bool = row.try_get("relforcerowsecurity")?;
        assert!(enabled, "RLS must be ENABLED on {table}");
        assert!(forced, "RLS must be FORCEd on {table}");
    }

    // The app.current_tenant helper exists and runs.
    sqlx::query("SELECT app_set_current_tenant($1)")
        .bind(1_i64)
        .execute(&pool)
        .await?;

    // Migration 0006: the kb_app application role exists and is the non-privileged role RLS
    // requires — NOT a superuser and NOT BYPASSRLS (P6-T14, §13). This is the property the
    // whole isolation model rests on.
    let app_role = sqlx::query(
        "SELECT rolsuper, rolbypassrls, rolcanlogin FROM pg_roles WHERE rolname = 'kb_app'",
    )
    .fetch_optional(&pool)
    .await?
    .expect("migration 0006 must create the kb_app role");
    let is_super: bool = app_role.try_get("rolsuper")?;
    let bypass_rls: bool = app_role.try_get("rolbypassrls")?;
    let can_login: bool = app_role.try_get("rolcanlogin")?;
    assert!(!is_super, "kb_app must NOT be a superuser");
    assert!(
        !bypass_rls,
        "kb_app must NOT have BYPASSRLS (else RLS is a no-op)"
    );
    assert!(can_login, "kb_app must be able to LOGIN for the app pool");

    // The embedder lock-in row records id + dim (plan §5 note).
    let embedder = sqlx::query(
        "SELECT value->>'id' AS id, (value->>'dim')::int AS dim \
                                FROM settings WHERE key = 'embedder'",
    )
    .fetch_one(&pool)
    .await?;
    let id: String = embedder.try_get("id")?;
    let dim: i32 = embedder.try_get("dim")?;
    assert_eq!(id, "qwen3-embed-4b");
    assert_eq!(dim, 2560);

    // Forward-only re-run is a no-op (the _sqlx_migrations ledger skips applied versions).
    MIGRATOR.run(&pool).await?;

    Ok(())
}
