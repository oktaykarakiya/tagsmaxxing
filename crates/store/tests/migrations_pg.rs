//! Integration test: the schema migrations apply cleanly on a *fresh* pgvector Postgres,
//! produce 1024-dim vector columns, enable RLS, and are idempotent on re-run (plan §5/§13/§16,
//! acceptance of P0-T6).
//!
//! This test needs a container runtime (Docker **or** Podman) and network access to pull the
//! `pgvector/pgvector` image, so it is gated `#[ignore]` and does NOT run in `just ci`
//! (CI/dev machines here have Podman only, and pulls may be offline). Run it explicitly:
//!
//! ```text
//! # Docker:
//! cargo test -p kb-store --test migrations_pg -- --ignored
//! # Podman (expose the socket testcontainers talks to):
//! systemctl --user start podman.socket
//! DOCKER_HOST=unix://$XDG_RUNTIME_DIR/podman/podman.sock \
//!   cargo test -p kb-store --test migrations_pg -- --ignored
//! ```
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::time::Duration;

use kb_store::MIGRATOR;
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};
use testcontainers::core::ports::IntoContainerPort;
use testcontainers::core::wait::WaitFor;
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};

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
#[ignore = "requires a container runtime (Docker/Podman) + image pull; run with --ignored"]
async fn migrations_apply_on_fresh_pgvector() -> TestResult {
    // A fresh pgvector image — nothing pre-created. POSTGRES_DB ensures the `kb` database exists.
    let container = GenericImage::new("pgvector/pgvector", "pg17")
        .with_exposed_port(5432u16.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_USER", "kb")
        .with_env_var("POSTGRES_PASSWORD", "kb")
        .with_env_var("POSTGRES_DB", "kb")
        .start()
        .await?;

    let port = container.get_host_port_ipv4(5432u16.tcp()).await?;
    let url = format!("postgres://kb:kb@127.0.0.1:{port}/kb?sslmode=disable");
    let pool = connect_with_retry(&url).await;

    // Apply forward-only migrations against the empty database.
    MIGRATOR.run(&pool).await?;

    // pgvector extension is installed.
    let has_vector: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'vector')")
            .fetch_one(&pool)
            .await?;
    assert!(has_vector, "vector extension must be installed");

    // The embedder lock-in: chunks.embedding is exactly vector(1024) (plan §5).
    let chunks_dim: String = sqlx::query_scalar(
        "SELECT format_type(a.atttypid, a.atttypmod) FROM pg_attribute a \
         WHERE a.attrelid = 'chunks'::regclass AND a.attname = 'embedding'",
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(chunks_dim, "vector(1024)", "chunks.embedding dim");

    let tags_dim: String = sqlx::query_scalar(
        "SELECT format_type(a.atttypid, a.atttypmod) FROM pg_attribute a \
         WHERE a.attrelid = 'tags'::regclass AND a.attname = 'embedding'",
    )
    .fetch_one(&pool)
    .await?;
    assert_eq!(tags_dim, "vector(1024)", "tags.embedding dim");

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

    // The embedder lock-in row records id + dim (plan §5 note).
    let embedder = sqlx::query(
        "SELECT value->>'id' AS id, (value->>'dim')::int AS dim \
                                FROM settings WHERE key = 'embedder'",
    )
    .fetch_one(&pool)
    .await?;
    let id: String = embedder.try_get("id")?;
    let dim: i32 = embedder.try_get("dim")?;
    assert_eq!(id, "bge-m3");
    assert_eq!(dim, 1024);

    // Forward-only re-run is a no-op (the _sqlx_migrations ledger skips applied versions).
    MIGRATOR.run(&pool).await?;

    Ok(())
}
