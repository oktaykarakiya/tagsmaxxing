//! `PgStore` — the [`Store`](kb_core::store::Store) implementation over Postgres + pgvector
//! (plan §5/§7/§13), connecting via sqlx.
//!
//! This module implements `upsert_file` and `upsert_chunks` (P2-T2); `hybrid_search` is
//! deferred to P4. The connection URL is hot-swappable behind [`arc_swap::ArcSwap`] so an
//! operator can point at a different Postgres instance without restarting (the hot-swap
//! rule, CLAUDE.md / §20).

use std::sync::Arc;

use anyhow::Context;
use arc_swap::ArcSwap;
use async_trait::async_trait;
use kb_core::chunk::Chunk;
use kb_core::file::FileRecord;
use kb_core::query::{Hit, Query};
use kb_core::store::Store;
use sqlx::Postgres;
use sqlx::postgres::{PgPool, PgPoolOptions};

use crate::MIGRATOR;

// ── PgStore ──────────────────────────────────────────────────────────────────

/// A Postgres-backed implementation of the core [`Store`] trait, connecting via a sqlx
/// [`PgPool`]. The connection URL is hot-swappable so an operator can relocate the database
/// without restarting.
///
/// # Lifecycle
///
/// 1. Construct with [`PgStore::new`].
/// 2. Call [`PgStore::connect`] to establish the pool and auto-apply schema migrations.
/// 3. Use [`Store::upsert_file`] and [`Store::upsert_chunks`].
///
/// Call [`PgStore::set_url`] then [`PgStore::connect`] again to hot-swap to a new Postgres
/// instance.
pub struct PgStore {
    /// Hot-swappable connection URL. Read at [`connect`](PgStore::connect) time so rotation
    /// never needs a restart.
    url: ArcSwap<String>,
    /// The active connection pool, replaced atomically on each connect.
    pool: ArcSwap<Option<PgPool>>,
}

impl PgStore {
    /// Create a new `PgStore` with the given connection URL, not yet connected.
    ///
    /// Call [`connect`](PgStore::connect) before using the [`Store`] trait methods.
    #[must_use]
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: ArcSwap::new(Arc::new(url.into())),
            pool: ArcSwap::new(Arc::new(None)),
        }
    }

    /// Hot-swap the database URL. The change takes effect on the **next** call to
    /// [`connect`](PgStore::connect) — active connections are not disrupted.
    pub fn set_url(&self, url: impl Into<String>) {
        self.url.store(Arc::new(url.into()));
    }

    /// Read the current URL snapshot.
    fn current_url(&self) -> Arc<String> {
        self.url.load_full()
    }

    /// Establish (or re-establish) the connection pool using the current URL, then
    /// auto-apply the forward-only schema migrations. Migrations already applied are
    /// skipped (the `_sqlx_migrations` ledger makes it idempotent).
    ///
    /// If a previous pool exists it is closed gracefully before the new one replaces it.
    ///
    /// # Errors
    /// Returns an error if the connection fails or migrations cannot be applied.
    pub async fn connect(&self) -> anyhow::Result<()> {
        let url = self.current_url();
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(url.as_ref())
            .await
            .context("failed to connect to Postgres")?;
        MIGRATOR
            .run(&pool)
            .await
            .context("failed to apply schema migrations")?;

        let old = self.pool.swap(Arc::new(Some(pool.clone())));
        if let Some(old_pool) = old.as_ref().as_ref() {
            old_pool.close().await;
        }

        Ok(())
    }

    /// Obtain a clone of the current connection pool.
    ///
    /// # Errors
    /// Returns an error if [`connect`](PgStore::connect) has not been called yet.
    pub fn pool(&self) -> anyhow::Result<PgPool> {
        self.pool
            .load_full()
            .as_ref()
            .as_ref()
            .cloned()
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "PgStore is not connected — call connect() before using Store methods"
                )
            })
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────────

/// Format a `Vec<f32>` into the pgvector text representation `[x1,x2,...]`.
///
/// This string can be cast to the `vector` type in SQL via `CAST($1 AS vector)`.
fn format_vector(v: &[f32]) -> String {
    // Use a pre-sized buffer to avoid reallocation. Worst case: each f32 formats as
    // ~15 chars, plus a comma per element, plus the brackets.
    let mut buf = String::with_capacity(2 + v.len() * 16);
    buf.push('[');
    for (i, x) in v.iter().enumerate() {
        if i > 0 {
            buf.push(',');
        }
        // Write the float with enough precision for round-tripping.
        use std::fmt::Write;
        let _ = write!(buf, "{x}");
    }
    buf.push(']');
    buf
}

// ── Store implementation ─────────────────────────────────────────────────────

#[async_trait]
impl Store for PgStore {
    /// Insert or update a file (page) record, returning its id.
    ///
    /// Idempotent on `(tenant_id, sha256)` conflict: existing rows are updated with
    /// the latest metadata, status, and document association.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    async fn upsert_file(&self, rec: &FileRecord) -> anyhow::Result<i64> {
        let pool = self.pool()?;

        let row = sqlx::query_scalar::<Postgres, i64>(
            "INSERT INTO files \
             (tenant_id, document_id, page_no, page_label, sha256, blob_key, \
              path, mime, size_bytes, meta, status, ingested_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) \
             ON CONFLICT (tenant_id, sha256) DO UPDATE SET \
               document_id = EXCLUDED.document_id, \
               page_no     = EXCLUDED.page_no, \
               page_label  = EXCLUDED.page_label, \
               blob_key    = EXCLUDED.blob_key, \
               path        = EXCLUDED.path, \
               mime        = EXCLUDED.mime, \
               size_bytes  = EXCLUDED.size_bytes, \
               meta        = EXCLUDED.meta, \
               status      = EXCLUDED.status, \
               ingested_at = EXCLUDED.ingested_at \
             RETURNING id",
        )
        .bind(rec.tenant_id)
        .bind(rec.document_id)
        .bind(rec.page_no)
        .bind(&rec.page_label)
        .bind(rec.sha256.as_bytes().as_slice())
        .bind(&rec.blob_key)
        .bind(&rec.path)
        .bind(&rec.mime)
        .bind(rec.size_bytes)
        .bind(&rec.meta)
        .bind(rec.status.as_str())
        .bind(rec.ingested_at)
        .fetch_one(&pool)
        .await
        .context("failed to upsert file record")?;

        Ok(row)
    }

    /// Atomically replace the chunks belonging to a file.
    ///
    /// Deletes all existing chunks for `file_id`, then inserts the new set in a single
    /// transaction. On an empty `chunks` slice, this is a no-op (the DELETE still runs
    /// to clear old data).
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    async fn upsert_chunks(&self, file_id: i64, chunks: &[Chunk]) -> anyhow::Result<()> {
        let pool = self.pool()?;
        let mut tx = pool
            .begin()
            .await
            .context("failed to begin transaction for upsert_chunks")?;

        // Delete existing chunks for this file. If there are no new chunks to insert,
        // the net effect is a clean removal (the caller explicitly passed an empty list).
        sqlx::query("DELETE FROM chunks WHERE file_id = $1")
            .bind(file_id)
            .execute(&mut *tx)
            .await
            .context("failed to delete old chunks")?;

        // Insert each new chunk. The embedding is cast from the pgvector text
        // representation because sqlx's extended-query protocol needs an explicit cast
        // for custom types.
        for chunk in chunks {
            let vec_str = format_vector(&chunk.embedding);
            sqlx::query(
                "INSERT INTO chunks (tenant_id, document_id, file_id, page_no, \
                 idx, content, ts_offset, embedding) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, CAST($8 AS vector))",
            )
            .bind(chunk.tenant_id)
            .bind(chunk.document_id)
            .bind(file_id)
            .bind(chunk.page_no)
            .bind(chunk.idx)
            .bind(&chunk.content)
            .bind(chunk.ts_offset)
            .bind(&vec_str)
            .execute(&mut *tx)
            .await
            .context("failed to insert chunk")?;
        }

        tx.commit()
            .await
            .context("failed to commit upsert_chunks transaction")?;

        Ok(())
    }

    /// Hybrid (vector + keyword) search — **not yet implemented** (deferred to P4 per
    /// the plan §8 ledger).
    ///
    /// # Errors
    /// Always returns an error indicating this method is not yet available.
    async fn hybrid_search(&self, _query: &Query) -> anyhow::Result<Vec<Hit>> {
        // Deferred to P4 — the P2 vertical slice stops at upsert semantics.
        anyhow::bail!("hybrid_search is not yet implemented (deferred to P4, plan §8)")
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    // ── format_vector ────────────────────────────────────────────────────

    #[test]
    fn format_vector_empty() {
        assert_eq!(format_vector(&[]), "[]");
    }

    #[test]
    fn format_vector_single() {
        assert_eq!(format_vector(&[1.5]), "[1.5]");
    }

    #[test]
    fn format_vector_multiple() {
        let v = format_vector(&[1.0, 2.5, -3.0]);
        assert_eq!(v, "[1,2.5,-3]");
    }

    #[test]
    fn format_vector_high_precision() {
        let v = format_vector(&[0.123_456_79_f32]);
        // The formatted string should contain the input value (float formatting is
        // lossy, but for pgvector 4–6 decimal places are sufficient).
        assert!(v.starts_with('['));
        assert!(v.ends_with(']'));
        assert!(v.contains("0.123"));
    }

    #[test]
    fn format_vector_1024_dim() {
        let embedding = vec![0.0f32; 1024];
        let f = format_vector(&embedding);
        assert!(f.starts_with('['));
        assert!(f.ends_with(']'));
        // 1024 zeros at ~2-3 chars each, plus commas.
        assert!(f.len() > 2048);
    }

    // ── PgStore construction / hot-swap ─────────────────────────────────

    #[test]
    fn new_stores_url() {
        let store = PgStore::new("postgres://localhost/kb");
        assert_eq!(*store.current_url(), "postgres://localhost/kb");
    }

    #[test]
    fn set_url_hot_swaps() {
        let store = PgStore::new("postgres://old/db");
        assert_eq!(*store.current_url(), "postgres://old/db");
        store.set_url("postgres://new/db");
        assert_eq!(*store.current_url(), "postgres://new/db");
    }

    #[test]
    fn set_url_is_independent_of_previous_url() {
        let store = PgStore::new("postgres://a/db");
        store.set_url("postgres://b/db");
        store.set_url("postgres://c/db");
        assert_eq!(*store.current_url(), "postgres://c/db");
    }

    #[test]
    fn pool_errors_before_connect() {
        let store = PgStore::new("postgres://localhost/nonexistent");
        let err = store.pool().unwrap_err();
        assert!(
            err.to_string().contains("not connected"),
            "expected 'not connected' error, got: {err}"
        );
    }

    // ── hybrid_search deferred ──────────────────────────────────────────

    #[tokio::test]
    async fn hybrid_search_returns_deferred_error() {
        let store = PgStore::new("postgres://localhost/db");
        let q = Query {
            text: "test".into(),
            filters: Default::default(),
            top_k: 10,
        };
        let err = store.hybrid_search(&q).await.unwrap_err();
        assert!(
            err.to_string().contains("not yet implemented"),
            "expected deferred error, got: {err}"
        );
    }
}
