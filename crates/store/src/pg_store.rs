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

/// Parse a pgvector text representation `[1,2.5,-3]` back into a `Vec<f32>`.
///
/// This is the inverse of [`format_vector`], used when reading vector columns back
/// from the database (sqlx 0.8 has no native pgvector type support, so we cast to
/// `::text` and parse).
fn parse_vector_text(s: &str) -> anyhow::Result<Vec<f32>> {
    let s = s.trim();
    let inner = s
        .strip_prefix('[')
        .and_then(|rest| rest.strip_suffix(']'))
        .with_context(|| format!("invalid vector text: not bracketed: {s}"))?;

    let inner = inner.trim();
    if inner.is_empty() {
        return Ok(Vec::new());
    }

    inner
        .split(',')
        .map(|part| {
            part.trim()
                .parse::<f32>()
                .with_context(|| format!("invalid float in vector text: {part}"))
        })
        .collect()
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

// ── Tag canonicalization methods (plan §6.5, P3-T3) ───────────────────────────

impl PgStore {
    /// Upsert a canonical tag: insert a new row or return the id of an existing
    /// tag with the same `(tenant_id, name)`. The embedding is stored only on
    /// insert; existing tags keep their original embedding.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn upsert_tag(
        &self,
        tenant_id: i64,
        name: &str,
        embedding: &[f32],
    ) -> anyhow::Result<i64> {
        let pool = self.pool()?;
        let vec_str = format_vector(embedding);

        let id = sqlx::query_scalar::<Postgres, i64>(
            "INSERT INTO tags (tenant_id, name, embedding) \
             VALUES ($1, $2, CAST($3 AS vector)) \
             ON CONFLICT (tenant_id, name) DO UPDATE SET name = EXCLUDED.name \
             RETURNING id",
        )
        .bind(tenant_id)
        .bind(name)
        .bind(&vec_str)
        .fetch_one(&pool)
        .await
        .context("failed to upsert tag")?;

        Ok(id)
    }

    /// Look up an exact alias match. Returns `Some(tag_id)` if the alias exists
    /// for the tenant, or `None` if no such alias is known.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn lookup_alias(&self, tenant_id: i64, alias: &str) -> anyhow::Result<Option<i64>> {
        let pool = self.pool()?;

        let tag_id: Option<i64> = sqlx::query_scalar(
            "SELECT tag_id FROM tag_aliases WHERE tenant_id = $1 AND alias = $2",
        )
        .bind(tenant_id)
        .bind(alias)
        .fetch_optional(&pool)
        .await
        .context("failed to look up tag alias")?;

        Ok(tag_id)
    }

    /// Return every tag for a tenant that has a non-null embedding, together
    /// with its parsed vector. Used for cosine-matching during canonicalization.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn find_similar_tags(&self, tenant_id: i64) -> anyhow::Result<Vec<(i64, Vec<f32>)>> {
        let pool = self.pool()?;

        #[derive(sqlx::FromRow)]
        struct TagRow {
            id: i64,
            embedding_text: String,
        }

        let rows: Vec<TagRow> = sqlx::query_as(
            "SELECT id, embedding::text AS embedding_text \
             FROM tags \
             WHERE tenant_id = $1 AND embedding IS NOT NULL",
        )
        .bind(tenant_id)
        .fetch_all(&pool)
        .await
        .context("failed to fetch similar tags")?;

        rows.into_iter()
            .map(|r| {
                let vec = parse_vector_text(&r.embedding_text)?;
                Ok((r.id, vec))
            })
            .collect()
    }

    /// Record a raw form as an alias for a canonical tag. Idempotent: if the
    /// alias already exists for this tenant it is silently ignored.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn insert_tag_alias(
        &self,
        tenant_id: i64,
        alias: &str,
        tag_id: i64,
    ) -> anyhow::Result<()> {
        let pool = self.pool()?;

        sqlx::query(
            "INSERT INTO tag_aliases (tenant_id, alias, tag_id) \
             VALUES ($1, $2, $3) \
             ON CONFLICT (tenant_id, alias) DO NOTHING",
        )
        .bind(tenant_id)
        .bind(alias)
        .bind(tag_id)
        .execute(&pool)
        .await
        .context("failed to insert tag alias")?;

        Ok(())
    }

    /// Attach canonical tags to a document. Idempotent: duplicate
    /// `(document_id, tag_id)` pairs are silently skipped.
    ///
    /// Runs inside a short transaction so that an error on one row doesn't leave
    /// the set half-inserted (the caller can retry the whole batch).
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn insert_document_tags(
        &self,
        document_id: i64,
        tag_ids: &[i64],
    ) -> anyhow::Result<()> {
        if tag_ids.is_empty() {
            return Ok(());
        }

        let pool = self.pool()?;
        let mut tx = pool
            .begin()
            .await
            .context("failed to begin transaction for insert_document_tags")?;

        for &tag_id in tag_ids {
            sqlx::query(
                "INSERT INTO document_tags (document_id, tag_id) \
                 VALUES ($1, $2) \
                 ON CONFLICT (document_id, tag_id) DO NOTHING",
            )
            .bind(document_id)
            .bind(tag_id)
            .execute(&mut *tx)
            .await
            .context("failed to insert document_tag row")?;
        }

        tx.commit()
            .await
            .context("failed to commit insert_document_tags")?;

        Ok(())
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

    // ── parse_vector_text ─────────────────────────────────────────────────

    #[test]
    fn parse_vector_empty() {
        let v = parse_vector_text("[]").unwrap();
        assert!(v.is_empty());
    }

    #[test]
    fn parse_vector_empty_with_whitespace() {
        let v = parse_vector_text("[ ]").unwrap();
        assert!(v.is_empty());
    }

    #[test]
    fn parse_vector_single() {
        let v = parse_vector_text("[1.5]").unwrap();
        assert_eq!(v, vec![1.5]);
    }

    #[test]
    fn parse_vector_multiple() {
        let v = parse_vector_text("[1,2.5,-3]").unwrap();
        assert_eq!(v, vec![1.0, 2.5, -3.0]);
    }

    #[test]
    fn parse_vector_with_spaces() {
        let v = parse_vector_text("[ 1.0 , 2.5 , -3.0 ]").unwrap();
        assert_eq!(v, vec![1.0, 2.5, -3.0]);
    }

    #[test]
    fn parse_vector_round_trips_with_format() {
        let original = vec![0.1, -0.2, 0.56789, -42.0];
        let formatted = format_vector(&original);
        let parsed = parse_vector_text(&formatted).unwrap();
        // Float round-tripping: after format + parse, values should be very close.
        assert_eq!(parsed.len(), original.len());
        for (a, b) in original.iter().zip(parsed.iter()) {
            assert!((a - b).abs() < 1e-6, "round-trip mismatch: {a} != {b}");
        }
    }

    #[test]
    fn parse_vector_no_brackets_is_error() {
        let err = parse_vector_text("1,2,3").unwrap_err();
        assert!(
            err.to_string().contains("not bracketed"),
            "expected 'not bracketed' error, got: {err}"
        );
    }

    #[test]
    fn parse_vector_only_open_bracket_is_error() {
        let err = parse_vector_text("[1,2,3").unwrap_err();
        assert!(err.to_string().contains("not bracketed"));
    }

    #[test]
    fn parse_vector_invalid_float_is_error() {
        let err = parse_vector_text("[1,abc,3]").unwrap_err();
        assert!(
            err.to_string().contains("invalid float"),
            "expected 'invalid float' error, got: {err}"
        );
    }

    // ── Tag methods — error before connect ────────────────────────────────

    #[tokio::test]
    async fn upsert_tag_errors_before_connect() {
        let store = PgStore::new("postgres://localhost/db");
        let err = store
            .upsert_tag(1, "invoice", &[0.1, 0.2])
            .await
            .unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn lookup_alias_errors_before_connect() {
        let store = PgStore::new("postgres://localhost/db");
        let err = store.lookup_alias(1, "bill").await.unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn find_similar_tags_errors_before_connect() {
        let store = PgStore::new("postgres://localhost/db");
        let err = store.find_similar_tags(1).await.unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn insert_tag_alias_errors_before_connect() {
        let store = PgStore::new("postgres://localhost/db");
        let err = store.insert_tag_alias(1, "bill", 42).await.unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn insert_document_tags_errors_before_connect() {
        let store = PgStore::new("postgres://localhost/db");
        let err = store.insert_document_tags(1, &[10, 20]).await.unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn insert_document_tags_empty_is_noop() {
        // Empty tag list is a no-op even without a connection (early return).
        let store = PgStore::new("postgres://localhost/db");
        store.insert_document_tags(1, &[]).await.unwrap();
    }

    // ── Tag integration tests (require Postgres + pgvector) ───────────────
    //
    // These are #[ignore] in just ci; run with `cargo test -- --ignored`
    // after `podman compose up -d`.

    #[cfg(test)]
    mod tag_integration {
        #![allow(clippy::unwrap_used, clippy::expect_used)]
        use super::*;

        /// Helper: connect a PgStore to a pgvector testcontainers Postgres,
        /// insert a tenant row, and call `f` with the store.
        async fn with_connected_store<F, Fut>(f: F) -> anyhow::Result<()>
        where
            F: FnOnce(PgStore) -> Fut,
            Fut: std::future::Future<Output = anyhow::Result<()>>,
        {
            use testcontainers::core::ports::IntoContainerPort;
            use testcontainers::core::wait::WaitFor;
            use testcontainers::runners::AsyncRunner;
            use testcontainers::{GenericImage, ImageExt};

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

            let host_port = container.get_host_port_ipv4(5432u16.tcp()).await?;
            let url = format!("postgres://kb:kb@127.0.0.1:{host_port}/kb?sslmode=disable");

            let store = PgStore::new(&url);
            store.connect().await?;

            // FK constraints on tags require a tenant row.
            let pool = store.pool()?;
            sqlx::query("INSERT INTO tenants (slug, name) VALUES ('test', 'Test')")
                .execute(&pool)
                .await?;

            f(store).await
        }

        #[ignore]
        #[tokio::test]
        async fn upsert_tag_inserts_and_is_idempotent() {
            with_connected_store(|store| async move {
                let id1 = store.upsert_tag(1, "invoice", &[0.1, 0.2, 0.3]).await?;
                assert!(id1 > 0, "expected positive tag id");

                // Same (tenant, name) → same id.
                let id2 = store
                    .upsert_tag(1, "invoice", &[0.999, 0.888, 0.777])
                    .await?;
                assert_eq!(id2, id1, "idempotent upsert must return same id");

                // Different name → different id.
                let id3 = store.upsert_tag(1, "receipt", &[0.1, 0.2, 0.3]).await?;
                assert_ne!(id3, id1);

                Ok(())
            })
            .await
            .unwrap();
        }

        #[ignore]
        #[tokio::test]
        async fn lookup_alias_finds_exact_match() {
            with_connected_store(|store| async move {
                let tag_id = store.upsert_tag(1, "invoice", &[0.1, 0.2, 0.3]).await?;
                store.insert_tag_alias(1, "bill", tag_id).await?;

                let found = store.lookup_alias(1, "bill").await?;
                assert_eq!(found, Some(tag_id));

                // Unknown alias → None.
                let not_found = store.lookup_alias(1, "nonexistent").await?;
                assert_eq!(not_found, None);

                Ok(())
            })
            .await
            .unwrap();
        }

        #[ignore]
        #[tokio::test]
        async fn find_similar_tags_returns_tags_with_embeddings() {
            with_connected_store(|store| async move {
                // Insert a few tags with embeddings.
                store.upsert_tag(1, "invoice", &[1.0, 0.0, 0.0]).await?;
                store.upsert_tag(1, "receipt", &[0.0, 1.0, 0.0]).await?;

                let tags: Vec<(i64, Vec<f32>)> = store.find_similar_tags(1).await?;
                assert_eq!(tags.len(), 2, "expected 2 tags with embeddings");

                // Verify both ids and non-empty vectors returned.
                for (_id, vec) in &tags {
                    assert!(!vec.is_empty(), "embedding must not be empty");
                }

                Ok(())
            })
            .await
            .unwrap();
        }

        #[ignore]
        #[tokio::test]
        async fn insert_document_tags_round_trip() {
            with_connected_store(|store| async move {
                // Need a document row first.
                let pool = store.pool()?;
                let doc_id: i64 = sqlx::query_scalar(
                    "INSERT INTO documents (tenant_id, kind) \
                     VALUES (1, 'document') RETURNING id",
                )
                .fetch_one(&pool)
                .await?;

                let tag1 = store.upsert_tag(1, "alpha", &[0.1, 0.2]).await?;
                let tag2 = store.upsert_tag(1, "beta", &[0.3, 0.4]).await?;

                store.insert_document_tags(doc_id, &[tag1, tag2]).await?;

                // Verify they're in the table.
                let count: i64 =
                    sqlx::query_scalar("SELECT COUNT(*) FROM document_tags WHERE document_id = $1")
                        .bind(doc_id)
                        .fetch_one(&pool)
                        .await?;
                assert_eq!(count, 2);

                // Idempotent re-insert does not duplicate.
                store.insert_document_tags(doc_id, &[tag1, tag2]).await?;
                let count2: i64 =
                    sqlx::query_scalar("SELECT COUNT(*) FROM document_tags WHERE document_id = $1")
                        .bind(doc_id)
                        .fetch_one(&pool)
                        .await?;
                assert_eq!(count2, 2);

                Ok(())
            })
            .await
            .unwrap();
        }

        #[ignore]
        #[tokio::test]
        async fn insert_tag_alias_is_idempotent() {
            with_connected_store(|store| async move {
                let tag_id = store.upsert_tag(1, "primary", &[0.5, 0.5]).await?;

                store.insert_tag_alias(1, "secondary", tag_id).await?;
                // Second insert should not error.
                store.insert_tag_alias(1, "secondary", tag_id).await?;

                // Alias still resolves to the same tag.
                let found = store.lookup_alias(1, "secondary").await?;
                assert_eq!(found, Some(tag_id));

                Ok(())
            })
            .await
            .unwrap();
        }
    }
}
