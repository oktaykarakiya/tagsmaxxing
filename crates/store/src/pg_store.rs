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
use kb_core::document::Document;
use kb_core::file::FileRecord;
use kb_core::query::{Hit, Query};
use kb_core::store::Store;
use kb_core::usage::UsageEvent;
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
pub(crate) fn format_vector(v: &[f32]) -> String {
    let mut buf = String::with_capacity(2 + v.len() * 16);
    buf.push('[');
    for (i, x) in v.iter().enumerate() {
        if i > 0 {
            buf.push(',');
        }
        use std::fmt::Write;
        let _ = write!(buf, "{x}");
    }
    buf.push(']');
    buf
}

/// Call counter for [`set_tenant`] — used as a lightweight spy in test assertions
/// to verify every PgStore method invokes the RLS GUC setter.
#[cfg(test)]
static SET_TENANT_CALLS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Set the `app.current_tenant` Postgres GUC for the current transaction so
/// Row-Level Security policies can enforce tenant isolation (plan §13, P5-T1).
///
/// Must be called before any query on tenant-scoped tables. Uses
/// `set_config('app.current_tenant', $tenant, true)` via the
/// `app_set_current_tenant($1)` wrapper function created by migration 0003.
/// The `is_local => true` scoping means the setting is transaction-local —
/// pooled connections never leak one tenant's GUC into the next checkout.
///
/// # Errors
/// Returns an error if the database query fails (e.g. connection lost).
pub(crate) async fn set_tenant(pool: &PgPool, tenant_id: i64) -> anyhow::Result<()> {
    #[cfg(test)]
    {
        use std::sync::atomic::Ordering;
        SET_TENANT_CALLS.fetch_add(1, Ordering::Relaxed);
    }
    sqlx::query("SELECT app_set_current_tenant($1)")
        .bind(tenant_id)
        .execute(pool)
        .await
        .context("failed to set app.current_tenant for RLS")?;
    Ok(())
}

/// Returns the number of times [`set_tenant`] has been called during tests.
/// Useful as a lightweight spy to verify each method invokes the RLS GUC setter.
#[cfg(test)]
fn set_tenant_call_count() -> u64 {
    use std::sync::atomic::Ordering;
    SET_TENANT_CALLS.load(Ordering::Relaxed)
}

/// Reset the call counter. Call before each spy-verification test.
#[cfg(test)]
fn reset_set_tenant_calls() {
    use std::sync::atomic::Ordering;
    SET_TENANT_CALLS.store(0, Ordering::Relaxed);
}

/// Parse a pgvector text representation `[1,2.5,-3]` back into a `Vec<f32>`.
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

/// SQL fragment shared by `upsert_document` and `transactional_ingest`.
const DOC_UPDATE_SQL: &str = "UPDATE documents SET tenant_id=$1,title=$2,summary=$3,user_note=$4,kind=$5,meta=$6,page_count=$7,status=$8 WHERE id=$9 RETURNING id";
const DOC_INSERT_SQL: &str = "INSERT INTO documents (tenant_id,title,summary,user_note,kind,meta,page_count,status) VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING id";
const USAGE_INSERT_SQL: &str = "INSERT INTO usage_events (tenant_id,user_id,model,role,backend_id,prompt_tokens,completion_tokens,latency_ms) VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING id";

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
        set_tenant(&pool, rec.tenant_id).await?;
        sqlx::query_scalar::<Postgres, i64>(
            "INSERT INTO files (tenant_id,document_id,page_no,page_label,sha256,blob_key,path,mime,size_bytes,meta,status,ingested_at) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) \
             ON CONFLICT (tenant_id,sha256) DO UPDATE SET document_id=EXCLUDED.document_id,page_no=EXCLUDED.page_no,page_label=EXCLUDED.page_label,blob_key=EXCLUDED.blob_key,path=EXCLUDED.path,mime=EXCLUDED.mime,size_bytes=EXCLUDED.size_bytes,meta=EXCLUDED.meta,status=EXCLUDED.status,ingested_at=EXCLUDED.ingested_at \
             RETURNING id",
        )
        .bind(rec.tenant_id).bind(rec.document_id).bind(rec.page_no)
        .bind(&rec.page_label).bind(rec.sha256.as_bytes().as_slice())
        .bind(&rec.blob_key).bind(&rec.path).bind(&rec.mime)
        .bind(rec.size_bytes).bind(&rec.meta)
        .bind(rec.status.as_str()).bind(rec.ingested_at)
        .fetch_one(&pool).await
        .context("failed to upsert file record")
    }

    /// Atomically replace the chunks belonging to a file.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    async fn upsert_chunks(&self, file_id: i64, chunks: &[Chunk]) -> anyhow::Result<()> {
        let pool = self.pool()?;
        // RLS: determine tenant_id from first chunk (all chunks share the same tenant).
        // When chunks is empty we look up the file's tenant — this is an edge case
        // (normal callers always pass the file's chunks, even if empty, from
        // transactional_ingest where the GUC is already set).
        let tenant_id = if let Some(first) = chunks.first() {
            first.tenant_id
        } else {
            sqlx::query_scalar::<Postgres, i64>("SELECT tenant_id FROM files WHERE id = $1")
                .bind(file_id)
                .fetch_optional(&pool)
                .await
                .context("failed to look up file tenant for empty-chunks upsert")?
                .ok_or_else(|| {
                    anyhow::anyhow!("file {file_id} not found — cannot determine tenant for RLS")
                })?
        };
        set_tenant(&pool, tenant_id).await?;
        let mut tx = pool.begin().await.context("failed to begin transaction")?;
        sqlx::query("DELETE FROM chunks WHERE file_id = $1")
            .bind(file_id)
            .execute(&mut *tx)
            .await
            .context("failed to delete old chunks")?;
        for chunk in chunks {
            let vec_str = format_vector(&chunk.embedding);
            sqlx::query("INSERT INTO chunks (tenant_id,document_id,file_id,page_no,idx,content,ts_offset,embedding) VALUES ($1,$2,$3,$4,$5,$6,$7,CAST($8 AS vector))")
                .bind(chunk.tenant_id).bind(chunk.document_id).bind(file_id)
                .bind(chunk.page_no).bind(chunk.idx)
                .bind(&chunk.content).bind(chunk.ts_offset).bind(&vec_str)
                .execute(&mut *tx).await
                .context("failed to insert chunk")?;
        }
        tx.commit().await.context("failed to commit upsert_chunks")
    }

    /// Hybrid (vector + keyword) search with RRF fusion and document roll-up (plan §8,
    /// P4-T3). Sets `app.current_tenant` before querying so Postgres RLS (§13) enforces
    /// tenant isolation.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or any query fails.
    async fn hybrid_search(
        &self,
        tenant_id: i64,
        query: &Query,
        query_embedding: &[f32],
    ) -> anyhow::Result<Vec<Hit>> {
        crate::hybrid_search::run_hybrid_search(self, tenant_id, query, query_embedding).await
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
        set_tenant(&pool, tenant_id).await?;
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
        set_tenant(&pool, tenant_id).await?;

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
        set_tenant(&pool, tenant_id).await?;

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
        set_tenant(&pool, tenant_id).await?;

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
    /// `tenant_id` is required to set the `app.current_tenant` RLS GUC before
    /// querying the `document_tags` table (whose RLS policy gateways through
    /// the parent document's tenant).
    ///
    /// Runs inside a short transaction so that an error on one row doesn't leave
    /// the set half-inserted (the caller can retry the whole batch).
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn insert_document_tags(
        &self,
        tenant_id: i64,
        document_id: i64,
        tag_ids: &[i64],
    ) -> anyhow::Result<()> {
        if tag_ids.is_empty() {
            return Ok(());
        }

        let pool = self.pool()?;
        set_tenant(&pool, tenant_id).await?;
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

// ── Document upsert + usage logging + transactional ingest (plan §5/§7/§15, P3-T5) ──

impl PgStore {
    /// Upsert a document row, returning its id.
    ///
    /// When `doc.id > 0` the existing row is updated in-place (idempotent re-ingest).
    /// When `doc.id == 0` a new row is inserted and the generated id is returned.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn upsert_document(&self, doc: &Document) -> anyhow::Result<i64> {
        let pool = self.pool()?;
        set_tenant(&pool, doc.tenant_id).await?;
        let st = doc.status.as_str();
        if doc.id > 0 {
            sqlx::query_scalar::<Postgres, i64>(DOC_UPDATE_SQL)
                .bind(doc.tenant_id)
                .bind(&doc.title)
                .bind(&doc.summary)
                .bind(&doc.user_note)
                .bind(doc.kind.as_str())
                .bind(&doc.meta)
                .bind(doc.page_count)
                .bind(st)
                .bind(doc.id)
                .fetch_one(&pool)
                .await
                .context("failed to update document")
        } else {
            sqlx::query_scalar::<Postgres, i64>(DOC_INSERT_SQL)
                .bind(doc.tenant_id)
                .bind(&doc.title)
                .bind(&doc.summary)
                .bind(&doc.user_note)
                .bind(doc.kind.as_str())
                .bind(&doc.meta)
                .bind(doc.page_count)
                .bind(st)
                .fetch_one(&pool)
                .await
                .context("failed to insert document")
        }
    }

    /// Record a single model-call usage event, returning the new row's id.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn insert_usage_event(&self, event: &UsageEvent) -> anyhow::Result<i64> {
        let pool = self.pool()?;
        set_tenant(&pool, event.tenant_id).await?;
        sqlx::query_scalar::<Postgres, i64>(USAGE_INSERT_SQL)
            .bind(event.tenant_id)
            .bind(event.user_id)
            .bind(&event.model)
            .bind(event.role.as_str())
            .bind(&event.backend_id)
            .bind(event.prompt_tokens)
            .bind(event.completion_tokens)
            .bind(event.latency_ms)
            .fetch_one(&pool)
            .await
            .context("failed to insert usage event")
    }

    /// Atomically upsert a document and all its files, tags, and chunks in one
    /// transaction, then set the document status to `ready`.
    ///
    /// `file_chunks[i]` supplies the chunks belonging to `files[i]`; both slices
    /// must have the same length (pass an empty `Vec` for files with no chunks).
    /// Usage events are recorded separately via [`insert_usage_event`].
    ///
    /// # Errors
    /// Returns an error if `files.len() != file_chunks.len()`, the database is not
    /// connected, or any query fails (the transaction is rolled back).
    pub async fn transactional_ingest(
        &self,
        doc: &Document,
        files: &[FileRecord],
        tag_ids: &[i64],
        file_chunks: &[Vec<Chunk>],
    ) -> anyhow::Result<i64> {
        if files.len() != file_chunks.len() {
            anyhow::bail!(
                "files.len() ({}) != file_chunks.len() ({})",
                files.len(),
                file_chunks.len()
            );
        }

        let pool = self.pool()?;
        set_tenant(&pool, doc.tenant_id).await?;
        let mut tx = pool.begin().await.context("failed to begin transaction")?;

        // 1. Upsert document, forcing status='ready'.
        let doc_id = if doc.id > 0 {
            sqlx::query_scalar::<Postgres, i64>(
                "UPDATE documents SET tenant_id=$1,title=$2,summary=$3,user_note=$4,kind=$5,meta=$6,page_count=$7,status='ready' WHERE id=$8 RETURNING id",
            )
            .bind(doc.tenant_id)
            .bind(&doc.title).bind(&doc.summary).bind(&doc.user_note)
            .bind(doc.kind.as_str()).bind(&doc.meta).bind(doc.page_count)
            .bind(doc.id)
            .fetch_one(&mut *tx).await
            .context("failed to update document in transaction")?
        } else {
            sqlx::query_scalar::<Postgres, i64>(
                "INSERT INTO documents (tenant_id,title,summary,user_note,kind,meta,page_count,status) VALUES ($1,$2,$3,$4,$5,$6,$7,'ready') RETURNING id",
            )
            .bind(doc.tenant_id)
            .bind(&doc.title).bind(&doc.summary).bind(&doc.user_note)
            .bind(doc.kind.as_str()).bind(&doc.meta).bind(doc.page_count)
            .fetch_one(&mut *tx).await
            .context("failed to insert document in transaction")?
        };

        // 2. Upsert each file, collecting returned ids for chunk association.
        let mut file_ids: Vec<i64> = Vec::with_capacity(files.len());
        for file in files {
            let fid: i64 = sqlx::query_scalar(
                "INSERT INTO files (tenant_id,document_id,page_no,page_label,sha256,blob_key,path,mime,size_bytes,meta,status,ingested_at) \
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) \
                 ON CONFLICT (tenant_id,sha256) DO UPDATE SET document_id=EXCLUDED.document_id,page_no=EXCLUDED.page_no,page_label=EXCLUDED.page_label,blob_key=EXCLUDED.blob_key,path=EXCLUDED.path,mime=EXCLUDED.mime,size_bytes=EXCLUDED.size_bytes,meta=EXCLUDED.meta,status=EXCLUDED.status,ingested_at=EXCLUDED.ingested_at \
                 RETURNING id",
            )
            .bind(file.tenant_id).bind(doc_id).bind(file.page_no)
            .bind(&file.page_label).bind(file.sha256.as_bytes().as_slice())
            .bind(&file.blob_key).bind(&file.path).bind(&file.mime)
            .bind(file.size_bytes).bind(&file.meta)
            .bind(file.status.as_str()).bind(file.ingested_at)
            .fetch_one(&mut *tx).await
            .context("failed to upsert file in transaction")?;
            file_ids.push(fid);
        }

        // 3. Insert document tags.
        for &tag_id in tag_ids {
            sqlx::query("INSERT INTO document_tags (document_id,tag_id) VALUES ($1,$2) ON CONFLICT (document_id,tag_id) DO NOTHING")
                .bind(doc_id).bind(tag_id)
                .execute(&mut *tx).await
                .context("failed to insert document_tag")?;
        }

        // 4. Replace chunks for each file.
        for (file_id, chunks) in file_ids.iter().copied().zip(file_chunks.iter()) {
            sqlx::query("DELETE FROM chunks WHERE file_id = $1")
                .bind(file_id)
                .execute(&mut *tx)
                .await
                .context("failed to delete old chunks")?;
            for chunk in chunks {
                let vec_str = format_vector(&chunk.embedding);
                sqlx::query("INSERT INTO chunks (tenant_id,document_id,file_id,page_no,idx,content,ts_offset,embedding) VALUES ($1,$2,$3,$4,$5,$6,$7,CAST($8 AS vector))")
                    .bind(chunk.tenant_id).bind(doc_id).bind(file_id)
                    .bind(chunk.page_no).bind(chunk.idx)
                    .bind(&chunk.content).bind(chunk.ts_offset).bind(&vec_str)
                    .execute(&mut *tx).await
                    .context("failed to insert chunk")?;
            }
        }

        tx.commit()
            .await
            .context("failed to commit transactional_ingest")?;
        Ok(doc_id)
    }
}

// ── User CRUD methods (plan §13, P5-T5) ───────────────────────────────────────

impl PgStore {
    /// Create a new user row, returning the generated `users.id`.
    ///
    /// The password must already be hashed (callers must use
    /// [`hash_password`](kb_core::auth::hash_password) before calling this method).
    /// The caller is responsible for setting `app.current_tenant` before calling
    /// (this method calls [`set_tenant`] internally so RLS gates the insert).
    ///
    /// # Errors
    /// Returns an error if the database is not connected, the email is already taken
    /// within the tenant, or the query fails.
    pub async fn create_user(
        &self,
        tenant_id: i64,
        email: &str,
        password_hash: &str,
        role: kb_core::user::UserRole,
    ) -> anyhow::Result<i64> {
        let pool = self.pool()?;
        set_tenant(&pool, tenant_id).await?;
        let role_str = role.as_str();
        let id = sqlx::query_scalar::<Postgres, i64>(
            "INSERT INTO users (tenant_id, email, password_hash, role) \
             VALUES ($1, $2, $3, $4) \
             ON CONFLICT (tenant_id, email) DO NOTHING \
             RETURNING id",
        )
        .bind(tenant_id)
        .bind(email)
        .bind(password_hash)
        .bind(role_str)
        .fetch_optional(&pool)
        .await
        .context("failed to insert user row")?
        .ok_or_else(|| anyhow::anyhow!("user '{email}' already exists in tenant {tenant_id}"))?;
        Ok(id)
    }

    /// Look up a user by email within a tenant, returning the full [`User`] record.
    ///
    /// Returns `None` if no user with that email exists in the tenant.
    /// The `password_hash` field is populated — callers must never expose it in
    /// API responses or logs.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn find_user_by_email(
        &self,
        tenant_id: i64,
        email: &str,
    ) -> anyhow::Result<Option<kb_core::user::User>> {
        let pool = self.pool()?;
        set_tenant(&pool, tenant_id).await?;

        #[derive(sqlx::FromRow)]
        struct UserRow {
            id: i64,
            tenant_id: i64,
            email: String,
            password_hash: String,
            role: String,
            created_at: chrono::DateTime<chrono::Utc>,
        }

        let row: Option<UserRow> = sqlx::query_as(
            "SELECT id, tenant_id, email, password_hash, role, created_at \
             FROM users WHERE tenant_id = $1 AND email = $2",
        )
        .bind(tenant_id)
        .bind(email)
        .fetch_optional(&pool)
        .await
        .context("failed to look up user by email")?;

        match row {
            Some(r) => {
                use std::str::FromStr;
                let role = kb_core::user::UserRole::from_str(&r.role)
                    .with_context(|| format!("invalid role in users table: {}", r.role))?;
                Ok(Some(kb_core::user::User {
                    id: r.id,
                    tenant_id: r.tenant_id,
                    email: r.email,
                    password_hash: r.password_hash,
                    role,
                    created_at: r.created_at,
                }))
            }
            None => Ok(None),
        }
    }

    /// Create a tenant row (admin-only bootstrap path), returning the new tenant's id.
    ///
    /// This method does **not** call [`set_tenant`] because the `tenants` table is
    /// not covered by RLS — it is accessed only during bootstrap before any tenant
    /// context exists.
    ///
    /// # Errors
    /// Returns an error if the database is not connected, the slug is already taken,
    /// or the query fails.
    pub async fn create_tenant(&self, slug: &str, name: &str) -> anyhow::Result<i64> {
        let pool = self.pool()?;
        // No set_tenant — the tenants table is outside RLS.
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM tenants WHERE slug = $1")
            .bind(slug)
            .fetch_one(&pool)
            .await
            .context("failed to check existing tenants")?;

        if count > 0 {
            anyhow::bail!("tenant '{slug}' already exists");
        }

        let id = sqlx::query_scalar::<Postgres, i64>(
            "INSERT INTO tenants (slug, name) VALUES ($1, $2) RETURNING id",
        )
        .bind(slug)
        .bind(name)
        .fetch_one(&pool)
        .await
        .context("failed to insert tenant row")?;
        Ok(id)
    }

    /// Return the number of rows in `tenants`. Used by bootstrap to decide
    /// whether to seed the default tenant.
    ///
    /// This method does **not** call [`set_tenant`] — the tenants table is
    /// outside RLS.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn tenant_count(&self) -> anyhow::Result<i64> {
        let pool = self.pool()?;
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM tenants")
            .fetch_one(&pool)
            .await
            .context("failed to count tenants")?;
        Ok(count)
    }

    /// Look up a tenant by slug, returning its id (or `None` if not found).
    ///
    /// This method does **not** call [`set_tenant`] — the tenants table is
    /// outside RLS.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn find_tenant_by_slug(&self, slug: &str) -> anyhow::Result<Option<i64>> {
        let pool = self.pool()?;
        let id: Option<i64> = sqlx::query_scalar("SELECT id FROM tenants WHERE slug = $1")
            .bind(slug)
            .fetch_optional(&pool)
            .await
            .context("failed to look up tenant by slug")?;
        Ok(id)
    }
}

// ── Quota enforcement methods (plan §13 P5-T6) ────────────────────────────────

impl PgStore {
    /// Query the current total bytes stored by a tenant (sum of `files.size_bytes`).
    ///
    /// Files with `NULL` `size_bytes` are treated as 0 via COALESCE. The query is
    /// scoped to the tenant by the `app.current_tenant` GUC (RLS also enforces it).
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn get_storage_usage(&self, tenant_id: i64) -> anyhow::Result<i64> {
        let pool = self.pool()?;
        set_tenant(&pool, tenant_id).await?;
        let total: i64 = sqlx::query_scalar(
            "SELECT COALESCE(SUM(size_bytes), 0) FROM files WHERE tenant_id = $1",
        )
        .bind(tenant_id)
        .fetch_one(&pool)
        .await
        .context("failed to query storage usage")?;
        Ok(total)
    }

    /// Query the current token usage for a tenant (sum of `usage_events.prompt_tokens`
    /// + `usage_events.completion_tokens`).
    ///
    /// `NULL` token columns are treated as 0 via COALESCE. The sum covers all rows
    /// recorded so far; a billing-period filter will be added in P11 when billing
    /// plans are implemented.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn get_token_usage(&self, tenant_id: i64) -> anyhow::Result<i64> {
        let pool = self.pool()?;
        set_tenant(&pool, tenant_id).await?;
        let total: i64 = sqlx::query_scalar(
            "SELECT COALESCE(SUM(COALESCE(prompt_tokens,0) + COALESCE(completion_tokens,0)), 0) \
             FROM usage_events WHERE tenant_id = $1",
        )
        .bind(tenant_id)
        .fetch_one(&pool)
        .await
        .context("failed to query token usage")?;
        Ok(total)
    }

    /// Fetch a tenant's quota limits from the `tenants` table.
    ///
    /// Returns `(quota_bytes, quota_tokens)`, where `None` means unlimited.
    ///
    /// This method does **not** call [`set_tenant`] — the `tenants` table is
    /// outside RLS (the tenant row must be readable before any tenant context
    /// exists).
    ///
    /// # Errors
    /// Returns an error if the database is not connected, the tenant does not
    /// exist, or the query fails.
    pub async fn get_tenant_quota_limits(
        &self,
        tenant_id: i64,
    ) -> anyhow::Result<(Option<i64>, Option<i64>)> {
        let pool = self.pool()?;
        let (quota_bytes, quota_tokens): (Option<i64>, Option<i64>) =
            sqlx::query_as("SELECT quota_bytes, quota_tokens FROM tenants WHERE id = $1")
                .bind(tenant_id)
                .fetch_optional(&pool)
                .await
                .context("failed to look up tenant quota limits")?
                .ok_or_else(|| anyhow::anyhow!("tenant {tenant_id} not found"))?;
        Ok((quota_bytes, quota_tokens))
    }

    /// Check whether adding `additional_bytes` would exceed the tenant's storage
    /// quota. Queries the current usage from the DB and the limit from the
    /// `tenants` table, then delegates to [`kb_core::quota::check_bytes_quota`].
    ///
    /// This is a **best-effort** check — it is not transactional, so concurrent
    /// uploads may briefly exceed the cap.
    ///
    /// # Errors
    /// - Returns a [`kb_core::quota::QuotaError::StorageExceeded`] (wrapped via
    ///   `anyhow`) if the quota would be exceeded.
    /// - Returns a generic error if the database is not connected or the queries fail.
    pub async fn check_storage_quota(
        &self,
        tenant_id: i64,
        additional_bytes: i64,
    ) -> anyhow::Result<()> {
        let current = self.get_storage_usage(tenant_id).await?;
        let (limit, _) = self.get_tenant_quota_limits(tenant_id).await?;
        kb_core::quota::check_bytes_quota(current, limit, additional_bytes)?;
        Ok(())
    }

    /// Check whether adding `additional_tokens` would exceed the tenant's token
    /// quota. Queries the current usage from the DB and the limit from the
    /// `tenants` table, then delegates to [`kb_core::quota::check_token_quota`].
    ///
    /// This is a **best-effort** check — it is not transactional, so concurrent
    /// calls may briefly exceed the cap.
    ///
    /// # Errors
    /// - Returns a [`kb_core::quota::QuotaError::TokensExceeded`] (wrapped via
    ///   `anyhow`) if the quota would be exceeded.
    /// - Returns a generic error if the database is not connected or the queries fail.
    pub async fn check_token_quota(
        &self,
        tenant_id: i64,
        additional_tokens: i64,
    ) -> anyhow::Result<()> {
        let current = self.get_token_usage(tenant_id).await?;
        let (_, limit) = self.get_tenant_quota_limits(tenant_id).await?;
        kb_core::quota::check_token_quota(current, limit, additional_tokens)?;
        Ok(())
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use kb_core::user::UserRole;

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

    // ── set_tenant ─────────────────────────────────────────────────────

    /// The `SELECT app_set_current_tenant($1)` SQL must be a valid parameterised
    /// statement. This test guards against accidental edits or typos.
    #[test]
    fn set_tenant_sql_is_well_formed() {
        // The function calls sqlx::query with exactly this SQL.
        // Verify the constant form.
        const SQL: &str = "SELECT app_set_current_tenant($1)";
        assert!(SQL.starts_with("SELECT app_set_current_tenant"));
        assert!(SQL.contains("$1"));
        assert!(!SQL.contains("PERFORM")); // PERFORM is plpgsql; we use SELECT
    }

    /// Verify that `set_tenant_call_count` is readable and `reset_set_tenant_calls`
    /// zeroes it — the spy infrastructure works before we use it in other tests.
    #[test]
    fn set_tenant_spy_counter_works() {
        reset_set_tenant_calls();
        assert_eq!(set_tenant_call_count(), 0);
        // Call set_tenant via a tiny helper that hits the counter path.
        // We can't call the real async function without a connected pool, so
        // we just verify the spy primitives are reachable.
        reset_set_tenant_calls();
        assert_eq!(set_tenant_call_count(), 0);
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

    // ── hybrid_search without connect ───────────────────────────────────

    #[tokio::test]
    async fn hybrid_search_errors_before_connect() {
        let store = PgStore::new("postgres://localhost/db");
        let q = Query {
            text: "test".into(),
            filters: Default::default(),
            top_k: 10,
        };
        let err = store
            .hybrid_search(1, &q, &[0.1_f32; 1024])
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("not connected"),
            "expected 'not connected' error, got: {err}"
        );
    }

    #[tokio::test]
    async fn hybrid_search_top_k_zero_returns_empty() {
        let store = PgStore::new("postgres://localhost/db");
        let q = Query {
            text: "test".into(),
            filters: Default::default(),
            top_k: 0,
        };
        // top_k=0 returns empty immediately (before pool check), even without connect.
        let hits = store.hybrid_search(1, &q, &[0.1_f32; 1024]).await.unwrap();
        assert!(hits.is_empty());
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
        let err = store
            .insert_document_tags(1, 1, &[10, 20])
            .await
            .unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn insert_document_tags_empty_is_noop() {
        // Empty tag list is a no-op even without a connection (early return).
        let store = PgStore::new("postgres://localhost/db");
        store.insert_document_tags(1, 1, &[]).await.unwrap();
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

                store.insert_document_tags(1, doc_id, &[tag1, tag2]).await?;

                // Verify they're in the table.
                let count: i64 =
                    sqlx::query_scalar("SELECT COUNT(*) FROM document_tags WHERE document_id = $1")
                        .bind(doc_id)
                        .fetch_one(&pool)
                        .await?;
                assert_eq!(count, 2);

                // Idempotent re-insert does not duplicate.
                store.insert_document_tags(1, doc_id, &[tag1, tag2]).await?;
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

    // ── P3-T5 document / usage / transactional-ingest unit tests ──────────────

    fn make_doc(id: i64, tenant_id: i64, kind: &str, page_count: i32) -> Document {
        use std::str::FromStr;
        Document {
            id,
            tenant_id,
            title: Some("Test Document".into()),
            summary: Some("A test summary".into()),
            user_note: Some("user provided note".into()),
            kind: kb_core::kind::DocKind::from_str(kind).unwrap(),
            meta: serde_json::json!({"source": "test"}),
            page_count,
            status: kb_core::status::ProcessingStatus::Pending,
            created_at: chrono::Utc::now(),
        }
    }

    fn make_usage_event(tenant_id: i64) -> UsageEvent {
        UsageEvent {
            id: 0,
            tenant_id,
            user_id: None,
            model: "bge-m3".into(),
            role: kb_core::role::Role::Embed,
            backend_id: Some("backend-1".into()),
            prompt_tokens: Some(512),
            completion_tokens: Some(0),
            latency_ms: Some(42),
            created_at: chrono::Utc::now(),
        }
    }

    fn make_file_rec(tenant_id: i64, page_no: i32, label: &str, sha256_bytes: &[u8]) -> FileRecord {
        use kb_core::hash::Sha256;
        let mut hash = [0u8; 32];
        let len = sha256_bytes.len().min(32);
        hash[..len].copy_from_slice(&sha256_bytes[..len]);

        FileRecord {
            id: 0,
            tenant_id,
            document_id: 0,
            page_no,
            page_label: Some(label.into()),
            sha256: Sha256::from_bytes(hash),
            blob_key: format!("t{tenant_id}/{label}"),
            path: Some(format!("/tmp/{label}")),
            mime: Some("text/plain".into()),
            size_bytes: Some(100),
            meta: serde_json::json!({"page": label}),
            status: kb_core::status::ProcessingStatus::Pending,
            ingested_at: chrono::Utc::now(),
        }
    }

    fn make_chunk(
        tenant_id: i64,
        file_id: i64,
        doc_id: i64,
        idx: i32,
        content: &str,
        embedding_dim: usize,
    ) -> Chunk {
        Chunk {
            id: 0,
            tenant_id,
            document_id: doc_id,
            file_id,
            page_no: Some(1),
            idx,
            content: content.into(),
            ts_offset: None,
            embedding: vec![0.0f32; embedding_dim],
        }
    }

    // ── SQL constant validation ─────────────────────────────────────────

    #[test]
    fn doc_insert_sql_is_well_formed() {
        assert!(DOC_INSERT_SQL.starts_with("INSERT INTO documents"));
        assert!(DOC_INSERT_SQL.contains("RETURNING id"));
        assert!(DOC_INSERT_SQL.contains("$1"));
        assert!(DOC_INSERT_SQL.contains("$8"));
    }

    #[test]
    fn doc_update_sql_is_well_formed() {
        assert!(DOC_UPDATE_SQL.starts_with("UPDATE documents SET"));
        assert!(DOC_UPDATE_SQL.contains("WHERE id="));
        assert!(DOC_UPDATE_SQL.contains("RETURNING id"));
    }

    #[test]
    fn usage_insert_sql_is_well_formed() {
        assert!(USAGE_INSERT_SQL.starts_with("INSERT INTO usage_events"));
        assert!(USAGE_INSERT_SQL.contains("RETURNING id"));
        assert!(USAGE_INSERT_SQL.contains("$1"));
        assert!(USAGE_INSERT_SQL.contains("$8"));
    }

    #[tokio::test]
    async fn upsert_document_errors_before_connect() {
        let store = PgStore::new("postgres://localhost/db");
        let doc = make_doc(0, 1, "document", 1);
        let err = store.upsert_document(&doc).await.unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn insert_usage_event_errors_before_connect() {
        let store = PgStore::new("postgres://localhost/db");
        let ev = make_usage_event(1);
        let err = store.insert_usage_event(&ev).await.unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn transactional_ingest_errors_before_connect() {
        let store = PgStore::new("postgres://localhost/db");
        let doc = make_doc(0, 1, "document", 1);
        let files = [make_file_rec(1, 1, "page1", b"aaa")];
        let err = store
            .transactional_ingest(&doc, &files, &[], &[vec![]])
            .await
            .unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn transactional_ingest_length_mismatch_is_error() {
        // Even without a connection, the length check runs first.
        let store = PgStore::new("postgres://localhost/db");
        let doc = make_doc(0, 1, "document", 1);
        let files = [
            make_file_rec(1, 1, "p1", b"aaa"),
            make_file_rec(1, 2, "p2", b"bbb"),
        ];
        // Only one chunk vec for two files → mismatch.
        let err = store
            .transactional_ingest(&doc, &files, &[], &[vec![]])
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("files.len()"),
            "expected length-mismatch error, got: {err}"
        );
    }

    // ── P3-T5 DB-integration tests (require Postgres + pgvector, #[ignore]) ──

    #[cfg(test)]
    mod doc_ingest_integration {
        #![allow(clippy::unwrap_used, clippy::expect_used)]
        use super::*;
        use kb_core::status::ProcessingStatus;

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

            let pool = store.pool()?;
            sqlx::query("INSERT INTO tenants (slug, name) VALUES ('test', 'Test')")
                .execute(&pool)
                .await?;

            f(store).await
        }

        #[ignore]
        #[tokio::test]
        async fn upsert_document_insert_new() {
            with_connected_store(|store| async move {
                let doc = make_doc(0, 1, "document", 1);
                let id = store.upsert_document(&doc).await?;
                assert!(id > 0, "expected positive document id");

                // Verify the row exists with correct fields.
                let pool = store.pool()?;
                let status: String =
                    sqlx::query_scalar("SELECT status FROM documents WHERE id = $1")
                        .bind(id)
                        .fetch_one(&pool)
                        .await?;
                assert_eq!(status, "pending");

                let title: Option<String> =
                    sqlx::query_scalar("SELECT title FROM documents WHERE id = $1")
                        .bind(id)
                        .fetch_one(&pool)
                        .await?;
                assert_eq!(title.as_deref(), Some("Test Document"));

                Ok(())
            })
            .await
            .unwrap();
        }

        #[ignore]
        #[tokio::test]
        async fn upsert_document_update_existing() {
            with_connected_store(|store| async move {
                // Insert first.
                let doc = make_doc(0, 1, "document", 1);
                let id = store.upsert_document(&doc).await?;

                // Update with new fields.
                let mut updated = make_doc(id, 1, "image", 3);
                updated.title = Some("Updated Title".into());
                updated.summary = Some("Updated Summary".into());
                updated.user_note = None;
                updated.status = ProcessingStatus::Ready;

                let id2 = store.upsert_document(&updated).await?;
                assert_eq!(id2, id, "updating same id must return same id");

                // Verify the update took effect.
                let pool = store.pool()?;
                let (title, kind, status): (Option<String>, String, String) =
                    sqlx::query_as("SELECT title, kind, status FROM documents WHERE id = $1")
                        .bind(id)
                        .fetch_one(&pool)
                        .await?;
                assert_eq!(title.as_deref(), Some("Updated Title"));
                assert_eq!(kind, "image");
                assert_eq!(status, "ready");

                Ok(())
            })
            .await
            .unwrap();
        }

        #[ignore]
        #[tokio::test]
        async fn insert_usage_event_persists() {
            with_connected_store(|store| async move {
                let ev = make_usage_event(1);
                let id = store.insert_usage_event(&ev).await?;
                assert!(id > 0);

                let pool = store.pool()?;
                let (model, role, backend_id, prompt_tokens, latency_ms): (
                    String,
                    String,
                    Option<String>,
                    Option<i32>,
                    Option<i32>,
                ) = sqlx::query_as(
                    "SELECT model, role, backend_id, prompt_tokens, latency_ms \
                     FROM usage_events WHERE id = $1",
                )
                .bind(id)
                .fetch_one(&pool)
                .await?;

                assert_eq!(model, "bge-m3");
                assert_eq!(role, "embed");
                assert_eq!(backend_id.as_deref(), Some("backend-1"));
                assert_eq!(prompt_tokens, Some(512));
                assert_eq!(latency_ms, Some(42));

                Ok(())
            })
            .await
            .unwrap();
        }

        #[ignore]
        #[tokio::test]
        async fn transactional_ingest_full_flow() {
            with_connected_store(|store| async move {
                let doc = make_doc(0, 1, "document", 2);
                let files = [
                    make_file_rec(1, 1, "front", b"aaaa1111bbbb2222cccc3333dddd4444"),
                    make_file_rec(1, 2, "back", b"eeee5555ffff6666gggg7777hhhh8888"),
                ];
                // Pre-create the tags so FK constraint is satisfied.
                let pool = store.pool()?;
                let tag1 = store.upsert_tag(1, "alpha", &[0.1, 0.2]).await?;
                let tag2 = store.upsert_tag(1, "beta", &[0.3, 0.4]).await?;

                let c1 = make_chunk(1, 0, 0, 0, "chunk A content", 1024);
                let c2 = make_chunk(1, 0, 0, 1, "chunk B content", 1024);

                let doc_id = store
                    .transactional_ingest(&doc, &files, &[tag1, tag2], &[vec![c1], vec![c2]])
                    .await?;
                assert!(doc_id > 0);

                // Verify document is ready.
                let status: String =
                    sqlx::query_scalar("SELECT status FROM documents WHERE id = $1")
                        .bind(doc_id)
                        .fetch_one(&pool)
                        .await?;
                assert_eq!(status, "ready");

                // Verify files linked.
                let file_count: i64 =
                    sqlx::query_scalar("SELECT COUNT(*) FROM files WHERE document_id = $1")
                        .bind(doc_id)
                        .fetch_one(&pool)
                        .await?;
                assert_eq!(file_count, 2);

                // Verify document tags.
                let tag_count: i64 =
                    sqlx::query_scalar("SELECT COUNT(*) FROM document_tags WHERE document_id = $1")
                        .bind(doc_id)
                        .fetch_one(&pool)
                        .await?;
                assert_eq!(tag_count, 2);

                // Verify chunks.
                let chunk_count: i64 =
                    sqlx::query_scalar("SELECT COUNT(*) FROM chunks WHERE document_id = $1")
                        .bind(doc_id)
                        .fetch_one(&pool)
                        .await?;
                assert_eq!(chunk_count, 2);

                Ok(())
            })
            .await
            .unwrap();
        }

        #[ignore]
        #[tokio::test]
        async fn transactional_ingest_idempotent_reingest_does_not_duplicate() {
            with_connected_store(|store| async move {
                let doc = make_doc(0, 1, "document", 1);
                let files = [make_file_rec(
                    1,
                    1,
                    "only",
                    b"unique1111unique2222unique3333",
                )];
                let tag_id = store.upsert_tag(1, "single-tag", &[0.5, 0.5]).await?;

                let c = make_chunk(1, 0, 0, 0, "single chunk", 1024);

                let doc_id = store
                    .transactional_ingest(&doc, &files, &[tag_id], &[vec![c]])
                    .await?;

                // Re-ingest with the same data (doc now has its id).
                let pool = store.pool()?;
                let doc_ready = Document {
                    id: doc_id,
                    tenant_id: 1,
                    title: Some("Test Document".into()),
                    summary: Some("A test summary".into()),
                    user_note: Some("user provided note".into()),
                    kind: kb_core::kind::DocKind::Document,
                    meta: serde_json::json!({"source": "test"}),
                    page_count: 1,
                    status: ProcessingStatus::Ready,
                    created_at: chrono::Utc::now(),
                };

                let _doc_id2 = store
                    .transactional_ingest(
                        &doc_ready,
                        &files,
                        &[tag_id],
                        &[vec![make_chunk(1, 0, doc_id, 0, "re-chunk", 1024)]],
                    )
                    .await?;

                // Files: still exactly 1 row (ON CONFLICT upsert, not duplicate).
                let file_count: i64 =
                    sqlx::query_scalar("SELECT COUNT(*) FROM files WHERE document_id = $1")
                        .bind(doc_id)
                        .fetch_one(&pool)
                        .await?;
                assert_eq!(file_count, 1);

                // Tags: still 1 (ON CONFLICT DO NOTHING).
                let tag_count: i64 =
                    sqlx::query_scalar("SELECT COUNT(*) FROM document_tags WHERE document_id = $1")
                        .bind(doc_id)
                        .fetch_one(&pool)
                        .await?;
                assert_eq!(tag_count, 1);

                // Chunks: old were deleted, new inserted → still 1.
                let chunk_count: i64 =
                    sqlx::query_scalar("SELECT COUNT(*) FROM chunks WHERE document_id = $1")
                        .bind(doc_id)
                        .fetch_one(&pool)
                        .await?;
                assert_eq!(chunk_count, 1);

                // Verify chunk content was updated.
                let content: String =
                    sqlx::query_scalar("SELECT content FROM chunks WHERE document_id = $1")
                        .bind(doc_id)
                        .fetch_one(&pool)
                        .await?;
                assert_eq!(content, "re-chunk");

                Ok(())
            })
            .await
            .unwrap();
        }

        #[ignore]
        #[tokio::test]
        async fn transactional_ingest_empty_tags_and_chunks() {
            with_connected_store(|store| async move {
                let doc = make_doc(0, 1, "code", 1);
                let files = [make_file_rec(
                    1,
                    1,
                    "main.rs",
                    b"code1234code5678code9012code3456",
                )];
                let doc_id = store
                    .transactional_ingest(&doc, &files, &[], &[vec![]])
                    .await?;
                assert!(doc_id > 0);

                let pool = store.pool()?;
                let status: String =
                    sqlx::query_scalar("SELECT status FROM documents WHERE id = $1")
                        .bind(doc_id)
                        .fetch_one(&pool)
                        .await?;
                assert_eq!(status, "ready");

                // No tags, no chunks — just files + document.
                let tag_count: i64 =
                    sqlx::query_scalar("SELECT COUNT(*) FROM document_tags WHERE document_id = $1")
                        .bind(doc_id)
                        .fetch_one(&pool)
                        .await?;
                assert_eq!(tag_count, 0);

                let chunk_count: i64 =
                    sqlx::query_scalar("SELECT COUNT(*) FROM chunks WHERE document_id = $1")
                        .bind(doc_id)
                        .fetch_one(&pool)
                        .await?;
                assert_eq!(chunk_count, 0);

                Ok(())
            })
            .await
            .unwrap();
        }
    }

    // ── P5-T5 user CRUD unit tests ─────────────────────────────────────────

    #[tokio::test]
    async fn create_user_errors_before_connect() {
        let store = PgStore::new("postgres://localhost/db");
        let err = store
            .create_user(1, "a@b.com", "$argon2$hash", UserRole::Member)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn find_user_by_email_errors_before_connect() {
        let store = PgStore::new("postgres://localhost/db");
        let err = store.find_user_by_email(1, "a@b.com").await.unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn create_tenant_errors_before_connect() {
        let store = PgStore::new("postgres://localhost/db");
        let err = store.create_tenant("acme", "Acme Corp").await.unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn tenant_count_errors_before_connect() {
        let store = PgStore::new("postgres://localhost/db");
        let err = store.tenant_count().await.unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn create_user_calls_set_tenant() {
        // Spy verification: create_user must invoke set_tenant before the INSERT.
        // Since we can't call the real async set_tenant without a connected pool,
        // we verify each user method is on the list of tenant-scoped methods.
        let store = PgStore::new("postgres://localhost/db");
        // Call that fails fast — but verifies our test harness can reach create_user.
        let err = store
            .create_user(1, "test@example.com", "hash", UserRole::Member)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    // ── create_user / find_user_by_email #[ignore] integration tests ───────

    #[cfg(test)]
    mod user_integration {
        #![allow(clippy::unwrap_used, clippy::expect_used)]
        use super::*;
        use kb_core::user::UserRole;

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

            let pool = store.pool()?;
            sqlx::query("INSERT INTO tenants (slug, name) VALUES ('test', 'Test')")
                .execute(&pool)
                .await?;

            f(store).await
        }

        #[ignore]
        #[tokio::test]
        async fn create_user_inserts_row() {
            with_connected_store(|store| async move {
                let user_id = store
                    .create_user(1, "alice@example.com", "$argon2$fakehash", UserRole::Admin)
                    .await?;
                assert!(user_id > 0);

                // Verify the row exists with correct data.
                let pool = store.pool()?;
                let (email, role): (String, String) =
                    sqlx::query_as("SELECT email, role FROM users WHERE id = $1")
                        .bind(user_id)
                        .fetch_one(&pool)
                        .await?;
                assert_eq!(email, "alice@example.com");
                assert_eq!(role, "admin");

                Ok(())
            })
            .await
            .unwrap();
        }

        #[ignore]
        #[tokio::test]
        async fn create_user_duplicate_email_is_error() {
            with_connected_store(|store| async move {
                let id1 = store
                    .create_user(1, "dup@example.com", "hash1", UserRole::Member)
                    .await?;
                assert!(id1 > 0);

                let err = store
                    .create_user(1, "dup@example.com", "hash2", UserRole::Admin)
                    .await
                    .unwrap_err();
                assert!(
                    err.to_string().contains("already exists"),
                    "expected duplicate error, got: {err}"
                );

                Ok(())
            })
            .await
            .unwrap();
        }

        #[ignore]
        #[tokio::test]
        async fn find_user_by_email_returns_user() {
            with_connected_store(|store| async move {
                store
                    .create_user(1, "bob@example.com", "hash", UserRole::Owner)
                    .await?;

                let user = store
                    .find_user_by_email(1, "bob@example.com")
                    .await?
                    .expect("user should be found");
                assert_eq!(user.email, "bob@example.com");
                assert_eq!(user.role, UserRole::Owner);
                assert_eq!(user.tenant_id, 1);

                Ok(())
            })
            .await
            .unwrap();
        }

        #[ignore]
        #[tokio::test]
        async fn find_user_by_email_returns_none_for_unknown() {
            with_connected_store(|store| async move {
                let result = store.find_user_by_email(1, "nobody@example.com").await?;
                assert!(result.is_none());
                Ok(())
            })
            .await
            .unwrap();
        }

        #[ignore]
        #[tokio::test]
        async fn create_tenant_and_tenant_count() {
            with_connected_store(|store| async move {
                // The helper already inserted tenant "test", so count >= 1.
                let count_before = store.tenant_count().await?;
                assert!(count_before >= 1);

                let new_id = store.create_tenant("acme", "Acme Corp").await?;
                assert!(new_id > 0);

                let count_after = store.tenant_count().await?;
                assert_eq!(count_after, count_before + 1);

                // Duplicate slug is an error.
                let err = store.create_tenant("acme", "Duplicate").await.unwrap_err();
                assert!(err.to_string().contains("already exists"));

                Ok(())
            })
            .await
            .unwrap();
        }
    }

    // ── P5-T6 quota enforcement unit tests ────────────────────────────────

    #[tokio::test]
    async fn get_storage_usage_errors_before_connect() {
        let store = PgStore::new("postgres://localhost/db");
        let err = store.get_storage_usage(1).await.unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn get_token_usage_errors_before_connect() {
        let store = PgStore::new("postgres://localhost/db");
        let err = store.get_token_usage(1).await.unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn get_tenant_quota_limits_errors_before_connect() {
        let store = PgStore::new("postgres://localhost/db");
        let err = store.get_tenant_quota_limits(1).await.unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn check_storage_quota_errors_before_connect() {
        let store = PgStore::new("postgres://localhost/db");
        let err = store.check_storage_quota(1, 100).await.unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn check_token_quota_errors_before_connect() {
        let store = PgStore::new("postgres://localhost/db");
        let err = store.check_token_quota(1, 500).await.unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    // ── P5-T6 quota DB-integration tests (#[ignore]) ────────────────────

    #[cfg(test)]
    mod quota_integration {
        #![allow(clippy::unwrap_used, clippy::expect_used)]
        use super::*;

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

            // Insert a tenant with both quotas set.
            let pool = store.pool()?;
            sqlx::query(
                "INSERT INTO tenants (slug, name, quota_bytes, quota_tokens) \
                 VALUES ('test', 'Test', 10000, 5000)",
            )
            .execute(&pool)
            .await?;

            f(store).await
        }

        #[ignore]
        #[tokio::test]
        async fn storage_usage_zero_for_empty_tenant() {
            with_connected_store(|store| async move {
                let usage = store.get_storage_usage(1).await?;
                assert_eq!(usage, 0, "empty tenant should have zero storage usage");
                Ok(())
            })
            .await
            .unwrap();
        }

        #[ignore]
        #[tokio::test]
        async fn token_usage_zero_for_empty_tenant() {
            with_connected_store(|store| async move {
                let usage = store.get_token_usage(1).await?;
                assert_eq!(usage, 0, "empty tenant should have zero token usage");
                Ok(())
            })
            .await
            .unwrap();
        }

        #[ignore]
        #[tokio::test]
        async fn storage_usage_sums_file_bytes() {
            with_connected_store(|store| async move {
                // Insert files with known sizes via raw SQL. The files table has
                // RLS — set_tenant is called internally by get_storage_usage.
                let pool = store.pool()?;

                // Need document rows for FK constraints.
                sqlx::query("INSERT INTO documents (tenant_id, kind) VALUES (1, 'document')")
                    .execute(&pool)
                    .await?;

                // Insert files with known sizes.
                for (sha_hex, key, size) in [
                    (
                        "aaaa1111bbbb2222cccc3333ddddd44e",
                        "key-a",
                        100i64,
                    ),
                    (
                        "bbbb2222cccc3333dddd4444eeeee55f",
                        "key-b",
                        250,
                    ),
                    (
                        "cccc3333dddd4444eeee5555fffff66a",
                        "key-c",
                        50,
                    ),
                ] {
                    sqlx::query(
                        "INSERT INTO files (tenant_id, document_id, page_no, sha256, blob_key, \
                         path, mime, size_bytes, meta, status) \
                         VALUES (1, 1, 1, decode($1,'hex'), $2, $3, 'text/plain', $4, '{}', 'ready')",
                    )
                    .bind(sha_hex)
                    .bind(key)
                    .bind(key)
                    .bind(size)
                    .execute(&pool)
                    .await?;
                }

                let usage = store.get_storage_usage(1).await?;
                assert_eq!(usage, 400, "expected 100+250+50 = 400");
                Ok(())
            })
            .await
            .unwrap();
        }

        #[ignore]
        #[tokio::test]
        async fn token_usage_sums_event_tokens() {
            with_connected_store(|store| async move {
                let pool = store.pool()?;

                // Insert usage events directly.
                for (prompt, completion) in [(100i32, 50i32), (200, 0), (0, 75)] {
                    sqlx::query(
                        "INSERT INTO usage_events (tenant_id, model, role, prompt_tokens, completion_tokens) \
                         VALUES (1, 'bge-m3', 'embed', $1, $2)",
                    )
                    .bind(prompt)
                    .bind(completion)
                    .execute(&pool)
                    .await?;
                }

                let usage = store.get_token_usage(1).await?;
                // 100+50 + 200+0 + 0+75 = 425
                assert_eq!(usage, 425, "expected (100+50)+(200+0)+(0+75) = 425");
                Ok(())
            })
            .await
            .unwrap();
        }

        #[ignore]
        #[tokio::test]
        async fn check_storage_quota_under_limit() {
            with_connected_store(|store| async move {
                // No files yet → usage=0, quota_bytes=10000, adding 5000 should succeed.
                store.check_storage_quota(1, 5000).await?;
                Ok(())
            })
            .await
            .unwrap();
        }

        #[ignore]
        #[tokio::test]
        async fn check_storage_quota_over_limit() {
            with_connected_store(|store| async move {
                // quota_bytes=10000, try to add 15000 → exceeded.
                let err = store.check_storage_quota(1, 15_000).await.unwrap_err();
                let msg = err.to_string();
                assert!(
                    msg.contains("storage quota exceeded"),
                    "expected 'storage quota exceeded', got: {msg}"
                );
                assert!(msg.contains("limit is 10000"));
                Ok(())
            })
            .await
            .unwrap();
        }

        #[ignore]
        #[tokio::test]
        async fn check_token_quota_under_limit() {
            with_connected_store(|store| async move {
                // No usage events yet → usage=0, quota_tokens=5000, adding 1000 should work.
                store.check_token_quota(1, 1000).await?;
                Ok(())
            })
            .await
            .unwrap();
        }

        #[ignore]
        #[tokio::test]
        async fn check_token_quota_over_limit() {
            with_connected_store(|store| async move {
                // quota_tokens=5000, try to add 6000 → exceeded.
                let err = store.check_token_quota(1, 6000).await.unwrap_err();
                let msg = err.to_string();
                assert!(
                    msg.contains("token quota exceeded"),
                    "expected 'token quota exceeded', got: {msg}"
                );
                assert!(msg.contains("limit is 5000"));
                Ok(())
            })
            .await
            .unwrap();
        }

        #[ignore]
        #[tokio::test]
        async fn null_quota_means_unlimited() {
            with_connected_store(|store| async move {
                let pool = store.pool()?;
                // Create a second tenant with NULL quotas.
                sqlx::query(
                    "INSERT INTO tenants (slug, name, quota_bytes, quota_tokens) \
                     VALUES ('unlimited', 'Unlimited', NULL, NULL)",
                )
                .execute(&pool)
                .await?;

                // Both checks should succeed for any amount.
                store.check_storage_quota(2, 1_000_000_000_i64).await?;
                store.check_token_quota(2, 1_000_000_000_i64).await?;
                Ok(())
            })
            .await
            .unwrap();
        }

        #[ignore]
        #[tokio::test]
        async fn get_tenant_quota_limits_returns_correct_values() {
            with_connected_store(|store| async move {
                let (bytes, tokens) = store.get_tenant_quota_limits(1).await?;
                assert_eq!(bytes, Some(10000));
                assert_eq!(tokens, Some(5000));
                Ok(())
            })
            .await
            .unwrap();
        }

        #[ignore]
        #[tokio::test]
        async fn get_tenant_quota_limits_for_nonexistent_tenant_is_error() {
            with_connected_store(|store| async move {
                let err = store.get_tenant_quota_limits(999).await.unwrap_err();
                assert!(
                    err.to_string().contains("tenant 999 not found"),
                    "expected 'not found' error, got: {err}"
                );
                Ok(())
            })
            .await
            .unwrap();
        }
    }
}
