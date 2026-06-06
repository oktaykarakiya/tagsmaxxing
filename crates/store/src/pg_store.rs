//! `PgStore` — the [`Store`](kb_core::store::Store) implementation over Postgres + pgvector
//! (plan §5/§7/§13), connecting via sqlx.
//!
//! # Two connection roles (RLS enforcement, P6-T14)
//!
//! Tenant isolation rests on Postgres Row-Level Security (migration `0003_rls.sql`), but RLS
//! is bypassed by superusers and by `BYPASSRLS` roles. So `PgStore` keeps **two pools**:
//!
//! * the **admin pool** ([`admin_pool`](PgStore::admin_pool)) — the privileged owner/superuser
//!   role; runs migrations and every legitimately cross-tenant path (the job queue, admin and
//!   usage roll-ups, the `tenants` / `sessions` / `settings` tables that carry no `tenant_id`);
//! * the **app pool** ([`app_pool`](PgStore::app_pool)) — the non-privileged `kb_app` role
//!   (`NOSUPERUSER NOBYPASSRLS`, migration `0006_app_role.sql`); used for **all** tenant-scoped
//!   data. Every such operation runs inside a transaction opened by [`begin_tenant_tx`], which
//!   sets the `app.current_tenant` GUC transaction-locally so the GUC and the query share one
//!   connection and the setting is discarded on commit — a pooled connection can never leak one
//!   tenant's context to the next checkout. See docs/analysis/07-rls-enforcement-blocker.md.
//!
//! Both connection URLs are hot-swappable behind [`arc_swap::ArcSwap`] so an operator can
//! rotate credentials or relocate the database without restarting (the hot-swap rule,
//! CLAUDE.md / §20). When no distinct app URL is configured, the app pool falls back to the
//! admin URL (single-role mode — RLS then relies on the explicit `tenant_id` filters only).

use std::sync::Arc;

use anyhow::Context;
use arc_swap::ArcSwap;
use async_trait::async_trait;
use kb_core::chunk::Chunk;
use kb_core::document::Document;
use kb_core::envelope;
use kb_core::file::FileRecord;
use kb_core::job::Job;
use kb_core::kek::KeyEncryptionKey;
use kb_core::query::{Hit, Query};
use kb_core::store::Store;
use kb_core::tag::TagSource;
use kb_core::usage::UsageEvent;
use sqlx::Postgres;
use sqlx::Row;
use sqlx::postgres::{PgPool, PgPoolOptions};

use crate::MIGRATOR;

/// Advisory lock key used to serialize concurrent startup migrations (plan §22, P8-T8).
///
/// When multiple app replicas start simultaneously, the first acquires this lock
/// (via `pg_advisory_lock`), applies migrations, and releases; subsequent replicas
/// block until the lock is freed, then find all migrations already applied and proceed.
/// The value itself is arbitrary — it just needs to be unique within this application's
/// lock namespace so it never collides with another subsystem's advisory lock.
const MIGRATION_ADVISORY_LOCK: i64 = 7_272_769_801;

// ── PgStore ──────────────────────────────────────────────────────────────────

/// A Postgres-backed implementation of the core [`Store`] trait, connecting via sqlx
/// [`PgPool`]s. Both connection URLs are hot-swappable so an operator can rotate credentials
/// or relocate the database without restarting.
///
/// # Lifecycle
///
/// 1. Construct with [`PgStore::new`] (single role) or [`PgStore::with_roles`] (admin +
///    `kb_app`).
/// 2. Call [`PgStore::connect`] to establish both pools and auto-apply schema migrations.
/// 3. Use the [`Store`] trait methods (tenant-scoped data → `kb_app`) and the inherent admin
///    methods (`create_tenant`, the job queue's pool, …).
///
/// Call [`PgStore::set_url`] / [`PgStore::set_app_url`] then [`PgStore::connect`] again to
/// hot-swap to new credentials or a new Postgres instance.
pub struct PgStore {
    /// Hot-swappable **privileged** (admin/owner) connection URL — migrations, the job queue,
    /// admin/usage roll-ups, and the non-tenant tables. Read at [`connect`](PgStore::connect)
    /// time so rotation never needs a restart.
    admin_url: ArcSwap<String>,
    /// Hot-swappable **application** (`kb_app`) connection URL for tenant-scoped data. Empty
    /// means "single-role mode": the app pool falls back to [`admin_url`](PgStore::admin_url).
    app_url: ArcSwap<String>,
    /// The active privileged pool, replaced atomically on each connect.
    admin_pool: ArcSwap<Option<PgPool>>,
    /// The active `kb_app` pool. Shares the admin pool when no distinct app URL is configured.
    app_pool: ArcSwap<Option<PgPool>>,
    /// Hot-swappable Key Encryption Key for envelope encryption of DB columns
    /// (`chunks.content_enc`, `documents.summary_enc`, `documents.user_note_enc`).
    /// When `None`, column encryption is skipped (plaintext-only, backward-compatible).
    /// Set via [`set_kek`](PgStore::set_kek); resolved at call time per the hot-swap rule.
    kek: ArcSwap<Option<Arc<dyn KeyEncryptionKey>>>,
}

impl PgStore {
    /// Create a new single-role `PgStore` with the given connection URL, not yet connected.
    ///
    /// Both the admin and app pools use this URL — RLS is then enforced only to the extent the
    /// role itself enforces it (a superuser bypasses it). Use [`with_roles`](PgStore::with_roles)
    /// to connect tenant-scoped queries as the non-privileged `kb_app` role.
    ///
    /// Call [`connect`](PgStore::connect) before using the [`Store`] trait methods.
    #[must_use]
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            admin_url: ArcSwap::new(Arc::new(url.into())),
            app_url: ArcSwap::new(Arc::new(String::new())),
            admin_pool: ArcSwap::new(Arc::new(None)),
            app_pool: ArcSwap::new(Arc::new(None)),
            kek: ArcSwap::new(Arc::new(None)),
        }
    }

    /// Create a two-role `PgStore`: `admin_url` (privileged) for migrations / queue / admin
    /// paths, and `app_url` (the non-privileged `kb_app` role) for all tenant-scoped data so
    /// Row-Level Security is enforced (P6-T14, §13).
    ///
    /// Call [`connect`](PgStore::connect) before using the [`Store`] trait methods.
    #[must_use]
    pub fn with_roles(admin_url: impl Into<String>, app_url: impl Into<String>) -> Self {
        Self {
            admin_url: ArcSwap::new(Arc::new(admin_url.into())),
            app_url: ArcSwap::new(Arc::new(app_url.into())),
            admin_pool: ArcSwap::new(Arc::new(None)),
            app_pool: ArcSwap::new(Arc::new(None)),
            kek: ArcSwap::new(Arc::new(None)),
        }
    }

    /// Hot-swap the **privileged** database URL. The change takes effect on the **next** call
    /// to [`connect`](PgStore::connect) — active connections are not disrupted.
    pub fn set_url(&self, url: impl Into<String>) {
        self.admin_url.store(Arc::new(url.into()));
    }

    /// Hot-swap the **application** (`kb_app`) database URL. Takes effect on the next
    /// [`connect`](PgStore::connect). Set to empty to fall back to single-role mode.
    pub fn set_app_url(&self, url: impl Into<String>) {
        self.app_url.store(Arc::new(url.into()));
    }

    /// The current privileged (admin) URL as a plain `String`.
    ///
    /// This is read on every call so it reflects hot-swaps without a restart.
    /// Used by [`PgRoutingNotifier`](crate::PgRoutingNotifier) to resolve the
    /// database URL at listen time.
    #[must_use]
    pub fn admin_url_snapshot(&self) -> String {
        self.admin_url.load().as_ref().clone()
    }

    /// Read the current privileged URL snapshot.
    fn current_admin_url(&self) -> Arc<String> {
        self.admin_url.load_full()
    }

    /// Read the URL the app pool should use: the configured app URL, or the admin URL when no
    /// distinct app URL is set (single-role mode).
    fn resolved_app_url(&self) -> Arc<String> {
        let app = self.app_url.load_full();
        if app.is_empty() {
            self.current_admin_url()
        } else {
            app
        }
    }

    /// Establish (or re-establish) both connection pools using the current URLs, then
    /// auto-apply the forward-only schema migrations **as the privileged role**. Migrations
    /// already applied are skipped (the `_sqlx_migrations` ledger makes it idempotent).
    ///
    /// A **Postgres advisory lock** is acquired before migrations run so concurrent
    /// replicas serialize their startup: the first replica acquires the lock, applies
    /// migrations, and releases; stragglers wait, then see all migrations already applied
    /// and exit the lock immediately (plan §22, P8-T8). The lock is held on a dedicated
    /// connection that is dropped after migrations finish, so release is automatic even if
    /// the process crashes.
    ///
    /// Order matters: the admin pool connects and migrates first (creating the `kb_app` role
    /// and its grants), and only then does the app pool connect — so `kb_app` exists by the
    /// time it is used. When the app URL is unset or identical to the admin URL, the app pool
    /// reuses the admin pool (no redundant connections).
    ///
    /// If previous pools exist they are closed gracefully before the new ones replace them.
    ///
    /// # Errors
    /// Returns an error if either connection fails or migrations cannot be applied.
    pub async fn connect(&self) -> anyhow::Result<()> {
        let admin_url = self.current_admin_url();
        let admin = PgPoolOptions::new()
            .max_connections(10)
            .connect(admin_url.as_ref())
            .await
            .context("failed to connect to Postgres (admin role)")?;

        // Acquire an advisory lock to serialize migrations across concurrent replicas.
        // pg_advisory_lock blocks until the lock is available, then holds it on this
        // connection. The connection is dropped at the end of this scope, releasing the
        // lock automatically (plan §22, P8-T8).
        {
            let mut lock_conn = admin
                .acquire()
                .await
                .context("failed to acquire connection for advisory lock")?;
            sqlx::query("SELECT pg_advisory_lock($1)")
                .bind(MIGRATION_ADVISORY_LOCK)
                .execute(&mut *lock_conn)
                .await
                .context("failed to acquire migration advisory lock")?;

            // Run migrations while holding the lock — straggler replicas block above.
            MIGRATOR
                .run(&admin)
                .await
                .context("failed to apply schema migrations")?;

            // Release the lock explicitly for clarity; the connection drop would also release it.
            sqlx::query("SELECT pg_advisory_unlock($1)")
                .bind(MIGRATION_ADVISORY_LOCK)
                .execute(&mut *lock_conn)
                .await
                .context("failed to release migration advisory lock")?;

            // BUG-INGEST-02: The original UNIQUE (tenant_id, sha256) on files
            // repoints the file row to the newest document on conflict,
            // orphaning the first document. Expand the constraint to include
            // document_id so identical bytes under a different filename create
            // a new file row instead of reassigning the existing one.
            sqlx::query("ALTER TABLE files DROP CONSTRAINT IF EXISTS files_tenant_id_sha256_key")
                .execute(&mut *lock_conn)
                .await
                .context("failed to drop old files unique constraint")?;
            sqlx::query(
                "CREATE UNIQUE INDEX IF NOT EXISTS files_tenant_sha256_doc_unique \
                 ON files (tenant_id, sha256, document_id)",
            )
            .execute(&mut *lock_conn)
            .await
            .context("failed to create new files unique index")?;
        }

        // Connect the app pool only after migrations have created the kb_app role + grants.
        let app_url = self.resolved_app_url();
        let app = if app_url.as_str() == admin_url.as_str() {
            admin.clone()
        } else {
            PgPoolOptions::new()
                .max_connections(10)
                .connect(app_url.as_ref())
                .await
                .context("failed to connect to Postgres (app role)")?
        };

        let old_admin = self.admin_pool.swap(Arc::new(Some(admin)));
        let old_app = self.app_pool.swap(Arc::new(Some(app)));
        if let Some(old_pool) = old_app.as_ref().as_ref() {
            old_pool.close().await;
        }
        if let Some(old_pool) = old_admin.as_ref().as_ref() {
            old_pool.close().await;
        }

        Ok(())
    }

    /// Probe whether the database is reachable via the admin pool.
    ///
    /// Executes a trivial `SELECT 1` on the admin pool. Returns `false` if the pool
    /// has not been connected yet or the query fails.
    ///
    /// This is used by the readiness `/health` endpoint (plan §22, P8-T8).
    pub async fn is_db_reachable(&self) -> bool {
        let loaded = self.admin_pool.load();
        let Some(pool) = loaded.as_ref().as_ref() else {
            return false;
        };
        sqlx::query("SELECT 1").execute(pool).await.is_ok()
    }

    /// Check whether schema migrations have been applied (i.e. `connect()` ran).
    ///
    /// Probes the `_sqlx_migrations` table that sqlx creates. Returns `false` when
    /// the pool is not connected or the table does not exist.
    ///
    /// This is used by the readiness `/health` endpoint (plan §22, P8-T8).
    pub async fn are_migrations_applied(&self) -> bool {
        let loaded = self.admin_pool.load();
        let Some(pool) = loaded.as_ref().as_ref() else {
            return false;
        };
        sqlx::query("SELECT 1 FROM _sqlx_migrations LIMIT 1")
            .execute(pool)
            .await
            .is_ok()
    }

    /// Obtain a clone of the **privileged** connection pool (migrations / job queue / admin
    /// and usage roll-ups / non-tenant tables). This is the pool to hand to
    /// [`JobQueue`](https://docs.rs/kb-pipeline) and [`PgSessionStore`](crate::PgSessionStore).
    ///
    /// # Errors
    /// Returns an error if [`connect`](PgStore::connect) has not been called yet.
    pub fn admin_pool(&self) -> anyhow::Result<PgPool> {
        Self::loaded(&self.admin_pool)
    }

    /// Obtain a clone of the **application** (`kb_app`) pool used for tenant-scoped data.
    /// Falls back to the admin pool in single-role mode.
    ///
    /// # Errors
    /// Returns an error if [`connect`](PgStore::connect) has not been called yet.
    pub fn app_pool(&self) -> anyhow::Result<PgPool> {
        Self::loaded(&self.app_pool)
    }

    /// Obtain a clone of the privileged pool. Retained as the back-compatible name for callers
    /// (the job queue, session store, bootstrap, test seeding) that predate the two-role split;
    /// equivalent to [`admin_pool`](PgStore::admin_pool).
    ///
    /// # Errors
    /// Returns an error if [`connect`](PgStore::connect) has not been called yet.
    pub fn pool(&self) -> anyhow::Result<PgPool> {
        self.admin_pool()
    }

    /// Shared accessor: clone the pool out of an `ArcSwap<Option<PgPool>>` or error if unset.
    fn loaded(slot: &ArcSwap<Option<PgPool>>) -> anyhow::Result<PgPool> {
        slot.load_full().as_ref().as_ref().cloned().ok_or_else(|| {
            anyhow::anyhow!("PgStore is not connected — call connect() before using Store methods")
        })
    }

    // ── KEK (Key Encryption Key) management ─────────────────────────────────

    /// Hot-swap the Key Encryption Key used for DB column envelope encryption.
    ///
    /// Set to `None` to disable column encryption (plaintext-only mode). The change takes
    /// effect on the next call — no restart is needed (the hot-swap rule, CLAUDE.md / §28).
    /// When the KEK is swapped, any tenant DEKs that were wrapped with the previous KEK
    /// will fail to unwrap; the caller should re-wrap tenant DEKs after rotation.
    pub fn set_kek(&self, kek: Option<Arc<dyn KeyEncryptionKey>>) {
        self.kek.store(Arc::new(kek));
    }

    /// Snapshot the current KEK, or `None` when column encryption is disabled.
    pub(crate) fn kek(&self) -> Option<Arc<dyn KeyEncryptionKey>> {
        self.kek.load_full().as_ref().clone()
    }

    /// Look up or create the tenant's raw (unwrapped) DEK for column encryption.
    ///
    /// Queries `tenant_data_keys` for the tenant's serialized [`DataKey`], unwraps
    /// it with the current KEK, and returns the raw 32-byte DEK. If no key exists
    /// for the tenant, a fresh DEK is generated, wrapped with the KEK, and persisted
    /// in `tenant_data_keys`.
    ///
    /// Uses the **admin pool** because `tenant_data_keys` is infrastructure, not
    /// tenant data (similar to sessions and settings).
    ///
    /// # Errors
    /// Returns an error if no KEK is configured (column encryption disabled), the
    /// database is not connected, or any crypto operation fails.
    async fn get_or_create_tenant_dek(&self, tenant_id: i64) -> anyhow::Result<[u8; 32]> {
        let kek = self.kek().ok_or_else(|| {
            anyhow::anyhow!("KEK not configured — cannot encrypt/decrypt columns")
        })?;

        let admin = self.admin_pool()?;

        // Check if a DataKey already exists for this tenant.
        let existing_json: Option<serde_json::Value> =
            sqlx::query_scalar("SELECT data_key_json FROM tenant_data_keys WHERE tenant_id = $1")
                .bind(tenant_id)
                .fetch_optional(&admin)
                .await
                .context("failed to look up tenant data key")?;

        if let Some(json) = existing_json {
            let data_key: kb_core::DataKey = serde_json::from_value(json)
                .context("failed to deserialize tenant DataKey from DB")?;
            let key_id = format!("dek:{}", data_key.id);
            let raw_dek = match kek.unwrap(&data_key.wrapped_dek).await {
                Ok(raw_dek) => {
                    // Audit: successful DEK unwrap.
                    let _ = self
                        .insert_decrypt_audit_event(
                            tenant_id,
                            kb_core::audit::DecryptAuditAction::UnwrapDek,
                            &key_id,
                            None, // background operation, no user session
                            true,
                        )
                        .await;
                    raw_dek
                }
                Err(e) => {
                    // Audit: failed DEK unwrap.
                    let _ = self
                        .insert_decrypt_audit_event(
                            tenant_id,
                            kb_core::audit::DecryptAuditAction::UnwrapDek,
                            &key_id,
                            None,
                            false,
                        )
                        .await;
                    return Err(e).context("failed to unwrap tenant DEK with KEK");
                }
            };
            if raw_dek.len() != 32 {
                anyhow::bail!(
                    "unwrapped tenant DEK has wrong length: {} bytes (expected 32)",
                    raw_dek.len()
                );
            }
            let mut dek = [0u8; 32];
            dek.copy_from_slice(&raw_dek);
            return Ok(dek);
        }

        // No existing key — generate a fresh one.
        let data_key = kb_core::DataKey::generate(kek.as_ref(), "db-column-dek:v1".into())
            .await
            .context("failed to generate tenant DEK")?;
        let data_key_json =
            serde_json::to_value(&data_key).context("failed to serialize DataKey")?;

        sqlx::query("INSERT INTO tenant_data_keys (tenant_id, data_key_json) VALUES ($1, $2)")
            .bind(tenant_id)
            .bind(&data_key_json)
            .execute(&admin)
            .await
            .context("failed to persist tenant data key")?;

        // Re-unwrap the freshly generated DEK to get raw bytes.
        let key_id = format!("dek:{}", data_key.id);
        let raw_dek = match kek.unwrap(&data_key.wrapped_dek).await {
            Ok(raw_dek) => {
                // Audit: successful fresh-DEK unwrap.
                let _ = self
                    .insert_decrypt_audit_event(
                        tenant_id,
                        kb_core::audit::DecryptAuditAction::UnwrapDek,
                        &key_id,
                        None,
                        true,
                    )
                    .await;
                raw_dek
            }
            Err(e) => {
                // Audit: failed fresh-DEK unwrap.
                let _ = self
                    .insert_decrypt_audit_event(
                        tenant_id,
                        kb_core::audit::DecryptAuditAction::UnwrapDek,
                        &key_id,
                        None,
                        false,
                    )
                    .await;
                return Err(e).context("failed to unwrap freshly generated tenant DEK");
            }
        };
        if raw_dek.len() != 32 {
            anyhow::bail!(
                "freshly generated tenant DEK has wrong length: {} bytes (expected 32)",
                raw_dek.len()
            );
        }
        let mut dek = [0u8; 32];
        dek.copy_from_slice(&raw_dek);
        Ok(dek)
    }

    /// Decrypt `summary_enc` → `summary` and `user_note_enc` → `user_note` on a
    /// single [`DocumentRow`] in-place. Does nothing when no KEK is configured or
    /// the `_enc` columns are all `NULL`.
    ///
    /// Decryption failures are logged and the plaintext fallback is kept.
    async fn decrypt_document_row(
        &self,
        tenant_id: i64,
        row: &mut DocumentRow,
    ) -> anyhow::Result<()> {
        if self.kek().is_none() {
            return Ok(());
        }
        if row.summary_enc.is_none() && row.user_note_enc.is_none() {
            return Ok(());
        }
        let dek = self.get_or_create_tenant_dek(tenant_id).await?;
        if let Some(ref enc) = row.summary_enc {
            match envelope::decrypt_column(&dek, enc) {
                Ok(decrypted) => row.summary = Some(decrypted),
                Err(e) => {
                    tracing::warn!(
                        doc_id = row.id,
                        error = %e,
                        "failed to decrypt document summary_enc, falling back to plaintext"
                    );
                }
            }
        }
        if let Some(ref enc) = row.user_note_enc {
            match envelope::decrypt_column(&dek, enc) {
                Ok(decrypted) => row.user_note = Some(decrypted),
                Err(e) => {
                    tracing::warn!(
                        doc_id = row.id,
                        error = %e,
                        "failed to decrypt document user_note_enc, falling back to plaintext"
                    );
                }
            }
        }
        Ok(())
    }

    /// Decrypt a single chunk's `content_enc` column, falling back to `plaintext`
    /// when decryption is not possible or not configured.
    ///
    /// This is `pub(crate)` so [`run_hybrid_search`] can decrypt snippet text
    /// after reading chunk rows.
    pub(crate) async fn decrypt_chunk_content(
        &self,
        tenant_id: i64,
        content_enc: Option<&[u8]>,
        plaintext: &str,
    ) -> String {
        let Some(enc) = content_enc else {
            return plaintext.to_string();
        };
        let Some(_kek) = self.kek() else {
            return plaintext.to_string();
        };
        let dek = match self.get_or_create_tenant_dek(tenant_id).await {
            Ok(d) => d,
            Err(e) => {
                tracing::warn!(
                    tenant_id = tenant_id,
                    error = %e,
                    "failed to acquire tenant DEK for chunk decryption, using plaintext"
                );
                return plaintext.to_string();
            }
        };
        envelope::decrypt_column(&dek, enc).unwrap_or_else(|e| {
            tracing::warn!(
                tenant_id = tenant_id,
                error = %e,
                "failed to decrypt chunk content_enc, falling back to plaintext"
            );
            plaintext.to_string()
        })
    }

    /// Decrypt `_enc` columns on a slice of [`DocumentRow`]s in-place.
    /// Acquires the tenant DEK once and reuses it for all rows.
    async fn decrypt_document_rows(
        &self,
        tenant_id: i64,
        rows: &mut [DocumentRow],
    ) -> anyhow::Result<()> {
        if self.kek().is_none() {
            return Ok(());
        }
        let need_dek = rows
            .iter()
            .any(|r| r.summary_enc.is_some() || r.user_note_enc.is_some());
        if !need_dek {
            return Ok(());
        }
        let dek = self.get_or_create_tenant_dek(tenant_id).await?;
        for row in rows {
            if let Some(ref enc) = row.summary_enc {
                match envelope::decrypt_column(&dek, enc) {
                    Ok(decrypted) => row.summary = Some(decrypted),
                    Err(e) => {
                        tracing::warn!(
                            doc_id = row.id,
                            error = %e,
                            "failed to decrypt document summary_enc, falling back to plaintext"
                        );
                    }
                }
            }
            if let Some(ref enc) = row.user_note_enc {
                match envelope::decrypt_column(&dek, enc) {
                    Ok(decrypted) => row.user_note = Some(decrypted),
                    Err(e) => {
                        tracing::warn!(
                            doc_id = row.id,
                            error = %e,
                            "failed to decrypt document user_note_enc, falling back to plaintext"
                        );
                    }
                }
            }
        }
        Ok(())
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

/// Call counter for [`begin_tenant_tx`] — a lightweight spy used in test assertions to verify
/// every tenant-scoped PgStore method opens a tenant transaction.
#[cfg(test)]
static SET_TENANT_CALLS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// The parameterised SQL that sets the per-transaction tenant GUC. Calls
/// `set_config('app.current_tenant', $tenant, true)` via the `app_set_current_tenant($1)`
/// wrapper function (migration `0003_rls.sql`).
pub(crate) const SET_TENANT_SQL: &str = "SELECT app_set_current_tenant($1)";

/// Begin a transaction on `pool` with the `app.current_tenant` GUC set transaction-locally,
/// so **every** statement run on the returned transaction is scoped to `tenant_id` by
/// Row-Level Security (plan §13, P6-T14).
///
/// This is the linchpin of tenant isolation: the GUC and the subsequent queries share one
/// connection within one transaction (the previous design set the GUC on a *separate*
/// round-trip, so under a non-`BYPASSRLS` role it reverted before the query and denied every
/// row — see docs/analysis/07-rls-enforcement-blocker.md). Because the GUC is set with
/// `is_local => true`, it is discarded when the transaction commits or rolls back, so a pooled
/// connection can never leak one tenant's context to the next checkout (reset-on-release).
///
/// Callers run their queries on `&mut *tx` and `tx.commit().await` at the end.
///
/// # Errors
/// Returns an error if the transaction cannot begin or the GUC cannot be set.
pub(crate) async fn begin_tenant_tx(
    pool: &PgPool,
    tenant_id: i64,
) -> anyhow::Result<sqlx::Transaction<'static, Postgres>> {
    #[cfg(test)]
    {
        use std::sync::atomic::Ordering;
        SET_TENANT_CALLS.fetch_add(1, Ordering::Relaxed);
    }
    let mut tx = pool
        .begin()
        .await
        .context("failed to begin tenant transaction")?;
    sqlx::query(SET_TENANT_SQL)
        .bind(tenant_id)
        .execute(&mut *tx)
        .await
        .context("failed to set app.current_tenant for RLS")?;
    Ok(tx)
}

/// Derive a stable, order-independent 64-bit advisory-lock key from a tenant id + the set of
/// file content hashes in an ingest, so concurrent ingests of the *same upload* (same files in
/// any order) serialize on `pg_advisory_xact_lock` while unrelated uploads lock on different keys
/// and never contend. Key collisions only cause rare, harmless extra serialization — never
/// incorrectness.
fn ingest_advisory_key(tenant_id: i64, file_shas: &[&[u8]]) -> i64 {
    file_shas
        .iter()
        .fold(tenant_id.rotate_left(32), |acc, sha| {
            let mut head = [0u8; 8];
            let n = sha.len().min(8);
            head[..n].copy_from_slice(&sha[..n]);
            acc ^ i64::from_le_bytes(head)
        })
}

#[cfg(test)]
mod advisory_key_tests {
    use super::ingest_advisory_key;

    #[test]
    fn stable_order_independent_and_set_sensitive() {
        let a: &[u8] = &[0x11u8; 32];
        let b: &[u8] = &[0x22u8; 32];
        // Deterministic, and order-independent (the same file set in any order → same lock).
        assert_eq!(
            ingest_advisory_key(1, &[a, b]),
            ingest_advisory_key(1, &[b, a])
        );
        // A different file set → a different key (one file vs two).
        assert_ne!(
            ingest_advisory_key(1, &[a]),
            ingest_advisory_key(1, &[a, b])
        );
        // Different file (same tenant) → different key, so unrelated ingests don't serialize.
        assert_ne!(ingest_advisory_key(1, &[a]), ingest_advisory_key(1, &[b]));
        // Different tenant (same set) → different key.
        assert_ne!(ingest_advisory_key(1, &[a]), ingest_advisory_key(2, &[a]));
        // A short hash must not panic (defensive — real sha256 is always 32 bytes).
        let _ = ingest_advisory_key(1, &[&[0xABu8][..]]);
    }
}

/// Returns the number of times [`begin_tenant_tx`] has been called during tests.
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

const USAGE_INSERT_SQL: &str = "INSERT INTO usage_events (tenant_id,user_id,model,role,backend_id,prompt_tokens,completion_tokens,latency_ms,cost_micros) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING id";

/// Upsert the per-tenant monthly token rollup (P14-T1). `$2` is the event's
/// `created_at`; it is truncated to the first day of its UTC calendar month so
/// every event in a month shares one `(tenant_id, period_month)` row. The
/// `date_trunc(...) AT TIME ZONE 'UTC'` cast yields a `DATE`, matching the
/// `period_month DATE` column. Runs inside the same tenant transaction as the
/// `usage_events` insert, so the event and its rollup commit atomically.
const MONTHLY_ROLLUP_UPSERT_SQL: &str = "INSERT INTO tenant_monthly_usage \
     (tenant_id, period_month, tokens_used, updated_at) \
     VALUES ($1, (date_trunc('month', $2::timestamptz AT TIME ZONE 'UTC'))::date, $3, now()) \
     ON CONFLICT (tenant_id, period_month) DO UPDATE \
     SET tokens_used = tenant_monthly_usage.tokens_used + EXCLUDED.tokens_used, \
         updated_at = now()";

/// Point-select the current UTC month's token rollup for a tenant (P14-T1).
const MONTHLY_ROLLUP_CURRENT_SQL: &str = "SELECT tokens_used FROM tenant_monthly_usage \
     WHERE tenant_id = $1 \
       AND period_month = (date_trunc('month', now() AT TIME ZONE 'UTC'))::date";

/// Sum a tenant's token usage over the **current UTC calendar month** (P14-T4).
///
/// The reconciliation/backstop read behind
/// [`PgStore::get_token_usage`]. Windows on `created_at >= date_trunc('month',
/// now() AT TIME ZONE 'UTC')` so it covers exactly the month the monthly rollup
/// is bucketed by. `SUM(bigint)` is `NUMERIC` in Postgres, so the result is cast
/// back to `BIGINT` for sqlx to decode (a tenant's monthly tokens fit in i64).
const TOKEN_USAGE_MONTH_SQL: &str = "SELECT COALESCE(SUM(COALESCE(prompt_tokens,0) + COALESCE(completion_tokens,0)), 0)::BIGINT \
     FROM usage_events \
     WHERE tenant_id = $1 \
       AND created_at >= date_trunc('month', now() AT TIME ZONE 'UTC')";

// ── Store implementation ─────────────────────────────────────────────────────

#[async_trait]
impl Store for PgStore {
    /// Insert or update a file (page) record, returning its id.
    ///
    /// Idempotent on `(tenant_id, sha256, document_id)` conflict: existing rows are
    /// updated with the latest metadata, status, and document association.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    async fn upsert_file(&self, rec: &FileRecord) -> anyhow::Result<i64> {
        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, rec.tenant_id).await?;
        let id = sqlx::query_scalar::<Postgres, i64>(
            "INSERT INTO files (tenant_id,document_id,page_no,page_label,sha256,blob_key,path,mime,size_bytes,meta,status,ingested_at) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) \
             ON CONFLICT (tenant_id,sha256,document_id) DO UPDATE SET document_id=EXCLUDED.document_id,page_no=EXCLUDED.page_no,page_label=EXCLUDED.page_label,blob_key=EXCLUDED.blob_key,path=EXCLUDED.path,mime=EXCLUDED.mime,size_bytes=EXCLUDED.size_bytes,meta=EXCLUDED.meta,status=EXCLUDED.status,ingested_at=EXCLUDED.ingested_at \
             RETURNING id",
        )
        .bind(rec.tenant_id).bind(rec.document_id).bind(rec.page_no)
        .bind(&rec.page_label).bind(rec.sha256.as_bytes().as_slice())
        .bind(&rec.blob_key).bind(&rec.path).bind(&rec.mime)
        .bind(rec.size_bytes).bind(&rec.meta)
        .bind(rec.status.as_str()).bind(rec.ingested_at)
        .fetch_one(&mut *tx).await
        .context("failed to upsert file record")?;
        tx.commit().await.context("failed to commit upsert_file")?;
        Ok(id)
    }

    /// Atomically replace the chunks belonging to a file.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    async fn upsert_chunks(&self, file_id: i64, chunks: &[Chunk]) -> anyhow::Result<()> {
        // RLS: determine tenant_id from first chunk (all chunks share the same tenant).
        // When chunks is empty we look up the file's tenant via the privileged pool — under
        // the RLS-enforced kb_app role that lookup would itself need a tenant context we don't
        // yet have, so this disambiguation runs on the admin pool (the actual delete then runs
        // tenant-scoped). Normal callers pass the file's chunks, so this is a rare edge case.
        let tenant_id = if let Some(first) = chunks.first() {
            first.tenant_id
        } else {
            let admin = self.admin_pool()?;
            sqlx::query_scalar::<Postgres, i64>("SELECT tenant_id FROM files WHERE id = $1")
                .bind(file_id)
                .fetch_optional(&admin)
                .await
                .context("failed to look up file tenant for empty-chunks upsert")?
                .ok_or_else(|| {
                    anyhow::anyhow!("file {file_id} not found — cannot determine tenant for RLS")
                })?
        };

        // Acquire the tenant DEK if a KEK is configured.
        let dek = if self.kek().is_some() {
            Some(self.get_or_create_tenant_dek(tenant_id).await?)
        } else {
            None
        };

        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, tenant_id).await?;
        sqlx::query("DELETE FROM chunks WHERE file_id = $1")
            .bind(file_id)
            .execute(&mut *tx)
            .await
            .context("failed to delete old chunks")?;
        for chunk in chunks {
            let content_enc = dek
                .as_ref()
                .map(|d| envelope::encrypt_column(d, &chunk.content))
                .transpose()
                .context("failed to encrypt chunk content")?;
            let vec_str = format_vector(&chunk.embedding);
            sqlx::query("INSERT INTO chunks (tenant_id,document_id,file_id,page_no,idx,content,content_enc,ts_offset,embedding) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,CAST($9 AS vector))")
                .bind(chunk.tenant_id).bind(chunk.document_id).bind(file_id)
                .bind(chunk.page_no).bind(chunk.idx)
                .bind(&chunk.content).bind(&content_enc).bind(chunk.ts_offset).bind(&vec_str)
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
        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, tenant_id).await?;
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
        .fetch_one(&mut *tx)
        .await
        .context("failed to upsert tag")?;

        tx.commit().await.context("failed to commit upsert_tag")?;
        Ok(id)
    }

    /// Look up an exact alias match. Returns `Some(tag_id)` if the alias exists
    /// for the tenant, or `None` if no such alias is known.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn lookup_alias(&self, tenant_id: i64, alias: &str) -> anyhow::Result<Option<i64>> {
        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, tenant_id).await?;

        let tag_id: Option<i64> = sqlx::query_scalar(
            "SELECT tag_id FROM tag_aliases WHERE tenant_id = $1 AND alias = $2",
        )
        .bind(tenant_id)
        .bind(alias)
        .fetch_optional(&mut *tx)
        .await
        .context("failed to look up tag alias")?;

        tx.commit().await.context("failed to commit lookup_alias")?;
        Ok(tag_id)
    }

    /// Return every tag for a tenant that has a non-null embedding, together
    /// with its parsed vector. Used for cosine-matching during canonicalization.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn find_similar_tags(&self, tenant_id: i64) -> anyhow::Result<Vec<(i64, Vec<f32>)>> {
        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, tenant_id).await?;

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
        .fetch_all(&mut *tx)
        .await
        .context("failed to fetch similar tags")?;

        tx.commit()
            .await
            .context("failed to commit find_similar_tags")?;

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
        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, tenant_id).await?;

        sqlx::query(
            "INSERT INTO tag_aliases (tenant_id, alias, tag_id) \
             VALUES ($1, $2, $3) \
             ON CONFLICT (tenant_id, alias) DO NOTHING",
        )
        .bind(tenant_id)
        .bind(alias)
        .bind(tag_id)
        .execute(&mut *tx)
        .await
        .context("failed to insert tag alias")?;

        tx.commit()
            .await
            .context("failed to commit insert_tag_alias")?;
        Ok(())
    }

    /// Attach canonical tags to a document. Idempotent: duplicate
    /// `(document_id, tag_id)` pairs are silently skipped.
    ///
    /// `tenant_id` is required to set the `app.current_tenant` RLS GUC before
    /// querying the `document_tags` table (whose RLS policy gateways through
    /// the parent document's tenant).
    ///
    /// The `source` parameter determines the provenance (`llm` or `user`) and drives
    /// the `locked` flag: user-assigned tags are locked by default so they survive
    /// re-tag operations (plan §6.5, P6-T11). LLM-assigned tags are not locked.
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
        source: TagSource,
    ) -> anyhow::Result<()> {
        if tag_ids.is_empty() {
            return Ok(());
        }

        let locked = matches!(source, TagSource::User);
        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, tenant_id).await?;

        for &tag_id in tag_ids {
            sqlx::query(
                "INSERT INTO document_tags (document_id, tag_id, source, locked) \
                 VALUES ($1, $2, $3, $4) \
                 ON CONFLICT (document_id, tag_id) DO UPDATE SET source = EXCLUDED.source, locked = EXCLUDED.locked",
            )
            .bind(document_id)
            .bind(tag_id)
            .bind(source.as_str())
            .bind(locked)
            .execute(&mut *tx)
            .await
            .context("failed to insert document_tag row")?;
        }

        tx.commit()
            .await
            .context("failed to commit insert_document_tags")?;

        Ok(())
    }

    /// Remove all LLM-sourced, non-locked document-tag links for a document.
    ///
    /// This is the "clear" step before a re-tag: locked user-assigned tags are preserved,
    /// while stale LLM tags are deleted so fresh ones can be inserted.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn clear_llm_document_tags(
        &self,
        tenant_id: i64,
        document_id: i64,
    ) -> anyhow::Result<u64> {
        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, tenant_id).await?;

        let rows = sqlx::query(
            "DELETE FROM document_tags \
             WHERE document_id = $1 AND source = 'llm' AND locked = false",
        )
        .bind(document_id)
        .execute(&mut *tx)
        .await
        .context("failed to clear llm document tags")?
        .rows_affected();

        tx.commit()
            .await
            .context("failed to commit clear_llm_document_tags")?;

        Ok(rows)
    }

    /// Return the canonical tag ids that are locked for a document (user-assigned and
    /// explicitly confirmed). Used by re-tag to merge these into the new result set.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn get_locked_document_tags(
        &self,
        tenant_id: i64,
        document_id: i64,
    ) -> anyhow::Result<Vec<i64>> {
        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, tenant_id).await?;

        let ids: Vec<i64> = sqlx::query_scalar(
            "SELECT tag_id FROM document_tags \
             WHERE document_id = $1 AND locked = true",
        )
        .bind(document_id)
        .fetch_all(&mut *tx)
        .await
        .context("failed to fetch locked document tags")?;

        tx.commit()
            .await
            .context("failed to commit get_locked_document_tags")?;

        Ok(ids)
    }

    /// Set the `locked` flag on a specific document-tag link. When `locked` is true the
    /// tag survives re-tag; when false it may be cleared by a future re-tag.
    ///
    /// Returns the number of rows affected (0 or 1).
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn set_document_tag_locked(
        &self,
        tenant_id: i64,
        document_id: i64,
        tag_id: i64,
        locked: bool,
    ) -> anyhow::Result<u64> {
        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, tenant_id).await?;

        let rows = sqlx::query(
            "UPDATE document_tags SET locked = $1 \
             WHERE document_id = $2 AND tag_id = $3",
        )
        .bind(locked)
        .bind(document_id)
        .bind(tag_id)
        .execute(&mut *tx)
        .await
        .context("failed to set document_tag locked flag")?
        .rows_affected();

        tx.commit()
            .await
            .context("failed to commit set_document_tag_locked")?;

        Ok(rows)
    }

    /// Remove a single document-tag link. Used by the document detail page's
    /// "X to remove" tag chip action (P6-T7).
    ///
    /// The row is deleted regardless of its `source` or `locked` flags — a
    /// human removing a tag is intentional. Returns the number of rows deleted
    /// (0 if the link did not exist).
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn remove_document_tag(
        &self,
        tenant_id: i64,
        document_id: i64,
        tag_id: i64,
    ) -> anyhow::Result<u64> {
        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, tenant_id).await?;

        let rows = sqlx::query(
            "DELETE FROM document_tags \
             WHERE document_id = $1 AND tag_id = $2",
        )
        .bind(document_id)
        .bind(tag_id)
        .execute(&mut *tx)
        .await
        .context("failed to remove document tag")?
        .rows_affected();

        tx.commit()
            .await
            .context("failed to commit remove_document_tag")?;

        Ok(rows)
    }

    /// Update the `page_no` for a single file within a document.
    ///
    /// Used by the document detail page's drag-and-drop reorder feature
    /// (P6-T7). The caller is responsible for ensuring all pages in the
    /// document retain a consistent ordering after the update.
    ///
    /// Returns the number of rows updated (0 if the file was not found).
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn update_file_page_order(
        &self,
        tenant_id: i64,
        file_id: i64,
        page_no: i32,
    ) -> anyhow::Result<u64> {
        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, tenant_id).await?;

        let rows = sqlx::query("UPDATE files SET page_no = $1 WHERE id = $2")
            .bind(page_no)
            .bind(file_id)
            .execute(&mut *tx)
            .await
            .context("failed to update file page order")?
            .rows_affected();

        tx.commit()
            .await
            .context("failed to commit update_file_page_order")?;

        Ok(rows)
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
        // Encrypt summary and user_note if a KEK is configured.
        let (summary_enc, user_note_enc) = if self.kek().is_some() {
            let dek = self.get_or_create_tenant_dek(doc.tenant_id).await?;
            let se = doc
                .summary
                .as_deref()
                .map(|s| envelope::encrypt_column(&dek, s))
                .transpose()
                .context("failed to encrypt document summary")?;
            let ue = doc
                .user_note
                .as_deref()
                .map(|s| envelope::encrypt_column(&dek, s))
                .transpose()
                .context("failed to encrypt document user_note")?;
            (se, ue)
        } else {
            (None, None)
        };

        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, doc.tenant_id).await?;
        let st = doc.status.as_str();
        let id = if doc.id > 0 {
            sqlx::query_scalar::<Postgres, i64>(
                "UPDATE documents SET tenant_id=$1,title=$2,summary=$3,summary_enc=$4,user_note=$5,user_note_enc=$6,kind=$7,meta=$8,page_count=$9,status=$10 WHERE id=$11 RETURNING id",
            )
            .bind(doc.tenant_id)
            .bind(&doc.title)
            .bind(&doc.summary)
            .bind(&summary_enc)
            .bind(&doc.user_note)
            .bind(&user_note_enc)
            .bind(doc.kind.as_str())
            .bind(&doc.meta)
            .bind(doc.page_count)
            .bind(st)
            .bind(doc.id)
            .fetch_one(&mut *tx)
            .await
            .context("failed to update document")?
        } else {
            sqlx::query_scalar::<Postgres, i64>(
                "INSERT INTO documents (tenant_id,title,summary,summary_enc,user_note,user_note_enc,kind,meta,page_count,status) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING id",
            )
            .bind(doc.tenant_id)
            .bind(&doc.title)
            .bind(&doc.summary)
            .bind(&summary_enc)
            .bind(&doc.user_note)
            .bind(&user_note_enc)
            .bind(doc.kind.as_str())
            .bind(&doc.meta)
            .bind(doc.page_count)
            .bind(st)
            .fetch_one(&mut *tx)
            .await
            .context("failed to insert document")?
        };
        tx.commit()
            .await
            .context("failed to commit upsert_document")?;
        Ok(id)
    }

    /// Record a single model-call usage event, returning the new row's id.
    ///
    /// In the SAME tenant transaction, when the event reports any tokens, the
    /// per-tenant monthly rollup (`tenant_monthly_usage`) is upserted by the
    /// event's UTC calendar month (P14-T1), so the event and its aggregate
    /// commit atomically and enforcement can read the month in O(1).
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn insert_usage_event(&self, event: &UsageEvent) -> anyhow::Result<i64> {
        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, event.tenant_id).await?;
        let id = sqlx::query_scalar::<Postgres, i64>(USAGE_INSERT_SQL)
            .bind(event.tenant_id)
            .bind(event.user_id)
            .bind(&event.model)
            .bind(event.role.as_str())
            .bind(&event.backend_id)
            .bind(event.prompt_tokens)
            .bind(event.completion_tokens)
            .bind(event.latency_ms)
            .bind(event.cost_micros)
            .fetch_one(&mut *tx)
            .await
            .context("failed to insert usage event")?;

        // Roll the event's tokens into the tenant's monthly counter (P14-T1).
        // Only when there is something to add, so zero-token calls (e.g. a
        // local backend that reports no usage) never create empty rows.
        let tokens =
            event.prompt_tokens.unwrap_or(0) as i64 + event.completion_tokens.unwrap_or(0) as i64;
        if tokens > 0 {
            sqlx::query(MONTHLY_ROLLUP_UPSERT_SQL)
                .bind(event.tenant_id)
                .bind(event.created_at)
                .bind(tokens)
                .execute(&mut *tx)
                .await
                .context("failed to upsert monthly token rollup")?;
        }

        tx.commit()
            .await
            .context("failed to commit insert_usage_event")?;
        Ok(id)
    }

    /// Return the tenant's token total for the current UTC calendar month from
    /// the pre-aggregated rollup (P14-T1), or `0` when no usage has been
    /// recorded this month. This is the O(1) read backing per-month token quota
    /// enforcement (no `SUM` over `usage_events`).
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn get_current_month_token_rollup(&self, tenant_id: i64) -> anyhow::Result<i64> {
        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, tenant_id).await?;
        let tokens: Option<i64> = sqlx::query_scalar::<Postgres, i64>(MONTHLY_ROLLUP_CURRENT_SQL)
            .bind(tenant_id)
            .fetch_optional(&mut *tx)
            .await
            .context("failed to read current-month token rollup")?;
        tx.commit()
            .await
            .context("failed to commit get_current_month_token_rollup")?;
        Ok(tokens.unwrap_or(0))
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

        // Acquire the tenant DEK once for the whole transaction if a KEK is configured.
        let dek = if self.kek().is_some() {
            Some(self.get_or_create_tenant_dek(doc.tenant_id).await?)
        } else {
            None
        };

        // Encrypt document summary and user_note.
        let (summary_enc, user_note_enc) = if let Some(ref d) = dek {
            let se = doc
                .summary
                .as_deref()
                .map(|s| envelope::encrypt_column(d, s))
                .transpose()
                .context("failed to encrypt document summary")?;
            let ue = doc
                .user_note
                .as_deref()
                .map(|s| envelope::encrypt_column(d, s))
                .transpose()
                .context("failed to encrypt document user_note")?;
            (se, ue)
        } else {
            (None, None)
        };

        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, doc.tenant_id).await?;

        // 0. Serialize concurrent ingests of the SAME upload (same set of files) so the
        //    idempotency lookup below cannot race: two READ COMMITTED transactions would
        //    otherwise both miss the other's *uncommitted* file rows and each INSERT a document
        //    (the (tenant_id, sha256, document_id) unique key cannot prevent that — distinct doc
        //    ids). This transaction-scoped advisory lock, keyed on the whole file set, makes the
        //    second ingest wait for the first to commit, after which the lookup finds and reuses
        //    the document. Unrelated uploads lock on different keys and never contend; the lock
        //    auto-releases on commit/rollback.
        if doc.id == 0 && !files.is_empty() {
            let shas: Vec<&[u8]> = files
                .iter()
                .map(|f| f.sha256.as_bytes().as_slice())
                .collect();
            let lock_key = ingest_advisory_key(doc.tenant_id, &shas);
            sqlx::query("SELECT pg_advisory_xact_lock($1)")
                .bind(lock_key)
                .execute(&mut *tx)
                .await
                .context("failed to acquire same-upload ingest advisory lock")?;
        }

        // 1. Resolve the target document, then upsert it (forcing status='ready').
        //    - explicit id → update it (caller threaded a known document).
        //    - id == 0 → reuse the document whose file set is EXACTLY this upload's
        //      (sha256, path) pairs, so re-ingesting an identical upload — single- or multi-file
        //      — is idempotent (plan §749) instead of duplicating. A different set (different
        //      bytes, a renamed page, or an added/removed page) does not match and makes a new
        //      document; keying on path preserves BUG-INGEST-02 orphan-avoidance.
        //    - otherwise → insert a new document.
        //    The advisory lock above makes this lookup + upsert race-free under concurrent
        //    same-upload ingests, not merely atomic within a single transaction.
        let target_id: i64 = if doc.id > 0 {
            doc.id
        } else if !files.is_empty() {
            let sha_hexes: Vec<String> = files.iter().map(|f| f.sha256.to_hex()).collect();
            let paths: Vec<Option<String>> = files.iter().map(|f| f.path.clone()).collect();
            sqlx::query_scalar::<Postgres, i64>(
                "WITH incoming AS ( \
                     SELECT decode(t.sha_hex, 'hex') AS sha256, t.path \
                     FROM unnest($2::text[], $3::text[]) AS t(sha_hex, path) \
                 ) \
                 SELECT f.document_id FROM files f \
                 JOIN incoming i ON f.sha256 = i.sha256 AND f.path IS NOT DISTINCT FROM i.path \
                 WHERE f.tenant_id = $1 \
                 GROUP BY f.document_id \
                 HAVING COUNT(*) = $4 \
                    AND (SELECT COUNT(*) FROM files f2 WHERE f2.document_id = f.document_id) = $4 \
                 ORDER BY f.document_id LIMIT 1",
            )
            .bind(doc.tenant_id)
            .bind(&sha_hexes)
            .bind(&paths)
            .bind(files.len() as i64)
            .fetch_optional(&mut *tx)
            .await
            .context("failed to look up existing document for idempotent re-ingest")?
            .unwrap_or(0)
        } else {
            0
        };

        // Upsert the document, forcing status='ready'.
        let doc_id = if target_id > 0 {
            sqlx::query_scalar::<Postgres, i64>(
                "UPDATE documents SET tenant_id=$1,title=$2,summary=$3,summary_enc=$4,user_note=$5,user_note_enc=$6,kind=$7,meta=$8,page_count=$9,status='ready' WHERE id=$10 RETURNING id",
            )
            .bind(doc.tenant_id)
            .bind(&doc.title).bind(&doc.summary).bind(&summary_enc)
            .bind(&doc.user_note).bind(&user_note_enc)
            .bind(doc.kind.as_str()).bind(&doc.meta).bind(doc.page_count)
            .bind(target_id)
            .fetch_one(&mut *tx).await
            .context("failed to update document in transaction")?
        } else {
            sqlx::query_scalar::<Postgres, i64>(
                "INSERT INTO documents (tenant_id,title,summary,summary_enc,user_note,user_note_enc,kind,meta,page_count,status) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'ready') RETURNING id",
            )
            .bind(doc.tenant_id)
            .bind(&doc.title).bind(&doc.summary).bind(&summary_enc)
            .bind(&doc.user_note).bind(&user_note_enc)
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
                 ON CONFLICT (tenant_id,sha256,document_id) DO UPDATE SET document_id=EXCLUDED.document_id,page_no=EXCLUDED.page_no,page_label=EXCLUDED.page_label,blob_key=EXCLUDED.blob_key,path=EXCLUDED.path,mime=EXCLUDED.mime,size_bytes=EXCLUDED.size_bytes,meta=EXCLUDED.meta,status=EXCLUDED.status,ingested_at=EXCLUDED.ingested_at \
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

        // 3. Insert document tags (LLM-sourced, not locked — plan §6.5, P6-T11).
        for &tag_id in tag_ids {
            sqlx::query(
                "INSERT INTO document_tags (document_id, tag_id, source, locked) \
                 VALUES ($1, $2, 'llm', false) \
                 ON CONFLICT (document_id, tag_id) DO NOTHING",
            )
            .bind(doc_id)
            .bind(tag_id)
            .execute(&mut *tx)
            .await
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
                let content_enc = dek
                    .as_ref()
                    .map(|d| envelope::encrypt_column(d, &chunk.content))
                    .transpose()
                    .context("failed to encrypt chunk content")?;
                let vec_str = format_vector(&chunk.embedding);
                sqlx::query("INSERT INTO chunks (tenant_id,document_id,file_id,page_no,idx,content,content_enc,ts_offset,embedding) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,CAST($9 AS vector))")
                    .bind(chunk.tenant_id).bind(doc_id).bind(file_id)
                    .bind(chunk.page_no).bind(chunk.idx)
                    .bind(&chunk.content).bind(&content_enc)
                    .bind(chunk.ts_offset).bind(&vec_str)
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
    /// Before inserting, this method checks the tenant's plan `max_users` limit
    /// (P11-T5). The check is **best-effort** — concurrent user creations may
    /// briefly exceed the cap.
    ///
    /// # Errors
    /// Returns an error if the database is not connected, the plan `max_users`
    /// limit is exceeded, the email is already taken within the tenant, or the
    /// query fails.
    pub async fn create_user(
        &self,
        tenant_id: i64,
        email: &str,
        password_hash: &str,
        role: kb_core::user::UserRole,
    ) -> anyhow::Result<i64> {
        // Plan-driven max_users enforcement (P11-T5). Best-effort — concurrent
        // user creations may briefly exceed the cap.
        self.check_tenant_max_users(tenant_id, 1).await?;

        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, tenant_id).await?;
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
        .fetch_optional(&mut *tx)
        .await
        .context("failed to insert user row")?
        .ok_or_else(|| anyhow::anyhow!("user '{email}' already exists in tenant {tenant_id}"))?;
        tx.commit().await.context("failed to commit create_user")?;
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
        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, tenant_id).await?;

        #[derive(sqlx::FromRow)]
        struct UserRow {
            id: i64,
            tenant_id: i64,
            email: String,
            password_hash: String,
            role: String,
            email_verified: bool,
            created_at: chrono::DateTime<chrono::Utc>,
        }

        let row: Option<UserRow> = sqlx::query_as(
            "SELECT id, tenant_id, email, password_hash, role, email_verified, created_at \
             FROM users WHERE tenant_id = $1 AND email = $2",
        )
        .bind(tenant_id)
        .bind(email)
        .fetch_optional(&mut *tx)
        .await
        .context("failed to look up user by email")?;

        tx.commit()
            .await
            .context("failed to commit find_user_by_email")?;

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
                    email_verified: r.email_verified,
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

    /// Returns `true` iff `tenant_id` is the **platform operator's** tenant — the
    /// first-seeded (lowest-`id`) tenant created by [`bootstrap_seed`](crate#bootstrap).
    ///
    /// Inference infrastructure (`providers` / `models` / `routes`, migration
    /// `0011`) is **global**: the `providers` and `models` tables carry no
    /// `tenant_id` column and are shared by every tenant. A customer tenant's own
    /// `Owner`/`Admin` administers only their own workspace, **not** the platform's
    /// shared infrastructure (this system has no platform-admin *role*, cf.
    /// BUG-ISOL-01). So only the operator's tenant may mutate that infrastructure;
    /// any other tenant's admin must be denied, otherwise one tenant could
    /// toggle/edit/delete the providers every other tenant depends on
    /// (BUG-ISOL-02). Read-only viewing is unaffected.
    ///
    /// The operator's tenant is identified as `MIN(id)` over `tenants`: `BIGSERIAL`
    /// starts at 1 and bootstrap seeds into an empty table, so the operator's
    /// tenant is always the lowest id. Returns `false` when the table is empty.
    ///
    /// Uses the admin pool (`tenants` is outside RLS); the comparison itself is the
    /// scoping gate, so no `tenant_id` filter is needed.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn is_platform_admin_tenant(&self, tenant_id: i64) -> anyhow::Result<bool> {
        let pool = self.pool()?;
        let min_id: Option<i64> = sqlx::query_scalar("SELECT MIN(id) FROM tenants")
            .fetch_one(&pool)
            .await
            .context("failed to resolve platform-admin tenant id")?;
        Ok(min_id == Some(tenant_id))
    }

    // ── Email verification (P12-T2) ────────────────────────────────────────────

    /// Set the email verification token for a user.
    ///
    /// The token is a cryptographically random 64-char hex string generated by
    /// [`kb_core::auth::generate_email_token`]. It is embedded in the verification
    /// URL sent to the user's email.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn set_email_verify_token(
        &self,
        tenant_id: i64,
        user_id: i64,
        token: &str,
    ) -> anyhow::Result<()> {
        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, tenant_id).await?;
        sqlx::query("UPDATE users SET email_verify_token = $3 WHERE id = $1 AND tenant_id = $2")
            .bind(user_id)
            .bind(tenant_id)
            .bind(token)
            .execute(&mut *tx)
            .await
            .context("failed to set email verify token")?;
        tx.commit()
            .await
            .context("failed to commit set_email_verify_token")?;
        Ok(())
    }

    /// Verify a user's email address using the token from the verification link.
    ///
    /// Sets `email_verified = true` and clears `email_verify_token` for the user
    /// matching the given token and tenant. Returns `true` if a row was updated
    /// (token was valid), `false` if the token was not found (expired or invalid).
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn verify_user_email(&self, tenant_id: i64, token: &str) -> anyhow::Result<bool> {
        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, tenant_id).await?;
        let rows_affected = sqlx::query(
            "UPDATE users SET email_verified = true, email_verify_token = NULL \
             WHERE tenant_id = $1 AND email_verify_token = $2",
        )
        .bind(tenant_id)
        .bind(token)
        .execute(&mut *tx)
        .await
        .context("failed to verify user email")?
        .rows_affected();
        tx.commit()
            .await
            .context("failed to commit verify_user_email")?;
        Ok(rows_affected > 0)
    }

    /// Set a password reset token and its expiration time for a user.
    ///
    /// Finds the user by email within the tenant and updates their
    /// `password_reset_token` and `password_reset_expires` columns. Returns
    /// `true` if the user was found and updated, `false` if no user matched
    /// the email (prevents user enumeration — caller should still show a
    /// success message).
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn set_password_reset_token(
        &self,
        tenant_id: i64,
        email: &str,
        token: &str,
        expires: chrono::DateTime<chrono::Utc>,
    ) -> anyhow::Result<bool> {
        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, tenant_id).await?;
        let rows_affected = sqlx::query(
            "UPDATE users SET password_reset_token = $3, password_reset_expires = $4 \
             WHERE tenant_id = $1 AND email = $2",
        )
        .bind(tenant_id)
        .bind(email)
        .bind(token)
        .bind(expires)
        .execute(&mut *tx)
        .await
        .context("failed to set password reset token")?
        .rows_affected();
        tx.commit()
            .await
            .context("failed to commit set_password_reset_token")?;
        Ok(rows_affected > 0)
    }

    /// Find a user by their password reset token, returning the user if the token
    /// is valid and not expired.
    ///
    /// Returns `None` if no user matches the token or if the token has expired.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn find_user_by_reset_token(
        &self,
        tenant_id: i64,
        token: &str,
    ) -> anyhow::Result<Option<kb_core::user::User>> {
        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, tenant_id).await?;

        #[derive(sqlx::FromRow)]
        struct Row {
            id: i64,
            tenant_id: i64,
            email: String,
            password_hash: String,
            role: String,
            email_verified: bool,
            created_at: chrono::DateTime<chrono::Utc>,
        }

        let row: Option<Row> = sqlx::query_as(
            "SELECT id, tenant_id, email, password_hash, role, email_verified, created_at \
             FROM users \
             WHERE tenant_id = $1 AND password_reset_token = $2 \
             AND password_reset_expires > now()",
        )
        .bind(tenant_id)
        .bind(token)
        .fetch_optional(&mut *tx)
        .await
        .context("failed to find user by reset token")?;

        tx.commit()
            .await
            .context("failed to commit find_user_by_reset_token")?;

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
                    email_verified: r.email_verified,
                    created_at: r.created_at,
                }))
            }
            None => Ok(None),
        }
    }

    /// Reset a user's password: update `password_hash` and clear the reset token.
    ///
    /// Returns `true` if a row was updated, `false` if the user was not found.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn reset_password(
        &self,
        tenant_id: i64,
        user_id: i64,
        new_password_hash: &str,
    ) -> anyhow::Result<bool> {
        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, tenant_id).await?;
        let rows_affected = sqlx::query(
            "UPDATE users SET password_hash = $3, password_reset_token = NULL, \
             password_reset_expires = NULL \
             WHERE id = $1 AND tenant_id = $2",
        )
        .bind(user_id)
        .bind(tenant_id)
        .bind(new_password_hash)
        .execute(&mut *tx)
        .await
        .context("failed to reset password")?
        .rows_affected();
        tx.commit()
            .await
            .context("failed to commit reset_password")?;
        Ok(rows_affected > 0)
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
        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, tenant_id).await?;
        // `SUM(bigint)` is typed `NUMERIC` in Postgres (guards against i64 overflow on the
        // running total), which sqlx cannot decode into an `i64`. We cap a single tenant's
        // storage well inside i64, so cast the aggregate back to `BIGINT` for decoding.
        let total: i64 = sqlx::query_scalar(
            "SELECT COALESCE(SUM(size_bytes), 0)::BIGINT FROM files WHERE tenant_id = $1",
        )
        .bind(tenant_id)
        .fetch_one(&mut *tx)
        .await
        .context("failed to query storage usage")?;
        tx.commit()
            .await
            .context("failed to commit get_storage_usage")?;
        Ok(total)
    }

    /// Query a tenant's token usage for the **current UTC calendar month**
    /// (sum of `usage_events.prompt_tokens` + `usage_events.completion_tokens`
    /// over rows whose `created_at` falls in the current month).
    ///
    /// `NULL` token columns are treated as 0 via COALESCE. The window is
    /// `created_at >= date_trunc('month', now() AT TIME ZONE 'UTC')` — the first
    /// instant of the current UTC month — matching the `(tenant_id, period_month)`
    /// key the [`tenant_monthly_usage`](Self::get_current_month_token_rollup)
    /// rollup is bucketed by (P14-T4). This `SUM` is the **reconciliation /
    /// backstop** read; the hot enforcement path reads the O(1) rollup instead.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn get_token_usage(&self, tenant_id: i64) -> anyhow::Result<i64> {
        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, tenant_id).await?;
        // Window to the current UTC calendar month so this lines up with the
        // monthly rollup. `now() AT TIME ZONE 'UTC'` yields the UTC wall-clock
        // timestamp; `date_trunc('month', …)` floors it to the month start.
        let total: i64 = sqlx::query_scalar(TOKEN_USAGE_MONTH_SQL)
            .bind(tenant_id)
            .fetch_one(&mut *tx)
            .await
            .context("failed to query token usage")?;
        tx.commit()
            .await
            .context("failed to commit get_token_usage")?;
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

// ── Export listing methods (plan §13 P5-T7) ───────────────────────────────────

impl PgStore {
    /// List all documents for a tenant, ordered by `id`.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn list_documents(&self, tenant_id: i64) -> anyhow::Result<Vec<Document>> {
        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, tenant_id).await?;
        let mut rows: Vec<DocumentRow> = sqlx::query_as(
            "SELECT id, tenant_id, title, summary, summary_enc, user_note, user_note_enc, kind, meta, page_count, local_only, status, created_at \
             FROM documents WHERE tenant_id = $1 ORDER BY id",
        )
        .bind(tenant_id)
        .fetch_all(&mut *tx)
        .await
        .context("failed to list documents for export")?;
        tx.commit()
            .await
            .context("failed to commit list_documents")?;

        // Decrypt _enc columns if a KEK is configured and any row has them.
        self.decrypt_document_rows(tenant_id, &mut rows).await?;

        rows.into_iter().map(document_from_row).collect()
    }

    /// List all files for a tenant, ordered by `document_id`, `page_no`.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn list_files(&self, tenant_id: i64) -> anyhow::Result<Vec<FileRecord>> {
        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, tenant_id).await?;
        let rows: Vec<FileRow> = sqlx::query_as(
            "SELECT id, tenant_id, document_id, page_no, page_label, sha256, blob_key, path, \
             mime, size_bytes, meta, status, ingested_at \
             FROM files WHERE tenant_id = $1 ORDER BY document_id, page_no",
        )
        .bind(tenant_id)
        .fetch_all(&mut *tx)
        .await
        .context("failed to list files for export")?;
        tx.commit().await.context("failed to commit list_files")?;
        rows.into_iter().map(file_from_row).collect()
    }

    /// List all tags for a tenant, ordered by `id`.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn list_tags(&self, tenant_id: i64) -> anyhow::Result<Vec<kb_core::tag::Tag>> {
        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, tenant_id).await?;
        #[derive(sqlx::FromRow)]
        struct Row {
            id: i64,
            tenant_id: i64,
            name: String,
            embedding_text: Option<String>,
        }
        let rows: Vec<Row> = sqlx::query_as(
            "SELECT id, tenant_id, name, embedding::text AS embedding_text \
             FROM tags WHERE tenant_id = $1 ORDER BY id",
        )
        .bind(tenant_id)
        .fetch_all(&mut *tx)
        .await
        .context("failed to list tags for export")?;
        tx.commit().await.context("failed to commit list_tags")?;
        rows.into_iter()
            .map(|r| {
                let embedding = r
                    .embedding_text
                    .map(|t| parse_vector_text(&t))
                    .transpose()?;
                Ok(kb_core::tag::Tag {
                    id: r.id,
                    tenant_id: r.tenant_id,
                    name: r.name,
                    embedding,
                })
            })
            .collect()
    }

    /// List all tag aliases for a tenant as `(alias, tag_id)` pairs.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn list_tag_aliases(&self, tenant_id: i64) -> anyhow::Result<Vec<(String, i64)>> {
        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, tenant_id).await?;
        #[derive(sqlx::FromRow)]
        struct Row {
            alias: String,
            tag_id: i64,
        }
        let rows: Vec<Row> = sqlx::query_as(
            "SELECT alias, tag_id FROM tag_aliases WHERE tenant_id = $1 ORDER BY alias",
        )
        .bind(tenant_id)
        .fetch_all(&mut *tx)
        .await
        .context("failed to list tag aliases for export")?;
        tx.commit()
            .await
            .context("failed to commit list_tag_aliases")?;
        Ok(rows.into_iter().map(|r| (r.alias, r.tag_id)).collect())
    }

    /// List all document-tag links for a tenant as `(document_id, tag_id)` pairs.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn list_document_tags(&self, tenant_id: i64) -> anyhow::Result<Vec<(i64, i64)>> {
        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, tenant_id).await?;
        #[derive(sqlx::FromRow)]
        struct Row {
            document_id: i64,
            tag_id: i64,
        }
        let rows: Vec<Row> = sqlx::query_as(
            "SELECT dt.document_id, dt.tag_id \
             FROM document_tags dt \
             JOIN documents d ON dt.document_id = d.id \
             WHERE d.tenant_id = $1 \
             ORDER BY dt.document_id, dt.tag_id",
        )
        .bind(tenant_id)
        .fetch_all(&mut *tx)
        .await
        .context("failed to list document tags for export")?;
        tx.commit()
            .await
            .context("failed to commit list_document_tags")?;
        Ok(rows
            .into_iter()
            .map(|r| (r.document_id, r.tag_id))
            .collect())
    }

    /// List all chunks for a tenant, ordered by `document_id`, `file_id`, `idx`.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn list_chunks(&self, tenant_id: i64) -> anyhow::Result<Vec<kb_core::chunk::Chunk>> {
        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, tenant_id).await?;
        #[derive(sqlx::FromRow)]
        struct Row {
            id: i64,
            tenant_id: i64,
            document_id: i64,
            file_id: i64,
            page_no: Option<i32>,
            idx: i32,
            content: String,
            content_enc: Option<Vec<u8>>,
            ts_offset: Option<f64>,
            embedding_text: String,
        }
        let rows: Vec<Row> = sqlx::query_as(
            "SELECT id, tenant_id, document_id, file_id, page_no, idx, content, content_enc, \
             ts_offset, embedding::text AS embedding_text \
             FROM chunks WHERE tenant_id = $1 \
             ORDER BY document_id, file_id, idx",
        )
        .bind(tenant_id)
        .fetch_all(&mut *tx)
        .await
        .context("failed to list chunks for export")?;
        tx.commit().await.context("failed to commit list_chunks")?;

        // Acquire the DEK only if any row has an encrypted content column.
        let dek: Option<[u8; 32]> = if rows.iter().any(|r| r.content_enc.is_some()) {
            if self.kek().is_some() {
                Some(self.get_or_create_tenant_dek(tenant_id).await?)
            } else {
                None
            }
        } else {
            None
        };

        rows.into_iter()
            .map(|r| {
                let content = if let (Some(d), Some(enc)) = (dek.as_ref(), &r.content_enc) {
                    envelope::decrypt_column(d, enc).unwrap_or_else(|e| {
                        tracing::warn!(
                            chunk_id = r.id,
                            error = %e,
                            "failed to decrypt chunk content_enc, falling back to plaintext"
                        );
                        r.content.clone()
                    })
                } else {
                    r.content
                };
                let embedding = parse_vector_text(&r.embedding_text)
                    .context("failed to parse chunk embedding")?;
                Ok(kb_core::chunk::Chunk {
                    id: r.id,
                    tenant_id: r.tenant_id,
                    document_id: r.document_id,
                    file_id: r.file_id,
                    page_no: r.page_no,
                    idx: r.idx,
                    content,
                    ts_offset: r.ts_offset,
                    embedding,
                })
            })
            .collect()
    }

    /// Get a tenant's slug by id.
    ///
    /// This method does **not** call [`set_tenant`] — the `tenants` table is outside RLS.
    ///
    /// # Errors
    /// Returns an error if the database is not connected, the tenant does not exist,
    /// or the query fails.
    pub async fn get_tenant_slug(&self, tenant_id: i64) -> anyhow::Result<String> {
        let pool = self.pool()?;
        let slug: Option<String> = sqlx::query_scalar("SELECT slug FROM tenants WHERE id = $1")
            .bind(tenant_id)
            .fetch_optional(&pool)
            .await
            .context("failed to look up tenant slug")?;
        slug.ok_or_else(|| anyhow::anyhow!("tenant {tenant_id} not found"))
    }
}

// ── API query methods (document detail, file download, job status) ────────────

impl PgStore {
    /// Fetch a single document by id, scoped to `tenant_id`.
    ///
    /// Returns `None` if no document with that id exists for the tenant
    /// (including cross-tenant access attempts — RLS enforces isolation).
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn get_document(
        &self,
        tenant_id: i64,
        doc_id: i64,
    ) -> anyhow::Result<Option<Document>> {
        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, tenant_id).await?;
        let mut row: Option<DocumentRow> = sqlx::query_as(
            "SELECT id, tenant_id, title, summary, summary_enc, user_note, user_note_enc, kind, meta, page_count, local_only, status, created_at \
             FROM documents WHERE id = $1",
        )
        .bind(doc_id)
        .fetch_optional(&mut *tx)
        .await
        .context("failed to fetch document")?;
        tx.commit().await.context("failed to commit get_document")?;

        // Decrypt _enc columns if available.
        if let Some(ref mut r) = row {
            self.decrypt_document_row(tenant_id, r).await?;
        }

        row.map(document_from_row).transpose()
    }

    /// Fetch all files belonging to a document, ordered by `page_no`.
    ///
    /// Returns an empty `Vec` if the document has no files or does not exist
    /// for the tenant (RLS enforces isolation).
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn get_files_for_document(
        &self,
        tenant_id: i64,
        doc_id: i64,
    ) -> anyhow::Result<Vec<FileRecord>> {
        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, tenant_id).await?;
        let rows: Vec<FileRow> = sqlx::query_as(
            "SELECT id, tenant_id, document_id, page_no, page_label, sha256, blob_key, path, \
             mime, size_bytes, meta, status, ingested_at \
             FROM files WHERE document_id = $1 ORDER BY page_no",
        )
        .bind(doc_id)
        .fetch_all(&mut *tx)
        .await
        .context("failed to fetch files for document")?;
        tx.commit()
            .await
            .context("failed to commit get_files_for_document")?;
        rows.into_iter().map(file_from_row).collect()
    }

    /// Fetch all canonical tags attached to a document via `document_tags`.
    ///
    /// Returns an empty `Vec` if the document has no tags or does not exist
    /// for the tenant (RLS enforces isolation).
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn get_tags_for_document(
        &self,
        tenant_id: i64,
        doc_id: i64,
    ) -> anyhow::Result<Vec<kb_core::tag::Tag>> {
        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, tenant_id).await?;
        #[derive(sqlx::FromRow)]
        struct TagRow {
            id: i64,
            tenant_id: i64,
            name: String,
            embedding_text: Option<String>,
        }
        let rows: Vec<TagRow> = sqlx::query_as(
            "SELECT t.id, t.tenant_id, t.name, t.embedding::text AS embedding_text \
             FROM tags t \
             JOIN document_tags dt ON t.id = dt.tag_id \
             WHERE dt.document_id = $1 \
             ORDER BY t.name",
        )
        .bind(doc_id)
        .fetch_all(&mut *tx)
        .await
        .context("failed to fetch tags for document")?;
        tx.commit()
            .await
            .context("failed to commit get_tags_for_document")?;
        rows.into_iter()
            .map(|r| {
                let embedding = r
                    .embedding_text
                    .map(|t| parse_vector_text(&t))
                    .transpose()?;
                Ok(kb_core::tag::Tag {
                    id: r.id,
                    tenant_id: r.tenant_id,
                    name: r.name,
                    embedding,
                })
            })
            .collect()
    }

    /// Look up a tag by canonical name within a tenant.
    ///
    /// Returns `None` when no tag with that name exists for the tenant.
    /// Used by the document detail page's add-tag flow (P6-T7).
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn find_tag_by_name(
        &self,
        tenant_id: i64,
        name: &str,
    ) -> anyhow::Result<Option<i64>> {
        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, tenant_id).await?;
        let id: Option<i64> =
            sqlx::query_scalar("SELECT id FROM tags WHERE tenant_id = $1 AND name = $2")
                .bind(tenant_id)
                .bind(name)
                .fetch_optional(&mut *tx)
                .await
                .context("failed to look up tag by name")?;
        tx.commit()
            .await
            .context("failed to commit find_tag_by_name")?;
        Ok(id)
    }

    /// Fetch a single file by id, scoped to `tenant_id`.
    ///
    /// Returns `None` if no file with that id exists for the tenant
    /// (including cross-tenant access attempts — RLS enforces isolation).
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn get_file(
        &self,
        tenant_id: i64,
        file_id: i64,
    ) -> anyhow::Result<Option<FileRecord>> {
        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, tenant_id).await?;
        let row: Option<FileRow> = sqlx::query_as(
            "SELECT id, tenant_id, document_id, page_no, page_label, sha256, blob_key, path, \
             mime, size_bytes, meta, status, ingested_at \
             FROM files WHERE id = $1",
        )
        .bind(file_id)
        .fetch_optional(&mut *tx)
        .await
        .context("failed to fetch file")?;
        tx.commit().await.context("failed to commit get_file")?;
        row.map(file_from_row).transpose()
    }

    /// Fetch a single job by id.
    ///
    /// The `jobs` table is not covered by RLS (job queue uses the privileged
    /// admin pool), so this method queries via the admin pool and filters by
    /// `tenant_id` explicitly to prevent cross-tenant job access.
    ///
    /// Returns `None` if no job with that id exists, or if the job belongs to
    /// a different tenant.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn get_job(&self, tenant_id: i64, job_id: i64) -> anyhow::Result<Option<Job>> {
        use kb_core::job::{Job, JobKind, JobStatus};
        use std::str::FromStr;
        let pool = self.pool()?;
        let row: Option<sqlx::postgres::PgRow> = sqlx::query(
            "SELECT id, tenant_id, file_id, document_id, kind, priority, status, attempts, \
             last_error, run_after, created_at, created_by \
             FROM jobs WHERE id = $1 AND tenant_id = $2",
        )
        .bind(job_id)
        .bind(tenant_id)
        .fetch_optional(&pool)
        .await
        .context("failed to fetch job")?;
        match row {
            Some(r) => {
                let kind_str: String = r.try_get("kind")?;
                let status_str: String = r.try_get("status")?;
                Ok(Some(Job {
                    id: r.try_get("id")?,
                    tenant_id: r.try_get("tenant_id")?,
                    file_id: r.try_get("file_id")?,
                    document_id: r.try_get("document_id")?,
                    kind: JobKind::from_str(&kind_str)
                        .with_context(|| format!("invalid job kind: '{kind_str}'"))?,
                    priority: r.try_get("priority")?,
                    status: JobStatus::from_str(&status_str)
                        .with_context(|| format!("invalid job status: '{status_str}'"))?,
                    attempts: r.try_get("attempts")?,
                    last_error: r.try_get("last_error")?,
                    run_after: r.try_get("run_after")?,
                    created_at: r.try_get("created_at")?,
                    created_by: r.try_get("created_by")?,
                }))
            }
            None => Ok(None),
        }
    }
}

// ── sqlx row type aliases + conversion helpers ────────────────────────────────

/// A raw row from the `documents` table, used by [`list_documents`](PgStore::list_documents).
#[derive(sqlx::FromRow)]
struct DocumentRow {
    id: i64,
    tenant_id: i64,
    title: Option<String>,
    summary: Option<String>,
    summary_enc: Option<Vec<u8>>,
    user_note: Option<String>,
    user_note_enc: Option<Vec<u8>>,
    kind: String,
    meta: serde_json::Value,
    page_count: i32,
    #[allow(dead_code)]
    local_only: bool,
    status: String,
    created_at: chrono::DateTime<chrono::Utc>,
}

/// Convert a [`DocumentRow`] into a [`Document`].
fn document_from_row(r: DocumentRow) -> anyhow::Result<Document> {
    use std::str::FromStr;
    let kind = kb_core::kind::DocKind::from_str(&r.kind)
        .with_context(|| format!("invalid DocKind in documents table: {}", r.kind))?;
    let status = kb_core::status::ProcessingStatus::from_str(&r.status)
        .with_context(|| format!("invalid ProcessingStatus in documents table: {}", r.status))?;
    Ok(Document {
        id: r.id,
        tenant_id: r.tenant_id,
        title: r.title,
        summary: r.summary,
        user_note: r.user_note,
        kind,
        meta: r.meta,
        page_count: r.page_count,
        status,
        created_at: r.created_at,
        local_only: r.local_only,
    })
}

/// A raw row from the `files` table, used by [`list_files`](PgStore::list_files).
#[derive(sqlx::FromRow)]
struct FileRow {
    id: i64,
    tenant_id: i64,
    document_id: i64,
    page_no: i32,
    page_label: Option<String>,
    sha256: Vec<u8>,
    blob_key: String,
    path: Option<String>,
    mime: Option<String>,
    size_bytes: Option<i64>,
    meta: serde_json::Value,
    status: String,
    ingested_at: chrono::DateTime<chrono::Utc>,
}

/// Convert a [`FileRow`] into a [`FileRecord`].
fn file_from_row(r: FileRow) -> anyhow::Result<FileRecord> {
    use std::str::FromStr;
    let mut hash = [0u8; 32];
    if r.sha256.len() != 32 {
        anyhow::bail!(
            "invalid sha256 length in files table: {} bytes",
            r.sha256.len()
        );
    }
    hash.copy_from_slice(&r.sha256);
    let status = kb_core::status::ProcessingStatus::from_str(&r.status)
        .with_context(|| format!("invalid ProcessingStatus in files table: {}", r.status))?;
    Ok(FileRecord {
        id: r.id,
        tenant_id: r.tenant_id,
        document_id: r.document_id,
        page_no: r.page_no,
        page_label: r.page_label,
        sha256: kb_core::hash::Sha256::from_bytes(hash),
        blob_key: r.blob_key,
        path: r.path,
        mime: r.mime,
        size_bytes: r.size_bytes,
        meta: r.meta,
        status,
        ingested_at: r.ingested_at,
    })
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

    // ── begin_tenant_tx ─────────────────────────────────────────────────

    /// The tenant-GUC SQL ([`SET_TENANT_SQL`]) must be a valid parameterised statement.
    /// This test guards against accidental edits or typos.
    #[test]
    fn set_tenant_sql_is_well_formed() {
        assert!(SET_TENANT_SQL.starts_with("SELECT app_set_current_tenant"));
        assert!(SET_TENANT_SQL.contains("$1"));
        assert!(!SET_TENANT_SQL.contains("PERFORM")); // PERFORM is plpgsql; we use SELECT
    }

    /// Verify that `set_tenant_call_count` is readable and `reset_set_tenant_calls`
    /// zeroes it — the spy infrastructure works before we use it in other tests.
    #[test]
    fn set_tenant_spy_counter_works() {
        reset_set_tenant_calls();
        assert_eq!(set_tenant_call_count(), 0);
        // We can't call the real async function without a connected pool, so
        // we just verify the spy primitives are reachable.
        reset_set_tenant_calls();
        assert_eq!(set_tenant_call_count(), 0);
    }

    // ── PgStore construction / hot-swap ─────────────────────────────────

    #[test]
    fn new_stores_url() {
        let store = PgStore::new("postgres://localhost/kb");
        assert_eq!(*store.current_admin_url(), "postgres://localhost/kb");
    }

    #[test]
    fn set_url_hot_swaps() {
        let store = PgStore::new("postgres://old/db");
        assert_eq!(*store.current_admin_url(), "postgres://old/db");
        store.set_url("postgres://new/db");
        assert_eq!(*store.current_admin_url(), "postgres://new/db");
    }

    #[test]
    fn set_url_is_independent_of_previous_url() {
        let store = PgStore::new("postgres://a/db");
        store.set_url("postgres://b/db");
        store.set_url("postgres://c/db");
        assert_eq!(*store.current_admin_url(), "postgres://c/db");
    }

    /// In single-role mode (`new`), the app URL is empty so it resolves to the admin URL.
    #[test]
    fn new_app_url_falls_back_to_admin() {
        let store = PgStore::new("postgres://localhost/kb");
        assert_eq!(*store.resolved_app_url(), "postgres://localhost/kb");
        // Hot-swapping the admin URL also moves the fallback app URL.
        store.set_url("postgres://relocated/kb");
        assert_eq!(*store.resolved_app_url(), "postgres://relocated/kb");
    }

    /// `with_roles` keeps the two URLs distinct, and `set_app_url` hot-swaps only the app URL.
    #[test]
    fn with_roles_keeps_urls_distinct_and_hot_swappable() {
        let store = PgStore::with_roles("postgres://kb_admin@h/kb", "postgres://kb_app@h/kb");
        assert_eq!(*store.current_admin_url(), "postgres://kb_admin@h/kb");
        assert_eq!(*store.resolved_app_url(), "postgres://kb_app@h/kb");

        store.set_app_url("postgres://kb_app_rotated@h/kb");
        assert_eq!(*store.resolved_app_url(), "postgres://kb_app_rotated@h/kb");
        // Admin URL is unaffected by an app-URL rotation.
        assert_eq!(*store.current_admin_url(), "postgres://kb_admin@h/kb");

        // Clearing the app URL reverts to single-role fallback.
        store.set_app_url("");
        assert_eq!(*store.resolved_app_url(), "postgres://kb_admin@h/kb");
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

    #[test]
    fn app_and_admin_pools_error_before_connect() {
        let store = PgStore::with_roles("postgres://a/db", "postgres://b/db");
        assert!(
            store
                .admin_pool()
                .unwrap_err()
                .to_string()
                .contains("not connected")
        );
        assert!(
            store
                .app_pool()
                .unwrap_err()
                .to_string()
                .contains("not connected")
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
            .hybrid_search(1, &q, &[0.1_f32; crate::EMBED_DIM])
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
        let hits = store
            .hybrid_search(1, &q, &[0.1_f32; crate::EMBED_DIM])
            .await
            .unwrap();
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
            .insert_document_tags(1, 1, &[10, 20], TagSource::Llm)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn insert_document_tags_empty_is_noop() {
        // Empty tag list is a no-op even without a connection (early return).
        let store = PgStore::new("postgres://localhost/db");
        store
            .insert_document_tags(1, 1, &[], TagSource::Llm)
            .await
            .unwrap();
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
            // Shared per-binary container + fresh database; tenant data runs as the
            // non-privileged kb_app role so RLS is enforced (P6-T14).
            let db = kb_testsupport::fresh_db().await?;
            let store = PgStore::with_roles(&db.admin_url, &db.app_url);
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
                let id1 = store
                    .upsert_tag(1, "invoice", &emb(&[0.1, 0.2, 0.3]))
                    .await?;
                assert!(id1 > 0, "expected positive tag id");

                // Same (tenant, name) → same id.
                let id2 = store
                    .upsert_tag(1, "invoice", &emb(&[0.999, 0.888, 0.777]))
                    .await?;
                assert_eq!(id2, id1, "idempotent upsert must return same id");

                // Different name → different id.
                let id3 = store
                    .upsert_tag(1, "receipt", &emb(&[0.1, 0.2, 0.3]))
                    .await?;
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
                let tag_id = store
                    .upsert_tag(1, "invoice", &emb(&[0.1, 0.2, 0.3]))
                    .await?;
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
                store
                    .upsert_tag(1, "invoice", &emb(&[1.0, 0.0, 0.0]))
                    .await?;
                store
                    .upsert_tag(1, "receipt", &emb(&[0.0, 1.0, 0.0]))
                    .await?;

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

                let tag1 = store.upsert_tag(1, "alpha", &emb(&[0.1, 0.2])).await?;
                let tag2 = store.upsert_tag(1, "beta", &emb(&[0.3, 0.4])).await?;

                store
                    .insert_document_tags(1, doc_id, &[tag1, tag2], TagSource::Llm)
                    .await?;

                // Verify they're in the table.
                let count: i64 =
                    sqlx::query_scalar("SELECT COUNT(*) FROM document_tags WHERE document_id = $1")
                        .bind(doc_id)
                        .fetch_one(&pool)
                        .await?;
                assert_eq!(count, 2);

                // Idempotent re-insert does not duplicate.
                store
                    .insert_document_tags(1, doc_id, &[tag1, tag2], TagSource::Llm)
                    .await?;
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
        async fn document_tag_source_and_locked_columns_are_populated() {
            with_connected_store(|store| async move {
                let pool = store.pool()?;
                let doc_id: i64 = sqlx::query_scalar(
                    "INSERT INTO documents (tenant_id, kind) \
                     VALUES (1, 'document') RETURNING id",
                )
                .fetch_one(&pool)
                .await?;

                let tag_id = store
                    .upsert_tag(1, "confetti", &emb(&[0.7, 0.8]))
                    .await?;

                // LLM tag: source='llm', locked=false.
                store
                    .insert_document_tags(1, doc_id, &[tag_id], TagSource::Llm)
                    .await?;
                let (src, lck): (String, bool) = sqlx::query_as(
                    "SELECT source, locked FROM document_tags WHERE document_id = $1 AND tag_id = $2",
                )
                .bind(doc_id)
                .bind(tag_id)
                .fetch_one(&pool)
                .await?;
                assert_eq!(src, "llm");
                assert!(!lck, "LLM tags must not be locked");

                // Re-insert with User source: ON CONFLICT DO UPDATE flips both columns.
                store
                    .insert_document_tags(1, doc_id, &[tag_id], TagSource::User)
                    .await?;
                let (src2, lck2): (String, bool) = sqlx::query_as(
                    "SELECT source, locked FROM document_tags WHERE document_id = $1 AND tag_id = $2",
                )
                .bind(doc_id)
                .bind(tag_id)
                .fetch_one(&pool)
                .await?;
                assert_eq!(src2, "user");
                assert!(lck2, "User tags must be locked");

                Ok(())
            })
            .await
            .unwrap();
        }

        #[ignore]
        #[tokio::test]
        async fn clear_llm_tags_preserves_locked_user_tags() {
            with_connected_store(|store| async move {
                let pool = store.pool()?;
                let doc_id: i64 = sqlx::query_scalar(
                    "INSERT INTO documents (tenant_id, kind) \
                     VALUES (1, 'document') RETURNING id",
                )
                .fetch_one(&pool)
                .await?;

                let llm_tag = store.upsert_tag(1, "llm-tag", &emb(&[0.1, 0.2])).await?;
                let user_tag = store.upsert_tag(1, "user-tag", &emb(&[0.5, 0.6])).await?;

                // One LLM tag, one user tag (locked).
                store
                    .insert_document_tags(1, doc_id, &[llm_tag], TagSource::Llm)
                    .await?;
                store
                    .insert_document_tags(1, doc_id, &[user_tag], TagSource::User)
                    .await?;

                // Clear LLM tags.
                let removed = store.clear_llm_document_tags(1, doc_id).await?;
                assert_eq!(removed, 1, "only the LLM tag should be removed");

                // User tag remains.
                let locked_ids = store.get_locked_document_tags(1, doc_id).await?;
                assert_eq!(locked_ids, vec![user_tag]);

                Ok(())
            })
            .await
            .unwrap();
        }

        #[ignore]
        #[tokio::test]
        async fn set_document_tag_locked_toggles_flag() {
            with_connected_store(|store| async move {
                let pool = store.pool()?;
                let doc_id: i64 = sqlx::query_scalar(
                    "INSERT INTO documents (tenant_id, kind) \
                     VALUES (1, 'document') RETURNING id",
                )
                .fetch_one(&pool)
                .await?;

                let tag_id = store.upsert_tag(1, "togglable", &emb(&[0.3, 0.4])).await?;

                // Plant an LLM (unlocked) tag.
                store
                    .insert_document_tags(1, doc_id, &[tag_id], TagSource::Llm)
                    .await?;

                // Lock it.
                let affected = store
                    .set_document_tag_locked(1, doc_id, tag_id, true)
                    .await?;
                assert_eq!(affected, 1);

                // Unlock it.
                let affected2 = store
                    .set_document_tag_locked(1, doc_id, tag_id, false)
                    .await?;
                assert_eq!(affected2, 1);

                // Now it should be cleared by clear_llm_document_tags.
                let removed = store.clear_llm_document_tags(1, doc_id).await?;
                assert_eq!(removed, 1);

                Ok(())
            })
            .await
            .unwrap();
        }

        #[ignore]
        #[tokio::test]
        async fn insert_tag_alias_is_idempotent() {
            with_connected_store(|store| async move {
                let tag_id = store.upsert_tag(1, "primary", &emb(&[0.5, 0.5])).await?;

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
            local_only: false,
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
            cost_micros: None,
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

    /// Pad a short seed into the full embedding width that the schema's `VECTOR(N)` columns
    /// (`tags.embedding`, `chunks.embedding`) require — pgvector rejects a mismatched-dimension
    /// insert (`expected N dimensions, not M`). `N` is [`crate::EMBED_DIM`]. Test fixtures only
    /// care about a few leading components, so the tail is zero-filled.
    fn emb(seed: &[f32]) -> Vec<f32> {
        let mut v = vec![0.0f32; crate::EMBED_DIM];
        let n = seed.len().min(crate::EMBED_DIM);
        v[..n].copy_from_slice(&seed[..n]);
        v
    }

    #[test]
    fn emb_pads_to_full_width_preserving_seed() {
        let v = emb(&[1.0, 2.0, 3.0]);
        assert_eq!(
            v.len(),
            crate::EMBED_DIM,
            "must match the VECTOR(N) column width"
        );
        assert_eq!(&v[..3], &[1.0, 2.0, 3.0], "leading seed preserved");
        assert!(v[3..].iter().all(|&x| x == 0.0), "tail zero-filled");
    }

    #[test]
    fn emb_truncates_oversized_seed() {
        let v = emb(&[0.5f32; crate::EMBED_DIM + 256]);
        assert_eq!(
            v.len(),
            crate::EMBED_DIM,
            "an over-long seed is truncated, never overflows"
        );
    }

    #[test]
    fn emb_handles_empty_seed() {
        let v = emb(&[]);
        assert_eq!(v.len(), crate::EMBED_DIM);
        assert!(v.iter().all(|&x| x == 0.0));
    }

    // ── SQL constant validation ─────────────────────────────────────────

    #[test]
    fn usage_insert_sql_is_well_formed() {
        assert!(USAGE_INSERT_SQL.starts_with("INSERT INTO usage_events"));
        assert!(USAGE_INSERT_SQL.contains("RETURNING id"));
        assert!(USAGE_INSERT_SQL.contains("$1"));
        assert!(USAGE_INSERT_SQL.contains("$9"));
        assert!(USAGE_INSERT_SQL.contains("cost_micros"));
    }

    #[test]
    fn monthly_rollup_upsert_sql_is_well_formed() {
        // P14-T1: month-truncated UTC upsert keyed on (tenant_id, period_month),
        // adding the new tokens to the running total on conflict.
        assert!(MONTHLY_ROLLUP_UPSERT_SQL.starts_with("INSERT INTO tenant_monthly_usage"));
        assert!(MONTHLY_ROLLUP_UPSERT_SQL.contains("date_trunc('month', $2::timestamptz"));
        assert!(MONTHLY_ROLLUP_UPSERT_SQL.contains("AT TIME ZONE 'UTC'))::date"));
        assert!(
            MONTHLY_ROLLUP_UPSERT_SQL.contains("ON CONFLICT (tenant_id, period_month) DO UPDATE")
        );
        assert!(
            MONTHLY_ROLLUP_UPSERT_SQL
                .contains("tokens_used = tenant_monthly_usage.tokens_used + EXCLUDED.tokens_used")
        );
        assert!(MONTHLY_ROLLUP_UPSERT_SQL.contains("$1"));
        assert!(MONTHLY_ROLLUP_UPSERT_SQL.contains("$3"));
    }

    #[test]
    fn monthly_rollup_current_sql_is_well_formed() {
        assert!(
            MONTHLY_ROLLUP_CURRENT_SQL.starts_with("SELECT tokens_used FROM tenant_monthly_usage")
        );
        assert!(
            MONTHLY_ROLLUP_CURRENT_SQL
                .contains("date_trunc('month', now() AT TIME ZONE 'UTC'))::date")
        );
        assert!(MONTHLY_ROLLUP_CURRENT_SQL.contains("$1"));
    }

    #[test]
    fn token_usage_month_sql_windows_to_current_utc_month() {
        // P14-T4: the reconciliation SUM must be scoped to the current UTC
        // calendar month (not all-time), matching the rollup's bucketing.
        assert!(TOKEN_USAGE_MONTH_SQL.contains("FROM usage_events"));
        assert!(TOKEN_USAGE_MONTH_SQL.contains("SUM("));
        assert!(TOKEN_USAGE_MONTH_SQL.contains("prompt_tokens"));
        assert!(TOKEN_USAGE_MONTH_SQL.contains("completion_tokens"));
        // The defining clause: a current-UTC-month lower bound on created_at.
        assert!(
            TOKEN_USAGE_MONTH_SQL
                .contains("created_at >= date_trunc('month', now() AT TIME ZONE 'UTC')"),
            "get_token_usage must window to the current UTC month: {TOKEN_USAGE_MONTH_SQL}"
        );
        assert!(TOKEN_USAGE_MONTH_SQL.contains("$1"));
        // Cast back to BIGINT so sqlx can decode the NUMERIC SUM.
        assert!(TOKEN_USAGE_MONTH_SQL.contains("::BIGINT"));
    }

    #[tokio::test]
    async fn get_current_month_token_rollup_errors_before_connect() {
        // Before a pool is connected the rollup read surfaces an error rather
        // than panicking (mirrors the other pre-connect guards).
        let store = PgStore::new("postgres://localhost/db");
        assert!(store.get_current_month_token_rollup(1).await.is_err());
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
            // Shared per-binary container + fresh database; tenant data runs as the
            // non-privileged kb_app role so RLS is enforced (P6-T14).
            let db = kb_testsupport::fresh_db().await?;
            let store = PgStore::with_roles(&db.admin_url, &db.app_url);
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
                let tag1 = store.upsert_tag(1, "alpha", &emb(&[0.1, 0.2])).await?;
                let tag2 = store.upsert_tag(1, "beta", &emb(&[0.3, 0.4])).await?;

                let c1 = make_chunk(1, 0, 0, 0, "chunk A content", crate::EMBED_DIM);
                let c2 = make_chunk(1, 0, 0, 1, "chunk B content", crate::EMBED_DIM);

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
                let tag_id = store.upsert_tag(1, "single-tag", &emb(&[0.5, 0.5])).await?;

                let c = make_chunk(1, 0, 0, 0, "single chunk", crate::EMBED_DIM);

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
                    local_only: false,
                };

                let _doc_id2 = store
                    .transactional_ingest(
                        &doc_ready,
                        &files,
                        &[tag_id],
                        &[vec![make_chunk(
                            1,
                            0,
                            doc_id,
                            0,
                            "re-chunk",
                            crate::EMBED_DIM,
                        )]],
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

        /// Re-ingesting an identical single-file upload (same sha256 + path) with
        /// `doc.id == 0` BOTH times reuses the existing document — the caller never has to
        /// thread the id back itself. This is the plan §749 idempotency contract at the
        /// store layer (the `IngestPipeline` always passes `doc.id == 0`), and is exactly
        /// what the pipeline's p3 integration test exercises end-to-end.
        #[ignore]
        #[tokio::test]
        async fn transactional_ingest_idempotent_reingest_without_explicit_id() {
            with_connected_store(|store| async move {
                let tag_id = store.upsert_tag(1, "t", &emb(&[0.5, 0.5])).await?;
                let files = [make_file_rec(1, 1, "same", b"reingest-without-explicit-id")];

                // First ingest: fresh document, id resolved by the store (not the caller).
                let doc = make_doc(0, 1, "document", 1);
                let c1 = make_chunk(1, 0, 0, 0, "v1", crate::EMBED_DIM);
                let id1 = store
                    .transactional_ingest(&doc, &files, &[tag_id], &[vec![c1]])
                    .await?;

                // Second ingest of the SAME file (same sha + path), again with id == 0.
                let doc2 = make_doc(0, 1, "document", 1);
                let c2 = make_chunk(1, 0, 0, 0, "v2", crate::EMBED_DIM);
                let id2 = store
                    .transactional_ingest(&doc2, &files, &[tag_id], &[vec![c2]])
                    .await?;

                assert_eq!(id1, id2, "identical re-ingest must reuse its document");

                let pool = store.pool()?;
                let doc_count: i64 =
                    sqlx::query_scalar("SELECT COUNT(*) FROM documents WHERE tenant_id = 1")
                        .fetch_one(&pool)
                        .await?;
                assert_eq!(doc_count, 1, "re-ingest must not create a second document");

                let file_count: i64 =
                    sqlx::query_scalar("SELECT COUNT(*) FROM files WHERE document_id = $1")
                        .bind(id1)
                        .fetch_one(&pool)
                        .await?;
                assert_eq!(file_count, 1, "re-ingest must not duplicate the file row");
                Ok(())
            })
            .await
            .unwrap();
        }

        /// Same bytes under a *different* filename must create a NEW document rather than
        /// reassigning the existing file — the BUG-INGEST-02 orphan-avoidance contract.
        /// The idempotency lookup keys on `path`, so a different name does not match and the
        /// first document keeps its file.
        #[ignore]
        #[tokio::test]
        async fn transactional_ingest_same_bytes_different_name_creates_new_document() {
            with_connected_store(|store| async move {
                let tag_id = store.upsert_tag(1, "t", &emb(&[0.5, 0.5])).await?;
                let sha = b"identical-bytes-distinct-names";
                let files_a = [make_file_rec(1, 1, "name-a", sha)];
                let files_b = [make_file_rec(1, 1, "name-b", sha)];

                let doc = make_doc(0, 1, "document", 1);
                let ca = make_chunk(1, 0, 0, 0, "a", crate::EMBED_DIM);
                let id1 = store
                    .transactional_ingest(&doc, &files_a, &[tag_id], &[vec![ca]])
                    .await?;

                // Same sha256, different path → a new document (no orphaning of the first).
                let doc2 = make_doc(0, 1, "document", 1);
                let cb = make_chunk(1, 0, 0, 0, "b", crate::EMBED_DIM);
                let id2 = store
                    .transactional_ingest(&doc2, &files_b, &[tag_id], &[vec![cb]])
                    .await?;

                assert_ne!(
                    id1, id2,
                    "same bytes + different name must create a new document"
                );

                let pool = store.pool()?;
                let first_files: i64 =
                    sqlx::query_scalar("SELECT COUNT(*) FROM files WHERE document_id = $1")
                        .bind(id1)
                        .fetch_one(&pool)
                        .await?;
                assert_eq!(
                    first_files, 1,
                    "first document must keep its file (no orphan)"
                );
                Ok(())
            })
            .await
            .unwrap();
        }

        /// Concurrency: several workers ingesting the SAME single file at once (the
        /// "impatient user uploads the same thing several times in a row" case — uploads become
        /// queued jobs run by a concurrent worker pool) must still collapse to ONE document.
        /// Without serialization the READ COMMITTED idempotency lookup races: each transaction
        /// misses the others' uncommitted file row and INSERTs its own document, and the
        /// `(tenant_id, sha256, document_id)` unique key cannot catch it (distinct doc ids). The
        /// per-(tenant, sha256) advisory lock in `transactional_ingest` serializes them.
        #[ignore]
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn transactional_ingest_concurrent_same_file_collapses_to_one_document() {
            with_connected_store(|store| async move {
                let store = std::sync::Arc::new(store);
                let tag_id = store.upsert_tag(1, "t", &emb(&[0.5, 0.5])).await?;

                const N: usize = 8;
                let mut handles = Vec::with_capacity(N);
                for i in 0..N {
                    let store = std::sync::Arc::clone(&store);
                    handles.push(tokio::spawn(async move {
                        let doc = make_doc(0, 1, "document", 1);
                        let files = [make_file_rec(1, 1, "racy", b"concurrent-same-file")];
                        let c = make_chunk(1, 0, 0, 0, &format!("v{i}"), crate::EMBED_DIM);
                        store
                            .transactional_ingest(&doc, &files, &[tag_id], &[vec![c]])
                            .await
                    }));
                }

                let mut ids = Vec::with_capacity(N);
                for h in handles {
                    ids.push(h.await.expect("ingest task panicked")?);
                }

                // Every concurrent call must resolve to the same document …
                assert!(
                    ids.iter().all(|&id| id == ids[0]),
                    "all concurrent re-ingests must resolve to one document, got {ids:?}"
                );

                // … and the DB must hold exactly one document + one file for the tenant.
                let pool = store.pool()?;
                let doc_count: i64 =
                    sqlx::query_scalar("SELECT COUNT(*) FROM documents WHERE tenant_id = 1")
                        .fetch_one(&pool)
                        .await?;
                assert_eq!(
                    doc_count, 1,
                    "concurrent same-file ingest must not duplicate documents"
                );

                let file_count: i64 =
                    sqlx::query_scalar("SELECT COUNT(*) FROM files WHERE document_id = $1")
                        .bind(ids[0])
                        .fetch_one(&pool)
                        .await?;
                assert_eq!(
                    file_count, 1,
                    "concurrent same-file ingest must not duplicate files"
                );
                Ok(())
            })
            .await
            .unwrap();
        }

        /// Multi-file (multi-page document) re-ingest: uploading the SAME set of files again with
        /// `doc.id == 0` reuses the document by its exact (sha256, path) file set rather than
        /// creating a duplicate.
        #[ignore]
        #[tokio::test]
        async fn transactional_ingest_multifile_reingest_reuses_document() {
            with_connected_store(|store| async move {
                let tag_id = store.upsert_tag(1, "t", &emb(&[0.5, 0.5])).await?;
                let files = [
                    make_file_rec(1, 1, "front", b"front-page-bytes"),
                    make_file_rec(1, 2, "back", b"back-page-bytes"),
                ];
                let chunks1 = vec![
                    vec![make_chunk(1, 0, 0, 0, "front v1", crate::EMBED_DIM)],
                    vec![make_chunk(1, 0, 0, 0, "back v1", crate::EMBED_DIM)],
                ];
                let id1 = store
                    .transactional_ingest(
                        &make_doc(0, 1, "document", 2),
                        &files,
                        &[tag_id],
                        &chunks1,
                    )
                    .await?;

                // Re-ingest the same two pages (id == 0 again) → reuse, not duplicate.
                let chunks2 = vec![
                    vec![make_chunk(1, 0, 0, 0, "front v2", crate::EMBED_DIM)],
                    vec![make_chunk(1, 0, 0, 0, "back v2", crate::EMBED_DIM)],
                ];
                let id2 = store
                    .transactional_ingest(
                        &make_doc(0, 1, "document", 2),
                        &files,
                        &[tag_id],
                        &chunks2,
                    )
                    .await?;

                assert_eq!(
                    id1, id2,
                    "re-ingesting the same multi-file document must reuse it"
                );

                let pool = store.pool()?;
                let doc_count: i64 =
                    sqlx::query_scalar("SELECT COUNT(*) FROM documents WHERE tenant_id = 1")
                        .fetch_one(&pool)
                        .await?;
                assert_eq!(
                    doc_count, 1,
                    "multi-file re-ingest must not duplicate the document"
                );
                let file_count: i64 =
                    sqlx::query_scalar("SELECT COUNT(*) FROM files WHERE document_id = $1")
                        .bind(id1)
                        .fetch_one(&pool)
                        .await?;
                assert_eq!(file_count, 2, "the document keeps exactly its two pages");
                Ok(())
            })
            .await
            .unwrap();
        }

        /// A *different* file set (a page swapped out) must create a NEW document — the lookup
        /// requires an exact (sha256, path) set match, not just an overlap.
        #[ignore]
        #[tokio::test]
        async fn transactional_ingest_multifile_different_set_creates_new_document() {
            with_connected_store(|store| async move {
                let tag_id = store.upsert_tag(1, "t", &emb(&[0.5, 0.5])).await?;
                let a = make_file_rec(1, 1, "a", b"page-a-bytes");
                let b = make_file_rec(1, 2, "b", b"page-b-bytes");
                let c = make_file_rec(1, 2, "c", b"page-c-bytes");
                let chunks = vec![
                    vec![make_chunk(1, 0, 0, 0, "x", crate::EMBED_DIM)],
                    vec![make_chunk(1, 0, 0, 0, "y", crate::EMBED_DIM)],
                ];

                let id1 = store
                    .transactional_ingest(
                        &make_doc(0, 1, "document", 2),
                        &[a.clone(), b],
                        &[tag_id],
                        &chunks,
                    )
                    .await?;
                // {a, c} overlaps {a, b} on `a` but is a different set → a new document.
                let id2 = store
                    .transactional_ingest(
                        &make_doc(0, 1, "document", 2),
                        &[a, c],
                        &[tag_id],
                        &chunks,
                    )
                    .await?;
                assert_ne!(id1, id2, "a different page set must create a new document");
                Ok(())
            })
            .await
            .unwrap();
        }

        /// Concurrency for multi-file: several workers ingesting the SAME multi-page upload at
        /// once must collapse to one document — the advisory lock keys on the whole file set.
        #[ignore]
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        async fn transactional_ingest_concurrent_same_multifile_collapses_to_one_document() {
            with_connected_store(|store| async move {
                let store = std::sync::Arc::new(store);
                let tag_id = store.upsert_tag(1, "t", &emb(&[0.5, 0.5])).await?;

                const N: usize = 6;
                let mut handles = Vec::with_capacity(N);
                for i in 0..N {
                    let store = std::sync::Arc::clone(&store);
                    handles.push(tokio::spawn(async move {
                        let files = [
                            make_file_rec(1, 1, "front", b"front-page"),
                            make_file_rec(1, 2, "back", b"back-page"),
                        ];
                        let chunks = vec![
                            vec![make_chunk(1, 0, 0, 0, &format!("f{i}"), crate::EMBED_DIM)],
                            vec![make_chunk(1, 0, 0, 0, &format!("b{i}"), crate::EMBED_DIM)],
                        ];
                        store
                            .transactional_ingest(
                                &make_doc(0, 1, "document", 2),
                                &files,
                                &[tag_id],
                                &chunks,
                            )
                            .await
                    }));
                }

                let mut ids = Vec::with_capacity(N);
                for h in handles {
                    ids.push(h.await.expect("ingest task panicked")?);
                }
                assert!(
                    ids.iter().all(|&id| id == ids[0]),
                    "concurrent same multi-file ingests must collapse to one document, got {ids:?}"
                );

                let pool = store.pool()?;
                let doc_count: i64 =
                    sqlx::query_scalar("SELECT COUNT(*) FROM documents WHERE tenant_id = 1")
                        .fetch_one(&pool)
                        .await?;
                assert_eq!(
                    doc_count, 1,
                    "concurrent multi-file ingest must not duplicate documents"
                );
                let file_count: i64 =
                    sqlx::query_scalar("SELECT COUNT(*) FROM files WHERE document_id = $1")
                        .bind(ids[0])
                        .fetch_one(&pool)
                        .await?;
                assert_eq!(file_count, 2, "the document keeps exactly its two pages");
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
    async fn is_platform_admin_tenant_errors_before_connect() {
        // Fails closed: callers (provider/model/route mutation gates) treat the
        // error as a denial, so a disconnected DB never grants access.
        let store = PgStore::new("postgres://localhost/db");
        let err = store.is_platform_admin_tenant(1).await.unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn create_user_is_tenant_scoped() {
        // create_user is a tenant-scoped method: it acquires the app pool and opens a tenant
        // transaction. Without a connected store it fails fast on the pool lookup (we can't
        // exercise begin_tenant_tx without a real DB — that path is integration-tested).
        let store = PgStore::new("postgres://localhost/db");
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
            // Shared per-binary container + fresh database; tenant data runs as the
            // non-privileged kb_app role so RLS is enforced (P6-T14).
            let db = kb_testsupport::fresh_db().await?;
            let store = PgStore::with_roles(&db.admin_url, &db.app_url);
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
        async fn is_platform_admin_tenant_only_matches_first_tenant() {
            with_connected_store(|store| async move {
                // The helper seeded the first tenant (id 1 — the operator's tenant).
                assert!(
                    store.is_platform_admin_tenant(1).await?,
                    "the first-seeded tenant must be the platform-admin tenant"
                );

                // A later customer tenant is NOT the platform-admin tenant.
                let other = store.create_tenant("customer", "Customer").await?;
                assert!(other > 1);
                assert!(
                    !store.is_platform_admin_tenant(other).await?,
                    "a later-created tenant must NOT be the platform-admin tenant"
                );

                // An unknown id is not the platform-admin tenant either.
                assert!(!store.is_platform_admin_tenant(9_999).await?);

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

    // ── P5-T7 export listing error-before-connect ───────────────────────

    #[tokio::test]
    async fn list_documents_errors_before_connect() {
        let store = PgStore::new("postgres://localhost/db");
        let err = store.list_documents(1).await.unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn list_files_errors_before_connect() {
        let store = PgStore::new("postgres://localhost/db");
        let err = store.list_files(1).await.unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn list_tags_errors_before_connect() {
        let store = PgStore::new("postgres://localhost/db");
        let err = store.list_tags(1).await.unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn list_tag_aliases_errors_before_connect() {
        let store = PgStore::new("postgres://localhost/db");
        let err = store.list_tag_aliases(1).await.unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn list_document_tags_errors_before_connect() {
        let store = PgStore::new("postgres://localhost/db");
        let err = store.list_document_tags(1).await.unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn clear_llm_document_tags_errors_before_connect() {
        let store = PgStore::new("postgres://localhost/db");
        let err = store.clear_llm_document_tags(1, 1).await.unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn get_locked_document_tags_errors_before_connect() {
        let store = PgStore::new("postgres://localhost/db");
        let err = store.get_locked_document_tags(1, 1).await.unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn set_document_tag_locked_errors_before_connect() {
        let store = PgStore::new("postgres://localhost/db");
        let err = store
            .set_document_tag_locked(1, 1, 42, true)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn remove_document_tag_errors_before_connect() {
        let store = PgStore::new("postgres://localhost/db");
        let err = store.remove_document_tag(1, 1, 42).await.unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn update_file_page_order_errors_before_connect() {
        let store = PgStore::new("postgres://localhost/db");
        let err = store.update_file_page_order(1, 1, 1).await.unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn list_chunks_errors_before_connect() {
        let store = PgStore::new("postgres://localhost/db");
        let err = store.list_chunks(1).await.unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn get_tenant_slug_errors_before_connect() {
        let store = PgStore::new("postgres://localhost/db");
        let err = store.get_tenant_slug(1).await.unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[test]
    fn document_from_row_valid() {
        let row = DocumentRow {
            id: 1,
            tenant_id: 42,
            title: Some("Test".into()),
            summary: Some("Summary".into()),
            summary_enc: None,
            user_note: None,
            user_note_enc: None,
            kind: "document".into(),
            meta: serde_json::json!({}),
            page_count: 1,
            local_only: false,
            status: "ready".into(),
            created_at: chrono::Utc::now(),
        };
        let doc = document_from_row(row).unwrap();
        assert_eq!(doc.id, 1);
        assert_eq!(doc.tenant_id, 42);
        assert_eq!(doc.title.as_deref(), Some("Test"));
        assert_eq!(doc.kind, kb_core::kind::DocKind::Document);
        assert_eq!(doc.status, kb_core::status::ProcessingStatus::Ready);
    }

    #[test]
    fn document_from_row_invalid_kind() {
        let row = DocumentRow {
            id: 1,
            tenant_id: 1,
            title: None,
            summary: None,
            summary_enc: None,
            user_note: None,
            user_note_enc: None,
            kind: "not_a_valid_kind".into(),
            meta: serde_json::json!({}),
            page_count: 1,
            local_only: false,
            status: "ready".into(),
            created_at: chrono::Utc::now(),
        };
        let err = document_from_row(row).unwrap_err();
        assert!(err.to_string().contains("invalid DocKind"));
    }

    #[test]
    fn file_from_row_valid() {
        let sha = vec![0u8; 32];
        let row = FileRow {
            id: 10,
            tenant_id: 1,
            document_id: 5,
            page_no: 1,
            page_label: Some("front".into()),
            sha256: sha.clone(),
            blob_key: "key".into(),
            path: Some("/tmp/test.txt".into()),
            mime: Some("text/plain".into()),
            size_bytes: Some(100),
            meta: serde_json::json!({}),
            status: "ready".into(),
            ingested_at: chrono::Utc::now(),
        };
        let file = file_from_row(row).unwrap();
        assert_eq!(file.id, 10);
        assert_eq!(file.document_id, 5);
        assert_eq!(file.page_label.as_deref(), Some("front"));
        assert_eq!(file.status, kb_core::status::ProcessingStatus::Ready);
    }

    #[test]
    fn file_from_row_invalid_sha_length() {
        let row = FileRow {
            id: 1,
            tenant_id: 1,
            document_id: 1,
            page_no: 1,
            page_label: None,
            sha256: vec![0u8; 16], // wrong length
            blob_key: "key".into(),
            path: None,
            mime: None,
            size_bytes: None,
            meta: serde_json::json!({}),
            status: "ready".into(),
            ingested_at: chrono::Utc::now(),
        };
        let err = file_from_row(row).unwrap_err();
        assert!(err.to_string().contains("invalid sha256 length"));
    }

    #[test]
    fn file_from_row_invalid_status() {
        let row = FileRow {
            id: 1,
            tenant_id: 1,
            document_id: 1,
            page_no: 1,
            page_label: None,
            sha256: vec![0u8; 32],
            blob_key: "key".into(),
            path: None,
            mime: None,
            size_bytes: None,
            meta: serde_json::json!({}),
            status: "bad_status".into(),
            ingested_at: chrono::Utc::now(),
        };
        let err = file_from_row(row).unwrap_err();
        assert!(err.to_string().contains("invalid ProcessingStatus"));
    }

    // ── P6-T2 API query methods — error-before-connect ──────────────────

    #[tokio::test]
    async fn get_document_errors_before_connect() {
        let store = PgStore::new("postgres://localhost/db");
        let err = store.get_document(1, 1).await.unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn get_files_for_document_errors_before_connect() {
        let store = PgStore::new("postgres://localhost/db");
        let err = store.get_files_for_document(1, 1).await.unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn get_tags_for_document_errors_before_connect() {
        let store = PgStore::new("postgres://localhost/db");
        let err = store.get_tags_for_document(1, 1).await.unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn find_tag_by_name_errors_before_connect() {
        let store = PgStore::new("postgres://localhost/db");
        let err = store.find_tag_by_name(1, "irrelevant").await.unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn get_file_errors_before_connect() {
        let store = PgStore::new("postgres://localhost/db");
        let err = store.get_file(1, 1).await.unwrap_err();
        assert!(err.to_string().contains("not connected"));
    }

    #[tokio::test]
    async fn get_job_errors_before_connect() {
        let store = PgStore::new("postgres://localhost/db");
        let err = store.get_job(1, 1).await.unwrap_err();
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
            // Shared per-binary container + fresh database; tenant data runs as the
            // non-privileged kb_app role so RLS is enforced (P6-T14).
            let db = kb_testsupport::fresh_db().await?;
            let store = PgStore::with_roles(&db.admin_url, &db.app_url);
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
