// SPDX-License-Identifier: AGPL-3.0-or-later

//! Pending-ingest staging on [`PgStore`] — the upload-time half of the async
//! queued ingestion flow (P15-T1, plan §16).
//!
//! In queued mode the upload handler stores blobs, then calls
//! [`PgStore::create_pending_ingest`] to persist the document + file rows with
//! `status = 'pending'` in **one tenant transaction**, and finally enqueues an
//! ingest job carrying the returned `document_id`. A worker later loads the
//! pending rows + blobs, runs the pipeline, and `transactional_ingest`
//! finalizes the same rows to `ready` (its file upsert keys on
//! `(tenant_id, sha256, document_id)`, so the staged rows are updated in
//! place — retries converge idempotently).
//!
//! The staging insert reuses `transactional_ingest`'s two race/duplication
//! defenses verbatim: the per-upload `pg_advisory_xact_lock` (keyed on the
//! whole file set) and the exact-file-set reuse lookup, so a duplicate upload
//! re-stages the existing document instead of creating a twin.

use anyhow::Context;
use kb_core::document::Document;
use kb_core::envelope;
use kb_core::file::FileRecord;
use kb_core::status::ProcessingStatus;
use sqlx::Postgres;

use crate::pg_store::{PgStore, begin_tenant_tx, format_vector, ingest_advisory_key};

/// Result of staging an upload as a pending document (queued-ingest mode).
#[derive(Debug)]
pub struct PendingIngest {
    /// The (new or reused) document id the ingest job must carry.
    pub document_id: i64,
    /// Ids of the staged file rows, in `page_no` order.
    pub file_ids: Vec<i64>,
    /// `true` when an existing document with exactly this file set was
    /// re-staged instead of a new one being created (idempotent re-upload).
    pub reused: bool,
}

/// Snapshot of the latest ingest job's retry state surfaced to the document
/// detail page so users see retry progress instead of a static badge.
pub struct PendingJobInfo {
    /// Total attempts so far (0 = first attempt in progress, 1+ = retrying).
    pub attempts: i32,
    /// When the job becomes eligible again (set by exponential backoff).
    pub run_after: Option<chrono::DateTime<chrono::Utc>>,
    /// The last recorded error message, if any.
    pub last_error: Option<String>,
}

impl PgStore {
    /// Stage an upload as a `pending` document + file rows in one tenant
    /// transaction, ready for a queued worker to process (P15-T1).
    ///
    /// `doc.id` must be `0` (the id is assigned here) and `files` non-empty;
    /// `doc.user_note` / `doc.local_only` are persisted so the worker can
    /// recover them at processing time. When a KEK is configured the
    /// `user_note` is envelope-encrypted exactly like the ready-path write.
    /// An identical upload (same `(sha256, path)` set) re-stages the existing
    /// document (`reused = true`) rather than duplicating it.
    ///
    /// # Errors
    /// Returns an error if `files` is empty, `doc.id != 0`, the database is
    /// not connected, encryption fails, or any query fails (the transaction
    /// rolls back — no partial staging).
    pub async fn create_pending_ingest(
        &self,
        doc: &Document,
        files: &[FileRecord],
    ) -> anyhow::Result<PendingIngest> {
        anyhow::ensure!(!files.is_empty(), "create_pending_ingest: no files");
        anyhow::ensure!(
            doc.id == 0,
            "create_pending_ingest: doc.id must be 0 (got {})",
            doc.id
        );

        // Encrypt the user note up front if a KEK is configured (the title and
        // summary are unknown until the worker tags the document).  Fetch the
        // DEK once — the stub-chunk path below reuses the same key.
        let dek = if self.kek().is_some() {
            Some(self.get_or_create_tenant_dek(doc.tenant_id).await?)
        } else {
            None
        };

        let user_note_enc = if let Some(ref d) = dek {
            doc.user_note
                .as_deref()
                .map(|s| envelope::encrypt_column(d, s))
                .transpose()
                .context("failed to encrypt pending document user_note")?
        } else {
            None
        };

        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, doc.tenant_id).await?;

        // Serialize concurrent stagings of the SAME upload so the reuse lookup
        // below cannot race (mirrors transactional_ingest's advisory lock; the
        // worker's finalize takes the explicit-id path and never contends).
        let shas: Vec<&[u8]> = files
            .iter()
            .map(|f| f.sha256.as_bytes().as_slice())
            .collect();
        let lock_key = ingest_advisory_key(doc.tenant_id, &shas);
        sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(lock_key)
            .execute(&mut *tx)
            .await
            .context("failed to acquire same-upload staging advisory lock")?;

        // Reuse the document whose file set is EXACTLY this upload's
        // (sha256, path) pairs — identical SQL to transactional_ingest so the
        // staged and ready paths agree on what "the same upload" means.
        let sha_hexes: Vec<String> = files.iter().map(|f| f.sha256.to_hex()).collect();
        let paths: Vec<Option<String>> = files.iter().map(|f| f.path.clone()).collect();
        let existing: Option<i64> = sqlx::query_scalar::<Postgres, i64>(
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
        .context("failed to look up existing document for idempotent staging")?;

        let (document_id, reused) = if let Some(id) = existing {
            // Re-stage: back to pending; refresh the note (it drives tagging).
            // Title/summary/meta stay until the worker overwrites them.
            sqlx::query(
                "UPDATE documents SET status='pending', user_note=$1, user_note_enc=$2, \
                 local_only=$3 WHERE id=$4",
            )
            .bind(&doc.user_note)
            .bind(&user_note_enc)
            .bind(doc.local_only)
            .bind(id)
            .execute(&mut *tx)
            .await
            .context("failed to re-stage existing document as pending")?;
            (id, true)
        } else {
            let id: i64 = sqlx::query_scalar(
                "INSERT INTO documents \
                 (tenant_id,title,summary,summary_enc,user_note,user_note_enc,kind,meta,\
                  page_count,status,local_only) \
                 VALUES ($1,NULL,NULL,NULL,$2,$3,$4,$5,$6,'pending',$7) RETURNING id",
            )
            .bind(doc.tenant_id)
            .bind(&doc.user_note)
            .bind(&user_note_enc)
            .bind(doc.kind.as_str())
            .bind(&doc.meta)
            .bind(doc.page_count)
            .bind(doc.local_only)
            .fetch_one(&mut *tx)
            .await
            .context("failed to insert pending document")?;
            (id, false)
        };

        // Stage each file as pending against the resolved document. The same
        // (tenant_id, sha256, document_id) conflict target as the finalize
        // upsert means a re-staged upload updates its existing rows in place.
        let mut file_ids = Vec::with_capacity(files.len());
        for file in files {
            let fid: i64 = sqlx::query_scalar(
                "INSERT INTO files (tenant_id,document_id,page_no,page_label,sha256,blob_key,\
                 path,mime,size_bytes,meta,status,ingested_at) \
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'pending',$11) \
                 ON CONFLICT (tenant_id,sha256,document_id) DO UPDATE SET \
                 page_no=EXCLUDED.page_no,page_label=EXCLUDED.page_label,\
                 blob_key=EXCLUDED.blob_key,path=EXCLUDED.path,mime=EXCLUDED.mime,\
                 size_bytes=EXCLUDED.size_bytes,meta=EXCLUDED.meta,status='pending',\
                 ingested_at=EXCLUDED.ingested_at \
                 RETURNING id",
            )
            .bind(file.tenant_id)
            .bind(document_id)
            .bind(file.page_no)
            .bind(&file.page_label)
            .bind(file.sha256.as_bytes().as_slice())
            .bind(&file.blob_key)
            .bind(&file.path)
            .bind(&file.mime)
            .bind(file.size_bytes)
            .bind(&file.meta)
            .bind(file.ingested_at)
            .fetch_one(&mut *tx)
            .await
            .context("failed to stage pending file")?;
            file_ids.push(fid);
        }

        // Stub chunks for immediate keyword searchability (P15-UX):
        // each pending file gets one stub chunk carrying its filename (and the
        // user note, if provided) so the document appears in keyword-search
        // results even before the worker finishes AI enrichment.  The worker's
        // `transactional_ingest` deletes all chunks `WHERE file_id = $1` before
        // inserting the real ones, so stubs are automatically cleaned up.
        // Re-staged documents already carry chunks from the prior cycle; skip.
        if !reused {
            let zero_vec = format_vector(&vec![0.0f32; 2560]);
            for (file, &fid) in files.iter().zip(file_ids.iter()) {
                let mut content = file.path.as_deref().unwrap_or("unnamed").to_string();
                if let Some(ref note) = doc.user_note {
                    content.push(' ');
                    content.push_str(note);
                }
                let content_enc = dek
                    .as_ref()
                    .map(|d| envelope::encrypt_column(d, &content))
                    .transpose()
                    .context("failed to encrypt stub chunk content")?;
                sqlx::query(
                    "INSERT INTO chunks \
                     (tenant_id,document_id,file_id,page_no,idx,content,content_enc,ts_offset,embedding) \
                     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,CAST($9 AS vector))",
                )
                .bind(doc.tenant_id)
                .bind(document_id)
                .bind(fid)
                .bind(file.page_no)
                .bind(0i32)
                .bind(&content)
                .bind(&content_enc)
                .bind(None::<f64>)
                .bind(&zero_vec)
                .execute(&mut *tx)
                .await
                .context("failed to insert stub chunk")?;
            }
        }

        tx.commit()
            .await
            .context("failed to commit create_pending_ingest")?;

        Ok(PendingIngest {
            document_id,
            file_ids,
            reused,
        })
    }

    /// Stage additional `pending` files against an **existing** document and
    /// flip the document back to `pending` for re-processing (P15-T5, the web
    /// "add page" flow). Returns the staged file row ids.
    ///
    /// The queued worker then re-processes the whole document (all pages, old
    /// and new) so the title/summary/tags reflect the updated content.
    ///
    /// # Errors
    /// Returns an error if `files` is empty, the database is not connected, or
    /// any query fails (the transaction rolls back).
    pub async fn stage_pending_files(
        &self,
        tenant_id: i64,
        document_id: i64,
        files: &[FileRecord],
    ) -> anyhow::Result<Vec<i64>> {
        anyhow::ensure!(!files.is_empty(), "stage_pending_files: no files");
        anyhow::ensure!(
            document_id > 0,
            "stage_pending_files: document_id must be positive"
        );

        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, tenant_id).await?;

        // The whole document is re-processed, so it goes back to pending.
        let updated = sqlx::query("UPDATE documents SET status='pending' WHERE id=$1")
            .bind(document_id)
            .execute(&mut *tx)
            .await
            .context("failed to mark document pending for added pages")?;
        anyhow::ensure!(
            updated.rows_affected() > 0,
            "document {document_id} not found for tenant"
        );

        let mut file_ids = Vec::with_capacity(files.len());
        for file in files {
            let fid: i64 = sqlx::query_scalar(
                "INSERT INTO files (tenant_id,document_id,page_no,page_label,sha256,blob_key,\
                 path,mime,size_bytes,meta,status,ingested_at) \
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'pending',$11) \
                 ON CONFLICT (tenant_id,sha256,document_id) DO UPDATE SET \
                 page_no=EXCLUDED.page_no,page_label=EXCLUDED.page_label,\
                 blob_key=EXCLUDED.blob_key,path=EXCLUDED.path,mime=EXCLUDED.mime,\
                 size_bytes=EXCLUDED.size_bytes,meta=EXCLUDED.meta,status='pending',\
                 ingested_at=EXCLUDED.ingested_at \
                 RETURNING id",
            )
            .bind(tenant_id)
            .bind(document_id)
            .bind(file.page_no)
            .bind(&file.page_label)
            .bind(file.sha256.as_bytes().as_slice())
            .bind(&file.blob_key)
            .bind(&file.path)
            .bind(&file.mime)
            .bind(file.size_bytes)
            .bind(&file.meta)
            .bind(file.ingested_at)
            .fetch_one(&mut *tx)
            .await
            .context("failed to stage added pending file")?;
            file_ids.push(fid);
        }

        tx.commit()
            .await
            .context("failed to commit stage_pending_files")?;
        Ok(file_ids)
    }

    /// Set a document's processing status, aligning its files (P15-T1).
    ///
    /// Used by the queued flow's failure/retry transitions: a dead-lettered
    /// job marks its document (and still-unfinished files) `failed`; a tenant
    /// retry resets them to `pending`. Files already `ready` are left `ready`
    /// (a partially-reused document keeps its finished pages; the worker's
    /// finalize overwrites file status wholesale anyway).
    ///
    /// # Errors
    /// Returns an error if the database is not connected or a query fails.
    pub async fn set_document_status(
        &self,
        tenant_id: i64,
        document_id: i64,
        status: ProcessingStatus,
    ) -> anyhow::Result<()> {
        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, tenant_id).await?;
        sqlx::query("UPDATE documents SET status=$1 WHERE id=$2")
            .bind(status.as_str())
            .bind(document_id)
            .execute(&mut *tx)
            .await
            .context("failed to set document status")?;
        sqlx::query("UPDATE files SET status=$1 WHERE document_id=$2 AND status <> 'ready'")
            .bind(status.as_str())
            .bind(document_id)
            .execute(&mut *tx)
            .await
            .context("failed to align file statuses")?;
        tx.commit()
            .await
            .context("failed to commit set_document_status")?;
        Ok(())
    }

    /// Count not-yet-done ingest jobs for bounded-queue admission (P15-T1).
    ///
    /// Returns `(tenant_pending, global_pending)` where "pending" means an
    /// ingest job in `queued`, `running`, or `failed` (retry-eligible) state —
    /// the work the system still owes. Served by the `jobs_tenant_pending`
    /// partial index; one indexed aggregate per upload request.
    ///
    /// Uses the **admin pool** — the `jobs` table is queue infrastructure
    /// outside RLS, and the global count spans tenants by design (the caller
    /// only ever compares it against the global cap).
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn count_pending_ingest_jobs(&self, tenant_id: i64) -> anyhow::Result<(i64, i64)> {
        let pool = self.pool()?;
        let (tenant_pending, global_pending): (i64, i64) = sqlx::query_as(
            "SELECT count(*) FILTER (WHERE tenant_id = $1), count(*) \
             FROM jobs \
             WHERE kind = 'ingest' AND status IN ('queued','running','failed')",
        )
        .bind(tenant_id)
        .fetch_one(&pool)
        .await
        .context("failed to count pending ingest jobs")?;
        Ok((tenant_pending, global_pending))
    }

    /// Count of this tenant's in-flight ingest jobs (queued or running) —
    /// feeds the nav "processing N" indicator (P15-T10).
    ///
    /// Admin pool with an explicit `tenant_id` filter (§31.5); served by the
    /// `jobs_tenant_pending` partial index.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn count_active_ingest_jobs(&self, tenant_id: i64) -> anyhow::Result<i64> {
        let pool = self.pool()?;
        let n: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM jobs \
             WHERE tenant_id = $1 AND kind = 'ingest' AND status IN ('queued','running')",
        )
        .bind(tenant_id)
        .fetch_one(&pool)
        .await
        .context("failed to count active ingest jobs")?;
        Ok(n)
    }

    /// The latest ingest job's `last_error`, `attempts`, and `run_after` for a
    /// document (failure UX, P15-T9 + retry-progress surfacing).
    ///
    /// Admin pool with an explicit `tenant_id` filter (§31.5 — jobs sit
    /// outside RLS); callers must already have proven document ownership via
    /// an RLS-scoped read. Returns `None` when the document has no ingest job.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or the query fails.
    pub async fn latest_ingest_job_info(
        &self,
        tenant_id: i64,
        document_id: i64,
    ) -> anyhow::Result<Option<PendingJobInfo>> {
        let pool = self.pool()?;
        let row: Option<(i32, Option<chrono::DateTime<chrono::Utc>>, Option<String>)> =
            sqlx::query_as(
                "SELECT attempts, run_after, last_error FROM jobs \
                 WHERE tenant_id = $1 AND document_id = $2 AND kind = 'ingest' \
                 ORDER BY id DESC LIMIT 1",
            )
            .bind(tenant_id)
            .bind(document_id)
            .fetch_optional(&pool)
            .await
            .context("failed to read latest ingest job info")?;
        Ok(row.map(|(attempts, run_after, last_error)| PendingJobInfo {
            attempts,
            run_after,
            last_error,
        }))
    }

    /// Convenience: the latest ingest job's `last_error` for a document.
    pub async fn latest_ingest_job_error(
        &self,
        tenant_id: i64,
        document_id: i64,
    ) -> anyhow::Result<Option<String>> {
        Ok(self
            .latest_ingest_job_info(tenant_id, document_id)
            .await?
            .and_then(|info| info.last_error))
    }

    /// Requeue a document's terminal ingest job and reset the document for
    /// reprocessing (the tenant retry action, P15-T9).
    ///
    /// Finds the document's **latest** ingest job; when it is `dead` or
    /// `failed`, resets it to `queued` (attempts 0, error and lease cleared,
    /// eligible now) and moves the document — plus its `failed` files — back
    /// to `pending`, all in one transaction.
    ///
    /// Runs on the **admin pool** (jobs sit outside RLS) with an explicit
    /// `tenant_id` on every statement (§31.5); the caller is responsible for
    /// proving document ownership first via an RLS-scoped read, so a
    /// cross-tenant id simply finds no job here and nothing changes.
    ///
    /// # Errors
    /// Returns an error if the database is not connected or a query fails.
    pub async fn retry_failed_ingest(
        &self,
        tenant_id: i64,
        document_id: i64,
    ) -> anyhow::Result<RetryIngestOutcome> {
        let pool = self.pool()?;
        let mut tx = pool
            .begin()
            .await
            .context("failed to begin retry transaction")?;

        let latest: Option<(i64, String)> = sqlx::query_as(
            "SELECT id, status FROM jobs \
             WHERE tenant_id = $1 AND document_id = $2 AND kind = 'ingest' \
             ORDER BY id DESC LIMIT 1 FOR UPDATE",
        )
        .bind(tenant_id)
        .bind(document_id)
        .fetch_optional(&mut *tx)
        .await
        .context("failed to read latest ingest job for retry")?;

        let Some((job_id, status)) = latest else {
            return Ok(RetryIngestOutcome::NoJob);
        };
        if status != "dead" && status != "failed" {
            return Ok(RetryIngestOutcome::NotRetryable { status });
        }

        sqlx::query(
            "UPDATE jobs SET status = 'queued', attempts = 0, last_error = NULL, \
             run_after = now(), lease_expires_at = NULL, locked_by = NULL \
             WHERE id = $1 AND tenant_id = $2",
        )
        .bind(job_id)
        .bind(tenant_id)
        .execute(&mut *tx)
        .await
        .context("failed to requeue ingest job")?;

        sqlx::query(
            "UPDATE documents SET status = 'pending' \
             WHERE id = $1 AND tenant_id = $2 AND status <> 'ready'",
        )
        .bind(document_id)
        .bind(tenant_id)
        .execute(&mut *tx)
        .await
        .context("failed to reset document for retry")?;
        sqlx::query(
            "UPDATE files SET status = 'pending' \
             WHERE document_id = $1 AND tenant_id = $2 AND status = 'failed'",
        )
        .bind(document_id)
        .bind(tenant_id)
        .execute(&mut *tx)
        .await
        .context("failed to reset files for retry")?;

        tx.commit()
            .await
            .context("failed to commit retry transaction")?;
        Ok(RetryIngestOutcome::Retried { job_id })
    }
}

/// Outcome of [`PgStore::retry_failed_ingest`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetryIngestOutcome {
    /// The job was requeued; the document is `pending` again.
    Retried {
        /// The requeued ingest job id (pollable via `/api/jobs/:id`).
        job_id: i64,
    },
    /// The latest ingest job is not in a terminal failure state.
    NotRetryable {
        /// Its current status (e.g. `queued`, `running`, `done`).
        status: String,
    },
    /// No ingest job exists for this document (e.g. ingested inline).
    NoJob,
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use kb_core::hash::Sha256;
    use kb_core::kind::DocKind;

    fn test_store() -> PgStore {
        PgStore::new("postgres://localhost/db")
    }

    fn doc(tenant_id: i64) -> Document {
        Document {
            id: 0,
            tenant_id,
            title: None,
            summary: None,
            user_note: Some("note".into()),
            kind: DocKind::Document,
            meta: serde_json::json!({}),
            page_count: 1,
            status: ProcessingStatus::Pending,
            created_at: chrono::Utc::now(),
            local_only: false,
            source_url: None,
            fetch_interval_secs: None,
            next_fetch_at: None,
            last_fetched_at: None,
            last_fetch_sha256: None,
            current_version: 1,
            fetch_failure_count: 0,
        }
    }

    fn file(tenant_id: i64) -> FileRecord {
        FileRecord {
            id: 0,
            tenant_id,
            document_id: 0,
            page_no: 1,
            page_label: Some("p1".into()),
            sha256: Sha256::from_hex(
                "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824",
            )
            .unwrap(),
            blob_key: "2cf24dba".into(),
            path: Some("a.txt".into()),
            mime: Some("text/plain".into()),
            size_bytes: Some(5),
            meta: serde_json::json!({}),
            status: ProcessingStatus::Pending,
            ingested_at: chrono::Utc::now(),
        }
    }

    #[tokio::test]
    async fn create_pending_ingest_errors_before_connect() {
        let store = test_store();
        let err = store
            .create_pending_ingest(&doc(1), &[file(1)])
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("not connected"),
            "expected 'not connected', got: {err}"
        );
    }

    #[tokio::test]
    async fn create_pending_ingest_rejects_empty_files() {
        let store = test_store();
        let err = store.create_pending_ingest(&doc(1), &[]).await.unwrap_err();
        assert!(err.to_string().contains("no files"), "got: {err}");
    }

    #[tokio::test]
    async fn create_pending_ingest_rejects_nonzero_doc_id() {
        let store = test_store();
        let mut d = doc(1);
        d.id = 7;
        let err = store
            .create_pending_ingest(&d, &[file(1)])
            .await
            .unwrap_err();
        assert!(err.to_string().contains("doc.id must be 0"), "got: {err}");
    }

    #[tokio::test]
    async fn set_document_status_errors_before_connect() {
        let store = test_store();
        let err = store
            .set_document_status(1, 1, ProcessingStatus::Failed)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("not connected"), "got: {err}");
    }

    #[tokio::test]
    async fn count_pending_ingest_jobs_errors_before_connect() {
        let store = test_store();
        let err = store.count_pending_ingest_jobs(1).await.unwrap_err();
        assert!(err.to_string().contains("not connected"), "got: {err}");
    }
}
