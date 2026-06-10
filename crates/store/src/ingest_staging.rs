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

use crate::pg_store::{PgStore, begin_tenant_tx, ingest_advisory_key};

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
        // summary are unknown until the worker tags the document).
        let user_note_enc = if self.kek().is_some() {
            let dek = self.get_or_create_tenant_dek(doc.tenant_id).await?;
            doc.user_note
                .as_deref()
                .map(|s| envelope::encrypt_column(&dek, s))
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

        tx.commit()
            .await
            .context("failed to commit create_pending_ingest")?;

        Ok(PendingIngest {
            document_id,
            file_ids,
            reused,
        })
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
