// SPDX-License-Identifier: AGPL-3.0-or-later

//! Refetch worker handler — P17 source-sync job runner.
//!
//! Called by the worker pool for each `JobKind::Refetch` job. Fetches the
//! document's `source_url`, compares the content hash, and either
//! short-circuits (no change) or runs the full re-ingest pipeline with a
//! version snapshot and LLM diff summary.

use anyhow::Context;
use chrono::Utc;
use kb_core::audit::AuditAction;
use kb_core::document::Document;
use kb_core::job::Job;
use kb_core::source_sync::{compute_next_fetch_at, decide_refetch_action};
use kb_extract::source_fetch::{self, FetchLimits, FetchedContent};
use kb_llm::DiffSummaryGenerator;
use kb_store::PgStore;

use crate::ingest::{IngestFile, IngestPipeline};

/// Process one `JobKind::Refetch` job.
///
/// Returns `Ok(())` on success (including no-change skip). Returns `Err`
/// for transient failures (retry) or permanent failures (dead-letter via
/// the `"permanent: "` prefix convention in [`crate::job_queue`]).
#[allow(clippy::too_many_arguments)]
pub async fn process_refetch_job(
    job: &Job,
    pipeline: &IngestPipeline,
    pg: &PgStore,
    diff_gen: &DiffSummaryGenerator,
    fetch_limits: &FetchLimits,
    min_fetch_interval: i64,
    max_fetch_interval: i64,
    max_backoff_secs: i64,
    plan_local_only: bool,
) -> anyhow::Result<()> {
    let tenant_id = job.tenant_id;
    let doc_id = job.document_id.context("Refetch job has no document_id")?;

    // 1. Load the document.
    let doc = pg
        .get_document(tenant_id, doc_id)
        .await?
        .context("document not found for refetch job")?;

    let source_url = doc
        .source_url
        .clone()
        .context("document has no source_url — refetch job is stale")?;

    let effective_local_only = doc.local_only || plan_local_only;

    // 2. Fetch the URL (SSRF-guarded).
    let fetched = match source_fetch::fetch_url(&source_url, fetch_limits).await {
        Ok(f) => f,
        Err(e) => {
            record_failure(pg, tenant_id, doc_id, min_fetch_interval, max_backoff_secs).await?;
            emit_audit(
                pg,
                job.created_by,
                tenant_id,
                AuditAction::DocumentRefetchFailed,
                doc_id,
                &serde_json::json!({"error": format!("{e:#}"), "source_url": source_url}),
            )
            .await;
            if e.is_permanent() {
                return Err(anyhow::anyhow!(
                    "permanent: refetch failed for doc {doc_id}: {e}"
                ));
            }
            return Err(anyhow::anyhow!("refetch failed for doc {doc_id}: {e}"));
        }
    };

    // 3. Compare hashes.
    let action = doc
        .last_fetch_sha256
        .as_deref()
        .and_then(|b| <[u8; 32]>::try_from(b).ok())
        .map(|old| decide_refetch_action(&old, &fetched.sha256))
        .unwrap_or(kb_core::source_sync::RefetchAction::Changed);

    match action {
        kb_core::source_sync::RefetchAction::Unchanged => {
            let interval = doc.fetch_interval_secs.unwrap_or(min_fetch_interval);
            let next = compute_next_fetch_at(Utc::now(), Some(interval))
                .context("compute next_fetch_at")?;

            pg.mark_document_fetch_unchanged(tenant_id, doc_id, &fetched.sha256, next)
                .await?;

            emit_audit(
                pg,
                job.created_by,
                tenant_id,
                AuditAction::DocumentRefetchSkipped,
                doc_id,
                &serde_json::json!({"sha256": hex::encode(fetched.sha256)}),
            )
            .await;
        }
        kb_core::source_sync::RefetchAction::Changed => {
            reingest_changed(
                job,
                &doc,
                pipeline,
                pg,
                diff_gen,
                fetched,
                effective_local_only,
                max_fetch_interval,
            )
            .await?;
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn reingest_changed(
    job: &Job,
    doc: &Document,
    pipeline: &IngestPipeline,
    pg: &PgStore,
    diff_gen: &DiffSummaryGenerator,
    fetched: FetchedContent,
    local_only: bool,
    max_fetch_interval: i64,
) -> anyhow::Result<()> {
    let tenant_id = job.tenant_id;
    let doc_id = doc.id;

    // Snapshot old text for the diff (best-effort).
    let old_text = pg
        .get_live_document_text(tenant_id, doc_id)
        .await
        .unwrap_or_default();

    // Derive filename from final URL.
    let filename = url_filename(&fetched.final_url);

    // Build IngestFile from fetched bytes.
    let ingest_file = IngestFile {
        bytes: fetched.bytes.clone(),
        page_label: Some(filename.clone()),
        path: Some(filename),
    };

    // Run full ingest pipeline into the existing document.
    pipeline
        .ingest_into(
            tenant_id,
            job.created_by,
            doc_id,
            vec![ingest_file],
            doc.user_note.clone(),
            local_only,
        )
        .await
        .context("re-ingest pipeline failed")?;

    // Reload the document to get new title/summary.
    let new_doc = pg
        .get_document(tenant_id, doc_id)
        .await?
        .context("document vanished after re-ingest")?;

    // New text for diff (best-effort).
    let new_text = pg
        .get_live_document_text(tenant_id, doc_id)
        .await
        .unwrap_or_default();

    let diff_summary = diff_gen
        .generate(
            &old_text,
            &new_text,
            local_only,
            0,
            tenant_id,
            job.created_by,
        )
        .await;

    // Insert version snapshot.
    let sha = kb_core::hash::Sha256::from_bytes(fetched.sha256);
    pg.insert_document_version_and_advance(
        tenant_id,
        doc_id,
        Some(&sha),
        new_doc.title.as_deref(),
        new_doc.summary.as_deref(),
        Some(&diff_summary),
    )
    .await?;

    // Schedule next fetch.
    let interval = new_doc
        .fetch_interval_secs
        .unwrap_or(max_fetch_interval.min(86_400));
    let clamped = interval.clamp(300, max_fetch_interval);
    let next = compute_next_fetch_at(Utc::now(), Some(clamped)).context("compute next_fetch_at")?;
    pg.set_document_next_fetch_at(tenant_id, doc_id, next)
        .await?;

    // Audit success.
    emit_audit(
        pg,
        job.created_by,
        tenant_id,
        AuditAction::DocumentRefetched,
        doc_id,
        &serde_json::json!({
            "old_version": doc.current_version,
            "new_version": new_doc.current_version,
            "sha256": hex::encode(fetched.sha256),
            "final_url": fetched.final_url,
        }),
    )
    .await;

    Ok(())
}

async fn record_failure(
    pg: &PgStore,
    tenant_id: i64,
    doc_id: i64,
    min_fetch_interval: i64,
    max_backoff_secs: i64,
) -> anyhow::Result<()> {
    pg.mark_document_fetch_failed(tenant_id, doc_id, min_fetch_interval, max_backoff_secs)
        .await
}

async fn emit_audit(
    pg: &PgStore,
    actor: Option<i64>,
    tenant_id: i64,
    action: AuditAction,
    doc_id: i64,
    details: &serde_json::Value,
) {
    let user_id = actor.unwrap_or(0);
    if let Err(e) = pg
        .admin_insert_audit_event(
            user_id,
            tenant_id,
            action.as_str(),
            "document",
            Some(doc_id),
            details,
        )
        .await
    {
        tracing::warn!(error=%e, tenant_id, doc_id, action=%action.as_str(),
            "refetch worker: audit event failed");
    }
}

fn url_filename(url: &str) -> String {
    url.split('/')
        .next_back()
        .unwrap_or("")
        .split('?')
        .next()
        .unwrap_or("")
        .to_string()
}
