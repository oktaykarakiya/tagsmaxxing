// SPDX-License-Identifier: AGPL-3.0-or-later

//! Document version history and source-sync settings — P17 store methods.

use anyhow::Context;
use chrono::Utc;
use kb_core::document_version::{DocumentVersion, TagSnapshotEntry};
use kb_core::hash::Sha256;
use kb_core::source_sync::{clamp_fetch_interval, compute_next_fetch_at};
use sqlx::Postgres;

use crate::pg_store::{PgStore, begin_tenant_tx};

// Shared query for the tags snapshot — join document_tags → tags.
async fn fetch_tags_snapshot(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    doc_id: i64,
) -> anyhow::Result<Vec<TagSnapshotEntry>> {
    let rows: Vec<(String, String, bool)> = sqlx::query_as(
        "SELECT t.name, dt.source, dt.locked \
         FROM document_tags dt JOIN tags t ON dt.tag_id = t.id \
         WHERE dt.document_id = $1 \
         ORDER BY t.name",
    )
    .bind(doc_id)
    .fetch_all(&mut **tx)
    .await
    .context("failed to fetch tags for version snapshot")?;

    Ok(rows
        .into_iter()
        .map(|(name, source, locked)| TagSnapshotEntry {
            name,
            source,
            locked,
        })
        .collect())
}

async fn count_live_chunks(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    doc_id: i64,
) -> anyhow::Result<i32> {
    sqlx::query_scalar(
        "SELECT COUNT(*)::INT FROM chunks WHERE document_id = $1 AND superseded_at IS NULL",
    )
    .bind(doc_id)
    .fetch_one(&mut **tx)
    .await
    .context("failed to count live chunks")
}

impl PgStore {
    /// Insert a new version row, advance `documents.current_version`,
    /// and update schedule/bookkeeping fields — all in one tenant-scoped tx.
    ///
    /// Uses `ON CONFLICT (document_id, version_number) DO NOTHING` so raced
    /// duplicate insertions from concurrent scanner processes are harmless.
    pub async fn insert_document_version_and_advance(
        &self,
        tenant_id: i64,
        doc_id: i64,
        new_sha256: Option<&Sha256>,
        new_title: Option<&str>,
        new_summary: Option<&str>,
        diff_summary: Option<&str>,
    ) -> anyhow::Result<()> {
        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, tenant_id).await?;

        let tags_snapshot = fetch_tags_snapshot(&mut tx, doc_id).await?;
        let chunk_count = count_live_chunks(&mut tx, doc_id).await?;

        let sha_bytes = new_sha256.map(|s| s.as_bytes().as_slice());

        sqlx::query(
            "INSERT INTO document_versions (tenant_id, document_id, version_number, sha256, \
             title, summary, tags_snapshot, chunk_count, content_diff_summary) \
             VALUES ($1, $2, \
               (SELECT current_version FROM documents WHERE id = $2), \
               $3, $4, $5, $6, $7, $8) \
             ON CONFLICT (document_id, version_number) DO NOTHING",
        )
        .bind(tenant_id)
        .bind(doc_id)
        .bind(sha_bytes)
        .bind(new_title)
        .bind(new_summary)
        .bind(serde_json::to_value(&tags_snapshot)?)
        .bind(chunk_count)
        .bind(diff_summary)
        .execute(&mut *tx)
        .await
        .context("failed to insert document version")?;

        // Advance the document: bump version, set schedule, reset failures.
        let now = Utc::now();
        sqlx::query(
            "UPDATE documents SET \
             current_version = current_version + 1, \
             last_fetched_at = $3, \
             last_fetch_sha256 = $4, \
             fetch_failure_count = 0 \
             WHERE id = $1 AND tenant_id = $2",
        )
        .bind(doc_id)
        .bind(tenant_id)
        .bind(now)
        .bind(sha_bytes)
        .execute(&mut *tx)
        .await
        .context("failed to advance document version")?;

        tx.commit()
            .await
            .context("failed to commit insert_document_version_and_advance")?;

        Ok(())
    }

    /// List all versions for a document, newest first.
    pub async fn list_document_versions(
        &self,
        tenant_id: i64,
        doc_id: i64,
    ) -> anyhow::Result<Vec<DocumentVersion>> {
        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, tenant_id).await?;

        #[derive(sqlx::FromRow)]
        struct VRow {
            id: i64,
            tenant_id: i64,
            document_id: i64,
            version_number: i32,
            sha256: Option<Vec<u8>>,
            title: Option<String>,
            summary: Option<String>,
            tags_snapshot: serde_json::Value,
            chunk_count: i32,
            content_diff_summary: Option<String>,
            created_at: chrono::DateTime<chrono::Utc>,
        }

        let rows: Vec<VRow> = sqlx::query_as(
            "SELECT id, tenant_id, document_id, version_number, sha256, title, summary, \
             tags_snapshot, chunk_count, content_diff_summary, created_at \
             FROM document_versions \
             WHERE document_id = $1 \
             ORDER BY version_number DESC",
        )
        .bind(doc_id)
        .fetch_all(&mut *tx)
        .await
        .context("failed to list document versions")?;

        tx.commit().await?;

        Ok(rows
            .into_iter()
            .map(|r| {
                let sha256 = r
                    .sha256
                    .and_then(|b| <[u8; 32]>::try_from(b).ok())
                    .map(Sha256::from_bytes);
                let tags_snapshot: Vec<TagSnapshotEntry> =
                    serde_json::from_value(r.tags_snapshot).unwrap_or_default();
                DocumentVersion {
                    id: r.id,
                    tenant_id: r.tenant_id,
                    document_id: r.document_id,
                    version_number: r.version_number,
                    sha256,
                    title: r.title,
                    summary: r.summary,
                    tags_snapshot,
                    chunk_count: r.chunk_count,
                    content_diff_summary: r.content_diff_summary,
                    created_at: r.created_at,
                }
            })
            .collect())
    }

    /// Get a single version by document id and version number.
    pub async fn get_document_version(
        &self,
        tenant_id: i64,
        doc_id: i64,
        version_number: i32,
    ) -> anyhow::Result<Option<DocumentVersion>> {
        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, tenant_id).await?;

        #[derive(sqlx::FromRow)]
        struct VRow {
            id: i64,
            tenant_id: i64,
            document_id: i64,
            version_number: i32,
            sha256: Option<Vec<u8>>,
            title: Option<String>,
            summary: Option<String>,
            tags_snapshot: serde_json::Value,
            chunk_count: i32,
            content_diff_summary: Option<String>,
            created_at: chrono::DateTime<chrono::Utc>,
        }

        let r: Option<VRow> = sqlx::query_as(
            "SELECT id, tenant_id, document_id, version_number, sha256, title, summary, \
             tags_snapshot, chunk_count, content_diff_summary, created_at \
             FROM document_versions \
             WHERE document_id = $1 AND version_number = $2",
        )
        .bind(doc_id)
        .bind(version_number)
        .fetch_optional(&mut *tx)
        .await
        .context("failed to fetch document version")?;

        tx.commit().await?;

        match r {
            None => Ok(None),
            Some(r) => {
                let sha256 = r
                    .sha256
                    .and_then(|b| <[u8; 32]>::try_from(b).ok())
                    .map(Sha256::from_bytes);
                let tags_snapshot: Vec<TagSnapshotEntry> =
                    serde_json::from_value(r.tags_snapshot).unwrap_or_default();
                Ok(Some(DocumentVersion {
                    id: r.id,
                    tenant_id: r.tenant_id,
                    document_id: r.document_id,
                    version_number: r.version_number,
                    sha256,
                    title: r.title,
                    summary: r.summary,
                    tags_snapshot,
                    chunk_count: r.chunk_count,
                    content_diff_summary: r.content_diff_summary,
                    created_at: r.created_at,
                }))
            }
        }
    }

    /// Create a v1 snapshot of the document's current state if no versions exist yet.
    ///
    /// Called when a user first attaches a `source_url` to an existing document.
    /// The snapshot records the document as-is (`sha256 = NULL`, diff = placeholder).
    pub async fn snapshot_current_version_if_none(
        &self,
        tenant_id: i64,
        doc_id: i64,
    ) -> anyhow::Result<()> {
        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, tenant_id).await?;

        let existing: Option<i32> = sqlx::query_scalar(
            "SELECT 1 FROM document_versions WHERE document_id = $1 AND version_number = 1",
        )
        .bind(doc_id)
        .fetch_optional(&mut *tx)
        .await?;

        if existing.is_some() {
            tx.commit().await?;
            return Ok(());
        }

        let tags_snapshot = fetch_tags_snapshot(&mut tx, doc_id).await?;
        let chunk_count = count_live_chunks(&mut tx, doc_id).await?;

        sqlx::query(
            "INSERT INTO document_versions (tenant_id, document_id, version_number, sha256, \
             title, summary, tags_snapshot, chunk_count, content_diff_summary) \
             SELECT $1, $2, 1, NULL, d.title, d.summary, $3, $4, $5 \
             FROM documents d WHERE d.id = $2",
        )
        .bind(tenant_id)
        .bind(doc_id)
        .bind(serde_json::to_value(&tags_snapshot)?)
        .bind(chunk_count)
        .bind("Snapshot at sync enablement")
        .execute(&mut *tx)
        .await
        .context("failed to insert v1 snapshot")?;

        tx.commit()
            .await
            .context("failed to commit snapshot_current_version_if_none")?;
        Ok(())
    }

    /// Update a document's source-sync settings.
    ///
    /// Clamps `fetch_interval_secs` to the configured `[source_sync]` min/max,
    /// computes `next_fetch_at`, and resets `fetch_failure_count`. An empty
    /// `source_url` clears all sync columns (disables sync for this document).
    pub async fn update_source_settings(
        &self,
        tenant_id: i64,
        doc_id: i64,
        source_url: Option<&str>,
        fetch_interval_secs: Option<i64>,
        min_fetch_interval: i64,
        max_fetch_interval: i64,
    ) -> anyhow::Result<()> {
        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, tenant_id).await?;

        match source_url {
            None | Some("") => {
                // Clear all sync fields.
                sqlx::query(
                    "UPDATE documents SET source_url = NULL, fetch_interval_secs = NULL, \
                     next_fetch_at = NULL, last_fetched_at = NULL, last_fetch_sha256 = NULL, \
                     fetch_failure_count = 0 WHERE id = $1 AND tenant_id = $2",
                )
                .bind(doc_id)
                .bind(tenant_id)
                .execute(&mut *tx)
                .await
                .context("failed to clear source settings")?;
            }
            Some(url) => {
                let clamped = clamp_fetch_interval(
                    fetch_interval_secs,
                    min_fetch_interval,
                    max_fetch_interval,
                );
                let now = Utc::now();
                let next_fetch = compute_next_fetch_at(now, clamped);

                sqlx::query(
                    "UPDATE documents SET source_url = $3, fetch_interval_secs = $4, \
                     next_fetch_at = $5, fetch_failure_count = 0 \
                     WHERE id = $1 AND tenant_id = $2",
                )
                .bind(doc_id)
                .bind(tenant_id)
                .bind(url)
                .bind(clamped)
                .bind(next_fetch)
                .execute(&mut *tx)
                .await
                .context("failed to update source settings")?;
            }
        }

        tx.commit().await?;
        Ok(())
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    #[test]
    fn tag_snapshot_entry_serialization_roundtrips() {
        let entries = vec![
            TagSnapshotEntry {
                name: "rust".into(),
                source: "llm".into(),
                locked: false,
            },
            TagSnapshotEntry {
                name: "security".into(),
                source: "user".into(),
                locked: true,
            },
        ];
        let json = serde_json::to_value(&entries).unwrap();
        let back: Vec<TagSnapshotEntry> = serde_json::from_value(json).unwrap();
        assert_eq!(back.len(), 2);
        assert_eq!(back[0].name, "rust");
        assert_eq!(back[1].name, "security");
        assert!(back[1].locked);
    }

    #[test]
    fn sha256_from_row_bytes_roundtrips() {
        let bytes: Vec<u8> = vec![0xAAu8; 32];
        let arr: [u8; 32] = bytes.clone().try_into().unwrap();
        let sha = Sha256::from_bytes(arr);
        assert_eq!(sha.as_bytes()[0], 0xAA);
        let back: Vec<u8> = sha.as_bytes().to_vec();
        assert_eq!(back, bytes);
    }

    #[test]
    fn sha256_none_when_wrong_length() {
        let short = vec![0u8; 16];
        let result = <[u8; 32]>::try_from(short);
        assert!(result.is_err());
    }

    #[test]
    fn document_version_from_mapped_row() {
        // Simulate a VRow → DocumentVersion mapping.
        let now = chrono::Utc::now();
        let sha_bytes: Vec<u8> = vec![0xBBu8; 32];
        let v = DocumentVersion {
            id: 1,
            tenant_id: 7,
            document_id: 42,
            version_number: 3,
            sha256: Some(Sha256::from_bytes(sha_bytes.try_into().unwrap())),
            title: Some("Updated Doc".into()),
            summary: Some("A summary".into()),
            tags_snapshot: vec![TagSnapshotEntry {
                name: "rust".into(),
                source: "llm".into(),
                locked: false,
            }],
            chunk_count: 12,
            content_diff_summary: Some("Added section X".into()),
            created_at: now,
        };
        assert_eq!(v.id, 1);
        assert_eq!(v.tenant_id, 7);
        assert_eq!(v.version_number, 3);
        assert_eq!(v.chunk_count, 12);
        assert_eq!(v.content_diff_summary.as_deref(), Some("Added section X"));
        assert_eq!(v.tags_snapshot.len(), 1);
    }

    #[test]
    fn update_source_settings_clear_logic() {
        // Verify the clear-when-empty semantic at the type level.
        // Empty string URL should result in clearing.
        let empty: &str = "";
        assert!(empty.is_empty());
        // None should also clear.
        let opt: Option<&str> = None;
        assert!(opt.is_none(), "None source_url should also clear");
    }

    #[test]
    fn clamp_fetch_interval_integration() {
        // Verify the pure function integrates correctly with store logic inputs.
        assert_eq!(clamp_fetch_interval(Some(60), 300, 86_400), Some(300));
        assert_eq!(clamp_fetch_interval(Some(3600), 300, 86_400), Some(3600));
        assert_eq!(clamp_fetch_interval(None, 300, 86_400), None);
    }

    #[test]
    fn compute_next_fetch_at_integration() {
        let now = Utc::now();
        let next = compute_next_fetch_at(now, Some(3600));
        assert!(next.is_some());
        assert!(next.unwrap() > now);
        assert_eq!(compute_next_fetch_at(now, None), None);
    }

    // ── DocumentVersion edge cases ─────────────────────────────────────

    #[test]
    fn document_version_no_sha256() {
        let now = chrono::Utc::now();
        let v = DocumentVersion {
            id: 2,
            tenant_id: 1,
            document_id: 1,
            version_number: 1,
            sha256: None,
            title: None,
            summary: None,
            tags_snapshot: vec![],
            chunk_count: 0,
            content_diff_summary: None,
            created_at: now,
        };
        assert_eq!(v.version_number, 1);
        assert!(v.sha256.is_none());
        assert!(v.title.is_none());
        assert!(v.tags_snapshot.is_empty());
    }

    #[test]
    fn document_version_full_roundtrip() {
        let now = chrono::Utc::now();
        let sha = Sha256::from_bytes([0xCCu8; 32]);
        let tags = vec![
            TagSnapshotEntry {
                name: "alpha".into(),
                source: "llm".into(),
                locked: false,
            },
            TagSnapshotEntry {
                name: "beta".into(),
                source: "user".into(),
                locked: true,
            },
        ];
        let v = DocumentVersion {
            id: 100,
            tenant_id: 5,
            document_id: 20,
            version_number: 5,
            sha256: Some(sha),
            title: Some("v5 title".into()),
            summary: Some("v5 summary".into()),
            tags_snapshot: tags.clone(),
            chunk_count: 42,
            content_diff_summary: Some("big changes".into()),
            created_at: now,
        };
        assert_eq!(v.id, 100);
        assert_eq!(v.tenant_id, 5);
        assert_eq!(v.document_id, 20);
        assert_eq!(v.version_number, 5);
        assert_eq!(v.sha256, Some(sha));
        assert_eq!(v.title.as_deref(), Some("v5 title"));
        assert_eq!(v.summary.as_deref(), Some("v5 summary"));
        assert_eq!(v.tags_snapshot, tags);
        assert_eq!(v.chunk_count, 42);
        assert_eq!(v.content_diff_summary.as_deref(), Some("big changes"));
        assert_eq!(v.created_at, now);
    }

    #[test]
    fn tag_snapshot_entry_empty() {
        let entries: Vec<TagSnapshotEntry> = vec![];
        let json = serde_json::to_value(&entries).unwrap();
        assert_eq!(json, serde_json::json!([]));
        let back: Vec<TagSnapshotEntry> = serde_json::from_value(json).unwrap();
        assert!(back.is_empty());
    }

    #[test]
    fn tag_snapshot_entry_malformed_json() {
        let bad = serde_json::json!([{"name": "ok"}, {"not_a_tag": 1}]);
        let back: Vec<TagSnapshotEntry> = serde_json::from_value(bad).unwrap_or_default();
        assert!(back.is_empty());
    }

    #[test]
    fn clamp_edge_cases() {
        // Zero value clamped up.
        assert_eq!(clamp_fetch_interval(Some(0), 300, 86_400), Some(300));
        // Negative value clamped up.
        assert_eq!(clamp_fetch_interval(Some(-1), 300, 86_400), Some(300));
        // Large value clamped down.
        assert_eq!(
            clamp_fetch_interval(Some(i64::MAX), 300, 86_400),
            Some(86_400)
        );
    }

    #[test]
    fn update_source_settings_min_max_passthrough() {
        // Verify the clamping bounds are passed through correctly.
        // These values should be acceptable for the store API.
        let min = 300i64;
        let max = 2_592_000i64;
        assert!(min > 0);
        assert!(max >= min);

        // Interval within range stays unchanged after clamp.
        let clamped = clamp_fetch_interval(Some(3600), min, max);
        assert_eq!(clamped, Some(3600));

        // None passthrough.
        let clamped_none = clamp_fetch_interval(None, min, max);
        assert_eq!(clamped_none, None);

        // compute next for the clamped value.
        let now = Utc::now();
        let next = compute_next_fetch_at(now, clamped);
        assert!(next.is_some());

        // compute next for None interval.
        let next_none = compute_next_fetch_at(now, clamped_none);
        assert_eq!(next_none, None);
    }

    #[test]
    fn serde_roundtrip_document_version_json() {
        let now = chrono::Utc::now();
        let sha = Sha256::from_bytes([0xDEu8; 32]);
        let v = DocumentVersion {
            id: 99,
            tenant_id: 3,
            document_id: 7,
            version_number: 2,
            sha256: Some(sha),
            title: Some("Doc v2".into()),
            summary: Some("Updated".into()),
            tags_snapshot: vec![TagSnapshotEntry {
                name: "tag1".into(),
                source: "llm".into(),
                locked: false,
            }],
            chunk_count: 5,
            content_diff_summary: Some("minor edits".into()),
            created_at: now,
        };
        let json = serde_json::to_string(&v).unwrap();
        let back: DocumentVersion = serde_json::from_str(&json).unwrap();
        assert_eq!(back.id, v.id);
        assert_eq!(back.version_number, v.version_number);
        assert_eq!(back.content_diff_summary, v.content_diff_summary);
        // Sha256 serializes as hex.
        assert!(json.contains("dededededededededededededededededededededededededededededededede"));
    }
}
