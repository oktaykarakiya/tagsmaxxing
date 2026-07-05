// SPDX-License-Identifier: AGPL-3.0-or-later

//! An immutable version snapshot of a document — one row in `document_versions`.
//!
//! Each time a source-synced document is re-fetched and its content changes, a new
//! version row is inserted capturing the AI-derived outputs (title, summary, tags)
//! at that point in time plus an LLM-generated human-readable change summary.
//! Rows are insert-only — they are never updated or deleted.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::hash::Sha256;

/// A single tag entry as recorded in `tags_snapshot` — denormalized from the
/// `document_tags` → `tags` join at version-creation time so the snapshot is
/// self-contained and independent of future tag renames/merges/deletions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TagSnapshotEntry {
    /// Canonical tag name.
    pub name: String,
    /// Who assigned this tag (`"llm"` or `"user"`).
    pub source: String,
    /// Whether the tag was locked by the user at snapshot time.
    pub locked: bool,
}

/// A single immutable row in the `document_versions` table.
///
/// Records the document's AI-derived metadata and a change summary for one
/// version. The raw document content is *not* stored here — it lives in the
/// current `chunks` table (the latest version's chunks) with tombstones
/// on superseded chunks.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DocumentVersion {
    /// Surrogate primary key (`document_versions.id`).
    pub id: i64,
    /// Owning tenant.
    pub tenant_id: i64,
    /// The document this version belongs to.
    pub document_id: i64,
    /// Monotonic version number within the document (1, 2, 3, …).
    pub version_number: i32,
    /// SHA-256 of the fetched content at this version, if the version was
    /// created from a fetch (nullable for attach-time snapshots).
    pub sha256: Option<Sha256>,
    /// LLM-generated title at this version.
    pub title: Option<String>,
    /// LLM-generated summary at this version.
    pub summary: Option<String>,
    /// Snapshot of `(tag_name, source, locked)` triples at version time.
    pub tags_snapshot: Vec<TagSnapshotEntry>,
    /// Number of live chunks at this version.
    pub chunk_count: i32,
    /// LLM-generated human-readable summary of what changed from the
    /// previous version (e.g. "Added section on pricing; removed …
    /// paragraph"). `None` for the initial version or when generation failed.
    pub content_diff_summary: Option<String>,
    /// Row creation time.
    pub created_at: DateTime<Utc>,
}
