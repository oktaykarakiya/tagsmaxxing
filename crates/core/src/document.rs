// SPDX-License-Identifier: AGPL-3.0-or-later

//! The document record — the semantic unit a user retrieves (plan §5, §27).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::kind::DocKind;
use crate::status::ProcessingStatus;

/// A document: the unit the user searches for, composed of one or more member
/// [`FileRecord`](crate::file::FileRecord) pages (e.g. front+back of an ID, pages of a PDF).
/// A single-file upload auto-creates a one-page document (plan §27).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Document {
    /// Surrogate primary key (`documents.id`).
    pub id: i64,
    /// Owning tenant.
    pub tenant_id: i64,
    /// LLM-generated title (may be `None` before tagging).
    pub title: Option<String>,
    /// LLM-generated summary synthesized across *all* pages.
    pub summary: Option<String>,
    /// The user's free-text note/description (typed or voice-transcribed); drives tagging.
    pub user_note: Option<String>,
    /// Document kind.
    pub kind: DocKind,
    /// Union of member metadata plus document-level fields (`documents.meta` JSONB).
    pub meta: serde_json::Value,
    /// Number of member pages/files.
    pub page_count: i32,
    /// Processing lifecycle status.
    pub status: ProcessingStatus,
    /// Row creation time.
    pub created_at: DateTime<Utc>,
    /// If `true`, this document's content must never be sent to a remote
    /// provider — even if the tenant allows remote. Set automatically for
    /// `identity_document` kind (plan §17) and optionally by the user.
    /// Enforced at the Pool::acquire level (plan §26.6, P9-T9).
    pub local_only: bool,
    /// Optional URL this document was ingested from / should be synced from
    /// (P17 source-sync feature).
    pub source_url: Option<String>,
    /// How often to re-fetch `source_url` in seconds. `None` = never auto-refresh.
    pub fetch_interval_secs: Option<i64>,
    /// When the next re-fetch is scheduled. `None` = not scheduled / paused.
    pub next_fetch_at: Option<DateTime<Utc>>,
    /// When `source_url` was last successfully fetched.
    pub last_fetched_at: Option<DateTime<Utc>>,
    /// SHA-256 of the last successfully fetched body — the compare target
    /// for change detection on the next re-fetch.
    pub last_fetch_sha256: Option<Vec<u8>>,
    /// Monotonic version counter (1 = initial, bumped on each changed re-fetch).
    pub current_version: i32,
    /// Consecutive fetch failures; used to implement backoff and auto-pause.
    pub fetch_failure_count: i32,
}
