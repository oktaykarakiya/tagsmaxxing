//! The file record — one physical blob = one page/member of a document (plan §5).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::hash::Sha256;
use crate::status::ProcessingStatus;

/// A physical file: a single content-addressed blob that is one ordered page/member of a
/// [`Document`](crate::document::Document). Deduplicated per tenant on `sha256`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FileRecord {
    /// Surrogate primary key (`files.id`).
    pub id: i64,
    /// Owning tenant.
    pub tenant_id: i64,
    /// Parent document.
    pub document_id: i64,
    /// 1-based order within the document.
    pub page_no: i32,
    /// Human label for the page (`front`/`back`/`p3`/original filename).
    pub page_label: Option<String>,
    /// SHA-256 of the blob contents (per-tenant dedup key).
    pub sha256: Sha256,
    /// Content-addressed, tenant-prefixed object-store key (= `hex(sha256)`).
    pub blob_key: String,
    /// Original filename/path, if known.
    pub path: Option<String>,
    /// MIME type, if detected.
    pub mime: Option<String>,
    /// Size of the blob in bytes.
    pub size_bytes: Option<i64>,
    /// Raw, namespaced per-page metadata (exif/ffprobe/doc props) as JSONB.
    pub meta: serde_json::Value,
    /// Processing lifecycle status.
    pub status: ProcessingStatus,
    /// Ingestion time.
    pub ingested_at: DateTime<Utc>,
}
