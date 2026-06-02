//! The durable ingestion job record and its enums (plan §5, §16).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::macros::str_enum;

str_enum! {
    /// What a queued job does.
    pub enum JobKind {
        /// Ingest a submitted document (extract -> tag -> embed -> store).
        Ingest = "ingest",
        /// Re-embed chunks after an embedder change (plan §5 lock-in note).
        Reembed = "reembed",
        /// Stream a tenant's data out as a ZIP (plan §13).
        Export = "export",
        /// Re-run the tagger + canonicalizer on a document's current text,
        /// merging results while preserving locked user-assigned tags
        /// (plan §6.5, P6-T11).
        Retag = "retag",
        /// Restore the latest pgBackRest backup to a scratch Postgres instance and
        /// run integrity checks to verify the backup is viable (plan §21, P8-T7).
        RestoreTest = "restore_test",
        /// Find and delete B2 blobs with no corresponding DB row (orphaned by
        /// failed dual-writes), and log DB rows whose blob is missing from B2
        /// as data-loss events (plan §23, P8-T10).
        OrphanGc = "orphan_gc",
        /// Periodically re-hash a random sample of B2 blobs and compare to the
        /// stored SHA-256 to detect bit-rot or tampering (plan §23, P8-T10).
        IntegrityScan = "integrity_scan",
        /// Crypto-shred a tenant: revoke sessions, delete DB rows, delete B2 blobs,
        /// destroy the DEK, and create a tombstone record (plan §28, P10-T4).
        DeleteTenant = "delete_tenant",
        /// Send a transactional email via the configured email provider (P12-T7).
        /// Payload is a JSON-serialized [`EmailJobPayload`] stored in `last_error`
        /// (reused as a payload column for this job kind).
        SendEmail = "send_email",
    }
}

str_enum! {
    /// Lifecycle of a queue row. Distinct from
    /// [`ProcessingStatus`](crate::status::ProcessingStatus) (the document/file state).
    pub enum JobStatus {
        /// Waiting to be claimed.
        Queued = "queued",
        /// Claimed by a worker and in progress.
        Running = "running",
        /// Failed an attempt; eligible for retry after `run_after`.
        Failed = "failed",
        /// Exhausted retries — parked in the dead-letter for inspection/replay.
        Dead = "dead",
        /// Completed successfully.
        Done = "done",
    }
}

/// A durable, retryable ingestion job (the `jobs` table). Workers claim rows with
/// `SELECT … FOR UPDATE SKIP LOCKED`; `run_after` implements exponential backoff (plan §16).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Job {
    /// Surrogate primary key (`jobs.id`).
    pub id: i64,
    /// Owning tenant.
    pub tenant_id: i64,
    /// Target file, if the job is file-scoped (ingest, reembed).
    pub file_id: Option<i64>,
    /// Target document, if the job is document-scoped (retag).
    pub document_id: Option<i64>,
    /// What the job does.
    pub kind: JobKind,
    /// Higher runs first (P9-T12); default 0. Interactive queries bypass the queue
    /// entirely (plan §16). The claim query adds an age-based boost via
    /// `effective_priority = priority + floor(age_seconds / 3600)` to prevent
    /// starvation of old low-priority jobs.
    pub priority: i32,
    /// Current status.
    pub status: JobStatus,
    /// Number of attempts made so far.
    pub attempts: i32,
    /// Last error message, if the most recent attempt failed.
    pub last_error: Option<String>,
    /// Earliest time the job may next run (backoff gate).
    pub run_after: DateTime<Utc>,
    /// Row creation time.
    pub created_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use std::str::FromStr;

    use super::*;

    #[test]
    fn kind_wire_strings_are_locked() {
        assert_eq!(JobKind::Ingest.as_str(), "ingest");
        assert_eq!(JobKind::Reembed.as_str(), "reembed");
        assert_eq!(JobKind::Export.as_str(), "export");
        assert_eq!(JobKind::Retag.as_str(), "retag");
        assert_eq!(JobKind::RestoreTest.as_str(), "restore_test");
        assert_eq!(JobKind::OrphanGc.as_str(), "orphan_gc");
        assert_eq!(JobKind::IntegrityScan.as_str(), "integrity_scan");
        assert_eq!(JobKind::DeleteTenant.as_str(), "delete_tenant");
        assert_eq!(JobKind::all().len(), 9);
    }

    #[test]
    fn status_wire_strings_are_locked() {
        assert_eq!(JobStatus::Queued.as_str(), "queued");
        assert_eq!(JobStatus::Dead.as_str(), "dead");
        assert_eq!(JobStatus::Done.as_str(), "done");
        assert_eq!(JobStatus::all().len(), 5);
    }

    #[test]
    fn enums_parse_every_variant() {
        for k in JobKind::all() {
            assert_eq!(JobKind::from_str(k.as_str()).unwrap(), *k);
        }
        for s in JobStatus::all() {
            assert_eq!(JobStatus::from_str(s.as_str()).unwrap(), *s);
        }
    }
}
