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
    /// Target file, if the job is file-scoped.
    pub file_id: Option<i64>,
    /// What the job does.
    pub kind: JobKind,
    /// Lower runs first; interactive queries bypass the queue entirely (plan §16).
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
