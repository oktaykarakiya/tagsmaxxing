//! Processing status shared by documents and files (the `status` column).

use crate::macros::str_enum;

str_enum! {
    /// Lifecycle of a document/file as it moves through ingestion. Distinct from
    /// [`JobStatus`](crate::job::JobStatus), which tracks the queue row that *does* the work.
    pub enum ProcessingStatus {
        /// Submitted, not yet fully ingested.
        Pending = "pending",
        /// Fully extracted, tagged, embedded, and queryable.
        Ready = "ready",
        /// Ingestion failed terminally (inspect the associated job's `last_error`).
        Failed = "failed",
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use std::str::FromStr;

    use super::*;

    #[test]
    fn wire_strings_are_locked() {
        assert_eq!(ProcessingStatus::Pending.as_str(), "pending");
        assert_eq!(ProcessingStatus::Ready.as_str(), "ready");
        assert_eq!(ProcessingStatus::Failed.as_str(), "failed");
    }

    #[test]
    fn parses_every_variant() {
        for s in ProcessingStatus::all() {
            assert_eq!(ProcessingStatus::from_str(s.as_str()).unwrap(), *s);
        }
    }
}
