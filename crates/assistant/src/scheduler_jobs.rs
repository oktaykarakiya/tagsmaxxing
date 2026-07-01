// SPDX-License-Identifier: AGPL-3.0-or-later

//! Scheduled background jobs for the assistant.
//!
//! Plugs into `kb_pipeline::job_queue` with three sentinel jobs:
//! - memory-consolidation (daily 3 AM): summarize recent sessions → ingest into KB
//! - memory-pruning (weekly Sun 4 AM): archive old transcripts
//! - stale-watch-check (daily 6 AM): check document freshness watches

/// Sentinel job names registered at startup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SentinelJob {
    /// Daily memory consolidation.
    MemoryConsolidation,
    /// Weekly memory pruning.
    MemoryPruning,
    /// Daily stale-watch check.
    StaleWatchCheck,
}

impl SentinelJob {
    /// Job name for the queue.
    pub fn name(self) -> &'static str {
        match self {
            Self::MemoryConsolidation => "assistant-memory-consolidation",
            Self::MemoryPruning => "assistant-memory-pruning",
            Self::StaleWatchCheck => "assistant-stale-watch-check",
        }
    }

    /// Cron expression.
    pub fn cron(self) -> &'static str {
        match self {
            Self::MemoryConsolidation => "0 3 * * *", // daily at 3 AM
            Self::MemoryPruning => "0 4 * * 0",       // weekly Sunday at 4 AM
            Self::StaleWatchCheck => "0 6 * * *",     // daily at 6 AM
        }
    }

    /// All sentinel jobs.
    pub fn all() -> &'static [SentinelJob] {
        &[
            Self::MemoryConsolidation,
            Self::MemoryPruning,
            Self::StaleWatchCheck,
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn sentinel_job_names_are_unique() {
        let names: Vec<_> = SentinelJob::all().iter().map(|j| j.name()).collect();
        let unique: HashSet<_> = names.iter().collect();
        assert_eq!(unique.len(), names.len());
    }

    #[test]
    fn sentinel_cron_expressions_parse() {
        for job in SentinelJob::all() {
            let cron = job.cron();
            let fields: Vec<_> = cron.split_whitespace().collect();
            assert_eq!(
                fields.len(),
                5,
                "cron expr for {:?} should have 5 fields, got {}: {:?}",
                job,
                fields.len(),
                cron
            );
        }
    }

    #[test]
    fn sentinel_jobs_count() {
        assert_eq!(SentinelJob::all().len(), 3);
    }
}
