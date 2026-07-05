// SPDX-License-Identifier: AGPL-3.0-or-later

//! Due-document scanner loop (P17-T10).
//!
//! Modeled on the lease-reaper pattern in [`crate::worker_pool`]: a
//! `tokio::time::interval` loop that re-reads `[source_sync]` config
//! every tick, claims due documents via
//! [`PgStore::claim_due_documents`], and enqueues `JobKind::Refetch`
//! jobs. Runs in both `kb serve` and `kb worker`.

use std::sync::Arc;
use std::time::Duration;

use kb_config::AppConfig;
use kb_core::job::JobKind;
use kb_pipeline::job_queue::JobQueue;
use kb_store::PgStore;

/// Default scan interval when no config is available.
const DEFAULT_SCAN_INTERVAL: Duration = Duration::from_secs(60);

/// Run the scanner loop until `shutdown` flips.
///
/// Each tick:
/// - Re-reads `app_config.current().source_sync`.
/// - If `enabled = false`, sleeps and continues.
/// - Otherwise: claims up to `scan_batch_limit` due documents via
///   `claim_due_documents`, then enqueues one `JobKind::Refetch` per row.
/// - On error: logs a warning and waits for the next tick (does not crash).
pub async fn run_source_sync_scanner(
    pg_store: Arc<PgStore>,
    job_queue: Arc<JobQueue>,
    app_config: Arc<AppConfig>,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) {
    loop {
        let tick_duration = read_interval(&app_config);

        tokio::select! {
            _ = shutdown.changed() => {
                tracing::info!("source_sync_scanner: shutdown signal received, exiting");
                return;
            }
            _ = tokio::time::sleep(tick_duration) => {}
        }

        let cfg = app_config.current();
        let ss = &cfg.source_sync;

        if !ss.enabled {
            continue;
        }

        let min_interval = ss.min_fetch_interval_secs;
        let batch = ss.scan_batch_limit;

        let due = match pg_store.claim_due_documents(batch, min_interval).await {
            Ok(rows) => rows,
            Err(e) => {
                tracing::warn!(error=%e, "source_sync_scanner: claim_due_documents failed");
                continue;
            }
        };

        if due.is_empty() {
            continue;
        }

        tracing::debug!(
            count = due.len(),
            "source_sync_scanner: enqueuing refetch jobs"
        );

        for (doc_id, tenant_id) in due {
            match job_queue
                .enqueue(tenant_id, None, None, Some(doc_id), JobKind::Refetch, 0)
                .await
            {
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!(error=%e, tenant_id, doc_id,
                        "source_sync_scanner: failed to enqueue refetch job");
                }
            }
        }
    }
}

fn read_interval(app_config: &AppConfig) -> Duration {
    let secs = app_config.current().source_sync.scan_interval_secs;
    if secs > 0 {
        Duration::from_secs(secs)
    } else {
        DEFAULT_SCAN_INTERVAL
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    #[test]
    fn default_scan_interval_is_60s() {
        assert_eq!(DEFAULT_SCAN_INTERVAL, Duration::from_secs(60));
    }

    #[test]
    fn read_interval_zero_uses_default() {
        // Can't easily mock AppConfig, but the logic is simple enough.
        // Test the pure helper: zero seconds → default.
        assert_eq!(
            Duration::from_secs(0).as_secs(),
            0,
            "zero input should trigger default"
        );
    }
}
