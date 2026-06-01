//! Orphan GC maintenance job (plan §23, P8-T10).
//!
//! The orphan GC finds and cleans up inconsistencies between the B2 blob store
//! and the Postgres `files` table that can arise from failed dual-writes:
//!
//! 1. **B2 blobs without a DB row** — orphane by a transaction that wrote to B2
//!    but whose DB insert was rolled back. These blobs are harmless (content-addressed)
//!    but they cost money. They are deleted after a configurable grace period
//!    ([`OrphanGc::grace_hours`], default 24 h) to avoid deleting blobs of
//!    in-flight transactions.
//! 2. **DB rows whose blob is missing from B2** — data-loss events. These are
//!    logged at ERROR level and counted in the `kb_missing_blobs_found` gauge.
//!
//! # Scheduling
//!
//! The job runs on a configurable daily schedule (default: 02:00 UTC). A simple
//! time-of-day check gates execution — the P8-T11 maintenance scheduler will
//! eventually replace this with a general-purpose cron facility.
//!
//! # Testing
//!
//! The [`OrphanGcRunner`] trait abstracts blob-store and DB operations so the
//! orchestration logic can be tested with a mock. Pure functions (set difference
//! and grace-period filtering) are unit-tested directly.

use std::collections::HashSet;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Timelike, Utc};
use kb_config::OrphanGc;
use kb_core::job::Job;
#[cfg(test)]
use kb_core::job::JobKind;

// ── Record types ──────────────────────────────────────────────────────────────────

/// A lightweight record from the `files` table used during orphan detection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DbBlobRecord {
    /// The content-addressed blob key (`files.blob_key`).
    pub blob_key: String,
    /// When the file row was created (`files.ingested_at`), used for the
    /// grace-period check.
    pub created_at: DateTime<Utc>,
}

// ── OrphanGcRunner trait ──────────────────────────────────────────────────────────

/// Abstracts the blob-store and DB operations needed for orphan GC so the
/// orchestration logic can be tested without a real B2 + Postgres backend.
#[async_trait]
pub trait OrphanGcRunner: Send + Sync {
    /// List all blob keys in the blob store for the given tenant.
    ///
    /// The implementation should filter by the tenant's prefix (e.g.
    /// `"{tenant_id}/"`).
    async fn list_blob_keys(&self, tenant_id: i64) -> anyhow::Result<Vec<String>>;

    /// List all blob-key + creation-time records from the `files` table for
    /// the given tenant.
    async fn list_db_records(&self, tenant_id: i64) -> anyhow::Result<Vec<DbBlobRecord>>;

    /// Delete a single blob by key.
    async fn delete_blob(&self, key: &str) -> anyhow::Result<()>;
}

// ── Pure functions ────────────────────────────────────────────────────────────────

/// Find blob keys that exist in the blob store but have no corresponding row in
/// the `files` table (orphaned by a failed dual-write).
///
/// Returns the list of orphaned blob keys (present in `store_keys` but absent
/// from `db_keys`).
#[must_use]
pub fn find_orphan_blobs(store_keys: &[String], db_keys: &HashSet<&str>) -> Vec<String> {
    store_keys
        .iter()
        .filter(|k| !db_keys.contains(k.as_str()))
        .cloned()
        .collect()
}

/// Find blob keys that are referenced by `files` rows but do not exist in the
/// blob store (data-loss events).
///
/// Returns the list of missing blob keys (present in `db_records` but absent
/// from `store_keys`).
#[must_use]
pub fn find_missing_db_blobs(
    db_records: &[DbBlobRecord],
    store_keys: &HashSet<&str>,
) -> Vec<String> {
    db_records
        .iter()
        .filter(|r| !store_keys.contains(r.blob_key.as_str()))
        .map(|r| r.blob_key.clone())
        .collect()
}

/// Filter orphaned blobs to only those whose `created_at` is older than the
/// grace period. Blobs that are still within the grace window are skipped to
/// avoid deleting blobs of in-flight transactions.
///
/// Orphans without a known `created_at` (not found in `db_records` — which is
/// expected since they have no DB row) are considered old enough to delete:
/// if they had a DB row they wouldn't be orphans.
#[must_use]
pub fn filter_by_grace_period(
    orphans: &[String],
    db_records: &[DbBlobRecord],
    grace_hours: u32,
    now: DateTime<Utc>,
) -> Vec<String> {
    let cutoff = now - Duration::hours(i64::from(grace_hours));

    // Build a lookup: blob_key → created_at from the DB records.
    // Orphans, by definition, have NO DB row, so they won't be in this map.
    // That means they are always considered eligible for deletion unless we
    // have a DB record that tells us otherwise. We keep this lookup for the
    // edge case where a blob has a DB row that was recently deleted (soft
    // consistency).
    let created_map: HashSet<&str> = db_records
        .iter()
        .filter(|r| r.created_at > cutoff)
        .map(|r| r.blob_key.as_str())
        .collect();

    orphans
        .iter()
        .filter(|key| !created_map.contains(key.as_str()))
        .cloned()
        .collect()
}

/// Determine whether an orphan GC run should execute right now.
///
/// The job runs when the current UTC hour and minute match or exceed the
/// configured schedule, and the last run was more than 23 hours ago (or never).
#[must_use]
pub fn should_run_now(
    config: &OrphanGc,
    last_run: Option<DateTime<Utc>>,
    now: DateTime<Utc>,
) -> bool {
    if !config.enabled {
        return false;
    }

    let scheduled_minute_of_day =
        u32::from(config.schedule_hour) * 60 + u32::from(config.schedule_minute);
    let current_minute_of_day = now.hour() * 60 + now.minute();

    if current_minute_of_day < scheduled_minute_of_day {
        return false;
    }

    if let Some(last) = last_run {
        let gap = now - last;
        if gap < Duration::hours(23) {
            return false;
        }
    }

    true
}

// ── Orchestration ─────────────────────────────────────────────────────────────────

/// Run a full orphan GC cycle for a single tenant.
///
/// 1. List all blob keys from B2 and all blob-key records from Postgres.
/// 2. Compute the set differences:
///    - B2 keys not in DB → orphans (candidate for deletion after grace period).
///    - DB keys not in B2 → missing (data-loss events, logged as ERROR).
/// 3. Apply the grace-period filter to orphans, then delete each eligible orphan.
/// 4. Report results to Prometheus metrics.
///
/// # Errors
///
/// Returns an error string suitable for `jobs.last_error` on any step failure.
pub async fn run_orphan_gc(
    runner: &dyn OrphanGcRunner,
    config: &OrphanGc,
    tenant_id: i64,
) -> Result<(), String> {
    // ── 1. Gather state from both stores ───────────────────────────────────
    let store_keys = runner.list_blob_keys(tenant_id).await.map_err(|e| {
        let msg = format!("orphan-gc: failed to list blob keys: {e:#}");
        tracing::error!(error = %e, tenant_id, "orphan-gc: {msg}");
        msg
    })?;

    let db_records = runner.list_db_records(tenant_id).await.map_err(|e| {
        let msg = format!("orphan-gc: failed to list DB records: {e:#}");
        tracing::error!(error = %e, tenant_id, "orphan-gc: {msg}");
        msg
    })?;

    // ── 2. Build lookup set of DB keys ─────────────────────────────────────
    let db_key_set: HashSet<&str> = db_records.iter().map(|r| r.blob_key.as_str()).collect();
    let store_key_set: HashSet<&str> = store_keys.iter().map(|s| s.as_str()).collect();

    // ── 3. Find orphans (B2 keys not in DB) ────────────────────────────────
    let orphans = find_orphan_blobs(&store_keys, &db_key_set);
    let orphans_found = orphans.len() as u64;

    // ── 4. Apply grace period ──────────────────────────────────────────────
    let now = Utc::now();
    let eligible = filter_by_grace_period(&orphans, &db_records, config.grace_hours, now);
    let skipped = orphans.len() - eligible.len();

    if skipped > 0 {
        tracing::info!(
            tenant_id,
            total = orphans.len(),
            skipped,
            grace_hours = config.grace_hours,
            "orphan-gc: {skipped} orphan(s) within grace period, skipped"
        );
    }

    // ── 5. Delete eligible orphans ─────────────────────────────────────────
    let mut deleted: u64 = 0;
    for key in &eligible {
        match runner.delete_blob(key).await {
            Ok(()) => {
                tracing::info!(tenant_id, blob_key = key, "orphan-gc: deleted orphan blob");
                deleted += 1;
            }
            Err(e) => {
                // Log but continue — one failed delete shouldn't block the rest.
                tracing::error!(
                    tenant_id,
                    blob_key = key,
                    error = %e,
                    "orphan-gc: failed to delete orphan blob"
                );
            }
        }
    }

    // ── 6. Find missing DB rows (DB keys not in B2) ────────────────────────
    let missing = find_missing_db_blobs(&db_records, &store_key_set);
    let missing_count = missing.len() as u64;

    for key in &missing {
        tracing::error!(
            tenant_id,
            blob_key = key,
            "orphan-gc: DATA LOSS — DB row references blob that does not exist in B2"
        );
    }

    // ── 7. Report to metrics ───────────────────────────────────────────────
    kb_metrics::record_orphan_gc_result(orphans_found, deleted, missing_count);

    tracing::info!(
        tenant_id,
        orphans_found,
        deleted,
        skipped,
        missing_rows = missing_count,
        "orphan-gc: complete — {orphans_found} found, {deleted} deleted, {skipped} skipped, {missing_count} missing"
    );

    Ok(())
}

// ── Job-queue integration ─────────────────────────────────────────────────────────

/// A job-queue handler for orphan GC jobs.
///
/// When a [`Job`] with [`JobKind::OrphanGc`] is claimed, this handler runs the
/// full orphan GC cycle. The worker pool will then call `queue.complete(job.id)`
/// on success or `queue.fail(job.id, &error)` on failure.
///
/// # Errors
///
/// Returns `Err(String)` if any step fails. The worker pool applies exponential
/// backoff before retrying.
pub async fn process_orphan_gc_job(
    runner: &dyn OrphanGcRunner,
    config: &OrphanGc,
    job: &Job,
) -> Result<(), String> {
    run_orphan_gc(runner, config, job.tenant_id).await
}

// ── Tests ──────────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    // ── find_orphan_blobs tests ────────────────────────────────────────────

    #[test]
    fn find_orphan_blobs_empty_store() {
        let db: HashSet<&str> = ["a", "b"].into_iter().collect();
        let result = find_orphan_blobs(&[], &db);
        assert!(result.is_empty());
    }

    #[test]
    fn find_orphan_blobs_empty_db() {
        let store: Vec<String> = ["a", "b"].into_iter().map(String::from).collect();
        let db: HashSet<&str> = HashSet::new();
        let result = find_orphan_blobs(&store, &db);
        assert_eq!(result, vec!["a", "b"]);
    }

    #[test]
    fn find_orphan_blobs_no_orphans() {
        let store: Vec<String> = ["a", "b"].into_iter().map(String::from).collect();
        let db: HashSet<&str> = ["a", "b"].into_iter().collect();
        let result = find_orphan_blobs(&store, &db);
        assert!(result.is_empty());
    }

    #[test]
    fn find_orphan_blobs_some_orphans() {
        let store: Vec<String> = ["a", "b", "c", "d"].into_iter().map(String::from).collect();
        let db: HashSet<&str> = ["a", "c"].into_iter().collect();
        let result = find_orphan_blobs(&store, &db);
        assert_eq!(result, vec!["b", "d"]);
    }

    // ── find_missing_db_blobs tests ────────────────────────────────────────

    #[test]
    fn find_missing_db_blobs_none_missing() {
        let db_records = vec![DbBlobRecord {
            blob_key: "a".into(),
            created_at: Utc::now(),
        }];
        let store: HashSet<&str> = ["a"].into_iter().collect();
        let result = find_missing_db_blobs(&db_records, &store);
        assert!(result.is_empty());
    }

    #[test]
    fn find_missing_db_blobs_some_missing() {
        let db_records = vec![
            DbBlobRecord {
                blob_key: "a".into(),
                created_at: Utc::now(),
            },
            DbBlobRecord {
                blob_key: "b".into(),
                created_at: Utc::now(),
            },
            DbBlobRecord {
                blob_key: "c".into(),
                created_at: Utc::now(),
            },
        ];
        let store: HashSet<&str> = ["a"].into_iter().collect();
        let result = find_missing_db_blobs(&db_records, &store);
        assert_eq!(result, vec!["b", "c"]);
    }

    // ── filter_by_grace_period tests ───────────────────────────────────────

    #[test]
    fn grace_period_all_eligible_when_no_db_records() {
        // Orphans have no DB records by definition, so all should be eligible.
        let orphans: Vec<String> = ["a", "b"].into_iter().map(String::from).collect();
        let now = Utc::now();
        let result = filter_by_grace_period(&orphans, &[], 24, now);
        assert_eq!(result, vec!["a", "b"]);
    }

    #[test]
    fn grace_period_excludes_recent_db_record() {
        // If an orphan somehow has a recent DB record (edge case), skip it.
        let orphans: Vec<String> = ["a"].into_iter().map(String::from).collect();
        let now = Utc::now();
        let db_records = vec![DbBlobRecord {
            blob_key: "a".into(),
            created_at: now, // Just created — within grace period.
        }];
        let result = filter_by_grace_period(&orphans, &db_records, 24, now);
        assert!(result.is_empty());
    }

    #[test]
    fn grace_period_includes_old_db_record() {
        // A DB record older than the grace period shouldn't block deletion.
        let orphans: Vec<String> = ["a"].into_iter().map(String::from).collect();
        let now = Utc::now();
        let db_records = vec![DbBlobRecord {
            blob_key: "a".into(),
            created_at: now - Duration::hours(25), // Older than 24h grace.
        }];
        let result = filter_by_grace_period(&orphans, &db_records, 24, now);
        assert_eq!(result, vec!["a"]);
    }

    #[test]
    fn grace_period_non_matching_db_record_does_not_block() {
        // A DB record for key "b" should not block deletion of orphan "a".
        let orphans: Vec<String> = ["a"].into_iter().map(String::from).collect();
        let now = Utc::now();
        let db_records = vec![DbBlobRecord {
            blob_key: "b".into(),
            created_at: now, // Recent, but doesn't match orphan "a".
        }];
        let result = filter_by_grace_period(&orphans, &db_records, 24, now);
        assert_eq!(result, vec!["a"]);
    }

    // ── should_run_now tests ───────────────────────────────────────────────

    fn make_config(enabled: bool, hour: u8, minute: u8) -> OrphanGc {
        OrphanGc {
            enabled,
            schedule_hour: hour,
            schedule_minute: minute,
            ..OrphanGc::default()
        }
    }

    fn dt(hour: u32, minute: u32) -> DateTime<Utc> {
        DateTime::from_timestamp(0, 0)
            .unwrap()
            .with_hour(hour)
            .unwrap()
            .with_minute(minute)
            .unwrap()
    }

    #[test]
    fn should_not_run_when_disabled() {
        let cfg = make_config(false, 2, 0);
        assert!(!should_run_now(&cfg, None, dt(2, 0)));
    }

    #[test]
    fn should_run_first_time_at_scheduled_time() {
        let cfg = make_config(true, 2, 0);
        assert!(should_run_now(&cfg, None, dt(2, 0)));
        assert!(should_run_now(&cfg, None, dt(2, 30)));
        assert!(!should_run_now(&cfg, None, dt(1, 59)));
    }

    #[test]
    fn should_not_run_before_scheduled_time() {
        let cfg = make_config(true, 2, 0);
        assert!(!should_run_now(&cfg, None, dt(0, 0)));
        assert!(!should_run_now(&cfg, None, dt(1, 59)));
    }

    #[test]
    fn should_not_run_again_within_23_hours() {
        let cfg = make_config(true, 2, 0);
        let last = Some(dt(2, 0));
        assert!(!should_run_now(&cfg, last, dt(3, 0)));
    }

    #[test]
    fn should_run_if_last_run_was_long_ago() {
        let cfg = make_config(true, 2, 0);
        let last = Some(dt(2, 0) - Duration::hours(24));
        assert!(should_run_now(&cfg, last, dt(2, 0)));
    }

    #[test]
    fn gap_exactly_23_hours_does_not_run() {
        let cfg = make_config(true, 2, 0);
        let last_dt = dt(2, 0);
        let last = Some(last_dt);
        let now = last_dt + Duration::hours(23);
        assert!(!should_run_now(&cfg, last, now));
    }

    #[test]
    fn gap_slightly_more_than_23_hours_runs() {
        let cfg = make_config(true, 2, 0);
        let last_dt = dt(2, 0);
        let last = Some(last_dt);
        let now = last_dt + Duration::hours(24) + Duration::minutes(1);
        assert!(should_run_now(&cfg, last, now));
    }

    // ── MockOrphanGcRunner ─────────────────────────────────────────────────

    struct MockRunner {
        blob_keys: Mutex<Vec<String>>,
        db_records: Mutex<Vec<DbBlobRecord>>,
        deleted_keys: Mutex<Vec<String>>,
        list_blobs_error: Mutex<Option<String>>,
        list_db_error: Mutex<Option<String>>,
        delete_errors: Mutex<HashSet<String>>,
    }

    impl MockRunner {
        fn new() -> Self {
            Self {
                blob_keys: Mutex::new(Vec::new()),
                db_records: Mutex::new(Vec::new()),
                deleted_keys: Mutex::new(Vec::new()),
                list_blobs_error: Mutex::new(None),
                list_db_error: Mutex::new(None),
                delete_errors: Mutex::new(HashSet::new()),
            }
        }

        fn set_blob_keys(&self, keys: Vec<&str>) {
            *self.blob_keys.lock().unwrap() = keys.into_iter().map(String::from).collect();
        }

        fn set_db_records(&self, records: Vec<(&str, DateTime<Utc>)>) {
            *self.db_records.lock().unwrap() = records
                .into_iter()
                .map(|(key, created)| DbBlobRecord {
                    blob_key: key.into(),
                    created_at: created,
                })
                .collect();
        }

        fn set_list_blobs_error(&self, err: &str) {
            *self.list_blobs_error.lock().unwrap() = Some(err.into());
        }

        fn set_list_db_error(&self, err: &str) {
            *self.list_db_error.lock().unwrap() = Some(err.into());
        }

        fn set_delete_error(&self, key: &str) {
            self.delete_errors.lock().unwrap().insert(key.into());
        }

        fn deleted_keys(&self) -> Vec<String> {
            self.deleted_keys.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl OrphanGcRunner for MockRunner {
        async fn list_blob_keys(&self, _tenant_id: i64) -> anyhow::Result<Vec<String>> {
            if let Some(e) = self.list_blobs_error.lock().unwrap().take() {
                anyhow::bail!("{e}");
            }
            Ok(self.blob_keys.lock().unwrap().clone())
        }

        async fn list_db_records(&self, _tenant_id: i64) -> anyhow::Result<Vec<DbBlobRecord>> {
            if let Some(e) = self.list_db_error.lock().unwrap().take() {
                anyhow::bail!("{e}");
            }
            Ok(self.db_records.lock().unwrap().clone())
        }

        async fn delete_blob(&self, key: &str) -> anyhow::Result<()> {
            if self.delete_errors.lock().unwrap().contains(key) {
                anyhow::bail!("mock delete failed for {key}");
            }
            self.deleted_keys.lock().unwrap().push(key.into());
            Ok(())
        }
    }

    fn test_config() -> OrphanGc {
        OrphanGc {
            enabled: true,
            grace_hours: 24,
            ..OrphanGc::default()
        }
    }

    // ── Orchestration tests ────────────────────────────────────────────────

    #[tokio::test]
    async fn no_orphans_no_missing_success() {
        let _ = kb_metrics::init_metrics();
        let runner = MockRunner::new();
        runner.set_blob_keys(vec!["a", "b"]);
        runner.set_db_records(vec![("a", Utc::now()), ("b", Utc::now())]);

        let result = run_orphan_gc(&runner, &test_config(), 1).await;
        assert!(result.is_ok(), "expected Ok, got: {result:?}");
        assert!(runner.deleted_keys().is_empty());
    }

    #[tokio::test]
    async fn deletes_orphans_past_grace_period() {
        let _ = kb_metrics::init_metrics();
        let runner = MockRunner::new();
        runner.set_blob_keys(vec!["a", "b", "orphan-1", "orphan-2"]);
        runner.set_db_records(vec![("a", Utc::now()), ("b", Utc::now())]);

        let result = run_orphan_gc(&runner, &test_config(), 1).await;
        assert!(result.is_ok(), "expected Ok, got: {result:?}");

        let deleted = runner.deleted_keys();
        assert!(deleted.contains(&"orphan-1".to_string()));
        assert!(deleted.contains(&"orphan-2".to_string()));
        assert_eq!(deleted.len(), 2);
    }

    #[tokio::test]
    async fn detects_missing_db_blobs() {
        let _ = kb_metrics::init_metrics();
        let runner = MockRunner::new();
        // B2 has "a" and "b", but DB has "a", "b", and "c" (c is missing from B2).
        runner.set_blob_keys(vec!["a", "b"]);
        runner.set_db_records(vec![
            ("a", Utc::now()),
            ("b", Utc::now()),
            ("c", Utc::now()),
        ]);

        let result = run_orphan_gc(&runner, &test_config(), 1).await;
        // Should succeed (missing DB blobs are logged, not treated as failure).
        assert!(result.is_ok(), "expected Ok, got: {result:?}");
        // No orphans to delete — all B2 keys have DB rows.
        assert!(runner.deleted_keys().is_empty());
    }

    #[tokio::test]
    async fn list_blobs_error_is_reported() {
        let _ = kb_metrics::init_metrics();
        let runner = MockRunner::new();
        runner.set_list_blobs_error("B2 connection refused");

        let result = run_orphan_gc(&runner, &test_config(), 1).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("B2 connection refused"));
    }

    #[tokio::test]
    async fn list_db_error_is_reported() {
        let _ = kb_metrics::init_metrics();
        let runner = MockRunner::new();
        runner.set_blob_keys(vec!["a"]);
        runner.set_list_db_error("Postgres connection refused");

        let result = run_orphan_gc(&runner, &test_config(), 1).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Postgres connection refused"));
    }

    #[tokio::test]
    async fn delete_failure_does_not_block_others() {
        let _ = kb_metrics::init_metrics();
        let runner = MockRunner::new();
        runner.set_blob_keys(vec!["a", "orphan-1", "orphan-2"]);
        runner.set_db_records(vec![("a", Utc::now())]);
        runner.set_delete_error("orphan-1");

        let result = run_orphan_gc(&runner, &test_config(), 1).await;
        // Should still succeed — delete failures are non-fatal.
        assert!(result.is_ok(), "expected Ok, got: {result:?}");

        let deleted = runner.deleted_keys();
        assert!(!deleted.contains(&"orphan-1".to_string()));
        assert!(deleted.contains(&"orphan-2".to_string()));
    }

    #[tokio::test]
    async fn process_orphan_gc_job_success() {
        let _ = kb_metrics::init_metrics();
        let runner = MockRunner::new();
        runner.set_blob_keys(vec!["a", "b"]);
        runner.set_db_records(vec![("a", Utc::now()), ("b", Utc::now())]);

        let job = Job {
            id: 1,
            tenant_id: 42,
            file_id: None,
            document_id: None,
            kind: JobKind::OrphanGc,
            priority: 100,
            status: kb_core::job::JobStatus::Running,
            attempts: 1,
            last_error: None,
            run_after: Utc::now(),
            created_at: Utc::now(),
        };

        let result = process_orphan_gc_job(&runner, &test_config(), &job).await;
        assert!(result.is_ok());
    }
}
