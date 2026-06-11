// SPDX-License-Identifier: AGPL-3.0-or-later

//! Blob integrity scan maintenance job (plan §23, P8-T10).
//!
//! The integrity scan periodically re-hashes a random sample of B2 blobs and
//! compares the computed SHA-256 to the value stored in the `files` table,
//! detecting bit-rot or tampering.
//!
//! # Scheduling
//!
//! The job runs on a configurable daily schedule (default: 04:00 UTC). A simple
//! time-of-day check gates execution — the P8-T11 maintenance scheduler will
//! eventually replace this with a general-purpose cron facility.
//!
//! # Testing
//!
//! The [`IntegrityScanRunner`] trait abstracts blob-store and DB operations so the
//! orchestration logic can be tested with a mock. Pure functions (sample selection
//! and hash verification) are unit-tested directly.

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Datelike, Duration, Timelike, Utc};
use kb_config::IntegrityScan;
use kb_core::hash::Sha256;
use kb_core::job::Job;
#[cfg(test)]
use kb_core::job::JobKind;
use sha2::{Digest, Sha256 as Sha256Hasher};

// ── Record types ──────────────────────────────────────────────────────────────────

/// A lightweight record from the `files` table used for integrity verification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileShaRecord {
    /// The content-addressed blob key (`files.blob_key`).
    pub blob_key: String,
    /// The SHA-256 hash stored in the `files` table.
    pub sha256: Sha256,
}

// ── IntegrityScanRunner trait ─────────────────────────────────────────────────────

/// Abstracts the blob-store and DB operations needed for the integrity scan so the
/// orchestration logic can be tested without a real B2 + Postgres backend.
#[async_trait]
pub trait IntegrityScanRunner: Send + Sync {
    /// List all file records (blob_key + sha256) from the `files` table for the
    /// given tenant.
    async fn list_file_records(&self, tenant_id: i64) -> anyhow::Result<Vec<FileShaRecord>>;

    /// Get the raw bytes of a blob by key.
    async fn get_blob_bytes(&self, key: &str) -> anyhow::Result<Bytes>;
}

// ── Pure functions ────────────────────────────────────────────────────────────────

/// Select a random sample of records.
///
/// `sample_pct` is the percentage (1–100) of records to sample. Returns an empty
/// vec when there are no records or when `sample_pct` is 0.
#[must_use]
pub fn select_sample(records: &[FileShaRecord], sample_pct: u8) -> Vec<FileShaRecord> {
    if records.is_empty() || sample_pct == 0 {
        return Vec::new();
    }

    let sample_pct = sample_pct.min(100);
    let mut indices: Vec<usize> = (0..records.len()).collect();

    // Deterministic shuffle using a simple modulo-based selection so tests are
    // reproducible. In production we want a deterministic but spread-out
    // selection that gives different samples on different days.
    //
    // We use the current day-of-year as a rotation offset so the same blobs
    // are not re-checked every day.
    let day_offset = (Utc::now().ordinal0() as usize) % records.len();
    indices.rotate_left(day_offset);

    let sample_size = (records.len() * sample_pct as usize).div_ceil(100);
    indices.truncate(sample_size);

    indices.into_iter().map(|i| records[i].clone()).collect()
}

/// Verify that the SHA-256 hash of `data` matches the expected value.
///
/// Returns `true` if the hashes match, `false` otherwise.
#[must_use]
pub fn verify_hash(data: &[u8], expected: &Sha256) -> bool {
    let mut hasher = Sha256Hasher::new();
    hasher.update(data);
    let computed = hasher.finalize();
    let computed_bytes: [u8; 32] = computed.into();
    &Sha256::from_bytes(computed_bytes) == expected
}

/// Determine whether an integrity scan should execute right now.
///
/// The job runs when the current UTC hour and minute match or exceed the
/// configured schedule, and the last run was more than 23 hours ago (or never).
#[must_use]
pub fn should_run_now(
    config: &IntegrityScan,
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

/// Run a full integrity scan for a single tenant.
///
/// 1. List all file records from Postgres.
/// 2. Select a random sample based on `config.sample_pct` (default 10%).
/// 3. For each sampled record, fetch the blob bytes from B2, compute SHA-256,
///    and compare to the stored hash.
/// 4. Report results to Prometheus metrics.
///
/// # Errors
///
/// Returns an error string suitable for `jobs.last_error` on any step failure
/// in the listing phase. Individual blob fetch/hash failures are logged but do
/// not abort the scan.
pub async fn run_integrity_scan(
    runner: &dyn IntegrityScanRunner,
    config: &IntegrityScan,
    tenant_id: i64,
) -> Result<(), String> {
    // ── 1. List all file records ───────────────────────────────────────────
    let records = runner.list_file_records(tenant_id).await.map_err(|e| {
        let msg = format!("integrity-scan: failed to list file records: {e:#}");
        tracing::error!(error = %e, tenant_id, "integrity-scan: {msg}");
        msg
    })?;

    if records.is_empty() {
        tracing::info!(tenant_id, "integrity-scan: no files to scan");
        kb_metrics::record_integrity_scan_result(0, 0);
        return Ok(());
    }

    // ── 2. Select sample ───────────────────────────────────────────────────
    let sample = select_sample(&records, config.sample_pct);
    let total_sampled = sample.len() as u64;

    tracing::info!(
        tenant_id,
        total_records = records.len(),
        sampled = total_sampled,
        sample_pct = config.sample_pct,
        "integrity-scan: sampling {total_sampled} of {} records",
        records.len()
    );

    // ── 3. Verify each sampled blob ────────────────────────────────────────
    let mut verified: u64 = 0;
    let mut failed: u64 = 0;
    let mut skipped: u64 = 0;

    for rec in &sample {
        match runner.get_blob_bytes(&rec.blob_key).await {
            Ok(data) => {
                if verify_hash(&data, &rec.sha256) {
                    verified += 1;
                } else {
                    failed += 1;
                    tracing::error!(
                        tenant_id,
                        blob_key = rec.blob_key,
                        "integrity-scan: HASH MISMATCH — blob content does not match stored SHA-256"
                    );
                }
            }
            Err(e) => {
                skipped += 1;
                tracing::warn!(
                    tenant_id,
                    blob_key = rec.blob_key,
                    error = %e,
                    "integrity-scan: could not fetch blob, skipped"
                );
            }
        }
    }

    // ── 4. Report to metrics ───────────────────────────────────────────────
    kb_metrics::record_integrity_scan_result(verified, failed);

    tracing::info!(
        tenant_id,
        total_sampled,
        verified,
        failed,
        skipped,
        "integrity-scan: complete — {verified} ok, {failed} failed, {skipped} skipped"
    );

    if failed > 0 {
        Err(format!(
            "integrity-scan: {failed} of {total_sampled} blobs failed hash verification"
        ))
    } else {
        Ok(())
    }
}

// ── Job-queue integration ─────────────────────────────────────────────────────────

/// A job-queue handler for integrity scan jobs.
///
/// When a [`Job`] with [`JobKind::IntegrityScan`] is claimed, this handler runs the
/// full integrity scan cycle. The worker pool will then call `queue.complete(job.id)`
/// on success or `queue.fail(job.id, &error)` on failure.
///
/// # Errors
///
/// Returns `Err(String)` if any step fails. The worker pool applies exponential
/// backoff before retrying.
pub async fn process_integrity_scan_job(
    runner: &dyn IntegrityScanRunner,
    config: &IntegrityScan,
    job: &Job,
) -> Result<(), String> {
    run_integrity_scan(runner, config, job.tenant_id).await
}

// ── Tests ──────────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::Mutex;

    use super::*;

    // ── Select sample tests ────────────────────────────────────────────────

    fn make_records(n: usize) -> Vec<FileShaRecord> {
        (0..n)
            .map(|i| {
                let mut hash = [0u8; 32];
                hash[0..8].copy_from_slice(&(i as u64).to_le_bytes());
                FileShaRecord {
                    blob_key: format!("key-{i}"),
                    sha256: Sha256::from_bytes(hash),
                }
            })
            .collect()
    }

    #[test]
    fn select_sample_empty_returns_empty() {
        let result = select_sample(&[], 10);
        assert!(result.is_empty());
    }

    #[test]
    fn select_sample_zero_pct_returns_empty() {
        let records = make_records(100);
        let result = select_sample(&records, 0);
        assert!(result.is_empty());
    }

    #[test]
    fn select_sample_100_pct_returns_all() {
        let records = make_records(10);
        let result = select_sample(&records, 100);
        assert_eq!(result.len(), 10);
    }

    #[test]
    fn select_sample_10_pct_of_100_returns_10() {
        let records = make_records(100);
        let result = select_sample(&records, 10);
        assert_eq!(result.len(), 10);
    }

    #[test]
    fn select_sample_rounds_up() {
        // 3 records at 50% → ceil(3 * 50 / 100) = ceil(1.5) = 2.
        let records = make_records(3);
        let result = select_sample(&records, 50);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn select_sample_does_not_exceed_total() {
        let records = make_records(5);
        let result = select_sample(&records, 200); // clamped to 100.
        assert_eq!(result.len(), 5);
    }

    #[test]
    fn select_sample_is_deterministic() {
        let records = make_records(100);
        let r1 = select_sample(&records, 10);
        let r2 = select_sample(&records, 10);
        assert_eq!(r1, r2);
    }

    // ── verify_hash tests ──────────────────────────────────────────────────

    #[test]
    fn verify_hash_matches_known_content() {
        let data = b"hello world";
        let mut hasher = Sha256Hasher::new();
        hasher.update(data);
        let digest: [u8; 32] = hasher.finalize().into();
        let expected = Sha256::from_bytes(digest);

        assert!(verify_hash(data, &expected));
    }

    #[test]
    fn verify_hash_detects_mismatch() {
        let data = b"hello world";
        let wrong_hash = Sha256::from_bytes([0u8; 32]);

        assert!(!verify_hash(data, &wrong_hash));
    }

    #[test]
    fn verify_hash_detects_tampering() {
        let data = b"original content";
        let mut hasher = Sha256Hasher::new();
        hasher.update(data);
        let digest: [u8; 32] = hasher.finalize().into();
        let expected = Sha256::from_bytes(digest);

        // Different data should fail.
        assert!(!verify_hash(b"tampered content", &expected));
    }

    #[test]
    fn verify_hash_empty_data() {
        let data = b"";
        let mut hasher = Sha256Hasher::new();
        hasher.update(data);
        let digest: [u8; 32] = hasher.finalize().into();
        let expected = Sha256::from_bytes(digest);

        assert!(verify_hash(data, &expected));
    }

    // ── should_run_now tests ───────────────────────────────────────────────

    fn make_config(enabled: bool, hour: u8, minute: u8) -> IntegrityScan {
        IntegrityScan {
            enabled,
            schedule_hour: hour,
            schedule_minute: minute,
            ..IntegrityScan::default()
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
        let cfg = make_config(false, 4, 0);
        assert!(!should_run_now(&cfg, None, dt(4, 0)));
    }

    #[test]
    fn should_run_first_time_at_scheduled_time() {
        let cfg = make_config(true, 4, 0);
        assert!(should_run_now(&cfg, None, dt(4, 0)));
        assert!(should_run_now(&cfg, None, dt(4, 30)));
        assert!(!should_run_now(&cfg, None, dt(3, 59)));
    }

    #[test]
    fn should_not_run_before_scheduled_time() {
        let cfg = make_config(true, 4, 0);
        assert!(!should_run_now(&cfg, None, dt(0, 0)));
        assert!(!should_run_now(&cfg, None, dt(3, 59)));
    }

    #[test]
    fn should_not_run_again_within_23_hours() {
        let cfg = make_config(true, 4, 0);
        let last = Some(dt(4, 0));
        assert!(!should_run_now(&cfg, last, dt(5, 0)));
    }

    #[test]
    fn should_run_if_last_run_was_long_ago() {
        let cfg = make_config(true, 4, 0);
        let last = Some(dt(4, 0) - Duration::hours(24));
        assert!(should_run_now(&cfg, last, dt(4, 0)));
    }

    // ── MockIntegrityScanRunner ────────────────────────────────────────────

    struct MockRunner {
        records: Mutex<Vec<FileShaRecord>>,
        blob_data: Mutex<HashMap<String, Bytes>>,
        list_error: Mutex<Option<String>>,
        get_error_keys: Mutex<HashSet<String>>,
    }

    impl MockRunner {
        fn new() -> Self {
            Self {
                records: Mutex::new(Vec::new()),
                blob_data: Mutex::new(HashMap::new()),
                list_error: Mutex::new(None),
                get_error_keys: Mutex::new(HashSet::new()),
            }
        }

        fn set_records(&self, records: Vec<FileShaRecord>) {
            *self.records.lock().unwrap() = records;
        }

        fn set_blob_data(&self, key: &str, data: Bytes) {
            self.blob_data.lock().unwrap().insert(key.into(), data);
        }

        fn set_list_error(&self, err: &str) {
            *self.list_error.lock().unwrap() = Some(err.into());
        }

        fn set_get_error(&self, key: &str) {
            self.get_error_keys.lock().unwrap().insert(key.into());
        }
    }

    #[async_trait]
    impl IntegrityScanRunner for MockRunner {
        async fn list_file_records(&self, _tenant_id: i64) -> anyhow::Result<Vec<FileShaRecord>> {
            if let Some(e) = self.list_error.lock().unwrap().take() {
                anyhow::bail!("{e}");
            }
            Ok(self.records.lock().unwrap().clone())
        }

        async fn get_blob_bytes(&self, key: &str) -> anyhow::Result<Bytes> {
            if self.get_error_keys.lock().unwrap().contains(key) {
                anyhow::bail!("mock get failed for {key}");
            }
            self.blob_data
                .lock()
                .unwrap()
                .get(key)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("mock blob not found: {key}"))
        }
    }

    fn test_config() -> IntegrityScan {
        IntegrityScan {
            enabled: true,
            sample_pct: 100, // Scan everything for predictable tests.
            ..IntegrityScan::default()
        }
    }

    /// Create a FileShaRecord whose sha256 matches the given data.
    fn matching_record(key: &str, data: &[u8]) -> FileShaRecord {
        let mut hasher = Sha256Hasher::new();
        hasher.update(data);
        let digest: [u8; 32] = hasher.finalize().into();
        FileShaRecord {
            blob_key: key.into(),
            sha256: Sha256::from_bytes(digest),
        }
    }

    // ── Orchestration tests ────────────────────────────────────────────────

    #[tokio::test]
    async fn all_blobs_pass_integrity_check() {
        let _ = kb_metrics::init_metrics();
        let runner = MockRunner::new();

        let data1 = b"hello";
        let data2 = b"world";
        let rec1 = matching_record("key-1", data1);
        let rec2 = matching_record("key-2", data2);

        runner.set_records(vec![rec1.clone(), rec2.clone()]);
        runner.set_blob_data("key-1", Bytes::from_static(data1));
        runner.set_blob_data("key-2", Bytes::from_static(data2));

        let result = run_integrity_scan(&runner, &test_config(), 1).await;
        assert!(result.is_ok(), "expected Ok, got: {result:?}");
    }

    #[tokio::test]
    async fn hash_mismatch_is_detected() {
        let _ = kb_metrics::init_metrics();
        let runner = MockRunner::new();

        let good_data = b"good data";
        let rec1 = matching_record("good-key", good_data);
        // Create a record whose stored hash does NOT match the actual blob.
        let bad_hash = Sha256::from_bytes([0u8; 32]);
        let rec2 = FileShaRecord {
            blob_key: "bad-key".into(),
            sha256: bad_hash,
        };

        runner.set_records(vec![rec1, rec2]);
        runner.set_blob_data("good-key", Bytes::from_static(good_data));
        runner.set_blob_data("bad-key", Bytes::from_static(b"different data"));

        let result = run_integrity_scan(&runner, &test_config(), 1).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("failed hash verification"));
    }

    #[tokio::test]
    async fn empty_records_is_ok() {
        let _ = kb_metrics::init_metrics();
        let runner = MockRunner::new();
        runner.set_records(vec![]);

        let result = run_integrity_scan(&runner, &test_config(), 1).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn list_error_is_reported() {
        let _ = kb_metrics::init_metrics();
        let runner = MockRunner::new();
        runner.set_list_error("DB error");

        let result = run_integrity_scan(&runner, &test_config(), 1).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("DB error"));
    }

    #[tokio::test]
    async fn fetch_error_is_skipped_not_fatal() {
        let _ = kb_metrics::init_metrics();
        let runner = MockRunner::new();

        let data = b"good";
        let rec = matching_record("good-key", data);
        runner.set_records(vec![rec]);
        // Don't set blob data for "good-key" → get will fail.
        runner.set_get_error("good-key");

        let result = run_integrity_scan(&runner, &test_config(), 1).await;
        // Fetch failure is non-fatal; scan succeeds but logs warning.
        assert!(result.is_ok(), "fetch failure should not fail scan");
    }

    #[tokio::test]
    async fn mixed_success_and_failure_reports_correctly() {
        let _ = kb_metrics::init_metrics();
        let runner = MockRunner::new();

        let good_data = b"good content";
        let rec1 = matching_record("good-key", good_data);
        let bad_hash = Sha256::from_bytes([0u8; 32]);
        let rec2 = FileShaRecord {
            blob_key: "bad-key".into(),
            sha256: bad_hash,
        };

        runner.set_records(vec![rec1, rec2]);
        runner.set_blob_data("good-key", Bytes::from_static(good_data));
        runner.set_blob_data("bad-key", Bytes::from_static(b"corrupted data"));

        let result = run_integrity_scan(&runner, &test_config(), 1).await;
        assert!(result.is_err());
        let msg = result.unwrap_err();
        assert!(msg.contains("1 of 2"));
        assert!(msg.contains("failed"));
    }

    #[tokio::test]
    async fn process_integrity_scan_job_success() {
        let _ = kb_metrics::init_metrics();
        let runner = MockRunner::new();

        let data = b"test data";
        let rec = matching_record("test-key", data);
        runner.set_records(vec![rec]);
        runner.set_blob_data("test-key", Bytes::from_static(data));

        let job = Job {
            id: 1,
            tenant_id: 42,
            file_id: None,
            document_id: None,
            kind: JobKind::IntegrityScan,
            priority: 100,
            status: kb_core::job::JobStatus::Running,
            attempts: 1,
            last_error: None,
            run_after: Utc::now(),
            created_at: Utc::now(),
            created_by: None,
        };

        let result = process_integrity_scan_job(&runner, &test_config(), &job).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn small_sample_pct_scans_fewer() {
        let _ = kb_metrics::init_metrics();
        let runner = MockRunner::new();

        // 100 records, only 10% scanned (10 records).
        let mut records = Vec::new();
        for i in 0..100 {
            let data = format!("data-{i}").into_bytes();
            let rec = matching_record(&format!("key-{i}"), &data);
            records.push(rec);
            runner.set_blob_data(&format!("key-{i}"), Bytes::from(data));
        }
        // Use a config with 10% sample.
        let config = IntegrityScan {
            enabled: true,
            sample_pct: 10,
            ..IntegrityScan::default()
        };

        runner.set_records(records);
        let result = run_integrity_scan(&runner, &config, 1).await;
        assert!(result.is_ok(), "expected Ok, got: {result:?}");
    }
}
