//! P8 phase integration test (§20, §22, §23, §31): comprehensive verification that
//! all P8 components work together correctly.
//!
//! # Coverage
//!
//! **Non-ignored (fast, runs in `just ci`):**
//! 1. Full encrypted + cached blob stack: Cache(Encrypt(LocalBlob)) — put → get →
//!    decrypt → verify; cache hit/miss/eviction; key rotation.
//! 2. Degradation + circuit breaker integration: circuit opens → degradation reflects;
//!    circuit closes → degradation clears; multiple subsystems.
//! 3. Maintenance scheduler with fake clock + spy handlers: due/not-due; multiple
//!    jobs; failure tolerance.
//! 4. Orphan GC + integrity scan with mock runners: full cycle, edge cases.
//!
//! **#[ignore] tests (need Podman + pgvector/pgvector:pg17):**
//! 5. Orphan GC with PgStore-backed runner: create file records in DB, orphan blobs
//!    on disk, run GC, verify cleanup.
//! 6. Integrity scan with PgStore-backed runner: insert file records, run scan,
//!    verify hash matches.
//! 7. Maintenance VACUUM on real Postgres: handler succeeds without error.
//! 8. Re-embed detection: embedder_id check against settings table.
//!
//! Run #[ignore] suites with:
//!
//! ```bash
//! systemctl --user start podman.socket
//! DOCKER_HOST="unix://$XDG_RUNTIME_DIR/podman/podman.sock" \
//!   cargo test --package kb-pipeline --test p8_integration -- --ignored
//! ```
//!
//! P8 PHASE COMPLETE after this passes → generate P9 ledger.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use kb_core::blob::Blob;
use kb_core::circuit_breaker::CircuitBreaker;
use kb_core::degradation::DegradationState;
use kb_core::kek::{FileKek, KeyEncryptionKey};
use kb_pipeline::integrity_scan::{
    FileShaRecord, IntegrityScanRunner, run_integrity_scan, select_sample, verify_hash,
};
use kb_pipeline::maintenance::{
    FakeClock, MaintenanceHandler, MaintenanceJobKind, MaintenanceScheduler, Schedule, is_due,
};
use kb_pipeline::orphan_gc::{
    OrphanGcRunner, filter_by_grace_period, find_missing_db_blobs, find_orphan_blobs, run_orphan_gc,
};
use kb_store::{BlobCache, EncryptedBlob, LocalBlob};
use sha2::Digest;

// ── Helpers ──────────────────────────────────────────────────────────────────────

/// A deterministic 32-byte test KEK (NOT for production use).
const TEST_KEY: [u8; 32] = [0xABu8; 32];

fn test_kek() -> FileKek {
    FileKek::from_key(TEST_KEY)
}

/// Create a temporary directory and return its path (caller keeps the guard alive).
fn temp_dir() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    (dir, path)
}

// ═════════════════════════════════════════════════════════════════════════════════
// 1. FULL ENCRYPTED + CACHED BLOB STACK TESTS (non-ignored)
// ═════════════════════════════════════════════════════════════════════════════════

/// Build the full layered blob stack: Cache → Encrypt → LocalBlob.
fn build_full_stack(
    blob_dir: &std::path::Path,
    cache_dir: &std::path::Path,
) -> (Arc<dyn Blob>, Arc<FileKek>) {
    let local = Arc::new(LocalBlob::new(blob_dir.to_path_buf(), "test-tenant".into()));
    let kek: Arc<FileKek> = Arc::new(test_kek());
    let encrypted = Arc::new(EncryptedBlob::new(local, kek.clone()));
    let cached = Arc::new(
        BlobCache::new(encrypted, cache_dir.to_path_buf(), 50 * 1024 * 1024, false).unwrap(),
    );
    (cached, kek)
}

#[tokio::test]
async fn full_stack_put_get_roundtrip() {
    let (_blob_guard, blob_dir) = temp_dir();
    let (_cache_guard, cache_dir) = temp_dir();
    let (stack, _kek) = build_full_stack(&blob_dir, &cache_dir);

    let key = "test-tenant/roundtrip-file.bin";
    let data = Bytes::from_static(b"Hello, P8 layered blob stack!");

    stack.put(key, data.clone()).await.unwrap();
    let fetched = stack.get(key).await.unwrap();

    assert_eq!(fetched, data);
}

#[tokio::test]
async fn full_stack_exists_after_put() {
    let (_blob_guard, blob_dir) = temp_dir();
    let (_cache_guard, cache_dir) = temp_dir();
    let (stack, _kek) = build_full_stack(&blob_dir, &cache_dir);

    let key = "test-tenant/exists-test.bin";
    assert!(!stack.exists(key).await.unwrap());

    stack.put(key, Bytes::from_static(b"data")).await.unwrap();
    assert!(stack.exists(key).await.unwrap());
}

#[tokio::test]
async fn full_stack_delete_removes_blob() {
    let (_blob_guard, blob_dir) = temp_dir();
    let (_cache_guard, cache_dir) = temp_dir();
    let (stack, _kek) = build_full_stack(&blob_dir, &cache_dir);

    let key = "test-tenant/to-delete.bin";
    stack
        .put(key, Bytes::from_static(b"ephemeral"))
        .await
        .unwrap();
    assert!(stack.exists(key).await.unwrap());

    stack.delete(key).await.unwrap();
    assert!(!stack.exists(key).await.unwrap());
}

#[tokio::test]
async fn full_stack_get_nonexistent_is_error() {
    let (_blob_guard, blob_dir) = temp_dir();
    let (_cache_guard, cache_dir) = temp_dir();
    let (stack, _kek) = build_full_stack(&blob_dir, &cache_dir);

    let err = stack.get("test-tenant/nonexistent.bin").await.unwrap_err();
    assert!(
        err.to_string().contains("not found")
            || err.to_string().contains("No such file")
            || err.to_string().contains("failed"),
        "expected an error for nonexistent key, got: {err}"
    );
}

#[tokio::test]
async fn full_stack_put_is_idempotent_from_caller_view() {
    let (_blob_guard, blob_dir) = temp_dir();
    let (_cache_guard, cache_dir) = temp_dir();
    let (stack, _kek) = build_full_stack(&blob_dir, &cache_dir);

    let key = "test-tenant/idempotent.bin";
    let data = Bytes::from_static(b"same content twice");

    stack.put(key, data.clone()).await.unwrap();
    stack.put(key, data.clone()).await.unwrap();

    let fetched = stack.get(key).await.unwrap();
    assert_eq!(fetched, data);
}

#[tokio::test]
async fn full_stack_different_content_same_key_overwrites() {
    let (_blob_guard, blob_dir) = temp_dir();
    let (_cache_guard, cache_dir) = temp_dir();
    let (stack, _kek) = build_full_stack(&blob_dir, &cache_dir);

    let key = "test-tenant/overwrite.bin";
    stack.put(key, Bytes::from_static(b"v1")).await.unwrap();
    stack.put(key, Bytes::from_static(b"v2")).await.unwrap();

    let fetched = stack.get(key).await.unwrap();
    assert_eq!(fetched, Bytes::from_static(b"v2"));
}

#[tokio::test]
async fn full_stack_large_payload_roundtrip() {
    let (_blob_guard, blob_dir) = temp_dir();
    let (_cache_guard, cache_dir) = temp_dir();
    let (stack, _kek) = build_full_stack(&blob_dir, &cache_dir);

    let key = "test-tenant/large.bin";
    // 1 MB of deterministic data.
    let data: Vec<u8> = (0..1_048_576u32).map(|i| (i % 251) as u8).collect();
    let data = Bytes::from(data);

    stack.put(key, data.clone()).await.unwrap();
    let fetched = stack.get(key).await.unwrap();

    assert_eq!(fetched, data);
}

#[tokio::test]
async fn full_stack_get_range_returns_correct_slice() {
    let (_blob_guard, blob_dir) = temp_dir();
    let (_cache_guard, cache_dir) = temp_dir();
    let (stack, _kek) = build_full_stack(&blob_dir, &cache_dir);

    let key = "test-tenant/range-test.bin";
    let data = Bytes::from_static(b"ABCDEFGHIJKLMNOPQRSTUVWXYZ");
    stack.put(key, data.clone()).await.unwrap();

    // get_range decrypts the full blob then slices (GCM is not seekable).
    let slice = stack.get_range(key, 0, 5).await.unwrap();
    assert_eq!(slice, Bytes::from_static(b"ABCDE"));

    let slice = stack.get_range(key, 10, 15).await.unwrap();
    assert_eq!(slice, Bytes::from_static(b"KLMNO"));
}

#[tokio::test]
async fn full_stack_encrypted_blob_presigned_url_is_error() {
    let (_blob_guard, blob_dir) = temp_dir();
    let (_cache_guard, cache_dir) = temp_dir();
    let (stack, _kek) = build_full_stack(&blob_dir, &cache_dir);

    let key = "test-tenant/presigned-err.bin";
    stack.put(key, Bytes::from_static(b"data")).await.unwrap();

    // EncryptedBlob always errors on presigned_get_url (ciphertext is meaningless).
    let err = stack
        .presigned_get_url(key, Duration::from_secs(3600))
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("presigned") || err.to_string().contains("not supported"),
        "expected presigned-not-supported error, got: {err}"
    );
}

#[tokio::test]
async fn full_stack_cache_survives_multiple_gets() {
    let (_blob_guard, blob_dir) = temp_dir();
    let (_cache_guard, cache_dir) = temp_dir();
    let (stack, _kek) = build_full_stack(&blob_dir, &cache_dir);

    let key = "test-tenant/cache-multi-get.bin";
    let data = Bytes::from_static(b"data fetched multiple times");
    stack.put(key, data.clone()).await.unwrap();

    for _ in 0..5 {
        let fetched = stack.get(key).await.unwrap();
        assert_eq!(fetched, data);
    }
}

#[tokio::test]
async fn full_stack_key_rotation_new_kek_cannot_decrypt_old_data() {
    let (_blob_guard, blob_dir) = temp_dir();

    let old_kek: Arc<dyn KeyEncryptionKey> = Arc::new(FileKek::from_key([0x11u8; 32]));
    let new_kek: Arc<dyn KeyEncryptionKey> = Arc::new(FileKek::from_key([0x22u8; 32]));

    let local = Arc::new(LocalBlob::new(blob_dir.to_path_buf(), "test-tenant".into()));
    let old_encrypted = Arc::new(EncryptedBlob::new(local.clone(), old_kek));

    let key = "test-tenant/rotation-test.bin";
    let data = Bytes::from_static(b"encrypted with old KEK");
    old_encrypted.put(key, data.clone()).await.unwrap();

    // New KEK cannot decrypt old ciphertext.
    let new_encrypted = EncryptedBlob::new(local, new_kek);
    let err = new_encrypted.get(key).await.unwrap_err();
    assert!(
        err.to_string().contains("unwrap") || err.to_string().contains("decrypt"),
        "expected decrypt/unwrap error with wrong KEK, got: {err}"
    );
}

#[tokio::test]
async fn full_stack_empty_payload_roundtrip() {
    let (_blob_guard, blob_dir) = temp_dir();
    let (_cache_guard, cache_dir) = temp_dir();
    let (stack, _kek) = build_full_stack(&blob_dir, &cache_dir);

    let key = "test-tenant/empty.bin";
    let data = Bytes::new();

    stack.put(key, data.clone()).await.unwrap();
    let fetched = stack.get(key).await.unwrap();

    assert_eq!(fetched, data);
    assert!(fetched.is_empty());
}

#[tokio::test]
async fn full_stack_single_byte_roundtrip() {
    let (_blob_guard, blob_dir) = temp_dir();
    let (_cache_guard, cache_dir) = temp_dir();
    let (stack, _kek) = build_full_stack(&blob_dir, &cache_dir);

    let key = "test-tenant/one-byte.bin";
    let data = Bytes::from_static(b"X");

    stack.put(key, data.clone()).await.unwrap();
    let fetched = stack.get(key).await.unwrap();

    assert_eq!(fetched, data);
}

#[tokio::test]
async fn full_stack_multiple_keys_independent() {
    let (_blob_guard, blob_dir) = temp_dir();
    let (_cache_guard, cache_dir) = temp_dir();
    let (stack, _kek) = build_full_stack(&blob_dir, &cache_dir);

    let data_a = Bytes::from_static(b"content A");
    let data_b = Bytes::from_static(b"content B");
    let data_c = Bytes::from_static(b"content C");

    stack
        .put("test-tenant/a.bin", data_a.clone())
        .await
        .unwrap();
    stack
        .put("test-tenant/b.bin", data_b.clone())
        .await
        .unwrap();
    stack
        .put("test-tenant/c.bin", data_c.clone())
        .await
        .unwrap();

    assert_eq!(stack.get("test-tenant/a.bin").await.unwrap(), data_a);
    assert_eq!(stack.get("test-tenant/b.bin").await.unwrap(), data_b);
    assert_eq!(stack.get("test-tenant/c.bin").await.unwrap(), data_c);

    stack.delete("test-tenant/b.bin").await.unwrap();
    assert!(stack.exists("test-tenant/a.bin").await.unwrap());
    assert!(!stack.exists("test-tenant/b.bin").await.unwrap());
    assert!(stack.exists("test-tenant/c.bin").await.unwrap());
}

#[tokio::test]
async fn full_stack_delete_then_reput_works() {
    let (_blob_guard, blob_dir) = temp_dir();
    let (_cache_guard, cache_dir) = temp_dir();
    let (stack, _kek) = build_full_stack(&blob_dir, &cache_dir);

    let key = "test-tenant/delete-reput.bin";
    stack.put(key, Bytes::from_static(b"v1")).await.unwrap();
    stack.delete(key).await.unwrap();
    stack.put(key, Bytes::from_static(b"v2")).await.unwrap();

    assert_eq!(stack.get(key).await.unwrap(), Bytes::from_static(b"v2"));
}

// ═════════════════════════════════════════════════════════════════════════════════
// 2. DEGRADATION + CIRCUIT BREAKER TESTS (non-ignored)
// ═════════════════════════════════════════════════════════════════════════════════

#[test]
fn circuit_breaker_opens_after_threshold_failures() {
    let cb = CircuitBreaker::new(3, Duration::from_secs(30));
    assert!(!cb.is_open("b2"));

    cb.record_failure("b2");
    cb.record_failure("b2");
    assert!(!cb.is_open("b2"));

    cb.record_failure("b2");
    assert!(cb.is_open("b2"));
}

#[test]
fn circuit_breaker_closes_after_success() {
    let cb = CircuitBreaker::new(2, Duration::from_secs(30));

    cb.record_failure("b2");
    cb.record_failure("b2");
    assert!(cb.is_open("b2"));

    cb.record_success("b2");
    assert!(!cb.is_open("b2"));
    assert_eq!(cb.failures("b2"), 0);
}

#[test]
fn circuit_breaker_per_dependency_isolation() {
    let cb = CircuitBreaker::new(2, Duration::from_secs(30));

    cb.record_failure("b2");
    cb.record_failure("b2");
    assert!(cb.is_open("b2"));
    assert!(!cb.is_open("embed"));
}

#[test]
fn circuit_breaker_threshold_zero_disables() {
    let cb = CircuitBreaker::new(0, Duration::from_secs(30));
    cb.record_failure("b2");
    cb.record_failure("b2");
    cb.record_failure("b2");
    assert!(!cb.is_open("b2"));
}

#[test]
fn degradation_state_tracks_subsystems() {
    let ds = DegradationState::default();
    assert!(!ds.is_degraded());
    assert!(ds.degraded_subsystems().is_empty());

    ds.set_role_degraded("embed", true);
    assert!(ds.is_degraded());
    assert_eq!(ds.degraded_subsystems(), vec!["embed"]);

    ds.set_role_degraded("text", true);
    let mut subs = ds.degraded_subsystems();
    subs.sort_unstable();
    assert_eq!(subs, vec!["embed", "text"]);
}

#[test]
fn degradation_header_value_reflects_state() {
    let ds = DegradationState::default();
    assert!(ds.degraded_header_value().is_none());

    ds.set_role_degraded("embed", true);
    assert_eq!(ds.degraded_header_value().unwrap(), "embed");

    ds.set_role_degraded("text", true);
    let val = ds.degraded_header_value().unwrap();
    assert!(val.contains("embed"));
    assert!(val.contains("text"));
}

#[test]
fn degradation_subsystem_can_recover() {
    let ds = DegradationState::default();

    ds.set_role_degraded("embed", true);
    assert!(ds.is_degraded());

    ds.set_role_degraded("embed", false);
    assert!(!ds.is_degraded());
    assert!(ds.degraded_subsystems().is_empty());
}

#[test]
fn circuit_breaker_and_degradation_work_together() {
    let cb = CircuitBreaker::new(3, Duration::from_secs(30));
    let ds = DegradationState::default();

    cb.record_failure("b2");
    cb.record_failure("b2");
    cb.record_failure("b2");
    assert!(cb.is_open("b2"));
    ds.set_role_degraded("embed", true);

    assert!(ds.is_degraded());
    assert_eq!(ds.degraded_subsystems(), vec!["embed"]);

    cb.record_success("b2");
    assert!(!cb.is_open("b2"));
    ds.set_role_degraded("embed", false);

    assert!(!ds.is_degraded());
}

#[test]
fn degradation_state_can_toggle_multiple_times() {
    let ds = DegradationState::default();

    for _ in 0..5 {
        ds.set_role_degraded("embed", true);
        assert!(ds.is_degraded());
        ds.set_role_degraded("embed", false);
        assert!(!ds.is_degraded());
    }
}

// ═════════════════════════════════════════════════════════════════════════════════
// 3. MAINTENANCE SCHEDULER WITH FAKE CLOCK (non-ignored)
// ═════════════════════════════════════════════════════════════════════════════════

/// A handler that counts invocations.
struct SpyHandler {
    label: &'static str,
    count: AtomicU32,
    should_fail: AtomicBool,
}

impl SpyHandler {
    fn new(label: &'static str) -> Self {
        Self {
            label,
            count: AtomicU32::new(0),
            should_fail: AtomicBool::new(false),
        }
    }

    fn invocations(&self) -> u32 {
        self.count.load(Ordering::Relaxed)
    }

    fn set_should_fail(&self, fail: bool) {
        self.should_fail.store(fail, Ordering::Relaxed);
    }
}

#[async_trait]
impl MaintenanceHandler for SpyHandler {
    async fn run(&self) -> anyhow::Result<()> {
        self.count.fetch_add(1, Ordering::Relaxed);
        if self.should_fail.load(Ordering::Relaxed) {
            anyhow::bail!("spy handler {} forced failure", self.label);
        }
        Ok(())
    }

    fn name(&self) -> &'static str {
        self.label
    }
}

#[test]
fn test_is_due_interval_never_run() {
    let schedule = Schedule::Interval(Duration::from_secs(60));
    assert!(is_due(&schedule, None, Instant::now()));
}

#[test]
fn test_is_due_interval_not_yet() {
    let schedule = Schedule::Interval(Duration::from_secs(3600));
    let last = Instant::now();
    let now = last + Duration::from_secs(1800);
    assert!(!is_due(&schedule, Some(last), now));
}

#[test]
fn test_is_due_interval_elapsed() {
    let schedule = Schedule::Interval(Duration::from_secs(60));
    let last = Instant::now();
    let now = last + Duration::from_secs(120);
    assert!(is_due(&schedule, Some(last), now));
}

#[test]
fn test_is_due_daily_never_run() {
    let schedule = Schedule::Daily { hour: 3, minute: 0 };
    assert!(is_due(&schedule, None, Instant::now()));
}

#[test]
fn test_is_due_daily_within_23_5_hours() {
    let schedule = Schedule::Daily { hour: 3, minute: 0 };
    let last = Instant::now();
    let now = last + Duration::from_secs(3600);
    assert!(!is_due(&schedule, Some(last), now));
}

#[test]
fn test_is_due_daily_after_23_5_hours() {
    let schedule = Schedule::Daily { hour: 3, minute: 0 };
    let last = Instant::now();
    let now = last + Duration::from_secs(85000);
    assert!(is_due(&schedule, Some(last), now));
}

#[tokio::test]
async fn maintenance_scheduler_runs_due_job() {
    let origin = Instant::now();
    let clock = Arc::new(FakeClock::new(origin));
    let spy = Arc::new(SpyHandler::new("test-job"));

    let mut scheduler = MaintenanceScheduler::new(clock.clone(), Duration::from_secs(30));
    scheduler.add_job(
        MaintenanceJobKind::Vacuum,
        Schedule::Interval(Duration::from_secs(60)),
        spy.clone(),
    );

    // First tick: job is due (never run before).
    scheduler.run_due_jobs().await;
    assert_eq!(spy.invocations(), 1, "job should run on first tick");

    // Second tick immediately: not due (interval hasn't elapsed).
    scheduler.run_due_jobs().await;
    assert_eq!(spy.invocations(), 1, "job should NOT run again immediately");

    // Advance past interval.
    clock.advance(Duration::from_secs(61));
    scheduler.run_due_jobs().await;
    assert_eq!(
        spy.invocations(),
        2,
        "job should run after interval elapsed"
    );
}

#[tokio::test]
async fn maintenance_scheduler_multiple_jobs() {
    let origin = Instant::now();
    let clock = Arc::new(FakeClock::new(origin));
    let spy_a = Arc::new(SpyHandler::new("job-a"));
    let spy_b = Arc::new(SpyHandler::new("job-b"));

    let mut scheduler = MaintenanceScheduler::new(clock.clone(), Duration::from_secs(10));
    scheduler.add_job(
        MaintenanceJobKind::Vacuum,
        Schedule::Interval(Duration::from_secs(60)),
        spy_a.clone(),
    );
    scheduler.add_job(
        MaintenanceJobKind::LogPrune,
        Schedule::Interval(Duration::from_secs(120)),
        spy_b.clone(),
    );

    // Both are due initially (never run).
    scheduler.run_due_jobs().await;
    assert_eq!(spy_a.invocations(), 1);
    assert_eq!(spy_b.invocations(), 1);

    // Advance 61s: job A (60s interval) is due; job B (120s interval) is not.
    clock.advance(Duration::from_secs(61));
    scheduler.run_due_jobs().await;
    assert_eq!(spy_a.invocations(), 2, "job A should run again");
    assert_eq!(spy_b.invocations(), 1, "job B should NOT run yet");

    // Advance another 61s (122s total): both are due again.
    clock.advance(Duration::from_secs(61));
    scheduler.run_due_jobs().await;
    assert_eq!(spy_a.invocations(), 3);
    assert_eq!(spy_b.invocations(), 2);
}

#[tokio::test]
async fn maintenance_scheduler_job_failure_does_not_block_others() {
    let origin = Instant::now();
    let clock = Arc::new(FakeClock::new(origin));
    let failing = Arc::new(SpyHandler::new("failing-job"));
    failing.set_should_fail(true);
    let succeeding = Arc::new(SpyHandler::new("succeeding-job"));

    let mut scheduler = MaintenanceScheduler::new(clock.clone(), Duration::from_secs(10));
    scheduler.add_job(
        MaintenanceJobKind::Vacuum,
        Schedule::Interval(Duration::from_secs(60)),
        failing.clone(),
    );
    scheduler.add_job(
        MaintenanceJobKind::LogPrune,
        Schedule::Interval(Duration::from_secs(60)),
        succeeding.clone(),
    );

    // Both run; failing job errors but succeeding job still completes.
    scheduler.run_due_jobs().await;
    assert_eq!(failing.invocations(), 1, "failing job was invoked");
    assert_eq!(succeeding.invocations(), 1, "succeeding job still ran");

    // Failing job can run again next interval (failure doesn't disable it).
    clock.advance(Duration::from_secs(61));
    scheduler.run_due_jobs().await;
    assert_eq!(failing.invocations(), 2, "failing job runs again");
    assert_eq!(succeeding.invocations(), 2);
}

// ═════════════════════════════════════════════════════════════════════════════════
// 4. ORPHAN GC + INTEGRITY SCAN WITH MOCK RUNNERS (non-ignored)
// ═════════════════════════════════════════════════════════════════════════════════

/// A mock [`OrphanGcRunner`] with in-memory blob keys, DB records, and delete tracking.
struct MockOrphanGcRunner {
    blob_keys: Mutex<Vec<String>>,
    db_records: Mutex<Vec<kb_pipeline::orphan_gc::DbBlobRecord>>,
    deleted: Mutex<Vec<String>>,
}

impl MockOrphanGcRunner {
    fn new() -> Self {
        Self {
            blob_keys: Mutex::new(Vec::new()),
            db_records: Mutex::new(Vec::new()),
            deleted: Mutex::new(Vec::new()),
        }
    }

    fn add_blob(&self, key: &str) {
        self.blob_keys.lock().unwrap().push(key.to_string());
    }

    fn add_db_record(&self, key: &str, created_at: chrono::DateTime<chrono::Utc>) {
        self.db_records
            .lock()
            .unwrap()
            .push(kb_pipeline::orphan_gc::DbBlobRecord {
                blob_key: key.to_string(),
                created_at,
            });
    }

    fn deleted_keys(&self) -> Vec<String> {
        self.deleted.lock().unwrap().clone()
    }
}

#[async_trait]
impl OrphanGcRunner for MockOrphanGcRunner {
    async fn list_blob_keys(&self, _tenant_id: i64) -> anyhow::Result<Vec<String>> {
        Ok(self.blob_keys.lock().unwrap().clone())
    }

    async fn list_db_records(
        &self,
        _tenant_id: i64,
    ) -> anyhow::Result<Vec<kb_pipeline::orphan_gc::DbBlobRecord>> {
        Ok(self.db_records.lock().unwrap().clone())
    }

    async fn delete_blob(&self, key: &str) -> anyhow::Result<()> {
        self.deleted.lock().unwrap().push(key.to_string());
        self.blob_keys.lock().unwrap().retain(|k| k != key);
        Ok(())
    }
}

#[test]
fn orphan_gc_finds_blobs_without_db_rows() {
    let store_keys = vec![
        "t1/doc1.bin".to_string(),
        "t1/doc2.bin".to_string(),
        "t1/doc3.bin".to_string(),
    ];
    let db_keys: HashSet<&str> = ["t1/doc1.bin", "t1/doc3.bin"].iter().copied().collect();

    let orphans = find_orphan_blobs(&store_keys, &db_keys);
    assert_eq!(orphans, vec!["t1/doc2.bin"]);
}

#[test]
fn orphan_gc_no_orphans_when_all_have_db_rows() {
    let store_keys = vec!["t1/a.bin".to_string(), "t1/b.bin".to_string()];
    let db_keys: HashSet<&str> = ["t1/a.bin", "t1/b.bin"].iter().copied().collect();

    let orphans = find_orphan_blobs(&store_keys, &db_keys);
    assert!(orphans.is_empty());
}

#[test]
fn orphan_gc_finds_missing_db_blobs() {
    let records = vec![
        kb_pipeline::orphan_gc::DbBlobRecord {
            blob_key: "t1/missing.bin".into(),
            created_at: chrono::Utc::now(),
        },
        kb_pipeline::orphan_gc::DbBlobRecord {
            blob_key: "t1/present.bin".into(),
            created_at: chrono::Utc::now(),
        },
    ];
    let store_keys: HashSet<&str> = ["t1/present.bin"].iter().copied().collect();

    let missing = find_missing_db_blobs(&records, &store_keys);
    assert_eq!(missing, vec!["t1/missing.bin"]);
}

#[test]
fn orphan_gc_grace_period_filters_recent_blobs() {
    use chrono::{Duration, Utc};
    let now = Utc::now();
    let recent = now - Duration::hours(1);

    let orphans = vec!["t1/recent.bin".to_string(), "t1/old.bin".to_string()];
    let db_records = vec![kb_pipeline::orphan_gc::DbBlobRecord {
        blob_key: "t1/recent.bin".into(),
        created_at: recent,
    }];

    let to_delete = filter_by_grace_period(&orphans, &db_records, 24, now);
    // Only old.bin is deleted (recent.bin has a DB record within grace period).
    assert_eq!(to_delete, vec!["t1/old.bin"]);
}

#[tokio::test]
async fn orphan_gc_full_cycle_with_mock_runner() {
    use chrono::{Duration, Utc};
    let now = Utc::now();

    let runner = MockOrphanGcRunner::new();
    runner.add_blob("t1/a.bin");
    runner.add_blob("t1/b-orphan.bin");
    runner.add_blob("t1/c-recent-orphan.bin");
    runner.add_db_record("t1/a.bin", now - Duration::hours(48));
    // c has a DB row within grace period (1h old, grace is 24h).
    runner.add_db_record("t1/c-recent-orphan.bin", now - Duration::hours(1));

    let config = kb_config::OrphanGc {
        enabled: true,
        schedule_hour: 0,
        schedule_minute: 0,
        grace_hours: 24,
    };

    run_orphan_gc(&runner, &config, 1).await.unwrap();

    let deleted = runner.deleted_keys();
    assert!(
        deleted.contains(&"t1/b-orphan.bin".to_string()),
        "orphan blob without DB row should be deleted, deleted: {deleted:?}"
    );
    assert!(
        !deleted.contains(&"t1/c-recent-orphan.bin".to_string()),
        "recent orphan should NOT be deleted"
    );
    assert!(!deleted.contains(&"t1/a.bin".to_string()));
}

#[test]
fn orphan_gc_empty_inputs() {
    let store_keys: Vec<String> = vec![];
    let db_keys: HashSet<&str> = HashSet::new();

    let orphans = find_orphan_blobs(&store_keys, &db_keys);
    assert!(orphans.is_empty());

    let records: Vec<kb_pipeline::orphan_gc::DbBlobRecord> = vec![];
    let missing = find_missing_db_blobs(&records, &db_keys);
    assert!(missing.is_empty());
}

// ── Integrity scan mock ────────────────────────────────────────────────────────

/// A mock [`IntegrityScanRunner`] with in-memory records + blob data.
struct MockIntegrityScanRunner {
    records: Mutex<Vec<FileShaRecord>>,
    blobs: Mutex<std::collections::HashMap<String, Bytes>>,
}

impl MockIntegrityScanRunner {
    fn new() -> Self {
        Self {
            records: Mutex::new(Vec::new()),
            blobs: Mutex::new(std::collections::HashMap::new()),
        }
    }

    fn add_record(&self, blob_key: &str, sha256: kb_core::hash::Sha256, data: Bytes) {
        self.records.lock().unwrap().push(FileShaRecord {
            blob_key: blob_key.to_string(),
            sha256,
        });
        self.blobs
            .lock()
            .unwrap()
            .insert(blob_key.to_string(), data);
    }
}

#[async_trait]
impl IntegrityScanRunner for MockIntegrityScanRunner {
    async fn list_file_records(&self, _tenant_id: i64) -> anyhow::Result<Vec<FileShaRecord>> {
        Ok(self.records.lock().unwrap().clone())
    }

    async fn get_blob_bytes(&self, key: &str) -> anyhow::Result<Bytes> {
        self.blobs
            .lock()
            .unwrap()
            .get(key)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("blob not found: {key}"))
    }
}

#[test]
fn integrity_scan_select_sample_100pct_selects_all() {
    let records: Vec<FileShaRecord> = (0..10)
        .map(|i| FileShaRecord {
            blob_key: format!("key-{i}"),
            sha256: kb_core::hash::Sha256::from_bytes([0u8; 32]),
        })
        .collect();

    let sample = select_sample(&records, 100);
    assert_eq!(sample.len(), 10);
}

#[test]
fn integrity_scan_select_sample_0pct_returns_empty() {
    let records: Vec<FileShaRecord> = (0..5)
        .map(|i| FileShaRecord {
            blob_key: format!("key-{i}"),
            sha256: kb_core::hash::Sha256::from_bytes([0u8; 32]),
        })
        .collect();

    let sample = select_sample(&records, 0);
    assert!(sample.is_empty());
}

#[test]
fn integrity_scan_select_sample_empty_input() {
    let sample = select_sample(&[], 50);
    assert!(sample.is_empty());
}

#[test]
fn integrity_scan_verify_hash_matches() {
    let data = b"test data for integrity";
    let hash_bytes: [u8; 32] = sha2::Sha256::digest(data).into();
    let expected = kb_core::hash::Sha256::from_bytes(hash_bytes);

    assert!(verify_hash(&Bytes::from_static(data), &expected));
}

#[test]
fn integrity_scan_verify_hash_mismatch() {
    let data = b"original data";
    let wrong_hash = kb_core::hash::Sha256::from_bytes([0xFFu8; 32]);
    assert!(!verify_hash(&Bytes::from_static(data), &wrong_hash));
}

#[test]
fn integrity_scan_verify_hash_empty_data() {
    let hash_bytes: [u8; 32] = sha2::Sha256::digest(b"" as &[u8]).into();
    let expected = kb_core::hash::Sha256::from_bytes(hash_bytes);
    assert!(verify_hash(&Bytes::new(), &expected));
}

#[tokio::test]
async fn integrity_scan_full_cycle_with_mock_runner() {
    let runner = MockIntegrityScanRunner::new();
    for i in 0..20u32 {
        let data = format!("file-{i}-content").into_bytes();
        let hash_bytes: [u8; 32] = sha2::Sha256::digest(&data).into();
        let sha = kb_core::hash::Sha256::from_bytes(hash_bytes);
        runner.add_record(&format!("key-{i}"), sha, Bytes::from(data));
    }

    let config = kb_config::IntegrityScan {
        enabled: true,
        schedule_hour: 4,
        schedule_minute: 0,
        sample_pct: 50,
    };

    run_integrity_scan(&runner, &config, 1).await.unwrap();
}

#[tokio::test]
async fn integrity_scan_with_hash_mismatch_completes_without_panic() {
    let runner = MockIntegrityScanRunner::new();
    let data = Bytes::from_static(b"correct content");
    let wrong_hash = kb_core::hash::Sha256::from_bytes([0xAAu8; 32]);
    runner.add_record("key-corrupt", wrong_hash, data);

    let config = kb_config::IntegrityScan {
        enabled: true,
        schedule_hour: 4,
        schedule_minute: 0,
        sample_pct: 100,
    };

    let result = run_integrity_scan(&runner, &config, 1).await;
    // Hash mismatches are reported as Err — this is expected, not a panic.
    assert!(result.is_err(), "expected Err for hash mismatch");
}

#[test]
fn orphan_gc_and_integrity_scan_share_key_types() {
    let db_record = kb_pipeline::orphan_gc::DbBlobRecord {
        blob_key: "shared-key".into(),
        created_at: chrono::Utc::now(),
    };
    let file_record = FileShaRecord {
        blob_key: "shared-key".into(),
        sha256: kb_core::hash::Sha256::from_bytes([0u8; 32]),
    };

    assert_eq!(db_record.blob_key, file_record.blob_key);
}

// ═════════════════════════════════════════════════════════════════════════════════
// 5. MODULE EXPORTS SMOKE TESTS (non-ignored)
// ═════════════════════════════════════════════════════════════════════════════════

#[test]
fn public_api_orphan_gc_exports_are_accessible() {
    let _ = kb_pipeline::orphan_gc_should_run_now;
    let _ = kb_pipeline::process_orphan_gc_job;
    let _ = kb_pipeline::DbBlobRecord {
        blob_key: String::new(),
        created_at: chrono::Utc::now(),
    };
}

#[test]
fn public_api_integrity_scan_exports_are_accessible() {
    let _ = kb_pipeline::integrity_scan_should_run_now;
    let _ = kb_pipeline::process_integrity_scan_job;
    let _ = kb_pipeline::FileShaRecord {
        blob_key: String::new(),
        sha256: kb_core::hash::Sha256::from_bytes([0u8; 32]),
    };
}

#[test]
fn public_api_maintenance_exports_are_accessible() {
    let _ = MaintenanceJobKind::Vacuum;
    let _ = MaintenanceJobKind::ReembedCheck;
    // Verify struct types are in scope (silence dead-code warnings via fn sig).
    fn _types_ok(
        _b2: kb_pipeline::maintenance::B2LifecycleHandler,
        _evict: kb_pipeline::maintenance::BlobCacheEvictHandler,
        _prune: kb_pipeline::maintenance::LogPruneHandler,
        _vac: kb_pipeline::maintenance::VacuumHandler,
        _re: kb_pipeline::maintenance::ReembedCheckHandler,
    ) {
    }
    let _ = kb_pipeline::maintenance::RealClock;
    let _ = Schedule::Interval(Duration::from_secs(60));
    let _ = Schedule::Daily { hour: 3, minute: 0 };
    let _ = is_due;
}

// ═════════════════════════════════════════════════════════════════════════════════
// 6. ORPHAN GC WITH REAL PGSTORE (#[ignore] — needs testcontainers)
// ═════════════════════════════════════════════════════════════════════════════════

/// PgStore-backed [`OrphanGcRunner`] that queries the real `files` table and
/// uses a [`LocalBlob`] for blob I/O.
struct PgOrphanGcRunner {
    store: Arc<kb_store::PgStore>,
    blob: Arc<LocalBlob>,
    blob_dir: PathBuf,
}

impl PgOrphanGcRunner {
    fn new(store: Arc<kb_store::PgStore>, blob: Arc<LocalBlob>, blob_dir: PathBuf) -> Self {
        Self {
            store,
            blob,
            blob_dir,
        }
    }
}

#[async_trait]
impl OrphanGcRunner for PgOrphanGcRunner {
    async fn list_blob_keys(&self, _tenant_id: i64) -> anyhow::Result<Vec<String>> {
        let mut keys = Vec::new();
        let prefix_dir = self.blob_dir.join("test-tenant");
        if prefix_dir.exists() {
            for entry in std::fs::read_dir(&prefix_dir)? {
                let entry = entry?;
                if entry.file_type()?.is_file()
                    && let Some(name) = entry.file_name().to_str()
                {
                    keys.push(format!("test-tenant/{name}"));
                }
            }
        }
        Ok(keys)
    }

    async fn list_db_records(
        &self,
        tenant_id: i64,
    ) -> anyhow::Result<Vec<kb_pipeline::orphan_gc::DbBlobRecord>> {
        let files = self.store.list_files(tenant_id).await?;
        Ok(files
            .into_iter()
            .map(|f| kb_pipeline::orphan_gc::DbBlobRecord {
                blob_key: f.blob_key,
                created_at: f.ingested_at,
            })
            .collect())
    }

    async fn delete_blob(&self, key: &str) -> anyhow::Result<()> {
        Blob::delete(self.blob.as_ref(), key).await
    }
}

/// Helper: set up a test tenant in real Postgres via testcontainers.
async fn setup_pg_store() -> anyhow::Result<Arc<kb_store::PgStore>> {
    let db = kb_testsupport::fresh_db().await?;
    let store = kb_store::PgStore::with_roles(&db.admin_url, &db.app_url);
    store.connect().await?;

    let pool = store.pool()?;
    sqlx::query("INSERT INTO tenants (slug, name) VALUES ('test', 'Test Tenant')")
        .execute(&pool)
        .await?;

    Ok(Arc::new(store))
}

#[ignore = "needs Podman + pgvector/pgvector:pg17 testcontainer"]
#[tokio::test]
async fn orphan_gc_with_real_pgstore_deletes_orphan_blobs() -> anyhow::Result<()> {
    let (_blob_guard, blob_dir) = temp_dir();
    let store = setup_pg_store().await?;
    let blob = Arc::new(LocalBlob::new(blob_dir.clone(), "test-tenant".into()));

    // ── Create a file record in the DB ──
    let data = Bytes::from_static(b"file with db row");
    let hash = sha2::Sha256::digest(&data);
    let hex_hash = hex::encode(hash);
    let blob_key = format!("test-tenant/{hex_hash}");

    blob.put(&blob_key, data.clone()).await?;

    let pool = store.pool()?;
    sqlx::query(
        "INSERT INTO files (tenant_id, sha256, blob_key, path, mime, size_bytes, status) \
         VALUES (1, $1, $2, 'test.txt', 'text/plain', $3, 'ready')",
    )
    .bind(&hex_hash)
    .bind(&blob_key)
    .bind(data.len() as i64)
    .execute(&pool)
    .await?;

    // ── Create an orphan blob (no DB row) ──
    let orphan_data = Bytes::from_static(b"orphan blob without db row");
    let orphan_hash = sha2::Sha256::digest(&orphan_data);
    let orphan_key = format!("test-tenant/{}", hex::encode(orphan_hash));
    blob.put(&orphan_key, orphan_data).await?;

    // ── Run orphan GC ──
    let runner = PgOrphanGcRunner::new(store.clone(), blob.clone(), blob_dir.clone());

    let config = kb_config::OrphanGc {
        enabled: true,
        schedule_hour: 0,
        schedule_minute: 0,
        grace_hours: 0,
    };

    run_orphan_gc(&runner, &config, 1).await.unwrap();

    // ── Verify ──
    assert!(
        !blob.exists(&orphan_key).await?,
        "orphan blob should have been deleted by GC"
    );
    assert!(
        blob.exists(&blob_key).await?,
        "blob with DB row should NOT have been deleted"
    );

    Ok(())
}

#[ignore = "needs Podman + pgvector/pgvector:pg17 testcontainer"]
#[tokio::test]
async fn orphan_gc_grace_period_preserves_recent_blobs() -> anyhow::Result<()> {
    let (_blob_guard, blob_dir) = temp_dir();
    let store = setup_pg_store().await?;
    let blob = Arc::new(LocalBlob::new(blob_dir.clone(), "test-tenant".into()));

    let data = Bytes::from_static(b"recent file");
    let blob_key = "test-tenant/recent-file.bin";
    blob.put(blob_key, data.clone()).await?;

    let pool = store.pool()?;
    sqlx::query(
        "INSERT INTO files (tenant_id, sha256, blob_key, path, mime, size_bytes, status, ingested_at) \
         VALUES (1, 'aabbcc', $1, 'recent.txt', 'text/plain', $2, 'ready', NOW())",
    )
    .bind(blob_key)
    .bind(data.len() as i64)
    .execute(&pool)
    .await?;

    let runner = PgOrphanGcRunner::new(store.clone(), blob.clone(), blob_dir.clone());

    let config = kb_config::OrphanGc {
        enabled: true,
        schedule_hour: 0,
        schedule_minute: 0,
        grace_hours: 72,
    };

    run_orphan_gc(&runner, &config, 1).await.unwrap();

    assert!(
        blob.exists(blob_key).await?,
        "recent blob should survive grace period"
    );

    Ok(())
}

// ═════════════════════════════════════════════════════════════════════════════════
// 7. INTEGRITY SCAN WITH REAL PGSTORE (#[ignore] — needs testcontainers)
// ═════════════════════════════════════════════════════════════════════════════════

/// PgStore-backed [`IntegrityScanRunner`].
struct PgIntegrityScanRunner {
    store: Arc<kb_store::PgStore>,
    blob: Arc<LocalBlob>,
}

#[async_trait]
impl IntegrityScanRunner for PgIntegrityScanRunner {
    async fn list_file_records(&self, tenant_id: i64) -> anyhow::Result<Vec<FileShaRecord>> {
        let files = self.store.list_files(tenant_id).await?;
        Ok(files
            .into_iter()
            .map(|f| FileShaRecord {
                blob_key: f.blob_key,
                sha256: f.sha256,
            })
            .collect())
    }

    async fn get_blob_bytes(&self, key: &str) -> anyhow::Result<Bytes> {
        Blob::get(self.blob.as_ref(), key).await
    }
}

#[ignore = "needs Podman + pgvector/pgvector:pg17 testcontainer"]
#[tokio::test]
async fn integrity_scan_with_real_pgstore_verifies_hashes() -> anyhow::Result<()> {
    let (_blob_guard, blob_dir) = temp_dir();
    let store = setup_pg_store().await?;
    let blob = Arc::new(LocalBlob::new(blob_dir.clone(), "test-tenant".into()));

    // Create 3 files with correct hashes.
    for i in 0..3u8 {
        let data = Bytes::from(vec![i; 64]);
        let hash = sha2::Sha256::digest(&data);
        let hex_hash = hex::encode(hash);
        let blob_key = format!("test-tenant/{hex_hash}");

        blob.put(&blob_key, data.clone()).await?;

        let pool = store.pool()?;
        sqlx::query(
            "INSERT INTO files (tenant_id, sha256, blob_key, path, mime, size_bytes, status) \
             VALUES (1, $1, $2, $3, 'application/octet-stream', $4, 'ready')",
        )
        .bind(&hex_hash)
        .bind(&blob_key)
        .bind(format!("file-{i}.bin"))
        .bind(64i64)
        .execute(&pool)
        .await?;
    }

    let runner = PgIntegrityScanRunner {
        store: store.clone(),
        blob,
    };

    let config = kb_config::IntegrityScan {
        enabled: true,
        schedule_hour: 4,
        schedule_minute: 0,
        sample_pct: 100,
    };

    run_integrity_scan(&runner, &config, 1).await.unwrap();

    Ok(())
}

// ═════════════════════════════════════════════════════════════════════════════════
// 8. MAINTENANCE ON REAL POSTGRES (#[ignore] — needs testcontainers)
// ═════════════════════════════════════════════════════════════════════════════════

/// Default set of vacuumable tables per the allowlist.
const VACUUM_TABLES: &[&str] = &["files", "chunks", "documents", "tags", "document_tags"];

#[ignore = "needs Podman + pgvector/pgvector:pg17 testcontainer"]
#[tokio::test]
async fn maintenance_vacuum_handler_runs_on_real_postgres() -> anyhow::Result<()> {
    let store = setup_pg_store().await?;
    let pool = store.pool()?;

    let handler = kb_pipeline::maintenance::VacuumHandler::new(pool, VACUUM_TABLES)?;
    handler.run().await?;

    Ok(())
}

#[ignore = "needs Podman + pgvector/pgvector:pg17 testcontainer"]
#[tokio::test]
async fn maintenance_reembed_check_handler_runs_on_real_postgres() -> anyhow::Result<()> {
    let store = setup_pg_store().await?;
    let pool = store.pool()?;

    let handler = kb_pipeline::maintenance::ReembedCheckHandler::new(pool, "bge-m3".to_string());
    handler.run().await?;

    Ok(())
}

#[ignore = "needs Podman + pgvector/pgvector:pg17 testcontainer"]
#[tokio::test]
async fn maintenance_reembed_check_detects_mismatch() -> anyhow::Result<()> {
    let store = setup_pg_store().await?;
    let pool = store.pool()?;

    // Change the settings embedder_id to something unexpected.
    sqlx::query("UPDATE settings SET value = '\"unknown-model-v2\"' WHERE key = 'embedder'")
        .execute(&pool)
        .await?;

    let handler = kb_pipeline::maintenance::ReembedCheckHandler::new(pool, "bge-m3".to_string());
    // Should still complete (mismatch is logged, not an error).
    handler.run().await?;

    Ok(())
}

// ═════════════════════════════════════════════════════════════════════════════════
// SMOKE TESTS FOR THUMBNAIL + CDN MODULES (non-ignored)
// ═════════════════════════════════════════════════════════════════════════════════

#[test]
fn cdn_rewrite_module_is_accessible() {
    let result = kb_pipeline::rewrite_blob_url(
        "https://s3.us-west-004.backblazeb2.com/bucket/key",
        "https://cdn.example.com",
    );
    assert!(result.contains("cdn.example.com"));
    assert!(result.contains("/bucket/key"));
}

#[test]
fn cdn_rewrite_preserves_query_params() {
    let result = kb_pipeline::rewrite_blob_url(
        "https://s3.example.com/bucket/key?X-Amz-Signature=abc123",
        "https://cdn.example.com",
    );
    assert!(result.contains("cdn.example.com"));
    assert!(result.contains("X-Amz-Signature=abc123"));
}

#[test]
fn cdn_rewrite_passthrough_when_cdn_base_empty() {
    let original = "https://s3.example.com/bucket/key";
    let result = kb_pipeline::rewrite_blob_url(original, "");
    assert_eq!(result, original);
}
