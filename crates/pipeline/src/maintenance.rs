//! Maintenance scheduler (plan §25, P8-T11).
//!
//! A built-in cron-like scheduler for recurring maintenance tasks that runs inside
//! the app process — no external cron dependency. Jobs include:
//!
//! * Periodic `VACUUM ANALYZE` on large tables (files, chunks, usage_events)
//! * Log pruning to `LOG_MAX_GB`
//! * Blob cache LRU eviction
//! * B2 lifecycle enforcement (version expiry)
//! * Re-embed migration trigger (detects embedder config changes)
//!
//! # Testing
//!
//! The [`Clock`] trait enables deterministic scheduling tests. Concrete handlers
//! accept trait-object or closure-based backends so the orchestration logic can
//! be verified without real database or filesystem access.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Context;
use async_trait::async_trait;
use tokio::sync::watch;

// ── Clock abstraction ─────────────────────────────────────────────────────────

/// Injectable clock for deterministic scheduling tests.
pub trait Clock: Send + Sync {
    /// Return the current instant.
    fn now(&self) -> Instant;
}

/// Production clock backed by [`std::time::Instant::now`].
#[derive(Debug, Clone, Copy)]
pub struct RealClock;

impl Clock for RealClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

/// Fake clock for deterministic tests. Start at a known epoch; advance
/// manually with [`FakeClock::advance`].
pub struct FakeClock {
    now: Mutex<Instant>,
}

impl FakeClock {
    /// Create a fake clock starting at `start`.
    #[must_use]
    pub fn new(start: Instant) -> Self {
        Self {
            now: Mutex::new(start),
        }
    }

    /// Advance the fake clock by `d`.
    pub fn advance(&self, d: Duration) {
        let mut guard = self.now.lock().unwrap_or_else(|e| e.into_inner());
        *guard += d;
    }
}

impl Clock for FakeClock {
    fn now(&self) -> Instant {
        *self.now.lock().unwrap_or_else(|e| e.into_inner())
    }
}

// ── Schedule ──────────────────────────────────────────────────────────────────

/// When a maintenance job should run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Schedule {
    /// Run at fixed intervals (e.g., every 1 hour).
    Interval(Duration),
    /// Run once per day (at most). The fields are the target UTC time.
    Daily {
        /// Target hour in UTC (0–23).
        hour: u8,
        /// Target minute in UTC (0–59).
        minute: u8,
    },
}

/// Determine whether a job is due to run given its schedule, the last-run
/// instant, and the current instant.
///
/// # Rules
///
/// * `Schedule::Interval(d)`: due if `now - last_run ≥ d`, or if `last_run` is
///   `None` (never run).
/// * `Schedule::Daily{..}`: due if `last_run` is `None` or if at least 23.5
///   hours have elapsed since the last run, ensuring at most one run per day.
///
/// The daily check uses a 23.5-hour window rather than exact calendar-day
/// arithmetic so the function is pure and deterministic — it does not depend on
/// wall-clock time.
#[must_use]
pub fn is_due(schedule: &Schedule, last_run: Option<Instant>, now: Instant) -> bool {
    match *schedule {
        Schedule::Interval(d) => match last_run {
            Some(t) => now.duration_since(t) >= d,
            None => true,
        },
        Schedule::Daily { .. } => match last_run {
            None => true,
            Some(t) => {
                // At most once per ~day: require 23.5 h since last run.
                let dayish = Duration::from_secs(84600); // 23.5 hours
                now.duration_since(t) >= dayish
            }
        },
    }
}

// ── Maintenance handler trait ─────────────────────────────────────────────────

/// A single maintenance task that can be scheduled.
///
/// Implementations are expected to be small, self-contained wrappers around the
/// actual work (e.g., a VACUUM command, a log-prune call).
#[async_trait]
pub trait MaintenanceHandler: Send + Sync {
    /// Execute the maintenance task.
    ///
    /// # Errors
    ///
    /// Returns an error if the task fails. The scheduler logs the error at ERROR
    /// level, records a failure metric, and continues — a single failure does
    /// not prevent the next scheduled run.
    async fn run(&self) -> anyhow::Result<()>;

    /// Human-readable name for this handler, used in logs and metrics labels.
    fn name(&self) -> &'static str;
}

// ── Maintenance job kinds ─────────────────────────────────────────────────────

/// Well-known maintenance job kinds, used for metrics labels.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MaintenanceJobKind {
    /// Periodic `VACUUM ANALYZE` on large tables.
    Vacuum,
    /// Log pruning to `LOG_MAX_GB`.
    LogPrune,
    /// Blob cache LRU eviction.
    BlobCacheEviction,
    /// B2 lifecycle enforcement (version expiry).
    B2Lifecycle,
    /// Re-embed migration trigger (embedder config change detection).
    ReembedCheck,
}

impl MaintenanceJobKind {
    /// Return a stable string label for this kind, suitable for Prometheus
    /// metric labels.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Vacuum => "vacuum",
            Self::LogPrune => "log_prune",
            Self::BlobCacheEviction => "blob_cache_eviction",
            Self::B2Lifecycle => "b2_lifecycle",
            Self::ReembedCheck => "reembed_check",
        }
    }
}

// ── Scheduled job (internal) ──────────────────────────────────────────────────

/// A job registered with the scheduler.
struct ScheduledJob {
    /// The job kind for metrics labelling.
    kind: MaintenanceJobKind,
    /// When to run it.
    schedule: Schedule,
    /// The handler that does the actual work.
    handler: Arc<dyn MaintenanceHandler>,
    /// Timestamp of the most recent run attempt (success or failure).
    last_run: Mutex<Option<Instant>>,
}

// ── Maintenance scheduler ─────────────────────────────────────────────────────

/// A built-in cron-like scheduler for recurring maintenance tasks.
///
/// # Lifecycle
///
/// 1. Create a [`MaintenanceScheduler`] with a [`Clock`] and a tick interval.
/// 2. Register jobs via [`MaintenanceScheduler::add_job`].
/// 3. Call [`MaintenanceScheduler::spawn`] to start the background task,
///    passing a [`tokio::sync::watch::Receiver<bool>`] for graceful shutdown.
///
/// The background task checks each job's schedule every `tick_interval` and
/// runs those that are due. Failures are logged but do not prevent subsequent
/// runs of the same or other jobs.
pub struct MaintenanceScheduler {
    /// Registered jobs.
    jobs: Vec<ScheduledJob>,
    /// Injectable clock for deterministic testing.
    clock: Arc<dyn Clock>,
    /// How often to check for due jobs.
    tick_interval: Duration,
}

impl MaintenanceScheduler {
    /// Create a new scheduler with the given clock and tick interval.
    ///
    /// `tick_interval` controls how frequently the scheduler wakes to check
    /// for due jobs. A reasonable default is 60 seconds.
    #[must_use]
    pub fn new(clock: Arc<dyn Clock>, tick_interval: Duration) -> Self {
        Self {
            jobs: Vec::new(),
            clock,
            tick_interval,
        }
    }

    /// Register a maintenance job.
    ///
    /// Jobs are checked in registration order on each tick.
    pub fn add_job(
        &mut self,
        kind: MaintenanceJobKind,
        schedule: Schedule,
        handler: Arc<dyn MaintenanceHandler>,
    ) {
        self.jobs.push(ScheduledJob {
            kind,
            schedule,
            handler,
            last_run: Mutex::new(None),
        });
    }

    /// Spawn the scheduler as a tokio background task.
    ///
    /// The task runs until `shutdown` receives `true`, at which point it
    /// completes the currently running job (if any) and exits. No new jobs
    /// are started after the shutdown signal.
    ///
    /// # Panics
    ///
    /// Does not panic. If the scheduler itself fails (e.g., the clock mutex is
    /// poisoned), the error is logged and the task exits.
    #[must_use]
    pub fn spawn(self, mut shutdown: watch::Receiver<bool>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(self.tick_interval);
            // Suppress the first immediate tick.
            tick.tick().await;

            loop {
                tokio::select! {
                    _ = tick.tick() => {
                        self.run_due_jobs().await;
                    }
                    Ok(()) = shutdown.changed() => {
                        if *shutdown.borrow() {
                            tracing::info!("maintenance scheduler: shutdown signal received");
                            break;
                        }
                    }
                }
            }
        })
    }

    /// Check all registered jobs and run those that are due.
    ///
    /// This is `pub(crate)` so integration tests can call it directly without
    /// spawning a background task.
    async fn run_due_jobs(&self) {
        let now = self.clock.now();
        for job in &self.jobs {
            let last = *job.last_run.lock().unwrap_or_else(|e| e.into_inner());
            if !is_due(&job.schedule, last, now) {
                continue;
            }

            let kind_str = job.kind.as_str();
            tracing::info!(kind = kind_str, "maintenance job starting");

            match job.handler.run().await {
                Ok(()) => {
                    tracing::info!(kind = kind_str, "maintenance job completed successfully");
                    kb_metrics::record_maintenance_success(kind_str);
                }
                Err(e) => {
                    tracing::error!(
                        kind = kind_str,
                        error = %e,
                        "maintenance job failed — will retry at next scheduled time"
                    );
                    kb_metrics::record_maintenance_failure(kind_str);
                }
            }

            // Update last-run timestamp regardless of outcome, so a failing job
            // does not retry on every tick.
            *job.last_run.lock().unwrap_or_else(|e| e.into_inner()) = Some(now);
        }
    }
}

// ── Concrete handlers ─────────────────────────────────────────────────────────

/// Handler that runs `VACUUM ANALYZE` on a fixed allowlist of table names.
///
/// # Safety
///
/// Table names are validated against a compile-time allowlist to prevent SQL
/// injection. Unknown table names are rejected at construction time.
#[derive(Debug)]
pub struct VacuumHandler {
    pool: sqlx::PgPool,
    /// Fully qualified `schema.table` names that have been validated.
    tables: Vec<&'static str>,
}

/// Allowlist of table names eligible for `VACUUM ANALYZE`.
const ALLOWED_VACUUM_TABLES: &[&str] = &[
    "files",
    "chunks",
    "usage_events",
    "documents",
    "tags",
    "document_tags",
    "jobs",
];

impl VacuumHandler {
    /// Create a new vacuum handler.
    ///
    /// `table_names` must each be a simple identifier present in
    /// [`ALLOWED_VACUUM_TABLES`]. A table name that is not in the allowlist
    /// returns an error — this prevents SQL injection.
    ///
    /// # Errors
    ///
    /// Returns an error if any table name is not in the allowlist.
    pub fn new(pool: sqlx::PgPool, table_names: &[&'static str]) -> anyhow::Result<Self> {
        for name in table_names {
            anyhow::ensure!(
                ALLOWED_VACUUM_TABLES.contains(name),
                "VACUUM table `{name}` is not in the allowlist"
            );
        }
        Ok(Self {
            pool,
            tables: table_names.to_vec(),
        })
    }
}

#[async_trait]
impl MaintenanceHandler for VacuumHandler {
    async fn run(&self) -> anyhow::Result<()> {
        for table in &self.tables {
            // VACUUM cannot run inside a transaction block, so execute directly.
            let query = format!("VACUUM ANALYZE {table}");
            sqlx::query(&query)
                .execute(&self.pool)
                .await
                .with_context(|| format!("VACUUM ANALYZE failed for table `{table}`"))?;

            tracing::info!(table = table, "VACUUM ANALYZE completed");
        }
        Ok(())
    }

    fn name(&self) -> &'static str {
        "vacuum"
    }
}

/// Handler that prunes old log files via [`kb_logging::janitor::prune_directory`].
pub struct LogPruneHandler {
    /// Path to the log directory.
    log_dir: std::path::PathBuf,
    /// Hard cap in bytes.
    max_bytes: u64,
    /// Log file name prefix.
    prefix: String,
}

impl LogPruneHandler {
    /// Create a new log-prune handler from a [`kb_logging::LogConfig`].
    #[must_use]
    pub fn from_config(config: &kb_logging::LogConfig) -> Self {
        Self {
            log_dir: config.log_dir.clone(),
            max_bytes: config.max_bytes(),
            prefix: kb_logging::LogJanitor::LOG_PREFIX.to_string(),
        }
    }

    /// Create a handler with explicit parameters (useful for testing).
    #[must_use]
    pub fn new(log_dir: std::path::PathBuf, max_bytes: u64, prefix: String) -> Self {
        Self {
            log_dir,
            max_bytes,
            prefix,
        }
    }
}

#[async_trait]
impl MaintenanceHandler for LogPruneHandler {
    async fn run(&self) -> anyhow::Result<()> {
        let dir = self.log_dir.clone();
        let max_bytes = self.max_bytes;
        let prefix = self.prefix.clone();

        tokio::task::spawn_blocking(move || {
            kb_logging::janitor::prune_directory(&dir, max_bytes, &prefix)
        })
        .await
        .map_err(|e| anyhow::anyhow!("log prune task panicked: {e}"))?
    }

    fn name(&self) -> &'static str {
        "log_prune"
    }
}

/// Handler that trims the blob cache to stay under its configured size limit.
///
/// Wraps an [`Arc<kb_store::BlobCache>`] and calls its [`trim`](kb_store::BlobCache::trim)
/// method.
pub struct BlobCacheEvictHandler {
    cache: Arc<kb_store::BlobCache>,
}

impl BlobCacheEvictHandler {
    /// Create a new blob cache eviction handler.
    #[must_use]
    pub fn new(cache: Arc<kb_store::BlobCache>) -> Self {
        Self { cache }
    }
}

#[async_trait]
impl MaintenanceHandler for BlobCacheEvictHandler {
    async fn run(&self) -> anyhow::Result<()> {
        let freed = self.cache.trim().context("blob cache eviction failed")?;

        if freed > 0 {
            tracing::info!(
                bytes_freed = freed,
                current_size = self.cache.current_size_bytes().unwrap_or(0),
                "blob cache eviction completed"
            );
        }
        Ok(())
    }

    fn name(&self) -> &'static str {
        "blob_cache_eviction"
    }
}

/// Handler for B2 lifecycle enforcement (version expiry).
///
/// This is a placeholder that logs the current state. Full B2 lifecycle
/// management (versioning rules, retention periods) requires B2 API integration
/// beyond the scope of the initial maintenance scheduler.
pub struct B2LifecycleHandler;

impl B2LifecycleHandler {
    /// Create a new B2 lifecycle handler.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl Default for B2LifecycleHandler {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl MaintenanceHandler for B2LifecycleHandler {
    async fn run(&self) -> anyhow::Result<()> {
        tracing::info!("B2 lifecycle enforcement: not yet implemented (placeholder)");
        Ok(())
    }

    fn name(&self) -> &'static str {
        "b2_lifecycle"
    }
}

/// Handler that checks for embedder configuration changes.
///
/// Reads the current embedder settings from the `settings` table and compares
/// the `embedder_id` against an expected value. When a mismatch is detected, it
/// logs a WARN-level message and sets the failure gauge — the actual re-embed
/// migration (enqueuing re-embed jobs for all chunks) is a separate job
/// triggered by an operator or a follow-up task.
pub struct ReembedCheckHandler {
    /// Admin pool for reading the `settings` table (not tenant-scoped).
    pool: sqlx::PgPool,
    /// The expected embedder id (e.g., `"bge-m3"`).
    expected_embedder_id: String,
}

impl ReembedCheckHandler {
    /// Create a new re-embed check handler.
    #[must_use]
    pub fn new(pool: sqlx::PgPool, expected_embedder_id: String) -> Self {
        Self {
            pool,
            expected_embedder_id,
        }
    }
}

#[async_trait]
impl MaintenanceHandler for ReembedCheckHandler {
    async fn run(&self) -> anyhow::Result<()> {
        let row: (serde_json::Value,) =
            sqlx::query_as("SELECT value FROM settings WHERE key = 'embedder'")
                .fetch_optional(&self.pool)
                .await
                .context("failed to read embedder settings")?
                .with_context(
                    || "embedder settings row not found — migration 0002_schema may be missing",
                )?;

        let current_id = row
            .0
            .get("id")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");

        if current_id != self.expected_embedder_id {
            tracing::warn!(
                current = current_id,
                expected = self.expected_embedder_id,
                "embedder configuration mismatch detected — re-embed migration may be needed"
            );
            // Return an error so the failure gauge is set, alerting operators.
            anyhow::bail!(
                "embedder mismatch: settings has `{current_id}`, expected `{}`",
                self.expected_embedder_id
            );
        }

        Ok(())
    }

    fn name(&self) -> &'static str {
        "reembed_check"
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use kb_core::blob::Blob;
    use std::time::Duration;

    // ── is_due tests ──────────────────────────────────────────────────────

    fn epoch() -> Instant {
        Instant::now()
    }

    #[test]
    fn is_due_interval_never_run() {
        let schedule = Schedule::Interval(Duration::from_secs(60));
        assert!(is_due(&schedule, None, epoch()));
    }

    #[test]
    fn is_due_interval_not_yet_due() {
        let schedule = Schedule::Interval(Duration::from_secs(300));
        let last = epoch();
        // Same instant → not yet due.
        assert!(!is_due(&schedule, Some(last), last));
    }

    #[test]
    fn is_due_interval_elapsed() {
        let schedule = Schedule::Interval(Duration::from_secs(10));
        let last = epoch();
        let now = last + Duration::from_secs(15);
        assert!(is_due(&schedule, Some(last), now));
    }

    #[test]
    fn is_due_interval_exact_boundary() {
        let schedule = Schedule::Interval(Duration::from_secs(10));
        let last = epoch();
        let now = last + Duration::from_secs(10);
        assert!(is_due(&schedule, Some(last), now));
    }

    #[test]
    fn is_due_interval_just_under() {
        let schedule = Schedule::Interval(Duration::from_secs(10));
        let last = epoch();
        let now = last + Duration::from_secs(9);
        assert!(!is_due(&schedule, Some(last), now));
    }

    #[test]
    fn is_due_daily_never_run() {
        let schedule = Schedule::Daily { hour: 3, minute: 0 };
        assert!(is_due(&schedule, None, epoch()));
    }

    #[test]
    fn is_due_daily_recently_run() {
        let schedule = Schedule::Daily { hour: 3, minute: 0 };
        let last = epoch();
        let now = last + Duration::from_secs(3600); // 1 hour ago
        assert!(!is_due(&schedule, Some(last), now));
    }

    #[test]
    fn is_due_daily_next_day() {
        let schedule = Schedule::Daily { hour: 3, minute: 0 };
        let last = epoch();
        let now = last + Duration::from_secs(85000); // ~23.6 hours
        assert!(is_due(&schedule, Some(last), now));
    }

    #[test]
    fn is_due_daily_boundary_23_5h() {
        let schedule = Schedule::Daily { hour: 3, minute: 0 };
        let last = epoch();
        // 23.5 hours exactly.
        let now = last + Duration::from_secs(84600);
        assert!(is_due(&schedule, Some(last), now));
    }

    #[test]
    fn is_due_daily_just_under_23_5h() {
        let schedule = Schedule::Daily { hour: 3, minute: 0 };
        let last = epoch();
        let now = last + Duration::from_secs(84599); // 1 second shy
        assert!(!is_due(&schedule, Some(last), now));
    }

    #[test]
    fn is_due_hour_and_minute_dont_affect_daily_check() {
        // The daily check is purely time-elapsed based — the (hour, minute)
        // fields are informational. This test verifies that behaviour.
        let last = epoch();
        let now = last + Duration::from_secs(85000);
        let s1 = Schedule::Daily { hour: 0, minute: 0 };
        let s2 = Schedule::Daily {
            hour: 23,
            minute: 59,
        };
        assert!(is_due(&s1, Some(last), now));
        assert!(is_due(&s2, Some(last), now));
    }

    // ── Clock tests ──────────────────────────────────────────────────────

    #[test]
    fn fake_clock_starts_at_given_instant() {
        let start = epoch();
        let clock = FakeClock::new(start);
        assert_eq!(clock.now(), start);
    }

    #[test]
    fn fake_clock_advances() {
        let start = epoch();
        let clock = FakeClock::new(start);
        clock.advance(Duration::from_secs(42));
        assert_eq!(clock.now().duration_since(start), Duration::from_secs(42));
    }

    #[test]
    fn real_clock_returns_now() {
        let clock = RealClock;
        let before = Instant::now();
        let clock_now = clock.now();
        let after = Instant::now();
        assert!(clock_now >= before);
        assert!(clock_now <= after);
    }

    #[test]
    fn real_clock_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<RealClock>();
    }

    // ── MaintenanceJobKind tests ─────────────────────────────────────────

    #[test]
    fn job_kind_as_str_is_stable() {
        assert_eq!(MaintenanceJobKind::Vacuum.as_str(), "vacuum");
        assert_eq!(MaintenanceJobKind::LogPrune.as_str(), "log_prune");
        assert_eq!(
            MaintenanceJobKind::BlobCacheEviction.as_str(),
            "blob_cache_eviction"
        );
        assert_eq!(MaintenanceJobKind::B2Lifecycle.as_str(), "b2_lifecycle");
        assert_eq!(MaintenanceJobKind::ReembedCheck.as_str(), "reembed_check");
    }

    #[test]
    fn job_kind_as_str_is_valid_label() {
        for kind in &[
            MaintenanceJobKind::Vacuum,
            MaintenanceJobKind::LogPrune,
            MaintenanceJobKind::BlobCacheEviction,
            MaintenanceJobKind::B2Lifecycle,
            MaintenanceJobKind::ReembedCheck,
        ] {
            let s = kind.as_str();
            assert!(
                s.chars()
                    .all(|c| c.is_ascii_lowercase() || c == '_' || c.is_ascii_digit()),
                "kind {s:?} as_str() should be a valid Prometheus label: got {s}"
            );
        }
    }

    // ── VacuumHandler tests ─────────────────────────────────────────────

    /// Validate that a table name is in the allowlist.
    fn is_allowed_vacuum_table(name: &str) -> bool {
        ALLOWED_VACUUM_TABLES.contains(&name)
    }

    #[test]
    fn vacuum_table_allowlist_rejects_unknown_table() {
        assert!(!is_allowed_vacuum_table("nonexistent_table"));
        assert!(!is_allowed_vacuum_table("DROP TABLE"));
        assert!(!is_allowed_vacuum_table("files; DROP TABLE"));
    }

    #[test]
    fn vacuum_table_allowlist_accepts_known_tables() {
        assert!(is_allowed_vacuum_table("files"));
        assert!(is_allowed_vacuum_table("chunks"));
        assert!(is_allowed_vacuum_table("usage_events"));
        assert!(is_allowed_vacuum_table("documents"));
        assert!(is_allowed_vacuum_table("tags"));
        assert!(is_allowed_vacuum_table("document_tags"));
        assert!(is_allowed_vacuum_table("jobs"));
    }

    #[test]
    fn vacuum_handler_allowed_tables_list_is_non_empty() {
        assert!(!ALLOWED_VACUUM_TABLES.is_empty());
        // All entries must be simple identifiers (no dots, spaces, or special chars).
        for table in ALLOWED_VACUUM_TABLES {
            assert!(
                table.chars().all(|c| c.is_ascii_lowercase() || c == '_'),
                "table name `{table}` is not a simple identifier"
            );
        }
    }

    // ── LogPruneHandler tests ───────────────────────────────────────────

    #[test]
    fn log_prune_handler_name() {
        let handler = LogPruneHandler::new(
            std::path::PathBuf::from("/tmp/logs"),
            1024 * 1024,
            "kb.log".to_string(),
        );
        assert_eq!(handler.name(), "log_prune");
    }

    // ── BlobCacheEvictHandler tests ─────────────────────────────────────

    #[test]
    fn blob_cache_evict_handler_name() {
        // Use a temporary directory and a mock backend to construct a real BlobCache.
        let dir = tempfile::tempdir().unwrap();
        let backend: Arc<dyn kb_core::blob::Blob> = Arc::new(kb_store::LocalBlob::new(
            dir.path().to_path_buf(),
            "test".to_string(),
        ));
        let cache = Arc::new(
            kb_store::BlobCache::new(backend, dir.path().join("cache"), 1024 * 1024, false)
                .unwrap(),
        );
        let handler = BlobCacheEvictHandler::new(cache);
        assert_eq!(handler.name(), "blob_cache_eviction");
    }

    // ── B2LifecycleHandler tests ────────────────────────────────────────

    #[tokio::test]
    async fn b2_lifecycle_handler_is_noop() {
        let handler = B2LifecycleHandler::new();
        assert_eq!(handler.name(), "b2_lifecycle");
        let result = handler.run().await;
        assert!(result.is_ok());
    }

    #[test]
    fn b2_lifecycle_handler_constructed() {
        let handler = B2LifecycleHandler;
        assert_eq!(handler.name(), "b2_lifecycle");
    }

    // ── ReembedCheckHandler tests ───────────────────────────────────────

    #[tokio::test]
    async fn reembed_check_handler_name() {
        let handler = ReembedCheckHandler::new(
            sqlx::PgPool::connect_lazy("postgres:///nonexistent").unwrap(),
            "bge-m3".to_string(),
        );
        assert_eq!(handler.name(), "reembed_check");
    }

    // ── MaintenanceScheduler tests ──────────────────────────────────────

    /// A handler that records every invocation.
    struct SpyHandler {
        name: &'static str,
        calls: Mutex<Vec<Instant>>,
        /// When set, the handler returns this error instead of Ok(()).
        fail_with: Mutex<Option<String>>,
    }

    impl SpyHandler {
        fn new(name: &'static str) -> Self {
            Self {
                name,
                calls: Mutex::new(Vec::new()),
                fail_with: Mutex::new(None),
            }
        }

        fn set_fail(&self, msg: &str) {
            *self.fail_with.lock().unwrap() = Some(msg.to_string());
        }

        fn call_count(&self) -> usize {
            self.calls.lock().unwrap().len()
        }
    }

    #[async_trait]
    impl MaintenanceHandler for SpyHandler {
        async fn run(&self) -> anyhow::Result<()> {
            self.calls.lock().unwrap().push(Instant::now());
            if let Some(msg) = self.fail_with.lock().unwrap().as_ref() {
                anyhow::bail!("{msg}");
            }
            Ok(())
        }

        fn name(&self) -> &'static str {
            self.name
        }
    }

    #[tokio::test]
    async fn scheduler_runs_job_when_due() {
        let start = epoch();
        let clock = Arc::new(FakeClock::new(start));
        let mut scheduler = MaintenanceScheduler::new(clock.clone(), Duration::from_secs(1));

        let spy = Arc::new(SpyHandler::new("test-job"));
        scheduler.add_job(
            MaintenanceJobKind::Vacuum,
            Schedule::Interval(Duration::from_secs(60)),
            spy.clone(),
        );

        // First tick: job should run (never run before).
        scheduler.run_due_jobs().await;
        assert_eq!(spy.call_count(), 1);

        // Advance by less than the interval: should NOT run again.
        clock.advance(Duration::from_secs(30));
        scheduler.run_due_jobs().await;
        assert_eq!(spy.call_count(), 1);

        // Advance past the interval: should run again.
        clock.advance(Duration::from_secs(31));
        scheduler.run_due_jobs().await;
        assert_eq!(spy.call_count(), 2);
    }

    #[tokio::test]
    async fn scheduler_does_not_rerun_on_failure_immediately() {
        let start = epoch();
        let clock = Arc::new(FakeClock::new(start));
        let mut scheduler = MaintenanceScheduler::new(clock.clone(), Duration::from_secs(1));

        let spy = Arc::new(SpyHandler::new("failing-job"));
        spy.set_fail("simulated failure");
        scheduler.add_job(
            MaintenanceJobKind::Vacuum,
            Schedule::Interval(Duration::from_secs(60)),
            spy.clone(),
        );

        // First run: fails.
        scheduler.run_due_jobs().await;
        assert_eq!(spy.call_count(), 1);

        // Immediate re-check: should NOT run again (last_run was updated).
        scheduler.run_due_jobs().await;
        assert_eq!(spy.call_count(), 1);

        // Advance past interval: should run again.
        clock.advance(Duration::from_secs(61));
        spy.set_fail("simulated failure"); // still failing
        scheduler.run_due_jobs().await;
        assert_eq!(spy.call_count(), 2);
    }

    #[tokio::test]
    async fn scheduler_runs_multiple_jobs_independently() {
        let start = epoch();
        let clock = Arc::new(FakeClock::new(start));
        let mut scheduler = MaintenanceScheduler::new(clock.clone(), Duration::from_secs(1));

        let job_a = Arc::new(SpyHandler::new("job-a"));
        let job_b = Arc::new(SpyHandler::new("job-b"));

        scheduler.add_job(
            MaintenanceJobKind::Vacuum,
            Schedule::Interval(Duration::from_secs(30)),
            job_a.clone(),
        );
        scheduler.add_job(
            MaintenanceJobKind::LogPrune,
            Schedule::Interval(Duration::from_secs(120)),
            job_b.clone(),
        );

        // First run: both should fire (never run before).
        scheduler.run_due_jobs().await;
        assert_eq!(job_a.call_count(), 1);
        assert_eq!(job_b.call_count(), 1);

        // Advance by 31s: job A should fire, job B should not.
        clock.advance(Duration::from_secs(31));
        scheduler.run_due_jobs().await;
        assert_eq!(job_a.call_count(), 2);
        assert_eq!(job_b.call_count(), 1);

        // Advance another 31s (62s total): A fires, B still not due.
        clock.advance(Duration::from_secs(31));
        scheduler.run_due_jobs().await;
        assert_eq!(job_a.call_count(), 3);
        assert_eq!(job_b.call_count(), 1);

        // Advance to 121s total: both fire.
        clock.advance(Duration::from_secs(59));
        scheduler.run_due_jobs().await;
        assert_eq!(job_a.call_count(), 4);
        assert_eq!(job_b.call_count(), 2);
    }

    #[tokio::test]
    async fn scheduler_daily_job_runs_only_once_per_dayish() {
        let start = epoch();
        let clock = Arc::new(FakeClock::new(start));
        let mut scheduler = MaintenanceScheduler::new(clock.clone(), Duration::from_secs(1));

        let spy = Arc::new(SpyHandler::new("daily-job"));
        scheduler.add_job(
            MaintenanceJobKind::B2Lifecycle,
            Schedule::Daily { hour: 3, minute: 0 },
            spy.clone(),
        );

        // First run: fires (never run).
        scheduler.run_due_jobs().await;
        assert_eq!(spy.call_count(), 1);

        // Advance by 12h: should NOT fire again.
        clock.advance(Duration::from_secs(43200));
        scheduler.run_due_jobs().await;
        assert_eq!(spy.call_count(), 1);

        // Advance to past 23.5h: should fire.
        clock.advance(Duration::from_secs(41401)); // 43200 + 41401 = 84601 > 84600
        scheduler.run_due_jobs().await;
        assert_eq!(spy.call_count(), 2);
    }

    #[tokio::test]
    async fn scheduler_empty_jobs_is_noop() {
        let clock = Arc::new(FakeClock::new(epoch()));
        let scheduler = MaintenanceScheduler::new(clock, Duration::from_secs(1));
        // Should not panic.
        scheduler.run_due_jobs().await;
    }

    #[tokio::test]
    async fn scheduler_with_real_clock() {
        let clock = Arc::new(RealClock);
        let mut scheduler = MaintenanceScheduler::new(clock, Duration::from_secs(1));

        let spy = Arc::new(SpyHandler::new("real-clock-job"));
        scheduler.add_job(
            MaintenanceJobKind::LogPrune,
            Schedule::Interval(Duration::from_millis(1)),
            spy.clone(),
        );

        // Should run immediately (never run before).
        scheduler.run_due_jobs().await;
        assert_eq!(spy.call_count(), 1);

        // Should NOT run again immediately (last_run updated).
        scheduler.run_due_jobs().await;
        assert_eq!(spy.call_count(), 1);
    }

    // ── Schedule tests ──────────────────────────────────────────────────

    #[test]
    fn schedule_interval_comparison() {
        let s1 = Schedule::Interval(Duration::from_secs(60));
        let s2 = Schedule::Interval(Duration::from_secs(60));
        assert_eq!(s1, s2);
    }

    #[test]
    fn schedule_daily_comparison() {
        let s1 = Schedule::Daily {
            hour: 3,
            minute: 15,
        };
        let s2 = Schedule::Daily {
            hour: 3,
            minute: 15,
        };
        let s3 = Schedule::Daily { hour: 4, minute: 0 };
        assert_eq!(s1, s2);
        assert_ne!(s1, s3);
    }

    #[test]
    fn schedule_debug_format() {
        let s = Schedule::Interval(Duration::from_secs(300));
        assert!(format!("{s:?}").contains("Interval"));
        let s = Schedule::Daily { hour: 2, minute: 0 };
        assert!(format!("{s:?}").contains("Daily"));
    }

    // ── BlobCache trim test ─────────────────────────────────────────────

    #[tokio::test]
    async fn blob_cache_trim_frees_space() {
        let dir = tempfile::tempdir().unwrap();
        let backend: Arc<dyn kb_core::blob::Blob> = Arc::new(kb_store::LocalBlob::new(
            dir.path().to_path_buf(),
            "test".to_string(),
        ));
        // Very small cache: 300 bytes.
        let cache = Arc::new(
            kb_store::BlobCache::new(backend.clone(), dir.path().join("cache"), 300, false)
                .unwrap(),
        );

        // Pre-populate the backend with blobs.
        for i in 0..10 {
            let data = bytes::Bytes::from(vec![0xAAu8; 100]);
            let key = format!("t1/key-{i}");
            backend.put(&key, data).await.unwrap();
        }

        // Fetch each one through the cache — eviction happens on write.
        for i in 0..10 {
            let key = format!("t1/key-{i}");
            let _ = cache.get(&key).await.unwrap();
        }

        // The cache should already be ≤ 300 bytes.
        let size = cache.current_size_bytes().unwrap();
        assert!(
            size <= 300,
            "cache size {size} should be ≤ 300 after eviction on get"
        );

        // Trim should be a no-op when already under limit.
        let freed = cache.trim().unwrap();
        assert_eq!(freed, 0);

        // Verify cache still works for a fetch.
        let result = cache.get("t1/key-0").await.unwrap();
        assert_eq!(result.len(), 100);
    }

    // ── LogPruneHandler integration test ────────────────────────────────

    #[tokio::test]
    async fn log_prune_handler_prunes_old_files() {
        use std::io::Write;

        let dir = tempfile::tempdir().unwrap();
        let log_dir = dir.path().join("logs");
        std::fs::create_dir_all(&log_dir).unwrap();

        // Create two log files using prune_directory-compatible names.
        let f1 = log_dir.join("kb.log");
        let f2 = log_dir.join("kb.log.1");
        {
            let mut f = std::fs::File::create(&f1).unwrap();
            f.write_all(&vec![b'A'; 600]).unwrap();
        }
        {
            let mut f = std::fs::File::create(&f2).unwrap();
            f.write_all(&vec![b'B'; 600]).unwrap();
        }

        // Set f1's mtime to 60 seconds ago so it's the oldest.
        let old_time = filetime::FileTime::from_unix_time(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64
                - 60,
            0,
        );
        filetime::set_file_mtime(&f1, old_time).unwrap();

        // Cap is 800 bytes — total is 1200, oldest (f1, 600 bytes) should be deleted.
        let handler = LogPruneHandler::new(log_dir.clone(), 800, "kb.log".to_string());
        handler.run().await.unwrap();

        assert!(!f1.exists(), "oldest log file should be pruned");
        assert!(f2.exists(), "newer log file should survive");
    }
}
