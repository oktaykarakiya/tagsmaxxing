// SPDX-License-Identifier: AGPL-3.0-or-later

//! Typed configuration model parsed from `config.toml` (plan §6).

use kb_core::role::Role;
use serde::Deserialize;

/// Default HTTP API port (workspace fact: the API serves on 9999).
pub const DEFAULT_PORT: u16 = 9999;

const fn default_port() -> u16 {
    DEFAULT_PORT
}
const fn default_secure_cookies() -> bool {
    true
}
const fn default_acquire_timeout_secs() -> u64 {
    30
}
const fn default_health_interval_secs() -> u64 {
    10
}
const fn default_max_retries() -> u32 {
    3
}
const fn default_slots() -> u32 {
    1
}

/// Top-level configuration for the knowledge-base services.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct Config {
    /// Durable-storage settings (Postgres is the only durable state).
    #[serde(default)]
    pub storage: Storage,
    /// HTTP API settings.
    #[serde(default)]
    pub api: Api,
    /// Model-scheduler settings (plan §6).
    #[serde(default)]
    pub scheduler: Scheduler,
    /// Inference backends; adding a machine is one `[[backend]]` entry (plan §6).
    #[serde(default, rename = "backend")]
    pub backends: Vec<Backend>,
    /// Folder-watcher settings (plan §10, P6-T3).
    #[serde(default, rename = "folder_watch")]
    pub folder_watch: FolderWatch,
    /// Automated restore-test settings (plan §21, P8-T7).
    #[serde(default, rename = "restore_test")]
    pub restore_test: RestoreTest,
    /// Graceful-degradation settings (plan §22, P8-T9).
    #[serde(default, rename = "degradation")]
    pub degradation: Degradation,
    /// Orphan GC job settings (plan §23, P8-T10).
    #[serde(default, rename = "orphan_gc")]
    pub orphan_gc: OrphanGc,
    /// Integrity scan job settings (plan §23, P8-T10).
    #[serde(default, rename = "integrity_scan")]
    pub integrity_scan: IntegrityScan,
    /// Thumbnail generation settings (plan §20, P8-T13).
    #[serde(default, rename = "thumbnail")]
    pub thumbnail: Thumbnail,
    /// Blob-store backend selection (plan §20, P15-T4).
    #[serde(default, rename = "blob")]
    pub blob: BlobConfig,
    /// Ingest-job worker settings (plan §16, P15-T4).
    #[serde(default, rename = "worker")]
    pub worker: Worker,
    /// Document source-sync settings: periodic URL re-fetch, diff, and versioning
    /// (P17). The global `enabled` flag gates both the scanner loop and the
    /// URL-ingest handler.
    #[serde(default, rename = "source_sync")]
    pub source_sync: SourceSync,
    /// Upload-ingestion mode + queue admission bounds (plan §16, P15-T4).
    #[serde(default, rename = "ingest")]
    pub ingest: Ingest,
    /// AI assistant settings (opencode binary, model, budgets).
    #[serde(default, rename = "assistant")]
    pub assistant: Assistant,
    /// Multi-turn chat settings (P18).
    #[serde(default, rename = "chat")]
    pub chat: ChatConfig,
}

/// AI assistant configuration section.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct Assistant {
    /// Path to the `opencode` CLI binary. When empty, read from `OPENCODE_BIN` env var.
    #[serde(default)]
    pub opencode_bin: Option<String>,
    /// Model reference for agent tasks, e.g. `"local/qwen-35b"`.
    /// When empty, read from `ASSISTANT_MODEL_REF` env var.
    #[serde(default)]
    pub model_ref: Option<String>,
    /// Maximum subprocess runtime per prompt in seconds. Default 300.
    #[serde(default)]
    pub prompt_timeout_secs: Option<u64>,
    /// Max fraction (percent) of context window for augmented prompts. Default 85.
    #[serde(default)]
    pub context_budget_pct: Option<u8>,
}

/// Durable-storage configuration.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct Storage {
    /// **Privileged** Postgres connection URL (owner/superuser): runs migrations, the
    /// cross-tenant job queue, and admin/usage roll-ups. Required; may be supplied via the
    /// `POSTGRES_URL` env var. Both URLs are read at connect time so rotation needs no restart
    /// (the hot-swap rule, CLAUDE.md).
    #[serde(default)]
    pub postgres_url: String,
    /// **Application** Postgres connection URL — the non-privileged `kb_app` role
    /// (`NOSUPERUSER NOBYPASSRLS`) used for all tenant-scoped
    /// data so Row-Level Security is enforced (P6-T14, §13). May be supplied via the
    /// `APP_POSTGRES_URL` env var. When empty, the store falls back to [`postgres_url`]
    /// (single-role mode — RLS then relies on the explicit `tenant_id` filters only).
    #[serde(default)]
    pub app_postgres_url: String,
    /// Optional CDN base URL for blob egress (plan §20, P8-T13).
    ///
    /// When set, presigned B2 URLs are rewritten through this CDN (e.g. Cloudflare in
    /// front of B2). B2 + Cloudflare are in the Bandwidth Alliance → B2→Cloudflare egress
    /// is free + edge-cached. This is the primary COGS lever for a low-priced product.
    /// Hot-swappable at runtime.
    #[serde(default)]
    pub cdn_base_url: String,
}

/// HTTP API configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct Api {
    /// TCP port the HTTP API listens on (defaults to [`DEFAULT_PORT`]).
    #[serde(default = "default_port")]
    pub port: u16,
    /// Whether session cookies get the `Secure` flag.
    ///
    /// Must be `false` for local dev without TLS, `true` for production behind
    /// HTTPS. Defaults to `true` to avoid accidentally shipping insecure cookies.
    #[serde(default = "default_secure_cookies")]
    pub secure_cookies: bool,
}

impl Default for Api {
    fn default() -> Self {
        Self {
            port: DEFAULT_PORT,
            secure_cookies: true,
        }
    }
}

/// Model-scheduler configuration (plan §6).
#[derive(Debug, Clone, Deserialize)]
pub struct Scheduler {
    /// Maximum time to wait for a free backend slot, in seconds.
    #[serde(default = "default_acquire_timeout_secs")]
    pub acquire_timeout_secs: u64,
    /// Interval between backend health probes, in seconds.
    #[serde(default = "default_health_interval_secs")]
    pub health_interval_secs: u64,
    /// Maximum number of failover retries for a single request.
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
}

impl Default for Scheduler {
    fn default() -> Self {
        Self {
            acquire_timeout_secs: default_acquire_timeout_secs(),
            health_interval_secs: default_health_interval_secs(),
            max_retries: default_max_retries(),
        }
    }
}

/// A single inference backend (one llama-server or remote provider endpoint).
#[derive(Debug, Clone, Deserialize)]
pub struct Backend {
    /// Stable, human-readable identifier, unique within the pool.
    pub id: String,
    /// Base URL of the OpenAI-compatible endpoint.
    pub base_url: String,
    /// Roles (capabilities) this backend serves; parsed via [`kb_core::role::Role`].
    pub roles: Vec<Role>,
    /// Concurrent request slots; MUST equal the server's `--parallel N` (plan §6).
    #[serde(default = "default_slots")]
    pub slots: u32,
    /// Selection priority; **lower is preferred** (e.g. local before remote), plan §6.
    #[serde(default)]
    pub priority: u8,
}

/// Folder-watcher configuration (plan §10, P6-T3).
///
/// When enabled, the watcher monitors `watch_root` for new or modified files and
/// automatically enqueues each as an ingest job. File extensions are filtered by
/// `allowed_extensions` (an empty list means allow-all), and files matching any
/// `ignore_patterns` entry are skipped.
#[derive(Debug, Clone, Deserialize)]
pub struct FolderWatch {
    /// Whether the folder watcher starts with the API server.
    #[serde(default)]
    pub enabled: bool,
    /// Absolute path to the watched directory.
    #[serde(default)]
    pub watch_root: String,
    /// File extensions to allow (without leading dot), e.g. `["txt", "pdf"]`.
    /// An empty list means every extension is allowed.
    #[serde(default)]
    pub allowed_extensions: Vec<String>,
    /// Patterns that cause a file to be ignored. Supports exact filename
    /// matches (`thumbs.db`), prefix matches (`~*`), and suffix matches
    /// (`*.tmp`). Path-component matches (e.g. `.git`) are also checked.
    #[serde(default = "default_ignore_patterns")]
    pub ignore_patterns: Vec<String>,
    /// Debounce window in milliseconds. File events within this window are
    /// coalesced so a rapidly-written file is only ingested once.
    #[serde(default = "default_debounce_ms")]
    pub debounce_ms: u64,
}

/// Default debounce window: 2000 ms (2 seconds).
const fn default_debounce_ms() -> u64 {
    2000
}

/// Default ignore patterns covering editor swap files, OS metadata, and VCS dirs.
fn default_ignore_patterns() -> Vec<String> {
    vec![
        ".git".into(),
        "thumbs.db".into(),
        "~*".into(),
        ".DS_Store".into(),
        "*.swp".into(),
        "*.swx".into(),
        ".~*".into(),
    ]
}

impl Default for FolderWatch {
    fn default() -> Self {
        Self {
            enabled: false,
            watch_root: String::new(),
            allowed_extensions: Vec::new(),
            ignore_patterns: default_ignore_patterns(),
            debounce_ms: default_debounce_ms(),
        }
    }
}

/// Automated restore-test settings (plan §21, P8-T7).
///
/// Configures the scheduled job that restores the latest pgBackRest backup to a scratch
/// Postgres instance, runs integrity checks, and alerts on failure or stale backups.
#[derive(Debug, Clone, Deserialize)]
pub struct RestoreTest {
    /// Whether the restore-test job is enabled.
    #[serde(default)]
    pub enabled: bool,
    /// Hour of day to run (0–23 UTC). Default: 3 (03:00 UTC).
    #[serde(default = "default_restore_test_hour")]
    pub schedule_hour: u8,
    /// Minute of hour to run (0–59). Default: 0.
    #[serde(default = "default_restore_test_minute")]
    pub schedule_minute: u8,
    /// Port for the scratch Postgres instance. Default: 5433.
    #[serde(default = "default_restore_test_scratch_port")]
    pub scratch_port: u16,
    /// Directory for the scratch Postgres data. Default: `/tmp/kb-restore-test`.
    #[serde(default = "default_restore_test_scratch_dir")]
    pub scratch_dir: String,
    /// Maximum age of the latest backup in hours before it is considered stale.
    /// Default: 25 (an extra hour of grace past the daily schedule).
    #[serde(default = "default_backup_max_age_hours")]
    pub backup_max_age_hours: u32,
    /// pgBackRest stanza name. Default: `"kb"`.
    #[serde(default = "default_restore_test_stanza")]
    pub stanza: String,
    /// Database name to connect to for integrity checks. Default: `"kb"`.
    #[serde(default = "default_restore_test_db")]
    pub db_name: String,
}

const fn default_restore_test_hour() -> u8 {
    3
}
const fn default_restore_test_minute() -> u8 {
    0
}
const fn default_restore_test_scratch_port() -> u16 {
    5433
}
fn default_restore_test_scratch_dir() -> String {
    "/tmp/kb-restore-test".into()
}
const fn default_backup_max_age_hours() -> u32 {
    25
}
fn default_restore_test_stanza() -> String {
    "kb".into()
}
fn default_restore_test_db() -> String {
    "kb".into()
}

impl Default for RestoreTest {
    fn default() -> Self {
        Self {
            enabled: false,
            schedule_hour: default_restore_test_hour(),
            schedule_minute: default_restore_test_minute(),
            scratch_port: default_restore_test_scratch_port(),
            scratch_dir: default_restore_test_scratch_dir(),
            backup_max_age_hours: default_backup_max_age_hours(),
            stanza: default_restore_test_stanza(),
            db_name: default_restore_test_db(),
        }
    }
}

/// Graceful-degradation settings (plan §22, P8-T9).
///
/// Controls circuit-breaker thresholds, backpressure limits, and health-check
/// intervals for external dependencies.
#[derive(Debug, Clone, Deserialize)]
pub struct Degradation {
    /// Maximum number of concurrent ingest requests **across all tenants**
    /// before returning 429 — the global system ceiling. Set to `0` to disable
    /// the limit. Default: 100.
    #[serde(default = "default_max_inflight_ingest")]
    pub max_inflight_ingest: u32,
    /// Maximum concurrent ingest requests a **single tenant** may hold (the
    /// per-tenant fair-share, plan §22, P14-T7). This bounds a noisy tenant so
    /// it cannot drain the global pool and `429` everyone else. `0` means
    /// *auto-derive* it as `max(1, max_inflight_ingest / 4)`, so one tenant gets
    /// at most a quarter of the global pool. Capped at `max_inflight_ingest`.
    /// Default: 0 (auto-derive).
    #[serde(default = "default_per_tenant_max_inflight")]
    pub per_tenant_max_inflight: u32,
    /// Consecutive failures before the blob-store circuit breaker trips.
    /// Set to `0` to disable. Default: 3.
    #[serde(default = "default_circuit_breaker_threshold")]
    pub blob_circuit_breaker_threshold: u32,
    /// Seconds the blob-store circuit breaker stays open before a half-open probe.
    /// Default: 30.
    #[serde(default = "default_circuit_breaker_cooldown_secs")]
    pub blob_circuit_breaker_cooldown_secs: u64,
    /// Interval in seconds between blob-store health probes (HEAD – bucket
    /// accessibility). Default: 15.
    #[serde(default = "default_blob_health_interval_secs")]
    pub blob_health_interval_secs: u64,
}

const fn default_max_inflight_ingest() -> u32 {
    // Sized conservatively for a single llama.cpp process (~4-6 concurrent
    // slots). Setting this higher than the backend can handle causes excess
    // requests to fail inside the pipeline as 500s instead of being throttled
    // cleanly at the limiter with 429s.
    8
}
const fn default_per_tenant_max_inflight() -> u32 {
    // 0 = auto-derive max(1, max_inflight_ingest / 4) at limiter construction.
    0
}
const fn default_circuit_breaker_threshold() -> u32 {
    3
}
const fn default_circuit_breaker_cooldown_secs() -> u64 {
    30
}
const fn default_blob_health_interval_secs() -> u64 {
    15
}

impl Default for Degradation {
    fn default() -> Self {
        Self {
            max_inflight_ingest: default_max_inflight_ingest(),
            per_tenant_max_inflight: default_per_tenant_max_inflight(),
            blob_circuit_breaker_threshold: default_circuit_breaker_threshold(),
            blob_circuit_breaker_cooldown_secs: default_circuit_breaker_cooldown_secs(),
            blob_health_interval_secs: default_blob_health_interval_secs(),
        }
    }
}

// ── Blob backend (plan §20, P15-T4) ───────────────────────────────────────────

fn default_blob_backend() -> String {
    "local".to_string()
}
fn default_blob_local_root() -> String {
    "kb-blobs".to_string()
}
fn default_blob_local_prefix() -> String {
    "default".to_string()
}

/// Blob-store backend selection (plan §20, P15-T4).
///
/// `local` (the default) stores blobs on the node's filesystem — fine for a
/// single box, but **workers on other machines cannot read them**. Multi-machine
/// deployments must use `s3` (any S3-compatible store: self-hosted MinIO or
/// Backblaze B2), where every node shares one bucket. Credentials come from the
/// environment (`B2_KEY_ID` / `B2_APPLICATION_KEY`), never the config file.
#[derive(Debug, Clone, Deserialize)]
pub struct BlobConfig {
    /// Backend kind: `"local"` (node filesystem) or `"s3"` (S3-compatible).
    #[serde(default = "default_blob_backend")]
    pub backend: String,
    /// Root directory for the `local` backend.
    #[serde(default = "default_blob_local_root")]
    pub local_root: String,
    /// Key-namespace prefix for the `local` backend.
    #[serde(default = "default_blob_local_prefix")]
    pub local_prefix: String,
    /// S3-compatible endpoint settings (used when `backend = "s3"`).
    #[serde(default)]
    pub s3: S3Blob,
}

impl Default for BlobConfig {
    fn default() -> Self {
        Self {
            backend: default_blob_backend(),
            local_root: default_blob_local_root(),
            local_prefix: default_blob_local_prefix(),
            s3: S3Blob::default(),
        }
    }
}

/// S3-compatible blob endpoint (MinIO, Backblaze B2, …; plan §20).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct S3Blob {
    /// Endpoint URL (e.g. `http://minio:9000` or the B2 S3 endpoint).
    #[serde(default)]
    pub endpoint: String,
    /// Region name (MinIO accepts any; B2 wants its bucket region).
    #[serde(default)]
    pub region: String,
    /// Bucket name.
    #[serde(default)]
    pub bucket: String,
}

// ── Worker (plan §16, P15-T4) ─────────────────────────────────────────────────

const fn default_worker_enabled() -> bool {
    true
}
const fn default_worker_concurrency() -> u32 {
    2
}
const fn default_worker_lease_secs() -> u64 {
    600
}
const fn default_worker_min_backoff_ms() -> u64 {
    30_000
}
const fn default_worker_max_retries() -> u32 {
    5
}

/// Ingest-job worker settings (plan §16, P15-T4).
///
/// `kb serve` runs an embedded worker pool when `enabled` (single-box default);
/// dedicated machines run `kb worker` with their own config. `concurrency` is
/// how many jobs this process works at once — independent of the model slots in
/// `[[backend]]`, which bound the model calls those jobs make.
#[derive(Debug, Clone, Deserialize)]
pub struct Worker {
    /// Run an embedded worker pool inside `kb serve`.
    #[serde(default = "default_worker_enabled")]
    pub enabled: bool,
    /// Concurrent jobs processed by this worker process.
    #[serde(default = "default_worker_concurrency")]
    pub concurrency: u32,
    /// Load the global DB routing table for model backends. Default **false**
    /// for exact per-machine capacity: the worker then uses only its own
    /// `[[backend]]` entries, so its slot semaphores are the sole gate on its
    /// local model servers (no cross-process over-subscription).
    #[serde(default)]
    pub use_db_routing: bool,
    /// Job lease (visibility timeout) in seconds; a crashed worker's job is
    /// requeued by the reaper after this long without a heartbeat.
    #[serde(default = "default_worker_lease_secs")]
    pub lease_secs: u64,
    /// Base retry backoff in milliseconds; attempt *n* waits
    /// `min_backoff_ms × 2^(n-1)`. The 30 s default spreads a 5-attempt budget
    /// over ~15 minutes so a transient backend outage or cooldown window
    /// cannot burn the whole budget in seconds (P15-T9). Deterministic
    /// failures skip retries entirely (permanent-error classification).
    #[serde(default = "default_worker_min_backoff_ms")]
    pub min_backoff_ms: u64,
    /// Attempts before a job is dead-lettered.
    #[serde(default = "default_worker_max_retries")]
    pub max_retries: u32,
}

impl Default for Worker {
    fn default() -> Self {
        Self {
            enabled: default_worker_enabled(),
            concurrency: default_worker_concurrency(),
            use_db_routing: false,
            lease_secs: default_worker_lease_secs(),
            min_backoff_ms: default_worker_min_backoff_ms(),
            max_retries: default_worker_max_retries(),
        }
    }
}

// ── Ingest mode + queue bounds (plan §16, P15-T4) ────────────────────────────

fn default_ingest_mode() -> String {
    "queued".to_string()
}
const fn default_max_pending_per_tenant() -> u32 {
    200
}
const fn default_max_pending_global() -> u32 {
    2000
}
const fn default_max_payload_bytes() -> u64 {
    100 * 1024 * 1024
}

/// Upload-ingestion mode + bounded-queue admission (plan §16, P15-T4).
///
/// In `queued` mode (the default) an upload stores its bytes, stages a pending
/// document, and enqueues a job — returning 202 immediately; background workers
/// process it. `inline` is the previous synchronous behaviour, kept as a
/// hot-swappable rollback lever (edit config.toml; no restart). The pending
/// caps bound the queue: an upload past either cap is rejected 429 `queue_full`
/// (with Retry-After) instead of growing the backlog without limit.
#[derive(Debug, Clone, Deserialize)]
pub struct Ingest {
    /// `"queued"` (async, default) or `"inline"` (synchronous rollback path).
    #[serde(default = "default_ingest_mode")]
    pub mode: String,
    /// Per-tenant cap on not-yet-done ingest jobs (queued/running/failed).
    #[serde(default = "default_max_pending_per_tenant")]
    pub max_pending_per_tenant: u32,
    /// Global cap on not-yet-done ingest jobs across all tenants.
    #[serde(default = "default_max_pending_global")]
    pub max_pending_global: u32,
    /// Maximum total bytes allowed in a single multipart upload request
    /// (default: 100 MiB). Override with `KB_MAX_UPLOAD_BYTES` env var.
    /// Files larger than this are rejected with 413 Payload Too Large.
    #[serde(default = "default_max_payload_bytes")]
    pub max_payload_bytes: u64,
}

impl Default for Ingest {
    fn default() -> Self {
        Self {
            mode: default_ingest_mode(),
            max_pending_per_tenant: default_max_pending_per_tenant(),
            max_pending_global: default_max_pending_global(),
            max_payload_bytes: default_max_payload_bytes(),
        }
    }
}

/// Document source-sync settings (P17).
///
/// Controls periodic re-fetch of documents with a `source_url`, LLM diff
/// summary generation, and version-history recording. The global `enabled`
/// flag gates both the scanner loop and the URL-ingest handler — setting it
/// to `false` disables all source-sync functionality at runtime without a
/// restart (read via `AppConfig::current()` at call time).
#[derive(Debug, Clone, Deserialize)]
pub struct SourceSync {
    /// Global kill-switch for source-sync functionality. When `false`, the
    /// scanner loop does not claim due documents and the URL-based ingest
    /// endpoint returns a 403 Forbidden.
    #[serde(default)]
    pub enabled: bool,
    /// How often the scanner loop checks for due documents, in seconds
    /// (minimum 5). Default: 60.
    #[serde(default = "default_scan_interval_secs")]
    pub scan_interval_secs: u64,
    /// Minimum allowed fetch interval in seconds. User-supplied intervals
    /// below this are clamped up. Default: 300 (5 min).
    #[serde(default = "default_min_fetch_interval_secs")]
    pub min_fetch_interval_secs: i64,
    /// Maximum allowed fetch interval in seconds. User-supplied intervals
    /// above this are clamped down. Default: 2_592_000 (30 days).
    #[serde(default = "default_max_fetch_interval_secs")]
    pub max_fetch_interval_secs: i64,
    /// Per-fetch HTTP request timeout in seconds. Default: 30.
    #[serde(default = "default_fetch_timeout_secs")]
    pub fetch_timeout_secs: u64,
    /// Maximum response body size in bytes. Bodies larger than this are
    /// aborted mid-stream. Default: 20_971_520 (20 MiB).
    #[serde(default = "default_max_response_bytes")]
    pub max_response_bytes: u64,
    /// Maximum number of HTTP redirect hops per fetch. Each hop is
    /// re-validated through the SSRF guard. Default: 5.
    #[serde(default = "default_max_redirects")]
    pub max_redirects: u8,
    /// Maximum number of due documents the scanner claims per tick.
    /// Default: 50.
    #[serde(default = "default_scan_batch_limit")]
    pub scan_batch_limit: usize,
}

const fn default_scan_interval_secs() -> u64 {
    60
}
const fn default_min_fetch_interval_secs() -> i64 {
    300
}
const fn default_max_fetch_interval_secs() -> i64 {
    2_592_000
}
const fn default_fetch_timeout_secs() -> u64 {
    30
}
const fn default_max_response_bytes() -> u64 {
    20_971_520
}
const fn default_max_redirects() -> u8 {
    5
}
const fn default_scan_batch_limit() -> usize {
    50
}

impl Default for SourceSync {
    fn default() -> Self {
        Self {
            enabled: false,
            scan_interval_secs: default_scan_interval_secs(),
            min_fetch_interval_secs: default_min_fetch_interval_secs(),
            max_fetch_interval_secs: default_max_fetch_interval_secs(),
            fetch_timeout_secs: default_fetch_timeout_secs(),
            max_response_bytes: default_max_response_bytes(),
            max_redirects: default_max_redirects(),
            scan_batch_limit: default_scan_batch_limit(),
        }
    }
}

/// Multi-turn chat settings (P18).
#[derive(Debug, Clone, Deserialize)]
pub struct ChatConfig {
    /// Whether the chat feature is enabled.
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Model reference for chat, e.g. `"local/qwen-35b"`. When empty, the
    /// first available text model from the backend pool is used.
    #[serde(default)]
    pub model: String,
    /// Maximum number of recent messages in the LLM context window.
    #[serde(default = "default_chat_max_history")]
    pub max_history_messages: usize,
    /// Maximum number of RAG documents to retrieve per turn.
    #[serde(default = "default_chat_max_rag_docs")]
    pub max_rag_docs: usize,
}

fn default_chat_max_history() -> usize {
    20
}
fn default_chat_max_rag_docs() -> usize {
    5
}
fn default_true() -> bool {
    true
}

impl Default for ChatConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            model: String::new(),
            max_history_messages: default_chat_max_history(),
            max_rag_docs: default_chat_max_rag_docs(),
        }
    }
}

/// Orphan GC job settings (plan §23, P8-T10).
///
/// The orphan GC finds B2 blobs with no corresponding files row in the database
/// (orphaned by failed dual-writes) and deletes them after a configurable grace
/// period. It also detects DB rows whose blob is missing from B2 and logs them
/// as data-loss events.
#[derive(Debug, Clone, Deserialize)]
pub struct OrphanGc {
    /// Whether the orphan GC job is enabled.
    #[serde(default)]
    pub enabled: bool,
    /// Hour of day to run (0–23 UTC). Default: 2 (02:00 UTC).
    #[serde(default = "default_orphan_gc_hour")]
    pub schedule_hour: u8,
    /// Minute of hour to run (0–59). Default: 0.
    #[serde(default = "default_orphan_gc_minute")]
    pub schedule_minute: u8,
    /// Grace period in hours before an orphaned blob is deleted. Blobs younger
    /// than this (based on their files.row `ingested_at` or the object's own
    /// last-modified timestamp) are skipped to avoid deleting blobs of
    /// in-flight transactions. Default: 24.
    #[serde(default = "default_blob_gc_grace_hours")]
    pub grace_hours: u32,
}

const fn default_orphan_gc_hour() -> u8 {
    2
}
const fn default_orphan_gc_minute() -> u8 {
    0
}
const fn default_blob_gc_grace_hours() -> u32 {
    24
}

impl Default for OrphanGc {
    fn default() -> Self {
        Self {
            enabled: false,
            schedule_hour: default_orphan_gc_hour(),
            schedule_minute: default_orphan_gc_minute(),
            grace_hours: default_blob_gc_grace_hours(),
        }
    }
}

/// Thumbnail generation settings (plan §20, P8-T13).
///
/// At ingest time, the pipeline can generate small preview/thumbnail blobs
/// (downscaled JPEG for images, keyframe for video) so the browse/grid/search
/// UI serves tiny cached thumbnails and never pulls the full original from B2.
#[derive(Debug, Clone, Deserialize)]
pub struct Thumbnail {
    /// Whether thumbnail generation is enabled at ingest time. Default: true.
    #[serde(default = "default_thumbnail_enabled")]
    pub enabled: bool,
    /// Maximum dimension (width or height) in pixels. Default: 256.
    #[serde(default = "default_thumbnail_max_dimension")]
    pub max_dimension: u32,
    /// JPEG quality 1–100. Default: 70.
    #[serde(default = "default_thumbnail_jpeg_quality")]
    pub jpeg_quality: u8,
}

const fn default_thumbnail_enabled() -> bool {
    true
}
const fn default_thumbnail_max_dimension() -> u32 {
    256
}
const fn default_thumbnail_jpeg_quality() -> u8 {
    70
}

impl Default for Thumbnail {
    fn default() -> Self {
        Self {
            enabled: default_thumbnail_enabled(),
            max_dimension: default_thumbnail_max_dimension(),
            jpeg_quality: default_thumbnail_jpeg_quality(),
        }
    }
}

/// Integrity scan job settings (plan §23, P8-T10).
///
/// The integrity scan periodically re-hashes a random sample of B2 blobs and
/// compares the computed SHA-256 to the value stored in the files table, detecting
/// bit-rot or tampering.
#[derive(Debug, Clone, Deserialize)]
pub struct IntegrityScan {
    /// Whether the integrity scan job is enabled.
    #[serde(default)]
    pub enabled: bool,
    /// Hour of day to run (0–23 UTC). Default: 4 (04:00 UTC).
    #[serde(default = "default_integrity_scan_hour")]
    pub schedule_hour: u8,
    /// Minute of hour to run (0–59). Default: 0.
    #[serde(default = "default_integrity_scan_minute")]
    pub schedule_minute: u8,
    /// Percentage of blobs to sample (1–100). Default: 10.
    #[serde(default = "default_integrity_scan_sample_pct")]
    pub sample_pct: u8,
}

const fn default_integrity_scan_hour() -> u8 {
    4
}
const fn default_integrity_scan_minute() -> u8 {
    0
}
const fn default_integrity_scan_sample_pct() -> u8 {
    10
}

impl Default for IntegrityScan {
    fn default() -> Self {
        Self {
            enabled: false,
            schedule_hour: default_integrity_scan_hour(),
            schedule_minute: default_integrity_scan_minute(),
            sample_pct: default_integrity_scan_sample_pct(),
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    #[test]
    fn config_default_uses_sane_values() {
        let cfg = Config::default();
        assert_eq!(cfg.api.port, DEFAULT_PORT);
        assert_eq!(cfg.scheduler.acquire_timeout_secs, 30);
        assert_eq!(cfg.scheduler.health_interval_secs, 10);
        assert_eq!(cfg.scheduler.max_retries, 3);
        assert!(cfg.storage.postgres_url.is_empty());
        assert!(cfg.backends.is_empty());
    }

    #[test]
    fn section_defaults_match_helpers() {
        assert_eq!(Api::default().port, default_port());
        assert!(Api::default().secure_cookies);
        assert!(default_secure_cookies());
        let s = Scheduler::default();
        assert_eq!(s.acquire_timeout_secs, default_acquire_timeout_secs());
        assert_eq!(s.health_interval_secs, default_health_interval_secs());
        assert_eq!(s.max_retries, default_max_retries());
        assert_eq!(default_slots(), 1);
    }

    #[test]
    fn folder_watch_defaults() {
        let fw = FolderWatch::default();
        assert!(!fw.enabled);
        assert!(fw.watch_root.is_empty());
        assert!(fw.allowed_extensions.is_empty());
        assert_eq!(fw.debounce_ms, 2000);
        // Default ignore patterns include common entries.
        assert!(fw.ignore_patterns.contains(&".git".to_string()));
        assert!(fw.ignore_patterns.contains(&"thumbs.db".to_string()));
        assert!(fw.ignore_patterns.contains(&"~*".to_string()));
    }

    #[test]
    fn folder_watch_deserialization() {
        let toml_str = r#"
[storage]
postgres_url = "pg://localhost/kb"

[folder_watch]
enabled = true
watch_root = "/home/user/docs"
allowed_extensions = ["txt", "md", "pdf"]
debounce_ms = 5000
ignore_patterns = [".git", "*.tmp"]
"#;
        let cfg: Config = toml::from_str(toml_str).expect("parse should succeed");
        assert!(cfg.folder_watch.enabled);
        assert_eq!(cfg.folder_watch.watch_root, "/home/user/docs");
        assert_eq!(
            cfg.folder_watch.allowed_extensions,
            vec!["txt", "md", "pdf"]
        );
        assert_eq!(cfg.folder_watch.debounce_ms, 5000);
        assert_eq!(cfg.folder_watch.ignore_patterns, vec![".git", "*.tmp"]);
    }

    #[test]
    fn folder_watch_default_ignore_patterns_in_deserialized() {
        let toml_str = "[folder_watch]\nenabled = true";
        let cfg: Config = toml::from_str(toml_str).expect("parse should succeed");
        assert!(cfg.folder_watch.ignore_patterns.len() >= 3);
        assert!(
            cfg.folder_watch
                .ignore_patterns
                .contains(&".git".to_string())
        );
    }

    #[test]
    fn restore_test_defaults() {
        let rt = RestoreTest::default();
        assert!(!rt.enabled);
        assert_eq!(rt.schedule_hour, 3);
        assert_eq!(rt.schedule_minute, 0);
        assert_eq!(rt.scratch_port, 5433);
        assert_eq!(rt.scratch_dir, "/tmp/kb-restore-test");
        assert_eq!(rt.backup_max_age_hours, 25);
        assert_eq!(rt.stanza, "kb");
        assert_eq!(rt.db_name, "kb");
    }

    #[test]
    fn restore_test_deserialization() {
        let toml_str = r#"
[restore_test]
enabled = true
schedule_hour = 4
schedule_minute = 30
scratch_port = 6000
scratch_dir = "/var/tmp/rt"
backup_max_age_hours = 13
stanza = "prod"
db_name = "kb_prod"
"#;
        let cfg: Config = toml::from_str(toml_str).expect("parse should succeed");
        let rt = &cfg.restore_test;
        assert!(rt.enabled);
        assert_eq!(rt.schedule_hour, 4);
        assert_eq!(rt.schedule_minute, 30);
        assert_eq!(rt.scratch_port, 6000);
        assert_eq!(rt.scratch_dir, "/var/tmp/rt");
        assert_eq!(rt.backup_max_age_hours, 13);
        assert_eq!(rt.stanza, "prod");
        assert_eq!(rt.db_name, "kb_prod");
    }

    #[test]
    fn restore_test_in_config_default_inherits_defaults() {
        let toml_str = r#"
[restore_test]
enabled = true
"#;
        let cfg: Config = toml::from_str(toml_str).expect("parse should succeed");
        let rt = &cfg.restore_test;
        assert!(rt.enabled);
        // Unspecified fields should use defaults.
        assert_eq!(rt.schedule_hour, 3);
        assert_eq!(rt.schedule_minute, 0);
        assert_eq!(rt.backup_max_age_hours, 25);
    }

    #[test]
    fn degradation_defaults() {
        let d = Degradation::default();
        assert_eq!(d.max_inflight_ingest, 8);
        assert_eq!(d.per_tenant_max_inflight, 0); // 0 = auto-derive
        assert_eq!(d.blob_circuit_breaker_threshold, 3);
        assert_eq!(d.blob_circuit_breaker_cooldown_secs, 30);
        assert_eq!(d.blob_health_interval_secs, 15);
    }

    #[test]
    fn degradation_deserialization() {
        let toml_str = r#"
[degradation]
max_inflight_ingest = 50
per_tenant_max_inflight = 8
blob_circuit_breaker_threshold = 5
blob_circuit_breaker_cooldown_secs = 60
blob_health_interval_secs = 10
"#;
        let cfg: Config = toml::from_str(toml_str).expect("parse should succeed");
        let d = &cfg.degradation;
        assert_eq!(d.max_inflight_ingest, 50);
        assert_eq!(d.per_tenant_max_inflight, 8);
        assert_eq!(d.blob_circuit_breaker_threshold, 5);
        assert_eq!(d.blob_circuit_breaker_cooldown_secs, 60);
        assert_eq!(d.blob_health_interval_secs, 10);
    }

    #[test]
    fn degradation_config_default_inherits() {
        let config = Config::default();
        let d = &config.degradation;
        assert_eq!(d.max_inflight_ingest, 8);
        assert_eq!(d.blob_circuit_breaker_threshold, 3);
    }

    #[test]
    fn orphan_gc_defaults() {
        let og = OrphanGc::default();
        assert!(!og.enabled);
        assert_eq!(og.schedule_hour, 2);
        assert_eq!(og.schedule_minute, 0);
        assert_eq!(og.grace_hours, 24);
    }

    #[test]
    fn orphan_gc_deserialization() {
        let toml_str = r#"
[orphan_gc]
enabled = true
schedule_hour = 5
schedule_minute = 30
grace_hours = 48
"#;
        let cfg: Config = toml::from_str(toml_str).expect("parse should succeed");
        let og = &cfg.orphan_gc;
        assert!(og.enabled);
        assert_eq!(og.schedule_hour, 5);
        assert_eq!(og.schedule_minute, 30);
        assert_eq!(og.grace_hours, 48);
    }

    #[test]
    fn orphan_gc_in_config_default_inherits() {
        let toml_str = "[orphan_gc]\nenabled = true";
        let cfg: Config = toml::from_str(toml_str).expect("parse should succeed");
        let og = &cfg.orphan_gc;
        assert!(og.enabled);
        assert_eq!(og.schedule_hour, 2);
        assert_eq!(og.grace_hours, 24);
    }

    #[test]
    fn integrity_scan_defaults() {
        let is = IntegrityScan::default();
        assert!(!is.enabled);
        assert_eq!(is.schedule_hour, 4);
        assert_eq!(is.schedule_minute, 0);
        assert_eq!(is.sample_pct, 10);
    }

    #[test]
    fn integrity_scan_deserialization() {
        let toml_str = r#"
[integrity_scan]
enabled = true
schedule_hour = 6
schedule_minute = 15
sample_pct = 25
"#;
        let cfg: Config = toml::from_str(toml_str).expect("parse should succeed");
        let is = &cfg.integrity_scan;
        assert!(is.enabled);
        assert_eq!(is.schedule_hour, 6);
        assert_eq!(is.schedule_minute, 15);
        assert_eq!(is.sample_pct, 25);
    }

    // ── blob / worker / ingest (P15-T4) ────────────────────────────────────

    #[test]
    fn blob_defaults_are_local() {
        let b = BlobConfig::default();
        assert_eq!(b.backend, "local");
        assert_eq!(b.local_root, "kb-blobs");
        assert_eq!(b.local_prefix, "default");
        assert!(b.s3.endpoint.is_empty());
        // And an empty config inherits the same.
        let cfg: Config = toml::from_str("").expect("empty config parses");
        assert_eq!(cfg.blob.backend, "local");
    }

    #[test]
    fn blob_s3_overrides_parse() {
        let toml_str = r#"
[blob]
backend = "s3"

[blob.s3]
endpoint = "http://minio:9000"
region = "us-east-1"
bucket = "kb-blobs"
"#;
        let cfg: Config = toml::from_str(toml_str).expect("parse should succeed");
        assert_eq!(cfg.blob.backend, "s3");
        assert_eq!(cfg.blob.s3.endpoint, "http://minio:9000");
        assert_eq!(cfg.blob.s3.region, "us-east-1");
        assert_eq!(cfg.blob.s3.bucket, "kb-blobs");
    }

    #[test]
    fn worker_defaults() {
        let w = Worker::default();
        assert!(w.enabled, "serve runs an embedded worker by default");
        assert_eq!(w.concurrency, 2);
        assert!(
            !w.use_db_routing,
            "workers default to their own per-machine backends (exact slot enforcement)"
        );
        assert_eq!(w.lease_secs, 600);
        // Retry policy (P15-T9): 30 s base spreads 5 attempts over ~15 min so
        // a transient outage can't burn the budget in seconds.
        assert_eq!(w.min_backoff_ms, 30_000);
        assert_eq!(w.max_retries, 5);
        let cfg: Config = toml::from_str("").expect("empty config parses");
        assert!(cfg.worker.enabled);
        assert_eq!(cfg.worker.min_backoff_ms, 30_000);
    }

    #[test]
    fn worker_overrides_parse() {
        let toml_str = r#"
[worker]
enabled = false
concurrency = 7
use_db_routing = true
lease_secs = 120
min_backoff_ms = 5000
max_retries = 3
"#;
        let cfg: Config = toml::from_str(toml_str).expect("parse should succeed");
        assert!(!cfg.worker.enabled);
        assert_eq!(cfg.worker.concurrency, 7);
        assert!(cfg.worker.use_db_routing);
        assert_eq!(cfg.worker.lease_secs, 120);
        assert_eq!(cfg.worker.min_backoff_ms, 5000);
        assert_eq!(cfg.worker.max_retries, 3);
    }

    #[test]
    fn ingest_defaults_are_queued_and_bounded() {
        let i = Ingest::default();
        assert_eq!(i.mode, "queued");
        assert_eq!(i.max_pending_per_tenant, 200);
        assert_eq!(i.max_pending_global, 2000);
        let cfg: Config = toml::from_str("").expect("empty config parses");
        assert_eq!(cfg.ingest.mode, "queued");
    }

    #[test]
    fn ingest_max_payload_bytes_defaults_to_100mb() {
        assert_eq!(Ingest::default().max_payload_bytes, 100 * 1024 * 1024);
    }

    #[test]
    fn ingest_inline_rollback_parses() {
        let toml_str = r#"
[ingest]
mode = "inline"
max_pending_per_tenant = 10
max_pending_global = 50
"#;
        let cfg: Config = toml::from_str(toml_str).expect("parse should succeed");
        assert_eq!(cfg.ingest.mode, "inline");
        assert_eq!(cfg.ingest.max_pending_per_tenant, 10);
        assert_eq!(cfg.ingest.max_pending_global, 50);
    }
}
