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
    /// (`NOSUPERUSER NOBYPASSRLS`, migration `0006_app_role.sql`) used for all tenant-scoped
    /// data so Row-Level Security is enforced (P6-T14, §13). May be supplied via the
    /// `APP_POSTGRES_URL` env var. When empty, the store falls back to [`postgres_url`]
    /// (single-role mode — RLS then relies on the explicit `tenant_id` filters only).
    #[serde(default)]
    pub app_postgres_url: String,
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
    /// Maximum number of concurrent ingest requests before returning 429.
    /// Set to `0` to disable the limit. Default: 100.
    #[serde(default = "default_max_inflight_ingest")]
    pub max_inflight_ingest: u32,
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
    100
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
            blob_circuit_breaker_threshold: default_circuit_breaker_threshold(),
            blob_circuit_breaker_cooldown_secs: default_circuit_breaker_cooldown_secs(),
            blob_health_interval_secs: default_blob_health_interval_secs(),
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
        assert_eq!(d.max_inflight_ingest, 100);
        assert_eq!(d.blob_circuit_breaker_threshold, 3);
        assert_eq!(d.blob_circuit_breaker_cooldown_secs, 30);
        assert_eq!(d.blob_health_interval_secs, 15);
    }

    #[test]
    fn degradation_deserialization() {
        let toml_str = r#"
[degradation]
max_inflight_ingest = 50
blob_circuit_breaker_threshold = 5
blob_circuit_breaker_cooldown_secs = 60
blob_health_interval_secs = 10
"#;
        let cfg: Config = toml::from_str(toml_str).expect("parse should succeed");
        let d = &cfg.degradation;
        assert_eq!(d.max_inflight_ingest, 50);
        assert_eq!(d.blob_circuit_breaker_threshold, 5);
        assert_eq!(d.blob_circuit_breaker_cooldown_secs, 60);
        assert_eq!(d.blob_health_interval_secs, 10);
    }

    #[test]
    fn degradation_config_default_inherits() {
        let config = Config::default();
        let d = &config.degradation;
        assert_eq!(d.max_inflight_ingest, 100);
        assert_eq!(d.blob_circuit_breaker_threshold, 3);
    }
}
