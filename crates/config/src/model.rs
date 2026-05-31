//! Typed configuration model parsed from `config.toml` (plan §6).

use kb_core::role::Role;
use serde::Deserialize;

/// Default HTTP API port (workspace fact: the API serves on 9999).
pub const DEFAULT_PORT: u16 = 9999;

const fn default_port() -> u16 {
    DEFAULT_PORT
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
}

impl Default for Api {
    fn default() -> Self {
        Self { port: DEFAULT_PORT }
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
}
