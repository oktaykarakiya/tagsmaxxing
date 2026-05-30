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
}

/// Durable-storage configuration.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct Storage {
    /// Postgres connection URL. Required; may be supplied via the `POSTGRES_URL` env var.
    #[serde(default)]
    pub postgres_url: String,
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
}
