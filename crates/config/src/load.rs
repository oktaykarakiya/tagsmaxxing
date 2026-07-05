// SPDX-License-Identifier: AGPL-3.0-or-later

//! Loading, environment overlay, and validation of [`Config`].

use std::fs;
use std::path::Path;

use crate::env::{EnvMap, apply_env};
use crate::error::ConfigError;
use crate::model::Config;

/// Parse a [`Config`] from a TOML string, apply the environment overlay, and validate.
pub fn load_str(toml_src: &str, env: &EnvMap) -> Result<Config, ConfigError> {
    let mut cfg: Config = toml::from_str(toml_src)?;
    apply_env(&mut cfg, env)?;
    validate(&cfg)?;
    Ok(cfg)
}

/// Read, parse, overlay, and validate a [`Config`] from a file path.
pub fn load_path(path: &Path, env: &EnvMap) -> Result<Config, ConfigError> {
    let src = fs::read_to_string(path).map_err(|source| ConfigError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    load_str(&src, env)
}

/// Validate a fully-overlaid [`Config`], returning a clear error for any invalid field.
pub fn validate(cfg: &Config) -> Result<(), ConfigError> {
    if cfg.storage.postgres_url.trim().is_empty() {
        return Err(ConfigError::MissingField("storage.postgres_url"));
    }
    if cfg.api.port == 0 {
        return Err(ConfigError::InvalidPort);
    }
    for backend in &cfg.backends {
        let invalid = |reason: &str| ConfigError::Backend {
            backend: backend.id.clone(),
            reason: reason.to_owned(),
        };
        if backend.id.trim().is_empty() {
            return Err(invalid("id must not be empty"));
        }
        // The `db:` prefix is reserved for DB-materialized routing backends
        // (BUG-SCHED-03): the scheduler prunes any `db:`-keyed entry from its
        // flat map on every routing apply, so a config backend using it would
        // silently vanish from health/failover bookkeeping.
        if backend.id.starts_with("db:") {
            return Err(invalid(
                "id must not start with the reserved 'db:' prefix \
                 (reserved for DB-materialized routing backends)",
            ));
        }
        if backend.base_url.trim().is_empty() {
            return Err(invalid("base_url must not be empty"));
        }
        if backend.roles.is_empty() {
            return Err(invalid("at least one role is required"));
        }
        if backend.slots == 0 {
            return Err(invalid("slots must be >= 1"));
        }
    }
    // ── source_sync validation ─────────────────────────────────────────────
    let ss = &cfg.source_sync;
    if ss.min_fetch_interval_secs <= 0 {
        return Err(ConfigError::SourceSync {
            reason: format!(
                "min_fetch_interval_secs must be > 0, got {}",
                ss.min_fetch_interval_secs
            ),
        });
    }
    if ss.min_fetch_interval_secs > ss.max_fetch_interval_secs {
        return Err(ConfigError::SourceSync {
            reason: format!(
                "min_fetch_interval_secs ({}) must be <= max_fetch_interval_secs ({})",
                ss.min_fetch_interval_secs, ss.max_fetch_interval_secs
            ),
        });
    }
    if ss.scan_interval_secs < 5 {
        return Err(ConfigError::SourceSync {
            reason: format!(
                "scan_interval_secs must be >= 5, got {}",
                ss.scan_interval_secs
            ),
        });
    }
    if ss.max_redirects > 10 {
        return Err(ConfigError::SourceSync {
            reason: format!("max_redirects must be <= 10, got {}", ss.max_redirects),
        });
    }
    if ss.max_response_bytes < 1024 {
        return Err(ConfigError::SourceSync {
            reason: format!(
                "max_response_bytes must be >= 1024, got {}",
                ss.max_response_bytes
            ),
        });
    }
    if ss.fetch_timeout_secs == 0 {
        return Err(ConfigError::SourceSync {
            reason: "fetch_timeout_secs must be > 0".to_string(),
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use crate::model::DEFAULT_PORT;
    use kb_core::role::Role;

    fn empty_env() -> EnvMap {
        EnvMap::new()
    }

    #[test]
    fn parses_example_config() {
        let src = include_str!("../config.example.toml");
        let cfg = load_str(src, &empty_env()).unwrap();
        assert_eq!(cfg.api.port, 9999);
        assert_eq!(cfg.storage.postgres_url, "postgres://kb:kb@localhost/kb");
        assert_eq!(
            cfg.storage.app_postgres_url,
            "postgres://kb_app:kb_app@localhost/kb"
        );
        assert_eq!(cfg.scheduler.acquire_timeout_secs, 120);
        assert_eq!(cfg.scheduler.health_interval_secs, 5);
        assert_eq!(cfg.scheduler.max_retries, 2);
        assert_eq!(cfg.backends.len(), 5);
        assert_eq!(cfg.backends[0].id, "local-vl");
        assert_eq!(cfg.backends[0].slots, 4);
        assert_eq!(
            cfg.backends[0].roles,
            vec![Role::Text, Role::Vision, Role::Code]
        );
    }

    #[test]
    fn minimal_config_fills_defaults() {
        let cfg = load_str("[storage]\npostgres_url = \"postgres://x\"\n", &empty_env()).unwrap();
        assert_eq!(cfg.api.port, DEFAULT_PORT);
        assert_eq!(cfg.scheduler.acquire_timeout_secs, 30);
        assert!(cfg.backends.is_empty());
    }

    #[test]
    fn missing_postgres_url_is_rejected() {
        let err = load_str("", &empty_env()).unwrap_err();
        assert!(matches!(
            err,
            ConfigError::MissingField("storage.postgres_url")
        ));
    }

    #[test]
    fn env_supplies_missing_url() {
        let mut env = empty_env();
        env.insert("POSTGRES_URL".into(), "postgres://from-env".into());
        let cfg = load_str("", &env).unwrap();
        assert_eq!(cfg.storage.postgres_url, "postgres://from-env");
    }

    #[test]
    fn env_overrides_file_port() {
        let mut env = empty_env();
        env.insert("PORT".into(), "1234".into());
        let cfg = load_str(
            "[storage]\npostgres_url = \"postgres://x\"\n[api]\nport = 9999\n",
            &env,
        )
        .unwrap();
        assert_eq!(cfg.api.port, 1234);
    }

    #[test]
    fn unknown_role_is_rejected() {
        let src = "[storage]\npostgres_url = \"postgres://x\"\n\
                   [[backend]]\nid = \"a\"\nbase_url = \"http://h\"\nroles = [\"__not_a_role__\"]\nslots = 1\n";
        let err = load_str(src, &empty_env()).unwrap_err();
        assert!(matches!(err, ConfigError::Parse(_)));
    }

    #[test]
    fn backend_without_roles_is_rejected() {
        let src = "[storage]\npostgres_url = \"postgres://x\"\n\
                   [[backend]]\nid = \"a\"\nbase_url = \"http://h\"\nroles = []\nslots = 1\n";
        let err = load_str(src, &empty_env()).unwrap_err();
        assert!(matches!(err, ConfigError::Backend { .. }));
    }

    #[test]
    fn backend_with_reserved_db_prefix_is_rejected() {
        // BUG-SCHED-03: the scheduler reserves `db:` for DB-materialized
        // routing backends and prunes such keys on every routing apply, so a
        // config backend using it would silently drop out of failover.
        let src = "[storage]\npostgres_url = \"postgres://x\"\n\
                   [[backend]]\nid = \"db:foo\"\nbase_url = \"http://h\"\nroles = [\"text\"]\nslots = 1\n";
        let err = load_str(src, &empty_env()).unwrap_err();
        match err {
            ConfigError::Backend { backend, reason } => {
                assert_eq!(backend, "db:foo");
                assert!(reason.contains("reserved"), "reason was: {reason}");
            }
            other => panic!("expected ConfigError::Backend, got {other:?}"),
        }
    }

    #[test]
    fn backend_with_zero_slots_is_rejected() {
        let src = "[storage]\npostgres_url = \"postgres://x\"\n\
                   [[backend]]\nid = \"a\"\nbase_url = \"http://h\"\nroles = [\"text\"]\nslots = 0\n";
        let err = load_str(src, &empty_env()).unwrap_err();
        assert!(matches!(err, ConfigError::Backend { .. }));
    }

    // ── source_sync validation ────────────────────────────────────────────

    fn base() -> String {
        "[storage]\npostgres_url = \"postgres://x\"\n".to_string()
    }

    #[test]
    fn source_sync_defaults_are_off() {
        let cfg = load_str(&base(), &empty_env()).unwrap();
        assert!(!cfg.source_sync.enabled);
        assert_eq!(cfg.source_sync.scan_interval_secs, 60);
        assert_eq!(cfg.source_sync.min_fetch_interval_secs, 300);
        assert_eq!(cfg.source_sync.max_fetch_interval_secs, 2_592_000);
    }

    #[test]
    fn source_sync_enabled_parses() {
        let src = format!("{}[source_sync]\nenabled = true", base());
        let cfg = load_str(&src, &empty_env()).unwrap();
        assert!(cfg.source_sync.enabled);
    }

    #[test]
    fn source_sync_min_greater_than_max_is_rejected() {
        let src = format!(
            "{}[source_sync]\nenabled = true\nmin_fetch_interval_secs = 1000\nmax_fetch_interval_secs = 500",
            base()
        );
        let err = load_str(&src, &empty_env()).unwrap_err();
        assert!(matches!(err, ConfigError::SourceSync { .. }));
        assert!(err.to_string().contains("min_fetch_interval_secs"));
    }

    #[test]
    fn source_sync_scan_interval_too_low_is_rejected() {
        let src = format!("{}[source_sync]\nscan_interval_secs = 2", base());
        let err = load_str(&src, &empty_env()).unwrap_err();
        assert!(matches!(err, ConfigError::SourceSync { .. }));
        assert!(err.to_string().contains("scan_interval_secs"));
    }

    #[test]
    fn source_sync_max_redirects_too_high_is_rejected() {
        let src = format!("{}[source_sync]\nmax_redirects = 15", base());
        let err = load_str(&src, &empty_env()).unwrap_err();
        assert!(matches!(err, ConfigError::SourceSync { .. }));
        assert!(err.to_string().contains("max_redirects"));
    }

    #[test]
    fn source_sync_max_response_bytes_too_low_is_rejected() {
        let src = format!("{}[source_sync]\nmax_response_bytes = 100", base());
        let err = load_str(&src, &empty_env()).unwrap_err();
        assert!(matches!(err, ConfigError::SourceSync { .. }));
        assert!(err.to_string().contains("max_response_bytes"));
    }

    #[test]
    fn source_sync_fetch_timeout_zero_is_rejected() {
        let src = format!("{}[source_sync]\nfetch_timeout_secs = 0", base());
        let err = load_str(&src, &empty_env()).unwrap_err();
        assert!(matches!(err, ConfigError::SourceSync { .. }));
        assert!(err.to_string().contains("fetch_timeout_secs"));
    }

    #[test]
    fn source_sync_valid_custom_values() {
        let src = format!(
            "{}[source_sync]\nenabled = true\nmin_fetch_interval_secs = 600\nmax_fetch_interval_secs = 86400\nscan_interval_secs = 15",
            base()
        );
        let cfg = load_str(&src, &empty_env()).unwrap();
        assert!(cfg.source_sync.enabled);
        assert_eq!(cfg.source_sync.min_fetch_interval_secs, 600);
        assert_eq!(cfg.source_sync.max_fetch_interval_secs, 86_400);
        assert_eq!(cfg.source_sync.scan_interval_secs, 15);
    }
}
