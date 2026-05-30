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
    fn backend_with_zero_slots_is_rejected() {
        let src = "[storage]\npostgres_url = \"postgres://x\"\n\
                   [[backend]]\nid = \"a\"\nbase_url = \"http://h\"\nroles = [\"text\"]\nslots = 0\n";
        let err = load_str(src, &empty_env()).unwrap_err();
        assert!(matches!(err, ConfigError::Backend { .. }));
    }
}
