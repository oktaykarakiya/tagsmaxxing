//! Environment-variable overlay for [`Config`].

use std::collections::BTreeMap;

use crate::error::ConfigError;
use crate::model::Config;

/// A snapshot of environment variables used to overlay file-based configuration.
///
/// An explicit map keeps the overlay pure and unit-testable; production code builds
/// it from the process environment via [`env_from_process`] **at call time**, so a
/// rotated value is picked up on the next [`crate::AppConfig::reload`].
pub type EnvMap = BTreeMap<String, String>;

/// Build an [`EnvMap`] from the current process environment (read at call time).
#[must_use]
pub fn env_from_process() -> EnvMap {
    std::env::vars().collect()
}

/// Overlay recognized environment variables onto `cfg`, overriding file values.
///
/// Recognized variables: `POSTGRES_URL`, `APP_POSTGRES_URL`, `PORT`,
/// `KB_ACQUIRE_TIMEOUT_SECS`, `KB_HEALTH_INTERVAL_SECS`, `KB_MAX_RETRIES`. Other `KB_*`
/// variables are left for other subsystems (e.g. the secrets store, plan §24).
pub fn apply_env(cfg: &mut Config, env: &EnvMap) -> Result<(), ConfigError> {
    if let Some(v) = env.get("POSTGRES_URL") {
        cfg.storage.postgres_url = v.clone();
    }
    if let Some(v) = env.get("APP_POSTGRES_URL") {
        cfg.storage.app_postgres_url = v.clone();
    }
    if let Some(v) = env.get("PORT") {
        cfg.api.port = parse_env("PORT", v)?;
    }
    if let Some(v) = env.get("KB_ACQUIRE_TIMEOUT_SECS") {
        cfg.scheduler.acquire_timeout_secs = parse_env("KB_ACQUIRE_TIMEOUT_SECS", v)?;
    }
    if let Some(v) = env.get("KB_HEALTH_INTERVAL_SECS") {
        cfg.scheduler.health_interval_secs = parse_env("KB_HEALTH_INTERVAL_SECS", v)?;
    }
    if let Some(v) = env.get("KB_MAX_RETRIES") {
        cfg.scheduler.max_retries = parse_env("KB_MAX_RETRIES", v)?;
    }
    Ok(())
}

/// Parse an environment value into `T`, mapping failures to a clear error.
fn parse_env<T: std::str::FromStr>(var: &'static str, value: &str) -> Result<T, ConfigError> {
    value
        .trim()
        .parse::<T>()
        .map_err(|_| ConfigError::InvalidEnvValue {
            var,
            value: value.to_string(),
        })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    fn map(pairs: &[(&str, &str)]) -> EnvMap {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    #[test]
    fn overlay_overrides_url_and_port() {
        let mut cfg = Config::default();
        apply_env(
            &mut cfg,
            &map(&[("POSTGRES_URL", "postgres://x"), ("PORT", "8080")]),
        )
        .unwrap();
        assert_eq!(cfg.storage.postgres_url, "postgres://x");
        assert_eq!(cfg.api.port, 8080);
    }

    #[test]
    fn overlay_sets_app_postgres_url_independently() {
        let mut cfg = Config::default();
        apply_env(
            &mut cfg,
            &map(&[
                ("POSTGRES_URL", "postgres://kb@h/kb"),
                ("APP_POSTGRES_URL", "postgres://kb_app@h/kb"),
            ]),
        )
        .unwrap();
        assert_eq!(cfg.storage.postgres_url, "postgres://kb@h/kb");
        assert_eq!(cfg.storage.app_postgres_url, "postgres://kb_app@h/kb");
    }

    #[test]
    fn app_postgres_url_defaults_empty_when_unset() {
        let mut cfg = Config::default();
        apply_env(&mut cfg, &map(&[("POSTGRES_URL", "postgres://x")])).unwrap();
        assert!(
            cfg.storage.app_postgres_url.is_empty(),
            "app URL stays empty (single-role fallback) when APP_POSTGRES_URL is unset"
        );
    }

    #[test]
    fn overlay_overrides_scheduler_fields() {
        let mut cfg = Config::default();
        apply_env(
            &mut cfg,
            &map(&[
                ("KB_ACQUIRE_TIMEOUT_SECS", "5"),
                ("KB_HEALTH_INTERVAL_SECS", "2"),
                ("KB_MAX_RETRIES", "7"),
            ]),
        )
        .unwrap();
        assert_eq!(cfg.scheduler.acquire_timeout_secs, 5);
        assert_eq!(cfg.scheduler.health_interval_secs, 2);
        assert_eq!(cfg.scheduler.max_retries, 7);
    }

    #[test]
    fn empty_overlay_changes_nothing() {
        let mut cfg = Config::default();
        apply_env(&mut cfg, &map(&[])).unwrap();
        assert_eq!(cfg.api.port, DEFAULT_PORT_FOR_TEST);
    }

    const DEFAULT_PORT_FOR_TEST: u16 = crate::model::DEFAULT_PORT;

    #[test]
    fn bad_port_is_rejected() {
        let mut cfg = Config::default();
        let err = apply_env(&mut cfg, &map(&[("PORT", "not-a-number")])).unwrap_err();
        assert!(matches!(
            err,
            ConfigError::InvalidEnvValue { var: "PORT", .. }
        ));
    }

    #[test]
    fn unknown_kb_var_is_ignored() {
        let mut cfg = Config::default();
        apply_env(&mut cfg, &map(&[("KB_SOMETHING_ELSE", "value")])).unwrap();
        assert_eq!(cfg.api.port, DEFAULT_PORT_FOR_TEST);
    }
}
