// SPDX-License-Identifier: AGPL-3.0-or-later

//! Logging configuration read from environment variables (plan §18).
//!
//! Configuration is resolved once at startup from the process environment
//! (`LOG_LEVEL`, `LOG_FORMAT`, `LOG_DIR`, `LOG_MAX_GB`, `LOG_FILE_MAX_SIZE_MB`).
//! All values have sensible defaults so the app starts without any env vars set.

use std::path::PathBuf;

/// Default disk cap for all log files (1 GB).
pub const DEFAULT_LOG_MAX_GB: u64 = 1;

/// Default per-file max size before rotation (10 MB).
pub const DEFAULT_FILE_MAX_SIZE_MB: u64 = 10;

/// Default log directory (relative to the working directory).
pub const DEFAULT_LOG_DIR: &str = "./logs";

/// Logging configuration resolved from environment variables.
///
/// Each field has a corresponding `LOG_*` env var with a sensible default
/// (see the `from_env` constructor). Configuration is resolved once at
/// startup — the subscriber and janitor are built from the resulting values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogConfig {
    /// Log level filter: `trace`, `debug`, `info`, `warn`, or `error`.
    pub log_level: String,
    /// Output format: JSON (structured) or pretty (human-readable).
    pub log_format: LogFormat,
    /// Directory where log files are written. Created if it does not exist.
    pub log_dir: PathBuf,
    /// Hard disk cap in GB for the entire log directory (belt +
    /// suspenders: the janitor prunes oldest files to stay under this).
    pub log_max_gb: u64,
    /// Per-file size limit in MB before the rotating writer rolls to a new
    /// numbered file.
    pub log_file_max_size_mb: u64,
}

/// Log output format (plan §18; `LOG_FORMAT=json|pretty`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogFormat {
    /// Structured JSON output — suitable for ingestion by log aggregators.
    Json,
    /// Human-readable coloured output — suitable for development / CLI use.
    Pretty,
}

impl LogConfig {
    /// Build a [`LogConfig`] from the process environment.
    ///
    /// Reads `LOG_LEVEL`, `LOG_FORMAT`, `LOG_DIR`, `LOG_MAX_GB`, and
    /// `LOG_FILE_MAX_SIZE_MB`. All variables are optional and default to the
    /// values listed in [`.env.example`].
    ///
    /// # Panics
    ///
    /// Panics if `LOG_MAX_GB` or `LOG_FILE_MAX_SIZE_MB` are set to zero
    /// (a log cap of zero makes the system unusable).
    pub fn from_env() -> Self {
        let log_level = read_env("LOG_LEVEL", "info");
        let log_format = match read_env("LOG_FORMAT", "json").to_lowercase().as_str() {
            "pretty" => LogFormat::Pretty,
            _ => LogFormat::Json,
        };
        let log_dir = PathBuf::from(read_env("LOG_DIR", DEFAULT_LOG_DIR));
        let log_max_gb = parse_u64_env("LOG_MAX_GB", DEFAULT_LOG_MAX_GB, "LOG_MAX_GB");
        let log_file_max_size_mb = parse_u64_env(
            "LOG_FILE_MAX_SIZE_MB",
            DEFAULT_FILE_MAX_SIZE_MB,
            "LOG_FILE_MAX_SIZE_MB",
        );
        assert!(log_max_gb > 0, "LOG_MAX_GB must be greater than 0");
        assert!(
            log_file_max_size_mb > 0,
            "LOG_FILE_MAX_SIZE_MB must be greater than 0"
        );
        Self {
            log_level,
            log_format,
            log_dir,
            log_max_gb,
            log_file_max_size_mb,
        }
    }

    /// Total log directory cap in bytes (GiB → bytes, 2^30).
    #[must_use]
    pub fn max_bytes(&self) -> u64 {
        self.log_max_gb.saturating_mul(1_073_741_824)
    }

    /// Per-file rotation threshold in bytes (MiB → bytes, 2^20).
    #[must_use]
    pub fn file_max_bytes(&self) -> u64 {
        self.log_file_max_size_mb.saturating_mul(1_048_576)
    }

    /// How many **rotated** files to keep (excluding the active main file).
    ///
    /// Derived from `max_bytes() / file_max_bytes()`, minus 1 for the main
    /// file. Always returns at least 0 so the rotation layer doesn't panic.
    #[must_use]
    pub fn max_rotated_files(&self) -> usize {
        let total = self.max_bytes() / self.file_max_bytes().max(1);
        total.saturating_sub(1).min(usize::MAX as u64) as usize
    }
}

impl Default for LogConfig {
    fn default() -> Self {
        Self::from_env()
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────────

/// Read an env var, returning `default` if the var is absent or empty.
fn read_env(key: &str, default: &str) -> String {
    std::env::var(key)
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| default.to_string())
}

/// Parse a `u64` env var, asserting the value is > 0 (zero means "disabled",
/// which destroys observability).
fn parse_u64_env(key: &str, default: u64, display_name: &str) -> u64 {
    let raw = read_env(key, "");
    if raw.is_empty() {
        return default;
    }
    let val: u64 = raw
        .parse()
        .unwrap_or_else(|_| panic!("{display_name} must be a positive integer, got '{raw}'"));
    assert!(val > 0, "{display_name} must be greater than 0, got {val}");
    val
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(unsafe_code, clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    /// Mutex serialising all tests that manipulate environment variables,
    /// because `cargo test` runs tests in parallel and env vars are
    /// per-process global state.
    static ENV_MUTEX: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// Clear a set of env vars for the duration of a test, then restore
    /// original values (or remove them). This prevents test pollution.
    /// All LOG_* keys that [`LogConfig::from_env`] reads. The restore step
    /// uses this full list so that a `should_panic` test which panics
    /// inside the closure (leaking a bad value) is still cleaned up.
    const ALL_LOG_KEYS: &[&str] = &[
        "LOG_LEVEL",
        "LOG_FORMAT",
        "LOG_DIR",
        "LOG_MAX_GB",
        "LOG_FILE_MAX_SIZE_MB",
    ];

    /// RAII guard that restores environment variables on drop.
    /// Survives panics in the test closure.
    struct EnvGuard {
        saved: Vec<(String, Option<String>)>,
        _mutex_guard: std::sync::MutexGuard<'static, ()>,
    }

    impl Drop for EnvGuard {
        fn drop(&mut self) {
            for (k, v) in &self.saved {
                match v {
                    Some(val) => {
                        // SAFETY: guarded by the ENV_MUTEX held in
                        // `_mutex_guard`. Use allow since set_var/remove_var
                        // are unsafe in Rust 2024.
                        #[allow(unsafe_code)]
                        unsafe {
                            std::env::set_var(k, val);
                        }
                    }
                    None => {
                        #[allow(unsafe_code)]
                        unsafe {
                            std::env::remove_var(k);
                        }
                    }
                }
            }
        }
    }

    fn with_clean_env<F>(keys: &[&str], f: F)
    where
        F: FnOnce(),
    {
        let mutex_guard = ENV_MUTEX.lock().unwrap_or_else(|e| e.into_inner());

        // Save ALL log env vars so restoration is complete even after a
        // should_panic test leaked a bad value.
        let saved: Vec<(String, Option<String>)> = ALL_LOG_KEYS
            .iter()
            .map(|k| ((*k).to_string(), std::env::var(k).ok()))
            .collect();

        let _guard = EnvGuard {
            saved,
            _mutex_guard: mutex_guard,
        };

        // Clear requested keys before running the closure.
        for k in keys {
            // SAFETY: guarded by the EnvGuard which holds the ENV_MUTEX.
            #[allow(unsafe_code)]
            unsafe {
                std::env::remove_var(k);
            }
        }
        f();
    }

    #[test]
    fn defaults_when_no_env() {
        with_clean_env(
            &[
                "LOG_LEVEL",
                "LOG_FORMAT",
                "LOG_DIR",
                "LOG_MAX_GB",
                "LOG_FILE_MAX_SIZE_MB",
            ],
            || {
                let cfg = LogConfig::from_env();
                assert_eq!(cfg.log_level, "info");
                assert_eq!(cfg.log_format, LogFormat::Json);
                assert_eq!(cfg.log_dir, PathBuf::from("./logs"));
                assert_eq!(cfg.log_max_gb, 1);
                assert_eq!(cfg.log_file_max_size_mb, 10);
            },
        );
    }

    #[test]
    fn custom_level_and_format() {
        with_clean_env(&["LOG_LEVEL", "LOG_FORMAT"], || {
            unsafe {
                std::env::set_var("LOG_LEVEL", "debug");
                std::env::set_var("LOG_FORMAT", "pretty");
            }
            let cfg = LogConfig::from_env();
            assert_eq!(cfg.log_level, "debug");
            assert_eq!(cfg.log_format, LogFormat::Pretty);
        });
    }

    #[test]
    fn unknown_format_falls_back_to_json() {
        with_clean_env(&["LOG_FORMAT"], || {
            unsafe {
                std::env::set_var("LOG_FORMAT", "yaml");
            }
            let cfg = LogConfig::from_env();
            assert_eq!(cfg.log_format, LogFormat::Json);
        });
    }

    #[test]
    fn log_format_case_insensitive() {
        with_clean_env(&["LOG_FORMAT"], || {
            unsafe {
                std::env::set_var("LOG_FORMAT", "PRETTY");
            }
            let cfg = LogConfig::from_env();
            assert_eq!(cfg.log_format, LogFormat::Pretty);
        });
    }

    #[test]
    fn custom_dir_and_caps() {
        with_clean_env(&["LOG_DIR", "LOG_MAX_GB", "LOG_FILE_MAX_SIZE_MB"], || {
            unsafe {
                std::env::set_var("LOG_DIR", "/var/log/kb");
                std::env::set_var("LOG_MAX_GB", "5");
                std::env::set_var("LOG_FILE_MAX_SIZE_MB", "50");
            }
            let cfg = LogConfig::from_env();
            assert_eq!(cfg.log_dir, PathBuf::from("/var/log/kb"));
            assert_eq!(cfg.log_max_gb, 5);
            assert_eq!(cfg.log_file_max_size_mb, 50);
        });
    }

    #[test]
    fn max_bytes_conversion() {
        // 1 GiB = 2^30 bytes
        let cfg = LogConfig {
            log_max_gb: 1,
            ..make_empty_cfg()
        };
        assert_eq!(cfg.max_bytes(), 1_073_741_824);
    }

    #[test]
    fn file_max_bytes_conversion() {
        // 10 MiB = 10 × 2^20 bytes
        let cfg = LogConfig {
            log_file_max_size_mb: 10,
            ..make_empty_cfg()
        };
        assert_eq!(cfg.file_max_bytes(), 10_485_760);
    }

    #[test]
    fn max_rotated_files_is_ceil_of_total_over_per_file_minus_one() {
        let cfg = LogConfig {
            log_max_gb: 1,             // 1 GiB (1073741824 bytes)
            log_file_max_size_mb: 100, // 100 MiB (104857600 bytes)
            ..make_empty_cfg()
        };
        // total = 1073741824 / 104857600 = 10 files total
        // rotated = 10 - 1 = 9
        assert_eq!(cfg.max_rotated_files(), 9);
    }

    #[test]
    fn max_rotated_files_zero_when_one_file_fits() {
        let cfg = LogConfig {
            log_max_gb: 1,
            log_file_max_size_mb: 2000, // larger than 1 GB
            ..make_empty_cfg()
        };
        // total = 1 GiB / 2 GiB = 0, saturating_sub(1) = 0
        assert_eq!(cfg.max_rotated_files(), 0);
    }

    #[test]
    fn max_rotated_files_handles_large_cap() {
        let cfg = LogConfig {
            log_max_gb: 100,
            log_file_max_size_mb: 10,
            ..make_empty_cfg()
        };
        // total = 100 × 1024^3 / (10 × 1024^2) = 10240
        // rotated = 10239
        assert_eq!(cfg.max_rotated_files(), 10_239);
    }

    #[test]
    #[should_panic(expected = "LOG_MAX_GB must be greater than 0")]
    fn panics_on_zero_max_gb() {
        with_clean_env(&["LOG_MAX_GB"], || {
            unsafe { std::env::set_var("LOG_MAX_GB", "0") };
            let _ = LogConfig::from_env();
        });
    }

    #[test]
    #[should_panic(expected = "LOG_FILE_MAX_SIZE_MB must be greater than 0")]
    fn panics_on_zero_file_max() {
        with_clean_env(&["LOG_FILE_MAX_SIZE_MB"], || {
            unsafe { std::env::set_var("LOG_FILE_MAX_SIZE_MB", "0") };
            let _ = LogConfig::from_env();
        });
    }

    #[test]
    fn default_implements_trait() {
        // Verify Default is consistent with from_env defaults.
        with_clean_env(
            &[
                "LOG_LEVEL",
                "LOG_FORMAT",
                "LOG_DIR",
                "LOG_MAX_GB",
                "LOG_FILE_MAX_SIZE_MB",
            ],
            || {
                let a = LogConfig::default();
                let b = LogConfig::from_env();
                assert_eq!(a, b);
            },
        );
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    fn make_empty_cfg() -> LogConfig {
        LogConfig {
            log_level: "info".into(),
            log_format: LogFormat::Json,
            log_dir: PathBuf::from("./logs"),
            log_max_gb: 1,
            log_file_max_size_mb: 10,
        }
    }
}
