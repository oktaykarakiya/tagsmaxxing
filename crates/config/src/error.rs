// SPDX-License-Identifier: AGPL-3.0-or-later

//! Error type for configuration loading, overlay, validation, and watching.

use std::path::PathBuf;

/// Errors produced while loading, overlaying, validating, or watching configuration.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ConfigError {
    /// The configuration file could not be read.
    #[error("failed to read config file `{path}`: {source}")]
    Io {
        /// Path that could not be read.
        path: PathBuf,
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },
    /// The TOML document failed to parse or deserialize (includes unknown roles).
    #[error("failed to parse config: {0}")]
    Parse(#[from] toml::de::Error),
    /// An environment variable held a value that could not be parsed.
    #[error("invalid value for environment variable `{var}`: `{value}`")]
    InvalidEnvValue {
        /// Name of the offending environment variable.
        var: &'static str,
        /// The value that failed to parse.
        value: String,
    },
    /// A required field was missing or empty after the environment overlay.
    #[error("required configuration field `{0}` is missing or empty")]
    MissingField(&'static str),
    /// A backend entry was misconfigured.
    #[error("backend `{backend}` is misconfigured: {reason}")]
    Backend {
        /// Identifier of the offending backend.
        backend: String,
        /// Human-readable reason the backend is invalid.
        reason: String,
    },
    /// The configured API port was zero.
    #[error("invalid api port: 0 is not a valid port")]
    InvalidPort,
    /// A path-dependent operation was attempted on a handle with no file path.
    #[error("no config file path is associated with this handle")]
    NoPath,
    /// The filesystem watcher could not be created or armed.
    #[error("filesystem watch error: {0}")]
    Watch(#[from] notify::Error),
}
