// SPDX-License-Identifier: AGPL-3.0-or-later

//! Error type for the coverage-gate tool.

use std::path::PathBuf;

/// Errors produced while checking coverage floors.
#[derive(Debug, thiserror::Error)]
pub enum CovGateError {
    /// The LCOV report file could not be read.
    #[error("failed to read LCOV report {path}: {source}")]
    Io {
        /// The path that could not be read.
        path: PathBuf,
        /// The underlying I/O error.
        source: std::io::Error,
    },
    /// A `--group` specification was malformed.
    #[error("invalid --group spec {spec:?}: {reason}")]
    BadGroupSpec {
        /// The offending spec string.
        spec: String,
        /// Why it was rejected.
        reason: String,
    },
    /// The command-line arguments were invalid.
    #[error("invalid arguments: {0}")]
    BadArgs(String),
    /// A group's path prefixes matched no files in the report. This is treated as an error
    /// rather than a trivial pass so a renamed or typo'd path can never silently disable a
    /// coverage floor (the exact failure mode P6-T0 guards against).
    #[error("coverage group {name:?} matched no files for prefixes {prefixes:?}")]
    NoFilesMatched {
        /// The group whose prefixes matched nothing.
        name: String,
        /// The prefixes that matched nothing.
        prefixes: Vec<String>,
    },
}
