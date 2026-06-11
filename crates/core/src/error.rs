// SPDX-License-Identifier: AGPL-3.0-or-later

//! Error type for `kb-core`'s own fallible logic (string/hex parsing).
//!
//! The high-level capability traits return [`anyhow::Result`] so implementations in other
//! crates can surface their own rich errors; this typed error is for `kb-core` internals.

/// Errors produced by `kb-core` parsing helpers.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CoreError {
    /// A string did not match any variant of an enum (e.g. an unexpected DB `kind`).
    #[error("unknown {kind} value: {value:?}")]
    UnknownVariant {
        /// The logical enum name the value was being parsed into.
        kind: &'static str,
        /// The offending input value.
        value: String,
    },

    /// A string was not a valid 64-character SHA-256 hex digest.
    #[error("invalid sha-256 hex digest: {0:?}")]
    InvalidHash(String),
}

/// Convenience alias for results carrying a [`CoreError`].
pub type Result<T> = std::result::Result<T, CoreError>;
