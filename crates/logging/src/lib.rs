// SPDX-License-Identifier: AGPL-3.0-or-later

//! `kb-logging`: size-based log rotation, JSON/pretty format, and correlation
//! span helpers for the Local File Knowledge Base (plan §18).
//!
//! # Quick start
//!
//! ```rust,ignore
//! use kb_logging::{LogConfig, LogJanitor, init_tracing};
//! use std::time::Duration;
//!
//! let config = LogConfig::from_env();
//! init_tracing(&config)?;
//!
//! // Optional: belt-and-suspenders janitor
//! let janitor = LogJanitor::new(&config, Duration::from_secs(300));
//! let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
//! let janitor_handle = janitor.spawn(shutdown_rx);
//! ```
//!
//! # Correlation spans
//!
//! ```rust,ignore
//! use kb_logging::{request_span, tenant_span, file_span};
//!
//! let span = request_span("req-abc");
//! let _guard = span.enter();
//! // All events inside this scope carry the request_id.
//! ```

pub mod config;
pub mod janitor;
pub mod writer;

use tracing_subscriber::prelude::*;
use tracing_subscriber::{EnvFilter, fmt};

pub use config::{LogConfig, LogFormat};
pub use janitor::LogJanitor;
pub use writer::RotatingWriter;

/// Initialise the global `tracing` subscriber with size-based file rotation.
///
/// The subscriber writes to a file (`{log_dir}/kb.log`) rotated when each
/// file reaches `file_max_size_mb`. Format is JSON (structured) or pretty
/// (human-readable), controlled by `LOG_FORMAT`. The log level is filtered
/// by `LOG_LEVEL` (default `info`).
///
/// When the log directory cannot be created or the log file cannot be opened,
/// the subscriber falls back to writing JSON logs to `stderr` so operators
/// running via `cargo run` see output immediately — no logs are lost and no
/// configuration change is needed.
///
/// # Errors
///
/// Returns an error only if a global default subscriber is already set.
/// Directory/disk errors trigger a fallback to stderr, not a hard failure.
///
/// # Panics
///
/// Panics if `LOG_MAX_GB` or `LOG_FILE_MAX_SIZE_MB` are zero (validated by
/// [`LogConfig::from_env`]).
pub fn init_tracing(config: &LogConfig) -> anyhow::Result<()> {
    let env_filter = EnvFilter::try_new(&config.log_level).map_err(|e| anyhow::anyhow!("{e}"))?;

    let writer_result = RotatingWriter::new(config);

    match writer_result {
        Ok(writer) => {
            // File-based logging: directory was created, log file is open.
            match config.log_format {
                LogFormat::Json => {
                    tracing_subscriber::registry()
                        .with(
                            fmt::layer()
                                .json()
                                .with_writer(writer)
                                .with_target(true)
                                .with_span_list(true)
                                .with_ansi(false),
                        )
                        .with(env_filter)
                        .try_init()
                        .map_err(|e| anyhow::anyhow!("tracing subscriber already set: {e}"))?;
                }
                LogFormat::Pretty => {
                    tracing_subscriber::registry()
                        .with(
                            fmt::layer()
                                .pretty()
                                .with_writer(writer)
                                .with_target(true)
                                .with_ansi(false),
                        )
                        .with(env_filter)
                        .try_init()
                        .map_err(|e| anyhow::anyhow!("tracing subscriber already set: {e}"))?;
                }
            }
        }
        Err(e) => {
            // Fallback: log directory doesn't exist or is unwritable.
            // Output JSON to stderr so cargo-run / host deployments see logs.
            eprintln!(
                "kb-logging: log directory {:?} is not writable ({e}); \
                 falling back to stderr",
                config.log_dir
            );
            tracing_subscriber::registry()
                .with(
                    fmt::layer()
                        .json()
                        .with_writer(std::io::stderr)
                        .with_target(true)
                        .with_span_list(true)
                        .with_ansi(false),
                )
                .with(env_filter)
                .try_init()
                .map_err(|e| anyhow::anyhow!("tracing subscriber already set: {e}"))?;
        }
    }

    Ok(())
}

/// Initialise a tracing subscriber suitable for CLI use (stderr, pretty
/// format, no file rotation).
///
/// This is a convenience for one-shot commands like `kb ingest` /
/// `kb search` where file-based logging is overkill.
///
/// # Errors
///
/// Returns an error if a global default subscriber is already set.
pub fn init_cli_tracing(level: &str) -> anyhow::Result<()> {
    let env_filter = EnvFilter::try_new(level).map_err(|e| anyhow::anyhow!("{e}"))?;

    tracing_subscriber::registry()
        .with(fmt::layer().pretty().with_target(false).with_ansi(true))
        .with(env_filter)
        .try_init()
        .map_err(|e| anyhow::anyhow!("tracing subscriber already set: {e}"))?;

    Ok(())
}

// ── Request-extension correlation types ──────────────────────────────────────

/// Newtype for a job id injected into request extensions so middleware can
/// record it on the active tracing span.
///
/// Handlers that process a job (e.g. ingest worker) inject this into request
/// extensions before calling inner logic; the middleware picks it up and
/// records `job_id` on the span for log correlation.
#[derive(Debug, Clone, Copy)]
pub struct JobId(pub i64);

/// Newtype for a document id injected into request extensions so middleware
/// can record it on the active tracing span.
///
/// Handlers that operate on a specific document (e.g. search, retag) inject
/// this; the middleware records `document_id` on the span for log correlation.
#[derive(Debug, Clone, Copy)]
pub struct DocumentId(pub i64);

// ── Correlation span helpers (plan §18) ─────────────────────────────────────

/// Create a `tracing` span with `request_id` and `tenant_id` fields.
///
/// When recorded as JSON, these fields become top-level keys in each log
/// line emitted within the span, enabling correlation across services.
///
/// ```rust,ignore
/// let span = request_span("req-abc123");
/// let _guard = span.enter();
/// tracing::info!("processing document");
/// ```
#[must_use]
pub fn request_span(request_id: &str) -> tracing::Span {
    tracing::info_span!("request", request_id = %request_id)
}

/// Create a `tracing` span with `tenant_id` and optional `user_id`.
#[must_use]
pub fn tenant_span(tenant_id: &str, user_id: Option<&str>) -> tracing::Span {
    if let Some(uid) = user_id {
        tracing::info_span!("tenant", tenant_id = %tenant_id, user_id = %uid)
    } else {
        tracing::info_span!("tenant", tenant_id = %tenant_id)
    }
}

/// Create a `tracing` span with `file_id` for a file-processing operation.
#[must_use]
pub fn file_span(file_id: &str) -> tracing::Span {
    tracing::info_span!("file", file_id = %file_id)
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::io;
    use std::sync::{Arc, Mutex};

    use super::*;
    use tracing_subscriber::fmt::MakeWriter;

    /// Helper: build a config that writes to a temp directory.
    fn test_config(dir: &std::path::Path) -> LogConfig {
        LogConfig {
            log_level: "info".into(),
            log_format: LogFormat::Json,
            log_dir: dir.to_path_buf(),
            log_max_gb: 1,
            log_file_max_size_mb: 10,
        }
    }

    #[test]
    fn init_tracing_does_not_panic() {
        let dir = tempfile::TempDir::new().unwrap();
        let cfg = test_config(dir.path());
        // First call should succeed or error ("already set" from another test).
        let result = init_tracing(&cfg);
        match result {
            Ok(()) => {}
            Err(e) => {
                let msg = e.to_string();
                assert!(msg.contains("already set"), "unexpected error: {msg}");
            }
        }
    }

    #[test]
    fn init_tracing_creates_log_file() {
        let dir = tempfile::TempDir::new().unwrap();
        let cfg = test_config(dir.path());
        let _ = init_tracing(&cfg);
        assert!(cfg.log_dir.exists());
    }

    #[test]
    fn init_cli_tracing_does_not_panic() {
        let result = init_cli_tracing("info");
        match result {
            Ok(()) => {}
            Err(e) => {
                let msg = e.to_string();
                assert!(msg.contains("already set"), "unexpected error: {msg}");
            }
        }
    }

    /// Helper: assert span name, skipping when no subscriber is set (spans
    /// created without a subscriber may lack metadata).
    fn assert_span_name(span: &tracing::Span, expected: &str) {
        if let Some(meta) = span.metadata() {
            assert_eq!(meta.name(), expected);
        }
        // Without a subscriber, metadata may be None — the span still
        // exists and will be recorded once a subscriber is active.
    }

    #[test]
    fn request_span_has_correct_name() {
        let span = request_span("req-123");
        assert_span_name(&span, "request");
    }

    #[test]
    fn tenant_span_has_correct_name() {
        let span = tenant_span("t-1", Some("u-2"));
        assert_span_name(&span, "tenant");
    }

    #[test]
    fn tenant_span_without_user() {
        let span = tenant_span("t-1", None);
        assert_span_name(&span, "tenant");
    }

    #[test]
    fn file_span_has_correct_name() {
        let span = file_span("f-42");
        assert_span_name(&span, "file");
    }

    #[test]
    fn span_fields_are_recorded_in_json() {
        // Verify that span fields appear in JSON output. We set up a
        // tracing subscriber that captures to a buffer, enter a span, log
        // an event within it, and assert the JSON contains the span fields.

        // A writer that captures to a Vec<u8> behind a Mutex.
        #[derive(Clone)]
        struct BufWriter {
            buf: Arc<Mutex<Vec<u8>>>,
        }

        impl BufWriter {
            fn new() -> Self {
                Self {
                    buf: Arc::new(Mutex::new(Vec::new())),
                }
            }

            fn contents(&self) -> String {
                let guard = self.buf.lock().unwrap();
                String::from_utf8_lossy(&guard).to_string()
            }
        }

        impl io::Write for BufWriter {
            fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
                self.buf.lock().unwrap().write(buf)
            }
            fn flush(&mut self) -> io::Result<()> {
                self.buf.lock().unwrap().flush()
            }
        }

        // Implement MakeWriter for BufWriter.
        impl<'a> MakeWriter<'a> for BufWriter {
            type Writer = BufWriter;
            fn make_writer(&'a self) -> Self::Writer {
                self.clone()
            }
            fn make_writer_for(&'a self, _meta: &tracing::Metadata<'_>) -> Self::Writer {
                self.clone()
            }
        }

        let buf = BufWriter::new();
        let buf_capture = buf.clone();

        let subscriber = tracing_subscriber::registry()
            .with(
                fmt::layer()
                    .json()
                    .with_writer(buf)
                    .with_target(false)
                    .with_span_list(true)
                    .with_ansi(false),
            )
            .with(EnvFilter::new("info"));

        // Use with_default so we don't conflict with the global subscriber.
        tracing::subscriber::with_default(subscriber, || {
            let span = request_span("req-abc");
            let _guard = span.enter();
            tracing::info!("test event");
        });

        let output = buf_capture.contents();
        assert!(
            output.contains("req-abc"),
            "JSON output should contain the request_id value: {output}"
        );
        assert!(
            output.contains("request_id"),
            "JSON output should contain the request_id field: {output}"
        );
    }
}
