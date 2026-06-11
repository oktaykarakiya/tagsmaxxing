// SPDX-License-Identifier: AGPL-3.0-or-later

//! File-rotate-based writer for `tracing-subscriber` (plan §18).
//!
//! The [`RotatingWriter`] wraps [`file_rotate::FileRotate`] in an
//! `Arc<Mutex<…>>` and implements [`tracing_subscriber::fmt::MakeWriter`]
//! so it can be passed directly to `with_writer(…)`.
//!
//! Rotation is triggered by [`file_rotate::ContentLimit::Bytes`]; file count
//! is bounded by [`file_rotate::suffix::AppendCount`] so the rotating layer
//! itself already enforces the per-file count cap. The janitor in
//! [`super::janitor`] provides belt-and-suspenders enforcement of the
//! total-disk cap.

use std::fs;
use std::io;
use std::sync::{Arc, Mutex, MutexGuard};

use file_rotate::compression::Compression;
use file_rotate::suffix::AppendCount;
use file_rotate::{ContentLimit, FileRotate};
use tracing_subscriber::fmt::MakeWriter;

use crate::config::LogConfig;

/// A size-based rotating log writer that implements [`MakeWriter`] for
/// `tracing-subscriber`.
///
/// Internally wraps [`FileRotate`] behind an `Arc<Mutex<…>>` so the writer
/// is `Clone + Send + Sync` — required by tracing layers that must live
/// for the entire program lifetime.
#[derive(Clone)]
pub struct RotatingWriter {
    /// The shared rotating file handle.
    inner: Arc<Mutex<FileRotate<AppendCount>>>,
}

/// An `io::Write` handle that dereferences a [`MutexGuard`] into the
/// inner [`FileRotate`]. This bridges the gap between `MakeWriter` (which
/// requires `Self::Writer: io::Write`) and `MutexGuard` (which does not
/// implement `io::Write` directly).
pub struct RotatingWriterHandle<'a> {
    guard: MutexGuard<'a, FileRotate<AppendCount>>,
}

impl io::Write for RotatingWriterHandle<'_> {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.guard.write(buf)
    }

    #[inline]
    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.guard.write_all(buf)
    }

    #[inline]
    fn flush(&mut self) -> io::Result<()> {
        self.guard.flush()
    }
}

impl RotatingWriter {
    /// Create a new rotating writer from [`LogConfig`].
    ///
    /// Creates the log directory if it does not exist. The file name is
    /// `{log_dir}/kb.log` and rotation is governed by
    /// `config.file_max_bytes()` (size limit) and
    /// `config.max_rotated_files()` (retention count).
    ///
    /// # Errors
    ///
    /// Returns an error if the directory cannot be created or the log file
    /// cannot be opened.
    pub fn new(config: &LogConfig) -> anyhow::Result<Self> {
        fs::create_dir_all(&config.log_dir)?;

        let log_path = config.log_dir.join("kb.log");

        let max_bytes = config.file_max_bytes() as usize;
        let max_files = config.max_rotated_files();

        let writer = FileRotate::new(
            log_path,
            AppendCount::new(max_files),
            ContentLimit::Bytes(max_bytes),
            Compression::None,
            None, // no custom open options
        );

        Ok(Self {
            inner: Arc::new(Mutex::new(writer)),
        })
    }

    /// Lock the writer and write a byte slice directly.
    ///
    /// Used by tests to inject content bypassing the tracing subscriber.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the write fails or the lock is poisoned.
    pub fn write_bytes(&self, buf: &[u8]) -> io::Result<()> {
        let mut handle = self.lock();
        io::Write::write_all(&mut handle, buf)
    }

    /// Flush the underlying file.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the flush fails or the lock is poisoned.
    pub fn flush(&self) -> io::Result<()> {
        let mut handle = self.lock();
        io::Write::flush(&mut handle)
    }

    /// Lock the writer, returning a handle that implements `io::Write`.
    fn lock(&self) -> RotatingWriterHandle<'_> {
        RotatingWriterHandle {
            guard: match self.inner.lock() {
                Ok(g) => g,
                Err(e) => e.into_inner(),
            },
        }
    }
}

/// The [`MakeWriter`] implementation returns a [`RotatingWriterHandle`]
/// which implements `io::Write` by delegating to the inner [`FileRotate`].
///
/// Because `tracing-subscriber` requires `for<'a> MakeWriter<'a>` and the
/// Mutex inside the Arc lives for the program lifetime, any borrow lifetime
/// is valid.
impl<'a> MakeWriter<'a> for RotatingWriter {
    type Writer = RotatingWriterHandle<'a>;

    fn make_writer(&'a self) -> Self::Writer {
        RotatingWriterHandle {
            guard: match self.inner.lock() {
                Ok(g) => g,
                Err(e) => e.into_inner(),
            },
        }
    }

    fn make_writer_for(&'a self, _meta: &tracing::Metadata<'_>) -> Self::Writer {
        self.make_writer()
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn test_config(dir: &tempfile::TempDir) -> LogConfig {
        LogConfig {
            log_level: "info".into(),
            log_format: crate::config::LogFormat::Json,
            log_dir: dir.path().to_path_buf(),
            log_max_gb: 1,
            log_file_max_size_mb: 1, // small → triggers rotation quickly in tests
        }
    }

    #[test]
    fn creates_log_directory() {
        let dir = tempfile::TempDir::new().unwrap();
        let log_dir = dir.path().join("nested").join("logs");
        let cfg = LogConfig {
            log_dir: log_dir.clone(),
            ..test_config(&dir)
        };
        let writer = RotatingWriter::new(&cfg).unwrap();
        writer.flush().unwrap();
        assert!(log_dir.exists());
        assert!(log_dir.join("kb.log").exists());
    }

    #[test]
    fn write_and_read_back() {
        let dir = tempfile::TempDir::new().unwrap();
        let cfg = test_config(&dir);
        let writer = RotatingWriter::new(&cfg).unwrap();
        writer.write_bytes(b"hello from test\n").unwrap();
        writer.flush().unwrap();

        let content = std::fs::read_to_string(cfg.log_dir.join("kb.log")).unwrap();
        assert!(content.contains("hello from test"));
    }

    #[test]
    fn multiple_writes_go_to_same_file() {
        let dir = tempfile::TempDir::new().unwrap();
        let cfg = test_config(&dir);
        let writer = RotatingWriter::new(&cfg).unwrap();
        writer.write_bytes(b"first\n").unwrap();
        writer.write_bytes(b"second\n").unwrap();
        writer.flush().unwrap();

        let content = std::fs::read_to_string(cfg.log_dir.join("kb.log")).unwrap();
        assert!(content.contains("first"));
        assert!(content.contains("second"));
    }

    #[test]
    fn writer_is_cloneable() {
        let dir = tempfile::TempDir::new().unwrap();
        let cfg = test_config(&dir);
        let writer = RotatingWriter::new(&cfg).unwrap();
        let writer2 = writer.clone();
        writer.write_bytes(b"via original\n").unwrap();
        writer2.write_bytes(b"via clone\n").unwrap();
        writer.flush().unwrap();

        let content = std::fs::read_to_string(cfg.log_dir.join("kb.log")).unwrap();
        assert!(content.contains("via original"));
        assert!(content.contains("via clone"));
    }

    #[test]
    fn make_writer_produces_io_write() {
        let dir = tempfile::TempDir::new().unwrap();
        let cfg = test_config(&dir);
        let writer = RotatingWriter::new(&cfg).unwrap();
        {
            let mut guard = MakeWriter::make_writer(&writer);
            io::Write::write_all(&mut guard, b"via MakeWriter\n").unwrap();
        }
        writer.flush().unwrap();
        let content = std::fs::read_to_string(cfg.log_dir.join("kb.log")).unwrap();
        assert!(content.contains("via MakeWriter"));
    }

    #[test]
    fn nonexistent_parent_is_created() {
        let dir = tempfile::TempDir::new().unwrap();
        let log_dir = dir
            .path()
            .join("deep")
            .join("path")
            .join("that")
            .join("does")
            .join("not")
            .join("exist");
        let cfg = LogConfig {
            log_dir: log_dir.clone(),
            ..test_config(&dir)
        };
        let writer = RotatingWriter::new(&cfg).unwrap();
        writer.flush().unwrap();
        assert!(log_dir.join("kb.log").exists());
    }
}
