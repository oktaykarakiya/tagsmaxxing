// SPDX-License-Identifier: AGPL-3.0-or-later

//! Belt-and-suspenders log-directory janitor (plan §18).
//!
//! In addition to the per-file rotation enforced by [`file_rotate`], a
//! background task periodically scans the log directory, sums all file
//! sizes, and deletes the oldest files when the total exceeds `LOG_MAX_GB`.
//! This protects against edge cases where rotation alone could exceed the
//! cap (e.g. a burst of very large logs before rotation kicks in).
//!
//! The janitor is intended to be spawned as a tokio background task with a
//! configurable poll interval.

use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use crate::config::LogConfig;

/// Background task that prunes the oldest log files when total directory
/// size exceeds the configured cap.
///
/// # Lifecycle
///
/// Call [`LogJanitor::spawn`] to start a periodic pruning loop. The loop
/// runs until the returned [`tokio::sync::watch::Receiver`] receives a
/// shutdown signal.
#[derive(Debug)]
pub struct LogJanitor {
    /// Directory to scan and prune.
    log_dir: PathBuf,
    /// Hard disk cap in bytes (from `LogConfig::max_bytes()`).
    max_bytes: u64,
    /// How often to scan and potentially prune.
    interval: Duration,
    /// Filename prefix for log files (used to avoid deleting non-log files).
    prefix: String,
}

impl LogJanitor {
    /// Default file-name prefix for log files written by the rotating writer.
    pub const LOG_PREFIX: &'static str = "kb.log";

    /// Create a new janitor from [`LogConfig`] with the given poll interval.
    #[must_use]
    pub fn new(config: &LogConfig, interval: Duration) -> Self {
        Self {
            log_dir: config.log_dir.clone(),
            max_bytes: config.max_bytes(),
            interval,
            prefix: Self::LOG_PREFIX.to_string(),
        }
    }

    /// Spawn the janitor as a tokio background task.
    ///
    /// The task runs [`Self::prune_once`] every `interval` until the
    /// `shutdown` watch channel is notified (a value of `true` is sent).
    /// Returns the [`tokio::task::JoinHandle`] so the caller can await
    /// graceful shutdown.
    #[must_use]
    pub fn spawn(
        self,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(self.interval) => {
                        if let Err(e) = self.prune_once().await {
                            tracing::warn!(
                                error = %e,
                                "log janitor prune cycle failed — will retry next interval"
                            );
                        }
                    }
                    Ok(()) = shutdown.changed() => {
                        if *shutdown.borrow() {
                            tracing::info!("log janitor received shutdown signal");
                            break;
                        }
                    }
                }
            }
        })
    }

    /// Run one prune cycle synchronously (the actual I/O is blocking —
    /// wrapped with `tokio::task::spawn_blocking` inside [`prune_once`]).
    pub async fn prune_once(&self) -> anyhow::Result<()> {
        let dir = self.log_dir.clone();
        let max_bytes = self.max_bytes;
        let prefix = self.prefix.clone();

        tokio::task::spawn_blocking(move || prune_directory(&dir, max_bytes, &prefix))
            .await
            .map_err(|e| anyhow::anyhow!("log janitor task panicked: {e}"))?
    }
}

/// Core pruning logic: scan `dir`, sort entries by modification time
/// (oldest first), and delete files until the total size of all matching
/// files is ≤ `max_bytes`.
///
/// This is `pub` so it can be unit-tested directly without spawning tokio
/// tasks.
///
/// # Errors
///
/// Returns an error if the directory cannot be read. Per-file deletion
/// failures are logged as warnings but do **not** halt pruning — the
/// janitor moves on to the next file.
pub fn prune_directory(dir: &std::path::Path, max_bytes: u64, prefix: &str) -> anyhow::Result<()> {
    // Not an error: nothing to prune if the directory doesn't exist yet.
    if !dir.exists() {
        return Ok(());
    }

    let mut entries: Vec<(PathBuf, u64, std::time::SystemTime)> = Vec::new();
    let mut total_bytes: u64 = 0;

    for entry in fs::read_dir(dir)? {
        let entry = match entry {
            Ok(e) => e,
            Err(e) => {
                tracing::warn!(error = %e, "log janitor: failed to read directory entry, skipping");
                continue;
            }
        };

        let path = entry.path();

        // Only consider regular files matching the log prefix.
        let file_name = path
            .file_name()
            .map(|n| n.to_string_lossy())
            .unwrap_or_default();
        if !file_name.starts_with(prefix) {
            continue;
        }

        let metadata = match entry.metadata() {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(path = %path.display(), error = %e, "log janitor: failed to read metadata, skipping");
                continue;
            }
        };

        if !metadata.is_file() {
            continue;
        }

        let size = metadata.len();
        // Use modified time; fall back to creation time if modified is unavailable.
        let modified = metadata
            .modified()
            .unwrap_or_else(|_| metadata.created().unwrap_or(std::time::UNIX_EPOCH));

        total_bytes = total_bytes.saturating_add(size);
        entries.push((path, size, modified));
    }

    // If we are under the cap, nothing to do.
    if total_bytes <= max_bytes {
        return Ok(());
    }

    // Sort oldest-first (ascending modified time).
    entries.sort_by_key(|e| e.2);

    let mut excess = total_bytes.saturating_sub(max_bytes);

    for (path, size, _) in &entries {
        if excess == 0 {
            break;
        }
        match fs::remove_file(path) {
            Ok(()) => {
                excess = excess.saturating_sub(*size);
                tracing::info!(
                    path = %path.display(),
                    size_bytes = size,
                    "log janitor: pruned old log file"
                );
            }
            Err(e) => {
                tracing::warn!(
                    path = %path.display(),
                    error = %e,
                    "log janitor: failed to prune file, skipping"
                );
            }
        }
    }
    Ok(())
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use std::io::Write;

    /// Create a file of `size` bytes with a given name inside `dir`.
    /// Returns the full path.
    fn create_file(dir: &std::path::Path, name: &str, size: usize) -> PathBuf {
        let path = dir.join(name);
        let mut f = fs::File::create(&path).unwrap();
        // Write `size` bytes (content is just 'A' repeated).
        let content = "A".repeat(size);
        f.write_all(content.as_bytes()).unwrap();
        f.flush().unwrap();
        path
    }

    #[test]
    fn empty_directory_is_noop() {
        let dir = tempfile::TempDir::new().unwrap();
        prune_directory(dir.path(), 1024, "kb.log").unwrap();
    }

    #[test]
    fn nonexistent_directory_is_noop() {
        let path = std::path::PathBuf::from("/tmp/nonexistent-log-dir-12345-test");
        assert!(!path.exists());
        prune_directory(&path, 1024, "kb.log").unwrap();
    }

    #[test]
    fn under_cap_is_noop() {
        let dir = tempfile::TempDir::new().unwrap();
        create_file(dir.path(), "kb.log", 100);
        create_file(dir.path(), "kb.log.1", 200);

        let before = count_files(dir.path(), "kb.log");
        prune_directory(dir.path(), 10_000, "kb.log").unwrap();
        let after = count_files(dir.path(), "kb.log");
        assert_eq!(before, after, "no files should be deleted when under cap");
    }

    #[test]
    fn prunes_oldest_first() {
        let dir = tempfile::TempDir::new().unwrap();
        let old = create_file(dir.path(), "kb.log", 500);
        create_file(dir.path(), "kb.log.1", 500);

        // Set the oldest file's modification time to 60 seconds ago.
        let old_mtime = filetime::FileTime::from_unix_time(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64
                - 60,
            0,
        );
        filetime::set_file_mtime(&old, old_mtime).unwrap();

        // Cap is 600 bytes — both files together are 1000, oldest (500)
        // should be deleted.
        prune_directory(dir.path(), 600, "kb.log").unwrap();

        assert!(!old.exists(), "oldest file should have been pruned");
    }

    #[test]
    fn ignores_non_log_files() {
        let dir = tempfile::TempDir::new().unwrap();
        create_file(dir.path(), "kb.log", 500);
        create_file(dir.path(), "other.txt", 500); // not a log file

        // Cap is 200 — kb.log is 500, should be pruned.
        prune_directory(dir.path(), 200, "kb.log").unwrap();

        // The other.txt file must survive.
        assert!(dir.path().join("other.txt").exists());
    }

    #[test]
    fn prunes_multiple_to_reach_cap() {
        let dir = tempfile::TempDir::new().unwrap();
        let f1 = create_file(dir.path(), "kb.log", 300);
        let f2 = create_file(dir.path(), "kb.log.1", 300);
        let f3 = create_file(dir.path(), "kb.log.2", 300);

        // Set modification times: f1 (oldest), f2, f3 (newest).
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        filetime::set_file_mtime(&f1, filetime::FileTime::from_unix_time(now - 60, 0)).unwrap();
        filetime::set_file_mtime(&f2, filetime::FileTime::from_unix_time(now - 30, 0)).unwrap();
        filetime::set_file_mtime(&f3, filetime::FileTime::from_unix_time(now, 0)).unwrap();

        // Cap is 400 — total is 900, need to prune 500+ bytes.
        // f1 (300) + f2 (300) = 600 pruned, leaving f3 (300) ≤ 400.
        prune_directory(dir.path(), 400, "kb.log").unwrap();

        assert!(!f1.exists(), "oldest file (f1) should be pruned");
        assert!(!f2.exists(), "second oldest (f2) should be pruned");
        assert!(f3.exists(), "newest file (f3) should survive");
    }

    #[test]
    fn config_defaults_are_reasonable() {
        // Use explicit values rather than LogConfig::default() which reads
        // process env vars — other tests may modify env vars concurrently.
        let cfg = LogConfig {
            log_level: "info".into(),
            log_format: crate::config::LogFormat::Json,
            log_dir: std::path::PathBuf::from("./logs"),
            log_max_gb: 1,
            log_file_max_size_mb: 10,
        };
        assert!(cfg.max_bytes() > 0);
        assert!(cfg.file_max_bytes() > 0);
    }

    /// Count files in `dir` whose name starts with `prefix`.
    fn count_files(dir: &std::path::Path, prefix: &str) -> usize {
        fs::read_dir(dir)
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .ok()
                    .and_then(|e| e.file_name().into_string().ok())
                    .map(|n| n.starts_with(prefix))
                    .unwrap_or(false)
            })
            .count()
    }
}
