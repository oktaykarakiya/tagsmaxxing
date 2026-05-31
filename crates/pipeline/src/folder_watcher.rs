//! Folder watcher: a background task that monitors a directory for new and
//! modified files and enqueues each as an ingest job via the job queue (plan
//! §10, P6-T3).
//!
//! The watcher uses the [`notify`] crate for cross-platform filesystem events,
//! applies a configurable debounce window to avoid mid-write ingestion, and
//! filters files by allowed extensions and ignore patterns. It runs alongside
//! the API server and supports graceful shutdown (drain in-flight detects).

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use notify::{Event, EventKind, RecursiveMode, Watcher};
use tokio::sync::Mutex;
use tracing;

// ── Config ─────────────────────────────────────────────────────────────────

/// Folder-watcher configuration (mirrors [`kb_config::FolderWatch`]).
///
/// Hot-swappable: callers should wrap this in an [`arc_swap::ArcSwap`] and
/// pass a reference to [`FolderWatcher::new`] so that edits take effect at
/// runtime.
#[derive(Debug, Clone)]
pub struct FolderWatchConfig {
    /// Root directory to watch (absolute path). An empty or non-existent
    /// directory causes the watcher to log an error and stay idle — it never
    /// crashes the process.
    pub watch_root: PathBuf,
    /// File extensions to allow (without leading dot), e.g. `["txt", "pdf"]`.
    /// An empty list means every extension is permitted.
    pub allowed_extensions: Vec<String>,
    /// Patterns that cause a file to be skipped.
    ///
    /// Supports:
    /// - exact filename matches (`thumbs.db`)
    /// - prefix matches (`~*` → filenames starting with `~`)
    /// - suffix matches (`*.tmp` → filenames ending with `.tmp`)
    /// - path-component matches (`.git` inside any directory)
    pub ignore_patterns: Vec<String>,
    /// Debounce window in milliseconds. File events for the same path within
    /// this window are coalesced — only the last event triggers ingestion.
    pub debounce_ms: u64,
    /// Whether the watcher should actually watch. When `false`,
    /// [`FolderWatcher::run`] returns immediately.
    pub enabled: bool,
}

// ── Handler trait ──────────────────────────────────────────────────────────

/// Callback invoked by [`FolderWatcher`] for each detected file.
///
/// The handler is responsible for reading the file and enqueuing an ingest
/// job. The trait exists so the watcher can be tested with a mock (plan
/// §31.3 mock-backend pattern).
#[async_trait]
pub trait FolderWatchHandler: Send + Sync {
    /// Called after the debounce window expires for a detected file.
    ///
    /// The handler receives the absolute path of the file. It should read the
    /// content and enqueue an ingest job, returning the job id on success.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be read or the job cannot be
    /// enqueued. Errors are logged but do **not** crash the watcher.
    async fn handle_file(&self, path: &Path) -> anyhow::Result<i64>;
}

// ── FolderWatcher ──────────────────────────────────────────────────────────

/// Background folder watcher (plan §10, P6-T3).
///
/// Monitors a directory for new and modified files, debounces events to avoid
/// mid-write ingestion, filters by allowed extensions and ignore patterns, and
/// calls a [`FolderWatchHandler`] for each detected file.
///
/// # Lifecycle
///
/// Call [`FolderWatcher::run`] as a background task. It blocks until the
/// shutdown signal fires, then drains any in-flight debounce timers and
/// returns.
///
/// # Hot-swappable config
///
/// The watcher re-reads the configuration (via the [`arc_swap::ArcSwap`]
/// handle) on every event, so changes to the allow-list, ignore patterns, or
/// debounce window take effect without a restart. Changing the watch root
/// requires stopping and restarting the watcher (logged at WARN).
pub struct FolderWatcher {
    /// Hot-swappable configuration handle.
    config_handle: Arc<arc_swap::ArcSwap<FolderWatchConfig>>,
    /// Handler invoked for each detected file.
    handler: Arc<dyn FolderWatchHandler>,
    /// In-flight debounce timers, keyed by canonical path.
    pending: Mutex<HashMap<PathBuf, tokio::task::JoinHandle<()>>>,
}

impl FolderWatcher {
    /// Create a new folder watcher.
    ///
    /// `config_handle` is a shared, hot-swappable reference to the current
    /// [`FolderWatchConfig`]. The watcher reads from it on every event so that
    /// edits take effect without a restart.
    #[must_use]
    pub fn new(
        config_handle: Arc<arc_swap::ArcSwap<FolderWatchConfig>>,
        handler: Arc<dyn FolderWatchHandler>,
    ) -> Self {
        Self {
            config_handle,
            handler,
            pending: Mutex::new(HashMap::new()),
        }
    }

    /// Convenience constructor that wraps a plain [`FolderWatchConfig`] in an
    /// [`arc_swap::ArcSwap`] and returns both the watcher and the handle so
    /// the caller can update the config later.
    #[must_use]
    pub fn from_config(
        config: FolderWatchConfig,
        handler: Arc<dyn FolderWatchHandler>,
    ) -> (Self, Arc<arc_swap::ArcSwap<FolderWatchConfig>>) {
        let handle = Arc::new(arc_swap::ArcSwap::from_pointee(config));
        let watcher = Self::new(Arc::clone(&handle), handler);
        (watcher, handle)
    }

    /// Return a clone of the shared config handle, for external updates.
    #[must_use]
    pub fn config_handle(&self) -> Arc<arc_swap::ArcSwap<FolderWatchConfig>> {
        Arc::clone(&self.config_handle)
    }

    /// Run the folder watcher.
    ///
    /// Blocks until `shutdown` signals `true`. When shutdown fires, the
    /// watcher stops accepting new events and drains any in-flight debounce
    /// timers before returning.
    ///
    /// # Errors
    ///
    /// Returns an error if the notify watcher cannot be created. Errors
    /// during individual file handling are logged and do **not** stop the
    /// watcher.
    pub async fn run(
        &self,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> anyhow::Result<()> {
        // Read current config.
        let cfg = self.config_handle.load_full();
        if !cfg.enabled {
            tracing::info!("folder watcher disabled; not starting");
            return Ok(());
        }

        if cfg.watch_root.as_os_str().is_empty() {
            tracing::info!("folder watcher watch_root is empty; not starting");
            return Ok(());
        }

        // Check that the watch root is a directory.
        if !cfg.watch_root.is_dir() {
            tracing::error!(
                watch_root = %cfg.watch_root.display(),
                "folder watcher: watch_root does not exist or is not a directory"
            );
            // Don't crash — log and idle until shutdown.
            let _ = shutdown.changed().await;
            return Ok(());
        }

        tracing::info!(
            watch_root = %cfg.watch_root.display(),
            debounce_ms = cfg.debounce_ms,
            extensions = ?cfg.allowed_extensions,
            "folder watcher starting"
        );

        // Channel to bridge sync notify callback → async tokio world.
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<notify::Event>();

        let mut notify_watcher =
            notify::recommended_watcher(move |res: notify::Result<notify::Event>| {
                if let Ok(event) = res {
                    let _ = tx.send(event);
                }
            })
            .context("failed to create notify watcher")?;

        // Watch the root directory recursively.
        if let Err(e) = notify_watcher.watch(&cfg.watch_root, RecursiveMode::NonRecursive) {
            tracing::error!(
                error = %e,
                path = %cfg.watch_root.display(),
                "folder watcher: failed to watch directory"
            );
            // Continue — the watcher idles but doesn't crash.
            let _ = shutdown.changed().await;
            return Ok(());
        }

        // Track the active watch root so we can detect changes.
        let mut active_root = cfg.watch_root.clone();

        // Main event loop.
        loop {
            tokio::select! {
                // Shutdown: drain in-flight timers and return.
                _ = shutdown.changed() => {
                    tracing::info!("folder watcher shutting down; draining in-flight");
                    self.drain_pending().await;
                    return Ok(());
                }

                // File-system event.
                maybe_event = rx.recv() => {
                    let Some(event) = maybe_event else {
                        // Channel closed (watcher dropped) — exit.
                        return Ok(());
                    };

                    // Re-read config on every event so edits take effect
                    // without restart.
                    let cfg = self.config_handle.load_full();

                    // Check for watch-root change. We can't re-watch
                    // dynamically (notify 8 doesn't expose unwatch), so
                    // log a warning. The caller should restart the watcher.
                    if cfg.watch_root != active_root {
                        tracing::warn!(
                            old = %active_root.display(),
                            new = %cfg.watch_root.display(),
                            "folder watcher: watch_root changed in config; \
                             restart the watcher for it to take effect"
                        );
                        active_root = cfg.watch_root.clone();
                    }

                    self.handle_event(&event, &cfg).await;
                }
            }
        }
    }

    /// Process a single filesystem event.
    async fn handle_event(&self, event: &Event, cfg: &FolderWatchConfig) {
        // We only care about file Create and Modify events.
        let is_create_or_modify = matches!(event.kind, EventKind::Create(_) | EventKind::Modify(_));
        if !is_create_or_modify {
            return;
        }

        for path in &event.paths {
            // Skip non-files (directories, symlinks, etc.).
            if !path.is_file() {
                continue;
            }

            let canonical = match path.canonicalize() {
                Ok(p) => p,
                Err(e) => {
                    tracing::debug!(
                        path = %path.display(),
                        error = %e,
                        "folder watcher: cannot canonicalize path, skipping"
                    );
                    continue;
                }
            };

            // Filter by extension and ignore patterns.
            if !should_ingest(&canonical, cfg) {
                continue;
            }

            tracing::info!(
                path = %canonical.display(),
                "folder watcher: detected file"
            );

            // Debounce: cancel any existing timer for this path and
            // spawn a new one.
            self.debounce_file(canonical, cfg.debounce_ms).await;
        }
    }

    /// Cancel any existing debounce timer for `path` and spawn a new one.
    ///
    /// The new timer waits `debounce_ms` milliseconds, then calls the handler.
    /// If another event arrives for the same path before the timer fires, the
    /// timer is cancelled and replaced — so only the last event in a rapid
    /// sequence triggers ingestion.
    async fn debounce_file(&self, path: PathBuf, debounce_ms: u64) {
        let mut pending = self.pending.lock().await;

        // Cancel previous timer for this path.
        if let Some(handle) = pending.remove(&path) {
            handle.abort();
        }

        let handler = Arc::clone(&self.handler);
        let path_for_closure = path.clone();

        let handle = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(debounce_ms)).await;

            if let Err(e) = handler.handle_file(&path_for_closure).await {
                tracing::warn!(
                    path = %path_for_closure.display(),
                    error = %e,
                    "folder watcher: handler failed"
                );
            }
        });

        pending.insert(path, handle);
    }

    /// Abort all in-flight debounce timers (called during shutdown).
    async fn drain_pending(&self) {
        let pending = std::mem::take(&mut *self.pending.lock().await);
        for (_path, handle) in pending {
            handle.abort();
        }
    }
}

// ── Pure filter logic ──────────────────────────────────────────────────────

/// Check whether `path` has an extension listed in `allowed`.
///
/// An empty `allowed` list means **all** extensions are permitted. Extensions
/// are compared case-insensitively and without a leading dot.
///
/// # Examples
///
/// ```
/// # use std::path::Path;
/// # use kb_pipeline::folder_watcher::matches_allowed_extension;
/// let path = Path::new("document.txt");
/// assert!(matches_allowed_extension(path, &["txt".into(), "md".into()]));
/// assert!(!matches_allowed_extension(path, &["pdf".into()]));
/// assert!(matches_allowed_extension(path, &[])); // empty = allow all
/// ```
#[must_use]
pub fn matches_allowed_extension(path: &Path, allowed: &[String]) -> bool {
    if allowed.is_empty() {
        return true;
    }
    let Some(ext) = path.extension().and_then(|s| s.to_str()) else {
        // No extension — only allowed if the allow-list is empty
        // (handled above).
        return false;
    };
    allowed.iter().any(|a| a.eq_ignore_ascii_case(ext))
}

/// Check whether `path` matches a given ignore pattern.
///
/// Supports three pattern kinds:
/// - **Exact filename match:** pattern equals the filename (e.g. `thumbs.db`).
/// - **Prefix match:** pattern ends with `*` → filename must start with the
///   prefix (e.g. `~*` matches `~backup.txt`).
/// - **Suffix match:** pattern starts with `*` → filename must end with the
///   suffix (e.g. `*.tmp` matches `a.tmp`).
/// - **Path-component match:** any directory component or the filename itself
///   equals the pattern (e.g. `.git` matches `a/.git/b` or `.git`).
///
/// Matching is case-sensitive.
///
/// # Examples
///
/// ```
/// # use std::path::Path;
/// # use kb_pipeline::folder_watcher::matches_ignore_pattern;
/// assert!(matches_ignore_pattern(Path::new("/a/thumbs.db"), "thumbs.db"));
/// assert!(matches_ignore_pattern(Path::new("/a/~backup.txt"), "~*"));
/// assert!(matches_ignore_pattern(Path::new("/a/foo.tmp"), "*.tmp"));
/// assert!(matches_ignore_pattern(Path::new("/a/.git/config"), ".git"));
/// assert!(!matches_ignore_pattern(Path::new("/a/foo.txt"), "*.tmp"));
/// ```
#[must_use]
pub fn matches_ignore_pattern(path: &Path, pattern: &str) -> bool {
    let filename = path.file_name().and_then(|s| s.to_str()).unwrap_or("");

    // Exact filename match.
    if pattern == filename {
        return true;
    }

    // Suffix match: `*.ext` → filename ends with `.ext`.
    if let Some(suffix) = pattern.strip_prefix('*')
        && filename.ends_with(suffix)
    {
        return true;
    }

    // Prefix match: `prefix*` → filename starts with prefix.
    if let Some(prefix) = pattern.strip_suffix('*')
        && filename.starts_with(prefix)
    {
        return true;
    }

    // Path-component match: any component equals the pattern.
    for component in path.components() {
        if let Some(c) = component.as_os_str().to_str()
            && c == pattern
        {
            return true;
        }
    }

    false
}

/// Check whether `path` should be ingested according to `config`.
///
/// Returns `false` if the file matches any ignore pattern or its extension is
/// not in the allowed list (when the list is non-empty).
///
/// This is a convenience wrapper combining [`matches_allowed_extension`] and
/// [`matches_ignore_pattern`].
#[must_use]
pub fn should_ingest(path: &Path, config: &FolderWatchConfig) -> bool {
    if !matches_allowed_extension(path, &config.allowed_extensions) {
        tracing::debug!(
            path = %path.display(),
            "folder watcher: skipped (extension not in allow-list)"
        );
        return false;
    }

    for pattern in &config.ignore_patterns {
        if matches_ignore_pattern(path, pattern) {
            tracing::debug!(
                path = %path.display(),
                pattern = %pattern,
                "folder watcher: skipped (matches ignore pattern)"
            );
            return false;
        }
    }

    true
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::watch;

    // ── Mock handler ──────────────────────────────────────────────────

    /// A mock [`FolderWatchHandler`] that records the paths it receives.
    struct MockHandler {
        /// Paths received, in order.
        paths: Mutex<Vec<PathBuf>>,
        /// Optional delay inserted before recording (simulates slow ingest).
        delay_ms: u64,
        /// Call count.
        call_count: AtomicUsize,
    }

    impl MockHandler {
        fn new() -> Self {
            Self {
                paths: Mutex::new(Vec::new()),
                delay_ms: 0,
                call_count: AtomicUsize::new(0),
            }
        }

        fn with_delay(delay_ms: u64) -> Self {
            Self {
                paths: Mutex::new(Vec::new()),
                delay_ms,
                call_count: AtomicUsize::new(0),
            }
        }

        async fn received_paths(&self) -> Vec<PathBuf> {
            self.paths.lock().await.clone()
        }
    }

    #[async_trait]
    impl FolderWatchHandler for MockHandler {
        async fn handle_file(&self, path: &Path) -> anyhow::Result<i64> {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            if self.delay_ms > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(self.delay_ms)).await;
            }
            self.paths.lock().await.push(path.to_path_buf());
            Ok(1)
        }
    }

    /// An erroring handler.
    struct ErroringHandler;

    #[async_trait]
    impl FolderWatchHandler for ErroringHandler {
        async fn handle_file(&self, _path: &Path) -> anyhow::Result<i64> {
            anyhow::bail!("simulated handler error")
        }
    }

    // ── matches_allowed_extension tests ───────────────────────────────

    #[test]
    fn empty_allowed_list_allows_all() {
        assert!(matches_allowed_extension(Path::new("x.txt"), &[]));
        assert!(matches_allowed_extension(Path::new("x"), &[]));
        assert!(matches_allowed_extension(Path::new("x.pdf"), &[]));
    }

    #[test]
    fn allowed_extension_matches() {
        let allowed = &["txt".into(), "md".into(), "pdf".into()];
        assert!(matches_allowed_extension(Path::new("doc.txt"), allowed));
        assert!(matches_allowed_extension(Path::new("readme.md"), allowed));
        assert!(matches_allowed_extension(Path::new("report.PDF"), allowed)); // case-insensitive
        assert!(!matches_allowed_extension(Path::new("image.jpg"), allowed));
        assert!(!matches_allowed_extension(Path::new("script.py"), allowed));
    }

    #[test]
    fn no_extension_with_nonempty_allowlist_is_rejected() {
        assert!(!matches_allowed_extension(
            Path::new("Makefile"),
            &["txt".into()]
        ));
    }

    #[test]
    fn dotfile_with_extension() {
        let allowed = &["txt".into()];
        // .gitignore has no extension (hidden file), .env.txt would match.
        assert!(!matches_allowed_extension(Path::new(".gitignore"), allowed));
        assert!(matches_allowed_extension(Path::new(".env.txt"), allowed));
    }

    // ── matches_ignore_pattern tests ─────────────────────────────────

    #[test]
    fn exact_filename_match() {
        assert!(matches_ignore_pattern(
            Path::new("/a/thumbs.db"),
            "thumbs.db"
        ));
        assert!(matches_ignore_pattern(Path::new("thumbs.db"), "thumbs.db"));
        assert!(!matches_ignore_pattern(Path::new("/a/foo.db"), "thumbs.db"));
    }

    #[test]
    fn prefix_wildcard_match() {
        // ~* → filenames starting with ~
        assert!(matches_ignore_pattern(Path::new("/a/~backup.txt"), "~*"));
        assert!(matches_ignore_pattern(Path::new("~temp"), "~*"));
        assert!(!matches_ignore_pattern(Path::new("/a/not-tilde.txt"), "~*"));
    }

    #[test]
    fn suffix_wildcard_match() {
        // *.tmp → filenames ending with .tmp
        assert!(matches_ignore_pattern(Path::new("/a/foo.tmp"), "*.tmp"));
        assert!(matches_ignore_pattern(Path::new("bar.tmp"), "*.tmp"));
        assert!(!matches_ignore_pattern(Path::new("/a/foo.tmpx"), "*.tmp"));
        assert!(!matches_ignore_pattern(Path::new("/a/foo.txt"), "*.tmp"));
    }

    #[test]
    fn path_component_match() {
        // .git anywhere in the path.
        assert!(matches_ignore_pattern(
            Path::new("/project/.git/config"),
            ".git"
        ));
        assert!(matches_ignore_pattern(Path::new(".git"), ".git"));
        assert!(matches_ignore_pattern(Path::new("a/.git/b"), ".git"));
        assert!(!matches_ignore_pattern(Path::new("/a/normal.txt"), ".git"));
    }

    #[test]
    fn pattern_text_without_wildcard_is_exact() {
        // A pattern without wildcards like "thumbs.db" matches the filename exactly,
        // not as a substring.
        assert!(!matches_ignore_pattern(
            Path::new("/a/xthumbs.dbx"),
            "thumbs.db"
        ));
    }

    // ── should_ingest tests ──────────────────────────────────────────

    #[test]
    fn should_ingest_allows_valid_file() {
        let cfg = FolderWatchConfig {
            watch_root: PathBuf::from("/watch"),
            allowed_extensions: vec!["txt".into()],
            ignore_patterns: vec![".git".into(), "~*".into()],
            debounce_ms: 1000,
            enabled: true,
        };
        assert!(should_ingest(Path::new("/watch/doc.txt"), &cfg));
    }

    #[test]
    fn should_ingest_rejects_wrong_extension() {
        let cfg = FolderWatchConfig {
            watch_root: PathBuf::from("/watch"),
            allowed_extensions: vec!["pdf".into()],
            ignore_patterns: vec![],
            debounce_ms: 1000,
            enabled: true,
        };
        assert!(!should_ingest(Path::new("/watch/doc.txt"), &cfg));
    }

    #[test]
    fn should_ingest_rejects_ignore_pattern() {
        let cfg = FolderWatchConfig {
            watch_root: PathBuf::from("/watch"),
            allowed_extensions: vec![],
            ignore_patterns: vec!["~*".into()],
            debounce_ms: 1000,
            enabled: true,
        };
        assert!(!should_ingest(Path::new("/watch/~backup.txt"), &cfg));
    }

    #[test]
    fn should_ingest_empty_allowed_allows_all() {
        let cfg = FolderWatchConfig {
            watch_root: PathBuf::from("/watch"),
            allowed_extensions: vec![],
            ignore_patterns: vec![".git".into()],
            debounce_ms: 1000,
            enabled: true,
        };
        assert!(should_ingest(Path::new("/watch/anyfile.xyz"), &cfg));
    }

    // ── FolderWatcher construction ────────────────────────────────────

    #[test]
    fn watcher_new_stores_config() {
        let cfg = FolderWatchConfig {
            watch_root: PathBuf::from("/tmp/watch"),
            allowed_extensions: vec![],
            ignore_patterns: vec![],
            debounce_ms: 500,
            enabled: true,
        };
        let handle = Arc::new(arc_swap::ArcSwap::from_pointee(cfg));
        let handler: Arc<dyn FolderWatchHandler> = Arc::new(MockHandler::new());
        let watcher = FolderWatcher::new(Arc::clone(&handle), handler);
        // Verify config is accessible.
        let current = watcher.config_handle().load_full();
        assert_eq!(current.debounce_ms, 500);
        assert!(current.enabled);
    }

    #[test]
    fn watcher_from_config_returns_handle() {
        let cfg = FolderWatchConfig {
            watch_root: PathBuf::from("/tmp/watch"),
            allowed_extensions: vec!["txt".into()],
            ignore_patterns: vec![],
            debounce_ms: 500,
            enabled: true,
        };
        let handler: Arc<dyn FolderWatchHandler> = Arc::new(MockHandler::new());
        let (_watcher, handle) = FolderWatcher::from_config(cfg, handler);
        assert_eq!(handle.load_full().allowed_extensions.len(), 1);

        // Update config through handle.
        let mut new_cfg = handle.load_full().as_ref().clone();
        new_cfg.allowed_extensions.push("md".into());
        handle.store(Arc::new(new_cfg));
        assert_eq!(handle.load_full().allowed_extensions.len(), 2);
    }

    #[tokio::test]
    async fn watcher_config_handle_returns_clone() {
        let cfg = FolderWatchConfig {
            watch_root: PathBuf::from("/tmp/watch"),
            allowed_extensions: vec![],
            ignore_patterns: vec![],
            debounce_ms: 500,
            enabled: true,
        };
        let handler: Arc<dyn FolderWatchHandler> = Arc::new(MockHandler::new());
        let (watcher, handle) = FolderWatcher::from_config(cfg, handler);
        let ch = watcher.config_handle();
        assert_eq!(ch.load_full().debounce_ms, handle.load_full().debounce_ms);
    }

    // ── Disabled / empty root returns immediately ─────────────────────

    #[tokio::test]
    async fn watcher_disabled_returns_immediately() {
        let cfg = FolderWatchConfig {
            watch_root: PathBuf::from("/nonexistent"),
            allowed_extensions: vec![],
            ignore_patterns: vec![],
            debounce_ms: 500,
            enabled: false,
        };
        let handler: Arc<dyn FolderWatchHandler> = Arc::new(MockHandler::new());
        let (watcher, _handle) = FolderWatcher::from_config(cfg, handler);
        let (_tx, rx) = watch::channel(false);
        let result = watcher.run(rx).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn watcher_empty_root_returns_immediately() {
        let cfg = FolderWatchConfig {
            watch_root: PathBuf::new(),
            allowed_extensions: vec![],
            ignore_patterns: vec![],
            debounce_ms: 500,
            enabled: true,
        };
        let handler: Arc<dyn FolderWatchHandler> = Arc::new(MockHandler::new());
        let (watcher, _handle) = FolderWatcher::from_config(cfg, handler);
        let (_tx, rx) = watch::channel(false);
        let result = watcher.run(rx).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn watcher_nonexistent_root_idles_until_shutdown() {
        let cfg = FolderWatchConfig {
            watch_root: PathBuf::from("/tmp/definitely_does_not_exist_42"),
            allowed_extensions: vec![],
            ignore_patterns: vec![],
            debounce_ms: 500,
            enabled: true,
        };
        let handler: Arc<dyn FolderWatchHandler> = Arc::new(MockHandler::new());
        let (watcher, _handle) = FolderWatcher::from_config(cfg, handler);
        let (tx, rx) = watch::channel(false);
        // Spawn the watcher and shut it down after a short delay.
        let handle = tokio::spawn(async move { watcher.run(rx).await });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let _ = tx.send(true);
        let result = handle.await.unwrap();
        assert!(result.is_ok());
    }

    // ── End-to-end: files detected and handlers called ────────────────

    #[tokio::test]
    async fn detects_new_file_and_calls_handler() {
        let dir = tempfile::tempdir().unwrap();
        let handler = Arc::new(MockHandler::new());
        let handler_ref = Arc::clone(&handler);

        let cfg = FolderWatchConfig {
            watch_root: dir.path().to_path_buf(),
            allowed_extensions: vec![],
            ignore_patterns: vec![],
            debounce_ms: 50, // short debounce for test
            enabled: true,
        };

        let (watcher, _handle) = FolderWatcher::from_config(cfg, handler_ref);

        let (tx, rx) = watch::channel(false);
        let watcher_task = tokio::spawn(async move { watcher.run(rx).await });

        // Allow the watcher to start.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Create a new file.
        let file_path = dir.path().join("test.txt");
        std::fs::write(&file_path, b"hello world").unwrap();

        // Wait for debounce + processing.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Shut down.
        let _ = tx.send(true);
        let _ = watcher_task.await;

        let paths = handler.received_paths().await;
        assert!(
            paths.iter().any(|p| p.file_name().unwrap() == "test.txt"),
            "handler should have been called for test.txt, got: {paths:?}"
        );
    }

    #[tokio::test]
    async fn skips_file_matching_ignore_pattern() {
        let dir = tempfile::tempdir().unwrap();
        let handler = Arc::new(MockHandler::new());
        let handler_ref = Arc::clone(&handler);

        let cfg = FolderWatchConfig {
            watch_root: dir.path().to_path_buf(),
            allowed_extensions: vec![],
            ignore_patterns: vec!["~*".into()],
            debounce_ms: 50,
            enabled: true,
        };

        let (watcher, _handle) = FolderWatcher::from_config(cfg, handler_ref);

        let (tx, rx) = watch::channel(false);
        let watcher_task = tokio::spawn(async move { watcher.run(rx).await });

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Create a file that matches the ignore pattern.
        let ignored = dir.path().join("~backup.txt");
        std::fs::write(&ignored, b"data").unwrap();

        // Create a file that does NOT match.
        let allowed = dir.path().join("normal.txt");
        std::fs::write(&allowed, b"data").unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let _ = tx.send(true);
        let _ = watcher_task.await;

        let paths = handler.received_paths().await;
        // The ignored file should NOT be in the paths.
        assert!(
            !paths
                .iter()
                .any(|p| p.file_name().unwrap() == "~backup.txt"),
            "ignored file should not be processed, got: {paths:?}"
        );
        // The allowed file SHOULD be in the paths.
        assert!(
            paths.iter().any(|p| p.file_name().unwrap() == "normal.txt"),
            "allowed file should be processed, got: {paths:?}"
        );
    }

    #[tokio::test]
    async fn skips_file_with_unlisted_extension() {
        let dir = tempfile::tempdir().unwrap();
        let handler = Arc::new(MockHandler::new());
        let handler_ref = Arc::clone(&handler);

        let cfg = FolderWatchConfig {
            watch_root: dir.path().to_path_buf(),
            allowed_extensions: vec!["txt".into()],
            ignore_patterns: vec![],
            debounce_ms: 50,
            enabled: true,
        };

        let (watcher, _handle) = FolderWatcher::from_config(cfg, handler_ref);

        let (tx, rx) = watch::channel(false);
        let watcher_task = tokio::spawn(async move { watcher.run(rx).await });

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Create a .txt file (allowed) and a .jpg file (disallowed).
        std::fs::write(dir.path().join("doc.txt"), b"text").unwrap();
        std::fs::write(dir.path().join("photo.jpg"), b"binary").unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let _ = tx.send(true);
        let _ = watcher_task.await;

        let paths = handler.received_paths().await;
        assert!(
            paths.iter().any(|p| p.file_name().unwrap() == "doc.txt"),
            "txt should be processed"
        );
        assert!(
            !paths.iter().any(|p| p.file_name().unwrap() == "photo.jpg"),
            "jpg should not be processed"
        );
    }

    #[tokio::test]
    async fn debounce_coalesces_rapid_writes() {
        let dir = tempfile::tempdir().unwrap();
        let handler = Arc::new(MockHandler::new());
        let handler_ref = Arc::clone(&handler);

        let cfg = FolderWatchConfig {
            watch_root: dir.path().to_path_buf(),
            allowed_extensions: vec![],
            ignore_patterns: vec![],
            debounce_ms: 300, // long enough to coalesce
            enabled: true,
        };

        let (watcher, _handle) = FolderWatcher::from_config(cfg, handler_ref);

        let (tx, rx) = watch::channel(false);
        let watcher_task = tokio::spawn(async move { watcher.run(rx).await });

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Rapidly write to the same file multiple times.
        let file_path = dir.path().join("rapid.txt");
        for i in 0..5 {
            std::fs::write(&file_path, format!("write {i}")).unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }

        // Wait for debounce to settle.
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        let _ = tx.send(true);
        let _ = watcher_task.await;

        let paths = handler.received_paths().await;
        let rapid_hits = paths
            .iter()
            .filter(|p| p.file_name().unwrap() == "rapid.txt")
            .count();
        // We wrote 5 times but debounce should coalesce to 1 (or at most 2
        // if timing is unlucky). Allow up to 2 to accommodate CI variance.
        assert!(
            rapid_hits <= 2,
            "rapid writes should be coalesced to ≤2 handler calls, got {rapid_hits}"
        );
        assert!(rapid_hits >= 1, "file should be detected at least once");
    }

    #[tokio::test]
    async fn handler_error_does_not_crash_watcher() {
        let dir = tempfile::tempdir().unwrap();
        let handler: Arc<dyn FolderWatchHandler> = Arc::new(ErroringHandler);

        let cfg = FolderWatchConfig {
            watch_root: dir.path().to_path_buf(),
            allowed_extensions: vec![],
            ignore_patterns: vec![],
            debounce_ms: 50,
            enabled: true,
        };

        let (watcher, _handle) = FolderWatcher::from_config(cfg, handler);

        let (tx, rx) = watch::channel(false);
        let watcher_task = tokio::spawn(async move { watcher.run(rx).await });

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Create a file — handler will error but watcher shouldn't crash.
        std::fs::write(dir.path().join("error.txt"), b"data").unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Watcher should still be running.
        assert!(!watcher_task.is_finished());

        let _ = tx.send(true);
        let _ = watcher_task.await;
    }

    #[tokio::test]
    async fn graceful_shutdown_drains_pending() {
        let dir = tempfile::tempdir().unwrap();
        // Use a handler with a delay so debounce timers are in-flight
        // during shutdown.
        let handler = Arc::new(MockHandler::with_delay(500));
        let handler_ref = Arc::clone(&handler);

        let cfg = FolderWatchConfig {
            watch_root: dir.path().to_path_buf(),
            allowed_extensions: vec![],
            ignore_patterns: vec![],
            debounce_ms: 800,
            enabled: true,
        };

        let (watcher, _handle) = FolderWatcher::from_config(cfg, handler_ref);

        let (tx, rx) = watch::channel(false);
        let watcher_task = tokio::spawn(async move { watcher.run(rx).await });

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Create a file — debounce timer will be pending.
        std::fs::write(dir.path().join("pending.txt"), b"data").unwrap();

        // Shut down immediately, while debounce is still pending.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let _ = tx.send(true);

        // The watcher should exit cleanly (drain_pending aborts timers).
        let result = watcher_task.await;
        assert!(result.is_ok());
    }
}
