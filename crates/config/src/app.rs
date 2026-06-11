// SPDX-License-Identifier: AGPL-3.0-or-later

//! Hot-swappable configuration handle backed by [`arc_swap::ArcSwap`].

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arc_swap::ArcSwap;
use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};

use crate::env::env_from_process;
use crate::error::ConfigError;
use crate::load::load_path;
use crate::model::Config;

/// A cloneable, hot-swappable handle to the current [`Config`].
///
/// Long-lived components should hold an `AppConfig` and call [`AppConfig::current`]
/// **per use** rather than capturing a snapshot, so configuration changes take
/// effect without a restart (the hot-swap rule, see `CLAUDE.md`).
#[derive(Debug, Clone)]
pub struct AppConfig {
    path: Option<PathBuf>,
    inner: Arc<ArcSwap<Config>>,
}

impl AppConfig {
    /// Create a handle from an already-loaded [`Config`] (no file association).
    #[must_use]
    pub fn from_config(cfg: Config) -> Self {
        Self {
            path: None,
            inner: Arc::new(ArcSwap::from_pointee(cfg)),
        }
    }

    /// Load configuration from `path` (overlaying the process environment) and return
    /// a hot-swappable handle bound to that path.
    pub fn from_path(path: impl Into<PathBuf>) -> Result<Self, ConfigError> {
        let path = path.into();
        let cfg = load_path(&path, &env_from_process())?;
        Ok(Self {
            path: Some(path),
            inner: Arc::new(ArcSwap::from_pointee(cfg)),
        })
    }

    /// Return the current configuration. Cheap; resolve this at every use site.
    #[must_use]
    pub fn current(&self) -> Arc<Config> {
        self.inner.load_full()
    }

    /// Atomically replace the current configuration.
    pub fn store(&self, cfg: Config) {
        self.inner.store(Arc::new(cfg));
    }

    /// The file path this handle reloads from, if any.
    #[must_use]
    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    /// Re-read the bound file (overlaying the current environment) and swap it in.
    pub fn reload(&self) -> Result<(), ConfigError> {
        let path = self.path.as_ref().ok_or(ConfigError::NoPath)?;
        let cfg = load_path(path, &env_from_process())?;
        self.store(cfg);
        Ok(())
    }

    /// Start watching the bound file; content-changing edits trigger a
    /// [`reload`](Self::reload) without a restart. The returned watcher must be kept
    /// alive for watching to continue.
    pub fn watch(&self) -> Result<RecommendedWatcher, ConfigError> {
        let path = self.path.clone().ok_or(ConfigError::NoPath)?;
        let handle = self.clone();
        let mut watcher = notify::recommended_watcher(move |res: notify::Result<Event>| {
            if let Ok(event) = res {
                handle_fs_event(&event, &handle);
            }
        })?;
        watcher.watch(&path, RecursiveMode::NonRecursive)?;
        Ok(watcher)
    }
}

/// React to a filesystem event by reloading on content-changing events.
///
/// Reload failures (e.g. a half-written or invalid file) are logged and the previous
/// configuration is retained, so a bad edit never takes down the process.
fn handle_fs_event(event: &Event, handle: &AppConfig) {
    if matches!(event.kind, EventKind::Modify(_) | EventKind::Create(_))
        && let Err(error) = handle.reload()
    {
        tracing::warn!(%error, "config hot-reload failed; keeping previous configuration");
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use notify::event::{AccessKind, ModifyKind};
    use std::io::Write;

    fn write_config(path: &Path, url: &str, port: u16) {
        let mut file = std::fs::File::create(path).unwrap();
        write!(
            file,
            "[storage]\npostgres_url = \"{url}\"\n[api]\nport = {port}\n"
        )
        .unwrap();
    }

    fn modify_event() -> Event {
        Event::new(EventKind::Modify(ModifyKind::Any))
    }

    #[test]
    fn from_config_round_trips() {
        let app = AppConfig::from_config(Config::default());
        assert_eq!(app.current().api.port, crate::model::DEFAULT_PORT);
        assert!(app.path().is_none());
    }

    #[test]
    fn store_replaces_current() {
        let app = AppConfig::from_config(Config::default());
        let mut cfg = Config::default();
        cfg.api.port = 4321;
        app.store(cfg);
        assert_eq!(app.current().api.port, 4321);
    }

    #[test]
    fn from_path_then_reload_reflects_edits() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        write_config(&path, "postgres://a", 9999);

        let app = AppConfig::from_path(&path).unwrap();
        assert_eq!(app.current().api.port, 9999);
        assert_eq!(app.path(), Some(path.as_path()));

        write_config(&path, "postgres://a", 8080);
        app.reload().unwrap();
        assert_eq!(app.current().api.port, 8080);
    }

    #[test]
    fn reload_without_path_errors() {
        let app = AppConfig::from_config(Config::default());
        assert!(matches!(app.reload(), Err(ConfigError::NoPath)));
    }

    #[test]
    fn watch_without_path_errors() {
        let app = AppConfig::from_config(Config::default());
        assert!(matches!(app.watch(), Err(ConfigError::NoPath)));
    }

    #[test]
    fn handle_event_reloads_on_modify() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        write_config(&path, "postgres://a", 9999);
        let app = AppConfig::from_path(&path).unwrap();

        write_config(&path, "postgres://a", 8080);
        handle_fs_event(&modify_event(), &app);
        assert_eq!(app.current().api.port, 8080);
    }

    #[test]
    fn handle_event_ignores_non_content_events() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        write_config(&path, "postgres://a", 9999);
        let app = AppConfig::from_path(&path).unwrap();

        write_config(&path, "postgres://a", 8080);
        handle_fs_event(&Event::new(EventKind::Access(AccessKind::Any)), &app);
        assert_eq!(app.current().api.port, 9999);
    }

    #[test]
    fn handle_event_retains_config_on_invalid_edit() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        write_config(&path, "postgres://a", 9999);
        let app = AppConfig::from_path(&path).unwrap();

        std::fs::write(&path, b"this is ((( not valid toml").unwrap();
        handle_fs_event(&modify_event(), &app);
        assert_eq!(app.current().api.port, 9999);
    }

    #[test]
    fn watch_constructs_for_a_file_handle() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        write_config(&path, "postgres://a", 9999);
        let app = AppConfig::from_path(&path).unwrap();
        let watcher = app.watch().unwrap();
        drop(watcher);
    }
}
