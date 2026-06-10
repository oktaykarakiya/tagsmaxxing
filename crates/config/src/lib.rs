//! Typed, validated, hot-swappable configuration for the Local File Knowledge Base.
//!
//! Configuration is parsed from a TOML file (plan §6), overlaid with environment
//! variables, validated, and exposed behind an [`arc_swap::ArcSwap`] handle
//! ([`AppConfig`]) offering `current()` / `reload()` plus file-watch hot-reload
//! (the hot-swap rule, see `CLAUDE.md`). Call sites resolve [`AppConfig::current`]
//! per use so that edits take effect without a restart.

mod app;
mod env;
mod error;
mod load;
mod model;

pub use app::AppConfig;
pub use env::{EnvMap, apply_env, env_from_process};
pub use error::ConfigError;
pub use load::{load_path, load_str, validate};
pub use model::{
    Api, Backend, BlobConfig, Config, DEFAULT_PORT, Degradation, FolderWatch, Ingest,
    IntegrityScan, OrphanGc, RestoreTest, S3Blob, Scheduler, Storage, Thumbnail, Worker,
};
