//! `kb worker` — a standalone, distributed ingest-worker process (P15-T8).
//!
//! Runs the exact same processing stack as `kb serve`'s embedded workers
//! (`kb_api::runtime::build_runtime` + `kb_api::worker_pool`) without the
//! HTTP server: claim queued ingest jobs, load the staged document + blobs,
//! and finalize them through the pipeline. Capacity is **per machine**: with
//! the default `[worker] use_db_routing = false` the scheduler pool is built
//! solely from this machine's own `[[backend]]` entries, so its slot
//! semaphores are the only gate on its local model servers — heterogeneous
//! fleets are expressed as one config.toml per machine, and no process can
//! over-subscribe another's models.
//!
//! Per-machine reachability also applies to the extraction sidecars: set
//! `TIKA_URL` / `WHISPER_URL` to endpoints this machine can reach — workers
//! call them during extraction exactly like the API node does. Multi-machine
//! fleets additionally require the shared blob store (`[blob] backend = "s3"`)
//! so workers can fetch bytes the API node staged.
//!
//! This module only binds OS signals and loads configuration; the runnable
//! core lives in [`kb_api::worker_pool::worker_main`] so integration tests
//! can drive the full command path without signals.

use anyhow::Context;
use kb_config::AppConfig;
use tracing::{info, warn};

use crate::cli::WorkerArgs;

/// Entry point for `kb worker`.
///
/// Initialises tracing, loads the per-machine config (keeping the hot-reload
/// file watcher alive), converts SIGINT (ctrl-c) / SIGTERM (container stop)
/// into the shared shutdown signal, and runs
/// [`kb_api::worker_pool::worker_main`].
///
/// # Errors
/// Returns an error if the config cannot be loaded, tracing cannot be
/// initialised, or the runtime cannot be built (Postgres unreachable, …).
pub async fn run_worker(args: &WorkerArgs) -> anyhow::Result<()> {
    // ── Tracing (same env-driven config as serve) ───────────────────────────
    let log_config = kb_logging::LogConfig::from_env();
    kb_logging::init_tracing(&log_config).context("failed to initialise tracing subscriber")?;

    // ── Load configuration ──────────────────────────────────────────────────
    let app_config = AppConfig::from_path(&args.config).with_context(|| {
        format!(
            "failed to load config from '{}' (create a config.toml or set KB_CONFIG)",
            args.config
        )
    })?;

    // Keep the config-file watcher alive for the life of the process so edits
    // to config.toml (ingest caps, log levels, …) hot-swap without a restart.
    let _config_watcher = match app_config.watch() {
        Ok(w) => Some(w),
        Err(e) => {
            warn!(error = %e, "config file watch unavailable; hot-reload disabled");
            None
        }
    };

    // ── Shutdown signal: SIGINT (ctrl-c) or SIGTERM (container stop) ────────
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        let ctrl_c = tokio::signal::ctrl_c();
        #[cfg(unix)]
        {
            let mut sigterm =
                match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                    Ok(s) => s,
                    Err(e) => {
                        warn!(error = %e, "SIGTERM handler unavailable; ctrl-c only");
                        let _ = ctrl_c.await;
                        let _ = shutdown_tx.send(true);
                        return;
                    }
                };
            tokio::select! {
                _ = ctrl_c => info!("SIGINT received, draining..."),
                _ = sigterm.recv() => info!("SIGTERM received, draining..."),
            }
        }
        #[cfg(not(unix))]
        {
            let _ = ctrl_c.await;
            info!("shutdown signal received, draining...");
        }
        let _ = shutdown_tx.send(true);
    });

    kb_api::worker_pool::worker_main(&app_config, args.concurrency, shutdown_rx).await
}
