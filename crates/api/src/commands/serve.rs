//! `kb serve` — start the HTTP API server (plan §10, §12).
//!
//! This command wires up all pipeline components (PgStore, scheduler pool,
//! LLM clients, extractors, ingest/retrieval pipelines, blob store, job
//! queue, session store) and starts the axum HTTP server on the configured
//! port (default 9999). The server handles auth, ingest, search, document
//! detail, file download, job status, Prometheus metrics, the Web UI, and
//! the admin panel.
//!
//! Graceful shutdown is triggered by SIGINT or SIGTERM.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use kb_config::AppConfig;
use kb_core::blob::Blob;
use kb_core::degradation::DegradationState;
use kb_core::kind::DocKind;
use kb_core::session::SessionStore;
use kb_extract::code::CodeExtractor;
use kb_extract::text::TextExtractor;
use kb_llm::{JsonSchemaTagger, LlamaClient, LlamaReranker};
use kb_pipeline::embedder::ChunkEmbedder;
use kb_pipeline::ingest::{ExtractorRouter, IngestPipeline};
use kb_pipeline::job_queue::JobQueue;
use kb_pipeline::retrieval::RetrievalPipeline;
use kb_pipeline::tag_canonicalizer::{TAG_MERGE_THRESHOLD, TagCanonicalizer};
use kb_scheduler::HealthLoop;
use kb_scheduler::Pool;
use kb_scheduler::Reloader;
use kb_store::ROUTING_CHANNEL;
use kb_store::{LocalBlob, PgRoutingNotifier, PgSessionStore, PgStore};
use tracing::info;

use crate::cli::ServeArgs;
use kb_api::AppState;
use kb_api::backpressure::InflightLimiter;
use kb_api::metrics_collector;
use kb_api::stripe_client;

/// Default embedding dimension (matches the schema embedder lock-in, plan §5).
const EMBED_DIM: usize = 1024;

/// Default model names for OpenAI-compatible API requests.
const DEFAULT_CHAT_MODEL: &str = "default";
const DEFAULT_EMBED_MODEL: &str = "bge-m3";
const DEFAULT_RERANK_MODEL: &str = "default";

/// Number of consecutive failures before a per-backend circuit breaker trips.
const CIRCUIT_THRESHOLD: u32 = 5;

/// How long a tripped circuit breaker stays open.
const COOLDOWN_SECS: u64 = 30;

/// Run the `kb serve` command: load config, wire up all components, and start
/// the HTTP server.
///
/// # Errors
///
/// Returns an error if config loading fails, database connection fails,
/// or the server fails to bind.
pub async fn run_serve(args: &ServeArgs) -> anyhow::Result<()> {
    // ── Install rustls crypto provider (needed for TLS) ───────────────────
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("failed to install rustls ring crypto provider");

    // ── Initialise tracing ──────────────────────────────────────────────────
    let log_config = kb_logging::LogConfig::from_env();
    kb_logging::init_tracing(&log_config).context("failed to initialise tracing subscriber")?;

    // ── Start log janitor (belt-and-suspenders total-disk-cap enforcement) ─
    let janitor = kb_logging::LogJanitor::new(&log_config, Duration::from_secs(300));
    let (janitor_shutdown, janitor_rx) = tokio::sync::watch::channel(false);
    let janitor_handle = janitor.spawn(janitor_rx);
    info!(
        log_dir = %log_config.log_dir.display(),
        max_gb = log_config.log_max_gb,
        "log janitor started"
    );

    // ── Initialise metrics ──────────────────────────────────────────────────
    let _ = kb_metrics::init_metrics();

    // ── Load configuration ──────────────────────────────────────────────────
    let app_config = AppConfig::from_path(&args.config).with_context(|| {
        format!(
            "failed to load config from '{}' (create a config.toml or set KB_CONFIG)",
            args.config
        )
    })?;

    let cfg = app_config.current();

    let bind_port = args.port.unwrap_or(cfg.api.port);

    // ── Connect to Postgres ─────────────────────────────────────────────────
    let pg_store = if cfg.storage.app_postgres_url.is_empty() {
        anyhow::ensure!(
            !cfg.storage.postgres_url.is_empty(),
            "storage.postgres_url is not set in {} — a Postgres database is required",
            args.config
        );
        PgStore::new(&cfg.storage.postgres_url)
    } else {
        PgStore::with_roles(&cfg.storage.postgres_url, &cfg.storage.app_postgres_url)
    };

    pg_store
        .connect()
        .await
        .context("failed to connect to Postgres — is the database running?")?;

    // ── Bootstrap first-run seeding ──────────────────────────────────────────
    kb_api::bootstrap::bootstrap_seed(&pg_store)
        .await
        .context("bootstrap seed failed")?;

    let pg_store = Arc::new(pg_store);

    // ── Session store ───────────────────────────────────────────────────────
    // Use the admin pool for the sessions table (not tenant-scoped).
    let session_pool = pg_store
        .admin_pool()
        .context("failed to get admin pool for session store")?;
    let session_store: Arc<dyn SessionStore> = Arc::new(PgSessionStore::new(session_pool));

    // ── Build shared infrastructure ─────────────────────────────────────────
    let pool = Pool::from_config(&cfg);

    // ── Routing hot-reload (plan §26.5, P9-T7) ────────────────────────────
    let reloader = Reloader::new(pool.clone());

    // Startup load: try DB first, fall back to config.toml backends.
    let db_entries = pg_store
        .get_routing_table()
        .await
        .context("failed to load routing table from database")?;
    reloader
        .startup_load(db_entries, Some(&cfg))
        .await
        .context("failed to perform routing startup load")?;

    // Spawn the hot-reload background loop.
    let admin_url_fn = {
        let s = Arc::clone(&pg_store);
        move || s.admin_url_snapshot()
    };
    let notifier = PgRoutingNotifier::new(
        admin_url_fn,
        ROUTING_CHANNEL.into(),
        Duration::from_secs(30), // poll fallback every 30s
    );
    let reloader_clone = reloader.clone();
    let pg_store_clone = Arc::clone(&pg_store);
    tokio::spawn(async move {
        if let Err(e) = reloader_clone
            .run(notifier, move || {
                let store = Arc::clone(&pg_store_clone);
                async move { store.get_routing_table().await }
            })
            .await
        {
            tracing::error!(error = %e, "routing hot-reload loop exited");
        }
    });
    info!("routing hot-reload background task started");

    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(120))
        .build()
        .context("failed to create HTTP client")?;

    let llm_factory = || {
        LlamaClient::new(
            pool.clone(),
            http.clone(),
            cfg.scheduler.max_retries,
            CIRCUIT_THRESHOLD,
            Duration::from_secs(COOLDOWN_SECS),
        )
    };

    // ── Build extractor router ──────────────────────────────────────────────
    let mut extractors: ExtractorRouter = HashMap::new();
    extractors.insert(DocKind::Document, Arc::new(TextExtractor));
    extractors.insert(DocKind::Code, Arc::new(CodeExtractor));

    // ── Build pipeline components ───────────────────────────────────────────
    let tagger = Arc::new(JsonSchemaTagger::new(
        llm_factory(),
        DEFAULT_CHAT_MODEL.into(),
    ));

    let embed_llm = Arc::new(llm_factory());
    let embedder = Arc::new(ChunkEmbedder::new(
        Arc::clone(&embed_llm),
        DEFAULT_EMBED_MODEL.into(),
        EMBED_DIM,
    ));
    let canonicalizer = Arc::new(TagCanonicalizer::new(
        Arc::clone(&pg_store) as Arc<dyn kb_pipeline::TagStore>,
        Arc::clone(&embed_llm),
        DEFAULT_EMBED_MODEL.into(),
        TAG_MERGE_THRESHOLD,
    ));

    let reranker = Arc::new(LlamaReranker::new(
        Arc::clone(&embed_llm),
        DEFAULT_RERANK_MODEL.into(),
    ));

    // ── Blob store ──────────────────────────────────────────────────────────
    let blob: Arc<dyn Blob> = Arc::new(LocalBlob::new("kb-blobs".into(), "default".into()));

    // ── Ingest pipeline ─────────────────────────────────────────────────────
    let ingest_pipeline = Arc::new(IngestPipeline::new(
        Arc::clone(&blob),
        extractors,
        tagger,
        canonicalizer,
        embedder.clone(),
        Arc::clone(&pg_store) as Arc<dyn kb_pipeline::ingest::IngestStore>,
    ));

    // ── Retrieval pipeline ──────────────────────────────────────────────────
    let retrieval_pipeline = Arc::new(RetrievalPipeline::new(
        embedder,
        Arc::clone(&pg_store) as Arc<dyn kb_core::store::Store>,
        reranker,
    ));

    // ── Job queue ───────────────────────────────────────────────────────────
    let job_queue_pool = pg_store
        .admin_pool()
        .context("failed to get admin pool for job queue")?;
    let job_queue = Arc::new(JobQueue::new(job_queue_pool, 1000, 5));

    // ── Start background health loop ────────────────────────────────────────
    let all_backends = pool.all_backends();
    let health_interval = Duration::from_secs(cfg.scheduler.health_interval_secs);
    let (health_handle, health_shutdown): (
        tokio::task::JoinHandle<()>,
        tokio::sync::watch::Sender<()>,
    ) = HealthLoop::new(all_backends, http.clone(), health_interval).spawn();
    info!(
        interval_secs = cfg.scheduler.health_interval_secs,
        "health loop started"
    );

    // ── Presigned URL TTL (P8-T3) ────────────────────────────────────────────
    let presigned_ttl = {
        let ttl: u64 = std::env::var("BLOB_PRESIGNED_TTL_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(kb_api::DEFAULT_PRESIGNED_TTL_SECS);
        Duration::from_secs(ttl)
    };
    info!(ttl_secs = presigned_ttl.as_secs(), "presigned URL TTL");

    // ── Degradation state (P8-T9) ────────────────────────────────────────────
    let degradation = Arc::new(DegradationState::default());
    let inflight_limiter = Arc::new(InflightLimiter::new(
        cfg.degradation.max_inflight_ingest as usize,
    ));
    info!(
        max_inflight_ingest = cfg.degradation.max_inflight_ingest,
        blob_circuit_threshold = cfg.degradation.blob_circuit_breaker_threshold,
        blob_circuit_cooldown_secs = cfg.degradation.blob_circuit_breaker_cooldown_secs,
        "degradation state initialised"
    );

    // ── Assemble app state ──────────────────────────────────────────────────
    let backend_pool = Arc::new(pool);

    // Stripe client (P11-T2): optional, gated on STRIPE_SECRET_KEY env var.
    let stripe_client: Option<Arc<dyn stripe_client::StripeClient>> =
        std::env::var("STRIPE_SECRET_KEY").ok().map(|key| {
            let client = stripe_client::RealStripeClient::new(key, http.clone());
            Arc::new(client) as Arc<dyn stripe_client::StripeClient>
        });

    // Stripe webhook signing secret (P11-T3): optional, gated on STRIPE_WEBHOOK_SECRET env var.
    let webhook_secret = std::env::var("STRIPE_WEBHOOK_SECRET").ok();

    // Public base URL for Stripe callback construction (P11-T2).
    let public_base_url =
        std::env::var("PUBLIC_BASE_URL").unwrap_or_else(|_| format!("http://0.0.0.0:{bind_port}"));

    let mut state = AppState {
        session_store,
        pg_store,
        session_ttl: Duration::from_secs(kb_core::session::DEFAULT_SESSION_TTL_SECS),
        secure_cookies: cfg.api.secure_cookies,
        ingest_pipeline: Some(ingest_pipeline),
        retrieval_pipeline: Some(retrieval_pipeline),
        blob: Some(blob),
        job_queue: Some(job_queue),
        backend_pool: Some(backend_pool.clone()),
        blob_presigned_ttl: presigned_ttl,
        degradation: Some(degradation.clone()),
        inflight_limiter: Some(inflight_limiter),
        stripe_client,
        stripe_webhook_secret: None,
        public_base_url,
        email_sender: Some(Arc::new(kb_core::email::LogEmail)),
        email_provider: Some(Arc::new(kb_core::email::LogEmail)),
        api_token_store: None,
    };
    if let Some(ws) = webhook_secret {
        use std::sync::Arc;
        state.stripe_webhook_secret = Some(Arc::new(arc_swap::ArcSwap::new(Arc::new(ws))));
    }

    // ── Build router ────────────────────────────────────────────────────────
    let router = kb_api::build_router(state);

    // ── Start metrics collector ─────────────────────────────────────────────
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let collector_pool = backend_pool.clone();
    tokio::spawn(async move {
        metrics_collector::start_collector(collector_pool, Duration::from_secs(30), shutdown_rx)
            .await;
    });

    // ── Bind and serve ──────────────────────────────────────────────────────
    let addr = SocketAddr::from(([0, 0, 0, 0], bind_port));

    // Graceful shutdown: on ctrl_c, signal the collector, stop the health
    // loop, stop the log janitor, close the listener, and wait for in-flight
    // requests to drain.
    let shutdown_signal = async move {
        let _ = tokio::signal::ctrl_c().await;
        info!("shutdown signal received, draining...");
        let _ = shutdown_tx.send(true);
        let _ = health_shutdown.send(());
        let _ = janitor_shutdown.send(true);
    };

    match (args.tls_cert.as_deref(), args.tls_key.as_deref()) {
        (Some(cert_path), Some(key_path)) => {
            // ── TLS (HTTPS) path ────────────────────────────────────────────
            use axum_server::tls_rustls::{RustlsConfig, bind_rustls};

            let tls_config = RustlsConfig::from_pem_file(cert_path, key_path)
                .await
                .map_err(|e| anyhow::anyhow!("failed to load TLS config: {e}"))?;

            let handle = axum_server::Handle::new();
            let shutdown_handle = handle.clone();

            // Spawn graceful-shutdown watcher.
            tokio::spawn(async move {
                shutdown_signal.await;
                shutdown_handle.graceful_shutdown(Some(Duration::from_secs(30)));
            });

            info!("kb API server listening on https://{addr}");

            bind_rustls(addr, tls_config)
                .handle(handle)
                .serve(router.into_make_service())
                .await
                .context("TLS server error")?;
        }
        _ => {
            // ── Plain HTTP path ─────────────────────────────────────────────
            let listener = tokio::net::TcpListener::bind(addr)
                .await
                .with_context(|| format!("failed to bind to {addr}"))?;

            info!("kb API server listening on http://{addr}");

            axum::serve(listener, router)
                .with_graceful_shutdown(shutdown_signal)
                .await
                .context("server error")?;
        }
    }

    // Wait for background tasks to drain.
    let _ = tokio::time::timeout(Duration::from_secs(10), janitor_handle).await;
    let _ = tokio::time::timeout(Duration::from_secs(10), health_handle).await;

    info!("server shut down cleanly");
    Ok(())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use crate::cli::Cli;
    use crate::cli::Command;
    use clap::Parser;

    #[test]
    fn serve_args_default_port_is_none() {
        let cli = Cli::try_parse_from(["kb", "serve"]).unwrap();
        match cli.command {
            Command::Serve(args) => {
                assert!(args.port.is_none());
            }
            other => panic!("expected Serve, got {other:?}"),
        }
    }

    #[test]
    fn serve_args_custom_port() {
        let cli = Cli::try_parse_from(["kb", "serve", "--port", "8080"]).unwrap();
        match cli.command {
            Command::Serve(args) => {
                assert_eq!(args.port, Some(8080));
            }
            other => panic!("expected Serve, got {other:?}"),
        }
    }

    #[test]
    fn serve_args_config_default() {
        let cli = Cli::try_parse_from(["kb", "serve"]).unwrap();
        match cli.command {
            Command::Serve(args) => {
                assert_eq!(args.config, "config.toml");
            }
            other => panic!("expected Serve, got {other:?}"),
        }
    }

    #[test]
    fn serve_args_custom_config() {
        let cli = Cli::try_parse_from(["kb", "serve", "--config", "/etc/kb/config.toml"]).unwrap();
        match cli.command {
            Command::Serve(args) => {
                assert_eq!(args.config, "/etc/kb/config.toml");
            }
            other => panic!("expected Serve, got {other:?}"),
        }
    }
}
