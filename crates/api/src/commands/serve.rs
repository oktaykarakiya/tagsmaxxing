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

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use axum::http::{StatusCode, header};
use axum::response::Response;
use kb_config::AppConfig;
use kb_core::degradation::DegradationState;
use kb_store::api_token_store::InMemoryApiTokenStore;
use tracing::{info, warn};

use super::runtime;
use crate::cli::ServeArgs;
use kb_api::AppState;
use kb_api::backpressure::InflightLimiter;
use kb_api::metrics_collector;
use kb_api::stripe_client;

/// `map_response` middleware: inject a `Retry-After: 5` header on every 429 and
/// 503 response produced by the app.
///
/// This ensures backpressure rejections (429), backend-unavailable responses
/// (503, e.g. the tagger has no healthy backend — campaign finding F4),
/// rate-limiters, and any other such site automatically carry actionable retry
/// guidance without each handler having to construct a full `Response` with
/// headers.
async fn ensure_retry_after(response: Response) -> Response {
    let retryable = matches!(
        response.status(),
        StatusCode::TOO_MANY_REQUESTS | StatusCode::SERVICE_UNAVAILABLE
    );
    if retryable && !response.headers().contains_key(header::RETRY_AFTER) {
        let (parts, body) = response.into_parts();
        let mut response = Response::from_parts(parts, body);
        response
            .headers_mut()
            .insert(header::RETRY_AFTER, header::HeaderValue::from_static("5"));
        response
    } else {
        response
    }
}

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
        .map_err(|_| anyhow::anyhow!("failed to install rustls ring crypto provider"))?;

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

    // ── Build the shared processing runtime (P15-T4) ─────────────────────────
    // Store, scheduler pool + DB routing reload, extractors, tagger/embedder/
    // canonicalizer/reranker, blob, job queue, usage metering, health loop —
    // all extracted into runtime::build_runtime so `kb worker` wires the exact
    // same stack. `true` = serve loads the global DB routing table.
    let rt = runtime::build_runtime(&app_config, true).await?;
    let runtime::PipelineRuntime {
        pg_store,
        session_store,
        backend_pool,
        blob,
        ingest_pipeline,
        retrieval_pipeline,
        job_queue,
        health_handle,
        health_shutdown,
        usage_flush_handle,
    } = rt;

    // Keep the config-file watcher alive for the life of the server so edits
    // to config.toml (ingest.mode, queue caps, …) hot-swap without a restart.
    let _config_watcher = match app_config.watch() {
        Ok(w) => Some(w),
        Err(e) => {
            warn!(error = %e, "config file watch unavailable; hot-reload disabled");
            None
        }
    };

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
    // Global ceiling + per-tenant fair-share (P14-T7). A per_tenant value of 0
    // tells the limiter to auto-derive max(1, global / 4), so a single tenant
    // can hold at most a quarter of the global pool and cannot 429 everyone.
    let inflight_limiter = Arc::new(InflightLimiter::with_per_tenant(
        cfg.degradation.max_inflight_ingest as usize,
        cfg.degradation.per_tenant_max_inflight as usize,
    ));
    info!(
        max_inflight_ingest = cfg.degradation.max_inflight_ingest,
        per_tenant_max_inflight = inflight_limiter.per_tenant_max(),
        blob_circuit_threshold = cfg.degradation.blob_circuit_breaker_threshold,
        blob_circuit_cooldown_secs = cfg.degradation.blob_circuit_breaker_cooldown_secs,
        "degradation state initialised"
    );

    // ── Assemble app state ──────────────────────────────────────────────────
    // Clone the store for the metrics collector before `pg_store` is moved into
    // `AppState` below (BUG-OBS-06: the collector reads global queue depth and
    // per-tenant storage usage each tick).
    let collector_pg_store = Arc::clone(&pg_store);
    // Likewise clone the in-flight limiter for the collector before it is moved
    // into `AppState` (P14-T9): the collector publishes its `in_flight()` count
    // as `kb_inflight_ingest`, reading the same shared limiter the ingest path
    // acquires permits from.
    let collector_inflight = Arc::clone(&inflight_limiter);

    // Stripe client (P11-T2): optional, gated on STRIPE_SECRET_KEY env var.
    let stripe_http = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .context("failed to create Stripe HTTP client")?;
    let stripe_client: Option<Arc<dyn stripe_client::StripeClient>> =
        std::env::var("STRIPE_SECRET_KEY").ok().map(|key| {
            let client = stripe_client::RealStripeClient::new(key, stripe_http);
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
        api_token_store: Some(Arc::new(InMemoryApiTokenStore::new())),
        app_config: Some(app_config.clone()),
    };
    if let Some(ws) = webhook_secret {
        use std::sync::Arc;
        state.stripe_webhook_secret = Some(Arc::new(arc_swap::ArcSwap::new(Arc::new(ws))));
    }

    // ── Build router ────────────────────────────────────────────────────────
    let router = kb_api::build_router(state);

    // ── Layer: ensure every 429/503 in the app carries a Retry-After header ───
    // This is applied as a map_response layer so that any handler or
    // middleware returning 429 (the backpressure gate) or 503 (the ingest
    // handler when the tagger backend is unavailable) automatically gets the
    // header without each site having to construct a full Response.
    let router = router.layer(axum::middleware::map_response(ensure_retry_after));

    // ── Start metrics collector ─────────────────────────────────────────────
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let collector_pool = backend_pool.clone();
    // The collector also publishes the in-memory runtime gauges (P14-T9): the
    // per-subsystem degraded flags and the in-flight ingest count. Both handles
    // were cloned above (`collector_inflight`, and `degradation` survives its
    // `.clone()` into AppState), so the collector reads the *same* live state the
    // request path mutates.
    let collector_degradation = Arc::clone(&degradation);
    tokio::spawn(async move {
        metrics_collector::start_collector(
            collector_pool,
            collector_pg_store,
            collector_degradation,
            collector_inflight,
            Duration::from_secs(30),
            shutdown_rx,
        )
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
            warn!(
                "EXPERIMENTAL BUILD — not ready for production. Features and data formats may change without notice."
            );

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
            warn!(
                "EXPERIMENTAL BUILD — not ready for production. Features and data formats may change without notice."
            );

            axum::serve(listener, router)
                .with_graceful_shutdown(shutdown_signal)
                .await
                .context("server error")?;
        }
    }

    // Wait for background tasks to drain.
    let _ = tokio::time::timeout(Duration::from_secs(10), janitor_handle).await;
    let _ = tokio::time::timeout(Duration::from_secs(10), health_handle).await;

    // Give the usage-recorder flush task a moment to drain any buffered events
    // (P14-T1). It runs continuously during operation, so the buffer is near-
    // empty here; it only fully returns once every sender (held inside the
    // router/AppState) is dropped, so this is a best-effort timed join.
    let _ = tokio::time::timeout(Duration::from_secs(5), usage_flush_handle).await;

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

    /// F4: the retry-after middleware tags both 429 and 503 (and leaves other
    /// statuses + an existing header untouched).
    #[tokio::test]
    async fn ensure_retry_after_sets_header_on_429_and_503() {
        use axum::body::Body;
        use axum::http::{StatusCode, header};
        use axum::response::Response;

        let make = |status: StatusCode| {
            let mut r = Response::new(Body::empty());
            *r.status_mut() = status;
            r
        };

        for status in [
            StatusCode::TOO_MANY_REQUESTS,
            StatusCode::SERVICE_UNAVAILABLE,
        ] {
            let resp = super::ensure_retry_after(make(status)).await;
            assert_eq!(
                resp.headers()
                    .get(header::RETRY_AFTER)
                    .and_then(|v| v.to_str().ok()),
                Some("5"),
                "status {status} must carry Retry-After"
            );
        }

        // A 200 OK is untouched.
        let ok = super::ensure_retry_after(make(StatusCode::OK)).await;
        assert!(ok.headers().get(header::RETRY_AFTER).is_none());

        // An existing Retry-After is preserved, not overwritten.
        let mut pre = make(StatusCode::SERVICE_UNAVAILABLE);
        pre.headers_mut()
            .insert(header::RETRY_AFTER, header::HeaderValue::from_static("120"));
        let kept = super::ensure_retry_after(pre).await;
        assert_eq!(
            kept.headers()
                .get(header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok()),
            Some("120")
        );
    }

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
