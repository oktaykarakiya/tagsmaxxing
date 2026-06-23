// SPDX-License-Identifier: AGPL-3.0-or-later

//! `kb` — the Local File Knowledge Base CLI.
//!
//! Entry point for the `kb` binary. Parses arguments via [`clap`], loads
//! configuration from `config.toml` (overlaid with environment variables, per
//! plan §6 and P0-T5), wires up the pipeline components, and dispatches to the
//! appropriate subcommand handler.
//!
//! # Quick start
//!
//! ```text
//! kb ingest notes.txt
//! kb search "quarterly report" --kind document --limit 5
//! ```
//!
//! # Requirements
//!
//! - A `config.toml` file in the current directory (or set `KB_CONFIG`).
//! - A running Postgres instance (see `compose.yaml`).
//! - At least one configured inference backend for LLM-based tagging,
//!   embedding, and reranking.

use std::process;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use clap::Parser;
use kb_config::AppConfig;
use kb_core::kind::DocKind;
use kb_extract::archive::ArchiveExtractor;
use kb_extract::audio::{AudioExtractor, WhisperConfig};
use kb_extract::code::CodeExtractor;
use kb_extract::document::DocumentExtractor;
use kb_extract::email::EmailExtractor;
use kb_extract::image::ImageExtractor;
use kb_extract::tika::{TikaConfig, TikaExtractor};
use kb_extract::video::VideoExtractor;
use kb_llm::{JsonSchemaTagger, LlamaClient, LlamaReranker};
use kb_pipeline::embedder::ChunkEmbedder;
use kb_pipeline::ingest::{ExtractorRouter, IngestPipeline};
use kb_pipeline::retrieval::RetrievalPipeline;
use kb_pipeline::tag_canonicalizer::{TAG_MERGE_THRESHOLD, TagCanonicalizer};
use kb_scheduler::Pool;
use kb_store::{EMBED_DIM, PgStore};

mod cli;
mod commands;

use cli::{Cli, Command};

/// Default model name sent in OpenAI-compatible API requests.
const DEFAULT_CHAT_MODEL: &str = "default";
const DEFAULT_EMBED_MODEL: &str = "qwen3-embed-4b";
const DEFAULT_RERANK_MODEL: &str = "default";

/// Number of consecutive failures before a per-backend circuit breaker trips.
const CIRCUIT_THRESHOLD: u32 = 5;

/// How long a tripped circuit breaker stays open.
const COOLDOWN_SECS: u64 = 30;

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("error: {err:#}");
        process::exit(1);
    }
}

/// Parse CLI args, load config, wire up pipeline components, and dispatch
/// to the correct subcommand handler (ingest or search).
async fn run() -> anyhow::Result<()> {
    // ── Initialise metrics ────────────────────────────────────────────────────
    // Metrics are available for any long-running process (server, watcher, etc.).
    // The background collector is started separately when the HTTP server runs.
    let _ = kb_metrics::init_metrics();

    let cli = Cli::parse();

    // ── Dispatch to subcommand ─────────────────────────────────────────────
    // The `serve` and `worker` subcommands manage their own config loading,
    // tracing, metrics, and component wiring independently — they do not share
    // the CLI bootstrap path (plan §10, P15-T8).
    if let Command::Serve(args) = &cli.command {
        return commands::serve::run_serve(args).await;
    }
    if let Command::Worker(args) = &cli.command {
        return commands::worker::run_worker(args).await;
    }

    // ── Load configuration (ingest/search CLI path) ────────────────────────
    let config_path = std::env::var("KB_CONFIG").unwrap_or_else(|_| "config.toml".into());
    let app_config = AppConfig::from_path(&config_path).with_context(|| {
        format!(
            "failed to load config from '{config_path}' \
             (create a config.toml or set KB_CONFIG)"
        )
    })?;

    let cfg = app_config.current();

    // ── Connect to Postgres ─────────────────────────────────────────────────
    let pg_store = if cfg.storage.app_postgres_url.is_empty() {
        // Single-role mode: RLS relies on explicit tenant_id filters.
        anyhow::ensure!(
            !cfg.storage.postgres_url.is_empty(),
            "storage.postgres_url is not set in {config_path} — \
             a Postgres database is required"
        );
        PgStore::new(&cfg.storage.postgres_url)
    } else {
        // Two-role mode (privileged + kb_app): RLS enforced by the database.
        PgStore::with_roles(&cfg.storage.postgres_url, &cfg.storage.app_postgres_url)
    };

    pg_store
        .connect()
        .await
        .context("failed to connect to Postgres — is the database running?")?;

    let pg_store = Arc::new(pg_store);

    // ── Build shared infrastructure ─────────────────────────────────────────
    // Pool and HTTP client are Clone — we share them across LlamaClient
    // instances (one per component group, so circuit-breaker state is isolated
    // per group while the underlying connection pool and semaphores are shared).
    let pool = Pool::from_config(&cfg);
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

    // ── Build extractor router ─────────────────────────────────────────────
    // Document: native text for plain-text MIME, Tika for PDF/rich docs. Image:
    // EXIF + VLM caption. TIKA_URL is hot-swappable via TikaConfig.
    let tika_url =
        std::env::var("TIKA_URL").unwrap_or_else(|_| "http://localhost:9998".to_string());
    let tika_config = TikaConfig::new(tika_url);
    let mut extractors: ExtractorRouter = std::collections::HashMap::new();
    extractors.insert(
        DocKind::Document,
        Arc::new(DocumentExtractor::new(TikaExtractor::new(
            http.clone(),
            tika_config,
        ))),
    );
    extractors.insert(DocKind::Code, Arc::new(CodeExtractor));
    extractors.insert(DocKind::Image, Arc::new(ImageExtractor));
    // Archive: extract entry text in-process (zip/tar/tar.gz) so archive *contents*
    // are searchable, with bomb/traversal guards (F2). Tika returned only a
    // manifest. OOXML/JAR are reclassified away from zip, so only plain archives
    // reach here.
    extractors.insert(DocKind::Archive, Arc::new(ArchiveExtractor));
    // Audio/video: ffmpeg transcode + whisper-server transcription (BUG-INGEST
    // audio/video). WHISPER_URL is hot-swappable via WhisperConfig.
    let whisper_url =
        std::env::var("WHISPER_URL").unwrap_or_else(|_| "http://localhost:9000".to_string());
    let whisper_config = WhisperConfig::new(whisper_url);
    extractors.insert(
        DocKind::Audio,
        Arc::new(AudioExtractor::new(http.clone(), whisper_config.clone())),
    );
    extractors.insert(
        DocKind::Video,
        Arc::new(VideoExtractor::new(http.clone(), whisper_config)),
    );
    // Other kinds (Image, Binary) fall back to Extracted::default() — no
    // extraction, just deterministic metadata.
    extractors.insert(DocKind::Email, Arc::new(EmailExtractor));

    match cli.command {
        Command::Ingest(args) => {
            // Owned LlamaClient for the tagger (JsonSchemaTagger takes by value).
            // The tagger + embedder meter token usage into usage_events via the
            // store (BUG-BILL-03).
            let tagger = Arc::new(
                JsonSchemaTagger::new(llm_factory(), DEFAULT_CHAT_MODEL.into())
                    .with_usage_recorder(
                        Arc::clone(&pg_store) as Arc<dyn kb_core::usage::UsageRecorder>
                    ),
            );
            // Shared LlamaClient for embedder + canonicalizer.
            let embed_llm = Arc::new(llm_factory());
            let embedder = Arc::new(
                ChunkEmbedder::new(
                    Arc::clone(&embed_llm),
                    DEFAULT_EMBED_MODEL.into(),
                    EMBED_DIM,
                )
                .with_usage_recorder(
                    Arc::clone(&pg_store) as Arc<dyn kb_core::usage::UsageRecorder>
                ),
            );
            let canonicalizer = Arc::new(
                TagCanonicalizer::new(
                    Arc::clone(&pg_store) as Arc<dyn kb_pipeline::TagStore>,
                    Arc::clone(&embed_llm),
                    DEFAULT_EMBED_MODEL.into(),
                    TAG_MERGE_THRESHOLD,
                )
                .with_usage_recorder(
                    Arc::clone(&pg_store) as Arc<dyn kb_core::usage::UsageRecorder>
                ),
            );

            let blob: Arc<dyn kb_core::blob::Blob> = Arc::new(kb_store::LocalBlob::new(
                "kb-blobs".into(),
                cli.tenant_id.to_string(),
            ));

            let pipeline = IngestPipeline::new(
                blob,
                extractors,
                tagger,
                canonicalizer,
                embedder,
                Arc::clone(&pg_store) as Arc<dyn kb_pipeline::ingest::IngestStore>,
            );

            commands::ingest::run_ingest(&args, &pipeline, cli.tenant_id).await?;
        }
        Command::Search(args) => {
            let embed_llm = Arc::new(llm_factory());
            let embedder = Arc::new(ChunkEmbedder::new(
                Arc::clone(&embed_llm),
                DEFAULT_EMBED_MODEL.into(),
                EMBED_DIM,
            ));
            let reranker = Arc::new(LlamaReranker::new(
                Arc::clone(&embed_llm),
                DEFAULT_RERANK_MODEL.into(),
            ));

            let pipeline = RetrievalPipeline::new(
                embedder,
                Arc::clone(&pg_store) as Arc<dyn kb_core::store::Store>,
                reranker,
            );

            commands::search::run_search(&args, &pipeline, cli.tenant_id).await?;
        }
        Command::Serve(_) | Command::Worker(_) => {
            // Already handled by the early returns above.
            return anyhow::Ok(());
        }
    }

    Ok(())
}
