// SPDX-License-Identifier: AGPL-3.0-or-later

//! Shared component wiring for `kb serve` and `kb worker` (P15-T4).
//!
//! Both processes need the same stack — Postgres store, scheduler pool with
//! routing, extractors (Tika/whisper), tagger/embedder/canonicalizer/reranker,
//! blob store, job queue, usage metering, health loop. [`build_runtime`]
//! constructs it once from the hot-swappable [`AppConfig`]; `serve` then adds
//! its HTTP-only state (sessions UI, limiter, Stripe, …) while `worker` runs a
//! claim loop. Extracted from `serve.rs` so the two stay in lock-step (§31.4:
//! one responsibility per module).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use kb_config::AppConfig;
use kb_core::blob::Blob;
use kb_core::kind::DocKind;
use kb_core::session::SessionStore;
use kb_extract::archive::ArchiveExtractor;
use kb_extract::audio::{AudioExtractor, WhisperConfig};
use kb_extract::code::CodeExtractor;
use kb_extract::document::DocumentExtractor;
use kb_extract::email::EmailExtractor;
use kb_extract::image::ImageExtractor;
use kb_extract::tika::{TikaConfig, TikaExtractor};
use kb_extract::video::VideoExtractor;
use kb_llm::{JsonSchemaTagger, LlamaClient, LlamaReranker, VisionCaptioner};
use kb_pipeline::embedder::ChunkEmbedder;
use kb_pipeline::ingest::{ExtractorRouter, IngestPipeline};
use kb_pipeline::job_queue::JobQueue;
use kb_pipeline::retrieval::RetrievalPipeline;
use kb_pipeline::tag_canonicalizer::{TAG_MERGE_THRESHOLD, TagCanonicalizer};
use kb_scheduler::{HealthLoop, Pool, Reloader};
use kb_store::ROUTING_CHANNEL;
use kb_store::{
    B2Blob, B2Config, BufferedUsageRecorder, EMBED_DIM, EncryptedBlob, LocalBlob,
    PgRoutingNotifier, PgSessionStore, PgStore,
};
use tracing::info;

/// Default model names for OpenAI-compatible API requests.
pub(crate) const DEFAULT_CHAT_MODEL: &str = "default";
pub(crate) const DEFAULT_EMBED_MODEL: &str = "qwen3-embed-4b";
pub(crate) const DEFAULT_RERANK_MODEL: &str = "default";
const DEFAULT_VISION_MODEL: &str = "default";

/// Number of consecutive failures before a per-backend circuit breaker trips.
pub(crate) const CIRCUIT_THRESHOLD: u32 = 5;

/// How long a tripped circuit breaker stays open.
pub(crate) const COOLDOWN_SECS: u64 = 30;

/// The fully wired processing stack shared by `kb serve` and `kb worker`.
///
/// Handles for the spawned background tasks are exposed so the owner can
/// signal + join them at shutdown.
pub struct PipelineRuntime {
    /// Connected Postgres store (admin + RLS app pools).
    pub pg_store: Arc<PgStore>,
    /// Postgres-backed session store (admin pool).
    pub session_store: Arc<dyn SessionStore>,
    /// Scheduler pool over this process's model backends.
    pub backend_pool: Arc<Pool>,
    /// Blob store selected by `[blob]` (see [`build_blob`]).
    pub blob: Arc<dyn Blob>,
    /// The full ingest pipeline (extract → tag → embed → store).
    pub ingest_pipeline: Arc<IngestPipeline>,
    /// The search/retrieval pipeline (embed → hybrid search → rerank).
    pub retrieval_pipeline: Arc<RetrievalPipeline>,
    /// Durable job queue on the admin pool, lease-configured from `[worker]`.
    pub job_queue: Arc<JobQueue>,
    /// Join handle for the backend health loop.
    pub health_handle: tokio::task::JoinHandle<()>,
    /// Shutdown signal for the backend health loop.
    pub health_shutdown: tokio::sync::watch::Sender<()>,
    /// Join handle for the buffered usage-recorder drain task (P14-T1).
    pub usage_flush_handle: tokio::task::JoinHandle<()>,
}

/// Build the blob store selected by `[blob]` (P15-T4, plan §20).
///
/// * `"local"` — node-filesystem [`LocalBlob`] (self-provisions envelope
///   encryption from `BLOB_KEK_B64`/`BLOB_KEK_FILE`). Single box only.
/// * `"s3"` — any S3-compatible store ([`B2Blob`]: MinIO, Backblaze B2, …).
///   Credentials come from `BLOB_S3_ACCESS_KEY` / `BLOB_S3_SECRET_KEY` (never
///   the config file); when a blob KEK is provisioned the store is wrapped in
///   [`EncryptedBlob`] so objects are envelope-encrypted at rest, matching the
///   local backend's behavior. Required for multi-machine workers.
///
/// # Errors
/// Returns an error for an unknown backend kind, a missing s3
/// bucket/credentials, or an s3 client construction failure.
pub fn build_blob(cfg: &kb_config::Config) -> anyhow::Result<Arc<dyn Blob>> {
    match cfg.blob.backend.as_str() {
        "local" => Ok(Arc::new(LocalBlob::new(
            cfg.blob.local_root.clone().into(),
            cfg.blob.local_prefix.clone(),
        ))),
        "s3" => {
            anyhow::ensure!(
                !cfg.blob.s3.bucket.is_empty(),
                "[blob].backend = \"s3\" requires [blob.s3].bucket"
            );
            let access_key = std::env::var("BLOB_S3_ACCESS_KEY")
                .context("[blob].backend = \"s3\" requires the BLOB_S3_ACCESS_KEY env var")?;
            let secret_key = std::env::var("BLOB_S3_SECRET_KEY")
                .context("[blob].backend = \"s3\" requires the BLOB_S3_SECRET_KEY env var")?;
            let b2 = B2Blob::new(B2Config::new(
                access_key,
                secret_key,
                cfg.blob.s3.bucket.clone(),
                cfg.blob.s3.endpoint.clone(),
            ))
            .context("failed to construct the S3-compatible blob client")?;

            // Match LocalBlob: when a blob KEK is provisioned, encrypt at rest.
            match kb_core::kek::FileKek::from_env() {
                Ok(kek) => Ok(Arc::new(EncryptedBlob::new(
                    Arc::new(b2),
                    Arc::new(kek) as Arc<dyn kb_core::kek::KeyEncryptionKey>,
                ))),
                Err(_) => Ok(Arc::new(b2)),
            }
        }
        other => anyhow::bail!("unknown [blob].backend '{other}' (expected \"local\" or \"s3\")"),
    }
}

/// Build the full shared processing runtime (P15-T4).
///
/// `load_db_routing` controls whether the scheduler loads the global DB
/// routing table (with LISTEN/NOTIFY hot reload): `kb serve` passes `true`
/// (current behavior); `kb worker` passes `cfg.worker.use_db_routing`
/// (default **false**, so a worker's capacity comes solely from its own
/// per-machine `[[backend]]` slots — exact enforcement, no cross-process
/// over-subscription).
///
/// Configuration is read per call from `app_config` at the few spots that are
/// construction-time by nature; everything hot-swappable (Tika/whisper URLs,
/// routing, blob KEK, …) keeps its existing `ArcSwap`/notify reload paths.
///
/// # Errors
/// Returns an error if Postgres is unreachable, bootstrap seeding fails,
/// routing startup fails, or any component cannot be constructed.
pub async fn build_runtime(
    app_config: &AppConfig,
    load_db_routing: bool,
) -> anyhow::Result<PipelineRuntime> {
    let cfg = app_config.current();

    // ── Connect to Postgres ─────────────────────────────────────────────────
    let pg_store = if cfg.storage.app_postgres_url.is_empty() {
        anyhow::ensure!(
            !cfg.storage.postgres_url.is_empty(),
            "storage.postgres_url is not set — a Postgres database is required",
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
    crate::bootstrap::bootstrap_seed(&pg_store)
        .await
        .context("bootstrap seed failed")?;

    let pg_store = Arc::new(pg_store);

    // ── Session store ───────────────────────────────────────────────────────
    // Admin pool: the sessions table is not tenant-scoped.
    let session_pool = pg_store
        .admin_pool()
        .context("failed to get admin pool for session store")?;
    let session_store: Arc<dyn SessionStore> = Arc::new(PgSessionStore::new(session_pool));

    // ── Scheduler pool + routing ────────────────────────────────────────────
    let pool = Pool::from_config(&cfg);

    if load_db_routing {
        let reloader = Reloader::new(pool.clone());
        // Startup load: apply the DB routing snapshot (materializing routed
        // models into live backends, BUG-SCHED-03); an empty routes table
        // leaves the pool in legacy flat-priority mode serving the config
        // [[backend]]s. Then spawn the LISTEN/NOTIFY hot-reload loop
        // (plan §26.5, P9-T7).
        let db_entries = pg_store
            .get_routing_table()
            .await
            .context("failed to load routing table from database")?;
        reloader.startup_load(db_entries);

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
    } else {
        // Per-machine capacity mode (workers, P15): no routing table at all —
        // the pool stays in legacy flat-priority mode, where acquire works
        // directly off this machine's [[backend]] entries and their slot
        // semaphores (exact local enforcement). With `use_db_routing = true`
        // a worker behaves like serve: DB routes are materialized and roles
        // without usable routes fall back to these local backends
        // (BUG-SCHED-03 fixed the previously-broken resolution).
        info!("per-machine backends (legacy flat-priority; DB routing not loaded)");
    }

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

    // ── Extractor router ────────────────────────────────────────────────────
    // Tika (TIKA_URL, hot-swappable via TikaConfig) reads rich documents; the
    // Document extractor uses native text extraction for plain text and falls
    // back to Tika for binary documents; Archive reads entry text in-process.
    let tika_url =
        std::env::var("TIKA_URL").unwrap_or_else(|_| "http://localhost:9998".to_string());
    let tika_config = TikaConfig::new(tika_url);
    let mut extractors: ExtractorRouter = HashMap::new();
    extractors.insert(
        DocKind::Document,
        Arc::new(DocumentExtractor::new(TikaExtractor::new(
            http.clone(),
            tika_config,
        ))),
    );
    extractors.insert(DocKind::Code, Arc::new(CodeExtractor));
    extractors.insert(DocKind::Image, Arc::new(ImageExtractor));
    // Archive: extract entry text in-process (zip/tar/tar.gz) so archive contents
    // are searchable, with bomb/traversal guards (F2). Only plain archives reach
    // here (OOXML/JAR are reclassified away from zip).
    extractors.insert(DocKind::Archive, Arc::new(ArchiveExtractor));
    // Audio/video: ffmpeg transcode + whisper transcription (WHISPER_URL is
    // hot-swappable via the WhisperConfig ArcSwap).
    let whisper_url =
        std::env::var("WHISPER_URL").unwrap_or_else(|_| "http://localhost:8083".to_string());
    let whisper_config = WhisperConfig::new(whisper_url);
    extractors.insert(
        DocKind::Audio,
        Arc::new(AudioExtractor::new(http.clone(), whisper_config.clone())),
    );
    extractors.insert(
        DocKind::Video,
        Arc::new(VideoExtractor::new(http.clone(), whisper_config)),
    );
    extractors.insert(DocKind::Email, Arc::new(EmailExtractor));

    // ── Pipeline components ─────────────────────────────────────────────────
    // Metering goes through the fail-open BufferedUsageRecorder (P14-T1) so a
    // slow or failing usage sink never blocks an ingest.
    let (usage_recorder, usage_flush_handle) = BufferedUsageRecorder::start(
        Arc::clone(&pg_store) as Arc<dyn kb_core::usage::UsageRecorder>,
        kb_store::buffered_recorder::DEFAULT_CAPACITY,
    );
    let usage_recorder: Arc<dyn kb_core::usage::UsageRecorder> = Arc::new(usage_recorder);

    let tagger = Arc::new(
        JsonSchemaTagger::new(llm_factory(), DEFAULT_CHAT_MODEL.into())
            .with_usage_recorder(Arc::clone(&usage_recorder)),
    );

    let embed_llm = Arc::new(llm_factory());
    let embedder = Arc::new(
        ChunkEmbedder::new(
            Arc::clone(&embed_llm),
            DEFAULT_EMBED_MODEL.into(),
            EMBED_DIM,
        )
        .with_usage_recorder(Arc::clone(&usage_recorder)),
    );
    let canonicalizer = Arc::new(
        TagCanonicalizer::new(
            Arc::clone(&pg_store) as Arc<dyn kb_pipeline::TagStore>,
            Arc::clone(&embed_llm),
            DEFAULT_EMBED_MODEL.into(),
            TAG_MERGE_THRESHOLD,
        )
        .with_usage_recorder(Arc::clone(&usage_recorder)),
    );
    let reranker = Arc::new(
        LlamaReranker::new(Arc::clone(&embed_llm), DEFAULT_RERANK_MODEL.into())
            .with_usage_recorder(Arc::clone(&usage_recorder)),
    );

    // ── Blob store ([blob], P15-T4) ─────────────────────────────────────────
    let blob = build_blob(&cfg)?;

    // ── Ingest pipeline ─────────────────────────────────────────────────────
    // Separate VLM client with max_retries=0 so VLM-specific failures do not
    // cascade into circuit-breaker trips for the shared `text` role backend.
    let vision_client = LlamaClient::new(
        pool.clone(),
        http.clone(),
        0,
        CIRCUIT_THRESHOLD,
        Duration::from_secs(COOLDOWN_SECS),
    );
    let vision_captioner = Arc::new(
        VisionCaptioner::new(vision_client, DEFAULT_VISION_MODEL.into())
            .with_usage_recorder(Arc::clone(&usage_recorder)),
    );

    let ingest_pipeline = Arc::new(
        IngestPipeline::new(
            Arc::clone(&blob),
            extractors,
            tagger,
            canonicalizer,
            embedder.clone(),
            Arc::clone(&pg_store) as Arc<dyn kb_pipeline::ingest::IngestStore>,
        )
        .with_vision_captioner(vision_captioner),
    );

    // ── Retrieval pipeline ──────────────────────────────────────────────────
    let retrieval_pipeline = Arc::new(RetrievalPipeline::new(
        embedder,
        Arc::clone(&pg_store) as Arc<dyn kb_core::store::Store>,
        reranker,
    ));

    // ── Job queue (lease + retry policy from [worker], P15-T2/T9) ───────────
    let job_queue_pool = pg_store
        .admin_pool()
        .context("failed to get admin pool for job queue")?;
    let job_queue = Arc::new(
        JobQueue::new(
            job_queue_pool,
            cfg.worker.min_backoff_ms as i64,
            cfg.worker.max_retries as i32,
        )
        .with_lease_secs(cfg.worker.lease_secs as i64),
    );

    // ── Backend health loop ─────────────────────────────────────────────────
    let all_backends = pool.all_backends();
    // Seed kb_circuit_breaker_open closed (0) for every configured backend so
    // the family is present — and truthful — from boot (P15-T13, the
    // BUG-OBS-06 family-presence rule). Breakers start closed by definition;
    // the LLM client updates the gauge on every recorded call thereafter.
    for backend in &all_backends {
        kb_metrics::record_circuit_breaker(&backend.id, false);
    }
    let health_interval = Duration::from_secs(cfg.scheduler.health_interval_secs);
    let (health_handle, health_shutdown) =
        HealthLoop::new(all_backends, http.clone(), health_interval).spawn();
    info!(
        interval_secs = cfg.scheduler.health_interval_secs,
        "health loop started"
    );

    Ok(PipelineRuntime {
        pg_store,
        session_store,
        backend_pool: Arc::new(pool),
        blob,
        ingest_pipeline,
        retrieval_pipeline,
        job_queue,
        health_handle,
        health_shutdown,
        usage_flush_handle,
    })
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    /// Unwrap the error of a `build_blob` call (the Ok side is a non-Debug
    /// trait object, so `unwrap_err` cannot be used directly).
    fn blob_err(cfg: &kb_config::Config) -> anyhow::Error {
        match build_blob(cfg) {
            Ok(_) => panic!("expected build_blob to fail"),
            Err(e) => e,
        }
    }

    #[test]
    fn build_blob_default_is_local() {
        let cfg = kb_config::Config::default();
        assert!(
            build_blob(&cfg).is_ok(),
            "default [blob] (local) must build"
        );
    }

    #[test]
    fn build_blob_unknown_backend_errors() {
        let mut cfg = kb_config::Config::default();
        cfg.blob.backend = "ftp".into();
        let err = blob_err(&cfg);
        assert!(err.to_string().contains("unknown [blob].backend"), "{err}");
    }

    #[test]
    fn build_blob_s3_requires_bucket() {
        let mut cfg = kb_config::Config::default();
        cfg.blob.backend = "s3".into();
        let err = blob_err(&cfg);
        assert!(err.to_string().contains("bucket"), "{err}");
    }
}
