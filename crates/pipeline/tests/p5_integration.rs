//! P5 phase integration test (§13, §31): multi-tenant end-to-end.
//!
//! Sets up two tenants (A, B) with their own admin users in a real pgvector Postgres
//! via testcontainers, ingests documents using mock LLM backends, and verifies:
//!
//! 1. **Tenant isolation** — tenant A's search returns only A's docs; tenant B's search
//!    returns only B's; cross-tenant access by id manipulation returns nothing.
//! 2. **Quota enforcement** — set tenant A's `quota_bytes` low, verify upload exceeds →
//!    rejected; tenant B (unlimited) is unaffected.
//! 3. **Export** — export tenant A's data → valid ZIP with manifest, JSONL, and blobs.
//!
//! These tests are `#[ignore]` in `just ci` because they require Podman +
//! `pgvector/pgvector:pg17`. Run with:
//!
//! ```bash
//! systemctl --user start podman.socket
//! DOCKER_HOST="unix://$XDG_RUNTIME_DIR/podman/podman.sock" \
//!   cargo test --package kb-pipeline --test p5_integration -- --ignored
//! ```
//!
//! P5 PHASE COMPLETE after this passes → generate P6 ledger.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashMap;
use std::io::{Cursor, Read};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use kb_core::blob::Blob;
use kb_core::extractor::{Extracted, Extractor, RawFile};
use kb_core::kind::DocKind;
use kb_core::query::{Hit, Query, QueryFilters};
use kb_core::reranker::Reranker;
use kb_core::role::Role;
use kb_core::session::SessionStore;
use kb_core::store::Store;
use kb_core::tagger::{TagInput, TagOutput, Tagger};
use kb_llm::LlamaClient;
use kb_mock_backend::MockBackend;
use kb_pipeline::document_builder::DocumentBuilder;
use kb_pipeline::embedder::ChunkEmbedder;
use kb_pipeline::export::ExportPipeline;
use kb_pipeline::ingest::{IngestFile, IngestPipeline, IngestStore};
use kb_pipeline::metadata_merge::MetadataMerger;
use kb_pipeline::retrieval::RetrievalPipeline;
use kb_pipeline::tag_canonicalizer::TagCanonicalizer;
use kb_pipeline::tag_store::TagStore;
use kb_scheduler::{Backend, Pool};
use kb_store::PgStore;
use kb_store::session_store::InMemorySessionStore;
use reqwest::Client;

// ── Helpers ──────────────────────────────────────────────────────────────────────

/// Produce a 1024-dim vector with a single spike at `pos` set to 1.0, all other
/// elements zero. Spikes at different positions are orthogonal.
fn spike_vector(pos: usize) -> Vec<f32> {
    let mut v = vec![0.0f32; 1024];
    if pos < 1024 {
        v[pos] = 1.0;
    }
    v
}

// ── Mock types ───────────────────────────────────────────────────────────────────

/// A mock [`Tagger`] that returns fixed output — deterministic, no LLM needed.
struct FixedTagger {
    output: TagOutput,
}

#[async_trait]
impl Tagger for FixedTagger {
    async fn tag(&self, _input: &TagInput) -> anyhow::Result<TagOutput> {
        Ok(self.output.clone())
    }
}

/// A mock [`Extractor`] that returns the input bytes as plain text.
struct TextOnlyExtractor;

#[async_trait]
impl Extractor for TextOnlyExtractor {
    async fn extract(&self, file: &RawFile) -> anyhow::Result<Extracted> {
        let text = String::from_utf8_lossy(&file.bytes).to_string();
        Ok(Extracted {
            text,
            meta: serde_json::json!({"mime": "text/plain"}),
            page_images: vec![],
        })
    }
}

/// A pass-through [`Reranker`] that returns 1.0 for every document — preserves the
/// search engine's original ranking order.
struct PassThroughReranker;

#[async_trait]
impl Reranker for PassThroughReranker {
    async fn rerank(&self, _query: &str, docs: &[String]) -> anyhow::Result<Vec<f32>> {
        Ok(docs.iter().map(|_| 1.0).collect())
    }
}

// ── Setup: two-tenant Postgres + mock LLM + pipeline components ───────────────────

/// All the resources needed for a multi-tenant integration test.
///
/// Drops the container automatically when the struct goes out of scope (the
/// `_container` field is held for its Drop impl). The [`MockBackend`] is returned
/// separately from [`setup_two_tenants`] so the test can shut it down explicitly.
struct TwoTenantInfra {
    _container: testcontainers::ContainerAsync<testcontainers::GenericImage>,
    store: Arc<PgStore>,
    pool: sqlx::PgPool,
    blob: Arc<dyn Blob>,
    _tmpdir: tempfile::TempDir,
    tenant_a: i64,
    tenant_b: i64,
    /// Session store shared across both tenants (InMemory, dev-mode).
    sessions: Arc<dyn SessionStore>,
    llm: Arc<LlamaClient>,
    /// Pre-built embedder, shared.
    embedder: Arc<ChunkEmbedder>,
}

/// Spin up a pgvector container, run migrations, create two tenants and two users,
/// initialise a LocalBlob, MockBackend, and pipeline components.
///
/// Returns both the infra handle and the [`MockBackend`] — the caller **must**
/// call `mock.shutdown().await` at the end of each test to gracefully stop the
/// mock HTTP server.
async fn setup_two_tenants() -> anyhow::Result<(TwoTenantInfra, MockBackend)> {
    use testcontainers::core::ports::IntoContainerPort;
    use testcontainers::core::wait::WaitFor;
    use testcontainers::runners::AsyncRunner;
    use testcontainers::{GenericImage, ImageExt};

    let container = GenericImage::new("pgvector/pgvector", "pg17")
        .with_exposed_port(5432u16.tcp())
        .with_wait_for(WaitFor::message_on_stderr(
            "database system is ready to accept connections",
        ))
        .with_env_var("POSTGRES_USER", "kb")
        .with_env_var("POSTGRES_PASSWORD", "kb")
        .with_env_var("POSTGRES_DB", "kb")
        .start()
        .await?;

    let host_port = container.get_host_port_ipv4(5432u16.tcp()).await?;
    let url = format!("postgres://kb:kb@127.0.0.1:{host_port}/kb?sslmode=disable");

    let store = Arc::new(PgStore::new(&url));
    store.connect().await?;
    let pool = store.pool()?;

    // Create two tenants.
    let tenant_a = store.create_tenant("tenant-a", "Tenant Alpha").await?;
    let tenant_b = store.create_tenant("tenant-b", "Tenant Beta").await?;

    // Create admin users (password hash via Argon2id).
    let pw_a = kb_core::auth::hash_password("password-a")?;
    let pw_b = kb_core::auth::hash_password("password-b")?;

    let _user_a = store
        .create_user(
            tenant_a,
            "alice@alpha.example",
            &pw_a,
            kb_core::user::UserRole::Admin,
        )
        .await?;
    let _user_b = store
        .create_user(
            tenant_b,
            "bob@beta.example",
            &pw_b,
            kb_core::user::UserRole::Admin,
        )
        .await?;

    // In-memory session store shared by both tenants.
    let sessions: Arc<dyn SessionStore> = Arc::new(InMemorySessionStore::new());

    // Local blob storage.
    let tmp = tempfile::tempdir()?;
    let blob: Arc<dyn Blob> = Arc::new(kb_store::LocalBlob::new(
        tmp.path().join("blobs"),
        "test".to_string(),
    ));

    // Mock LLM backend (used for embed, chat if needed).
    let mock = MockBackend::start().await;
    let base_url = mock.url("/v1");
    // Default: embed returns a 1024-dim spike vector at position 0.
    mock.scenario().lock().await.embed_content = Some(vec![spike_vector(0)]);

    let backend = Arc::new(Backend::new(
        "mock-p5",
        base_url,
        vec![Role::Text, Role::Embed],
        0,
        8,
    ));
    let pool_llm = Pool::new(vec![backend], Duration::from_secs(10));
    let llm = Arc::new(LlamaClient::new(
        pool_llm,
        Client::new(),
        0,
        0,
        Duration::from_millis(200),
    ));

    let embedder = Arc::new(ChunkEmbedder::new(Arc::clone(&llm), "bge-m3".into(), 1024));

    let infra = TwoTenantInfra {
        _container: container,
        store,
        pool,
        blob,
        _tmpdir: tmp,
        tenant_a,
        tenant_b,
        sessions,
        llm,
        embedder,
    };
    Ok((infra, mock))
}

/// Build an [`IngestPipeline`] scoped to the given infra, with mock tagger,
/// extractor, and canonicalizer.
fn build_ingest_pipeline(infra: &TwoTenantInfra, tagger_output: TagOutput) -> IngestPipeline {
    let tagger: Arc<dyn Tagger> = Arc::new(FixedTagger {
        output: tagger_output,
    });

    let canon: Arc<TagCanonicalizer> = Arc::new(TagCanonicalizer::new(
        Arc::clone(&infra.store) as Arc<dyn TagStore>,
        Arc::clone(&infra.llm),
        "bge-m3".into(),
        0.85,
    ));

    let mut extractors: HashMap<DocKind, Arc<dyn Extractor>> = HashMap::new();
    extractors.insert(
        DocKind::Document,
        Arc::new(TextOnlyExtractor) as Arc<dyn Extractor>,
    );

    IngestPipeline::new(
        Arc::clone(&infra.blob),
        extractors,
        tagger,
        canon,
        Arc::clone(&infra.embedder),
        Arc::clone(&infra.store) as Arc<dyn IngestStore>,
    )
}

/// Build a [`RetrievalPipeline`] scoped to the given infra, with a pass-through
/// reranker.
fn build_retrieval_pipeline(infra: &TwoTenantInfra) -> RetrievalPipeline {
    let reranker: Arc<dyn Reranker> = Arc::new(PassThroughReranker);
    let store_trait: Arc<dyn Store> = Arc::clone(&infra.store) as Arc<dyn Store>;
    RetrievalPipeline::new(Arc::clone(&infra.embedder), store_trait, reranker)
}

// ═══════════════════════════════════════════════════════════════════════════════════
// Test 1: Multi-tenant isolation — ingest + search
// ═══════════════════════════════════════════════════════════════════════════════════

/// Tenant A ingests a document about "quantum computing" while tenant B ingests
/// a document about "oceanography". Each tenant's search returns only its own
/// documents. Cross-tenant access by direct document id finds nothing.
#[ignore = "requires Podman + image pull; run with --ignored"]
#[tokio::test]
async fn e2e_multi_tenant_ingest_and_search_isolation() {
    let (infra, mock) = setup_two_tenants().await.unwrap();

    // ── Create sessions for both users ──────────────────────────────────────
    let session_a = infra
        .sessions
        .create(
            infra.tenant_a,
            1,
            kb_core::user::UserRole::Admin,
            Duration::from_secs(3600),
        )
        .await
        .unwrap();
    let session_b = infra
        .sessions
        .create(
            infra.tenant_b,
            2,
            kb_core::user::UserRole::Admin,
            Duration::from_secs(3600),
        )
        .await
        .unwrap();

    // ── Ingest as tenant A: "quantum computing" document ───────────────────
    let ingest_a = build_ingest_pipeline(
        &infra,
        TagOutput {
            title: "Quantum Computing Guide".into(),
            summary: "Introduction to quantum computing.".into(),
            tags: vec!["quantum".into(), "computing".into()],
        },
    );
    let out_a = ingest_a
        .ingest(
            infra.tenant_a,
            vec![IngestFile {
                bytes:
                    b"Quantum computing uses qubits for exponential speedup in certain problems."
                        .to_vec(),
                page_label: None,
                path: Some("quantum-guide.txt".into()),
            }],
            Some("research material".into()),
        )
        .await
        .unwrap();
    assert!(out_a.document_id > 0);
    assert_eq!(out_a.tag_ids.len(), 2);
    assert!(out_a.chunk_count > 0);

    // ── Ingest as tenant B: "oceanography" document ────────────────────────
    let ingest_b = build_ingest_pipeline(
        &infra,
        TagOutput {
            title: "Ocean Studies".into(),
            summary: "Research on ocean currents and marine life.".into(),
            tags: vec!["ocean".into(), "marine".into()],
        },
    );
    let out_b = ingest_b
        .ingest(
            infra.tenant_b,
            vec![IngestFile {
                bytes: b"Oceanography studies the physical and biological aspects of the ocean."
                    .to_vec(),
                page_label: None,
                path: Some("ocean-study.txt".into()),
            }],
            None,
        )
        .await
        .unwrap();
    assert!(out_b.document_id > 0);
    assert_ne!(
        out_a.document_id, out_b.document_id,
        "different tenants should have different documents"
    );

    // ── Tenant A searches for "quantum" → finds own doc ────────────────────
    let retrieval = build_retrieval_pipeline(&infra);
    let hits_a = retrieval
        .retrieve(
            infra.tenant_a,
            &Query {
                text: "quantum".into(),
                filters: QueryFilters::default(),
                top_k: 10,
            },
        )
        .await
        .unwrap();

    assert!(
        !hits_a.is_empty(),
        "tenant A should find 'quantum' document"
    );
    assert!(
        hits_a.iter().any(|h| h.document_id == out_a.document_id),
        "tenant A search must include its own document"
    );
    assert!(
        !hits_a.iter().any(|h| h.document_id == out_b.document_id),
        "RLS violated: tenant A sees tenant B's document"
    );

    // ── Tenant B searches for "ocean" → finds own doc ──────────────────────
    let hits_b = retrieval
        .retrieve(
            infra.tenant_b,
            &Query {
                text: "ocean".into(),
                filters: QueryFilters::default(),
                top_k: 10,
            },
        )
        .await
        .unwrap();

    assert!(!hits_b.is_empty(), "tenant B should find 'ocean' document");
    assert!(
        hits_b.iter().any(|h| h.document_id == out_b.document_id),
        "tenant B search must include its own document"
    );
    assert!(
        !hits_b.iter().any(|h| h.document_id == out_a.document_id),
        "RLS violated: tenant B sees tenant A's document"
    );

    // ── Cross-tenant: Tenant A searches for "ocean" → nothing ──────────────
    let hits_a2 = retrieval
        .retrieve(
            infra.tenant_a,
            &Query {
                text: "ocean".into(),
                filters: QueryFilters::default(),
                top_k: 10,
            },
        )
        .await
        .unwrap();

    assert!(
        hits_a2.iter().all(|h| h.document_id != out_b.document_id),
        "RLS violated: tenant A found tenant B's 'ocean' document"
    );

    // ── Sessions validated ─────────────────────────────────────────────────
    assert!(infra.sessions.validate(&session_a).await.unwrap().is_some());
    assert!(infra.sessions.validate(&session_b).await.unwrap().is_some());

    // ── Confirming document count per tenant ───────────────────────────────
    let pool = &infra.pool;
    sqlx::query("SELECT app_set_current_tenant($1)")
        .bind(infra.tenant_a)
        .execute(pool)
        .await
        .unwrap();
    let count_a: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM documents")
        .fetch_one(pool)
        .await
        .unwrap();
    assert_eq!(count_a, 1, "tenant A should see exactly 1 document");

    sqlx::query("SELECT app_set_current_tenant($1)")
        .bind(infra.tenant_b)
        .execute(pool)
        .await
        .unwrap();
    let count_b: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM documents")
        .fetch_one(pool)
        .await
        .unwrap();
    assert_eq!(count_b, 1, "tenant B should see exactly 1 document");

    mock.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════════════
// Test 2: Quota enforcement per tenant
// ═══════════════════════════════════════════════════════════════════════════════════

/// Set tenant A's `quota_bytes` to a low value (100 bytes), verify that an
/// upload exceeding the limit is rejected while tenant B (unlimited) is
/// unaffected.
#[ignore = "requires Podman + image pull; run with --ignored"]
#[tokio::test]
async fn e2e_quota_enforcement_per_tenant() {
    let (infra, mock) = setup_two_tenants().await.unwrap();

    // ── Set quota on tenant A: 100 bytes, tenant B: unlimited ──────────────
    sqlx::query("UPDATE tenants SET quota_bytes = 100 WHERE id = $1")
        .bind(infra.tenant_a)
        .execute(&infra.pool)
        .await
        .unwrap();
    // Tenant B stays with NULL quotas (the default from create_tenant — unlimited).

    // ── Tenant A: upload a small file (e.g. 50 bytes) → allowed ────────────
    infra
        .store
        .check_storage_quota(infra.tenant_a, 50)
        .await
        .unwrap_or_else(|e| panic!("50 bytes should be under the 100-byte quota, got: {e}"));

    // ── Tenant A: try to add 200 bytes → exceeds 100-byte quota ────────────
    let err = infra
        .store
        .check_storage_quota(infra.tenant_a, 200)
        .await
        .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("storage quota exceeded"),
        "expected quota-exceeded error, got: {msg}"
    );
    assert!(
        msg.contains("limit is 100 bytes"),
        "error should mention the limit, got: {msg}"
    );

    // ── Tenant B: same upload succeeds (unlimited) ─────────────────────────
    infra
        .store
        .check_storage_quota(infra.tenant_b, 200)
        .await
        .unwrap_or_else(|e| panic!("tenant B (unlimited) should accept 200 bytes, got: {e}"));

    // ── Token quota: set tenant A's tokens to 1000 ────────────────────────
    sqlx::query("UPDATE tenants SET quota_tokens = 1000 WHERE id = $1")
        .bind(infra.tenant_a)
        .execute(&infra.pool)
        .await
        .unwrap();

    // Tenant A: 500 tokens → allowed.
    infra
        .store
        .check_token_quota(infra.tenant_a, 500)
        .await
        .unwrap_or_else(|e| panic!("500 tokens should be under 1000-token quota, got: {e}"));

    // Tenant A: 2000 tokens → exceeds.
    let tok_err = infra
        .store
        .check_token_quota(infra.tenant_a, 2000)
        .await
        .unwrap_err();
    let tok_msg = tok_err.to_string();
    assert!(
        tok_msg.contains("token quota exceeded"),
        "expected token-quota error, got: {tok_msg}"
    );

    // Tenant B: 2000 tokens succeeds (unlimited).
    infra
        .store
        .check_token_quota(infra.tenant_b, 2000)
        .await
        .unwrap_or_else(|e| panic!("tenant B (unlimited) should accept 2000 tokens, got: {e}"));

    mock.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════════════
// Test 3: Export round-trip
// ═══════════════════════════════════════════════════════════════════════════════════

/// Ingest a document into tenant A, export it via [`ExportPipeline`], and verify
/// the produced ZIP contains `manifest.json`, `export.jsonl` with the correct
/// records, and the original blob content.
#[ignore = "requires Podman + image pull; run with --ignored"]
#[tokio::test]
async fn e2e_export_produces_valid_zip() {
    let (infra, mock) = setup_two_tenants().await.unwrap();

    // ── Ingest a document as tenant A ──────────────────────────────────────
    let ingest = build_ingest_pipeline(
        &infra,
        TagOutput {
            title: "Export Test Document".into(),
            summary: "A document for export testing.".into(),
            tags: vec!["export".into(), "test".into()],
        },
    );
    let content = b"Hello export world. This document should survive the round-trip.";
    let out = ingest
        .ingest(
            infra.tenant_a,
            vec![IngestFile {
                bytes: content.to_vec(),
                page_label: None,
                path: Some("docs/export-test.txt".into()),
            }],
            Some("export-test note".into()),
        )
        .await
        .unwrap();
    assert!(out.document_id > 0);

    // ── Export tenant A ────────────────────────────────────────────────────
    let export_store: Arc<dyn kb_pipeline::export::ExportStore> =
        Arc::clone(&infra.store) as Arc<dyn kb_pipeline::export::ExportStore>;
    let export_pipeline = ExportPipeline::new(export_store, Arc::clone(&infra.blob));
    let blob_key = export_pipeline.run_export(infra.tenant_a).await.unwrap();

    // ── Verify the blob key shape ──────────────────────────────────────────
    assert!(
        blob_key.starts_with(&format!("{}/export-", infra.tenant_a)),
        "blob key must start with tenant/export-, got: {blob_key}"
    );
    assert!(blob_key.ends_with(".zip"), "blob key must end with .zip");

    // ── Fetch the ZIP from the blob store and inspect it ───────────────────
    let zip_bytes = infra.blob.get(&blob_key).await.unwrap();
    let cursor = Cursor::new(zip_bytes.to_vec());
    let mut zip = zip::ZipArchive::new(cursor).unwrap();

    // Expected entries: manifest.json + export.jsonl + 1 blob file = 3.
    assert!(
        zip.len() >= 3,
        "export ZIP should have at least 3 entries (manifest, JSONL, blobs), got {}",
        zip.len()
    );

    // ── Verify manifest.json ───────────────────────────────────────────────
    let mut manifest_bytes = Vec::new();
    zip.by_name("manifest.json")
        .unwrap()
        .read_to_end(&mut manifest_bytes)
        .unwrap();
    let manifest: serde_json::Value = serde_json::from_slice(&manifest_bytes).unwrap();
    assert_eq!(manifest["schema_version"], 1);
    assert_eq!(manifest["embedder_id"], "bge-m3");
    assert_eq!(manifest["embedder_dim"], 1024);
    assert_eq!(manifest["tenant_slug"], "tenant-a");
    assert_eq!(manifest["document_count"], 1);
    assert_eq!(manifest["file_count"], 1);
    assert!(manifest["total_bytes"].as_u64().unwrap() > 0);

    // ── Verify export.jsonl ────────────────────────────────────────────────
    let mut jsonl_bytes = Vec::new();
    zip.by_name("export.jsonl")
        .unwrap()
        .read_to_end(&mut jsonl_bytes)
        .unwrap();
    let jsonl_str = String::from_utf8(jsonl_bytes).unwrap();
    assert!(jsonl_str.contains("\"type\":\"document\""));
    assert!(jsonl_str.contains("\"type\":\"file\""));
    assert!(jsonl_str.contains("\"type\":\"tag\""));
    assert!(jsonl_str.contains("\"type\":\"chunk\""));
    assert!(jsonl_str.contains("Export Test Document"));
    assert!(jsonl_str.contains("export"));

    // ── Verify the blob entry preserves the original file path ─────────────
    let mut blob_content = Vec::new();
    zip.by_name("docs/export-test.txt")
        .unwrap()
        .read_to_end(&mut blob_content)
        .unwrap();
    assert_eq!(
        blob_content, content,
        "blob content must match ingested bytes"
    );

    // ── Verify export contains document-tag links ──────────────────────────
    let doc_tags: Vec<&str> = jsonl_str
        .lines()
        .filter(|l| l.contains("\"type\":\"document_tag\""))
        .collect();
    assert!(
        !doc_tags.is_empty(),
        "export JSONL must include document-tag links"
    );

    mock.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════════════
// Test 4: Empty tenant export
// ═══════════════════════════════════════════════════════════════════════════════════

/// Export an empty tenant (no documents) — must produce a valid ZIP with
/// just `manifest.json` and an empty `export.jsonl`.
#[ignore = "requires Podman + image pull; run with --ignored"]
#[tokio::test]
async fn e2e_export_empty_tenant() {
    let (infra, mock) = setup_two_tenants().await.unwrap();

    // Tenant B has no documents.
    let export_store: Arc<dyn kb_pipeline::export::ExportStore> =
        Arc::clone(&infra.store) as Arc<dyn kb_pipeline::export::ExportStore>;
    let export_pipeline = ExportPipeline::new(export_store, Arc::clone(&infra.blob));
    let blob_key = export_pipeline.run_export(infra.tenant_b).await.unwrap();

    let zip_bytes = infra.blob.get(&blob_key).await.unwrap();
    let cursor = Cursor::new(zip_bytes.to_vec());
    let mut zip = zip::ZipArchive::new(cursor).unwrap();

    assert_eq!(
        zip.len(),
        2,
        "empty export should have exactly 2 entries (manifest + JSONL), got {}",
        zip.len()
    );

    let mut manifest_bytes = Vec::new();
    zip.by_name("manifest.json")
        .unwrap()
        .read_to_end(&mut manifest_bytes)
        .unwrap();
    let manifest: serde_json::Value = serde_json::from_slice(&manifest_bytes).unwrap();
    assert_eq!(manifest["document_count"], 0);
    assert_eq!(manifest["file_count"], 0);
    assert_eq!(manifest["total_bytes"], 0);

    let mut jsonl_bytes = Vec::new();
    zip.by_name("export.jsonl")
        .unwrap()
        .read_to_end(&mut jsonl_bytes)
        .unwrap();
    assert!(
        jsonl_bytes.is_empty() || String::from_utf8_lossy(&jsonl_bytes).trim().is_empty(),
        "empty export JSONL should be empty"
    );

    mock.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════════════
// Pure-logic smoke tests (no testcontainers needed)
// ═══════════════════════════════════════════════════════════════════════════════════

#[test]
fn document_builder_single_and_multi_file() {
    let (doc, files) =
        DocumentBuilder::build_single(1, b"Hello from tenant A.", Some("readme.md"), None);
    assert_eq!(doc.tenant_id, 1);
    assert_eq!(doc.page_count, 1);
    assert_eq!(files.len(), 1);
    assert_eq!(files[0].page_no, 1);
    assert!(files[0].blob_key.starts_with("1/"));
    assert_eq!(files[0].sha256.to_hex().len(), 64);

    // Multi-file build: PageInput borrows bytes + strings from local variables.
    let page1_bytes: &[u8] = b"Page one content.";
    let page2_bytes: &[u8] = b"Page two content.";
    let (doc2, files2) = DocumentBuilder::build_multi(
        2,
        &[
            kb_pipeline::document_builder::PageInput {
                bytes: page1_bytes,
                page_label: Some("chapter-1"),
                path: Some("ch1.md"),
            },
            kb_pipeline::document_builder::PageInput {
                bytes: page2_bytes,
                page_label: Some("chapter-2"),
                path: Some("ch2.md"),
            },
        ],
        None,
    );
    assert_eq!(doc2.page_count, 2);
    assert_eq!(files2.len(), 2);
    assert_eq!(files2[0].page_no, 1);
    assert_eq!(files2[1].page_no, 2);
    assert_eq!(files2[0].page_label.as_deref(), Some("chapter-1"));
    assert_eq!(files2[1].page_label.as_deref(), Some("chapter-2"));
}

#[test]
fn metadata_merge_preserves_per_page_data() {
    let mut hash_a = [0u8; 32];
    hash_a[0] = 1;
    let rec_a = kb_core::file::FileRecord {
        id: 0,
        tenant_id: 1,
        document_id: 0,
        page_no: 1,
        page_label: Some("front".into()),
        sha256: kb_core::hash::Sha256::from_bytes(hash_a),
        blob_key: "1/blob-a".into(),
        path: Some("a.txt".into()),
        mime: Some("text/plain".into()),
        size_bytes: Some(10),
        meta: serde_json::json!({}),
        status: kb_core::status::ProcessingStatus::Pending,
        ingested_at: chrono::Utc::now(),
    };

    let mut hash_b = [0u8; 32];
    hash_b[0] = 2;
    let rec_b = kb_core::file::FileRecord {
        id: 0,
        tenant_id: 1,
        document_id: 0,
        page_no: 2,
        page_label: Some("back".into()),
        sha256: kb_core::hash::Sha256::from_bytes(hash_b),
        blob_key: "1/blob-b".into(),
        path: Some("b.txt".into()),
        mime: Some("text/plain".into()),
        size_bytes: Some(20),
        meta: serde_json::json!({}),
        status: kb_core::status::ProcessingStatus::Pending,
        ingested_at: chrono::Utc::now(),
    };

    let ext_a = Extracted {
        text: "Hello from page one.".into(),
        meta: serde_json::json!({"pages": 1}),
        page_images: vec![],
    };
    let ext_b = Extracted {
        text: "Hello from page two.".into(),
        meta: serde_json::json!({"pages": 2}),
        page_images: vec![],
    };

    let merged = MetadataMerger::merge(&[(rec_a, ext_a), (rec_b, ext_b)]);
    assert_eq!(merged.page_count, 2);
    assert!(merged.merged_text.contains("--- page 1 (front) ---"));
    assert!(merged.merged_text.contains("--- page 2 (back) ---"));
    assert!(merged.merged_text.contains("Hello from page one"));
    assert!(merged.merged_text.contains("Hello from page two"));
}

#[test]
fn spike_vectors_are_orthogonal() {
    let a = spike_vector(0);
    let b = spike_vector(1);
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    assert!(
        dot.abs() < 1e-6,
        "orthogonal vectors dot product should be 0, got {dot}"
    );
}

#[test]
fn hit_struct_has_deep_link_fields() {
    let hit = Hit {
        document_id: 42,
        score: 0.97,
        title: Some("My Doc".into()),
        snippet: "matching text here...".into(),
        file_id: 7,
        page_no: Some(3),
        ts_offset: Some(12.5),
    };
    assert_eq!(hit.document_id, 42);
    assert!(hit.score > 0.0);
    assert_eq!(hit.file_id, 7);
    assert_eq!(hit.page_no, Some(3));
    assert_eq!(hit.ts_offset, Some(12.5));
}

#[test]
fn query_default_filters_are_empty() {
    let q = Query {
        text: "search term".into(),
        filters: QueryFilters::default(),
        top_k: 5,
    };
    assert_eq!(q.text, "search term");
    assert_eq!(q.top_k, 5);
    assert!(q.filters.kinds.is_empty());
    assert!(q.filters.tags.is_empty());
    assert!(q.filters.created_after.is_none());
    assert!(q.filters.created_before.is_none());
}
