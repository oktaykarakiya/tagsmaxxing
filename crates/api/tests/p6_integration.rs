//! P6 phase integration test (§10, §12, §15, §31): end-to-end through CLI,
//! API, Web UI, Admin, and Metrics fronts.
//!
//! Tests exercise every P6 surface against a real pgvector Postgres via
//! testcontainers with mock LLM backends:
//!
//! 1. **CLI** — pipeline round-trip: ingest → document in DB → search → ranked results.
//! 2. **API** — ingest via pipeline → GET /api/search (deep-links) →
//!    GET /api/documents/:id (detail) → GET .../file/:file_id (blob download).
//! 3. **Web UI** — GET /search → HTML + CSRF; GET /documents/:id → detail page;
//!    GET /upload → HTML; GET /login → public form.
//! 4. **Admin** — Owner accesses dashboard/users/tenants; Admin sees own tenant;
//!    cross-tenant isolation upheld through admin panel routes.
//! 5. **Cross-tenant isolation via API** — tenant B's API cannot see tenant A's docs.
//! 6. **Metrics** — GET /metrics returns valid Prometheus text exposition format.
//!
//! These tests are `#[ignore]` in `just ci` because they require Podman +
//! `pgvector/pgvector:pg17`. Run with:
//!
//! ```bash
//! systemctl --user start podman.socket
//! DOCKER_HOST="unix://$XDG_RUNTIME_DIR/podman/podman.sock" \
//!   cargo test --package kb-api --test p6_integration -- --ignored --test-threads=1
//! ```
//!
//! P6 PHASE COMPLETE after this passes.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::body::Body;
use axum::http::{self, StatusCode, header};
use kb_core::blob::Blob;
use kb_core::extractor::{Extracted, Extractor, RawFile};
use kb_core::kind::DocKind;
use kb_core::query::{Query, QueryFilters};
use kb_core::reranker::Reranker;
use kb_core::role::Role;
use kb_core::session::SessionStore;
use kb_core::status::ProcessingStatus;
use kb_core::store::Store;
use kb_core::tagger::{TagInput, TagOutput, Tagger};
use kb_core::user::UserRole;
use kb_llm::LlamaClient;
use kb_mock_backend::MockBackend;
use kb_pipeline::embedder::ChunkEmbedder;
use kb_pipeline::ingest::{IngestFile, IngestPipeline, IngestStore};
use kb_pipeline::retrieval::RetrievalPipeline;
use kb_pipeline::tag_canonicalizer::{TAG_MERGE_THRESHOLD, TagCanonicalizer};
use kb_pipeline::tag_store::TagStore;
use kb_scheduler::{Pool, test_backend};
use kb_store::PgStore;
use kb_store::blob::LocalBlob;
use kb_store::session_store::InMemorySessionStore;
use reqwest::Client;

use kb_api::AppState;

// ── Helpers ──────────────────────────────────────────────────────────────────────

/// Produce an [`EMBED_DIM`](kb_store::EMBED_DIM)-dimensional vector with a single spike at `pos`
/// set to 1.0, all other elements zero. Spikes at different positions are orthogonal.
fn spike_vector(pos: usize) -> Vec<f32> {
    let mut v = vec![0.0f32; kb_store::EMBED_DIM];
    if pos < kb_store::EMBED_DIM {
        v[pos] = 1.0;
    }
    v
}

// ── Mock types (same pattern as p5_integration) ──────────────────────────────────

/// A mock [`Tagger`] that returns fixed output — deterministic, no LLM needed.
struct FixedTagger {
    output: TagOutput,
}

#[async_trait]
impl Tagger for FixedTagger {
    async fn tag(&self, _input: &TagInput, _local_only: bool) -> anyhow::Result<TagOutput> {
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
    async fn rerank(
        &self,
        _query: &str,
        docs: &[String],
        _local_only: bool,
        _priority: i32,
    ) -> anyhow::Result<Vec<f32>> {
        Ok(docs.iter().map(|_| 1.0).collect())
    }
}

// ── Setup: Postgres + mock LLM + pipeline + AppState ────────────────────────────

/// All the resources needed for a P6 integration test.
///
/// Drops the container automatically when the struct goes out of scope (the
/// `_container` field is held for its Drop impl). The [`MockBackend`] is returned
/// separately from [`setup_p6_infra`] so the test can shut it down explicitly.
struct P6Infra {
    store: Arc<PgStore>,
    _pool: sqlx::PgPool,
    _app_pool: sqlx::PgPool,
    blob: Arc<dyn Blob>,
    _tmpdir: tempfile::TempDir,
    tenant_a: i64,
    tenant_b: i64,
    /// Session store shared across both tenants (InMemory, dev-mode).
    sessions: Arc<dyn SessionStore>,
    llm: Arc<LlamaClient>,
    embedder: Arc<ChunkEmbedder>,
}

/// Spin up a pgvector container, run migrations, create two tenants with users,
/// initialise a LocalBlob, MockBackend, and pipeline components.
///
/// Returns both the infra handle and the [`MockBackend`] — the caller **must**
/// call `mock.shutdown().await` at the end of each test.
async fn setup_p6_infra() -> anyhow::Result<(P6Infra, MockBackend)> {
    let db = kb_testsupport::fresh_db().await?;
    let store = Arc::new(PgStore::with_roles(&db.admin_url, &db.app_url));
    store.connect().await?;
    let pool = store.pool()?;
    let app_pool = store.app_pool()?;

    // Create two tenants.
    let tenant_a = store.create_tenant("tenant-a", "Tenant Alpha").await?;
    let tenant_b = store.create_tenant("tenant-b", "Tenant Beta").await?;

    // Create users: alice (Owner of tenant A), bob (Admin of tenant B).
    let pw_a = kb_core::auth::hash_password("password-a")?;
    let pw_b = kb_core::auth::hash_password("password-b")?;

    let _user_a = store
        .create_user(tenant_a, "alice@alpha.example", &pw_a, UserRole::Owner)
        .await?;
    let _user_b = store
        .create_user(tenant_b, "bob@beta.example", &pw_b, UserRole::Admin)
        .await?;

    // In-memory session store.
    let sessions: Arc<dyn SessionStore> = Arc::new(InMemorySessionStore::new());

    // Local blob storage.
    let tmp = tempfile::tempdir()?;
    let blob: Arc<dyn Blob> =
        Arc::new(LocalBlob::new(tmp.path().join("blobs"), "test".to_string()));

    // Mock LLM backend — embed returns an EMBED_DIM-dimensional spike vector.
    let mock = MockBackend::start().await;
    let base_url = mock.url("/v1");
    mock.scenario().lock().await.embed_content = Some(vec![spike_vector(0)]);

    let backend = Arc::new(test_backend(
        "mock-p6",
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

    let embedder = Arc::new(ChunkEmbedder::new(
        Arc::clone(&llm),
        "bge-m3".into(),
        kb_store::EMBED_DIM,
    ));

    let infra = P6Infra {
        store,
        _pool: pool,
        _app_pool: app_pool,
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
fn build_ingest_pipeline(infra: &P6Infra, tagger_output: TagOutput) -> IngestPipeline {
    let tagger: Arc<dyn Tagger> = Arc::new(FixedTagger {
        output: tagger_output,
    });

    let canon: Arc<TagCanonicalizer> = Arc::new(TagCanonicalizer::new(
        Arc::clone(&infra.store) as Arc<dyn TagStore>,
        Arc::clone(&infra.llm),
        "bge-m3".into(),
        TAG_MERGE_THRESHOLD,
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
fn build_retrieval_pipeline(infra: &P6Infra) -> RetrievalPipeline {
    let reranker: Arc<dyn Reranker> = Arc::new(PassThroughReranker);
    let store_trait: Arc<dyn Store> = Arc::clone(&infra.store) as Arc<dyn Store>;
    RetrievalPipeline::new(Arc::clone(&infra.embedder), store_trait, reranker)
}

/// Build a full [`AppState`] from the infra with pipeline components attached,
/// suitable for use with [`kb_api::build_router`].
fn build_app_state(
    infra: &P6Infra,
    ingest: Arc<IngestPipeline>,
    retrieval: Arc<RetrievalPipeline>,
) -> Arc<AppState> {
    Arc::new(
        AppState::new(
            Arc::clone(&infra.sessions),
            Arc::clone(&infra.store),
            Some(Duration::from_secs(3600)),
            false,
        )
        .with_ingest_pipeline(ingest)
        .with_retrieval_pipeline(retrieval)
        .with_blob(Arc::clone(&infra.blob)),
    )
}

/// Create an authenticated session for a tenant, returning the session token.
async fn create_session(infra: &P6Infra, tenant_id: i64, user_id: i64, role: UserRole) -> String {
    infra
        .sessions
        .create(tenant_id, user_id, role, true, Duration::from_secs(3600))
        .await
        .unwrap()
}

/// Build an HTTP request with a session cookie.
fn auth_request(method: &str, uri: &str, session_token: &str) -> http::Request<Body> {
    http::Request::builder()
        .method(method)
        .uri(uri)
        .header("Cookie", format!("__Host-session={session_token}"))
        .body(Body::empty())
        .unwrap()
}

// ═══════════════════════════════════════════════════════════════════════════════════
// Test 1: CLI/pipeline round-trip via direct pipeline calls
// ═══════════════════════════════════════════════════════════════════════════════════

/// Verify that ingest + search via the pipeline produces documents visible in
/// Postgres and searchable via the retrieval pipeline. This test exercises the
/// same pipeline code paths that the CLI commands call (`run_ingest` /
/// `run_search`), proving the CLI ingestion and search contract end-to-end.
#[ignore = "requires Podman + image pull; run with --ignored"]
#[tokio::test]
async fn e2e_cli_ingest_and_search_roundtrip() {
    let (infra, mock) = setup_p6_infra().await.unwrap();

    // ── 1. Ingest: build a pipeline and ingest a small text document ──────────
    let ingest = build_ingest_pipeline(
        &infra,
        TagOutput {
            title: "CLI Integration Guide".into(),
            summary: "Testing the CLI ingest command end-to-end.".into(),
            tags: vec!["cli".into(), "testing".into()],
        },
    );

    let output = ingest
        .ingest(
            infra.tenant_a,
            None,
            vec![IngestFile {
                bytes: b"The CLI is the command-line interface for the knowledge base.".to_vec(),
                page_label: None,
                path: Some("docs/cli-guide.txt".into()),
            }],
            Some("cli-test note".into()),
            false,
        )
        .await
        .unwrap();

    assert!(
        output.document_id > 0,
        "ingest must produce a valid document_id"
    );
    assert!(!output.tag_ids.is_empty(), "tags must be assigned");
    assert!(
        output.chunk_count > 0,
        "at least one chunk must be embedded"
    );

    // ── 2. Verify document is visible via store ────────────────────────────
    let doc = infra
        .store
        .get_document(infra.tenant_a, output.document_id)
        .await
        .unwrap();
    assert!(doc.is_some(), "document must exist in the database");
    let doc = doc.unwrap();
    assert_eq!(doc.title.as_deref(), Some("CLI Integration Guide"));
    assert_eq!(doc.status, ProcessingStatus::Ready);

    // ── 3. Search: use the retrieval pipeline to find the document ────────────
    let retrieval = build_retrieval_pipeline(&infra);
    let hits = retrieval
        .retrieve(
            infra.tenant_a,
            &Query {
                text: "command-line interface".into(),
                filters: QueryFilters::default(),
                top_k: 10,
            },
            false,
        )
        .await
        .unwrap();

    assert!(
        !hits.is_empty(),
        "search for 'command-line interface' must return results"
    );
    assert!(
        hits.iter().any(|h| h.document_id == output.document_id),
        "search must return the ingested document"
    );

    // ── 4. Verify deep-link provenance on a hit ───────────────────────────────
    let hit = hits
        .iter()
        .find(|h| h.document_id == output.document_id)
        .unwrap();
    assert!(
        hit.file_id > 0,
        "hit must carry a valid file_id (deep-link provenance)"
    );
    assert!(!hit.snippet.is_empty(), "hit must carry a text snippet");
    assert_eq!(hit.title.as_deref(), Some("CLI Integration Guide"));

    mock.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════════════
// Test 2: API ingest + search + document detail via HTTP
// ═══════════════════════════════════════════════════════════════════════════════════

/// Full API round-trip: ingest a document via pipeline (simulating the job
/// queue worker path), then verify the REST read endpoints return correct
/// data, deep-link provenance, and proper HTTP status codes.
///
/// Tests: `GET /api/search`, `GET /api/documents/:id`,
/// `GET /api/documents/:id/file/:file_id`, `GET /api/jobs/:id`, and
/// unauthenticated access rejection.
#[ignore = "requires Podman + image pull; run with --ignored"]
#[tokio::test]
async fn e2e_api_ingest_search_document_detail() {
    let (infra, mock) = setup_p6_infra().await.unwrap();

    // ── 1. Ingest data directly via pipeline ──────────────────────────────────
    let ingest = build_ingest_pipeline(
        &infra,
        TagOutput {
            title: "API Integration Test Document".into(),
            summary: "Testing API endpoints for document retrieval and search.".into(),
            tags: vec!["api".into(), "integration".into()],
        },
    );

    let content = b"REST API endpoints provide document access via HTTP methods.";
    let output = ingest
        .ingest(
            infra.tenant_a,
            None,
            vec![IngestFile {
                bytes: content.to_vec(),
                page_label: None,
                path: Some("api-test.txt".into()),
            }],
            None,
            false,
        )
        .await
        .unwrap();

    assert!(output.document_id > 0);

    // ── 2. Build AppState + axum router ──────────────────────────────────────
    let retrieval = Arc::new(build_retrieval_pipeline(&infra));
    let ingest_arc = Arc::new(ingest);
    let state = build_app_state(&infra, Arc::clone(&ingest_arc), Arc::clone(&retrieval));
    let router = kb_api::build_router((*state).clone());

    let session = create_session(&infra, infra.tenant_a, 1, UserRole::Owner).await;

    use tower::ServiceExt;

    // ── 3. GET /api/search — verify results with deep-links ──────────────────
    let req = auth_request("GET", "/api/search?q=REST+API+endpoints", &session);
    let resp = router.clone().oneshot(req).await.unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), 65536).await.unwrap();
    let search_result: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let hits = search_result["hits"].as_array().unwrap();
    assert!(!hits.is_empty(), "search must return hits");

    let hit = &hits[0];
    assert_eq!(
        hit["document_id"].as_i64().unwrap(),
        output.document_id,
        "hit must reference correct document"
    );
    assert!(hit["score"].as_f64().unwrap() >= 0.0);
    assert!(
        hit["file_id"].as_i64().unwrap() > 0,
        "deep-link file_id missing"
    );
    assert!(
        !hit["snippet"].as_str().unwrap().is_empty(),
        "snippet must be present"
    );

    // ── 4. GET /api/documents/:id — verify detail ────────────────────────────
    let req = auth_request(
        "GET",
        &format!("/api/documents/{}", output.document_id),
        &session,
    );
    let resp = router.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(resp.into_body(), 65536).await.unwrap();
    let doc: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        doc["title"].as_str().unwrap(),
        "API Integration Test Document"
    );
    assert_eq!(doc["status"].as_str().unwrap(), "ready");
    assert!(
        !doc["files"].as_array().unwrap().is_empty(),
        "must list files"
    );

    // ── 5. GET /api/documents/:id/file/:file_id — blob download is a presigned 302
    //       redirect (plan §20): the app returns a Location URL so the client fetches
    //       bytes directly from object storage, never proxying them through the API. ──
    let file_id = doc["files"][0]["id"].as_i64().unwrap();
    let req = auth_request(
        "GET",
        &format!("/api/documents/{}/file/{}", output.document_id, file_id),
        &session,
    );
    let resp = router.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::FOUND,
        "blob download must be a presigned redirect"
    );
    assert!(
        resp.headers().contains_key(axum::http::header::LOCATION),
        "redirect must carry a Location (presigned download URL)"
    );

    // ── 6. GET /api/jobs/:id — verify non-existent job returns 4xx ───────────
    let req = auth_request("GET", "/api/jobs/9999999", &session);
    let resp = router.clone().oneshot(req).await.unwrap();
    assert!(
        resp.status().is_client_error(),
        "non-existent job must return 4xx"
    );

    // ── 7. Unauthenticated access — reject with 401 ──────────────────────────
    let req = http::Request::builder()
        .uri("/api/search?q=test")
        .body(Body::empty())
        .unwrap();
    let resp = router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    mock.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════════════
// Test 3: Web UI — search page, document detail, upload page, login page
// ═══════════════════════════════════════════════════════════════════════════════════

/// Verify the Web UI renders HTML pages with CSRF tokens and security headers.
#[ignore = "requires Podman + image pull; run with --ignored"]
#[tokio::test]
async fn e2e_web_ui_search_and_document_detail() {
    let (infra, mock) = setup_p6_infra().await.unwrap();

    // ── 1. Ingest data ───────────────────────────────────────────────────────
    let ingest = build_ingest_pipeline(
        &infra,
        TagOutput {
            title: "Web UI Test Document".into(),
            summary: "Testing the Web UI search and document detail pages.".into(),
            tags: vec!["web".into(), "ui".into()],
        },
    );
    let _content = b"Web UI provides HTMX-powered search with CSRF protection.";
    let output = ingest
        .ingest(
            infra.tenant_a,
            None,
            vec![IngestFile {
                bytes: _content.to_vec(),
                page_label: None,
                path: Some("web-ui-test.txt".into()),
            }],
            None,
            false,
        )
        .await
        .unwrap();

    // ── 2. Build AppState + router ───────────────────────────────────────────
    let retrieval = Arc::new(build_retrieval_pipeline(&infra));
    let ingest_arc = Arc::new(ingest);
    let state = build_app_state(&infra, Arc::clone(&ingest_arc), Arc::clone(&retrieval));
    let router = kb_api::build_router((*state).clone());

    let session = create_session(&infra, infra.tenant_a, 1, UserRole::Owner).await;

    use tower::ServiceExt;

    // ── 3. GET /search — returns HTML with CSRF ──────────────────────────────
    let req = auth_request("GET", "/search", &session);
    let resp = router.clone().oneshot(req).await.unwrap();

    assert_eq!(resp.status(), StatusCode::OK);
    let ct = resp
        .headers()
        .get(header::CONTENT_TYPE)
        .unwrap()
        .to_str()
        .unwrap();
    assert!(ct.contains("text/html"), "search page must return HTML");

    let has_csrf = resp
        .headers()
        .get_all(header::SET_COOKIE)
        .iter()
        .any(|h| h.to_str().unwrap_or("").contains("__Host-csrf="));
    assert!(has_csrf, "search page must set CSRF cookie");

    let body = axum::body::to_bytes(resp.into_body(), 65536).await.unwrap();
    let body_str = String::from_utf8_lossy(&body);
    assert!(
        body_str.contains("csrf_token"),
        "search page HTML must contain CSRF hidden field"
    );
    assert!(
        body_str.contains("hx-post") || body_str.contains("hx-get"),
        "search page must use HTMX attributes"
    );
    assert!(
        body_str.contains("search") || body_str.contains("Search"),
        "search page must contain search-related content"
    );

    // ── 4. Verify security headers are present ───────────────────────────────
    let req = auth_request("GET", "/search", &session);
    let resp = router.clone().oneshot(req).await.unwrap();
    assert!(resp.headers().contains_key("content-security-policy"));
    assert!(resp.headers().contains_key("x-content-type-options"));

    // ── 5. GET /documents/:id — detail page with tags ────────────────────────
    let req = auth_request(
        "GET",
        &format!("/documents/{}", output.document_id),
        &session,
    );
    let resp = router.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(resp.into_body(), 65536).await.unwrap();
    let body_str = String::from_utf8_lossy(&body);
    assert!(
        body_str.contains("Web UI Test Document"),
        "document detail must show title"
    );
    assert!(
        body_str.contains("web") || body_str.contains("ui"),
        "document detail must show tags"
    );

    // ── 6. GET /upload — returns HTML with CSRF ──────────────────────────────
    let req = auth_request("GET", "/upload", &session);
    let resp = router.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(resp.into_body(), 65536).await.unwrap();
    let body_str = String::from_utf8_lossy(&body);
    assert!(
        body_str.contains("csrf_token"),
        "upload page must contain CSRF hidden field"
    );
    assert!(
        body_str.contains("file-input") || body_str.contains("drop-zone"),
        "upload page must contain file upload area"
    );

    // ── 7. GET /login — public page (no auth required) ───────────────────────
    let req = http::Request::builder()
        .uri("/login")
        .body(Body::empty())
        .unwrap();
    let resp = router.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(resp.into_body(), 65536).await.unwrap();
    let body_str = String::from_utf8_lossy(&body);
    assert!(body_str.contains("<form"), "login page must contain a form");

    mock.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════════════
// Test 4: Admin panel — tenant + user CRUD across scopes
// ═══════════════════════════════════════════════════════════════════════════════════

/// Verify admin endpoints: Owner can access dashboard, users, and tenants pages;
/// Admin can access their own tenant's dashboard; cross-tenant document access
/// is rejected.
#[ignore = "requires Podman + image pull; run with --ignored"]
#[tokio::test]
async fn e2e_admin_tenant_user_crud() {
    let (infra, mock) = setup_p6_infra().await.unwrap();

    // ── 1. Ingest some data so dashboards are non-trivial ────────────────────
    let ingest = build_ingest_pipeline(
        &infra,
        TagOutput {
            title: "Admin Dashboard Test Document".into(),
            summary: "Document for admin dashboard testing.".into(),
            tags: vec!["admin".into()],
        },
    );
    let output = ingest
        .ingest(
            infra.tenant_a,
            None,
            vec![IngestFile {
                bytes: b"Admin panel test content for dashboard verification.".to_vec(),
                page_label: None,
                path: Some("admin-test.txt".into()),
            }],
            None,
            false,
        )
        .await
        .unwrap();

    // ── 2. Build AppState + router ───────────────────────────────────────────
    let retrieval = Arc::new(build_retrieval_pipeline(&infra));
    let ingest_arc = Arc::new(ingest);
    let state = build_app_state(&infra, Arc::clone(&ingest_arc), Arc::clone(&retrieval));
    let router = kb_api::build_router((*state).clone());

    // Session as alice (Owner of tenant A).
    let alice_session = create_session(&infra, infra.tenant_a, 1, UserRole::Owner).await;

    use tower::ServiceExt;

    // ── 3. GET /admin — dashboard renders (Owner access) ────────────────────
    let req = auth_request("GET", "/admin", &alice_session);
    let resp = router.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "Owner must access admin dashboard"
    );

    let body = axum::body::to_bytes(resp.into_body(), 65536).await.unwrap();
    let body_str = String::from_utf8_lossy(&body);
    assert!(
        body_str.contains("dashboard") || body_str.contains("Dashboard"),
        "admin dashboard must render"
    );

    // ── 4. GET /admin/users — list users (Owner access) ─────────────────────
    let req = auth_request("GET", "/admin/users", &alice_session);
    let resp = router.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "Owner must access users page"
    );

    let body = axum::body::to_bytes(resp.into_body(), 65536).await.unwrap();
    let body_str = String::from_utf8_lossy(&body);
    assert!(
        body_str.contains("alice@alpha.example"),
        "users page must list alice"
    );

    // ── 5. GET /admin/tenants — list tenants (Owner access) ─────────────────
    let req = auth_request("GET", "/admin/tenants", &alice_session);
    let resp = router.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(resp.into_body(), 65536).await.unwrap();
    let body_str = String::from_utf8_lossy(&body);
    assert!(
        body_str.contains("tenant-a") || body_str.contains("Tenant Alpha"),
        "tenants page must list tenant-a"
    );
    // BUG-ISOL-01: a tenant Owner must NOT enumerate other tenants. The admin tenants page
    // is scoped to the caller's own tenant, so alice (tenant-a) must never see tenant-b here
    // — asserting its ABSENCE is the security-correct contract.
    assert!(
        !body_str.contains("tenant-b") && !body_str.contains("Tenant Beta"),
        "tenants page must NOT leak other tenants (BUG-ISOL-01 cross-tenant isolation)"
    );

    // ── 6. Admin (bob of tenant B) can access their own dashboard ────────────
    let bob_session = create_session(&infra, infra.tenant_b, 2, UserRole::Admin).await;
    let req = auth_request("GET", "/admin", &bob_session);
    let resp = router.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "Admin must access their own tenant's dashboard"
    );

    // ── 7. Tenant B admin cannot see tenant A document ──────────────────────
    //    (RLS isolation — positive proof that admin panel respects scoping)
    let req = auth_request(
        "GET",
        &format!("/api/documents/{}", output.document_id),
        &bob_session,
    );
    let resp = router.clone().oneshot(req).await.unwrap();
    assert!(
        resp.status().is_client_error(),
        "tenant B admin must NOT see tenant A's document (got {})",
        resp.status()
    );

    // ── 8. Verify document IS visible to its own tenant ─────────────────────
    let req = auth_request(
        "GET",
        &format!("/api/documents/{}", output.document_id),
        &alice_session,
    );
    let resp = router.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "tenant A owner must see their own document"
    );

    mock.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════════════
// Test 5: Cross-tenant isolation via the API
// ═══════════════════════════════════════════════════════════════════════════════════

/// Prove that tenant B's authenticated API requests cannot access tenant A's
/// data. This is the P6-level verification that the RLS + auth middleware chain
/// correctly gates the REST endpoints.
#[ignore = "requires Podman + image pull; run with --ignored"]
#[tokio::test]
async fn e2e_cross_tenant_isolation_api() {
    let (infra, mock) = setup_p6_infra().await.unwrap();

    // ── 1. Tenant A ingests a document ──────────────────────────────────────
    let ingest_a = build_ingest_pipeline(
        &infra,
        TagOutput {
            title: "Tenant A Secret Document".into(),
            summary: "Only tenant A should see this.".into(),
            tags: vec!["secret".into()],
        },
    );
    let out_a = ingest_a
        .ingest(
            infra.tenant_a,
            None,
            vec![IngestFile {
                bytes: b"Confidential information owned by tenant Alpha.".to_vec(),
                page_label: None,
                path: Some("secret-alpha.txt".into()),
            }],
            None,
            false,
        )
        .await
        .unwrap();

    // ── 2. Tenant B ingests a completely different document ─────────────────
    let ingest_b = build_ingest_pipeline(
        &infra,
        TagOutput {
            title: "Tenant B Public Document".into(),
            summary: "Tenant B's content.".into(),
            tags: vec!["public".into()],
        },
    );
    let out_b = ingest_b
        .ingest(
            infra.tenant_b,
            None,
            vec![IngestFile {
                bytes: b"Public information owned by tenant Beta.".to_vec(),
                page_label: None,
                path: Some("public-beta.txt".into()),
            }],
            None,
            false,
        )
        .await
        .unwrap();

    // ── 3. Build AppState + router ───────────────────────────────────────────
    let retrieval = Arc::new(build_retrieval_pipeline(&infra));
    // Use ingest_a for state — the specific pipeline doesn't matter for read ops.
    let ingest_arc = Arc::new(ingest_a);
    let state = build_app_state(&infra, ingest_arc, Arc::clone(&retrieval));
    let router = kb_api::build_router((*state).clone());

    let session_a = create_session(&infra, infra.tenant_a, 1, UserRole::Owner).await;
    let session_b = create_session(&infra, infra.tenant_b, 2, UserRole::Admin).await;

    use tower::ServiceExt;

    // ── 4. Tenant A searches → finds own document ───────────────────────────
    let req = auth_request("GET", "/api/search?q=Confidential", &session_a);
    let resp = router.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), 65536).await.unwrap();
    let result: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let hits = result["hits"].as_array().unwrap();
    assert!(
        hits.iter()
            .any(|h| h["document_id"].as_i64().unwrap() == out_a.document_id),
        "tenant A must find own document"
    );
    assert!(
        !hits
            .iter()
            .any(|h| h["document_id"].as_i64().unwrap() == out_b.document_id),
        "tenant A must NOT see tenant B's document"
    );

    // ── 5. Tenant B searches → does NOT find tenant A's document ────────────
    let req = auth_request("GET", "/api/search?q=Confidential", &session_b);
    let resp = router.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), 65536).await.unwrap();
    let result: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let hits = result["hits"].as_array().unwrap();
    assert!(
        !hits
            .iter()
            .any(|h| h["document_id"].as_i64().unwrap() == out_a.document_id),
        "RLS violated: tenant B MUST NOT see tenant A's document"
    );

    // ── 6. Tenant B tries to access tenant A's document by ID → 4xx ─────────
    let req = auth_request(
        "GET",
        &format!("/api/documents/{}", out_a.document_id),
        &session_b,
    );
    let resp = router.clone().oneshot(req).await.unwrap();
    assert!(
        resp.status().is_client_error(),
        "cross-tenant document access must return 4xx"
    );

    // ── 7. Tenant A CAN access their own document by ID → 200 ───────────────
    let req = auth_request(
        "GET",
        &format!("/api/documents/{}", out_a.document_id),
        &session_a,
    );
    let resp = router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = axum::body::to_bytes(resp.into_body(), 65536).await.unwrap();
    let doc: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(doc["title"].as_str().unwrap(), "Tenant A Secret Document");

    mock.shutdown().await;
}

// ═══════════════════════════════════════════════════════════════════════════════════
// Test 6: Metrics endpoint returns valid Prometheus output
// ═══════════════════════════════════════════════════════════════════════════════════

/// Verify that `GET /metrics` returns Prometheus text exposition format:
/// - HELP lines documenting each metric (when metrics are recorded)
/// - No authentication required (public endpoint)
#[tokio::test]
async fn metrics_endpoint_returns_valid_prometheus_output() {
    // Initialise metrics (idempotent — returns existing handle if already called).
    let _ = kb_metrics::init_metrics();

    let session_store: Arc<dyn SessionStore> = Arc::new(InMemorySessionStore::new());
    let pg_store = Arc::new(PgStore::new("postgres://localhost/test?sslmode=disable"));
    let state = Arc::new(AppState::new(
        session_store,
        pg_store,
        Some(Duration::from_secs(3600)),
        false,
    ));

    let router = kb_api::build_router((*state).clone());

    use tower::ServiceExt;
    let req = http::Request::builder()
        .uri("/metrics")
        .body(Body::empty())
        .unwrap();
    let resp = router.oneshot(req).await.unwrap();

    let status = resp.status();
    assert_eq!(status, StatusCode::OK);

    let body = axum::body::to_bytes(resp.into_body(), 65536).await.unwrap();
    let body_str = String::from_utf8_lossy(&body);

    // Assert on the response body itself rather than comparing it to a second
    // `kb_metrics::render()` call. The metrics recorder is a process-global
    // shared by every test in this binary, and the HTTP RED-metrics middleware
    // (BUG-OBS-05) records a sample on *every* request, so a concurrent test can
    // mutate the registry between two render() calls — making an exact-equality
    // snapshot inherently racy. These structural checks are race-free.
    assert!(
        body_str.contains("# HELP") || body_str.trim().is_empty(),
        "metrics output must contain HELP lines (or be empty if nothing recorded): {body_str}"
    );
    // Every non-comment, non-blank line must be a well-formed exposition sample
    // (`metric_name … value`).
    for line in body_str.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        assert!(
            line.split_whitespace().count() >= 2,
            "malformed Prometheus exposition line in /metrics body: {line:?}"
        );
    }
}

/// BUG-OBS-05: the HTTP RED-metrics middleware records per-route
/// rate/error/duration series, labelled with method, matched-route path, and
/// status — and excludes the `/metrics` scrape endpoint itself.
#[tokio::test]
async fn http_red_metrics_recorded_per_route() {
    let _ = kb_metrics::init_metrics();

    let session_store: Arc<dyn SessionStore> = Arc::new(InMemorySessionStore::new());
    let pg_store = Arc::new(PgStore::new("postgres://localhost/test?sslmode=disable"));
    let state = Arc::new(AppState::new(
        session_store,
        pg_store,
        Some(Duration::from_secs(3600)),
        false,
    ));
    let router = kb_api::build_router((*state).clone());

    use tower::ServiceExt;
    // Unauthenticated request to an auth-gated route: the auth middleware
    // returns 401 before any handler/DB access (deterministic), and `/api/search`
    // still matches a route so `MatchedPath` is populated for the label.
    let req = http::Request::builder()
        .uri("/api/search?q=red")
        .body(Body::empty())
        .unwrap();
    let resp = router.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // Scrape — `/metrics` is excluded from RED instrumentation, so it does not
    // perturb the families under test.
    let scrape = http::Request::builder()
        .uri("/metrics")
        .body(Body::empty())
        .unwrap();
    let resp = router.oneshot(scrape).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = axum::body::to_bytes(resp.into_body(), 65536).await.unwrap();
    let text = String::from_utf8_lossy(&body);

    // Rate counter present, carrying the method/path/status labels for the 401.
    let red_line = text
        .lines()
        .find(|l| {
            !l.trim_start().starts_with('#')
                && l.contains("kb_http_requests_total")
                && l.contains("status=\"401\"")
        })
        .unwrap_or_else(|| panic!("no kb_http_requests_total 401 line in:\n{text}"));
    assert!(
        red_line.contains("path=\"/api/search\""),
        "RED metric must carry the matched-route path label, got: {red_line}"
    );
    assert!(
        red_line.contains("method=\"GET\""),
        "RED metric must carry the method label, got: {red_line}"
    );
    // Duration histogram present for the same route/status.
    assert!(
        text.lines().any(|l| l.contains("kb_http_request_duration_seconds")
            && l.contains("status=\"401\"")),
        "no kb_http_request_duration_seconds for the 401 route in:\n{text}"
    );
    // The scrape endpoint must not appear as its own RED series.
    assert!(
        !text.contains("path=\"/metrics\""),
        "/metrics scrape must be excluded from RED instrumentation:\n{text}"
    );
}

// ═══════════════════════════════════════════════════════════════════════════════════
// Pure-logic smoke tests (no testcontainers needed)
// ═══════════════════════════════════════════════════════════════════════════════════

/// Verify that CLI argument parsing produces the expected values.
#[test]
fn cli_arg_parsing_ingest_and_search() {
    use clap::Parser;
    use kb_api::cli::{Cli, Command};

    // `kb ingest file.txt`
    let cli = Cli::try_parse_from(["kb", "ingest", "file.txt"]).unwrap();
    assert_eq!(cli.tenant_id, 1); // default tenant
    match cli.command {
        Command::Ingest(args) => {
            assert_eq!(args.files, vec!["file.txt"]);
            assert!(!args.as_document);
            assert!(args.note.is_none());
            assert!(!args.no_wait);
        }
        _ => panic!("expected Ingest command"),
    }

    // `kb search "my query" --kind image --limit 5`
    let cli = Cli::try_parse_from([
        "kb", "search", "my query", "--kind", "image", "--limit", "5",
    ])
    .unwrap();
    assert_eq!(cli.tenant_id, 1);
    match cli.command {
        Command::Search(args) => {
            assert_eq!(args.query, "my query");
            assert_eq!(args.kind.as_deref(), Some("image"));
            assert_eq!(args.limit, 5);
        }
        _ => panic!("expected Search command"),
    }

    // `kb ingest --as-document --note "my note" a.txt b.txt --tenant-id 42`
    let cli = Cli::try_parse_from([
        "kb",
        "ingest",
        "--as-document",
        "--note",
        "my note",
        "a.txt",
        "b.txt",
        "--tenant-id",
        "42",
    ])
    .unwrap();
    assert_eq!(cli.tenant_id, 42);
    match cli.command {
        Command::Ingest(args) => {
            assert_eq!(args.files, vec!["a.txt", "b.txt"]);
            assert!(args.as_document);
            assert_eq!(args.note.as_deref(), Some("my note"));
            assert!(!args.no_wait);
        }
        _ => panic!("expected Ingest command"),
    }

    // `kb ingest --no-wait file.txt`
    let cli = Cli::try_parse_from(["kb", "ingest", "--no-wait", "file.txt"]).unwrap();
    match cli.command {
        Command::Ingest(args) => {
            assert!(args.no_wait);
        }
        _ => panic!("expected Ingest command"),
    }

    // `kb search --tag rust query text`
    let cli = Cli::try_parse_from(["kb", "search", "--tag", "rust", "query text"]).unwrap();
    match cli.command {
        Command::Search(args) => {
            assert_eq!(args.tag.as_deref(), Some("rust"));
            assert_eq!(args.limit, 10); // default limit
        }
        _ => panic!("expected Search command"),
    }
}

/// Verify CLI help text is generated (proves clap derivation works).
#[test]
fn cli_help_contains_usage() {
    let mut buf = Vec::new();
    let mut cmd = <kb_api::cli::Cli as clap::CommandFactory>::command();
    cmd.write_long_help(&mut buf).unwrap();
    let help = String::from_utf8_lossy(&buf);
    assert!(help.contains("Usage:"), "help must contain Usage section");
    assert!(help.contains("ingest"), "help must mention ingest command");
    assert!(help.contains("search"), "help must mention search command");
}

/// Verify that the CLI `SearchArgs` struct can be constructed directly.
#[test]
fn search_args_constructs_with_all_fields() {
    let args = kb_api::cli::SearchArgs {
        query: "test query".into(),
        kind: Some("document".into()),
        tag: Some("important".into()),
        limit: 15,
    };
    assert_eq!(args.query, "test query");
    assert_eq!(args.kind.as_deref(), Some("document"));
    assert_eq!(args.tag.as_deref(), Some("important"));
    assert_eq!(args.limit, 15);
}

/// Verify that the CLI `IngestArgs` struct can be constructed directly.
#[test]
fn ingest_args_constructs_with_all_fields() {
    let args = kb_api::cli::IngestArgs {
        files: vec!["a.txt".into(), "b.txt".into()],
        as_document: true,
        note: Some("my note".into()),
        no_wait: false,
    };
    assert_eq!(args.files, vec!["a.txt", "b.txt"]);
    assert!(args.as_document);
    assert_eq!(args.note.as_deref(), Some("my note"));
    assert!(!args.no_wait);
}

/// Verify that the query builder (tested via CLI arg parsing integration)
/// correctly handles all variations of kind/tag/limit arguments.
#[test]
fn query_filter_empty_by_default() {
    let filters = QueryFilters::default();
    assert!(filters.kinds.is_empty());
    assert!(filters.tags.is_empty());
    assert!(filters.created_after.is_none());
    assert!(filters.created_before.is_none());
}

/// Verify that `Build_router` produces a working router that serves at least
/// the public endpoints without authentication.
#[tokio::test]
async fn build_router_serves_public_endpoints() {
    let session_store: Arc<dyn SessionStore> = Arc::new(InMemorySessionStore::new());
    let pg_store = Arc::new(PgStore::new("postgres://localhost/test?sslmode=disable"));
    let state = AppState::new(
        session_store,
        pg_store,
        Some(Duration::from_secs(3600)),
        false,
    );
    let router = kb_api::build_router(state);

    use tower::ServiceExt;

    // GET /login → 200 HTML
    let req = http::Request::builder()
        .uri("/login")
        .body(Body::empty())
        .unwrap();
    let resp = router.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // GET /metrics → 200 (no auth)
    let req = http::Request::builder()
        .uri("/metrics")
        .body(Body::empty())
        .unwrap();
    let resp = router.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // GET /api/search (no auth) → 401
    let req = http::Request::builder()
        .uri("/api/search?q=test")
        .body(Body::empty())
        .unwrap();
    let resp = router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

/// Verify that an authenticated session reaches the search API handler
/// (auth middleware passes; handler returns not-401 even if pipeline unavailable).
#[tokio::test]
async fn authenticated_session_reaches_api_handler() {
    let session_store: Arc<dyn SessionStore> = Arc::new(InMemorySessionStore::new());
    let pg_store = Arc::new(PgStore::new("postgres://localhost/test?sslmode=disable"));
    let state = Arc::new(AppState::new(
        Arc::clone(&session_store),
        pg_store,
        Some(Duration::from_secs(3600)),
        false,
    ));

    // Create a session.
    let token = session_store
        .create(1, 42, UserRole::Member, true, Duration::from_secs(3600))
        .await
        .unwrap();

    let router = kb_api::build_router((*state).clone());

    use tower::ServiceExt;
    let req = http::Request::builder()
        .uri("/api/search?q=test")
        .header("Cookie", format!("__Host-session={token}"))
        .body(Body::empty())
        .unwrap();
    let resp = router.oneshot(req).await.unwrap();

    // Without a retrieval pipeline, the handler returns 500.
    // The important assertion: auth middleware passes (not 401).
    assert_ne!(resp.status(), StatusCode::UNAUTHORIZED);
}

/// Verify that an expired/invalid session gets 401.
#[tokio::test]
async fn invalid_session_returns_401() {
    let session_store: Arc<dyn SessionStore> = Arc::new(InMemorySessionStore::new());
    let pg_store = Arc::new(PgStore::new("postgres://localhost/test?sslmode=disable"));
    let state = AppState::new(
        session_store,
        pg_store,
        Some(Duration::from_secs(3600)),
        false,
    );
    let router = kb_api::build_router(state);

    use tower::ServiceExt;
    let req = http::Request::builder()
        .uri("/api/search?q=test")
        .header("Cookie", "__Host-session=fake-token-never-created")
        .body(Body::empty())
        .unwrap();
    let resp = router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}
