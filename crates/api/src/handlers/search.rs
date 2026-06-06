//! `GET /api/search` — hybrid search handler.
//!
//! Accepts query parameters (`q`, `kind`, `tag`, `limit`), calls the
//! [`RetrievalPipeline`], and returns ranked [`Hit`]s with deep-link provenance.

use std::sync::Arc;

use axum::Extension;
use axum::extract::{Query, State};
use axum::http::{HeaderValue, StatusCode};
use axum::response::{IntoResponse, Json, Response};
use kb_core::query::{Query as SearchQuery, QueryFilters};
use kb_pipeline::SearchMode;
use serde::{Deserialize, Serialize};

use crate::AppState;
use crate::AuthUser;

/// Response header naming which search path produced the results (P14-T5):
/// `hybrid` (full AI path) or `keyword` (degraded lexical-only path).
const SEARCH_MODE_HEADER: &str = "X-Search-Mode";

// ── Request / response types ───────────────────────────────────────────────────

/// Query-string parameters for `GET /api/search`.
#[derive(Debug, Deserialize)]
pub struct SearchParams {
    /// Natural-language search query (required).
    pub q: String,
    /// Comma-separated document kinds, e.g. `?kind=image,document`.
    #[serde(default)]
    pub kind: Option<String>,
    /// Comma-separated canonical tag names, e.g. `?tag=invoice,report`.
    #[serde(default)]
    pub tag: Option<String>,
    /// Maximum results to return (default 10, capped at 100).
    #[serde(default = "default_limit")]
    pub limit: usize,
}

/// Default `limit` when not specified.
const fn default_limit() -> usize {
    10
}

/// Maximum allowed `limit` value.
const MAX_LIMIT: usize = 100;

/// A single hit in the search response (stripped of internal ids where needed,
/// but preserving deep-link provenance).
#[derive(Debug, Serialize)]
pub struct SearchHit {
    /// The matched document id.
    document_id: i64,
    /// Relevance score (higher = better).
    score: f32,
    /// Document title, if available.
    title: Option<String>,
    /// Snippet from the best-matching chunk.
    snippet: String,
    /// The file/page that produced the winning chunk.
    file_id: i64,
    /// Page number, if applicable.
    page_no: Option<i32>,
    /// Seconds offset for audio/video chunks.
    ts_offset: Option<f64>,
    /// Document kind string (e.g. "document", "image"), if available.
    kind: Option<String>,
}

/// Successful search response.
#[derive(Debug, Serialize)]
pub struct SearchResponse {
    /// Ranked search results (already reranked, top-k).
    hits: Vec<SearchHit>,
    /// Echoed query text.
    query: String,
    /// Number of results returned.
    count: usize,
    /// Which search path produced these results: `"hybrid"` (full AI path) or
    /// `"keyword"` (degraded lexical-only path — tenant over its AI budget or the
    /// embed/rerank backend was unavailable). Mirrors the `X-Search-Mode` header (P14-T5).
    mode: String,
}

/// Error response.
#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    error: String,
    message: String,
}

// ── Handler ────────────────────────────────────────────────────────────────────

/// `GET /api/search` — hybrid search with optional filters.
///
/// # Query parameters
///
/// * `q` (required) — natural-language search query.
/// * `kind` (optional, repeatable) — filter by [`DocKind`](kb_core::kind::DocKind).
/// * `tag` (optional, repeatable) — filter by canonical tag name.
/// * `limit` (optional, default 10, max 100) — max results to return.
///
/// # Response
///
/// * `200 OK` — [`SearchResponse`] with ranked hits.
/// * `400 Bad Request` — invalid filter (e.g. unknown document kind).
/// * `401 Unauthorized` — rejected by middleware.
/// * `500 Internal Server Error` — pipeline or store failure.
pub async fn search(
    State(state): State<Arc<AppState>>,
    Extension(auth_user): Extension<AuthUser>,
    Query(params): Query<SearchParams>,
) -> Result<Response, (StatusCode, Json<ErrorResponse>)> {
    // ── Handle empty query ───────────────────────────────────────────────────
    if params.q.trim().is_empty() {
        return Ok(search_ok(
            SearchResponse {
                hits: Vec::new(),
                query: params.q.clone(),
                count: 0,
                mode: SearchMode::Hybrid.as_str().to_string(),
            },
            SearchMode::Hybrid,
        ));
    }

    let limit = params.limit.min(MAX_LIMIT);

    // ── Resolve filters ─────────────────────────────────────────────────────
    let kind_strs: Vec<&str> = params
        .kind
        .as_deref()
        .map(|s| {
            s.split(',')
                .map(|p| p.trim())
                .filter(|p| !p.is_empty())
                .collect()
        })
        .unwrap_or_default();
    let kinds: Result<Vec<kb_core::kind::DocKind>, _> = kind_strs
        .iter()
        .map(|k| {
            std::str::FromStr::from_str(k).map_err(|_| format!("unknown document kind: '{k}'"))
        })
        .collect();
    let kinds = kinds.map_err(|msg| bad_request("invalid_kind", &msg))?;

    let tags: Vec<String> = params
        .tag
        .as_deref()
        .map(|s| {
            s.split(',')
                .map(|p| p.trim().to_string())
                .filter(|t| !t.is_empty())
                .collect()
        })
        .unwrap_or_default();

    // ── Ensure pipeline is configured ────────────────────────────────────────
    let retrieval = state.retrieval_pipeline.as_ref().ok_or_else(|| {
        internal_error(anyhow::anyhow!("search: retrieval pipeline not configured"))
    })?;

    // ── Budget gate → degrade (NOT reject) ───────────────────────────────────
    // If the tenant has already met/exceeded its monthly token budget, search must NOT
    // 429 — it degrades to the keyword-only (lexical) path (P14-T5).
    let budget = state
        .pg_store
        .check_plan_token_budget_rollup(auth_user.tenant_id)
        .await;
    let degrade = budget_check_forces_degrade(budget);

    // ── Build query + run pipeline ───────────────────────────────────────────
    let query = SearchQuery {
        text: params.q.clone(),
        filters: QueryFilters {
            kinds,
            tags,
            ..Default::default()
        },
        top_k: limit,
    };

    let (hits, mode) = retrieval
        .retrieve(
            auth_user.tenant_id,
            Some(auth_user.user_id),
            &query,
            false,
            degrade,
        )
        .await
        .map_err(|e| {
            tracing::warn!(error = %e, "search failed");
            internal_error(e)
        })?;

    let count = hits.len();
    let response_hits: Vec<SearchHit> = hits
        .into_iter()
        .map(|h| SearchHit {
            document_id: h.document_id,
            score: h.score,
            title: h.title,
            snippet: h.snippet,
            file_id: h.file_id,
            page_no: h.page_no,
            ts_offset: h.ts_offset,
            kind: h.kind,
        })
        .collect();

    Ok(search_ok(
        SearchResponse {
            hits: response_hits,
            query: params.q,
            count,
            mode: mode.as_str().to_string(),
        },
        mode,
    ))
}

/// Build a `200 OK` JSON search response carrying the `X-Search-Mode` header (P14-T5).
fn search_ok(body: SearchResponse, mode: SearchMode) -> Response {
    let mut resp = (StatusCode::OK, Json(body)).into_response();
    resp.headers_mut()
        .insert(SEARCH_MODE_HEADER, HeaderValue::from_static(mode.as_str()));
    resp
}

/// Decide whether the monthly-token-budget check should force the keyword degrade path
/// (B2, P14-T5). Search **never rejects** on budget — over-budget tenants degrade.
///
/// - `Ok(())` → budget OK → `false` (run the full AI path).
/// - `Err` carrying a [`QuotaError`](kb_core::quota::QuotaError) (the over-budget signal
///   from [`check_plan_token_budget_rollup`](kb_store::PgStore::check_plan_token_budget_rollup))
///   → records a `search_tokens` quota rejection and returns `true` (degrade, NOT 429).
/// - Any other `Err` (e.g. a transient DB error) → `false`, failing safe to the full path;
///   the only consequence of misjudging this is which (equally tenant-isolated) path runs.
pub(crate) fn budget_check_forces_degrade(budget: anyhow::Result<()>) -> bool {
    match budget {
        Ok(()) => false,
        Err(e) if e.downcast_ref::<kb_core::quota::QuotaError>().is_some() => {
            kb_metrics::record_quota_rejection("search_tokens");
            true
        }
        Err(e) => {
            tracing::warn!(error = %e, "search: token-budget check failed; using full path");
            false
        }
    }
}

// ── Error helpers ──────────────────────────────────────────────────────────────

fn bad_request(code: &str, msg: &str) -> (StatusCode, Json<ErrorResponse>) {
    (
        StatusCode::BAD_REQUEST,
        Json(ErrorResponse {
            error: code.into(),
            message: msg.into(),
        }),
    )
}

fn internal_error(e: impl std::fmt::Display) -> (StatusCode, Json<ErrorResponse>) {
    tracing::error!(error = %e, "search handler error");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorResponse {
            error: "internal_error".into(),
            message: "an unexpected error occurred".into(),
        }),
    )
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::sync::Arc;
    use std::time::Duration;

    use async_trait::async_trait;
    use axum::Router;
    use axum::body::Body;
    use axum::http::{Method, StatusCode};
    use axum::routing::get;
    use kb_core::chunk::Chunk;
    use kb_core::file::FileRecord;
    use kb_core::query::{Hit, Query};
    use kb_core::reranker::Reranker;
    use kb_core::session::{DEFAULT_SESSION_TTL_SECS, SessionStore};
    use kb_core::store::Store;
    use kb_core::user::UserRole;
    use kb_pipeline::RetrievalPipeline;
    use kb_pipeline::embedder::ChunkEmbedder;
    use kb_store::PgStore;
    use kb_store::session_store::InMemorySessionStore;
    use tower::ServiceExt;

    use super::*;
    use crate::AppState;
    use crate::middleware::auth_middleware;

    // ── Mock Store ──────────────────────────────────────────────────────────

    struct MockStore {
        hits: Arc<[Hit]>,
    }

    impl MockStore {
        fn new(hits: Vec<Hit>) -> Self {
            Self { hits: hits.into() }
        }
    }

    #[async_trait]
    impl Store for MockStore {
        async fn upsert_file(&self, _rec: &FileRecord) -> anyhow::Result<i64> {
            anyhow::bail!("mock: only search is used")
        }
        async fn upsert_chunks(&self, _file_id: i64, _chunks: &[Chunk]) -> anyhow::Result<()> {
            anyhow::bail!("mock: only search is used")
        }
        async fn hybrid_search(
            &self,
            _tenant_id: i64,
            _query: &Query,
            _query_embedding: &[f32],
        ) -> anyhow::Result<Vec<Hit>> {
            Ok(self.hits.to_vec())
        }
        async fn keyword_search(
            &self,
            _tenant_id: i64,
            _query: &Query,
        ) -> anyhow::Result<Vec<Hit>> {
            Ok(self.hits.to_vec())
        }
    }

    // ── Mock Reranker ───────────────────────────────────────────────────────

    struct MockReranker {
        scores: Arc<[f32]>,
    }

    impl MockReranker {
        fn new(scores: Vec<f32>) -> Self {
            Self {
                scores: scores.into(),
            }
        }
    }

    #[async_trait]
    impl Reranker for MockReranker {
        async fn rerank(
            &self,
            _query: &str,
            _docs: &[String],
            _local_only: bool,
            _priority: i32,
            _tenant_id: i64,
            _user_id: Option<i64>,
        ) -> anyhow::Result<Vec<f32>> {
            Ok(self.scores.to_vec())
        }
    }

    // ── Test helpers ────────────────────────────────────────────────────────

    fn make_hit(doc_id: i64, score: f32, snippet: &str, file_id: i64, page: i32) -> Hit {
        Hit {
            document_id: doc_id,
            score,
            title: Some(format!("Doc {doc_id}")),
            snippet: snippet.to_string(),
            file_id,
            page_no: Some(page),
            ts_offset: None,
            kind: None,
        }
    }

    /// Build a pipeline with mock store + mock reranker + a mock-backed embedder.
    async fn build_mock_pipeline(
        hits: Vec<Hit>,
        scores: Vec<f32>,
    ) -> (RetrievalPipeline, kb_mock_backend::MockBackend) {
        use kb_core::role::Role;
        use kb_llm::LlamaClient;
        use kb_mock_backend::MockBackend;
        use kb_scheduler::{Pool, test_backend};
        use reqwest::Client;

        let mock = MockBackend::start().await;
        let base_url = mock.url("/v1");
        let backend = Arc::new(test_backend(
            "mock-search-api",
            base_url,
            vec![Role::Embed],
            0,
            4,
        ));
        let pool = Pool::new(vec![backend], Duration::from_secs(5));
        let llm = Arc::new(LlamaClient::new(
            pool,
            Client::new(),
            0,
            0,
            Duration::from_millis(200),
        ));
        let embedder = Arc::new(ChunkEmbedder::new(llm, "test-model".into(), 3));
        let store: Arc<dyn Store> = Arc::new(MockStore::new(hits));
        let reranker: Arc<dyn Reranker> = Arc::new(MockReranker::new(scores));

        let pipeline = RetrievalPipeline::new(embedder, store, reranker);
        (pipeline, mock)
    }

    /// Build a pipeline whose embedder has NO backends, so the full path's `embed_query`
    /// fails with a backend-unavailable error and `retrieve` falls back to keyword-only.
    /// The `MockStore` returns `hits` from `keyword_search`. No mock HTTP server needed.
    fn build_degrading_pipeline(hits: Vec<Hit>) -> RetrievalPipeline {
        use kb_llm::LlamaClient;
        use kb_scheduler::Pool;
        use reqwest::Client;

        let pool = Pool::new(vec![], Duration::from_millis(50));
        let llm = Arc::new(LlamaClient::new(
            pool,
            Client::new(),
            0,
            0,
            Duration::from_millis(50),
        ));
        let embedder = Arc::new(ChunkEmbedder::new(llm, "test-model".into(), 3));
        let store: Arc<dyn Store> = Arc::new(MockStore::new(hits));
        let reranker: Arc<dyn Reranker> = Arc::new(MockReranker::new(vec![]));
        RetrievalPipeline::new(embedder, store, reranker)
    }

    fn test_state() -> Arc<AppState> {
        let session_store: Arc<dyn SessionStore> = Arc::new(InMemorySessionStore::new());
        let pg_store = Arc::new(PgStore::new("postgres://localhost/test?sslmode=disable"));
        Arc::new(AppState::new(
            session_store,
            pg_store,
            Some(Duration::from_secs(DEFAULT_SESSION_TTL_SECS)),
            false,
        ))
    }

    fn search_router(state: Arc<AppState>) -> Router {
        Router::new()
            .route("/api/search", get(search))
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                auth_middleware,
            ))
            .with_state(state)
    }

    // ── Search handler integration tests ────────────────────────────────────

    #[tokio::test]
    async fn unauthenticated_returns_401() {
        let state = test_state();
        let router = search_router(state);

        let request = axum::http::Request::builder()
            .method(Method::GET)
            .uri("/api/search?q=test")
            .body(Body::empty())
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn empty_query_returns_200_with_empty_results() {
        let state = test_state();
        let token = state
            .session_store
            .create(1, 42, UserRole::Member, true, Duration::from_secs(3600))
            .await
            .unwrap();
        let router = search_router(state);

        // Empty q parameter → 200 with zero hits (usability — not a client error).
        let request = axum::http::Request::builder()
            .method(Method::GET)
            .uri("/api/search?q=%20%20")
            .header("Cookie", format!("__Host-session={token}"))
            .body(Body::empty())
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let result: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(result["count"], 0);
        assert!(result["hits"].as_array().unwrap().is_empty());
        assert_eq!(result["query"], "  "); // echoed as-is
        assert!(result.get("error").is_none());
    }

    #[tokio::test]
    async fn pipeline_not_configured_returns_500() {
        let state = test_state();
        let token = state
            .session_store
            .create(1, 42, UserRole::Member, true, Duration::from_secs(3600))
            .await
            .unwrap();
        let router = search_router(state);

        let request = axum::http::Request::builder()
            .method(Method::GET)
            .uri("/api/search?q=hello")
            .header("Cookie", format!("__Host-session={token}"))
            .body(Body::empty())
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn search_returns_ranked_hits() {
        let hits = vec![
            make_hit(10, 0.9, "snippet 10", 100, 1),
            make_hit(20, 0.7, "snippet 20", 200, 2),
        ];
        let rerank_scores = vec![0.95, 0.65];
        let (pipeline, mock) = build_mock_pipeline(hits, rerank_scores).await;

        let state = test_state();
        // We can't modify an Arc, so build a new AppState with the pipeline.
        let session_store = state.session_store.clone();
        let pg_store = state.pg_store.clone();
        let new_state = Arc::new(
            AppState::new(
                session_store,
                pg_store,
                Some(Duration::from_secs(DEFAULT_SESSION_TTL_SECS)),
                false,
            )
            .with_retrieval_pipeline(Arc::new(pipeline)),
        );

        let token = new_state
            .session_store
            .create(1, 42, UserRole::Member, true, Duration::from_secs(3600))
            .await
            .unwrap();
        let router = search_router(new_state);

        let request = axum::http::Request::builder()
            .method(Method::GET)
            .uri("/api/search?q=test+query&limit=5")
            .header("Cookie", format!("__Host-session={token}"))
            .body(Body::empty())
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        // Full AI path → hybrid mode is signalled in the header.
        assert_eq!(
            response.headers().get("X-Search-Mode").unwrap(),
            "hybrid",
            "successful AI search must report hybrid mode"
        );

        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let result: serde_json::Value = serde_json::from_slice(&body).unwrap();

        assert_eq!(result["count"], 2);
        assert_eq!(result["query"], "test query");
        assert_eq!(result["mode"], "hybrid");
        assert_eq!(result["hits"][0]["document_id"], 10); // higher rerank score (0.95) first
        assert_eq!(result["hits"][1]["document_id"], 20);
        assert_eq!(result["hits"][0]["page_no"], 1);

        mock.shutdown().await;
    }

    #[tokio::test]
    async fn degraded_search_returns_keyword_mode_not_429() {
        // The embedder has no backends → the full path's embed fails → retrieve falls back
        // to keyword-only. The handler must answer 200 with X-Search-Mode: keyword (search
        // NEVER 429s — it degrades). This also covers the budget-degrade signalling shape,
        // since both routes converge on SearchMode::Keyword.
        let hits = vec![make_hit(5, 0.5, "lexical match", 50, 1)];
        let pipeline = build_degrading_pipeline(hits);

        let session_store: Arc<dyn SessionStore> = Arc::new(InMemorySessionStore::new());
        let pg_store = Arc::new(PgStore::new("postgres://localhost/test?sslmode=disable"));
        let state = Arc::new(
            AppState::new(
                session_store,
                pg_store,
                Some(Duration::from_secs(DEFAULT_SESSION_TTL_SECS)),
                false,
            )
            .with_retrieval_pipeline(Arc::new(pipeline)),
        );

        let token = state
            .session_store
            .create(1, 42, UserRole::Member, true, Duration::from_secs(3600))
            .await
            .unwrap();
        let router = search_router(state);

        let request = axum::http::Request::builder()
            .method(Method::GET)
            .uri("/api/search?q=anything")
            .header("Cookie", format!("__Host-session={token}"))
            .body(Body::empty())
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        // NOT 429 — search degrades, it does not reject.
        assert_eq!(response.status(), StatusCode::OK);
        assert_ne!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            response.headers().get("X-Search-Mode").unwrap(),
            "keyword",
            "degraded search must report keyword mode"
        );

        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let result: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(result["mode"], "keyword");
        assert_eq!(result["count"], 1);
        assert_eq!(result["hits"][0]["document_id"], 5);
    }

    #[tokio::test]
    async fn search_with_kind_and_tag_filters() {
        let hits = vec![make_hit(1, 0.8, "match", 10, 1)];
        let (pipeline, mock) = build_mock_pipeline(hits, vec![0.9]).await;

        let session_store: Arc<dyn SessionStore> = Arc::new(InMemorySessionStore::new());
        let pg_store = Arc::new(PgStore::new("postgres://localhost/test?sslmode=disable"));
        let state = Arc::new(
            AppState::new(
                session_store,
                pg_store,
                Some(Duration::from_secs(DEFAULT_SESSION_TTL_SECS)),
                false,
            )
            .with_retrieval_pipeline(Arc::new(pipeline)),
        );

        let token = state
            .session_store
            .create(1, 42, UserRole::Member, true, Duration::from_secs(3600))
            .await
            .unwrap();
        let router = search_router(state);

        let request = axum::http::Request::builder()
            .method(Method::GET)
            .uri("/api/search?q=photo&kind=image&tag=vacation&limit=3")
            .header("Cookie", format!("__Host-session={token}"))
            .body(Body::empty())
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        mock.shutdown().await;
    }

    #[tokio::test]
    async fn invalid_kind_returns_400() {
        let state = test_state();
        let token = state
            .session_store
            .create(1, 42, UserRole::Member, true, Duration::from_secs(3600))
            .await
            .unwrap();
        let router = search_router(state);

        let request = axum::http::Request::builder()
            .method(Method::GET)
            .uri("/api/search?q=test&kind=notarealkind")
            .header("Cookie", format!("__Host-session={token}"))
            .body(Body::empty())
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let err: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(err["error"], "invalid_kind");
    }

    #[tokio::test]
    async fn limit_capped_at_100() {
        let hits: Vec<Hit> = (0..150)
            .map(|i| make_hit(i as i64, 0.5, &format!("s{i}"), (i * 10) as i64, 1))
            .collect();
        let scores: Vec<f32> = (0..150).map(|_| 0.5f32).collect();
        let (pipeline, mock) = build_mock_pipeline(hits, scores).await;

        let session_store: Arc<dyn SessionStore> = Arc::new(InMemorySessionStore::new());
        let pg_store = Arc::new(PgStore::new("postgres://localhost/test?sslmode=disable"));
        let state = Arc::new(
            AppState::new(
                session_store,
                pg_store,
                Some(Duration::from_secs(DEFAULT_SESSION_TTL_SECS)),
                false,
            )
            .with_retrieval_pipeline(Arc::new(pipeline)),
        );

        let token = state
            .session_store
            .create(1, 42, UserRole::Member, true, Duration::from_secs(3600))
            .await
            .unwrap();
        let router = search_router(state);

        // Request limit=200 but should be capped at 100.
        let request = axum::http::Request::builder()
            .method(Method::GET)
            .uri("/api/search?q=many&limit=200")
            .header("Cookie", format!("__Host-session={token}"))
            .body(Body::empty())
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), 65536)
            .await
            .unwrap();
        let result: serde_json::Value = serde_json::from_slice(&body).unwrap();
        // top_k is capped, so pipeline returns at most 100.
        assert!(result["count"].as_u64().unwrap() <= 100);

        mock.shutdown().await;
    }

    #[tokio::test]
    async fn empty_results_is_success() {
        let (pipeline, mock) = build_mock_pipeline(vec![], vec![]).await;

        let session_store: Arc<dyn SessionStore> = Arc::new(InMemorySessionStore::new());
        let pg_store = Arc::new(PgStore::new("postgres://localhost/test?sslmode=disable"));
        let state = Arc::new(
            AppState::new(
                session_store,
                pg_store,
                Some(Duration::from_secs(DEFAULT_SESSION_TTL_SECS)),
                false,
            )
            .with_retrieval_pipeline(Arc::new(pipeline)),
        );

        let token = state
            .session_store
            .create(1, 42, UserRole::Member, true, Duration::from_secs(3600))
            .await
            .unwrap();
        let router = search_router(state);

        let request = axum::http::Request::builder()
            .method(Method::GET)
            .uri("/api/search?q=nonexistent")
            .header("Cookie", format!("__Host-session={token}"))
            .body(Body::empty())
            .unwrap();

        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        let result: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(result["count"], 0);
        assert!(result["hits"].as_array().unwrap().is_empty());

        mock.shutdown().await;
    }

    // ── Error helper tests ──────────────────────────────────────────────────

    #[test]
    fn bad_request_has_400_status() {
        let (status, body) = bad_request("missing_query", "q is required");
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body.error, "missing_query");
    }

    #[test]
    fn internal_error_has_500_status() {
        let (status, body) = internal_error(anyhow::anyhow!("pipeline error"));
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(body.error, "internal_error");
    }

    // ── Budget → degrade gate (B2, P14-T5) ───────────────────────────────────

    #[test]
    fn budget_ok_does_not_degrade() {
        assert!(!budget_check_forces_degrade(Ok(())));
    }

    #[test]
    fn budget_quota_error_forces_degrade() {
        // An over-budget tenant surfaces a QuotaError::TokensExceeded → degrade (NOT 429).
        let err = anyhow::Error::new(kb_core::quota::QuotaError::TokensExceeded {
            current: 10_000,
            additional: 1,
            total: 10_001,
            limit: 10_000,
            plan_code: Some("free".into()),
            upsell_plan_code: Some("pro".into()),
        });
        assert!(
            budget_check_forces_degrade(Err(err)),
            "over-budget tenant must degrade to keyword-only"
        );
    }

    #[test]
    fn budget_other_error_does_not_degrade() {
        // A transient DB error is not a quota signal → fail safe to the full path.
        let err = anyhow::anyhow!("database connection reset");
        assert!(!budget_check_forces_degrade(Err(err)));
    }

    #[test]
    fn search_mode_field_serializes() {
        let resp = SearchResponse {
            hits: Vec::new(),
            query: "q".into(),
            count: 0,
            mode: SearchMode::Keyword.as_str().to_string(),
        };
        let v = serde_json::to_value(&resp).unwrap();
        assert_eq!(v["mode"], "keyword");
    }

    // ── SearchParams deserialization tests ──────────────────────────────────

    #[test]
    fn search_params_defaults() {
        let params: SearchParams = serde_urlencoded::from_str("q=hello").unwrap();
        assert_eq!(params.q, "hello");
        assert!(params.kind.is_none());
        assert!(params.tag.is_none());
        assert_eq!(params.limit, 10);
    }

    #[test]
    fn search_params_full() {
        let params: SearchParams =
            serde_urlencoded::from_str("q=test&kind=image,document&tag=photo&limit=25").unwrap();
        assert_eq!(params.q, "test");
        assert_eq!(params.kind.as_deref(), Some("image,document"));
        assert_eq!(params.tag.as_deref(), Some("photo"));
        assert_eq!(params.limit, 25);
    }
}
