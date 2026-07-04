// SPDX-License-Identifier: AGPL-3.0-or-later

//! `POST /assistant/upload` — multipart file ingestion into the KB pipeline.

use std::sync::Arc;

use axum::Extension;
use axum::Json;
use axum::extract::Multipart;
use kb_core::auth::AuthUser;

use crate::handlers::AssistantState;

/// Response for a single uploaded file.
#[derive(serde::Serialize)]
pub(crate) struct UploadedFile {
    name: String,
    document_id: Option<i64>,
    status: String,
}

/// Response for the batch upload.
#[derive(serde::Serialize)]
pub(crate) struct UploadResponse {
    files: Vec<UploadedFile>,
}

/// Handle multipart file upload. Each file is ingested into the KB pipeline
/// (extract → tag → embed → store) scoped to the authenticated user's tenant.
/// Returns document IDs that the assistant can reference in subsequent prompts.
///
/// The auth middleware (mounted by `kb-api` around the assistant sub-router)
/// injects [`AuthUser`]; we extract it as `Option` and refuse with 401 rather
/// than 500 if it is ever absent, so a mis-mounted router fails closed instead
/// of ingesting into the wrong tenant.
pub async fn upload_handler(
    Extension(state): Extension<Arc<AssistantState>>,
    auth: Option<Extension<AuthUser>>,
    mut multipart: Multipart,
) -> Result<Json<UploadResponse>, (axum::http::StatusCode, Json<serde_json::Value>)> {
    let Some(Extension(auth)) = auth else {
        return Err((
            axum::http::StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"error": "authentication required"})),
        ));
    };

    let ingest = match &state.ingest {
        Some(p) => p.clone(),
        None => {
            return Err((
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error": "ingestion pipeline not configured"})),
            ));
        }
    };

    // Scope every ingested document to the authenticated tenant/user so the
    // upload lands in (and is only searchable from) the caller's own corpus.
    let tenant_id = auth.tenant_id;
    let user_id = Some(auth.user_id);

    let mut uploaded = Vec::new();
    while let Ok(Some(field)) = multipart.next_field().await {
        let name = field.file_name().unwrap_or("unknown").to_string();
        let data = match field.bytes().await {
            Ok(b) => b,
            Err(e) => {
                uploaded.push(UploadedFile {
                    name,
                    document_id: None,
                    status: format!("read error: {e}"),
                });
                continue;
            }
        };

        let file = kb_pipeline::IngestFile {
            bytes: data.to_vec(),
            page_label: Some(name.clone()),
            path: Some(name.clone()),
        };
        match ingest
            .ingest(tenant_id, user_id, vec![file], None, true)
            .await
        {
            Ok(output) => {
                uploaded.push(UploadedFile {
                    name,
                    document_id: Some(output.document_id),
                    status: "ingested".into(),
                });
            }
            Err(e) => {
                uploaded.push(UploadedFile {
                    name,
                    document_id: None,
                    status: format!("ingestion failed: {e}"),
                });
            }
        }
    }

    Ok(Json(UploadResponse { files: uploaded }))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::time::Duration;

    use async_trait::async_trait;
    use axum::Router;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use axum::routing::post;
    use kb_core::chunk::Chunk;
    use kb_core::file::FileRecord;
    use kb_core::query::{Hit, Query};
    use kb_core::reranker::Reranker;
    use kb_core::store::Store;
    use kb_core::user::UserRole;
    use kb_store::PgStore;
    use tower::ServiceExt;

    use super::*;
    use crate::config_ext::AssistantConfig;
    use crate::prompt::PromptBuilder;
    use crate::session::SessionManager;

    /// A `Store` that fails every call — the upload handler must never reach it
    /// in the paths these tests exercise.
    struct NoopStore;

    #[async_trait]
    impl Store for NoopStore {
        async fn upsert_file(&self, _rec: &FileRecord) -> anyhow::Result<i64> {
            anyhow::bail!("NoopStore: not implemented")
        }
        async fn upsert_chunks(&self, _file_id: i64, _chunks: &[Chunk]) -> anyhow::Result<()> {
            anyhow::bail!("NoopStore: not implemented")
        }
        async fn hybrid_search(
            &self,
            _tenant_id: i64,
            _query: &Query,
            _query_embedding: &[f32],
        ) -> anyhow::Result<Vec<Hit>> {
            anyhow::bail!("NoopStore: not implemented")
        }
        async fn keyword_search(
            &self,
            _tenant_id: i64,
            _query: &Query,
        ) -> anyhow::Result<Vec<Hit>> {
            anyhow::bail!("NoopStore: not implemented")
        }
    }

    /// A `Reranker` that fails every call, for the same reason.
    struct NoopReranker;

    #[async_trait]
    impl Reranker for NoopReranker {
        async fn rerank(
            &self,
            _query: &str,
            _docs: &[String],
            _local_only: bool,
            _priority: i32,
            _tenant_id: i64,
            _user_id: Option<i64>,
        ) -> anyhow::Result<Vec<f32>> {
            anyhow::bail!("NoopReranker: not implemented")
        }
    }

    /// Assistant state with a backend-less retrieval pipeline and NO ingest
    /// pipeline — enough for the auth / 503 paths, which must reject before
    /// touching any pipeline component.
    fn test_state() -> Arc<AssistantState> {
        let pool = kb_scheduler::Pool::new(vec![], Duration::from_millis(50));
        let llm = Arc::new(kb_llm::LlamaClient::new(
            pool,
            reqwest::Client::new(),
            0,
            0,
            Duration::from_millis(50),
        ));
        let embedder = Arc::new(kb_pipeline::embedder::ChunkEmbedder::new(
            llm,
            "test-model".into(),
            3,
        ));
        let store: Arc<dyn Store> = Arc::new(NoopStore);
        let reranker: Arc<dyn Reranker> = Arc::new(NoopReranker);
        let pipeline = Arc::new(kb_pipeline::RetrievalPipeline::new(
            embedder, store, reranker,
        ));
        Arc::new(AssistantState {
            cfg: AssistantConfig::default(),
            store: Arc::new(PgStore::new("postgres://user:pw@127.0.0.1:1/unreachable")),
            pipeline,
            ingest: None,
            executor: None,
            prompt_builder: PromptBuilder::new(50),
            sessions: SessionManager::new(),
        })
    }

    fn test_auth_user() -> AuthUser {
        AuthUser {
            tenant_id: 42,
            user_id: 7,
            role: UserRole::Member,
            email_verified: true,
        }
    }

    fn multipart_request() -> Request<Body> {
        let body = concat!(
            "--BOUNDARY\r\n",
            "Content-Disposition: form-data; name=\"file\"; filename=\"a.txt\"\r\n",
            "Content-Type: text/plain\r\n",
            "\r\n",
            "hello\r\n",
            "--BOUNDARY--\r\n",
        );
        Request::post("/upload")
            .header("content-type", "multipart/form-data; boundary=BOUNDARY")
            .body(Body::from(body))
            .unwrap()
    }

    #[tokio::test]
    async fn upload_without_auth_extension_is_401_not_500() {
        // Fails closed: no AuthUser in extensions → 401, and the check runs
        // BEFORE the pipeline check (a mis-mounted router must not leak a 503
        // capability probe to unauthenticated callers).
        let app = Router::new()
            .route("/upload", post(upload_handler))
            .layer(Extension(test_state()));
        let resp = app.oneshot(multipart_request()).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn upload_without_ingest_pipeline_is_503() {
        let app = Router::new()
            .route("/upload", post(upload_handler))
            .layer(Extension(test_state()))
            .layer(Extension(test_auth_user()));
        let resp = app.oneshot(multipart_request()).await.unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        let bytes = axum::body::to_bytes(resp.into_body(), 4096).await.unwrap();
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v["error"], "ingestion pipeline not configured");
    }

    #[test]
    fn upload_response_serializes_document_ids() {
        let resp = UploadResponse {
            files: vec![
                UploadedFile {
                    name: "a.txt".into(),
                    document_id: Some(5),
                    status: "ingested".into(),
                },
                UploadedFile {
                    name: "b.txt".into(),
                    document_id: None,
                    status: "ingestion failed: boom".into(),
                },
            ],
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert_eq!(json["files"][0]["document_id"], 5);
        assert_eq!(json["files"][1]["document_id"], serde_json::Value::Null);
        assert_eq!(json["files"][1]["status"], "ingestion failed: boom");
    }
}
