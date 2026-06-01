//! Retrieval eval integration test (plan §19, P6-T12).
//!
//! Ingests a deterministic fixture corpus of 8 documents into a real pgvector
//! Postgres via testcontainers, runs the full [`RetrievalPipeline`] against 7
//! labelled queries, computes recall@k + MRR, and asserts the metrics do not
//! regress below the committed baseline in `tests/fixtures/eval_baseline.json`.
//!
//! Mock LLM backends provide the query embedding (1024-dim spike vectors) and
//! a pass-through reranker. Data is inserted directly via SQL so the test can
//! control embeddings precisely.
//!
//! This test is `#[ignore]` in `just ci` because it requires Podman +
//! `pgvector/pgvector:pg17`. Run with `just ci-integration`.

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use async_trait::async_trait;
    use kb_core::query::{Query, QueryFilters};
    use kb_core::reranker::Reranker;
    use kb_core::role::Role;
    use kb_core::store::Store;
    use kb_llm::LlamaClient;
    use kb_mock_backend::MockBackend;
    use kb_pipeline::embedder::ChunkEmbedder;
    use kb_pipeline::eval::{
        EvalBaseline, LabeledQuery, check_regression, compute_metrics, eval_queries,
        fixture_corpus, format_vector, spike_1024,
    };
    use kb_pipeline::retrieval::RetrievalPipeline;
    use kb_scheduler::{Backend, Pool};
    use kb_store::PgStore;
    use reqwest::Client;

    // ── Mock reranker ────────────────────────────────────────────────────────────

    /// A pass-through reranker that returns 1.0 for every document so the
    /// ranking is determined entirely by the store's hybrid search + RRF fusion.
    struct PassThroughReranker;

    #[async_trait]
    impl Reranker for PassThroughReranker {
        async fn rerank(&self, _query: &str, docs: &[String]) -> anyhow::Result<Vec<f32>> {
            Ok(docs.iter().map(|_| 1.0).collect())
        }
    }

    // ── Testcontainers setup ─────────────────────────────────────────────────────

    /// Connect to a pgvector/pgvector:pg17 testcontainers Postgres via the
    /// shared-per-binary container (kb-testsupport), run migrations, and insert
    /// a tenant row.
    async fn setup_store() -> anyhow::Result<PgStore> {
        let db = kb_testsupport::fresh_db().await?;
        let store = PgStore::with_roles(&db.admin_url, &db.app_url);
        store.connect().await?;

        let pool = store.admin_pool()?;
        sqlx::query("INSERT INTO tenants (slug, name) VALUES ('eval', 'Eval')")
            .execute(&pool)
            .await?;

        Ok(store)
    }

    /// Insert the fixture corpus into Postgres.
    ///
    /// Returns `(Vec<i64>)` — the document ids in insertion order (1-based per
    /// this test, matching [`fixture_corpus`]).
    async fn insert_fixture_corpus(pool: &sqlx::postgres::PgPool) -> anyhow::Result<Vec<i64>> {
        let docs = fixture_corpus();
        let mut doc_ids = Vec::with_capacity(docs.len());

        let emb = format_vector(&spike_1024(0));

        for (i, doc) in docs.iter().enumerate() {
            // Insert document.
            let doc_id: i64 = sqlx::query_scalar(
                "INSERT INTO documents (tenant_id, title, kind, status) \
                 VALUES (1, $1, $2, 'ready') RETURNING id",
            )
            .bind(doc.title)
            .bind(doc.kind)
            .fetch_one(pool)
            .await?;
            doc_ids.push(doc_id);

            // Insert file (unique sha per doc).
            let sha = {
                let n = i as u64;
                let mut buf = vec![0u8; 32];
                buf[..8].copy_from_slice(&n.to_be_bytes());
                buf
            };
            let file_id: i64 = sqlx::query_scalar(
                "INSERT INTO files (tenant_id, document_id, page_no, sha256, blob_key, status) \
                 VALUES (1, $1, 1, $2, $3, 'ready') RETURNING id",
            )
            .bind(doc_id)
            .bind(&sha)
            .bind(format!("eval/{doc_id}"))
            .fetch_one(pool)
            .await?;

            // Insert chunk.
            let _chunk_id: i64 = sqlx::query_scalar(
                "INSERT INTO chunks (tenant_id, document_id, file_id, page_no, idx, content, \
                 embedding) \
                 VALUES (1, $1, $2, 1, 0, $3, CAST($4 AS vector)) RETURNING id",
            )
            .bind(doc_id)
            .bind(file_id)
            .bind(doc.content)
            .bind(&emb)
            .fetch_one(pool)
            .await?;

            // Optionally insert tag + document_tag.
            if let Some(tag_name) = doc.tag {
                let tag_id: i64 = sqlx::query_scalar(
                    "INSERT INTO tags (tenant_id, name, embedding) \
                     VALUES (1, $1, CAST($2 AS vector)) \
                     ON CONFLICT (tenant_id, name) DO UPDATE SET name = EXCLUDED.name \
                     RETURNING id",
                )
                .bind(tag_name)
                .bind(&emb)
                .fetch_one(pool)
                .await?;

                sqlx::query(
                    "INSERT INTO document_tags (document_id, tag_id) \
                     VALUES ($1, $2) ON CONFLICT (document_id, tag_id) DO NOTHING",
                )
                .bind(doc_id)
                .bind(tag_id)
                .execute(pool)
                .await?;
            }
        }

        Ok(doc_ids)
    }

    /// Build a [`RetrievalPipeline`] with a mock LLM backend for query
    /// embedding (returns spike at 0) and a pass-through reranker.
    async fn build_retrieval_pipeline(store: Arc<PgStore>) -> (RetrievalPipeline, MockBackend) {
        let mock = MockBackend::start().await;
        let base_url = mock.url("/v1");

        // 1024-dim spike at 0 for all query embeddings.
        mock.scenario().lock().await.embed_content = Some(vec![spike_1024(0)]);

        let backend = Arc::new(Backend::new("mock-eval", base_url, vec![Role::Embed], 0, 8));
        let pool = Pool::new(vec![backend], Duration::from_secs(10));
        let llm = Arc::new(LlamaClient::new(
            pool,
            Client::new(),
            0,
            0,
            Duration::from_millis(200),
        ));

        let embedder = Arc::new(ChunkEmbedder::new(llm, "bge-m3".into(), 1024));
        let store_trait: Arc<dyn Store> = store as Arc<dyn Store>;
        let reranker: Arc<dyn Reranker> = Arc::new(PassThroughReranker);

        let pipeline = RetrievalPipeline::new(embedder, store_trait, reranker);
        (pipeline, mock)
    }

    // ── The eval integration test ────────────────────────────────────────────────

    /// Full eval: ingest fixture corpus → run labelled queries → compute metrics
    /// → assert no regression against the committed baseline.
    #[ignore]
    #[tokio::test]
    async fn eval_retrieval_regression_gate() {
        // ── Setup ───────────────────────────────────────────────────────────
        let store = setup_store().await.unwrap();
        let pool = store.admin_pool().unwrap();

        let doc_ids = insert_fixture_corpus(&pool).await.unwrap();
        assert_eq!(doc_ids.len(), 8, "fixture corpus must have 8 documents");

        let store_arc = Arc::new(store);
        let (pipeline, mock) = build_retrieval_pipeline(store_arc).await;

        // ── Run queries ─────────────────────────────────────────────────────
        let queries = eval_queries();
        let mut all_retrieved: Vec<Vec<i64>> = Vec::with_capacity(queries.len());

        for query in &queries {
            let hits = pipeline
                .retrieve(
                    1, // tenant_id
                    &Query {
                        text: query.text.clone(),
                        filters: QueryFilters::default(),
                        top_k: 10,
                    },
                )
                .await
                .unwrap();

            all_retrieved.push(hits.iter().map(|h| h.document_id).collect());
        }

        // ── Compute metrics ─────────────────────────────────────────────────
        let metrics = compute_metrics(&queries, &all_retrieved);

        // ── Check against baseline ──────────────────────────────────────────
        let baseline_json = include_str!("fixtures/eval_baseline.json");
        let baseline: EvalBaseline =
            serde_json::from_str(baseline_json).expect("eval_baseline.json must be valid");

        let regressions = check_regression(&metrics, &baseline);
        if !regressions.is_empty() {
            panic!(
                "RETRIEVAL EVAL REGRESSION DETECTED:\n  {}\n\n\
                 Computed metrics:\n  recall@1={:.4}  recall@3={:.4}  recall@5={:.4}  MRR={:.4}\n\
                 Baseline ({}):\n  recall@1={:.4}  recall@3={:.4}  recall@5={:.4}  MRR={:.4}\n\n\
                 If this change is intentional (e.g. tuning RRF weights or TAG_MERGE_THRESHOLD), \
                 update crates/pipeline/tests/fixtures/eval_baseline.json with the new values \
                 and document the reason in the note field.",
                regressions.join("\n  "),
                metrics.recall_at_1,
                metrics.recall_at_3,
                metrics.recall_at_5,
                metrics.mrr,
                baseline.note,
                baseline.metrics.recall_at_1,
                baseline.metrics.recall_at_3,
                baseline.metrics.recall_at_5,
                baseline.metrics.mrr,
            );
        }

        // ── Clean shutdown ──────────────────────────────────────────────────
        mock.shutdown().await;
    }

    // ── Additional integration tests ────────────────────────────────────────────

    /// Verify that an empty corpus (no documents) produces vacuous perfect
    /// metrics when there are no labelled queries either.
    #[ignore]
    #[tokio::test]
    async fn empty_corpus_produces_no_regression() {
        let queries: Vec<LabeledQuery> = vec![];
        let results: Vec<Vec<i64>> = vec![];
        let metrics = compute_metrics(&queries, &results);
        assert!((metrics.recall_at_1 - 1.0).abs() < f64::EPSILON);
        assert!((metrics.mrr - 1.0).abs() < f64::EPSILON);
    }

    /// Verify the fixture corpus documents can be inserted and the
    /// corresponding chunks have their `tsv` column auto-populated.
    #[ignore]
    #[tokio::test]
    async fn fixture_chunks_have_tsv() {
        let store = setup_store().await.unwrap();
        let pool = store.admin_pool().unwrap();

        let doc_ids = insert_fixture_corpus(&pool).await.unwrap();
        assert!(!doc_ids.is_empty());

        // Every chunk should have a non-null tsv.
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM chunks WHERE tenant_id = 1 AND tsv IS NOT NULL",
        )
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(count, 8, "all 8 chunks must have generated tsv");
    }

    /// Verify keyword search reaches chunks by querying the `tsv` column
    /// directly — sanity check that our fixture content is searchable.
    #[ignore]
    #[tokio::test]
    async fn keyword_search_finds_fixture_documents() {
        let store = setup_store().await.unwrap();
        let pool = store.admin_pool().unwrap();

        insert_fixture_corpus(&pool).await.unwrap();

        // The word "Rust" appears in docs 1 and 2.
        let rows: Vec<(i64, String)> = sqlx::query_as(
            "SELECT c.document_id, c.content \
             FROM chunks c \
             WHERE c.tenant_id = 1 AND c.tsv @@ websearch_to_tsquery('simple', 'Rust') \
             ORDER BY ts_rank(c.tsv, websearch_to_tsquery('simple', 'Rust')) DESC",
        )
        .fetch_all(&pool)
        .await
        .unwrap();

        assert!(
            rows.len() >= 2,
            "'Rust' keyword should match at least 2 chunks, got {}",
            rows.len()
        );
        for (_doc_id, content) in &rows {
            assert!(
                content.to_lowercase().contains("rust"),
                "matched chunk must contain 'Rust': {content}"
            );
        }
    }
}
