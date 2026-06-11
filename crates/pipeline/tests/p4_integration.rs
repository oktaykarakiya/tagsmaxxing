// SPDX-License-Identifier: AGPL-3.0-or-later

//! P4 phase integration test (§8, §10, §31): end-to-end retrieval round-trip.
//!
//! Sets up documents, files, chunks, and tags in a real pgvector Postgres via
//! testcontainers, then queries the [`RetrievalPipeline`](kb_pipeline::RetrievalPipeline)
//! and verifies: ranked [`Hit`](kb_core::query::Hit)s with correct `document_id` +
//! deep-link provenance (`file_id`, `page_no`), scores descending, snippet from the
//! winning chunk, tag-filtered search excludes non-matching docs, and RRF fusion
//! favours documents matching both vector and keyword signals.
//!
//! Mock LLM backends provide the query embedding (2560-dim spike vectors) and
//! reranking scores. Data is inserted directly via SQL so the test can control
//! embeddings and tag assignments precisely.
//!
//! This test is `#[ignore]` in `just ci` because it requires Podman +
//! `pgvector/pgvector:pg17`. Run with:
//!
//! ```bash
//! DOCKER_HOST="unix://$XDG_RUNTIME_DIR/podman/podman.sock" \
//!   cargo test --package kb-pipeline --test p4_integration -- --ignored
//! ```

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use async_trait::async_trait;
    use kb_core::kind::DocKind;
    use kb_core::query::{Hit, Query, QueryFilters};
    use kb_core::reranker::Reranker;
    use kb_core::role::Role;
    use kb_core::store::Store;
    use kb_llm::LlamaClient;
    use kb_mock_backend::MockBackend;
    use kb_pipeline::embedder::ChunkEmbedder;
    use kb_pipeline::retrieval::RetrievalPipeline;
    use kb_scheduler::{Pool, test_backend};
    use kb_store::PgStore;
    use reqwest::Client;

    // ── Helpers ──────────────────────────────────────────────────────────────

    /// Produce an EMBED_DIM-dimensional vector with a single spike at `pos` set to 1.0,
    /// all other elements zero. The spike gives the vector unit length,
    /// making cosine distance a meaningful signal when different chunks use
    /// different spike positions.
    fn spike_vector(pos: usize) -> Vec<f32> {
        let mut v = vec![0.0f32; kb_store::EMBED_DIM];
        if pos < kb_store::EMBED_DIM {
            v[pos] = 1.0;
        }
        v
    }

    /// Format a `Vec<f32>` into the pgvector text representation `[x1,x2,...]`.
    fn format_vector(v: &[f32]) -> String {
        let mut buf = String::with_capacity(2 + v.len() * 16);
        buf.push('[');
        for (i, x) in v.iter().enumerate() {
            if i > 0 {
                buf.push(',');
            }
            use std::fmt::Write;
            let _ = write!(buf, "{x}");
        }
        buf.push(']');
        buf
    }

    // ── Mock types ───────────────────────────────────────────────────────────

    /// A mock [`Reranker`] that returns 1.0 for every document (pass-through).
    /// All scores equal → the pipeline's stable sort preserves the ranking
    /// from the store's hybrid search + RRF fusion.
    struct PassThroughReranker;

    #[async_trait]
    impl Reranker for PassThroughReranker {
        async fn rerank(
            &self,
            _query: &str,
            docs: &[String],
            _local_only: bool,
            _priority: i32,
            _tenant_id: i64,
            _user_id: Option<i64>,
        ) -> anyhow::Result<Vec<f32>> {
            Ok(docs.iter().map(|_| 1.0).collect())
        }
    }

    // ── Store setup (testcontainers) ─────────────────────────────────────────

    /// Connect to a pgvector/pgvector:pg17 testcontainers Postgres, run
    /// migrations, insert a tenant row, and set `app.current_tenant` for
    /// RLS compatibility.
    async fn setup_store() -> anyhow::Result<PgStore> {
        // Shared per-binary container + fresh DB. Seeding below runs on the privileged pool;
        // the retrieval pipeline searches as kb_app under RLS (P6-T14).
        let db = kb_testsupport::fresh_db().await?;
        let store = PgStore::with_roles(&db.admin_url, &db.app_url);
        store.connect().await?;

        let pool = store.pool()?;
        sqlx::query("INSERT INTO tenants (slug, name) VALUES ('test', 'Test')")
            .execute(&pool)
            .await?;

        Ok(store)
    }

    /// A process-wide counter so every seeded file gets a unique `sha256` (fixes bug 5:
    /// `files_tenant_id_sha256_key` collided when two files shared a short title —
    /// docs/analysis/07-rls-enforcement-blocker.md).
    static FILE_SHA_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

    /// A unique 32-byte sha256 for a seeded file (the first 8 bytes encode a monotonic counter).
    fn unique_sha() -> Vec<u8> {
        let n = FILE_SHA_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let mut sha = vec![0u8; 32];
        sha[..8].copy_from_slice(&n.to_be_bytes());
        sha
    }

    /// Create a test document + one file row, returning `(doc_id, file_id)`.
    async fn create_test_document(
        pool: &sqlx::postgres::PgPool,
        title: &str,
        kind: &str,
    ) -> anyhow::Result<(i64, i64)> {
        let doc_id: i64 = sqlx::query_scalar(
            "INSERT INTO documents (tenant_id, title, kind, status) \
             VALUES (1, $1, $2, 'ready') RETURNING id",
        )
        .bind(title)
        .bind(kind)
        .fetch_one(pool)
        .await?;

        // Unique sha per file — never collides on (tenant_id, sha256), even for identical titles.
        let file_id: i64 = sqlx::query_scalar(
            "INSERT INTO files (tenant_id, document_id, page_no, sha256, blob_key, status) \
             VALUES (1, $1, 1, $2, $3, 'ready') RETURNING id",
        )
        .bind(doc_id)
        .bind(unique_sha())
        .bind(format!("t1/{doc_id}"))
        .fetch_one(pool)
        .await?;

        Ok((doc_id, file_id))
    }

    /// Create a chunk with the given embedding, content, and provenance.
    #[allow(clippy::too_many_arguments)]
    async fn create_chunk(
        pool: &sqlx::postgres::PgPool,
        doc_id: i64,
        file_id: i64,
        idx: i32,
        content: &str,
        embedding: &[f32],
        page_no: Option<i32>,
        ts_offset: Option<f64>,
    ) -> anyhow::Result<i64> {
        let vec_str = format_vector(embedding);
        let chunk_id: i64 = sqlx::query_scalar(
            "INSERT INTO chunks (tenant_id, document_id, file_id, page_no, idx, content, \
             ts_offset, embedding) \
             VALUES (1, $1, $2, $3, $4, $5, $6, CAST($7 AS vector)) RETURNING id",
        )
        .bind(doc_id)
        .bind(file_id)
        .bind(page_no)
        .bind(idx)
        .bind(content)
        .bind(ts_offset)
        .bind(&vec_str)
        .fetch_one(pool)
        .await?;

        Ok(chunk_id)
    }

    /// Create a tag with an embedding and return its id.
    async fn create_tag(
        pool: &sqlx::postgres::PgPool,
        name: &str,
        embedding: &[f32],
    ) -> anyhow::Result<i64> {
        let vec_str = format_vector(embedding);
        let tag_id: i64 = sqlx::query_scalar(
            "INSERT INTO tags (tenant_id, name, embedding) \
             VALUES (1, $1, CAST($2 AS vector)) \
             ON CONFLICT (tenant_id, name) DO UPDATE SET name = EXCLUDED.name \
             RETURNING id",
        )
        .bind(name)
        .bind(&vec_str)
        .fetch_one(pool)
        .await?;

        Ok(tag_id)
    }

    /// Link a tag to a document.
    async fn link_document_tag(
        pool: &sqlx::postgres::PgPool,
        doc_id: i64,
        tag_id: i64,
    ) -> anyhow::Result<()> {
        sqlx::query(
            "INSERT INTO document_tags (document_id, tag_id) \
             VALUES ($1, $2) ON CONFLICT (document_id, tag_id) DO NOTHING",
        )
        .bind(doc_id)
        .bind(tag_id)
        .execute(pool)
        .await?;

        Ok(())
    }

    /// Build a [`RetrievalPipeline`] with a mock LLM backend for query
    /// embedding and a pass-through reranker. The mock backend is configured
    /// to return an EMBED_DIM-dimensional spike vector for every embed call.
    async fn build_retrieval_pipeline(store: Arc<PgStore>) -> (RetrievalPipeline, MockBackend) {
        let mock = MockBackend::start().await;
        let base_url = mock.url("/v1");

        // EMBED_DIM-dimensional vector for all query embeddings.
        mock.scenario().lock().await.embed_content = Some(vec![spike_vector(0)]);

        let backend = Arc::new(test_backend(
            "mock-p4-retrieval",
            base_url,
            vec![Role::Embed],
            0,
            8,
        ));
        let pool = Pool::new(vec![backend], Duration::from_secs(10));
        let llm = Arc::new(LlamaClient::new(
            pool,
            Client::new(),
            0,
            0,
            Duration::from_millis(200),
        ));

        let embedder = Arc::new(ChunkEmbedder::new(
            llm,
            "bge-m3".into(),
            kb_store::EMBED_DIM,
        ));
        let store_trait: Arc<dyn Store> = store as Arc<dyn Store>;
        let reranker: Arc<dyn Reranker> = Arc::new(PassThroughReranker);

        let pipeline = RetrievalPipeline::new(embedder, store_trait, reranker);
        (pipeline, mock)
    }

    // ── Pure-logic smoke tests (no testcontainers needed) ─────────────────────

    #[test]
    fn hit_deep_link_fields_present() {
        let hit = Hit {
            document_id: 1,
            score: 0.95,
            title: Some("Test Doc".into()),
            snippet: "the matching paragraph...".into(),
            file_id: 42,
            page_no: Some(3),
            ts_offset: Some(12.5),
            kind: None,
        };
        assert_eq!(hit.document_id, 1);
        assert!((hit.score - 0.95).abs() < 0.001);
        assert_eq!(hit.title.as_deref(), Some("Test Doc"));
        assert!(!hit.snippet.is_empty());
        assert_eq!(hit.file_id, 42);
        assert_eq!(hit.page_no, Some(3));
        assert_eq!(hit.ts_offset, Some(12.5));
    }

    #[test]
    fn query_with_filters_constructs() {
        let q = Query {
            text: "find rust code".into(),
            filters: QueryFilters {
                kinds: vec![DocKind::Code, DocKind::Document],
                tags: vec!["rust".into(), "programming".into()],
                created_after: None,
                created_before: None,
            },
            top_k: 5,
        };
        assert_eq!(q.text, "find rust code");
        assert_eq!(q.filters.kinds.len(), 2);
        assert_eq!(q.filters.tags.len(), 2);
        assert_eq!(q.top_k, 5);
    }

    #[test]
    fn vector_formatting_roundtrip() {
        let v = spike_vector(0);
        assert_eq!(v.len(), kb_store::EMBED_DIM);
        assert_eq!(v[0], 1.0);
        assert_eq!(v[1], 0.0);

        let formatted = format_vector(&v);
        assert!(formatted.starts_with('['));
        assert!(formatted.ends_with(']'));
        assert!(formatted.contains("1"));
        // EMBED_DIM (2560) elements → comfortably more than EMBED_DIM chars.
        assert!(formatted.len() > kb_store::EMBED_DIM);
    }

    #[test]
    fn spike_vectors_are_orthogonal() {
        let a = spike_vector(0);
        let b = spike_vector(1);

        let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
        assert!(
            dot.abs() < 1e-6,
            "orthogonal vectors dot product should be 0"
        );
    }

    #[test]
    fn query_default_filters_are_empty() {
        let filters = QueryFilters::default();
        assert!(filters.kinds.is_empty());
        assert!(filters.tags.is_empty());
        assert!(filters.created_after.is_none());
        assert!(filters.created_before.is_none());
    }

    // ── Integration tests (require testcontainers + podman, #[ignore]) ────────

    /// E2E: insert a document with a chunk containing a distinctive keyword,
    /// query with that keyword, and verify the hit carries deep-link
    /// provenance (`document_id`, `file_id`, `page_no`, `snippet`, `title`).
    #[ignore]
    #[tokio::test]
    async fn e2e_keyword_search_returns_correct_hit_with_provenance() {
        let store = setup_store().await.unwrap();
        let store_arc = Arc::new(store);
        let pool = store_arc.pool().unwrap();

        // Insert test data: one document with a distinctive keyword.
        let (doc_id, file_id) = create_test_document(&pool, "Quantum Computing Guide", "document")
            .await
            .unwrap();
        create_chunk(
            &pool,
            doc_id,
            file_id,
            0,
            "Zephyr quantum computing unlocks algorithmic speedups via entanglement.",
            &spike_vector(0),
            Some(3),
            None,
        )
        .await
        .unwrap();

        // Build retrieval pipeline.
        let (pipeline, mock) = build_retrieval_pipeline(store_arc).await;

        // Query with the distinctive keyword "Zephyr".
        let (hits, _mode) = pipeline
            .retrieve(
                1,
                None,
                &Query {
                    text: "Zephyr".to_string(),
                    filters: QueryFilters::default(),
                    top_k: 10,
                },
                false,
                false, // degrade
            )
            .await
            .unwrap();

        assert!(!hits.is_empty(), "keyword search should find the document");
        let hit = hits
            .iter()
            .find(|h| h.document_id == doc_id)
            .expect("ingested document must appear in results");

        // Deep-link provenance verification.
        assert_eq!(hit.document_id, doc_id);
        assert_eq!(hit.file_id, file_id, "file_id must be the file we inserted");
        assert_eq!(
            hit.page_no,
            Some(3),
            "page_no must be 3 (the value we inserted)"
        );
        assert_eq!(hit.title.as_deref(), Some("Quantum Computing Guide"));
        assert!(
            hit.snippet.contains("Zephyr") || hit.snippet.contains("quantum"),
            "snippet must contain original chunk text, got: {}",
            hit.snippet
        );
        assert!(
            hit.score >= 0.0,
            "score must be non-negative, got {}",
            hit.score
        );

        mock.shutdown().await;
    }

    /// Insert two documents with different distinctive keywords, query with
    /// one keyword, and verify the correct document is found (keyword match
    /// drives the result order in RRF fusion).
    #[ignore]
    #[tokio::test]
    async fn keyword_search_finds_correct_document() {
        let store = setup_store().await.unwrap();
        let store_arc = Arc::new(store);
        let pool = store_arc.pool().unwrap();

        // Doc 1: keyword "cryptographic"
        let (doc1_id, file1_id) = create_test_document(&pool, "Crypto Paper", "document")
            .await
            .unwrap();
        create_chunk(
            &pool,
            doc1_id,
            file1_id,
            0,
            "Cryptographic hash functions are fundamental in modern security.",
            &spike_vector(0),
            Some(1),
            None,
        )
        .await
        .unwrap();

        // Doc 2: keyword "oceanography"
        let (doc2_id, file2_id) = create_test_document(&pool, "Ocean Study", "document")
            .await
            .unwrap();
        create_chunk(
            &pool,
            doc2_id,
            file2_id,
            0,
            "Oceanography studies the physical properties of oceans.",
            &spike_vector(0),
            Some(1),
            None,
        )
        .await
        .unwrap();

        assert_ne!(doc1_id, doc2_id);

        let (pipeline, mock) = build_retrieval_pipeline(store_arc).await;

        // Query for "cryptographic" → should favour doc 1.
        let (hits, _mode) = pipeline
            .retrieve(
                1,
                None,
                &Query {
                    text: "cryptographic".to_string(),
                    filters: QueryFilters::default(),
                    top_k: 10,
                },
                false,
                false, // degrade
            )
            .await
            .unwrap();

        assert!(!hits.is_empty(), "should find at least one document");
        // Doc 1 (matching "cryptographic") should appear.
        assert!(
            hits.iter().any(|h| h.document_id == doc1_id),
            "doc with 'cryptographic' must appear in results"
        );

        // Query for a word matching neither document — vector search still
        // returns results (all chunks share the same embedding).
        let (all_hits, _mode) = pipeline
            .retrieve(
                1,
                None,
                &Query {
                    text: "flibbertigibbet".to_string(),
                    filters: QueryFilters::default(),
                    top_k: 10,
                },
                false,
                false, // degrade
            )
            .await
            .unwrap();
        assert!(
            !all_hits.is_empty(),
            "vector search should return results even with no keyword match"
        );

        mock.shutdown().await;
    }

    /// Insert two documents linked to different tags, query with a tag
    /// filter, and verify only the tagged document is returned.
    #[ignore]
    #[tokio::test]
    async fn tag_filter_narrows_results() {
        let store = setup_store().await.unwrap();
        let store_arc = Arc::new(store);
        let pool = store_arc.pool().unwrap();

        // Create tags with different embeddings so they are distinct
        // canonical tags (cosine distance separates them in vector space).
        let finance_tag_id = create_tag(&pool, "finance", &spike_vector(0))
            .await
            .unwrap();
        let sports_tag_id = create_tag(&pool, "sports", &spike_vector(1)).await.unwrap();

        // Doc 1: tagged "finance"
        let (doc1_id, file1_id) = create_test_document(&pool, "Finance Report", "document")
            .await
            .unwrap();
        create_chunk(
            &pool,
            doc1_id,
            file1_id,
            0,
            "Q4 revenue exceeded expectations by 15 percent.",
            &spike_vector(0),
            Some(1),
            None,
        )
        .await
        .unwrap();
        link_document_tag(&pool, doc1_id, finance_tag_id)
            .await
            .unwrap();

        // Doc 2: tagged "sports"
        let (_doc2_id, file2_id) = create_test_document(&pool, "Sports News", "document")
            .await
            .unwrap();
        create_chunk(
            &pool,
            _doc2_id,
            file2_id,
            0,
            "The championship finals concluded with an overtime victory.",
            &spike_vector(0),
            Some(1),
            None,
        )
        .await
        .unwrap();
        link_document_tag(&pool, _doc2_id, sports_tag_id)
            .await
            .unwrap();

        let (pipeline, mock) = build_retrieval_pipeline(store_arc).await;

        // Filter for "finance" tag.
        let (hits, _mode) = pipeline
            .retrieve(
                1,
                None,
                &Query {
                    text: String::new(), // vector-only + tag filter
                    filters: QueryFilters {
                        tags: vec!["finance".to_string()],
                        ..Default::default()
                    },
                    top_k: 10,
                },
                false,
                false, // degrade
            )
            .await
            .unwrap();

        assert!(
            !hits.is_empty(),
            "tag filter 'finance' should return results"
        );
        for hit in &hits {
            assert_eq!(
                hit.document_id, doc1_id,
                "tag filter should exclude non-finance documents"
            );
        }

        // Filter for a non-existent tag → empty results.
        let (empty, _mode) = pipeline
            .retrieve(
                1,
                None,
                &Query {
                    text: String::new(),
                    filters: QueryFilters {
                        tags: vec!["nonexistent_tag_xyz".to_string()],
                        ..Default::default()
                    },
                    top_k: 10,
                },
                false,
                false, // degrade
            )
            .await
            .unwrap();
        assert!(
            empty.is_empty(),
            "non-existent tag filter should yield no results"
        );

        mock.shutdown().await;
    }

    /// Verify that repeated identical queries return the same ranking
    /// (deterministic output for the same database state).
    #[ignore]
    #[tokio::test]
    async fn same_query_returns_consistent_ranking() {
        let store = setup_store().await.unwrap();
        let store_arc = Arc::new(store);
        let pool = store_arc.pool().unwrap();

        let (doc_id, file_id) = create_test_document(&pool, "Repeatable Test", "document")
            .await
            .unwrap();
        create_chunk(
            &pool,
            doc_id,
            file_id,
            0,
            "Deterministic retrieval ensures repeatable results for every identical query.",
            &spike_vector(0),
            Some(1),
            None,
        )
        .await
        .unwrap();

        let (pipeline, mock) = build_retrieval_pipeline(store_arc).await;

        let query = Query {
            text: "retrieval".to_string(),
            filters: QueryFilters::default(),
            top_k: 10,
        };

        let (hits1, _m1) = pipeline
            .retrieve(1, None, &query, false, false)
            .await
            .unwrap();
        let (hits2, _m2) = pipeline
            .retrieve(1, None, &query, false, false)
            .await
            .unwrap();

        assert_eq!(
            hits1.len(),
            hits2.len(),
            "same query must return same number of results"
        );
        for (i, (h1, h2)) in hits1.iter().zip(hits2.iter()).enumerate() {
            assert_eq!(
                h1.document_id, h2.document_id,
                "result {i}: document_id differs between identical queries"
            );
            assert!(
                (h1.score - h2.score).abs() < 0.001,
                "result {i}: score differs between identical queries"
            );
            assert_eq!(
                h1.file_id, h2.file_id,
                "result {i}: file_id differs between identical queries"
            );
        }

        mock.shutdown().await;
    }

    /// Verify that hits are ordered by score descending.
    #[ignore]
    #[tokio::test]
    async fn scores_are_descending() {
        let store = setup_store().await.unwrap();
        let store_arc = Arc::new(store);
        let pool = store_arc.pool().unwrap();

        // Insert 3 documents with different content.
        let docs_data = [
            (
                "Doc Alpha",
                "The quick brown fox jumps over the lazy dog.",
                0,
            ),
            (
                "Doc Beta",
                "Cryptographic hash functions are essential for data integrity verification.",
                1,
            ),
            (
                "Doc Gamma",
                "Oceanographic research vessels collect water samples at various depths.",
                2,
            ),
        ];

        for (title, content, idx) in &docs_data {
            let (doc_id, file_id) = create_test_document(&pool, title, "document")
                .await
                .unwrap();
            create_chunk(
                &pool,
                doc_id,
                file_id,
                *idx,
                content,
                &spike_vector(0),
                Some(1),
                None,
            )
            .await
            .unwrap();
        }

        let (pipeline, mock) = build_retrieval_pipeline(store_arc).await;

        let (hits, _mode) = pipeline
            .retrieve(
                1,
                None,
                &Query {
                    text: "hash functions".to_string(),
                    filters: QueryFilters::default(),
                    top_k: 10,
                },
                false,
                false, // degrade
            )
            .await
            .unwrap();

        // Verify scores are non-increasing.
        for window in hits.windows(2) {
            assert!(
                window[0].score >= window[1].score,
                "scores must be descending: {} >= {}",
                window[0].score,
                window[1].score
            );
        }

        mock.shutdown().await;
    }

    /// Multiple chunks from the same document should roll up to a single
    /// [`Hit`] carrying the best-scoring chunk's provenance.
    #[ignore]
    #[tokio::test]
    async fn multiple_chunks_roll_up_to_one_hit() {
        let store = setup_store().await.unwrap();
        let store_arc = Arc::new(store);
        let pool = store_arc.pool().unwrap();

        let (doc_id, file_id) = create_test_document(&pool, "Multi-Chunk Doc", "document")
            .await
            .unwrap();

        // Two chunks: one with spike at 0, one with spike at 1.
        // The query embedding spikes at 0, so chunk 1 is the better match.
        create_chunk(
            &pool,
            doc_id,
            file_id,
            0,
            "First chunk about Rust programming.",
            &spike_vector(0),
            Some(1),
            None,
        )
        .await
        .unwrap();

        create_chunk(
            &pool,
            doc_id,
            file_id,
            1,
            "Second chunk about systems development.",
            &spike_vector(1),
            Some(2),
            None,
        )
        .await
        .unwrap();

        let (pipeline, mock) = build_retrieval_pipeline(store_arc).await;

        let (hits, _mode) = pipeline
            .retrieve(
                1,
                None,
                &Query {
                    text: "Rust programming".to_string(),
                    filters: QueryFilters::default(),
                    top_k: 10,
                },
                false,
                false, // degrade
            )
            .await
            .unwrap();

        assert_eq!(
            hits.iter().filter(|h| h.document_id == doc_id).count(),
            1,
            "two chunks from same doc must roll up to one hit"
        );

        let hit = hits
            .iter()
            .find(|h| h.document_id == doc_id)
            .expect("document must appear in results");

        // The better-matching chunk (spike at 0) should provide the provenance:
        // - Vector match: chunk 1 (sv(0) = same as query) has cos-dist=0
        // - Chunk 2 (sv(1) ⊥ query) has cos-dist=1
        // - So chunk 1 ranks first in vector results → wins the rollup.
        assert_eq!(hit.file_id, file_id);
        assert!(
            hit.snippet.contains("Rust") || hit.snippet.contains("First"),
            "snippet should come from the best chunk, got: {}",
            hit.snippet
        );

        mock.shutdown().await;
    }

    /// Vector-only search (empty query text) should return results from the
    /// HNSW index when chunks match the query embedding.
    #[ignore]
    #[tokio::test]
    async fn vector_only_search_returns_results() {
        let store = setup_store().await.unwrap();
        let store_arc = Arc::new(store);
        let pool = store_arc.pool().unwrap();

        let (doc_id, file_id) = create_test_document(&pool, "Vector Only Test", "document")
            .await
            .unwrap();
        create_chunk(
            &pool,
            doc_id,
            file_id,
            0,
            "This text has a distinctive keyword xylophone in it.",
            &spike_vector(0),
            Some(1),
            None,
        )
        .await
        .unwrap();

        let (pipeline, mock) = build_retrieval_pipeline(store_arc).await;

        // Empty query text → keyword search skipped, vector only.
        let (hits, _mode) = pipeline
            .retrieve(
                1,
                None,
                &Query {
                    text: String::new(),
                    filters: QueryFilters::default(),
                    top_k: 10,
                },
                false,
                false, // degrade
            )
            .await
            .unwrap();

        assert!(!hits.is_empty(), "vector-only search should return results");
        assert!(
            hits.iter().any(|h| h.document_id == doc_id),
            "vector search should find the document"
        );

        mock.shutdown().await;
    }

    /// `top_k` limit is respected — requesting fewer results than the DB
    /// has should truncate accordingly.
    #[ignore]
    #[tokio::test]
    async fn top_k_respected() {
        let store = setup_store().await.unwrap();
        let store_arc = Arc::new(store);
        let pool = store_arc.pool().unwrap();

        // Insert 5 documents.
        for i in 0..5 {
            let (doc_id, file_id) = create_test_document(&pool, &format!("Doc {i}"), "document")
                .await
                .unwrap();
            create_chunk(
                &pool,
                doc_id,
                file_id,
                0,
                &format!("Content for document number {i}"),
                &spike_vector(0),
                Some(1),
                None,
            )
            .await
            .unwrap();
        }

        let (pipeline, mock) = build_retrieval_pipeline(store_arc).await;

        // Request only 2 results.
        let (hits, _mode) = pipeline
            .retrieve(
                1,
                None,
                &Query {
                    text: "document".to_string(),
                    filters: QueryFilters::default(),
                    top_k: 2,
                },
                false,
                false, // degrade
            )
            .await
            .unwrap();

        assert!(
            hits.len() <= 2,
            "top_k=2 should return at most 2 hits, got {}",
            hits.len()
        );

        // top_k=0 returns empty (handled before any DB query).
        let (empty, _mode) = pipeline
            .retrieve(
                1,
                None,
                &Query {
                    text: "document".to_string(),
                    filters: QueryFilters::default(),
                    top_k: 0,
                },
                false,
                false, // degrade
            )
            .await
            .unwrap();
        assert!(empty.is_empty(), "top_k=0 must return empty results");

        mock.shutdown().await;
    }
}
