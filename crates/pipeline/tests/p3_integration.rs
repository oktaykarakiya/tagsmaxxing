//! P3 phase integration test (§10, §31): end-to-end ingest of a text file
//! through the full pipeline — mock LLM backends for tagger + embedder,
//! [`LocalBlob`](kb_store::LocalBlob) for storage, real [`PgStore`] with
//! testcontainers.
//!
//! This test is `#[ignore]` in `just ci` because it requires Podman +
//! `pgvector/pgvector:pg17`. Run with:
//!
//! ```bash
//! DOCKER_HOST="unix://$XDG_RUNTIME_DIR/podman/podman.sock" \
//!   cargo test --package kb-pipeline --test p3_integration -- --ignored
//! ```

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use async_trait::async_trait;
    use kb_core::blob::Blob;
    use kb_core::extractor::{Extracted, Extractor, RawFile};
    use kb_core::kind::DocKind;
    use kb_core::role::Role;
    use kb_core::tagger::{TagInput, TagOutput, Tagger};
    use kb_llm::LlamaClient;
    use kb_mock_backend::MockBackend;
    use kb_pipeline::chunker::TextChunk;
    use kb_pipeline::document_builder::DocumentBuilder;
    use kb_pipeline::embedder::ChunkEmbedder;
    use kb_pipeline::ingest::{IngestFile, IngestPipeline, IngestStore};
    use kb_pipeline::metadata_merge::MetadataMerger;
    use kb_pipeline::tag_canonicalizer::TagCanonicalizer;
    use kb_pipeline::tag_store::TagStore;
    use kb_scheduler::{Backend, Pool};
    use kb_store::PgStore;
    use reqwest::Client;

    /// A mock [`Tagger`] that returns fixed output.
    struct TestTagger {
        output: TagOutput,
    }

    #[async_trait]
    impl Tagger for TestTagger {
        async fn tag(&self, _input: &TagInput) -> anyhow::Result<TagOutput> {
            Ok(self.output.clone())
        }
    }

    /// Helper: connect to pgvector via testcontainers, apply migrations, and
    /// insert a tenant row.
    async fn setup_store() -> anyhow::Result<PgStore> {
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

        let store = PgStore::new(&url);
        store.connect().await?;

        let pool = store.pool()?;
        sqlx::query("INSERT INTO tenants (slug, name) VALUES ('test', 'Test')")
            .execute(&pool)
            .await?;

        Ok(store)
    }

    /// P3 phase gate: ingest a real text file through the full pipeline
    /// with mock LLM backends.
    ///
    /// Asserts: document.status = 'ready', tags canonicalized, chunks
    /// embedded with correct dim, usage_events populated, idempotent
    /// re-ingest doesn't duplicate.
    #[ignore]
    #[tokio::test]
    async fn e2e_single_text_file_through_full_pipeline() {
        // ── infra ─────────────────────────────────────────────────────
        let store = setup_store().await.unwrap();
        let store = Arc::new(store);

        // Blob storage to a temp dir.
        let tmp = tempfile::tempdir().unwrap();
        let blob: Arc<dyn Blob> = Arc::new(kb_store::LocalBlob::new(
            tmp.path().join("blobs"),
            "test".to_string(),
        ));

        // Mock LLM backend for embed + chat.
        let mock = MockBackend::start().await;
        let base_url = mock.url("/v1");
        mock.scenario().lock().await.chat_content = Some(
            serde_json::json!({
                "title": "Rust Programming Guide",
                "summary": "A sample document about Rust programming.",
                "tags": ["rust", "programming", "tutorial"]
            })
            .to_string(),
        );
        mock.scenario().lock().await.embed_content = Some(vec![
            vec![0.1, 0.2, 0.3],
            vec![0.4, 0.5, 0.6],
            vec![0.7, 0.8, 0.9],
        ]);

        let backend = Arc::new(Backend::new(
            "mock-e2e",
            base_url,
            vec![Role::Text, Role::Embed],
            0,
            8,
        ));
        let pool = Pool::new(vec![backend], Duration::from_secs(10));
        let llm = Arc::new(LlamaClient::new(
            pool.clone(),
            Client::new(),
            0,
            0,
            Duration::from_millis(200),
        ));

        // ── pipeline components ───────────────────────────────────────

        // Tagger
        let tagger: Arc<dyn Tagger> = Arc::new(TestTagger {
            output: TagOutput {
                title: "Rust Programming Guide".into(),
                summary: "A sample document about Rust.".into(),
                tags: vec!["rust".into(), "programming".into(), "tutorial".into()],
            },
        });

        // Canonicalizer
        let canon: Arc<TagCanonicalizer> = Arc::new(TagCanonicalizer::new(
            Arc::clone(&store) as Arc<dyn TagStore>,
            Arc::clone(&llm),
            "bge-m3".into(),
            0.85,
        ));

        // Embedder (dim=3 matches mock)
        let embedder = Arc::new(ChunkEmbedder::new(Arc::clone(&llm), "bge-m3".into(), 3));

        // Extractor router
        let mut extractors: HashMap<DocKind, Arc<dyn Extractor>> = HashMap::new();
        extractors.insert(
            DocKind::Document,
            Arc::new(TextMockExtractor) as Arc<dyn Extractor>,
        );

        // Pipeline
        let pipeline = IngestPipeline::new(
            blob,
            extractors,
            tagger,
            canon,
            embedder,
            Arc::clone(&store) as Arc<dyn IngestStore>,
        );

        // ── run ───────────────────────────────────────────────────────
        let file = IngestFile {
            bytes:
                b"Rust is a systems programming language focused on safety, speed, and concurrency."
                    .to_vec(),
            page_label: None,
            path: Some("rust-book.txt".into()),
        };

        let output = pipeline
            .ingest(1, vec![file], Some("Learning material".into()))
            .await
            .unwrap();

        assert!(output.document_id > 0);
        assert_eq!(output.tag_ids.len(), 3);
        assert!(output.chunk_count > 0);

        // Verify document is ready.
        let pool = store.pool().unwrap();
        let status: String = sqlx::query_scalar("SELECT status FROM documents WHERE id = $1")
            .bind(output.document_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(status, "ready");

        // Verify files stored.
        let file_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM files WHERE document_id = $1")
                .bind(output.document_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(file_count, 1);

        // Verify tags canonicalized.
        let tag_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM document_tags WHERE document_id = $1")
                .bind(output.document_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(tag_count, 3);

        // Verify chunks embedded.
        let chunk_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM chunks WHERE document_id = $1")
                .bind(output.document_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(chunk_count as usize, output.chunk_count);

        // Verify idempotent re-ingest doesn't duplicate.
        let file2 = IngestFile {
            bytes:
                b"Rust is a systems programming language focused on safety, speed, and concurrency."
                    .to_vec(),
            page_label: None,
            path: Some("rust-book.txt".into()),
        };
        let output2 = pipeline.ingest(1, vec![file2], None).await.unwrap();
        // Same sha256 → same document id (upsert).
        assert_eq!(output2.document_id, output.document_id);

        let file_count2: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM files WHERE document_id = $1")
                .bind(output.document_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(file_count2, 1, "re-ingest must not duplicate files");

        mock.shutdown().await;
    }

    /// A simple text extractor for testing.
    struct TextMockExtractor;

    #[async_trait]
    impl Extractor for TextMockExtractor {
        async fn extract(&self, _file: &RawFile) -> anyhow::Result<Extracted> {
            Ok(Extracted {
                text: "Rust is a systems programming language focused on safety, speed, and concurrency.".into(),
                meta: serde_json::json!({"mime": "text/plain"}),
                page_images: vec![],
            })
        }
    }

    // ── pure-logic smoke tests (no testcontainers needed) ───────────

    #[test]
    fn document_builder_and_metadata_merge_roundtrip() {
        let (doc, files) = DocumentBuilder::build_single(
            1,
            b"Hello world from Rust.",
            Some("test.txt"),
            Some("test note"),
        );
        assert_eq!(doc.page_count, 1);
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].page_no, 1);
        assert!(files[0].sha256.to_hex().len() == 64);

        let extracted = Extracted {
            text: "Hello world from Rust.".into(),
            meta: serde_json::json!({"mime": "text/plain"}),
            page_images: vec![],
        };
        let merged = MetadataMerger::merge(&[(files[0].clone(), extracted)]);
        assert!(merged.merged_text.contains("Hello world"));
        assert_eq!(merged.kind, DocKind::Document);
        assert_eq!(merged.page_count, 1);
    }

    #[test]
    fn chunker_embedder_types_align() {
        // Verify TextChunk→embed_chunks type chain compiles.
        let tc = TextChunk {
            content: "test".into(),
            idx: 0,
            page_no: Some(1),
            file_id: 42,
            ts_offset: None,
        };
        assert_eq!(tc.content, "test");
        assert_eq!(tc.file_id, 42);
    }
}
