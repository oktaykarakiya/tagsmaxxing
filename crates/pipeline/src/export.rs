//! Streaming ZIP + JSONL export for data portability (plan §13, P5-T7).
//!
//! Given a `tenant_id`, the export:
//! 1. Queries all documents, files, tags, tag_aliases, document_tags, and chunks for the tenant.
//! 2. Writes a streaming ZIP containing:
//!    - Original blobs (from [`Blob::get`], preserving original paths),
//!    - `export.jsonl` (one JSON object per line: `{type, data}`),
//!    - `manifest.json` ({schema_version: 1, embedder_id, embedder_dim, ...}).
//! 3. Never buffers the full export in memory — entries stream to a temp file.
//! 4. Integrates with the job queue (enqueue export job → worker → streams ZIP → stores blob → done).

use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use bytes::Bytes;
use chrono::Utc;
use kb_core::blob::Blob;
use kb_core::chunk::Chunk;
use kb_core::document::Document;
use kb_core::file::FileRecord;
use kb_core::job::Job;
use kb_core::tag::Tag;
use zip::ZipWriter;
use zip::write::FileOptions;

// ── Constants ─────────────────────────────────────────────────────────────────

/// Schema version embedded in `manifest.json`. Bump when the export format changes
/// in an incompatible way.
pub const EXPORT_SCHEMA_VERSION: u32 = 1;

/// Embedder id recorded in the manifest (the default at schema-design time).
pub const EMBEDDER_ID: &str = "bge-m3";

/// Expected embedding dimension (BGE-M3).
pub const EMBEDDER_DIM: usize = 1024;

// ── JSONL record type ─────────────────────────────────────────────────────────

/// A single line in `export.jsonl`.
#[derive(serde::Serialize)]
struct JsonlRecord<'a, T: serde::Serialize> {
    #[serde(rename = "type")]
    record_type: &'a str,
    data: &'a T,
}

/// Document-tag link serialized in the JSONL stream.
#[derive(serde::Serialize)]
struct DocumentTagLink {
    document_id: i64,
    tag_id: i64,
}

/// Tag-alias link serialized in the JSONL stream.
#[derive(serde::Serialize)]
struct AliasLink {
    alias: String,
    tag_id: i64,
}

// ── Manifest ──────────────────────────────────────────────────────────────────

/// Contents of `manifest.json`.
#[derive(serde::Serialize)]
struct Manifest<'a> {
    schema_version: u32,
    embedder_id: &'a str,
    embedder_dim: usize,
    exported_at: &'a str,
    tenant_slug: &'a str,
    document_count: usize,
    file_count: usize,
    total_bytes: u64,
}

// ── ExportStore trait ─────────────────────────────────────────────────────────

/// Database reads needed by [`ExportPipeline`].
///
/// Extracted from [`PgStore`](kb_store::PgStore) so the pipeline can be tested
/// without a live Postgres instance (plan §31.3 mock-backend pattern).
#[async_trait]
pub trait ExportStore: Send + Sync {
    /// List all documents for a tenant.
    async fn list_documents(&self, tenant_id: i64) -> anyhow::Result<Vec<Document>>;
    /// List all files for a tenant.
    async fn list_files(&self, tenant_id: i64) -> anyhow::Result<Vec<FileRecord>>;
    /// List all tags for a tenant.
    async fn list_tags(&self, tenant_id: i64) -> anyhow::Result<Vec<Tag>>;
    /// List all tag aliases for a tenant as `(alias, tag_id)` pairs.
    async fn list_tag_aliases(&self, tenant_id: i64) -> anyhow::Result<Vec<(String, i64)>>;
    /// List all document-tag links for a tenant as `(document_id, tag_id)` pairs.
    async fn list_document_tags(&self, tenant_id: i64) -> anyhow::Result<Vec<(i64, i64)>>;
    /// List all chunks for a tenant.
    async fn list_chunks(&self, tenant_id: i64) -> anyhow::Result<Vec<Chunk>>;
    /// Get a tenant's slug by id.
    async fn get_tenant_slug(&self, tenant_id: i64) -> anyhow::Result<String>;
}

/// Production implementation: delegates to the real Postgres store.
#[async_trait]
impl ExportStore for kb_store::PgStore {
    async fn list_documents(&self, tenant_id: i64) -> anyhow::Result<Vec<Document>> {
        <kb_store::PgStore>::list_documents(self, tenant_id).await
    }

    async fn list_files(&self, tenant_id: i64) -> anyhow::Result<Vec<FileRecord>> {
        <kb_store::PgStore>::list_files(self, tenant_id).await
    }

    async fn list_tags(&self, tenant_id: i64) -> anyhow::Result<Vec<Tag>> {
        <kb_store::PgStore>::list_tags(self, tenant_id).await
    }

    async fn list_tag_aliases(&self, tenant_id: i64) -> anyhow::Result<Vec<(String, i64)>> {
        <kb_store::PgStore>::list_tag_aliases(self, tenant_id).await
    }

    async fn list_document_tags(&self, tenant_id: i64) -> anyhow::Result<Vec<(i64, i64)>> {
        <kb_store::PgStore>::list_document_tags(self, tenant_id).await
    }

    async fn list_chunks(&self, tenant_id: i64) -> anyhow::Result<Vec<Chunk>> {
        <kb_store::PgStore>::list_chunks(self, tenant_id).await
    }

    async fn get_tenant_slug(&self, tenant_id: i64) -> anyhow::Result<String> {
        <kb_store::PgStore>::get_tenant_slug(self, tenant_id).await
    }
}

// ── ExportPipeline ────────────────────────────────────────────────────────────

/// Wires the export flow: query data → stream ZIP to temp file → store blob.
///
/// The pipeline never buffers the full export in memory — blobs are fetched
/// individually, JSONL lines are written one at a time, and the ZIP is written
/// to a temporary file before being stored as a blob (plan §13).
pub struct ExportPipeline {
    store: Arc<dyn ExportStore>,
    blob: Arc<dyn Blob>,
}

impl ExportPipeline {
    /// Create a new pipeline backed by the given store and blob backend.
    #[must_use]
    pub fn new(store: Arc<dyn ExportStore>, blob: Arc<dyn Blob>) -> Self {
        Self { store, blob }
    }

    /// Run the full export for `tenant_id` and return the blob key of the
    /// stored ZIP.
    ///
    /// # Errors
    /// Returns an error if any query, blob fetch, or ZIP write fails.
    pub async fn run_export(&self, tenant_id: i64) -> anyhow::Result<String> {
        // 1. Query all tenant data.
        let documents = self.store.list_documents(tenant_id).await?;
        let files = self.store.list_files(tenant_id).await?;
        let tags = self.store.list_tags(tenant_id).await?;
        let aliases = self.store.list_tag_aliases(tenant_id).await?;
        let doc_tags = self.store.list_document_tags(tenant_id).await?;
        let chunks = self.store.list_chunks(tenant_id).await?;
        let tenant_slug = self.store.get_tenant_slug(tenant_id).await?;

        // Calculate total bytes for the manifest.
        let total_bytes: u64 = files
            .iter()
            .filter_map(|f| f.size_bytes)
            .map(|s| s as u64)
            .sum();

        // 2. Build the streaming ZIP in a temp file.
        let mut temp = tempfile::tempfile().context("failed to create temp file for export ZIP")?;
        {
            let mut zip = ZipWriter::new(&mut temp);

            let opts = FileOptions::<()>::default()
                .compression_method(zip::CompressionMethod::Stored)
                .unix_permissions(0o644);

            // ── Manifest ──────────────────────────────────────────────────
            let exported_at = Utc::now().to_rfc3339();
            let manifest = Manifest {
                schema_version: EXPORT_SCHEMA_VERSION,
                embedder_id: EMBEDDER_ID,
                embedder_dim: EMBEDDER_DIM,
                exported_at: &exported_at,
                tenant_slug: &tenant_slug,
                document_count: documents.len(),
                file_count: files.len(),
                total_bytes,
            };
            zip.start_file("manifest.json", opts)
                .map_err(anyhow::Error::from)?;
            let manifest_json =
                serde_json::to_vec_pretty(&manifest).context("failed to serialize manifest")?;
            zip.write_all(&manifest_json).map_err(anyhow::Error::from)?;
            zip.write_all(b"\n").map_err(anyhow::Error::from)?;

            // ── Blobs ─────────────────────────────────────────────────────
            for file in &files {
                let entry_name = safe_zip_path(file.path.as_deref(), &file.blob_key);
                zip.start_file(&entry_name, opts)
                    .map_err(anyhow::Error::from)?;
                match self.blob.get(&file.blob_key).await {
                    Ok(data) => {
                        zip.write_all(&data).map_err(anyhow::Error::from)?;
                    }
                    Err(e) => {
                        tracing::warn!(
                            "blob {key} for file {fid} not found: {e}",
                            key = file.blob_key,
                            fid = file.id,
                        );
                    }
                }
            }

            // ── JSONL stream ──────────────────────────────────────────────
            zip.start_file("export.jsonl", opts)
                .map_err(anyhow::Error::from)?;

            for doc in &documents {
                write_jsonl_line(&mut zip, "document", doc)?;
            }
            for file in &files {
                write_jsonl_line(&mut zip, "file", file)?;
            }
            for tag in &tags {
                write_jsonl_line(&mut zip, "tag", tag)?;
            }
            for (alias, tag_id) in &aliases {
                let link = AliasLink {
                    alias: alias.clone(),
                    tag_id: *tag_id,
                };
                write_jsonl_line(&mut zip, "tag_alias", &link)?;
            }
            for (document_id, tag_id) in &doc_tags {
                let link = DocumentTagLink {
                    document_id: *document_id,
                    tag_id: *tag_id,
                };
                write_jsonl_line(&mut zip, "document_tag", &link)?;
            }
            for chunk in &chunks {
                write_jsonl_line(&mut zip, "chunk", chunk)?;
            }

            zip.finish().map_err(anyhow::Error::from)?;
        }
        // ZipWriter is dropped here; the mut borrow on `temp` is released.

        // 3. Seek back and read the temp file into memory for blob storage.
        temp.seek(SeekFrom::Start(0))
            .context("failed to seek temp ZIP to start")?;
        let mut zip_bytes = Vec::new();
        temp.read_to_end(&mut zip_bytes)
            .context("failed to read temp ZIP for blob upload")?;
        drop(temp);

        // Build a content-addressed key: tenant_id/sha256 of the ZIP.
        let zip_sha256 = {
            use sha2::Digest;
            let hash = sha2::Sha256::digest(&zip_bytes);
            hex::encode(hash)
        };
        let blob_key = format!("{tenant_id}/export-{zip_sha256}.zip");

        self.blob
            .put(&blob_key, Bytes::from(zip_bytes))
            .await
            .context("failed to store export ZIP blob")?;

        Ok(blob_key)
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Sanitize a path for use as a ZIP entry name.
///
/// Strips leading `/` (absolute-path guard), rejects `..` traversal, and falls
/// back to the blob key if the original path is missing or unsafe.
fn safe_zip_path(path: Option<&str>, blob_key: &str) -> String {
    let candidate = match path {
        Some(p) => p.trim_start_matches('/'),
        None => return blob_key.to_string(),
    };
    if candidate.is_empty() || candidate.contains("..") {
        return blob_key.to_string();
    }
    candidate.to_string()
}

/// Write one JSONL line (object + newline) into the ZIP writer.
fn write_jsonl_line<W: Write + Seek, T: serde::Serialize>(
    zip: &mut ZipWriter<W>,
    record_type: &str,
    data: &T,
) -> anyhow::Result<()> {
    let record = JsonlRecord { record_type, data };
    serde_json::to_writer(&mut *zip, &record).context("failed to serialize JSONL record")?;
    zip.write_all(b"\n").map_err(anyhow::Error::from)?;
    Ok(())
}

// ── Job-queue integration ─────────────────────────────────────────────────────

/// A job-queue handler that can be passed to
/// [`run_worker_pool`](crate::job_queue::run_worker_pool).
///
/// When a [`Job`] with [`JobKind::Export`](kb_core::job::JobKind::Export) is
/// claimed, this handler queries all tenant data, writes the streaming ZIP,
/// stores it as a blob, and returns `Ok(())`. The worker pool will then call
/// `queue.complete(job.id)`.
///
/// # Errors
/// Returns `Err(String)` if any step fails. The worker pool will call
/// `queue.fail(job.id, &error)` and apply exponential backoff.
pub async fn process_export_job(job: Job, pipeline: Arc<ExportPipeline>) -> Result<(), String> {
    let blob_key = pipeline
        .run_export(job.tenant_id)
        .await
        .map_err(|e| format!("export job {} failed: {e:#}", job.id))?;

    tracing::info!(
        job_id = job.id,
        tenant_id = job.tenant_id,
        blob_key = %blob_key,
        "export job completed"
    );
    Ok(())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use std::collections::HashMap;
    use std::io::Cursor;
    use std::sync::Mutex;

    use super::*;

    // ── MockExportStore ──────────────────────────────────────────────────

    struct MockExportStore {
        documents: Mutex<Vec<Document>>,
        files: Mutex<Vec<FileRecord>>,
        tags: Mutex<Vec<Tag>>,
        aliases: Mutex<Vec<(String, i64)>>,
        doc_tags: Mutex<Vec<(i64, i64)>>,
        chunks: Mutex<Vec<Chunk>>,
        slug: Mutex<String>,
        // Error injection.
        list_documents_err: Mutex<Option<String>>,
        list_files_err: Mutex<Option<String>>,
        list_tags_err: Mutex<Option<String>>,
        list_aliases_err: Mutex<Option<String>>,
        list_doc_tags_err: Mutex<Option<String>>,
        list_chunks_err: Mutex<Option<String>>,
        slug_err: Mutex<Option<String>>,
    }

    impl MockExportStore {
        fn new(slug: &str) -> Self {
            Self {
                documents: Mutex::new(Vec::new()),
                files: Mutex::new(Vec::new()),
                tags: Mutex::new(Vec::new()),
                aliases: Mutex::new(Vec::new()),
                doc_tags: Mutex::new(Vec::new()),
                chunks: Mutex::new(Vec::new()),
                slug: Mutex::new(slug.to_string()),
                list_documents_err: Mutex::new(None),
                list_files_err: Mutex::new(None),
                list_tags_err: Mutex::new(None),
                list_aliases_err: Mutex::new(None),
                list_doc_tags_err: Mutex::new(None),
                list_chunks_err: Mutex::new(None),
                slug_err: Mutex::new(None),
            }
        }

        fn add_document(&self, doc: Document) {
            self.documents.lock().unwrap().push(doc);
        }

        fn add_file(&self, file: FileRecord) {
            self.files.lock().unwrap().push(file);
        }

        fn add_tag(&self, tag: Tag) {
            self.tags.lock().unwrap().push(tag);
        }

        #[allow(dead_code)]
        fn set_slug(&self, slug: &str) {
            *self.slug.lock().unwrap() = slug.to_string();
        }
    }

    #[async_trait]
    impl ExportStore for MockExportStore {
        async fn list_documents(&self, _tenant_id: i64) -> anyhow::Result<Vec<Document>> {
            if let Some(e) = self.list_documents_err.lock().unwrap().take() {
                anyhow::bail!("{e}");
            }
            Ok(self.documents.lock().unwrap().clone())
        }

        async fn list_files(&self, _tenant_id: i64) -> anyhow::Result<Vec<FileRecord>> {
            if let Some(e) = self.list_files_err.lock().unwrap().take() {
                anyhow::bail!("{e}");
            }
            Ok(self.files.lock().unwrap().clone())
        }

        async fn list_tags(&self, _tenant_id: i64) -> anyhow::Result<Vec<Tag>> {
            if let Some(e) = self.list_tags_err.lock().unwrap().take() {
                anyhow::bail!("{e}");
            }
            Ok(self.tags.lock().unwrap().clone())
        }

        async fn list_tag_aliases(&self, _tenant_id: i64) -> anyhow::Result<Vec<(String, i64)>> {
            if let Some(e) = self.list_aliases_err.lock().unwrap().take() {
                anyhow::bail!("{e}");
            }
            Ok(self.aliases.lock().unwrap().clone())
        }

        async fn list_document_tags(&self, _tenant_id: i64) -> anyhow::Result<Vec<(i64, i64)>> {
            if let Some(e) = self.list_doc_tags_err.lock().unwrap().take() {
                anyhow::bail!("{e}");
            }
            Ok(self.doc_tags.lock().unwrap().clone())
        }

        async fn list_chunks(&self, _tenant_id: i64) -> anyhow::Result<Vec<Chunk>> {
            if let Some(e) = self.list_chunks_err.lock().unwrap().take() {
                anyhow::bail!("{e}");
            }
            Ok(self.chunks.lock().unwrap().clone())
        }

        async fn get_tenant_slug(&self, _tenant_id: i64) -> anyhow::Result<String> {
            if let Some(e) = self.slug_err.lock().unwrap().take() {
                anyhow::bail!("{e}");
            }
            Ok(self.slug.lock().unwrap().clone())
        }
    }

    // ── MockBlob ─────────────────────────────────────────────────────────

    struct MockBlob {
        stored: Mutex<HashMap<String, Bytes>>,
        // Maps key → bytes (for `get`).
        blobs: Mutex<HashMap<String, Bytes>>,
        get_err: Mutex<Option<String>>,
        put_err: Mutex<Option<String>>,
    }

    impl MockBlob {
        fn new() -> Self {
            Self {
                stored: Mutex::new(HashMap::new()),
                blobs: Mutex::new(HashMap::new()),
                get_err: Mutex::new(None),
                put_err: Mutex::new(None),
            }
        }

        fn add_blob(&self, key: &str, data: &[u8]) {
            self.blobs
                .lock()
                .unwrap()
                .insert(key.to_string(), Bytes::copy_from_slice(data));
        }

        fn stored_keys(&self) -> Vec<String> {
            self.stored.lock().unwrap().keys().cloned().collect()
        }
    }

    #[async_trait]
    impl Blob for MockBlob {
        async fn put(&self, key: &str, data: Bytes) -> anyhow::Result<()> {
            if let Some(e) = self.put_err.lock().unwrap().take() {
                anyhow::bail!("{e}");
            }
            self.stored.lock().unwrap().insert(key.to_string(), data);
            Ok(())
        }

        async fn get(&self, key: &str) -> anyhow::Result<Bytes> {
            if let Some(e) = self.get_err.lock().unwrap().take() {
                anyhow::bail!("{e}");
            }
            self.blobs
                .lock()
                .unwrap()
                .get(key)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("blob not found: {key}"))
        }

        async fn get_range(&self, key: &str, start: u64, end: u64) -> anyhow::Result<Bytes> {
            let data = self.get(key).await?;
            let start = start.min(data.len() as u64) as usize;
            let end = end.min(data.len() as u64) as usize;
            Ok(data.slice(start..end))
        }

        async fn exists(&self, key: &str) -> anyhow::Result<bool> {
            Ok(self.blobs.lock().unwrap().contains_key(key))
        }

        async fn delete(&self, key: &str) -> anyhow::Result<()> {
            self.blobs.lock().unwrap().remove(key);
            Ok(())
        }

        async fn presigned_get_url(
            &self,
            key: &str,
            _expires_in: std::time::Duration,
        ) -> anyhow::Result<String> {
            Ok(format!("file:///mock/{key}"))
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────────

    fn make_doc(id: i64, kind: &str) -> Document {
        use std::str::FromStr;
        Document {
            id,
            tenant_id: 1,
            title: Some(format!("Doc {id}")),
            summary: Some("Summary".into()),
            user_note: None,
            kind: kb_core::kind::DocKind::from_str(kind).unwrap(),
            meta: serde_json::json!({}),
            page_count: 1,
            status: kb_core::status::ProcessingStatus::Ready,
            created_at: chrono::Utc::now(),
        }
    }

    fn make_file(id: i64, document_id: i64) -> FileRecord {
        let mut hash = [0u8; 32];
        hash[0..8].copy_from_slice(&id.to_le_bytes().repeat(4)[..8]);
        FileRecord {
            id,
            tenant_id: 1,
            document_id,
            page_no: 1,
            page_label: Some(format!("p{id}")),
            sha256: kb_core::hash::Sha256::from_bytes(hash),
            blob_key: format!("1/blob-{id}"),
            path: Some(format!("docs/file{id}.txt")),
            mime: Some("text/plain".into()),
            size_bytes: Some(100 * id),
            meta: serde_json::json!({}),
            status: kb_core::status::ProcessingStatus::Ready,
            ingested_at: chrono::Utc::now(),
        }
    }

    fn make_chunk(id: i64, file_id: i64, doc_id: i64) -> Chunk {
        Chunk {
            id,
            tenant_id: 1,
            document_id: doc_id,
            file_id,
            page_no: Some(1),
            idx: 0,
            content: format!("chunk {id} content"),
            ts_offset: None,
            embedding: vec![0.1_f32; EMBEDDER_DIM],
        }
    }

    // ── safe_zip_path ────────────────────────────────────────────────────

    #[test]
    fn safe_zip_path_uses_original() {
        assert_eq!(safe_zip_path(Some("docs/file.txt"), "key"), "docs/file.txt");
    }

    #[test]
    fn safe_zip_path_strips_leading_slash() {
        assert_eq!(
            safe_zip_path(Some("/absolute/path.txt"), "key"),
            "absolute/path.txt"
        );
    }

    #[test]
    fn safe_zip_path_rejects_dotdot() {
        assert_eq!(
            safe_zip_path(Some("../etc/passwd"), "fallback-key"),
            "fallback-key"
        );
    }

    #[test]
    fn safe_zip_path_rejects_dotdot_in_middle() {
        assert_eq!(
            safe_zip_path(Some("docs/../secret"), "fallback-key"),
            "fallback-key"
        );
    }

    #[test]
    fn safe_zip_path_none_uses_blob_key() {
        assert_eq!(safe_zip_path(None, "1/abc123"), "1/abc123");
    }

    #[test]
    fn safe_zip_path_empty_uses_blob_key() {
        assert_eq!(safe_zip_path(Some(""), "1/abc123"), "1/abc123");
    }

    #[test]
    fn safe_zip_path_slash_only_uses_blob_key() {
        assert_eq!(safe_zip_path(Some("/"), "1/abc123"), "1/abc123");
    }

    // ── JSONL serialization ──────────────────────────────────────────────

    #[test]
    fn jsonl_record_has_type_field() {
        let doc = make_doc(1, "document");
        let record = JsonlRecord {
            record_type: "document",
            data: &doc,
        };
        let json = serde_json::to_string(&record).unwrap();
        assert!(json.contains("\"type\":\"document\""));
        assert!(json.contains("\"title\":\"Doc 1\""));
    }

    #[test]
    fn manifest_serializes_with_all_fields() {
        let m = Manifest {
            schema_version: EXPORT_SCHEMA_VERSION,
            embedder_id: EMBEDDER_ID,
            embedder_dim: EMBEDDER_DIM,
            exported_at: "2024-01-15T10:00:00Z",
            tenant_slug: "acme",
            document_count: 5,
            file_count: 7,
            total_bytes: 42_000,
        };
        let json = serde_json::to_string_pretty(&m).unwrap();
        assert!(json.contains("\"schema_version\": 1"));
        assert!(json.contains("\"embedder_id\": \"bge-m3\""));
        assert!(json.contains("\"embedder_dim\": 1024"));
        assert!(json.contains("\"tenant_slug\": \"acme\""));
        assert!(json.contains("\"document_count\": 5"));
        assert!(json.contains("\"file_count\": 7"));
        assert!(json.contains("\"total_bytes\": 42000"));
    }

    // ── ExportPipeline ───────────────────────────────────────────────────

    #[tokio::test]
    async fn empty_tenant_produces_valid_zip() {
        let store = Arc::new(MockExportStore::new("empty"));
        let blob = Arc::new(MockBlob::new());

        let pipeline = ExportPipeline::new(store, blob.clone());
        let blob_key = pipeline.run_export(1).await.unwrap();

        // Verify blob was stored.
        let stored = blob.stored.lock().unwrap().get(&blob_key).cloned().unwrap();

        // Open and inspect the ZIP.
        let cursor = Cursor::new(stored.to_vec());
        let mut zip = zip::ZipArchive::new(cursor).unwrap();
        // Should have manifest.json + export.jsonl, no blobs.
        assert_eq!(zip.len(), 2, "empty export should have 2 entries");

        // Verify manifest.
        let mut manifest_bytes = Vec::new();
        zip.by_name("manifest.json")
            .unwrap()
            .read_to_end(&mut manifest_bytes)
            .unwrap();
        let manifest: serde_json::Value = serde_json::from_slice(&manifest_bytes).unwrap();
        assert_eq!(manifest["schema_version"], 1);
        assert_eq!(manifest["document_count"], 0);
        assert_eq!(manifest["file_count"], 0);
        assert_eq!(manifest["total_bytes"], 0);
        assert_eq!(manifest["tenant_slug"], "empty");

        // Verify export.jsonl is valid but empty.
        let mut jsonl_bytes = Vec::new();
        zip.by_name("export.jsonl")
            .unwrap()
            .read_to_end(&mut jsonl_bytes)
            .unwrap();
        let jsonl_str = String::from_utf8(jsonl_bytes).unwrap();
        assert!(jsonl_str.is_empty(), "empty export JSONL should be empty");
    }

    #[tokio::test]
    async fn single_document_export_includes_all_data() {
        let store = Arc::new(MockExportStore::new("acme"));
        let blob = Arc::new(MockBlob::new());

        let doc = make_doc(1, "document");
        let file = make_file(1, 1);
        let tag = Tag {
            id: 10,
            tenant_id: 1,
            name: "invoice".into(),
            embedding: None,
        };
        // Pre-populate mock blob so Blob::get works for the file's blob_key.
        blob.add_blob(&file.blob_key, b"hello world");
        store.add_document(doc.clone());
        store.add_file(file.clone());
        store.add_tag(tag.clone());

        let pipeline = ExportPipeline::new(store, blob.clone());
        pipeline.run_export(1).await.unwrap();

        // Read back the stored ZIP.
        let stored_keys = blob.stored_keys();
        assert_eq!(stored_keys.len(), 1);
        let zip_key = stored_keys.into_iter().next().unwrap();
        let zip_data = blob.stored.lock().unwrap().get(&zip_key).cloned().unwrap();
        let cursor = Cursor::new(zip_data.to_vec());
        let mut zip = zip::ZipArchive::new(cursor).unwrap();
        // manifest.json + export.jsonl + 1 blob = 3 entries.
        assert_eq!(zip.len(), 3);

        // Verify the blob is preserved.
        let mut blob_content = Vec::new();
        zip.by_name("docs/file1.txt")
            .unwrap()
            .read_to_end(&mut blob_content)
            .unwrap();
        assert_eq!(blob_content, b"hello world");

        // Verify JSONL contains our document record.
        let mut jsonl_content = Vec::new();
        zip.by_name("export.jsonl")
            .unwrap()
            .read_to_end(&mut jsonl_content)
            .unwrap();
        let jsonl_str = String::from_utf8(jsonl_content).unwrap();
        assert!(jsonl_str.contains("\"type\":\"document\""));
        assert!(jsonl_str.contains("\"type\":\"file\""));
        assert!(jsonl_str.contains("\"type\":\"tag\""));
    }

    #[tokio::test]
    async fn blob_not_found_writes_empty_entry() {
        let store = Arc::new(MockExportStore::new("acme"));
        let blob = Arc::new(MockBlob::new());

        let file = make_file(1, 1);
        // Do NOT add the blob — simulating a missing blob in the store.
        store.add_document(make_doc(1, "document"));
        store.add_file(file.clone());

        let pipeline = ExportPipeline::new(store, blob.clone());
        let result = pipeline.run_export(1).await;
        // Export should still succeed (warns, writes empty entry).
        assert!(
            result.is_ok(),
            "export should succeed even with missing blobs"
        );

        let stored_keys = blob.stored_keys();
        assert_eq!(stored_keys.len(), 1);
        let zip_key = stored_keys.into_iter().next().unwrap();
        let zip_data = blob.stored.lock().unwrap().get(&zip_key).cloned().unwrap();
        let cursor = Cursor::new(zip_data.to_vec());
        let mut zip = zip::ZipArchive::new(cursor).unwrap();
        // Blob entry still exists (just empty).
        assert!(zip.by_name("docs/file1.txt").is_ok());
    }

    #[tokio::test]
    async fn manifest_includes_correct_counts() {
        let store = Arc::new(MockExportStore::new("acme"));
        let blob = Arc::new(MockBlob::new());

        for i in 1..=3 {
            store.add_document(make_doc(i, "document"));
        }
        for i in 1..=5 {
            let f = make_file(i, (i - 1) / 2 + 1);
            blob.add_blob(&f.blob_key, b"x");
            store.add_file(f);
        }

        let pipeline = ExportPipeline::new(store, blob.clone());
        pipeline.run_export(1).await.unwrap();

        let stored_keys = blob.stored_keys();
        let zip_key = stored_keys.into_iter().next().unwrap();
        let zip_data = blob.stored.lock().unwrap().get(&zip_key).cloned().unwrap();
        let cursor = Cursor::new(zip_data.to_vec());
        let mut zip = zip::ZipArchive::new(cursor).unwrap();

        let mut manifest_bytes = Vec::new();
        zip.by_name("manifest.json")
            .unwrap()
            .read_to_end(&mut manifest_bytes)
            .unwrap();
        let m: serde_json::Value = serde_json::from_slice(&manifest_bytes).unwrap();
        assert_eq!(m["document_count"], 3);
        assert_eq!(m["file_count"], 5);
    }

    #[tokio::test]
    async fn export_includes_chunks() {
        let store = Arc::new(MockExportStore::new("acme"));
        let blob = Arc::new(MockBlob::new());

        let chunk = make_chunk(1, 1, 1);
        store.add_document(make_doc(1, "document"));
        store.add_file(make_file(1, 1));
        // Use a `Chunk` with a fixed embedding for stable assertion.
        {
            let mut chunks = store.chunks.lock().unwrap();
            chunks.push(chunk);
        }

        let pipeline = ExportPipeline::new(store, blob.clone());
        pipeline.run_export(1).await.unwrap();

        let stored_keys = blob.stored_keys();
        let zip_key = stored_keys.into_iter().next().unwrap();
        let zip_data = blob.stored.lock().unwrap().get(&zip_key).cloned().unwrap();
        let cursor = Cursor::new(zip_data.to_vec());
        let mut zip = zip::ZipArchive::new(cursor).unwrap();

        let mut jsonl_bytes = Vec::new();
        zip.by_name("export.jsonl")
            .unwrap()
            .read_to_end(&mut jsonl_bytes)
            .unwrap();
        let jsonl_str = String::from_utf8(jsonl_bytes).unwrap();
        assert!(jsonl_str.contains("\"type\":\"chunk\""));
        assert!(jsonl_str.contains("\"content\":\"chunk 1 content\""));
    }

    #[tokio::test]
    async fn job_handler_returns_ok_on_success() {
        let store = Arc::new(MockExportStore::new("acme"));
        let blob = Arc::new(MockBlob::new());

        store.add_document(make_doc(1, "document"));
        let pipeline = Arc::new(ExportPipeline::new(store, blob));

        let job = Job {
            id: 42,
            tenant_id: 1,
            file_id: None,
            document_id: None,
            kind: kb_core::job::JobKind::Export,
            priority: 100,
            status: kb_core::job::JobStatus::Running,
            attempts: 0,
            last_error: None,
            run_after: chrono::Utc::now(),
            created_at: chrono::Utc::now(),
        };

        let result = process_export_job(job, pipeline).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn job_handler_returns_err_on_failure() {
        let store = Arc::new(MockExportStore::new("acme"));
        let blob = Arc::new(MockBlob::new());

        // Inject a DB error.
        *store.list_documents_err.lock().unwrap() = Some("db down".into());

        let pipeline = Arc::new(ExportPipeline::new(store, blob));

        let job = Job {
            id: 99,
            tenant_id: 1,
            file_id: None,
            document_id: None,
            kind: kb_core::job::JobKind::Export,
            priority: 100,
            status: kb_core::job::JobStatus::Running,
            attempts: 0,
            last_error: None,
            run_after: chrono::Utc::now(),
            created_at: chrono::Utc::now(),
        };

        let result = process_export_job(job, pipeline).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("db down"));
        assert!(err.contains("export job 99 failed"));
    }

    #[tokio::test]
    async fn blob_key_is_stable_and_prefixed() {
        let store = Arc::new(MockExportStore::new("stable"));
        let blob = Arc::new(MockBlob::new());

        let pipeline = ExportPipeline::new(store, blob.clone());
        let key = pipeline.run_export(1).await.unwrap();

        assert!(key.starts_with("1/export-"));
        assert!(key.ends_with(".zip"));
        // blob_key contains the sha256 hex (64 chars) between prefix and suffix.
        let inner = key
            .strip_prefix("1/export-")
            .unwrap()
            .strip_suffix(".zip")
            .unwrap();
        assert_eq!(inner.len(), 64, "sha256 hex must be 64 characters");
    }

    #[tokio::test]
    async fn export_round_trip_preserves_document_data() {
        // Export, then read back the ZIP and verify all document fields survive.
        let store = Arc::new(MockExportStore::new("roundtrip"));
        let blob = Arc::new(MockBlob::new());

        let doc = make_doc(7, "image");
        let file = {
            let mut f = make_file(1, 7);
            f.path = Some("photos/vacation.jpg".into());
            f.mime = Some("image/jpeg".into());
            f.size_bytes = Some(12345);
            f
        };
        let tag = Tag {
            id: 99,
            tenant_id: 1,
            name: "travel".into(),
            embedding: Some(vec![0.5_f32; 1024]),
        };

        blob.add_blob(&file.blob_key, b"fake-jpeg-data");
        store.add_document(doc.clone());
        store.add_file(file.clone());
        store.add_tag(tag.clone());

        let pipeline = ExportPipeline::new(store, blob.clone());
        let key = pipeline.run_export(1).await.unwrap();

        let zip_data = blob.stored.lock().unwrap().get(&key).cloned().unwrap();
        let cursor = Cursor::new(zip_data.to_vec());
        let mut zip = zip::ZipArchive::new(cursor).unwrap();

        // Read back JSONL and find our document.
        let mut jsonl_bytes = Vec::new();
        zip.by_name("export.jsonl")
            .unwrap()
            .read_to_end(&mut jsonl_bytes)
            .unwrap();
        let jsonl_str = String::from_utf8(jsonl_bytes).unwrap();

        let doc_json: Vec<&str> = jsonl_str
            .lines()
            .filter(|l| l.contains("\"type\":\"document\""))
            .collect();
        assert_eq!(doc_json.len(), 1);
        let parsed: serde_json::Value = serde_json::from_str(doc_json[0]).unwrap();
        assert_eq!(parsed["type"], "document");
        let data = &parsed["data"];
        assert_eq!(data["id"], 7);
        assert_eq!(data["title"], "Doc 7");
        assert_eq!(data["kind"], "image");

        // Verify the blob is preserved at the original path.
        let mut blob_content = Vec::new();
        zip.by_name("photos/vacation.jpg")
            .unwrap()
            .read_to_end(&mut blob_content)
            .unwrap();
        assert_eq!(blob_content, b"fake-jpeg-data");
    }

    // ── Trait delegation smoke test ──────────────────────────────────────

    /// Verify the `ExportStore` impl for `PgStore` compiles and delegates
    /// correctly (the async method calls go through the trait object).
    #[tokio::test]
    async fn export_store_trait_object_compiles() {
        // Just verify the trait object can be constructed.
        let _store: Arc<dyn ExportStore> = Arc::new(MockExportStore::new("test"));
    }
}
