//! Backblaze B2 blob backend implementing the [`kb_core::blob::Blob`] capability trait
//! (plan §20).
//!
//! Uses the [`object_store`] crate with the S3-compatible B2 endpoint. Content-addressed
//! key layout: `{tenant_id}/{sha256_hex}` — same as [`super::LocalBlob`].
//!
//! # Hot-swappable configuration
//!
//! The B2 credentials, endpoint, and bucket name are held behind an [`arc_swap::ArcSwap`]
//! so an operator can rotate keys or relocate storage without a restart (the hot-swap
//! rule, CLAUDE.md). The underlying [`object_store::aws::AmazonS3`] client is rebuilt
//! when [`B2Blob::reload_config`] is called.
//!
//! # Multipart uploads
//!
//! Files larger than 5 MB are uploaded via S3 multipart to avoid buffering the entire
//! object in RAM during transfer. Parts are sent sequentially at 5 MB boundaries.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use arc_swap::ArcSwap;
use async_trait::async_trait;
use bytes::Bytes;
use object_store::ObjectStoreExt;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as OSPath;
use object_store::signer::Signer;
use reqwest::Method;

/// Files larger than this threshold (5 MB) are uploaded via multipart.
const MULTIPART_THRESHOLD: usize = 5 * 1024 * 1024; // 5 MB

/// Each multipart part is at most this many bytes (S3 minimum part size = 5 MB).
const MULTIPART_CHUNK_SIZE: usize = 5 * 1024 * 1024; // 5 MB

/// Default B2 S3-compatible endpoint (US West).
const DEFAULT_B2_ENDPOINT: &str = "https://s3.us-west-004.backblazeb2.com";

/// Default region for B2 S3-compatible endpoint.
const DEFAULT_B2_REGION: &str = "us-west-004";

// ── B2Config ──────────────────────────────────────────────────────────────────

/// Hot-swappable configuration for the Backblaze B2 blob backend.
///
/// All fields are plain strings so the struct can be cheaply cloned and stored
/// inside an [`ArcSwap`]. Call [`Self::build_store`] to materialize the
/// [`object_store::aws::AmazonS3`] client.
#[derive(Clone)]
pub struct B2Config {
    /// B2 application key ID (equivalent to an S3 access key).
    key_id: String,
    /// B2 application key (equivalent to an S3 secret key).
    application_key: String,
    /// B2 bucket name.
    bucket: String,
    /// S3-compatible endpoint URL (e.g. `https://s3.us-west-004.backblazeb2.com`).
    endpoint: String,
}

/// Manual [`Debug`] impl that redacts the secret `application_key` field
/// so it is never unintentionally logged.
impl std::fmt::Debug for B2Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("B2Config")
            .field("key_id", &self.key_id)
            .field("application_key", &"[REDACTED]")
            .field("bucket", &self.bucket)
            .field("endpoint", &self.endpoint)
            .finish()
    }
}

impl B2Config {
    /// Create a new B2 configuration.
    ///
    /// If `endpoint` is empty, the default B2 US-West endpoint is used.
    pub fn new(
        key_id: impl Into<String>,
        application_key: impl Into<String>,
        bucket: impl Into<String>,
        endpoint: impl Into<String>,
    ) -> Self {
        let endpoint: String = endpoint.into();
        Self {
            key_id: key_id.into(),
            application_key: application_key.into(),
            bucket: bucket.into(),
            endpoint: if endpoint.is_empty() {
                DEFAULT_B2_ENDPOINT.to_string()
            } else {
                endpoint
            },
        }
    }

    /// Return the key ID.
    pub fn key_id(&self) -> &str {
        &self.key_id
    }

    /// Return the application key.
    pub fn application_key(&self) -> &str {
        &self.application_key
    }

    /// Return the bucket name.
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Return the endpoint URL.
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Construct an [`object_store::aws::AmazonS3`] client from this configuration.
    ///
    /// # Errors
    /// Returns an error if the builder fails (e.g. invalid endpoint URL).
    fn build_store(&self) -> anyhow::Result<object_store::aws::AmazonS3> {
        AmazonS3Builder::new()
            .with_access_key_id(&self.key_id)
            .with_secret_access_key(&self.application_key)
            .with_endpoint(&self.endpoint)
            .with_bucket_name(&self.bucket)
            .with_region(DEFAULT_B2_REGION)
            .with_allow_http(self.endpoint.starts_with("http://"))
            .build()
            .context("failed to build B2/S3 object store client")
    }
}

// ── B2Blob ────────────────────────────────────────────────────────────────────

/// Backblaze B2 blob backend implementing the [`kb_core::blob::Blob`] capability
/// trait (plan §20).
///
/// Content-addressed key layout: `{tenant_id}/{sha256_hex}`. The underlying
/// S3-compatible client is rebuilt on config changes (hot-swappable).
///
/// # Multipart uploads
///
/// Payloads larger than 5 MB are split into parts and uploaded via S3 multipart,
/// so the full object is never buffered in the HTTP client.
pub struct B2Blob {
    /// Hot-swappable configuration (credentials, endpoint, bucket).
    config: ArcSwap<B2Config>,
    /// The materialized S3-compatible object store client. Rebuilt when config
    /// changes (via [`Self::reload_config`]).
    store: ArcSwap<object_store::aws::AmazonS3>,
}

impl B2Blob {
    /// Create a new `B2Blob` with the given configuration. The S3-compatible
    /// client is built immediately.
    ///
    /// # Errors
    /// Returns an error if the initial client build fails (e.g. bad endpoint).
    pub fn new(config: B2Config) -> anyhow::Result<Self> {
        let store = config.build_store()?;
        Ok(Self {
            config: ArcSwap::new(Arc::new(config)),
            store: ArcSwap::new(Arc::new(store)),
        })
    }

    /// Reload the configuration from fresh values and rebuild the underlying
    /// S3-compatible client. Call this when credentials are rotated or the
    /// endpoint/bucket changes. This is the hot-swap entry point (CLAUDE.md).
    ///
    /// # Errors
    /// Returns an error if the new client cannot be built. The old client
    /// remains in place on failure.
    pub fn reload_config(&self, new_config: B2Config) -> anyhow::Result<()> {
        let new_store = new_config.build_store()?;
        self.config.store(Arc::new(new_config));
        self.store.store(Arc::new(new_store));
        Ok(())
    }

    /// Snapshot the current S3-compatible client.
    fn current_store(&self) -> Arc<object_store::aws::AmazonS3> {
        self.store.load_full()
    }

    /// Return a snapshot of the current configuration.
    pub fn current_config(&self) -> Arc<B2Config> {
        self.config.load_full()
    }

    /// Convert a blob key to an [`object_store::path::Path`].
    ///
    /// Keys must be non-empty and free of leading `/` — they are already
    /// tenant-prefixed content addresses (e.g. `tenant-1/abcdef01…`).
    fn os_path(&self, key: &str) -> OSPath {
        // object_store::path::Path internally normalises, but guard against
        // empty keys anyway.
        debug_assert!(!key.is_empty(), "blob key must be non-empty");
        OSPath::from(key)
    }
}

// ── Blob trait implementation ─────────────────────────────────────────────────

#[async_trait]
impl kb_core::blob::Blob for B2Blob {
    /// Store `data` under `key` via S3-compatible API. Files larger than 5 MB
    /// use multipart upload for streaming efficiency.
    ///
    /// Idempotent: content-addressed keys mean same key = same content; a
    /// subsequent write is a no-op (the object already exists).
    ///
    /// # Errors
    /// Returns an error if the put or multipart upload fails.
    async fn put(&self, key: &str, data: Bytes) -> anyhow::Result<()> {
        let store = self.current_store();
        let path = self.os_path(key);

        if data.len() <= MULTIPART_THRESHOLD {
            // Small payload: single PUT.
            let payload = object_store::PutPayload::from_bytes(data);
            store
                .put(&path, payload)
                .await
                .with_context(|| format!("B2 put failed for key `{key}`"))?;
        } else {
            // Large payload: multipart upload in 5 MB parts.
            let mut upload = store
                .put_multipart(&path)
                .await
                .with_context(|| format!("B2 multipart initiation failed for key `{key}`"))?;

            let mut offset: usize = 0;
            let mut part_idx: usize = 0;
            while offset < data.len() {
                let end = (offset + MULTIPART_CHUNK_SIZE).min(data.len());
                let chunk = data.slice(offset..end);
                upload
                    .put_part(object_store::PutPayload::from_bytes(chunk))
                    .await
                    .with_context(|| {
                        format!("B2 multipart part {part_idx} upload failed for key `{key}`")
                    })?;
                offset = end;
                part_idx += 1;
            }
            upload
                .complete()
                .await
                .with_context(|| format!("B2 multipart completion failed for key `{key}`"))?;
        }

        Ok(())
    }

    /// Fetch the full blob at `key` from B2.
    ///
    /// # Errors
    /// Returns an error if the object is missing or the read fails.
    async fn get(&self, key: &str) -> anyhow::Result<Bytes> {
        let store = self.current_store();
        let path = self.os_path(key);
        let result = store
            .get(&path)
            .await
            .with_context(|| format!("B2 get failed for key `{key}`"))?;
        let data = result
            .bytes()
            .await
            .with_context(|| format!("B2 body read failed for key `{key}`"))?;
        Ok(data)
    }

    /// Fetch a byte range `[start, end)` of the blob at `key`.
    ///
    /// # Errors
    /// Returns an error if the object is missing or the read fails.
    async fn get_range(&self, key: &str, start: u64, end: u64) -> anyhow::Result<Bytes> {
        let store = self.current_store();
        let path = self.os_path(key);
        let range = start..end;
        let data = store
            .get_range(&path, range)
            .await
            .with_context(|| format!("B2 get_range [{start},{end}) failed for key `{key}`"))?;
        Ok(data)
    }

    /// Whether an object exists at `key`.
    ///
    /// Returns `true` if the HEAD request succeeds, `false` if the object is
    /// not found, and an error on any other failure.
    ///
    /// # Errors
    /// Returns an error if the existence check itself fails (distinct from
    /// a definitive "absent").
    async fn exists(&self, key: &str) -> anyhow::Result<bool> {
        let store = self.current_store();
        let path = self.os_path(key);
        match store.head(&path).await {
            Ok(_meta) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(anyhow::Error::from(e))
                .with_context(|| format!("B2 existence check failed for key `{key}`")),
        }
    }

    /// Delete the object at `key`.
    ///
    /// # Errors
    /// Returns an error if the delete fails.
    async fn delete(&self, key: &str) -> anyhow::Result<()> {
        let store = self.current_store();
        let path = self.os_path(key);
        store
            .delete(&path)
            .await
            .with_context(|| format!("B2 delete failed for key `{key}`"))?;
        Ok(())
    }

    /// Return a time-limited presigned GET URL for the object at `key`, valid
    /// for `expires_in`. This allows direct client downloads without proxying
    /// through the app server (plan §20).
    ///
    /// Uses the S3 SigV4 query-parameter signing that B2's S3-compatible API
    /// accepts.
    ///
    /// # Errors
    /// Returns an error if the signing operation fails.
    async fn presigned_get_url(&self, key: &str, expires_in: Duration) -> anyhow::Result<String> {
        let store = self.current_store();
        let path = self.os_path(key);
        // `AmazonS3` directly implements `Signer` (object_store 0.13).
        let url = store
            .signed_url(Method::GET, &path, expires_in)
            .await
            .with_context(|| format!("B2 presigned URL generation failed for key `{key}`"))?;
        Ok(url.to_string())
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    // ── B2Config tests ──────────────────────────────────────────────────

    #[test]
    fn config_defaults_endpoint_when_empty() {
        let cfg = B2Config::new("id", "key", "bucket", "");
        assert_eq!(cfg.endpoint(), DEFAULT_B2_ENDPOINT);
    }

    #[test]
    fn config_preserves_custom_endpoint() {
        let cfg = B2Config::new(
            "id",
            "key",
            "bucket",
            "https://s3.eu-central-003.backblazeb2.com",
        );
        assert_eq!(cfg.endpoint(), "https://s3.eu-central-003.backblazeb2.com");
    }

    #[test]
    fn config_clones_correctly() {
        let cfg = B2Config::new("my-id", "my-key", "my-bucket", "https://ep.example.com");
        let cfg2 = cfg.clone();
        assert_eq!(cfg2.key_id(), "my-id");
        assert_eq!(cfg2.application_key(), "my-key");
        assert_eq!(cfg2.bucket(), "my-bucket");
        assert_eq!(cfg2.endpoint(), "https://ep.example.com");
    }

    #[test]
    fn config_build_store_succeeds_with_https_endpoint() {
        // A valid HTTPS endpoint should build without error.
        let cfg = B2Config::new(
            "id",
            "key",
            "bucket",
            "https://s3.us-west-004.backblazeb2.com",
        );
        assert!(cfg.build_store().is_ok());
    }

    #[test]
    fn config_accessors_return_correct_values() {
        let cfg = B2Config::new("akid", "secret", "the-bucket", "https://ep.example.com");
        assert_eq!(cfg.key_id(), "akid");
        assert_eq!(cfg.application_key(), "secret");
        assert_eq!(cfg.bucket(), "the-bucket");
        assert_eq!(cfg.endpoint(), "https://ep.example.com");
    }

    #[test]
    fn config_empty_endpoint_uses_default() {
        let cfg = B2Config::new("id", "key", "bucket", "");
        assert_eq!(cfg.endpoint(), DEFAULT_B2_ENDPOINT);
        // Verify the default is the documented B2 endpoint.
        assert!(cfg.endpoint().contains("backblazeb2.com"));
    }

    #[test]
    fn config_build_store_succeeds_with_any_endpoint() {
        // AmazonS3Builder::build() is lazy — it doesn't validate
        // the endpoint URL at build time; validation happens on first
        // HTTP request. So even unusual inputs succeed here.
        let cfg = B2Config::new("id", "key", "bucket", "hostname-only");
        // build() succeeds; actual connectivity errors surface at
        // request time.
        assert!(cfg.build_store().is_ok());
    }

    #[test]
    fn config_build_store_allows_http() {
        // HTTP endpoint with allow_http flag set.
        let cfg = B2Config::new("id", "key", "bucket", "http://127.0.0.1:9000");
        assert!(cfg.build_store().is_ok());
    }

    // ── B2Blob construction + reload ───────────────────────────────────

    #[test]
    fn new_builds_with_valid_config() {
        let cfg = B2Config::new("id", "key", "bucket", "http://127.0.0.1:9000");
        let result = B2Blob::new(cfg);
        assert!(result.is_ok());
    }

    #[test]
    fn new_succeeds_with_any_endpoint() {
        // B2Blob::new() builds an AmazonS3 client which is lazy about
        // endpoint validation — any syntactically acceptable input works.
        let cfg = B2Config::new("id", "key", "bucket", "bad-endpoint");
        let result = B2Blob::new(cfg);
        assert!(result.is_ok());
    }

    #[test]
    fn reload_config_succeeds_with_valid_config() {
        let cfg1 = B2Config::new("id1", "key1", "bucket1", "http://127.0.0.1:9000");
        let blob = B2Blob::new(cfg1).unwrap();

        let cfg2 = B2Config::new("id2", "key2", "bucket2", "http://127.0.0.1:9001");
        let result = blob.reload_config(cfg2.clone());
        assert!(result.is_ok());

        // Configuration is updated after reload.
        let current = blob.current_config();
        assert_eq!(current.key_id(), "id2");
        assert_eq!(current.bucket(), "bucket2");
    }

    #[test]
    fn reload_config_restores_store_after_valid_reload() {
        let cfg1 = B2Config::new("id1", "key1", "bucket1", "http://127.0.0.1:9000");
        let blob = B2Blob::new(cfg1).unwrap();

        // Reload with a different valid config.
        let cfg2 = B2Config::new("id2", "key2", "bucket2", "http://127.0.0.1:9001");
        blob.reload_config(cfg2).unwrap();

        // The config should be updated.
        let current = blob.current_config();
        assert_eq!(current.key_id(), "id2");
        assert_eq!(current.bucket(), "bucket2");
        assert_eq!(current.endpoint(), "http://127.0.0.1:9001");
    }

    // ── os_path construction ────────────────────────────────────────────

    #[test]
    fn os_path_maps_key_to_path() {
        let cfg = B2Config::new("id", "key", "bucket", "http://127.0.0.1:9000");
        let blob = B2Blob::new(cfg).unwrap();
        let path = blob.os_path("tenant-1/abcdef");
        assert_eq!(path.as_ref(), "tenant-1/abcdef");
    }

    #[test]
    fn os_path_handles_nested_keys() {
        let cfg = B2Config::new("id", "key", "bucket", "http://127.0.0.1:9000");
        let blob = B2Blob::new(cfg).unwrap();
        let path = blob.os_path("tenant-1/a/b/c/key");
        assert_eq!(path.as_ref(), "tenant-1/a/b/c/key");
    }

    #[test]
    fn os_path_handles_hex_sha256_keys() {
        let cfg = B2Config::new("id", "key", "bucket", "http://127.0.0.1:9000");
        let blob = B2Blob::new(cfg).unwrap();
        // Typical content-addressed key: tenant prefix + 64-char SHA-256 hex.
        let key = "tenant-42/e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        let path = blob.os_path(key);
        assert_eq!(path.as_ref(), key);
    }

    // ── Constants ───────────────────────────────────────────────────────

    #[test]
    fn multipart_threshold_is_5mb() {
        assert_eq!(MULTIPART_THRESHOLD, 5 * 1024 * 1024);
    }

    #[test]
    fn multipart_chunk_size_is_5mb() {
        assert_eq!(MULTIPART_CHUNK_SIZE, 5 * 1024 * 1024);
    }

    #[test]
    fn default_endpoint_is_backblaze() {
        assert!(DEFAULT_B2_ENDPOINT.contains("backblazeb2.com"));
        assert!(DEFAULT_B2_ENDPOINT.starts_with("https://"));
    }

    // ── Integration tests using real S3-compatible backend ──────────────
    //
    // These tests use B2Blob pointed at a real S3-compatible endpoint
    // (MinIO testcontainer, or a real B2 bucket). They are #[ignore] in
    // `just ci` (require DOCKER_HOST + testcontainers or real credentials).

    mod b2_integration {
        use super::*;
        use kb_core::blob::Blob;

        /// Helper to construct a `B2Blob` pointed at a MinIO testcontainer
        /// (or any S3-compatible endpoint). Set `S3_TEST_ENDPOINT`,
        /// `S3_TEST_ACCESS_KEY`, `S3_TEST_SECRET_KEY` in the environment
        /// to run these tests.
        fn test_b2_from_env() -> anyhow::Result<B2Blob> {
            let endpoint = std::env::var("S3_TEST_ENDPOINT")
                .unwrap_or_else(|_| "http://127.0.0.1:9000".to_string());
            let access_key =
                std::env::var("S3_TEST_ACCESS_KEY").unwrap_or_else(|_| "minioadmin".to_string());
            let secret_key =
                std::env::var("S3_TEST_SECRET_KEY").unwrap_or_else(|_| "minioadmin".to_string());
            let bucket =
                std::env::var("S3_TEST_BUCKET").unwrap_or_else(|_| "test-bucket".to_string());

            let cfg = B2Config::new(access_key, secret_key, bucket, endpoint);
            B2Blob::new(cfg)
        }

        /// Put + get round-trip against an S3-compatible endpoint.
        #[tokio::test]
        #[ignore = "needs S3-compatible endpoint (minIO testcontainer or B2 account)"]
        async fn put_and_get_roundtrip_s3() {
            let blob = test_b2_from_env().expect("build test blob");
            let key = "tenant-test/put-get-roundtrip";
            let data = Bytes::from_static(b"hello from b2 blob test");

            blob.put(key, data.clone()).await.unwrap();
            let got = blob.get(key).await.unwrap();

            assert_eq!(got, data);

            // Clean up.
            blob.delete(key).await.unwrap();
        }

        /// Put is idempotent (same key + content = no error on second put).
        #[tokio::test]
        #[ignore = "needs S3-compatible endpoint"]
        async fn put_is_idempotent_s3() {
            let blob = test_b2_from_env().expect("build test blob");
            let key = "tenant-test/idempotent";
            let data = Bytes::from_static(b"idempotent content");

            blob.put(key, data.clone()).await.unwrap();
            // Second put with same key+content should succeed.
            blob.put(key, data.clone()).await.unwrap();

            let got = blob.get(key).await.unwrap();
            assert_eq!(got, data);

            blob.delete(key).await.unwrap();
        }

        /// Exists returns true after put, false before and after delete.
        #[tokio::test]
        #[ignore = "needs S3-compatible endpoint"]
        async fn exists_s3() {
            let blob = test_b2_from_env().expect("build test blob");
            let key = "tenant-test/exists-test";

            assert!(!blob.exists(key).await.unwrap());
            blob.put(key, Bytes::from_static(b"data")).await.unwrap();
            assert!(blob.exists(key).await.unwrap());
            blob.delete(key).await.unwrap();
            assert!(!blob.exists(key).await.unwrap());
        }

        /// Get on non-existent key returns an error.
        #[tokio::test]
        #[ignore = "needs S3-compatible endpoint"]
        async fn get_nonexistent_key_is_error_s3() {
            let blob = test_b2_from_env().expect("build test blob");
            let result = blob.get("tenant-test/does-not-exist").await;
            assert!(result.is_err());
        }

        /// Get_range returns the correct byte slice.
        #[tokio::test]
        #[ignore = "needs S3-compatible endpoint"]
        async fn get_range_s3() {
            let blob = test_b2_from_env().expect("build test blob");
            let key = "tenant-test/range-test";
            let data = Bytes::from_static(b"0123456789");

            blob.put(key, data.clone()).await.unwrap();

            let slice = blob.get_range(key, 3, 7).await.unwrap();
            assert_eq!(slice, Bytes::from_static(b"3456"));

            blob.delete(key).await.unwrap();
        }

        /// Delete is idempotent (no error on missing key).
        #[tokio::test]
        #[ignore = "needs S3-compatible endpoint"]
        async fn delete_idempotent_s3() {
            let blob = test_b2_from_env().expect("build test blob");
            // Deleting a key that doesn't exist shouldn't error.
            blob.delete("tenant-test/ghost-key").await.unwrap();
        }

        /// Multipart upload for a 6 MB payload (just over the 5 MB threshold).
        #[tokio::test]
        #[ignore = "needs S3-compatible endpoint"]
        async fn multipart_upload_s3() {
            let blob = test_b2_from_env().expect("build test blob");
            let key = "tenant-test/multipart-test";
            // 6 MB of data — two parts (5 MB + 1 MB).
            let data = Bytes::from(vec![0xABu8; 6 * 1024 * 1024]);

            blob.put(key, data.clone()).await.unwrap();
            let got = blob.get(key).await.unwrap();

            assert_eq!(got.len(), data.len());
            assert_eq!(got, data);

            blob.delete(key).await.unwrap();
        }

        /// Hot-reload changes the underlying store without a restart.
        #[tokio::test]
        #[ignore = "needs S3-compatible endpoint"]
        async fn hot_reload_swaps_store_s3() {
            let blob = test_b2_from_env().expect("build test blob");
            let key = "tenant-test/hot-reload";
            let data = Bytes::from_static(b"pre-reload data");

            // Put some data with the first config.
            blob.put(key, data.clone()).await.unwrap();
            assert_eq!(blob.get(key).await.unwrap(), data);

            // Reload with the same config.
            let endpoint = std::env::var("S3_TEST_ENDPOINT")
                .unwrap_or_else(|_| "http://127.0.0.1:9000".to_string());
            let access_key =
                std::env::var("S3_TEST_ACCESS_KEY").unwrap_or_else(|_| "minioadmin".to_string());
            let secret_key =
                std::env::var("S3_TEST_SECRET_KEY").unwrap_or_else(|_| "minioadmin".to_string());
            let bucket =
                std::env::var("S3_TEST_BUCKET").unwrap_or_else(|_| "test-bucket".to_string());

            let cfg = B2Config::new(access_key, secret_key, bucket, endpoint);
            blob.reload_config(cfg).unwrap();

            // After reload, the same key is still accessible.
            assert!(blob.exists(key).await.unwrap());
            assert_eq!(blob.get(key).await.unwrap(), data);

            blob.delete(key).await.unwrap();
        }

        /// Presigned URL generation succeeds against S3-compatible backend.
        #[tokio::test]
        #[ignore = "needs S3-compatible endpoint"]
        async fn presigned_get_url_s3() {
            let blob = test_b2_from_env().expect("build test blob");
            let key = "tenant-test/presigned";
            let data = Bytes::from_static(b"presigned test data");

            blob.put(key, data.clone()).await.unwrap();

            let url = blob
                .presigned_get_url(key, Duration::from_secs(3600))
                .await
                .unwrap();

            // The URL should contain the key and S3 signature params.
            assert!(url.contains("tenant-test/presigned"));
            assert!(url.contains("X-Amz-Signature"));
            // It should be a valid HTTP(S) URL.
            assert!(url.starts_with("http"));

            blob.delete(key).await.unwrap();
        }
    }
}
