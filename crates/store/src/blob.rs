//! Local-disk implementation of the [`kb_core::blob::Blob`] capability trait (plan §20).
//!
//! Content-addressed key layout: `{root}/{prefix}/{blob_key}`.
//! The root directory is hot-swappable via [`arc_swap::ArcSwap`] so an operator can
//! relocate storage without a restart (the hot-swap rule, CLAUDE.md).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use arc_swap::ArcSwap;
use async_trait::async_trait;
use bytes::Bytes;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

/// Local-disk blob store. Stores each object as a regular file under
/// `{root}/{prefix}/{key}`. The root is hot-swappable via [`ArcSwap`].
///
/// This is the "local first" implementation (§20); the production B2 backend lands in P8.
/// Content-addressed keys mean same path = same content, so `put` is naturally idempotent.
pub struct LocalBlob {
    root: ArcSwap<PathBuf>,
    prefix: String,
}

impl LocalBlob {
    /// Create a new local blob store rooted at `root` with the given namespace
    /// `prefix` (typically the tenant id).
    pub fn new(root: PathBuf, prefix: String) -> Self {
        Self {
            root: ArcSwap::new(Arc::new(root)),
            prefix,
        }
    }

    /// Update the root directory live, without a restart (hot-swappable).
    pub fn set_root(&self, new_root: PathBuf) {
        self.root.store(Arc::new(new_root));
    }

    /// Read the current root directory.
    fn current_root(&self) -> Arc<PathBuf> {
        self.root.load_full()
    }

    /// Compute the on-disk path for a given blob key.
    ///
    /// Validates that `key` contains no path-traversal sequences (`..`, leading
    /// `/`/`\`) and canonicalizes the prefix directory so symlinks in the root
    /// path cannot be exploited. The returned path is guaranteed to live inside
    /// `{root}/{prefix}`.
    ///
    /// # Errors
    /// Returns an error if the key attempts path traversal or if the prefix
    /// directory cannot be resolved.
    fn blob_path(&self, key: &str) -> anyhow::Result<PathBuf> {
        // Reject keys with path traversal sequences.
        if key.contains("..") || key.starts_with('/') || key.starts_with('\\') {
            anyhow::bail!("invalid blob key: path traversal rejected");
        }

        let root = self.current_root();
        let prefix_dir = Path::new(root.as_ref()).join(&self.prefix);

        // Ensure the prefix directory exists (a no-op when it already does),
        // then canonicalize so symlinks in root/prefix can't be used to escape.
        std::fs::create_dir_all(&prefix_dir)
            .with_context(|| "failed to create prefix directory for blob storage")?;
        let canonical_prefix = std::fs::canonicalize(&prefix_dir)
            .with_context(|| "failed to resolve prefix directory")?;

        // Key has been validated (no "..", no leading '/' or '\') so joining
        // onto a canonicalized prefix guarantees containment.
        Ok(canonical_prefix.join(key))
    }

    /// Streaming write: receive chunks from `rx` and write them to a file
    /// without buffering the entire blob in memory.
    ///
    /// Writes to a temporary file first, then atomically renames it to the
    /// final content-addressed path on success. If the target file already
    /// exists (idempotent put) the chunks are still received and discarded
    /// to avoid blocking the sender.
    ///
    /// # Errors
    /// Returns an error if the file cannot be created, written, or renamed.
    pub async fn streaming_put(
        &self,
        key: &str,
        mut rx: tokio::sync::mpsc::Receiver<Bytes>,
    ) -> anyhow::Result<()> {
        let final_path = self.blob_path(key)?;

        // Idempotent: if the file already exists, drain the receiver and
        // return — no need to re-write.
        if final_path.exists() {
            while rx.recv().await.is_some() {}
            return Ok(());
        }

        // Ensure parent directories exist.
        if let Some(parent) = final_path.parent() {
            tokio::fs::create_dir_all(parent).await.with_context(|| {
                format!("failed to create parent directories for blob at key `{key}`")
            })?;
        }

        // Write to a temporary file alongside the final path, then rename
        // atomically once all chunks are received.
        let temp_path = final_path.with_extension(".partial");
        let mut file: tokio::fs::File = tokio::fs::File::create(&temp_path)
            .await
            .with_context(|| format!("failed to create temp file for blob at key `{key}`"))?;

        let mut write_result: anyhow::Result<()> = Ok(());
        while let Some(chunk) = rx.recv().await {
            if write_result.is_err() {
                // Drain remaining chunks on write failure.
                continue;
            }
            if let Err(e) = file.write_all(&chunk).await {
                write_result = Err(e)
                    .with_context(|| format!("failed to write chunk for blob at key `{key}`"));
            }
        }

        // Flush before renaming.
        if write_result.is_ok()
            && let Err(e) = file.flush().await
        {
            write_result = Err(e).with_context(|| format!("failed to flush blob at key `{key}`"));
        }

        // On success, rename atomically. On failure, remove the temp file.
        match write_result {
            Ok(()) => {
                tokio::fs::rename(&temp_path, &final_path)
                    .await
                    .with_context(|| {
                        format!("failed to rename temp file to final path for key `{key}`")
                    })?;
                Ok(())
            }
            Err(e) => {
                let _ = tokio::fs::remove_file(&temp_path).await;
                Err(e)
            }
        }
    }
}
#[async_trait]
impl kb_core::blob::Blob for LocalBlob {
    /// Store `data` under `key`. Idempotent: if the file already exists at the
    /// content-addressed path, the write is skipped.
    ///
    /// # Errors
    /// Returns an error if parent directories cannot be created or the write fails.
    async fn put(&self, key: &str, data: Bytes) -> anyhow::Result<()> {
        let path = self.blob_path(key)?;

        // Idempotent — content-addressed keys mean same path = same content.
        if path.exists() {
            return Ok(());
        }

        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.with_context(|| {
                format!("failed to create parent directories for blob at key `{key}`")
            })?;
        }

        tokio::fs::write(&path, &data)
            .await
            .with_context(|| format!("failed to write blob at key `{key}`"))?;

        Ok(())
    }

    /// Fetch the full blob at `key`.
    ///
    /// # Errors
    /// Returns an error if the file does not exist or cannot be read.
    async fn get(&self, key: &str) -> anyhow::Result<Bytes> {
        let path = self.blob_path(key)?;
        let data = tokio::fs::read(&path)
            .await
            .with_context(|| format!("failed to read blob at key `{key}`"))?;
        Ok(Bytes::from(data))
    }

    /// Fetch a byte range `[start, end)` of the blob at `key`.
    ///
    /// If `start` is beyond the file length, an empty `Bytes` is returned.
    /// If `end` exceeds the file length it is clamped to the actual length.
    ///
    /// # Errors
    /// Returns an error if the file does not exist or cannot be read.
    async fn get_range(&self, key: &str, start: u64, end: u64) -> anyhow::Result<Bytes> {
        let path = self.blob_path(key)?;

        let mut file = tokio::fs::File::open(&path)
            .await
            .with_context(|| format!("failed to open blob at key `{key}` for range read"))?;

        let file_len = file
            .metadata()
            .await
            .with_context(|| format!("failed to read metadata for blob at key `{key}`"))?
            .len();

        // If start is past the end, there's nothing to return.
        if start >= file_len {
            return Ok(Bytes::new());
        }

        let end = end.min(file_len);
        // Safety: end > start because start < file_len and end = min(end, file_len) >= start+1.
        #[allow(clippy::cast_possible_truncation)]
        let len = (end - start) as usize;
        let mut buf = vec![0u8; len];

        file.seek(std::io::SeekFrom::Start(start))
            .await
            .with_context(|| format!("failed to seek to offset {start} in blob at key `{key}`"))?;
        file.read_exact(&mut buf)
            .await
            .with_context(|| format!("failed to read {len} bytes from blob at key `{key}`"))?;

        Ok(Bytes::from(buf))
    }

    /// Whether an object exists at `key`.
    ///
    /// # Errors
    /// Returns an error if the existence check itself fails (distinct from absent).
    async fn exists(&self, key: &str) -> anyhow::Result<bool> {
        let path = self.blob_path(key)?;
        // `Path::exists()` is a thin wrapper around `metadata()` with error suppression;
        // for a local disk, this is instantaneous and fine in an async context.
        Ok(path.exists())
    }

    /// Delete the object at `key`. If the file does not exist this is a no-op.
    ///
    /// # Errors
    /// Returns an error if the delete fails for a reason other than the file being absent.
    async fn delete(&self, key: &str) -> anyhow::Result<()> {
        let path = self.blob_path(key)?;
        match tokio::fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e).with_context(|| format!("failed to delete blob at key `{key}`")),
        }
    }

    /// Return a `file://` URL for the blob at `key` (dev convenience; production uses
    /// presigned B2 URLs — plan §20). The `expires_in` parameter is ignored for local
    /// storage since there is no expiry mechanism.
    ///
    /// # Errors
    /// Returns an error if a URL cannot be constructed.
    async fn presigned_get_url(&self, key: &str, _expires_in: Duration) -> anyhow::Result<String> {
        let path = self.blob_path(key)?;
        // `blob_path` already returns a canonicalized path, but verify the file exists.
        if !path.exists() {
            anyhow::bail!("blob at key `{key}` does not exist");
        }
        Ok(format!("file://{}", path.display()))
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use kb_core::blob::Blob;

    /// Helper: create a `LocalBlob` rooted in a temporary directory with the default
    /// test prefix.
    fn test_blob() -> (LocalBlob, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let blob = LocalBlob::new(dir.path().to_path_buf(), "test-prefix".into());
        (blob, dir)
    }

    // ── put + get round-trip ─────────────────────────────────────────────

    #[tokio::test]
    async fn put_and_get_roundtrip() {
        let (blob, _dir) = test_blob();
        let key = "abc123";
        let data = Bytes::from_static(b"hello, world");

        blob.put(key, data.clone()).await.unwrap();
        let got = blob.get(key).await.unwrap();

        assert_eq!(got, data);
    }

    #[tokio::test]
    async fn put_large_blob() {
        let (blob, _dir) = test_blob();
        let key = "large";
        let data = Bytes::from(vec![0xABu8; 1_000_000]);

        blob.put(key, data.clone()).await.unwrap();
        let got = blob.get(key).await.unwrap();

        assert_eq!(got, data);
    }

    // ── put idempotency ──────────────────────────────────────────────────

    #[tokio::test]
    async fn put_is_idempotent() {
        let (blob, _dir) = test_blob();
        let key = "idem-key";
        let data = Bytes::from_static(b"content-addressed data");

        // First write.
        blob.put(key, data.clone()).await.unwrap();
        // Second write with same key + content — should be a no-op.
        blob.put(key, data.clone()).await.unwrap();

        let got = blob.get(key).await.unwrap();
        assert_eq!(got, data, "content must be unchanged after idempotent put");
    }

    // ── get on non-existent key ──────────────────────────────────────────

    #[tokio::test]
    async fn get_nonexistent_key_is_error() {
        let (blob, _dir) = test_blob();
        let result = blob.get("no-such-key").await;
        assert!(result.is_err(), "get on missing key should fail");
    }

    // ── get_range ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn get_range_returns_correct_slice() {
        let (blob, _dir) = test_blob();
        let key = "range-test";
        let data = Bytes::from_static(b"0123456789");

        blob.put(key, data.clone()).await.unwrap();

        // [3, 7) → "3456"
        let slice = blob.get_range(key, 3, 7).await.unwrap();
        assert_eq!(slice, Bytes::from_static(b"3456"));
    }

    #[tokio::test]
    async fn get_range_clamps_end_to_file_length() {
        let (blob, _dir) = test_blob();
        let key = "clamp-end";
        let data = Bytes::from_static(b"ABCD"); // 4 bytes

        blob.put(key, data.clone()).await.unwrap();

        // end=100 is well past the file length → should clamp to 4.
        let slice = blob.get_range(key, 2, 100).await.unwrap();
        assert_eq!(slice, Bytes::from_static(b"CD"));
    }

    #[tokio::test]
    async fn get_range_start_beyond_length_returns_empty() {
        let (blob, _dir) = test_blob();
        let key = "beyond";
        let data = Bytes::from_static(b"XYZ");

        blob.put(key, data.clone()).await.unwrap();

        // start at 10, file is only 3 bytes.
        let slice = blob.get_range(key, 10, 20).await.unwrap();
        assert!(slice.is_empty());
    }

    #[tokio::test]
    async fn get_range_exact_file() {
        let (blob, _dir) = test_blob();
        let key = "exact";
        let data = Bytes::from_static(b"exact-match");

        blob.put(key, data.clone()).await.unwrap();

        let slice = blob.get_range(key, 0, data.len() as u64).await.unwrap();
        assert_eq!(slice, data);
    }

    #[tokio::test]
    async fn get_range_nonexistent_key_is_error() {
        let (blob, _dir) = test_blob();
        let result = blob.get_range("nope", 0, 10).await;
        assert!(result.is_err());
    }

    // ── exists ───────────────────────────────────────────────────────────

    #[tokio::test]
    async fn exists_true_after_put() {
        let (blob, _dir) = test_blob();
        let key = "exists-true";

        blob.put(key, Bytes::from_static(b"data")).await.unwrap();
        assert!(blob.exists(key).await.unwrap());
    }

    #[tokio::test]
    async fn exists_false_for_missing_key() {
        let (blob, _dir) = test_blob();
        assert!(!blob.exists("nothing-here").await.unwrap());
    }

    #[tokio::test]
    async fn exists_false_after_delete() {
        let (blob, _dir) = test_blob();
        let key = "gone";

        blob.put(key, Bytes::from_static(b"ephemeral"))
            .await
            .unwrap();
        assert!(blob.exists(key).await.unwrap());

        blob.delete(key).await.unwrap();
        assert!(!blob.exists(key).await.unwrap());
    }

    // ── delete ───────────────────────────────────────────────────────────

    #[tokio::test]
    async fn delete_removes_file() {
        let (blob, _dir) = test_blob();
        let key = "del-me";

        blob.put(key, Bytes::from_static(b"data")).await.unwrap();
        blob.delete(key).await.unwrap();

        assert!(!blob.exists(key).await.unwrap());
        assert!(blob.get(key).await.is_err());
    }

    #[tokio::test]
    async fn delete_nonexistent_key_is_noop() {
        let (blob, _dir) = test_blob();
        // Should succeed (no-op), not panic or error.
        blob.delete("ghost").await.unwrap();
    }

    // ── presigned_get_url ────────────────────────────────────────────────

    #[tokio::test]
    async fn presigned_get_url_returns_file_path() {
        let (blob, _dir) = test_blob();
        let key = "presign-test";

        blob.put(key, Bytes::from_static(b"stuff")).await.unwrap();
        let url = blob
            .presigned_get_url(key, Duration::from_secs(60))
            .await
            .unwrap();

        assert!(
            url.starts_with("file://"),
            "expected file:// URL, got: {url}"
        );
        // The path portion should contain both the prefix and the key.
        assert!(
            url.contains("test-prefix"),
            "URL should contain the prefix: {url}"
        );
        assert!(url.contains(key), "URL should contain the key: {url}");
    }

    #[tokio::test]
    async fn presigned_get_url_nonexistent_key_is_error() {
        let (blob, _dir) = test_blob();
        let result = blob
            .presigned_get_url("no-file", Duration::from_secs(60))
            .await;
        assert!(result.is_err());
    }

    // ── hot-swap root ────────────────────────────────────────────────────

    #[tokio::test]
    async fn hot_swap_root_moves_storage() {
        let dir1 = tempfile::tempdir().expect("tempdir 1");
        let dir2 = tempfile::tempdir().expect("tempdir 2");

        let blob = LocalBlob::new(dir1.path().to_path_buf(), "ns".into());
        let key = "hot-swap-key";
        let data = Bytes::from_static(b"pre-swap data");

        // Write under root 1.
        blob.put(key, data.clone()).await.unwrap();
        assert!(blob.exists(key).await.unwrap());
        assert!(dir1.path().join("ns").join(key).exists());

        // Swap root to dir2.
        blob.set_root(dir2.path().to_path_buf());

        // The key should NOT exist under the new root.
        assert!(!blob.exists(key).await.unwrap());

        // Write the same key under root 2.
        blob.put(key, data.clone()).await.unwrap();
        assert!(blob.exists(key).await.unwrap());
        assert!(dir2.path().join("ns").join(key).exists());
    }

    // ── key isolation across prefixes ────────────────────────────────────

    #[tokio::test]
    async fn different_prefixes_isolate_keys() {
        let dir = tempfile::tempdir().expect("tempdir");
        let blob_a = LocalBlob::new(dir.path().to_path_buf(), "tenant-a".into());
        let blob_b = LocalBlob::new(dir.path().to_path_buf(), "tenant-b".into());

        let key = "shared-key";
        let data_a = Bytes::from_static(b"tenant A data");
        let data_b = Bytes::from_static(b"tenant B data");

        blob_a.put(key, data_a.clone()).await.unwrap();
        blob_b.put(key, data_b.clone()).await.unwrap();

        // Each sees only its own data.
        assert_eq!(blob_a.get(key).await.unwrap(), data_a);
        assert_eq!(blob_b.get(key).await.unwrap(), data_b);
    }

    // ── put creates nested directories ───────────────────────────────────

    #[tokio::test]
    async fn put_creates_parent_directories() {
        let (blob, dir) = test_blob();
        // Use a key that includes directory separators (simulating a
        // structured key layout).
        let key = "a/b/c/nested-key";
        let data = Bytes::from_static(b"nested");

        blob.put(key, data.clone()).await.unwrap();

        // Verify the file is at the expected location.
        let expected_path = dir.path().join("test-prefix").join(key);
        assert!(expected_path.exists());
        assert_eq!(blob.get(key).await.unwrap(), data);
    }

    // ── path traversal rejection ────────────────────────────────────────

    #[tokio::test]
    async fn put_rejects_dot_dot_paths() {
        let (blob, _dir) = test_blob();
        let result = blob.put("../escape", Bytes::from_static(b"bad")).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("path traversal"),
            "expected traversal error, got: {err}"
        );
    }

    #[tokio::test]
    async fn get_rejects_dot_dot_paths() {
        let (blob, _dir) = test_blob();
        let result = blob.get("../../etc/passwd").await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("path traversal"),
            "expected traversal error, got: {err}"
        );
    }

    #[tokio::test]
    async fn delete_rejects_dot_dot_paths() {
        let (blob, _dir) = test_blob();
        let err = blob
            .delete("..%2F..%2Fsecret")
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("path traversal"),
            "expected traversal error, got: {err}"
        );
    }

    #[tokio::test]
    async fn rejects_leading_slash_keys() {
        let (blob, _dir) = test_blob();
        let result = blob.put("/etc/hosts", Bytes::from_static(b"bad")).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("path traversal"),
            "expected traversal error, got: {err}"
        );
    }

    #[tokio::test]
    async fn rejects_leading_backslash_keys() {
        let (blob, _dir) = test_blob();
        let result = blob.get("\\Windows\\System32\\config").await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("path traversal"),
            "expected traversal error, got: {err}"
        );
    }
}
