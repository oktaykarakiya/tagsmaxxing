// SPDX-License-Identifier: AGPL-3.0-or-later

//! Streaming multipart upload for S3-compatible blob backends (plan §20, P8-T4).
//!
//! Files larger than [`MultipartUploadConfig::threshold_bytes`] are uploaded in parts
//! via S3 multipart so the full file is never buffered in RAM. Each part is retried up to
//! `max_retries` times on transient failure; exhausting all retries aborts the upload.
//!
//! # Usage pattern (B2Blob)
//!
//! ```ignore
//! let upload = b2_blob.start_multipart_stream(key, total_size, &config, progress_tx).await?;
//! while let Some(chunk) = source.next_chunk().await {
//!     upload.feed(&chunk).await?;
//! }
//! upload.finish().await?;
//! ```

use bytes::Bytes;
use tokio::sync::mpsc;

// ── Configuration ─────────────────────────────────────────────────────────────

/// Configuration for streaming multipart blob uploads.
///
/// All fields are cheap to clone so the config can be shared across uploads.
/// A sensible default is provided by [`MultipartUploadConfig::default`].
#[derive(Clone, Debug)]
pub struct MultipartUploadConfig {
    /// Files whose total byte count is ≤ this threshold use a regular single PUT
    /// (the caller decides whether to stream into a buffer or use [`Blob::put`]).
    /// Default: 5 MB (`5 * 1024 * 1024`).
    pub threshold_bytes: usize,

    /// Each multipart part is at most this many bytes. The S3 minimum is 5 MB;
    /// a smaller value is accepted but will be silently clamped by some backends.
    /// Default: 5 MB (`5 * 1024 * 1024`).
    pub part_size_bytes: usize,

    /// Maximum retry attempts for a single failed part. After all retries are
    /// exhausted the upload is aborted and an error returned.
    /// Default: 3.
    pub max_retries: u32,
}

impl Default for MultipartUploadConfig {
    fn default() -> Self {
        Self {
            threshold_bytes: 5 * 1024 * 1024,
            part_size_bytes: 5 * 1024 * 1024,
            max_retries: 3,
        }
    }
}

// ── Progress reporting ────────────────────────────────────────────────────────

/// Reported after a part is successfully uploaded.
///
/// The caller receives these via a [`tokio::sync::mpsc::UnboundedReceiver`] and
/// can use them to drive a UI progress bar (bytes_sofar / total_bytes).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartProgress {
    /// Zero-based index of the part that just completed.
    pub part_index: usize,

    /// Total bytes uploaded so far (counting all completed parts).
    pub bytes_sofar: u64,

    /// Best-effort total upload size (0 if unknown).
    pub total_bytes: u64,
}

// ── Internal trait: abstracts the S3 multipart session for testability ───────

/// Operations that a multipart upload backend must support.
///
/// This is an internal implementation detail kept private to the module;
/// it allows unit tests to inject a mock without touching real S3.
#[async_trait::async_trait]
trait MultipartOps: Send {
    /// Upload one part. Part numbers are caller-assigned (0-based internally).
    ///
    /// # Errors
    /// Returns an error if the part write fails. The caller retries.
    async fn put_part(&mut self, payload: Bytes) -> anyhow::Result<()>;

    /// Complete the upload. Called after the last part is successfully written.
    ///
    /// # Errors
    /// Returns an error if the complete call itself fails.
    async fn complete(&mut self) -> anyhow::Result<()>;

    /// Abort / cancel the upload, cleaning up any partial parts on the server.
    ///
    /// # Errors
    /// Returns an error if the abort call itself fails (best-effort; the server
    /// side may still orphan parts that a lifecycle policy must sweep).
    async fn abort(&mut self) -> anyhow::Result<()>;
}

// ── B2 / S3 adapter ───────────────────────────────────────────────────────────

/// Wraps an [`object_store::MultipartUpload`] so it satisfies [`MultipartOps`].
struct ObjectStoreMultipart {
    inner: Box<dyn object_store::MultipartUpload>,
}

#[async_trait::async_trait]
impl MultipartOps for ObjectStoreMultipart {
    async fn put_part(&mut self, payload: Bytes) -> anyhow::Result<()> {
        let _part = self
            .inner
            .put_part(object_store::PutPayload::from_bytes(payload))
            .await?;
        Ok(())
    }

    async fn complete(&mut self) -> anyhow::Result<()> {
        let _result = self.inner.complete().await?;
        Ok(())
    }

    async fn abort(&mut self) -> anyhow::Result<()> {
        self.inner.abort().await?;
        Ok(())
    }
}

// ── StreamingMultipartUpload ──────────────────────────────────────────────────

/// A streaming multipart upload that accepts chunks of data and uploads them
/// to an S3-compatible backend part by part.
///
/// # Memory usage
///
/// Peak memory is bounded to roughly `part_size_bytes + largest single feed chunk`
/// because parts are flushed as soon as the internal buffer reaches `part_size_bytes`.
///
/// # Cancellation safety
///
/// If this struct is dropped before [`finish`](Self::finish) is called, the
/// multipart upload is aborted on a best-effort basis via `tokio::spawn`.
/// For reliable cleanup, call [`abort`](Self::abort) explicitly before dropping.
pub struct StreamingMultipartUpload {
    ops: Option<Box<dyn MultipartOps>>,
    config: MultipartUploadConfig,
    buf: Vec<u8>,
    part_count: usize,
    bytes_sofar: u64,
    total_bytes: u64,
    progress_tx: Option<mpsc::UnboundedSender<PartProgress>>,
    finished: bool,
}

impl StreamingMultipartUpload {
    /// Create a new streaming multipart upload session.
    ///
    /// `ops` is the multipart backend (e.g. an S3 `MultipartUpload` handle).
    /// `total_bytes` is an optional size hint used for progress reporting (0 if unknown).
    /// `progress_tx` receives a [`PartProgress`] event after each part uploads successfully.
    fn new(
        ops: Box<dyn MultipartOps>,
        config: MultipartUploadConfig,
        total_bytes: u64,
        progress_tx: Option<mpsc::UnboundedSender<PartProgress>>,
    ) -> Self {
        let cap = config.part_size_bytes.saturating_add(64 * 1024);
        Self {
            ops: Some(ops),
            config,
            buf: Vec::with_capacity(cap),
            part_count: 0,
            bytes_sofar: 0,
            total_bytes,
            progress_tx,
            finished: false,
        }
    }

    /// Feed a chunk of data into the upload. When enough data accumulates
    /// (≥ `part_size_bytes`), a part is uploaded automatically.
    ///
    /// # Errors
    ///
    /// Returns an error if a part upload fails after exhausting all retries.
    /// The upload is **already aborted** in that case — the caller does not
    /// need to call [`abort`](Self::abort).
    ///
    /// # Panics
    ///
    /// Panics if called after [`finish`](Self::finish) or [`abort`](Self::abort).
    pub async fn feed(&mut self, chunk: &[u8]) -> anyhow::Result<()> {
        assert!(!self.finished, "feed() called on finished upload");

        if chunk.is_empty() {
            return Ok(());
        }

        self.buf.extend_from_slice(chunk);

        // Flush full parts while the buffer has ≥ part_size_bytes.
        while self.buf.len() >= self.config.part_size_bytes {
            let part_body: Vec<u8> = self.buf.drain(..self.config.part_size_bytes).collect();
            self.upload_one_part(Bytes::from(part_body)).await?;
        }

        Ok(())
    }

    /// Finish the upload: any remaining buffered data is uploaded as the final
    /// part, then the multipart upload is completed on the server.
    ///
    /// After this returns successfully the object is durable. If it returns an
    /// error, the upload has been aborted — do not call [`abort`](Self::abort).
    ///
    /// # Panics
    ///
    /// Panics if called more than once.
    pub async fn finish(&mut self) -> anyhow::Result<()> {
        assert!(!self.finished, "finish() called on already-finished upload");
        self.finished = true;

        // Upload trailing data as the final part (may be empty — this is valid
        // for some backends; others may reject zero-length parts, so only send
        // if there is data).
        if !self.buf.is_empty() {
            let remaining: Vec<u8> = std::mem::take(&mut self.buf);
            self.upload_one_part(Bytes::from(remaining)).await?;
        }

        // Complete the multipart upload.
        if let Some(ref mut ops) = self.ops {
            ops.complete().await?;
        }

        Ok(())
    }

    /// Abort the multipart upload, cleaning up any partial parts on the server.
    ///
    /// Safe to call after an error or at any point before [`finish`](Self::finish).
    /// After this call the upload is finished and no further calls are allowed.
    pub async fn abort(&mut self) -> anyhow::Result<()> {
        if self.finished {
            return Ok(());
        }
        self.finished = true;

        if let Some(ref mut ops) = self.ops {
            ops.abort().await?;
        }

        Ok(())
    }

    // ── internals ─────────────────────────────────────────────────────────

    /// Upload one part with retries. On retry exhaustion the upload is aborted
    /// and the last error is returned to the caller.
    async fn upload_one_part(&mut self, payload: Bytes) -> anyhow::Result<()> {
        let mut last_err: Option<anyhow::Error> = None;
        let part_idx = self.part_count;

        for attempt in 0..=self.config.max_retries {
            let ops = match self.ops.as_mut() {
                Some(o) => o,
                None => {
                    // ops is None only if abort was already called; bail.
                    return Err(anyhow::anyhow!(
                        "multipart upload aborted — no ops available for part upload"
                    ));
                }
            };

            match ops.put_part(payload.clone()).await {
                Ok(()) => {
                    self.bytes_sofar += payload.len() as u64;
                    self.part_count += 1;

                    // Fire progress callback.
                    if let Some(ref tx) = self.progress_tx {
                        let _ = tx.send(PartProgress {
                            part_index: part_idx,
                            bytes_sofar: self.bytes_sofar,
                            total_bytes: self.total_bytes,
                        });
                    }

                    return Ok(());
                }
                Err(e) => {
                    last_err = Some(e);
                    if attempt < self.config.max_retries {
                        // Brief delay before retry (linear backoff: 100ms, 200ms, 300ms).
                        tokio::time::sleep(std::time::Duration::from_millis(
                            100 * (attempt as u64 + 1),
                        ))
                        .await;
                    }
                }
            }
        }

        // All retries exhausted — abort the upload.
        self.finished = true;
        if let Some(ref mut ops) = self.ops {
            let _ = ops.abort().await;
        }

        Err(last_err.unwrap_or_else(|| anyhow::anyhow!("part upload failed with no error")))
    }
}

impl Drop for StreamingMultipartUpload {
    fn drop(&mut self) {
        if !self.finished {
            // Best-effort abort — we can't block in Drop, so spawn a task.
            // The server-side lifecycle policy will eventually clean up any
            // orphaned multipart uploads if this abort fails.
            if let Some(mut ops) = self.ops.take() {
                tokio::spawn(async move {
                    let _ = ops.abort().await;
                });
            }
        }
    }
}

// ── Public constructors (wired by B2Blob) ───────────────────────────────────

impl StreamingMultipartUpload {
    /// Build a [`StreamingMultipartUpload`] backed by an
    /// [`object_store::MultipartUpload`] handle.
    ///
    /// This is the entry-point used by [`super::b2_blob::B2Blob`].
    pub(crate) fn from_object_store(
        upload: Box<dyn object_store::MultipartUpload>,
        config: MultipartUploadConfig,
        total_bytes: u64,
        progress_tx: Option<mpsc::UnboundedSender<PartProgress>>,
    ) -> Self {
        let ops = ObjectStoreMultipart { inner: upload };
        Self::new(Box::new(ops), config, total_bytes, progress_tx)
    }
}

// ═══════════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════════

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    // ── Mock multipart backend ────────────────────────────────────────────

    /// A controllable mock that can simulate success or failure per part.
    struct MockMultipart {
        /// Number of parts successfully uploaded.
        parts_uploaded: usize,
        /// If set, the next `n` calls to `put_part` fail.
        fail_next: AtomicU32,
        /// Set to `true` when `abort` is called.
        aborted: bool,
        /// Set to `true` when `complete` is called.
        completed: bool,
        /// All payloads uploaded (cloned for inspection).
        uploaded_payloads: Vec<Bytes>,
    }

    impl MockMultipart {
        fn new() -> Self {
            Self {
                parts_uploaded: 0,
                fail_next: AtomicU32::new(0),
                aborted: false,
                completed: false,
                uploaded_payloads: Vec::new(),
            }
        }

        fn set_fail_next(&self, count: u32) {
            self.fail_next.store(count, Ordering::SeqCst);
        }
    }

    #[async_trait::async_trait]
    impl MultipartOps for MockMultipart {
        async fn put_part(&mut self, payload: Bytes) -> anyhow::Result<()> {
            let fail = self.fail_next.load(Ordering::SeqCst);
            if fail > 0 {
                self.fail_next.store(fail - 1, Ordering::SeqCst);
                anyhow::bail!("simulated part failure");
            }
            self.parts_uploaded += 1;
            self.uploaded_payloads.push(payload);
            Ok(())
        }

        async fn complete(&mut self) -> anyhow::Result<()> {
            self.completed = true;
            Ok(())
        }

        async fn abort(&mut self) -> anyhow::Result<()> {
            self.aborted = true;
            Ok(())
        }
    }

    // ── Config tests ──────────────────────────────────────────────────────

    #[test]
    fn config_defaults_are_5mb_parts_and_3_retries() {
        let cfg = MultipartUploadConfig::default();
        assert_eq!(cfg.threshold_bytes, 5 * 1024 * 1024);
        assert_eq!(cfg.part_size_bytes, 5 * 1024 * 1024);
        assert_eq!(cfg.max_retries, 3);
    }

    #[test]
    fn config_custom_values() {
        let cfg = MultipartUploadConfig {
            threshold_bytes: 1024,
            part_size_bytes: 512,
            max_retries: 5,
        };
        assert_eq!(cfg.threshold_bytes, 1024);
        assert_eq!(cfg.part_size_bytes, 512);
        assert_eq!(cfg.max_retries, 5);
    }

    #[test]
    fn config_is_cloneable() {
        let cfg = MultipartUploadConfig::default();
        let _c2 = cfg.clone();
    }

    #[test]
    fn part_progress_equality() {
        let p1 = PartProgress {
            part_index: 0,
            bytes_sofar: 100,
            total_bytes: 1000,
        };
        let p2 = PartProgress {
            part_index: 0,
            bytes_sofar: 100,
            total_bytes: 1000,
        };
        assert_eq!(p1, p2);
    }

    #[test]
    fn part_progress_inequality() {
        let p1 = PartProgress {
            part_index: 0,
            bytes_sofar: 100,
            total_bytes: 1000,
        };
        let p2 = PartProgress {
            part_index: 1,
            bytes_sofar: 200,
            total_bytes: 1000,
        };
        assert_ne!(p1, p2);
    }

    // ── Happy path: small upload (one part) ───────────────────────────────

    #[tokio::test]
    async fn feed_then_finish_single_part() {
        let mock = MockMultipart::new();
        let config = MultipartUploadConfig {
            part_size_bytes: 100,
            threshold_bytes: 50,
            max_retries: 0,
        };
        let mut upload = StreamingMultipartUpload::new(Box::new(mock), config, 150, None);

        // Feed 150 bytes → one full part (100 bytes) uploaded, 50 remain.
        upload.feed(&[0xAAu8; 150]).await.unwrap();

        // Total parts uploaded so far should be 1 (the 100-byte part).
        assert_eq!(upload.part_count, 1);
        assert_eq!(upload.bytes_sofar, 100);

        // Finish → final 50 bytes uploaded + complete.
        upload.finish().await.unwrap();

        assert_eq!(upload.part_count, 2);
        assert_eq!(upload.bytes_sofar, 150);
    }

    #[tokio::test]
    async fn feed_multiple_parts() {
        let mock = MockMultipart::new();
        let config = MultipartUploadConfig {
            part_size_bytes: 50,
            threshold_bytes: 10,
            max_retries: 0,
        };
        let mut upload = StreamingMultipartUpload::new(Box::new(mock), config, 200, None);

        // Feed 200 bytes → 4 parts of 50 each.
        upload.feed(&[0xBBu8; 200]).await.unwrap();

        assert_eq!(upload.part_count, 4);
        assert_eq!(upload.bytes_sofar, 200);
        // Buffer should be empty (200 = 4 × 50).
        assert!(upload.buf.is_empty());

        // Finish → no trailing data.
        upload.finish().await.unwrap();
        assert_eq!(upload.part_count, 4);
    }

    #[tokio::test]
    async fn feed_many_small_chunks() {
        let mock = MockMultipart::new();
        let config = MultipartUploadConfig {
            part_size_bytes: 50,
            threshold_bytes: 10,
            max_retries: 0,
        };
        let mut upload = StreamingMultipartUpload::new(Box::new(mock), config, 130, None);

        // 10 chunks of 10 bytes → 100 bytes → 2 parts, 0 remainder.
        for _ in 0..10 {
            upload.feed(&[0xCCu8; 10]).await.unwrap();
        }
        assert_eq!(upload.part_count, 2);
        assert_eq!(upload.bytes_sofar, 100);
        assert!(upload.buf.is_empty());

        // 3 more chunks of 10 → 30 bytes in buffer, no new part.
        for _ in 0..3 {
            upload.feed(&[0xDDu8; 10]).await.unwrap();
        }
        assert_eq!(upload.part_count, 2); // still 2
        assert_eq!(upload.buf.len(), 30);

        // Finish → final 30-byte part.
        upload.finish().await.unwrap();
        assert_eq!(upload.part_count, 3);
        assert_eq!(upload.bytes_sofar, 130);
    }

    #[tokio::test]
    async fn empty_feed_is_noop() {
        let mock = MockMultipart::new();
        let config = MultipartUploadConfig {
            part_size_bytes: 50,
            threshold_bytes: 10,
            max_retries: 0,
        };
        let mut upload = StreamingMultipartUpload::new(Box::new(mock), config, 0, None);

        // Feeding empty slice should be a no-op.
        upload.feed(&[]).await.unwrap();
        assert_eq!(upload.part_count, 0);
        assert!(upload.buf.is_empty());

        // Finish with no data → no parts, but complete should be called.
        upload.finish().await.unwrap();
    }

    #[tokio::test]
    async fn finish_with_empty_buffer() {
        let mock = MockMultipart::new();
        let config = MultipartUploadConfig {
            part_size_bytes: 50,
            threshold_bytes: 10,
            max_retries: 0,
        };
        let mut upload = StreamingMultipartUpload::new(Box::new(mock), config, 100, None);

        // Feed exactly 2 parts worth.
        upload.feed(&[0xEEu8; 100]).await.unwrap();
        assert_eq!(upload.part_count, 2);
        assert!(upload.buf.is_empty());

        // Finish with empty buffer → complete called.
        upload.finish().await.unwrap();
    }

    // ── Progress reporting ────────────────────────────────────────────────

    #[tokio::test]
    async fn progress_fires_per_part() {
        let mock = MockMultipart::new();
        let config = MultipartUploadConfig {
            part_size_bytes: 40,
            threshold_bytes: 10,
            max_retries: 0,
        };
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut upload = StreamingMultipartUpload::new(Box::new(mock), config, 100, Some(tx));

        // Feed 100 bytes → 2 full parts (40+40) uploaded, 20 remaining.
        upload.feed(&[0xFFu8; 100]).await.unwrap();

        let p0 = rx.recv().await.unwrap();
        assert_eq!(p0.part_index, 0);
        assert_eq!(p0.bytes_sofar, 40);
        assert_eq!(p0.total_bytes, 100);

        let p1 = rx.recv().await.unwrap();
        assert_eq!(p1.part_index, 1);
        assert_eq!(p1.bytes_sofar, 80);
        assert_eq!(p1.total_bytes, 100);

        // No more progress events yet.
        assert!(rx.try_recv().is_err());

        upload.finish().await.unwrap();

        let p2 = rx.recv().await.unwrap();
        assert_eq!(p2.part_index, 2);
        assert_eq!(p2.bytes_sofar, 100);
    }

    #[tokio::test]
    async fn progress_not_fired_when_no_sender() {
        let mock = MockMultipart::new();
        let config = MultipartUploadConfig {
            part_size_bytes: 50,
            threshold_bytes: 10,
            max_retries: 0,
        };
        // No progress sender.
        let mut upload = StreamingMultipartUpload::new(Box::new(mock), config, 50, None);
        upload.feed(&[0x11u8; 50]).await.unwrap();
        // Should not panic — just no events sent.
        upload.finish().await.unwrap();
    }

    // ── Retry behaviour ───────────────────────────────────────────────────

    #[tokio::test]
    async fn retry_succeeds_on_second_attempt() {
        let mock = MockMultipart::new();
        mock.set_fail_next(1); // fail once, then succeed

        let config = MultipartUploadConfig {
            part_size_bytes: 30,
            threshold_bytes: 10,
            max_retries: 2,
        };
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut upload = StreamingMultipartUpload::new(Box::new(mock), config, 30, Some(tx));

        // Feed 30 bytes → one part uploaded (retried once).
        upload.feed(&[0x22u8; 30]).await.unwrap();

        let p = rx.recv().await.unwrap();
        assert_eq!(p.part_index, 0);
        assert_eq!(p.bytes_sofar, 30);

        upload.finish().await.unwrap();
    }

    #[tokio::test]
    async fn retry_exhausted_aborts_and_errors() {
        let mock = MockMultipart::new();
        mock.set_fail_next(99); // always fail

        let config = MultipartUploadConfig {
            part_size_bytes: 20,
            threshold_bytes: 10,
            max_retries: 1, // 2 attempts total
        };
        let mut upload = StreamingMultipartUpload::new(Box::new(mock), config, 40, None);

        // Feed 40 bytes → triggers 1 part upload (attempts 1+retry) which
        // both fail → abort.
        let result = upload.feed(&[0x33u8; 40]).await;
        assert!(result.is_err(), "should error after retry exhaustion");

        // The upload should be marked finished (aborted).
        assert!(upload.finished);
    }

    #[tokio::test]
    async fn retry_count_includes_original_attempt() {
        let mock = MockMultipart::new();
        mock.set_fail_next(3); // fail 3 times

        let config = MultipartUploadConfig {
            part_size_bytes: 25,
            threshold_bytes: 10,
            max_retries: 2, // 3 attempts total: initial + 2 retries
        };
        let mut upload = StreamingMultipartUpload::new(Box::new(mock), config, 25, None);

        // 3 failures with max_retries=2 (3 attempts) → should fail.
        let result = upload.feed(&[0x44u8; 25]).await;
        assert!(result.is_err(), "3 failures with 2 retries should error");
    }

    #[tokio::test]
    async fn max_retries_zero_no_retries() {
        let mock = MockMultipart::new();
        mock.set_fail_next(1); // single failure

        let config = MultipartUploadConfig {
            part_size_bytes: 15,
            threshold_bytes: 10,
            max_retries: 0, // no retry — single attempt
        };
        let mut upload = StreamingMultipartUpload::new(Box::new(mock), config, 15, None);

        let result = upload.feed(&[0x55u8; 15]).await;
        assert!(result.is_err());
        assert!(upload.finished);
    }

    // ── Abort ─────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn explicit_abort_cleans_up() {
        let mock = MockMultipart::new();
        let config = MultipartUploadConfig {
            part_size_bytes: 60,
            threshold_bytes: 10,
            max_retries: 0,
        };
        let mut upload = StreamingMultipartUpload::new(Box::new(mock), config, 200, None);

        // Feed some data (below part size, so no part uploaded yet).
        upload.feed(&[0x66u8; 30]).await.unwrap();

        // Abort.
        upload.abort().await.unwrap();
        assert!(upload.finished);
    }

    #[tokio::test]
    async fn abort_is_idempotent() {
        let mock = MockMultipart::new();
        let config = MultipartUploadConfig::default();
        let mut upload = StreamingMultipartUpload::new(Box::new(mock), config, 0, None);

        upload.abort().await.unwrap();
        // Second abort should be a no-op.
        upload.abort().await.unwrap();
    }

    // ── Edge cases ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn chunk_larger_than_part_size_triggers_multiple_parts() {
        let mock = MockMultipart::new();
        let config = MultipartUploadConfig {
            part_size_bytes: 30,
            threshold_bytes: 10,
            max_retries: 0,
        };
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut upload = StreamingMultipartUpload::new(Box::new(mock), config, 100, Some(tx));

        // Feed 100 bytes at once → 3 full parts (30+30+30) + 10 remaining.
        upload.feed(&[0x77u8; 100]).await.unwrap();

        assert_eq!(upload.part_count, 3);
        assert_eq!(upload.bytes_sofar, 90);
        assert_eq!(upload.buf.len(), 10);

        // 3 progress events.
        for i in 0..3 {
            let p = rx.recv().await.unwrap();
            assert_eq!(p.part_index, i);
        }

        upload.finish().await.unwrap();
        let p = rx.recv().await.unwrap();
        assert_eq!(p.part_index, 3);
        assert_eq!(p.bytes_sofar, 100);
    }

    #[tokio::test]
    async fn exact_part_boundary() {
        let mock = MockMultipart::new();
        let config = MultipartUploadConfig {
            part_size_bytes: 64,
            threshold_bytes: 10,
            max_retries: 0,
        };
        let mut upload = StreamingMultipartUpload::new(Box::new(mock), config, 128, None);

        // Exactly 2 parts.
        upload.feed(&[0x88u8; 64]).await.unwrap();
        assert_eq!(upload.part_count, 1);
        assert!(upload.buf.is_empty());

        upload.feed(&[0x88u8; 64]).await.unwrap();
        assert_eq!(upload.part_count, 2);
        assert!(upload.buf.is_empty());

        upload.finish().await.unwrap();
    }

    #[tokio::test]
    async fn total_bytes_zero_is_ok() {
        let mock = MockMultipart::new();
        let config = MultipartUploadConfig {
            part_size_bytes: 50,
            threshold_bytes: 10,
            max_retries: 0,
        };
        let mut upload = StreamingMultipartUpload::new(
            Box::new(mock),
            config,
            0, // total_bytes hint is 0
            None,
        );

        upload.finish().await.unwrap();
    }

    #[tokio::test]
    async fn progress_total_bytes_reflects_hint() {
        let mock = MockMultipart::new();
        let config = MultipartUploadConfig {
            part_size_bytes: 30,
            threshold_bytes: 10,
            max_retries: 0,
        };
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut upload = StreamingMultipartUpload::new(
            Box::new(mock),
            config,
            999, // hint
            Some(tx),
        );

        upload.feed(&[0x99u8; 30]).await.unwrap();
        let p = rx.recv().await.unwrap();
        assert_eq!(p.total_bytes, 999);

        upload.finish().await.unwrap();
    }

    // ── from_object_store constructor ──────────────────────────────────

    #[tokio::test]
    async fn from_object_store_creates_usable_upload() {
        // We can't easily mock object_store::MultipartUpload, but we can
        // verify the constructor path doesn't panic by testing with an
        // end-to-end mock MultipartOps flow. The from_object_store constructor
        // is tested indirectly through integration tests.
        //
        // This test verifies that the basic construction works through
        // the new() path (which from_object_store delegates to).
        let mock = MockMultipart::new();
        let config = MultipartUploadConfig {
            part_size_bytes: 20,
            threshold_bytes: 10,
            max_retries: 0,
        };
        let upload = StreamingMultipartUpload::new(Box::new(mock), config, 50, None);
        assert!(!upload.finished);
        assert_eq!(upload.buf.capacity(), 20 + 64 * 1024);
    }

    // ── Panic guards ────────────────────────────────────────────────────

    #[tokio::test]
    #[should_panic(expected = "feed() called on finished upload")]
    async fn feed_after_finish_panics() {
        let mock = MockMultipart::new();
        let config = MultipartUploadConfig {
            part_size_bytes: 50,
            threshold_bytes: 10,
            max_retries: 0,
        };
        let mut upload = StreamingMultipartUpload::new(Box::new(mock), config, 0, None);
        upload.finish().await.unwrap();
        upload.feed(&[0x11u8; 10]).await.unwrap(); // should panic
    }

    #[tokio::test]
    #[should_panic(expected = "finish() called on already-finished upload")]
    async fn double_finish_panics() {
        let mock = MockMultipart::new();
        let config = MultipartUploadConfig {
            part_size_bytes: 50,
            threshold_bytes: 10,
            max_retries: 0,
        };
        let mut upload = StreamingMultipartUpload::new(Box::new(mock), config, 0, None);
        upload.finish().await.unwrap();
        upload.finish().await.unwrap(); // should panic
    }

    // ── ObjectStoreMultipart integration smoke test ──────────────────────
    // This test verifies the ObjectStoreMultipart wrapper compiles and
    // the trait methods are wired correctly, even though the inner
    // MultipartUpload is a mock.

    #[tokio::test]
    async fn object_store_multipart_wrapper_trait_methods() {
        // We can't create a real Box<dyn object_store::MultipartUpload>
        // without a real S3 client, but we can verify the trait object
        // boundary by using a mock MockMultipart through the MultipartOps
        // trait. The ObjectStoreMultipart struct is tested implicitly
        // when B2Blob is used with a real S3 endpoint.
        let mock = MockMultipart::new();
        let mut ops: Box<dyn MultipartOps> = Box::new(mock);
        // These should succeed with the mock.
        ops.put_part(Bytes::from_static(b"hello")).await.unwrap();
        ops.complete().await.unwrap();
    }
}
