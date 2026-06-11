// SPDX-License-Identifier: AGPL-3.0-or-later

//! The `Blob` capability — content-addressed object storage (plan §13, §20).

use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;

/// Object storage for file blobs, keyed by content address (`blob_key = hex(sha256)`,
/// tenant-prefixed). Backblaze B2 is the production impl; a local-disk impl is kept for
/// dev/tests (plan §20). Blobs are encrypted client-side before they reach a backend.
#[async_trait]
pub trait Blob: Send + Sync {
    /// Store `data` under `key`. Idempotent: writing identical content to the same
    /// content-addressed key is a no-op.
    ///
    /// # Errors
    /// Returns an error if the write fails.
    async fn put(&self, key: &str, data: Bytes) -> anyhow::Result<()>;

    /// Fetch the full blob at `key`.
    ///
    /// # Errors
    /// Returns an error if the object is missing or the read fails.
    async fn get(&self, key: &str) -> anyhow::Result<Bytes>;

    /// Fetch a byte range `[start, end)` of the blob at `key` (for media range requests).
    ///
    /// # Errors
    /// Returns an error if the object is missing or the read fails.
    async fn get_range(&self, key: &str, start: u64, end: u64) -> anyhow::Result<Bytes>;

    /// Whether an object exists at `key`.
    ///
    /// # Errors
    /// Returns an error if the existence check fails (distinct from a definitive "absent").
    async fn exists(&self, key: &str) -> anyhow::Result<bool>;

    /// Delete the object at `key`.
    ///
    /// # Errors
    /// Returns an error if the delete fails.
    async fn delete(&self, key: &str) -> anyhow::Result<()>;

    /// A short-lived URL for direct client download, valid for `expires_in`. Backends without
    /// native presigning may return an app-proxied URL instead (plan §20).
    ///
    /// # Errors
    /// Returns an error if a URL cannot be produced.
    async fn presigned_get_url(&self, key: &str, expires_in: Duration) -> anyhow::Result<String>;
}
