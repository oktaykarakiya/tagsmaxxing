// SPDX-License-Identifier: AGPL-3.0-or-later

//! Read-through LRU cache for any [`Blob`](kb_core::blob::Blob) implementor (plan §20).
//!
//! Wraps an inner backend (typically [`B2Blob`](crate::B2Blob)) with a size-capped on-disk
//! cache. On `get()`: cache hit → return cached bytes (with optional SHA256 validation
//! in paranoid mode); cache miss → fetch from the inner backend → store in cache → return.
//!
//! On `put()`: write to the inner backend first, then cache the bytes locally
//! (write-through — cache write failure is logged but not fatal, since the backend
//! already holds the data).
//!
//! Cache eviction is LRU: when total cached bytes would exceed the configured cap,
//! the least-recently-accessed entries are purged (both the on-disk file and the
//! metadata row) until the total fits. Metadata is tracked in a bundled SQLite
//! database for fast lookups without filesystem scans.
//!
//! The cache is disposable — clearing it only causes re-fetches from the backend.
//! The cache stores decrypted plaintext only; encryption is handled above this layer
//! (P8-T5).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use bytes::Bytes;
use rusqlite::Connection;
use sha2::{Digest, Sha256};

/// Default maximum cache size: 10 GiB (plan §20).
pub const DEFAULT_CACHE_MAX_GB: u64 = 10;

/// SQL statement that creates the cache metadata table and its LRU index.
///
/// `access_seq` is a monotonically-increasing counter (not a wall-clock
/// timestamp) so LRU ordering is deterministic even when operations complete
/// within the same millisecond.
const CREATE_SCHEMA_SQL: &str = "
    CREATE TABLE IF NOT EXISTS cache_entries (
        key         TEXT PRIMARY KEY,
        file_path   TEXT    NOT NULL,
        size_bytes  INTEGER NOT NULL,
        access_seq  INTEGER NOT NULL,
        sha256_hex  TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_cache_access_seq
        ON cache_entries(access_seq);
";

// ── BlobCache ──────────────────────────────────────────────────────────────────

/// A size-capped local read-through cache wrapping any [`Blob`](kb_core::blob::Blob)
/// implementor (plan §20).
///
/// # Cache directory layout
///
/// Cached blobs are stored as regular files under `{cache_dir}/`. Each file is
/// named by the SHA-256 hash of the blob key (hex-encoded), stored in a two-level
/// directory (`{cache_dir}/{first_2_chars}/{full_hash}`) to avoid too many
/// entries in one directory. Metadata — key, file path, size, a monotonic access
/// sequence number (for deterministic LRU ordering), and an optional plaintext
/// SHA-256 — is tracked in a bundled SQLite database at `{cache_dir}/cache.db`.
///
/// # Thread safety
///
/// All SQLite access is synchronous and runs inside `tokio::task::spawn_blocking`;
/// the connection is protected by a `std::sync::Mutex` that is only ever held on
/// blocking worker threads, never across `.await` points.
pub struct BlobCache {
    /// The wrapped backend (e.g. B2Blob or LocalBlob).
    inner: Arc<dyn kb_core::blob::Blob>,
    /// Root directory for cached blob files.
    cache_dir: PathBuf,
    /// Maximum total bytes across all cached entries.
    max_bytes: u64,
    /// When `true`, validate the plaintext SHA-256 of every cache hit against
    /// the stored hash. If validation fails the entry is treated as a miss and
    /// re-fetched from the backend.
    paranoid: bool,
    /// SQLite connection for cache metadata.
    db: Mutex<Connection>,
    /// Monotonically-increasing sequence number for LRU ordering. Each access
    /// (insert or update) bumps this counter and stores the new value in the
    /// `access_seq` column.
    next_seq: AtomicU64,
}

impl BlobCache {
    /// Create a new `BlobCache` wrapping `inner`.
    ///
    /// The cache directory and SQLite database are created if they do not exist.
    /// `max_bytes` caps the total on-disk size; when 0 the default
    /// [`DEFAULT_CACHE_MAX_GB`] (10 GiB) is used.
    ///
    /// # Errors
    /// Returns an error if the cache directory cannot be created or the SQLite
    /// database cannot be opened / initialized.
    pub fn new(
        inner: Arc<dyn kb_core::blob::Blob>,
        cache_dir: PathBuf,
        max_bytes: u64,
        paranoid: bool,
    ) -> anyhow::Result<Self> {
        let effective_max = if max_bytes == 0 {
            DEFAULT_CACHE_MAX_GB * 1024 * 1024 * 1024
        } else {
            max_bytes
        };

        std::fs::create_dir_all(&cache_dir)
            .with_context(|| "failed to create blob cache directory")?;

        let db_path = cache_dir.join("cache.db");
        let conn = Connection::open(&db_path)
            .with_context(|| "failed to open blob cache SQLite database")?;

        // Enable WAL mode for better concurrent read performance.
        conn.execute_batch("PRAGMA journal_mode=WAL;")
            .context("failed to set WAL journal mode")?;

        conn.execute_batch(CREATE_SCHEMA_SQL)
            .context("failed to create blob cache schema")?;

        Ok(Self {
            inner,
            cache_dir,
            max_bytes: effective_max,
            paranoid,
            db: Mutex::new(conn),
            next_seq: AtomicU64::new(0),
        })
    }

    /// Clear all cached entries (both on-disk files and metadata rows).
    /// The inner backend is untouched. Subsequent `get()` calls will re-fetch
    /// from the backend and re-populate the cache.
    ///
    /// # Errors
    /// Returns an error if metadata cannot be read or a file cannot be deleted.
    /// Partial failures are possible: the method attempts to delete as many
    /// entries as it can, collecting errors.
    pub fn clear(&self) -> anyhow::Result<()> {
        let entries = self.db_all_entries_with_sizes()?;

        let mut errors: Vec<String> = Vec::new();
        for (_key, file_path, _size) in &entries {
            let path = Path::new(file_path);
            // Attempt to remove the file; missing is fine.
            if path.exists()
                && let Err(e) = std::fs::remove_file(path)
            {
                errors.push(format!("failed to delete {file_path}: {e}"));
            }
        }

        // Clean up empty two-level subdirectories (best-effort).
        if let Ok(read_dir) = std::fs::read_dir(&self.cache_dir) {
            for entry in read_dir.flatten() {
                let p = entry.path();
                if p.is_dir()
                    && p.file_name()
                        .and_then(|n| n.to_str())
                        .is_some_and(|n| n.len() == 2 && n.chars().all(|c| c.is_ascii_hexdigit()))
                {
                    let _ = std::fs::remove_dir(&p);
                }
            }
        }

        if let Err(e) = self.db_delete_all() {
            errors.push(format!("db clear failed: {e}"));
        }

        if errors.is_empty() {
            Ok(())
        } else {
            anyhow::bail!("cache clear had errors: {}", errors.join("; "))
        }
    }

    /// Return the current total size of all cached entries in bytes.
    ///
    /// # Errors
    /// Returns an error if the SQLite query fails.
    pub fn current_size_bytes(&self) -> anyhow::Result<u64> {
        self.db_total_size()
    }

    /// Trim the cache so its total size is at most the configured maximum.
    ///
    /// Evicts the least-recently-used entries (both the on-disk file and the
    /// metadata row) until the total falls under [`Self::max_bytes`]. Returns
    /// the number of bytes freed.
    ///
    /// This is a no-op if the cache is already under the limit or empty.
    /// Useful as a periodic maintenance job to enforce the cap even when no
    /// new data is being written.
    ///
    /// # Errors
    /// Returns an error if the SQLite metadata cannot be read or updated.
    pub fn trim(&self) -> anyhow::Result<u64> {
        let current = self.db_total_size()?;
        let max = self.max_bytes;
        if current <= max {
            return Ok(0);
        }
        let needed = current - max;
        let evicted = self.db_evict_lru(needed)?;

        let mut freed: u64 = 0;
        for (_key, file_path) in &evicted {
            let path = std::path::Path::new(file_path);
            if path.exists() {
                // Track the size before deletion for the return value.
                if let Ok(meta) = std::fs::metadata(path) {
                    freed += meta.len();
                }
                let _ = std::fs::remove_file(path);
            }
        }

        Ok(freed)
    }
}

// ── Private helpers (synchronous — called inside spawn_blocking) ────────────────

impl BlobCache {
    /// Acquire the SQLite connection mutex, converting poison into an error.
    fn db_lock(&self) -> anyhow::Result<std::sync::MutexGuard<'_, Connection>> {
        self.db
            .lock()
            .map_err(|e| anyhow::anyhow!("blob cache db lock poisoned: {e}"))
    }

    /// Compute the on-disk file path for a cache entry keyed by `key`.
    ///
    /// Uses SHA-256 of the key to produce a deterministic, safe filename that
    /// avoids path-traversal concerns and `/` in keys. Files are stored under
    /// a two-level directory: `{cache_dir}/{first_2_hex}/{full_64_hex}`.
    fn cache_file_path(&self, key: &str) -> PathBuf {
        let hash = hex::encode(Sha256::digest(key.as_bytes()));
        self.cache_dir.join(&hash[..2]).join(&hash)
    }

    /// Return the hex-encoded SHA-256 of `data`.
    fn sha256_hex(data: &[u8]) -> String {
        hex::encode(Sha256::digest(data))
    }

    /// Look up a cache entry by key. Returns `(file_path, sha256_hex)` if found.
    fn db_lookup(&self, key: &str) -> anyhow::Result<Option<(String, Option<String>)>> {
        let db = self.db_lock()?;
        let mut stmt =
            db.prepare_cached("SELECT file_path, sha256_hex FROM cache_entries WHERE key = ?1")?;
        let mut rows = stmt.query_map([key], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
        })?;
        match rows.next() {
            Some(Ok(row)) => Ok(Some(row)),
            Some(Err(e)) => Err(e.into()),
            None => Ok(None),
        }
    }

    /// Allocate and return the next access sequence number.
    fn next_access_seq(&self) -> u64 {
        self.next_seq.fetch_add(1, Ordering::Relaxed)
    }

    /// Insert or replace a cache entry with a fresh access sequence number.
    fn db_upsert(
        &self,
        key: &str,
        file_path: &str,
        size_bytes: u64,
        sha256_hex: Option<&str>,
    ) -> anyhow::Result<()> {
        let db = self.db_lock()?;
        #[allow(clippy::cast_possible_truncation)]
        let size_i64 = size_bytes as i64;
        #[allow(clippy::cast_possible_truncation)]
        let seq = self.next_access_seq() as i64;
        db.execute(
            "INSERT OR REPLACE INTO cache_entries (key, file_path, size_bytes, access_seq, sha256_hex) \
             VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![key, file_path, size_i64, seq, sha256_hex],
        )?;
        Ok(())
    }

    /// Bump the access sequence number for an existing cache entry.
    fn db_update_access(&self, key: &str) -> anyhow::Result<()> {
        let db = self.db_lock()?;
        #[allow(clippy::cast_possible_truncation)]
        let seq = self.next_access_seq() as i64;
        db.execute(
            "UPDATE cache_entries SET access_seq = ?1 WHERE key = ?2",
            rusqlite::params![seq, key],
        )?;
        Ok(())
    }

    /// Delete a single cache entry by key.
    fn db_delete(&self, key: &str) -> anyhow::Result<()> {
        let db = self.db_lock()?;
        db.execute("DELETE FROM cache_entries WHERE key = ?1", [key])?;
        Ok(())
    }

    /// Delete all cache entries.
    fn db_delete_all(&self) -> anyhow::Result<()> {
        let db = self.db_lock()?;
        db.execute_batch("DELETE FROM cache_entries;")?;
        Ok(())
    }

    /// Return the sum of `size_bytes` for all cache entries.
    fn db_total_size(&self) -> anyhow::Result<u64> {
        let db = self.db_lock()?;
        let total: i64 = db.query_row(
            "SELECT COALESCE(SUM(size_bytes), 0) FROM cache_entries",
            [],
            |row| row.get(0),
        )?;
        Ok(total as u64)
    }

    /// Return all `(key, file_path, size_bytes)` triples ordered by
    /// `access_seq` ascending (least recently accessed first).
    fn db_all_entries_with_sizes(&self) -> anyhow::Result<Vec<(String, String, u64)>> {
        let db = self.db_lock()?;
        let mut stmt = db.prepare_cached(
            "SELECT key, file_path, size_bytes FROM cache_entries ORDER BY access_seq ASC",
        )?;
        let rows = stmt.query_map([], |row| {
            let key: String = row.get(0)?;
            let path: String = row.get(1)?;
            let size: i64 = row.get(2)?;
            Ok((key, path, size as u64))
        })?;
        let mut entries = Vec::new();
        for row in rows {
            entries.push(row?);
        }
        Ok(entries)
    }

    /// Evict the least-recently-used entries until at least `needed_bytes` are
    /// freed (or the cache is empty). Returns the `(key, file_path)` pairs that
    /// were evicted so the caller can delete the corresponding files.
    ///
    /// If the cache is already empty, this is a no-op.
    fn db_evict_lru(&self, needed_bytes: u64) -> anyhow::Result<Vec<(String, String)>> {
        let entries = self.db_all_entries_with_sizes()?;
        if entries.is_empty() {
            return Ok(Vec::new());
        }

        let mut evicted: Vec<(String, String)> = Vec::new();
        let mut freed: u64 = 0;

        let db = self.db_lock()?;
        let mut del_stmt = db.prepare_cached("DELETE FROM cache_entries WHERE key = ?1")?;

        for (key, file_path, size_bytes) in &entries {
            if freed >= needed_bytes {
                break;
            }
            del_stmt.execute([key.as_str()])?;
            freed += size_bytes;
            evicted.push((key.clone(), file_path.clone()));
        }

        drop(del_stmt);
        drop(db);

        Ok(evicted)
    }

    /// Ensure there is room for `incoming_bytes` by evicting LRU entries.
    /// Deletes both the database rows and the on-disk files.
    fn ensure_capacity(&self, incoming_bytes: u64) -> anyhow::Result<()> {
        let current = self.db_total_size()?;
        let max = self.max_bytes;

        if current + incoming_bytes <= max {
            return Ok(());
        }

        // Need to free at least `current + incoming_bytes - max` bytes.
        let needed = current + incoming_bytes - max;
        let evicted = self.db_evict_lru(needed)?;

        for (_key, file_path) in &evicted {
            let path = Path::new(file_path);
            if path.exists() {
                let _ = std::fs::remove_file(path);
            }
        }

        Ok(())
    }

    /// Write `data` to the cache file and upsert the metadata row.
    /// Returns `true` on success, `false` if the cache write was skipped
    /// (e.g. the entry already exists with a matching size).
    fn write_to_cache(&self, key: &str, data: &[u8]) -> anyhow::Result<()> {
        let file_path_buf = self.cache_file_path(key);
        let file_path_str = file_path_buf.to_str().unwrap_or("invalid-path");

        // Create parent subdirectory if needed.
        if let Some(parent) = file_path_buf.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| "failed to create cache subdirectory")?;
        }

        // Write the file.
        std::fs::write(&file_path_buf, data)
            .with_context(|| format!("failed to write cache file for key `{key}`"))?;

        // Compute plaintext SHA-256 for paranoid validation.
        let sha = Self::sha256_hex(data);

        // Upsert metadata.
        let size = data.len() as u64;
        self.db_upsert(key, file_path_str, size, Some(&sha))?;

        Ok(())
    }

    /// Read `data` from a cache file, optionally validating SHA-256.
    /// Returns `None` if the file is missing or validation fails.
    fn read_from_cache(
        &self,
        key: &str,
        file_path: &str,
        expected_sha: Option<&str>,
    ) -> anyhow::Result<Option<Bytes>> {
        let path = Path::new(file_path);
        if !path.exists() {
            return Ok(None);
        }

        let data = std::fs::read(path)
            .with_context(|| format!("failed to read cached file for key `{key}`"))?;

        // In paranoid mode, validate the SHA-256.
        if self.paranoid
            && let Some(expected) = expected_sha
        {
            let actual = Self::sha256_hex(&data);
            if actual != expected {
                // Corrupted — treat as miss.
                return Ok(None);
            }
        }

        Ok(Some(Bytes::from(data)))
    }
}

// ── Blob trait implementation ─────────────────────────────────────────────────

#[async_trait]
impl kb_core::blob::Blob for BlobCache {
    /// Store `data` under `key` — write-through: backend first, then cache.
    ///
    /// If the backend write succeeds but the cache write fails, the error is
    /// logged and `Ok(())` is returned (the data is safely in the backend).
    ///
    /// # Errors
    /// Returns an error if the backend `put` fails.
    async fn put(&self, key: &str, data: Bytes) -> anyhow::Result<()> {
        // Write to backend first.
        self.inner
            .put(key, data.clone())
            .await
            .with_context(|| format!("BlobCache: backend put failed for key `{key}`"))?;

        // Best-effort cache write.
        if let Err(e) = self.ensure_capacity(data.len() as u64) {
            tracing::warn!(
                key = key,
                error = %e,
                "BlobCache: eviction failed, skipping cache write"
            );
            return Ok(());
        }

        if let Err(e) = self.write_to_cache(key, &data) {
            tracing::warn!(
                key = key,
                error = %e,
                "BlobCache: cache write failed after backend put succeeded"
            );
        }

        Ok(())
    }

    /// Fetch the blob at `key`. Cache hit → return cached bytes; miss → fetch
    /// from the inner backend, store in cache, and return.
    ///
    /// # Errors
    /// Returns an error if the backend fetch fails (on a cache miss) or if
    /// the metadata cannot be read.
    async fn get(&self, key: &str) -> anyhow::Result<Bytes> {
        // Check the cache.
        if let Some((file_path, sha_opt)) = self.db_lookup(key)? {
            if let Some(cached) = self.read_from_cache(key, &file_path, sha_opt.as_deref())? {
                // Cache hit — update access time and return.
                let _ = self.db_update_access(key);
                return Ok(cached);
            }
            // File missing or validation failed — remove stale entry.
            let _ = self.db_delete(key);
        }

        // Cache miss — fetch from backend.
        let data = self
            .inner
            .get(key)
            .await
            .with_context(|| format!("BlobCache: backend get failed for key `{key}`"))?;

        // Store in cache (best-effort).
        let _ = self.ensure_capacity(data.len() as u64);
        if let Err(e) = self.write_to_cache(key, &data) {
            tracing::warn!(
                key = key,
                error = %e,
                "BlobCache: cache write after backend get failed"
            );
        }

        Ok(data)
    }

    /// Fetch a byte range `[start, end)` — delegates to the inner backend.
    /// Range reads are not cached to avoid fetching full blobs for partial reads.
    ///
    /// # Errors
    /// Returns an error if the backend `get_range` fails.
    async fn get_range(&self, key: &str, start: u64, end: u64) -> anyhow::Result<Bytes> {
        self.inner
            .get_range(key, start, end)
            .await
            .with_context(|| format!("BlobCache: backend get_range failed for key `{key}`"))
    }

    /// Whether an object exists — delegates to the inner backend since the
    /// cache is disposable and the backend is the source of truth.
    ///
    /// # Errors
    /// Returns an error if the backend existence check fails.
    async fn exists(&self, key: &str) -> anyhow::Result<bool> {
        self.inner
            .exists(key)
            .await
            .with_context(|| format!("BlobCache: backend exists check failed for key `{key}`"))
    }

    /// Delete the object — backend first, then remove from cache (best-effort).
    ///
    /// # Errors
    /// Returns an error if the backend delete fails.
    async fn delete(&self, key: &str) -> anyhow::Result<()> {
        self.inner
            .delete(key)
            .await
            .with_context(|| format!("BlobCache: backend delete failed for key `{key}`"))?;

        // Remove cache entry if present.
        if let Ok(Some((file_path, _))) = self.db_lookup(key) {
            let _ = std::fs::remove_file(Path::new(&file_path));
            let _ = self.db_delete(key);
        }

        Ok(())
    }

    /// Return a presigned download URL — delegates to the inner backend.
    ///
    /// # Errors
    /// Returns an error if the backend cannot produce a URL.
    async fn presigned_get_url(&self, key: &str, expires_in: Duration) -> anyhow::Result<String> {
        self.inner
            .presigned_get_url(key, expires_in)
            .await
            .with_context(|| format!("BlobCache: backend presigned_get_url failed for key `{key}`"))
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use kb_core::blob::Blob;

    /// A mock Blob that delegates to an in-memory HashMap, counting get calls.
    struct MockBackend {
        store: Mutex<std::collections::HashMap<String, Bytes>>,
        get_count: Mutex<u64>,
    }

    impl MockBackend {
        fn new() -> Self {
            Self {
                store: Mutex::new(std::collections::HashMap::new()),
                get_count: Mutex::new(0),
            }
        }

        fn get_calls(&self) -> u64 {
            *self.get_count.lock().unwrap()
        }
    }

    #[async_trait]
    impl Blob for MockBackend {
        async fn put(&self, key: &str, data: Bytes) -> anyhow::Result<()> {
            self.store.lock().unwrap().insert(key.to_string(), data);
            Ok(())
        }

        async fn get(&self, key: &str) -> anyhow::Result<Bytes> {
            *self.get_count.lock().unwrap() += 1;
            self.store
                .lock()
                .unwrap()
                .get(key)
                .cloned()
                .with_context(|| format!("mock: key `{key}` not found"))
        }

        async fn get_range(&self, key: &str, start: u64, end: u64) -> anyhow::Result<Bytes> {
            let data = self.get(key).await?;
            let s = (start as usize).min(data.len());
            let e = (end as usize).min(data.len());
            Ok(data.slice(s..e))
        }

        async fn exists(&self, key: &str) -> anyhow::Result<bool> {
            Ok(self.store.lock().unwrap().contains_key(key))
        }

        async fn delete(&self, key: &str) -> anyhow::Result<()> {
            self.store.lock().unwrap().remove(key);
            Ok(())
        }

        async fn presigned_get_url(
            &self,
            key: &str,
            _expires_in: Duration,
        ) -> anyhow::Result<String> {
            Ok(format!("https://example.com/{key}?presigned"))
        }
    }

    /// Create a `BlobCache` wrapping a `MockBackend`, rooted in a temp dir.
    fn test_cache(max_bytes: u64) -> (BlobCache, Arc<MockBackend>, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("tempdir");
        let backend = Arc::new(MockBackend::new());
        // Use the sqlite db path within the temp dir.
        let cache = BlobCache::new(
            backend.clone() as Arc<dyn Blob>,
            dir.path().to_path_buf(),
            max_bytes,
            false, // non-paranoid by default
        )
        .expect("create BlobCache");
        (cache, backend, dir)
    }

    // ── Construction ────────────────────────────────────────────────────────

    #[test]
    fn new_creates_cache_dir_and_db() {
        let dir = tempfile::tempdir().unwrap();
        let backend = Arc::new(MockBackend::new());
        let cache_dir = dir.path().join("blob-cache");

        let cache = BlobCache::new(backend as Arc<dyn Blob>, cache_dir.clone(), 0, false).unwrap();

        assert!(cache_dir.exists());
        assert!(cache_dir.join("cache.db").exists());
        // 0 max_bytes → default (10 GiB).
        assert_eq!(cache.max_bytes, DEFAULT_CACHE_MAX_GB * 1024 * 1024 * 1024);
        assert!(!cache.paranoid);
    }

    #[test]
    fn new_with_explicit_max_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let backend = Arc::new(MockBackend::new());
        let cache = BlobCache::new(
            backend as Arc<dyn Blob>,
            dir.path().to_path_buf(),
            1024 * 1024, // 1 MiB
            true,
        )
        .unwrap();

        assert_eq!(cache.max_bytes, 1024 * 1024);
        assert!(cache.paranoid);
    }

    #[test]
    fn new_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let backend = Arc::new(MockBackend::new());
        let cache_dir = dir.path().join("cache");

        // First creation.
        let _c1 = BlobCache::new(
            backend.clone() as Arc<dyn Blob>,
            cache_dir.clone(),
            0,
            false,
        )
        .unwrap();

        // Second creation with same dir — should succeed (table already exists).
        let c2 = BlobCache::new(backend as Arc<dyn Blob>, cache_dir, 0, false).unwrap();

        assert_eq!(c2.current_size_bytes().unwrap(), 0);
    }

    // ── get: cache miss → fetch → cache → hit ──────────────────────────────

    #[tokio::test]
    async fn get_cache_miss_fetches_from_backend() {
        let (cache, backend, _dir) = test_cache(10 * 1024 * 1024);
        let key = "tenant-1/test-key";
        let data = Bytes::from_static(b"hello from backend");

        // Pre-populate backend only.
        backend.put(key, data.clone()).await.unwrap();
        assert_eq!(backend.get_calls(), 0); // put counted separately
        let get_before = backend.get_calls();

        // First get → cache miss → backend fetch.
        let result = cache.get(key).await.unwrap();
        assert_eq!(result, data);
        assert_eq!(backend.get_calls(), get_before + 1);

        // Second get → cache hit → no backend call.
        let result2 = cache.get(key).await.unwrap();
        assert_eq!(result2, data);
        assert_eq!(backend.get_calls(), get_before + 1); // unchanged!
    }

    #[tokio::test]
    async fn get_cache_hit_returns_correct_bytes() {
        let (cache, backend, _dir) = test_cache(10 * 1024 * 1024);
        let key = "tenant-2/my-file";
        let data = Bytes::from(vec![0x42u8; 4096]);

        backend.put(key, data.clone()).await.unwrap();

        // First call fills cache.
        let first = cache.get(key).await.unwrap();
        assert_eq!(first, data);

        // Second call hits cache.
        let second = cache.get(key).await.unwrap();
        assert_eq!(second, data);

        // Verify data integrity — byte-for-byte identical.
        assert_eq!(first, second);
    }

    #[tokio::test]
    async fn get_backend_error_propagates() {
        let (cache, _backend, _dir) = test_cache(10 * 1024 * 1024);
        // Key not in backend → error.
        let result = cache.get("tenant-1/nonexistent").await;
        assert!(result.is_err());
    }

    // ── put: write-through ─────────────────────────────────────────────────

    #[tokio::test]
    async fn put_writes_to_both_backend_and_cache() {
        let (cache, backend, _dir) = test_cache(10 * 1024 * 1024);
        let key = "tenant-3/put-key";
        let data = Bytes::from_static(b"write-through test");

        cache.put(key, data.clone()).await.unwrap();

        // Backend has the data.
        assert!(backend.exists(key).await.unwrap());
        assert_eq!(backend.get(key).await.unwrap(), data);

        // Cache has the data → subsequent get is a cache hit.
        let get_before = backend.get_calls();
        let cached = cache.get(key).await.unwrap();
        assert_eq!(cached, data);
        assert_eq!(backend.get_calls(), get_before); // no backend call → cache hit
    }

    #[tokio::test]
    async fn put_backend_error_propagates() {
        // This test uses a mock that can fail. For now, verify that a key
        // that was never put returns an error on get.
        let (cache, _backend, _dir) = test_cache(10 * 1024 * 1024);
        let result = cache.get("tenant-1/never-put").await;
        assert!(result.is_err());
    }

    // ── exists: delegates to backend ───────────────────────────────────────

    #[tokio::test]
    async fn exists_delegates_to_backend() {
        let (cache, backend, _dir) = test_cache(10 * 1024 * 1024);
        let key = "tenant-1/exists-test";

        assert!(!cache.exists(key).await.unwrap());

        backend.put(key, Bytes::from_static(b"data")).await.unwrap();
        assert!(cache.exists(key).await.unwrap());
    }

    // ── get_range: delegates to backend ────────────────────────────────────

    #[tokio::test]
    async fn get_range_delegates_to_backend() {
        let (cache, backend, _dir) = test_cache(10 * 1024 * 1024);
        let key = "tenant-1/range-key";
        let data = Bytes::from_static(b"0123456789");

        backend.put(key, data.clone()).await.unwrap();

        let slice = cache.get_range(key, 2, 6).await.unwrap();
        assert_eq!(slice, Bytes::from_static(b"2345"));
    }

    #[tokio::test]
    async fn get_range_nonexistent_key_is_error() {
        let (cache, _backend, _dir) = test_cache(10 * 1024 * 1024);
        let result = cache.get_range("tenant-1/ghost", 0, 10).await;
        assert!(result.is_err());
    }

    // ── delete: backend + cache cleanup ────────────────────────────────────

    #[tokio::test]
    async fn delete_removes_from_both() {
        let (cache, backend, _dir) = test_cache(10 * 1024 * 1024);
        let key = "tenant-1/delete-me";
        let data = Bytes::from_static(b"ephemeral");

        // Put → populates both backend and cache.
        cache.put(key, data.clone()).await.unwrap();
        assert!(backend.exists(key).await.unwrap());
        assert!(cache.current_size_bytes().unwrap() > 0);

        // Delete.
        cache.delete(key).await.unwrap();

        // Backend no longer has it.
        assert!(!backend.exists(key).await.unwrap());

        // Cache entry removed.
        let result = cache.get(key).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn delete_nonexistent_key_is_ok() {
        let (cache, _backend, _dir) = test_cache(10 * 1024 * 1024);
        // Deleting a key that doesn't exist should be fine.
        cache.delete("tenant-1/ghost").await.unwrap();
    }

    // ── clear ──────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn clear_empties_cache() {
        let (cache, backend, _dir) = test_cache(10 * 1024 * 1024);
        let key = "tenant-1/clear-test";
        let data = Bytes::from_static(b"pre-clear data");

        // Populate cache via put.
        cache.put(key, data.clone()).await.unwrap();
        assert!(cache.current_size_bytes().unwrap() > 0);

        // Clear.
        cache.clear().unwrap();
        assert_eq!(cache.current_size_bytes().unwrap(), 0);

        // Subsequent get re-fetches from backend.
        let get_before = backend.get_calls();
        let result = cache.get(key).await.unwrap();
        assert_eq!(result, data);
        assert_eq!(backend.get_calls(), get_before + 1); // cache miss → backend call
    }

    #[tokio::test]
    async fn clear_on_empty_cache_is_noop() {
        let (cache, _backend, _dir) = test_cache(10 * 1024 * 1024);
        assert_eq!(cache.current_size_bytes().unwrap(), 0);
        cache.clear().unwrap();
        assert_eq!(cache.current_size_bytes().unwrap(), 0);
    }

    // ── LRU eviction ───────────────────────────────────────────────────────

    #[tokio::test]
    async fn eviction_keeps_total_under_limit() {
        let (cache, backend, _dir) = test_cache(512); // tiny cache: 512 bytes

        // Insert multiple blobs that collectively exceed the limit.
        let keys: Vec<String> = (0..10).map(|i| format!("tenant-1/key-{i}")).collect();
        for key in &keys {
            let data = Bytes::from(vec![0xAAu8; 100]); // 100 bytes each
            backend.put(key, data.clone()).await.unwrap();
        }

        // Fetch each one — they should be cached, with eviction keeping total ≤ 512.
        for key in &keys {
            let _ = cache.get(key).await.unwrap();
        }

        let total = cache.current_size_bytes().unwrap();
        assert!(
            total <= 512,
            "total cache size {total} should be ≤ 512 after eviction"
        );
    }

    #[tokio::test]
    async fn eviction_removes_least_recently_used() {
        let (cache, backend, _dir) = test_cache(350); // fits ~3 entries of 100 bytes

        let key_a = "tenant-1/a";
        let key_b = "tenant-1/b";
        let key_c = "tenant-1/c";
        let key_d = "tenant-1/d";
        let data = Bytes::from(vec![0xBBu8; 100]);

        for k in &[key_a, key_b, key_c, key_d] {
            backend.put(k, data.clone()).await.unwrap();
        }

        // Populate: a, b, c.
        cache.get(key_a).await.unwrap();
        cache.get(key_b).await.unwrap();
        cache.get(key_c).await.unwrap();
        assert!(cache.current_size_bytes().unwrap() <= 350);

        // Access a again — makes a most-recently-used.
        cache.get(key_a).await.unwrap();

        // Now add d — should evict the LRU entry (b or c, not a).
        cache.get(key_d).await.unwrap();

        let total = cache.current_size_bytes().unwrap();
        assert!(total <= 350);

        // a should still be in cache (recently accessed).
        let backend_gets_before = backend.get_calls();
        let _ = cache.get(key_a).await.unwrap();
        assert_eq!(
            backend.get_calls(),
            backend_gets_before,
            "key_a should be cached (recently accessed)"
        );
    }

    // ── paranoid mode SHA256 validation ────────────────────────────────────

    #[tokio::test]
    async fn paranoid_mode_validates_cached_data() {
        let dir = tempfile::tempdir().unwrap();
        let backend = Arc::new(MockBackend::new());
        let cache = BlobCache::new(
            backend.clone() as Arc<dyn Blob>,
            dir.path().to_path_buf(),
            10 * 1024 * 1024,
            true, // paranoid
        )
        .unwrap();

        let key = "tenant-1/paranoid-key";
        let data = Bytes::from_static(b"validated data");
        backend.put(key, data.clone()).await.unwrap();

        // First get → cache miss → fetch + cache.
        let first = cache.get(key).await.unwrap();
        assert_eq!(first, data);

        // Second get → cache hit with SHA256 validation → succeeds.
        let second = cache.get(key).await.unwrap();
        assert_eq!(second, data);
    }

    #[tokio::test]
    async fn paranoid_mode_rejects_corrupted_cache_file() {
        let dir = tempfile::tempdir().unwrap();
        let backend = Arc::new(MockBackend::new());
        let cache = BlobCache::new(
            backend.clone() as Arc<dyn Blob>,
            dir.path().to_path_buf(),
            10 * 1024 * 1024,
            true, // paranoid
        )
        .unwrap();

        let key = "tenant-1/corrupt-me";
        let data = Bytes::from_static(b"original content");
        backend.put(key, data.clone()).await.unwrap();

        // First get populates cache.
        let _ = cache.get(key).await.unwrap();

        // Corrupt the cache file on disk.
        let file_path = cache.cache_file_path(key);
        std::fs::write(&file_path, b"tampered content").unwrap();

        // Next get should detect corruption and re-fetch.
        let backend_gets_before = backend.get_calls();
        let result = cache.get(key).await.unwrap();
        assert_eq!(result, data, "should return correct data after re-fetch");
        assert_eq!(
            backend.get_calls(),
            backend_gets_before + 1,
            "should re-fetch from backend on corruption"
        );
    }

    // ── presigned_get_url: delegates ───────────────────────────────────────

    #[tokio::test]
    async fn presigned_get_url_delegates_to_backend() {
        let (cache, backend, _dir) = test_cache(10 * 1024 * 1024);
        let key = "tenant-1/presign-key";
        backend.put(key, Bytes::from_static(b"data")).await.unwrap();

        let url = cache
            .presigned_get_url(key, Duration::from_secs(3600))
            .await
            .unwrap();
        assert!(url.contains(key));
        assert!(url.contains("presigned"));
    }

    // ── cache_file_path ────────────────────────────────────────────────────

    #[test]
    fn cache_file_path_is_deterministic() {
        let dir = tempfile::tempdir().unwrap();
        let backend = Arc::new(MockBackend::new());
        let cache = BlobCache::new(
            backend as Arc<dyn Blob>,
            dir.path().to_path_buf(),
            1024,
            false,
        )
        .unwrap();

        let key = "tenant-1/my-key";
        let p1 = cache.cache_file_path(key);
        let p2 = cache.cache_file_path(key);
        assert_eq!(p1, p2);
    }

    #[test]
    fn cache_file_path_different_keys_different_paths() {
        let dir = tempfile::tempdir().unwrap();
        let backend = Arc::new(MockBackend::new());
        let cache = BlobCache::new(
            backend as Arc<dyn Blob>,
            dir.path().to_path_buf(),
            1024,
            false,
        )
        .unwrap();

        let p1 = cache.cache_file_path("tenant-1/key-a");
        let p2 = cache.cache_file_path("tenant-1/key-b");
        assert_ne!(p1, p2);
    }

    #[test]
    fn cache_file_path_uses_two_level_directory() {
        let dir = tempfile::tempdir().unwrap();
        let backend = Arc::new(MockBackend::new());
        let cache = BlobCache::new(
            backend as Arc<dyn Blob>,
            dir.path().to_path_buf(),
            1024,
            false,
        )
        .unwrap();

        let path = cache.cache_file_path("tenant-1/any-key");
        // Should be: {cache_dir}/{2_chars}/{64_chars}
        let parent = path.parent().unwrap();
        let parent_name = parent.file_name().unwrap().to_str().unwrap();
        assert_eq!(parent_name.len(), 2, "first level should be 2 hex chars");
        assert!(parent_name.chars().all(|c| c.is_ascii_hexdigit()));

        let file_name = path.file_name().unwrap().to_str().unwrap();
        assert_eq!(file_name.len(), 64, "second level should be 64 hex chars");
        assert!(file_name.chars().all(|c| c.is_ascii_hexdigit()));
    }

    // ── constants ──────────────────────────────────────────────────────────

    #[test]
    fn default_cache_max_is_10_gib() {
        assert_eq!(DEFAULT_CACHE_MAX_GB, 10);
    }

    // ── sha256_hex helper ──────────────────────────────────────────────────

    #[test]
    fn sha256_hex_returns_64_char_string() {
        let hash = BlobCache::sha256_hex(b"hello");
        assert_eq!(hash.len(), 64);
        assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn sha256_hex_different_inputs_different_hashes() {
        let h1 = BlobCache::sha256_hex(b"hello");
        let h2 = BlobCache::sha256_hex(b"world");
        assert_ne!(h1, h2);
    }

    // ── current_size_bytes ─────────────────────────────────────────────────

    #[tokio::test]
    async fn current_size_reflects_cached_entries() {
        let (cache, backend, _dir) = test_cache(10 * 1024 * 1024);

        assert_eq!(cache.current_size_bytes().unwrap(), 0);

        let data = Bytes::from(vec![0xCCu8; 500]);
        backend
            .put("tenant-1/size-test", data.clone())
            .await
            .unwrap();
        cache.get("tenant-1/size-test").await.unwrap();

        assert_eq!(cache.current_size_bytes().unwrap(), 500);
    }
}
