// SPDX-License-Identifier: AGPL-3.0-or-later

//! Envelope encryption wrapper for any [`kb_core::blob::Blob`] backend (plan §20, §24, §28).
//!
//! [`EncryptedBlob`] wraps an inner `Blob` implementation and transparently encrypts
//! data on `put` / decrypts on `get`, using per-file random Data Encryption Keys (DEKs)
//! that are themselves wrapped by a [`KeyEncryptionKey`](kb_core::kek::KeyEncryptionKey)
//! (KEK). The inner backend never sees plaintext.
//!
//! # Binary format (version 1)
//!
//! Each stored blob has a fixed 76-byte header followed by the encrypted payload:
//!
//! ```text
//! [0..3)    MAGIC              b"EBl"
//! [3]       VERSION            u8 (1)
//! [4..16)   wrapped_dek_nonce  [u8; 12]
//! [16..64)  wrapped_dek        [u8; 48]  (AES-256-GCM: ciphertext || auth-tag)
//! [64..76)  payload_nonce      [u8; 12]
//! [76..)    payload            AES-256-GCM ciphertext with appended 16-byte tag
//! ```
//!
//! # Security properties
//!
//! - **DEK uniqueness:** A fresh 256-bit random DEK is generated for every `put` call.
//!   Two calls with identical plaintext produce different ciphertexts in storage
//!   (different DEK + different nonce), so the backend sees no content-hash collisions.
//!   Deduplication is by plaintext SHA-256, which happens *before* encryption.
//! - **Nonce freshness:** Every AES-256-GCM operation (both KEK-wrapping and DEK-encryption)
//!   uses a unique random 96-bit nonce from the OS CSPRNG.
//! - **Wrong KEK → error:** If the KEK changes (or the wrapped DEK is corrupted), `get`
//!   returns an error — never garbage plaintext.
//! - **Presigned URLs are NOT supported:** The encrypted blob has no meaningful presigned
//!   URL because the client cannot decrypt; `presigned_get_url` returns an error.
//!   Use server-side decryption via `get` / `get_range` instead.

use std::sync::Arc;
use std::time::Duration;

use aes_gcm::aead::Aead;
use aes_gcm::{Aes256Gcm, KeyInit};
use anyhow::Context;
use async_trait::async_trait;
use bytes::Bytes;
use rand::TryRng;
use rand::rngs::SysRng;

use kb_core::blob::Blob;
use kb_core::kek::KeyEncryptionKey;

// ── Format constants ───────────────────────────────────────────────────────────

/// Magic bytes identifying an encrypted blob: `b"EBl"`.
const MAGIC: [u8; 3] = *b"EBl";

/// Current format version.
const VERSION: u8 = 1;

/// AES-256-GCM nonce size: 12 bytes (96 bits).
const NONCE_SIZE: usize = 12;

/// AES-256 key size: 32 bytes (256 bits).
const KEY_SIZE: usize = 32;

/// AES-256-GCM authentication tag size: 16 bytes.
const TAG_SIZE: usize = 16;

/// Size of the wrapped DEK block in the header:
/// `nonce(12) || ciphertext(KEY_SIZE + TAG_SIZE = 48)` = 60 bytes.
const WRAPPED_DEK_BLOCK: usize = NONCE_SIZE + KEY_SIZE + TAG_SIZE;

/// Total header size: `magic(3) + version(1) + wrapped_dek_block(60) +
/// payload_nonce(12)` = 76 bytes.
///
/// Note: the wrapped DEK block already includes its own 12-byte nonce
/// (it is the full output of [`KeyEncryptionKey::wrap`]).
const HEADER_SIZE: usize = 3 + 1 + WRAPPED_DEK_BLOCK + NONCE_SIZE;

// Offsets within the header.
const OFF_VERSION: usize = 3;
/// The wrapped DEK block (nonce || ciphertext || tag) starts right after magic+version.
const OFF_WRAPPED_DEK: usize = 4;
/// The payload nonce starts after the wrapped DEK block.
const OFF_PAYLOAD_NONCE: usize = OFF_WRAPPED_DEK + WRAPPED_DEK_BLOCK; // 64

// ── Free crypto helpers (shared with LocalBlob for optional encryption) ──────

/// Returns `true` if `raw` starts with the encrypted blob magic bytes (`EBl`).
#[must_use]
pub(crate) fn is_envelope(raw: &[u8]) -> bool {
    raw.len() >= 3 && raw[..3] == MAGIC
}

/// Encrypt `plaintext` with a fresh random DEK, wrap the DEK with `kek`.
///
/// Returns `(wrapped_dek_blob, encrypted_payload)` where:
/// - `wrapped_dek_blob` is `nonce(12) || ciphertext(32) || tag(16)` = 60 bytes.
/// - `encrypted_payload` is `nonce(12) || ciphertext || tag`.
pub(crate) async fn seal(
    kek: &dyn KeyEncryptionKey,
    plaintext: &[u8],
) -> anyhow::Result<(Vec<u8>, Vec<u8>)> {
    // 1. Generate random 256-bit DEK.
    let mut dek = [0u8; KEY_SIZE];
    SysRng
        .try_fill_bytes(&mut dek)
        .map_err(|e| anyhow::anyhow!("failed to generate DEK: {e}"))?;

    // 2. Encrypt plaintext with the DEK (AES-256-GCM).
    let dek_cipher = Aes256Gcm::new_from_slice(&dek)
        .map_err(|e| anyhow::anyhow!("failed to initialise AES-256-GCM for DEK: {e}"))?;

    let mut payload_nonce = [0u8; NONCE_SIZE];
    SysRng
        .try_fill_bytes(&mut payload_nonce)
        .map_err(|e| anyhow::anyhow!("failed to generate payload nonce: {e}"))?;
    let pnonce = aes_gcm::Nonce::from_slice(&payload_nonce);

    let payload_ct = dek_cipher
        .encrypt(pnonce, plaintext)
        .map_err(|e| anyhow::anyhow!("DEK encryption failed: {e}"))?;

    // Payload block: nonce || ciphertext+tag
    let mut payload = Vec::with_capacity(NONCE_SIZE + payload_ct.len());
    payload.extend_from_slice(&payload_nonce);
    payload.extend_from_slice(&payload_ct);

    // 3. Wrap the DEK with the KEK.
    let wrapped = kek.wrap(&dek).await.context("KEK wrap failed")?;
    // wrapped = nonce(12) || ciphertext(32) || tag(16) = 60 bytes
    debug_assert_eq!(
        wrapped.len(),
        NONCE_SIZE + KEY_SIZE + TAG_SIZE,
        "wrapped DEK must be 60 bytes"
    );

    Ok((wrapped, payload))
}

/// Decrypt a sealed envelope (header + ciphertext) using `kek`.
///
/// Returns the plaintext on success.
pub(crate) async fn open(kek: &dyn KeyEncryptionKey, raw: &[u8]) -> anyhow::Result<Bytes> {
    anyhow::ensure!(
        raw.len() >= HEADER_SIZE,
        "encrypted blob too short: {} bytes (need at least {HEADER_SIZE})",
        raw.len()
    );

    // Validate magic + version.
    anyhow::ensure!(
        raw[..3] == MAGIC,
        "not an encrypted blob (bad magic bytes: {:02x?})",
        &raw[..3]
    );
    let version = raw[OFF_VERSION];
    anyhow::ensure!(
        version == VERSION,
        "unsupported encrypted blob version {version} (expected {VERSION})"
    );

    // Extract header fields.
    let wrapped_dek = &raw[OFF_WRAPPED_DEK..OFF_PAYLOAD_NONCE];
    let payload_nonce = &raw[OFF_PAYLOAD_NONCE..HEADER_SIZE];
    let payload_ct_and_tag = &raw[HEADER_SIZE..];

    // 1. Unwrap the DEK with the KEK.
    let dek = kek
        .unwrap(wrapped_dek)
        .await
        .context("KEK unwrap failed (wrong key or corrupted data)")?;

    anyhow::ensure!(
        dek.len() == KEY_SIZE,
        "unwrapped DEK has wrong size: {} (expected {KEY_SIZE})",
        dek.len()
    );

    // 2. Decrypt payload with the DEK.
    let mut dek_arr = [0u8; KEY_SIZE];
    dek_arr.copy_from_slice(&dek);

    let dek_cipher = Aes256Gcm::new_from_slice(&dek_arr)
        .map_err(|e| anyhow::anyhow!("failed to initialise AES-256-GCM for DEK decrypt: {e}"))?;

    let pnonce = aes_gcm::Nonce::from_slice(payload_nonce);
    let plaintext = dek_cipher
        .decrypt(pnonce, payload_ct_and_tag)
        .map_err(|e| anyhow::anyhow!("DEK decryption failed (corrupted data): {e}"))?;

    Ok(Bytes::from(plaintext))
}

/// Assemble the full binary envelope: header + encrypted payload.
#[must_use]
pub(crate) fn assemble(wrapped_dek: &[u8], encrypted_payload: &[u8]) -> Vec<u8> {
    // wrapped_dek: nonce(12) || ciphertext(32) || tag(16) = 60 bytes total
    debug_assert_eq!(wrapped_dek.len(), WRAPPED_DEK_BLOCK);

    let mut blob = Vec::with_capacity(HEADER_SIZE + encrypted_payload.len());
    blob.extend_from_slice(&MAGIC);
    blob.push(VERSION);
    // The wrapped_dek already includes its own nonce prefix.
    blob.extend_from_slice(wrapped_dek);
    // encrypted_payload already includes its nonce prefix.
    blob.extend_from_slice(encrypted_payload);
    blob
}

// ── EncryptedBlob ──────────────────────────────────────────────────────────────

/// An envelope-encrypting wrapper around any [`Blob`] implementation.
///
/// On `put`, the plaintext is encrypted with a fresh random DEK (AES-256-GCM); the
/// DEK is wrapped with the configured [`KeyEncryptionKey`]. The wrapped DEK, nonces,
/// and ciphertext are packed into a self-describing binary format and stored via the
/// inner `Blob`. On `get`, the reverse happens — the DEK is unwrapped and the
/// plaintext is recovered.
///
/// This wrapper works with any backend that implements [`Blob`]: B2 (production),
/// LocalBlob (dev/test), or any future backend.
///
/// # Limitations
///
/// - `get_range` decrypts the *entire* blob before slicing, because AES-256-GCM is
///   not seekable. For large objects where range reads are expected, consider a
///   chunked-encryption scheme.
/// - `presigned_get_url` returns an error — the backend stores ciphertext, so a
///   direct URL would be useless to a client without the KEK.
pub struct EncryptedBlob<B: ?Sized + Blob> {
    /// The inner (unwrapped) blob backend where encrypted bytes are stored.
    inner: Arc<B>,
    /// The Key Encryption Key used to wrap/unwrap per-file DEKs.
    kek: Arc<dyn KeyEncryptionKey>,
}

impl<B: ?Sized + Blob> EncryptedBlob<B> {
    /// Create a new encrypted blob wrapper.
    ///
    /// `inner` is the backend where ciphertext will be stored (e.g. a
    /// [`B2Blob`](crate::B2Blob) or [`LocalBlob`](crate::LocalBlob)).
    /// `kek` is the master key that wraps per-file DEKs.
    #[must_use]
    pub fn new(inner: Arc<B>, kek: Arc<dyn KeyEncryptionKey>) -> Self {
        Self { inner, kek }
    }

    /// Return a reference to the inner blob backend.
    #[must_use]
    pub fn inner(&self) -> &Arc<B> {
        &self.inner
    }

    /// Return a reference to the KEK.
    #[must_use]
    pub fn kek(&self) -> &Arc<dyn KeyEncryptionKey> {
        &self.kek
    }

    // ── Internal helpers ──────────────────────────────────────────────────

    /// Encrypt `plaintext` with a freshly generated random DEK.
    ///
    /// Returns `(wrapped_dek_blob, encrypted_payload)` where:
    /// - `wrapped_dek_blob` is `wrapped_dek_nonce || wrapped_dek_ciphertext+tag`
    ///   (60 bytes total).
    /// - `encrypted_payload` is `payload_nonce || ciphertext+tag`.
    async fn encrypt(&self, plaintext: &[u8]) -> anyhow::Result<(Vec<u8>, Vec<u8>)> {
        seal(self.kek.as_ref(), plaintext).await
    }

    /// Parse the binary header and decrypt the payload.
    ///
    /// Returns the plaintext bytes on success.
    async fn decrypt(&self, raw: &[u8]) -> anyhow::Result<Bytes> {
        open(self.kek.as_ref(), raw).await
    }

    /// Assemble the full binary blob: header + encrypted payload.
    fn assemble_blob(wrapped_dek: &[u8], encrypted_payload: &[u8]) -> Vec<u8> {
        assemble(wrapped_dek, encrypted_payload)
    }
}

// ── Blob trait implementation ──────────────────────────────────────────────────

#[async_trait]
impl<B: ?Sized + Blob> Blob for EncryptedBlob<B> {
    /// Store `data` encrypted under a fresh random DEK, with the DEK wrapped by the KEK.
    ///
    /// The inner backend receives the full encrypted package (header + ciphertext).
    /// Idempotency is preserved at the *plaintext* level in practice because the
    /// caller hashes plaintext before reaching this layer (content-addressed keys
    /// are assigned before encryption, per plan §20).
    ///
    /// # Errors
    /// Returns an error if crypto or the inner backend's `put` fails.
    async fn put(&self, key: &str, data: Bytes) -> anyhow::Result<()> {
        let (wrapped_dek, encrypted_payload) = self
            .encrypt(&data)
            .await
            .with_context(|| format!("encryption failed for key `{key}`"))?;

        let blob = Self::assemble_blob(&wrapped_dek, &encrypted_payload);
        self.inner
            .put(key, Bytes::from(blob))
            .await
            .with_context(|| format!("inner put failed for encrypted key `{key}`"))?;

        Ok(())
    }

    /// Fetch and decrypt the full blob at `key`.
    ///
    /// # Errors
    /// Returns an error if the object is missing, the crypto fails (wrong KEK,
    /// corrupted data), or the inner backend's `get` fails.
    async fn get(&self, key: &str) -> anyhow::Result<Bytes> {
        let raw = self
            .inner
            .get(key)
            .await
            .with_context(|| format!("inner get failed for encrypted key `{key}`"))?;

        self.decrypt(&raw)
            .await
            .with_context(|| format!("decryption failed for key `{key}`"))
    }

    /// Fetch a byte range of the plaintext at `key`.
    ///
    /// **Performance note:** AES-256-GCM is not seekable, so this decrypts the
    /// **entire** blob and then slices the requested range. For very large objects
    /// where range reads are frequent, consider a chunked-encryption scheme at the
    /// application layer instead.
    ///
    /// # Errors
    /// Returns an error if the object is missing, the crypto fails, or the
    /// inner backend's `get` fails.
    async fn get_range(&self, key: &str, start: u64, end: u64) -> anyhow::Result<Bytes> {
        let plaintext = self.get(key).await?;
        let s = start as usize;
        let e = (end as usize).min(plaintext.len());

        // Allow [0, 0) on empty blobs.
        if s == 0 && e == 0 && plaintext.is_empty() {
            return Ok(Bytes::new());
        }

        anyhow::ensure!(
            s < plaintext.len() && e <= plaintext.len() && s <= e,
            "invalid range [{start},{end}) for blob `{key}` (length {})",
            plaintext.len()
        );

        Ok(plaintext.slice(s..e))
    }

    /// Delegate to the inner backend.
    async fn exists(&self, key: &str) -> anyhow::Result<bool> {
        self.inner.exists(key).await
    }

    /// Delegate to the inner backend.
    async fn delete(&self, key: &str) -> anyhow::Result<()> {
        self.inner.delete(key).await
    }

    /// Encrypted blobs cannot produce a meaningful presigned URL because the
    /// backend stores ciphertext and the client has no access to the KEK.
    ///
    /// Callers should use [`Self::get`] to decrypt server-side instead.
    ///
    /// # Errors
    /// Always returns an error with a descriptive message.
    async fn presigned_get_url(&self, _key: &str, _expires_in: Duration) -> anyhow::Result<String> {
        anyhow::bail!(
            "presigned_get_url is not supported for encrypted blobs: \
             the backend stores ciphertext and the client cannot decrypt it; \
             use server-side get() instead"
        )
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::sync::Arc;

    use kb_core::kek::FileKek;

    use super::*;

    /// A test KEK with a known 32-byte key.
    fn test_kek() -> Arc<dyn KeyEncryptionKey> {
        Arc::new(FileKek::from_key([0xABu8; 32]))
    }

    /// A different KEK for wrong-key tests.
    fn wrong_kek() -> Arc<dyn KeyEncryptionKey> {
        Arc::new(FileKek::from_key([0xCDu8; 32]))
    }

    // ── Construction ──────────────────────────────────────────────────────

    #[test]
    fn new_constructs() {
        let inner = Arc::new(MockBlob::default());
        let kek = test_kek();
        let enc = EncryptedBlob::new(Arc::clone(&inner), Arc::clone(&kek));
        assert!(Arc::ptr_eq(enc.inner(), &inner));
    }

    #[test]
    fn new_with_different_backend() {
        // EncryptedBlob is generic over B — confirm it works with different
        // concrete types via the trait object pattern.
        let inner1: Arc<dyn Blob> = Arc::new(MockBlob::default());
        let kek = test_kek();
        let _enc = EncryptedBlob::new(Arc::clone(&inner1), Arc::clone(&kek));
        // The struct compiles and holds the references.
    }

    // ── put + get round-trip ─────────────────────────────────────────────

    #[tokio::test]
    async fn put_get_roundtrip_small() {
        let mock = Arc::new(MockBlob::default());
        let enc = EncryptedBlob::new(Arc::clone(&mock), test_kek());

        let data = Bytes::from_static(b"hello encrypted world");
        enc.put("key-1", data.clone()).await.unwrap();
        let got = enc.get("key-1").await.unwrap();

        assert_eq!(got, data);
    }

    #[tokio::test]
    async fn put_get_roundtrip_empty() {
        let mock = Arc::new(MockBlob::default());
        let enc = EncryptedBlob::new(Arc::clone(&mock), test_kek());

        let data = Bytes::new();
        enc.put("key-empty", data.clone()).await.unwrap();
        let got = enc.get("key-empty").await.unwrap();

        assert_eq!(got, data);
    }

    #[tokio::test]
    async fn put_get_roundtrip_1kb() {
        let mock = Arc::new(MockBlob::default());
        let enc = EncryptedBlob::new(Arc::clone(&mock), test_kek());

        let data = Bytes::from(vec![0x7Fu8; 1024]);
        enc.put("key-1k", data.clone()).await.unwrap();
        let got = enc.get("key-1k").await.unwrap();

        assert_eq!(got, data);
    }

    #[tokio::test]
    async fn put_get_roundtrip_binary() {
        let mock = Arc::new(MockBlob::default());
        let enc = EncryptedBlob::new(Arc::clone(&mock), test_kek());

        // Raw bytes including nulls and high bytes.
        let data = Bytes::from((0..=255u8).collect::<Vec<_>>());
        enc.put("key-bin", data.clone()).await.unwrap();
        let got = enc.get("key-bin").await.unwrap();

        assert_eq!(got, data);
    }

    #[tokio::test]
    async fn put_get_roundtrip_large() {
        let mock = Arc::new(MockBlob::default());
        let enc = EncryptedBlob::new(Arc::clone(&mock), test_kek());

        // 100 KB of data.
        let data = Bytes::from(vec![0x3Cu8; 100_000]);
        enc.put("key-large", data.clone()).await.unwrap();
        let got = enc.get("key-large").await.unwrap();

        assert_eq!(got, data);
    }

    // ── DEK uniqueness ───────────────────────────────────────────────────

    #[tokio::test]
    async fn same_plaintext_produces_different_ciphertext_in_storage() {
        let mock = Arc::new(MockBlob::default());
        let enc = EncryptedBlob::new(Arc::clone(&mock), test_kek());

        let data = Bytes::from_static(b"same data, different DEK");
        enc.put("key-a", data.clone()).await.unwrap();
        enc.put("key-b", data.clone()).await.unwrap();

        let (raw_a, raw_b) = {
            let stored = mock.stored.lock().unwrap();
            let a = stored.get("key-a").unwrap().clone();
            let b = stored.get("key-b").unwrap().clone();
            (a, b)
        };

        // Different DEKs and nonces → different ciphertexts in storage.
        assert_ne!(
            raw_a, raw_b,
            "identical plaintext must produce different ciphertext"
        );

        // But both decrypt to the same plaintext.
        let got_a = enc.get("key-a").await.unwrap();
        let got_b = enc.get("key-b").await.unwrap();
        assert_eq!(got_a, data);
        assert_eq!(got_b, data);
    }

    // ── Wrong KEK ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn wrong_kek_errors_on_get() {
        let mock = Arc::new(MockBlob::default());
        let enc_put = EncryptedBlob::new(Arc::clone(&mock), test_kek());
        let enc_get = EncryptedBlob::new(Arc::clone(&mock), wrong_kek());

        let data = Bytes::from_static(b"data under key A, accessed with key B");
        enc_put.put("key-wrong", data).await.unwrap();

        let err = enc_get.get("key-wrong").await.unwrap_err();
        let full_chain = format!("{err:#}");
        assert!(
            full_chain.contains("KEK unwrap failed"),
            "expected KEK unwrap error in chain, got: {full_chain}"
        );
    }

    // ── Corrupted data ───────────────────────────────────────────────────

    #[tokio::test]
    async fn corrupted_blob_errors_on_get() {
        let mock = Arc::new(MockBlob::default());
        let enc = EncryptedBlob::new(Arc::clone(&mock), test_kek());

        // Store a blob that is valid for the format check but has bad ciphertext.
        let fake = {
            let mut buf = vec![0u8; HEADER_SIZE + 32];
            buf[..3].copy_from_slice(&MAGIC);
            buf[OFF_VERSION] = VERSION;
            buf
        };
        mock.stored
            .lock()
            .unwrap()
            .insert("key-corrupt".to_string(), Bytes::from(fake));

        let err = enc.get("key-corrupt").await.unwrap_err();
        let full_chain = format!("{err:#}");
        assert!(
            full_chain.contains("KEK unwrap failed")
                || full_chain.contains("too short")
                || full_chain.contains("DEK decryption failed"),
            "expected crypto error in chain, got: {full_chain}"
        );
    }

    #[tokio::test]
    async fn too_short_blob_errors() {
        let mock = Arc::new(MockBlob::default());
        let enc = EncryptedBlob::new(Arc::clone(&mock), test_kek());

        mock.stored
            .lock()
            .unwrap()
            .insert("key-short".to_string(), Bytes::from_static(b"tiny"));

        let err = enc.get("key-short").await.unwrap_err();
        let full_chain = format!("{err:#}");
        assert!(full_chain.contains("too short"));
    }

    #[tokio::test]
    async fn bad_magic_bytes_errors() {
        let mock = Arc::new(MockBlob::default());
        let enc = EncryptedBlob::new(Arc::clone(&mock), test_kek());

        let mut fake = vec![0x00u8; HEADER_SIZE + 16];
        fake[0] = b'B';
        fake[1] = b'A';
        fake[2] = b'D';
        fake[OFF_VERSION] = VERSION;

        mock.stored
            .lock()
            .unwrap()
            .insert("key-badmagic".to_string(), Bytes::from(fake));

        let err = enc.get("key-badmagic").await.unwrap_err();
        let full_chain = format!("{err:#}");
        assert!(full_chain.contains("bad magic bytes"));
    }

    #[tokio::test]
    async fn wrong_version_errors() {
        let mock = Arc::new(MockBlob::default());
        let enc = EncryptedBlob::new(Arc::clone(&mock), test_kek());

        let mut fake = vec![0x00u8; HEADER_SIZE + 16];
        fake[..3].copy_from_slice(&MAGIC);
        fake[OFF_VERSION] = 99; // unsupported version

        mock.stored
            .lock()
            .unwrap()
            .insert("key-ver".to_string(), Bytes::from(fake));

        let err = enc.get("key-ver").await.unwrap_err();
        let full_chain = format!("{err:#}");
        assert!(full_chain.contains("unsupported encrypted blob version"));
    }

    // ── exists / delete ──────────────────────────────────────────────────

    #[tokio::test]
    async fn exists_returns_true_after_put() {
        let mock = Arc::new(MockBlob::default());
        let enc = EncryptedBlob::new(Arc::clone(&mock), test_kek());

        assert!(!enc.exists("key-ex").await.unwrap());
        enc.put("key-ex", Bytes::from_static(b"data"))
            .await
            .unwrap();
        assert!(enc.exists("key-ex").await.unwrap());
    }

    #[tokio::test]
    async fn exists_returns_false_after_delete() {
        let mock = Arc::new(MockBlob::default());
        let enc = EncryptedBlob::new(Arc::clone(&mock), test_kek());

        enc.put("key-del", Bytes::from_static(b"data"))
            .await
            .unwrap();
        assert!(enc.exists("key-del").await.unwrap());
        enc.delete("key-del").await.unwrap();
        assert!(!enc.exists("key-del").await.unwrap());
    }

    #[tokio::test]
    async fn delete_is_idempotent() {
        let mock = Arc::new(MockBlob::default());
        let enc = EncryptedBlob::new(Arc::clone(&mock), test_kek());

        // Deleting a non-existent key should not error.
        enc.delete("key-ghost").await.unwrap();

        // Put, delete, delete again — all should succeed.
        enc.put("key-d", Bytes::from_static(b"d")).await.unwrap();
        enc.delete("key-d").await.unwrap();
        enc.delete("key-d").await.unwrap();
    }

    // ── get_range ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn get_range_returns_correct_slice() {
        let mock = Arc::new(MockBlob::default());
        let enc = EncryptedBlob::new(Arc::clone(&mock), test_kek());

        let data = Bytes::from_static(b"0123456789");
        enc.put("key-range", data).await.unwrap();

        let slice = enc.get_range("key-range", 3, 7).await.unwrap();
        assert_eq!(slice, Bytes::from_static(b"3456"));
    }

    #[tokio::test]
    async fn get_range_full_blob() {
        let mock = Arc::new(MockBlob::default());
        let enc = EncryptedBlob::new(Arc::clone(&mock), test_kek());

        let data = Bytes::from_static(b"full range test");
        enc.put("key-fullr", data.clone()).await.unwrap();

        let slice = enc
            .get_range("key-fullr", 0, data.len() as u64)
            .await
            .unwrap();
        assert_eq!(slice, data);
    }

    #[tokio::test]
    async fn get_range_empty() {
        let mock = Arc::new(MockBlob::default());
        let enc = EncryptedBlob::new(Arc::clone(&mock), test_kek());

        enc.put("key-emptyr", Bytes::new()).await.unwrap();

        let slice = enc.get_range("key-emptyr", 0, 0).await.unwrap();
        assert_eq!(slice, Bytes::new());
    }

    // ── presigned_get_url ────────────────────────────────────────────────

    #[tokio::test]
    async fn presigned_get_url_returns_error() {
        let mock = Arc::new(MockBlob::default());
        let enc = EncryptedBlob::new(Arc::clone(&mock), test_kek());

        enc.put("key-presign", Bytes::from_static(b"x"))
            .await
            .unwrap();

        let err = enc
            .presigned_get_url("key-presign", Duration::from_secs(3600))
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("presigned_get_url is not supported")
        );
    }

    // ── Round-trip with real LocalBlob ───────────────────────────────────

    #[tokio::test]
    async fn roundtrip_with_local_blob() {
        let dir = tempfile::tempdir().unwrap();
        let local = Arc::new(crate::LocalBlob::new(
            dir.path().to_path_buf(),
            "test".into(),
        ));
        let enc = EncryptedBlob::new(Arc::clone(&local), test_kek());

        let data = Bytes::from_static(b"local encrypted blob round-trip");
        enc.put("tenant-1/roundtrip", data.clone()).await.unwrap();

        let got = enc.get("tenant-1/roundtrip").await.unwrap();
        assert_eq!(got, data);

        // Verify the stored bytes on disk are NOT the plaintext.
        let raw: Bytes = kb_core::blob::Blob::get(local.as_ref(), "tenant-1/roundtrip")
            .await
            .unwrap();
        assert_ne!(raw, data, "stored bytes must not be plaintext");
        assert!(raw.len() > HEADER_SIZE, "stored blob must include header");
    }

    // ── Mock backend for unit tests ──────────────────────────────────────

    /// An in-memory mock [`Blob`] implementation for testing the encryption
    /// layer without a real backend.
    #[derive(Default)]
    struct MockBlob {
        stored: std::sync::Mutex<std::collections::HashMap<String, Bytes>>,
    }

    #[async_trait]
    impl Blob for MockBlob {
        async fn put(&self, key: &str, data: Bytes) -> anyhow::Result<()> {
            self.stored.lock().unwrap().insert(key.to_string(), data);
            Ok(())
        }

        async fn get(&self, key: &str) -> anyhow::Result<Bytes> {
            self.stored
                .lock()
                .unwrap()
                .get(key)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("not found: {key}"))
        }

        async fn get_range(&self, key: &str, start: u64, end: u64) -> anyhow::Result<Bytes> {
            let data = self.get(key).await?;
            let s = start as usize;
            let e = (end as usize).min(data.len());
            Ok(data.slice(s..e))
        }

        async fn exists(&self, key: &str) -> anyhow::Result<bool> {
            Ok(self.stored.lock().unwrap().contains_key(key))
        }

        async fn delete(&self, key: &str) -> anyhow::Result<()> {
            self.stored.lock().unwrap().remove(key);
            Ok(())
        }

        async fn presigned_get_url(
            &self,
            _key: &str,
            _expires_in: Duration,
        ) -> anyhow::Result<String> {
            anyhow::bail!("mock does not support presigned URLs")
        }
    }
}
