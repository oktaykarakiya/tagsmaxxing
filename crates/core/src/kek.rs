// SPDX-License-Identifier: AGPL-3.0-or-later

//! Key Encryption Key (KEK) — the master-key capability trait and its file/env-backed
//! implementation (plan §20, §24, §28).
//!
//! The KEK wraps (encrypts) and unwraps (decrypts) per-file Data Encryption Keys (DEKs)
//! so that each blob stored in the object backend carries its own unique key, and the
//! master key never touches the bulk data directly. This is standard envelope encryption:
//! the DEK encrypts the file; the KEK encrypts the DEK.
//!
//! The [`KeyEncryptionKey`] trait is defined in `kb-core` so that every layer that needs
//! to hold a KEK reference can depend on the trait rather than a concrete implementation.
//! The concrete [`FileKek`] loads a 256-bit key from an environment variable or file;
//! P10-T1 adds a KMS/Vault-backed impl behind the same trait.

use aes_gcm::aead::Aead;
use aes_gcm::{Aes256Gcm, KeyInit};
use async_trait::async_trait;
use rand::TryRng;
use rand::rngs::SysRng;

/// Nonce size for AES-256-GCM: 96 bits (12 bytes), per NIST SP 800-38D.
const NONCE_SIZE: usize = 12;

/// A master key that wraps and unwraps Data Encryption Keys (DEKs).
///
/// This is the top of the key hierarchy (§28). The trait is async from the start so
/// that a KMS/Vault-backed implementation (P10-T1) can make network calls without a
/// breaking signature change. File-based implementations like [`FileKek`] perform the
/// crypto synchronously — the async is a no-op for them, but it keeps the trait uniform.
///
/// # Security properties
///
/// - `wrap` produces a ciphertext that includes the AES-256-GCM authentication tag
///   appended to the ciphertext (the standard `aes-gcm` output format).
/// - `unwrap` verifies the authentication tag before returning plaintext; tampered
///   ciphertext or a wrong key produces an error, never garbage plaintext.
/// - Implementations must use a cryptographically random nonce (12 bytes) for every
///   `wrap` call — reusing a nonce with the same key catastrophically breaks GCM.
#[async_trait]
pub trait KeyEncryptionKey: Send + Sync {
    /// Encrypt (wrap) `plaintext` under this KEK, returning the ciphertext with the
    /// 16-byte authentication tag appended.
    ///
    /// The caller must supply the plaintext DEK to be wrapped. The returned
    /// ciphertext is `encrypted_dek || auth_tag` (standard AES-256-GCM output).
    ///
    /// # Errors
    /// Returns an error if the underlying crypto operation fails.
    async fn wrap(&self, plaintext: &[u8]) -> anyhow::Result<Vec<u8>>;

    /// Decrypt (unwrap) `ciphertext` (which must include the appended 16-byte
    /// authentication tag) with this KEK, returning the original plaintext DEK.
    ///
    /// # Errors
    /// Returns an error if the authentication tag does not verify (wrong key,
    /// tampered data, or corruption) or if the underlying crypto operation fails.
    async fn unwrap(&self, ciphertext: &[u8]) -> anyhow::Result<Vec<u8>>;
}

// ── FileKek ────────────────────────────────────────────────────────────────────

/// A KEK backed by a 256-bit key loaded from an environment variable or file.
///
/// Construction order:
/// 1. If `BLOB_KEK_B64` is set and non-empty, decode it as base64 and use the
///    resulting bytes as the 256-bit AES key.
/// 2. Otherwise, if `BLOB_KEK_FILE` is set and non-empty, read the file and use the
///    first 32 bytes as the key.
/// 3. Otherwise, construction fails — a KEK must be explicitly provisioned.
///
/// The key is held in memory for the lifetime of the process. Rotation means
/// replacing the env-var / file contents and calling a reload (or restarting);
/// P10-T1 replaces this with a Vault-based KEK that supports online rotation.
#[derive(Debug, Clone)]
pub struct FileKek {
    key: [u8; 32],
}

impl FileKek {
    /// Load the KEK from the environment.
    ///
    /// Tries `BLOB_KEK_B64` (base64) first, then falls back to `BLOB_KEK_FILE`
    /// (raw file read). Returns an error if neither source provides a valid 32-byte
    /// key.
    ///
    /// # Errors
    /// Returns an error if the key source is missing, the base64 is invalid, the
    /// decoded key is not exactly 32 bytes, or the file cannot be read.
    pub fn from_env() -> anyhow::Result<Self> {
        // 1. Try the base64-encoded env var first.
        if let Ok(b64) = std::env::var("BLOB_KEK_B64")
            && !b64.is_empty()
        {
            return Self::from_b64(&b64);
        }

        // 2. Fall back to the file path.
        if let Ok(path) = std::env::var("BLOB_KEK_FILE")
            && !path.is_empty()
        {
            return Self::from_file(&path);
        }

        anyhow::bail!(
            "KEK not provisioned: set BLOB_KEK_B64 (base64-encoded 32-byte key) or \
             BLOB_KEK_FILE (path to a file containing the raw 32-byte key)"
        );
    }

    /// Construct a `FileKek` from a base64-encoded 32-byte key string.
    ///
    /// # Errors
    /// Returns an error if the base64 is invalid or the decoded key is not
    /// exactly 32 bytes.
    pub fn from_b64(b64: &str) -> anyhow::Result<Self> {
        use base64::Engine;
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(b64.as_bytes())
            .map_err(|e| anyhow::anyhow!("invalid BLOB_KEK_B64 base64: {e}"))?;

        if decoded.len() != 32 {
            anyhow::bail!(
                "BLOB_KEK_B64 must decode to exactly 32 bytes (got {})",
                decoded.len()
            );
        }

        let mut key = [0u8; 32];
        key.copy_from_slice(&decoded);
        Ok(Self { key })
    }

    /// Construct a `FileKek` by reading the raw 32-byte key from a file path.
    ///
    /// Only the first 32 bytes are read; extra bytes are ignored.
    ///
    /// # Errors
    /// Returns an error if the file cannot be read or contains fewer than 32 bytes.
    pub fn from_file(path: &str) -> anyhow::Result<Self> {
        let data = std::fs::read(path)
            .map_err(|e| anyhow::anyhow!("failed to read BLOB_KEK_FILE `{path}`: {e}"))?;

        if data.len() < 32 {
            anyhow::bail!(
                "BLOB_KEK_FILE `{path}` must contain at least 32 bytes (got {})",
                data.len()
            );
        }

        let mut key = [0u8; 32];
        key.copy_from_slice(&data[..32]);
        Ok(Self { key })
    }

    /// Construct a `FileKek` directly from a 32-byte key slice.
    ///
    /// This is intended for tests and programmatic construction. In production,
    /// prefer [`Self::from_env`].
    #[must_use]
    pub fn from_key(key: [u8; 32]) -> Self {
        Self { key }
    }

    /// Encrypt `plaintext` with this KEK using AES-256-GCM.
    ///
    /// Generates a fresh random 12-byte nonce per call. Returns
    /// `nonce || ciphertext || auth_tag` so the nonce is bundled with the output
    /// for later decryption.
    fn encrypt_with_kek(&self, plaintext: &[u8]) -> anyhow::Result<Vec<u8>> {
        let cipher = Aes256Gcm::new_from_slice(&self.key)
            .map_err(|e| anyhow::anyhow!("failed to initialise AES-256-GCM for KEK: {e}"))?;

        let mut nonce_bytes = [0u8; NONCE_SIZE];
        SysRng
            .try_fill_bytes(&mut nonce_bytes)
            .map_err(|e| anyhow::anyhow!("failed to generate KEK nonce: {e}"))?;
        let nonce = aes_gcm::Nonce::from_slice(&nonce_bytes);

        let mut result = Vec::with_capacity(NONCE_SIZE + plaintext.len() + 16);
        result.extend_from_slice(&nonce_bytes);
        let ct = cipher
            .encrypt(nonce, plaintext)
            .map_err(|e| anyhow::anyhow!("KEK wrap failed: {e}"))?;
        result.extend_from_slice(&ct);
        Ok(result)
    }

    /// Decrypt `wrapped` with this KEK using AES-256-GCM.
    ///
    /// Expects the input format produced by [`Self::encrypt_with_kek`]:
    /// `nonce (12) || ciphertext || auth_tag (16)`.
    fn decrypt_with_kek(&self, wrapped: &[u8]) -> anyhow::Result<Vec<u8>> {
        if wrapped.len() < NONCE_SIZE + 16 {
            anyhow::bail!(
                "wrapped data too short for AES-256-GCM: {} bytes (need at least nonce+tag)",
                wrapped.len()
            );
        }

        let cipher = Aes256Gcm::new_from_slice(&self.key)
            .map_err(|e| anyhow::anyhow!("failed to initialise AES-256-GCM for KEK: {e}"))?;

        let nonce = aes_gcm::Nonce::from_slice(&wrapped[..NONCE_SIZE]);
        let ct_and_tag = &wrapped[NONCE_SIZE..];

        cipher
            .decrypt(nonce, ct_and_tag)
            .map_err(|e| anyhow::anyhow!("KEK unwrap failed (wrong key or corrupted data): {e}"))
    }
}

// ── KeyEncryptionKey impl ──────────────────────────────────────────────────────

#[async_trait]
impl KeyEncryptionKey for FileKek {
    async fn wrap(&self, plaintext: &[u8]) -> anyhow::Result<Vec<u8>> {
        self.encrypt_with_kek(plaintext)
    }

    async fn unwrap(&self, ciphertext: &[u8]) -> anyhow::Result<Vec<u8>> {
        self.decrypt_with_kek(ciphertext)
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::sync::Arc;

    use super::*;

    // A known 32-byte test key (DO NOT use in production).
    const TEST_KEY: [u8; 32] = [0xABu8; 32];

    fn test_kek() -> FileKek {
        FileKek::from_key(TEST_KEY)
    }

    // ── FileKek construction ──────────────────────────────────────────────

    #[test]
    fn from_key_round_trips() {
        let kek = FileKek::from_key(TEST_KEY);
        assert_eq!(kek.key, TEST_KEY);
    }

    #[test]
    fn from_b64_valid_32_byte_key() {
        use base64::Engine;
        let b64 = base64::engine::general_purpose::STANDARD.encode(TEST_KEY);
        let kek = FileKek::from_b64(&b64).unwrap();
        assert_eq!(kek.key, TEST_KEY);
    }

    #[test]
    fn from_b64_rejects_short_key() {
        use base64::Engine;
        let b64 = base64::engine::general_purpose::STANDARD.encode([0xABu8; 16]);
        let err = FileKek::from_b64(&b64).unwrap_err();
        assert!(err.to_string().contains("exactly 32 bytes"));
        assert!(err.to_string().contains("16"));
    }

    #[test]
    fn from_b64_rejects_long_key() {
        use base64::Engine;
        let b64 = base64::engine::general_purpose::STANDARD.encode([0xABu8; 64]);
        let err = FileKek::from_b64(&b64).unwrap_err();
        assert!(err.to_string().contains("exactly 32 bytes"));
        assert!(err.to_string().contains("64"));
    }

    #[test]
    fn from_b64_rejects_invalid_base64() {
        let err = FileKek::from_b64("not-valid-base64!!!").unwrap_err();
        assert!(err.to_string().contains("invalid BLOB_KEK_B64"));
    }

    #[test]
    fn from_b64_rejects_empty_string() {
        let err = FileKek::from_b64("").unwrap_err();
        assert!(err.to_string().contains("exactly 32 bytes"));
    }

    #[test]
    fn from_file_reads_32_byte_key() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), TEST_KEY).unwrap();
        let kek = FileKek::from_file(tmp.path().to_str().unwrap()).unwrap();
        assert_eq!(kek.key, TEST_KEY);
    }

    #[test]
    fn from_file_reads_first_32_bytes_from_larger_file() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let mut data = vec![0xABu8; 32];
        data.extend_from_slice(&[0xCDu8; 16]);
        std::fs::write(tmp.path(), &data).unwrap();
        let kek = FileKek::from_file(tmp.path().to_str().unwrap()).unwrap();
        assert_eq!(kek.key, TEST_KEY);
    }

    #[test]
    fn from_file_rejects_short_file() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(tmp.path(), [0xABu8; 16]).unwrap();
        let err = FileKek::from_file(tmp.path().to_str().unwrap()).unwrap_err();
        assert!(err.to_string().contains("at least 32 bytes"));
        assert!(err.to_string().contains("16"));
    }

    #[test]
    fn from_file_rejects_nonexistent() {
        let err = FileKek::from_file("/nonexistent/path/for/test/key.bin").unwrap_err();
        assert!(err.to_string().contains("failed to read BLOB_KEK_FILE"));
    }

    // ── wrap / unwrap round-trip ─────────────────────────────────────────

    #[tokio::test]
    async fn wrap_unwrap_roundtrip_32_bytes() {
        let kek = test_kek();
        let dek = [0x42u8; 32];
        let wrapped = kek.wrap(&dek).await.unwrap();
        let unwrapped = kek.unwrap(&wrapped).await.unwrap();
        assert_eq!(unwrapped, dek);
    }

    #[tokio::test]
    async fn wrap_unwrap_roundtrip_16_bytes() {
        let kek = test_kek();
        let data = b"short-16-byte-key";
        let wrapped = kek.wrap(data).await.unwrap();
        let unwrapped = kek.unwrap(&wrapped).await.unwrap();
        assert_eq!(unwrapped, data);
    }

    #[tokio::test]
    async fn wrap_unwrap_roundtrip_empty() {
        let kek = test_kek();
        let wrapped = kek.wrap(b"").await.unwrap();
        let unwrapped = kek.unwrap(&wrapped).await.unwrap();
        assert_eq!(unwrapped, b"");
    }

    #[tokio::test]
    async fn wrap_unwrap_roundtrip_1kb() {
        let kek = test_kek();
        let data = vec![0x7Fu8; 1024];
        let wrapped = kek.wrap(&data).await.unwrap();
        let unwrapped = kek.unwrap(&wrapped).await.unwrap();
        assert_eq!(unwrapped, data);
    }

    #[tokio::test]
    async fn wrap_produces_different_output_for_same_input() {
        let kek = test_kek();
        let dek = [0x01u8; 32];
        let w1 = kek.wrap(&dek).await.unwrap();
        let w2 = kek.wrap(&dek).await.unwrap();
        // Different nonces → different ciphertexts.
        assert_ne!(w1, w2);
    }

    #[tokio::test]
    async fn wrap_output_contains_space_for_nonce_and_tag() {
        let kek = test_kek();
        let dek = [0x03u8; 32];
        let wrapped = kek.wrap(&dek).await.unwrap();
        // nonce (12) + ciphertext (32) + auth tag (16) = 60
        assert_eq!(wrapped.len(), NONCE_SIZE + dek.len() + 16);
    }

    // ── wrong key / tampering ────────────────────────────────────────────

    #[tokio::test]
    async fn unwrap_with_wrong_key_errors() {
        let kek_a = FileKek::from_key([0xAAu8; 32]);
        let kek_b = FileKek::from_key([0xBBu8; 32]);
        let dek = [0x42u8; 32];

        let wrapped = kek_a.wrap(&dek).await.unwrap();
        let err = kek_b.unwrap(&wrapped).await.unwrap_err();
        assert!(err.to_string().contains("KEK unwrap failed"));
    }

    #[tokio::test]
    async fn unwrap_with_tampered_ciphertext_errors() {
        let kek = test_kek();
        let dek = [0x42u8; 32];
        let mut wrapped = kek.wrap(&dek).await.unwrap();

        // Flip a bit in the ciphertext portion (after the nonce).
        if wrapped.len() > NONCE_SIZE {
            wrapped[NONCE_SIZE] ^= 0x01;
        }
        let err = kek.unwrap(&wrapped).await.unwrap_err();
        assert!(err.to_string().contains("KEK unwrap failed"));
    }

    #[tokio::test]
    async fn unwrap_with_truncated_data_errors() {
        let kek = test_kek();
        let truncated = vec![0x00u8; 5]; // shorter than nonce+tag minimum
        let err = kek.unwrap(&truncated).await.unwrap_err();
        assert!(err.to_string().contains("too short"));
    }

    // ── KEK rotation (re-wrap) ──────────────────────────────────────────

    #[tokio::test]
    async fn rotation_re_wrap_preserves_dek() {
        let old_kek = FileKek::from_key([0x11u8; 32]);
        let new_kek = FileKek::from_key([0x22u8; 32]);
        let dek = [0x99u8; 32];

        // Wrap with old KEK.
        let wrapped_old = old_kek.wrap(&dek).await.unwrap();

        // Unwrap with old KEK → plaintext DEK.
        let recovered = old_kek.unwrap(&wrapped_old).await.unwrap();
        assert_eq!(recovered, dek);

        // Re-wrap with new KEK.
        let wrapped_new = new_kek.wrap(&recovered).await.unwrap();

        // Unwrap with new KEK.
        let final_dek = new_kek.unwrap(&wrapped_new).await.unwrap();
        assert_eq!(final_dek, dek);
    }

    // ── Trait object safety ──────────────────────────────────────────────

    #[tokio::test]
    async fn trait_object_roundtrip() {
        let kek: Arc<dyn KeyEncryptionKey> = Arc::new(test_kek());
        let dek = [0x77u8; 32];
        let wrapped = kek.wrap(&dek).await.unwrap();
        let unwrapped = kek.unwrap(&wrapped).await.unwrap();
        assert_eq!(unwrapped, dek);
    }
}
