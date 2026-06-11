// SPDX-License-Identifier: AGPL-3.0-or-later

//! Provider API key encryption and secure in-memory handling (plan §24, P10-T3).
//!
//! Provider API keys (Anthropic, OpenAI, Gemini, etc.) are stored **encrypted at rest**
//! in the `providers.api_key_enc` column using the Key Encryption Key (KEK). At call time
//! they are decrypted into a [`ProviderApiKey`] which:
//!
//! * **Zeroizes** its memory on drop (backed by the [`zeroize`] crate) so the plaintext
//!   never outlives the call that needs it.
//! * **Redacts** its value in [`Debug`] and [`Display`] output so the key is never
//!   accidentally logged, even if the value is passed to a tracing invocation.
//!
//! The encrypt/decrypt functions delegate to the [`KeyEncryptionKey`] trait — which is
//! resolved at call time per the hot-swappable configuration rule (CLAUDE.md). Rotating
//! the KEK takes effect on the **next** call without a restart.

use std::fmt;

use zeroize::{Zeroize, Zeroizing};

use crate::kek::KeyEncryptionKey;

// ── ProviderApiKey ───────────────────────────────────────────────────────────────

/// A decrypted provider API key that is backed by [`Zeroizing`] memory and redacts
/// itself in formatted output.
///
/// # Security properties
///
/// * The backing `Vec<u8>` is wrapped in a [`Zeroizing`] container that overwrites
///   the buffer with zeroes when the value is dropped, preventing the plaintext
///   from persisting in heap memory after the key is no longer needed.
/// * Both [`Debug`](ProviderApiKey::fmt) and [`Display`](ProviderApiKey::fmt) output
///   the constant string `ProviderApiKey(****)` — never the key bytes. This means
///   `tracing::info!("key: {:?}", key)` is safe.
/// * The only way to access the raw bytes is [`as_bytes`](Self::as_bytes), which
///   returns a reference valid for the lifetime of the `ProviderApiKey`.
#[derive(Clone)]
pub struct ProviderApiKey(Zeroizing<Vec<u8>>);

impl ProviderApiKey {
    /// Wrap `bytes` in a [`Zeroizing`] container that zeroizes on drop.
    #[must_use]
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(Zeroizing::new(bytes))
    }

    /// The raw key bytes, e.g. for use in an HTTP `Authorization` header.
    ///
    /// The returned reference is valid only as long as this `ProviderApiKey`
    /// lives — do not hold it across an await point that may drop the key.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Length of the key in bytes.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Whether the key is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

// ── Redacted Debug / Display ─────────────────────────────────────────────────────

impl fmt::Debug for ProviderApiKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ProviderApiKey(****)")
    }
}

impl fmt::Display for ProviderApiKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ProviderApiKey(****)")
    }
}

// ── Zeroize ──────────────────────────────────────────────────────────────────────

impl Zeroize for ProviderApiKey {
    fn zeroize(&mut self) {
        self.0.zeroize();
    }
}

// ── Encrypt / decrypt ────────────────────────────────────────────────────────────

/// Encrypt a provider API key with the KEK for storage in `providers.api_key_enc`.
///
/// The returned `Vec<u8>` is safe to store in the database — it is the
/// KEK-wrapped ciphertext, not the plaintext key.
///
/// # Errors
/// Returns an error if the underlying crypto operation fails.
pub async fn encrypt_provider_key(
    kek: &dyn KeyEncryptionKey,
    plaintext: &[u8],
) -> anyhow::Result<Vec<u8>> {
    kek.wrap(plaintext)
        .await
        .map_err(|e| anyhow::anyhow!("failed to encrypt provider API key: {e}"))
}

/// Decrypt a provider API key from the ciphertext stored in
/// `providers.api_key_enc`.
///
/// The returned [`ProviderApiKey`] zeroizes its memory on drop — the caller
/// should use it and let it go out of scope as soon as possible.
///
/// # Errors
/// Returns an error if:
/// - the KEK is not configured,
/// - the ciphertext was encrypted with a different KEK (wrong key), or
/// - the ciphertext has been tampered with (authentication tag mismatch).
pub async fn decrypt_provider_key(
    kek: &dyn KeyEncryptionKey,
    ciphertext: &[u8],
) -> anyhow::Result<ProviderApiKey> {
    let plaintext = kek.unwrap(ciphertext).await.map_err(|e| {
        anyhow::anyhow!("failed to decrypt provider API key (wrong KEK or corrupted data): {e}")
    })?;
    Ok(ProviderApiKey::new(plaintext))
}

// ── Tests ────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::sync::Arc;

    use super::*;
    use crate::kek::FileKek;

    fn test_kek() -> FileKek {
        FileKek::from_key([0x7Fu8; 32])
    }

    fn other_kek() -> FileKek {
        FileKek::from_key([0xE3u8; 32])
    }

    // ── ProviderApiKey construction & access ───────────────────────────────

    #[test]
    fn new_stores_bytes() {
        let key = ProviderApiKey::new(b"sk-test-key".to_vec());
        assert_eq!(key.as_bytes(), b"sk-test-key");
        assert_eq!(key.len(), 11);
        assert!(!key.is_empty());
    }

    #[test]
    fn empty_key() {
        let key = ProviderApiKey::new(vec![]);
        assert!(key.is_empty());
        assert_eq!(key.len(), 0);
    }

    #[test]
    fn clone_preserves_bytes() {
        let key = ProviderApiKey::new(b"secret".to_vec());
        let cloned = key.clone();
        assert_eq!(cloned.as_bytes(), b"secret");
    }

    // ── Debug / Display redaction ──────────────────────────────────────────

    #[test]
    fn debug_never_contains_key_bytes() {
        let key = ProviderApiKey::new(b"sk-ant-api03-abc123-super-secret".to_vec());
        let debug_str = format!("{:?}", key);
        assert_eq!(debug_str, "ProviderApiKey(****)");
        assert!(
            !debug_str.contains("sk-ant"),
            "key leaked in Debug output: {debug_str}"
        );
        assert!(
            !debug_str.contains("secret"),
            "key leaked in Debug output: {debug_str}"
        );
    }

    #[test]
    fn display_never_contains_key_bytes() {
        let key = ProviderApiKey::new(b"sk-ant-api03-abc123-super-secret".to_vec());
        let display_str = format!("{}", key);
        assert_eq!(display_str, "ProviderApiKey(****)");
        assert!(
            !display_str.contains("sk-ant"),
            "key leaked in Display output: {display_str}"
        );
        assert!(
            !display_str.contains("secret"),
            "key leaked in Display output: {display_str}"
        );
    }

    #[test]
    fn debug_redaction_works_for_empty_key() {
        let key = ProviderApiKey::new(vec![]);
        assert_eq!(format!("{:?}", key), "ProviderApiKey(****)");
    }

    #[test]
    fn display_redaction_works_for_empty_key() {
        let key = ProviderApiKey::new(vec![]);
        assert_eq!(format!("{}", key), "ProviderApiKey(****)");
    }

    // ── Zeroization ────────────────────────────────────────────────────────

    #[test]
    fn zeroize_clears_memory() {
        let mut key = ProviderApiKey::new(b"super-secret-api-key-material".to_vec());
        assert!(!key.is_empty());
        assert_eq!(key.as_bytes(), b"super-secret-api-key-material");

        key.zeroize();

        // After zeroize, the buffer must be all zeroes.
        let bytes = key.as_bytes();
        assert!(
            bytes.iter().all(|&b| b == 0),
            "memory was not zeroized: first byte={:02x}",
            bytes.first().copied().unwrap_or(0xFF)
        );
    }

    #[test]
    fn zeroize_handles_empty_key() {
        let mut key = ProviderApiKey::new(vec![]);
        key.zeroize();
        // No panic — empty key zeroization is a no-op.
    }

    #[test]
    fn zeroize_long_key() {
        let data = vec![0xABu8; 4096];
        let mut key = ProviderApiKey::new(data);
        assert_eq!(key.len(), 4096);

        key.zeroize();

        assert!(
            key.as_bytes().iter().all(|&b| b == 0),
            "long key was not fully zeroized"
        );
    }

    // ── encrypt / decrypt round-trip ───────────────────────────────────────

    #[tokio::test]
    async fn encrypt_decrypt_roundtrip() {
        let kek = test_kek();
        let plaintext = b"sk-my-api-key-12345";

        let ct = encrypt_provider_key(&kek, plaintext).await.unwrap();
        let key = decrypt_provider_key(&kek, &ct).await.unwrap();

        assert_eq!(key.as_bytes(), plaintext);
    }

    #[tokio::test]
    async fn encrypt_decrypt_empty_key() {
        let kek = test_kek();
        let ct = encrypt_provider_key(&kek, b"").await.unwrap();
        let key = decrypt_provider_key(&kek, &ct).await.unwrap();
        assert!(key.is_empty());
    }

    #[tokio::test]
    async fn encrypt_decrypt_long_key() {
        let kek = test_kek();
        let plaintext = vec![0x55u8; 1024];

        let ct = encrypt_provider_key(&kek, &plaintext).await.unwrap();
        let key = decrypt_provider_key(&kek, &ct).await.unwrap();

        assert_eq!(key.as_bytes(), plaintext);
    }

    #[tokio::test]
    async fn encrypt_produces_different_output_each_time() {
        let kek = test_kek();
        let ct1 = encrypt_provider_key(&kek, b"same-key").await.unwrap();
        let ct2 = encrypt_provider_key(&kek, b"same-key").await.unwrap();
        assert_ne!(ct1, ct2, "different nonces → different ciphertexts");
    }

    // ── wrong KEK → clear error ───────────────────────────────────────────

    #[tokio::test]
    async fn decrypt_with_wrong_kek_errors_clearly() {
        let kek_a = test_kek();
        let kek_b = other_kek();

        let ct = encrypt_provider_key(&kek_a, b"my-secret").await.unwrap();
        let err = decrypt_provider_key(&kek_b, &ct).await.unwrap_err();

        let msg = err.to_string();
        assert!(
            msg.contains("wrong KEK") || msg.contains("corrupted data"),
            "expected clear error about wrong KEK, got: {msg}"
        );
    }

    #[tokio::test]
    async fn decrypt_tampered_ciphertext_errors_clearly() {
        let kek = test_kek();
        let mut ct = encrypt_provider_key(&kek, b"secret").await.unwrap();

        // Flip a bit in the ciphertext portion (after the 12-byte nonce).
        if ct.len() > 12 {
            ct[15] ^= 0x01;
        }

        let err = decrypt_provider_key(&kek, &ct).await.unwrap_err();
        assert!(
            err.to_string().contains("wrong KEK") || err.to_string().contains("corrupted data"),
            "expected clear error about tampered data, got: {err}"
        );
    }

    #[tokio::test]
    async fn decrypt_truncated_ciphertext_errors() {
        let kek = test_kek();
        let err = decrypt_provider_key(&kek, &[0u8; 5]).await.unwrap_err();
        assert!(
            err.to_string().contains("wrong KEK")
                || err.to_string().contains("corrupted data")
                || err.to_string().contains("too short"),
            "expected clear error for truncated input, got: {err}"
        );
    }

    // ── KEK rotation (hot-swap, no restart) ───────────────────────────────

    #[tokio::test]
    async fn rotation_new_kek_used_on_next_call() {
        let old_kek = FileKek::from_key([0x11u8; 32]);
        let new_kek = FileKek::from_key([0x22u8; 32]);
        let plaintext = b"key-for-rotation-test";

        // Encrypt with old KEK.
        let ct_old = encrypt_provider_key(&old_kek, plaintext).await.unwrap();

        // "Rotate": re-wrap with new KEK.
        let recovered = decrypt_provider_key(&old_kek, &ct_old).await.unwrap();
        let ct_new = encrypt_provider_key(&new_kek, recovered.as_bytes())
            .await
            .unwrap();

        // After rotation, the new KEK decrypts successfully.
        let key = decrypt_provider_key(&new_kek, &ct_new).await.unwrap();
        assert_eq!(key.as_bytes(), plaintext);

        // And the old KEK cannot decrypt the new ciphertext.
        let err = decrypt_provider_key(&old_kek, &ct_new).await.unwrap_err();
        assert!(
            err.to_string().contains("wrong KEK") || err.to_string().contains("corrupted data"),
            "old KEK must not decrypt ciphertext wrapped by new KEK"
        );
    }

    // ── Trait object (Arc<dyn KeyEncryptionKey>) ──────────────────────────

    #[tokio::test]
    async fn encrypt_decrypt_with_trait_object() {
        let kek: Arc<dyn KeyEncryptionKey> = Arc::new(test_kek());
        let plaintext = b"trait-object-test";

        let ct = encrypt_provider_key(kek.as_ref(), plaintext).await.unwrap();
        let key = decrypt_provider_key(kek.as_ref(), &ct).await.unwrap();

        assert_eq!(key.as_bytes(), plaintext);
    }
}
