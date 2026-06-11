// SPDX-License-Identifier: AGPL-3.0-or-later

//! AES-256-GCM column encryption for sensitive DB columns (plan §28, P10-T2).
//!
//! Pure, I/O-free functions for encrypting and decrypting individual database column values
//! with a 32-byte Data Encryption Key (DEK). The caller (typically [`PgStore`]) manages DEK
//! generation, wrapping with the KEK, and persistence in the `tenant_data_keys` table.
//!
//! # Wire format
//!
//! Each encrypted value is `nonce (12 bytes) || ciphertext || auth_tag (16 bytes)` — the
//! standard AES-256-GCM layout produced by the [`aes_gcm`] crate. The nonce is freshly
//! generated from the OS CSPRNG on every [`encrypt_column`] call so a given plaintext
//! produces a different ciphertext each time.
//!
//! # Security properties
//!
//! - AES-256-GCM provides authenticated encryption: tampered ciphertext is detected
//!   (decryption fails with an error, never returns garbage).
//! - The caller must keep the 32-byte DEK secret. These functions assume the DEK has
//!   already been unwrapped from its KEK-protected form.
//! - Column values are less than 1 KB in practice; no chunked/streaming interface is needed.

use aes_gcm::aead::Aead;
use aes_gcm::{Aes256Gcm, KeyInit, Nonce};
use rand::TryRng;
use rand::rngs::SysRng;

/// Nonce size for AES-256-GCM: 96 bits (12 bytes), per NIST SP 800-38D.
const NONCE_SIZE: usize = 12;

/// Encrypt a plaintext string with a 32-byte DEK using AES-256-GCM.
///
/// Returns the wire-format ciphertext: `nonce (12 bytes) || ciphertext || auth_tag (16 bytes)`.
/// The nonce is freshly generated from the OS CSPRNG on every call, so encrypting the same
/// plaintext twice yields different ciphertexts.
///
/// Empty plaintext is handled correctly (produces `nonce || tag` with no ciphertext body).
///
/// # Errors
/// Returns an error if the CSPRNG fails or the AES-256-GCM operation fails (extremely
/// unlikely — indicates a crypto library bug or key material issue).
pub fn encrypt_column(dek: &[u8; 32], plaintext: &str) -> anyhow::Result<Vec<u8>> {
    let cipher = Aes256Gcm::new_from_slice(dek)
        .map_err(|e| anyhow::anyhow!("failed to initialise AES-256-GCM for column encrypt: {e}"))?;

    let mut nonce_bytes = [0u8; NONCE_SIZE];
    SysRng
        .try_fill_bytes(&mut nonce_bytes)
        .map_err(|e| anyhow::anyhow!("failed to generate column encrypt nonce: {e}"))?;
    let nonce = Nonce::from_slice(&nonce_bytes);

    let ct = cipher
        .encrypt(nonce, plaintext.as_bytes())
        .map_err(|e| anyhow::anyhow!("column encrypt failed: {e}"))?;

    let mut result = Vec::with_capacity(NONCE_SIZE + ct.len());
    result.extend_from_slice(&nonce_bytes);
    result.extend_from_slice(&ct);
    Ok(result)
}

/// Decrypt ciphertext produced by [`encrypt_column`] back into a UTF-8 [`String`].
///
/// The input must be in the wire format: `nonce (12 bytes) || ciphertext || auth_tag (16 bytes)`.
/// The authentication tag is verified before any plaintext is returned — wrong-key or tampered
/// ciphertext produces an error, never garbage bytes.
///
/// # Errors
/// Returns an error if:
/// - the ciphertext is shorter than `nonce + tag` (12 + 16 = 28 bytes minimum),
/// - the authentication tag does not verify (wrong DEK, tampered data, or corruption),
/// - the decrypted bytes are not valid UTF-8.
pub fn decrypt_column(dek: &[u8; 32], ciphertext: &[u8]) -> anyhow::Result<String> {
    if ciphertext.len() < NONCE_SIZE + 16 {
        anyhow::bail!(
            "column ciphertext too short for AES-256-GCM: {} bytes (need at least nonce+tag={})",
            ciphertext.len(),
            NONCE_SIZE + 16
        );
    }

    let cipher = Aes256Gcm::new_from_slice(dek)
        .map_err(|e| anyhow::anyhow!("failed to initialise AES-256-GCM for column decrypt: {e}"))?;

    let nonce = Nonce::from_slice(&ciphertext[..NONCE_SIZE]);
    let ct_and_tag = &ciphertext[NONCE_SIZE..];

    let plaintext_bytes = cipher
        .decrypt(nonce, ct_and_tag)
        .map_err(|e| anyhow::anyhow!("column decrypt failed (wrong key or corrupted data): {e}"))?;

    String::from_utf8(plaintext_bytes)
        .map_err(|e| anyhow::anyhow!("decrypted column value is not valid UTF-8: {e}"))
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    fn test_dek() -> [u8; 32] {
        [0x7Fu8; 32]
    }

    fn other_dek() -> [u8; 32] {
        [0xE3u8; 32]
    }

    // ── encrypt / decrypt round-trip ─────────────────────────────────────

    #[test]
    fn roundtrip_ascii() {
        let dek = test_dek();
        let ct = encrypt_column(&dek, "hello world").unwrap();
        let pt = decrypt_column(&dek, &ct).unwrap();
        assert_eq!(pt, "hello world");
    }

    #[test]
    fn roundtrip_unicode() {
        let dek = test_dek();
        let input = "こんにちは世界 🌍 — café naïve";
        let ct = encrypt_column(&dek, input).unwrap();
        let pt = decrypt_column(&dek, &ct).unwrap();
        assert_eq!(pt, input);
    }

    #[test]
    fn roundtrip_empty() {
        let dek = test_dek();
        let ct = encrypt_column(&dek, "").unwrap();
        let pt = decrypt_column(&dek, &ct).unwrap();
        assert_eq!(pt, "");
    }

    #[test]
    fn roundtrip_long() {
        let dek = test_dek();
        let input = "A".repeat(10_000);
        let ct = encrypt_column(&dek, &input).unwrap();
        let pt = decrypt_column(&dek, &ct).unwrap();
        assert_eq!(pt, input);
    }

    #[test]
    fn roundtrip_single_char() {
        let dek = test_dek();
        let ct = encrypt_column(&dek, "X").unwrap();
        let pt = decrypt_column(&dek, &ct).unwrap();
        assert_eq!(pt, "X");
    }

    #[test]
    fn roundtrip_multiline() {
        let dek = test_dek();
        let input = "line 1\nline 2\n\nline 3\tindented";
        let ct = encrypt_column(&dek, input).unwrap();
        let pt = decrypt_column(&dek, &ct).unwrap();
        assert_eq!(pt, input);
    }

    // ── ciphertext properties ────────────────────────────────────────────

    #[test]
    fn encrypt_produces_different_output_for_same_input() {
        let dek = test_dek();
        let ct1 = encrypt_column(&dek, "same text").unwrap();
        let ct2 = encrypt_column(&dek, "same text").unwrap();
        assert_ne!(ct1, ct2, "different nonces → different ciphertexts");
    }

    #[test]
    fn encrypt_output_minimum_size() {
        let dek = test_dek();
        let ct = encrypt_column(&dek, "").unwrap();
        // nonce (12) + tag (16) = 28 minimum
        assert_eq!(ct.len(), 28);
    }

    #[test]
    fn encrypt_output_grows_with_input() {
        let dek = test_dek();
        let ct_short = encrypt_column(&dek, "hi").unwrap();
        let ct_long = encrypt_column(&dek, "hello world, a longer input").unwrap();
        assert!(ct_long.len() > ct_short.len());
    }

    // ── wrong key / tampering ────────────────────────────────────────────

    #[test]
    fn decrypt_with_wrong_key_errors() {
        let dek_a = test_dek();
        let dek_b = other_dek();
        let ct = encrypt_column(&dek_a, "secret").unwrap();
        let err = decrypt_column(&dek_b, &ct).unwrap_err();
        assert!(
            err.to_string().contains("column decrypt failed"),
            "expected decrypt error, got: {err}"
        );
    }

    #[test]
    fn decrypt_tampered_ciphertext_errors() {
        let dek = test_dek();
        let mut ct = encrypt_column(&dek, "secret").unwrap();
        // Flip a bit in the ciphertext portion (after the nonce).
        if ct.len() > NONCE_SIZE {
            ct[NONCE_SIZE] ^= 0x01;
        }
        let err = decrypt_column(&dek, &ct).unwrap_err();
        assert!(
            err.to_string().contains("column decrypt failed"),
            "expected tamper error, got: {err}"
        );
    }

    #[test]
    fn decrypt_tampered_nonce_errors() {
        let dek = test_dek();
        let mut ct = encrypt_column(&dek, "secret").unwrap();
        ct[0] ^= 0x01;
        let err = decrypt_column(&dek, &ct).unwrap_err();
        assert!(
            err.to_string().contains("column decrypt failed"),
            "expected tamper error, got: {err}"
        );
    }

    #[test]
    fn decrypt_truncated_errors() {
        let dek = test_dek();
        let truncated = vec![0u8; 5]; // shorter than 28-byte minimum
        let err = decrypt_column(&dek, &truncated).unwrap_err();
        assert!(
            err.to_string().contains("too short"),
            "expected 'too short' error, got: {err}"
        );
    }

    #[test]
    fn decrypt_empty_errors() {
        let dek = test_dek();
        let err = decrypt_column(&dek, &[]).unwrap_err();
        assert!(
            err.to_string().contains("too short"),
            "expected 'too short' error, got: {err}"
        );
    }

    #[test]
    fn decrypt_zero_bytes_errors() {
        let dek = test_dek();
        let zeros = vec![0u8; 28]; // exactly minimum size but wrong
        let err = decrypt_column(&dek, &zeros).unwrap_err();
        assert!(
            err.to_string().contains("column decrypt failed"),
            "expected decrypt error, got: {err}"
        );
    }
}
