//! DataKey envelope-tracking struct and KEK rotation helpers (plan §28, P10-T1).
//!
//! A [`DataKey`] tracks which Key Encryption Key (KEK) wrapped a particular Data Encryption
//! Key (DEK). When a KEK is rotated, the DEK itself does not need to be regenerated — only
//! the wrapping changes: unwrap the DEK with the old KEK, re-wrap with the new KEK, and
//! store a new [`DataKey`] record. This avoids bulk re-encryption of file data.
//!
//! The struct is a pure data abstraction with no I/O, making it trivially unit-testable
//! without a real KMS.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::kek::KeyEncryptionKey;

/// A record that tracks which KEK wrapped a particular DEK.
///
/// # Role in the key hierarchy (§28)
///
/// - `id` uniquely identifies this DEK record (useful for audit logs and DB foreign keys).
/// - `wrapped_dek` is the output of [`KeyEncryptionKey::wrap`] on the raw DEK bytes.
/// - `kek_id` identifies which master key performed the wrapping (e.g. `"vault:transit:my-key"`,
///   `"file:v1"`, or a KMS key ARN). During rotation, the stored `kek_id` tells the operator
///   which old KEK to use for unwrapping.
///
/// # Serialization
///
/// `DataKey` derives `Serialize` and `Deserialize` so it can be stored as JSON in Postgres
/// or logged in audit trails. The `wrapped_dek` field is serialized as base64 in human-readable
/// formats (controlled by serde).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DataKey {
    /// Unique identifier for this DEK record.
    pub id: Uuid,
    /// The DEK ciphertext as produced by [`KeyEncryptionKey::wrap`].
    /// May be binary or a Vault-style transit ciphertext string.
    pub wrapped_dek: Vec<u8>,
    /// An opaque identifier for the KEK that performed the wrapping.
    /// Examples: `"vault:v1:transit/key-name"`, `"file:v1"`, `"aws:kms:arn:..."`.
    pub kek_id: String,
}

impl DataKey {
    /// Create a new `DataKey` record.
    ///
    /// `id` should be a fresh UUID (e.g. from [`Uuid::new_v4`]).
    /// `wrapped_dek` is the output of calling `kek.wrap(&raw_dek)`.
    /// `kek_id` is a human-readable identifier for the wrapping KEK.
    #[must_use]
    pub fn new(id: Uuid, wrapped_dek: Vec<u8>, kek_id: String) -> Self {
        Self {
            id,
            wrapped_dek,
            kek_id,
        }
    }

    /// Generate a fresh `DataKey` by wrapping a random DEK with the given KEK.
    ///
    /// Generates a 32-byte random DEK from the OS CSPRNG, wraps it with `kek`,
    /// and returns a `DataKey` with a new UUID.
    ///
    /// # Errors
    /// Returns an error if the CSPRNG or the KEK's `wrap` call fails.
    pub async fn generate(kek: &dyn KeyEncryptionKey, kek_id: String) -> anyhow::Result<Self> {
        use rand::TryRng;
        use rand::rngs::SysRng;

        let mut raw_dek = [0u8; 32];
        SysRng
            .try_fill_bytes(&mut raw_dek)
            .map_err(|e| anyhow::anyhow!("failed to generate random DEK: {e}"))?;

        let wrapped_dek = kek.wrap(&raw_dek).await?;
        Ok(Self {
            id: Uuid::new_v4(),
            wrapped_dek,
            kek_id,
        })
    }

    /// Serialize to a JSON string.
    ///
    /// # Errors
    /// Returns an error if serialization fails (should not happen for this struct).
    pub fn to_json(&self) -> serde_json::Result<String> {
        serde_json::to_string(self)
    }

    /// Deserialize from a JSON string.
    ///
    /// # Errors
    /// Returns an error if the JSON is malformed or fields are missing.
    pub fn from_json(json: &str) -> serde_json::Result<Self> {
        serde_json::from_str(json)
    }
}

/// Rotate a DEK from an old KEK to a new KEK.
///
/// This is the core rotation primitive: it unwraps the DEK with `old_kek` and
/// immediately re-wraps it with `new_kek`. The plaintext DEK never leaves this
/// function; only the caller receives the new wrapped form. No bulk re-encryption
/// of file data is needed.
///
/// The returned `DataKey` gets a fresh UUID but retains the same logical DEK.
/// The `new_kek_id` should be set to the identifier of `new_kek`.
///
/// # Errors
/// Returns an error if the old KEK cannot unwrap the DEK (wrong key, corruption)
/// or the new KEK's `wrap` call fails.
pub async fn rotate_dek(
    old_kek: &dyn KeyEncryptionKey,
    new_kek: &dyn KeyEncryptionKey,
    data_key: &DataKey,
    new_kek_id: String,
) -> anyhow::Result<DataKey> {
    let raw_dek = old_kek
        .unwrap(&data_key.wrapped_dek)
        .await
        .map_err(|e| anyhow::anyhow!("DEK rotation failed during unwrap with old KEK: {e}"))?;

    let new_wrapped = new_kek
        .wrap(&raw_dek)
        .await
        .map_err(|e| anyhow::anyhow!("DEK rotation failed during wrap with new KEK: {e}"))?;

    Ok(DataKey {
        id: Uuid::new_v4(),
        wrapped_dek: new_wrapped,
        kek_id: new_kek_id,
    })
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use crate::kek::FileKek;

    // Known 32-byte test keys.
    const KEY_A: [u8; 32] = [0xAAu8; 32];
    const KEY_B: [u8; 32] = [0xBBu8; 32];

    // ── DataKey construction ──────────────────────────────────────────────

    #[test]
    fn new_stores_fields() {
        let id = Uuid::new_v4();
        let dk = DataKey::new(id, b"wrapped-dek-bytes".to_vec(), "vault:v1".into());
        assert_eq!(dk.id, id);
        assert_eq!(dk.wrapped_dek, b"wrapped-dek-bytes");
        assert_eq!(dk.kek_id, "vault:v1");
    }

    #[tokio::test]
    async fn generate_produces_valid_data_key() {
        let kek = FileKek::from_key(KEY_A);
        let dk = DataKey::generate(&kek, "test-kek:v1".into()).await.unwrap();

        assert!(!dk.wrapped_dek.is_empty(), "wrapped DEK must not be empty");
        assert_eq!(dk.kek_id, "test-kek:v1");

        // The wrapped DEK should be decryptable with the same KEK.
        let unwrapped = kek.unwrap(&dk.wrapped_dek).await.unwrap();
        assert_eq!(unwrapped.len(), 32, "DEK must be 32 bytes");
    }

    #[tokio::test]
    async fn generate_produces_unique_ids() {
        let kek = FileKek::from_key(KEY_A);
        let dk1 = DataKey::generate(&kek, "k1".into()).await.unwrap();
        let dk2 = DataKey::generate(&kek, "k1".into()).await.unwrap();
        assert_ne!(dk1.id, dk2.id);
    }

    #[tokio::test]
    async fn generate_produces_unique_wrapped_deks() {
        let kek = FileKek::from_key(KEY_A);
        let dk1 = DataKey::generate(&kek, "k1".into()).await.unwrap();
        let dk2 = DataKey::generate(&kek, "k1".into()).await.unwrap();

        // Different DEKs → different wrapped outputs.
        assert_ne!(dk1.wrapped_dek, dk2.wrapped_dek);
    }

    // ── Serialization ─────────────────────────────────────────────────────

    #[test]
    fn roundtrip_json() {
        let id = Uuid::new_v4();
        let dk = DataKey::new(id, vec![1, 2, 3, 4, 5], "vault:my-key".into());
        let json = dk.to_json().unwrap();
        let dk2 = DataKey::from_json(&json).unwrap();
        assert_eq!(dk, dk2);
    }

    #[test]
    fn roundtrip_json_empty_dek() {
        let dk = DataKey::new(Uuid::new_v4(), vec![], "file:v1".into());
        let json = dk.to_json().unwrap();
        let dk2 = DataKey::from_json(&json).unwrap();
        assert_eq!(dk, dk2);
    }

    #[test]
    fn from_json_rejects_missing_fields() {
        // Use a valid UUID so the error is about missing fields, not invalid UUID.
        let valid_id = Uuid::new_v4().to_string();
        let json = format!(r#"{{"id": "{}"}}"#, valid_id);
        let err = DataKey::from_json(&json).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("wrapped_dek") || msg.contains("kek_id"),
            "expected error about missing fields, got: {msg}"
        );
    }

    #[test]
    fn from_json_rejects_invalid_json() {
        let err = DataKey::from_json("not json").unwrap_err();
        assert!(err.to_string().contains("expected"));
    }

    #[test]
    fn json_field_names_are_snake_case() {
        let dk = DataKey::new(Uuid::new_v4(), b"data".to_vec(), "k:v".into());
        let json = dk.to_json().unwrap();
        assert!(json.contains("\"wrapped_dek\""));
        assert!(json.contains("\"kek_id\""));
        assert!(json.contains("\"id\""));
    }

    #[test]
    fn partial_eq_works() {
        let id = Uuid::new_v4();
        let a = DataKey::new(id, b"x".to_vec(), "k1".into());
        let b = DataKey::new(id, b"x".to_vec(), "k1".into());
        let c = DataKey::new(id, b"y".to_vec(), "k1".into());
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    // ── Rotation ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn rotation_preserves_dek() {
        let old_kek = FileKek::from_key(KEY_A);
        let new_kek = FileKek::from_key(KEY_B);

        // Generate a DEK wrapped with the old KEK.
        let old_dk = DataKey::generate(&old_kek, "old-kek:v1".into())
            .await
            .unwrap();

        // Rotate to the new KEK.
        let new_dk = rotate_dek(&old_kek, &new_kek, &old_dk, "new-kek:v1".into())
            .await
            .unwrap();

        // The new DataKey should unwrap with the new KEK.
        let raw = new_kek.unwrap(&new_dk.wrapped_dek).await.unwrap();
        assert_eq!(raw.len(), 32);

        // The old KEK should NOT be able to unwrap the new wrapping.
        let err = old_kek.unwrap(&new_dk.wrapped_dek).await.unwrap_err();
        assert!(
            err.to_string().contains("KEK unwrap failed"),
            "old KEK should not be able to unwrap new wrapping"
        );

        // Old DEK still works with old KEK (not destroyed by rotation).
        let raw_old = old_kek.unwrap(&old_dk.wrapped_dek).await.unwrap();
        assert_eq!(raw_old.len(), 32);
    }

    #[tokio::test]
    async fn rotation_updates_kek_id() {
        let old_kek = FileKek::from_key(KEY_A);
        let new_kek = FileKek::from_key(KEY_B);

        let old_dk = DataKey::generate(&old_kek, "old-id".into()).await.unwrap();
        assert_eq!(old_dk.kek_id, "old-id");

        let new_dk = rotate_dek(&old_kek, &new_kek, &old_dk, "new-id".into())
            .await
            .unwrap();
        assert_eq!(new_dk.kek_id, "new-id");
    }

    #[tokio::test]
    async fn rotation_with_wrong_old_kek_errors() {
        let correct_kek = FileKek::from_key(KEY_A);
        let wrong_kek = FileKek::from_key(KEY_B);
        let new_kek = FileKek::from_key([0xCCu8; 32]);

        let dk = DataKey::generate(&correct_kek, "k:v".into()).await.unwrap();

        let err = rotate_dek(&wrong_kek, &new_kek, &dk, "new:v1".into())
            .await
            .unwrap_err();

        let msg = err.to_string();
        assert!(
            msg.contains("DEK rotation failed"),
            "expected rotation error, got: {msg}"
        );
    }

    #[tokio::test]
    async fn rotation_produces_fresh_uuid() {
        let old_kek = FileKek::from_key(KEY_A);
        let new_kek = FileKek::from_key(KEY_B);

        let old_dk = DataKey::generate(&old_kek, "old:v1".into()).await.unwrap();

        let new_dk = rotate_dek(&old_kek, &new_kek, &old_dk, "new:v1".into())
            .await
            .unwrap();

        assert_ne!(new_dk.id, old_dk.id);
    }
}
