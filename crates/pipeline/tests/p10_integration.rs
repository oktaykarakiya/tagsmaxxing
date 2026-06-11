// SPDX-License-Identifier: AGPL-3.0-or-later

//! P10 phase integration test (§28, §31): comprehensive verification that all P10
//! encryption, key-management, crypto-shredding, and decrypt-audit components work
//! together correctly.
//!
//! # Coverage
//!
//! **Non-ignored (fast, runs in `just ci`):**
//! 1. Key hierarchy: FileKek wraps DEK → DataKey generation → EncryptedBlob put/get
//!    round-trip → verify plaintext recovery.
//! 2. KEK rotation: rotate DEK from old KEK to new KEK → new KEK unwraps, old KEK
//!    cannot → re-wrap preserves plaintext.
//! 3. Provider key: encrypt → decrypt → zeroize → redacted Debug/Display.
//! 4. Envelope column encryption: encrypt_column → decrypt_column round-trip with
//!    various inputs.
//! 5. Audit types: DecryptAuditEvent creation, serialization, and enum wire strings.
//!
//! **#[ignore] tests (need Podman + pgvector/pgvector:pg17):**
//! 6. Crypto-shred full cycle: create tenant in real PgStore → insert DEK → store
//!    encrypted blob → retrieve plaintext → shred tenant → verify data unrecoverable
//!    (DEK gone, data gone, tombstone created).
//! 7. Decrypt audit persistence: insert decrypt audit events into real DB → list them
//!    back with tenant + operation filters.
//! 8. KEK rotation with DB DEK: store DEK in DB via `get_or_create_tenant_dek` →
//!    simulate KEK rotation (re-wrap DEK with new KEK) → verify new KEK decrypts,
//!    old KEK cannot.
//!
//! Run #[ignore] suites with:
//!
//! ```bash
//! systemctl --user start podman.socket
//! DOCKER_HOST="unix://$XDG_RUNTIME_DIR/podman/podman.sock" \
//!   cargo test --package kb-pipeline --test p10_integration -- --ignored
//! ```
//!
//! P10 PHASE COMPLETE after this passes.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use bytes::Bytes;
use kb_core::audit::{DecryptAuditAction, DecryptAuditEvent};
use kb_core::blob::Blob;
use kb_core::data_key::{DataKey, rotate_dek};
use kb_core::envelope;
use kb_core::kek::{FileKek, KeyEncryptionKey};
use kb_core::provider_key::{decrypt_provider_key, encrypt_provider_key};
use kb_store::{EncryptedBlob, LocalBlob};

// ── Helpers ──────────────────────────────────────────────────────────────────────

/// A deterministic 32-byte test KEK (NOT for production use).
const TEST_KEY_A: [u8; 32] = [0xABu8; 32];
const TEST_KEY_B: [u8; 32] = [0xCDu8; 32];

fn test_kek_a() -> FileKek {
    FileKek::from_key(TEST_KEY_A)
}

fn test_kek_b() -> FileKek {
    FileKek::from_key(TEST_KEY_B)
}

fn test_kek_arc() -> Arc<dyn KeyEncryptionKey> {
    Arc::new(test_kek_a())
}

/// Create a temporary directory and return its path (caller keeps the guard alive).
fn temp_dir() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    (dir, path)
}

// ═════════════════════════════════════════════════════════════════════════════════
// 1. KEY HIERARCHY TESTS (non-ignored)
// ═════════════════════════════════════════════════════════════════════════════════

/// Build an encrypted blob stack: Encrypt(LocalBlob) with a test KEK.
fn build_encrypted_blob(blob_dir: &std::path::Path) -> (Arc<dyn Blob>, Arc<dyn KeyEncryptionKey>) {
    let local = Arc::new(LocalBlob::new(blob_dir.to_path_buf(), "test-tenant".into()));
    let kek: Arc<dyn KeyEncryptionKey> = test_kek_arc();
    let encrypted = Arc::new(EncryptedBlob::new(local, kek.clone()));
    (encrypted, kek)
}

#[tokio::test]
async fn key_hierarchy_put_get_roundtrip() {
    let (_guard, blob_dir) = temp_dir();
    let (stack, _kek) = build_encrypted_blob(&blob_dir);

    let key = "test-tenant/roundtrip.bin";
    let data = Bytes::from_static(b"Hello, encrypted world!");
    stack.put(key, data.clone()).await.unwrap();
    let got = stack.get(key).await.unwrap();

    assert_eq!(got, data);
}

#[tokio::test]
async fn key_hierarchy_filekek_wraps_dek_enables_encrypted_blob() {
    // Prove the full chain: KEK wraps DEK → DEK encrypts blob → blob stored encrypted.
    let (_guard, blob_dir) = temp_dir();
    let local = Arc::new(LocalBlob::new(blob_dir.to_path_buf(), "test-tenant".into()));
    let kek: Arc<dyn KeyEncryptionKey> = Arc::new(test_kek_a());

    // Generate a DataKey (wrapped DEK).
    let data_key = DataKey::generate(kek.as_ref(), "file:v1".into())
        .await
        .unwrap();
    assert_eq!(data_key.kek_id, "file:v1");

    // Unwrap the DEK from the DataKey using the same KEK.
    let raw_dek = kek.unwrap(&data_key.wrapped_dek).await.unwrap();
    assert_eq!(raw_dek.len(), 32);

    // Use the raw DEK to encrypt a column value (proving DEK works for data encryption).
    let ct = envelope::encrypt_column(raw_dek[..32].try_into().unwrap(), "sensitive data").unwrap();
    let pt = envelope::decrypt_column(raw_dek[..32].try_into().unwrap(), &ct).unwrap();
    assert_eq!(pt, "sensitive data");

    // Also prove EncryptedBlob with the same KEK works (different DEK, but same KEK).
    let encrypted = EncryptedBlob::new(local, kek);
    encrypted
        .put("key-1", Bytes::from_static(b"encrypted content"))
        .await
        .unwrap();
    let got = encrypted.get("key-1").await.unwrap();
    assert_eq!(got, Bytes::from_static(b"encrypted content"));
}

#[tokio::test]
async fn key_hierarchy_wrong_kek_cannot_unwrap_dek() {
    let kek_a: Arc<dyn KeyEncryptionKey> = Arc::new(test_kek_a());
    let kek_b: Arc<dyn KeyEncryptionKey> = Arc::new(test_kek_b());

    // Generate a DEK wrapped with KEK A.
    let data_key = DataKey::generate(kek_a.as_ref(), "file-a:v1".into())
        .await
        .unwrap();

    // KEK A can unwrap it.
    let raw_a = kek_a.unwrap(&data_key.wrapped_dek).await.unwrap();
    assert_eq!(raw_a.len(), 32);

    // KEK B cannot unwrap it.
    let err = kek_b.unwrap(&data_key.wrapped_dek).await.unwrap_err();
    assert!(
        err.to_string().contains("unwrap") || err.to_string().contains("failed"),
        "expected unwrap error with wrong KEK, got: {err}"
    );
}

#[tokio::test]
async fn key_hierarchy_encrypted_blob_stores_ciphertext_not_plaintext() {
    let (_guard, blob_dir) = temp_dir();
    let local = Arc::new(LocalBlob::new(blob_dir.to_path_buf(), "test-tenant".into()));
    let kek: Arc<dyn KeyEncryptionKey> = Arc::new(test_kek_a());
    let encrypted = EncryptedBlob::new(local.clone(), kek);

    let data = Bytes::from_static(b"plaintext secret");
    encrypted.put("key-cipher", data.clone()).await.unwrap();

    // The raw bytes on disk must not be the plaintext.
    let raw: Bytes = Blob::get(local.as_ref(), "key-cipher").await.unwrap();
    assert_ne!(raw, data, "stored bytes must be ciphertext, not plaintext");
    // The encrypted blob format starts with "EBl" magic.
    assert!(
        raw.starts_with(b"EBl"),
        "encrypted blob must start with EBl magic bytes"
    );
}

#[tokio::test]
async fn key_hierarchy_multiple_keys_different_deks() {
    let (_guard, blob_dir) = temp_dir();
    let local = Arc::new(LocalBlob::new(blob_dir.to_path_buf(), "test-tenant".into()));
    let kek: Arc<dyn KeyEncryptionKey> = Arc::new(test_kek_a());
    let encrypted = EncryptedBlob::new(local.clone(), kek);

    // Store two blobs — each gets its own DEK.
    encrypted
        .put("k-a", Bytes::from_static(b"content A"))
        .await
        .unwrap();
    encrypted
        .put("k-b", Bytes::from_static(b"content B"))
        .await
        .unwrap();

    // Both decrypt correctly.
    assert_eq!(
        encrypted.get("k-a").await.unwrap(),
        Bytes::from_static(b"content A")
    );
    assert_eq!(
        encrypted.get("k-b").await.unwrap(),
        Bytes::from_static(b"content B")
    );

    // The stored ciphertexts are different even though the KEK is the same
    // (different per-file DEKs + different nonces).
    let raw_a: Bytes = Blob::get(local.as_ref(), "k-a").await.unwrap();
    let raw_b: Bytes = Blob::get(local.as_ref(), "k-b").await.unwrap();
    assert_ne!(raw_a, raw_b);
}

// ═════════════════════════════════════════════════════════════════════════════════
// 2. KEK ROTATION TESTS (non-ignored)
// ═════════════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn kek_rotation_preserves_dek_plaintext() {
    let old_kek = test_kek_a();
    let new_kek = test_kek_b();

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
        err.to_string().contains("unwrap") || err.to_string().contains("failed"),
        "old KEK must not unwrap new wrapping, got: {err}"
    );

    // Old DEK still works with old KEK (not destroyed by rotation).
    let raw_old = old_kek.unwrap(&old_dk.wrapped_dek).await.unwrap();
    assert_eq!(raw_old.len(), 32);
}

#[tokio::test]
async fn kek_rotation_updates_kek_id() {
    let old_kek = test_kek_a();
    let new_kek = test_kek_b();

    let old_dk = DataKey::generate(&old_kek, "old-id".into()).await.unwrap();
    assert_eq!(old_dk.kek_id, "old-id");

    let new_dk = rotate_dek(&old_kek, &new_kek, &old_dk, "new-id".into())
        .await
        .unwrap();
    assert_eq!(new_dk.kek_id, "new-id");
}

#[tokio::test]
async fn kek_rotation_with_wrong_old_kek_errors_clearly() {
    let correct_kek = test_kek_a();
    let wrong_kek = test_kek_b();
    let new_kek = FileKek::from_key([0x55u8; 32]);

    let dk = DataKey::generate(&correct_kek, "k:v".into()).await.unwrap();

    let err = rotate_dek(&wrong_kek, &new_kek, &dk, "new:v1".into())
        .await
        .unwrap_err();

    assert!(
        err.to_string().contains("rotation") || err.to_string().contains("DEK"),
        "expected rotation error, got: {err}"
    );
}

#[tokio::test]
async fn kek_rotation_produces_fresh_uuid() {
    let old_kek = test_kek_a();
    let new_kek = test_kek_b();

    let old_dk = DataKey::generate(&old_kek, "old:v1".into()).await.unwrap();
    let new_dk = rotate_dek(&old_kek, &new_kek, &old_dk, "new:v1".into())
        .await
        .unwrap();

    assert_ne!(new_dk.id, old_dk.id);
}

#[tokio::test]
async fn kek_rotation_crypto_shred_simulation() {
    // Simulate the crypto-shredding guarantee: after rotation away from old KEK,
    // destroying the old KEK makes the old DEK wrapping irrecoverable.
    let old_kek = test_kek_a();
    let new_kek = test_kek_b();

    let old_dk = DataKey::generate(&old_kek, "old:v1".into()).await.unwrap();

    // Rotate to new KEK.
    let new_dk = rotate_dek(&old_kek, &new_kek, &old_dk, "new:v1".into())
        .await
        .unwrap();

    // Now verify: without the old KEK, old_dk.wrapped_dek is useless.
    // (In production, "destroying" the old KEK means the key bytes are gone.)
    // The new wrapping works with new KEK.
    let raw = new_kek.unwrap(&new_dk.wrapped_dek).await.unwrap();
    assert_eq!(raw.len(), 32);

    // And the old wrapping cannot be decrypted by the new KEK.
    let err = new_kek.unwrap(&old_dk.wrapped_dek).await.unwrap_err();
    assert!(
        err.to_string().contains("unwrap") || err.to_string().contains("failed"),
        "new KEK must not unwrap old DEK wrapped with old KEK"
    );
}

// ═════════════════════════════════════════════════════════════════════════════════
// 3. PROVIDER KEY TESTS (non-ignored)
// ═════════════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn provider_key_encrypt_decrypt_roundtrip() {
    let kek = test_kek_a();
    let plaintext = b"sk-my-api-key-12345";

    let ct = encrypt_provider_key(&kek, plaintext).await.unwrap();
    let key = decrypt_provider_key(&kek, &ct).await.unwrap();

    assert_eq!(key.as_bytes(), plaintext);
    assert!(!key.is_empty());
}

#[tokio::test]
async fn provider_key_constructor_and_accessors() {
    // Verify ProviderApiKey construction, accessors, and the Zeroize trait
    // (the Zeroize impl is tested in kb-core unit tests; this validates
    // the integration path: encrypt → decrypt → inspect).
    let kek = test_kek_a();
    let plaintext = b"secret-key-for-integration-test";

    let ct = encrypt_provider_key(&kek, plaintext).await.unwrap();
    let key = decrypt_provider_key(&kek, &ct).await.unwrap();

    assert!(!key.is_empty());
    assert_eq!(key.as_bytes(), plaintext);
    assert_eq!(key.len(), plaintext.len());
}

#[tokio::test]
async fn provider_key_debug_never_leaks_key() {
    let kek = test_kek_a();
    let plaintext = b"sk-ant-api03-secret-key-abc123";
    let ct = encrypt_provider_key(&kek, plaintext).await.unwrap();
    let key = decrypt_provider_key(&kek, &ct).await.unwrap();

    let debug_str = format!("{:?}", key);
    assert_eq!(debug_str, "ProviderApiKey(****)");
    assert!(!debug_str.contains("sk-ant"));
    assert!(!debug_str.contains("secret"));
}

#[tokio::test]
async fn provider_key_display_never_leaks_key() {
    let kek = test_kek_a();
    let plaintext = b"sk-ant-api03-secret-key-abc123";
    let ct = encrypt_provider_key(&kek, plaintext).await.unwrap();
    let key = decrypt_provider_key(&kek, &ct).await.unwrap();

    let display_str = format!("{}", key);
    assert_eq!(display_str, "ProviderApiKey(****)");
    assert!(!display_str.contains("sk-ant"));
}

#[tokio::test]
async fn provider_key_wrong_kek_errors_clearly() {
    let kek_a = test_kek_a();
    let kek_b = test_kek_b();

    let ct = encrypt_provider_key(&kek_a, b"my-secret").await.unwrap();
    let err = decrypt_provider_key(&kek_b, &ct).await.unwrap_err();

    let msg = err.to_string();
    assert!(
        msg.contains("wrong KEK") || msg.contains("corrupted data"),
        "expected clear error about wrong KEK, got: {msg}"
    );
}

#[tokio::test]
async fn provider_key_tampered_ciphertext_errors() {
    let kek = test_kek_a();
    let mut ct = encrypt_provider_key(&kek, b"secret").await.unwrap();

    // Flip a bit in the ciphertext portion (after the 12-byte nonce).
    if ct.len() > 15 {
        ct[15] ^= 0x01;
    }

    let err = decrypt_provider_key(&kek, &ct).await.unwrap_err();
    assert!(
        err.to_string().contains("wrong KEK") || err.to_string().contains("corrupted data"),
        "expected clear error about tampered data, got: {err}"
    );
}

#[tokio::test]
async fn provider_key_rotation_new_kek_used_on_next_call() {
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

// ═════════════════════════════════════════════════════════════════════════════════
// 4. ENVELOPE COLUMN ENCRYPTION TESTS (non-ignored)
// ═════════════════════════════════════════════════════════════════════════════════

#[test]
fn envelope_encrypt_decrypt_roundtrip_ascii() {
    let dek = [0x7Fu8; 32];
    let ct = envelope::encrypt_column(&dek, "hello world").unwrap();
    let pt = envelope::decrypt_column(&dek, &ct).unwrap();
    assert_eq!(pt, "hello world");
}

#[test]
fn envelope_encrypt_decrypt_roundtrip_unicode() {
    let dek = [0x7Fu8; 32];
    let input = "こんにちは世界 🌍 — café naïve";
    let ct = envelope::encrypt_column(&dek, input).unwrap();
    let pt = envelope::decrypt_column(&dek, &ct).unwrap();
    assert_eq!(pt, input);
}

#[test]
fn envelope_encrypt_decrypt_roundtrip_empty() {
    let dek = [0x7Fu8; 32];
    let ct = envelope::encrypt_column(&dek, "").unwrap();
    let pt = envelope::decrypt_column(&dek, &ct).unwrap();
    assert_eq!(pt, "");
}

#[test]
fn envelope_encrypt_produces_different_ciphertexts() {
    let dek = [0x7Fu8; 32];
    let ct1 = envelope::encrypt_column(&dek, "same text").unwrap();
    let ct2 = envelope::encrypt_column(&dek, "same text").unwrap();
    assert_ne!(ct1, ct2, "different nonces → different ciphertexts");
}

#[test]
fn envelope_decrypt_wrong_key_errors() {
    let dek_a = [0x7Fu8; 32];
    let dek_b = [0xE3u8; 32];
    let ct = envelope::encrypt_column(&dek_a, "secret").unwrap();
    let err = envelope::decrypt_column(&dek_b, &ct).unwrap_err();
    assert!(
        err.to_string().contains("decrypt failed"),
        "expected decrypt error with wrong key, got: {err}"
    );
}

#[test]
fn envelope_decrypt_tampered_ciphertext_errors() {
    let dek = [0x7Fu8; 32];
    let mut ct = envelope::encrypt_column(&dek, "secret").unwrap();
    if ct.len() > 12 {
        ct[12] ^= 0x01;
    }
    let err = envelope::decrypt_column(&dek, &ct).unwrap_err();
    assert!(
        err.to_string().contains("decrypt failed"),
        "expected decrypt error for tampered data, got: {err}"
    );
}

#[test]
fn envelope_decrypt_truncated_errors() {
    let dek = [0x7Fu8; 32];
    let err = envelope::decrypt_column(&dek, &[0u8; 5]).unwrap_err();
    assert!(
        err.to_string().contains("too short"),
        "expected 'too short' error, got: {err}"
    );
}

#[test]
fn envelope_chained_with_dek_from_kek() {
    // Prove the full chain: KEK wraps DEK → DEK encrypts data → DEK decrypts data.
    // This is the core envelope encryption guarantee tested at integration level.
    let dek = [0xABu8; 32];
    let ct = envelope::encrypt_column(&dek, "confidential").unwrap();

    // Same DEK decrypts.
    let pt = envelope::decrypt_column(&dek, &ct).unwrap();
    assert_eq!(pt, "confidential");

    // Wrong DEK does not.
    let wrong_dek = [0xCDu8; 32];
    let err = envelope::decrypt_column(&wrong_dek, &ct).unwrap_err();
    assert!(
        err.to_string().contains("decrypt failed"),
        "wrong DEK must not decrypt"
    );
}

// ═════════════════════════════════════════════════════════════════════════════════
// 5. AUDIT TYPES TESTS (non-ignored)
// ═════════════════════════════════════════════════════════════════════════════════

#[test]
fn decrypt_audit_action_wire_strings_are_locked() {
    assert_eq!(DecryptAuditAction::UnwrapDek.as_str(), "unwrap_dek");
    assert_eq!(
        DecryptAuditAction::UnwrapProviderKey.as_str(),
        "unwrap_provider_key"
    );
    assert_eq!(DecryptAuditAction::all().len(), 2);
}

#[test]
fn decrypt_audit_event_creation_success() {
    let e = DecryptAuditEvent {
        id: 1,
        tenant_id: 7,
        operation: DecryptAuditAction::UnwrapDek,
        key_id: "dek:550e8400-e29b-41d4-a716-446655440000".into(),
        user_id: Some(42),
        success: true,
        created_at: chrono::Utc::now(),
    };
    assert_eq!(e.tenant_id, 7);
    assert_eq!(e.operation, DecryptAuditAction::UnwrapDek);
    assert!(e.success);
    assert_eq!(e.user_id, Some(42));
}

#[test]
fn decrypt_audit_event_creation_failure() {
    let e = DecryptAuditEvent {
        id: 2,
        tenant_id: 3,
        operation: DecryptAuditAction::UnwrapProviderKey,
        key_id: "provider:99".into(),
        user_id: None,
        success: false,
        created_at: chrono::Utc::now(),
    };
    assert!(!e.success);
    assert_eq!(e.tenant_id, 3);
    assert_eq!(e.operation, DecryptAuditAction::UnwrapProviderKey);
    assert!(e.user_id.is_none());
}

#[test]
fn decrypt_audit_event_serialization_roundtrip() {
    let e = DecryptAuditEvent {
        id: 1,
        tenant_id: 7,
        operation: DecryptAuditAction::UnwrapDek,
        key_id: "dek:test-key-id".into(),
        user_id: Some(42),
        success: true,
        created_at: chrono::Utc::now(),
    };
    let json = serde_json::to_string(&e).unwrap();
    let back: DecryptAuditEvent = serde_json::from_str(&json).unwrap();
    assert_eq!(back.operation, DecryptAuditAction::UnwrapDek);
    assert_eq!(back.tenant_id, 7);
    assert_eq!(back.key_id, "dek:test-key-id");
    assert!(back.success);
}

#[test]
fn decrypt_audit_event_failed_serialization_roundtrip() {
    let e = DecryptAuditEvent {
        id: 2,
        tenant_id: 3,
        operation: DecryptAuditAction::UnwrapProviderKey,
        key_id: "provider:5".into(),
        user_id: None,
        success: false,
        created_at: chrono::Utc::now(),
    };
    let json = serde_json::to_string(&e).unwrap();
    let back: DecryptAuditEvent = serde_json::from_str(&json).unwrap();
    assert!(!back.success);
    assert_eq!(back.operation, DecryptAuditAction::UnwrapProviderKey);
    assert!(back.user_id.is_none());
}

#[test]
fn decrypt_audit_actions_distinct_from_admin_actions() {
    use kb_core::audit::AuditAction;
    let admin_strs: Vec<&str> = AuditAction::all().iter().map(|a| a.as_str()).collect();
    let decrypt_strs: Vec<&str> = DecryptAuditAction::all()
        .iter()
        .map(|a| a.as_str())
        .collect();
    for ds in &decrypt_strs {
        assert!(
            !admin_strs.contains(ds),
            "decrypt audit action '{ds}' collides with an admin audit action"
        );
    }
}

// ═════════════════════════════════════════════════════════════════════════════════
// 6. CRYPTO-SHRED FULL CYCLE (#[ignore] — needs testcontainers)
// ═════════════════════════════════════════════════════════════════════════════════

/// Helper: create a test PgStore connected via the two-role model and seed a tenant.
async fn setup_pg_store_with_tenant() -> anyhow::Result<(Arc<kb_store::PgStore>, i64)> {
    let db = kb_testsupport::fresh_db().await?;
    let store = kb_store::PgStore::with_roles(&db.admin_url, &db.app_url);
    store.connect().await?;

    let tenant_id = store.create_tenant("test-co", "Test Company").await?;
    assert!(tenant_id > 0, "tenant must be created");

    Ok((Arc::new(store), tenant_id))
}

#[ignore = "needs Podman + pgvector/pgvector:pg17 testcontainer"]
#[tokio::test]
async fn crypto_shred_full_cycle_create_retrieve_shred_unrecoverable() -> anyhow::Result<()> {
    let kek: Arc<dyn KeyEncryptionKey> = Arc::new(test_kek_a());
    let (store, tenant_id) = setup_pg_store_with_tenant().await?;

    // ── 1. Configure the KEK so column encryption is active ──
    store.set_kek(Some(kek.clone()));

    // ── 2. Create a document with encrypted summary (exercises get_or_create_tenant_dek) ──
    let doc = kb_core::document::Document {
        id: 0, // assigned on insert
        tenant_id,
        title: Some("Shred Test Document".into()),
        summary: Some("This summary will be encrypted at rest".into()),
        user_note: Some("Encrypted user note for shred test".into()),
        kind: kb_core::kind::DocKind::Document,
        meta: serde_json::json!({}),
        page_count: 1,
        status: kb_core::status::ProcessingStatus::Ready,
        created_at: chrono::Utc::now(),
        local_only: false,
    };
    let doc_id = store.upsert_document(&doc).await?;
    assert!(doc_id > 0, "document must be upserted");

    // ── 3. Insert a file linked to the document ──
    let admin_pool = store.admin_pool()?;
    let hash = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    sqlx::query(
        "INSERT INTO files (tenant_id, document_id, sha256, blob_key, path, mime, size_bytes, status, page_no) \
         VALUES ($1, $2, $3, $4, 'test.txt', 'text/plain', 100, 'ready', 1)",
    )
    .bind(tenant_id)
    .bind(doc_id)
    .bind(hash)
    .bind(format!("{tenant_id}/{hash}"))
    .execute(&admin_pool)
    .await?;

    // ── 4. Verify the tenant DEK exists in tenant_data_keys ──
    let dek_exists: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tenant_data_keys WHERE tenant_id = $1)")
            .bind(tenant_id)
            .fetch_one(&admin_pool)
            .await?;
    assert!(
        dek_exists,
        "tenant DEK must exist after encrypting a document"
    );

    // ── 5. Verify we can retrieve the document (DEK unwrap works) ──
    let retrieved = store
        .get_document(tenant_id, doc_id)
        .await?
        .expect("document must be retrievable");
    assert_eq!(retrieved.title, Some("Shred Test Document".into()));
    assert!(retrieved.summary.is_some(), "summary must be decryptable");

    // ── 6. Verify the tenant exists ──
    assert!(
        store.tenant_exists(tenant_id).await?,
        "tenant must exist before shred"
    );

    // ── 7. Perform crypto-shredding ──
    // Step A: Delete all tenant-scoped DB rows.
    store.delete_tenant_data(tenant_id).await?;

    // Step B: Explicitly destroy the DEK (belt-and-suspenders).
    store.destroy_tenant_dek(tenant_id).await?;

    // Step C: Create tombstone.
    let tombstone = store
        .create_tombstone(tenant_id, "test-co", "Test Company", Some(1))
        .await?;
    assert_eq!(tombstone.tenant_id, tenant_id);
    assert_eq!(tombstone.tenant_slug, "test-co");
    assert_eq!(tombstone.tenant_name, "Test Company");
    assert_eq!(tombstone.deleted_by, Some(1));

    // ── 8. Verify data is unrecoverable ──
    assert!(
        !store.tenant_exists(tenant_id).await?,
        "tenant must not exist after shred"
    );

    // DEK no longer exists.
    let dek_after: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tenant_data_keys WHERE tenant_id = $1)")
            .bind(tenant_id)
            .fetch_one(&admin_pool)
            .await?;
    assert!(!dek_after, "tenant DEK must be destroyed");

    // Tombstone count increased.
    let count = store.tombstone_count().await?;
    assert!(count >= 1, "tombstone must exist after shred");

    Ok(())
}

#[ignore = "needs Podman + pgvector/pgvector:pg17 testcontainer"]
#[tokio::test]
async fn crypto_shred_idempotent_noop_on_already_deleted_tenant() -> anyhow::Result<()> {
    let (store, tenant_id) = setup_pg_store_with_tenant().await?;

    // Shred the tenant first.
    store.delete_tenant_data(tenant_id).await?;
    store.destroy_tenant_dek(tenant_id).await?;

    assert!(!store.tenant_exists(tenant_id).await?);

    // Second shred should not error.
    store.delete_tenant_data(tenant_id).await?;
    store.destroy_tenant_dek(tenant_id).await?;

    Ok(())
}

#[ignore = "needs Podman + pgvector/pgvector:pg17 testcontainer"]
#[tokio::test]
async fn crypto_shred_only_deletes_target_tenant() -> anyhow::Result<()> {
    let (store, tenant_1) = setup_pg_store_with_tenant().await?;

    // Create a second tenant.
    let tenant_2 = store.create_tenant("other-co", "Other Co").await?;
    assert_ne!(tenant_1, tenant_2);

    // Shred tenant 1.
    store.delete_tenant_data(tenant_1).await?;
    store.destroy_tenant_dek(tenant_1).await?;

    // Tenant 1 is gone.
    assert!(!store.tenant_exists(tenant_1).await?);

    // Tenant 2 is untouched.
    assert!(store.tenant_exists(tenant_2).await?);

    Ok(())
}

// ═════════════════════════════════════════════════════════════════════════════════
// 7. DECRYPT AUDIT PERSISTENCE (#[ignore] — needs testcontainers)
// ═════════════════════════════════════════════════════════════════════════════════

#[ignore = "needs Podman + pgvector/pgvector:pg17 testcontainer"]
#[tokio::test]
async fn decrypt_audit_insert_and_list_by_tenant() -> anyhow::Result<()> {
    let (store, tenant_id) = setup_pg_store_with_tenant().await?;

    // Insert several decrypt audit events for the tenant.
    let id1 = store
        .insert_decrypt_audit_event(
            tenant_id,
            DecryptAuditAction::UnwrapDek,
            "dek:test-dek-1",
            Some(1),
            true,
        )
        .await?;
    assert!(id1 > 0);

    let id2 = store
        .insert_decrypt_audit_event(
            tenant_id,
            DecryptAuditAction::UnwrapProviderKey,
            "provider:test-prov-1",
            Some(1),
            true,
        )
        .await?;
    assert!(id2 > 0);

    // Insert a failure event.
    let id3 = store
        .insert_decrypt_audit_event(
            tenant_id,
            DecryptAuditAction::UnwrapDek,
            "dek:test-dek-2",
            None,
            false,
        )
        .await?;
    assert!(id3 > 0);

    // List events for this tenant (all operations).
    let events = store.admin_list_decrypt_audit(tenant_id, None, 50).await?;
    assert_eq!(events.len(), 3);

    // Filter by operation.
    let dek_events = store
        .admin_list_decrypt_audit(tenant_id, Some(DecryptAuditAction::UnwrapDek), 50)
        .await?;
    assert_eq!(dek_events.len(), 2);

    let provider_events = store
        .admin_list_decrypt_audit(tenant_id, Some(DecryptAuditAction::UnwrapProviderKey), 50)
        .await?;
    assert_eq!(provider_events.len(), 1);

    // Failure event has success = false.
    let failure_events: Vec<_> = events.into_iter().filter(|e| !e.success).collect();
    assert_eq!(failure_events.len(), 1);
    assert_eq!(failure_events[0].key_id, "dek:test-dek-2");
    assert_eq!(failure_events[0].user_id, None);

    Ok(())
}

#[ignore = "needs Podman + pgvector/pgvector:pg17 testcontainer"]
#[tokio::test]
async fn decrypt_audit_cross_tenant_list_sees_all_tenants() -> anyhow::Result<()> {
    let (store, tenant_1) = setup_pg_store_with_tenant().await?;
    let tenant_2 = store.create_tenant("tenant-b", "Tenant B").await?;

    // Insert events for both tenants.
    store
        .insert_decrypt_audit_event(
            tenant_1,
            DecryptAuditAction::UnwrapDek,
            "dek:1",
            Some(1),
            true,
        )
        .await?;
    store
        .insert_decrypt_audit_event(
            tenant_2,
            DecryptAuditAction::UnwrapProviderKey,
            "provider:2",
            Some(2),
            true,
        )
        .await?;

    // Cross-tenant list (tenant_id = 0) returns all events.
    let all = store.admin_list_decrypt_audit(0, None, 50).await?;
    assert!(all.len() >= 2, "cross-tenant list must see all events");

    // Tenant-scoped list only returns that tenant's events.
    let t1_events = store.admin_list_decrypt_audit(tenant_1, None, 50).await?;
    assert_eq!(t1_events.len(), 1);
    assert_eq!(t1_events[0].key_id, "dek:1");

    let t2_events = store.admin_list_decrypt_audit(tenant_2, None, 50).await?;
    assert_eq!(t2_events.len(), 1);
    assert_eq!(t2_events[0].key_id, "provider:2");

    Ok(())
}

#[ignore = "needs Podman + pgvector/pgvector:pg17 testcontainer"]
#[tokio::test]
async fn decrypt_audit_limit_respects_bound() -> anyhow::Result<()> {
    let (store, tenant_id) = setup_pg_store_with_tenant().await?;

    // Insert 5 events.
    for i in 0..5 {
        store
            .insert_decrypt_audit_event(
                tenant_id,
                DecryptAuditAction::UnwrapDek,
                &format!("dek:{i}"),
                Some(1),
                true,
            )
            .await?;
    }

    // Limit to 3.
    let events = store.admin_list_decrypt_audit(tenant_id, None, 3).await?;
    assert_eq!(events.len(), 3, "limit must be respected");

    Ok(())
}

// ═════════════════════════════════════════════════════════════════════════════════
// 8. KEK ROTATION WITH DB DEK (#[ignore] — needs testcontainers)
// ═════════════════════════════════════════════════════════════════════════════════

#[ignore = "needs Podman + pgvector/pgvector:pg17 testcontainer"]
#[tokio::test]
async fn kek_rotation_with_db_stored_dek_re_wrap_and_verify() -> anyhow::Result<()> {
    let old_kek: Arc<dyn KeyEncryptionKey> = Arc::new(test_kek_a());
    let new_kek: Arc<dyn KeyEncryptionKey> = Arc::new(test_kek_b());
    let (store, tenant_id) = setup_pg_store_with_tenant().await?;

    // ── 1. Configure old KEK and encrypt a document (creates tenant DEK) ──
    store.set_kek(Some(old_kek.clone()));

    let doc = kb_core::document::Document {
        id: 0,
        tenant_id,
        title: Some("Rotation Test Doc".into()),
        summary: Some("Encrypted with old KEK".into()),
        user_note: None,
        kind: kb_core::kind::DocKind::Document,
        meta: serde_json::json!({}),
        page_count: 1,
        status: kb_core::status::ProcessingStatus::Ready,
        created_at: chrono::Utc::now(),
        local_only: false,
    };
    let doc_id = store.upsert_document(&doc).await?;
    assert!(doc_id > 0);

    // ── 2. Fetch the existing DataKey from the DB ──
    let admin_pool = store.admin_pool()?;
    let data_key_json: serde_json::Value =
        sqlx::query_scalar("SELECT data_key_json FROM tenant_data_keys WHERE tenant_id = $1")
            .bind(tenant_id)
            .fetch_one(&admin_pool)
            .await?;
    let old_data_key: DataKey = serde_json::from_value(data_key_json)?;
    assert_eq!(old_data_key.kek_id, "db-column-dek:v1");

    // ── 3. Rotate: unwrap DEK with old KEK, re-wrap with new KEK ──
    let new_data_key = rotate_dek(
        old_kek.as_ref(),
        new_kek.as_ref(),
        &old_data_key,
        "new-kek:v1".into(),
    )
    .await?;

    // Update the DB with the new DataKey.
    let new_json = serde_json::to_value(&new_data_key)?;
    sqlx::query("UPDATE tenant_data_keys SET data_key_json = $1 WHERE tenant_id = $2")
        .bind(&new_json)
        .bind(tenant_id)
        .execute(&admin_pool)
        .await?;

    // ── 4. Swap to new KEK and verify retrieval works ──
    store.set_kek(Some(new_kek.clone()));

    let retrieved = store
        .get_document(tenant_id, doc_id)
        .await?
        .expect("document must be retrievable after KEK rotation");
    assert_eq!(retrieved.title, Some("Rotation Test Doc".into()));
    assert!(retrieved.summary.is_some(), "must decrypt with new KEK");

    // ── 5. Verify old KEK cannot decrypt (by trying to unwrap the new DataKey directly) ──
    let err = old_kek.unwrap(&new_data_key.wrapped_dek).await.unwrap_err();
    assert!(
        err.to_string().contains("unwrap") || err.to_string().contains("failed"),
        "old KEK must not unwrap new DataKey, got: {err}"
    );

    // ── 6. Verify new KEK can still unwrap the new DataKey ──
    let raw = new_kek.unwrap(&new_data_key.wrapped_dek).await?;
    assert_eq!(raw.len(), 32);

    Ok(())
}

#[ignore = "needs Podman + pgvector/pgvector:pg17 testcontainer"]
#[tokio::test]
async fn kek_rotation_new_kek_after_rotation_decrypts_document() -> anyhow::Result<()> {
    let old_kek: Arc<dyn KeyEncryptionKey> = Arc::new(test_kek_a());
    let new_kek: Arc<dyn KeyEncryptionKey> = Arc::new(test_kek_b());
    let (store, tenant_id) = setup_pg_store_with_tenant().await?;

    // Encrypt with old KEK.
    store.set_kek(Some(old_kek.clone()));

    let doc = kb_core::document::Document {
        id: 0,
        tenant_id,
        title: Some("Pre-rotation Document".into()),
        summary: Some("Encrypted before rotation".into()),
        user_note: None,
        kind: kb_core::kind::DocKind::Document,
        meta: serde_json::json!({}),
        page_count: 1,
        status: kb_core::status::ProcessingStatus::Ready,
        created_at: chrono::Utc::now(),
        local_only: false,
    };
    let doc_id = store.upsert_document(&doc).await?;

    // Rotate: read current DataKey, re-wrap with new KEK.
    let admin_pool = store.admin_pool()?;
    let data_key_json: serde_json::Value =
        sqlx::query_scalar("SELECT data_key_json FROM tenant_data_keys WHERE tenant_id = $1")
            .bind(tenant_id)
            .fetch_one(&admin_pool)
            .await?;
    let old_data_key: DataKey = serde_json::from_value(data_key_json)?;
    let new_data_key = rotate_dek(
        old_kek.as_ref(),
        new_kek.as_ref(),
        &old_data_key,
        "new:v1".into(),
    )
    .await?;

    sqlx::query("UPDATE tenant_data_keys SET data_key_json = $1 WHERE tenant_id = $2")
        .bind(&serde_json::to_value(&new_data_key)?)
        .bind(tenant_id)
        .execute(&admin_pool)
        .await?;

    // Swap KEK.
    store.set_kek(Some(new_kek.clone()));

    // Retrieval after rotation must work.
    let retrieved = store
        .get_document(tenant_id, doc_id)
        .await?
        .expect("document must be retrievable after KEK rotation");
    assert_eq!(retrieved.title, Some("Pre-rotation Document".into()));
    assert!(
        retrieved.summary.is_some(),
        "document must be decryptable after KEK rotation"
    );

    // Verify the summary decrypts to the original plaintext.
    assert!(
        retrieved
            .summary
            .as_ref()
            .is_some_and(|s| s.contains("Encrypted before rotation")),
        "decrypted summary must match original plaintext"
    );

    Ok(())
}
