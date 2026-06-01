//! HashiCorp Vault Transit-backed [`KeyEncryptionKey`](kb_core::kek::KeyEncryptionKey)
//! implementation (plan §24, §28, P10-T1).
//!
//! [`VaultTransitKek`] delegates all cryptographic operations to a Vault server's
//! [Transit secrets engine](https://developer.hashicorp.com/vault/docs/secrets/transit).
//! The master key never enters the application process — only ciphertext travels over
//! the network. This is the production replacement for the file/env-backed [`FileKek`].
//!
//! # API used
//!
//! - **wrap:** `POST /v1/transit/encrypt/{key_name}` with `{"plaintext": "<base64>"}`
//! - **unwrap:** `POST /v1/transit/decrypt/{key_name}` with `{"ciphertext": "<vault-ciphertext>"}`
//!
//! # Hot-swappable configuration
//!
//! The Vault base URL and token are held behind [`arc_swap::ArcSwap`] so they can be
//! updated at runtime without a restart. Callers resolve the current values at each
//! `wrap`/`unwrap` call — never capture a snapshot at construction time (per the
//! hot-swappable rule in CLAUDE.md).

use std::sync::Arc;

use anyhow::Context;
use arc_swap::ArcSwap;
use async_trait::async_trait;
use base64::Engine;
use kb_core::kek::KeyEncryptionKey;
use serde::{Deserialize, Serialize};

// ── Vault API types ─────────────────────────────────────────────────────────────

/// Request body for Vault Transit encrypt.
#[derive(Debug, Serialize, Deserialize)]
struct VaultEncryptRequest {
    /// Base64-encoded plaintext to encrypt.
    plaintext: String,
}

/// Request body for Vault Transit decrypt.
#[derive(Debug, Serialize, Deserialize)]
struct VaultDecryptRequest {
    /// The ciphertext returned by a previous encrypt call (Vault format).
    ciphertext: String,
}

/// Vault Transit encrypt response wrapper.
#[derive(Debug, Serialize, Deserialize)]
struct VaultEncryptResponse {
    data: VaultEncryptData,
}

#[derive(Debug, Serialize, Deserialize)]
struct VaultEncryptData {
    /// The Vault ciphertext (starts with "vault:v1:...").
    ciphertext: String,
}

/// Vault Transit decrypt response wrapper.
#[derive(Debug, Serialize, Deserialize)]
struct VaultDecryptResponse {
    data: VaultDecryptData,
}

#[derive(Debug, Serialize, Deserialize)]
struct VaultDecryptData {
    /// Base64-encoded plaintext.
    plaintext: String,
}

// ── VaultTransitKek ─────────────────────────────────────────────────────────────

/// A [`KeyEncryptionKey`] backed by HashiCorp Vault's Transit secrets engine.
///
/// Every `wrap`/`unwrap` call makes an HTTP request to the Vault server. The KEK
/// (the transit key) never leaves Vault — the application process only sees Vault's
/// ciphertext, which is opaque to it.
///
/// # Construction
///
/// ```no_run
/// use std::sync::Arc;
/// use kb_store::VaultTransitKek;
///
/// let kek = VaultTransitKek::new(
///     "http://vault:8200",
///     "hvs.token",
///     "my-transit-key",
/// );
/// ```
///
/// # Hot-swappable URL and token
///
/// Use [`Self::set_vault_url`] and [`Self::set_vault_token`] to update the
/// configuration at runtime:
///
/// ```no_run
/// use kb_store::VaultTransitKek;
/// # let kek = VaultTransitKek::new("", "", "").unwrap();
/// kek.set_vault_url("http://vault2:8200");
/// kek.set_vault_token("hvs.new-token");
/// ```
pub struct VaultTransitKek {
    /// Hot-swappable Vault base URL (e.g. `"http://vault:8200"`).
    vault_url: ArcSwap<String>,
    /// Hot-swappable Vault authentication token.
    vault_token: ArcSwap<String>,
    /// The name of the Transit key to use for encrypt/decrypt operations.
    key_name: String,
    /// Shared HTTP client with connection pooling.
    client: reqwest::Client,
}

impl VaultTransitKek {
    /// Create a new `VaultTransitKek`.
    ///
    /// `vault_url` is the base URL of the Vault server (e.g. `"http://vault:8200"`).
    /// `vault_token` is the authentication token for Vault.
    /// `key_name` is the Transit key name to use for wrap/unwrap.
    ///
    /// The HTTP client is created with a 30-second timeout per request.
    ///
    /// # Errors
    /// Returns an error if the HTTP client cannot be built (this should never
    /// happen with the default configuration; it is only possible if the system
    /// lacks a TLS provider when the `rustls-tls` feature is active).
    pub fn new(vault_url: &str, vault_token: &str, key_name: &str) -> anyhow::Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| {
                anyhow::anyhow!("failed to build reqwest client for VaultTransitKek: {e}")
            })?;

        Ok(Self {
            vault_url: ArcSwap::new(Arc::new(vault_url.to_string())),
            vault_token: ArcSwap::new(Arc::new(vault_token.to_string())),
            key_name: key_name.to_string(),
            client,
        })
    }

    /// Hot-swap the Vault base URL.
    ///
    /// Subsequent `wrap`/`unwrap` calls will use the new URL immediately.
    /// No restart is required.
    pub fn set_vault_url(&self, url: &str) {
        self.vault_url.store(Arc::new(url.to_string()));
    }

    /// Hot-swap the Vault authentication token.
    ///
    /// Subsequent `wrap`/`unwrap` calls will use the new token immediately.
    /// No restart is required.
    pub fn set_vault_token(&self, token: &str) {
        self.vault_token.store(Arc::new(token.to_string()));
    }

    /// Return the current Vault base URL.
    #[must_use]
    pub fn vault_url(&self) -> String {
        self.vault_url.load().as_ref().clone()
    }

    /// Return the name of the transit key.
    #[must_use]
    pub fn key_name(&self) -> &str {
        &self.key_name
    }

    // ── Internal helpers ──────────────────────────────────────────────────

    /// Build the full encrypt URL: `{base}/v1/transit/encrypt/{key_name}`.
    fn encrypt_url(base_url: &str, key_name: &str) -> String {
        format!("{base_url}/v1/transit/encrypt/{key_name}")
    }

    /// Build the full decrypt URL: `{base_url}/v1/transit/decrypt/{key_name}`.
    fn decrypt_url(base_url: &str, key_name: &str) -> String {
        format!("{base_url}/v1/transit/decrypt/{key_name}")
    }
}

// ── KeyEncryptionKey impl ──────────────────────────────────────────────────────

#[async_trait]
impl KeyEncryptionKey for VaultTransitKek {
    /// Wrap (encrypt) `plaintext` by delegating to Vault Transit's encrypt endpoint.
    ///
    /// The plaintext is base64-encoded before sending to Vault, per the Transit API
    /// convention. The returned ciphertext is the raw Vault ciphertext bytes (which
    /// include the Vault version prefix like `vault:v1:`).
    ///
    /// # Errors
    /// Returns an error if:
    /// - The base64 encoding of the plaintext fails (should never happen).
    /// - The Vault server is unreachable or returns an error.
    /// - The Vault response cannot be parsed.
    async fn wrap(&self, plaintext: &[u8]) -> anyhow::Result<Vec<u8>> {
        let vault_url = self.vault_url.load();
        let vault_token = self.vault_token.load();

        let encoded = base64::engine::general_purpose::STANDARD.encode(plaintext);
        let req_body = VaultEncryptRequest { plaintext: encoded };

        let url = Self::encrypt_url(&vault_url, &self.key_name);
        let resp = self
            .client
            .post(&url)
            .header("X-Vault-Token", vault_token.as_str())
            .json(&req_body)
            .send()
            .await
            .with_context(|| format!("Vault encrypt request failed for key `{}`", self.key_name))?;

        let status = resp.status();
        let body_text = resp
            .text()
            .await
            .context("failed to read Vault encrypt response body")?;

        if !status.is_success() {
            anyhow::bail!("Vault encrypt returned {status}: {body_text}",);
        }

        let vault_resp: VaultEncryptResponse = serde_json::from_str(&body_text)
            .with_context(|| format!("failed to parse Vault encrypt response: {body_text}"))?;

        Ok(vault_resp.data.ciphertext.into_bytes())
    }

    /// Unwrap (decrypt) `ciphertext` by delegating to Vault Transit's decrypt endpoint.
    ///
    /// The `ciphertext` must be a Vault transit ciphertext (as returned by
    /// [`Self::wrap`]). The returned plaintext is base64-decoded from Vault's response.
    ///
    /// # Errors
    /// Returns an error if:
    /// - The Vault server is unreachable or returns an error.
    /// - The Vault response cannot be parsed.
    /// - The base64 decoding of the returned plaintext fails.
    async fn unwrap(&self, ciphertext: &[u8]) -> anyhow::Result<Vec<u8>> {
        let vault_url = self.vault_url.load();
        let vault_token = self.vault_token.load();

        let ct_str = String::from_utf8(ciphertext.to_vec())
            .context("Vault ciphertext is not valid UTF-8")?;

        let req_body = VaultDecryptRequest { ciphertext: ct_str };

        let url = Self::decrypt_url(&vault_url, &self.key_name);
        let resp = self
            .client
            .post(&url)
            .header("X-Vault-Token", vault_token.as_str())
            .json(&req_body)
            .send()
            .await
            .with_context(|| format!("Vault decrypt request failed for key `{}`", self.key_name))?;

        let status = resp.status();
        let body_text = resp
            .text()
            .await
            .context("failed to read Vault decrypt response body")?;

        if !status.is_success() {
            anyhow::bail!("Vault decrypt returned {status}: {body_text}",);
        }

        let vault_resp: VaultDecryptResponse = serde_json::from_str(&body_text)
            .with_context(|| format!("failed to parse Vault decrypt response: {body_text}"))?;

        base64::engine::general_purpose::STANDARD
            .decode(vault_resp.data.plaintext.as_bytes())
            .context("failed to base64-decode Vault decrypt response plaintext")
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use axum::Json;
    use axum::Router;
    use axum::extract::{Path, State};
    use axum::routing::post;
    use kb_core::kek::FileKek;

    use super::*;

    // ── Mock Vault Transit server ─────────────────────────────────────────

    /// Shared state for the mock Vault server, tracking call counts for test
    /// assertions.
    #[derive(Default, Clone)]
    struct MockVaultState {
        encrypt_calls: Arc<AtomicUsize>,
        decrypt_calls: Arc<AtomicUsize>,
    }

    /// In-process mock Vault Transit server for deterministic testing.
    struct MockVault {
        addr: SocketAddr,
        shutdown: Option<tokio::sync::oneshot::Sender<()>>,
        state: MockVaultState,
    }

    impl MockVault {
        /// Start a new mock Vault server on a random local port.
        async fn start() -> Self {
            let state = MockVaultState::default();
            let app_state = state.clone();

            let app = Router::new()
                .route("/v1/transit/encrypt/{key_name}", post(handle_encrypt))
                .route("/v1/transit/decrypt/{key_name}", post(handle_decrypt))
                .with_state(app_state);

            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();

            let (tx, rx) = tokio::sync::oneshot::channel::<()>();

            tokio::spawn(async move {
                axum::serve(listener, app)
                    .with_graceful_shutdown(async {
                        let _ = rx.await;
                    })
                    .await
                    .unwrap();
            });

            MockVault {
                addr,
                shutdown: Some(tx),
                state,
            }
        }

        /// Base URL for the mock server.
        fn base_url(&self) -> String {
            format!("http://{}", self.addr)
        }

        /// Number of encrypt calls received.
        fn encrypt_calls(&self) -> usize {
            self.state.encrypt_calls.load(Ordering::SeqCst)
        }

        /// Number of decrypt calls received.
        fn decrypt_calls(&self) -> usize {
            self.state.decrypt_calls.load(Ordering::SeqCst)
        }
    }

    impl Drop for MockVault {
        fn drop(&mut self) {
            // Fire-and-forget: best-effort shutdown.
            if let Some(tx) = self.shutdown.take() {
                let _ = tx.send(());
            }
        }
    }

    /// Mock encrypt handler: base64-decode the plaintext, then "encrypt" it by
    /// wrapping in a mock Vault ciphertext string that encodes the original bytes.
    async fn handle_encrypt(
        Path(key_name): Path<String>,
        State(state): State<MockVaultState>,
        Json(body): Json<VaultEncryptRequest>,
    ) -> Json<VaultEncryptResponse> {
        state.encrypt_calls.fetch_add(1, Ordering::SeqCst);

        // Make the ciphertext deterministic for testing: include key_name so
        // tests can verify it was used.
        let ciphertext = format!("vault:v1:{}:{}", key_name, &body.plaintext);

        Json(VaultEncryptResponse {
            data: VaultEncryptData { ciphertext },
        })
    }

    /// Mock decrypt handler: extract the base64-encoded plaintext from the mock
    /// ciphertext format, validate the key_name, and return it.
    async fn handle_decrypt(
        Path(key_name): Path<String>,
        State(state): State<MockVaultState>,
        Json(body): Json<VaultDecryptRequest>,
    ) -> Result<Json<VaultDecryptResponse>, (axum::http::StatusCode, String)> {
        state.decrypt_calls.fetch_add(1, Ordering::SeqCst);

        // Expected format: "vault:v1:{key_name}:{base64_plaintext}"
        let parts: Vec<&str> = body.ciphertext.splitn(4, ':').collect();
        if parts.len() != 4 || parts[0] != "vault" || parts[1] != "v1" {
            return Err((
                axum::http::StatusCode::BAD_REQUEST,
                "invalid ciphertext format".into(),
            ));
        }

        if parts[2] != key_name {
            return Err((
                axum::http::StatusCode::BAD_REQUEST,
                format!(
                    "key name mismatch: expected `{key_name}`, got `{}`",
                    parts[2]
                ),
            ));
        }

        Ok(Json(VaultDecryptResponse {
            data: VaultDecryptData {
                plaintext: parts[3].to_string(),
            },
        }))
    }

    // ── URL building ──────────────────────────────────────────────────────

    #[test]
    fn encrypt_url_format() {
        let url = VaultTransitKek::encrypt_url("http://vault:8200", "my-key");
        assert_eq!(url, "http://vault:8200/v1/transit/encrypt/my-key");
    }

    #[test]
    fn decrypt_url_format() {
        let url = VaultTransitKek::decrypt_url("http://vault:8200", "my-key");
        assert_eq!(url, "http://vault:8200/v1/transit/decrypt/my-key");
    }

    #[test]
    fn encrypt_url_trailing_slash_in_base() {
        // If the base URL has a trailing slash, the path should still work
        // (Vault server is tolerant of this).
        let url = VaultTransitKek::encrypt_url("http://vault:8200/", "key");
        assert_eq!(url, "http://vault:8200//v1/transit/encrypt/key");
    }

    // ── Construction ──────────────────────────────────────────────────────

    #[test]
    fn construction_stores_fields() {
        let kek = VaultTransitKek::new("http://v:8200", "tok", "my-key").unwrap();
        assert_eq!(kek.vault_url(), "http://v:8200");
        assert_eq!(kek.key_name(), "my-key");
    }

    // ── Hot-swap ──────────────────────────────────────────────────────────

    #[test]
    fn set_vault_url_updates_current() {
        let kek = VaultTransitKek::new("http://old:8200", "tok", "k").unwrap();
        assert_eq!(kek.vault_url(), "http://old:8200");
        kek.set_vault_url("http://new:8200");
        assert_eq!(kek.vault_url(), "http://new:8200");
    }

    #[test]
    fn set_vault_token_updates_future_calls() {
        let kek = VaultTransitKek::new("http://v:8200", "old-token", "k").unwrap();
        // Token is not directly exposed, so we just test the setter doesn't panic.
        kek.set_vault_token("new-token");
    }

    // ── wrap / unwrap round-trip against mock Vault ───────────────────────

    #[tokio::test]
    async fn wrap_unwrap_roundtrip_small() {
        let mock = MockVault::start().await;
        let kek = VaultTransitKek::new(&mock.base_url(), "test-token", "test-key").unwrap();

        let plaintext = b"hello from vault transit";
        let wrapped = kek.wrap(plaintext).await.unwrap();
        let unwrapped = kek.unwrap(&wrapped).await.unwrap();

        assert_eq!(unwrapped, plaintext);
        assert_eq!(mock.encrypt_calls(), 1);
        assert_eq!(mock.decrypt_calls(), 1);
    }

    #[tokio::test]
    async fn wrap_unwrap_roundtrip_32_bytes() {
        let mock = MockVault::start().await;
        let kek = VaultTransitKek::new(&mock.base_url(), "t", "k").unwrap();

        let dek = [0x42u8; 32];
        let wrapped = kek.wrap(&dek).await.unwrap();
        let unwrapped = kek.unwrap(&wrapped).await.unwrap();

        assert_eq!(unwrapped, dek);
    }

    #[tokio::test]
    async fn wrap_unwrap_roundtrip_empty() {
        let mock = MockVault::start().await;
        let kek = VaultTransitKek::new(&mock.base_url(), "t", "k").unwrap();

        let wrapped = kek.wrap(b"").await.unwrap();
        let unwrapped = kek.unwrap(&wrapped).await.unwrap();

        assert_eq!(unwrapped, b"");
    }

    #[tokio::test]
    async fn wrap_unwrap_roundtrip_binary() {
        let mock = MockVault::start().await;
        let kek = VaultTransitKek::new(&mock.base_url(), "t", "k").unwrap();

        let data: Vec<u8> = (0..=255).collect();
        let wrapped = kek.wrap(&data).await.unwrap();
        let unwrapped = kek.unwrap(&wrapped).await.unwrap();

        assert_eq!(unwrapped, data);
    }

    #[tokio::test]
    async fn wrap_unwrap_roundtrip_large() {
        let mock = MockVault::start().await;
        let kek = VaultTransitKek::new(&mock.base_url(), "t", "large-key").unwrap();

        let data = vec![0x3Cu8; 4096];
        let wrapped = kek.wrap(&data).await.unwrap();
        let unwrapped = kek.unwrap(&wrapped).await.unwrap();

        assert_eq!(unwrapped, data);
    }

    #[tokio::test]
    async fn different_key_names_produce_different_ciphertexts() {
        let mock = MockVault::start().await;
        let kek_a = VaultTransitKek::new(&mock.base_url(), "t", "key-a").unwrap();
        let kek_b = VaultTransitKek::new(&mock.base_url(), "t", "key-b").unwrap();

        let data = b"same data";
        let wrapped_a = kek_a.wrap(data).await.unwrap();
        let wrapped_b = kek_b.wrap(data).await.unwrap();

        // Different key names → different ciphertexts.
        assert_ne!(wrapped_a, wrapped_b);
    }

    // ── Hot-swap: URL change is reflected in wrap/unwrap calls ────────────

    #[tokio::test]
    async fn hot_swap_url_is_used_on_next_wrap() {
        let mock1 = MockVault::start().await;
        let mock2 = MockVault::start().await;

        let kek = VaultTransitKek::new(&mock1.base_url(), "t", "k").unwrap();

        // First wrap goes to mock1.
        let w1 = kek.wrap(b"data1").await.unwrap();
        assert_eq!(mock1.encrypt_calls(), 1);
        assert_eq!(mock2.encrypt_calls(), 0);

        // Swap URL to mock2.
        kek.set_vault_url(&mock2.base_url());

        // Second wrap goes to mock2.
        let w2 = kek.wrap(b"data2").await.unwrap();
        assert_eq!(mock1.encrypt_calls(), 1);
        assert_eq!(mock2.encrypt_calls(), 1);

        // Both unwrap correctly via their respective servers.
        // But w1 was encrypted by mock1 — decrypting via mock2 would fail
        // because mock2 doesn't have the same ciphertext format. Switch back.
        kek.set_vault_url(&mock1.base_url());
        let p1 = kek.unwrap(&w1).await.unwrap();
        assert_eq!(p1, b"data1");

        kek.set_vault_url(&mock2.base_url());
        let p2 = kek.unwrap(&w2).await.unwrap();
        assert_eq!(p2, b"data2");
    }

    // ── Error: connection refused ─────────────────────────────────────────

    #[tokio::test]
    async fn connection_refused_returns_error() {
        // Use a random port that nothing is listening on.
        let kek = VaultTransitKek::new("http://127.0.0.1:1", "t", "k").unwrap();
        let err = kek.wrap(b"data").await.unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("Vault encrypt request failed")
                || msg.contains("error")
                || msg.contains("connection"),
            "expected connection error, got: {msg}"
        );
    }

    #[tokio::test]
    async fn unwrap_with_invalid_ciphertext_errors() {
        let mock = MockVault::start().await;
        let kek = VaultTransitKek::new(&mock.base_url(), "t", "k").unwrap();

        // Send a ciphertext that doesn't match the mock's expected format.
        let err = kek
            .unwrap(b"not-a-valid-vault-ciphertext")
            .await
            .unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("Vault decrypt returned") || msg.contains("400"),
            "expected error for invalid ciphertext, got: {msg}"
        );
    }

    // ── Trait object safety ───────────────────────────────────────────────

    #[tokio::test]
    async fn trait_object_roundtrip() {
        let mock = MockVault::start().await;
        let kek: Arc<dyn KeyEncryptionKey> =
            Arc::new(VaultTransitKek::new(&mock.base_url(), "t", "trait-key").unwrap());

        let data = b"trait object test";
        let wrapped = kek.wrap(data).await.unwrap();
        let unwrapped = kek.unwrap(&wrapped).await.unwrap();

        assert_eq!(unwrapped, data);
    }

    // ── Interoperability with FileKek (proves same trait works) ───────────

    #[tokio::test]
    async fn vault_kek_and_file_kek_are_interchangeable() {
        let mock = MockVault::start().await;
        let vault_kek = VaultTransitKek::new(&mock.base_url(), "t", "k").unwrap();
        let file_kek = FileKek::from_key([0xABu8; 32]);

        // Both implement the same trait — they can be stored together.
        let keks: Vec<Arc<dyn KeyEncryptionKey>> = vec![Arc::new(vault_kek), Arc::new(file_kek)];

        let data = b"interchangeable";
        let wrapped = keks[0].wrap(data).await.unwrap();
        let unwrapped = keks[0].unwrap(&wrapped).await.unwrap();
        assert_eq!(unwrapped, data);

        // FileKek works independently.
        let w2 = keks[1].wrap(data).await.unwrap();
        let u2 = keks[1].unwrap(&w2).await.unwrap();
        assert_eq!(u2, data);
    }

    // ── KEK rotation: Vault → FileKek and back ───────────────────────────

    #[tokio::test]
    async fn rotation_vault_to_file_and_back() {
        use kb_core::data_key::{DataKey, rotate_dek};

        let mock = MockVault::start().await;
        let vault_kek = VaultTransitKek::new(&mock.base_url(), "t", "rot-key").unwrap();
        let file_kek = FileKek::from_key([0xAAu8; 32]);

        // Generate a DEK wrapped with Vault.
        let dk_vault = DataKey::generate(&vault_kek, "vault:v1".into())
            .await
            .unwrap();

        // Rotate from Vault to FileKek.
        let dk_file = rotate_dek(&vault_kek, &file_kek, &dk_vault, "file:v1".into())
            .await
            .unwrap();
        assert_eq!(dk_file.kek_id, "file:v1");

        // Rotate back from FileKek to Vault.
        let dk_vault2 = rotate_dek(&file_kek, &vault_kek, &dk_file, "vault:v2".into())
            .await
            .unwrap();
        assert_eq!(dk_vault2.kek_id, "vault:v2");

        // The final DEK can be unwrapped by Vault.
        let raw = vault_kek.unwrap(&dk_vault2.wrapped_dek).await.unwrap();
        assert_eq!(raw.len(), 32);
    }
}
