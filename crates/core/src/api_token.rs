// SPDX-License-Identifier: AGPL-3.0-or-later

//! API tokens for programmatic Bearer-auth access (plan §30, P12-T5).
//!
//! This module defines the [`ApiTokenStore`] trait, the metadata types returned
//! by listing endpoints, and the pure token-generation / hashing functions.
//! Token hashing uses SHA-256 so the plaintext is never persisted in the database.
//!
//! # Token lifecycle
//!
//! 1. **Create** — generate a 32-byte random token → return the **full** token once
//!    to the user (it cannot be recovered later). Store only `SHA-256(token)`.
//! 2. **List** — return metadata (`id`, `name`, `last_used_at`, `expires_at`, `created_at`)
//!    — **never** the full token.
//! 3. **Revoke** — set `revoked = true`; subsequent validations return `None`.
//! 4. **Validate** — hash the incoming Bearer token → look up `token_hash` → check
//!    `revoked = false` and `expires_at > now()` → return [`SessionInfo`].

use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rand::TryRng;
use rand::rngs::SysRng;
use sha2::{Digest, Sha256};

use crate::session::SessionInfo;

/// Number of random bytes in an API token (32 bytes → 64 hex chars).
const API_TOKEN_BYTES: usize = 32;

/// Default API token lifetime: 365 days.
pub const DEFAULT_API_TOKEN_TTL_SECS: u64 = 365 * 86_400;

// ── Types ────────────────────────────────────────────────────────────────────────

/// Metadata for a single API token, returned by the list endpoint.
///
/// **Never** contains the raw token value — the raw token is only returned once
/// at creation time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiTokenMeta {
    /// Primary key.
    pub id: i64,
    /// Human-readable label assigned at creation time.
    pub name: String,
    /// When this token was last used for authentication, if ever.
    pub last_used_at: Option<DateTime<Utc>>,
    /// When this token expires.
    pub expires_at: DateTime<Utc>,
    /// Row creation time.
    pub created_at: DateTime<Utc>,
}

/// The result of a successful [`ApiTokenStore::create_token`] call.
///
/// The `raw_token` is the **only** time the plaintext token is returned to the
/// caller — it is never stored in the database and cannot be recovered later.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateTokenResult {
    /// Primary key of the created token row.
    pub id: i64,
    /// The full plaintext token (64 hex chars). **Show it once** to the user.
    pub raw_token: String,
    /// Human-readable label.
    pub name: String,
    /// Expiration time.
    pub expires_at: DateTime<Utc>,
}

// ── ApiTokenStore trait ──────────────────────────────────────────────────────────

/// The API token persistence contract (plan §30, P12-T5).
///
/// Two implementations ship in `kb-store`:
///
/// * [`InMemoryApiTokenStore`] — `HashMap` + `tokio::RwLock`, zero-setup (dev / tests).
/// * `PgApiTokenStore` — Postgres-backed, survives restarts (production).
///
/// # Thread safety
///
/// The trait requires `Send + Sync` so it can be wrapped in an `Arc` and shared across
/// axum worker tasks.
#[async_trait]
pub trait ApiTokenStore: Send + Sync {
    /// Create a new API token for the given user, returning the **plaintext token**
    /// (it is never stored and cannot be recovered later).
    ///
    /// The caller must supply the user's current `role` and `email_verified` status
    /// so the token can carry them without a per-request DB join.
    ///
    /// `ttl` controls how long the token is valid from `now`. Implementations should
    /// default to [`DEFAULT_API_TOKEN_TTL_SECS`] (365 days) when the caller passes
    /// [`Duration::MAX`] or a zero duration.
    ///
    /// # Errors
    ///
    /// Returns an error when the underlying store is unavailable.
    async fn create_token(
        &self,
        tenant_id: i64,
        user_id: i64,
        user_role: crate::user::UserRole,
        email_verified: bool,
        name: &str,
        ttl: Duration,
    ) -> anyhow::Result<CreateTokenResult>;

    /// List metadata for every non-revoked, non-expired API token belonging to a
    /// tenant. **Never** returns raw token values.
    ///
    /// # Errors
    ///
    /// Returns an error when the underlying store is unavailable.
    async fn list_tokens(&self, tenant_id: i64) -> anyhow::Result<Vec<ApiTokenMeta>>;

    /// Revoke a specific API token by id, scoped to the owning tenant.
    ///
    /// After this call, [`validate_token`](Self::validate_token) must return `None`
    /// for the revoked token. This is a no-op if the token does not exist or belongs
    /// to a different tenant (no error — the caller sees "not found").
    ///
    /// # Errors
    ///
    /// Returns an error when the underlying store is unavailable.
    async fn revoke_token(&self, tenant_id: i64, token_id: i64) -> anyhow::Result<()>;

    /// Validate a raw API token (extracted from the `Authorization: Bearer` header).
    ///
    /// Returns [`SessionInfo`] if the token is known, not revoked, and not expired,
    /// or `None` if the token is unknown, revoked, or expired.
    ///
    /// Implementations should update `last_used_at` on successful validation so the
    /// user can see when each token was last active.
    ///
    /// # Errors
    ///
    /// Returns an error only when the underlying store is unavailable (not for
    /// expired/unknown/revoked tokens — those return `Ok(None)`).
    async fn validate_token(&self, token: &str) -> anyhow::Result<Option<SessionInfo>>;
}

// ── Pure helpers ─────────────────────────────────────────────────────────────────

/// Generate a cryptographically random 32-byte API token, hex-encoded (64 chars).
///
/// This is the single source of API tokens. Every [`ApiTokenStore`] implementation
/// should call this function when creating a new token.
///
/// # Errors
///
/// Returns an error if the OS randomness source fails (extremely rare).
pub fn generate_api_token() -> anyhow::Result<String> {
    let mut bytes = [0u8; API_TOKEN_BYTES];
    SysRng
        .try_fill_bytes(&mut bytes)
        .map_err(|e| anyhow::anyhow!("failed to generate API token: {e}"))?;
    Ok(hex::encode(bytes))
}

/// Hash an API token with SHA-256 for storage.
///
/// The token itself has 256 bits of entropy (32 random bytes), so a simple SHA-256
/// hash is sufficient — no salt or keyed hash is needed (the input is already
/// unguessable).
///
/// # Examples
///
/// ```
/// let token = kb_core::api_token::generate_api_token().unwrap();
/// let hash = kb_core::api_token::hash_api_token(&token);
/// assert_eq!(hash.len(), 64); // SHA-256 hex output
/// ```
#[must_use]
pub fn hash_api_token(token: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(token.as_bytes());
    hex::encode(hasher.finalize())
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    // ── generate_api_token ──────────────────────────────────────────────────

    #[test]
    fn token_is_64_hex_chars() {
        let token = generate_api_token().unwrap();
        assert_eq!(token.len(), 64, "32 bytes hex-encoded = 64 chars");
        assert!(token.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn token_is_unique_per_call() {
        let t1 = generate_api_token().unwrap();
        let t2 = generate_api_token().unwrap();
        let t3 = generate_api_token().unwrap();
        assert_ne!(t1, t2);
        assert_ne!(t1, t3);
        assert_ne!(t2, t3);
    }

    #[test]
    fn token_does_not_contain_non_hex() {
        for _ in 0..100 {
            let token = generate_api_token().unwrap();
            assert!(
                token.chars().all(|c| c.is_ascii_hexdigit()),
                "unexpected char in token: {token}"
            );
        }
    }

    #[test]
    fn token_is_stable_length_across_many_calls() {
        for _ in 0..50 {
            assert_eq!(generate_api_token().unwrap().len(), 64);
        }
    }

    // ── hash_api_token ──────────────────────────────────────────────────────

    #[test]
    fn hash_is_64_hex_chars() {
        let token = generate_api_token().unwrap();
        let hash = hash_api_token(&token);
        assert_eq!(hash.len(), 64, "SHA-256 hex output = 64 chars");
        assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn same_token_produces_same_hash() {
        let token = "test-token-0123456789abcdef0123456789abcdef0123456789abcdef";
        let h1 = hash_api_token(token);
        let h2 = hash_api_token(token);
        assert_eq!(h1, h2);
    }

    #[test]
    fn different_tokens_produce_different_hashes() {
        let t1 = generate_api_token().unwrap();
        let t2 = generate_api_token().unwrap();
        let h1 = hash_api_token(&t1);
        let h2 = hash_api_token(&t2);
        assert_ne!(h1, h2);
    }

    #[test]
    fn hash_is_deterministic_for_known_input() {
        // SHA-256 test vector from FIPS 180-4 for the empty string.
        let hash = hash_api_token("");
        assert_eq!(
            hash,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn hash_of_64_char_token_is_not_the_token_itself() {
        // The hash must differ from the input — a plaintext-equality check
        // would be catastrophic.
        let token = generate_api_token().unwrap();
        let hash = hash_api_token(&token);
        assert_ne!(hash, token);
    }

    // ── DEFAULT_API_TOKEN_TTL_SECS ──────────────────────────────────────────

    #[test]
    fn default_ttl_is_365_days() {
        assert_eq!(DEFAULT_API_TOKEN_TTL_SECS, 31_536_000);
        assert_eq!(
            Duration::from_secs(DEFAULT_API_TOKEN_TTL_SECS),
            Duration::from_secs(365 * 24 * 60 * 60)
        );
    }

    // ── ApiTokenMeta ────────────────────────────────────────────────────────

    #[test]
    fn api_token_meta_debug_format() {
        let meta = ApiTokenMeta {
            id: 42,
            name: "cli-token".into(),
            last_used_at: None,
            expires_at: chrono::Utc::now(),
            created_at: chrono::Utc::now(),
        };
        let dbg = format!("{meta:?}");
        assert!(dbg.contains("42"));
        assert!(dbg.contains("cli-token"));
    }

    // ── CreateTokenResult ───────────────────────────────────────────────────

    #[test]
    fn create_token_result_debug_format() {
        let result = CreateTokenResult {
            id: 7,
            raw_token: "aa".repeat(32),
            name: "my-token".into(),
            expires_at: chrono::Utc::now(),
        };
        let dbg = format!("{result:?}");
        assert!(dbg.contains("7"));
        assert!(dbg.contains("my-token"));
        // The raw token should be present in the debug output since this is
        // the one time it's available to the caller.
        assert!(dbg.contains("aa"));
    }
}
