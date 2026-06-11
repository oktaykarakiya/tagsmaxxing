// SPDX-License-Identifier: AGPL-3.0-or-later

//! Cookie-based session management — the trait and pure helpers.
//!
//! This module defines the [`Session`] record, the [`SessionStore`] async trait (plan §13),
//! and the cryptographically random token generator. The actual HTTP cookie assembly lives in
//! the API layer (P5-T5); this module documents the recommended cookie properties so
//! implementors produce a consistent cookie.
//!
//! # Cookie contract (P5-T5)
//!
//! Implementors setting the session cookie on an HTTP response **must** use these properties
//! (plan §13, P5-T4 acceptance criteria):
//!
//! | Property   | Value                                |
//! |------------|--------------------------------------|
//! | Name       | `__Host-session`                     |
//! | HttpOnly   | `true`                               |
//! | Secure     | `true` (except local dev)            |
//! | SameSite   | `Lax`                                |
//! | Path       | `/`                                  |
//! | Max-Age    | [`DEFAULT_SESSION_TTL_SECS`]          |
//!
//! The `__Host-` prefix signals to browsers that the cookie was set with `Secure` + `Path=/`
//! (the only valid combination), defending against subdomain attacks.

use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rand::TryRng;
use rand::rngs::SysRng;

use crate::user::UserRole;

/// Default session lifetime: 24 hours (plan §13).
pub const DEFAULT_SESSION_TTL_SECS: u64 = 86_400;

/// "Remember me" session lifetime: 30 days (P12-T2).
pub const REMEMBER_ME_SESSION_TTL_SECS: u64 = 30 * 86_400;

/// Number of random bytes in a session token (32 bytes → 64 hex chars).
const SESSION_TOKEN_BYTES: usize = 32;

// ── Session ────────────────────────────────────────────────────────────────────

/// A session row — the server-side record backing a cookie.
///
/// The `id` is the **opaque bearer token** placed in the cookie. It is 32 bytes of
/// `SysRng` output hex-encoded (64 characters). Possession of this token is the sole
/// authentication proof for the session duration, so it must never be logged, URI-encoded,
/// or exposed to JavaScript (the cookie is `HttpOnly`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Session {
    /// Opaque session token (64 hex chars).
    pub id: String,
    /// Owning tenant.
    pub tenant_id: i64,
    /// Authenticated user.
    pub user_id: i64,
    /// Role within the tenant at session creation time.
    pub user_role: UserRole,
    /// Whether the user's email was verified at session creation time (P12-T2).
    pub email_verified: bool,
    /// When this session expires. After this point [`SessionStore::validate`] returns
    /// `None` and the client must re-authenticate.
    pub expires_at: DateTime<Utc>,
    /// Row creation time.
    pub created_at: DateTime<Utc>,
}

// ── SessionInfo ──────────────────────────────────────────────────────────────────

/// The result of a successful [`SessionStore::validate`] call.
///
/// Carries the minimum information the auth middleware needs to populate request
/// extensions: which tenant and user, what role the user holds, and whether their
/// email has been verified.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SessionInfo {
    /// Owning tenant.
    pub tenant_id: i64,
    /// Authenticated user.
    pub user_id: i64,
    /// Role within the tenant.
    pub user_role: UserRole,
    /// Whether the user's email address has been verified (P12-T2).
    pub email_verified: bool,
}

// ── SessionStore trait ─────────────────────────────────────────────────────────

/// The session persistence contract (plan §13, P5-T4).
///
/// Implementations back cookie-based authentication with an opaque bearer token. Two
/// implementations ship in `kb-store`:
///
/// * [`InMemorySessionStore`] — `HashMap` + `tokio::RwLock`, zero-setup (dev / tests).
/// * `PgSessionStore` — Postgres-backed, survives restarts (production).
///
/// # Thread safety
///
/// The trait requires `Send + Sync` so it can be wrapped in an `Arc` and shared across
/// axum worker tasks.
#[async_trait]
pub trait SessionStore: Send + Sync {
    /// Create a new session for the given user, returning the opaque token.
    ///
    /// `ttl` controls how long the session is valid from `now`. `user_role` is the
    /// role of the authenticated user at session-creation time — it is stored in the
    /// session record and returned by [`validate`](Self::validate) so the auth
    /// middleware can inject it into request extensions without a separate DB lookup.
    /// `email_verified` is stored in the session record so the middleware can expose
    /// it without a per-request DB query (P12-T2). Implementations should use
    /// [`DEFAULT_SESSION_TTL_SECS`] unless the caller explicitly overrides it.
    ///
    /// # Errors
    ///
    /// Returns an error when the underlying store is unavailable.
    async fn create(
        &self,
        tenant_id: i64,
        user_id: i64,
        user_role: UserRole,
        email_verified: bool,
        ttl: Duration,
    ) -> anyhow::Result<String>;

    /// Validate a session token, returning [`SessionInfo`] if the session exists and
    /// has not expired, or `None` if the token is unknown, expired, or revoked.
    ///
    /// Expiry is checked **at call time** against the store's clock, so a session that
    /// expires mid-request is rejected by the next validation.
    ///
    /// # Errors
    ///
    /// Returns an error only when the underlying store is unavailable (not for
    /// expired/unknown tokens — those return `Ok(None)`).
    async fn validate(&self, token: &str) -> anyhow::Result<Option<SessionInfo>>;

    /// Extend (slide) the expiration of an existing session.
    ///
    /// Sets `expires_at = now + ttl`. This is a no-op for unknown tokens (no error).
    /// Callers should call this on every authenticated request to implement sliding
    /// expiration.
    ///
    /// # Errors
    ///
    /// Returns an error only when the underlying store is unavailable.
    async fn extend(&self, token: &str, ttl: Duration) -> anyhow::Result<()>;

    /// Revoke (destroy) a session immediately.
    ///
    /// After this call, [`validate`](Self::validate) must return `None` for the token.
    /// This is a no-op for unknown tokens (no error — it's already gone).
    ///
    /// # Errors
    ///
    /// Returns an error only when the underlying store is unavailable.
    async fn revoke(&self, token: &str) -> anyhow::Result<()>;

    /// Revoke **all** sessions belonging to a tenant.
    ///
    /// Used by the crypto-shredding delete-tenant flow (plan §28, P10-T4) to ensure
    /// no stale session token can authenticate against a deleted tenant. After this
    /// call, all sessions for `tenant_id` return `None` from [`validate`](Self::validate).
    ///
    /// This is a no-op if the tenant has no active sessions (no error).
    ///
    /// # Errors
    ///
    /// Returns an error only when the underlying store is unavailable.
    async fn revoke_all_for_tenant(&self, tenant_id: i64) -> anyhow::Result<()>;

    /// Revoke **all** sessions for a specific user within a tenant.
    ///
    /// Used by security-sensitive operations (password reset, role change) to ensure
    /// no stale sessions survive a credential or privilege change. After this call,
    /// all sessions for `(tenant_id, user_id)` return `None` from
    /// [`validate`](Self::validate).
    ///
    /// This is a no-op if the user has no active sessions (no error).
    ///
    /// # Errors
    ///
    /// Returns an error only when the underlying store is unavailable.
    async fn revoke_user_sessions(&self, tenant_id: i64, user_id: i64) -> anyhow::Result<()>;
}

// ── Token generation (pure) ────────────────────────────────────────────────────

/// Generate a cryptographically random 32-byte session token, hex-encoded.
///
/// This is the single source of session tokens. Every [`SessionStore`] implementation
/// should call this function when creating a new session.
///
/// # Errors
///
/// Returns an error if the OS randomness source fails (extremely rare).
pub fn generate_session_token() -> anyhow::Result<String> {
    let mut bytes = [0u8; SESSION_TOKEN_BYTES];
    SysRng
        .try_fill_bytes(&mut bytes)
        .map_err(|e| anyhow::anyhow!("failed to generate session token: {e}"))?;
    Ok(hex::encode(bytes))
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    // ── generate_session_token ──────────────────────────────────────────────

    #[test]
    fn token_is_64_hex_chars() {
        let token = generate_session_token().unwrap();
        assert_eq!(token.len(), 64, "32 bytes hex-encoded = 64 chars");
        // Must be all hex characters.
        assert!(token.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn token_is_unique_per_call() {
        let t1 = generate_session_token().unwrap();
        let t2 = generate_session_token().unwrap();
        let t3 = generate_session_token().unwrap();
        assert_ne!(t1, t2);
        assert_ne!(t1, t3);
        assert_ne!(t2, t3);
    }

    #[test]
    fn token_does_not_contain_non_hex() {
        for _ in 0..100 {
            let token = generate_session_token().unwrap();
            assert!(
                token.chars().all(|c| c.is_ascii_hexdigit()),
                "unexpected char in token: {token}"
            );
        }
    }

    #[test]
    fn token_is_stable_length_across_many_calls() {
        for _ in 0..50 {
            assert_eq!(generate_session_token().unwrap().len(), 64);
        }
    }

    // ── DEFAULT_SESSION_TTL_SECS ────────────────────────────────────────────

    #[test]
    fn default_ttl_is_24_hours() {
        assert_eq!(DEFAULT_SESSION_TTL_SECS, 86_400);
        assert_eq!(
            Duration::from_secs(DEFAULT_SESSION_TTL_SECS),
            Duration::from_secs(24 * 60 * 60)
        );
    }

    #[test]
    fn remember_me_ttl_is_30_days() {
        assert_eq!(REMEMBER_ME_SESSION_TTL_SECS, 2_592_000);
        assert_eq!(
            Duration::from_secs(REMEMBER_ME_SESSION_TTL_SECS),
            Duration::from_secs(30 * 24 * 60 * 60)
        );
    }
}
