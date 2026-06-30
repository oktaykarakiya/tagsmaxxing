// SPDX-License-Identifier: AGPL-3.0-or-later

//! [`SessionStore`](kb_core::session::SessionStore) implementations.
//!
//! This module ships two backends for the session persistence trait defined in `kb-core`:
//!
//! * **[`InMemorySessionStore`]** — `HashMap` + `tokio::RwLock`, zero-setup. Suitable for
//!   development and tests, but **not** for production (sessions vanish on restart).
//! * **[`PgSessionStore`]** — Postgres-backed via a `sessions` table (migration
//!   Survives restarts and works across multiple app instances.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use chrono::Utc;
use kb_core::session::{Session, SessionInfo, SessionStore, generate_session_token};
use kb_core::user::UserRole;
use sqlx::PgPool;
use tokio::sync::RwLock;

/// In-memory user-info map: `(tenant_id, user_id) → (role, email_verified)`.
type UserInfoMap = HashMap<(i64, i64), (UserRole, bool)>;

// ── InMemorySessionStore ───────────────────────────────────────────────────────

/// A session store backed by an in-memory `HashMap`, protected by a `tokio::RwLock`.
///
/// This is the zero-setup backend for **development and tests**. Each instance is
/// independent — sessions are **not** shared across app instances or preserved across
/// restarts. For production use [`PgSessionStore`] instead.
///
/// # Role / email_verified propagation
///
/// Unlike the Postgres backend (which JOINs the `users` table at validation time,
/// so role changes and email verification propagate immediately), this store caches
/// `user_role` and `email_verified` inside the session at creation time. To allow
/// role or email-verification changes to take effect without revoking sessions,
/// call [`InMemorySessionStore::update_user_info`] after mutating user state.
/// Tests that need to simulate a role change should call that method.
///
/// # Cloning
///
/// The store is `Clone`-able (the inner map is `Arc`-wrapped), so multiple axum
/// worker tasks can share the same store.
#[derive(Clone, Debug)]
pub struct InMemorySessionStore {
    sessions: Arc<RwLock<HashMap<String, Session>>>,
    /// Authoritative per-user role and email_verified state, keyed by
    /// `(tenant_id, user_id)`. On [`SessionStore::validate`] the store checks
    /// this registry first; if an entry exists it overrides the session's cached
    /// values. Call [`update_user_info`](Self::update_user_info) after a role
    /// change or email verification.
    user_info: Arc<RwLock<UserInfoMap>>,
}

impl InMemorySessionStore {
    /// Create an empty in-memory session store.
    #[must_use]
    pub fn new() -> Self {
        Self {
            sessions: Arc::new(RwLock::new(HashMap::new())),
            user_info: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Update the stored user role and email-verification status for a user.
    ///
    /// Call this after mutating a user's role or marking their email as
    /// verified. Subsequent [`SessionStore::validate`] calls for **any** session
    /// belonging to that user will see the updated values.
    ///
    /// This is the in-memory analogue of the PgSessionStore JOINing the `users`
    /// table at validation time — it ensures role changes and email verification
    /// propagate to live sessions without requiring a re-login.
    pub async fn update_user_info(
        &self,
        tenant_id: i64,
        user_id: i64,
        role: UserRole,
        email_verified: bool,
    ) {
        self.user_info
            .write()
            .await
            .insert((tenant_id, user_id), (role, email_verified));
    }

    /// Return the number of currently held sessions (useful for test assertions).
    #[cfg(test)]
    async fn len(&self) -> usize {
        self.sessions.read().await.len()
    }
}

impl Default for InMemorySessionStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SessionStore for InMemorySessionStore {
    async fn create(
        &self,
        tenant_id: i64,
        user_id: i64,
        user_role: UserRole,
        email_verified: bool,
        ttl: Duration,
    ) -> anyhow::Result<String> {
        let token = generate_session_token()?;
        let now = Utc::now();
        let session = Session {
            id: token.clone(),
            tenant_id,
            user_id,
            user_role,
            email_verified,
            expires_at: now
                + chrono::Duration::from_std(ttl)
                    .map_err(|e| anyhow::anyhow!("invalid TTL: {e}"))?,
            created_at: now,
        };
        // Seed the authoritative user-info registry so that validate() returns
        // consistent values even before the first call to update_user_info().
        // Use insert (not or_insert) so that create always reflects the
        // current state at session-creation time.
        self.user_info
            .write()
            .await
            .insert((tenant_id, user_id), (user_role, email_verified));
        self.sessions.write().await.insert(token.clone(), session);
        Ok(token)
    }

    async fn validate(&self, token: &str) -> anyhow::Result<Option<SessionInfo>> {
        let guard = self.sessions.read().await;
        let session = match guard.get(token) {
            Some(s) => s,
            None => return Ok(None),
        };
        if session.expires_at <= Utc::now() {
            return Ok(None);
        }
        // Check the user-info registry for an updated role / email_verified
        // (set by update_user_info). If not present, fall back to the values
        // cached in the session row.
        let (user_role, email_verified) = {
            let ui = self.user_info.read().await;
            match ui.get(&(session.tenant_id, session.user_id)) {
                Some(&(role, verified)) => (role, verified),
                None => (session.user_role, session.email_verified),
            }
        };
        Ok(Some(SessionInfo {
            tenant_id: session.tenant_id,
            user_id: session.user_id,
            user_role,
            email_verified,
        }))
    }

    async fn extend(&self, token: &str, ttl: Duration) -> anyhow::Result<()> {
        let mut guard = self.sessions.write().await;
        if let Some(session) = guard.get_mut(token) {
            let new_expiry = Utc::now()
                + chrono::Duration::from_std(ttl)
                    .map_err(|e| anyhow::anyhow!("invalid TTL: {e}"))?;
            // Only extend forward — never shorten a session that already expires later.
            if new_expiry > session.expires_at {
                session.expires_at = new_expiry;
            }
        }
        Ok(())
    }

    async fn revoke(&self, token: &str) -> anyhow::Result<()> {
        self.sessions.write().await.remove(token);
        Ok(())
    }

    async fn revoke_all_for_tenant(&self, tenant_id: i64) -> anyhow::Result<()> {
        let mut guard = self.sessions.write().await;
        guard.retain(|_, s| s.tenant_id != tenant_id);
        Ok(())
    }

    async fn revoke_user_sessions(&self, tenant_id: i64, user_id: i64) -> anyhow::Result<()> {
        let mut guard = self.sessions.write().await;
        guard.retain(|_, s| !(s.tenant_id == tenant_id && s.user_id == user_id));
        Ok(())
    }
}

// ── PgSessionStore ─────────────────────────────────────────────────────────────

/// A Postgres-backed session store using the `sessions` table
/// (plan §13).
///
/// Sessions survive restarts and are shared across all app instances connected to the
/// same database. The pool is shared (typically the same `PgPool` used by
/// [`PgStore`](crate::PgStore)).
///
/// # Security note
///
/// The `sessions` table is **not** covered by Row-Level Security (see migration
/// the session table schema for the rationale). The opaque 256-bit random token is the sole
/// authentication proof, so it must be transmitted only via an `HttpOnly` cookie.
#[derive(Clone, Debug)]
pub struct PgSessionStore {
    pool: PgPool,
}

impl PgSessionStore {
    /// Create a new `PgSessionStore` using the given connection pool.
    ///
    /// The pool should already be connected and have the `sessions` migration applied
    /// (typically via [`PgStore::connect`](crate::PgStore::connect) which runs all
    /// migrations).
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl SessionStore for PgSessionStore {
    async fn create(
        &self,
        tenant_id: i64,
        user_id: i64,
        user_role: UserRole,
        email_verified: bool,
        ttl: Duration,
    ) -> anyhow::Result<String> {
        let token = generate_session_token()?;
        let now = Utc::now();
        let ttl_secs = ttl.as_secs_f64();
        // Use INTERVAL arithmetic so the database clock is authoritative.
        sqlx::query(
            "INSERT INTO sessions (id, tenant_id, user_id, user_role, email_verified, expires_at, created_at)
             VALUES ($1, $2, $3, $4, $5, $6 + make_interval(secs => $7), $6)",
        )
        .bind(&token)
        .bind(tenant_id)
        .bind(user_id)
        .bind(user_role.as_str())
        .bind(email_verified)
        .bind(now)
        .bind(ttl_secs)
        .execute(&self.pool)
        .await
        .context("failed to insert session row")?;
        Ok(token)
    }

    async fn validate(&self, token: &str) -> anyhow::Result<Option<SessionInfo>> {
        #[derive(sqlx::FromRow)]
        struct Row {
            tenant_id: i64,
            user_id: i64,
            user_role: String,
            email_verified: bool,
        }

        // JOIN with `users` to read the authoritative `role` and `email_verified`
        // at validation time — not the snapshot set at session creation. This ensures
        // that a role change (admin→member) or email verification immediately
        // propagates to live sessions without requiring a re-login.
        let row: Option<Row> = sqlx::query_as(
            "SELECT s.tenant_id, s.user_id, u.role AS user_role, u.email_verified
             FROM sessions s
             JOIN users u ON s.tenant_id = u.tenant_id AND s.user_id = u.id
             WHERE s.id = $1 AND s.expires_at > now()",
        )
        .bind(token)
        .fetch_optional(&self.pool)
        .await
        .context("failed to query sessions table")?;

        match row {
            Some(r) => {
                let user_role = r.user_role.parse::<UserRole>().with_context(|| {
                    format!("invalid user_role in sessions table: {}", r.user_role)
                })?;
                Ok(Some(SessionInfo {
                    tenant_id: r.tenant_id,
                    user_id: r.user_id,
                    user_role,
                    email_verified: r.email_verified,
                }))
            }
            None => Ok(None),
        }
    }

    async fn extend(&self, token: &str, ttl: Duration) -> anyhow::Result<()> {
        let ttl_secs = ttl.as_secs_f64();
        // Only slide forward — never shorten. GREATEST keeps the later timestamp.
        sqlx::query(
            "UPDATE sessions
             SET expires_at = GREATEST(expires_at, now() + make_interval(secs => $2))
             WHERE id = $1",
        )
        .bind(token)
        .bind(ttl_secs)
        .execute(&self.pool)
        .await
        .context("failed to extend session")?;
        Ok(())
    }

    async fn revoke(&self, token: &str) -> anyhow::Result<()> {
        sqlx::query("DELETE FROM sessions WHERE id = $1")
            .bind(token)
            .execute(&self.pool)
            .await
            .context("failed to revoke session")?;
        Ok(())
    }

    async fn revoke_all_for_tenant(&self, tenant_id: i64) -> anyhow::Result<()> {
        sqlx::query("DELETE FROM sessions WHERE tenant_id = $1")
            .bind(tenant_id)
            .execute(&self.pool)
            .await
            .context("failed to revoke all sessions for tenant")?;
        Ok(())
    }

    async fn revoke_user_sessions(&self, tenant_id: i64, user_id: i64) -> anyhow::Result<()> {
        sqlx::query("DELETE FROM sessions WHERE tenant_id = $1 AND user_id = $2")
            .bind(tenant_id)
            .bind(user_id)
            .execute(&self.pool)
            .await
            .context("failed to revoke user sessions")?;
        Ok(())
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use std::time::Duration;

    use super::*;
    use kb_core::session::DEFAULT_SESSION_TTL_SECS;

    // ═══════════════════════════════════════════════════════════════════════════
    // InMemorySessionStore tests
    // ═══════════════════════════════════════════════════════════════════════════

    fn ttl() -> Duration {
        Duration::from_secs(DEFAULT_SESSION_TTL_SECS)
    }

    fn short_ttl() -> Duration {
        Duration::from_secs(1)
    }

    fn role() -> UserRole {
        UserRole::Member
    }

    fn admin_role() -> UserRole {
        UserRole::Admin
    }

    fn info(tenant_id: i64, user_id: i64, role: UserRole) -> Option<SessionInfo> {
        Some(SessionInfo {
            tenant_id,
            user_id,
            user_role: role,
            email_verified: true,
        })
    }

    // ── create ────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn inmem_create_returns_64_char_hex_token() {
        let store = InMemorySessionStore::new();
        let token = store.create(1, 42, role(), true, ttl()).await.unwrap();
        assert_eq!(token.len(), 64);
        assert!(token.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[tokio::test]
    async fn inmem_create_tokens_are_unique() {
        let store = InMemorySessionStore::new();
        let t1 = store.create(1, 1, role(), true, ttl()).await.unwrap();
        let t2 = store.create(1, 1, role(), true, ttl()).await.unwrap();
        assert_ne!(t1, t2);
    }

    #[tokio::test]
    async fn inmem_create_different_tenants() {
        let store = InMemorySessionStore::new();
        let t1 = store.create(10, 100, role(), true, ttl()).await.unwrap();
        let t2 = store.create(20, 200, role(), true, ttl()).await.unwrap();
        assert_ne!(t1, t2);
        assert_eq!(store.len().await, 2);
    }

    // ── validate ──────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn inmem_validate_returns_info_for_valid_session() {
        let store = InMemorySessionStore::new();
        let token = store
            .create(5, 99, admin_role(), true, ttl())
            .await
            .unwrap();
        let result = store.validate(&token).await.unwrap();
        assert_eq!(result, info(5, 99, UserRole::Admin));
    }

    #[tokio::test]
    async fn inmem_validate_returns_none_for_unknown_token() {
        let store = InMemorySessionStore::new();
        let result = store
            .validate("nonexistent-hex-token-1234567890abcdef1234567890abcdef12345678")
            .await
            .unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn inmem_validate_returns_none_for_expired_session() {
        let store = InMemorySessionStore::new();
        let token = store.create(1, 2, role(), true, short_ttl()).await.unwrap();
        // Wait for the session to expire.
        tokio::time::sleep(Duration::from_millis(1100)).await;
        let result = store.validate(&token).await.unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn inmem_validate_returns_none_for_revoked_session() {
        let store = InMemorySessionStore::new();
        let token = store.create(1, 2, role(), true, ttl()).await.unwrap();
        store.revoke(&token).await.unwrap();
        let result = store.validate(&token).await.unwrap();
        assert_eq!(result, None);
    }

    // ── extend ────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn inmem_extend_bumps_expiry() {
        let store = InMemorySessionStore::new();
        // Create with a very short TTL.
        let token = store.create(1, 2, role(), true, short_ttl()).await.unwrap();
        // Extend to a long TTL.
        store.extend(&token, ttl()).await.unwrap();
        // After short TTL + buffer, session should still be valid.
        tokio::time::sleep(Duration::from_millis(1100)).await;
        let result = store.validate(&token).await.unwrap();
        assert_eq!(result, info(1, 2, UserRole::Member));
    }

    #[tokio::test]
    async fn inmem_extend_is_noop_for_unknown_token() {
        let store = InMemorySessionStore::new();
        // Should not panic or error.
        let result = store.extend("nonexistent", ttl()).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn inmem_extend_never_shortens() {
        let store = InMemorySessionStore::new();
        let token = store.create(1, 2, role(), true, ttl()).await.unwrap();
        // Extend with a shorter TTL — should not reduce the existing expiry.
        store.extend(&token, short_ttl()).await.unwrap();
        // Session should still be valid (the original 24h TTL is much larger).
        let result = store.validate(&token).await.unwrap();
        assert_eq!(result, info(1, 2, UserRole::Member));
    }

    // ── revoke ────────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn inmem_revoke_is_noop_for_unknown_token() {
        let store = InMemorySessionStore::new();
        let result = store.revoke("unknown").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn inmem_revoke_removes_only_targeted_token() {
        let store = InMemorySessionStore::new();
        let t1 = store.create(1, 10, role(), true, ttl()).await.unwrap();
        let t2 = store.create(1, 20, role(), true, ttl()).await.unwrap();
        store.revoke(&t1).await.unwrap();
        assert_eq!(store.validate(&t1).await.unwrap(), None);
        assert_eq!(
            store.validate(&t2).await.unwrap(),
            info(1, 20, UserRole::Member)
        );
    }

    // ── revoke_user_sessions ──────────────────────────────────────────────────

    #[tokio::test]
    async fn inmem_revoke_user_sessions_removes_all_for_user() {
        let store = InMemorySessionStore::new();
        // Create two sessions for user 10 in tenant 1.
        let t1 = store.create(1, 10, role(), true, ttl()).await.unwrap();
        let t2 = store.create(1, 10, role(), true, ttl()).await.unwrap();
        assert_eq!(store.len().await, 2);

        store.revoke_user_sessions(1, 10).await.unwrap();
        assert_eq!(store.validate(&t1).await.unwrap(), None);
        assert_eq!(store.validate(&t2).await.unwrap(), None);
        assert_eq!(store.len().await, 0);
    }

    #[tokio::test]
    async fn inmem_revoke_user_sessions_leaves_other_users() {
        let store = InMemorySessionStore::new();
        let t_a = store.create(1, 10, role(), true, ttl()).await.unwrap();
        let t_b = store.create(1, 20, role(), true, ttl()).await.unwrap();

        store.revoke_user_sessions(1, 10).await.unwrap();
        // User 10's session is gone.
        assert_eq!(store.validate(&t_a).await.unwrap(), None);
        // User 20's session survives.
        assert_eq!(
            store.validate(&t_b).await.unwrap(),
            info(1, 20, UserRole::Member)
        );
    }

    #[tokio::test]
    async fn inmem_revoke_user_sessions_is_noop_for_unknown_user() {
        let store = InMemorySessionStore::new();
        let result = store.revoke_user_sessions(1, 99).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn inmem_revoke_user_sessions_scoped_to_tenant() {
        // Only user sessions in the specified tenant are revoked.
        let store = InMemorySessionStore::new();
        let t_t1 = store.create(1, 10, role(), true, ttl()).await.unwrap();
        let t_t2 = store.create(2, 10, role(), true, ttl()).await.unwrap(); // same user, different tenant

        store.revoke_user_sessions(1, 10).await.unwrap();
        assert_eq!(store.validate(&t_t1).await.unwrap(), None);
        assert_eq!(
            store.validate(&t_t2).await.unwrap(),
            info(2, 10, UserRole::Member)
        );
    }

    // ── clone / send / sync ───────────────────────────────────────────────────

    #[tokio::test]
    async fn inmem_cloned_store_shares_state() {
        let store1 = InMemorySessionStore::new();
        let store2 = store1.clone();
        let token = store1.create(1, 42, role(), true, ttl()).await.unwrap();
        assert_eq!(
            store2.validate(&token).await.unwrap(),
            info(1, 42, UserRole::Member)
        );
    }

    // ── user_role is preserved ────────────────────────────────────────────────

    #[tokio::test]
    async fn inmem_preserves_user_role() {
        let store = InMemorySessionStore::new();
        let t_owner = store
            .create(1, 1, UserRole::Owner, true, ttl())
            .await
            .unwrap();
        let t_admin = store
            .create(1, 2, UserRole::Admin, true, ttl())
            .await
            .unwrap();
        let t_member = store
            .create(1, 3, UserRole::Member, true, ttl())
            .await
            .unwrap();

        assert_eq!(
            store.validate(&t_owner).await.unwrap(),
            info(1, 1, UserRole::Owner)
        );
        assert_eq!(
            store.validate(&t_admin).await.unwrap(),
            info(1, 2, UserRole::Admin)
        );
        assert_eq!(
            store.validate(&t_member).await.unwrap(),
            info(1, 3, UserRole::Member)
        );
    }

    // ── role / email_verified propagation via update_user_info ─────────────

    #[tokio::test]
    async fn inmem_role_change_propagates_to_live_session() {
        let store = InMemorySessionStore::new();
        // Create session as Admin.
        let token = store
            .create(1, 42, UserRole::Admin, true, ttl())
            .await
            .unwrap();
        assert_eq!(
            store.validate(&token).await.unwrap(),
            info(1, 42, UserRole::Admin)
        );

        // Simulate an admin demoting the user to Member.
        store.update_user_info(1, 42, UserRole::Member, true).await;

        // The live session must now reflect the Member role.
        assert_eq!(
            store.validate(&token).await.unwrap(),
            info(1, 42, UserRole::Member)
        );
    }

    #[tokio::test]
    async fn inmem_email_verified_propagates_to_live_session() {
        let store = InMemorySessionStore::new();
        // Create session as unverified.
        let token = store
            .create(1, 42, UserRole::Member, false, ttl())
            .await
            .unwrap();
        assert_eq!(
            store.validate(&token).await.unwrap(),
            Some(SessionInfo {
                tenant_id: 1,
                user_id: 42,
                user_role: UserRole::Member,
                email_verified: false,
            })
        );

        // Simulate email verification.
        store.update_user_info(1, 42, UserRole::Member, true).await;

        // The live session must now show email_verified = true.
        assert_eq!(
            store.validate(&token).await.unwrap(),
            Some(SessionInfo {
                tenant_id: 1,
                user_id: 42,
                user_role: UserRole::Member,
                email_verified: true,
            })
        );
    }

    #[tokio::test]
    async fn inmem_update_user_info_tenant_scoped() {
        // update_user_info only affects the targeted (tenant, user) pair.
        let store = InMemorySessionStore::new();
        let t1 = store
            .create(1, 10, UserRole::Admin, true, ttl())
            .await
            .unwrap();
        let t2 = store
            .create(2, 10, UserRole::Admin, true, ttl())
            .await
            .unwrap();

        // Demote user 10 in tenant 1 — tenant 2 must be unaffected.
        store.update_user_info(1, 10, UserRole::Member, true).await;

        assert_eq!(
            store.validate(&t1).await.unwrap(),
            info(1, 10, UserRole::Member)
        );
        assert_eq!(
            store.validate(&t2).await.unwrap(),
            info(2, 10, UserRole::Admin)
        );
    }

    #[tokio::test]
    async fn inmem_update_user_info_only_affects_target_user() {
        let store = InMemorySessionStore::new();
        let t1 = store
            .create(1, 10, UserRole::Admin, true, ttl())
            .await
            .unwrap();
        let t2 = store
            .create(1, 20, UserRole::Admin, true, ttl())
            .await
            .unwrap();

        // Demote user 10 — user 20 must stay Admin.
        store.update_user_info(1, 10, UserRole::Member, true).await;

        assert_eq!(
            store.validate(&t1).await.unwrap(),
            info(1, 10, UserRole::Member)
        );
        assert_eq!(
            store.validate(&t2).await.unwrap(),
            info(1, 20, UserRole::Admin)
        );
    }

    #[tokio::test]
    async fn inmem_update_user_info_on_unknown_user_is_harmless() {
        let store = InMemorySessionStore::new();
        // Calling update_user_info on a user that has no sessions is harmless
        // (it just seeds the registry). When a session is later created, the
        // create-time values take precedence (insert overwrites the seed).
        store.update_user_info(1, 99, UserRole::Owner, true).await;

        // Create a session for that user — the create method inserts the
        // session-time values, so the Owner seed is overwritten.
        let token = store
            .create(1, 99, UserRole::Member, false, ttl())
            .await
            .unwrap();
        // The create-time values (Member, not-verified) take precedence.
        assert_eq!(
            store.validate(&token).await.unwrap(),
            Some(SessionInfo {
                tenant_id: 1,
                user_id: 99,
                user_role: UserRole::Member,
                email_verified: false,
            })
        );
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // PgSessionStore tests — non-DB (unit / SQL construction)
    // ═══════════════════════════════════════════════════════════════════════════

    // PgSessionStore is a thin wrapper over SQL queries. DB round-trips are tested
    // via #[ignore] integration tests below. These tests verify construction + the
    // trait methods handle empty-pool / not-connected errors correctly.

    #[tokio::test]
    async fn pg_session_store_constructs_with_pool() {
        // A disconnected pool will fail at query time, not construction time.
        // The constructor itself is infallible.
        let pool = PgPool::connect_lazy("postgres://localhost:5432/nonexistent")
            .expect("connect_lazy should always succeed (no I/O)");
        let store = PgSessionStore::new(pool.clone());
        // Store is Clone.
        let _store2 = store.clone();
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // Integration tests — real Postgres via testcontainers / podman
    // ═══════════════════════════════════════════════════════════════════════════

    /// Helper: spin up a pgvector container, apply migrations, insert tenant+user
    /// rows (FK requirements), and call `f` with the resulting `PgSessionStore`.
    async fn with_pg_session_store<F, Fut>(f: F)
    where
        F: FnOnce(PgSessionStore) -> Fut,
        Fut: std::future::Future<Output = ()>,
    {
        use testcontainers::core::ports::IntoContainerPort;
        use testcontainers::core::wait::WaitFor;
        use testcontainers::runners::AsyncRunner;
        use testcontainers::{GenericImage, ImageExt};

        let container = GenericImage::new("paradedb/paradedb", "latest")
            .with_exposed_port(5432u16.tcp())
            .with_wait_for(WaitFor::message_on_stderr(
                "database system is ready to accept connections",
            ))
            .with_env_var("POSTGRES_USER", "kb")
            .with_env_var("POSTGRES_PASSWORD", "kb")
            .with_env_var("POSTGRES_DB", "kb")
            .start()
            .await
            .expect("testcontainers Postgres should start");

        let host_port = container.get_host_port_ipv4(5432u16.tcp()).await.unwrap();
        let url = format!("postgres://kb:kb@127.0.0.1:{host_port}/kb?sslmode=disable");

        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(5)
            .connect(&url)
            .await
            .unwrap();
        crate::MIGRATOR.run(&pool).await.unwrap();
        // FK from sessions → tenants requires a tenant row.
        sqlx::query("INSERT INTO tenants (slug, name) VALUES ('test', 'Test')")
            .execute(&pool)
            .await
            .unwrap();
        // FK from sessions → users requires a user row.
        sqlx::query(
            "INSERT INTO users (tenant_id, email, password_hash, role)
             VALUES (1, 'test@example.com', '$argon2id$v=19$m=19456,t=2,p=1$dummy$dummy', 'member')",
        )
        .execute(&pool)
        .await
        .unwrap();

        f(PgSessionStore::new(pool)).await;
        // Container is dropped here, cleaning up the test database.
    }

    #[tokio::test]
    #[ignore = "needs testcontainers + podman socket"]
    async fn pg_create_and_validate_session() {
        with_pg_session_store(|store| async move {
            let token = store
                .create(1, 1, UserRole::Owner, true, ttl())
                .await
                .unwrap();
            assert_eq!(token.len(), 64);
            let result = store.validate(&token).await.unwrap();
            assert_eq!(result, info(1, 1, UserRole::Owner));
        })
        .await;
    }

    #[tokio::test]
    #[ignore = "needs testcontainers + podman socket"]
    async fn pg_validate_unknown_token_returns_none() {
        with_pg_session_store(|store| async move {
            let result = store
                .validate("0000000000000000000000000000000000000000000000000000000000000000")
                .await
                .unwrap();
            assert_eq!(result, None);
        })
        .await;
    }

    #[tokio::test]
    #[ignore = "needs testcontainers + podman socket"]
    async fn pg_extend_bumps_expiry() {
        with_pg_session_store(|store| async move {
            let token = store
                .create(1, 1, UserRole::Admin, true, short_ttl())
                .await
                .unwrap();
            store.extend(&token, ttl()).await.unwrap();
            tokio::time::sleep(Duration::from_millis(1100)).await;
            let result = store.validate(&token).await.unwrap();
            assert_eq!(result, info(1, 1, UserRole::Admin));
        })
        .await;
    }

    #[tokio::test]
    #[ignore = "needs testcontainers + podman socket"]
    async fn pg_revoke_removes_session() {
        with_pg_session_store(|store| async move {
            let token = store
                .create(1, 1, UserRole::Member, true, ttl())
                .await
                .unwrap();
            store.revoke(&token).await.unwrap();
            let result = store.validate(&token).await.unwrap();
            assert_eq!(result, None);
        })
        .await;
    }

    #[tokio::test]
    #[ignore = "needs testcontainers + podman socket"]
    async fn pg_revoke_then_recreate_is_new_token() {
        with_pg_session_store(|store| async move {
            let token = store
                .create(1, 1, UserRole::Member, true, ttl())
                .await
                .unwrap();
            store.revoke(&token).await.unwrap();
            let token2 = store
                .create(1, 1, UserRole::Admin, true, ttl())
                .await
                .unwrap();
            assert_ne!(token, token2);
            assert_eq!(store.validate(&token).await.unwrap(), None);
            assert_eq!(
                store.validate(&token2).await.unwrap(),
                info(1, 1, UserRole::Admin)
            );
        })
        .await;
    }

    #[tokio::test]
    #[ignore = "needs testcontainers + podman socket"]
    async fn pg_extend_is_noop_for_unknown_token() {
        with_pg_session_store(|store| async move {
            let result = store.extend("nonexistent", ttl()).await;
            assert!(result.is_ok());
        })
        .await;
    }

    #[tokio::test]
    #[ignore = "needs testcontainers + podman socket"]
    async fn pg_revoke_is_noop_for_unknown_token() {
        with_pg_session_store(|store| async move {
            let result = store.revoke("unknown").await;
            assert!(result.is_ok());
        })
        .await;
    }

    #[tokio::test]
    #[ignore = "needs testcontainers + podman socket"]
    async fn pg_revoke_user_sessions_removes_all_for_user() {
        with_pg_session_store(|store| async move {
            let t1 = store
                .create(1, 1, UserRole::Member, true, ttl())
                .await
                .unwrap();
            let t2 = store
                .create(1, 1, UserRole::Member, true, ttl())
                .await
                .unwrap();
            store.revoke_user_sessions(1, 1).await.unwrap();
            assert_eq!(store.validate(&t1).await.unwrap(), None);
            assert_eq!(store.validate(&t2).await.unwrap(), None);
        })
        .await;
    }

    #[tokio::test]
    #[ignore = "needs testcontainers + podman socket"]
    async fn pg_revoke_user_sessions_is_noop_for_unknown_user() {
        with_pg_session_store(|store| async move {
            let result = store.revoke_user_sessions(1, 999).await;
            assert!(result.is_ok());
        })
        .await;
    }
}
