// SPDX-License-Identifier: AGPL-3.0-or-later

//! Axum request handlers, organized by domain.
//!
//! * [`admin_quota`] — super-admin quota-override endpoint (P14-T14).
//! * [`api_tokens`] — create, list, revoke API tokens (Bearer auth).
//! * [`auth`] — login, register, logout.
//! * [`billing`] — Stripe Checkout integration (P11-T2).
//! * [`ingest`] — multipart file upload → enqueue ingest job.
//! * [`search`] — hybrid search query → ranked results.
//! * [`documents`] — document detail + file download.
//! * [`jobs`] — job status lookup.

pub mod admin_quota;
pub mod api_tokens;
pub mod auth;
pub mod billing;
pub mod chat;
pub mod documents;
pub mod ingest;
pub mod jobs;
pub mod search;
pub mod webhook;

use std::sync::Arc;

use kb_store::PgStore;

// ── Remote-models plan gate (P14-T6) ─────────────────────────────────────────────

/// Map a tenant's `remote_models` plan capability to the pipeline's `local_only` flag.
///
/// Pure inverse: the ingest/search pipelines take `local_only` (`true` = use ONLY
/// local models, never a remote backend), while the plan exposes the positive
/// capability `remote_models_allowed`. So:
/// - `remote_allowed == true` (pro/team, or grandfathered no-plan) → `local_only == false`
///   (remote backends may be used if configured).
/// - `remote_allowed == false` (free plan) → `local_only == true` (forced local-only).
pub(crate) fn local_only_for_plan(remote_allowed: bool) -> bool {
    !remote_allowed
}

/// Resolve the pipeline `local_only` flag for `tenant_id`, **per request**.
///
/// Reads the tenant's current plan capability via
/// [`PgStore::is_remote_models_allowed`] at call time (a small admin-pool query)
/// and inverts it with [`local_only_for_plan`]. Resolving here — rather than
/// capturing a value at startup — keeps the gate hot-swappable: a plan change
/// (e.g. an upgrade/downgrade or a Stripe webhook flipping the plan) takes effect
/// on the very next request with no restart, per the CLAUDE.md standing rule.
///
/// **Fail-safe (fail-closed):** if the plan lookup errors (e.g. a transient DB
/// fault), this returns `true` (force local-only) rather than propagating. This
/// is the correct bias for a billing/capability gate — an error must never *grant*
/// a paid capability the plan may not include, so it can't leak remote-model access
/// to a free-plan tenant. The worst case is a pro/team request briefly using local
/// models (degraded quality, not a billing or isolation breach), and ingest/search
/// stay fully available because local models still run.
pub(crate) async fn resolve_local_only(pg_store: &Arc<PgStore>, tenant_id: i64) -> bool {
    let remote_allowed =
        crate::billing_cache::is_remote_models_allowed_cached(pg_store, tenant_id).await;
    local_only_for_plan(remote_allowed)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::{local_only_for_plan, resolve_local_only};

    #[test]
    fn remote_allowed_means_not_local_only() {
        // pro/team (and grandfathered no-plan) tenants → remote permitted.
        assert!(!local_only_for_plan(true));
    }

    #[test]
    fn remote_denied_forces_local_only() {
        // free plan → all model calls forced local-only.
        assert!(local_only_for_plan(true) != local_only_for_plan(false));
        assert!(local_only_for_plan(false));
    }

    // ── PG integration: resolved `local_only` per plan (P14-T6) ──────────────────
    // Exercises the full resolver (`is_remote_models_allowed` → `local_only_for_plan`)
    // against a real Postgres. `#[ignore]` per CLAUDE.md (testcontainers/Podman socket).

    /// Connected `PgStore` against a fresh testcontainers Postgres database.
    /// `connect()` runs migrations; the shared container is held by `kb_testsupport`
    /// for the whole test binary, so dropping the returned `TestDb` is harmless.
    async fn connected_store() -> std::sync::Arc<kb_store::PgStore> {
        let db = kb_testsupport::fresh_db().await.expect("fresh_db");
        let store = kb_store::PgStore::with_roles(&db.admin_url, &db.app_url);
        store.connect().await.expect("connect");
        std::sync::Arc::new(store)
    }

    /// Assign `plan_code`'s plan to a freshly created tenant; returns the tenant id.
    async fn tenant_on_plan(store: &kb_store::PgStore, slug: &str, plan_code: &str) -> i64 {
        let tenant_id = store
            .create_tenant(slug, slug)
            .await
            .expect("create_tenant");
        let plan = store
            .get_plan_by_code(plan_code)
            .await
            .expect("get_plan_by_code")
            .expect("plan exists");
        store
            .update_tenant_plan(tenant_id, Some(plan.id), None, None, Some("active"), None)
            .await
            .expect("update_tenant_plan");
        tenant_id
    }

    #[tokio::test]
    #[ignore]
    async fn free_plan_resolves_local_only_true() {
        let store = connected_store().await;
        let tenant_id = tenant_on_plan(&store, "lo-free", "free").await;
        let local_only = resolve_local_only(&store, tenant_id).await;
        assert!(local_only, "free plan must force local_only = true");
    }

    #[tokio::test]
    #[ignore]
    async fn pro_plan_resolves_local_only_false() {
        let store = connected_store().await;
        let tenant_id = tenant_on_plan(&store, "lo-pro", "pro").await;
        let local_only = resolve_local_only(&store, tenant_id).await;
        assert!(
            !local_only,
            "pro plan must allow remote (local_only = false)"
        );
    }

    #[tokio::test]
    #[ignore]
    async fn team_plan_resolves_local_only_false() {
        let store = connected_store().await;
        let tenant_id = tenant_on_plan(&store, "lo-team", "team").await;
        let local_only = resolve_local_only(&store, tenant_id).await;
        assert!(
            !local_only,
            "team plan must allow remote (local_only = false)"
        );
    }

    #[tokio::test]
    #[ignore]
    async fn grandfathered_tenant_resolves_local_only_false() {
        let store = connected_store().await;
        // No plan assigned — grandfathered.
        let tenant_id = store
            .create_tenant("lo-gf", "lo-gf")
            .await
            .expect("create_tenant");
        let local_only = resolve_local_only(&store, tenant_id).await;
        assert!(
            !local_only,
            "grandfathered (no-plan) tenant must allow remote (local_only = false)"
        );
    }
}
