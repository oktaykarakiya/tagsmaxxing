//! Askama template types for the admin Web UI (P6-T8).
//!
//! Each struct corresponds to a template file under `templates/`. Fields use
//! simple types (`String`, `i64`, `bool`, `Vec`) so Askama's HTML escaping works
//! without `Option` display issues.

use askama::Template;

// ── Admin dashboard ──────────────────────────────────────────────────────────

/// `GET /admin` — the admin dashboard with summary counts, recent jobs,
/// backend status, and recent audit events.
#[derive(Debug, Template)]
#[template(path = "admin_dashboard.html")]
#[allow(dead_code)]
pub(crate) struct AdminDashboardPage {
    /// CSRF token for mutating forms (e.g. job retry buttons).
    pub csrf_token: String,
    /// Error message to display (empty when none).
    pub error: String,
    /// Tenant name for the current context.
    pub tenant_name: String,
    /// Whether the user is a super-admin (Owner role → cross-tenant).
    pub is_super_admin: bool,
    /// Total tenants (super-admin only, 0 for tenant-admin).
    pub tenant_count: i64,
    /// Total users in the current tenant.
    pub user_count: i64,
    /// Total documents in the current tenant.
    pub document_count: i64,
    /// Storage bytes used (human-readable in the template).
    pub storage_used_bytes: i64,
    /// Token usage count.
    pub token_usage: i64,
    /// Recent jobs (up to 10).
    pub recent_jobs: Vec<AdminJobRow>,
    /// Recent audit events (up to 10).
    pub recent_audit: Vec<AdminAuditRow>,
    /// Backend health rows.
    pub backends: Vec<AdminBackendRow>,
}

/// A single job row in the admin dashboard.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct AdminJobRow {
    pub id: i64,
    pub kind: String,
    pub status: String,
    pub attempts: i32,
    pub last_error: String,
    pub created_at: String,
}

/// A single audit event row in the admin dashboard.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct AdminAuditRow {
    pub id: i64,
    pub actor_user_id: i64,
    pub action: String,
    pub target_type: String,
    pub target_id_display: String,
    pub created_at: String,
}

/// A single backend status row.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct AdminBackendRow {
    pub id: String,
    pub base_url: String,
    pub healthy: bool,
    pub free_slots: usize,
    pub total_slots: usize,
    pub roles: String,
}

// ── Tenant management ────────────────────────────────────────────────────────

/// `GET /admin/tenants` — list all tenants (super-admin only).
#[derive(Debug, Template)]
#[template(path = "admin_tenants.html")]
#[allow(dead_code)]
pub(crate) struct AdminTenantsPage {
    /// CSRF token for create/edit forms.
    pub csrf_token: String,
    /// Error message.
    pub error: String,
    /// All tenants (super-admin only).
    pub tenants: Vec<AdminTenantRow>,
}

/// A single tenant row.
#[derive(Debug, Clone)]
pub(crate) struct AdminTenantRow {
    pub id: i64,
    pub slug: String,
    pub name: String,
    pub quota_bytes_display: String,
    pub quota_tokens_display: String,
    pub created_at: String,
}

// ── User management ──────────────────────────────────────────────────────────

/// `GET /admin/users` — list all users in the current tenant.
#[derive(Debug, Template)]
#[template(path = "admin_users.html")]
pub(crate) struct AdminUsersPage {
    /// CSRF token.
    pub csrf_token: String,
    /// Error message.
    pub error: String,
    /// Tenant name.
    pub tenant_name: String,
    /// Whether the user is Owner (can see Tenants nav item).
    pub is_super_admin: bool,
    /// Users in this tenant.
    pub users: Vec<AdminUserRow>,
}

/// A single user row.
#[derive(Debug, Clone)]
pub(crate) struct AdminUserRow {
    pub id: i64,
    pub email: String,
    pub role: String,
    pub created_at: String,
}

// ── Dead-letter queue ────────────────────────────────────────────────────────

/// `GET /admin/jobs` — dead-letter queue (and all jobs).
#[derive(Debug, Template)]
#[template(path = "admin_jobs.html")]
pub(crate) struct AdminJobsPage {
    /// CSRF token.
    pub csrf_token: String,
    /// Error message.
    pub error: String,
    /// Whether the user is Owner (can see Tenants nav item).
    pub is_super_admin: bool,
    /// Filter: status to show (e.g. "dead" or empty for all).
    pub filter_status: String,
    /// Jobs matching the filter.
    pub jobs: Vec<AdminJobRow>,
}

// ── Tag merge ────────────────────────────────────────────────────────────────

/// `GET /admin/tags` — select two tags to merge.
#[derive(Debug, Template)]
#[template(path = "admin_tags.html")]
pub(crate) struct AdminTagsPage {
    /// CSRF token.
    pub csrf_token: String,
    /// Error message.
    pub error: String,
    /// Success message (e.g. after a merge).
    pub success: String,
    /// Whether the user is Owner (can see Tenants nav item).
    pub is_super_admin: bool,
    /// Tenant tags to select from.
    pub tags: Vec<AdminTagOption>,
}

/// A tag option for merge `<select>` dropdowns.
#[derive(Debug, Clone)]
pub(crate) struct AdminTagOption {
    pub tag_id: i64,
    pub tag_name: String,
}
