//! The admin audit log — records every mutating admin action for accountability
//! (plan §15 admin panel).
//!
//! Every admin handler that mutates state (tenant CRUD, user management, tag merge,
//! dead-letter replay/delete) calls [`PgStore::insert_audit_event`] to record **who**
//! did **what** to **which target** and **when**. The audit log is append-only and
//! read-only to the admin UI — rows are never deleted or updated.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::macros::str_enum;

str_enum! {
    /// The category of admin action being logged.
    pub enum AuditAction {
        /// A new tenant was created.
        TenantCreated = "tenant_created",
        /// A tenant's quotas were updated.
        TenantUpdated = "tenant_updated",
        /// A tenant was suspended or re-activated.
        TenantSuspended = "tenant_suspended",
        /// A new user was invited/created.
        UserCreated = "user_created",
        /// A user's role was changed.
        UserRoleChanged = "user_role_changed",
        /// A user was disabled or re-enabled.
        UserDisabled = "user_disabled",
        /// Two tags were merged (all aliases + doc links repointed).
        TagMerged = "tag_merged",
        /// A dead-letter job was retried.
        JobRetried = "job_retried",
        /// A dead-letter job was deleted.
        JobDeleted = "job_deleted",
        /// A document was re-tagged via admin action.
        DocumentRetagged = "document_retagged",
    }
}

/// A single immutable audit log entry — the **who did what when** record.
///
/// Each row is created by an admin handler and is read-only thereafter.
/// `details` carries action-specific JSON for debugging (e.g. the old and new
/// role for a role change, or the merged tag ids). The audit log is **not**
/// tenant-scoped — it lives in the privileged schema and is queried via the
/// admin pool so super-admins can see cross-tenant actions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditEvent {
    /// Surrogate primary key.
    pub id: i64,
    /// The user who performed the action.
    pub actor_user_id: i64,
    /// Which tenant was affected (may be 0 for cross-tenant super-admin actions).
    pub tenant_id: i64,
    /// What happened.
    pub action: AuditAction,
    /// The kind of entity affected (e.g. "tenant", "user", "tag", "job").
    pub target_type: String,
    /// The id of the entity affected, if applicable.
    pub target_id: Option<i64>,
    /// JSON payload with action-specific context (old/new values, affected rows).
    pub details: serde_json::Value,
    /// When the action occurred (set by the database `now()` on insert).
    pub created_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use std::str::FromStr;

    use super::*;
    use serde_json::json;

    #[test]
    fn action_wire_strings_are_locked() {
        assert_eq!(AuditAction::TenantCreated.as_str(), "tenant_created");
        assert_eq!(AuditAction::TenantUpdated.as_str(), "tenant_updated");
        assert_eq!(AuditAction::TenantSuspended.as_str(), "tenant_suspended");
        assert_eq!(AuditAction::UserCreated.as_str(), "user_created");
        assert_eq!(AuditAction::UserRoleChanged.as_str(), "user_role_changed");
        assert_eq!(AuditAction::UserDisabled.as_str(), "user_disabled");
        assert_eq!(AuditAction::TagMerged.as_str(), "tag_merged");
        assert_eq!(AuditAction::JobRetried.as_str(), "job_retried");
        assert_eq!(AuditAction::JobDeleted.as_str(), "job_deleted");
        assert_eq!(AuditAction::DocumentRetagged.as_str(), "document_retagged");
        assert_eq!(AuditAction::all().len(), 10);
    }

    #[test]
    fn action_parses_every_variant() {
        for a in AuditAction::all() {
            assert_eq!(AuditAction::from_str(a.as_str()).unwrap(), *a);
        }
    }

    #[test]
    fn event_is_serializable() {
        let e = AuditEvent {
            id: 1,
            actor_user_id: 42,
            tenant_id: 7,
            action: AuditAction::TagMerged,
            target_type: "tag".into(),
            target_id: Some(99),
            details: json!({"from_tag_id": 99, "to_tag_id": 100, "aliases_moved": 3, "doc_tags_repointed": 12}),
            created_at: chrono::Utc::now(),
        };
        let json = serde_json::to_string(&e).unwrap();
        let back: AuditEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(back.action, AuditAction::TagMerged);
        assert_eq!(back.target_id, Some(99));
    }
}
