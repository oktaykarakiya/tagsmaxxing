// SPDX-License-Identifier: AGPL-3.0-or-later

//! The audit log — records every mutating admin action for accountability
//! (plan §15 admin panel) and every key-unwrap operation for confidentiality
//! auditing (plan §28, P10-T5).
//!
//! # Admin audit (`audit_events` table)
//!
//! Every admin handler that mutates state (tenant CRUD, user management, tag merge,
//! dead-letter replay/delete) calls [`PgStore::insert_audit_event`] to record **who**
//! did **what** to **which target** and **when**. The audit log is append-only and
//! read-only to the admin UI — rows are never deleted or updated.
//!
//! # Decrypt-access audit (`decrypt_audit` table)
//!
//! Every key-unwrap operation (DEK unwrap, provider API key decrypt) writes a
//! [`DecryptAuditEvent`] recording `{timestamp, tenant_id, operation, key_id, user_id,
//! success}`. Failed unwraps are also logged — a spike in failures triggers a
//! Prometheus alert for possible attack or key corruption. All rows are append-only
//! (no UPDATE / DELETE).

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
        /// A provider was created via the admin panel.
        ProviderCreated = "provider_created",
        /// A provider's configuration was updated.
        ProviderUpdated = "provider_updated",
        /// A provider was enabled or disabled.
        ProviderToggled = "provider_toggled",
        /// A model was created via the admin panel.
        ModelCreated = "model_created",
        /// A model's configuration was updated.
        ModelUpdated = "model_updated",
        /// A model was enabled or disabled.
        ModelToggled = "model_toggled",
        /// A route was created via the admin panel.
        RouteCreated = "route_created",
        /// A route's configuration was updated.
        RouteUpdated = "route_updated",
        /// A route was deleted.
        RouteDeleted = "route_deleted",
        /// A provider connection test was executed.
        ProviderTested = "provider_tested",
        /// A tenant's billing plan was assigned or changed (via webhook or admin).
        PlanChanged = "plan_changed",
        /// An admin manually overrode a tenant's billing status or plan.
        BillingOverride = "billing_override",
        /// A tenant's billing status was changed (lifecycle transition).
        BillingStatusChanged = "billing_status_changed",
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

// ── Decrypt-access audit (plan §28, P10-T5) ─────────────────────────────────────

str_enum! {
    /// The kind of key-unwrap operation being audited.
    ///
    /// Every call to [`KeyEncryptionKey::unwrap`](crate::kek::KeyEncryptionKey::unwrap)
    /// for a tenant-scoped key must produce a [`DecryptAuditEvent`] with the matching
    /// operation. The audit trail answers "who accessed which keys when" for compliance
    /// and detects key-compromise or brute-force attacks on wrapped DEKs.
    pub enum DecryptAuditAction {
        /// A tenant Data Encryption Key (DEK) was unwrapped via the KEK
        /// (e.g. to decrypt an envelope-encrypted DB column or blob).
        UnwrapDek = "unwrap_dek",
        /// A provider API key was decrypted from its KEK-wrapped stored form
        /// (e.g. to sign an outbound LLM request).
        UnwrapProviderKey = "unwrap_provider_key",
    }
}

/// A single immutable decrypt-access audit entry — records **who** accessed
/// **which key** for **which tenant** **when**, and whether it succeeded.
///
/// This table (`decrypt_audit`) is append-only: rows are INSERTed but never
/// UPDATEd or DELETEd. Failed unwraps are also recorded (with `success = false`)
/// so a spike in failures is detectable via the `kb_decrypt_audit_failed`
/// Prometheus gauge for alerting on possible attacks or key corruption.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DecryptAuditEvent {
    /// Surrogate primary key.
    pub id: i64,
    /// The tenant whose key was accessed.
    pub tenant_id: i64,
    /// What kind of key access happened.
    pub operation: DecryptAuditAction,
    /// An opaque identifier for the key that was unwrapped.
    /// Examples: `"dek:{uuid}"`, `"provider:{id}"`, `"KEK:vault:v1:transit/key"`.
    pub key_id: String,
    /// The user who initiated the operation, if known (may be `None` for
    /// background jobs and server-initiated operations).
    pub user_id: Option<i64>,
    /// `true` if the unwrap succeeded, `false` if it failed (e.g. wrong KEK,
    /// tampered ciphertext, or corruption).
    pub success: bool,
    /// When the operation occurred (set by the database `now()` on insert).
    pub created_at: chrono::DateTime<chrono::Utc>,
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use std::str::FromStr;

    use super::*;
    use serde_json::json;

    // ── Admin AuditAction tests ────────────────────────────────────────────

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
        assert_eq!(AuditAction::PlanChanged.as_str(), "plan_changed");
        assert_eq!(AuditAction::BillingOverride.as_str(), "billing_override");
        assert_eq!(
            AuditAction::BillingStatusChanged.as_str(),
            "billing_status_changed"
        );
        assert_eq!(AuditAction::all().len(), 23);
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

    // ── DecryptAuditAction tests ───────────────────────────────────────────

    #[test]
    fn decrypt_action_wire_strings_are_locked() {
        assert_eq!(DecryptAuditAction::UnwrapDek.as_str(), "unwrap_dek");
        assert_eq!(
            DecryptAuditAction::UnwrapProviderKey.as_str(),
            "unwrap_provider_key"
        );
        assert_eq!(DecryptAuditAction::all().len(), 2);
    }

    #[test]
    fn decrypt_action_parses_every_variant() {
        for a in DecryptAuditAction::all() {
            assert_eq!(DecryptAuditAction::from_str(a.as_str()).unwrap(), *a);
        }
    }

    #[test]
    fn decrypt_action_from_str_rejects_unknown() {
        assert!(DecryptAuditAction::from_str("decrypt_something").is_err());
    }

    // ── DecryptAuditEvent tests ────────────────────────────────────────────

    #[test]
    fn decrypt_event_is_serializable() {
        let e = DecryptAuditEvent {
            id: 1,
            tenant_id: 7,
            operation: DecryptAuditAction::UnwrapDek,
            key_id: "dek:550e8400-e29b-41d4-a716-446655440000".into(),
            user_id: Some(42),
            success: true,
            created_at: chrono::Utc::now(),
        };
        let json = serde_json::to_string(&e).unwrap();
        let back: DecryptAuditEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(back.operation, DecryptAuditAction::UnwrapDek);
        assert_eq!(back.tenant_id, 7);
        assert_eq!(back.key_id, "dek:550e8400-e29b-41d4-a716-446655440000");
        assert_eq!(back.user_id, Some(42));
        assert!(back.success);
    }

    #[test]
    fn decrypt_event_failed_unwrap() {
        let e = DecryptAuditEvent {
            id: 2,
            tenant_id: 3,
            operation: DecryptAuditAction::UnwrapProviderKey,
            key_id: "provider:99".into(),
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
    fn decrypt_event_no_user_id() {
        let e = DecryptAuditEvent {
            id: 3,
            tenant_id: 1,
            operation: DecryptAuditAction::UnwrapDek,
            key_id: "dek:some-key".into(),
            user_id: None,
            success: true,
            created_at: chrono::Utc::now(),
        };
        let json = serde_json::to_string(&e).unwrap();
        let back: DecryptAuditEvent = serde_json::from_str(&json).unwrap();
        assert!(back.user_id.is_none());
    }

    #[test]
    fn decrypt_actions_are_distinct_from_admin_actions() {
        // The two action enums are different types — they cannot be compared.
        // Just verify the string representations don't collide.
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
}
