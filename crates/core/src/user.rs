//! The user record and role.

use chrono::{DateTime, Utc};

use crate::macros::str_enum;

str_enum! {
    /// A user's role within their tenant (plan §13).
    pub enum UserRole {
        /// Tenant owner — full control, billing.
        Owner = "owner",
        /// Administrator — manage users, routing, tags.
        Admin = "admin",
        /// Regular member.
        Member = "member",
    }
}

/// A user belonging to exactly one tenant.
///
/// Note: this type intentionally does **not** derive `Serialize`/`Deserialize`. It carries
/// `password_hash`, and a blanket serde impl would risk leaking it into an API response or
/// log line. API layers expose a dedicated view DTO instead (review checkpoint: auth, §31.5).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct User {
    /// Surrogate primary key (`users.id`).
    pub id: i64,
    /// Owning tenant.
    pub tenant_id: i64,
    /// Login email (unique per tenant).
    pub email: String,
    /// Argon2id password **hash** — never the plaintext, and never logged.
    pub password_hash: String,
    /// Role within the tenant.
    pub role: UserRole,
    /// Whether the user's email address has been verified (P12-T2).
    pub email_verified: bool,
    /// Row creation time.
    pub created_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use std::str::FromStr;

    use super::*;

    #[test]
    fn role_wire_strings_are_locked() {
        assert_eq!(UserRole::Owner.as_str(), "owner");
        assert_eq!(UserRole::Admin.as_str(), "admin");
        assert_eq!(UserRole::Member.as_str(), "member");
    }

    #[test]
    fn role_parses_every_variant() {
        for r in UserRole::all() {
            assert_eq!(UserRole::from_str(r.as_str()).unwrap(), *r);
        }
    }
}
