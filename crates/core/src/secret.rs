// SPDX-License-Identifier: AGPL-3.0-or-later

//! Encrypted-key reference types (plan §26.3, §24).
//!
//! A [`SecretRef`] is a lightweight handle to a provider API key stored
//! encrypted in the database. The actual key is fetched and decrypted at
//! call time by the key-management layer (P10-T3), never cached in memory.

/// A reference to an encrypted provider API key stored in the `providers` table
/// (plan §26.3, §24).
///
/// This is a newtype wrapper to prevent accidental logging or serialization of
/// the actual key id. The plaintext key is fetched + decrypted at call time by
/// the KMS/KEK layer (P10-T3) and zeroized after use.
///
/// # Display safety
///
/// The [`std::fmt::Display`] impl shows a redacted form (`SecretRef(id-0123)`)
/// so debug and log output never leaks the raw id.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct SecretRef(pub String);

impl SecretRef {
    /// Create a new secret reference.
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// The opaque key identifier (e.g., a DB primary key or UUID).
    pub fn id(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for SecretRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("SecretRef").field(&"****").finish()
    }
}

impl std::fmt::Display for SecretRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Delegate to Debug for a uniformly redacted form.
        std::fmt::Debug::fmt(self, f)
    }
}

impl From<String> for SecretRef {
    fn from(s: String) -> Self {
        Self::new(s)
    }
}

impl From<&str> for SecretRef {
    fn from(s: &str) -> Self {
        Self::new(s)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    #[test]
    fn display_redacts_long_id() {
        let sr = SecretRef::new("provider-key-abc-123");
        let d = sr.to_string();
        assert!(
            d.contains("****") || d.contains("…"),
            "must be redacted: {d}"
        );
        assert!(!d.contains("abc-123"), "full id must not appear: {d}");
    }

    #[test]
    fn display_redacts_short_id_too() {
        let sr = SecretRef::new("kv");
        let d = sr.to_string();
        assert!(!d.contains("kv"), "short id must be redacted: {d}");
        assert!(d.contains("****"), "{d}");
    }

    #[test]
    fn id_accessor_returns_raw() {
        let sr = SecretRef::new("my-key");
        assert_eq!(sr.id(), "my-key");
    }

    #[test]
    fn from_str_works() {
        let sr: SecretRef = "sk-abc".into();
        assert_eq!(sr.id(), "sk-abc");
    }

    #[test]
    fn debug_redacted_too() {
        let sr = SecretRef::new("top-secret-key-555");
        let debugged = format!("{sr:?}");
        // Debug should NOT contain the raw id.
        assert!(
            !debugged.contains("top-secret"),
            "debug leaked id: {debugged}"
        );
    }
}
