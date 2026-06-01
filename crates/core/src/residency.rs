//! [`Residency`]: per-tenant data-residency policy (plan §26.6, P9-T9).
//!
//! A tenant's residency governs whether its content may be sent to remote
//! cloud providers. When set to [`Residency::LocalOnly`], the scheduler
//! excludes every backend with [`DataClass::Remote`](crate::data_class::DataClass)
//! at acquire time — "spill to cloud" can never silently override this flag.

use crate::macros::str_enum;

str_enum! {
    /// Per-tenant data-residency policy.
    ///
    /// Residency is enforced by the scheduler pool at acquire time: a
    /// [`LocalOnly`](Residency::LocalOnly) tenant's calls never reach a
    /// remote provider, regardless of routing configuration.
    pub enum Residency {
        /// Data must stay on-premises — remote backends are excluded.
        LocalOnly = "local_only",
        /// Remote cloud providers are eligible (subject to per-document flags).
        AllowRemote = "allow_remote",
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use std::str::FromStr;

    use super::*;

    #[test]
    fn wire_strings_are_locked() {
        assert_eq!(Residency::LocalOnly.as_str(), "local_only");
        assert_eq!(Residency::AllowRemote.as_str(), "allow_remote");
        assert_eq!(Residency::all().len(), 2);
    }

    #[test]
    fn parses_every_variant() {
        for r in Residency::all() {
            assert_eq!(Residency::from_str(r.as_str()).unwrap(), *r);
        }
    }

    #[test]
    fn local_only_parses() {
        assert_eq!(
            Residency::from_str("local_only").unwrap(),
            Residency::LocalOnly
        );
    }

    #[test]
    fn allow_remote_parses() {
        assert_eq!(
            Residency::from_str("allow_remote").unwrap(),
            Residency::AllowRemote
        );
    }

    #[test]
    fn invalid_string_returns_error() {
        assert!(Residency::from_str("bad").is_err());
    }
}
