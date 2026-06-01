//! [`DataClass`]: where inference runs — local or remote (plan §26.3, §26.6).
//!
//! This drives data-residency policy: local-only tenants must never send data
//! to backends with [`DataClass::Remote`].

use crate::macros::str_enum;

str_enum! {
    /// Whether a backend runs locally or in a remote cloud provider.
    ///
    /// [`DataClass::Local`] backends process data on-premises; [`DataClass::Remote`]
    /// backends send data to a third-party API. The scheduler enforces data-residency
    /// policy at acquire time (plan §26.6).
    pub enum DataClass {
        /// On-premises inference (llama.cpp, local server).
        Local = "local",
        /// External cloud API (OpenAI, Anthropic, etc.).
        Remote = "remote",
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use std::str::FromStr;

    use super::*;

    #[test]
    fn wire_strings_are_locked() {
        assert_eq!(DataClass::Local.as_str(), "local");
        assert_eq!(DataClass::Remote.as_str(), "remote");
        assert_eq!(DataClass::all().len(), 2);
    }

    #[test]
    fn parses_every_variant() {
        for dc in DataClass::all() {
            assert_eq!(DataClass::from_str(dc.as_str()).unwrap(), *dc);
        }
    }
}
