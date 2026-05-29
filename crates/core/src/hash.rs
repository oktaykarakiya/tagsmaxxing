//! Content-address primitive: a SHA-256 digest with hex (de)serialization.

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::error::CoreError;

/// A SHA-256 digest (32 raw bytes).
///
/// Used for per-tenant blob dedup (`files.sha256`) and to derive the content-addressed
/// `blob_key = hex(sha256)`. Serializes as a lowercase 64-character hex string so it is
/// portable across the JSON API and export (`export.jsonl`).
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Sha256([u8; 32]);

impl Sha256 {
    /// Wrap 32 raw digest bytes.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Borrow the 32 raw digest bytes.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Lowercase hex encoding — this is the content-addressed `blob_key`.
    #[must_use]
    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    /// Parse from a 64-character hex string (case-insensitive).
    ///
    /// # Errors
    /// Returns [`CoreError::InvalidHash`] if `s` is not exactly 32 bytes of valid hex.
    pub fn from_hex(s: &str) -> Result<Self, CoreError> {
        let mut out = [0u8; 32];
        hex::decode_to_slice(s, &mut out).map_err(|_| CoreError::InvalidHash(s.to_owned()))?;
        Ok(Self(out))
    }
}

impl fmt::Display for Sha256 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl fmt::Debug for Sha256 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Sha256({})", self.to_hex())
    }
}

impl Serialize for Sha256 {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_hex())
    }
}

impl<'de> Deserialize<'de> for Sha256 {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Self::from_hex(&s).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    fn sample() -> Sha256 {
        // 0x00, 0x01, ... 0x1f
        let mut b = [0u8; 32];
        for (i, slot) in b.iter_mut().enumerate() {
            *slot = u8::try_from(i).unwrap();
        }
        Sha256::from_bytes(b)
    }

    #[test]
    fn hex_round_trips() {
        let h = sample();
        assert_eq!(h.as_bytes()[1], 1);
        let s = h.to_hex();
        assert_eq!(s.len(), 64);
        let parsed = Sha256::from_hex(&s).unwrap();
        assert_eq!(parsed, h);
        assert_eq!(parsed.as_bytes(), h.as_bytes());
    }

    #[test]
    fn from_hex_is_case_insensitive() {
        let lower = "00".repeat(32);
        let upper = "00".repeat(32).to_uppercase();
        assert_eq!(
            Sha256::from_hex(&lower).unwrap(),
            Sha256::from_hex(&upper).unwrap()
        );
    }

    #[test]
    fn from_hex_rejects_wrong_length() {
        let err = Sha256::from_hex("00").unwrap_err();
        assert!(matches!(err, CoreError::InvalidHash(_)));
    }

    #[test]
    fn from_hex_rejects_non_hex() {
        let err = Sha256::from_hex(&"zz".repeat(32)).unwrap_err();
        assert!(matches!(err, CoreError::InvalidHash(_)));
    }

    #[test]
    fn serde_is_hex_string_and_round_trips() {
        let h = sample();
        let json = serde_json::to_string(&h).unwrap();
        assert_eq!(json, format!("\"{}\"", h.to_hex()));
        let back: Sha256 = serde_json::from_str(&json).unwrap();
        assert_eq!(back, h);
    }

    #[test]
    fn deserialize_rejects_invalid_hex() {
        let parsed: std::result::Result<Sha256, _> = serde_json::from_str("\"not-valid-hex\"");
        assert!(parsed.is_err());
    }

    #[test]
    fn display_and_debug() {
        let h = Sha256::from_bytes([0u8; 32]);
        assert_eq!(h.to_string(), "0".repeat(64));
        assert_eq!(format!("{h:?}"), format!("Sha256({})", "0".repeat(64)));
    }
}
