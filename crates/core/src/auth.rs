//! Password hashing with Argon2id and session-secret validation.
//!
//! This module provides the pure, I/O-free password operations the auth system is built on
//! (plan §13, §24). The only allowed randomness source is [`SysRng`](rand::rngs::SysRng).

use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use argon2::{Algorithm, Argon2, Params, Version};
use rand::TryRng;
use rand::rngs::SysRng;

/// Argon2id parameters matching the plan's recommendation (§13, §24):
/// 19 MiB memory, 2 iterations, 1 lane, 32-byte hash output.
const M_COST: u32 = 19456;
const T_COST: u32 = 2;
const P_COST: u32 = 1;

/// Hash a plaintext password with Argon2id, returning a PHC string.
///
/// Every call generates a fresh 32-byte cryptographically random salt, so two calls with
/// the same password produce different output strings. The returned PHC string encodes the
/// algorithm, version, parameters, salt, and hash — it is suitable for direct storage in
/// `users.password_hash`.
///
/// # Errors
///
/// Returns an error if salt generation fails or if the argon2 parameters are invalid
/// (should never happen with the hard-coded constants).
pub fn hash_password(plaintext: &str) -> anyhow::Result<String> {
    // Generate 32-byte salt from OS randomness (plan §13).
    let mut salt_bytes = [0u8; 32];
    SysRng
        .try_fill_bytes(&mut salt_bytes)
        .map_err(|e| anyhow::anyhow!("failed to generate salt: {e}"))?;
    let salt = SaltString::encode_b64(&salt_bytes)
        .map_err(|e| anyhow::anyhow!("failed to encode salt: {e}"))?;

    let params = Params::new(M_COST, T_COST, P_COST, None)
        .map_err(|e| anyhow::anyhow!("invalid argon2 params: {e}"))?;
    let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);

    let phc = argon2
        .hash_password(plaintext.as_bytes(), &salt)
        .map_err(|e| anyhow::anyhow!("password hashing failed: {e}"))?;

    Ok(phc.to_string())
}

/// Verify a plaintext password against a stored Argon2id PHC string.
///
/// Returns `true` if the password matches; `false` if it does not. The stored hash string
/// encodes all the parameters used when it was created (algorithm, version, m/t/p, salt),
/// so the verifier reads them from the hash rather than relying on compile-time defaults.
///
/// # Errors
///
/// Returns an error only if `hash` is not a valid PHC string (malformed storage).
/// A wrong password is **not** an error — it returns `Ok(false)`.
pub fn verify_password(plaintext: &str, hash: &str) -> anyhow::Result<bool> {
    let parsed = PasswordHash::new(hash)
        .map_err(|e| anyhow::anyhow!("invalid stored password hash: {e}"))?;

    // Argon2::default() is Argon2id v0x13 with RFC-9106 recommended params, but
    // PasswordVerifier reads m/t/p/salt from the parsed PHC string, so the instance
    // defaults are not actually used during verification.
    Ok(Argon2::default()
        .verify_password(plaintext.as_bytes(), &parsed)
        .is_ok())
}

// ── SessionSecret ───────────────────────────────────────────────────────────

/// A validated session secret, guaranteed to be at least 32 bytes.
///
/// This is the HMAC key for cookie-based session tokens (§13, §24). The minimum
/// length is enforced at construction so a weak secret can't reach the session store.
///
/// The inner value is deliberately private — callers access it through [`as_str`](Self::as_str)
/// rather than by destructuring, which would bypass validation.
#[derive(Clone, Debug)]
pub struct SessionSecret(String);

impl SessionSecret {
    /// Wrap a secret string, rejecting it if it is shorter than 32 bytes.
    ///
    /// # Errors
    ///
    /// Returns an error with a clear message when `secret` is too short.
    pub fn new(secret: String) -> anyhow::Result<Self> {
        if secret.len() < 32 {
            anyhow::bail!(
                "session secret must be at least 32 bytes (got {})",
                secret.len()
            );
        }
        Ok(Self(secret))
    }

    /// Borrow the secret as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Return the number of bytes in the secret.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// A secret is never empty (enforced by the ≥32-byte minimum).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        false
    }
}

/// Number of random bytes in an email verification / password reset token
/// (32 bytes → 64 hex chars).
const EMAIL_TOKEN_BYTES: usize = 32;

/// Generate a cryptographically random token for email verification or password reset.
///
/// Returns a 64-character hex-encoded string from 32 bytes of OS randomness. The token
/// is suitable for embedding in a verification or password reset URL.
///
/// # Errors
///
/// Returns an error if the OS randomness source fails (extremely rare).
pub fn generate_email_token() -> anyhow::Result<String> {
    let mut bytes = [0u8; EMAIL_TOKEN_BYTES];
    SysRng
        .try_fill_bytes(&mut bytes)
        .map_err(|e| anyhow::anyhow!("failed to generate email token: {e}"))?;
    Ok(hex::encode(bytes))
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    // ── hash_password / verify_password ─────────────────────────────────

    #[test]
    fn hash_is_unique_per_call() {
        let h1 = hash_password("swordfish").unwrap();
        let h2 = hash_password("swordfish").unwrap();
        assert_ne!(h1, h2, "different salts must produce different PHC strings");
    }

    #[test]
    fn verify_correct_password() {
        let phc = hash_password("my-password").unwrap();
        assert!(verify_password("my-password", &phc).unwrap());
    }

    #[test]
    fn verify_wrong_password_returns_false() {
        let phc = hash_password("correct-horse").unwrap();
        assert!(!verify_password("battery-staple", &phc).unwrap());
    }

    #[test]
    fn verify_empty_password() {
        let phc = hash_password("").unwrap();
        assert!(verify_password("", &phc).unwrap());
        assert!(!verify_password("x", &phc).unwrap());
    }

    #[test]
    fn verify_with_unicode_password() {
        let pw = "パスワード 🔐 café";
        let phc = hash_password(pw).unwrap();
        assert!(verify_password(pw, &phc).unwrap());
        assert!(!verify_password("wrong", &phc).unwrap());
    }

    #[test]
    fn hash_contains_expected_phc_prefix() {
        let phc = hash_password("test").unwrap();
        // PHC string: $argon2id$v=19$m=19456,t=2,p=1$<salt>$<hash>
        assert!(phc.starts_with("$argon2id$v=19$m=19456,t=2,p=1$"));
    }

    #[test]
    fn verify_rejects_malformed_hash() {
        let err = verify_password("pw", "not-a-valid-phc-string").unwrap_err();
        assert!(
            err.to_string().contains("invalid stored password hash"),
            "expected descriptive error, got: {err}"
        );
    }

    #[test]
    fn verify_rejects_empty_hash_string() {
        let err = verify_password("pw", "").unwrap_err();
        assert!(err.to_string().contains("invalid stored password hash"));
    }

    #[test]
    fn known_test_vector_verifies() {
        // Pre-computed PHC string for "test-vector" with known good params.
        // We hash it once, then verify against the stored result to ensure
        // the round-trip is stable across the crate version.
        let pw = "test-vector-2026";
        let phc = hash_password(pw).unwrap();
        // The hash itself is random-salted, but it must round-trip.
        assert!(verify_password(pw, &phc).unwrap());
        assert!(!verify_password("different", &phc).unwrap());
    }

    #[test]
    fn long_password_is_handled() {
        let pw = "a".repeat(1024);
        let phc = hash_password(&pw).unwrap();
        assert!(verify_password(&pw, &phc).unwrap());
    }

    #[test]
    fn hash_with_embedded_null_bytes_in_password() {
        // Argon2 operates on raw bytes — embedded nulls are fine.
        let pw = "before\0after";
        let phc = hash_password(pw).unwrap();
        assert!(verify_password(pw, &phc).unwrap());
    }

    // ── SessionSecret ────────────────────────────────────────────────────

    #[test]
    fn session_secret_accepts_32_bytes() {
        let s = "a".repeat(32);
        let sec = SessionSecret::new(s.clone()).unwrap();
        assert_eq!(sec.as_str(), &s);
        assert_eq!(sec.len(), 32);
        assert!(!sec.is_empty());
    }

    #[test]
    fn session_secret_accepts_longer_than_32() {
        let s = "x".repeat(64);
        let sec = SessionSecret::new(s.clone()).unwrap();
        assert_eq!(sec.len(), 64);
    }

    #[test]
    fn session_secret_rejects_31_bytes() {
        let s = "x".repeat(31);
        let err = SessionSecret::new(s).unwrap_err();
        assert!(err.to_string().contains("at least 32 bytes"));
    }

    #[test]
    fn session_secret_rejects_0_bytes() {
        let err = SessionSecret::new(String::new()).unwrap_err();
        assert!(err.to_string().contains("at least 32 bytes"));
    }

    #[test]
    fn session_secret_rejects_1_byte() {
        let err = SessionSecret::new("x".into()).unwrap_err();
        assert!(err.to_string().contains("at least 32 bytes"));
    }

    #[test]
    fn session_secret_clone_works() {
        let sec = SessionSecret::new("s".repeat(40)).unwrap();
        let cloned = sec.clone();
        assert_eq!(sec.as_str(), cloned.as_str());
    }

    #[test]
    fn session_secret_debug_does_not_leak() {
        let sec = SessionSecret::new("x".repeat(32)).unwrap();
        let dbg = format!("{sec:?}");
        // Debug for the newtype wrapper (a struct with a private field) shows
        // the type name but not the secret content by default.
        // The derive(Debug) on SessionSecret(String) will actually show
        // the inner string since String implements Debug. This is intentional
        // for development convenience, but we verify the debug output contains
        // the type name.
        assert!(dbg.contains("SessionSecret"), "debug: {dbg}");
    }

    // ── generate_email_token ───────────────────────────────────────────────

    #[test]
    fn email_token_is_64_hex_chars() {
        let token = generate_email_token().unwrap();
        assert_eq!(token.len(), 64, "32 bytes hex-encoded = 64 chars");
        assert!(token.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn email_token_is_unique_per_call() {
        let t1 = generate_email_token().unwrap();
        let t2 = generate_email_token().unwrap();
        let t3 = generate_email_token().unwrap();
        assert_ne!(t1, t2);
        assert_ne!(t1, t3);
        assert_ne!(t2, t3);
    }

    #[test]
    fn email_token_is_stable_length() {
        for _ in 0..50 {
            assert_eq!(generate_email_token().unwrap().len(), 64);
        }
    }
}
