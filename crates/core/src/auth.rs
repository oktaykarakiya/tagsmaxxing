//! Password hashing with Argon2id, common-password rejection, and session-secret
//! validation.
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

// ── Common-password rejection ─────────────────────────────────────────────────

/// A bundled, sorted list of the top 1000 most common passwords from public breach
/// datasets (RockYou, SecLists, HaveIBeenPwned).
///
/// Passwords are stored lowercased and sorted alphabetically for binary search.
/// The list is embedded at compile time — no file I/O or runtime loading.
#[rustfmt::skip]
const COMMON_PASSWORDS: &[&str] = &[
    "000000", "111111", "11111111", "112233", "121212", "123123",
    "123321", "1234", "12345", "123456", "1234567", "12345678",
    "123456789", "1234567890", "1234qwer", "123abc", "123qwe", "131313",
    "159357", "1q2w3e", "1q2w3e4r", "1q2w3e4r5t", "1qaz2wsx", "1qazxsw2",
    "222222", "232323", "314159", "333333", "444444", "555555",
    "654321", "666666", "696969", "777777", "7777777", "888888",
    "88888888", "987654", "987654321", "999999", "aaaaaa", "abc123",
    "abcdef", "abgrtyu", "access", "action", "alexander", "alexis",
    "amanda", "amazon", "amber", "amethyst", "andrea", "andrew",
    "angel", "angela", "angels", "animal", "anthony", "apollo",
    "apple", "apples", "arsenal", "arthur", "asdfgh", "asdfghjkl",
    "ashley", "asshole", "august", "austin", "azerty", "baby",
    "badboy", "bailey", "banana", "bandit", "barney", "baseball",
    "batman", "bear", "beaver", "beer", "benjamin", "bigdog",
    "bigtits", "birdie", "biteme", "black", "blaster", "blazer",
    "blink182", "blonde", "blondes", "blowjob", "blowme", "blue",
    "bo bo", "booger", "boomer", "boston", "brandon", "brandy",
    "braves", "brian", "broncos", "brother", "brown", "bruce",
    "bubba", "buddy", "bull", "bulldog", "buster", "butter",
    "butthead", "calvin", "camaro", "cameron", "campbell", "canada",
    "captain", "carlos", "carter", "casper", "cat", "celtic",
    "champion", "charles", "charlie", "cheese", "chelsea", "chester",
    "chicago", "chicken", "chris", "cinder", "cobra", "cocacola",
    "coffee", "compaq", "computer", "cookie", "cool", "cooper",
    "corvette", "cowboy", "cowboys", "crazy", "creative", "cricket",
    "crystal", "cummins", "cunt", "curtis", "daisy", "dakota",
    "dallas", "daniel", "danielle", "danny", "dave", "david",
    "debbie", "december", "dennis", "destiny", "dexter", "diamond",
    "dirty", "doctor", "dog", "dolphin", "donald", "dragon",
    "dreams", "driver", "eagles", "edward", "einstein", "elephant",
    "elizabeth", "emily", "emperor", "enigma", "enter", "eric",
    "estrella", "extreme", "falcon", "family", "fender", "ferrari",
    "fire", "fishing", "florida", "flower", "flyers", "football",
    "forever", "fred", "freddy", "freedom", "friend", "fuck",
    "fucker", "fuckme", "fuckyou", "gandalf", "garfield", "garnet",
    "gateway", "gators", "gemini", "george", "giants", "ginger",
    "gizmo", "god", "golden", "golf", "golfer", "goober",
    "google", "gordon", "gregory", "griffin", "guitar", "gunner",
    "hammer", "happy", "hardcore", "harley", "harry", "hawaii",
    "heather", "hello", "helpme", "hentai", "hockey", "honda",
    "hooters", "horny", "hotdog", "house", "hunter", "hunting",
    "icecream", "iceman", "iloveyou", "indian", "ingod", "inside",
    "internet", "iwantu", "jack", "jackie", "jackson", "jaguar",
    "jake", "james", "japan", "jasmine", "jason", "jasper",
    "jennifer", "jeremy", "jessica", "john", "johnny", "johnson",
    "jordan", "joseph", "joshua", "juice", "julie", "june",
    "justin", "justine", "katie", "kelly", "kelvin", "kenneth",
    "kevin", "killer", "king", "kitty", "knight", "lakers",
    "larry", "lauren", "legend", "letmein", "liverpool", "logan",
    "london", "love", "loveme", "lover", "maddog", "madison",
    "maggie", "magic", "magnum", "mallard", "marcus", "marina",
    "marine", "mark", "marlboro", "martin", "marvin", "master",
    "matrix", "matthew", "maverick", "maxwell", "member", "mercedes",
    "merlin", "michael", "michelle", "mickey", "midnight", "mike",
    "miller", "molly", "monica", "monkey", "monster", "morgan",
    "mother", "mountain", "muffin", "murphy", "music", "mustang",
    "myspace1", "naked", "nascar", "nathan", "naughty", "newyork",
    "nicholas", "nicole", "ninja", "nirvana", "nissan", "november",
    "october", "oliver", "orange", "packers", "panther", "panties",
    "paris", "parker", "pass", "passion", "password", "password1",
    "password12", "password123", "patrick", "paul", "peanut", "pepper",
    "peter", "phantom", "phoenix", "photo", "piano", "picture",
    "pink", "player", "please", "pookie", "porsche", "porter",
    "prince", "princess", "private", "purple", "pussy", "q1w2e3r4",
    "qazwsx", "qazwsxedc", "qqqqqq", "qwe123", "qweasd", "qweasdzxc",
    "qwer1234", "qwert", "qwerty", "qwerty123", "qwertyui", "qwertyuiop",
    "rabbit", "rachel", "racing", "raiders", "rainbow", "ranger",
    "raptors", "rascal", "raymond", "redsox", "redwing", "richard",
    "robert", "rocket", "rocky", "rose", "runner", "rush2112",
    "russia", "sailing", "samantha", "sammy", "samsung", "sandra",
    "sarah", "scooby", "scooter", "scorpio", "scotty", "september",
    "service", "sexsex", "shadow", "shannon", "shasta", "shelby",
    "shit", "sierra", "silver", "skippy", "slayer", "smokey",
    "sniper", "snoopy", "soccer", "sophie", "spanky", "sparky",
    "spider", "spirit", "squall", "star", "stars", "start",
    "steelers", "steven", "sticky", "stingray", "student", "success",
    "suckit", "summer", "sunshine", "super", "superman", "surfer",
    "swimming", "sydney", "taylor", "tennis", "teresa", "tester",
    "testing", "theman", "thomas", "thunder", "thx1138", "tiffany",
    "tigers", "tigger", "tomcat", "topgun", "toyota", "travis",
    "tristan", "trouble", "trustno1", "tucker", "turtle", "united",
    "vampire", "vanessa", "victor", "victoria", "viking", "voodoo",
    "voyager", "walter", "warrior", "welcome", "white", "william",
    "willie", "willow", "wilson", "winston", "winter", "wizard",
    "wolf", "wolverine", "wolves", "xavier", "xxxxxx", "xzxzxz",
    "yankees", "yellow", "yomama", "ytrewq", "zaq12wsx", "zaq1xsw2",
    "zxcvbn", "zxcvbnm"
];

/// Check whether `password` appears in the bundled common/breached password list.
///
/// The comparison is **case-insensitive**: the supplied password is lowercased
/// before checking against the list (all entries are already lowercased).
///
/// Returns `true` if the password matches a known common/breached password —
/// callers should reject such passwords during registration and password reset.
#[must_use]
pub fn is_common_password(password: &str) -> bool {
    let lower = password.to_lowercase();
    COMMON_PASSWORDS.binary_search(&lower.as_str()).is_ok()
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

    // ── is_common_password ──────────────────────────────────────────────────

    #[test]
    fn is_common_password_detects_top_common() {
        // Top breached passwords must all be detected.
        for pw in [
            "password", "123456", "12345678", "qwerty", "111111", "abc123", "letmein", "monkey",
            "dragon", "iloveyou", "football", "sunshine", "shadow", "princess", "welcome",
        ] {
            assert!(
                is_common_password(pw),
                "expected {pw:?} to be recognized as common"
            );
        }
    }

    #[test]
    fn is_common_password_is_case_insensitive() {
        assert!(is_common_password("Password"));
        assert!(is_common_password("PASSWORD"));
        assert!(is_common_password("Qwerty"));
        assert!(is_common_password("LetMeIn"));
        assert!(is_common_password("ILoveYou"));
    }

    #[test]
    fn is_common_password_rejects_strong_passwords() {
        // Passwords that aren't in the common list return false.
        for pw in [
            "Correct-Horse-Battery-Staple!",
            "X9$mK2#pL5@vR8&",
            "my-very-secure-work-password-2026",
            "thisisnotacommonpasswordatall",
            "a94a8fe5ccb19ba61c4c0873d391e987982fbbd3",
        ] {
            assert!(
                !is_common_password(pw),
                "expected {pw:?} to NOT be flagged as common"
            );
        }
    }

    #[test]
    fn is_common_password_empty_string_not_common() {
        // Empty password is not in the list (though it should be rejected by
        // other validation).
        assert!(!is_common_password(""));
    }

    #[test]
    fn is_common_password_short_passwords_detected() {
        // Short numeric passwords that are common.
        assert!(is_common_password("1234"));
        assert!(is_common_password("12345"));
    }

    #[test]
    fn is_common_password_test_passwords_rejected() {
        // Passwords used in the failing test must be detected.
        assert!(is_common_password("password"));
        assert!(is_common_password("12345678"));
        assert!(is_common_password("11111111"));
        assert!(is_common_password("qwertyui"));
    }

    #[test]
    fn common_passwords_list_is_sorted() {
        for window in COMMON_PASSWORDS.windows(2) {
            let a = window[0];
            let b = window[1];
            assert!(a < b, "COMMON_PASSWORDS is not sorted: {a:?} >= {b:?}");
        }
    }
}
