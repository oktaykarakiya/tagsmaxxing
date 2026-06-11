// SPDX-License-Identifier: AGPL-3.0-or-later

//! CSRF token generation and validation for HTML forms.
//!
//! Each mutating form (`POST`, `PUT`, `DELETE`) on the Web UI includes a hidden
//! `csrf_token` field. The token is a cryptographically random 32-byte value,
//! stored in the user's session cookie (same cookie jar, different cookie name),
//! and validated server-side on every mutating request.
//!
//! # Cookie property
//!
//! The CSRF token cookie is **not** `HttpOnly` — HTMX reads it from a `<meta>` tag
//! (or `HX-CSRF-Token` header), but the browser sends it on every request
//! automatically. This is the standard double-submit cookie pattern.

use axum::http::HeaderMap;

/// The name of the CSRF cookie sent to the browser.
pub(crate) const CSRF_COOKIE_NAME: &str = "__Host-csrf";

/// Generate a 32-byte cryptographically random CSRF token.
///
/// The token is generated with [`SysRng`](rand::rngs::SysRng) and hex-encoded to 64 chars.
/// It is stored in a `__Host-csrf` cookie alongside the session cookie.
///
/// # Errors
///
/// Propagates errors from the OS random source (extremely rare).
pub(crate) fn generate_csrf_token() -> anyhow::Result<String> {
    use anyhow::Context;
    use rand::TryRng;
    let mut buf = [0u8; 32];
    rand::rngs::SysRng
        .try_fill_bytes(&mut buf)
        .context("failed to generate CSRF token — system random source unavailable")?;
    Ok(hex::encode(buf))
}

/// Build the `Set-Cookie` header value for the CSRF cookie.
///
/// The cookie mirrors the session cookie's lifetime and security properties:
/// `__Host-csrf=<token>; SameSite=Lax; Path=/; HttpOnly=false[; Secure]`.
/// `HttpOnly` is disabled so HTMX (or a `<meta>` tag reader) can access it
/// for the `HX-CSRF-Token` header pattern.
pub(crate) fn csrf_cookie_value(
    token: &str,
    ttl: std::time::Duration,
    secure: bool,
) -> axum::http::HeaderValue {
    let max_age = ttl.as_secs();
    let value = if secure {
        format!("__Host-csrf={token}; SameSite=Lax; Path=/; Max-Age={max_age}; Secure")
    } else {
        format!("__Host-csrf={token}; SameSite=Lax; Path=/; Max-Age={max_age}")
    };
    axum::http::HeaderValue::from_str(&value).unwrap_or(axum::http::HeaderValue::from_static(""))
}

/// Build the `Set-Cookie` header value that clears the CSRF cookie.
pub(crate) fn cleared_csrf_cookie_value(secure: bool) -> axum::http::HeaderValue {
    let value = if secure {
        "__Host-csrf=; SameSite=Lax; Path=/; Max-Age=0; Secure"
    } else {
        "__Host-csrf=; SameSite=Lax; Path=/; Max-Age=0"
    };
    axum::http::HeaderValue::from_str(value).unwrap_or(axum::http::HeaderValue::from_static(""))
}

/// Extract the CSRF token from the request `Cookie` header.
///
/// Returns `Some(token)` if `__Host-csrf=<value>` is present and non-empty,
/// or `None` if the cookie is missing or empty.
fn extract_csrf_from_cookies(headers: &HeaderMap) -> Option<String> {
    let cookie_header = headers.get(axum::http::header::COOKIE)?.to_str().ok()?;

    for pair in cookie_header.split(';') {
        let pair = pair.trim();
        if let Some((name, value)) = pair.split_once('=')
            && name.trim() == CSRF_COOKIE_NAME
        {
            let token = value.trim();
            if !token.is_empty() {
                return Some(token.to_owned());
            }
        }
    }

    None
}

/// Validate a mutating request's CSRF token via the double-submit cookie pattern.
///
/// Compares the `csrf_token` form field (or `X-CSRF-Token` header) against the
/// `__Host-csrf` cookie value. Mismatch returns `403 Forbidden`.
///
/// This function is called by every web UI handler that accepts `POST`, `PUT`,
/// or `DELETE`. It does **not** require a database round-trip — the comparison
/// is a simple constant-time string equality check against the cookie.
pub(crate) fn validate_csrf(
    headers: &HeaderMap,
    form_token: &str,
) -> Result<(), (axum::http::StatusCode, String)> {
    let cookie_token = match extract_csrf_from_cookies(headers) {
        Some(t) => t,
        None => {
            return Err((
                axum::http::StatusCode::FORBIDDEN,
                "CSRF cookie missing — please enable cookies".into(),
            ));
        }
    };

    // Constant-time comparison to prevent timing attacks.
    // The token length is always 64 hex chars (32 bytes → 64 hex).
    if form_token.len() != cookie_token.len() || !constant_time_eq(form_token, &cookie_token) {
        return Err((
            axum::http::StatusCode::FORBIDDEN,
            "CSRF token mismatch — please reload the page and try again".into(),
        ));
    }

    Ok(())
}

/// Constant-time byte comparison for CSRF tokens.
///
/// Uses bitwise XOR over all bytes so that timing does not reveal
/// how many bytes matched before a mismatch.
fn constant_time_eq(a: &str, b: &str) -> bool {
    let a_bytes = a.as_bytes();
    let b_bytes = b.as_bytes();
    if a_bytes.len() != b_bytes.len() {
        return false;
    }
    a_bytes
        .iter()
        .zip(b_bytes.iter())
        .fold(0u8, |acc, (a, b)| acc | (a ^ b))
        == 0
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    #[test]
    fn generate_token_is_64_hex_chars() {
        let token = generate_csrf_token().expect("should generate a token");
        assert_eq!(
            token.len(),
            64,
            "CSRF token should be 32 bytes = 64 hex chars"
        );
        // Verify it's valid hex.
        assert!(token.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn generate_token_is_unique() {
        let t1 = generate_csrf_token().expect("gen 1");
        let t2 = generate_csrf_token().expect("gen 2");
        assert_ne!(t1, t2, "consecutive tokens should differ");
    }

    #[test]
    fn csrf_cookie_value_has_correct_properties() {
        let val = csrf_cookie_value("abc123", std::time::Duration::from_secs(86400), false);
        let s = val.to_str().unwrap();
        assert!(s.starts_with("__Host-csrf=abc123"));
        assert!(s.contains("SameSite=Lax"));
        assert!(s.contains("Path=/"));
        assert!(s.contains("Max-Age=86400"));
        assert!(!s.contains("Secure"));
    }

    #[test]
    fn csrf_cookie_value_secure_when_enabled() {
        let val = csrf_cookie_value("abc", std::time::Duration::from_secs(60), true);
        let s = val.to_str().unwrap();
        assert!(s.contains("Secure"));
    }

    #[test]
    fn cleared_csrf_cookie_has_max_age_zero() {
        let val = cleared_csrf_cookie_value(false);
        let s = val.to_str().unwrap();
        assert!(s.contains("Max-Age=0"));
    }

    #[test]
    fn cleared_csrf_cookie_has_secure_when_enabled() {
        let val = cleared_csrf_cookie_value(true);
        assert!(val.to_str().unwrap().contains("Secure"));
    }

    #[test]
    fn constant_time_eq_matches_identical() {
        assert!(constant_time_eq("abc123", "abc123"));
    }

    #[test]
    fn constant_time_eq_rejects_different() {
        assert!(!constant_time_eq("abc123", "abc124"));
    }

    #[test]
    fn constant_time_eq_rejects_different_lengths() {
        assert!(!constant_time_eq("abc", "abcd"));
    }

    #[test]
    fn constant_time_eq_rejects_empty_vs_nonempty() {
        assert!(!constant_time_eq("", "a"));
    }

    #[test]
    fn constant_time_eq_empty_both_match() {
        assert!(constant_time_eq("", ""));
    }

    #[test]
    fn extract_from_cookies_finds_token() {
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::COOKIE,
            axum::http::HeaderValue::from_str("__Host-csrf=mycsrf123").unwrap(),
        );
        assert_eq!(
            extract_csrf_from_cookies(&headers).as_deref(),
            Some("mycsrf123")
        );
    }

    #[test]
    fn extract_from_cookies_missing_returns_none() {
        let headers = HeaderMap::new();
        assert_eq!(extract_csrf_from_cookies(&headers), None);
    }

    #[test]
    fn validate_csrf_matching_tokens_passes() {
        let mut headers = HeaderMap::new();
        let token = generate_csrf_token().expect("gen");
        headers.insert(
            axum::http::header::COOKIE,
            axum::http::HeaderValue::from_str(&format!("__Host-csrf={token}")).unwrap(),
        );
        assert!(validate_csrf(&headers, &token).is_ok());
    }

    #[test]
    fn validate_csrf_mismatched_token_fails() {
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::COOKIE,
            axum::http::HeaderValue::from_static("__Host-csrf=aaaabb"),
        );
        let err = validate_csrf(&headers, "bbbbcc").unwrap_err();
        assert_eq!(err.0, axum::http::StatusCode::FORBIDDEN);
    }

    #[test]
    fn validate_csrf_missing_cookie_fails() {
        let headers = HeaderMap::new();
        let err = validate_csrf(&headers, "whatever").unwrap_err();
        assert_eq!(err.0, axum::http::StatusCode::FORBIDDEN);
    }
}
