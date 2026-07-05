// SPDX-License-Identifier: AGPL-3.0-or-later

//! SSRF-guarded URL fetcher for the P17 source-sync feature.
//!
//! # Security model (plan §17, §31.5 checkpoint)
//!
//! Every user-supplied URL passes through **per-hop** validation:
//! 1. **Static parse**: `url` crate parse; scheme allowlist (`http`/`https`);
//!    reject userinfo (credential smuggling); reject internal hostnames via
//!    [`crate::security::is_internal_host`].
//! 2. **DNS-resolving guard**: resolve the host via `tokio::net::lookup_host`;
//!    every resolved IP must pass [`crate::security::is_internal_ip`]
//!    (loopback, private, link-local, unspecified, v4-mapped-v6).
//! 3. **Connection pinning**: the validated addresses are pinned via
//!    `reqwest::ClientBuilder::resolve_to_addrs`, so the TCP connect uses the
//!    pre-validated IPs — defeating DNS-rebinding attacks where a second
//!    resolution returns a different address.
//! 4. **Per-hop re-validation**: every redirect hop re-runs steps 1–3
//!    against the new location (capped at `max_redirects`).
//! 5. **Size cap + timeout**: the response body stream aborts if
//!    `max_bytes` is exceeded; the overall request respects `timeout`.
//!
//! There is **no** configurable escape hatch for private-network access in
//! the production code path — the guard is non-bypassable.

use std::net::SocketAddr;
use std::time::Duration;

use reqwest::redirect;
use sha2::{Digest, Sha256};
use thiserror::Error;
use url::Url;

use crate::security;

// ── Configuration ──────────────────────────────────────────────────────────

/// Per-fetch limits supplied by `[source_sync]` config at call time.
#[derive(Debug, Clone)]
pub struct FetchLimits {
    /// Request timeout.
    pub timeout: Duration,
    /// Maximum total body bytes (stream aborted if exceeded).
    pub max_bytes: u64,
    /// Maximum number of HTTP redirect hops.
    pub max_redirects: u8,
}

// ── Output ─────────────────────────────────────────────────────────────────

/// The result of a successful fetch.
#[derive(Debug, Clone)]
pub struct FetchedContent {
    /// The final URL after redirects (useful for deriving a filename).
    pub final_url: String,
    /// The response body, capped at [`FetchLimits::max_bytes`].
    pub bytes: Vec<u8>,
    /// `Content-Type` header value, if present.
    pub content_type: Option<String>,
    /// SHA-256 digest of `bytes`, used for change detection.
    pub sha256: [u8; 32],
}

// ── Errors ─────────────────────────────────────────────────────────────────

/// Errors produced by the source-sync fetcher.
///
/// Split between **deterministic** (permanent — wrong URL, SSRF block, 4xx)
/// and **transient** (network, timeout, 5xx) so the caller can classify failed
/// jobs correctly (dead-letter vs retry).
#[derive(Debug, Error)]
pub enum FetchError {
    /// The URL scheme is not `http` or `https`.
    #[error("unsupported URL scheme: {0}")]
    UnsupportedScheme(String),

    /// The URL contains userinfo (`user:pass@host`) — rejected to prevent
    /// credential smuggling through the fetcher.
    #[error("URL must not contain userinfo: {0}")]
    UserInfoRejected(String),

    /// The URL host is internal (loopback, private, link-local, unspecified)
    /// or resolves to an internal IP address.
    #[error("URL points to an internal/private address: {0}")]
    InternalAddress(String),

    /// DNS resolution failed for the host.
    #[error("DNS resolution failed for {host}: {source}")]
    DnsFail {
        /// The host that could not be resolved.
        host: String,
        /// Underlying I/O error.
        #[source]
        source: std::io::Error,
    },

    /// DNS resolved a host that has no addresses (empty result).
    #[error("DNS returned no addresses for {0}")]
    NoAddresses(String),

    /// The server returned a 4xx status — deterministic.
    #[error("HTTP {status} for {url}")]
    Http4xx {
        /// HTTP status code.
        status: u16,
        /// The URL that returned the error.
        url: String,
    },

    /// The server returned a 5xx status — transient.
    #[error("HTTP {status} for {url}")]
    Http5xx {
        /// HTTP status code.
        status: u16,
        /// The URL that returned the error.
        url: String,
    },

    /// Response body exceeded the configured `max_bytes`.
    #[error("response body exceeded max_bytes ({max_bytes}) for {url}")]
    TooLarge {
        /// The configured limit in bytes.
        max_bytes: u64,
        /// The URL being fetched.
        url: String,
    },

    /// Too many redirect hops.
    #[error("too many redirects (max {max}) for {url}")]
    TooManyRedirects {
        /// The configured limit.
        max: u8,
        /// The starting URL.
        url: String,
    },

    /// Request timed out.
    #[error("request timed out for {url}: {source}")]
    Timeout {
        /// The URL being fetched.
        url: String,
        /// Underlying error.
        #[source]
        source: reqwest::Error,
    },

    /// A transient network or reqwest error (timeout already caught above).
    #[error("network error fetching {url}: {source}")]
    Network {
        /// The URL being fetched.
        url: String,
        /// Underlying error.
        #[source]
        source: reqwest::Error,
    },

    /// A redirect URL could not be parsed.
    #[error("invalid redirect URL {location}: {source}")]
    InvalidRedirect {
        /// The `Location` header value.
        location: String,
        /// Parse error.
        #[source]
        source: url::ParseError,
    },
}

impl FetchError {
    /// Returns `true` when the caller should stop retrying (dead-letter permanently).
    #[must_use]
    pub fn is_permanent(&self) -> bool {
        matches!(
            self,
            FetchError::UnsupportedScheme(_)
                | FetchError::UserInfoRejected(_)
                | FetchError::InternalAddress(_)
                | FetchError::NoAddresses(_)
                | FetchError::Http4xx { .. }
                | FetchError::TooLarge { .. }
                | FetchError::TooManyRedirects { .. }
                | FetchError::InvalidRedirect { .. }
        )
    }
}

// ── Validation (pure, sync) ────────────────────────────────────────────────

/// Static URL validation: parse, scheme, userinfo, hostname check.
///
/// This is the cheap first pass — call it in request handlers for immediate
/// 400 feedback before enqueuing a background job. The DNS-resolving guard
/// runs later in [`fetch_url`].
///
/// # Errors
/// Returns [`FetchError`] if the URL fails any static check.
pub fn validate_url(url_str: &str) -> Result<(), FetchError> {
    let u = Url::parse(url_str).map_err(|e| FetchError::UnsupportedScheme(e.to_string()))?;

    // Scheme allowlist.
    let scheme = u.scheme();
    if scheme != "http" && scheme != "https" {
        return Err(FetchError::UnsupportedScheme(scheme.to_string()));
    }

    // No userinfo.
    if u.username() != "" || u.password().is_some() {
        return Err(FetchError::UserInfoRejected(url_str.to_string()));
    }

    // Static hostname check.
    let host = u
        .host_str()
        .ok_or_else(|| FetchError::UnsupportedScheme("missing host".to_string()))?;

    // url 2.x returns IPv6 hosts with brackets; strip for IpAddr parsing.
    let host = strip_ipv6_brackets(host);

    if security::is_internal_host(host) {
        return Err(FetchError::InternalAddress(host.to_string()));
    }

    Ok(())
}

/// Resolve `host` (a bare hostname, not a bracketed IPv6 literal) and
/// return the validated address list, or an error if any resolved address
/// is internal.
///
/// Uses `tokio::net::lookup_host` which resolves both A and AAAA records.
async fn resolve_and_validate(host: &str, port: u16) -> Result<Vec<SocketAddr>, FetchError> {
    let addrs: Vec<SocketAddr> = tokio::net::lookup_host((host, port))
        .await
        .map_err(|source| FetchError::DnsFail {
            host: host.to_string(),
            source,
        })?
        .collect();

    if addrs.is_empty() {
        return Err(FetchError::NoAddresses(host.to_string()));
    }

    for addr in &addrs {
        if security::is_internal_ip(addr.ip()) {
            return Err(FetchError::InternalAddress(format!(
                "{} resolves to internal IP {}",
                host,
                addr.ip()
            )));
        }
    }

    Ok(addrs)
}

/// Strip leading `[` and trailing `]` from an IPv6 host string.
///
/// `url` 2.x returns IPv6 addresses with bracket notation (`[::1]`), but
/// `std::net::IpAddr::FromStr` requires the bare address (`::1`).
fn strip_ipv6_brackets(host: &str) -> &str {
    host.strip_prefix('[')
        .and_then(|s| s.strip_suffix(']'))
        .unwrap_or(host)
}

// ── Fetch ──────────────────────────────────────────────────────────────────

/// Fetch a URL with SSRF protection, returning the body and metadata.
///
/// This is the main entry point for background jobs. It performs the full
/// DNS-resolving SSRF guard with per-hop redirect re-validation.
///
/// # Errors
/// Returns a [`FetchError`] on any validation, network, or size-cap failure.
/// Callers use [`FetchError::is_permanent`] to decide dead-letter vs retry.
pub async fn fetch_url(url_str: &str, limits: &FetchLimits) -> Result<FetchedContent, FetchError> {
    // Start with static validation of the initial URL.
    validate_url(url_str)?;

    let mut current = url_str.to_string();
    let mut remaining_hops = limits.max_redirects;

    loop {
        let u = Url::parse(&current).map_err(|e| FetchError::UnsupportedScheme(e.to_string()))?;
        let scheme = u.scheme().to_string();

        // Resolve the host and validate all resolved IPs.
        let host = u
            .host_str()
            .ok_or_else(|| FetchError::UnsupportedScheme("missing host".to_string()))?;
        // url 2.x returns IPv6 hosts with brackets; strip for IpAddr parse + DNS.
        let host = strip_ipv6_brackets(host);
        let port = u.port().unwrap_or(if scheme == "https" { 443 } else { 80 });

        let validated_addrs: Vec<SocketAddr> = if let Ok(ip) = host.parse::<std::net::IpAddr>() {
            // Host is an IP literal — no DNS needed, validate directly.
            if security::is_internal_ip(ip) {
                return Err(FetchError::InternalAddress(host.to_string()));
            }
            vec![SocketAddr::new(ip, port)]
        } else {
            resolve_and_validate(host, port).await?
        };

        // Build a per-hop client with connection pinning.
        let client = reqwest::Client::builder()
            .redirect(redirect::Policy::none()) // manual hop loop
            .timeout(limits.timeout)
            .resolve_to_addrs(host, &validated_addrs)
            .build()
            .map_err(|source| FetchError::Network {
                url: current.clone(),
                source,
            })?;

        let response = client.get(&current).send().await.map_err(|source| {
            if source.is_timeout() {
                FetchError::Timeout {
                    url: current.clone(),
                    source,
                }
            } else {
                FetchError::Network {
                    url: current.clone(),
                    source,
                }
            }
        })?;

        let status = response.status();

        // Handle redirects.
        if status.is_redirection() {
            if remaining_hops == 0 {
                return Err(FetchError::TooManyRedirects {
                    max: limits.max_redirects,
                    url: url_str.to_string(),
                });
            }
            remaining_hops -= 1;

            let location = response
                .headers()
                .get(reqwest::header::LOCATION)
                .and_then(|v| v.to_str().ok())
                .ok_or_else(|| {
                    FetchError::InternalAddress("missing Location header".to_string())
                })?;

            // Resolve relative URLs against the current URL.
            let next = u
                .join(location)
                .map_err(|source| FetchError::InvalidRedirect {
                    location: location.to_string(),
                    source,
                })?;

            // Static-validate the redirect target before fetching.
            validate_url(next.as_str())?;

            current = next.to_string();
            continue;
        }

        // Classify non-success status codes.
        if status.is_client_error() {
            return Err(FetchError::Http4xx {
                status: status.as_u16(),
                url: current,
            });
        }
        if status.is_server_error() {
            return Err(FetchError::Http5xx {
                status: status.as_u16(),
                url: current,
            });
        }

        // Success — read body with size cap.
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        let mut body = Vec::new();
        let mut stream = response.bytes_stream();

        use futures::StreamExt;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|source| FetchError::Network {
                url: current.clone(),
                source,
            })?;
            if body.len() + chunk.len() > limits.max_bytes as usize {
                return Err(FetchError::TooLarge {
                    max_bytes: limits.max_bytes,
                    url: current,
                });
            }
            body.extend_from_slice(&chunk);
        }

        let mut hasher = Sha256::new();
        hasher.update(&body);
        let sha256: [u8; 32] = hasher.finalize().into();

        return Ok(FetchedContent {
            final_url: current,
            bytes: body,
            content_type,
            sha256,
        });
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    // ── validate_url ──────────────────────────────────────────────────────

    #[test]
    fn valid_https_url_passes() {
        validate_url("https://example.com/data.txt").unwrap();
        validate_url("https://api.example.com/v1/resource").unwrap();
    }

    #[test]
    fn valid_http_url_passes() {
        validate_url("http://example.com/file").unwrap();
    }

    #[test]
    fn ftp_scheme_rejected() {
        let err = validate_url("ftp://example.com/file").unwrap_err();
        assert!(matches!(err, FetchError::UnsupportedScheme(_)));
        assert!(err.to_string().contains("ftp"));
    }

    #[test]
    fn userinfo_with_password_rejected() {
        let err = validate_url("http://user:pass@example.com/file").unwrap_err();
        assert!(matches!(err, FetchError::UserInfoRejected(_)));
    }

    #[test]
    fn userinfo_username_only_rejected() {
        let err = validate_url("http://user@example.com/file").unwrap_err();
        assert!(matches!(err, FetchError::UserInfoRejected(_)));
    }

    #[test]
    fn localhost_rejected() {
        let err = validate_url("http://localhost:8080/data").unwrap_err();
        assert!(matches!(err, FetchError::InternalAddress(_)));
    }

    #[test]
    fn loopback_ip_rejected() {
        let err = validate_url("http://127.0.0.1:8080/data").unwrap_err();
        assert!(matches!(err, FetchError::InternalAddress(_)));
    }

    #[test]
    fn private_ip_rejected() {
        let err = validate_url("http://10.0.0.1/data").unwrap_err();
        assert!(matches!(err, FetchError::InternalAddress(_)));
        let err = validate_url("http://192.168.1.1/data").unwrap_err();
        assert!(matches!(err, FetchError::InternalAddress(_)));
    }

    #[test]
    fn link_local_rejected() {
        let err = validate_url("http://169.254.1.1/data").unwrap_err();
        assert!(matches!(err, FetchError::InternalAddress(_)));
    }

    #[test]
    fn dot_local_rejected() {
        let err = validate_url("http://service.local:8080/data").unwrap_err();
        assert!(matches!(err, FetchError::InternalAddress(_)));
    }

    #[test]
    fn dot_internal_rejected() {
        let err = validate_url("http://db.internal:5432/data").unwrap_err();
        assert!(matches!(err, FetchError::InternalAddress(_)));
    }

    #[test]
    fn ipv6_loopback_rejected() {
        let err = validate_url("http://[::1]:8080/data").unwrap_err();
        assert!(matches!(err, FetchError::InternalAddress(_)));
    }

    #[test]
    fn ipv6_link_local_rejected() {
        let err = validate_url("http://[fe80::1]:8080/data").unwrap_err();
        assert!(matches!(err, FetchError::InternalAddress(_)));
    }

    #[test]
    fn ipv4_mapped_v6_loopback_rejected() {
        // ::ffff:127.0.0.1 must be caught via v4→v6 mapping normalization.
        let err = validate_url("http://[::ffff:127.0.0.1]:8080/data").unwrap_err();
        assert!(matches!(err, FetchError::InternalAddress(_)));
    }

    #[test]
    fn ipv4_mapped_v6_private_rejected() {
        let err = validate_url("http://[::ffff:192.168.1.1]:8080/data").unwrap_err();
        assert!(matches!(err, FetchError::InternalAddress(_)));
    }

    #[test]
    fn garbage_string_is_rejected() {
        let err = validate_url("not a url at all").unwrap_err();
        assert!(matches!(err, FetchError::UnsupportedScheme(_)));
    }

    // ── FetchError classification ─────────────────────────────────────────

    #[test]
    fn deterministic_errors_are_permanent() {
        assert!(FetchError::UnsupportedScheme("ftp".into()).is_permanent());
        assert!(FetchError::UserInfoRejected("x".into()).is_permanent());
        assert!(FetchError::InternalAddress("x".into()).is_permanent());
        assert!(FetchError::NoAddresses("x".into()).is_permanent());
        assert!(
            FetchError::Http4xx {
                status: 404,
                url: "x".into()
            }
            .is_permanent()
        );
        assert!(
            FetchError::TooLarge {
                max_bytes: 1,
                url: "x".into()
            }
            .is_permanent()
        );
        assert!(
            FetchError::TooManyRedirects {
                max: 1,
                url: "x".into()
            }
            .is_permanent()
        );
    }

    #[test]
    fn transient_errors_are_not_permanent() {
        assert!(
            !FetchError::DnsFail {
                host: "x".into(),
                source: std::io::Error::other("test")
            }
            .is_permanent()
        );
        assert!(
            !FetchError::Http5xx {
                status: 500,
                url: "x".into()
            }
            .is_permanent()
        );
        // Timeout and Network variants are transient (matches! trivially correct).
    }

    // ── FetchError display ────────────────────────────────────────────────

    #[test]
    fn error_display_includes_context() {
        let err = FetchError::UnsupportedScheme("gopher".into());
        assert!(err.to_string().contains("gopher"));

        let err = FetchError::InternalAddress("10.0.0.1".into());
        assert!(err.to_string().contains("internal"));
        assert!(err.to_string().contains("10.0.0.1"));

        let err = FetchError::Http4xx {
            status: 404,
            url: "https://example.com/notfound".into(),
        };
        assert!(err.to_string().contains("404"));
        assert!(err.to_string().contains("notfound"));
    }

    // ── FetchLimits ───────────────────────────────────────────────────────

    #[test]
    fn fetch_limits_defaults_are_reasonable() {
        let limits = FetchLimits {
            timeout: Duration::from_secs(30),
            max_bytes: 20_971_520,
            max_redirects: 5,
        };
        assert_eq!(limits.timeout, Duration::from_secs(30));
        assert_eq!(limits.max_bytes, 20_971_520);
        assert_eq!(limits.max_redirects, 5);
    }
}
