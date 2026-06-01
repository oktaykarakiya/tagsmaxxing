//! CDN URL rewriting for blob egress economics (plan §20, P8-T13).
//!
//! B2 + Cloudflare are in the [Bandwidth
//! Alliance](https://www.cloudflare.com/bandwidth-alliance/backblaze/) — B2→Cloudflare egress
//! is free and edge-cached. When a CDN base URL is configured, presigned and static blob URLs
//! are rewritten through the CDN so clients pull from the edge rather than directly from B2.
//! This is the primary COGS lever for a low-priced product.

/// Rewrite a blob URL through the configured CDN.
///
/// When `cdn_base` is empty, the URL is returned unchanged. When set, the host and
/// scheme of the original URL are replaced with those from `cdn_base`, preserving
/// the path and query string. This works for both presigned B2 URLs (which carry S3
/// signature query params) and static blob URLs.
///
/// # Examples
///
/// ```
/// # use kb_pipeline::cdn::rewrite_blob_url;
/// // CDN configured — URL is rewritten.
/// let url = "https://s3.us-west-004.backblazeb2.com/file/my-bucket/key";
/// let cdn  = "https://cdn.example.com";
/// assert_eq!(
///     rewrite_blob_url(url, cdn),
///     "https://cdn.example.com/file/my-bucket/key"
/// );
///
/// // No CDN configured — passthrough.
/// assert_eq!(rewrite_blob_url(url, ""), url);
/// ```
///
/// # Presigned URLs
///
/// Presigned B2 URLs carry authentication in the query string (`?X-Amz-...`).
/// The CDN must be configured to forward query strings to the origin:
///
/// ```text
/// https://s3.us-west-004.backblazeb2.com/file/bucket/key?X-Amz-Algorithm=...
///   → https://cdn.example.com/file/bucket/key?X-Amz-Algorithm=...
/// ```
///
/// If the `cdn_base` URL has a path prefix (e.g. `https://cdn.example.com/assets`),
/// that prefix is prepended to the original path.
#[must_use]
pub fn rewrite_blob_url(url: &str, cdn_base: &str) -> String {
    if cdn_base.is_empty() {
        return url.to_string();
    }

    // Parse the CDN base URL to extract scheme + host + optional path prefix.
    let (cdn_scheme_host, cdn_path_prefix) = split_scheme_host_path(cdn_base);

    match split_url_after_scheme_host(url) {
        None => url.to_string(), // Cannot parse — pass through unchanged.
        Some(path_and_query) => {
            let path_and_query = path_and_query.trim_start_matches('/');
            if cdn_path_prefix.is_empty() {
                format!("{cdn_scheme_host}/{path_and_query}")
            } else {
                format!("{cdn_scheme_host}/{cdn_path_prefix}/{path_and_query}")
            }
        }
    }
}

/// Split a URL into `(scheme://host, path_prefix)`.
///
/// Returns `("https://host", "optional/path")` — the path_prefix has no
/// leading or trailing slash.
fn split_scheme_host_path(raw: &str) -> (String, String) {
    let without_scheme = raw
        .strip_prefix("https://")
        .or_else(|| raw.strip_prefix("http://"))
        .unwrap_or(raw);

    match without_scheme.find('/') {
        Some(slash) => {
            let host = &without_scheme[..slash];
            let path = without_scheme[slash + 1..].trim_end_matches('/');
            let scheme = if raw.starts_with("https://") {
                "https"
            } else {
                "http"
            };
            (format!("{scheme}://{host}"), path.to_string())
        }
        None => {
            // No path in CDN base.
            let scheme = if raw.starts_with("https://") {
                "https"
            } else {
                "http"
            };
            (format!("{scheme}://{without_scheme}"), String::new())
        }
    }
}

/// Extract the portion of a URL after `scheme://host`, including the leading `/`.
///
/// Returns `None` if the URL cannot be parsed (no `://` separator).
fn split_url_after_scheme_host(url: &str) -> Option<&str> {
    let rest = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))?;
    rest.find('/').map(|slash| &rest[slash..])
}

// ── Tests ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    // ── rewrite_blob_url ──────────────────────────────────────────────────

    #[test]
    fn empty_cdn_is_passthrough() {
        let url = "https://s3.us-west-004.backblazeb2.com/file/bucket/obj";
        assert_eq!(rewrite_blob_url(url, ""), url);
    }

    #[test]
    fn rewrites_b2_url_to_cdn() {
        let url = "https://s3.us-west-004.backblazeb2.com/file/my-bucket/tenant/abc123";
        let cdn = "https://cdn.example.com";
        assert_eq!(
            rewrite_blob_url(url, cdn),
            "https://cdn.example.com/file/my-bucket/tenant/abc123"
        );
    }

    #[test]
    fn rewrites_presigned_url_preserving_query() {
        let url = "https://s3.us-west-004.backblazeb2.com/file/bucket/key?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=foo&X-Amz-Signature=bar";
        let cdn = "https://cdn.example.com";
        let rewritten = rewrite_blob_url(url, cdn);
        assert!(rewritten.starts_with("https://cdn.example.com/"));
        assert!(rewritten.contains("X-Amz-Algorithm=AWS4-HMAC-SHA256"));
        assert!(rewritten.contains("X-Amz-Signature=bar"));
    }

    #[test]
    fn cdn_with_path_prefix() {
        let url = "https://s3.example.com/file/bucket/key";
        let cdn = "https://cdn.example.com/assets";
        assert_eq!(
            rewrite_blob_url(url, cdn),
            "https://cdn.example.com/assets/file/bucket/key"
        );
    }

    #[test]
    fn cdn_with_trailing_slash_is_normalized() {
        let url = "https://s3.example.com/file/bucket/key";
        let cdn = "https://cdn.example.com/";
        assert_eq!(
            rewrite_blob_url(url, cdn),
            "https://cdn.example.com/file/bucket/key"
        );
    }

    #[test]
    fn http_cdn_preserves_http() {
        let url = "https://s3.example.com/file/bucket/key";
        let cdn = "http://cdn.local:8080";
        assert_eq!(
            rewrite_blob_url(url, cdn),
            "http://cdn.local:8080/file/bucket/key"
        );
    }

    #[test]
    fn handles_local_file_url_passthrough() {
        let url = "file:///var/lib/kb/blobs/key";
        // file:// URLs don't match the https? scheme extraction, so they
        // pass through the "cannot parse" branch unchanged.
        assert_eq!(rewrite_blob_url(url, "https://cdn.example.com"), url);
    }

    #[test]
    fn handles_minio_local_url() {
        let url = "http://127.0.0.1:9000/test-bucket/tenant/key";
        let cdn = "https://cdn.example.com";
        assert_eq!(
            rewrite_blob_url(url, cdn),
            "https://cdn.example.com/test-bucket/tenant/key"
        );
    }

    #[test]
    fn url_with_no_path_is_passthrough() {
        // URL with no path component (e.g. endpoint root).
        // The function passes through because there's nothing to rewrite.
        let url = "https://s3.example.com";
        let cdn = "https://cdn.example.com";
        assert_eq!(rewrite_blob_url(url, cdn), "https://s3.example.com");
    }

    // ── split_scheme_host_path ────────────────────────────────────────────

    #[test]
    fn split_https_no_path() {
        let (sch_host, path) = split_scheme_host_path("https://cdn.example.com");
        assert_eq!(sch_host, "https://cdn.example.com");
        assert!(path.is_empty());
    }

    #[test]
    fn split_https_with_path() {
        let (sch_host, path) = split_scheme_host_path("https://cdn.example.com/assets");
        assert_eq!(sch_host, "https://cdn.example.com");
        assert_eq!(path, "assets");
    }

    #[test]
    fn split_https_with_deep_path() {
        let (sch_host, path) = split_scheme_host_path("https://cdn.example.com/a/b/c");
        assert_eq!(sch_host, "https://cdn.example.com");
        assert_eq!(path, "a/b/c");
    }

    #[test]
    fn split_http_with_path() {
        let (sch_host, path) = split_scheme_host_path("http://localhost:8080/prefix");
        assert_eq!(sch_host, "http://localhost:8080");
        assert_eq!(path, "prefix");
    }

    #[test]
    fn split_trims_trailing_slash() {
        let (sch_host, path) = split_scheme_host_path("https://cdn.example.com/foo/");
        assert_eq!(sch_host, "https://cdn.example.com");
        assert_eq!(path, "foo");
    }

    // ── split_url_after_scheme_host ───────────────────────────────────────

    #[test]
    fn split_url_with_path() {
        let rest = split_url_after_scheme_host("https://s3.example.com/file/bucket/key").unwrap();
        assert_eq!(rest, "/file/bucket/key");
    }

    #[test]
    fn split_url_with_query() {
        let rest =
            split_url_after_scheme_host("https://s3.example.com/file/bucket/key?X-Amz-Alg=x")
                .unwrap();
        assert_eq!(rest, "/file/bucket/key?X-Amz-Alg=x");
    }

    #[test]
    fn split_url_no_path_returns_none() {
        assert!(split_url_after_scheme_host("https://s3.example.com").is_none());
    }

    #[test]
    fn split_url_no_scheme_returns_none() {
        assert!(split_url_after_scheme_host("not-a-url").is_none());
    }

    #[test]
    fn split_file_url() {
        // file:// has no https?/http? prefix — returns None.
        assert!(split_url_after_scheme_host("file:///tmp/blob").is_none());
    }
}
