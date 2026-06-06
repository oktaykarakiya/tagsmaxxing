//! Handler for `kb search <QUERY>`.
//!
//! Builds a [`Query`] from the parsed CLI arguments, calls
//! [`RetrievalPipeline::retrieve`], and prints ranked [`Hit`]s with their
//! title, summary/snippet, score, and deep-link provenance (page_no, file_id).

use std::str::FromStr;

use anyhow::Context;
use kb_core::kind::DocKind;
use kb_core::query::{Hit, Query, QueryFilters};
use kb_pipeline::RetrievalPipeline;

use crate::cli::SearchArgs;

/// Run the search command against the given pipeline.
///
/// # Errors
///
/// Returns an error if the query embedding, hybrid search, or reranking fails.
/// An empty result set is not an error — it prints "No results." and exits
/// successfully.
pub async fn run_search(
    args: &SearchArgs,
    pipeline: &RetrievalPipeline,
    tenant_id: i64,
) -> anyhow::Result<()> {
    let query = build_query(args);
    // CLI/system search: no attributable acting user (P14-T2), and never force the
    // keyword degrade — `degrade = false` (P14-T5). The mode is ignored for CLI output.
    // `local_only = false`: a system context, not a plan-gated request path, so the
    // remote-models plan gate (P14-T6) does not apply — remote backends used if configured.
    let (hits, _mode) = pipeline
        .retrieve(tenant_id, None, &query, false, false)
        .await
        .context("search pipeline failed")?;

    if hits.is_empty() {
        println!("No results.");
        return Ok(());
    }

    for (i, hit) in hits.iter().enumerate() {
        print_hit(i + 1, hit);
    }

    Ok(())
}

// ── Query construction (pure logic) ───────────────────────────────────────────

/// Build a typed [`Query`] from the CLI search arguments.
///
/// Kind and tag filters are parsed from their string representations. Invalid
/// kind names are silently dropped (no filter is applied for that dimension).
fn build_query(args: &SearchArgs) -> Query {
    let mut filters = QueryFilters::default();

    if let Some(ref kind_str) = args.kind
        && let Ok(kind) = DocKind::from_str(kind_str)
    {
        filters.kinds = vec![kind];
    }
    // Unknown kind → no kind filter (the user probably typo'd; the search
    // still runs, just without the kind restriction).

    if let Some(ref tag) = args.tag {
        filters.tags = vec![tag.clone()];
    }

    Query {
        text: args.query.clone(),
        filters,
        top_k: args.limit,
    }
}

// ── Output formatting (pure logic) ────────────────────────────────────────────

/// Print a single hit to stdout in a compact, human-readable format.
///
/// Each hit is numbered and shows the document title (or `(untitled)`), snippet,
/// score, and deep-link provenance (page number and file id).
fn print_hit(idx: usize, hit: &Hit) {
    let title = hit.title.as_deref().unwrap_or("(untitled)");
    let page = hit
        .page_no
        .map_or_else(|| "—".to_string(), |p| p.to_string());

    println!("{idx}. [{score:.4}] {title}", score = hit.score);
    println!("   page: {page}  file_id: {}", hit.file_id);

    // Print snippet, truncating long lines for terminal readability.
    let snippet = truncate_str(&hit.snippet, 200);
    println!("   {snippet}");
}

/// Truncate a string to at most `max_len` **bytes**, appending "…" when
/// truncation actually occurs. The ellipsis is included in the byte budget.
fn truncate_str(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        return s.to_string();
    }

    let ellipsis = "…";
    // If max_len is smaller than the ellipsis itself, just return the ellipsis.
    if max_len <= ellipsis.len() {
        return ellipsis.to_string();
    }

    let content_max = max_len - ellipsis.len();
    let mut end = content_max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}{}", &s[..end], ellipsis)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    // ── build_query tests ──────────────────────────────────────────────────

    #[test]
    fn query_basic_with_default_limit() {
        let args = SearchArgs {
            query: "hello world".into(),
            kind: None,
            tag: None,
            limit: 10,
        };
        let query = build_query(&args);
        assert_eq!(query.text, "hello world");
        assert_eq!(query.top_k, 10);
        assert!(query.filters.kinds.is_empty());
        assert!(query.filters.tags.is_empty());
    }

    #[test]
    fn query_with_kind_filter_valid() {
        let args = SearchArgs {
            query: "cats".into(),
            kind: Some("image".into()),
            tag: None,
            limit: 5,
        };
        let query = build_query(&args);
        assert_eq!(query.filters.kinds, vec![DocKind::Image]);
    }

    #[test]
    fn query_with_kind_filter_invalid_is_ignored() {
        let args = SearchArgs {
            query: "cats".into(),
            kind: Some("not_a_kind".into()),
            tag: None,
            limit: 5,
        };
        let query = build_query(&args);
        assert!(query.filters.kinds.is_empty());
    }

    #[test]
    fn query_with_tag_filter() {
        let args = SearchArgs {
            query: "report".into(),
            kind: None,
            tag: Some("invoice".into()),
            limit: 20,
        };
        let query = build_query(&args);
        assert_eq!(query.filters.tags, vec!["invoice"]);
    }

    #[test]
    fn query_with_both_filters() {
        let args = SearchArgs {
            query: "budget".into(),
            kind: Some("document".into()),
            tag: Some("finance".into()),
            limit: 15,
        };
        let query = build_query(&args);
        assert_eq!(query.filters.kinds, vec![DocKind::Document]);
        assert_eq!(query.filters.tags, vec!["finance"]);
        assert_eq!(query.top_k, 15);
    }

    #[test]
    fn query_all_doc_kinds_parse_correctly() {
        for kind in DocKind::all() {
            let args = SearchArgs {
                query: "x".into(),
                kind: Some(kind.as_str().to_string()),
                tag: None,
                limit: 10,
            };
            let query = build_query(&args);
            assert_eq!(query.filters.kinds, vec![*kind]);
        }
    }

    // ── truncate_str tests ─────────────────────────────────────────────────

    #[test]
    fn truncate_short_string_is_noop() {
        assert_eq!(truncate_str("hi", 200), "hi");
    }

    #[test]
    fn truncate_empty_string() {
        assert_eq!(truncate_str("", 10), "");
    }

    #[test]
    fn truncate_long_string_adds_ellipsis() {
        let s = "a".repeat(100);
        let result = truncate_str(&s, 10);
        // 10 byte budget: 7 content bytes + 3-byte "…" ellipsis
        assert_eq!(result.len(), 10);
        assert!(result.ends_with('…'));
    }

    #[test]
    fn truncate_boundary_exact_length_is_noop() {
        let s = "1234567890"; // 10 ASCII chars
        assert_eq!(truncate_str(s, 10), s);
    }

    #[test]
    fn truncate_max_len_zero_is_handled() {
        // max_len=0: smaller than "…" (3 bytes) → just returns "…"
        let result = truncate_str("hello", 0);
        assert_eq!(result, "…");
    }

    #[test]
    fn truncate_unicode_boundary_is_respected() {
        // "é" is 2 bytes — we must not split it.
        let s = "éééééééééé"; // 10 × 2-byte chars = 20 bytes
        let result = truncate_str(s, 7);
        // 7 byte budget: 4 content bytes (2×"é") + 3-byte "…"
        assert!(result.ends_with('…'));
        // Should contain two full "é" chars (4 bytes) + ellipsis (3 bytes) = 7
        assert_eq!(result.len(), 7);
    }

    // ── print_hit smoke tests (just checks no panic) ─────────────────────────

    #[test]
    fn print_hit_with_all_fields() {
        let hit = Hit {
            document_id: 1,
            score: 0.9876,
            title: Some("Quarterly Report".into()),
            snippet: "The Q3 revenue exceeded expectations...".into(),
            file_id: 42,
            page_no: Some(3),
            ts_offset: None,
            kind: None,
        };
        print_hit(1, &hit); // should not panic
    }

    #[test]
    fn print_hit_minimal_fields() {
        let hit = Hit {
            document_id: 2,
            score: 0.5,
            title: None,
            snippet: String::new(),
            file_id: 7,
            page_no: None,
            ts_offset: Some(12.5),
            kind: None,
        };
        print_hit(2, &hit); // should not panic
    }

    #[test]
    fn print_hit_long_snippet_is_truncated() {
        let hit = Hit {
            document_id: 3,
            score: 0.99,
            title: Some("Doc".into()),
            snippet: "x".repeat(500),
            file_id: 1,
            page_no: Some(1),
            ts_offset: None,
            kind: None,
        };
        print_hit(1, &hit); // should not panic, snippet truncated
    }
}
