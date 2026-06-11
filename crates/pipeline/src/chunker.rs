//! Token-aware text chunker with controllable size, overlap, and provenance
//! (plan §7, §8).
//!
//! Each chunk records exactly which `file_id` (page) it came from, its `page_no`
//! ordering, and an optional `ts_offset` for transcript deep-linking. The chunker is
//! pure logic — no I/O, no async — so it is trivially unit-testable.

// ── Constants ─────────────────────────────────────────────────────────────────

/// Default chunk size in characters. At ~4 chars per English token this is roughly
/// 512 tokens — small enough to fit comfortably in an LLM context window with room
/// for the retrieval prompt (plan §7).
pub const DEFAULT_CHUNK_SIZE_CHARS: usize = 2048;

/// Default overlap in characters between consecutive chunks (~16 tokens). Overlap
/// prevents splitting a key phrase across a chunk boundary (plan §7).
pub const DEFAULT_OVERLAP_CHARS: usize = 64;

/// Maximum number of texts to send in a single embedding API call. Batches larger
/// than this are split transparently by [`super::embedder::ChunkEmbedder`].
pub const MAX_EMBED_BATCH_SIZE: usize = 32;

// ── TextChunk ─────────────────────────────────────────────────────────────────

/// An unembedded text chunk with full provenance.
///
/// After chunking, the caller passes these to
/// [`ChunkEmbedder`](super::embedder::ChunkEmbedder) to produce fully-formed
/// [`Chunk`](kb_core::chunk::Chunk) records with embeddings attached.
#[derive(Debug, Clone, PartialEq)]
pub struct TextChunk {
    /// The chunk's text content.
    pub content: String,
    /// Zero-based index of this chunk within its source file.
    pub idx: i32,
    /// Page number within the document (1-based, from the source [`FileRecord`]).
    pub page_no: Option<i32>,
    /// Source file id for provenance (maps to `chunks.file_id`).
    pub file_id: i64,
    /// Seconds into audio/video for transcript chunks; `None` otherwise.
    pub ts_offset: Option<f64>,
}

// ── Public API ────────────────────────────────────────────────────────────────

/// Split `text` into overlapping chunks with the given provenance fields.
///
/// Each chunk (except possibly the last) is `chunk_size` **characters** long (not
/// bytes — multi-byte UTF-8 is handled correctly). Consecutive chunks overlap by
/// `overlap` characters.
///
/// # Panics
///
/// Panics in debug builds if `chunk_size <= overlap`.
///
/// # Edge cases
///
/// - **Empty text** → empty `Vec`.
/// - **Text shorter than `chunk_size`** → a single chunk containing all of `text`.
/// - **Overlap = 0** → contiguous non-overlapping chunks.
#[must_use]
pub fn chunk_text(
    text: &str,
    file_id: i64,
    page_no: Option<i32>,
    ts_offset: Option<f64>,
    chunk_size: usize,
    overlap: usize,
) -> Vec<TextChunk> {
    assert!(
        chunk_size > overlap,
        "chunk_size ({chunk_size}) must be greater than overlap ({overlap})"
    );

    let chars: Vec<char> = text.chars().collect();
    let total_chars = chars.len();

    if total_chars == 0 {
        return Vec::new();
    }

    let step = chunk_size.saturating_sub(overlap);
    // Safety: step ≥ 1 because chunk_size > overlap (asserted above).
    let mut chunks = Vec::new();
    let mut start: usize = 0;
    let mut idx: i32 = 0;

    while start < total_chars {
        let end = (start + chunk_size).min(total_chars);
        let content: String = chars[start..end].iter().collect();

        // Guard: if we can't extract any chars (shouldn't happen given the
        // loop condition, but be defensive).
        if content.is_empty() {
            break;
        }

        chunks.push(TextChunk {
            content,
            idx,
            page_no,
            file_id,
            ts_offset,
        });

        idx += 1;
        start += step;
    }

    chunks
}

/// Collapse chunks with identical content within one file (BUG-INGEST-19).
///
/// Pathologically repetitive content — e.g. a 200 KB uniform-byte file —
/// otherwise stores thousands of identical chunks whose identical embeddings
/// act as a semantic "attractor" in vector search (BUG-SEARCH-04's corpus
/// poisoning) while adding zero retrieval value and paying per-chunk embedding
/// cost for nothing. The FIRST occurrence survives with its provenance fields
/// (earliest page/position); later duplicates are dropped. Original `idx`
/// values are preserved — gaps are fine, `(file_id, idx)` stays unique and
/// position-ordered.
#[must_use]
pub fn dedup_identical_chunks(chunks: Vec<TextChunk>) -> Vec<TextChunk> {
    let mut seen = std::collections::HashSet::new();
    chunks
        .into_iter()
        .filter(|c| seen.insert(c.content.clone()))
        .collect()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    // ── Short text → 1 chunk ──────────────────────────────────────────────

    #[test]
    fn short_text_single_chunk() {
        let text = "Hello, world!";
        let chunks = chunk_text(text, 42, Some(1), None, 2048, 64);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].content, "Hello, world!");
        assert_eq!(chunks[0].idx, 0);
        assert_eq!(chunks[0].file_id, 42);
        assert_eq!(chunks[0].page_no, Some(1));
        assert_eq!(chunks[0].ts_offset, None);
    }

    #[test]
    fn exact_chunk_size_text_gives_one_chunk() {
        // Exactly 10 chars with chunk_size=10, overlap=0 → 1 chunk.
        let text = "0123456789";
        let chunks = chunk_text(text, 1, Some(1), None, 10, 0);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].content, "0123456789");
        assert_eq!(chunks[0].idx, 0);
    }

    // ── Empty text → empty vec ────────────────────────────────────────────

    #[test]
    fn empty_text_empty_vec() {
        let chunks = chunk_text("", 1, None, None, 2048, 64);
        assert!(chunks.is_empty());
    }

    #[test]
    fn whitespace_only_text_gets_one_chunk() {
        let text = "   \n\t  ";
        let chunks = chunk_text(text, 1, None, None, 10, 2);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].content, text);
    }

    // ── Long text → multiple chunks ───────────────────────────────────────

    #[test]
    fn long_text_multiple_chunks() {
        // 50 chars, chunk_size=20, overlap=0 → 3 chunks (20+20+10).
        let text: String = "a".repeat(50);
        let chunks = chunk_text(&text, 1, None, None, 20, 0);
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].content.len(), 20);
        assert_eq!(chunks[0].idx, 0);
        assert_eq!(chunks[1].content.len(), 20);
        assert_eq!(chunks[1].idx, 1);
        assert_eq!(chunks[2].content.len(), 10);
        assert_eq!(chunks[2].idx, 2);
    }

    #[test]
    fn long_text_with_overlap() {
        // 30 chars, chunk_size=12, overlap=4 → step=8.
        // Chunks start at: 0, 8, 16, 24 → 4 chunks.
        let text = "abcdefghijklmnopqrstuvwxyz0123";
        let chunks = chunk_text(text, 1, None, None, 12, 4);

        assert_eq!(chunks.len(), 4);

        // Chunk 0: chars 0..12
        assert_eq!(chunks[0].content, "abcdefghijkl");
        assert_eq!(chunks[0].idx, 0);

        // Chunk 1: chars 8..20
        assert_eq!(chunks[1].content, "ijklmnopqrst");
        assert_eq!(chunks[1].idx, 1);

        // Chunk 2: chars 16..28
        assert_eq!(chunks[2].content, "qrstuvwxyz01");
        assert_eq!(chunks[2].idx, 2);

        // Chunk 3: chars 24..30
        assert_eq!(chunks[3].content, "yz0123");
        assert_eq!(chunks[3].idx, 3);

        // Overlap between chunk 0 and chunk 1 should be "ijkl" (chars 8..12 of
        // text, which is chars 4..8 of chunk 0 and 0..4 of chunk 1).
        assert!(chunks[0].content.ends_with("ijkl"));
        assert!(chunks[1].content.starts_with("ijkl"));
    }

    // ── Provenance fields ─────────────────────────────────────────────────

    #[test]
    fn chunk_preserves_file_id() {
        let text = "some text for testing provenance";
        let chunks = chunk_text(text, 99, Some(2), None, 10, 2);
        for chunk in &chunks {
            assert_eq!(chunk.file_id, 99);
        }
    }

    #[test]
    fn chunk_preserves_page_no() {
        let text = "testing page number preservation";
        let chunks = chunk_text(text, 1, Some(3), None, 10, 2);
        for chunk in &chunks {
            assert_eq!(chunk.page_no, Some(3));
        }
    }

    #[test]
    fn chunk_preserves_ts_offset() {
        let text = "transcript text with timestamp offset";
        let chunks = chunk_text(text, 1, Some(1), Some(42.5), 10, 2);
        for chunk in &chunks {
            assert_eq!(chunk.ts_offset, Some(42.5));
        }
    }

    #[test]
    fn chunk_ts_offset_none_for_non_transcript() {
        let text = "regular text, no timestamp";
        let chunks = chunk_text(text, 1, Some(1), None, 10, 2);
        for chunk in &chunks {
            assert_eq!(chunk.ts_offset, None);
        }
    }

    // ── Index ordering ────────────────────────────────────────────────────

    #[test]
    fn chunk_indices_are_sequential() {
        let text: String = "x".repeat(100);
        let chunks = chunk_text(&text, 1, None, None, 15, 3);
        for (i, chunk) in chunks.iter().enumerate() {
            assert_eq!(chunk.idx, i as i32);
        }
    }

    // ── Multi-byte UTF-8 ──────────────────────────────────────────────────

    #[test]
    fn handles_multi_byte_utf8_characters() {
        // Each emoji is a single char but 4 bytes.
        let text = "🦀🦀🦀🦀🦀🦀🦀🦀🦀🦀"; // 10 crab emojis
        let chunks = chunk_text(text, 1, None, None, 4, 1);
        // step=3: starts at 0,3,6,9 → 4 chunks
        assert_eq!(chunks.len(), 4);
        // Each chunk should have correct char count, not byte count.
        assert_eq!(chunks[0].content.chars().count(), 4);
        assert_eq!(chunks[1].content.chars().count(), 4);
        assert_eq!(chunks[2].content.chars().count(), 4);
        assert_eq!(chunks[3].content.chars().count(), 1);
    }

    #[test]
    fn handles_mixed_ascii_and_utf8() {
        let text = "a🦀b🦀c🦀d🦀e";
        let chunks = chunk_text(text, 1, None, None, 3, 1);
        // step=2: 9 chars, starts at 0,2,4,6,8 → 5 chunks
        assert_eq!(chunks.len(), 5);
        // All content concatenated should reconstruct the original.
        // (With overlap some chars appear multiple times.)
        // Just verify no panics and correct char counts.
        for chunk in &chunks {
            assert!(!chunk.content.is_empty());
        }
    }

    // ── Overlap zero ──────────────────────────────────────────────────────

    #[test]
    fn zero_overlap_no_shared_content() {
        let text = "ABCDEFGHIJ";
        let chunks = chunk_text(text, 1, None, None, 4, 0);
        // step=4: starts at 0,4,8 → 3 chunks
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].content, "ABCD");
        assert_eq!(chunks[1].content, "EFGH");
        assert_eq!(chunks[2].content, "IJ");
    }

    // ── Panic guard ───────────────────────────────────────────────────────

    #[test]
    #[should_panic(expected = "chunk_size")]
    fn panics_when_chunk_size_not_greater_than_overlap() {
        let _ = chunk_text("hello", 1, None, None, 10, 10);
    }

    #[test]
    #[should_panic(expected = "chunk_size")]
    fn panics_when_overlap_greater_than_chunk_size() {
        let _ = chunk_text("hello", 1, None, None, 5, 10);
    }

    // ── Chunk content integrity ───────────────────────────────────────────

    #[test]
    fn all_content_covered_without_gaps() {
        // For zero-overlap chunks, concatenating should give original text.
        let text = "This is a test of chunk coverage integrity.";
        let chunks = chunk_text(text, 1, None, None, 15, 0);
        let reconstructed: String = chunks.iter().map(|c| c.content.as_str()).collect();
        assert_eq!(reconstructed, text);
    }

    // ── Large step (relative to text) ─────────────────────────────────────

    #[test]
    fn chunk_size_larger_than_text_is_single_chunk() {
        let text = "tiny";
        let chunks = chunk_text(text, 1, None, None, 1000, 64);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].content, "tiny");
    }

    // ── dedup_identical_chunks (BUG-INGEST-19) ────────────────────────────

    #[test]
    fn dedup_uniform_content_collapses_to_distinct_chunks() {
        // 200 KB of a single repeated character — the corpus-poisoning case.
        // Every full-size chunk is identical; only the (shorter) tail differs,
        // so thousands of chunks must collapse to exactly two.
        let text = "A".repeat(200 * 1024);
        let raw = chunk_text(&text, 7, Some(1), None, 1000, 0);
        assert!(raw.len() > 100, "precondition: many raw chunks");
        let deduped = dedup_identical_chunks(raw);
        assert_eq!(
            deduped.len(),
            2,
            "uniform text must collapse to the repeated block + the tail"
        );
        assert_eq!(deduped[0].idx, 0, "first occurrence survives");
        assert_eq!(deduped[0].file_id, 7);
    }

    #[test]
    fn dedup_keeps_distinct_chunks_and_order() {
        let mut chunks = chunk_text("alpha beta", 1, None, None, 5, 0);
        chunks.extend(chunk_text("alpha beta", 1, None, None, 5, 0));
        assert_eq!(chunks.len(), 4); // "alpha", " beta" twice each
        let deduped = dedup_identical_chunks(chunks);
        let contents: Vec<&str> = deduped.iter().map(|c| c.content.as_str()).collect();
        assert_eq!(
            contents,
            vec!["alpha", " beta"],
            "order of first occurrences kept"
        );
    }

    #[test]
    fn dedup_preserves_original_idx_values() {
        // Survivors keep their position idx (gaps allowed) so provenance and
        // the (file_id, idx) uniqueness both hold.
        let text = "xxxxx".repeat(3); // chunks of 5: "xxxxx" ×3 → 1 survivor
        let raw = chunk_text(&text, 1, None, None, 5, 0);
        assert_eq!(raw.len(), 3);
        let deduped = dedup_identical_chunks(raw);
        assert_eq!(deduped.len(), 1);
        assert_eq!(deduped[0].idx, 0);
    }

    #[test]
    fn dedup_empty_input_is_empty() {
        assert!(dedup_identical_chunks(Vec::new()).is_empty());
    }

    // ── Constants are stable ──────────────────────────────────────────────

    #[test]
    fn default_constants_are_sensible() {
        // These are design-time constants; changing them is a deliberate
        // decision that should be reflected in a test update.
        assert_eq!(DEFAULT_CHUNK_SIZE_CHARS, 2048);
        assert_eq!(DEFAULT_OVERLAP_CHARS, 64);
        assert_eq!(MAX_EMBED_BATCH_SIZE, 32);
    }
}
