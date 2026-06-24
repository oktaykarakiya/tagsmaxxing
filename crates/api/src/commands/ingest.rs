// SPDX-License-Identifier: AGPL-3.0-or-later

//! Handler for `kb ingest <FILES...>`.
//!
//! Reads each file from disk, constructs [`IngestFile`] records, and calls
//! [`IngestPipeline::ingest`] synchronously (the pipeline itself is async, but the
//! CLI blocks until it finishes). Results are printed to stdout.

use anyhow::Context;
use kb_pipeline::{IngestFile, IngestPipeline};

use crate::cli::IngestArgs;

/// Run the ingest command against the given pipeline.
///
/// # Errors
///
/// Returns an error if any file cannot be read, or if the pipeline fails at any
/// step (extraction, tagging, embedding, or database upsert).
pub async fn run_ingest(
    args: &IngestArgs,
    pipeline: &IngestPipeline,
    tenant_id: i64,
    max_payload_bytes: u64,
) -> anyhow::Result<()> {
    let files = read_files(&args.files, max_payload_bytes)?;

    let note = args.note.clone();
    // CLI ingest runs as the operator (no per-user attribution), so usage is
    // recorded tenant-only (P14-T1). `local_only = false`: this is a system
    // context, not a plan-gated request path, so the remote-models plan gate
    // (P14-T6) does not apply here — remote backends are used if configured.
    let output = pipeline
        .ingest(tenant_id, None, files, note, false)
        .await
        .context("ingest pipeline failed")?;

    println!("document_id: {}", output.document_id);
    println!("tags: {}", output.tag_ids.len());
    println!("chunks: {}", output.chunk_count);
    println!("status: ready");

    Ok(())
}

// ── File reading ──────────────────────────────────────────────────────────────

/// Read raw bytes for each path on disk into [`IngestFile`] records.
///
/// Each file's `path` is stored for provenance (display and blob-key derivation).
/// The `page_label` is left empty — multi-page labelling is handled by the
/// `--as-document` flag at a higher level.
///
/// Files exceeding `max_bytes` are rejected before reading.
///
/// # Errors
///
/// Returns an error if any individual file cannot be read (missing, permission
/// denied, etc.) or exceeds the size limit. The error message includes the
/// offending path.
fn read_files(paths: &[String], max_bytes: u64) -> anyhow::Result<Vec<IngestFile>> {
    paths
        .iter()
        .map(|p| {
            let metadata = std::fs::metadata(p)
                .with_context(|| format!("failed to read metadata for '{p}'"))?;
            if metadata.len() > max_bytes {
                anyhow::bail!(
                    "file '{p}' exceeds maximum upload size of {max_bytes} bytes ({len} bytes)",
                    len = metadata.len()
                );
            }
            let bytes = std::fs::read(p).with_context(|| format!("failed to read file '{p}'"))?;
            Ok(IngestFile {
                bytes,
                page_label: None,
                path: Some(p.clone()),
            })
        })
        .collect()
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    const TEST_MAX_BYTES: u64 = 100 * 1024 * 1024;

    // ── read_files tests ──────────────────────────────────────────────────

    #[test]
    fn read_single_text_file() {
        let mut tmp = NamedTempFile::new().unwrap();
        writeln!(tmp, "hello world").unwrap();
        let path = tmp.path().to_string_lossy().to_string();

        let files = read_files(&[path], TEST_MAX_BYTES).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].bytes, b"hello world\n");
        assert!(files[0].page_label.is_none());
    }

    #[test]
    fn read_multiple_files_preserves_order() {
        let mut tmp1 = NamedTempFile::new().unwrap();
        writeln!(tmp1, "aaa").unwrap();
        let mut tmp2 = NamedTempFile::new().unwrap();
        writeln!(tmp2, "bbb").unwrap();
        let mut tmp3 = NamedTempFile::new().unwrap();
        writeln!(tmp3, "ccc").unwrap();

        let paths = [
            tmp1.path().to_string_lossy().to_string(),
            tmp2.path().to_string_lossy().to_string(),
            tmp3.path().to_string_lossy().to_string(),
        ];
        let files = read_files(&paths, TEST_MAX_BYTES).unwrap();
        assert_eq!(files.len(), 3);
        assert_eq!(files[0].bytes, b"aaa\n");
        assert_eq!(files[1].bytes, b"bbb\n");
        assert_eq!(files[2].bytes, b"ccc\n");
    }

    #[test]
    fn read_missing_file_returns_error() {
        let result = read_files(&["/nonexistent/path/xyzzy.txt".to_string()], TEST_MAX_BYTES);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("failed to read metadata"));
        assert!(err.contains("xyzzy.txt"));
    }

    #[test]
    fn read_empty_file_is_ok() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_string_lossy().to_string();
        let files = read_files(&[path], TEST_MAX_BYTES).unwrap();
        assert_eq!(files.len(), 1);
        assert!(files[0].bytes.is_empty());
    }

    #[test]
    fn read_binary_file_preserves_exact_bytes() {
        let mut tmp = NamedTempFile::new().unwrap();
        let data: Vec<u8> = (0..=255).collect();
        tmp.write_all(&data).unwrap();
        let path = tmp.path().to_string_lossy().to_string();

        let files = read_files(&[path], TEST_MAX_BYTES).unwrap();
        assert_eq!(files[0].bytes, data);
    }

    #[test]
    fn path_is_stored_on_ingest_file() {
        let mut tmp = NamedTempFile::new().unwrap();
        writeln!(tmp, "content").unwrap();
        let path = tmp.path().to_string_lossy().to_string();

        let files = read_files(std::slice::from_ref(&path), TEST_MAX_BYTES).unwrap();
        assert_eq!(files[0].path.as_deref(), Some(&*path));
    }

    #[test]
    fn oversized_file_is_rejected() {
        let mut tmp = NamedTempFile::new().unwrap();
        writeln!(tmp, "more than 10 bytes").unwrap();
        let path = tmp.path().to_string_lossy().to_string();

        let result = read_files(&[path], 5);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("exceeds maximum upload size"));
    }
}
