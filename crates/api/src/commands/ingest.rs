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
) -> anyhow::Result<()> {
    let files = read_files(&args.files)?;

    let note = args.note.clone();
    let output = pipeline
        .ingest(tenant_id, files, note, false)
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
/// # Errors
///
/// Returns an error if any individual file cannot be read (missing, permission
/// denied, etc.). The error message includes the offending path.
fn read_files(paths: &[String]) -> anyhow::Result<Vec<IngestFile>> {
    paths
        .iter()
        .map(|p| {
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

    // ── read_files tests ──────────────────────────────────────────────────

    #[test]
    fn read_single_text_file() {
        let mut tmp = NamedTempFile::new().unwrap();
        writeln!(tmp, "hello world").unwrap();
        let path = tmp.path().to_string_lossy().to_string();

        let files = read_files(&[path]).unwrap();
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
        let files = read_files(&paths).unwrap();
        assert_eq!(files.len(), 3);
        assert_eq!(files[0].bytes, b"aaa\n");
        assert_eq!(files[1].bytes, b"bbb\n");
        assert_eq!(files[2].bytes, b"ccc\n");
    }

    #[test]
    fn read_missing_file_returns_error() {
        let result = read_files(&["/nonexistent/path/xyzzy.txt".to_string()]);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("failed to read file"));
        assert!(err.contains("xyzzy.txt"));
    }

    #[test]
    fn read_empty_file_is_ok() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_string_lossy().to_string();
        let files = read_files(&[path]).unwrap();
        assert_eq!(files.len(), 1);
        assert!(files[0].bytes.is_empty());
    }

    #[test]
    fn read_binary_file_preserves_exact_bytes() {
        let mut tmp = NamedTempFile::new().unwrap();
        let data: Vec<u8> = (0..=255).collect();
        tmp.write_all(&data).unwrap();
        let path = tmp.path().to_string_lossy().to_string();

        let files = read_files(&[path]).unwrap();
        assert_eq!(files[0].bytes, data);
    }

    #[test]
    fn path_is_stored_on_ingest_file() {
        let mut tmp = NamedTempFile::new().unwrap();
        writeln!(tmp, "content").unwrap();
        let path = tmp.path().to_string_lossy().to_string();

        let files = read_files(std::slice::from_ref(&path)).unwrap();
        assert_eq!(files[0].path.as_deref(), Some(&*path));
    }
}
