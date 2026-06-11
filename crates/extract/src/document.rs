// SPDX-License-Identifier: AGPL-3.0-or-later

//! [`DocumentExtractor`] — the composite extractor for [`DocKind::Document`].
//!
//! `DocKind::Document` spans both plain-text formats (`text/plain`, markdown,
//! HTML, CSV, source code, JSON) and rich binary documents (PDF, legacy MS
//! Office, OpenDocument). The two need different engines, so this extractor
//! dispatches on the detected MIME:
//!
//! - **Plain text** → [`TextExtractor`] (native Rust, no network round-trip).
//! - **Everything else** (PDF, `.doc`, ODF, …) → [`TikaExtractor`] (Apache Tika
//!   sidecar), which reads the binary document's text.
//!
//! This keeps fast-path text extraction native while making PDFs and other rich
//! documents searchable without mislabelling them as archives.

use async_trait::async_trait;
use kb_core::extractor::{Extracted, Extractor, RawFile};

use crate::text::TextExtractor;
use crate::tika::TikaExtractor;

/// Composite extractor for [`DocKind::Document`](kb_core::kind::DocKind::Document):
/// native text extraction for plain-text MIME types, Apache Tika for rich
/// binary documents (PDF, MS Office, OpenDocument).
pub struct DocumentExtractor {
    text: TextExtractor,
    tika: TikaExtractor,
}

impl DocumentExtractor {
    /// Build a document extractor that falls back to `tika` for non-plain-text
    /// document formats.
    pub fn new(tika: TikaExtractor) -> Self {
        Self {
            text: TextExtractor,
            tika,
        }
    }

    /// Whether `mime` is a plain-text format handled natively (vs a rich binary
    /// document that needs Tika). `text/*` and `application/json` are native;
    /// `application/pdf`, `application/msword`, OpenDocument, etc. go to Tika.
    fn is_plain_text(mime: Option<&str>) -> bool {
        matches!(mime, Some(m) if m.starts_with("text/") || m == "application/json")
    }
}

#[async_trait]
impl Extractor for DocumentExtractor {
    async fn extract(&self, file: &RawFile) -> anyhow::Result<Extracted> {
        if Self::is_plain_text(file.mime.as_deref()) {
            self.text.extract(file).await
        } else {
            self.tika.extract(file).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_text_mimes_route_native() {
        assert!(DocumentExtractor::is_plain_text(Some("text/plain")));
        assert!(DocumentExtractor::is_plain_text(Some("text/markdown")));
        assert!(DocumentExtractor::is_plain_text(Some("text/csv")));
        assert!(DocumentExtractor::is_plain_text(Some("text/x-python")));
        assert!(DocumentExtractor::is_plain_text(Some("application/json")));
    }

    #[test]
    fn rich_document_mimes_route_to_tika() {
        assert!(!DocumentExtractor::is_plain_text(Some("application/pdf")));
        assert!(!DocumentExtractor::is_plain_text(Some(
            "application/msword"
        )));
        assert!(!DocumentExtractor::is_plain_text(Some(
            "application/vnd.oasis.opendocument.text"
        )));
        assert!(!DocumentExtractor::is_plain_text(None));
    }
}
