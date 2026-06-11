// SPDX-License-Identifier: AGPL-3.0-or-later

//! The kind of a document/file (drives extractor routing and the `kind` column).

use crate::macros::str_enum;

str_enum! {
    /// What a document (and its member files) fundamentally *is*, determined by magic-byte
    /// detection — not a free-text LLM "category" (the design deliberately has no category
    /// field; see plan §1 and §6.5). Drives per-filetype extractor routing (plan §2).
    pub enum DocKind {
        /// PDF/DOCX/PPTX/XLSX/MD/HTML/TXT and other office/text documents.
        Document = "document",
        /// A still image (captioned by the VLM; EXIF harvested).
        Image = "image",
        /// An audio file (transcribed by whisper).
        Audio = "audio",
        /// A video file (keyframes captioned + audio transcribed).
        Video = "video",
        /// A complete identity document; PII-concentrated, defaults to `local_only` routing
        /// (plan §17).
        IdentityDocument = "identity_document",
        /// Source code (read as text; tree-sitter chunked).
        Code = "code",
        /// An archive whose contents are listed/recursed.
        Archive = "archive",
        /// An opaque/unknown binary (hashes + `strings` + the user's note).
        Binary = "binary",
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use std::str::FromStr;

    use super::*;

    #[test]
    fn wire_strings_are_locked() {
        // These strings are persisted in `documents.kind`/`files.kind`; changing one is a
        // breaking schema change, so assert them explicitly.
        assert_eq!(DocKind::Document.as_str(), "document");
        assert_eq!(DocKind::IdentityDocument.as_str(), "identity_document");
        assert_eq!(DocKind::Binary.as_str(), "binary");
        assert_eq!(DocKind::all().len(), 8);
    }

    #[test]
    fn parses_every_variant() {
        for k in DocKind::all() {
            assert_eq!(DocKind::from_str(k.as_str()).unwrap(), *k);
        }
    }
}
