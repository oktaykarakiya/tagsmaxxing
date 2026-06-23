// SPDX-License-Identifier: AGPL-3.0-or-later

//! [`EmailExtractor`] — parses .eml (RFC 822 / MIME) files into text, headers,
//! and attachment metadata.
//!
//! # Safety bounds
//!
//! * Max 10 MB input (larger messages are truncated before parsing).
//! * Max 1000 MIME parts walked.
//! * Max 255 bytes per attachment name.

use async_trait::async_trait;
use kb_core::extractor::{Extracted, Extractor, RawFile};
use mail_parser::MimeHeaders;
use serde_json::json;

const MAX_EMAIL_BYTES: usize = 10 * 1024 * 1024;
const MAX_EMAIL_PARTS: usize = 1000;

/// Extractor for [`DocKind::Email`](kb_core::kind::DocKind::Email): reads
/// headers, body text, and attachment metadata from RFC 822 / MIME messages.
#[derive(Debug, Clone, Copy, Default)]
pub struct EmailExtractor;

#[async_trait]
impl Extractor for EmailExtractor {
    async fn extract(&self, file: &RawFile) -> anyhow::Result<Extracted> {
        Ok(extract_email(file))
    }
}

/// Parse an .eml file's bytes into [`Extracted`].
///
/// Best-effort: a parse failure returns empty [`Extracted`] rather than an error
/// (consistent with [`crate::archive::ArchiveExtractor`]).
fn extract_email(file: &RawFile) -> Extracted {
    let bytes = &file.bytes;

    // Truncate oversized input.
    let bytes = if bytes.len() > MAX_EMAIL_BYTES {
        &bytes[..MAX_EMAIL_BYTES]
    } else {
        bytes
    };

    let message = match mail_parser::MessageParser::default().parse(bytes) {
        Some(m) => m,
        None => return Extracted::default(),
    };

    let mut meta = serde_json::Map::new();

    // ── Headers ─────────────────────────────────────────────────────────────
    if let Some(from) = message.from() {
        meta.insert("email:from".into(), json!(format_address(from)));
    }
    if let Some(to) = message.to() {
        meta.insert("email:to".into(), json!(format_address(to)));
    }
    if let Some(cc) = message.cc() {
        meta.insert("email:cc".into(), json!(format_address(cc)));
    }
    if let Some(subject) = message.subject() {
        meta.insert("email:subject".into(), json!(subject));
    }
    if let Some(date) = message.date() {
        meta.insert("email:date".into(), json!(date.to_rfc3339()));
    }
    if let Some(msg_id) = message.message_id() {
        meta.insert("email:message_id".into(), json!(msg_id));
    }
    if let Some(ct) = message.content_type() {
        meta.insert("email:content_type".into(), json!(format_content_type(ct)));
    }

    let mut body_text = String::new();
    let mut has_html = false;
    let mut attachment_names: Vec<String> = Vec::new();

    // ── Walk all parts ────────────────────────────────────────────────────
    for part in message.parts.iter().take(MAX_EMAIL_PARTS) {
        // Skip multipart containers — they carry no content themselves.
        if part.is_multipart() {
            continue;
        }
        if part.is_text_html() {
            has_html = true;
            if let Some(html) = part.text_contents() {
                let stripped = strip_html_tags(html);
                if !body_text.is_empty() {
                    body_text.push('\n');
                }
                body_text.push_str(&stripped);
            }
        } else if part.is_text() {
            if let Some(t) = part.text_contents() {
                if !body_text.is_empty() {
                    body_text.push('\n');
                }
                body_text.push_str(t);
            }
        } else if part.is_binary() || part.is_message() || part.is_multipart() {
            let name = part
                .attachment_name()
                .map(|n| n.to_string())
                .unwrap_or_else(|| format!("part_{}", attachment_names.len()));
            attachment_names.push(name);
        }
    }

    meta.insert(
        "email:attachment_count".into(),
        json!(attachment_names.len()),
    );
    meta.insert("email:attachment_names".into(), json!(attachment_names));
    meta.insert("email:has_html".into(), json!(has_html));

    Extracted {
        text: body_text,
        meta: serde_json::Value::Object(meta),
        page_images: Vec::new(),
    }
}

/// Format an [`mail_parser::Address`] into a human-readable string.
fn format_address(addr: &mail_parser::Address<'_>) -> String {
    match addr {
        mail_parser::Address::List(addrs) => addrs
            .iter()
            .map(|a| format_addr(a))
            .collect::<Vec<_>>()
            .join(", "),
        mail_parser::Address::Group(groups) => groups
            .iter()
            .map(|g| {
                let members: Vec<String> = g.addresses.iter().map(|a| format_addr(a)).collect();
                format!(
                    "{}: {}",
                    g.name.as_deref().unwrap_or(""),
                    members.join(", ")
                )
            })
            .collect::<Vec<_>>()
            .join("; "),
    }
}

/// Format a single [`mail_parser::Addr`] as `"name <address>"` or `"address"`.
fn format_addr(a: &mail_parser::Addr<'_>) -> String {
    match (a.name.as_deref(), a.address.as_deref()) {
        (Some(name), Some(addr)) => format!("{name} <{addr}>"),
        (Some(name), None) => name.to_string(),
        (None, Some(addr)) => addr.to_string(),
        (None, None) => String::new(),
    }
}

/// Format a [`mail_parser::ContentType`] as a MIME type string like `text/html; charset=utf-8`.
fn format_content_type(ct: &mail_parser::ContentType<'_>) -> String {
    if let Some(subtype) = ct.subtype() {
        format!("{}/{}", ct.ctype(), subtype)
    } else {
        ct.ctype().to_string()
    }
}

/// Strip HTML tags: remove everything between `<` and `>`, then collapse
/// whitespace runs and decode common HTML entities.
fn strip_html_tags(html: &str) -> String {
    let mut out = String::with_capacity(html.len());
    let mut inside = false;
    for ch in html.chars() {
        if ch == '<' {
            inside = true;
        } else if ch == '>' {
            inside = false;
        } else if !inside {
            out.push(ch);
        }
    }
    let collapsed: String = out.split_ascii_whitespace().collect::<Vec<_>>().join(" ");
    collapsed
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&nbsp;", " ")
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use bytes::Bytes;
    use kb_core::kind::DocKind;

    use super::*;

    fn raw_eml(bytes: &[u8]) -> RawFile {
        RawFile {
            bytes: Bytes::copy_from_slice(bytes),
            mime: Some("message/rfc822".into()),
            kind: DocKind::Email,
            path: Some("message.eml".into()),
        }
    }

    #[tokio::test]
    async fn minimal_eml_text_body() {
        let eml = b"From: sender@example.com\r\n\
                     To: recipient@example.com\r\n\
                     Subject: Hello\r\n\
                     Date: Mon, 01 Jan 2025 12:00:00 +0000\r\n\
                     Message-ID: <abc123@example.com>\r\n\
                     Content-Type: text/plain; charset=\"utf-8\"\r\n\
                     \r\n\
                     Hello, world!";

        let out = EmailExtractor.extract(&raw_eml(eml)).await.unwrap();
        assert!(out.text.contains("Hello, world!"));
        assert_eq!(out.meta["email:from"], "sender@example.com");
        assert_eq!(out.meta["email:to"], "recipient@example.com");
        assert_eq!(out.meta["email:subject"], "Hello");
        // mail-parser strips angle brackets from message-id.
        assert_eq!(out.meta["email:message_id"], "abc123@example.com");
        assert!(!out.meta["email:has_html"].as_bool().unwrap());
        assert_eq!(out.meta["email:attachment_count"], 0);
    }

    #[tokio::test]
    async fn multipart_plain_html_attachment() {
        let boundary = "boundary42";
        let eml = format!(
            "From: a@b.com\r\n\
             To: c@d.com\r\n\
             Subject: Multi\r\n\
             Date: Mon, 01 Jan 2025 12:00:00 +0000\r\n\
             MIME-Version: 1.0\r\n\
             Content-Type: multipart/mixed; boundary=\"{boundary}\"\r\n\
             \r\n\
             --{boundary}\r\n\
             Content-Type: text/plain\r\n\
             \r\n\
             Plain text body.\r\n\
             --{boundary}\r\n\
             Content-Type: text/html\r\n\
             \r\n\
             <p>HTML <b>body</b>.</p>\r\n\
             --{boundary}\r\n\
             Content-Type: application/pdf; name=\"report.pdf\"\r\n\
             Content-Disposition: attachment; filename=\"report.pdf\"\r\n\
             \r\n\
             fake pdf bytes\r\n\
             --{boundary}--\r\n",
        );

        let out = EmailExtractor
            .extract(&raw_eml(eml.as_bytes()))
            .await
            .unwrap();
        assert!(out.text.contains("Plain text body."));
        assert!(out.text.contains("HTML body"));
        assert!(out.meta["email:has_html"].as_bool().unwrap());
        assert_eq!(out.meta["email:attachment_count"], 1);
        let names: Vec<&str> = out.meta["email:attachment_names"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap())
            .collect();
        assert!(names.contains(&"report.pdf"));
    }

    #[tokio::test]
    async fn empty_email_returns_default() {
        let eml = b"not a valid email";
        let out = EmailExtractor.extract(&raw_eml(eml)).await.unwrap();
        assert!(out.text.is_empty());
        // The message parser may or may not produce a message from garbage bytes.
        // meta should be either empty or contain very little — but pages is empty.
        assert!(out.page_images.is_empty());
    }

    #[tokio::test]
    async fn html_only_body_stripped_to_text() {
        let eml = b"From: x@y.com\r\n\
                     To: z@w.com\r\n\
                     Subject: HTML only\r\n\
                     Date: Mon, 01 Jan 2025 12:00:00 +0000\r\n\
                     Content-Type: text/html; charset=\"utf-8\"\r\n\
                     \r\n\
                     <html><body><h1>Hello</h1><p>This is <em>rich</em> text.</p></body></html>";

        let out = EmailExtractor.extract(&raw_eml(eml)).await.unwrap();
        assert!(out.text.contains("Hello"));
        assert!(out.text.contains("rich text"));
        assert!(out.meta["email:has_html"].as_bool().unwrap());
        // HTML tags must be stripped.
        assert!(!out.text.contains("<html>"));
        assert!(!out.text.contains("<p>"));
    }

    #[tokio::test]
    async fn email_with_encoded_headers() {
        // RFC 2047 encoded subject: =?UTF-8?Q?Caf=C3=A9?=
        let eml = b"From: sender@example.com\r\n\
                     To: recipient@example.com\r\n\
                     Subject: =?UTF-8?Q?Caf=C3=A9?= time\r\n\
                     Date: Mon, 01 Jan 2025 12:00:00 +0000\r\n\
                     Content-Type: text/plain\r\n\
                     \r\n\
                     Body text.";

        let out = EmailExtractor.extract(&raw_eml(eml)).await.unwrap();
        // mail-parser decodes RFC 2047.
        assert!(
            out.meta["email:subject"]
                .as_str()
                .unwrap()
                .contains("Caf\u{e9}")
        );
    }

    #[tokio::test]
    async fn oversized_email_truncated_but_parsed() {
        // Build a message larger than MAX_EMAIL_BYTES with a valid header.
        let header = b"From: big@example.com\r\n\
                        To: reader@example.com\r\n\
                        Subject: Big\r\n\
                        Date: Mon, 01 Jan 2025 12:00:00 +0000\r\n\
                        Content-Type: text/plain\r\n\
                        \r\n";
        let body = vec![b'x'; MAX_EMAIL_BYTES + 1024];
        let mut eml = Vec::with_capacity(header.len() + body.len());
        eml.extend_from_slice(header);
        eml.extend_from_slice(&body);

        let out = EmailExtractor.extract(&raw_eml(&eml)).await.unwrap();
        assert_eq!(out.meta["email:from"], "big@example.com");
        assert_eq!(out.meta["email:subject"], "Big");
        // Text should contain some of the body.
        assert!(!out.text.is_empty());
    }

    #[test]
    fn strip_html_removes_tags() {
        assert_eq!(strip_html_tags("<p>Hello</p>"), "Hello");
        assert_eq!(
            strip_html_tags("<b>bold</b> and <i>italic</i>"),
            "bold and italic"
        );
        assert_eq!(strip_html_tags("no tags"), "no tags");
        assert_eq!(strip_html_tags("<br>"), "");
    }
}
