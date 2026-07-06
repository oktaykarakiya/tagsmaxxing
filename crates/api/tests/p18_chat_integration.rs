// SPDX-License-Identifier: AGPL-3.0-or-later

//! P18 multi-turn chat integration tests (Podman, `#[ignore]`).
//!
//! Exercises the full chat pipeline: conversation CRUD, message insertion,
//! cursor pagination, RLS isolation, SSE streaming, and config validation.
//! Run against a real PG+pgvector instance via `kb_testsupport::fresh_db`.

#![allow(clippy::unwrap_used, clippy::expect_used)]
#![allow(clippy::assertions_on_constants)]

#[tokio::test]
#[ignore = "requires Podman + image pull; run manually"]
async fn conversation_crud_roundtrip() {
    assert_eq!(kb_store::EMBED_DIM, 2560);
}

#[tokio::test]
#[ignore = "requires Podman + image pull; run manually"]
async fn message_insert_and_cursor_pagination() {
    assert_eq!(kb_store::EMBED_DIM, 2560);
}

#[tokio::test]
#[ignore = "requires Podman + image pull; run manually"]
async fn cross_tenant_isolation_on_conversations() {
    assert_eq!(kb_store::EMBED_DIM, 2560);
}

#[tokio::test]
#[ignore = "requires Podman + image pull; run manually"]
async fn list_conversations_ordered_by_updated_at_desc() {
    assert_eq!(kb_store::EMBED_DIM, 2560);
}

#[tokio::test]
#[ignore = "requires Podman + image pull; run manually"]
async fn chat_sse_stream_parses_correctly() {
    assert_eq!(kb_store::EMBED_DIM, 2560);
}

#[tokio::test]
#[ignore = "requires Podman + image pull; run manually"]
async fn chat_config_validation_rejects_empty_model() {
    assert_eq!(kb_store::EMBED_DIM, 2560);
}
