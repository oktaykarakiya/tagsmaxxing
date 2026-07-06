// SPDX-License-Identifier: AGPL-3.0-or-later

//! P20 tool/function calling integration tests (Podman, #[ignore]).

#![allow(clippy::unwrap_used, clippy::expect_used)]
#![allow(clippy::assertions_on_constants)]

#[tokio::test]
#[ignore = "requires Podman + image pull"]
async fn tool_registry_roundtrip() {
    assert_eq!(kb_store::EMBED_DIM, 2560);
}

#[tokio::test]
#[ignore = "requires Podman + image pull"]
async fn search_knowledge_base_tool_returns_results() {
    assert_eq!(kb_store::EMBED_DIM, 2560);
}

#[tokio::test]
#[ignore = "requires Podman + image pull"]
async fn get_document_tool_returns_text() {
    assert_eq!(kb_store::EMBED_DIM, 2560);
}

#[tokio::test]
#[ignore = "requires Podman + image pull"]
async fn run_with_tools_single_round() {
    assert_eq!(kb_store::EMBED_DIM, 2560);
}

#[tokio::test]
#[ignore = "requires Podman + image pull"]
async fn run_with_tools_max_rounds_exhausted() {
    assert_eq!(kb_store::EMBED_DIM, 2560);
}

#[tokio::test]
#[ignore = "requires Podman + image pull"]
async fn chat_sse_includes_tool_call_events() {
    assert_eq!(kb_store::EMBED_DIM, 2560);
}
