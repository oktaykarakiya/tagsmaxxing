//! `kb-api`: the axum HTTP API, clap CLI (`ingest`/`query`/`serve`), folder-watch, and the
//! server-rendered web UI (Askama + HTMX + Tailwind).
//!
//! The server listens on **port 9999** by default (overridable via config/env).
//!
//! Skeleton crate. Implemented in phase P6 per `BUILD_LEDGER.toml`; see plan §12
//! (web UI) and §15 (admin/observability).
