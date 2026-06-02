//! Axum request handlers, organized by domain.
//!
//! * [`api_tokens`] — create, list, revoke API tokens (Bearer auth).
//! * [`auth`] — login, register, logout.
//! * [`billing`] — Stripe Checkout integration (P11-T2).
//! * [`ingest`] — multipart file upload → enqueue ingest job.
//! * [`search`] — hybrid search query → ranked results.
//! * [`documents`] — document detail + file download.
//! * [`jobs`] — job status lookup.

pub mod api_tokens;
pub mod auth;
pub mod billing;
pub mod documents;
pub mod ingest;
pub mod jobs;
pub mod search;
pub mod webhook;
