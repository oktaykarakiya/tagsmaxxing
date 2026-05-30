//! `kb-llm`: OpenAI-compatible client that routes every call through the
//! [`kb_scheduler::Pool`] with failover, bounded retries, and per-backend
//! circuit-breaking (plan §6.4).
//!
//! # Architecture
//!
//! - [`LlamaClient`] owns the scheduler pool and a shared `reqwest::Client`.
//! - [`LlamaClient::chat`] and [`LlamaClient::embed`] acquire a slot, POST to
//!   the backend's OpenAI-compatible endpoint, and normalize the response into
//!   the core [`kb_core::provider`] types.
//! - On transport error / non-2xx the backend is marked unhealthy (so the pool
//!   skips it) and the circuit-breaker counter is advanced. After
//!   `circuit_threshold` consecutive failures the breaker trips; the backend
//!   enters a cooldown and is rejected even if the health loop re-marks it
//!   healthy.
//! - The [`kb_scheduler::Lease`] is always dropped at the end of each attempt,
//!   freeing the slot unconditionally (RAII — plan §6.2).
//!
//! # Embed role caveat
//!
//! The `embed` role flows through the same failover loop, but the caller MUST
//! ensure every backend serving [`kb_core::role::Role::Embed`] uses the
//! **same embedding model+version**. Mixing models silently corrupts the
//! vector space (plan §11). The routing-table enforcement lands in P9.

mod client;
mod error;

pub use client::LlamaClient;
pub use error::LlmError;
