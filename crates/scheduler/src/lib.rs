//! `kb-scheduler`: capacity-aware, multi-host model pool (plan §6) — the centerpiece.
//!
//! Phase P1 builds this crate up one ledger task at a time. **P1-T1** lands the
//! core types only:
//!
//! - [`Backend`] — one OpenAI-compatible inference server with a concurrency-limited
//!   slot pool;
//! - [`Lease`] — an RAII guard holding exactly one slot for an in-flight request;
//! - [`Pool`] — backends indexed by [`kb_core::role::Role`], built from config (§6.6);
//! - [`AcquireError`] — why a slot could not be obtained.
//!
//! The acquisition algorithm (§6.3), the health loop (§6.5), and the failover
//! wrapper (§6.4) arrive in later P1 tasks. See `local-kb-plan.md` §6 for the
//! authoritative design.

mod backend;
mod error;
mod health;
mod lease;
mod pool;

pub use backend::Backend;
pub use error::AcquireError;
pub use health::HealthLoop;
pub use lease::Lease;
pub use pool::Pool;
