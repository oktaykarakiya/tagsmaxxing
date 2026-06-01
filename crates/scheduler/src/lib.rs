//! `kb-scheduler`: capacity-aware, multi-host model pool (plan §6) — the centerpiece.
//!
//! Phase P1 builds this crate up one ledger task at a time. **P1-T1** lands the
//! core types only:
//!
//! - [`Backend`] — one OpenAI-compatible inference server with a pluggable
//!   [`Capacity`] guard (§26.2);
//! - [`Lease`] — an RAII guard holding acquired capacity for an in-flight request;
//! - [`Pool`] — backends indexed by [`kb_core::role::Role`], built from config (§6.6);
//! - [`AcquireError`] — why capacity could not be obtained.
//!
//! P9-T3 adds the pluggable [`Capacity`] enum, [`TokenBucket`], and [`Clock`]
//! abstraction (§26.2).
//!
//! The acquisition algorithm (§6.3), the health loop (§6.5), and the failover
//! wrapper (§6.4) arrive in later P1 tasks. See `local-kb-plan.md` §6 for the
//! authoritative design.

pub mod backend;
mod capacity;
mod error;
mod health;
mod lease;
mod pool;

pub use backend::{Backend, test_backend};
pub use capacity::{Capacity, CapacityPermit, Clock, TokenBucket, WallClock};
pub use error::AcquireError;
pub use health::HealthLoop;
pub use lease::Lease;
pub use pool::Pool;
