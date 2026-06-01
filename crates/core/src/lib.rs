//! `kb-core`: the stable, I/O-free contract for the Local File Knowledge Base.
//!
//! This crate holds the domain types (mirroring the Postgres schema, plan §5) and the
//! capability traits every other crate depends on — [`extractor::Extractor`],
//! [`tagger::Tagger`], [`embedder::Embedder`], [`reranker::Reranker`], [`store::Store`],
//! [`provider::ProviderAdapter`], and [`blob::Blob`]. Depending on traits rather than
//! concretions is what keeps each component swappable and independently testable (plan §4).
//!
//! It performs no I/O and pulls in no heavy/runtime dependencies (no `tokio`, no HTTP, no
//! database driver, no image codecs) so it stays cheap to compile and easy to reason about.

// Internal helper macro for the string-enum pattern; not part of the public API.
mod macros;

pub mod audit;
pub mod auth;
pub mod blob;
pub mod budget;
pub mod capability;
pub mod chunk;
pub mod circuit_breaker;
pub mod data_class;
pub mod data_key;
pub use data_key::{DataKey, rotate_dek};
pub mod degradation;
pub mod document;
pub mod embedder;
pub mod error;
pub mod extractor;
pub mod file;
pub mod hash;
pub mod job;
pub mod kek;
pub mod kind;
pub mod pricing;
pub mod provider;
pub mod query;
pub mod quota;
pub mod reranker;
pub mod residency;
pub mod role;
pub mod routing;
pub mod secret;
pub mod session;
pub mod status;
pub mod store;
pub mod tag;
pub mod tagger;
pub mod tenant;
pub mod usage;
pub mod user;
