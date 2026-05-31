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

pub mod auth;
pub mod blob;
pub mod chunk;
pub mod document;
pub mod embedder;
pub mod error;
pub mod extractor;
pub mod file;
pub mod hash;
pub mod job;
pub mod kind;
pub mod provider;
pub mod query;
pub mod reranker;
pub mod role;
pub mod status;
pub mod store;
pub mod tag;
pub mod tagger;
pub mod tenant;
pub mod usage;
pub mod user;
