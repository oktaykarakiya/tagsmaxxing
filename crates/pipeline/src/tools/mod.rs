// SPDX-License-Identifier: AGPL-3.0-or-later

//! Tool registry and built-in tools for LLM function calling (P20).
//!
//! Provides a [`ToolRegistry`] that collects tool definitions and dispatches
//! execution, plus built-in tools for knowledge base search and document
//! retrieval.

pub mod builtin;
pub mod executor;
pub mod registry;

pub use builtin::{GetDocumentTool, ListDocumentsTool, SearchKnowledgeBaseTool};
pub use executor::run_with_tools;
pub use registry::{Tool, ToolContext, ToolRegistry};
