// SPDX-License-Identifier: AGPL-3.0-or-later

//! Tool registry — collects tool definitions and dispatches execution (P20).

use std::sync::Arc;

use async_trait::async_trait;
use kb_core::provider::ToolDef;
use kb_store::PgStore;

use crate::retrieval::RetrievalPipeline;

/// Context available to all tool implementations during execution.
pub struct ToolContext {
    /// The tenant making the request.
    pub tenant_id: i64,
    /// The user making the request, if known.
    pub user_id: Option<i64>,
    /// Database access for document lookups.
    pub pg_store: Arc<PgStore>,
    /// Retrieval pipeline for search tools.
    pub retrieval: Option<Arc<RetrievalPipeline>>,
}

/// A tool that an LLM can invoke via function calling.
#[async_trait]
pub trait Tool: Send + Sync {
    /// The tool's definition (name, description, parameter JSON Schema).
    fn definition(&self) -> ToolDef;

    /// Execute the tool with JSON-stringified arguments.
    ///
    /// Returns a JSON string result to feed back to the LLM.
    async fn execute(&self, args: &str, ctx: &ToolContext) -> anyhow::Result<String>;
}

/// A registry of available tools, keyed by name.
pub struct ToolRegistry {
    tools: Vec<Arc<dyn Tool>>,
}

impl ToolRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self { tools: Vec::new() }
    }

    /// Register a tool.
    pub fn register(&mut self, tool: Arc<dyn Tool>) {
        self.tools.push(tool);
    }

    /// Return tool definitions for all registered tools.
    pub fn definitions(&self) -> Vec<ToolDef> {
        self.tools.iter().map(|t| t.definition()).collect()
    }

    /// Look up a tool by name.
    pub fn find(&self, name: &str) -> Option<&Arc<dyn Tool>> {
        self.tools.iter().find(|t| t.definition().name == name)
    }
}

impl Default for ToolRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    struct TestTool {
        name: String,
    }

    #[async_trait]
    impl Tool for TestTool {
        fn definition(&self) -> ToolDef {
            ToolDef {
                name: self.name.clone(),
                description: "test tool".into(),
                parameters: serde_json::json!({"type": "object", "properties": {}}),
            }
        }

        async fn execute(&self, _args: &str, _ctx: &ToolContext) -> anyhow::Result<String> {
            Ok(r#"{"ok":true}"#.into())
        }
    }

    #[test]
    fn registry_register_and_find() {
        let mut reg = ToolRegistry::new();
        let tool = Arc::new(TestTool {
            name: "test".into(),
        });
        reg.register(tool);
        assert_eq!(reg.definitions().len(), 1);
        assert!(reg.find("test").is_some());
        assert!(reg.find("nonexistent").is_none());
    }
}
