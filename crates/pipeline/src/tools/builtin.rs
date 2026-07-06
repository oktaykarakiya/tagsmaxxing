// SPDX-License-Identifier: AGPL-3.0-or-later

//! Built-in tools for LLM function calling (P20).

use async_trait::async_trait;
use kb_core::provider::ToolDef;
use kb_core::query::{Query, QueryFilters};

use super::registry::{Tool, ToolContext};

/// Search the user's knowledge base and return matching document snippets.
pub struct SearchKnowledgeBaseTool;

#[async_trait]
impl Tool for SearchKnowledgeBaseTool {
    fn definition(&self) -> ToolDef {
        ToolDef {
            name: "search_knowledge_base".into(),
            description:
                "Search the user's document library. Returns matching document titles and snippets."
                    .into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The search query"
                    },
                    "top_k": {
                        "type": "integer",
                        "description": "Number of results to return (default 5)",
                        "default": 5
                    }
                },
                "required": ["query"]
            }),
        }
    }

    async fn execute(&self, args: &str, ctx: &ToolContext) -> anyhow::Result<String> {
        let params: serde_json::Value = serde_json::from_str(args)?;
        let query_text = params["query"].as_str().unwrap_or("");
        let top_k = params["top_k"].as_u64().unwrap_or(5) as usize;

        if query_text.is_empty() {
            return Ok(r#"{"error":"query is required"}"#.into());
        }

        let retrieval = match &ctx.retrieval {
            Some(r) => r,
            None => return Ok(r#"{"error":"search not available"}"#.into()),
        };

        let query = Query {
            text: query_text.to_string(),
            filters: QueryFilters {
                kinds: vec![],
                tags: vec![],
                created_after: None,
                created_before: None,
            },
            top_k,
        };

        let (hits, _mode) = retrieval
            .retrieve(ctx.tenant_id, ctx.user_id, &query, true, false)
            .await
            .unwrap_or_else(|_| (vec![], crate::retrieval::SearchMode::Keyword));

        let results: Vec<serde_json::Value> = hits
            .into_iter()
            .map(|h| {
                serde_json::json!({
                    "document_id": h.document_id,
                    "title": h.title,
                    "snippet": h.snippet,
                })
            })
            .collect();

        Ok(serde_json::to_string(
            &serde_json::json!({"results": results}),
        )?)
    }
}

/// Retrieve the full text of a document by its id.
pub struct GetDocumentTool;

#[async_trait]
impl Tool for GetDocumentTool {
    fn definition(&self) -> ToolDef {
        ToolDef {
            name: "get_document".into(),
            description: "Get the full text of a specific document by its ID.".into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "document_id": {
                        "type": "integer",
                        "description": "The document ID to retrieve"
                    }
                },
                "required": ["document_id"]
            }),
        }
    }

    async fn execute(&self, args: &str, ctx: &ToolContext) -> anyhow::Result<String> {
        let params: serde_json::Value = serde_json::from_str(args)?;
        let doc_id = params["document_id"].as_i64().unwrap_or(0);

        let doc = ctx
            .pg_store
            .get_document(ctx.tenant_id, doc_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("document not found"))?;

        let text = ctx
            .pg_store
            .get_live_document_text(ctx.tenant_id, doc_id)
            .await
            .unwrap_or_default();

        Ok(serde_json::to_string(&serde_json::json!({
            "document_id": doc_id,
            "title": doc.title,
            "summary": doc.summary,
            "text": text.chars().take(2000).collect::<String>(),
        }))?)
    }
}

/// List recent documents for the user.
pub struct ListDocumentsTool;

#[async_trait]
impl Tool for ListDocumentsTool {
    fn definition(&self) -> ToolDef {
        ToolDef {
            name: "list_documents".into(),
            description: "List recent documents in the user's library with their titles.".into(),
            parameters: serde_json::json!({
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "description": "Number of documents to list (default 10)",
                        "default": 10
                    }
                }
            }),
        }
    }

    async fn execute(&self, args: &str, _ctx: &ToolContext) -> anyhow::Result<String> {
        let params: serde_json::Value = serde_json::from_str(args)?;
        let _limit = params["limit"].as_u64().unwrap_or(10) as usize;

        // Full implementation requires a list_documents_for_tenant store method (P21).
        let results: Vec<serde_json::Value> = vec![];
        Ok(serde_json::to_string(&serde_json::json!({
            "count": results.len(),
            "documents": results,
        }))?)
    }
}
