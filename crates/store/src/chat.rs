// SPDX-License-Identifier: AGPL-3.0-or-later

//! Chat conversation + message store methods (P18).
//!
//! These methods operate on the `conversations` and `conversation_messages`
//! tables via the app pool + `begin_tenant_tx` for RLS enforcement.

use anyhow::Context;
use kb_core::chat::{ChatConversation, ChatMessage};

use crate::pg_store::{PgStore, begin_tenant_tx};

impl PgStore {
    /// Create a new conversation for a tenant + user.
    ///
    /// The `model_ref` determines which LLM model is used for this conversation.
    /// Title starts as `None` — the caller should update it from the first
    /// user message.
    pub async fn create_conversation(
        &self,
        tenant_id: i64,
        user_id: i64,
        model_ref: &str,
    ) -> anyhow::Result<ChatConversation> {
        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, tenant_id).await?;

        #[derive(sqlx::FromRow)]
        struct Row {
            id: i64,
            tenant_id: i64,
            user_id: i64,
            title: Option<String>,
            model_ref: String,
            message_count: i32,
            created_at: chrono::DateTime<chrono::Utc>,
            updated_at: chrono::DateTime<chrono::Utc>,
        }

        let row: Row = sqlx::query_as(
            "INSERT INTO conversations (tenant_id, user_id, model_ref) \
             VALUES ($1, $2, $3) RETURNING *",
        )
        .bind(tenant_id)
        .bind(user_id)
        .bind(model_ref)
        .fetch_one(&mut *tx)
        .await
        .context("failed to create conversation")?;

        tx.commit().await?;

        Ok(ChatConversation {
            id: row.id,
            tenant_id: row.tenant_id,
            user_id: row.user_id,
            title: row.title,
            model_ref: row.model_ref,
            message_count: row.message_count,
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
    }

    /// Get a single conversation by id and user — scoped to tenant + user.
    /// The `user_id` parameter prevents cross-user IDOR within the same tenant.
    pub async fn get_conversation(
        &self,
        tenant_id: i64,
        user_id: i64,
        conv_id: i64,
    ) -> anyhow::Result<Option<ChatConversation>> {
        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, tenant_id).await?;

        #[derive(sqlx::FromRow)]
        struct Row {
            id: i64,
            tenant_id: i64,
            user_id: i64,
            title: Option<String>,
            model_ref: String,
            message_count: i32,
            created_at: chrono::DateTime<chrono::Utc>,
            updated_at: chrono::DateTime<chrono::Utc>,
        }

        let row: Option<Row> =
            sqlx::query_as("SELECT * FROM conversations WHERE id = $1 AND user_id = $2")
                .bind(conv_id)
                .bind(user_id)
                .fetch_optional(&mut *tx)
                .await
                .context("failed to fetch conversation")?;

        tx.commit().await?;

        Ok(row.map(|r| ChatConversation {
            id: r.id,
            tenant_id: r.tenant_id,
            user_id: r.user_id,
            title: r.title,
            model_ref: r.model_ref,
            message_count: r.message_count,
            created_at: r.created_at,
            updated_at: r.updated_at,
        }))
    }

    /// List conversations for a tenant + user, newest first.
    pub async fn list_conversations(
        &self,
        tenant_id: i64,
        user_id: i64,
        limit: i64,
    ) -> anyhow::Result<Vec<ChatConversation>> {
        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, tenant_id).await?;

        #[derive(sqlx::FromRow)]
        struct Row {
            id: i64,
            tenant_id: i64,
            user_id: i64,
            title: Option<String>,
            model_ref: String,
            message_count: i32,
            created_at: chrono::DateTime<chrono::Utc>,
            updated_at: chrono::DateTime<chrono::Utc>,
        }

        let rows: Vec<Row> = sqlx::query_as(
            "SELECT * FROM conversations \
             WHERE user_id = $1 \
             ORDER BY updated_at DESC LIMIT $2",
        )
        .bind(user_id)
        .bind(limit)
        .fetch_all(&mut *tx)
        .await
        .context("failed to list conversations")?;

        tx.commit().await?;

        Ok(rows
            .into_iter()
            .map(|r| ChatConversation {
                id: r.id,
                tenant_id: r.tenant_id,
                user_id: r.user_id,
                title: r.title,
                model_ref: r.model_ref,
                message_count: r.message_count,
                created_at: r.created_at,
                updated_at: r.updated_at,
            })
            .collect())
    }

    /// Update the title of a conversation — scoped to tenant + user.
    pub async fn update_conversation_title(
        &self,
        tenant_id: i64,
        user_id: i64,
        conv_id: i64,
        title: &str,
    ) -> anyhow::Result<()> {
        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, tenant_id).await?;

        sqlx::query(
            "UPDATE conversations SET title = $4 WHERE id = $1 AND tenant_id = $2 AND user_id = $3",
        )
        .bind(conv_id)
        .bind(tenant_id)
        .bind(user_id)
        .bind(title)
        .execute(&mut *tx)
        .await
        .context("failed to update conversation title")?;

        tx.commit().await?;
        Ok(())
    }

    /// Delete a conversation and all its messages (CASCADE) — scoped to tenant + user.
    pub async fn delete_conversation(
        &self,
        tenant_id: i64,
        user_id: i64,
        conv_id: i64,
    ) -> anyhow::Result<()> {
        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, tenant_id).await?;

        sqlx::query("DELETE FROM conversations WHERE id = $1 AND tenant_id = $2 AND user_id = $3")
            .bind(conv_id)
            .bind(tenant_id)
            .bind(user_id)
            .execute(&mut *tx)
            .await
            .context("failed to delete conversation")?;

        tx.commit().await?;
        Ok(())
    }

    /// Insert a new message and bump the conversation's message_count + updated_at.
    ///
    /// The `user_id` parameter is checked against the conversation's owner to
    /// prevent cross-user IDOR within the same tenant.
    ///
    /// Returns the new message row.
    #[allow(clippy::too_many_arguments)]
    pub async fn insert_message(
        &self,
        tenant_id: i64,
        user_id: i64,
        conv_id: i64,
        role: &str,
        content: &str,
        tokens_in: Option<i32>,
        tokens_out: Option<i32>,
        search_results_json: Option<&serde_json::Value>,
    ) -> anyhow::Result<ChatMessage> {
        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, tenant_id).await?;

        #[derive(sqlx::FromRow)]
        struct Row {
            id: i64,
            tenant_id: i64,
            conversation_id: i64,
            role: String,
            content: String,
            tokens_in: Option<i32>,
            tokens_out: Option<i32>,
            search_results_json: Option<serde_json::Value>,
            created_at: chrono::DateTime<chrono::Utc>,
        }

        let row: Row = sqlx::query_as(
            "INSERT INTO conversation_messages \
             (tenant_id, conversation_id, role, content, tokens_in, tokens_out, search_results_json) \
             SELECT $1, $2, $3, $4, $5, $6, $7 \
             FROM conversations WHERE id = $2 AND user_id = $8 \
             RETURNING conversation_messages.*",
        )
        .bind(tenant_id)
        .bind(conv_id)
        .bind(role)
        .bind(content)
        .bind(tokens_in)
        .bind(tokens_out)
        .bind(search_results_json)
        .bind(user_id)
        .fetch_one(&mut *tx)
        .await
        .context("failed to insert message")?;

        // Bump conversation metadata.
        sqlx::query(
            "UPDATE conversations SET message_count = message_count + 1, updated_at = now() \
             WHERE id = $1",
        )
        .bind(conv_id)
        .execute(&mut *tx)
        .await
        .context("failed to bump conversation metadata")?;

        tx.commit().await?;

        Ok(ChatMessage {
            id: row.id,
            tenant_id: row.tenant_id,
            conversation_id: row.conversation_id,
            role: row.role,
            content: row.content,
            tokens_in: row.tokens_in,
            tokens_out: row.tokens_out,
            search_results_json: row.search_results_json,
            created_at: row.created_at,
        })
    }

    /// Get messages for a conversation, cursor-based (before a given message id).
    ///
    /// The `user_id` parameter is verified against the conversation's owner via
    /// a subquery — if the conversation doesn't belong to this user, the result
    /// set is empty (not an error, for consistent UX with RLS patterns).
    ///
    /// Returns up to `limit` messages with `id < before_id`, ordered by
    /// `created_at ASC` (chronological). Pass `before_id = i64::MAX` for the
    /// most recent messages. Used for scrollback/infinite scroll.
    pub async fn get_messages(
        &self,
        tenant_id: i64,
        user_id: i64,
        conv_id: i64,
        before_id: i64,
        limit: i64,
    ) -> anyhow::Result<Vec<ChatMessage>> {
        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, tenant_id).await?;

        #[derive(sqlx::FromRow)]
        struct Row {
            id: i64,
            tenant_id: i64,
            conversation_id: i64,
            role: String,
            content: String,
            tokens_in: Option<i32>,
            tokens_out: Option<i32>,
            search_results_json: Option<serde_json::Value>,
            created_at: chrono::DateTime<chrono::Utc>,
        }

        let rows: Vec<Row> = sqlx::query_as(
            "SELECT cm.* FROM conversation_messages cm \
             JOIN conversations c ON cm.conversation_id = c.id \
             WHERE cm.conversation_id = $1 AND c.user_id = $2 AND cm.id < $3 \
             ORDER BY cm.created_at ASC LIMIT $4",
        )
        .bind(conv_id)
        .bind(user_id)
        .bind(before_id)
        .bind(limit)
        .fetch_all(&mut *tx)
        .await
        .context("failed to fetch messages")?;

        tx.commit().await?;

        Ok(rows
            .into_iter()
            .map(|r| ChatMessage {
                id: r.id,
                tenant_id: r.tenant_id,
                conversation_id: r.conversation_id,
                role: r.role,
                content: r.content,
                tokens_in: r.tokens_in,
                tokens_out: r.tokens_out,
                search_results_json: r.search_results_json,
                created_at: r.created_at,
            })
            .collect())
    }

    /// Get the most recent N messages for a conversation, for LLM context window.
    ///
    /// The `user_id` parameter is verified against the conversation's owner.
    /// Ordered by `created_at ASC` (chronological), limited to `limit` most recent.
    pub async fn get_recent_messages(
        &self,
        tenant_id: i64,
        user_id: i64,
        conv_id: i64,
        limit: i64,
    ) -> anyhow::Result<Vec<ChatMessage>> {
        let pool = self.app_pool()?;
        let mut tx = begin_tenant_tx(&pool, tenant_id).await?;

        #[derive(sqlx::FromRow)]
        struct Row {
            id: i64,
            tenant_id: i64,
            conversation_id: i64,
            role: String,
            content: String,
            tokens_in: Option<i32>,
            tokens_out: Option<i32>,
            search_results_json: Option<serde_json::Value>,
            created_at: chrono::DateTime<chrono::Utc>,
        }

        let rows: Vec<Row> = sqlx::query_as(
            "SELECT cm.* FROM (\
               SELECT cm.* FROM conversation_messages cm \
               JOIN conversations c ON cm.conversation_id = c.id \
               WHERE cm.conversation_id = $1 AND c.user_id = $2 \
               ORDER BY cm.created_at DESC LIMIT $3 \
             ) sub ORDER BY created_at ASC",
        )
        .bind(conv_id)
        .bind(user_id)
        .bind(limit)
        .fetch_all(&mut *tx)
        .await
        .context("failed to fetch recent messages")?;

        tx.commit().await?;

        Ok(rows
            .into_iter()
            .map(|r| ChatMessage {
                id: r.id,
                tenant_id: r.tenant_id,
                conversation_id: r.conversation_id,
                role: r.role,
                content: r.content,
                tokens_in: r.tokens_in,
                tokens_out: r.tokens_out,
                search_results_json: r.search_results_json,
                created_at: r.created_at,
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use chrono::Utc;
    use kb_core::chat::ChatMessage;

    #[test]
    fn chat_message_roundtrip_via_domain_types() {
        let msg = ChatMessage {
            id: 1,
            tenant_id: 2,
            conversation_id: 3,
            role: "user".into(),
            content: "hello".into(),
            tokens_in: Some(10),
            tokens_out: None,
            search_results_json: None,
            created_at: Utc::now(),
        };
        assert_eq!(msg.role, "user");
        assert_eq!(msg.content, "hello");
    }

    #[test]
    fn conversation_defaults() {
        let now = Utc::now();
        let c = ChatConversation {
            id: 0,
            tenant_id: 1,
            user_id: 1,
            title: None,
            model_ref: "local/test".into(),
            message_count: 0,
            created_at: now,
            updated_at: now,
        };
        assert_eq!(c.message_count, 0);
        assert!(c.title.is_none());
    }
}
