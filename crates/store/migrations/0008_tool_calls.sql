-- 0008_tool_calls.sql — Tool calling support (P20)
--
-- Adds tool_calls_json to conversation_messages so the chat system can
-- store tool invocations and results alongside regular messages.

ALTER TABLE conversation_messages ADD COLUMN tool_calls_json JSONB;
