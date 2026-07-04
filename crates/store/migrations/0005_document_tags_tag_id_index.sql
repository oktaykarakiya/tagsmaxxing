-- ═══════════════════════════════════════════════════════════════════
-- 0005_document_tags_tag_id_index.sql — tag merge acceleration
-- ═══════════════════════════════════════════════════════════════════
--
-- admin_merge_tags filters document_tags by tag_id alone in three
-- separate queries (INSERT…SELECT, COUNT, DELETE).  The PK index on
-- (document_id, tag_id) cannot serve these efficiently because
-- tag_id is the trailing column.

CREATE INDEX IF NOT EXISTS document_tags_tag_id_idx
    ON document_tags (tag_id);
