//! Candidate-diversification regression for hybrid search (BUG-SEARCH-04).
//!
//! A single document whose many near-identical chunks all sit close to the
//! query must NOT monopolize a search arm: observed in e2e, a ~200 KB
//! uniform-content file chunked into ~100 identical embeddings became a
//! semantic "attractor" that filled the entire vector top-k, so the document
//! rollup produced exactly one hit and every other relevant document was
//! starved out. Both arms now overscan (bounded) and keep only the best chunk
//! per document.
//!
//! Run with the Podman pgvector lane:
//! `DOCKER_HOST=unix://$XDG_RUNTIME_DIR/podman/podman.sock cargo test -p kb-store --test hybrid_diversification_pg -- --ignored`

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use chrono::Utc;
use kb_core::chunk::Chunk;
use kb_core::file::FileRecord;
use kb_core::hash::Sha256;
use kb_core::query::{Query, QueryFilters};
use kb_core::status::ProcessingStatus;
use kb_core::store::Store;
use kb_store::PgStore;
use sqlx::PgPool;

/// One tenant + one user, fresh DB, connected two-role store.
struct Setup {
    store: Arc<PgStore>,
    app_pool: PgPool,
    tenant: i64,
}

async fn setup() -> anyhow::Result<Setup> {
    let db = kb_testsupport::fresh_db().await?;
    let store = Arc::new(PgStore::with_roles(&db.admin_url, &db.app_url));
    store.connect().await?;
    let admin_pool = store.pool()?;
    let app_pool = store.app_pool()?;

    let tenant: i64 = sqlx::query_scalar(
        "INSERT INTO tenants (slug, name) VALUES ('divers-t', 'Diversification') RETURNING id",
    )
    .fetch_one(&admin_pool)
    .await?;
    sqlx::query(
        "INSERT INTO users (tenant_id, email, password_hash, role) \
         VALUES ($1, 'diver@t.example', 'hash', 'admin')",
    )
    .bind(tenant)
    .execute(&admin_pool)
    .await?;

    Ok(Setup {
        store,
        app_pool,
        tenant,
    })
}

/// Insert a ready document as the tenant (RLS-checked) and return its id.
async fn insert_document(s: &Setup, title: &str) -> i64 {
    let mut tx = kb_testsupport::tenant_tx(&s.app_pool, s.tenant)
        .await
        .unwrap();
    let id: i64 = sqlx::query_scalar(
        "INSERT INTO documents (tenant_id, title, kind, status) \
         VALUES ($1, $2, 'document', 'ready') RETURNING id",
    )
    .bind(s.tenant)
    .bind(title)
    .fetch_one(&mut *tx)
    .await
    .unwrap();
    tx.commit().await.unwrap();
    id
}

fn file_rec(tenant: i64, document_id: i64, label: &str) -> FileRecord {
    let mut hash = [0u8; 32];
    let bytes = label.as_bytes();
    let n = bytes.len().min(32);
    hash[..n].copy_from_slice(&bytes[..n]);
    FileRecord {
        id: 0,
        tenant_id: tenant,
        document_id,
        page_no: 1,
        page_label: Some(label.into()),
        sha256: Sha256::from_bytes(hash),
        blob_key: format!("t{tenant}/{label}"),
        path: None,
        mime: Some("text/plain".into()),
        size_bytes: Some(100),
        meta: serde_json::Value::Null,
        status: ProcessingStatus::Ready,
        ingested_at: Utc::now(),
    }
}

/// A unit-norm embedding: `1.0` at position 0 scaled, plus a small distinct
/// component so "real" documents rank strictly behind the attractor.
fn embedding(closeness: f32, distinct_pos: usize) -> Vec<f32> {
    let mut v = vec![0.0f32; kb_store::EMBED_DIM];
    v[0] = closeness;
    if distinct_pos > 0 && distinct_pos < kb_store::EMBED_DIM {
        v[distinct_pos] = (1.0 - closeness * closeness).max(0.0).sqrt();
    }
    v
}

fn chunk(
    tenant: i64,
    document_id: i64,
    file_id: i64,
    idx: i32,
    content: &str,
    emb: Vec<f32>,
) -> Chunk {
    Chunk {
        id: 0,
        tenant_id: tenant,
        document_id,
        file_id,
        page_no: Some(1),
        idx,
        content: content.into(),
        ts_offset: None,
        embedding: emb,
    }
}

/// The observed corpus poisoning, reduced: one attractor document with 100
/// identical chunks nearest the query + five distinct real documents slightly
/// farther. Pre-fix the vector arm returned five attractor chunks → one hit;
/// post-fix the hit list must contain the attractor AND the real documents.
#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn attractor_document_cannot_monopolize_vector_arm() -> anyhow::Result<()> {
    let s = setup().await?;

    // Attractor: exactly at the query vector (cosine distance 0).
    let attractor = insert_document(&s, "attractor").await;
    let af = s
        .store
        .upsert_file(&file_rec(s.tenant, attractor, "attractor-page"))
        .await?;
    let attractor_chunks: Vec<Chunk> = (0..100)
        .map(|i| {
            chunk(
                s.tenant,
                attractor,
                af,
                i,
                "uniform attractor content block",
                embedding(1.0, 0),
            )
        })
        .collect();
    s.store.upsert_chunks(af, &attractor_chunks).await?;

    // Five real documents, each one chunk, slightly farther from the query.
    let mut real_docs = Vec::new();
    for i in 0..5usize {
        let d = insert_document(&s, &format!("real-{i}")).await;
        let f = s
            .store
            .upsert_file(&file_rec(s.tenant, d, &format!("real-page-{i}")))
            .await?;
        let c = chunk(
            s.tenant,
            d,
            f,
            0,
            &format!("real document {i} topical content"),
            embedding(0.9, 10 + i),
        );
        s.store.upsert_chunks(f, &[c]).await?;
        real_docs.push(d);
    }

    // Vector-only query (empty text → keyword arm contributes nothing).
    let q = Query {
        text: String::new(),
        filters: QueryFilters::default(),
        top_k: 5,
    };
    let hits = s
        .store
        .hybrid_search(s.tenant, &q, &embedding(1.0, 0))
        .await?;

    let hit_docs: Vec<i64> = hits.iter().map(|h| h.document_id).collect();
    assert!(
        hit_docs.contains(&attractor),
        "the attractor itself is still the best match: {hit_docs:?}"
    );
    assert!(
        hits.len() >= 3,
        "vector arm must surface documents beyond the attractor \
         (got {hit_docs:?} — the attractor monopolized the candidate set)"
    );
    assert_eq!(
        hit_docs[0], attractor,
        "rank order preserved: the closest document stays first"
    );
    let distinct: std::collections::HashSet<i64> = hit_docs.iter().copied().collect();
    assert_eq!(distinct.len(), hit_docs.len(), "one hit per document");

    Ok(())
}

/// The keyword arm has the same starvation property: a document with many
/// chunks matching the term must not crowd out other matching documents.
#[tokio::test]
#[ignore = "requires Podman + image pull; run with --ignored"]
async fn many_chunk_document_cannot_monopolize_keyword_arm() -> anyhow::Result<()> {
    let s = setup().await?;

    let big = insert_document(&s, "big-keyword-doc").await;
    let bf = s
        .store
        .upsert_file(&file_rec(s.tenant, big, "big-page"))
        .await?;
    let big_chunks: Vec<Chunk> = (0..60)
        .map(|i| {
            chunk(
                s.tenant,
                big,
                bf,
                i,
                "zanzibar shared keyword block repeated",
                embedding(0.5, 1),
            )
        })
        .collect();
    s.store.upsert_chunks(bf, &big_chunks).await?;

    let other = insert_document(&s, "other-keyword-doc").await;
    let of = s
        .store
        .upsert_file(&file_rec(s.tenant, other, "other-page"))
        .await?;
    let oc = chunk(
        s.tenant,
        other,
        of,
        0,
        "zanzibar appears here too in a different document",
        embedding(0.4, 2),
    );
    s.store.upsert_chunks(of, &[oc]).await?;

    let q = Query {
        text: "zanzibar".to_string(),
        filters: QueryFilters::default(),
        top_k: 5,
    };
    // Query vector far from both docs — the keyword arm carries the signal.
    let hits = s
        .store
        .hybrid_search(s.tenant, &q, &embedding(0.0, 100))
        .await?;

    let hit_docs: Vec<i64> = hits.iter().map(|h| h.document_id).collect();
    assert!(
        hit_docs.contains(&big) && hit_docs.contains(&other),
        "both keyword-matching documents must surface, got {hit_docs:?}"
    );

    Ok(())
}
