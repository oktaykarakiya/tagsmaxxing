#!/usr/bin/env python3
"""Serve ettin-reranker-400m-v1 behind the app's existing rerank contract.

ettin-reranker-400m-v1 is a multi-module sentence-transformers CrossEncoder
(ModernBERT base + Dense/LayerNorm/Dense scoring head). It does NOT run on
llama.cpp/GGUF like the rest of the stack, so we serve it as a small sidecar that
speaks the same wire shape the kb-llm reranker client already uses — the Rust app
is unchanged.

Wire contract (kb-llm crates/llm/src/client.rs):
  POST /rerank  (and /v1/rerank)
    req : {"model": str|null, "query": str, "documents": [str, ...]}
    resp: {"results": [{"index": int, "relevance_score": float}, ...]}  one per doc
  GET  /health  (and /v1/health) -> 200   (kb-scheduler health probe)

The scheduler probes `{base_url}/health`; base_url may or may not include `/v1`,
so both paths are served. Scores are the cross-encoder's raw logits (higher =
more relevant); the app only uses them for ordering, so no normalization is
needed.

Run:
  PORT=8083 ~/.venvs/ettin-rerank/bin/python tools/ettin-rerank/server.py
"""

from __future__ import annotations

import os

import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel
from sentence_transformers import CrossEncoder

MODEL_ID = os.environ.get("ETTIN_MODEL", "cross-encoder/ettin-reranker-400m-v1")

# CPU + eager attention (flash-attention is unavailable without a GPU). Loaded
# once at startup; the model is ~400M params so CPU scoring of a search's
# candidate set is well within latency budget.
_model = CrossEncoder(
    MODEL_ID,
    device="cpu",
    model_kwargs={"attn_implementation": "eager"},
)

app = FastAPI(title="ettin-reranker sidecar")


class RerankRequest(BaseModel):
    """Mirror of the OpenAI/llama.cpp rerank request the kb-llm client sends."""

    query: str
    documents: list[str]
    model: str | None = None


@app.get("/health")
@app.get("/v1/health")
def health() -> dict[str, str]:
    """Liveness probe answered for both `/health` and `/v1/health`."""
    return {"status": "ok"}


@app.post("/rerank")
@app.post("/v1/rerank")
def rerank(req: RerankRequest) -> dict[str, list[dict[str, float | int]]]:
    """Score each document against the query; return one result per document.

    Results are emitted in input order with their `index`; the client re-sorts by
    `index`, so order here is not load-bearing, but completeness (one per doc) is.
    """
    if not req.documents:
        return {"results": []}
    pairs = [(req.query, doc) for doc in req.documents]
    scores = _model.predict(pairs)
    results = [
        {"index": i, "relevance_score": float(score)}
        for i, score in enumerate(scores)
    ]
    return {"results": results}


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8083"))
    # Bind all interfaces by default so the reranker is reachable on the LAN
    # (override with HOST=127.0.0.1 to keep it host-local).
    host = os.environ.get("HOST", "0.0.0.0")  # noqa: S104 — intentional LAN bind
    uvicorn.run(app, host=host, port=port, log_level="info")
