"""Catalog (step 2) — semantic retrieval quality (DeepSeek LLM-as-judge).

Judges the *quality of search* — does hybrid retrieval (embeddings + rerank)
actually surface the right document for a meaning-based query? This is a
different axis from everything judged today: ``test_tagging`` judges **tagging**
quality (tags/summary/title), and ``test_search`` asserts retrieval only
**structurally** (marker round-trips, deterministic ordering by injected markers).
Nothing currently judges whether retrieval is *semantically* good — that gap is
this file.

These carry ``@pytest.mark.judge`` and run only in the quality lane
(``./test/run.sh quality``). The judge grades relevance, not exact text. A failing
verdict means the retrieval quality misses the intended contract and is left
failing.
"""

from __future__ import annotations

import pytest

from lib import config, flows

pytestmark = [pytest.mark.search, pytest.mark.judge]


# ── Local helpers ────────────────────────────────────────────────────────────
def _index_of(hits: list[dict], doc_id: int) -> int | None:
    """Return the rank (0-based position) of ``doc_id`` in ``hits``, or ``None``."""
    for i, h in enumerate(hits):
        if h.get("document_id") == doc_id:
            return i
    return None


def _retrieved_text(api, hit: dict) -> str:
    """Assemble a judge-friendly description of what retrieval surfaced for a hit.

    Combines the document's model-generated title and summary (the system's own
    representation of the doc) with the matching chunk snippet, so the judge sees
    exactly what the retriever returned rather than the raw ingested text.
    """
    parts: list[str] = []
    doc = api.get_document(hit["document_id"])
    if doc.get("title"):
        parts.append(f"Title: {doc['title']}")
    if doc.get("summary"):
        parts.append(f"Summary: {doc['summary']}")
    if hit.get("snippet"):
        parts.append(f"Matched snippet: {hit['snippet']}")
    return "\n".join(parts) or "(retrieval returned a document with no extractable text)"


@pytest.mark.judge
def test_paraphrased_query_retrieves_relevant_doc(api, judge):
    """A semantically-paraphrased query (no shared keywords) finds the right doc.

    Ingest a document about a specific topic, then search with a paraphrase that
    shares *meaning* but few/no surface words with the document. Take the top hit's
    document detail and have the judge rule whether that retrieved document is a
    relevant answer to the paraphrased query — testing that embedding retrieval
    captures meaning, not just lexical overlap.
    """
    tenant, email, password = config.tenant_slug(), config.admin_email(), config.admin_password()
    api.login(tenant, email, password)

    # A document about black holes — note it never uses the words in the query.
    target_content = (
        "When a sufficiently massive star exhausts its nuclear fuel, its core "
        "collapses under its own gravity, forming a region of spacetime so dense "
        "that nothing — not even light — can escape once it crosses the boundary "
        "known as the event horizon. Astronomers detect these collapsed remnants "
        "indirectly, by observing the intense X-ray radiation emitted as "
        "surrounding gas spirals inward and heats up, and by tracking the orbits "
        "of nearby stars whipping around an unseen, enormously heavy companion."
    )
    _marker, target_doc = flows.ingest_and_wait(api, target_content)

    # A paraphrase that shares meaning but virtually no surface words with the doc.
    query = (
        "How do scientists find invisible cosmic objects whose pull is so strong "
        "that nothing can get away from them?"
    )
    result = api.search(query)
    hits = result.get("hits", [])
    assert hits, (
        f"paraphrased query {query!r} returned no hits; embedding retrieval failed "
        "to surface any document for a meaning-based query"
    )

    # Contract: the paraphrase should retrieve the relevant doc despite zero
    # keyword overlap (recall via embeddings, not lexical match).
    assert _index_of(hits, target_doc["id"]) is not None, (
        f"the relevant document {target_doc['id']} was not retrieved by the "
        f"paraphrased query {query!r}; embedding retrieval missed it entirely"
    )

    # Quality: the top-ranked result must be a relevant answer to the query.
    top = hits[0]
    v = judge.verdict(
        rubric=(
            "A search engine returned the document described in OUTPUT in response "
            "to the query in CONTEXT. The query is a paraphrase that uses different "
            "words than the document. Decide whether the returned document is a "
            "RELEVANT answer to the query — i.e. it is genuinely about the same "
            "subject the query is asking about. PASS if the document's subject "
            "matches the meaning of the query even though the wording differs. "
            "FAIL if the document is about an unrelated subject."
        ),
        output=_retrieved_text(api, top),
        context=f"Search query (paraphrase): {query}",
    )
    assert v.passed, v.reason


@pytest.mark.judge
def test_conceptual_query_ranks_relevant_doc_first(api, judge):
    """For a conceptual query over mixed documents, the on-topic doc ranks first.

    Ingest several documents on clearly different topics (one strongly relevant to
    a concept, the others off-topic), then issue a conceptual query about that one
    topic. Have the judge confirm the **top-ranked** result is the topically
    relevant document — testing rerank/fusion ordering quality, not just recall.
    """
    tenant, email, password = config.tenant_slug(), config.admin_email(), config.admin_password()
    api.login(tenant, email, password)

    # On-topic: coral-reef ecosystems.
    _m_reef, reef_doc = flows.ingest_and_wait(
        api,
        "Coral reefs are among the most biologically diverse ecosystems on the "
        "planet. Built over millennia by colonies of tiny animals called polyps, "
        "which secrete calcium carbonate skeletons, these underwater structures "
        "shelter fish, crustaceans, and mollusks. Rising sea temperatures cause "
        "the symbiotic algae living in the coral tissue to be expelled, a "
        "phenomenon called bleaching that can kill the reef if prolonged.",
    )
    # Off-topic distractor 1: medieval castles.
    _m_castle, castle_doc = flows.ingest_and_wait(
        api,
        "Medieval European castles evolved from simple wooden motte-and-bailey "
        "forts into massive stone fortresses. Thick curtain walls, crenellated "
        "battlements, arrow slits, and a defensible keep allowed a small garrison "
        "to withstand prolonged sieges, while a moat and drawbridge controlled "
        "access to the gatehouse.",
    )
    # Off-topic distractor 2: jazz history.
    _m_jazz, jazz_doc = flows.ingest_and_wait(
        api,
        "Jazz emerged in early twentieth-century New Orleans, blending African "
        "rhythmic traditions, blues, and ragtime. Improvisation is its defining "
        "feature, with soloists spontaneously inventing melodic lines over a "
        "repeating chord progression; bebop later pushed harmonic complexity and "
        "tempo to new extremes.",
    )

    query = (
        "underwater marine habitats where colonies of small organisms support "
        "diverse sea life and are threatened by warming oceans"
    )
    hits = api.search(query, limit=10).get("hits", [])
    assert hits, f"conceptual query {query!r} returned no hits"

    # Contract (recall + ordering): the on-topic doc must be retrieved, and it
    # must rank ahead of every off-topic doc that the query also surfaced.
    pos_reef = _index_of(hits, reef_doc["id"])
    assert pos_reef is not None, (
        f"the on-topic coral-reef document {reef_doc['id']} was not retrieved for "
        f"the conceptual query {query!r}"
    )
    for label, off_doc in (("medieval-castles", castle_doc), ("jazz-history", jazz_doc)):
        pos_off = _index_of(hits, off_doc["id"])
        if pos_off is not None:
            assert pos_reef < pos_off, (
                f"off-topic {label} doc (rank {pos_off}) ranked at or above the "
                f"on-topic coral-reef doc (rank {pos_reef}) for a coral-reef "
                "concept query; rerank/fusion ordering is wrong"
            )

    # Quality: the judge must confirm the actual top-ranked result is on-topic.
    top = hits[0]
    v = judge.verdict(
        rubric=(
            "A search engine returned the document described in OUTPUT as its "
            "TOP-RANKED result for the conceptual query in CONTEXT. The query is "
            "about coral reefs / underwater marine ecosystems. PASS only if the "
            "top result is genuinely about coral reefs, marine/ocean ecosystems, "
            "or marine life. FAIL if the top result is about an unrelated subject "
            "such as castles, music, or anything not concerning the sea."
        ),
        output=_retrieved_text(api, top),
        context=f"Conceptual search query: {query}",
    )
    assert v.passed, v.reason


@pytest.mark.judge
def test_multilingual_query_retrieves_same_topic_doc(api, judge):
    """A query in one language retrieves a same-topic document written in another.

    bge-m3 is multilingual: ingest a document written in one language about a
    topic, then search for that topic in a *different* language. Have the judge
    rule whether the top hit is about the same topic — testing cross-lingual
    semantic retrieval, a capability no other test exercises.
    """
    tenant, email, password = config.tenant_slug(), config.admin_email(), config.admin_password()
    api.login(tenant, email, password)

    # Document written in French, about volcanoes. The only English in it is the
    # appended marker token, which shares no topical words with the query below.
    french_volcano = (
        "Les volcans se forment lorsque le magma, une roche en fusion provenant "
        "des profondeurs de la Terre, remonte vers la surface. Lorsque la "
        "pression accumulée sous la croûte terrestre devient trop importante, une "
        "éruption se déclenche, projetant de la lave, des cendres et des gaz dans "
        "l'atmosphère. Certaines éruptions construisent lentement des montagnes "
        "coniques, tandis que d'autres sont explosives et dévastatrices."
    )
    _marker, target_doc = flows.ingest_and_wait(api, french_volcano)

    # English query for the same topic — no shared topical vocabulary with the
    # French document, so retrieval must rely on cross-lingual semantics.
    query = "how molten rock erupts from mountains and spreads lava and ash"
    result = api.search(query)
    hits = result.get("hits", [])
    assert hits, (
        f"cross-lingual query {query!r} returned no hits; multilingual embedding "
        "retrieval surfaced nothing"
    )

    # Contract: the same-topic French document must be retrieved by the English
    # query (cross-lingual recall).
    assert _index_of(hits, target_doc["id"]) is not None, (
        f"the same-topic French document {target_doc['id']} was not retrieved by "
        f"the English query {query!r}; cross-lingual retrieval missed it"
    )

    # Quality: the judge confirms the top hit is about the same topic.
    top = hits[0]
    v = judge.verdict(
        rubric=(
            "A search engine returned the document described in OUTPUT in response "
            "to the English query in CONTEXT. The query is about volcanoes / "
            "volcanic eruptions / lava. The document may be written in another "
            "language (e.g. French). PASS if the returned document is about the "
            "same topic — volcanoes, eruptions, magma, or lava — regardless of its "
            "language. FAIL if it is about an unrelated subject."
        ),
        output=_retrieved_text(api, top),
        context=f"English search query: {query}",
    )
    assert v.passed, v.reason
