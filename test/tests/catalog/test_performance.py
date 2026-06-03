"""Catalog (step-2 stubs) — performance / load.

Measures the system under concurrency: parallel uploads, parallel searches,
ingest throughput, search latency under load, and burst drain. These run in
their own lane (`./test/run.sh perf`) and are excluded from the default and
quality runs because they are slow and their thresholds are environment-specific.

Implementation guidance (step 2): drive concurrency with
``concurrent.futures.ThreadPoolExecutor`` and a SEPARATE ``ApiClient`` per worker
thread (httpx clients are not safe to share across threads). Prefer asserting
*sanity* (all requests succeed, the queue drains, throughput above a floor) and
read any numeric thresholds from env vars with lenient defaults, rather than
hard-coding machine-specific latency numbers.

List with `pytest --collect-only -m perf`.
"""

from __future__ import annotations

import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

from lib import config, flows
from lib.api_client import ApiClient

pytestmark = pytest.mark.perf

# ── Bump the per-request HTTP timeout for this module ────────────────────────
# The default 60 s is fine for single-request smoke tests, but performance
# tests fire concurrent uploads that queue up behind the single-threaded LLM
# worker. Each upload runs the full pipeline (extract → tag → embed → store)
# through real models; five concurrent uploads can need 5×30–90 s wall-clock.
# Setting the env var here ensures every ApiClient constructed in this module
# picks up the longer timeout (config.http_timeout() reads the env at call time).
_PERF_HTTP_TIMEOUT = os.environ.get("PERF_HTTP_TIMEOUT", "600")
os.environ["HTTP_TIMEOUT"] = _PERF_HTTP_TIMEOUT

# ── Configurable thresholds (env vars with lenient defaults) ─────────────────
# These defaults are deliberately lenient because the real models (Qwen, bge-m3,
# bge-reranker) run on local llama.cpp and ingest (extract → tag → embed → store)
# can take 30–90+ seconds per document depending on hardware.


def _env_int(key: str, default: int) -> int:
    return int(os.environ.get(key, str(default)))


def _env_float(key: str, default: float) -> float:
    return float(os.environ.get(key, str(default)))


N_UPLOADS = _env_int("PERF_CONCURRENT_UPLOADS", 5)
N_SEARCHES = _env_int("PERF_CONCURRENT_SEARCHES", 10)
INGEST_COUNT = _env_int("PERF_INGEST_COUNT", 5)
INGEST_FLOOR_DOCS_PER_SEC = _env_float("PERF_INGEST_FLOOR_DOCS_PER_SEC", 0.005)
SEARCH_LATENCY_P95_CEILING_S = _env_float("PERF_SEARCH_LATENCY_P95_CEILING_S", 60.0)
LARGE_DOC_MB = _env_int("PERF_LARGE_DOC_MB", 5)
LARGE_DOC_TIMEOUT_S = _env_float("PERF_LARGE_DOC_TIMEOUT_S", 600.0)
BURST_COUNT = _env_int("PERF_BURST_COUNT", 10)
SEARCH_QPS = _env_int("PERF_SEARCH_QPS", 1)
SEARCH_QPS_DURATION_S = _env_int("PERF_SEARCH_QPS_DURATION_S", 10)

# ── Shared content ───────────────────────────────────────────────────────────
_SAMPLE = (
    "Performance benchmark document for the Local File Knowledge Base. "
    "This document covers throughput, latency, and concurrency measurements. "
    "Unique marker: {marker}."
)


# ── Helpers ──────────────────────────────────────────────────────────────────


def _new_client() -> ApiClient:
    """Create a fresh ``ApiClient`` logged in as the bootstrap admin.

    Each thread/worker must use its own client because httpx clients are not
    safe to share across threads.
    """
    client = ApiClient()
    client.login(config.tenant_slug(), config.admin_email(), config.admin_password())
    return client


# ═══════════════════════════════════════════════════════════════════════════════
# Tests
# ═══════════════════════════════════════════════════════════════════════════════


@pytest.mark.perf
def test_concurrent_uploads(api):
    """Fire N concurrent uploads; every upload returns a document with tags + summary."""
    api.login(config.tenant_slug(), config.admin_email(), config.admin_password())

    markers = [flows.unique_marker() for _ in range(N_UPLOADS)]

    def _upload(marker: str) -> tuple[str, dict[str, object] | Exception]:
        c = _new_client()
        try:
            _, doc = flows.ingest_and_wait(c, _SAMPLE.format(marker=marker))
            return marker, doc
        except Exception as exc:
            return marker, exc
        finally:
            c.close()

    results: dict[str, object] = {}
    with ThreadPoolExecutor(max_workers=N_UPLOADS) as pool:
        futures = {pool.submit(_upload, m): m for m in markers}
        for fut in as_completed(futures):
            marker, result = fut.result()
            results[marker] = result

    # Every upload must succeed (return a dict with tags + summary, not an exception).
    failures = {
        m: str(r)
        for m, r in results.items()
        if isinstance(r, Exception)
    }
    assert not failures, (
        f"{len(failures)}/{N_UPLOADS} concurrent uploads failed:\n"
        + "\n".join(f"  {m}: {err}" for m, err in failures.items())
    )

    # Every uploaded document must have tags and a summary.
    for marker, doc in results.items():
        if isinstance(doc, Exception):
            continue
        assert doc.get("tags"), f"document for {marker!r} has no tags"
        assert doc.get("summary"), f"document for {marker!r} has no summary"

    # Each document is searchable by its unique marker.
    for marker in markers:
        if isinstance(results[marker], Exception):
            continue
        hits = api.search(marker).get("hits", [])
        assert hits, f"search for {marker!r} returned no hits after concurrent upload"


@pytest.mark.perf
def test_concurrent_searches(api):
    """Run N concurrent searches against seeded data; all return 200 with results."""
    api.login(config.tenant_slug(), config.admin_email(), config.admin_password())

    # Seed one document that all searches will query.
    marker, seed_doc = flows.ingest_and_wait(api, _SAMPLE)
    assert seed_doc.get("tags"), "seed document has no tags"

    def _search(query: str) -> dict[str, object] | Exception:
        c = _new_client()
        try:
            return c.search(query)
        except Exception as exc:
            return exc
        finally:
            c.close()

    with ThreadPoolExecutor(max_workers=min(N_SEARCHES, 16)) as pool:
        futures = [pool.submit(_search, marker) for _ in range(N_SEARCHES)]
        results = [fut.result() for fut in as_completed(futures)]

    errors = [r for r in results if isinstance(r, Exception)]
    error_rate = len(errors) / N_SEARCHES if N_SEARCHES else 0
    assert error_rate == 0.0, (
        f"{len(errors)}/{N_SEARCHES} concurrent searches failed:\n"
        + "\n".join(str(e) for e in errors[:5])
    )

    # Every successful search must return the seeded document.
    for resp in results:
        if isinstance(resp, Exception):
            continue
        hits = resp.get("hits", [])
        doc_ids = [h.get("document_id") for h in hits]
        assert seed_doc.get("id") in doc_ids or len(hits) > 0, (
            "concurrent search returned no hits for seeded document"
        )


@pytest.mark.perf
def test_ingest_throughput(api):
    """Ingest M documents and assert sustained throughput above a configured floor."""
    api.login(config.tenant_slug(), config.admin_email(), config.admin_password())

    markers = [flows.unique_marker() for _ in range(INGEST_COUNT)]
    start = time.monotonic()

    for i, marker in enumerate(markers):
        _, doc = flows.ingest_and_wait(api, _SAMPLE.format(marker=marker))
        assert doc.get("tags"), f"doc {i} has no tags"
        assert doc.get("summary"), f"doc {i} has no summary"

    elapsed = time.monotonic() - start
    throughput = INGEST_COUNT / elapsed if elapsed > 0 else float("inf")

    assert throughput >= INGEST_FLOOR_DOCS_PER_SEC, (
        f"ingest throughput {throughput:.4f} docs/s below floor "
        f"{INGEST_FLOOR_DOCS_PER_SEC} docs/s ({INGEST_COUNT} docs in {elapsed:.1f}s)"
    )

    # All documents must be searchable.
    for marker in markers:
        hits = api.search(marker).get("hits", [])
        assert hits, f"search for {marker!r} returned no hits after ingest"


@pytest.mark.perf
def test_search_latency_p95_under_load(api):
    """Under concurrent load, p95 search latency stays under a configured ceiling."""
    api.login(config.tenant_slug(), config.admin_email(), config.admin_password())

    # Seed a document.
    marker, seed_doc = flows.ingest_and_wait(api, _SAMPLE)
    assert seed_doc.get("tags"), "seed document has no tags"

    latencies: list[float] = []

    def _search_and_measure(query: str) -> float:
        """Run one search and return its latency in seconds."""
        c = _new_client()
        try:
            start = time.monotonic()
            c.search(query)
            return time.monotonic() - start
        except Exception:
            return -1.0  # sentinel for error
        finally:
            c.close()

    with ThreadPoolExecutor(max_workers=min(N_SEARCHES, 16)) as pool:
        futures = [pool.submit(_search_and_measure, marker) for _ in range(N_SEARCHES)]
        for fut in as_completed(futures):
            lat = fut.result()
            if lat >= 0:
                latencies.append(lat)

    assert len(latencies) >= N_SEARCHES * 0.8, (
        f"too few successful searches to compute p95: {len(latencies)}/{N_SEARCHES}"
    )

    latencies.sort()
    p95_idx = int(len(latencies) * 0.95)
    p95 = latencies[p95_idx] if p95_idx < len(latencies) else latencies[-1]

    assert p95 <= SEARCH_LATENCY_P95_CEILING_S, (
        f"p95 search latency {p95:.2f}s exceeds ceiling "
        f"{SEARCH_LATENCY_P95_CEILING_S}s (n={len(latencies)})"
    )


@pytest.mark.perf
def test_mixed_read_write_load(api):
    """Concurrent uploads + searches sustain with a ~0 error rate."""
    api.login(config.tenant_slug(), config.admin_email(), config.admin_password())

    # Pre-seed one document so searches have something to find.
    seed_marker, seed_doc = flows.ingest_and_wait(api, _SAMPLE)
    assert seed_doc.get("tags")

    write_markers = [flows.unique_marker() for _ in range(N_UPLOADS)]
    errors: list[tuple[str, str]] = []

    def _do_upload(marker: str) -> tuple[str, object]:
        c = _new_client()
        try:
            _, doc = flows.ingest_and_wait(c, _SAMPLE.format(marker=marker))
            return marker, doc
        except Exception as exc:
            return marker, exc
        finally:
            c.close()

    def _do_search(query: str) -> tuple[float, object]:
        c = _new_client()
        try:
            start = time.monotonic()
            result = c.search(query)
            elapsed = time.monotonic() - start
            return elapsed, result
        except Exception as exc:
            return -1.0, exc
        finally:
            c.close()

    # Interleave uploads and searches in the same thread pool.
    all_tasks: list[tuple[str, object]] = []
    for m in write_markers:
        all_tasks.append(("upload", m))  # type: ignore[arg-type]
    for _ in range(N_SEARCHES):
        all_tasks.append(("search", seed_marker))  # type: ignore[arg-type]

    results: list[object] = []
    with ThreadPoolExecutor(max_workers=min(len(all_tasks), 16)) as pool:
        future_map = {}
        for kind, payload in all_tasks:
            if kind == "upload":
                fut = pool.submit(_do_upload, payload)
            else:
                fut = pool.submit(_do_search, payload)
            future_map[fut] = (kind, payload)

        for fut in as_completed(future_map):
            kind, payload = future_map[fut]
            try:
                result = fut.result()
            except Exception as exc:
                errors.append((kind, str(exc)))
                continue
            if kind == "upload":
                marker, doc_or_err = result
                if isinstance(doc_or_err, Exception):
                    errors.append(("upload", f"{marker}: {doc_or_err}"))
            else:
                elapsed, search_result = result
                if isinstance(search_result, Exception):
                    errors.append(("search", str(search_result)))

    total_ops = len(all_tasks)
    error_rate = len(errors) / total_ops if total_ops else 0
    assert error_rate < 0.1, (
        f"mixed read/write error rate {error_rate:.1%} ({len(errors)}/{total_ops} ops); "
        f"first errors: {errors[:5]}"
    )

    # All uploads must be searchable.
    for marker in write_markers:
        hits = api.search(marker).get("hits", [])
        assert hits, f"search for uploaded {marker!r} returned no hits under mixed load"


@pytest.mark.perf
def test_large_document_ingest(api):
    """A large (multi-MB) document ingests and becomes searchable within the timeout."""
    api.login(config.tenant_slug(), config.admin_email(), config.admin_password())

    marker = flows.unique_marker()
    # Generate ~N MiB of distinct-ish text (roughly 1 MiB = 1_048_576 bytes).
    # Each paragraph is ~100 bytes; we need ~10_486 paragraphs per MiB.
    paragraphs_per_mb = 10_486
    paragraph = (
        "The Local File Knowledge Base indexes and searches personal files using "
        "hybrid retrieval with LLM-powered tagging. "
    )
    bulk = "\n\n".join(
        f"Paragraph {i}: {paragraph} (iteration {i % 1000})."
        for i in range(LARGE_DOC_MB * paragraphs_per_mb)
    )
    content = f"{bulk}\n\nUnique large-document marker: {marker}."

    deadline = time.monotonic() + LARGE_DOC_TIMEOUT_S
    start = time.monotonic()

    resp = api.ingest_text(f"{marker}.txt", content)
    doc_id = resp.get("document_id")
    assert doc_id is not None, f"large ingest returned no document_id: {resp}"

    doc = api.get_document(doc_id)
    elapsed = time.monotonic() - start
    assert elapsed <= LARGE_DOC_TIMEOUT_S, (
        f"large document ({LARGE_DOC_MB} MiB) ingest took {elapsed:.1f}s, "
        f"exceeding {LARGE_DOC_TIMEOUT_S}s budget"
    )

    assert doc.get("tags"), "large document has no tags"
    assert doc.get("summary"), "large document has no summary"

    # Must be searchable by its unique marker.
    hits = api.search(marker).get("hits", [])
    assert hits, f"search for large document marker {marker!r} returned no hits"


@pytest.mark.perf
def test_job_queue_burst_drains(api):
    """Fire a burst of concurrent uploads; all complete and are searchable.

    The system must handle a burst of concurrent ingest requests without errors,
    and every uploaded document must be fully processed (tags, summary, searchable).
    Because ingest is synchronous (the pipeline runs inline before the 202 response),
    the "queue" being drained is the axum/tokio task queue and any internal
    backpressure mechanisms.
    """
    api.login(config.tenant_slug(), config.admin_email(), config.admin_password())

    markers = [flows.unique_marker() for _ in range(BURST_COUNT)]

    def _burst_upload(marker: str) -> tuple[str, object]:
        c = _new_client()
        try:
            _, doc = flows.ingest_and_wait(c, _SAMPLE.format(marker=marker))
            return marker, doc
        except Exception as exc:
            return marker, exc
        finally:
            c.close()

    results: dict[str, object] = {}
    start = time.monotonic()
    with ThreadPoolExecutor(max_workers=min(BURST_COUNT, 16)) as pool:
        futures = {pool.submit(_burst_upload, m): m for m in markers}
        for fut in as_completed(futures):
            marker, result = fut.result()
            results[marker] = result
    elapsed = time.monotonic() - start

    # Every upload must succeed.
    failures = {
        m: str(r) for m, r in results.items() if isinstance(r, Exception)
    }
    assert not failures, (
        f"{len(failures)}/{BURST_COUNT} burst uploads failed:\n"
        + "\n".join(f"  {m}: {err}" for m, err in list(failures.items())[:5])
    )

    # Every document must be fully processed.
    for marker, doc in results.items():
        if isinstance(doc, Exception):
            continue
        assert doc.get("tags"), f"burst doc {marker!r} has no tags"
        assert doc.get("summary"), f"burst doc {marker!r} has no summary"

    # Every document must be searchable (the "drain" verification).
    drained = 0
    for marker in markers:
        if isinstance(results[marker], Exception):
            continue
        hits = api.search(marker).get("hits", [])
        if hits:
            drained += 1

    assert drained == BURST_COUNT, (
        f"only {drained}/{BURST_COUNT} burst documents are searchable; "
        f"the queue did not fully drain"
    )

    # Health check must still pass after the burst.
    health = api.health()
    assert health.status_code == 200, (
        f"health check returned {health.status_code} after burst; "
        f"system may be degraded under load"
    )


@pytest.mark.perf
def test_sustained_search_qps(api):
    """Hold a target search QPS for a fixed duration with a ~0 error rate."""
    api.login(config.tenant_slug(), config.admin_email(), config.admin_password())

    # Seed a document to search for.
    marker, seed_doc = flows.ingest_and_wait(api, _SAMPLE)
    assert seed_doc.get("tags"), "seed document has no tags"

    # Run searches at the target rate.
    deadline = time.monotonic() + SEARCH_QPS_DURATION_S
    interval = 1.0 / SEARCH_QPS if SEARCH_QPS > 0 else 1.0
    searches_done = 0
    errors: list[Exception] = []
    latencies: list[float] = []

    while time.monotonic() < deadline:
        batch_start = time.monotonic()

        def _timed_search() -> tuple[float, dict[str, object] | Exception]:
            c = _new_client()
            try:
                t0 = time.monotonic()
                result = c.search(marker)
                return time.monotonic() - t0, result
            except Exception as exc:
                return -1.0, exc
            finally:
                c.close()

        with ThreadPoolExecutor(max_workers=min(SEARCH_QPS, 8)) as pool:
            futs = [pool.submit(_timed_search) for _ in range(SEARCH_QPS)]
            for fut in as_completed(futs):
                lat, result = fut.result()
                searches_done += 1
                if isinstance(result, Exception):
                    errors.append(result)
                elif lat >= 0:
                    latencies.append(lat)

        batch_elapsed = time.monotonic() - batch_start
        sleep_time = interval - batch_elapsed
        if sleep_time > 0 and time.monotonic() < deadline:
            time.sleep(sleep_time)

    assert searches_done > 0, "no searches completed during the QPS test"
    error_rate = len(errors) / searches_done if searches_done else 0
    assert error_rate < 0.05, (
        f"sustained QPS error rate {error_rate:.1%} "
        f"({len(errors)}/{searches_done} searches over {SEARCH_QPS_DURATION_S}s)"
    )

    # Every successful search must return the seeded document.
    # This is a spot check sampled from the last few results.
    final_check = api.search(marker)
    hits = final_check.get("hits", [])
    assert hits, "final search after sustained QPS returned no hits"
