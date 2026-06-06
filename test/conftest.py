"""Pytest fixtures shared across the e2e suite.

Makes ``lib`` importable, points Playwright at the app over HTTPS (ignoring the
local/self-signed cert), and exposes an API client, admin credentials, and the
DeepSeek judge (which auto-skips when no key is configured).
"""

from __future__ import annotations

import csv
import datetime
import os
import sys
import uuid
from collections import Counter
from pathlib import Path

import pytest

# Make the harness package importable regardless of where pytest is invoked from.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from lib import config  # noqa: E402  (after sys.path tweak)


@pytest.fixture(scope="session")
def base_url() -> str:
    """Base URL of the app under test."""
    return config.base_url()


@pytest.fixture(scope="session")
def browser_type_launch_args(browser_type_launch_args):
    """Add ``--no-sandbox`` flags so Chromium can launch in container/CI environments.

    The upstream ``pytest-playwright`` default does not include sandbox-disabling
    flags. Without them, ``browser_type.launch()`` fails and the entire ``page``
    fixture chain is unavailable, blocking every browser test.

    On AMD/Vulkan systems (Radeon iGPU sharing VRAM with llama-server),
    ``--disable-gpu`` is required — Chromium's GPU path conflicts with
    llama.cpp's Vulkan context, causing an indefinite hang on startup.
    ``--disable-software-rasterizer`` prevents the CPU fallback path from
    also getting stuck.
    """
    return {
        **browser_type_launch_args,
        "args": browser_type_launch_args.get("args", []) + [
            "--no-sandbox",
            "--disable-setuid-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--disable-software-rasterizer",
        ],
    }


@pytest.fixture
def browser_context_args(browser_context_args, base_url):
    """Default every browser context to the app's base URL and accept its local TLS cert."""
    return {
        **browser_context_args,
        "base_url": base_url,
        "ignore_https_errors": True,
    }


@pytest.fixture
def api(base_url):
    """A black-box HTTP client for the JSON API (mirrors a real API consumer)."""
    from lib.api_client import ApiClient

    client = ApiClient(base_url)
    try:
        yield client
    finally:
        client.close()


@pytest.fixture(scope="session")
def admin_creds() -> tuple[str, str, str]:
    """(tenant_slug, email, password) for the bootstrap admin user."""
    return (config.tenant_slug(), config.admin_email(), config.admin_password())


@pytest.fixture
def judge():
    """The DeepSeek LLM-as-judge module; skips the test if no API key is configured."""
    from lib import judge as _judge

    if not _judge.available():
        pytest.skip("DEEPSEEK_API_KEY not set — skipping LLM-as-judge test")
    return _judge


# ── Results recording (historical CSV) ────────────────────────────────────────
# When E2E_RECORD is set (the run.sh `run`/`quality`/`perf` lanes set it), every
# test outcome for the session is appended to test/results/history.csv, plus a
# one-row-per-run summary in test/results/runs.csv. Agent verification runs
# (run.sh `pytest …`) do NOT set it, so they never pollute the history. The
# output directory can be overridden with E2E_RESULTS_DIR (used by validation).
#
# Philosophy: a FAILED row is a valid result — the test asserts intended behavior,
# so a failure means an app bug. We record it; we do not fix the app or the test.

_RESULTS_DIR = Path(os.environ.get("E2E_RESULTS_DIR") or (Path(__file__).resolve().parent / "results"))
_HISTORY_CSV = _RESULTS_DIR / "history.csv"
_RUNS_CSV = _RESULTS_DIR / "runs.csv"
_RUN_ID = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]
_RESULTS: dict[str, dict] = {}


def _recording() -> bool:
    """True when E2E_RECORD is set (official run.sh lanes set it)."""
    return bool(os.environ.get("E2E_RECORD"))


def _short(text: str | None, limit: int = 300) -> str:
    """Collapse whitespace and truncate an error/skip message for one CSV cell."""
    return " ".join((text or "").split())[:limit]


def pytest_runtest_logreport(report):
    """Accumulate each test's final outcome for the historical CSV (when recording)."""
    if not _recording():
        return
    rec = _RESULTS.setdefault(report.nodeid, {"outcome": None, "duration": 0.0, "error": ""})
    rec["duration"] += float(getattr(report, "duration", 0.0) or 0.0)
    if report.when == "call":
        rec["outcome"] = report.outcome
        if report.failed:
            rec["error"] = _short(report.longreprtext)
    elif report.when == "setup":
        if report.skipped:
            rec["outcome"] = "skipped"
            lr = report.longrepr
            rec["error"] = _short(lr[2] if isinstance(lr, tuple) and len(lr) == 3 else str(lr))
        elif report.failed:
            rec["outcome"] = "error"
            rec["error"] = _short(report.longreprtext)


def pytest_sessionfinish(session, exitstatus):
    """Append this run's per-test rows + a summary row to the results CSVs."""
    if not _recording() or not _RESULTS:
        return
    try:
        _RESULTS_DIR.mkdir(parents=True, exist_ok=True)
        ts = datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")
        write_header = not _HISTORY_CSV.exists()
        with _HISTORY_CSV.open("a", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            if write_header:
                w.writerow(["run_id", "timestamp_utc", "nodeid", "outcome", "duration_s", "error"])
            for nodeid, rec in sorted(_RESULTS.items()):
                w.writerow([_RUN_ID, ts, nodeid, rec["outcome"] or "unknown",
                            f'{rec["duration"]:.3f}', rec["error"]])
        counts = Counter(r["outcome"] or "unknown" for r in _RESULTS.values())
        write_header = not _RUNS_CSV.exists()
        with _RUNS_CSV.open("a", newline="", encoding="utf-8") as fh:
            w = csv.writer(fh)
            if write_header:
                w.writerow(["run_id", "timestamp_utc", "total", "passed", "failed", "skipped", "error"])
            w.writerow([_RUN_ID, ts, len(_RESULTS), counts.get("passed", 0),
                        counts.get("failed", 0), counts.get("skipped", 0), counts.get("error", 0)])
        print(f"\n[e2e] recorded {len(_RESULTS)} results (run {_RUN_ID}) → {_HISTORY_CSV}")
    except Exception as exc:  # recording must never break a test run
        print(f"\n[e2e] WARNING: could not write results CSV: {exc}")
