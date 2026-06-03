"""Runtime configuration for the e2e harness.

Every value is read from the environment **at call time** so it stays
hot-swappable: edit ``test/.env`` (or export a variable) and the next call picks
it up. ``test/.env`` is loaded once at import as a convenience for local runs;
real environment variables always take precedence (``override=False``).
"""

from __future__ import annotations

import os
from pathlib import Path

try:  # dotenv is a declared dependency, but degrade gracefully if absent.
    from dotenv import load_dotenv

    load_dotenv(Path(__file__).resolve().parents[1] / ".env", override=False)
except Exception:  # pragma: no cover - convenience only
    pass


def _get(key: str, default: str) -> str:
    """Return env var ``key`` at call time, falling back to ``default`` when unset/empty."""
    val = os.environ.get(key)
    return val if val not in (None, "") else default


# ── App under test ──────────────────────────────────────────────────────────────
def base_url() -> str:
    """Base URL of the running app (HTTPS via Caddy by default)."""
    return _get("BASE_URL", "https://localhost:9443")


def verify_tls() -> bool:
    """Whether HTTP clients verify TLS certs (False for Caddy's local/self-signed cert)."""
    return _get("E2E_VERIFY_TLS", "false").lower() in ("1", "true", "yes")


def tenant_slug() -> str:
    """Slug of the bootstrap tenant."""
    return _get("KB_TENANT_SLUG", "default")


def admin_email() -> str:
    """Email of the bootstrap admin user."""
    return _get("KB_ADMIN_EMAIL", "admin@local.kb")


def admin_password() -> str:
    """Password of the bootstrap admin user."""
    return _get("KB_ADMIN_PASSWORD", "admin")


# ── Timeouts (seconds) ────────────────────────────────────────────────────────────
def job_done_timeout() -> float:
    """How long to wait for an async ingest job to finish."""
    return float(_get("JOB_DONE_TIMEOUT", "420"))


def http_timeout() -> float:
    """Per-request HTTP timeout for the API client."""
    return float(_get("HTTP_TIMEOUT", "60"))


# ── DeepSeek LLM-as-judge ─────────────────────────────────────────────────────────
def deepseek_key() -> str:
    """DeepSeek API key (empty string disables the judge lane)."""
    return _get("DEEPSEEK_API_KEY", "")


def deepseek_base_url() -> str:
    """DeepSeek OpenAI-compatible base URL."""
    return _get("DEEPSEEK_BASE_URL", "https://api.deepseek.com")


def deepseek_model() -> str:
    """Judge model id (DeepSeek v4 pro by default)."""
    return _get("DEEPSEEK_JUDGE_MODEL", "deepseek-v4-pro")


def deepseek_effort() -> str:
    """Reasoning effort for the judge (``high`` = max)."""
    return _get("DEEPSEEK_REASONING_EFFORT", "high")


def judge_samples() -> int:
    """Number of judge samples to majority-vote over (>=1)."""
    return max(1, int(_get("JUDGE_SAMPLES", "1")))
