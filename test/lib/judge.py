"""DeepSeek LLM-as-judge for grading nondeterministic output (tags, summaries).

Pinned to **DeepSeek v4 pro at max reasoning effort**; the model id, key, base
URL, and effort are all read from the environment at call time (edit
``test/.env``), so they are hot-swappable. This is **not** part of the
deterministic core suite — tests that use it carry ``@pytest.mark.judge`` and run
only in the ``quality`` lane.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass

from . import config


@dataclass
class Verdict:
    """A pass/fail judgement with a one-line reason and the raw judge response."""

    passed: bool
    reason: str
    raw: str = ""


def available() -> bool:
    """True when a DeepSeek API key is configured (otherwise judge tests skip)."""
    return bool(config.deepseek_key())


def _client():
    """Build an OpenAI-compatible client pointed at DeepSeek (imported lazily)."""
    from openai import OpenAI

    return OpenAI(api_key=config.deepseek_key(), base_url=config.deepseek_base_url())


_SYSTEM = (
    "You are a strict evaluator for an automated test suite. Given a RUBRIC and an "
    "OUTPUT, decide whether the OUTPUT satisfies the RUBRIC. Reply with ONLY a JSON "
    'object of the form {"pass": 0 or 1, "reason": "<one short sentence>"}.'
)


def _parse(raw: str) -> tuple[bool, str]:
    """Extract the final ``{"pass": .., "reason": ..}`` object from a judge reply."""
    for blob in reversed(re.findall(r"\{.*?\}", raw, flags=re.DOTALL)):
        try:
            obj = json.loads(blob)
        except json.JSONDecodeError:
            continue
        if "pass" in obj:
            return bool(int(obj["pass"])), str(obj.get("reason", ""))
    return False, f"could not parse judge response: {raw[:200]!r}"


def _ask_once(rubric: str, output: str, context: str) -> Verdict:
    resp = _client().chat.completions.create(
        model=config.deepseek_model(),
        messages=[
            {"role": "system", "content": _SYSTEM},
            {
                "role": "user",
                "content": f"RUBRIC:\n{rubric}\n\nCONTEXT:\n{context}\n\nOUTPUT:\n{output}",
            },
        ],
        temperature=0,
        extra_body={"reasoning_effort": config.deepseek_effort()},
    )
    raw = resp.choices[0].message.content or ""
    passed, reason = _parse(raw)
    return Verdict(passed=passed, reason=reason, raw=raw)


def verdict(*, rubric: str, output: str, context: str = "") -> Verdict:
    """Grade ``output`` against ``rubric``; majority-votes over ``JUDGE_SAMPLES``."""
    samples = config.judge_samples()
    votes = [_ask_once(rubric, output, context) for _ in range(samples)]
    passed = sum(1 for v in votes if v.passed) * 2 > samples
    reason = next((v.reason for v in votes if v.passed == passed), votes[0].reason)
    return Verdict(passed=passed, reason=reason, raw=votes[0].raw)
