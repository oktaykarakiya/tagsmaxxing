"""Deterministic 2-way (pairwise) covering-array generator for the ingest matrix.

A full Cartesian product of the upload factors is ~1.6M combinations; empirically
(NIST/Kuhn) ≤2-factor interactions trigger the large majority of faults, so a
pairwise covering array — every value-*pair* of every factor-pair appears in at
least one row — gives near-equivalent fault detection in ~dozens of rows.

This is a greedy, seeded construction (deterministic: no RNG): each row is seeded
with one still-uncovered pair (guaranteeing forward progress) and the remaining
factors are filled to cover the most additional pairs. The result is a *valid*
pairwise array (``verify_pairwise`` proves full 2-way coverage); it is not
guaranteed minimal, which does not matter for test selection.

Mandatory ``seeds`` (high-risk triples pairwise alone need not cover) are emitted
first and their pairs pre-marked, so they always appear in the suite.
"""

from __future__ import annotations

from typing import Any, Hashable

Factors = dict[str, list[Any]]
Row = dict[str, Any]
_Pair = tuple[tuple[str, Hashable], tuple[str, Hashable]]


def _key(name: str, value: Any) -> tuple[str, Hashable]:
    """A hashable (factor, value) key; values are repr'd when not hashable."""
    try:
        hash(value)
        return (name, value)
    except TypeError:
        return (name, repr(value))


def _pair(a: tuple[str, Hashable], b: tuple[str, Hashable]) -> _Pair:
    """Canonical (order-independent) key for the unordered pair {a, b}."""
    return (a, b) if a <= b else (b, a)


def _all_pairs(factors: Factors) -> set[_Pair]:
    """Every cross-factor value-pair that a 2-way array must cover."""
    names = list(factors)
    pairs: set[_Pair] = set()
    for i in range(len(names)):
        for j in range(i + 1, len(names)):
            for va in factors[names[i]]:
                for vb in factors[names[j]]:
                    pairs.add(_pair(_key(names[i], va), _key(names[j], vb)))
    return pairs


def _row_pairs(row: Row) -> set[_Pair]:
    """All cross-factor pairs realised by a single row."""
    items = [_key(n, v) for n, v in row.items()]
    out: set[_Pair] = set()
    for i in range(len(items)):
        for j in range(i + 1, len(items)):
            out.add(_pair(items[i], items[j]))
    return out


def pairwise(factors: Factors, *, seeds: list[Row] | None = None, max_rows: int = 5000) -> list[Row]:
    """Return a list of rows covering every 2-way combination of *factors*.

    Each row maps every factor name to one of its values. ``seeds`` are emitted
    first (use for mandatory high-risk combinations). Deterministic for a given
    input (factors and their level order).
    """
    names = list(factors)
    if any(not factors[n] for n in names):
        raise ValueError("every factor must have at least one level")

    uncovered = _all_pairs(factors)
    rows: list[Row] = []

    # Emit mandatory seed rows first; pre-mark the pairs they cover.
    for seed in seeds or []:
        if set(seed) != set(names):
            raise ValueError(f"seed row must set every factor; got {sorted(seed)}")
        rows.append(dict(seed))
        uncovered -= _row_pairs(seed)

    while uncovered and len(rows) < max_rows:
        # Seed the row with the lexicographically-smallest uncovered pair so the
        # loop always makes progress (≥1 new pair per row → guaranteed to finish).
        (n1, v1), (n2, v2) = min(uncovered)
        row: Row = {n1: v1, n2: v2}

        # Fill the remaining factors greedily: pick the level covering the most
        # currently-uncovered pairs against the already-chosen cells.
        for n in names:
            if n in row:
                continue
            best_val, best_gain = factors[n][0], -1
            for v in factors[n]:
                cand = _key(n, v)
                gain = sum(1 for m, mv in row.items() if _pair(_key(m, mv), cand) in uncovered)
                if gain > best_gain:
                    best_val, best_gain = v, gain
            row[n] = best_val

        uncovered -= _row_pairs(row)
        rows.append(row)

    if uncovered:
        raise RuntimeError(f"pairwise did not converge: {len(uncovered)} pairs left after {len(rows)} rows")
    return rows


def verify_pairwise(factors: Factors, rows: list[Row]) -> bool:
    """True iff *rows* realise every 2-way value-pair of *factors* (full coverage)."""
    need = _all_pairs(factors)
    have: set[_Pair] = set()
    for r in rows:
        have |= _row_pairs(r)
    return need <= have
