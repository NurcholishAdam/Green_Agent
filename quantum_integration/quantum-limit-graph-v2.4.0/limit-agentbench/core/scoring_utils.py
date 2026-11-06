"""
core/scoring_utils.py — Robust scoring primitives.

Public contract:
    * Every function returns a float in [0, 1]; never raises on malformed input.
    * `SCORE_BANDS` is the single source of truth for the delivery report's
      scoring table (§3). Tests lock these values; the report derives its
      table from them.

Canonical score bands:
    success        → [0.0, 1.0]   (0.8 if expected_output is None)
    partial        → [0.4, 0.8]   with output; 0.0 without
    timeout        → [0.4, 0.8]   with output; 0.0 without
    invalid_output → [0.2, 0.4]   with output; 0.0 without
    oom/crash/failure/unknown → 0.0
"""
from __future__ import annotations

import math
import re
from typing import Any

__all__ = [
    "SCORE_BANDS",
    "calculate_robust_score",
    "evaluate_similarity",
    "estimate_completeness",
    "evaluate_partial_match",
]

# Canonical bands — imported by tests and by scripts/verify_delivery.py.
SCORE_BANDS: dict[str, tuple[float, float]] = {
    "success":        (0.0, 1.0),
    "success_no_exp": (0.8, 0.8),
    "partial":        (0.4, 0.8),
    "timeout":        (0.4, 0.8),
    "invalid_output": (0.2, 0.4),
    "oom":            (0.0, 0.0),
    "crash":          (0.0, 0.0),
    "failure":        (0.0, 0.0),
}


def _clamp(x: float) -> float:
    """Clamp x to [0, 1]; map NaN/inf to 0."""
    try:
        x = float(x)
    except (TypeError, ValueError):
        return 0.0
    if math.isnan(x) or math.isinf(x):
        return 0.0
    return max(0.0, min(1.0, x))


def _tokens(s: str) -> list[str]:
    return re.findall(r"[a-z0-9]+", s.lower())


def _flatten(obj: Any) -> list[Any]:
    if isinstance(obj, dict):
        return [x for v in obj.values() for x in _flatten(v)]
    if isinstance(obj, (list, tuple)):
        return [x for v in obj for x in _flatten(v)]
    return [obj]


def evaluate_similarity(
    output: dict[str, Any] | None,
    expected: dict[str, Any] | None,
) -> float:
    """Jaccard token overlap — placeholder for domain-specific similarity."""
    if not output or not expected:
        return 0.0
    a = set(_tokens(" ".join(map(str, _flatten(output)))))
    b = set(_tokens(" ".join(map(str, _flatten(expected)))))
    if not a or not b:
        return 0.0
    return _clamp(len(a & b) / len(a | b))


def estimate_completeness(output: dict[str, Any] | None) -> float:
    """Structural completeness over {answer, trace, sources, conclusion}."""
    if not output:
        return 0.0
    sections = ("answer", "trace", "sources", "conclusion")
    hits = sum(1 for k in sections if output.get(k))
    return _clamp(hits / len(sections))


def evaluate_partial_match(
    output: dict[str, Any] | None,
    expected: dict[str, Any] | None,
) -> float:
    """Fraction of expected top-level keys present in output."""
    if not output or not expected:
        return 0.0
    ek = set(expected.keys())
    if not ek:
        return 0.0
    return _clamp(len(ek & set(output.keys())) / len(ek))


def calculate_robust_score(
    status: str,
    output: dict[str, Any] | None,
    expected_output: dict[str, Any] | None,
) -> float:
    """
    Deterministic, clamped score in [0, 1].

    Canonical score bands (locked by tests/test_robust_scoring.py):
        success        → [0.0, 1.0]   (0.8 if expected_output is None)
        partial        → [0.4, 0.8]   with output; 0.0 without
        timeout        → [0.4, 0.8]   with output; 0.0 without
        invalid_output → [0.2, 0.4]   with output; 0.0 without
        oom/crash/failure/unknown → 0.0
    """
    if status == "success":
        if expected_output is None:
            return 0.8
        return _clamp(evaluate_similarity(output or {}, expected_output))

    if status in ("partial", "timeout") and output:
        base = _clamp(evaluate_similarity(output, expected_output or {}))
        completeness = _clamp(estimate_completeness(output))
        return _clamp(0.4 + 0.4 * base * completeness)

    if status == "invalid_output" and output:
        partial = _clamp(evaluate_partial_match(output, expected_output or {}))
        return _clamp(0.2 + 0.2 * partial)

    return 0.0
