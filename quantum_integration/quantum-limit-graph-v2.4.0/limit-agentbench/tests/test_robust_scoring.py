"""
tests/test_robust_scoring.py — Lock the canonical score bands.

Any change to core/scoring_utils.py that violates the delivery report's §3
table fails here first.
"""
from __future__ import annotations

import math

import pytest

from core.scoring_utils import (
    SCORE_BANDS,
    calculate_robust_score,
    estimate_completeness,
    evaluate_partial_match,
    evaluate_similarity,
)


# --- Primitives ------------------------------------------------------------

def test_similarity_identical_is_one():
    doc = {"answer": "hello world", "trace": ["a"],
           "sources": ["x"], "conclusion": "done"}
    assert evaluate_similarity(doc, doc) == pytest.approx(1.0)


def test_similarity_disjoint_is_zero():
    assert evaluate_similarity({"a": "foo"}, {"b": "bar"}) == pytest.approx(0.0)


def test_similarity_empty_is_zero():
    assert evaluate_similarity(None, {"a": 1}) == 0.0
    assert evaluate_similarity({"a": 1}, None) == 0.0
    assert evaluate_similarity({}, {}) == 0.0


def test_completeness_empty_list_is_falsy():
    doc = {"answer": 1, "trace": [], "sources": [], "conclusion": "x"}
    assert estimate_completeness(doc) == pytest.approx(0.5)


def test_completeness_all_present():
    doc = {"answer": 1, "trace": [1], "sources": [1], "conclusion": "x"}
    assert estimate_completeness(doc) == pytest.approx(1.0)


def test_completeness_empty_is_zero():
    assert estimate_completeness(None) == 0.0
    assert estimate_completeness({}) == 0.0


def test_partial_match_basic():
    assert evaluate_partial_match({"a": 1, "b": 2}, {"a": 0, "b": 0}) == pytest.approx(1.0)
    assert evaluate_partial_match({"a": 1}, {"a": 0, "b": 0}) == pytest.approx(0.5)
    assert evaluate_partial_match({"x": 1}, {"a": 0}) == pytest.approx(0.0)


# --- Orchestrator: bands ---------------------------------------------------

@pytest.mark.parametrize("output,expected,lo,hi", [
    ({"answer": "hello world"}, {"answer": "hello world"}, 0.99, 1.0),
    ({"answer": "hello world"}, {"answer": "totally different"}, 0.0, 0.2),
])
def test_success_band(output, expected, lo, hi):
    score = calculate_robust_score("success", output, expected)
    assert lo <= score <= hi
    assert 0.0 <= score <= 1.0


def test_success_no_expected_fixed_at_08():
    score = calculate_robust_score("success", {"anything": "x"}, None)
    assert score == pytest.approx(SCORE_BANDS["success_no_exp"][0])


@pytest.mark.parametrize("status", ["partial", "timeout"])
@pytest.mark.parametrize("output", [
    None,
    {},
    {"answer": "partial answer"},
    {"answer": "a", "trace": [1], "sources": [1], "conclusion": "c"},
])
def test_partial_and_timeout_band(status, output):
    score = calculate_robust_score(status, output, {"answer": "a"})
    lo, hi = SCORE_BANDS[status]
    assert lo <= score <= hi, f"{status}/{output!r} → {score} not in [{lo},{hi}]"


def test_partial_no_output_is_zero():
    assert calculate_robust_score("partial", None, {"answer": "a"}) == 0.0
    assert calculate_robust_score("timeout", None, {"answer": "a"}) == 0.0


def test_invalid_output_band():
    lo, hi = SCORE_BANDS["invalid_output"]
    for out in ({"a": 1}, {"a": 1, "b": 2, "c": 3}):
        score = calculate_robust_score("invalid_output", out, {"a": 0, "b": 0, "c": 0})
        assert lo <= score <= hi


@pytest.mark.parametrize("status", ["oom", "crash", "failure", "totally-unknown"])
def test_no_output_statuses_are_zero(status):
    assert calculate_robust_score(status, {"anything": "x"}, {"anything": "x"}) == 0.0


def test_nan_inf_never_leak():
    score = calculate_robust_score("success", {"a": float("nan")}, {"a": "x"})
    assert not math.isnan(score) and 0.0 <= score <= 1.0


def test_band_map_is_consistent():
    """Any drift in SCORE_BANDS must be an intentional, reviewed change."""
    assert SCORE_BANDS["success"]        == (0.0, 1.0)
    assert SCORE_BANDS["success_no_exp"] == (0.8, 0.8)
    assert SCORE_BANDS["partial"]        == (0.4, 0.8)
    assert SCORE_BANDS["timeout"]        == (0.4, 0.8)
    assert SCORE_BANDS["invalid_output"] == (0.2, 0.4)
    assert SCORE_BANDS["oom"]            == (0.0, 0.0)
