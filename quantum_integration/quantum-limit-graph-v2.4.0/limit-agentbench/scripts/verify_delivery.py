#!/usr/bin/env python3
"""
scripts/verify_delivery.py — Reproduce the claims in AGENTBEATS_DELIVERY_SUMMARY.md.

Checks:
    schemas load as valid JSON Schema
    every fixture under tests/fixtures/ validates against its schema
    SCORE_BANDS hold (via core.scoring_utils)
    results/example_response.json validates against a2a_response schema
    results/provenance.json image_digest is a real sha256 digest
    delivery report contains required section anchors and has no ':latest' images

Exit 0 = all claims reproduced. Exit 1 = at least one claim failed.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

from jsonschema import Draft202012Validator

ROOT = Path(__file__).resolve().parent.parent
SCHEMAS = ROOT / "schemas"
FIXTURES = ROOT / "tests" / "fixtures"
RESULTS = ROOT / "results"

_failed = False


def _fail(msg: str) -> None:
    global _failed
    _failed = True
    print(f"FAIL: {msg}", file=sys.stderr)


def _ok(msg: str) -> None:
    print(f"OK   {msg}")


def check_schemas_load() -> None:
    for name in ("a2a_request.v1_1.json",
                 "a2a_response.v1_1.json",
                 "rlhf_feedback.v1.json"):
        p = SCHEMAS / name
        if not p.exists():
            _fail(f"missing schema: {p}")
            continue
        try:
            Draft202012Validator(json.loads(p.read_text()))
        except Exception as exc:
            _fail(f"invalid schema {name}: {exc}")
        else:
            _ok(f"schema {name}")


def check_fixtures() -> None:
    if not FIXTURES.exists():
        _fail(f"missing fixtures dir: {FIXTURES}")
        return
    resp_validator = Draft202012Validator(
        json.loads((SCHEMAS / "a2a_response.v1_1.json").read_text())
    )
    req_validator = Draft202012Validator(
        json.loads((SCHEMAS / "a2a_request.v1_1.json").read_text())
    )
    for p in sorted(FIXTURES.glob("*.json")):
        doc = json.loads(p.read_text())
        validator = req_validator if p.name.startswith("request_") else resp_validator
        errs = list(validator.iter_errors(doc))
        if errs:
            _fail(f"{p.name}: " + "; ".join(f"{list(e.path)}:{e.message}" for e in errs))
        else:
            _ok(f"fixture {p.name}")


def check_score_bands() -> None:
    from core.scoring_utils import calculate_robust_score, SCORE_BANDS
    cases = [
        ("success",        {"a": 1},         {"a": 1},         "success"),
        ("success",        None,              {"a": 1},         "success"),
        ("success",        {"anything": "x"}, None,             "success_no_exp"),
        ("partial",        {"answer": "x"},  {"answer": "x"},  "partial"),
        ("partial",        None,              {"answer": "x"},  "partial"),
        ("timeout",        {"answer": "x"},  {"answer": "x"},  "timeout"),
        ("timeout",        None,              {"answer": "x"},  "timeout"),
        ("invalid_output", {"a": 1},         {"a": 1},         "invalid_output"),
        ("oom",            None,              None,             "oom"),
        ("crash",          None,              None,             "crash"),
        ("failure",        None,              None,             "failure"),
        ("bogus-status",   None,              None,             "failure"),
    ]
    for status, out, exp, band_key in cases:
        score = calculate_robust_score(status, out, exp)
        lo, hi = SCORE_BANDS[band_key]
        if not (lo - 1e-9 <= score <= hi + 1e-9):
            _fail(f"band {status}/{band_key}: score={score} not in [{lo},{hi}]")
        else:
            _ok(f"band {status} ({band_key}) ∈ [{lo},{hi}] (got {score})")


def check_example_response() -> None:
    p = RESULTS / "example_response.json"
    if not p.exists():
        _fail(f"missing {p}")
        return
    validator = Draft202012Validator(
        json.loads((SCHEMAS / "a2a_response.v1_1.json").read_text())
    )
    errs = list(validator.iter_errors(json.loads(p.read_text())))
    if errs:
        _fail("results/example_response.json invalid: "
              + "; ".join(f"{list(e.path)}:{e.message}" for e in errs))
    else:
        _ok("results/example_response.json validates")


def check_provenance() -> None:
    p = RESULTS / "provenance.json"
    if not p.exists():
        _fail(f"missing {p}")
        return
    doc = json.loads(p.read_text())
    digest = doc.get("image_digest", "")
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
        _fail(f"provenance.json image_digest malformed: {digest!r}")
    else:
        _ok(f"image digest pinned: {digest[:20]}…")


def check_report_claims(report: Path) -> None:
    if not report.exists():
        _fail(f"missing report: {report}")
        return
    text = report.read_text()
    for anchor in ("## 0.", "## 3.", "## 4.", "## 6.", "## 7.", "## 9.", "## 10."):
        if anchor not in text:
            _fail(f"report missing section anchor {anchor!r}")
        else:
            _ok(f"report contains {anchor}")

    if re.search(r"\"image\"\s*:\s*\"[^\"]+:latest\"", text):
        _fail("report contains ':latest' image reference")
    else:
        _ok("report has no unpinned ':latest' in image fields")

    # The deprecated alias should be flagged.
    if "demo_enhanced_green_agent.py" in text and "Deprecated" not in text:
        _fail("deprecated demo mentioned without 'Deprecated' marker")
    else:
        _ok("deprecated demo marked")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--report-md", default="AGENTBEATS_DELIVERY_SUMMARY.md")
    args = ap.parse_args()

    check_schemas_load()
    check_fixtures()
    check_score_bands()
    check_example_response()
    check_provenance()
    check_report_claims(ROOT / args.report_md)

    if _failed:
        print("\nRESULT: FAIL", file=sys.stderr)
        return 1
    print("\nRESULT: OK — all delivery claims reproduced")
    return 0


if __name__ == "__main__":
    sys.exit(main())
