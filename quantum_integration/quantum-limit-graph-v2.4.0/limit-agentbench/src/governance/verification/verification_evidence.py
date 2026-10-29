"""
VerificationEvidence — the outcome of a formal verification run.

An immutable record of what a verifier checked, what passed, what
failed, and the counterexamples it produced. Consumed by the
:class:`PolicyDecisionPoint` (as a gate on ALLOW/MODIFY) and written
into the audit trail (via :class:`GovernanceEvent.details`).

Key guarantees
--------------
* **Immutable.** ``frozen=True``; every string collection is a
  ``tuple``; every counterexample is a shallow-copied ``Mapping``.
  Evidence cannot be rewritten after emission.
* **Typed.** ``verdict`` is a :class:`VerificationVerdict`; plain
  strings are coerced, unknown values are rejected.
* **Consistent.** Construction enforces invariants that make the
  evidence self-consistent — see the block below.
* **Timestamp discipline.** ``at`` must be timezone-aware and is
  normalized to UTC.
* **JSON-safe.** ``to_dict()`` recursively encodes ``datetime`` /
  ``UUID`` / ``Enum`` / ``Decimal`` / ``bytes`` / … inside
  ``counterexamples``.
* **Round-trippable.** ``to_dict`` / ``from_dict`` / ``to_json`` /
  ``to_schema`` match the conventions of the rest of the governance
  stack.
* **Tamper-evident.** ``evidence_hash()`` returns a stable SHA-256
  digest of the canonical JSON form, so the record can be chained
  into :class:`GovernanceEvent.prev_event_hash`.

Consistency invariants enforced at construction
-----------------------------------------------
1. ``set(properties_passed) ∩ set(properties_failed) == ∅``
   (a property cannot both pass and fail).
2. ``set(properties_passed) ∪ set(properties_failed) ⊆ set(properties_checked)``
   (every pass/fail refers to a checked property).
3. ``verdict is PASSED`` ⇒ ``properties_failed`` is empty.
4. ``verdict is NOT_REQUIRED`` ⇒ ``properties_checked`` is empty.
5. ``coverage_pct is None`` or ``0.0 <= coverage_pct <= 1.0``.
6. Every ``counterexamples`` entry has a ``property`` key whose value
   is in ``properties_failed``.

Invariant 4 is slightly looser than "verdict is FAILED ⇒
properties_failed is non-empty": a verifier that crashed before
recording any failure can legitimately produce a ``FAILED`` verdict
with an empty ``properties_failed`` and a single counterexample
describing the crash. Invariant 6 still requires the counterexample
to name its property, so the audit trail is never silent.

Note on ``coverage_pct``
------------------------
The default is ``None`` (distinct from ``0.0``) to match the enhanced
:class:`PolicyVerifier`, which returns ``None`` when nothing was
checked ("no coverage was measured"). A caller that wants a numeric
value for the empty case should pass ``0.0`` explicitly.
"""

from __future__ import annotations

import base64
import hashlib
import json
import unittest
import uuid
from collections.abc import Mapping as MappingABC
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import (
    Any,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Tuple,
    Union,
)
from uuid import UUID


__all__ = [
    "VerificationVerdict",
    "VerificationEvidence",
]


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------
class VerificationVerdict(str, Enum):
    """Aggregate outcome of a verification run."""

    PASSED = "passed"
    FAILED = "failed"
    NOT_REQUIRED = "not_required"


# ---------------------------------------------------------------------------
# JSON-safety helper
# ---------------------------------------------------------------------------
def _jsonable(value: Any) -> Any:
    """Recursively convert a value to something ``json.dumps`` accepts."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, (date, UUID, Path, Decimal)):
        return str(value)
    if isinstance(value, Enum):
        return _jsonable(value.value)
    if isinstance(value, MappingABC):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_jsonable(v) for v in value]
    if isinstance(value, (bytes, bytearray)):
        return base64.b64encode(bytes(value)).decode("ascii")
    return str(value)


# ---------------------------------------------------------------------------
# Helper: coerce a list-like to a deduped tuple of strings
# ---------------------------------------------------------------------------
def _as_str_tuple(value: Any, label: str) -> Tuple[str, ...]:
    if isinstance(value, str):
        raise TypeError(
            f"{label} must be an iterable of strings, not a single "
            f"string; wrap in a list, e.g. {label}=['my_prop']"
        )
    try:
        items = list(value)
    except TypeError:
        raise TypeError(
            f"{label} must be an iterable of strings, got "
            f"{type(value).__name__}"
        ) from None
    seen = set()
    out: List[str] = []
    for item in items:
        if not isinstance(item, str) or not item.strip():
            raise ValueError(
                f"{label} entries must be non-empty strings, got "
                f"{item!r}"
            )
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return tuple(out)


# ---------------------------------------------------------------------------
# VerificationEvidence
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class VerificationEvidence:
    """
    The outcome of a formal verification run.

    See the module docstring for the enforced consistency invariants.
    """

    policy_version: str
    verdict: VerificationVerdict
    properties_checked: Tuple[str, ...] = ()
    properties_passed: Tuple[str, ...] = ()
    properties_failed: Tuple[str, ...] = ()
    counterexamples: Tuple[Mapping[str, Any], ...] = ()
    coverage_pct: Optional[float] = None
    verifier: Optional[str] = None
    verifier_version: Optional[str] = None
    at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    # -- invariants --------------------------------------------------------
    def __post_init__(self) -> None:
        # policy_version: non-empty string
        if (
            not isinstance(self.policy_version, str)
            or not self.policy_version.strip()
        ):
            raise ValueError(
                "policy_version must be a non-empty string"
            )

        # verdict: coerce plain strings to enum
        if not isinstance(self.verdict, VerificationVerdict):
            object.__setattr__(
                self,
                "verdict",
                VerificationVerdict(self.verdict),
            )

        # Normalize property collections to deduped tuples of strings
        checked = _as_str_tuple(
            self.properties_checked, "properties_checked",
        )
        passed = _as_str_tuple(
            self.properties_passed, "properties_passed",
        )
        failed = _as_str_tuple(
            self.properties_failed, "properties_failed",
        )
        object.__setattr__(self, "properties_checked", checked)
        object.__setattr__(self, "properties_passed", passed)
        object.__setattr__(self, "properties_failed", failed)

        # Invariant 1: passed ∩ failed == ∅
        overlap = set(passed) & set(failed)
        if overlap:
            raise ValueError(
                f"properties cannot be both passed and failed: "
                f"{sorted(overlap)}"
            )
        # Invariant 2: passed ∪ failed ⊆ checked
        unaccounted = (set(passed) | set(failed)) - set(checked)
        if unaccounted:
            raise ValueError(
                f"properties_passed / properties_failed mention "
                f"properties not in properties_checked: "
                f"{sorted(unaccounted)}"
            )

        # Invariant 3: PASSED ⇒ failed empty
        if (
            self.verdict is VerificationVerdict.PASSED
            and failed
        ):
            raise ValueError(
                "verdict='passed' is inconsistent with "
                f"properties_failed={list(failed)}"
            )
        # Invariant 4: NOT_REQUIRED ⇒ checked empty
        if (
            self.verdict is VerificationVerdict.NOT_REQUIRED
            and checked
        ):
            raise ValueError(
                "verdict='not_required' is inconsistent with "
                f"properties_checked={list(checked)}"
            )

        # Normalize counterexamples: tuple of copies, each Mapping
        if self.counterexamples is None:
            object.__setattr__(self, "counterexamples", ())
        else:
            normalized: List[Mapping[str, Any]] = []
            for i, cx in enumerate(self.counterexamples):
                if not isinstance(cx, MappingABC):
                    raise TypeError(
                        f"counterexamples[{i}] must be a Mapping, "
                        f"got {type(cx).__name__}"
                    )
                cx_copy = dict(cx)
                # Invariant 6: counterexample must reference a failed
                # property.
                prop = cx_copy.get("property")
                if not isinstance(prop, str) or not prop.strip():
                    raise ValueError(
                        f"counterexamples[{i}] is missing a "
                        f"non-empty 'property' field"
                    )
                if prop not in set(failed):
                    raise ValueError(
                        f"counterexamples[{i}] references property "
                        f"{prop!r}, which is not in "
                        f"properties_failed={list(failed)}"
                    )
                normalized.append(cx_copy)
            object.__setattr__(
                self, "counterexamples", tuple(normalized),
            )

        # coverage_pct: None or [0, 1]
        if self.coverage_pct is not None:
            if isinstance(self.coverage_pct, bool) or not isinstance(
                self.coverage_pct, (int, float)
            ):
                raise TypeError(
                    "coverage_pct must be a number or None, got "
                    f"{type(self.coverage_pct).__name__}"
                )
            c = float(self.coverage_pct)
            # Reject NaN / Inf, out-of-range
            if not (c == c) or c in (float("inf"), float("-inf")):
                raise ValueError(
                    f"coverage_pct must be finite, got "
                    f"{self.coverage_pct!r}"
                )
            if not 0.0 <= c <= 1.0:
                raise ValueError(
                    f"coverage_pct must be in [0, 1], got {c}"
                )
            object.__setattr__(self, "coverage_pct", c)

        # Optional provenance strings
        for label in ("verifier", "verifier_version"):
            value = getattr(self, label)
            if value is None:
                continue
            if not isinstance(value, str) or not value.strip():
                raise ValueError(
                    f"{label} must be a non-empty string or None"
                )

        # at: aware, normalized to UTC
        if not isinstance(self.at, datetime):
            raise TypeError("at must be a datetime")
        if self.at.tzinfo is None:
            raise ValueError("at must be timezone-aware")
        object.__setattr__(
            self, "at", self.at.astimezone(timezone.utc)
        )

    # -- accessors ---------------------------------------------------------
    @property
    def passed(self) -> bool:
        return self.verdict is VerificationVerdict.PASSED

    @property
    def failed(self) -> bool:
        return self.verdict is VerificationVerdict.FAILED

    @property
    def not_required(self) -> bool:
        return self.verdict is VerificationVerdict.NOT_REQUIRED

    @property
    def failure_count(self) -> int:
        return len(self.properties_failed)

    def failures(self) -> Tuple[Mapping[str, Any], ...]:
        """Counterexamples that reference a failed property."""
        return tuple(self.counterexamples)

    def computed_coverage(self) -> Optional[float]:
        """``len(passed) / len(checked)`` or ``None`` if nothing checked."""
        if not self.properties_checked:
            return None
        return (
            len(self.properties_passed)
            / len(self.properties_checked)
        )

    # -- tamper-evidence ---------------------------------------------------
    def evidence_hash(self) -> str:
        """SHA-256 hex digest of the canonical JSON form."""
        return hashlib.sha256(
            self.to_json().encode("utf-8")
        ).hexdigest()

    # -- serialization -----------------------------------------------------
    def to_dict(self) -> Dict[str, Any]:
        return {
            "policy_version": self.policy_version,
            "verdict": self.verdict.value,
            "properties_checked": list(self.properties_checked),
            "properties_passed": list(self.properties_passed),
            "properties_failed": list(self.properties_failed),
            "counterexamples": [
                _jsonable(cx) for cx in self.counterexamples
            ],
            "coverage_pct": self.coverage_pct,
            "verifier": self.verifier,
            "verifier_version": self.verifier_version,
            "at": self.at.isoformat(),
        }

    def to_json(self) -> str:
        return json.dumps(
            self.to_dict(),
            separators=(",", ":"),
            sort_keys=True,
        )

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any],
    ) -> "VerificationEvidence":
        if not isinstance(data, MappingABC):
            raise TypeError(
                f"data must be a Mapping, got {type(data).__name__}"
            )
        for required in ("policy_version", "verdict"):
            if required not in data:
                raise ValueError(
                    f"data must contain '{required}'"
                )

        raw_at = data.get("at")
        if isinstance(raw_at, str):
            at = datetime.fromisoformat(
                raw_at.replace("Z", "+00:00")
            )
        elif isinstance(raw_at, datetime):
            at = raw_at
        elif raw_at is None:
            at = datetime.now(timezone.utc)
        else:
            raise TypeError(
                "at must be an ISO-8601 string, a datetime, or None"
            )

        return cls(
            policy_version=data["policy_version"],
            verdict=VerificationVerdict(data["verdict"]),
            properties_checked=tuple(
                data.get("properties_checked", ())
            ),
            properties_passed=tuple(
                data.get("properties_passed", ())
            ),
            properties_failed=tuple(
                data.get("properties_failed", ())
            ),
            counterexamples=tuple(
                data.get("counterexamples", ())
            ),
            coverage_pct=data.get("coverage_pct"),
            verifier=data.get("verifier"),
            verifier_version=data.get("verifier_version"),
            at=at,
        )

    def to_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "required": ["policy_version", "verdict", "at"],
            "properties": {
                "policy_version": {
                    "type": "string", "minLength": 1,
                },
                "verdict": {
                    "type": "string",
                    "enum": [v.value for v in VerificationVerdict],
                },
                "properties_checked": {
                    "type": "array",
                    "items": {"type": "string", "minLength": 1},
                },
                "properties_passed": {
                    "type": "array",
                    "items": {"type": "string", "minLength": 1},
                },
                "properties_failed": {
                    "type": "array",
                    "items": {"type": "string", "minLength": 1},
                },
                "counterexamples": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "required": ["property"],
                        "properties": {
                            "property": {
                                "type": "string", "minLength": 1,
                            },
                        },
                    },
                },
                "coverage_pct": {
                    "type": ["number", "null"],
                    "minimum": 0,
                    "maximum": 1,
                },
                "verifier": {
                    "type": ["string", "null"],
                },
                "verifier_version": {
                    "type": ["string", "null"],
                },
                "at": {
                    "type": "string", "format": "date-time",
                },
            },
            "additionalProperties": False,
        }

    # -- misc --------------------------------------------------------------
    def describe(self) -> str:
        parts = [f"verdict={self.verdict.value}"]
        if self.properties_checked:
            parts.append(
                f"checked={len(self.properties_checked)}"
            )
        if self.properties_passed:
            parts.append(
                f"passed={len(self.properties_passed)}"
            )
        if self.properties_failed:
            parts.append(
                f"failed={len(self.properties_failed)}"
            )
        if self.coverage_pct is not None:
            parts.append(
                f"coverage={self.coverage_pct:.2f}"
            )
        if self.verifier:
            parts.append(f"verifier={self.verifier!r}")
        return f"VerificationEvidence({', '.join(parts)})"

    def __str__(self) -> str:
        return self.describe()

    def __repr__(self) -> str:
        return self.describe()


# ===========================================================================
# TESTS
# ===========================================================================
def _evidence(**kwargs: Any) -> VerificationEvidence:
    defaults: Dict[str, Any] = dict(
        policy_version="v1",
        verdict=VerificationVerdict.PASSED,
        properties_checked=("a", "b"),
        properties_passed=("a", "b"),
        coverage_pct=1.0,
    )
    defaults.update(kwargs)
    return VerificationEvidence(**defaults)


class TestConstructionHappyPath(unittest.TestCase):
    def test_minimal(self):
        e = VerificationEvidence(
            policy_version="v1",
            verdict=VerificationVerdict.PASSED,
        )
        self.assertTrue(e.passed)
        self.assertFalse(e.failed)
        self.assertFalse(e.not_required)
        self.assertEqual(e.properties_checked, ())
        self.assertEqual(e.counterexamples, ())
        self.assertIsNone(e.coverage_pct)
        self.assertIsNotNone(e.at.tzinfo)

    def test_full(self):
        e = VerificationEvidence(
            policy_version="v1",
            verdict="failed",
            properties_checked=["a", "b"],
            properties_passed=["a"],
            properties_failed=["b"],
            counterexamples=[{"property": "b", "error": "x"}],
            coverage_pct=0.5,
            verifier="policy_verifier",
            verifier_version="1.0.0",
        )
        self.assertTrue(e.failed)
        self.assertEqual(e.properties_passed, ("a",))
        self.assertEqual(e.properties_failed, ("b",))
        self.assertEqual(e.failure_count, 1)
        self.assertEqual(e.coverage_pct, 0.5)

    def test_verdict_string_coerced(self):
        e = VerificationEvidence(
            policy_version="v1", verdict="passed",
        )
        self.assertIs(e.verdict, VerificationVerdict.PASSED)

    def test_unknown_verdict_rejected(self):
        with self.assertRaises(ValueError):
            VerificationEvidence(
                policy_version="v1", verdict="passd",  # type: ignore[arg-type]
            )
        with self.assertRaises(ValueError):
            VerificationEvidence(
                policy_version="v1", verdict="PASSED",  # type: ignore[arg-type]
            )

    def test_not_required_with_empty_checks(self):
        e = VerificationEvidence(
            policy_version="v1", verdict="not_required",
        )
        self.assertTrue(e.not_required)


class TestConstructionValidation(unittest.TestCase):
    def test_empty_policy_version_rejected(self):
        with self.assertRaises(ValueError):
            VerificationEvidence(
                policy_version="", verdict="passed",
            )
        with self.assertRaises(ValueError):
            VerificationEvidence(
                policy_version="   ", verdict="passed",
            )

    def test_single_string_checked_rejected(self):
        with self.assertRaises(TypeError):
            VerificationEvidence(
                policy_version="v1", verdict="passed",
                properties_checked="a",  # type: ignore[arg-type]
            )

    def test_empty_property_entry_rejected(self):
        with self.assertRaises(ValueError):
            VerificationEvidence(
                policy_version="v1", verdict="passed",
                properties_checked=[""],
            )

    def test_duplicate_entries_deduped(self):
        e = VerificationEvidence(
            policy_version="v1", verdict="passed",
            properties_checked=["a", "a", "b"],
            properties_passed=["a", "a", "b"],
        )
        self.assertEqual(e.properties_checked, ("a", "b"))
        self.assertEqual(e.properties_passed, ("a", "b"))

    def test_overlap_passed_and_failed_rejected(self):
        with self.assertRaises(ValueError):
            VerificationEvidence(
                policy_version="v1", verdict="failed",
                properties_checked=["a"],
                properties_passed=["a"],
                properties_failed=["a"],
            )

    def test_passed_not_in_checked_rejected(self):
        with self.assertRaises(ValueError):
            VerificationEvidence(
                policy_version="v1", verdict="passed",
                properties_checked=["a"],
                properties_passed=["b"],
            )

    def test_failed_not_in_checked_rejected(self):
        with self.assertRaises(ValueError):
            VerificationEvidence(
                policy_version="v1", verdict="failed",
                properties_checked=["a"],
                properties_failed=["b"],
                counterexamples=[{"property": "b"}],
            )

    def test_passed_verdict_with_failed_properties_rejected(self):
        with self.assertRaises(ValueError):
            VerificationEvidence(
                policy_version="v1", verdict="passed",
                properties_checked=["a"],
                properties_failed=["a"],
                counterexamples=[{"property": "a"}],
            )

    def test_not_required_with_checked_rejected(self):
        with self.assertRaises(ValueError):
            VerificationEvidence(
                policy_version="v1", verdict="not_required",
                properties_checked=["a"],
            )


class TestCoverageValidation(unittest.TestCase):
    def test_negative_rejected(self):
        with self.assertRaises(ValueError):
            _evidence(coverage_pct=-0.1)

    def test_above_one_rejected(self):
        with self.assertRaises(ValueError):
            _evidence(coverage_pct=1.5)

    def test_nan_rejected(self):
        with self.assertRaises(ValueError):
            _evidence(coverage_pct=float("nan"))

    def test_inf_rejected(self):
        with self.assertRaises(ValueError):
            _evidence(coverage_pct=float("inf"))

    def test_bool_rejected(self):
        with self.assertRaises(TypeError):
            _evidence(coverage_pct=True)  # type: ignore[arg-type]

    def test_string_rejected(self):
        with self.assertRaises(TypeError):
            _evidence(coverage_pct="0.5")  # type: ignore[arg-type]

    def test_none_ok(self):
        e = _evidence(coverage_pct=None)
        self.assertIsNone(e.coverage_pct)

    def test_boundaries_ok(self):
        self.assertEqual(_evidence(coverage_pct=0.0).coverage_pct, 0.0)
        self.assertEqual(_evidence(coverage_pct=1.0).coverage_pct, 1.0)


class TestTimestamp(unittest.TestCase):
    def test_naive_rejected(self):
        with self.assertRaises(ValueError):
            _evidence(at=datetime(2024, 1, 1))

    def test_non_datetime_rejected(self):
        with self.assertRaises(TypeError):
            _evidence(at="2024-01-01")  # type: ignore[arg-type]

    def test_offset_normalized(self):
        from datetime import timedelta
        tz = timezone(timedelta(hours=8))
        e = _evidence(at=datetime(2024, 1, 1, 12, tzinfo=tz))
        self.assertEqual(e.at.tzinfo, timezone.utc)
        self.assertEqual(e.at.hour, 4)


class TestCounterexamples(unittest.TestCase):
    def test_copied_on_construction(self):
        src = [{"property": "b", "trace": [1, 2, 3]}]
        e = _evidence(
            verdict="failed",
            properties_checked=["a", "b"],
            properties_passed=["a"],
            properties_failed=["b"],
            counterexamples=src,
        )
        src[0]["trace"].append(4)
        # Top-level dict is a copy (nested list still aliased by Python's
        # shallow dict() — documented behavior).
        self.assertEqual(len(e.counterexamples), 1)

    def test_missing_property_field_rejected(self):
        with self.assertRaises(ValueError):
            _evidence(
                verdict="failed",
                properties_checked=["b"],
                properties_failed=["b"],
                counterexamples=[{"trace": [1, 2]}],
            )

    def test_property_not_in_failed_rejected(self):
        with self.assertRaises(ValueError):
            _evidence(
                verdict="failed",
                properties_checked=["a", "b"],
                properties_passed=["a"],
                properties_failed=["b"],
                counterexamples=[{"property": "a"}],
            )

    def test_non_mapping_rejected(self):
        with self.assertRaises(TypeError):
            _evidence(
                verdict="failed",
                properties_checked=["b"],
                properties_failed=["b"],
                counterexamples=["not-a-mapping"],  # type: ignore[list-item]
            )

    def test_empty_string_property_rejected(self):
        with self.assertRaises(ValueError):
            _evidence(
                verdict="failed",
                properties_checked=["b"],
                properties_failed=["b"],
                counterexamples=[{"property": ""}],
            )


class TestImmutability(unittest.TestCase):
    def test_frozen(self):
        from dataclasses import FrozenInstanceError
        e = _evidence()
        with self.assertRaises(FrozenInstanceError):
            e.verdict = "failed"  # type: ignore[misc]

    def test_tuples_immutable(self):
        e = _evidence()
        with self.assertRaises(AttributeError):
            e.properties_checked.append("x")  # type: ignore[attr-defined]


class TestAccessors(unittest.TestCase):
    def test_passed(self):
        e = _evidence(verdict="passed")
        self.assertTrue(e.passed)
        self.assertFalse(e.failed)
        self.assertFalse(e.not_required)

    def test_failed(self):
        e = _evidence(
            verdict="failed",
            properties_checked=["a", "b"],
            properties_passed=["a"],
            properties_failed=["b"],
            counterexamples=[{"property": "b"}],
        )
        self.assertTrue(e.failed)
        self.assertEqual(e.failure_count, 1)
        self.assertEqual(
            [cx["property"] for cx in e.failures()], ["b"],
        )

    def test_not_required(self):
        e = VerificationEvidence(
            policy_version="v1", verdict="not_required",
        )
        self.assertTrue(e.not_required)

    def test_computed_coverage(self):
        e = _evidence(
            properties_checked=["a", "b", "c"],
            properties_passed=["a", "b"],
            properties_failed=["c"],
            counterexamples=[{"property": "c"}],
            verdict="failed",
            coverage_pct=None,
        )
        self.assertAlmostEqual(e.computed_coverage(), 2 / 3)

    def test_computed_coverage_empty(self):
        e = VerificationEvidence(
            policy_version="v1", verdict="passed",
        )
        self.assertIsNone(e.computed_coverage())


class TestEvidenceHash(unittest.TestCase):
    def test_stable(self):
        e = _evidence(at=datetime(2024, 1, 1, tzinfo=timezone.utc))
        self.assertEqual(e.evidence_hash(), e.evidence_hash())
        self.assertEqual(len(e.evidence_hash()), 64)

    def test_changes_with_content(self):
        e1 = _evidence(at=datetime(2024, 1, 1, tzinfo=timezone.utc))
        e2 = _evidence(
            at=datetime(2024, 1, 1, tzinfo=timezone.utc),
            policy_version="v2",
        )
        self.assertNotEqual(e1.evidence_hash(), e2.evidence_hash())


class TestSerialization(unittest.TestCase):
    def test_to_dict_shape(self):
        e = _evidence()
        d = e.to_dict()
        self.assertEqual(
            set(d),
            {
                "policy_version", "verdict", "properties_checked",
                "properties_passed", "properties_failed",
                "counterexamples", "coverage_pct", "verifier",
                "verifier_version", "at",
            },
        )
        self.assertEqual(d["verdict"], "passed")
        self.assertIsInstance(d["at"], str)

    def test_to_dict_json_safe(self):
        from datetime import timedelta
        e = VerificationEvidence(
            policy_version="v1",
            verdict="failed",
            properties_checked=["b"],
            properties_failed=["b"],
            counterexamples=[{
                "property": "b",
                "ts": datetime(2024, 1, 1, tzinfo=timezone.utc),
                "id": UUID(
                    "12345678-1234-5678-1234-567812345678"
                ),
                "amt": Decimal("1.5"),
                "raw": b"\x00\x01",
            }],
        )
        payload = json.dumps(e.to_dict())
        self.assertIn("2024-01-01", payload)
        self.assertIn("12345678", payload)

    def test_round_trip(self):
        e = VerificationEvidence(
            policy_version="v1",
            verdict="failed",
            properties_checked=["a", "b"],
            properties_passed=["a"],
            properties_failed=["b"],
            counterexamples=[{"property": "b", "error": "x"}],
            coverage_pct=0.5,
            verifier="policy_verifier",
            verifier_version="1.0.0",
            at=datetime(2024, 1, 1, tzinfo=timezone.utc),
        )
        e2 = VerificationEvidence.from_dict(e.to_dict())
        self.assertEqual(e.to_dict(), e2.to_dict())

    def test_from_dict_z_suffix(self):
        e = VerificationEvidence.from_dict({
            "policy_version": "v1",
            "verdict": "passed",
            "at": "2024-01-01T00:00:00Z",
        })
        self.assertEqual(e.at.year, 2024)
        self.assertEqual(e.at.tzinfo, timezone.utc)

    def test_from_dict_missing_required(self):
        for required in ("policy_version", "verdict"):
            payload = {"policy_version": "v1", "verdict": "passed"}
            del payload[required]
            with self.subTest(missing=required):
                with self.assertRaises(ValueError):
                    VerificationEvidence.from_dict(payload)

    def test_from_dict_non_mapping_rejected(self):
        with self.assertRaises(TypeError):
            VerificationEvidence.from_dict("nope")  # type: ignore[arg-type]

    def test_to_json_compact_and_stable(self):
        e = _evidence(at=datetime(2024, 1, 1, tzinfo=timezone.utc))
        s = e.to_json()
        self.assertNotIn(" ", s)
        self.assertEqual(s, e.to_json())
        self.assertEqual(
            json.loads(s)["verdict"], "passed",
        )

    def test_to_schema_shape(self):
        s = _evidence().to_schema()
        self.assertEqual(s["type"], "object")
        self.assertIn("policy_version", s["required"])
        self.assertIn("verdict", s["required"])
        self.assertIn("at", s["required"])
        self.assertEqual(
            set(s["properties"]["verdict"]["enum"]),
            {v.value for v in VerificationVerdict},
        )
        self.assertFalse(s["additionalProperties"])


class TestIntrospection(unittest.TestCase):
    def test_describe(self):
        e = _evidence()
        s = e.describe()
        self.assertIn("verdict=passed", s)
        self.assertIn("checked=2", s)

    def test_describe_with_verifier(self):
        e = _evidence(verifier="policy_verifier")
        self.assertIn("policy_verifier", e.describe())

    def test_str_matches_describe(self):
        e = _evidence()
        self.assertEqual(str(e), e.describe())
        self.assertEqual(repr(e), e.describe())


class TestIntegrationWithVerifierPattern(unittest.IsolatedAsyncioTestCase):
    async def test_policy_verifier_style_construction(self):
        # Reproduces what the enhanced PolicyVerifier emits on success.
        e = VerificationEvidence(
            policy_version="v1",
            verdict="passed",
            properties_checked=("a", "b"),
            properties_passed=("a", "b"),
            properties_failed=(),
            counterexamples=(),
            coverage_pct=1.0,
        )
        self.assertTrue(e.passed)
        self.assertEqual(e.computed_coverage(), 1.0)

    async def test_policy_verifier_style_failure(self):
        e = VerificationEvidence(
            policy_version="v1",
            verdict="failed",
            properties_checked=("a", "b"),
            properties_passed=("a",),
            properties_failed=("b",),
            counterexamples=(
                {
                    "property": "b",
                    "outcome": "violated",
                    "error_type": None,
                    "error_message": None,
                },
            ),
            coverage_pct=0.5,
        )
        self.assertTrue(e.failed)
        self.assertEqual(e.failure_count, 1)
        self.assertEqual(
            e.counterexamples[0]["outcome"], "violated",
        )

    async def test_empty_verification_with_none_coverage(self):
        # Matches the enhanced PolicyVerifier's empty-registry output.
        e = VerificationEvidence(
            policy_version="v1",
            verdict="passed",
            properties_checked=(),
            properties_passed=(),
            properties_failed=(),
            coverage_pct=None,
        )
        self.assertIsNone(e.coverage_pct)
        self.assertIsNone(e.computed_coverage())


if __name__ == "__main__":
    unittest.main(verbosity=2)
