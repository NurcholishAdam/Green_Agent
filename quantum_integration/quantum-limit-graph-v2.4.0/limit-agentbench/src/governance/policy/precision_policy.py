"""
PrecisionPolicy — approved precisions, quality floors, thermal limits.

A rule-based governance policy that inspects a :class:`DecisionRequest`
and its :class:`RiskClassification` and returns a
:class:`PolicyDecision` telling the PDP whether to ``ALLOW`` or
``DENY``. Four independent rules:

* ``approved_precisions``: allowed precision labels.
* ``min_quality_per_precision``: per-precision quality floors.
* ``max_thermal_c``: thermal ceiling.
* ``max_memory_mb``: memory ceiling.

Key guarantees
--------------
* **Fail-closed.** Every numeric context value is validated before
  use: ``NaN``, ``Inf``, ``bool``, and wrong-typed values (e.g. the
  string ``"95"``) are **denied** with an ``INVALID_CONTEXT`` reason
  code rather than silently skipped. An empty ``precision=""`` is
  denied rather than treated as "no check needed".
* **Missing quality when a floor exists denies.** If the policy
  declares a floor for a precision, a submission that declares the
  precision but omits ``expected_quality`` is **denied** with
  ``QUALITY_MISSING``. A floor that cannot be verified cannot be
  satisfied.
* **Immutable.** ``frozen=True``; ``approved_precisions`` is a
  ``FrozenSet``; ``min_quality_per_precision`` is a
  ``MappingProxyType`` wrapping a copy of the caller's mapping.
* **Validated.** ``__post_init__`` rejects empty names/versions,
  empty precision strings, quality floors for unapproved precisions,
  negative or non-finite floors, and negative or non-finite ceilings.
* **Structured reasons.** Every failure is prefixed with a
  ``ReasonCode`` value (e.g. ``"[thermal_exceeded] thermal 95.0C …"``)
  so downstream tooling can filter without string-parsing.
* **Round-trippable.** ``to_dict`` / ``from_dict`` / ``to_schema``
  match the conventions of the rest of the governance stack.

Note on ``policy_version``
--------------------------
When this policy is used inside a :class:`PolicyDecisionPoint`, the
PDP stamps its own ``policy_version`` on the final decision — this
field is overwritten. It matters only for standalone use, and for
``to_dict()`` round-trips.

Note on immutability
--------------------
``min_quality_per_precision`` is wrapped in
:class:`types.MappingProxyType`, which prevents mutation but makes
the dataclass unhashable. This is intentional: a policy object is
never used as a dict key or stored in a set; callers that need a
stable identifier should use ``id(policy)`` or the ``name`` field.
"""

from __future__ import annotations

import logging
import math
import numbers
import unittest
from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType
from typing import (
    Any,
    Dict,
    FrozenSet,
    List,
    Mapping,
    Optional,
    Tuple,
    Union,
)

# ---------------------------------------------------------------------------
# Real imports with standalone stubs
# ---------------------------------------------------------------------------
try:  # pragma: no cover
    from ..contracts.decision_request import DecisionRequest  # type: ignore
    from ..contracts.policy_decision import (  # type: ignore
        DecisionVerdict,
        PolicyDecision,
    )
    from ..contracts.risk_classification import (  # type: ignore
        RiskClassification,
        RiskLevel,
    )
except Exception:  # pragma: no cover — standalone / test mode
    import uuid as _uuid
    from datetime import datetime as _dt, timezone as _tz

    class RiskLevel(str, Enum):
        LOW = "low"
        MEDIUM = "medium"
        HIGH = "high"
        CRITICAL = "critical"

        @property
        def rank(self) -> int:
            return _RISK_RANK[self.value]

    _RISK_RANK = {r.value: i for i, r in enumerate(RiskLevel)}

    @dataclass(frozen=True)
    class RiskClassification:
        level: RiskLevel = RiskLevel.LOW
        factors: Tuple[str, ...] = ()

        def __post_init__(self):
            if not isinstance(self.level, RiskLevel):
                object.__setattr__(self, "level", RiskLevel(self.level))

    @dataclass(frozen=True)
    class DecisionRequest:
        action: str
        request_id: str = field(default_factory=lambda: _uuid.uuid4().hex)
        agent_id: Optional[str] = None
        task_id: Optional[str] = None
        run_id: Optional[str] = None
        policy_version: Optional[str] = None
        context: Dict[str, Any] = field(default_factory=dict)
        requested_at: _dt = field(default_factory=lambda: _dt.now(_tz.utc))

        def __post_init__(self):
            if not isinstance(self.action, str) or not self.action.strip():
                raise ValueError("action must be non-empty")
            if not isinstance(self.request_id, str) or not self.request_id.strip():
                raise ValueError("request_id must be non-empty")
            object.__setattr__(self, "context", dict(self.context))

        def to_dict(self):
            return {
                "action": self.action,
                "request_id": self.request_id,
                "context": dict(self.context),
            }

    class DecisionVerdict(str, Enum):
        ALLOW = "allow"
        DENY = "deny"
        MODIFY = "modify"
        ESCALATE = "escalate"
        FALLBACK = "fallback"

    @dataclass(frozen=True)
    class PolicyDecision:
        request_id: str
        verdict: DecisionVerdict
        policy_version: str
        reasons: List[str] = field(default_factory=list)
        obligations: List[str] = field(default_factory=list)
        risk_level: RiskLevel = RiskLevel.LOW
        verification_status: str = "not_required"
        human_approval_required: bool = False
        audit_event_id: str = ""
        modified_request: Optional[Dict[str, Any]] = None
        issued_at: _dt = field(default_factory=lambda: _dt.now(_tz.utc))
        expires_at: Optional[_dt] = None

        def __post_init__(self):
            if not isinstance(self.verdict, DecisionVerdict):
                object.__setattr__(self, "verdict", DecisionVerdict(self.verdict))
            if not isinstance(self.risk_level, RiskLevel):
                object.__setattr__(self, "risk_level", RiskLevel(self.risk_level))
            object.__setattr__(self, "reasons", list(self.reasons))
            object.__setattr__(self, "obligations", list(self.obligations))
            if self.modified_request is not None:
                object.__setattr__(self, "modified_request", dict(self.modified_request))
            if self.verdict is DecisionVerdict.MODIFY and self.modified_request is None:
                raise ValueError("MODIFY requires modified_request")
            if self.verdict is not DecisionVerdict.MODIFY and self.modified_request is not None:
                raise ValueError("modified_request only valid with MODIFY")
            if self.verdict is DecisionVerdict.ALLOW and self.human_approval_required:
                raise ValueError("ALLOW cannot require human approval")
            if self.verdict is DecisionVerdict.DENY and self.human_approval_required:
                raise ValueError("DENY cannot require human approval")


logger = logging.getLogger(__name__)

__all__ = [
    "ReasonCode",
    "PrecisionPolicy",
    "DEFAULT_PRECISION_POLICY",
]


# ---------------------------------------------------------------------------
# Reason codes
# ---------------------------------------------------------------------------
class ReasonCode(str, Enum):
    """Machine-readable codes emitted as the prefix of each reason."""

    PRECISION_NOT_APPROVED = "precision_not_approved"
    QUALITY_BELOW_FLOOR = "quality_below_floor"
    QUALITY_MISSING = "quality_missing"
    THERMAL_EXCEEDED = "thermal_exceeded"
    MEMORY_EXCEEDED = "memory_exceeded"
    INVALID_CONTEXT = "invalid_context"


# ---------------------------------------------------------------------------
# Context helpers
# ---------------------------------------------------------------------------
def _get_numeric(
    ctx: Mapping[str, Any], key: str,
) -> Tuple[Optional[float], Optional[ReasonCode], Optional[str]]:
    """Return ``(value, code, message)``.

    * absent          → ``(None, None, None)``
    * wrong type/bool → ``(None, INVALID_CONTEXT, message)``
    * NaN / Inf       → ``(None, INVALID_CONTEXT, message)``
    * valid           → ``(float(value), None, None)``
    """
    if key not in ctx:
        return (None, None, None)
    raw = ctx[key]
    if isinstance(raw, bool) or not isinstance(raw, numbers.Real):
        return (
            None,
            ReasonCode.INVALID_CONTEXT,
            f"context[{key!r}] must be a number, got {type(raw).__name__}",
        )
    try:
        value = float(raw)
    except (TypeError, ValueError, OverflowError):
        return (
            None,
            ReasonCode.INVALID_CONTEXT,
            f"context[{key!r}] could not be converted to float",
        )
    if not math.isfinite(value):
        return (
            None,
            ReasonCode.INVALID_CONTEXT,
            f"context[{key!r}] must be finite, got {value!r}",
        )
    return (value, None, None)


def _format(code: ReasonCode, message: str) -> str:
    return f"[{code.value}] {message}"


# ---------------------------------------------------------------------------
# Policy
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class PrecisionPolicy:
    """
    Rule-based precision policy.

    Enforces:

    * ``approved_precisions``: set of allowed precision labels.
    * ``min_quality_per_precision``: per-precision quality floors.
    * ``max_thermal_c``: thermal ceiling.
    * ``max_memory_mb``: memory ceiling.

    Every numeric context value is validated before comparison; invalid
    values (``NaN``, ``Inf``, ``bool``, strings, ``None`` explicitly
    set) are **denied** rather than skipped.
    """

    approved_precisions: FrozenSet[str] = frozenset(
        {"fp32", "fp16", "bf16", "int8"}
    )
    min_quality_per_precision: Mapping[str, float] = field(
        default_factory=dict,
    )
    max_thermal_c: Optional[float] = None
    max_memory_mb: Optional[float] = None
    name: str = "precision"
    policy_version: str = "v5.0.0"

    # -- invariants --------------------------------------------------------
    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name.strip():
            raise ValueError("name must be a non-empty string")
        if (
            not isinstance(self.policy_version, str)
            or not self.policy_version.strip()
        ):
            raise ValueError("policy_version must be a non-empty string")

        # -- approved_precisions --------------------------------------
        if isinstance(self.approved_precisions, str):
            raise TypeError(
                "approved_precisions must be an iterable of strings, "
                "not a single string; wrap in a list"
            )
        try:
            precisions = frozenset(self.approved_precisions)
        except TypeError:
            raise TypeError(
                "approved_precisions must be iterable of strings"
            ) from None
        for p in precisions:
            if not isinstance(p, str) or not p.strip():
                raise ValueError(
                    f"approved_precisions entries must be non-empty "
                    f"strings, got {p!r}"
                )
        if not precisions:
            raise ValueError(
                "approved_precisions must contain at least one precision"
            )
        object.__setattr__(self, "approved_precisions", precisions)

        # -- min_quality_per_precision --------------------------------
        if not isinstance(self.min_quality_per_precision, Mapping):
            raise TypeError(
                "min_quality_per_precision must be a Mapping"
            )
        floors: Dict[str, float] = {}
        for key, value in self.min_quality_per_precision.items():
            if not isinstance(key, str) or not key.strip():
                raise ValueError(
                    f"min_quality_per_precision keys must be "
                    f"non-empty strings, got {key!r}"
                )
            if key not in precisions:
                raise ValueError(
                    f"min_quality_per_precision declares a floor for "
                    f"unapproved precision {key!r}; add it to "
                    f"approved_precisions or remove the floor"
                )
            if isinstance(value, bool) or not isinstance(value, numbers.Real):
                raise TypeError(
                    f"min_quality_per_precision[{key!r}] must be "
                    f"numeric, got {type(value).__name__}"
                )
            fv = float(value)
            if not math.isfinite(fv):
                raise ValueError(
                    f"min_quality_per_precision[{key!r}] must be "
                    f"finite, got {fv!r}"
                )
            if fv < 0:
                raise ValueError(
                    f"min_quality_per_precision[{key!r}] must be "
                    f">= 0, got {fv!r}"
                )
            floors[key] = fv
        object.__setattr__(
            self,
            "min_quality_per_precision",
            MappingProxyType(floors),
        )

        # -- ceilings -------------------------------------------------
        for label in ("max_thermal_c", "max_memory_mb"):
            value = getattr(self, label)
            if value is None:
                continue
            if isinstance(value, bool) or not isinstance(value, numbers.Real):
                raise TypeError(
                    f"{label} must be numeric or None, got "
                    f"{type(value).__name__}"
                )
            fv = float(value)
            if not math.isfinite(fv):
                raise ValueError(f"{label} must be finite, got {fv!r}")
            if fv < 0:
                raise ValueError(f"{label} must be >= 0, got {fv!r}")
            object.__setattr__(self, label, fv)

    # -- evaluation --------------------------------------------------------
    def evaluate(
        self,
        request: DecisionRequest,
        risk: RiskClassification,
    ) -> PolicyDecision:
        if not hasattr(request, "context") or not hasattr(
            request, "request_id"
        ):
            raise TypeError(
                "request must be a DecisionRequest-like object with "
                "context and request_id"
            )
        if not hasattr(risk, "level"):
            raise TypeError(
                "risk must be a RiskClassification-like object with "
                "level"
            )

        ctx = request.context or {}
        failures: List[Tuple[ReasonCode, str]] = []

        # -- Rule 1 + 2: precision approval + quality floor ------------
        if "precision" in ctx:
            precision_raw = ctx["precision"]
            if (
                not isinstance(precision_raw, str)
                or not precision_raw.strip()
            ):
                failures.append((
                    ReasonCode.INVALID_CONTEXT,
                    f"context['precision'] must be a non-empty string, "
                    f"got {precision_raw!r}",
                ))
            elif precision_raw not in self.approved_precisions:
                failures.append((
                    ReasonCode.PRECISION_NOT_APPROVED,
                    f"precision {precision_raw!r} not in approved set "
                    f"{sorted(self.approved_precisions)}",
                ))
            else:
                floor = self.min_quality_per_precision.get(precision_raw)
                if floor is not None:
                    expected, code, message = _get_numeric(
                        ctx, "expected_quality",
                    )
                    if code is not None:
                        failures.append((code, message))
                    elif expected is None:
                        failures.append((
                            ReasonCode.QUALITY_MISSING,
                            f"precision {precision_raw!r} requires "
                            f"quality >= {floor}, but "
                            f"'expected_quality' is missing",
                        ))
                    elif expected < floor:
                        failures.append((
                            ReasonCode.QUALITY_BELOW_FLOOR,
                            f"precision {precision_raw!r} requires "
                            f"quality >= {floor}, got {expected}",
                        ))

        # -- Rule 3: thermal ceiling -----------------------------------
        if self.max_thermal_c is not None:
            thermal, code, message = _get_numeric(ctx, "thermal_c")
            if code is not None:
                failures.append((code, message))
            elif thermal is not None and thermal > self.max_thermal_c:
                failures.append((
                    ReasonCode.THERMAL_EXCEEDED,
                    f"thermal {thermal:.1f}C exceeds ceiling "
                    f"{self.max_thermal_c:.1f}C",
                ))

        # -- Rule 4: memory ceiling ------------------------------------
        if self.max_memory_mb is not None:
            memory, code, message = _get_numeric(ctx, "memory_mb")
            if code is not None:
                failures.append((code, message))
            elif memory is not None and memory > self.max_memory_mb:
                failures.append((
                    ReasonCode.MEMORY_EXCEEDED,
                    f"memory {memory:.1f}MB exceeds ceiling "
                    f"{self.max_memory_mb:.1f}MB",
                ))

        # -- Assemble --------------------------------------------------
        verdict = (
            DecisionVerdict.DENY if failures else DecisionVerdict.ALLOW
        )
        reasons: List[str] = (
            [_format(c, m) for c, m in failures]
            if failures
            else ["no precision objections"]
        )

        # The obligation is always attached — the audit trail should
        # reflect the requested precision regardless of the verdict.
        obligations = ["record_precision_choice"]

        human_approval_required = verdict is DecisionVerdict.ESCALATE
        if (
            verdict is DecisionVerdict.ALLOW
            and human_approval_required
        ):  # pragma: no cover — impossible by construction
            raise RuntimeError(
                "internal invariant violated: ALLOW with "
                "human_approval_required=True"
            )

        return PolicyDecision(
            request_id=request.request_id,
            verdict=verdict.value,
            policy_version=self.policy_version,
            reasons=reasons,
            obligations=obligations,
            risk_level=risk.level,
            human_approval_required=human_approval_required,
        )

    # -- introspection -----------------------------------------------------
    def describe(self) -> str:
        parts: List[str] = [f"name={self.name!r}"]
        parts.append(
            f"approved={sorted(self.approved_precisions)}"
        )
        if self.min_quality_per_precision:
            floors = {
                k: self.min_quality_per_precision[k]
                for k in sorted(self.min_quality_per_precision)
            }
            parts.append(f"quality_floors={floors}")
        if self.max_thermal_c is not None:
            parts.append(f"max_thermal={self.max_thermal_c}C")
        if self.max_memory_mb is not None:
            parts.append(f"max_memory={self.max_memory_mb}MB")
        return f"PrecisionPolicy({', '.join(parts)})"

    def __str__(self) -> str:
        return self.describe()

    def __repr__(self) -> str:
        return self.describe()

    # -- serialization -----------------------------------------------------
    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "policy_version": self.policy_version,
            "approved_precisions": sorted(self.approved_precisions),
            "min_quality_per_precision": dict(
                self.min_quality_per_precision
            ),
            "max_thermal_c": self.max_thermal_c,
            "max_memory_mb": self.max_memory_mb,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "PrecisionPolicy":
        if not isinstance(data, Mapping):
            raise TypeError(
                f"data must be a Mapping, got {type(data).__name__}"
            )
        return cls(
            approved_precisions=tuple(
                data.get(
                    "approved_precisions",
                    ("fp32", "fp16", "bf16", "int8"),
                )
            ),
            min_quality_per_precision=dict(
                data.get("min_quality_per_precision", {})
            ),
            max_thermal_c=data.get("max_thermal_c"),
            max_memory_mb=data.get("max_memory_mb"),
            name=data.get("name", "precision"),
            policy_version=data.get("policy_version", "v5.0.0"),
        )

    def to_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "required": [
                "name",
                "policy_version",
                "approved_precisions",
            ],
            "properties": {
                "name": {"type": "string", "minLength": 1},
                "policy_version": {"type": "string", "minLength": 1},
                "approved_precisions": {
                    "type": "array",
                    "minItems": 1,
                    "items": {"type": "string", "minLength": 1},
                },
                "min_quality_per_precision": {
                    "type": "object",
                    "additionalProperties": {
                        "type": "number",
                        "minimum": 0,
                    },
                },
                "max_thermal_c": {
                    "type": ["number", "null"],
                    "minimum": 0,
                },
                "max_memory_mb": {
                    "type": ["number", "null"],
                    "minimum": 0,
                },
            },
            "additionalProperties": False,
        }


# ---------------------------------------------------------------------------
# Convenience singleton
# ---------------------------------------------------------------------------
DEFAULT_PRECISION_POLICY: PrecisionPolicy = PrecisionPolicy()


# ===========================================================================
# TESTS
# ===========================================================================
def _request(context: Optional[Dict[str, Any]] = None) -> DecisionRequest:
    return DecisionRequest(
        action="run_inference",
        context=context or {},
    )


def _risk(level: Union[str, RiskLevel] = "low") -> RiskClassification:
    return RiskClassification(level=level)


class TestConstruction(unittest.TestCase):
    def test_defaults(self):
        p = PrecisionPolicy()
        self.assertEqual(
            p.approved_precisions,
            frozenset({"fp32", "fp16", "bf16", "int8"}),
        )
        self.assertEqual(dict(p.min_quality_per_precision), {})
        self.assertIsNone(p.max_thermal_c)
        self.assertIsNone(p.max_memory_mb)
        self.assertEqual(p.name, "precision")
        self.assertEqual(p.policy_version, "v5.0.0")

    def test_frozen(self):
        from dataclasses import FrozenInstanceError
        p = PrecisionPolicy()
        with self.assertRaises(FrozenInstanceError):
            p.max_thermal_c = 100.0  # type: ignore[misc]

    def test_empty_name_rejected(self):
        with self.assertRaises(ValueError):
            PrecisionPolicy(name="")
        with self.assertRaises(ValueError):
            PrecisionPolicy(name="   ")

    def test_empty_policy_version_rejected(self):
        with self.assertRaises(ValueError):
            PrecisionPolicy(policy_version="")

    def test_single_string_precisions_rejected(self):
        with self.assertRaises(TypeError):
            PrecisionPolicy(approved_precisions="fp16")  # type: ignore[arg-type]

    def test_empty_precision_entry_rejected(self):
        with self.assertRaises(ValueError):
            PrecisionPolicy(approved_precisions={"fp16", ""})

    def test_empty_precision_set_rejected(self):
        with self.assertRaises(ValueError):
            PrecisionPolicy(approved_precisions=frozenset())

    def test_precisions_coerced_to_frozenset(self):
        p = PrecisionPolicy(approved_precisions=["fp32", "fp16"])
        self.assertIsInstance(p.approved_precisions, frozenset)
        self.assertEqual(p.approved_precisions, frozenset({"fp32", "fp16"}))

    def test_quality_floor_for_unapproved_precision_rejected(self):
        with self.assertRaises(ValueError):
            PrecisionPolicy(
                approved_precisions={"fp16"},
                min_quality_per_precision={"fp32": 0.9},
            )

    def test_quality_floor_negative_rejected(self):
        with self.assertRaises(ValueError):
            PrecisionPolicy(
                min_quality_per_precision={"fp16": -0.1},
            )

    def test_quality_floor_nan_rejected(self):
        with self.assertRaises(ValueError):
            PrecisionPolicy(
                min_quality_per_precision={"fp16": float("nan")},
            )

    def test_quality_floor_inf_rejected(self):
        with self.assertRaises(ValueError):
            PrecisionPolicy(
                min_quality_per_precision={"fp16": float("inf")},
            )

    def test_quality_floor_string_rejected(self):
        with self.assertRaises(TypeError):
            PrecisionPolicy(
                min_quality_per_precision={"fp16": "0.9"},  # type: ignore[dict-item]
            )

    def test_quality_floor_bool_rejected(self):
        with self.assertRaises(TypeError):
            PrecisionPolicy(
                min_quality_per_precision={"fp16": True},  # type: ignore[dict-item]
            )

    def test_quality_map_is_read_only(self):
        p = PrecisionPolicy(min_quality_per_precision={"fp16": 0.9})
        with self.assertRaises(TypeError):
            p.min_quality_per_precision["fp16"] = 0.0  # type: ignore[index]

    def test_negative_thermal_rejected(self):
        with self.assertRaises(ValueError):
            PrecisionPolicy(max_thermal_c=-1.0)

    def test_nan_thermal_rejected(self):
        with self.assertRaises(ValueError):
            PrecisionPolicy(max_thermal_c=float("nan"))

    def test_negative_memory_rejected(self):
        with self.assertRaises(ValueError):
            PrecisionPolicy(max_memory_mb=-100.0)

    def test_nan_memory_rejected(self):
        with self.assertRaises(ValueError):
            PrecisionPolicy(max_memory_mb=float("nan"))

    def test_caller_set_not_aliased(self):
        src = {"fp32"}
        p = PrecisionPolicy(approved_precisions=src)
        src.add("bogus")
        self.assertEqual(p.approved_precisions, frozenset({"fp32"}))

    def test_caller_dict_not_aliased(self):
        src = {"fp16": 0.9}
        p = PrecisionPolicy(min_quality_per_precision=src)
        src["fp16"] = 0.0
        self.assertEqual(p.min_quality_per_precision["fp16"], 0.9)

    def test_default_singleton_valid(self):
        self.assertIsInstance(
            DEFAULT_PRECISION_POLICY, PrecisionPolicy,
        )


class TestEvaluatePrecisionApproval(unittest.TestCase):
    def test_approved_precision_allows(self):
        p = PrecisionPolicy()
        d = p.evaluate(_request({"precision": "fp16"}), _risk())
        self.assertEqual(d.verdict, "allow")

    def test_unapproved_precision_denies(self):
        p = PrecisionPolicy()
        d = p.evaluate(_request({"precision": "fp64"}), _risk())
        self.assertEqual(d.verdict, "deny")
        self.assertTrue(
            any("[precision_not_approved]" in r for r in d.reasons)
        )

    def test_missing_precision_skips_check(self):
        p = PrecisionPolicy()
        d = p.evaluate(_request({}), _risk())
        self.assertEqual(d.verdict, "allow")

    def test_empty_precision_denies(self):
        p = PrecisionPolicy()
        d = p.evaluate(_request({"precision": ""}), _risk())
        self.assertEqual(d.verdict, "deny")
        self.assertTrue(
            any("[invalid_context]" in r for r in d.reasons)
        )

    def test_non_string_precision_denies(self):
        p = PrecisionPolicy()
        d = p.evaluate(_request({"precision": 123}), _risk())
        self.assertEqual(d.verdict, "deny")
        self.assertTrue(
            any("[invalid_context]" in r for r in d.reasons)
        )


class TestEvaluateQualityFloors(unittest.TestCase):
    def _policy(self) -> PrecisionPolicy:
        return PrecisionPolicy(
            approved_precisions={"fp16"},
            min_quality_per_precision={"fp16": 0.9},
        )

    def test_quality_at_floor_allows(self):
        d = self._policy().evaluate(
            _request({"precision": "fp16", "expected_quality": 0.9}),
            _risk(),
        )
        self.assertEqual(d.verdict, "allow")

    def test_quality_below_floor_denies(self):
        d = self._policy().evaluate(
            _request({"precision": "fp16", "expected_quality": 0.5}),
            _risk(),
        )
        self.assertEqual(d.verdict, "deny")
        self.assertTrue(
            any("[quality_below_floor]" in r for r in d.reasons)
        )

    def test_quality_missing_denies(self):
        # Regression: original silently skipped.
        d = self._policy().evaluate(
            _request({"precision": "fp16"}), _risk(),
        )
        self.assertEqual(d.verdict, "deny")
        self.assertTrue(
            any("[quality_missing]" in r for r in d.reasons)
        )

    def test_quality_string_denies(self):
        # Regression: original silently skipped.
        d = self._policy().evaluate(
            _request({
                "precision": "fp16",
                "expected_quality": "0.95",
            }),
            _risk(),
        )
        self.assertEqual(d.verdict, "deny")
        self.assertTrue(
            any("[invalid_context]" in r for r in d.reasons)
        )

    def test_quality_nan_denies(self):
        d = self._policy().evaluate(
            _request({
                "precision": "fp16",
                "expected_quality": float("nan"),
            }),
            _risk(),
        )
        self.assertEqual(d.verdict, "deny")
        self.assertTrue(
            any("[invalid_context]" in r for r in d.reasons)
        )

    def test_quality_bool_denies(self):
        d = self._policy().evaluate(
            _request({
                "precision": "fp16",
                "expected_quality": True,
            }),
            _risk(),
        )
        self.assertEqual(d.verdict, "deny")
        self.assertTrue(
            any("[invalid_context]" in r for r in d.reasons)
        )

    def test_no_floor_precision_skips_quality(self):
        p = PrecisionPolicy(approved_precisions={"fp16"})
        d = p.evaluate(_request({"precision": "fp16"}), _risk())
        self.assertEqual(d.verdict, "allow")


class TestEvaluateThermalCeiling(unittest.TestCase):
    def _policy(self) -> PrecisionPolicy:
        return PrecisionPolicy(max_thermal_c=80.0)

    def test_thermal_at_ceiling_allows(self):
        d = self._policy().evaluate(
            _request({"thermal_c": 80.0}), _risk(),
        )
        self.assertEqual(d.verdict, "allow")

    def test_thermal_above_ceiling_denies(self):
        d = self._policy().evaluate(
            _request({"thermal_c": 95.0}), _risk(),
        )
        self.assertEqual(d.verdict, "deny")
        self.assertTrue(
            any("[thermal_exceeded]" in r for r in d.reasons)
        )

    def test_thermal_missing_allows(self):
        d = self._policy().evaluate(_request({}), _risk())
        self.assertEqual(d.verdict, "allow")

    def test_thermal_nan_denies(self):
        # Regression: original silently passed.
        d = self._policy().evaluate(
            _request({"thermal_c": float("nan")}), _risk(),
        )
        self.assertEqual(d.verdict, "deny")
        self.assertTrue(
            any("[invalid_context]" in r for r in d.reasons)
        )

    def test_thermal_string_denies(self):
        # Regression: original silently passed.
        d = self._policy().evaluate(
            _request({"thermal_c": "95"}), _risk(),
        )
        self.assertEqual(d.verdict, "deny")
        self.assertTrue(
            any("[invalid_context]" in r for r in d.reasons)
        )


class TestEvaluateMemoryCeiling(unittest.TestCase):
    def _policy(self) -> PrecisionPolicy:
        return PrecisionPolicy(max_memory_mb=8192.0)

    def test_memory_at_ceiling_allows(self):
        d = self._policy().evaluate(
            _request({"memory_mb": 8192.0}), _risk(),
        )
        self.assertEqual(d.verdict, "allow")

    def test_memory_above_ceiling_denies(self):
        d = self._policy().evaluate(
            _request({"memory_mb": 16384.0}), _risk(),
        )
        self.assertEqual(d.verdict, "deny")
        self.assertTrue(
            any("[memory_exceeded]" in r for r in d.reasons)
        )

    def test_memory_missing_allows(self):
        d = self._policy().evaluate(_request({}), _risk())
        self.assertEqual(d.verdict, "allow")

    def test_memory_nan_denies(self):
        d = self._policy().evaluate(
            _request({"memory_mb": float("nan")}), _risk(),
        )
        self.assertEqual(d.verdict, "deny")

    def test_memory_string_denies(self):
        d = self._policy().evaluate(
            _request({"memory_mb": "8192"}), _risk(),
        )
        self.assertEqual(d.verdict, "deny")


class TestEvaluateAggregation(unittest.TestCase):
    def test_all_four_rules_trigger(self):
        p = PrecisionPolicy(
            approved_precisions={"fp16"},
            min_quality_per_precision={"fp16": 0.9},
            max_thermal_c=80.0,
            max_memory_mb=8192.0,
        )
        d = p.evaluate(
            _request({
                "precision": "fp64",
                "thermal_c": 95.0,
                "memory_mb": 16384.0,
            }),
            _risk(),
        )
        self.assertEqual(d.verdict, "deny")
        # 3 distinct failures: precision, thermal, memory
        self.assertEqual(len(d.reasons), 3)

    def test_no_rules_trigger_allows(self):
        p = PrecisionPolicy(
            max_thermal_c=80.0, max_memory_mb=8192.0,
        )
        d = p.evaluate(_request({}), _risk())
        self.assertEqual(d.verdict, "allow")
        self.assertEqual(d.reasons, ["no precision objections"])

    def test_human_approval_required_false_on_allow(self):
        d = PrecisionPolicy().evaluate(_request({}), _risk())
        self.assertFalse(d.human_approval_required)

    def test_human_approval_required_false_on_deny(self):
        p = PrecisionPolicy(max_thermal_c=80.0)
        d = p.evaluate(_request({"thermal_c": 95.0}), _risk())
        self.assertFalse(d.human_approval_required)


class TestObligations(unittest.TestCase):
    def test_record_obligation_present_on_allow(self):
        d = PrecisionPolicy().evaluate(_request({}), _risk())
        self.assertIn("record_precision_choice", d.obligations)

    def test_record_obligation_present_on_deny(self):
        p = PrecisionPolicy(max_thermal_c=80.0)
        d = p.evaluate(_request({"thermal_c": 95.0}), _risk())
        self.assertIn("record_precision_choice", d.obligations)


class TestSerialization(unittest.TestCase):
    def test_round_trip(self):
        p = PrecisionPolicy(
            approved_precisions={"fp16", "int8"},
            min_quality_per_precision={"fp16": 0.9, "int8": 0.7},
            max_thermal_c=80.0,
            max_memory_mb=8192.0,
            name="prod",
            policy_version="v5.1.0",
        )
        p2 = PrecisionPolicy.from_dict(p.to_dict())
        self.assertEqual(p.to_dict(), p2.to_dict())

    def test_from_dict_defaults(self):
        p = PrecisionPolicy.from_dict({})
        self.assertEqual(p.name, "precision")
        self.assertEqual(p.policy_version, "v5.0.0")
        self.assertIsNone(p.max_thermal_c)

    def test_from_dict_non_mapping_rejected(self):
        with self.assertRaises(TypeError):
            PrecisionPolicy.from_dict("nope")  # type: ignore[arg-type]

    def test_to_dict_json_safe(self):
        import json
        p = PrecisionPolicy(
            approved_precisions={"fp16"},
            min_quality_per_precision={"fp16": 0.9},
        )
        json.dumps(p.to_dict())  # must not raise

    def test_to_schema_shape(self):
        s = PrecisionPolicy().to_schema()
        self.assertEqual(s["type"], "object")
        self.assertIn("approved_precisions", s["required"])
        self.assertFalse(s["additionalProperties"])


class TestIntrospection(unittest.TestCase):
    def test_describe_mentions_rules(self):
        p = PrecisionPolicy(
            approved_precisions={"fp16"},
            min_quality_per_precision={"fp16": 0.9},
            max_thermal_c=80.0,
            max_memory_mb=8192.0,
        )
        s = p.describe()
        self.assertIn("fp16", s)
        self.assertIn("quality_floors", s)
        self.assertIn("max_thermal", s)
        self.assertIn("max_memory", s)

    def test_str_matches_describe(self):
        p = PrecisionPolicy()
        self.assertEqual(str(p), p.describe())
        self.assertEqual(repr(p), p.describe())


if __name__ == "__main__":
    unittest.main(verbosity=2)
