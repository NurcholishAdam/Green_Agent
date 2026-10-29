"""
SustainabilityPolicy — quality floors, energy/carbon budgets.

A rule-based governance policy that inspects a
:class:`DecisionRequest` and its :class:`RiskClassification` and
returns a :class:`PolicyDecision`. Three optional budgets:

* ``quality_floor``: minimum ``expected_quality`` the request must
  declare and meet.
* ``energy_budget_wh``: maximum ``expected_energy_wh`` per request.
* ``carbon_budget_g``: maximum ``expected_carbon_g`` per request.

Key guarantees
--------------
* **Fail-closed.** Every numeric context value is validated before
  use: ``NaN``, ``Inf``, ``bool``, and wrong-typed values (e.g. the
  string ``"95"``) are **denied** with an ``INVALID_CONTEXT`` reason
  code. When a budget is configured and the corresponding expected
  value is absent, the request is **denied** with a ``*_MISSING``
  code — an unverifiable budget cannot be satisfied.
* **Consistent defaults.** ``quality_floor``, ``energy_budget_wh``,
  and ``carbon_budget_g`` all default to ``None`` (disabled). A
  budget is only enforced when it is explicitly configured, and then
  the corresponding expected value is required.
* **Immutable.** ``frozen=True``; callers cannot mutate the policy
  after construction.
* **Validated.** ``__post_init__`` rejects negative / non-finite
  budgets, empty ``name`` / ``policy_version``, and wrong-typed
  numeric fields.
* **Boolean-safe.** ``regional_baseline is True`` — the string
  ``"false"`` does not trigger the obligation.
* **Structured reasons.** Every failure is prefixed with a
  :class:`ReasonCode` value (e.g. ``"[energy_exceeded] …"``) so
  downstream tooling can filter without string-parsing.
* **Round-trippable.** ``to_dict`` / ``from_dict`` / ``to_schema``
  match the conventions of the rest of the governance stack.

Note on ``deny_low_quality_low_gain``
-------------------------------------
The original docstring described a rule that was never implemented.
The behaviour it describes — "block a green action that drops quality
below the floor even if energy improves" — is **exactly what the
``quality_floor`` check already does**: the quality floor is enforced
independently of any energy improvement, so a request that trades
quality for energy is denied at the quality gate. No separate rule is
needed; the documentation now says so.

Note on ``policy_version``
--------------------------
When this policy is used inside a :class:`PolicyDecisionPoint`, the
PDP stamps its own ``policy_version`` on the final decision — this
field is overwritten. It matters only for standalone use, and for
``to_dict()`` round-trips.
"""

from __future__ import annotations

import logging
import math
import numbers
import unittest
from dataclasses import dataclass
from enum import Enum
from typing import (
    Any,
    Dict,
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
    from dataclasses import field as _field

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
        request_id: str = _field(default_factory=lambda: _uuid.uuid4().hex)
        agent_id: Optional[str] = None
        task_id: Optional[str] = None
        run_id: Optional[str] = None
        policy_version: Optional[str] = None
        context: Dict[str, Any] = _field(default_factory=dict)
        requested_at: _dt = _field(default_factory=lambda: _dt.now(_tz.utc))

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
        reasons: List[str] = _field(default_factory=list)
        obligations: List[str] = _field(default_factory=list)
        risk_level: RiskLevel = RiskLevel.LOW
        verification_status: str = "not_required"
        human_approval_required: bool = False
        audit_event_id: str = ""
        modified_request: Optional[Dict[str, Any]] = None
        issued_at: _dt = _field(default_factory=lambda: _dt.now(_tz.utc))
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
    "Obligation",
    "SustainabilityPolicy",
    "DEFAULT_SUSTAINABILITY_POLICY",
]


# ---------------------------------------------------------------------------
# Reason codes and obligation vocabulary
# ---------------------------------------------------------------------------
class ReasonCode(str, Enum):
    """Machine-readable codes emitted as the prefix of each reason."""

    QUALITY_BELOW_FLOOR = "quality_below_floor"
    QUALITY_MISSING = "quality_missing"
    ENERGY_EXCEEDED = "energy_exceeded"
    ENERGY_MISSING = "energy_missing"
    CARBON_EXCEEDED = "carbon_exceeded"
    CARBON_MISSING = "carbon_missing"
    INVALID_CONTEXT = "invalid_context"


class Obligation(str, Enum):
    """Recognised obligation identifiers attached to a decision."""

    LOG_ENERGY_TELEMETRY = "log_energy_telemetry"
    ATTACH_REGIONAL_BASELINE = "attach_regional_baseline"


def _format_reason(code: ReasonCode, message: str) -> str:
    return f"[{code.value}] {message}"


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


# ---------------------------------------------------------------------------
# Policy
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class SustainabilityPolicy:
    """
    Rule-based sustainability policy.

    Rules (all optional — a rule fires only when its budget is set):

    * ``quality_floor``: minimum ``expected_quality`` the request
      must declare and meet. Note that this rule already implements
      what the original docstring called ``deny_low_quality_low_gain``
      — a green optimisation that drops quality below the floor is
      denied at the quality gate.
    * ``energy_budget_wh``: maximum ``expected_energy_wh``.
    * ``carbon_budget_g``: maximum ``expected_carbon_g``.

    A rule with a configured budget requires the corresponding
    expected value to be present and valid; the absence of the value
    is a failure, not a pass.
    """

    quality_floor: Optional[float] = None
    energy_budget_wh: Optional[float] = None
    carbon_budget_g: Optional[float] = None
    name: str = "sustainability"
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

        for label in (
            "quality_floor",
            "energy_budget_wh",
            "carbon_budget_g",
        ):
            value = getattr(self, label)
            if value is None:
                continue
            if isinstance(value, bool) or not isinstance(
                value, numbers.Real
            ):
                raise TypeError(
                    f"{label} must be numeric or None, got "
                    f"{type(value).__name__}"
                )
            fv = float(value)
            if not math.isfinite(fv):
                raise ValueError(
                    f"{label} must be finite, got {fv!r}"
                )
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

        # -- Rule 1: quality floor ------------------------------------
        if self.quality_floor is not None:
            quality, code, message = _get_numeric(
                ctx, "expected_quality",
            )
            if code is not None:
                failures.append((code, message))
            elif quality is None:
                failures.append((
                    ReasonCode.QUALITY_MISSING,
                    f"'expected_quality' is required when "
                    f"quality_floor={self.quality_floor}",
                ))
            elif quality < self.quality_floor:
                failures.append((
                    ReasonCode.QUALITY_BELOW_FLOOR,
                    f"expected quality {quality:.3f} below floor "
                    f"{self.quality_floor:.3f}",
                ))

        # -- Rule 2: energy budget ------------------------------------
        if self.energy_budget_wh is not None:
            energy, code, message = _get_numeric(
                ctx, "expected_energy_wh",
            )
            if code is not None:
                failures.append((code, message))
            elif energy is None:
                failures.append((
                    ReasonCode.ENERGY_MISSING,
                    f"'expected_energy_wh' is required when "
                    f"energy_budget_wh={self.energy_budget_wh}",
                ))
            elif energy > self.energy_budget_wh:
                failures.append((
                    ReasonCode.ENERGY_EXCEEDED,
                    f"expected energy {energy:.3f} Wh exceeds "
                    f"budget {self.energy_budget_wh:.3f} Wh",
                ))

        # -- Rule 3: carbon budget ------------------------------------
        if self.carbon_budget_g is not None:
            carbon, code, message = _get_numeric(
                ctx, "expected_carbon_g",
            )
            if code is not None:
                failures.append((code, message))
            elif carbon is None:
                failures.append((
                    ReasonCode.CARBON_MISSING,
                    f"'expected_carbon_g' is required when "
                    f"carbon_budget_g={self.carbon_budget_g}",
                ))
            elif carbon > self.carbon_budget_g:
                failures.append((
                    ReasonCode.CARBON_EXCEEDED,
                    f"expected carbon {carbon:.3f} g exceeds "
                    f"budget {self.carbon_budget_g:.3f} g",
                ))

        # -- Obligations ----------------------------------------------
        # The telemetry obligation is always attached — the audit
        # trail should reflect that a sustainability decision was made.
        obligations: List[str] = [Obligation.LOG_ENERGY_TELEMETRY.value]
        if ctx.get("regional_baseline") is True:
            obligations.append(
                Obligation.ATTACH_REGIONAL_BASELINE.value
            )

        # -- Assemble -------------------------------------------------
        verdict = (
            DecisionVerdict.DENY if failures else DecisionVerdict.ALLOW
        )
        reasons: List[str] = (
            [_format_reason(c, m) for c, m in failures]
            if failures
            else ["no sustainability objections"]
        )

        human_approval_required = (
            verdict is DecisionVerdict.ESCALATE
        )
        if (
            verdict is DecisionVerdict.ALLOW
            and human_approval_required
        ):  # pragma: no cover — impossible by construction
            raise RuntimeError(
                "internal invariant violated: ALLOW with "
                "human_approval_required=True"
            )
        if (
            verdict is DecisionVerdict.DENY
            and human_approval_required
        ):  # pragma: no cover — impossible by construction
            raise RuntimeError(
                "internal invariant violated: DENY with "
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
        if self.quality_floor is not None:
            parts.append(f"quality_floor={self.quality_floor}")
        if self.energy_budget_wh is not None:
            parts.append(
                f"energy_budget={self.energy_budget_wh}Wh"
            )
        if self.carbon_budget_g is not None:
            parts.append(
                f"carbon_budget={self.carbon_budget_g}g"
            )
        return f"SustainabilityPolicy({', '.join(parts)})"

    def __str__(self) -> str:
        return self.describe()

    def __repr__(self) -> str:
        return self.describe()

    # -- serialization -----------------------------------------------------
    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "policy_version": self.policy_version,
            "quality_floor": self.quality_floor,
            "energy_budget_wh": self.energy_budget_wh,
            "carbon_budget_g": self.carbon_budget_g,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "SustainabilityPolicy":
        if not isinstance(data, Mapping):
            raise TypeError(
                f"data must be a Mapping, got {type(data).__name__}"
            )
        return cls(
            quality_floor=data.get("quality_floor"),
            energy_budget_wh=data.get("energy_budget_wh"),
            carbon_budget_g=data.get("carbon_budget_g"),
            name=data.get("name", "sustainability"),
            policy_version=data.get("policy_version", "v5.0.0"),
        )

    def to_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "required": ["name", "policy_version"],
            "properties": {
                "name": {"type": "string", "minLength": 1},
                "policy_version": {"type": "string", "minLength": 1},
                "quality_floor": {
                    "type": ["number", "null"],
                    "minimum": 0,
                },
                "energy_budget_wh": {
                    "type": ["number", "null"],
                    "minimum": 0,
                },
                "carbon_budget_g": {
                    "type": ["number", "null"],
                    "minimum": 0,
                },
            },
            "additionalProperties": False,
        }


# ---------------------------------------------------------------------------
# Convenience singleton
# ---------------------------------------------------------------------------
DEFAULT_SUSTAINABILITY_POLICY: SustainabilityPolicy = SustainabilityPolicy()


# ===========================================================================
# TESTS
# ===========================================================================
def _request(context: Optional[Dict[str, Any]] = None) -> DecisionRequest:
    return DecisionRequest(action="run_job", context=context or {})


def _risk(level: Union[str, RiskLevel] = "low") -> RiskClassification:
    return RiskClassification(level=level)


class TestConstruction(unittest.TestCase):
    def test_defaults(self):
        p = SustainabilityPolicy()
        self.assertIsNone(p.quality_floor)
        self.assertIsNone(p.energy_budget_wh)
        self.assertIsNone(p.carbon_budget_g)
        self.assertEqual(p.name, "sustainability")
        self.assertEqual(p.policy_version, "v5.0.0")

    def test_frozen(self):
        from dataclasses import FrozenInstanceError
        p = SustainabilityPolicy()
        with self.assertRaises(FrozenInstanceError):
            p.quality_floor = 0.9  # type: ignore[misc]

    def test_empty_name_rejected(self):
        with self.assertRaises(ValueError):
            SustainabilityPolicy(name="")
        with self.assertRaises(ValueError):
            SustainabilityPolicy(name="   ")

    def test_empty_policy_version_rejected(self):
        with self.assertRaises(ValueError):
            SustainabilityPolicy(policy_version="")

    def test_negative_quality_floor_rejected(self):
        with self.assertRaises(ValueError):
            SustainabilityPolicy(quality_floor=-0.1)

    def test_nan_quality_floor_rejected(self):
        with self.assertRaises(ValueError):
            SustainabilityPolicy(quality_floor=float("nan"))

    def test_inf_quality_floor_rejected(self):
        with self.assertRaises(ValueError):
            SustainabilityPolicy(quality_floor=float("inf"))

    def test_bool_quality_floor_rejected(self):
        with self.assertRaises(TypeError):
            SustainabilityPolicy(quality_floor=True)  # type: ignore[arg-type]

    def test_string_quality_floor_rejected(self):
        with self.assertRaises(TypeError):
            SustainabilityPolicy(quality_floor="0.9")  # type: ignore[arg-type]

    def test_negative_energy_rejected(self):
        with self.assertRaises(ValueError):
            SustainabilityPolicy(energy_budget_wh=-1.0)

    def test_nan_energy_rejected(self):
        with self.assertRaises(ValueError):
            SustainabilityPolicy(energy_budget_wh=float("nan"))

    def test_negative_carbon_rejected(self):
        with self.assertRaises(ValueError):
            SustainabilityPolicy(carbon_budget_g=-0.1)

    def test_nan_carbon_rejected(self):
        with self.assertRaises(ValueError):
            SustainabilityPolicy(carbon_budget_g=float("nan"))

    def test_hashable(self):
        p = SustainabilityPolicy()
        self.assertEqual({p, p}, {p})

    def test_default_singleton_valid(self):
        self.assertIsInstance(
            DEFAULT_SUSTAINABILITY_POLICY, SustainabilityPolicy,
        )


class TestQualityFloor(unittest.TestCase):
    def _policy(self, floor: float = 0.9) -> SustainabilityPolicy:
        return SustainabilityPolicy(quality_floor=floor)

    def test_quality_at_floor_allows(self):
        d = self._policy(0.9).evaluate(
            _request({"expected_quality": 0.9}), _risk(),
        )
        self.assertEqual(d.verdict, "allow")

    def test_quality_above_floor_allows(self):
        d = self._policy(0.9).evaluate(
            _request({"expected_quality": 0.95}), _risk(),
        )
        self.assertEqual(d.verdict, "allow")

    def test_quality_below_floor_denies(self):
        d = self._policy(0.9).evaluate(
            _request({"expected_quality": 0.5}), _risk(),
        )
        self.assertEqual(d.verdict, "deny")
        self.assertTrue(
            any("[quality_below_floor]" in r for r in d.reasons)
        )

    def test_quality_missing_denies(self):
        # Regression: original silently passed.
        d = self._policy(0.9).evaluate(_request({}), _risk())
        self.assertEqual(d.verdict, "deny")
        self.assertTrue(
            any("[quality_missing]" in r for r in d.reasons)
        )

    def test_quality_nan_denies(self):
        # Regression: original silently passed.
        d = self._policy(0.9).evaluate(
            _request({"expected_quality": float("nan")}), _risk(),
        )
        self.assertEqual(d.verdict, "deny")
        self.assertTrue(
            any("[invalid_context]" in r for r in d.reasons)
        )

    def test_quality_string_denies(self):
        # Regression: original silently passed.
        d = self._policy(0.9).evaluate(
            _request({"expected_quality": "0.5"}), _risk(),
        )
        self.assertEqual(d.verdict, "deny")
        self.assertTrue(
            any("[invalid_context]" in r for r in d.reasons)
        )

    def test_quality_bool_denies(self):
        d = self._policy(0.9).evaluate(
            _request({"expected_quality": True}), _risk(),
        )
        self.assertEqual(d.verdict, "deny")
        self.assertTrue(
            any("[invalid_context]" in r for r in d.reasons)
        )

    def test_no_floor_allows_any_quality(self):
        # Regression: default None disables the check entirely.
        d = SustainabilityPolicy().evaluate(
            _request({"expected_quality": 0.1}), _risk(),
        )
        self.assertEqual(d.verdict, "allow")


class TestEnergyBudget(unittest.TestCase):
    def _policy(self, budget: float = 100.0) -> SustainabilityPolicy:
        return SustainabilityPolicy(energy_budget_wh=budget)

    def test_energy_at_budget_allows(self):
        d = self._policy().evaluate(
            _request({"expected_energy_wh": 100.0}), _risk(),
        )
        self.assertEqual(d.verdict, "allow")

    def test_energy_above_budget_denies(self):
        d = self._policy().evaluate(
            _request({"expected_energy_wh": 150.0}), _risk(),
        )
        self.assertEqual(d.verdict, "deny")
        self.assertTrue(
            any("[energy_exceeded]" in r for r in d.reasons)
        )

    def test_energy_missing_denies(self):
        # Regression: original silently passed.
        d = self._policy().evaluate(_request({}), _risk())
        self.assertEqual(d.verdict, "deny")
        self.assertTrue(
            any("[energy_missing]" in r for r in d.reasons)
        )

    def test_energy_nan_denies(self):
        # Regression: original silently passed.
        d = self._policy().evaluate(
            _request({"expected_energy_wh": float("nan")}), _risk(),
        )
        self.assertEqual(d.verdict, "deny")
        self.assertTrue(
            any("[invalid_context]" in r for r in d.reasons)
        )

    def test_energy_string_denies(self):
        # Regression: original silently passed.
        d = self._policy().evaluate(
            _request({"expected_energy_wh": "150"}), _risk(),
        )
        self.assertEqual(d.verdict, "deny")
        self.assertTrue(
            any("[invalid_context]" in r for r in d.reasons)
        )

    def test_energy_bool_denies(self):
        d = self._policy().evaluate(
            _request({"expected_energy_wh": True}), _risk(),
        )
        self.assertEqual(d.verdict, "deny")
        self.assertTrue(
            any("[invalid_context]" in r for r in d.reasons)
        )


class TestCarbonBudget(unittest.TestCase):
    def _policy(self, budget: float = 10.0) -> SustainabilityPolicy:
        return SustainabilityPolicy(carbon_budget_g=budget)

    def test_carbon_at_budget_allows(self):
        d = self._policy().evaluate(
            _request({"expected_carbon_g": 10.0}), _risk(),
        )
        self.assertEqual(d.verdict, "allow")

    def test_carbon_above_budget_denies(self):
        d = self._policy().evaluate(
            _request({"expected_carbon_g": 25.0}), _risk(),
        )
        self.assertEqual(d.verdict, "deny")
        self.assertTrue(
            any("[carbon_exceeded]" in r for r in d.reasons)
        )

    def test_carbon_missing_denies(self):
        # Regression: original silently passed.
        d = self._policy().evaluate(_request({}), _risk())
        self.assertEqual(d.verdict, "deny")
        self.assertTrue(
            any("[carbon_missing]" in r for r in d.reasons)
        )

    def test_carbon_nan_denies(self):
        d = self._policy().evaluate(
            _request({"expected_carbon_g": float("nan")}), _risk(),
        )
        self.assertEqual(d.verdict, "deny")
        self.assertTrue(
            any("[invalid_context]" in r for r in d.reasons)
        )


class TestAggregation(unittest.TestCase):
    def test_all_three_budgets_trigger(self):
        p = SustainabilityPolicy(
            quality_floor=0.9,
            energy_budget_wh=100.0,
            carbon_budget_g=10.0,
        )
        d = p.evaluate(
            _request({
                "expected_quality": 0.5,
                "expected_energy_wh": 150.0,
                "expected_carbon_g": 25.0,
            }),
            _risk(),
        )
        self.assertEqual(d.verdict, "deny")
        self.assertEqual(len(d.reasons), 3)

    def test_no_budgets_allow_any_request(self):
        d = SustainabilityPolicy().evaluate(_request({}), _risk())
        self.assertEqual(d.verdict, "allow")
        self.assertEqual(
            d.reasons, ["no sustainability objections"],
        )

    def test_human_approval_required_false_on_allow(self):
        d = SustainabilityPolicy().evaluate(_request(), _risk())
        self.assertFalse(d.human_approval_required)

    def test_human_approval_required_false_on_deny(self):
        p = SustainabilityPolicy(energy_budget_wh=100.0)
        d = p.evaluate(
            _request({"expected_energy_wh": 200.0}), _risk(),
        )
        self.assertFalse(d.human_approval_required)


class TestObligations(unittest.TestCase):
    def test_log_telemetry_always_present(self):
        d = SustainabilityPolicy().evaluate(_request(), _risk())
        self.assertIn("log_energy_telemetry", d.obligations)

    def test_log_telemetry_on_deny(self):
        p = SustainabilityPolicy(energy_budget_wh=100.0)
        d = p.evaluate(
            _request({"expected_energy_wh": 200.0}), _risk(),
        )
        self.assertIn("log_energy_telemetry", d.obligations)

    def test_regional_baseline_true_adds_obligation(self):
        d = SustainabilityPolicy().evaluate(
            _request({"regional_baseline": True}), _risk(),
        )
        self.assertIn("attach_regional_baseline", d.obligations)

    def test_regional_baseline_string_false_does_not_trigger(self):
        # Regression for the string-bool trap.
        d = SustainabilityPolicy().evaluate(
            _request({"regional_baseline": "false"}), _risk(),
        )
        self.assertNotIn("attach_regional_baseline", d.obligations)

    def test_regional_baseline_absent(self):
        d = SustainabilityPolicy().evaluate(_request({}), _risk())
        self.assertNotIn("attach_regional_baseline", d.obligations)


class TestSerialization(unittest.TestCase):
    def test_round_trip(self):
        p = SustainabilityPolicy(
            quality_floor=0.9,
            energy_budget_wh=100.0,
            carbon_budget_g=10.0,
            name="prod",
            policy_version="v5.1.0",
        )
        p2 = SustainabilityPolicy.from_dict(p.to_dict())
        self.assertEqual(p.to_dict(), p2.to_dict())

    def test_from_dict_defaults(self):
        p = SustainabilityPolicy.from_dict({})
        self.assertEqual(p.name, "sustainability")
        self.assertEqual(p.policy_version, "v5.0.0")
        self.assertIsNone(p.quality_floor)
        self.assertIsNone(p.energy_budget_wh)
        self.assertIsNone(p.carbon_budget_g)

    def test_from_dict_null_budgets(self):
        p = SustainabilityPolicy.from_dict({
            "quality_floor": None,
            "energy_budget_wh": None,
            "carbon_budget_g": None,
        })
        self.assertIsNone(p.quality_floor)

    def test_from_dict_non_mapping_rejected(self):
        with self.assertRaises(TypeError):
            SustainabilityPolicy.from_dict("nope")  # type: ignore[arg-type]

    def test_to_dict_json_safe(self):
        import json
        p = SustainabilityPolicy(quality_floor=0.9)
        json.dumps(p.to_dict())  # must not raise

    def test_to_schema_shape(self):
        s = SustainabilityPolicy().to_schema()
        self.assertEqual(s["type"], "object")
        self.assertIn("name", s["required"])
        self.assertIn("policy_version", s["required"])
        self.assertFalse(s["additionalProperties"])


class TestIntrospection(unittest.TestCase):
    def test_describe_mentions_budgets(self):
        p = SustainabilityPolicy(
            quality_floor=0.9,
            energy_budget_wh=100.0,
            carbon_budget_g=10.0,
        )
        s = p.describe()
        self.assertIn("quality_floor", s)
        self.assertIn("energy_budget", s)
        self.assertIn("carbon_budget", s)

    def test_describe_minimal(self):
        s = SustainabilityPolicy().describe()
        self.assertIn("name=", s)

    def test_str_matches_describe(self):
        p = SustainabilityPolicy()
        self.assertEqual(str(p), p.describe())
        self.assertEqual(repr(p), p.describe())


class TestReasonCodes(unittest.TestCase):
    def test_quality_codes_present(self):
        self.assertEqual(
            ReasonCode.QUALITY_BELOW_FLOOR.value,
            "quality_below_floor",
        )
        self.assertEqual(
            ReasonCode.QUALITY_MISSING.value, "quality_missing",
        )

    def test_energy_codes_present(self):
        self.assertEqual(
            ReasonCode.ENERGY_EXCEEDED.value, "energy_exceeded",
        )
        self.assertEqual(
            ReasonCode.ENERGY_MISSING.value, "energy_missing",
        )

    def test_carbon_codes_present(self):
        self.assertEqual(
            ReasonCode.CARBON_EXCEEDED.value, "carbon_exceeded",
        )
        self.assertEqual(
            ReasonCode.CARBON_MISSING.value, "carbon_missing",
        )


class TestObligationVocabulary(unittest.TestCase):
    def test_telemetry_obligation_value(self):
        self.assertEqual(
            Obligation.LOG_ENERGY_TELEMETRY.value,
            "log_energy_telemetry",
        )

    def test_regional_baseline_obligation_value(self):
        self.assertEqual(
            Obligation.ATTACH_REGIONAL_BASELINE.value,
            "attach_regional_baseline",
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
