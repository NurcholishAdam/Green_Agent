"""
HumanOversightPolicy — decide when human approval is required.

A pure, rule-based policy that inspects a :class:`DecisionRequest` and
its :class:`RiskClassification` and returns a :class:`PolicyDecision`
telling the PDP whether to ``ALLOW`` or ``ESCALATE``.

Key guarantees
--------------
* **Dual-control escalates.** An action in ``dual_control_actions``
  produces an ``ESCALATE`` verdict **and** the ``"dual_control"``
  obligation — the two-human gate cannot be silently bypassed.
* **Typed risk threshold.** ``require_approval_above_risk`` is an
  ``Optional[RiskLevel]``; plain strings are coerced at construction
  time and typos raise immediately. ``None`` disables the risk-based
  rule entirely.
* **Immutable.** The policy is a frozen dataclass; both action sets
  are ``frozenset``; callers cannot mutate the policy after
  construction.
* **Validated.** ``__post_init__`` rejects empty ``name`` /
  ``policy_version``, empty action strings, non-iterable action
  collections, and unknown risk levels.
* **Structured.** Emits a :class:`PolicyDecision` whose
  ``human_approval_required`` field is derived from the verdict, so it
  always satisfies the enhanced ``PolicyDecision`` invariants.
* **Round-trippable.** ``to_dict`` / ``from_dict`` / ``to_schema``
  match the conventions of the rest of the governance stack.

Note on ``policy_version``
--------------------------
When this policy is used inside a :class:`PolicyDecisionPoint`, the
PDP stamps its own ``policy_version`` on the final decision — this
field is overwritten. It matters only for standalone use, and for
``to_dict()`` round-trips.
"""

from __future__ import annotations

import logging
import unittest
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, FrozenSet, Iterable, List, Optional, Tuple, Union

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

        def at_least(self, level):
            lvl = level if isinstance(level, RiskLevel) else RiskLevel(level)
            return self.level.rank >= lvl.rank

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
                "agent_id": self.agent_id,
                "task_id": self.task_id,
                "run_id": self.run_id,
                "policy_version": self.policy_version,
                "context": dict(self.context),
                "requested_at": self.requested_at.isoformat(),
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

__all__ = ["HumanOversightPolicy", "DEFAULT_OVERSIGHT_POLICY"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _as_risk_level(value: Any) -> RiskLevel:
    """Coerce ``value`` to ``RiskLevel`` or raise ``ValueError``."""
    if isinstance(value, RiskLevel):
        return value
    return RiskLevel(value)


def _as_str_frozenset(value: Any, label: str) -> FrozenSet[str]:
    if isinstance(value, str):
        raise TypeError(
            f"{label} must be an iterable of strings, not a single "
            f"string; wrap in a list, e.g. {label}=['my_action']"
        )
    try:
        result = frozenset(value)
    except TypeError:
        raise TypeError(
            f"{label} must be an iterable of strings, got "
            f"{type(value).__name__}"
        ) from None
    for item in result:
        if not isinstance(item, str) or not item.strip():
            raise ValueError(
                f"{label} entries must be non-empty strings, got "
                f"{item!r}"
            )
    return result


# ---------------------------------------------------------------------------
# Policy
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class HumanOversightPolicy:
    """
    Governs when human approval is required.

    Rules (each contributing independently):

    * ``require_approval_for_actions``: actions that always escalate.
    * ``require_approval_above_risk``: risk levels that always escalate;
      ``None`` disables this rule.
    * ``dual_control_actions``: actions that require **two** human
      approvers — escalate **and** attach the ``"dual_control"``
      obligation.
    """

    require_approval_for_actions: FrozenSet[str] = frozenset()
    require_approval_above_risk: Optional[RiskLevel] = RiskLevel.HIGH
    dual_control_actions: FrozenSet[str] = frozenset()
    name: str = "human_oversight"
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

        object.__setattr__(
            self,
            "require_approval_for_actions",
            _as_str_frozenset(
                self.require_approval_for_actions,
                "require_approval_for_actions",
            ),
        )
        object.__setattr__(
            self,
            "dual_control_actions",
            _as_str_frozenset(
                self.dual_control_actions, "dual_control_actions",
            ),
        )

        if self.require_approval_above_risk is not None:
            object.__setattr__(
                self,
                "require_approval_above_risk",
                _as_risk_level(self.require_approval_above_risk),
            )

    # -- evaluation --------------------------------------------------------
    def evaluate(
        self,
        request: DecisionRequest,
        risk: RiskClassification,
    ) -> PolicyDecision:
        if not hasattr(request, "action") or not hasattr(request, "request_id"):
            raise TypeError(
                "request must be a DecisionRequest-like object with "
                "action and request_id"
            )
        if not hasattr(risk, "level"):
            raise TypeError(
                "risk must be a RiskClassification-like object with "
                "level"
            )

        reasons: List[str] = []
        obligations: List[str] = []
        verdict = DecisionVerdict.ALLOW

        # -- rule 1: explicit approval action --------------------------
        if request.action in self.require_approval_for_actions:
            verdict = DecisionVerdict.ESCALATE
            reasons.append(
                f"action '{request.action}' always requires human "
                f"approval"
            )

        # -- rule 2: risk threshold ------------------------------------
        if self.require_approval_above_risk is not None:
            current = _as_risk_level(risk.level)
            if current.rank >= self.require_approval_above_risk.rank:
                verdict = DecisionVerdict.ESCALATE
                reasons.append(
                    f"risk level '{current.value}' requires human "
                    f"approval"
                )

        # -- rule 3: dual control --------------------------------------
        # Regression: this used to only append the obligation without
        # escalating, making dual-control a silent no-op.
        if request.action in self.dual_control_actions:
            verdict = DecisionVerdict.ESCALATE
            reasons.append(
                f"action '{request.action}' requires dual control "
                f"(two approvers)"
            )
            obligations.append("dual_control")

        if not reasons:
            reasons = ["no human oversight required"]

        human_approval_required = verdict is DecisionVerdict.ESCALATE

        # Defensive invariant check — a refactor that accidentally
        # sets this True on ALLOW would violate PolicyDecision.
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
            risk_level=_as_risk_level(risk.level),
            human_approval_required=human_approval_required,
        )

    # -- introspection -----------------------------------------------------
    def describe(self) -> str:
        parts: List[str] = [f"name={self.name!r}"]
        if self.require_approval_for_actions:
            parts.append(
                f"approval_actions="
                f"{sorted(self.require_approval_for_actions)}"
            )
        if self.require_approval_above_risk is not None:
            parts.append(
                f"approval_risk>="
                f"{self.require_approval_above_risk.value!r}"
            )
        else:
            parts.append("approval_risk=disabled")
        if self.dual_control_actions:
            parts.append(
                f"dual_control={sorted(self.dual_control_actions)}"
            )
        return f"HumanOversightPolicy({', '.join(parts)})"

    def __str__(self) -> str:
        return self.describe()

    def __repr__(self) -> str:
        return self.describe()

    # -- serialization -----------------------------------------------------
    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "policy_version": self.policy_version,
            "require_approval_for_actions": sorted(
                self.require_approval_for_actions
            ),
            "require_approval_above_risk": (
                self.require_approval_above_risk.value
                if self.require_approval_above_risk is not None
                else None
            ),
            "dual_control_actions": sorted(self.dual_control_actions),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "HumanOversightPolicy":
        if not isinstance(data, dict):
            raise TypeError(
                f"data must be a dict, got {type(data).__name__}"
            )
        raw_risk = data.get("require_approval_above_risk", "high")
        return cls(
            name=data.get("name", "human_oversight"),
            policy_version=data.get("policy_version", "v5.0.0"),
            require_approval_for_actions=tuple(
                data.get("require_approval_for_actions", ())
            ),
            require_approval_above_risk=(
                RiskLevel(raw_risk) if raw_risk is not None else None
            ),
            dual_control_actions=tuple(
                data.get("dual_control_actions", ())
            ),
        )

    def to_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "required": [
                "name",
                "policy_version",
                "require_approval_for_actions",
                "dual_control_actions",
            ],
            "properties": {
                "name": {"type": "string", "minLength": 1},
                "policy_version": {"type": "string", "minLength": 1},
                "require_approval_for_actions": {
                    "type": "array",
                    "items": {"type": "string", "minLength": 1},
                },
                "require_approval_above_risk": {
                    "type": ["string", "null"],
                    "enum": [r.value for r in RiskLevel] + [None],
                },
                "dual_control_actions": {
                    "type": "array",
                    "items": {"type": "string", "minLength": 1},
                },
            },
            "additionalProperties": False,
        }


# ---------------------------------------------------------------------------
# Convenience singleton
# ---------------------------------------------------------------------------
DEFAULT_OVERSIGHT_POLICY: HumanOversightPolicy = HumanOversightPolicy()


# ===========================================================================
# TESTS
# ===========================================================================
def _request(action: str = "deploy_model", **kwargs: Any) -> DecisionRequest:
    return DecisionRequest(action=action, **kwargs)


def _risk(level: Union[RiskLevel, str] = "low") -> RiskClassification:
    return RiskClassification(level=_as_risk_level(level))


class TestConstruction(unittest.TestCase):
    def test_defaults(self):
        p = HumanOversightPolicy()
        self.assertEqual(p.require_approval_for_actions, frozenset())
        self.assertIs(p.require_approval_above_risk, RiskLevel.HIGH)
        self.assertEqual(p.dual_control_actions, frozenset())
        self.assertEqual(p.name, "human_oversight")
        self.assertEqual(p.policy_version, "v5.0.0")

    def test_frozen(self):
        from dataclasses import FrozenInstanceError
        p = HumanOversightPolicy()
        with self.assertRaises(FrozenInstanceError):
            p.name = "x"  # type: ignore[misc]

    def test_empty_name_rejected(self):
        with self.assertRaises(ValueError):
            HumanOversightPolicy(name="")
        with self.assertRaises(ValueError):
            HumanOversightPolicy(name="   ")

    def test_empty_policy_version_rejected(self):
        with self.assertRaises(ValueError):
            HumanOversightPolicy(policy_version="")

    def test_actions_coerced_to_frozenset(self):
        p = HumanOversightPolicy(
            require_approval_for_actions=["deploy", "promote"],
        )
        self.assertIsInstance(p.require_approval_for_actions, frozenset)
        self.assertEqual(
            p.require_approval_for_actions,
            frozenset({"deploy", "promote"}),
        )

    def test_single_string_actions_rejected(self):
        with self.assertRaises(TypeError):
            HumanOversightPolicy(require_approval_for_actions="deploy")  # type: ignore[arg-type]

    def test_empty_action_entry_rejected(self):
        with self.assertRaises(ValueError):
            HumanOversightPolicy(
                require_approval_for_actions={"deploy", ""},
            )

    def test_whitespace_action_entry_rejected(self):
        with self.assertRaises(ValueError):
            HumanOversightPolicy(
                dual_control_actions={"   "},
            )

    def test_non_str_action_entry_rejected(self):
        with self.assertRaises(ValueError):
            HumanOversightPolicy(
                require_approval_for_actions={1, 2},  # type: ignore[set-item]
            )

    def test_risk_level_string_coerced(self):
        p = HumanOversightPolicy(require_approval_above_risk="medium")
        self.assertIs(p.require_approval_above_risk, RiskLevel.MEDIUM)

    def test_risk_level_enum_ok(self):
        p = HumanOversightPolicy(
            require_approval_above_risk=RiskLevel.CRITICAL,
        )
        self.assertIs(
            p.require_approval_above_risk, RiskLevel.CRITICAL,
        )

    def test_invalid_risk_level_rejected(self):
        with self.assertRaises(ValueError):
            HumanOversightPolicy(require_approval_above_risk="HIGH")  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            HumanOversightPolicy(require_approval_above_risk="hgih")  # type: ignore[arg-type]

    def test_none_risk_level_allowed(self):
        p = HumanOversightPolicy(require_approval_above_risk=None)
        self.assertIsNone(p.require_approval_above_risk)

    def test_caller_set_not_aliased(self):
        src = {"deploy"}
        p = HumanOversightPolicy(require_approval_for_actions=src)
        src.add("rm_rf")
        self.assertEqual(
            p.require_approval_for_actions, frozenset({"deploy"})
        )

    def test_hashable(self):
        p = HumanOversightPolicy()
        self.assertEqual({p, p}, {p})


class TestEvaluateActionRule(unittest.TestCase):
    def test_action_matches_escalates(self):
        p = HumanOversightPolicy(
            require_approval_for_actions={"deploy_model"},
        )
        d = p.evaluate(_request("deploy_model"), _risk("low"))
        self.assertEqual(d.verdict, "escalate")
        self.assertTrue(d.human_approval_required)
        self.assertTrue(
            any("deploy_model" in r for r in d.reasons)
        )

    def test_action_no_match_allows(self):
        p = HumanOversightPolicy(
            require_approval_for_actions={"deploy_model"},
            require_approval_above_risk=None,
        )
        d = p.evaluate(_request("read_metric"), _risk("low"))
        self.assertEqual(d.verdict, "allow")
        self.assertFalse(d.human_approval_required)


class TestEvaluateRiskRule(unittest.TestCase):
    def test_risk_at_threshold_escalates(self):
        p = HumanOversightPolicy(require_approval_above_risk="high")
        d = p.evaluate(_request(), _risk("high"))
        self.assertEqual(d.verdict, "escalate")
        self.assertTrue(d.human_approval_required)

    def test_risk_below_threshold_allows(self):
        p = HumanOversightPolicy(require_approval_above_risk="high")
        d = p.evaluate(_request(), _risk("medium"))
        self.assertEqual(d.verdict, "allow")

    def test_risk_above_threshold_escalates(self):
        p = HumanOversightPolicy(require_approval_above_risk="high")
        d = p.evaluate(_request(), _risk("critical"))
        self.assertEqual(d.verdict, "escalate")

    def test_low_threshold_escalates_everything(self):
        p = HumanOversightPolicy(require_approval_above_risk="low")
        for level in ("low", "medium", "high", "critical"):
            with self.subTest(level=level):
                d = p.evaluate(_request(), _risk(level))
                self.assertEqual(d.verdict, "escalate")

    def test_none_disables_risk_rule(self):
        p = HumanOversightPolicy(
            require_approval_above_risk=None,
            require_approval_for_actions=frozenset(),
            dual_control_actions=frozenset(),
        )
        d = p.evaluate(_request(), _risk("critical"))
        self.assertEqual(d.verdict, "allow")


class TestEvaluateDualControl(unittest.TestCase):
    def test_dual_control_escalates(self):
        # Regression for the original bug where dual-control only
        # added an obligation without escalating.
        p = HumanOversightPolicy(
            dual_control_actions={"change_policy"},
            require_approval_above_risk=None,
        )
        d = p.evaluate(_request("change_policy"), _risk("low"))
        self.assertEqual(d.verdict, "escalate")
        self.assertTrue(d.human_approval_required)
        self.assertIn("dual_control", d.obligations)
        self.assertTrue(
            any("dual control" in r for r in d.reasons)
        )

    def test_dual_control_and_approval_both_apply(self):
        p = HumanOversightPolicy(
            require_approval_for_actions={"change_policy"},
            dual_control_actions={"change_policy"},
            require_approval_above_risk=None,
        )
        d = p.evaluate(_request("change_policy"), _risk("low"))
        self.assertEqual(d.verdict, "escalate")
        self.assertIn("dual_control", d.obligations)
        # Both rules contributed a reason
        self.assertGreaterEqual(len(d.reasons), 2)


class TestEvaluateAggregation(unittest.TestCase):
    def test_all_three_trigger(self):
        p = HumanOversightPolicy(
            require_approval_for_actions={"change_policy"},
            dual_control_actions={"change_policy"},
            require_approval_above_risk="high",
        )
        d = p.evaluate(_request("change_policy"), _risk("critical"))
        self.assertEqual(d.verdict, "escalate")
        self.assertTrue(d.human_approval_required)
        self.assertIn("dual_control", d.obligations)
        # three distinct reasons
        self.assertEqual(len(d.reasons), 3)

    def test_no_rules_match_gives_allow_reason(self):
        p = HumanOversightPolicy(require_approval_above_risk=None)
        d = p.evaluate(_request(), _risk("low"))
        self.assertEqual(d.verdict, "allow")
        self.assertEqual(d.reasons, ["no human oversight required"])

    def test_allow_never_sets_human_approval_required(self):
        p = HumanOversightPolicy(require_approval_above_risk=None)
        d = p.evaluate(_request(), _risk("low"))
        self.assertFalse(d.human_approval_required)

    def test_escalate_always_sets_human_approval_required(self):
        p = HumanOversightPolicy(require_approval_above_risk="low")
        d = p.evaluate(_request(), _risk("low"))
        self.assertTrue(d.human_approval_required)


class TestDefensiveCoercion(unittest.TestCase):
    def test_plain_string_risk_level(self):
        # Simulate a RiskClassification built before enum coercion.
        class Raw:
            level = "high"

        p = HumanOversightPolicy(require_approval_above_risk="high")
        d = p.evaluate(_request(), Raw())  # type: ignore[arg-type]
        self.assertEqual(d.verdict, "escalate")

    def test_invalid_request_type(self):
        p = HumanOversightPolicy()
        with self.assertRaises(TypeError):
            p.evaluate(None, _risk("low"))  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            p.evaluate(_request(), None)  # type: ignore[arg-type]


class TestSerialization(unittest.TestCase):
    def test_round_trip(self):
        p = HumanOversightPolicy(
            require_approval_for_actions={"deploy", "promote"},
            require_approval_above_risk="high",
            dual_control_actions={"change_policy"},
            name="prod",
            policy_version="v5.1.0",
        )
        p2 = HumanOversightPolicy.from_dict(p.to_dict())
        self.assertEqual(p.to_dict(), p2.to_dict())

    def test_from_dict_defaults(self):
        p = HumanOversightPolicy.from_dict({})
        self.assertEqual(p.name, "human_oversight")
        self.assertEqual(p.policy_version, "v5.0.0")
        self.assertIs(p.require_approval_above_risk, RiskLevel.HIGH)

    def test_from_dict_null_risk(self):
        p = HumanOversightPolicy.from_dict(
            {"require_approval_above_risk": None}
        )
        self.assertIsNone(p.require_approval_above_risk)

    def test_from_dict_non_dict_rejected(self):
        with self.assertRaises(TypeError):
            HumanOversightPolicy.from_dict("nope")  # type: ignore[arg-type]

    def test_to_dict_is_json_safe(self):
        import json
        p = HumanOversightPolicy(
            require_approval_for_actions={"deploy"},
        )
        json.dumps(p.to_dict())  # must not raise

    def test_to_schema_shape(self):
        s = HumanOversightPolicy().to_schema()
        self.assertEqual(s["type"], "object")
        self.assertIn("name", s["required"])
        self.assertIn(
            RiskLevel.HIGH.value,
            s["properties"]["require_approval_above_risk"]["enum"],
        )
        self.assertIn(
            None,
            s["properties"]["require_approval_above_risk"]["enum"],
        )


class TestIntrospection(unittest.TestCase):
    def test_describe_mentions_rules(self):
        p = HumanOversightPolicy(
            require_approval_for_actions={"deploy"},
            require_approval_above_risk="high",
            dual_control_actions={"change_policy"},
        )
        s = p.describe()
        self.assertIn("deploy", s)
        self.assertIn("high", s)
        self.assertIn("change_policy", s)

    def test_describe_disabled_risk(self):
        p = HumanOversightPolicy(require_approval_above_risk=None)
        self.assertIn("disabled", p.describe())

    def test_str_matches_describe(self):
        p = HumanOversightPolicy()
        self.assertEqual(str(p), p.describe())
        self.assertEqual(repr(p), p.describe())

    def test_default_singleton(self):
        self.assertIsInstance(
            DEFAULT_OVERSIGHT_POLICY, HumanOversightPolicy,
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
