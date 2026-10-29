"""
SafetyPolicy — rules that block unsafe actions.

A rule-based governance policy that inspects a
:class:`DecisionRequest` and its :class:`RiskClassification` and
returns a :class:`PolicyDecision`. Three independent rules:

* ``forbidden_actions``: action verbs that are always ``DENY``'d.
* ``require_verification_actions``: action verbs that must pass formal
  verification (attached as a ``require_formal_verification``
  obligation).
* ``require_human_approval_actions``: action verbs that must be
  ``ESCALATE``'d to a human.

Precedence
----------
When an action appears in **multiple** rule sets:

    forbidden_actions  >  require_human_approval_actions  >  allow

A forbidden action that also requires human approval is **denied**,
not escalated — a forbidden action cannot be granted by a human
approval. The verification rule is additive: it attaches an obligation
regardless of the verdict.

Key guarantees
--------------
* **Fail-closed precedence.** The decision is expressed as a single
  ``if/elif/else`` so forbidden always wins. No sequence of rule
  assignments can accidentally downgrade a ``DENY`` to ``ESCALATE``.
* **Consistent fields.** ``human_approval_required`` is derived from
  the verdict, so the emitted :class:`PolicyDecision` always satisfies
  the enhanced ``PolicyDecision`` invariants (``ALLOW ⇒ ¬human``,
  ``DENY ⇒ ¬human``, ``ESCALATE`` may be ``True``).
* **Immutable.** ``frozen=True``; all three action sets are
  ``FrozenSet[str]``; callers cannot mutate the policy after
  construction.
* **Validated.** ``__post_init__`` rejects empty ``name`` /
  ``policy_version``, empty action strings, non-iterable action
  collections, and the single-string trap.
* **Case-sensitive.** Action matching is exact-match. Producers are
  expected to normalize action strings before submission; the policy
  does not lowercase or otherwise transform them.

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
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, FrozenSet, List, Mapping, Optional, Tuple, Union

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
    "SafetyPolicy",
    "DEFAULT_SAFETY_POLICY",
]


# ---------------------------------------------------------------------------
# Reason codes and obligation vocabulary
# ---------------------------------------------------------------------------
class ReasonCode(str, Enum):
    """Machine-readable codes emitted as the prefix of each reason."""

    FORBIDDEN_ACTION = "forbidden_action"
    HUMAN_APPROVAL_REQUIRED = "human_approval_required"
    INVALID_ACTION = "invalid_action"


class Obligation(str, Enum):
    """Recognised obligation identifiers attached to a decision."""

    VERIFY = "require_formal_verification"
    DUAL_CONTROL = "dual_control"


def _format_reason(code: ReasonCode, message: str) -> str:
    return f"[{code.value}] {message}"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
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
class SafetyPolicy:
    """
    Rule-based safety policy.

    Rules (each contributing independently):

    * ``forbidden_actions``: action verbs that are always ``DENY``'d.
    * ``require_verification_actions``: action verbs that must pass
      formal verification (attached as a ``require_formal_verification``
      obligation).
    * ``require_human_approval_actions``: action verbs that must be
      ``ESCALATE``'d to a human.

    When an action appears in multiple sets, ``forbidden`` wins; see the
    module docstring for the precedence table.
    """

    forbidden_actions: FrozenSet[str] = frozenset()
    require_verification_actions: FrozenSet[str] = frozenset()
    require_human_approval_actions: FrozenSet[str] = frozenset()
    name: str = "safety"
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
            "forbidden_actions",
            _as_str_frozenset(
                self.forbidden_actions, "forbidden_actions",
            ),
        )
        object.__setattr__(
            self,
            "require_verification_actions",
            _as_str_frozenset(
                self.require_verification_actions,
                "require_verification_actions",
            ),
        )
        object.__setattr__(
            self,
            "require_human_approval_actions",
            _as_str_frozenset(
                self.require_human_approval_actions,
                "require_human_approval_actions",
            ),
        )

    # -- evaluation --------------------------------------------------------
    def evaluate(
        self,
        request: DecisionRequest,
        risk: RiskClassification,
    ) -> PolicyDecision:
        if not hasattr(request, "action") or not hasattr(
            request, "request_id"
        ):
            raise TypeError(
                "request must be a DecisionRequest-like object with "
                "action and request_id"
            )
        if not hasattr(risk, "level"):
            raise TypeError(
                "risk must be a RiskClassification-like object with "
                "level"
            )

        action = request.action
        if not isinstance(action, str) or not action.strip():
            # Structural failure — deny with a clear code.
            verdict = DecisionVerdict.DENY
            reasons = [
                _format_reason(
                    ReasonCode.INVALID_ACTION,
                    f"request.action must be a non-empty string, "
                    f"got {action!r}",
                )
            ]
            obligations: List[str] = []
        else:
            # -- Rule precedence (structural, not sequential) ----------
            # Forbidden > require_human_approval > allow
            reasons = []
            obligations = []

            if action in self.forbidden_actions:
                verdict = DecisionVerdict.DENY
                reasons.append(_format_reason(
                    ReasonCode.FORBIDDEN_ACTION,
                    f"action {action!r} is forbidden",
                ))
            elif action in self.require_human_approval_actions:
                verdict = DecisionVerdict.ESCALATE
                reasons.append(_format_reason(
                    ReasonCode.HUMAN_APPROVAL_REQUIRED,
                    f"action {action!r} requires human approval",
                ))
            else:
                verdict = DecisionVerdict.ALLOW

            # -- Verification obligation (additive) --------------------
            if action in self.require_verification_actions:
                obligations.append(Obligation.VERIFY.value)

            if not reasons:
                reasons = ["no safety objections"]

        # -- Derive the human-approval flag from the verdict -----------
        human_approval_required = verdict is DecisionVerdict.ESCALATE

        # -- Defensive invariant check ---------------------------------
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
        if self.forbidden_actions:
            parts.append(
                f"forbidden={sorted(self.forbidden_actions)}"
            )
        if self.require_verification_actions:
            parts.append(
                f"require_verification="
                f"{sorted(self.require_verification_actions)}"
            )
        if self.require_human_approval_actions:
            parts.append(
                f"require_human="
                f"{sorted(self.require_human_approval_actions)}"
            )
        return f"SafetyPolicy({', '.join(parts)})"

    def __str__(self) -> str:
        return self.describe()

    def __repr__(self) -> str:
        return self.describe()

    # -- serialization -----------------------------------------------------
    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "policy_version": self.policy_version,
            "forbidden_actions": sorted(self.forbidden_actions),
            "require_verification_actions": sorted(
                self.require_verification_actions
            ),
            "require_human_approval_actions": sorted(
                self.require_human_approval_actions
            ),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "SafetyPolicy":
        if not isinstance(data, Mapping):
            raise TypeError(
                f"data must be a Mapping, got {type(data).__name__}"
            )
        return cls(
            forbidden_actions=tuple(
                data.get("forbidden_actions", ())
            ),
            require_verification_actions=tuple(
                data.get("require_verification_actions", ())
            ),
            require_human_approval_actions=tuple(
                data.get("require_human_approval_actions", ())
            ),
            name=data.get("name", "safety"),
            policy_version=data.get("policy_version", "v5.0.0"),
        )

    def to_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "required": [
                "name",
                "policy_version",
                "forbidden_actions",
                "require_verification_actions",
                "require_human_approval_actions",
            ],
            "properties": {
                "name": {"type": "string", "minLength": 1},
                "policy_version": {"type": "string", "minLength": 1},
                "forbidden_actions": {
                    "type": "array",
                    "items": {"type": "string", "minLength": 1},
                },
                "require_verification_actions": {
                    "type": "array",
                    "items": {"type": "string", "minLength": 1},
                },
                "require_human_approval_actions": {
                    "type": "array",
                    "items": {"type": "string", "minLength": 1},
                },
            },
            "additionalProperties": False,
        }


# ---------------------------------------------------------------------------
# Convenience singleton
# ---------------------------------------------------------------------------
DEFAULT_SAFETY_POLICY: SafetyPolicy = SafetyPolicy()


# ===========================================================================
# TESTS
# ===========================================================================
def _request(action: str = "deploy") -> DecisionRequest:
    return DecisionRequest(action=action)


def _risk(level: Union[str, RiskLevel] = "low") -> RiskClassification:
    return RiskClassification(level=level)


class TestConstruction(unittest.TestCase):
    def test_defaults(self):
        p = SafetyPolicy()
        self.assertEqual(p.forbidden_actions, frozenset())
        self.assertEqual(p.require_verification_actions, frozenset())
        self.assertEqual(p.require_human_approval_actions, frozenset())
        self.assertEqual(p.name, "safety")
        self.assertEqual(p.policy_version, "v5.0.0")

    def test_frozen(self):
        from dataclasses import FrozenInstanceError
        p = SafetyPolicy()
        with self.assertRaises(FrozenInstanceError):
            p.name = "x"  # type: ignore[misc]

    def test_empty_name_rejected(self):
        with self.assertRaises(ValueError):
            SafetyPolicy(name="")
        with self.assertRaises(ValueError):
            SafetyPolicy(name="   ")

    def test_empty_policy_version_rejected(self):
        with self.assertRaises(ValueError):
            SafetyPolicy(policy_version="")

    def test_single_string_forbidden_rejected(self):
        with self.assertRaises(TypeError):
            SafetyPolicy(forbidden_actions="deploy")  # type: ignore[arg-type]

    def test_empty_forbidden_entry_rejected(self):
        with self.assertRaises(ValueError):
            SafetyPolicy(forbidden_actions={"deploy", ""})

    def test_non_string_forbidden_entry_rejected(self):
        with self.assertRaises(ValueError):
            SafetyPolicy(forbidden_actions={1, 2})  # type: ignore[set-item]

    def test_same_for_verification_and_human_sets(self):
        with self.assertRaises(TypeError):
            SafetyPolicy(require_verification_actions="x")  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            SafetyPolicy(require_human_approval_actions="x")  # type: ignore[arg-type]

    def test_sets_coerced_to_frozenset(self):
        p = SafetyPolicy(forbidden_actions=["a", "b"])
        self.assertIsInstance(p.forbidden_actions, frozenset)
        self.assertEqual(p.forbidden_actions, frozenset({"a", "b"}))

    def test_caller_set_not_aliased(self):
        src = {"a"}
        p = SafetyPolicy(forbidden_actions=src)
        src.add("b")
        self.assertEqual(p.forbidden_actions, frozenset({"a"}))

    def test_hashable(self):
        p = SafetyPolicy()
        self.assertEqual({p, p}, {p})

    def test_default_singleton(self):
        self.assertIsInstance(DEFAULT_SAFETY_POLICY, SafetyPolicy)


class TestForbiddenRule(unittest.TestCase):
    def test_forbidden_denies(self):
        p = SafetyPolicy(forbidden_actions={"rm_rf"})
        d = p.evaluate(_request("rm_rf"), _risk())
        self.assertEqual(d.verdict, "deny")
        self.assertFalse(d.human_approval_required)
        self.assertTrue(
            any("[forbidden_action]" in r for r in d.reasons)
        )

    def test_non_forbidden_allows(self):
        p = SafetyPolicy(forbidden_actions={"rm_rf"})
        d = p.evaluate(_request("deploy"), _risk())
        self.assertEqual(d.verdict, "allow")


class TestHumanApprovalRule(unittest.TestCase):
    def test_human_approval_escalates(self):
        p = SafetyPolicy(require_human_approval_actions={"deploy_prod"})
        d = p.evaluate(_request("deploy_prod"), _risk())
        self.assertEqual(d.verdict, "escalate")
        self.assertTrue(d.human_approval_required)
        self.assertTrue(
            any("[human_approval_required]" in r for r in d.reasons)
        )

    def test_human_approval_not_forbidden(self):
        p = SafetyPolicy(
            require_human_approval_actions={"deploy_prod"},
        )
        d = p.evaluate(_request("deploy_stage"), _risk())
        self.assertEqual(d.verdict, "allow")
        self.assertFalse(d.human_approval_required)


class TestForbiddenBeatsHumanApproval(unittest.TestCase):
    def test_forbidden_wins_when_both_apply(self):
        # Regression for the original fail-open bug: an action in both
        # sets must be DENY, not ESCALATE.
        p = SafetyPolicy(
            forbidden_actions={"nuke_everything"},
            require_human_approval_actions={"nuke_everything"},
        )
        d = p.evaluate(_request("nuke_everything"), _risk())
        self.assertEqual(d.verdict, "deny")
        self.assertFalse(d.human_approval_required)
        self.assertTrue(
            any("[forbidden_action]" in r for r in d.reasons)
        )
        # The human approval reason should NOT be attached — the
        # forbidden reason is the operative one.
        self.assertFalse(
            any("[human_approval_required]" in r for r in d.reasons)
        )


class TestVerificationRule(unittest.TestCase):
    def test_verification_attaches_obligation(self):
        p = SafetyPolicy(
            require_verification_actions={"change_config"},
        )
        d = p.evaluate(_request("change_config"), _risk())
        self.assertEqual(d.verdict, "allow")
        self.assertIn("require_formal_verification", d.obligations)

    def test_verification_obligation_on_deny(self):
        p = SafetyPolicy(
            forbidden_actions={"rm_rf"},
            require_verification_actions={"rm_rf"},
        )
        d = p.evaluate(_request("rm_rf"), _risk())
        self.assertEqual(d.verdict, "deny")
        self.assertIn("require_formal_verification", d.obligations)

    def test_verification_obligation_on_escalate(self):
        p = SafetyPolicy(
            require_human_approval_actions={"change_config"},
            require_verification_actions={"change_config"},
        )
        d = p.evaluate(_request("change_config"), _risk())
        self.assertEqual(d.verdict, "escalate")
        self.assertIn("require_formal_verification", d.obligations)


class TestAggregation(unittest.TestCase):
    def test_all_three_trigger(self):
        p = SafetyPolicy(
            forbidden_actions={"nuke"},
            require_verification_actions={"nuke"},
            require_human_approval_actions={"nuke"},
        )
        d = p.evaluate(_request("nuke"), _risk())
        # Forbidden wins.
        self.assertEqual(d.verdict, "deny")
        self.assertFalse(d.human_approval_required)
        # Verification obligation still attached (audit trail).
        self.assertIn("require_formal_verification", d.obligations)

    def test_allow_reason(self):
        d = SafetyPolicy().evaluate(_request("read"), _risk())
        self.assertEqual(d.verdict, "allow")
        self.assertEqual(d.reasons, ["no safety objections"])

    def test_human_approval_flag_iff_escalate(self):
        # Exhaustive over combinations of the three rules.
        actions = ["forbidden", "approval", "verification", "allow"]
        p = SafetyPolicy(
            forbidden_actions={"forbidden"},
            require_human_approval_actions={"approval"},
            require_verification_actions={"verification"},
        )
        for action in actions:
            d = p.evaluate(_request(action), _risk())
            expected = (d.verdict == "escalate")
            self.assertEqual(d.human_approval_required, expected)


class TestInvalidAction(unittest.TestCase):
    def test_non_string_action_denies(self):
        # Force a non-string action into the request.
        class Raw:
            request_id = "r1"
            action = 123  # not a string

        p = SafetyPolicy()
        d = p.evaluate(Raw(), _risk())  # type: ignore[arg-type]
        self.assertEqual(d.verdict, "deny")
        self.assertTrue(
            any("[invalid_action]" in r for r in d.reasons)
        )

    def test_invalid_request_type(self):
        p = SafetyPolicy()
        with self.assertRaises(TypeError):
            p.evaluate(None, _risk())  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            p.evaluate(_request(), None)  # type: ignore[arg-type]


class TestSerialization(unittest.TestCase):
    def test_round_trip(self):
        p = SafetyPolicy(
            forbidden_actions={"a", "b"},
            require_verification_actions={"c"},
            require_human_approval_actions={"d"},
            name="prod",
            policy_version="v5.1.0",
        )
        p2 = SafetyPolicy.from_dict(p.to_dict())
        self.assertEqual(p.to_dict(), p2.to_dict())

    def test_from_dict_defaults(self):
        p = SafetyPolicy.from_dict({})
        self.assertEqual(p.name, "safety")
        self.assertEqual(p.policy_version, "v5.0.0")
        self.assertEqual(p.forbidden_actions, frozenset())

    def test_from_dict_non_mapping_rejected(self):
        with self.assertRaises(TypeError):
            SafetyPolicy.from_dict("nope")  # type: ignore[arg-type]

    def test_to_dict_json_safe(self):
        import json
        p = SafetyPolicy(forbidden_actions={"a"})
        json.dumps(p.to_dict())  # must not raise

    def test_to_schema_shape(self):
        s = SafetyPolicy().to_schema()
        self.assertEqual(s["type"], "object")
        self.assertIn("forbidden_actions", s["required"])
        self.assertIn("require_verification_actions", s["required"])
        self.assertIn("require_human_approval_actions", s["required"])
        self.assertFalse(s["additionalProperties"])


class TestIntrospection(unittest.TestCase):
    def test_describe_mentions_rules(self):
        p = SafetyPolicy(
            forbidden_actions={"a"},
            require_verification_actions={"b"},
            require_human_approval_actions={"c"},
        )
        s = p.describe()
        self.assertIn("forbidden", s)
        self.assertIn("require_verification", s)
        self.assertIn("require_human", s)

    def test_describe_minimal(self):
        s = SafetyPolicy().describe()
        self.assertIn("name=", s)

    def test_str_matches_describe(self):
        p = SafetyPolicy()
        self.assertEqual(str(p), p.describe())
        self.assertEqual(repr(p), p.describe())


class TestObligationVocabulary(unittest.TestCase):
    def test_verify_value(self):
        self.assertEqual(
            Obligation.VERIFY.value, "require_formal_verification",
        )

    def test_dual_control_value(self):
        self.assertEqual(Obligation.DUAL_CONTROL.value, "dual_control")


if __name__ == "__main__":
    unittest.main(verbosity=2)
