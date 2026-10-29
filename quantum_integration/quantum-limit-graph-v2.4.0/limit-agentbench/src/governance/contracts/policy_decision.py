"""
PolicyDecision — the essential contract.

Structured, immutable verdict for a single proposed action. A
``PolicyDecision`` is a **security artifact**: after the PDP issues it,
its contents are the authorization token that downstream executors
trust. It is therefore:

* **Immutable** (``frozen=True``) — cannot be rewritten after issue.
* **Typed** — ``verdict``, ``risk_level``, and ``verification_status``
  are enums; plain strings are coerced and unknown values rejected.
* **Self-consistent** — :meth:`__post_init__` enforces invariants that
  must hold for a decision to mean anything (``modify`` ⇒
  ``modified_request``, ``deny``/``allow`` ⇒ no human approval gate,
  ``deny``/``fallback`` ⇒ non-empty ``reasons``).
* **Timestamped** — ``issued_at`` must be timezone-aware and is
  normalized to UTC; optional ``expires_at`` bounds the decision's
  validity window.
* **Auditable** — ``execution_allowed()`` returns ``False`` when
  ``audit_event_id`` is blank, closing the decision↔audit correlation
  loop at the exact point it matters.
* **JSON-safe** — ``to_dict()`` recursively encodes nested
  ``datetime`` / ``UUID`` / ``Decimal`` / ``Enum`` / ``bytes`` /
  ``Path`` / ``set`` inside ``modified_request`` and elsewhere.
"""

from __future__ import annotations

import base64
import json
import unittest
from dataclasses import FrozenInstanceError, dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional
from uuid import UUID

__all__ = [
    "DecisionVerdict",
    "RiskLevel",
    "VerificationStatus",
    "PolicyDecision",
]


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------
class DecisionVerdict(str, Enum):
    """The five possible verdicts a policy decision may carry."""

    ALLOW = "allow"
    DENY = "deny"
    MODIFY = "modify"
    ESCALATE = "escalate"
    FALLBACK = "fallback"


class RiskLevel(str, Enum):
    """Closed vocabulary for the decision's risk assessment."""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class VerificationStatus(str, Enum):
    """State of any verification that must precede execution."""

    NOT_REQUIRED = "not_required"
    PENDING = "pending"
    PASSED = "passed"
    FAILED = "failed"


# ---------------------------------------------------------------------------
# JSON-safety helper (mirrors DecisionRequest / GovernanceEvent)
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
    if isinstance(value, Mapping):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_jsonable(v) for v in value]
    if isinstance(value, (bytes, bytearray)):
        return base64.b64encode(bytes(value)).decode("ascii")
    return str(value)


def _normalize_utc(value: datetime, label: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{label} must be a datetime")
    if value.tzinfo is None:
        raise ValueError(f"{label} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _parse_iso(value: Any, label: str) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    raise TypeError(
        f"{label} must be an ISO-8601 string, a datetime, or None; "
        f"got {type(value).__name__}"
    )


# ---------------------------------------------------------------------------
# PolicyDecision
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class PolicyDecision:
    """
    Structured, immutable verdict for a single proposed action.

    Every runtime action must obtain a ``PolicyDecision`` from the
    PolicyDecisionPoint before execution. The decision carries:

    * The verdict (``allow`` / ``deny`` / ``modify`` / ``escalate`` / ``fallback``)
    * The policy version under which it was issued
    * Human-readable reasons
    * Machine-actionable obligations
    * Risk level
    * Verification status
    * Whether human approval is required
    * A reference to the immutable audit event

    The class is a frozen dataclass: immutable, hashable, and safe to
    share across threads. Use the accessor helpers
    (:meth:`execution_allowed`, :meth:`was_modified`, etc.) to branch.
    """

    request_id: str
    verdict: DecisionVerdict
    policy_version: str
    reasons: List[str] = field(default_factory=list)
    obligations: List[str] = field(default_factory=list)
    risk_level: RiskLevel = RiskLevel.LOW
    verification_status: VerificationStatus = VerificationStatus.NOT_REQUIRED
    human_approval_required: bool = False
    audit_event_id: str = ""
    modified_request: Optional[Dict[str, Any]] = None
    issued_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    expires_at: Optional[datetime] = None

    # -- invariants --------------------------------------------------------
    def __post_init__(self) -> None:
        # Required non-empty strings
        if not isinstance(self.request_id, str) or not self.request_id.strip():
            raise ValueError("request_id must be a non-empty string")
        if not isinstance(self.policy_version, str) or not self.policy_version.strip():
            raise ValueError("policy_version must be a non-empty string")
        if not isinstance(self.audit_event_id, str):
            raise TypeError("audit_event_id must be a string")

        # Enum coercion for the three closed vocabularies
        if not isinstance(self.verdict, DecisionVerdict):
            object.__setattr__(self, "verdict", DecisionVerdict(self.verdict))
        if not isinstance(self.risk_level, RiskLevel):
            object.__setattr__(self, "risk_level", RiskLevel(self.risk_level))
        if not isinstance(self.verification_status, VerificationStatus):
            object.__setattr__(
                self,
                "verification_status",
                VerificationStatus(self.verification_status),
            )

        # Copy list fields so caller mutation can't leak in
        if not isinstance(self.reasons, list):
            raise TypeError("reasons must be a list of strings")
        if not isinstance(self.obligations, list):
            raise TypeError("obligations must be a list of strings")
        for label, seq in (("reasons", self.reasons), ("obligations", self.obligations)):
            for item in seq:
                if not isinstance(item, str) or not item.strip():
                    raise ValueError(f"{label} entries must be non-empty strings")
        object.__setattr__(self, "reasons", list(self.reasons))
        object.__setattr__(self, "obligations", list(self.obligations))

        # Copy / validate modified_request
        if self.modified_request is not None:
            if not isinstance(self.modified_request, Mapping):
                raise TypeError("modified_request must be a Mapping or None")
            object.__setattr__(self, "modified_request", dict(self.modified_request))

        # Timestamps: aware, UTC, ordered
        issued = _normalize_utc(self.issued_at, "issued_at")
        object.__setattr__(self, "issued_at", issued)
        if self.expires_at is not None:
            expires = _normalize_utc(self.expires_at, "expires_at")
            object.__setattr__(self, "expires_at", expires)
            if expires <= issued:
                raise ValueError("expires_at must be after issued_at")

        # --- semantic invariants -----------------------------------------
        if self.verdict is DecisionVerdict.MODIFY and self.modified_request is None:
            raise ValueError("verdict='modify' requires modified_request")
        if (
            self.verdict is not DecisionVerdict.MODIFY
            and self.modified_request is not None
        ):
            raise ValueError("modified_request is only valid with verdict='modify'")
        if (
            self.verdict is DecisionVerdict.ALLOW
            and self.human_approval_required
        ):
            raise ValueError(
                "verdict='allow' cannot require human approval; "
                "use verdict='escalate' to require a human gate"
            )
        if (
            self.verdict is DecisionVerdict.DENY
            and self.human_approval_required
        ):
            raise ValueError("verdict='deny' cannot require human approval")
        if (
            self.verdict in (DecisionVerdict.DENY, DecisionVerdict.FALLBACK)
            and not self.reasons
        ):
            raise ValueError(
                f"verdict='{self.verdict.value}' requires at least one reason"
            )

    # -- verdict accessors -------------------------------------------------
    def is_allowed(self) -> bool:
        return self.verdict is DecisionVerdict.ALLOW

    def is_blocked(self) -> bool:
        return self.verdict is DecisionVerdict.DENY

    def was_modified(self) -> bool:
        return self.verdict is DecisionVerdict.MODIFY

    def requires_human(self) -> bool:
        return (
            self.verdict is DecisionVerdict.ESCALATE
            or self.human_approval_required
        )

    # -- consumer-facing answers ------------------------------------------
    def execution_allowed(self, *, now: Optional[datetime] = None) -> bool:
        """Can the caller execute the action right now?

        A decision permits execution only when **all** of the following
        hold:

        * the verdict is ``allow`` or ``modify``,
        * the decision has not expired,
        * no human approval gate is pending,
        * verification has not failed,
        * an ``audit_event_id`` is present (so the execution is traceable).
        """
        now = now or datetime.now(timezone.utc)
        if not isinstance(now, datetime) or now.tzinfo is None:
            raise ValueError("now must be timezone-aware")
        now = now.astimezone(timezone.utc)

        if self.expires_at is not None and now > self.expires_at:
            return False
        if self.verdict not in (
            DecisionVerdict.ALLOW,
            DecisionVerdict.MODIFY,
        ):
            return False
        if self.human_approval_required:
            return False
        if self.verification_status is VerificationStatus.FAILED:
            return False
        if not self.audit_event_id:
            return False
        return True

    def execution_blocked_reason(
        self, *, now: Optional[datetime] = None
    ) -> Optional[str]:
        """Human-readable reason execution is blocked, or ``None``."""
        now = now or datetime.now(timezone.utc)
        if not isinstance(now, datetime) or now.tzinfo is None:
            raise ValueError("now must be timezone-aware")
        now = now.astimezone(timezone.utc)

        if self.expires_at is not None and now > self.expires_at:
            return "decision expired"
        if self.verdict in (DecisionVerdict.DENY, DecisionVerdict.FALLBACK):
            return f"verdict='{self.verdict.value}'"
        if self.verdict is DecisionVerdict.ESCALATE:
            return "escalation pending"
        if self.human_approval_required:
            return "human approval required"
        if self.verification_status is VerificationStatus.FAILED:
            return "verification failed"
        if not self.audit_event_id:
            return "missing audit_event_id"
        return None

    # -- serialization -----------------------------------------------------
    def to_dict(self) -> Dict[str, Any]:
        """Return a JSON-safe dict representation of this decision."""
        return {
            "request_id": self.request_id,
            "verdict": self.verdict.value,
            "policy_version": self.policy_version,
            "reasons": list(self.reasons),
            "obligations": list(self.obligations),
            "risk_level": self.risk_level.value,
            "verification_status": self.verification_status.value,
            "human_approval_required": self.human_approval_required,
            "audit_event_id": self.audit_event_id,
            "modified_request": _jsonable(self.modified_request),
            "issued_at": self.issued_at.isoformat(),
            "expires_at": (
                self.expires_at.isoformat() if self.expires_at else None
            ),
        }

    def to_json(self) -> str:
        """Canonical, compact JSON encoding of :meth:`to_dict`."""
        return json.dumps(
            self.to_dict(), separators=(",", ":"), sort_keys=True
        )

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "PolicyDecision":
        """Reconstruct a decision from its serialized form."""
        if not isinstance(data, Mapping):
            raise TypeError(
                f"data must be a Mapping, got {type(data).__name__}"
            )
        for required in ("request_id", "verdict", "policy_version"):
            if required not in data:
                raise ValueError(f"data must contain '{required}'")

        return cls(
            request_id=data["request_id"],
            verdict=DecisionVerdict(data["verdict"]),
            policy_version=data["policy_version"],
            reasons=list(data.get("reasons") or []),
            obligations=list(data.get("obligations") or []),
            risk_level=RiskLevel(data.get("risk_level", "low")),
            verification_status=VerificationStatus(
                data.get("verification_status", "not_required")
            ),
            human_approval_required=bool(
                data.get("human_approval_required", False)
            ),
            audit_event_id=data.get("audit_event_id", "") or "",
            modified_request=(
                dict(data["modified_request"])
                if data.get("modified_request") is not None
                else None
            ),
            issued_at=(
                _parse_iso(data.get("issued_at"), "issued_at")
                or datetime.now(timezone.utc)
            ),
            expires_at=_parse_iso(data.get("expires_at"), "expires_at"),
        )

    def to_schema(self) -> Dict[str, Any]:
        """Return a JSON-schema-shaped dict describing this decision."""
        return {
            "type": "object",
            "required": [
                "request_id",
                "verdict",
                "policy_version",
                "reasons",
                "obligations",
                "risk_level",
                "verification_status",
                "human_approval_required",
                "audit_event_id",
                "issued_at",
            ],
            "properties": {
                "request_id": {"type": "string", "minLength": 1},
                "verdict": {
                    "type": "string",
                    "enum": [v.value for v in DecisionVerdict],
                },
                "policy_version": {"type": "string", "minLength": 1},
                "reasons": {
                    "type": "array",
                    "items": {"type": "string", "minLength": 1},
                },
                "obligations": {
                    "type": "array",
                    "items": {"type": "string", "minLength": 1},
                },
                "risk_level": {
                    "type": "string",
                    "enum": [r.value for r in RiskLevel],
                },
                "verification_status": {
                    "type": "string",
                    "enum": [s.value for s in VerificationStatus],
                },
                "human_approval_required": {"type": "boolean"},
                "audit_event_id": {"type": "string"},
                "modified_request": {"type": ["object", "null"]},
                "issued_at": {"type": "string", "format": "date-time"},
                "expires_at": {
                    "type": ["string", "null"],
                    "format": "date-time",
                },
            },
            "additionalProperties": False,
        }

    # -- convenience -------------------------------------------------------
    @property
    def obligations_set(self) -> frozenset:
        """Obligations as a frozen set for O(1) membership tests."""
        return frozenset(self.obligations)

    def __str__(self) -> str:
        return (
            f"PolicyDecision({self.verdict.value} "
            f"{self.request_id[:8]} risk={self.risk_level.value})"
        )


# ===========================================================================
# TESTS
# ===========================================================================
class _Color(Enum):
    RED = "red"
    GREEN = "green"


class TestConstruction(unittest.TestCase):
    def test_minimal_construction(self):
        d = PolicyDecision(
            request_id="r1", verdict="allow", policy_version="v1"
        )
        self.assertIs(d.verdict, DecisionVerdict.ALLOW)
        self.assertIs(d.risk_level, RiskLevel.LOW)
        self.assertIs(d.verification_status, VerificationStatus.NOT_REQUIRED)
        self.assertEqual(d.reasons, [])
        self.assertEqual(d.obligations, [])
        self.assertFalse(d.human_approval_required)
        self.assertEqual(d.audit_event_id, "")
        self.assertIsNone(d.modified_request)
        self.assertIsNone(d.expires_at)
        self.assertIsNotNone(d.issued_at.tzinfo)

    def test_unknown_verdict_rejected(self):
        with self.assertRaises(ValueError):
            PolicyDecision(
                request_id="r1", verdict="alow", policy_version="v1"
            )

    def test_unknown_risk_rejected(self):
        with self.assertRaises(ValueError):
            PolicyDecision(
                request_id="r1", verdict="allow", policy_version="v1",
                risk_level="panic",
            )

    def test_unknown_verification_status_rejected(self):
        with self.assertRaises(ValueError):
            PolicyDecision(
                request_id="r1", verdict="allow", policy_version="v1",
                verification_status="maybe",
            )

    def test_enum_string_coercion(self):
        d = PolicyDecision(
            request_id="r1", verdict="deny", policy_version="v1",
            reasons=["policy forbids"],
            risk_level="high",
            verification_status="passed",
        )
        self.assertIs(d.verdict, DecisionVerdict.DENY)
        self.assertIs(d.risk_level, RiskLevel.HIGH)
        self.assertIs(d.verification_status, VerificationStatus.PASSED)

    def test_empty_request_id_rejected(self):
        with self.assertRaises(ValueError):
            PolicyDecision(request_id="", verdict="allow", policy_version="v1")

    def test_whitespace_request_id_rejected(self):
        with self.assertRaises(ValueError):
            PolicyDecision(
                request_id="   ", verdict="allow", policy_version="v1"
            )

    def test_empty_policy_version_rejected(self):
        with self.assertRaises(ValueError):
            PolicyDecision(request_id="r1", verdict="allow", policy_version="")

    def test_non_str_audit_event_id_rejected(self):
        with self.assertRaises(TypeError):
            PolicyDecision(
                request_id="r1", verdict="allow", policy_version="v1",
                audit_event_id=123,  # type: ignore[arg-type]
            )

    def test_non_list_reasons_rejected(self):
        with self.assertRaises(TypeError):
            PolicyDecision(
                request_id="r1", verdict="allow", policy_version="v1",
                reasons=("a",),  # type: ignore[arg-type]
            )

    def test_empty_reason_entry_rejected(self):
        with self.assertRaises(ValueError):
            PolicyDecision(
                request_id="r1", verdict="deny", policy_version="v1",
                reasons=[""],
            )

    def test_empty_obligation_entry_rejected(self):
        with self.assertRaises(ValueError):
            PolicyDecision(
                request_id="r1", verdict="allow", policy_version="v1",
                obligations=["  "],
            )


class TestSemanticInvariants(unittest.TestCase):
    def test_modify_requires_modified_request(self):
        with self.assertRaises(ValueError):
            PolicyDecision(
                request_id="r1", verdict="modify", policy_version="v1"
            )

    def test_modify_with_modified_request_ok(self):
        d = PolicyDecision(
            request_id="r1", verdict="modify", policy_version="v1",
            modified_request={"precision": "fp32"},
        )
        self.assertTrue(d.was_modified())

    def test_modified_request_only_with_modify(self):
        with self.assertRaises(ValueError):
            PolicyDecision(
                request_id="r1", verdict="allow", policy_version="v1",
                modified_request={"x": 1},
            )

    def test_allow_cannot_require_human(self):
        with self.assertRaises(ValueError):
            PolicyDecision(
                request_id="r1", verdict="allow", policy_version="v1",
                human_approval_required=True,
            )

    def test_deny_cannot_require_human(self):
        with self.assertRaises(ValueError):
            PolicyDecision(
                request_id="r1", verdict="deny", policy_version="v1",
                reasons=["no"], human_approval_required=True,
            )

    def test_deny_requires_reasons(self):
        with self.assertRaises(ValueError):
            PolicyDecision(
                request_id="r1", verdict="deny", policy_version="v1"
            )

    def test_deny_with_reason_ok(self):
        d = PolicyDecision(
            request_id="r1", verdict="deny", policy_version="v1",
            reasons=["policy forbids"],
        )
        self.assertTrue(d.is_blocked())

    def test_fallback_requires_reasons(self):
        with self.assertRaises(ValueError):
            PolicyDecision(
                request_id="r1", verdict="fallback", policy_version="v1"
            )

    def test_escalate_with_human_ok(self):
        d = PolicyDecision(
            request_id="r1", verdict="escalate", policy_version="v1",
            human_approval_required=True,
        )
        self.assertTrue(d.requires_human())


class TestTimestamp(unittest.TestCase):
    def test_naive_issued_at_rejected(self):
        with self.assertRaises(ValueError):
            PolicyDecision(
                request_id="r1", verdict="allow", policy_version="v1",
                issued_at=datetime(2024, 1, 1),
            )

    def test_naive_expires_at_rejected(self):
        with self.assertRaises(ValueError):
            PolicyDecision(
                request_id="r1", verdict="allow", policy_version="v1",
                expires_at=datetime(2024, 1, 1),
            )

    def test_non_datetime_issued_at_rejected(self):
        with self.assertRaises(TypeError):
            PolicyDecision(
                request_id="r1", verdict="allow", policy_version="v1",
                issued_at="2024-01-01",  # type: ignore[arg-type]
            )

    def test_expires_before_issued_rejected(self):
        with self.assertRaises(ValueError):
            PolicyDecision(
                request_id="r1", verdict="allow", policy_version="v1",
                issued_at=datetime(2024, 1, 2, tzinfo=timezone.utc),
                expires_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
            )

    def test_expires_equals_issued_rejected(self):
        t = datetime(2024, 1, 1, tzinfo=timezone.utc)
        with self.assertRaises(ValueError):
            PolicyDecision(
                request_id="r1", verdict="allow", policy_version="v1",
                issued_at=t, expires_at=t,
            )

    def test_offset_normalized(self):
        tz = timezone(timedelta(hours=8))
        d = PolicyDecision(
            request_id="r1", verdict="allow", policy_version="v1",
            issued_at=datetime(2024, 1, 1, 12, tzinfo=tz),
        )
        self.assertEqual(d.issued_at.tzinfo, timezone.utc)
        self.assertEqual(d.issued_at.hour, 4)


class TestImmutability(unittest.TestCase):
    def test_frozen(self):
        d = PolicyDecision(
            request_id="r1", verdict="allow", policy_version="v1"
        )
        with self.assertRaises(FrozenInstanceError):
            d.verdict = "deny"  # type: ignore[misc]

    def test_reasons_copied(self):
        src = ["a"]
        d = PolicyDecision(
            request_id="r1", verdict="allow", policy_version="v1",
            reasons=src,
        )
        src.append("b")
        self.assertEqual(d.reasons, ["a"])

    def test_obligations_copied(self):
        src = ["log"]
        d = PolicyDecision(
            request_id="r1", verdict="allow", policy_version="v1",
            obligations=src,
        )
        src.append("audit")
        self.assertEqual(d.obligations, ["log"])

    def test_modified_request_copied(self):
        src = {"k": 1}
        d = PolicyDecision(
            request_id="r1", verdict="modify", policy_version="v1",
            modified_request=src,
        )
        src["k"] = 2
        self.assertEqual(d.modified_request, {"k": 1})

    def test_hashable(self):
        d = PolicyDecision(
            request_id="r1", verdict="allow", policy_version="v1"
        )
        self.assertEqual({d, d}, {d})


class TestExecutionAllowed(unittest.TestCase):
    def _allow(self, **kwargs):
        return PolicyDecision(
            request_id="r1", verdict="allow", policy_version="v1",
            audit_event_id="audit-1", **kwargs,
        )

    def test_allow_with_audit(self):
        self.assertTrue(self._allow().execution_allowed())
        self.assertIsNone(self._allow().execution_blocked_reason())

    def test_allow_without_audit_blocks(self):
        d = PolicyDecision(
            request_id="r1", verdict="allow", policy_version="v1"
        )
        self.assertFalse(d.execution_allowed())
        self.assertEqual(
            d.execution_blocked_reason(), "missing audit_event_id"
        )

    def test_deny_blocks(self):
        d = PolicyDecision(
            request_id="r1", verdict="deny", policy_version="v1",
            reasons=["no"], audit_event_id="a",
        )
        self.assertFalse(d.execution_allowed())
        self.assertEqual(
            d.execution_blocked_reason(), "verdict='deny'"
        )

    def test_fallback_blocks(self):
        d = PolicyDecision(
            request_id="r1", verdict="fallback", policy_version="v1",
            reasons=["upstream error"], audit_event_id="a",
        )
        self.assertFalse(d.execution_allowed())
        self.assertEqual(
            d.execution_blocked_reason(), "verdict='fallback'"
        )

    def test_escalate_blocks(self):
        d = PolicyDecision(
            request_id="r1", verdict="escalate", policy_version="v1",
            human_approval_required=True, audit_event_id="a",
        )
        self.assertFalse(d.execution_allowed())
        self.assertEqual(
            d.execution_blocked_reason(), "escalation pending"
        )

    def test_expired_blocks(self):
        d = self._allow(
            issued_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
            expires_at=datetime(2024, 1, 2, tzinfo=timezone.utc),
        )
        now = datetime(2024, 1, 3, tzinfo=timezone.utc)
        self.assertFalse(d.execution_allowed(now=now))
        self.assertEqual(
            d.execution_blocked_reason(now=now), "decision expired"
        )

    def test_not_yet_expired_allowed(self):
        d = self._allow(
            issued_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
            expires_at=datetime(2024, 1, 2, tzinfo=timezone.utc),
        )
        now = datetime(2024, 1, 1, 12, tzinfo=timezone.utc)
        self.assertTrue(d.execution_allowed(now=now))

    def test_failed_verification_blocks(self):
        d = self._allow(verification_status="failed")
        self.assertFalse(d.execution_allowed())
        self.assertEqual(
            d.execution_blocked_reason(), "verification failed"
        )

    def test_modify_with_audit_allowed(self):
        d = PolicyDecision(
            request_id="r1", verdict="modify", policy_version="v1",
            modified_request={"x": 1}, audit_event_id="a",
        )
        self.assertTrue(d.execution_allowed())

    def test_naive_now_rejected(self):
        with self.assertRaises(ValueError):
            self._allow().execution_allowed(now=datetime(2024, 1, 1))


class TestSerialization(unittest.TestCase):
    def test_to_dict_shape(self):
        d = PolicyDecision(
            request_id="r1", verdict="allow", policy_version="v1"
        )
        out = d.to_dict()
        self.assertEqual(
            set(out),
            {
                "request_id", "verdict", "policy_version", "reasons",
                "obligations", "risk_level", "verification_status",
                "human_approval_required", "audit_event_id",
                "modified_request", "issued_at", "expires_at",
            },
        )
        self.assertEqual(out["verdict"], "allow")
        self.assertEqual(out["risk_level"], "low")
        self.assertEqual(out["verification_status"], "not_required")
        self.assertIsInstance(out["issued_at"], str)
        self.assertIsNone(out["expires_at"])

    def test_to_dict_json_safe(self):
        d = PolicyDecision(
            request_id="r1", verdict="modify", policy_version="v1",
            modified_request={
                "ts": datetime(2024, 1, 1, tzinfo=timezone.utc),
                "d": date(2024, 1, 1),
                "uid": UUID("12345678-1234-5678-1234-567812345678"),
                "dec": Decimal("1.5"),
                "path": Path("/tmp/x"),
                "color": _Color.RED,
                "raw": b"\x00\x01",
                "set": {1, 2, 3},
                "nested": {
                    "ts": datetime(2024, 2, 2, tzinfo=timezone.utc),
                    "list": [Decimal("2.0"), _Color.GREEN],
                },
            },
            audit_event_id="a",
        )
        payload = json.dumps(d.to_dict())  # must not raise
        self.assertIn("2024-01-01T00:00:00+00:00", payload)
        self.assertIn("12345678-1234-5678-1234-567812345678", payload)
        self.assertIn("AQI=", payload)
        self.assertIn('"red"', payload)

    def test_to_json_is_compact_and_stable(self):
        d = PolicyDecision(
            request_id="r1", verdict="allow", policy_version="v1"
        )
        s = d.to_json()
        self.assertNotIn(" ", s)
        self.assertEqual(s, d.to_json())

    def test_round_trip(self):
        d = PolicyDecision(
            request_id="r1", verdict="modify", policy_version="v1",
            reasons=["ok"], obligations=["log", "audit"],
            risk_level="high",
            verification_status="passed",
            modified_request={"x": 1},
            audit_event_id="audit-1",
            issued_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
            expires_at=datetime(2024, 2, 1, tzinfo=timezone.utc),
        )
        self.assertEqual(PolicyDecision.from_dict(d.to_dict()), d)

    def test_from_dict_missing_required(self):
        for required in ("request_id", "verdict", "policy_version"):
            payload = {
                "request_id": "r1",
                "verdict": "allow",
                "policy_version": "v1",
            }
            del payload[required]
            with self.subTest(missing=required):
                with self.assertRaises(ValueError):
                    PolicyDecision.from_dict(payload)

    def test_from_dict_non_mapping_rejected(self):
        with self.assertRaises(TypeError):
            PolicyDecision.from_dict("nope")  # type: ignore[arg-type]

    def test_from_dict_z_suffix(self):
        d = PolicyDecision.from_dict(
            {
                "request_id": "r1",
                "verdict": "allow",
                "policy_version": "v1",
                "issued_at": "2024-01-01T00:00:00Z",
            }
        )
        self.assertEqual(d.issued_at.year, 2024)
        self.assertEqual(d.issued_at.tzinfo, timezone.utc)

    def test_from_dict_missing_issued_at_uses_now(self):
        d = PolicyDecision.from_dict(
            {"request_id": "r1", "verdict": "allow", "policy_version": "v1"}
        )
        self.assertIsNotNone(d.issued_at.tzinfo)


class TestSchema(unittest.TestCase):
    def test_schema_shape(self):
        s = PolicyDecision(
            request_id="r1", verdict="allow", policy_version="v1"
        ).to_schema()
        self.assertEqual(s["type"], "object")
        self.assertEqual(
            set(s["properties"]["verdict"]["enum"]),
            {v.value for v in DecisionVerdict},
        )
        self.assertEqual(
            set(s["properties"]["risk_level"]["enum"]),
            {r.value for r in RiskLevel},
        )
        self.assertEqual(
            set(s["properties"]["verification_status"]["enum"]),
            {v.value for v in VerificationStatus},
        )
        self.assertFalse(s["additionalProperties"])


class TestObligations(unittest.TestCase):
    def test_obligations_set_is_frozenset(self):
        d = PolicyDecision(
            request_id="r1", verdict="allow", policy_version="v1",
            obligations=["log", "audit", "log"],
        )
        self.assertIsInstance(d.obligations_set, frozenset)
        self.assertEqual(
            d.obligations_set, frozenset({"log", "audit"})
        )
        self.assertIn("log", d.obligations_set)

    def test_obligations_set_cached_shape(self):
        # Even though a new frozenset is returned each call, they're equal.
        d = PolicyDecision(
            request_id="r1", verdict="allow", policy_version="v1",
            obligations=["log"],
        )
        self.assertEqual(d.obligations_set, d.obligations_set)


class TestStr(unittest.TestCase):
    def test_str_contains_verdict_id_and_risk(self):
        d = PolicyDecision(
            request_id="abcdef0123456789",
            verdict="allow",
            policy_version="v1",
            risk_level="high",
        )
        s = str(d)
        self.assertIn("allow", s)
        self.assertIn("abcdef01", s)
        self.assertIn("high", s)


if __name__ == "__main__":
    unittest.main(verbosity=2)
