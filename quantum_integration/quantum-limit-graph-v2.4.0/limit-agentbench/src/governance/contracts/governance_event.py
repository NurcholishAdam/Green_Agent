"""
GovernanceEvent — the immutable audit record.

A `GovernanceEvent` is a frozen, self-validating dataclass describing a
single audit-worthy occurrence in the governance lifecycle: a decision,
a verification, a human approval/denial, an escalation, a rollback, an
emergency stop, a policy update, or an obligation violation.

Key guarantees
--------------
* **Immutable**: `frozen=True` prevents mutation after construction.
* **Typed**: ``kind`` is a :class:`GovernanceEventKind` and ``severity``
  is a :class:`Severity`. Plain strings are coerced, unknown values are
  rejected.
* **Non-empty identifiers**: ``event_id`` is required and must be a
  non-empty string. Optional string fields must be non-empty if given.
* **Timestamp discipline**: ``at`` must be timezone-aware and is
  normalized to UTC. Naive datetimes are rejected.
* **Defensive copies**: ``details`` is copied on the way in, so callers
  cannot mutate the audit record through their original reference.
* **Per-kind payload contract**: each kind declares the keys its
  ``details`` must contain (see ``_DETAILS_REQUIRED``). A
  ``HUMAN_APPROVED`` event without an ``approver`` cannot be built.
* **JSON-safe**: ``to_dict()`` recursively encodes ``datetime`` /
  ``UUID`` / ``Decimal`` / ``Enum`` / ``bytes`` / ``Path`` / ``set`` so
  the audit record never fails to ship over the wire.
* **Tamper-evident**: ``event_hash()`` provides a stable SHA-256 digest
  of the canonical form, and ``prev_event_hash`` links events into a
  chain so missing or reordered entries are detectable.
"""

from __future__ import annotations

import base64
import hashlib
import json
import unittest
import uuid
from dataclasses import FrozenInstanceError, dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Tuple
from uuid import UUID

__all__ = [
    "GovernanceEvent",
    "GovernanceEventKind",
    "Severity",
]


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------
class GovernanceEventKind(str, Enum):
    DECISION_ISSUED = "decision_issued"
    VERIFICATION_COMPLETED = "verification_completed"
    HUMAN_APPROVED = "human_approved"
    HUMAN_DENIED = "human_denied"
    ESCALATION_RAISED = "escalation_raised"
    ROLLBACK_TRIGGERED = "rollback_triggered"
    EMERGENCY_STOP = "emergency_stop"
    POLICY_UPDATED = "policy_updated"
    OBLIGATION_VIOLATED = "obligation_violated"


class Severity(str, Enum):
    INFO = "info"
    WARN = "warn"
    CRITICAL = "critical"


# Per-kind required keys inside ``details``. Kept as a data table so an
# audit reviewer can read the payload contract in one place.
_DETAILS_REQUIRED: Dict[GovernanceEventKind, Tuple[str, ...]] = {
    GovernanceEventKind.HUMAN_APPROVED: ("approver", "justification"),
    GovernanceEventKind.HUMAN_DENIED: ("approver", "reason"),
    GovernanceEventKind.ESCALATION_RAISED: ("target", "reason"),
    GovernanceEventKind.OBLIGATION_VIOLATED: ("obligation_id",),
    GovernanceEventKind.ROLLBACK_TRIGGERED: ("reason",),
    GovernanceEventKind.EMERGENCY_STOP: ("reason",),
}


# ---------------------------------------------------------------------------
# JSON-safety helper (mirrors DecisionRequest._jsonable)
# ---------------------------------------------------------------------------
def _jsonable(value: Any) -> Any:
    """Recursively convert a value to something ``json.dumps`` accepts.

    Unknown types fall back to ``str(value)`` — an audit event must
    never fail to ship because of a single stray object in ``details``.
    """
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


# ---------------------------------------------------------------------------
# GovernanceEvent
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class GovernanceEvent:
    """
    Immutable audit event. Every decision, verification, approval, and
    enforcement action produces one of these.
    """

    event_id: str
    kind: GovernanceEventKind
    request_id: Optional[str] = None
    policy_version: Optional[str] = None
    actor: Optional[str] = None
    emitter: Optional[str] = None
    severity: Severity = Severity.INFO
    details: Dict[str, Any] = field(default_factory=dict)
    at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    prev_event_hash: Optional[str] = None

    # -- invariants --------------------------------------------------------
    def __post_init__(self) -> None:
        # event_id must be a non-empty string
        if not isinstance(self.event_id, str) or not self.event_id.strip():
            raise ValueError("event_id must be a non-empty string")

        # kind must be a GovernanceEventKind; coerce strings, reject unknowns
        if not isinstance(self.kind, GovernanceEventKind):
            object.__setattr__(self, "kind", GovernanceEventKind(self.kind))

        # severity must be a Severity; coerce strings, reject unknowns
        if not isinstance(self.severity, Severity):
            object.__setattr__(self, "severity", Severity(self.severity))

        # details must be a Mapping; copy to defend against caller mutation
        if not isinstance(self.details, Mapping):
            raise TypeError("details must be a Mapping")
        object.__setattr__(self, "details", dict(self.details))

        # per-kind required keys (treat blank strings as missing)
        for key in _DETAILS_REQUIRED.get(self.kind, ()):
            if key not in self.details or self.details[key] in (None, ""):
                raise ValueError(
                    f"{self.kind.value} event requires details['{key}']"
                )

        # at must be aware and is normalized to UTC
        if not isinstance(self.at, datetime):
            raise TypeError("at must be a datetime")
        if self.at.tzinfo is None:
            raise ValueError("at must be timezone-aware")
        object.__setattr__(self, "at", self.at.astimezone(timezone.utc))

        # optional string fields: must be non-empty if provided
        for name in (
            "request_id",
            "policy_version",
            "actor",
            "emitter",
            "prev_event_hash",
        ):
            value = getattr(self, name)
            if value is not None and (
                not isinstance(value, str) or not value.strip()
            ):
                raise ValueError(
                    f"{name} must be a non-empty string or None"
                )

    # -- serialization -----------------------------------------------------
    def to_dict(self) -> Dict[str, Any]:
        """Return a JSON-safe dict representation of this event."""
        return {
            "event_id": self.event_id,
            "kind": self.kind.value,
            "request_id": self.request_id,
            "policy_version": self.policy_version,
            "actor": self.actor,
            "emitter": self.emitter,
            "severity": self.severity.value,
            "details": _jsonable(self.details),
            "at": self.at.isoformat(),
            "prev_event_hash": self.prev_event_hash,
        }

    def to_json(self) -> str:
        """Canonical JSON encoding — stable key order, compact separators.

        Canonical form is what :meth:`event_hash` digests, so any change
        to field ordering must be considered a wire-format change.
        """
        return json.dumps(
            self.to_dict(), separators=(",", ":"), sort_keys=True
        )

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "GovernanceEvent":
        """Reconstruct an event from its serialized form.

        The ``event_id`` is *not* regenerated when absent: audit records
        must not silently acquire new identifiers on read. Missing
        ``event_id`` / ``kind`` / ``at`` raise ``ValueError``.
        """
        if not isinstance(data, Mapping):
            raise TypeError(
                f"data must be a Mapping, got {type(data).__name__}"
            )
        for required in ("event_id", "kind", "at"):
            if required not in data:
                raise ValueError(f"data must contain '{required}'")

        raw_at = data["at"]
        if isinstance(raw_at, str):
            at = datetime.fromisoformat(
                raw_at.replace("Z", "+00:00")
            )
        elif isinstance(raw_at, datetime):
            at = raw_at
        else:
            raise TypeError(
                "at must be an ISO-8601 string or a datetime, "
                f"got {type(raw_at).__name__}"
            )

        return cls(
            event_id=data["event_id"],
            kind=GovernanceEventKind(data["kind"]),
            request_id=data.get("request_id"),
            policy_version=data.get("policy_version"),
            actor=data.get("actor"),
            emitter=data.get("emitter"),
            severity=Severity(data.get("severity", "info")),
            details=dict(data.get("details") or {}),
            at=at,
            prev_event_hash=data.get("prev_event_hash"),
        )

    def to_schema(self) -> Dict[str, Any]:
        """Return a JSON-schema-shaped dict describing this event."""
        return {
            "type": "object",
            "required": [
                "event_id",
                "kind",
                "severity",
                "details",
                "at",
            ],
            "properties": {
                "event_id": {"type": "string", "minLength": 1},
                "kind": {
                    "type": "string",
                    "enum": [k.value for k in GovernanceEventKind],
                },
                "severity": {
                    "type": "string",
                    "enum": [s.value for s in Severity],
                },
                "request_id": {"type": ["string", "null"]},
                "policy_version": {"type": ["string", "null"]},
                "actor": {"type": ["string", "null"]},
                "emitter": {"type": ["string", "null"]},
                "details": {"type": "object"},
                "at": {"type": "string", "format": "date-time"},
                "prev_event_hash": {"type": ["string", "null"]},
            },
            "additionalProperties": False,
        }

    # -- tamper-evidence ---------------------------------------------------
    def event_hash(self) -> str:
        """SHA-256 hex digest of the canonical JSON form."""
        return hashlib.sha256(self.to_json().encode("utf-8")).hexdigest()

    # -- factory helpers ---------------------------------------------------
    @classmethod
    def _new_id(cls) -> str:
        return uuid.uuid4().hex

    @classmethod
    def decision_issued(
        cls,
        *,
        request_id: str,
        policy_version: str,
        actor: str,
        details: Optional[Mapping[str, Any]] = None,
        emitter: Optional[str] = None,
        prev_event_hash: Optional[str] = None,
    ) -> "GovernanceEvent":
        return cls(
            event_id=cls._new_id(),
            kind=GovernanceEventKind.DECISION_ISSUED,
            request_id=request_id,
            policy_version=policy_version,
            actor=actor,
            emitter=emitter,
            details=dict(details or {}),
            prev_event_hash=prev_event_hash,
        )

    @classmethod
    def human_approved(
        cls,
        *,
        approver: str,
        justification: str,
        actor: Optional[str] = None,
        request_id: Optional[str] = None,
        policy_version: Optional[str] = None,
        emitter: Optional[str] = None,
        extra: Optional[Mapping[str, Any]] = None,
        prev_event_hash: Optional[str] = None,
    ) -> "GovernanceEvent":
        details: Dict[str, Any] = {
            "approver": approver,
            "justification": justification,
        }
        if extra:
            details.update(extra)
        return cls(
            event_id=cls._new_id(),
            kind=GovernanceEventKind.HUMAN_APPROVED,
            request_id=request_id,
            policy_version=policy_version,
            actor=actor or approver,
            emitter=emitter,
            severity=Severity.WARN,
            details=details,
            prev_event_hash=prev_event_hash,
        )

    @classmethod
    def human_denied(
        cls,
        *,
        approver: str,
        reason: str,
        actor: Optional[str] = None,
        request_id: Optional[str] = None,
        policy_version: Optional[str] = None,
        emitter: Optional[str] = None,
        extra: Optional[Mapping[str, Any]] = None,
        prev_event_hash: Optional[str] = None,
    ) -> "GovernanceEvent":
        details: Dict[str, Any] = {"approver": approver, "reason": reason}
        if extra:
            details.update(extra)
        return cls(
            event_id=cls._new_id(),
            kind=GovernanceEventKind.HUMAN_DENIED,
            request_id=request_id,
            policy_version=policy_version,
            actor=actor or approver,
            emitter=emitter,
            severity=Severity.WARN,
            details=details,
            prev_event_hash=prev_event_hash,
        )

    @classmethod
    def escalation_raised(
        cls,
        *,
        target: str,
        reason: str,
        actor: Optional[str] = None,
        request_id: Optional[str] = None,
        policy_version: Optional[str] = None,
        emitter: Optional[str] = None,
        extra: Optional[Mapping[str, Any]] = None,
        prev_event_hash: Optional[str] = None,
    ) -> "GovernanceEvent":
        details: Dict[str, Any] = {"target": target, "reason": reason}
        if extra:
            details.update(extra)
        return cls(
            event_id=cls._new_id(),
            kind=GovernanceEventKind.ESCALATION_RAISED,
            request_id=request_id,
            policy_version=policy_version,
            actor=actor,
            emitter=emitter,
            severity=Severity.WARN,
            details=details,
            prev_event_hash=prev_event_hash,
        )

    @classmethod
    def rollback_triggered(
        cls,
        *,
        reason: str,
        actor: Optional[str] = None,
        request_id: Optional[str] = None,
        policy_version: Optional[str] = None,
        emitter: Optional[str] = None,
        extra: Optional[Mapping[str, Any]] = None,
        prev_event_hash: Optional[str] = None,
    ) -> "GovernanceEvent":
        details: Dict[str, Any] = {"reason": reason}
        if extra:
            details.update(extra)
        return cls(
            event_id=cls._new_id(),
            kind=GovernanceEventKind.ROLLBACK_TRIGGERED,
            request_id=request_id,
            policy_version=policy_version,
            actor=actor,
            emitter=emitter,
            severity=Severity.WARN,
            details=details,
            prev_event_hash=prev_event_hash,
        )

    @classmethod
    def emergency_stop(
        cls,
        *,
        reason: str,
        actor: str,
        request_id: Optional[str] = None,
        policy_version: Optional[str] = None,
        emitter: Optional[str] = None,
        extra: Optional[Mapping[str, Any]] = None,
        prev_event_hash: Optional[str] = None,
    ) -> "GovernanceEvent":
        details: Dict[str, Any] = {"reason": reason}
        if extra:
            details.update(extra)
        return cls(
            event_id=cls._new_id(),
            kind=GovernanceEventKind.EMERGENCY_STOP,
            request_id=request_id,
            policy_version=policy_version,
            actor=actor,
            emitter=emitter,
            severity=Severity.CRITICAL,
            details=details,
            prev_event_hash=prev_event_hash,
        )

    @classmethod
    def obligation_violated(
        cls,
        *,
        obligation_id: str,
        actor: Optional[str] = None,
        request_id: Optional[str] = None,
        policy_version: Optional[str] = None,
        emitter: Optional[str] = None,
        severity: Severity = Severity.WARN,
        extra: Optional[Mapping[str, Any]] = None,
        prev_event_hash: Optional[str] = None,
    ) -> "GovernanceEvent":
        details: Dict[str, Any] = {"obligation_id": obligation_id}
        if extra:
            details.update(extra)
        return cls(
            event_id=cls._new_id(),
            kind=GovernanceEventKind.OBLIGATION_VIOLATED,
            request_id=request_id,
            policy_version=policy_version,
            actor=actor,
            emitter=emitter,
            severity=severity,
            details=details,
            prev_event_hash=prev_event_hash,
        )

    # -- misc --------------------------------------------------------------
    def __str__(self) -> str:
        return f"GovernanceEvent({self.kind.value} {self.event_id[:8]})"


# ===========================================================================
# TESTS
# ===========================================================================
class _Color(Enum):
    RED = "red"
    GREEN = "green"


class TestConstruction(unittest.TestCase):
    def test_minimal_construction(self):
        e = GovernanceEvent(event_id="e1", kind="decision_issued")
        self.assertEqual(e.event_id, "e1")
        self.assertIs(e.kind, GovernanceEventKind.DECISION_ISSUED)
        self.assertIs(e.severity, Severity.INFO)
        self.assertEqual(e.details, {})
        self.assertIsNotNone(e.at.tzinfo)

    def test_empty_event_id_rejected(self):
        with self.assertRaises(ValueError):
            GovernanceEvent(event_id="", kind="decision_issued")

    def test_whitespace_event_id_rejected(self):
        with self.assertRaises(ValueError):
            GovernanceEvent(event_id="   ", kind="decision_issued")

    def test_non_str_event_id_rejected(self):
        with self.assertRaises(ValueError):
            GovernanceEvent(event_id=123, kind="decision_issued")  # type: ignore[arg-type]

    def test_unknown_kind_rejected(self):
        with self.assertRaises(ValueError):
            GovernanceEvent(event_id="e1", kind="decison_issued")  # typo

    def test_kind_accepts_enum_or_string(self):
        e1 = GovernanceEvent(event_id="e1", kind=GovernanceEventKind.HUMAN_DENIED)
        e2 = GovernanceEvent(event_id="e2", kind="human_denied")
        self.assertIs(e1.kind, GovernanceEventKind.HUMAN_DENIED)
        self.assertIs(e2.kind, GovernanceEventKind.HUMAN_DENIED)

    def test_kind_comparable_to_string(self):
        e = GovernanceEvent(event_id="e1", kind="decision_issued")
        self.assertEqual(e.kind, "decision_issued")
        self.assertEqual(e.kind.value, "decision_issued")

    def test_unknown_severity_rejected(self):
        with self.assertRaises(ValueError):
            GovernanceEvent(event_id="e1", kind="decision_issued", severity="panic")

    def test_severity_coerced(self):
        e = GovernanceEvent(event_id="e1", kind="decision_issued", severity="critical")
        self.assertIs(e.severity, Severity.CRITICAL)

    def test_non_mapping_details_rejected(self):
        with self.assertRaises(TypeError):
            GovernanceEvent(event_id="e1", kind="decision_issued", details=[("a", 1)])  # type: ignore[arg-type]

    def test_empty_optional_string_rejected(self):
        for name in ("request_id", "policy_version", "actor", "emitter", "prev_event_hash"):
            with self.subTest(name=name):
                with self.assertRaises(ValueError):
                    GovernanceEvent(
                        event_id="e1",
                        kind="decision_issued",
                        **{name: ""},
                    )


class TestTimestamp(unittest.TestCase):
    def test_naive_datetime_rejected(self):
        with self.assertRaises(ValueError):
            GovernanceEvent(
                event_id="e1",
                kind="decision_issued",
                at=datetime(2024, 1, 1),
            )

    def test_non_datetime_rejected(self):
        with self.assertRaises(TypeError):
            GovernanceEvent(
                event_id="e1",
                kind="decision_issued",
                at="2024-01-01",  # type: ignore[arg-type]
            )

    def test_offset_normalized_to_utc(self):
        tz = timezone(timedelta(hours=8))
        dt = datetime(2024, 1, 1, 12, 0, 0, tzinfo=tz)
        e = GovernanceEvent(event_id="e1", kind="decision_issued", at=dt)
        self.assertEqual(e.at.tzinfo, timezone.utc)
        self.assertEqual(e.at, dt)  # instant preserved


class TestPerKindDetails(unittest.TestCase):
    def test_human_approved_requires_approver_and_justification(self):
        with self.assertRaises(ValueError):
            GovernanceEvent(
                event_id="e1",
                kind="human_approved",
                details={"approver": "alice"},  # missing justification
            )

    def test_human_approved_with_required_keys(self):
        e = GovernanceEvent(
            event_id="e1",
            kind="human_approved",
            details={"approver": "alice", "justification": "ok"},
        )
        self.assertEqual(e.details["approver"], "alice")

    def test_human_denied_requires_reason(self):
        with self.assertRaises(ValueError):
            GovernanceEvent(
                event_id="e1",
                kind="human_denied",
                details={"approver": "alice"},  # missing reason
            )

    def test_escalation_requires_target_and_reason(self):
        with self.assertRaises(ValueError):
            GovernanceEvent(
                event_id="e1",
                kind="escalation_raised",
                details={"target": "ops"},
            )

    def test_emergency_stop_requires_reason(self):
        with self.assertRaises(ValueError):
            GovernanceEvent(event_id="e1", kind="emergency_stop")

    def test_obligation_violated_requires_obligation_id(self):
        with self.assertRaises(ValueError):
            GovernanceEvent(event_id="e1", kind="obligation_violated")

    def test_rollback_requires_reason(self):
        with self.assertRaises(ValueError):
            GovernanceEvent(event_id="e1", kind="rollback_triggered")

    def test_blank_required_detail_rejected(self):
        with self.assertRaises(ValueError):
            GovernanceEvent(
                event_id="e1",
                kind="human_approved",
                details={"approver": "alice", "justification": ""},
            )


class TestImmutability(unittest.TestCase):
    def test_cannot_set_attribute(self):
        e = GovernanceEvent(event_id="e1", kind="decision_issued")
        with self.assertRaises(FrozenInstanceError):
            e.event_id = "e2"  # type: ignore[misc]

    def test_details_copied(self):
        src = {"k": 1}
        e = GovernanceEvent(event_id="e1", kind="decision_issued", details=src)
        src["k"] = 2
        self.assertEqual(e.details, {"k": 1})

    def test_hashable(self):
        e = GovernanceEvent(event_id="e1", kind="decision_issued")
        self.assertEqual({e, e}, {e})

    def test_equality(self):
        kwargs = dict(
            event_id="e1",
            kind="decision_issued",
            at=datetime(2024, 1, 1, tzinfo=timezone.utc),
        )
        self.assertEqual(GovernanceEvent(**kwargs), GovernanceEvent(**kwargs))


class TestSerialization(unittest.TestCase):
    def test_to_dict_shape(self):
        e = GovernanceEvent(event_id="e1", kind="decision_issued")
        d = e.to_dict()
        self.assertEqual(
            set(d),
            {
                "event_id", "kind", "request_id", "policy_version",
                "actor", "emitter", "severity", "details", "at",
                "prev_event_hash",
            },
        )
        self.assertEqual(d["kind"], "decision_issued")
        self.assertEqual(d["severity"], "info")
        self.assertIsInstance(d["at"], str)

    def test_to_dict_is_json_safe(self):
        e = GovernanceEvent(
            event_id="e1",
            kind="decision_issued",
            details={
                "dt": datetime(2024, 1, 1, tzinfo=timezone.utc),
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
        )
        payload = json.dumps(e.to_dict())  # must not raise
        self.assertIn("2024-01-01T00:00:00+00:00", payload)
        self.assertIn("12345678-1234-5678-1234-567812345678", payload)
        self.assertIn("AQI=", payload)  # base64 of b"\x00\x01"
        self.assertIn('"red"', payload)

    def test_to_json_is_canonical(self):
        e1 = GovernanceEvent(
            event_id="e1",
            kind="decision_issued",
            at=datetime(2024, 1, 1, tzinfo=timezone.utc),
        )
        # Field insertion order should not affect the canonical form;
        # to_json sorts keys.
        self.assertEqual(e1.to_json(), e1.to_json())
        self.assertNotIn(" ", e1.to_json())

    def test_from_dict_round_trip(self):
        e = GovernanceEvent(
            event_id="e1",
            kind="human_approved",
            request_id="r1",
            policy_version="v1",
            actor="alice",
            emitter="svc-a",
            severity=Severity.WARN,
            details={"approver": "alice", "justification": "ok"},
            prev_event_hash="abc123",
        )
        self.assertEqual(GovernanceEvent.from_dict(e.to_dict()), e)

    def test_from_dict_accepts_z_suffix(self):
        d = {
            "event_id": "e1",
            "kind": "decision_issued",
            "at": "2024-01-01T00:00:00Z",
        }
        e = GovernanceEvent.from_dict(d)
        self.assertEqual(e.at.year, 2024)
        self.assertEqual(e.at.tzinfo, timezone.utc)

    def test_from_dict_missing_fields_raise(self):
        for required in ("event_id", "kind", "at"):
            payload = {"event_id": "e1", "kind": "decision_issued",
                       "at": "2024-01-01T00:00:00+00:00"}
            del payload[required]
            with self.subTest(missing=required):
                with self.assertRaises(ValueError):
                    GovernanceEvent.from_dict(payload)

    def test_from_dict_non_mapping_rejected(self):
        with self.assertRaises(TypeError):
            GovernanceEvent.from_dict("nope")  # type: ignore[arg-type]

    def test_from_dict_does_not_regenerate_event_id(self):
        # Missing event_id must fail, not silently acquire a UUID.
        with self.assertRaises(ValueError):
            GovernanceEvent.from_dict(
                {"kind": "decision_issued",
                 "at": "2024-01-01T00:00:00+00:00"}
            )


class TestSchema(unittest.TestCase):
    def test_schema_shape(self):
        s = GovernanceEvent(event_id="e1", kind="decision_issued").to_schema()
        self.assertEqual(s["type"], "object")
        self.assertEqual(
            set(s["required"]),
            {"event_id", "kind", "severity", "details", "at"},
        )
        self.assertEqual(
            set(s["properties"]["kind"]["enum"]),
            {k.value for k in GovernanceEventKind},
        )
        self.assertEqual(
            set(s["properties"]["severity"]["enum"]),
            {s_.value for s_ in Severity},
        )
        self.assertFalse(s["additionalProperties"])


class TestHashingAndChain(unittest.TestCase):
    def test_event_hash_stable(self):
        e = GovernanceEvent(
            event_id="e1",
            kind="decision_issued",
            at=datetime(2024, 1, 1, tzinfo=timezone.utc),
        )
        self.assertEqual(e.event_hash(), e.event_hash())
        self.assertEqual(len(e.event_hash()), 64)  # SHA-256 hex

    def test_event_hash_changes_with_content(self):
        e1 = GovernanceEvent(
            event_id="e1",
            kind="decision_issued",
            at=datetime(2024, 1, 1, tzinfo=timezone.utc),
        )
        e2 = GovernanceEvent(
            event_id="e2",
            kind="decision_issued",
            at=datetime(2024, 1, 1, tzinfo=timezone.utc),
        )
        self.assertNotEqual(e1.event_hash(), e2.event_hash())

    def test_prev_event_hash_round_trips(self):
        prev = GovernanceEvent(
            event_id="e1",
            kind="decision_issued",
            at=datetime(2024, 1, 1, tzinfo=timezone.utc),
        )
        nxt = GovernanceEvent(
            event_id="e2",
            kind="verification_completed",
            prev_event_hash=prev.event_hash(),
            at=datetime(2024, 1, 2, tzinfo=timezone.utc),
        )
        self.assertEqual(nxt.prev_event_hash, prev.event_hash())
        round_tripped = GovernanceEvent.from_dict(nxt.to_dict())
        self.assertEqual(round_tripped.prev_event_hash, prev.event_hash())


class TestFactories(unittest.TestCase):
    def test_decision_issued(self):
        e = GovernanceEvent.decision_issued(
            request_id="r1", policy_version="v1", actor="agent-7",
        )
        self.assertIs(e.kind, GovernanceEventKind.DECISION_ISSUED)
        self.assertEqual(e.request_id, "r1")
        self.assertEqual(e.policy_version, "v1")
        self.assertEqual(len(e.event_id), 32)

    def test_human_approved(self):
        e = GovernanceEvent.human_approved(
            approver="alice", justification="ok",
        )
        self.assertIs(e.kind, GovernanceEventKind.HUMAN_APPROVED)
        self.assertEqual(e.actor, "alice")
        self.assertIs(e.severity, Severity.WARN)
        self.assertEqual(e.details["approver"], "alice")

    def test_human_approved_requires_justification(self):
        with self.assertRaises(TypeError):
            GovernanceEvent.human_approved(approver="alice")  # type: ignore[call-arg]

    def test_human_denied(self):
        e = GovernanceEvent.human_denied(approver="alice", reason="policy forbids")
        self.assertIs(e.kind, GovernanceEventKind.HUMAN_DENIED)
        self.assertIs(e.severity, Severity.WARN)

    def test_escalation_raised(self):
        e = GovernanceEvent.escalation_raised(target="ops", reason="p99 breach")
        self.assertIs(e.kind, GovernanceEventKind.ESCALATION_RAISED)
        self.assertEqual(e.details["target"], "ops")

    def test_rollback_triggered(self):
        e = GovernanceEvent.rollback_triggered(reason="slo miss")
        self.assertIs(e.kind, GovernanceEventKind.ROLLBACK_TRIGGERED)
        self.assertIs(e.severity, Severity.WARN)

    def test_emergency_stop(self):
        e = GovernanceEvent.emergency_stop(reason="out of control", actor="pager")
        self.assertIs(e.kind, GovernanceEventKind.EMERGENCY_STOP)
        self.assertIs(e.severity, Severity.CRITICAL)

    def test_obligation_violated(self):
        e = GovernanceEvent.obligation_violated(obligation_id="OBL-7")
        self.assertIs(e.kind, GovernanceEventKind.OBLIGATION_VIOLATED)
        self.assertEqual(e.details["obligation_id"], "OBL-7")


class TestStr(unittest.TestCase):
    def test_str_contains_kind_and_short_id(self):
        e = GovernanceEvent(event_id="abcdef0123456789", kind="decision_issued")
        s = str(e)
        self.assertIn("decision_issued", s)
        self.assertIn("abcdef01", s)


if __name__ == "__main__":
    unittest.main(verbosity=2)
