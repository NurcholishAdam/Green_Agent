"""
DecisionRequest — the input to the decision point.

A single, immutable, self-validating DTO describing a proposed action
submitted to the PolicyDecisionPoint (PDP).

Key guarantees:

* ``action`` and ``request_id`` are non-empty strings. If ``request_id``
  is omitted, a UUID4 hex is generated automatically.
* ``requested_at`` is timezone-aware and normalized to UTC. Naive
  datetimes are rejected.
* ``context`` is copied on construction, so callers cannot mutate the
  request through their original reference.
* ``to_dict()`` is JSON-safe for common Python types
  (``datetime``/``UUID``/``Enum``/``Decimal``/``bytes``/``Path``/``set``).
* The class is a frozen dataclass: it is hashable, immutable after
  construction, and safe to share across threads.

Example
-------
>>> req = DecisionRequest(action="deploy_model", agent_id="agent-7")
>>> req.to_dict()["action"]
'deploy_model'
>>> len(req.request_id)
32
>>> DecisionRequest.from_dict(req.to_dict()) == req
True
"""

from __future__ import annotations

import base64
import json
import unittest
import uuid
from dataclasses import FrozenInstanceError, dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Mapping, Optional
from uuid import UUID

__all__ = ["DecisionRequest"]


# ---------------------------------------------------------------------------
# JSON-safety helper
# ---------------------------------------------------------------------------
def _jsonable(value: Any) -> Any:
    """Recursively convert a value into something ``json.dumps`` accepts.

    This is best-effort: unknown types fall back to ``str(value)`` so a
    stray object never crashes the transport layer that ships the
    request to the PDP.
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
# DecisionRequest
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class DecisionRequest:
    """
    A proposed action submitted to the PolicyDecisionPoint.

    Every runtime action must present:

        * An action verb (e.g. ``"deploy_model"``, ``"switch_precision"``)
        * A unique ``request_id`` (auto-generated UUID if omitted)
        * The full ``context`` needed by the policies
        * Optional provenance (``agent_id``, ``task_id``, ``run_id``,
          ``policy_version``)

    The request is immutable after construction. Use :meth:`with_context`
    to derive a modified copy without mutating the original.
    """

    # ``action`` is required; ``request_id`` defaults to a fresh UUID.
    # NOTE: the field order intentionally places ``action`` first so that
    # ``DecisionRequest("deploy_model")`` is a valid positional call.
    # Callers that previously relied on ``DecisionRequest(request_id,
    # action, ...)`` positionally must switch to keyword arguments.
    action: str
    request_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    agent_id: Optional[str] = None
    task_id: Optional[str] = None
    run_id: Optional[str] = None
    policy_version: Optional[str] = None
    context: Dict[str, Any] = field(default_factory=dict)
    requested_at: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    # -- invariants --------------------------------------------------------
    def __post_init__(self) -> None:
        if not isinstance(self.action, str) or not self.action.strip():
            raise ValueError("action must be a non-empty string")
        if not isinstance(self.request_id, str) or not self.request_id.strip():
            raise ValueError("request_id must be a non-empty string")
        if not isinstance(self.context, Mapping):
            raise TypeError("context must be a Mapping")
        # Copy the context so callers cannot mutate the request through
        # their original reference.
        object.__setattr__(self, "context", dict(self.context))

        if not isinstance(self.requested_at, datetime):
            raise TypeError("requested_at must be a datetime")
        if self.requested_at.tzinfo is None:
            raise ValueError("requested_at must be timezone-aware")
        object.__setattr__(
            self,
            "requested_at",
            self.requested_at.astimezone(timezone.utc),
        )

        for name in ("agent_id", "task_id", "run_id", "policy_version"):
            value = getattr(self, name)
            if value is not None and (
                not isinstance(value, str) or not value.strip()
            ):
                raise ValueError(
                    f"{name} must be a non-empty string or None"
                )

    # -- serialization -----------------------------------------------------
    def to_dict(self) -> Dict[str, Any]:
        """Return a JSON-safe dict representation of this request."""
        return {
            "request_id": self.request_id,
            "action": self.action,
            "agent_id": self.agent_id,
            "task_id": self.task_id,
            "run_id": self.run_id,
            "policy_version": self.policy_version,
            "context": _jsonable(self.context),
            "requested_at": self.requested_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "DecisionRequest":
        """Reconstruct a request from a dict (inverse of :meth:`to_dict`).

        Tolerant of unknown keys and of a missing/blank ``request_id``
        (a fresh UUID is generated). Requires ``action`` to be present.
        """
        if not isinstance(data, Mapping):
            raise TypeError(
                f"data must be a Mapping, got {type(data).__name__}"
            )
        if "action" not in data:
            raise ValueError("data must contain 'action'")

        raw_ts = data.get("requested_at")
        if isinstance(raw_ts, str):
            # Python's fromisoformat historically doesn't accept "Z";
            # normalize so both "…Z" and "…+00:00" work.
            requested_at = datetime.fromisoformat(
                raw_ts.replace("Z", "+00:00")
            )
        elif isinstance(raw_ts, datetime):
            requested_at = raw_ts
        elif raw_ts is None:
            requested_at = datetime.now(timezone.utc)
        else:
            raise TypeError(
                "requested_at must be an ISO-8601 string, a datetime, "
                f"or None; got {type(raw_ts).__name__}"
            )

        request_id = data.get("request_id") or uuid.uuid4().hex

        return cls(
            action=data["action"],
            request_id=request_id,
            agent_id=data.get("agent_id"),
            task_id=data.get("task_id"),
            run_id=data.get("run_id"),
            policy_version=data.get("policy_version"),
            context=dict(data.get("context") or {}),
            requested_at=requested_at,
        )

    def to_json(self) -> str:
        """Compact canonical JSON encoding of :meth:`to_dict`."""
        return json.dumps(self.to_dict(), separators=(",", ":"))

    def to_schema(self) -> Dict[str, Any]:
        """Return a JSON-schema-shaped dict describing this request."""
        return {
            "type": "object",
            "required": ["request_id", "action", "context", "requested_at"],
            "properties": {
                "request_id": {"type": "string", "minLength": 1},
                "action": {"type": "string", "minLength": 1},
                "agent_id": {"type": ["string", "null"]},
                "task_id": {"type": ["string", "null"]},
                "run_id": {"type": ["string", "null"]},
                "policy_version": {"type": ["string", "null"]},
                "context": {"type": "object"},
                "requested_at": {"type": "string", "format": "date-time"},
            },
            "additionalProperties": False,
        }

    # -- provenance helpers ------------------------------------------------
    def is_fully_attributed(self) -> bool:
        """True when every provenance field is populated.

        A PDP may use this to reject requests that lack full attribution
        while still letting tests construct minimal requests.
        """
        return all(
            getattr(self, name)
            for name in ("agent_id", "task_id", "run_id", "policy_version")
        )

    # -- ergonomics --------------------------------------------------------
    def with_context(self, **updates: Any) -> "DecisionRequest":
        """Return a new request whose ``context`` is merged with ``updates``."""
        merged = dict(self.context)
        merged.update(updates)
        return DecisionRequest(
            action=self.action,
            request_id=self.request_id,
            agent_id=self.agent_id,
            task_id=self.task_id,
            run_id=self.run_id,
            policy_version=self.policy_version,
            context=merged,
            requested_at=self.requested_at,
        )

    def __str__(self) -> str:
        return f"DecisionRequest({self.action} {self.request_id[:8]})"


# ===========================================================================
# TESTS
# ===========================================================================
class _Color(Enum):
    RED = "red"
    GREEN = "green"


class TestDecisionRequestConstruction(unittest.TestCase):
    def test_minimal_construction(self):
        r = DecisionRequest(action="deploy_model")
        self.assertEqual(r.action, "deploy_model")
        self.assertEqual(len(r.request_id), 32)  # uuid4().hex
        self.assertEqual(r.context, {})
        self.assertIsNotNone(r.requested_at.tzinfo)

    def test_action_is_first_positional(self):
        r = DecisionRequest("switch_precision")
        self.assertEqual(r.action, "switch_precision")

    def test_auto_generated_request_ids_are_unique(self):
        ids = {DecisionRequest(action="x").request_id for _ in range(100)}
        self.assertEqual(len(ids), 100)

    def test_empty_action_rejected(self):
        with self.assertRaises(ValueError):
            DecisionRequest(action="")

    def test_whitespace_action_rejected(self):
        with self.assertRaises(ValueError):
            DecisionRequest(action="   ")

    def test_non_str_action_rejected(self):
        with self.assertRaises(ValueError):
            DecisionRequest(action=123)  # type: ignore[arg-type]

    def test_empty_request_id_rejected(self):
        with self.assertRaises(ValueError):
            DecisionRequest(action="x", request_id="")

    def test_whitespace_request_id_rejected(self):
        with self.assertRaises(ValueError):
            DecisionRequest(action="x", request_id="\t\n")

    def test_empty_optional_field_rejected(self):
        for name in ("agent_id", "task_id", "run_id", "policy_version"):
            with self.subTest(name=name):
                with self.assertRaises(ValueError):
                    DecisionRequest(action="x", **{name: ""})

    def test_none_optional_fields_ok(self):
        r = DecisionRequest(action="x")
        self.assertIsNone(r.agent_id)
        self.assertIsNone(r.task_id)
        self.assertIsNone(r.run_id)
        self.assertIsNone(r.policy_version)

    def test_non_mapping_context_rejected(self):
        with self.assertRaises(TypeError):
            DecisionRequest(action="x", context=[("a", 1)])  # type: ignore[arg-type]

    def test_default_timestamp_is_tz_aware(self):
        r = DecisionRequest(action="x")
        self.assertIsNotNone(r.requested_at.tzinfo)
        self.assertEqual(r.requested_at.utcoffset(), timedelta(0))


class TestTimestampHandling(unittest.TestCase):
    def test_naive_datetime_rejected(self):
        with self.assertRaises(ValueError):
            DecisionRequest(action="x", requested_at=datetime(2024, 1, 1))

    def test_non_datetime_rejected(self):
        with self.assertRaises(TypeError):
            DecisionRequest(action="x", requested_at="2024-01-01")  # type: ignore[arg-type]

    def test_offset_normalized_to_utc(self):
        tz = timezone(timedelta(hours=8))
        dt = datetime(2024, 1, 1, 12, 0, 0, tzinfo=tz)
        r = DecisionRequest(action="x", requested_at=dt)
        self.assertEqual(r.requested_at.tzinfo, timezone.utc)
        self.assertEqual(r.requested_at.hour, 4)  # 12:00 +08:00 == 04:00 UTC
        self.assertEqual(r.requested_at, dt)  # instant preserved


class TestImmutability(unittest.TestCase):
    def test_cannot_set_attribute(self):
        r = DecisionRequest(action="x")
        with self.assertRaises(FrozenInstanceError):
            r.action = "y"  # type: ignore[misc]

    def test_context_is_copied(self):
        src = {"k": 1}
        r = DecisionRequest(action="x", context=src)
        src["k"] = 2
        self.assertEqual(r.context, {"k": 1})

    def test_hashable(self):
        r = DecisionRequest(action="x", request_id="fixed")
        s = {r, r}
        self.assertEqual(len(s), 1)

    def test_equality(self):
        kwargs = dict(
            action="x",
            request_id="abc",
            requested_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
        )
        self.assertEqual(DecisionRequest(**kwargs), DecisionRequest(**kwargs))


class TestSerialization(unittest.TestCase):
    def test_to_dict_shape(self):
        r = DecisionRequest(
            action="deploy_model",
            request_id="abc",
            agent_id="agent-7",
            context={"x": 1},
        )
        d = r.to_dict()
        self.assertEqual(
            set(d),
            {
                "request_id", "action", "agent_id", "task_id",
                "run_id", "policy_version", "context", "requested_at",
            },
        )
        self.assertEqual(d["request_id"], "abc")
        self.assertEqual(d["action"], "deploy_model")
        self.assertEqual(d["context"], {"x": 1})
        # requested_at is a string
        self.assertIsInstance(d["requested_at"], str)

    def test_to_dict_is_json_safe(self):
        r = DecisionRequest(
            action="x",
            context={
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
        # Must not raise.
        payload = json.dumps(r.to_dict())
        self.assertIn("2024-01-01T00:00:00+00:00", payload)
        self.assertIn("12345678-1234-5678-1234-567812345678", payload)
        self.assertIn("AQI=", payload)  # base64 of b"\x00\x01"
        self.assertIn('"red"', payload)

    def test_to_json_round_trip(self):
        r = DecisionRequest(action="x", context={"a": 1})
        decoded = json.loads(r.to_json())
        self.assertEqual(decoded["action"], "x")
        self.assertEqual(decoded["context"], {"a": 1})

    def test_to_json_is_compact(self):
        r = DecisionRequest(action="x")
        s = r.to_json()
        self.assertNotIn(" ", s)

    def test_from_dict_round_trip(self):
        r = DecisionRequest(
            action="deploy_model",
            request_id="abc",
            agent_id="a",
            task_id="t",
            run_id="r",
            policy_version="v1",
            context={"k": "v"},
        )
        self.assertEqual(DecisionRequest.from_dict(r.to_dict()), r)

    def test_from_dict_accepts_z_suffix(self):
        d = {
            "action": "x",
            "request_id": "abc",
            "requested_at": "2024-01-01T00:00:00Z",
        }
        r = DecisionRequest.from_dict(d)
        self.assertEqual(r.requested_at.year, 2024)

    def test_from_dict_regenerates_blank_request_id(self):
        d = {"action": "x", "request_id": ""}
        r = DecisionRequest.from_dict(d)
        self.assertEqual(len(r.request_id), 32)

    def test_from_dict_missing_action_raises(self):
        with self.assertRaises(ValueError):
            DecisionRequest.from_dict({"request_id": "abc"})

    def test_from_dict_non_mapping_raises(self):
        with self.assertRaises(TypeError):
            DecisionRequest.from_dict("not a mapping")  # type: ignore[arg-type]

    def test_from_dict_tolerates_unknown_keys(self):
        d = {
            "action": "x",
            "request_id": "abc",
            "requested_at": "2024-01-01T00:00:00+00:00",
            "future_field": 42,
        }
        r = DecisionRequest.from_dict(d)
        self.assertEqual(r.action, "x")

    def test_from_dict_naive_string_rejected_downstream(self):
        d = {
            "action": "x",
            "requested_at": "2024-01-01T00:00:00",  # no tz
        }
        with self.assertRaises(ValueError):
            DecisionRequest.from_dict(d)

    def test_from_dict_missing_timestamp_uses_now(self):
        r = DecisionRequest.from_dict({"action": "x"})
        self.assertIsNotNone(r.requested_at.tzinfo)


class TestSchema(unittest.TestCase):
    def test_schema_shape(self):
        s = DecisionRequest(action="x").to_schema()
        self.assertEqual(s["type"], "object")
        self.assertEqual(
            set(s["required"]),
            {"request_id", "action", "context", "requested_at"},
        )
        self.assertEqual(s["properties"]["action"]["type"], "string")
        self.assertEqual(
            s["properties"]["requested_at"]["format"], "date-time"
        )
        self.assertFalse(s["additionalProperties"])

    def test_schema_does_not_raise_on_instance(self):
        r = DecisionRequest(action="x")
        # Structural check: every required field must appear in to_dict().
        for key in r.to_schema()["required"]:
            self.assertIn(key, r.to_dict())


class TestProvenance(unittest.TestCase):
    def test_fully_attributed_true(self):
        r = DecisionRequest(
            action="x",
            agent_id="a",
            task_id="t",
            run_id="r",
            policy_version="v1",
        )
        self.assertTrue(r.is_fully_attributed())

    def test_fully_attributed_false_when_any_missing(self):
        for name in ("agent_id", "task_id", "run_id", "policy_version"):
            kwargs = dict(
                action="x",
                agent_id="a",
                task_id="t",
                run_id="r",
                policy_version="v1",
            )
            kwargs[name] = None
            with self.subTest(missing=name):
                self.assertFalse(
                    DecisionRequest(**kwargs).is_fully_attributed()
                )


class TestWithContext(unittest.TestCase):
    def test_merges_without_mutating_original(self):
        r = DecisionRequest(action="x", context={"a": 1})
        r2 = r.with_context(b=2)
        self.assertEqual(r.context, {"a": 1})
        self.assertEqual(r2.context, {"a": 1, "b": 2})

    def test_overwrites_existing_key(self):
        r = DecisionRequest(action="x", context={"a": 1})
        r2 = r.with_context(a=99)
        self.assertEqual(r2.context, {"a": 99})
        self.assertEqual(r.context, {"a": 1})

    def test_preserves_identity_fields(self):
        r = DecisionRequest(
            action="x",
            request_id="abc",
            agent_id="agent",
            requested_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
        )
        r2 = r.with_context(extra=1)
        self.assertEqual(r2.request_id, "abc")
        self.assertEqual(r2.action, "x")
        self.assertEqual(r2.agent_id, "agent")
        self.assertEqual(r2.requested_at, r.requested_at)


class TestStr(unittest.TestCase):
    def test_str_contains_action_and_short_id(self):
        r = DecisionRequest(action="deploy_model", request_id="abcdef0123456789")
        s = str(r)
        self.assertIn("deploy_model", s)
        self.assertIn("abcdef01", s)


if __name__ == "__main__":
    unittest.main(verbosity=2)
