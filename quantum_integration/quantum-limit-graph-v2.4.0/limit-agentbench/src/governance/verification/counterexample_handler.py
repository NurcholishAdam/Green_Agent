"""
CounterexampleHandler — decides what to do when verification fails.

Given the counterexamples produced by formal verification, decide
whether the action should be ``allow``ed, ``deny``'d, ``escalate``'d
to a human, or sent to a ``fallback`` action.

Key guarantees
--------------
* **Fail-closed.** A ``None``, non-list, or malformed counterexample
  collection is treated as **invalid input** and returns ``DENY`` with
  ``code=INVALID_INPUT``. A broken verifier cannot authorize an action
  by returning nothing useful.
* **Typed.** Both ``action`` and ``code`` are closed-vocabulary
  enums; plain strings are coerced at construction and unknown values
  raise ``ValueError``.
* **Immutable.** Both dataclasses are frozen; the response carries a
  ``Tuple`` of defensively-copied counterexample mappings, so callers
  cannot mutate the audit record after the handler returned it.
* **Configurable.** ``on_counterexamples`` chooses ``DENY`` /
  ``ESCALATE`` / ``FALLBACK`` for the base case, and ``escalate_above``
  optionally escalates when the counterexample count reaches a
  threshold (e.g. "few issues → deny; many → human review").
* **Bounded.** ``max_kept`` caps how many counterexamples are carried
  in the response; the true count is exposed via ``total_count``, and
  ``truncated`` reports whether the cap was hit.
* **Round-trippable.** ``to_dict`` / ``from_dict`` / ``to_schema``
  match the conventions of the rest of the governance stack, and
  ``CounterexampleResponse.to_dict`` is JSON-safe for nested values
  (``datetime``, ``UUID``, ``Enum``, ``Decimal``, ``bytes``, …).

Note on the ``ALLOW`` action
----------------------------
``Action.ALLOW`` in the response means *the handler ran and found no
counterexamples*. It does **not** mean the handler was never invoked.
Callers that need to distinguish "handler skipped" from "handler found
nothing" must track that externally — the response itself does not
carry that information.
"""

from __future__ import annotations

import base64
import json
import math
import numbers
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
    List,
    Mapping,
    Optional,
    Tuple,
    Union,
)
from uuid import UUID


__all__ = [
    "Action",
    "ReasonCode",
    "CounterexampleResponse",
    "CounterexampleHandler",
    "DEFAULT_COUNTEREXAMPLE_HANDLER",
]


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------
class Action(str, Enum):
    """The decision the handler recommends for the caller."""

    ALLOW = "allow"
    DENY = "deny"
    ESCALATE = "escalate"
    FALLBACK = "fallback"


class ReasonCode(str, Enum):
    """Machine-readable codes explaining the response's ``action``."""

    NONE = "none"
    COUNTEREXAMPLES_PRESENT = "counterexamples_present"
    ESCALATION_THRESHOLD_EXCEEDED = "escalation_threshold_exceeded"
    INVALID_INPUT = "invalid_input"


# ---------------------------------------------------------------------------
# JSON-safety helper (mirrors DecisionRequest / PolicyDecision / …)
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
# CounterexampleResponse
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class CounterexampleResponse:
    """
    Result of :meth:`CounterexampleHandler.handle`.

    ``counterexamples`` holds at most ``max_kept`` items; ``total_count``
    is the true number the handler saw; ``truncated`` reports the
    difference. Every mapping in ``counterexamples`` is a shallow copy
    of the input — mutating the caller's dicts will not affect the
    response.
    """

    action: Action
    reason: str
    counterexamples: Tuple[Mapping[str, Any], ...] = ()
    code: ReasonCode = ReasonCode.NONE
    total_count: int = 0

    def __post_init__(self) -> None:
        if not isinstance(self.action, Action):
            object.__setattr__(self, "action", Action(self.action))
        if not isinstance(self.reason, str):
            raise TypeError(
                f"reason must be a string, got "
                f"{type(self.reason).__name__}"
            )
        if not isinstance(self.code, ReasonCode):
            object.__setattr__(self, "code", ReasonCode(self.code))

        if not isinstance(self.counterexamples, (tuple, list)):
            raise TypeError(
                "counterexamples must be a tuple or list of mappings"
            )
        normalized: List[Mapping[str, Any]] = []
        for i, item in enumerate(self.counterexamples):
            if not isinstance(item, MappingABC):
                raise TypeError(
                    f"counterexamples[{i}] must be a Mapping, got "
                    f"{type(item).__name__}"
                )
            normalized.append(dict(item))
        object.__setattr__(self, "counterexamples", tuple(normalized))

        if not isinstance(self.total_count, int) or isinstance(
            self.total_count, bool
        ):
            raise TypeError("total_count must be an int")
        if self.total_count < len(self.counterexamples):
            raise ValueError(
                "total_count must be >= len(counterexamples)"
            )

    @property
    def truncated(self) -> bool:
        return self.total_count > len(self.counterexamples)

    # -- serialization -----------------------------------------------------
    def to_dict(self) -> Dict[str, Any]:
        return {
            "action": self.action.value,
            "reason": self.reason,
            "code": self.code.value,
            "counterexamples": [
                _jsonable(cx) for cx in self.counterexamples
            ],
            "total_count": self.total_count,
            "truncated": self.truncated,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "CounterexampleResponse":
        if not isinstance(data, MappingABC):
            raise TypeError(
                f"data must be a Mapping, got {type(data).__name__}"
            )
        return cls(
            action=Action(data["action"]),
            reason=data.get("reason", ""),
            counterexamples=tuple(
                data.get("counterexamples", ())
            ),
            code=ReasonCode(data.get("code", "none")),
            total_count=int(
                data.get(
                    "total_count", len(data.get("counterexamples", ())),
                )
            ),
        )

    def to_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "required": ["action", "reason", "code", "counterexamples"],
            "properties": {
                "action": {
                    "type": "string",
                    "enum": [a.value for a in Action],
                },
                "reason": {"type": "string"},
                "code": {
                    "type": "string",
                    "enum": [c.value for c in ReasonCode],
                },
                "counterexamples": {
                    "type": "array",
                    "items": {"type": "object"},
                },
                "total_count": {"type": "integer", "minimum": 0},
                "truncated": {"type": "boolean"},
            },
            "additionalProperties": False,
        }

    def __str__(self) -> str:
        return (
            f"CounterexampleResponse({self.action.value}: "
            f"{len(self.counterexamples)}/"
            f"{self.total_count} counterexamples)"
        )


# ---------------------------------------------------------------------------
# CounterexampleHandler
# ---------------------------------------------------------------------------
_DEFAULT_MAX_KEPT = 32


@dataclass(frozen=True)
class CounterexampleHandler:
    """
    Default policy: deny on any counterexample.

    Configuration:

    * ``on_counterexamples``: the action to take when there are one or
      more counterexamples. Must be ``DENY``, ``ESCALATE``, or
      ``FALLBACK`` (``ALLOW`` is rejected — it would defeat the
      handler's purpose).
    * ``escalate_above``: optional threshold. When the number of
      counterexamples is at least this value, ``ESCALATE`` overrides
      ``on_counterexamples``. Useful for "few → deny; many → human".
    * ``max_kept``: maximum counterexamples carried in the response.
      The rest are summarized in ``total_count`` and ``truncated``.
    """

    on_counterexamples: Action = Action.DENY
    escalate_above: Optional[int] = None
    max_kept: int = _DEFAULT_MAX_KEPT

    def __post_init__(self) -> None:
        if not isinstance(self.on_counterexamples, Action):
            object.__setattr__(
                self,
                "on_counterexamples",
                Action(self.on_counterexamples),
            )
        if self.on_counterexamples is Action.ALLOW:
            raise ValueError(
                "on_counterexamples must be DENY, ESCALATE, or "
                "FALLBACK (ALLOW would defeat the handler)"
            )

        if self.escalate_above is not None:
            if isinstance(self.escalate_above, bool) or not isinstance(
                self.escalate_above, int
            ):
                raise TypeError(
                    "escalate_above must be an int or None"
                )
            if self.escalate_above < 1:
                raise ValueError(
                    "escalate_above must be >= 1 or None"
                )

        if isinstance(self.max_kept, bool) or not isinstance(
            self.max_kept, int
        ):
            raise TypeError("max_kept must be an int")
        if self.max_kept < 1:
            raise ValueError("max_kept must be >= 1")

    # -- handling ----------------------------------------------------------
    def handle(self, counterexamples: Any) -> CounterexampleResponse:
        """
        Decide the action for ``counterexamples``.

        Returns a fail-closed ``DENY`` when the input is not a list or
        tuple of mappings — a broken verifier must not be able to
        authorize an action by passing nothing useful.
        """
        # -- Fail-closed validation ------------------------------------
        if counterexamples is None:
            return CounterexampleResponse(
                action=Action.DENY,
                reason="counterexample collection is None",
                code=ReasonCode.INVALID_INPUT,
                total_count=0,
            )
        if not isinstance(counterexamples, (list, tuple)):
            return CounterexampleResponse(
                action=Action.DENY,
                reason=(
                    f"counterexamples must be a list or tuple, got "
                    f"{type(counterexamples).__name__}"
                ),
                code=ReasonCode.INVALID_INPUT,
                total_count=0,
            )
        for i, item in enumerate(counterexamples):
            if not isinstance(item, MappingABC):
                return CounterexampleResponse(
                    action=Action.DENY,
                    reason=(
                        f"counterexamples[{i}] is "
                        f"{type(item).__name__}, expected a Mapping"
                    ),
                    code=ReasonCode.INVALID_INPUT,
                    total_count=len(counterexamples),
                )

        # -- Happy path: empty ----------------------------------------
        if not counterexamples:
            return CounterexampleResponse(
                action=Action.ALLOW,
                reason="no counterexamples",
                code=ReasonCode.NONE,
                total_count=0,
            )

        # -- Build a bounded list of copies ---------------------------
        total = len(counterexamples)
        kept = tuple(
            dict(cx) for cx in counterexamples[: self.max_kept]
        )
        truncated_note = ""
        if total > self.max_kept:
            truncated_note = (
                f" (showing first {self.max_kept} of {total})"
            )

        # -- Escalation threshold overrides the base action -----------
        if (
            self.escalate_above is not None
            and total >= self.escalate_above
        ):
            return CounterexampleResponse(
                action=Action.ESCALATE,
                reason=(
                    f"{total} counterexample(s) exceed escalation "
                    f"threshold {self.escalate_above}"
                    f"{truncated_note}"
                ),
                counterexamples=kept,
                code=ReasonCode.ESCALATION_THRESHOLD_EXCEEDED,
                total_count=total,
            )

        return CounterexampleResponse(
            action=self.on_counterexamples,
            reason=(
                f"{total} counterexample(s) present{truncated_note}"
            ),
            counterexamples=kept,
            code=ReasonCode.COUNTEREXAMPLES_PRESENT,
            total_count=total,
        )

    # -- introspection -----------------------------------------------------
    def describe(self) -> str:
        parts = [f"on_counterexamples={self.on_counterexamples.value}"]
        if self.escalate_above is not None:
            parts.append(f"escalate_above={self.escalate_above}")
        parts.append(f"max_kept={self.max_kept}")
        return f"CounterexampleHandler({', '.join(parts)})"

    def __str__(self) -> str:
        return self.describe()

    def __repr__(self) -> str:
        return self.describe()

    # -- serialization -----------------------------------------------------
    def to_dict(self) -> Dict[str, Any]:
        return {
            "on_counterexamples": self.on_counterexamples.value,
            "escalate_above": self.escalate_above,
            "max_kept": self.max_kept,
        }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any],
    ) -> "CounterexampleHandler":
        if not isinstance(data, MappingABC):
            raise TypeError(
                f"data must be a Mapping, got {type(data).__name__}"
            )
        return cls(
            on_counterexamples=Action(
                data.get("on_counterexamples", "deny")
            ),
            escalate_above=data.get("escalate_above"),
            max_kept=int(data.get("max_kept", _DEFAULT_MAX_KEPT)),
        )

    def to_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "properties": {
                "on_counterexamples": {
                    "type": "string",
                    "enum": [
                        a.value for a in Action
                        if a is not Action.ALLOW
                    ],
                },
                "escalate_above": {
                    "type": ["integer", "null"],
                    "minimum": 1,
                },
                "max_kept": {"type": "integer", "minimum": 1},
            },
            "additionalProperties": False,
        }


# ---------------------------------------------------------------------------
# Convenience singleton
# ---------------------------------------------------------------------------
DEFAULT_COUNTEREXAMPLE_HANDLER: CounterexampleHandler = CounterexampleHandler()


# ===========================================================================
# TESTS
# ===========================================================================
def _cx(**kwargs: Any) -> Dict[str, Any]:
    defaults: Dict[str, Any] = {
        "property": "safety_bound",
        "trace": [1, 2, 3],
    }
    defaults.update(kwargs)
    return defaults


class TestHandlerConstruction(unittest.TestCase):
    def test_defaults(self):
        h = CounterexampleHandler()
        self.assertIs(h.on_counterexamples, Action.DENY)
        self.assertIsNone(h.escalate_above)
        self.assertEqual(h.max_kept, 32)

    def test_frozen(self):
        from dataclasses import FrozenInstanceError
        h = CounterexampleHandler()
        with self.assertRaises(FrozenInstanceError):
            h.max_kept = 10  # type: ignore[misc]

    def test_on_counterexamples_allow_rejected(self):
        with self.assertRaises(ValueError):
            CounterexampleHandler(on_counterexamples=Action.ALLOW)

    def test_on_counterexamples_string_coerced(self):
        h = CounterexampleHandler(on_counterexamples="escalate")
        self.assertIs(h.on_counterexamples, Action.ESCALATE)

    def test_on_counterexamples_wrong_case_rejected(self):
        with self.assertRaises(ValueError):
            CounterexampleHandler(on_counterexamples="Deny")  # type: ignore[arg-type]

    def test_escalate_above_zero_rejected(self):
        with self.assertRaises(ValueError):
            CounterexampleHandler(escalate_above=0)

    def test_escalate_above_negative_rejected(self):
        with self.assertRaises(ValueError):
            CounterexampleHandler(escalate_above=-1)

    def test_escalate_above_non_int_rejected(self):
        with self.assertRaises(TypeError):
            CounterexampleHandler(escalate_above="3")  # type: ignore[arg-type]

    def test_max_kept_zero_rejected(self):
        with self.assertRaises(ValueError):
            CounterexampleHandler(max_kept=0)

    def test_max_kept_non_int_rejected(self):
        with self.assertRaises(TypeError):
            CounterexampleHandler(max_kept="32")  # type: ignore[arg-type]

    def test_default_singleton_valid(self):
        self.assertIsInstance(
            DEFAULT_COUNTEREXAMPLE_HANDLER, CounterexampleHandler,
        )


class TestHandlerHappyPath(unittest.TestCase):
    def test_empty_allows(self):
        h = CounterexampleHandler()
        r = h.handle([])
        self.assertIs(r.action, Action.ALLOW)
        self.assertIs(r.code, ReasonCode.NONE)
        self.assertEqual(r.total_count, 0)
        self.assertFalse(r.truncated)

    def test_single_denies(self):
        h = CounterexampleHandler()
        r = h.handle([_cx()])
        self.assertIs(r.action, Action.DENY)
        self.assertIs(r.code, ReasonCode.COUNTEREXAMPLES_PRESENT)
        self.assertEqual(r.total_count, 1)
        self.assertEqual(len(r.counterexamples), 1)

    def test_multiple_denies(self):
        h = CounterexampleHandler()
        r = h.handle([_cx(), _cx(property="other")])
        self.assertIs(r.action, Action.DENY)
        self.assertEqual(r.total_count, 2)
        self.assertEqual(len(r.counterexamples), 2)

    def test_configurable_escalate(self):
        h = CounterexampleHandler(on_counterexamples=Action.ESCALATE)
        r = h.handle([_cx()])
        self.assertIs(r.action, Action.ESCALATE)

    def test_configurable_fallback(self):
        h = CounterexampleHandler(on_counterexamples=Action.FALLBACK)
        r = h.handle([_cx()])
        self.assertIs(r.action, Action.FALLBACK)


class TestHandlerEscalateThreshold(unittest.TestCase):
    def test_below_threshold_uses_base_action(self):
        h = CounterexampleHandler(
            on_counterexamples=Action.DENY, escalate_above=3,
        )
        r = h.handle([_cx(), _cx()])
        self.assertIs(r.action, Action.DENY)
        self.assertIs(r.code, ReasonCode.COUNTEREXAMPLES_PRESENT)

    def test_at_threshold_escalates(self):
        h = CounterexampleHandler(
            on_counterexamples=Action.DENY, escalate_above=3,
        )
        r = h.handle([_cx() for _ in range(3)])
        self.assertIs(r.action, Action.ESCALATE)
        self.assertIs(r.code, ReasonCode.ESCALATION_THRESHOLD_EXCEEDED)

    def test_above_threshold_escalates(self):
        h = CounterexampleHandler(
            on_counterexamples=Action.DENY, escalate_above=3,
        )
        r = h.handle([_cx() for _ in range(10)])
        self.assertIs(r.action, Action.ESCALATE)

    def test_no_threshold_never_escalates(self):
        h = CounterexampleHandler(
            on_counterexamples=Action.DENY, escalate_above=None,
        )
        r = h.handle([_cx() for _ in range(1000)])
        self.assertIs(r.action, Action.DENY)


class TestHandlerFailClosed(unittest.TestCase):
    def test_none_input_denies(self):
        # Regression: original returned ALLOW.
        h = CounterexampleHandler()
        r = h.handle(None)  # type: ignore[arg-type]
        self.assertIs(r.action, Action.DENY)
        self.assertIs(r.code, ReasonCode.INVALID_INPUT)

    def test_string_input_denies(self):
        h = CounterexampleHandler()
        r = h.handle("not a list")  # type: ignore[arg-type]
        self.assertIs(r.action, Action.DENY)
        self.assertIs(r.code, ReasonCode.INVALID_INPUT)

    def test_int_input_denies(self):
        h = CounterexampleHandler()
        r = h.handle(42)  # type: ignore[arg-type]
        self.assertIs(r.action, Action.DENY)
        self.assertIs(r.code, ReasonCode.INVALID_INPUT)

    def test_none_item_denies(self):
        h = CounterexampleHandler()
        r = h.handle([None])  # type: ignore[list-item]
        self.assertIs(r.action, Action.DENY)
        self.assertIs(r.code, ReasonCode.INVALID_INPUT)

    def test_mixed_item_denies(self):
        h = CounterexampleHandler()
        r = h.handle([{"x": 1}, 42])  # type: ignore[list-item]
        self.assertIs(r.action, Action.DENY)
        self.assertIs(r.code, ReasonCode.INVALID_INPUT)

    def test_tuple_item_denies(self):
        h = CounterexampleHandler()
        r = h.handle([("x", 1)])  # type: ignore[list-item]
        self.assertIs(r.action, Action.DENY)
        self.assertIs(r.code, ReasonCode.INVALID_INPUT)


class TestHandlerTruncation(unittest.TestCase):
    def test_truncates_above_max_kept(self):
        h = CounterexampleHandler(max_kept=2)
        r = h.handle([_cx(n=i) for i in range(5)])
        self.assertEqual(len(r.counterexamples), 2)
        self.assertEqual(r.total_count, 5)
        self.assertTrue(r.truncated)
        self.assertIn("5", r.reason)

    def test_no_truncation_under_max_kept(self):
        h = CounterexampleHandler(max_kept=32)
        r = h.handle([_cx(n=i) for i in range(5)])
        self.assertEqual(len(r.counterexamples), 5)
        self.assertEqual(r.total_count, 5)
        self.assertFalse(r.truncated)


class TestResponseImmutability(unittest.TestCase):
    def test_response_frozen(self):
        from dataclasses import FrozenInstanceError
        h = CounterexampleHandler()
        r = h.handle([_cx()])
        with self.assertRaises(FrozenInstanceError):
            r.action = Action.ALLOW  # type: ignore[misc]

    def test_caller_list_mutation_does_not_affect_response(self):
        h = CounterexampleHandler()
        cxs = [_cx()]
        r = h.handle(cxs)
        cxs.append(_cx(property="new"))
        self.assertEqual(len(r.counterexamples), 1)

    def test_caller_dict_mutation_does_not_affect_response(self):
        h = CounterexampleHandler()
        cx = _cx(property="original")
        r = h.handle([cx])
        cx["property"] = "mutated"
        self.assertEqual(
            r.counterexamples[0]["property"], "original",
        )


class TestResponseConstruction(unittest.TestCase):
    def test_string_action_coerced(self):
        r = CounterexampleResponse(action="deny", reason="x")  # type: ignore[arg-type]
        self.assertIs(r.action, Action.DENY)

    def test_unknown_action_rejected(self):
        with self.assertRaises(ValueError):
            CounterexampleResponse(action="Deny", reason="x")  # type: ignore[arg-type]

    def test_non_string_reason_rejected(self):
        with self.assertRaises(TypeError):
            CounterexampleResponse(
                action=Action.DENY, reason=123,  # type: ignore[arg-type]
            )

    def test_counterexamples_copied(self):
        src = [{"k": "v"}]
        r = CounterexampleResponse(
            action=Action.DENY, reason="x", counterexamples=src,
        )
        src[0]["k"] = "changed"
        self.assertEqual(r.counterexamples[0]["k"], "v")

    def test_total_count_less_than_kept_rejected(self):
        with self.assertRaises(ValueError):
            CounterexampleResponse(
                action=Action.DENY,
                reason="x",
                counterexamples=({"a": 1}, {"b": 2}),
                total_count=1,
            )


class TestSerialization(unittest.TestCase):
    def test_response_round_trip(self):
        h = CounterexampleHandler()
        r = h.handle([_cx(), _cx(property="b")])
        r2 = CounterexampleResponse.from_dict(r.to_dict())
        self.assertEqual(r.to_dict(), r2.to_dict())

    def test_response_to_dict_json_safe(self):
        r = CounterexampleResponse(
            action=Action.DENY,
            reason="x",
            counterexamples=(
                {
                    "ts": datetime(2024, 1, 1, tzinfo=timezone.utc),
                    "id": uuid.UUID(
                        "12345678-1234-5678-1234-567812345678"
                    ),
                    "amt": Decimal("1.5"),
                    "raw": b"\x00\x01",
                },
            ),
            total_count=1,
        )
        payload = json.dumps(r.to_dict())
        self.assertIn("2024-01-01", payload)
        self.assertIn("12345678", payload)

    def test_handler_round_trip(self):
        h = CounterexampleHandler(
            on_counterexamples=Action.ESCALATE,
            escalate_above=5,
            max_kept=10,
        )
        h2 = CounterexampleHandler.from_dict(h.to_dict())
        self.assertEqual(h, h2)

    def test_handler_from_dict_defaults(self):
        h = CounterexampleHandler.from_dict({})
        self.assertIs(h.on_counterexamples, Action.DENY)
        self.assertIsNone(h.escalate_above)
        self.assertEqual(h.max_kept, 32)

    def test_handler_from_dict_non_mapping_rejected(self):
        with self.assertRaises(TypeError):
            CounterexampleHandler.from_dict("nope")  # type: ignore[arg-type]

    def test_schemas(self):
        self.assertIn(
            "required", CounterexampleResponse(
                action=Action.ALLOW, reason="x",
            ).to_schema(),
        )
        self.assertIn(
            "properties", CounterexampleHandler().to_schema(),
        )


class TestIntrospection(unittest.TestCase):
    def test_describe(self):
        h = CounterexampleHandler(
            on_counterexamples=Action.ESCALATE,
            escalate_above=5,
            max_kept=10,
        )
        s = h.describe()
        self.assertIn("escalate", s)
        self.assertIn("5", s)
        self.assertIn("10", s)

    def test_str_matches_describe(self):
        h = CounterexampleHandler()
        self.assertEqual(str(h), h.describe())
        self.assertEqual(repr(h), h.describe())


if __name__ == "__main__":
    unittest.main(verbosity=2)
