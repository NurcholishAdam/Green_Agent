"""
PolicyVerifier — formal verification of safety-critical policies.

Verifies a :class:`DecisionRequest` against a set of registered safety
properties. Each property is a named callable that returns ``True``
(satisfied) or ``False`` (violated). Async predicates are supported.

Key guarantees
--------------
* **No silent passing.** Sync predicates are called inline; async
  predicates (``async def``) are detected via ``inspect.isawaitable``
  and awaited with ``asyncio.wait_for(timeout)``. An async predicate
  is never treated as "truthy because it returned a coroutine".
* **Fail-closed.** A predicate that raises, times out, or returns
  a falsey value is recorded as a failure. Every failure contributes
  a structured counterexample with ``outcome`` and, where available,
  ``error_type`` / ``error_message``.
* **Typed outcomes.** ``VerificationVerdict`` is a ``str``-enum; the
  wire form (``"passed"`` / ``"failed"``) matches what downstream
  consumers like the PDP already expect.
* **Thread-safe.** Registration and verification are serialized on an
  internal ``RLock``; ``verify`` iterates a stable snapshot.
* **Validated.** Registration rejects empty names and non-callables;
  ``verify`` rejects non-``DecisionRequest`` inputs.
* **Deterministic empty case.** A verifier with no properties returns
  ``verdict="passed"`` with ``properties_checked=()`` and
  ``coverage_pct=None`` — the former preserves backward compatibility;
  the latter signals "no coverage was measured", not "100% coverage".

Contract on predicates
----------------------
* **Sync predicates** must be fast (goal: < 100 ms). They are called
  inline and **are not subject to ``timeout``**; a blocking sync
  predicate blocks the verifier. If a property needs I/O, implement
  it as an ``async def`` predicate.
* **Async predicates** are awaited with ``asyncio.wait_for`` at the
  configured ``timeout`` (default 5 s). A timeout is recorded as a
  failure with ``outcome="timeout"``.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import threading
import unittest
from dataclasses import dataclass, field
from enum import Enum
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    FrozenSet,
    Iterator,
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
    from .verification_evidence import VerificationEvidence  # type: ignore
except Exception:  # pragma: no cover — standalone / test mode
    import uuid as _uuid
    from datetime import datetime as _dt, timezone as _tz

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

    @dataclass
    class VerificationEvidence:  # type: ignore[no-redef]
        policy_version: str
        verdict: str
        properties_checked: List[str] = field(default_factory=list)
        properties_passed: List[str] = field(default_factory=list)
        properties_failed: List[str] = field(default_factory=list)
        counterexamples: List[Dict[str, Any]] = field(default_factory=list)
        coverage_pct: Optional[float] = 1.0


logger = logging.getLogger(__name__)

__all__ = [
    "VerificationVerdict",
    "CounterexampleOutcome",
    "PolicyVerifier",
    "DEFAULT_VERIFIER",
    "DEFAULT_VERIFY_TIMEOUT",
]


DEFAULT_VERIFY_TIMEOUT = 5.0
PropertyPredicate = Callable[
    [DecisionRequest], Union[bool, Awaitable[bool]]
]


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------
class VerificationVerdict(str, Enum):
    """Aggregate outcome of a verification run."""

    PASSED = "passed"
    FAILED = "failed"


class CounterexampleOutcome(str, Enum):
    """Why one property contributed a counterexample."""

    VIOLATED = "violated"
    CRASHED = "crashed"
    TIMEOUT = "timeout"


# ---------------------------------------------------------------------------
# PolicyVerifier
# ---------------------------------------------------------------------------
class PolicyVerifier:
    """
    Verifies a request against a set of registered safety properties.

    A property is a named callable; sync predicates are called inline
    and must be fast, async predicates are awaited with a timeout.
    A property that returns a falsey value, raises, or times out is
    recorded as a failure with a structured counterexample.
    """

    def __init__(
        self,
        *,
        timeout: float = DEFAULT_VERIFY_TIMEOUT,
        default_policy_version: str = "unknown",
        strict_policy_version: bool = False,
    ) -> None:
        if not isinstance(timeout, (int, float)) or isinstance(timeout, bool):
            raise TypeError("timeout must be a number")
        if not asyncio.iscoroutinefunction(lambda: None) and timeout <= 0:
            raise ValueError("timeout must be positive")
        if timeout <= 0:
            raise ValueError("timeout must be positive")

        if (
            not isinstance(default_policy_version, str)
            or not default_policy_version.strip()
        ):
            raise ValueError(
                "default_policy_version must be a non-empty string"
            )
        if not isinstance(strict_policy_version, bool):
            raise TypeError("strict_policy_version must be a bool")

        self._properties: Dict[str, PropertyPredicate] = {}
        self._lock = threading.RLock()
        self._timeout = float(timeout)
        self._default_policy_version = default_policy_version
        self._strict_policy_version = strict_policy_version

    # -- registration ------------------------------------------------------
    def register(
        self,
        name: str,
        predicate: PropertyPredicate,
        *,
        overwrite: bool = False,
    ) -> None:
        """Register a safety property under ``name``.

        Raises ``ValueError`` for an empty name, ``TypeError`` for a
        non-callable predicate, and ``KeyError`` if ``name`` already
        exists and ``overwrite=False``.
        """
        if not isinstance(name, str) or not name.strip():
            raise ValueError("property name must be a non-empty string")
        if not callable(predicate):
            raise TypeError("predicate must be callable")
        if not isinstance(overwrite, bool):
            raise TypeError("overwrite must be a bool")

        with self._lock:
            if name in self._properties and not overwrite:
                raise KeyError(
                    f"property {name!r} already registered "
                    f"(pass overwrite=True to replace)"
                )
            self._properties[name] = predicate

    def unregister(self, name: str) -> bool:
        """Remove a property. Returns ``True`` if it existed."""
        with self._lock:
            return self._properties.pop(name, None) is not None

    def clear(self) -> None:
        with self._lock:
            self._properties.clear()

    def __contains__(self, name: object) -> bool:
        return name in self._properties

    def __len__(self) -> int:
        return len(self._properties)

    def __iter__(self) -> Iterator[str]:
        with self._lock:
            return iter(list(self._properties))

    def __repr__(self) -> str:
        names = ", ".join(sorted(self._properties))
        return f"PolicyVerifier([{names}], timeout={self._timeout}s)"

    # -- introspection -----------------------------------------------------
    @property
    def properties(self) -> FrozenSet[str]:
        return frozenset(self._properties)

    def describe(self) -> str:
        names = ", ".join(sorted(self._properties)) or "<none>"
        return (
            f"PolicyVerifier(properties=[{names}], "
            f"timeout={self._timeout}s, "
            f"strict_policy_version={self._strict_policy_version})"
        )

    def to_dict(self) -> Dict[str, Any]:
        # Predicates are callables and cannot be serialized; only
        # names and configuration are exposed.
        return {
            "properties": sorted(self._properties),
            "timeout": self._timeout,
            "default_policy_version": self._default_policy_version,
            "strict_policy_version": self._strict_policy_version,
        }

    def to_schema(self) -> Dict[str, Any]:
        return {
            "type": "object",
            "required": ["properties", "timeout"],
            "properties": {
                "properties": {
                    "type": "array",
                    "items": {"type": "string", "minLength": 1},
                },
                "timeout": {"type": "number", "exclusiveMinimum": 0},
                "default_policy_version": {
                    "type": "string", "minLength": 1,
                },
                "strict_policy_version": {"type": "boolean"},
            },
            "additionalProperties": False,
        }

    # -- verification ------------------------------------------------------
    async def verify(
        self, request: DecisionRequest,
    ) -> VerificationEvidence:
        """Run every registered property against ``request``."""
        if not hasattr(request, "policy_version"):
            raise TypeError(
                f"request must be a DecisionRequest-like object, got "
                f"{type(request).__name__}"
            )

        # -- Resolve policy_version -----------------------------------
        if self._strict_policy_version and not request.policy_version:
            raise ValueError(
                "request.policy_version is required "
                "(strict_policy_version=True)"
            )
        policy_version = (
            request.policy_version or self._default_policy_version
        )

        # -- Snapshot for thread safety -------------------------------
        with self._lock:
            snapshot = dict(self._properties)

        checked: List[str] = []
        passed: List[str] = []
        failed: List[str] = []
        counterexamples: List[Dict[str, Any]] = []

        for name, predicate in snapshot.items():
            checked.append(name)

            # -- Invoke predicate (sync or async) ----------------------
            try:
                result = predicate(request)
                if inspect.isawaitable(result):
                    result = await asyncio.wait_for(
                        result, timeout=self._timeout,
                    )
            except asyncio.TimeoutError:
                logger.warning(
                    "property %r timed out after %ss",
                    name, self._timeout,
                )
                failed.append(name)
                counterexamples.append({
                    "property": name,
                    "outcome": CounterexampleOutcome.TIMEOUT.value,
                    "error_type": "TimeoutError",
                    "error_message": (
                        f"timed out after {self._timeout}s"
                    ),
                })
                continue
            except Exception as exc:  # noqa: BLE001 — deliberate isolation
                logger.warning(
                    "property %r raised", name, exc_info=True,
                )
                failed.append(name)
                counterexamples.append({
                    "property": name,
                    "outcome": CounterexampleOutcome.CRASHED.value,
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                })
                continue

            # -- Evaluate result --------------------------------------
            if result:
                passed.append(name)
            else:
                failed.append(name)
                counterexamples.append({
                    "property": name,
                    "outcome": CounterexampleOutcome.VIOLATED.value,
                    "error_type": None,
                    "error_message": None,
                })

        # -- Aggregate ------------------------------------------------
        verdict = (
            VerificationVerdict.PASSED if not failed
            else VerificationVerdict.FAILED
        )

        # ``coverage_pct=None`` for "no properties checked" — distinct
        # from "100% of properties checked passed".
        coverage_pct: Optional[float] = (
            None if not checked
            else len(passed) / len(checked)
        )

        return VerificationEvidence(
            policy_version=policy_version,
            verdict=verdict.value,
            properties_checked=checked,
            properties_passed=passed,
            properties_failed=failed,
            counterexamples=counterexamples,
            coverage_pct=coverage_pct,
        )


# ---------------------------------------------------------------------------
# Convenience singleton (empty registry — equivalent to no verification)
# ---------------------------------------------------------------------------
DEFAULT_VERIFIER: PolicyVerifier = PolicyVerifier()


# ===========================================================================
# TESTS
# ===========================================================================
import uuid as _uuid_test


def _request(**kwargs: Any) -> DecisionRequest:
    defaults: Dict[str, Any] = {
        "action": "run",
        "request_id": _uuid_test.uuid4().hex,
        "policy_version": "v1",
    }
    defaults.update(kwargs)
    return DecisionRequest(**defaults)


def _sync_pass(req: DecisionRequest) -> bool:
    return True


def _sync_fail(req: DecisionRequest) -> bool:
    return False


def _sync_raise(req: DecisionRequest) -> bool:
    raise ValueError("boom")


async def _async_pass(req: DecisionRequest) -> bool:
    return True


async def _async_fail(req: DecisionRequest) -> bool:
    return False


async def _async_raise(req: DecisionRequest) -> bool:
    raise KeyError("async boom")


async def _async_slow(req: DecisionRequest) -> bool:
    await asyncio.sleep(10.0)
    return True


class TestRegistration(unittest.TestCase):
    def test_register(self):
        v = PolicyVerifier()
        v.register("p1", _sync_pass)
        self.assertIn("p1", v)
        self.assertEqual(len(v), 1)

    def test_empty_name_rejected(self):
        v = PolicyVerifier()
        with self.assertRaises(ValueError):
            v.register("", _sync_pass)
        with self.assertRaises(ValueError):
            v.register("   ", _sync_pass)

    def test_non_callable_rejected(self):
        v = PolicyVerifier()
        with self.assertRaises(TypeError):
            v.register("p1", None)  # type: ignore[arg-type]
        with self.assertRaises(TypeError):
            v.register("p1", 42)  # type: ignore[arg-type]

    def test_duplicate_raises(self):
        v = PolicyVerifier()
        v.register("p1", _sync_pass)
        with self.assertRaises(KeyError):
            v.register("p1", _sync_fail)

    def test_overwrite(self):
        v = PolicyVerifier()
        v.register("p1", _sync_fail)
        v.register("p1", _sync_pass, overwrite=True)
        self.assertEqual(len(v), 1)

    def test_unregister(self):
        v = PolicyVerifier()
        v.register("p1", _sync_pass)
        self.assertTrue(v.unregister("p1"))
        self.assertFalse(v.unregister("p1"))

    def test_clear(self):
        v = PolicyVerifier()
        v.register("p1", _sync_pass)
        v.register("p2", _sync_pass)
        v.clear()
        self.assertEqual(len(v), 0)

    def test_iter_repr_describe(self):
        v = PolicyVerifier()
        v.register("p1", _sync_pass)
        v.register("p2", _sync_pass)
        self.assertEqual(set(v), {"p1", "p2"})
        self.assertIn("p1", repr(v))
        self.assertIn("p1", v.describe())


class TestVerifierConstruction(unittest.TestCase):
    def test_defaults(self):
        v = PolicyVerifier()
        self.assertEqual(v._timeout, DEFAULT_VERIFY_TIMEOUT)
        self.assertEqual(v._default_policy_version, "unknown")
        self.assertFalse(v._strict_policy_version)

    def test_negative_timeout_rejected(self):
        with self.assertRaises(ValueError):
            PolicyVerifier(timeout=0)
        with self.assertRaises(ValueError):
            PolicyVerifier(timeout=-1.0)

    def test_non_number_timeout_rejected(self):
        with self.assertRaises(TypeError):
            PolicyVerifier(timeout="5")  # type: ignore[arg-type]

    def test_bad_default_policy_version_rejected(self):
        with self.assertRaises(ValueError):
            PolicyVerifier(default_policy_version="")

    def test_bad_strict_flag_rejected(self):
        with self.assertRaises(TypeError):
            PolicyVerifier(strict_policy_version="yes")  # type: ignore[arg-type]

    def test_default_singleton(self):
        self.assertIsInstance(DEFAULT_VERIFIER, PolicyVerifier)


class TestSyncPredicates(unittest.IsolatedAsyncioTestCase):
    async def test_all_pass(self):
        v = PolicyVerifier()
        v.register("a", _sync_pass)
        v.register("b", _sync_pass)
        ev = await v.verify(_request())
        self.assertEqual(ev.verdict, "passed")
        self.assertEqual(set(ev.properties_checked), {"a", "b"})
        self.assertEqual(set(ev.properties_passed), {"a", "b"})
        self.assertEqual(ev.properties_failed, [])
        self.assertEqual(ev.counterexamples, [])
        self.assertEqual(ev.coverage_pct, 1.0)

    async def test_one_fails(self):
        v = PolicyVerifier()
        v.register("a", _sync_pass)
        v.register("b", _sync_fail)
        ev = await v.verify(_request())
        self.assertEqual(ev.verdict, "failed")
        self.assertEqual(ev.properties_failed, ["b"])
        self.assertEqual(ev.coverage_pct, 0.5)
        self.assertEqual(len(ev.counterexamples), 1)
        self.assertEqual(
            ev.counterexamples[0]["outcome"], "violated",
        )
        self.assertEqual(
            ev.counterexamples[0]["property"], "b",
        )

    async def test_predicate_raises(self):
        v = PolicyVerifier()
        v.register("a", _sync_raise)
        ev = await v.verify(_request())
        self.assertEqual(ev.verdict, "failed")
        cx = ev.counterexamples[0]
        self.assertEqual(cx["outcome"], "crashed")
        self.assertEqual(cx["error_type"], "ValueError")
        self.assertIn("boom", cx["error_message"])

    async def test_empty_registry(self):
        v = PolicyVerifier()
        ev = await v.verify(_request())
        self.assertEqual(ev.verdict, "passed")
        self.assertEqual(ev.properties_checked, [])
        self.assertIsNone(ev.coverage_pct)


class TestAsyncPredicates(unittest.IsolatedAsyncioTestCase):
    async def test_async_pass_is_awaited(self):
        # Regression: original never awaited async predicates —
        # the coroutine object was truthy, so this silently passed
        # regardless of what the predicate returned. With the fix, the
        # predicate is awaited and evaluated correctly.
        v = PolicyVerifier()
        v.register("a", _async_pass)
        ev = await v.verify(_request())
        self.assertEqual(ev.verdict, "passed")
        self.assertEqual(ev.properties_passed, ["a"])

    async def test_async_fail_is_awaited(self):
        # This is the important regression: a failing async predicate
        # must produce a failure, not a silent pass.
        v = PolicyVerifier()
        v.register("a", _async_fail)
        ev = await v.verify(_request())
        self.assertEqual(ev.verdict, "failed")
        self.assertEqual(ev.properties_failed, ["a"])
        self.assertEqual(
            ev.counterexamples[0]["outcome"], "violated",
        )

    async def test_async_raise_recorded(self):
        v = PolicyVerifier()
        v.register("a", _async_raise)
        ev = await v.verify(_request())
        self.assertEqual(ev.verdict, "failed")
        cx = ev.counterexamples[0]
        self.assertEqual(cx["outcome"], "crashed")
        self.assertEqual(cx["error_type"], "KeyError")

    async def test_async_timeout(self):
        v = PolicyVerifier(timeout=0.05)
        v.register("a", _async_slow)
        ev = await v.verify(_request())
        self.assertEqual(ev.verdict, "failed")
        cx = ev.counterexamples[0]
        self.assertEqual(cx["outcome"], "timeout")
        self.assertEqual(cx["error_type"], "TimeoutError")


class TestPolicyVersion(unittest.IsolatedAsyncioTestCase):
    async def test_uses_request_version(self):
        v = PolicyVerifier()
        ev = await v.verify(_request(policy_version="v42"))
        self.assertEqual(ev.policy_version, "v42")

    async def test_falls_back_to_default(self):
        v = PolicyVerifier(default_policy_version="fallback")
        ev = await v.verify(_request(policy_version=None))
        self.assertEqual(ev.policy_version, "fallback")

    async def test_default_unknown(self):
        v = PolicyVerifier()
        ev = await v.verify(_request(policy_version=None))
        self.assertEqual(ev.policy_version, "unknown")

    async def test_strict_mode_raises(self):
        v = PolicyVerifier(strict_policy_version=True)
        with self.assertRaises(ValueError):
            await v.verify(_request(policy_version=None))


class TestInputValidation(unittest.IsolatedAsyncioTestCase):
    async def test_none_request(self):
        v = PolicyVerifier()
        with self.assertRaises(TypeError):
            await v.verify(None)  # type: ignore[arg-type]

    async def test_wrong_type(self):
        v = PolicyVerifier()
        with self.assertRaises(TypeError):
            await v.verify("not a request")  # type: ignore[arg-type]


class TestSnapshotUnderConcurrency(unittest.TestCase):
    def test_concurrent_register_and_iter(self):
        import threading

        v = PolicyVerifier()
        stop = threading.Event()

        def registrar(i: int) -> None:
            for n in range(30):
                try:
                    v.register(f"p-{i}-{n}", _sync_pass)
                except KeyError:
                    pass
            stop.set()

        threads = [
            threading.Thread(target=registrar, args=(i,))
            for i in range(2)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        # Snapshot check: 60 unique properties registered.
        self.assertEqual(len(v), 60)


class TestSerialization(unittest.TestCase):
    def test_to_dict_shape(self):
        v = PolicyVerifier(timeout=2.0)
        v.register("a", _sync_pass)
        d = v.to_dict()
        self.assertEqual(d["properties"], ["a"])
        self.assertEqual(d["timeout"], 2.0)
        self.assertEqual(
            d["default_policy_version"], "unknown",
        )
        self.assertFalse(d["strict_policy_version"])

    def test_to_dict_json_safe(self):
        import json
        v = PolicyVerifier()
        v.register("a", _sync_pass)
        json.dumps(v.to_dict())  # must not raise

    def test_to_schema_shape(self):
        s = PolicyVerifier().to_schema()
        self.assertEqual(s["type"], "object")
        self.assertIn("properties", s["required"])
        self.assertIn("timeout", s["required"])
        self.assertFalse(s["additionalProperties"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
