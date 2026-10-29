"""
TemporalLogic — minimal STL-style property combinators.

Combines predicates over a ``DecisionRequest`` into named property
callables that can be registered with a :class:`PolicyVerifier`.

STL semantics
-------------
Each combinator is evaluated against a **sample window**. The window
is derived from ``request.context[history_key]`` (a list of prior
samples) plus the current request itself:

    samples = context[history_key] + [request]

* ``always(p, window=N)``: ``p`` holds for every sample in the window.
* ``eventually(p, window=N)``: ``p`` holds for at least one sample.
* ``until(phi, psi)``: standard strong until — ∃ j, ψ(sⱼ) ∧ ∀ k<j,
  φ(sₖ). Degenerates to ``ψ(request)`` when no history is present.
* ``implies(a, c)``: pointwise implication (does NOT iterate history).
  Use ``always(implies(a, c))`` for the temporal form.
* ``not_``, ``and_``, ``or_``: pointwise propositional operators.

When no history is present (``history_key`` not in ``context``), every
combinator degenerates to its pointwise form on the current request.
This preserves the behavior of the original ``always``/``implies``
for callers that do not supply history.

Async dispatch
--------------
Combinators **dispatch on their input predicates**: if any input is a
coroutine function, the returned callable is async (and awaits each
predicate); otherwise it is sync. This lets combinators compose with
the async :class:`PolicyVerifier` without a separate API.

Error handling
--------------
Predicate exceptions propagate unchanged. The verifier catches them
and records a ``crashed`` outcome with the exception type and message,
so a broken predicate is visible in the evidence rather than silently
satisfied. A predicate that returns ``None`` where a ``bool`` is
required raises ``TypeError`` for the same reason.

Name resolution
---------------
Every combinator accepts an optional ``name``. If omitted, the name
defaults to the combinator's own name (``"always"``, ``"eventually"``,
etc.). The returned callable's ``__name__`` is set to that value so
tracebacks and audit logs remain readable.
"""

from __future__ import annotations

import inspect
import unittest
from dataclasses import dataclass, field
from typing import (
    Any,
    Awaitable,
    Callable,
    List,
    Mapping,
    Optional,
    Tuple,
    Union,
)


__all__ = [
    "DEFAULT_HISTORY_KEY",
    "always",
    "eventually",
    "until",
    "implies",
    "not_",
    "and_",
    "or_",
    "TemporalLogic",
]


DEFAULT_HISTORY_KEY = "history"
Predicate = Callable[[Any], Union[bool, Awaitable[bool]]]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _validate_name(name: Optional[str], default: str) -> str:
    if name is None:
        return default
    if not isinstance(name, str) or not name.strip():
        raise ValueError("name must be a non-empty string")
    return name


def _is_async(pred: Predicate) -> bool:
    return inspect.iscoroutinefunction(pred)


def _bool(value: Any, pred: Predicate) -> bool:
    """Coerce a predicate result to ``bool``, rejecting ``None``.

    ``None`` almost always means the predicate forgot to ``return`` —
    coercing it to ``False`` would make a broken predicate behave as
    "not satisfied", which is *fail-closed but silent*. Raising lets
    the verifier record a ``crashed`` outcome instead, which is what
    an audit trail needs.
    """
    if value is None:
        name = getattr(pred, "__name__", "<predicate>")
        raise TypeError(
            f"predicate {name!r} returned None; a bool is required"
        )
    return bool(value)


def _history_samples(
    request: Any,
    history_key: str,
    window: Optional[int],
) -> List[Any]:
    """Return the samples a temporal combinator should evaluate."""
    ctx = getattr(request, "context", None) or {}
    if not isinstance(ctx, Mapping):
        ctx = {}
    raw = ctx.get(history_key)
    if not raw:
        history: List[Any] = []
    elif isinstance(raw, (list, tuple)):
        history = list(raw)
    else:
        # A single sample rather than a list — accept it.
        history = [raw]
    samples = history + [request]
    if window is not None:
        if window < 1:
            raise ValueError("window must be >= 1 or None")
        samples = samples[-window:]
    return samples


# ---------------------------------------------------------------------------
# always
# ---------------------------------------------------------------------------
def always(
    predicate: Predicate,
    *,
    window: Optional[int] = None,
    history_key: str = DEFAULT_HISTORY_KEY,
    name: Optional[str] = None,
) -> Predicate:
    """STL □: ``predicate`` holds at every sample in the window."""
    _name = _validate_name(name, "always")

    if _is_async(predicate):
        async def _check_async(request: Any) -> bool:
            for sample in _history_samples(request, history_key, window):
                result = await predicate(sample)  # type: ignore[misc]
                if not _bool(result, predicate):
                    return False
            return True
        _check_async.__name__ = _name
        return _check_async

    def _check_sync(request: Any) -> bool:
        for sample in _history_samples(request, history_key, window):
            if not _bool(predicate(sample), predicate):
                return False
        return True
    _check_sync.__name__ = _name
    return _check_sync


# ---------------------------------------------------------------------------
# eventually
# ---------------------------------------------------------------------------
def eventually(
    predicate: Predicate,
    *,
    window: Optional[int] = None,
    history_key: str = DEFAULT_HISTORY_KEY,
    name: Optional[str] = None,
) -> Predicate:
    """STL ◇: ``predicate`` holds at some sample in the window."""
    _name = _validate_name(name, "eventually")

    if _is_async(predicate):
        async def _check_async(request: Any) -> bool:
            for sample in _history_samples(request, history_key, window):
                result = await predicate(sample)  # type: ignore[misc]
                if _bool(result, predicate):
                    return True
            return False
        _check_async.__name__ = _name
        return _check_async

    def _check_sync(request: Any) -> bool:
        for sample in _history_samples(request, history_key, window):
            if _bool(predicate(sample), predicate):
                return True
        return False
    _check_sync.__name__ = _name
    return _check_sync


# ---------------------------------------------------------------------------
# until
# ---------------------------------------------------------------------------
def until(
    phi: Predicate,
    psi: Predicate,
    *,
    history_key: str = DEFAULT_HISTORY_KEY,
    name: Optional[str] = None,
) -> Predicate:
    """STL U: φ holds strictly until ψ becomes true.

    Standard strong until over the full history (no window). A trace
    satisfies ``phi U psi`` when there exists some index ``j`` such
    that ``psi(sⱼ)`` holds and ``phi(sₖ)`` holds for every ``k < j``.
    With no history, degenerates to ``psi(request)``.
    """
    _name = _validate_name(name, "until")
    both_async = _is_async(phi) or _is_async(psi)

    if both_async:
        async def _check_async(request: Any) -> bool:
            for sample in _history_samples(
                request, history_key, None,
            ):
                if _is_async(psi):
                    psi_value = await psi(sample)  # type: ignore[misc]
                else:
                    psi_value = psi(sample)
                if _bool(psi_value, psi):
                    return True
                if _is_async(phi):
                    phi_value = await phi(sample)  # type: ignore[misc]
                else:
                    phi_value = phi(sample)
                if not _bool(phi_value, phi):
                    return False
            return False
        _check_async.__name__ = _name
        return _check_async

    def _check_sync(request: Any) -> bool:
        for sample in _history_samples(request, history_key, None):
            if _bool(psi(sample), psi):
                return True
            if not _bool(phi(sample), phi):
                return False
        return False
    _check_sync.__name__ = _name
    return _check_sync


# ---------------------------------------------------------------------------
# implies
# ---------------------------------------------------------------------------
def implies(
    antecedent: Predicate,
    consequent: Predicate,
    *,
    name: Optional[str] = None,
) -> Predicate:
    """Pointwise implication: ``antecedent(x) → consequent(x)``.

    Evaluated on the current request only. Use
    ``always(implies(a, c))`` for the temporal form.
    """
    _name = _validate_name(name, "implies")
    any_async = _is_async(antecedent) or _is_async(consequent)

    if any_async:
        async def _check_async(request: Any) -> bool:
            if _is_async(antecedent):
                a_val = await antecedent(request)  # type: ignore[misc]
            else:
                a_val = antecedent(request)
            if not _bool(a_val, antecedent):
                return True
            if _is_async(consequent):
                c_val = await consequent(request)  # type: ignore[misc]
            else:
                c_val = consequent(request)
            return _bool(c_val, consequent)
        _check_async.__name__ = _name
        return _check_async

    def _check_sync(request: Any) -> bool:
        if not _bool(antecedent(request), antecedent):
            return True
        return _bool(consequent(request), consequent)
    _check_sync.__name__ = _name
    return _check_sync


# ---------------------------------------------------------------------------
# Propositional helpers
# ---------------------------------------------------------------------------
def not_(
    predicate: Predicate,
    *,
    name: Optional[str] = None,
) -> Predicate:
    """Pointwise negation."""
    _name = _validate_name(name, "not")

    if _is_async(predicate):
        async def _check_async(request: Any) -> bool:
            val = await predicate(request)  # type: ignore[misc]
            return not _bool(val, predicate)
        _check_async.__name__ = _name
        return _check_async

    def _check_sync(request: Any) -> bool:
        return not _bool(predicate(request), predicate)
    _check_sync.__name__ = _name
    return _check_sync


def and_(
    *predicates: Predicate,
    name: Optional[str] = None,
) -> Predicate:
    """Pointwise conjunction of every predicate."""
    if not predicates:
        raise ValueError("and_ requires at least one predicate")
    _name = _validate_name(name, "and")
    any_async = any(_is_async(p) for p in predicates)

    if any_async:
        async def _check_async(request: Any) -> bool:
            for p in predicates:
                if _is_async(p):
                    val = await p(request)  # type: ignore[misc]
                else:
                    val = p(request)
                if not _bool(val, p):
                    return False
            return True
        _check_async.__name__ = _name
        return _check_async

    def _check_sync(request: Any) -> bool:
        for p in predicates:
            if not _bool(p(request), p):
                return False
        return True
    _check_sync.__name__ = _name
    return _check_sync


def or_(
    *predicates: Predicate,
    name: Optional[str] = None,
) -> Predicate:
    """Pointwise disjunction of every predicate."""
    if not predicates:
        raise ValueError("or_ requires at least one predicate")
    _name = _validate_name(name, "or")
    any_async = any(_is_async(p) for p in predicates)

    if any_async:
        async def _check_async(request: Any) -> bool:
            for p in predicates:
                if _is_async(p):
                    val = await p(request)  # type: ignore[misc]
                else:
                    val = p(request)
                if _bool(val, p):
                    return True
            return False
        _check_async.__name__ = _name
        return _check_async

    def _check_sync(request: Any) -> bool:
        for p in predicates:
            if _bool(p(request), p):
                return True
        return False
    _check_sync.__name__ = _name
    return _check_sync


# ---------------------------------------------------------------------------
# Backward-compatible namespace
# ---------------------------------------------------------------------------
class TemporalLogic:
    """Backward-compatible namespace for the STL combinators."""

    always = staticmethod(always)
    eventually = staticmethod(eventually)
    until = staticmethod(until)
    implies = staticmethod(implies)
    not_ = staticmethod(not_)
    and_ = staticmethod(and_)
    or_ = staticmethod(or_)


# ===========================================================================
# TESTS
# ===========================================================================
import uuid as _uuid_test


@dataclass
class _Req:
    action: str = "run"
    request_id: str = field(default_factory=lambda: _uuid_test.uuid4().hex)
    context: dict = field(default_factory=dict)
    policy_version: Optional[str] = "v1"


def _req(context=None) -> _Req:
    return _Req(context=context or {})


def _with_history(*samples, key="history") -> _Req:
    """Build a request whose context carries ``samples`` as history."""
    return _req(context={key: list(samples)})


# -- predicate helpers ------------------------------------------------------
def _always_true(_): return True
def _always_false(_): return False
def _attr_true(name):
    def _check(sample):
        return bool(getattr(sample, name, False))
    _check.__name__ = f"attr_true({name})"
    return _check
def _attr_false(name):
    def _check(sample):
        return not bool(getattr(sample, name, False))
    _check.__name__ = f"attr_false({name})"
    return _check
def _attr_equals(name, expected):
    def _check(sample):
        return getattr(sample, name, None) == expected
    _check.__name__ = f"attr_equals({name},{expected})"
    return _check


# --- always ----------------------------------------------------------------
class TestAlways(unittest.TestCase):
    def test_no_history_is_pointwise(self):
        # Backward compatibility: without history, always(p) == p(request).
        self.assertTrue(always(_always_true)(_req()))
        self.assertFalse(always(_always_false)(_req()))

    def test_all_samples_pass(self):
        req = _with_history(
            _req(context={"ok": True}),
            _req(context={"ok": True}),
            _req(context={"ok": True}),
        )
        p = always(lambda s: s.context.get("ok") is True)
        self.assertTrue(p(req))

    def test_one_sample_fails(self):
        req = _with_history(
            _req(context={"ok": True}),
            _req(context={"ok": False}),
            _req(context={"ok": True}),
        )
        p = always(lambda s: s.context.get("ok") is True)
        self.assertFalse(p(req))

    def test_window_limits_samples(self):
        # window=2 → last history sample + current only.
        req = _with_history(
            _req(context={"ok": False}),  # out of window
            _req(context={"ok": True}),
            _req(context={"ok": True}),
        )
        self.assertFalse(always(
            lambda s: s.context.get("ok") is True,
        )(req))
        self.assertTrue(always(
            lambda s: s.context.get("ok") is True,
            window=2,
        )(req))

    def test_window_one_is_current_only(self):
        req = _with_history(_req(context={"ok": False}))
        self.assertTrue(always(
            lambda s: s.context.get("ok") is True,
            window=1,
        )(req))

    def test_custom_history_key(self):
        req = _with_history(
            _req(context={"ok": True}),
            key="trace",
        )
        p = always(
            lambda s: s.context.get("ok") is True,
            history_key="trace",
        )
        self.assertTrue(p(req))


class TestAlwaysAsync(unittest.TestCase):
    def test_async_predicate_dispatches_async(self):
        async def ok(_):
            return True
        p = always(ok)
        self.assertTrue(inspect.iscoroutinefunction(p))

    def test_async_all_pass(self):
        import asyncio

        async def ok(_):
            return True
        p = always(ok)
        req = _with_history(_req(), _req())
        self.assertTrue(asyncio.run(p(req)))

    def test_async_one_fails(self):
        import asyncio
        state = {"n": 0}

        async def sometimes(_):
            state["n"] += 1
            return state["n"] != 2
        p = always(sometimes)
        req = _with_history(_req(), _req())
        self.assertFalse(asyncio.run(p(req)))


# --- eventually ------------------------------------------------------------
class TestEventually(unittest.TestCase):
    def test_no_history_is_pointwise(self):
        self.assertTrue(eventually(_always_true)(_req()))
        self.assertFalse(eventually(_always_false)(_req()))

    def test_any_sample_passes(self):
        req = _with_history(
            _req(context={"ok": False}),
            _req(context={"ok": True}),
            _req(context={"ok": False}),
        )
        p = eventually(lambda s: s.context.get("ok") is True)
        self.assertTrue(p(req))

    def test_no_sample_passes(self):
        req = _with_history(
            _req(context={"ok": False}),
            _req(context={"ok": False}),
        )
        p = eventually(lambda s: s.context.get("ok") is True)
        self.assertFalse(p(req))

    def test_window_limits_samples(self):
        # The only True sample is out of window → False.
        req = _with_history(
            _req(context={"ok": True}),   # out of window
            _req(context={"ok": False}),
            _req(context={"ok": False}),
        )
        self.assertTrue(eventually(
            lambda s: s.context.get("ok") is True,
        )(req))
        self.assertFalse(eventually(
            lambda s: s.context.get("ok") is True,
            window=2,
        )(req))


class TestEventuallyAsync(unittest.TestCase):
    def test_async_dispatch(self):
        async def ok(_):
            return True
        self.assertTrue(inspect.iscoroutinefunction(eventually(ok)))

    def test_async_passes(self):
        import asyncio

        async def ok(_):
            return True
        req = _with_history(_req(), _req())
        self.assertTrue(asyncio.run(eventually(ok)(req)))


# --- until -----------------------------------------------------------------
class TestUntil(unittest.TestCase):
    def test_no_history_is_psi_pointwise(self):
        self.assertTrue(until(_always_true, _always_true)(_req()))
        self.assertFalse(until(_always_true, _always_false)(_req()))

    def test_psi_becomes_true_at_end(self):
        req = _with_history(
            _req(context={"psi": False}),
            _req(context={"psi": False}),
            _req(context={"psi": True}),
        )
        phi = lambda s: True  # noqa: E731
        psi = lambda s: s.context.get("psi") is True  # noqa: E731
        self.assertTrue(until(phi, psi)(req))

    def test_phi_violated_before_psi(self):
        req = _with_history(
            _req(context={"phi": True, "psi": False}),
            _req(context={"phi": False, "psi": False}),
            _req(context={"phi": True, "psi": True}),
        )
        phi = lambda s: s.context.get("phi") is True  # noqa: E731
        psi = lambda s: s.context.get("psi") is True  # noqa: E731
        self.assertFalse(until(phi, psi)(req))

    def test_psi_immediately_true(self):
        req = _with_history(_req(), _req())  # empty history samples
        req = _req(context={
            "history": [_req(), _req()],
            "psi": True,
        })
        phi = lambda s: False  # noqa: E731
        psi = lambda s: s.context.get("psi") is True  # noqa: E731
        # psi holds on the current request (last sample).
        self.assertTrue(until(phi, psi)(req))


class TestUntilAsync(unittest.TestCase):
    def test_async_dispatch(self):
        async def p(_):
            return True
        self.assertTrue(inspect.iscoroutinefunction(until(p, p)))

    def test_async_evaluation(self):
        import asyncio

        async def phi(_):
            return True

        async def psi(sample):
            return bool(sample.context.get("psi"))
        req = _with_history(
            _req(context={"psi": False}),
            _req(context={"psi": True}),
        )
        self.assertTrue(asyncio.run(until(phi, psi)(req)))


# --- implies ---------------------------------------------------------------
class TestImplies(unittest.TestCase):
    def test_antecedent_false_passes(self):
        p = implies(_always_false, _always_false)
        self.assertTrue(p(_req()))

    def test_antecedent_true_consequent_true(self):
        p = implies(_always_true, _always_true)
        self.assertTrue(p(_req()))

    def test_antecedent_true_consequent_false(self):
        p = implies(_always_true, _always_false)
        self.assertFalse(p(_req()))

    def test_none_antecedent_raises(self):
        def bad(_):
            return None
        p = implies(bad, _always_true)
        with self.assertRaises(TypeError):
            p(_req())


class TestImpliesAsync(unittest.TestCase):
    def test_async_dispatch(self):
        async def p(_):
            return True
        self.assertTrue(inspect.iscoroutinefunction(implies(p, p)))

    def test_async_true(self):
        import asyncio

        async def t(_):
            return True
        self.assertTrue(asyncio.run(implies(t, t)(_req())))


# --- propositional helpers -------------------------------------------------
class TestPropositionalHelpers(unittest.TestCase):
    def test_not(self):
        self.assertFalse(not_(_always_true)(_req()))
        self.assertTrue(not_(_always_false)(_req()))

    def test_and(self):
        self.assertTrue(and_(_always_true, _always_true)(_req()))
        self.assertFalse(and_(_always_true, _always_false)(_req()))

    def test_and_requires_args(self):
        with self.assertRaises(ValueError):
            and_()

    def test_or(self):
        self.assertTrue(or_(_always_true, _always_false)(_req()))
        self.assertFalse(or_(_always_false, _always_false)(_req()))

    def test_or_requires_args(self):
        with self.assertRaises(ValueError):
            or_()

    def test_not_async(self):
        import asyncio

        async def t(_):
            return True
        self.assertFalse(asyncio.run(not_(t)(_req())))


# --- composition -----------------------------------------------------------
class TestComposition(unittest.TestCase):
    def test_always_eventually(self):
        # "Eventually True, at every sample" ≡ there is a True sample.
        # A single True sample → both always(eventually(...)) and
        # eventually(...) are True when the trace has a True sample.
        # Actually always(eventually(p)) is vacuously true whenever p
        # holds on some sample, which is exactly what eventually(p)
        # already gives us in a finite trace. The composition is
        # algebraically correct but degenerate — test it anyway.
        req = _with_history(
            _req(context={"ok": True}),
            _req(context={"ok": False}),
        )
        p = always(eventually(
            lambda s: s.context.get("ok") is True,
        ))
        self.assertTrue(p(req))

    def test_always_implies(self):
        # always(ok → logged): every sample where ok is true must be
        # logged. Two samples violate.
        req = _with_history(
            _req(context={"ok": True,  "logged": True}),
            _req(context={"ok": True,  "logged": False}),
            _req(context={"ok": False, "logged": False}),
        )
        p = always(implies(
            lambda s: s.context.get("ok") is True,
            lambda s: s.context.get("logged") is True,
        ))
        self.assertFalse(p(req))


# --- name handling ---------------------------------------------------------
class TestNames(unittest.TestCase):
    def test_default_name(self):
        self.assertEqual(always(_always_true).__name__, "always")
        self.assertEqual(eventually(_always_true).__name__, "eventually")
        self.assertEqual(until(_always_true, _always_true).__name__, "until")
        self.assertEqual(implies(_always_true, _always_true).__name__,
                         "implies")
        self.assertEqual(not_(_always_true).__name__, "not")
        self.assertEqual(and_(_always_true).__name__, "and")
        self.assertEqual(or_(_always_true).__name__, "or")

    def test_custom_name(self):
        self.assertEqual(
            always(_always_true, name="my_prop").__name__, "my_prop",
        )

    def test_empty_name_rejected(self):
        with self.assertRaises(ValueError):
            always(_always_true, name="")
        with self.assertRaises(ValueError):
            always(_always_true, name="   ")

    def test_non_string_name_rejected(self):
        with self.assertRaises(ValueError):
            always(_always_true, name=123)  # type: ignore[arg-type]


# --- namespace compat ------------------------------------------------------
class TestTemporalLogicNamespace(unittest.TestCase):
    def test_always_via_namespace(self):
        self.assertTrue(TemporalLogic.always(_always_true)(_req()))

    def test_eventually_via_namespace(self):
        self.assertTrue(TemporalLogic.eventually(_always_true)(_req()))

    def test_implies_via_namespace(self):
        self.assertTrue(
            TemporalLogic.implies(_always_false, _always_false)(_req())
        )

    def test_namespace_instance_call(self):
        # An instance call works because the methods are static.
        ns = TemporalLogic()
        self.assertTrue(ns.always(_always_true)(_req()))


# --- integration with verifier --------------------------------------------
class TestVerifierIntegration(unittest.IsolatedAsyncioTestCase):
    async def test_sync_combinator_with_verifier(self):
        # A minimal stand-in for PolicyVerifier.
        class FakeVerifier:
            def __init__(self):
                self.properties = {}
            def register(self, name, pred):
                self.properties[name] = pred
            async def run(self, req):
                results = {}
                for n, p in self.properties.items():
                    results[n] = p(req)
                return results

        req = _with_history(_req(context={"ok": True}))
        v = FakeVerifier()
        v.register("ok_always", always(
            lambda s: s.context.get("ok") is True,
        ))
        results = await v.run(req)
        self.assertTrue(results["ok_always"])

    async def test_async_combinator_with_verifier(self):
        async def ok(_):
            return True

        async def run_all(preds, req):
            return [await p(req) for p in preds]

        p = always(ok)
        self.assertTrue(await p(_req()))


if __name__ == "__main__":
    unittest.main(verbosity=2)
