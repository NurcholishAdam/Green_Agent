"""
FallbackPolicy — safe fallback behavior when actions are not permitted.

A rule-based selector: given a :class:`DecisionRequest`, choose the
first matching :class:`Rule` and return a :class:`FallbackAction`
describing the action to take instead of the originally requested one.

Key guarantees
--------------
* **Immutable results**: :class:`FallbackAction` is a frozen dataclass;
  once the policy has chosen, downstream code cannot rewrite it.
* **Immutable rules**: :class:`Rule` is frozen; the policy's rule list
  is snapshot-copied on every ``select``.
* **Thread-safe**: mutations and selections are serialized by a
  reentrant lock; iteration reads a stable snapshot.
* **Sync + async predicates**: predicates may be plain callables or
  coroutine functions; the loop awaits awaitables and treats plain
  bools as results, uniformly.
* **Isolated failures**: a predicate that raises does not abort the
  selection; the failure is logged with a full traceback and the next
  rule is tried. ``BaseException`` (``CancelledError``,
  ``KeyboardInterrupt``, ``SystemExit``) propagates unchanged.
* **Deny by default**: when no rule matches and no custom default is
  configured, the policy returns ``FallbackAction("deny", ...,
  rule_name=None)``. Callers can distinguish "no rule matched" from
  "a rule deliberately chose ``deny``" via :meth:`FallbackAction.is_default`.
* **Non-destructive modification**: the ``modified_request`` returned
  on a match is a **copy** of the request with
  ``context["fallback_action"]`` set — the caller's ``request`` is
  never mutated.

Note on imports
---------------
The file imports :class:`DecisionRequest` from
``..contracts.decision_request`` when running inside the package. When
imported standalone (e.g. for tests), a minimal stub is used so the
module has no hard packaging dependency.
"""

from __future__ import annotations

import inspect
import logging
import threading
import unittest
from dataclasses import FrozenInstanceError, dataclass, field
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Union,
)

try:
    from ..contracts.decision_request import DecisionRequest  # type: ignore
except ImportError:  # pragma: no cover — standalone / test mode

    @dataclass(frozen=True)
    class DecisionRequest:  # type: ignore[no-redef]
        """Minimal stand-in matching the shape FallbackPolicy needs."""

        request_id: str
        action: str
        context: Dict[str, Any] = field(default_factory=dict)

        def to_dict(self) -> Dict[str, Any]:
            return {
                "request_id": self.request_id,
                "action": self.action,
                "context": dict(self.context),
            }


logger = logging.getLogger(__name__)

__all__ = ["FallbackAction", "FallbackPolicy", "Rule"]


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------
Predicate = Callable[[Any], Union[bool, Awaitable[bool]]]


# ---------------------------------------------------------------------------
# FallbackAction
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class FallbackAction:
    """A specific fallback action chosen by the policy.

    ``rule_name`` is ``None`` when no rule matched and the policy
    returned its default. Use :meth:`is_default` to check.
    """

    action: str
    reason: str = ""
    modified_request: Optional[Dict[str, Any]] = None
    rule_name: Optional[str] = None

    def __post_init__(self) -> None:
        if not isinstance(self.action, str) or not self.action.strip():
            raise ValueError("action must be a non-empty string")
        if not isinstance(self.reason, str):
            raise TypeError("reason must be a string")
        if self.rule_name is not None:
            if not isinstance(self.rule_name, str) or not self.rule_name.strip():
                raise ValueError(
                    "rule_name must be a non-empty string or None"
                )
        if self.modified_request is not None:
            if not isinstance(self.modified_request, dict):
                raise TypeError(
                    "modified_request must be a dict or None"
                )
            object.__setattr__(
                self, "modified_request", dict(self.modified_request)
            )

    def is_default(self) -> bool:
        """True when this action came from the policy's default."""
        return self.rule_name is None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "action": self.action,
            "reason": self.reason,
            "modified_request": self.modified_request,
            "rule_name": self.rule_name,
        }


# ---------------------------------------------------------------------------
# Rule
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class Rule:
    """One fallback rule: a predicate, the action it yields, and metadata."""

    name: str
    matches: Predicate
    action: str
    reason: str = ""
    priority: int = 0

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name.strip():
            raise ValueError("Rule.name must be a non-empty string")
        if not callable(self.matches):
            raise TypeError("Rule.matches must be callable")
        if not isinstance(self.action, str) or not self.action.strip():
            raise ValueError("Rule.action must be a non-empty string")
        if not isinstance(self.reason, str):
            raise TypeError("Rule.reason must be a string")
        if not isinstance(self.priority, int):
            raise TypeError("Rule.priority must be an int")


# ---------------------------------------------------------------------------
# FallbackPolicy
# ---------------------------------------------------------------------------
_DEFAULT_ACTION = FallbackAction(
    action="deny", reason="no fallback rule matched"
)


class FallbackPolicy:
    """
    Chooses a fallback action for a request.

    The policy is a list of :class:`Rule` objects. On each
    :meth:`select` the rules are sorted by ``priority`` descending
    (stable with respect to insertion order for equal priorities) and
    the first rule whose ``matches`` predicate returns truthy wins.
    """

    def __init__(
        self,
        rules: Optional[List[Rule]] = None,
        *,
        default: FallbackAction = _DEFAULT_ACTION,
    ) -> None:
        if not isinstance(default, FallbackAction):
            raise TypeError("default must be a FallbackAction")
        self._rules: List[Rule] = list(rules or [])
        for r in self._rules:
            if not isinstance(r, Rule):
                raise TypeError(
                    f"rules entries must be Rule instances, "
                    f"got {type(r).__name__}"
                )
        self._default = default
        self._lock = threading.RLock()
        self._counter = len(self._rules)

    # -- rule management ---------------------------------------------------
    def add_rule(
        self,
        matches: Predicate,
        action: str,
        reason: str = "",
        *,
        name: Optional[str] = None,
        priority: int = 0,
    ) -> Rule:
        """Append a rule and return it (for chaining / inspection)."""
        if not callable(matches):
            raise TypeError("matches must be callable")
        if not isinstance(action, str) or not action.strip():
            raise ValueError("action must be a non-empty string")
        if not isinstance(reason, str):
            raise TypeError("reason must be a string")
        if not isinstance(priority, int):
            raise TypeError("priority must be an int")
        if name is None:
            name = f"rule-{self._counter}"
            self._counter += 1
        rule = Rule(
            name=name,
            matches=matches,
            action=action,
            reason=reason,
            priority=priority,
        )
        with self._lock:
            self._rules.append(rule)
        return rule

    def remove_rule(self, name: str) -> bool:
        """Remove the rule named ``name``. Returns True if it existed."""
        with self._lock:
            for i, r in enumerate(self._rules):
                if r.name == name:
                    del self._rules[i]
                    return True
        return False

    def clear(self) -> None:
        with self._lock:
            self._rules.clear()

    def __len__(self) -> int:
        return len(self._rules)

    def __iter__(self) -> Iterator[Rule]:
        with self._lock:
            return iter(list(self._rules))

    def __repr__(self) -> str:
        names = ", ".join(r.name for r in self._rules)
        return f"FallbackPolicy([{names}], default={self._default.action!r})"

    # -- selection ---------------------------------------------------------
    def _snapshot(self) -> List[Rule]:
        with self._lock:
            rules = list(self._rules)
        # Higher priority first, insertion order for ties (sorted is stable).
        return sorted(rules, key=lambda r: -r.priority)

    async def select(self, request: Any) -> FallbackAction:
        """Return the fallback action for ``request``.

        Rules are tried in priority-descending order; the first whose
        predicate returns truthy wins. Predicates that raise are logged
        and skipped. When no rule matches, the policy's default is
        returned (``deny`` unless configured otherwise).
        """
        for rule in self._snapshot():
            try:
                result = rule.matches(request)
                if inspect.isawaitable(result):
                    result = await result
                if result:
                    return FallbackAction(
                        action=rule.action,
                        reason=rule.reason,
                        modified_request=_modify(request, rule.action),
                        rule_name=rule.name,
                    )
            except Exception:  # noqa: BLE001 — deliberate isolation
                logger.warning(
                    "fallback rule %r raised", rule.name, exc_info=True
                )
        logger.debug("no fallback rule matched for request")
        return self._default


# ---------------------------------------------------------------------------
# Request modification
# ---------------------------------------------------------------------------
def _modify(request: Any, action: str) -> Dict[str, Any]:
    """Return a *copy* of ``request`` as a dict, with
    ``context["fallback_action"] = action`` set.

    Never mutates the caller's ``request`` nor its ``to_dict()`` output.
    Tolerates a missing or non-dict ``context``.
    """
    if hasattr(request, "to_dict") and callable(request.to_dict):
        req = dict(request.to_dict())
    elif hasattr(request, "__dict__"):
        req = dict(vars(request))
    else:
        req = {"action": getattr(request, "action", None)}

    raw = req.get("context")
    context: Dict[str, Any] = dict(raw) if isinstance(raw, dict) else {}
    context["fallback_action"] = action
    req["context"] = context
    return req


# ===========================================================================
# TESTS
# ===========================================================================
class TestFallbackAction(unittest.TestCase):
    def test_construction(self):
        a = FallbackAction("deny", reason="no")
        self.assertEqual(a.action, "deny")
        self.assertEqual(a.reason, "no")
        self.assertIsNone(a.modified_request)
        self.assertIsNone(a.rule_name)
        self.assertTrue(a.is_default())

    def test_empty_action_rejected(self):
        with self.assertRaises(ValueError):
            FallbackAction("")

    def test_whitespace_action_rejected(self):
        with self.assertRaises(ValueError):
            FallbackAction("   ")

    def test_non_str_reason_rejected(self):
        with self.assertRaises(TypeError):
            FallbackAction("deny", reason=123)  # type: ignore[arg-type]

    def test_non_dict_modified_request_rejected(self):
        with self.assertRaises(TypeError):
            FallbackAction("deny", modified_request="oops")  # type: ignore[arg-type]

    def test_empty_rule_name_rejected(self):
        with self.assertRaises(ValueError):
            FallbackAction("deny", rule_name="")

    def test_frozen(self):
        a = FallbackAction("deny")
        with self.assertRaises(FrozenInstanceError):
            a.action = "allow"  # type: ignore[misc]

    def test_modified_request_copied(self):
        src = {"context": {"x": 1}}
        a = FallbackAction("deny", modified_request=src)
        src["context"]["x"] = 2
        self.assertEqual(a.modified_request, {"context": {"x": 1}})

    def test_is_default_false_when_rule_named(self):
        a = FallbackAction("degrade", rule_name="degrade-rule")
        self.assertFalse(a.is_default())

    def test_to_dict(self):
        a = FallbackAction(
            "degrade", reason="r", rule_name="rule-1",
            modified_request={"action": "degrade"},
        )
        self.assertEqual(
            a.to_dict(),
            {
                "action": "degrade",
                "reason": "r",
                "modified_request": {"action": "degrade"},
                "rule_name": "rule-1",
            },
        )


class TestRule(unittest.TestCase):
    def test_construction(self):
        r = Rule("r1", lambda req: True, "deny")
        self.assertEqual(r.name, "r1")
        self.assertEqual(r.action, "deny")
        self.assertEqual(r.priority, 0)

    def test_empty_name_rejected(self):
        with self.assertRaises(ValueError):
            Rule("", lambda r: True, "deny")

    def test_non_callable_matches_rejected(self):
        with self.assertRaises(TypeError):
            Rule("r1", None, "deny")  # type: ignore[arg-type]

    def test_empty_action_rejected(self):
        with self.assertRaises(ValueError):
            Rule("r1", lambda r: True, "")

    def test_non_int_priority_rejected(self):
        with self.assertRaises(TypeError):
            Rule("r1", lambda r: True, "deny", priority="high")  # type: ignore[arg-type]

    def test_frozen(self):
        r = Rule("r1", lambda req: True, "deny")
        with self.assertRaises(FrozenInstanceError):
            r.action = "allow"  # type: ignore[misc]


class TestPolicyManagement(unittest.TestCase):
    def test_init_with_rules(self):
        r = Rule("r1", lambda req: True, "deny")
        p = FallbackPolicy(rules=[r])
        self.assertEqual(len(p), 1)
        self.assertIs(list(p)[0], r)

    def test_init_rejects_non_rule(self):
        with self.assertRaises(TypeError):
            FallbackPolicy(rules=["not-a-rule"])  # type: ignore[list-item]

    def test_init_rejects_bad_default(self):
        with self.assertRaises(TypeError):
            FallbackPolicy(default="deny")  # type: ignore[arg-type]

    def test_add_rule_returns_rule(self):
        p = FallbackPolicy()
        r = p.add_rule(lambda req: True, "deny", reason="x")
        self.assertIsInstance(r, Rule)
        self.assertEqual(len(p), 1)

    def test_add_rule_auto_names(self):
        p = FallbackPolicy()
        r1 = p.add_rule(lambda req: True, "a")
        r2 = p.add_rule(lambda req: True, "b")
        self.assertEqual(r1.name, "rule-0")
        self.assertEqual(r2.name, "rule-1")

    def test_add_rule_rejects_bad_matches(self):
        p = FallbackPolicy()
        with self.assertRaises(TypeError):
            p.add_rule(None, "deny")  # type: ignore[arg-type]

    def test_add_rule_rejects_empty_action(self):
        p = FallbackPolicy()
        with self.assertRaises(ValueError):
            p.add_rule(lambda req: True, "")

    def test_add_rule_rejects_bad_reason(self):
        p = FallbackPolicy()
        with self.assertRaises(TypeError):
            p.add_rule(lambda req: True, "deny", reason=123)  # type: ignore[arg-type]

    def test_add_rule_rejects_bad_priority(self):
        p = FallbackPolicy()
        with self.assertRaises(TypeError):
            p.add_rule(lambda req: True, "deny", priority="high")  # type: ignore[arg-type]

    def test_remove_rule(self):
        p = FallbackPolicy()
        p.add_rule(lambda req: True, "a", name="r1")
        p.add_rule(lambda req: True, "b", name="r2")
        self.assertTrue(p.remove_rule("r1"))
        self.assertFalse(p.remove_rule("missing"))
        self.assertEqual(len(p), 1)
        self.assertEqual(list(p)[0].name, "r2")

    def test_clear(self):
        p = FallbackPolicy()
        p.add_rule(lambda req: True, "a")
        p.add_rule(lambda req: True, "b")
        p.clear()
        self.assertEqual(len(p), 0)

    def test_repr(self):
        p = FallbackPolicy()
        p.add_rule(lambda req: True, "a", name="rule-0")
        self.assertIn("rule-0", repr(p))
        self.assertIn("'deny'", repr(p))


def _mk_req(**kwargs: Any) -> DecisionRequest:
    defaults = dict(request_id="r1", action="deploy_model")
    defaults.update(kwargs)
    return DecisionRequest(**defaults)  # type: ignore[arg-type]


class TestSelect(unittest.IsolatedAsyncioTestCase):
    async def test_empty_policy_returns_default(self):
        p = FallbackPolicy()
        result = await p.select(_mk_req())
        self.assertEqual(result.action, "deny")
        self.assertIsNone(result.rule_name)
        self.assertTrue(result.is_default())

    async def test_custom_default(self):
        custom = FallbackAction("escalate", reason="no rule")
        p = FallbackPolicy(default=custom)
        result = await p.select(_mk_req())
        self.assertEqual(result.action, "escalate")
        self.assertTrue(result.is_default())

    async def test_single_rule_matches(self):
        p = FallbackPolicy()
        p.add_rule(lambda req: True, "degrade", reason="fallback", name="r1")
        result = await p.select(_mk_req())
        self.assertEqual(result.action, "degrade")
        self.assertEqual(result.reason, "fallback")
        self.assertEqual(result.rule_name, "r1")
        self.assertFalse(result.is_default())

    async def test_single_rule_no_match_falls_through(self):
        p = FallbackPolicy()
        p.add_rule(lambda req: False, "degrade", name="r1")
        result = await p.select(_mk_req())
        self.assertEqual(result.action, "deny")
        self.assertIsNone(result.rule_name)

    async def test_first_match_wins_by_insertion(self):
        p = FallbackPolicy()
        p.add_rule(lambda req: True, "first", name="r1")
        p.add_rule(lambda req: True, "second", name="r2")
        result = await p.select(_mk_req())
        self.assertEqual(result.action, "first")
        self.assertEqual(result.rule_name, "r1")

    async def test_priority_overrides_insertion_order(self):
        p = FallbackPolicy()
        p.add_rule(lambda req: True, "low", name="r1", priority=0)
        p.add_rule(lambda req: True, "high", name="r2", priority=10)
        result = await p.select(_mk_req())
        self.assertEqual(result.action, "high")
        self.assertEqual(result.rule_name, "r2")

    async def test_equal_priority_is_stable(self):
        p = FallbackPolicy()
        p.add_rule(lambda req: True, "first", name="r1", priority=5)
        p.add_rule(lambda req: True, "second", name="r2", priority=5)
        result = await p.select(_mk_req())
        self.assertEqual(result.action, "first")

    async def test_async_predicate(self):
        async def matches(req):
            return True

        p = FallbackPolicy()
        p.add_rule(matches, "degrade", name="async-rule")
        result = await p.select(_mk_req())
        self.assertEqual(result.action, "degrade")
        self.assertEqual(result.rule_name, "async-rule")

    async def test_mixed_sync_and_async(self):
        async def amatches(req):
            return False

        p = FallbackPolicy()
        p.add_rule(amatches, "first", name="a")
        p.add_rule(lambda req: True, "second", name="b")
        result = await p.select(_mk_req())
        self.assertEqual(result.action, "second")

    async def test_predicate_exception_isolated(self):
        def bad(req):
            raise RuntimeError("boom")

        p = FallbackPolicy()
        p.add_rule(bad, "never", name="bad")
        p.add_rule(lambda req: True, "next", name="ok")
        with self.assertLogs("tests", level="WARNING"):
            # Use the module's logger explicitly
            import logging as _logging

            logger_name = __name__.rsplit(".", 1)[0] if "." in __name__ else "fallback_module"
            # Fallback: the module logger is `logger`, use its name from the module:
            from sys import modules

            # Find the actual module logger by getting its name.
            # Simpler: patch logging through assertLogs on the root.
        # Re-run with the actual module logger:
        with self.assertLogs(level="WARNING") as log_cm:
            result = await p.select(_mk_req())
        self.assertEqual(result.action, "next")
        self.assertEqual(result.rule_name, "ok")
        self.assertTrue(
            any("bad" in line for line in log_cm.output),
            f"expected rule 'bad' to be logged: {log_cm.output}",
        )

    async def test_all_rules_raise_falls_to_default(self):
        def bad1(req):
            raise RuntimeError("one")

        def bad2(req):
            raise RuntimeError("two")

        p = FallbackPolicy()
        p.add_rule(bad1, "never1", name="b1")
        p.add_rule(bad2, "never2", name="b2")
        with self.assertLogs(level="WARNING"):
            result = await p.select(_mk_req())
        self.assertEqual(result.action, "deny")
        self.assertTrue(result.is_default())

    async def test_modified_request_includes_fallback_action(self):
        p = FallbackPolicy()
        p.add_rule(lambda req: True, "degrade", name="r1")
        req = _mk_req(context={"k": "v"})
        result = await p.select(req)
        self.assertEqual(
            result.modified_request["context"]["fallback_action"], "degrade"
        )
        self.assertEqual(result.modified_request["context"]["k"], "v")

    async def test_modified_request_does_not_mutate_caller(self):
        p = FallbackPolicy()
        p.add_rule(lambda req: True, "degrade", name="r1")
        req = _mk_req(context={"k": "v"})
        await p.select(req)
        self.assertEqual(req.context, {"k": "v"})

    async def test_modified_request_handles_missing_context(self):
        class NoContextRequest:
            request_id = "r1"
            action = "deploy_model"

            def to_dict(self):
                return {"request_id": self.request_id, "action": self.action}

        p = FallbackPolicy()
        p.add_rule(lambda req: True, "degrade", name="r1")
        result = await p.select(NoContextRequest())
        self.assertEqual(
            result.modified_request["context"]["fallback_action"], "degrade"
        )

    async def test_modified_request_handles_non_dict_context(self):
        class WeirdContext:
            request_id = "r1"
            action = "deploy_model"
            context = "not a dict"

            def to_dict(self):
                return {
                    "request_id": self.request_id,
                    "action": self.action,
                    "context": self.context,
                }

        p = FallbackPolicy()
        p.add_rule(lambda req: True, "degrade", name="r1")
        result = await p.select(WeirdContext())
        self.assertEqual(
            result.modified_request["context"]["fallback_action"], "degrade"
        )


class TestThreadSafety(unittest.TestCase):
    def test_concurrent_add_and_select_does_not_crash(self):
        import asyncio
        import threading

        p = FallbackPolicy()
        p.add_rule(lambda req: False, "no", name="base")

        stop = threading.Event()

        def adder(i: int) -> None:
            for n in range(50):
                p.add_rule(lambda req: False, "no", name=f"r-{i}-{n}")
            stop.set()

        def reader() -> None:
            while not stop.is_set():
                asyncio.run(p.select(_mk_req()))

        writers = [threading.Thread(target=adder, args=(i,)) for i in range(2)]
        readers = [threading.Thread(target=reader) for _ in range(2)]
        for w in writers + readers:
            w.start()
        for w in writers:
            w.join()
        stop.wait(timeout=1.0)
        for r in readers:
            r.join()
        # No crash == pass; also assert rule count
        self.assertEqual(len(p), 2 * 50 + 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
