"""
PermissionGuard — enforce per-actor permissions.

A thread-safe registry of :class:`ActorPermissions` with a single
authorization query, :meth:`PermissionGuard.check` (returns ``bool``)
and its structured sibling :meth:`PermissionGuard.evaluate` (returns a
:class:`CheckResult` carrying the reason for the denial).

Precedence
----------
For a query ``(actor_id, action, risk_level)``:

1. Unknown actor                        → deny  (``UNKNOWN_ACTOR``)
2. ``action`` in the actor's denied set → deny  (``EXPLICITLY_DENIED``)
3. Not ``allow_all`` and ``action`` not
   in the actor's allowlist             → deny  (``NOT_IN_ALLOWLIST``)
4. ``risk_level`` above the actor's
   ``max_risk_level``                   → deny  (``RISK_TOO_HIGH``)
5. Otherwise                            → allow

An empty ``allowed_actions`` set is **deny-by-default** — to grant an
actor unrestricted action access, set ``allow_all=True`` on its
:class:`ActorPermissions`. This is deliberate: an empty allowlist is
the most common source of accidental over-permission in real systems.

RiskLevel
---------
``RiskLevel`` is defined locally so this module stays self-contained;
``PolicyDecision`` and ``RiskClassification`` define identical enums.
In a shared codebase these should be imported from one canonical
module (e.g. ``governance.enums``). Because all three mix in ``str``,
cross-module ``==`` comparisons work; ``isinstance`` does not.
"""

from __future__ import annotations

import logging
import threading
import unittest
from dataclasses import FrozenInstanceError, dataclass
from enum import Enum
from typing import (
    Any,
    Dict,
    FrozenSet,
    Iterable,
    Iterator,
    Optional,
    Union,
)

logger = logging.getLogger(__name__)

__all__ = [
    "RiskLevel",
    "DenyReason",
    "CheckResult",
    "ActorPermissions",
    "PermissionGuard",
]


# ---------------------------------------------------------------------------
# RiskLevel
# ---------------------------------------------------------------------------
class RiskLevel(str, Enum):
    """Closed vocabulary for risk severity, ordered by ``rank``."""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

    @property
    def rank(self) -> int:
        return _RISK_RANK[self.value]


_RISK_RANK: Dict[str, int] = {r.value: i for i, r in enumerate(RiskLevel)}


def _as_risk(level: Union[RiskLevel, str]) -> RiskLevel:
    return level if isinstance(level, RiskLevel) else RiskLevel(level)


# ---------------------------------------------------------------------------
# DenyReason / CheckResult
# ---------------------------------------------------------------------------
class DenyReason(str, Enum):
    """Machine-readable code explaining why a check denied."""

    UNKNOWN_ACTOR = "unknown_actor"
    EXPLICITLY_DENIED = "explicitly_denied"
    NOT_IN_ALLOWLIST = "not_in_allowlist"
    RISK_TOO_HIGH = "risk_too_high"
    INVALID_RISK = "invalid_risk"


@dataclass(frozen=True)
class CheckResult:
    """Structured outcome of :meth:`PermissionGuard.evaluate`."""

    allowed: bool
    code: Optional[DenyReason] = None
    message: str = ""

    def __bool__(self) -> bool:
        """Ergonomic: ``if guard.evaluate(...): ...`` mirrors ``check``."""
        return self.allowed

    def to_dict(self) -> Dict[str, Any]:
        return {
            "allowed": self.allowed,
            "code": self.code.value if self.code else None,
            "message": self.message,
        }


# ---------------------------------------------------------------------------
# ActorPermissions
# ---------------------------------------------------------------------------
def _as_str_frozenset(value: Any, label: str) -> FrozenSet[str]:
    if isinstance(value, str):
        raise TypeError(
            f"{label} must be an iterable of strings, not a single "
            f"string; wrap in a list, e.g. {label}=['my_action']"
        )
    result = frozenset(value)
    for item in result:
        if not isinstance(item, str) or not item.strip():
            raise ValueError(
                f"{label} entries must be non-empty strings, got {item!r}"
            )
    return result


@dataclass(frozen=True)
class ActorPermissions:
    """Immutable description of what one actor is allowed to do."""

    actor_id: str
    allowed_actions: FrozenSet[str] = frozenset()
    denied_actions: FrozenSet[str] = frozenset()
    max_risk_level: RiskLevel = RiskLevel.MEDIUM
    allow_all: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.actor_id, str) or not self.actor_id.strip():
            raise ValueError("actor_id must be a non-empty string")

        object.__setattr__(
            self,
            "allowed_actions",
            _as_str_frozenset(self.allowed_actions, "allowed_actions"),
        )
        object.__setattr__(
            self,
            "denied_actions",
            _as_str_frozenset(self.denied_actions, "denied_actions"),
        )

        if not isinstance(self.max_risk_level, RiskLevel):
            object.__setattr__(
                self,
                "max_risk_level",
                RiskLevel(self.max_risk_level),
            )

        if not isinstance(self.allow_all, bool):
            raise TypeError("allow_all must be a bool")


# ---------------------------------------------------------------------------
# PermissionGuard
# ---------------------------------------------------------------------------
class PermissionGuard:
    """Thread-safe enforcement of per-actor permissions."""

    # Preserved for callers that reference it directly. Derived from the
    # enum so it can never drift.
    RISK_ORDER: list = [r.value for r in RiskLevel]

    def __init__(self) -> None:
        self._actors: Dict[str, ActorPermissions] = {}
        self._lock = threading.RLock()

    # -- registration ------------------------------------------------------
    def register(
        self,
        permissions: ActorPermissions,
        *,
        overwrite: bool = False,
    ) -> ActorPermissions:
        """Register ``permissions``. Raises ``KeyError`` on duplicates
        unless ``overwrite=True``."""
        if not isinstance(permissions, ActorPermissions):
            raise TypeError(
                f"expected ActorPermissions, got "
                f"{type(permissions).__name__}"
            )
        with self._lock:
            if permissions.actor_id in self._actors and not overwrite:
                raise KeyError(
                    f"actor {permissions.actor_id!r} already registered "
                    f"(pass overwrite=True to replace)"
                )
            self._actors[permissions.actor_id] = permissions
        return permissions

    def register_many(
        self,
        permissions: Iterable[ActorPermissions],
        *,
        overwrite: bool = False,
    ) -> None:
        for p in permissions:
            self.register(p, overwrite=overwrite)

    def unregister(self, actor_id: str) -> bool:
        """Remove an actor. Returns ``True`` if it existed."""
        with self._lock:
            return self._actors.pop(actor_id, None) is not None

    def get(self, actor_id: str) -> ActorPermissions:
        try:
            return self._actors[actor_id]
        except KeyError:
            raise KeyError(f"actor {actor_id!r} not registered") from None

    def __contains__(self, actor_id: object) -> bool:
        return actor_id in self._actors

    def __len__(self) -> int:
        return len(self._actors)

    def __iter__(self) -> Iterator[str]:
        with self._lock:
            return iter(list(self._actors))

    def __repr__(self) -> str:
        ids = ", ".join(sorted(self._actors))
        return f"PermissionGuard([{ids}])"

    # -- authorization -----------------------------------------------------
    def evaluate(
        self,
        actor_id: str,
        action: str,
        risk_level: Union[RiskLevel, str] = "low",
    ) -> CheckResult:
        """Return a structured authorization decision."""
        # Programming errors → raise.
        if not isinstance(actor_id, str) or not actor_id.strip():
            raise ValueError("actor_id must be a non-empty string")
        if not isinstance(action, str) or not action.strip():
            raise ValueError("action must be a non-empty string")

        # Data errors → structured deny.
        try:
            risk = _as_risk(risk_level)
        except ValueError:
            return self._deny(
                actor_id,
                action,
                DenyReason.INVALID_RISK,
                f"invalid risk level {risk_level!r}",
            )

        perm = self._actors.get(actor_id)
        if perm is None:
            return self._deny(
                actor_id,
                action,
                DenyReason.UNKNOWN_ACTOR,
                f"actor {actor_id!r} not registered",
            )

        if action in perm.denied_actions:
            return self._deny(
                actor_id,
                action,
                DenyReason.EXPLICITLY_DENIED,
                f"action {action!r} is explicitly denied for {actor_id!r}",
            )

        if not perm.allow_all and action not in perm.allowed_actions:
            return self._deny(
                actor_id,
                action,
                DenyReason.NOT_IN_ALLOWLIST,
                f"action {action!r} is not in the allowlist for "
                f"{actor_id!r}",
            )

        if risk.rank > perm.max_risk_level.rank:
            return self._deny(
                actor_id,
                action,
                DenyReason.RISK_TOO_HIGH,
                f"risk {risk.value!r} exceeds {actor_id!r}'s cap "
                f"of {perm.max_risk_level.value!r}",
            )

        return CheckResult(allowed=True, code=None, message="allowed")

    def check(
        self,
        actor_id: str,
        action: str,
        risk_level: Union[RiskLevel, str] = "low",
    ) -> bool:
        """Backward-compatible boolean wrapper over :meth:`evaluate`."""
        return self.evaluate(actor_id, action, risk_level).allowed

    # -- internals ---------------------------------------------------------
    @staticmethod
    def _deny(
        actor_id: str,
        action: str,
        code: DenyReason,
        message: str,
    ) -> CheckResult:
        logger.debug(
            "permission denied actor=%r action=%r code=%s",
            actor_id,
            action,
            code.value,
        )
        return CheckResult(allowed=False, code=code, message=message)


# ===========================================================================
# TESTS
# ===========================================================================
def _perm(**kwargs: Any) -> ActorPermissions:
    defaults = dict(actor_id="alice")
    defaults.update(kwargs)
    return ActorPermissions(**defaults)


class TestRiskLevel(unittest.TestCase):
    def test_rank_ordering(self):
        self.assertEqual(RiskLevel.LOW.rank, 0)
        self.assertEqual(RiskLevel.MEDIUM.rank, 1)
        self.assertEqual(RiskLevel.HIGH.rank, 2)
        self.assertEqual(RiskLevel.CRITICAL.rank, 3)

    def test_risk_order_compat(self):
        self.assertEqual(
            PermissionGuard.RISK_ORDER,
            ["low", "medium", "high", "critical"],
        )

    def test_str_enum_equality(self):
        self.assertEqual(RiskLevel.LOW, "low")
        self.assertEqual(RiskLevel.HIGH, "high")


class TestCheckResult(unittest.TestCase):
    def test_bool_true(self):
        self.assertTrue(CheckResult(allowed=True))

    def test_bool_false(self):
        self.assertFalse(CheckResult(allowed=False))

    def test_to_dict(self):
        r = CheckResult(
            allowed=False,
            code=DenyReason.RISK_TOO_HIGH,
            message="too risky",
        )
        self.assertEqual(
            r.to_dict(),
            {
                "allowed": False,
                "code": "risk_too_high",
                "message": "too risky",
            },
        )

    def test_to_dict_allow(self):
        self.assertEqual(
            CheckResult(allowed=True).to_dict(),
            {"allowed": True, "code": None, "message": ""},
        )

    def test_frozen(self):
        r = CheckResult(allowed=True)
        with self.assertRaises(FrozenInstanceError):
            r.allowed = False  # type: ignore[misc]


class TestActorPermissions(unittest.TestCase):
    def test_minimal(self):
        p = ActorPermissions(actor_id="alice")
        self.assertEqual(p.actor_id, "alice")
        self.assertEqual(p.allowed_actions, frozenset())
        self.assertEqual(p.denied_actions, frozenset())
        self.assertIs(p.max_risk_level, RiskLevel.MEDIUM)
        self.assertFalse(p.allow_all)

    def test_empty_actor_id_rejected(self):
        with self.assertRaises(ValueError):
            ActorPermissions(actor_id="")

    def test_whitespace_actor_id_rejected(self):
        with self.assertRaises(ValueError):
            ActorPermissions(actor_id="   ")

    def test_non_str_actor_id_rejected(self):
        with self.assertRaises(ValueError):
            ActorPermissions(actor_id=123)  # type: ignore[arg-type]

    def test_allowed_actions_coerced_to_frozenset(self):
        p = ActorPermissions(actor_id="a", allowed_actions=["x", "y"])
        self.assertIsInstance(p.allowed_actions, frozenset)
        self.assertEqual(p.allowed_actions, frozenset({"x", "y"}))

    def test_single_string_allowed_actions_rejected(self):
        with self.assertRaises(TypeError):
            ActorPermissions(actor_id="a", allowed_actions="xy")  # type: ignore[arg-type]

    def test_single_string_denied_actions_rejected(self):
        with self.assertRaises(TypeError):
            ActorPermissions(actor_id="a", denied_actions="xy")  # type: ignore[arg-type]

    def test_empty_action_entry_rejected(self):
        with self.assertRaises(ValueError):
            ActorPermissions(actor_id="a", allowed_actions=[""])

    def test_whitespace_action_entry_rejected(self):
        with self.assertRaises(ValueError):
            ActorPermissions(actor_id="a", denied_actions=["   "])

    def test_non_str_action_entry_rejected(self):
        with self.assertRaises(ValueError):
            ActorPermissions(actor_id="a", allowed_actions=[123])  # type: ignore[list-item]

    def test_max_risk_coercion(self):
        p = ActorPermissions(actor_id="a", max_risk_level="critical")  # type: ignore[arg-type]
        self.assertIs(p.max_risk_level, RiskLevel.CRITICAL)

    def test_max_risk_enum(self):
        p = ActorPermissions(actor_id="a", max_risk_level=RiskLevel.HIGH)
        self.assertIs(p.max_risk_level, RiskLevel.HIGH)

    def test_max_risk_wrong_case_rejected(self):
        with self.assertRaises(ValueError):
            ActorPermissions(actor_id="a", max_risk_level="HIGH")  # type: ignore[arg-type]

    def test_max_risk_typo_rejected(self):
        with self.assertRaises(ValueError):
            ActorPermissions(actor_id="a", max_risk_level="hgih")  # type: ignore[arg-type]

    def test_allow_all_type_checked(self):
        with self.assertRaises(TypeError):
            ActorPermissions(actor_id="a", allow_all=1)  # type: ignore[arg-type]

    def test_frozen(self):
        p = ActorPermissions(actor_id="a")
        with self.assertRaises(FrozenInstanceError):
            p.actor_id = "b"  # type: ignore[misc]

    def test_hashable(self):
        p = ActorPermissions(actor_id="a")
        self.assertEqual({p, p}, {p})

    def test_caller_list_not_aliased(self):
        src = ["x"]
        p = ActorPermissions(actor_id="a", allowed_actions=src)
        src.append("y")
        self.assertEqual(p.allowed_actions, frozenset({"x"}))


class TestRegistration(unittest.TestCase):
    def test_register_and_get(self):
        g = PermissionGuard()
        p = g.register(_perm(allowed_actions=["deploy"]))
        self.assertIs(g.get("alice"), p)
        self.assertIn("alice", g)
        self.assertEqual(len(g), 1)

    def test_register_duplicate_raises(self):
        g = PermissionGuard()
        g.register(_perm())
        with self.assertRaises(KeyError):
            g.register(_perm())

    def test_register_overwrite(self):
        g = PermissionGuard()
        g.register(_perm(allowed_actions=["a"]))
        g.register(_perm(allowed_actions=["b"]), overwrite=True)
        self.assertEqual(
            g.get("alice").allowed_actions, frozenset({"b"})
        )

    def test_register_non_permissions_raises(self):
        g = PermissionGuard()
        with self.assertRaises(TypeError):
            g.register("alice")  # type: ignore[arg-type]

    def test_register_many(self):
        g = PermissionGuard()
        g.register_many(
            [
                _perm(actor_id="alice", allowed_actions=["x"]),
                _perm(actor_id="bob", allowed_actions=["y"]),
            ]
        )
        self.assertEqual(len(g), 2)

    def test_register_many_overwrite(self):
        g = PermissionGuard()
        g.register(_perm(actor_id="alice"))
        g.register_many(
            [_perm(actor_id="alice", allowed_actions=["x"])],
            overwrite=True,
        )
        self.assertEqual(
            g.get("alice").allowed_actions, frozenset({"x"})
        )

    def test_unregister(self):
        g = PermissionGuard()
        g.register(_perm())
        self.assertTrue(g.unregister("alice"))
        self.assertFalse(g.unregister("alice"))
        self.assertNotIn("alice", g)

    def test_get_missing_raises(self):
        g = PermissionGuard()
        with self.assertRaises(KeyError):
            g.get("ghost")

    def test_iter_and_repr(self):
        g = PermissionGuard()
        g.register(_perm(actor_id="alice"))
        g.register(_perm(actor_id="bob"))
        self.assertEqual(set(g), {"alice", "bob"})
        self.assertIn("alice", repr(g))
        self.assertIn("bob", repr(g))


class TestEvaluate(unittest.TestCase):
    def _guard(self, **perm_kwargs: Any) -> PermissionGuard:
        g = PermissionGuard()
        g.register(_perm(**perm_kwargs))
        return g

    def test_unknown_actor(self):
        g = PermissionGuard()
        r = g.evaluate("ghost", "deploy")
        self.assertFalse(r)
        self.assertIs(r.code, DenyReason.UNKNOWN_ACTOR)

    def test_explicitly_denied(self):
        g = self._guard(
            allowed_actions=["deploy"], denied_actions=["deploy"]
        )
        r = g.evaluate("alice", "deploy")
        self.assertFalse(r)
        self.assertIs(r.code, DenyReason.EXPLICITLY_DENIED)

    def test_denied_beats_allow_all(self):
        g = self._guard(allow_all=True, denied_actions=["deploy"])
        r = g.evaluate("alice", "deploy")
        self.assertFalse(r)
        self.assertIs(r.code, DenyReason.EXPLICITLY_DENIED)

    def test_empty_allowlist_denies_by_default(self):
        g = self._guard()  # empty allowed_actions, allow_all=False
        r = g.evaluate("alice", "deploy")
        self.assertFalse(r)
        self.assertIs(r.code, DenyReason.NOT_IN_ALLOWLIST)

    def test_allow_all_grants(self):
        g = self._guard(allow_all=True)
        self.assertTrue(g.evaluate("alice", "anything"))

    def test_allowlist_permits(self):
        g = self._guard(allowed_actions=["deploy"])
        self.assertTrue(g.evaluate("alice", "deploy"))

    def test_allowlist_denies_others(self):
        g = self._guard(allowed_actions=["deploy"])
        r = g.evaluate("alice", "delete")
        self.assertFalse(r)
        self.assertIs(r.code, DenyReason.NOT_IN_ALLOWLIST)

    def test_risk_at_cap(self):
        g = self._guard(
            allow_all=True, max_risk_level=RiskLevel.HIGH,
        )
        self.assertTrue(g.evaluate("alice", "deploy", "high"))

    def test_risk_above_cap(self):
        g = self._guard(
            allow_all=True, max_risk_level=RiskLevel.HIGH,
        )
        r = g.evaluate("alice", "deploy", "critical")
        self.assertFalse(r)
        self.assertIs(r.code, DenyReason.RISK_TOO_HIGH)

    def test_risk_below_cap(self):
        g = self._guard(
            allow_all=True, max_risk_level=RiskLevel.HIGH,
        )
        self.assertTrue(g.evaluate("alice", "deploy", "low"))

    def test_risk_level_accepts_enum_and_str(self):
        g = self._guard(allow_all=True, max_risk_level=RiskLevel.LOW)
        self.assertTrue(g.evaluate("alice", "x", RiskLevel.LOW))
        self.assertTrue(g.evaluate("alice", "x", "low"))

    def test_invalid_risk_returns_structured_deny(self):
        g = self._guard(allow_all=True)
        r = g.evaluate("alice", "x", "HIGH")  # wrong case
        self.assertFalse(r)
        self.assertIs(r.code, DenyReason.INVALID_RISK)
        self.assertIn("HIGH", r.message)

    def test_invalid_risk_typo(self):
        g = self._guard(allow_all=True)
        r = g.evaluate("alice", "x", "hgih")
        self.assertFalse(r)
        self.assertIs(r.code, DenyReason.INVALID_RISK)

    def test_empty_actor_id_raises(self):
        g = PermissionGuard()
        with self.assertRaises(ValueError):
            g.evaluate("", "deploy")

    def test_empty_action_raises(self):
        g = PermissionGuard()
        with self.assertRaises(ValueError):
            g.evaluate("alice", "")

    def test_check_wraps_evaluate(self):
        g = self._guard(allow_all=True)
        self.assertTrue(g.check("alice", "deploy"))
        g2 = self._guard()  # no allowlist, no allow_all
        self.assertFalse(g2.check("alice", "deploy"))

    def test_check_unknown_actor(self):
        g = PermissionGuard()
        self.assertFalse(g.check("ghost", "deploy"))

    def test_allowlist_with_risk_cap(self):
        # Both must pass.
        g = self._guard(
            allowed_actions=["deploy"], max_risk_level=RiskLevel.MEDIUM,
        )
        self.assertTrue(g.evaluate("alice", "deploy", "medium"))
        self.assertFalse(g.evaluate("alice", "deploy", "high"))
        self.assertFalse(g.evaluate("alice", "delete", "low"))

    def test_deny_reason_messages_informative(self):
        g = self._guard(allow_all=True, max_risk_level=RiskLevel.LOW)
        r = g.evaluate("alice", "x", "high")
        self.assertIn("high", r.message)
        self.assertIn("low", r.message)


class TestThreadSafety(unittest.TestCase):
    def test_concurrent_register_and_evaluate(self):
        g = PermissionGuard()
        stop = threading.Event()

        def registrar(i: int) -> None:
            for n in range(50):
                try:
                    g.register(
                        _perm(actor_id=f"a-{i}-{n}", allow_all=True)
                    )
                except KeyError:
                    pass

        def reader() -> None:
            while not stop.is_set():
                g.evaluate("a-0-0", "x")
                g.check("a-0-0", "x")

        writers = [
            threading.Thread(target=registrar, args=(i,))
            for i in range(2)
        ]
        readers = [threading.Thread(target=reader) for _ in range(2)]
        for w in writers + readers:
            w.start()
        for w in writers:
            w.join()
        stop.set()
        for r in readers:
            r.join()
        self.assertEqual(len(g), 100)


if __name__ == "__main__":
    unittest.main(verbosity=2)
