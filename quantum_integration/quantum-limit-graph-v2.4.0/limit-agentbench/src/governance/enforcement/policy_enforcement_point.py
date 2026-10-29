"""
PolicyEnforcementPoint — executes the PolicyDecision.

The PEP is the runtime side of the PDP: it applies the verdict, runs
the obligations, and dispatches the requested action.

Key guarantees
--------------
* **Fail-closed.** Before executing, the PEP re-checks the decision's
  executability (verdict ∈ {ALLOW, MODIFY}; not expired; not failed
  verification; ``human_approval_required`` false; ``audit_event_id``
  present). An unrecognized verdict string **blocks**; it never falls
  through to execution.
* **Obligations are fail-closed by default.** A ``required=True``
  obligation whose handler raises (or times out) blocks execution.
  ``required=False`` obligations are best-effort; their failures are
  recorded but do not block.
* **Timeouts.** Every obligation handler and the executor are wrapped
  in ``asyncio.wait_for``. A slow collaborator denies the action.
* **Executor failures are structured.** An exception from the executor
  becomes ``outcome.error`` — callers get a summary dict even when the
  executor throws.
* **Truthiness-safe.** ``MODIFY`` with ``modified_request={}`` uses the
  empty dict; it never falls back to ``request.to_dict()``.
* **Thread-safe.** Registry mutations and enforcement are serialized
  on a reentrant lock; enforcement uses a stable snapshot.
* **Observable outcome.** :class:`EnforcementOutcome` is a frozen
  dataclass with a ``to_dict()`` that is JSON-serializable
  (``verdict`` is a string, not an enum).
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import threading
import unittest
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Tuple,
    Union,
)

# ---------------------------------------------------------------------------
# Real imports with standalone stubs
# ---------------------------------------------------------------------------
try:  # pragma: no cover
    from ..contracts.policy_decision import (  # type: ignore
        DecisionVerdict,
        PolicyDecision,
    )
    from ..contracts.decision_request import DecisionRequest  # type: ignore
except Exception:  # pragma: no cover — standalone / test mode
    class DecisionVerdict(str, Enum):
        ALLOW = "allow"
        DENY = "deny"
        MODIFY = "modify"
        ESCALATE = "escalate"
        FALLBACK = "fallback"

    class RiskLevel(str, Enum):
        LOW = "low"
        MEDIUM = "medium"
        HIGH = "high"
        CRITICAL = "critical"

    @dataclass(frozen=True)
    class DecisionRequest:
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

        def __post_init__(self):
            if not isinstance(self.action, str) or not self.action.strip():
                raise ValueError("action must be non-empty")
            if not isinstance(self.request_id, str) or not self.request_id.strip():
                raise ValueError("request_id must be non-empty")
            object.__setattr__(self, "context", dict(self.context))

        def to_dict(self) -> Dict[str, Any]:
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
        issued_at: datetime = field(
            default_factory=lambda: datetime.now(timezone.utc)
        )
        expires_at: Optional[datetime] = None

        def __post_init__(self):
            if not isinstance(self.verdict, DecisionVerdict):
                object.__setattr__(self, "verdict", DecisionVerdict(self.verdict))
            if not isinstance(self.risk_level, RiskLevel):
                object.__setattr__(self, "risk_level", RiskLevel(self.risk_level))
            object.__setattr__(self, "reasons", list(self.reasons))
            object.__setattr__(self, "obligations", list(self.obligations))
            if self.modified_request is not None:
                object.__setattr__(self, "modified_request", dict(self.modified_request))
            if not isinstance(self.issued_at, datetime) or self.issued_at.tzinfo is None:
                raise ValueError("issued_at must be timezone-aware")
            object.__setattr__(
                self, "issued_at", self.issued_at.astimezone(timezone.utc)
            )
            if self.expires_at is not None:
                if not isinstance(self.expires_at, datetime):
                    raise TypeError("expires_at must be a datetime")
                if self.expires_at.tzinfo is None:
                    raise ValueError("expires_at must be timezone-aware")
                object.__setattr__(
                    self, "expires_at", self.expires_at.astimezone(timezone.utc)
                )
                if self.expires_at <= self.issued_at:
                    raise ValueError("expires_at must be after issued_at")
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
    "EnforcementOutcome",
    "PolicyEnforcementPoint",
    "DEFAULT_OBLIGATION_TIMEOUT",
    "DEFAULT_EXECUTOR_TIMEOUT",
]

DEFAULT_OBLIGATION_TIMEOUT = 30.0
DEFAULT_EXECUTOR_TIMEOUT = 30.0


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _coerce_verdict(raw: Any) -> Optional[DecisionVerdict]:
    """Return a DecisionVerdict, or None if the value is unrecognized."""
    if isinstance(raw, DecisionVerdict):
        return raw
    if isinstance(raw, str):
        try:
            return DecisionVerdict(raw)
        except ValueError:
            return None
    return None


async def _maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value


def _why_not_executable(
    decision: PolicyDecision,
    *,
    now: Optional[datetime] = None,
) -> Optional[str]:
    """Return a human-readable reason the decision cannot be executed
    right now, or ``None`` if it can."""
    now = now or datetime.now(timezone.utc)
    if not isinstance(now, datetime) or now.tzinfo is None:
        raise ValueError("now must be timezone-aware")
    now = now.astimezone(timezone.utc)

    if decision.verdict not in (DecisionVerdict.ALLOW, DecisionVerdict.MODIFY):
        return f"verdict {decision.verdict.value!r} is not executable"
    if decision.human_approval_required:
        return "human approval required"
    if decision.verification_status == "failed":
        return "verification failed"
    if not decision.audit_event_id:
        return "missing audit_event_id"
    expires = getattr(decision, "expires_at", None)
    if expires is not None:
        if not isinstance(expires, datetime) or expires.tzinfo is None:
            return "invalid expires_at"
        if now > expires.astimezone(timezone.utc):
            return "decision expired"
    return None


# ---------------------------------------------------------------------------
# Obligation entry
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class _ObligationEntry:
    handler: Callable
    required: bool = True


# ---------------------------------------------------------------------------
# Outcome
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class EnforcementOutcome:
    """Structured result of :meth:`PolicyEnforcementPoint.enforce`."""

    verdict: str
    executed: bool = False
    blocked: bool = False
    blocked_reason: Optional[str] = None
    awaiting_human: bool = False
    escalation_ticket: Any = None
    obligations_run: Tuple[str, ...] = ()
    obligations_failed: Tuple[Dict[str, Any], ...] = ()
    result: Any = None
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "verdict": self.verdict,
            "executed": self.executed,
            "blocked": self.blocked,
            "blocked_reason": self.blocked_reason,
            "awaiting_human": self.awaiting_human,
            "escalation_ticket": self.escalation_ticket,
            "obligations_run": list(self.obligations_run),
            "obligations_failed": list(self.obligations_failed),
            "result": self.result,
            "error": self.error,
        }


# ---------------------------------------------------------------------------
# PolicyEnforcementPoint
# ---------------------------------------------------------------------------
class PolicyEnforcementPoint:
    """
    Consumes a :class:`PolicyDecision` and dispatches the corresponding
    action.

    Obligation handlers are registered by name; an obligation of the
    form ``"name:suffix"`` uses the handler registered as ``"name"``.
    A ``required=True`` obligation that fails blocks execution. Handlers
    are assumed to be safe to call concurrently; registry mutation is
    serialized on an internal lock.
    """

    def __init__(
        self,
        *,
        obligation_timeout: float = DEFAULT_OBLIGATION_TIMEOUT,
        executor_timeout: float = DEFAULT_EXECUTOR_TIMEOUT,
    ) -> None:
        if not isinstance(obligation_timeout, (int, float)) or obligation_timeout <= 0:
            raise ValueError("obligation_timeout must be a positive number")
        if not isinstance(executor_timeout, (int, float)) or executor_timeout <= 0:
            raise ValueError("executor_timeout must be a positive number")

        self._obligation_handlers: Dict[str, _ObligationEntry] = {}
        self._verdict_handlers: Dict[str, Callable] = {}
        self._lock = threading.RLock()
        self.obligation_timeout = float(obligation_timeout)
        self.executor_timeout = float(executor_timeout)

    # -- registration ------------------------------------------------------
    def register_obligation(
        self,
        name: str,
        handler: Callable,
        *,
        required: bool = True,
        overwrite: bool = False,
    ) -> None:
        if not isinstance(name, str) or not name.strip():
            raise ValueError("obligation name must be a non-empty string")
        if not callable(handler):
            raise TypeError("handler must be callable")
        if not isinstance(required, bool):
            raise TypeError("required must be a bool")
        with self._lock:
            if name in self._obligation_handlers and not overwrite:
                raise KeyError(
                    f"obligation {name!r} already registered "
                    f"(pass overwrite=True to replace)"
                )
            self._obligation_handlers[name] = _ObligationEntry(handler, required)

    def register_verdict(
        self,
        verdict: Union[DecisionVerdict, str],
        handler: Callable,
        *,
        overwrite: bool = False,
    ) -> None:
        try:
            key = DecisionVerdict(verdict).value
        except ValueError:
            raise ValueError(f"unknown verdict {verdict!r}") from None
        if not callable(handler):
            raise TypeError("handler must be callable")
        with self._lock:
            if key in self._verdict_handlers and not overwrite:
                raise KeyError(
                    f"verdict {key!r} already registered "
                    f"(pass overwrite=True to replace)"
                )
            self._verdict_handlers[key] = handler

    def unregister_obligation(self, name: str) -> bool:
        with self._lock:
            return self._obligation_handlers.pop(name, None) is not None

    def unregister_verdict(self, verdict: Union[DecisionVerdict, str]) -> bool:
        key = DecisionVerdict(verdict).value
        with self._lock:
            return self._verdict_handlers.pop(key, None) is not None

    def __contains__(self, name: object) -> bool:
        return name in self._obligation_handlers

    def __len__(self) -> int:
        return len(self._obligation_handlers)

    def __iter__(self) -> Iterator[str]:
        with self._lock:
            return iter(list(self._obligation_handlers))

    def __repr__(self) -> str:
        obl = ", ".join(sorted(self._obligation_handlers))
        vrd = ", ".join(sorted(self._verdict_handlers))
        return f"PolicyEnforcementPoint(obligations=[{obl}], verdicts=[{vrd}])"

    # -- enforcement -------------------------------------------------------
    async def enforce(
        self,
        decision: PolicyDecision,
        request: DecisionRequest,
        *,
        executor: Optional[Callable] = None,
        fallback_action: Optional[str] = None,
        now: Optional[datetime] = None,
    ) -> EnforcementOutcome:
        """
        Enforce ``decision`` for ``request`` and return an
        :class:`EnforcementOutcome`.
        """
        if not isinstance(decision, PolicyDecision):
            raise TypeError(
                f"decision must be PolicyDecision, got "
                f"{type(decision).__name__}"
            )
        if not isinstance(request, DecisionRequest):
            raise TypeError(
                f"request must be DecisionRequest, got "
                f"{type(request).__name__}"
            )

        verdict = _coerce_verdict(decision.verdict)
        if verdict is None:
            return EnforcementOutcome(
                verdict=str(decision.verdict),
                blocked=True,
                blocked_reason=(
                    f"unrecognized verdict {decision.verdict!r}"
                ),
            )

        # -- 1. Obligations ------------------------------------------
        obligations_run: List[str] = []
        obligations_failed: List[Dict[str, Any]] = []

        with self._lock:
            obligation_snapshot = dict(self._obligation_handlers)
            verdict_snapshot = dict(self._verdict_handlers)

        for obligation in decision.obligations:
            prefix = obligation.split(":", 1)[0]
            entry = obligation_snapshot.get(prefix)
            if entry is None:
                # Unknown obligation. Fail-closed: the decision asked us
                # to do something we cannot do.
                obligations_failed.append({
                    "obligation": obligation,
                    "reason": "no handler registered",
                })
                return EnforcementOutcome(
                    verdict=verdict.value,
                    blocked=True,
                    blocked_reason=(
                        f"no handler for obligation {obligation!r}"
                    ),
                    obligations_run=tuple(obligations_run),
                    obligations_failed=tuple(obligations_failed),
                )

            try:
                await asyncio.wait_for(
                    _maybe_await(entry.handler(request, decision)),
                    timeout=self.obligation_timeout,
                )
                obligations_run.append(obligation)
            except asyncio.TimeoutError:
                obligations_failed.append({
                    "obligation": obligation,
                    "reason": "timed out",
                })
                if entry.required:
                    return EnforcementOutcome(
                        verdict=verdict.value,
                        blocked=True,
                        blocked_reason=(
                            f"required obligation {obligation!r} timed out"
                        ),
                        obligations_run=tuple(obligations_run),
                        obligations_failed=tuple(obligations_failed),
                    )
            except Exception as exc:
                logger.exception(
                    "obligation %r raised", obligation,
                )
                obligations_failed.append({
                    "obligation": obligation,
                    "reason": repr(exc),
                })
                if entry.required:
                    return EnforcementOutcome(
                        verdict=verdict.value,
                        blocked=True,
                        blocked_reason=(
                            f"required obligation {obligation!r} failed"
                        ),
                        obligations_run=tuple(obligations_run),
                        obligations_failed=tuple(obligations_failed),
                    )

        # -- 2. Verdict dispatch -------------------------------------
        if verdict is DecisionVerdict.DENY:
            handler = verdict_snapshot.get("deny")
            if handler is not None:
                try:
                    await asyncio.wait_for(
                        _maybe_await(handler(request, decision)),
                        timeout=self.obligation_timeout,
                    )
                except Exception:
                    logger.exception("deny handler raised")
            return EnforcementOutcome(
                verdict=verdict.value,
                blocked=True,
                blocked_reason="verdict is deny",
                obligations_run=tuple(obligations_run),
                obligations_failed=tuple(obligations_failed),
            )

        if verdict is DecisionVerdict.ESCALATE:
            ticket: Any = None
            handler = verdict_snapshot.get("escalate")
            if handler is not None:
                try:
                    ticket = await asyncio.wait_for(
                        _maybe_await(
                            handler(request, decision)
                        ),
                        timeout=self.obligation_timeout,
                    )
                except Exception:
                    logger.exception("escalate handler raised")
            return EnforcementOutcome(
                verdict=verdict.value,
                awaiting_human=True,
                escalation_ticket=ticket,
                obligations_run=tuple(obligations_run),
                obligations_failed=tuple(obligations_failed),
            )

        if verdict is DecisionVerdict.FALLBACK:
            handler = verdict_snapshot.get("fallback")
            if handler is not None:
                try:
                    await asyncio.wait_for(
                        _maybe_await(
                            handler(request, decision, fallback_action)
                        ),
                        timeout=self.obligation_timeout,
                    )
                except Exception:
                    logger.exception("fallback handler raised")
            return EnforcementOutcome(
                verdict=verdict.value,
                blocked=True,
                blocked_reason="verdict is fallback",
                obligations_run=tuple(obligations_run),
                obligations_failed=tuple(obligations_failed),
            )

        # -- 3. ALLOW / MODIFY: re-check executability ---------------
        reason = _why_not_executable(decision, now=now)
        if reason is not None:
            return EnforcementOutcome(
                verdict=verdict.value,
                blocked=True,
                blocked_reason=reason,
                obligations_run=tuple(obligations_run),
                obligations_failed=tuple(obligations_failed),
            )

        # -- 4. Determine the executor target -----------------------
        if verdict is DecisionVerdict.MODIFY:
            target: Dict[str, Any] = decision.modified_request  # type: ignore
            # __post_init__ enforces non-None, but be defensive.
            if target is None:
                return EnforcementOutcome(
                    verdict=verdict.value,
                    blocked=True,
                    blocked_reason="MODIFY without modified_request",
                    obligations_run=tuple(obligations_run),
                    obligations_failed=tuple(obligations_failed),
                )
        else:
            target = request.to_dict()

        if executor is None:
            # The decision is approved; the caller chose not to run an
            # executor here. Not blocked.
            return EnforcementOutcome(
                verdict=verdict.value,
                executed=False,
                obligations_run=tuple(obligations_run),
                obligations_failed=tuple(obligations_failed),
            )

        # -- 5. Run executor with timeout ---------------------------
        try:
            result = await asyncio.wait_for(
                _maybe_await(executor(target)),
                timeout=self.executor_timeout,
            )
            return EnforcementOutcome(
                verdict=verdict.value,
                executed=True,
                result=result,
                obligations_run=tuple(obligations_run),
                obligations_failed=tuple(obligations_failed),
            )
        except asyncio.TimeoutError:
            logger.warning("executor timed out")
            return EnforcementOutcome(
                verdict=verdict.value,
                executed=False,
                blocked=True,
                blocked_reason="executor timed out",
                error="executor timed out",
                obligations_run=tuple(obligations_run),
                obligations_failed=tuple(obligations_failed),
            )
        except Exception as exc:
            logger.exception("executor raised")
            return EnforcementOutcome(
                verdict=verdict.value,
                executed=False,
                blocked=True,
                blocked_reason=f"executor raised: {exc}",
                error=f"executor raised: {exc}",
                obligations_run=tuple(obligations_run),
                obligations_failed=tuple(obligations_failed),
            )


# ===========================================================================
# TESTS
# ===========================================================================
def _request(**kwargs: Any) -> DecisionRequest:
    defaults = dict(action="deploy_model", request_id="req-1")
    defaults.update(kwargs)
    return DecisionRequest(**defaults)


def _decision(**kwargs: Any) -> PolicyDecision:
    """Build a valid decision with sensible defaults for tests."""
    defaults: Dict[str, Any] = dict(
        request_id="req-1",
        verdict=DecisionVerdict.ALLOW,
        policy_version="v1",
        audit_event_id="audit-1",
    )
    defaults.update(kwargs)
    return PolicyDecision(**defaults)


def _expired_allow() -> PolicyDecision:
    issued = datetime.now(timezone.utc) - timedelta(hours=2)
    expires = datetime.now(timezone.utc) - timedelta(hours=1)
    return PolicyDecision(
        request_id="req-1",
        verdict=DecisionVerdict.ALLOW,
        policy_version="v1",
        audit_event_id="audit-1",
        issued_at=issued,
        expires_at=expires,
    )


class TestRegistration(unittest.TestCase):
    def test_register_obligation(self):
        pep = PolicyEnforcementPoint()
        pep.register_obligation("audit", lambda r, d: None)
        self.assertIn("audit", pep)
        self.assertEqual(len(pep), 1)

    def test_register_obligation_bad_name(self):
        pep = PolicyEnforcementPoint()
        with self.assertRaises(ValueError):
            pep.register_obligation("", lambda r, d: None)
        with self.assertRaises(ValueError):
            pep.register_obligation("   ", lambda r, d: None)

    def test_register_obligation_bad_handler(self):
        pep = PolicyEnforcementPoint()
        with self.assertRaises(TypeError):
            pep.register_obligation("audit", None)  # type: ignore
        with self.assertRaises(TypeError):
            pep.register_obligation("audit", 42)  # type: ignore

    def test_register_obligation_bad_required(self):
        pep = PolicyEnforcementPoint()
        with self.assertRaises(TypeError):
            pep.register_obligation(
                "audit", lambda r, d: None, required="yes",  # type: ignore
            )

    def test_register_obligation_duplicate_raises(self):
        pep = PolicyEnforcementPoint()
        pep.register_obligation("audit", lambda r, d: None)
        with self.assertRaises(KeyError):
            pep.register_obligation("audit", lambda r, d: None)

    def test_register_obligation_overwrite(self):
        pep = PolicyEnforcementPoint()
        calls: List[str] = []
        pep.register_obligation("audit", lambda r, d: calls.append("v1"))
        pep.register_obligation(
            "audit", lambda r, d: calls.append("v2"), overwrite=True,
        )
        self.assertEqual(len(pep), 1)

    def test_register_verdict_bad_verdict(self):
        pep = PolicyEnforcementPoint()
        with self.assertRaises(ValueError):
            pep.register_verdict("nope", lambda r, d: None)

    def test_register_verdict_duplicate_raises(self):
        pep = PolicyEnforcementPoint()
        pep.register_verdict("deny", lambda r, d: None)
        with self.assertRaises(KeyError):
            pep.register_verdict("deny", lambda r, d: None)

    def test_unregister(self):
        pep = PolicyEnforcementPoint()
        pep.register_obligation("audit", lambda r, d: None)
        pep.register_verdict("deny", lambda r, d: None)
        self.assertTrue(pep.unregister_obligation("audit"))
        self.assertTrue(pep.unregister_verdict("deny"))
        self.assertFalse(pep.unregister_obligation("audit"))
        self.assertFalse(pep.unregister_verdict("deny"))

    def test_iter_and_repr(self):
        pep = PolicyEnforcementPoint()
        pep.register_obligation("audit", lambda r, d: None)
        pep.register_verdict("deny", lambda r, d: None)
        self.assertEqual(set(pep), {"audit"})
        self.assertIn("audit", repr(pep))
        self.assertIn("deny", repr(pep))


class TestInputValidation(unittest.IsolatedAsyncioTestCase):
    async def test_none_decision(self):
        pep = PolicyEnforcementPoint()
        with self.assertRaises(TypeError):
            await pep.enforce(None, _request())  # type: ignore

    async def test_none_request(self):
        pep = PolicyEnforcementPoint()
        with self.assertRaises(TypeError):
            await pep.enforce(_decision(), None)  # type: ignore

    async def test_wrong_decision_type(self):
        pep = PolicyEnforcementPoint()
        with self.assertRaises(TypeError):
            await pep.enforce("nope", _request())  # type: ignore


class TestFailClosedOnUnrecognizedVerdict(unittest.IsolatedAsyncioTestCase):
    async def test_wrong_case_blocks(self):
        pep = PolicyEnforcementPoint()
        # Create a "decision-like" object with a broken verdict.
        d = _decision()
        object.__setattr__(d, "verdict", "DENY")  # type: ignore
        executed = []

        async def executor(target):
            executed.append(target)

        out = await pep.enforce(d, _request(), executor=executor)
        self.assertTrue(out.blocked)
        self.assertEqual(out.blocked_reason, "unrecognized verdict 'DENY'")
        self.assertEqual(executed, [])

    async def test_typo_blocks(self):
        pep = PolicyEnforcementPoint()
        d = _decision()
        object.__setattr__(d, "verdict", "alloww")  # type: ignore
        out = await pep.enforce(d, _request(), executor=lambda t: None)
        self.assertTrue(out.blocked)


class TestFailClosedOnBadExecutability(unittest.IsolatedAsyncioTestCase):
    async def _run(self, decision):
        pep = PolicyEnforcementPoint()
        calls = []

        async def executor(target):
            calls.append(target)
            return "ok"

        out = await pep.enforce(decision, _request(), executor=executor)
        return out, calls

    async def test_expired_blocks(self):
        out, calls = await self._run(_expired_allow())
        self.assertTrue(out.blocked)
        self.assertIn("expired", out.blocked_reason)
        self.assertEqual(calls, [])

    async def test_failed_verification_blocks(self):
        d = _decision(verification_status="failed")
        out, calls = await self._run(d)
        self.assertTrue(out.blocked)
        self.assertEqual(out.blocked_reason, "verification failed")
        self.assertEqual(calls, [])

    async def test_missing_audit_event_blocks(self):
        d = _decision(audit_event_id="")
        out, calls = await self._run(d)
        self.assertTrue(out.blocked)
        self.assertEqual(out.blocked_reason, "missing audit_event_id")
        self.assertEqual(calls, [])


class TestObligations(unittest.IsolatedAsyncioTestCase):
    async def test_obligation_runs(self):
        pep = PolicyEnforcementPoint()
        calls = []
        pep.register_obligation(
            "log", lambda req, dec: calls.append("logged"),
        )
        d = _decision(obligations=["log"])
        out = await pep.enforce(d, _request())
        self.assertIn("log", out.obligations_run)
        self.assertEqual(calls, ["logged"])

    async def test_prefixed_obligation_dispatches(self):
        pep = PolicyEnforcementPoint()
        calls = []
        pep.register_obligation(
            "log_to", lambda req, dec: calls.append("logged"),
        )
        d = _decision(obligations=["log_to:splunk"])
        out = await pep.enforce(d, _request())
        self.assertIn("log_to:splunk", out.obligations_run)
        self.assertEqual(calls, ["logged"])

    async def test_async_obligation(self):
        pep = PolicyEnforcementPoint()
        calls = []

        async def handler(req, dec):
            calls.append("async")

        pep.register_obligation("log", handler)
        d = _decision(obligations=["log"])
        out = await pep.enforce(d, _request())
        self.assertIn("log", out.obligations_run)
        self.assertEqual(calls, ["async"])

    async def test_required_obligation_failure_blocks(self):
        pep = PolicyEnforcementPoint()
        calls = []

        def boom(req, dec):
            raise RuntimeError("disk full")

        pep.register_obligation("audit", boom, required=True)

        async def executor(target):
            calls.append(target)

        d = _decision(obligations=["audit"])
        out = await pep.enforce(d, _request(), executor=executor)
        self.assertTrue(out.blocked)
        self.assertIn("required obligation", out.blocked_reason)
        self.assertEqual(calls, [])

    async def test_optional_obligation_failure_continues(self):
        pep = PolicyEnforcementPoint()
        calls = []

        def boom(req, dec):
            raise RuntimeError("metrics down")

        pep.register_obligation("metrics", boom, required=False)

        async def executor(target):
            calls.append(target)
            return "ok"

        d = _decision(obligations=["metrics"])
        out = await pep.enforce(d, _request(), executor=executor)
        self.assertFalse(out.blocked)
        self.assertTrue(out.executed)
        self.assertEqual(len(calls), 1)
        self.assertEqual(len(out.obligations_failed), 1)
        self.assertEqual(out.obligations_failed[0]["obligation"], "metrics")

    async def test_unknown_obligation_blocks(self):
        pep = PolicyEnforcementPoint()
        d = _decision(obligations=["not_registered"])
        out = await pep.enforce(d, _request())
        self.assertTrue(out.blocked)
        self.assertIn("no handler", out.blocked_reason)

    async def test_obligation_timeout_blocks(self):
        pep = PolicyEnforcementPoint(obligation_timeout=0.01)

        async def slow(req, dec):
            await asyncio.sleep(0.5)

        pep.register_obligation("audit", slow, required=True)
        d = _decision(obligations=["audit"])
        out = await pep.enforce(d, _request())
        self.assertTrue(out.blocked)
        self.assertIn("timed out", out.blocked_reason)


class TestDispatch(unittest.IsolatedAsyncioTestCase):
    async def test_allow_calls_executor_with_request(self):
        pep = PolicyEnforcementPoint()
        got: List[Dict[str, Any]] = []

        async def executor(target):
            got.append(target)
            return "done"

        req = _request(action="deploy_model", request_id="req-1")
        out = await pep.enforce(_decision(), req, executor=executor)
        self.assertTrue(out.executed)
        self.assertEqual(out.result, "done")
        self.assertEqual(got[0]["action"], "deploy_model")

    async def test_modify_calls_executor_with_modified_request(self):
        pep = PolicyEnforcementPoint()
        got: List[Dict[str, Any]] = []

        async def executor(target):
            got.append(target)

        d = _decision(
            verdict=DecisionVerdict.MODIFY,
            modified_request={"precision": "fp32"},
        )
        await pep.enforce(d, _request(), executor=executor)
        self.assertEqual(got[0], {"precision": "fp32"})

    async def test_modify_with_empty_dict_is_used(self):
        pep = PolicyEnforcementPoint()
        got: List[Dict[str, Any]] = []

        async def executor(target):
            got.append(target)

        d = _decision(
            verdict=DecisionVerdict.MODIFY,
            modified_request={},
        )
        await pep.enforce(d, _request(), executor=executor)
        # Regression: empty dict must not fall back to request.to_dict()
        self.assertEqual(got[0], {})

    async def test_deny_blocks(self):
        pep = PolicyEnforcementPoint()
        calls: List[Any] = []
        pep.register_verdict("deny", lambda req, dec: calls.append("deny"))
        out = await pep.enforce(_decision(
            verdict=DecisionVerdict.DENY, reasons=["no"],
        ), _request())
        self.assertTrue(out.blocked)
        self.assertFalse(out.executed)
        self.assertEqual(calls, ["deny"])

    async def test_escalate_awaits_human(self):
        pep = PolicyEnforcementPoint()
        pep.register_verdict(
            "escalate",
            lambda req, dec: {"ticket": "T-1"},
        )
        d = _decision(
            verdict=DecisionVerdict.ESCALATE,
            human_approval_required=True,
        )
        out = await pep.enforce(d, _request())
        self.assertTrue(out.awaiting_human)
        self.assertEqual(out.escalation_ticket, {"ticket": "T-1"})
        self.assertFalse(out.executed)

    async def test_fallback_calls_handler(self):
        pep = PolicyEnforcementPoint()
        calls: List[Any] = []

        async def handler(req, dec, fallback_action):
            calls.append(fallback_action)

        pep.register_verdict("fallback", handler)
        d = _decision(verdict=DecisionVerdict.FALLBACK)
        out = await pep.enforce(
            d, _request(), fallback_action="degrade",
        )
        self.assertTrue(out.blocked)
        self.assertFalse(out.executed)
        self.assertEqual(calls, ["degrade"])

    async def test_allow_with_no_executor_not_blocked(self):
        pep = PolicyEnforcementPoint()
        out = await pep.enforce(_decision(), _request(), executor=None)
        self.assertFalse(out.blocked)
        self.assertFalse(out.executed)

    async def test_executor_exception_is_structured(self):
        pep = PolicyEnforcementPoint()

        async def executor(target):
            raise RuntimeError("boom")

        out = await pep.enforce(
            _decision(), _request(), executor=executor,
        )
        self.assertFalse(out.executed)
        self.assertTrue(out.blocked)
        self.assertIn("executor raised", out.error)

    async def test_executor_timeout_blocks(self):
        pep = PolicyEnforcementPoint(executor_timeout=0.01)

        async def executor(target):
            await asyncio.sleep(0.5)

        out = await pep.enforce(
            _decision(), _request(), executor=executor,
        )
        self.assertTrue(out.blocked)
        self.assertIn("timed out", out.blocked_reason)


class TestOutcomeSerialization(unittest.TestCase):
    def test_to_dict_is_json_safe(self):
        out = EnforcementOutcome(
            verdict="allow",
            executed=True,
            result={"ok": True},
            obligations_run=("log",),
        )
        payload = json.dumps(out.to_dict())
        self.assertIn("allow", payload)
        self.assertIn("log", payload)

    def test_verdict_is_string(self):
        out = EnforcementOutcome(verdict="allow")
        self.assertIsInstance(out.verdict, str)


class TestThreadSafety(unittest.IsolatedAsyncioTestCase):
    async def test_concurrent_register_and_enforce(self):
        pep = PolicyEnforcementPoint()
        stop = threading.Event()

        def registrar():
            for i in range(30):
                try:
                    pep.register_obligation(
                        f"audit-{i}",
                        lambda req, dec: None,
                        required=False,
                    )
                except KeyError:
                    pass
            stop.set()

        t = threading.Thread(target=registrar)
        t.start()
        while not stop.is_set():
            await pep.enforce(_decision(), _request())
        t.join()
        self.assertEqual(len(pep), 30)


if __name__ == "__main__":
    unittest.main(verbosity=2)
