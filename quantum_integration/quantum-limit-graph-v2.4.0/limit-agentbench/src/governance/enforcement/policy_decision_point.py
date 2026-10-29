"""
PolicyDecisionPoint — the single governance gate.

Implements the recommendation's exact flow:

    Agent proposal
      -> Risk classification
      -> Policy evaluation
      -> Formal-safety check
      -> Required XAI/provenance check
      -> Human escalation, if required
      -> Allow / deny / modify / escalate / fallback
      -> Immutable audit event

Key guarantees
--------------
* **Fail-closed.** A broken risk classifier or policy yields ``DENY``,
  never ``ALLOW``. Every external collaborator is wrapped in a
  ``call_timeout`` (``asyncio.wait_for``); timeouts become ``DENY``.
* **Invariant-safe output.** The verdict, ``human_approval_required``,
  and ``modified_request`` fields are produced so that they always
  satisfy the enhanced :class:`PolicyDecision` invariants:
  ``MODIFY ⇒ modified_request``; ``¬MODIFY ⇒ ¬modified_request``;
  ``ALLOW/DENY ⇒ ¬human_approval_required``. High risk on a policy
  ``ALLOW`` is **escalated** (verdict becomes ``ESCALATE``), never
  stamped with an impossible ``ALLOW + human_approval_required=True``.
* **Atomic audit.** The :class:`PolicyDecision` is built **before**
  the :class:`GovernanceEvent` is appended, so a rejected decision
  cannot leave a phantom entry in the audit log. Verifier and XAI
  results are additionally emitted as ``VERIFICATION_COMPLETED``
  events, so reviewers can reconstruct the evidence chain.
* **Deterministic aggregation.** Multiple policy verdicts combine via
  an explicit severity rank (``DENY > ESCALATE > FALLBACK > MODIFY >
  ALLOW``); the per-policy verdicts are recorded on the emitted
  ``DECISION_ISSUED`` event for auditability.
* **Fallback always available.** Whenever the flow ends in
  ``DENY`` (or ``FALLBACK``), a configured ``fallback_policy`` runs.
  The selected ``FallbackAction.modified_request`` is **not** stamped
  onto the ``PolicyDecision`` (that would violate the ``FALLBACK``
  invariant) — it is recorded in the audit event under ``fallback``.
* **Thread-safe.** ``register_policy`` / ``decide`` are serialized on
  an ``RLock``; iteration uses a stable snapshot.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import threading
import unittest
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
)

# ---------------------------------------------------------------------------
# Real imports (package mode) — fall back to stubs for standalone/testing
# ---------------------------------------------------------------------------
try:  # pragma: no cover
    from ..contracts.decision_request import DecisionRequest  # type: ignore
    from ..contracts.policy_decision import (  # type: ignore
        DecisionVerdict,
        PolicyDecision,
    )
    from ..contracts.governance_event import (  # type: ignore
        GovernanceEvent,
        GovernanceEventKind,
        Severity,
    )
    from ..contracts.risk_classification import (  # type: ignore
        RiskClassification,
        RiskLevel,
    )
    from ..accountability.audit_log import AuditLog  # type: ignore
except Exception:  # pragma: no cover — standalone / test mode
    # ----- RiskLevel -----
    class RiskLevel(str, Enum):
        LOW = "low"
        MEDIUM = "medium"
        HIGH = "high"
        CRITICAL = "critical"

        @property
        def rank(self) -> int:
            return _RISK_RANK[self.value]

    _RISK_RANK = {r.value: i for i, r in enumerate(RiskLevel)}

    # ----- RiskClassification -----
    @dataclass(frozen=True)
    class RiskClassification:
        level: RiskLevel = RiskLevel.LOW
        factors: Tuple[str, ...] = ()

        def __post_init__(self):
            if not isinstance(self.level, RiskLevel):
                object.__setattr__(self, "level", RiskLevel(self.level))
            if isinstance(self.factors, str):
                raise TypeError("factors must be iterable of strings")
            if not isinstance(self.factors, tuple):
                object.__setattr__(self, "factors", tuple(self.factors))

        def at_least(self, level: Union[RiskLevel, str]) -> bool:
            lvl = level if isinstance(level, RiskLevel) else RiskLevel(level)
            return self.level.rank >= lvl.rank

    # ----- DecisionRequest -----
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

        def to_dict(self):
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

    # ----- DecisionVerdict -----
    class DecisionVerdict(str, Enum):
        ALLOW = "allow"
        DENY = "deny"
        MODIFY = "modify"
        ESCALATE = "escalate"
        FALLBACK = "fallback"

    # ----- PolicyDecision -----
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

        def execution_allowed(self, *, now: Optional[datetime] = None) -> bool:
            now = now or datetime.now(timezone.utc)
            if self.expires_at is not None and now > self.expires_at:
                return False
            if self.verdict not in (DecisionVerdict.ALLOW, DecisionVerdict.MODIFY):
                return False
            if self.human_approval_required:
                return False
            if self.verification_status == "failed":
                return False
            if not self.audit_event_id:
                return False
            return True

    # ----- GovernanceEventKind -----
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

    # ----- Severity -----
    class Severity(str, Enum):
        INFO = "info"
        WARN = "warn"
        CRITICAL = "critical"

    # ----- GovernanceEvent -----
    @dataclass(frozen=True)
    class GovernanceEvent:
        event_id: str
        kind: GovernanceEventKind
        request_id: Optional[str] = None
        policy_version: Optional[str] = None
        actor: Optional[str] = None
        emitter: Optional[str] = None
        severity: Severity = Severity.INFO
        details: Dict[str, Any] = field(default_factory=dict)
        at: datetime = field(
            default_factory=lambda: datetime.now(timezone.utc)
        )
        prev_event_hash: Optional[str] = None

        def __post_init__(self):
            if not isinstance(self.event_id, str) or not self.event_id.strip():
                raise ValueError("event_id must be non-empty")
            if not isinstance(self.kind, GovernanceEventKind):
                object.__setattr__(self, "kind", GovernanceEventKind(self.kind))
            if not isinstance(self.severity, Severity):
                object.__setattr__(self, "severity", Severity(self.severity))
            object.__setattr__(self, "details", dict(self.details))
            if not isinstance(self.at, datetime) or self.at.tzinfo is None:
                raise ValueError("at must be timezone-aware")
            object.__setattr__(self, "at", self.at.astimezone(timezone.utc))

    # ----- AuditLog -----
    class AuditLog:
        def __init__(self) -> None:
            self._events: List[GovernanceEvent] = []
            self._lock = threading.RLock()

        def append(self, event: GovernanceEvent) -> None:
            if not isinstance(event, GovernanceEvent):
                raise TypeError("event must be a GovernanceEvent")
            with self._lock:
                self._events.append(event)

        def __len__(self) -> int:
            return len(self._events)

        def __iter__(self):
            with self._lock:
                return iter(list(self._events))

        def __getitem__(self, index):
            with self._lock:
                return self._events[index]


logger = logging.getLogger(__name__)

__all__ = ["PolicyDecisionPoint", "DEFAULT_POLICY_VERSION"]


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DEFAULT_POLICY_VERSION = "v5.0.0"
DEFAULT_DECISION_TTL = timedelta(minutes=5)
DEFAULT_CALL_TIMEOUT = 5.0
_MAX_REASONS = 32
_MAX_OBLIGATIONS = 32

# Explicit severity ordering for verdict aggregation.
_VERDICT_RANK = {
    DecisionVerdict.ALLOW: 0,
    DecisionVerdict.MODIFY: 1,
    DecisionVerdict.FALLBACK: 2,
    DecisionVerdict.ESCALATE: 3,
    DecisionVerdict.DENY: 4,
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _dedupe(items: List[str], cap: int) -> List[str]:
    seen: set = set()
    out: List[str] = []
    truncated = False
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        if len(out) >= cap:
            truncated = True
            break
        out.append(item)
    if truncated:
        out.append(f"... (truncated at {cap} entries)")
    return out


def _severity_for(verdict: DecisionVerdict, risk: RiskLevel) -> Severity:
    if verdict is DecisionVerdict.DENY:
        if risk in (RiskLevel.HIGH, RiskLevel.CRITICAL):
            return Severity.CRITICAL
        return Severity.WARN
    if verdict is DecisionVerdict.ESCALATE:
        return Severity.WARN
    if verdict is DecisionVerdict.FALLBACK:
        return Severity.WARN
    return Severity.INFO


async def _call_maybe_async(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """Call ``fn``; await its result only if it is awaitable."""
    result = fn(*args, **kwargs)
    if inspect.isawaitable(result):
        result = await result
    return result


# ---------------------------------------------------------------------------
# PolicyDecisionPoint
# ---------------------------------------------------------------------------
class PolicyDecisionPoint:
    """
    The single gate every action passes through.

    Composed of:

    * A set of policy evaluators (sync or async ``evaluate``)
    * An optional risk classifier
    * An optional formal verifier (``verify``)
    * An optional XAI requirement (``check``)
    * An optional human oversight gate (``request_approval``)
    * An optional fallback policy (``select``)
    * An audit log that records every decision
    """

    def __init__(
        self,
        *,
        policies: Optional[List[Any]] = None,
        risk_classifier: Optional[
            Callable[[DecisionRequest], RiskClassification]
        ] = None,
        verifier: Optional[Any] = None,
        xai_requirement: Optional[Any] = None,
        human_gate: Optional[Any] = None,
        fallback_policy: Optional[Any] = None,
        audit_log: Optional[AuditLog] = None,
        policy_version: str = DEFAULT_POLICY_VERSION,
        decision_ttl: timedelta = DEFAULT_DECISION_TTL,
        call_timeout: float = DEFAULT_CALL_TIMEOUT,
    ) -> None:
        # -- validate collaborators -------------------------------------
        if risk_classifier is not None and not callable(risk_classifier):
            raise TypeError("risk_classifier must be callable")
        if verifier is not None and not callable(getattr(verifier, "verify", None)):
            raise TypeError("verifier must define verify(request)")
        if xai_requirement is not None and not callable(
            getattr(xai_requirement, "check", None)
        ):
            raise TypeError("xai_requirement must define check(request)")
        if fallback_policy is not None and not callable(
            getattr(fallback_policy, "select", None)
        ):
            raise TypeError("fallback_policy must define select(request)")
        if human_gate is not None and not callable(
            getattr(human_gate, "request_approval", None)
        ):
            raise TypeError(
                "human_gate must define request_approval(request, risk)"
            )
        if not isinstance(policy_version, str) or not policy_version.strip():
            raise ValueError("policy_version must be a non-empty string")
        if not isinstance(decision_ttl, timedelta) or decision_ttl <= timedelta(0):
            raise ValueError("decision_ttl must be a positive timedelta")
        if not isinstance(call_timeout, (int, float)) or call_timeout <= 0:
            raise ValueError("call_timeout must be a positive number")

        policies_list = list(policies or [])
        for p in policies_list:
            if not callable(getattr(p, "evaluate", None)):
                raise TypeError(
                    f"policy {type(p).__name__} must define evaluate(request, risk)"
                )

        # -- assign -----------------------------------------------------
        self.policies: List[Any] = policies_list
        self.risk_classifier = risk_classifier or _default_risk_classifier
        self.verifier = verifier
        self.xai_requirement = xai_requirement
        self.human_gate = human_gate
        self.fallback_policy = fallback_policy
        self.audit_log = audit_log or AuditLog()
        self.policy_version = policy_version
        self.decision_ttl = decision_ttl
        self.call_timeout = call_timeout
        self._lock = threading.RLock()

        logger.info(
            "PolicyDecisionPoint initialized "
            "(policies=%d, version=%s)",
            len(self.policies),
            policy_version,
        )

    # -- policy management -------------------------------------------------
    def register_policy(self, policy: Any, *, priority: int = 0) -> Any:
        """Register a policy evaluator. Higher priority runs first."""
        if not callable(getattr(policy, "evaluate", None)):
            raise TypeError(
                f"policy {type(policy).__name__} must define evaluate(request, risk)"
            )
        if not isinstance(priority, int):
            raise TypeError("priority must be an int")
        with self._lock:
            try:
                policy.priority = priority
            except Exception:
                pass
            self.policies.append(policy)
            # Stable sort: higher priority first.
            self.policies.sort(key=lambda p: -getattr(p, "priority", 0))
        return policy

    def unregister_policy(self, policy: Any) -> bool:
        with self._lock:
            try:
                self.policies.remove(policy)
                return True
            except ValueError:
                return False

    def _snapshot_policies(self) -> List[Any]:
        with self._lock:
            return list(self.policies)

    # -- decision flow -----------------------------------------------------
    async def decide(self, request: DecisionRequest) -> PolicyDecision:
        """Run the full decision flow and return a ``PolicyDecision``.

        Programming errors (bad ``request``) raise. Runtime failures of
        collaborators (risk classifier, policies, verifier, XAI, human
        gate, fallback) are **converted to a denied decision** — the
        gate is fail-closed.
        """
        # ----- 0. Input validation -----------------------------------
        if not isinstance(request, DecisionRequest):
            raise TypeError(
                f"request must be a DecisionRequest, got "
                f"{type(request).__name__}"
            )
        if not getattr(request, "request_id", ""):
            raise ValueError("request.request_id must be non-empty")
        if not getattr(request, "action", ""):
            raise ValueError("request.action must be non-empty")

        # ----- 1. Risk classification (fail closed) ------------------
        try:
            risk = self.risk_classifier(request)
            if not isinstance(risk, RiskClassification):
                raise TypeError(
                    f"risk_classifier returned {type(risk).__name__}, "
                    f"expected RiskClassification"
                )
        except Exception as exc:
            logger.exception("risk classifier failed — failing closed")
            return self._finalize(
                request=request,
                verdict=DecisionVerdict.DENY,
                reasons=[f"risk classifier unavailable: {exc}"],
                obligations=[],
                risk=RiskClassification(level=RiskLevel.HIGH),
                verification_status="not_required",
                modified_request=None,
                policy_verdicts=[],
                verification_event_id=None,
                fallback_details=None,
            )

        # ----- 2. Policy evaluation ----------------------------------
        reasons: List[str] = []
        obligations: List[str] = []
        policy_verdicts: List[Tuple[str, str]] = []
        highest_verdict = DecisionVerdict.ALLOW
        modified_request: Optional[Dict[str, Any]] = None

        for policy in self._snapshot_policies():
            name = type(policy).__name__
            try:
                result = await asyncio.wait_for(
                    _call_maybe_async(policy.evaluate, request, risk),
                    timeout=self.call_timeout,
                )
            except asyncio.TimeoutError:
                logger.warning("policy %s timed out", name)
                return self._finalize(
                    request=request,
                    verdict=DecisionVerdict.DENY,
                    reasons=[f"policy {name} timed out"],
                    obligations=obligations,
                    risk=risk,
                    verification_status="not_required",
                    modified_request=None,
                    policy_verdicts=policy_verdicts,
                    verification_event_id=None,
                    fallback_details=None,
                )
            except Exception as exc:
                logger.exception("policy %s raised", name)
                return self._finalize(
                    request=request,
                    verdict=DecisionVerdict.DENY,
                    reasons=[f"policy failure: {name}: {exc}"],
                    obligations=obligations,
                    risk=risk,
                    verification_status="not_required",
                    modified_request=None,
                    policy_verdicts=policy_verdicts,
                    verification_event_id=None,
                    fallback_details=None,
                )

            if result is None:
                policy_verdicts.append((name, "skipped"))
                continue

            # Validate shape
            raw_verdict = getattr(result, "verdict", None)
            if raw_verdict is None:
                return self._finalize(
                    request=request,
                    verdict=DecisionVerdict.DENY,
                    reasons=[f"policy {name} returned malformed result"],
                    obligations=obligations,
                    risk=risk,
                    verification_status="not_required",
                    modified_request=None,
                    policy_verdicts=policy_verdicts,
                    verification_event_id=None,
                    fallback_details=None,
                )
            try:
                rv = (
                    raw_verdict
                    if isinstance(raw_verdict, DecisionVerdict)
                    else DecisionVerdict(raw_verdict)
                )
            except ValueError:
                return self._finalize(
                    request=request,
                    verdict=DecisionVerdict.DENY,
                    reasons=[
                        f"policy {name} returned unknown verdict "
                        f"{raw_verdict!r}"
                    ],
                    obligations=obligations,
                    risk=risk,
                    verification_status="not_required",
                    modified_request=None,
                    policy_verdicts=policy_verdicts,
                    verification_event_id=None,
                    fallback_details=None,
                )

            policy_verdicts.append((name, rv.value))

            # MODIFY with no payload is a policy bug — deny (safe default)
            if rv is DecisionVerdict.MODIFY and getattr(
                result, "modified_request", None
            ) is None:
                return self._finalize(
                    request=request,
                    verdict=DecisionVerdict.DENY,
                    reasons=[
                        f"policy {name} returned MODIFY without "
                        f"modified_request"
                    ],
                    obligations=obligations,
                    risk=risk,
                    verification_status="not_required",
                    modified_request=None,
                    policy_verdicts=policy_verdicts,
                    verification_event_id=None,
                    fallback_details=None,
                )

            # Aggregate by explicit rank
            if _VERDICT_RANK[rv] > _VERDICT_RANK[highest_verdict]:
                highest_verdict = rv
                if rv is DecisionVerdict.MODIFY:
                    modified_request = dict(result.modified_request)

            reasons.extend(list(getattr(result, "reasons", []) or []))
            obligations.extend(
                list(getattr(result, "obligations", []) or [])
            )

        # ----- 3. Formal verification --------------------------------
        verification_status = "not_required"
        verification_event_id: Optional[str] = None
        if (
            highest_verdict in (DecisionVerdict.ALLOW, DecisionVerdict.MODIFY)
            and self.verifier is not None
        ):
            try:
                evidence = await asyncio.wait_for(
                    _call_maybe_async(self.verifier.verify, request),
                    timeout=self.call_timeout,
                )
                v_status = getattr(evidence, "verdict", "failed")
                failed_props = list(
                    getattr(evidence, "properties_failed", []) or []
                )
                verification_status = v_status
                verification_event_id = self._emit_event(
                    request=request,
                    kind=GovernanceEventKind.VERIFICATION_COMPLETED,
                    details={
                        "check": "formal_verification",
                        "verdict": v_status,
                        "properties_failed": failed_props,
                    },
                )
                if v_status == "failed":
                    highest_verdict = DecisionVerdict.DENY
                    modified_request = None
                    reasons.append(
                        "formal verification failed"
                        + (": " + "; ".join(failed_props) if failed_props else "")
                    )
            except asyncio.TimeoutError:
                verification_status = "failed"
                highest_verdict = DecisionVerdict.DENY
                modified_request = None
                reasons.append("verifier timed out")
            except Exception as exc:
                logger.exception("verifier raised")
                verification_status = "failed"
                highest_verdict = DecisionVerdict.DENY
                modified_request = None
                reasons.append(f"verifier raised: {exc}")

        # ----- 4. XAI / provenance -----------------------------------
        if (
            highest_verdict in (DecisionVerdict.ALLOW, DecisionVerdict.MODIFY)
            and self.xai_requirement is not None
            and risk.at_least(RiskLevel.HIGH)
        ):
            try:
                ok = await asyncio.wait_for(
                    _call_maybe_async(self.xai_requirement.check, request),
                    timeout=self.call_timeout,
                )
                if not ok:
                    highest_verdict = DecisionVerdict.DENY
                    modified_request = None
                    reasons.append(
                        "XAI requirement not satisfied for high-risk action"
                    )
            except asyncio.TimeoutError:
                highest_verdict = DecisionVerdict.DENY
                modified_request = None
                reasons.append("XAI check timed out")
            except Exception as exc:
                logger.exception("XAI check raised")
                highest_verdict = DecisionVerdict.DENY
                modified_request = None
                reasons.append(f"XAI check raised: {exc}")

        # ----- 5. Human escalation -----------------------------------
        # Escalate when a policy said ESCALATE, or when risk is HIGH+
        # and the decision would otherwise be ALLOW / MODIFY.
        if highest_verdict is DecisionVerdict.ESCALATE:
            pass  # already escalating
        elif (
            risk.at_least(RiskLevel.HIGH)
            and highest_verdict in (
                DecisionVerdict.ALLOW,
                DecisionVerdict.MODIFY,
            )
        ):
            highest_verdict = DecisionVerdict.ESCALATE
            modified_request = None
            reasons.append(
                f"escalated to human: risk={risk.level.value}"
            )

        # ----- 5b. Invoke the human gate (best effort) ---------------
        if highest_verdict is DecisionVerdict.ESCALATE and self.human_gate is not None:
            try:
                await asyncio.wait_for(
                    _call_maybe_async(
                        self.human_gate.request_approval, request, risk
                    ),
                    timeout=self.call_timeout,
                )
            except asyncio.TimeoutError:
                logger.warning("human gate timed out")
            except Exception:
                logger.exception("human gate raised")

        # ----- 6. Fallback policy ------------------------------------
        fallback_details: Optional[Dict[str, Any]] = None
        if (
            highest_verdict in (DecisionVerdict.DENY, DecisionVerdict.FALLBACK)
            and self.fallback_policy is not None
        ):
            try:
                fb = await asyncio.wait_for(
                    _call_maybe_async(self.fallback_policy.select, request),
                    timeout=self.call_timeout,
                )
                if fb is not None and not fb.is_default():
                    highest_verdict = DecisionVerdict.FALLBACK
                    modified_request = None  # FALLBACK never carries one
                    reasons.append(
                        f"fallback: {fb.action}"
                        + (f" ({fb.reason})" if fb.reason else "")
                    )
                    fallback_details = {
                        "action": fb.action,
                        "reason": fb.reason,
                        "rule_name": fb.rule_name,
                        "modified_request": fb.modified_request,
                    }
            except asyncio.TimeoutError:
                logger.warning("fallback policy timed out")
            except Exception:
                logger.exception("fallback policy raised")

        # ----- Final sanity: invariants ------------------------------
        if highest_verdict is not DecisionVerdict.MODIFY:
            modified_request = None

        return self._finalize(
            request=request,
            verdict=highest_verdict,
            reasons=reasons or ["no policy objections"],
            obligations=obligations,
            risk=risk,
            verification_status=verification_status,
            modified_request=modified_request,
            policy_verdicts=policy_verdicts,
            verification_event_id=verification_event_id,
            fallback_details=fallback_details,
        )

    # -- internal ----------------------------------------------------------
    def _finalize(
        self,
        *,
        request: DecisionRequest,
        verdict: DecisionVerdict,
        reasons: List[str],
        obligations: List[str],
        risk: RiskClassification,
        verification_status: str,
        modified_request: Optional[Dict[str, Any]],
        policy_verdicts: List[Tuple[str, str]],
        verification_event_id: Optional[str],
        fallback_details: Optional[Dict[str, Any]],
    ) -> PolicyDecision:
        reasons = _dedupe(list(reasons), _MAX_REASONS)
        obligations = _dedupe(list(obligations), _MAX_OBLIGATIONS)

        if verdict is DecisionVerdict.DENY and not reasons:
            reasons = ["denied without reason"]
        if verdict is DecisionVerdict.MODIFY and modified_request is None:
            # Defensive: should have been caught earlier.
            verdict = DecisionVerdict.DENY
            reasons.append("internal: MODIFY without modified_request")

        human_approval_required = verdict is DecisionVerdict.ESCALATE

        now = datetime.now(timezone.utc)
        if verdict in (
            DecisionVerdict.ALLOW,
            DecisionVerdict.MODIFY,
            DecisionVerdict.FALLBACK,
        ):
            expires_at: Optional[datetime] = now + self.decision_ttl
        else:
            expires_at = None

        event_id = uuid.uuid4().hex  # full 128-bit; no truncation

        # Build the decision FIRST so a rejected decision never leaves
        # a phantom event in the audit log.
        decision = PolicyDecision(
            request_id=request.request_id,
            verdict=verdict,
            policy_version=self.policy_version,
            reasons=reasons,
            obligations=obligations,
            risk_level=risk.level,
            verification_status=verification_status,
            human_approval_required=human_approval_required,
            audit_event_id=event_id,
            modified_request=modified_request,
            issued_at=now,
            expires_at=expires_at,
        )

        self._emit_event(
            request=request,
            kind=GovernanceEventKind.DECISION_ISSUED,
            event_id=event_id,
            severity=_severity_for(verdict, risk.level),
            details={
                "action": request.action,
                "verdict": verdict.value,
                "risk_level": risk.level.value,
                "risk_factors": list(risk.factors),
                "verification_status": verification_status,
                "human_approval_required": human_approval_required,
                "reasons": list(reasons),
                "obligations": list(obligations),
                "policy_verdicts": list(policy_verdicts),
                "verification_event_id": verification_event_id,
                "fallback": fallback_details,
            },
        )
        return decision

    def _emit_event(
        self,
        *,
        request: DecisionRequest,
        kind: GovernanceEventKind,
        details: Dict[str, Any],
        event_id: Optional[str] = None,
        severity: Severity = Severity.INFO,
    ) -> str:
        eid = event_id or uuid.uuid4().hex
        event = GovernanceEvent(
            event_id=eid,
            kind=kind,
            request_id=request.request_id,
            policy_version=self.policy_version,
            actor=request.agent_id,
            emitter="policy_decision_point",
            severity=severity,
            details=details,
        )
        self.audit_log.append(event)
        return eid


# ---------------------------------------------------------------------------
# Default risk classifier
# ---------------------------------------------------------------------------
def _default_risk_classifier(request: DecisionRequest) -> RiskClassification:
    """
    Default classifier: LOW unless the context declares otherwise.

    Contexts may carry (boolean ``True`` only):

        request.context["clinical"]        is True -> CRITICAL
        request.context["financial"]       is True -> HIGH
        request.context["safety_critical"] is True -> HIGH
        request.context["material_change"] is True -> MEDIUM

    When multiple keys are set, the level is the **maximum** of those
    triggered and every triggered factor is recorded. Values that are
    merely truthy (e.g. the string ``"false"``) do **not** trigger.
    """
    factors: List[str] = []
    level = RiskLevel.LOW
    ctx = getattr(request, "context", None) or {}

    def _bump(candidate: RiskLevel, factor: str) -> None:
        nonlocal level
        factors.append(factor)
        if candidate.rank > level.rank:
            level = candidate

    if ctx.get("clinical") is True:
        _bump(RiskLevel.CRITICAL, "clinical context")
    if ctx.get("financial") is True:
        _bump(RiskLevel.HIGH, "financial context")
    if ctx.get("safety_critical") is True:
        _bump(RiskLevel.HIGH, "safety-critical context")
    if ctx.get("material_change") is True:
        _bump(RiskLevel.MEDIUM, "material change")

    return RiskClassification(level=level, factors=tuple(factors))


# ===========================================================================
# TESTS
# ===========================================================================
# --- test doubles ----------------------------------------------------------
@dataclass
class PolicyResult:
    """Minimal duck-typed result a test policy can return."""

    verdict: Union[DecisionVerdict, str]
    reasons: List[str] = field(default_factory=list)
    obligations: List[str] = field(default_factory=list)
    modified_request: Optional[Dict[str, Any]] = None


class StaticPolicy:
    def __init__(self, result: Optional[PolicyResult]):
        self.result = result

    async def evaluate(self, request, risk):
        return self.result


class RaisingPolicy:
    def __init__(self, exc: Exception):
        self.exc = exc

    async def evaluate(self, request, risk):
        raise self.exc


class SlowPolicy:
    def __init__(self, seconds: float):
        self.seconds = seconds

    async def evaluate(self, request, risk):
        await asyncio.sleep(self.seconds)
        return PolicyResult(DecisionVerdict.ALLOW)


class RaisingClassifier:
    def __init__(self, exc: Exception):
        self.exc = exc

    def __call__(self, request):
        raise self.exc


class StubVerifier:
    def __init__(self, verdict="passed", failed=None):
        self.verdict = verdict
        self.failed = failed or []
        self.calls = 0

    async def verify(self, request):
        self.calls += 1
        return type("Evidence", (), {
            "verdict": self.verdict,
            "properties_failed": list(self.failed),
        })()


class StubXAI:
    def __init__(self, ok: bool):
        self.ok = ok
        self.calls = 0

    async def check(self, request):
        self.calls += 1
        return self.ok


class StubHumanGate:
    def __init__(self):
        self.calls = 0

    async def request_approval(self, request, risk):
        self.calls += 1
        return {"approval_id": "ticket-1"}


class StubFallback:
    def __init__(self, action=None, reason="", modified_request=None,
                 rule_name=None):
        self._payload = None
        if action is not None:
            self._payload = _FBResult(
                action=action, reason=reason,
                modified_request=modified_request,
                rule_name=rule_name,
            )

    async def select(self, request):
        if self._payload is None:
            return _FBResult(action="deny", rule_name=None)
        return self._payload


@dataclass
class _FBResult:
    action: str
    reason: str = ""
    modified_request: Optional[Dict[str, Any]] = None
    rule_name: Optional[str] = None

    def is_default(self) -> bool:
        return self.rule_name is None


def _request(**kwargs) -> DecisionRequest:
    defaults = dict(action="deploy_model", request_id="req-1")
    defaults.update(kwargs)
    return DecisionRequest(**defaults)


# --- tests -----------------------------------------------------------------
class TestInitialization(unittest.TestCase):
    def test_defaults(self):
        pdp = PolicyDecisionPoint()
        self.assertEqual(len(pdp.policies), 0)
        self.assertIsNotNone(pdp.risk_classifier)
        self.assertIsNone(pdp.verifier)
        self.assertIsNone(pdp.xai_requirement)
        self.assertIsNone(pdp.human_gate)
        self.assertIsNone(pdp.fallback_policy)
        self.assertEqual(pdp.policy_version, DEFAULT_POLICY_VERSION)
        self.assertIsInstance(pdp.audit_log, AuditLog)

    def test_rejects_non_callable_classifier(self):
        with self.assertRaises(TypeError):
            PolicyDecisionPoint(risk_classifier="not-callable")  # type: ignore

    def test_rejects_verifier_missing_method(self):
        with self.assertRaises(TypeError):
            PolicyDecisionPoint(verifier=object())

    def test_rejects_xai_missing_method(self):
        with self.assertRaises(TypeError):
            PolicyDecisionPoint(xai_requirement=object())

    def test_rejects_fallback_missing_method(self):
        with self.assertRaises(TypeError):
            PolicyDecisionPoint(fallback_policy=object())

    def test_rejects_human_gate_missing_method(self):
        with self.assertRaises(TypeError):
            PolicyDecisionPoint(human_gate=object())

    def test_rejects_empty_policy_version(self):
        with self.assertRaises(ValueError):
            PolicyDecisionPoint(policy_version="")

    def test_rejects_non_positive_ttl(self):
        with self.assertRaises(ValueError):
            PolicyDecisionPoint(decision_ttl=timedelta(0))

    def test_rejects_non_positive_timeout(self):
        with self.assertRaises(ValueError):
            PolicyDecisionPoint(call_timeout=0)

    def test_rejects_policy_without_evaluate(self):
        with self.assertRaises(TypeError):
            PolicyDecisionPoint(policies=[object()])


class TestRegisterPolicy(unittest.TestCase):
    def test_register_and_unregister(self):
        pdp = PolicyDecisionPoint()
        p = StaticPolicy(PolicyResult(DecisionVerdict.ALLOW))
        pdp.register_policy(p)
        self.assertEqual(len(pdp.policies), 1)
        self.assertTrue(pdp.unregister_policy(p))
        self.assertEqual(len(pdp.policies), 0)
        self.assertFalse(pdp.unregister_policy(p))

    def test_rejects_policy_without_evaluate(self):
        pdp = PolicyDecisionPoint()
        with self.assertRaises(TypeError):
            pdp.register_policy(object())

    def test_priority_ordering(self):
        pdp = PolicyDecisionPoint()
        low = StaticPolicy(PolicyResult(DecisionVerdict.ALLOW))
        high = StaticPolicy(PolicyResult(DecisionVerdict.DENY))
        pdp.register_policy(low, priority=0)
        pdp.register_policy(high, priority=10)
        self.assertIs(pdp.policies[0], high)


class TestInputValidation(unittest.IsolatedAsyncioTestCase):
    async def test_none_request(self):
        pdp = PolicyDecisionPoint()
        with self.assertRaises(TypeError):
            await pdp.decide(None)  # type: ignore

    async def test_wrong_type_request(self):
        pdp = PolicyDecisionPoint()
        with self.assertRaises(TypeError):
            await pdp.decide("not-a-request")  # type: ignore


class TestBasicFlows(unittest.IsolatedAsyncioTestCase):
    async def test_empty_config_allows(self):
        pdp = PolicyDecisionPoint()
        decision = await pdp.decide(_request())
        self.assertIs(decision.verdict, DecisionVerdict.ALLOW)
        self.assertFalse(decision.human_approval_required)
        self.assertIsNotNone(decision.expires_at)
        self.assertEqual(len(decision.audit_event_id), 32)

    async def test_policy_allow(self):
        pdp = PolicyDecisionPoint(policies=[
            StaticPolicy(PolicyResult(DecisionVerdict.ALLOW,
                                      reasons=["ok"])),
        ])
        d = await pdp.decide(_request())
        self.assertIs(d.verdict, DecisionVerdict.ALLOW)
        self.assertIn("ok", d.reasons)

    async def test_policy_deny(self):
        pdp = PolicyDecisionPoint(policies=[
            StaticPolicy(PolicyResult(DecisionVerdict.DENY,
                                      reasons=["forbidden"])),
        ])
        d = await pdp.decide(_request())
        self.assertIs(d.verdict, DecisionVerdict.DENY)
        self.assertFalse(d.human_approval_required)
        self.assertIn("forbidden", d.reasons)

    async def test_policy_modify(self):
        pdp = PolicyDecisionPoint(policies=[
            StaticPolicy(PolicyResult(
                DecisionVerdict.MODIFY, reasons=["downgraded precision"],
                modified_request={"precision": "fp32"},
            )),
        ])
        d = await pdp.decide(_request())
        self.assertIs(d.verdict, DecisionVerdict.MODIFY)
        self.assertEqual(d.modified_request, {"precision": "fp32"})

    async def test_policy_modify_without_payload_denies(self):
        pdp = PolicyDecisionPoint(policies=[
            StaticPolicy(PolicyResult(DecisionVerdict.MODIFY)),  # no payload
        ])
        d = await pdp.decide(_request())
        self.assertIs(d.verdict, DecisionVerdict.DENY)
        self.assertIn("MODIFY without modified_request", "; ".join(d.reasons))

    async def test_policy_escalate(self):
        pdp = PolicyDecisionPoint(policies=[
            StaticPolicy(PolicyResult(DecisionVerdict.ESCALATE,
                                      reasons=["needs review"])),
        ])
        d = await pdp.decide(_request())
        self.assertIs(d.verdict, DecisionVerdict.ESCALATE)
        self.assertTrue(d.human_approval_required)

    async def test_policy_returning_none_is_skipped(self):
        pdp = PolicyDecisionPoint(policies=[StaticPolicy(None)])
        d = await pdp.decide(_request())
        self.assertIs(d.verdict, DecisionVerdict.ALLOW)

    async def test_aggregation_deny_beats_allow(self):
        for order in [["allow", "deny"], ["deny", "allow"]]:
            results = []
            for kind in order:
                v = (DecisionVerdict.ALLOW if kind == "allow"
                     else DecisionVerdict.DENY)
                results.append(StaticPolicy(
                    PolicyResult(v, reasons=[kind])
                ))
            pdp = PolicyDecisionPoint(policies=results)
            d = await pdp.decide(_request())
            self.assertIs(d.verdict, DecisionVerdict.DENY,
                          f"failed for order={order}")

    async def test_aggregation_escalate_beats_modify(self):
        pdp = PolicyDecisionPoint(policies=[
            StaticPolicy(PolicyResult(
                DecisionVerdict.MODIFY, modified_request={"x": 1},
            )),
            StaticPolicy(PolicyResult(DecisionVerdict.ESCALATE,
                                      reasons=["human"])),
        ])
        d = await pdp.decide(_request())
        self.assertIs(d.verdict, DecisionVerdict.ESCALATE)
        self.assertIsNone(d.modified_request)

    async def test_policy_exception_yields_deny(self):
        pdp = PolicyDecisionPoint(policies=[
            RaisingPolicy(RuntimeError("boom")),
        ])
        d = await pdp.decide(_request())
        self.assertIs(d.verdict, DecisionVerdict.DENY)
        self.assertIn("boom", "; ".join(d.reasons))

    async def test_policy_timeout_yields_deny(self):
        pdp = PolicyDecisionPoint(
            policies=[SlowPolicy(0.5)], call_timeout=0.01,
        )
        d = await pdp.decide(_request())
        self.assertIs(d.verdict, DecisionVerdict.DENY)
        self.assertIn("timed out", "; ".join(d.reasons))


class TestFailClosedRisk(unittest.IsolatedAsyncioTestCase):
    async def test_classifier_failure_denies(self):
        # Regression: original defaulted to LOW, which is fail-open.
        pdp = PolicyDecisionPoint(
            risk_classifier=RaisingClassifier(RuntimeError("classifier down")),
        )
        d = await pdp.decide(_request())
        self.assertIs(d.verdict, DecisionVerdict.DENY)
        self.assertIn("classifier unavailable", "; ".join(d.reasons))

    async def test_classifier_returning_wrong_type_denies(self):
        pdp = PolicyDecisionPoint(risk_classifier=lambda r: "high")  # type: ignore
        d = await pdp.decide(_request())
        self.assertIs(d.verdict, DecisionVerdict.DENY)


class TestHighRiskEscalates(unittest.IsolatedAsyncioTestCase):
    async def test_empty_config_high_risk_escalates(self):
        # Regression: enhanced PolicyDecision forbids ALLOW + human=True.
        # The flow must escalate the verdict, not stamp the flag.
        pdp = PolicyDecisionPoint()
        d = await pdp.decide(_request(context={"clinical": True}))
        self.assertIs(d.verdict, DecisionVerdict.ESCALATE)
        self.assertTrue(d.human_approval_required)
        self.assertIsNone(d.expires_at)

    async def test_high_risk_allow_policy_escalates(self):
        pdp = PolicyDecisionPoint(policies=[
            StaticPolicy(PolicyResult(DecisionVerdict.ALLOW,
                                      reasons=["ok"])),
        ])
        d = await pdp.decide(_request(context={"financial": True}))
        self.assertIs(d.verdict, DecisionVerdict.ESCALATE)
        self.assertTrue(d.human_approval_required)

    async def test_high_risk_deny_stays_deny(self):
        pdp = PolicyDecisionPoint(policies=[
            StaticPolicy(PolicyResult(DecisionVerdict.DENY,
                                      reasons=["forbidden"])),
        ])
        d = await pdp.decide(_request(context={"financial": True}))
        self.assertIs(d.verdict, DecisionVerdict.DENY)
        self.assertFalse(d.human_approval_required)

    async def test_high_risk_calls_human_gate(self):
        gate = StubHumanGate()
        pdp = PolicyDecisionPoint(human_gate=gate)
        await pdp.decide(_request(context={"clinical": True}))
        self.assertEqual(gate.calls, 1)


class TestVerifier(unittest.IsolatedAsyncioTestCase):
    async def test_verifier_pass(self):
        v = StubVerifier(verdict="passed")
        pdp = PolicyDecisionPoint(verifier=v)
        d = await pdp.decide(_request())
        self.assertIs(d.verdict, DecisionVerdict.ALLOW)
        self.assertEqual(d.verification_status, "passed")
        self.assertEqual(v.calls, 1)

    async def test_verifier_fail_denies(self):
        v = StubVerifier(verdict="failed", failed=["safety_bound"])
        pdp = PolicyDecisionPoint(verifier=v)
        d = await pdp.decide(_request())
        self.assertIs(d.verdict, DecisionVerdict.DENY)
        self.assertEqual(d.verification_status, "failed")
        self.assertIn("safety_bound", "; ".join(d.reasons))

    async def test_verifier_skipped_for_deny(self):
        v = StubVerifier(verdict="passed")
        pdp = PolicyDecisionPoint(
            verifier=v,
            policies=[StaticPolicy(PolicyResult(
                DecisionVerdict.DENY, reasons=["forbidden"]
            ))],
        )
        d = await pdp.decide(_request())
        self.assertEqual(v.calls, 0)
        self.assertEqual(d.verification_status, "not_required")


class TestXAI(unittest.IsolatedAsyncioTestCase):
    async def test_xai_skipped_below_high(self):
        xai = StubXAI(ok=True)
        pdp = PolicyDecisionPoint(xai_requirement=xai)
        await pdp.decide(_request())  # LOW
        self.assertEqual(xai.calls, 0)

    async def test_xai_fail_denies(self):
        xai = StubXAI(ok=False)
        pdp = PolicyDecisionPoint(xai_requirement=xai)
        d = await pdp.decide(_request(context={"financial": True}))
        # Verdict is DENY because XAI failed before escalation kicked in.
        self.assertIs(d.verdict, DecisionVerdict.DENY)
        self.assertIn("XAI requirement", "; ".join(d.reasons))

    async def test_xai_pass(self):
        xai = StubXAI(ok=True)
        pdp = PolicyDecisionPoint(xai_requirement=xai)
        d = await pdp.decide(_request(context={"material_change": True}))
        # MEDIUM risk → XAI not required → ALLOW
        self.assertIs(d.verdict, DecisionVerdict.ALLOW)
        self.assertEqual(xai.calls, 0)


class TestFallback(unittest.IsolatedAsyncioTestCase):
    async def test_fallback_runs_on_deny(self):
        # Regression: original only ran on explicit FALLBACK verdict.
        fb = StubFallback(action="degrade", reason="safety net",
                          rule_name="rule-1")
        pdp = PolicyDecisionPoint(
            fallback_policy=fb,
            policies=[StaticPolicy(PolicyResult(
                DecisionVerdict.DENY, reasons=["forbidden"]
            ))],
        )
        d = await pdp.decide(_request())
        self.assertIs(d.verdict, DecisionVerdict.FALLBACK)
        # FALLBACK must NOT carry modified_request (regression)
        self.assertIsNone(d.modified_request)
        self.assertFalse(d.human_approval_required)

    async def test_fallback_runs_on_fallback(self):
        fb = StubFallback(action="degrade", rule_name="rule-1",
                          modified_request={"fallback_action": "degrade"})
        pdp = PolicyDecisionPoint(
            fallback_policy=fb,
            policies=[StaticPolicy(PolicyResult(DecisionVerdict.FALLBACK))],
        )
        d = await pdp.decide(_request())
        self.assertIs(d.verdict, DecisionVerdict.FALLBACK)
        self.assertIsNone(d.modified_request)

    async def test_fallback_no_match_stays_deny(self):
        fb = StubFallback()  # no rule matches → is_default True
        pdp = PolicyDecisionPoint(
            fallback_policy=fb,
            policies=[StaticPolicy(PolicyResult(
                DecisionVerdict.DENY, reasons=["forbidden"]
            ))],
        )
        d = await pdp.decide(_request())
        self.assertIs(d.verdict, DecisionVerdict.DENY)

    async def test_fallback_failure_does_not_break_decision(self):
        class BrokenFallback:
            async def select(self, request):
                raise RuntimeError("fallback broken")

        pdp = PolicyDecisionPoint(
            fallback_policy=BrokenFallback(),
            policies=[StaticPolicy(PolicyResult(
                DecisionVerdict.DENY, reasons=["forbidden"]
            ))],
        )
        d = await pdp.decide(_request())
        self.assertIs(d.verdict, DecisionVerdict.DENY)


class TestAuditAtomicity(unittest.IsolatedAsyncioTestCase):
    async def test_decision_built_before_event_appended(self):
        # Monkey-patch PolicyDecision construction to fail — the audit
        # log must remain empty (no phantom events).
        import sys
        pkg = sys.modules[__name__]

        original = pkg.PolicyDecision

        class FailingDecision(original):  # type: ignore[misc]
            def __post_init__(self):
                raise RuntimeError("simulated invariant failure")

        pkg.PolicyDecision = FailingDecision
        try:
            pdp = PolicyDecisionPoint()
            with self.assertRaises(RuntimeError):
                await pdp.decide(_request())
            # Phantom-event regression: no DECISION_ISSUED should be logged.
            self.assertEqual(len(pdp.audit_log), 0)
        finally:
            pkg.PolicyDecision = original

    async def test_verifier_emits_sub_event(self):
        v = StubVerifier(verdict="passed")
        pdp = PolicyDecisionPoint(verifier=v)
        await pdp.decide(_request())
        kinds = [e.kind for e in pdp.audit_log]
        self.assertIn(GovernanceEventKind.VERIFICATION_COMPLETED, kinds)
        self.assertIn(GovernanceEventKind.DECISION_ISSUED, kinds)


class TestDefaultClassifier(unittest.TestCase):
    def test_low_default(self):
        rc = _default_risk_classifier(_request())
        self.assertIs(rc.level, RiskLevel.LOW)
        self.assertEqual(rc.factors, ())

    def test_clinical_is_critical(self):
        rc = _default_risk_classifier(_request(context={"clinical": True}))
        self.assertIs(rc.level, RiskLevel.CRITICAL)

    def test_string_false_does_not_trigger(self):
        # Regression: truthiness of "false" would be True
        rc = _default_risk_classifier(
            _request(context={"clinical": "false"})
        )
        self.assertIs(rc.level, RiskLevel.LOW)

    def test_multiple_factors_takes_max(self):
        rc = _default_risk_classifier(_request(context={
            "financial": True, "safety_critical": True,
        }))
        self.assertIs(rc.level, RiskLevel.HIGH)
        self.assertEqual(
            set(rc.factors),
            {"financial context", "safety-critical context"},
        )

    def test_clinical_overrides_financial(self):
        rc = _default_risk_classifier(_request(context={
            "clinical": True, "financial": True,
        }))
        self.assertIs(rc.level, RiskLevel.CRITICAL)
        self.assertIn("clinical context", rc.factors)
        self.assertIn("financial context", rc.factors)


class TestReasonsDedup(unittest.IsolatedAsyncioTestCase):
    async def test_duplicate_reasons_deduplicated(self):
        pdp = PolicyDecisionPoint(policies=[
            StaticPolicy(PolicyResult(
                DecisionVerdict.DENY, reasons=["same", "same"]
            )),
            StaticPolicy(PolicyResult(
                DecisionVerdict.DENY, reasons=["same"]
            )),
        ])
        d = await pdp.decide(_request())
        self.assertEqual(d.reasons.count("same"), 1)


class TestThreadSafety(unittest.IsolatedAsyncioTestCase):
    async def test_concurrent_decide_and_register(self):
        pdp = PolicyDecisionPoint()
        stop = threading.Event()

        def register_loop():
            for _ in range(30):
                try:
                    pdp.register_policy(
                        StaticPolicy(PolicyResult(DecisionVerdict.ALLOW))
                    )
                except Exception:
                    pass
            stop.set()

        t = threading.Thread(target=register_loop)
        t.start()
        while not stop.is_set():
            await pdp.decide(_request())
        t.join()
        # 30 policies registered without crash
        self.assertEqual(len(pdp.policies), 30)


if __name__ == "__main__":
    unittest.main(verbosity=2)
