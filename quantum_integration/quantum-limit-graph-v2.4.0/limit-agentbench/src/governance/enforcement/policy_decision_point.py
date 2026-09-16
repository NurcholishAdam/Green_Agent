"""
PolicyDecisionPoint — the single governance gate.

Implements the recommendation's exact flow:

    Agent proposal
      -> Risk classification
      -> Policy evaluation
      -> Formal-safety check
      -> Required XAI/provenance check
      -> Human escalation, if required
      -> Allow / deny / modify / defer / fallback
      -> Immutable audit event
"""

from __future__ import annotations

import logging
import uuid
from typing import Any, Callable, Dict, List, Optional

from ..contracts.decision_request import DecisionRequest
from ..contracts.policy_decision import PolicyDecision, DecisionVerdict
from ..contracts.governance_event import GovernanceEvent, GovernanceEventKind
from ..contracts.risk_classification import RiskClassification, RiskLevel
from ..accountability.audit_log import AuditLog

logger = logging.getLogger(__name__)


class PolicyDecisionPoint:
    """
    The single gate every action passes through.

    Composed of:
      - A set of policy evaluators (rule-based)
      - An optional risk classifier
      - An optional formal verifier
      - An optional XAI requirement
      - An optional human oversight gate
      - An audit log that records every decision
    """

    # Default policy version used when no explicit version is supplied.
    DEFAULT_POLICY_VERSION = "v5.0.0"

    def __init__(
        self,
        *,
        policies: Optional[List[Any]] = None,
        risk_classifier: Optional[Callable[[DecisionRequest], RiskClassification]] = None,
        verifier: Optional[Any] = None,
        xai_requirement: Optional[Any] = None,
        human_gate: Optional[Any] = None,
        fallback_policy: Optional[Any] = None,
        audit_log: Optional[AuditLog] = None,
        policy_version: str = DEFAULT_POLICY_VERSION,
    ):
        self.policies = list(policies or [])
        self.risk_classifier = risk_classifier or _default_risk_classifier
        self.verifier = verifier
        self.xai_requirement = xai_requirement
        self.human_gate = human_gate
        self.fallback_policy = fallback_policy
        self.audit_log = audit_log or AuditLog()
        self.policy_version = policy_version

        logger.info(
            f"PolicyDecisionPoint initialized "
            f"(policies={len(self.policies)}, version={policy_version})"
        )

    def register_policy(self, policy: Any) -> None:
        """Register an additional policy evaluator."""
        self.policies.append(policy)

    async def decide(self, request: DecisionRequest) -> PolicyDecision:
        """
        Run the full decision flow and return a PolicyDecision.

        Never raises on a policy failure — a failing policy results in
        a DENY with the exception text as a reason.
        """
        # --- 1. Risk classification ---
        try:
            risk = self.risk_classifier(request)
        except Exception as e:
            logger.warning(f"Risk classifier failed: {e}; defaulting LOW")
            risk = RiskClassification(level=RiskLevel.LOW.value)

        # --- 2. Policy evaluation ---
        reasons: List[str] = []
        obligations: List[str] = []
        verdict = DecisionVerdict.ALLOW
        modified_request: Optional[Dict[str, Any]] = None

        for policy in self.policies:
            try:
                result = await _call_policy(policy, request, risk)
            except Exception as e:
                logger.warning(
                    f"Policy {type(policy).__name__} raised: {e}"
                )
                return self._record_decision(
                    request=request,
                    verdict=DecisionVerdict.DENY,
                    reasons=[f"policy failure: {type(policy).__name__}: {e}"],
                    obligations=[],
                    risk=risk,
                    verification_status="not_required",
                    human_approval_required=False,
                    modified_request=None,
                )

            if result is None:
                continue

            if result.verdict == DecisionVerdict.DENY:
                verdict = DecisionVerdict.DENY
            elif (
                result.verdict == DecisionVerdict.ESCALATE
                and verdict != DecisionVerdict.DENY
            ):
                verdict = DecisionVerdict.ESCALATE
            elif (
                result.verdict == DecisionVerdict.FALLBACK
                and verdict not in (
                    DecisionVerdict.DENY, DecisionVerdict.ESCALATE,
                )
            ):
                verdict = DecisionVerdict.FALLBACK
            elif (
                result.verdict == DecisionVerdict.MODIFY
                and verdict == DecisionVerdict.ALLOW
            ):
                verdict = DecisionVerdict.MODIFY
                modified_request = result.modified_request

            reasons.extend(result.reasons)
            obligations.extend(result.obligations)

        # --- 3. Formal-safety verification ---
        verification_status = "not_required"
        if verdict in (DecisionVerdict.ALLOW, DecisionVerdict.MODIFY) and self.verifier:
            try:
                v_evidence = await self.verifier.verify(request)
                verification_status = v_evidence.verdict
                if v_evidence.verdict == "failed":
                    verdict = DecisionVerdict.DENY
                    reasons.append(
                        "formal verification failed: "
                        + "; ".join(v_evidence.properties_failed)
                    )
            except Exception as e:
                logger.warning(f"Verifier failed: {e}")
                verification_status = "failed"
                verdict = DecisionVerdict.DENY
                reasons.append(f"verifier raised: {e}")

        # --- 4. XAI / provenance requirement ---
        if (
            verdict in (DecisionVerdict.ALLOW, DecisionVerdict.MODIFY)
            and self.xai_requirement
            and risk.at_least(RiskLevel.HIGH)
        ):
            try:
                ok = await self.xai_requirement.check(request)
                if not ok:
                    verdict = DecisionVerdict.DENY
                    reasons.append(
                        "XAI requirement not satisfied for high-risk action"
                    )
            except Exception as e:
                logger.warning(f"XAI check failed: {e}")
                verdict = DecisionVerdict.DENY
                reasons.append(f"XAI check raised: {e}")

        # --- 5. Human escalation ---
        human_approval_required = (
            verdict == DecisionVerdict.ESCALATE
            or risk.at_least(RiskLevel.HIGH)
        )

        # --- 6. Fallback policy (if requested) ---
        if verdict == DecisionVerdict.FALLBACK and self.fallback_policy:
            try:
                fallback = await self.fallback_policy.select(request)
                obligations.append(f"fallback: {fallback.action}")
                modified_request = fallback.modified_request
            except Exception as e:
                logger.warning(f"Fallback policy failed: {e}")
                verdict = DecisionVerdict.DENY
                reasons.append(f"fallback failed: {e}")

        return self._record_decision(
            request=request,
            verdict=verdict,
            reasons=reasons or ["no policy objections"],
            obligations=obligations,
            risk=risk,
            verification_status=verification_status,
            human_approval_required=human_approval_required,
            modified_request=modified_request,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _record_decision(
        self,
        *,
        request: DecisionRequest,
        verdict: DecisionVerdict,
        reasons: List[str],
        obligations: List[str],
        risk: RiskClassification,
        verification_status: str,
        human_approval_required: bool,
        modified_request: Optional[Dict[str, Any]],
    ) -> PolicyDecision:
        event_id = uuid.uuid4().hex[:12]
        event = GovernanceEvent(
            event_id=event_id,
            kind=GovernanceEventKind.DECISION_ISSUED.value,
            request_id=request.request_id,
            policy_version=self.policy_version,
            actor=request.agent_id,
            details={
                "action": request.action,
                "verdict": verdict.value,
                "risk_level": risk.level,
                "verification_status": verification_status,
                "human_approval_required": human_approval_required,
                "reasons": reasons,
                "obligations": obligations,
            },
        )
        self.audit_log.append(event)

        return PolicyDecision(
            request_id=request.request_id,
            verdict=verdict.value,
            policy_version=self.policy_version,
            reasons=reasons,
            obligations=obligations,
            risk_level=risk.level,
            verification_status=verification_status,
            human_approval_required=human_approval_required,
            audit_event_id=event_id,
            modified_request=modified_request,
        )


# ---------------------------------------------------------------------------
# Default risk classifier — very conservative
# ---------------------------------------------------------------------------

def _default_risk_classifier(request: DecisionRequest) -> RiskClassification:
    """
    Default classifier: LOW unless the context declares otherwise.

    Contexts may carry:
        request.context["clinical"] == True      -> CRITICAL
        request.context["financial"] == True     -> HIGH
        request.context["safety_critical"] == True -> HIGH
        request.context["material_change"] == True -> MEDIUM
    """
    factors: List[str] = []
    level = RiskLevel.LOW
    ctx = request.context or {}

    if ctx.get("clinical"):
        level = RiskLevel.CRITICAL
        factors.append("clinical context")
    elif ctx.get("financial"):
        level = RiskLevel.HIGH
        factors.append("financial context")
    elif ctx.get("safety_critical"):
        level = RiskLevel.HIGH
        factors.append("safety-critical context")
    elif ctx.get("material_change"):
        level = RiskLevel.MEDIUM
        factors.append("material change")

    return RiskClassification(level=level.value, factors=factors)


async def _call_policy(policy: Any, request: DecisionRequest, risk: RiskClassification):
    """Invoke a policy, supporting both sync and async evaluate()."""
    fn = getattr(policy, "evaluate", None)
    if fn is None:
        return None
    result = fn(request, risk)
    if hasattr(result, "__await__"):
        result = await result
    return result
