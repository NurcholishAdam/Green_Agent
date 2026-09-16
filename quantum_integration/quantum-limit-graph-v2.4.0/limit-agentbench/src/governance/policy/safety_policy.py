"""
SafetyPolicy — rules that block unsafe actions.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import List, Set

from ..contracts.decision_request import DecisionRequest
from ..contracts.policy_decision import PolicyDecision, DecisionVerdict
from ..contracts.risk_classification import RiskClassification

logger = logging.getLogger(__name__)


@dataclass
class SafetyPolicy:
    """
    Rule-based safety policy.

    Rules:
        forbidden_actions: set of action verbs that are always denied.
        require_verification_actions: set of actions that must pass
            formal verification.
        require_human_approval_actions: set of actions that must be
            escalated to a human.
    """
    forbidden_actions: Set[str] = field(default_factory=set)
    require_verification_actions: Set[str] = field(default_factory=set)
    require_human_approval_actions: Set[str] = field(default_factory=set)
    name: str = "safety"

    def evaluate(
        self, request: DecisionRequest, risk: RiskClassification,
    ) -> PolicyDecision:
        reasons: List[str] = []
        obligations: List[str] = []
        verdict = DecisionVerdict.ALLOW

        if request.action in self.forbidden_actions:
            verdict = DecisionVerdict.DENY
            reasons.append(f"action '{request.action}' is forbidden")

        if request.action in self.require_verification_actions:
            obligations.append("require_formal_verification")

        if request.action in self.require_human_approval_actions:
            verdict = DecisionVerdict.ESCALATE
            reasons.append(
                f"action '{request.action}' requires human approval"
            )

        return PolicyDecision(
            request_id=request.request_id,
            verdict=verdict.value,
            policy_version="v5.0.0",
            reasons=reasons or ["safety policy passed"],
            obligations=obligations,
            risk_level=risk.level,
        )
