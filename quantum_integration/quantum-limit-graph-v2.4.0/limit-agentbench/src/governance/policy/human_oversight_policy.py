"""
HumanOversightPolicy — decide when human approval is required.
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
class HumanOversightPolicy:
    """
    Governs when human approval is required.

    Rules:
        require_approval_for_actions: actions that always escalate.
        require_approval_above_risk: risk levels that always escalate.
        dual_control_actions: actions requiring two humans.
    """
    require_approval_for_actions: Set[str] = field(default_factory=set)
    require_approval_above_risk: str = "high"
    dual_control_actions: Set[str] = field(default_factory=set)
    name: str = "human_oversight"

    def evaluate(
        self, request: DecisionRequest, risk: RiskClassification,
    ) -> PolicyDecision:
        reasons: List[str] = []
        obligations: List[str] = []
        verdict = DecisionVerdict.ALLOW

        if request.action in self.require_approval_for_actions:
            verdict = DecisionVerdict.ESCALATE
            reasons.append(
                f"action '{request.action}' always requires human approval"
            )

        risk_order = ["low", "medium", "high", "critical"]
        threshold_index = risk_order.index(self.require_approval_above_risk)
        current_index = risk_order.index(risk.level)
        if current_index >= threshold_index:
            verdict = DecisionVerdict.ESCALATE
            reasons.append(
                f"risk level '{risk.level}' requires human approval"
            )

        if request.action in self.dual_control_actions:
            obligations.append("dual_control")

        return PolicyDecision(
            request_id=request.request_id,
            verdict=verdict.value,
            policy_version="v5.0.0",
            reasons=reasons or ["human oversight policy passed"],
            obligations=obligations,
            risk_level=risk.level,
            human_approval_required=(verdict == DecisionVerdict.ESCALATE),
        )
