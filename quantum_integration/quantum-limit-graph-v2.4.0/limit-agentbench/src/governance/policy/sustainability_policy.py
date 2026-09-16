"""
SustainabilityPolicy — quality floors, energy/carbon budgets.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, List, Optional

from ..contracts.decision_request import DecisionRequest
from ..contracts.policy_decision import PolicyDecision, DecisionVerdict
from ..contracts.risk_classification import RiskClassification

logger = logging.getLogger(__name__)


@dataclass
class SustainabilityPolicy:
    """
    Rule-based sustainability policy.

    Rules (all optional):
        quality_floor: minimum accuracy the action must maintain.
        energy_budget_wh: maximum energy per task.
        carbon_budget_g: maximum operational carbon per task.
        deny_low_quality_low_gain: block "green" actions that drop
            quality below floor even if energy improves.
    """
    quality_floor: Optional[float] = 0.0
    energy_budget_wh: Optional[float] = None
    carbon_budget_g: Optional[float] = None
    name: str = "sustainability"

    def evaluate(
        self, request: DecisionRequest, risk: RiskClassification,
    ) -> PolicyDecision:
        reasons: List[str] = []
        obligations: List[str] = []
        verdict = DecisionVerdict.ALLOW
        ctx = request.context or {}

        # --- Quality floor ---
        if self.quality_floor is not None:
            expected_quality = ctx.get("expected_quality")
            if (
                isinstance(expected_quality, (int, float))
                and expected_quality < self.quality_floor
            ):
                verdict = DecisionVerdict.DENY
                reasons.append(
                    f"expected quality {expected_quality:.3f} "
                    f"below floor {self.quality_floor:.3f}"
                )

        # --- Energy budget ---
        if self.energy_budget_wh is not None:
            energy = ctx.get("expected_energy_wh")
            if isinstance(energy, (int, float)) and energy > self.energy_budget_wh:
                verdict = DecisionVerdict.DENY
                reasons.append(
                    f"expected energy {energy:.3f} Wh exceeds "
                    f"budget {self.energy_budget_wh:.3f} Wh"
                )

        # --- Carbon budget ---
        if self.carbon_budget_g is not None:
            carbon = ctx.get("expected_carbon_g")
            if isinstance(carbon, (int, float)) and carbon > self.carbon_budget_g:
                verdict = DecisionVerdict.DENY
                reasons.append(
                    f"expected carbon {carbon:.3f} g exceeds "
                    f"budget {self.carbon_budget_g:.3f} g"
                )

        # --- Obligations ---
        obligations.append("log_energy_telemetry")
        if ctx.get("regional_baseline"):
            obligations.append("attach_regional_baseline")

        return PolicyDecision(
            request_id=request.request_id,
            verdict=verdict.value,
            policy_version="v5.0.0",
            reasons=reasons or ["sustainability policy passed"],
            obligations=obligations,
            risk_level=risk.level,
        )
