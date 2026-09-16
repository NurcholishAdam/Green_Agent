"""
PrecisionPolicy — approved precisions, quality floors, thermal limits.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import List, Optional, Set

from ..contracts.decision_request import DecisionRequest
from ..contracts.policy_decision import PolicyDecision, DecisionVerdict
from ..contracts.risk_classification import RiskClassification

logger = logging.getLogger(__name__)


@dataclass
class PrecisionPolicy:
    """
    Rule-based precision policy.

    Enforces:
        approved_precisions: set of allowed precision labels.
        min_quality_per_precision: per-precision quality floors.
        max_thermal_c: thermal ceiling.
        max_memory_mb: memory ceiling.
    """
    approved_precisions: Set[str] = field(
        default_factory=lambda: {"fp32", "fp16", "bf16", "int8"}
    )
    min_quality_per_precision: dict = field(default_factory=dict)
    max_thermal_c: Optional[float] = None
    max_memory_mb: Optional[float] = None
    name: str = "precision"

    def evaluate(
        self, request: DecisionRequest, risk: RiskClassification,
    ) -> PolicyDecision:
        reasons: List[str] = []
        obligations: List[str] = []
        verdict = DecisionVerdict.ALLOW
        ctx = request.context or {}

        precision = ctx.get("precision")
        if precision:
            if precision not in self.approved_precisions:
                verdict = DecisionVerdict.DENY
                reasons.append(
                    f"precision '{precision}' not in approved set "
                    f"{sorted(self.approved_precisions)}"
                )
            floor = self.min_quality_per_precision.get(precision)
            if floor is not None:
                expected = ctx.get("expected_quality")
                if isinstance(expected, (int, float)) and expected < floor:
                    verdict = DecisionVerdict.DENY
                    reasons.append(
                        f"precision '{precision}' requires quality >= {floor}, "
                        f"got {expected}"
                    )

        if self.max_thermal_c is not None:
            thermal = ctx.get("thermal_c")
            if isinstance(thermal, (int, float)) and thermal > self.max_thermal_c:
                verdict = DecisionVerdict.DENY
                reasons.append(
                    f"thermal {thermal:.1f}C exceeds ceiling "
                    f"{self.max_thermal_c:.1f}C"
                )

        if self.max_memory_mb is not None:
            memory = ctx.get("memory_mb")
            if isinstance(memory, (int, float)) and memory > self.max_memory_mb:
                verdict = DecisionVerdict.DENY
                reasons.append(
                    f"memory {memory:.1f}MB exceeds ceiling "
                    f"{self.max_memory_mb:.1f}MB"
                )

        obligations.append("record_precision_choice")

        return PolicyDecision(
            request_id=request.request_id,
            verdict=verdict.value,
            policy_version="v5.0.0",
            reasons=reasons or ["precision policy passed"],
            obligations=obligations,
            risk_level=risk.level,
        )
