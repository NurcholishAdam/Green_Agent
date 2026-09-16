"""
RiskClassification — the risk level attached to a decision.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import List


class RiskLevel(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class RiskClassification:
    """
    Risk level with the contributing factors that produced it.
    """
    level: str = RiskLevel.LOW.value
    factors: List[str] = field(default_factory=list)

    def at_least(self, level: RiskLevel) -> bool:
        order = [
            RiskLevel.LOW.value,
            RiskLevel.MEDIUM.value,
            RiskLevel.HIGH.value,
            RiskLevel.CRITICAL.value,
        ]
        return order.index(self.level) >= order.index(level.value)
