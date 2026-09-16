"""
ShotBudget — track and enforce shot allocation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class ShotBudgetAllocation:
    """A shot allocation plan."""
    total_shots: int
    exploratory_shots: int = 0
    production_shots: int = 0
    mitigation_shots: int = 0
    reserved_shots: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total": self.total_shots,
            "exploratory": self.exploratory_shots,
            "production": self.production_shots,
            "mitigation": self.mitigation_shots,
            "reserved": self.reserved_shots,
        }


class ShotBudget:
    """
    Allocates shots across exploratory, production, and mitigation phases.

    Default split:
        exploratory = 20%
        mitigation  = 30%
        reserved    = 5%
        production  = remaining
    """

    def __init__(
        self,
        *,
        exploratory_pct: float = 0.2,
        mitigation_pct: float = 0.3,
        reserved_pct: float = 0.05,
    ):
        if abs(exploratory_pct + mitigation_pct + reserved_pct) > 1.0:
            raise ValueError("budget percentages cannot exceed 100%")
        self.exploratory_pct = exploratory_pct
        self.mitigation_pct = mitigation_pct
        self.reserved_pct = reserved_pct

    def allocate(self, total_shots: int) -> ShotBudgetAllocation:
        exploratory = int(total_shots * self.exploratory_pct)
        mitigation = int(total_shots * self.mitigation_pct)
        reserved = int(total_shots * self.reserved_pct)
        production = total_shots - exploratory - mitigation - reserved
        return ShotBudgetAllocation(
            total_shots=total_shots,
            exploratory_shots=exploratory,
            production_shots=max(0, production),
            mitigation_shots=mitigation,
            reserved_shots=reserved,
        )

    def can_proceed(self, requested: int, allocated: int) -> bool:
        return requested <= allocated
