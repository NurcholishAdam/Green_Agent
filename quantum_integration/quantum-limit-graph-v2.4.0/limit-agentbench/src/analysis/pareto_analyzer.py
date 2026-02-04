"""
Pareto analysis for multi-objective evaluation.

This module CONTINUES the original Pareto logic and
adds support for richer metrics without breaking compatibility.
"""

from dataclasses import dataclass, asdict
from typing import List, Dict


@dataclass
class ParetoPoint:
    energy_wh: float
    carbon_kg: float
    latency_s: float
    memory_mb: float
    framework_overhead_latency: float = 0.0
    framework_overhead_energy: float = 0.0
    tool_calls: int = 0
    conversation_depth: int = 0
    accuracy: float = 0.0

    def to_dict(self) -> Dict:
        return asdict(self)


class ParetoAnalyzer:
    """
    Computes Pareto frontier across sustainability + performance metrics.
    """

    MINIMIZE_KEYS = {
        "energy_wh",
        "carbon_kg",
        "latency_s",
        "memory_mb",
        "framework_overhead_latency",
        "framework_overhead_energy",
        "tool_calls",
        "conversation_depth",
    }

    MAXIMIZE_KEYS = {"accuracy"}

    def dominates(self, a: Dict, b: Dict) -> bool:
        better_or_equal = True
        strictly_better = False

        for k in self.MINIMIZE_KEYS:
            if a.get(k, 0) > b.get(k, 0):
                better_or_equal = False
            elif a.get(k, 0) < b.get(k, 0):
                strictly_better = True

        for k in self.MAXIMIZE_KEYS:
            if a.get(k, 0) < b.get(k, 0):
                better_or_equal = False
            elif a.get(k, 0) > b.get(k, 0):
                strictly_better = True

        return better_or_equal and strictly_better

    def frontier(self, points: List[Dict]) -> List[Dict]:
        frontier = []
        for p in points:
            if not any(self.dominates(other, p) for other in points if other != p):
                frontier.append(p)
        return frontier
