"""
Pareto analysis utilities for Green Agent.

This module computes Pareto-optimal runs across
environmental, performance, and framework-overhead metrics.
"""

from typing import Dict, List


DEFAULT_METRICS = [
    "energy",
    "carbon",
    "latency",
    "memory",
    "framework_overhead_energy",
    "framework_overhead_latency",
    "tool_calls",
    "conversation_depth",
]


class ParetoAnalyzer:
    """
    Computes Pareto frontier for multi-objective optimization.

    All metrics are minimized.
    """

    def __init__(self, metrics: List[str] = None):
        self.metrics = metrics or DEFAULT_METRICS

    def _value(self, record: Dict, key: str) -> float:
        """
        Safe metric extraction with defaults.
        Missing metrics are penalized.
        """
        return float(record.get(key, float("inf")))

    def dominates(self, a: Dict, b: Dict) -> bool:
        """
        Returns True if a Pareto-dominates b.
        """
        better_or_equal = True
        strictly_better = False

        for m in self.metrics:
            va = self._value(a, m)
            vb = self._value(b, m)

            if va > vb:
                better_or_equal = False
                break
            if va < vb:
                strictly_better = True

        return better_or_equal and strictly_better

    def pareto_frontier(self, records: List[Dict]) -> List[Dict]:
        """
        Compute Pareto-optimal subset.
        """
        frontier = []

        for candidate in records:
            dominated = False
            for other in records:
                if other is candidate:
                    continue
                if self.dominates(other, candidate):
                    dominated = True
                    break

            if not dominated:
                frontier.append(candidate)

        return frontier
