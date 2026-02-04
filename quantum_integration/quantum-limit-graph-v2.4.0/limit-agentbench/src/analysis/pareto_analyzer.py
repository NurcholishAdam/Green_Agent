"""
Policy-aware Pareto analyzer with metric provenance support.
"""

from typing import Dict, List, Optional
from src.policy.policy_engine import PolicyEngine


class ParetoAnalyzer:
    def __init__(
        self,
        metrics: List[str],
        policy_engine: Optional[PolicyEngine] = None,
    ):
        self.metrics = metrics
        self.policy_engine = policy_engine

    def dominates(self, a: Dict, b: Dict) -> bool:
        """
        Policy-aware domination check.
        """
        if self.policy_engine:
            if not self.policy_engine.check_constraints(a):
                return False
            if not self.policy_engine.check_constraints(b):
                return True

        better_or_equal = True
        strictly_better = False

        for m in self.metrics:
            va = a.get(m, float("inf"))
            vb = b.get(m, float("inf"))

            if va > vb:
                better_or_equal = False
                break
            if va < vb:
                strictly_better = True

        return better_or_equal and strictly_better

    def pareto_frontier(self, records: List[Dict]) -> List[Dict]:
        frontier = []

        for cand in records:
            dominated = False
            for other in records:
                if other is cand:
                    continue
                if self.dominates(other, cand):
                    dominated = True
                    break
            if not dominated:
                frontier.append(cand)

        # Policy-aware sorting
        if self.policy_engine:
            frontier.sort(key=self.policy_engine.weighted_score)

        return frontier
