# src/analysis/pareto_analyzer.py

from typing import List, Dict
from .dominance_checker import dominates


class ParetoAnalyzer:
    """
    Multi-objective Pareto analyzer with framework overhead awareness.
    """

    def __init__(
        self,
        minimize=(
            "energy",
            "carbon",
            "latency",
            "memory",
            "framework_overhead_latency",
            "framework_overhead_energy",
            "tool_calls",
            "conversation_depth",
        ),
        maximize=("accuracy",),
    ):
        self.minimize = set(minimize)
        self.maximize = set(maximize)

    def pareto_frontier(self, results: List[Dict]) -> List[Dict]:
        frontier = []
        for r in results:
            dominated = False
            for other in results:
                if other is r:
                    continue
                if dominates(other, r, self.minimize, self.maximize):
                    dominated = True
                    break
            if not dominated:
                frontier.append(r)
        return frontier
