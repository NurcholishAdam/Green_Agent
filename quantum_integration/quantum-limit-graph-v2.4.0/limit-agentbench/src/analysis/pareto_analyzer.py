# src/analysis/pareto_analyzer.py

from typing import List, Dict
from .dominance_checker import dominates


class ParetoAnalyzer:
    """
    Computes Pareto frontier over multi-objective agent results.
    """

    def __init__(
        self,
        minimize=("energy", "carbon", "latency", "memory"),
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
