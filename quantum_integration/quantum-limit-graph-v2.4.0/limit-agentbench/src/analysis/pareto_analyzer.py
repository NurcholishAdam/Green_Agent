"""
pareto_analyzer.py

Computes Pareto frontiers and dominance tiers for multi-objective agent evaluation.
"""

from typing import Dict, List
from dominance_checker import DominanceChecker


class ParetoAnalyzer:
    def __init__(
        self,
        agent_metrics: Dict[str, Dict[str, float]],
        objectives: Dict[str, str],
    ):
        """
        Parameters
        ----------
        agent_metrics : Dict[str, Dict[str, float]]
            Mapping agent_id -> metric_name -> value
        objectives : Dict[str, str]
            Mapping metric_name -> "min" or "max"
        """
        self.agent_metrics = agent_metrics
        self.objectives = objectives
        self.checker = DominanceChecker(objectives)

    def _is_dominated(self, agent_id: str, candidate_ids: List[str]) -> bool:
        """
        Check if agent_id is dominated by any agent in candidate_ids.
        """
        for other_id in candidate_ids:
            if other_id == agent_id:
                continue
            if self.checker.dominates(
                self.agent_metrics[other_id],
                self.agent_metrics[agent_id],
            ):
                return True
        return False

    def compute_pareto_front(self) -> List[str]:
        """
        Compute the first Pareto front (non-dominated agents).
        """
        agent_ids = list(self.agent_metrics.keys())
        front = []

        for agent_id in agent_ids:
            if not self._is_dominated(agent_id, agent_ids):
                front.append(agent_id)

        return front

    def compute_pareto_tiers(self) -> Dict[str, int]:
        """
        Compute Pareto tiers (Front 1, Front 2, ...).

        Returns
        -------
        Dict[str, int]
            agent_id -> tier number (1 = best)
        """
        remaining = set(self.agent_metrics.keys())
        tiers = {}
        tier = 1

        while remaining:
            current_front = []
            for agent_id in remaining:
                if not self._is_dominated(agent_id, list(remaining)):
                    current_front.append(agent_id)

            for agent_id in current_front:
                tiers[agent_id] = tier

            remaining -= set(current_front)
            tier += 1

        return tiers
