"""
budget_constrained.py

Applies hard resource constraints to agent evaluation.
"""

from typing import Dict


class BudgetConstrainedEvaluator:
    def __init__(
        self,
        agent_metrics: Dict[str, Dict[str, float]],
        budgets: Dict[str, float],
    ):
        """
        Parameters
        ----------
        agent_metrics : Dict[str, Dict[str, float]]
            agent_id -> metric_name -> value
        budgets : Dict[str, float]
            metric_name -> maximum allowed value
            Example:
            {
                "energy": 5.0,
                "co2": 200.0,
                "latency": 30.0
            }
        """
        self.agent_metrics = agent_metrics
        self.budgets = budgets

    def is_feasible(self, agent_id: str) -> bool:
        """
        Check if an agent satisfies all budget constraints.
        """
        metrics = self.agent_metrics[agent_id]

        for metric, limit in self.budgets.items():
            if metric not in metrics:
                raise KeyError(
                    f"Missing budget metric '{metric}' for agent '{agent_id}'"
                )
            if metrics[metric] > limit:
                return False

        return True

    def evaluate_all(self) -> Dict[str, bool]:
        """
        Evaluate feasibility of all agents.

        Returns
        -------
        Dict[str, bool]
            agent_id -> feasible (True/False)
        """
        return {
            agent_id: self.is_feasible(agent_id)
            for agent_id in self.agent_metrics
        }

    def compute_slack(self, agent_id: str) -> Dict[str, float]:
        """
        Compute remaining budget slack for an agent.

        Returns
        -------
        Dict[str, float]
            metric_name -> remaining budget (limit - used)
        """
        metrics = self.agent_metrics[agent_id]
        slack = {}

        for metric, limit in self.budgets.items():
            slack[metric] = limit - metrics[metric]

        return slack
