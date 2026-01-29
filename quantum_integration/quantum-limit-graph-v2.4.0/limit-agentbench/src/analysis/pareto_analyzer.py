"""
pareto_analyzer.py

Multi-objective Pareto frontier computation for green benchmarking
and framework overhead comparison.

AgentBeats-safe:
- Deterministic
- No I/O
- No external dependencies
"""

from dataclasses import dataclass
from typing import Dict, List, Iterable


# ---------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------

@dataclass(frozen=True)
class ParetoPoint:
    """
    Represents a single evaluation result in multi-objective space.

    Conventions:
    - Lower is better (minimize):
        energy
        carbon
        latency
        memory
        framework_overhead_latency
        framework_overhead_energy
        tool_calls
        conversation_depth

    - Higher is better (maximize):
        accuracy
    """
    metrics: Dict[str, float]


# ---------------------------------------------------------------------
# Pareto frontier analyzer
# ---------------------------------------------------------------------

class ParetoFrontierAnalyzer:
    """
    Computes Pareto-optimal results across multiple objectives.
    """

    def __init__(
        self,
        minimize: Iterable[str] = None,
        maximize: Iterable[str] = None,
    ):
        # Default minimization objectives
        self.minimize = list(minimize) if minimize is not None else [
            "energy",
            "carbon",
            "latency",
            "memory",
            "framework_overhead_latency",
            "framework_overhead_energy",
            "tool_calls",
            "conversation_depth",
        ]

        # Default maximization objectives
        self.maximize = list(maximize) if maximize is not None else [
            "accuracy",
        ]

    # -----------------------------------------------------------------
    # Dominance logic
    # -----------------------------------------------------------------

    def dominates(self, a: ParetoPoint, b: ParetoPoint) -> bool:
        """
        Returns True if point `a` Pareto-dominates point `b`.

        Dominance definition:
        - a is no worse than b in all objectives
        - a is strictly better in at least one objective
        """
        strictly_better = False

        # ---- Minimize objectives ----
        for key in self.minimize:
            if key not in a.metrics or key not in b.metrics:
                continue

            if a.metrics[key] > b.metrics[key]:
                return False
            if a.metrics[key] < b.metrics[key]:
                strictly_better = True

        # ---- Maximize objectives ----
        for key in self.maximize:
            if key not in a.metrics or key not in b.metrics:
                continue

            if a.metrics[key] < b.metrics[key]:
                return False
            if a.metrics[key] > b.metrics[key]:
                strictly_better = True

        return strictly_better

    # -----------------------------------------------------------------
    # Frontier computation
    # -----------------------------------------------------------------

    def compute(self, results: List[Dict[str, float]]) -> List[Dict[str, float]]:
        """
        Computes the Pareto frontier from raw result dictionaries.

        Parameters
        ----------
        results : List[Dict[str, float]]
            List of agent evaluation result dictionaries.

        Returns
        -------
        List[Dict[str, float]]
            Pareto-optimal subset of results.
        """
        points = [ParetoPoint(r) for r in results]
        frontier: List[ParetoPoint] = []

        for candidate in points:
            dominated = False
            for other in points:
                if other is candidate:
                    continue
                if self.dominates(other, candidate):
                    dominated = True
                    break

            if not dominated:
                frontier.append(candidate)

        return [p.metrics for p in frontier]

    # -----------------------------------------------------------------
    # Backward compatibility alias
    # -----------------------------------------------------------------

    def pareto_frontier(self, results: List[Dict[str, float]]) -> List[Dict[str, float]]:
        """
        Alias for compute(), kept for compatibility with earlier code.
        """
        return self.compute(results)
