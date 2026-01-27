"""
dominance_checker.py

Defines Pareto dominance logic for multi-objective evaluation.

A solution A dominates solution B if:
1. A is no worse than B in all objectives, and
2. A is strictly better than B in at least one objective.
"""

from typing import Dict


class DominanceChecker:
    def __init__(self, objectives: Dict[str, str]):
        """
        Parameters
        ----------
        objectives : Dict[str, str]
            Mapping of metric name to optimization direction.
            Example:
            {
                "accuracy": "max",
                "energy": "min",
                "latency": "min",
                "co2": "min"
            }
        """
        self.objectives = objectives
        self._validate_objectives()

    def _validate_objectives(self):
        for metric, direction in self.objectives.items():
            if direction not in ("min", "max"):
                raise ValueError(
                    f"Invalid objective direction for '{metric}': {direction}. "
                    "Must be 'min' or 'max'."
                )

    def dominates(self, a: Dict[str, float], b: Dict[str, float]) -> bool:
        """
        Check whether solution a Pareto-dominates solution b.

        Parameters
        ----------
        a : Dict[str, float]
            Metrics of solution A.
        b : Dict[str, float]
            Metrics of solution B.

        Returns
        -------
        bool
            True if A dominates B, False otherwise.
        """
        better_in_any = False

        for metric, direction in self.objectives.items():
            if metric not in a or metric not in b:
                raise KeyError(
                    f"Missing metric '{metric}' in comparison "
                    f"(a has: {list(a.keys())}, b has: {list(b.keys())})"
                )

            a_val = a[metric]
            b_val = b[metric]

            if direction == "max":
                if a_val < b_val:
                    return False  # worse in this objective
                elif a_val > b_val:
                    better_in_any = True

            elif direction == "min":
                if a_val > b_val:
                    return False  # worse in this objective
                elif a_val < b_val:
                    better_in_any = True

        return better_in_any


# src/analysis/dominance_checker.py

def dominates(a: dict, b: dict, minimize: set, maximize: set) -> bool:
    better_or_equal = True
    strictly_better = False

    for k in minimize:
        if a[k] > b[k]:
            better_or_equal = False
        elif a[k] < b[k]:
            strictly_better = True

    for k in maximize:
        if a[k] < b[k]:
            better_or_equal = False
        elif a[k] > b[k]:
            strictly_better = True

    return better_or_equal and strictly_better
