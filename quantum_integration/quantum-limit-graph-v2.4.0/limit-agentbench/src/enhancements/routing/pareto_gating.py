"""
Pareto Gating Module
====================
Filters infeasible actions via hard constraints and returns Pareto‑optimal options.
Enhanced with dynamic constraints, configurable objectives, and scalar scoring.
"""
import logging
from typing import List, Dict, Any, Optional, Tuple, Callable
import numpy as np

# Import config (adjust path as needed)
from ..config import config
from ..logger import logger

# Default objective configuration:
# Each objective is a dict with 'key' (field name), 'direction' ('max' or 'min').
# The order matters for dominance check.
DEFAULT_OBJECTIVES = [
    {"key": "quality_score", "direction": "max"},
    {"key": "latency_ms", "direction": "min"},
    {"key": "carbon_g", "direction": "min"},
    {"key": "energy_joules", "direction": "min"},
    {"key": "helium_cost", "direction": "min"},
    {"key": "resource_usage", "direction": "min"},  # optional
]


class ParetoGating:
    """
    Ensures hard constraints are met and returns Pareto‑optimal options.
    Supports dynamic constraints and configurable objectives.
    """

    def __init__(
        self,
        constraints: Optional[Dict[str, Any]] = None,
        objectives: Optional[List[Dict[str, str]]] = None,
    ):
        """
        Args:
            constraints: Dictionary of hard constraints (e.g., {'quality': 0.7, 'latency_ms': 500}).
                         If None, defaults are loaded from config.
            objectives: List of objective definitions, each with 'key' and 'direction' ('max'/'min').
                         If None, uses DEFAULT_OBJECTIVES.
        """
        self.constraints = constraints or {
            "quality_score": config.PARETO_QUALITY_MIN,
            "latency_ms": config.PARETO_LATENCY_MAX,
            "carbon_g": config.PARETO_CARBON_MAX,
        }
        self.objectives = objectives or DEFAULT_OBJECTIVES
        logger.info(f"ParetoGating initialized with {len(self.objectives)} objectives.")

    def filter(
        self,
        candidates: List[Dict[str, Any]],
        dynamic_constraints: Optional[Dict[str, Any]] = None,
        return_stats: bool = False,
    ) -> Tuple[List[Dict[str, Any]], Optional[Dict[str, int]]]:
        """
        Apply hard constraints and Pareto dominance to a list of candidates.

        Args:
            candidates: List of candidate dicts; each must contain all objective keys.
            dynamic_constraints: Optional overrides to the hard constraints.
            return_stats: If True, return statistics about filtering.

        Returns:
            List of Pareto‑optimal candidates (dicts).
            If return_stats is True, also returns a dict with counts.
        """
        # Merge constraints: dynamic overrides default
        effective_constraints = self.constraints.copy()
        if dynamic_constraints:
            effective_constraints.update(dynamic_constraints)

        # 1. Hard constraint filtering
        feasible = []
        for c in candidates:
            if self._satisfies_constraints(c, effective_constraints):
                feasible.append(c)

        if not feasible:
            if return_stats:
                return [], {"total": len(candidates), "feasible": 0, "pareto": 0}
            return []

        # 2. Pareto dominance check
        pareto = self._pareto_filter(feasible)

        # Logging
        logger.debug(
            f"Pareto filtering: {len(candidates)} candidates, "
            f"{len(feasible)} feasible, {len(pareto)} Pareto-optimal."
        )

        if return_stats:
            stats = {
                "total": len(candidates),
                "feasible": len(feasible),
                "pareto": len(pareto),
            }
            return pareto, stats

        return pareto

    def _satisfies_constraints(
        self, candidate: Dict[str, Any], constraints: Dict[str, Any]
    ) -> bool:
        """Check if candidate meets all hard constraints."""
        for key, threshold in constraints.items():
            value = candidate.get(key)
            if value is None:
                # If key not present, treat as violating (strict)
                logger.warning(f"Constraint key '{key}' missing in candidate.")
                return False
            # Assume constraints are of the form: value >= threshold for quality, <= for others.
            # We'll use a generic check: if the key is 'quality_score', require >=; else <=.
            if key == "quality_score":
                if value < threshold:
                    return False
            else:
                if value > threshold:
                    return False
        return True

    def _pareto_filter(self, candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Return only Pareto‑optimal candidates given the objective definitions.
        A candidate dominates another if it is better in at least one objective
        and not worse in all objectives.
        """
        n = len(candidates)
        pareto = []
        for i, c1 in enumerate(candidates):
            dominated = False
            for j, c2 in enumerate(candidates):
                if i == j:
                    continue
                if self._dominates(c2, c1):
                    dominated = True
                    break
            if not dominated:
                pareto.append(c1)
        return pareto

    def _dominates(self, a: Dict[str, Any], b: Dict[str, Any]) -> bool:
        """
        Return True if candidate a dominates candidate b.
        Dominance: a is better or equal in all objectives, and strictly better in at least one.
        """
        better_or_equal = []
        strictly_better = False
        for obj in self.objectives:
            key = obj["key"]
            direction = obj["direction"]
            val_a = a.get(key)
            val_b = b.get(key)
            if val_a is None or val_b is None:
                # If missing, cannot dominate; treat as not dominating
                return False
            if direction == "max":
                if val_a < val_b:
                    return False  # worse in this objective
                if val_a > val_b:
                    strictly_better = True
            else:  # min
                if val_a > val_b:
                    return False
                if val_a < val_b:
                    strictly_better = True
        return strictly_better

    def score_candidates(
        self,
        candidates: List[Dict[str, Any]],
        weights: Optional[Dict[str, float]] = None,
    ) -> List[Tuple[Dict[str, Any], float]]:
        """
        Compute a scalar score for each candidate using weighted sum of normalized objectives.
        Useful for selecting among Pareto‑optimal options.

        Args:
            candidates: List of candidate dicts.
            weights: Dictionary mapping objective keys to weights. If None, equal weights.

        Returns:
            List of (candidate, score) sorted descending.
        """
        if weights is None:
            weights = {obj["key"]: 1.0 for obj in self.objectives}

        # Extract objective values and normalize (min‑max across candidates)
        obj_keys = [obj["key"] for obj in self.objectives]
        # Build matrix: rows=candidates, cols=objectives
        matrix = np.array([[c.get(k, 0.0) for k in obj_keys] for c in candidates])
        # Normalize: (x - min) / (max - min) with safety
        mins = matrix.min(axis=0)
        maxs = matrix.max(axis=0)
        ranges = maxs - mins
        ranges[ranges == 0] = 1.0  # avoid division by zero
        normalized = (matrix - mins) / ranges

        # Adjust direction: for 'min' objectives, we want higher normalized score to be better.
        # So we invert for 'min' objectives: score = 1 - normalized.
        for i, obj in enumerate(self.objectives):
            if obj["direction"] == "min":
                normalized[:, i] = 1.0 - normalized[:, i]

        # Weighted sum
        weight_vec = np.array([weights.get(k, 1.0) for k in obj_keys])
        scores = normalized @ weight_vec

        # Pair with candidates and sort descending
        paired = list(zip(candidates, scores.tolist()))
        paired.sort(key=lambda x: x[1], reverse=True)
        return paired
