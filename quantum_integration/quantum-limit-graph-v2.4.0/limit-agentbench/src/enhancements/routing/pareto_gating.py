"""
Pareto Gating Module
====================
Filters infeasible actions via hard constraints and returns Pareto‑optimal options.
Enhanced with dynamic constraints, configurable objectives, and scalar scoring.

Enhancements included:
- Generalised constraint definitions (operators like >=, <=, ==, etc.)
- Support for missing objective values (drop or impute with worst value)
- Vectorised Pareto dominance for better performance
- Improved scalar scoring with multiple normalisation methods (minmax, zscore, rank)
- Dynamic objective updates via `set_objectives`
"""
import logging
from typing import List, Dict, Any, Optional, Tuple, Callable, Union
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

# Define supported comparison operators
_OPS = {
    ">=": lambda a, b: a >= b,
    "<=": lambda a, b: a <= b,
    ">": lambda a, b: a > b,
    "<": lambda a, b: a < b,
    "==": lambda a, b: a == b,
    "!=": lambda a, b: a != b,
}


class ParetoGating:
    """
    Ensures hard constraints are met and returns Pareto‑optimal options.
    Supports dynamic constraints, configurable objectives, missing value policies,
    and multiple scoring normalisation methods.
    """

    def __init__(
        self,
        constraints: Optional[Dict[str, Any]] = None,
        objectives: Optional[List[Dict[str, str]]] = None,
        missing_policy: str = "drop",
    ):
        """
        Args:
            constraints: Dictionary of hard constraints. Values can be:
                - numeric (legacy style: quality_score assumed >=, others <=)
                - dict with 'op' (one of '>=', '<=', '>', '<', '==', '!=') and 'value'
                         If None, defaults are loaded from config.
            objectives: List of objective definitions, each with 'key' and 'direction' ('max'/'min').
                         If None, uses DEFAULT_OBJECTIVES.
            missing_policy: How to handle missing objective values.
                'drop'   : candidate is infeasible and cannot dominate (default).
                'worst'  : impute with worst possible value based on direction.
                           For constraints, missing -> infeasible regardless.
        """
        self.missing_policy = missing_policy
        self.constraints = self._normalise_constraints(
            constraints
            or {
                "quality_score": config.PARETO_QUALITY_MIN,
                "latency_ms": config.PARETO_LATENCY_MAX,
                "carbon_g": config.PARETO_CARBON_MAX,
            }
        )
        self.objectives = objectives or DEFAULT_OBJECTIVES
        self._objective_keys = [obj["key"] for obj in self.objectives]
        self._objective_dirs = [obj["direction"] for obj in self.objectives]
        logger.info(
            f"ParetoGating initialized with {len(self.objectives)} objectives, "
            f"missing_policy='{self.missing_policy}'."
        )

    @staticmethod
    def _normalise_constraints(
        constraints: Dict[str, Any]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Convert legacy numeric constraints to the new dict format.
        Legacy assumption: quality_score uses '>=', all others use '<='.
        """
        normalised = {}
        for key, value in constraints.items():
            if isinstance(value, (int, float)):
                if key == "quality_score":
                    normalised[key] = {"op": ">=", "value": value}
                else:
                    normalised[key] = {"op": "<=", "value": value}
            elif isinstance(value, dict) and "op" in value and "value" in value:
                if value["op"] not in _OPS:
                    raise ValueError(f"Unsupported operator '{value['op']}' for key '{key}'.")
                normalised[key] = value
            else:
                raise ValueError(
                    f"Constraint for '{key}' must be numeric or dict with 'op' and 'value'."
                )
        return normalised

    def set_objectives(self, objectives: List[Dict[str, str]]) -> None:
        """
        Update the objective definitions dynamically.
        """
        self.objectives = objectives
        self._objective_keys = [obj["key"] for obj in self.objectives]
        self._objective_dirs = [obj["direction"] for obj in self.objectives]
        logger.info(f"Objectives updated to {len(self.objectives)} objectives.")

    def filter(
        self,
        candidates: List[Dict[str, Any]],
        dynamic_constraints: Optional[Dict[str, Any]] = None,
        return_stats: bool = False,
    ) -> Tuple[List[Dict[str, Any]], Optional[Dict[str, int]]]:
        """
        Apply hard constraints and Pareto dominance to a list of candidates.

        Args:
            candidates: List of candidate dicts; each must contain all objective keys
                        (unless missing_policy='worst').
            dynamic_constraints: Optional overrides to the hard constraints.
            return_stats: If True, return statistics about filtering.

        Returns:
            List of Pareto‑optimal candidates (dicts).
            If return_stats is True, also returns a dict with counts.
        """
        # Merge constraints: dynamic overrides default
        effective_constraints = self.constraints.copy()
        if dynamic_constraints:
            effective_constraints.update(self._normalise_constraints(dynamic_constraints))

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
        self, candidate: Dict[str, Any], constraints: Dict[str, Dict[str, Any]]
    ) -> bool:
        """Check if candidate meets all hard constraints."""
        for key, constraint in constraints.items():
            value = candidate.get(key)
            if value is None:
                # Missing value always infeasible for hard constraints
                logger.debug(f"Constraint key '{key}' missing in candidate; infeasible.")
                return False
            op_func = _OPS[constraint["op"]]
            if not op_func(value, constraint["value"]):
                return False
        return True

    def _pareto_filter(self, candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Return only Pareto‑optimal candidates using vectorised dominance.
        Falls back to loop-based for very large candidate sets to avoid memory issues.
        """
        n = len(candidates)
        if n == 0:
            return []
        if n > 1000:  # fallback threshold to avoid large memory
            return self._pareto_filter_loop(candidates)

        # Build numpy array of objective values, adjusting for direction
        # For 'min' objectives, multiply by -1 so that larger is always better.
        m = len(self.objectives)
        obj_matrix = np.zeros((n, m))
        for i, c in enumerate(candidates):
            for j, obj in enumerate(self.objectives):
                key = obj["key"]
                val = c.get(key)
                if val is None:
                    if self.missing_policy == "worst":
                        # worst possible: -inf for max after sign adjustment, i.e., -inf always
                        val = -np.inf
                    else:  # 'drop' -> candidate cannot participate in dominance
                        # Set to -inf so it can be dominated but never dominates
                        val = -np.inf
                if obj["direction"] == "min":
                    val = -val
                obj_matrix[i, j] = val

        # Vectorised dominance: a dominates b if all(a >= b) and any(a > b)
        # better_or_equal[i, j] is True if candidate i >= candidate j in all objectives
        better_or_equal = np.all(obj_matrix[:, None, :] >= obj_matrix[None, :, :], axis=2)
        # strictly_better[i, j] is True if candidate i > candidate j in at least one objective
        strictly_better = np.any(obj_matrix[:, None, :] > obj_matrix[None, :, :], axis=2)

        # dominated_by[i] is True if candidate i is dominated by any other candidate
        dominated_by = better_or_equal & strictly_better
        # Exclude self-dominance
        np.fill_diagonal(dominated_by, False)
        is_dominated = np.any(dominated_by, axis=0)  # column-wise: candidate i dominated by any row

        pareto_indices = np.where(~is_dominated)[0]
        return [candidates[i] for i in pareto_indices]

    def _pareto_filter_loop(self, candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Fallback loop-based Pareto filter for very large sets."""
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
        better_or_equal = True
        strictly_better = False
        for obj in self.objectives:
            key = obj["key"]
            direction = obj["direction"]
            val_a = a.get(key)
            val_b = b.get(key)
            if val_a is None or val_b is None:
                if self.missing_policy == "drop":
                    return False  # cannot dominate if missing values
                else:  # 'worst'
                    # Treat missing as worst possible: -inf for max, +inf for min
                    if direction == "max":
                        val_a = -np.inf if val_a is None else val_a
                        val_b = -np.inf if val_b is None else val_b
                    else:
                        val_a = np.inf if val_a is None else val_a
                        val_b = np.inf if val_b is None else val_b
            if direction == "max":
                if val_a < val_b:
                    return False
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
        normalisation: str = "minmax",
    ) -> List[Tuple[Dict[str, Any], float]]:
        """
        Compute a scalar score for each candidate using weighted sum of normalized objectives.
        Useful for selecting among Pareto‑optimal options.

        Args:
            candidates: List of candidate dicts.
            weights: Dictionary mapping objective keys to weights. If None, equal weights.
            normalisation: 'minmax' (default), 'zscore', or 'rank'.

        Returns:
            List of (candidate, score) sorted descending.
        """
        if weights is None:
            weights = {obj["key"]: 1.0 for obj in self.objectives}

        obj_keys = [obj["key"] for obj in self.objectives]
        # Build matrix: rows=candidates, cols=objectives
        # Handle missing values according to policy
        matrix = []
        for c in candidates:
            row = []
            for key, obj in zip(obj_keys, self.objectives):
                val = c.get(key)
                if val is None:
                    if self.missing_policy == "worst":
                        val = -np.inf if obj["direction"] == "max" else np.inf
                    else:
                        # 'drop' -> skip candidate? Or use worst? We'll use worst to avoid NaN.
                        val = -np.inf if obj["direction"] == "max" else np.inf
                row.append(val)
            matrix.append(row)
        matrix = np.array(matrix, dtype=float)

        # Normalisation
        if normalisation == "minmax":
            mins = matrix.min(axis=0)
            maxs = matrix.max(axis=0)
            ranges = maxs - mins
            ranges[ranges == 0] = 1.0
            normalised = (matrix - mins) / ranges
            # For 'min' objectives, invert so higher normalised is better
            for i, obj in enumerate(self.objectives):
                if obj["direction"] == "min":
                    normalised[:, i] = 1.0 - normalised[:, i]
        elif normalisation == "zscore":
            means = matrix.mean(axis=0)
            stds = matrix.std(axis=0)
            stds[stds == 0] = 1.0
            normalised = (matrix - means) / stds
            # For 'min', negate so higher is better
            for i, obj in enumerate(self.objectives):
                if obj["direction"] == "min":
                    normalised[:, i] = -normalised[:, i]
        elif normalisation == "rank":
            # Rank normalisation: replace each value by its rank (0..n-1) for each objective
            normalised = np.zeros_like(matrix, dtype=float)
            for j in range(matrix.shape[1]):
                col = matrix[:, j]
                order = col.argsort()
                ranks = np.empty_like(order, dtype=float)
                ranks[order] = np.arange(len(col))
                # For 'max', higher rank is better; for 'min', lower rank is better
                if self.objectives[j]["direction"] == "min":
                    ranks = len(col) - 1 - ranks  # invert so higher is better
                normalised[:, j] = ranks
                # Scale to [0,1]
                max_rank = len(col) - 1 if len(col) > 1 else 1.0
                normalised[:, j] /= max_rank
        else:
            raise ValueError(f"Unknown normalisation method '{normalisation}'.")

        # Weighted sum
        weight_vec = np.array([weights.get(k, 1.0) for k in obj_keys])
        scores = normalised @ weight_vec

        # Pair with candidates and sort descending
        paired = list(zip(candidates, scores.tolist()))
        paired.sort(key=lambda x: x[1], reverse=True)
        return paired
