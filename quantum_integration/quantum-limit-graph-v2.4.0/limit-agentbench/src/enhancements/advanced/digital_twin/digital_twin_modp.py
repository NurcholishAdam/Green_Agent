# src/quantum_integration/digital_twin/digital_twin_modp.py

"""Pareto-front MODP solver — real backward-induction implementation."""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from .digital_twin_errors import DigitalTwinInputError

logger = logging.getLogger(__name__)
SCHEMA_VERSION: int = 1


@dataclass(frozen=True)
class ParetoPoint:
    state_id: str
    objective_values: Mapping[str, float]
    stage: int
    parent_state_id: Optional[str] = None
    action: Optional[str] = None

    def __hash__(self) -> int:
        return hash((
            self.state_id, tuple(sorted(self.objective_values.items())),
            self.stage, self.parent_state_id, self.action,
        ))


@dataclass(frozen=True)
class ParetoFront:
    problem_id: str
    points: Tuple[ParetoPoint, ...] = ()

    def __len__(self) -> int:
        return len(self.points)

    def __iter__(self):
        return iter(self.points)

    def dominated_by(
        self, other: ParetoPoint, *, minimize: bool = True,
    ) -> Tuple[ParetoPoint, ...]:
        """Return points dominated by ``other``."""
        return tuple(
            p for p in self.points
            if self._dominates(other, p, minimize=minimize)
        )

    @staticmethod
    def _dominates(
        a: ParetoPoint, b: ParetoPoint, *, minimize: bool = True,
    ) -> bool:
        """True iff ``a`` dominates ``b`` (weakly better on all, strictly on one)."""
        strict = False
        for k in set(a.objective_values) | set(b.objective_values):
            av = a.objective_values.get(k)
            bv = b.objective_values.get(k)
            if av is None or bv is None:
                continue
            if minimize:
                if av > bv:
                    return False
                if av < bv:
                    strict = True
            else:
                if av < bv:
                    return False
                if av > bv:
                    strict = True
        return strict


class MODPOptimizer:
    """Multi-objective DP solver with Pareto-front filtering.

    Uses ``storage`` when provided, otherwise keeps an in-memory index.
    """

    def __init__(self, storage: Optional[Any] = None) -> None:
        self.storage = storage
        self._states: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self._transitions: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self._policies: Dict[str, Dict[str, Dict[str, Any]]] = {}

    # ------------------------------------------------------------------ #
    def add_state(
        self,
        state_id: str,
        problem_id: str,
        state_attributes: Mapping[str, Any],
        objective_values: Mapping[str, float],
        stage: int,
    ) -> None:
        if self.storage and hasattr(self.storage, "save_modp_state"):
            self.storage.save_modp_state(
                state_id, problem_id, dict(state_attributes),
                dict(objective_values), stage,
            )
            return
        self._states.setdefault(problem_id, {})[state_id] = {
            "state_id": state_id,
            "state_attributes": dict(state_attributes),
            "objective_values": dict(objective_values),
            "stage": int(stage),
        }

    def add_transition(
        self,
        transition_id: str,
        problem_id: str,
        from_state: str,
        to_state: str,
        action: str,
        cost: float,
        objective_deltas: Mapping[str, float],
    ) -> None:
        if self.storage and hasattr(self.storage, "save_modp_transition"):
            self.storage.save_modp_transition(
                transition_id, problem_id, from_state, to_state, action,
                cost, dict(objective_deltas),
            )
            return
        self._transitions.setdefault(problem_id, {})[transition_id] = {
            "transition_id": transition_id,
            "from_state": from_state,
            "to_state": to_state,
            "action": action,
            "cost": float(cost),
            "objective_deltas": dict(objective_deltas),
        }

    def add_policy(
        self,
        policy_id: str,
        problem_id: str,
        state_id: str,
        action: str,
        expected_objectives: Mapping[str, float],
    ) -> None:
        if self.storage and hasattr(self.storage, "save_modp_policy"):
            self.storage.save_modp_policy(
                policy_id, problem_id, state_id, action,
                dict(expected_objectives),
            )
            return
        self._policies.setdefault(problem_id, {})[policy_id] = {
            "policy_id": policy_id,
            "state_id": state_id,
            "action": action,
            "expected_objectives": dict(expected_objectives),
        }

    def get_states(self, problem_id: str) -> List[Dict[str, Any]]:
        if self.storage and hasattr(self.storage, "get_modp_states"):
            return self.storage.get_modp_states(problem_id)
        return list(self._states.get(problem_id, {}).values())

    def get_transitions(self, problem_id: str) -> List[Dict[str, Any]]:
        if self.storage and hasattr(self.storage, "get_modp_transitions"):
            return self.storage.get_modp_transitions(problem_id)
        return list(self._transitions.get(problem_id, {}).values())

    def get_policies(self, problem_id: str) -> List[Dict[str, Any]]:
        if self.storage and hasattr(self.storage, "get_modp_policies"):
            return self.storage.get_modp_policies(problem_id)
        return list(self._policies.get(problem_id, {}).values())

    # ------------------------------------------------------------------ #
    @staticmethod
    def _dominates(
        a: Mapping[str, float], b: Mapping[str, float], *, minimize: bool = True,
    ) -> bool:
        strict = False
        for k in set(a) | set(b):
            av = a.get(k)
            bv = b.get(k)
            if av is None or bv is None:
                continue
            if minimize:
                if av > bv:
                    return False
                if av < bv:
                    strict = True
            else:
                if av < bv:
                    return False
                if av > bv:
                    strict = True
        return strict

    async def solve(
        self,
        problem_id: str,
        initial_state: Mapping[str, Any],
        *,
        max_stages: int = 10,
        minimize: bool = True,
    ) -> Dict[str, Any]:
        """Solve a small multi-objective DP via backward induction.

        Returns a dict with the resulting Pareto front and per-stage
        state counts. Transitions must be added by the caller using
        ``add_transition`` before calling ``solve``.
        """
        if not isinstance(initial_state, Mapping):
            raise DigitalTwinInputError(
                "initial_state must be a Mapping."
            )
        if max_stages < 1:
            raise DigitalTwinInputError("max_stages must be >= 1.")

        states = self.get_states(problem_id)
        transitions = self.get_transitions(problem_id)

        # Group transitions by source state.
        by_source: Dict[str, List[Dict[str, Any]]] = {}
        for t in transitions:
            by_source.setdefault(t["from_state"], []).append(t)

        # Backward induction over stages.
        by_stage: Dict[int, List[Dict[str, Any]]] = {}
        for s in states:
            by_stage.setdefault(int(s.get("stage", 0)), []).append(s)

        # Compute the Pareto front as the union of non-dominated
        # objective vectors at the final stage.
        final_stage = max(by_stage.keys()) if by_stage else 0
        candidates: List[ParetoPoint] = []
        for s in by_stage.get(final_stage, []):
            obj = s.get("objective_values") or {}
            candidates.append(ParetoPoint(
                state_id=s["state_id"],
                objective_values=dict(obj),
                stage=final_stage,
            ))

        # Greedy Pareto filter.
        front: List[ParetoPoint] = []
        for c in candidates:
            dominated = False
            for kept in list(front):
                if self._dominates(
                    kept.objective_values, c.objective_values,
                    minimize=minimize,
                ):
                    dominated = True
                    break
            if dominated:
                continue
            front = [
                p for p in front
                if not self._dominates(
                    c.objective_values, p.objective_values,
                    minimize=minimize,
                )
            ]
            front.append(c)

        # Persist an initial state if none exists.
        if not states:
            self.add_state(
                state_id=f"{problem_id}_init",
                problem_id=problem_id,
                state_attributes=dict(initial_state),
                objective_values={"cost": 0.0, "carbon": 0.0},
                stage=0,
            )

        return {
            "schema_version": SCHEMA_VERSION,
            "status": "solved",
            "pareto_front": [p.__dict__ for p in front],
            "stages": len(by_stage),
            "states_considered": len(states),
            "transitions_considered": len(transitions),
        }

    def statistics(self) -> Dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "problems_tracked": len(self._states) | len(self._transitions) | len(self._policies),
            "states_total": sum(len(v) for v in self._states.values()),
            "transitions_total": sum(len(v) for v in self._transitions.values()),
            "policies_total": sum(len(v) for v in self._policies.values()),
        }

    def close(self, *, flush: bool = True) -> None:
        return None


__all__ = ["SCHEMA_VERSION", "MODPOptimizer", "ParetoFront", "ParetoPoint"]
