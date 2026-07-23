# File: src/enhancements/pareto_router.py
"""
Pareto Frontier Routing – Multi‑objective optimization for Green Agent.
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
import numpy as np

from ..expert_router import ExpertRouter
from ..expert_registry import ExpertProfile
from ..node_registry import NodeRegistry
from .adaptive_cost_function import AdaptiveCostFunction
from ..user_preferences import UserPreferences

logger = logging.getLogger(__name__)

class ParetoRouter(ExpertRouter):
    """
    Multi‑objective router that computes the Pareto frontier of non‑dominated experts.
    """

    def __init__(
        self,
        config: Dict[str, Any],
        cost_function: AdaptiveCostFunction,
        node_registry: NodeRegistry,
        user_preferences: Optional[UserPreferences] = None,
        *args,
        **kwargs
    ):
        super().__init__(config, *args, **kwargs)
        self.cost_function = cost_function
        self.node_registry = node_registry
        self.user_prefs = user_preferences

        # Cache for vectors during a routing call
        self._vectors: Dict[str, np.ndarray] = {}

    async def route(self, task: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Route a task by computing the Pareto frontier and then selecting an expert
        based on user preferences or a default strategy.
        """
        # 1. Get candidate experts (existing logic)
        candidates = self.get_candidate_experts(task, context)

        # 2. Compute vectors for each candidate
        self._vectors = {}
        for expert in candidates:
            vec = await self._compute_vector(expert, context)
            self._vectors[expert.expert_id] = vec

        # 3. Find Pareto frontier
        frontier = self._pareto_frontier(self._vectors)

        # 4. Apply preference selection
        if self.user_prefs and frontier:
            selected_id = await self._apply_preferences(frontier, context)
        else:
            selected_id = self._select_knee(frontier)

        # 5. If no frontier (shouldn't happen), fallback to first candidate
        if selected_id is None and candidates:
            selected_id = candidates[0].expert_id

        selected_expert = self.registry.get_expert(selected_id)

        # 6. Prepare frontier data for response
        frontier_data = [
            {'expert_id': pid, 'vector': self._vectors[pid].tolist()}
            for pid in frontier
        ] if frontier else []

        return {
            'expert': selected_expert,
            'frontier': frontier_data,
            'selected_id': selected_id,
            'timestamp': datetime.now().isoformat()
        }

    async def _compute_vector(self, expert: ExpertProfile, context: Dict[str, Any]) -> np.ndarray:
        """
        Compute the 6‑dimensional vector for an expert:
        [Energy, Carbon, Helium, Material, Latency, Accuracy (1‑accuracy)].
        """
        tokens = context.get('token_count', 1)
        target_node = context.get('target_node_id')

        # Energy (J)
        E = expert.energy_per_inference * tokens

        # Carbon (kg CO₂)
        carbon_intensity = 0.4  # kg/kWh fallback
        if self.cost_function.carbon_manager:
            intensity_data = await self.cost_function.carbon_manager.get_current_intensity()
            carbon_intensity = intensity_data.get('intensity', 400) / 1000  # g/kWh → kg/kWh
        CO2 = expert.carbon_per_inference * tokens * carbon_intensity

        # Helium (units)
        helium_usage = expert.helium_per_inference * tokens
        helium_index = 0.0
        if target_node and self.node_registry:
            desc = await self.node_registry.get_node(target_node)
            if desc:
                helium_index = desc.helium_index
        H = helium_usage * (1 + helium_index)

        # Material (index)
        material_index = 0.0
        if target_node and self.node_registry:
            desc = await self.node_registry.get_node(target_node)
            if desc:
                material_index = desc.material_index
        M = material_index

        # Latency (ms)
        L = context.get('expected_latency_ms', 100)

        # Accuracy (1 - accuracy, to minimize)
        A = 1.0 - expert.accuracy_score

        return np.array([E, CO2, H, M, L, A])

    def _pareto_frontier(self, vectors: Dict[str, np.ndarray]) -> List[str]:
        """
        Return the list of expert IDs that are Pareto‑optimal.
        A vector A dominates B if all components of A ≤ B and at least one is strictly less.
        """
        expert_ids = list(vectors.keys())
        n = len(expert_ids)
        dominated = [False] * n

        for i in range(n):
            for j in range(n):
                if i == j:
                    continue
                vec_i = vectors[expert_ids[i]]
                vec_j = vectors[expert_ids[j]]
                if self._dominates(vec_i, vec_j):
                    dominated[j] = True

        frontier = [expert_ids[i] for i in range(n) if not dominated[i]]
        return frontier

    def _dominates(self, a: np.ndarray, b: np.ndarray) -> bool:
        """Return True if a dominates b (all components <= and at least one <)."""
        return np.all(a <= b) and np.any(a < b)

    async def _apply_preferences(self, frontier: List[str], context: Dict[str, Any]) -> Optional[str]:
        """
        Apply user preferences to select a single expert from the frontier.
        Uses weighted sum with user‑defined weights.
        """
        if not self.user_prefs or not frontier:
            return None

        weights = self.user_prefs.get_weights()
        if not weights:
            return None

        best_id = None
        best_score = float('inf')
        for pid in frontier:
            vec = self._vectors[pid]
            score = sum(
                weights.get('alpha', 1.0) * vec[0] +
                weights.get('beta', 1.0) * vec[1] +
                weights.get('gamma', 0.5) * vec[2] +
                weights.get('delta', 0.3) * vec[3] +
                weights.get('epsilon', 0.1) * vec[4] +
                weights.get('zeta', -0.1) * vec[5]
            )
            if score < best_score:
                best_score = score
                best_id = pid
        return best_id

    def _select_knee(self, frontier: List[str]) -> Optional[str]:
        """
        Select the 'knee' point: the point closest to the ideal point (min of each component).
        """
        if not frontier:
            return None

        # Compute ideal point (component‑wise minimum)
        vectors = [self._vectors[pid] for pid in frontier]
        ideal = np.min(vectors, axis=0)

        # Find point with minimum Euclidean distance to ideal
        best_id = None
        best_dist = float('inf')
        for pid in frontier:
            vec = self._vectors[pid]
            dist = np.linalg.norm(vec - ideal)
            if dist < best_dist:
                best_dist = dist
                best_id = pid
        return best_id

    async def get_frontier(self, task: Dict[str, Any], context: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Return only the Pareto frontier without selecting an expert.
        Useful for visualisation or decision support.
        """
        candidates = self.get_candidate_experts(task, context)
        self._vectors = {}
        for expert in candidates:
            vec = await self._compute_vector(expert, context)
            self._vectors[expert.expert_id] = vec

        frontier = self._pareto_frontier(self._vectors)
        return [
            {'expert_id': pid, 'vector': self._vectors[pid].tolist()}
            for pid in frontier
        ]
