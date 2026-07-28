# File: src/enhancements/sustainability_cost.py
"""
Unified Sustainability Cost Function v2.0.

Computes the cost C = αE + βCO₂ + γH + δM + εL + ζA
for a given expert and context, with caching and batch optimizations.
"""

import asyncio
import logging
from typing import Dict, Any, Optional, List, Union
from datetime import datetime, timedelta
import numpy as np

from ..expert_registry import ExpertProfile
from ..carbon_manager import CarbonIntensityManager
from ..helium_dashboard import HeliumEfficiencyDashboard  # kept for backward compatibility
from .node_registry import NodeRegistry

logger = logging.getLogger(__name__)

class SustainabilityCostFunction:
    """
    Computes the cost C = αE + βCO₂ + γH + δM + εL + ζA
    for a given expert and context.

    Supports caching of node data and carbon intensity to reduce async calls.
    """

    def __init__(self, config: Dict[str, float]):
        """
        Args:
            config: weights for each component, e.g.,
                {'alpha': 1.0, 'beta': 2.0, 'gamma': 0.5, 'delta': 0.3, 'epsilon': 0.1, 'zeta': -0.1}
        """
        self._validate_config(config)
        self.weights = config.copy()
        self.carbon_manager: Optional[CarbonIntensityManager] = None
        self.node_registry: Optional[NodeRegistry] = None

        # Caches
        self._node_cache: Dict[str, Dict] = {}
        self._carbon_cache: Optional[Dict] = None
        self._carbon_cache_timestamp: Optional[datetime] = None
        self._carbon_cache_ttl = timedelta(seconds=300)  # 5 minutes

        self._lock = asyncio.Lock()  # for thread-safe cache updates

    def _validate_config(self, config: Dict[str, float]):
        """Ensure all required keys are present and values are floats."""
        required_keys = {'alpha', 'beta', 'gamma', 'delta', 'epsilon', 'zeta'}
        missing = required_keys - set(config.keys())
        if missing:
            raise ValueError(f"Missing required config keys: {missing}")
        for key, value in config.items():
            if not isinstance(value, (int, float)):
                raise TypeError(f"Config key '{key}' must be a number, got {type(value).__name__}")

    def inject_dependencies(
        self,
        carbon_manager: CarbonIntensityManager,
        node_registry: NodeRegistry,
        helium_dashboard: Optional[HeliumEfficiencyDashboard] = None
    ):
        """
        Inject external dependencies.

        Note: `helium_dashboard` is kept for backward compatibility but is not used.
        """
        self.carbon_manager = carbon_manager
        self.node_registry = node_registry
        if helium_dashboard:
            logger.debug("HeliumEfficiencyDashboard injected but will not be used.")

    async def _get_carbon_intensity(self) -> float:
        """
        Get current carbon intensity (kg/kWh) with caching.
        If cache is fresh, return cached value; otherwise fetch from manager.
        """
        async with self._lock:
            if (self._carbon_cache is not None and
                self._carbon_cache_timestamp is not None and
                datetime.now() - self._carbon_cache_timestamp < self._carbon_cache_ttl):
                return self._carbon_cache

            if self.carbon_manager is None:
                logger.warning("Carbon manager not injected; using fallback 0.4 kg/kWh.")
                intensity_kg = 0.4
            else:
                try:
                    intensity_data = await self.carbon_manager.get_current_intensity()
                    # Assume intensity is in g/kWh; convert to kg/kWh
                    intensity_g = intensity_data.get('intensity', 400)
                    intensity_kg = intensity_g / 1000.0
                except Exception as e:
                    logger.error(f"Failed to fetch carbon intensity: {e}; using fallback 0.4 kg/kWh.")
                    intensity_kg = 0.4

            self._carbon_cache = intensity_kg
            self._carbon_cache_timestamp = datetime.now()
            return intensity_kg

    async def _get_node_data(self, node_id: str) -> Dict[str, float]:
        """
        Get node data (helium_index, material_index) with caching.
        """
        if node_id in self._node_cache:
            return self._node_cache[node_id]

        if self.node_registry is None:
            logger.warning("Node registry not injected; defaulting to zero indices.")
            default = {'helium_index': 0.0, 'material_index': 0.0}
            self._node_cache[node_id] = default
            return default

        try:
            desc = await self.node_registry.get_node(node_id)
            if desc:
                data = {
                    'helium_index': getattr(desc, 'helium_index', 0.0),
                    'material_index': getattr(desc, 'material_index', 0.0)
                }
                self._node_cache[node_id] = data
                return data
            else:
                logger.warning(f"Node {node_id} not found; defaulting to zero indices.")
                default = {'helium_index': 0.0, 'material_index': 0.0}
                self._node_cache[node_id] = default
                return default
        except Exception as e:
            logger.error(f"Failed to fetch node {node_id}: {e}; defaulting to zero indices.")
            default = {'helium_index': 0.0, 'material_index': 0.0}
            self._node_cache[node_id] = default
            return default

    async def compute(self, expert: ExpertProfile, context: Dict[str, Any]) -> float:
        """
        Compute cost for a single expert given a context.

        Args:
            expert: ExpertProfile object.
            context: Dict containing 'token_count', 'target_node_id', and optionally 'expected_latency_ms'.

        Returns:
            float: computed cost.
        """
        tokens = context.get('token_count', 1)
        target_node = context.get('target_node_id')

        # Energy (E)
        E = expert.energy_per_inference * tokens

        # Carbon (CO₂)
        carbon_intensity = await self._get_carbon_intensity()
        CO2 = expert.carbon_per_inference * tokens * carbon_intensity

        # Helium (H)
        helium_usage = expert.helium_per_inference * tokens
        helium_index = 0.0
        if target_node:
            node_data = await self._get_node_data(target_node)
            helium_index = node_data.get('helium_index', 0.0)
        H = helium_usage * (1 + helium_index)

        # Material (M)
        material_index = 0.0
        if target_node:
            node_data = await self._get_node_data(target_node)  # cached, so no extra DB call
            material_index = node_data.get('material_index', 0.0)
        M = material_index

        # Latency (L)
        L = context.get('expected_latency_ms', 100.0)

        # Accuracy (A) – lower is better, so use 1 - accuracy
        # Ensure accuracy_score is between 0 and 1
        acc = max(0.0, min(1.0, expert.accuracy_score))
        A = 1.0 - acc

        # Apply weights
        cost = (
            self.weights.get('alpha', 1.0) * E +
            self.weights.get('beta', 1.0) * CO2 +
            self.weights.get('gamma', 1.0) * H +
            self.weights.get('delta', 1.0) * M +
            self.weights.get('epsilon', 1.0) * L +
            self.weights.get('zeta', 1.0) * A
        )
        return cost

    async def compute_multiple(self, experts: List[ExpertProfile], context: Dict[str, Any]) -> Dict[str, float]:
        """
        Return cost for each expert in a batch.

        This method fetches carbon intensity and node data once, then computes all costs
        concurrently for efficiency.

        Args:
            experts: List of ExpertProfile objects.
            context: Dict containing 'token_count', 'target_node_id', etc.

        Returns:
            Dict[str, float]: mapping expert_id -> cost.
        """
        # Fetch carbon intensity and node data once
        carbon_intensity = await self._get_carbon_intensity()
        target_node = context.get('target_node_id')
        node_data = {}
        if target_node:
            node_data = await self._get_node_data(target_node)

        # Pre‑compute common values
        tokens = context.get('token_count', 1)
        latency = context.get('expected_latency_ms', 100.0)

        async def compute_one(expert: ExpertProfile) -> float:
            # Energy
            E = expert.energy_per_inference * tokens

            # Carbon
            CO2 = expert.carbon_per_inference * tokens * carbon_intensity

            # Helium
            helium_usage = expert.helium_per_inference * tokens
            helium_index = node_data.get('helium_index', 0.0) if target_node else 0.0
            H = helium_usage * (1 + helium_index)

            # Material
            material_index = node_data.get('material_index', 0.0) if target_node else 0.0
            M = material_index

            # Accuracy
            acc = max(0.0, min(1.0, expert.accuracy_score))
            A = 1.0 - acc

            # Cost
            return (
                self.weights.get('alpha', 1.0) * E +
                self.weights.get('beta', 1.0) * CO2 +
                self.weights.get('gamma', 1.0) * H +
                self.weights.get('delta', 1.0) * M +
                self.weights.get('epsilon', 1.0) * latency +
                self.weights.get('zeta', 1.0) * A
            )

        # Run all computations concurrently
        tasks = [compute_one(expert) for expert in experts]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Map results to expert IDs, handling errors
        cost_dict = {}
        for expert, res in zip(experts, results):
            if isinstance(res, Exception):
                logger.error(f"Failed to compute cost for expert {expert.expert_id}: {res}")
                cost_dict[expert.expert_id] = float('inf')
            else:
                cost_dict[expert.expert_id] = res

        return cost_dict

    def set_weights(self, new_weights: Dict[str, float]):
        """
        Update the weights.
        """
        self._validate_config(new_weights)
        self.weights.update(new_weights)

    async def clear_cache(self):
        """Clear all caches (node and carbon)."""
        async with self._lock:
            self._node_cache.clear()
            self._carbon_cache = None
            self._carbon_cache_timestamp = None
            logger.debug("Caches cleared.")
