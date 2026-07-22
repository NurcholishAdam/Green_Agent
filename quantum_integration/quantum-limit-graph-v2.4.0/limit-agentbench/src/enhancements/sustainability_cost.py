# File: src/enhancements/sustainability_cost.py
"""
Unified Sustainability Cost Function.
"""

from typing import Dict, Any, Optional, List
import numpy as np

from ..expert_registry import ExpertProfile
from ..carbon_manager import CarbonIntensityManager
from ..helium_dashboard import HeliumEfficiencyDashboard
from .node_registry import NodeRegistry

class SustainabilityCostFunction:
    """
    Computes the cost C = αE + βCO₂ + γH + δM + εL + ζA
    for a given expert and context.
    """

    def __init__(self, config: Dict[str, float]):
        """
        config: weights for each component, e.g.,
            {'alpha': 1.0, 'beta': 2.0, 'gamma': 0.5, 'delta': 0.3, 'epsilon': 0.1, 'zeta': -0.1}
        """
        self.weights = config
        self.carbon_manager: Optional[CarbonIntensityManager] = None
        self.helium_dashboard: Optional[HeliumEfficiencyDashboard] = None
        self.node_registry: Optional[NodeRegistry] = None

    def inject_dependencies(
        self,
        carbon_manager: CarbonIntensityManager,
        helium_dashboard: HeliumEfficiencyDashboard,
        node_registry: NodeRegistry
    ):
        self.carbon_manager = carbon_manager
        self.helium_dashboard = helium_dashboard
        self.node_registry = node_registry

    async def compute(self, expert: ExpertProfile, context: Dict[str, Any]) -> float:
        """Compute cost for a single expert given a context."""
        tokens = context.get('token_count', 1)
        target_node = context.get('target_node_id')

        # Energy (E) – from expert's energy_per_inference * tokens
        E = expert.energy_per_inference * tokens

        # Carbon (CO₂) – from expert's carbon_per_inference * tokens * grid intensity
        carbon_intensity = 0.4  # kg/kWh fallback
        if self.carbon_manager:
            intensity_data = await self.carbon_manager.get_current_intensity()
            carbon_intensity = intensity_data.get('intensity', 400) / 1000  # g/kWh -> kg/kWh
        CO2 = expert.carbon_per_inference * tokens * carbon_intensity

        # Helium (H) – from expert usage and node helium index
        helium_usage = expert.helium_per_inference * tokens
        helium_index = 0.0
        if target_node and self.node_registry:
            desc = await self.node_registry.get_node(target_node)
            if desc:
                helium_index = desc.helium_index
        H = helium_usage * (1 + helium_index)

        # Material (M) – from node material index
        material_index = 0.0
        if target_node and self.node_registry:
            desc = await self.node_registry.get_node(target_node)
            if desc:
                material_index = desc.material_index
        M = material_index

        # Latency (L) – from context
        L = context.get('expected_latency_ms', 100)

        # Accuracy (A) – lower is better, so use 1 - accuracy
        A = 1.0 - expert.accuracy_score

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
        """Return cost for each expert in a batch."""
        results = {}
        for expert in experts:
            results[expert.expert_id] = await self.compute(expert, context)
        return results

    def set_weights(self, new_weights: Dict[str, float]):
        self.weights.update(new_weights)
