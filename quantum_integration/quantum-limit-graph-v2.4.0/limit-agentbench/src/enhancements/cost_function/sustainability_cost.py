from ..schemas.node_descriptor import NodeDescriptor
from ..schemas.workload_descriptor import WorkloadDescriptor
from ..data_integration.carbon_intensity import CarbonIntensityFetcher
from ..data_integration.material_footprint import MaterialFootprintUpdater
from ..data_integration.helium_collector import HeliumCollector

class SustainabilityCostFunction:
    """Multi‑objective sustainability cost function using real data."""
    def __init__(
        self,
        carbon_fetcher: CarbonIntensityFetcher,
        material_updater: MaterialFootprintUpdater,
        helium_collector: HeliumCollector,
        weights: dict = None
    ):
        self.carbon = carbon_fetcher
        self.material = material_updater
        self.helium = helium_collector
        self.weights = weights or {
            'energy': 0.2,
            'carbon': 0.3,
            'helium': 0.15,
            'material': 0.15,
            'latency': 0.1,
            'accuracy': 0.1
        }

    async def compute(self, node_desc: NodeDescriptor, workload: WorkloadDescriptor) -> float:
        # Energy cost
        energy_cost = node_desc.energy_per_token * workload.tokens

        # Carbon cost (kg CO₂)
        carbon_intensity = await self.carbon.get_intensity(node_desc.region)
        carbon_cost = energy_cost * carbon_intensity

        # Helium cost (1 - connectivity score)
        helium_cost = (1 - node_desc.helium_connectivity_score) * 0.5

        # Material cost
        material_cost = 0.0
        if node_desc.material_footprint_id:
            fp = self.material.get_footprint(node_desc.material_footprint_id)
            if fp:
                material_cost = fp['material_index']

        # Latency (normalized to seconds)
        latency_cost = workload.latency_target / 1000.0

        # Accuracy placeholder (should come from expert accuracy)
        accuracy_cost = 1.0

        total = (
            self.weights['energy'] * energy_cost +
            self.weights['carbon'] * carbon_cost +
            self.weights['helium'] * helium_cost +
            self.weights['material'] * material_cost +
            self.weights['latency'] * latency_cost +
            self.weights['accuracy'] * accuracy_cost
        )
        return total
