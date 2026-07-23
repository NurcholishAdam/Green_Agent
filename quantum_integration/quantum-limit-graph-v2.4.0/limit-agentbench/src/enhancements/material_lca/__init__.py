# material_lca/__init__.py (or material_lca.py)
"""
Material Index Integration with Hardware Life‑Cycle Databases
=============================================================

Fetches accurate embodied carbon and rare‑earth content from public LCA databases
(Ecoinvent, OpenLCA, etc.) and integrates them into the Green_Agent system.

Features:
- Cached API client with fallback to heuristics.
- Automatic material index update on hardware registration.
- DigitalTwin simulation for long‑term material impact.
- Extended sustainability cost function using real‑world material footprints.
"""

import json
import logging
import time
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, timedelta
import hashlib
import os

logger = logging.getLogger(__name__)

# ============================================================================
# 1. CONFIGURATION
# ============================================================================
LCA_CONFIG = {
    # Choose the data source: "mock", "ecoinvent", "openlca", or "cache_only"
    "source": os.environ.get("LCA_SOURCE", "mock"),
    # API endpoint for the chosen source (if applicable)
    "api_url": os.environ.get("LCA_API_URL", "https://api.example.com/lca"),
    "api_key": os.environ.get("LCA_API_KEY", ""),
    # Cache directory for offline use
    "cache_dir": os.environ.get("LCA_CACHE_DIR", "./lca_cache"),
    # Fallback values if no data is found
    "default_embodied_carbon": 50.0,  # kg CO₂ per kg of hardware
    "default_rare_earth_fraction": 0.001,  # fraction of total mass
    "default_material_index": 1.0,  # dimensionless
    # Refresh interval (seconds) for cache
    "cache_ttl": 86400 * 7,  # 7 days
}


# ============================================================================
# 2. DATA STRUCTURES
# ============================================================================
@dataclass
class MaterialFootprint:
    """Complete material footprint for a hardware model."""
    hardware_model: str
    embodied_carbon_kg: float  # kg CO₂ eq per unit
    rare_earth_kg: float  # kg of rare‑earth elements per unit
    total_mass_kg: float
    material_index: float  # composite score (higher = worse)
    # Additional metrics (optional)
    water_usage_l: float = 0.0
    energy_mj: float = 0.0
    source: str = "mock"  # which database provided this data
    timestamp: datetime = field(default_factory=datetime.now)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "hardware_model": self.hardware_model,
            "embodied_carbon_kg": self.embodied_carbon_kg,
            "rare_earth_kg": self.rare_earth_kg,
            "total_mass_kg": self.total_mass_kg,
            "material_index": self.material_index,
            "water_usage_l": self.water_usage_l,
            "energy_mj": self.energy_mj,
            "source": self.source,
            "timestamp": self.timestamp.isoformat(),
        }


# ============================================================================
# 3. LCA API CLIENT (with caching & mock fallback)
# ============================================================================
class LCAClient:
    """
    Client for fetching material footprints from external LCA databases.
    Supports mock data, Ecoinvent, OpenLCA, or other APIs.
    """

    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or LCA_CONFIG
        self.cache: Dict[str, MaterialFootprint] = {}
        self._load_cache()

    def _cache_path(self) -> str:
        """Return the file path for the local cache."""
        os.makedirs(self.config["cache_dir"], exist_ok=True)
        return os.path.join(self.config["cache_dir"], "lca_cache.json")

    def _load_cache(self) -> None:
        """Load cached footprints from disk."""
        path = self._cache_path()
        if os.path.exists(path):
            try:
                with open(path, "r") as f:
                    data = json.load(f)
                for model, fp_dict in data.items():
                    fp = MaterialFootprint(
                        hardware_model=model,
                        embodied_carbon_kg=fp_dict["embodied_carbon_kg"],
                        rare_earth_kg=fp_dict["rare_earth_kg"],
                        total_mass_kg=fp_dict["total_mass_kg"],
                        material_index=fp_dict["material_index"],
                        water_usage_l=fp_dict.get("water_usage_l", 0.0),
                        energy_mj=fp_dict.get("energy_mj", 0.0),
                        source=fp_dict.get("source", "cache"),
                        timestamp=datetime.fromisoformat(fp_dict["timestamp"]),
                    )
                    self.cache[model] = fp
            except Exception as e:
                logger.warning(f"Failed to load LCA cache: {e}")

    def _save_cache(self) -> None:
        """Save current cache to disk."""
        data = {model: fp.to_dict() for model, fp in self.cache.items()}
        path = self._cache_path()
        try:
            with open(path, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.warning(f"Failed to save LCA cache: {e}")

    def _fetch_from_api(self, hardware_model: str) -> Optional[MaterialFootprint]:
        """
        Query the external LCA API for the given hardware model.
        This is a mock implementation – replace with actual API calls.
        """
        source = self.config["source"]
        if source == "mock":
            return self._generate_mock_footprint(hardware_model)
        elif source in ("ecoinvent", "openlca"):
            # Here you would implement the real API call.
            # For demonstration, we'll fallback to mock.
            logger.info(f"Would query {source} for {hardware_model}")
            return self._generate_mock_footprint(hardware_model)
        else:
            # No API source configured
            return None

    def _generate_mock_footprint(self, hardware_model: str) -> MaterialFootprint:
        """Generate a plausible footprint based on the model name."""
        # Create deterministic but varied values
        hash_val = int(hashlib.md5(hardware_model.encode()).hexdigest(), 16) % 1000
        mass_kg = 2.0 + (hash_val % 10) * 0.5  # 2–7 kg
        carbon = 20.0 + (hash_val % 80) * 0.5  # 20–60 kg CO₂
        rare_earth = 0.002 + (hash_val % 5) * 0.001  # 0.002–0.007 kg
        material_index = carbon / 50.0 + rare_earth * 100  # simple composite

        return MaterialFootprint(
            hardware_model=hardware_model,
            embodied_carbon_kg=carbon,
            rare_earth_kg=rare_earth,
            total_mass_kg=mass_kg,
            material_index=material_index,
            water_usage_l=5.0 + (hash_val % 20),
            energy_mj=10.0 + (hash_val % 40),
            source="mock",
        )

    def get_footprint(self, hardware_model: str, force_refresh: bool = False) -> MaterialFootprint:
        """
        Retrieve the material footprint for a hardware model.
        Uses cache if available and not expired; otherwise fetches from API.
        """
        # Check cache
        if not force_refresh and hardware_model in self.cache:
            cached = self.cache[hardware_model]
            age = (datetime.now() - cached.timestamp).total_seconds()
            if age < self.config["cache_ttl"]:
                return cached

        # Fetch from API
        fp = self._fetch_from_api(hardware_model)
        if fp is None:
            # Fallback to default values
            fp = MaterialFootprint(
                hardware_model=hardware_model,
                embodied_carbon_kg=self.config["default_embodied_carbon"],
                rare_earth_kg=self.config["default_rare_earth_fraction"] * 1.0,  # assume 1kg mass
                total_mass_kg=1.0,
                material_index=self.config["default_material_index"],
                source="default",
            )
        # Store in cache
        self.cache[hardware_model] = fp
        self._save_cache()
        return fp

    def update_footprint(self, hardware_model: str, footprint: MaterialFootprint) -> None:
        """Manually update the cache with a custom footprint."""
        self.cache[hardware_model] = footprint
        self._save_cache()


# ============================================================================
# 4. EXTENSION TO NODEDESCRIPTOR (assume it exists)
# ============================================================================
# In your existing system, you have a class like NodeDescriptor.
# We will extend it by adding a 'material_index' field and a method to update it.
# Since we cannot modify the original, we provide a wrapper or monkey‑patch.

class MaterialAwareNodeDescriptor:
    """
    Wrapper / extension for NodeDescriptor that adds material footprint awareness.
    In practice, you would add these fields to your actual NodeDescriptor class.
    """
    def __init__(self, original_node_descriptor):
        self._node = original_node_descriptor
        self.material_footprint: Optional[MaterialFootprint] = None
        self.material_index: float = LCA_CONFIG["default_material_index"]

    def update_material_footprint(self, footprint: MaterialFootprint):
        self.material_footprint = footprint
        self.material_index = footprint.material_index
        # Also store in the original if it has a field
        if hasattr(self._node, "material_index"):
            self._node.material_index = footprint.material_index
        if hasattr(self._node, "material_footprint"):
            self._node.material_footprint = footprint


# ============================================================================
# 5. NODEREGISTRY EXTENSION
# ============================================================================
class NodeRegistryLCAExtension:
    """
    Extends the NodeRegistry to automatically fetch and attach material footprints
    when new hardware is registered.
    """

    def __init__(self, node_registry, lca_client: LCAClient = None):
        self.node_registry = node_registry
        self.lca_client = lca_client or LCAClient()

    def register_node(self, node_id: str, hardware_model: str, **kwargs):
        """
        Override or wrap the register_node method.
        After registration, fetch material data and update the node descriptor.
        """
        # Call the original registration method
        descriptor = self.node_registry.register_node(node_id, hardware_model, **kwargs)

        # Fetch footprint
        footprint = self.lca_client.get_footprint(hardware_model)

        # Attach to the descriptor (if it supports it)
        if hasattr(descriptor, "material_index"):
            descriptor.material_index = footprint.material_index
        if hasattr(descriptor, "material_footprint"):
            descriptor.material_footprint = footprint
        else:
            # If not, we can store it externally (e.g., in a dict)
            logger.warning("NodeDescriptor does not have material_index; storing externally.")
            if not hasattr(self, "_material_map"):
                self._material_map = {}
            self._material_map[node_id] = footprint

        # Return the descriptor (or updated wrapper)
        return descriptor


# ============================================================================
# 6. DIGITALTWIN SIMULATION
# ============================================================================
class DigitalTwinMaterialSimulator:
    """
    Simulates long‑term material impact of hardware refresh decisions.
    Uses material footprints to project cumulative embodied carbon, rare‑earth usage,
    and waste over time.
    """

    def __init__(self, lca_client: LCAClient = None):
        self.lca_client = lca_client or LCAClient()

    def simulate_refresh_cycle(
        self,
        hardware_model: str,
        quantity: int = 1,
        lifetime_years: float = 5,
        refresh_interval_years: float = 3,
        years_to_simulate: int = 10,
    ) -> Dict[str, Any]:
        """
        Simulate the material impact of refreshing hardware over a period.

        Returns a dict with cumulative footprints.
        """
        footprint = self.lca_client.get_footprint(hardware_model)

        # Number of refresh cycles over the simulation period
        num_cycles = int(years_to_simulate / refresh_interval_years) + 1

        total_carbon = 0.0
        total_rare_earth = 0.0
        total_mass = 0.0
        timeline = []

        for cycle in range(num_cycles):
            year = cycle * refresh_interval_years
            if year > years_to_simulate:
                break
            # Each cycle replaces the quantity of hardware
            total_carbon += footprint.embodied_carbon_kg * quantity
            total_rare_earth += footprint.rare_earth_kg * quantity
            total_mass += footprint.total_mass_kg * quantity
            timeline.append({
                "year": year,
                "carbon_kg": total_carbon,
                "rare_earth_kg": total_rare_earth,
                "mass_kg": total_mass,
            })

        # Calculate per‑year averages
        avg_carbon_per_year = total_carbon / years_to_simulate if years_to_simulate > 0 else 0

        return {
            "hardware_model": hardware_model,
            "quantity": quantity,
            "years_to_simulate": years_to_simulate,
            "refresh_interval_years": refresh_interval_years,
            "total_carbon_kg": total_carbon,
            "total_rare_earth_kg": total_rare_earth,
            "total_mass_kg": total_mass,
            "avg_carbon_per_year": avg_carbon_per_year,
            "timeline": timeline,
        }

    def compare_refresh_strategies(
        self,
        hardware_model: str,
        quantity: int = 1,
        strategies: List[Dict[str, float]] = None,
        years_to_simulate: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Compare different refresh strategies (e.g., different lifetimes or intervals).
        """
        if strategies is None:
            strategies = [
                {"lifetime": 3, "interval": 3},
                {"lifetime": 5, "interval": 5},
                {"lifetime": 7, "interval": 7},
            ]
        results = []
        for strat in strategies:
            lifetime = strat.get("lifetime", 5)
            interval = strat.get("interval", 3)
            sim = self.simulate_refresh_cycle(
                hardware_model=hardware_model,
                quantity=quantity,
                lifetime_years=lifetime,
                refresh_interval_years=interval,
                years_to_simulate=years_to_simulate,
            )
            results.append(sim)
        return results


# ============================================================================
# 7. SUSTAINABILITY COST FUNCTION EXTENSION
# ============================================================================
class MaterialAwareSustainabilityCostFunction:
    """
    Extended sustainability cost function that incorporates material indices
    into the overall cost calculation.
    """

    def __init__(self, lca_client: LCAClient = None):
        self.lca_client = lca_client or LCAClient()
        # Weightings for different components (adjust as needed)
        self.weights = {
            "embodied_carbon": 0.3,
            "rare_earth": 0.4,
            "operational_energy": 0.2,
            "water_usage": 0.1,
        }

    def compute_cost(
        self,
        hardware_model: str,
        operational_energy_joules: float = 0.0,
        lifetime_years: float = 5.0,
    ) -> float:
        """
        Compute a sustainability cost score for a node based on material footprint
        and operational energy.
        Higher cost = less sustainable.
        """
        footprint = self.lca_client.get_footprint(hardware_model)

        # Normalize each metric to a 0‑1 scale (simple scaling using thresholds)
        carbon_score = min(footprint.embodied_carbon_kg / 100.0, 1.0)
        rare_earth_score = min(footprint.rare_earth_kg / 0.01, 1.0)
        water_score = min(footprint.water_usage_l / 50.0, 1.0)

        # Operational energy (Joules) to carbon equivalent (very rough)
        # 1 kWh = 0.2 kg CO₂, 1 kWh = 3.6e6 J
        operational_carbon = operational_energy_joules / 3.6e6 * 0.2
        operational_score = min(operational_carbon / 10.0, 1.0)  # cap at 10 kg CO₂

        # Weighted sum
        total_cost = (
            self.weights["embodied_carbon"] * carbon_score +
            self.weights["rare_earth"] * rare_earth_score +
            self.weights["water_usage"] * water_score +
            self.weights["operational_energy"] * operational_score
        )

        return total_cost

    def material_index(self, hardware_model: str) -> float:
        """Return the material index (composite footprint) for the model."""
        footprint = self.lca_client.get_footprint(hardware_model)
        return footprint.material_index


# ============================================================================
# 8. CONVENIENCE FACTORY & INTEGRATION
# ============================================================================
def create_material_lca_integration(node_registry=None):
    """
    Factory to create all components and return them.
    Also patches the node_registry if provided.
    """
    lca_client = LCAClient()
    node_extension = None
    if node_registry:
        node_extension = NodeRegistryLCAExtension(node_registry, lca_client)
        # Optionally monkey‑patch the original register_node method
        original_register = node_registry.register_node
        def patched_register(node_id, hardware_model, **kwargs):
            return node_extension.register_node(node_id, hardware_model, **kwargs)
        node_registry.register_node = patched_register

    simulator = DigitalTwinMaterialSimulator(lca_client)
    cost_function = MaterialAwareSustainabilityCostFunction(lca_client)

    return {
        "lca_client": lca_client,
        "node_extension": node_extension,
        "simulator": simulator,
        "cost_function": cost_function,
        "node_registry": node_registry,
    }


# ============================================================================
# 9. EXAMPLE USAGE (if run directly)
# ============================================================================
if __name__ == "__main__":
    # Mock a simple NodeRegistry for demonstration
    class MockNodeRegistry:
        def register_node(self, node_id, hardware_model, **kwargs):
            # Return a descriptor with material_index field
            class Descriptor:
                pass
            desc = Descriptor()
            desc.node_id = node_id
            desc.hardware_model = hardware_model
            desc.material_index = None
            desc.material_footprint = None
            return desc

    registry = MockNodeRegistry()
    integration = create_material_lca_integration(registry)

    # Register a node (this will fetch footprint automatically)
    node_desc = registry.register_node("node-001", "NVIDIA A100")
    print(f"Node material index: {node_desc.material_index}")
    print(f"Footprint: {node_desc.material_footprint.to_dict()}")

    # Simulate refresh cycles
    sim_result = integration["simulator"].simulate_refresh_cycle(
        hardware_model="NVIDIA A100",
        quantity=10,
        refresh_interval_years=4,
        years_to_simulate=12,
    )
    print("\nSimulation results:")
    for key, val in sim_result.items():
        if key != "timeline":
            print(f"  {key}: {val}")

    # Compare strategies
    strategies = [
        {"interval": 3, "lifetime": 3},
        {"interval": 5, "lifetime": 5},
        {"interval": 7, "lifetime": 7},
    ]
    comparisons = integration["simulator"].compare_refresh_strategies(
        hardware_model="NVIDIA A100",
        quantity=10,
        strategies=strategies,
        years_to_simulate=10,
    )
    print("\nStrategy comparison (total carbon):")
    for s in comparisons:
        print(f"  interval={s['refresh_interval_years']}y: {s['total_carbon_kg']:.1f} kg CO₂")

    # Compute cost function
    cost = integration["cost_function"].compute_cost(
        hardware_model="NVIDIA A100",
        operational_energy_joules=1e6,  # 1 MJ
    )
    print(f"\nSustainability cost score: {cost:.3f}")
