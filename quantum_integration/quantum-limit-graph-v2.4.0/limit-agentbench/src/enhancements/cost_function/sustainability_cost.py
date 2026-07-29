# src/enhancements/cost_function/sustainability_cost.py
"""
Enhanced Sustainability Cost Function v2.1.0
=============================================
Multi‑objective sustainability cost function using real data, with adaptive weights,
carbon intensity caching, helium connectivity, material footprint, and optional
integration with anomaly detection and predictive maintenance.

ENHANCEMENTS OVER v2.0.0:
- Normalized each cost component to [0,1] scale with configurable baselines and maxima.
- Actual integration with anomaly detection and predictive maintenance.
- Carbon cache with LRU eviction and max size.
- Configurable normalization parameters for material cost.
- Improved error handling and fallbacks.
- Guarded Prometheus metrics access.
- Weight normalization in fallback config.
- Comprehensive docstrings and type hints.
"""

import asyncio
import logging
from typing import Dict, Any, Optional, Union, List
from datetime import datetime, timedelta
from collections import OrderedDict

# ---------- Local imports ----------
from ..schemas.node_descriptor import NodeDescriptor
from ..schemas.workload_descriptor import WorkloadDescriptor
from ..data_integration.carbon_intensity import CarbonIntensityFetcher
from ..data_integration.material_footprint import MaterialFootprintUpdater
from ..data_integration.helium_collector import HeliumCollector
from ..expert_registry import ExpertProfile  # optional

# ---------- Optional adaptive cost function ----------
try:
    from ..adaptive_cost_function import AdaptiveCostFunction
    ADAPTIVE_AVAILABLE = True
except ImportError:
    ADAPTIVE_AVAILABLE = False

# ---------- Prometheus metrics ----------
try:
    from prometheus_client import Counter, Gauge, Histogram
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

logger = logging.getLogger(__name__)

# ---------- Configuration (Pydantic) ----------
try:
    from pydantic import BaseModel, Field, validator
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

if PYDANTIC_AVAILABLE:
    class CostConfig(BaseModel):
        """Configuration for the sustainability cost function."""
        # Weights (initial)
        energy_weight: float = Field(0.2, ge=0, le=1)
        carbon_weight: float = Field(0.3, ge=0, le=1)
        helium_weight: float = Field(0.15, ge=0, le=1)
        material_weight: float = Field(0.15, ge=0, le=1)
        latency_weight: float = Field(0.1, ge=0, le=1)
        accuracy_weight: float = Field(0.1, ge=0, le=1)
        # Normalization baselines and maxima
        latency_baseline_ms: float = Field(1000.0, gt=0)
        latency_max_ms: float = Field(5000.0, gt=0)
        accuracy_baseline: float = Field(0.9, gt=0, le=1)
        accuracy_max: float = Field(1.0, gt=0, le=1)
        energy_baseline_joules: float = Field(0.0001, gt=0)
        energy_max_joules: float = Field(0.001, gt=0)
        carbon_intensity_baseline_kg_per_kwh: float = Field(0.4, gt=0)
        carbon_intensity_max_kg_per_kwh: float = Field(1.0, gt=0)
        helium_scarcity_threshold: float = Field(0.7, ge=0, le=1)
        helium_max_scarcity: float = Field(1.0, ge=0, le=1)
        material_embodied_norm: float = Field(200.0, gt=0)  # kg CO2 equivalent
        material_rare_earth_norm: float = Field(0.01, gt=0)  # kg
        material_max_composite: float = Field(1.0, gt=0)
        # Carbon caching
        carbon_cache_ttl_seconds: int = Field(300, ge=0)
        carbon_cache_max_size: int = Field(100, ge=1)
        # Integration flags
        use_adaptive_weights: bool = Field(False)
        integrate_anomaly_detection: bool = Field(False)
        integrate_predictive_maintenance: bool = Field(False)

        @validator('energy_weight', 'carbon_weight', 'helium_weight', 'material_weight', 'latency_weight', 'accuracy_weight')
        def weights_sum_one(cls, v, values):
            weights = [v] + [values.get(k, 0) for k in ['carbon_weight', 'helium_weight', 'material_weight', 'latency_weight', 'accuracy_weight']]
            total = sum(weights)
            if abs(total - 1.0) > 1e-6:
                raise ValueError("All weights must sum to 1")
            return v

        class Config:
            env_prefix = "COST_"
else:
    # Fallback dict with normalization parameters included
    COST_CONFIG = {
        "energy_weight": 0.2,
        "carbon_weight": 0.3,
        "helium_weight": 0.15,
        "material_weight": 0.15,
        "latency_weight": 0.1,
        "accuracy_weight": 0.1,
        "latency_baseline_ms": 1000.0,
        "latency_max_ms": 5000.0,
        "accuracy_baseline": 0.9,
        "accuracy_max": 1.0,
        "energy_baseline_joules": 0.0001,
        "energy_max_joules": 0.001,
        "carbon_intensity_baseline_kg_per_kwh": 0.4,
        "carbon_intensity_max_kg_per_kwh": 1.0,
        "helium_scarcity_threshold": 0.7,
        "helium_max_scarcity": 1.0,
        "material_embodied_norm": 200.0,
        "material_rare_earth_norm": 0.01,
        "material_max_composite": 1.0,
        "carbon_cache_ttl_seconds": 300,
        "carbon_cache_max_size": 100,
        "use_adaptive_weights": False,
        "integrate_anomaly_detection": False,
        "integrate_predictive_maintenance": False,
    }


class SustainabilityCostFunction:
    """
    Enhanced multi‑objective sustainability cost function.

    Computes a weighted sum of six normalized cost components (each in [0,1]):
        - Energy: joules per token * tokens, normalized by max energy.
        - Carbon: energy * carbon intensity, normalized by max carbon.
        - Helium: inverse of connectivity score, adjusted by scarcity, normalized by max.
        - Material: composite of embodied carbon and rare earth, normalized.
        - Latency: latency target normalized by baseline and max.
        - Accuracy: 1 - accuracy score, normalized by max.

    Supports:
        - Adaptive weights via an injected AdaptiveCostFunction.
        - Caching of carbon intensity to reduce API calls (LRU with size limit).
        - Integration with anomaly detection (adjusts weights on anomaly).
        - Integration with predictive maintenance (accounts for node efficiency degradation).
        - Prometheus metrics for each cost component (optional).
    """

    def __init__(
        self,
        carbon_fetcher: CarbonIntensityFetcher,
        material_updater: MaterialFootprintUpdater,
        helium_collector: HeliumCollector,
        config: Optional[Union[Dict[str, Any], CostConfig]] = None,
        adaptive_cost_function: Optional['AdaptiveCostFunction'] = None,
        anomaly_detector: Optional[Any] = None,
        predictive_maintenance: Optional[Any] = None,
    ):
        """
        Initialize the cost function.

        Args:
            carbon_fetcher: Carbon intensity data source.
            material_updater: Material footprint data source.
            helium_collector: Helium connectivity data source.
            config: Configuration (dict or Pydantic model).
            adaptive_cost_function: Optional adaptive cost function for dynamic weights.
            anomaly_detector: Optional anomaly detection module (should have `is_anomaly_active()`).
            predictive_maintenance: Optional predictive maintenance engine (should have `get_efficiency_factor(node_id)`).
        """
        # Configuration
        if config is None:
            if PYDANTIC_AVAILABLE:
                self.config = CostConfig()
            else:
                self.config = COST_CONFIG.copy()
        elif isinstance(config, dict):
            if PYDANTIC_AVAILABLE:
                self.config = CostConfig(**config)
            else:
                self.config = config.copy()
        else:
            self.config = config

        # Dependencies
        self.carbon = carbon_fetcher
        self.material = material_updater
        self.helium = helium_collector
        self.adaptive_cost = adaptive_cost_function
        self.anomaly_detector = anomaly_detector
        self.predictive_maintenance = predictive_maintenance

        # Weights (initial from config)
        self._weights = self._get_initial_weights()

        # Carbon intensity cache (LRU)
        self._carbon_cache: OrderedDict[str, Tuple[float, datetime]] = OrderedDict()
        self._carbon_cache_ttl = self._get_config('carbon_cache_ttl_seconds', 300)
        self._carbon_cache_max_size = self._get_config('carbon_cache_max_size', 100)

        # Metrics (Prometheus)
        if PROMETHEUS_AVAILABLE:
            self.metrics = {
                'energy': Histogram('cost_energy', 'Energy cost component (normalized)'),
                'carbon': Histogram('cost_carbon', 'Carbon cost component (normalized)'),
                'helium': Histogram('cost_helium', 'Helium cost component (normalized)'),
                'material': Histogram('cost_material', 'Material cost component (normalized)'),
                'latency': Histogram('cost_latency', 'Latency cost component (normalized)'),
                'accuracy': Histogram('cost_accuracy', 'Accuracy cost component (normalized)'),
                'total': Histogram('cost_total', 'Total sustainability cost'),
                'weights': Gauge('cost_weights', 'Current weights', ['component']),
                'anomaly_triggered': Counter('cost_anomaly_triggered', 'Anomaly triggered weight adjustments'),
            }
        else:
            self.metrics = None

        # State for anomaly cooldown
        self._last_anomaly_time: Optional[datetime] = None
        self._anomaly_cooldown = timedelta(seconds=300)

        logger.info("SustainabilityCostFunction v2.1.0 initialized with config: %s", self.config)

    def _get_config(self, key: str, default: Any = None) -> Any:
        """Safely get a config value, supporting both dict and Pydantic."""
        if hasattr(self.config, 'dict'):
            return getattr(self.config, key, default)
        return self.config.get(key, default)

    def _get_initial_weights(self) -> Dict[str, float]:
        """Extract initial weights from config and ensure they sum to 1."""
        weights = {
            'energy': self._get_config('energy_weight', 0.2),
            'carbon': self._get_config('carbon_weight', 0.3),
            'helium': self._get_config('helium_weight', 0.15),
            'material': self._get_config('material_weight', 0.15),
            'latency': self._get_config('latency_weight', 0.1),
            'accuracy': self._get_config('accuracy_weight', 0.1),
        }
        total = sum(weights.values())
        if total != 1.0 and total > 0:
            weights = {k: v / total for k, v in weights.items()}
        return weights

    # ---------- Normalization helpers ----------
    def _normalize_energy(self, energy_joules: float) -> float:
        """Normalize energy cost to [0,1]."""
        baseline = self._get_config('energy_baseline_joules', 0.0001)
        max_val = self._get_config('energy_max_joules', 0.001)
        # Clamp and scale
        val = max(0.0, min(max_val, energy_joules))
        if max_val == baseline:
            return 0.0
        return (val - baseline) / (max_val - baseline)

    def _normalize_carbon(self, carbon_kg: float) -> float:
        """Normalize carbon cost to [0,1]."""
        baseline = self._get_config('carbon_intensity_baseline_kg_per_kwh', 0.4)
        max_val = self._get_config('carbon_intensity_max_kg_per_kwh', 1.0)
        val = max(0.0, min(max_val, carbon_kg))
        if max_val == baseline:
            return 0.0
        return (val - baseline) / (max_val - baseline)

    def _normalize_helium(self, helium_cost: float) -> float:
        """Normalize helium cost to [0,1]."""
        max_val = self._get_config('helium_max_scarcity', 1.0)
        val = max(0.0, min(max_val, helium_cost))
        return val / max_val if max_val > 0 else 0.0

    def _normalize_material(self, material_composite: float) -> float:
        """Normalize material cost to [0,1]."""
        max_val = self._get_config('material_max_composite', 1.0)
        val = max(0.0, min(max_val, material_composite))
        return val / max_val if max_val > 0 else 0.0

    def _normalize_latency(self, latency_ms: float) -> float:
        """Normalize latency cost to [0,1]."""
        baseline = self._get_config('latency_baseline_ms', 1000.0)
        max_val = self._get_config('latency_max_ms', 5000.0)
        val = max(0.0, min(max_val, latency_ms))
        if max_val == baseline:
            return 0.0
        return (val - baseline) / (max_val - baseline)

    def _normalize_accuracy(self, accuracy: float) -> float:
        """Normalize accuracy cost to [0,1] (higher accuracy => lower cost)."""
        baseline = self._get_config('accuracy_baseline', 0.9)
        max_val = self._get_config('accuracy_max', 1.0)
        # We want cost = 0 when accuracy >= max, cost = 1 when accuracy <= baseline
        val = max(baseline, min(max_val, accuracy))
        if max_val == baseline:
            return 0.0
        return (max_val - val) / (max_val - baseline)

    # ---------- Core computation ----------
    async def compute(
        self,
        node_desc: NodeDescriptor,
        workload: WorkloadDescriptor,
        expert_profile: Optional[ExpertProfile] = None,
    ) -> float:
        """
        Compute the sustainability cost for a given node and workload.

        Args:
            node_desc: Node descriptor.
            workload: Workload descriptor.
            expert_profile: Optional expert profile (for accuracy and efficiency).

        Returns:
            Total sustainability cost (lower is better).
        """
        # --- Energy cost ---
        # Apply predictive maintenance efficiency factor if enabled
        if self._get_config('integrate_predictive_maintenance', False) and self.predictive_maintenance:
            try:
                eff_factor = await self.predictive_maintenance.get_efficiency_factor(node_desc.id)
                if eff_factor is not None and eff_factor > 0:
                    energy_used = node_desc.energy_per_token * workload.tokens / eff_factor
                else:
                    energy_used = node_desc.energy_per_token * workload.tokens
            except Exception as e:
                logger.warning(f"Predictive maintenance efficiency factor failed: {e}")
                energy_used = node_desc.energy_per_token * workload.tokens
        else:
            energy_used = node_desc.energy_per_token * workload.tokens

        energy_cost = self._normalize_energy(energy_used)
        if PROMETHEUS_AVAILABLE and self.metrics:
            self.metrics['energy'].observe(energy_cost)

        # --- Carbon cost ---
        carbon_intensity = await self._get_carbon_intensity(node_desc.region)
        carbon_kg = energy_used * carbon_intensity
        carbon_cost = self._normalize_carbon(carbon_kg)
        if PROMETHEUS_AVAILABLE and self.metrics:
            self.metrics['carbon'].observe(carbon_cost)

        # --- Helium cost ---
        helium_scarcity = await self._get_helium_scarcity(node_desc)
        helium_base = (1 - node_desc.helium_connectivity_score) * 0.5
        if helium_scarcity > self._get_config('helium_scarcity_threshold', 0.7):
            helium_base *= (1 + helium_scarcity)
        helium_cost = self._normalize_helium(helium_base)
        if PROMETHEUS_AVAILABLE and self.metrics:
            self.metrics['helium'].observe(helium_cost)

        # --- Material cost ---
        material_composite = await self._get_material_composite(node_desc)
        material_cost = self._normalize_material(material_composite)
        if PROMETHEUS_AVAILABLE and self.metrics:
            self.metrics['material'].observe(material_cost)

        # --- Latency cost ---
        latency_cost = self._normalize_latency(workload.latency_target)
        if PROMETHEUS_AVAILABLE and self.metrics:
            self.metrics['latency'].observe(latency_cost)

        # --- Accuracy cost ---
        if expert_profile:
            acc = expert_profile.accuracy_score
        else:
            acc = self._get_config('accuracy_baseline', 0.9)
        accuracy_cost = self._normalize_accuracy(acc)
        if PROMETHEUS_AVAILABLE and self.metrics:
            self.metrics['accuracy'].observe(accuracy_cost)

        # --- Get current weights (possibly adaptive) ---
        weights = await self._get_weights()

        # --- Total cost ---
        total = (
            weights['energy'] * energy_cost +
            weights['carbon'] * carbon_cost +
            weights['helium'] * helium_cost +
            weights['material'] * material_cost +
            weights['latency'] * latency_cost +
            weights['accuracy'] * accuracy_cost
        )
        if PROMETHEUS_AVAILABLE and self.metrics:
            self.metrics['total'].observe(total)
            for k, v in weights.items():
                self.metrics['weights'].labels(component=k).set(v)

        logger.debug(
            "Cost components (normalized): energy=%.4f, carbon=%.4f, helium=%.4f, material=%.4f, latency=%.4f, accuracy=%.4f, total=%.4f",
            energy_cost, carbon_cost, helium_cost, material_cost, latency_cost, accuracy_cost, total
        )
        return total

    # ---------- Helper methods ----------
    async def _get_carbon_intensity(self, region: str) -> float:
        """Fetch carbon intensity with LRU caching."""
        now = datetime.now()
        # Check cache
        if region in self._carbon_cache:
            value, timestamp = self._carbon_cache[region]
            if (now - timestamp).total_seconds() < self._carbon_cache_ttl:
                # Move to end (mark as recently used)
                self._carbon_cache.move_to_end(region)
                return value
            else:
                # Remove expired entry
                del self._carbon_cache[region]

        # Fetch fresh
        try:
            intensity = await self.carbon.get_intensity(region)
        except Exception as e:
            logger.error(f"Carbon intensity fetch failed: {e}")
            intensity = self._get_config('carbon_intensity_baseline_kg_per_kwh', 0.4)

        # Store in cache with LRU
        self._carbon_cache[region] = (intensity, now)
        if len(self._carbon_cache) > self._carbon_cache_max_size:
            self._carbon_cache.popitem(last=False)  # remove oldest
        return intensity

    async def _get_helium_scarcity(self, node_desc: NodeDescriptor) -> float:
        """Get helium scarcity index for the node's region."""
        # For now, use helium_connectivity_score as proxy.
        # In future, could query a scarcity API.
        return 1.0 - node_desc.helium_connectivity_score

    async def _get_material_composite(self, node_desc: NodeDescriptor) -> float:
        """Fetch material composite (embodied carbon + rare earth) and normalize."""
        if not node_desc.material_footprint_id:
            return 0.0
        fp = self.material.get_footprint(node_desc.material_footprint_id)
        if not fp:
            return 0.0
        # Composite: embodied carbon + rare earth (weighted)
        embodied = fp.get('embodied_carbon_kg', 0)
        rare_earth = fp.get('rare_earth_kg', 0)
        # Normalize each component
        embodied_norm = self._get_config('material_embodied_norm', 200.0)
        rare_earth_norm = self._get_config('material_rare_earth_norm', 0.01)
        normalized_embodied = embodied / embodied_norm if embodied_norm > 0 else 0.0
        normalized_rare = rare_earth / rare_earth_norm if rare_earth_norm > 0 else 0.0
        # Weighted composite (0.7 embodied, 0.3 rare earth)
        composite = (normalized_embodied * 0.7 + normalized_rare * 0.3)
        # Clamp to [0, max]
        max_composite = self._get_config('material_max_composite', 1.0)
        return min(max_composite, composite)

    async def _get_weights(self) -> Dict[str, float]:
        """
        Return current weights.
        If adaptive cost function is available and enabled, use it.
        Also adjust weights if anomaly detected (with cooldown).
        """
        # Start with current weights (may have been adjusted by anomaly)
        weights = self._weights.copy()

        # Apply adaptive weights (if enabled)
        if (self._get_config('use_adaptive_weights', False) and
                self.adaptive_cost and ADAPTIVE_AVAILABLE):
            try:
                if hasattr(self.adaptive_cost, 'get_weights'):
                    adaptive_weights = await self.adaptive_cost.get_weights()
                else:
                    adaptive_weights = self.adaptive_cost.weights
                # Map adaptive keys to cost components
                mapping = {
                    'alpha': 'energy',
                    'beta': 'carbon',
                    'gamma': 'helium',
                    'delta': 'material',
                    'epsilon': 'latency',
                    'zeta': 'accuracy',
                }
                for ad_key, comp in mapping.items():
                    if ad_key in adaptive_weights:
                        weights[comp] = adaptive_weights[ad_key]
                # Normalize
                total = sum(weights.values())
                if total > 0:
                    weights = {k: v / total for k, v in weights.items()}
            except Exception as e:
                logger.warning(f"Adaptive weight update failed: {e}")

        # Adjust weights if anomaly detected (cooldown)
        if (self._get_config('integrate_anomaly_detection', False) and
                self.anomaly_detector):
            now = datetime.now()
            # Check if enough time has passed since last anomaly adjustment
            if (not self._last_anomaly_time or
                    (now - self._last_anomaly_time) > self._anomaly_cooldown):
                try:
                    if hasattr(self.anomaly_detector, 'is_anomaly_active'):
                        is_anomaly = await self.anomaly_detector.is_anomaly_active()
                        if is_anomaly:
                            # Increase carbon and energy weights
                            weights['carbon'] = min(0.5, weights['carbon'] * 1.2)
                            weights['energy'] = min(0.4, weights['energy'] * 1.1)
                            total = sum(weights.values())
                            if total > 0:
                                weights = {k: v / total for k, v in weights.items()}
                            self._last_anomaly_time = now
                            if PROMETHEUS_AVAILABLE and self.metrics:
                                self.metrics['anomaly_triggered'].inc()
                            logger.info(f"Anomaly detected, weights adjusted: {weights}")
                except Exception as e:
                    logger.warning(f"Anomaly detection integration failed: {e}")

        return weights

    async def on_anomaly_detected(self, anomaly_severity: float):
        """
        Callback from anomaly detection module.
        Adjusts weights temporarily based on severity.
        """
        if not self._get_config('integrate_anomaly_detection', False):
            return
        self._last_anomaly_time = datetime.now()
        # Increase carbon and energy weights based on severity
        factor = 1.0 + anomaly_severity * 0.5  # severity 0-1 -> factor 1.0-1.5
        self._weights['carbon'] = min(0.5, self._weights['carbon'] * factor)
        self._weights['energy'] = min(0.4, self._weights['energy'] * factor * 0.8)
        # Reduce other weights proportionally
        total = sum(self._weights.values())
        if total > 0:
            self._weights = {k: v / total for k, v in self._weights.items()}
        logger.info(f"Anomaly detected, weights adjusted: {self._weights}")

    async def update_from_predictive_maintenance(self, node_id: str, efficiency_factor: float):
        """
        Update cost based on predictive maintenance feedback.
        """
        if not self._get_config('integrate_predictive_maintenance', False):
            return
        # efficiency_factor: 1.0 = normal, <1.0 = degraded
        # This method can be used to adjust energy_per_token on the fly.
        # Since we don't store node-specific state here, we simply log.
        logger.debug(f"Predictive maintenance update for node {node_id}: factor={efficiency_factor}")

    # ---------- Utility methods ----------
    async def get_weights(self) -> Dict[str, float]:
        """Return current weights."""
        return await self._get_weights()

    async def set_weights(self, new_weights: Dict[str, float]) -> None:
        """Manually set weights (e.g., via API)."""
        total = sum(new_weights.values())
        if total == 0:
            raise ValueError("Weights sum cannot be zero")
        self._weights = {k: v / total for k, v in new_weights.items()}
        logger.info(f"Manual weights set: {self._weights}")

    async def reset_weights(self) -> None:
        """Reset weights to initial config values."""
        self._weights = self._get_initial_weights()
        logger.info("Weights reset to initial configuration")

    async def reset_carbon_cache(self) -> None:
        """Clear the carbon intensity cache."""
        self._carbon_cache.clear()
        logger.info("Carbon cache cleared")

    async def get_cost_breakdown(
        self,
        node_desc: NodeDescriptor,
        workload: WorkloadDescriptor,
        expert_profile: Optional[ExpertProfile] = None,
    ) -> Dict[str, Any]:
        """
        Return a breakdown of cost components (for dashboard/explanation).
        """
        energy_used = node_desc.energy_per_token * workload.tokens
        energy_cost = self._normalize_energy(energy_used)
        carbon_intensity = await self._get_carbon_intensity(node_desc.region)
        carbon_kg = energy_used * carbon_intensity
        carbon_cost = self._normalize_carbon(carbon_kg)
        helium_scarcity = await self._get_helium_scarcity(node_desc)
        helium_base = (1 - node_desc.helium_connectivity_score) * 0.5
        if helium_scarcity > self._get_config('helium_scarcity_threshold', 0.7):
            helium_base *= (1 + helium_scarcity)
        helium_cost = self._normalize_helium(helium_base)
        material_composite = await self._get_material_composite(node_desc)
        material_cost = self._normalize_material(material_composite)
        latency_cost = self._normalize_latency(workload.latency_target)
        if expert_profile:
            acc = expert_profile.accuracy_score
        else:
            acc = self._get_config('accuracy_baseline', 0.9)
        accuracy_cost = self._normalize_accuracy(acc)
        weights = await self._get_weights()
        total = (
            weights['energy'] * energy_cost +
            weights['carbon'] * carbon_cost +
            weights['helium'] * helium_cost +
            weights['material'] * material_cost +
            weights['latency'] * latency_cost +
            weights['accuracy'] * accuracy_cost
        )
        return {
            'energy': {'raw': energy_used, 'normalized': energy_cost},
            'carbon': {'raw': carbon_kg, 'normalized': carbon_cost},
            'helium': {'raw': helium_base, 'normalized': helium_cost},
            'material': {'raw': material_composite, 'normalized': material_cost},
            'latency': {'raw': workload.latency_target, 'normalized': latency_cost},
            'accuracy': {'raw': acc, 'normalized': accuracy_cost},
            'total': total,
            'weights': weights,
        }

    async def close(self):
        """Clean up resources (if any)."""
        # No resources to close currently.
        pass


# ============================================================================
# Convenience factory
# ============================================================================
def create_cost_function(
    carbon_fetcher: CarbonIntensityFetcher,
    material_updater: MaterialFootprintUpdater,
    helium_collector: HeliumCollector,
    config: Optional[Dict[str, Any]] = None,
    adaptive_cost_function: Optional[Any] = None,
    anomaly_detector: Optional[Any] = None,
    predictive_maintenance: Optional[Any] = None,
) -> SustainabilityCostFunction:
    """
    Factory to create a fully configured SustainabilityCostFunction.
    """
    return SustainabilityCostFunction(
        carbon_fetcher=carbon_fetcher,
        material_updater=material_updater,
        helium_collector=helium_collector,
        config=config,
        adaptive_cost_function=adaptive_cost_function,
        anomaly_detector=anomaly_detector,
        predictive_maintenance=predictive_maintenance,
    )


# ============================================================================
# Example usage (if run directly)
# ============================================================================
if __name__ == "__main__":
    import asyncio
    import sys
    sys.path.append('../')  # Allow imports

    # Mock dependencies for testing
    class MockCarbonFetcher:
        async def get_intensity(self, region: str) -> float:
            return 0.42

    class MockMaterialUpdater:
        def get_footprint(self, product_id: str) -> Dict:
            return {'embodied_carbon_kg': 200, 'rare_earth_kg': 0.01}

    class MockHeliumCollector:
        async def get_connectivity_score(self, hotspot_id: str) -> float:
            return 0.8

    class MockExpertProfile:
        def __init__(self):
            self.accuracy_score = 0.95

    async def main():
        carbon = MockCarbonFetcher()
        material = MockMaterialUpdater()
        helium = MockHeliumCollector()
        cost_func = create_cost_function(carbon, material, helium)

        node_desc = NodeDescriptor(
            id="test",
            type="edge",
            region="us-east",
            region_carbon_intensity=0.42,
            energy_per_token=0.00005,
            helium_connectivity_score=0.9,
            material_footprint_id="gpu-a100"
        )
        workload = WorkloadDescriptor(
            task_type="inference",
            tokens=512,
            latency_target=200.0,
            sector_emission_factor=0.03,
            bio_mode="none",
            priority="balanced"
        )
        expert = MockExpertProfile()
        cost = await cost_func.compute(node_desc, workload, expert)
        print(f"Total cost: {cost}")
        breakdown = await cost_func.get_cost_breakdown(node_desc, workload, expert)
        print("Cost breakdown:", breakdown)

    asyncio.run(main())
