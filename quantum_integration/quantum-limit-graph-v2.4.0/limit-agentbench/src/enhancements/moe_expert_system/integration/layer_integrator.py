#!/usr/bin/env python3
"""
Enhanced Layer Integrator v7.2.0 – Production‑ready with full bio‑inspired core integration and MOPD support.

Key enhancements over v7.1.0:
- Added MOPD (Multi‑Objective Pareto Decision) framework.
- New MOPDPlan dataclass to represent execution strategies.
- Pareto front generation for layer execution alternatives.
- Selection of best plan via scalarisation with configurable weights.
- Extended build_context to include MOPD‑relevant parameters.
- New method call_layer_with_mopd for Pareto‑aware calls.
- Telemetry tracks MOPD usage.
- Full backward compatibility.
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Set, Callable, Union
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from enum import Enum
import numpy as np
import hashlib
import json
import time
import inspect
import functools
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
import aiohttp
import os
import uuid
import zlib
import pickle
import sys

logger = logging.getLogger(__name__)

# ============================================================================
# Bio-Inspired Core Import (with fallback)
# ============================================================================
try:
    from enhancements.bio_inspired.__init__ import EnhancedBioInspiredCore, BioEvent, CircuitBreaker, Persistence
    from enhancements.bio_inspired.eco_atp_currency import (
        EcoATPTokenManager, DynamicExchangeRate, EcoATPSource, EcoATPConsumer,
        TokenState, EcoATPToken, EcoATPAccount
    )
    from enhancements.bio_inspired.proton_gradient_fields import (
        GradientFieldManager, GradientField
    )
    from enhancements.bio_inspired.atp_synthase_scheduler import (
        ATPSynthaseScheduler, SynthaseConfig
    )
    from enhancements.bio_inspired.chromatophore_compartments import (
        CompartmentManager, ChromatophoreCompartment, CompartmentState,
        MembranePermeability
    )
    from enhancements.bio_inspired.biomass_storage import (
        BiomassStorage, StorageTier, GuaranteeLevel
    )
    from enhancements.bio_inspired.photosynthetic_harvester import (
        PhotosyntheticHarvester
    )
    from enhancements.bio_inspired.time_tick_engine import TimeTickEngine
    from enhancements.bio_inspired.quantum_bridge import QuantumBridge
    BIO_INSPIRED_AVAILABLE = True
    logger.info("Bio-inspired core modules loaded for Layer Integrator")
except ImportError as e:
    BIO_INSPIRED_AVAILABLE = False
    logger.warning(f"Bio-inspired core modules not available: {str(e)} - using fallback")
    # Fallback definitions
    class BioEvent:
        def __init__(self, event_type, source, data=None):
            self.event_type = event_type
            self.source = source
            self.data = data or {}

    class CircuitBreaker:
        def __init__(self, name, failure_threshold=3, recovery_timeout=30.0):
            self.name = name
            self.failure_threshold = failure_threshold
            self.recovery_timeout = recovery_timeout
            self._state = "closed"
            self._failure_count = 0
            self._last_failure_time = None
            self._lock = asyncio.Lock()
        async def call(self, func, *args, **kwargs):
            return await func(*args, **kwargs)

# ============================================================================
# MoE Expert Router Import (optional)
# ============================================================================
try:
    from ..expert_router import ExpertRouter
    MOE_AVAILABLE = True
except ImportError:
    MOE_AVAILABLE = False
    logger.warning("MoE Expert Router not available - context building will be limited")

# ============================================================================
# Configuration Dataclass (Enhanced with MOPD)
# ============================================================================
@dataclass
class LayerIntegratorConfig:
    """Centralized configuration for the Layer Integrator."""
    # Feature flags
    enable_cache: bool = True
    enable_circuit_breaker: bool = True
    enable_retry: bool = True
    enable_events: bool = True
    enable_transactions: bool = True
    enable_monitoring: bool = True
    enable_bio_integration: bool = True
    enable_carbon_intensity: bool = True
    enable_predictive: bool = True
    enable_cross_domain: bool = True
    enable_sustainability_scoring: bool = True
    enable_dynamic_discovery: bool = True
    enable_event_correlation: bool = True
    enable_gradient_cache: bool = True
    enable_distributed_txns: bool = True
    enable_persistence: bool = True
    enable_event_driven: bool = True
    enable_self_healing: bool = True
    enable_mopd: bool = True               # NEW: MOPD feature flag

    # Tunable parameters
    cache_ttl_seconds: float = 60.0
    max_cache_size: int = 1000
    coordinator_id: str = "main_coordinator"
    carbon_api_region: str = "us-east"
    carbon_update_interval: int = 300
    discovery_interval: int = 60
    health_check_interval: int = 10
    max_retries: int = 3
    retry_base_delay_ms: float = 100.0
    retry_max_delay_ms: float = 5000.0
    circuit_breaker_failure_threshold: int = 5
    circuit_breaker_recovery_timeout: float = 30.0
    half_open_max_requests: int = 3
    transaction_timeout_seconds: float = 60.0
    token_reserve_factor: float = 10.0
    gradient_health_threshold: float = 0.6
    sustainability_weights: Dict[str, float] = field(default_factory=lambda: {
        'carbon_savings': 0.3,
        'helium_efficiency': 0.2,
        'renewable_usage': 0.2,
        'token_efficiency': 0.15,
        'layer_health': 0.15
    })
    persistence_path: str = "./layer_integrator_state.json.gz"
    self_healing_enabled: bool = True
    workflow_on_degradation: str = "repair_layer"

    # MOPD-specific parameters (NEW)
    mopd_objective_weights: Dict[str, float] = field(default_factory=lambda: {
        'carbon': 0.3,
        'helium': 0.2,
        'cost': 0.2,
        'latency': 0.15,
        'success_prob': 0.15,
    })
    mopd_grid_resolution: int = 5   # number of discrete alternatives for continuous variables

    def __post_init__(self):
        for key, value in self.__dict__.items():
            if isinstance(value, bool):
                setattr(self, key, bool(value))

# ============================================================================
# Enums and Data Classes (Enhanced)
# ============================================================================
class LayerStatus(Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    RECOVERING = "recovering"
    OFFLINE = "offline"
    MAINTENANCE = "maintenance"
    DISCOVERED = "discovered"

    def to_membrane_state(self):
        if not BIO_INSPIRED_AVAILABLE:
            return None
        mapping = {
            LayerStatus.HEALTHY: MembranePermeability.PERMEABLE,
            LayerStatus.DEGRADED: MembranePermeability.SELECTIVE,
            LayerStatus.UNHEALTHY: MembranePermeability.RESTRICTIVE,
            LayerStatus.RECOVERING: MembranePermeability.SELECTIVE,
            LayerStatus.OFFLINE: MembranePermeability.IMPERMEABLE,
            LayerStatus.MAINTENANCE: MembranePermeability.RESTRICTIVE,
            LayerStatus.DISCOVERED: MembranePermeability.SELECTIVE
        }
        return mapping.get(self)

class IntegrationMode(Enum):
    SYNCHRONOUS = "synchronous"
    ASYNCHRONOUS = "asynchronous"
    EVENT_DRIVEN = "event_driven"
    BATCH = "batch"
    STREAMING = "streaming"

class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"
    RECOVERING = "recovering"

@dataclass
class LayerInfo:
    # ... same as before ...
    layer_number: int
    layer_name: str
    version: str
    status: LayerStatus = LayerStatus.HEALTHY
    integration_mode: IntegrationMode = IntegrationMode.SYNCHRONOUS
    last_heartbeat: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    dependencies: List[int] = field(default_factory=list)
    capabilities: List[str] = field(default_factory=list)
    endpoints: Dict[str, str] = field(default_factory=dict)
    metrics: Dict[str, Any] = field(default_factory=dict)
    config: Dict[str, Any] = field(default_factory=dict)
    circuit_breaker: 'LayerCircuitBreaker' = None
    gradient_health: float = 0.7
    membrane_permeability: str = "selective"
    token_balance: float = 0.0
    harvester_vitality: float = 0.5
    entangled_layers: List[int] = field(default_factory=list)
    sustainability_score: float = 0.0
    carbon_savings_kg: float = 0.0
    discovery_timestamp: Optional[datetime] = None
    health_history: List[Dict] = field(default_factory=list)
    recovery_attempts: int = 0
    max_recovery_attempts: int = 5

    def __post_init__(self):
        if self.circuit_breaker is None:
            self.circuit_breaker = LayerCircuitBreaker(f"layer_{self.layer_number}")

@dataclass
class LayerCircuitBreaker:
    # ... same as before ...
    pass

@dataclass
class LayerEvent:
    # ... same as before ...
    pass

@dataclass
class CacheEntry:
    # ... same as before ...
    pass

@dataclass
class RetryConfig:
    # ... same as before ...
    pass

@dataclass
class TransactionContext:
    # ... same as before ...
    pass

# ============================================================================
# MOPDPlan - NEW Dataclass for Pareto execution plans
# ============================================================================
@dataclass
class MOPDPlan:
    """Represents a layer execution strategy with its computed objectives."""
    # Decision variables
    use_cache: bool
    use_quantum: bool
    data_center: str                  # 'us-east', 'us-west', etc.
    retry_strategy: str               # 'aggressive', 'moderate', 'conservative'
    token_allocation: float
    # Objectives (to be minimised/maximised)
    carbon_kg: float = 0.0
    helium_units: float = 0.0
    cost_usd: float = 0.0
    latency_ms: float = 0.0
    success_probability: float = 0.0
    # Scalarised score (will be computed later)
    scalarised_score: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MOPDPlan':
        return cls(**data)

# ============================================================================
# Serialization helpers (unchanged)
# ============================================================================
class DateTimeEncoder(json.JSONEncoder):
    # ... same as before ...
    pass

def json_decoder_hook(dct):
    # ... same as before ...
    pass

# ============================================================================
# Persistence (unchanged)
# ============================================================================
class LayerIntegratorPersistence:
    # ... same as before ...
    pass

# ============================================================================
# Carbon Intensity Manager (unchanged)
# ============================================================================
class CarbonIntensityManager:
    # ... same as before ...
    pass

# ============================================================================
# Predictive Layer Analyzer (unchanged)
# ============================================================================
class PredictiveLayerAnalyzer:
    # ... same as before ...
    pass

# ============================================================================
# Layer Cross-Domain Transfer (unchanged)
# ============================================================================
class LayerCrossDomainTransfer:
    # ... same as before ...
    pass

# ============================================================================
# Dynamic Layer Discovery Manager (unchanged)
# ============================================================================
class DynamicLayerDiscoveryManager:
    # ... same as before ...
    pass

# ============================================================================
# Event Correlation Engine (unchanged)
# ============================================================================
class EventCorrelationEngine:
    # ... same as before ...
    pass

# ============================================================================
# Gradient-Aware Cache Manager (unchanged)
# ============================================================================
class GradientAwareCacheManager:
    # ... same as before ...
    pass

# ============================================================================
# Distributed Transaction Coordinator (unchanged)
# ============================================================================
class DistributedTransactionCoordinator:
    # ... same as before ...
    pass

# ============================================================================
# Sustainability Score Calculator (unchanged)
# ============================================================================
class SustainabilityScoreCalculator:
    # ... same as before ...
    pass

# ============================================================================
# Telemetry Collector (unchanged)
# ============================================================================
class TelemetryCollector:
    # ... same as before ...
    pass

# ============================================================================
# Enhanced Layer Integrator (Main Class) – v7.2.0 with MOPD
# ============================================================================
class EnhancedLayerIntegrator:
    """
    Enhanced Layer Integrator v7.2.0 – Complete Green Agent Implementation with MOPD.
    """

    def __init__(
        self,
        bio_core: Optional[EnhancedBioInspiredCore] = None,
        config: Optional[Union[LayerIntegratorConfig, Dict[str, Any]]] = None,
        expert_router: Optional['ExpertRouter'] = None,
    ):
        # Load config (same as before, but includes MOPD fields)
        if isinstance(config, dict):
            self.config = LayerIntegratorConfig(**config)
        elif isinstance(config, LayerIntegratorConfig):
            self.config = config
        else:
            self.config = LayerIntegratorConfig()

        # Store bio‑core reference (same as before)
        self.bio_core = bio_core
        self.event_broker = None
        self.alert_system = None
        self.anomaly_detection = None
        self.cost_benefit_engine = None
        self.quantum_bridge = None
        self.tick_engine = None
        self.swarm_coordinator = None
        self.self_healer = None
        self.workflow_orchestrator = None
        self.token_manager = None
        self.gradient_manager = None
        self.scheduler = None
        self.compartment_manager = None
        self.biomass_storage = None
        self.harvester = None
        if self.bio_core:
            # ... extraction as before ...
            pass

        # Feature flags (including MOPD)
        self.enable_mopd = self.config.enable_mopd

        # Initialize components (same as before)
        self.carbon_manager = CarbonIntensityManager(self.config) if self.enable_carbon_intensity else None
        self.predictive_analyzer = PredictiveLayerAnalyzer(self.config) if self.enable_predictive else None
        self.cross_domain_transfer = LayerCrossDomainTransfer() if self.enable_cross_domain else None
        self.discovery_manager = DynamicLayerDiscoveryManager(self.config) if self.enable_dynamic_discovery else None
        self.event_correlation = EventCorrelationEngine(self.config) if self.enable_event_correlation else None
        self.gradient_cache = GradientAwareCacheManager(self.config) if self.enable_gradient_cache else None
        self.distributed_coordinator = DistributedTransactionCoordinator(self.config) if self.enable_distributed_txns else None
        self.sustainability_calculator = SustainabilityScoreCalculator(self.config) if self.enable_sustainability_scoring else None
        self.telemetry = TelemetryCollector() if self.enable_monitoring else None

        # Persistence
        self.persistence = LayerIntegratorPersistence(self.config.persistence_path) if self.enable_persistence else None

        # MoE integration
        self.expert_router = expert_router
        self.helium_provider = None
        self.fl_monitor = None

        # Layer registry (same as before)
        self.layers: Dict[int, LayerInfo] = {}
        self.layer_modules: Dict[int, Any] = {}
        self.integration_status: Dict[int, bool] = {i: False for i in range(12)}

        # Cache fallback
        self._simple_cache: Dict[str, CacheEntry] = {}
        self.cache_ttl = self.config.cache_ttl_seconds
        self.max_cache_size = self.config.max_cache_size

        # Event system
        self.event_subscribers: Dict[str, List[Callable]] = defaultdict(list)
        self.event_queue: asyncio.Queue = asyncio.Queue(maxsize=10000)

        # Retry config
        self.retry_config = RetryConfig(
            max_retries=self.config.max_retries,
            base_delay_ms=self.config.retry_base_delay_ms,
            max_delay_ms=self.config.retry_max_delay_ms
        )

        # Transactions
        self.active_transactions: Dict[str, TransactionContext] = {}

        # Performance metrics
        self.layer_latency: Dict[int, List[float]] = defaultdict(list)
        self.layer_errors: Dict[int, int] = defaultdict(int)
        self.layer_calls: Dict[int, int] = defaultdict(int)

        # Sustainability tracking
        self.total_carbon_savings_kg = 0.0
        self.total_helium_saved_l = 0.0
        self.total_energy_saved_kwh = 0.0
        self.sustainability_score = 0.0

        # Health status
        self.health_status = "healthy"
        self.last_error = None
        self.correlation_id = str(uuid.uuid4())

        # Thread pool
        self.executor = ThreadPoolExecutor(max_workers=4)

        # Initialize all 12 layers
        self._initialize_all_layers()

        # Load persisted state
        if self.persistence:
            asyncio.create_task(self._load_state_async())

        # Subscribe to core events
        if self.enable_event_driven and self.event_broker:
            self._subscribe_events()

        # Start background tasks
        self._start_background_tasks()

        logger.info(
            f"EnhancedLayerIntegrator v7.2.0 initialized: "
            f"mopd={self.enable_mopd}, "
            f"layers={len(self.layers)}/12, "
            f"bio_integration={self.enable_bio_integration}, "
            f"carbon_intensity={self.enable_carbon_intensity}, "
            f"predictive={self.enable_predictive}, "
            f"dynamic_discovery={self.enable_dynamic_discovery}, "
            f"event_correlation={self.enable_event_correlation}, "
            f"gradient_cache={self.enable_gradient_cache}, "
            f"distributed_txns={self.enable_distributed_txns}, "
            f"sustainability_scoring={self.enable_sustainability_scoring}, "
            f"persistence={self.enable_persistence}, "
            f"event_driven={self.enable_event_driven}"
        )

    # ============================================================================
    # Layer Initialization (unchanged)
    # ============================================================================
    def _initialize_all_layers(self):
        # ... same as before ...
        pass

    def _get_layer_capabilities(self, layer_number: int) -> List[str]:
        # ... same as before ...
        pass

    # ============================================================================
    # Update Boundaries (unchanged)
    # ============================================================================
    def _update_boundaries(self):
        # ... same as before ...
        pass

    # ============================================================================
    # Event Subscriptions (unchanged)
    # ============================================================================
    def _subscribe_events(self):
        # ... same as before ...
        pass

    # ============================================================================
    # Persistence Methods (unchanged)
    # ============================================================================
    async def _load_state_async(self):
        # ... same as before ...
        pass

    async def _save_state(self):
        # ... same as before ...
        pass

    # ============================================================================
    # Background Tasks (unchanged)
    # ============================================================================
    def _start_background_tasks(self):
        # ... same as before ...
        pass

    # ============================================================================
    # Injection Methods (unchanged)
    # ============================================================================
    def inject_bio_core(self, bio_core: Any = None, **kwargs):
        # ... same as before ...
        pass

    def set_expert_router(self, router: 'ExpertRouter'):
        # ... same as before ...
        pass

    def set_helium_provider(self, provider):
        # ... same as before ...
        pass

    def set_fl_monitor(self, fl_monitor):
        # ... same as before ...
        pass

    # ============================================================================
    # Context Builder (Enhanced with MOPD info)
    # ============================================================================
    async def build_context(self) -> Dict[str, Any]:
        """Build a comprehensive context for MoE expert router, including MOPD parameters."""
        context = {}

        # 1. Helium telemetry
        if self.helium_provider:
            context['helium_scarcity'] = self.helium_provider.get_scarcity()
            context['helium_cost_index'] = self.helium_provider.get_cost_index()
            context['avg_client_energy'] = self.helium_provider.get_avg_client_energy()
        else:
            context['helium_scarcity'] = 0.5
            context['helium_cost_index'] = 1.0
            context['avg_client_energy'] = 0.5

        # 2. Carbon intensity
        if self.enable_carbon_intensity and self.carbon_manager:
            carbon_intensity = await self.carbon_manager.get_current_intensity()
            carbon_price = await self.carbon_manager.get_current_price()
            context['carbon_intensity'] = carbon_intensity / 1000.0
            context['carbon_price_usd'] = carbon_price
        else:
            context['carbon_intensity'] = 0.5
            context['carbon_price_usd'] = 50.0

        # 3. Bio-inspired signals
        gradients = self._get_real_gradient_levels()
        context['gradient_carbon'] = gradients.get('carbon', 0.5)
        context['gradient_helium'] = gradients.get('helium', 0.5)
        context['gradient_trust'] = gradients.get('trust', 0.5)
        context['gradient_opportunity'] = gradients.get('opportunity', 0.5)
        context['token_balance_norm'] = self._get_real_token_availability()
        context['harvester_stress'] = self._get_harvester_vitality()
        context['avg_layer_health'] = np.mean([info.gradient_health for info in self.layers.values()])

        # 4. Federated Learning metrics
        if self.fl_monitor:
            context['model_loss'] = self.fl_monitor.get_loss()
            context['gradient_variance'] = self.fl_monitor.get_gradient_variance()
            context['accuracy'] = self.fl_monitor.get_accuracy()
        else:
            context['model_loss'] = 0.0
            context['gradient_variance'] = 0.0
            context['accuracy'] = 0.0

        # 5. Sustainability
        context['sustainability_score'] = self.sustainability_score
        context['carbon_savings_kg'] = self.total_carbon_savings_kg
        context['helium_saved_l'] = self.total_helium_saved_l
        context['energy_saved_kwh'] = self.total_energy_saved_kwh

        # 6. Predictions
        if self.enable_predictive and self.predictive_analyzer:
            forecast = await self.predictive_analyzer.predict_layer_health()
            context['predicted_layer_health'] = forecast.get('predicted_health', 0.5)
            context['prediction_confidence'] = forecast.get('confidence', 0.0)

        # 7. MOPD-specific context (NEW)
        if self.enable_mopd:
            context['mopd_enabled'] = True
            context['mopd_weights'] = self.config.mopd_objective_weights
            context['mopd_grid_resolution'] = self.config.mopd_grid_resolution
            # Include current objectives from latest layer calls (if any)
            # This can be extended with real-time data

        return context

    # ============================================================================
    # Bio-Inspired Helper Methods (unchanged)
    # ============================================================================
    # ... all previous helpers remain unchanged ...

    # ============================================================================
    # Layer Communication (Existing call_layer unchanged)
    # ============================================================================
    async def call_layer(
        self,
        layer_number: int,
        method: str,
        *args,
        timeout: float = 30.0,
        retry: Optional[bool] = None,
        cache_key: Optional[str] = None,
        **kwargs
    ) -> Any:
        # ... same as before (unchanged) ...
        pass

    # ============================================================================
    # MOPD Methods (NEW)
    # ============================================================================
    async def _enumerate_execution_plans(self, layer_number: int, method: str, *args, **kwargs) -> List[MOPDPlan]:
        """Generate all feasible execution plans for a layer call."""
        # Decision variables:
        # - use_cache: True/False
        # - use_quantum: True/False (if layer supports quantum)
        # - data_center: 'us-east', 'us-west' (if applicable)
        # - retry_strategy: 'aggressive', 'moderate', 'conservative'
        # - token_allocation: we could have continuous range, but we'll sample

        use_cache_options = [True, False]
        use_quantum_options = [False]
        if self.enable_bio_integration and self.quantum_bridge and self.layers[layer_number].capabilities.get('quantum'):
            use_quantum_options = [False, True]
        data_center_options = ['us-east']
        if self.carbon_manager:
            intensity = await self.carbon_manager.get_current_intensity()
            if intensity > 400:
                data_center_options.append('us-west')
        retry_strategies = ['moderate']  # can expand
        # Token allocation: sample around the default
        token_allocation_values = [self.config.token_reserve_factor * 0.5, self.config.token_reserve_factor, self.config.token_reserve_factor * 2]

        plans = []
        for use_cache in use_cache_options:
            for use_quantum in use_quantum_options:
                for dc in data_center_options:
                    for retry_strat in retry_strategies:
                        for token_alloc in token_allocation_values:
                            plan = MOPDPlan(
                                use_cache=use_cache,
                                use_quantum=use_quantum,
                                data_center=dc,
                                retry_strategy=retry_strat,
                                token_allocation=token_alloc
                            )
                            plans.append(plan)
        return plans

    async def _compute_plan_objectives(self, plan: MOPDPlan, layer_number: int, method: str, *args, **kwargs) -> MOPDPlan:
        """Calculate carbon, helium, cost, latency, success probability for a given plan."""
        # Base values (us-east, no cache, no quantum, moderate retry, default token)
        carbon_kg = 0.5
        helium_units = 0.1
        cost_usd = 1.0
        latency_ms = 100.0
        success_prob = 0.95

        # Adjust based on plan
        if plan.use_cache:
            latency_ms *= 0.5
            carbon_kg *= 0.8   # less energy for compute
            cost_usd *= 0.7
            success_prob *= 1.02
        if plan.use_quantum:
            latency_ms *= 0.3
            carbon_kg *= 0.6
            cost_usd *= 2.0
            success_prob *= 0.9
        if plan.data_center == 'us-west':
            carbon_kg *= 0.7
            latency_ms *= 1.5
            cost_usd *= 1.1
        if plan.retry_strategy == 'aggressive':
            latency_ms *= 1.3
            success_prob *= 0.95
            carbon_kg *= 1.1
        elif plan.retry_strategy == 'conservative':
            latency_ms *= 1.5
            success_prob *= 1.05
            carbon_kg *= 0.9
        # Token allocation affects cost and success
        cost_usd += plan.token_allocation * 0.1
        success_prob += 0.01 * (plan.token_allocation / self.config.token_reserve_factor)

        # Clamp values
        plan.carbon_kg = max(0, carbon_kg)
        plan.helium_units = helium_units
        plan.cost_usd = max(0, cost_usd)
        plan.latency_ms = max(0, latency_ms)
        plan.success_probability = min(1.0, max(0.0, success_prob))
        return plan

    async def _generate_pareto_front_for_layer_call(self, layer_number: int, method: str, *args, **kwargs) -> List[MOPDPlan]:
        """Generate a Pareto‑optimal set of execution plans for a layer call."""
        plans = await self._enumerate_execution_plans(layer_number, method, *args, **kwargs)
        computed_plans = []
        for plan in plans:
            computed = await self._compute_plan_objectives(plan, layer_number, method, *args, **kwargs)
            computed_plans.append(computed)

        # Filter dominated plans
        objective_keys = ['carbon_kg', 'helium_units', 'cost_usd', 'latency_ms', 'success_probability']
        pareto = []
        for i, plan_a in enumerate(computed_plans):
            dominated = False
            for j, plan_b in enumerate(computed_plans):
                if i == j:
                    continue
                # For success_probability, higher is better -> we negate for dominance
                a_vec = [plan_a.carbon_kg, plan_a.helium_units, plan_a.cost_usd, plan_a.latency_ms, -plan_a.success_probability]
                b_vec = [plan_b.carbon_kg, plan_b.helium_units, plan_b.cost_usd, plan_b.latency_ms, -plan_b.success_probability]
                if all(b <= a for a, b in zip(a_vec, b_vec)) and any(b < a for a, b in zip(a_vec, b_vec)):
                    dominated = True
                    break
            if not dominated:
                pareto.append(plan_a)
        return pareto

    def _select_best_from_pareto(self, pareto_front: List[MOPDPlan]) -> Optional[MOPDPlan]:
        """Select the best plan using scalarisation with current MOPD weights."""
        if not pareto_front:
            return None
        weights = self.config.mopd_objective_weights
        # Normalise objectives across Pareto front
        carbon_vals = [p.carbon_kg for p in pareto_front]
        helium_vals = [p.helium_units for p in pareto_front]
        cost_vals = [p.cost_usd for p in pareto_front]
        latency_vals = [p.latency_ms for p in pareto_front]
        success_vals = [p.success_probability for p in pareto_front]

        max_carbon = max(carbon_vals) if carbon_vals else 1
        max_helium = max(helium_vals) if helium_vals else 1
        max_cost = max(cost_vals) if cost_vals else 1
        max_latency = max(latency_vals) if latency_vals else 1
        max_success = max(success_vals) if success_vals else 1

        best = None
        best_score = -float('inf')
        for plan in pareto_front:
            carbon_norm = 1 - (plan.carbon_kg / max_carbon) if max_carbon > 0 else 0
            helium_norm = 1 - (plan.helium_units / max_helium) if max_helium > 0 else 0
            cost_norm = 1 - (plan.cost_usd / max_cost) if max_cost > 0 else 0
            latency_norm = 1 - (plan.latency_ms / max_latency) if max_latency > 0 else 0
            success_norm = plan.success_probability / max_success if max_success > 0 else 0
            score = (weights['carbon'] * carbon_norm +
                     weights['helium'] * helium_norm +
                     weights['cost'] * cost_norm +
                     weights['latency'] * latency_norm +
                     weights['success_prob'] * success_norm)
            if score > best_score:
                best_score = score
                best = plan
        return best

    # ============================================================================
    # MOPD-aware Layer Call (NEW)
    # ============================================================================
    async def call_layer_with_mopd(
        self,
        layer_number: int,
        method: str,
        *args,
        return_pareto: bool = False,
        **kwargs
    ) -> Union[Any, Tuple[Any, List[MOPDPlan], MOPDPlan]]:
        """
        Call a layer with MOPD optimisation.
        If return_pareto is False, returns the result of the best plan.
        If return_pareto is True, returns (result, pareto_front, best_plan).
        """
        if not self.enable_mopd:
            # Fallback to standard call
            return await self.call_layer(layer_number, method, *args, **kwargs)

        # Generate Pareto front
        pareto_front = await self._generate_pareto_front_for_layer_call(layer_number, method, *args, **kwargs)
        if not pareto_front:
            # Fallback to standard
            return await self.call_layer(layer_number, method, *args, **kwargs)

        best_plan = self._select_best_from_pareto(pareto_front)
        if not best_plan:
            return await self.call_layer(layer_number, method, *args, **kwargs)

        # Apply best plan's decisions to the actual call
        # We need to map plan decisions to call parameters
        # For simplicity, we can adjust cache_key, retry settings, etc.
        cache_key = f"mopd_{layer_number}_{method}" if best_plan.use_cache else None
        retry = best_plan.retry_strategy != 'conservative'  # example mapping
        # We also may adjust timeout or other kwargs

        # Telemetry for MOPD
        if self.enable_monitoring and self.telemetry:
            self.telemetry.increment('mopd_calls')
            self.telemetry.histogram('mopd_pareto_front_size', len(pareto_front))
            self.telemetry.gauge('mopd_selected_carbon', best_plan.carbon_kg)
            self.telemetry.gauge('mopd_selected_latency', best_plan.latency_ms)

        # Execute with best plan parameters
        result = await self.call_layer(
            layer_number,
            method,
            *args,
            cache_key=cache_key,
            retry=retry,
            **kwargs
        )

        if return_pareto:
            return result, pareto_front, best_plan
        else:
            return result

    # ============================================================================
    # Event System (unchanged)
    # ============================================================================
    def subscribe_to_event(self, event_type: str, callback: Callable):
        # ... same as before ...
        pass

    def unsubscribe_from_event(self, event_type: str, callback: Callable):
        # ... same as before ...
        pass

    async def publish_event(self, event: LayerEvent):
        # ... same as before ...
        pass

    # ============================================================================
    # Transaction Support (unchanged)
    # ============================================================================
    async def begin_transaction(
        self,
        layers_involved: List[int],
        timeout_seconds: float = 60.0,
        distributed: bool = False,
        participants: List[str] = None
    ) -> TransactionContext:
        # ... same as before ...
        pass

    async def rollback_transaction(self, transaction_id: str):
        # ... same as before ...
        pass

    async def commit_transaction(self, transaction_id: str) -> bool:
        # ... same as before ...
        pass

    # ============================================================================
    # Layer Registration (unchanged)
    # ============================================================================
    def register_layer_module(
        self,
        layer_number: int,
        module: Any,
        version: Optional[str] = None,
        endpoints: Optional[Dict[str, str]] = None
    ) -> bool:
        # ... same as before ...
        pass

    # ============================================================================
    # Metrics and Recording (unchanged)
    # ============================================================================
    def _record_layer_success(self, layer_number: int, execution_time_ms: float):
        # ... same as before ...
        pass

    def _record_layer_error(self, layer_number: int):
        # ... same as before ...
        pass

    # ============================================================================
    # Status Methods (Enhanced with MOPD)
    # ============================================================================
    def get_health_status(self) -> Dict[str, Any]:
        # ... same as before ...
        pass

    def get_integration_status(self) -> Dict[str, Any]:
        status = {
            # ... existing fields ...
            'mopd_enabled': self.enable_mopd,
            'mopd_weights': self.config.mopd_objective_weights,
            'mopd_grid_resolution': self.config.mopd_grid_resolution,
        }
        # ... merge with existing status dictionary ...
        return status

    def get_layer_health(self) -> Dict[int, Dict[str, Any]]:
        # ... same as before ...
        pass

    def get_bio_cache_config(self) -> Dict[str, Any]:
        # ... same as before ...
        pass

    def get_sustainability_report(self) -> Dict[str, Any]:
        report = {
            # ... existing fields ...
            'mopd_enabled': self.enable_mopd,
        }
        # ... merge with existing report ...
        return report

    def _generate_sustainability_recommendations(self) -> List[str]:
        # ... same as before, can add MOPD recommendations ...
        pass

    def clear_cache(self):
        # ... same as before ...
        pass

    def reset_circuit_breaker(self, layer_number: int):
        # ... same as before ...
        pass

    async def self_heal(self):
        # ... same as before ...
        pass

    async def shutdown(self):
        # ... same as before ...
        pass
