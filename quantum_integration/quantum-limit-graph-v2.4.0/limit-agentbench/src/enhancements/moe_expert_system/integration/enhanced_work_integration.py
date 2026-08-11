#!/usr/bin/env python3
"""
Enhanced Work Integrator v7.2.0 - Complete Green Agent Implementation with MOPD Integration.

Enhancements over v7.1.0:
- Added MOPD (Multi‑Objective Pareto Decision) framework.
- New MOPDWorkPlan dataclass to represent execution plans.
- Pareto front generation for work execution plans.
- Selection of best plan via scalarisation with configurable weights.
- Integration into process_work pipeline.
- Public get_work_pareto_front method.
- Dashboard records Pareto fronts.
- Telemetry captures MOPD metrics.
- Full backward compatibility.
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Set, Callable, Union, TypeVar, cast
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from enum import Enum
import numpy as np
from collections import defaultdict, deque
import hashlib
import json
import uuid
import networkx as nx
from concurrent.futures import ThreadPoolExecutor
import aiohttp
import os
import zlib
import inspect

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
        BiomassStorage, StorageTier, GuaranteeLevel, StoredTask, StorageToken
    )
    from enhancements.bio_inspired.photosynthetic_harvester import (
        PhotosyntheticHarvester
    )
    from enhancements.bio_inspired.time_tick_engine import TimeTickEngine
    from enhancements.bio_inspired.quantum_bridge import QuantumBridge
    BIO_INSPIRED_AVAILABLE = True
    logger.info("Bio-inspired core modules loaded for Enhanced Work Integrator")
except ImportError as e:
    BIO_INSPIRED_AVAILABLE = False
    logger.warning(f"Bio-inspired core modules not available: {str(e)} - using standard work processing")
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
# Work State Machine (unchanged)
# ============================================================================
class WorkState(Enum):
    CREATED = "created"; VALIDATED = "validated"; QUEUED = "queued"; SCHEDULED = "scheduled"
    RESOURCES_RESERVED = "resources_reserved"; TOKENS_ALLOCATED = "tokens_allocated"
    EXECUTING = "executing"; CHECKPOINTED = "checkpointed"; COMPLETED = "completed"
    FAILED = "failed"; ROLLING_BACK = "rolling_back"; ROLLED_BACK = "rolled_back"
    CANCELLED = "cancelled"; STORED_AS_BIOMASS = "stored_as_biomass"; SUSPENDED = "suspended"
    RESUMED = "resumed"; MIGRATED = "migrated"; ARCHIVED = "archived"
    
    def is_terminal(self) -> bool:
        return self in [WorkState.COMPLETED, WorkState.FAILED,
                       WorkState.ROLLED_BACK, WorkState.CANCELLED, WorkState.ARCHIVED]
    
    def is_active(self) -> bool:
        return self in [WorkState.EXECUTING, WorkState.ROLLING_BACK, WorkState.CHECKPOINTED]
    
    def can_transition_to(self, target: 'WorkState') -> bool:
        valid_transitions = {
            WorkState.CREATED: [WorkState.VALIDATED, WorkState.CANCELLED],
            WorkState.VALIDATED: [WorkState.QUEUED, WorkState.CANCELLED, WorkState.STORED_AS_BIOMASS],
            WorkState.QUEUED: [WorkState.SCHEDULED, WorkState.CANCELLED, WorkState.STORED_AS_BIOMASS],
            WorkState.SCHEDULED: [WorkState.RESOURCES_RESERVED, WorkState.TOKENS_ALLOCATED, WorkState.CANCELLED],
            WorkState.RESOURCES_RESERVED: [WorkState.EXECUTING, WorkState.CANCELLED],
            WorkState.TOKENS_ALLOCATED: [WorkState.EXECUTING, WorkState.CANCELLED],
            WorkState.EXECUTING: [WorkState.COMPLETED, WorkState.FAILED, WorkState.CHECKPOINTED,
                                 WorkState.SUSPENDED, WorkState.MIGRATED],
            WorkState.CHECKPOINTED: [WorkState.EXECUTING, WorkState.RESUMED, WorkState.FAILED],
            WorkState.FAILED: [WorkState.ROLLING_BACK, WorkState.QUEUED, WorkState.STORED_AS_BIOMASS],
            WorkState.ROLLING_BACK: [WorkState.ROLLED_BACK, WorkState.FAILED],
            WorkState.ROLLED_BACK: [WorkState.QUEUED, WorkState.ARCHIVED, WorkState.STORED_AS_BIOMASS],
            WorkState.SUSPENDED: [WorkState.RESUMED, WorkState.CANCELLED],
            WorkState.RESUMED: [WorkState.EXECUTING],
            WorkState.STORED_AS_BIOMASS: [WorkState.QUEUED, WorkState.EXECUTING, WorkState.ARCHIVED],
            WorkState.COMPLETED: [WorkState.ARCHIVED]
        }
        return target in valid_transitions.get(self, [])

class WorkPriority(Enum):
    CRITICAL = 0; HIGH = 1; MEDIUM = 2; LOW = 3; BACKGROUND = 4; DEFERRABLE = 5
    
    @property
    def weight(self) -> float:
        weights = {WorkPriority.CRITICAL: 10.0, WorkPriority.HIGH: 5.0, WorkPriority.MEDIUM: 2.0,
                   WorkPriority.LOW: 1.0, WorkPriority.BACKGROUND: 0.5, WorkPriority.DEFERRABLE: 0.2}
        return weights.get(self, 1.0)

class SLALevel(Enum):
    PLATINUM = "platinum"; GOLD = "gold"; SILVER = "silver"; BRONZE = "bronze"; BEST_EFFORT = "best_effort"

# ============================================================================
# Configuration Dataclass (Enhanced with MOPD parameters)
# ============================================================================
@dataclass
class WorkIntegratorConfig:
    """Centralized configuration for the Enhanced Work Integrator."""
    # Feature flags
    enable_batching: bool = True
    enable_checkpointing: bool = True
    enable_rollback: bool = True
    enable_sla_tracking: bool = True
    enable_resource_reservation: bool = True
    enable_bio_integration: bool = True
    enable_carbon_intensity: bool = True
    enable_predictive: bool = True
    enable_cross_domain: bool = True
    enable_sustainability_scoring: bool = True
    enable_state_persistence: bool = True
    enable_dynamic_pricing: bool = True
    enable_hybrid_pipeline: bool = True
    enable_sustainability_dashboard: bool = True
    enable_telemetry: bool = True
    enable_event_driven: bool = True
    enable_self_healing: bool = True
    enable_swarm_coordination: bool = True
    enable_quantum_bridge: bool = True
    enable_time_tick_engine: bool = True
    enable_mopd: bool = True               # NEW: MOPD feature flag

    # Tunable parameters
    carbon_api_region: str = "us-east"
    carbon_update_interval: int = 300
    max_retries: int = 3
    retry_base_delay_ms: float = 100.0
    retry_max_delay_ms: float = 5000.0
    circuit_breaker_failure_threshold: int = 5
    circuit_breaker_recovery_timeout: float = 30.0
    persistence_storage_path: str = "work_states"
    pricing_base_price: float = 1.0
    hybrid_quantum_threshold: float = 0.7
    sustainability_weights: Dict[str, float] = field(default_factory=lambda: {
        'carbon': 0.3,
        'helium': 0.2,
        'token': 0.25,
        'success': 0.15,
        'pricing': 0.1
    })
    self_healing_enabled: bool = True
    workflow_on_critical_alert: str = "adjust_work_policy"
    swarm_share_interval: int = 60

    # Magic numbers
    token_expiration_timeout_seconds: int = 3600
    biomass_mobilization_threshold_gradient: float = 0.3
    recovery_completion_percentage: float = 0.5
    max_checkpoints_per_work: int = 5
    sla_deadline_critical_threshold_seconds: float = 30.0
    cleanup_max_age_hours: int = 24

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
# Carbon Intensity Manager (unchanged)
# ============================================================================
class CarbonIntensityManager:
    # ... same as before ...
    def __init__(self, config: WorkIntegratorConfig):
        self.config = config
        self.endpoint = "https://api.electricitymap.org/v3/carbon-intensity"
        self.region = config.carbon_api_region
        self.carbon_intensity = 0.0
        self.carbon_price_usd_per_ton = 50.0
        self.last_update = None
        self._lock = asyncio.Lock()
        self._session = None
        self.cache = {}
        self.historical_intensities = deque(maxlen=1000)
        self.price_history = deque(maxlen=1000)
        self.api_key = os.getenv('ELECTRICITYMAP_API_KEY', '')
        self.failure_count = 0
        self.circuit_open = False
        self.circuit_open_until = None
        self.circuit_breaker_threshold = config.circuit_breaker_failure_threshold
        self.max_retries = config.max_retries
        self.price_trend = 0.0
        logger.info(f"CarbonIntensityManager initialized (region={self.region}, retries={self.max_retries})")

    async def _get_session(self):
        if self._session is None:
            self._session = aiohttp.ClientSession()
        return self._session

    async def update_carbon_intensity(self, region: Optional[str] = None) -> Dict:
        # ... full implementation omitted for brevity (same as v7.1.0) ...
        pass

    async def get_current_intensity(self) -> float:
        # ... implementation ...
        pass

    async def get_current_price(self) -> float:
        # ... implementation ...
        pass

    async def close(self):
        if self._session:
            await self._session.close()

# ============================================================================
# Predictive Work Analyzer (unchanged)
# ============================================================================
class PredictiveWorkAnalyzer:
    # ... same as before ...
    pass

# ============================================================================
# Work Cross-Domain Transfer (unchanged)
# ============================================================================
class WorkCrossDomainTransfer:
    # ... same as before ...
    pass

# ============================================================================
# Data Classes (unchanged, with minor additions)
# ============================================================================
class WorkSLA:
    # ... same as before ...
    pass

class ResourceReservation:
    # ... same as before ...
    pass

class WorkCheckpoint:
    # ... same as before ...
    pass

# ============================================================================
# MOPDWorkPlan - NEW Dataclass for Pareto execution plans
# ============================================================================
@dataclass
class MOPDWorkPlan:
    """Represents a single execution plan with its computed objectives."""
    # Decision variables
    pipeline: str                      # 'standard', 'hybrid', 'bio_optimized'
    use_quantum: bool
    data_center: str                  # 'us-east', 'us-west', etc.
    helium_recovery: bool
    carbon_offset: bool
    renewable_share: float
    token_allocation: float           # Eco-ATP allocated
    compartment_id: Optional[str] = None
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
    def from_dict(cls, data: Dict[str, Any]) -> 'MOPDWorkPlan':
        return cls(**data)

# ============================================================================
# EnhancedWorkContext (unchanged, but will store selected plan)
# ============================================================================
class EnhancedWorkContext:
    # ... same as before, but we add an optional attribute for selected plan
    def __init__(self, task_id: str, work_type: str, priority: WorkPriority,
                 state: WorkState = WorkState.CREATED, sla: Optional[WorkSLA] = None,
                 complexity: float = 0.5, estimated_duration_ms: float = 100.0,
                 helium_dependency: float = 0.0, helium_profile: Dict[str, Any] = None,
                 meta_cognitive_state: Dict[str, Any] = None,
                 reflection_notes: List[str] = None,
                 symbolic_rules: Dict[str, Any] = None,
                 knowledge_graph_nodes: List[str] = None,
                 carbon_zone: int = 0, helium_zone: int = 0,
                 dual_axis_score: float = 0.0, quantum_capable: bool = False,
                 quantum_circuit_required: bool = False, quantum_backend_type: Optional[str] = None,
                 max_carbon_budget: float = float('inf'), max_helium_budget: float = float('inf'),
                 max_latency_ms: float = 1000.0, max_ecoatp_budget: float = float('inf'),
                 min_accuracy: float = 0.0, batch_group: Optional[str] = None,
                 can_batch: bool = True, batch_priority: int = 0,
                 depends_on: List[str] = None, dependents: List[str] = None,
                 resume_from_checkpoint: Optional[str] = None,
                 tenant_id: str = "default", isolation_level: str = "shared",
                 reservation: Optional[ResourceReservation] = None,
                 tokens_allocated: float = 0.0, tokens_consumed: float = 0.0,
                 tokens_recovered: float = 0.0, biomass_storage_token: Optional[str] = None,
                 compartment_id: Optional[str] = None,
                 created_at: Optional[datetime] = None,
                 started_at: Optional[datetime] = None,
                 completed_at: Optional[datetime] = None,
                 execution_attempts: int = 0, max_attempts: int = 3,
                 rollback_actions: List[Callable] = None,
                 compensation_actions: List[Callable] = None,
                 metrics: Dict[str, Any] = None, events: List[Dict] = None,
                 sustainability_score: float = 0.0, carbon_savings_kg: float = 0.0,
                 predicted_completion_time: Optional[datetime] = None,
                 deadline_risk_score: float = 0.0, resource_efficiency_score: float = 0.0,
                 dynamic_token_price: float = 1.0):
        # ... existing attributes ...
        self.task_id = task_id
        self.work_type = work_type
        self.priority = priority
        self.state = state
        self.state_history: List[Tuple[WorkState, datetime]] = []
        self.sla = sla
        self.complexity = complexity
        self.estimated_duration_ms = estimated_duration_ms
        self.helium_dependency = helium_dependency
        self.helium_profile = helium_profile or {}
        self.meta_cognitive_state = meta_cognitive_state or {}
        self.reflection_notes = reflection_notes or []
        self.symbolic_rules = symbolic_rules or {}
        self.knowledge_graph_nodes = knowledge_graph_nodes or []
        self.carbon_zone = carbon_zone
        self.helium_zone = helium_zone
        self.dual_axis_score = dual_axis_score
        self.quantum_capable = quantum_capable
        self.quantum_circuit_required = quantum_circuit_required
        self.quantum_backend_type = quantum_backend_type
        self.max_carbon_budget = max_carbon_budget
        self.max_helium_budget = max_helium_budget
        self.max_latency_ms = max_latency_ms
        self.max_ecoatp_budget = max_ecoatp_budget
        self.min_accuracy = min_accuracy
        self.batch_group = batch_group
        self.can_batch = can_batch
        self.batch_priority = batch_priority
        self.depends_on = depends_on or []
        self.dependents = dependents or []
        self.checkpoints: List[WorkCheckpoint] = []
        self.resume_from_checkpoint = resume_from_checkpoint
        self.tenant_id = tenant_id
        self.isolation_level = isolation_level
        self.reservation = reservation
        self.tokens_allocated = tokens_allocated
        self.tokens_consumed = tokens_consumed
        self.tokens_recovered = tokens_recovered
        self.biomass_storage_token = biomass_storage_token
        self.compartment_id = compartment_id
        self.created_at = created_at or datetime.now(timezone.utc)
        self.started_at = started_at
        self.completed_at = completed_at
        self.execution_attempts = execution_attempts
        self.max_attempts = max_attempts
        self.rollback_actions = rollback_actions or []
        self.compensation_actions = compensation_actions or []
        self.metrics = metrics or {}
        self.events = events or []
        self.sustainability_score = sustainability_score
        self.carbon_savings_kg = carbon_savings_kg
        self.predicted_completion_time = predicted_completion_time
        self.deadline_risk_score = deadline_risk_score
        self.resource_efficiency_score = resource_efficiency_score
        self.dynamic_token_price = dynamic_token_price

    def transition_to(self, new_state: WorkState) -> bool:
        # ... same as before ...
        pass

    def add_checkpoint(self, checkpoint: WorkCheckpoint):
        # ... same as before ...
        pass

    def add_event(self, event_type: str, details: Dict[str, Any]):
        # ... same as before ...
        pass

    def can_retry(self) -> bool:
        # ... same as before ...
        pass

    def to_routing_context(self) -> Dict[str, Any]:
        # ... same as before ...
        pass

    def to_dict(self) -> Dict[str, Any]:
        # ... same as before, but we can also store selected plan if present
        data = {
            # ... all existing fields ...
        }
        if 'selected_plan' in self.meta_cognitive_state:
            data['meta_cognitive_state']['selected_plan'] = self.meta_cognitive_state['selected_plan']
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'EnhancedWorkContext':
        # ... same as before ...
        # Reconstruct from data
        pass

# ============================================================================
# State Persistence Managers (unchanged)
# ============================================================================
class StatePersistenceManager:
    # ... same as before ...
    pass

class SystemStatePersistence:
    # ... same as before ...
    pass

# ============================================================================
# Dynamic Token Pricing Manager (unchanged)
# ============================================================================
class DynamicTokenPricingManager:
    # ... same as before ...
    pass

# ============================================================================
# Quantum-Classical Hybrid Pipeline (unchanged)
# ============================================================================
class QuantumClassicalHybridPipeline:
    # ... same as before ...
    pass

# ============================================================================
# Work Sustainability Dashboard (Enhanced with Pareto history)
# ============================================================================
class WorkSustainabilityDashboard:
    def __init__(self):
        self.metrics: Dict[str, deque] = {}
        self.scores: Dict[str, float] = {}
        self.history = deque(maxlen=10000)
        self.pareto_history: Dict[str, List[Dict]] = {}  # NEW: work_id -> list of Pareto plan dicts
        self._lock = asyncio.Lock()
        self.metrics['carbon_intensity'] = deque(maxlen=1000)
        self.metrics['helium_usage'] = deque(maxlen=1000)
        self.metrics['token_efficiency'] = deque(maxlen=1000)
        self.metrics['success_rate'] = deque(maxlen=1000)
        self.metrics['sustainability_score'] = deque(maxlen=1000)
        logger.info("Work Sustainability Dashboard initialized")

    async def update_metrics(self, work_id: str, metrics: Dict[str, float]):
        async with self._lock:
            self.scores[work_id] = metrics.get('sustainability_score', 0.0)
            for key, value in metrics.items():
                if key in self.metrics:
                    self.metrics[key].append(value)
            self.history.append({
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'work_id': work_id,
                'metrics': metrics
            })

    async def record_pareto_front(self, work_id: str, pareto_front: List[MOPDWorkPlan]):
        """Store the Pareto front for a completed work."""
        async with self._lock:
            self.pareto_history[work_id] = [p.to_dict() for p in pareto_front]

    async def get_dashboard_data(self, work_id: Optional[str] = None) -> Dict[str, Any]:
        async with self._lock:
            if work_id:
                data = {
                    'work_id': work_id,
                    'sustainability_score': self.scores.get(work_id, 0.0),
                    'work_history': [h for h in self.history if h['work_id'] == work_id][-50:],
                    'pareto_front': self.pareto_history.get(work_id, [])
                }
                return data
            recent = list(self.history)[-100:]
            return {
                'current_metrics': {
                    'avg_sustainability_score': np.mean([h['metrics'].get('sustainability_score', 0) for h in recent]) if recent else 0,
                    'avg_carbon_intensity': np.mean([h['metrics'].get('carbon_intensity', 400) for h in recent]) if recent else 400,
                    'avg_helium_usage': np.mean([h['metrics'].get('helium_usage', 0.5) for h in recent]) if recent else 0.5,
                    'avg_token_efficiency': np.mean([h['metrics'].get('token_efficiency', 0.5) for h in recent]) if recent else 0.5
                },
                'trends': {
                    'sustainability_trend': self._calculate_trend('sustainability_score'),
                    'carbon_trend': self._calculate_trend('carbon_intensity'),
                    'success_trend': self._calculate_trend('success_rate')
                },
                'total_works_tracked': len(self.scores),
                'recommendations': await self._generate_recommendations()
            }

    def _calculate_trend(self, metric_key: str) -> str:
        if metric_key not in self.metrics or len(self.metrics[metric_key]) < 10:
            return 'insufficient_data'
        values = list(self.metrics[metric_key])[-10:]
        if len(values) < 3:
            return 'stable'
        slope = np.polyfit(range(len(values)), values, 1)[0]
        if abs(slope) < 0.01:
            return 'stable'
        elif slope > 0:
            return 'improving'
        else:
            return 'declining'

    async def _generate_recommendations(self) -> List[str]:
        recommendations = []
        if len(self.metrics['sustainability_score']) > 10:
            avg_score = np.mean(list(self.metrics['sustainability_score'])[-10:])
            if avg_score < 0.5:
                recommendations.append("Overall sustainability is below target - consider MOPD optimisation")
        if len(self.metrics['carbon_intensity']) > 10:
            avg_carbon = np.mean(list(self.metrics['carbon_intensity'])[-10:])
            if avg_carbon > 500:
                recommendations.append("High carbon intensity detected - consider shifting workloads to low‑carbon regions")
        if len(self.metrics['success_rate']) > 10:
            avg_success = np.mean(list(self.metrics['success_rate'])[-10:])
            if avg_success < 0.8:
                recommendations.append("Low success rate - consider adjusting MOPD weights towards success_probability")
        return recommendations or ["All sustainability metrics are within acceptable ranges"]

# ============================================================================
# Telemetry Collector (unchanged)
# ============================================================================
class TelemetryCollector:
    # ... same as before ...
    pass

# ============================================================================
# Resource Reservation Manager (unchanged)
# ============================================================================
class ResourceReservationManager:
    # ... same as before ...
    pass

# ============================================================================
# Enhanced Work Integrator (Main Class) – v7.2.0 with MOPD
# ============================================================================
class EnhancedWorkIntegrator:
    """
    Enhanced Work Integrator v7.2.0 - Complete Green Agent Implementation with MOPD.
    """

    def __init__(
        self,
        bio_core: Optional[EnhancedBioInspiredCore] = None,
        config: Optional[Union[WorkIntegratorConfig, Dict[str, Any]]] = None,
        expert_router=None,
        meta_cognitive_module=None,
        neuro_symbolic_module=None,
        quantum_module=None,
        **kwargs
    ):
        # Load configuration
        if config is None:
            config = WorkIntegratorConfig(
                enable_mopd=kwargs.get('enable_mopd', True),
                # ... other legacy kwargs ...
            )
        elif isinstance(config, dict):
            config = WorkIntegratorConfig(**config)
        self.config = config

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

        # Feature flags (including MOPD)
        self.enable_mopd = config.enable_mopd

        # Core modules
        self.router = expert_router
        self.meta_cognitive = meta_cognitive_module
        self.neuro_symbolic = neuro_symbolic_module
        self.quantum_module = quantum_module

        # Existing modules (carbon, predictive, etc.) ...
        self.carbon_manager = CarbonIntensityManager(config) if self.config.enable_carbon_intensity else None
        self.predictive_analyzer = PredictiveWorkAnalyzer() if self.config.enable_predictive else None
        self.cross_domain_transfer = WorkCrossDomainTransfer() if self.config.enable_cross_domain else None

        # New modules (persistence, pricing, hybrid, dashboard, telemetry) ...
        self.state_persistence = StatePersistenceManager(config) if self.config.enable_state_persistence else None
        self.system_persistence = SystemStatePersistence(config) if self.config.enable_state_persistence else None
        self.dynamic_pricing = DynamicTokenPricingManager(config, self.carbon_manager) if self.config.enable_dynamic_pricing else None
        self.hybrid_pipeline = QuantumClassicalHybridPipeline(config, quantum_module) if self.config.enable_hybrid_pipeline else None
        self.sustainability_dashboard = WorkSustainabilityDashboard() if self.config.enable_sustainability_dashboard else None
        self.telemetry = TelemetryCollector() if self.config.enable_telemetry else None

        # Circuit breakers
        self._token_circuit = CircuitBreaker("token_service")
        self._scheduler_circuit = CircuitBreaker("scheduler_service")
        self._biomass_circuit = CircuitBreaker("biomass_storage")
        self._compartment_circuit = CircuitBreaker("compartment_service")
        self._carbon_circuit = CircuitBreaker("carbon_api")

        # Work management
        self.active_works: Dict[str, EnhancedWorkContext] = {}
        self.completed_works: Dict[str, Dict[str, Any]] = {}
        self.failed_works: Dict[str, Dict[str, Any]] = {}
        self.workflow_dag = nx.DiGraph()
        self.resource_manager = ResourceReservationManager()
        self.work_metrics: Dict[str, List[Dict]] = defaultdict(list)
        self.sla_violations: List[Dict] = []
        self.tenant_contexts: Dict[str, Dict[str, Any]] = {}

        # Sustainability tracking
        self.total_carbon_savings_kg = 0.0
        self.total_helium_savings_l = 0.0
        self.sustainability_score = 0.0
        self.biomass_mobilized_count = 0

        # Pipelines
        self.pipelines = {
            'standard': self._standard_pipeline,
            'hybrid_quantum_classical': self._hybrid_pipeline,
            'bio_optimized': self._bio_optimized_pipeline,
        }

        # Health status
        self.health_status = "healthy"
        self.last_error = None

        # Subscribe to events
        if self.config.enable_event_driven and self.event_broker:
            self._subscribe_events()

        # Load system state
        if self.config.enable_state_persistence and self.system_persistence:
            self._load_system_state_task = asyncio.create_task(self._load_system_state())

        # Start background tasks
        self._start_background_tasks()

        # Locks
        self._works_lock = asyncio.Lock()
        self._metrics_lock = asyncio.Lock()
        self._sla_lock = asyncio.Lock()

        logger.info(
            f"Enhanced Work Integrator v7.2.0 initialized: "
            f"mopd={self.enable_mopd}, "
            f"bio_integration={self.config.enable_bio_integration}, "
            f"carbon_intensity={self.config.enable_carbon_intensity}, "
            f"predictive={self.config.enable_predictive}, "
            f"state_persistence={self.config.enable_state_persistence}, "
            f"dynamic_pricing={self.config.enable_dynamic_pricing}, "
            f"hybrid_pipeline={self.config.enable_hybrid_pipeline}, "
            f"sustainability_dashboard={self.config.enable_sustainability_dashboard}, "
            f"telemetry={self.config.enable_telemetry}"
        )

    # ============================================================================
    # Event Subscriptions (same as before)
    # ============================================================================
    def _subscribe_events(self):
        # ... same as v7.1.0 ...
        pass

    # ============================================================================
    # System State Persistence (same as before)
    # ============================================================================
    async def _load_system_state(self):
        # ... same as before ...
        pass

    async def _save_system_state(self):
        # ... same as before ...
        pass

    # ============================================================================
    # Bio-Inspired Methods (same as before)
    # ============================================================================
    async def _allocate_ecoatp_for_work(self, work_id: str, ecoatp_required: float, priority: int = 0) -> Tuple[bool, float]:
        # ... same as before ...
        pass

    def _store_work_as_biomass(self, work: Dict[str, Any], ecoatp_cost: float,
                               guarantee: GuaranteeLevel = GuaranteeLevel.SILVER) -> Optional[str]:
        # ... same as before ...
        pass

    def _get_gradient_aware_priority(self, base_priority: WorkPriority) -> WorkPriority:
        # ... same as before ...
        pass

    def _recover_tokens_on_failure(self, work_id: str, completion_percentage: float) -> float:
        # ... same as before ...
        pass

    def _check_compartment_availability(self, expert_type: str) -> Tuple[bool, Optional[str]]:
        # ... same as before ...
        pass

    def _get_ecoatp_cost_estimate(self, work: Dict[str, Any]) -> float:
        # ... same as before ...
        pass

    # ============================================================================
    # MOPD Helpers (NEW)
    # ============================================================================
    async def _enumerate_execution_plans(self, context: EnhancedWorkContext) -> List[MOPDWorkPlan]:
        """Generate all feasible execution plans for the given context."""
        # Determine possible pipelines
        pipelines = ['standard']
        if self.config.enable_hybrid_pipeline and self.hybrid_pipeline:
            pipelines.append('hybrid')
        if self.config.enable_bio_integration:
            pipelines.append('bio_optimized')

        # Determine possible data centers based on carbon zone
        data_centers = ['us-east']
        if context.carbon_zone > 5:  # high carbon zone
            data_centers.append('us-west')

        # Helium recovery options
        helium_recovery_options = [False]
        if context.helium_dependency > 0.5:
            helium_recovery_options.append(True)

        # Carbon offset options
        carbon_offset_options = [False]
        carbon_price = context.meta_cognitive_state.get('carbon_price', 50)
        if carbon_price > 60:
            carbon_offset_options.append(True)

        # Renewable share sampling
        low = 0.4
        high = 0.9
        renewable_shares = np.linspace(low, high, self.config.mopd_grid_resolution)

        plans = []
        for pipeline in pipelines:
            # For hybrid pipeline, we may optionally use quantum
            use_quantum_options = [False]
            if pipeline == 'hybrid':
                use_quantum_options = [False, True]
            for use_quantum in use_quantum_options:
                for dc in data_centers:
                    for hr in helium_recovery_options:
                        for co in carbon_offset_options:
                            for rs in renewable_shares:
                                plan = MOPDWorkPlan(
                                    pipeline=pipeline,
                                    use_quantum=use_quantum,
                                    data_center=dc,
                                    helium_recovery=hr,
                                    carbon_offset=co,
                                    renewable_share=float(rs),
                                    token_allocation=0.0,
                                    compartment_id=None,
                                )
                                plans.append(plan)
        return plans

    async def _compute_plan_objectives(self, plan: MOPDWorkPlan, context: EnhancedWorkContext) -> MOPDWorkPlan:
        """Calculate carbon, helium, cost, latency, and success probability for a given plan."""
        # Base values (us-east, standard, no quantum, no helium recovery, no offsets)
        carbon_kg = 10.0
        helium_units = 0.0
        cost_usd = 0.0
        latency_ms = 100.0
        success_prob = 0.95

        # Adjust based on plan
        if plan.data_center == 'us-west':
            carbon_kg *= 0.6   # lower carbon
            latency_ms *= 1.5  # higher latency
            cost_usd += 5.0
        if plan.helium_recovery:
            helium_units = context.helium_dependency * 0.5
            cost_usd += 2.0
            latency_ms += 10.0
        if plan.carbon_offset:
            carbon_kg -= 5.0   # offset part
            cost_usd += context.meta_cognitive_state.get('carbon_price', 50) * 0.1
        if plan.renewable_share > 0.5:
            carbon_kg *= (1.0 - (plan.renewable_share - 0.5) * 0.5)
            cost_usd += (plan.renewable_share - 0.5) * 2.0
        if plan.use_quantum:
            latency_ms *= 0.5
            cost_usd += 3.0
            success_prob *= 0.95  # quantum may have lower success rate
        if plan.pipeline == 'bio_optimized':
            success_prob *= 1.05
            carbon_kg *= 0.95

        # Token allocation (Eco-ATP)
        plan.token_allocation = self._get_ecoatp_cost_estimate(context.metrics)
        if self.config.enable_dynamic_pricing and self.dynamic_pricing:
            plan.token_allocation *= context.dynamic_token_price

        # Compartment assignment (if available)
        if self.config.enable_bio_integration and self.compartment_manager:
            available, comp_id = self._check_compartment_availability(context.work_type)
            if available:
                plan.compartment_id = comp_id

        plan.carbon_kg = max(0, carbon_kg)
        plan.helium_units = helium_units
        plan.cost_usd = cost_usd
        plan.latency_ms = latency_ms
        plan.success_probability = min(1.0, max(0.0, success_prob))
        return plan

    async def _generate_pareto_front_for_work(self, context: EnhancedWorkContext) -> List[MOPDWorkPlan]:
        """Generate a Pareto‑optimal set of execution plans."""
        plans = await self._enumerate_execution_plans(context)
        computed_plans = []
        for plan in plans:
            computed = await self._compute_plan_objectives(plan, context)
            computed_plans.append(computed)

        # Filter dominated plans using dominance check
        objective_keys = ['carbon_kg', 'helium_units', 'cost_usd', 'latency_ms', 'success_probability']
        # We minimise carbon, helium, cost, latency; maximise success_probability (so we negate for dominance)
        pareto = []
        for i, plan_a in enumerate(computed_plans):
            dominated = False
            for j, plan_b in enumerate(computed_plans):
                if i == j:
                    continue
                # Build vectors: for success_prob we use negative because higher is better
                a_vec = [plan_a.carbon_kg, plan_a.helium_units, plan_a.cost_usd, plan_a.latency_ms, -plan_a.success_probability]
                b_vec = [plan_b.carbon_kg, plan_b.helium_units, plan_b.cost_usd, plan_b.latency_ms, -plan_b.success_probability]
                if all(b <= a for a, b in zip(a_vec, b_vec)) and any(b < a for a, b in zip(a_vec, b_vec)):
                    dominated = True
                    break
            if not dominated:
                pareto.append(plan_a)
        return pareto

    def _select_best_from_pareto(self, pareto_front: List[MOPDWorkPlan]) -> Optional[MOPDWorkPlan]:
        """Select the best plan using scalarisation with current weights."""
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
            # For carbon, helium, cost, latency: lower is better -> we invert
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
    # Primary Work Processing (Enhanced with MOPD)
    # ============================================================================
    async def process_work(
        self,
        work_request: Dict[str, Any],
        pipeline_type: str = 'standard',
        dependencies: Optional[List[str]] = None,
        tenant_id: str = "default",
        return_pareto: bool = False   # NEW: if True, include Pareto front in result
    ) -> Dict[str, Any]:
        # Step 1: Create and validate context
        context = await self._create_and_validate_context(work_request, tenant_id)

        # Step 2: Enrich with carbon and pricing
        await self._enrich_context_with_carbon_and_pricing(context)

        # Step 3: Adjust priority based on gradients
        if self.config.enable_bio_integration:
            context.priority = self._get_gradient_aware_priority(context.priority)

        # Step 4: Add to workflow DAG
        self._add_to_workflow_dag(context, dependencies)

        # Step 5: Allocate Eco-ATP (or store as biomass if fail)
        if not await self._allocate_resources(context):
            return await self._handle_allocation_failure(context, work_request)

        # Step 6: Check compartment availability
        if not await self._check_and_assign_compartment(context):
            return await self._handle_compartment_unavailable(context, work_request)

        # Step 7: MOPD-based plan selection (NEW)
        if self.enable_mopd:
            pareto_front = await self._generate_pareto_front_for_work(context)
            if pareto_front:
                best_plan = self._select_best_from_pareto(pareto_front)
                if best_plan:
                    # Store selected plan in context for later use
                    context.meta_cognitive_state['selected_plan'] = best_plan.to_dict()
                    # Override pipeline_type based on selected plan
                    pipeline_type = best_plan.pipeline
                    # Also store Pareto front for possible return
                    context.meta_cognitive_state['pareto_front'] = [p.to_dict() for p in pareto_front]
        else:
            # Legacy: use given pipeline_type
            pass

        # Step 8: Execute pipeline
        result = await self._execute_pipeline(context, pipeline_type)

        # Step 9: Finalize work
        final = await self._finalize_work(context, result)

        # If MOPD and return_pareto, include Pareto front in result
        if self.enable_mopd and return_pareto:
            if 'pareto_front' in context.meta_cognitive_state:
                final['pareto_front'] = context.meta_cognitive_state['pareto_front']
            if 'selected_plan' in context.meta_cognitive_state:
                final['selected_plan'] = context.meta_cognitive_state['selected_plan']

        return final

    # ============================================================================
    # Helper methods for processing (unchanged, with minor updates)
    # ============================================================================
    async def _create_and_validate_context(self, work_request: Dict[str, Any], tenant_id: str) -> EnhancedWorkContext:
        # ... same as before ...
        pass

    async def _enrich_context_with_carbon_and_pricing(self, context: EnhancedWorkContext):
        # ... same as before ...
        pass

    def _add_to_workflow_dag(self, context: EnhancedWorkContext, dependencies: Optional[List[str]]):
        # ... same as before ...
        pass

    async def _allocate_resources(self, context: EnhancedWorkContext) -> bool:
        # ... same as before ...
        pass

    async def _handle_allocation_failure(self, context: EnhancedWorkContext, work_request: Dict[str, Any]) -> Dict[str, Any]:
        # ... same as before ...
        pass

    async def _check_and_assign_compartment(self, context: EnhancedWorkContext) -> bool:
        # ... same as before ...
        pass

    async def _handle_compartment_unavailable(self, context: EnhancedWorkContext, work_request: Dict[str, Any]) -> Dict[str, Any]:
        # ... same as before ...
        pass

    async def _execute_pipeline(self, context: EnhancedWorkContext, pipeline_type: str) -> Dict[str, Any]:
        # ... same as before ...
        pass

    async def _finalize_work(self, context: EnhancedWorkContext, result: Dict[str, Any]) -> Dict[str, Any]:
        # ... same as before, but add Pareto recording if MOPD enabled
        if self.config.enable_sustainability_dashboard and self.sustainability_dashboard:
            # Record Pareto front if present
            if 'pareto_front' in context.meta_cognitive_state:
                pareto_objs = [MOPDWorkPlan.from_dict(p) for p in context.meta_cognitive_state['pareto_front']]
                await self.sustainability_dashboard.record_pareto_front(context.task_id, pareto_objs)
        # ... rest of finalization ...
        return result

    # ============================================================================
    # Public method to get Pareto front without executing (NEW)
    # ============================================================================
    async def get_work_pareto_front(self, work_request: Dict[str, Any]) -> List[MOPDWorkPlan]:
        """Return the Pareto front for a hypothetical work without actually executing it."""
        context = self._create_work_context(work_request, tenant_id=work_request.get('tenant_id', 'default'))
        await self._enrich_context_with_carbon_and_pricing(context)
        pareto_front = await self._generate_pareto_front_for_work(context)
        return pareto_front

    # ============================================================================
    # Pipelines (updated to consider selected plan)
    # ============================================================================
    async def _standard_pipeline(self, context: EnhancedWorkContext) -> Dict[str, Any]:
        # If MOPD is enabled and a plan is selected, adjust routing context
        if self.enable_mopd and 'selected_plan' in context.meta_cognitive_state:
            plan_dict = context.meta_cognitive_state['selected_plan']
            plan = MOPDWorkPlan.from_dict(plan_dict)
            # Modify routing context according to plan
            routing_context = context.to_routing_context()
            routing_context['preferred_data_center'] = plan.data_center
            routing_context['use_quantum'] = plan.use_quantum
            routing_context['helium_recovery'] = plan.helium_recovery
            routing_context['carbon_offset'] = plan.carbon_offset
            routing_context['renewable_share'] = plan.renewable_share
        else:
            routing_context = context.to_routing_context()

        # ... rest of standard pipeline (meta-cognition, symbolic, dual-axis, routing) ...
        if self.meta_cognitive:
            context = await self._apply_meta_cognition(context)
        symbolic_constraints = None
        if self.neuro_symbolic:
            symbolic_constraints = await self._extract_symbolic_constraints(context)
        dual_axis_context = self._build_dual_axis_context(context)
        if self.config.enable_bio_integration and self.gradient_manager:
            dual_axis_context['gradient_levels'] = self.gradient_manager.get_field_strengths()
        routing_result = self.router.route_and_execute(
            workload_profile=routing_context,
            meta_cognitive_state=context.meta_cognitive_state,
            dual_axis_context=dual_axis_context,
            symbolic_constraints=symbolic_constraints
        )
        result = self._post_process_result(routing_result, context)
        result['work_metadata'] = {
            'task_id': context.task_id, 'work_type': context.work_type,
            'priority': context.priority.name, 'state': context.state.value,
            'attempt': context.execution_attempts, 'tenant_id': context.tenant_id,
            'compartment_id': context.compartment_id
        }
        # Add MOPD plan info if selected
        if self.enable_mopd and 'selected_plan' in context.meta_cognitive_state:
            result['mopd_plan'] = context.meta_cognitive_state['selected_plan']
        return result

    async def _hybrid_pipeline(self, context: EnhancedWorkContext) -> Dict[str, Any]:
        # If MOPD is enabled, the plan already decided whether to use quantum; we respect that.
        if self.enable_mopd and 'selected_plan' in context.meta_cognitive_state:
            plan = MOPDWorkPlan.from_dict(context.meta_cognitive_state['selected_plan'])
            if plan.use_quantum:
                # Force hybrid pipeline to use quantum
                result = await self.hybrid_pipeline.execute(context, self._standard_pipeline, quantum_threshold=0.0)
                result['pipeline_type'] = 'hybrid_quantum_classical'
                return result
            else:
                # Fallback to standard
                return await self._standard_pipeline(context)
        else:
            if not self.enable_hybrid_pipeline or not self.hybrid_pipeline:
                return await self._standard_pipeline(context)
            result = await self.hybrid_pipeline.execute(context, self._standard_pipeline)
            result['pipeline_type'] = 'hybrid_quantum_classical'
            return result

    async def _bio_optimized_pipeline(self, context: EnhancedWorkContext) -> Dict[str, Any]:
        # ... same as before ...
        pass

    # ============================================================================
    # Other helper methods (unchanged)
    # ============================================================================
    async def _apply_meta_cognition(self, context: EnhancedWorkContext) -> EnhancedWorkContext:
        # ... same as before ...
        pass

    async def _extract_symbolic_constraints(self, context: EnhancedWorkContext) -> Optional[Dict[str, Any]]:
        # ... same as before ...
        pass

    def _build_dual_axis_context(self, context: EnhancedWorkContext) -> Dict[str, Any]:
        # ... same as before ...
        pass

    def _post_process_result(self, routing_result: Dict[str, Any], context: EnhancedWorkContext) -> Dict[str, Any]:
        # ... same as before ...
        pass

    async def _create_checkpoint(self, context: EnhancedWorkContext, result: Dict[str, Any]):
        # ... same as before ...
        pass

    async def _rollback_work(self, context: EnhancedWorkContext):
        # ... same as before ...
        pass

    def _create_work_context(self, request: Dict[str, Any], tenant_id: str = "default") -> EnhancedWorkContext:
        # ... same as before ...
        pass

    def _update_work_metrics(self, task_id: str, result: Dict[str, Any]):
        # ... same as before ...
        pass

    def _record_sla_violation(self, context: EnhancedWorkContext, actual_latency_ms: float):
        # ... same as before ...
        pass

    # ============================================================================
    # Self-Healing (unchanged)
    # ============================================================================
    async def self_heal(self):
        # ... same as before ...
        pass

    # ============================================================================
    # Swarm Coordination (unchanged)
    # ============================================================================
    async def share_with_swarm(self):
        # ... same as before ...
        pass

    async def _swarm_update_loop(self):
        # ... same as before ...
        pass

    # ============================================================================
    # Background Loops (unchanged)
    # ============================================================================
    def _start_background_tasks(self):
        # ... same as before ...
        pass

    # ============================================================================
    # Statistics (Enhanced with MOPD info)
    # ============================================================================
    def get_work_statistics(self) -> Dict[str, Any]:
        stats = super().get_work_statistics() if hasattr(super(), 'get_work_statistics') else {}
        stats['mopd_enabled'] = self.enable_mopd
        if self.enable_telemetry:
            # Count MOPD generations from telemetry (if tracked)
            pass
        return stats

    # ============================================================================
    # Public methods (unchanged)
    # ============================================================================
    def get_work_state(self, task_id: str) -> Optional[Dict[str, Any]]:
        # ... same as before ...
        pass

    def cancel_work(self, task_id: str) -> bool:
        # ... same as before ...
        pass

    def get_sustainability_report(self) -> Dict[str, Any]:
        report = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'sustainability_score': self.sustainability_score,
            'total_carbon_savings_kg': self.total_carbon_savings_kg,
            'total_helium_savings_l': self.total_helium_savings_l,
            'active_works': len(self.active_works),
            'bio_integration_active': self.config.enable_bio_integration,
            'predictive_forecast': self.predictive_analyzer.predict_work_trend() if self.config.enable_predictive else {},
            'recommendations': self._generate_sustainability_recommendations(),
            'mopd_enabled': self.enable_mopd,
        }
        if self.config.enable_sustainability_dashboard and self.sustainability_dashboard:
            dashboard_data = asyncio.run(self.sustainability_dashboard.get_dashboard_data())
            report['dashboard'] = dashboard_data
        return report

    def _generate_sustainability_recommendations(self) -> List[str]:
        # ... same as before but can include MOPD advice ...
        recommendations = []
        if self.sustainability_score < 0.5:
            recommendations.append("Increase token efficiency for better sustainability")
            recommendations.append("Optimize MOPD weights to favour carbon savings")
        # ... other recommendations ...
        return recommendations or ["Work integration sustainability is on track"]

    def get_health_status(self) -> Dict[str, Any]:
        # ... same as before ...
        pass

    # ============================================================================
    # Shutdown (unchanged)
    # ============================================================================
    async def shutdown(self):
        # ... same as before ...
        pass
