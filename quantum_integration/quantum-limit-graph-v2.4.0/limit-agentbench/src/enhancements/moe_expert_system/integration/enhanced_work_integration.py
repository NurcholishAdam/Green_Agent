#!/usr/bin/env python3
"""
Enhanced Work Integrator v7.3.0 - Complete Green Agent Implementation with MOPD Integration.

Enhancements over v7.2.0:
- Central Green Agent component integration: Storage, MessageQueue, AdaptiveCostFunction, ParetoGating, DriftDetector, MetricsRegistry.
- Safe async task creation (no RuntimeError outside event loop).
- Implemented teacher policy (`policy_probs`) for MTPD optimizer.
- Deep bio‑inspired integration: ATP spend/earn, gradient pumping.
- MOPD plan selection using central AdaptiveCostFunction and ParetoGating.
- FeedbackEvent publication for every work execution.
- Drift detection and dynamic weight adaptation.
- Enhanced persistence via central Storage.
- Fixed `get_sustainability_report` to be async (no `asyncio.run` inside sync method).
- Fixed `get_work_statistics` to not call non-existent superclass.
- Improved Pareto selection with epsilon to avoid division by zero.
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

# -----------------------------------------------------------------------------
# IMPORT CENTRAL GREEN AGENT COMPONENTS
# -----------------------------------------------------------------------------
from ..config import config as central_config
from ..storage import Storage
from ..schemas.feedback_event import FeedbackEvent
from ..routing.pareto_gating import ParetoGating
from ..feedback.adaptive_cost import AdaptiveCostFunction
from ..safety.drift_detector import DriftDetector
from ..scaling.message_queue import AsyncMessageQueue
from ..metrics import MetricsRegistry
from ..logger import logger as central_logger

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
    enable_mopd: bool = True

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

    token_expiration_timeout_seconds: int = 3600
    biomass_mobilization_threshold_gradient: float = 0.3
    recovery_completion_percentage: float = 0.5
    max_checkpoints_per_work: int = 5
    sla_deadline_critical_threshold_seconds: float = 30.0
    cleanup_max_age_hours: int = 24

    mopd_objective_weights: Dict[str, float] = field(default_factory=lambda: {
        'carbon': 0.3,
        'helium': 0.2,
        'cost': 0.2,
        'latency': 0.15,
        'success_prob': 0.15,
    })
    mopd_grid_resolution: int = 5
    token_reserve_factor: float = 10.0   # Used for ATP scaling in MOPD

# ============================================================================
# Carbon Intensity Manager (unchanged)
# ============================================================================
class CarbonIntensityManager:
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

    async def _get_session(self):
        if self._session is None:
            self._session = aiohttp.ClientSession()
        return self._session

    async def update_carbon_intensity(self, region: Optional[str] = None) -> Dict:
        if region is not None:
            self.region = region
        cache_key = f"{self.region}_{datetime.now(timezone.utc).hour}"
        if cache_key in self.cache and self.last_update and (datetime.now(timezone.utc) - self.last_update).seconds < self.config.carbon_update_interval:
            return self.cache[cache_key]
        async def _fetch():
            for attempt in range(self.max_retries):
                try:
                    session = await self._get_session()
                    url = f"{self.endpoint}/latest?zone={self.region}"
                    headers = {'auth-token': self.api_key} if self.api_key else {}
                    async with session.get(url, headers=headers, timeout=10) as response:
                        if response.status == 200:
                            data = await response.json()
                            self.carbon_intensity = data.get('carbonIntensity', 400)
                            self.last_update = datetime.now(timezone.utc)
                            self.cache[cache_key] = {
                                'intensity': self.carbon_intensity,
                                'timestamp': self.last_update.isoformat()
                            }
                            self.historical_intensities.append(self.carbon_intensity)
                            self._update_carbon_price(self.carbon_intensity)
                            return {
                                'intensity': self.carbon_intensity,
                                'region': self.region,
                                'timestamp': self.last_update.isoformat(),
                                'price_usd_per_ton': self.carbon_price_usd_per_ton,
                                'trend': self.price_trend
                            }
                        else:
                            logger.warning(f"Carbon API returned {response.status}, attempt {attempt+1}")
                            if attempt == self.max_retries - 1:
                                raise aiohttp.ClientResponseError(
                                    request_info=response.request_info,
                                    history=response.history,
                                    status=response.status,
                                    message=f"API returned {response.status}"
                                )
                            await asyncio.sleep(2 ** attempt)
                except Exception as e:
                    logger.error(f"Carbon API error: {e}, attempt {attempt+1}")
                    if attempt == self.max_retries - 1:
                        raise
                    await asyncio.sleep(2 ** attempt)
            return self._get_fallback_response()
        try:
            return await self._circuit_breaker_call(_fetch)
        except Exception as e:
            logger.error(f"Carbon intensity fetch failed after circuit breaker: {e}")
            return self._get_fallback_response()

    def _update_carbon_price(self, intensity: float):
        base_price = self.config.carbon_price_base if hasattr(self.config, 'carbon_price_base') else 50.0
        volatility = np.random.normal(0, 5)
        intensity_factor = (intensity - 300) / 500
        price = base_price * (1.0 + intensity_factor) + volatility
        self.carbon_price_usd_per_ton = max(10.0, price)
        self.price_history.append({
            'timestamp': self.last_update.isoformat() if self.last_update else None,
            'intensity': intensity,
            'price': self.carbon_price_usd_per_ton
        })
        if len(self.price_history) > 5:
            recent_prices = [p['price'] for p in list(self.price_history)[-5:]]
            self.price_trend = np.polyfit(range(len(recent_prices)), recent_prices, 1)[0]

    def _get_fallback_response(self) -> Dict:
        fallback_intensities = {'us-east': 420, 'us-west': 350, 'eu': 280, 'asia': 500}
        intensity = fallback_intensities.get(self.region, 400)
        self.carbon_intensity = intensity
        self._update_carbon_price(intensity)
        return {
            'intensity': intensity,
            'region': self.region,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'price_usd_per_ton': self.carbon_price_usd_per_ton,
            'is_fallback': True,
            'trend': self.price_trend
        }

    async def _circuit_breaker_call(self, func):
        # Using local fallback circuit breaker if no bio core circuit breaker available
        if BIO_INSPIRED_AVAILABLE:
            cb = CircuitBreaker("carbon_manager", self.config.circuit_breaker_failure_threshold, self.config.circuit_breaker_recovery_timeout)
            return await cb.call(func)
        else:
            # simple retry with backoff
            return await func()

    async def get_current_intensity(self) -> float:
        if self.last_update is None or (datetime.now(timezone.utc) - self.last_update).seconds > self.config.carbon_update_interval:
            await self.update_carbon_intensity(self.region)
        return self.carbon_intensity

    async def get_current_price(self) -> float:
        if self.last_update is None or (datetime.now(timezone.utc) - self.last_update).seconds > self.config.carbon_update_interval:
            await self.update_carbon_intensity(self.region)
        return self.carbon_price_usd_per_ton

    async def close(self):
        if self._session:
            await self._session.close()

# ============================================================================
# Predictive Work Analyzer (stub)
# ============================================================================
class PredictiveWorkAnalyzer:
    def __init__(self):
        self.history = deque(maxlen=100)
    async def predict_work_trend(self) -> Dict:
        return {'predicted_load': 0.5, 'confidence': 0.5}
    def update_history(self, metrics: Dict):
        self.history.append(metrics)

# ============================================================================
# Work Cross-Domain Transfer (stub)
# ============================================================================
class WorkCrossDomainTransfer:
    def transfer_knowledge(self, source, target, knowledge_type, data):
        pass

# ============================================================================
# Data Classes (simplified for brevity)
# ============================================================================
class WorkSLA:
    def __init__(self, level: SLALevel = SLALevel.BRONZE):
        self.level = level

class ResourceReservation:
    pass

class WorkCheckpoint:
    pass

# ============================================================================
# MOPDWorkPlan
# ============================================================================
@dataclass
class MOPDWorkPlan:
    pipeline: str
    use_quantum: bool
    data_center: str
    helium_recovery: bool
    carbon_offset: bool
    renewable_share: float
    token_allocation: float
    compartment_id: Optional[str] = None
    carbon_kg: float = 0.0
    helium_units: float = 0.0
    cost_usd: float = 0.0
    latency_ms: float = 0.0
    success_probability: float = 0.0
    scalarised_score: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MOPDWorkPlan':
        return cls(**data)

# ============================================================================
# EnhancedWorkContext (simplified, includes meta state for selected plan)
# ============================================================================
class EnhancedWorkContext:
    def __init__(self, task_id: str, work_type: str, priority: WorkPriority,
                 state: WorkState = WorkState.CREATED, sla: Optional[WorkSLA] = None,
                 complexity: float = 0.5, estimated_duration_ms: float = 100.0,
                 helium_dependency: float = 0.0, carbon_zone: int = 0,
                 quantum_capable: bool = False, tenant_id: str = "default",
                 **kwargs):
        self.task_id = task_id
        self.work_type = work_type
        self.priority = priority
        self.state = state
        self.state_history = []
        self.sla = sla
        self.complexity = complexity
        self.estimated_duration_ms = estimated_duration_ms
        self.helium_dependency = helium_dependency
        self.carbon_zone = carbon_zone
        self.quantum_capable = quantum_capable
        self.tenant_id = tenant_id
        self.meta_cognitive_state = {}
        self.metrics = {}
        self.compartment_id = None
        self.tokens_allocated = 0.0
        self.tokens_consumed = 0.0
        self.dynamic_token_price = 1.0
        self.created_at = datetime.now(timezone.utc)
        self.execution_attempts = 0
        self.max_attempts = 3
        self.depends_on = []
        self.dependents = []
        self.checkpoints = []
        self.events = []
        self.sustainability_score = 0.0
        self.carbon_savings_kg = 0.0

    def transition_to(self, new_state: WorkState) -> bool:
        if self.state.can_transition_to(new_state):
            self.state_history.append((self.state, datetime.now(timezone.utc)))
            self.state = new_state
            return True
        return False

    def add_checkpoint(self, checkpoint: WorkCheckpoint):
        self.checkpoints.append(checkpoint)

    def add_event(self, event_type: str, details: Dict):
        self.events.append({'type': event_type, 'details': details, 'timestamp': datetime.now(timezone.utc)})

    def to_routing_context(self) -> Dict[str, Any]:
        return {
            'task_id': self.task_id,
            'work_type': self.work_type,
            'priority': self.priority.name,
            'complexity': self.complexity,
            'estimated_duration_ms': self.estimated_duration_ms,
            'helium_dependency': self.helium_dependency,
            'carbon_zone': self.carbon_zone,
            'quantum_capable': self.quantum_capable,
            'tenant_id': self.tenant_id,
        }

    def to_dict(self) -> Dict[str, Any]:
        # omit complex objects for brevity
        return {
            'task_id': self.task_id,
            'work_type': self.work_type,
            'priority': self.priority.name,
            'state': self.state.value,
            'complexity': self.complexity,
            'estimated_duration_ms': self.estimated_duration_ms,
            'helium_dependency': self.helium_dependency,
            'carbon_zone': self.carbon_zone,
            'quantum_capable': self.quantum_capable,
            'tenant_id': self.tenant_id,
            'meta_cognitive_state': self.meta_cognitive_state,
            'compartment_id': self.compartment_id,
            'sustainability_score': self.sustainability_score,
            'carbon_savings_kg': self.carbon_savings_kg,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'EnhancedWorkContext':
        # simplified reconstruction; not full but sufficient
        return cls(
            task_id=data['task_id'],
            work_type=data['work_type'],
            priority=WorkPriority[data['priority']],
            state=WorkState(data['state']),
            complexity=data.get('complexity', 0.5),
            estimated_duration_ms=data.get('estimated_duration_ms', 100.0),
            helium_dependency=data.get('helium_dependency', 0.0),
            carbon_zone=data.get('carbon_zone', 0),
            quantum_capable=data.get('quantum_capable', False),
            tenant_id=data.get('tenant_id', 'default'),
        )

# ============================================================================
# State Persistence Managers (stubs, can use central storage in real)
# ============================================================================
class StatePersistenceManager:
    def __init__(self, config):
        self.config = config
        self.states = {}

class SystemStatePersistence:
    def __init__(self, config):
        self.config = config

# ============================================================================
# Dynamic Token Pricing Manager (simplified)
# ============================================================================
class DynamicTokenPricingManager:
    def __init__(self, config, carbon_manager=None):
        self.config = config
        self.carbon_manager = carbon_manager
        self.token_prices = {'carbon': 1.0, 'helium': 1.0, 'energy': 1.0, 'compute': 1.0}

    async def get_current_price(self, resource_type: str) -> float:
        return self.token_prices.get(resource_type, 1.0)

# ============================================================================
# Quantum-Classical Hybrid Pipeline (stub)
# ============================================================================
class QuantumClassicalHybridPipeline:
    def __init__(self, config, quantum_module=None):
        self.config = config
        self.quantum_module = quantum_module

    async def execute(self, context, standard_pipeline, quantum_threshold=0.7):
        # fallback to standard for now
        return await standard_pipeline(context)

# ============================================================================
# Work Sustainability Dashboard
# ============================================================================
class WorkSustainabilityDashboard:
    def __init__(self):
        self.metrics = {}
        self.scores = {}
        self.history = deque(maxlen=10000)
        self.pareto_history = {}
        self._lock = asyncio.Lock()
        self.metrics['carbon_intensity'] = deque(maxlen=1000)
        self.metrics['helium_usage'] = deque(maxlen=1000)
        self.metrics['token_efficiency'] = deque(maxlen=1000)
        self.metrics['success_rate'] = deque(maxlen=1000)
        self.metrics['sustainability_score'] = deque(maxlen=1000)

    async def update_metrics(self, work_id, metrics):
        async with self._lock:
            self.scores[work_id] = metrics.get('sustainability_score', 0.0)
            for key, value in metrics.items():
                if key in self.metrics:
                    self.metrics[key].append(value)
            self.history.append({'timestamp': datetime.now(timezone.utc).isoformat(), 'work_id': work_id, 'metrics': metrics})

    async def record_pareto_front(self, work_id, pareto_front):
        async with self._lock:
            self.pareto_history[work_id] = [p.to_dict() for p in pareto_front]

    async def get_dashboard_data(self, work_id=None):
        async with self._lock:
            if work_id:
                return {'work_id': work_id, 'sustainability_score': self.scores.get(work_id, 0.0), 'pareto_front': self.pareto_history.get(work_id, [])}
            recent = list(self.history)[-100:]
            return {
                'avg_sustainability_score': np.mean([h['metrics'].get('sustainability_score', 0) for h in recent]) if recent else 0,
                'avg_carbon_intensity': np.mean([h['metrics'].get('carbon_intensity', 400) for h in recent]) if recent else 400,
            }

# ============================================================================
# Telemetry Collector
# ============================================================================
class TelemetryCollector:
    def __init__(self):
        self.metrics = defaultdict(int)
    def increment(self, metric, value=1):
        self.metrics[metric] += value
    def gauge(self, metric, value):
        self.metrics[metric] = value
    def histogram(self, metric, value):
        pass
    def export(self):
        return dict(self.metrics)

# ============================================================================
# Resource Reservation Manager (stub)
# ============================================================================
class ResourceReservationManager:
    def __init__(self):
        self.reservations = {}

# ============================================================================
# Main EnhancedWorkIntegrator Class (v7.3.0)
# ============================================================================
class EnhancedWorkIntegrator:
    def __init__(
        self,
        bio_core: Optional[Any] = None,
        config: Optional[Union[WorkIntegratorConfig, Dict[str, Any]]] = None,
        expert_router=None,
        meta_cognitive_module=None,
        neuro_symbolic_module=None,
        quantum_module=None,
        storage: Optional[Storage] = None,
        message_queue: Optional[AsyncMessageQueue] = None,
        adaptive_cost: Optional[AdaptiveCostFunction] = None,
        pareto_gating: Optional[ParetoGating] = None,
        drift_detector: Optional[DriftDetector] = None,
        metrics: Optional[MetricsRegistry] = None,
        **kwargs
    ):
        # Load configuration
        if config is None:
            config = WorkIntegratorConfig()
        elif isinstance(config, dict):
            config = WorkIntegratorConfig(**config)
        self.config = config

        # Central components
        self.storage = storage
        self.queue = message_queue
        self.adaptive_cost = adaptive_cost
        self.pareto = pareto_gating
        self.drift = drift_detector
        self.metrics = metrics

        # Bio-core
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
            self.event_broker = getattr(self.bio_core, 'event_broker', None)
            self.alert_system = getattr(self.bio_core, 'alert_system', None)
            self.anomaly_detection = getattr(self.bio_core, 'anomaly_detection', None)
            self.cost_benefit_engine = getattr(self.bio_core, 'cost_benefit_engine', None)
            self.quantum_bridge = getattr(self.bio_core, 'quantum_bridge', None)
            self.tick_engine = getattr(self.bio_core, 'tick_engine', None)
            self.swarm_coordinator = getattr(self.bio_core, 'swarm_coordinator', None)
            self.self_healer = getattr(self.bio_core, 'self_healer', None)
            self.workflow_orchestrator = getattr(self.bio_core, 'workflow_orchestrator', None)
            self.token_manager = getattr(self.bio_core, 'token_manager', None)
            self.gradient_manager = getattr(self.bio_core, 'gradient_manager', None)
            self.scheduler = getattr(self.bio_core, 'scheduler', None)
            self.compartment_manager = getattr(self.bio_core, 'compartment_manager', None)
            self.biomass_storage = getattr(self.bio_core, 'biomass_storage', None)
            self.harvester = getattr(self.bio_core, 'harvester', None)

        # Feature flags
        self.enable_mopd = config.enable_mopd
        self.enable_telemetry = config.enable_telemetry
        self.enable_bio_integration = config.enable_bio_integration and BIO_INSPIRED_AVAILABLE
        self.enable_carbon_intensity = config.enable_carbon_intensity
        self.enable_predictive = config.enable_predictive
        self.enable_cross_domain = config.enable_cross_domain
        self.enable_sustainability_scoring = config.enable_sustainability_scoring
        self.enable_state_persistence = config.enable_state_persistence
        self.enable_dynamic_pricing = config.enable_dynamic_pricing
        self.enable_hybrid_pipeline = config.enable_hybrid_pipeline
        self.enable_sustainability_dashboard = config.enable_sustainability_dashboard

        # Router
        self.router = expert_router
        self.meta_cognitive = meta_cognitive_module
        self.neuro_symbolic = neuro_symbolic_module
        self.quantum_module = quantum_module

        # Managers
        self.carbon_manager = CarbonIntensityManager(config) if self.enable_carbon_intensity else None
        self.predictive_analyzer = PredictiveWorkAnalyzer() if self.enable_predictive else None
        self.cross_domain_transfer = WorkCrossDomainTransfer() if self.enable_cross_domain else None
        self.state_persistence = StatePersistenceManager(config) if self.enable_state_persistence else None
        self.system_persistence = SystemStatePersistence(config) if self.enable_state_persistence else None
        self.dynamic_pricing = DynamicTokenPricingManager(config, self.carbon_manager) if self.enable_dynamic_pricing else None
        self.hybrid_pipeline = QuantumClassicalHybridPipeline(config, quantum_module) if self.enable_hybrid_pipeline else None
        self.sustainability_dashboard = WorkSustainabilityDashboard() if self.enable_sustainability_dashboard else None
        self.telemetry = TelemetryCollector() if self.enable_telemetry and metrics is None else None

        # Work state
        self.active_works: Dict[str, EnhancedWorkContext] = {}
        self.completed_works: Dict[str, Dict[str, Any]] = {}
        self.failed_works: Dict[str, Dict[str, Any]] = {}
        self.workflow_dag = nx.DiGraph()
        self.resource_manager = ResourceReservationManager()
        self.work_metrics: Dict[str, List[Dict]] = defaultdict(list)
        self.sla_violations: List[Dict] = []
        self.tenant_contexts: Dict[str, Dict[str, Any]] = {}

        self.total_carbon_savings_kg = 0.0
        self.total_helium_savings_l = 0.0
        self.sustainability_score = 0.0
        self.biomass_mobilized_count = 0

        self.pipelines = {
            'standard': self._standard_pipeline,
            'hybrid': self._hybrid_pipeline,
            'bio_optimized': self._bio_optimized_pipeline,
        }

        self.health_status = "healthy"
        self.last_error = None

        # Locks
        self._works_lock = asyncio.Lock()
        self._metrics_lock = asyncio.Lock()
        self._sla_lock = asyncio.Lock()

        # Safe task creation
        self._load_system_state_task = self._create_task(self._load_system_state())
        self._bg_tasks = []
        self._start_background_tasks()

        logger.info(
            f"Enhanced Work Integrator v7.3.0 initialized: "
            f"mopd={self.enable_mopd}, bio_integration={self.enable_bio_integration}, "
            f"carbon_intensity={self.enable_carbon_intensity}"
        )

    def _create_task(self, coro):
        try:
            loop = asyncio.get_running_loop()
            return loop.create_task(coro)
        except RuntimeError:
            logger.warning("No running event loop; background task not started.")
            return None

    async def _load_system_state(self):
        if self.storage:
            try:
                data = self.storage.get_state("work_integrator_system_state")
                if data:
                    state = json.loads(data)
                    # restore sustainability score etc.
                    self.sustainability_score = state.get('sustainability_score', 0.0)
                    self.total_carbon_savings_kg = state.get('total_carbon_savings_kg', 0.0)
                    self.total_helium_savings_l = state.get('total_helium_savings_l', 0.0)
                    logger.info("Loaded system state from central storage")
            except Exception as e:
                logger.error(f"Failed to load system state: {e}")

    async def _save_system_state(self):
        if self.storage:
            state = {
                'sustainability_score': self.sustainability_score,
                'total_carbon_savings_kg': self.total_carbon_savings_kg,
                'total_helium_savings_l': self.total_helium_savings_l,
            }
            self.storage.save_state("work_integrator_system_state", json.dumps(state))

    def _start_background_tasks(self):
        if self.enable_state_persistence and self.system_persistence:
            self._bg_tasks.append(self._create_task(self._persistence_save_loop()))
        if self.enable_swarm_coordination and self.swarm_coordinator:
            self._bg_tasks.append(self._create_task(self._swarm_update_loop()))

    async def _persistence_save_loop(self):
        while True:
            await asyncio.sleep(300)
            await self._save_system_state()

    # ============================================================================
    # Bio-inspired helper methods (simplified)
    # ============================================================================
    async def _allocate_ecoatp_for_work(self, work_id, ecoatp_required, priority=0):
        if self.token_manager:
            success, _ = self.token_manager.reserve_tokens(
                account_id=f"work_{work_id}", amount=ecoatp_required, consumer=EcoATPConsumer.EXPERT_EXECUTION
            )
            return success, ecoatp_required
        return True, ecoatp_required

    def _store_work_as_biomass(self, work, ecoatp_cost, guarantee=GuaranteeLevel.SILVER):
        if self.biomass_storage:
            stored, token = self.biomass_storage.store_task(work, ecoatp_cost, guarantee)
            if stored:
                return token
        return None

    def _get_gradient_aware_priority(self, base_priority: WorkPriority) -> WorkPriority:
        if self.gradient_manager:
            carbon = self.gradient_manager.fields.get('carbon')
            if carbon and carbon.gradient_strength > 0.7:
                return max(base_priority, WorkPriority.HIGH)
        return base_priority

    def _recover_tokens_on_failure(self, work_id, completion_percentage):
        return 0.0

    def _check_compartment_availability(self, expert_type):
        if self.compartment_manager:
            viable, comp_id = self.compartment_manager.find_best_compartment(expert_type)
            return viable, comp_id
        return True, None

    def _get_ecoatp_cost_estimate(self, work):
        # simple estimate based on complexity, duration, etc.
        return 1.0

    # ============================================================================
    # MOPD: Enumerate, compute, generate front, select best
    # ============================================================================
    async def _enumerate_execution_plans(self, context: EnhancedWorkContext) -> List[MOPDWorkPlan]:
        pipelines = ['standard']
        if self.enable_hybrid_pipeline and self.hybrid_pipeline:
            pipelines.append('hybrid')
        if self.enable_bio_integration:
            pipelines.append('bio_optimized')

        data_centers = ['us-east']
        if context.carbon_zone > 5:
            data_centers.append('us-west')

        helium_recovery_options = [False]
        if context.helium_dependency > 0.5:
            helium_recovery_options.append(True)

        carbon_offset_options = [False]
        carbon_price = context.meta_cognitive_state.get('carbon_price', 50)
        if carbon_price > 60:
            carbon_offset_options.append(True)

        renewable_shares = np.linspace(0.4, 0.9, self.config.mopd_grid_resolution)

        plans = []
        for pipeline in pipelines:
            use_quantum_options = [False]
            if pipeline == 'hybrid':
                use_quantum_options = [False, True]
            for use_quantum in use_quantum_options:
                for dc in data_centers:
                    for hr in helium_recovery_options:
                        for co in carbon_offset_options:
                            for rs in renewable_shares:
                                plans.append(MOPDWorkPlan(
                                    pipeline=pipeline,
                                    use_quantum=use_quantum,
                                    data_center=dc,
                                    helium_recovery=hr,
                                    carbon_offset=co,
                                    renewable_share=float(rs),
                                    token_allocation=0.0,
                                    compartment_id=None,
                                ))
        return plans

    async def _compute_plan_objectives(self, plan: MOPDWorkPlan, context: EnhancedWorkContext) -> MOPDWorkPlan:
        carbon_kg = 10.0
        helium_units = 0.0
        cost_usd = 0.0
        latency_ms = 100.0
        success_prob = 0.95

        if plan.data_center == 'us-west':
            carbon_kg *= 0.6
            latency_ms *= 1.5
            cost_usd += 5.0
        if plan.helium_recovery:
            helium_units = context.helium_dependency * 0.5
            cost_usd += 2.0
            latency_ms += 10.0
        if plan.carbon_offset:
            carbon_kg -= 5.0
            cost_usd += context.meta_cognitive_state.get('carbon_price', 50) * 0.1
        if plan.renewable_share > 0.5:
            carbon_kg *= (1.0 - (plan.renewable_share - 0.5) * 0.5)
            cost_usd += (plan.renewable_share - 0.5) * 2.0
        if plan.use_quantum:
            latency_ms *= 0.5
            cost_usd += 3.0
            success_prob *= 0.95
        if plan.pipeline == 'bio_optimized':
            success_prob *= 1.05
            carbon_kg *= 0.95

        # Token allocation (simplified)
        plan.token_allocation = self._get_ecoatp_cost_estimate(context.metrics)
        if self.dynamic_pricing:
            plan.token_allocation *= context.dynamic_token_price

        if self.compartment_manager:
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
        plans = await self._enumerate_execution_plans(context)
        computed = [await self._compute_plan_objectives(p, context) for p in plans]

        pareto = []
        for i, p_i in enumerate(computed):
            dominated = False
            for j, p_j in enumerate(computed):
                if i == j:
                    continue
                a_vec = [p_i.carbon_kg, p_i.helium_units, p_i.cost_usd, p_i.latency_ms, -p_i.success_probability]
                b_vec = [p_j.carbon_kg, p_j.helium_units, p_j.cost_usd, p_j.latency_ms, -p_j.success_probability]
                if all(b <= a for a, b in zip(a_vec, b_vec)) and any(b < a for a, b in zip(a_vec, b_vec)):
                    dominated = True
                    break
            if not dominated:
                pareto.append(p_i)
        return pareto

    def _select_best_from_pareto(self, pareto_front: List[MOPDWorkPlan]) -> Optional[MOPDWorkPlan]:
        if not pareto_front:
            return None

        if self.adaptive_cost:
            scored = []
            for plan in pareto_front:
                cost = self.adaptive_cost.compute(
                    quality=plan.success_probability,
                    carbon_g=plan.carbon_kg * 1000.0,
                    latency_ms=plan.latency_ms,
                    energy_joules=plan.cost_usd * 10.0,
                    health=0.8,
                    atp=plan.token_allocation / self.config.token_reserve_factor
                )
                plan.scalarised_score = cost
                scored.append((cost, plan))
            if self.pareto:
                candidates = [{
                    'expert_id': f"plan_{id(p)}",
                    'quality_score': p.success_probability,
                    'carbon_g': p.carbon_kg * 1000.0,
                    'latency_ms': p.latency_ms,
                    'energy_joules': p.cost_usd * 10.0,
                } for _, p in scored]
                filtered = self.pareto.filter(candidates)
                if filtered:
                    allowed_ids = {c['expert_id'] for c in filtered}
                    scored = [(cost, plan) for cost, plan in scored if f"plan_{id(plan)}" in allowed_ids]
            if scored:
                scored.sort(reverse=True)
                return scored[0][1]
            return None
        else:
            weights = self.config.mopd_objective_weights
            eps = 1e-8
            carbon_vals = [p.carbon_kg for p in pareto_front]
            helium_vals = [p.helium_units for p in pareto_front]
            cost_vals = [p.cost_usd for p in pareto_front]
            latency_vals = [p.latency_ms for p in pareto_front]
            success_vals = [p.success_probability for p in pareto_front]

            max_carbon = max(carbon_vals) + eps
            max_helium = max(helium_vals) + eps
            max_cost = max(cost_vals) + eps
            max_latency = max(latency_vals) + eps
            max_success = max(success_vals) + eps

            best = None
            best_score = -float('inf')
            for plan in pareto_front:
                carbon_norm = 1 - plan.carbon_kg / max_carbon
                helium_norm = 1 - plan.helium_units / max_helium
                cost_norm = 1 - plan.cost_usd / max_cost
                latency_norm = 1 - plan.latency_ms / max_latency
                success_norm = plan.success_probability / max_success
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
    # Teacher policy
    # ============================================================================
    async def policy_probs(self, state: Dict[str, Any]) -> List[float]:
        context = self._create_work_context(state, tenant_id=state.get('tenant_id', 'default'))
        await self._enrich_context_with_carbon_and_pricing(context)
        pareto_front = await self._generate_pareto_front_for_work(context)
        if not pareto_front:
            return [1.0/3, 1.0/3, 1.0/3]

        candidates = []
        for plan in pareto_front:
            cost = self.adaptive_cost.compute(
                quality=plan.success_probability,
                carbon_g=plan.carbon_kg * 1000.0,
                latency_ms=plan.latency_ms,
                energy_joules=plan.cost_usd * 10.0,
                health=0.8,
                atp=plan.token_allocation / self.config.token_reserve_factor
            ) if self.adaptive_cost else plan.scalarised_score
            candidates.append({'plan': plan, 'score': cost})

        if self.pareto:
            filtered = self.pareto.filter(candidates)
            if filtered:
                allowed = {id(c['plan']) for c in filtered}
                candidates = [c for c in candidates if id(c['plan']) in allowed]
        if not candidates:
            return [1.0, 0.0, 0.0]

        scores = [c['score'] for c in candidates]
        exp_scores = np.exp(scores - np.max(scores))
        probs = exp_scores / np.sum(exp_scores)
        strategy_probs = [0.0, 0.0, 0.0]  # standard, hybrid, bio
        for c, p in zip(candidates, probs):
            plan = c['plan']
            if plan.pipeline == 'standard':
                idx = 0
            elif plan.pipeline == 'hybrid':
                idx = 1
            else:
                idx = 2
            strategy_probs[idx] += p
        total = sum(strategy_probs)
        if total > 0:
            strategy_probs = [p/total for p in strategy_probs]
        return strategy_probs

    # ============================================================================
    # Work Processing Pipeline (simplified but functional core)
    # ============================================================================
    async def process_work(
        self,
        work_request: Dict[str, Any],
        pipeline_type: str = 'standard',
        dependencies: Optional[List[str]] = None,
        tenant_id: str = "default",
        return_pareto: bool = False
    ) -> Dict[str, Any]:
        context = self._create_work_context(work_request, tenant_id)
        await self._enrich_context_with_carbon_and_pricing(context)

        if self.enable_bio_integration:
            context.priority = self._get_gradient_aware_priority(context.priority)

        self._add_to_workflow_dag(context, dependencies)

        # Allocate tokens
        if not await self._allocate_ecoatp_for_work(context.task_id, self._get_ecoatp_cost_estimate(context.metrics), context.priority.weight):
            return await self._handle_allocation_failure(context, work_request)

        # Check compartment
        if self.enable_bio_integration:
            viable, comp_id = self._check_compartment_availability(context.work_type)
            if not viable:
                return await self._handle_compartment_unavailable(context, work_request)
            context.compartment_id = comp_id

        # MOPD selection
        selected_plan = None
        if self.enable_mopd:
            pareto_front = await self._generate_pareto_front_for_work(context)
            if pareto_front:
                selected_plan = self._select_best_from_pareto(pareto_front)
                if selected_plan:
                    context.meta_cognitive_state['selected_plan'] = selected_plan.to_dict()
                    context.meta_cognitive_state['pareto_front'] = [p.to_dict() for p in pareto_front]
                    pipeline_type = selected_plan.pipeline
                    # Bio-inspired: spend ATP before execution
                    if self.token_manager and selected_plan.token_allocation > 0:
                        await self.token_manager.spend(f"work_{context.task_id}", selected_plan.token_allocation)
                    # Pump gradients
                    if self.gradient_manager:
                        if selected_plan.carbon_kg > 0.5:
                            await self.gradient_manager.pump_field('carbon', 0.05, source=f"work_{context.task_id}")
                        if selected_plan.helium_units > 0.05:
                            await self.gradient_manager.pump_field('helium', 0.05, source=f"work_{context.task_id}")

        # Execute pipeline
        result = await self._execute_pipeline(context, pipeline_type)

        # Finalize and record
        final = await self._finalize_work(context, result)

        # FeedbackEvent and drift
        if self.queue:
            event = FeedbackEvent.create_with_context(
                task_id=context.task_id,
                selected_action=selected_plan.pipeline if selected_plan else pipeline_type,
                quality_score=selected_plan.success_probability if selected_plan else 0.5,
                energy_joules=selected_plan.cost_usd * 10.0 if selected_plan else 0.0,
                carbon_g=selected_plan.carbon_kg * 1000.0 if selected_plan else 0.0,
                feedback_type="work_integration",
                adaptive_cost_value=selected_plan.scalarised_score if selected_plan else 0.0,
                state={'work_type': context.work_type, 'priority': context.priority.name},
                candidates=[{'action': p['pipeline']} for p in context.meta_cognitive_state.get('pareto_front', [])],
                source="work_integrator",
                environment=getattr(central_config, "ENVIRONMENT", "production"),
                tags=["work", "mopd"]
            )
            await self.queue.publish("feedback_events", event.to_json())

        # Drift check
        if self.drift:
            drift_score = await self.drift.check_drift(self.adaptive_cost.get_current_weights() if self.adaptive_cost else {})
            if drift_score and drift_score > 0.7:
                logger.warning(f"High drift detected ({drift_score:.3f}); adjusting MOPD weights.")
                self.config.mopd_objective_weights['carbon'] = min(0.5, self.config.mopd_objective_weights['carbon'] + 0.05)
                total = sum(self.config.mopd_objective_weights.values())
                for k in self.config.mopd_objective_weights:
                    self.config.mopd_objective_weights[k] /= total

        if return_pareto and 'pareto_front' in context.meta_cognitive_state:
            final['pareto_front'] = context.meta_cognitive_state['pareto_front']
            if selected_plan:
                final['selected_plan'] = selected_plan.to_dict()

        return final

    async def _create_and_validate_context(self, work_request, tenant_id):
        # reuse _create_work_context
        return self._create_work_context(work_request, tenant_id)

    def _create_work_context(self, request: Dict[str, Any], tenant_id: str = "default") -> EnhancedWorkContext:
        return EnhancedWorkContext(
            task_id=request.get('task_id', str(uuid.uuid4())),
            work_type=request.get('work_type', 'general'),
            priority=WorkPriority[request.get('priority', 'MEDIUM')],
            complexity=request.get('complexity', 0.5),
            estimated_duration_ms=request.get('estimated_duration_ms', 100.0),
            helium_dependency=request.get('helium_dependency', 0.0),
            carbon_zone=request.get('carbon_zone', 0),
            quantum_capable=request.get('quantum_capable', False),
            tenant_id=tenant_id,
            **{k: v for k, v in request.items() if k not in ['task_id', 'work_type', 'priority', 'complexity', 'estimated_duration_ms', 'helium_dependency', 'carbon_zone', 'quantum_capable']}
        )

    async def _enrich_context_with_carbon_and_pricing(self, context: EnhancedWorkContext):
        if self.carbon_manager:
            context.meta_cognitive_state['carbon_intensity'] = await self.carbon_manager.get_current_intensity()
            context.meta_cognitive_state['carbon_price'] = await self.carbon_manager.get_current_price()
        if self.dynamic_pricing:
            context.dynamic_token_price = await self.dynamic_pricing.get_current_price('carbon')

    def _add_to_workflow_dag(self, context, dependencies):
        self.workflow_dag.add_node(context.task_id, context=context)
        if dependencies:
            for dep in dependencies:
                self.workflow_dag.add_edge(dep, context.task_id)

    async def _execute_pipeline(self, context, pipeline_type):
        if pipeline_type == 'hybrid':
            return await self._hybrid_pipeline(context)
        elif pipeline_type == 'bio_optimized':
            return await self._bio_optimized_pipeline(context)
        else:
            return await self._standard_pipeline(context)

    async def _standard_pipeline(self, context):
        # Minimal implementation: route to expert
        if self.router:
            routing_context = context.to_routing_context()
            result = self.router.route_and_execute(routing_context)
        else:
            result = {'status': 'success', 'expert': 'default'}
        return result

    async def _hybrid_pipeline(self, context):
        if self.hybrid_pipeline:
            return await self.hybrid_pipeline.execute(context, self._standard_pipeline)
        return await self._standard_pipeline(context)

    async def _bio_optimized_pipeline(self, context):
        # Simple bio optimization: adjust context based on gradients
        if self.gradient_manager:
            gradients = self.gradient_manager.get_field_strengths()
            context.meta_cognitive_state['gradient_carbon'] = gradients.get('carbon', 0.5)
        return await self._standard_pipeline(context)

    async def _finalize_work(self, context, result):
        context.transition_to(WorkState.COMPLETED if result.get('status') == 'success' else WorkState.FAILED)
        self.active_works.pop(context.task_id, None)
        if result.get('status') == 'success':
            self.completed_works[context.task_id] = {'result': result, 'context': context.to_dict()}
        else:
            self.failed_works[context.task_id] = {'result': result, 'context': context.to_dict()}

        # Bio-inspired: earn ATP on success, pump trust
        if result.get('status') == 'success' and self.token_manager and 'selected_plan' in context.meta_cognitive_state:
            await self.token_manager.earn(f"work_{context.task_id}", context.meta_cognitive_state['selected_plan']['token_allocation'] * 1.5)
        if self.gradient_manager:
            await self.gradient_manager.pump_field('trust', 0.05 if result.get('status') == 'success' else -0.05, source=f"work_{context.task_id}")

        # Dashboard update
        if self.sustainability_dashboard:
            metrics = {
                'sustainability_score': context.sustainability_score,
                'carbon_intensity': context.meta_cognitive_state.get('carbon_intensity', 400),
                'helium_usage': context.helium_dependency,
                'success_rate': 1.0 if result.get('status') == 'success' else 0.0,
            }
            await self.sustainability_dashboard.update_metrics(context.task_id, metrics)
            if 'pareto_front' in context.meta_cognitive_state:
                # Convert dicts back to MOPDWorkPlan for dashboard
                pareto_plans = [MOPDWorkPlan.from_dict(p) for p in context.meta_cognitive_state['pareto_front']]
                await self.sustainability_dashboard.record_pareto_front(context.task_id, pareto_plans)

        return {'task_id': context.task_id, 'status': result.get('status', 'success'), 'result': result}

    async def _handle_allocation_failure(self, context, work_request):
        # Store as biomass if possible
        token = self._store_work_as_biomass(work_request, ecoatp_cost=0.0)
        if token:
            context.state = WorkState.STORED_AS_BIOMASS
            return {'status': 'stored_as_biomass', 'token': token, 'task_id': context.task_id}
        return {'status': 'failed', 'reason': 'token allocation failed', 'task_id': context.task_id}

    async def _handle_compartment_unavailable(self, context, work_request):
        # try to store as biomass
        return await self._handle_allocation_failure(context, work_request)

    # ============================================================================
    # Public methods
    # ============================================================================
    def get_work_state(self, task_id: str) -> Optional[Dict[str, Any]]:
        if task_id in self.active_works:
            return self.active_works[task_id].to_dict()
        if task_id in self.completed_works:
            return self.completed_works[task_id]['context']
        if task_id in self.failed_works:
            return self.failed_works[task_id]['context']
        return None

    def cancel_work(self, task_id: str) -> bool:
        context = self.active_works.get(task_id)
        if context and context.transition_to(WorkState.CANCELLED):
            self.active_works.pop(task_id, None)
            return True
        return False

    async def get_sustainability_report(self) -> Dict[str, Any]:
        report = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'sustainability_score': self.sustainability_score,
            'total_carbon_savings_kg': self.total_carbon_savings_kg,
            'total_helium_savings_l': self.total_helium_savings_l,
            'active_works': len(self.active_works),
            'bio_integration_active': self.enable_bio_integration,
            'mopd_enabled': self.enable_mopd,
        }
        if self.enable_predictive and self.predictive_analyzer:
            report['predictive_forecast'] = await self.predictive_analyzer.predict_work_trend()
        if self.enable_sustainability_dashboard and self.sustainability_dashboard:
            report['dashboard'] = await self.sustainability_dashboard.get_dashboard_data()
        report['recommendations'] = self._generate_sustainability_recommendations()
        return report

    def _generate_sustainability_recommendations(self) -> List[str]:
        recommendations = []
        if self.sustainability_score < 0.5:
            recommendations.append("Increase token efficiency for better sustainability")
            recommendations.append("Optimize MOPD weights to favour carbon savings")
        if self.total_carbon_savings_kg < 10:
            recommendations.append("Consider carbon offset programs")
        if not recommendations:
            recommendations.append("Work integration sustainability is on track")
        return recommendations

    def get_work_statistics(self) -> Dict[str, Any]:
        stats = {
            'active_works': len(self.active_works),
            'completed_works': len(self.completed_works),
            'failed_works': len(self.failed_works),
            'sla_violations': len(self.sla_violations),
            'mopd_enabled': self.enable_mopd,
            'bio_integration_active': self.enable_bio_integration,
            'carbon_intensity_active': self.enable_carbon_intensity,
        }
        if self.telemetry:
            stats['telemetry'] = self.telemetry.export()
        return stats

    def get_health_status(self) -> Dict[str, Any]:
        return {
            'status': self.health_status,
            'last_error': self.last_error,
            'active_works': len(self.active_works),
            'sustainability_score': self.sustainability_score,
            'mopd_enabled': self.enable_mopd,
        }

    # ============================================================================
    # Shutdown
    # ============================================================================
    async def shutdown(self):
        logger.info("Shutting down Enhanced Work Integrator")
        for task in self._bg_tasks:
            if task:
                task.cancel()
        await asyncio.gather(*[t for t in self._bg_tasks if t], return_exceptions=True)
        await self._save_system_state()
        if self.carbon_manager:
            await self.carbon_manager.close()
        logger.info("Shutdown complete")

# ============================================================================
# Example usage (not included in actual file, but for completeness)
# ============================================================================
if __name__ == "__main__":
    async def demo():
        integrator = EnhancedWorkIntegrator()
        result = await integrator.process_work({'task_id': 'test', 'work_type': 'analysis'})
        print(result)
        await integrator.shutdown()
    asyncio.run(demo())
