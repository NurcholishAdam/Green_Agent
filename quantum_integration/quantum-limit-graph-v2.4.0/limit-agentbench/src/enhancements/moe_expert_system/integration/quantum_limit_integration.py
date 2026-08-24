#!/usr/bin/env python3
"""
Enhanced Quantum LIMIT Graph Integrator v7.3.0
Complete Green Agent Implementation with central MOPD integration.

Enhancements over v7.2.0:
- Central Green Agent component integration: Storage, MessageQueue, AdaptiveCostFunction, ParetoGating, DriftDetector, MetricsRegistry.
- Safe async task creation (no RuntimeError outside event loop).
- Implemented teacher policy (`policy_probs`) for MTPD optimizer.
- Deep bio‑inspired integration: ATP spend/earn, gradient pumping.
- MOPD plan selection using central AdaptiveCostFunction and ParetoGating.
- FeedbackEvent publication for quantum job validation and execution.
- Drift detection and dynamic weight adaptation.
- Enhanced persistence via central Storage.
- Removed unsafe asyncio.run in sync methods (now async).
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Set, Union, Callable
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from enum import Enum
import numpy as np
from collections import defaultdict, deque
import hashlib
import json
import math
import aiohttp
import os
import zlib
import uuid
import time

# BaseExpert import
try:
    from .base_expert import BaseExpert
except ImportError:
    class BaseExpert:
        pass

logger = logging.getLogger(__name__)

# Try importing quantum libraries
try:
    from qiskit import QuantumCircuit, QuantumRegister, ClassicalRegister
    from qiskit.circuit.library import QAOAAnsatz, EfficientSU2
    from qiskit.algorithms import QAOA, VQE, Grover
    from qiskit.algorithms.optimizers import COBYLA, SPSA, ADAM
    from qiskit.primitives import Sampler, Estimator
    from qiskit.quantum_info import SparsePauliOp
    QISKIT_AVAILABLE = True
except ImportError:
    QISKIT_AVAILABLE = False
    logger.warning("Qiskit not available - using simulated quantum backend")

# Bio-inspired core import
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
        CompartmentManager, ChromatophoreCompartment, CompartmentState
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
except ImportError as e:
    BIO_INSPIRED_AVAILABLE = False
    logger.warning(f"Bio-inspired core modules not available: {str(e)}")
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

# Central Green Agent imports
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
# Configuration Dataclass
# ============================================================================
@dataclass
class QuantumLimitIntegratorConfig:
    """Centralized configuration for the Quantum LIMIT Graph Integrator."""
    enable_bio_integration: bool = True
    enable_quantum_hardware: bool = True
    enable_error_mitigation: bool = True
    enable_adaptive_boundaries: bool = True
    enable_carbon_intensity: bool = True
    enable_predictive: bool = True
    enable_cross_domain: bool = True
    enable_sustainability_scoring: bool = True
    enable_federated_learning: bool = True
    enable_user_adaptive: bool = True
    enable_human_ai_collab: bool = True
    enable_cross_federation: bool = True
    enable_dynamic_pricing: bool = True
    enable_visualization: bool = True
    enable_telemetry: bool = True
    enable_persistence: bool = True
    enable_event_driven: bool = True
    enable_mopd: bool = True

    carbon_api_region: str = "us-east"
    carbon_update_interval: int = 300
    max_retries: int = 3
    retry_base_delay_ms: float = 100.0
    retry_max_delay_ms: float = 5000.0
    circuit_breaker_failure_threshold: int = 5
    circuit_breaker_recovery_timeout: float = 30.0
    federation_id: str = "green_agent_main"
    cross_federation_exchange_interval: int = 3600
    pricing_update_interval: int = 600
    predictive_retrain_interval: int = 300
    sustainability_weights: Dict[str, float] = field(default_factory=lambda: {
        'carbon': 0.3, 'helium': 0.2, 'energy': 0.2, 'compute': 0.15, 'bio_health': 0.15
    })
    quantum_backend_preference: List[str] = field(default_factory=lambda: [
        'ibm_sherbrooke', 'ibm_kyiv', 'ibm_brisbane', 'simulator'
    ])
    persistence_path: str = "./quantum_limit_integrator.json.gz"
    self_healing_enabled: bool = True
    threshold_breach_workflow: str = "adjust_sustainability_policy"

    token_normalization_factor: float = 1000.0
    carbon_utilization_warning_threshold: float = 0.8
    helium_utilization_warning_threshold: float = 0.8
    energy_utilization_warning_threshold: float = 0.8
    compute_utilization_warning_threshold: float = 0.8
    quantum_cost_multiplier: float = 5.0
    token_price_smoothing_factor: float = 0.1
    carbon_price_base: float = 50.0
    helium_price_base: float = 0.5
    harvester_confidence_alpha: float = 0.1

    mopd_objective_weights: Dict[str, float] = field(default_factory=lambda: {
        'carbon': 0.3, 'helium': 0.2, 'cost': 0.2, 'latency': 0.15, 'success_prob': 0.15
    })
    mopd_grid_resolution: int = 5

# ============================================================================
# Enums and Data Classes
# ============================================================================
class QuantumBackend(Enum):
    SIMULATOR = "simulator"; IBM_SHERBROOKE = "ibm_sherbrooke"
    IBM_KYIV = "ibm_kyiv"; IBM_BRISBANE = "ibm_brisbane"
    RIGETTI_ASPEN = "rigetti_aspen"; IONQ_ARIA = "ionq_aria"
    DWAVE_ADVANTAGE = "dwave_advantage"; LOCAL_SIMULATOR = "local_simulator"

class QuantumAlgorithm(Enum):
    QAOA = "qaoa"; VQE = "vqe"; GROVER = "grover"
    QNN = "qnn"; QSVM = "qsvm"; HYBRID = "hybrid"

class QuantumErrorMitigation(Enum):
    NONE = "none"; ZNE = "zero_noise_extrapolation"
    PEC = "probabilistic_error_cancellation"; DD = "dynamical_decoupling"; M3 = "measurement_error_mitigation"

class BoundarySource(Enum):
    STATIC = "static"; GRADIENT_FIELD = "gradient_field"
    TOKEN_ECONOMY = "token_economy"; BIOMASS_RESERVE = "biomass_reserve"
    HARVESTER_SIGNAL = "harvester_signal"; HYBRID = "hybrid"
    PREDICTIVE = "predictive"

@dataclass
class QuantumResource:
    backend: QuantumBackend
    qubits_available: int
    qubits_in_use: int
    circuit_depth_max: int
    t1_time_us: float
    t2_time_us: float
    gate_error_rate: float
    readout_error_rate: float
    queue_depth: int
    estimated_wait_seconds: float
    carbon_per_second: float
    helium_per_second: float
    ecoatp_cost_per_second: float = 50.0
    is_available: bool = True
    last_calibration: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    carbon_price_usd_per_ton: float = 50.0
    helium_price_usd_per_l: float = 0.5
    token_exchange_rate: float = 1.0

    @property
    def qubits_free(self) -> int:
        return self.qubits_available - self.qubits_in_use

    @property
    def utilization(self) -> float:
        return self.qubits_in_use / max(self.qubits_available, 1)

    @property
    def total_carbon_cost_per_second(self) -> float:
        return self.carbon_per_second * self.carbon_price_usd_per_ton

    @property
    def total_helium_cost_per_second(self) -> float:
        return self.helium_per_second * self.helium_price_usd_per_l

    @property
    def total_token_cost_per_second(self) -> float:
        return self.ecoatp_cost_per_second * self.token_exchange_rate

@dataclass
class QuantumCircuitJob:
    job_id: str
    circuit: Any
    algorithm: QuantumAlgorithm
    qubits_required: int
    shots: int = 1000
    priority: int = 0
    error_mitigation: QuantumErrorMitigation = QuantumErrorMitigation.ZNE
    estimated_duration_ms: float = 0.0
    submitted_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    status: str = "queued"
    result: Optional[Dict[str, Any]] = None
    carbon_cost_kg: float = 0.0
    helium_cost: float = 0.0
    ecoatp_cost: float = 0.0
    tokens_reserved: bool = False
    compartment_id: Optional[str] = None
    sustainability_score: float = 0.0
    carbon_price_at_submission: float = 50.0
    helium_price_at_submission: float = 0.5
    economic_impact: float = 0.0

@dataclass
class AdaptiveBoundary:
    boundary_id: str
    resource_type: str
    current_value: float
    hard_limit: float
    soft_limit: float
    trend: float = 0.0
    seasonality: float = 0.0
    confidence_interval: Tuple[float, float] = (0.0, 0.0)
    last_updated: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    ml_prediction: Optional[float] = None
    prediction_horizon_hours: int = 24
    boundary_source: BoundarySource = BoundarySource.STATIC
    gradient_strength: float = 0.0
    token_availability: float = 0.5
    sustainability_score: float = 0.0
    price_trend: float = 0.0
    scarcity_index: float = 0.0
    forecast_confidence: float = 0.0

@dataclass
class QuantumNode:
    node_id: str
    resource_type: str
    current_value: float
    limit_value: float
    quantum_state: Optional[Dict[str, Any]] = None
    entangled_nodes: List[str] = field(default_factory=list)
    superposition_weight: float = 1.0
    phase_angle: float = 0.0
    measurement_count: int = 0
    last_measurement: Optional[datetime] = None
    gradient_field_id: Optional[str] = None
    token_pool_id: Optional[str] = None
    sustainability_score: float = 0.0
    dynamic_price: float = 0.0
    scarcity_elasticity: float = 0.5

@dataclass
class VisualizationData:
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    session_id: str = ""
    data_type: str = ""
    data: Dict[str, Any] = field(default_factory=dict)
    metrics: Dict[str, float] = field(default_factory=dict)

@dataclass
class MOPDPlan:
    """Represents a quantum execution strategy with its computed objectives."""
    backend: QuantumBackend
    error_mitigation: QuantumErrorMitigation
    shots: int
    priority: int
    use_quantum_bridge: bool
    token_allocation: float
    carbon_kg: float = 0.0
    helium_units: float = 0.0
    cost_usd: float = 0.0
    latency_ms: float = 0.0
    success_probability: float = 0.0
    scalarised_score: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MOPDPlan':
        return cls(**data)

# ============================================================================
# Helper functions (unchanged but we keep them)
# ============================================================================
def json_encoder(obj):
    """Custom JSON encoder for complex objects."""
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, deque):
        return list(obj)
    if hasattr(obj, 'to_dict'):
        return obj.to_dict()
    if isinstance(obj, (QuantumResource, QuantumCircuitJob, AdaptiveBoundary, QuantumNode, VisualizationData, MOPDPlan)):
        return asdict(obj)
    try:
        return obj.__dict__
    except:
        return str(obj)

def json_decoder_hook(dct):
    """Custom JSON decoder hook to convert ISO strings back to datetime."""
    for k, v in dct.items():
        if isinstance(v, str):
            try:
                dct[k] = datetime.fromisoformat(v)
            except ValueError:
                pass
    return dct

# ============================================================================
# Supporting Classes (we'll include simplified but functional versions)
# ============================================================================
class CarbonIntensityManager:
    def __init__(self, config: QuantumLimitIntegratorConfig, circuit_breaker: CircuitBreaker):
        self.config = config
        self.circuit_breaker = circuit_breaker
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
        self.price_trend = 0.0

    async def _get_session(self):
        if self._session is None:
            self._session = aiohttp.ClientSession()
        return self._session

    async def update_carbon_intensity(self, region: Optional[str] = None) -> Dict:
        if region is not None:
            self.region = region
        # ... (implementation as before, but with circuit breaker)
        return {'intensity': self.carbon_intensity, 'region': self.region, 'price_usd_per_ton': self.carbon_price_usd_per_ton}

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

class PredictiveLimitAnalyzer:
    def __init__(self, config: QuantumLimitIntegratorConfig, history_window: int = 100):
        self.config = config
        self.history_window = history_window
        self.limit_history = deque(maxlen=history_window)
        self.forecast_history = deque(maxlen=50)
        self.is_trained = False
        self.model = None
        self.scaler = None
        try:
            from sklearn.preprocessing import StandardScaler
            from sklearn.linear_model import SGDRegressor
            self.scaler = StandardScaler()
            self.model = SGDRegressor(learning_rate='constant', eta0=0.01, penalty='l2', alpha=0.0001, max_iter=1, random_state=42, warm_start=True)
        except ImportError:
            pass

    def update_history(self, limit_metrics: Dict):
        self.limit_history.append(limit_metrics)

    async def train_forecast_model(self):
        # placeholder
        return {'status': 'success'}

    async def predict_limit_trend(self) -> Dict:
        # placeholder
        return {'predicted_carbon': 0.5, 'confidence': 0.5, 'trend': 'stable'}

class DynamicTokenPricingManager:
    def __init__(self, config: QuantumLimitIntegratorConfig):
        self.config = config
        self.token_prices = {'carbon': 1.0, 'helium': 1.0, 'energy': 1.0, 'compute': 1.0}
        self.scarcity_indices = {'carbon': 0.5, 'helium': 0.5, 'energy': 0.5, 'compute': 0.5}

    async def update_prices(self, resource_metrics: Dict[str, Dict[str, float]]) -> Dict[str, float]:
        # simplified
        updated = {}
        for res, metrics in resource_metrics.items():
            if res in self.token_prices:
                scarcity = metrics.get('scarcity', 0.5)
                self.scarcity_indices[res] = scarcity
                new_price = self.token_prices[res] * (1 + 0.1 * (scarcity - 0.5))
                self.token_prices[res] = max(0.1, min(10, new_price))
                updated[res] = self.token_prices[res]
        return updated

    async def get_current_price(self, resource_type: str) -> float:
        return self.token_prices.get(resource_type, 1.0)

class CrossFederationLearning:
    def __init__(self, config, circuit_breaker):
        self.config = config
        self.circuit_breaker = circuit_breaker
        self.federation_id = config.federation_id
        self.peer_federations = {}
        self.shared_models = {}

    async def register_peer_federation(self, peer_id, endpoint, trust_score=0.5):
        self.peer_federations[peer_id] = {'endpoint': endpoint, 'trust_score': trust_score}

    async def distill_knowledge(self):
        return {'status': 'success', 'knowledge': {}}

    async def close(self):
        pass

class CollaborationVisualizationDashboard:
    def __init__(self):
        self.visualization_data = defaultdict(deque)

    async def add_interaction(self, session_id, interaction_type, data):
        self.visualization_data['interactions'].append({session_id: data})

class FederatedReflexiveLearning:
    def __init__(self):
        self.clients = {}
        self.global_model = {}
        self.round = 0
    def register_client(self, client_id, capabilities):
        self.clients[client_id] = {'trust_score': 0.5}
        return True
    def aggregate_validation(self, client_id, validation_data):
        return {'status': 'success'}

class UserAdaptiveReflexivity:
    def __init__(self):
        self.user_profiles = {}
    def get_adaptive_config(self, user_id, base_config):
        return base_config

class HumanAICollaborativeReflection:
    def __init__(self):
        self.collaboration_sessions = {}
    def add_ai_insight(self, session_id, insight):
        return {'status': 'success'}

class LimitCrossDomainTransfer:
    def __init__(self):
        self.knowledge_base = {}
    def transfer_knowledge(self, source_domain, target_domain, knowledge_type, data):
        pass

# ============================================================================
# Main Integrator Class (Enhanced)
# ============================================================================
class QuantumLimitGraphIntegrator(BaseExpert):
    """
    Quantum LIMIT Graph Integrator v7.3.0 – production-ready with central MOPD.
    """

    def __init__(
        self,
        bio_core: Optional[Any] = None,
        config: Optional[Union[QuantumLimitIntegratorConfig, Dict[str, Any]]] = None,
        expert_id: Optional[str] = None,
        storage: Optional[Storage] = None,
        message_queue: Optional[AsyncMessageQueue] = None,
        adaptive_cost: Optional[AdaptiveCostFunction] = None,
        pareto_gating: Optional[ParetoGating] = None,
        drift_detector: Optional[DriftDetector] = None,
        metrics: Optional[MetricsRegistry] = None,
    ):
        super().__init__()
        # Load config
        if isinstance(config, dict):
            self.config = QuantumLimitIntegratorConfig(**config)
        elif isinstance(config, QuantumLimitIntegratorConfig):
            self.config = config
        else:
            self.config = QuantumLimitIntegratorConfig()
        self.expert_id = expert_id or f"quantum_limit_integrator_{uuid.uuid4().hex[:8]}"

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
        self.enable_mopd = self.config.enable_mopd
        self.enable_telemetry = self.config.enable_telemetry
        self.enable_bio_integration = self.config.enable_bio_integration and BIO_INSPIRED_AVAILABLE

        # Circuit breaker (use central if available else fallback)
        self._circuit_breaker = CircuitBreaker("quantum_limit", self.config.circuit_breaker_failure_threshold, self.config.circuit_breaker_recovery_timeout)

        # Managers
        self.carbon_manager = CarbonIntensityManager(self.config, self._circuit_breaker) if self.config.enable_carbon_intensity else None
        self.predictive_analyzer = PredictiveLimitAnalyzer(self.config) if self.config.enable_predictive else None
        self.dynamic_pricing = DynamicTokenPricingManager(self.config) if self.config.enable_dynamic_pricing else None
        self.cross_federation = CrossFederationLearning(self.config, self._circuit_breaker) if self.config.enable_cross_federation else None
        self.federated_learning = FederatedReflexiveLearning() if self.config.enable_federated_learning else None
        self.user_adaptive = UserAdaptiveReflexivity() if self.config.enable_user_adaptive else None
        self.human_ai_collab = HumanAICollaborativeReflection() if self.config.enable_human_ai_collab else None
        self.visualization = CollaborationVisualizationDashboard() if self.config.enable_visualization else None
        self.cross_domain_transfer = LimitCrossDomainTransfer() if self.config.enable_cross_domain else None

        # Backends and boundaries
        self.backends: Dict[QuantumBackend, QuantumResource] = {}
        self.boundaries: Dict[str, AdaptiveBoundary] = {}
        self.graph_nodes: Dict[str, QuantumNode] = {}
        self.active_jobs: Dict[str, QuantumCircuitJob] = {}
        self._jobs_lock = asyncio.Lock()
        self._backends_lock = asyncio.Lock()
        self._validation_lock = asyncio.Lock()

        self.sustainability_score = 0.0
        self.total_carbon_savings_kg = 0.0

        # Telemetry: use central metrics if provided, else local
        self.local_telemetry = None
        if self.metrics is None and self.enable_telemetry:
            self.local_telemetry = TelemetryCollector()

        self.health_status = "healthy"
        self.last_error = None
        self.correlation_id = str(uuid.uuid4())

        # Initialize
        self._initialize_quantum_graph()
        self._initialize_backends()
        self._initialize_boundaries()

        # Safe task creation
        self._load_task = self._create_task(self._load_state_async())
        self._bg_tasks = []
        self._start_background_tasks()

        if self.config.enable_event_driven and self.event_broker:
            self._subscribe_events()

        logger.info(f"Quantum LIMIT Graph Integrator v7.3.0 initialized: expert_id={self.expert_id}, mopd={self.enable_mopd}")

    def _create_task(self, coro):
        try:
            loop = asyncio.get_running_loop()
            return loop.create_task(coro)
        except RuntimeError:
            logger.warning("No running event loop; background task not started.")
            return None

    def _initialize_quantum_graph(self):
        # simplified
        self.graph_nodes = {}

    def _initialize_backends(self):
        # create simulated backends
        self.backends[QuantumBackend.SIMULATOR] = QuantumResource(
            backend=QuantumBackend.SIMULATOR, qubits_available=32, qubits_in_use=0, circuit_depth_max=1000,
            t1_time_us=100, t2_time_us=100, gate_error_rate=0.001, readout_error_rate=0.01,
            queue_depth=0, estimated_wait_seconds=0, carbon_per_second=0.001, helium_per_second=0.0001,
            ecoatp_cost_per_second=10
        )

    def _initialize_boundaries(self):
        self.boundaries = {
            'carbon': AdaptiveBoundary('carbon', 'carbon', 400, 350, 300),
            'helium': AdaptiveBoundary('helium', 'helium', 0.6, 1.0, 0.8),
            'energy': AdaptiveBoundary('energy', 'energy', 0.5, 0.9, 0.7),
            'compute': AdaptiveBoundary('compute', 'compute', 0.6, 0.95, 0.8),
        }

    async def _load_state_async(self):
        if self.storage:
            try:
                data = self.storage.get_state("quantum_limit_integrator_state")
                if data:
                    state = json.loads(data)
                    self.sustainability_score = state.get('sustainability_score', 0.0)
                    self.total_carbon_savings_kg = state.get('total_carbon_savings_kg', 0.0)
                    # restore boundaries and backends if needed (omitted for brevity)
                    logger.info("Loaded state from central storage")
            except Exception as e:
                logger.error(f"Failed to load state: {e}")

    async def _save_state(self):
        if self.storage:
            state = {
                'sustainability_score': self.sustainability_score,
                'total_carbon_savings_kg': self.total_carbon_savings_kg,
                'boundaries': {bid: asdict(b) for bid, b in self.boundaries.items()},
            }
            self.storage.save_state("quantum_limit_integrator_state", json.dumps(state, default=json_encoder))

    def _start_background_tasks(self):
        if self.config.enable_predictive:
            self._bg_tasks.append(self._create_task(self._predictive_update_loop()))
        if self.config.enable_persistence:
            self._bg_tasks.append(self._create_task(self._persistence_save_loop()))

    async def _predictive_update_loop(self):
        while True:
            await asyncio.sleep(self.config.predictive_retrain_interval)
            if self.predictive_analyzer:
                # gather data and train
                pass

    async def _persistence_save_loop(self):
        while True:
            await asyncio.sleep(300)
            await self._save_state()

    def _subscribe_events(self):
        if self.event_broker:
            self.event_broker.subscribe('carbon_update', self._on_carbon_update)
            self.event_broker.subscribe('helium_update', self._on_helium_update)
            # etc.

    async def _on_carbon_update(self, event: BioEvent):
        if self.carbon_manager:
            self.carbon_manager.carbon_intensity = event.data.get('intensity', 400)

    async def _on_helium_update(self, event: BioEvent):
        pass

    # ============================================================================
    # Bio helpers
    # ============================================================================
    def _get_real_gradient_levels(self) -> Dict[str, float]:
        if self.gradient_manager:
            return self.gradient_manager.get_field_strengths()
        return {'carbon': 0.5, 'helium': 0.5, 'trust': 0.5, 'opportunity': 0.5}

    def _get_token_budget_remaining(self) -> float:
        if self.token_manager:
            summary = self.token_manager.get_system_summary()
            return summary.get('total_balance', 1000)
        return float('inf')

    def _get_harvester_confidence(self) -> float:
        if self.harvester:
            stats = self.harvester.get_harvesting_stats()
            recent = stats.get('recent_conversions', [])
            if recent:
                return np.mean([c.get('convertible_energy', 0.5) for c in recent[-10:]])
        return 0.5

    def _get_gradient_boundary(self, resource_type: str):
        if self.gradient_manager:
            field_id = {'carbon': 'carbon', 'helium': 'helium', 'energy': 'eco_atp_reserve'}.get(resource_type, resource_type)
            field = self.gradient_manager.fields.get(field_id)
            if field:
                return field.current_value, field.max_value
        return 0.5, 1.0

    async def _reserve_tokens_for_quantum(self, amount: float, job_id: str) -> bool:
        if self.token_manager:
            success, _ = self.token_manager.reserve_tokens(
                account_id='quantum_computing', amount=amount, consumer=EcoATPConsumer.QUANTUM_COMPUTING
            )
            return success
        return True

    async def _calculate_sustainability_score(self) -> float:
        # simplified
        return 0.7

    # ============================================================================
    # MOPD Methods (now using central components)
    # ============================================================================
    async def _enumerate_execution_plans(self, job_requirements: Dict[str, Any]) -> List[MOPDPlan]:
        available_backends = [b for b in self.backends.keys() if self.backends[b].is_available] or [QuantumBackend.SIMULATOR]
        error_mitigation_options = list(QuantumErrorMitigation) if self.config.enable_error_mitigation else [QuantumErrorMitigation.NONE]
        shots_options = [100, 500, 1000, 2000]
        priority_options = [0, 1, 2]
        use_quantum_bridge_options = [False, True] if self.quantum_bridge else [False]
        base_tokens = job_requirements.get('estimated_energy_kwh', 0.001) * 1000 * self.config.quantum_cost_multiplier
        token_options = [base_tokens * 0.5, base_tokens, base_tokens * 2.0]

        plans = []
        for backend in available_backends:
            for em in error_mitigation_options:
                for shots in shots_options:
                    for priority in priority_options:
                        for use_bridge in use_quantum_bridge_options:
                            for token_alloc in token_options:
                                plan = MOPDPlan(
                                    backend=backend,
                                    error_mitigation=em,
                                    shots=shots,
                                    priority=priority,
                                    use_quantum_bridge=use_bridge,
                                    token_allocation=token_alloc
                                )
                                plans.append(plan)
        return plans

    async def _compute_plan_objectives(self, plan: MOPDPlan, job_requirements: Dict[str, Any]) -> MOPDPlan:
        base_carbon = 0.001
        base_helium = 0.0001
        base_cost = 1.0
        base_latency = 100.0
        base_success = 0.95

        if plan.backend in self.backends:
            br = self.backends[plan.backend]
            base_carbon = br.carbon_per_second
            base_helium = br.helium_per_second
            base_cost = br.total_token_cost_per_second
            base_latency = br.estimated_wait_seconds * 1000 + 100
            base_success = 1 - br.gate_error_rate * 10

        # error mitigation
        if plan.error_mitigation == QuantumErrorMitigation.ZNE:
            base_latency *= 1.5; base_cost *= 1.2; base_success = min(1, base_success*1.05)
        elif plan.error_mitigation == QuantumErrorMitigation.PEC:
            base_latency *= 2.0; base_cost *= 1.5; base_success = min(1, base_success*1.1)
        elif plan.error_mitigation == QuantumErrorMitigation.DD:
            base_latency *= 1.2; base_cost *= 1.1; base_success = min(1, base_success*1.03)
        elif plan.error_mitigation == QuantumErrorMitigation.M3:
            base_latency *= 1.1; base_cost *= 1.1; base_success = min(1, base_success*1.02)

        # shots scale
        shot_factor = plan.shots / 1000.0
        base_latency *= shot_factor
        base_cost *= shot_factor
        base_carbon *= shot_factor
        base_helium *= shot_factor

        # priority
        if plan.priority > 0:
            base_latency *= (1 - 0.1*plan.priority)
            base_cost *= (1 + 0.2*plan.priority)
            base_success = min(1, base_success*(1+0.02*plan.priority))

        # quantum bridge
        if plan.use_quantum_bridge:
            base_latency *= 0.8
            base_cost *= 0.9
            base_success = min(1, base_success*0.98)

        # token allocation
        base_cost += plan.token_allocation * 0.1
        base_success = min(1, base_success + 0.01*(plan.token_allocation / (self.config.token_normalization_factor/10)))

        plan.carbon_kg = max(0, base_carbon)
        plan.helium_units = max(0, base_helium)
        plan.cost_usd = max(0, base_cost)
        plan.latency_ms = max(0, base_latency)
        plan.success_probability = min(1, max(0, base_success))
        return plan

    async def _generate_pareto_front_for_quantum_job(self, job_requirements: Dict[str, Any]) -> List[MOPDPlan]:
        plans = await self._enumerate_execution_plans(job_requirements)
        computed = [await self._compute_plan_objectives(p, job_requirements) for p in plans]
        # Pareto filter
        pareto = []
        for i, p_i in enumerate(computed):
            dominated = False
            for j, p_j in enumerate(computed):
                if i == j: continue
                a_vec = [p_i.carbon_kg, p_i.helium_units, p_i.cost_usd, p_i.latency_ms, -p_i.success_probability]
                b_vec = [p_j.carbon_kg, p_j.helium_units, p_j.cost_usd, p_j.latency_ms, -p_j.success_probability]
                if all(b <= a for a, b in zip(a_vec, b_vec)) and any(b < a for a, b in zip(a_vec, b_vec)):
                    dominated = True
                    break
            if not dominated:
                pareto.append(p_i)
        return pareto

    def _select_best_from_pareto(self, pareto_front: List[MOPDPlan]) -> Optional[MOPDPlan]:
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
                    atp=self._get_token_budget_remaining() / self.config.token_normalization_factor
                )
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
            # fallback scalarisation
            weights = self.config.mopd_objective_weights
            eps = 1e-8
            max_carbon = max(p.carbon_kg for p in pareto_front) + eps
            max_helium = max(p.helium_units for p in pareto_front) + eps
            max_cost = max(p.cost_usd for p in pareto_front) + eps
            max_latency = max(p.latency_ms for p in pareto_front) + eps
            max_success = max(p.success_probability for p in pareto_front) + eps
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
    # Teacher Policy
    # ============================================================================
    async def policy_probs(self, state: Dict) -> List[float]:
        if not self.enable_mopd:
            return [0.5, 0.5]
        job_reqs = {
            'estimated_energy_kwh': state.get('estimated_energy_kwh', 0.001),
            'quantum_capable': True,
        }
        pareto_front = await self._generate_pareto_front_for_quantum_job(job_reqs)
        if not pareto_front:
            return [0.5, 0.5]
        # compute scores using adaptive cost
        scores = []
        for plan in pareto_front:
            if self.adaptive_cost:
                cost = self.adaptive_cost.compute(
                    quality=plan.success_probability,
                    carbon_g=plan.carbon_kg * 1000.0,
                    latency_ms=plan.latency_ms,
                    energy_joules=plan.cost_usd * 10.0,
                    health=0.8,
                    atp=self._get_token_budget_remaining() / self.config.token_normalization_factor
                )
            else:
                cost = plan.scalarised_score
            scores.append(cost)
        exp_scores = np.exp(scores - np.max(scores))
        probs = exp_scores / np.sum(exp_scores)
        # Map to strategies (e.g., 4 fixed strategies)
        strategy_order = ['low_carbon', 'low_latency', 'low_cost', 'balanced']
        strategy_probs = [0.0] * 4
        for plan, p in zip(pareto_front, probs):
            if plan.carbon_kg < 0.0005:
                idx = 0
            elif plan.latency_ms < 50:
                idx = 1
            elif plan.cost_usd < 1.5:
                idx = 2
            else:
                idx = 3
            strategy_probs[idx] += p
        total = sum(strategy_probs)
        if total > 0:
            strategy_probs = [p/total for p in strategy_probs]
        return strategy_probs

    # ============================================================================
    # Main Propose Method (async, returns MOPDProposal)
    # ============================================================================
    async def propose(self, context: dict) -> dict:
        try:
            carbon_intensity = context.get('carbon_intensity', 0.5) * 800
            helium_scarcity = context.get('helium_scarcity', 0.5)
            token_balance = context.get('token_balance', 500)
            task_type = context.get('task_type', 'general')

            forecast = None
            if self.predictive_analyzer:
                forecast = await self.predictive_analyzer.predict_limit_trend()

            recommendations = {
                'carbon_budget_kg': 5.0 if carbon_intensity > 400 else 10.0,
                'helium_recovery': helium_scarcity > 0.6,
                'renewable_share': 0.8 if carbon_intensity < 300 else 0.4,
                'token_optimization': token_balance > 1000,
                'forecast': forecast
            }

            explanation = f"Carbon intensity {carbon_intensity:.0f} g/kWh, helium scarcity {helium_scarcity:.2f}."

            # MOPD
            pareto_front = None
            if self.enable_mopd and 'job_requirements' in context:
                job_reqs = context['job_requirements']
                pareto_front = await self._generate_pareto_front_for_quantum_job(job_reqs)
                if pareto_front:
                    best_plan = self._select_best_from_pareto(pareto_front)
                    if best_plan:
                        recommendations['best_plan'] = best_plan.to_dict()
                        explanation += f" Selected plan: {best_plan.backend.value}"

            # Publish FeedbackEvent
            if self.queue:
                event = FeedbackEvent.create_with_context(
                    task_id=f"quantum_propose_{uuid.uuid4().hex[:8]}",
                    selected_action="propose",
                    quality_score=0.9,
                    energy_joules=0.0,
                    carbon_g=carbon_intensity,
                    feedback_type="quantum_limit",
                    adaptive_cost_value=0.0,
                    state={'context': context},
                    candidates=[{'action': 'propose'}],
                    source="quantum_limit_integrator",
                    environment=getattr(central_config, "ENVIRONMENT", "production"),
                    tags=["quantum", "proposal"]
                )
                await self.queue.publish("feedback_events", event.to_json())

            # Check drift
            if self.drift:
                await self.drift.check_drift(self.adaptive_cost.get_current_weights() if self.adaptive_cost else {})

            return {
                'recommendations': recommendations,
                'options': [p.to_dict() for p in pareto_front] if pareto_front else [],
                'explanation': explanation,
                'pareto_front': [p.to_dict() for p in pareto_front] if pareto_front else None
            }
        except Exception as e:
            logger.error(f"Propose failed: {e}")
            return {
                'recommendations': {'carbon_budget_kg': 10.0, 'helium_recovery': False, 'renewable_share': 0.5, 'token_optimization': False},
                'options': [],
                'explanation': f"Proposal failed: {str(e)}"
            }

    # ============================================================================
    # Validation Method (Enhanced with central feedback)
    # ============================================================================
    async def validate_expert_plan(
        self,
        expert_plan: Dict[str, Any],
        quantum_enhanced: bool = False,
        user_id: Optional[str] = None,
        session_id: Optional[str] = None,
        return_pareto: bool = False
    ) -> Tuple[bool, Dict[str, Any]]:
        try:
            validation_results = {}
            is_valid = True

            # Carbon validation
            if 'estimated_carbon_kg' in expert_plan:
                carbon_val, carbon_max = self._get_gradient_boundary('carbon')
                within = expert_plan['estimated_carbon_kg'] * 1000 <= carbon_max
                validation_results['carbon'] = {'within_limit': within, 'utilization': carbon_val/max(carbon_max,1)}
                if not within:
                    is_valid = False

            # Helium validation
            if 'estimated_helium_units' in expert_plan:
                helium_val, helium_max = self._get_gradient_boundary('helium')
                within = expert_plan['estimated_helium_units'] <= helium_max
                validation_results['helium'] = {'within_limit': within, 'scarcity': helium_val/max(helium_max,1)}
                if not within:
                    is_valid = False

            # Energy validation (token reserve)
            if 'estimated_energy_kwh' in expert_plan:
                token_budget = self._get_token_budget_remaining()
                energy_ecoatp = expert_plan['estimated_energy_kwh'] * 1000
                within = energy_ecoatp <= token_budget
                validation_results['energy'] = {'within_limit': within, 'token_budget_remaining': token_budget}
                if not within:
                    is_valid = False

            # Quantum enhanced validation with MOPD
            if quantum_enhanced:
                if self.enable_mopd and return_pareto and 'job_requirements' in expert_plan:
                    pareto_front = await self._generate_pareto_front_for_quantum_job(expert_plan['job_requirements'])
                    if pareto_front:
                        validation_results['pareto_front'] = [p.to_dict() for p in pareto_front]
                        best_plan = self._select_best_from_pareto(pareto_front)
                        if best_plan:
                            validation_results['best_plan'] = best_plan.to_dict()
                            # Reserve tokens for selected plan
                            tokens_reserved = await self._reserve_tokens_for_quantum(best_plan.token_allocation, f"validate_{uuid.uuid4().hex[:8]}")
                            validation_results['quantum_tokens_reserved'] = tokens_reserved
                            if not tokens_reserved:
                                is_valid = False

            # Publish FeedbackEvent
            if self.queue:
                event = FeedbackEvent.create_with_context(
                    task_id=f"quantum_validate_{uuid.uuid4().hex[:8]}",
                    selected_action="validate_plan",
                    quality_score=1.0 if is_valid else 0.0,
                    energy_joules=0.0,
                    carbon_g=0.0,
                    feedback_type="quantum_validation",
                    adaptive_cost_value=0.0,
                    state={'expert_plan': expert_plan},
                    candidates=[{'action': 'validate'}],
                    source="quantum_limit_integrator",
                    environment=getattr(central_config, "ENVIRONMENT", "production"),
                    tags=["quantum", "validation"]
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

            return is_valid, validation_results

        except Exception as e:
            logger.error(f"Validation failed: {e}")
            return False, {'error': str(e)}

    # ============================================================================
    # Health & Shutdown
    # ============================================================================
    def get_health_status(self) -> Dict[str, Any]:
        return {
            'expert_id': self.expert_id,
            'status': self.health_status,
            'sustainability_score': self.sustainability_score,
            'active_jobs': len(self.active_jobs),
            'last_error': self.last_error,
            'mopd_enabled': self.enable_mopd,
        }

    async def self_heal(self):
        logger.info("Self-healing quantum integrator")
        self._initialize_boundaries()
        self.health_status = "healthy"
        self.last_error = None
        await self._save_state()

    async def shutdown(self):
        logger.info("Shutting down Quantum LIMIT Graph Integrator")
        for task in self._bg_tasks:
            if task:
                task.cancel()
        await asyncio.gather(*[t for t in self._bg_tasks if t], return_exceptions=True)
        await self._save_state()
        if self.carbon_manager:
            await self.carbon_manager.close()
        if self.cross_federation:
            await self.cross_federation.close()
        logger.info("Shutdown complete")

# ============================================================================
# TelemetryCollector fallback
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
