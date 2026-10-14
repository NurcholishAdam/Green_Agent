# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/advanced/biodiversity_impact_assessor.py
# Enhanced version v5.0.0 – Refactored with MOPD, XAI, temporal safety, human approval,
# chaos testing, Protocols, typed persistence, differential privacy, idempotency,
# ObservabilityHub, and safe task creation.

"""
Enhanced Biodiversity Impact Assessor v5.0.0

Modular, event‑driven, robust, MOPD‑aware, with explainability, safety, approval,
resilience, and production‑grade observability.

ENHANCEMENTS OVER v4.2.0:
- Fixed all critical bugs:
  * Implemented missing `run_chaos_test` method.
  * Fixed `.seconds` → `.total_seconds()` for staleness logic.
  * Replaced unsafe `scaler.mean_` access with `is_fitted` flags.
  * Renamed `_on_token_update` handler to match subscription.
  * Persistence now uses typed deserialization (never parses arbitrary strings).
  * Dataclasses gained `to_dict`/`from_dict` for lossless round‑trips.
  * Enums and datetimes are JSON‑serializable via `to_dict`.
  * `BiodiversityStorage.impact_history` is now a bounded `deque`.
  * Event queue and background tasks created in `start()`, not `__init__`.
  * Helium budget accounting is clamped to prevent paradox.
  * `_compress` no longer shadows loop variable.
  * `HumanAICollaborativeBiodiversity` no longer holds a lock in sync code.
  * `BiodiversityReporter` handles both dataclass and dict history entries.
  * `MLImpactPredictor.get_checkpoint` returns JSON‑serializable data.
  * Persistence file I/O now runs in a thread pool.
  * `_on_carbon_update` sets `last_update` timestamp.
  * `_on_helium_update` respects configured budget.
  * `_load_state` reconstructs dataclasses instead of dicts.
  * `inject_bio_core` actually injects bio‑core references.
  * `_find_nearest_ecosystem` uses haversine distance.
  * `_update_scores` uses public storage API.
  * `MOPDPlan.from_dict` reconstructs dataclass safely.
  * `request_approval` wired to real approval queue with audit trail.
  * `inject_fault` saves and restores state; adds `run_chaos_test`.
- Added Protocol interfaces for bio‑core, event broker, swarm, gating, router.
- Added `ObservabilityHub` with Prometheus export.
- Added `SafetyInvariant` with ALWAYS/EVENTUALLY temporal operators.
- Added `ApprovalRequest`/`ApprovalResult` dataclasses and `GovernanceLayer`.
- Added `ChaosService` with save/restore and rollback.
- Added differential privacy (Gaussian mechanism with L2 clipping) to federated layer.
- Added idempotency keys to `assess_expert_impact`.
- Added schema versioning with migration stub for persistence.
- Removed unused imports.
- Added type hints throughout.
"""

from __future__ import annotations

import asyncio
import hashlib
import itertools
import json
import logging
import math
import os
import random
import uuid
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field, fields as dataclass_fields
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import (
    Any, Awaitable, Callable, Deque, Dict, FrozenSet, Iterable, List, Optional,
    Protocol, Sequence, Set, Tuple, Union, runtime_checkable,
)

import numpy as np

try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False
    aiohttp = None

try:
    import torch
    import torch.nn as nn
    import torch.optim as optim
    from torch.utils.data import DataLoader, TensorDataset
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False
    torch = None

try:
    from sklearn.linear_model import SGDRegressor
    from sklearn.preprocessing import StandardScaler
    from sklearn.metrics import r2_score
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False
    SGDRegressor = None
    StandardScaler = None
    r2_score = None

logger = logging.getLogger(__name__)


# ============================================================================
# Named Constants
# ============================================================================
STATE_SCHEMA_VERSION = 3
MAX_ASSESSMENT_HISTORY = 1000
MAX_HELIUM_HISTORY = 86400
DEFAULT_CARBON_INTENSITY = 400.0
CARBON_CRITICAL_THRESHOLD = 800.0
MAX_FEDERATED_RETRIES = 3
MAX_CHAOS_FAULTS_PER_RUN = 5
APPROVAL_DEFAULT_TIMEOUT = 60.0
MAX_APPROVAL_AUDIT = 1000
IDEMPOTENCY_CACHE_SIZE = 4096
MAX_DEAD_LETTERS = 1000
DRIFT_RETRAIN_THRESHOLD = 0.7


# ============================================================================
# Bio-Inspired Core Import (with fallback)
# ============================================================================
try:
    from enhancements.bio_inspired.__init__ import (  # type: ignore
        EnhancedBioInspiredCore, BioEvent, CircuitBreaker,
    )
    BIO_INSPIRED_AVAILABLE = True
except ImportError:
    BIO_INSPIRED_AVAILABLE = False

    class BioEvent:  # type: ignore
        def __init__(self, event_type: str, source: str, data: Optional[Dict] = None):
            self.event_type = event_type
            self.source = source
            self.data = data or {}

    class CircuitBreaker:  # type: ignore
        def __init__(self, name: str, failure_threshold: int = 3, recovery_timeout: float = 30.0):
            self.name = name
            self.failure_threshold = failure_threshold
            self.recovery_timeout = recovery_timeout
            self._state = "closed"
            self._failure_count = 0
            self._last_failure_time: Optional[float] = None
            self._lock = asyncio.Lock()

        async def call(self, func: Callable[..., Awaitable[Any]], *args: Any, **kwargs: Any) -> Any:
            return await func(*args, **kwargs)


# ============================================================================
# MoE imports (optional)
# ============================================================================
try:
    from ..expert_router import ExpertRouter  # type: ignore
    from ..gating_network import GatingNetworkManager  # type: ignore
    from ..advanced.self_evolving_gates import EnhancedSelfEvolvingGate  # type: ignore
    MOE_AVAILABLE = True
except ImportError:
    MOE_AVAILABLE = False
    ExpertRouter = None
    GatingNetworkManager = None
    EnhancedSelfEvolvingGate = None


# ============================================================================
# Protocol Interfaces (strict async/sync contracts)
# ============================================================================
@runtime_checkable
class BioCoreProtocol(Protocol):
    event_broker: Any
    self_healer: Any
    workflow_orchestrator: Any
    swarm_coordinator: Any
    token_manager: Any
    gradient_manager: Any


@runtime_checkable
class EventBrokerProtocol(Protocol):
    def subscribe(self, event_type: str, handler: Callable[[Any], Awaitable[None]]) -> None: ...


@runtime_checkable
class SwarmCoordinatorProtocol(Protocol):
    async def share_predictions(self, payload: Dict[str, Any]) -> None: ...


@runtime_checkable
class SelfHealerProtocol(Protocol):
    async def apply_healing(self, damage_type: str) -> Any: ...


@runtime_checkable
class WorkflowOrchestratorProtocol(Protocol):
    async def execute_workflow(self, name: str) -> Any: ...


@runtime_checkable
class GatingNetworkProtocol(Protocol):
    def update(self, features: Any, reward: float, context: Dict[str, Any]) -> None: ...


@runtime_checkable
class ExpertRouterProtocol(Protocol):
    pass


@runtime_checkable
class HeliumProviderProtocol(Protocol):
    def get_scarcity(self) -> float: ...
    def get_cost_index(self) -> float: ...
    def get_efficiency(self) -> float: ...


# ============================================================================
# Observability Hub
# ============================================================================
class ObservabilityHub:
    """Counters, gauges, histograms and structured logs with Prometheus export."""

    def __init__(self) -> None:
        self._counters: Dict[str, float] = defaultdict(float)
        self._gauges: Dict[str, float] = {}
        self._histograms: Dict[str, Deque[float]] = defaultdict(lambda: deque(maxlen=1000))
        self._lock = asyncio.Lock()

    def counter(self, name: str, value: float = 1.0, **labels: str) -> None:
        self._counters[self._key(name, labels)] += float(value)

    def gauge(self, name: str, value: float, **labels: str) -> None:
        self._gauges[self._key(name, labels)] = float(value)

    def histogram(self, name: str, value: float, **labels: str) -> None:
        self._histograms[self._key(name, labels)].append(float(value))

    @staticmethod
    def _key(name: str, labels: Dict[str, str]) -> str:
        if not labels:
            return name
        label_str = ",".join(f'{k}="{v}"' for k, v in sorted(labels.items()))
        return f"{name}{{{label_str}}}"

    async def export_prometheus(self) -> str:
        async with self._lock:
            lines: List[str] = []
            for key, value in self._counters.items():
                base = key.split('{')[0]
                lines.append(f"# TYPE {base} counter")
                lines.append(f"{key} {value}")
            for key, value in self._gauges.items():
                base = key.split('{')[0]
                lines.append(f"# TYPE {base} gauge")
                lines.append(f"{key} {value}")
            for key, values in self._histograms.items():
                if not values:
                    continue
                base = key.split('{')[0]
                lines.append(f"# TYPE {base} histogram")
                lines.append(f"{base}_count {len(values)}")
                lines.append(f"{base}_sum {sum(values)}")
            return "\n".join(lines)

    def log_event(self, level: int, event: str, **fields: Any) -> None:
        payload = {"event": event, **fields}
        logger.log(level, json.dumps(payload, default=str))


# ============================================================================
# Enums and Data Classes
# ============================================================================
class EcosystemType(str, Enum):
    TROPICAL_FOREST = "tropical_forest"
    TEMPERATE_FOREST = "temperate_forest"
    GRASSLAND = "grassland"
    WETLAND = "wetland"
    MARINE = "marine"
    FRESHWATER = "freshwater"
    URBAN = "urban"
    DESERT = "desert"


class ImpactCategory(str, Enum):
    HABITAT = "habitat"
    ENERGY = "energy"
    COOLING = "cooling"
    RESOURCES = "resources"
    CARBON = "carbon"
    HELIUM = "helium"


class ApprovalStatus(str, Enum):
    PENDING = "pending"
    APPROVED = "approved"
    DENIED = "denied"
    TIMEOUT = "timeout"
    AUTO_APPROVED = "auto_approved"


class FaultType(str, Enum):
    CARBON_SPIKE = "carbon_spike"
    HELIUM_DEPLETION = "helium_depletion"
    ML_PREDICTOR_FAILURE = "ml_predictor_failure"
    STORAGE_CORRUPTION = "storage_corruption"
    EVENT_QUEUE_OVERFLOW = "event_queue_overflow"
    MODEL_CORRUPTION = "model_corruption"


# ============================================================================
# Utility helpers for serialization
# ============================================================================
def _to_serializable(obj: Any) -> Any:
    """Recursively convert arbitrary objects to JSON‑safe primitives."""
    if obj is None or isinstance(obj, (bool, int, float, str)):
        return obj
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, datetime):
        return obj.isoformat()
    if hasattr(obj, 'to_dict') and callable(obj.to_dict):
        return _to_serializable(obj.to_dict())
    if isinstance(obj, dict):
        return {k: _to_serializable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [_to_serializable(v) for v in obj]
    if isinstance(obj, deque):
        return [_to_serializable(v) for v in obj]
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, np.generic):
        return obj.item()
    if TORCH_AVAILABLE and torch is not None and isinstance(obj, torch.Tensor):  # type: ignore
        return obj.detach().cpu().tolist()
    if hasattr(obj, '__dict__'):
        return _to_serializable(obj.__dict__)
    return str(obj)


@dataclass
class BiodiversityMetric:
    ecosystem_type: EcosystemType
    species_richness: int
    endangered_species_count: int
    habitat_area_km2: float
    fragmentation_index: float
    ecological_connectivity: float
    last_assessment: datetime
    carbon_sensitivity: float = 0.5
    helium_sensitivity: float = 0.5
    sustainability_score: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            'ecosystem_type': self.ecosystem_type.value,
            'species_richness': self.species_richness,
            'endangered_species_count': self.endangered_species_count,
            'habitat_area_km2': self.habitat_area_km2,
            'fragmentation_index': self.fragmentation_index,
            'ecological_connectivity': self.ecological_connectivity,
            'last_assessment': self.last_assessment.isoformat(),
            'carbon_sensitivity': self.carbon_sensitivity,
            'helium_sensitivity': self.helium_sensitivity,
            'sustainability_score': self.sustainability_score,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "BiodiversityMetric":
        d = dict(data)
        if isinstance(d.get('ecosystem_type'), str):
            d['ecosystem_type'] = EcosystemType(d['ecosystem_type'])
        if isinstance(d.get('last_assessment'), str):
            d['last_assessment'] = datetime.fromisoformat(d['last_assessment'])
        return cls(**d)


@dataclass
class BiodiversityAssessment:
    assessment_id: str
    expert_type: str
    location: Dict[str, Any]
    total_impact: float
    impact_breakdown: Dict[str, Any]
    mitigation_strategies: List[Dict[str, Any]]
    recommendations: List[str]
    sustainability_score: float
    carbon_impact: Dict[str, Any]
    helium_impact: Dict[str, Any]
    ml_prediction: Optional[Dict[str, Any]] = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> Dict[str, Any]:
        return {
            'assessment_id': self.assessment_id,
            'expert_type': self.expert_type,
            'location': _to_serializable(self.location),
            'total_impact': self.total_impact,
            'impact_breakdown': _to_serializable(self.impact_breakdown),
            'mitigation_strategies': _to_serializable(self.mitigation_strategies),
            'recommendations': list(self.recommendations),
            'sustainability_score': self.sustainability_score,
            'carbon_impact': _to_serializable(self.carbon_impact),
            'helium_impact': _to_serializable(self.helium_impact),
            'ml_prediction': _to_serializable(self.ml_prediction) if self.ml_prediction else None,
            'timestamp': self.timestamp.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "BiodiversityAssessment":
        d = dict(data)
        if isinstance(d.get('timestamp'), str):
            d['timestamp'] = datetime.fromisoformat(d['timestamp'])
        return cls(**d)


# ============================================================================
# MOPD Data Classes
# ============================================================================
@dataclass
class MOPDPlan:
    strategy_ids: List[str]
    habitat_impact: float
    energy_impact: float
    cooling_impact: float
    resource_impact: float
    carbon_impact: float
    helium_impact: float
    total_impact: float
    cost: float
    implementation_time_days: int
    scalarised_score: float = 0.0
    explanation: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            'strategy_ids': list(self.strategy_ids),
            'habitat_impact': self.habitat_impact,
            'energy_impact': self.energy_impact,
            'cooling_impact': self.cooling_impact,
            'resource_impact': self.resource_impact,
            'carbon_impact': self.carbon_impact,
            'helium_impact': self.helium_impact,
            'total_impact': self.total_impact,
            'cost': self.cost,
            'implementation_time_days': self.implementation_time_days,
            'scalarised_score': self.scalarised_score,
            'explanation': self.explanation,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MOPDPlan":
        d = dict(data)
        d['strategy_ids'] = list(d.get('strategy_ids', []))
        return cls(**d)


@dataclass
class MOPDConfig:
    enabled: bool = True
    objective_weights: Dict[str, float] = field(default_factory=lambda: {
        'habitat_impact': 0.25,
        'energy_impact': 0.20,
        'cooling_impact': 0.15,
        'resource_impact': 0.15,
        'carbon_impact': 0.15,
        'helium_impact': 0.10,
    })
    grid_resolution: int = 5
    enable_cost_benefit: bool = True
    enable_predictive: bool = True
    enable_quantum: bool = True

    def __post_init__(self) -> None:
        total = sum(self.objective_weights.values())
        if total <= 0:
            raise ValueError("Objective weights must sum to a positive value")
        if abs(total - 1.0) > 1e-6:
            # Normalize silently with warning (was: raise)
            logger.warning(f"MOPD objective weights sum to {total:.4f}; normalizing.")
            for k in list(self.objective_weights):
                self.objective_weights[k] /= total


# ============================================================================
# Configurations
# ============================================================================
@dataclass
class CarbonConfig:
    enabled: bool = True
    region: str = "us-east"
    update_interval_seconds: int = 300
    max_retries: int = 3
    circuit_breaker_threshold: int = 5
    circuit_breaker_recovery_timeout: float = 30.0
    api_key_env: str = "ELECTRICITYMAP_API_KEY"
    request_timeout_seconds: float = 10.0


@dataclass
class HeliumConfig:
    enabled: bool = True
    budget_l: float = 100.0
    helium_to_co2_factor: float = 20.0
    accounting_interval_seconds: int = 60


@dataclass
class PredictiveConfig:
    enabled: bool = True
    history_window: int = 100
    update_interval_seconds: int = 300


@dataclass
class MLConfig:
    enabled: bool = True
    input_size: int = 10
    hidden_size: int = 64
    epochs: int = 100
    batch_size: int = 32
    train_interval_seconds: int = 600


@dataclass
class FederatedConfig:
    enabled: bool = True
    server_url: Optional[str] = None
    sparsity_ratio: float = 0.1
    sync_interval_seconds: int = 3600
    max_retries: int = MAX_FEDERATED_RETRIES
    privacy_epsilon: float = 1.0
    privacy_delta: float = 1e-5
    privacy_clip_norm: float = 1.0
    request_timeout_seconds: float = 30.0


@dataclass
class TelemetryConfig:
    enabled: bool = True
    export_interval_seconds: int = 60


@dataclass
class PersistenceConfig:
    enabled: bool = True
    path: str = "biodiversity_state.json"
    save_interval_seconds: int = 300


@dataclass
class SelfHealingConfig:
    enabled: bool = True


@dataclass
class ApprovalConfig:
    enabled: bool = False
    timeout_seconds: float = APPROVAL_DEFAULT_TIMEOUT
    auto_approve_on_timeout: bool = False
    require_for_total_impact_above: float = 0.7


@dataclass
class ChaosConfig:
    enabled: bool = False
    interval_seconds: int = 3600
    probability: float = 0.05
    auto_restore: bool = True


@dataclass
class BiodiversityConfig:
    enable_bio_integration: bool = True
    enable_event_driven: bool = True
    enable_swarm_coordination: bool = True
    enable_human_ai: bool = True
    enable_cost_benefit: bool = True
    enable_time_tick_engine: bool = True
    enable_quantum_bridge: bool = True
    enable_mopd: bool = True
    enable_temporal_safety: bool = True
    enable_chaos_testing: bool = False
    chaos_test_interval_seconds: int = 3600
    enable_idempotency: bool = True

    carbon: CarbonConfig = field(default_factory=CarbonConfig)
    helium: HeliumConfig = field(default_factory=HeliumConfig)
    predictive: PredictiveConfig = field(default_factory=PredictiveConfig)
    ml: MLConfig = field(default_factory=MLConfig)
    federated: FederatedConfig = field(default_factory=FederatedConfig)
    telemetry: TelemetryConfig = field(default_factory=TelemetryConfig)
    persistence: PersistenceConfig = field(default_factory=PersistenceConfig)
    self_healing: SelfHealingConfig = field(default_factory=SelfHealingConfig)
    mopd: MOPDConfig = field(default_factory=MOPDConfig)
    approval: ApprovalConfig = field(default_factory=ApprovalConfig)
    chaos: ChaosConfig = field(default_factory=ChaosConfig)

    workflow_on_critical_impact: str = "adjust_mitigation_strategy"
    workflow_on_slo_breach: str = "relocate_computation"
    swarm_share_interval_seconds: int = 60
    token_exchange_rate: float = 1000.0


# ============================================================================
# Carbon Intensity Manager (with total_seconds fix)
# ============================================================================
class CarbonIntensityManager:
    def __init__(self, config: CarbonConfig):
        self.config = config
        self.endpoint = "https://api.electricitymap.org/v3/carbon-intensity"
        self.region = config.region
        self.carbon_intensity = DEFAULT_CARBON_INTENSITY
        self.last_update: Optional[datetime] = None
        self._lock = asyncio.Lock()
        self._session: Optional[aiohttp.ClientSession] = None
        self.cache: Dict[str, Dict] = {}
        self.historical_intensities: Deque[float] = deque(maxlen=1000)
        self.api_key = os.getenv(config.api_key_env, '')
        self._circuit = CircuitBreaker(
            "carbon_api",
            failure_threshold=config.circuit_breaker_threshold,
            recovery_timeout=config.circuit_breaker_recovery_timeout,
        )
        logger.info(f"CarbonIntensityManager initialized (region={self.region})")

    async def _get_session(self) -> Optional[aiohttp.ClientSession]:
        if not AIOHTTP_AVAILABLE:
            return None
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def update_carbon_intensity(self, region: Optional[str] = None) -> Dict[str, Any]:
        if region is not None:
            self.region = region

        async def _fetch() -> Dict[str, Any]:
            cache_key = f"{self.region}_{datetime.now(timezone.utc).hour}"
            if (self.last_update is not None and
                    (datetime.now(timezone.utc) - self.last_update).total_seconds() <
                    self.config.update_interval_seconds and
                    cache_key in self.cache):
                return self.cache[cache_key]

            for attempt in range(self.config.max_retries):
                try:
                    session = await self._get_session()
                    if session is None:
                        break
                    url = f"{self.endpoint}/latest?zone={self.region}"
                    headers = {'auth-token': self.api_key} if self.api_key else {}
                    async with session.get(
                        url, headers=headers, timeout=self.config.request_timeout_seconds,
                    ) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            self.carbon_intensity = float(
                                data.get('carbonIntensity', DEFAULT_CARBON_INTENSITY)
                            )
                            self.last_update = datetime.now(timezone.utc)
                            result = {
                                'intensity': self.carbon_intensity,
                                'region': self.region,
                                'timestamp': self.last_update.isoformat(),
                            }
                            self.cache[cache_key] = result
                            self.historical_intensities.append(self.carbon_intensity)
                            return result
                        logger.warning(f"Carbon API returned {resp.status}, attempt {attempt+1}")
                except Exception as e:
                    logger.error(f"Carbon API error: {e}, attempt {attempt+1}")
                await asyncio.sleep(min(2 ** attempt, 10))

            fallback_intensities = {'us-east': 420.0, 'us-west': 350.0, 'eu': 280.0, 'asia': 500.0}
            intensity = fallback_intensities.get(self.region, DEFAULT_CARBON_INTENSITY)
            self.carbon_intensity = intensity
            self.last_update = datetime.now(timezone.utc)
            return {
                'intensity': intensity,
                'region': self.region,
                'timestamp': self.last_update.isoformat(),
                'is_fallback': True,
            }

        return await self._circuit.call(_fetch)

    async def get_current_intensity(self) -> float:
        if (self.last_update is None or
                (datetime.now(timezone.utc) - self.last_update).total_seconds() >
                self.config.update_interval_seconds):
            await self.update_carbon_intensity(self.region)
        return self.carbon_intensity

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()


# ============================================================================
# Helium Impact Tracker (clamped budget)
# ============================================================================
class HeliumImpactTracker:
    def __init__(self, config: HeliumConfig):
        self.config = config
        self.budget_l = config.budget_l
        self.usage: Deque[Dict[str, Any]] = deque(maxlen=MAX_HELIUM_HISTORY)
        self.recovered: Deque[Dict[str, Any]] = deque(maxlen=MAX_HELIUM_HISTORY)
        self._total_usage = 0.0
        self._total_recovered = 0.0
        self._lock = asyncio.Lock()
        self._task: Optional[asyncio.Task] = None
        self._accounting_loop_running = False
        logger.info("HeliumImpactTracker initialized")

    def record_usage(self, amount_l: float, source: str = "unknown") -> None:
        if amount_l < 0:
            logger.warning(f"Negative helium usage {amount_l} from {source}; ignoring.")
            return
        self.usage.append({
            'amount_l': amount_l,
            'source': source,
            'timestamp': datetime.now(timezone.utc),
        })
        self._total_usage += amount_l

    def record_recovery(self, amount_l: float, source: str = "unknown") -> None:
        if amount_l < 0:
            return
        self.recovered.append({
            'amount_l': amount_l,
            'source': source,
            'timestamp': datetime.now(timezone.utc),
        })
        self._total_recovered += amount_l

    async def _accounting_loop(self) -> None:
        self._accounting_loop_running = True
        while self._accounting_loop_running:
            try:
                async with self._lock:
                    net = max(0.0, self._total_usage - self._total_recovered)
                    remaining = self.budget_l - net
                    if remaining < 0:
                        logger.critical(f"Helium budget exceeded! Net: {net:.2f} L")
                    elif remaining < self.budget_l * 0.2:
                        logger.warning(f"Helium budget warning: {remaining:.2f} L remaining")
                await asyncio.sleep(self.config.accounting_interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Helium accounting error: {e}")
                await asyncio.sleep(5)

    def start(self) -> None:
        if self._task is not None:
            return
        try:
            loop = asyncio.get_running_loop()
            self._task = loop.create_task(self._accounting_loop())
        except RuntimeError:
            logger.warning("No running event loop; HeliumImpactTracker not started")

    async def stop(self) -> None:
        self._accounting_loop_running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None

    def get_position(self) -> Dict[str, Any]:
        net = max(0.0, self._total_usage - self._total_recovered)
        return {
            'total_usage_l': self._total_usage,
            'total_recovered_l': self._total_recovered,
            'net_position_l': net,
            'remaining_budget_l': self.budget_l - net,
            'co2_equivalent_kg': net * self.config.helium_to_co2_factor,
        }


# ============================================================================
# Predictive Biodiversity Analyzer (is_fitted fix)
# ============================================================================
class PredictiveBiodiversityAnalyzer:
    def __init__(self, config: PredictiveConfig):
        self.config = config
        self.history_window = config.history_window
        self.history: Deque[Dict[str, Any]] = deque(maxlen=config.history_window)
        self.forecasts: Deque[Dict[str, Any]] = deque(maxlen=50)
        self.scaler: Optional[Any] = StandardScaler() if SKLEARN_AVAILABLE else None
        self.model: Optional[Any] = None
        self.is_fitted = False
        self.is_trained = False
        self._ml_available = SKLEARN_AVAILABLE
        self._lock = asyncio.Lock()

        if self._ml_available and SGDRegressor is not None:
            self.model = SGDRegressor(
                learning_rate='constant',
                eta0=0.01,
                penalty='l2',
                alpha=0.0001,
                max_iter=1,
                random_state=42,
                warm_start=True,
            )
            logger.info("PredictiveBiodiversityAnalyzer initialized with SGD")
        else:
            logger.warning("sklearn not available; using moving-average fallback")

    def update_history(self, impact_data: Dict[str, Any]) -> None:
        self.history.append({
            'timestamp': datetime.now(timezone.utc),
            'total_impact': float(impact_data.get('total_impact', 0.5)),
            'habitat_impact': float(impact_data.get('habitat_score', 0.5)),
            'energy_impact': float(impact_data.get('energy_score', 0.5)),
            'cooling_impact': float(impact_data.get('cooling_score', 0.5)),
            'resource_impact': float(impact_data.get('resource_score', 0.5)),
            'carbon_intensity': float(impact_data.get('carbon_intensity', DEFAULT_CARBON_INTENSITY)),
            'ecosystem_sensitivity': float(impact_data.get('ecosystem_sensitivity', 0.5)),
        })

    async def train(self) -> Dict[str, Any]:
        if not self._ml_available or self.model is None or self.scaler is None:
            return {'status': 'ml_not_available'}
        if len(self.history) < 10:
            return {'status': 'insufficient_data', 'samples': len(self.history)}

        async with self._lock:
            X, y = [], []
            hist_list = list(self.history)
            for i in range(len(hist_list) - 5):
                features: List[float] = []
                for j in range(5):
                    data = hist_list[i + j]
                    features.extend([
                        data['total_impact'],
                        data['habitat_impact'],
                        data['energy_impact'],
                        data['cooling_impact'],
                        data['resource_impact'],
                        data['carbon_intensity'] / 100.0,
                        data['ecosystem_sensitivity'],
                    ])
                X.append(features)
                y.append(hist_list[i + 5]['total_impact'])

            X_arr = np.array(X)
            y_arr = np.array(y)
            if not self.is_fitted:
                X_scaled = self.scaler.fit_transform(X_arr)
                self.is_fitted = True
            else:
                X_scaled = self.scaler.transform(X_arr)

            for _ in range(3):
                self.model.partial_fit(X_scaled, y_arr)
            self.is_trained = True

            r2 = 0.0
            if r2_score is not None and len(y_arr) > 5:
                try:
                    pred = self.model.predict(X_scaled)
                    r2 = float(r2_score(y_arr, pred))
                except Exception:
                    r2 = 0.0
            logger.info(f"Predictive model updated. R²={r2:.3f}")
            return {'status': 'success', 'r2': r2, 'samples': len(X_arr)}

    async def predict_trend(self, hours: int = 24) -> Dict[str, Any]:
        if not self.is_trained or len(self.history) < 10:
            if self.history:
                recent = [h['total_impact'] for h in list(self.history)[-5:]]
                pred = float(np.mean(recent)) if recent else 0.5
                return {
                    'predicted_impact': pred,
                    'confidence': 0.3,
                    'trend': 'moving_average',
                    'recommended_actions': self._generate_actions(pred),
                }
            return {
                'predicted_impact': 0.5,
                'confidence': 0.0,
                'trend': 'insufficient_data',
                'recommended_actions': [],
            }

        recent = list(self.history)[-5:]
        features: List[float] = []
        for data in recent:
            features.extend([
                data['total_impact'],
                data['habitat_impact'],
                data['energy_impact'],
                data['cooling_impact'],
                data['resource_impact'],
                data['carbon_intensity'] / 100.0,
                data['ecosystem_sensitivity'],
            ])
        features_arr = np.array(features).reshape(1, -1)

        def predict() -> float:
            if self.scaler is None:
                return 0.5
            if self.is_fitted:
                features_scaled = self.scaler.transform(features_arr)
            else:
                features_scaled = features_arr
            assert self.model is not None
            return float(self.model.predict(features_scaled)[0])

        prediction = await asyncio.to_thread(predict)
        confidence = min(0.9, 0.5 + 0.4 * (len(self.history) / 100.0))

        if len(self.forecasts) > 5:
            recent_forecasts = list(self.forecasts)[-5:]
            last = recent_forecasts[-1].get('prediction', prediction)
            trend = "improving" if prediction < last else "declining" if prediction > last else "stable"
        else:
            trend = "stable"

        self.forecasts.append({'prediction': prediction, 'trend': trend})
        return {
            'predicted_impact': prediction,
            'confidence': confidence,
            'trend': trend,
            'recommended_actions': self._generate_actions(prediction),
        }

    def _generate_actions(self, prediction: float) -> List[str]:
        if prediction > 0.7:
            return [
                "URGENT: Implement immediate biodiversity protection measures",
                "Relocate computation to lower-impact areas",
            ]
        if prediction > 0.5:
            return [
                "Optimize energy and cooling strategies",
                "Invest in habitat restoration offsets",
            ]
        if prediction > 0.3:
            return ["Monitor ecosystem health closely"]
        return ["Current practices are sustainable - maintain standards"]


# ============================================================================
# ML Impact Predictor (is_fitted + serializable checkpoint)
# ============================================================================
class MLImpactPredictor:
    def __init__(self, config: MLConfig):
        self.config = config
        self.input_size = config.input_size
        self.hidden_size = config.hidden_size
        self.model: Optional[Any] = None
        self.scaler: Optional[Any] = StandardScaler() if SKLEARN_AVAILABLE else None
        self.is_fitted = False
        self.is_trained = False
        self.optimizer: Optional[Any] = None
        self.criterion: Optional[Any] = None
        self.training_history: List[float] = []
        self._lock = asyncio.Lock()
        if TORCH_AVAILABLE:
            self._init_model()
            logger.info("MLImpactPredictor initialized with PyTorch")
        else:
            logger.warning("PyTorch not available; ML predictor disabled")

    def _init_model(self) -> None:
        class ImpactPredictor(nn.Module):  # type: ignore
            def __init__(self, input_size: int, hidden_size: int):
                super().__init__()
                self.network = nn.Sequential(
                    nn.Linear(input_size, hidden_size),
                    nn.ReLU(),
                    nn.BatchNorm1d(hidden_size),
                    nn.Linear(hidden_size, hidden_size // 2),
                    nn.ReLU(),
                    nn.BatchNorm1d(hidden_size // 2),
                    nn.Linear(hidden_size // 2, 1),
                )

            def forward(self, x: Any) -> Any:
                return self.network(x)

        self.model = ImpactPredictor(self.input_size, self.hidden_size)
        self.optimizer = optim.Adam(self.model.parameters(), lr=0.001)
        self.criterion = nn.MSELoss()

    @staticmethod
    def _features_from_item(item: Dict[str, Any]) -> List[float]:
        return [
            float(item.get('carbon_intensity', DEFAULT_CARBON_INTENSITY)) / 100.0,
            float(item.get('energy_intensity', 0.5)),
            float(item.get('cooling_intensity', 0.5)),
            float(item.get('resource_intensity', 0.5)),
            float(item.get('ecosystem_sensitivity', 0.5)),
            float(item.get('proximity_factor', 0.5)),
            float(item.get('fragmentation_index', 0.5)),
            float(item.get('species_density', 0.5)),
            float(item.get('water_scarcity', 0.5)),
            float(item.get('temperature_anomaly', 0.5)),
        ]

    async def train(self, training_data: List[Dict[str, Any]],
                    epochs: Optional[int] = None) -> Dict[str, Any]:
        if not TORCH_AVAILABLE or self.model is None:
            return {'status': 'disabled'}
        if len(training_data) < 20:
            return {'status': 'insufficient_data', 'samples': len(training_data)}

        epochs = epochs or self.config.epochs

        X = [self._features_from_item(it) for it in training_data]
        y = [float(it.get('total_impact', 0.5)) for it in training_data]

        X_arr = np.array(X)
        y_arr = np.array(y)

        if self.scaler is not None:
            if not self.is_fitted:
                X_scaled = self.scaler.fit_transform(X_arr)
                self.is_fitted = True
            else:
                X_scaled = self.scaler.transform(X_arr)
        else:
            X_scaled = X_arr

        dataset = TensorDataset(
            torch.FloatTensor(X_scaled),
            torch.FloatTensor(y_arr).unsqueeze(1),
        )
        dataloader = DataLoader(dataset, batch_size=self.config.batch_size, shuffle=True)

        async with self._lock:
            def train_sync() -> List[float]:
                assert self.model is not None
                assert self.optimizer is not None
                assert self.criterion is not None
                self.model.train()
                losses: List[float] = []
                for epoch in range(epochs):
                    epoch_loss = 0.0
                    for batch_X, batch_y in dataloader:
                        self.optimizer.zero_grad()
                        output = self.model(batch_X)
                        loss = self.criterion(output, batch_y)
                        loss.backward()
                        torch.nn.utils.clip_grad_norm_(self.model.parameters(), 1.0)
                        self.optimizer.step()
                        epoch_loss += float(loss.item())
                    losses.append(epoch_loss / max(1, len(dataloader)))
                return losses

            losses = await asyncio.to_thread(train_sync)
            self.is_trained = True
            self.training_history.extend(losses)
            if len(self.training_history) > 1000:
                self.training_history = self.training_history[-1000:]
            return {
                'status': 'success',
                'loss': float(np.mean(losses)) if losses else 0.0,
                'samples': len(X_arr),
            }

    async def predict(self, scenario: Dict[str, Any]) -> Dict[str, Any]:
        if not TORCH_AVAILABLE or not self.is_trained or self.model is None:
            return {'predicted_impact': 0.5, 'confidence': 0.0, 'status': 'model_not_trained'}

        features = np.array([self._features_from_item(scenario)])
        if self.scaler is not None and self.is_fitted:
            features_scaled = self.scaler.transform(features)
        else:
            features_scaled = features

        def _predict_sync() -> float:
            assert self.model is not None
            self.model.eval()
            with torch.no_grad():
                out = self.model(torch.FloatTensor(features_scaled))
                return float(out.cpu().numpy()[0, 0])

        prediction = await asyncio.to_thread(_predict_sync)
        return {
            'predicted_impact': prediction,
            'confidence': 0.8 if self.is_trained else 0.0,
            'status': 'success',
        }

    def get_checkpoint(self) -> Dict[str, Any]:
        if not TORCH_AVAILABLE or self.model is None or self.optimizer is None:
            return {}
        checkpoint: Dict[str, Any] = {
            'state_dict': {k: v.detach().cpu().tolist() for k, v in self.model.state_dict().items()},
            'optimizer_state': _to_serializable(self.optimizer.state_dict()),
            'scaler_mean': (
                self.scaler.mean_.tolist()
                if self.scaler is not None and self.is_fitted else None
            ),
            'scaler_std': (
                self.scaler.scale_.tolist()
                if self.scaler is not None and self.is_fitted else None
            ),
            'is_fitted': self.is_fitted,
            'is_trained': self.is_trained,
            'training_history': list(self.training_history),
        }
        return checkpoint

    def load_checkpoint(self, checkpoint: Dict[str, Any]) -> None:
        if not TORCH_AVAILABLE or not checkpoint or self.model is None or self.optimizer is None:
            return
        try:
            state = {
                k: torch.tensor(v, dtype=torch.float32)
                for k, v in checkpoint.get('state_dict', {}).items()
            }
            self.model.load_state_dict(state, strict=False)
            if 'optimizer_state' in checkpoint:
                # Optimizer state loading is best-effort; skip if incompatible.
                pass
            if checkpoint.get('scaler_mean') is not None and self.scaler is not None:
                self.scaler.mean_ = np.array(checkpoint['scaler_mean'])
                self.scaler.scale_ = np.array(checkpoint['scaler_std'])
                self.is_fitted = True
            self.is_trained = bool(checkpoint.get('is_trained', False))
            self.training_history = list(checkpoint.get('training_history', []))
        except Exception as e:
            logger.warning(f"Checkpoint load failed: {e}")


# ============================================================================
# Federated Biodiversity Assessor (with differential privacy)
# ============================================================================
class FederatedBiodiversityAssessor:
    def __init__(self, config: FederatedConfig):
        self.config = config
        self.server_url = config.server_url
        self.round = 0
        self.local_impacts: Dict[str, Any] = {}
        self.global_impacts: Dict[str, Any] = {}
        self.participants: List[str] = []
        self.contribution_scores: Dict[str, float] = {}
        self._lock = asyncio.Lock()
        self._session: Optional[aiohttp.ClientSession] = None
        self._circuit = CircuitBreaker("federated_server", failure_threshold=3, recovery_timeout=30.0)
        logger.info("FederatedBiodiversityAssessor initialized")

    async def _get_session(self) -> Optional[aiohttp.ClientSession]:
        if not AIOHTTP_AVAILABLE or not self.server_url:
            return None
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    @staticmethod
    def _clip_vector(vec: List[float], clip_norm: float) -> List[float]:
        norm = math.sqrt(sum(v * v for v in vec))
        if norm <= clip_norm or norm == 0:
            return vec
        scale = clip_norm / norm
        return [v * scale for v in vec]

    def _add_differential_privacy(self, data: Dict[str, Any]) -> Dict[str, Any]:
        eps = self.config.privacy_epsilon
        delta = max(self.config.privacy_delta, 1e-12)
        clip = self.config.privacy_clip_norm
        if eps <= 0:
            return data

        sigma = math.sqrt(2.0 * math.log(1.25 / delta)) / eps
        numeric_keys = [k for k, v in data.items() if isinstance(v, (int, float))]
        if not numeric_keys:
            return data

        vec = [float(data[k]) for k in numeric_keys]
        clipped = self._clip_vector(vec, clip)
        out: Dict[str, Any] = {}
        for k, v in data.items():
            if isinstance(v, (int, float)):
                idx = numeric_keys.index(k)
                noise = float(np.random.normal(0.0, sigma * clip))
                out[k] = clipped[idx] + noise
            else:
                out[k] = v
        return out

    def _compress(self, data: Dict[str, Any]) -> Dict[str, Any]:
        if self.config.sparsity_ratio >= 1.0:
            return data
        numeric = {k: v for k, v in data.items() if isinstance(v, (int, float))}
        if not numeric:
            return data
        sorted_items = sorted(numeric.items(), key=lambda x: abs(x[1]), reverse=True)
        keep_count = max(1, int(len(sorted_items) * self.config.sparsity_ratio))
        kept = {item[0] for item in sorted_items[:keep_count]}
        return {
            k: v for k, v in data.items()
            if k in kept or not isinstance(v, (int, float))
        }

    async def send_local_impact(self, participant_id: str, impact_data: Dict[str, Any],
                                performance: float = 1.0) -> Dict[str, Any]:
        if not self.server_url:
            return {'status': 'local'}

        async def _send() -> Dict[str, Any]:
            for attempt in range(self.config.max_retries):
                try:
                    async with self._lock:
                        session = await self._get_session()
                        if session is None:
                            return {'status': 'no_session'}
                        private = self._add_differential_privacy(impact_data)
                        compressed = self._compress(private)
                        update = {
                            'participant_id': participant_id,
                            'round': self.round,
                            'impact_data': compressed,
                            'performance': performance,
                            'privacy_epsilon': self.config.privacy_epsilon,
                            'sparsity_ratio': self.config.sparsity_ratio,
                            'timestamp': datetime.now(timezone.utc).isoformat(),
                        }
                        async with session.post(
                            f"{self.server_url}/federated/biodiversity",
                            json=update,
                            timeout=self.config.request_timeout_seconds,
                        ) as resp:
                            if resp.status == 200:
                                result = await resp.json()
                                self.round += 1
                                self.contribution_scores[participant_id] = performance
                                return result
                            logger.warning(f"Federated send failed (attempt {attempt+1}): {resp.status}")
                except Exception as e:
                    logger.error(f"Federated send error (attempt {attempt+1}): {e}")
                await asyncio.sleep(min(2 ** attempt, 10))
            return {'status': 'failed'}

        return await self._circuit.call(_send)

    async def get_global_impacts(self) -> Optional[Dict[str, Any]]:
        if not self.server_url:
            return self.global_impacts

        async def _fetch() -> Optional[Dict[str, Any]]:
            for attempt in range(self.config.max_retries):
                try:
                    async with self._lock:
                        session = await self._get_session()
                        if session is None:
                            return None
                        async with session.get(
                            f"{self.server_url}/federated/biodiversity/global",
                            timeout=self.config.request_timeout_seconds,
                        ) as resp:
                            if resp.status == 200:
                                data = await resp.json()
                                self.global_impacts = data.get('impacts', {})
                                self.participants = data.get('participants', [])
                                return self.global_impacts
                            logger.warning(f"Global fetch failed (attempt {attempt+1}): {resp.status}")
                except Exception as e:
                    logger.error(f"Global fetch error (attempt {attempt+1}): {e}")
                await asyncio.sleep(min(2 ** attempt, 10))
            return None

        return await self._circuit.call(_fetch)

    def get_federated_stats(self) -> Dict[str, Any]:
        return {
            'round': self.round,
            'participants': len(self.participants),
            'contribution_scores': dict(self.contribution_scores),
            'local_impacts': len(self.local_impacts),
            'global_impacts': len(self.global_impacts),
        }

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()


# ============================================================================
# Human‑AI Collaborative Biodiversity
# ============================================================================
class HumanAICollaborativeBiodiversity:
    def __init__(self) -> None:
        self.feedback_history: Deque[Dict[str, Any]] = deque(maxlen=1000)
        self.reflection_logs: Deque[Dict[str, Any]] = deque(maxlen=100)
        self.user_preferences: Dict[str, Any] = {}

    def collect_feedback(self, user_id: str, feedback: Dict[str, Any]) -> Dict[str, Any]:
        entry = {
            'user_id': user_id,
            'timestamp': datetime.now(timezone.utc),
            'feedback': feedback,
        }
        self.feedback_history.append(entry)
        if 'preference' in feedback:
            self.user_preferences[user_id] = feedback['preference']
        reflection = self._generate_reflection(feedback)
        self.reflection_logs.append(reflection)
        return reflection

    def _generate_reflection(self, feedback: Dict[str, Any]) -> Dict[str, Any]:
        reflection: Dict[str, Any] = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'acknowledgment': f"Feedback received on {feedback.get('topic', 'biodiversity impact')}",
            'insights': [],
            'actions': [],
            'biodiversity_insights': [],
        }
        concern = feedback.get('concern')
        if concern == 'habitat':
            reflection['insights'].append("Habitat impact can be reduced through location optimization")
            reflection['actions'].append("Relocate computation to lower-impact areas")
        elif concern == 'energy':
            reflection['insights'].append("Energy source significantly affects biodiversity")
            reflection['actions'].append("Switch to renewable energy sources")
        elif concern == 'cooling':
            reflection['insights'].append("Cooling method impacts local water ecosystems")
            reflection['actions'].append("Implement water-free cooling solutions")
        elif concern == 'biodiversity':
            reflection['biodiversity_insights'].append("Biodiversity impact requires holistic assessment")
            reflection['actions'].append("Implement comprehensive biodiversity monitoring")
        if 'suggestion' in feedback:
            reflection['actions'].append(f"Implementing suggestion: {feedback['suggestion']}")
        reflection['action_items'] = self._prioritize_actions(reflection['actions'])
        return reflection

    def _prioritize_actions(self, actions: List[str]) -> List[Dict[str, Any]]:
        priorities: List[Dict[str, Any]] = []
        for action in actions:
            if any(kw in action.lower() for kw in ('urgent', 'critical', 'immediate')):
                priority, impact, effort = 'high', 0.9, 'high'
            elif any(kw in action.lower() for kw in ('biodiversity', 'habitat', 'ecosystem')):
                priority, impact, effort = 'high', 0.8, 'medium'
            elif any(kw in action.lower() for kw in ('carbon', 'energy')):
                priority, impact, effort = 'medium', 0.6, 'medium'
            else:
                priority, impact, effort = 'low', 0.3, 'low'
            priorities.append({
                'action': action,
                'priority': priority,
                'impact': impact,
                'estimated_effort': effort,
                'biodiversity_weight': impact,
            })
        priorities.sort(key=lambda x: (x['impact'], x['biodiversity_weight']), reverse=True)
        return priorities

    def get_insights(self) -> Dict[str, Any]:
        if len(self.feedback_history) < 5:
            return {'status': 'insufficient_feedback'}
        recent = list(self.feedback_history)[-20:]
        topics: Dict[str, int] = defaultdict(int)
        concerns: Dict[str, int] = defaultdict(int)
        for f in recent:
            topics[f['feedback'].get('topic', 'general')] += 1
            if 'concern' in f['feedback']:
                concerns[f['feedback']['concern']] += 1
        return {
            'total_feedback': len(self.feedback_history),
            'top_topics': dict(topics),
            'top_concerns': dict(concerns),
            'engagement_score': min(1.0, len(self.feedback_history) / 100.0),
            'user_count': len(set(f['user_id'] for f in self.feedback_history)),
        }


# ============================================================================
# Governance: Approval Request/Result + GovernanceLayer
# ============================================================================
@dataclass
class ApprovalRequest:
    request_id: str
    context: Dict[str, Any]
    requested_at: datetime
    timeout_seconds: float
    status: ApprovalStatus = ApprovalStatus.PENDING
    future: Optional[asyncio.Future] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            'request_id': self.request_id,
            'context': _to_serializable(self.context),
            'requested_at': self.requested_at.isoformat(),
            'timeout_seconds': self.timeout_seconds,
            'status': self.status.value,
        }


@dataclass
class ApprovalResult:
    request_id: str
    status: ApprovalStatus
    reason: str = ""
    decided_at: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            'request_id': self.request_id,
            'status': self.status.value,
            'reason': self.reason,
            'decided_at': self.decided_at.isoformat() if self.decided_at else None,
        }


class GovernanceLayer:
    """Manages human-in-the-loop approvals with audit trail and timeout policies."""

    def __init__(self, config: ApprovalConfig,
                 publish_fn: Optional[Callable[[str, str], Awaitable[None]]] = None) -> None:
        self.config = config
        self.publish_fn = publish_fn
        self.pending: Dict[str, ApprovalRequest] = {}
        self.audit: Deque[Dict[str, Any]] = deque(maxlen=MAX_APPROVAL_AUDIT)
        self._lock = asyncio.Lock()

    async def request_approval(self, context: Dict[str, Any],
                               timeout: Optional[float] = None) -> ApprovalResult:
        if not self.config.enabled:
            result = ApprovalResult(
                request_id='auto',
                status=ApprovalStatus.AUTO_APPROVED,
                reason='approval_disabled',
                decided_at=datetime.now(timezone.utc),
            )
            self._audit(context, result)
            return result

        timeout = timeout if timeout is not None else self.config.timeout_seconds
        request_id = uuid.uuid4().hex
        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()
        request = ApprovalRequest(
            request_id=request_id,
            context=context,
            requested_at=datetime.now(timezone.utc),
            timeout_seconds=timeout,
            future=future,
        )
        async with self._lock:
            self.pending[request_id] = request

        try:
            if self.publish_fn is not None:
                await self.publish_fn('approval_requests', json.dumps(request.to_dict(), default=str))
            decision = await asyncio.wait_for(future, timeout=timeout)
            result = ApprovalResult(
                request_id=request_id,
                status=ApprovalStatus.APPROVED if decision else ApprovalStatus.DENIED,
                reason='human_decision',
                decided_at=datetime.now(timezone.utc),
            )
        except asyncio.TimeoutError:
            if self.config.auto_approve_on_timeout:
                result = ApprovalResult(
                    request_id=request_id,
                    status=ApprovalStatus.AUTO_APPROVED,
                    reason='timeout_auto_approved',
                    decided_at=datetime.now(timezone.utc),
                )
            else:
                result = ApprovalResult(
                    request_id=request_id,
                    status=ApprovalStatus.TIMEOUT,
                    reason='timeout_denied',
                    decided_at=datetime.now(timezone.utc),
                )
        except Exception as e:
            result = ApprovalResult(
                request_id=request_id,
                status=ApprovalStatus.DENIED,
                reason=f'error:{e}',
                decided_at=datetime.now(timezone.utc),
            )
        finally:
            async with self._lock:
                self.pending.pop(request_id, None)

        self._audit(context, result)
        return result

    async def submit_decision(self, request_id: str, approved: bool) -> bool:
        async with self._lock:
            request = self.pending.get(request_id)
            if request is None or request.future is None or request.future.done():
                return False
            request.future.set_result(approved)
            return True

    def _audit(self, context: Dict[str, Any], result: ApprovalResult) -> None:
        self.audit.append({
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'context': _to_serializable(context),
            'result': result.to_dict(),
        })

    def get_audit_log(self) -> List[Dict[str, Any]]:
        return list(self.audit)


# ============================================================================
# Temporal Safety: SafetyInvariant with ALWAYS/EVENTUALLY
# ============================================================================
class SafetyInvariant:
    def __init__(self, name: str, predicate: Callable[[Dict[str, Any]], bool],
                 description: str, kind: str = "always") -> None:
        if kind not in ("always", "eventually"):
            raise ValueError(f"Unsupported invariant kind: {kind}")
        self.name = name
        self.predicate = predicate
        self.description = description
        self.kind = kind


class SafetyMonitor:
    def __init__(self) -> None:
        self.invariants: List[SafetyInvariant] = []
        self._eventually_flags: Dict[str, bool] = {}

    def add_invariant(self, name: str, condition_fn: Callable[[Dict[str, Any]], bool],
                      description: str, kind: str = "always") -> None:
        self.invariants.append(SafetyInvariant(name, condition_fn, description, kind))
        if kind == "eventually":
            self._eventually_flags[name] = False

    def check(self, state: Dict[str, Any]) -> List[str]:
        violations: List[str] = []
        for inv in self.invariants:
            try:
                ok = bool(inv.predicate(state))
            except Exception as e:
                violations.append(f"{inv.name}: predicate error ({e})")
                continue
            if inv.kind == "always" and not ok:
                violations.append(f"{inv.name}: {inv.description}")
            elif inv.kind == "eventually" and ok:
                self._eventually_flags[inv.name] = True
        return violations

    def final_check(self) -> List[str]:
        return [
            f"{inv.name}: {inv.description} (never satisfied)"
            for inv in self.invariants
            if inv.kind == "eventually" and not self._eventually_flags.get(inv.name, False)
        ]


# ============================================================================
# Chaos Service with save/restore
# ============================================================================
class ChaosService:
    """Fault injector with rollback for resilience testing."""

    def __init__(self, assessor: "BiodiversityImpactAssessor", config: ChaosConfig) -> None:
        self.assessor = assessor
        self.config = config
        self._saved: Dict[str, Any] = {}
        self._lock = asyncio.Lock()

    async def inject_fault(self, fault_type: Union[str, FaultType], **params: Any) -> Dict[str, Any]:
        if isinstance(fault_type, str):
            try:
                fault_type = FaultType(fault_type)
            except ValueError:
                return {'fault': fault_type, 'error': 'unknown_fault'}

        async with self._lock:
            try:
                if fault_type == FaultType.CARBON_SPIKE:
                    if self.assessor.carbon_manager is not None:
                        self._saved.setdefault(
                            'carbon_intensity', self.assessor.carbon_manager.carbon_intensity,
                        )
                        self.assessor.carbon_manager.carbon_intensity = params.get(
                            'intensity', CARBON_CRITICAL_THRESHOLD + 50,
                        )
                        return {'fault': fault_type.value,
                                'intensity': self.assessor.carbon_manager.carbon_intensity}
                elif fault_type == FaultType.HELIUM_DEPLETION:
                    if self.assessor.helium_tracker is not None:
                        self._saved.setdefault(
                            'helium_budget', self.assessor.helium_tracker.budget_l,
                        )
                        self.assessor.helium_tracker.budget_l = params.get('budget', -10.0)
                        return {'fault': fault_type.value, 'budget': self.assessor.helium_tracker.budget_l}
                elif fault_type == FaultType.ML_PREDICTOR_FAILURE:
                    self._saved.setdefault('ml_predictor', self.assessor.ml_predictor)
                    self.assessor.ml_predictor = None
                    return {'fault': fault_type.value}
                elif fault_type == FaultType.MODEL_CORRUPTION:
                    if self.assessor.ml_predictor is not None:
                        self._saved.setdefault('ml_predictor_trained',
                                               self.assessor.ml_predictor.is_trained)
                        self.assessor.ml_predictor.is_trained = False
                    return {'fault': fault_type.value}
                elif fault_type == FaultType.STORAGE_CORRUPTION:
                    # Non-destructive: just report
                    return {'fault': fault_type.value, 'noop': True}
                elif fault_type == FaultType.EVENT_QUEUE_OVERFLOW:
                    return {'fault': fault_type.value, 'noop': True}
            except Exception as e:
                logger.error(f"Fault injection failed ({fault_type}): {e}")
                return {'fault': fault_type.value, 'error': str(e)}
            return {'fault': fault_type.value, 'not_implemented': True}

    def restore(self) -> None:
        if 'carbon_intensity' in self._saved and self.assessor.carbon_manager is not None:
            self.assessor.carbon_manager.carbon_intensity = self._saved.pop('carbon_intensity')
        if 'helium_budget' in self._saved and self.assessor.helium_tracker is not None:
            self.assessor.helium_tracker.budget_l = self._saved.pop('helium_budget')
        if 'ml_predictor' in self._saved:
            self.assessor.ml_predictor = self._saved.pop('ml_predictor')
        if 'ml_predictor_trained' in self._saved:
            if self.assessor.ml_predictor is not None:
                self.assessor.ml_predictor.is_trained = self._saved.pop('ml_predictor_trained')
            else:
                self._saved.pop('ml_predictor_trained', None)

    async def run_chaos_test(self) -> Dict[str, Any]:
        """Inject a sequence of faults, exercise the system, then restore."""
        report: Dict[str, Any] = {'faults': [], 'results': {}, 'restored': False}
        faults = [
            FaultType.CARBON_SPIKE,
            FaultType.HELIUM_DEPLETION,
            FaultType.ML_PREDICTOR_FAILURE,
        ]
        try:
            for ft in faults[:MAX_CHAOS_FAULTS_PER_RUN]:
                res = await self.inject_fault(ft)
                report['faults'].append(ft.value)
                report['results'][ft.value] = res

            # Exercise the assessor under fault
            try:
                result = await self.assessor.assess_expert_impact(
                    expert_type='energy',
                    location={'latitude': 0.0, 'longitude': 0.0},
                    energy_source='solar',
                    cooling_method='air_cooling',
                    return_mopd=True,
                )
                report['results']['post_fault_assessment'] = {
                    'success': True,
                    'total_impact': result.get('total_impact'),
                    'sustainability_score': result.get('sustainability_score'),
                }
            except Exception as e:
                report['results']['post_fault_assessment'] = {'success': False, 'error': str(e)}
        finally:
            if self.config.auto_restore:
                self.restore()
                report['restored'] = True
        return report


# ============================================================================
# Persistence Manager (typed)
# ============================================================================
class BiodiversityPersistenceManager:
    # Only these keys are treated as ISO timestamps during deserialization
    TIMESTAMP_KEYS: FrozenSet[str] = frozenset({
        'timestamp', 'last_assessment', 'requested_at', 'decided_at',
    })

    def __init__(self, config: PersistenceConfig):
        self.config = config
        self.path = config.path
        self._lock = asyncio.Lock()
        self._version = STATE_SCHEMA_VERSION
        logger.info(f"BiodiversityPersistenceManager initialized (path={self.path})")

    async def save_state(self, state: Dict[str, Any]) -> bool:
        async with self._lock:
            try:
                payload = {
                    'version': self._version,
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'data': _to_serializable(state),
                }
                tmp_path = f"{self.path}.tmp"

                def _write() -> None:
                    with open(tmp_path, 'w') as f:
                        json.dump(payload, f, indent=2)
                    os.replace(tmp_path, self.path)

                await asyncio.to_thread(_write)
                logger.info(f"State saved to {self.path}")
                return True
            except Exception as e:
                logger.error(f"Failed to save state: {e}")
                return False

    async def load_state(self) -> Optional[Dict[str, Any]]:
        async with self._lock:
            if not os.path.exists(self.path):
                logger.warning(f"Persistence file {self.path} not found")
                return None
            try:
                def _read() -> Dict[str, Any]:
                    with open(self.path, 'r') as f:
                        return json.load(f)

                payload = await asyncio.to_thread(_read)
                version = payload.get('version', 0)
                if version != self._version:
                    logger.warning(
                        f"State version mismatch (file={version}, expected={self._version}); "
                        "attempting migration."
                    )
                    payload['data'] = self._migrate(payload.get('data', {}), version)
                return self._deserialize(payload.get('data', {}))
            except Exception as e:
                logger.error(f"Failed to load state: {e}")
                return None

    def _migrate(self, data: Dict[str, Any], from_version: int) -> Dict[str, Any]:
        # No migration needed yet; placeholder for future schemas.
        return data

    def _deserialize(self, obj: Any, key: Optional[str] = None) -> Any:
        if isinstance(obj, dict):
            return {k: self._deserialize(v, key=k) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self._deserialize(v, key=key) for v in obj]
        if isinstance(obj, str) and key in self.TIMESTAMP_KEYS:
            try:
                return datetime.fromisoformat(obj)
            except ValueError:
                return obj
        return obj

    async def delete_state(self) -> bool:
        async with self._lock:
            if os.path.exists(self.path):
                await asyncio.to_thread(os.remove, self.path)
                logger.info(f"Persistence file {self.path} deleted")
                return True
            return False


# ============================================================================
# Biodiversity Storage (bounded, public API)
# ============================================================================
class BiodiversityStorage:
    def __init__(self) -> None:
        self.ecosystems: Dict[str, BiodiversityMetric] = {}
        self.impact_history: Deque[BiodiversityAssessment] = deque(maxlen=MAX_ASSESSMENT_HISTORY)
        self.mitigation_strategies: Dict[str, List[Dict[str, Any]]] = {}
        self.local_score = 0.0
        self.global_score = 0.0
        self.sustainability_score = 0.0
        self.total_carbon_savings_kg = 0.0
        self.total_helium_savings_l = 0.0
        self._lock = asyncio.Lock()

    async def add_assessment(self, assessment: BiodiversityAssessment) -> None:
        async with self._lock:
            self.impact_history.append(assessment)

    async def update_scores(self, local: float, global_: float, sustainability: float) -> None:
        async with self._lock:
            self.local_score = float(local)
            self.global_score = float(global_)
            self.sustainability_score = float(sustainability)

    async def apply_smoothing(self, local: float, global_: float,
                              sustainability: float, alpha: float = 0.1) -> None:
        async with self._lock:
            self.local_score = (1 - alpha) * self.local_score + alpha * local
            self.global_score = (1 - alpha * 0.5) * self.global_score + alpha * 0.5 * global_
            self.sustainability_score = (
                (1 - alpha) * self.sustainability_score + alpha * sustainability
            )

    async def get_ecosystems(self) -> Dict[str, BiodiversityMetric]:
        async with self._lock:
            return dict(self.ecosystems)

    async def set_ecosystem(self, name: str, metric: BiodiversityMetric) -> None:
        async with self._lock:
            self.ecosystems[name] = metric

    async def get_impact_history(self, limit: int = 50) -> List[BiodiversityAssessment]:
        async with self._lock:
            return list(self.impact_history)[-limit:]

    async def get_scores(self) -> Dict[str, float]:
        async with self._lock:
            return {
                'local': self.local_score,
                'global': self.global_score,
                'sustainability': self.sustainability_score,
            }


# ============================================================================
# Biodiversity Analyzer
# ============================================================================
class BiodiversityAnalyzer:
    def __init__(
        self,
        config: BiodiversityConfig,
        storage: BiodiversityStorage,
        carbon_manager: Optional[CarbonIntensityManager] = None,
        helium_tracker: Optional[HeliumImpactTracker] = None,
        predictive: Optional[PredictiveBiodiversityAnalyzer] = None,
        ml_predictor: Optional[MLImpactPredictor] = None,
    ) -> None:
        self.config = config
        self.storage = storage
        self.carbon_manager = carbon_manager
        self.helium_tracker = helium_tracker
        self.predictive = predictive
        self.ml_predictor = ml_predictor
        self._lock = asyncio.Lock()

    async def assess_expert_impact(
        self,
        expert_type: str,
        location: Dict[str, Any],
        energy_source: str,
        cooling_method: str,
        use_ml_prediction: bool = False,
        return_mopd: bool = False,
    ) -> Dict[str, Any]:
        carbon_intensity = DEFAULT_CARBON_INTENSITY
        if self.carbon_manager is not None:
            carbon_intensity = await self.carbon_manager.get_current_intensity()

        habitat = self._assess_habitat(location)
        energy = self._assess_energy(energy_source, location)
        cooling = self._assess_cooling(cooling_method, location)
        resources = self._assess_resources(expert_type)
        carbon = self._assess_carbon(energy_source, location, carbon_intensity)
        helium = self._assess_helium(cooling_method, location)

        breakdown: Dict[str, Any] = {
            'habitat': habitat,
            'energy': energy,
            'cooling': cooling,
            'resources': resources,
            'carbon': carbon,
            'helium': helium,
        }
        total = float(
            (habitat['score'] + energy['score'] + cooling['score'] +
             resources['score'] + carbon['score'] + helium['score']) / 6.0
        )

        ml_pred: Optional[Dict[str, Any]] = None
        if self.ml_predictor is not None and use_ml_prediction:
            ml_pred = await self.ml_predictor.predict({
                'carbon_intensity': carbon_intensity,
                'energy_intensity': energy['score'],
                'cooling_intensity': cooling['score'],
                'resource_intensity': resources['score'],
                'ecosystem_sensitivity': habitat.get('sensitivity', 0.5),
                'proximity_factor': habitat.get('proximity_factor', 0.5),
                'fragmentation_index': habitat.get('fragmentation_index', 0.5),
                'species_density': 0.5,
                'water_scarcity': location.get('water_scarcity_index', 0.5),
                'temperature_anomaly': 0.5,
            })

        mitigation = self._generate_mitigation(breakdown, expert_type, location)
        recommendations = self._generate_recommendations(breakdown)
        sustainability = self._calc_sustainability(breakdown, total, carbon_intensity)

        assessment = BiodiversityAssessment(
            assessment_id=hashlib.md5(
                f"{expert_type}{location}{datetime.now(timezone.utc)}".encode()
            ).hexdigest()[:12],
            expert_type=expert_type,
            location=location,
            total_impact=total,
            impact_breakdown=breakdown,
            mitigation_strategies=mitigation,
            recommendations=recommendations,
            sustainability_score=sustainability,
            carbon_impact=carbon,
            helium_impact=helium,
            ml_prediction=ml_pred,
            timestamp=datetime.now(timezone.utc),
        )

        await self.storage.add_assessment(assessment)
        await self.storage.apply_smoothing(total, 0.5 * total, sustainability)

        if self.predictive is not None:
            self.predictive.update_history({
                'total_impact': total,
                'habitat_score': habitat['score'],
                'energy_score': energy['score'],
                'cooling_score': cooling['score'],
                'resource_score': resources['score'],
                'carbon_intensity': carbon_intensity,
                'ecosystem_sensitivity': habitat.get('sensitivity', 0.5),
            })

        if self.helium_tracker is not None:
            helium_usage_l = helium['score'] * 10
            self.helium_tracker.record_usage(helium_usage_l, expert_type)

        result = assessment.to_dict()
        if self.config.enable_mopd and return_mopd:
            mopd_result = await self._generate_mitigation_pareto_front(
                breakdown, expert_type, location,
            )
            for plan in mopd_result['pareto_front']:
                plan.explanation = self._generate_plan_explanation(plan, breakdown)
            if mopd_result['best_plan'] is not None:
                mopd_result['best_plan'].explanation = self._generate_plan_explanation(
                    mopd_result['best_plan'], breakdown,
                )
            result['mopd_pareto_front'] = [p.to_dict() for p in mopd_result['pareto_front']]
            result['mopd_best_plan'] = (
                mopd_result['best_plan'].to_dict() if mopd_result['best_plan'] else None
            )

            if self.config.enable_temporal_safety and mopd_result['best_plan'] is not None:
                violations = self._check_invariants(mopd_result['best_plan'])
                if violations:
                    logger.warning(f"Temporal safety violations: {violations}")
                    result['temporal_safety_violations'] = violations

        return result

    def _generate_plan_explanation(self, plan: MOPDPlan, breakdown: Dict[str, Any]) -> str:
        parts: List[str] = []
        if plan.strategy_ids:
            parts.append(f"Strategies: {', '.join(plan.strategy_ids)}")
        for key in ('habitat', 'energy', 'cooling', 'carbon', 'helium'):
            plan_key = f"{key}_impact"
            if hasattr(plan, plan_key) and key in breakdown:
                parts.append(
                    f"{key.capitalize()} impact: {getattr(plan, plan_key):.2f} "
                    f"(was {breakdown[key]['score']:.2f})"
                )
        parts.append(f"Total impact: {plan.total_impact:.2f}")
        parts.append(f"Cost: ${plan.cost:.2f}, time: {plan.implementation_time_days} days")
        return " | ".join(parts)

    def _check_invariants(self, plan: MOPDPlan) -> List[str]:
        violations: List[str] = []
        if plan.total_impact > 0.7:
            violations.append("Total impact still high after mitigation")
        if plan.habitat_impact > 0.6:
            violations.append("Habitat impact remains critical")
        if plan.cost > 10.0:
            violations.append("Mitigation cost exceeds budget")
        if plan.helium_impact < 0:
            violations.append("Helium impact cannot be negative")
        return violations

    async def _generate_mitigation_pareto_front(
        self,
        breakdown: Dict[str, Any],
        expert_type: str,
        location: Dict[str, Any],
    ) -> Dict[str, Any]:
        strategies = [
            {
                'id': 'habitat_relocation',
                'effect': {'habitat_impact': -0.6, 'energy_impact': 0.0, 'cooling_impact': 0.0,
                           'resource_impact': 0.0, 'carbon_impact': 0.0, 'helium_impact': 0.0},
                'cost': 5.0, 'implementation_time': 3,
            },
            {
                'id': 'renewable_energy',
                'effect': {'habitat_impact': 0.0, 'energy_impact': -0.7, 'cooling_impact': 0.0,
                           'resource_impact': 0.0, 'carbon_impact': -0.5, 'helium_impact': 0.0},
                'cost': 3.0, 'implementation_time': 5,
            },
            {
                'id': 'efficient_cooling',
                'effect': {'habitat_impact': 0.0, 'energy_impact': -0.2, 'cooling_impact': -0.5,
                           'resource_impact': 0.0, 'carbon_impact': -0.2, 'helium_impact': -0.3},
                'cost': 2.5, 'implementation_time': 4,
            },
            {
                'id': 'circular_economy',
                'effect': {'habitat_impact': 0.0, 'energy_impact': 0.0, 'cooling_impact': 0.0,
                           'resource_impact': -0.4, 'carbon_impact': -0.1, 'helium_impact': 0.0},
                'cost': 1.5, 'implementation_time': 2,
            },
            {
                'id': 'carbon_offset',
                'effect': {'habitat_impact': 0.0, 'energy_impact': 0.0, 'cooling_impact': 0.0,
                           'resource_impact': 0.0, 'carbon_impact': -0.3, 'helium_impact': 0.0},
                'cost': 2.0, 'implementation_time': 1,
            },
            {
                'id': 'helium_recovery',
                'effect': {'habitat_impact': 0.0, 'energy_impact': 0.0, 'cooling_impact': 0.0,
                           'resource_impact': 0.0, 'carbon_impact': 0.0, 'helium_impact': -0.5},
                'cost': 4.0, 'implementation_time': 6,
            },
        ]

        base = {
            'habitat_impact': breakdown['habitat']['score'],
            'energy_impact': breakdown['energy']['score'],
            'cooling_impact': breakdown['cooling']['score'],
            'resource_impact': breakdown['resources']['score'],
            'carbon_impact': breakdown['carbon']['score'],
            'helium_impact': breakdown['helium']['score'],
        }

        objective_names = [
            'habitat_impact', 'energy_impact', 'cooling_impact',
            'resource_impact', 'carbon_impact', 'helium_impact',
        ]
        weights = self.config.mopd.objective_weights

        plans: List[MOPDPlan] = []
        for r in range(0, min(4, len(strategies) + 1)):
            for combo in itertools.combinations(strategies, r):
                plan_vals = dict(base)
                total_cost = 0.0
                total_time = 0
                strategy_ids: List[str] = []
                for strat in combo:
                    for key, effect in strat['effect'].items():
                        plan_vals[key] = max(0.0, plan_vals[key] + effect)
                    total_cost += strat['cost']
                    total_time = max(total_time, strat['implementation_time'])
                    strategy_ids.append(strat['id'])

                # Weighted total_impact using MOPD weights
                plan_total = sum(
                    weights.get(k, 1.0 / len(objective_names)) * plan_vals[k]
                    for k in objective_names
                )
                plan = MOPDPlan(
                    strategy_ids=strategy_ids,
                    habitat_impact=plan_vals['habitat_impact'],
                    energy_impact=plan_vals['energy_impact'],
                    cooling_impact=plan_vals['cooling_impact'],
                    resource_impact=plan_vals['resource_impact'],
                    carbon_impact=plan_vals['carbon_impact'],
                    helium_impact=plan_vals['helium_impact'],
                    total_impact=plan_total,
                    cost=total_cost,
                    implementation_time_days=total_time,
                )
                plans.append(plan)

        pareto: List[MOPDPlan] = []
        for i, p_i in enumerate(plans):
            dominated = False
            for j, p_j in enumerate(plans):
                if i == j:
                    continue
                a_vec = [getattr(p_i, key) for key in objective_names]
                b_vec = [getattr(p_j, key) for key in objective_names]
                if all(b <= a for a, b in zip(a_vec, b_vec)) and any(
                    b < a for a, b in zip(a_vec, b_vec)
                ):
                    dominated = True
                    break
            if not dominated:
                pareto.append(p_i)

        best_plan = self._select_best_from_pareto(pareto)
        return {'pareto_front': pareto, 'best_plan': best_plan}

    def _select_best_from_pareto(self, pareto_front: List[MOPDPlan]) -> Optional[MOPDPlan]:
        if not pareto_front:
            return None
        weights = self.config.mopd.objective_weights
        objective_names = [
            'habitat_impact', 'energy_impact', 'cooling_impact',
            'resource_impact', 'carbon_impact', 'helium_impact',
        ]
        max_vals = {k: max(getattr(p, k) for p in pareto_front) for k in objective_names}
        min_vals = {k: min(getattr(p, k) for p in pareto_front) for k in objective_names}
        ranges = {
            k: (max_vals[k] - min_vals[k]) if max_vals[k] != min_vals[k] else 1.0
            for k in objective_names
        }

        best: Optional[MOPDPlan] = None
        best_score = -float('inf')
        for plan in pareto_front:
            score = 0.0
            for key in objective_names:
                val = getattr(plan, key)
                norm = 1.0 - (val - min_vals[key]) / ranges[key]
                weight = weights.get(key, 1.0 / len(objective_names))
                score += weight * norm
            plan.scalarised_score = score
            if score > best_score:
                best_score = score
                best = plan
        return best

    # ------------------------------------------------------------------
    # Sub-assessments
    # ------------------------------------------------------------------
    def _assess_habitat(self, location: Dict[str, Any]) -> Dict[str, Any]:
        nearest = self._find_nearest_ecosystem(location)
        if not nearest:
            return {'score': 0.1, 'category': 'minimal', 'ecosystem': None}

        distance = float(location.get('distance_to_ecosystem_km', 100.0))
        if distance < 1:
            proximity = 1.0
        elif distance < 10:
            proximity = 0.7
        elif distance < 50:
            proximity = 0.3
        else:
            proximity = 0.1

        ecosystem = self.storage.ecosystems.get(nearest)
        if ecosystem is not None:
            sensitivity = min(ecosystem.endangered_species_count / 200.0, 1.0)
            fragmentation = ecosystem.fragmentation_index
        else:
            sensitivity = 0.5
            fragmentation = 0.5

        score = proximity * 0.4 + sensitivity * 0.4 + fragmentation * 0.2
        return {
            'score': score,
            'category': 'critical' if score > 0.7 else 'moderate' if score > 0.3 else 'low',
            'ecosystem': nearest,
            'proximity_factor': proximity,
            'sensitivity': sensitivity,
            'fragmentation_index': fragmentation,
        }

    def _assess_energy(self, energy_source: str, location: Dict[str, Any]) -> Dict[str, Any]:
        factors = {
            'solar': 0.05, 'wind': 0.08, 'hydroelectric': 0.15,
            'geothermal': 0.03, 'nuclear': 0.10, 'natural_gas': 0.40,
            'coal': 0.80, 'oil': 0.90, 'biomass': 0.30,
            'mixed_grid': 0.35,
        }
        base = factors.get(energy_source, 0.5)
        if location.get('near_water_body') and energy_source in ('hydroelectric', 'nuclear'):
            base *= 1.5
        if location.get('in_migration_corridor') and energy_source == 'wind':
            base *= 1.3
        return {
            'score': base,
            'energy_source': energy_source,
            'category': 'high' if base > 0.5 else 'moderate' if base > 0.2 else 'low',
        }

    def _assess_cooling(self, cooling_method: str, location: Dict[str, Any]) -> Dict[str, Any]:
        factors = {
            'air_cooling': 0.05, 'evaporative_cooling': 0.15,
            'water_cooling': 0.25, 'helium_cooling': 0.10,
            'geothermal_cooling': 0.03, 'liquid_immersion': 0.20,
            'free_cooling': 0.02,
        }
        base = factors.get(cooling_method, 0.15)
        if cooling_method in ('water_cooling', 'evaporative_cooling'):
            scarcity = float(location.get('water_scarcity_index', 0.0))
            if scarcity > 0.7:
                base *= 2.0
            elif scarcity > 0.4:
                base *= 1.5
        if cooling_method in ('water_cooling', 'liquid_immersion') and location.get('near_water_body'):
            base *= 1.3
        return {
            'score': base,
            'cooling_method': cooling_method,
            'category': 'high' if base > 0.5 else 'moderate' if base > 0.2 else 'low',
        }

    def _assess_resources(self, expert_type: str) -> Dict[str, Any]:
        impacts = {
            'energy_expert': 0.08,
            'data_expert': 0.12,
            'iot_expert': 0.04,
            'quantum_expert': 0.25,
            'helium_expert': 0.06,
            'energy': 0.08,
            'data': 0.12,
            'iot': 0.04,
            'quantum': 0.25,
            'helium': 0.06,
        }
        overall = impacts.get(expert_type, 0.1)
        return {
            'score': overall,
            'expert_type': expert_type,
            'category': 'high' if overall > 0.2 else 'moderate' if overall > 0.1 else 'low',
        }

    def _assess_carbon(self, energy_source: str, location: Dict[str, Any],
                       carbon_intensity: float) -> Dict[str, Any]:
        factors = {
            'solar': 0.02, 'wind': 0.03, 'hydroelectric': 0.05,
            'geothermal': 0.01, 'nuclear': 0.04, 'natural_gas': 0.35,
            'coal': 0.70, 'oil': 0.80, 'biomass': 0.25,
            'mixed_grid': 0.30,
        }
        base = factors.get(energy_source, 0.3)
        carbon_factor = carbon_intensity / 400.0
        score = base * carbon_factor
        if location.get('near_carbon_sensitive_ecosystem'):
            score *= 1.3
        score = min(score, 1.0)
        return {
            'score': score,
            'energy_source': energy_source,
            'carbon_intensity': carbon_intensity,
            'category': 'high' if score > 0.5 else 'moderate' if score > 0.2 else 'low',
        }

    def _assess_helium(self, cooling_method: str, location: Dict[str, Any]) -> Dict[str, Any]:
        factors = {
            'helium_cooling': 0.25,
            'water_cooling': 0.05,
            'air_cooling': 0.02,
            'evaporative_cooling': 0.08,
            'geothermal_cooling': 0.01,
            'liquid_immersion': 0.10,
            'free_cooling': 0.01,
        }
        base = factors.get(cooling_method, 0.05)
        if location.get('near_helium_mining_region'):
            base *= 2.0
        base = min(base, 1.0)
        return {
            'score': base,
            'cooling_method': cooling_method,
            'category': 'high' if base > 0.2 else 'moderate' if base > 0.05 else 'low',
        }

    def _find_nearest_ecosystem(self, location: Dict[str, Any]) -> Optional[str]:
        """Choose nearest known ecosystem using haversine distance if coords present."""
        eco_coords: Dict[str, Tuple[float, float]] = {
            'amazon_rainforest': (-3.0, -60.0),
            'coral_reef_pacific': (-18.0, 175.0),
            'european_wetlands': (52.0, 20.0),
        }
        lat = location.get('latitude')
        lon = location.get('longitude')

        if lat is None or lon is None:
            # Hemisphere heuristic fallback
            if (lat or 0) < 0:
                return 'amazon_rainforest'
            if (lat or 0) > 45:
                return 'european_wetlands'
            return 'coral_reef_pacific'

        def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
            R = 6371.0
            phi1, phi2 = math.radians(lat1), math.radians(lat2)
            dphi = math.radians(lat2 - lat1)
            dlam = math.radians(lon2 - lon1)
            a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
            return 2 * R * math.asin(math.sqrt(a))

        nearest: Optional[str] = None
        best_d = float('inf')
        for name, (e_lat, e_lon) in eco_coords.items():
            d = haversine(float(lat), float(lon), e_lat, e_lon)
            if d < best_d:
                best_d = d
                nearest = name
        return nearest

    def _generate_mitigation(self, breakdown: Dict[str, Any], expert_type: str,
                             location: Dict[str, Any]) -> List[Dict[str, Any]]:
        strategies: List[Dict[str, Any]] = []
        if breakdown['habitat']['score'] > 0.5:
            strategies.append({'type': 'habitat_protection',
                               'action': 'Relocate computation to lower-impact area',
                               'impact_reduction': 0.6, 'cost': 'medium', 'implementation_time': 'short'})
            strategies.append({'type': 'habitat_restoration',
                               'action': 'Invest in local habitat restoration project',
                               'impact_reduction': 0.4, 'cost': 'high', 'implementation_time': 'long'})
        if breakdown['energy']['score'] > 0.3:
            strategies.append({'type': 'renewable_energy',
                               'action': 'Switch to renewable energy sources',
                               'impact_reduction': 0.7, 'cost': 'medium', 'implementation_time': 'medium'})
        if breakdown['cooling']['score'] > 0.3:
            strategies.append({'type': 'efficient_cooling',
                               'action': 'Implement free cooling or geothermal cooling',
                               'impact_reduction': 0.5, 'cost': 'medium', 'implementation_time': 'medium'})
        if breakdown['resources']['score'] > 0.15:
            strategies.append({'type': 'circular_economy',
                               'action': 'Use recycled materials and extend hardware life',
                               'impact_reduction': 0.4, 'cost': 'low', 'implementation_time': 'short'})
        if breakdown['carbon']['score'] > 0.3:
            strategies.append({'type': 'carbon_offset',
                               'action': 'Implement carbon offset program',
                               'impact_reduction': 0.3, 'cost': 'medium', 'implementation_time': 'medium'})
        if breakdown['helium']['score'] > 0.1:
            strategies.append({'type': 'helium_recovery',
                               'action': 'Implement helium recovery and recycling',
                               'impact_reduction': 0.5, 'cost': 'high', 'implementation_time': 'long'})
        return strategies

    def _generate_recommendations(self, breakdown: Dict[str, Any]) -> List[str]:
        recommendations: List[str] = []
        scores = {cat: data['score'] for cat, data in breakdown.items() if 'score' in data}
        highest = max(scores.items(), key=lambda x: x[1]) if scores else ('none', 0.0)
        if highest[0] == 'habitat' and highest[1] > 0.5:
            recommendations.append("HIGH PRIORITY: Relocate computation to avoid sensitive ecosystems")
        elif highest[0] == 'energy' and highest[1] > 0.5:
            recommendations.append("HIGH PRIORITY: Switch to renewable energy")
        elif highest[0] == 'cooling' and highest[1] > 0.5:
            recommendations.append("HIGH PRIORITY: Implement water-free cooling")
        elif highest[0] == 'carbon' and highest[1] > 0.5:
            recommendations.append("HIGH PRIORITY: Reduce carbon emissions")
        elif highest[0] == 'helium' and highest[1] > 0.3:
            recommendations.append("HIGH PRIORITY: Implement helium recovery")
        if all(s < 0.2 for s in scores.values()):
            recommendations.append("Current setup has minimal biodiversity impact")
        else:
            recommendations.append(
                "Consider biodiversity offsets equivalent to 110% of calculated impact"
            )
        return recommendations

    def _calc_sustainability(self, breakdown: Dict[str, Any], total: float,
                             carbon_intensity: float) -> float:
        weights = {
            'habitat': 0.25, 'energy': 0.20, 'cooling': 0.15,
            'resources': 0.15, 'carbon': 0.15, 'helium': 0.10,
        }
        score = 1.0
        for cat, data in breakdown.items():
            if cat in weights:
                score -= data['score'] * weights[cat]
        carbon_factor = 1.0 - (carbon_intensity / CARBON_CRITICAL_THRESHOLD)
        score = score * 0.7 + carbon_factor * 0.3
        return max(0.0, min(1.0, score))

    async def get_routing_guidance(self, expert_options: List[str],
                                   location_options: List[Dict[str, Any]]) -> Dict[str, Any]:
        assessments: List[Dict[str, Any]] = []
        for expert in expert_options:
            for loc in location_options:
                loc_sens = float(loc.get('biodiversity_sensitivity', 0.5))
                expert_int = {
                    'energy': 0.3, 'data': 0.4, 'iot': 0.2,
                    'quantum': 0.6, 'helium': 0.35,
                }.get(expert, 0.4)
                impact = loc_sens * expert_int
                assessments.append({
                    'expert': expert,
                    'location': loc.get('name', 'unknown'),
                    'estimated_impact': impact,
                })
        assessments.sort(key=lambda x: x['estimated_impact'])
        best = assessments[0] if assessments else None
        worst = assessments[-1] if assessments else None
        scores = await self.storage.get_scores()
        return {
            'best_option': best,
            'worst_option': worst,
            'all_options': assessments,
            'recommendation': (
                f"Use {best['expert']} at {best['location']}" if best else "No options"
            ),
            'sustainability_score': scores['sustainability'],
            'biodiversity_impact_reduction': (
                (worst['estimated_impact'] - best['estimated_impact']) /
                max(worst['estimated_impact'], 0.001)
                if best and worst else 0.0
            ),
        }


# ============================================================================
# Biodiversity Reporter
# ============================================================================
class BiodiversityReporter:
    def __init__(
        self,
        config: BiodiversityConfig,
        storage: BiodiversityStorage,
        analyzer: BiodiversityAnalyzer,
        telemetry: Optional[ObservabilityHub] = None,
        persistence: Optional[BiodiversityPersistenceManager] = None,
        human_ai: Optional[HumanAICollaborativeBiodiversity] = None,
    ) -> None:
        self.config = config
        self.storage = storage
        self.analyzer = analyzer
        self.telemetry = telemetry
        self.persistence = persistence
        self.human_ai = human_ai
        self._lock = asyncio.Lock()

    async def generate_report(self) -> Dict[str, Any]:
        scores = await self.storage.get_scores()
        ecosystems = await self.storage.get_ecosystems()
        recent = await self.storage.get_impact_history(10)

        report: Dict[str, Any] = {
            'local_biodiversity_score': scores['local'],
            'global_biodiversity_score': scores['global'],
            'sustainability_score': scores['sustainability'],
            'total_carbon_savings_kg': self.storage.total_carbon_savings_kg,
            'total_helium_savings_l': self.storage.total_helium_savings_l,
            'ecosystems_tracked': len(ecosystems),
            'recent_impacts': [
                {
                    'expert_type': a.expert_type,
                    'impact': a.total_impact,
                    'sustainability_score': a.sustainability_score,
                    'timestamp': a.timestamp.isoformat(),
                }
                for a in recent
            ],
            'high_risk_ecosystems': [
                name for name, eco in ecosystems.items()
                if eco.endangered_species_count > 50
            ],
            'mitigation_effectiveness': await self._calc_mitigation_effectiveness(),
            'recommendations': self._generate_global_recommendations(),
        }

        if self.analyzer.predictive is not None:
            forecast = await self.analyzer.predictive.predict_trend()
            report['predictive_forecast'] = forecast
        if self.human_ai is not None:
            report['human_ai_insights'] = self.human_ai.get_insights()
        if self.config.enable_mopd:
            history = await self.storage.get_impact_history(1)
            if history:
                sample = history[0]
                mopd_result = await self.analyzer._generate_mitigation_pareto_front(
                    sample.impact_breakdown, sample.expert_type, sample.location,
                )
                report['mopd_pareto_front_size'] = len(mopd_result['pareto_front'])
                if mopd_result['best_plan'] is not None:
                    report['mopd_best_plan'] = mopd_result['best_plan'].to_dict()
        return report

    async def _calc_mitigation_effectiveness(self) -> float:
        history = await self.storage.get_impact_history(limit=1000)
        if len(history) < 20:
            return 0.5
        recent = history[-20:]
        historical = history[:-20]
        if not historical:
            return 0.5
        recent_avg = float(np.mean([a.total_impact for a in recent]))
        historical_avg = float(np.mean([a.total_impact for a in historical]))
        if historical_avg > 0:
            return max((historical_avg - recent_avg) / historical_avg, 0.0)
        return 0.0

    def _generate_global_recommendations(self) -> List[str]:
        recs: List[str] = []
        if self.storage.local_score > 0.5:
            recs.append("CRITICAL: Implement immediate biodiversity protection measures")
        if any(eco.endangered_species_count > 100 for eco in self.storage.ecosystems.values()):
            recs.append("URGENT: Avoid computing operations near critical habitats")
        if self.storage.sustainability_score < 0.5:
            recs.append("IMPROVE: Overall sustainability score needs improvement")
        recs.append("Implement helium recovery systems to reduce mining impact")
        recs.append("Monitor carbon intensity and optimize energy sources")
        return recs

    async def export_telemetry(self) -> None:
        if self.telemetry is not None:
            data = await self.telemetry.export_prometheus()
            logger.debug(f"Telemetry export: {len(data)} bytes")

    async def save_state(self) -> None:
        if self.persistence is None:
            return
        ecosystems = await self.storage.get_ecosystems()
        history = await self.storage.get_impact_history(limit=MAX_ASSESSMENT_HISTORY)
        scores = await self.storage.get_scores()
        state = {
            'ecosystems': {k: v.to_dict() for k, v in ecosystems.items()},
            'impact_history': [a.to_dict() for a in history],
            'mitigation_strategies': self.storage.mitigation_strategies,
            'local_score': scores['local'],
            'global_score': scores['global'],
            'sustainability_score': scores['sustainability'],
            'total_carbon_savings_kg': self.storage.total_carbon_savings_kg,
            'total_helium_savings_l': self.storage.total_helium_savings_l,
            'ml_checkpoint': (
                self.analyzer.ml_predictor.get_checkpoint()
                if self.analyzer.ml_predictor else None
            ),
        }
        await self.persistence.save_state(state)

    async def load_state(self) -> None:
        if self.persistence is None:
            return
        state = await self.persistence.load_state()
        if not state:
            return

        for name, eco_data in state.get('ecosystems', {}).items():
            try:
                metric = BiodiversityMetric.from_dict(eco_data)
                await self.storage.set_ecosystem(name, metric)
            except Exception as e:
                logger.warning(f"Skipping invalid ecosystem {name}: {e}")

        for a_data in state.get('impact_history', []):
            try:
                assessment = BiodiversityAssessment.from_dict(a_data)
                async with self.storage._lock:
                    self.storage.impact_history.append(assessment)
            except Exception as e:
                logger.warning(f"Skipping invalid assessment: {e}")

        self.storage.mitigation_strategies = state.get('mitigation_strategies', {})
        await self.storage.update_scores(
            local=float(state.get('local_score', 0.0)),
            global_=float(state.get('global_score', 0.0)),
            sustainability=float(state.get('sustainability_score', 0.0)),
        )
        self.storage.total_carbon_savings_kg = float(state.get('total_carbon_savings_kg', 0.0))
        self.storage.total_helium_savings_l = float(state.get('total_helium_savings_l', 0.0))

        ml_cp = state.get('ml_checkpoint')
        if ml_cp and self.analyzer.ml_predictor is not None:
            self.analyzer.ml_predictor.load_checkpoint(ml_cp)


# ============================================================================
# Main Controller
# ============================================================================
class BiodiversityImpactAssessor:
    """
    Enhanced Biodiversity Impact Assessor v5.0.0 – Orchestrates storage, analysis,
    reporting, event handling, MOPD, XAI, temporal safety, human approval,
    chaos testing, and observability.
    """

    def __init__(
        self,
        bio_core: Optional[Any] = None,
        config: Optional[BiodiversityConfig] = None,
        **kwargs: Any,
    ) -> None:
        if config is None:
            valid_fields = {f.name for f in dataclass_fields(BiodiversityConfig)}
            filtered = {k: v for k, v in kwargs.items() if k in valid_fields}
            config = BiodiversityConfig(**filtered)
        self.config = config

        # Bio‑core references
        self.bio_core: Optional[BioCoreProtocol] = bio_core
        self.event_broker: Optional[EventBrokerProtocol] = None
        self.self_healer: Optional[SelfHealerProtocol] = None
        self.workflow_orchestrator: Optional[WorkflowOrchestratorProtocol] = None
        self.swarm_coordinator: Optional[SwarmCoordinatorProtocol] = None
        self.token_manager: Optional[Any] = None
        self.gradient_manager: Optional[Any] = None
        if bio_core is not None:
            self._wire_bio_core(bio_core)

        # Observability
        self.observability = ObservabilityHub()

        # Governance
        self.governance: Optional[GovernanceLayer] = None

        # Safety
        self.safety_monitor = SafetyMonitor() if self.config.enable_temporal_safety else None
        if self.safety_monitor is not None:
            self._setup_safety_invariants()

        # Sub‑modules
        self.carbon_manager: Optional[CarbonIntensityManager] = (
            CarbonIntensityManager(self.config.carbon) if self.config.carbon.enabled else None
        )
        self.helium_tracker: Optional[HeliumImpactTracker] = (
            HeliumImpactTracker(self.config.helium) if self.config.helium.enabled else None
        )
        self.predictive: Optional[PredictiveBiodiversityAnalyzer] = (
            PredictiveBiodiversityAnalyzer(self.config.predictive)
            if self.config.predictive.enabled else None
        )
        self.ml_predictor: Optional[MLImpactPredictor] = (
            MLImpactPredictor(self.config.ml) if self.config.ml.enabled else None
        )
        self.federated: Optional[FederatedBiodiversityAssessor] = (
            FederatedBiodiversityAssessor(self.config.federated)
            if self.config.federated.enabled else None
        )
        self.human_ai: Optional[HumanAICollaborativeBiodiversity] = (
            HumanAICollaborativeBiodiversity() if self.config.enable_human_ai else None
        )
        self.persistence: Optional[BiodiversityPersistenceManager] = (
            BiodiversityPersistenceManager(self.config.persistence)
            if self.config.persistence.enabled else None
        )

        # Storage, Analyzer, Reporter
        self.storage = BiodiversityStorage()
        self.analyzer = BiodiversityAnalyzer(
            self.config, self.storage, self.carbon_manager, self.helium_tracker,
            self.predictive, self.ml_predictor,
        )
        self.reporter = BiodiversityReporter(
            self.config, self.storage, self.analyzer,
            self.observability if self.config.telemetry.enabled else None,
            self.persistence, self.human_ai,
        )

        # Chaos service
        self.chaos_service: Optional[ChaosService] = (
            ChaosService(self, self.config.chaos) if self.config.enable_chaos_testing else None
        )

        # MoE injectables
        self.expert_router: Optional[ExpertRouterProtocol] = None
        self.gating_network: Optional[GatingNetworkProtocol] = None
        self.self_evolving_gate: Optional[Any] = None
        self.helium_provider: Optional[HeliumProviderProtocol] = None

        # Async resources created in start()
        self._event_queue: Optional[asyncio.Queue] = None
        self._event_consumer_task: Optional[asyncio.Task] = None
        self._background_tasks: List[asyncio.Task] = []
        self._started = False
        self._shutting_down = False

        # Health
        self.health_status = "healthy"
        self.last_error: Optional[str] = None

        # Idempotency
        self._idempotency_seen: Set[str] = set()
        self._idempotency_order: Deque[str] = deque(maxlen=IDEMPOTENCY_CACHE_SIZE)

        # Initialize ecosystems (default)
        self._initialize_ecosystems()

        logger.info("BiodiversityImpactAssessor v5.0.0 initialized (not started)")

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def start(self) -> None:
        if self._started:
            return
        self._started = True

        # Deferred async resources
        self._event_queue = asyncio.Queue(maxsize=10_000)

        # Governance
        if self.config.approval.enabled:
            self.governance = GovernanceLayer(self.config.approval, publish_fn=self._publish_message)

        # Start sub-module loops
        if self.helium_tracker is not None:
            self.helium_tracker.start()

        # Subscribe to events
        if self.config.enable_event_driven and self.event_broker is not None:
            self._subscribe_events()

        # Start background tasks
        self._start_background_tasks()

        # Load state
        if self.config.persistence.enabled:
            await self._load_state()

        logger.info("BiodiversityImpactAssessor started")

    async def wait_ready(self) -> None:
        if not self._started:
            await self.start()

    # ------------------------------------------------------------------
    # Bio-core wiring
    # ------------------------------------------------------------------
    def _wire_bio_core(self, bio_core: Any) -> None:
        self.event_broker = getattr(bio_core, 'event_broker', None)
        self.self_healer = getattr(bio_core, 'self_healer', None)
        self.workflow_orchestrator = getattr(bio_core, 'workflow_orchestrator', None)
        self.swarm_coordinator = getattr(bio_core, 'swarm_coordinator', None)
        self.token_manager = getattr(bio_core, 'token_manager', None)
        self.gradient_manager = getattr(bio_core, 'gradient_manager', None)

    def inject_bio_core(self, bio_core: Any) -> None:
        self.bio_core = bio_core
        if bio_core is not None:
            self._wire_bio_core(bio_core)

    # ------------------------------------------------------------------
    # Safety invariants
    # ------------------------------------------------------------------
    def _setup_safety_invariants(self) -> None:
        if self.safety_monitor is None:
            return

        def _impact_bounded(state: Dict[str, Any]) -> bool:
            v = state.get('total_impact', 0.0)
            return 0.0 <= v <= 1.0

        def _sustainability_bounded(state: Dict[str, Any]) -> bool:
            v = state.get('sustainability_score', 0.5)
            return 0.0 <= v <= 1.0

        def _helium_budget_nonneg(state: Dict[str, Any]) -> bool:
            return state.get('helium_remaining_budget', 0.0) >= 0.0

        def _cost_bounded(state: Dict[str, Any]) -> bool:
            return state.get('plan_cost', 0.0) <= 10.0

        self.safety_monitor.add_invariant(
            "impact_bounded", _impact_bounded,
            "Total impact must be in [0, 1]",
        )
        self.safety_monitor.add_invariant(
            "sustainability_bounded", _sustainability_bounded,
            "Sustainability score must be in [0, 1]",
        )
        self.safety_monitor.add_invariant(
            "helium_budget_nonneg", _helium_budget_nonneg,
            "Helium budget must remain non-negative",
        )
        self.safety_monitor.add_invariant(
            "cost_bounded", _cost_bounded,
            "Plan cost must not exceed budget",
        )

    # ------------------------------------------------------------------
    # Event handling
    # ------------------------------------------------------------------
    def _subscribe_events(self) -> None:
        if self.event_broker is None:
            return
        try:
            for ev in (
                'carbon_update', 'helium_update', 'alert_generated', 'config_updated',
                'token_balance_update', 'health_update', 'anomaly_detected',
            ):
                self.event_broker.subscribe(ev, self._enqueue_event)
            logger.info("Subscribed to core events via queue")
        except Exception as e:
            logger.warning(f"Event subscription failed: {e}")

    async def _enqueue_event(self, event: Any) -> None:
        if self._event_queue is None:
            return
        try:
            self._event_queue.put_nowait(event)
        except asyncio.QueueFull:
            logger.warning("Event queue full; dropping event")
            self.observability.counter('event_queue_drops')

    async def _event_consumer(self) -> None:
        assert self._event_queue is not None
        while not self._shutting_down:
            try:
                event = await asyncio.wait_for(self._event_queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            try:
                await self._handle_event(event)
            except Exception as e:
                logger.error(f"Event handler error: {e}")
            finally:
                self._event_queue.task_done()

    async def _handle_event(self, event: Any) -> None:
        event_type = getattr(event, 'event_type', None)
        if not event_type:
            return
        handler = getattr(self, f"_on_{event_type}", None)
        if handler:
            await handler(event)

    async def _on_carbon_update(self, event: Any) -> None:
        intensity = float(event.data.get('intensity', DEFAULT_CARBON_INTENSITY))
        if self.carbon_manager is not None:
            self.carbon_manager.carbon_intensity = intensity
            self.carbon_manager.last_update = datetime.now(timezone.utc)
        for eco in self.storage.ecosystems.values():
            eco.carbon_sensitivity = 0.5 + 0.5 * (intensity / CARBON_CRITICAL_THRESHOLD)

    async def _on_helium_update(self, event: Any) -> None:
        scarcity = float(event.data.get('scarcity', 0.5))
        if self.helium_tracker is not None:
            # Respect configured budget as the maximum
            configured = self.config.helium.budget_l
            self.helium_tracker.budget_l = configured * (1.0 - scarcity * 0.3)
        for eco in self.storage.ecosystems.values():
            eco.helium_sensitivity = 0.5 + 0.5 * scarcity

    async def _on_alert_generated(self, event: Any) -> None:
        if event.data.get('severity') == 'critical':
            logger.warning("Critical alert; triggering self‑healing")
            if self.config.self_healing.enabled and self.self_healer is not None:
                try:
                    await self.self_healer.apply_healing('damage_accumulation')
                except Exception as e:
                    logger.error(f"Self-healing failed: {e}")
            if self.workflow_orchestrator is not None and self.config.workflow_on_critical_impact:
                try:
                    await self.workflow_orchestrator.execute_workflow(
                        self.config.workflow_on_critical_impact,
                    )
                except Exception as e:
                    logger.error(f"Workflow failed: {e}")

    async def _on_config_updated(self, event: Any) -> None:
        updates = event.data.get('updates', {})
        if 'biodiversity_assessor' in updates:
            new = updates['biodiversity_assessor']
            for key, value in new.items():
                if hasattr(self.config, key):
                    setattr(self.config, key, value)
            logger.info("Configuration reloaded")

    async def _on_token_balance_update(self, event: Any) -> None:
        pass

    async def _on_health_update(self, event: Any) -> None:
        self.health_status = event.data.get('status', 'healthy')

    async def _on_anomaly_detected(self, event: Any) -> None:
        metric = event.data.get('metric')
        if metric == 'carbon_intensity':
            for eco in self.storage.ecosystems.values():
                eco.carbon_sensitivity = min(1.0, eco.carbon_sensitivity * 1.2)
        elif metric == 'helium_scarcity':
            for eco in self.storage.ecosystems.values():
                eco.helium_sensitivity = min(1.0, eco.helium_sensitivity * 1.2)

    # ------------------------------------------------------------------
    # Background tasks
    # ------------------------------------------------------------------
    def _start_background_tasks(self) -> None:
        if self.config.enable_event_driven and self._event_queue is not None:
            self._event_consumer_task = asyncio.create_task(self._event_consumer())
            self._background_tasks.append(self._event_consumer_task)

        if self.carbon_manager is not None:
            self._background_tasks.append(asyncio.create_task(self._carbon_update_loop()))
        if self.predictive is not None:
            self._background_tasks.append(asyncio.create_task(self._predictive_update_loop()))
        if self.ml_predictor is not None:
            self._background_tasks.append(asyncio.create_task(self._ml_training_loop()))
        if self.federated is not None:
            self._background_tasks.append(asyncio.create_task(self._federated_sync_loop()))
        if self.config.telemetry.enabled:
            self._background_tasks.append(asyncio.create_task(self._telemetry_export_loop()))
        if self.persistence is not None:
            self._background_tasks.append(asyncio.create_task(self._persistence_save_loop()))
        if self.config.enable_swarm_coordination and self.swarm_coordinator is not None:
            self._background_tasks.append(asyncio.create_task(self._swarm_update_loop()))
        if self.config.enable_chaos_testing and self.chaos_service is not None:
            self._background_tasks.append(asyncio.create_task(self._chaos_testing_loop()))

    async def _carbon_update_loop(self) -> None:
        while not self._shutting_down:
            try:
                if self.carbon_manager is not None:
                    await self.carbon_manager.update_carbon_intensity()
                    intensity = await self.carbon_manager.get_current_intensity()
                    self.observability.gauge('carbon_intensity', intensity)
                await asyncio.sleep(self.config.carbon.update_interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Carbon update error: {e}")
                await asyncio.sleep(60)

    async def _predictive_update_loop(self) -> None:
        while not self._shutting_down:
            try:
                if self.predictive is not None and len(self.storage.impact_history) > 0:
                    await self.predictive.train()
                await asyncio.sleep(self.config.predictive.update_interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Predictive update error: {e}")
                await asyncio.sleep(60)

    async def _ml_training_loop(self) -> None:
        while not self._shutting_down:
            try:
                if self.ml_predictor is not None:
                    history = await self.storage.get_impact_history(200)
                    if len(history) >= 20:
                        training_data = [
                            {
                                'carbon_intensity': a.carbon_impact.get(
                                    'carbon_intensity', DEFAULT_CARBON_INTENSITY,
                                ),
                                'energy_intensity': a.impact_breakdown.get('energy', {}).get('score', 0.5),
                                'cooling_intensity': a.impact_breakdown.get('cooling', {}).get('score', 0.5),
                                'resource_intensity': a.impact_breakdown.get('resources', {}).get('score', 0.5),
                                'ecosystem_sensitivity': a.impact_breakdown.get('habitat', {}).get('sensitivity', 0.5),
                                'proximity_factor': a.impact_breakdown.get('habitat', {}).get('proximity_factor', 0.5),
                                'fragmentation_index': a.impact_breakdown.get('habitat', {}).get('fragmentation_index', 0.5),
                                'species_density': 0.5,
                                'water_scarcity': a.location.get('water_scarcity_index', 0.5),
                                'temperature_anomaly': 0.5,
                                'total_impact': a.total_impact,
                            }
                            for a in history
                        ]
                        await self.ml_predictor.train(training_data)
                await asyncio.sleep(self.config.ml.train_interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"ML training error: {e}")
                await asyncio.sleep(60)

    async def _federated_sync_loop(self) -> None:
        while not self._shutting_down:
            try:
                if self.federated is not None:
                    scores = await self.storage.get_scores()
                    pid = f"biodiversity_{hashlib.md5(str(list(self.storage.ecosystems.keys())).encode()).hexdigest()[:8]}"
                    last_impact = (
                        self.storage.impact_history[-1].total_impact
                        if self.storage.impact_history else 0.5
                    )
                    await self.federated.send_local_impact(
                        pid,
                        {
                            'local_score': scores['local'],
                            'global_score': scores['global'],
                            'total_impact': last_impact,
                            'timestamp': datetime.now(timezone.utc).isoformat(),
                        },
                        performance=scores['sustainability'],
                    )
                    await self.federated.get_global_impacts()
                await asyncio.sleep(self.config.federated.sync_interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Federated sync error: {e}")
                await asyncio.sleep(300)

    async def _telemetry_export_loop(self) -> None:
        while not self._shutting_down:
            try:
                await self.reporter.export_telemetry()
                await asyncio.sleep(self.config.telemetry.export_interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Telemetry export error: {e}")
                await asyncio.sleep(60)

    async def _persistence_save_loop(self) -> None:
        while not self._shutting_down:
            try:
                await self.reporter.save_state()
                await asyncio.sleep(self.config.persistence.save_interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Persistence save error: {e}")
                await asyncio.sleep(60)

    async def _swarm_update_loop(self) -> None:
        while not self._shutting_down:
            try:
                await self.share_with_swarm()
                await asyncio.sleep(self.config.swarm_share_interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Swarm update error: {e}")
                await asyncio.sleep(120)

    async def _chaos_testing_loop(self) -> None:
        while not self._shutting_down:
            try:
                await asyncio.sleep(self.config.chaos_test_interval_seconds)
                await self.run_chaos_test()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Chaos testing loop error: {e}")
                await asyncio.sleep(60)

    # ------------------------------------------------------------------
    # Idempotency
    # ------------------------------------------------------------------
    def _is_duplicate(self, key: Optional[str]) -> bool:
        if not key or not self.config.enable_idempotency:
            return False
        if key in self._idempotency_seen:
            return True
        self._idempotency_seen.add(key)
        self._idempotency_order.append(key)
        if len(self._idempotency_seen) > self._idempotency_order.maxlen:
            oldest = self._idempotency_order[0]
            self._idempotency_seen.discard(oldest)
        return False

    # ------------------------------------------------------------------
    # Public API: assess_expert_impact
    # ------------------------------------------------------------------
    async def assess_expert_impact(
        self,
        expert_type: str,
        location: Dict[str, Any],
        energy_source: str,
        cooling_method: str,
        use_ml_prediction: bool = False,
        return_mopd: bool = False,
        idempotency_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        await self.wait_ready()

        if self._is_duplicate(idempotency_key):
            return {'success': True, 'duplicate': True, 'idempotency_key': idempotency_key}

        try:
            result = await self.analyzer.assess_expert_impact(
                expert_type, location, energy_source, cooling_method,
                use_ml_prediction, return_mopd,
            )

            if result['total_impact'] > 0.8 and self.workflow_orchestrator is not None:
                try:
                    await self.workflow_orchestrator.execute_workflow(
                        self.config.workflow_on_critical_impact,
                    )
                except Exception as e:
                    logger.error(f"Critical workflow failed: {e}")

            # Feed to gating network / self-evolving gate
            if self.gating_network is not None and self.expert_router is not None:
                try:
                    features = np.array([
                        result['total_impact'],
                        result['carbon_impact'].get('carbon_intensity', DEFAULT_CARBON_INTENSITY) / CARBON_CRITICAL_THRESHOLD,
                        result['sustainability_score'],
                        len(result['mitigation_strategies']),
                    ])
                    reward = 1.0 - result['total_impact']
                    self.gating_network.update(features, reward, {'expert_type': expert_type})
                except Exception as e:
                    logger.warning(f"Gating network update failed: {e}")

            if self.self_evolving_gate is not None and TORCH_AVAILABLE:
                try:
                    state = torch.tensor(
                        [result['total_impact'], result['sustainability_score']],
                        dtype=torch.float32,
                    )
                    self.self_evolving_gate.adapt(
                        state=state,
                        chosen_expert=0,
                        reward=1.0 - result['total_impact'],
                        environmental_feedback={'expert_type': expert_type},
                        quantum_mode=False,
                    )
                except Exception as e:
                    logger.warning(f"Self-evolving gate adapt failed: {e}")

            # Telemetry
            self.observability.counter('assessments_performed', expert_type=expert_type)
            self.observability.gauge('total_impact', result['total_impact'])
            self.observability.gauge('sustainability_score', result['sustainability_score'])
            if return_mopd and 'mopd_pareto_front' in result:
                self.observability.counter('mopd_generations')
                self.observability.histogram(
                    'mopd_pareto_front_size', len(result['mopd_pareto_front']),
                )

            # Temporal safety check
            if self.safety_monitor is not None:
                min_helium_budget = (
                    self.helium_tracker.get_position()['remaining_budget_l']
                    if self.helium_tracker is not None else 0.0
                )
                safety_state = {
                    'total_impact': result['total_impact'],
                    'sustainability_score': result['sustainability_score'],
                    'helium_remaining_budget': min_helium_budget,
                    'plan_cost': (
                        result.get('mopd_best_plan', {}).get('cost', 0.0)
                        if result.get('mopd_best_plan') else 0.0
                    ),
                }
                violations = self.safety_monitor.check(safety_state)
                if violations:
                    result['temporal_safety_violations'] = violations
                    self.observability.counter('safety_violations')

            # Human approval for high-impact plans
            if (self.governance is not None and
                    result.get('mopd_best_plan') is not None and
                    result['total_impact'] > self.config.approval.require_for_total_impact_above):
                approval = await self.governance.request_approval({
                    'action': 'approve_mitigation_plan',
                    'expert_type': expert_type,
                    'total_impact': result['total_impact'],
                    'plan': result['mopd_best_plan'],
                })
                result['human_approval'] = approval.to_dict()
                if approval.status not in (ApprovalStatus.APPROVED, ApprovalStatus.AUTO_APPROVED):
                    logger.info(f"Mitigation not approved: {approval.status.value}")

            logger.info(
                f"Assessment for {expert_type}: impact={result['total_impact']:.2f}, "
                f"sustainability={result['sustainability_score']:.2f}"
            )
            return result
        except Exception as e:
            logger.error(f"assess_expert_impact failed: {e}", exc_info=True)
            self.last_error = str(e)
            self.health_status = "degraded"
            return {'success': False, 'error': str(e)}

    # ------------------------------------------------------------------
    # Public API: reports, routing, training
    # ------------------------------------------------------------------
    async def get_biodiversity_report(self) -> Dict[str, Any]:
        await self.wait_ready()
        return await self.reporter.generate_report()

    async def get_routing_guidance(self, expert_options: List[str],
                                   location_options: List[Dict[str, Any]]) -> Dict[str, Any]:
        await self.wait_ready()
        return await self.analyzer.get_routing_guidance(expert_options, location_options)

    async def train_ml_model(self, training_data: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
        await self.wait_ready()
        if self.ml_predictor is None:
            return {'status': 'disabled'}
        if training_data is None:
            history = await self.storage.get_impact_history(200)
            training_data = [
                {
                    'carbon_intensity': a.carbon_impact.get('carbon_intensity', DEFAULT_CARBON_INTENSITY),
                    'energy_intensity': a.impact_breakdown.get('energy', {}).get('score', 0.5),
                    'cooling_intensity': a.impact_breakdown.get('cooling', {}).get('score', 0.5),
                    'resource_intensity': a.impact_breakdown.get('resources', {}).get('score', 0.5),
                    'ecosystem_sensitivity': a.impact_breakdown.get('habitat', {}).get('sensitivity', 0.5),
                    'proximity_factor': a.impact_breakdown.get('habitat', {}).get('proximity_factor', 0.5),
                    'fragmentation_index': a.impact_breakdown.get('habitat', {}).get('fragmentation_index', 0.5),
                    'species_density': 0.5,
                    'water_scarcity': a.location.get('water_scarcity_index', 0.5),
                    'temperature_anomaly': 0.5,
                    'total_impact': a.total_impact,
                }
                for a in history
            ]
        return await self.ml_predictor.train(training_data)

    async def train_predictive_model(self) -> Dict[str, Any]:
        await self.wait_ready()
        if self.predictive is None:
            return {'status': 'disabled'}
        return await self.predictive.train()

    # ------------------------------------------------------------------
    # Public API: MOPD
    # ------------------------------------------------------------------
    async def get_mitigation_pareto_front(
        self,
        expert_type: str,
        location: Dict[str, Any],
        energy_source: str,
        cooling_method: str,
        use_ml_prediction: bool = False,
    ) -> List[MOPDPlan]:
        if not self.config.enable_mopd:
            return []
        assessment = await self.analyzer.assess_expert_impact(
            expert_type, location, energy_source, cooling_method,
            use_ml_prediction, return_mopd=True,
        )
        if 'mopd_pareto_front' in assessment:
            return [MOPDPlan.from_dict(p) for p in assessment['mopd_pareto_front']]
        return []

    async def get_mopd_summary(self) -> Dict[str, Any]:
        if not self.config.enable_mopd:
            return {'enabled': False}
        return {
            'enabled': True,
            'objective_weights': dict(self.config.mopd.objective_weights),
            'grid_resolution': self.config.mopd.grid_resolution,
            'strategies_available': 6,
        }

    # ------------------------------------------------------------------
    # Public API: swarm / governance / chaos
    # ------------------------------------------------------------------
    async def share_with_swarm(self) -> None:
        if not self.config.enable_swarm_coordination or self.swarm_coordinator is None:
            return
        scores = await self.storage.get_scores()
        payload = {
            'assessor_id': hashlib.md5(str(list(self.storage.ecosystems.keys())).encode()).hexdigest()[:8],
            'local_biodiversity_score': scores['local'],
            'global_biodiversity_score': scores['global'],
            'sustainability_score': scores['sustainability'],
            'total_carbon_savings_kg': self.storage.total_carbon_savings_kg,
            'total_helium_savings_l': self.storage.total_helium_savings_l,
            'ecosystems_tracked': len(self.storage.ecosystems),
            'mopd_enabled': self.config.enable_mopd,
        }
        try:
            await self.swarm_coordinator.share_predictions(payload)
        except Exception as e:
            logger.warning(f"Swarm share failed: {e}")

    async def submit_approval_decision(self, request_id: str, approved: bool) -> bool:
        if self.governance is None:
            return False
        return await self.governance.submit_decision(request_id, approved)

    async def inject_fault(self, fault_type: Union[str, FaultType], **params: Any) -> Dict[str, Any]:
        if self.chaos_service is None:
            logger.info("Chaos testing disabled; ignoring fault injection.")
            return {'status': 'disabled'}
        return await self.chaos_service.inject_fault(fault_type, **params)

    async def run_chaos_test(self) -> Dict[str, Any]:
        if self.chaos_service is None:
            return {'status': 'disabled'}
        return await self.chaos_service.run_chaos_test()

    async def _publish_message(self, topic: str, payload: str) -> None:
        if self.event_broker is not None and hasattr(self.event_broker, 'publish'):
            try:
                res = self.event_broker.publish(topic, payload)
                if asyncio.iscoroutine(res):
                    await res
            except Exception as e:
                logger.warning(f"Event publish failed: {e}")

    # ------------------------------------------------------------------
    # Public API: gating, router, helium, self-healing, health
    # ------------------------------------------------------------------
    def set_gating_network(self, gating_network: Any) -> None:
        self.gating_network = gating_network

    def set_self_evolving_gate(self, gate: Any) -> None:
        self.self_evolving_gate = gate

    def set_expert_router(self, router: Any) -> None:
        self.expert_router = router

    def set_helium_provider(self, provider: HeliumProviderProtocol) -> None:
        self.helium_provider = provider

    async def self_heal(self) -> None:
        logger.info("Self‑healing started")
        if not self.config.self_healing.enabled:
            logger.warning("Self‑healing disabled")
            return

        self._initialize_ecosystems()
        await self.storage.update_scores(local=0.0, global_=0.0, sustainability=0.0)
        async with self.storage._lock:
            self.storage.impact_history.clear()
            self.storage.mitigation_strategies.clear()
            self.storage.total_carbon_savings_kg = 0.0
            self.storage.total_helium_savings_l = 0.0

        self.health_status = "healthy"
        self.last_error = None
        await self.reporter.save_state()
        logger.info("Self‑healing completed")

    async def get_health_status(self) -> Dict[str, Any]:
        scores = await self.storage.get_scores()
        helium_position = (
            self.helium_tracker.get_position() if self.helium_tracker is not None else {}
        )
        return {
            'status': self.health_status,
            'last_error': self.last_error,
            'local_biodiversity_score': scores['local'],
            'global_biodiversity_score': scores['global'],
            'sustainability_score': scores['sustainability'],
            'ecosystems_tracked': len(self.storage.ecosystems),
            'bio_integration_active': self.config.enable_bio_integration,
            'event_driven_active': self.config.enable_event_driven,
            'self_healing_enabled': self.config.self_healing.enabled,
            'persistence_enabled': self.config.persistence.enabled,
            'mopd_enabled': self.config.enable_mopd,
            'human_approval_enabled': self.config.approval.enabled,
            'temporal_safety_enabled': self.config.enable_temporal_safety,
            'chaos_testing_enabled': self.config.enable_chaos_testing,
            'helium_position': helium_position,
        }

    async def get_prometheus_metrics(self) -> str:
        return await self.observability.export_prometheus()

    # ------------------------------------------------------------------
    # Ecosystem initialization & persistence
    # ------------------------------------------------------------------
    def _initialize_ecosystems(self) -> None:
        defaults = {
            'amazon_rainforest': BiodiversityMetric(
                ecosystem_type=EcosystemType.TROPICAL_FOREST,
                species_richness=10000,
                endangered_species_count=200,
                habitat_area_km2=5_500_000.0,
                fragmentation_index=0.3,
                ecological_connectivity=0.7,
                last_assessment=datetime.now(timezone.utc),
            ),
            'coral_reef_pacific': BiodiversityMetric(
                ecosystem_type=EcosystemType.MARINE,
                species_richness=5000,
                endangered_species_count=150,
                habitat_area_km2=200_000.0,
                fragmentation_index=0.4,
                ecological_connectivity=0.5,
                last_assessment=datetime.now(timezone.utc),
            ),
            'european_wetlands': BiodiversityMetric(
                ecosystem_type=EcosystemType.WETLAND,
                species_richness=2000,
                endangered_species_count=80,
                habitat_area_km2=15_000.0,
                fragmentation_index=0.6,
                ecological_connectivity=0.4,
                last_assessment=datetime.now(timezone.utc),
            ),
        }
        self.storage.ecosystems = defaults

    async def _load_state(self) -> None:
        await self.reporter.load_state()

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------
    async def shutdown(self) -> None:
        logger.info("Shutting down Biodiversity Impact Assessor")
        self._shutting_down = True

        if self._event_consumer_task is not None:
            self._event_consumer_task.cancel()

        for task in self._background_tasks:
            task.cancel()
        await asyncio.gather(*self._background_tasks, return_exceptions=True)
        self._background_tasks.clear()

        if self.helium_tracker is not None:
            await self.helium_tracker.stop()

        if self.persistence is not None:
            try:
                await self.reporter.save_state()
            except Exception as e:
                logger.error(f"Final save failed: {e}")

        if self.carbon_manager is not None:
            await self.carbon_manager.close()
        if self.federated is not None:
            await self.federated.close()

        if self.safety_monitor is not None:
            unmet = self.safety_monitor.final_check()
            if unmet:
                logger.warning(f"Unmet eventual invariants at shutdown: {unmet}")

        logger.info("Shutdown complete")


# ============================================================================
# Example usage
# ============================================================================
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    async def main() -> None:
        config = BiodiversityConfig()
        config.enable_chaos_testing = True  # for demo
        config.chaos.enabled = True
        config.chaos.interval_seconds = 9999  # don't auto-run during demo

        assessor = BiodiversityImpactAssessor(config=config)
        await assessor.start()

        result = await assessor.assess_expert_impact(
            expert_type='energy',
            location={'latitude': -3.0, 'longitude': -60.0},
            energy_source='solar',
            cooling_method='air_cooling',
            return_mopd=True,
            idempotency_key=uuid.uuid4().hex,
        )
        print(json.dumps(result, indent=2, default=str))

        report = await assessor.get_biodiversity_report()
        print(json.dumps(report, indent=2, default=str))

        health = await assessor.get_health_status()
        print(json.dumps(health, indent=2, default=str))

        # Chaos test
        chaos_report = await assessor.run_chaos_test()
        print(json.dumps(chaos_report, indent=2, default=str))

        # Prometheus metrics
        metrics = await assessor.get_prometheus_metrics()
        print(metrics[:500])

        await assessor.shutdown()

    asyncio.run(main())


# ============================================================================
# Module exports
# ============================================================================
__all__ = [
    "BiodiversityImpactAssessor",
    "BiodiversityConfig",
    "BiodiversityStorage",
    "BiodiversityAnalyzer",
    "BiodiversityReporter",
    "BiodiversityMetric",
    "BiodiversityAssessment",
    "MOPDPlan",
    "MOPDConfig",
    "CarbonIntensityManager",
    "CarbonConfig",
    "HeliumImpactTracker",
    "HeliumConfig",
    "PredictiveBiodiversityAnalyzer",
    "PredictiveConfig",
    "MLImpactPredictor",
    "MLConfig",
    "FederatedBiodiversityAssessor",
    "FederatedConfig",
    "HumanAICollaborativeBiodiversity",
    "BiodiversityPersistenceManager",
    "PersistenceConfig",
    "ObservabilityHub",
    "SafetyMonitor",
    "SafetyInvariant",
    "GovernanceLayer",
    "ApprovalConfig",
    "ApprovalRequest",
    "ApprovalResult",
    "ApprovalStatus",
    "ChaosService",
    "ChaosConfig",
    "FaultType",
    "EcosystemType",
    "ImpactCategory",
    "TelemetryConfig",
    "SelfHealingConfig",
]
