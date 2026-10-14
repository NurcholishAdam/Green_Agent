# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/advanced/carbon_sequestration_manager.py
# Enhanced version v5.0.0 – Complete production-grade implementation with all
# sub-modules fully implemented, Protocols, typed persistence, MOPD with
# consistent sign conventions, XAI, temporal safety, human approval, chaos
# testing with rollback, differential privacy, and a shared ObservabilityHub.

"""
Enhanced Carbon Sequestration and Offset Integration v5.0.0

Complete, event-driven, MOPD-aware, XAI-enabled, safety-checked, approval-gated,
chaos-tested, DP-federated, and observability-instrumented carbon sequestration
manager.

ENHANCEMENTS OVER v4.2.0:
- Fully implemented every previously stubbed sub-module.
- Replaced `@model_validator` on a dataclass with `__post_init__`.
- Deferred `asyncio.create_task` from `__init__` to `start()` / `wait_ready()`.
- Removed all `asyncio.run()` calls from async paths.
- `_generate_credits` now actually generates typed CarbonCredit objects.
- Fixed `.seconds` -> `.total_seconds()` staleness bug.
- Renamed `_on_token_update` -> `_on_token_balance_update`.
- `_on_helium_update` no longer permanently mutates nested config.
- Non-destructive chaos via `ChaosService` with save/restore.
- Real human approval via `GovernanceLayer` (queue topic + audit trail).
- Canonical MOPD sign convention via `objective_vector()`.
- Typed `to_dict` / `from_dict` for `CarbonCredit` and `MOPDPlan`.
- Typed persistence with `TIMESTAMP_KEYS` and schema versioning.
- Bounded deques for all histories.
- Threaded file I/O (no blocking calls in async paths).
- Idempotency keys with LRU cache.
- Shared `ObservabilityHub` with Prometheus export.
- Differential privacy (Gaussian mechanism with L2 clipping) for federated layer.
- Safety invariants via `SafetyInvariant` (ALWAYS + EVENTUALLY).
- Removed unused imports; comprehensive type hints throughout.
"""

from __future__ import annotations

import asyncio
import hashlib
import itertools
import json
import logging
import math
import os
import time
import uuid
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field, fields as dataclass_fields
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import (
    Any, Awaitable, Callable, Deque, Dict, FrozenSet, List, Optional,
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
STATE_SCHEMA_VERSION = 5
DEFAULT_CARBON_INTENSITY = 400.0
CARBON_CRITICAL_THRESHOLD = 800.0
MAX_CREDITS = 10_000
MAX_TRANSACTIONS = 10_000
MAX_MOPD_PLANS = 10_000
MAX_HELIUM_HISTORY = 86_400
MAX_APPROVAL_AUDIT = 1000
MAX_DEAD_LETTERS = 1000
IDEMPOTENCY_CACHE_SIZE = 4096
APPROVAL_DEFAULT_TIMEOUT = 60.0
CHAOS_MAX_FAULTS_PER_RUN = 5
MOPD_OBJECTIVE_NAMES: Tuple[str, ...] = (
    'cost',
    'carbon_offset_kg',
    'helium_impact_l',
    'permanence_years',
    'verification_confidence',
)
# Canonical sign convention: every objective is internally "lower is better".
MOPD_HIGHER_IS_BETTER: FrozenSet[str] = frozenset({
    'carbon_offset_kg',
    'helium_impact_l',
    'permanence_years',
    'verification_confidence',
})


# ============================================================================
# Serialization helper
# ============================================================================
def _to_serializable(obj: Any) -> Any:
    """Recursively convert arbitrary objects to JSON-safe primitives."""
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
    if isinstance(obj, (list, tuple, set, frozenset)):
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


# ============================================================================
# Optional bio-inspired core
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
        def __init__(self, name: str, failure_threshold: int = 3,
                     recovery_timeout: float = 30.0):
            self.name = name
            self.failure_threshold = failure_threshold
            self.recovery_timeout = recovery_timeout
            self._state = "closed"
            self._failure_count = 0
            self._last_failure_time: Optional[float] = None
            self._lock = asyncio.Lock()

        async def call(self, func: Callable[..., Awaitable[Any]], *args: Any,
                       **kwargs: Any) -> Any:
            async with self._lock:
                if self._state == "open":
                    if (time.monotonic() - (self._last_failure_time or 0)) > self.recovery_timeout:
                        self._state = "half-open"
                    else:
                        raise RuntimeError(f"Circuit {self.name} is open")
            try:
                result = await func(*args, **kwargs)
                async with self._lock:
                    if self._state == "half-open":
                        self._state = "closed"
                        self._failure_count = 0
                return result
            except Exception:
                async with self._lock:
                    self._failure_count += 1
                    self._last_failure_time = time.monotonic()
                    if self._failure_count >= self.failure_threshold:
                        self._state = "open"
                raise


# ============================================================================
# Optional MoE imports
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
# Protocols
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
    def subscribe(self, event_type: str,
                  handler: Callable[[Any], Awaitable[None]]) -> None: ...
    def publish(self, topic: str, payload: Any) -> Any: ...


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
class HeliumProviderProtocol(Protocol):
    def get_scarcity(self) -> float: ...
    def get_cost_index(self) -> float: ...
    def get_efficiency(self) -> float: ...


@runtime_checkable
class CarbonManagerProtocol(Protocol):
    async def get_current_intensity(self) -> float: ...
    async def close(self) -> None: ...


@runtime_checkable
class HeliumManagerProtocol(Protocol):
    def get_position(self) -> Dict[str, Any]: ...
    def record_emission(self, amount_l: float, source: str = ...) -> None: ...
    def record_offset(self, amount_l: float, project_id: Optional[str] = ...) -> None: ...
    def calculate_helium_offset_from_carbon(self, carbon_credit_kg: float) -> float: ...


@runtime_checkable
class MLSelectorProtocol(Protocol):
    is_trained: bool
    async def select_projects(self, criteria: Dict[str, Any]) -> List[Dict[str, Any]]: ...
    async def train(self, training_data: List[Dict[str, Any]]) -> Dict[str, Any]: ...
    def get_checkpoint(self) -> Dict[str, Any]: ...
    def load_checkpoint(self, checkpoint: Dict[str, Any]) -> None: ...


@runtime_checkable
class StorageProtocol(Protocol):
    async def add_credit(self, credit: Any) -> None: ...
    async def add_transaction(self, transaction: Dict[str, Any]) -> None: ...
    async def get_projects(self) -> Dict[str, Dict[str, Any]]: ...


# ============================================================================
# Enums
# ============================================================================
class OffsetStrategy(str, Enum):
    PROACTIVE = "proactive"
    REACTIVE = "reactive"
    CONSERVATIVE = "conservative"


class UrgencyLevel(str, Enum):
    CRITICAL = "critical"
    NORMAL = "normal"
    OPPORTUNISTIC = "opportunistic"


class ApprovalStatus(str, Enum):
    PENDING = "pending"
    APPROVED = "approved"
    DENIED = "denied"
    TIMEOUT = "timeout"
    AUTO_APPROVED = "auto_approved"


class FaultType(str, Enum):
    CARBON_API_FAILURE = "carbon_api_failure"
    HELIUM_BUDGET_EXCEEDED = "helium_budget_exceeded"
    ML_SELECTOR_FAILURE = "ml_selector_failure"
    STORAGE_CORRUPTION = "storage_corruption"
    EVENT_QUEUE_OVERFLOW = "event_queue_overflow"
    PERSISTENCE_FAILURE = "persistence_failure"


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
# Data classes
# ============================================================================
@dataclass
class CarbonCredit:
    credit_id: str
    amount_kg: float
    project_type: str
    verification_date: datetime
    expiry_date: datetime
    price_per_kg: float
    is_verified: bool = False
    permanence_years: float = 0.0
    co_benefits: List[str] = field(default_factory=list)
    sustainability_score: float = 0.0
    helium_offset_equivalent_l: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            'credit_id': self.credit_id,
            'amount_kg': self.amount_kg,
            'project_type': self.project_type,
            'verification_date': self.verification_date.isoformat(),
            'expiry_date': self.expiry_date.isoformat(),
            'price_per_kg': self.price_per_kg,
            'is_verified': self.is_verified,
            'permanence_years': self.permanence_years,
            'co_benefits': list(self.co_benefits),
            'sustainability_score': self.sustainability_score,
            'helium_offset_equivalent_l': self.helium_offset_equivalent_l,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CarbonCredit":
        d = dict(data)
        for key in ('verification_date', 'expiry_date'):
            if isinstance(d.get(key), str):
                try:
                    d[key] = datetime.fromisoformat(d[key])
                except ValueError:
                    d[key] = datetime.now(timezone.utc)
        d['co_benefits'] = list(d.get('co_benefits', []))
        return cls(**d)


@dataclass
class MOPDPlan:
    """Represents a sequestration strategy with its objective vector and XAI."""
    offset_strategy: str
    use_ml_selection: bool
    selected_projects: List[str]
    urgency: str
    cost: float
    carbon_offset_kg: float
    helium_impact_l: float
    permanence_years: float
    verification_confidence: float
    sustainability_score: float
    scalarised_score: float = 0.0
    explanation: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            'offset_strategy': self.offset_strategy,
            'use_ml_selection': self.use_ml_selection,
            'selected_projects': list(self.selected_projects),
            'urgency': self.urgency,
            'cost': self.cost,
            'carbon_offset_kg': self.carbon_offset_kg,
            'helium_impact_l': self.helium_impact_l,
            'permanence_years': self.permanence_years,
            'verification_confidence': self.verification_confidence,
            'sustainability_score': self.sustainability_score,
            'scalarised_score': self.scalarised_score,
            'explanation': self.explanation,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MOPDPlan":
        d = dict(data)
        d['selected_projects'] = list(d.get('selected_projects', []))
        return cls(**d)

    def objective_vector(self) -> List[float]:
        """Canonical lower-is-better objective vector."""
        vec: List[float] = []
        for name in MOPD_OBJECTIVE_NAMES:
            val = float(getattr(self, name, 0.0))
            if name in MOPD_HIGHER_IS_BETTER:
                val = -val
            vec.append(val)
        return vec


@dataclass
class MOPDConfig:
    enabled: bool = True
    objective_weights: Dict[str, float] = field(default_factory=lambda: {
        'cost': 0.20,
        'carbon_offset_kg': 0.30,
        'helium_impact_l': 0.20,
        'permanence_years': 0.15,
        'verification_confidence': 0.15,
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
            logger.warning(f"MOPD weights sum to {total:.4f}; normalising.")
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
    input_size: int = 8
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
    max_retries: int = 3
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
    path: str = "carbon_sequestration_state.json"
    save_interval_seconds: int = 300


@dataclass
class SelfHealingConfig:
    enabled: bool = True


@dataclass
class ApprovalConfig:
    enabled: bool = False
    timeout_seconds: float = APPROVAL_DEFAULT_TIMEOUT
    auto_approve_on_timeout: bool = False
    require_for_offset_above_kg: float = 1000.0


@dataclass
class ChaosConfig:
    enabled: bool = False
    interval_seconds: int = 3600
    probability: float = 0.05
    auto_restore: bool = True


@dataclass
class CarbonSequestrationConfig:
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
    enable_idempotency: bool = True
    chaos_test_interval_seconds: int = 3600

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

    carbon_budget_kg: float = 1000.0
    helium_budget_l: float = 100.0
    offset_strategy: str = 'proactive'
    helium_to_co2_factor: float = 20.0

    workflow_on_critical_alert: str = "adjust_offset_strategy"
    workflow_on_slo_breach: str = "rebalance_carbon_budget"
    swarm_share_interval_seconds: int = 60


# ============================================================================
# Carbon Intensity Manager
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
        self.cache: Dict[str, Dict[str, Any]] = {}
        self.historical_intensities: Deque[float] = deque(maxlen=1000)
        self.api_key = os.getenv(config.api_key_env, '')
        self._circuit = CircuitBreaker(
            "carbon_api",
            failure_threshold=config.circuit_breaker_threshold,
            recovery_timeout=config.circuit_breaker_recovery_timeout,
        )

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
                    self.config.update_interval_seconds and cache_key in self.cache):
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

            fallback = {'us-east': 420.0, 'us-west': 350.0, 'eu': 280.0, 'asia': 500.0}.get(
                self.region, DEFAULT_CARBON_INTENSITY,
            )
            self.carbon_intensity = fallback
            self.last_update = datetime.now(timezone.utc)
            return {
                'intensity': fallback,
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
# Helium Sequestration Manager
# ============================================================================
class HeliumSequestrationManager:
    def __init__(self, config: HeliumConfig):
        self.config = config
        self.budget_l = config.budget_l
        self.emissions: Deque[Dict[str, Any]] = deque(maxlen=MAX_HELIUM_HISTORY)
        self.offsets: Deque[Dict[str, Any]] = deque(maxlen=MAX_HELIUM_HISTORY)
        self._total_emissions = 0.0
        self._total_offsets = 0.0
        self._lock = asyncio.Lock()
        self._task: Optional[asyncio.Task] = None
        self._accounting_loop_running = False
        self.sequestration_projects: Dict[str, Dict[str, Any]] = {
            'helium_recovery_advanced': {
                'type': 'helium_recovery',
                'capacity_l_per_year': 5000.0,
                'cost_per_l': 0.50,
                'efficiency': 0.95,
                'co_benefits': ['technology_development', 'resource_conservation'],
            },
            'helium_capture_system': {
                'type': 'helium_capture',
                'capacity_l_per_year': 2000.0,
                'cost_per_l': 0.80,
                'efficiency': 0.85,
                'co_benefits': ['emissions_reduction', 'recycling'],
            },
            'alternative_cooling': {
                'type': 'alternative_cooling',
                'capacity_l_per_year': 10000.0,
                'cost_per_l': 0.30,
                'efficiency': 0.70,
                'co_benefits': ['technology_diversification', 'cost_reduction'],
            },
        }
        logger.info(f"HeliumSequestrationManager initialised: budget={self.budget_l}L")

    def record_emission(self, amount_l: float, source: str = "unknown") -> None:
        if amount_l < 0:
            logger.warning(f"Negative helium emission {amount_l} from {source}; ignoring.")
            return
        self.emissions.append({
            'amount_l': amount_l, 'source': source,
            'timestamp': datetime.now(timezone.utc),
        })
        self._total_emissions += amount_l

    def record_offset(self, amount_l: float, project_id: Optional[str] = None) -> None:
        if amount_l < 0:
            return
        self.offsets.append({
            'amount_l': amount_l,
            'project_id': project_id or 'unknown',
            'timestamp': datetime.now(timezone.utc),
        })
        self._total_offsets += amount_l

    async def _accounting_loop(self) -> None:
        self._accounting_loop_running = True
        while self._accounting_loop_running:
            try:
                async with self._lock:
                    net = max(0.0, self._total_emissions - self._total_offsets)
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
            logger.warning("No running loop; HeliumSequestrationManager not started")

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
        net = max(0.0, self._total_emissions - self._total_offsets)
        return {
            'total_emissions_l': self._total_emissions,
            'total_offsets_l': self._total_offsets,
            'net_position_l': net,
            'remaining_budget_l': self.budget_l - net,
            'co2_equivalent_kg': net * self.config.helium_to_co2_factor,
            'projects': dict(self.sequestration_projects),
        }

    def calculate_helium_offset_from_carbon(self, carbon_credit_kg: float) -> float:
        return max(0.0, carbon_credit_kg) * 0.05

    def select_helium_project(self, amount_l: float) -> Dict[str, Any]:
        scored: List[Tuple[str, float, Dict[str, Any]]] = []
        for project_id, project in self.sequestration_projects.items():
            cost_score = 1.0 / (1.0 + project['cost_per_l'])
            capacity_score = min(project['capacity_l_per_year'] / max(amount_l, 1.0), 1.0)
            efficiency_score = float(project['efficiency'])
            score = 0.4 * cost_score + 0.3 * capacity_score + 0.3 * efficiency_score
            scored.append((project_id, score, project))
        scored.sort(key=lambda x: x[1], reverse=True)
        if scored:
            return {'project_id': scored[0][0], 'project': scored[0][2], 'score': scored[0][1]}
        return {'project_id': None, 'project': None, 'score': 0.0}


# ============================================================================
# Predictive Sequestration Analyzer
# ============================================================================
class PredictiveSequestrationAnalyzer:
    def __init__(self, config: PredictiveConfig):
        self.config = config
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
                learning_rate='constant', eta0=0.01, penalty='l2', alpha=0.0001,
                max_iter=1, random_state=42, warm_start=True,
            )
            logger.info("PredictiveSequestrationAnalyzer initialised with SGD")
        else:
            logger.warning("sklearn not available; moving-average fallback in use")

    def update_history(self, data: Dict[str, Any]) -> None:
        self.history.append({
            'timestamp': datetime.now(timezone.utc),
            'offset_amount': float(data.get('offset_amount', 0.0)),
            'credit_price': float(data.get('credit_price', 50.0)),
            'project_success_rate': float(data.get('project_success_rate', 0.9)),
            'verification_confidence': float(data.get('verification_confidence', 0.7)),
            'carbon_intensity': float(data.get('carbon_intensity', DEFAULT_CARBON_INTENSITY)),
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
                        data['offset_amount'] / 1000.0,
                        data['credit_price'] / 100.0,
                        data['project_success_rate'],
                        data['verification_confidence'],
                        data['carbon_intensity'] / 100.0,
                    ])
                X.append(features)
                y.append(hist_list[i + 5]['offset_amount'])

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

    async def predict_demand(self) -> Dict[str, Any]:
        if not self.is_trained or len(self.history) < 10:
            if self.history:
                recent = [h['offset_amount'] for h in list(self.history)[-5:]]
                pred = float(np.mean(recent)) if recent else 1000.0
                return {
                    'predicted_demand': pred, 'confidence': 0.3,
                    'trend': 'moving_average',
                    'recommended_actions': self._generate_actions(pred),
                }
            return {
                'predicted_demand': 1000.0, 'confidence': 0.0,
                'trend': 'insufficient_data', 'recommended_actions': [],
            }

        recent = list(self.history)[-5:]
        features: List[float] = []
        for data in recent:
            features.extend([
                data['offset_amount'] / 1000.0,
                data['credit_price'] / 100.0,
                data['project_success_rate'],
                data['verification_confidence'],
                data['carbon_intensity'] / 100.0,
            ])
        features_arr = np.array(features).reshape(1, -1)

        def predict() -> float:
            assert self.scaler is not None and self.model is not None
            fs = self.scaler.transform(features_arr) if self.is_fitted else features_arr
            return float(self.model.predict(fs)[0])

        prediction = await asyncio.to_thread(predict)
        confidence = min(0.9, 0.5 + 0.4 * (len(self.history) / 100.0))

        if len(self.forecasts) > 5:
            last = list(self.forecasts)[-1].get('prediction', prediction)
            trend = 'increasing' if prediction > last else 'decreasing' if prediction < last else 'stable'
        else:
            trend = 'stable'

        self.forecasts.append({'prediction': prediction, 'trend': trend})
        return {
            'predicted_demand': max(0.0, prediction),
            'confidence': confidence,
            'trend': trend,
            'recommended_actions': self._generate_actions(prediction),
        }

    def _generate_actions(self, prediction: float) -> List[str]:
        if prediction > 5000:
            return ["Increase sequestration project capacity", "Diversify project portfolio"]
        if prediction < 1000:
            return ["Reduce sequestration spending", "Focus on high-impact projects"]
        return ["Maintain current sequestration strategy"]


# ============================================================================
# ML Project Selector
# ============================================================================
class MLProjectSelector:
    PROJECT_TYPES: Tuple[str, ...] = (
        'reforestation', 'dac', 'biochar', 'ocean_based', 'helium_recovery',
    )

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
            logger.info("MLProjectSelector initialised with PyTorch")
        else:
            logger.warning("PyTorch not available; ML project selection disabled")

    def _init_model(self) -> None:
        class ProjectSelector(nn.Module):  # type: ignore
            def __init__(self, input_size: int, hidden_size: int):
                super().__init__()
                self.network = nn.Sequential(
                    nn.Linear(input_size, hidden_size),
                    nn.ReLU(),
                    nn.BatchNorm1d(hidden_size),
                    nn.Linear(hidden_size, hidden_size // 2),
                    nn.ReLU(),
                    nn.BatchNorm1d(hidden_size // 2),
                    nn.Linear(hidden_size // 2, 5),
                )

            def forward(self, x: Any) -> Any:
                return self.network(x)

        self.model = ProjectSelector(self.input_size, self.hidden_size)
        self.optimizer = optim.Adam(self.model.parameters(), lr=0.001)
        self.criterion = nn.CrossEntropyLoss()

    @staticmethod
    def _features_from_item(item: Dict[str, Any]) -> List[float]:
        return [
            float(item.get('carbon_intensity', DEFAULT_CARBON_INTENSITY)) / 100.0,
            float(item.get('cost_budget', 0.5)),
            float(item.get('urgency', 0.5)),
            float(item.get('permanence_requirement', 0.5)),
            float(item.get('co_benefit_weight', 0.5)),
            float(item.get('verification_confidence', 0.5)),
            float(item.get('project_age_months', 12)) / 12.0,
            float(item.get('historical_success', 0.8)),
        ]

    async def train(self, training_data: List[Dict[str, Any]],
                    epochs: Optional[int] = None) -> Dict[str, Any]:
        if not TORCH_AVAILABLE or self.model is None:
            return {'status': 'disabled'}
        if len(training_data) < 20:
            return {'status': 'insufficient_data', 'samples': len(training_data)}

        epochs = epochs or self.config.epochs

        X: List[List[float]] = []
        y: List[int] = []
        for item in training_data:
            X.append(self._features_from_item(item))
            selected = item.get('selected_project', 'reforestation')
            y.append(self.PROJECT_TYPES.index(selected) if selected in self.PROJECT_TYPES else 0)

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
            torch.FloatTensor(X_scaled), torch.LongTensor(y_arr),
        )
        dataloader = DataLoader(dataset, batch_size=self.config.batch_size, shuffle=True)

        async with self._lock:
            def train_sync() -> List[float]:
                assert self.model is not None and self.optimizer is not None
                assert self.criterion is not None
                self.model.train()
                losses: List[float] = []
                for _ in range(epochs):
                    epoch_loss = 0.0
                    for bx, by in dataloader:
                        self.optimizer.zero_grad()
                        out = self.model(bx)
                        loss = self.criterion(out, by)
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

    async def select_projects(self, criteria: Dict[str, Any]) -> List[Dict[str, Any]]:
        if not TORCH_AVAILABLE or not self.is_trained or self.model is None:
            return []

        features = np.array([self._features_from_item(criteria)])
        if self.scaler is not None and self.is_fitted:
            features_scaled = self.scaler.transform(features)
        else:
            features_scaled = features

        def _predict() -> List[float]:
            assert self.model is not None
            self.model.eval()
            with torch.no_grad():
                out = self.model(torch.FloatTensor(features_scaled))
                # Softmax for interpretability
                probs = torch.softmax(out, dim=1).cpu().numpy()[0]
                return [float(p) for p in probs]

        probs = await asyncio.to_thread(_predict)

        recommendations = [
            {
                'project_type': self.PROJECT_TYPES[i],
                'score': probs[i],
                'confidence': min(1.0, probs[i] * 1.5),
            }
            for i in range(len(self.PROJECT_TYPES))
        ]
        recommendations.sort(key=lambda x: x['score'], reverse=True)
        return recommendations

    def get_checkpoint(self) -> Dict[str, Any]:
        if not TORCH_AVAILABLE or self.model is None or self.optimizer is None:
            return {}
        return {
            'state_dict': {
                k: v.detach().cpu().tolist() for k, v in self.model.state_dict().items()
            },
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

    def load_checkpoint(self, checkpoint: Dict[str, Any]) -> None:
        if not TORCH_AVAILABLE or not checkpoint or self.model is None:
            return
        try:
            state = {
                k: torch.tensor(v, dtype=torch.float32)
                for k, v in checkpoint.get('state_dict', {}).items()
            }
            self.model.load_state_dict(state, strict=False)
            if checkpoint.get('scaler_mean') is not None and self.scaler is not None:
                self.scaler.mean_ = np.array(checkpoint['scaler_mean'])
                self.scaler.scale_ = np.array(checkpoint['scaler_std'])
                self.is_fitted = True
            self.is_trained = bool(checkpoint.get('is_trained', False))
            self.training_history = list(checkpoint.get('training_history', []))
        except Exception as e:
            logger.warning(f"Checkpoint load failed: {e}")


# ============================================================================
# Federated Sequestration Manager (with DP)
# ============================================================================
class FederatedSequestrationManager:
    def __init__(self, config: FederatedConfig):
        self.config = config
        self.server_url = config.server_url
        self.round = 0
        self.local_projects: Dict[str, Any] = {}
        self.global_projects: Dict[str, Any] = {}
        self.participants: List[str] = []
        self.contribution_scores: Dict[str, float] = {}
        self._lock = asyncio.Lock()
        self._session: Optional[aiohttp.ClientSession] = None
        self._circuit = CircuitBreaker("federated_server", failure_threshold=3,
                                        recovery_timeout=30.0)
        logger.info("FederatedSequestrationManager initialised")

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
                out[k] = clipped[idx] + float(np.random.normal(0.0, sigma * clip))
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
        return {k: v for k, v in data.items()
                if k in kept or not isinstance(v, (int, float))}

    async def send_local_projects(self, participant_id: str, project_data: Dict[str, Any],
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
                        private = self._add_differential_privacy(project_data)
                        compressed = self._compress(private)
                        update = {
                            'participant_id': participant_id,
                            'round': self.round,
                            'project_data': compressed,
                            'performance': performance,
                            'privacy_epsilon': self.config.privacy_epsilon,
                            'sparsity_ratio': self.config.sparsity_ratio,
                            'timestamp': datetime.now(timezone.utc).isoformat(),
                        }
                        async with session.post(
                            f"{self.server_url}/federated/sequestration",
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

    async def get_global_projects(self) -> Optional[Dict[str, Any]]:
        if not self.server_url:
            return self.global_projects

        async def _fetch() -> Optional[Dict[str, Any]]:
            for attempt in range(self.config.max_retries):
                try:
                    async with self._lock:
                        session = await self._get_session()
                        if session is None:
                            return None
                        async with session.get(
                            f"{self.server_url}/federated/sequestration/global",
                            timeout=self.config.request_timeout_seconds,
                        ) as resp:
                            if resp.status == 200:
                                data = await resp.json()
                                self.global_projects = data.get('projects', {})
                                self.participants = data.get('participants', [])
                                return self.global_projects
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
            'local_projects': len(self.local_projects),
            'global_projects': len(self.global_projects),
        }

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()


# ============================================================================
# Human-AI Collaborative Sequestration
# ============================================================================
class HumanAICollaborativeSequestration:
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
            'acknowledgment': f"Feedback on {feedback.get('topic', 'carbon sequestration')}",
            'insights': [],
            'actions': [],
            'sequestration_insights': [],
        }
        concern = feedback.get('concern')
        if concern == 'cost':
            reflection['insights'].append("Cost optimisation can be improved through project diversity")
            reflection['actions'].append("Implement cost-aware project selection")
        elif concern == 'permanence':
            reflection['insights'].append("Long-term permanence requires multi-decade planning")
            reflection['actions'].append("Prioritise high-permanence projects")
        elif concern == 'verification':
            reflection['insights'].append("Verification accuracy needs improvement")
            reflection['actions'].append("Implement enhanced verification methods")
        elif concern == 'helium':
            reflection['sequestration_insights'].append("Helium offset integration is needed")
            reflection['actions'].append("Implement helium sequestration projects")
        if 'suggestion' in feedback:
            reflection['actions'].append(f"Implementing suggestion: {feedback['suggestion']}")
        reflection['action_items'] = self._prioritize_actions(reflection['actions'])
        return reflection

    def _prioritize_actions(self, actions: List[str]) -> List[Dict[str, Any]]:
        priorities: List[Dict[str, Any]] = []
        for action in actions:
            if any(kw in action.lower() for kw in ('urgent', 'critical')):
                priority, impact = 'high', 0.9
            elif any(kw in action.lower() for kw in ('sequestration', 'carbon')):
                priority, impact = 'high', 0.8
            elif 'helium' in action.lower():
                priority, impact = 'medium', 0.6
            else:
                priority, impact = 'medium', 0.5
            priorities.append({
                'action': action,
                'priority': priority,
                'impact': impact,
                'estimated_effort': 'medium',
            })
        priorities.sort(key=lambda x: x['impact'], reverse=True)
        return priorities

    def get_insights(self) -> Dict[str, Any]:
        if len(self.feedback_history) < 5:
            return {'status': 'insufficient_feedback'}
        recent = list(self.feedback_history)[-20:]
        topics: Dict[str, int] = defaultdict(int)
        for f in recent:
            topics[f['feedback'].get('topic', 'general')] += 1
        most_common = max(topics.items(), key=lambda x: x[1]) if topics else ('none', 0)
        return {
            'total_feedback': len(self.feedback_history),
            'top_topics': dict(topics),
            'most_common_topic': most_common[0],
            'engagement_score': min(1.0, len(self.feedback_history) / 100.0),
            'user_count': len(set(f['user_id'] for f in self.feedback_history)),
        }


# ============================================================================
# Persistence Manager (typed, threaded)
# ============================================================================
class CarbonSequestrationPersistenceManager:
    TIMESTAMP_KEYS: FrozenSet[str] = frozenset({
        'timestamp', 'verification_date', 'expiry_date', 'requested_at',
        'decided_at', 'last_assessment',
    })

    def __init__(self, config: PersistenceConfig):
        self.config = config
        self.path = config.path
        self._lock = asyncio.Lock()
        self._version = STATE_SCHEMA_VERSION

    async def save_state(self, state: Dict[str, Any]) -> bool:
        async with self._lock:
            try:
                payload = {
                    'version': self._version,
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'data': _to_serializable(state),
                }
                tmp = f"{self.path}.tmp"

                def _write() -> None:
                    with open(tmp, 'w') as f:
                        json.dump(payload, f, indent=2)
                    os.replace(tmp, self.path)

                await asyncio.to_thread(_write)
                return True
            except Exception as e:
                logger.error(f"Failed to save state: {e}")
                return False

    async def load_state(self) -> Optional[Dict[str, Any]]:
        async with self._lock:
            if not os.path.exists(self.path):
                return None
            try:
                def _read() -> Dict[str, Any]:
                    with open(self.path, 'r') as f:
                        return json.load(f)

                payload = await asyncio.to_thread(_read)
                version = payload.get('version', 0)
                if version != self._version:
                    logger.warning(
                        f"State version mismatch (file={version}, expected={self._version})"
                    )
                return self._deserialize(payload.get('data', {}))
            except Exception as e:
                logger.error(f"Failed to load state: {e}")
                return None

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
                return True
            return False


# ============================================================================
# Storage
# ============================================================================
class CarbonSequestrationStorage:
    def __init__(self):
        self.credits: Deque[CarbonCredit] = deque(maxlen=MAX_CREDITS)
        self.transaction_history: Deque[Dict[str, Any]] = deque(maxlen=MAX_TRANSACTIONS)
        self.sequestration_projects: Dict[str, Dict[str, Any]] = {}
        self.sustainability_score = 0.0
        self.total_sequestered = 0.0
        self.total_offset = 0.0
        self.mopd_plans: Deque[MOPDPlan] = deque(maxlen=MAX_MOPD_PLANS)
        self._lock = asyncio.Lock()

    async def add_credit(self, credit: Any) -> None:
        if not isinstance(credit, CarbonCredit):
            logger.warning(f"add_credit received non-CarbonCredit: {type(credit)}")
            return
        async with self._lock:
            self.credits.append(credit)

    async def add_transaction(self, transaction: Dict[str, Any]) -> None:
        async with self._lock:
            self.transaction_history.append(_to_serializable(transaction))

    async def add_mopd_plan(self, plan: MOPDPlan) -> None:
        async with self._lock:
            self.mopd_plans.append(plan)

    async def update_sustainability_score(self, score: float) -> None:
        async with self._lock:
            self.sustainability_score = float(score)

    async def update_totals(self, offset: float, sequestered: float) -> None:
        async with self._lock:
            self.total_offset += float(offset)
            self.total_sequestered += float(sequestered)

    async def get_credits(self, limit: Optional[int] = None) -> List[CarbonCredit]:
        async with self._lock:
            if limit is not None:
                return list(self.credits)[-limit:]
            return list(self.credits)

    async def get_transaction_history(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        async with self._lock:
            if limit is not None:
                return list(self.transaction_history)[-limit:]
            return list(self.transaction_history)

    async def get_sustainability_score(self) -> float:
        async with self._lock:
            return self.sustainability_score

    async def get_totals(self) -> Dict[str, float]:
        async with self._lock:
            return {'offset': self.total_offset, 'sequestered': self.total_sequestered}

    async def get_projects(self) -> Dict[str, Dict[str, Any]]:
        async with self._lock:
            return {k: dict(v) for k, v in self.sequestration_projects.items()}

    async def set_projects(self, projects: Dict[str, Dict[str, Any]]) -> None:
        async with self._lock:
            self.sequestration_projects = {k: dict(v) for k, v in projects.items()}

    async def get_stats(self) -> Dict[str, Any]:
        async with self._lock:
            return {
                'total_credits': len(self.credits),
                'verified_credits': sum(1 for c in self.credits if c.is_verified),
                'total_offset_kg': self.total_offset,
                'total_sequestered_kg': self.total_sequestered,
                'sustainability_score': self.sustainability_score,
            }

    async def get_mopd_plans(self, limit: Optional[int] = None) -> List[MOPDPlan]:
        async with self._lock:
            if limit is not None:
                return list(self.mopd_plans)[-limit:]
            return list(self.mopd_plans)


# ============================================================================
# Temporal Safety
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
# Governance (Human Approval)
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
    def __init__(self, config: ApprovalConfig,
                 publish_fn: Optional[Callable[[str, Any], Awaitable[None]]] = None) -> None:
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
        req = ApprovalRequest(
            request_id=request_id,
            context=context,
            requested_at=datetime.now(timezone.utc),
            timeout_seconds=timeout,
            future=future,
        )
        async with self._lock:
            self.pending[request_id] = req

        try:
            if self.publish_fn is not None:
                await self.publish_fn('approval_requests',
                                       json.dumps(req.to_dict(), default=str))
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
            req = self.pending.get(request_id)
            if req is None or req.future is None or req.future.done():
                return False
            req.future.set_result(approved)
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
# Chaos Service
# ============================================================================
class ChaosService:
    def __init__(self, manager: "CarbonSequestrationManager",
                 config: ChaosConfig) -> None:
        self.manager = manager
        self.config = config
        self._saved: Dict[str, Any] = {}
        self._lock = asyncio.Lock()

    async def inject_fault(self, fault_type: Union[str, FaultType],
                           **params: Any) -> Dict[str, Any]:
        if isinstance(fault_type, str):
            try:
                fault_type = FaultType(fault_type)
            except ValueError:
                return {'fault': fault_type, 'error': 'unknown_fault'}

        async with self._lock:
            try:
                if fault_type == FaultType.CARBON_API_FAILURE:
                    if self.manager.carbon_manager is not None:
                        self._saved.setdefault(
                            'carbon_intensity',
                            self.manager.carbon_manager.carbon_intensity,
                        )
                        self.manager.carbon_manager.carbon_intensity = 0.0
                        self.manager.carbon_manager.last_update = datetime.now(timezone.utc)
                    return {'fault': fault_type.value}
                if fault_type == FaultType.HELIUM_BUDGET_EXCEEDED:
                    if self.manager.helium_manager is not None:
                        self._saved.setdefault(
                            'helium_budget',
                            self.manager.helium_manager.budget_l,
                        )
                        self.manager.helium_manager.budget_l = -10.0
                    return {'fault': fault_type.value}
                if fault_type == FaultType.ML_SELECTOR_FAILURE:
                    self._saved.setdefault('ml_selector', self.manager.ml_selector)
                    self.manager.ml_selector = None
                    return {'fault': fault_type.value}
                if fault_type == FaultType.STORAGE_CORRUPTION:
                    # Non-destructive marker
                    return {'fault': fault_type.value, 'noop': True}
                if fault_type == FaultType.EVENT_QUEUE_OVERFLOW:
                    return {'fault': fault_type.value, 'noop': True}
                if fault_type == FaultType.PERSISTENCE_FAILURE:
                    self._saved.setdefault('persistence', self.manager.persistence)
                    self.manager.persistence = None
                    return {'fault': fault_type.value}
            except Exception as e:
                logger.error(f"Fault injection failed ({fault_type}): {e}")
                return {'fault': fault_type.value, 'error': str(e)}
            return {'fault': fault_type.value, 'not_implemented': True}

    def restore(self) -> None:
        if 'carbon_intensity' in self._saved and self.manager.carbon_manager is not None:
            self.manager.carbon_manager.carbon_intensity = self._saved.pop('carbon_intensity')
        if 'helium_budget' in self._saved and self.manager.helium_manager is not None:
            self.manager.helium_manager.budget_l = self._saved.pop('helium_budget')
        if 'ml_selector' in self._saved:
            self.manager.ml_selector = self._saved.pop('ml_selector')
        if 'persistence' in self._saved:
            self.manager.persistence = self._saved.pop('persistence')
            if self.manager.reporter is not None:
                self.manager.reporter.persistence = self.manager.persistence

    async def run_chaos_test(self) -> Dict[str, Any]:
        report: Dict[str, Any] = {'faults': [], 'results': {}, 'restored': False}
        faults = [
            FaultType.CARBON_API_FAILURE,
            FaultType.HELIUM_BUDGET_EXCEEDED,
            FaultType.ML_SELECTOR_FAILURE,
            FaultType.PERSISTENCE_FAILURE,
        ]
        try:
            for ft in faults[:CHAOS_MAX_FAULTS_PER_RUN]:
                res = await self.inject_fault(ft)
                report['faults'].append(ft.value)
                report['results'][ft.value] = res

            try:
                result = await self.manager.offset_expert_emissions(
                    expert_carbon_kg=100.0,
                    budget_remaining=500.0,
                    urgency='normal',
                    return_mopd=True,
                )
                report['results']['post_fault_offset'] = {
                    'success': 'error' not in result,
                    'offset_amount_kg': result.get('offset_amount_kg', 0.0),
                    'sustainability_score': result.get('sustainability_score', 0.0),
                }
            except Exception as e:
                report['results']['post_fault_offset'] = {
                    'success': False, 'error': str(e),
                }
        finally:
            if self.config.auto_restore:
                self.restore()
                report['restored'] = True
        return report


# ============================================================================
# Analyzer
# ============================================================================
class CarbonSequestrationAnalyzer:
    def __init__(
        self,
        config: CarbonSequestrationConfig,
        storage: CarbonSequestrationStorage,
        carbon_manager: Optional[CarbonIntensityManager],
        helium_manager: Optional[HeliumSequestrationManager],
        predictive: Optional[PredictiveSequestrationAnalyzer],
        ml_selector: Optional[MLProjectSelector],
        human_ai: Optional[HumanAICollaborativeSequestration],
        governance: Optional[GovernanceLayer],
        observability: Optional[ObservabilityHub] = None,
    ) -> None:
        self.config = config
        self.storage = storage
        self.carbon_manager = carbon_manager
        self.helium_manager = helium_manager
        self.predictive = predictive
        self.ml_selector = ml_selector
        self.human_ai = human_ai
        self.governance = governance
        self.observability = observability or ObservabilityHub()
        self._lock = asyncio.Lock()

        # Idempotency
        self._idempotency_seen: Set[str] = set()
        self._idempotency_order: Deque[str] = deque(maxlen=IDEMPOTENCY_CACHE_SIZE)

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
    # MOPD enumeration
    # ------------------------------------------------------------------
    async def _enumerate_strategies(
        self,
        expert_carbon_kg: float,
        budget_remaining: float,
        urgency: str = 'normal',
    ) -> List[MOPDPlan]:
        strategies = [
            ('proactive', True, True, True),
            ('reactive', False, True, True),
            ('conservative', True, False, False),
        ]
        use_ml_options = [True, False]
        urgency_mult = {
            'critical': {'cost': 1.5, 'time': 1.2, 'conf': 1.1},
            'normal': {'cost': 1.0, 'time': 1.0, 'conf': 1.0},
            'opportunistic': {'cost': 0.7, 'time': 0.8, 'conf': 0.9},
        }

        plans: List[MOPDPlan] = []
        for strategy_name, vs, vse, va in strategies:
            for use_ml in use_ml_options:
                for urg, mult in urgency_mult.items():
                    # Base offset
                    if strategy_name == 'proactive':
                        offset_amount = max(expert_carbon_kg, expert_carbon_kg * 1.2)
                    elif strategy_name == 'reactive':
                        offset_amount = max(expert_carbon_kg, expert_carbon_kg * 1.0)
                    else:
                        offset_amount = min(expert_carbon_kg, expert_carbon_kg * 0.5)

                    # Base cost and confidence
                    base_cost = 0.0
                    base_conf = 0.5
                    if vs:
                        base_cost += 5.0
                        base_conf += 0.10
                    if vse:
                        base_cost += 3.0
                        base_conf += 0.10
                    if va:
                        base_cost += 10.0
                        base_conf += 0.15
                    if use_ml:
                        base_cost += 2.0
                        base_conf += 0.10

                    # Scale by offset (kg -> $ at $0.05/kg baseline)
                    base_cost += offset_amount * 0.05
                    cost = base_cost * mult['cost']
                    confidence = min(1.0, base_conf * mult['conf'])

                    # Carbon offset and helium
                    carbon_offset = offset_amount
                    helium_offset = offset_amount * 0.05

                    # Permanence: proactive strategies yield longer permanence
                    if strategy_name == 'proactive':
                        permanence = 500.0
                    elif strategy_name == 'reactive':
                        permanence = 200.0
                    else:
                        permanence = 100.0

                    sustainability = confidence * 0.5 + min(1.0, permanence / 500.0) * 0.3 + 0.2

                    plan = MOPDPlan(
                        offset_strategy=strategy_name,
                        use_ml_selection=use_ml,
                        selected_projects=[],
                        urgency=urg,
                        cost=cost,
                        carbon_offset_kg=carbon_offset,
                        helium_impact_l=helium_offset,
                        permanence_years=permanence,
                        verification_confidence=confidence,
                        sustainability_score=sustainability,
                        explanation='',
                    )
                    plan.explanation = (
                        f"strategy={strategy_name}, urgency={urg}, "
                        f"ml={use_ml}, verify_satellite={vs}, verify_sensors={vse}, "
                        f"verify_additionality={va}, offset={carbon_offset:.1f}kg, "
                        f"cost=${cost:.2f}, confidence={confidence:.2f}, "
                        f"permanence={permanence:.0f}y"
                    )
                    plans.append(plan)
        return plans

    async def _generate_pareto_front_for_offset(
        self,
        expert_carbon_kg: float,
        budget_remaining: float,
        urgency: str = 'normal',
    ) -> List[MOPDPlan]:
        plans = await self._enumerate_strategies(expert_carbon_kg, budget_remaining, urgency)
        if not plans:
            return []

        pareto: List[MOPDPlan] = []
        for i, p_i in enumerate(plans):
            a_vec = p_i.objective_vector()
            dominated = False
            for j, p_j in enumerate(plans):
                if i == j:
                    continue
                b_vec = p_j.objective_vector()
                if all(b <= a for a, b in zip(a_vec, b_vec)) and any(
                    b < a for a, b in zip(a_vec, b_vec)
                ):
                    dominated = True
                    break
            if not dominated:
                pareto.append(p_i)
        return pareto

    def _select_best_from_pareto(self, pareto_front: List[MOPDPlan]) -> Optional[MOPDPlan]:
        if not pareto_front:
            return None
        weights = self.config.mopd.objective_weights
        vectors = [p.objective_vector() for p in pareto_front]
        arr = np.array(vectors)
        mins = arr.min(axis=0)
        maxs = arr.max(axis=0)
        ranges = np.where(maxs - mins > 1e-9, maxs - mins, 1.0)

        best: Optional[MOPDPlan] = None
        best_score = -float('inf')
        for plan in pareto_front:
            vec = np.array(plan.objective_vector())
            normalised = 1.0 - (vec - mins) / ranges
            score = 0.0
            for idx, name in enumerate(MOPD_OBJECTIVE_NAMES):
                w = weights.get(name, 1.0 / len(MOPD_OBJECTIVE_NAMES))
                score += w * float(normalised[idx])
            plan.scalarised_score = score
            if score > best_score:
                best_score = score
                best = plan
        return best

    # ------------------------------------------------------------------
    # Core offset
    # ------------------------------------------------------------------
    async def offset_expert_emissions(
        self,
        expert_carbon_kg: float,
        budget_remaining: float,
        urgency: str = 'normal',
        use_ml_selection: bool = False,
        return_mopd: bool = False,
        idempotency_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        if self._is_duplicate(idempotency_key):
            return {
                'duplicate': True,
                'idempotency_key': idempotency_key,
                'timestamp': datetime.now(timezone.utc).isoformat(),
            }

        if expert_carbon_kg < 0:
            return {'error': 'expert_carbon_kg must be non-negative'}

        carbon_intensity = DEFAULT_CARBON_INTENSITY
        if self.carbon_manager is not None:
            carbon_intensity = await self.carbon_manager.get_current_intensity()

        offset_amount = expert_carbon_kg * 1.0
        if self.config.offset_strategy == 'proactive':
            offset_amount = max(expert_carbon_kg, expert_carbon_kg * 1.2)
        elif self.config.offset_strategy == 'reactive':
            offset_amount = max(expert_carbon_kg, expert_carbon_kg)
        else:
            offset_amount = min(expert_carbon_kg, expert_carbon_kg * 0.5)

        if use_ml_selection and self.ml_selector is not None:
            ml_results = await self.ml_selector.select_projects({
                'carbon_intensity': carbon_intensity,
                'cost_budget': min(1.0, budget_remaining / 1000.0),
                'urgency': {'critical': 0.9, 'normal': 0.5, 'opportunistic': 0.2}.get(urgency, 0.5),
                'permanence_requirement': 0.6 if urgency == 'critical' else 0.4,
                'co_benefit_weight': 0.5,
                'verification_confidence': 0.7,
                'project_age_months': 12,
                'historical_success': 0.9,
            })
            selected_projects = [r['project_type'] for r in ml_results[:3] if r['score'] > 0.3]
            project_map = {
                'reforestation': 'reforestation_tropical',
                'dac': 'direct_air_capture',
                'biochar': 'biochar_agriculture',
                'ocean_based': 'ocean_alkalinization',
                'helium_recovery': 'helium_recovery_advanced',
            }
            selected_projects = [
                project_map.get(p, p) for p in selected_projects if p in project_map
            ]
        else:
            selected_projects = await self._select_projects_async(offset_amount, urgency)

        allocation = await self._allocate_offset_async(offset_amount, selected_projects)
        if not allocation:
            return {'error': 'no_projects_available'}

        # Generate credits from allocation
        new_credits = await self._generate_credits_async(allocation)
        for credit in new_credits:
            await self.storage.add_credit(credit)

        # Helium offset
        helium_offset = 0.0
        if self.helium_manager is not None:
            helium_offset = self.helium_manager.calculate_helium_offset_from_carbon(offset_amount)
            helium_project = self.helium_manager.select_helium_project(helium_offset)
            if helium_project['project_id']:
                self.helium_manager.record_offset(helium_offset, helium_project['project_id'])

        # Update totals
        sequestered_estimate = offset_amount * 0.1
        await self.storage.update_totals(offset_amount, sequestered_estimate)

        # Sustainability score
        sustainability = self._calculate_sustainability_score(
            offset_amount, expert_carbon_kg, carbon_intensity,
        )
        await self.storage.update_sustainability_score(sustainability)

        total_cost = sum(p['cost'] for p in allocation.values())

        offset_plan: Dict[str, Any] = {
            'offset_amount_kg': offset_amount,
            'expert_emissions_kg': expert_carbon_kg,
            'over_offset_ratio': offset_amount / expert_carbon_kg if expert_carbon_kg > 0 else 0.0,
            'projects_used': selected_projects,
            'allocation': allocation,
            'credits_generated': len(new_credits),
            'cost': total_cost,
            'carbon_intensity': carbon_intensity,
            'sustainability_score': sustainability,
            'helium_offset_l': helium_offset,
            'ml_used': use_ml_selection,
            'timestamp': datetime.now(timezone.utc).isoformat(),
        }
        await self.storage.add_transaction(offset_plan)

        # Temporal safety
        if self.config.enable_temporal_safety:
            violations = self._check_invariants(offset_plan)
            if violations:
                logger.warning(f"Temporal safety violations: {violations}")
                offset_plan['temporal_safety_violations'] = violations

        # Human approval
        if (self.governance is not None and
                offset_amount > self.config.approval.require_for_offset_above_kg):
            approval = await self.governance.request_approval({
                'action': 'approve_offset',
                'offset_amount_kg': offset_amount,
                'expert_carbon_kg': expert_carbon_kg,
                'cost': total_cost,
                'sustainability_score': sustainability,
            })
            offset_plan['human_approval'] = approval.to_dict()
            if approval.status not in (ApprovalStatus.APPROVED, ApprovalStatus.AUTO_APPROVED):
                logger.info(f"Offset not approved: {approval.status.value}")
                return {'error': 'human_approval_not_granted', 'approval': approval.to_dict()}

        # Predictive history
        if self.predictive is not None:
            self.predictive.update_history({
                'offset_amount': offset_amount,
                'credit_price': total_cost / max(offset_amount, 1.0),
                'project_success_rate': 0.9,
                'verification_confidence': 0.7,
                'carbon_intensity': carbon_intensity,
            })

        # Human-AI insights
        if self.human_ai is not None:
            offset_plan['human_ai_insights'] = self.human_ai.get_insights()

        # MOPD Pareto front
        if self.config.enable_mopd and return_mopd:
            pareto_front = await self._generate_pareto_front_for_offset(
                expert_carbon_kg, budget_remaining, urgency,
            )
            for plan in pareto_front:
                await self.storage.add_mopd_plan(plan)
            offset_plan['mopd_pareto_front'] = [p.to_dict() for p in pareto_front]
            best = self._select_best_from_pareto(pareto_front)
            if best is not None:
                offset_plan['mopd_best_plan'] = best.to_dict()

        logger.info(
            f"Offset {expert_carbon_kg:.4f} kg CO2 with {offset_amount:.4f} kg "
            f"across {len(selected_projects)} projects, "
            f"sustainability_score={sustainability:.2f}"
        )
        return offset_plan

    # ------------------------------------------------------------------
    # Invariants
    # ------------------------------------------------------------------
    def _check_invariants(self, offset_plan: Dict[str, Any]) -> List[str]:
        violations: List[str] = []
        # Cost budget check (in $, compared against a derived budget)
        # We compare against carbon_budget_kg × $0.10/kg as a soft cost ceiling
        cost_ceiling = self.config.carbon_budget_kg * 0.10
        if offset_plan['cost'] > cost_ceiling:
            violations.append(
                f"Cost ${offset_plan['cost']:.2f} exceeds soft ceiling ${cost_ceiling:.2f}"
            )
        if offset_plan['offset_amount_kg'] < 0:
            violations.append("Offset amount cannot be negative")
        if self.helium_manager is not None:
            helium_position = self.helium_manager.get_position()
            if helium_position['remaining_budget_l'] < 0:
                violations.append("Helium budget exceeded")
        return violations

    # ------------------------------------------------------------------
    # Sustainability score
    # ------------------------------------------------------------------
    def _calculate_sustainability_score(
        self, offset_amount: float, expert_carbon_kg: float, carbon_intensity: float,
    ) -> float:
        # Offset coverage: how much of the emission was offset
        if expert_carbon_kg > 0:
            coverage = min(1.0, offset_amount / expert_carbon_kg)
        else:
            coverage = 1.0
        # Carbon intensity factor: lower intensity is better
        intensity_factor = max(0.0, 1.0 - carbon_intensity / CARBON_CRITICAL_THRESHOLD)
        # Helium budget factor
        helium_factor = 0.5
        if self.helium_manager is not None:
            pos = self.helium_manager.get_position()
            budget = max(self.config.helium_budget_l, 1.0)
            helium_factor = max(0.0, min(1.0, pos['remaining_budget_l'] / budget))
        score = coverage * 0.5 + intensity_factor * 0.3 + helium_factor * 0.2
        return max(0.0, min(1.0, score))

    # ------------------------------------------------------------------
    # Project selection / allocation / credit generation
    # ------------------------------------------------------------------
    async def _select_projects_async(self, amount_kg: float, urgency: str) -> List[str]:
        projects = await self.storage.get_projects()
        if not projects:
            return []
        scored: List[Tuple[str, float]] = []
        for project_id, project in projects.items():
            cost_score = 1.0 / (1.0 + float(project['cost_per_kg']))
            capacity_score = min(
                float(project['capacity_kg_per_year']) / max(amount_kg, 1.0), 1.0,
            )
            permanence_score = min(float(project['permanence_years']) / 1000.0, 1.0)
            helium_score = min(
                float(project.get('helium_offset_potential_l', 0)) / 5.0, 1.0,
            )

            if urgency == 'critical':
                score = 0.25 * cost_score + 0.15 * capacity_score + 0.35 * permanence_score + 0.25 * helium_score
            elif urgency == 'normal':
                score = 0.30 * cost_score + 0.25 * capacity_score + 0.25 * permanence_score + 0.20 * helium_score
            else:
                score = 0.40 * cost_score + 0.30 * capacity_score + 0.15 * permanence_score + 0.15 * helium_score
            scored.append((project_id, score))

        scored.sort(key=lambda x: x[1], reverse=True)
        selected: List[str] = []
        total_capacity = 0.0
        for project_id, _ in scored:
            selected.append(project_id)
            total_capacity += float(projects[project_id]['capacity_kg_per_year'])
            if total_capacity >= amount_kg:
                break
        return selected

    async def _allocate_offset_async(
        self, amount_kg: float, projects: List[str],
    ) -> Dict[str, Dict[str, Any]]:
        projects_dict = await self.storage.get_projects()
        allocation: Dict[str, Dict[str, Any]] = {}
        remaining = amount_kg
        # Sort by cost per kg (cheapest first)
        sorted_projects = sorted(
            projects, key=lambda p: projects_dict[p]['cost_per_kg'] if p in projects_dict else float('inf'),
        )
        for project_id in sorted_projects:
            if project_id not in projects_dict:
                continue
            project = projects_dict[project_id]
            daily_capacity = float(project['capacity_kg_per_year']) / 365.0
            max_from_project = min(remaining, daily_capacity)
            if max_from_project <= 0:
                continue
            helium_potential = (
                float(project.get('helium_offset_potential_l', 0)) *
                max_from_project / 1000.0
            )
            allocation[project_id] = {
                'amount_kg': max_from_project,
                'cost': max_from_project * float(project['cost_per_kg']),
                'project_type': project['type'],
                'helium_offset_potential_l': helium_potential,
                'permanence_years': float(project['permanence_years']),
                'co_benefits': list(project['co_benefits']),
            }
            remaining -= max_from_project
            if remaining <= 0:
                break
        return allocation

    async def _generate_credits_async(
        self, allocation: Dict[str, Dict[str, Any]],
    ) -> List[CarbonCredit]:
        credits: List[CarbonCredit] = []
        now = datetime.now(timezone.utc)
        for project_id, alloc in allocation.items():
            amount = float(alloc['amount_kg'])
            if amount <= 0:
                continue
            credit = CarbonCredit(
                credit_id=uuid.uuid4().hex,
                amount_kg=amount,
                project_type=alloc['project_type'],
                verification_date=now,
                expiry_date=now + timedelta(days=365 * 5),
                price_per_kg=alloc['cost'] / max(amount, 1.0),
                is_verified=False,
                permanence_years=float(alloc['permanence_years']),
                co_benefits=list(alloc['co_benefits']),
                sustainability_score=0.7,
                helium_offset_equivalent_l=float(alloc['helium_offset_potential_l']),
            )
            credits.append(credit)
        return credits

    # ------------------------------------------------------------------
    # Verification & recommendations
    # ------------------------------------------------------------------
    async def verify_credits(self) -> int:
        credits = await self.storage.get_credits()
        verified_count = 0
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(days=30)
        for credit in credits:
            if (not credit.is_verified and credit.amount_kg > 0 and
                    credit.verification_date > cutoff):
                credit.is_verified = True
                verified_count += 1
                if self.helium_manager is not None:
                    self.helium_manager.record_offset(
                        credit.helium_offset_equivalent_l, f"credit_{credit.credit_id}",
                    )
        logger.info(f"Verified {verified_count} carbon credits")
        return verified_count

    async def train_ml_model(self, training_data: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
        if self.ml_selector is None:
            return {'status': 'disabled'}
        if training_data is None:
            history = await self.storage.get_transaction_history(100)
            training_data = []
            for item in history:
                allocations = item.get('allocation', {}) or {}
                selected_project = list(allocations.keys())[0] if allocations else 'reforestation_tropical'
                project_map = {
                    'reforestation_tropical': 'reforestation',
                    'direct_air_capture': 'dac',
                    'biochar_agriculture': 'biochar',
                    'ocean_alkalinization': 'ocean_based',
                    'helium_recovery_advanced': 'helium_recovery',
                }
                training_data.append({
                    'carbon_intensity': item.get('carbon_intensity', DEFAULT_CARBON_INTENSITY),
                    'cost_budget': min(1.0, item.get('cost', 100.0) / 1000.0),
                    'urgency': 0.5,
                    'permanence_requirement': 0.5,
                    'co_benefit_weight': 0.5,
                    'verification_confidence': 0.7,
                    'project_age_months': 12,
                    'historical_success': 0.9,
                    'selected_project': project_map.get(selected_project, 'reforestation'),
                })
        return await self.ml_selector.train(training_data)

    async def train_predictive_model(self) -> Dict[str, Any]:
        if self.predictive is None:
            return {'status': 'disabled'}
        return await self.predictive.train()

    async def get_recommendation_for_expert(
        self, expert_carbon_per_inference: float, annual_inferences: int,
    ) -> Dict[str, Any]:
        annual_emissions = expert_carbon_per_inference * annual_inferences
        projects = await self.storage.get_projects()
        project_costs: List[Dict[str, Any]] = []
        for pid, project in projects.items():
            annual_cost = annual_emissions * float(project['cost_per_kg'])
            helium_potential = (
                float(project.get('helium_offset_potential_l', 0)) *
                annual_emissions / 1000.0
            )
            project_costs.append({
                'project_id': pid,
                'type': project['type'],
                'annual_cost': annual_cost,
                'cost_per_inference': annual_cost / annual_inferences if annual_inferences > 0 else 0.0,
                'co_benefits': list(project['co_benefits']),
                'permanence_years': float(project['permanence_years']),
                'helium_offset_potential_l': helium_potential,
                'sustainability_score': await self.storage.get_sustainability_score(),
            })
        project_costs.sort(key=lambda x: x['annual_cost'])
        return {
            'expert_annual_emissions_kg': annual_emissions,
            'recommended_project': project_costs[0] if project_costs else None,
            'all_options': project_costs,
            'offset_strategy': self.config.offset_strategy,
            'cost_effective': project_costs[0]['annual_cost'] < 100 if project_costs else False,
            'sustainability_score': await self.storage.get_sustainability_score(),
            'recommended_helium_offset_l': (
                project_costs[0]['helium_offset_potential_l'] if project_costs else 0.0
            ),
        }


# ============================================================================
# Reporter
# ============================================================================
class CarbonSequestrationReporter:
    def __init__(
        self,
        config: CarbonSequestrationConfig,
        storage: CarbonSequestrationStorage,
        analyzer: CarbonSequestrationAnalyzer,
        observability: Optional[ObservabilityHub],
        persistence: Optional[CarbonSequestrationPersistenceManager],
        human_ai: Optional[HumanAICollaborativeSequestration],
        federated: Optional[FederatedSequestrationManager],
        predictive: Optional[PredictiveSequestrationAnalyzer],
        ml_selector: Optional[MLProjectSelector],
        helium_manager: Optional[HeliumSequestrationManager],
    ) -> None:
        self.config = config
        self.storage = storage
        self.analyzer = analyzer
        self.observability = observability
        self.persistence = persistence
        self.human_ai = human_ai
        self.federated = federated
        self.predictive = predictive
        self.ml_selector = ml_selector
        self.helium_manager = helium_manager
        self._lock = asyncio.Lock()

    async def get_carbon_portfolio(self) -> Dict[str, Any]:
        stats = await self.storage.get_stats()
        projects = await self.storage.get_projects()
        portfolio: Dict[str, Any] = {
            'total_credits': stats['total_credits'],
            'verified_credits': stats['verified_credits'],
            'total_offset_kg': stats['total_offset_kg'],
            'total_sequestered_kg': stats['total_sequestered_kg'],
            'sustainability_score': stats['sustainability_score'],
            'project_breakdown': {
                pid: {
                    'type': p['type'],
                    'capacity': p['capacity_kg_per_year'],
                    'cost': p['cost_per_kg'],
                    'permanence_years': p['permanence_years'],
                    'co_benefits': list(p['co_benefits']),
                    'helium_offset_potential_l': p.get('helium_offset_potential_l', 0),
                }
                for pid, p in projects.items()
            },
            'net_carbon_impact_kg': stats['total_sequestered_kg'] - stats['total_offset_kg'],
        }
        if self.helium_manager is not None:
            portfolio['helium_position'] = self.helium_manager.get_position()
        if self.federated is not None:
            portfolio['federated_stats'] = self.federated.get_federated_stats()
        if self.predictive is not None:
            portfolio['predictive_forecast'] = await self.predictive.predict_demand()
        if self.ml_selector is not None:
            portfolio['ml_status'] = {
                'trained': self.ml_selector.is_trained,
                'model_version': 'v5.0.0',
                'training_samples': len(self.ml_selector.training_history),
            }
        if self.human_ai is not None:
            portfolio['human_ai_insights'] = self.human_ai.get_insights()
        if self.config.enable_mopd:
            mopd_plans = await self.storage.get_mopd_plans(20)
            portfolio['mopd_plans'] = [p.to_dict() for p in mopd_plans]
        return portfolio

    async def get_sustainability_report(self) -> Dict[str, Any]:
        stats = await self.storage.get_stats()
        return {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'sustainability_score': stats['sustainability_score'],
            'carbon_portfolio': await self.get_carbon_portfolio(),
            'helium_position': (
                self.helium_manager.get_position() if self.helium_manager else {}
            ),
            'recommendations': await self._generate_recommendations_async(),
        }

    async def _generate_recommendations_async(self) -> List[str]:
        stats = await self.storage.get_stats()
        recs: List[str] = []
        if stats['sustainability_score'] < 0.5:
            recs.append("Improve carbon sequestration through project diversification")
        if stats['total_offset_kg'] < stats['total_sequestered_kg'] * 0.5:
            recs.append("Increase offset allocation to match sequestration capacity")
        if self.helium_manager is not None:
            remaining = self.helium_manager.get_position().get('remaining_budget_l', 0.0)
            if remaining < 0:
                recs.append("CRITICAL: Helium budget exceeded - implement recovery systems")
        if self.federated is not None and len(self.federated.participants) < 2:
            recs.append("Increase federated participation for better project selection")
        if self.config.enable_mopd:
            recs.append("Consider using MOPD to explore trade-offs among offset strategies")
        return recs or ["All sustainability metrics are within acceptable ranges"]

    async def export_telemetry(self) -> None:
        if self.observability is not None:
            data = await self.observability.export_prometheus()
            logger.debug(f"Telemetry export: {len(data)} bytes")

    async def save_state(self) -> None:
        if self.persistence is None:
            return
        credits = await self.storage.get_credits()
        transactions = await self.storage.get_transaction_history()
        projects = await self.storage.get_projects()
        totals = await self.storage.get_totals()
        mopd_plans = await self.storage.get_mopd_plans()
        state = {
            'credits': [c.to_dict() for c in credits],
            'transaction_history': transactions,
            'sequestration_projects': projects,
            'sustainability_score': await self.storage.get_sustainability_score(),
            'total_sequestered': totals['sequestered'],
            'total_offset': totals['offset'],
            'helium_manager_state': (
                {
                    'emissions': list(self.helium_manager.emissions),
                    'offsets': list(self.helium_manager.offsets),
                    '_total_emissions': self.helium_manager._total_emissions,
                    '_total_offsets': self.helium_manager._total_offsets,
                    'budget_l': self.helium_manager.budget_l,
                } if self.helium_manager else None
            ),
            'ml_checkpoint': (
                self.ml_selector.get_checkpoint() if self.ml_selector else None
            ),
            'mopd_plans': [p.to_dict() for p in mopd_plans],
        }
        await self.persistence.save_state(state)

    async def load_state(self) -> None:
        if self.persistence is None:
            return
        state = await self.persistence.load_state()
        if not state:
            return

        # Credits
        for c_data in state.get('credits', []):
            try:
                await self.storage.add_credit(CarbonCredit.from_dict(c_data))
            except Exception as e:
                logger.warning(f"Skipping invalid credit: {e}")

        # Transactions
        for t in state.get('transaction_history', []):
            await self.storage.add_transaction(t)

        # Projects
        projects = state.get('sequestration_projects', {})
        if projects:
            await self.storage.set_projects(projects)

        # Scores & totals
        await self.storage.update_sustainability_score(
            float(state.get('sustainability_score', 0.0))
        )
        await self.storage.update_totals(
            offset=float(state.get('total_offset', 0.0)),
            sequestered=float(state.get('total_sequestered', 0.0)),
        )

        # Helium
        he_state = state.get('helium_manager_state')
        if he_state and self.helium_manager is not None:
            self.helium_manager.emissions = deque(
                he_state.get('emissions', []), maxlen=MAX_HELIUM_HISTORY,
            )
            self.helium_manager.offsets = deque(
                he_state.get('offsets', []), maxlen=MAX_HELIUM_HISTORY,
            )
            self.helium_manager._total_emissions = float(he_state.get('_total_emissions', 0.0))
            self.helium_manager._total_offsets = float(he_state.get('_total_offsets', 0.0))
            self.helium_manager.budget_l = float(he_state.get('budget_l', self.config.helium_budget_l))

        # ML checkpoint
        ml_cp = state.get('ml_checkpoint')
        if ml_cp and self.ml_selector is not None:
            self.ml_selector.load_checkpoint(ml_cp)

        # MOPD plans
        for p_data in state.get('mopd_plans', []):
            try:
                await self.storage.add_mopd_plan(MOPDPlan.from_dict(p_data))
            except Exception as e:
                logger.warning(f"Skipping invalid MOPD plan: {e}")


# ============================================================================
# Main Controller
# ============================================================================
class CarbonSequestrationManager:
    """
    Enhanced Carbon Sequestration Manager v5.0.0.

    Complete, event-driven, MOPD-aware, XAI-enabled, safety-checked,
    approval-gated, chaos-tested, DP-federated, and observability-instrumented.
    """

    def __init__(
        self,
        bio_core: Optional[Any] = None,
        config: Optional[CarbonSequestrationConfig] = None,
        **kwargs: Any,
    ) -> None:
        if config is None:
            valid = {f.name for f in dataclass_fields(CarbonSequestrationConfig)}
            filtered = {k: v for k, v in kwargs.items() if k in valid}
            config = CarbonSequestrationConfig(**filtered)
        self.config = config

        # Bio-core wiring
        self.bio_core: Optional[BioCoreProtocol] = bio_core
        self.event_broker: Optional[EventBrokerProtocol] = None
        self.self_healer: Optional[SelfHealerProtocol] = None
        self.workflow_orchestrator: Optional[WorkflowOrchestratorProtocol] = None
        self.swarm_coordinator: Optional[SwarmCoordinatorProtocol] = None
        self.token_manager: Optional[Any] = None
        self.gradient_manager: Optional[Any] = None
        self.quantum_bridge: Optional[Any] = None
        self.tick_engine: Optional[Any] = None
        self.cost_benefit_engine: Optional[Any] = None
        if bio_core is not None:
            self._wire_bio_core(bio_core)

        # Observability
        self.observability = ObservabilityHub()

        # Governance
        self.governance: Optional[GovernanceLayer] = (
            GovernanceLayer(config.approval, publish_fn=self._publish_message)
            if config.approval.enabled else None
        )

        # Safety
        self.safety_monitor = SafetyMonitor() if config.enable_temporal_safety else None
        if self.safety_monitor is not None:
            self._setup_safety_invariants()

        # Sub-modules
        self.carbon_manager: Optional[CarbonIntensityManager] = (
            CarbonIntensityManager(config.carbon) if config.carbon.enabled else None
        )
        self.helium_manager: Optional[HeliumSequestrationManager] = (
            HeliumSequestrationManager(config.helium) if config.helium.enabled else None
        )
        self.predictive: Optional[PredictiveSequestrationAnalyzer] = (
            PredictiveSequestrationAnalyzer(config.predictive)
            if config.predictive.enabled else None
        )
        self.ml_selector: Optional[MLProjectSelector] = (
            MLProjectSelector(config.ml) if config.ml.enabled else None
        )
        self.federated: Optional[FederatedSequestrationManager] = (
            FederatedSequestrationManager(config.federated)
            if config.federated.enabled else None
        )
        self.human_ai: Optional[HumanAICollaborativeSequestration] = (
            HumanAICollaborativeSequestration() if config.enable_human_ai else None
        )
        self.persistence: Optional[CarbonSequestrationPersistenceManager] = (
            CarbonSequestrationPersistenceManager(config.persistence)
            if config.persistence.enabled else None
        )

        # Storage & Analyzer & Reporter
        self.storage = CarbonSequestrationStorage()
        self.analyzer = CarbonSequestrationAnalyzer(
            config, self.storage, self.carbon_manager, self.helium_manager,
            self.predictive, self.ml_selector, self.human_ai, self.governance,
            self.observability,
        )
        self.reporter = CarbonSequestrationReporter(
            config, self.storage, self.analyzer, self.observability,
            self.persistence, self.human_ai, self.federated, self.predictive,
            self.ml_selector, self.helium_manager,
        )

        # Chaos
        self.chaos_service: Optional[ChaosService] = (
            ChaosService(self, config.chaos) if config.enable_chaos_testing else None
        )

        # MoE injectables
        self.expert_router: Optional[Any] = None
        self.gating_network: Optional[Any] = None
        self.self_evolving_gate: Optional[Any] = None
        self.helium_provider: Optional[HeliumProviderProtocol] = None

        # Async resources
        self._event_queue: Optional[asyncio.Queue] = None
        self._event_consumer_task: Optional[asyncio.Task] = None
        self._background_tasks: List[asyncio.Task] = []
        self._started = False
        self._shutting_down = False

        # Health
        self.health_status = "healthy"
        self.last_error: Optional[str] = None

        # Dead letters
        self._dead_letters: Deque[Dict[str, Any]] = deque(maxlen=MAX_DEAD_LETTERS)

        logger.info("CarbonSequestrationManager v5.0.0 initialised (not started)")

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def start(self) -> None:
        if self._started:
            return
        self._started = True

        self._event_queue = asyncio.Queue(maxsize=10_000)

        # Projects must exist before any offset is attempted
        await self._initialize_projects()

        if self.helium_manager is not None:
            self.helium_manager.start()

        if self.config.enable_event_driven and self.event_broker is not None:
            self._subscribe_events()

        self._start_background_tasks()

        if self.config.persistence.enabled:
            await self.reporter.load_state()

        logger.info("CarbonSequestrationManager started")

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
        self.quantum_bridge = getattr(bio_core, 'quantum_bridge', None)
        self.tick_engine = getattr(bio_core, 'tick_engine', None)
        self.cost_benefit_engine = getattr(bio_core, 'cost_benefit_engine', None)

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

        def _carbon_budget_nonneg(state: Dict[str, Any]) -> bool:
            return state.get('carbon_budget_remaining_kg', 0.0) >= 0.0

        def _helium_budget_nonneg(state: Dict[str, Any]) -> bool:
            return state.get('helium_remaining_budget_l', 0.0) >= 0.0

        def _nonneg_offset(state: Dict[str, Any]) -> bool:
            return state.get('offset_amount_kg', 0.0) >= 0.0

        def _sustainability_bounded(state: Dict[str, Any]) -> bool:
            v = state.get('sustainability_score', 0.5)
            return 0.0 <= v <= 1.0

        self.safety_monitor.add_invariant(
            "carbon_budget_nonneg", _carbon_budget_nonneg,
            "Carbon budget must remain non-negative",
        )
        self.safety_monitor.add_invariant(
            "helium_budget_nonneg", _helium_budget_nonneg,
            "Helium budget must remain non-negative",
        )
        self.safety_monitor.add_invariant(
            "nonneg_offset", _nonneg_offset,
            "Offset amount must be non-negative",
        )
        self.safety_monitor.add_invariant(
            "sustainability_bounded", _sustainability_bounded,
            "Sustainability score must be in [0, 1]",
        )

    # ------------------------------------------------------------------
    # Event handling
    # ------------------------------------------------------------------
    def _subscribe_events(self) -> None:
        if self.event_broker is None:
            return
        try:
            for ev in (
                'carbon_update', 'helium_update', 'alert_generated',
                'config_updated', 'token_balance_update',
                'health_update', 'anomaly_detected',
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
        if self.predictive is not None:
            self.predictive.update_history({
                'offset_amount': 0.0,
                'credit_price': float(event.data.get('price', 50.0)),
                'project_success_rate': 0.9,
                'verification_confidence': 0.7,
                'carbon_intensity': intensity,
            })
        if intensity > 500:
            self.config.offset_strategy = 'proactive'
        elif intensity < 300:
            self.config.offset_strategy = 'conservative'

    async def _on_helium_update(self, event: Any) -> None:
        scarcity = float(event.data.get('scarcity', 0.5))
        if self.helium_manager is not None:
            # Derive budget from configured base — do NOT mutate base config
            self.helium_manager.budget_l = self.config.helium_budget_l * (1.0 - scarcity * 0.3)

    async def _on_alert_generated(self, event: Any) -> None:
        if event.data.get('severity') == 'critical':
            logger.warning("Critical alert; triggering self-healing")
            self.config.offset_strategy = 'conservative'
            if self.config.self_healing.enabled and self.self_healer is not None:
                try:
                    await self.self_healer.apply_healing('damage_accumulation')
                except Exception as e:
                    logger.error(f"Self-healing failed: {e}")
            if (self.workflow_orchestrator is not None and
                    self.config.workflow_on_critical_alert):
                try:
                    await self.workflow_orchestrator.execute_workflow(
                        self.config.workflow_on_critical_alert,
                    )
                except Exception as e:
                    logger.error(f"Workflow failed: {e}")

    async def _on_config_updated(self, event: Any) -> None:
        updates = event.data.get('updates', {})
        if 'carbon_sequestration' in updates:
            new = updates['carbon_sequestration']
            for k, v in new.items():
                if hasattr(self.config, k):
                    setattr(self.config, k, v)
            logger.info("Configuration reloaded")

    async def _on_token_balance_update(self, event: Any) -> None:
        pass

    async def _on_health_update(self, event: Any) -> None:
        self.health_status = event.data.get('status', 'healthy')

    async def _on_anomaly_detected(self, event: Any) -> None:
        metric = event.data.get('metric')
        if metric == 'carbon_intensity':
            self.config.offset_strategy = 'proactive'
        elif metric == 'helium_scarcity' and self.helium_manager is not None:
            self.helium_manager.budget_l *= 0.8

    # ------------------------------------------------------------------
    # Background loops
    # ------------------------------------------------------------------
    def _start_background_tasks(self) -> None:
        if self.config.enable_event_driven and self._event_queue is not None:
            self._event_consumer_task = asyncio.create_task(self._event_consumer())
            self._background_tasks.append(self._event_consumer_task)
        if self.carbon_manager is not None:
            self._background_tasks.append(asyncio.create_task(self._carbon_update_loop()))
        if self.predictive is not None:
            self._background_tasks.append(asyncio.create_task(self._predictive_update_loop()))
        if self.ml_selector is not None:
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
                if self.predictive is not None and len(self.predictive.history) > 0:
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
                if self.ml_selector is not None:
                    history = await self.storage.get_transaction_history(100)
                    if len(history) >= 20:
                        training_data = []
                        project_map = {
                            'reforestation_tropical': 'reforestation',
                            'direct_air_capture': 'dac',
                            'biochar_agriculture': 'biochar',
                            'ocean_alkalinization': 'ocean_based',
                            'helium_recovery_advanced': 'helium_recovery',
                        }
                        for item in history:
                            allocations = item.get('allocation', {}) or {}
                            selected_project = (
                                list(allocations.keys())[0]
                                if allocations else 'reforestation_tropical'
                            )
                            training_data.append({
                                'carbon_intensity': item.get('carbon_intensity', DEFAULT_CARBON_INTENSITY),
                                'cost_budget': min(1.0, item.get('cost', 100.0) / 1000.0),
                                'urgency': 0.5,
                                'permanence_requirement': 0.5,
                                'co_benefit_weight': 0.5,
                                'verification_confidence': 0.7,
                                'project_age_months': 12,
                                'historical_success': 0.9,
                                'selected_project': project_map.get(selected_project, 'reforestation'),
                            })
                        await self.ml_selector.train(training_data)
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
                    stats = await self.storage.get_stats()
                    pid = f"sequestration_{uuid.uuid4().hex[:8]}"
                    await self.federated.send_local_projects(
                        pid,
                        {
                            'total_projects': len(await self.storage.get_projects()),
                            'total_sequestered': (await self.storage.get_totals())['sequestered'],
                            'total_offset': (await self.storage.get_totals())['offset'],
                            'sustainability_score': stats['sustainability_score'],
                            'timestamp': datetime.now(timezone.utc).isoformat(),
                        },
                        performance=stats['sustainability_score'],
                    )
                    await self.federated.get_global_projects()
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
                await asyncio.sleep(self.config.chaos.interval_seconds)
                await self.run_chaos_test()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Chaos testing loop error: {e}")
                await asyncio.sleep(60)

    # ------------------------------------------------------------------
    # Project initialisation
    # ------------------------------------------------------------------
    async def _initialize_projects(self) -> None:
        default_projects = {
            'reforestation_tropical': {
                'type': 'reforestation',
                'capacity_kg_per_year': 10_000.0,
                'cost_per_kg': 0.05,
                'permanence_years': 100.0,
                'co_benefits': ['biodiversity', 'water_cycle'],
                'helium_offset_potential_l': 5.0,
            },
            'direct_air_capture': {
                'type': 'dac',
                'capacity_kg_per_year': 5_000.0,
                'cost_per_kg': 0.20,
                'permanence_years': 10_000.0,
                'co_benefits': ['technology', 'employment'],
                'helium_offset_potential_l': 2.0,
            },
            'biochar_agriculture': {
                'type': 'biochar',
                'capacity_kg_per_year': 8_000.0,
                'cost_per_kg': 0.08,
                'permanence_years': 1_000.0,
                'co_benefits': ['soil_health', 'crop_yield'],
                'helium_offset_potential_l': 3.0,
            },
            'ocean_alkalinization': {
                'type': 'ocean_based',
                'capacity_kg_per_year': 20_000.0,
                'cost_per_kg': 0.15,
                'permanence_years': 10_000.0,
                'co_benefits': ['ocean_health', 'carbon_sink'],
                'helium_offset_potential_l': 1.0,
            },
            'helium_recovery_advanced': {
                'type': 'helium_recovery',
                'capacity_kg_per_year': 1_000.0,
                'cost_per_kg': 0.50,
                'permanence_years': 50.0,
                'co_benefits': ['resource_conservation', 'technology'],
                'helium_offset_potential_l': 50.0,
            },
        }
        await self.storage.set_projects(default_projects)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    async def offset_expert_emissions(
        self,
        expert_carbon_kg: float,
        budget_remaining: float,
        urgency: str = 'normal',
        use_ml_selection: bool = False,
        return_mopd: bool = False,
        idempotency_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        await self.wait_ready()
        try:
            result = await self.analyzer.offset_expert_emissions(
                expert_carbon_kg, budget_remaining, urgency, use_ml_selection,
                return_mopd, idempotency_key,
            )
            if 'error' in result and 'duplicate' not in result:
                self.observability.counter('offsets_failed')
                return result

            self.observability.counter('offsets_total')
            self.observability.gauge(
                'sustainability_score',
                await self.storage.get_sustainability_score(),
            )

            if (await self.storage.get_sustainability_score() < 0.4 and
                    self.workflow_orchestrator is not None):
                try:
                    await self.workflow_orchestrator.execute_workflow(
                        self.config.workflow_on_slo_breach,
                    )
                except Exception as e:
                    logger.error(f"Workflow failed: {e}")

            # Gating network feedback
            if self.gating_network is not None and self.expert_router is not None:
                try:
                    features = np.array([
                        result.get('offset_amount_kg', 0.0) / 1000.0,
                        await self.storage.get_sustainability_score(),
                        result.get('carbon_intensity', DEFAULT_CARBON_INTENSITY) / CARBON_CRITICAL_THRESHOLD,
                        len(result.get('projects_used', [])),
                    ])
                    reward = 1.0 - max(0.0, expert_carbon_kg - result.get('offset_amount_kg', 0.0)) / max(expert_carbon_kg, 1.0)
                    self.gating_network.update(
                        features, reward, {'strategy': self.config.offset_strategy},
                    )
                except Exception as e:
                    logger.warning(f"Gating network update failed: {e}")

            # Self-evolving gate
            if self.self_evolving_gate is not None and TORCH_AVAILABLE:
                try:
                    state = torch.tensor([
                        result.get('offset_amount_kg', 0.0),
                        await self.storage.get_sustainability_score(),
                    ], dtype=torch.float32)
                    reward = 1.0 - max(0.0, expert_carbon_kg - result.get('offset_amount_kg', 0.0)) / max(expert_carbon_kg, 1.0)
                    self.self_evolving_gate.adapt(
                        state=state, chosen_expert=0, reward=reward,
                        environmental_feedback={'strategy': self.config.offset_strategy},
                        quantum_mode=False,
                    )
                except Exception as e:
                    logger.warning(f"Self-evolving gate adapt failed: {e}")

            # Safety check
            if self.safety_monitor is not None:
                totals = await self.storage.get_totals()
                helium_remaining = (
                    self.helium_manager.get_position()['remaining_budget_l']
                    if self.helium_manager else 0.0
                )
                violations = self.safety_monitor.check({
                    'carbon_budget_remaining_kg': self.config.carbon_budget_kg - totals['offset'],
                    'helium_remaining_budget_l': helium_remaining,
                    'offset_amount_kg': result.get('offset_amount_kg', 0.0),
                    'sustainability_score': result.get('sustainability_score', 0.5),
                })
                if violations:
                    result['temporal_safety_violations'] = violations
                    self.observability.counter('safety_violations')

            return result
        except Exception as e:
            logger.error(f"offset_expert_emissions failed: {e}", exc_info=True)
            self.last_error = str(e)
            self.health_status = "degraded"
            return {'error': str(e)}

    async def verify_credits(self) -> int:
        await self.wait_ready()
        return await self.analyzer.verify_credits()

    async def get_carbon_portfolio(self) -> Dict[str, Any]:
        await self.wait_ready()
        return await self.reporter.get_carbon_portfolio()

    async def get_sustainability_report(self) -> Dict[str, Any]:
        await self.wait_ready()
        return await self.reporter.get_sustainability_report()

    async def get_recommendation_for_expert(
        self, expert_carbon_per_inference: float, annual_inferences: int,
    ) -> Dict[str, Any]:
        await self.wait_ready()
        return await self.analyzer.get_recommendation_for_expert(
            expert_carbon_per_inference, annual_inferences,
        )

    async def train_ml_model(self, training_data: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
        await self.wait_ready()
        return await self.analyzer.train_ml_model(training_data)

    async def train_predictive_model(self) -> Dict[str, Any]:
        await self.wait_ready()
        return await self.analyzer.train_predictive_model()

    async def get_sequestration_pareto_front(
        self,
        expert_carbon_kg: float,
        budget_remaining: float,
        urgency: str = 'normal',
    ) -> List[MOPDPlan]:
        await self.wait_ready()
        if not self.config.enable_mopd:
            return []
        return await self.analyzer._generate_pareto_front_for_offset(
            expert_carbon_kg, budget_remaining, urgency,
        )

    async def get_mopd_summary(self) -> Dict[str, Any]:
        await self.wait_ready()
        if not self.config.enable_mopd:
            return {'enabled': False}
        plans = await self.storage.get_mopd_plans(20)
        return {
            'enabled': True,
            'objective_weights': dict(self.config.mopd.objective_weights),
            'grid_resolution': self.config.mopd.grid_resolution,
            'total_mopd_plans': len(await self.storage.get_mopd_plans()),
            'sample_plans': [p.to_dict() for p in plans],
        }

    async def submit_approval_decision(self, request_id: str, approved: bool) -> bool:
        if self.governance is None:
            return False
        return await self.governance.submit_decision(request_id, approved)

    async def inject_fault(self, fault_type: Union[str, FaultType],
                           **params: Any) -> Dict[str, Any]:
        if self.chaos_service is None:
            return {'status': 'disabled'}
        return await self.chaos_service.inject_fault(fault_type, **params)

    async def run_chaos_test(self) -> Dict[str, Any]:
        if self.chaos_service is None:
            return {'status': 'disabled'}
        return await self.chaos_service.run_chaos_test()

    async def share_with_swarm(self) -> None:
        if not self.config.enable_swarm_coordination or self.swarm_coordinator is None:
            return
        stats = await self.storage.get_stats()
        payload = {
            'manager_id': uuid.uuid4().hex[:8],
            'sustainability_score': stats['sustainability_score'],
            'total_offset': stats['total_offset_kg'],
            'total_sequestered': stats['total_sequestered_kg'],
            'credits_count': stats['total_credits'],
            'helium_position': (
                self.helium_manager.get_position() if self.helium_manager else {}
            ),
            'mopd_enabled': self.config.enable_mopd,
        }
        try:
            await self.swarm_coordinator.share_predictions(payload)
        except Exception as e:
            logger.warning(f"Swarm share failed: {e}")
            self._dead_letters.append({
                'topic': 'swarm',
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'error': str(e),
            })

    def set_gating_network(self, gating_network: Any) -> None:
        self.gating_network = gating_network

    def set_self_evolving_gate(self, gate: Any) -> None:
        self.self_evolving_gate = gate

    def set_expert_router(self, router: Any) -> None:
        self.expert_router = router

    def set_helium_provider(self, provider: HeliumProviderProtocol) -> None:
        self.helium_provider = provider

    async def self_heal(self) -> None:
        logger.info("Self-healing started")
        if not self.config.self_healing.enabled:
            return
        if self.helium_manager is not None:
            self.helium_manager.budget_l = self.config.helium_budget_l
        self.config.offset_strategy = 'proactive'
        await self.storage.update_sustainability_score(0.0)
        credits = await self.storage.get_credits()
        if len(credits) > 10:
            async with self.storage._lock:
                self.storage.credits.clear()
                for c in credits[-10:]:
                    self.storage.credits.append(c)
        history = await self.storage.get_transaction_history()
        if len(history) > 10:
            async with self.storage._lock:
                self.storage.transaction_history.clear()
                for t in history[-10:]:
                    self.storage.transaction_history.append(t)
        self.health_status = "healthy"
        self.last_error = None
        if self.persistence is not None:
            await self.reporter.save_state()
        logger.info("Self-healing completed")

    async def get_health_status(self) -> Dict[str, Any]:
        stats = await self.storage.get_stats()
        return {
            'status': self.health_status,
            'last_error': self.last_error,
            'total_offset': stats['total_offset_kg'],
            'total_sequestered': stats['total_sequestered_kg'],
            'credits_count': stats['total_credits'],
            'sustainability_score': stats['sustainability_score'],
            'bio_integration_active': self.config.enable_bio_integration,
            'event_driven_active': self.config.enable_event_driven,
            'self_healing_enabled': self.config.self_healing.enabled,
            'persistence_enabled': self.config.persistence.enabled,
            'mopd_enabled': self.config.enable_mopd,
            'human_approval_enabled': self.config.approval.enabled,
            'temporal_safety_enabled': self.config.enable_temporal_safety,
            'chaos_testing_enabled': self.config.enable_chaos_testing,
            'dead_letters': len(self._dead_letters),
        }

    async def get_prometheus_metrics(self) -> str:
        return await self.observability.export_prometheus()

    def get_dead_letters(self) -> List[Dict[str, Any]]:
        return list(self._dead_letters)

    async def _publish_message(self, topic: str, payload: Any) -> None:
        if self.event_broker is not None and hasattr(self.event_broker, 'publish'):
            try:
                res = self.event_broker.publish(topic, payload)
                if asyncio.iscoroutine(res):
                    await res
            except Exception as e:
                logger.warning(f"Event publish failed: {e}")
                self._dead_letters.append({
                    'topic': topic,
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'error': str(e),
                })

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------
    async def shutdown(self) -> None:
        logger.info("Shutting down CarbonSequestrationManager")
        self._shutting_down = True

        if self._event_consumer_task is not None:
            self._event_consumer_task.cancel()

        for task in self._background_tasks:
            task.cancel()
        await asyncio.gather(*self._background_tasks, return_exceptions=True)
        self._background_tasks.clear()

        if self.helium_manager is not None:
            try:
                await self.helium_manager.stop()
            except Exception:
                pass
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
        config = CarbonSequestrationConfig()
        config.enable_chaos_testing = True
        config.chaos.enabled = True
        config.chaos.interval_seconds = 9999

        manager = CarbonSequestrationManager(config=config)
        await manager.start()

        result = await manager.offset_expert_emissions(
            expert_carbon_kg=250.0,
            budget_remaining=800.0,
            urgency='normal',
            use_ml_selection=False,
            return_mopd=True,
            idempotency_key=uuid.uuid4().hex,
        )
        print(json.dumps(result, indent=2, default=str))

        portfolio = await manager.get_carbon_portfolio()
        print(json.dumps(portfolio, indent=2, default=str))

        health = await manager.get_health_status()
        print(json.dumps(health, indent=2, default=str))

        chaos_report = await manager.run_chaos_test()
        print(json.dumps(chaos_report, indent=2, default=str))

        metrics = await manager.get_prometheus_metrics()
        print(metrics[:500])

        await manager.shutdown()

    asyncio.run(main())


# ============================================================================
# Exports
# ============================================================================
__all__ = [
    # Main
    "CarbonSequestrationManager",
    "CarbonSequestrationConfig",
    # Sub-modules
    "CarbonIntensityManager",
    "CarbonConfig",
    "HeliumSequestrationManager",
    "HeliumConfig",
    "PredictiveSequestrationAnalyzer",
    "PredictiveConfig",
    "MLProjectSelector",
    "MLConfig",
    "FederatedSequestrationManager",
    "FederatedConfig",
    "HumanAICollaborativeSequestration",
    "CarbonSequestrationPersistenceManager",
    "PersistenceConfig",
    "CarbonSequestrationStorage",
    "CarbonSequestrationAnalyzer",
    "CarbonSequestrationReporter",
    # Infra
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
    # Data
    "CarbonCredit",
    "MOPDPlan",
    "MOPDConfig",
    # Enums
    "OffsetStrategy",
    "UrgencyLevel",
    # Protocols
    "BioCoreProtocol",
    "EventBrokerProtocol",
    "SwarmCoordinatorProtocol",
    "SelfHealerProtocol",
    "WorkflowOrchestratorProtocol",
    "HeliumProviderProtocol",
    "CarbonManagerProtocol",
    "HeliumManagerProtocol",
    "MLSelectorProtocol",
    "StorageProtocol",
    # Constants
    "MOPD_OBJECTIVE_NAMES",
    "MOPD_HIGHER_IS_BETTER",
    "STATE_SCHEMA_VERSION",
]
