# File: quantum_integration/quantum-limit-graph-v2.4.0/limit-agentbench/src/enhancements/moe_expert_system/advanced/circular_computing_manager.py
# Enhanced version v5.0.0 – Complete production-grade implementation with all
# sub-modules fully implemented, Protocols, typed persistence, MOPD with
# consistent sign conventions, XAI, temporal safety, human approval, chaos
# testing with rollback, differential privacy, and a shared ObservabilityHub.

"""
Enhanced Circular Computing Module v5.0.0

Complete, event-driven, MOPD-aware, XAI-enabled, safety-checked,
approval-gated, chaos-tested, DP-federated, and observability-instrumented
circular computing manager.

ENHANCEMENTS OVER v4.2.0:
- Fully implemented every previously stubbed sub-module.
- Added missing `register_component`, `deploy_component`, `record_utilization`.
- Fixed `_check_plan_safety` inversion.
- Added `MOPDConfig.__post_init__` (warn + normalise).
- Replaced unsafe `scaler.mean_` access with `is_fitted` flags.
- `get_checkpoint` now returns JSON-safe data (no raw tensors).
- Fixed `.seconds` → `.total_seconds()` staleness bug.
- Deferred `asyncio.create_task` from `__init__` to `start()` / `wait_ready()`.
- Renamed `_on_token_update` → `_on_token_balance_update`.
- `_on_helium_update` no longer permanently mutates nested config.
- `_on_anomaly_detected` no longer compounds budget.
- Non-destructive chaos via `ChaosService` with save/restore.
- Real human approval via `GovernanceLayer` (queue topic + audit trail).
- Canonical MOPD sign convention via `objective_vector()`.
- Typed `to_dict` / `from_dict` for `HardwareComponent` and `MOPDPlan`.
- Typed persistence with `TIMESTAMP_KEYS` and schema versioning.
- Bounded deques for all histories.
- Threaded file I/O (no blocking calls in async paths).
- Idempotency keys with LRU cache.
- Shared `ObservabilityHub` with Prometheus export.
- Differential privacy (Gaussian mechanism with L2 clipping) for federated layer.
- Atomic inventory updates (single lock scope).
- Removed unused imports; comprehensive type hints.
"""

from __future__ import annotations

import asyncio
import hashlib
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
MAX_COMPONENTS = 10_000
MAX_RECYCLING_RECORDS = 10_000
MAX_MOPD_PLANS = 10_000
MAX_HELIUM_HISTORY = 86_400
MAX_APPROVAL_AUDIT = 1000
MAX_DEAD_LETTERS = 1000
IDEMPOTENCY_CACHE_SIZE = 4096
APPROVAL_DEFAULT_TIMEOUT = 60.0
CHAOS_MAX_FAULTS_PER_RUN = 5
MOPD_OBJECTIVE_NAMES: Tuple[str, ...] = (
    'cost',
    'carbon_saved_kg',
    'helium_recovered_l',
    'material_recovery_rate',
    'time_days',
)
# Canonical sign convention: every objective is internally "lower is better".
MOPD_HIGHER_IS_BETTER: FrozenSet[str] = frozenset({
    'carbon_saved_kg',
    'helium_recovered_l',
    'material_recovery_rate',
})
DEFAULT_RECOVERY_RATES: Dict[str, float] = {
    'silicon': 0.95,
    'copper': 0.98,
    'gold': 0.99,
    'aluminum': 0.95,
    'plastic': 0.80,
    'rare_earth': 0.90,
    'helium': 0.85,
}


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
        return {
            (k.value if isinstance(k, Enum) else str(k)): _to_serializable(v)
            for k, v in obj.items()
        }
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
    def register_component_helium(
        self, component_id: str, helium_content_l: float, component_type: str = ...,
    ) -> None: ...
    def calculate_recovery(self, component_id: str) -> float: ...
    def record_recovery(self, component_id: str, amount_l: float) -> None: ...


@runtime_checkable
class MLSelectorProtocol(Protocol):
    is_trained: bool
    async def select_component(self, requirements: Dict[str, Any]) -> Dict[str, Any]: ...
    async def train(self, training_data: List[Dict[str, Any]]) -> Dict[str, Any]: ...
    def get_checkpoint(self) -> Dict[str, Any]: ...
    def load_checkpoint(self, checkpoint: Dict[str, Any]) -> None: ...


# ============================================================================
# Enums
# ============================================================================
class HardwareState(str, Enum):
    MANUFACTURING = "manufacturing"
    DEPLOYED = "deployed"
    MAINTENANCE = "maintenance"
    DEGRADED = "degraded"
    REPURPOSED = "repurposed"
    RECYCLED = "recycled"
    DECOMMISSIONED = "decommissioned"
    HELIUM_RECOVERED = "helium_recovered"


class MaterialType(str, Enum):
    SILICON = "silicon"
    COPPER = "copper"
    GOLD = "gold"
    ALUMINUM = "aluminum"
    PLASTIC = "plastic"
    RARE_EARTH = "rare_earth"
    HELIUM = "helium"


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
    COMPONENT_FAILURE = "component_failure"


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
class HardwareComponent:
    component_id: str
    type: str
    materials: Dict[MaterialType, float]
    manufacturing_carbon: float
    current_state: HardwareState
    deployment_date: datetime
    expected_lifetime_days: int
    utilization_history: List[float] = field(default_factory=list)
    maintenance_log: List[Dict[str, Any]] = field(default_factory=list)
    sustainability_score: float = 0.0
    helium_content_l: float = 0.0
    carbon_savings_kg: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            'component_id': self.component_id,
            'type': self.type,
            'materials': {m.value: float(v) for m, v in self.materials.items()},
            'manufacturing_carbon': self.manufacturing_carbon,
            'current_state': self.current_state.value,
            'deployment_date': self.deployment_date.isoformat(),
            'expected_lifetime_days': self.expected_lifetime_days,
            'utilization_history': list(self.utilization_history),
            'maintenance_log': _to_serializable(self.maintenance_log),
            'sustainability_score': self.sustainability_score,
            'helium_content_l': self.helium_content_l,
            'carbon_savings_kg': self.carbon_savings_kg,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "HardwareComponent":
        d = dict(data)
        d['current_state'] = HardwareState(d['current_state'])
        if isinstance(d.get('deployment_date'), str):
            d['deployment_date'] = datetime.fromisoformat(d['deployment_date'])
        raw_materials = d.get('materials', {}) or {}
        d['materials'] = {
            MaterialType(k): float(v) for k, v in raw_materials.items()
        }
        d['utilization_history'] = list(d.get('utilization_history', []))
        d['maintenance_log'] = list(d.get('maintenance_log', []))
        return cls(**d)


@dataclass
class MOPDPlan:
    """Represents a recycling strategy with its objective vector and XAI."""
    recycling_method: str
    helium_recovery: bool
    material_recovery_target: float
    use_ml_optimization: bool
    cost: float
    carbon_saved_kg: float
    helium_recovered_l: float
    material_recovery_rate: float
    time_days: float
    sustainability_score: float
    scalarised_score: float = 0.0
    explanation: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            'recycling_method': self.recycling_method,
            'helium_recovery': self.helium_recovery,
            'material_recovery_target': self.material_recovery_target,
            'use_ml_optimization': self.use_ml_optimization,
            'cost': self.cost,
            'carbon_saved_kg': self.carbon_saved_kg,
            'helium_recovered_l': self.helium_recovered_l,
            'material_recovery_rate': self.material_recovery_rate,
            'time_days': self.time_days,
            'sustainability_score': self.sustainability_score,
            'scalarised_score': self.scalarised_score,
            'explanation': self.explanation,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MOPDPlan":
        return cls(**data)

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
        'carbon_saved_kg': 0.30,
        'helium_recovered_l': 0.20,
        'material_recovery_rate': 0.15,
        'time_days': 0.15,
    })
    grid_resolution: int = 5
    enable_cost_benefit: bool = True
    enable_predictive: bool = True
    enable_quantum: bool = True
    require_human_approval: bool = False
    aggressive_threshold: float = 0.7

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
    path: str = "circular_computing_state.json"
    save_interval_seconds: int = 300


@dataclass
class SelfHealingConfig:
    enabled: bool = True


@dataclass
class ApprovalConfig:
    enabled: bool = False
    timeout_seconds: float = APPROVAL_DEFAULT_TIMEOUT
    auto_approve_on_timeout: bool = False
    require_for_sustainability_below: float = 0.5


@dataclass
class ChaosConfig:
    enabled: bool = False
    interval_seconds: int = 3600
    probability: float = 0.05
    auto_restore: bool = True


@dataclass
class CircularComputingConfig:
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

    helium_budget_l: float = 100.0
    helium_to_co2_factor: float = 20.0

    workflow_on_critical_alert: str = "adjust_circular_strategy"
    workflow_on_slo_breach: str = "rebalance_materials"
    swarm_share_interval_seconds: int = 60

    # Declared config fields previously set ad-hoc
    carbon_recycling_priority: float = 0.5
    circularity_strategy: str = "balanced"


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
        logger.info(f"CarbonIntensityManager initialised (region={self.region})")

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
# Helium Lifecycle Manager
# ============================================================================
class HeliumLifecycleManager:
    DEFAULT_RECOVERY_RATES: Dict[str, float] = {
        'cooling_system': 0.85,
        'quantum_computer': 0.90,
        'cryogenic_system': 0.80,
        'standard_cooling': 0.75,
        'mri_system': 0.95,
    }

    def __init__(self, config: HeliumConfig):
        self.config = config
        self.budget_l = config.budget_l
        self.usage: Deque[Dict[str, Any]] = deque(maxlen=MAX_HELIUM_HISTORY)
        self.recovered: Deque[Dict[str, Any]] = deque(maxlen=MAX_HELIUM_HISTORY)
        self.component_helium: Dict[str, Dict[str, Any]] = {}
        self._total_usage = 0.0
        self._total_recovered = 0.0
        self._lock = asyncio.Lock()
        self._task: Optional[asyncio.Task] = None
        self._accounting_loop_running = False
        self.recovery_rates = dict(self.DEFAULT_RECOVERY_RATES)
        logger.info(f"HeliumLifecycleManager initialised: budget={self.budget_l}L")

    def register_component_helium(self, component_id: str, helium_content_l: float,
                                  component_type: str = 'cooling_system') -> None:
        self.component_helium[component_id] = {
            'total_l': float(helium_content_l),
            'recovered_l': 0.0,
            'used_l': 0.0,
            'type': component_type,
            'recovery_rate': self.recovery_rates.get(component_type, 0.85),
            'registered_at': datetime.now(timezone.utc),
        }
        logger.info(f"Registered helium content for {component_id}: {helium_content_l}L")

    def track_usage(self, component_id: str, usage_l: float) -> None:
        if usage_l < 0:
            return
        self.usage.append({
            'component_id': component_id,
            'amount_l': usage_l,
            'timestamp': datetime.now(timezone.utc),
        })
        self._total_usage += usage_l
        if component_id in self.component_helium:
            self.component_helium[component_id]['used_l'] += usage_l

    def calculate_recovery(self, component_id: str) -> float:
        if component_id not in self.component_helium:
            return 0.0
        comp = self.component_helium[component_id]
        total = comp['total_l']
        used = comp.get('used_l', 0.0)
        rate = comp['recovery_rate']
        remaining = max(0.0, total - used)
        return remaining * rate

    def record_recovery(self, component_id: str, amount_l: float) -> None:
        if amount_l < 0:
            return
        self.recovered.append({
            'component_id': component_id,
            'amount_l': amount_l,
            'timestamp': datetime.now(timezone.utc),
        })
        self._total_recovered += amount_l
        if component_id in self.component_helium:
            self.component_helium[component_id]['recovered_l'] += amount_l

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
            logger.warning("No running loop; HeliumLifecycleManager not started")

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
            'components': dict(self.component_helium),
        }


# ============================================================================
# Predictive Lifecycle Analyzer
# ============================================================================
class PredictiveLifecycleAnalyzer:
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
            logger.info("PredictiveLifecycleAnalyzer initialised with SGD")
        else:
            logger.warning("sklearn not available; moving-average fallback in use")

    def update_history(self, data: Dict[str, Any]) -> None:
        self.history.append({
            'timestamp': datetime.now(timezone.utc),
            'age_days': float(data.get('age_days', 0.0)),
            'utilization': float(data.get('utilization', 0.5)),
            'maintenance_count': int(data.get('maintenance_count', 0)),
            'carbon_score': float(data.get('carbon_score', 0.5)),
            'helium_remaining': float(data.get('helium_remaining', 0.5)),
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
                        data['age_days'] / 1000.0,
                        data['utilization'],
                        data['maintenance_count'] / 10.0,
                        data['carbon_score'],
                        data['helium_remaining'],
                    ])
                X.append(features)
                y.append(hist_list[i + 5]['age_days'])

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

    async def predict_lifetime(self, component_data: Dict[str, Any]) -> Dict[str, Any]:
        if not self.is_trained or len(self.history) < 10:
            if self.history:
                recent = [h['age_days'] for h in list(self.history)[-5:]]
                pred = float(np.mean(recent)) if recent else 365.0
                return {
                    'predicted_days': pred, 'confidence': 0.3,
                    'trend': 'moving_average',
                    'recommended_actions': self._generate_actions(pred),
                }
            return {
                'predicted_days': 365.0, 'confidence': 0.0,
                'trend': 'insufficient_data', 'recommended_actions': [],
            }

        recent = list(self.history)[-5:]
        features: List[float] = []
        for data in recent:
            features.extend([
                data['age_days'] / 1000.0,
                data['utilization'],
                data['maintenance_count'] / 10.0,
                data['carbon_score'],
                data['helium_remaining'],
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
            trend = "increasing" if prediction > last else "decreasing" if prediction < last else "stable"
        else:
            trend = "stable"

        self.forecasts.append({'prediction': prediction, 'trend': trend})
        return {
            'predicted_days': max(0.0, prediction),
            'confidence': confidence,
            'trend': trend,
            'recommended_actions': self._generate_actions(prediction),
        }

    def _generate_actions(self, prediction: float) -> List[str]:
        if prediction < 100:
            return ["URGENT: Schedule component replacement", "Prioritize material recovery"]
        if prediction < 365:
            return ["Plan for repurposing", "Optimize utilization"]
        if prediction < 730:
            return ["Schedule preventive maintenance", "Monitor helium levels"]
        return ["Component health is good - maintain current practices"]


# ============================================================================
# ML Component Selector
# ============================================================================
class MLComponentSelector:
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
            logger.info("MLComponentSelector initialised with PyTorch")
        else:
            logger.warning("PyTorch not available; ML component selection disabled")

    def _init_model(self) -> None:
        class ComponentSelector(nn.Module):  # type: ignore
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

        self.model = ComponentSelector(self.input_size, self.hidden_size)
        self.optimizer = optim.Adam(self.model.parameters(), lr=0.001)
        self.criterion = nn.MSELoss()

    @staticmethod
    def _features_from_item(item: Dict[str, Any]) -> List[float]:
        return [
            float(item.get('age_days', 0.0)) / 1000.0,
            float(item.get('utilization', 0.5)),
            float(item.get('maintenance_count', 0)) / 10.0,
            float(item.get('carbon_footprint', 0.5)),
            float(item.get('helium_content', 0.5)),
            float(item.get('recycling_potential', 0.5)),
            float(item.get('reliability', 0.5)),
            float(item.get('cost_efficiency', 0.5)),
        ]

    async def train(self, training_data: List[Dict[str, Any]],
                    epochs: Optional[int] = None) -> Dict[str, Any]:
        if not TORCH_AVAILABLE or self.model is None:
            return {'status': 'disabled'}
        if len(training_data) < 20:
            return {'status': 'insufficient_data', 'samples': len(training_data)}

        epochs = epochs or self.config.epochs

        X = [self._features_from_item(it) for it in training_data]
        y = [float(it.get('selection_score', 0.5)) for it in training_data]

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

    async def select_component(self, requirements: Dict[str, Any]) -> Dict[str, Any]:
        if not TORCH_AVAILABLE or not self.is_trained or self.model is None:
            return {'score': 0.5, 'confidence': 0.0, 'status': 'model_not_trained'}

        features = np.array([self._features_from_item(requirements)])
        if self.scaler is not None and self.is_fitted:
            features_scaled = self.scaler.transform(features)
        else:
            features_scaled = features

        def _predict() -> float:
            assert self.model is not None
            self.model.eval()
            with torch.no_grad():
                out = self.model(torch.FloatTensor(features_scaled))
                return float(out.cpu().numpy()[0, 0])

        score = await asyncio.to_thread(_predict)
        return {
            'score': score,
            'confidence': 0.8,
            'status': 'success',
        }

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
# Federated Circular Manager (with DP)
# ============================================================================
class FederatedCircularManager:
    def __init__(self, config: FederatedConfig):
        self.config = config
        self.server_url = config.server_url
        self.round = 0
        self.local_components: Dict[str, Any] = {}
        self.global_components: Dict[str, Any] = {}
        self.participants: List[str] = []
        self.contribution_scores: Dict[str, float] = {}
        self._lock = asyncio.Lock()
        self._session: Optional[aiohttp.ClientSession] = None
        self._circuit = CircuitBreaker("federated_server", failure_threshold=3,
                                        recovery_timeout=30.0)
        logger.info("FederatedCircularManager initialised")

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

    async def send_local_components(self, participant_id: str,
                                     component_data: Dict[str, Any],
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
                        private = self._add_differential_privacy(component_data)
                        compressed = self._compress(private)
                        update = {
                            'participant_id': participant_id,
                            'round': self.round,
                            'component_data': compressed,
                            'performance': performance,
                            'privacy_epsilon': self.config.privacy_epsilon,
                            'sparsity_ratio': self.config.sparsity_ratio,
                            'timestamp': datetime.now(timezone.utc).isoformat(),
                        }
                        async with session.post(
                            f"{self.server_url}/federated/circular",
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

    async def get_global_components(self) -> Optional[Dict[str, Any]]:
        if not self.server_url:
            return self.global_components

        async def _fetch() -> Optional[Dict[str, Any]]:
            for attempt in range(self.config.max_retries):
                try:
                    async with self._lock:
                        session = await self._get_session()
                        if session is None:
                            return None
                        async with session.get(
                            f"{self.server_url}/federated/circular/global",
                            timeout=self.config.request_timeout_seconds,
                        ) as resp:
                            if resp.status == 200:
                                data = await resp.json()
                                self.global_components = data.get('components', {})
                                self.participants = data.get('participants', [])
                                return self.global_components
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
            'local_components': len(self.local_components),
            'global_components': len(self.global_components),
        }

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()


# ============================================================================
# Human-AI Collaborative Circular
# ============================================================================
class HumanAICollaborativeCircular:
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
            'acknowledgment': f"Feedback on {feedback.get('topic', 'circular computing')}",
            'insights': [],
            'actions': [],
            'circular_insights': [],
        }
        concern = feedback.get('concern')
        if concern == 'recycling':
            reflection['insights'].append("Recycling efficiency can be improved through material sorting")
            reflection['actions'].append("Implement automated material recovery")
        elif concern == 'helium':
            reflection['insights'].append("Helium recovery requires specialized handling")
            reflection['actions'].append("Implement helium capture systems")
        elif concern == 'lifecycle':
            reflection['insights'].append("Lifecycle extension reduces carbon footprint")
            reflection['actions'].append("Implement predictive maintenance")
        elif concern == 'carbon':
            reflection['circular_insights'].append("Carbon-aware hardware selection is critical")
            reflection['actions'].append("Integrate carbon intensity tracking")
        if 'suggestion' in feedback:
            reflection['actions'].append(f"Implementing suggestion: {feedback['suggestion']}")
        reflection['action_items'] = self._prioritize_actions(reflection['actions'])
        return reflection

    def _prioritize_actions(self, actions: List[str]) -> List[Dict[str, Any]]:
        priorities: List[Dict[str, Any]] = []
        for action in actions:
            if any(kw in action.lower() for kw in ('urgent', 'critical')):
                priority, impact = 'high', 0.9
            elif any(kw in action.lower() for kw in ('recycling', 'circular')):
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
class CircularComputingPersistenceManager:
    TIMESTAMP_KEYS: FrozenSet[str] = frozenset({
        'timestamp', 'deployment_date', 'verification_date', 'expiry_date',
        'requested_at', 'decided_at', 'registered_at', 'last_assessment',
    })

    def __init__(self, config: PersistenceConfig):
        self.config = config
        self.path = config.path
        self._lock = asyncio.Lock()
        self._version = STATE_SCHEMA_VERSION
        logger.info(f"CircularComputingPersistenceManager initialised (path={self.path})")

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
class CircularStorage:
    def __init__(self):
        self.components: Dict[str, HardwareComponent] = {}
        self.material_inventory: Dict[MaterialType, float] = defaultdict(float)
        self.recycling_history: Deque[Dict[str, Any]] = deque(maxlen=MAX_RECYCLING_RECORDS)
        self.circularity_score = 0.0
        self.waste_diversion_rate = 0.0
        self.material_recovery_rate = 0.0
        self.sustainability_score = 0.0
        self.mopd_plans: Deque[MOPDPlan] = deque(maxlen=MAX_MOPD_PLANS)
        self._lock = asyncio.Lock()

    async def add_component(self, component: HardwareComponent) -> None:
        if not isinstance(component, HardwareComponent):
            logger.warning(f"add_component received non-HardwareComponent: {type(component)}")
            return
        async with self._lock:
            if len(self.components) >= MAX_COMPONENTS:
                oldest = next(iter(self.components))
                del self.components[oldest]
            self.components[component.component_id] = component
            for mat, amount in component.materials.items():
                self.material_inventory[mat] += amount

    async def get_component(self, component_id: str) -> Optional[HardwareComponent]:
        async with self._lock:
            return self.components.get(component_id)

    async def get_components(self, state: Optional[HardwareState] = None) -> List[HardwareComponent]:
        async with self._lock:
            if state is None:
                return list(self.components.values())
            return [c for c in self.components.values() if c.current_state == state]

    async def update_component_state(self, component_id: str, new_state: HardwareState) -> None:
        async with self._lock:
            if component_id in self.components:
                self.components[component_id].current_state = new_state

    async def add_recycling_record(self, record: Dict[str, Any]) -> None:
        async with self._lock:
            self.recycling_history.append(_to_serializable(record))

    async def add_mopd_plan(self, plan: MOPDPlan) -> None:
        async with self._lock:
            self.mopd_plans.append(plan)

    async def update_metrics(self, circularity: float, waste: float, recovery: float) -> None:
        async with self._lock:
            self.circularity_score = float(circularity)
            self.waste_diversion_rate = float(waste)
            self.material_recovery_rate = float(recovery)

    async def update_sustainability_score(self, score: float) -> None:
        async with self._lock:
            self.sustainability_score = float(score)

    async def get_metrics(self) -> Dict[str, float]:
        async with self._lock:
            return {
                'circularity_score': self.circularity_score,
                'waste_diversion_rate': self.waste_diversion_rate,
                'material_recovery_rate': self.material_recovery_rate,
                'sustainability_score': self.sustainability_score,
            }

    async def get_stats(self) -> Dict[str, Any]:
        async with self._lock:
            total = len(self.components)
            states = {s.value: 0 for s in HardwareState}
            for c in self.components.values():
                states[c.current_state.value] += 1
            total_recycled = (
                states.get(HardwareState.RECYCLED.value, 0) +
                states.get(HardwareState.REPURPOSED.value, 0)
            )
            return {
                'total_components': total,
                'components_by_state': states,
                'recycled_or_repurposed': total_recycled,
                'material_inventory': {m.value: v for m, v in self.material_inventory.items()},
                'sustainability_score': self.sustainability_score,
            }

    async def get_recycling_history(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        async with self._lock:
            if limit is not None:
                return list(self.recycling_history)[-limit:]
            return list(self.recycling_history)

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
    def __init__(self, manager: "CircularComputingManager",
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
                        self._saved.setdefault(
                            'carbon_last_update',
                            self.manager.carbon_manager.last_update,
                        )
                        self.manager.carbon_manager.carbon_intensity = 0.0
                        self.manager.carbon_manager.last_update = datetime.now(timezone.utc)
                    return {'fault': fault_type.value}
                if fault_type == FaultType.HELIUM_BUDGET_EXCEEDED:
                    if self.manager.helium_manager is not None:
                        self._saved.setdefault(
                            'helium_total_usage',
                            self.manager.helium_manager._total_usage,
                        )
                        self.manager.helium_manager._total_usage = (
                            self.manager.helium_manager.budget_l + 10.0
                        )
                    return {'fault': fault_type.value}
                if fault_type == FaultType.ML_SELECTOR_FAILURE:
                    self._saved.setdefault('ml_selector', self.manager.ml_selector)
                    self.manager.ml_selector = None
                    if self.manager.analyzer is not None:
                        self.manager.analyzer.ml_selector = None
                    return {'fault': fault_type.value}
                if fault_type == FaultType.STORAGE_CORRUPTION:
                    # Non-destructive marker
                    return {'fault': fault_type.value, 'noop': True}
                if fault_type == FaultType.EVENT_QUEUE_OVERFLOW:
                    return {'fault': fault_type.value, 'noop': True}
                if fault_type == FaultType.PERSISTENCE_FAILURE:
                    self._saved.setdefault('persistence', self.manager.persistence)
                    self.manager.persistence = None
                    if self.manager.reporter is not None:
                        self.manager.reporter.persistence = None
                    return {'fault': fault_type.value}
                if fault_type == FaultType.COMPONENT_FAILURE:
                    component_id = params.get('component_id')
                    if component_id and component_id in self.manager.storage.components:
                        prior_state = self.manager.storage.components[component_id].current_state
                        self._saved.setdefault(f'component_state:{component_id}', prior_state)
                        await self.manager.storage.update_component_state(
                            component_id, HardwareState.DEGRADED,
                        )
                        return {'fault': fault_type.value, 'component_id': component_id}
                    return {'fault': fault_type.value, 'noop': True}
            except Exception as e:
                logger.error(f"Fault injection failed ({fault_type}): {e}")
                return {'fault': fault_type.value, 'error': str(e)}
            return {'fault': fault_type.value, 'not_implemented': True}

    def restore(self) -> None:
        if 'carbon_intensity' in self._saved and self.manager.carbon_manager is not None:
            self.manager.carbon_manager.carbon_intensity = self._saved.pop('carbon_intensity')
        if 'carbon_last_update' in self._saved and self.manager.carbon_manager is not None:
            self.manager.carbon_manager.last_update = self._saved.pop('carbon_last_update')
        if 'helium_total_usage' in self._saved and self.manager.helium_manager is not None:
            self.manager.helium_manager._total_usage = self._saved.pop('helium_total_usage')
        if 'ml_selector' in self._saved:
            self.manager.ml_selector = self._saved.pop('ml_selector')
            if self.manager.analyzer is not None:
                self.manager.analyzer.ml_selector = self.manager.ml_selector
        if 'persistence' in self._saved:
            self.manager.persistence = self._saved.pop('persistence')
            if self.manager.reporter is not None:
                self.manager.reporter.persistence = self.manager.persistence
        # Restore component states (deferred since restore is sync)
        for key in list(self._saved.keys()):
            if key.startswith('component_state:'):
                component_id = key.split(':', 1)[1]
                prior_state = self._saved.pop(key)
                if component_id in self.manager.storage.components:
                    self.manager.storage.components[component_id].current_state = prior_state

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

            # Register a throwaway component and exercise the system under fault
            try:
                await self.manager.register_component(
                    component_type='chaos_test',
                    materials={MaterialType.SILICON: 10.0, MaterialType.COPPER: 5.0},
                    manufacturing_carbon=20.0,
                )
                # Find the newly registered component
                components = await self.manager.storage.get_components()
                target = components[-1] if components else None
                if target is not None:
                    await self.manager.deploy_component(target.component_id)
                    result = await self.manager.recycle_component(
                        target.component_id, use_ml_optimization=True, return_mopd=True,
                    )
                    report['results']['post_fault_recycle'] = {
                        'success': 'error' not in result,
                        'average_recovery_rate': result.get('average_recovery_rate', 0.0),
                        'carbon_saved_kg': result.get('carbon_saved_kg', 0.0),
                    }
                else:
                    report['results']['post_fault_recycle'] = {
                        'success': False, 'error': 'no_component_registered',
                    }
            except Exception as e:
                report['results']['post_fault_recycle'] = {
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
class CircularAnalyzer:
    def __init__(
        self,
        config: CircularComputingConfig,
        storage: CircularStorage,
        carbon_manager: Optional[CarbonIntensityManager],
        helium_manager: Optional[HeliumLifecycleManager],
        predictive: Optional[PredictiveLifecycleAnalyzer],
        ml_selector: Optional[MLComponentSelector],
        human_ai: Optional[HumanAICollaborativeCircular],
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
    # Component lifecycle
    # ------------------------------------------------------------------
    async def register_component(
        self,
        component_type: str,
        materials: Dict[MaterialType, float],
        manufacturing_carbon: float,
        expected_lifetime_days: int = 1825,
        helium_content_l: float = 0.0,
    ) -> str:
        component_id = uuid.uuid4().hex
        component = HardwareComponent(
            component_id=component_id,
            type=component_type,
            materials=dict(materials),
            manufacturing_carbon=float(manufacturing_carbon),
            current_state=HardwareState.MANUFACTURING,
            deployment_date=datetime.now(timezone.utc),
            expected_lifetime_days=int(expected_lifetime_days),
            helium_content_l=float(helium_content_l),
        )
        await self.storage.add_component(component)
        if helium_content_l > 0 and self.helium_manager is not None:
            self.helium_manager.register_component_helium(
                component_id, helium_content_l, component_type,
            )
        self.observability.counter('components_registered', type=component_type)
        logger.info(
            f"Registered component {component_id} ({component_type}) with "
            f"{sum(materials.values()):.2f}g materials"
        )
        return component_id

    async def deploy_component(self, component_id: str) -> None:
        component = await self.storage.get_component(component_id)
        if component is None:
            raise KeyError(f"Component {component_id} not found")
        component.deployment_date = datetime.now(timezone.utc)
        await self.storage.update_component_state(component_id, HardwareState.DEPLOYED)
        self.observability.counter('components_deployed')
        logger.info(f"Deployed component {component_id}")

    async def record_utilization(self, component_id: str, utilization_rate: float) -> None:
        component = await self.storage.get_component(component_id)
        if component is None:
            raise KeyError(f"Component {component_id} not found")
        rate = max(0.0, min(1.0, float(utilization_rate)))
        component.utilization_history.append(rate)
        if len(component.utilization_history) > 1000:
            component.utilization_history = component.utilization_history[-1000:]
        self.observability.histogram('component_utilization', rate, type=component.type)

    # ------------------------------------------------------------------
    # MOPD enumeration
    # ------------------------------------------------------------------
    async def _enumerate_recycling_strategies(
        self, component: HardwareComponent,
    ) -> List[MOPDPlan]:
        recycling_methods = ['full_recycling', 'repurposing', 'material_recovery']
        helium_recovery_options = [False]
        if component.helium_content_l > 0:
            helium_recovery_options = [False, True]
        material_targets = [0.5, 0.75, 0.95]
        use_ml_options = [True, False]

        plans: List[MOPDPlan] = []
        for method in recycling_methods:
            for hr in helium_recovery_options:
                for target in material_targets:
                    for use_ml in use_ml_options:
                        plan = MOPDPlan(
                            recycling_method=method,
                            helium_recovery=hr,
                            material_recovery_target=target,
                            use_ml_optimization=use_ml,
                            cost=0.0,
                            carbon_saved_kg=0.0,
                            helium_recovered_l=0.0,
                            material_recovery_rate=0.0,
                            time_days=0.0,
                            sustainability_score=0.0,
                            explanation='',
                        )
                        plans.append(plan)
        return plans

    async def _compute_plan_objectives(
        self, plan: MOPDPlan, component: HardwareComponent,
    ) -> MOPDPlan:
        cost = 0.0
        carbon_saved = component.manufacturing_carbon * 0.8
        helium_recovered = 0.0
        material_recovery = 0.0
        time_days = 0.0

        if plan.recycling_method == 'full_recycling':
            cost = 5.0
            carbon_saved = component.manufacturing_carbon * 0.8
            material_recovery = 0.95
            time_days = 10.0
        elif plan.recycling_method == 'repurposing':
            cost = 2.0
            carbon_saved = component.manufacturing_carbon * 0.5
            material_recovery = 0.7
            time_days = 5.0
        elif plan.recycling_method == 'material_recovery':
            cost = 3.0
            carbon_saved = component.manufacturing_carbon * 0.6
            material_recovery = plan.material_recovery_target
            time_days = 7.0

        if plan.helium_recovery and self.helium_manager is not None:
            helium_recovered = self.helium_manager.calculate_recovery(component.component_id)
            if helium_recovered > 0:
                cost += 2.0
                time_days += 3.0
                carbon_saved += helium_recovered * 5.0

        if plan.use_ml_optimization and self.ml_selector is not None:
            try:
                ml_result = await self.ml_selector.select_component({
                    'age_days': (datetime.now(timezone.utc) - component.deployment_date).days,
                    'utilization': (
                        float(np.mean(component.utilization_history[-50:]))
                        if component.utilization_history else 0.5
                    ),
                    'maintenance_count': len(component.maintenance_log),
                    'carbon_footprint': component.manufacturing_carbon,
                    'helium_content': component.helium_content_l,
                    'recycling_potential': 0.8,
                    'reliability': 0.9,
                    'cost_efficiency': 0.7,
                })
                if ml_result and ml_result.get('score', 0.0) > 0.5:
                    material_recovery *= 1.05
                    carbon_saved *= 1.1
                    cost *= 0.95
            except Exception as e:
                logger.warning(f"ML selector failed: {e}")

        carbon_ratio = (
            carbon_saved / component.manufacturing_carbon
            if component.manufacturing_carbon > 0 else 0.0
        )
        helium_ratio = (
            helium_recovered / component.helium_content_l
            if component.helium_content_l > 0 else 0.0
        )
        sustainability = (
            material_recovery * 0.4 +
            carbon_ratio * 0.3 +
            helium_ratio * 0.3
        )

        plan.cost = cost
        plan.carbon_saved_kg = carbon_saved
        plan.helium_recovered_l = helium_recovered
        plan.material_recovery_rate = min(1.0, material_recovery)
        plan.time_days = time_days
        plan.sustainability_score = min(1.0, max(0.0, sustainability))

        plan.explanation = (
            f"method={plan.recycling_method}, helium_recovery={plan.helium_recovery}, "
            f"material_target={plan.material_recovery_target:.2f}, "
            f"ml={plan.use_ml_optimization} | carbon_saved={plan.carbon_saved_kg:.2f}kg, "
            f"helium_recovered={plan.helium_recovered_l:.2f}L, "
            f"recovery_rate={plan.material_recovery_rate:.2f}, "
            f"cost=${plan.cost:.1f}, time={plan.time_days:.0f}d"
        )
        return plan

    async def _generate_pareto_front_for_recycling(
        self, component_id: str,
    ) -> List[MOPDPlan]:
        component = await self.storage.get_component(component_id)
        if component is None:
            return []

        plans = await self._enumerate_recycling_strategies(component)
        computed: List[MOPDPlan] = []
        for plan in plans:
            computed.append(await self._compute_plan_objectives(plan, component))

        if not computed:
            return []

        pareto: List[MOPDPlan] = []
        for i, p_i in enumerate(computed):
            a_vec = p_i.objective_vector()
            dominated = False
            for j, p_j in enumerate(computed):
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
    # Safety helpers
    # ------------------------------------------------------------------
    def _check_invariants(self, component: HardwareComponent) -> List[str]:
        violations: List[str] = []
        if component.helium_content_l > 10 and self.helium_manager is None:
            violations.append("Large helium content but no helium manager")
        if component.manufacturing_carbon < 0:
            violations.append("Manufacturing carbon cannot be negative")
        total_materials = sum(component.materials.values())
        if total_materials < 0:
            violations.append("Material mass cannot be negative")
        return violations

    def _is_safe_plan(self, plan: MOPDPlan) -> bool:
        return plan.cost <= 10.0 and plan.time_days <= 15.0

    # ------------------------------------------------------------------
    # Core recycling
    # ------------------------------------------------------------------
    async def recycle_component(
        self,
        component_id: str,
        use_ml_optimization: bool = False,
        return_mopd: bool = False,
        idempotency_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        if self._is_duplicate(idempotency_key):
            return {
                'duplicate': True,
                'idempotency_key': idempotency_key,
                'timestamp': datetime.now(timezone.utc).isoformat(),
            }

        component = await self.storage.get_component(component_id)
        if component is None:
            return {'error': 'Component not found'}

        pareto_front: Optional[List[MOPDPlan]] = None
        best_plan: Optional[MOPDPlan] = None

        # MOPD Pareto front
        if self.config.enable_mopd and return_mopd:
            pareto_front = await self._generate_pareto_front_for_recycling(component_id)
            if pareto_front:
                for plan in pareto_front:
                    await self.storage.add_mopd_plan(plan)
                best_plan = self._select_best_from_pareto(pareto_front)
                if best_plan is not None:
                    use_ml_optimization = best_plan.use_ml_optimization

        # Temporal safety
        if self.config.enable_temporal_safety:
            violations = self._check_invariants(component)
            if violations:
                logger.warning(f"Temporal safety violations for {component_id}: {violations}")
                if pareto_front:
                    safe_plans = [p for p in pareto_front if self._is_safe_plan(p)]
                    if safe_plans:
                        best_plan = min(
                            safe_plans,
                            key=lambda p: p.sustainability_score,
                            reverse=True,
                        )
                        use_ml_optimization = best_plan.use_ml_optimization
                    else:
                        return {
                            'error': 'All plans violate temporal safety',
                            'temporal_safety_violations': violations,
                        }

        # Human approval
        if (self.governance is not None and best_plan is not None and
                best_plan.sustainability_score < self.config.approval.require_for_sustainability_below):
            approval = await self.governance.request_approval({
                'action': 'approve_recycling_plan',
                'component_id': component_id,
                'component_type': component.type,
                'plan': best_plan.to_dict(),
                'sustainability_score': best_plan.sustainability_score,
            })
            if approval.status not in (ApprovalStatus.APPROVED, ApprovalStatus.AUTO_APPROVED):
                logger.info(
                    f"Recycling not approved for {component_id}: {approval.status.value}"
                )
                return {
                    'error': 'human_approval_not_granted',
                    'approval': approval.to_dict(),
                }

        # ML optimization
        ml_result: Optional[Dict[str, Any]] = None
        if use_ml_optimization and self.ml_selector is not None:
            try:
                ml_result = await self.ml_selector.select_component({
                    'age_days': (datetime.now(timezone.utc) - component.deployment_date).days,
                    'utilization': (
                        float(np.mean(component.utilization_history[-50:]))
                        if component.utilization_history else 0.5
                    ),
                    'maintenance_count': len(component.maintenance_log),
                    'carbon_footprint': component.manufacturing_carbon,
                    'helium_content': component.helium_content_l,
                    'recycling_potential': 0.8,
                    'reliability': 0.9,
                    'cost_efficiency': 0.7,
                })
            except Exception as e:
                logger.warning(f"ML selector failed: {e}")

        # Atomic inventory update
        recovered_materials: Dict[str, Dict[str, float]] = {}
        total_recovery_rate = 0.0

        async with self.storage._lock:
            for material, amount in component.materials.items():
                rate = DEFAULT_RECOVERY_RATES.get(material.value, 0.9)
                if best_plan is not None and best_plan.material_recovery_target < 1.0:
                    rate = min(rate, best_plan.material_recovery_target)
                recovered = amount * rate
                recovered_materials[material.value] = {
                    'original_g': float(amount),
                    'recovered_g': float(recovered),
                    'recovery_rate': float(rate),
                }
                self.storage.material_inventory[material] -= amount
                self.storage.material_inventory[material] += recovered
                total_recovery_rate += rate

            avg_recovery = (
                total_recovery_rate / len(recovered_materials)
                if recovered_materials else 0.0
            )

            carbon_saved = component.manufacturing_carbon * 0.8
            component.carbon_savings_kg = carbon_saved

            helium_recovered = 0.0
            if self.helium_manager is not None and component.helium_content_l > 0:
                helium_recovered = self.helium_manager.calculate_recovery(component_id)
                if helium_recovered > 0:
                    self.helium_manager.record_recovery(component_id, helium_recovered)
                    recovered_materials['helium_recovered'] = {
                        'original_g': component.helium_content_l * 1000.0,
                        'recovered_g': helium_recovered * 1000.0,
                        'recovery_rate': 0.85,
                    }
                    self.storage.material_inventory[MaterialType.HELIUM] -= (
                        component.helium_content_l * 1000.0
                    )
                    self.storage.material_inventory[MaterialType.HELIUM] += (
                        helium_recovered * 1000.0
                    )

            component.current_state = HardwareState.RECYCLED

        sustainability = self._calc_sustainability(
            avg_recovery, carbon_saved, helium_recovered,
        )
        await self.storage.update_sustainability_score(sustainability)

        record: Dict[str, Any] = {
            'component_id': component_id,
            'component_type': component.type,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'materials_recovered': recovered_materials,
            'average_recovery_rate': avg_recovery,
            'carbon_saved_kg': carbon_saved,
            'helium_recovered_g': helium_recovered * 1000.0,
            'ml_optimization': ml_result,
            'sustainability_score': sustainability,
            'explanation': best_plan.explanation if best_plan else "Standard recycling",
            'requires_approval': bool(
                best_plan is not None and
                best_plan.sustainability_score <
                self.config.approval.require_for_sustainability_below
            ),
            'mopd_pareto_front': (
                [p.to_dict() for p in pareto_front] if pareto_front else None
            ),
            'mopd_best_plan': best_plan.to_dict() if best_plan else None,
        }
        await self.storage.add_recycling_record(record)
        await self._update_circularity_metrics()

        # Predictive history
        if self.predictive is not None:
            age_days = (datetime.now(timezone.utc) - component.deployment_date).days
            avg_util = (
                float(np.mean(component.utilization_history[-50:]))
                if component.utilization_history else 0.5
            )
            self.predictive.update_history({
                'age_days': age_days,
                'utilization': avg_util,
                'maintenance_count': len(component.maintenance_log),
                'carbon_score': 1.0 / (1.0 + component.manufacturing_carbon),
                'helium_remaining': component.helium_content_l,
            })

        # Human-AI insights
        if self.human_ai is not None:
            record['human_ai_insights'] = self.human_ai.get_insights()

        # Observability
        self.observability.counter('recycles_total', type=component.type)
        self.observability.gauge('material_recovery_rate', avg_recovery)
        self.observability.gauge('sustainability_score', sustainability)
        if pareto_front:
            self.observability.histogram('mopd_pareto_front_size', len(pareto_front))

        logger.info(
            f"Recycled {component_id}: {avg_recovery:.1%} recovery, "
            f"{carbon_saved:.2f} kg CO2 saved"
        )
        return record

    def _calc_sustainability(self, recovery_rate: float, carbon_saved: float,
                             helium_recovered: float) -> float:
        recovery_factor = max(0.0, min(1.0, recovery_rate))
        carbon_factor = min(1.0, carbon_saved / 10.0)
        helium_factor = min(1.0, helium_recovered / 10.0)
        score = recovery_factor * 0.4 + carbon_factor * 0.3 + helium_factor * 0.3
        return max(0.0, min(1.0, score))

    async def _update_circularity_metrics(self) -> None:
        stats = await self.storage.get_stats()
        total = stats['total_components']
        if total == 0:
            return
        recycled = stats['components_by_state'].get(HardwareState.RECYCLED.value, 0)
        repurposed = stats['components_by_state'].get(HardwareState.REPURPOSED.value, 0)
        circularity = (recycled + repurposed) / total

        history = await self.storage.get_recycling_history()
        recovery = (
            float(np.mean([h['average_recovery_rate'] for h in history]))
            if history else 0.0
        )
        waste_diversion = circularity
        await self.storage.update_metrics(circularity, waste_diversion, recovery)

    # ------------------------------------------------------------------
    # ML training
    # ------------------------------------------------------------------
    async def train_ml_model(self, training_data: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
        if self.ml_selector is None:
            return {'status': 'disabled'}
        if training_data is None:
            history = await self.storage.get_recycling_history(100)
            training_data = []
            for item in history:
                try:
                    ts = datetime.fromisoformat(item['timestamp'])
                    age_days = (datetime.now(timezone.utc) - ts).days
                except Exception:
                    age_days = 365
                training_data.append({
                    'age_days': age_days,
                    'utilization': 0.5,
                    'maintenance_count': 0,
                    'carbon_footprint': item.get('carbon_saved_kg', 0.5) / 10.0,
                    'helium_content': item.get('helium_recovered_g', 0.0) / 1000.0,
                    'recycling_potential': item.get('average_recovery_rate', 0.5),
                    'reliability': 0.9,
                    'cost_efficiency': 0.7,
                    'selection_score': item.get('sustainability_score', 0.5),
                })
        return await self.ml_selector.train(training_data)

    async def train_predictive_model(self) -> Dict[str, Any]:
        if self.predictive is None:
            return {'status': 'disabled'}
        return await self.predictive.train()

    # ------------------------------------------------------------------
    # Hardware allocation
    # ------------------------------------------------------------------
    async def optimize_hardware_allocation(
        self,
        expert_requirements: Dict[str, Any],
        carbon_budget: float,
        helium_budget: float,
        use_ml: bool = False,
    ) -> Dict[str, Any]:
        available = await self.storage.get_components(HardwareState.DEPLOYED)
        available += await self.storage.get_components(HardwareState.MAINTENANCE)
        if not available:
            return {'error': 'No available hardware', 'suggestion': 'deploy_new'}

        carbon_intensity = (
            self.carbon_manager.carbon_intensity
            if self.carbon_manager is not None else DEFAULT_CARBON_INTENSITY
        )
        metrics = await self.storage.get_metrics()
        sustainability = metrics['sustainability_score']

        # Thresholds: treat budgets < 10% of total capacity as "tight"
        carbon_tight = carbon_budget < 100.0
        helium_tight = helium_budget < 10.0

        scored: List[Tuple[HardwareComponent, float]] = []
        for comp in available:
            age = (datetime.now(timezone.utc) - comp.deployment_date).days
            lifecycle = max(0.1, 1.0 - (age / max(comp.expected_lifetime_days, 1)))
            carbon_score = 1.0 / (1.0 + comp.manufacturing_carbon)
            helium_score = (
                comp.helium_content_l / 100.0 if self.helium_manager is not None else 0.5
            )
            if comp.utilization_history:
                avg_util = float(np.mean(comp.utilization_history[-50:]))
                util_score = max(0.0, 1.0 - avg_util)
            else:
                util_score = 0.5

            if carbon_tight:
                score = (
                    0.3 * carbon_score + 0.25 * lifecycle +
                    0.25 * util_score + 0.2 * sustainability
                )
            elif helium_tight:
                score = (
                    0.3 * helium_score + 0.25 * carbon_score +
                    0.25 * lifecycle + 0.2 * sustainability
                )
            else:
                score = (
                    0.2 * carbon_score + 0.2 * lifecycle + 0.2 * util_score +
                    0.2 * helium_score + 0.2 * sustainability
                )
            scored.append((comp, score))

        scored.sort(key=lambda x: x[1], reverse=True)
        best = scored[0][0] if scored else None

        ml_result: Optional[Dict[str, Any]] = None
        if best is not None and use_ml and self.ml_selector is not None:
            try:
                ml_result = await self.ml_selector.select_component({
                    'age_days': (datetime.now(timezone.utc) - best.deployment_date).days,
                    'utilization': (
                        float(np.mean(best.utilization_history[-50:]))
                        if best.utilization_history else 0.5
                    ),
                    'maintenance_count': len(best.maintenance_log),
                    'carbon_footprint': best.manufacturing_carbon,
                    'helium_content': best.helium_content_l,
                    'recycling_potential': 0.8,
                    'reliability': 0.9,
                    'cost_efficiency': 0.7,
                })
            except Exception as e:
                logger.warning(f"ML selector failed: {e}")

        return {
            'selected_component': best.component_id if best else None,
            'score': scored[0][1] if scored else 0.0,
            'component_type': best.type if best else None,
            'age_days': (
                (datetime.now(timezone.utc) - best.deployment_date).days if best else 0
            ),
            'manufacturing_carbon': best.manufacturing_carbon if best else 0.0,
            'helium_content_l': best.helium_content_l if best else 0.0,
            'carbon_intensity': carbon_intensity,
            'sustainability_score': sustainability,
            'ml_result': ml_result,
            'recommendation': (
                'use_existing' if scored and scored[0][1] > 0.5 else 'consider_repurposing'
            ),
        }


# ============================================================================
# Reporter
# ============================================================================
class CircularReporter:
    def __init__(
        self,
        config: CircularComputingConfig,
        storage: CircularStorage,
        analyzer: CircularAnalyzer,
        observability: Optional[ObservabilityHub],
        persistence: Optional[CircularComputingPersistenceManager],
        human_ai: Optional[HumanAICollaborativeCircular],
        federated: Optional[FederatedCircularManager],
        predictive: Optional[PredictiveLifecycleAnalyzer],
        ml_selector: Optional[MLComponentSelector],
        helium_manager: Optional[HeliumLifecycleManager],
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

    async def get_circularity_report(self) -> Dict[str, Any]:
        stats = await self.storage.get_stats()
        metrics = await self.storage.get_metrics()
        history = await self.storage.get_recycling_history()
        components = await self.storage.get_components()

        material_flows: Dict[str, Dict[str, float]] = {}
        for material in MaterialType:
            in_use = sum(
                c.materials.get(material, 0.0)
                for c in components
                if c.current_state != HardwareState.RECYCLED
            )
            recovered = sum(
                r.get('materials_recovered', {}).get(material.value, {}).get('recovered_g', 0.0)
                for r in history
            )
            material_flows[material.value] = {
                'in_use_g': float(in_use),
                'recovered_g': float(recovered),
                'inventory_g': float(stats['material_inventory'].get(material.value, 0.0)),
            }

        report: Dict[str, Any] = {
            'circularity_score': metrics['circularity_score'],
            'waste_diversion_rate': metrics['waste_diversion_rate'],
            'material_recovery_rate': metrics['material_recovery_rate'],
            'sustainability_score': metrics['sustainability_score'],
            'total_components': stats['total_components'],
            'components_by_state': stats['components_by_state'],
            'material_flows': material_flows,
            'total_carbon_saved_kg': float(sum(r.get('carbon_saved_kg', 0.0) for r in history)),
            'helium_recovered_g': float(sum(r.get('helium_recovered_g', 0.0) for r in history)),
        }
        if self.helium_manager is not None:
            report['helium_position'] = self.helium_manager.get_position()
        if self.federated is not None:
            report['federated_stats'] = self.federated.get_federated_stats()
        if self.predictive is not None:
            report['predictive_forecast'] = await self.predictive.predict_lifetime(
                {'age_days': 365, 'utilization': 0.5},
            )
        if self.ml_selector is not None:
            report['ml_status'] = {
                'trained': self.ml_selector.is_trained,
                'model_version': 'v5.0.0',
                'training_samples': len(self.ml_selector.training_history),
            }
        if self.human_ai is not None:
            report['human_ai_insights'] = self.human_ai.get_insights()
        if self.config.enable_mopd:
            mopd_plans = await self.storage.get_mopd_plans(20)
            report['mopd_plans'] = [p.to_dict() for p in mopd_plans]
        return report

    async def get_sustainability_report(self) -> Dict[str, Any]:
        metrics = await self.storage.get_metrics()
        return {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'sustainability_score': metrics['sustainability_score'],
            'circularity_report': await self.get_circularity_report(),
            'recommendations': await self._generate_recommendations(),
        }

    async def _generate_recommendations(self) -> List[str]:
        metrics = await self.storage.get_metrics()
        recs: List[str] = []
        if metrics['sustainability_score'] < 0.5:
            recs.append("Improve circularity through better material recovery")
        if metrics['circularity_score'] < 0.5:
            recs.append("Increase component repurposing and recycling")
        if self.helium_manager is not None:
            pos = self.helium_manager.get_position()
            if pos.get('remaining_budget_l', 0.0) < 0:
                recs.append("CRITICAL: Helium budget exceeded - implement recovery systems")
        if self.federated is not None and len(self.federated.participants) < 2:
            recs.append("Increase federated participation for better circularity insights")
        if metrics['material_recovery_rate'] < 0.5:
            recs.append("Improve material recovery rate through better recycling processes")
        if self.config.enable_mopd:
            recs.append("Consider using MOPD to explore trade-offs among recycling strategies")
        if self.config.enable_chaos_testing:
            recs.append("Run chaos tests to verify system resilience")
        return recs or ["All circularity metrics are within acceptable ranges"]

    async def export_telemetry(self) -> None:
        if self.observability is not None:
            data = await self.observability.export_prometheus()
            logger.debug(f"Telemetry export: {len(data)} bytes")

    async def save_state(self) -> None:
        if self.persistence is None:
            return
        stats = await self.storage.get_stats()
        metrics = await self.storage.get_metrics()
        components = await self.storage.get_components()
        history = await self.storage.get_recycling_history()
        mopd_plans = await self.storage.get_mopd_plans()

        state = {
            'components': {c.component_id: c.to_dict() for c in components},
            'material_inventory': stats['material_inventory'],
            'recycling_history': history,
            'circularity_score': metrics['circularity_score'],
            'waste_diversion_rate': metrics['waste_diversion_rate'],
            'material_recovery_rate': metrics['material_recovery_rate'],
            'sustainability_score': metrics['sustainability_score'],
            'helium_manager_state': (
                {
                    'usage': list(self.helium_manager.usage),
                    'recovered': list(self.helium_manager.recovered),
                    'component_helium': self.helium_manager.component_helium,
                    '_total_usage': self.helium_manager._total_usage,
                    '_total_recovered': self.helium_manager._total_recovered,
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

        # Restore components
        async with self.storage._lock:
            self.storage.components.clear()
            self.storage.material_inventory.clear()
            for c_data in state.get('components', {}).values():
                try:
                    comp = HardwareComponent.from_dict(c_data)
                    self.storage.components[comp.component_id] = comp
                    for mat, amount in comp.materials.items():
                        self.storage.material_inventory[mat] += amount
                except Exception as e:
                    logger.warning(f"Skipping invalid component: {e}")

            # Overlay persisted inventory (corrected by history)
            for mat_str, amount in state.get('material_inventory', {}).items():
                try:
                    self.storage.material_inventory[MaterialType(mat_str)] = float(amount)
                except (ValueError, KeyError):
                    pass

            self.storage.recycling_history.clear()
            for r in state.get('recycling_history', []):
                self.storage.recycling_history.append(r)

            self.storage.mopd_plans.clear()
            for p_data in state.get('mopd_plans', []):
                try:
                    self.storage.mopd_plans.append(MOPDPlan.from_dict(p_data))
                except Exception as e:
                    logger.warning(f"Skipping invalid MOPD plan: {e}")

        await self.storage.update_metrics(
            float(state.get('circularity_score', 0.0)),
            float(state.get('waste_diversion_rate', 0.0)),
            float(state.get('material_recovery_rate', 0.0)),
        )
        await self.storage.update_sustainability_score(
            float(state.get('sustainability_score', 0.0))
        )

        # Helium
        he_state = state.get('helium_manager_state')
        if he_state and self.helium_manager is not None:
            self.helium_manager.usage = deque(
                he_state.get('usage', []), maxlen=MAX_HELIUM_HISTORY,
            )
            self.helium_manager.recovered = deque(
                he_state.get('recovered', []), maxlen=MAX_HELIUM_HISTORY,
            )
            self.helium_manager.component_helium = he_state.get('component_helium', {})
            self.helium_manager._total_usage = float(he_state.get('_total_usage', 0.0))
            self.helium_manager._total_recovered = float(he_state.get('_total_recovered', 0.0))
            self.helium_manager.budget_l = float(
                he_state.get('budget_l', self.config.helium_budget_l)
            )

        # ML checkpoint
        ml_cp = state.get('ml_checkpoint')
        if ml_cp and self.ml_selector is not None:
            self.ml_selector.load_checkpoint(ml_cp)


# ============================================================================
# Main Controller
# ============================================================================
class CircularComputingManager:
    """
    Enhanced Circular Computing Manager v5.0.0.

    Complete, event-driven, MOPD-aware, XAI-enabled, safety-checked,
    approval-gated, chaos-tested, DP-federated, and observability-instrumented.
    """

    def __init__(
        self,
        bio_core: Optional[Any] = None,
        config: Optional[CircularComputingConfig] = None,
        **kwargs: Any,
    ) -> None:
        if config is None:
            valid = {f.name for f in dataclass_fields(CircularComputingConfig)}
            filtered = {k: v for k, v in kwargs.items() if k in valid}
            config = CircularComputingConfig(**filtered)
        self.config = config

        # Bio-core
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
        self.helium_manager: Optional[HeliumLifecycleManager] = (
            HeliumLifecycleManager(config.helium) if config.helium.enabled else None
        )
        self.predictive: Optional[PredictiveLifecycleAnalyzer] = (
            PredictiveLifecycleAnalyzer(config.predictive)
            if config.predictive.enabled else None
        )
        self.ml_selector: Optional[MLComponentSelector] = (
            MLComponentSelector(config.ml) if config.ml.enabled else None
        )
        self.federated: Optional[FederatedCircularManager] = (
            FederatedCircularManager(config.federated)
            if config.federated.enabled else None
        )
        self.human_ai: Optional[HumanAICollaborativeCircular] = (
            HumanAICollaborativeCircular() if config.enable_human_ai else None
        )
        self.persistence: Optional[CircularComputingPersistenceManager] = (
            CircularComputingPersistenceManager(config.persistence)
            if config.persistence.enabled else None
        )

        # Storage / Analyzer / Reporter
        self.storage = CircularStorage()
        self.analyzer = CircularAnalyzer(
            config, self.storage, self.carbon_manager, self.helium_manager,
            self.predictive, self.ml_selector, self.human_ai, self.governance,
            self.observability,
        )
        self.reporter = CircularReporter(
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

        logger.info("CircularComputingManager v5.0.0 initialised (not started)")

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def start(self) -> None:
        if self._started:
            return
        self._started = True

        self._event_queue = asyncio.Queue(maxsize=10_000)

        if self.helium_manager is not None:
            self.helium_manager.start()

        if self.config.enable_event_driven and self.event_broker is not None:
            self._subscribe_events()

        self._start_background_tasks()

        if self.config.persistence.enabled:
            await self.reporter.load_state()

        logger.info("CircularComputingManager started")

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

        def _inventory_nonneg(state: Dict[str, Any]) -> bool:
            inv = state.get('material_inventory', {})
            return all(v >= 0 for v in inv.values())

        def _helium_budget_nonneg(state: Dict[str, Any]) -> bool:
            return state.get('helium_remaining_budget_l', 0.0) >= 0.0

        def _sustainability_bounded(state: Dict[str, Any]) -> bool:
            v = state.get('sustainability_score', 0.5)
            return 0.0 <= v <= 1.0

        def _circularity_bounded(state: Dict[str, Any]) -> bool:
            v = state.get('circularity_score', 0.5)
            return 0.0 <= v <= 1.0

        self.safety_monitor.add_invariant(
            "material_inventory_nonneg", _inventory_nonneg,
            "Material inventory must remain non-negative",
        )
        self.safety_monitor.add_invariant(
            "helium_budget_nonneg", _helium_budget_nonneg,
            "Helium budget must remain non-negative",
        )
        self.safety_monitor.add_invariant(
            "sustainability_bounded", _sustainability_bounded,
            "Sustainability score must be in [0, 1]",
        )
        self.safety_monitor.add_invariant(
            "circularity_bounded", _circularity_bounded,
            "Circularity score must be in [0, 1]",
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
                'age_days': 0.0,
                'utilization': 0.5,
                'maintenance_count': 0,
                'carbon_score': 1.0 / (1.0 + float(event.data.get('price', 50.0)) / 50.0),
                'helium_remaining': 0.5,
            })
        self.config.carbon_recycling_priority = (
            0.8 if intensity > 500 else self.config.carbon_recycling_priority
        )

    async def _on_helium_update(self, event: Any) -> None:
        scarcity = float(event.data.get('scarcity', 0.5))
        if self.helium_manager is not None:
            # Derive budget from configured base — do NOT mutate nested config
            self.helium_manager.budget_l = self.config.helium_budget_l * (
                1.0 - scarcity * 0.3
            )

    async def _on_alert_generated(self, event: Any) -> None:
        if event.data.get('severity') == 'critical':
            logger.warning("Critical alert; triggering self-healing")
            self.config.circularity_strategy = 'conservative'
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
        if 'circular_computing' in updates:
            new = updates['circular_computing']
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
            self.config.carbon_recycling_priority = 0.9
        elif metric == 'helium_scarcity':
            if self.helium_manager is not None:
                # Derive from configured base — do NOT compound
                self.helium_manager.budget_l = self.config.helium_budget_l * 0.8

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
                    await self.analyzer.train_ml_model()
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
                    metrics = await self.storage.get_metrics()
                    pid = f"circular_{uuid.uuid4().hex[:8]}"
                    await self.federated.send_local_components(
                        pid,
                        {
                            'total_components': stats['total_components'],
                            'circularity_score': metrics['circularity_score'],
                            'waste_diversion_rate': metrics['waste_diversion_rate'],
                            'sustainability_score': metrics['sustainability_score'],
                            'timestamp': datetime.now(timezone.utc).isoformat(),
                        },
                        performance=metrics['sustainability_score'],
                    )
                    await self.federated.get_global_components()
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
    # Public API
    # ------------------------------------------------------------------
    async def register_component(
        self,
        component_type: str,
        materials: Dict[MaterialType, float],
        manufacturing_carbon: float,
        expected_lifetime_days: int = 1825,
        helium_content_l: float = 0.0,
    ) -> str:
        await self.wait_ready()
        return await self.analyzer.register_component(
            component_type, materials, manufacturing_carbon,
            expected_lifetime_days, helium_content_l,
        )

    async def deploy_component(self, component_id: str) -> None:
        await self.wait_ready()
        await self.analyzer.deploy_component(component_id)

    async def record_utilization(self, component_id: str, utilization_rate: float) -> None:
        await self.wait_ready()
        await self.analyzer.record_utilization(component_id, utilization_rate)

    async def recycle_component(
        self,
        component_id: str,
        use_ml_optimization: bool = False,
        return_mopd: bool = False,
        idempotency_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        await self.wait_ready()
        try:
            result = await self.analyzer.recycle_component(
                component_id, use_ml_optimization, return_mopd, idempotency_key,
            )
            if 'error' in result and 'duplicate' not in result:
                self.observability.counter('recycles_failed')
                return result

            self.observability.counter('recycles_success')
            metrics = await self.storage.get_metrics()
            self.observability.gauge('sustainability_score', metrics['sustainability_score'])

            # MoE feedback
            if self.gating_network is not None and self.expert_router is not None:
                try:
                    features = np.array([
                        result.get('average_recovery_rate', 0.0),
                        result.get('carbon_saved_kg', 0.0) / 10.0,
                        result.get('helium_recovered_g', 0.0) / 1000.0,
                        metrics['sustainability_score'],
                    ])
                    reward = metrics['sustainability_score']
                    self.gating_network.update(
                        features, reward, {'component_id': component_id},
                    )
                except Exception as e:
                    logger.warning(f"Gating update failed: {e}")

            if self.self_evolving_gate is not None and TORCH_AVAILABLE:
                try:
                    state = torch.tensor([
                        result.get('carbon_saved_kg', 0.0),
                        result.get('helium_recovered_g', 0.0) / 1000.0,
                    ], dtype=torch.float32)
                    self.self_evolving_gate.adapt(
                        state=state, chosen_expert=0,
                        reward=metrics['sustainability_score'],
                        environmental_feedback={'component_id': component_id},
                        quantum_mode=False,
                    )
                except Exception as e:
                    logger.warning(f"Self-evolving gate adapt failed: {e}")

            # Workflow on low sustainability
            if metrics['sustainability_score'] < 0.4 and self.workflow_orchestrator is not None:
                try:
                    await self.workflow_orchestrator.execute_workflow(
                        self.config.workflow_on_slo_breach,
                    )
                except Exception as e:
                    logger.error(f"Workflow failed: {e}")

            # Safety check
            if self.safety_monitor is not None:
                helium_remaining = (
                    self.helium_manager.get_position()['remaining_budget_l']
                    if self.helium_manager else 0.0
                )
                stats = await self.storage.get_stats()
                violations = self.safety_monitor.check({
                    'material_inventory': stats['material_inventory'],
                    'helium_remaining_budget_l': helium_remaining,
                    'sustainability_score': metrics['sustainability_score'],
                    'circularity_score': metrics['circularity_score'],
                })
                if violations:
                    result['temporal_safety_violations'] = violations
                    self.observability.counter('safety_violations')

            return result
        except Exception as e:
            logger.error(f"recycle_component failed: {e}", exc_info=True)
            self.last_error = str(e)
            self.health_status = "degraded"
            return {'error': str(e)}

    async def get_circularity_report(self) -> Dict[str, Any]:
        await self.wait_ready()
        return await self.reporter.get_circularity_report()

    async def get_sustainability_report(self) -> Dict[str, Any]:
        await self.wait_ready()
        return await self.reporter.get_sustainability_report()

    async def optimize_hardware_allocation(
        self,
        expert_requirements: Dict[str, Any],
        carbon_budget: float,
        helium_budget: float,
        use_ml: bool = False,
    ) -> Dict[str, Any]:
        await self.wait_ready()
        return await self.analyzer.optimize_hardware_allocation(
            expert_requirements, carbon_budget, helium_budget, use_ml,
        )

    async def train_ml_model(self, training_data: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
        await self.wait_ready()
        return await self.analyzer.train_ml_model(training_data)

    async def train_predictive_model(self) -> Dict[str, Any]:
        await self.wait_ready()
        return await self.analyzer.train_predictive_model()

    async def get_recycling_pareto_front(self, component_id: str) -> List[MOPDPlan]:
        await self.wait_ready()
        if not self.config.enable_mopd:
            return []
        return await self.analyzer._generate_pareto_front_for_recycling(component_id)

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
        metrics = await self.storage.get_metrics()
        payload = {
            'manager_id': uuid.uuid4().hex[:8],
            'sustainability_score': metrics['sustainability_score'],
            'circularity_score': metrics['circularity_score'],
            'total_components': stats['total_components'],
            'material_recovery_rate': metrics['material_recovery_rate'],
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
        self.config.circularity_strategy = 'balanced'
        await self.storage.update_sustainability_score(0.0)

        # Trim component set (keep most recent 10)
        async with self.storage._lock:
            if len(self.storage.components) > 10:
                sorted_ids = sorted(
                    self.storage.components.keys(),
                    key=lambda cid: self.storage.components[cid].deployment_date,
                )
                for cid in sorted_ids[:-10]:
                    del self.storage.components[cid]

            # Trim history
            if len(self.storage.recycling_history) > 10:
                trimmed = list(self.storage.recycling_history)[-10:]
                self.storage.recycling_history.clear()
                for r in trimmed:
                    self.storage.recycling_history.append(r)

        self.health_status = "healthy"
        self.last_error = None
        if self.persistence is not None:
            await self.reporter.save_state()
        logger.info("Self-healing completed")

    async def get_health_status(self) -> Dict[str, Any]:
        stats = await self.storage.get_stats()
        metrics = await self.storage.get_metrics()
        return {
            'status': self.health_status,
            'last_error': self.last_error,
            'total_components': stats['total_components'],
            'circularity_score': metrics['circularity_score'],
            'sustainability_score': metrics['sustainability_score'],
            'material_recovery_rate': metrics['material_recovery_rate'],
            'bio_integration_active': self.config.enable_bio_integration,
            'event_driven_active': self.config.enable_event_driven,
            'self_healing_enabled': self.config.self_healing.enabled,
            'swarm_coordination_active': self.config.enable_swarm_coordination,
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
        logger.info("Shutting down CircularComputingManager")
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
        config = CircularComputingConfig()
        config.enable_chaos_testing = True
        config.chaos.enabled = True
        config.chaos.interval_seconds = 9999

        manager = CircularComputingManager(config=config)
        await manager.start()

        cid = await manager.register_component(
            component_type='gpu_accelerator',
            materials={
                MaterialType.SILICON: 500.0,
                MaterialType.COPPER: 200.0,
                MaterialType.GOLD: 5.0,
                MaterialType.HELIUM: 20.0,
            },
            manufacturing_carbon=150.0,
            helium_content_l=8.0,
        )
        await manager.deploy_component(cid)
        await manager.record_utilization(cid, 0.75)

        result = await manager.recycle_component(
            cid, use_ml_optimization=False, return_mopd=True,
            idempotency_key=uuid.uuid4().hex,
        )
        print(json.dumps(result, indent=2, default=str))

        report = await manager.get_circularity_report()
        print(json.dumps(report, indent=2, default=str))

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
    "CircularComputingManager",
    "CircularComputingConfig",
    # Sub-modules
    "CarbonIntensityManager",
    "CarbonConfig",
    "HeliumLifecycleManager",
    "HeliumConfig",
    "PredictiveLifecycleAnalyzer",
    "PredictiveConfig",
    "MLComponentSelector",
    "MLConfig",
    "FederatedCircularManager",
    "FederatedConfig",
    "HumanAICollaborativeCircular",
    "CircularComputingPersistenceManager",
    "PersistenceConfig",
    "CircularStorage",
    "CircularAnalyzer",
    "CircularReporter",
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
    "HardwareComponent",
    "MOPDPlan",
    "MOPDConfig",
    # Enums
    "HardwareState",
    "MaterialType",
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
    # Constants
    "MOPD_OBJECTIVE_NAMES",
    "MOPD_HIGHER_IS_BETTER",
    "DEFAULT_RECOVERY_RATES",
    "STATE_SCHEMA_VERSION",
]
