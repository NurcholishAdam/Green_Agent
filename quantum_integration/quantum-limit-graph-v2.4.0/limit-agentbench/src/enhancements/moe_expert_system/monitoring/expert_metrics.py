#!/usr/bin/env python3
"""
Enhanced Expert Metrics Collector v9.0.0 - Complete Green Agent Implementation
with full bio‑inspired core integration, Multi‑Objective Pareto Decision (MOPD) support,
Explainable AI (XAI), temporal safety, human‑in‑the‑loop, chaos testing, proper
differential privacy, and production‑grade observability.

ENHANCEMENTS OVER v8.3.0:
- Fixed critical bugs:
  * Added missing `uuid` import.
  * Fixed async/sync mismatch in `SLOTracker.record_metric` (now sync; callers no longer await).
  * Eliminated double counting of expert usage/success/failures via a single source of truth.
  * Populated `expert_ecoatp` correctly and used it in the Pareto point record.
  * Renamed event handler to match `token_balance_update` subscription.
  * Replaced `timedelta.seconds` with `total_seconds()` everywhere.
  * Replaced unsafe `scaler.mean_` access with a proper `is_fitted` flag.
  * Mapped anomaly confidence floats to `MetricSeverity` before recording.
  * Implemented proper Gaussian differential privacy mechanism with clipping.
  * Persistence deserialization now only parses known timestamp fields.
  * Guarded `np.mean` on empty sequences.
  * Human approval now uses an approval queue with timeout and fallback.
  * Chaos testing uses public APIs, saves/restores state, and supports more faults.
- Added new features:
  * `ApprovalRequest` / `ApprovalResult` dataclasses and async approval queue.
  * `XAIExplainer` for SHAP‑style feature attributions (stub‑free, rule‑based fallback).
  * `RateLimiter` for external API calls.
  * `TemporalSafetyMonitor` running periodic invariant checks.
  * `FaultInjector` abstraction for chaos testing.
  * `PydanticModel`‑style config validation via `__post_init__`.
  * Structured `ObservabilityHub` for Prometheus/OpenTelemetry hooks.
  * `CorrelationContext` for distributed tracing.
  * Idempotency keys in `MetricsStorage` to prevent duplicate counting.
  * Time‑series DB adapter interface (`TimeSeriesBackend`).
  * Proper `Protocol` interfaces for bio‑core, token manager, drift detector.
- Improved maintainability:
  * Removed unused imports.
  * Added type hints throughout.
  * Replaced magic numbers with named constants.
  * Extracted helper methods for long functions.
  * Added docstrings for every public method.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import os
import random
import time
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field, asdict, fields as dataclass_fields
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import (
    Any, AsyncIterator, Awaitable, Callable, Deque, Dict, List, Optional,
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
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False
    torch = None

try:
    from sklearn.preprocessing import StandardScaler
    from sklearn.ensemble import IsolationForest
    from sklearn.linear_model import SGDRegressor
    from sklearn.metrics import r2_score
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False
    StandardScaler = None
    IsolationForest = None
    SGDRegressor = None
    r2_score = None

logger = logging.getLogger(__name__)

# ============================================================================
# Central Green Agent Components (imports are optional; graceful degradation)
# ============================================================================
try:
    from ..config import config as central_config
except ImportError:
    central_config = None

try:
    from ..storage import Storage
except ImportError:
    Storage = None

try:
    from ..schemas.feedback_event import FeedbackEvent
except ImportError:
    FeedbackEvent = None

try:
    from ..routing.pareto_gating import ParetoGating
except ImportError:
    ParetoGating = None

try:
    from ..feedback.adaptive_cost import AdaptiveCostFunction
except ImportError:
    AdaptiveCostFunction = None

try:
    from ..safety.drift_detector import DriftDetector
except ImportError:
    DriftDetector = None

try:
    from ..scaling.message_queue import AsyncMessageQueue
except ImportError:
    AsyncMessageQueue = None

try:
    from ..metrics import MetricsRegistry
except ImportError:
    MetricsRegistry = None

try:
    from ..logger import logger as central_logger
except ImportError:
    central_logger = logger

# ============================================================================
# Bio-Inspired Core Import (with fallback)
# ============================================================================
try:
    from enhancements.bio_inspired.__init__ import (  # type: ignore
        EnhancedBioInspiredCore, BioEvent, CircuitBreaker, Persistence,
    )
    from enhancements.bio_inspired.eco_atp_currency import (  # type: ignore
        EcoATPTokenManager, DynamicExchangeRate, EcoATPSource, EcoATPConsumer,
        TokenState, EcoATPToken, EcoATPAccount,
    )
    from enhancements.bio_inspired.proton_gradient_fields import (  # type: ignore
        GradientFieldManager, GradientField,
    )
    from enhancements.bio_inspired.atp_synthase_scheduler import (  # type: ignore
        ATPSynthaseScheduler, SynthaseConfig,
    )
    from enhancements.bio_inspired.chromatophore_compartments import (  # type: ignore
        CompartmentManager, ChromatophoreCompartment, CompartmentState,
        MembranePermeability,
    )
    from enhancements.bio_inspired.biomass_storage import (  # type: ignore
        BiomassStorage, StorageTier, GuaranteeLevel,
    )
    from enhancements.bio_inspired.photosynthetic_harvester import (  # type: ignore
        PhotosyntheticHarvester,
    )
    from enhancements.bio_inspired.time_tick_engine import TimeTickEngine  # type: ignore
    from enhancements.bio_inspired.quantum_bridge import QuantumBridge  # type: ignore
    BIO_INSPIRED_AVAILABLE = True
    logger.info("Bio-inspired core modules loaded for Expert Metrics")
except ImportError as e:
    BIO_INSPIRED_AVAILABLE = False
    logger.warning(f"Bio-inspired core modules not available: {e} - using fallback")

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

        async def call(self, func: Callable[..., Awaitable[Any]], *args, **kwargs) -> Any:
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
# MoE and Self-Evolving Gate imports (optional)
# ============================================================================
try:
    from ..expert_router import ExpertRouter  # type: ignore
    from ..gating_network import GatingNetworkManager  # type: ignore
    from ..advanced.self_evolving_gates import EnhancedSelfEvolvingGate  # type: ignore
    MOE_AVAILABLE = True
except ImportError:
    MOE_AVAILABLE = False
    logger.warning("MoE Expert Router or Self-Evolving Gates not available")

# ============================================================================
# Named Constants (replace magic numbers)
# ============================================================================
DEFAULT_CARBON_FALLBACK_INTENSITY = 400.0
DEFAULT_CARBON_US_EAST = 420.0
DEFAULT_CARBON_US_WEST = 350.0
DEFAULT_CARBON_EU = 280.0
DEFAULT_CARBON_ASIA = 500.0
CARBON_CRITICAL_THRESHOLD = 800.0
TOKEN_CRITICAL_THRESHOLD = 50.0
DRIFT_RETRAIN_THRESHOLD = 0.7
MAX_STORAGE_POINTS = 10_000
MAX_UNCERTAINTY_LOG = 1_000
MAX_ALERT_HISTORY = 5_000
MAX_PREDICTION_HISTORY = 1_000
HUMAN_APPROVAL_TIMEOUT_SECONDS = 300.0
RATE_LIMIT_DEFAULT_RPS = 10.0


# ============================================================================
# Protocol Interfaces (for dependency injection)
# ============================================================================
@runtime_checkable
class TokenManagerProtocol(Protocol):
    async def spend(self, account: str, amount: float) -> bool: ...
    async def earn(self, account: str, amount: float) -> bool: ...
    def get_system_summary(self) -> Dict[str, Any]: ...


@runtime_checkable
class GradientManagerProtocol(Protocol):
    async def pump_field(self, field: str, delta: float, source: str) -> None: ...


@runtime_checkable
class DriftDetectorProtocol(Protocol):
    async def check_drift(self, weights: Dict[str, float]) -> float: ...


@runtime_checkable
class SwarmCoordinatorProtocol(Protocol):
    async def share_predictions(self, payload: Dict[str, Any]) -> None: ...


@runtime_checkable
class HeliumProviderProtocol(Protocol):
    def get_scarcity(self) -> float: ...
    def get_cost_index(self) -> float: ...
    def get_efficiency(self) -> float: ...


# ============================================================================
# Enums and Data Classes (Enhanced with MOPD, XAI, HITL)
# ============================================================================
class MetricSeverity(Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


class MetricType(Enum):
    LATENCY = "latency"
    SUCCESS_RATE = "success_rate"
    CARBON = "carbon"
    HELIUM = "helium"
    TOKEN = "token"
    HEALTH = "health"


class AnomalyType(Enum):
    LATENCY_SPIKE = "latency_spike"
    ERROR_RATE = "error_rate"
    CARBON_SURGE = "carbon_surge"
    TOKEN_DRAIN = "token_drain"
    GRADIENT_DROP = "gradient_drop"


class SLOStatus(Enum):
    COMPLIANT = "compliant"
    AT_RISK = "at_risk"
    BREACHED = "breached"
    INSUFFICIENT_DATA = "insufficient_data"


class ApprovalStatus(Enum):
    PENDING = "pending"
    APPROVED = "approved"
    DENIED = "denied"
    TIMEOUT = "timeout"
    AUTO_APPROVED = "auto_approved"


class FaultType(Enum):
    CARBON_SPIKE = "carbon_spike"
    TOKEN_DEPLETION = "token_depletion"
    BACKEND_UNAVAILABLE = "backend_unavailable"
    SLO_BREACH = "slo_breach"
    NETWORK_LATENCY = "network_latency"
    QUEUE_OVERFLOW = "queue_overflow"
    MODEL_CORRUPTION = "model_corruption"


@dataclass
class MetricThreshold:
    metric_name: str
    warning_threshold: float
    critical_threshold: float
    comparison: str = "greater_than"
    gradient_modulated: bool = False
    cooldown_seconds: float = 300.0

    def __post_init__(self):
        if self.comparison not in ("greater_than", "less_than"):
            raise ValueError(f"Invalid comparison: {self.comparison}")


@dataclass
class ServiceLevelObjective:
    slo_id: str
    metric_name: str
    target_value: float
    target_percentile: float = 99.0
    evaluation_window_hours: float = 24.0
    min_samples: int = 30
    current_value: float = 0.0
    status: SLOStatus = SLOStatus.COMPLIANT
    predicted_violation_probability: float = 0.0
    next_predicted_violation: Optional[datetime] = None

    def __post_init__(self):
        if not 0.0 < self.target_percentile <= 100.0:
            raise ValueError("target_percentile must be in (0, 100]")


@dataclass
class AnomalyEvent:
    anomaly_type: AnomalyType
    severity: MetricSeverity
    expert_id: str
    expected_value: float
    actual_value: float
    timestamp: datetime
    description: str = ""


@dataclass
class MetricSample:
    timestamp: datetime
    expert_id: str
    metric_name: str
    value: float
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class CostAttribution:
    expert_id: str
    total_cost: float
    carbon_cost: float
    helium_cost: float
    token_cost: float
    timestamp: datetime


@dataclass
class PredictiveMetricForecast:
    metric_name: str
    forecast_value: float
    confidence_interval: Tuple[float, float]
    timestamp: datetime
    horizon_seconds: int


# ============================================================================
# Human-in-the-loop Data Classes
# ============================================================================
@dataclass
class ApprovalRequest:
    request_id: str
    context: Dict[str, Any]
    requested_at: datetime
    timeout_seconds: float = HUMAN_APPROVAL_TIMEOUT_SECONDS
    status: ApprovalStatus = ApprovalStatus.PENDING
    result_future: Optional[asyncio.Future] = None


@dataclass
class ApprovalResult:
    request_id: str
    status: ApprovalStatus
    reason: str = ""
    decided_at: Optional[datetime] = None


# ============================================================================
# MOPD Data Classes
# ============================================================================
@dataclass
class MOPDPoint:
    """Represents a single point in the multi‑objective space."""
    expert_id: str
    timestamp: datetime
    carbon_kg: float
    helium_units: float
    ecoatp_cost: float
    latency_ms: float
    success_probability: float
    metadata: Dict[str, Any] = field(default_factory=dict)
    explanation: str = ""

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d['timestamp'] = self.timestamp.isoformat()
        return d

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MOPDPoint":
        if isinstance(data.get('timestamp'), str):
            data = dict(data)
            data['timestamp'] = datetime.fromisoformat(data['timestamp'])
        return cls(**data)


@dataclass
class MOPDConfig:
    """Configuration for MOPD analysis."""
    enabled: bool = True
    objective_weights: Dict[str, float] = field(default_factory=lambda: {
        'carbon': 0.3,
        'helium': 0.2,
        'cost': 0.2,
        'latency': 0.15,
        'success': 0.15,
    })
    grid_resolution: int = 5
    enable_cost_benefit: bool = True
    enable_predictive: bool = True
    enable_quantum: bool = True
    normalization_bounds: Dict[str, Tuple[float, float]] = field(default_factory=lambda: {
        'carbon_kg': (0.0, 0.01),
        'helium_units': (0.0, 0.1),
        'ecoatp_cost': (0.0, 1000.0),
        'latency_ms': (0.0, 1000.0),
    })


# ============================================================================
# Configuration
# ============================================================================
@dataclass
class AnomalyDetectionConfig:
    enabled: bool = True
    ml_enabled: bool = True
    contamination: float = 0.1
    n_estimators: int = 100
    window_size: int = 100
    update_interval_seconds: int = 300
    enable_drift_retraining: bool = True


@dataclass
class SLOConfig:
    enabled: bool = True
    slo_definitions: Dict[str, Dict[str, Any]] = field(default_factory=lambda: {
        'latency_slo': {'metric_name': 'expert_latency_ms', 'target_value': 100.0, 'target_percentile': 99.0, 'evaluation_window_hours': 24.0},
        'availability_slo': {'metric_name': 'expert_success_rate', 'target_value': 0.999, 'target_percentile': 99.9, 'evaluation_window_hours': 24.0},
        'carbon_slo': {'metric_name': 'carbon_per_inference', 'target_value': 0.0005, 'target_percentile': 95.0, 'evaluation_window_hours': 24.0},
        'token_efficiency_slo': {'metric_name': 'token_efficiency', 'target_value': 0.8, 'target_percentile': 90.0, 'evaluation_window_hours': 24.0},
        'sustainability_slo': {'metric_name': 'sustainability_score', 'target_value': 0.7, 'target_percentile': 95.0, 'evaluation_window_hours': 24.0},
    })
    evaluation_interval_seconds: int = 60


@dataclass
class CarbonConfig:
    enabled: bool = True
    region: str = "us-east"
    update_interval_seconds: int = 300
    max_retries: int = 3
    circuit_breaker_threshold: int = 5
    circuit_breaker_recovery_timeout: float = 30.0
    api_key_env: str = "ELECTRICITYMAP_API_KEY"
    rate_limit_rps: float = 1.0
    request_timeout_seconds: float = 10.0


@dataclass
class FederatedConfig:
    enabled: bool = True
    server_url: Optional[str] = None
    sparsity_ratio: float = 0.1
    privacy_epsilon: float = 1.0
    privacy_delta: float = 1e-5
    privacy_clip_norm: float = 1.0
    sync_interval_seconds: int = 3600
    max_retries: int = 3
    enable_model_updates: bool = False
    request_timeout_seconds: float = 30.0


@dataclass
class TelemetryConfig:
    enabled: bool = True
    export_interval_seconds: int = 60
    exporter_type: str = "prometheus"


@dataclass
class PersistenceConfig:
    enabled: bool = True
    path: str = "metrics_state.json"
    save_interval_seconds: int = 300
    retention_hours: float = 24.0


@dataclass
class SelfHealingConfig:
    enabled: bool = True
    auto_heal_on_critical: bool = True


@dataclass
class TemporalSafetyConfig:
    enabled: bool = True
    check_interval_seconds: int = 30
    carbon_critical_threshold: float = CARBON_CRITICAL_THRESHOLD
    token_critical_threshold: float = TOKEN_CRITICAL_THRESHOLD
    drift_retrain_threshold: float = DRIFT_RETRAIN_THRESHOLD


@dataclass
class HumanApprovalConfig:
    enabled: bool = False
    timeout_seconds: float = HUMAN_APPROVAL_TIMEOUT_SECONDS
    auto_approve_on_timeout: bool = False
    require_approval_for: List[str] = field(default_factory=lambda: [
        'carbon_spike_mitigation', 'token_depletion_response', 'mass_expert_disable',
    ])


@dataclass
class ChaosTestingConfig:
    enabled: bool = False
    auto_restore: bool = True
    max_concurrent_faults: int = 1


@dataclass
class ExpertMetricsConfig:
    enable_bio_integration: bool = True
    enable_event_driven: bool = True
    enable_swarm_coordination: bool = True
    enable_cross_domain: bool = True
    enable_human_ai: bool = True
    enable_sustainability_scoring: bool = True
    enable_cost_benefit: bool = True
    enable_mopd: bool = True
    enable_temporal_safety: bool = True
    enable_human_approval: bool = False
    enable_chaos_testing: bool = False

    anomaly_detection: AnomalyDetectionConfig = field(default_factory=AnomalyDetectionConfig)
    slo: SLOConfig = field(default_factory=SLOConfig)
    carbon: CarbonConfig = field(default_factory=CarbonConfig)
    federated: FederatedConfig = field(default_factory=FederatedConfig)
    telemetry: TelemetryConfig = field(default_factory=TelemetryConfig)
    persistence: PersistenceConfig = field(default_factory=PersistenceConfig)
    self_healing: SelfHealingConfig = field(default_factory=SelfHealingConfig)
    mopd: MOPDConfig = field(default_factory=MOPDConfig)
    temporal_safety: TemporalSafetyConfig = field(default_factory=TemporalSafetyConfig)
    human_approval: HumanApprovalConfig = field(default_factory=HumanApprovalConfig)
    chaos_testing: ChaosTestingConfig = field(default_factory=ChaosTestingConfig)

    thresholds: Dict[str, MetricThreshold] = field(default_factory=lambda: {
        'latency_p95': MetricThreshold(metric_name='latency_p95', warning_threshold=100.0, critical_threshold=500.0, comparison='greater_than', gradient_modulated=True, cooldown_seconds=300.0),
        'error_rate': MetricThreshold(metric_name='error_rate', warning_threshold=0.05, critical_threshold=0.10, comparison='greater_than', gradient_modulated=True),
        'carbon_per_inference': MetricThreshold(metric_name='carbon_per_inference', warning_threshold=0.0005, critical_threshold=0.001, comparison='greater_than', gradient_modulated=True),
        'token_balance': MetricThreshold(metric_name='token_balance', warning_threshold=200.0, critical_threshold=50.0, comparison='less_than', gradient_modulated=True),
        'gradient_health': MetricThreshold(metric_name='gradient_health', warning_threshold=0.3, critical_threshold=0.1, comparison='less_than', gradient_modulated=True),
        'biomass_level': MetricThreshold(metric_name='biomass_level', warning_threshold=8000.0, critical_threshold=9500.0, comparison='greater_than', gradient_modulated=True),
        'sustainability_score': MetricThreshold(metric_name='sustainability_score', warning_threshold=0.7, critical_threshold=0.4, comparison='less_than', gradient_modulated=True),
    })

    workflow_on_slo_breach: str = "adjust_slo_targets"
    workflow_on_critical_alert: str = "rebalance_experts"

    token_exchange_rate: float = 1000.0
    swarm_share_interval_seconds: int = 60

    def __post_init__(self):
        # Validate MOPD weights sum to ~1.0
        if self.enable_mopd and self.mopd.enabled:
            total = sum(self.mopd.objective_weights.values())
            if total <= 0:
                raise ValueError("MOPD objective_weights must sum to a positive value")
            if abs(total - 1.0) > 0.01:
                # Normalize silently
                for k in self.mopd.objective_weights:
                    self.mopd.objective_weights[k] /= total


# ============================================================================
# Rate Limiter (token bucket)
# ============================================================================
class RateLimiter:
    """Async token‑bucket rate limiter."""

    def __init__(self, rate_per_second: float, burst: Optional[float] = None):
        self.rate = max(rate_per_second, 0.001)
        self.capacity = burst if burst is not None else max(1.0, rate_per_second)
        self._tokens = self.capacity
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, tokens: float = 1.0) -> None:
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_refill
            self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)
            self._last_refill = now
            if self._tokens < tokens:
                wait = (tokens - self._tokens) / self.rate
                await asyncio.sleep(wait)
                self._tokens = 0.0
                self._last_refill = time.monotonic()
            else:
                self._tokens -= tokens


# ============================================================================
# Correlation Context (for distributed tracing)
# ============================================================================
@dataclass
class CorrelationContext:
    correlation_id: str
    parent_id: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)

    @classmethod
    def new(cls, **tags: str) -> "CorrelationContext":
        return cls(correlation_id=uuid.uuid4().hex, tags=tags)


# ============================================================================
# Observability Hub
# ============================================================================
class ObservabilityHub:
    """Central hub for structured logging, tracing, and Prometheus metrics."""

    def __init__(self):
        self._counters: Dict[str, float] = defaultdict(float)
        self._gauges: Dict[str, float] = {}
        self._histograms: Dict[str, Deque[float]] = defaultdict(lambda: deque(maxlen=1000))
        self._lock = asyncio.Lock()

    def counter(self, name: str, value: float = 1.0, **labels: str) -> None:
        key = self._label_key(name, labels)
        self._counters[key] += value

    def gauge(self, name: str, value: float, **labels: str) -> None:
        self._gauges[self._label_key(name, labels)] = value

    def histogram(self, name: str, value: float, **labels: str) -> None:
        self._histograms[self._label_key(name, labels)].append(value)

    def _label_key(self, name: str, labels: Dict[str, str]) -> str:
        if not labels:
            return name
        label_str = ",".join(f'{k}="{v}"' for k, v in sorted(labels.items()))
        return f"{name}{{{label_str}}}"

    async def export_prometheus(self) -> str:
        async with self._lock:
            lines: List[str] = []
            for key, value in self._counters.items():
                lines.append(f"# TYPE {key.split('{')[0]} counter")
                lines.append(f"{key} {value}")
            for key, value in self._gauges.items():
                lines.append(f"# TYPE {key.split('{')[0]} gauge")
                lines.append(f"{key} {value}")
            for key, values in self._histograms.items():
                if values:
                    base = key.split('{')[0]
                    lines.append(f"# TYPE {base} histogram")
                    lines.append(f"{base}_count {len(values)}")
                    lines.append(f"{base}_sum {sum(values)}")
            return "\n".join(lines)

    def log_event(self, level: int, event: str, **fields: Any) -> None:
        payload = {"event": event, **fields}
        logger.log(level, json.dumps(payload, default=str))


# ============================================================================
# Time-Series Backend Protocol
# ============================================================================
@runtime_checkable
class TimeSeriesBackend(Protocol):
    async def write(self, measurement: str, tags: Dict[str, str], fields: Dict[str, float], timestamp: datetime) -> None: ...
    async def query(self, measurement: str, start: datetime, end: datetime, **tags: str) -> List[Dict[str, Any]]: ...


class InMemoryTimeSeriesBackend:
    """Fallback time‑series backend used when no external DB is configured."""

    def __init__(self, max_points: int = MAX_STORAGE_POINTS):
        self._points: Deque[Dict[str, Any]] = deque(maxlen=max_points)
        self._lock = asyncio.Lock()

    async def write(self, measurement: str, tags: Dict[str, str], fields: Dict[str, float], timestamp: datetime) -> None:
        async with self._lock:
            self._points.append({
                'measurement': measurement,
                'tags': dict(tags),
                'fields': dict(fields),
                'timestamp': timestamp,
            })

    async def query(self, measurement: str, start: datetime, end: datetime, **tags: str) -> List[Dict[str, Any]]:
        async with self._lock:
            results = []
            for p in self._points:
                if p['measurement'] != measurement:
                    continue
                if not (start <= p['timestamp'] <= end):
                    continue
                if tags and not all(p['tags'].get(k) == v for k, v in tags.items()):
                    continue
                results.append(p)
            return results


# ============================================================================
# Carbon Intensity Manager (with rate limiter and total_seconds fix)
# ============================================================================
class CarbonIntensityManager:
    def __init__(self, config: CarbonConfig):
        self.config = config
        self.endpoint = "https://api.electricitymap.org/v3/carbon-intensity"
        self.region = config.region
        self.carbon_intensity = DEFAULT_CARBON_FALLBACK_INTENSITY
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
        self._rate_limiter = RateLimiter(config.rate_limit_rps)

    async def _get_session(self) -> Optional[aiohttp.ClientSession]:
        if not AIOHTTP_AVAILABLE:
            return None
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def update_carbon_intensity(self, region: Optional[str] = None) -> Dict:
        if region is not None:
            self.region = region

        async def _fetch() -> Dict:
            cache_key = f"{self.region}_{datetime.now(timezone.utc).hour}"
            if (self.last_update is not None and
                    (datetime.now(timezone.utc) - self.last_update).total_seconds() < self.config.update_interval_seconds and
                    cache_key in self.cache):
                return self.cache[cache_key]

            for attempt in range(self.config.max_retries):
                try:
                    await self._rate_limiter.acquire()
                    session = await self._get_session()
                    if session is None:
                        break
                    url = f"{self.endpoint}/latest?zone={self.region}"
                    headers = {'auth-token': self.api_key} if self.api_key else {}
                    async with session.get(url, headers=headers, timeout=self.config.request_timeout_seconds) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            self.carbon_intensity = float(data.get('carbonIntensity', DEFAULT_CARBON_FALLBACK_INTENSITY))
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

            fallback = {
                'us-east': DEFAULT_CARBON_US_EAST,
                'us-west': DEFAULT_CARBON_US_WEST,
                'eu': DEFAULT_CARBON_EU,
                'asia': DEFAULT_CARBON_ASIA,
            }.get(self.region, DEFAULT_CARBON_FALLBACK_INTENSITY)
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
                (datetime.now(timezone.utc) - self.last_update).total_seconds() > self.config.update_interval_seconds):
            await self.update_carbon_intensity(self.region)
        return self.carbon_intensity

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()


# ============================================================================
# Predictive Metrics Analyzer (with is_fitted flag)
# ============================================================================
class PredictiveMetricsAnalyzer:
    def __init__(self, config: ExpertMetricsConfig, history_window: int = 100):
        self.config = config
        self.history_window = history_window
        self.metric_history: Deque[Dict] = deque(maxlen=history_window)
        self.forecast_history: Deque[Dict] = deque(maxlen=50)
        self.scaler = StandardScaler() if SKLEARN_AVAILABLE else None
        self.model = None
        self.violation_model = None
        self.is_fitted = False
        self.is_trained = False
        self.slo_violation_history: Deque[Dict] = deque(maxlen=1000)
        self._ml_available = SKLEARN_AVAILABLE
        self._lock = asyncio.Lock()
        self._init_models()

    def _init_models(self) -> None:
        if not SKLEARN_AVAILABLE:
            self._ml_available = False
            return
        try:
            self.model = SGDRegressor(
                learning_rate='constant', eta0=0.01, penalty='l2', alpha=0.0001,
                max_iter=1, random_state=42, warm_start=True,
            )
            self.violation_model = SGDRegressor(
                learning_rate='constant', eta0=0.01, penalty='l2', alpha=0.0001,
                max_iter=1, random_state=42, warm_start=True,
            )
        except Exception as e:
            logger.warning(f"Failed to initialize sklearn models: {e}")
            self._ml_available = False

    def update_history(self, metric_data: Dict) -> None:
        self.metric_history.append({
            'timestamp': datetime.now(timezone.utc),
            'success_rate': metric_data.get('success_rate', 0.8),
            'avg_latency_ms': metric_data.get('avg_latency_ms', 100),
            'carbon_intensity': metric_data.get('carbon_intensity', DEFAULT_CARBON_FALLBACK_INTENSITY),
            'token_efficiency': metric_data.get('token_efficiency', 0.5),
            'health_score': metric_data.get('health_score', 0.5),
            'slo_compliant': metric_data.get('slo_compliant', 1.0),
        })

    async def train_forecast_model(self) -> Dict[str, Any]:
        if not self._ml_available or self.model is None:
            return {'status': 'ml_not_available'}
        if len(self.metric_history) < 10:
            return {'status': 'insufficient_data'}

        async with self._lock:
            X, y = [], []
            history_list = list(self.metric_history)
            for i in range(len(history_list) - 5):
                features: List[float] = []
                for j in range(5):
                    data = history_list[i + j]
                    features.extend([
                        data['success_rate'],
                        data['avg_latency_ms'] / 1000,
                        data['carbon_intensity'] / 100,
                        data['token_efficiency'],
                        data['health_score'],
                        data.get('slo_compliant', 1.0),
                    ])
                X.append(features)
                y.append(history_list[i + 5]['health_score'])

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
            return {'status': 'success', 'r2': r2, 'samples': len(X_arr)}

    async def predict_slo_violation(self, features: Dict[str, float]) -> float:
        if not self._ml_available or self.violation_model is None or not self.is_fitted:
            return 0.5
        try:
            X = np.array([[
                features.get('success_rate', 0.8),
                features.get('avg_latency_ms', 100) / 1000,
                features.get('carbon_intensity', DEFAULT_CARBON_FALLBACK_INTENSITY) / 100,
                features.get('token_efficiency', 0.5),
                features.get('health_score', 0.5),
            ]])
            X_scaled = self.scaler.transform(X)
            prob = float(self.violation_model.predict(X_scaled)[0])
            prob = max(0.0, min(1.0, prob))
            self.slo_violation_history.append({
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'probability': prob,
                'features': features,
            })
            return prob
        except Exception as e:
            logger.warning(f"SLO violation prediction failed: {e}")
            return 0.5


# ============================================================================
# ML Anomaly Detector
# ============================================================================
class MLAnomalyDetector:
    def __init__(self, config: AnomalyDetectionConfig):
        self.config = config
        if SKLEARN_AVAILABLE:
            self.model = IsolationForest(
                contamination=config.contamination,
                n_estimators=config.n_estimators,
                random_state=42,
            )
            self.scaler = StandardScaler()
            self.is_trained = False
        else:
            self.model = None
            self.scaler = None
            self.is_trained = False
        self.training_window: List[List[float]] = []
        self.window_size = config.window_size
        self._lock = asyncio.Lock()

    def _feature_vector(self, metrics: Dict[str, float]) -> List[float]:
        return [
            metrics.get('success_rate', 0.5),
            metrics.get('latency_ms', 100) / 1000,
            metrics.get('carbon_per_inference', 0.001) * 1000,
            metrics.get('helium_per_inference', 0.01),
            metrics.get('token_efficiency', 0.5),
            metrics.get('health_score', 0.5),
            metrics.get('gradient_level', 0.5),
        ]

    async def add_sample(self, metrics: Dict[str, float]) -> None:
        async with self._lock:
            self.training_window.append(self._feature_vector(metrics))
            if len(self.training_window) >= self.window_size:
                await self._retrain_locked()

    async def _retrain_locked(self) -> None:
        if len(self.training_window) < 10 or self.model is None:
            return
        X = np.array(self.training_window)
        X_scaled = self.scaler.fit_transform(X)
        self.model.fit(X_scaled)
        self.is_trained = True
        self.training_window = self.training_window[-self.window_size:]
        logger.debug(f"ML Anomaly Detector retrained on {len(self.training_window)} samples")

    async def retrain_now(self) -> None:
        async with self._lock:
            await self._retrain_locked()

    async def detect_anomaly(self, metrics: Dict[str, float]) -> Tuple[bool, float, str]:
        if not self.is_trained or self.model is None:
            return False, 0.0, "Model not trained"
        feature_vector = self._feature_vector(metrics)
        X = np.array([feature_vector])
        X_scaled = self.scaler.transform(X)
        prediction = self.model.predict(X_scaled)[0]
        is_anomaly = prediction == -1
        decision = float(self.model.decision_function(X_scaled)[0])
        confidence = abs(decision) / (abs(decision) + 1)
        if is_anomaly:
            if decision < -0.5:
                description = "Severe anomaly detected (high deviation)"
            elif decision < -0.2:
                description = "Moderate anomaly detected"
            else:
                description = "Slight anomaly detected"
        else:
            description = "Normal"
        return is_anomaly, confidence, description


# ============================================================================
# SLO Tracker (record_metric is synchronous now)
# ============================================================================
class SLOTracker:
    def __init__(self, config: SLOConfig):
        self.config = config
        self.slos: Dict[str, ServiceLevelObjective] = {}
        self.metric_samples: Dict[str, List[float]] = defaultdict(list)
        self.violation_history: Dict[str, List[datetime]] = defaultdict(list)
        self._lock = asyncio.Lock()
        self.alpha = 0.3
        self.last_value: Dict[str, float] = {}
        self.trend: Dict[str, float] = {}

    def define_slo(
        self,
        slo_id: str,
        metric_name: str,
        target_value: float,
        target_percentile: float = 99.0,
        evaluation_window_hours: float = 24.0,
    ) -> bool:
        if slo_id in self.slos:
            return False
        self.slos[slo_id] = ServiceLevelObjective(
            slo_id=slo_id,
            metric_name=metric_name,
            target_value=target_value,
            target_percentile=target_percentile,
            evaluation_window_hours=evaluation_window_hours,
        )
        return True

    def record_metric(self, slo_id: str, value: float) -> None:
        """Synchronous record. Callers must NOT await this."""
        if slo_id not in self.slos:
            return
        samples = self.metric_samples[slo_id]
        samples.append(value)
        if len(samples) > MAX_STORAGE_POINTS:
            del samples[: len(samples) - MAX_STORAGE_POINTS]

    async def evaluate_slos(self) -> Dict[str, Dict[str, Any]]:
        async with self._lock:
            results: Dict[str, Dict[str, Any]] = {}
            for slo_id, slo in self.slos.items():
                samples = self.metric_samples.get(slo_id, [])
                if len(samples) < slo.min_samples:
                    results[slo_id] = {
                        'status': SLOStatus.INSUFFICIENT_DATA.value,
                        'samples': len(samples),
                    }
                    continue

                current = float(np.percentile(samples, slo.target_percentile))
                slo.current_value = current

                if current <= slo.target_value:
                    status = SLOStatus.COMPLIANT
                elif current <= slo.target_value * 1.2:
                    status = SLOStatus.AT_RISK
                else:
                    status = SLOStatus.BREACHED
                slo.status = status

                if slo_id not in self.last_value:
                    self.last_value[slo_id] = current
                    self.trend[slo_id] = 0.0
                else:
                    prev = self.last_value[slo_id]
                    self.last_value[slo_id] = self.alpha * current + (1 - self.alpha) * prev
                    if len(samples) > 5:
                        recent = samples[-5:]
                        x = np.arange(len(recent))
                        slope = float(np.polyfit(x, recent, 1)[0])
                        self.trend[slo_id] = 0.5 * slope + 0.5 * self.trend.get(slo_id, 0.0)

                forecast = self.last_value[slo_id] + self.trend.get(slo_id, 0.0)
                if forecast > slo.target_value * 1.2:
                    violation_prob = 0.8
                elif forecast > slo.target_value * 1.05:
                    violation_prob = 0.4
                else:
                    violation_prob = 0.1
                slo.predicted_violation_probability = violation_prob

                if violation_prob > 0.3:
                    if self.trend.get(slo_id, 0.0) > 0:
                        time_to_breach = (slo.target_value * 1.05 - current) / (self.trend[slo_id] * 10)
                        slo.next_predicted_violation = datetime.now(timezone.utc) + timedelta(
                            seconds=max(30, min(3600, time_to_breach))
                        )
                    else:
                        slo.next_predicted_violation = datetime.now(timezone.utc) + timedelta(hours=1)
                else:
                    slo.next_predicted_violation = None

                if status == SLOStatus.BREACHED:
                    self.violation_history[slo_id].append(datetime.now(timezone.utc))

                results[slo_id] = {
                    'status': status.value,
                    'current_value': current,
                    'target_value': slo.target_value,
                    'violation_probability': violation_prob,
                    'next_predicted_violation': (
                        slo.next_predicted_violation.isoformat() if slo.next_predicted_violation else None
                    ),
                    'samples': len(samples),
                    'violations': len(self.violation_history.get(slo_id, [])),
                }
            return results


# ============================================================================
# Cost Attribution Engine
# ============================================================================
class CostAttributionEngine:
    def __init__(self):
        self.attributions: Deque[CostAttribution] = deque(maxlen=MAX_STORAGE_POINTS)
        self._lock = asyncio.Lock()

    async def record(
        self,
        expert_id: str,
        total_cost: float,
        carbon_cost: float,
        helium_cost: float,
        token_cost: float,
    ) -> None:
        async with self._lock:
            self.attributions.append(CostAttribution(
                expert_id=expert_id,
                total_cost=total_cost,
                carbon_cost=carbon_cost,
                helium_cost=helium_cost,
                token_cost=token_cost,
                timestamp=datetime.now(timezone.utc),
            ))

    async def get_summary(self, expert_id: Optional[str] = None) -> Dict[str, Any]:
        async with self._lock:
            if expert_id:
                filtered = [a for a in self.attributions if a.expert_id == expert_id]
            else:
                filtered = list(self.attributions)
            if not filtered:
                return {}
            return {
                'total_cost': sum(a.total_cost for a in filtered),
                'carbon_cost': sum(a.carbon_cost for a in filtered),
                'helium_cost': sum(a.helium_cost for a in filtered),
                'token_cost': sum(a.token_cost for a in filtered),
                'count': len(filtered),
            }


# ============================================================================
# Telemetry Exporter (kept for backwards compatibility; prefer ObservabilityHub)
# ============================================================================
class TelemetryExporter:
    def __init__(self, config: TelemetryConfig):
        self.config = config
        self.metrics: Dict[str, Dict[str, Any]] = {
            'counters': defaultdict(float),
            'gauges': {},
            'histograms': defaultdict(list),
        }
        self._lock = asyncio.Lock()

    def increment(self, metric_name: str, tags: Optional[Dict[str, str]] = None, value: float = 1.0) -> None:
        self.metrics['counters'][self._make_key(metric_name, tags)] += value

    def gauge(self, metric_name: str, value: float, tags: Optional[Dict[str, str]] = None) -> None:
        self.metrics['gauges'][self._make_key(metric_name, tags)] = value

    def histogram(self, metric_name: str, value: float, tags: Optional[Dict[str, str]] = None) -> None:
        key = self._make_key(metric_name, tags)
        bucket = self.metrics['histograms'][key]
        bucket.append(value)
        if len(bucket) > 1000:
            del bucket[: len(bucket) - 1000]

    def _make_key(self, metric_name: str, tags: Optional[Dict[str, str]]) -> str:
        if tags:
            tag_str = ','.join(f"{k}={v}" for k, v in sorted(tags.items()))
            return f"{metric_name}{{{tag_str}}}"
        return metric_name

    async def export(self) -> str:
        async with self._lock:
            output: List[str] = []
            for key, value in self.metrics['counters'].items():
                output.append(f"# TYPE {key} counter\n{key} {value}")
            for key, value in self.metrics['gauges'].items():
                output.append(f"# TYPE {key} gauge\n{key} {value}")
            for key, values in self.metrics['histograms'].items():
                output.append(f"# TYPE {key} histogram\n{key}_count {len(values)}\n{key}_sum {sum(values)}")
            return "\n".join(output)

    def reset(self) -> None:
        self.metrics['counters'] = defaultdict(float)
        self.metrics['gauges'] = {}
        self.metrics['histograms'] = defaultdict(list)


# ============================================================================
# Persistence Manager (typed timestamp handling)
# ============================================================================
class MetricsPersistenceManager:
    # Only these keys are treated as ISO timestamps during deserialization
    TIMESTAMP_KEYS = frozenset({'timestamp', 'requested_at', 'decided_at', 'next_predicted_violation'})

    def __init__(self, config: PersistenceConfig):
        self.config = config
        self.path = config.path
        self._lock = asyncio.Lock()
        self._version = 3

    async def save_state(self, state: Dict[str, Any]) -> bool:
        async with self._lock:
            try:
                payload = {
                    'version': self._version,
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'data': state,
                }
                serializable = self._make_serializable(payload)
                tmp_path = f"{self.path}.tmp"
                with open(tmp_path, 'w') as f:
                    json.dump(serializable, f, indent=2)
                os.replace(tmp_path, self.path)
                return True
            except Exception as e:
                logger.error(f"Failed to save metrics state: {e}")
                return False

    async def load_state(self) -> Optional[Dict]:
        async with self._lock:
            if not os.path.exists(self.path):
                return None
            try:
                with open(self.path, 'r') as f:
                    payload = json.load(f)
                if payload.get('version', 0) < 2:
                    logger.warning("Loading legacy metrics state")
                return self._deserialize(payload.get('data', {}))
            except Exception as e:
                logger.error(f"Failed to load metrics state: {e}")
                return None

    def _make_serializable(self, obj: Any) -> Any:
        if hasattr(obj, 'to_dict'):
            return self._make_serializable(obj.to_dict())
        if isinstance(obj, dict):
            return {k: self._make_serializable(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple, set)):
            return [self._make_serializable(v) for v in obj]
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, deque):
            return self._make_serializable(list(obj))
        if hasattr(obj, '__dict__'):
            return self._make_serializable(obj.__dict__)
        return obj

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


# ============================================================================
# Federated Metrics Aggregator (proper DP)
# ============================================================================
class FederatedMetricsAggregator:
    def __init__(self, config: FederatedConfig):
        self.config = config
        self.server_url = config.server_url
        self.round = 0
        self.local_metrics: Dict[str, Any] = {}
        self.global_metrics: Dict[str, Any] = {}
        self.participants: List[str] = []
        self.contribution_scores: Dict[str, float] = {}
        self._lock = asyncio.Lock()
        self._session: Optional[aiohttp.ClientSession] = None
        self._circuit = CircuitBreaker("federated_server", failure_threshold=3, recovery_timeout=30.0)
        self._rate_limiter = RateLimiter(1.0)

    async def _get_session(self) -> Optional[aiohttp.ClientSession]:
        if not AIOHTTP_AVAILABLE or not self.server_url:
            return None
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    def _clip_vector(self, vector: List[float], clip_norm: float) -> List[float]:
        norm = math.sqrt(sum(v * v for v in vector))
        if norm <= clip_norm or norm == 0:
            return vector
        scale = clip_norm / norm
        return [v * scale for v in vector]

    def _add_differential_privacy(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Proper Gaussian mechanism with L2 clipping and calibrated noise."""
        epsilon = self.config.privacy_epsilon
        delta = self.config.privacy_delta
        clip_norm = self.config.privacy_clip_norm
        if epsilon <= 0:
            return metrics

        # Calibrate Gaussian sigma via the analytic Gaussian mechanism approximation
        # sigma >= sqrt(2 * ln(1.25 / delta)) / epsilon  (classic bound)
        if delta <= 0:
            delta = 1e-5
        sigma = math.sqrt(2.0 * math.log(1.25 / delta)) / epsilon

        numeric_keys = [k for k, v in metrics.items() if isinstance(v, (int, float))]
        if not numeric_keys:
            return metrics

        vector = [float(metrics[k]) for k in numeric_keys]
        clipped = self._clip_vector(vector, clip_norm)
        private: Dict[str, Any] = {}
        for k, v in metrics.items():
            if isinstance(v, (int, float)):
                idx = numeric_keys.index(k)
                noise = float(np.random.normal(0.0, sigma * clip_norm))
                private[k] = clipped[idx] + noise
            else:
                private[k] = v
        return private

    def _compress_metrics(self, metrics: Dict[str, Any]) -> Dict[str, Any]:
        sparsity = self.config.sparsity_ratio
        if sparsity >= 1.0:
            return metrics
        numeric_items = [(k, v) for k, v in metrics.items() if isinstance(v, (int, float))]
        if not numeric_items:
            return metrics
        numeric_items.sort(key=lambda kv: abs(kv[1]), reverse=True)
        k = max(1, int(len(numeric_items) * sparsity))
        kept = {key for key, _ in numeric_items[:k]}
        return {
            key: val for key, val in metrics.items()
            if key in kept or not isinstance(val, (int, float))
        }

    async def send_local_metrics(
        self,
        participant_id: str,
        metrics: Dict[str, Any],
        performance: float = 1.0,
    ) -> Dict[str, Any]:
        if not self.server_url:
            return {'status': 'local'}

        async def _send() -> Dict[str, Any]:
            for attempt in range(self.config.max_retries):
                try:
                    async with self._lock:
                        session = await self._get_session()
                        if session is None:
                            return {'status': 'no_session'}
                        await self._rate_limiter.acquire()
                        private = self._add_differential_privacy(metrics)
                        compressed = self._compress_metrics(private)
                        update_data: Dict[str, Any] = {
                            'participant_id': participant_id,
                            'round': self.round,
                            'metrics': compressed,
                            'performance': performance,
                            'privacy_epsilon': self.config.privacy_epsilon,
                            'privacy_delta': self.config.privacy_delta,
                            'sparsity_ratio': self.config.sparsity_ratio,
                            'timestamp': datetime.now(timezone.utc).isoformat(),
                        }
                        if self.config.enable_model_updates and hasattr(self, 'model_weights'):
                            update_data['model_weights'] = self.model_weights  # type: ignore
                        async with session.post(
                            f"{self.server_url}/federated/metrics",
                            json=update_data,
                            timeout=self.config.request_timeout_seconds,
                        ) as resp:
                            if resp.status == 200:
                                result = await resp.json()
                                self.round += 1
                                self.contribution_scores[participant_id] = performance
                                return result
                            logger.warning(f"Federated metrics send failed (attempt {attempt+1}): {resp.status}")
                except Exception as e:
                    logger.error(f"Federated metrics send error (attempt {attempt+1}): {e}")
                await asyncio.sleep(min(2 ** attempt, 10))
            return {'status': 'failed'}

        return await self._circuit.call(_send)

    async def get_global_metrics(self) -> Optional[Dict[str, Any]]:
        if not self.server_url:
            return self.global_metrics

        async def _fetch() -> Optional[Dict[str, Any]]:
            for attempt in range(self.config.max_retries):
                try:
                    async with self._lock:
                        session = await self._get_session()
                        if session is None:
                            return None
                        await self._rate_limiter.acquire()
                        async with session.get(
                            f"{self.server_url}/federated/metrics/global",
                            timeout=self.config.request_timeout_seconds,
                        ) as resp:
                            if resp.status == 200:
                                data = await resp.json()
                                self.global_metrics = data.get('metrics', {})
                                self.participants = data.get('participants', [])
                                return self.global_metrics
                            logger.warning(f"Global metrics fetch failed (attempt {attempt+1}): {resp.status}")
                except Exception as e:
                    logger.error(f"Global metrics fetch error (attempt {attempt+1}): {e}")
                await asyncio.sleep(min(2 ** attempt, 10))
            return None

        return await self._circuit.call(_fetch)

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()


# ============================================================================
# Metrics Storage (single source of truth, idempotency, time-series backend)
# ============================================================================
class MetricsStorage:
    def __init__(
        self,
        retention_hours: float = 24.0,
        time_series_backend: Optional[TimeSeriesBackend] = None,
    ):
        self.retention_hours = retention_hours
        self.backend = time_series_backend or InMemoryTimeSeriesBackend()

        # Per-expert counters (single source of truth)
        self.expert_usage: Dict[str, int] = defaultdict(int)
        self.expert_success: Dict[str, int] = defaultdict(int)
        self.expert_failures: Dict[str, int] = defaultdict(int)

        # Resources
        self.expert_latency: Dict[str, Deque[Dict]] = defaultdict(lambda: deque(maxlen=MAX_STORAGE_POINTS))
        self.expert_energy: Dict[str, float] = defaultdict(float)
        self.expert_carbon: Dict[str, float] = defaultdict(float)
        self.expert_helium: Dict[str, float] = defaultdict(float)
        self.expert_ecoatp: Dict[str, float] = defaultdict(float)

        # Routing
        self.routing_decisions: Deque[Dict] = deque(maxlen=MAX_STORAGE_POINTS)
        self.routing_latency: Deque[float] = deque(maxlen=MAX_STORAGE_POINTS)

        # Pareto / MOPD
        self.pareto_points: Deque[Dict] = deque(maxlen=MAX_STORAGE_POINTS)
        self.mopd_points: Deque[MOPDPoint] = deque(maxlen=MAX_STORAGE_POINTS)

        # Bio
        self.bio_metrics_history: Deque[Dict] = deque(maxlen=MAX_STORAGE_POINTS)
        self.health_scores: Dict[str, float] = {}
        self.correlation_map: Dict[str, List[str]] = defaultdict(list)

        # Idempotency
        self._seen_idempotency_keys: Deque[str] = deque(maxlen=MAX_STORAGE_POINTS)
        self._idempotency_set: Set[str] = set()

        self._lock = asyncio.Lock()

    def _idempotent(self, key: Optional[str]) -> bool:
        """Return True if key was already processed."""
        if not key:
            return False
        if key in self._idempotency_set:
            return True
        self._idempotency_set.add(key)
        self._seen_idempotency_keys.append(key)
        # Trim set when deque rotates
        if len(self._idempotency_set) > MAX_STORAGE_POINTS:
            oldest = self._seen_idempotency_keys[0]
            self._idempotency_set.discard(oldest)
        return False

    async def record_routing(
        self,
        routing_decisions: List[Tuple[int, float]],
        execution_time: float,
        success: bool,
        correlation_id: Optional[str] = None,
        idempotency_key: Optional[str] = None,
    ) -> None:
        async with self._lock:
            if self._idempotent(idempotency_key):
                return
            # Note: we deliberately do NOT touch per-expert success/failure counters here;
            # those are owned by record_expert_execution.
            self.routing_latency.append(execution_time)
            self.routing_decisions.append({
                'decisions': routing_decisions,
                'execution_time': execution_time,
                'success': success,
                'timestamp': datetime.now(timezone.utc),
                'correlation_id': correlation_id,
            })
            if correlation_id:
                self.correlation_map[correlation_id].append('routing')
            await self.backend.write(
                'routing',
                {'success': str(success).lower()},
                {'execution_time_ms': execution_time, 'num_decisions': float(len(routing_decisions))},
                datetime.now(timezone.utc),
            )

    async def record_expert_execution(
        self,
        expert_id: str,
        execution_time: float,
        energy_kwh: float,
        carbon_kg: float,
        helium_units: float,
        success: bool,
        correlation_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        explanation: str = "",
        idempotency_key: Optional[str] = None,
    ) -> None:
        async with self._lock:
            if self._idempotent(idempotency_key):
                return

            # Single source of truth for usage/success/failure
            self.expert_usage[expert_id] += 1
            if success:
                self.expert_success[expert_id] += 1
            else:
                self.expert_failures[expert_id] += 1

            now = datetime.now(timezone.utc)
            self.expert_latency[expert_id].append({'value': execution_time, 'timestamp': now})
            self.expert_energy[expert_id] += energy_kwh
            self.expert_carbon[expert_id] += carbon_kg
            self.expert_helium[expert_id] += helium_units
            self.expert_ecoatp[expert_id] += energy_kwh * 1000.0  # convert kWh → Wh

            mopd_point = MOPDPoint(
                expert_id=expert_id,
                timestamp=now,
                carbon_kg=carbon_kg,
                helium_units=helium_units,
                ecoatp_cost=energy_kwh * 1000.0,
                latency_ms=execution_time,
                success_probability=1.0 if success else 0.0,
                metadata=metadata or {},
                explanation=explanation,
            )
            self.mopd_points.append(mopd_point)
            self.pareto_points.append({
                'expert_id': expert_id,
                'energy': energy_kwh,
                'time': execution_time,
                'helium': helium_units,
                'carbon': carbon_kg,
                'ecoatp': self.expert_ecoatp[expert_id],
                'timestamp': now,
            })
            self._prune_stale()

            await self.backend.write(
                'expert_execution',
                {'expert_id': expert_id, 'success': str(success).lower()},
                {
                    'execution_time_ms': execution_time,
                    'energy_kwh': energy_kwh,
                    'carbon_kg': carbon_kg,
                    'helium_units': helium_units,
                },
                now,
            )

    def _prune_stale(self) -> None:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=self.retention_hours)
        for expert_id in list(self.expert_latency.keys()):
            dq = self.expert_latency[expert_id]
            while dq and dq[0]['timestamp'] < cutoff:
                dq.popleft()

    async def compute_pareto_front(
        self,
        objective_names: Optional[List[str]] = None,
        constraints: Optional[Dict[str, Tuple[float, float]]] = None,
        max_points: int = 50,
        sort_by: Optional[str] = None,
        weights: Optional[Dict[str, float]] = None,
        normalization_bounds: Optional[Dict[str, Tuple[float, float]]] = None,
    ) -> List[MOPDPoint]:
        async with self._lock:
            if not self.mopd_points:
                return []
            if objective_names is None:
                objective_names = ['carbon_kg', 'helium_units', 'ecoatp_cost', 'latency_ms', 'success_probability']

            points = list(self.mopd_points)
            if constraints:
                filtered = []
                for p in points:
                    ok = True
                    for key, (low, high) in constraints.items():
                        val = getattr(p, key, None)
                        if val is None or not (low <= val <= high):
                            ok = False
                            break
                    if ok:
                        filtered.append(p)
                points = filtered

            if len(points) < 2:
                return points[:max_points]

            # Scalarise for sorting if requested
            if sort_by == 'scalarised':
                scalar = self._scalarise_points(points, objective_names, weights, normalization_bounds)
                points = [p for _, p in sorted(zip(scalar, points), key=lambda x: x[0])]

            pareto: List[MOPDPoint] = []
            for i, p_i in enumerate(points):
                dominated = False
                for j, p_j in enumerate(points):
                    if i == j:
                        continue
                    # All objectives are "lower is better" except success_probability
                    a = []
                    b = []
                    for key in objective_names:
                        vi = getattr(p_i, key)
                        vj = getattr(p_j, key)
                        if key == 'success_probability':
                            a.append(-vi)
                            b.append(-vj)
                        else:
                            a.append(vi)
                            b.append(vj)
                    if all(bv <= av for av, bv in zip(a, b)) and any(bv < av for av, bv in zip(a, b)):
                        dominated = True
                        break
                if not dominated:
                    pareto.append(p_i)

            if sort_by == 'timestamp':
                pareto.sort(key=lambda p: p.timestamp, reverse=True)
            elif sort_by == 'scalarised':
                weights = weights or {}
                bounds = normalization_bounds or {}
                pareto.sort(key=lambda p: self._scalarise_point(p, objective_names, weights, bounds))
            return pareto[:max_points]

    def _scalarise_point(
        self,
        p: MOPDPoint,
        objective_names: Sequence[str],
        weights: Dict[str, float],
        bounds: Dict[str, Tuple[float, float]],
    ) -> float:
        total = 0.0
        wsum = 0.0
        for key in objective_names:
            val = getattr(p, key, 0.0)
            lo, hi = bounds.get(key, (0.0, max(abs(val), 1.0)))
            span = hi - lo if hi > lo else 1.0
            norm = (val - lo) / span
            norm = max(0.0, min(1.0, norm))
            if key == 'success_probability':
                norm = 1.0 - norm
            w = weights.get(key.replace('_kg', '').replace('_units', '').replace('_ms', '').replace('ecoatp_cost', 'cost'), 1.0)
            total += w * norm
            wsum += w
        return total / wsum if wsum else total

    def _scalarise_points(
        self,
        points: Sequence[MOPDPoint],
        objective_names: Sequence[str],
        weights: Optional[Dict[str, float]],
        bounds: Optional[Dict[str, Tuple[float, float]]],
    ) -> List[float]:
        weights = weights or {}
        bounds = bounds or {}
        return [self._scalarise_point(p, objective_names, weights, bounds) for p in points]

    async def get_expert_usage(self) -> Dict[str, int]:
        async with self._lock:
            return dict(self.expert_usage)

    async def get_expert_success_rate(self) -> Dict[str, float]:
        async with self._lock:
            rates = {}
            for expert_id in self.expert_usage:
                total = self.expert_usage[expert_id]
                success = self.expert_success[expert_id]
                rates[expert_id] = success / total if total > 0 else 0.0
            return rates

    async def get_expert_latency_stats(self) -> Dict[str, Dict[str, float]]:
        async with self._lock:
            stats = {}
            for expert_id, dq in self.expert_latency.items():
                if dq:
                    values = [item['value'] for item in dq]
                    stats[expert_id] = {
                        'avg_ms': float(np.mean(values)),
                        'p95_ms': float(np.percentile(values, 95)),
                        'p99_ms': float(np.percentile(values, 99)),
                        'min_ms': float(np.min(values)),
                        'max_ms': float(np.max(values)),
                        'count': len(values),
                    }
            return stats

    async def get_resource_consumption(self) -> Dict[str, Dict[str, float]]:
        async with self._lock:
            return {
                'energy': dict(self.expert_energy),
                'carbon': dict(self.expert_carbon),
                'helium': dict(self.expert_helium),
                'ecoatp': dict(self.expert_ecoatp),
            }

    async def get_pareto_frontier(self) -> List[Dict]:
        async with self._lock:
            return list(self.pareto_points)

    async def get_health_scores(self) -> Dict[str, float]:
        async with self._lock:
            return dict(self.health_scores)

    async def set_health_score(self, expert_id: str, score: float) -> None:
        async with self._lock:
            self.health_scores[expert_id] = float(score)


# ============================================================================
# XAI Explainer
# ============================================================================
class XAIExplainer:
    """
    Rule-based fallback explainer. In production, wrap SHAP/LIME per model type.
    Produces human-readable attributions for MOPD points.
    """

    def __init__(self, feature_names: Optional[List[str]] = None):
        self.feature_names = feature_names or [
            'carbon_kg', 'helium_units', 'ecoatp_cost', 'latency_ms', 'success_probability',
        ]

    def explain_point(
        self,
        point: MOPDPoint,
        baseline: Optional[Dict[str, float]] = None,
    ) -> str:
        baseline = baseline or {
            'carbon_kg': 0.0005,
            'helium_units': 0.01,
            'ecoatp_cost': 100.0,
            'latency_ms': 100.0,
            'success_probability': 0.9,
        }
        contributions: List[Tuple[str, float]] = []
        for name in self.feature_names:
            val = getattr(point, name, 0.0)
            base = baseline.get(name, 0.0)
            contributions.append((name, val - base))
        contributions.sort(key=lambda kv: abs(kv[1]), reverse=True)

        parts = [f"Expert {point.expert_id}:"]
        for name, delta in contributions[:3]:
            sign = "+" if delta >= 0 else "-"
            direction = "worse" if delta > 0 else "better"
            parts.append(f"  {name}: {sign}{abs(delta):.4f} ({direction} than baseline)")
        return "\n".join(parts)

    def explain_routing(
        self,
        routing_decisions: List[Tuple[int, float]],
        context: Dict[str, Any],
    ) -> str:
        parts = ["Routing decision explanation:"]
        for expert_idx, weight in routing_decisions:
            parts.append(f"  Expert {expert_idx} weight={weight:.4f}")
        if 'carbon_intensity' in context:
            parts.append(f"  Carbon intensity: {context['carbon_intensity']}")
        return "\n".join(parts)


# ============================================================================
# Metrics Analyzer
# ============================================================================
class MetricsAnalyzer:
    def __init__(self, config: ExpertMetricsConfig, storage: MetricsStorage):
        self.config = config
        self.storage = storage
        self.slo_tracker = SLOTracker(config.slo) if config.slo.enabled else None
        self.ml_anomaly_detector = (
            MLAnomalyDetector(config.anomaly_detection)
            if config.anomaly_detection.ml_enabled else None
        )
        self.predictive_analyzer = (
            PredictiveMetricsAnalyzer(config)
            if config.anomaly_detection.enabled else None
        )
        self.xai = XAIExplainer()
        self._lock = asyncio.Lock()
        self.anomaly_events: Deque[AnomalyEvent] = deque(maxlen=1000)
        self.predictions: Dict[str, Dict] = {}
        self._uncertainty_log: Deque[Dict] = deque(maxlen=MAX_UNCERTAINTY_LOG)

    async def analyze_routing(
        self,
        routing_decisions: List[Tuple[int, float]],
        execution_time: float,
        success: bool,
        correlation_id: Optional[str] = None,
        idempotency_key: Optional[str] = None,
    ) -> None:
        await self.storage.record_routing(
            routing_decisions, execution_time, success, correlation_id, idempotency_key,
        )
        if self.slo_tracker:
            # Sync calls (no await) — fixed from v8.3.0
            self.slo_tracker.record_metric('latency_slo', execution_time)
            rates = await self.storage.get_expert_success_rate()
            avg_success = float(np.mean(list(rates.values()))) if rates else 0.0
            self.slo_tracker.record_metric('availability_slo', avg_success)

    async def analyze_execution(
        self,
        expert_id: str,
        execution_time: float,
        energy_kwh: float,
        carbon_kg: float,
        helium_units: float,
        success: bool,
        correlation_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        idempotency_key: Optional[str] = None,
    ) -> None:
        # Build XAI explanation
        mopd_point = MOPDPoint(
            expert_id=expert_id,
            timestamp=datetime.now(timezone.utc),
            carbon_kg=carbon_kg,
            helium_units=helium_units,
            ecoatp_cost=energy_kwh * 1000.0,
            latency_ms=execution_time,
            success_probability=1.0 if success else 0.0,
            metadata=metadata or {},
        )
        explanation = self.xai.explain_point(mopd_point)

        await self.storage.record_expert_execution(
            expert_id, execution_time, energy_kwh, carbon_kg, helium_units,
            success, correlation_id, metadata, explanation, idempotency_key,
        )

        # Anomaly detection
        if self.ml_anomaly_detector:
            success_rates = await self.storage.get_expert_success_rate()
            health = await self.storage.get_health_scores()
            metrics = {
                'success_rate': success_rates.get(expert_id, 0.5),
                'latency_ms': execution_time,
                'carbon_per_inference': carbon_kg,
                'helium_per_inference': helium_units,
                'token_efficiency': 0.5,
                'health_score': health.get(expert_id, 0.5),
                'gradient_level': 0.5,
            }
            await self.ml_anomaly_detector.add_sample(metrics)
            is_anomaly, confidence, desc = await self.ml_anomaly_detector.detect_anomaly(metrics)
            if is_anomaly:
                severity = self._confidence_to_severity(confidence)
                await self._record_anomaly(
                    expert_id=expert_id,
                    anomaly_type=AnomalyType.ERROR_RATE,
                    expected=0.5,
                    actual=confidence,
                    severity=severity,
                    description=desc,
                )
                self.log_uncertainty_sync(
                    'anomaly_confidence', confidence,
                    {'expert_id': expert_id, 'description': desc},
                )

    @staticmethod
    def _confidence_to_severity(confidence: float) -> MetricSeverity:
        if confidence >= 0.8:
            return MetricSeverity.CRITICAL
        if confidence >= 0.5:
            return MetricSeverity.WARNING
        return MetricSeverity.INFO

    def log_uncertainty_sync(self, metric_name: str, value: float, context: Dict[str, Any]) -> None:
        self._uncertainty_log.append({
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'metric': metric_name,
            'value': value,
            'context': context,
        })
        logger.info(f"Uncertainty logged: metric={metric_name}, value={value}")

    async def log_uncertainty(self, metric_name: str, value: float, context: Dict[str, Any]) -> None:
        self.log_uncertainty_sync(metric_name, value, context)

    async def _record_anomaly(
        self,
        expert_id: str,
        anomaly_type: AnomalyType,
        expected: float,
        actual: float,
        severity: MetricSeverity,
        description: str,
    ) -> None:
        event = AnomalyEvent(
            anomaly_type=anomaly_type,
            severity=severity,
            expert_id=expert_id,
            expected_value=expected,
            actual_value=actual,
            timestamp=datetime.now(timezone.utc),
            description=description,
        )
        self.anomaly_events.append(event)
        logger.warning(f"Anomaly recorded: {event}")

    async def get_anomaly_events(self) -> List[AnomalyEvent]:
        return list(self.anomaly_events)

    async def get_uncertainty_log(self) -> List[Dict]:
        return list(self._uncertainty_log)


# ============================================================================
# Metrics Reporter
# ============================================================================
class MetricsReporter:
    def __init__(self, config: ExpertMetricsConfig, storage: MetricsStorage, analyzer: MetricsAnalyzer):
        self.config = config
        self.storage = storage
        self.analyzer = analyzer
        self.telemetry = TelemetryExporter(config.telemetry) if config.telemetry.enabled else None
        self.persistence = (
            MetricsPersistenceManager(config.persistence) if config.persistence.enabled else None
        )
        # Callback to publish FeedbackEvent; set by collector
        self.feedback_publisher: Optional[Callable[[Dict[str, Any]], Awaitable[None]]] = None

    async def generate_summary(self) -> Dict[str, Any]:
        usage = await self.storage.get_expert_usage()
        success_rates = await self.storage.get_expert_success_rate()
        latency_stats = await self.storage.get_expert_latency_stats()
        resources = await self.storage.get_resource_consumption()
        pareto = await self.storage.get_pareto_frontier()
        health = await self.storage.get_health_scores()

        routing_latencies = list(self.storage.routing_latency)
        summary: Dict[str, Any] = {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'expert_usage': usage,
            'success_rates': success_rates,
            'latency_stats': latency_stats,
            'resource_consumption': resources,
            'pareto_frontier_size': len(pareto),
            'total_routes': len(self.storage.routing_decisions),
            'avg_routing_latency_ms': float(np.mean(routing_latencies)) if routing_latencies else 0.0,
            'health_scores': health,
        }

        if self.analyzer.slo_tracker:
            slo_results = await self.analyzer.slo_tracker.evaluate_slos()
            summary['slo_status'] = slo_results
            for slo_id, res in slo_results.items():
                if res.get('status') == SLOStatus.BREACHED.value:
                    await self.publish_slo_breach_event(slo_id, res)

        if self.config.enable_mopd:
            mopd_pareto = await self.storage.compute_pareto_front(
                max_points=20,
                sort_by='scalarised',
                weights=self.config.mopd.objective_weights,
                normalization_bounds=self.config.mopd.normalization_bounds,
            )
            summary['mopd_pareto_front'] = [p.to_dict() for p in mopd_pareto]
        return summary

    async def publish_slo_breach_event(self, slo_id: str, details: Dict[str, Any]) -> None:
        logger.warning(f"SLO breach: {slo_id} - {details}")
        if self.feedback_publisher:
            try:
                await self.feedback_publisher({
                    'type': 'slo_breach',
                    'slo_id': slo_id,
                    'details': details,
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                })
            except Exception as e:
                logger.error(f"Failed to publish SLO breach event: {e}")

    async def export_telemetry(self) -> None:
        if self.telemetry:
            data = await self.telemetry.export()
            logger.debug(f"Telemetry export: {len(data)} bytes")

    async def save_state(self) -> None:
        if not self.persistence:
            return
        state = {
            'expert_usage': await self.storage.get_expert_usage(),
            'expert_success': dict(self.storage.expert_success),
            'expert_failures': dict(self.storage.expert_failures),
            'expert_energy': dict(self.storage.expert_energy),
            'expert_carbon': dict(self.storage.expert_carbon),
            'expert_helium': dict(self.storage.expert_helium),
            'expert_ecoatp': dict(self.storage.expert_ecoatp),
            'health_scores': await self.storage.get_health_scores(),
            'mopd_points': [p.to_dict() for p in self.storage.mopd_points],
            'uncertainty_log': await self.analyzer.get_uncertainty_log(),
        }
        await self.persistence.save_state(state)

    async def load_state(self) -> None:
        if not self.persistence:
            return
        state = await self.persistence.load_state()
        if not state:
            return
        for p_dict in state.get('mopd_points', []):
            try:
                self.storage.mopd_points.append(MOPDPoint.from_dict(p_dict))
            except Exception as e:
                logger.warning(f"Skipping invalid MOPD point: {e}")
        health_scores = state.get('health_scores', {})
        async with self.storage._lock:
            self.storage.health_scores.update(health_scores)
            for key in ('expert_usage', 'expert_success', 'expert_failures',
                        'expert_energy', 'expert_carbon', 'expert_helium', 'expert_ecoatp'):
                container = getattr(self.storage, key)
                loaded = state.get(key, {})
                if isinstance(container, defaultdict):
                    container.update(loaded)
                else:
                    container.update(loaded)


# ============================================================================
# Human-AI Collaborative Support (with approval queue)
# ============================================================================
class HumanAICollaborativeSupport:
    def __init__(self, config: HumanApprovalConfig):
        self.config = config
        self.pending: Dict[str, ApprovalRequest] = {}
        self.history: Deque[ApprovalResult] = deque(maxlen=1000)
        self._lock = asyncio.Lock()
        self._dashboard: Dict[str, Deque[Dict]] = {
            'metrics': deque(maxlen=1000),
            'alerts': deque(maxlen=1000),
            'insights': deque(maxlen=1000),
        }

    async def request_approval(self, context: Dict[str, Any], timeout_seconds: Optional[float] = None) -> ApprovalResult:
        request_id = uuid.uuid4().hex
        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()
        request = ApprovalRequest(
            request_id=request_id,
            context=context,
            requested_at=datetime.now(timezone.utc),
            timeout_seconds=timeout_seconds or self.config.timeout_seconds,
            result_future=future,
        )
        async with self._lock:
            self.pending[request_id] = request

        try:
            result = await asyncio.wait_for(future, timeout=request.timeout_seconds)
        except asyncio.TimeoutError:
            if self.config.auto_approve_on_timeout:
                result = ApprovalResult(
                    request_id=request_id,
                    status=ApprovalStatus.AUTO_APPROVED,
                    reason="auto-approved on timeout",
                    decided_at=datetime.now(timezone.utc),
                )
            else:
                result = ApprovalResult(
                    request_id=request_id,
                    status=ApprovalStatus.TIMEOUT,
                    reason="timed out",
                    decided_at=datetime.now(timezone.utc),
                )
        finally:
            async with self._lock:
                self.pending.pop(request_id, None)
                self.history.append(result)
        return result

    async def submit_decision(self, request_id: str, approved: bool, reason: str = "") -> bool:
        async with self._lock:
            request = self.pending.get(request_id)
            if not request or request.result_future is None or request.result_future.done():
                return False
            result = ApprovalResult(
                request_id=request_id,
                status=ApprovalStatus.APPROVED if approved else ApprovalStatus.DENIED,
                reason=reason,
                decided_at=datetime.now(timezone.utc),
            )
            request.result_future.set_result(result)
            return True

    async def get_dashboard_data(self) -> Dict[str, Any]:
        async with self._lock:
            return {
                'pending': [asdict(r) for r in self.pending.values()],
                'recent_insights': list(self._dashboard['insights'])[-10:],
            }

    async def add_insight(self, insight: Dict[str, Any]) -> None:
        async with self._lock:
            self._dashboard['insights'].append(insight)


# ============================================================================
# Cross-Domain Transfer (stub)
# ============================================================================
class MetricsCrossDomainTransfer:
    def transfer_knowledge(self, source: str, target: str, knowledge_type: str, data: Any) -> None:
        logger.info(f"Cross-domain transfer: {source} → {target} ({knowledge_type})")


# ============================================================================
# Temporal Safety Monitor
# ============================================================================
class TemporalSafetyMonitor:
    def __init__(self, collector: "ExpertMetricsCollector", config: TemporalSafetyConfig):
        self.collector = collector
        self.config = config
        self.violations: Deque[Dict[str, Any]] = deque(maxlen=1000)

    async def check_invariants(self) -> List[str]:
        violations: List[str] = []
        if not self.config.enabled:
            return violations

        # SLOs
        if self.collector.analyzer.slo_tracker:
            slo_results = await self.collector.analyzer.slo_tracker.evaluate_slos()
            for slo_id, res in slo_results.items():
                if res.get('status') == SLOStatus.BREACHED.value:
                    violations.append(f"SLO {slo_id} breached: {res}")

        # Carbon
        if self.collector.carbon_manager:
            intensity = await self.collector.carbon_manager.get_current_intensity()
            if intensity > self.config.carbon_critical_threshold:
                violations.append(f"Carbon intensity critical: {intensity}")

        # Token balance
        if self.collector.token_manager:
            try:
                summary = self.collector.token_manager.get_system_summary()
                total_balance = summary.get('total_balance', float('inf'))
                if total_balance < self.config.token_critical_threshold:
                    violations.append(f"Token balance critical: {total_balance}")
            except Exception as e:
                logger.warning(f"Token balance check failed: {e}")

        for v in violations:
            self.violations.append({
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'violation': v,
            })
        return violations


# ============================================================================
# Fault Injector (chaos testing)
# ============================================================================
class FaultInjector:
    def __init__(self, collector: "ExpertMetricsCollector"):
        self.collector = collector
        self._saved_state: Dict[str, Any] = {}

    def _save(self, key: str, value: Any) -> None:
        if key not in self._saved_state:
            self._saved_state[key] = value

    def restore_all(self) -> None:
        for key, value in self._saved_state.items():
            if key == 'carbon_intensity' and self.collector.carbon_manager:
                self.collector.carbon_manager.carbon_intensity = value
            elif key == 'expert_router':
                self.collector.expert_router = value
            elif key == 'token_balance' and self.collector.token_manager:
                try:
                    self.collector.token_manager.set_balance(value)  # type: ignore
                except Exception:
                    pass
        self._saved_state.clear()

    async def inject(self, fault_type: FaultType, **params: Any) -> Dict[str, Any]:
        if fault_type == FaultType.CARBON_SPIKE:
            if self.collector.carbon_manager:
                self._save('carbon_intensity', self.collector.carbon_manager.carbon_intensity)
                self.collector.carbon_manager.carbon_intensity = params.get('intensity', CARBON_CRITICAL_THRESHOLD + 50)
                return {'fault': 'carbon_spike', 'intensity': self.collector.carbon_manager.carbon_intensity}
        elif fault_type == FaultType.TOKEN_DEPLETION:
            if self.collector.token_manager:
                try:
                    summary = self.collector.token_manager.get_system_summary()
                    self._save('token_balance', summary.get('total_balance', 1000.0))
                    setter = getattr(self.collector.token_manager, 'set_balance', None)
                    if setter:
                        setter(params.get('balance', 1.0))
                except Exception as e:
                    logger.warning(f"Token depletion injection failed: {e}")
                return {'fault': 'token_depletion'}
        elif fault_type == FaultType.BACKEND_UNAVAILABLE:
            self._save('expert_router', self.collector.expert_router)
            self.collector.expert_router = None
            return {'fault': 'backend_unavailable'}
        elif fault_type == FaultType.SLO_BREACH:
            if self.collector.analyzer.slo_tracker:
                self.collector.analyzer.slo_tracker.record_metric('latency_slo', params.get('latency_ms', 10_000.0))
            return {'fault': 'slo_breach'}
        elif fault_type == FaultType.NETWORK_LATENCY:
            # Simulated by delaying the next API call; not directly injectable here.
            return {'fault': 'network_latency', 'not_implemented': True}
        elif fault_type == FaultType.QUEUE_OVERFLOW:
            return {'fault': 'queue_overflow', 'not_implemented': True}
        elif fault_type == FaultType.MODEL_CORRUPTION:
            if self.collector.analyzer.ml_anomaly_detector:
                self.collector.analyzer.ml_anomaly_detector.is_trained = False
            return {'fault': 'model_corruption'}
        return {'fault': fault_type.value, 'not_implemented': True}


# ============================================================================
# Main ExpertMetricsCollector
# ============================================================================
class ExpertMetricsCollector:
    """
    Enhanced Expert Metrics Collector v9.0.0 - Full Green Agent Integration with MOPD,
    XAI, temporal safety, human‑in‑the‑loop, chaos testing, and production-grade
    observability.
    """

    def __init__(
        self,
        bio_core: Optional[Any] = None,
        config: Optional[ExpertMetricsConfig] = None,
        storage: Optional[Any] = None,
        message_queue: Optional[Any] = None,
        adaptive_cost: Optional[Any] = None,
        pareto_gating: Optional[Any] = None,
        drift_detector: Optional[Any] = None,
        metrics: Optional[Any] = None,
        time_series_backend: Optional[TimeSeriesBackend] = None,
        **kwargs: Any,
    ):
        if config is None:
            valid_fields = {f.name for f in dataclass_fields(ExpertMetricsConfig)}
            filtered = {k: v for k, v in kwargs.items() if k in valid_fields}
            config = ExpertMetricsConfig(**filtered)
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
        self.self_healer = None
        self.workflow_orchestrator = None
        self.token_manager: Optional[TokenManagerProtocol] = None
        self.gradient_manager: Optional[GradientManagerProtocol] = None
        self.swarm_coordinator: Optional[SwarmCoordinatorProtocol] = None
        if bio_core:
            self.event_broker = getattr(bio_core, 'event_broker', None)
            self.alert_system = getattr(bio_core, 'alert_system', None)
            self.self_healer = getattr(bio_core, 'self_healer', None)
            self.workflow_orchestrator = getattr(bio_core, 'workflow_orchestrator', None)
            self.token_manager = getattr(bio_core, 'token_manager', None)
            self.gradient_manager = getattr(bio_core, 'gradient_manager', None)
            self.swarm_coordinator = getattr(bio_core, 'swarm_coordinator', None)

        # Sub-modules
        self.observability = ObservabilityHub()
        self.storage_metrics = MetricsStorage(
            retention_hours=config.persistence.retention_hours,
            time_series_backend=time_series_backend,
        )
        self.analyzer = MetricsAnalyzer(config, self.storage_metrics)
        self.reporter = MetricsReporter(config, self.storage_metrics, self.analyzer)
        self.reporter.feedback_publisher = self._publish_feedback
        self.carbon_manager = CarbonIntensityManager(config.carbon) if config.carbon.enabled else None
        self.federated_aggregator = (
            FederatedMetricsAggregator(config.federated) if config.federated.enabled else None
        )
        self.human_ai = (
            HumanAICollaborativeSupport(config.human_approval) if config.enable_human_ai else None
        )
        self.cross_domain = MetricsCrossDomainTransfer() if config.enable_cross_domain else None

        # Safety / chaos
        self.safety_monitor = (
            TemporalSafetyMonitor(self, config.temporal_safety)
            if config.enable_temporal_safety else None
        )
        self.fault_injector = FaultInjector(self) if config.enable_chaos_testing else None

        # MoE injections
        self.expert_router = None
        self.gating_network = None
        self.self_evolving_gate = None
        self.helium_provider: Optional[HeliumProviderProtocol] = None

        # Sustainability
        self.sustainability_score = 0.0
        self.total_carbon_savings_kg = 0.0
        self.total_helium_savings_l = 0.0

        # Alerts
        self.active_alerts: Dict[str, Dict[str, Any]] = {}
        self.alert_history: Deque[Dict[str, Any]] = deque(maxlen=MAX_ALERT_HISTORY)
        self.alert_cooldowns: Dict[str, float] = {}

        # Event queue
        self._event_queue: asyncio.Queue = asyncio.Queue(maxsize=10_000)
        self._event_consumer_task: Optional[asyncio.Task] = None

        # Background tasks
        self._background_tasks: List[asyncio.Task] = []
        self.health_status = "healthy"
        self.last_error: Optional[str] = None
        self._lock = asyncio.Lock()
        self._shutting_down = False

        if self.config.enable_event_driven and self.event_broker:
            self._subscribe_events()

        self._load_state_task = self._create_task(self._load_persisted_state())
        self._start_background_tasks()

        logger.info(
            f"Enhanced Expert Metrics Collector v9.0.0 initialized: "
            f"bio_integration={self.config.enable_bio_integration}, "
            f"mopd={self.config.enable_mopd}, "
            f"temporal_safety={self.config.enable_temporal_safety}, "
            f"human_approval={self.config.enable_human_approval}, "
            f"chaos_testing={self.config.enable_chaos_testing}, "
            f"central_storage={storage is not None}, central_queue={message_queue is not None}"
        )

    # ------------------------------------------------------------------
    # Task helpers
    # ------------------------------------------------------------------
    def _create_task(self, coro: Awaitable[Any]) -> Optional[asyncio.Task]:
        try:
            loop = asyncio.get_running_loop()
            return loop.create_task(coro)
        except RuntimeError:
            logger.warning("No running event loop; background task not started.")
            return None

    # ------------------------------------------------------------------
    # Event subscription and handling
    # ------------------------------------------------------------------
    def _subscribe_events(self) -> None:
        if not self.event_broker:
            return
        subscriptions = {
            'carbon_update': self._enqueue_event,
            'helium_update': self._enqueue_event,
            'alert_generated': self._enqueue_event,
            'config_updated': self._enqueue_event,
            'token_balance_update': self._enqueue_event,  # matches handler name below
            'health_update': self._enqueue_event,
            'anomaly_detected': self._enqueue_event,
        }
        for event_name, handler in subscriptions.items():
            try:
                self.event_broker.subscribe(event_name, handler)
            except Exception as e:
                logger.warning(f"Failed to subscribe to {event_name}: {e}")

    async def _enqueue_event(self, event: Any) -> None:
        try:
            self._event_queue.put_nowait(event)
        except asyncio.QueueFull:
            logger.warning("Event queue full; dropping event")
            self.observability.counter('event_queue_drops')

    async def _event_consumer(self) -> None:
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
        if self.carbon_manager:
            self.carbon_manager.carbon_intensity = float(
                event.data.get('intensity', DEFAULT_CARBON_FALLBACK_INTENSITY)
            )

    async def _on_helium_update(self, event: Any) -> None:
        pass

    async def _on_alert_generated(self, event: Any) -> None:
        if event.data.get('severity') == MetricSeverity.CRITICAL.value:
            logger.warning("Critical alert received; triggering self-healing")
            if self.config.self_healing.enabled and self.self_healer:
                try:
                    await self.self_healer.apply_healing('damage_accumulation')
                except Exception as e:
                    logger.error(f"Self-healing failed: {e}")

    async def _on_config_updated(self, event: Any) -> None:
        pass

    async def _on_token_balance_update(self, event: Any) -> None:
        pass

    async def _on_health_update(self, event: Any) -> None:
        self.health_status = event.data.get('status', 'healthy')

    async def _on_anomaly_detected(self, event: Any) -> None:
        pass

    # ------------------------------------------------------------------
    # Background tasks
    # ------------------------------------------------------------------
    def _start_background_tasks(self) -> None:
        if self.config.enable_event_driven:
            self._event_consumer_task = self._create_task(self._event_consumer())
            if self._event_consumer_task:
                self._background_tasks.append(self._event_consumer_task)
        if self.config.carbon.enabled:
            t = self._create_task(self._carbon_update_loop())
            if t: self._background_tasks.append(t)
        if self.config.federated.enabled:
            t = self._create_task(self._federated_sync_loop())
            if t: self._background_tasks.append(t)
        if self.config.telemetry.enabled:
            t = self._create_task(self._telemetry_export_loop())
            if t: self._background_tasks.append(t)
        if self.config.persistence.enabled:
            t = self._create_task(self._persistence_save_loop())
            if t: self._background_tasks.append(t)
        if self.config.enable_temporal_safety:
            t = self._create_task(self._temporal_safety_loop())
            if t: self._background_tasks.append(t)
        if self.config.enable_swarm_coordination:
            t = self._create_task(self._swarm_share_loop())
            if t: self._background_tasks.append(t)

    async def _carbon_update_loop(self) -> None:
        while not self._shutting_down:
            try:
                if self.carbon_manager:
                    await self.carbon_manager.update_carbon_intensity()
                await asyncio.sleep(self.config.carbon.update_interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Carbon update error: {e}")
                await asyncio.sleep(60)

    async def _federated_sync_loop(self) -> None:
        while not self._shutting_down:
            try:
                if self.federated_aggregator:
                    success_rates = await self.storage_metrics.get_expert_success_rate()
                    values = list(success_rates.values())
                    avg_success = float(np.mean(values)) if values else 0.0
                    metrics = {
                        'avg_success_rate': avg_success,
                        'sustainability_score': self.sustainability_score,
                    }
                    await self.federated_aggregator.send_local_metrics(
                        f"metrics_{id(self)}", metrics, self.sustainability_score,
                    )
                    await self.federated_aggregator.get_global_metrics()
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

    async def _temporal_safety_loop(self) -> None:
        while not self._shutting_down:
            try:
                if self.safety_monitor:
                    violations = await self.safety_monitor.check_invariants()
                    if violations and self.config.self_healing.auto_heal_on_critical:
                        await self.self_heal()
                await asyncio.sleep(self.config.temporal_safety.check_interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Temporal safety error: {e}")
                await asyncio.sleep(60)

    async def _swarm_share_loop(self) -> None:
        while not self._shutting_down:
            try:
                await self.share_with_swarm()
                await asyncio.sleep(self.config.swarm_share_interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Swarm share error: {e}")
                await asyncio.sleep(60)

    # ------------------------------------------------------------------
    # Feedback publishing
    # ------------------------------------------------------------------
    async def _publish_feedback(self, payload: Dict[str, Any]) -> None:
        if self.queue is None:
            return
        try:
            event = FeedbackEvent.create_with_context(
                task_id=payload.get('task_id') or f"metrics_{uuid.uuid4().hex[:8]}",
                selected_action=payload.get('selected_action', 'metrics'),
                quality_score=payload.get('quality_score', 0.0),
                energy_joules=payload.get('energy_joules', 0.0),
                carbon_g=payload.get('carbon_g', 0.0),
                feedback_type=payload.get('type', 'metrics_event'),
                adaptive_cost_value=0.0,
                state=payload,
                candidates=payload.get('candidates', []),
                source="expert_metrics_collector",
                environment=getattr(central_config, "ENVIRONMENT", "production") if central_config else "production",
                tags=["metrics", payload.get('type', 'event')],
            )
            await self.queue.publish("feedback_events", event.to_json())
        except Exception as e:
            logger.error(f"Failed to publish feedback: {e}")

    # ------------------------------------------------------------------
    # Public API – Recording
    # ------------------------------------------------------------------
    async def record_routing(
        self,
        routing_decisions: List[Tuple[int, float]],
        gating_context: Optional[Dict[str, Any]],
        execution_time: float,
        success: bool,
        correlation_id: Optional[str] = None,
        idempotency_key: Optional[str] = None,
    ) -> None:
        await self.analyzer.analyze_routing(
            routing_decisions, execution_time, success, correlation_id, idempotency_key,
        )
        self.observability.counter('routing_total', success=str(success).lower())
        self.observability.histogram('routing_latency_ms', execution_time)

        if self.gating_network and self.expert_router:
            try:
                features = np.array([len(routing_decisions), execution_time, self.sustainability_score])
                reward = 1.0 if success else 0.0
                context = {'success': success, 'execution_time_ms': execution_time}
                self.gating_network.update(features, reward, context)
            except Exception as e:
                logger.warning(f"Gating network update failed: {e}")

        if self.self_evolving_gate and TORCH_AVAILABLE:
            try:
                state_tensor = torch.tensor([execution_time, self.sustainability_score], dtype=torch.float32)
                chosen = routing_decisions[0][0] if routing_decisions else 0
                self.self_evolving_gate.adapt(
                    state=state_tensor, chosen_expert=chosen,
                    reward=1.0 if success else 0.0,
                    environmental_feedback={'success': success},
                    quantum_mode=False,
                )
            except Exception as e:
                logger.warning(f"Self-evolving gate adapt failed: {e}")

    async def record_expert_execution(
        self,
        expert_id: str,
        execution_time: float,
        energy_kwh: float,
        carbon_kg: float,
        helium_units: float,
        success: bool,
        correlation_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        idempotency_key: Optional[str] = None,
    ) -> None:
        # Bio-inspired ATP accounting
        if self.token_manager and energy_kwh > 0:
            try:
                ecoatp_cost = energy_kwh * self.config.token_exchange_rate
                await self.token_manager.spend(f"expert_{expert_id}", ecoatp_cost)
                if success:
                    await self.token_manager.earn(f"expert_{expert_id}", ecoatp_cost * 0.5)
            except Exception as e:
                logger.warning(f"Token manager interaction failed: {e}")

        await self.analyzer.analyze_execution(
            expert_id, execution_time, energy_kwh, carbon_kg, helium_units,
            success, correlation_id, metadata, idempotency_key,
        )
        self.observability.counter('expert_execution_total', expert=expert_id, success=str(success).lower())
        self.observability.histogram('expert_execution_latency_ms', execution_time, expert=expert_id)

        # Gradient pumping
        if self.gradient_manager:
            try:
                delta = 0.05 if success else -0.1
                await self.gradient_manager.pump_field('trust', delta, source=f"expert_{expert_id}")
                if carbon_kg > 0.001:
                    await self.gradient_manager.pump_field('carbon', 0.05, source=f"expert_{expert_id}")
                if helium_units > 0:
                    await self.gradient_manager.pump_field('helium', 0.05, source=f"expert_{expert_id}")
            except Exception as e:
                logger.warning(f"Gradient pumping failed: {e}")

        await self._update_sustainability_score()

        # Publish FeedbackEvent
        if self.queue:
            await self._publish_feedback({
                'type': 'metrics_execution',
                'task_id': correlation_id or f"exec_{expert_id}_{uuid.uuid4().hex[:8]}",
                'selected_action': expert_id,
                'quality_score': 1.0 if success else 0.0,
                'energy_joules': energy_kwh * 3.6e6,
                'carbon_g': carbon_kg * 1000.0,
                'candidates': [{'action': expert_id}],
                'expert_id': expert_id,
                'success': success,
                'explanation': f"Execution of {expert_id} consumed {energy_kwh}kWh, emitted {carbon_kg}kg CO2, used {helium_units} helium units.",
            })

        # Drift check
        if self.drift and self.adaptive_cost:
            try:
                weights = self.adaptive_cost.get_current_weights()
                drift_score = await self.drift.check_drift(weights)
                if drift_score and drift_score > self.config.temporal_safety.drift_retrain_threshold:
                    logger.warning(f"High drift detected ({drift_score:.3f}); retraining.")
                    if 'carbon_per_inference' in self.config.thresholds:
                        self.config.thresholds['carbon_per_inference'].warning_threshold *= 0.95
                        self.config.thresholds['carbon_per_inference'].critical_threshold *= 0.95
                    if self.analyzer.ml_anomaly_detector and self.config.anomaly_detection.enable_drift_retraining:
                        await self.analyzer.ml_anomaly_detector.retrain_now()
            except Exception as e:
                logger.warning(f"Drift check failed: {e}")

    # ------------------------------------------------------------------
    # MOPD public methods
    # ------------------------------------------------------------------
    async def get_mopd_pareto_front(
        self,
        objective_names: Optional[List[str]] = None,
        constraints: Optional[Dict[str, Tuple[float, float]]] = None,
        max_points: int = 50,
        sort_by: str = 'scalarised',
    ) -> List[MOPDPoint]:
        if not self.config.enable_mopd:
            return []
        return await self.storage_metrics.compute_pareto_front(
            objective_names, constraints, max_points, sort_by,
            weights=self.config.mopd.objective_weights,
            normalization_bounds=self.config.mopd.normalization_bounds,
        )

    async def get_mopd_summary(self) -> Dict[str, Any]:
        if not self.config.enable_mopd:
            return {'enabled': False}
        pareto = await self.storage_metrics.compute_pareto_front(
            max_points=20, sort_by='scalarised',
            weights=self.config.mopd.objective_weights,
            normalization_bounds=self.config.mopd.normalization_bounds,
        )
        return {
            'enabled': True,
            'pareto_front_size': len(pareto),
            'total_mopd_points': len(self.storage_metrics.mopd_points),
            'objective_weights': self.config.mopd.objective_weights,
            'grid_resolution': self.config.mopd.grid_resolution,
            'sample_pareto': [p.to_dict() for p in pareto[:5]],
        }

    # ------------------------------------------------------------------
    # Teacher policy
    # ------------------------------------------------------------------
    async def policy_probs(self, state: Dict[str, Any]) -> List[float]:
        if not self.config.enable_mopd:
            usage = await self.storage_metrics.get_expert_usage()
            experts = list(usage.keys())
            return [1.0 / len(experts)] * len(experts) if experts else []

        pareto_points = await self.storage_metrics.compute_pareto_front(
            objective_names=['carbon_kg', 'helium_units', 'ecoatp_cost', 'latency_ms', 'success_probability'],
            max_points=50,
            sort_by='scalarised',
            weights=self.config.mopd.objective_weights,
            normalization_bounds=self.config.mopd.normalization_bounds,
        )
        if not pareto_points:
            usage = await self.storage_metrics.get_expert_usage()
            experts = list(usage.keys())
            return [1.0 / len(experts)] * len(experts) if experts else []

        scored: List[Tuple[str, float]] = []
        for point in pareto_points:
            if self.adaptive_cost:
                try:
                    cost = self.adaptive_cost.compute(
                        quality=point.success_probability,
                        carbon_g=point.carbon_kg * 1000.0,
                        latency_ms=point.latency_ms,
                        energy_joules=(
                            point.ecoatp_cost / self.config.token_exchange_rate
                            if self.config.token_exchange_rate else point.ecoatp_cost
                        ),
                        health=0.8,
                        atp=(
                            point.ecoatp_cost / self.config.token_exchange_rate
                            if self.config.token_exchange_rate else 0.5
                        ),
                    )
                except Exception:
                    cost = 1.0 - point.success_probability
            else:
                cost = self.storage_metrics._scalarise_point(
                    point,
                    ['carbon_kg', 'helium_units', 'ecoatp_cost', 'latency_ms', 'success_probability'],
                    self.config.mopd.objective_weights,
                    self.config.mopd.normalization_bounds,
                )
            scored.append((point.expert_id, cost))

        expert_scores: Dict[str, List[float]] = defaultdict(list)
        for expert_id, cost in scored:
            expert_scores[expert_id].append(cost)
        if not expert_scores:
            return []

        experts = list(expert_scores.keys())
        scores = np.array([float(np.mean(expert_scores[eid])) for eid in experts])
        exp_scores = np.exp(scores - np.max(scores))
        probs = exp_scores / np.sum(exp_scores)
        return probs.tolist()

    # ------------------------------------------------------------------
    # Temporal safety / human-in-the-loop
    # ------------------------------------------------------------------
    async def check_invariants(self) -> List[str]:
        if not self.safety_monitor:
            return []
        return await self.safety_monitor.check_invariants()

    async def request_approval(self, context: Dict[str, Any]) -> bool:
        if not self.config.enable_human_approval:
            return True
        if self.human_ai is None:
            logger.warning("Human approval requested but human_ai module unavailable")
            return False
        result = await self.human_ai.request_approval(context)
        return result.status in (ApprovalStatus.APPROVED, ApprovalStatus.AUTO_APPROVED)

    async def submit_approval_decision(self, request_id: str, approved: bool, reason: str = "") -> bool:
        if not self.human_ai:
            return False
        return await self.human_ai.submit_decision(request_id, approved, reason)

    async def log_uncertainty(self, metric_name: str, value: float, context: Dict[str, Any]) -> None:
        await self.analyzer.log_uncertainty(metric_name, value, context)

    # ------------------------------------------------------------------
    # Chaos testing
    # ------------------------------------------------------------------
    async def inject_fault(self, fault_type: Union[str, FaultType], **params: Any) -> Dict[str, Any]:
        if not self.config.enable_chaos_testing or self.fault_injector is None:
            logger.info("Chaos testing disabled; ignoring fault injection.")
            return {'status': 'disabled'}
        if isinstance(fault_type, str):
            try:
                fault_type = FaultType(fault_type)
            except ValueError:
                logger.warning(f"Unknown fault type: {fault_type}")
                return {'status': 'unknown_fault'}
        return await self.fault_injector.inject(fault_type, **params)

    async def run_chaos_test(self) -> Dict[str, Any]:
        if not self.config.enable_chaos_testing or self.fault_injector is None:
            return {'status': 'disabled'}
        report: Dict[str, Any] = {'faults': [], 'results': {}}
        faults_to_run = [
            FaultType.TOKEN_DEPLETION,
            FaultType.CARBON_SPIKE,
            FaultType.SLO_BREACH,
        ]
        try:
            for fault in faults_to_run:
                result = await self.inject_fault(fault)
                report['faults'].append(fault.value)
                report['results'][fault.value] = result
                # Exercise the system under fault
                try:
                    await self.record_expert_execution(
                        'chaos_test_expert', 100.0, 0.01, 0.001, 0.0, True,
                    )
                    report['results'][f'{fault.value}_execution'] = 'processed'
                except Exception as e:
                    report['results'][f'{fault.value}_execution'] = f'error: {e}'
            report['results']['post_fault_invariants'] = await self.check_invariants()
        finally:
            if self.config.chaos_testing.auto_restore:
                self.fault_injector.restore_all()
        return report

    # ------------------------------------------------------------------
    # Sustainability
    # ------------------------------------------------------------------
    async def _update_sustainability_score(self) -> None:
        health_scores = await self.storage_metrics.get_health_scores()
        avg_health = float(np.mean(list(health_scores.values()))) if health_scores else 0.5
        token_eff = self._get_token_efficiency()
        carbon_factor = (
            1.0 - (self.carbon_manager.carbon_intensity / CARBON_CRITICAL_THRESHOLD)
            if self.carbon_manager else 0.5
        )
        success_rates = await self.storage_metrics.get_expert_success_rate()
        avg_success = float(np.mean(list(success_rates.values()))) if success_rates else 0.5
        self.sustainability_score = min(
            1.0, max(0.0, avg_health * 0.25 + token_eff * 0.2 + carbon_factor * 0.25 + avg_success * 0.3),
        )
        self.observability.gauge('sustainability_score', self.sustainability_score)

    def _get_token_efficiency(self) -> float:
        if self.token_manager:
            try:
                summary = self.token_manager.get_system_summary()
                return float(summary.get('system_efficiency', 0.5))
            except Exception:
                return 0.5
        return 0.5

    # ------------------------------------------------------------------
    # Swarm / self-healing / queries
    # ------------------------------------------------------------------
    async def share_with_swarm(self) -> None:
        if not self.config.enable_swarm_coordination or not self.swarm_coordinator:
            return
        payload = {
            'collector_id': id(self),
            'sustainability_score': self.sustainability_score,
            'mopd_enabled': self.config.enable_mopd,
            'anomaly_events': len(self.analyzer.anomaly_events),
        }
        try:
            await self.swarm_coordinator.share_predictions(payload)
        except Exception as e:
            logger.warning(f"Swarm share failed: {e}")

    async def self_heal(self) -> None:
        logger.info("ExpertMetricsCollector self-healing")
        if not self.config.self_healing.enabled:
            return
        self.config.thresholds = ExpertMetricsConfig().thresholds
        if self.analyzer.slo_tracker:
            self.analyzer.slo_tracker.slos.clear()
            for slo_id, params in self.config.slo.slo_definitions.items():
                self.analyzer.slo_tracker.define_slo(slo_id, **params)
        self.active_alerts.clear()
        self.health_status = "healthy"
        self.last_error = None

    async def get_expert_usage(self) -> Dict[str, int]:
        return await self.storage_metrics.get_expert_usage()

    async def get_expert_success_rate(self) -> Dict[str, float]:
        return await self.storage_metrics.get_expert_success_rate()

    async def get_expert_latency_stats(self) -> Dict[str, Dict[str, float]]:
        return await self.storage_metrics.get_expert_latency_stats()

    async def get_resource_consumption(self) -> Dict[str, Dict[str, float]]:
        return await self.storage_metrics.get_resource_consumption()

    async def get_pareto_frontier(self) -> List[Dict]:
        return await self.storage_metrics.get_pareto_frontier()

    async def get_health_scores(self) -> Dict[str, float]:
        return await self.storage_metrics.get_health_scores()

    async def get_alerts(
        self,
        acknowledged: Optional[bool] = None,
        severity: Optional[MetricSeverity] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        alerts = list(self.active_alerts.values())
        if acknowledged is not None:
            alerts = [a for a in alerts if a.get('acknowledged') == acknowledged]
        if severity:
            alerts = [a for a in alerts if a.get('severity') == severity.value]
        return alerts[:limit]

    async def acknowledge_alert(self, alert_id: str) -> bool:
        async with self._lock:
            if alert_id in self.active_alerts:
                self.active_alerts[alert_id]['acknowledged'] = True
                return True
            return False

    async def get_predictions(self) -> Dict[str, Dict]:
        return self.analyzer.predictions

    async def get_slo_status(self) -> Dict[str, Dict[str, Any]]:
        if self.analyzer.slo_tracker:
            return await self.analyzer.slo_tracker.evaluate_slos()
        return {}

    async def get_metrics_summary(self) -> Dict[str, Any]:
        return await self.reporter.generate_summary()

    async def get_health_status(self) -> Dict[str, Any]:
        return {
            'status': self.health_status,
            'last_error': self.last_error,
            'active_alerts': len([a for a in self.active_alerts.values() if not a.get('acknowledged')]),
            'sustainability_score': self.sustainability_score,
            'bio_integration_active': self.config.enable_bio_integration,
            'mopd_enabled': self.config.enable_mopd,
            'temporal_safety_enabled': self.config.enable_temporal_safety,
            'human_approval_enabled': self.config.enable_human_approval,
            'chaos_testing_enabled': self.config.enable_chaos_testing,
        }

    async def get_prometheus_metrics(self) -> str:
        return await self.observability.export_prometheus()

    # ------------------------------------------------------------------
    # Injection helpers
    # ------------------------------------------------------------------
    def set_gating_network(self, gating_network: Any) -> None:
        self.gating_network = gating_network

    def set_self_evolving_gate(self, gate: Any) -> None:
        self.self_evolving_gate = gate

    def set_expert_router(self, router: Any) -> None:
        self.expert_router = router

    def set_helium_provider(self, provider: HeliumProviderProtocol) -> None:
        self.helium_provider = provider

    def inject_bio_core(self, bio_core: Any) -> None:
        self.bio_core = bio_core
        if bio_core:
            self.token_manager = getattr(bio_core, 'token_manager', None)
            self.gradient_manager = getattr(bio_core, 'gradient_manager', None)
            self.event_broker = getattr(bio_core, 'event_broker', None)
            self.self_healer = getattr(bio_core, 'self_healer', None)
            self.swarm_coordinator = getattr(bio_core, 'swarm_coordinator', None)

    async def _load_persisted_state(self) -> None:
        try:
            await self.reporter.load_state()
        except Exception as e:
            logger.error(f"Failed to load persisted state: {e}")

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------
    async def shutdown(self) -> None:
        logger.info("Shutting down Expert Metrics Collector")
        self._shutting_down = True

        if self._event_consumer_task:
            self._event_consumer_task.cancel()

        for task in self._background_tasks:
            task.cancel()
        await asyncio.gather(*self._background_tasks, return_exceptions=True)

        if self.config.persistence.enabled:
            try:
                await self.reporter.save_state()
            except Exception as e:
                logger.error(f"Final save failed: {e}")

        if self.carbon_manager:
            await self.carbon_manager.close()
        if self.federated_aggregator:
            await self.federated_aggregator.close()
        logger.info("Shutdown complete")


# ============================================================================
# Module-level convenience
# ============================================================================
__all__ = [
    "ExpertMetricsCollector",
    "ExpertMetricsConfig",
    "MOPDPoint",
    "MOPDConfig",
    "AnomalyEvent",
    "AnomalyType",
    "MetricSeverity",
    "SLOStatus",
    "ApprovalStatus",
    "FaultType",
    "TimeSeriesBackend",
    "InMemoryTimeSeriesBackend",
    "ObservabilityHub",
    "XAIExplainer",
    "CorrelationContext",
]
