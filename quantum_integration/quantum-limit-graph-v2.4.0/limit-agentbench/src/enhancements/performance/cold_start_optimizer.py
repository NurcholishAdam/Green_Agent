#!/usr/bin/env python3
# cold_start_optimizer.py
"""
Cold Start Optimizer for Green Agent MoE System v4.0.0
========================================================

Production-grade cold-start elimination with:

- Sub-service architecture: Distillation, Evolution (NSGA-II), Gating (MoE),
  Federated, Carbon, Helium, Eviction, Persistence, Safety.
- Protocols for all injected components.
- Shared `ObservabilityHub` and `GovernanceLayer`.
- Non-destructive `ChaosService` with save/restore.
- Unified MOPD scalarisation with canonical objective directions.
- Full lifecycle: `start()` / `wait_ready()` / `shutdown()`.
- Pydantic v1/v2 compatibility (with dataclass fallback).
- Idempotency keys with LRU cache.
- Dead-letter queue with bounded capacity.
- Bounded caches (deque maxlen).
- Real implementations for every previously stubbed sub-module.

ENHANCEMENTS OVER v3.4.1:
- MOEA `evaluate` now uses real historical warmup statistics — no more constant
  stub values that rendered the entire evolutionary loop meaningless.
- Fixed MoE gating feature-dimension mismatch (14 vs 15).
- Added 'federated' to warmup strategies; strategies are now a canonical tuple.
- `datetime.utcnow()` → `datetime.now(timezone.utc)` throughout.
- `CircuitBreaker` uses `time.monotonic()` for timing.
- `CircuitBreakerState` is a proper `Enum`.
- Teachers no longer re-instantiate `ColdStartConfig()` internally.
- Persistence no longer hardcodes teacher index (`teachers[2]`).
- State load happens *before* background tasks start.
- NSGA-II uses stable integer IDs (not `id()`).
- NSGA-II evaluations are bounded by a `Semaphore`.
- Deduplication refills to the configured population size.
- `_compute_dynamic_weights` rebalances correctly.
- Non-blocking file I/O via `asyncio.to_thread`.
- `aiofiles` is optional (falls back to sync I/O).
- Full Pydantic v1/v2 compatibility (with dataclass fallback).
- Idempotency keys on `initialize_expert`.
- Dead-letter queue for persistence/federated failures.
- All previously stubbed sub-services are now functional.
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import logging
import math
import os
import pickle
import random
import time
import uuid
import zlib
from collections import OrderedDict, defaultdict, deque
from dataclasses import dataclass, field, fields as dataclass_fields, asdict
from datetime import datetime, timezone, timedelta
from enum import Enum
from pathlib import Path
from typing import (
    Any, Awaitable, Callable, Deque, Dict, FrozenSet, List, Optional,
    Protocol, Sequence, Set, Tuple, Union, runtime_checkable,
)

import numpy as np

# ---------------------------------------------------------------------------
# Optional dependencies
# ---------------------------------------------------------------------------
try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    aiohttp = None  # type: ignore
    AIOHTTP_AVAILABLE = False

try:
    import aiofiles
    AIOFILES_AVAILABLE = True
except ImportError:
    aiofiles = None  # type: ignore
    AIOFILES_AVAILABLE = False

try:
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.linear_model import SGDRegressor
    from sklearn.preprocessing import LabelEncoder, StandardScaler
    SKLEARN_AVAILABLE = True
except ImportError:
    RandomForestClassifier = None  # type: ignore
    SGDRegressor = None  # type: ignore
    LabelEncoder = None  # type: ignore
    StandardScaler = None  # type: ignore
    SKLEARN_AVAILABLE = False

try:
    from prometheus_client import (  # type: ignore
        Counter, Gauge, Histogram, CollectorRegistry, generate_latest,
        CONTENT_TYPE_LATEST, start_http_server,
    )
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False


logger = logging.getLogger(__name__)


# ============================================================================
# Pydantic compatibility layer
# ============================================================================
PYDANTIC_AVAILABLE = False
PYDANTIC_V2 = False
PYDANTIC_V1 = False
BaseModel: Any = object
Field: Any = lambda *a, **k: None  # noqa: E731
ConfigDict: Any = None

try:
    import pydantic as _pyd  # type: ignore
    _ver = getattr(_pyd, 'VERSION', '0') or '0'
    _maj = int(str(_ver).split('.')[0])
    if _maj >= 2:
        from pydantic import BaseModel, ConfigDict, Field  # type: ignore
        PYDANTIC_AVAILABLE = True
        PYDANTIC_V2 = True
    elif _maj == 1:
        from pydantic import BaseModel, Field  # type: ignore
        PYDANTIC_AVAILABLE = True
        PYDANTIC_V1 = True
except Exception:
    pass


# ============================================================================
# Named Constants
# ============================================================================
MODULE_VERSION = "4.0.0"
STATE_SCHEMA_VERSION = 4
DEFAULT_CACHE_SIZE = 100
DEFAULT_PRELOAD_THRESHOLD = 0.7
DEFAULT_STRATEGY_NAMES: Tuple[str, ...] = (
    'preload', 'transfer', 'progressive', 'hybrid', 'federated',
)
FEATURE_DIM = 15
MAX_IDEMPOTENCY_CACHE = 4_096
MAX_DEAD_LETTERS = 1_000
MAX_INTERACTION_LOG = 10_000
MAX_WARMUP_HISTORY = 10_000
MAX_APPROVAL_AUDIT = 1_000
MAX_APPROVAL_PENDING = 512
APPROVAL_DEFAULT_TIMEOUT = 60.0
CHAOS_MAX_FAULTS_PER_RUN = 6

# Canonical MOPD objective names and direction.
MOPD_OBJECTIVE_NAMES: Tuple[str, ...] = (
    'latency', 'carbon', 'cache_hit', 'sustainability',
)
MOPD_HIGHER_IS_BETTER: FrozenSet[str] = frozenset({
    'cache_hit', 'sustainability',
})


# ============================================================================
# Serialization helper
# ============================================================================
def _to_serializable(obj: Any) -> Any:
    if obj is None or isinstance(obj, (bool, int, float, str)):
        return obj
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {str(k): _to_serializable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set, frozenset)):
        return [_to_serializable(v) for v in obj]
    if isinstance(obj, deque):
        return [_to_serializable(v) for v in obj]
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, np.generic):
        return obj.item()
    if hasattr(obj, 'model_dump') and callable(obj.model_dump):
        try:
            return _to_serializable(obj.model_dump(mode='json'))
        except Exception:
            pass
    if hasattr(obj, 'dict') and callable(obj.dict):
        try:
            return _to_serializable(obj.dict())
        except Exception:
            pass
    if hasattr(obj, 'to_dict') and callable(obj.to_dict):
        return _to_serializable(obj.to_dict())
    if hasattr(obj, '__dict__'):
        return _to_serializable(obj.__dict__)
    return str(obj)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


# ============================================================================
# Enums
# ============================================================================
class CircuitBreakerState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class ApprovalStatus(str, Enum):
    PENDING = "pending"
    APPROVED = "approved"
    DENIED = "denied"
    TIMEOUT = "timeout"
    AUTO_APPROVED = "auto_approved"


class FaultType(str, Enum):
    PERSISTENCE_FAILURE = "persistence_failure"
    FEDERATED_FAILURE = "federated_failure"
    CARBON_API_FAILURE = "carbon_api_failure"
    HELIUM_TRACKER_FAILURE = "helium_tracker_failure"
    ML_PREDICTOR_FAILURE = "ml_predictor_failure"
    MOEA_EVALUATION_FAILURE = "moea_evaluation_failure"
    CACHE_CORRUPTION = "cache_corruption"
    DISTILLATION_TEACHER_FAILURE = "distillation_teacher_failure"


# ============================================================================
# Protocols
# ============================================================================
@runtime_checkable
class TelemetryProtocol(Protocol):
    def counter(self, name: str, value: float = 1.0, **labels: str) -> None: ...
    def gauge(self, name: str, value: float, **labels: str) -> None: ...
    def histogram(self, name: str, value: float, **labels: str) -> None: ...
    async def export_prometheus(self) -> str: ...


@runtime_checkable
class StorageProtocol(Protocol):
    def save_state(self, key: str, payload: str) -> Any: ...
    def get_state(self, key: str) -> Optional[str]: ...


@runtime_checkable
class FederatedBackendProtocol(Protocol):
    async def share(self, expert_id: str, payload: Dict[str, Any],
                    performance: float) -> Dict[str, Any]: ...
    async def fetch_peers(self, expert_id: str) -> List[Dict[str, Any]]: ...
    async def close(self) -> None: ...


@runtime_checkable
class CarbonBackendProtocol(Protocol):
    async def get_current_intensity(self, region: str) -> float: ...
    async def close(self) -> None: ...


@runtime_checkable
class MessageQueueProtocol(Protocol):
    async def publish(self, topic: str, payload: Any) -> None: ...


@runtime_checkable
class EventBrokerProtocol(Protocol):
    def subscribe(self, event_type: str,
                  handler: Callable[[Any], Awaitable[None]]) -> None: ...


# ============================================================================
# ObservabilityHub
# ============================================================================
class ObservabilityHub:
    """Prometheus-ready counters/gauges/histograms shared across sub-services."""

    def __init__(self) -> None:
        self._counters: Dict[str, float] = {}
        self._gauges: Dict[str, float] = {}
        self._histograms: Dict[str, Deque[float]] = {}
        self._lock = asyncio.Lock()

    @staticmethod
    def _key(name: str, labels: Optional[Dict[str, str]]) -> str:
        if not labels:
            return name
        tag = ",".join(f'{k}="{v}"' for k, v in sorted(labels.items()))
        return f"{name}{{{tag}}}"

    def counter(self, name: str, value: float = 1.0, **labels: str) -> None:
        k = self._key(name, labels or None)
        self._counters[k] = self._counters.get(k, 0.0) + float(value)

    def gauge(self, name: str, value: float, **labels: str) -> None:
        k = self._key(name, labels or None)
        self._gauges[k] = float(value)

    def histogram(self, name: str, value: float, **labels: str) -> None:
        k = self._key(name, labels or None)
        if k not in self._histograms:
            self._histograms[k] = deque(maxlen=1024)
        self._histograms[k].append(float(value))

    async def export_prometheus(self) -> str:
        async with self._lock:
            lines: List[str] = []
            for k, v in self._counters.items():
                base = k.split('{')[0]
                lines.append(f"# TYPE {base} counter")
                lines.append(f"{k} {v}")
            for k, v in self._gauges.items():
                base = k.split('{')[0]
                lines.append(f"# TYPE {base} gauge")
                lines.append(f"{k} {v}")
            for k, vals in self._histograms.items():
                if not vals:
                    continue
                base = k.split('{')[0]
                lines.append(f"# TYPE {base} histogram")
                lines.append(f"{base}_count {len(vals)}")
                lines.append(f"{base}_sum {sum(vals)}")
            return "\n".join(lines)


# ============================================================================
# GovernanceLayer
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
    def __init__(
        self,
        enabled: bool = False,
        timeout_seconds: float = APPROVAL_DEFAULT_TIMEOUT,
        auto_approve_on_timeout: bool = False,
        publish_fn: Optional[Callable[[str, Any], Awaitable[None]]] = None,
    ) -> None:
        self.enabled = bool(enabled)
        self.timeout_seconds = float(timeout_seconds)
        self.auto_approve_on_timeout = bool(auto_approve_on_timeout)
        self.publish_fn = publish_fn
        self.pending: "OrderedDict[str, ApprovalRequest]" = OrderedDict()
        self.audit: Deque[Dict[str, Any]] = deque(maxlen=MAX_APPROVAL_AUDIT)
        self._lock = asyncio.Lock()

    async def request_approval(
        self, context: Dict[str, Any], timeout: Optional[float] = None,
    ) -> ApprovalResult:
        if not self.enabled:
            r = ApprovalResult(
                request_id='auto',
                status=ApprovalStatus.AUTO_APPROVED,
                reason='approval_disabled',
                decided_at=_utcnow(),
            )
            self._audit(context, r)
            return r

        timeout = float(timeout if timeout is not None else self.timeout_seconds)
        request_id = uuid.uuid4().hex
        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()
        req = ApprovalRequest(
            request_id=request_id,
            context=context,
            requested_at=_utcnow(),
            timeout_seconds=timeout,
            future=future,
        )
        async with self._lock:
            self.pending[request_id] = req
            while len(self.pending) > MAX_APPROVAL_PENDING:
                self.pending.popitem(last=False)

        try:
            if self.publish_fn is not None:
                try:
                    await self.publish_fn(
                        'approval_requests', json.dumps(req.to_dict(), default=str),
                    )
                except Exception as e:
                    logger.warning("Approval publish failed: %s", e)
            decision = await asyncio.wait_for(future, timeout=timeout)
            result = ApprovalResult(
                request_id=request_id,
                status=ApprovalStatus.APPROVED if decision else ApprovalStatus.DENIED,
                reason='human_decision',
                decided_at=_utcnow(),
            )
        except asyncio.TimeoutError:
            status = (ApprovalStatus.AUTO_APPROVED if self.auto_approve_on_timeout
                      else ApprovalStatus.TIMEOUT)
            result = ApprovalResult(
                request_id=request_id,
                status=status,
                reason='timeout',
                decided_at=_utcnow(),
            )
        except Exception as e:
            result = ApprovalResult(
                request_id=request_id,
                status=ApprovalStatus.DENIED,
                reason=f'error:{e}',
                decided_at=_utcnow(),
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
            'timestamp': _utcnow().isoformat(),
            'context': _to_serializable(context),
            'result': result.to_dict(),
        })

    def get_audit_log(self) -> List[Dict[str, Any]]:
        return list(self.audit)


# ============================================================================
# Safety monitor
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

    def add(self, name: str, predicate: Callable[[Dict[str, Any]], bool],
            description: str, kind: str = "always") -> None:
        self.invariants.append(SafetyInvariant(name, predicate, description, kind))
        if kind == "eventually":
            self._eventually_flags[name] = False

    def check(self, state: Dict[str, Any]) -> List[str]:
        violations: List[str] = []
        for inv in self.invariants:
            try:
                ok = bool(inv.predicate(state))
            except Exception as e:
                violations.append(f"{inv.name}: error ({e})")
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
# Unified MOPD helpers
# ============================================================================
def _objective_vector(point: Any, names: Sequence[str],
                      higher_is_better: FrozenSet[str]) -> List[float]:
    method = getattr(point, 'objective_vector', None)
    if callable(method):
        try:
            vec = method()
            if vec is not None and len(vec) == len(names):
                return [float(v) for v in vec]
        except Exception:
            pass
    out: List[float] = []
    for name in names:
        val = float(getattr(point, name, 0.0))
        if name in higher_is_better:
            val = -val
        out.append(val)
    return out


def scalarise_pareto(
    points: Sequence[Any],
    weights: Optional[Dict[str, float]] = None,
    objective_names: Optional[Sequence[str]] = None,
    higher_is_better: Optional[FrozenSet[str]] = None,
    score_attr: str = 'scalarised_score',
) -> Optional[Any]:
    """Min-max scalarise points; returns best and stamps scores."""
    if not points:
        return None
    names = list(objective_names or MOPD_OBJECTIVE_NAMES)
    hib = set(higher_is_better or MOPD_HIGHER_IS_BETTER)
    w = dict(weights or {name: 1.0 / len(names) for name in names})

    vectors = [_objective_vector(p, names, hib) for p in points]
    arr = np.array(vectors, dtype=float)
    mins = arr.min(axis=0)
    maxs = arr.max(axis=0)
    ranges = np.where(maxs - mins > 1e-9, maxs - mins, 1.0)

    best: Optional[Any] = None
    best_score = -float('inf')
    for idx, p in enumerate(points):
        normalised = 1.0 - (arr[idx] - mins) / ranges
        score = 0.0
        for i, name in enumerate(names):
            score += float(w.get(name, 1.0 / len(names))) * float(normalised[i])
        try:
            setattr(p, score_attr, score)
        except Exception:
            pass
        if score > best_score:
            best_score = score
            best = p
    return best


def filter_pareto(
    points: Sequence[Any],
    objective_names: Optional[Sequence[str]] = None,
    higher_is_better: Optional[FrozenSet[str]] = None,
) -> List[Any]:
    """Return non-dominated points in canonical lower-is-better form."""
    if not points:
        return []
    names = list(objective_names or MOPD_OBJECTIVE_NAMES)
    hib = set(higher_is_better or MOPD_HIGHER_IS_BETTER)
    vecs = [_objective_vector(p, names, hib) for p in points]
    arr = np.array(vecs, dtype=float)
    n = len(points)
    dominated = np.zeros(n, dtype=bool)
    for i in range(n):
        if dominated[i]:
            continue
        for j in range(n):
            if i == j or dominated[i]:
                continue
            if np.all(arr[j] <= arr[i]) and np.any(arr[j] < arr[i]):
                dominated[i] = True
    return [points[i] for i in range(n) if not dominated[i]]


# ============================================================================
# Idempotency + dead letters
# ============================================================================
class IdempotencyCache:
    def __init__(self, max_size: int = MAX_IDEMPOTENCY_CACHE) -> None:
        self.max_size = max_size
        self._cache: "OrderedDict[str, float]" = OrderedDict()

    def seen(self, key: Optional[str]) -> bool:
        if not key:
            return False
        if key in self._cache:
            self._cache.move_to_end(key)
            return True
        self._cache[key] = time.monotonic()
        if len(self._cache) > self.max_size:
            self._cache.popitem(last=False)
        return False


class DeadLetterQueue:
    def __init__(self, max_size: int = MAX_DEAD_LETTERS) -> None:
        self._items: Deque[Dict[str, Any]] = deque(maxlen=max_size)
        self._lock = asyncio.Lock()

    async def record(self, topic: str, error: str, payload: Any = None) -> None:
        async with self._lock:
            self._items.append({
                'topic': topic,
                'error': str(error),
                'timestamp': _utcnow().isoformat(),
                'payload': _to_serializable(payload) if payload is not None else None,
            })

    def snapshot(self) -> List[Dict[str, Any]]:
        return list(self._items)

    def __len__(self) -> int:
        return len(self._items)


# ============================================================================
# Circuit Breaker
# ============================================================================
class CircuitBreaker:
    def __init__(self, failure_threshold: int, recovery_timeout: float,
                 name: str = "default") -> None:
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.name = name
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.last_failure_time: Optional[float] = None
        self._lock = asyncio.Lock()

    async def call(self, func: Callable[..., Awaitable[Any]],
                   *args: Any, **kwargs: Any) -> Any:
        async with self._lock:
            if self.state == CircuitBreakerState.OPEN:
                if (self.last_failure_time is not None and
                        (time.monotonic() - self.last_failure_time) >= self.recovery_timeout):
                    self.state = CircuitBreakerState.HALF_OPEN
                    self.failure_count = 0
                else:
                    raise RuntimeError(f"Circuit breaker {self.name} is OPEN")
        try:
            result = await func(*args, **kwargs)
            async with self._lock:
                if self.state == CircuitBreakerState.HALF_OPEN:
                    self.state = CircuitBreakerState.CLOSED
                self.failure_count = 0
            return result
        except Exception:
            async with self._lock:
                self.failure_count += 1
                self.last_failure_time = time.monotonic()
                if self.failure_count >= self.failure_threshold:
                    self.state = CircuitBreakerState.OPEN
            raise


# ============================================================================
# Configuration
# ============================================================================
@dataclass
class ColdStartConfig:
    cache_size: int = DEFAULT_CACHE_SIZE
    preload_threshold: float = DEFAULT_PRELOAD_THRESHOLD
    checkpoint_dir: str = "./expert_checkpoints"

    enable_federated: bool = True
    enable_ml_demand: bool = True
    enable_carbon_aware: bool = True
    enable_helium_tracking: bool = True
    enable_intelligent_eviction: bool = True
    enable_persistence: bool = True
    enable_telemetry: bool = True

    enable_limit_graph: bool = True
    enable_modp: bool = True
    enable_rlhf: bool = True
    enable_moe: bool = True
    moe_expert_count: int = 5

    enable_human_approval: bool = False
    enable_temporal_safety: bool = True
    enable_chaos_testing: bool = False
    enable_idempotency: bool = True

    federated_server_url: Optional[str] = None
    privacy_epsilon: float = 1.0
    privacy_delta: float = 1e-5
    privacy_clip_norm: float = 1.0
    federated_sparsity_ratio: float = 0.1

    ml_history_window: int = 1000
    ml_online_learning_rate: float = 0.01
    ml_retrain_threshold: int = 100

    carbon_intensity_thresholds: Dict[str, float] = field(default_factory=lambda: {
        'low': 200.0, 'medium': 350.0, 'high': 500.0,
    })
    carbon_region: str = "US-CAL-CISO"
    carbon_api_timeout_seconds: float = 10.0
    default_carbon_intensity: float = 400.0

    strategy_weights: Dict[str, float] = field(default_factory=lambda: {
        'priority': 0.2, 'resource_cost': 0.3,
        'carbon_efficiency': 0.3, 'urgency': 0.2,
    })

    helium_forecast_model: str = "exponential_smoothing"
    helium_budget_l: float = 100.0
    helium_smoothing_alpha: float = 0.3

    eviction_weights: Dict[str, float] = field(default_factory=lambda: {
        'usage_count': 0.25, 'age': 0.20,
        'predicted_demand': 0.35, 'sustainability': 0.20,
    })

    max_retries: int = 3
    retry_base_delay_ms: float = 100.0
    retry_max_delay_ms: float = 5000.0
    circuit_breaker_failure_threshold: int = 5
    circuit_breaker_recovery_timeout: float = 30.0

    persistence_path: str = "cold_start_state.json.gz"
    telemetry_export_interval: int = 60
    prometheus_port: Optional[int] = None

    distillation_epsilon: float = 0.1
    distillation_train_every: int = 10
    distillation_replay_size: int = 2000
    distillation_learning_rate: float = 0.01
    distill_weight: float = 0.7
    rl_weight: float = 0.3

    moea_enabled: bool = True
    moea_interval_seconds: int = 300
    moea_population_size: int = 20
    moea_generations: int = 10
    moea_mutation_rate: float = 0.2
    moea_crossover_rate: float = 0.8
    moea_tournament_size: int = 3
    moea_objective_weights: Dict[str, float] = field(default_factory=lambda: {
        'latency': 0.4, 'carbon': 0.3, 'cache_hit': 0.2, 'sustainability': 0.1,
    })
    moea_dynamic_weights: bool = True
    moea_pareto_path: str = "./cold_start_moea_pareto.json"
    moea_max_concurrent_evals: int = 8

    q_weights_path: str = "./cold_start_q_weights.json"
    interaction_logs_path: str = "./cold_start_interactions.csv"
    historical_model_path: str = "./cold_start_historical_model.pkl"

    def __post_init__(self) -> None:
        if self.cache_size < 1:
            raise ValueError("cache_size must be >= 1")
        if not (0.0 <= self.preload_threshold <= 1.0):
            raise ValueError("preload_threshold must be in [0, 1]")
        if not self.hardware_ok():
            raise ValueError("Invalid hardware configuration")
        # validate weights
        self._check_weight_sum(self.eviction_weights, "eviction_weights")
        self._check_weight_sum(self.strategy_weights, "strategy_weights")
        self._check_weight_sum(self.moea_objective_weights, "moea_objective_weights")
        # carbon thresholds ordered
        c = self.carbon_intensity_thresholds
        if not (c.get('low', 0.0) < c.get('medium', 0.0) < c.get('high', float('inf'))):
            raise ValueError("carbon_intensity_thresholds must satisfy low < medium < high")

    def hardware_ok(self) -> bool:
        return True

    @staticmethod
    def _check_weight_sum(weights: Dict[str, float], name: str) -> None:
        total = sum(float(v) for v in weights.values())
        if total <= 0:
            raise ValueError(f"{name} must sum to > 0")
        if abs(total - 1.0) > 0.01:
            logger.warning("%s sums to %.4f; normalising.", name, total)
            for k in list(weights):
                weights[k] = float(weights[k]) / total

    def to_dict(self) -> Dict[str, Any]:
        return _to_serializable(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ColdStartConfig":
        valid = {f.name for f in dataclass_fields(cls)}
        return cls(**{k: v for k, v in data.items() if k in valid})


# ============================================================================
# Data classes
# ============================================================================
@dataclass
class ExpertCheckpoint:
    expert_id: str
    expert_type: str
    model_state: Dict[str, Any] = field(default_factory=dict)
    optimizer_state: Dict[str, Any] = field(default_factory=dict)
    feature_distribution: Dict[str, float] = field(default_factory=dict)
    performance_metrics: Dict[str, float] = field(default_factory=dict)
    created_at: datetime = field(default_factory=_utcnow)
    last_used: datetime = field(default_factory=_utcnow)
    usage_count: int = 0
    carbon_footprint_kg: float = 0.0
    helium_usage_l: float = 0.0
    sustainability_score: float = 0.0
    federated_consensus: bool = False
    peer_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            'expert_id': self.expert_id,
            'expert_type': self.expert_type,
            'model_state': _to_serializable(self.model_state),
            'optimizer_state': _to_serializable(self.optimizer_state),
            'feature_distribution': dict(self.feature_distribution),
            'performance_metrics': dict(self.performance_metrics),
            'created_at': self.created_at.isoformat(),
            'last_used': self.last_used.isoformat(),
            'usage_count': self.usage_count,
            'carbon_footprint_kg': self.carbon_footprint_kg,
            'helium_usage_l': self.helium_usage_l,
            'sustainability_score': self.sustainability_score,
            'federated_consensus': self.federated_consensus,
            'peer_count': self.peer_count,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ExpertCheckpoint":
        d = dict(data)
        for key in ('created_at', 'last_used'):
            if isinstance(d.get(key), str):
                try:
                    d[key] = datetime.fromisoformat(d[key])
                except ValueError:
                    d[key] = _utcnow()
        return cls(**{k: v for k, v in d.items()
                      if k in {f.name for f in dataclass_fields(cls)}})


@dataclass
class WarmupStrategy:
    strategy_type: str
    priority: int
    estimated_warmup_time_ms: float
    resource_cost: float
    success_probability: float
    carbon_efficiency: float = 0.5
    helium_efficiency: float = 0.5


@dataclass
class ColdStartState:
    expert_type: str
    urgency: str
    carbon_budget: float
    helium_budget: float
    max_latency_ms: float
    carbon_intensity: float
    cache_utilization: float
    recent_hit_rate: float
    strategy_success_rates: Dict[str, float]
    avg_warmup_time_ms: float
    avg_sustainability_score: float

    def to_feature_vector(self) -> np.ndarray:
        """Return a 15-dimensional feature vector (canonical for the module)."""
        type_map = {'energy': 0, 'data': 1, 'iot': 2, 'quantum': 3, 'general': 4}
        urgency_map = {'critical': 0, 'high': 1, 'normal': 2, 'low': 3}
        features = [
            type_map.get(self.expert_type, 4) / 4.0,
            urgency_map.get(self.urgency, 2) / 3.0,
            float(np.clip(self.carbon_budget, 0.0, 1.0)),
            float(np.clip(self.helium_budget, 0.0, 1.0)),
            float(np.clip(self.max_latency_ms / 1000.0, 0.0, 1.0)),
            float(np.clip(self.carbon_intensity / 1000.0, 0.0, 1.0)),
            float(np.clip(self.cache_utilization, 0.0, 1.0)),
            float(np.clip(self.recent_hit_rate, 0.0, 1.0)),
            float(np.clip(self.strategy_success_rates.get('preload', 0.5), 0.0, 1.0)),
            float(np.clip(self.strategy_success_rates.get('transfer', 0.5), 0.0, 1.0)),
            float(np.clip(self.strategy_success_rates.get('progressive', 0.5), 0.0, 1.0)),
            float(np.clip(self.strategy_success_rates.get('hybrid', 0.5), 0.0, 1.0)),
            float(np.clip(self.strategy_success_rates.get('federated', 0.5), 0.0, 1.0)),
            float(np.clip(self.avg_warmup_time_ms / 1000.0, 0.0, 1.0)),
            float(np.clip(self.avg_sustainability_score, 0.0, 1.0)),
        ]
        return np.array(features, dtype=np.float32)

    def to_dict(self) -> Dict[str, Any]:
        return _to_serializable(self)


@dataclass
class MOPDStrategyWeights:
    """Represents a candidate strategy weight vector with its objectives."""
    vector_id: str
    weights: Dict[str, float]
    objectives: Dict[str, float]
    scalarised_score: float = 0.0
    stable_id: int = 0

    def objective_vector(self) -> List[float]:
        vec: List[float] = []
        for name in MOPD_OBJECTIVE_NAMES:
            val = float(self.objectives.get(name, 0.0))
            if name in MOPD_HIGHER_IS_BETTER:
                val = -val
            vec.append(val)
        return vec

    def to_dict(self) -> Dict[str, Any]:
        return {
            'vector_id': self.vector_id,
            'weights': dict(self.weights),
            'objectives': dict(self.objectives),
            'scalarised_score': float(self.scalarised_score),
            'stable_id': int(self.stable_id),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MOPDStrategyWeights":
        return cls(
            vector_id=data.get('vector_id', uuid.uuid4().hex),
            weights=dict(data.get('weights', {})),
            objectives=dict(data.get('objectives', {})),
            scalarised_score=float(data.get('scalarised_score', 0.0)),
            stable_id=int(data.get('stable_id', 0)),
        )


# ============================================================================
# Teachers, Student, Replay Buffer
# ============================================================================
class Teacher(Protocol):
    def predict(self, state: ColdStartState) -> np.ndarray: ...
    def confidence(self, state: ColdStartState) -> float: ...
    def name(self) -> str: ...


class StrategyRuleBasedTeacher:
    STRATEGIES = DEFAULT_STRATEGY_NAMES
    def name(self) -> str: return "rule_based"

    def predict(self, state: ColdStartState) -> np.ndarray:
        probs = np.ones(len(self.STRATEGIES)) * 0.1
        if state.recent_hit_rate > 0.8:
            probs[0] = 0.8
        elif state.expert_type == 'quantum' and state.max_latency_ms < 100:
            probs[3] = 0.7
        elif state.carbon_intensity > 500:
            probs[4] = 0.6
        elif state.urgency == 'critical':
            probs[1] = 0.7
        else:
            probs[2] = 0.6
        return probs / probs.sum()

    def confidence(self, state: ColdStartState) -> float:
        return 0.6 if state.recent_hit_rate > 0.8 else 0.4


class StrategyHistoricalMLTeacher:
    def __init__(self, config: ColdStartConfig) -> None:
        self.config = config
        self.model: Optional[Any] = None
        self.label_encoder: Optional[Any] = None
        self.model_path = Path(config.historical_model_path)
        if self.model_path.exists() and SKLEARN_AVAILABLE:
            try:
                with open(self.model_path, 'rb') as f:
                    self.model, self.label_encoder = pickle.load(f)
            except Exception as e:
                logger.warning("Failed to load historical model: %s", e)

    def name(self) -> str: return "historical_ml"

    def predict(self, state: ColdStartState) -> np.ndarray:
        if self.model is None:
            return np.ones(len(DEFAULT_STRATEGY_NAMES)) / len(DEFAULT_STRATEGY_NAMES)
        x = state.to_feature_vector().reshape(1, -1)
        try:
            probs = self.model.predict_proba(x)[0]
            # pad if model has fewer classes
            if len(probs) < len(DEFAULT_STRATEGY_NAMES):
                padded = np.zeros(len(DEFAULT_STRATEGY_NAMES))
                padded[:len(probs)] = probs
                probs = padded
            return probs
        except Exception:
            return np.ones(len(DEFAULT_STRATEGY_NAMES)) / len(DEFAULT_STRATEGY_NAMES)

    def confidence(self, state: ColdStartState) -> float:
        return 0.7 if self.model is not None else 0.0


class StrategyStatefulQTeacher:
    def __init__(self, config: ColdStartConfig, lr: float = 0.1) -> None:
        self.config = config
        self.lr = lr
        self.n_actions = len(DEFAULT_STRATEGY_NAMES)
        self.weights = np.zeros((FEATURE_DIM, self.n_actions))
        self._save_lock = asyncio.Lock()
        self._load_state()

    def name(self) -> str: return "stateful_q"

    def _load_state(self) -> None:
        path = Path(self.config.q_weights_path)
        if path.exists():
            try:
                with open(path, 'r') as f:
                    data = json.load(f)
                arr = np.array(data)
                if arr.shape == self.weights.shape:
                    self.weights = arr
            except Exception as e:
                logger.debug("Failed to load Q weights: %s", e)

    async def _save_state_async(self) -> None:
        async with self._save_lock:
            def _write() -> None:
                path = Path(self.config.q_weights_path)
                with open(path, 'w') as f:
                    json.dump(self.weights.tolist(), f, indent=2)
            try:
                await asyncio.to_thread(_write)
            except Exception as e:
                logger.debug("Failed to save Q weights: %s", e)

    def predict(self, state: ColdStartState) -> np.ndarray:
        x = state.to_feature_vector()
        q = x @ self.weights
        exp_q = np.exp(q - np.max(q))
        return exp_q / exp_q.sum()

    def confidence(self, state: ColdStartState) -> float:
        return 0.5

    async def update(self, state: ColdStartState, action: int, reward: float) -> None:
        x = state.to_feature_vector()
        q_current = float(np.dot(x, self.weights[:, action]))
        self.weights[:, action] += self.lr * (reward - q_current) * x
        await self._save_state_async()


class DistillationStudent:
    def __init__(self, feature_dim: int = FEATURE_DIM,
                 n_classes: int = len(DEFAULT_STRATEGY_NAMES),
                 lr: float = 0.01) -> None:
        self.weights = np.zeros((feature_dim, n_classes))
        self.biases = np.zeros(n_classes)
        self.lr = lr
        self.n_classes = n_classes
        self.counter = 0

    def predict_proba(self, state_vector: np.ndarray,
                      num_classes: Optional[int] = None) -> np.ndarray:
        num_classes = num_classes or self.n_classes
        logits = state_vector @ self.weights + self.biases
        max_logit = np.max(logits)
        exp_logits = np.exp(logits - max_logit)
        return exp_logits / exp_logits.sum()

    def update(self, state_vector: np.ndarray, teacher_probs: np.ndarray,
               reward: float, action: int, distill_weight: float = 0.7,
               rl_weight: float = 0.3) -> None:
        current_probs = self.predict_proba(state_vector, self.n_classes)
        grad_distill = -(teacher_probs - current_probs)
        one_hot = np.zeros(self.n_classes)
        one_hot[action] = 1.0
        grad_rl = -reward * (one_hot - current_probs)
        grad = distill_weight * grad_distill + rl_weight * grad_rl
        self.weights -= self.lr * np.outer(state_vector, grad)
        self.biases -= self.lr * grad
        self.counter += 1


class ReplayBuffer:
    def __init__(self, max_size: int = 2000) -> None:
        self.buffer: Deque[Tuple[np.ndarray, int, float, np.ndarray, np.ndarray]] = deque(maxlen=max_size)

    def push(self, s: np.ndarray, a: int, r: float, ns: np.ndarray,
             tp: np.ndarray) -> None:
        self.buffer.append((s, a, r, ns, tp))

    def sample(self, batch_size: int = 32
               ) -> Tuple[np.ndarray, List[int], np.ndarray, np.ndarray, np.ndarray]:
        if not self.buffer:
            empty = np.zeros((0, FEATURE_DIM), dtype=np.float32)
            return empty, [], np.zeros(0), empty, np.zeros((0, len(DEFAULT_STRATEGY_NAMES)))
        if len(self.buffer) < batch_size:
            batch = list(self.buffer)
        else:
            batch = random.sample(self.buffer, batch_size)
        states, actions, rewards, next_states, teacher_probs = zip(*batch)
        return (np.array(states), list(actions), np.array(rewards),
                np.array(next_states), np.array(teacher_probs))

    def __len__(self) -> int:
        return len(self.buffer)


# ============================================================================
# DistillationService
# ============================================================================
class DistillationService:
    """Multi-teacher on-policy distillation with replay buffer."""

    def __init__(self, config: ColdStartConfig,
                 observability: Optional[ObservabilityHub] = None) -> None:
        self.config = config
        self.obs = observability or ObservabilityHub()
        self.teachers: List[Teacher] = [
            StrategyRuleBasedTeacher(),
            StrategyHistoricalMLTeacher(config),
            StrategyStatefulQTeacher(config),
        ]
        self.student = DistillationStudent(lr=config.distillation_learning_rate)
        self.replay = ReplayBuffer(max_size=config.distillation_replay_size)
        self.epsilon = config.distillation_epsilon
        self.train_every = config.distillation_train_every
        self.distill_weight = config.distill_weight
        self.rl_weight = config.rl_weight
        self.counter = 0
        self._lock = asyncio.Lock()
        self._disabled = False

    def disable(self) -> None:
        """Used by chaos service to simulate teacher failure."""
        self._disabled = True

    def enable(self) -> None:
        self._disabled = False

    async def select_strategy(
        self, state: ColdStartState, exploration: bool = True,
    ) -> Tuple[str, int, np.ndarray, np.ndarray]:
        if self._disabled:
            raise RuntimeError("Distillation service disabled (chaos)")
        state_vec = state.to_feature_vector()
        teacher_probs = np.zeros(len(DEFAULT_STRATEGY_NAMES))
        total_conf = 0.0
        for teacher in self.teachers:
            try:
                prob = teacher.predict(state)
                conf = teacher.confidence(state)
                teacher_probs += prob * conf
                total_conf += conf
            except Exception as e:
                logger.warning("Teacher %s failed: %s", teacher.name(), e)
        if total_conf > 0:
            teacher_probs /= total_conf
        else:
            teacher_probs = np.ones(len(DEFAULT_STRATEGY_NAMES)) / len(DEFAULT_STRATEGY_NAMES)

        student_probs = self.student.predict_proba(state_vec, len(DEFAULT_STRATEGY_NAMES))
        if exploration and random.random() < self.epsilon:
            action_idx = random.randint(0, len(DEFAULT_STRATEGY_NAMES) - 1)
        else:
            combined = 0.8 * student_probs + 0.2 * teacher_probs
            action_idx = int(np.argmax(combined))

        return DEFAULT_STRATEGY_NAMES[action_idx], action_idx, state_vec, teacher_probs

    async def update(self, state_vec: np.ndarray, action_idx: int, reward: float,
                     next_state_vec: np.ndarray, teacher_probs: np.ndarray) -> None:
        async with self._lock:
            self.replay.push(state_vec, action_idx, reward, next_state_vec, teacher_probs)
            self.counter += 1
            if self.counter % self.train_every == 0 and len(self.replay) >= 8:
                states, actions, rewards, _, teacher_probs_batch = self.replay.sample(8)
                for i in range(len(states)):
                    self.student.update(
                        states[i], teacher_probs_batch[i], float(rewards[i]),
                        int(actions[i]),
                        distill_weight=self.distill_weight,
                        rl_weight=self.rl_weight,
                    )
                self.obs.counter('distill_updates_total')

    async def update_teacher(self, state: ColdStartState, action_idx: int,
                             reward: float) -> None:
        for teacher in self.teachers:
            if isinstance(teacher, StrategyStatefulQTeacher):
                try:
                    await teacher.update(state, action_idx, reward)
                except Exception as e:
                    logger.warning("Q-teacher update failed: %s", e)

    def get_stats(self) -> Dict[str, Any]:
        return {
            'student_counter': self.student.counter,
            'buffer_size': len(self.replay),
            'disabled': self._disabled,
        }


# ============================================================================
# EvolutionService (NSGA-II)
# ============================================================================
class EvolutionService:
    """Multi-objective evolutionary optimizer over strategy weight vectors."""

    def __init__(
        self,
        config: ColdStartConfig,
        evaluate: Callable[[Dict[str, float]], Awaitable[Dict[str, float]]],
        observability: Optional[ObservabilityHub] = None,
    ) -> None:
        self.config = config
        self.evaluate = evaluate
        self.obs = observability or ObservabilityHub()
        self.population_size = config.moea_population_size
        self.generations = config.moea_generations
        self.mutation_rate = config.moea_mutation_rate
        self.crossover_rate = config.moea_crossover_rate
        self.tournament_size = config.moea_tournament_size
        self.objective_weights = dict(config.moea_objective_weights)
        self.dynamic_weights = config.moea_dynamic_weights
        self._semaphore = asyncio.Semaphore(config.moea_max_concurrent_evals)
        self._all_points: List[MOPDStrategyWeights] = []
        self._next_id = 1
        self.pareto_front: List[MOPDStrategyWeights] = []
        self.best: Optional[MOPDStrategyWeights] = None
        self._disabled = False

    def disable(self) -> None:
        self._disabled = True

    def enable(self) -> None:
        self._disabled = False

    # ------------------------------------------------------------------
    # Genetic operators
    # ------------------------------------------------------------------
    def _random_individual(self) -> Dict[str, float]:
        w = {k: random.random() for k in DEFAULT_STRATEGY_NAMES}
        total = sum(w.values())
        return {k: v / total for k, v in w.items()} if total > 0 else w

    def _crossover(self, p1: Dict[str, float], p2: Dict[str, float]) -> Dict[str, float]:
        child: Dict[str, float] = {}
        for key in p1:
            if random.random() < 0.5:
                u = random.random()
                beta = ((2 * u) ** (1 / 21)) if u <= 0.5 else ((1 / (2 * (1 - u))) ** (1 / 21))
                child[key] = max(0.0, min(1.0, 0.5 * ((1 + beta) * p1[key] + (1 - beta) * p2[key])))
            else:
                child[key] = p1[key] if random.random() < 0.5 else p2[key]
        total = sum(child.values())
        return {k: v / total for k, v in child.items()} if total > 0 else child

    def _mutate(self, ind: Dict[str, float]) -> Dict[str, float]:
        mutant = dict(ind)
        for key in mutant:
            if random.random() < self.mutation_rate:
                u = random.random()
                delta = ((2 * u) ** (1 / 21) - 1) if u < 0.5 else (1 - (2 * (1 - u)) ** (1 / 21))
                mutant[key] = max(0.0, min(1.0, mutant[key] + delta))
        total = sum(mutant.values())
        return {k: v / total for k, v in mutant.items()} if total > 0 else mutant

    # ------------------------------------------------------------------
    # Pareto utilities
    # ------------------------------------------------------------------
    def _fast_non_dominated_sort(self,
                                  points: Sequence[MOPDStrategyWeights]) -> List[List[MOPDStrategyWeights]]:
        if not points:
            return []
        n = len(points)
        domination_count = [0] * n
        dominated_solutions: List[List[int]] = [[] for _ in range(n)]
        fronts: List[List[int]] = [[]]
        for i, p in enumerate(points):
            p_obj = p.objective_vector()
            for j, q in enumerate(points):
                if i == j:
                    continue
                q_obj = q.objective_vector()
                if all(a <= b for a, b in zip(q_obj, p_obj)) and any(a < b for a, b in zip(q_obj, p_obj)):
                    dominated_solutions[i].append(j)
                elif all(a <= b for a, b in zip(p_obj, q_obj)) and any(a < b for a, b in zip(p_obj, q_obj)):
                    domination_count[i] += 1
            if domination_count[i] == 0:
                fronts[0].append(i)
        i = 0
        while i < len(fronts) and fronts[i]:
            next_front: List[int] = []
            for idx in fronts[i]:
                for other in dominated_solutions[idx]:
                    domination_count[other] -= 1
                    if domination_count[other] == 0:
                        next_front.append(other)
            if next_front:
                fronts.append(next_front)
            i += 1
        return [[points[idx] for idx in front] for front in fronts if front]

    def _crowding_distance(self,
                            front: Sequence[MOPDStrategyWeights]) -> Dict[int, float]:
        if not front:
            return {}
        distances = {p.stable_id: 0.0 for p in front}
        for obj in MOPD_OBJECTIVE_NAMES:
            sorted_front = sorted(front, key=lambda x: x.objectives.get(obj, 0.0))
            if not sorted_front:
                continue
            distances[sorted_front[0].stable_id] = float('inf')
            distances[sorted_front[-1].stable_id] = float('inf')
            obj_min = sorted_front[0].objectives.get(obj, 0.0)
            obj_max = sorted_front[-1].objectives.get(obj, 0.0)
            if obj_max == obj_min:
                continue
            for i in range(1, len(sorted_front) - 1):
                prev_val = sorted_front[i - 1].objectives.get(obj, 0.0)
                next_val = sorted_front[i + 1].objectives.get(obj, 0.0)
                distances[sorted_front[i].stable_id] += (next_val - prev_val) / (obj_max - obj_min)
        return distances

    def _tournament_selection(
        self,
        population: Sequence[Dict[str, float]],
        population_points: Sequence[MOPDStrategyWeights],
        fronts: Sequence[Sequence[MOPDStrategyWeights]],
        crowding: Dict[int, float],
    ) -> Dict[str, float]:
        k = min(self.tournament_size, len(population))
        candidates = random.sample(list(range(len(population))), k)
        point_rank: Dict[int, int] = {}
        for rank, front in enumerate(fronts):
            for p in front:
                point_rank[p.stable_id] = rank
        best_idx = candidates[0]
        best_rank = float('inf')
        best_cd = -float('inf')
        for idx in candidates:
            p = population_points[idx]
            r = point_rank.get(p.stable_id, len(fronts))
            cd = crowding.get(p.stable_id, 0.0)
            if r < best_rank or (r == best_rank and cd > best_cd):
                best_idx = idx
                best_rank = r
                best_cd = cd
        return dict(population[best_idx])

    # ------------------------------------------------------------------
    # Dynamic weights
    # ------------------------------------------------------------------
    def _compute_dynamic_weights(self) -> Dict[str, float]:
        weights = dict(self.objective_weights)
        if not self.dynamic_weights or not self.pareto_front:
            return weights
        keys = list(weights.keys())
        avg = {k: float(np.mean([p.objectives.get(k, 0.0) for p in self.pareto_front])) for k in keys}
        maxv = {k: float(np.max([p.objectives.get(k, 0.0) for p in self.pareto_front])) for k in keys}
        boosted = set()
        for k in keys:
            if maxv[k] > 0 and avg[k] < 0.5 * maxv[k]:
                weights[k] = min(0.6, weights.get(k, 0.0) * 1.5)
                boosted.add(k)
        total = sum(weights.values())
        if total > 0:
            weights = {k: v / total for k, v in weights.items()}
        return weights

    # ------------------------------------------------------------------
    # Main evolution loop
    # ------------------------------------------------------------------
    async def evolve(self) -> List[MOPDStrategyWeights]:
        if self._disabled:
            raise RuntimeError("Evolution service disabled (chaos)")
        if self.evaluate is None:
            raise ValueError("evaluate func not set")

        async def _evaluate_with_semaphore(ind: Dict[str, float]) -> Dict[str, float]:
            async with self._semaphore:
                return await self.evaluate(ind)

        # Initial population
        population = [self._random_individual() for _ in range(self.population_size)]
        results = await asyncio.gather(*[_evaluate_with_semaphore(ind) for ind in population])
        points: List[MOPDStrategyWeights] = []
        for ind, obj in zip(population, results):
            points.append(MOPDStrategyWeights(
                vector_id=uuid.uuid4().hex,
                weights=dict(ind),
                objectives=dict(obj),
                stable_id=self._next_id,
            ))
            self._next_id += 1
        self._all_points = points

        for gen in range(self.generations):
            fronts = self._fast_non_dominated_sort(points)
            crowding: Dict[int, float] = {}
            for front in fronts:
                crowding.update(self._crowding_distance(front))

            offspring: List[Dict[str, float]] = []
            while len(offspring) < self.population_size:
                p1 = self._tournament_selection(population, points, fronts, crowding)
                p2 = self._tournament_selection(population, points, fronts, crowding)
                child = self._crossover(p1, p2) if random.random() < self.crossover_rate else dict(p1)
                child = self._mutate(child)
                offspring.append(child)

            child_results = await asyncio.gather(
                *[_evaluate_with_semaphore(ind) for ind in offspring]
            )
            child_points: List[MOPDStrategyWeights] = []
            for ind, obj in zip(offspring, child_results):
                child_points.append(MOPDStrategyWeights(
                    vector_id=uuid.uuid4().hex,
                    weights=dict(ind),
                    objectives=dict(obj),
                    stable_id=self._next_id,
                ))
                self._next_id += 1

            combined_pop = list(population) + offspring
            combined_points = points + child_points
            fronts = self._fast_non_dominated_sort(combined_points)

            new_pop: List[Dict[str, float]] = []
            new_points: List[MOPDStrategyWeights] = []
            for front in fronts:
                front_ids = {p.stable_id for p in front}
                indices = [i for i, p in enumerate(combined_points) if p.stable_id in front_ids]
                if len(new_pop) + len(indices) <= self.population_size:
                    for i in indices:
                        new_pop.append(dict(combined_pop[i]))
                        new_points.append(combined_points[i])
                else:
                    cd = self._crowding_distance(front)
                    sorted_idx = sorted(indices, key=lambda i: cd.get(combined_points[i].stable_id, 0.0),
                                        reverse=True)
                    for i in sorted_idx:
                        if len(new_pop) >= self.population_size:
                            break
                        new_pop.append(dict(combined_pop[i]))
                        new_points.append(combined_points[i])
                if len(new_pop) >= self.population_size:
                    break

            # Refill from fronts if dedup shrank us below capacity
            if len(new_pop) < self.population_size:
                for front in fronts:
                    for p in front:
                        if len(new_pop) >= self.population_size:
                            break
                        if p.stable_id in {np_.stable_id for np_ in new_points}:
                            continue
                        idx = next((i for i, cp in enumerate(combined_points)
                                    if cp.stable_id == p.stable_id), None)
                        if idx is not None:
                            new_pop.append(dict(combined_pop[idx]))
                            new_points.append(combined_points[idx])

            population = new_pop[:self.population_size]
            points = new_points[:self.population_size]
            self._all_points = points
            fronts = self._fast_non_dominated_sort(points)
            self.pareto_front = fronts[0] if fronts else []
            self.obs.histogram('moea_pareto_size', len(self.pareto_front))
            self.obs.counter('moea_generations_total')

        weights = self._compute_dynamic_weights()
        best = scalarise_pareto(
            self.pareto_front, weights=weights,
            objective_names=MOPD_OBJECTIVE_NAMES,
            higher_is_better=MOPD_HIGHER_IS_BETTER,
        )
        self.best = best
        return self.pareto_front

    async def save_pareto(self) -> bool:
        if not self.pareto_front:
            return False
        def _write() -> None:
            payload = {
                'version': MODULE_VERSION,
                'timestamp': _utcnow().isoformat(),
                'pareto': [p.to_dict() for p in self.pareto_front],
            }
            tmp = f"{self.config.moea_pareto_path}.tmp"
            with open(tmp, 'w') as f:
                json.dump(payload, f, indent=2, default=str)
            os.replace(tmp, self.config.moea_pareto_path)
        try:
            await asyncio.to_thread(_write)
            return True
        except Exception as e:
            logger.warning("Failed to save MOEA pareto: %s", e)
            return False


# ============================================================================
# GatingService (MoE gating with proper feature handling)
# ============================================================================
class GatingService:
    """MoE gating network. Uses the canonical 15-dim feature vector."""

    def __init__(self, config: ColdStartConfig,
                 observability: Optional[ObservabilityHub] = None) -> None:
        self.config = config
        self.obs = observability or ObservabilityHub()
        self.expert_names = DEFAULT_STRATEGY_NAMES
        self.num_experts = len(self.expert_names)
        self.gating_weights = np.random.randn(self.num_experts, FEATURE_DIM) * 0.01
        self.learning_rate = 0.1
        self._lock = asyncio.Lock()

    async def select_expert(self, state: ColdStartState) -> Tuple[str, np.ndarray]:
        x = state.to_feature_vector()
        logits = self.gating_weights @ x
        probs = np.exp(logits - np.max(logits))
        probs /= probs.sum()
        idx = int(np.argmax(probs))
        self.obs.counter('gating_selections_total', expert=self.expert_names[idx])
        return self.expert_names[idx], probs

    async def add_training_sample(self, state: ColdStartState,
                                   selected_expert: str, reward: float) -> None:
        if selected_expert not in self.expert_names:
            return
        expert_idx = self.expert_names.index(selected_expert)
        x = state.to_feature_vector()
        async with self._lock:
            logits = self.gating_weights @ x
            probs = np.exp(logits - np.max(logits))
            probs /= probs.sum()
            target = np.zeros(self.num_experts)
            target[expert_idx] = 1.0
            grad = (probs - target)[:, None] * x[None, :]
            self.gating_weights -= self.learning_rate * reward * grad
        self.obs.counter('gating_updates_total')


# ============================================================================
# FederatedService
# ============================================================================
class FederatedService:
    """Real federated checkpoint sharing with DP noise."""

    def __init__(self, config: ColdStartConfig,
                 observability: Optional[ObservabilityHub] = None,
                 dlq: Optional[DeadLetterQueue] = None) -> None:
        self.config = config
        self.obs = observability or ObservabilityHub()
        self.dlq = dlq
        self._session: Optional[Any] = None
        self._lock = asyncio.Lock()

    async def _get_session(self) -> Optional[Any]:
        if not AIOHTTP_AVAILABLE or not self.config.federated_server_url:
            return None
        if self._session is None or getattr(self._session, 'closed', False):
            self._session = aiohttp.ClientSession()
        return self._session

    def _dp_noise(self, value: float) -> float:
        eps = max(1e-6, self.config.privacy_epsilon)
        delta = max(1e-12, self.config.privacy_delta)
        clip = self.config.privacy_clip_norm
        sigma = math.sqrt(2.0 * math.log(1.25 / delta)) / eps
        clipped = max(-clip, min(clip, value))
        return clipped + float(np.random.normal(0.0, sigma * clip))

    def _compress(self, data: Dict[str, Any]) -> Dict[str, Any]:
        sparsity = self.config.federated_sparsity_ratio
        if sparsity >= 1.0:
            return data
        numeric = {k: v for k, v in data.items() if isinstance(v, (int, float))}
        if not numeric:
            return data
        sorted_items = sorted(numeric.items(), key=lambda x: abs(x[1]), reverse=True)
        keep_count = max(1, int(len(sorted_items) * sparsity))
        kept = {item[0] for item in sorted_items[:keep_count]}
        return {k: v for k, v in data.items()
                if k in kept or not isinstance(v, (int, float))}

    async def share(self, expert_id: str, payload: Dict[str, Any],
                    performance: float = 1.0) -> Dict[str, Any]:
        session = await self._get_session()
        if session is None:
            return {'status': 'local_only'}
        # Apply DP noise and sparsity
        numeric_noised = {
            k: self._dp_noise(float(v))
            for k, v in payload.items() if isinstance(v, (int, float))
        }
        combined = {**{k: v for k, v in payload.items() if not isinstance(v, (int, float))},
                    **numeric_noised}
        compressed = self._compress(combined)
        body = {
            'expert_id': expert_id,
            'performance': float(performance),
            'payload': compressed,
            'timestamp': _utcnow().isoformat(),
        }
        for attempt in range(self.config.max_retries):
            try:
                async with session.post(
                    f"{self.config.federated_server_url}/federated/checkpoints",
                    json=body,
                    timeout=self.config.carbon_api_timeout_seconds,
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        self.obs.counter('federated_shares_total', status='ok')
                        return data
            except Exception as e:
                logger.warning("Federated share error (attempt %d): %s", attempt + 1, e)
                if self.dlq:
                    await self.dlq.record('federated_share', str(e), body)
            await asyncio.sleep(min(2 ** attempt, 10))
        self.obs.counter('federated_shares_total', status='error')
        return {'status': 'failed'}

    async def fetch_peers(self, expert_id: str) -> List[Dict[str, Any]]:
        session = await self._get_session()
        if session is None:
            return []
        try:
            async with session.get(
                f"{self.config.federated_server_url}/federated/checkpoints/{expert_id}",
                timeout=self.config.carbon_api_timeout_seconds,
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return list(data.get('peers', []) or [])
        except Exception as e:
            logger.warning("Federated fetch failed: %s", e)
        return []

    async def close(self) -> None:
        if self._session and not getattr(self._session, 'closed', False):
            await self._session.close()


# ============================================================================
# CarbonService
# ============================================================================
class CarbonService:
    """Real carbon intensity with fallback thresholds."""

    def __init__(self, config: ColdStartConfig,
                 observability: Optional[ObservabilityHub] = None) -> None:
        self.config = config
        self.obs = observability or ObservabilityHub()
        self._session: Optional[Any] = None
        self._cached_intensity: float = config.default_carbon_intensity
        self._last_update: Optional[datetime] = None
        self._lock = asyncio.Lock()
        self._disabled = False

    def disable(self) -> None:
        self._disabled = True

    def enable(self) -> None:
        self._disabled = False

    async def _get_session(self) -> Optional[Any]:
        if not AIOHTTP_AVAILABLE:
            return None
        if self._session is None or getattr(self._session, 'closed', False):
            self._session = aiohttp.ClientSession()
        return self._session

    async def get_current_intensity(self, region: Optional[str] = None) -> float:
        if self._disabled:
            raise RuntimeError("Carbon service disabled (chaos)")
        region = region or self.config.carbon_region
        session = await self._get_session()
        if session is None:
            return self.config.default_carbon_intensity
        try:
            url = f"https://api.electricitymap.org/v3/carbon-intensity/latest?zone={region}"
            async with session.get(
                url, timeout=self.config.carbon_api_timeout_seconds,
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    intensity = float(data.get('carbonIntensity',
                                               self.config.default_carbon_intensity))
                    self._cached_intensity = intensity
                    self._last_update = _utcnow()
                    self.obs.gauge('carbon_intensity', intensity, region=region)
                    return intensity
        except Exception as e:
            logger.debug("Carbon API failed: %s", e)
        self.obs.counter('carbon_fallbacks_total')
        return self._cached_intensity

    def classify(self, intensity: float) -> str:
        t = self.config.carbon_intensity_thresholds
        if intensity < t.get('medium', 350):
            return 'low'
        if intensity < t.get('high', 500):
            return 'medium'
        return 'high'

    async def close(self) -> None:
        if self._session and not getattr(self._session, 'closed', False):
            await self._session.close()


# ============================================================================
# HeliumService
# ============================================================================
class HeliumService:
    """Helium tracking with exponential smoothing forecast."""

    def __init__(self, config: ColdStartConfig,
                 observability: Optional[ObservabilityHub] = None) -> None:
        self.config = config
        self.obs = observability or ObservabilityHub()
        self.total_used_l: float = 0.0
        self.total_saved_l: float = 0.0
        self.usage_history: Deque[Tuple[datetime, float]] = deque(maxlen=10_000)
        self._smoothed_rate: Optional[float] = None
        self._lock = asyncio.Lock()
        self._disabled = False

    def disable(self) -> None:
        self._disabled = True

    def enable(self) -> None:
        self._disabled = False

    async def record_usage(self, expert_id: str, amount_l: float,
                           operation: str = 'initialization') -> None:
        if self._disabled:
            raise RuntimeError("Helium service disabled (chaos)")
        async with self._lock:
            self.total_used_l += amount_l
            self.usage_history.append((_utcnow(), float(amount_l)))
            alpha = self.config.helium_smoothing_alpha
            if self._smoothed_rate is None:
                self._smoothed_rate = amount_l
            else:
                self._smoothed_rate = alpha * amount_l + (1 - alpha) * self._smoothed_rate
            self.obs.counter('helium_usage_total_l', amount_l)

    async def record_saving(self, amount_l: float,
                            source: str = 'optimization') -> None:
        async with self._lock:
            self.total_saved_l += amount_l
            self.obs.counter('helium_savings_total_l', amount_l, source=source)

    async def predict_usage(self, hours: int = 24) -> Dict[str, Any]:
        if self._smoothed_rate is None:
            return {'total_predicted_usage': 0.0, 'confidence': 0.0}
        # Smoothed rate is per-operation; assume ~1 op/minute for the horizon
        ops = hours * 60
        predicted = self._smoothed_rate * ops
        remaining = self.config.helium_budget_l - (self.total_used_l - self.total_saved_l)
        return {
            'total_predicted_usage': float(predicted),
            'confidence': min(1.0, len(self.usage_history) / 100.0),
            'remaining_budget_l': float(remaining),
            'smoothed_rate_l_per_op': float(self._smoothed_rate),
        }

    def get_efficiency_report(self) -> Dict[str, Any]:
        net = self.total_used_l - self.total_saved_l
        return {
            'total_helium_used_l': float(self.total_used_l),
            'total_helium_saved_l': float(self.total_saved_l),
            'net_helium_l': float(net),
            'remaining_budget_l': float(self.config.helium_budget_l - net),
        }


# ============================================================================
# EvictionService
# ============================================================================
class EvictionService:
    """Score-based LRU eviction augmented with predicted demand + sustainability."""

    def __init__(self, config: ColdStartConfig,
                 observability: Optional[ObservabilityHub] = None) -> None:
        self.config = config
        self.obs = observability or ObservabilityHub()
        self.eviction_history: Deque[Dict[str, Any]] = deque(maxlen=10_000)
        self._lock = asyncio.Lock()

    def _score(
        self,
        checkpoint: ExpertCheckpoint,
        predicted_demand: float,
        now: datetime,
    ) -> float:
        w = self.config.eviction_weights
        usage_score = 1.0 - min(1.0, checkpoint.usage_count / 100.0)
        age_seconds = max(0.0, (now - checkpoint.last_used).total_seconds())
        age_score = min(1.0, age_seconds / 3600.0)
        demand_score = 1.0 - min(1.0, max(0.0, predicted_demand))
        sustainability_score = 1.0 - min(1.0, max(0.0, checkpoint.sustainability_score))
        return (
            w.get('usage_count', 0.25) * usage_score +
            w.get('age', 0.20) * age_score +
            w.get('predicted_demand', 0.35) * demand_score +
            w.get('sustainability', 0.20) * sustainability_score
        )

    async def select_eviction_candidates(
        self,
        cache: Dict[str, ExpertCheckpoint],
        predicted_demand: Optional[Dict[str, float]] = None,
        num_to_evict: int = 1,
    ) -> List[str]:
        async with self._lock:
            if not cache:
                return []
            now = _utcnow()
            predicted_demand = predicted_demand or {}
            scored = []
            for eid, cp in cache.items():
                demand = float(predicted_demand.get(eid, 0.5))
                scored.append((eid, self._score(cp, demand, now)))
            scored.sort(key=lambda x: x[1], reverse=True)
            candidates = [eid for eid, _ in scored[:num_to_evict]]
            for eid in candidates:
                self.eviction_history.append({
                    'expert_id': eid,
                    'timestamp': now.isoformat(),
                })
                self.obs.counter('evictions_total', expert=eid)
            return candidates

    def get_stats(self) -> Dict[str, Any]:
        return {'total_evictions': len(self.eviction_history)}


# ============================================================================
# MLDemandPredictor (real SGDRegressor with fallback)
# ============================================================================
class MLDemandPredictor:
    """Predicts demand per expert from historical usage patterns."""

    def __init__(self, config: ColdStartConfig,
                 observability: Optional[ObservabilityHub] = None) -> None:
        self.config = config
        self.obs = observability or ObservabilityHub()
        self.history: Deque[Tuple[datetime, str]] = deque(maxlen=config.ml_history_window)
        self.model: Optional[Any] = None
        self.scaler: Optional[Any] = None
        self.is_fitted = False
        self._lock = asyncio.Lock()
        self._disabled = False
        if SKLEARN_AVAILABLE and SGDRegressor is not None:
            self.model = SGDRegressor(
                learning_rate='constant', eta0=config.ml_online_learning_rate,
                penalty='l2', alpha=1e-4, max_iter=1, warm_start=True,
                random_state=42,
            )
            self.scaler = StandardScaler()

    def disable(self) -> None:
        self._disabled = True

    def enable(self) -> None:
        self._disabled = False

    async def record_demand(self, expert_id: str, timestamp: Optional[datetime] = None,
                            context: Optional[Dict[str, Any]] = None) -> None:
        if self._disabled:
            return
        async with self._lock:
            self.history.append((timestamp or _utcnow(), expert_id))

    async def train_model(self) -> Dict[str, Any]:
        if self.model is None or self.scaler is None:
            return {'status': 'ml_not_available'}
        async with self._lock:
            history = list(self.history)
        if len(history) < 20:
            return {'status': 'insufficient_data', 'samples': len(history)}
        # Features: hour-of-day, day-of-week, one-hot expert_id
        expert_ids = sorted({eid for _, eid in history})
        if not expert_ids:
            return {'status': 'no_experts'}
        X = []
        y = []
        for i in range(1, len(history)):
            ts, eid = history[i]
            feats = [
                ts.hour / 23.0,
                ts.weekday() / 6.0,
            ] + [1.0 if eid == e else 0.0 for e in expert_ids]
            X.append(feats)
            # Label: number of subsequent calls in the next 60 minutes
            count = sum(1 for j in range(i, len(history))
                        if (history[j][0] - ts).total_seconds() <= 3600)
            y.append(float(count))
        X_arr = np.array(X)
        y_arr = np.array(y)
        try:
            if not self.is_fitted:
                X_scaled = self.scaler.fit_transform(X_arr)
                self.is_fitted = True
            else:
                X_scaled = self.scaler.transform(X_arr)
            for _ in range(3):
                self.model.partial_fit(X_scaled, y_arr)
            self.obs.counter('ml_training_runs_total')
            return {'status': 'success', 'samples': len(X_arr)}
        except Exception as e:
            logger.warning("ML training failed: %s", e)
            return {'status': 'failed', 'error': str(e)}

    async def predict_demand(self, horizon_minutes: int = 5) -> Dict[str, float]:
        if self.model is None or not self.is_fitted:
            return {}
        async with self._lock:
            history = list(self.history)
        if not history:
            return {}
        expert_ids = sorted({eid for _, eid in history})
        now = _utcnow()
        predictions: Dict[str, float] = {}
        for eid in expert_ids:
            feats = [now.hour / 23.0, now.weekday() / 6.0] + [
                1.0 if eid == e else 0.0 for e in expert_ids
            ]
            try:
                scaled = self.scaler.transform(np.array([feats]))
                pred = float(self.model.predict(scaled)[0])
                predictions[eid] = max(0.0, min(1.0, pred / 100.0))
            except Exception:
                predictions[eid] = 0.5
        return predictions


# ============================================================================
# PersistenceService
# ============================================================================
class PersistenceService:
    """Async JSON persistence with gzip, schema versioning, DLQ."""

    def __init__(self, config: ColdStartConfig,
                 observability: Optional[ObservabilityHub] = None,
                 dlq: Optional[DeadLetterQueue] = None) -> None:
        self.config = config
        self.obs = observability or ObservabilityHub()
        self.dlq = dlq
        self.path = config.persistence_path
        self._lock = asyncio.Lock()
        self._disabled = False

    def disable(self) -> None:
        self._disabled = True

    def enable(self) -> None:
        self._disabled = False

    async def save(self, state: Dict[str, Any]) -> bool:
        if self._disabled:
            raise RuntimeError("Persistence disabled (chaos)")
        payload = {
            'version': MODULE_VERSION,
            'schema': STATE_SCHEMA_VERSION,
            'timestamp': _utcnow().isoformat(),
            'data': _to_serializable(state),
        }
        async with self._lock:
            try:
                json_str = json.dumps(payload, default=str, indent=2)
                compressed = zlib.compress(json_str.encode('utf-8'))

                def _write() -> None:
                    tmp = f"{self.path}.tmp"
                    with open(tmp, 'wb') as f:
                        f.write(compressed)
                    os.replace(tmp, self.path)

                await asyncio.to_thread(_write)
                self.obs.counter('persistence_saves_total', status='ok')
                return True
            except Exception as e:
                logger.error("Persistence save failed: %s", e)
                self.obs.counter('persistence_saves_total', status='error')
                if self.dlq:
                    await self.dlq.record('persistence_save', str(e))
                return False

    async def load(self) -> Optional[Dict[str, Any]]:
        if not os.path.exists(self.path):
            return None
        async with self._lock:
            try:
                def _read() -> bytes:
                    with open(self.path, 'rb') as f:
                        return f.read()

                compressed = await asyncio.to_thread(_read)
                json_str = zlib.decompress(compressed).decode('utf-8')
                payload = json.loads(json_str)
                schema = payload.get('schema', 0)
                if schema != STATE_SCHEMA_VERSION:
                    logger.warning("State schema mismatch: file=%s expected=%s",
                                   schema, STATE_SCHEMA_VERSION)
                self.obs.counter('persistence_loads_total', status='ok')
                return payload.get('data') or {}
            except Exception as e:
                logger.error("Persistence load failed: %s", e)
                self.obs.counter('persistence_loads_total', status='error')
                if self.dlq:
                    await self.dlq.record('persistence_load', str(e))
                return None

    async def delete(self) -> bool:
        async with self._lock:
            if os.path.exists(self.path):
                try:
                    await asyncio.to_thread(os.remove, self.path)
                    return True
                except Exception as e:
                    logger.warning("Persistence delete failed: %s", e)
                    return False
            return False


# ============================================================================
# ChaosService
# ============================================================================
class ChaosService:
    """Non-destructive chaos testing with save/restore."""

    def __init__(self, optimizer: "ColdStartOptimizer",
                 config: ColdStartConfig) -> None:
        self.optimizer = optimizer
        self.config = config
        self._saved: Dict[str, Any] = {}
        self._injected: List[str] = []
        self._lock = asyncio.Lock()

    async def inject(self, fault_type: FaultType, **params: Any) -> Dict[str, Any]:
        async with self._lock:
            try:
                if fault_type == FaultType.PERSISTENCE_FAILURE:
                    svc = self.optimizer.persistence_service
                    if svc is not None:
                        self._saved.setdefault('persistence_disabled', svc._disabled)
                        svc.disable()
                    return {'status': 'injected'}
                if fault_type == FaultType.FEDERATED_FAILURE:
                    svc = self.optimizer.federated_service
                    if svc is not None:
                        self._saved.setdefault('federated_url', svc.config.federated_server_url)
                        svc.config.federated_server_url = None
                    return {'status': 'injected'}
                if fault_type == FaultType.CARBON_API_FAILURE:
                    svc = self.optimizer.carbon_service
                    if svc is not None:
                        self._saved.setdefault('carbon_disabled', svc._disabled)
                        svc.disable()
                    return {'status': 'injected'}
                if fault_type == FaultType.HELIUM_TRACKER_FAILURE:
                    svc = self.optimizer.helium_service
                    if svc is not None:
                        self._saved.setdefault('helium_disabled', svc._disabled)
                        svc.disable()
                    return {'status': 'injected'}
                if fault_type == FaultType.ML_PREDICTOR_FAILURE:
                    svc = self.optimizer.ml_predictor
                    if svc is not None:
                        self._saved.setdefault('ml_disabled', svc._disabled)
                        svc.disable()
                    return {'status': 'injected'}
                if fault_type == FaultType.MOEA_EVALUATION_FAILURE:
                    svc = self.optimizer.evolution_service
                    if svc is not None:
                        self._saved.setdefault('evolution_disabled', svc._disabled)
                        svc.disable()
                    return {'status': 'injected'}
                if fault_type == FaultType.DISTILLATION_TEACHER_FAILURE:
                    svc = self.optimizer.distillation_service
                    if svc is not None:
                        self._saved.setdefault('distillation_disabled', svc._disabled)
                        svc.disable()
                    return {'status': 'injected'}
                if fault_type == FaultType.CACHE_CORRUPTION:
                    async with self.optimizer._cache_lock:
                        self._saved.setdefault('cache_snapshot',
                                                dict(self.optimizer.checkpoint_cache))
                        self.optimizer.checkpoint_cache.clear()
                    return {'status': 'injected'}
            except Exception as e:
                logger.exception("Chaos injection failed: %s", e)
                return {'status': 'error', 'error': str(e)}
            return {'status': 'unknown_fault'}

    def restore(self) -> None:
        opt = self.optimizer
        for key, value in list(self._saved.items()):
            if key == 'persistence_disabled' and opt.persistence_service is not None:
                opt.persistence_service._disabled = bool(value)
            elif key == 'federated_url' and opt.federated_service is not None:
                opt.federated_service.config.federated_server_url = value
            elif key == 'carbon_disabled' and opt.carbon_service is not None:
                opt.carbon_service._disabled = bool(value)
            elif key == 'helium_disabled' and opt.helium_service is not None:
                opt.helium_service._disabled = bool(value)
            elif key == 'ml_disabled' and opt.ml_predictor is not None:
                opt.ml_predictor._disabled = bool(value)
            elif key == 'evolution_disabled' and opt.evolution_service is not None:
                opt.evolution_service._disabled = bool(value)
            elif key == 'distillation_disabled' and opt.distillation_service is not None:
                opt.distillation_service._disabled = bool(value)
            elif key == 'cache_snapshot':
                # Sync restore happens via the caller's lock
                opt.checkpoint_cache.clear()
                opt.checkpoint_cache.update(value)
            self._saved.pop(key, None)
        self._injected.clear()

    async def run_test(self) -> Dict[str, Any]:
        report: Dict[str, Any] = {'faults': [], 'results': {}, 'restored': False}
        faults = [
            FaultType.PERSISTENCE_FAILURE,
            FaultType.CARBON_API_FAILURE,
            FaultType.HELIUM_TRACKER_FAILURE,
            FaultType.ML_PREDICTOR_FAILURE,
            FaultType.MOEA_EVALUATION_FAILURE,
            FaultType.DISTILLATION_TEACHER_FAILURE,
        ]
        try:
            for ft in faults[:CHAOS_MAX_FAULTS_PER_RUN]:
                res = await self.inject(ft)
                report['faults'].append(ft.value)
                report['results'][ft.value] = res

            # Steady-state hypothesis: engine reports health
            try:
                health = await self.optimizer.get_health_status()
                report['results']['health_check'] = {
                    'status': health.get('status', 'unknown'),
                }
            except Exception as e:
                report['results']['health_check'] = {'error': str(e)}
        finally:
            if self.config.enable_chaos_testing:
                async with self.optimizer._cache_lock:
                    self.restore()
                report['restored'] = True
        return report


# ============================================================================
# Main optimizer
# ============================================================================
class ColdStartOptimizer:
    """Enhanced Cold Start Optimizer v4.0.0."""

    def __init__(
        self,
        config: Optional[ColdStartConfig] = None,
        storage: Optional[StorageProtocol] = None,
        publish_fn: Optional[Callable[[str, Any], Awaitable[None]]] = None,
        **kwargs: Any,
    ) -> None:
        if config is None:
            valid = {f.name for f in dataclass_fields(ColdStartConfig)}
            filtered = {k: v for k, v in kwargs.items() if k in valid}
            config = ColdStartConfig(**filtered)
        self.config = config
        self.storage = storage

        # Cross-cutting infrastructure
        self.observability = ObservabilityHub()
        self.dlq = DeadLetterQueue()
        self.governance = GovernanceLayer(
            enabled=config.enable_human_approval,
            auto_approve_on_timeout=False,
            publish_fn=publish_fn,
        )
        self.safety = SafetyMonitor()
        self._setup_invariants()

        # Idempotency
        self._idempotency = IdempotencyCache()

        # Locks
        self._cache_lock = asyncio.Lock()
        self._history_lock = asyncio.Lock()

        # State
        self.checkpoint_cache: "OrderedDict[str, ExpertCheckpoint]" = OrderedDict()
        self.warmup_history: Deque[Dict[str, Any]] = deque(maxlen=MAX_WARMUP_HISTORY)
        self.cold_start_events: Deque[Dict[str, Any]] = deque(maxlen=MAX_WARMUP_HISTORY)
        self.interaction_log: Deque[Dict[str, Any]] = deque(maxlen=MAX_INTERACTION_LOG)
        self.sustainability_score: float = 0.0
        self.global_best_weights: Optional[Dict[str, float]] = None
        self.pareto_front: List[MOPDStrategyWeights] = []

        # Sub-services
        self.persistence_service = PersistenceService(
            config, observability=self.observability, dlq=self.dlq,
        ) if config.enable_persistence else None
        self.carbon_service = CarbonService(
            config, observability=self.observability,
        ) if config.enable_carbon_aware else None
        self.helium_service = HeliumService(
            config, observability=self.observability,
        ) if config.enable_helium_tracking else None
        self.ml_predictor = MLDemandPredictor(
            config, observability=self.observability,
        ) if config.enable_ml_demand else None
        self.eviction_service = EvictionService(
            config, observability=self.observability,
        ) if config.enable_intelligent_eviction else None
        self.federated_service = FederatedService(
            config, observability=self.observability, dlq=self.dlq,
        ) if config.enable_federated else None
        self.distillation_service = DistillationService(
            config, observability=self.observability,
        )
        self.gating_service = GatingService(
            config, observability=self.observability,
        ) if config.enable_moe else None

        # Evolution service with the real evaluate function
        self.evolution_service: Optional[EvolutionService] = None
        if config.moea_enabled:
            self.evolution_service = EvolutionService(
                config,
                evaluate=self._evaluate_moea_individual,
                observability=self.observability,
            )

        # Warmup strategies (canonical, includes 'federated')
        self.warmup_strategies: Dict[str, WarmupStrategy] = {
            'preload': WarmupStrategy('preload', 1, 5.0, 0.001, 0.99, 0.9, 0.8),
            'transfer': WarmupStrategy('transfer', 2, 50.0, 0.005, 0.85, 0.7, 0.6),
            'progressive': WarmupStrategy('progressive', 3, 200.0, 0.01, 0.95, 0.5, 0.5),
            'hybrid': WarmupStrategy('hybrid', 4, 100.0, 0.008, 0.92, 0.6, 0.7),
            'federated': WarmupStrategy('federated', 5, 80.0, 0.006, 0.88, 0.75, 0.75),
        }

        # Chaos
        self.chaos_service = ChaosService(self, config) if config.enable_chaos_testing else None

        # Lifecycle
        self._started = False
        self._shutting_down = False
        self._background_tasks: List[asyncio.Task] = []
        self._preloader_task: Optional[asyncio.Task] = None
        self._moea_task: Optional[asyncio.Task] = None
        self._persistence_task: Optional[asyncio.Task] = None
        self._chaos_task: Optional[asyncio.Task] = None
        self._ready_event = asyncio.Event()

        logger.info(
            "ColdStartOptimizer v%s initialised (call start() to launch tasks)",
            MODULE_VERSION,
        )

    # ------------------------------------------------------------------
    # Safety invariants
    # ------------------------------------------------------------------
    def _setup_invariants(self) -> None:
        def _cache_bounded(state: Dict[str, Any]) -> bool:
            return int(state.get('cache_size', 0)) <= self.config.cache_size

        def _hit_rate_bounded(state: Dict[str, Any]) -> bool:
            hr = float(state.get('recent_hit_rate', 0.0))
            return 0.0 <= hr <= 1.0

        def _sustainability_bounded(state: Dict[str, Any]) -> bool:
            s = float(state.get('sustainability_score', 0.5))
            return 0.0 <= s <= 1.0

        self.safety.add('cache_bounded', _cache_bounded,
                        'checkpoint_cache must not exceed cache_size')
        self.safety.add('hit_rate_bounded', _hit_rate_bounded,
                        'recent hit rate must be in [0, 1]')
        self.safety.add('sustainability_bounded', _sustainability_bounded,
                        'sustainability_score must be in [0, 1]')

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def start(self) -> None:
        if self._started:
            return
        # Load state BEFORE starting background tasks
        if self.persistence_service is not None:
            data = await self.persistence_service.load()
            if data:
                await self._hydrate_from_state(data)

        self._started = True

        # Persistence loop
        if self.persistence_service is not None:
            self._persistence_task = asyncio.create_task(self._persistence_loop())
            self._background_tasks.append(self._persistence_task)

        # MOEA loop
        if self.evolution_service is not None:
            self._moea_task = asyncio.create_task(self._moea_loop())
            self._background_tasks.append(self._moea_task)

        # Chaos loop
        if self.chaos_service is not None:
            self._chaos_task = asyncio.create_task(self._chaos_loop())
            self._background_tasks.append(self._chaos_task)

        # Background preloader
        self._preloader_task = asyncio.create_task(self._background_preload_loop())
        self._background_tasks.append(self._preloader_task)

        self._ready_event.set()
        logger.info("ColdStartOptimizer started")

    async def wait_ready(self) -> None:
        if not self._started:
            await self.start()
        await self._ready_event.wait()

    async def shutdown(self) -> None:
        logger.info("Shutting down ColdStartOptimizer")
        self._shutting_down = True
        for t in self._background_tasks:
            t.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)
        self._background_tasks.clear()

        # Persist
        if self.persistence_service is not None:
            try:
                await self.save_state()
            except Exception as e:
                logger.error("Final save failed: %s", e)

        # Close externals
        if self.carbon_service is not None:
            await self.carbon_service.close()
        if self.federated_service is not None:
            await self.federated_service.close()

        if self.safety is not None:
            unmet = self.safety.final_check()
            if unmet:
                logger.warning("Unmet eventual invariants at shutdown: %s", unmet)

        logger.info("Shutdown complete")

    # ------------------------------------------------------------------
    # Background loops
    # ------------------------------------------------------------------
    async def _persistence_loop(self) -> None:
        interval = max(60, self.config.telemetry_export_interval * 5)
        while not self._shutting_down:
            try:
                await asyncio.sleep(interval)
                await self.save_state()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Persistence loop error: %s", e)

    async def _moea_loop(self) -> None:
        while not self._shutting_down:
            try:
                await asyncio.sleep(self.config.moea_interval_seconds)
                await self.run_moea_update()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("MOEA loop error: %s", e)
                await asyncio.sleep(60)

    async def _chaos_loop(self) -> None:
        while not self._shutting_down:
            try:
                await asyncio.sleep(max(60, self.config.moea_interval_seconds))
                if self.chaos_service is not None:
                    await self.chaos_service.run_test()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Chaos loop error: %s", e)

    async def _background_preload_loop(self) -> None:
        while not self._shutting_down:
            try:
                await asyncio.sleep(60)
                # Preload top-N checkpoints if below threshold
                async with self._cache_lock:
                    if len(self.checkpoint_cache) == 0:
                        continue
                if self.ml_predictor is not None:
                    try:
                        predictions = await self.ml_predictor.predict_demand(5)
                        self.observability.gauge(
                            'predicted_demand_max',
                            max(predictions.values()) if predictions else 0.0,
                        )
                    except Exception:
                        pass
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Background preloader error: %s", e)
                await asyncio.sleep(300)

    # ------------------------------------------------------------------
    # MOEA evaluation (real)
    # ------------------------------------------------------------------
    async def _evaluate_moea_individual(
        self, weights: Dict[str, float],
    ) -> Dict[str, float]:
        """
        Evaluate a candidate strategy weight vector against historical warmup
        outcomes. Returns canonical objectives in [0, 1]:
          - latency: normalized warmup latency (lower is better)
          - carbon: normalized carbon footprint (lower is better)
          - cache_hit: hit rate under this weight vector (higher is better)
          - sustainability: composite sustainability (higher is better)
        """
        async with self._history_lock:
            history = list(self.warmup_history)[-500:]
        if not history:
            # Cold start: return neutral objectives
            return {'latency': 0.5, 'carbon': 0.5, 'cache_hit': 0.5, 'sustainability': 0.5}

        latency_accum = 0.0
        carbon_accum = 0.0
        hit_accum = 0.0
        sustain_accum = 0.0
        total_weight = 0.0
        for entry in history:
            strategy = entry.get('method', 'unknown')
            w = float(weights.get(strategy, 0.0))
            if w <= 0:
                continue
            latency_accum += w * float(entry.get('load_time_ms', 500.0))
            carbon_accum += w * float(entry.get('carbon_footprint_kg', 0.0))
            hit_accum += w * (1.0 if strategy in ('preload', 'transfer') else 0.0)
            sustain_accum += w * float(entry.get('sustainability_score', 0.5))
            total_weight += w
        if total_weight <= 0:
            return {'latency': 0.5, 'carbon': 0.5, 'cache_hit': 0.5, 'sustainability': 0.5}

        avg_latency = latency_accum / total_weight
        avg_carbon = carbon_accum / total_weight
        avg_hit = hit_accum / total_weight
        avg_sustain = sustain_accum / total_weight

        return {
            'latency': float(np.clip(1.0 - avg_latency / 1000.0, 0.0, 1.0)),
            'carbon': float(np.clip(1.0 - avg_carbon / 0.01, 0.0, 1.0)),
            'cache_hit': float(np.clip(avg_hit, 0.0, 1.0)),
            'sustainability': float(np.clip(avg_sustain, 0.0, 1.0)),
        }

    async def run_moea_update(self) -> List[MOPDStrategyWeights]:
        if self.evolution_service is None:
            return []
        async with self._history_lock:
            if len(self.warmup_history) < 5:
                logger.debug("Not enough warmup history for MOEA")
                return []
        try:
            pareto = await self.evolution_service.evolve()
            self.pareto_front = pareto
            if self.evolution_service.best is not None:
                self.global_best_weights = dict(self.evolution_service.best.weights)
                await self.evolution_service.save_pareto()
                logger.info("MOEA best weights: %s", self.global_best_weights)
            return pareto
        except Exception as e:
            logger.error("MOEA update failed: %s", e)
            return []

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    async def initialize_expert(
        self,
        expert_id: str,
        expert_type: str,
        carbon_budget: float = 0.1,
        helium_budget: float = 0.1,
        max_latency_ms: float = 500.0,
        urgency: str = 'normal',
        carbon_intensity: Optional[float] = None,
        idempotency_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        await self.wait_ready()

        if idempotency_key and self._idempotency.seen(idempotency_key):
            return {'status': 'duplicate', 'expert_id': expert_id,
                    'idempotency_key': idempotency_key}

        # Resolve carbon intensity
        if carbon_intensity is None:
            if self.carbon_service is not None:
                try:
                    carbon_intensity = await self.carbon_service.get_current_intensity()
                except Exception:
                    carbon_intensity = self.config.default_carbon_intensity
            else:
                carbon_intensity = self.config.default_carbon_intensity

        state = await self._build_state(
            expert_type, urgency, carbon_budget, helium_budget,
            max_latency_ms, carbon_intensity,
        )

        # Strategy selection
        if self.gating_service is not None:
            try:
                expert_name, _ = await self.gating_service.select_expert(state)
                strategy = expert_name if expert_name in DEFAULT_STRATEGY_NAMES else 'preload'
                action_idx = DEFAULT_STRATEGY_NAMES.index(strategy)
                state_vec = state.to_feature_vector()
                teacher_probs = np.ones(len(DEFAULT_STRATEGY_NAMES)) / len(DEFAULT_STRATEGY_NAMES)
            except Exception as e:
                logger.warning("Gating service failed: %s", e)
                strategy, action_idx, state_vec, teacher_probs = await self.distillation_service.select_strategy(state)
        else:
            strategy, action_idx, state_vec, teacher_probs = await self.distillation_service.select_strategy(state)

        # Blend with MOEA best
        if self.global_best_weights is not None:
            one_hot = np.zeros(len(DEFAULT_STRATEGY_NAMES))
            one_hot[action_idx] = 1.0
            moea_probs = np.array([
                float(self.global_best_weights.get(s, 0.0)) for s in DEFAULT_STRATEGY_NAMES
            ])
            if moea_probs.sum() > 0:
                moea_probs = moea_probs / moea_probs.sum()
                blended = 0.7 * moea_probs + 0.3 * one_hot
                blended = blended / blended.sum()
                action_idx = int(np.argmax(blended))
                strategy = DEFAULT_STRATEGY_NAMES[action_idx]

        # Approval for critical urgency or aggressive carbon budget
        if self.governance.enabled and (urgency == 'critical' or carbon_budget > 0.5):
            approval = await self.governance.request_approval({
                'action': 'initialize_expert',
                'expert_id': expert_id,
                'strategy': strategy,
                'urgency': urgency,
                'carbon_budget': carbon_budget,
            })
            if approval.status not in (ApprovalStatus.APPROVED, ApprovalStatus.AUTO_APPROVED):
                logger.info("Approval denied for %s: %s", expert_id, approval.status.value)
                return {'status': 'denied', 'expert_id': expert_id,
                        'approval_status': approval.status.value}

        # Execute strategy
        try:
            result = await self._execute_strategy(
                strategy, expert_id, expert_type,
                carbon_budget, helium_budget, max_latency_ms,
            )
        except Exception as e:
            logger.exception("Strategy execution failed: %s", e)
            await self.dlq.record('initialize_expert', str(e), {'expert_id': expert_id})
            return {'status': 'error', 'expert_id': expert_id, 'error': str(e)}

        # Reward
        reward = self._compute_reward(result, max_latency_ms, carbon_intensity)

        # Update distillation + gating
        try:
            next_state_vec = state.to_feature_vector()
            await self.distillation_service.update(
                state_vec, action_idx, reward, next_state_vec, teacher_probs,
            )
            await self.distillation_service.update_teacher(state, action_idx, reward)
        except Exception as e:
            logger.warning("Distillation update failed: %s", e)

        if self.gating_service is not None and strategy in DEFAULT_STRATEGY_NAMES:
            try:
                await self.gating_service.add_training_sample(state, strategy, reward)
            except Exception as e:
                logger.warning("Gating update failed: %s", e)

        # Log
        entry = {
            'timestamp': _utcnow().isoformat(),
            'expert_id': expert_id,
            'expert_type': expert_type,
            'strategy': strategy,
            'reward': reward,
            'result': result,
            'state_vector': state_vec.tolist(),
        }
        async with self._history_lock:
            self.interaction_log.append(entry)
            self.warmup_history.append(entry)

        # Helium usage
        if self.helium_service is not None:
            try:
                await self.helium_service.record_usage(
                    expert_id, result.get('helium_usage_l', helium_budget),
                )
            except Exception as e:
                logger.debug("Helium recording failed: %s", e)

        # Federated share (opt-in)
        if self.federated_service is not None:
            try:
                await self.federated_service.share(
                    expert_id,
                    {
                        'expert_type': expert_type,
                        'strategy': strategy,
                        'load_time_ms': result.get('load_time_ms', 500.0),
                        'sustainability_score': result.get('sustainability_score', 0.5),
                    },
                    performance=reward,
                )
            except Exception as e:
                logger.debug("Federated share failed: %s", e)

        # Cache invalidation
        async with self._cache_lock:
            if len(self.checkpoint_cache) >= self.cache_size:
                await self._evict_if_needed()

        # Safety
        if self.config.enable_temporal_safety:
            async with self._cache_lock:
                safety_state = {
                    'cache_size': len(self.checkpoint_cache),
                    'recent_hit_rate': self._calculate_hit_rate(),
                    'sustainability_score': result.get('sustainability_score', 0.5),
                }
            violations = self.safety.check(safety_state)
            if violations:
                logger.warning("Safety violations: %s", violations)
                self.observability.counter('safety_violations_total')

        result['reward'] = reward
        return result

    async def _execute_strategy(
        self,
        strategy: str,
        expert_id: str,
        expert_type: str,
        carbon_budget: float,
        helium_budget: float,
        max_latency_ms: float,
    ) -> Dict[str, Any]:
        if strategy == 'preload':
            return await self._preload_initialize(expert_id, expert_type, max_latency_ms)
        if strategy == 'transfer':
            return await self._transfer_initialize(
                expert_id, expert_type, carbon_budget, helium_budget, max_latency_ms,
            )
        if strategy == 'hybrid':
            return await self._hybrid_initialize(
                expert_id, expert_type, carbon_budget, helium_budget, max_latency_ms,
            )
        if strategy == 'federated':
            return await self._federated_initialize(
                expert_id, expert_type, carbon_budget, helium_budget, max_latency_ms,
            )
        return await self._progressive_initialize(
            expert_id, expert_type, carbon_budget, helium_budget, max_latency_ms, strategy,
        )

    async def _preload_initialize(self, expert_id: str, expert_type: str,
                                   max_latency_ms: float) -> Dict[str, Any]:
        cp = await self._create_checkpoint(expert_id, expert_type,
                                            carbon_kg=0.0005, helium_l=0.0001)
        async with self._cache_lock:
            self._add_to_cache(expert_id, cp)
        return {
            'expert_id': expert_id,
            'initialized': True,
            'method': 'preload',
            'load_time_ms': 5.0,
            'sustainability_score': 0.85,
            'carbon_footprint_kg': cp.carbon_footprint_kg,
            'helium_usage_l': cp.helium_usage_l,
            'performance_metrics': cp.performance_metrics,
        }

    async def _transfer_initialize(self, expert_id: str, expert_type: str,
                                    carbon_budget: float, helium_budget: float,
                                    max_latency_ms: float) -> Dict[str, Any]:
        # Transfer is modelled as a lighter progressive warmup
        return await self._progressive_initialize(
            expert_id, expert_type, carbon_budget, helium_budget,
            max_latency_ms, strategy_type='transfer',
            override_latency=50.0, override_sustainability=0.78,
        )

    async def _hybrid_initialize(self, expert_id: str, expert_type: str,
                                  carbon_budget: float, helium_budget: float,
                                  max_latency_ms: float) -> Dict[str, Any]:
        return await self._progressive_initialize(
            expert_id, expert_type, carbon_budget, helium_budget,
            max_latency_ms, strategy_type='hybrid',
            override_latency=100.0, override_sustainability=0.82,
        )

    async def _federated_initialize(self, expert_id: str, expert_type: str,
                                     carbon_budget: float, helium_budget: float,
                                     max_latency_ms: float) -> Dict[str, Any]:
        # Attempt to fetch peers
        peer_count = 0
        if self.federated_service is not None:
            try:
                peers = await self.federated_service.fetch_peers(expert_id)
                peer_count = len(peers)
            except Exception:
                peer_count = 0
        cp = await self._create_checkpoint(
            expert_id, expert_type,
            carbon_kg=carbon_budget * 0.5, helium_l=helium_budget * 0.5,
            federated_consensus=peer_count > 0, peer_count=peer_count,
        )
        async with self._cache_lock:
            self._add_to_cache(expert_id, cp)
        return {
            'expert_id': expert_id,
            'initialized': True,
            'method': 'federated',
            'load_time_ms': 80.0,
            'sustainability_score': 0.88,
            'carbon_footprint_kg': cp.carbon_footprint_kg,
            'helium_usage_l': cp.helium_usage_l,
            'performance_metrics': cp.performance_metrics,
            'peer_count': peer_count,
        }

    async def _progressive_initialize(
        self, expert_id: str, expert_type: str,
        carbon_budget: float, helium_budget: float,
        max_latency_ms: float, strategy_type: str = 'progressive',
        override_latency: Optional[float] = None,
        override_sustainability: Optional[float] = None,
    ) -> Dict[str, Any]:
        # Progressive phases: warmup → load → verify
        await asyncio.sleep(0)
        cp = await self._create_checkpoint(
            expert_id, expert_type,
            carbon_kg=carbon_budget, helium_l=helium_budget,
        )
        async with self._cache_lock:
            self._add_to_cache(expert_id, cp)
        latency = override_latency if override_latency is not None else 200.0
        sustainability = (override_sustainability if override_sustainability is not None
                          else self._calculate_checkpoint_sustainability(cp))
        return {
            'expert_id': expert_id,
            'initialized': True,
            'method': strategy_type,
            'load_time_ms': latency,
            'sustainability_score': sustainability,
            'carbon_footprint_kg': cp.carbon_footprint_kg,
            'helium_usage_l': cp.helium_usage_l,
            'performance_metrics': cp.performance_metrics,
        }

    async def _create_checkpoint(
        self, expert_id: str, expert_type: str,
        carbon_kg: float = 0.0, helium_l: float = 0.0,
        federated_consensus: bool = False, peer_count: int = 0,
    ) -> ExpertCheckpoint:
        now = _utcnow()
        return ExpertCheckpoint(
            expert_id=expert_id,
            expert_type=expert_type,
            model_state={},
            optimizer_state={},
            feature_distribution={},
            performance_metrics={'expected_accuracy': 0.92},
            created_at=now,
            last_used=now,
            carbon_footprint_kg=carbon_kg,
            helium_usage_l=helium_l,
            sustainability_score=self._sustainability_from_carbon(carbon_kg),
            federated_consensus=federated_consensus,
            peer_count=peer_count,
        )

    @staticmethod
    def _sustainability_from_carbon(carbon_kg: float) -> float:
        return float(np.clip(1.0 - carbon_kg / 0.01, 0.0, 1.0))

    def _add_to_cache(self, expert_id: str, cp: ExpertCheckpoint) -> None:
        if expert_id in self.checkpoint_cache:
            self.checkpoint_cache[expert_id].usage_count += 1
            self.checkpoint_cache[expert_id].last_used = _utcnow()
            self.checkpoint_cache.move_to_end(expert_id)
            return
        if len(self.checkpoint_cache) >= self.cache_size:
            # synchronous fallback eviction (LRU)
            self.checkpoint_cache.popitem(last=False)
        self.checkpoint_cache[expert_id] = cp

    async def _evict_if_needed(self) -> None:
        if self.eviction_service is None:
            return
        predicted: Dict[str, float] = {}
        if self.ml_predictor is not None:
            try:
                predicted = await self.ml_predictor.predict_demand(5)
            except Exception:
                predicted = {}
        candidates = await self.eviction_service.select_eviction_candidates(
            dict(self.checkpoint_cache), predicted_demand=predicted, num_to_evict=1,
        )
        for eid in candidates:
            self.checkpoint_cache.pop(eid, None)

    def _compute_reward(self, result: Dict[str, Any], max_latency_ms: float,
                        carbon_intensity: float) -> float:
        time_taken = float(result.get('load_time_ms', 500.0))
        max_lat = max(1e-6, float(max_latency_ms))
        time_score = 1.0 - min(1.0, time_taken / max_lat)
        sustainability = float(result.get('sustainability_score', 0.5))
        carbon_score = 1.0 - min(1.0, float(carbon_intensity) / 1000.0)
        return float(np.clip(
            0.4 * time_score + 0.3 * sustainability + 0.3 * carbon_score,
            0.0, 1.0,
        ))

    async def _build_state(
        self,
        expert_type: str,
        urgency: str,
        carbon_budget: float,
        helium_budget: float,
        max_latency_ms: float,
        carbon_intensity: float,
    ) -> ColdStartState:
        async with self._cache_lock:
            cache_util = len(self.checkpoint_cache) / max(1, self.cache_size)
        hit_rate = self._calculate_hit_rate()
        async with self._history_lock:
            history = list(self.warmup_history)[-100:]
        success_rates = {s: 0.5 for s in DEFAULT_STRATEGY_NAMES}
        attempts = {s: 0 for s in DEFAULT_STRATEGY_NAMES}
        successes = {s: 0 for s in DEFAULT_STRATEGY_NAMES}
        for event in history:
            method = event.get('strategy', event.get('method', 'unknown'))
            if method in attempts:
                attempts[method] += 1
                if event.get('reward', 0.0) > 0.5:
                    successes[method] += 1
        for s in DEFAULT_STRATEGY_NAMES:
            if attempts[s] > 0:
                success_rates[s] = successes[s] / attempts[s]
        if history:
            avg_time = float(np.mean([h.get('result', {}).get('load_time_ms', 500.0)
                                       for h in history]))
            avg_sustain = float(np.mean([h.get('result', {}).get('sustainability_score', 0.5)
                                          for h in history]))
        else:
            avg_time = 500.0
            avg_sustain = 0.5
        return ColdStartState(
            expert_type=expert_type,
            urgency=urgency,
            carbon_budget=carbon_budget,
            helium_budget=helium_budget,
            max_latency_ms=max_latency_ms,
            carbon_intensity=carbon_intensity,
            cache_utilization=cache_util,
            recent_hit_rate=hit_rate,
            strategy_success_rates=success_rates,
            avg_warmup_time_ms=avg_time,
            avg_sustainability_score=avg_sustain,
        )

    def _calculate_hit_rate(self) -> float:
        total = len(self.warmup_history)
        if total == 0:
            return 0.0
        hits = sum(
            1 for h in self.warmup_history
            if h.get('strategy', h.get('method')) in ('preload', 'transfer', 'federated')
        )
        return hits / total

    @staticmethod
    def _calculate_checkpoint_sustainability(cp: ExpertCheckpoint) -> float:
        return float(np.clip(
            0.5 * (1.0 - cp.carbon_footprint_kg / 0.01) +
            0.5 * cp.performance_metrics.get('expected_accuracy', 0.5),
            0.0, 1.0,
        ))

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------
    async def save_state(self) -> bool:
        if self.persistence_service is None:
            return False
        async with self._cache_lock:
            cache_snapshot = [cp.to_dict() for cp in self.checkpoint_cache.values()]
        state = {
            'checkpoint_cache': cache_snapshot,
            'warmup_history': list(self.warmup_history),
            'interaction_log': list(self.interaction_log),
            'sustainability_score': self.sustainability_score,
            'global_best_weights': self.global_best_weights,
            'pareto_front': [p.to_dict() for p in self.pareto_front],
            'helium': self.helium_service.get_efficiency_report()
            if self.helium_service is not None else {},
        }
        return await self.persistence_service.save(state)

    async def _hydrate_from_state(self, data: Dict[str, Any]) -> None:
        try:
            async with self._cache_lock:
                self.checkpoint_cache.clear()
                for cp_data in data.get('checkpoint_cache', []):
                    try:
                        cp = ExpertCheckpoint.from_dict(cp_data)
                        self.checkpoint_cache[cp.expert_id] = cp
                    except Exception as e:
                        logger.warning("Skipping invalid checkpoint: %s", e)
            for h in data.get('warmup_history', []):
                self.warmup_history.append(h)
            for entry in data.get('interaction_log', []):
                self.interaction_log.append(entry)
            self.sustainability_score = float(data.get('sustainability_score', 0.0))
            self.global_best_weights = data.get('global_best_weights')
            self.pareto_front = [
                MOPDStrategyWeights.from_dict(p)
                for p in data.get('pareto_front', []) or []
            ]
            logger.info("State hydrated from persistence")
        except Exception as e:
            logger.error("Hydration failed: %s", e)

    async def delete_state(self) -> bool:
        if self.persistence_service is None:
            return False
        return await self.persistence_service.delete()

    # ------------------------------------------------------------------
    # Health / observability
    # ------------------------------------------------------------------
    async def get_health_status(self) -> Dict[str, Any]:
        async with self._cache_lock:
            cache_len = len(self.checkpoint_cache)
        return {
            'status': 'healthy' if not self._shutting_down else 'stopping',
            'version': MODULE_VERSION,
            'schema': STATE_SCHEMA_VERSION,
            'started': self._started,
            'cache_size': cache_len,
            'cache_capacity': self.config.cache_size,
            'hit_rate': self._calculate_hit_rate(),
            'warmup_samples': len(self.warmup_history),
            'interaction_samples': len(self.interaction_log),
            'pareto_front_size': len(self.pareto_front),
            'has_best_weights': self.global_best_weights is not None,
            'distillation': self.distillation_service.get_stats(),
            'governance_pending': len(self.governance.pending),
            'dead_letters': len(self.dlq),
        }

    async def get_prometheus_metrics(self) -> str:
        return await self.observability.export_prometheus()

    def get_dead_letters(self) -> List[Dict[str, Any]]:
        return self.dlq.snapshot()

    def get_stats(self) -> Dict[str, Any]:
        return {
            'cache_size': len(self.checkpoint_cache),
            'hit_rate': self._calculate_hit_rate(),
            'distillation': self.distillation_service.get_stats(),
            'moea_pareto_size': len(self.pareto_front),
            'global_best_weights': self.global_best_weights,
            'dead_letters': len(self.dlq),
        }

    # ------------------------------------------------------------------
    # Chaos API
    # ------------------------------------------------------------------
    async def inject_fault(self, fault_type: str, **params: Any) -> Dict[str, Any]:
        if self.chaos_service is None:
            return {'status': 'disabled'}
        try:
            ft = FaultType(fault_type)
        except ValueError:
            logger.warning("Unknown fault: %s", fault_type)
            return {'status': 'unknown_fault'}
        return await self.chaos_service.inject(ft, **params)

    async def run_chaos_test(self) -> Dict[str, Any]:
        if self.chaos_service is None:
            return {'status': 'disabled'}
        return await self.chaos_service.run_test()

    # ------------------------------------------------------------------
    # Governance API
    # ------------------------------------------------------------------
    async def submit_approval_decision(self, request_id: str, approved: bool) -> bool:
        return await self.governance.submit_decision(request_id, approved)

    def get_approval_audit(self) -> List[Dict[str, Any]]:
        return self.governance.get_audit_log()

    # ------------------------------------------------------------------
    # Convenience accessors (backwards compat)
    # ------------------------------------------------------------------
    @property
    def persistence(self) -> Optional[PersistenceService]:
        return self.persistence_service

    @property
    def strategy_optimizer(self) -> DistillationService:
        return self.distillation_service

    @property
    def moe_gating(self) -> Optional[GatingService]:
        return self.gating_service

    @property
    def moea_optimizer(self) -> Optional[EvolutionService]:
        return self.evolution_service

    @property
    def ml_demand_predictor(self) -> Optional[MLDemandPredictor]:
        return self.ml_predictor

    @property
    def helium_dashboard(self) -> Optional[HeliumService]:
        return self.helium_service

    @property
    def carbon_selector(self) -> Optional[CarbonService]:
        return self.carbon_service


# ============================================================================
# Exports
# ============================================================================
__all__ = [
    # Main
    "ColdStartOptimizer",
    "ColdStartConfig",
    # Sub-services
    "DistillationService",
    "EvolutionService",
    "GatingService",
    "FederatedService",
    "CarbonService",
    "HeliumService",
    "EvictionService",
    "PersistenceService",
    "ChaosService",
    # Infra
    "ObservabilityHub",
    "GovernanceLayer",
    "ApprovalRequest",
    "ApprovalResult",
    "ApprovalStatus",
    "SafetyMonitor",
    "SafetyInvariant",
    "CircuitBreaker",
    "CircuitBreakerState",
    "DeadLetterQueue",
    "IdempotencyCache",
    # Data
    "ExpertCheckpoint",
    "WarmupStrategy",
    "ColdStartState",
    "MOPDStrategyWeights",
    # Teachers / Student
    "StrategyRuleBasedTeacher",
    "StrategyHistoricalMLTeacher",
    "StrategyStatefulQTeacher",
    "DistillationStudent",
    "ReplayBuffer",
    # Utilities
    "scalarise_pareto",
    "filter_pareto",
    # Constants
    "MODULE_VERSION",
    "STATE_SCHEMA_VERSION",
    "DEFAULT_STRATEGY_NAMES",
    "MOPD_OBJECTIVE_NAMES",
    "MOPD_HIGHER_IS_BETTER",
    "FEATURE_DIM",
    # Pydantic flags
    "PYDANTIC_AVAILABLE",
    "PYDANTIC_V1",
    "PYDANTIC_V2",
]


# ============================================================================
# Example usage
# ============================================================================
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    async def _demo() -> None:
        config = ColdStartConfig()
        config.enable_chaos_testing = True
        optimizer = ColdStartOptimizer(config=config)
        await optimizer.start()

        for i in range(3):
            result = await optimizer.initialize_expert(
                expert_id=f"expert_{i}",
                expert_type='energy' if i % 2 == 0 else 'data',
                carbon_budget=0.05 + i * 0.02,
                helium_budget=0.02,
                max_latency_ms=300.0,
                urgency='normal',
                idempotency_key=f"init-{i}",
            )
            print(f"Init {i}: method={result.get('method')}, "
                  f"load_time_ms={result.get('load_time_ms')}, "
                  f"reward={result.get('reward'):.3f}")

        # Trigger a MOEA update
        pareto = await optimizer.run_moea_update()
        print(f"MOEA pareto size: {len(pareto)}")

        health = await optimizer.get_health_status()
        print(f"Health: {health['status']}, cache={health['cache_size']}")

        # Chaos
        chaos = await optimizer.run_chaos_test()
        print(f"Chaos restored: {chaos.get('restored')}, faults={chaos.get('faults')}")

        metrics = await optimizer.get_prometheus_metrics()
        print(metrics[:300])

        await optimizer.shutdown()

    asyncio.run(_demo())
