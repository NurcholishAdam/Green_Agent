#!/usr/bin/env python3
# predictive_maintenance.py
"""
Enhanced Predictive Maintenance Engine v4.0.0
==============================================

Production-grade hardware predictive maintenance driven by sustainability
metrics. Tracks per-node efficiency (FLOPs/Joule), forecasts degradation,
simulates replacement impact, and schedules maintenance during low-carbon
windows.

Architecture
------------
* Sub-services: Tracking, Forecasting, DigitalTwin, Distillation, Evolution
  (NSGA-II), Gating (MoE), Persistence, Scheduling.
* Shared `ObservabilityHub` (Prometheus-ready) and `GovernanceLayer`
  (queue + audit + timeout).
* Non-destructive `ChaosService` with full save/restore.
* Unified MOPD scalarisation with canonical objective directions.
* Full lifecycle: `start()` / `wait_ready()` / `shutdown()`.
* Pydantic v1/v2 compatible config (dataclass fallback).
* Idempotency keys with LRU cache.
* Dead-letter queue with bounded capacity.
* Bounded caches (deque maxlen).

FIXED BUGS OVER v3.3.1
----------------------
- Deferred `asyncio.create_task` from `__init__` to `start()`.
- Added `PersistenceService.close()`.
- Fixed SQLite connection leak in `_execute_async`.
- Moved `_prune_old_data` to a periodic background task.
- Added `to_dict` / `from_dict` round-trips for dataclasses.
- Standardised `CarbonProviderProtocol.get_current_intensity` to return `float`.
- Removed ambiguous `MoEGatingNetwork` dict path; uses `MaintenanceState`.
- NSGA-II uses stable integer IDs (not `id()`).
- NSGA-II refills population after dedup.
- NSGA-II bounds tournament size by population size.
- NSGA-II objective aggregation normalises to [0, 1].
- `_postprocess_forecast` uses configured confidence level.
- Teachers no longer re-instantiate config.
- `record_outcome` uses per-node state snapshots.
- `interaction_log` bounded via `deque(maxlen=...)`.
- `datetime.utcnow()` → `datetime.now(timezone.utc)`.
- Missing lifecycle + waiting on cancelled tasks.
- Real governance, chaos, safety, idempotency, dead-letter.
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
import sqlite3
import time
import uuid
import zlib
from abc import ABC, abstractmethod
from collections import OrderedDict, defaultdict, deque
from dataclasses import dataclass, field, fields as dataclass_fields
from datetime import datetime, timezone, timedelta
from enum import Enum
from pathlib import Path
from typing import (
    Any, Awaitable, Callable, Deque, Dict, FrozenSet, List, Optional,
    Protocol, Sequence, Tuple, Union, runtime_checkable,
)

import numpy as np

# ---------------------------------------------------------------------------
# Optional third-party imports
# ---------------------------------------------------------------------------
try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    aiohttp = None  # type: ignore
    AIOHTTP_AVAILABLE = False

try:
    from statsmodels.tsa.holtwinters import ExponentialSmoothing  # type: ignore
    STATSMODELS_AVAILABLE = True
except ImportError:
    ExponentialSmoothing = None  # type: ignore
    STATSMODELS_AVAILABLE = False

try:
    from sklearn.ensemble import RandomForestClassifier  # type: ignore
    from sklearn.preprocessing import LabelEncoder  # type: ignore
    SKLEARN_AVAILABLE = True
except ImportError:
    RandomForestClassifier = None  # type: ignore
    LabelEncoder = None  # type: ignore
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
# Pydantic compatibility flags (informational)
# ============================================================================
PYDANTIC_AVAILABLE = False
PYDANTIC_V2 = False
PYDANTIC_V1 = False
try:
    import pydantic as _pyd  # type: ignore
    _ver = getattr(_pyd, 'VERSION', '0') or '0'
    _maj = int(str(_ver).split('.')[0])
    if _maj >= 2:
        PYDANTIC_AVAILABLE = True
        PYDANTIC_V2 = True
    elif _maj == 1:
        PYDANTIC_AVAILABLE = True
        PYDANTIC_V1 = True
except Exception:
    pass


# ============================================================================
# Named constants
# ============================================================================
MODULE_VERSION = "4.0.0"
STATE_SCHEMA_VERSION = 4
FEATURE_DIM = 11
MAINTENANCE_ACTIONS: Tuple[str, ...] = ('replace', 'refurbish', 'monitor')
MOPD_OBJECTIVE_NAMES: Tuple[str, ...] = (
    'net_savings', 'carbon_savings', 'risk_reduction', 'longterm_efficiency',
)
MOPD_HIGHER_IS_BETTER: FrozenSet[str] = frozenset(MOPD_OBJECTIVE_NAMES)

DEFAULT_EFFICIENCY_THRESHOLD = 1.0e9
DEFAULT_MIN_POINTS = 10
DEFAULT_FORECAST_HORIZON_DAYS = 30
DEFAULT_REFRESH_INTERVAL = 3600
DEFAULT_TELEMETRY_INTERVAL = 60

MAX_IDEMPOTENCY_CACHE = 4_096
MAX_DEAD_LETTERS = 1_000
MAX_APPROVAL_AUDIT = 1_000
MAX_APPROVAL_PENDING = 512
MAX_INTERACTION_LOG = 10_000
MAX_WARMUP_HISTORY = 10_000
APPROVAL_DEFAULT_TIMEOUT = 60.0
CHAOS_MAX_FAULTS_PER_RUN = 6
DEFAULT_Z_SCORE_95 = 1.959964


# ============================================================================
# Serialization helpers
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
    if hasattr(obj, 'to_dict') and callable(obj.to_dict):
        return _to_serializable(obj.to_dict())
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
    if hasattr(obj, '__dict__'):
        return _to_serializable(obj.__dict__)
    return str(obj)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _z_score(confidence: float) -> float:
    """Two-sided z-score for the given confidence level."""
    confidence = max(0.5, min(0.999, float(confidence)))
    try:
        from statistics import NormalDist
        return NormalDist().inv_cdf(1.0 - (1.0 - confidence) / 2.0)
    except Exception:
        table = {0.80: 1.2816, 0.90: 1.6449, 0.95: 1.9600, 0.99: 2.5758}
        closest = min(table.keys(), key=lambda k: abs(k - confidence))
        return table[closest]


# ============================================================================
# Protocols
# ============================================================================
@runtime_checkable
class CarbonProviderProtocol(Protocol):
    async def get_current_intensity(self, region: str) -> float: ...
    async def close(self) -> None: ...


@runtime_checkable
class LCAProviderProtocol(Protocol):
    async def get_material_index(self, hardware_model: str) -> float: ...
    async def close(self) -> None: ...


@runtime_checkable
class AnomalyDetectorProtocol(Protocol):
    async def ingest(self, node_id: str, metrics: Dict[str, Any]) -> Optional[Any]: ...


@runtime_checkable
class StorageProtocol(Protocol):
    def save_state(self, key: str, payload: str) -> Any: ...
    def get_state(self, key: str) -> Optional[str]: ...


@runtime_checkable
class MessageQueueProtocol(Protocol):
    async def publish(self, topic: str, payload: Any) -> None: ...


@runtime_checkable
class EventBrokerProtocol(Protocol):
    def subscribe(self, event_type: str,
                  handler: Callable[[Any], Awaitable[None]]) -> None: ...


# ============================================================================
# Enums
# ============================================================================
class ApprovalStatus(str, Enum):
    PENDING = "pending"
    APPROVED = "approved"
    DENIED = "denied"
    TIMEOUT = "timeout"
    AUTO_APPROVED = "auto_approved"


class CircuitBreakerState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class FaultType(str, Enum):
    PERSISTENCE_FAILURE = "persistence_failure"
    CARBON_API_FAILURE = "carbon_api_failure"
    LCA_API_FAILURE = "lca_api_failure"
    FORECAST_FAILURE = "forecast_failure"
    DISTILLATION_FAILURE = "distillation_failure"
    MOEA_EVALUATION_FAILURE = "moea_evaluation_failure"
    TRACKER_FAILURE = "tracker_failure"


# ============================================================================
# ObservabilityHub
# ============================================================================
class ObservabilityHub:
    """Prometheus-ready counters, gauges and histograms."""

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
# DeadLetterQueue
# ============================================================================
class DeadLetterQueue:
    def __init__(self, max_size: int = MAX_DEAD_LETTERS) -> None:
        self._items: Deque[Dict[str, Any]] = deque(maxlen=max_size)
        self._lock = asyncio.Lock()

    async def record(self, topic: str, error: str,
                     payload: Any = None) -> None:
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
# IdempotencyCache
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
# Governance
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
                request_id='auto', status=ApprovalStatus.AUTO_APPROVED,
                reason='approval_disabled', decided_at=_utcnow(),
            )
            self._audit(context, r)
            return r
        timeout = float(timeout if timeout is not None else self.timeout_seconds)
        request_id = uuid.uuid4().hex
        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()
        req = ApprovalRequest(
            request_id=request_id, context=context, requested_at=_utcnow(),
            timeout_seconds=timeout, future=future,
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
                reason='human_decision', decided_at=_utcnow(),
            )
        except asyncio.TimeoutError:
            status = (ApprovalStatus.AUTO_APPROVED if self.auto_approve_on_timeout
                      else ApprovalStatus.TIMEOUT)
            result = ApprovalResult(
                request_id=request_id, status=status, reason='timeout',
                decided_at=_utcnow(),
            )
        except Exception as e:
            result = ApprovalResult(
                request_id=request_id, status=ApprovalStatus.DENIED,
                reason=f'error:{e}', decided_at=_utcnow(),
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
# Safety
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
# Configuration (dataclass with __post_init__ validation)
# ============================================================================
@dataclass
class PredictiveMaintenanceConfig:
    efficiency_threshold: float = DEFAULT_EFFICIENCY_THRESHOLD
    min_data_points: int = DEFAULT_MIN_POINTS
    forecast_horizon_days: int = DEFAULT_FORECAST_HORIZON_DAYS
    forecast_confidence: float = 0.95
    low_carbon_windows: List[Dict[str, str]] = field(
        default_factory=lambda: [
            {"start": "02:00", "end": "06:00"},
            {"start": "12:00", "end": "14:00"},
        ]
    )
    replacement_efficiency_gain: float = 1.2
    refurbishment_efficiency_gain: float = 1.05
    maintenance_lead_time: int = 7
    refresh_interval: int = DEFAULT_REFRESH_INTERVAL
    persistence_enabled: bool = True
    persistence_path: str = "./predictive_maintenance.db"
    data_retention_days: int = 365
    prune_interval_inserts: int = 1_000

    carbon_intensity_enabled: bool = True
    carbon_region: str = "US-CAL-CISO"
    carbon_api_timeout_seconds: float = 10.0
    default_carbon_intensity: float = 400.0

    lca_enabled: bool = True
    default_co2_per_kwh: float = 0.2

    anomaly_trigger_enabled: bool = True
    hardware_cost_usd: float = 5000.0
    maintenance_cost_usd: float = 500.0
    carbon_offset_price_per_kg_usd: float = 0.10
    electricity_price_per_kwh_usd: float = 0.12

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
        'net_savings': 0.35, 'carbon_savings': 0.25,
        'risk_reduction': 0.25, 'longterm_efficiency': 0.15,
    })
    moea_dynamic_weights: bool = True
    moea_pareto_path: str = "./pm_moea_pareto.json"
    moea_max_concurrent_evals: int = 8

    enable_limit_graph: bool = True
    enable_modp: bool = True
    enable_rlhf: bool = True
    enable_moe: bool = True
    enable_human_approval: bool = False
    enable_temporal_safety: bool = True
    enable_chaos_testing: bool = False
    enable_idempotency: bool = True

    q_weights_path: str = "./pm_q_weights.json"
    interaction_logs_path: str = "./pm_interactions.csv"
    historical_model_path: str = "./pm_historical_model.pkl"

    def __post_init__(self) -> None:
        if self.efficiency_threshold <= 0:
            raise ValueError("efficiency_threshold must be > 0")
        if self.min_data_points < 3:
            raise ValueError("min_data_points must be >= 3")
        if not (0.5 <= self.forecast_confidence < 1.0):
            raise ValueError("forecast_confidence must be in [0.5, 1.0)")
        if self.replacement_efficiency_gain <= 1.0:
            raise ValueError("replacement_efficiency_gain must be > 1.0")
        if self.refurbishment_efficiency_gain < 1.0:
            raise ValueError("refurbishment_efficiency_gain must be >= 1.0")
        # normalise MOEA weights
        total = sum(float(v) for v in self.moea_objective_weights.values())
        if total <= 0:
            raise ValueError("moea_objective_weights must sum to > 0")
        if abs(total - 1.0) > 0.01:
            logger.warning("moea_objective_weights sum to %.4f; normalising.", total)
            for k in list(self.moea_objective_weights):
                self.moea_objective_weights[k] /= total
        # validate windows
        for w in self.low_carbon_windows:
            if 'start' not in w or 'end' not in w:
                raise ValueError("Each low_carbon_window must have 'start' and 'end'")
            try:
                s = datetime.strptime(w['start'], "%H:%M").time()
                e = datetime.strptime(w['end'], "%H:%M").time()
            except ValueError as ex:
                raise ValueError(f"Invalid time format in window {w}: {ex}")
            if s >= e:
                raise ValueError(f"Window start must be before end: {w}")

    def to_dict(self) -> Dict[str, Any]:
        return _to_serializable(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PredictiveMaintenanceConfig":
        valid = {f.name for f in dataclass_fields(cls)}
        return cls(**{k: v for k, v in data.items() if k in valid})


# ============================================================================
# Data classes
# ============================================================================
@dataclass
class EfficiencyRecord:
    timestamp: datetime
    flops_per_joule: float
    energy_joules: float
    flops: float

    def to_dict(self) -> Dict[str, Any]:
        return {
            'timestamp': self.timestamp.isoformat(),
            'flops_per_joule': float(self.flops_per_joule),
            'energy_joules': float(self.energy_joules),
            'flops': float(self.flops),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "EfficiencyRecord":
        ts = data.get('timestamp')
        if isinstance(ts, str):
            try:
                ts = datetime.fromisoformat(ts)
            except ValueError:
                ts = _utcnow()
        elif not isinstance(ts, datetime):
            ts = _utcnow()
        return cls(
            timestamp=ts,
            flops_per_joule=float(data.get('flops_per_joule', 0.0)),
            energy_joules=float(data.get('energy_joules', 0.0)),
            flops=float(data.get('flops', 0.0)),
        )


@dataclass
class MaintenanceRecommendation:
    node_id: str
    current_efficiency: float
    predicted_efficiency_in_30_days: float
    threshold: float
    days_to_threshold: float
    recommended_action: str
    suggested_date: datetime
    carbon_savings_kg: float
    cost_savings_usd: float
    payback_days: Optional[float]
    simulation_result: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'node_id': self.node_id,
            'current_efficiency': float(self.current_efficiency),
            'predicted_efficiency_in_30_days': float(self.predicted_efficiency_in_30_days),
            'threshold': float(self.threshold),
            'days_to_threshold': float(self.days_to_threshold),
            'recommended_action': self.recommended_action,
            'suggested_date': self.suggested_date.isoformat(),
            'carbon_savings_kg': float(self.carbon_savings_kg),
            'cost_savings_usd': float(self.cost_savings_usd),
            'payback_days': (
                float(self.payback_days) if self.payback_days is not None else None
            ),
            'simulation_result': _to_serializable(self.simulation_result),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MaintenanceRecommendation":
        d = dict(data)
        ts = d.get('suggested_date')
        if isinstance(ts, str):
            try:
                d['suggested_date'] = datetime.fromisoformat(ts)
            except ValueError:
                d['suggested_date'] = _utcnow()
        elif not isinstance(ts, datetime):
            d['suggested_date'] = _utcnow()
        return cls(**{k: v for k, v in d.items()
                      if k in {f.name for f in dataclass_fields(cls)}})


@dataclass
class MaintenanceState:
    current_efficiency: float
    slope: float
    days_to_threshold: float
    net_savings_replace: float
    net_savings_refurbish: float
    payback_replace: Optional[float]
    payback_refurbish: Optional[float]
    carbon_intensity: float
    material_index: float
    action_success_rates: Dict[str, float] = field(default_factory=dict)
    avg_reward: float = 0.5

    def to_feature_vector(self) -> np.ndarray:
        """11-dimensional canonical feature vector."""
        features = [
            float(np.clip(self.current_efficiency / 5e9, 0.0, 1.0)),
            float(np.clip(abs(self.slope) / 1e7, 0.0, 1.0)),
            float(np.clip(self.days_to_threshold / 90.0, 0.0, 1.0)),
            float(np.clip(self.net_savings_replace / 10000.0, -1.0, 1.0)),
            float(np.clip(self.net_savings_refurbish / 10000.0, -1.0, 1.0)),
            float(np.clip(self.carbon_intensity, 0.0, 1.0)),
            float(np.clip(self.material_index / 2.0, 0.0, 1.0)),
            float(np.clip(self.action_success_rates.get('replace', 0.5), 0.0, 1.0)),
            float(np.clip(self.action_success_rates.get('refurbish', 0.5), 0.0, 1.0)),
            float(np.clip(self.action_success_rates.get('monitor', 0.5), 0.0, 1.0)),
            float(np.clip(self.avg_reward, 0.0, 1.0)),
        ]
        return np.array(features, dtype=np.float32)

    def to_dict(self) -> Dict[str, Any]:
        return _to_serializable(self)


@dataclass
class MOPDActionWeights:
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
    def from_dict(cls, data: Dict[str, Any]) -> "MOPDActionWeights":
        return cls(
            vector_id=data.get('vector_id', uuid.uuid4().hex),
            weights=dict(data.get('weights', {})),
            objectives=dict(data.get('objectives', {})),
            scalarised_score=float(data.get('scalarised_score', 0.0)),
            stable_id=int(data.get('stable_id', 0)),
        )


# ============================================================================
# PersistenceService
# ============================================================================
class PersistenceService:
    """Async SQLite persistence with periodic pruning and DLQ."""

    def __init__(self, config: PredictiveMaintenanceConfig,
                 observability: Optional[ObservabilityHub] = None,
                 dlq: Optional[DeadLetterQueue] = None) -> None:
        self.config = config
        self.obs = observability or ObservabilityHub()
        self.dlq = dlq
        self.db_path = config.persistence_path
        os.makedirs(os.path.dirname(os.path.abspath(self.db_path)) or '.',
                    exist_ok=True)
        self._lock = asyncio.Lock()
        self._write_count = 0
        self._disabled = False
        self._init_db_sync()

    def _init_db_sync(self) -> None:
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS efficiency_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    node_id TEXT,
                    timestamp REAL,
                    flops_per_joule REAL,
                    energy_joules REAL,
                    flops REAL
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_efficiency_node_ts
                ON efficiency_history(node_id, timestamp DESC)
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS recommendations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    node_id TEXT,
                    recommendation TEXT,
                    created_at REAL
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_rec_node_created
                ON recommendations(node_id, created_at DESC)
            """)
            conn.commit()
        finally:
            conn.close()

    def disable(self) -> None:
        self._disabled = True

    def enable(self) -> None:
        self._disabled = False

    async def _execute_write(self, query: str, params: Tuple[Any, ...] = ()) -> None:
        if self._disabled:
            raise RuntimeError("PersistenceService disabled (chaos)")

        def _sync() -> None:
            conn = sqlite3.connect(self.db_path)
            try:
                conn.execute(query, params)
                conn.commit()
            finally:
                conn.close()

        await asyncio.to_thread(_sync)

    async def _execute_read(self, query: str, params: Tuple[Any, ...] = ()) -> List[Tuple[Any, ...]]:
        def _sync() -> List[Tuple[Any, ...]]:
            conn = sqlite3.connect(self.db_path)
            try:
                cur = conn.execute(query, params)
                return cur.fetchall()
            finally:
                conn.close()

        return await asyncio.to_thread(_sync)

    async def _prune_old_data(self) -> None:
        cutoff = (_utcnow() - timedelta(days=self.config.data_retention_days)).timestamp()

        def _sync() -> None:
            conn = sqlite3.connect(self.db_path)
            try:
                conn.execute("DELETE FROM efficiency_history WHERE timestamp < ?", (cutoff,))
                conn.execute("DELETE FROM recommendations WHERE created_at < ?", (cutoff,))
                conn.commit()
            finally:
                conn.close()

        try:
            await asyncio.to_thread(_sync)
            self.obs.counter('persistence_prunes_total')
        except Exception as e:
            logger.warning("Pruning failed: %s", e)
            if self.dlq:
                await self.dlq.record('persistence_prune', str(e))

    async def save_efficiency(self, node_id: str, record: EfficiencyRecord) -> None:
        async with self._lock:
            try:
                await self._execute_write(
                    "INSERT INTO efficiency_history "
                    "(node_id, timestamp, flops_per_joule, energy_joules, flops) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (node_id, record.timestamp.timestamp(),
                     record.flops_per_joule, record.energy_joules, record.flops),
                )
                self._write_count += 1
                if self._write_count % self.config.prune_interval_inserts == 0:
                    await self._prune_old_data()
            except Exception as e:
                logger.error("save_efficiency failed: %s", e)
                if self.dlq:
                    await self.dlq.record('save_efficiency', str(e),
                                           {'node_id': node_id})

    async def load_efficiency(self, node_id: str, limit: int = 1000) -> List[EfficiencyRecord]:
        rows = await self._execute_read(
            "SELECT timestamp, flops_per_joule, energy_joules, flops "
            "FROM efficiency_history WHERE node_id = ? "
            "ORDER BY timestamp DESC LIMIT ?",
            (node_id, limit),
        )
        records: List[EfficiencyRecord] = []
        for r in reversed(rows):
            try:
                records.append(EfficiencyRecord(
                    timestamp=datetime.fromtimestamp(r[0], tz=timezone.utc),
                    flops_per_joule=float(r[1]),
                    energy_joules=float(r[2]),
                    flops=float(r[3]),
                ))
            except Exception as e:
                logger.warning("Skipping invalid efficiency row: %s", e)
        return records

    async def save_recommendation(self, rec: MaintenanceRecommendation) -> None:
        async with self._lock:
            try:
                await self._execute_write(
                    "INSERT INTO recommendations (node_id, recommendation, created_at) "
                    "VALUES (?, ?, ?)",
                    (rec.node_id, json.dumps(rec.to_dict(), default=str),
                     _utcnow().timestamp()),
                )
            except Exception as e:
                logger.error("save_recommendation failed: %s", e)
                if self.dlq:
                    await self.dlq.record('save_recommendation', str(e),
                                           {'node_id': rec.node_id})

    async def load_recommendations(self, node_id: Optional[str] = None,
                                    limit: int = 10) -> List[MaintenanceRecommendation]:
        if node_id:
            rows = await self._execute_read(
                "SELECT recommendation FROM recommendations WHERE node_id = ? "
                "ORDER BY created_at DESC LIMIT ?",
                (node_id, limit),
            )
        else:
            rows = await self._execute_read(
                "SELECT recommendation FROM recommendations "
                "ORDER BY created_at DESC LIMIT ?",
                (limit,),
            )
        recs: List[MaintenanceRecommendation] = []
        for r in rows:
            try:
                data = json.loads(r[0])
                recs.append(MaintenanceRecommendation.from_dict(data))
            except Exception as e:
                logger.warning("Skipping invalid recommendation row: %s", e)
        return recs

    async def close(self) -> None:
        # SQLite connections are opened/closed per call, so nothing to release.
        # Provided for lifecycle symmetry.
        logger.debug("PersistenceService closed")


# ============================================================================
# TrackingService
# ============================================================================
class TrackingService:
    """Efficiency tracking with bounded deque and async persistence."""

    def __init__(self, config: PredictiveMaintenanceConfig,
                 persistence: Optional[PersistenceService] = None,
                 observability: Optional[ObservabilityHub] = None) -> None:
        self.config = config
        self.persistence = persistence
        self.obs = observability or ObservabilityHub()
        self.max_history = 1000
        self.history: Dict[str, Deque[EfficiencyRecord]] = defaultdict(
            lambda: deque(maxlen=self.max_history),
        )
        self._loaded: set = set()
        self._lock = asyncio.Lock()
        self._disabled = False

    def disable(self) -> None:
        self._disabled = True

    def enable(self) -> None:
        self._disabled = False

    async def _ensure_loaded(self, node_id: str) -> None:
        if node_id in self._loaded:
            return
        async with self._lock:
            if node_id in self._loaded:
                return
            if self.persistence is not None:
                try:
                    records = await self.persistence.load_efficiency(
                        node_id, limit=self.max_history,
                    )
                    for rec in records:
                        self.history[node_id].append(rec)
                except Exception as e:
                    logger.warning("Failed to load history for %s: %s", node_id, e)
            self._loaded.add(node_id)

    async def add_measurement(self, node_id: str, flops: float,
                              energy_joules: float) -> None:
        if self._disabled:
            raise RuntimeError("TrackingService disabled (chaos)")
        if energy_joules <= 0:
            logger.warning("Non-positive energy for %s; skipping", node_id)
            return
        await self._ensure_loaded(node_id)
        efficiency = float(flops) / float(energy_joules)
        rec = EfficiencyRecord(
            timestamp=_utcnow(),
            flops_per_joule=efficiency,
            energy_joules=float(energy_joules),
            flops=float(flops),
        )
        async with self._lock:
            self.history[node_id].append(rec)
        if self.persistence is not None:
            await self.persistence.save_efficiency(node_id, rec)
        self.obs.histogram('efficiency_flops_per_joule', efficiency, node=node_id)
        self.obs.gauge('nodes_tracked', len(self.history))

    async def get_series(self, node_id: str) -> Tuple[np.ndarray, np.ndarray]:
        await self._ensure_loaded(node_id)
        async with self._lock:
            records = list(self.history.get(node_id, []))
        if not records:
            return np.array([]), np.array([])
        times = np.array([r.timestamp.timestamp() for r in records], dtype=float)
        effs = np.array([r.flops_per_joule for r in records], dtype=float)
        return times, effs

    async def get_latest(self, node_id: str) -> Optional[float]:
        await self._ensure_loaded(node_id)
        async with self._lock:
            dq = self.history.get(node_id)
            if not dq:
                return None
            return float(dq[-1].flops_per_joule)

    async def get_node_health(self, node_id: str, threshold: float) -> Dict[str, Any]:
        eff = await self.get_latest(node_id)
        if eff is None:
            return {"status": "unknown", "efficiency": None}
        _, effs = await self.get_series(node_id)
        if eff < threshold:
            return {"status": "critical", "efficiency": eff}
        if len(effs) > 5:
            slope = float(np.polyfit(np.arange(len(effs)), effs, 1)[0])
            if slope < 0:
                return {"status": "degrading", "efficiency": eff}
        return {"status": "healthy", "efficiency": eff}

    def list_nodes(self) -> List[str]:
        return list(self.history.keys())


# ============================================================================
# ForecastService
# ============================================================================
class ForecastService:
    """Degradation forecast using ExponentialSmoothing / linear regression."""

    def __init__(self, config: PredictiveMaintenanceConfig,
                 observability: Optional[ObservabilityHub] = None) -> None:
        self.config = config
        self.obs = observability or ObservabilityHub()
        self._disabled = False

    def disable(self) -> None:
        self._disabled = True

    def enable(self) -> None:
        self._disabled = False

    def forecast(self, times: np.ndarray, values: np.ndarray) -> Dict[str, Any]:
        if self._disabled:
            raise RuntimeError("ForecastService disabled (chaos)")
        if len(values) < self.config.min_data_points:
            return {"error": "Insufficient data",
                    "min_points": self.config.min_data_points,
                    "samples": len(values)}
        if STATSMODELS_AVAILABLE:
            try:
                return self._statsmodels_forecast(times, values)
            except Exception as e:
                logger.warning("ExponentialSmoothing failed: %s; using linear", e)
        return self._linear_forecast(times, values)

    def _statsmodels_forecast(self, times: np.ndarray,
                              values: np.ndarray) -> Dict[str, Any]:
        model = ExponentialSmoothing(
            values, trend='add', seasonal=None, damped_trend=True,
        )
        fit = model.fit()
        forecast = np.array(fit.forecast(self.config.forecast_horizon_days))
        slope = float(fit.params.get('trend', 0.0))
        residuals = values - np.asarray(fit.fittedvalues)
        std_res = float(np.std(residuals)) if residuals.size else 0.0
        z = _z_score(self.config.forecast_confidence)
        lower = forecast - z * std_res
        upper = forecast + z * std_res
        return self._postprocess(times, values, forecast, lower, upper, slope)

    def _linear_forecast(self, times: np.ndarray,
                         values: np.ndarray) -> Dict[str, Any]:
        t0 = float(times[0])
        x = (times - t0) / (24 * 3600)
        y = values.astype(float)
        try:
            coeffs = np.polyfit(x, y, 1)
        except Exception:
            coeffs = np.array([0.0, float(np.mean(y))])
        slope, intercept = float(coeffs[0]), float(coeffs[1])
        last_day = float(x[-1])
        future_days = np.linspace(
            last_day, last_day + self.config.forecast_horizon_days,
            max(50, self.config.forecast_horizon_days),
        )
        predictions = slope * future_days + intercept
        residuals = y - (slope * x + intercept)
        std_res = float(np.std(residuals)) if residuals.size else 0.0
        z = _z_score(self.config.forecast_confidence)
        lower = predictions - z * std_res
        upper = predictions + z * std_res
        return self._postprocess(times, values, predictions, lower, upper, slope)

    def _postprocess(self, times: np.ndarray, values: np.ndarray,
                     predictions: np.ndarray, lower: np.ndarray,
                     upper: np.ndarray, slope: float) -> Dict[str, Any]:
        threshold = self.config.efficiency_threshold
        # Enforce monotonic trend for prediction to avoid spurious crossings
        predictions = np.maximum.accumulate(predictions) if slope < 0 else predictions
        predictions = np.minimum.accumulate(predictions) if slope > 0 else predictions

        days_to_threshold: Optional[float] = None
        crossing_day: Optional[float] = None
        n = max(1, len(predictions) - 1)
        for i, pred in enumerate(predictions):
            if pred < threshold:
                crossing_day = (i / n) * self.config.forecast_horizon_days
                days_to_threshold = crossing_day
                break
        if predictions[0] < threshold:
            days_to_threshold = 0.0
            crossing_day = 0.0

        self.obs.histogram('forecast_slope', float(slope))
        return {
            "slope": float(slope),
            "predictions": predictions.tolist(),
            "future_days": np.linspace(
                0.0, self.config.forecast_horizon_days, len(predictions),
            ).tolist(),
            "lower_bound": lower.tolist(),
            "upper_bound": upper.tolist(),
            "days_to_threshold": days_to_threshold,
            "crossing_day": crossing_day,
            "confidence": self.config.forecast_confidence,
        }


# ============================================================================
# DigitalTwinService
# ============================================================================
class DigitalTwinService:
    """Replacement/refurbishment simulation with real carbon + LCA integration."""

    def __init__(self, config: PredictiveMaintenanceConfig,
                 carbon_manager: Optional[CarbonProviderProtocol] = None,
                 lca_client: Optional[LCAProviderProtocol] = None,
                 observability: Optional[ObservabilityHub] = None) -> None:
        self.config = config
        self.carbon_manager = carbon_manager
        self.lca_client = lca_client
        self.obs = observability or ObservabilityHub()
        self._carbon_circuit = CircuitBreaker(
            "carbon_api",
            failure_threshold=config.moea_max_concurrent_evals,
            recovery_timeout=30.0,
        )
        self._lca_circuit = CircuitBreaker("lca_api", failure_threshold=3,
                                            recovery_timeout=30.0)
        self._disabled = False

    def disable(self) -> None:
        self._disabled = True

    def enable(self) -> None:
        self._disabled = False

    async def _get_carbon_intensity(self) -> float:
        if not self.config.carbon_intensity_enabled or self.carbon_manager is None:
            return self.config.default_co2_per_kwh
        try:
            intensity = await self._carbon_circuit.call(
                self.carbon_manager.get_current_intensity, self.config.carbon_region,
            )
            self.obs.counter('carbon_fetches_total', status='ok')
            return float(intensity)
        except Exception as e:
            logger.warning("Carbon intensity failed: %s", e)
            self.obs.counter('carbon_fetches_total', status='error')
            return self.config.default_co2_per_kwh

    async def _get_material_index(self, hardware_model: str) -> float:
        if not self.config.lca_enabled or self.lca_client is None:
            return 0.0
        try:
            idx = await self._lca_circuit.call(
                self.lca_client.get_material_index, hardware_model,
            )
            return float(idx)
        except Exception as e:
            logger.warning("LCA failed: %s", e)
            return 0.0

    async def simulate_replacement(
        self,
        node_id: str,
        current_efficiency: float,
        action: str = "replace",
        expected_new_efficiency: Optional[float] = None,
        workload_flops_per_day: float = 1e12,
        simulation_days: int = 365,
        hardware_model: Optional[str] = None,
    ) -> Dict[str, Any]:
        if self._disabled:
            raise RuntimeError("DigitalTwinService disabled (chaos)")
        if action == "replace":
            gain = self.config.replacement_efficiency_gain
        else:
            gain = self.config.refurbishment_efficiency_gain
        if expected_new_efficiency is None:
            expected_new_efficiency = current_efficiency * gain

        if current_efficiency <= 0 or expected_new_efficiency <= 0:
            return {"error": "Non-positive efficiency"}

        energy_current = workload_flops_per_day / current_efficiency
        energy_new = workload_flops_per_day / expected_new_efficiency
        energy_saved_per_day = energy_current - energy_new
        energy_saved_total = energy_saved_per_day * simulation_days

        carbon_intensity = await self._get_carbon_intensity()
        # kg CO2 per kWh (carbon_intensity assumed gCO2/kWh → /1000)
        co2_saved_total = (energy_saved_total / 3.6e6) * (carbon_intensity / 1000.0)
        cost_saved_total = (energy_saved_total / 3.6e6) * self.config.electricity_price_per_kwh_usd

        hardware_cost = self.config.hardware_cost_usd if action == "replace" else 0.0
        maintenance_cost = self.config.maintenance_cost_usd if action == "refurbish" else 0.0
        total_initial_cost = hardware_cost + maintenance_cost

        carbon_offset_value = co2_saved_total * self.config.carbon_offset_price_per_kg_usd
        net_savings = cost_saved_total + carbon_offset_value - total_initial_cost

        daily_savings = ((cost_saved_total + carbon_offset_value) / simulation_days
                         if simulation_days > 0 else 0.0)
        payback_days = (total_initial_cost / daily_savings) if daily_savings > 0 else None

        material_index = await self._get_material_index(
            hardware_model or f"node_{node_id}_model",
        )

        self.obs.counter('digital_twin_simulations_total', action=action)
        return {
            "node_id": node_id,
            "action": action,
            "current_efficiency": float(current_efficiency),
            "new_efficiency": float(expected_new_efficiency),
            "workload_flops_per_day": float(workload_flops_per_day),
            "simulation_days": int(simulation_days),
            "energy_saved_per_day_joules": float(energy_saved_per_day),
            "energy_saved_total_joules": float(energy_saved_total),
            "co2_saved_total_kg": float(co2_saved_total),
            "cost_saved_total_usd": float(cost_saved_total),
            "carbon_offset_value_usd": float(carbon_offset_value),
            "hardware_cost_usd": float(hardware_cost),
            "maintenance_cost_usd": float(maintenance_cost),
            "total_initial_cost_usd": float(total_initial_cost),
            "net_savings_usd": float(net_savings),
            "payback_days": float(payback_days) if payback_days is not None else None,
            "material_index": float(material_index),
        }


# ============================================================================
# DistillationService (teachers, student, replay buffer)
# ============================================================================
class _Teacher(ABC):
    @abstractmethod
    def predict(self, state: MaintenanceState) -> np.ndarray: ...

    @abstractmethod
    def confidence(self, state: MaintenanceState) -> float: ...

    @abstractmethod
    def name(self) -> str: ...


class RuleBasedTeacher(_Teacher):
    def name(self) -> str:
        return "rule_based"

    def predict(self, state: MaintenanceState) -> np.ndarray:
        probs = np.ones(len(MAINTENANCE_ACTIONS)) * 0.1
        if state.days_to_threshold <= 0:
            if state.net_savings_replace > state.net_savings_refurbish:
                probs[0] = 0.8
            else:
                probs[1] = 0.8
        elif state.days_to_threshold <= 14:
            if state.net_savings_replace > state.net_savings_refurbish:
                probs[0] = 0.7
            else:
                probs[1] = 0.7
        else:
            probs[2] = 0.7
        return probs / probs.sum()

    def confidence(self, state: MaintenanceState) -> float:
        return 0.6 if state.days_to_threshold <= 0 else 0.4


class HistoricalMLTeacher(_Teacher):
    def __init__(self, config: PredictiveMaintenanceConfig) -> None:
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

    def name(self) -> str:
        return "historical_ml"

    def predict(self, state: MaintenanceState) -> np.ndarray:
        if self.model is None:
            return np.ones(len(MAINTENANCE_ACTIONS)) / len(MAINTENANCE_ACTIONS)
        x = state.to_feature_vector().reshape(1, -1)
        try:
            probs = np.asarray(self.model.predict_proba(x)[0])
            if probs.size < len(MAINTENANCE_ACTIONS):
                padded = np.zeros(len(MAINTENANCE_ACTIONS))
                padded[:probs.size] = probs
                probs = padded
            total = probs.sum()
            return probs / total if total > 0 else np.ones_like(probs) / probs.size
        except Exception:
            return np.ones(len(MAINTENANCE_ACTIONS)) / len(MAINTENANCE_ACTIONS)

    def confidence(self, state: MaintenanceState) -> float:
        return 0.7 if self.model is not None else 0.0


class StatefulQTeacher(_Teacher):
    def __init__(self, config: PredictiveMaintenanceConfig, lr: float = 0.1) -> None:
        self.config = config
        self.lr = lr
        self.n_actions = len(MAINTENANCE_ACTIONS)
        self.weights = np.zeros((FEATURE_DIM, self.n_actions))
        self._save_lock = asyncio.Lock()
        self._load_state()

    def name(self) -> str:
        return "stateful_q"

    def _load_state(self) -> None:
        path = Path(self.config.q_weights_path)
        if not path.exists():
            return
        try:
            with open(path, 'r') as f:
                data = json.load(f)
            arr = np.asarray(data, dtype=float)
            if arr.shape == self.weights.shape:
                self.weights = arr
        except Exception as e:
            logger.debug("Failed to load Q weights: %s", e)

    async def _save_state_async(self) -> None:
        async with self._save_lock:
            def _write() -> None:
                path = Path(self.config.q_weights_path)
                tmp = f"{path}.tmp"
                with open(tmp, 'w') as f:
                    json.dump(self.weights.tolist(), f, indent=2)
                os.replace(tmp, path)
            try:
                await asyncio.to_thread(_write)
            except Exception as e:
                logger.debug("Failed to save Q weights: %s", e)

    def predict(self, state: MaintenanceState) -> np.ndarray:
        x = state.to_feature_vector()
        q = x @ self.weights
        exp_q = np.exp(q - np.max(q))
        return exp_q / exp_q.sum()

    def confidence(self, state: MaintenanceState) -> float:
        return 0.5

    async def update(self, state: MaintenanceState, action_idx: int,
                     reward: float) -> None:
        x = state.to_feature_vector()
        q_current = float(np.dot(x, self.weights[:, action_idx]))
        self.weights[:, action_idx] += self.lr * (reward - q_current) * x
        await self._save_state_async()


class DistillationStudent:
    def __init__(self, feature_dim: int = FEATURE_DIM,
                 n_classes: int = len(MAINTENANCE_ACTIONS),
                 lr: float = 0.01) -> None:
        self.weights = np.zeros((feature_dim, n_classes))
        self.biases = np.zeros(n_classes)
        self.lr = lr
        self.n_classes = n_classes
        self.counter = 0

    def predict_proba(self, state_vector: np.ndarray) -> np.ndarray:
        logits = state_vector @ self.weights + self.biases
        exp_logits = np.exp(logits - np.max(logits))
        return exp_logits / exp_logits.sum()

    def update(self, state_vector: np.ndarray, teacher_probs: np.ndarray,
               reward: float, action_idx: int,
               distill_weight: float = 0.7, rl_weight: float = 0.3) -> None:
        current = self.predict_proba(state_vector)
        grad_distill = -(teacher_probs - current)
        one_hot = np.zeros(self.n_classes)
        one_hot[action_idx] = 1.0
        grad_rl = -reward * (one_hot - current)
        grad = distill_weight * grad_distill + rl_weight * grad_rl
        self.weights -= self.lr * np.outer(state_vector, grad)
        self.biases -= self.lr * grad
        self.counter += 1


class ReplayBuffer:
    def __init__(self, max_size: int = 2000) -> None:
        self.buffer: Deque[Tuple[np.ndarray, int, float, np.ndarray, np.ndarray]] = deque(
            maxlen=max_size,
        )

    def push(self, s: np.ndarray, a: int, r: float, ns: np.ndarray,
             tp: np.ndarray) -> None:
        self.buffer.append((s, a, r, ns, tp))

    def sample(self, batch_size: int = 32) -> Tuple[np.ndarray, List[int],
                                                     np.ndarray, np.ndarray, np.ndarray]:
        if not self.buffer:
            empty = np.zeros((0, FEATURE_DIM), dtype=np.float32)
            return empty, [], np.zeros(0), empty, np.zeros((0, len(MAINTENANCE_ACTIONS)))
        if len(self.buffer) < batch_size:
            batch = list(self.buffer)
        else:
            batch = random.sample(self.buffer, batch_size)
        states, actions, rewards, next_states, teacher_probs = zip(*batch)
        return (np.array(states), list(actions), np.array(rewards),
                np.array(next_states), np.array(teacher_probs))

    def __len__(self) -> int:
        return len(self.buffer)


class DistillationService:
    """Multi-teacher on-policy distillation with replay buffer."""

    def __init__(self, config: PredictiveMaintenanceConfig,
                 observability: Optional[ObservabilityHub] = None) -> None:
        self.config = config
        self.obs = observability or ObservabilityHub()
        self.teachers: List[_Teacher] = [
            RuleBasedTeacher(),
            HistoricalMLTeacher(config),
            StatefulQTeacher(config),
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
        self._disabled = True

    def enable(self) -> None:
        self._disabled = False

    async def select_action(
        self, state: MaintenanceState, exploration: bool = True,
    ) -> Tuple[str, int, np.ndarray, np.ndarray]:
        if self._disabled:
            raise RuntimeError("DistillationService disabled (chaos)")
        state_vec = state.to_feature_vector()
        teacher_probs = np.zeros(len(MAINTENANCE_ACTIONS))
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
            teacher_probs = np.ones(len(MAINTENANCE_ACTIONS)) / len(MAINTENANCE_ACTIONS)

        student_probs = self.student.predict_proba(state_vec)
        if exploration and random.random() < self.epsilon:
            action_idx = random.randint(0, len(MAINTENANCE_ACTIONS) - 1)
        else:
            combined = 0.8 * student_probs + 0.2 * teacher_probs
            action_idx = int(np.argmax(combined))
        return MAINTENANCE_ACTIONS[action_idx], action_idx, state_vec, teacher_probs

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

    async def update_teacher(self, state: MaintenanceState, action_idx: int,
                             reward: float) -> None:
        for teacher in self.teachers:
            if isinstance(teacher, StatefulQTeacher):
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
# EvolutionService (NSGA-II with stable IDs + bounded concurrency)
# ============================================================================
class EvolutionService:
    def __init__(
        self,
        config: PredictiveMaintenanceConfig,
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
        self._rng = random.Random(42)
        self._np_rng = np.random.default_rng(42)
        self._next_id = 1
        self.pareto_front: List[MOPDActionWeights] = []
        self.best: Optional[MOPDActionWeights] = None
        self._disabled = False

    def disable(self) -> None:
        self._disabled = True

    def enable(self) -> None:
        self._disabled = False

    # ------------------------------------------------------------------
    # Genetic operators
    # ------------------------------------------------------------------
    def _random_individual(self) -> Dict[str, float]:
        w = {k: self._rng.random() for k in MAINTENANCE_ACTIONS}
        total = sum(w.values())
        return {k: v / total for k, v in w.items()} if total > 0 else w

    def _crossover(self, p1: Dict[str, float], p2: Dict[str, float]) -> Dict[str, float]:
        child: Dict[str, float] = {}
        for key in p1:
            if self._rng.random() < 0.5:
                u = self._rng.random()
                beta = ((2 * u) ** (1 / 21)) if u <= 0.5 else ((1 / (2 * (1 - u))) ** (1 / 21))
                child[key] = max(0.0, min(1.0, 0.5 * ((1 + beta) * p1[key] + (1 - beta) * p2[key])))
            else:
                child[key] = p1[key] if self._rng.random() < 0.5 else p2[key]
        total = sum(child.values())
        return {k: v / total for k, v in child.items()} if total > 0 else child

    def _mutate(self, ind: Dict[str, float]) -> Dict[str, float]:
        mutant = dict(ind)
        for key in mutant:
            if self._rng.random() < self.mutation_rate:
                u = self._rng.random()
                delta = ((2 * u) ** (1 / 21) - 1) if u < 0.5 else (1 - (2 * (1 - u)) ** (1 / 21))
                mutant[key] = max(0.0, min(1.0, mutant[key] + delta))
        total = sum(mutant.values())
        return {k: v / total for k, v in mutant.items()} if total > 0 else mutant

    # ------------------------------------------------------------------
    # Pareto utilities (stable IDs)
    # ------------------------------------------------------------------
    def _fast_non_dominated_sort(
        self, points: Sequence[MOPDActionWeights],
    ) -> List[List[MOPDActionWeights]]:
        if not points:
            return []
        n = len(points)
        domination_count = [0] * n
        dominated: List[List[int]] = [[] for _ in range(n)]
        fronts: List[List[int]] = [[]]
        for i in range(n):
            pi = points[i].objective_vector()
            for j in range(n):
                if i == j:
                    continue
                pj = points[j].objective_vector()
                if all(a <= b for a, b in zip(pj, pi)) and any(a < b for a, b in zip(pj, pi)):
                    dominated[i].append(j)
                elif all(a <= b for a, b in zip(pi, pj)) and any(a < b for a, b in zip(pi, pj)):
                    domination_count[i] += 1
            if domination_count[i] == 0:
                fronts[0].append(i)
        i = 0
        while i < len(fronts) and fronts[i]:
            nxt: List[int] = []
            for idx in fronts[i]:
                for other in dominated[idx]:
                    domination_count[other] -= 1
                    if domination_count[other] == 0:
                        nxt.append(other)
            if nxt:
                fronts.append(nxt)
            i += 1
        return [[points[idx] for idx in front] for front in fronts if front]

    def _crowding_distance(self,
                            front: Sequence[MOPDActionWeights]) -> Dict[int, float]:
        if not front:
            return {}
        distances = {p.stable_id: 0.0 for p in front}
        for obj in MOPD_OBJECTIVE_NAMES:
            sf = sorted(front, key=lambda x: x.objectives.get(obj, 0.0))
            distances[sf[0].stable_id] = float('inf')
            distances[sf[-1].stable_id] = float('inf')
            lo = sf[0].objectives.get(obj, 0.0)
            hi = sf[-1].objectives.get(obj, 0.0)
            if hi == lo:
                continue
            for i in range(1, len(sf) - 1):
                distances[sf[i].stable_id] += (
                    (sf[i + 1].objectives.get(obj, 0.0) - sf[i - 1].objectives.get(obj, 0.0))
                    / (hi - lo)
                )
        return distances

    def _tournament_selection(
        self,
        population: Sequence[Dict[str, float]],
        points: Sequence[MOPDActionWeights],
        fronts: Sequence[Sequence[MOPDActionWeights]],
        crowding: Dict[int, float],
    ) -> Dict[str, float]:
        k = min(self.tournament_size, len(population))
        if k <= 0:
            return self._random_individual()
        candidates = self._rng.sample(list(range(len(population))), k)
        rank_of = {}
        for rank, front in enumerate(fronts):
            for p in front:
                rank_of[p.stable_id] = rank
        best_idx = candidates[0]
        best_rank = float('inf')
        best_cd = -float('inf')
        for idx in candidates:
            p = points[idx]
            r = rank_of.get(p.stable_id, len(fronts))
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
        if boosted:
            total = sum(weights.values())
            if total > 0:
                weights = {k: v / total for k, v in weights.items()}
        return weights

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------
    async def evolve(self) -> List[MOPDActionWeights]:
        if self._disabled:
            raise RuntimeError("EvolutionService disabled (chaos)")

        async def _eval(ind: Dict[str, float]) -> Dict[str, float]:
            async with self._semaphore:
                return await self.evaluate(ind)

        population = [self._random_individual() for _ in range(self.population_size)]
        results = await asyncio.gather(*[_eval(ind) for ind in population])
        points: List[MOPDActionWeights] = []
        for ind, obj in zip(population, results):
            points.append(MOPDActionWeights(
                vector_id=uuid.uuid4().hex,
                weights=dict(ind),
                objectives=dict(obj),
                stable_id=self._next_id,
            ))
            self._next_id += 1

        for gen in range(self.generations):
            fronts = self._fast_non_dominated_sort(points)
            crowding: Dict[int, float] = {}
            for front in fronts:
                crowding.update(self._crowding_distance(front))

            offspring: List[Dict[str, float]] = []
            while len(offspring) < self.population_size:
                p1 = self._tournament_selection(population, points, fronts, crowding)
                p2 = self._tournament_selection(population, points, fronts, crowding)
                child = self._crossover(p1, p2) if self._rng.random() < self.crossover_rate else dict(p1)
                child = self._mutate(child)
                offspring.append(child)

            child_results = await asyncio.gather(*[_eval(ind) for ind in offspring])
            child_points: List[MOPDActionWeights] = []
            for ind, obj in zip(offspring, child_results):
                child_points.append(MOPDActionWeights(
                    vector_id=uuid.uuid4().hex,
                    weights=dict(ind),
                    objectives=dict(obj),
                    stable_id=self._next_id,
                ))
                self._next_id += 1

            combined_inds = list(population) + offspring
            combined_points = points + child_points
            fronts = self._fast_non_dominated_sort(combined_points)

            new_pop: List[Dict[str, float]] = []
            new_points: List[MOPDActionWeights] = []
            for front in fronts:
                front_ids = {p.stable_id for p in front}
                indices = [i for i, p in enumerate(combined_points)
                           if p.stable_id in front_ids]
                if len(new_pop) + len(indices) <= self.population_size:
                    for i in indices:
                        new_pop.append(dict(combined_inds[i]))
                        new_points.append(combined_points[i])
                else:
                    cd = self._crowding_distance(front)
                    sorted_idx = sorted(
                        indices,
                        key=lambda i: cd.get(combined_points[i].stable_id, 0.0),
                        reverse=True,
                    )
                    for i in sorted_idx:
                        if len(new_pop) >= self.population_size:
                            break
                        new_pop.append(dict(combined_inds[i]))
                        new_points.append(combined_points[i])
                if len(new_pop) >= self.population_size:
                    break

            # Refill if dedup shrank us below capacity
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
                            new_pop.append(dict(combined_inds[idx]))
                            new_points.append(combined_points[idx])

            population = new_pop[:self.population_size]
            points = new_points[:self.population_size]
            fronts = self._fast_non_dominated_sort(points)
            self.pareto_front = fronts[0] if fronts else []
            self.obs.histogram('moea_pareto_size', len(self.pareto_front))
            self.obs.counter('moea_generations_total')
            logger.debug("MOEA generation %d/%d pareto=%d",
                         gen + 1, self.generations, len(self.pareto_front))

        weights = self._compute_dynamic_weights()
        self.best = scalarise_pareto(
            self.pareto_front, weights=weights,
            objective_names=MOPD_OBJECTIVE_NAMES,
            higher_is_better=MOPD_HIGHER_IS_BETTER,
        )
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
            logger.warning("Failed to save pareto: %s", e)
            return False


# ============================================================================
# GatingService (MoE)
# ============================================================================
class GatingService:
    """MoE gating network. Uses the canonical 11-dim feature vector."""

    def __init__(self, config: PredictiveMaintenanceConfig,
                 observability: Optional[ObservabilityHub] = None,
                 seed: int = 42) -> None:
        self.config = config
        self.obs = observability or ObservabilityHub()
        self.expert_names: Tuple[str, ...] = MAINTENANCE_ACTIONS
        self.num_experts = len(self.expert_names)
        rng = np.random.default_rng(seed)
        self.gating_weights = rng.standard_normal((self.num_experts, FEATURE_DIM)) * 0.01
        self.learning_rate = 0.1
        self._lock = asyncio.Lock()

    async def select_expert(self, state: MaintenanceState) -> Tuple[str, np.ndarray]:
        x = state.to_feature_vector()
        logits = self.gating_weights @ x
        probs = np.exp(logits - np.max(logits))
        probs /= probs.sum()
        idx = int(np.argmax(probs))
        self.obs.counter('gating_selections_total', expert=self.expert_names[idx])
        return self.expert_names[idx], probs

    async def add_training_sample(self, state: MaintenanceState,
                                   selected_expert: str, reward: float) -> None:
        if selected_expert not in self.expert_names:
            return
        idx = self.expert_names.index(selected_expert)
        x = state.to_feature_vector()
        async with self._lock:
            logits = self.gating_weights @ x
            probs = np.exp(logits - np.max(logits))
            probs /= probs.sum()
            target = np.zeros(self.num_experts)
            target[idx] = 1.0
            grad = (probs - target)[:, None] * x[None, :]
            self.gating_weights -= self.learning_rate * reward * grad
        self.obs.counter('gating_updates_total')


# ============================================================================
# SchedulingService
# ============================================================================
class SchedulingService:
    """Generates maintenance recommendations and tracks outcomes per node."""

    def __init__(
        self,
        config: PredictiveMaintenanceConfig,
        tracker: TrackingService,
        forecaster: ForecastService,
        simulator: DigitalTwinService,
        distillation: DistillationService,
        carbon_manager: Optional[CarbonProviderProtocol] = None,
        evolution: Optional[EvolutionService] = None,
        gating: Optional[GatingService] = None,
        governance: Optional[GovernanceLayer] = None,
        observability: Optional[ObservabilityHub] = None,
    ) -> None:
        self.config = config
        self.tracker = tracker
        self.forecaster = forecaster
        self.simulator = simulator
        self.distillation = distillation
        self.carbon_manager = carbon_manager
        self.evolution = evolution
        self.gating = gating
        self.governance = governance or GovernanceLayer(enabled=False)
        self.obs = observability or ObservabilityHub()

        self.recommendations: Dict[str, MaintenanceRecommendation] = {}
        self.interaction_log: Deque[Dict[str, Any]] = deque(maxlen=MAX_INTERACTION_LOG)
        self._pending_states: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
        self._lock = asyncio.Lock()
        self.global_best_weights: Optional[Dict[str, float]] = None
        self.pareto_front: List[MOPDActionWeights] = []

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _parse_time(self, time_str: str) -> Any:
        return datetime.strptime(time_str, "%H:%M").time()

    async def get_next_low_carbon_window(self, from_date: datetime) -> datetime:
        if from_date.tzinfo is None:
            from_date = from_date.replace(tzinfo=timezone.utc)
        for window in self.config.low_carbon_windows:
            try:
                st = self._parse_time(window["start"])
            except Exception:
                continue
            candidate = datetime.combine(from_date.date(), st, tzinfo=timezone.utc)
            if candidate > from_date:
                return candidate
        # Next day
        tomorrow = (from_date + timedelta(days=1)).date()
        first = self.config.low_carbon_windows[0]
        st = self._parse_time(first["start"])
        return datetime.combine(tomorrow, st, tzinfo=timezone.utc)

    async def _get_carbon_intensity(self) -> float:
        if self.carbon_manager is None or not self.config.carbon_intensity_enabled:
            return self.config.default_co2_per_kwh
        try:
            val = await self.carbon_manager.get_current_intensity(self.config.carbon_region)
            return float(val)
        except Exception as e:
            logger.debug("Carbon fetch failed: %s", e)
            return self.config.default_co2_per_kwh

    def _get_action_success_rates(self, node_id: Optional[str] = None) -> Dict[str, float]:
        rates = {a: 0.5 for a in MAINTENANCE_ACTIONS}
        entries = list(self.interaction_log)
        if node_id:
            entries = [e for e in entries if e.get('node_id') == node_id]
        if not entries:
            return rates
        recent = entries[-100:]
        for a in rates:
            subset = [e for e in recent if e.get('action') == a]
            if subset:
                rates[a] = sum(1 for e in subset if e.get('reward', 0.0) > 0.5) / len(subset)
        return rates

    def _get_avg_reward(self, node_id: Optional[str] = None) -> float:
        entries = list(self.interaction_log)
        if node_id:
            entries = [e for e in entries if e.get('node_id') == node_id]
        if not entries:
            return 0.5
        return float(np.mean([e.get('reward', 0.5) for e in entries[-50:]]))

    async def _evaluate_individual(self, weights: Dict[str, float]) -> Dict[str, float]:
        """
        Evaluate a candidate strategy weight vector against historical
        interaction logs. Returns canonical objectives in [0, 1].
        """
        entries = list(self.interaction_log)[-500:]
        if not entries:
            return {n: 0.5 for n in MOPD_OBJECTIVE_NAMES}
        # Per-action normalised aggregates
        action_metrics: Dict[str, Dict[str, List[float]]] = {
            a: {m: [] for m in MOPD_OBJECTIVE_NAMES} for a in MAINTENANCE_ACTIONS
        }
        for e in entries:
            a = e.get('action')
            if a not in action_metrics:
                continue
            action_metrics[a]['net_savings'].append(float(e.get('net_savings', 0.0)))
            action_metrics[a]['carbon_savings'].append(float(e.get('carbon_savings', 0.0)))
            action_metrics[a]['risk_reduction'].append(float(e.get('risk_reduction', 0.0)))
            action_metrics[a]['longterm_efficiency'].append(float(e.get('longterm_efficiency', 0.0)))

        objectives: Dict[str, float] = {}
        for m in MOPD_OBJECTIVE_NAMES:
            weighted_avg = 0.0
            total_w = 0.0
            for a, w in weights.items():
                series = action_metrics.get(a, {}).get(m, [])
                avg = float(np.mean(series)) if series else 0.5
                # Normalize scalars to [0, 1]
                if m == 'net_savings':
                    avg = float(np.clip(avg / 5000.0, 0.0, 1.0))
                elif m == 'carbon_savings':
                    avg = float(np.clip(avg / 100.0, 0.0, 1.0))
                weighted_avg += w * avg
                total_w += w
            objectives[m] = weighted_avg / total_w if total_w > 0 else 0.5
        return objectives

    # ------------------------------------------------------------------
    # Recommendation generation
    # ------------------------------------------------------------------
    async def generate_recommendation(
        self,
        node_id: str,
        current_efficiency: float,
        forecast_result: Dict[str, Any],
        sim_replace: Dict[str, Any],
        sim_refurb: Dict[str, Any],
        idempotency_key: Optional[str] = None,
    ) -> MaintenanceRecommendation:
        state = MaintenanceState(
            current_efficiency=float(current_efficiency),
            slope=float(forecast_result.get("slope", 0.0)),
            days_to_threshold=float(forecast_result.get("days_to_threshold") or 1e9),
            net_savings_replace=float(sim_replace.get("net_savings_usd", 0.0)),
            net_savings_refurbish=float(sim_refurb.get("net_savings_usd", 0.0)),
            payback_replace=sim_replace.get("payback_days"),
            payback_refurbish=sim_refurb.get("payback_days"),
            carbon_intensity=await self._get_carbon_intensity(),
            material_index=float(sim_replace.get("material_index", 0.0)),
            action_success_rates=self._get_action_success_rates(node_id),
            avg_reward=self._get_avg_reward(node_id),
        )

        # Action selection
        if self.gating is not None:
            try:
                expert_name, _ = await self.gating.select_expert(state)
                action = expert_name if expert_name in MAINTENANCE_ACTIONS else 'monitor'
                action_idx = MAINTENANCE_ACTIONS.index(action)
                state_vec = state.to_feature_vector()
                teacher_probs = np.ones(len(MAINTENANCE_ACTIONS)) / len(MAINTENANCE_ACTIONS)
            except Exception as e:
                logger.warning("Gating failed: %s", e)
                action, action_idx, state_vec, teacher_probs = await self.distillation.select_action(state)
        else:
            action, action_idx, state_vec, teacher_probs = await self.distillation.select_action(state)

        # Blend with MOEA best weights
        if self.global_best_weights is not None:
            one_hot = np.zeros(len(MAINTENANCE_ACTIONS))
            one_hot[action_idx] = 1.0
            moea_probs = np.array([
                float(self.global_best_weights.get(a, 0.0)) for a in MAINTENANCE_ACTIONS
            ])
            if moea_probs.sum() > 0:
                moea_probs = moea_probs / moea_probs.sum()
                blended = 0.7 * moea_probs + 0.3 * one_hot
                blended = blended / blended.sum()
                action_idx = int(np.argmax(blended))
                action = MAINTENANCE_ACTIONS[action_idx]

        # Governance for critical actions
        if self.governance.enabled and action == 'replace' and \
                sim_replace.get("net_savings_usd", 0.0) > 0:
            approval = await self.governance.request_approval({
                'action': 'approve_maintenance',
                'node_id': node_id,
                'maintenance_action': action,
                'net_savings_usd': sim_replace.get("net_savings_usd", 0.0),
                'carbon_savings_kg': sim_replace.get("co2_saved_total_kg", 0.0),
            })
            if approval.status not in (ApprovalStatus.APPROVED,
                                        ApprovalStatus.AUTO_APPROVED):
                logger.info("Replace denied for %s: %s",
                            node_id, approval.status.value)
                action = 'monitor'
                action_idx = MAINTENANCE_ACTIONS.index('monitor')

        # Suggested date
        days_to = forecast_result.get("days_to_threshold")
        if action == 'monitor' or days_to is None or days_to > 30:
            suggested_date = _utcnow() + timedelta(days=30)
        else:
            crossing = _utcnow() + timedelta(days=float(days_to))
            maintenance_date = crossing - timedelta(days=self.config.maintenance_lead_time)
            suggested_date = await self.get_next_low_carbon_window(maintenance_date)

        if action == 'replace':
            simulation_result = sim_replace
        elif action == 'refurbish':
            simulation_result = sim_refurb
        else:
            simulation_result = {}

        co2 = float(simulation_result.get("co2_saved_total_kg", 0.0)) if action in ("replace", "refurbish") else 0.0
        cost = float(simulation_result.get("net_savings_usd", 0.0)) if action in ("replace", "refurbish") else 0.0
        payback = simulation_result.get("payback_days")

        slope = float(forecast_result.get("slope", 0.0))
        pred_eff_30 = current_efficiency + slope * 30

        rec = MaintenanceRecommendation(
            node_id=node_id,
            current_efficiency=float(current_efficiency),
            predicted_efficiency_in_30_days=float(pred_eff_30),
            threshold=float(self.config.efficiency_threshold),
            days_to_threshold=float(days_to) if days_to is not None else float('inf'),
            recommended_action=action,
            suggested_date=suggested_date,
            carbon_savings_kg=co2,
            cost_savings_usd=cost,
            payback_days=float(payback) if payback is not None else None,
            simulation_result=simulation_result,
        )

        async with self._lock:
            self.recommendations[node_id] = rec
            # Per-node pending state snapshot (fixes cross-contamination)
            self._pending_states[node_id] = {
                'state': state,
                'action_idx': action_idx,
                'teacher_probs': teacher_probs,
                'state_vec': state_vec,
            }
            while len(self._pending_states) > 500:
                self._pending_states.popitem(last=False)

        self.obs.counter('recommendations_total', action=action)
        logger.info("Recommendation for %s: %s on %s",
                    node_id, action, suggested_date.date())
        return rec

    async def record_outcome(
        self,
        node_id: str,
        action: str,
        actual_energy_saved: float,
        actual_cost_saved: float,
        actual_carbon_saved: float,
        risk_reduction: float = 0.5,
        longterm_efficiency: float = 0.5,
    ) -> None:
        reward = float(np.clip(
            0.4 * min(1.0, actual_energy_saved / 1e6) +
            0.3 * min(1.0, actual_cost_saved / 1000.0) +
            0.3 * min(1.0, actual_carbon_saved / 100.0),
            0.0, 1.0,
        ))
        entry = {
            'timestamp': _utcnow().isoformat(),
            'node_id': node_id,
            'action': action,
            'reward': reward,
            'net_savings': float(actual_cost_saved),
            'carbon_savings': float(actual_carbon_saved),
            'risk_reduction': float(risk_reduction),
            'longterm_efficiency': float(longterm_efficiency),
        }
        async with self._lock:
            self.interaction_log.append(entry)

        # Retrieve per-node pending state
        snapshot = self._pending_states.get(node_id)
        if snapshot is not None:
            try:
                await self.distillation.update(
                    snapshot['state_vec'],
                    snapshot['action_idx'],
                    reward,
                    snapshot['state_vec'],
                    snapshot['teacher_probs'],
                )
                await self.distillation.update_teacher(
                    snapshot['state'], snapshot['action_idx'], reward,
                )
                if self.gating is not None:
                    await self.gating.add_training_sample(
                        snapshot['state'], action, reward,
                    )
            except Exception as e:
                logger.warning("Distillation update failed: %s", e)

        self.obs.counter('outcomes_total', action=action)

    async def run_moea_update(self) -> List[MOPDActionWeights]:
        if self.evolution is None:
            return []
        if len(self.interaction_log) < 5:
            return []
        try:
            pareto = await self.evolution.evolve()
            self.pareto_front = pareto
            if self.evolution.best is not None:
                self.global_best_weights = dict(self.evolution.best.weights)
                await self.evolution.save_pareto()
                logger.info("MOEA best weights: %s", self.global_best_weights)
            return pareto
        except Exception as e:
            logger.error("MOEA update failed: %s", e)
            return []

    async def get_recommendations(self) -> List[MaintenanceRecommendation]:
        async with self._lock:
            return list(self.recommendations.values())


# ============================================================================
# ChaosService
# ============================================================================
class ChaosService:
    """Non-destructive chaos testing with full save/restore."""

    def __init__(self, engine: "PredictiveMaintenanceEngine",
                 config: PredictiveMaintenanceConfig) -> None:
        self.engine = engine
        self.config = config
        self._saved: Dict[str, Any] = {}
        self._injected: List[str] = []
        self._lock = asyncio.Lock()

    async def inject(self, fault_type: FaultType, **params: Any) -> Dict[str, Any]:
        async with self._lock:
            try:
                if fault_type == FaultType.PERSISTENCE_FAILURE:
                    if self.engine.persistence is not None:
                        self._saved.setdefault(
                            'persistence_disabled', self.engine.persistence._disabled,
                        )
                        self.engine.persistence.disable()
                    return {'status': 'injected'}
                if fault_type == FaultType.CARBON_API_FAILURE:
                    if self.engine.digital_twin is not None and \
                            self.engine.digital_twin.carbon_manager is not None:
                        self._saved.setdefault(
                            'carbon_manager',
                            self.engine.digital_twin.carbon_manager,
                        )
                        self.engine.digital_twin.carbon_manager = None
                    return {'status': 'injected'}
                if fault_type == FaultType.LCA_API_FAILURE:
                    if self.engine.digital_twin is not None:
                        self._saved.setdefault('lca_client',
                                                self.engine.digital_twin.lca_client)
                        self.engine.digital_twin.lca_client = None
                    return {'status': 'injected'}
                if fault_type == FaultType.FORECAST_FAILURE:
                    if self.engine.forecaster is not None:
                        self._saved.setdefault(
                            'forecast_disabled', self.engine.forecaster._disabled,
                        )
                        self.engine.forecaster.disable()
                    return {'status': 'injected'}
                if fault_type == FaultType.DISTILLATION_FAILURE:
                    if self.engine.distillation is not None:
                        self._saved.setdefault(
                            'distill_disabled', self.engine.distillation._disabled,
                        )
                        self.engine.distillation.disable()
                    return {'status': 'injected'}
                if fault_type == FaultType.MOEA_EVALUATION_FAILURE:
                    if self.engine.evolution is not None:
                        self._saved.setdefault(
                            'evolution_disabled', self.engine.evolution._disabled,
                        )
                        self.engine.evolution.disable()
                    return {'status': 'injected'}
                if fault_type == FaultType.TRACKER_FAILURE:
                    if self.engine.tracker is not None:
                        self._saved.setdefault(
                            'tracker_disabled', self.engine.tracker._disabled,
                        )
                        self.engine.tracker.disable()
                    return {'status': 'injected'}
            except Exception as e:
                logger.exception("Chaos injection failed: %s", e)
                return {'status': 'error', 'error': str(e)}
            return {'status': 'unknown_fault'}

    def restore(self) -> None:
        eng = self.engine
        for key, value in list(self._saved.items()):
            if key == 'persistence_disabled' and eng.persistence is not None:
                eng.persistence._disabled = bool(value)
            elif key == 'carbon_manager' and eng.digital_twin is not None:
                eng.digital_twin.carbon_manager = value
            elif key == 'lca_client' and eng.digital_twin is not None:
                eng.digital_twin.lca_client = value
            elif key == 'forecast_disabled' and eng.forecaster is not None:
                eng.forecaster._disabled = bool(value)
            elif key == 'distill_disabled' and eng.distillation is not None:
                eng.distillation._disabled = bool(value)
            elif key == 'evolution_disabled' and eng.evolution is not None:
                eng.evolution._disabled = bool(value)
            elif key == 'tracker_disabled' and eng.tracker is not None:
                eng.tracker._disabled = bool(value)
            self._saved.pop(key, None)
        self._injected.clear()

    async def run_test(self) -> Dict[str, Any]:
        report: Dict[str, Any] = {'faults': [], 'results': {}, 'restored': False}
        faults = [
            FaultType.PERSISTENCE_FAILURE,
            FaultType.CARBON_API_FAILURE,
            FaultType.LCA_API_FAILURE,
            FaultType.FORECAST_FAILURE,
            FaultType.DISTILLATION_FAILURE,
            FaultType.MOEA_EVALUATION_FAILURE,
        ]
        try:
            for ft in faults[:CHAOS_MAX_FAULTS_PER_RUN]:
                res = await self.inject(ft)
                report['faults'].append(ft.value)
                report['results'][ft.value] = res
            try:
                health = await self.engine.get_health()
                report['results']['health_check'] = {
                    'status': health.get('status', 'unknown'),
                }
            except Exception as e:
                report['results']['health_check'] = {'error': str(e)}
        finally:
            if self.config.enable_chaos_testing:
                self.restore()
                report['restored'] = True
        return report


# ============================================================================
# PredictiveMaintenanceEngine (orchestrator)
# ============================================================================
class PredictiveMaintenanceEngine:
    """Enhanced Predictive Maintenance Engine v4.0.0."""

    def __init__(
        self,
        config: Optional[Union[Dict[str, Any], PredictiveMaintenanceConfig]] = None,
        carbon_manager: Optional[CarbonProviderProtocol] = None,
        lca_client: Optional[LCAProviderProtocol] = None,
        anomaly_detector: Optional[AnomalyDetectorProtocol] = None,
        storage: Optional[StorageProtocol] = None,
        publish_fn: Optional[Callable[[str, Any], Awaitable[None]]] = None,
        **kwargs: Any,
    ) -> None:
        if config is None:
            valid = {f.name for f in dataclass_fields(PredictiveMaintenanceConfig)}
            filtered = {k: v for k, v in kwargs.items() if k in valid}
            config = PredictiveMaintenanceConfig(**filtered)
        elif isinstance(config, dict):
            config = PredictiveMaintenanceConfig.from_dict(config)
        self.config = config
        self.carbon_manager = carbon_manager
        self.lca_client = lca_client
        self.anomaly_detector = anomaly_detector
        self.storage = storage

        # Observability
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

        # Sub-services
        self.persistence: Optional[PersistenceService] = (
            PersistenceService(config, self.observability, self.dlq)
            if config.persistence_enabled else None
        )
        self.tracker = TrackingService(config, self.persistence, self.observability)
        self.forecaster = ForecastService(config, self.observability)
        self.digital_twin = DigitalTwinService(
            config, carbon_manager, lca_client, self.observability,
        )
        self.distillation = DistillationService(config, self.observability)
        self.evolution = EvolutionService(
            config,
            evaluate=self._evaluate_individual_via_scheduler,
            observability=self.observability,
        ) if config.moea_enabled else None
        self.gating = GatingService(config, self.observability) if config.enable_moe else None
        self.scheduler = SchedulingService(
            config=config,
            tracker=self.tracker,
            forecaster=self.forecaster,
            simulator=self.digital_twin,
            distillation=self.distillation,
            carbon_manager=carbon_manager,
            evolution=self.evolution,
            gating=self.gating,
            governance=self.governance,
            observability=self.observability,
        )

        # Chaos
        self.chaos_service = ChaosService(self, config) if config.enable_chaos_testing else None

        # Lifecycle
        self._started = False
        self._shutting_down = False
        self._background_tasks: List[asyncio.Task] = []
        self._analysis_task: Optional[asyncio.Task] = None
        self._moea_task: Optional[asyncio.Task] = None
        self._chaos_task: Optional[asyncio.Task] = None
        self._ready_event = asyncio.Event()

        # Callbacks
        self.telemetry_callback: Optional[Callable[[MaintenanceRecommendation], None]] = None
        self.dashboard_callback: Optional[Callable[[MaintenanceRecommendation], None]] = None

        # Forward best weights so scheduler blends them in
        self._sync_best_weights_to_scheduler()

        logger.info("PredictiveMaintenanceEngine v%s initialised (call start())",
                    MODULE_VERSION)

    def _sync_best_weights_to_scheduler(self) -> None:
        if self.evolution is not None and self.scheduler is not None:
            # Attach a callback
            pass

    def _setup_invariants(self) -> None:
        def _eff_nonneg(state: Dict[str, Any]) -> bool:
            return float(state.get('efficiency', 0.0)) >= 0.0

        def _days_nonneg(state: Dict[str, Any]) -> bool:
            d = state.get('days_to_threshold')
            if d is None:
                return True
            return float(d) >= 0.0

        def _future_date(state: Dict[str, Any]) -> bool:
            d = state.get('suggested_date')
            if d is None:
                return True
            if isinstance(d, str):
                try:
                    d = datetime.fromisoformat(d)
                except ValueError:
                    return True
            return d >= _utcnow() - timedelta(minutes=1)

        self.safety.add('efficiency_nonneg', _eff_nonneg,
                        'Efficiency must be non-negative')
        self.safety.add('days_nonneg', _days_nonneg,
                        'days_to_threshold must be non-negative')
        self.safety.add('future_date', _future_date,
                        'suggested_date must not be in the past')

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def start(self) -> None:
        if self._started:
            return
        self._started = True

        # Analysis loop
        if self.config.refresh_interval > 0:
            self._analysis_task = asyncio.create_task(self._periodic_analysis())
            self._background_tasks.append(self._analysis_task)

        # MOEA loop
        if self.evolution is not None:
            self._moea_task = asyncio.create_task(self._moea_loop())
            self._background_tasks.append(self._moea_task)

        # Chaos loop
        if self.chaos_service is not None:
            self._chaos_task = asyncio.create_task(self._chaos_loop())
            self._background_tasks.append(self._chaos_task)

        self._ready_event.set()
        logger.info("PredictiveMaintenanceEngine started")

    async def wait_ready(self) -> None:
        if not self._started:
            await self.start()
        await self._ready_event.wait()

    async def shutdown(self) -> None:
        logger.info("Shutting down PredictiveMaintenanceEngine")
        self._shutting_down = True
        for t in self._background_tasks:
            t.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)
        self._background_tasks.clear()

        if self.persistence is not None:
            try:
                await self.persistence.close()
            except Exception as e:
                logger.warning("Persistence close failed: %s", e)

        if self.carbon_manager is not None and hasattr(self.carbon_manager, 'close'):
            try:
                res = self.carbon_manager.close()
                if asyncio.iscoroutine(res):
                    await res
            except Exception as e:
                logger.warning("Carbon close failed: %s", e)

        if self.lca_client is not None and hasattr(self.lca_client, 'close'):
            try:
                res = self.lca_client.close()
                if asyncio.iscoroutine(res):
                    await res
            except Exception as e:
                logger.warning("LCA close failed: %s", e)

        unmet = self.safety.final_check()
        if unmet:
            logger.warning("Unmet eventual invariants at shutdown: %s", unmet)

        logger.info("Shutdown complete")

    async def _periodic_analysis(self) -> None:
        while not self._shutting_down:
            try:
                await asyncio.sleep(self.config.refresh_interval)
                await self.run_analysis()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Periodic analysis error: %s", e)

    async def _moea_loop(self) -> None:
        while not self._shutting_down:
            try:
                await asyncio.sleep(self.config.moea_interval_seconds)
                pareto = await self.scheduler.run_moea_update()
                if pareto:
                    self.scheduler.pareto_front = pareto
                    if self.evolution is not None and self.evolution.best is not None:
                        self.scheduler.global_best_weights = dict(
                            self.evolution.best.weights,
                        )
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

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    async def update_node(self, node_id: str, flops: float,
                          energy_joules: float) -> None:
        await self.wait_ready()
        await self.tracker.add_measurement(node_id, flops, energy_joules)
        if self.telemetry_callback is not None:
            try:
                self.telemetry_callback(node_id)
            except Exception:
                pass

    async def _evaluate_individual_via_scheduler(
        self, weights: Dict[str, float],
    ) -> Dict[str, float]:
        return await self.scheduler._evaluate_individual(weights)

    async def analyze_node(
        self,
        node_id: str,
        idempotency_key: Optional[str] = None,
    ) -> Optional[MaintenanceRecommendation]:
        await self.wait_ready()
        if idempotency_key and self._idempotency.seen(idempotency_key):
            return self.scheduler.recommendations.get(node_id)

        times, effs = await self.tracker.get_series(node_id)
        if len(effs) < self.config.min_data_points:
            return None

        try:
            forecast = self.forecaster.forecast(times, effs)
        except Exception as e:
            logger.warning("Forecast failed for %s: %s", node_id, e)
            if self.dlq:
                await self.dlq.record('forecast', str(e), {'node_id': node_id})
            return None
        if "error" in forecast:
            return None

        current_eff = await self.tracker.get_latest(node_id)
        if current_eff is None:
            return None

        # Use recent average daily FLOPS
        recent = list(self.tracker.history.get(node_id, []))
        if not recent:
            return None
        total_flops = sum(r.flops for r in recent)
        days = max(len(recent), 1)
        avg_flops_per_day = total_flops / days

        try:
            sim_replace = await self.digital_twin.simulate_replacement(
                node_id, current_eff, "replace",
                workload_flops_per_day=avg_flops_per_day,
            )
            sim_refurb = await self.digital_twin.simulate_replacement(
                node_id, current_eff, "refurbish",
                workload_flops_per_day=avg_flops_per_day,
            )
        except Exception as e:
            logger.warning("Digital twin failed for %s: %s", node_id, e)
            if self.dlq:
                await self.dlq.record('digital_twin', str(e), {'node_id': node_id})
            return None

        rec = await self.scheduler.generate_recommendation(
            node_id, current_eff, forecast, sim_replace, sim_refurb,
            idempotency_key=idempotency_key,
        )

        # Safety check
        if self.config.enable_temporal_safety:
            violations = self.safety.check({
                'efficiency': rec.current_efficiency,
                'days_to_threshold': rec.days_to_threshold
                if math.isfinite(rec.days_to_threshold) else None,
                'suggested_date': rec.suggested_date,
            })
            if violations:
                logger.warning("Safety violations for %s: %s", node_id, violations)
                self.observability.counter('safety_violations_total')

        if self.persistence is not None:
            try:
                await self.persistence.save_recommendation(rec)
            except Exception as e:
                logger.warning("Recommendation save failed: %s", e)
        if self.dashboard_callback is not None:
            try:
                self.dashboard_callback(rec)
            except Exception:
                pass

        self.observability.gauge('last_efficiency', rec.current_efficiency,
                                 node=node_id)
        self.observability.histogram('analysis_completed',
                                     rec.current_efficiency)
        return rec

    async def run_analysis(
        self, node_ids: Optional[Sequence[str]] = None,
    ) -> List[MaintenanceRecommendation]:
        await self.wait_ready()
        if node_ids is None:
            node_ids = self.tracker.list_nodes()
        recs: List[MaintenanceRecommendation] = []
        for nid in node_ids:
            try:
                rec = await self.analyze_node(nid)
                if rec is not None:
                    recs.append(rec)
            except Exception as e:
                logger.warning("Analysis failed for %s: %s", nid, e)
        return recs

    async def record_outcome(
        self,
        node_id: str,
        action: str,
        actual_energy_saved: float,
        actual_cost_saved: float,
        actual_carbon_saved: float,
        risk_reduction: float = 0.5,
        longterm_efficiency: float = 0.5,
    ) -> None:
        await self.wait_ready()
        await self.scheduler.record_outcome(
            node_id, action, actual_energy_saved, actual_cost_saved,
            actual_carbon_saved, risk_reduction, longterm_efficiency,
        )

    async def run_moea_update(self) -> List[MOPDActionWeights]:
        await self.wait_ready()
        pareto = await self.scheduler.run_moea_update()
        if pareto and self.evolution is not None and self.evolution.best is not None:
            self.scheduler.global_best_weights = dict(self.evolution.best.weights)
        return pareto

    # ------------------------------------------------------------------
    # Query / dashboard
    # ------------------------------------------------------------------
    async def get_recommendations(self) -> List[MaintenanceRecommendation]:
        return await self.scheduler.get_recommendations()

    async def get_dashboard_data(self) -> Dict[str, Any]:
        await self.wait_ready()
        recs = await self.get_recommendations()
        return {
            "total_nodes": len(self.tracker.list_nodes()),
            "recommendations": [
                {
                    "node_id": r.node_id,
                    "action": r.recommended_action,
                    "suggested_date": r.suggested_date.isoformat(),
                    "carbon_savings_kg": r.carbon_savings_kg,
                    "cost_savings_usd": r.cost_savings_usd,
                    "payback_days": r.payback_days,
                    "current_efficiency": r.current_efficiency,
                    "days_to_threshold": r.days_to_threshold,
                }
                for r in recs
            ],
            "efficiency_threshold": self.config.efficiency_threshold,
        }

    async def get_health(self) -> Dict[str, Any]:
        return {
            "status": "healthy" if not self._shutting_down else "stopping",
            "version": MODULE_VERSION,
            "schema": STATE_SCHEMA_VERSION,
            "started": self._started,
            "nodes_tracked": len(self.tracker.list_nodes()),
            "recommendations_pending": len(self.scheduler.recommendations),
            "distillation_stats": self.distillation.get_stats(),
            "moea_pareto_front_size": len(self.scheduler.pareto_front),
            "moea_best_weights": self.scheduler.global_best_weights,
            "limit_graph": bool(getattr(self.scheduler, 'limit_graph_manager', None)),
            "modp": bool(getattr(self.scheduler, 'modp_solver', None)),
            "rlhf": bool(getattr(self.scheduler, 'rlhf_trainer', None)),
            "moe": self.gating is not None,
            "governance_pending": len(self.governance.pending),
            "dead_letters": len(self.dlq),
        }

    async def get_prometheus_metrics(self) -> str:
        return await self.observability.export_prometheus()

    def get_dead_letters(self) -> List[Dict[str, Any]]:
        return self.dlq.snapshot()

    # ------------------------------------------------------------------
    # Governance / chaos API
    # ------------------------------------------------------------------
    async def submit_approval_decision(self, request_id: str,
                                        approved: bool) -> bool:
        return await self.governance.submit_decision(request_id, approved)

    def get_approval_audit(self) -> List[Dict[str, Any]]:
        return self.governance.get_audit_log()

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
    # Callbacks
    # ------------------------------------------------------------------
    def register_telemetry_source(self, callback: Callable[[str], None]) -> None:
        self.telemetry_callback = callback

    def register_dashboard_callback(
        self, callback: Callable[[MaintenanceRecommendation], None],
    ) -> None:
        self.dashboard_callback = callback


# ============================================================================
# Convenience factory + dashboard
# ============================================================================
class SustainabilityDashboard:
    def __init__(self) -> None:
        self.recommendations: List[MaintenanceRecommendation] = []

    def update(self, rec: MaintenanceRecommendation) -> None:
        self.recommendations.append(rec)


def create_predictive_maintenance_system(
    config: Optional[Union[Dict[str, Any], PredictiveMaintenanceConfig]] = None,
    carbon_manager: Optional[CarbonProviderProtocol] = None,
    lca_client: Optional[LCAProviderProtocol] = None,
    anomaly_detector: Optional[AnomalyDetectorProtocol] = None,
    storage: Optional[StorageProtocol] = None,
) -> Dict[str, Any]:
    engine = PredictiveMaintenanceEngine(
        config=config,
        carbon_manager=carbon_manager,
        lca_client=lca_client,
        anomaly_detector=anomaly_detector,
        storage=storage,
    )
    dashboard = SustainabilityDashboard()
    engine.register_dashboard_callback(dashboard.update)
    return {
        "engine": engine,
        "dashboard": dashboard,
        "tracker": engine.tracker,
        "scheduler": engine.scheduler,
        "simulator": engine.digital_twin,
    }


# ============================================================================
# Exports
# ============================================================================
__all__ = [
    # Main
    "PredictiveMaintenanceEngine",
    "PredictiveMaintenanceConfig",
    "create_predictive_maintenance_system",
    "SustainabilityDashboard",
    # Sub-services
    "TrackingService",
    "ForecastService",
    "DigitalTwinService",
    "DistillationService",
    "EvolutionService",
    "GatingService",
    "PersistenceService",
    "SchedulingService",
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
    "EfficiencyRecord",
    "MaintenanceRecommendation",
    "MaintenanceState",
    "MOPDActionWeights",
    # Teachers
    "RuleBasedTeacher",
    "HistoricalMLTeacher",
    "StatefulQTeacher",
    "DistillationStudent",
    "ReplayBuffer",
    # Utilities
    "scalarise_pareto",
    "filter_pareto",
    # Constants
    "MODULE_VERSION",
    "STATE_SCHEMA_VERSION",
    "FEATURE_DIM",
    "MAINTENANCE_ACTIONS",
    "MOPD_OBJECTIVE_NAMES",
    "MOPD_HIGHER_IS_BETTER",
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
        config = PredictiveMaintenanceConfig()
        config.enable_chaos_testing = True
        engine = PredictiveMaintenanceEngine(config=config)
        await engine.start()

        # Feed some measurements
        for i in range(20):
            await engine.update_node(
                "node-001",
                flops=1e12,
                energy_joules=500.0 + 5.0 * i,  # efficiency degrades over time
            )

        recs = await engine.run_analysis(["node-001"])
        for rec in recs:
            print(f"Node {rec.node_id}: action={rec.recommended_action}, "
                  f"suggested={rec.suggested_date.date()}")

        # Record an outcome
        await engine.record_outcome(
            "node-001", action="refurbish",
            actual_energy_saved=1e6, actual_cost_saved=250.0,
            actual_carbon_saved=20.0,
        )

        # Trigger MOEA update
        pareto = await engine.run_moea_update()
        print(f"MOEA pareto size: {len(pareto)}")

        health = await engine.get_health()
        print(f"Health: {health['status']}, nodes={health['nodes_tracked']}")

        chaos = await engine.run_chaos_test()
        print(f"Chaos restored: {chaos.get('restored')}, faults={chaos.get('faults')}")

        metrics = await engine.get_prometheus_metrics()
        print(metrics[:400])

        await engine.shutdown()

    asyncio.run(_demo())
