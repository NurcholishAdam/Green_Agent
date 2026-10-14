#!/usr/bin/env python3
# sustainability/unified_engine.py
"""
Unified Sustainability Valuation Engine v6.0.0
================================================

Production-grade sustainability engine with:

- Pydantic v1/v2 compatibility layer (with dataclass fallback).
- Sub-service architecture: Dimension, Threshold, Weight, Prediction,
  MOPD, Report, Emissions, Persistence, Swarm.
- Protocols for all injected providers.
- Shared `ObservabilityHub` with Prometheus export.
- Real `GovernanceLayer` for human-in-the-loop approval (queue + audit).
- Non-destructive `ChaosService` with full save/restore.
- Unified `scalarise_pareto` / `filter_pareto` helpers (single source of truth).
- Full lifecycle: `start()` / `wait_ready()` / `shutdown()`.
- Idempotency keys with LRU cache.
- Temporal safety invariants (ALWAYS + EVENTUALLY).
- Threaded SQLite / file I/O.
- Schema versioning for persistence.
- Every previously missing method fully implemented.

Backward compatibility
----------------------
`UnifiedSustainabilityEngine` keeps the same public surface as v5.2.0 while
delegating to the new sub-services internally.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import os
import random
import sqlite3
import time
import uuid
from collections import OrderedDict, defaultdict, deque
from dataclasses import dataclass, field, fields as dataclass_fields, asdict
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import (
    Any, Awaitable, Callable, Deque, Dict, FrozenSet, List, Optional,
    Protocol, Sequence, Tuple, runtime_checkable,
)

import numpy as np

# ============================================================================
# Optional third-party imports
# ============================================================================
try:
    from prometheus_client import (  # type: ignore
        Counter, Gauge, Histogram, CollectorRegistry,
        generate_latest, CONTENT_TYPE_LATEST,
    )
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    from fastapi import FastAPI, HTTPException, BackgroundTasks, Request  # type: ignore
    from fastapi.responses import JSONResponse, Response  # type: ignore
    from contextlib import asynccontextmanager  # type: ignore
    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False

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
model_validator: Any = None
field_validator: Any = None

try:
    import pydantic as _pyd  # type: ignore
    _ver = getattr(_pyd, 'VERSION', '0') or '0'
    _maj = int(str(_ver).split('.')[0])
    if _maj >= 2:
        from pydantic import (  # type: ignore
            BaseModel, ConfigDict, Field, field_validator, model_validator,
        )
        PYDANTIC_AVAILABLE = True
        PYDANTIC_V2 = True
    elif _maj == 1:
        from pydantic import (  # type: ignore
            BaseModel, Field, root_validator, validator,
        )
        PYDANTIC_AVAILABLE = True
        PYDANTIC_V1 = True
except Exception:
    PYDANTIC_AVAILABLE = False
    PYDANTIC_V1 = False
    PYDANTIC_V2 = False


# ============================================================================
# Named constants
# ============================================================================
ENGINE_VERSION = "6.0.0"
STATE_SCHEMA_VERSION = 6
ENGINE_NAME = "unified_sustainability_engine"

DEFAULT_HISTORY_LIMIT = 10_000
DEFAULT_DIMENSION_HISTORY_LIMIT = 100
DEFAULT_PARETO_HISTORY_LIMIT = 1_000
MAX_APPROVAL_AUDIT = 1_000
MAX_DEAD_LETTERS = 1_000
MAX_IDEMPOTENCY_CACHE = 4_096
MAX_APPROVAL_PENDING = 512
APPROVAL_DEFAULT_TIMEOUT = 60.0
CHAOS_MAX_FAULTS_PER_RUN = 6

DIMENSION_NAMES: Tuple[str, ...] = (
    'carbon', 'helium', 'energy', 'circularity', 'biodiversity',
)
DIMENSION_UNITS: Dict[str, str] = {
    'carbon': 'gCO2e/kWh',
    'helium': 'L/inference',
    'energy': 'J/inference',
    'circularity': 'index',
    'biodiversity': 'index',
}
DEFAULT_SCARCITY: Dict[str, float] = {d: 1.0 for d in DIMENSION_NAMES}
DEFAULT_WEIGHTS: Dict[str, float] = {
    'carbon': 0.25, 'helium': 0.20, 'energy': 0.15,
    'circularity': 0.25, 'biodiversity': 0.15,
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


# ============================================================================
# Protocols
# ============================================================================
@runtime_checkable
class CarbonProviderProtocol(Protocol):
    async def get_current_intensity(self, region: str) -> float: ...


@runtime_checkable
class HeliumTrackerProtocol(Protocol):
    async def get_helium_position(self) -> Dict[str, Any]: ...


@runtime_checkable
class CircularManagerProtocol(Protocol):
    async def get_circularity_report(self) -> Dict[str, Any]: ...


@runtime_checkable
class BiodiversityProviderProtocol(Protocol):
    async def get_biodiversity_report(self) -> Dict[str, Any]: ...


@runtime_checkable
class ExpertRegistryProtocol(Protocol):
    async def get_all_active_experts(self) -> List[Any]: ...


@runtime_checkable
class QuantumLimitsProtocol(Protocol):
    async def update_sustainability_limits(self, score: float,
                                          dimensions: Dict[str, float]) -> None: ...


@runtime_checkable
class EventBrokerProtocol(Protocol):
    def subscribe(self, event_type: str,
                  handler: Callable[[Any], Awaitable[None]]) -> None: ...
    def publish(self, topic: str, payload: Any) -> Any: ...


@runtime_checkable
class MessageQueueProtocol(Protocol):
    async def publish(self, topic: str, payload: Any) -> None: ...


@runtime_checkable
class SwarmCoordinatorProtocol(Protocol):
    async def share_predictions(self, payload: Dict[str, Any]) -> None: ...


@runtime_checkable
class SelfHealerProtocol(Protocol):
    async def apply_healing(self, damage_type: str) -> Any: ...


@runtime_checkable
class WorkflowOrchestratorProtocol(Protocol):
    async def execute_workflow(self, name: str) -> Any: ...


# ============================================================================
# Enums
# ============================================================================
class ApprovalStatus(str, Enum):
    PENDING = "pending"
    APPROVED = "approved"
    DENIED = "denied"
    TIMEOUT = "timeout"
    AUTO_APPROVED = "auto_approved"


class FaultType(str, Enum):
    CARBON_SPIKE = "carbon_spike"
    HELIUM_CRITICAL = "helium_critical"
    PROVIDER_FAILURE = "provider_failure"
    PERSISTENCE_FAILURE = "persistence_failure"
    TELEMETRY_FAILURE = "telemetry_failure"
    SWARM_FAILURE = "swarm_failure"


# ============================================================================
# ObservabilityHub
# ============================================================================
class ObservabilityHub:
    """Prometheus-ready metrics hub shared across sub-services."""

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
# Circuit breaker
# ============================================================================
class CircuitBreakerState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class EnhancedCircuitBreaker:
    def __init__(self, name: str, failure_threshold: int = 5,
                 recovery_timeout: float = 30.0) -> None:
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time: Optional[float] = None
        self._lock = asyncio.Lock()
        self.metrics = {"total": 0, "failed": 0, "ok": 0}

    async def call(self, func: Callable[..., Awaitable[Any]],
                   *args: Any, **kwargs: Any) -> Any:
        async with self._lock:
            if self.state == CircuitBreakerState.OPEN:
                if (self.last_failure_time is not None and
                        (time.monotonic() - self.last_failure_time) >= self.recovery_timeout):
                    self.state = CircuitBreakerState.HALF_OPEN
                    self.success_count = 0
                else:
                    raise RuntimeError(f"Circuit {self.name} OPEN")
        self.metrics["total"] += 1
        try:
            result = await func(*args, **kwargs)
            async with self._lock:
                self.metrics["ok"] += 1
                self.success_count += 1
                if self.state == CircuitBreakerState.HALF_OPEN and self.success_count >= 2:
                    self.state = CircuitBreakerState.CLOSED
                    self.failure_count = 0
            return result
        except Exception:
            async with self._lock:
                self.metrics["failed"] += 1
                self.failure_count += 1
                self.last_failure_time = time.monotonic()
                if (self.state == CircuitBreakerState.CLOSED and
                        self.failure_count >= self.failure_threshold):
                    self.state = CircuitBreakerState.OPEN
                elif self.state == CircuitBreakerState.HALF_OPEN:
                    self.state = CircuitBreakerState.OPEN
            raise


# ============================================================================
# Retry helper
# ============================================================================
async def retry_async(func: Callable[..., Awaitable[Any]], max_retries: int,
                      base_delay_ms: float, max_delay_ms: float,
                      *args: Any, **kwargs: Any) -> Any:
    for attempt in range(max_retries):
        try:
            return await func(*args, **kwargs)
        except Exception:
            if attempt == max_retries - 1:
                raise
            delay = min(base_delay_ms * (2 ** attempt), max_delay_ms) / 1000.0
            delay *= (1 + random.uniform(-0.2, 0.2))
            await asyncio.sleep(delay)
    raise RuntimeError("Max retries exceeded")


# ============================================================================
# Idempotency cache
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
    """Human-in-the-loop approval with queue topic, audit, and timeout."""

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
        self,
        context: Dict[str, Any],
        timeout: Optional[float] = None,
    ) -> ApprovalResult:
        if not self.enabled:
            r = ApprovalResult(
                request_id='auto',
                status=ApprovalStatus.AUTO_APPROVED,
                reason='approval_disabled',
                decided_at=datetime.now(timezone.utc),
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
            requested_at=datetime.now(timezone.utc),
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
                        'approval_requests',
                        json.dumps(req.to_dict(), default=str),
                    )
                except Exception as e:
                    logger.warning("Approval publish failed: %s", e)
            decision = await asyncio.wait_for(future, timeout=timeout)
            result = ApprovalResult(
                request_id=request_id,
                status=ApprovalStatus.APPROVED if decision else ApprovalStatus.DENIED,
                reason='human_decision',
                decided_at=datetime.now(timezone.utc),
            )
        except asyncio.TimeoutError:
            status = (ApprovalStatus.AUTO_APPROVED
                      if self.auto_approve_on_timeout
                      else ApprovalStatus.TIMEOUT)
            result = ApprovalResult(
                request_id=request_id,
                status=status,
                reason='timeout',
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
# Temporal safety
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
# Unified MOPD scalarisation
# ============================================================================
def _point_objective_vector(point: Any, names: Sequence[str],
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
    """Min-max scalarise Pareto points; returns best plan and stamps scores."""
    if not points:
        return None
    names = list(objective_names or DIMENSION_NAMES)
    hib = set(higher_is_better or frozenset())
    w = dict(weights or {name: 1.0 / len(names) for name in names})

    vectors = [_point_objective_vector(p, names, hib) for p in points]
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
    names = list(objective_names or DIMENSION_NAMES)
    hib = set(higher_is_better or frozenset())
    vecs = [_point_objective_vector(p, names, hib) for p in points]
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
# Data classes
# ============================================================================
@dataclass
class SustainabilityDimension:
    name: str
    current_value: float = 0.5
    target_value: float = 1.0
    weight: float = 0.2
    units: str = ""
    trend: str = "stable"
    confidence: float = 0.8
    scarcity_factor: float = 1.0
    historical_weights: List[float] = field(default_factory=list)
    volatility: float = 0.0
    prediction: float = 0.0
    prediction_confidence: float = 0.0
    last_update: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            'name': self.name,
            'current_value': float(self.current_value),
            'target_value': float(self.target_value),
            'weight': float(self.weight),
            'units': self.units,
            'trend': self.trend,
            'confidence': float(self.confidence),
            'scarcity_factor': float(self.scarcity_factor),
            'volatility': float(self.volatility),
            'prediction': float(self.prediction),
            'prediction_confidence': float(self.prediction_confidence),
            'last_update': self.last_update.isoformat() if self.last_update else None,
        }


@dataclass
class SustainabilityThreshold:
    dimension: str
    warning_threshold: float = 0.3
    critical_threshold: float = 0.1
    current_value: float = 0.0
    status: str = "unknown"
    adaptive_warning: float = 0.0
    adaptive_critical: float = 0.0
    historical_avg: float = 0.0
    history_std: float = 0.0
    alert_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return _to_serializable(self)


@dataclass
class UnifiedSustainabilityScore:
    total_score: float
    dimensions: Dict[str, Dict[str, Any]]
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    confidence: float = 0.8
    trend: str = "stable"
    risk_factors: List[str] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)
    predicted_future_score: Optional[float] = None
    scenario_scores: Dict[str, float] = field(default_factory=dict)
    report_template: str = "standard"

    def to_dict(self) -> Dict[str, Any]:
        return _to_serializable(self)


@dataclass
class MOPDPoint:
    weights: Dict[str, float]
    dimensions: Dict[str, float]
    scalarised_score: float = 0.0
    explanation: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            'weights': dict(self.weights),
            'dimensions': dict(self.dimensions),
            'scalarised_score': float(self.scalarised_score),
            'explanation': self.explanation,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MOPDPoint":
        return cls(
            weights=dict(data.get('weights', {})),
            dimensions=dict(data.get('dimensions', {})),
            scalarised_score=float(data.get('scalarised_score', 0.0)),
            explanation=str(data.get('explanation', '')),
        )

    def objective_vector(self) -> List[float]:
        # Higher dimension scores are better → negate for lower-is-better form.
        return [-float(self.dimensions.get(name, 0.0)) for name in DIMENSION_NAMES]


@dataclass
class ReportTemplate:
    name: str
    description: str
    included_dimensions: List[str]
    metrics: List[str]
    format: str = "json"
    frequency: str = "daily"
    target_audience: str = "general"
    customization: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return _to_serializable(self)


# ============================================================================
# Configuration (dataclass-based, Pydantic-shaped)
# ============================================================================
@dataclass
class MOPDConfig:
    enabled: bool = True
    objective_weights: Dict[str, float] = field(
        default_factory=lambda: dict(DEFAULT_WEIGHTS),
    )
    grid_resolution: int = 5
    enable_cost_benefit: bool = True
    enable_predictive: bool = True

    def __post_init__(self) -> None:
        total = sum(self.objective_weights.values())
        if total <= 0:
            raise ValueError("MOPD objective_weights must sum to > 0")
        if abs(total - 1.0) > 1e-6:
            logger.warning("MOPD weights sum to %.4f; normalising.", total)
            for k in list(self.objective_weights):
                self.objective_weights[k] /= total
        extra = set(self.objective_weights) - set(DIMENSION_NAMES)
        if extra:
            raise ValueError(f"Unknown MOPD weight keys: {sorted(extra)}")


@dataclass
class FederatedSustainabilityConfig:
    enabled: bool = False
    server_url: Optional[str] = None
    sparsity_ratio: float = 0.1
    privacy_epsilon: float = 1.0
    privacy_delta: float = 1e-5
    privacy_clip_norm: float = 1.0
    sync_interval_seconds: int = 3600
    request_timeout_seconds: float = 30.0


@dataclass
class ApprovalConfig:
    enabled: bool = False
    timeout_seconds: float = APPROVAL_DEFAULT_TIMEOUT
    auto_approve_on_timeout: bool = False
    aggressive_weight_change_threshold: float = 0.2


@dataclass
class ChaosConfig:
    enabled: bool = False
    interval_seconds: int = 3600
    auto_restore: bool = True


@dataclass
class SustainabilityEngineConfig:
    dimension_weights: Dict[str, float] = field(
        default_factory=lambda: dict(DEFAULT_WEIGHTS),
    )
    warning_threshold: float = 0.3
    critical_threshold: float = 0.1
    adaptation_rate: float = 0.1
    adaptive_window_size: int = 100
    prediction_window: int = 50
    model_weights: Dict[str, float] = field(default_factory=lambda: {
        'linear': 0.4, 'exponential': 0.3, 'moving_average': 0.3,
    })
    max_retries: int = 3
    retry_base_delay_ms: float = 100.0
    retry_max_delay_ms: float = 5000.0
    circuit_breaker_failure_threshold: int = 5
    circuit_breaker_recovery_timeout: float = 30.0
    history_limit: int = DEFAULT_HISTORY_LIMIT
    dimension_history_limit: int = DEFAULT_DIMENSION_HISTORY_LIMIT
    persistence_path: str = "sustainability_engine_state.json"
    telemetry_export_interval: int = 60
    report_templates_path: Optional[str] = None

    enable_event_driven: bool = True
    enable_self_healing: bool = True
    enable_swarm_coordination: bool = True
    enable_time_tick_engine: bool = True
    enable_quantum_bridge: bool = True
    enable_cost_benefit: bool = True
    enable_workflow_orchestration: bool = True

    workflow_on_critical_alert: str = "adjust_sustainability_strategy"
    workflow_on_slo_breach: str = "rebalance_dimensions"
    swarm_share_interval: int = 60
    cache_ttl: int = 60
    default_region: str = "global"

    dimension_adaptive_mapping: Dict[str, str] = field(default_factory=lambda: {
        'carbon': 'beta', 'helium': 'gamma', 'energy': 'alpha',
        'circularity': 'delta', 'biodiversity': 'epsilon',
    })

    # Cross-cutting flags (top-level, as of v6.0.0)
    enable_xai: bool = True
    enable_temporal_safety: bool = True
    enable_chaos_testing: bool = False
    enable_idempotency: bool = True

    mopd: MOPDConfig = field(default_factory=MOPDConfig)
    federated: FederatedSustainabilityConfig = field(
        default_factory=FederatedSustainabilityConfig,
    )
    approval: ApprovalConfig = field(default_factory=ApprovalConfig)
    chaos: ChaosConfig = field(default_factory=ChaosConfig)

    def __post_init__(self) -> None:
        total = sum(self.dimension_weights.values())
        if abs(total - 1.0) > 1e-6:
            logger.warning("dimension_weights sum to %.4f; normalising.", total)
            for k in list(self.dimension_weights):
                self.dimension_weights[k] /= total
        if self.critical_threshold > self.warning_threshold:
            raise ValueError("critical_threshold must be <= warning_threshold")
        if self.retry_max_delay_ms < self.retry_base_delay_ms:
            raise ValueError("retry_max_delay_ms must be >= retry_base_delay_ms")

    def to_dict(self) -> Dict[str, Any]:
        return _to_serializable(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SustainabilityEngineConfig":
        data = dict(data)
        if isinstance(data.get('mopd'), dict):
            data['mopd'] = MOPDConfig(**data['mopd'])
        if isinstance(data.get('federated'), dict):
            data['federated'] = FederatedSustainabilityConfig(**data['federated'])
        if isinstance(data.get('approval'), dict):
            data['approval'] = ApprovalConfig(**data['approval'])
        if isinstance(data.get('chaos'), dict):
            data['chaos'] = ChaosConfig(**data['chaos'])
        return cls(**data)


# ============================================================================
# Sub-service: DimensionService
# ============================================================================
class DimensionService:
    """Collects dimension scores from injected providers."""

    def __init__(self, engine: "UnifiedSustainabilityEngine") -> None:
        self.engine = engine
        self.obs = engine.observability
        self._circuits: Dict[str, EnhancedCircuitBreaker] = {
            'carbon': engine._carbon_circuit,
            'helium': engine._helium_circuit,
            'circularity': engine._circular_circuit,
            'biodiversity': engine._biodiversity_circuit,
            'expert': engine._expert_circuit,
            'quantum': engine._quantum_circuit,
        }

    async def collect(self, region: str) -> Dict[str, SustainabilityDimension]:
        cfg = self.engine.config
        dims: Dict[str, SustainabilityDimension] = {}

        # Carbon
        carbon_value = 0.5
        if self.engine.carbon_manager is not None:
            try:
                intensity = await self._circuits['carbon'].call(
                    retry_async,
                    self.engine.carbon_manager.get_current_intensity, region,
                    cfg.max_retries, cfg.retry_base_delay_ms, cfg.retry_max_delay_ms,
                )
                carbon_value = max(0.0, min(1.0, 1.0 - float(intensity) / 800.0))
                self.obs.counter('provider_calls_total', status='ok', provider='carbon')
            except Exception as e:
                logger.warning("carbon provider failed: %s", e)
                self.obs.counter('provider_calls_total', status='error', provider='carbon')
        dims['carbon'] = SustainabilityDimension(
            name='carbon',
            current_value=carbon_value,
            weight=self.engine.dimension_weights.get('carbon', 0.25),
            units=DIMENSION_UNITS['carbon'],
        )

        # Helium
        helium_value = 0.5
        if self.engine.helium_tracker is not None:
            try:
                pos = await self._circuits['helium'].call(
                    retry_async,
                    self.engine.helium_tracker.get_helium_position,
                    cfg.max_retries, cfg.retry_base_delay_ms, cfg.retry_max_delay_ms,
                )
                scarcity = float(pos.get('scarcity_index', 0.5))
                helium_value = max(0.0, min(1.0, 1.0 - scarcity))
                self.obs.counter('provider_calls_total', status='ok', provider='helium')
            except Exception as e:
                logger.warning("helium provider failed: %s", e)
                self.obs.counter('provider_calls_total', status='error', provider='helium')
        dims['helium'] = SustainabilityDimension(
            name='helium',
            current_value=helium_value,
            weight=self.engine.dimension_weights.get('helium', 0.20),
            units=DIMENSION_UNITS['helium'],
        )

        # Energy
        energy_value = 0.5
        if self.engine.adaptive_cost_function is not None:
            try:
                weights = getattr(self.engine.adaptive_cost_function,
                                  'get_current_weights', lambda: {})()
                alpha = float(weights.get('alpha', 0.5))
                energy_value = max(0.0, min(1.0, 1.0 - alpha))
            except Exception as e:
                logger.warning("energy provider failed: %s", e)
        dims['energy'] = SustainabilityDimension(
            name='energy',
            current_value=energy_value,
            weight=self.engine.dimension_weights.get('energy', 0.15),
            units=DIMENSION_UNITS['energy'],
        )

        # Circularity
        circ_value = 0.5
        if self.engine.circular_manager is not None:
            try:
                report = await self._circuits['circularity'].call(
                    retry_async,
                    self.engine.circular_manager.get_circularity_report,
                    cfg.max_retries, cfg.retry_base_delay_ms, cfg.retry_max_delay_ms,
                )
                circ_value = float(report.get('circularity_score', 0.5))
                circ_value = max(0.0, min(1.0, circ_value))
                self.obs.counter('provider_calls_total', status='ok', provider='circularity')
            except Exception as e:
                logger.warning("circularity provider failed: %s", e)
                self.obs.counter('provider_calls_total', status='error', provider='circularity')
        dims['circularity'] = SustainabilityDimension(
            name='circularity',
            current_value=circ_value,
            weight=self.engine.dimension_weights.get('circularity', 0.25),
            units=DIMENSION_UNITS['circularity'],
        )

        # Biodiversity
        bio_value = 0.5
        if self.engine.biodiversity is not None:
            try:
                report = await self._circuits['biodiversity'].call(
                    retry_async,
                    self.engine.biodiversity.get_biodiversity_report,
                    cfg.max_retries, cfg.retry_base_delay_ms, cfg.retry_max_delay_ms,
                )
                bio_value = float(report.get('sustainability_score', 0.5))
                bio_value = max(0.0, min(1.0, bio_value))
                self.obs.counter('provider_calls_total', status='ok', provider='biodiversity')
            except Exception as e:
                logger.warning("biodiversity provider failed: %s", e)
                self.obs.counter('provider_calls_total', status='error', provider='biodiversity')
        dims['biodiversity'] = SustainabilityDimension(
            name='biodiversity',
            current_value=bio_value,
            weight=self.engine.dimension_weights.get('biodiversity', 0.15),
            units=DIMENSION_UNITS['biodiversity'],
        )

        # Apply scarcity multipliers (from event stream)
        for name, dim in dims.items():
            dim.scarcity_factor = float(self.engine.scarcity_factors.get(name, 1.0))
            dim.last_update = datetime.now(timezone.utc)

        # Update engine state
        self.engine.dimensions = dims
        return dims


# ============================================================================
# Sub-service: ThresholdService
# ============================================================================
class ThresholdService:
    """Adaptive thresholds based on dimension history."""

    def __init__(self, engine: "UnifiedSustainabilityEngine") -> None:
        self.engine = engine
        self.history: Dict[str, Deque[float]] = defaultdict(
            lambda: deque(maxlen=engine.config.adaptive_window_size),
        )

    async def update_all(self, dimensions: Dict[str, SustainabilityDimension]) -> None:
        cfg = self.engine.config
        for name, dim in dimensions.items():
            self.history[name].append(float(dim.current_value))
            base_warning = cfg.warning_threshold
            base_critical = cfg.critical_threshold

            hist = list(self.history[name])
            if len(hist) >= 5:
                arr = np.array(hist, dtype=float)
                mean = float(np.mean(arr))
                std = max(1e-3, float(np.std(arr)))
                adaptive_warning = min(1.0, max(0.0, mean + 0.5 * std))
                adaptive_critical = min(1.0, max(0.0, mean + 1.5 * std))
                aw = cfg.adaptation_rate * adaptive_warning + (1 - cfg.adaptation_rate) * base_warning
                ac = cfg.adaptation_rate * adaptive_critical + (1 - cfg.adaptation_rate) * base_critical
            else:
                aw, ac = base_warning, base_critical

            threshold = self.engine.thresholds.get(name)
            if threshold is None:
                threshold = SustainabilityThreshold(
                    dimension=name,
                    warning_threshold=base_warning,
                    critical_threshold=base_critical,
                )
                self.engine.thresholds[name] = threshold
            threshold.current_value = float(dim.current_value)
            threshold.adaptive_warning = float(aw)
            threshold.adaptive_critical = float(ac)
            if hist:
                threshold.historical_avg = float(np.mean(hist))
                threshold.history_std = float(np.std(hist))
            if dim.current_value < ac:
                threshold.status = "critical"
                threshold.alert_count += 1
                self.engine.observability.counter('threshold_breaches_total',
                                                   dimension=name, level='critical')
            elif dim.current_value < aw:
                threshold.status = "warning"
                self.engine.observability.counter('threshold_breaches_total',
                                                   dimension=name, level='warning')
            else:
                threshold.status = "healthy"

    def get_stats(self, dimension: str) -> Dict[str, Any]:
        hist = list(self.history.get(dimension, []))
        if not hist:
            return {}
        arr = np.array(hist, dtype=float)
        return {
            'mean': float(np.mean(arr)),
            'std': float(np.std(arr)),
            'min': float(np.min(arr)),
            'max': float(np.max(arr)),
            'count': len(arr),
        }

    def get_anomaly_score(self, dimension: str, value: float) -> float:
        hist = list(self.history.get(dimension, []))
        if len(hist) < 5:
            return 0.0
        arr = np.array(hist, dtype=float)
        std = float(np.std(arr))
        if std < 1e-6:
            return 0.0
        z = abs(value - float(np.mean(arr))) / std
        return min(1.0, z / 4.0)


# ============================================================================
# Sub-service: WeightService
# ============================================================================
class WeightService:
    """Dynamic dimension weight adjustment with renormalisation."""

    def __init__(self, engine: "UnifiedSustainabilityEngine") -> None:
        self.engine = engine
        self.history: Dict[str, Deque[float]] = defaultdict(
            lambda: deque(maxlen=engine.config.dimension_history_limit),
        )

    async def adjust(self,
                     dimensions: Dict[str, SustainabilityDimension]
                     ) -> Dict[str, float]:
        base = dict(self.engine.config.dimension_weights)
        scarcity = self.engine.scarcity_factors

        weights = {}
        for name in dimensions:
            base_w = float(base.get(name, 1.0 / max(1, len(dimensions))))
            s = float(scarcity.get(name, 1.0))
            weights[name] = base_w * (1.0 + 0.1 * (s - 1.0))

        total = sum(weights.values())
        if total > 0:
            weights = {k: v / total for k, v in weights.items()}
        else:
            n = max(1, len(dimensions))
            weights = {k: 1.0 / n for k in dimensions}

        for name, w in weights.items():
            self.history[name].append(w)

        self.engine.dimension_weights = weights
        return weights

    def get_trends(self) -> Dict[str, List[float]]:
        return {k: list(v) for k, v in self.history.items()}


# ============================================================================
# Sub-service: PredictionService
# ============================================================================
class PredictionService:
    """Trend prediction using linear regression."""

    def __init__(self, engine: "UnifiedSustainabilityEngine") -> None:
        self.engine = engine

    async def update_all(self,
                         dimensions: Dict[str, SustainabilityDimension]) -> None:
        for name, dim in dimensions.items():
            self.engine.dimension_history[name].append(float(dim.current_value))
            if len(self.engine.dimension_history[name]) > self.engine.config.dimension_history_limit:
                # bounded deque enforces this
                pass
            pred, conf, vol = await self.predict(name, steps=10)
            dim.prediction = pred
            dim.prediction_confidence = conf
            dim.volatility = vol
            # trend
            hist = list(self.engine.dimension_history[name])
            if len(hist) >= 3:
                if hist[-1] > hist[0] * 1.05:
                    dim.trend = "improving"
                elif hist[-1] < hist[0] * 0.95:
                    dim.trend = "declining"
                else:
                    dim.trend = "stable"

    async def predict(self, dimension: str,
                      steps: int = 10) -> Tuple[float, float, float]:
        hist = list(self.engine.dimension_history.get(dimension, []))
        if len(hist) < 3:
            return 0.5, 0.0, 0.0

        def _compute() -> Tuple[float, float, float]:
            y = np.array(hist, dtype=float)
            if not np.all(np.isfinite(y)):
                return 0.5, 0.0, 0.0
            x = np.arange(len(y))
            try:
                coeffs = np.polyfit(x, y, 1)
            except Exception:
                return 0.5, 0.0, 0.0
            slope, intercept = float(coeffs[0]), float(coeffs[1])
            pred = intercept + slope * (len(y) + steps)
            y_pred = slope * x + intercept
            ss_res = float(np.sum((y - y_pred) ** 2))
            ss_tot = float(np.sum((y - np.mean(y)) ** 2)) + 1e-10
            r2 = 1.0 - ss_res / ss_tot
            confidence = max(0.0, min(1.0, r2))
            volatility = float(np.std(y - y_pred)) if len(y) > 2 else 0.0
            pred = max(0.0, min(1.0, pred))
            return pred, confidence, volatility

        return await asyncio.to_thread(_compute)


# ============================================================================
# Sub-service: MOPDService (real Pareto front)
# ============================================================================
class MOPDService:
    """Real Pareto front over enumerated policy candidates."""

    def __init__(self, engine: "UnifiedSustainabilityEngine") -> None:
        self.engine = engine
        self.obs = engine.observability

    async def generate(
        self,
        dimensions: Dict[str, SustainabilityDimension],
        num_samples: int = 50,
    ) -> List[MOPDPoint]:
        """
        Enumerate candidate weight vectors and simulate their effect on the
        base dimension scores using a small trade-off model. The resulting
        points are filtered for Pareto dominance.
        """
        cfg = self.engine.config
        if not cfg.mopd.enabled:
            return []

        names = list(dimensions.keys())
        base = {n: float(dimensions[n].current_value) for n in names}

        # Deterministic Dirichlet sampling over dimension weights
        def _sample() -> List[np.ndarray]:
            rng = np.random.default_rng(42)
            return [rng.dirichlet(np.ones(len(names))) for _ in range(num_samples)]

        weight_samples = await asyncio.to_thread(_sample)

        # Trade-off model: increasing a dimension's score by δ reduces others
        # proportionally to a shared "budget". This is a stand-in for real
        # policy effects and gives a meaningful (non-degenerate) Pareto front.
        points: List[MOPDPoint] = []
        for weights in weight_samples:
            wdict = {names[i]: float(weights[i]) for i in range(len(names))}
            scores = dict(base)
            total_gain = 0.0
            for n in names:
                if wdict[n] > 0.2:  # emphasise significant weights
                    gain = wdict[n] * 0.10
                    scores[n] = min(1.0, scores[n] + gain)
                    total_gain += gain
            if total_gain > 0:
                n_others = max(1, len(names) - 1)
                decrease = total_gain / n_others
                for n in names:
                    if wdict[n] <= 0.2:
                        scores[n] = max(0.0, scores[n] - decrease)
            explanation = (
                f"weights={ {k: round(v, 3) for k, v in wdict.items()} }, "
                f"scores={ {k: round(v, 3) for k, v in scores.items()} }"
            )
            points.append(MOPDPoint(
                weights=wdict,
                dimensions=scores,
                explanation=explanation,
            ))

        pareto = filter_pareto(
            points,
            objective_names=names,
            higher_is_better=frozenset(names),  # higher score is better
        )

        best = scalarise_pareto(
            pareto,
            weights=cfg.mopd.objective_weights,
            objective_names=names,
            higher_is_better=frozenset(names),
        )

        for p in pareto:
            self.engine.pareto_front_history.append(p)

        self.obs.counter('mopd_generations_total')
        self.obs.histogram('mopd_pareto_size', len(pareto))
        if best is not None:
            self.obs.gauge('mopd_best_score', best.scalarised_score)

        return pareto


# ============================================================================
# Sub-service: ReportService
# ============================================================================
class ReportService:
    def __init__(self, engine: "UnifiedSustainabilityEngine") -> None:
        self.engine = engine
        self.templates: Dict[str, ReportTemplate] = {}
        self._load_defaults()

    def _load_defaults(self) -> None:
        self.templates['executive_summary'] = ReportTemplate(
            name='executive_summary',
            description='High-level summary for executives',
            included_dimensions=list(DIMENSION_NAMES),
            metrics=['total_score', 'trend', 'risk_factors'],
            format='json', frequency='daily', target_audience='executive',
        )
        self.templates['detailed'] = ReportTemplate(
            name='detailed',
            description='Full breakdown with predictions',
            included_dimensions=list(DIMENSION_NAMES),
            metrics=['score', 'trend', 'prediction', 'volatility', 'scarcity'],
            format='json', frequency='weekly', target_audience='engineers',
        )
        self.templates['standard'] = ReportTemplate(
            name='standard',
            description='Default report',
            included_dimensions=list(DIMENSION_NAMES),
            metrics=['total_score', 'trend', 'dimensions'],
            format='json', frequency='daily', target_audience='general',
        )

    def list_templates(self) -> List[str]:
        return list(self.templates.keys())

    def create_template(self, template: ReportTemplate) -> bool:
        if template.name in self.templates:
            return False
        self.templates[template.name] = template
        return True

    async def generate(
        self,
        template_name: str,
        data: Dict[str, Any],
        output_format: str = "json",
    ) -> Dict[str, Any]:
        template = self.templates.get(template_name)
        if template is None:
            return {'status': 'template_not_found'}
        filtered: Dict[str, Any] = {'template_name': template_name}
        if 'dimensions' in data:
            filtered['dimensions'] = {
                k: v for k, v in data['dimensions'].items()
                if k in template.included_dimensions
            }
        for m in template.metrics:
            if m in data:
                filtered[m] = data[m]
        filtered['timestamp'] = datetime.now(timezone.utc).isoformat()
        return {'status': 'generated', 'report': filtered}


# ============================================================================
# Sub-service: EmissionsService (async-safe SQLite)
# ============================================================================
class EmissionsService:
    def __init__(self, db_path: str = "emissions.db") -> None:
        self.db_path = db_path
        os.makedirs(os.path.dirname(os.path.abspath(db_path)) or '.', exist_ok=True)
        self._lock = asyncio.Lock()
        self._init_db()

    def _init_db(self) -> None:
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS emission_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    source TEXT,
                    amount_kg REAL NOT NULL,
                    metadata TEXT
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS offsets (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    source TEXT,
                    amount_kg REAL NOT NULL,
                    metadata TEXT
                )
            """)
            conn.commit()
        finally:
            conn.close()

    async def record_emission(self, amount_kg: float,
                              source: Optional[str] = None,
                              metadata: Optional[Dict[str, Any]] = None) -> None:
        ts = datetime.now(timezone.utc).isoformat()
        meta_json = json.dumps(metadata or {})

        def _write() -> None:
            conn = sqlite3.connect(self.db_path)
            try:
                conn.execute(
                    "INSERT INTO emission_records (timestamp, source, amount_kg, metadata) "
                    "VALUES (?, ?, ?, ?)",
                    (ts, source, float(amount_kg), meta_json),
                )
                conn.commit()
            finally:
                conn.close()

        async with self._lock:
            await asyncio.to_thread(_write)

    async def record_offset(self, amount_kg: float,
                            source: Optional[str] = None,
                            metadata: Optional[Dict[str, Any]] = None) -> None:
        ts = datetime.now(timezone.utc).isoformat()
        meta_json = json.dumps(metadata or {})

        def _write() -> None:
            conn = sqlite3.connect(self.db_path)
            try:
                conn.execute(
                    "INSERT INTO offsets (timestamp, source, amount_kg, metadata) "
                    "VALUES (?, ?, ?, ?)",
                    (ts, source, float(amount_kg), meta_json),
                )
                conn.commit()
            finally:
                conn.close()

        async with self._lock:
            await asyncio.to_thread(_write)

    async def get_recent_emissions(self, hours: int = 24) -> float:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()

        def _read() -> float:
            conn = sqlite3.connect(self.db_path)
            try:
                cur = conn.execute(
                    "SELECT SUM(amount_kg) FROM emission_records WHERE timestamp >= ?",
                    (cutoff,),
                )
                row = cur.fetchone()
                return float(row[0]) if row and row[0] is not None else 0.0
            finally:
                conn.close()

        async with self._lock:
            return await asyncio.to_thread(_read)


# ============================================================================
# Sub-service: PersistenceService
# ============================================================================
class PersistenceService:
    TIMESTAMP_KEYS = frozenset({
        'timestamp', 'last_update', 'requested_at', 'decided_at',
    })

    def __init__(self, path: str, observability: ObservabilityHub) -> None:
        self.path = path
        self.obs = observability
        self._lock = asyncio.Lock()

    async def save(self, state: Dict[str, Any]) -> bool:
        payload = {
            'engine': ENGINE_NAME,
            'version': ENGINE_VERSION,
            'schema': STATE_SCHEMA_VERSION,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'data': _to_serializable(state),
        }
        async with self._lock:
            try:
                json_str = json.dumps(payload, indent=2)

                def _write() -> None:
                    tmp = f"{self.path}.tmp"
                    with open(tmp, 'w') as f:
                        f.write(json_str)
                    os.replace(tmp, self.path)

                await asyncio.to_thread(_write)
                self.obs.counter('persistence_saves_total', status='ok')
                return True
            except Exception as e:
                logger.error("Persistence save failed: %s", e)
                self.obs.counter('persistence_saves_total', status='error')
                return False

    async def load(self) -> Optional[Dict[str, Any]]:
        async with self._lock:
            if not os.path.exists(self.path):
                return None
            try:
                def _read() -> str:
                    with open(self.path, 'r') as f:
                        return f.read()

                json_str = await asyncio.to_thread(_read)
                payload = json.loads(json_str)
                if not isinstance(payload, dict):
                    return None
                schema = payload.get('schema', 0)
                if schema != STATE_SCHEMA_VERSION:
                    logger.warning(
                        "State schema mismatch: file=%s expected=%s",
                        schema, STATE_SCHEMA_VERSION,
                    )
                data = payload.get('data') or {}
                if not isinstance(data, dict):
                    return None
                self.obs.counter('persistence_loads_total', status='ok')
                return data
            except Exception as e:
                logger.error("Persistence load failed: %s", e)
                self.obs.counter('persistence_loads_total', status='error')
                return None

    async def delete(self) -> bool:
        async with self._lock:
            if os.path.exists(self.path):
                try:
                    await asyncio.to_thread(os.remove, self.path)
                    return True
                except Exception as e:
                    logger.error("Persistence delete failed: %s", e)
                    return False
            return False


# ============================================================================
# Sub-service: SwarmService
# ============================================================================
class SwarmService:
    def __init__(self, engine: "UnifiedSustainabilityEngine") -> None:
        self.engine = engine
        self.obs = engine.observability

    async def share(self) -> None:
        engine = self.engine
        if not engine.config.enable_swarm_coordination or engine.swarm_coordinator is None:
            return
        engine_id = hashlib.md5(
            f"{ENGINE_NAME}:{engine.engine_uuid}".encode(),
        ).hexdigest()[:8]
        payload = {
            'engine_id': engine_id,
            'sustainability_score': engine.sustainability_score,
            'dimension_scores': {k: v.current_value for k, v in engine.dimensions.items()},
            'scarcity_factors': dict(engine.scarcity_factors),
            'history_sample_count': len(engine.history),
            'mopd_enabled': engine.config.mopd.enabled,
        }
        try:
            await engine.swarm_coordinator.share_predictions(payload)
            self.obs.counter('swarm_shares_total', status='ok')
        except Exception as e:
            logger.warning("Swarm share failed: %s", e)
            self.obs.counter('swarm_shares_total', status='error')
            engine._dead_letter('swarm', str(e))


# ============================================================================
# Sub-service: ChaosService (non-destructive)
# ============================================================================
class ChaosService:
    def __init__(self, engine: "UnifiedSustainabilityEngine",
                 config: ChaosConfig) -> None:
        self.engine = engine
        self.config = config
        self._saved: Dict[str, Any] = {}
        self._injected: List[str] = []
        self._lock = asyncio.Lock()

    @property
    def enabled(self) -> bool:
        return bool(self.config.enabled)

    async def inject(self, fault_type: FaultType, **params: Any) -> Dict[str, Any]:
        if not self.enabled:
            return {'status': 'disabled'}
        async with self._lock:
            try:
                if fault_type == FaultType.CARBON_SPIKE:
                    self._saved.setdefault('scarcity_carbon',
                                            self.engine.scarcity_factors.get('carbon', 1.0))
                    self.engine.scarcity_factors['carbon'] = 2.0
                    if self.engine.carbon_manager is not None and \
                            hasattr(self.engine.carbon_manager, 'carbon_intensity'):
                        self._saved.setdefault('carbon_intensity',
                                                self.engine.carbon_manager.carbon_intensity)
                        self.engine.carbon_manager.carbon_intensity = 1000.0
                    self._injected.append(fault_type.value)
                    return {'status': 'injected'}
                if fault_type == FaultType.HELIUM_CRITICAL:
                    self._saved.setdefault('scarcity_helium',
                                            self.engine.scarcity_factors.get('helium', 1.0))
                    self.engine.scarcity_factors['helium'] = 2.0
                    self._injected.append(fault_type.value)
                    return {'status': 'injected'}
                if fault_type == FaultType.PROVIDER_FAILURE:
                    self._saved.setdefault('carbon_manager', self.engine.carbon_manager)
                    self.engine.carbon_manager = None
                    if self.engine.dimension_service is not None:
                        self.engine.dimension_service.engine = self.engine  # no-op
                    self._injected.append(fault_type.value)
                    return {'status': 'injected'}
                if fault_type == FaultType.PERSISTENCE_FAILURE:
                    self._saved.setdefault('persistence_save',
                                            self.engine.persistence.save)

                    async def _fail(*a: Any, **k: Any) -> bool:
                        raise IOError("Simulated persistence failure")

                    self.engine.persistence.save = _fail  # type: ignore
                    self._injected.append(fault_type.value)
                    return {'status': 'injected'}
                if fault_type == FaultType.TELEMETRY_FAILURE:
                    self._saved.setdefault('observability', self.engine.observability)
                    self.engine.observability = ObservabilityHub()  # still works, no-op
                    self._injected.append(fault_type.value)
                    return {'status': 'injected'}
                if fault_type == FaultType.SWARM_FAILURE:
                    self._saved.setdefault('swarm_coordinator',
                                            self.engine.swarm_coordinator)
                    self.engine.swarm_coordinator = None
                    self._injected.append(fault_type.value)
                    return {'status': 'injected'}
            except Exception as e:
                logger.exception("Chaos injection failed: %s", e)
                return {'status': 'error', 'error': str(e)}
            return {'status': 'unknown_fault'}

    def restore(self) -> None:
        eng = self.engine
        for key, value in list(self._saved.items()):
            if key == 'scarcity_carbon':
                eng.scarcity_factors['carbon'] = float(value)
            elif key == 'scarcity_helium':
                eng.scarcity_factors['helium'] = float(value)
            elif key == 'carbon_intensity':
                if eng.carbon_manager is not None:
                    eng.carbon_manager.carbon_intensity = value
            elif key == 'carbon_manager':
                eng.carbon_manager = value
            elif key == 'persistence_save':
                eng.persistence.save = value  # type: ignore
            elif key == 'observability':
                eng.observability = value
            elif key == 'swarm_coordinator':
                eng.swarm_coordinator = value
            self._saved.pop(key, None)
        self._injected.clear()

    async def run_test(self) -> Dict[str, Any]:
        if not self.enabled:
            return {'status': 'disabled'}
        report: Dict[str, Any] = {'faults': [], 'results': {}, 'restored': False}
        faults = [
            FaultType.CARBON_SPIKE,
            FaultType.HELIUM_CRITICAL,
            FaultType.PROVIDER_FAILURE,
            FaultType.PERSISTENCE_FAILURE,
            FaultType.SWARM_FAILURE,
        ]
        try:
            for ft in faults[:CHAOS_MAX_FAULTS_PER_RUN]:
                res = await self.inject(ft)
                report['faults'].append(ft.value)
                report['results'][ft.value] = res

            # Steady-state hypothesis: engine can still produce a score
            try:
                result = await self.engine.update_sustainability_score()
                report['results']['post_fault_score'] = {
                    'success': 'score' in result,
                    'total_score': result.get('score', {}).get('total_score', 0.0)
                    if isinstance(result.get('score'), dict)
                    else getattr(result.get('score'), 'total_score', 0.0),
                }
            except Exception as e:
                report['results']['post_fault_score'] = {
                    'success': False, 'error': str(e),
                }

            # Health check must not crash
            try:
                health = await self.engine.get_health_status()
                report['results']['health_check'] = {
                    'status': health.get('status', 'unknown'),
                }
            except Exception as e:
                report['results']['health_check'] = {'error': str(e)}
        finally:
            if self.config.auto_restore:
                self.restore()
                report['restored'] = True
        return report


# ============================================================================
# Sub-service: ProviderInjection (helpers)
# ============================================================================
class ProviderBundle:
    """Container for injected provider references."""

    def __init__(self) -> None:
        self.carbon_manager: Optional[CarbonProviderProtocol] = None
        self.helium_tracker: Optional[HeliumTrackerProtocol] = None
        self.circular_manager: Optional[CircularManagerProtocol] = None
        self.biodiversity: Optional[BiodiversityProviderProtocol] = None
        self.expert_registry: Optional[ExpertRegistryProtocol] = None
        self.quantum_limits: Optional[QuantumLimitsProtocol] = None
        self.adaptive_cost_function: Optional[Any] = None
        self.swarm_coordinator: Optional[SwarmCoordinatorProtocol] = None
        self.self_healer: Optional[SelfHealerProtocol] = None
        self.workflow_orchestrator: Optional[WorkflowOrchestratorProtocol] = None
        self.event_broker: Optional[EventBrokerProtocol] = None
        self.helium_provider: Optional[Any] = None
        self.gating_network: Optional[Any] = None
        self.self_evolving_gate: Optional[Any] = None
        self.expert_router: Optional[Any] = None


# ============================================================================
# Main engine
# ============================================================================
class UnifiedSustainabilityEngine:
    """Unified Sustainability Valuation Engine v6.0.0."""

    def __init__(
        self,
        bio_core: Optional[Any] = None,
        config: Optional[SustainabilityEngineConfig] = None,
        publish_fn: Optional[Callable[[str, Any], Awaitable[None]]] = None,
        **kwargs: Any,
    ) -> None:
        if config is None:
            valid = {f.name for f in dataclass_fields(SustainabilityEngineConfig)}
            filtered = {k: v for k, v in kwargs.items() if k in valid}
            config = SustainabilityEngineConfig(**filtered)
        self.config = config
        self.bio_core = bio_core
        self.engine_uuid = uuid.uuid4().hex

        # Observability
        self.observability = ObservabilityHub()

        # Governance
        self.governance = GovernanceLayer(
            enabled=config.approval.enabled,
            timeout_seconds=config.approval.timeout_seconds,
            auto_approve_on_timeout=config.approval.auto_approve_on_timeout,
            publish_fn=publish_fn,
        )

        # Safety
        self.safety_monitor = SafetyMonitor()
        self._setup_invariants()

        # Providers (via bundle)
        self.providers = ProviderBundle()
        self._wire_bio_core(bio_core)

        # Sub-services
        self.dimension_service: Optional[DimensionService] = None
        self.threshold_service: Optional[ThresholdService] = None
        self.weight_service: Optional[WeightService] = None
        self.prediction_service: Optional[PredictionService] = None
        self.mopd_service: Optional[MOPDService] = None
        self.report_service: Optional[ReportService] = None
        self.emissions_service: Optional[EmissionsService] = None
        self.persistence: Optional[PersistenceService] = None
        self.swarm_service: Optional[SwarmService] = None
        self.chaos_service: Optional[ChaosService] = None

        # Circuits (owned by engine, used by dimension service)
        self._carbon_circuit = EnhancedCircuitBreaker(
            "carbon_manager",
            failure_threshold=config.circuit_breaker_failure_threshold,
            recovery_timeout=config.circuit_breaker_recovery_timeout,
        )
        self._helium_circuit = EnhancedCircuitBreaker(
            "helium_tracker",
            failure_threshold=config.circuit_breaker_failure_threshold,
            recovery_timeout=config.circuit_breaker_recovery_timeout,
        )
        self._circular_circuit = EnhancedCircuitBreaker(
            "circular_manager",
            failure_threshold=config.circuit_breaker_failure_threshold,
            recovery_timeout=config.circuit_breaker_recovery_timeout,
        )
        self._biodiversity_circuit = EnhancedCircuitBreaker(
            "biodiversity_provider",
            failure_threshold=config.circuit_breaker_failure_threshold,
            recovery_timeout=config.circuit_breaker_recovery_timeout,
        )
        self._expert_circuit = EnhancedCircuitBreaker(
            "expert_registry",
            failure_threshold=config.circuit_breaker_failure_threshold,
            recovery_timeout=config.circuit_breaker_recovery_timeout,
        )
        self._quantum_circuit = EnhancedCircuitBreaker(
            "quantum_limits",
            failure_threshold=config.circuit_breaker_failure_threshold,
            recovery_timeout=config.circuit_breaker_recovery_timeout,
        )

        # State
        self.sustainability_score: float = 0.5
        self.dimensions: Dict[str, SustainabilityDimension] = {}
        self.thresholds: Dict[str, SustainabilityThreshold] = {}
        self.history: Deque[Dict[str, Any]] = deque(maxlen=config.history_limit)
        self.dimension_history: Dict[str, Deque[float]] = defaultdict(
            lambda: deque(maxlen=config.dimension_history_limit),
        )
        self.last_update: Optional[datetime] = None
        self.dimension_weights: Dict[str, float] = dict(config.dimension_weights)
        self.scarcity_factors: Dict[str, float] = dict(DEFAULT_SCARCITY)
        self.pareto_front_history: Deque[MOPDPoint] = deque(
            maxlen=DEFAULT_PARETO_HISTORY_LIMIT,
        )
        self._score_cache: Dict[str, Tuple[float, datetime]] = {}

        # Idempotency
        self._idempotency = IdempotencyCache()

        # Dead letters
        self._dead_letters: Deque[Dict[str, Any]] = deque(maxlen=MAX_DEAD_LETTERS)

        # Locks
        self._lock = asyncio.Lock()

        # Lifecycle
        self._started = False
        self._shutting_down = False
        self._background_tasks: List[asyncio.Task] = []
        self._persistence_task: Optional[asyncio.Task] = None
        self._swarm_task: Optional[asyncio.Task] = None
        self._chaos_task: Optional[asyncio.Task] = None

        # Initialize thresholds structure
        self._init_thresholds()

        # Subscribe to events after providers are wired
        if config.enable_event_driven and self.providers.event_broker is not None:
            self._subscribe_events()

        logger.info("UnifiedSustainabilityEngine v%s initialised (not started)",
                    ENGINE_VERSION)

    # ------------------------------------------------------------------
    # Wiring
    # ------------------------------------------------------------------
    def _wire_bio_core(self, bio_core: Optional[Any]) -> None:
        if bio_core is None:
            return
        p = self.providers
        p.event_broker = getattr(bio_core, 'event_broker', None)
        p.swarm_coordinator = getattr(bio_core, 'swarm_coordinator', None)
        p.self_healer = getattr(bio_core, 'self_healer', None)
        p.workflow_orchestrator = getattr(bio_core, 'workflow_orchestrator', None)
        p.helium_provider = getattr(bio_core, 'helium_provider', None)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def start(self) -> None:
        if self._started:
            return
        self._started = True

        # Instantiate sub-services that depend on engine state
        self.persistence = PersistenceService(self.config.persistence_path, self.observability)
        self.emissions_service = EmissionsService()
        self.threshold_service = ThresholdService(self)
        self.weight_service = WeightService(self)
        self.prediction_service = PredictionService(self)
        self.mopd_service = MOPDService(self)
        self.report_service = ReportService(self)
        self.swarm_service = SwarmService(self)
        self.dimension_service = DimensionService(self)
        self.chaos_service = ChaosService(self, self.config.chaos)

        await self._load_state()
        self._start_background_tasks()
        logger.info("UnifiedSustainabilityEngine started")

    async def wait_ready(self) -> None:
        if not self._started:
            await self.start()

    async def shutdown(self) -> None:
        logger.info("Shutting down UnifiedSustainabilityEngine")
        self._shutting_down = True
        self._started = False

        for t in self._background_tasks:
            t.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)
        self._background_tasks.clear()

        try:
            await self.save_state()
        except Exception as e:
            logger.error("Final save failed: %s", e)

        if self.safety_monitor is not None:
            unmet = self.safety_monitor.final_check()
            if unmet:
                logger.warning("Unmet eventual invariants at shutdown: %s", unmet)

        logger.info("Shutdown complete")

    def _start_background_tasks(self) -> None:
        if self._background_tasks:
            return
        if self.config.telemetry_export_interval > 0:
            self._persistence_task = asyncio.create_task(self._persistence_loop())
            self._background_tasks.append(self._persistence_task)
        if self.config.enable_swarm_coordination and self.providers.swarm_coordinator is not None:
            self._swarm_task = asyncio.create_task(self._swarm_loop())
            self._background_tasks.append(self._swarm_task)
        if self.config.enable_chaos_testing and self.config.chaos.enabled:
            self._chaos_task = asyncio.create_task(self._chaos_loop())
            self._background_tasks.append(self._chaos_task)

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

    async def _swarm_loop(self) -> None:
        interval = max(60, self.config.swarm_share_interval)
        while not self._shutting_down:
            try:
                await asyncio.sleep(interval)
                await self.share_with_swarm()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Swarm loop error: %s", e)

    async def _chaos_loop(self) -> None:
        interval = max(60, self.config.chaos.interval_seconds)
        while not self._shutting_down:
            try:
                await asyncio.sleep(interval)
                if self.chaos_service is not None:
                    await self.chaos_service.run_test()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Chaos loop error: %s", e)

    # ------------------------------------------------------------------
    # Event subscription
    # ------------------------------------------------------------------
    def _subscribe_events(self) -> None:
        eb = self.providers.event_broker
        if eb is None:
            return
        try:
            eb.subscribe('carbon_update', self._on_carbon_update)
            eb.subscribe('helium_update', self._on_helium_update)
            eb.subscribe('alert_generated', self._on_alert_generated)
            eb.subscribe('config_updated', self._on_config_updated)
            eb.subscribe('token_balance_update', self._on_token_update)
            eb.subscribe('health_update', self._on_health_update)
            eb.subscribe('anomaly_detected', self._on_anomaly_detected)
            logger.info("Sustainability Engine subscribed to core events")
        except Exception as e:
            logger.warning("Event subscription failed: %s", e)

    async def _on_carbon_update(self, event: Any) -> None:
        intensity = float(event.data.get('intensity', 400.0))
        self.scarcity_factors['carbon'] = max(1.0, min(2.0, intensity / 500.0))
        self._score_cache.pop('carbon', None)

    async def _on_helium_update(self, event: Any) -> None:
        scarcity = float(event.data.get('scarcity', 0.5))
        self.scarcity_factors['helium'] = max(1.0, min(2.0, 2.0 - (1.0 - scarcity) * 2.0))
        self._score_cache.pop('helium', None)

    async def _on_alert_generated(self, event: Any) -> None:
        if event.data.get('severity') == 'critical':
            logger.warning("Critical alert; triggering conservative mode")
            self.config.adaptation_rate = min(self.config.adaptation_rate, 0.05)
            if self.config.enable_self_healing and self.providers.self_healer is not None:
                try:
                    await self.providers.self_healer.apply_healing('damage_accumulation')
                except Exception as e:
                    logger.error("Self-healing failed: %s", e)
            if (self.providers.workflow_orchestrator is not None and
                    self.config.workflow_on_critical_alert):
                try:
                    await self.providers.workflow_orchestrator.execute_workflow(
                        self.config.workflow_on_critical_alert,
                    )
                except Exception as e:
                    logger.error("Workflow failed: %s", e)

    async def _on_config_updated(self, event: Any) -> None:
        updates = event.data.get('updates', {})
        if 'sustainability_engine' in updates:
            new = updates['sustainability_engine']
            for k, v in new.items():
                if hasattr(self.config, k):
                    setattr(self.config, k, v)
            logger.info("Configuration reloaded")

    async def _on_token_update(self, event: Any) -> None:
        pass

    async def _on_health_update(self, event: Any) -> None:
        pass

    async def _on_anomaly_detected(self, event: Any) -> None:
        metric = event.data.get('metric')
        if metric == 'carbon_intensity':
            self.dimension_weights['carbon'] = min(
                0.5, self.dimension_weights.get('carbon', 0.25) * 1.2,
            )
        elif metric == 'helium_scarcity':
            self.dimension_weights['helium'] = min(
                0.5, self.dimension_weights.get('helium', 0.20) * 1.2,
            )
        # Renormalise — invariant requires sum == 1
        total = sum(self.dimension_weights.values())
        if total > 0:
            for k in self.dimension_weights:
                self.dimension_weights[k] /= total

    # ------------------------------------------------------------------
    # Safety invariants
    # ------------------------------------------------------------------
    def _setup_invariants(self) -> None:
        def _weights_sum_one(state: Dict[str, Any]) -> bool:
            w = state.get('dimension_weights', {})
            if not w:
                return True
            return abs(sum(w.values()) - 1.0) < 0.01

        def _dims_bounded(state: Dict[str, Any]) -> bool:
            for _, v in (state.get('dimensions') or {}).items():
                val = v.current_value if hasattr(v, 'current_value') else float(v)
                if not (0.0 <= val <= 1.0):
                    return False
            return True

        def _scarcity_nonneg(state: Dict[str, Any]) -> bool:
            for v in (state.get('scarcity_factors') or {}).values():
                if float(v) < 0.0:
                    return False
            return True

        self.safety_monitor.add('weights_sum_one', _weights_sum_one,
                                'Dimension weights must sum to 1')
        self.safety_monitor.add('dims_bounded', _dims_bounded,
                                'Dimension values must be in [0, 1]')
        self.safety_monitor.add('scarcity_nonneg', _scarcity_nonneg,
                                'Scarcity factors must be non-negative')

    def _init_thresholds(self) -> None:
        for name in DIMENSION_NAMES:
            self.thresholds[name] = SustainabilityThreshold(
                dimension=name,
                warning_threshold=self.config.warning_threshold,
                critical_threshold=self.config.critical_threshold,
                adaptive_warning=self.config.warning_threshold,
                adaptive_critical=self.config.critical_threshold,
            )

    # ------------------------------------------------------------------
    # Public API (backward compatible)
    # ------------------------------------------------------------------
    async def update_sustainability_score(
        self,
        region: Optional[str] = None,
        return_mopd: bool = False,
        idempotency_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        await self.wait_ready()
        if self._idempotency.seen(idempotency_key):
            logger.debug("Duplicate update ignored")
            return {'status': 'duplicate', 'idempotency_key': idempotency_key}

        region = region or self.config.default_region
        t0 = time.monotonic()

        assert self.dimension_service is not None
        assert self.threshold_service is not None
        assert self.weight_service is not None
        assert self.prediction_service is not None

        dims = await self.dimension_service.collect(region)
        await self.threshold_service.update_all(dims)
        weights = await self.weight_service.adjust(dims)
        await self.prediction_service.update_all(dims)

        total = self._aggregate_score(dims, weights)
        self._record_history(total, dims, weights)

        risk_factors = self._assess_risks(dims)
        recommendations = self._generate_recommendations(dims, total)
        scenario_scores = await self._compute_scenarios(dims)
        predicted_total = self._compute_predicted_total(dims)
        await self._update_external_systems(total, dims)

        self.sustainability_score = float(total)
        self.last_update = datetime.now(timezone.utc)
        elapsed = time.monotonic() - t0
        self.observability.gauge('sustainability_total_score', float(total))
        self.observability.histogram('sustainability_update_latency_s', elapsed)

        # Build score result
        score = UnifiedSustainabilityScore(
            total_score=float(total),
            dimensions={k: v.to_dict() for k, v in dims.items()},
            confidence=float(np.mean([d.confidence for d in dims.values()])) if dims else 0.5,
            trend=self._calculate_global_trend(),
            risk_factors=risk_factors,
            recommendations=recommendations,
            predicted_future_score=predicted_total,
            scenario_scores=scenario_scores,
            report_template='standard',
        )

        # Check safety invariants
        if self.config.enable_temporal_safety:
            violations = self.safety_monitor.check({
                'dimension_weights': self.dimension_weights,
                'dimensions': dims,
                'scarcity_factors': self.scarcity_factors,
            })
            if violations:
                logger.warning("Safety violations: %s", violations)
                self.observability.counter('safety_violations_total')

        # MOPD Pareto front (real)
        pareto: Optional[List[MOPDPoint]] = None
        best: Optional[MOPDPoint] = None
        if return_mopd and self.config.mopd.enabled:
            assert self.mopd_service is not None
            pareto = await self.mopd_service.generate(dims)
            if pareto:
                # Already stamped by scalarise_pareto inside MOPDService
                best = max(pareto, key=lambda p: p.scalarised_score)

        # Persist the score itself in the history
        return {
            'score': score,
            'pareto_front': [p.to_dict() for p in pareto] if pareto else None,
            'best_plan': best.to_dict() if best else None,
        }

    async def get_current_score(self) -> float:
        await self.wait_ready()
        return self.sustainability_score

    async def get_dimension_status(self) -> Dict[str, str]:
        await self.wait_ready()
        status: Dict[str, str] = {}
        for name, t in self.thresholds.items():
            if t.current_value < t.adaptive_critical:
                status[name] = "critical"
            elif t.current_value < t.adaptive_warning:
                status[name] = "warning"
            else:
                status[name] = "healthy"
        return status

    async def get_historical_scores(self, n: int = 100) -> List[Dict[str, Any]]:
        await self.wait_ready()
        return list(self.history)[-n:]

    async def get_dimension_predictions(self) -> Dict[str, Any]:
        await self.wait_ready()
        assert self.prediction_service is not None
        predictions: Dict[str, Any] = {}
        for name, hist in self.dimension_history.items():
            if len(hist) > 3:
                pred, conf, vol = await self.prediction_service.predict(name, 10)
                predictions[name] = {
                    'prediction': pred,
                    'confidence': conf,
                    'volatility': vol,
                    'sample_size': len(hist),
                }
        return predictions

    async def get_sustainability_report(
        self,
        template_name: str = "executive_summary",
        output_format: str = "json",
    ) -> Dict[str, Any]:
        await self.wait_ready()
        result = await self.update_sustainability_score()
        score: UnifiedSustainabilityScore = result['score']
        status = await self.get_dimension_status()
        predictions = await self.get_dimension_predictions()

        report_data: Dict[str, Any] = {
            'total_score': score.total_score,
            'trend': score.trend,
            'dimensions': {
                name: {
                    'value': dim['current_value'],
                    'weight': dim['weight'],
                    'trend': dim['trend'],
                    'status': status.get(name, 'unknown'),
                    'scarcity_factor': dim['scarcity_factor'],
                    'prediction': dim.get('prediction', 0.0),
                    'prediction_confidence': dim.get('prediction_confidence', 0.0),
                    'volatility': dim.get('volatility', 0.0),
                }
                for name, dim in score.dimensions.items()
            },
            'risk_factors': score.risk_factors,
            'recommendations': score.recommendations,
            'predictions': predictions,
            'history': await self.get_historical_scores(10),
            'weight_trends': self.weight_service.get_trends() if self.weight_service else {},
            'threshold_stats': {
                name: self.threshold_service.get_stats(name)
                for name in self.thresholds
            } if self.threshold_service else {},
        }

        assert self.report_service is not None
        report = await self.report_service.generate(template_name, report_data, output_format)
        if report.get('status') == 'generated':
            report['data'] = report_data
            return report
        return report_data

    async def update_scarcity_factors(self, new_factors: Dict[str, float]) -> None:
        await self.wait_ready()
        for dim, factor in new_factors.items():
            if dim not in DIMENSION_NAMES:
                continue
            f = float(factor)
            if f < 0.0:
                logger.warning("Negative scarcity factor ignored: %s", f)
                continue
            self.scarcity_factors[dim] = min(2.0, f)
        logger.info("Updated scarcity factors: %s", new_factors)

    def get_available_templates(self) -> List[str]:
        if self.report_service is None:
            return []
        return self.report_service.list_templates()

    async def create_custom_template(self, template: ReportTemplate) -> bool:
        await self.wait_ready()
        assert self.report_service is not None
        return self.report_service.create_template(template)

    # ------------------------------------------------------------------
    # MOPD public methods
    # ------------------------------------------------------------------
    async def get_mopd_pareto_front(
        self,
        region: Optional[str] = None,
        num_samples: int = 50,
    ) -> List[MOPDPoint]:
        await self.wait_ready()
        if not self.config.mopd.enabled:
            return []
        assert self.dimension_service is not None
        assert self.mopd_service is not None
        dims = await self.dimension_service.collect(
            region or self.config.default_region,
        )
        return await self.mopd_service.generate(dims, num_samples=num_samples)

    async def get_mopd_summary(self) -> Dict[str, Any]:
        await self.wait_ready()
        if not self.config.mopd.enabled:
            return {"enabled": False}
        return {
            "enabled": True,
            "objective_weights": dict(self.config.mopd.objective_weights),
            "grid_resolution": int(self.config.mopd.grid_resolution),
            "pareto_front_history_size": len(self.pareto_front_history),
            "dimensions": list(self.dimension_weights.keys()),
            "xai_enabled": self.config.enable_xai,
            "temporal_safety_enabled": self.config.enable_temporal_safety,
            "human_approval_enabled": self.config.approval.enabled,
            "chaos_testing_enabled": self.config.enable_chaos_testing,
        }

    # ------------------------------------------------------------------
    # Emissions / offsets
    # ------------------------------------------------------------------
    async def get_recent_emissions(self, hours: int = 24) -> float:
        await self.wait_ready()
        assert self.emissions_service is not None
        return await self.emissions_service.get_recent_emissions(hours)

    async def record_offset(self, kg: float, source: Optional[str] = None) -> None:
        await self.wait_ready()
        if kg < 0:
            raise ValueError("Offset kg must be >= 0")
        assert self.emissions_service is not None
        await self.emissions_service.record_offset(kg, source=source)

    async def record_emission(self, kg: float, source: Optional[str] = None) -> None:
        await self.wait_ready()
        if kg < 0:
            raise ValueError("Emission kg must be >= 0")
        assert self.emissions_service is not None
        await self.emissions_service.record_emission(kg, source=source)

    # ------------------------------------------------------------------
    # Config reload
    # ------------------------------------------------------------------
    async def reload_config(self, new_config: SustainabilityEngineConfig) -> None:
        await self.wait_ready()
        self.config = new_config
        self.dimension_weights = dict(new_config.dimension_weights)
        self._init_thresholds()
        if self.threshold_service is not None:
            self.threshold_service.history.clear()
        if self.weight_service is not None:
            self.weight_service.history.clear()
        logger.info("Configuration reloaded")

    # ------------------------------------------------------------------
    # Approval / chaos / swarm public API
    # ------------------------------------------------------------------
    async def request_approval(self, new_weights: Dict[str, float]) -> bool:
        await self.wait_ready()
        if not self.config.approval.enabled:
            return True
        max_change = 0.0
        for d in new_weights:
            old = self.dimension_weights.get(d, 0.0)
            max_change = max(max_change, abs(float(new_weights[d]) - old))
        if max_change < self.config.approval.aggressive_weight_change_threshold:
            return True
        result = await self.governance.request_approval({
            'action': 'weight_change',
            'max_change': max_change,
            'new_weights': new_weights,
        })
        return result.status in (ApprovalStatus.APPROVED, ApprovalStatus.AUTO_APPROVED)

    async def submit_approval_decision(self, request_id: str, approved: bool) -> bool:
        return await self.governance.submit_decision(request_id, approved)

    def get_approval_audit(self) -> List[Dict[str, Any]]:
        return self.governance.get_audit_log()

    async def inject_fault(self, fault_type: str, **params: Any) -> Dict[str, Any]:
        await self.wait_ready()
        if self.chaos_service is None:
            return {'status': 'disabled'}
        try:
            ft = FaultType(fault_type)
        except ValueError:
            logger.warning("Unknown fault type: %s", fault_type)
            return {'status': 'unknown_fault'}
        return await self.chaos_service.inject(ft, **params)

    async def run_chaos_test(self) -> Dict[str, Any]:
        await self.wait_ready()
        if self.chaos_service is None:
            return {'status': 'disabled'}
        return await self.chaos_service.run_test()

    async def share_with_swarm(self) -> None:
        await self.wait_ready()
        if self.swarm_service is None:
            return
        await self.swarm_service.share()

    # ------------------------------------------------------------------
    # Self-healing
    # ------------------------------------------------------------------
    async def self_heal(self) -> None:
        await self.wait_ready()
        logger.info("SustainabilityEngine self-healing")
        if not self.config.enable_self_healing:
            return
        self.dimension_weights = dict(self.config.dimension_weights)
        self.scarcity_factors = dict(DEFAULT_SCARCITY)
        self.config.adaptation_rate = 0.1
        if len(self.history) > 10:
            self.history = deque(list(self.history)[-10:],
                                maxlen=self.config.history_limit)
        self._score_cache.clear()
        for threshold in self.thresholds.values():
            threshold.adaptive_warning = threshold.warning_threshold
            threshold.adaptive_critical = threshold.critical_threshold
            threshold.alert_count = 0
        await self.save_state()
        logger.info("Self-healing completed")

    # ------------------------------------------------------------------
    # Health
    # ------------------------------------------------------------------
    async def get_health_status(self) -> Dict[str, Any]:
        return {
            'status': 'healthy',
            'engine_version': ENGINE_VERSION,
            'state_schema': STATE_SCHEMA_VERSION,
            'started': self._started,
            'shutting_down': self._shutting_down,
            'sustainability_score': self.sustainability_score,
            'dimension_count': len(self.dimensions),
            'history_samples': len(self.history),
            'bio_integration_active': self.bio_core is not None,
            'event_driven_active': self.config.enable_event_driven,
            'self_healing_enabled': self.config.enable_self_healing,
            'swarm_coordination_active': self.config.enable_swarm_coordination,
            'persistence_enabled': self.persistence is not None,
            'mopd_enabled': self.config.mopd.enabled,
            'pareto_front_history_size': len(self.pareto_front_history),
            'xai_enabled': self.config.enable_xai,
            'temporal_safety_enabled': self.config.enable_temporal_safety,
            'human_approval_enabled': self.config.approval.enabled,
            'chaos_testing_enabled': self.config.enable_chaos_testing,
            'federated_enabled': self.config.federated.enabled,
            'dead_letters': len(self._dead_letters),
            'governance_pending': len(self.governance.pending),
        }

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------
    async def save_state(self) -> bool:
        if self.persistence is None:
            return False
        state = {
            'sustainability_score': self.sustainability_score,
            'history': list(self.history),
            'dimension_history': {k: list(v) for k, v in self.dimension_history.items()},
            'thresholds': {k: t.to_dict() for k, t in self.thresholds.items()},
            'scarcity_factors': dict(self.scarcity_factors),
            'dimension_weights': dict(self.dimension_weights),
            'last_update': self.last_update.isoformat() if self.last_update else None,
            'pareto_front_history': [p.to_dict() for p in self.pareto_front_history],
        }
        return await self.persistence.save(state)

    async def _load_state(self) -> bool:
        if self.persistence is None:
            return False
        state = await self.persistence.load()
        if not state:
            return False
        try:
            self.sustainability_score = float(state.get('sustainability_score', 0.5))
            for entry in state.get('history', []):
                self.history.append(entry)
            for k, v in (state.get('dimension_history') or {}).items():
                self.dimension_history[k] = deque(
                    v, maxlen=self.config.dimension_history_limit,
                )
            for k, t_data in (state.get('thresholds') or {}).items():
                if k in self.thresholds:
                    t = self.thresholds[k]
                    for attr, val in t_data.items():
                        if hasattr(t, attr):
                            setattr(t, attr, val)
            self.scarcity_factors.update(state.get('scarcity_factors') or {})
            w = state.get('dimension_weights')
            if isinstance(w, dict) and w:
                self.dimension_weights = {k: float(v) for k, v in w.items()}
            last = state.get('last_update')
            if last:
                try:
                    self.last_update = datetime.fromisoformat(last)
                except ValueError:
                    self.last_update = None
            for p_data in state.get('pareto_front_history', []):
                try:
                    self.pareto_front_history.append(MOPDPoint.from_dict(p_data))
                except Exception as e:
                    logger.debug("Skipping invalid pareto entry: %s", e)
            logger.info("State loaded from %s", self.persistence.path)
            return True
        except Exception as e:
            logger.error("State load parse error: %s", e)
            return False

    async def delete_state(self) -> bool:
        if self.persistence is None:
            return False
        return await self.persistence.delete()

    # ------------------------------------------------------------------
    # Internal helpers — implementing previously-missing methods
    # ------------------------------------------------------------------
    def _aggregate_score(
        self,
        dimensions: Dict[str, SustainabilityDimension],
        weights: Dict[str, float],
    ) -> float:
        total = 0.0
        total_weight = 0.0
        for name, dim in dimensions.items():
            w = float(weights.get(name, 0.0))
            total += w * float(dim.current_value)
            total_weight += w
        if total_weight <= 0:
            return 0.5
        return max(0.0, min(1.0, total / total_weight))

    def _record_history(
        self,
        total: float,
        dimensions: Dict[str, SustainabilityDimension],
        weights: Dict[str, float],
    ) -> None:
        self.history.append({
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'total_score': float(total),
            'dimension_scores': {k: float(v.current_value) for k, v in dimensions.items()},
            'weights': {k: float(v) for k, v in weights.items()},
        })

    def _assess_risks(
        self, dimensions: Dict[str, SustainabilityDimension],
    ) -> List[str]:
        risks: List[str] = []
        for name, dim in dimensions.items():
            if dim.current_value < self.config.critical_threshold:
                risks.append(f"{name} below critical threshold")
            elif dim.current_value < self.config.warning_threshold:
                risks.append(f"{name} below warning threshold")
            if dim.volatility > 0.15:
                risks.append(f"{name} highly volatile")
        return risks

    def _generate_recommendations(
        self, dimensions: Dict[str, SustainabilityDimension], total: float,
    ) -> List[str]:
        recs: List[str] = []
        for name, dim in dimensions.items():
            if dim.current_value < self.config.warning_threshold:
                recs.append(
                    f"Improve {name}: currently {dim.current_value:.2f}, "
                    f"target above {dim.target_value:.2f}"
                )
        if total < 0.4:
            recs.append("Overall sustainability below acceptable level")
        elif total > 0.8:
            recs.append("Maintain current best practices")
        return recs

    async def _compute_scenarios(
        self, dimensions: Dict[str, SustainabilityDimension],
    ) -> Dict[str, float]:
        base = self._aggregate_score(dimensions, self.dimension_weights)
        return {
            'optimistic': min(1.0, base * 1.1),
            'pessimistic': max(0.0, base * 0.9),
            'base': base,
        }

    async def _update_external_systems(
        self,
        total: float,
        dimensions: Dict[str, SustainabilityDimension],
    ) -> None:
        if self.providers.quantum_limits is None:
            return
        try:
            await self._quantum_circuit.call(
                self.providers.quantum_limits.update_sustainability_limits,
                total,
                {k: v.current_value for k, v in dimensions.items()},
            )
        except Exception as e:
            logger.warning("External system update failed: %s", e)

    def _calculate_global_trend(self) -> str:
        hist = list(self.history)
        if len(hist) < 5:
            return "stable"
        recent = [h['total_score'] for h in hist[-5:]]
        older = [h['total_score'] for h in hist[-10:-5]] if len(hist) >= 10 else recent
        r_avg = float(np.mean(recent))
        o_avg = float(np.mean(older))
        if r_avg > o_avg * 1.02:
            return "improving"
        if r_avg < o_avg * 0.98:
            return "declining"
        return "stable"

    def _compute_predicted_total(
        self, dimensions: Dict[str, SustainabilityDimension],
    ) -> float:
        preds = []
        weights = self.dimension_weights
        for name, dim in dimensions.items():
            preds.append(weights.get(name, 0.0) * float(dim.prediction or dim.current_value))
        return max(0.0, min(1.0, float(sum(preds))))

    def _dead_letter(self, topic: str, error: str) -> None:
        self._dead_letters.append({
            'topic': topic,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'error': str(error),
        })

    def get_dead_letters(self) -> List[Dict[str, Any]]:
        return list(self._dead_letters)

    # ------------------------------------------------------------------
    # Injection API (backward compatible)
    # ------------------------------------------------------------------
    def inject_modules(self, **modules: Any) -> None:
        for name, module in modules.items():
            setattr(self, name, module)
            if hasattr(self.providers, name):
                setattr(self.providers, name, module)
            logger.info("Injected module: %s", name)

    def set_gating_network(self, gating_network: Any) -> None:
        self.providers.gating_network = gating_network
        logger.info("Gating network injected")

    def set_self_evolving_gate(self, gate: Any) -> None:
        self.providers.self_evolving_gate = gate
        logger.info("Self-evolving gate injected")

    def set_expert_router(self, router: Any) -> None:
        self.providers.expert_router = router
        logger.info("Expert router injected")

    def set_helium_provider(self, provider: Any) -> None:
        self.providers.helium_provider = provider
        logger.info("Helium provider injected")

    def set_adaptive_cost_function(self, cost_func: Any) -> None:
        self.providers.adaptive_cost_function = cost_func
        logger.info("Adaptive cost function injected")

    def inject_bio_core(self, bio_core: Optional[Any] = None, **kwargs: Any) -> None:
        if bio_core is not None:
            self.bio_core = bio_core
            self._wire_bio_core(bio_core)
        for name, value in kwargs.items():
            if hasattr(self.providers, name):
                setattr(self.providers, name, value)
        logger.info("Bio-inspired core injected")

    # ------------------------------------------------------------------
    # Convenience accessors (for backward compat with direct attribute use)
    # ------------------------------------------------------------------
    @property
    def carbon_manager(self) -> Optional[CarbonProviderProtocol]:
        return self.providers.carbon_manager

    @carbon_manager.setter
    def carbon_manager(self, value: Optional[CarbonProviderProtocol]) -> None:
        self.providers.carbon_manager = value

    @property
    def helium_tracker(self) -> Optional[HeliumTrackerProtocol]:
        return self.providers.helium_tracker

    @helium_tracker.setter
    def helium_tracker(self, value: Optional[HeliumTrackerProtocol]) -> None:
        self.providers.helium_tracker = value

    @property
    def circular_manager(self) -> Optional[CircularManagerProtocol]:
        return self.providers.circular_manager

    @circular_manager.setter
    def circular_manager(self, value: Optional[CircularManagerProtocol]) -> None:
        self.providers.circular_manager = value

    @property
    def biodiversity(self) -> Optional[BiodiversityProviderProtocol]:
        return self.providers.biodiversity

    @biodiversity.setter
    def biodiversity(self, value: Optional[BiodiversityProviderProtocol]) -> None:
        self.providers.biodiversity = value

    @property
    def expert_registry(self) -> Optional[ExpertRegistryProtocol]:
        return self.providers.expert_registry

    @expert_registry.setter
    def expert_registry(self, value: Optional[ExpertRegistryProtocol]) -> None:
        self.providers.expert_registry = value

    @property
    def quantum_limits(self) -> Optional[QuantumLimitsProtocol]:
        return self.providers.quantum_limits

    @quantum_limits.setter
    def quantum_limits(self, value: Optional[QuantumLimitsProtocol]) -> None:
        self.providers.quantum_limits = value

    @property
    def swarm_coordinator(self) -> Optional[SwarmCoordinatorProtocol]:
        return self.providers.swarm_coordinator

    @swarm_coordinator.setter
    def swarm_coordinator(self, value: Optional[SwarmCoordinatorProtocol]) -> None:
        self.providers.swarm_coordinator = value

    @property
    def self_healer(self) -> Optional[SelfHealerProtocol]:
        return self.providers.self_healer

    @self_healer.setter
    def self_healer(self, value: Optional[SelfHealerProtocol]) -> None:
        self.providers.self_healer = value

    @property
    def workflow_orchestrator(self) -> Optional[WorkflowOrchestratorProtocol]:
        return self.providers.workflow_orchestrator

    @workflow_orchestrator.setter
    def workflow_orchestrator(self, value: Optional[WorkflowOrchestratorProtocol]) -> None:
        self.providers.workflow_orchestrator = value

    @property
    def event_broker(self) -> Optional[EventBrokerProtocol]:
        return self.providers.event_broker

    @event_broker.setter
    def event_broker(self, value: Optional[EventBrokerProtocol]) -> None:
        self.providers.event_broker = value

    @property
    def adaptive_cost_function(self) -> Optional[Any]:
        return self.providers.adaptive_cost_function

    @adaptive_cost_function.setter
    def adaptive_cost_function(self, value: Optional[Any]) -> None:
        self.providers.adaptive_cost_function = value

    # ------------------------------------------------------------------
    # Federated (stub, but DP-aware)
    # ------------------------------------------------------------------
    async def participate_in_federation(self) -> None:
        if not self.config.federated.enabled:
            return
        # Minimal DP-aware stub: emit a noisy aggregated payload
        eps = max(1e-6, self.config.federated.privacy_epsilon)
        delta = max(1e-12, self.config.federated.privacy_delta)
        clip = self.config.federated.privacy_clip_norm
        sigma = math.sqrt(2.0 * math.log(1.25 / delta)) / eps

        payload = {
            'sustainability_score': self.sustainability_score,
            'feedback_count': len(self.history),
        }
        for k, v in list(payload.items()):
            if isinstance(v, (int, float)):
                payload[k] = float(v) + float(np.random.normal(0.0, sigma * clip))
        logger.info("Federated stub payload (DP noised): %s", payload)


# ============================================================================
# FastAPI surface (optional)
# ============================================================================
if FASTAPI_AVAILABLE:
    _engine_ref: Dict[str, Optional[UnifiedSustainabilityEngine]] = {'engine': None}

    @asynccontextmanager
    async def _lifespan(app: FastAPI):  # type: ignore
        engine = UnifiedSustainabilityEngine(config=SustainabilityEngineConfig())
        await engine.start()
        _engine_ref['engine'] = engine
        logger.info("FastAPI startup complete")
        try:
            yield
        finally:
            await engine.shutdown()
            _engine_ref['engine'] = None
            logger.info("FastAPI shutdown complete")

    app = FastAPI(title="Sustainability Engine API", version=ENGINE_VERSION,
                  lifespan=_lifespan)

    def _require_engine() -> UnifiedSustainabilityEngine:
        eng = _engine_ref.get('engine')
        if eng is None:
            raise HTTPException(status_code=503, detail="Engine not initialized")
        return eng

    @app.get("/health")
    async def health() -> Dict[str, Any]:
        return await _require_engine().get_health_status()

    @app.get("/score")
    async def score() -> Dict[str, float]:
        eng = _require_engine()
        return {"score": await eng.get_current_score()}

    @app.post("/update")
    async def update(background_tasks: BackgroundTasks,
                     region: Optional[str] = None) -> Dict[str, str]:
        eng = _require_engine()
        background_tasks.add_task(eng.update_sustainability_score, region)
        return {"status": "update started"}

    @app.get("/report")
    async def report(template: str = "executive_summary",
                     format: str = "json") -> Dict[str, Any]:
        return await _require_engine().get_sustainability_report(template, format)

    @app.get("/dimensions")
    async def dimensions() -> Dict[str, Any]:
        eng = _require_engine()
        return {
            name: {
                "value": d.current_value,
                "weight": d.weight,
                "trend": d.trend,
                "scarcity": d.scarcity_factor,
            }
            for name, d in eng.dimensions.items()
        }

    @app.get("/mopd/pareto")
    async def mopd_pareto(region: Optional[str] = None,
                          num_samples: int = 50) -> Dict[str, Any]:
        pareto = await _require_engine().get_mopd_pareto_front(region, num_samples)
        return {"pareto_front": [p.to_dict() for p in pareto]}

    @app.get("/mopd/summary")
    async def mopd_summary() -> Dict[str, Any]:
        return await _require_engine().get_mopd_summary()

    @app.post("/chaos/run")
    async def chaos() -> Dict[str, Any]:
        return await _require_engine().run_chaos_test()

    @app.post("/self-heal")
    async def self_heal() -> Dict[str, str]:
        await _require_engine().self_heal()
        return {"status": "self-heal triggered"}

    @app.get("/prometheus")
    async def prometheus() -> Response:
        eng = _require_engine()
        body = await eng.observability.export_prometheus()
        return Response(content=body, media_type="text/plain")


# ============================================================================
# Exports
# ============================================================================
__all__ = [
    # Engine
    "UnifiedSustainabilityEngine",
    "SustainabilityEngineConfig",
    "MOPDConfig",
    "FederatedSustainabilityConfig",
    "ApprovalConfig",
    "ChaosConfig",
    # Sub-services
    "DimensionService",
    "ThresholdService",
    "WeightService",
    "PredictionService",
    "MOPDService",
    "ReportService",
    "EmissionsService",
    "PersistenceService",
    "SwarmService",
    "ChaosService",
    # Infra
    "ObservabilityHub",
    "GovernanceLayer",
    "ApprovalRequest",
    "ApprovalResult",
    "ApprovalStatus",
    "ChaosConfig",
    "FaultType",
    "SafetyInvariant",
    "SafetyMonitor",
    "EnhancedCircuitBreaker",
    "CircuitBreakerState",
    "IdempotencyCache",
    "ProviderBundle",
    # Data
    "SustainabilityDimension",
    "SustainabilityThreshold",
    "UnifiedSustainabilityScore",
    "MOPDPoint",
    "ReportTemplate",
    # Utilities
    "scalarise_pareto",
    "filter_pareto",
    "retry_async",
    # Constants
    "ENGINE_VERSION",
    "STATE_SCHEMA_VERSION",
    "DIMENSION_NAMES",
    "DEFAULT_WEIGHTS",
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

    async def main() -> None:
        cfg = SustainabilityEngineConfig()
        engine = UnifiedSustainabilityEngine(config=cfg)
        await engine.start()

        result = await engine.update_sustainability_score(return_mopd=True)
        score = result['score']
        print(f"Total score: {score.total_score:.4f}")
        print(f"Trend: {score.trend}")
        if result['pareto_front']:
            print(f"Pareto front size: {len(result['pareto_front'])}")
            for p in result['pareto_front'][:3]:
                print(f"  {p['explanation']}")

        health = await engine.get_health_status()
        print(f"Health: {health['status']}")

        chaos = await engine.run_chaos_test()
        print(f"Chaos restored: {chaos.get('restored')}")

        metrics = await engine.observability.export_prometheus()
        print(metrics[:400])

        await engine.shutdown()

    asyncio.run(main())
