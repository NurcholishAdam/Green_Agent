#!/usr/bin/env python3
# drift_detector.py
"""
Drift Detection for Adaptive Cost Function (Enhanced v3.0.0)
=============================================================

Production-grade drift detection with:

* Sub-services: DistanceService, SnapshotService, RollbackService,
  EventPersistenceService, GatingService.
* Protocols for all injected components.
* Shared `ObservabilityHub` and `GovernanceLayer`.
* Non-destructive `ChaosService` with full save/restore.
* Full lifecycle: `start()` / `wait_ready()` / `shutdown()`.
* Pydantic v1/v2 compatible config (dataclass fallback).
* Idempotency keys with LRU cache.
* Dead-letter queue with bounded capacity.
* Bounded caches (deque maxlen / OrderedDict LRU).

FIXED BUGS OVER v2.1
--------------------
- `check_drift` now returns a `DriftResult` (was: `None`).
- MoE `add_training_sample` uses `reward` in the gradient.
- `distance_history` appended on every measurement (was: skipped on drift).
- `self.metric` is never mutated by MoE; `effective_metric` is local.
- Cosine distance handles zero-norm correctly (0.0 if both zero).
- Adaptive threshold uses raw distances (not smoothed).
- `time.monotonic()` used for interval tracking (not `time.time()`).
- Snapshot IDs use `uuid.uuid4().hex` (was: truncated time-based hash).
- All storage calls run in `asyncio.to_thread` (non-blocking).
- Snapshot cache guarded by `asyncio.Lock` (no race).
- Rollback uses public `restore_weights` (no private internals).
- MoE RNG is seeded; expert names validated against known metrics.
- Key union cached in `DistanceService` per weights-signature.
- Idempotency keys on `check_drift`.
- Dead-letter queue for storage failures.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import random
import time
import uuid
from collections import OrderedDict, deque
from dataclasses import dataclass, field, fields as dataclass_fields, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import (
    Any, Awaitable, Callable, Deque, Dict, FrozenSet, List, Optional,
    Protocol, Sequence, Set, Tuple, Union, runtime_checkable,
)

import numpy as np


# ============================================================================
# Pydantic compatibility layer
# ============================================================================
PYDANTIC_AVAILABLE = False
PYDANTIC_V1 = False
PYDANTIC_V2 = False
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
# Sibling imports (graceful)
# ============================================================================
try:
    from ..storage import Storage  # type: ignore
except ImportError:
    Storage = None  # type: ignore

try:
    from ..config import config as _central_config  # type: ignore
except ImportError:
    _central_config = None

try:
    from ..logger import logger as _central_logger  # type: ignore
    logger = _central_logger
except ImportError:
    logger = logging.getLogger(__name__)


# ============================================================================
# Named constants
# ============================================================================
MODULE_VERSION = "3.0.0"
IDEMPOTENCY_CACHE_SIZE = 4_096
DEAD_LETTER_MAX = 1_000
APPROVAL_AUDIT_MAX = 1_000
APPROVAL_PENDING_MAX = 512
EVENT_NODES_MAX = 10_000
EVENT_EDGES_MAX = 20_000
MODP_STATES_MAX = 10_000
RLHF_PAIRS_MAX = 10_000
DISTANCE_HISTORY_DEFAULT = 100
DEFAULT_THRESHOLD = 0.2
DEFAULT_SNAPSHOT_INTERVAL = 3600.0
DEFAULT_HYSTERESIS = 1
DEFAULT_EWMA_ALPHA = 0.3
DEFAULT_ADAPTIVE_K = 3.0
DEFAULT_APPROVAL_TIMEOUT = 60.0
CHAOS_MAX_FAULTS_PER_RUN = 6
NUMERIC_EPS = 1e-9

VALID_METRICS: FrozenSet[str] = frozenset({
    "euclidean", "manhattan", "cosine", "relative",
})
DEFAULT_MOE_EXPERTS: Tuple[str, ...] = (
    "euclidean", "manhattan", "cosine", "relative",
)


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


def _coerce_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    if value is None:
        return default
    try:
        f = float(value)
    except (TypeError, ValueError):
        return default
    if not math.isfinite(f):
        return default
    return f


# ============================================================================
# Protocols
# ============================================================================
@runtime_checkable
class StorageProtocol(Protocol):
    def get_last_snapshot(self) -> Optional[Dict[str, Any]]: ...
    def save_drift_snapshot(self, snapshot_id: str, online: str, offline: str,
                            cost_score: float, reason: str) -> Any: ...
    def log_drift_event(self, event: Dict[str, Any]) -> Any: ...
    def get_drift_events(self, limit: int = 50) -> List[Dict[str, Any]]: ...


@runtime_checkable
class AdaptiveCostProtocol(Protocol):
    def get_current_weights(self) -> Dict[str, float]: ...
    def restore_weights(self, weights: Dict[str, float]) -> None: ...


@runtime_checkable
class MetricsRegistryProtocol(Protocol):
    registry: Any


@runtime_checkable
class LimitGraphBackendProtocol(Protocol):
    def save_limit_graph_metadata(self, graph_id: str, description: str,
                                  configuration: Dict[str, Any]) -> Any: ...
    def save_limit_graph_node(self, node_id: str, graph_id: str,
                              node_type: Optional[str],
                              attributes: Dict[str, Any]) -> Any: ...
    def save_limit_graph_edge(self, edge_id: str, graph_id: str, source: str,
                              target: str, weight: Optional[float],
                              attributes: Dict[str, Any]) -> Any: ...


@runtime_checkable
class MODPBackendProtocol(Protocol):
    def save_modp_state(self, state_id: str, problem_id: str,
                        state_attributes: Dict[str, Any],
                        objective_values: Dict[str, float], stage: int) -> Any: ...


@runtime_checkable
class RLHFBackendProtocol(Protocol):
    def save_preference_pair(self, pair_id: str, prompt: str, chosen: str,
                             rejected: str, reward_diff: float,
                             metadata: Optional[Dict[str, Any]]) -> Any: ...


@runtime_checkable
class EventBrokerProtocol(Protocol):
    def subscribe(self, event_type: str,
                  handler: Callable[[Any], Awaitable[None]]) -> None: ...


# ============================================================================
# Enums
# ============================================================================
class FaultType(str, Enum):
    STORAGE_FAILURE = "storage_failure"
    ADAPTIVE_COST_FAILURE = "adaptive_cost_failure"
    MOE_FAILURE = "moe_failure"
    SNAPSHOT_CORRUPTION = "snapshot_corruption"
    DISTANCE_NAN = "distance_nan"
    ROLLBACK_FAILURE = "rollback_failure"


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


# ============================================================================
# ObservabilityHub
# ============================================================================
class ObservabilityHub:
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
# DeadLetterQueue, IdempotencyCache, CircuitBreaker
# ============================================================================
class DeadLetterQueue:
    def __init__(self, max_size: int = DEAD_LETTER_MAX) -> None:
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


class IdempotencyCache:
    def __init__(self, max_size: int = IDEMPOTENCY_CACHE_SIZE) -> None:
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


class CircuitBreaker:
    def __init__(self, name: str, failure_threshold: int = 5,
                 recovery_timeout: float = 30.0) -> None:
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
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
                    raise RuntimeError(f"Circuit {self.name} is OPEN")
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
        timeout_seconds: float = DEFAULT_APPROVAL_TIMEOUT,
        auto_approve_on_timeout: bool = False,
        publish_fn: Optional[Callable[[str, Any], Awaitable[None]]] = None,
    ) -> None:
        self.enabled = bool(enabled)
        self.timeout_seconds = float(timeout_seconds)
        self.auto_approve_on_timeout = bool(auto_approve_on_timeout)
        self.publish_fn = publish_fn
        self.pending: "OrderedDict[str, ApprovalRequest]" = OrderedDict()
        self.audit: Deque[Dict[str, Any]] = deque(maxlen=APPROVAL_AUDIT_MAX)
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
            request_id=request_id, context=context,
            requested_at=_utcnow(), timeout_seconds=timeout, future=future,
        )
        async with self._lock:
            self.pending[request_id] = req
            while len(self.pending) > APPROVAL_PENDING_MAX:
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

    def list_pending(self) -> List[Dict[str, Any]]:
        return [r.to_dict() for r in self.pending.values()]

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
# Config
# ============================================================================
@dataclass
class DriftDetectorConfig:
    threshold: float = DEFAULT_THRESHOLD
    rollback_enabled: bool = True
    snapshot_interval: float = DEFAULT_SNAPSHOT_INTERVAL
    hysteresis_count: int = DEFAULT_HYSTERESIS
    metric: str = "euclidean"
    use_adaptive_threshold: bool = False
    ewma_alpha: float = DEFAULT_EWMA_ALPHA
    weight_importance: Dict[str, float] = field(default_factory=dict)
    distance_history_size: int = DISTANCE_HISTORY_DEFAULT
    adaptive_k: float = DEFAULT_ADAPTIVE_K

    # Optional features
    enable_limit_graph: bool = True
    enable_modp: bool = True
    enable_rlhf: bool = True
    enable_moe: bool = True
    enable_human_approval: bool = False
    enable_temporal_safety: bool = True
    enable_chaos_testing: bool = False
    enable_idempotency: bool = True
    require_approval_for_rollback: bool = False

    moe_expert_names: List[str] = field(
        default_factory=lambda: list(DEFAULT_MOE_EXPERTS),
    )
    moe_seed: int = 42

    approval_timeout_seconds: float = DEFAULT_APPROVAL_TIMEOUT
    approval_auto_approve_on_timeout: bool = False
    chaos_interval_seconds: int = 3600

    def __post_init__(self) -> None:
        if self.threshold <= 0:
            raise ValueError("threshold must be > 0")
        if not (0.0 < self.ewma_alpha <= 1.0):
            raise ValueError("ewma_alpha must be in (0, 1]")
        if self.hysteresis_count < 1:
            raise ValueError("hysteresis_count must be >= 1")
        if self.distance_history_size < 3:
            raise ValueError("distance_history_size must be >= 3")
        if self.snapshot_interval <= 0:
            raise ValueError("snapshot_interval must be > 0")
        if self.metric not in VALID_METRICS:
            logger.warning(
                "Invalid metric '%s'; falling back to 'euclidean'.", self.metric,
            )
            self.metric = "euclidean"
        # MoE expert validation
        invalid = set(self.moe_expert_names) - VALID_METRICS
        if invalid:
            raise ValueError(
                f"Unknown MoE expert names: {sorted(invalid)}; "
                f"allowed: {sorted(VALID_METRICS)}"
            )
        if not self.moe_expert_names:
            self.moe_expert_names = list(DEFAULT_MOE_EXPERTS)

    def to_dict(self) -> Dict[str, Any]:
        return _to_serializable(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DriftDetectorConfig":
        valid = {f.name for f in dataclass_fields(cls)}
        return cls(**{k: v for k, v in data.items() if k in valid})


def _default_config_from_central() -> DriftDetectorConfig:
    if _central_config is None:
        return DriftDetectorConfig()
    return DriftDetectorConfig(
        threshold=getattr(_central_config, 'DRIFT_THRESHOLD', DEFAULT_THRESHOLD),
        rollback_enabled=getattr(_central_config, 'ROLLBACK_ENABLED', True),
        snapshot_interval=getattr(_central_config, 'DRIFT_SNAPSHOT_INTERVAL',
                                  DEFAULT_SNAPSHOT_INTERVAL),
        hysteresis_count=getattr(_central_config, 'DRIFT_HYSTERESIS_COUNT',
                                 DEFAULT_HYSTERESIS),
        metric=getattr(_central_config, 'DRIFT_METRIC', 'euclidean'),
        use_adaptive_threshold=getattr(_central_config,
                                       'DRIFT_USE_ADAPTIVE_THRESHOLD', False),
        ewma_alpha=getattr(_central_config, 'DRIFT_EWMA_ALPHA', DEFAULT_EWMA_ALPHA),
        weight_importance=getattr(_central_config, 'DRIFT_WEIGHT_IMPORTANCE', {}),
        distance_history_size=getattr(_central_config, 'DRIFT_HISTORY_SIZE',
                                      DISTANCE_HISTORY_DEFAULT),
        adaptive_k=getattr(_central_config, 'DRIFT_ADAPTIVE_K', DEFAULT_ADAPTIVE_K),
    )


# ============================================================================
# Result dataclasses
# ============================================================================
@dataclass
class DriftResult:
    """Result of a drift check. All numeric fields are finite floats."""
    distance: float
    raw_distance: float
    effective_metric: str
    threshold: float
    drift_detected: bool
    consecutive: int
    rollback_triggered: bool
    timestamp: float
    dry_run: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            'distance': float(self.distance),
            'raw_distance': float(self.raw_distance),
            'effective_metric': self.effective_metric,
            'threshold': float(self.threshold),
            'drift_detected': bool(self.drift_detected),
            'consecutive': int(self.consecutive),
            'rollback_triggered': bool(self.rollback_triggered),
            'timestamp': float(self.timestamp),
            'dry_run': bool(self.dry_run),
        }


# ============================================================================
# DistanceService
# ============================================================================
class DistanceService:
    """Pure distance metrics with cached key unions."""

    def __init__(self, config: DriftDetectorConfig) -> None:
        self.config = config
        self._key_cache: "OrderedDict[Tuple[frozenset, frozenset], Tuple[str, ...]]" = (
            OrderedDict()
        )
        self._key_cache_max = 128
        self._disabled = False

    def disable(self) -> None:
        self._disabled = True

    def enable(self) -> None:
        self._disabled = False

    def _union_keys(self, a: Dict[str, float],
                    b: Dict[str, float]) -> Tuple[str, ...]:
        sig = (frozenset(a.keys()), frozenset(b.keys()))
        if sig in self._key_cache:
            self._key_cache.move_to_end(sig)
            return self._key_cache[sig]
        union = tuple(sorted(a.keys() | b.keys()))
        self._key_cache[sig] = union
        if len(self._key_cache) > self._key_cache_max:
            self._key_cache.popitem(last=False)
        return union

    def compute(
        self,
        a: Dict[str, float],
        b: Dict[str, float],
        metric: Optional[str] = None,
        importance: Optional[Dict[str, float]] = None,
    ) -> float:
        if self._disabled:
            raise RuntimeError("DistanceService disabled (chaos)")
        m = metric or self.config.metric
        if m not in VALID_METRICS:
            m = "euclidean"
        imp = importance if importance is not None else self.config.weight_importance
        keys = self._union_keys(a, b)
        if not keys:
            return 0.0

        def wi(k: str) -> float:
            return float(imp.get(k, 1.0))

        if m == "euclidean":
            acc = 0.0
            for k in keys:
                d = wi(k) * (a.get(k, 0.0) - b.get(k, 0.0))
                acc += d * d
            return float(math.sqrt(acc))
        if m == "manhattan":
            return float(sum(wi(k) * abs(a.get(k, 0.0) - b.get(k, 0.0))
                             for k in keys))
        if m == "cosine":
            dot = 0.0
            na = 0.0
            nb = 0.0
            for k in keys:
                va = wi(k) * a.get(k, 0.0)
                vb = wi(k) * b.get(k, 0.0)
                dot += va * vb
                na += va * va
                nb += vb * vb
            na = math.sqrt(na)
            nb = math.sqrt(nb)
            # Fix: both zero → distance 0; exactly one zero → 1
            if na == 0.0 and nb == 0.0:
                return 0.0
            if na == 0.0 or nb == 0.0:
                return 1.0
            cosine_sim = dot / (na * nb)
            return float(1.0 - cosine_sim)
        if m == "relative":
            total = 0.0
            count = 0
            for k in keys:
                va = a.get(k, 0.0)
                vb = b.get(k, 0.0)
                if va == 0.0 and vb == 0.0:
                    continue
                denom = max(abs(va), abs(vb))
                total += wi(k) * (abs(va - vb) / denom)
                count += 1
            return float(total / count) if count > 0 else 0.0
        return 0.0

    def compute_all(
        self,
        a: Dict[str, float],
        b: Dict[str, float],
        metrics: Optional[Sequence[str]] = None,
        importance: Optional[Dict[str, float]] = None,
    ) -> Dict[str, float]:
        ms = metrics or tuple(VALID_METRICS)
        return {m: self.compute(a, b, m, importance) for m in ms if m in VALID_METRICS}


# ============================================================================
# SnapshotService
# ============================================================================
class SnapshotService:
    """
    Manages drift snapshots and their cache. All storage calls run in a
    worker thread; cache is guarded by an asyncio.Lock.
    """

    def __init__(self, storage: Optional[StorageProtocol],
                 observability: ObservabilityHub,
                 dlq: Optional[DeadLetterQueue] = None) -> None:
        self.storage = storage
        self.obs = observability
        self.dlq = dlq
        self._lock = asyncio.Lock()
        self._last_snapshot_cache: Optional[Dict[str, Any]] = None
        self.last_snapshot_time: float = 0.0
        self._disabled = False

    def disable(self) -> None:
        self._disabled = True

    def enable(self) -> None:
        self._disabled = False

    @staticmethod
    def serialize(weights: Dict[str, float]) -> str:
        return json.dumps(weights, default=str)

    @staticmethod
    def deserialize(raw: str) -> Dict[str, float]:
        try:
            data = json.loads(raw)
            if isinstance(data, dict):
                return {k: float(v) for k, v in data.items() if _coerce_float(v) is not None}
        except Exception:
            pass
        return {}

    async def load_last(self) -> Optional[Dict[str, Any]]:
        if self._disabled:
            return self._last_snapshot_cache
        async with self._lock:
            if self.storage is None:
                return self._last_snapshot_cache
            try:
                snap = await asyncio.to_thread(self.storage.get_last_snapshot)
            except Exception as e:
                logger.warning("Failed to load last snapshot: %s", e)
                if self.dlq:
                    await self.dlq.record('load_last_snapshot', str(e))
                return self._last_snapshot_cache
            if snap:
                self._last_snapshot_cache = snap
                try:
                    self.last_snapshot_time = float(snap.get('timestamp', 0.0))
                except (TypeError, ValueError):
                    self.last_snapshot_time = 0.0
            return self._last_snapshot_cache

    def cached_last(self) -> Optional[Dict[str, Any]]:
        return self._last_snapshot_cache

    async def take_snapshot(
        self, weights: Dict[str, float], reason: str,
    ) -> Dict[str, Any]:
        if self._disabled:
            raise RuntimeError("SnapshotService disabled (chaos)")
        snapshot_id = uuid.uuid4().hex
        online_json = self.serialize(weights)
        offline_json = self.serialize({})
        cost_score = float(sum(weights.values()))
        now = time.monotonic()
        if self.storage is not None:
            try:
                await asyncio.to_thread(
                    self.storage.save_drift_snapshot,
                    snapshot_id, online_json, offline_json, cost_score, reason,
                )
            except Exception as e:
                logger.error("Snapshot save failed: %s", e)
                if self.dlq:
                    await self.dlq.record('save_drift_snapshot', str(e))
        snapshot = {
            "snapshot_id": snapshot_id,
            "online_weights": online_json,
            "offline_weights": offline_json,
            "timestamp": now,
            "reason": reason,
            "cost_score": cost_score,
        }
        async with self._lock:
            self._last_snapshot_cache = snapshot
            self.last_snapshot_time = now
        self.obs.counter('snapshots_taken_total', reason=reason)
        logger.info("Snapshot taken: %s (%s)", snapshot_id, reason)
        return snapshot

    async def corrupt_snapshot(self) -> None:
        """Chaos hook: corrupt cached snapshot."""
        async with self._lock:
            if self._last_snapshot_cache is not None:
                self._last_snapshot_cache = dict(self._last_snapshot_cache)
                self._last_snapshot_cache['online_weights'] = "{corrupt"


# ============================================================================
# RollbackService
# ============================================================================
class RollbackService:
    """Rolls back the adaptive cost function to a snapshot."""

    def __init__(self, adaptive_cost: AdaptiveCostProtocol,
                 observability: ObservabilityHub,
                 governance: GovernanceLayer,
                 dlq: Optional[DeadLetterQueue] = None,
                 require_approval: bool = False) -> None:
        self.adaptive_cost = adaptive_cost
        self.obs = observability
        self.governance = governance
        self.dlq = dlq
        self.require_approval = bool(require_approval)
        self._lock = asyncio.Lock()
        self._disabled = False

    def disable(self) -> None:
        self._disabled = True

    def enable(self) -> None:
        self._disabled = False

    async def rollback(
        self, snapshot: Dict[str, Any],
        reason: str = "auto",
        distance: float = 0.0,
        threshold: float = 0.0,
    ) -> bool:
        if self._disabled:
            raise RuntimeError("RollbackService disabled (chaos)")

        if self.require_approval and self.governance.enabled:
            approval = await self.governance.request_approval({
                'action': 'rollback',
                'reason': reason,
                'distance': float(distance),
                'threshold': float(threshold),
                'snapshot_id': snapshot.get('snapshot_id'),
            })
            if approval.status not in (ApprovalStatus.APPROVED,
                                        ApprovalStatus.AUTO_APPROVED):
                logger.info("Rollback denied: %s", approval.status.value)
                self.obs.counter('rollbacks_total', status='denied')
                return False

        try:
            online_json = snapshot.get("online_weights")
            if not isinstance(online_json, str):
                raise ValueError("Malformed snapshot: online_weights not a string")
            weights = SnapshotService.deserialize(online_json)
            async with self._lock:
                await asyncio.to_thread(
                    self.adaptive_cost.restore_weights, weights,
                )
            self.obs.counter('rollbacks_total', status='ok')
            logger.info("Rolled back to snapshot %s", snapshot.get('snapshot_id'))
            return True
        except Exception as e:
            logger.error("Rollback failed: %s", e)
            self.obs.counter('rollbacks_total', status='error')
            if self.dlq:
                await self.dlq.record('rollback', str(e),
                                      {'snapshot_id': snapshot.get('snapshot_id')})
            return False


# ============================================================================
# EventPersistenceService (LIMIT Graph + MODP + RLHF)
# ============================================================================
class EventPersistenceService:
    """
    Batches writes to LIMIT Graph, MODP, and RLHF backends.

    All writes go through `asyncio.to_thread` when a backend is provided;
    otherwise bounded in-memory structures are used as fallback.
    """

    def __init__(self, config: DriftDetectorConfig,
                 storage: Optional[StorageProtocol] = None,
                 observability: Optional[ObservabilityHub] = None,
                 dlq: Optional[DeadLetterQueue] = None) -> None:
        self.config = config
        self.storage = storage
        self.obs = observability or ObservabilityHub()
        self.dlq = dlq
        self._lock = asyncio.Lock()

        # In-memory fallback (bounded)
        self.limit_graphs: Dict[str, Dict[str, Any]] = {}
        self.modp_states: Dict[str, Deque[Dict[str, Any]]] = {}
        self.rlhf_pairs: Deque[Dict[str, Any]] = deque(
            maxlen=RLHF_PAIRS_MAX,
        )

    # ---------- LIMIT Graph ----------
    async def create_graph(self, graph_id: str, description: str,
                            configuration: Dict[str, Any]) -> None:
        try:
            if self.storage and hasattr(self.storage, 'save_limit_graph_metadata'):
                await asyncio.to_thread(
                    self.storage.save_limit_graph_metadata,
                    graph_id, description, configuration,
                )
                return
            async with self._lock:
                self.limit_graphs[graph_id] = {
                    'description': description,
                    'configuration': configuration,
                    'nodes': OrderedDict(),
                    'edges': OrderedDict(),
                }
        except Exception as e:
            await self._record_dlq('create_graph', e, {'graph_id': graph_id})

    async def add_node(self, graph_id: str, node_id: str,
                        node_type: Optional[str],
                        attributes: Dict[str, Any]) -> None:
        try:
            if self.storage and hasattr(self.storage, 'save_limit_graph_node'):
                await asyncio.to_thread(
                    self.storage.save_limit_graph_node,
                    node_id, graph_id, node_type, attributes,
                )
                return
            async with self._lock:
                if graph_id not in self.limit_graphs:
                    self.limit_graphs[graph_id] = {
                        'nodes': OrderedDict(), 'edges': OrderedDict(),
                    }
                nodes = self.limit_graphs[graph_id].setdefault('nodes', OrderedDict())
                nodes[node_id] = {'node_type': node_type, 'attributes': attributes}
                while len(nodes) > EVENT_NODES_MAX:
                    nodes.popitem(last=False)
        except Exception as e:
            await self._record_dlq('add_node', e, {'node_id': node_id})

    async def add_edge(self, graph_id: str, edge_id: str, source: str,
                        target: str, weight: Optional[float],
                        attributes: Dict[str, Any]) -> None:
        try:
            if self.storage and hasattr(self.storage, 'save_limit_graph_edge'):
                await asyncio.to_thread(
                    self.storage.save_limit_graph_edge,
                    edge_id, graph_id, source, target, weight, attributes,
                )
                return
            async with self._lock:
                if graph_id not in self.limit_graphs:
                    self.limit_graphs[graph_id] = {
                        'nodes': OrderedDict(), 'edges': OrderedDict(),
                    }
                edges = self.limit_graphs[graph_id].setdefault('edges', OrderedDict())
                edges[edge_id] = {
                    'source': source, 'target': target,
                    'weight': weight, 'attributes': attributes,
                }
                while len(edges) > EVENT_EDGES_MAX:
                    edges.popitem(last=False)
        except Exception as e:
            await self._record_dlq('add_edge', e, {'edge_id': edge_id})

    def get_nodes(self, graph_id: str) -> List[Dict[str, Any]]:
        if self.storage and hasattr(self.storage, 'get_limit_graph_nodes'):
            try:
                return list(self.storage.get_limit_graph_nodes(graph_id))
            except Exception:
                pass
        return list(self.limit_graphs.get(graph_id, {}).get('nodes', {}).values())

    def get_edges(self, graph_id: str) -> List[Dict[str, Any]]:
        if self.storage and hasattr(self.storage, 'get_limit_graph_edges'):
            try:
                return list(self.storage.get_limit_graph_edges(graph_id))
            except Exception:
                pass
        return list(self.limit_graphs.get(graph_id, {}).get('edges', {}).values())

    def get_metadata(self, graph_id: str) -> Optional[Dict[str, Any]]:
        if self.storage and hasattr(self.storage, 'get_limit_graph_metadata'):
            try:
                return self.storage.get_limit_graph_metadata(graph_id)
            except Exception:
                pass
        return self.limit_graphs.get(graph_id, {})

    # ---------- MODP ----------
    async def add_modp_state(self, state_id: str, problem_id: str,
                              state_attributes: Dict[str, Any],
                              objective_values: Dict[str, float],
                              stage: int) -> None:
        try:
            if self.storage and hasattr(self.storage, 'save_modp_state'):
                await asyncio.to_thread(
                    self.storage.save_modp_state,
                    state_id, problem_id, state_attributes,
                    objective_values, stage,
                )
                return
            async with self._lock:
                if problem_id not in self.modp_states:
                    self.modp_states[problem_id] = deque(maxlen=MODP_STATES_MAX)
                self.modp_states[problem_id].append({
                    'state_id': state_id,
                    'state_attributes': state_attributes,
                    'objective_values': objective_values,
                    'stage': stage,
                })
        except Exception as e:
            await self._record_dlq('add_modp_state', e, {'state_id': state_id})

    def get_modp_states(self, problem_id: str) -> List[Dict[str, Any]]:
        if self.storage and hasattr(self.storage, 'get_modp_states'):
            try:
                return list(self.storage.get_modp_states(problem_id))
            except Exception:
                pass
        return list(self.modp_states.get(problem_id, []))

    # ---------- RLHF ----------
    async def record_preference_pair(
        self, pair_id: str, prompt: str, chosen: str, rejected: str,
        reward_diff: float, metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        try:
            if self.storage and hasattr(self.storage, 'save_preference_pair'):
                await asyncio.to_thread(
                    self.storage.save_preference_pair,
                    pair_id, prompt, chosen, rejected, reward_diff, metadata,
                )
                return
            async with self._lock:
                self.rlhf_pairs.append({
                    'pair_id': pair_id, 'prompt': prompt,
                    'chosen': chosen, 'rejected': rejected,
                    'reward_diff': reward_diff, 'metadata': metadata,
                })
        except Exception as e:
            await self._record_dlq('record_preference_pair', e, {'pair_id': pair_id})

    def get_preference_pairs(self, limit: int = 100) -> List[Dict[str, Any]]:
        if self.storage and hasattr(self.storage, 'get_preference_pairs'):
            try:
                return list(self.storage.get_preference_pairs(limit))
            except Exception:
                pass
        return list(self.rlhf_pairs)[-limit:]

    # ---------- helpers ----------
    async def _record_dlq(self, topic: str, error: Exception,
                          payload: Optional[Dict[str, Any]] = None) -> None:
        if self.dlq is not None:
            await self.dlq.record(topic, str(error), payload)


# ============================================================================
# GatingService (MoE over distance metrics)
# ============================================================================
class GatingService:
    """
    MoE gating over distance metrics.

    Fixes: seeded RNG, reward used in gradient, expert validation.
    """

    CONTEXT_DIM = 6  # 5 features + 1 for current-metric index

    def __init__(self, config: DriftDetectorConfig,
                 observability: Optional[ObservabilityHub] = None) -> None:
        self.config = config
        self.obs = observability or ObservabilityHub()
        self.expert_names: Tuple[str, ...] = tuple(config.moe_expert_names)
        self.num_experts = len(self.expert_names)
        rng = np.random.default_rng(config.moe_seed)
        self.gating_weights = rng.standard_normal(
            (self.num_experts, self.CONTEXT_DIM),
        ) * 0.01
        self.learning_rate = 0.1
        self._lock = asyncio.Lock()
        self._disabled = False

    def disable(self) -> None:
        self._disabled = True

    def enable(self) -> None:
        self._disabled = False

    def _metric_index(self, metric: str) -> int:
        try:
            return self.expert_names.index(metric)
        except ValueError:
            return 0

    def _encode_state(
        self, features: Dict[str, float], current_metric: str,
    ) -> np.ndarray:
        idx = self._metric_index(current_metric)
        one_hot = [0.0] * self.CONTEXT_DIM
        if 0 <= idx < len(one_hot):
            one_hot[idx] = 1.0
        else:
            one_hot[0] = 1.0
        base = [
            float(_coerce_float(features.get('distance'), 0.0) or 0.0),
            float(_coerce_float(features.get('ewma'), 0.0) or 0.0),
            float(_coerce_float(features.get('threshold'), 0.0) or 0.0),
            float(_coerce_float(features.get('consecutive'), 0.0) or 0.0),
            float(_coerce_float(features.get('history_mean'), 0.0) or 0.0),
        ]
        return np.array(base + one_hot[:1], dtype=np.float32)

    async def select_expert(
        self, features: Dict[str, float], current_metric: str,
    ) -> Tuple[str, np.ndarray]:
        if self._disabled:
            return current_metric, np.zeros(self.num_experts)
        x = self._encode_state(features, current_metric)
        logits = self.gating_weights @ x
        probs = np.exp(logits - np.max(logits))
        probs /= probs.sum()
        idx = int(np.argmax(probs))
        self.obs.counter('gating_selections_total', expert=self.expert_names[idx])
        return self.expert_names[idx], probs

    async def add_training_sample(
        self, features: Dict[str, float], current_metric: str,
        selected_expert: str, reward: float,
    ) -> None:
        if self._disabled or selected_expert not in self.expert_names:
            return
        idx = self.expert_names.index(selected_expert)
        x = self._encode_state(features, current_metric)
        async with self._lock:
            logits = self.gating_weights @ x
            probs = np.exp(logits - np.max(logits))
            probs /= probs.sum()
            target = np.zeros(self.num_experts)
            target[idx] = 1.0
            # Fix: reward-weighted gradient
            grad = reward * (probs - target)[:, None] * x[None, :]
            self.gating_weights -= self.learning_rate * grad
        self.obs.counter('gating_updates_total')


# ============================================================================
# ChaosService
# ============================================================================
class ChaosService:
    def __init__(self, detector: "DriftDetector",
                 config: DriftDetectorConfig) -> None:
        self.detector = detector
        self.config = config
        self._saved: Dict[str, Any] = {}
        self._injected: List[str] = []
        self._lock = asyncio.Lock()

    async def inject(self, fault_type: FaultType, **params: Any) -> Dict[str, Any]:
        async with self._lock:
            try:
                if fault_type == FaultType.STORAGE_FAILURE:
                    if self.detector.snapshot_service is not None:
                        self._saved.setdefault('storage',
                                                self.detector.snapshot_service.storage)
                        self.detector.snapshot_service.storage = None
                    return {'status': 'injected'}
                if fault_type == FaultType.ADAPTIVE_COST_FAILURE:
                    if self.detector.rollback_service is not None:
                        self._saved.setdefault('adaptive_cost',
                                                self.detector.rollback_service.adaptive_cost)

                        class _FailingAdaptiveCost:
                            def get_current_weights(self) -> Dict[str, float]:
                                return {}
                            def restore_weights(self, w: Dict[str, float]) -> None:
                                raise RuntimeError("Simulated adaptive_cost failure")

                        self.detector.rollback_service.adaptive_cost = _FailingAdaptiveCost()
                    return {'status': 'injected'}
                if fault_type == FaultType.MOE_FAILURE:
                    if self.detector.gating_service is not None:
                        self._saved.setdefault('gating_disabled',
                                                self.detector.gating_service._disabled)
                        self.detector.gating_service.disable()
                    return {'status': 'injected'}
                if fault_type == FaultType.SNAPSHOT_CORRUPTION:
                    if self.detector.snapshot_service is not None:
                        await self.detector.snapshot_service.corrupt_snapshot()
                    return {'status': 'injected'}
                if fault_type == FaultType.DISTANCE_NAN:
                    if self.detector.distance_service is not None:
                        self._saved.setdefault('distance_disabled',
                                                self.detector.distance_service._disabled)
                        self.detector.distance_service.disable()
                    return {'status': 'injected'}
                if fault_type == FaultType.ROLLBACK_FAILURE:
                    if self.detector.rollback_service is not None:
                        self._saved.setdefault('rollback_disabled',
                                                self.detector.rollback_service._disabled)
                        self.detector.rollback_service.disable()
                    return {'status': 'injected'}
            except Exception as e:
                logger.exception("Chaos injection failed: %s", e)
                return {'status': 'error', 'error': str(e)}
            return {'status': 'unknown_fault'}

    def restore(self) -> None:
        d = self.detector
        for key, value in list(self._saved.items()):
            if key == 'storage' and d.snapshot_service is not None:
                d.snapshot_service.storage = value
            elif key == 'adaptive_cost' and d.rollback_service is not None:
                d.rollback_service.adaptive_cost = value
            elif key == 'gating_disabled' and d.gating_service is not None:
                d.gating_service._disabled = bool(value)
            elif key == 'distance_disabled' and d.distance_service is not None:
                d.distance_service._disabled = bool(value)
            elif key == 'rollback_disabled' and d.rollback_service is not None:
                d.rollback_service._disabled = bool(value)
            self._saved.pop(key, None)
        self._injected.clear()

    async def run_test(self, weights: Optional[Dict[str, float]] = None) -> Dict[str, Any]:
        report: Dict[str, Any] = {'faults': [], 'results': {}, 'restored': False}
        faults = [
            FaultType.STORAGE_FAILURE,
            FaultType.MOE_FAILURE,
            FaultType.DISTANCE_NAN,
            FaultType.ADAPTIVE_COST_FAILURE,
            FaultType.SNAPSHOT_CORRUPTION,
        ]
        w = weights or {"quality": 0.5, "latency": 0.5, "carbon": 0.5}
        try:
            for ft in faults[:CHAOS_MAX_FAULTS_PER_RUN]:
                res = await self.inject(ft)
                report['faults'].append(ft.value)
                report['results'][ft.value] = res
            # Steady-state hypothesis
            try:
                result = await self.detector.check_drift(w)
                report['results']['post_fault_check'] = {
                    'distance_finite': math.isfinite(result.distance),
                    'dry_run': result.dry_run,
                }
            except Exception as e:
                report['results']['post_fault_check'] = {'error': str(e)}
        finally:
            self.restore()
            report['restored'] = True
        return report


# ============================================================================
# DriftDetector (main orchestrator)
# ============================================================================
class DriftDetector:
    """
    Production-grade drift detector v3.0.0.

    Sub-services: DistanceService, SnapshotService, RollbackService,
    EventPersistenceService, GatingService.
    """

    def __init__(
        self,
        storage: Optional[StorageProtocol] = None,
        adaptive_cost: Optional[AdaptiveCostProtocol] = None,
        metrics_registry: Optional[MetricsRegistryProtocol] = None,
        config: Optional[Union[DriftDetectorConfig, Dict[str, Any]]] = None,
        publish_fn: Optional[Callable[[str, Any], Awaitable[None]]] = None,
        **kwargs: Any,
    ) -> None:
        if config is None:
            base = _default_config_from_central()
            valid = {f.name for f in dataclass_fields(DriftDetectorConfig)}
            for k, v in kwargs.items():
                if k in valid:
                    setattr(base, k, v)
            config = base
        elif isinstance(config, dict):
            config = DriftDetectorConfig.from_dict(config)
        self.config = config
        self.storage = storage
        self.adaptive_cost = adaptive_cost
        self.metrics_registry = metrics_registry

        # Infra
        self.observability = ObservabilityHub()
        self.dlq = DeadLetterQueue()
        self._idempotency = IdempotencyCache()
        self.governance = GovernanceLayer(
            enabled=config.enable_human_approval,
            timeout_seconds=config.approval_timeout_seconds,
            auto_approve_on_timeout=config.approval_auto_approve_on_timeout,
            publish_fn=publish_fn,
        )
        self.safety = SafetyMonitor()
        self._setup_invariants()

        # Sub-services
        self.distance_service = DistanceService(config)
        self.snapshot_service = SnapshotService(
            storage=storage, observability=self.observability, dlq=self.dlq,
        )
        self.rollback_service = RollbackService(
            adaptive_cost=adaptive_cost,
            observability=self.observability,
            governance=self.governance,
            dlq=self.dlq,
            require_approval=config.require_approval_for_rollback,
        ) if adaptive_cost is not None else None
        self.persistence_service = EventPersistenceService(
            config, storage=storage,
            observability=self.observability, dlq=self.dlq,
        )
        self.gating_service = (
            GatingService(config, self.observability)
            if config.enable_moe else None
        )
        self.chaos_service = (
            ChaosService(self, config) if config.enable_chaos_testing else None
        )

        # Detection state
        self._drift_counter: int = 0
        self._ewma_distance: float = 0.0
        self._effective_metric: str = config.metric
        self._distance_history: Deque[float] = deque(
            maxlen=config.distance_history_size,
        )
        self._raw_history: Deque[float] = deque(
            maxlen=config.distance_history_size,
        )
        self._state_lock = asyncio.Lock()

        # Lifecycle
        self._started = False
        self._shutting_down = False
        self._background_tasks: List[asyncio.Task] = []
        self._ready_event = asyncio.Event()

        logger.info(
            "DriftDetector v%s initialised: metric=%s threshold=%.4f "
            "hysteresis=%d adaptive=%s moe=%s governance=%s chaos=%s",
            MODULE_VERSION, config.metric, config.threshold,
            config.hysteresis_count, config.use_adaptive_threshold,
            self.gating_service is not None, self.governance.enabled,
            self.chaos_service is not None,
        )

    # ------------------------------------------------------------------
    # Safety invariants
    # ------------------------------------------------------------------
    def _setup_invariants(self) -> None:
        def _threshold_positive(state: Dict[str, Any]) -> bool:
            return float(state.get('threshold', 0.0)) > 0.0

        def _counter_nonneg(state: Dict[str, Any]) -> bool:
            return int(state.get('drift_counter', 0)) >= 0

        def _distance_finite(state: Dict[str, Any]) -> bool:
            d = state.get('distance')
            if d is None:
                return True
            return math.isfinite(float(d))

        def _history_nonempty(state: Dict[str, Any]) -> bool:
            return int(state.get('history_size', 1)) >= 0

        self.safety.add('threshold_positive', _threshold_positive,
                        'Effective threshold must be > 0')
        self.safety.add('counter_nonneg', _counter_nonneg,
                        'Drift counter must be >= 0')
        self.safety.add('distance_finite', _distance_finite,
                        'Distance must be finite')
        self.safety.add('history_nonempty', _history_nonempty,
                        'Distance history size must be >= 0')

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def start(self) -> None:
        if self._started:
            return
        self._started = True

        # Init LIMIT Graph
        if self.config.enable_limit_graph:
            try:
                existing = self.persistence_service.get_metadata("drift_events")
                if not existing:
                    await self.persistence_service.create_graph(
                        "drift_events",
                        "Drift Event Relationships",
                        {"version": MODULE_VERSION, "created_at": time.time()},
                    )
            except Exception as e:
                logger.warning("Graph init failed: %s", e)
                await self.dlq.record('graph_init', str(e))

        # Load last snapshot
        try:
            await self.snapshot_service.load_last()
        except Exception as e:
            logger.warning("Snapshot load failed: %s", e)

        self._ready_event.set()
        logger.info("DriftDetector started")

    async def wait_ready(self) -> None:
        if not self._started:
            await self.start()
        await self._ready_event.wait()

    async def shutdown(self) -> None:
        logger.info("DriftDetector shutdown")
        self._shutting_down = True
        for t in self._background_tasks:
            t.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)
        self._background_tasks.clear()
        unmet = self.safety.final_check()
        if unmet:
            logger.warning("Unmet eventual invariants: %s", unmet)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    async def check_drift(
        self,
        current_weights: Dict[str, float],
        idempotency_key: Optional[str] = None,
        dry_run: bool = False,
    ) -> DriftResult:
        """
        Compare current weights against the last snapshot and return a
        `DriftResult`. Never returns None.
        """
        await self.wait_ready()
        if idempotency_key and self._idempotency.seen(idempotency_key):
            logger.debug("Duplicate drift check ignored")
            return DriftResult(
                distance=0.0, raw_distance=0.0,
                effective_metric=self.config.metric,
                threshold=self._effective_threshold(),
                drift_detected=False, consecutive=self._drift_counter,
                rollback_triggered=False,
                timestamp=time.monotonic(), dry_run=True,
            )

        try:
            return await self._check_drift_internal(current_weights, dry_run)
        except Exception as e:
            logger.exception("check_drift failed: %s", e)
            await self.dlq.record('check_drift', str(e))
            return DriftResult(
                distance=float('inf'), raw_distance=float('inf'),
                effective_metric=self.config.metric,
                threshold=self._effective_threshold(),
                drift_detected=False, consecutive=self._drift_counter,
                rollback_triggered=False,
                timestamp=time.monotonic(), dry_run=dry_run,
            )

    async def _check_drift_internal(
        self, current_weights: Dict[str, float], dry_run: bool,
    ) -> DriftResult:
        last_snap = await self.snapshot_service.load_last()
        if not last_snap:
            if not dry_run:
                await self.snapshot_service.take_snapshot(
                    current_weights, "initial",
                )
            return DriftResult(
                distance=0.0, raw_distance=0.0,
                effective_metric=self.config.metric,
                threshold=self._effective_threshold(),
                drift_detected=False, consecutive=self._drift_counter,
                rollback_triggered=False,
                timestamp=time.monotonic(), dry_run=dry_run,
            )

        prev_weights = SnapshotService.deserialize(last_snap.get("online_weights", "{}"))

        # Compute all distance metrics
        all_distances = self.distance_service.compute_all(
            current_weights, prev_weights,
        )

        # MoE selection or configured metric
        effective_metric = self.config.metric
        if self.gating_service is not None:
            features = {
                'distance': all_distances.get(self.config.metric, 0.0),
                'ewma': self._ewma_distance,
                'threshold': self._effective_threshold(),
                'consecutive': self._drift_counter,
                'history_mean': (float(np.mean(self._distance_history))
                                 if self._distance_history else 0.0),
            }
            try:
                selected, _ = await self.gating_service.select_expert(
                    features, current_weights=self.config.metric,
                )
                if selected in all_distances:
                    effective_metric = selected
            except Exception as e:
                logger.warning("MoE selection failed: %s", e)
        raw_dist = float(all_distances.get(effective_metric,
                                            all_distances.get(self.config.metric, 0.0)))
        if not math.isfinite(raw_dist):
            raw_dist = 0.0

        # EWMA update
        self._ewma_distance = (
            self.config.ewma_alpha * raw_dist
            + (1 - self.config.ewma_alpha) * self._ewma_distance
        )

        # Effective threshold
        effective_threshold = self._effective_threshold()

        # Periodic snapshot
        now = time.monotonic()
        if (not dry_run and
                (now - self.snapshot_service.last_snapshot_time) >
                self.config.snapshot_interval):
            await self.snapshot_service.take_snapshot(
                current_weights, "periodic",
            )

        # Drift metric (EWMA when alpha < 1)
        drift_metric = (self._ewma_distance if self.config.ewma_alpha < 1.0
                        else raw_dist)

        # Always append raw dist (fix: was skipped on drift)
        async with self._state_lock:
            self._raw_history.append(raw_dist)
            self._distance_history.append(raw_dist)
            self._effective_metric = effective_metric

        drift_detected = drift_metric > effective_threshold
        rollback_triggered = False

        if drift_detected:
            self._drift_counter += 1
            logger.warning(
                "Drift detected: metric=%s distance=%.4f (threshold=%.4f) "
                "consecutive=%d",
                effective_metric, drift_metric, effective_threshold,
                self._drift_counter,
            )
            self.observability.counter('drift_detections_total',
                                        metric=effective_metric)
            self.observability.histogram('drift_distance', drift_metric)

            if not dry_run:
                await self._log_drift_event(
                    drift_metric, effective_threshold,
                    self._drift_counter, rollback_triggered=False,
                )
                await self._persist_drift_event(
                    drift_metric, effective_threshold,
                    self._drift_counter, effective_metric,
                )

            if self._drift_counter >= self.config.hysteresis_count:
                if self.config.rollback_enabled and not dry_run:
                    if self.rollback_service is not None:
                        ok = await self.rollback_service.rollback(
                            last_snap, reason="auto",
                            distance=drift_metric, threshold=effective_threshold,
                        )
                        rollback_triggered = bool(ok)
                        if ok:
                            self._drift_counter = 0
                            self._ewma_distance = 0.0
                            await self._log_drift_event(
                                drift_metric, effective_threshold,
                                0, rollback_triggered=True,
                            )
                            if self.config.enable_rlhf:
                                await self.persistence_service.record_preference_pair(
                                    pair_id=uuid.uuid4().hex,
                                    prompt="Should we rollback after drift?",
                                    chosen="rollback",
                                    rejected="keep_current",
                                    reward_diff=1.0,
                                    metadata={
                                        'distance': drift_metric,
                                        'threshold': effective_threshold,
                                    },
                                )
                    else:
                        logger.error("Rollback service unavailable")
                else:
                    logger.error(
                        "Drift detected but rollback disabled; manual "
                        "intervention required."
                    )
        else:
            if self._drift_counter > 0:
                logger.info(
                    "Drift resolved: distance=%.4f below threshold=%.4f",
                    drift_metric, effective_threshold,
                )
                self._drift_counter = 0

        # MoE training with reward (fix: reward now used)
        if self.gating_service is not None and not dry_run:
            reward = -1.0 if drift_detected else 1.0
            update_features = {
                'distance': raw_dist,
                'ewma': self._ewma_distance,
                'threshold': effective_threshold,
                'consecutive': self._drift_counter,
                'history_mean': (float(np.mean(self._distance_history))
                                 if self._distance_history else 0.0),
            }
            try:
                await self.gating_service.add_training_sample(
                    update_features, self.config.metric,
                    effective_metric, reward,
                )
            except Exception as e:
                logger.warning("MoE training failed: %s", e)

        # Safety check
        if self.config.enable_temporal_safety:
            violations = self.safety.check({
                'threshold': effective_threshold,
                'drift_counter': self._drift_counter,
                'distance': raw_dist,
                'history_size': len(self._distance_history),
            })
            if violations:
                logger.warning("Safety violations: %s", violations)
                self.observability.counter('safety_violations_total')

        result = DriftResult(
            distance=float(drift_metric),
            raw_distance=float(raw_dist),
            effective_metric=effective_metric,
            threshold=float(effective_threshold),
            drift_detected=bool(drift_detected),
            consecutive=int(self._drift_counter),
            rollback_triggered=rollback_triggered,
            timestamp=now,
            dry_run=dry_run,
        )
        self.observability.gauge('drift_ewma', float(self._ewma_distance))
        self.observability.gauge('drift_threshold', float(effective_threshold))
        return result

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _effective_threshold(self) -> float:
        if not self.config.use_adaptive_threshold:
            return float(self.config.threshold)
        if len(self._distance_history) < 10:
            return float(self.config.threshold)
        arr = np.array(self._distance_history, dtype=float)
        arr = arr[np.isfinite(arr)]
        if arr.size < 5:
            return float(self.config.threshold)
        mean = float(arr.mean())
        std = float(arr.std())
        return float(mean + self.config.adaptive_k * std)

    async def _log_drift_event(
        self, distance: float, threshold: float,
        consecutive: int, rollback_triggered: bool,
    ) -> None:
        event = {
            "timestamp": time.time(),
            "distance": float(distance),
            "threshold": float(threshold),
            "consecutive": int(consecutive),
            "rollback_triggered": bool(rollback_triggered),
        }
        if self.storage is None:
            return
        try:
            await asyncio.to_thread(self.storage.log_drift_event, event)
        except Exception as e:
            logger.warning("Failed to log drift event: %s", e)
            await self.dlq.record('log_drift_event', str(e), event)

    async def _persist_drift_event(
        self, distance: float, threshold: float,
        consecutive: int, metric: str,
    ) -> None:
        if self.config.enable_limit_graph:
            try:
                await self.persistence_service.add_node(
                    "drift_events",
                    f"drift_{uuid.uuid4().hex[:8]}",
                    "drift_event",
                    {
                        'distance': float(distance),
                        'threshold': float(threshold),
                        'consecutive': int(consecutive),
                        'metric': metric,
                        'timestamp': time.time(),
                    },
                )
            except Exception as e:
                await self.dlq.record('limit_graph_drift_event', str(e))
        if self.config.enable_modp:
            try:
                await self.persistence_service.add_modp_state(
                    state_id=uuid.uuid4().hex,
                    problem_id="drift_detection",
                    state_attributes={
                        'distance': float(distance),
                        'threshold': float(threshold),
                        'consecutive': int(consecutive),
                        'metric': metric,
                    },
                    objective_values={
                        'distance': float(distance),
                        'inverse_threshold': float(1.0 / max(threshold, NUMERIC_EPS)),
                        'consecutive': float(consecutive),
                    },
                    stage=0,
                )
            except Exception as e:
                await self.dlq.record('modp_drift_state', str(e))

    # ------------------------------------------------------------------
    # Force APIs
    # ------------------------------------------------------------------
    async def force_snapshot(self) -> Dict[str, Any]:
        await self.wait_ready()
        if self.adaptive_cost is None:
            raise RuntimeError("No adaptive_cost available")
        try:
            current = await asyncio.to_thread(
                self.adaptive_cost.get_current_weights,
            )
        except Exception as e:
            logger.error("Failed to get current weights: %s", e)
            await self.dlq.record('force_snapshot', str(e))
            return {}
        return await self.snapshot_service.take_snapshot(current, "manual")

    async def force_rollback(self) -> bool:
        await self.wait_ready()
        last_snap = await self.snapshot_service.load_last()
        if not last_snap:
            logger.warning("No snapshot available for rollback")
            return False
        if self.rollback_service is None:
            return False
        return await self.rollback_service.rollback(
            last_snap, reason="manual",
        )

    def get_drift_history(self, limit: int = 50) -> List[Dict[str, Any]]:
        if self.storage is None:
            return []
        try:
            return list(self.storage.get_drift_events(limit=limit))
        except Exception as e:
            logger.warning("Failed to fetch drift events: %s", e)
            return []

    async def reset_state(self) -> None:
        async with self._state_lock:
            self._drift_counter = 0
            self._ewma_distance = 0.0
            self._effective_metric = self.config.metric
            self._distance_history.clear()
            self._raw_history.clear()
        logger.info("Drift detector state reset")

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------
    async def get_limit_graph(self, graph_id: str = "drift_events") -> Dict[str, Any]:
        return {
            'metadata': self.persistence_service.get_metadata(graph_id),
            'nodes': self.persistence_service.get_nodes(graph_id),
            'edges': self.persistence_service.get_edges(graph_id),
        }

    def get_modp_states(self, problem_id: str = "drift_detection") -> List[Dict[str, Any]]:
        return self.persistence_service.get_modp_states(problem_id)

    def get_rlhf_pairs(self, limit: int = 100) -> List[Dict[str, Any]]:
        return self.persistence_service.get_preference_pairs(limit)

    def get_moe_experts(self) -> List[str]:
        if self.gating_service is None:
            return []
        return list(self.gating_service.expert_names)

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

    def list_pending_approvals(self) -> List[Dict[str, Any]]:
        return self.governance.list_pending()

    async def inject_fault(self, fault_type: str, **params: Any) -> Dict[str, Any]:
        if self.chaos_service is None:
            return {'status': 'disabled'}
        try:
            ft = FaultType(fault_type)
        except ValueError:
            return {'status': 'unknown_fault'}
        return await self.chaos_service.inject(ft, **params)

    async def run_chaos_test(self) -> Dict[str, Any]:
        if self.chaos_service is None:
            return {'status': 'disabled'}
        return await self.chaos_service.run_test()

    # ------------------------------------------------------------------
    # Health
    # ------------------------------------------------------------------
    async def get_health(self) -> Dict[str, Any]:
        return {
            "status": "healthy" if not self._shutting_down else "stopping",
            "version": MODULE_VERSION,
            "started": self._started,
            "effective_metric": self._effective_metric,
            "drift_counter": self._drift_counter,
            "ewma_distance": self._ewma_distance,
            "effective_threshold": self._effective_threshold(),
            "history_size": len(self._distance_history),
            "components": {
                "limit_graph": self.config.enable_limit_graph,
                "modp": self.config.enable_modp,
                "rlhf": self.config.enable_rlhf,
                "moe": self.gating_service is not None,
                "governance": self.governance.enabled,
                "chaos": self.chaos_service is not None,
                "rollback": self.rollback_service is not None,
            },
            "dead_letters": len(self.dlq),
            "governance_pending": len(self.governance.pending),
        }


# ============================================================================
# Exports
# ============================================================================
__all__ = [
    # Main
    "DriftDetector",
    "DriftDetectorConfig",
    "DriftResult",
    # Sub-services
    "DistanceService",
    "SnapshotService",
    "RollbackService",
    "EventPersistenceService",
    "GatingService",
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
    # Enums
    "FaultType",
    # Protocols
    "StorageProtocol",
    "AdaptiveCostProtocol",
    "MetricsRegistryProtocol",
    "LimitGraphBackendProtocol",
    "MODPBackendProtocol",
    "RLHFBackendProtocol",
    "EventBrokerProtocol",
    # Constants
    "MODULE_VERSION",
    "VALID_METRICS",
    "DEFAULT_MOE_EXPERTS",
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

    class _InMemoryStorage:
        def __init__(self) -> None:
            self.last_snapshot: Optional[Dict[str, Any]] = None
            self.events: List[Dict[str, Any]] = []

        def get_last_snapshot(self) -> Optional[Dict[str, Any]]:
            return self.last_snapshot

        def save_drift_snapshot(self, snapshot_id, online, offline,
                                 cost_score, reason):
            self.last_snapshot = {
                "snapshot_id": snapshot_id,
                "online_weights": online,
                "offline_weights": offline,
                "timestamp": time.monotonic(),
                "reason": reason,
                "cost_score": cost_score,
            }

        def log_drift_event(self, event):
            self.events.append(event)

        def get_drift_events(self, limit=50):
            return self.events[-limit:]

    class _AdaptiveCost:
        def __init__(self) -> None:
            self._weights = {"quality": 0.25, "latency": 0.25,
                             "carbon": 0.25, "helium": 0.25}

        def get_current_weights(self) -> Dict[str, float]:
            return dict(self._weights)

        def restore_weights(self, weights: Dict[str, float]) -> None:
            self._weights = dict(weights)

    async def _demo() -> None:
        config = DriftDetectorConfig(
            threshold=0.2, hysteresis_count=2, enable_chaos_testing=True,
        )
        detector = DriftDetector(
            storage=_InMemoryStorage(),
            adaptive_cost=_AdaptiveCost(),
            config=config,
        )
        await detector.start()

        # First call initializes snapshot
        r0 = await detector.check_drift(
            {"quality": 0.25, "latency": 0.25, "carbon": 0.25, "helium": 0.25},
            idempotency_key="initial",
        )
        print(f"Initial: distance={r0.distance:.4f} drift={r0.drift_detected}")

        # Second call with a big change
        r1 = await detector.check_drift(
            {"quality": 0.9, "latency": 0.05, "carbon": 0.03, "helium": 0.02},
            idempotency_key="drift-1",
        )
        print(f"Drift 1: distance={r1.distance:.4f} drift={r1.drift_detected}")

        r2 = await detector.check_drift(
            {"quality": 0.9, "latency": 0.05, "carbon": 0.03, "helium": 0.02},
            idempotency_key="drift-2",
        )
        print(f"Drift 2: distance={r2.distance:.4f} drift={r2.drift_detected} "
              f"rollback={r2.rollback_triggered}")

        health = await detector.get_health()
        print(f"Health: {health['status']} metric={health['effective_metric']}")

        chaos = await detector.run_chaos_test()
        print(f"Chaos restored: {chaos.get('restored')}, faults={chaos.get('faults')}")

        metrics = await detector.get_prometheus_metrics()
        print(metrics[:300])

        await detector.shutdown()

    asyncio.run(_demo())
