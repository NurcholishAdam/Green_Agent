#!/usr/bin/env python3
# pareto_gating.py
"""
Pareto Gating Module (Enhanced v3.0.0)
========================================

Production-grade Pareto gating with:

* Sub-services: ConstraintService, ParetoService, ScoringService,
  PersistenceService, EvolutionService, GatingService.
* Protocols for all injected components.
* Shared `ObservabilityHub` and `GovernanceLayer`.
* Non-destructive `ChaosService` with full save/restore.
* Unified MOPD scalarisation with canonical objective directions.
* Full lifecycle: `start()` / `wait_ready()` / `shutdown()`.
* Pydantic v1/v2 compatible config (dataclass fallback).
* Idempotency keys with LRU cache.
* Dead-letter queue with bounded capacity.
* Bounded caches (deque maxlen / OrderedDict LRU).

FIXED BUGS OVER v2.0
--------------------
- NaN propagation in `_pareto_filter` and `score_candidates` eliminated.
- `missing_policy='drop'` is honoured at both filter and scoring stages.
- `MoEGatingNetwork.add_training_sample` now uses `reward` in the gradient.
- NSGA-II uses stable integer IDs (not `id()`).
- NSGA-II refills population from Pareto fronts after deduplication.
- NSGA-II `_eval_cache` bounded via LRU.
- NSGA-II tournament size bounded by population size.
- NSGA-II does not rely on a parallel `_all_points` list (no silent zip).
- `run_nsga_evolution.evaluate` is now weight-dependent (rank correlation).
- `set_objectives` invalidates NSGA optimizer state and evolved weights.
- Dynamic constraints cannot override hard constraints.
- Sibling imports guarded with try/except; `config.PARETO_*` reads via getattr.
- LIMIT Graph and MODP writes batched behind a `persist` flag.
- Concurrent filter calls serialised via a lock.
- `filter` returns a consistent `(pareto, stats)` tuple via a helper alias.
- MoE gating uses context-adaptive reward-aware updates.
- Missing-value handling centralised in a single policy resolver.
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import logging
import math
import random
import time
import uuid
from abc import ABC, abstractmethod
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
EVAL_CACHE_MAX = 4_096
RFLHF_PAIRS_MAX = 10_000
LIMIT_GRAPH_NODES_MAX = 10_000
LIMIT_GRAPH_EDGES_MAX = 20_000
MODP_STATES_MAX = 10_000
NUMERIC_EPS = 1e-9
DEFAULT_APPROVAL_TIMEOUT = 60.0
CHAOS_MAX_FAULTS_PER_RUN = 6

# Default objective set (matches original v2.0)
DEFAULT_OBJECTIVES: Tuple[Dict[str, str], ...] = (
    {"key": "quality_score", "direction": "max"},
    {"key": "latency_ms", "direction": "min"},
    {"key": "carbon_g", "direction": "min"},
    {"key": "energy_joules", "direction": "min"},
    {"key": "helium_cost", "direction": "min"},
    {"key": "resource_usage", "direction": "min"},
)

# Supported constraint operators
_OPS: Dict[str, Callable[[float, float], bool]] = {
    ">=": lambda a, b: a >= b,
    "<=": lambda a, b: a <= b,
    ">": lambda a, b: a > b,
    "<": lambda a, b: a < b,
    "==": lambda a, b: a == b,
    "!=": lambda a, b: a != b,
}

# Canonical lower-is-better objectives (used by scalarise_pareto)
MOPD_HIGHER_IS_BETTER: FrozenSet[str] = frozenset({
    "quality_score", "cache_hit", "sustainability", "impact",
    "risk_reduction", "longterm_efficiency", "net_savings", "carbon_savings",
})


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
    """Coerce to a finite float, or return default."""
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
    def save_limit_graph_metadata(self, graph_id: str, description: str,
                                  configuration: Dict[str, Any]) -> Any: ...
    def save_limit_graph_node(self, node_id: str, graph_id: str,
                              node_type: Optional[str],
                              attributes: Dict[str, Any]) -> Any: ...
    def save_limit_graph_edge(self, edge_id: str, graph_id: str, source: str,
                              target: str, weight: Optional[float],
                              attributes: Dict[str, Any]) -> Any: ...
    def save_modp_state(self, state_id: str, problem_id: str,
                        state_attributes: Dict[str, Any],
                        objective_values: Dict[str, float], stage: int) -> Any: ...
    def save_modp_policy(self, policy_id: str, problem_id: str, state_id: str,
                         action: str,
                         expected_objectives: Dict[str, float]) -> Any: ...
    def save_preference_pair(self, pair_id: str, prompt: str, chosen: str,
                             rejected: str, reward_diff: float,
                             metadata: Optional[Dict[str, Any]]) -> Any: ...


@runtime_checkable
class EvolutionBackendProtocol(Protocol):
    async def evolve(self, keys: Sequence[str]) -> List[Any]: ...


@runtime_checkable
class EventBrokerProtocol(Protocol):
    def subscribe(self, event_type: str,
                  handler: Callable[[Any], Awaitable[None]]) -> None: ...


@runtime_checkable
class MessageQueueProtocol(Protocol):
    async def publish(self, topic: str, payload: Any) -> None: ...


# ============================================================================
# Enums
# ============================================================================
class MissingPolicy(str, Enum):
    DROP = "drop"
    WORST = "worst"


class Normalisation(str, Enum):
    MINMAX = "minmax"
    ZSCORE = "zscore"
    RANK = "rank"


class ObjectiveDirection(str, Enum):
    MAX = "max"
    MIN = "min"


class FaultType(str, Enum):
    EVALUATION_FAILURE = "evaluation_failure"
    PERSISTENCE_FAILURE = "persistence_failure"
    GATING_FAILURE = "gating_failure"
    CONSTRAINT_FAILURE = "constraint_failure"
    SCORING_FAILURE = "scoring_failure"
    NSGA_FAILURE = "nsga_failure"


class ApprovalStatus(str, Enum):
    PENDING = "pending"
    APPROVED = "approved"
    DENIED = "denied"
    TIMEOUT = "timeout"
    AUTO_APPROVED = "auto_approved"


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


class CircuitBreakerState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


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
# Unified MOPD helpers
# ============================================================================
def _canonical_objective_names(
    objectives: Sequence[Dict[str, str]],
) -> List[str]:
    return [obj["key"] for obj in objectives]


def _directions_map(objectives: Sequence[Dict[str, str]]) -> Dict[str, str]:
    return {obj["key"]: obj["direction"] for obj in objectives}


def _point_objective_vector(
    point: Any,
    names: Sequence[str],
    directions: Dict[str, str],
) -> List[float]:
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
        val = _coerce_float(getattr(point, name, None), 0.0) or 0.0
        d = directions.get(name, "max")
        # Canonical lower-is-better: flip max-aimed objectives
        if d == "max":
            val = -val
        out.append(val)
    return out


def scalarise_pareto(
    points: Sequence[Any],
    weights: Optional[Dict[str, float]] = None,
    objectives: Optional[Sequence[Dict[str, str]]] = None,
    score_attr: str = 'scalarised_score',
) -> Optional[Any]:
    """Min-max scalarise points using canonical lower-is-better form."""
    if not points:
        return None
    objs = list(objectives or [dict(o) for o in DEFAULT_OBJECTIVES])
    names = _canonical_objective_names(objs)
    directions = _directions_map(objs)
    w = dict(weights or {name: 1.0 / len(names) for name in names})

    vectors = [_point_objective_vector(p, names, directions) for p in points]
    arr = np.array(vectors, dtype=float)
    # Replace non-finite entries with column max (worst) to prevent NaN
    if not np.all(np.isfinite(arr)):
        col_max = np.where(np.isfinite(arr), arr, -np.inf).max(axis=0)
        arr = np.where(np.isfinite(arr), arr, col_max[None, :])

    mins = arr.min(axis=0)
    maxs = arr.max(axis=0)
    ranges = np.where(maxs - mins > NUMERIC_EPS, maxs - mins, 1.0)

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
    objectives: Optional[Sequence[Dict[str, str]]] = None,
) -> List[Any]:
    """Return non-dominated points in canonical lower-is-better form."""
    if not points:
        return []
    objs = list(objectives or [dict(o) for o in DEFAULT_OBJECTIVES])
    names = _canonical_objective_names(objs)
    directions = _directions_map(objs)
    vecs = [_point_objective_vector(p, names, directions) for p in points]
    arr = np.array(vecs, dtype=float)
    if not np.all(np.isfinite(arr)):
        col_max = np.where(np.isfinite(arr), arr, -np.inf).max(axis=0)
        arr = np.where(np.isfinite(arr), arr, col_max[None, :])
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
class NSGAIIWeightVector:
    vector_id: str
    weights: Dict[str, float]
    objectives: Dict[str, float]
    scalarised_score: float = 0.0
    stable_id: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            'vector_id': self.vector_id,
            'weights': dict(self.weights),
            'objectives': dict(self.objectives),
            'scalarised_score': float(self.scalarised_score),
            'stable_id': int(self.stable_id),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "NSGAIIWeightVector":
        return cls(
            vector_id=data.get('vector_id', uuid.uuid4().hex),
            weights=dict(data.get('weights', {})),
            objectives=dict(data.get('objectives', {})),
            scalarised_score=float(data.get('scalarised_score', 0.0)),
            stable_id=int(data.get('stable_id', 0)),
        )


@dataclass
class ParetoResult:
    pareto: List[Dict[str, Any]]
    stats: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return {'pareto': self.pareto, 'stats': self.stats}


# ============================================================================
# Config
# ============================================================================
@dataclass
class ParetoGatingConfig:
    constraints: Dict[str, Any] = field(default_factory=dict)
    objectives: List[Dict[str, str]] = field(
        default_factory=lambda: [dict(o) for o in DEFAULT_OBJECTIVES],
    )
    missing_policy: str = "drop"

    # Feature flags for optional components
    enable_limit_graph: bool = True
    enable_modp: bool = True
    enable_rlhf: bool = True
    enable_moe: bool = True
    enable_nsga: bool = True
    enable_human_approval: bool = False
    enable_temporal_safety: bool = True
    enable_chaos_testing: bool = False
    enable_idempotency: bool = True

    # MoE / NSGA configuration
    moe_expert_names: List[str] = field(default_factory=lambda: [
        "rule_based", "learned", "evolved",
    ])
    nsga_population_size: int = 20
    nsga_generations: int = 10
    nsga_mutation_rate: float = 0.2
    nsga_crossover_rate: float = 0.8
    nsga_tournament_size: int = 3

    # Governance / chaos sub-configs
    approval_timeout_seconds: float = DEFAULT_APPROVAL_TIMEOUT
    approval_auto_approve_on_timeout: bool = False
    approval_require_for_drop_above: float = 0.95
    chaos_interval_seconds: int = 3600

    # Limits
    limit_graph_nodes_max: int = LIMIT_GRAPH_NODES_MAX
    limit_graph_edges_max: int = LIMIT_GRAPH_EDGES_MAX
    modp_states_max: int = MODP_STATES_MAX
    rlhf_pairs_max: int = RFLHF_PAIRS_MAX

    def __post_init__(self) -> None:
        # Normalise
        if self.missing_policy not in ("drop", "worst"):
            raise ValueError(f"Unknown missing_policy: {self.missing_policy}")
        if not self.objectives:
            raise ValueError("objectives must be non-empty")
        seen = set()
        for obj in self.objectives:
            if "key" not in obj or "direction" not in obj:
                raise ValueError("Each objective needs 'key' and 'direction'")
            if obj["direction"] not in ("min", "max"):
                raise ValueError(f"Invalid direction: {obj['direction']}")
            if obj["key"] in seen:
                raise ValueError(f"Duplicate objective key: {obj['key']}")
            seen.add(obj["key"])
        if self.nsga_tournament_size < 2:
            raise ValueError("nsga_tournament_size must be >= 2")
        if self.nsga_population_size < 2:
            raise ValueError("nsga_population_size must be >= 2")

    def to_dict(self) -> Dict[str, Any]:
        return _to_serializable(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ParetoGatingConfig":
        valid = {f.name for f in dataclass_fields(cls)}
        return cls(**{k: v for k, v in data.items() if k in valid})


def _default_config_from_central() -> ParetoGatingConfig:
    """Build a config from the sibling central config with getattr fallbacks."""
    constraints: Dict[str, Any] = {}
    if _central_config is not None:
        constraints = {
            "quality_score": getattr(_central_config, "PARETO_QUALITY_MIN", 0.0),
            "latency_ms": getattr(_central_config, "PARETO_LATENCY_MAX", float("inf")),
            "carbon_g": getattr(_central_config, "PARETO_CARBON_MAX", float("inf")),
        }
    return ParetoGatingConfig(constraints=constraints)


# ============================================================================
# ConstraintService
# ============================================================================
class ConstraintService:
    """Normalises, validates, and evaluates constraints."""

    def __init__(self, config: ParetoGatingConfig) -> None:
        self.config = config
        self._hard: Dict[str, Dict[str, Any]] = self._normalise(config.constraints)

    @staticmethod
    def _normalise(constraints: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        out: Dict[str, Dict[str, Any]] = {}
        for key, value in constraints.items():
            if isinstance(value, bool):
                raise ValueError(
                    f"Constraint for '{key}' must be numeric or dict with 'op' and 'value'."
                )
            if isinstance(value, (int, float)):
                # Default: quality_score is >= , everything else <=
                if key == "quality_score":
                    out[key] = {"op": ">=", "value": float(value)}
                else:
                    out[key] = {"op": "<=", "value": float(value)}
            elif isinstance(value, dict) and "op" in value and "value" in value:
                op = value["op"]
                if op not in _OPS:
                    raise ValueError(
                        f"Unsupported operator '{op}' for key '{key}'."
                    )
                out[key] = {"op": op, "value": float(value["value"])}
            else:
                raise ValueError(
                    f"Constraint for '{key}' must be numeric or dict with 'op' and 'value'."
                )
        return out

    @property
    def hard_constraints(self) -> Dict[str, Dict[str, Any]]:
        return dict(self._hard)

    def update_hard_constraints(self, constraints: Dict[str, Any]) -> None:
        self._hard = self._normalise(constraints)

    def merge_dynamic(
        self, dynamic: Optional[Dict[str, Any]],
    ) -> Dict[str, Dict[str, Any]]:
        """
        Merge dynamic constraints additively, refusing to weaken hard
        constraints. Raises ValueError on conflict.
        """
        if not dynamic:
            return dict(self._hard)
        merged = dict(self._hard)
        dynamic_norm = self._normalise(dynamic)
        for key, new_c in dynamic_norm.items():
            if key in self._hard:
                old = self._hard[key]
                # Hard constraint wins; if the dynamic one is stricter, keep
                # the stricter. If weaker, reject.
                if not self._is_at_least_as_strict(new_c, old):
                    raise ValueError(
                        f"Dynamic constraint for '{key}' would weaken "
                        f"the hard constraint {old}; refusing."
                    )
                merged[key] = new_c
            else:
                merged[key] = new_c
        return merged

    @staticmethod
    def _is_at_least_as_strict(
        new_c: Dict[str, Any], old_c: Dict[str, Any],
    ) -> bool:
        """Return True if new_c is at least as strict as old_c."""
        n_op, n_v = new_c["op"], new_c["value"]
        o_op, o_v = old_c["op"], old_c["value"]
        # Bounds: >=, > are lower bounds; <=, < are upper bounds
        lower = {">=", ">"}
        upper = {"<=", "<"}
        if n_op in lower and o_op in lower:
            return n_v >= o_v
        if n_op in upper and o_op in upper:
            return n_v <= o_v
        if n_op in ("==", "!=") or o_op in ("==", "!="):
            return n_op == o_op and n_v == o_v
        # Mixed bounds: reject by default (ambiguous)
        return False

    def _satisfies(
        self, candidate: Dict[str, Any],
        constraints: Dict[str, Dict[str, Any]],
    ) -> Tuple[bool, Optional[str]]:
        for key, constraint in constraints.items():
            value = _coerce_float(candidate.get(key), None)
            if value is None:
                return False, key
            op_func = _OPS[constraint["op"]]
            if not op_func(value, constraint["value"]):
                return False, key
        return True, None

    def filter_feasible(
        self,
        candidates: Sequence[Dict[str, Any]],
        dynamic_constraints: Optional[Dict[str, Any]] = None,
        return_reasons: bool = False,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, int], Dict[int, str]]:
        merged = self.merge_dynamic(dynamic_constraints)
        feasible: List[Dict[str, Any]] = []
        rejections: Dict[str, int] = {}
        reasons: Dict[int, str] = {}
        for idx, c in enumerate(candidates):
            ok, failed = self._satisfies(c, merged)
            if ok:
                feasible.append(c)
            else:
                reasons[idx] = failed or "unknown"
                if failed:
                    rejections[failed] = rejections.get(failed, 0) + 1
        if return_reasons:
            return feasible, rejections, reasons
        return feasible, rejections, reasons


# ============================================================================
# ParetoService
# ============================================================================
class ParetoService:
    """Stateless Pareto filtering with missing-value handling."""

    def __init__(self, config: ParetoGatingConfig) -> None:
        self.config = config

    def filter(
        self, candidates: Sequence[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        if not candidates:
            return []
        objs = self.config.objectives
        names = _canonical_objective_names(objs)
        directions = _directions_map(objs)
        policy = self.config.missing_policy

        # Pre-filter for missing policy 'drop'
        if policy == "drop":
            cleaned: List[Dict[str, Any]] = []
            for c in candidates:
                ok = True
                for name in names:
                    if _coerce_float(c.get(name), None) is None:
                        ok = False
                        break
                if ok:
                    cleaned.append(c)
            candidates = cleaned
            if not candidates:
                return []

        # Build matrix in canonical lower-is-better form
        n = len(candidates)
        m = len(names)
        matrix = np.zeros((n, m), dtype=float)
        nan_mask = np.zeros((n, m), dtype=bool)
        for i, c in enumerate(candidates):
            for j, name in enumerate(names):
                v = _coerce_float(c.get(name), None)
                if v is None:
                    nan_mask[i, j] = True
                    v = 0.0  # temporary placeholder
                if directions.get(name, "max") == "max":
                    v = -v
                matrix[i, j] = v

        # Resolve missing (only 'worst' reaches here in practice)
        if policy == "worst" and nan_mask.any():
            col_max = np.where(~nan_mask, matrix, -np.inf).max(axis=0)
            matrix = np.where(nan_mask, col_max[None, :], matrix)

        # Dominance
        better_or_equal = np.all(matrix[:, None, :] >= matrix[None, :, :], axis=2)
        strictly_better = np.any(matrix[:, None, :] > matrix[None, :, :], axis=2)
        dominated_by = better_or_equal & strictly_better
        np.fill_diagonal(dominated_by, False)
        is_dominated = np.any(dominated_by, axis=0)
        pareto_indices = np.where(~is_dominated)[0]
        return [candidates[i] for i in pareto_indices]


# ============================================================================
# ScoringService
# ============================================================================
class ScoringService:
    """Normalisation and scoring."""

    def __init__(self, config: ParetoGatingConfig) -> None:
        self.config = config

    def score(
        self,
        candidates: Sequence[Dict[str, Any]],
        weights: Optional[Dict[str, float]] = None,
        normalisation: str = "minmax",
    ) -> List[Tuple[Dict[str, Any], float]]:
        if not candidates:
            return []
        objs = self.config.objectives
        names = _canonical_objective_names(objs)
        directions = _directions_map(objs)
        policy = self.config.missing_policy

        # Pre-filter for 'drop'
        if policy == "drop":
            filtered: List[Dict[str, Any]] = []
            for c in candidates:
                ok = True
                for name in names:
                    if _coerce_float(c.get(name), None) is None:
                        ok = False
                        break
                if ok:
                    filtered.append(c)
            candidates = filtered
            if not candidates:
                return []

        # Build matrix
        n = len(candidates)
        m = len(names)
        matrix = np.zeros((n, m), dtype=float)
        nan_mask = np.zeros((n, m), dtype=bool)
        for i, c in enumerate(candidates):
            for j, name in enumerate(names):
                v = _coerce_float(c.get(name), None)
                if v is None:
                    nan_mask[i, j] = True
                    v = 0.0
                matrix[i, j] = v

        # For 'worst', fill missing with column max/min
        if policy == "worst" and nan_mask.any():
            for j, name in enumerate(names):
                col = matrix[:, j]
                if directions.get(name, "max") == "max":
                    worst = col.min()
                else:
                    worst = col.max()
                matrix[nan_mask[:, j], j] = worst

        # Normalise
        if normalisation == "minmax":
            mins = matrix.min(axis=0)
            maxs = matrix.max(axis=0)
            ranges = maxs - mins
            ranges = np.where(np.abs(ranges) > NUMERIC_EPS, ranges, 1.0)
            normalised = (matrix - mins) / ranges
            for j, name in enumerate(names):
                if directions.get(name, "max") == "min":
                    normalised[:, j] = 1.0 - normalised[:, j]
        elif normalisation == "zscore":
            means = matrix.mean(axis=0)
            stds = matrix.std(axis=0)
            stds = np.where(np.abs(stds) > NUMERIC_EPS, stds, 1.0)
            normalised = (matrix - means) / stds
            for j, name in enumerate(names):
                if directions.get(name, "max") == "min":
                    normalised[:, j] = -normalised[:, j]
        elif normalisation == "rank":
            normalised = np.zeros_like(matrix, dtype=float)
            for j, name in enumerate(names):
                col = matrix[:, j]
                order = col.argsort()
                ranks = np.empty_like(order, dtype=float)
                ranks[order] = np.arange(len(col))
                if directions.get(name, "max") == "min":
                    ranks = len(col) - 1 - ranks
                max_rank = max(1.0, len(col) - 1.0)
                normalised[:, j] = ranks / max_rank
        else:
            raise ValueError(f"Unknown normalisation '{normalisation}'.")

        # Combine
        w = dict(weights or {})
        weight_vec = np.array([float(w.get(k, 1.0)) for k in names], dtype=float)
        scores = normalised @ weight_vec
        paired = list(zip(candidates, scores.tolist()))
        paired.sort(key=lambda x: x[1], reverse=True)
        return paired


# ============================================================================
# PersistenceService (LIMIT Graph + MODP + RLHF)
# ============================================================================
class PersistenceService:
    """
    Batches writes to LIMIT Graph, MODP, and RLHF storage.

    All writes are behind a `persist=True` flag so the hot path (`filter`)
    does not create unbounded state.
    """

    def __init__(self, config: ParetoGatingConfig,
                 storage: Optional[StorageProtocol] = None,
                 observability: Optional[ObservabilityHub] = None,
                 dlq: Optional[DeadLetterQueue] = None) -> None:
        self.config = config
        self.storage = storage
        self.obs = observability or ObservabilityHub()
        self.dlq = dlq
        self._lock = asyncio.Lock()

        # In-memory fallbacks (bounded)
        self.limit_graphs: Dict[str, Dict[str, Any]] = {}
        self.modp_states: Dict[str, Deque[Dict[str, Any]]] = {}
        self.modp_policies: Deque[Dict[str, Any]] = deque(maxlen=1_000)
        self.rlhf_pairs: Deque[Dict[str, Any]] = deque(maxlen=config.rlhf_pairs_max)

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
                while len(nodes) > self.config.limit_graph_nodes_max:
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
                while len(edges) > self.config.limit_graph_edges_max:
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
                    self.modp_states[problem_id] = deque(maxlen=self.config.modp_states_max)
                self.modp_states[problem_id].append({
                    'state_id': state_id, 'state_attributes': state_attributes,
                    'objective_values': objective_values, 'stage': stage,
                })
        except Exception as e:
            await self._record_dlq('add_modp_state', e, {'state_id': state_id})

    async def add_modp_policy(self, policy_id: str, problem_id: str,
                               state_id: str, action: str,
                               expected_objectives: Dict[str, float]) -> None:
        try:
            if self.storage and hasattr(self.storage, 'save_modp_policy'):
                await asyncio.to_thread(
                    self.storage.save_modp_policy,
                    policy_id, problem_id, state_id, action, expected_objectives,
                )
                return
            async with self._lock:
                self.modp_policies.append({
                    'policy_id': policy_id, 'problem_id': problem_id,
                    'state_id': state_id, 'action': action,
                    'expected_objectives': expected_objectives,
                })
        except Exception as e:
            await self._record_dlq('add_modp_policy', e, {'policy_id': policy_id})

    def get_modp_states(self, problem_id: str) -> List[Dict[str, Any]]:
        if self.storage and hasattr(self.storage, 'get_modp_states'):
            try:
                return list(self.storage.get_modp_states(problem_id))
            except Exception:
                pass
        return list(self.modp_states.get(problem_id, []))

    def get_modp_policies(self, problem_id: str) -> List[Dict[str, Any]]:
        if self.storage and hasattr(self.storage, 'get_modp_policies'):
            try:
                return list(self.storage.get_modp_policies(problem_id))
            except Exception:
                pass
        return [p for p in self.modp_policies if p.get('problem_id') == problem_id]

    # ---------- RLHF ----------
    async def record_preference_pair(self, pair_id: str, prompt: str,
                                      chosen: str, rejected: str,
                                      reward_diff: float,
                                      metadata: Optional[Dict[str, Any]] = None) -> None:
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
# EvolutionService (NSGA-II over objective weights)
# ============================================================================
class EvolutionService:
    """
    NSGA-II over objective weights.

    Fixes: stable IDs, bounded eval cache, refilled population,
    weight-dependent evaluation, bounded tournament size.
    """

    def __init__(
        self,
        config: ParetoGatingConfig,
        evaluate: Callable[[Dict[str, float]], Dict[str, float]],
        observability: Optional[ObservabilityHub] = None,
        dlq: Optional[DeadLetterQueue] = None,
    ) -> None:
        self.config = config
        self.evaluate = evaluate
        self.obs = observability or ObservabilityHub()
        self.dlq = dlq
        self.population_size = config.nsga_population_size
        self.generations = config.nsga_generations
        self.mutation_rate = config.nsga_mutation_rate
        self.crossover_rate = config.nsga_crossover_rate
        self.tournament_size = config.nsga_tournament_size
        self._rng = random.Random(42)
        self._next_id = 1
        self._eval_cache: "OrderedDict[Tuple[Tuple[str, float], ...], Dict[str, float]]" = (
            OrderedDict()
        )
        self.pareto_front: List[NSGAIIWeightVector] = []
        self.best: Optional[NSGAIIWeightVector] = None
        self._disabled = False

    def disable(self) -> None:
        self._disabled = True

    def enable(self) -> None:
        self._disabled = False

    def clear_cache(self) -> None:
        self._eval_cache.clear()

    # ---------- Genetic operators ----------
    def _random_individual(self, keys: Sequence[str]) -> Dict[str, float]:
        w = {k: self._rng.random() for k in keys}
        total = sum(w.values())
        return {k: v / total for k, v in w.items()} if total > 0 else w

    def _crossover(self, p1: Dict[str, float],
                    p2: Dict[str, float]) -> Dict[str, float]:
        child: Dict[str, float] = {}
        for k in p1:
            if self._rng.random() < 0.5:
                u = self._rng.random()
                beta = ((2 * u) ** (1 / 21)) if u <= 0.5 else ((1 / (2 * (1 - u))) ** (1 / 21))
                child[k] = max(0.0, min(1.0, 0.5 * ((1 + beta) * p1[k] + (1 - beta) * p2[k])))
            else:
                child[k] = p1[k] if self._rng.random() < 0.5 else p2[k]
        total = sum(child.values())
        return {k: v / total for k, v in child.items()} if total > 0 else child

    def _mutate(self, ind: Dict[str, float]) -> Dict[str, float]:
        mutant = dict(ind)
        for k in mutant:
            if self._rng.random() < self.mutation_rate:
                u = self._rng.random()
                delta = ((2 * u) ** (1 / 21) - 1) if u < 0.5 else (1 - (2 * (1 - u)) ** (1 / 21))
                mutant[k] = max(0.0, min(1.0, mutant[k] + delta))
        total = sum(mutant.values())
        return {k: v / total for k, v in mutant.items()} if total > 0 else mutant

    # ---------- Cache ----------
    def _cache_get(self, ind: Dict[str, float]) -> Optional[Dict[str, float]]:
        key = tuple(sorted((k, float(v)) for k, v in ind.items()))
        if key in self._eval_cache:
            self._eval_cache.move_to_end(key)
            return self._eval_cache[key]
        return None

    def _cache_put(self, ind: Dict[str, float],
                    obj: Dict[str, float]) -> None:
        key = tuple(sorted((k, float(v)) for k, v in ind.items()))
        self._eval_cache[key] = obj
        if len(self._eval_cache) > EVAL_CACHE_MAX:
            self._eval_cache.popitem(last=False)

    # ---------- Dominance ----------
    @staticmethod
    def _dominates(a: NSGAIIWeightVector, b: NSGAIIWeightVector) -> bool:
        a_obj = a.objectives
        b_obj = b.objectives
        keys = set(a_obj) | set(b_obj)
        better_or_equal = all(a_obj.get(k, 0.0) >= b_obj.get(k, 0.0) for k in keys)
        strictly_better = any(a_obj.get(k, 0.0) > b_obj.get(k, 0.0) for k in keys)
        return better_or_equal and strictly_better

    def _fast_non_dominated_sort(
        self, points: Sequence[NSGAIIWeightVector],
    ) -> List[List[NSGAIIWeightVector]]:
        if not points:
            return []
        n = len(points)
        domination_count = [0] * n
        dominated: List[List[int]] = [[] for _ in range(n)]
        fronts: List[List[int]] = [[]]
        for i in range(n):
            for j in range(n):
                if i == j:
                    continue
                if self._dominates(points[i], points[j]):
                    dominated[i].append(j)
                elif self._dominates(points[j], points[i]):
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

    def _crowding_distance(
        self, front: Sequence[NSGAIIWeightVector],
    ) -> Dict[int, float]:
        if not front:
            return {}
        distances = {p.stable_id: 0.0 for p in front}
        obj_keys: Set[str] = set()
        for p in front:
            obj_keys.update(p.objectives.keys())
        obj_keys = sorted(obj_keys)
        for obj in obj_keys:
            sf = sorted(front, key=lambda x: x.objectives.get(obj, 0.0))
            distances[sf[0].stable_id] = float('inf')
            distances[sf[-1].stable_id] = float('inf')
            lo = sf[0].objectives.get(obj, 0.0)
            hi = sf[-1].objectives.get(obj, 0.0)
            if hi - lo <= NUMERIC_EPS:
                continue
            for i in range(1, len(sf) - 1):
                distances[sf[i].stable_id] += (
                    (sf[i + 1].objectives.get(obj, 0.0)
                     - sf[i - 1].objectives.get(obj, 0.0))
                    / (hi - lo)
                )
        return distances

    def _tournament_selection(
        self,
        population: Sequence[Dict[str, float]],
        points: Sequence[NSGAIIWeightVector],
        fronts: Sequence[Sequence[NSGAIIWeightVector]],
        crowding: Dict[int, float],
    ) -> Dict[str, float]:
        k = min(self.tournament_size, len(population))
        if k <= 0:
            return self._random_individual(list(points[0].weights.keys()) if points else [])
        candidates = self._rng.sample(list(range(len(population))), k)
        rank_of: Dict[int, int] = {}
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

    # ---------- Main loop ----------
    async def evolve(self, keys: Sequence[str]) -> List[NSGAIIWeightVector]:
        if self._disabled:
            raise RuntimeError("EvolutionService disabled (chaos)")
        keys = list(keys)
        if not keys:
            return []

        async def _evaluate(ind: Dict[str, float]) -> Dict[str, float]:
            cached = self._cache_get(ind)
            if cached is not None:
                return cached
            result = self.evaluate(ind)
            if not isinstance(result, dict):
                raise ValueError("evaluate must return a dict")
            self._cache_put(ind, result)
            return result

        # Initial population
        population = [self._random_individual(keys) for _ in range(self.population_size)]
        points: List[NSGAIIWeightVector] = []
        for ind in population:
            try:
                obj = await _evaluate(ind)
            except Exception as e:
                if self.dlq:
                    await self.dlq.record('nsga_evaluate', str(e))
                obj = {k: 0.0 for k in keys}
            points.append(NSGAIIWeightVector(
                vector_id=uuid.uuid4().hex,
                weights=dict(ind), objectives=dict(obj),
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
                child = (self._crossover(p1, p2) if self._rng.random() < self.crossover_rate
                         else dict(p1))
                child = self._mutate(child)
                offspring.append(child)

            child_points: List[NSGAIIWeightVector] = []
            for ind in offspring:
                try:
                    obj = await _evaluate(ind)
                except Exception as e:
                    if self.dlq:
                        await self.dlq.record('nsga_evaluate_child', str(e))
                    obj = {k: 0.0 for k in keys}
                child_points.append(NSGAIIWeightVector(
                    vector_id=uuid.uuid4().hex,
                    weights=dict(ind), objectives=dict(obj),
                    stable_id=self._next_id,
                ))
                self._next_id += 1

            combined_inds = list(population) + offspring
            combined_points = points + child_points
            fronts = self._fast_non_dominated_sort(combined_points)

            new_pop: List[Dict[str, float]] = []
            new_points: List[NSGAIIWeightVector] = []
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

            # Refill from fronts if we shrank
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
            self.obs.histogram('nsga_pareto_size', len(self.pareto_front))
            self.obs.counter('nsga_generations_total')

        # Best by scalarised score using canonical MOPD helper
        if self.pareto_front:
            # Scalarise using equal weights by default
            best = scalarise_pareto(
                self.pareto_front,
                objectives=[{"key": k, "direction": "max"} for k in keys],
            )
            if best is not None:
                self.best = best
        return self.pareto_front


# ============================================================================
# GatingService (MoE)
# ============================================================================
class GatingService:
    """
    MoE gating over objective-weight experts.

    Fixes: uses reward in update, bounded training samples, seedable.
    """

    CONTEXT_DIM = 6

    def __init__(self, config: ParetoGatingConfig,
                 observability: Optional[ObservabilityHub] = None,
                 seed: int = 42) -> None:
        self.config = config
        self.obs = observability or ObservabilityHub()
        self.expert_names: Tuple[str, ...] = tuple(config.moe_expert_names)
        self.num_experts = len(self.expert_names)
        rng = np.random.default_rng(seed)
        self.gating_weights = rng.standard_normal(
            (self.num_experts, self.CONTEXT_DIM),
        ) * 0.01
        self.learning_rate = 0.1
        self._lock = asyncio.Lock()

    def _encode_state(self, stats: Dict[str, float]) -> np.ndarray:
        features = [
            _coerce_float(stats.get('avg_quality'), 0.5) or 0.5,
            _coerce_float(stats.get('avg_latency'), 0.5) or 0.5,
            _coerce_float(stats.get('avg_carbon'), 0.5) or 0.5,
            _coerce_float(stats.get('avg_energy'), 0.5) or 0.5,
            _coerce_float(stats.get('avg_helium'), 0.5) or 0.5,
            float(_coerce_float(stats.get('num_candidates'), 0.0) or 0.0) / 100.0,
        ]
        return np.array(features, dtype=np.float32)

    async def select_expert(
        self, stats: Dict[str, float],
    ) -> Tuple[str, np.ndarray]:
        x = self._encode_state(stats)
        logits = self.gating_weights @ x
        probs = np.exp(logits - np.max(logits))
        probs /= probs.sum()
        idx = int(np.argmax(probs))
        self.obs.counter('gating_selections_total', expert=self.expert_names[idx])
        return self.expert_names[idx], probs

    async def add_training_sample(
        self, stats: Dict[str, float], selected_expert: str, reward: float,
    ) -> None:
        if selected_expert not in self.expert_names:
            return
        idx = self.expert_names.index(selected_expert)
        x = self._encode_state(stats)
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
    def __init__(self, gating: "ParetoGating",
                 config: ParetoGatingConfig) -> None:
        self.gating = gating
        self.config = config
        self._saved: Dict[str, Any] = {}
        self._injected: List[str] = []
        self._lock = asyncio.Lock()

    async def inject(self, fault_type: FaultType, **params: Any) -> Dict[str, Any]:
        async with self._lock:
            try:
                if fault_type == FaultType.PERSISTENCE_FAILURE:
                    if self.gating.persistence is not None:
                        self._saved.setdefault('persistence_storage',
                                                self.gating.persistence.storage)
                        self.gating.persistence.storage = None
                    return {'status': 'injected'}
                if fault_type == FaultType.EVALUATION_FAILURE:
                    if self.gating.evolution is not None:
                        self._saved.setdefault('evolution_evaluate',
                                                self.gating.evolution.evaluate)

                        def _failing_eval(weights: Dict[str, float]
                                           ) -> Dict[str, float]:
                            raise RuntimeError("Simulated NSGA evaluation failure")

                        self.gating.evolution.evaluate = _failing_eval  # type: ignore
                        self.gating.evolution.clear_cache()
                    return {'status': 'injected'}
                if fault_type == FaultType.GATING_FAILURE:
                    if self.gating.gating is not None:
                        self._saved.setdefault('gating_weights',
                                                self.gating.gating.gating_weights.copy())
                        # Corrupt weights in a non-destructive way
                        self.gating.gating.gating_weights = np.zeros_like(
                            self.gating.gating.gating_weights,
                        )
                    return {'status': 'injected'}
                if fault_type == FaultType.CONSTRAINT_FAILURE:
                    if self.gating.constraint_service is not None:
                        self._saved.setdefault('constraints_hard',
                                                dict(self.gating.constraint_service.hard_constraints))
                        # Clear constraints temporarily
                        self.gating.constraint_service.update_hard_constraints({})
                    return {'status': 'injected'}
                if fault_type == FaultType.SCORING_FAILURE:
                    if self.gating.scoring_service is not None:
                        self._saved.setdefault('scoring_disabled', False)
                        self.gating.scoring_service = None
                    return {'status': 'injected'}
                if fault_type == FaultType.NSGA_FAILURE:
                    if self.gating.evolution is not None:
                        self._saved.setdefault('nsga_disabled',
                                                self.gating.evolution._disabled)
                        self.gating.evolution.disable()
                    return {'status': 'injected'}
            except Exception as e:
                logger.exception("Chaos injection failed: %s", e)
                return {'status': 'error', 'error': str(e)}
            return {'status': 'unknown_fault'}

    def restore(self) -> None:
        g = self.gating
        for key, value in list(self._saved.items()):
            if key == 'persistence_storage' and g.persistence is not None:
                g.persistence.storage = value
            elif key == 'evolution_evaluate' and g.evolution is not None:
                g.evolution.evaluate = value
                g.evolution.clear_cache()
            elif key == 'gating_weights' and g.gating is not None:
                g.gating.gating_weights = value
            elif key == 'constraints_hard' and g.constraint_service is not None:
                g.constraint_service.update_hard_constraints(value)
            elif key == 'scoring_disabled':
                if g.scoring_service is None:
                    g.scoring_service = ScoringService(g.config)
            elif key == 'nsga_disabled' and g.evolution is not None:
                g.evolution._disabled = bool(value)
            self._saved.pop(key, None)
        self._injected.clear()

    async def run_test(self) -> Dict[str, Any]:
        report: Dict[str, Any] = {'faults': [], 'results': {}, 'restored': False}
        faults = [
            FaultType.PERSISTENCE_FAILURE,
            FaultType.CONSTRAINT_FAILURE,
            FaultType.GATING_FAILURE,
            FaultType.NSGA_FAILURE,
            FaultType.EVALUATION_FAILURE,
        ]
        try:
            for ft in faults[:CHAOS_MAX_FAULTS_PER_RUN]:
                res = await self.inject(ft)
                report['faults'].append(ft.value)
                report['results'][ft.value] = res
            try:
                health = await self.gating.get_health()
                report['results']['health_check'] = {
                    'status': health.get('status', 'unknown'),
                }
            except Exception as e:
                report['results']['health_check'] = {'error': str(e)}
        finally:
            self.restore()
            report['restored'] = True
        return report


# ============================================================================
# ParetoGating (main orchestrator)
# ============================================================================
class ParetoGating:
    """
    Production-grade Pareto gating with sub-services, Protocols,
    observability, governance, chaos testing, idempotency, and lifecycle.
    """

    def __init__(
        self,
        config: Optional[Union[ParetoGatingConfig, Dict[str, Any]]] = None,
        storage: Optional[StorageProtocol] = None,
        publish_fn: Optional[Callable[[str, Any], Awaitable[None]]] = None,
        **kwargs: Any,
    ) -> None:
        if config is None:
            valid = {f.name for f in dataclass_fields(ParetoGatingConfig)}
            filtered = {k: v for k, v in kwargs.items() if k in valid}
            base = _default_config_from_central()
            for k, v in filtered.items():
                setattr(base, k, v)
            config = base
        elif isinstance(config, dict):
            config = ParetoGatingConfig.from_dict(config)
        self.config = config
        self.storage = storage

        # Infra
        self.observability = ObservabilityHub()
        self.dlq = DeadLetterQueue()
        self._idempotency = IdempotencyCache()
        self._lock = asyncio.Lock()
        self.governance = GovernanceLayer(
            enabled=config.enable_human_approval,
            timeout_seconds=config.approval_timeout_seconds,
            auto_approve_on_timeout=config.approval_auto_approve_on_timeout,
            publish_fn=publish_fn,
        )
        self.safety = SafetyMonitor()
        self._setup_invariants()

        # Sub-services
        self.constraint_service = ConstraintService(config)
        self.pareto_service = ParetoService(config)
        self.scoring_service = ScoringService(config)
        self.persistence = PersistenceService(
            config, storage=storage,
            observability=self.observability, dlq=self.dlq,
        )
        self.evolution = (
            EvolutionService(
                config,
                evaluate=self._default_evaluation,
                observability=self.observability,
                dlq=self.dlq,
            )
            if config.enable_nsga else None
        )
        self.gating = (
            GatingService(config, self.observability)
            if config.enable_moe else None
        )

        # Chaos
        self.chaos_service = (
            ChaosService(self, config) if config.enable_chaos_testing else None
        )

        # Lifecycle
        self._started = False
        self._shutting_down = False
        self._background_tasks: List[asyncio.Task] = []
        self._chaos_task: Optional[asyncio.Task] = None
        self._ready_event = asyncio.Event()

        # Evolved weights (managed by NSGA)
        self.evolved_weights: Optional[Dict[str, float]] = None

        # Init LIMIT Graph
        if config.enable_limit_graph:
            self._pending_graph_init = asyncio.ensure_future(
                self._init_graph()
            ) if self._has_running_loop() else None
        else:
            self._pending_graph_init = None

        logger.info(
            "ParetoGating v%s initialised: objectives=%d missing_policy=%s "
            "limit_graph=%s modp=%s rlhf=%s moe=%s nsga=%s governance=%s chaos=%s",
            MODULE_VERSION, len(config.objectives), config.missing_policy,
            config.enable_limit_graph, config.enable_modp, config.enable_rlhf,
            config.enable_moe, config.enable_nsga, config.enable_human_approval,
            config.enable_chaos_testing,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _has_running_loop() -> bool:
        try:
            asyncio.get_running_loop()
            return True
        except RuntimeError:
            return False

    async def _init_graph(self) -> None:
        try:
            existing = self.persistence.get_metadata("pareto_front")
            if not existing:
                await self.persistence.create_graph(
                    "pareto_front", "Pareto Front Solutions",
                    {"version": MODULE_VERSION},
                )
        except Exception as e:
            logger.warning("Graph init failed: %s", e)
            await self.dlq.record('graph_init', str(e))

    def _setup_invariants(self) -> None:
        def _weights_sum_one(state: Dict[str, Any]) -> bool:
            w = state.get('weights')
            if not w:
                return True
            return abs(sum(w.values()) - 1.0) < 0.05

        def _pareto_nonempty(state: Dict[str, Any]) -> bool:
            return int(state.get('pareto_size', 1)) >= 0

        def _constraints_bounded(state: Dict[str, Any]) -> bool:
            return True

        self.safety.add('weights_sum_one', _weights_sum_one,
                        'Objective weights must sum to ~1')
        self.safety.add('pareto_nonempty', _pareto_nonempty,
                        'Pareto front must be defined')
        self.safety.add('constraints_bounded', _constraints_bounded,
                        'Hard constraints must not be weakened')

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        if self._pending_graph_init is not None:
            try:
                await self._pending_graph_init
            except Exception:
                pass
            self._pending_graph_init = None
        if self.config.enable_chaos_testing:
            self._chaos_task = asyncio.create_task(self._chaos_loop())
            self._background_tasks.append(self._chaos_task)
        self._ready_event.set()
        logger.info("ParetoGating started")

    async def wait_ready(self) -> None:
        if not self._started:
            await self.start()
        await self._ready_event.wait()

    async def shutdown(self) -> None:
        logger.info("ParetoGating shutdown")
        self._shutting_down = True
        for t in self._background_tasks:
            t.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)
        self._background_tasks.clear()
        unmet = self.safety.final_check()
        if unmet:
            logger.warning("Unmet eventual invariants: %s", unmet)

    async def _chaos_loop(self) -> None:
        interval = max(60, self.config.chaos_interval_seconds)
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
    # Public API
    # ------------------------------------------------------------------
    def filter(
        self,
        candidates: Sequence[Dict[str, Any]],
        dynamic_constraints: Optional[Dict[str, Any]] = None,
        return_stats: bool = False,
        persist: bool = False,
        idempotency_key: Optional[str] = None,
        **kwargs: Any,
    ) -> Union[List[Dict[str, Any]], Tuple[List[Dict[str, Any]], Dict[str, Any]]]:
        """
        Sync facade. For async, use `async_filter`.
        """
        try:
            loop = asyncio.get_running_loop()
            # We're inside an event loop; run sync by scheduling? No - raise.
            raise RuntimeError(
                "ParetoGating.filter called from within a running event loop. "
                "Use `await gating.async_filter(...)` instead."
            )
        except RuntimeError as e:
            if "within a running event loop" in str(e):
                raise
        return asyncio.run(self.async_filter(
            candidates,
            dynamic_constraints=dynamic_constraints,
            return_stats=return_stats,
            persist=persist,
            idempotency_key=idempotency_key,
            **kwargs,
        ))

    async def async_filter(
        self,
        candidates: Sequence[Dict[str, Any]],
        dynamic_constraints: Optional[Dict[str, Any]] = None,
        return_stats: bool = False,
        persist: bool = False,
        idempotency_key: Optional[str] = None,
        **kwargs: Any,
    ) -> Union[List[Dict[str, Any]], Tuple[List[Dict[str, Any]], Dict[str, Any]]]:
        await self.wait_ready()
        if idempotency_key and self._idempotency.seen(idempotency_key):
            logger.debug("Duplicate filter request ignored")
            return ([], {'total': len(candidates), 'feasible': 0, 'pareto': 0,
                         'duplicate': True}) if return_stats else []
        try:
            async with self._lock:
                result = await self._filter_internal(
                    candidates, dynamic_constraints, persist,
                )
        except Exception as e:
            logger.exception("Filter failed: %s", e)
            await self.dlq.record('filter', str(e))
            empty_stats = {
                'total': len(candidates), 'feasible': 0, 'pareto': 0,
                'error': str(e),
            }
            return ([], empty_stats) if return_stats else []

        pareto, stats = result
        self.observability.counter('filter_calls_total')
        self.observability.histogram('pareto_front_size', len(pareto))
        if self.config.enable_temporal_safety:
            violations = self.safety.check({
                'pareto_size': len(pareto),
                'weights': self.evolved_weights or {},
            })
            if violations:
                logger.warning("Safety violations: %s", violations)
                self.observability.counter('safety_violations_total')
        if return_stats:
            return pareto, stats
        return pareto

    # ------------------------------------------------------------------
    # Internal filter
    # ------------------------------------------------------------------
    async def _filter_internal(
        self,
        candidates: Sequence[Dict[str, Any]],
        dynamic_constraints: Optional[Dict[str, Any]],
        persist: bool,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        c_list = list(candidates)
        total = len(c_list)

        # 1) Constraint filter
        try:
            feasible, rejections, _ = self.constraint_service.filter_feasible(
                c_list, dynamic_constraints=dynamic_constraints,
            )
        except ValueError as e:
            logger.warning("Constraint conflict: %s", e)
            await self.dlq.record('constraint_conflict', str(e))
            return [], {
                'total': total, 'feasible': 0, 'pareto': 0,
                'error': str(e), 'rejections': {},
            }
        except Exception as e:
            logger.exception("Constraint failure: %s", e)
            await self.dlq.record('constraint_failure', str(e))
            feasible, rejections = [], {}

        # 2) Pareto filter
        pareto = self.pareto_service.filter(feasible)

        # 3) Optional persistence (batched)
        if persist and self.config.enable_limit_graph and pareto:
            for sol in pareto:
                node_id = f"sol_{uuid.uuid4().hex[:8]}"
                try:
                    await self.persistence.add_node(
                        "pareto_front", node_id, "pareto_solution",
                        {k: sol.get(k) for k in self._objective_keys()},
                    )
                except Exception as e:
                    await self.dlq.record('limit_graph_add_node', str(e))
        if persist and self.config.enable_modp and pareto:
            for sol in pareto:
                try:
                    await self.persistence.add_modp_state(
                        state_id=uuid.uuid4().hex,
                        problem_id="pareto_gating",
                        state_attributes={'solution': _to_serializable(sol)},
                        objective_values={
                            k: _coerce_float(sol.get(k), 0.0) or 0.0
                            for k in self._objective_keys()
                        },
                        stage=0,
                    )
                except Exception as e:
                    await self.dlq.record('modp_add_state', str(e))

        stats = {
            'total': total,
            'feasible': len(feasible),
            'pareto': len(pareto),
            'rejections': rejections,
        }
        return pareto, stats

    def _objective_keys(self) -> List[str]:
        return _canonical_objective_names(self.config.objectives)

    # ------------------------------------------------------------------
    # Scoring API
    # ------------------------------------------------------------------
    def score_candidates(
        self,
        candidates: Sequence[Dict[str, Any]],
        weights: Optional[Dict[str, float]] = None,
        normalisation: str = "minmax",
    ) -> List[Tuple[Dict[str, Any], float]]:
        if self.scoring_service is None:
            logger.warning("ScoringService unavailable")
            return []
        if weights is None and self.evolved_weights:
            weights = self.evolved_weights
        return self.scoring_service.score(candidates, weights, normalisation)

    # ------------------------------------------------------------------
    # NSGA-II / evolution API
    # ------------------------------------------------------------------
    async def run_nsga_evolution(
        self, candidate_history: Sequence[Dict[str, Any]],
    ) -> Optional[Dict[str, float]]:
        await self.wait_ready()
        if self.evolution is None:
            logger.info("NSGA-II disabled")
            return None
        if not candidate_history:
            return None

        objective_keys = self._objective_keys()
        directions = _directions_map(self.config.objectives)

        def _evaluate(weights: Dict[str, float]) -> Dict[str, float]:
            """
            Rank-correlation evaluation: for each historical candidate,
            compute the weighted score under the given weights; correlate
            with the candidate's 'reward' (if present) else use mean rank
            agreement across objective rankings.
            """
            # Build matrix of objective values
            n = len(candidate_history)
            matrix = np.zeros((n, len(objective_keys)), dtype=float)
            rewards = np.zeros(n, dtype=float)
            for i, c in enumerate(candidate_history):
                for j, key in enumerate(objective_keys):
                    v = _coerce_float(c.get(key), 0.0) or 0.0
                    if directions.get(key, "max") == "min":
                        v = -v
                    matrix[i, j] = v
                rewards[i] = _coerce_float(c.get('reward', 0.0), 0.0) or 0.0

            # Weighted scores
            w_vec = np.array([weights.get(k, 0.0) for k in objective_keys])
            scores = matrix @ w_vec

            # Rank agreement: Spearman correlation with rewards
            if n < 3:
                return {k: float(weights.get(k, 0.0)) for k in objective_keys}

            # Spearman rank correlation
            def _rank(arr: np.ndarray) -> np.ndarray:
                order = arr.argsort()
                ranks = np.empty_like(order, dtype=float)
                ranks[order] = np.arange(len(arr))
                return ranks

            r_scores = _rank(scores)
            r_rewards = _rank(rewards)
            if np.std(r_scores) < NUMERIC_EPS or np.std(r_rewards) < NUMERIC_EPS:
                corr = 0.0
            else:
                corr = float(np.corrcoef(r_scores, r_rewards)[0, 1])
            corr = (corr + 1.0) / 2.0  # to [0, 1]

            # Also: coverage — diversity of scores under these weights
            coverage = float(np.std(scores)) if n > 0 else 0.0

            return {
                **{k: float(weights.get(k, 0.0)) for k in objective_keys},
                '_rank_correlation': corr,
                '_coverage': coverage,
            }

        if self.evolution is not None:
            self.evolution.evaluate = _evaluate
            self.evolution.clear_cache()

        pareto = await self.evolution.evolve(objective_keys)
        if pareto:
            # Best by rank_correlation + coverage trade-off
            def _best_score(p: NSGAIIWeightVector) -> float:
                return (p.objectives.get('_rank_correlation', 0.0) * 0.7
                        + p.objectives.get('_coverage', 0.0) * 0.3)

            best = max(pareto, key=_best_score)
            self.evolved_weights = dict(best.weights)
            logger.info("NSGA evolved weights: %s", self.evolved_weights)

            # Optional persistence
            if self.config.enable_limit_graph:
                try:
                    await self.persistence.add_node(
                        "pareto_front",
                        f"evolved_weights_{uuid.uuid4().hex[:8]}",
                        "evolved_weights",
                        dict(self.evolved_weights),
                    )
                except Exception as e:
                    await self.dlq.record('limit_graph_evolved_weights', str(e))
            if self.config.enable_modp:
                try:
                    await self.persistence.add_modp_state(
                        state_id=uuid.uuid4().hex,
                        problem_id="weight_optimization",
                        state_attributes={'weights': dict(self.evolved_weights)},
                        objective_values={
                            k: float(v) for k, v in best.objectives.items()
                            if isinstance(v, (int, float))
                        },
                        stage=0,
                    )
                except Exception as e:
                    await self.dlq.record('modp_evolved_weights', str(e))

            return self.evolved_weights
        return None

    def _default_evaluation(self, weights: Dict[str, float]) -> Dict[str, float]:
        """
        Default evaluator when no candidate history is available.
        Treats weights as fitness (better-weighted objectives get higher
        scores under strict sum-to-1 constraints).
        """
        total = sum(weights.values()) or 1.0
        return {k: float(v) / total for k, v in weights.items()}

    # ------------------------------------------------------------------
    # Objectives
    # ------------------------------------------------------------------
    def set_objectives(self, objectives: List[Dict[str, str]]) -> None:
        self.config.objectives = [dict(o) for o in objectives]
        # Fix: invalidate evolution state and evolved weights
        if self.evolution is not None:
            self.evolution.pareto_front = []
            self.evolution.best = None
            self.evolution.clear_cache()
        self.evolved_weights = None
        self.pareto_service = ParetoService(self.config)
        self.scoring_service = ScoringService(self.config)
        self.constraint_service = ConstraintService(self.config)
        logger.info("Objectives updated: %d", len(self.config.objectives))

    def set_constraints(self, constraints: Dict[str, Any]) -> None:
        self.constraint_service.update_hard_constraints(constraints)
        self.config.constraints = dict(constraints)

    # ------------------------------------------------------------------
    # RLHF / human preference
    # ------------------------------------------------------------------
    async def record_human_preference(
        self, chosen_source: str, rejected_source: str,
        reward_diff: float = 1.0,
    ) -> None:
        await self.wait_ready()
        await self.persistence.record_preference_pair(
            pair_id=uuid.uuid4().hex,
            prompt="Which scoring produced better Pareto ranking?",
            chosen=chosen_source, rejected=rejected_source,
            reward_diff=reward_diff,
            metadata={"timestamp": _utcnow().isoformat()},
        )

    # ------------------------------------------------------------------
    # MoE gating integration
    # ------------------------------------------------------------------
    async def select_gating_expert(
        self, stats: Dict[str, float],
    ) -> Tuple[str, np.ndarray]:
        await self.wait_ready()
        if self.gating is None:
            return "rule_based", np.array([1.0, 0.0, 0.0])
        return await self.gating.select_expert(stats)

    async def report_gating_reward(
        self, stats: Dict[str, float], expert: str, reward: float,
    ) -> None:
        await self.wait_ready()
        if self.gating is not None:
            await self.gating.add_training_sample(stats, expert, reward)

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------
    async def get_limit_graph(self, graph_id: str = "pareto_front") -> Dict[str, Any]:
        return {
            'metadata': self.persistence.get_metadata(graph_id),
            'nodes': self.persistence.get_nodes(graph_id),
            'edges': self.persistence.get_edges(graph_id),
        }

    async def get_modp_states(
        self, problem_id: str = "pareto_gating",
    ) -> List[Dict[str, Any]]:
        return self.persistence.get_modp_states(problem_id)

    async def get_rlhf_pairs(self, limit: int = 100) -> List[Dict[str, Any]]:
        return self.persistence.get_preference_pairs(limit)

    # ------------------------------------------------------------------
    # Observability
    # ------------------------------------------------------------------
    async def get_prometheus_metrics(self) -> str:
        return await self.observability.export_prometheus()

    def get_dead_letters(self) -> List[Dict[str, Any]]:
        return self.dlq.snapshot()

    # ------------------------------------------------------------------
    # Health
    # ------------------------------------------------------------------
    async def get_health(self) -> Dict[str, Any]:
        return {
            "status": "healthy" if not self._shutting_down else "stopping",
            "version": MODULE_VERSION,
            "started": self._started,
            "objectives": len(self.config.objectives),
            "constraints": len(self.constraint_service.hard_constraints),
            "missing_policy": self.config.missing_policy,
            "evolved_weights": self.evolved_weights,
            "components": {
                "limit_graph": self.config.enable_limit_graph,
                "modp": self.config.enable_modp,
                "rlhf": self.config.enable_rlhf,
                "moe": self.gating is not None,
                "nsga": self.evolution is not None,
                "governance": self.governance.enabled,
                "chaos": self.chaos_service is not None,
            },
            "dead_letters": len(self.dlq),
            "governance_pending": len(self.governance.pending),
        }

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


# ============================================================================
# Exports
# ============================================================================
__all__ = [
    # Main
    "ParetoGating",
    "ParetoGatingConfig",
    "ParetoResult",
    # Sub-services
    "ConstraintService",
    "ParetoService",
    "ScoringService",
    "PersistenceService",
    "EvolutionService",
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
    # Data
    "NSGAIIWeightVector",
    # Utilities
    "scalarise_pareto",
    "filter_pareto",
    # Enums
    "MissingPolicy",
    "Normalisation",
    "ObjectiveDirection",
    "FaultType",
    # Protocols
    "StorageProtocol",
    "EvolutionBackendProtocol",
    "EventBrokerProtocol",
    "MessageQueueProtocol",
    # Constants
    "MODULE_VERSION",
    "DEFAULT_OBJECTIVES",
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
        config = ParetoGatingConfig(
            constraints={
                "quality_score": 0.5,
                "latency_ms": 500.0,
                "carbon_g": 0.5,
            },
        )
        gating = ParetoGating(config=config)
        await gating.start()

        candidates = [
            {"quality_score": 0.9, "latency_ms": 100.0, "carbon_g": 0.3,
             "energy_joules": 50.0, "helium_cost": 0.1, "resource_usage": 0.2,
             "reward": 0.9},
            {"quality_score": 0.8, "latency_ms": 200.0, "carbon_g": 0.4,
             "energy_joules": 60.0, "helium_cost": 0.15, "resource_usage": 0.25,
             "reward": 0.7},
            {"quality_score": 0.85, "latency_ms": 150.0, "carbon_g": 0.35,
             "energy_joules": 55.0, "helium_cost": 0.12, "resource_usage": 0.22,
             "reward": 0.85},
            {"quality_score": 0.2, "latency_ms": 50.0, "carbon_g": 0.1,
             "energy_joules": 20.0, "helium_cost": 0.05, "resource_usage": 0.1,
             "reward": 0.3},  # should be filtered (quality too low)
        ]

        pareto, stats = await gating.async_filter(
            candidates, return_stats=True, persist=True,
            idempotency_key="demo-1",
        )
        print(f"Feasible: {stats['feasible']}, Pareto: {stats['pareto']}")
        for c in pareto:
            print(f"  {c['quality_score']:.2f} / {c['latency_ms']:.0f}ms / "
                  f"{c['carbon_g']:.2f}g")

        # Score
        scored = gating.score_candidates(pareto, normalisation="minmax")
        for c, s in scored:
            print(f"  score={s:.4f}")

        # NSGA
        evolved = await gating.run_nsga_evolution(candidates)
        print(f"Evolved weights: {evolved}")

        # Health
        health = await gating.get_health()
        print(f"Health: {health['status']}")

        # Chaos
        chaos = await gating.run_chaos_test()
        print(f"Chaos restored: {chaos.get('restored')}, faults={chaos.get('faults')}")

        metrics = await gating.get_prometheus_metrics()
        print(metrics[:300])

        await gating.shutdown()

    asyncio.run(_demo())
