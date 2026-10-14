#!/usr/bin/env python3
# coevolution/engine.py
"""
Human-AI Co-Evolution Engine v5.0.0
====================================

Production-grade co-evolution engine with sub-service architecture,
Pydantic v1/v2 compatibility, shared ObservabilityHub, real human-in-the-loop
GovernanceLayer, non-destructive chaos testing, unified MOPD scalarisation,
and full lifecycle management.

Highlights
----------
* Pydantic v1 AND v2 supported through a runtime compatibility layer.
* Sub-services: Feedback, UserModel, Sentiment, Policy, Simulation,
  Collaboration, Persistence, Federated.
* `ObservabilityHub` — Prometheus-ready counters/gauges/histograms.
* `GovernanceLayer` — human-in-the-loop with queue topic, audit trail, and
  timeout policy.
* `ChaosService` — non-destructive fault injection with full save/restore.
* Unified `scalarise_pareto()` — a single source of truth for MOPD decisions.
* Full lifecycle — `start()` / `wait_ready()` / `shutdown()`.
* Idempotency keys with bounded LRU cache and dead-letter queue.
* Temporal safety invariants (ALWAYS + EVENTUALLY).
* Bounded caches, consistent lock discipline, rich health checks.

Backward compatibility
----------------------
`HumanAICoEvolutionEngine` retains a compatible public API with v4.x while
delegating to the new sub-services internally.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import os
import re
import time
import uuid
from collections import OrderedDict, defaultdict, deque
from dataclasses import dataclass, field, fields as dataclass_fields
from datetime import datetime, timezone
from enum import Enum
from typing import (
    Any, Awaitable, Callable, Deque, Dict, FrozenSet, List, Optional,
    Protocol, Sequence, Tuple, runtime_checkable,
)

import numpy as np

# ---------------------------------------------------------------------------
# Optional third-party imports
# ---------------------------------------------------------------------------
try:
    import aiofiles
    AIOFILES_AVAILABLE = True
except ImportError:
    aiofiles = None  # type: ignore
    AIOFILES_AVAILABLE = False

try:
    from transformers import pipeline as hf_pipeline
    TRANSFORMERS_AVAILABLE = True
except ImportError:
    hf_pipeline = None  # type: ignore
    TRANSFORMERS_AVAILABLE = False

try:
    from sklearn.cluster import MiniBatchKMeans
    SKLEARN_AVAILABLE = True
except ImportError:
    MiniBatchKMeans = None  # type: ignore
    SKLEARN_AVAILABLE = False

try:
    from prometheus_client import (  # type: ignore
        Counter, Gauge, Histogram, generate_latest, start_http_server,
    )
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

# ============================================================================
# Pydantic compatibility layer (v1 / v2 / dataclass fallback)
# ============================================================================
PYDANTIC_AVAILABLE = False
PYDANTIC_V2 = False
PYDANTIC_V1 = False

BaseModel: Any = object
Field: Any = lambda *a, **k: None  # noqa: E731
model_validator: Any = None
field_validator: Any = None
root_validator: Any = None
validator: Any = None
ConfigDict: Any = None
ValidationError: Any = Exception
BaseSettings: Any = object
SettingsConfigDict: Any = None

try:
    import pydantic as _pyd  # type: ignore
    _ver = getattr(_pyd, 'VERSION', '0') or '0'
    _maj = int(str(_ver).split('.')[0])

    if _maj >= 2:
        from pydantic import (  # type: ignore
            BaseModel, ConfigDict, Field, ValidationError,
            field_validator, model_validator,
        )
        PYDANTIC_AVAILABLE = True
        PYDANTIC_V2 = True
    elif _maj == 1:
        from pydantic import (  # type: ignore
            BaseModel, Field, ValidationError, root_validator, validator,
        )
        # shim names for v2-only symbols
        model_validator = None
        field_validator = None
        ConfigDict = None
        PYDANTIC_AVAILABLE = True
        PYDANTIC_V1 = True

    try:
        from pydantic_settings import (  # type: ignore
            BaseSettings, SettingsConfigDict,
        )
        SETTINGS_AVAILABLE = True
    except ImportError:
        try:
            from pydantic import BaseSettings  # type: ignore  # v1 only
            SETTINGS_AVAILABLE = True
            SettingsConfigDict = None
        except ImportError:
            SETTINGS_AVAILABLE = False
except Exception:
    PYDANTIC_AVAILABLE = False
    PYDANTIC_V1 = False
    PYDANTIC_V2 = False
    SETTINGS_AVAILABLE = False

logger = logging.getLogger(__name__)


# ============================================================================
# Named Constants
# ============================================================================
ENGINE_VERSION = "5.0.0"
STATE_SCHEMA_VERSION = 5

DEFAULT_FEEDBACK_HISTORY_LIMIT = 10_000
DEFAULT_POLICY_LIMIT = 1_000
DEFAULT_DECISION_LIMIT = 100
DEFAULT_PARETO_HISTORY_LIMIT = 1_000
MAX_APPROVAL_AUDIT = 1_000
MAX_DEAD_LETTERS = 1_000
MAX_IDEMPOTENCY_CACHE = 4_096
MAX_APPROVAL_PENDING = 512
APPROVAL_DEFAULT_TIMEOUT = 60.0
CHAOS_MAX_FAULTS_PER_RUN = 6

MOPD_OBJECTIVE_NAMES: Tuple[str, ...] = (
    'carbon_impact',
    'helium_impact',
    'energy_impact',
    'cost',
    'user_satisfaction',
)
MOPD_HIGHER_IS_BETTER: FrozenSet[str] = frozenset({
    'user_satisfaction',
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
    if isinstance(obj, dict):
        return {str(k): _to_serializable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set, frozenset)):
        return [_to_serializable(v) for v in obj]
    if isinstance(obj, deque):
        return [_to_serializable(v) for v in obj]
    if hasattr(obj, 'model_dump') and callable(obj.model_dump):  # pydantic v2
        try:
            return _to_serializable(obj.model_dump())
        except Exception:
            pass
    if hasattr(obj, 'dict') and callable(obj.dict):  # pydantic v1
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
class TelemetryProtocol(Protocol):
    async def increment(self, metric: str, value: float = 1.0,
                        tags: Optional[Dict[str, str]] = None) -> None: ...
    async def gauge(self, metric: str, value: float,
                    tags: Optional[Dict[str, str]] = None) -> None: ...
    async def histogram(self, metric: str, value: float,
                        tags: Optional[Dict[str, str]] = None) -> None: ...


@runtime_checkable
class MessageQueueProtocol(Protocol):
    async def publish(self, topic: str, payload: Any) -> None: ...


@runtime_checkable
class EventBrokerProtocol(Protocol):
    def subscribe(self, event_type: str,
                  handler: Callable[[Any], Awaitable[None]]) -> None: ...
    def publish(self, topic: str, payload: Any) -> Any: ...


@runtime_checkable
class SentimentAnalyzerProtocol(Protocol):
    async def analyze_sentiment(self, text: str) -> Dict[str, Any]: ...


@runtime_checkable
class PersistenceBackendProtocol(Protocol):
    async def save(self, key: str, data: Dict[str, Any]) -> bool: ...
    async def load(self, key: str) -> Optional[Dict[str, Any]]: ...
    async def delete(self, key: str) -> bool: ...


# ============================================================================
# ObservabilityHub
# ============================================================================
class ObservabilityHub:
    """Prometheus-ready metrics hub."""

    def __init__(self) -> None:
        self._counters: Dict[str, float] = {}
        self._gauges: Dict[str, float] = {}
        self._histograms: Dict[str, Deque[float]] = {}
        self._lock = asyncio.Lock()

    @staticmethod
    def _key(name: str, labels: Optional[Dict[str, str]]) -> str:
        if not labels:
            return name
        tag_str = ",".join(f'{k}="{v}"' for k, v in sorted(labels.items()))
        return f"{name}{{{tag_str}}}"

    def counter(self, name: str, value: float = 1.0, **labels: str) -> None:
        key = self._key(name, labels or None)
        self._counters[key] = self._counters.get(key, 0.0) + float(value)

    def gauge(self, name: str, value: float, **labels: str) -> None:
        key = self._key(name, labels or None)
        self._gauges[key] = float(value)

    def histogram(self, name: str, value: float, **labels: str) -> None:
        key = self._key(name, labels or None)
        if key not in self._histograms:
            self._histograms[key] = deque(maxlen=1024)
        self._histograms[key].append(float(value))

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
    SENTIMENT_MODEL_DOWN = "sentiment_model_down"
    PERSISTENCE_FAILURE = "persistence_failure"
    TELEMETRY_FAILURE = "telemetry_failure"
    CLUSTERING_FAILURE = "clustering_failure"
    SIMULATION_FAILURE = "simulation_failure"
    USER_MODEL_CORRUPTION = "user_model_corruption"
    GOVERNANCE_TIMEOUT = "governance_timeout"


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
    """Human-in-the-loop approval with queue, audit, and timeout policy."""

    def __init__(
        self,
        enabled: bool = False,
        timeout_seconds: float = APPROVAL_DEFAULT_TIMEOUT,
        auto_approve_on_timeout: bool = False,
        publish_fn: Optional[Callable[[str, Any], Awaitable[None]]] = None,
    ) -> None:
        self.enabled = enabled
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
            result = ApprovalResult(
                request_id='auto',
                status=ApprovalStatus.AUTO_APPROVED,
                reason='approval_disabled',
                decided_at=datetime.now(timezone.utc),
            )
            self._audit(context, result)
            return result

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
            if len(self.pending) > MAX_APPROVAL_PENDING:
                # Evict the oldest pending
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
            status = (ApprovalStatus.AUTO_APPROVED if self.auto_approve_on_timeout
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
# Unified MOPD scalarisation
# ============================================================================
def _point_objective_vector(point: Any) -> List[float]:
    """Canonical lower-is-better vector."""
    method = getattr(point, 'objective_vector', None)
    if callable(method):
        try:
            vec = method()
            if vec is not None and len(vec) == len(MOPD_OBJECTIVE_NAMES):
                return [float(v) for v in vec]
        except Exception:
            pass
    out: List[float] = []
    for name in MOPD_OBJECTIVE_NAMES:
        val = float(getattr(point, name, 0.0))
        if name in MOPD_HIGHER_IS_BETTER:
            val = -val
        out.append(val)
    return out


def scalarise_pareto(
    points: Sequence[Any],
    weights: Optional[Dict[str, float]] = None,
    objective_names: Optional[Sequence[str]] = None,
    higher_is_better: Optional[FrozenSet[str]] = None,
) -> Optional[Any]:
    """
    Scalarise Pareto points using min-max normalisation, returning the best
    plan. Every plan is stamped with `.scalarised_score` in the range [0, 1].
    """
    if not points:
        return None
    names = list(objective_names or MOPD_OBJECTIVE_NAMES)
    w = dict(weights or {name: 1.0 / len(names) for name in names})

    vectors = [_point_objective_vector(p) for p in points]
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
            setattr(p, 'scalarised_score', score)
        except Exception:
            pass
        if score > best_score:
            best_score = score
            best = p
    return best


def filter_pareto(points: Sequence[Any]) -> List[Any]:
    """Return non-dominated points (all objectives in canonical form)."""
    if not points:
        return []
    vecs = [_point_objective_vector(p) for p in points]
    arr = np.array(vecs, dtype=float)
    n = len(points)
    is_dominated = np.zeros(n, dtype=bool)
    for i in range(n):
        if is_dominated[i]:
            continue
        for j in range(n):
            if i == j or is_dominated[i]:
                continue
            if np.all(arr[j] <= arr[i]) and np.any(arr[j] < arr[i]):
                is_dominated[i] = True
    return [points[i] for i in range(n) if not is_dominated[i]]


# ============================================================================
# Idempotency cache (LRU)
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
# Configuration (dataclass-based; Pydantic-compatible surface)
# ============================================================================
@dataclass
class MOPDConfig:
    enabled: bool = True
    objective_weights: Dict[str, float] = field(default_factory=lambda: {
        'carbon_impact': 0.30,
        'helium_impact': 0.20,
        'energy_impact': 0.20,
        'cost': 0.15,
        'user_satisfaction': 0.15,
    })
    grid_resolution: int = 5
    enable_cost_benefit: bool = True
    enable_predictive: bool = True

    def __post_init__(self) -> None:
        total = sum(self.objective_weights.values())
        if total <= 0:
            raise ValueError("MOPD objective_weights must sum to a positive value")
        if abs(total - 1.0) > 1e-6:
            logger.warning("MOPD weights sum to %.4f; normalising.", total)
            for k in list(self.objective_weights):
                self.objective_weights[k] /= total
        # Key check
        extra = set(self.objective_weights) - set(MOPD_OBJECTIVE_NAMES)
        missing = set(MOPD_OBJECTIVE_NAMES) - set(self.objective_weights)
        if extra:
            raise ValueError(f"objective_weights has unknown keys {sorted(extra)}")
        if missing:
            raise ValueError(f"objective_weights missing keys {sorted(missing)}")


@dataclass
class FederatedConfig:
    enabled: bool = False
    server_url: Optional[str] = None
    sparsity_ratio: float = 0.1
    privacy_epsilon: float = 1.0
    privacy_delta: float = 1e-5
    privacy_clip_norm: float = 1.0
    sync_interval_seconds: int = 3600
    request_timeout_seconds: float = 30.0


@dataclass
class SimulationConfig:
    simulation_steps: int = 10
    num_scenarios: int = 5
    confidence_level: float = 0.95
    carbon_noise_level: float = 0.02
    helium_noise_level: float = 0.02
    energy_noise_level: float = 0.02
    enable_simulation_coupling: bool = True
    renewable_influence_factor: float = 0.3

    def __post_init__(self) -> None:
        if not (0.8 <= self.confidence_level <= 0.99):
            raise ValueError("confidence_level must be in [0.8, 0.99]")


@dataclass
class TelemetryConfig:
    telemetry_export_interval: int = 60
    telemetry_enable_prometheus: bool = False
    telemetry_prometheus_port: int = 8000


@dataclass
class ClusteringConfig:
    enable_user_clustering: bool = True
    clustering_update_interval: int = 3600


@dataclass
class ApprovalConfig:
    enabled: bool = False
    timeout_seconds: float = APPROVAL_DEFAULT_TIMEOUT
    auto_approve_on_timeout: bool = False
    require_for_low_confidence_below: float = 0.5


@dataclass
class ChaosConfig:
    enabled: bool = False
    interval_seconds: int = 3600
    auto_restore: bool = True


@dataclass
class CoEvolutionConfig:
    learning_rate: float = 0.01
    exploration_rate: float = 0.1

    feedback_history_limit: int = DEFAULT_FEEDBACK_HISTORY_LIMIT
    user_model_limit: int = 1_000
    policy_suggestions_limit: int = DEFAULT_POLICY_LIMIT
    collaborative_decisions_limit: int = DEFAULT_DECISION_LIMIT
    pareto_history_limit: int = DEFAULT_PARETO_HISTORY_LIMIT

    default_consensus_threshold: float = 0.7
    sentiment_model_name: Optional[str] = None
    sentiment_model_device: Optional[str] = None

    persistence_path: str = "co_evolution_state.json"
    persistence_auto_save_interval: int = 60

    enable_idempotency: bool = True
    enable_temporal_safety: bool = True
    enable_chaos_testing: bool = False

    simulation: SimulationConfig = field(default_factory=SimulationConfig)
    telemetry: TelemetryConfig = field(default_factory=TelemetryConfig)
    clustering: ClusteringConfig = field(default_factory=ClusteringConfig)
    mopd: MOPDConfig = field(default_factory=MOPDConfig)
    federated: FederatedConfig = field(default_factory=FederatedConfig)
    approval: ApprovalConfig = field(default_factory=ApprovalConfig)
    chaos: ChaosConfig = field(default_factory=ChaosConfig)

    def __post_init__(self) -> None:
        if not 0 <= self.learning_rate <= 1:
            raise ValueError("learning_rate must be in [0, 1]")
        if not 0 <= self.exploration_rate <= 1:
            raise ValueError("exploration_rate must be in [0, 1]")

    def to_dict(self) -> Dict[str, Any]:
        return _to_serializable(self)


# ============================================================================
# MOPD data model
# ============================================================================
@dataclass
class MOPDPoint:
    policy_id: str
    actions: List[str]
    carbon_impact: float
    helium_impact: float
    energy_impact: float
    cost: float
    user_satisfaction: float
    scalarised_score: float = 0.0
    explanation: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            'policy_id': self.policy_id,
            'actions': list(self.actions),
            'carbon_impact': self.carbon_impact,
            'helium_impact': self.helium_impact,
            'energy_impact': self.energy_impact,
            'cost': self.cost,
            'user_satisfaction': self.user_satisfaction,
            'scalarised_score': self.scalarised_score,
            'explanation': self.explanation,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MOPDPoint":
        return cls(**data)

    def objective_vector(self) -> List[float]:
        vec: List[float] = []
        for name in MOPD_OBJECTIVE_NAMES:
            val = float(getattr(self, name, 0.0))
            if name in MOPD_HIGHER_IS_BETTER:
                val = -val
            vec.append(val)
        return vec


# ============================================================================
# Data models (plain dataclasses to avoid Pydantic coupling)
# ============================================================================
@dataclass
class FeedbackEntry:
    user_id: str
    policy_id: str
    feedback: Dict[str, Any]
    sentiment: Optional[Dict[str, Any]] = None
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> Dict[str, Any]:
        return {
            'user_id': self.user_id,
            'policy_id': self.policy_id,
            'feedback': _to_serializable(self.feedback),
            'sentiment': _to_serializable(self.sentiment),
            'timestamp': self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "FeedbackEntry":
        return cls(
            user_id=data['user_id'],
            policy_id=data['policy_id'],
            feedback=dict(data.get('feedback', {})),
            sentiment=data.get('sentiment'),
            timestamp=data.get('timestamp') or datetime.now(timezone.utc).isoformat(),
        )


@dataclass
class UserModel:
    preferences: Dict[str, float] = field(default_factory=dict)
    history: List[Dict[str, Any]] = field(default_factory=list)
    trust_score: float = 0.5
    feedback_count: int = 0
    sentiment_score: float = 0.0
    engagement_level: float = 0.5
    preference_timeline: List[Dict[str, Any]] = field(default_factory=list)
    last_active: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    cluster_id: Optional[int] = None

    def clamp(self) -> None:
        self.trust_score = max(0.0, min(1.0, self.trust_score))
        self.engagement_level = max(0.0, min(1.0, self.engagement_level))
        self.sentiment_score = max(-1.0, min(1.0, self.sentiment_score))

    def to_dict(self) -> Dict[str, Any]:
        return {
            'preferences': dict(self.preferences),
            'history': [_to_serializable(h) for h in self.history],
            'trust_score': self.trust_score,
            'feedback_count': self.feedback_count,
            'sentiment_score': self.sentiment_score,
            'engagement_level': self.engagement_level,
            'preference_timeline': [_to_serializable(p) for p in self.preference_timeline],
            'last_active': self.last_active,
            'cluster_id': self.cluster_id,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "UserModel":
        model = cls(
            preferences=dict(data.get('preferences', {})),
            history=list(data.get('history', [])),
            trust_score=float(data.get('trust_score', 0.5)),
            feedback_count=int(data.get('feedback_count', 0)),
            sentiment_score=float(data.get('sentiment_score', 0.0)),
            engagement_level=float(data.get('engagement_level', 0.5)),
            preference_timeline=list(data.get('preference_timeline', [])),
            last_active=data.get('last_active') or datetime.now(timezone.utc).isoformat(),
            cluster_id=data.get('cluster_id'),
        )
        model.clamp()
        return model


@dataclass
class PolicySuggestion:
    timestamp: str
    context: Dict[str, Any]
    actions: List[str]
    rationale: List[str]
    expected_impact: Dict[str, float]
    explanations: List[str]
    confidence: float = 0.5
    personalized: bool = False
    alternative_actions: List[Dict[str, Any]] = field(default_factory=list)
    pareto_front: Optional[List[Dict[str, Any]]] = None
    best_plan: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            'timestamp': self.timestamp,
            'context': _to_serializable(self.context),
            'actions': list(self.actions),
            'rationale': list(self.rationale),
            'expected_impact': dict(self.expected_impact),
            'explanations': list(self.explanations),
            'confidence': self.confidence,
            'personalized': self.personalized,
            'alternative_actions': [_to_serializable(a) for a in self.alternative_actions],
            'pareto_front': [_to_serializable(p) for p in (self.pareto_front or [])],
            'best_plan': _to_serializable(self.best_plan) if self.best_plan else None,
        }


@dataclass
class CollaborativeDecision:
    selected_option: Dict[str, Any]
    scores: List[float]
    participants: List[str]
    consensus_reached: bool
    consensus_score: float = 0.0
    vote_distribution: Dict[str, Any] = field(default_factory=dict)
    consensus_threshold: Optional[float] = None
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    disagreeing_users: Optional[List[str]] = None
    disagreement_analysis: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            'selected_option': _to_serializable(self.selected_option),
            'scores': list(self.scores),
            'participants': list(self.participants),
            'consensus_reached': self.consensus_reached,
            'consensus_score': self.consensus_score,
            'vote_distribution': _to_serializable(self.vote_distribution),
            'consensus_threshold': self.consensus_threshold,
            'timestamp': self.timestamp,
            'disagreeing_users': list(self.disagreeing_users or []),
            'disagreement_analysis': _to_serializable(self.disagreement_analysis or {}),
        }


@dataclass
class SimulationResult:
    carbon_trajectory: List[float]
    helium_trajectory: List[float]
    energy_trajectory: List[float]
    sustainability_score: float
    confidence_intervals: Dict[str, Tuple[float, float]] = field(default_factory=dict)
    trajectory_confidence_intervals: Dict[str, List[Tuple[float, float]]] = field(default_factory=dict)
    probabilities: Dict[str, float] = field(default_factory=dict)
    scenario_metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return _to_serializable(self)


# ============================================================================
# Sentiment Analyzer (sub-service, non-destructive to chaos)
# ============================================================================
class SentimentService:
    """Sentiment analyzer with transformer model or rule-based fallback."""

    def __init__(
        self,
        model_name: Optional[str] = None,
        device: Optional[str] = None,
        observability: Optional[ObservabilityHub] = None,
    ) -> None:
        self.model_name = model_name
        self.device = device
        self.pipeline: Optional[Any] = None
        self.observability = observability or ObservabilityHub()
        self._lock = asyncio.Lock()
        if model_name and TRANSFORMERS_AVAILABLE:
            try:
                self.pipeline = hf_pipeline(
                    "sentiment-analysis",
                    model=model_name,
                    device=device,
                    truncation=True,
                )
                logger.info("Loaded sentiment model: %s", model_name)
            except Exception as e:
                logger.warning("Failed to load sentiment model: %s; using rule-based", e)

        self._positive = {
            'excellent': 1.0, 'great': 0.8, 'good': 0.6, 'nice': 0.5,
            'happy': 0.7, 'satisfied': 0.8, 'impressed': 0.9, 'love': 1.0,
            'amazing': 1.0, 'perfect': 1.0, 'awesome': 0.9, 'fantastic': 1.0,
            'helpful': 0.6, 'useful': 0.5, 'improved': 0.7, 'better': 0.6,
            'efficient': 0.7, 'sustainable': 0.8, 'innovative': 0.9,
        }
        self._negative = {
            'bad': -0.6, 'terrible': -1.0, 'awful': -0.9, 'horrible': -1.0,
            'sad': -0.5, 'disappointed': -0.7, 'frustrated': -0.8, 'angry': -0.9,
            'useless': -0.7, 'broken': -0.8, 'confusing': -0.5, 'slow': -0.5,
            'worse': -0.6, 'issue': -0.4, 'problem': -0.5, 'error': -0.6,
            'wasteful': -0.7, 'inefficient': -0.6, 'unsustainable': -0.9,
        }
        self._emotions = {
            'joy': ['happy', 'glad', 'delighted', 'pleased', 'joy'],
            'trust': ['trust', 'confident', 'reliable', 'sure', 'dependable'],
            'fear': ['worry', 'afraid', 'scared', 'anxious', 'nervous', 'concern'],
            'surprise': ['surprised', 'amazed', 'astonished', 'shocked'],
            'sadness': ['sad', 'depressed', 'unhappy', 'miserable', 'disappointed'],
            'anger': ['angry', 'furious', 'outraged', 'irritated', 'annoyed'],
            'anticipation': ['expect', 'anticipate', 'hope', 'eager'],
        }
        self._intensifiers = ['very', 'really', 'extremely', 'absolutely',
                              'completely', 'totally', 'highly', 'incredibly']
        self._downtoners = ['somewhat', 'slightly', 'a bit', 'a little',
                            'fairly', 'moderately', 'kind of', 'rather']
        self._negations = ['not', 'never', 'none', 'nobody', 'no', 'neither',
                           'nor', 'hardly', 'scarcely', 'barely']

    async def analyze_sentiment(self, text: str) -> Dict[str, Any]:
        if not text or not text.strip():
            return {
                'score': 0.0, 'confidence': 0.0, 'sentiment': 'neutral',
                'emotions': {}, 'key_phrases': [],
            }

        if self.pipeline is not None:
            try:
                async with self._lock:
                    result = await asyncio.to_thread(self.pipeline, text[:512])
                result = result[0]
                label = str(result.get('label', '')).upper()
                score = float(result.get('score', 0.0))
                signed = score if label == 'POSITIVE' else (-score if label == 'NEGATIVE' else 0.0)
                return {
                    'score': signed,
                    'confidence': score,
                    'sentiment': label.lower(),
                    'emotions': {},
                    'key_phrases': [],
                }
            except Exception as e:
                logger.warning("Transformer sentiment failed: %s; fallback", e)

        return self._rule_based(text)

    def _rule_based(self, text: str) -> Dict[str, Any]:
        text_lower = text.lower()
        words = text_lower.split()
        score = 0.0
        total_weight = 0.0
        negate_next = False

        for i, word in enumerate(words):
            if word in self._negations:
                negate_next = True
                continue
            multiplier = 1.0
            if i > 0 and words[i - 1] in self._intensifiers:
                multiplier = 1.5
            elif i > 0 and words[i - 1] in self._downtoners:
                multiplier = 0.6
            for phrase, val in self._positive.items():
                if ' ' in phrase and phrase in text_lower:
                    score += val * multiplier
                    total_weight += 1.0
            for phrase, val in self._negative.items():
                if ' ' in phrase and phrase in text_lower:
                    score += val * multiplier
                    total_weight += 1.0
            val = self._positive.get(word)
            if val is None:
                val = self._negative.get(word)
            if val is not None:
                v = val * multiplier
                if negate_next:
                    v = -v
                    negate_next = False
                score += v
                total_weight += 1.0

        score = score / total_weight if total_weight > 0 else 0.0
        score = max(-1.0, min(1.0, score))
        sentiment = 'positive' if score > 0.2 else 'negative' if score < -0.2 else 'neutral'
        return {
            'score': score,
            'confidence': min(0.95, total_weight / 10.0),
            'sentiment': sentiment,
            'emotions': self._detect_emotions(text_lower),
            'key_phrases': self._extract_key_phrases(text),
        }

    def _detect_emotions(self, text_lower: str) -> Dict[str, float]:
        emotions: Dict[str, float] = {}
        for emotion, keywords in self._emotions.items():
            cnt = sum(1 for k in keywords if k in text_lower)
            if cnt > 0:
                emotions[emotion] = min(1.0, cnt / 3.0)
        if emotions:
            m = max(emotions.values())
            if m > 0:
                emotions = {k: v / m for k, v in emotions.items()}
        return emotions

    def _extract_key_phrases(self, text: str) -> List[str]:
        phrases: List[str] = []
        quoted = re.findall(r'"([^"]*)"', text)
        if quoted:
            phrases.extend(quoted)
        for ind in ('especially', 'particularly', 'specifically', 'the issue is',
                    'the problem is', 'suggestion', 'recommendation'):
            if ind in text.lower():
                parts = text.lower().split(ind)
                if len(parts) > 1:
                    p = parts[1].strip()
                    if len(p) > 10:
                        phrases.append(p[:100])
        return list(set(phrases))[:5]


# ============================================================================
# Persistence service
# ============================================================================
class PersistenceService:
    """Threaded JSON persistence with schema versioning."""

    def __init__(self, path: str, observability: Optional[ObservabilityHub] = None) -> None:
        self.path = path
        self.observability = observability or ObservabilityHub()
        self._lock = asyncio.Lock()

    async def save(self, data: Dict[str, Any]) -> bool:
        payload = {
            'version': ENGINE_VERSION,
            'schema': STATE_SCHEMA_VERSION,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'data': _to_serializable(data),
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
                self.observability.counter('persistence_saves_total')
                return True
            except Exception as e:
                logger.error("Failed to save state: %s", e)
                self.observability.counter('persistence_failures_total')
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
                        "State schema mismatch: file=%s, expected=%s",
                        schema, STATE_SCHEMA_VERSION,
                    )
                data = payload.get('data') or {}
                if not isinstance(data, dict):
                    return None
                return data
            except Exception as e:
                logger.error("Failed to load state: %s", e)
                return None

    async def delete(self) -> bool:
        async with self._lock:
            if os.path.exists(self.path):
                try:
                    await asyncio.to_thread(os.remove, self.path)
                    return True
                except Exception as e:
                    logger.error("Failed to delete state: %s", e)
                    return False
            return False


# ============================================================================
# Sub-services
# ============================================================================
class FeedbackService:
    """Records feedback and updates user models."""

    def __init__(self, engine: "HumanAICoEvolutionEngine") -> None:
        self.engine = engine
        self.sentiment = engine.sentiment_service
        self.observability = engine.observability

    async def record(
        self,
        user_id: str,
        policy_id: str,
        feedback: Dict[str, Any],
        idempotency_key: Optional[str] = None,
    ) -> FeedbackEntry:
        engine = self.engine
        if engine._idempotency.seen(idempotency_key):
            logger.debug("Duplicate feedback ignored")
            raise ValueError("duplicate_feedback")

        comment = feedback.get('comment', '')
        sentiment = await self.sentiment.analyze_sentiment(comment) if comment else None

        entry = FeedbackEntry(
            user_id=user_id,
            policy_id=policy_id,
            feedback=feedback,
            sentiment=sentiment,
            timestamp=datetime.now(timezone.utc).isoformat(),
        )

        async with engine._lock:
            engine.feedback_history.append(entry)
            user_model = engine.user_models.get(user_id)
            if user_model is None:
                user_model = UserModel()
                engine.user_models[user_id] = user_model

            # Preferences
            if 'preferences' in feedback and isinstance(feedback['preferences'], dict):
                for key, value in feedback['preferences'].items():
                    try:
                        v = float(value)
                    except (TypeError, ValueError):
                        continue
                    user_model.preferences[key] = v
                    user_model.preference_timeline.append({
                        'timestamp': entry.timestamp,
                        'key': key,
                        'value': v,
                    })

            user_model.history.append(entry.to_dict())
            user_model.feedback_count += 1
            user_model.last_active = entry.timestamp

            # Trust update via Bayesian-ish blend
            try:
                rating = float(feedback.get('rating', 0))
            except (TypeError, ValueError):
                rating = 0.0
            sentiment_score = sentiment['score'] if sentiment else 0.0
            combined = (rating / 5.0) * 0.6 + (sentiment_score + 1.0) / 2.0 * 0.4
            evidence_w = 0.1 + 0.05 * min(user_model.feedback_count, 20) / 20.0
            user_model.trust_score = (
                user_model.trust_score * (1 - evidence_w) + combined * evidence_w
            )
            if sentiment:
                user_model.sentiment_score = (
                    user_model.sentiment_score * 0.9 + sentiment['score'] * 0.1
                )
            user_model.engagement_level = min(1.0, user_model.feedback_count / 20.0)
            user_model.clamp()

            # Behavior history
            engine.behavior_history[user_id].append({
                'timestamp': entry.timestamp,
                'policy_id': policy_id,
                'rating': rating,
                'sentiment': sentiment_score,
                'trust_score': user_model.trust_score,
            })
            engine._stats_cache = None

        self.observability.counter('feedback_received')
        self.observability.gauge('trust_score', user_model.trust_score, user_id=user_id)
        if sentiment:
            self.observability.gauge('sentiment_score', sentiment['score'], user_id=user_id)

        if engine.config.clustering.enable_user_clustering:
            engine.user_service.schedule_cluster_update()
        logger.info("Feedback recorded from %s on %s", user_id, policy_id)
        return entry


class UserModelService:
    """Manages user clustering and preference prediction."""

    def __init__(self, engine: "HumanAICoEvolutionEngine") -> None:
        self.engine = engine
        self.observability = engine.observability
        self._cluster_task: Optional[asyncio.Task] = None
        self._cluster_lock = asyncio.Lock()
        self._last_cluster_update: Optional[datetime] = None

    def schedule_cluster_update(self) -> None:
        if not self.engine.config.clustering.enable_user_clustering:
            return
        if self._cluster_task is not None and not self._cluster_task.done():
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        self._cluster_task = loop.create_task(self._cluster_update_if_needed())

    async def _cluster_update_if_needed(self) -> None:
        now = datetime.now(timezone.utc)
        interval = self.engine.config.clustering.clustering_update_interval
        if (self._last_cluster_update is not None and
                (now - self._last_cluster_update).total_seconds() < interval):
            return
        async with self._cluster_lock:
            await self._update_clusters()
            self._last_cluster_update = datetime.now(timezone.utc)

    async def _update_clusters(self) -> None:
        if not SKLEARN_AVAILABLE:
            return
        users = list(self.engine.user_models.values())
        if len(users) < 3:
            return
        features = []
        user_ids = []
        for uid, model in self.engine.user_models.items():
            features.append([
                model.trust_score,
                model.engagement_level,
                model.sentiment_score,
                float(model.preferences.get('sustainability', 0.5)),
                float(model.preferences.get('cost', 0.5)),
                float(model.preferences.get('speed', 0.5)),
                float(model.preferences.get('risk', 0.5)),
            ])
            user_ids.append(uid)
        try:
            def _do() -> Any:
                k = max(1, min(5, len(users) // 2))
                return MiniBatchKMeans(n_clusters=k, random_state=42,
                                       batch_size=100, n_init=10).fit_predict(features)

            labels = await asyncio.to_thread(_do)
            for uid, label in zip(user_ids, labels):
                self.engine.user_models[uid].cluster_id = int(label)
            self.observability.counter('cluster_updates_total')
        except Exception as e:
            logger.error("Clustering failed: %s", e)

    async def predict_preferences(self, user_id: str, days: int = 30) -> Dict[str, Any]:
        user_model = self.engine.user_models.get(user_id)
        if user_model is None:
            return {'status': 'user_not_found'}
        timeline = user_model.preference_timeline
        if len(timeline) < 5:
            return {'status': 'insufficient_data', 'sample_size': len(timeline)}

        by_key: Dict[str, List[Tuple[str, float]]] = defaultdict(list)
        for entry in timeline:
            k = entry.get('key')
            v = entry.get('value')
            ts = entry.get('timestamp', '')
            if k is not None and v is not None:
                by_key[k].append((ts, float(v)))

        predictions: Dict[str, Any] = {}
        for key, values in by_key.items():
            if len(values) < 3:
                continue
            recent = sorted(values, key=lambda x: x[0])[-20:]
            arr = np.array([v for _, v in recent], dtype=float)
            weights = np.array([0.9 ** (len(arr) - 1 - i) for i in range(len(arr))])
            wavg = float(np.average(arr, weights=weights))
            slope = 0.0
            if len(arr) >= 5:
                x = np.arange(len(arr))
                slope = float(np.polyfit(x, arr, 1)[0])
            predicted = max(0.0, min(1.0, wavg + slope * (days / 30.0)))
            current = float(user_model.preferences.get(key, wavg))
            trend = 'increasing' if predicted > current * 1.05 else (
                'decreasing' if predicted < current * 0.95 else 'stable')
            predictions[key] = {
                'predicted_value': predicted,
                'current_value': current,
                'confidence': min(0.9, len(arr) / 20.0),
                'trend': trend,
            }
        return {
            'user_id': user_id,
            'predictions': predictions,
            'sample_size': len(timeline),
            'prediction_horizon_days': days,
            'confidence': min(0.8, len(timeline) / 50.0),
        }

    async def shutdown(self) -> None:
        if self._cluster_task is not None:
            self._cluster_task.cancel()
            try:
                await self._cluster_task
            except (asyncio.CancelledError, Exception):
                pass
            self._cluster_task = None


class PolicyService:
    """Generates MOPD-aware policy suggestions with XAI."""

    def __init__(self, engine: "HumanAICoEvolutionEngine") -> None:
        self.engine = engine
        self.observability = engine.observability

    async def generate(
        self,
        context: Dict[str, Any],
        user_id: Optional[str] = None,
        idempotency_key: Optional[str] = None,
    ) -> PolicySuggestion:
        engine = self.engine
        if engine._idempotency.seen(idempotency_key):
            logger.debug("Duplicate policy suggestion ignored")
            raise ValueError("duplicate_suggestion")

        ctx = self._validate_context(context)

        base_actions = [
            {'id': 'reduce_carbon', 'effect': {'carbon': -0.3, 'helium': 0.0,
                                                'energy': 0.0, 'cost': 0.1}},
            {'id': 'conserve_helium', 'effect': {'carbon': 0.0, 'helium': -0.2,
                                                  'energy': -0.1, 'cost': 0.2}},
            {'id': 'optimize_energy', 'effect': {'carbon': -0.1, 'helium': -0.1,
                                                  'energy': -0.3, 'cost': -0.1}},
            {'id': 'increase_renewable', 'effect': {'carbon': -0.4, 'helium': 0.0,
                                                     'energy': -0.2, 'cost': 0.3}},
        ]
        from itertools import combinations
        combos: List[Tuple[Dict[str, Any], ...]] = []
        for r in range(1, 4):
            combos.extend(combinations(base_actions, r))

        # Pre-fetch user preferences once
        user_prefs = {}
        if user_id and user_id in engine.user_models:
            user_prefs = engine.user_models[user_id].preferences

        points: List[MOPDPoint] = []
        for combo in combos:
            carbon = float(ctx.get('carbon_intensity') or 400.0)
            helium = float(ctx.get('helium_scarcity') or 0.5)
            energy = float(ctx.get('energy_price') or 0.1)
            cost = 0.0
            for act in combo:
                carbon += act['effect'].get('carbon', 0.0) * 100.0
                helium += act['effect'].get('helium', 0.0)
                energy += act['effect'].get('energy', 0.0) * 0.1
                cost += act['effect'].get('cost', 0.0)

            carbon_norm = max(0.0, min(1.0, (carbon - 300.0) / 700.0))
            helium_norm = max(0.0, min(1.0, helium))
            energy_norm = max(0.0, min(1.0, energy / 0.5))
            cost_norm = max(0.0, min(1.0, cost / 2.0))

            if user_prefs.get('sustainability_focus'):
                satisfaction = 0.8 - carbon_norm * 0.3
            elif user_prefs.get('cost_focus'):
                satisfaction = 0.8 - cost_norm * 0.3
            else:
                satisfaction = 0.6
            satisfaction = max(0.0, min(1.0, satisfaction))

            point = MOPDPoint(
                policy_id=hashlib.md5(str(combo).encode()).hexdigest()[:8],
                actions=[a['id'] for a in combo],
                carbon_impact=carbon_norm,
                helium_impact=helium_norm,
                energy_impact=energy_norm,
                cost=cost_norm,
                user_satisfaction=satisfaction,
            )
            point.explanation = (
                f"Policy [{', '.join(point.actions)}] -> "
                f"carbon={carbon_norm:.2f}, helium={helium_norm:.2f}, "
                f"energy={energy_norm:.2f}, cost={cost_norm:.2f}, "
                f"satisfaction={satisfaction:.2f}"
            )
            points.append(point)

        pareto = filter_pareto(points)
        best: Optional[MOPDPoint] = None
        if engine.config.mopd.enabled and pareto:
            best = scalarise_pareto(pareto, engine.config.mopd.objective_weights)
        elif pareto:
            best = max(pareto, key=lambda p: p.user_satisfaction)
        else:
            best = None

        # Temporal safety
        if best is not None and engine.config.enable_temporal_safety:
            violations = engine.safety_monitor.check({
                'carbon_impact': best.carbon_impact,
                'helium_impact': best.helium_impact,
                'cost': best.cost,
            })
            if violations:
                logger.warning("Safety violations: %s", violations)
                # Fallback: choose least-bad safety-compliant point
                safe = [p for p in pareto if not engine.safety_monitor.check({
                    'carbon_impact': p.carbon_impact,
                    'helium_impact': p.helium_impact,
                    'cost': p.cost,
                })]
                if safe:
                    best = scalarise_pareto(safe, engine.config.mopd.objective_weights)
                else:
                    best = None
                    logger.error("No safety-compliant policy candidate")

        # Human approval (only for low-confidence suggestions)
        approval_result: Optional[ApprovalResult] = None
        if best is not None and engine.governance.enabled:
            confidence_preview = 0.5
            threshold = engine.config.approval.require_for_low_confidence_below
            if confidence_preview < threshold:
                approval_result = await engine.governance.request_approval({
                    'action': 'policy_suggestion',
                    'policy_id': best.policy_id,
                    'actions': best.actions,
                    'context': ctx,
                    'user_id': user_id,
                })
                if approval_result.status not in (
                        ApprovalStatus.APPROVED, ApprovalStatus.AUTO_APPROVED):
                    logger.info("Policy suggestion denied by governance: %s",
                                approval_result.status.value)
                    best = None

        rationale = self._build_rationale(best, ctx) if best else []
        impact = self._build_impact(best) if best else {}
        explanations = self._build_explanations(best, user_id) if best else []

        suggestion = PolicySuggestion(
            timestamp=datetime.now(timezone.utc).isoformat(),
            context=ctx,
            actions=best.actions if best else [],
            rationale=rationale,
            expected_impact=impact,
            explanations=explanations,
            confidence=0.5,
            personalized=False,
            pareto_front=[p.to_dict() for p in pareto] if pareto else None,
            best_plan=best.to_dict() if best else None,
        )

        # Personalization
        if user_id and user_id in engine.user_models:
            user_model = engine.user_models[user_id]
            if user_model.trust_score > 0.7:
                suggestion.personalized = True
                suggestion.confidence = user_model.trust_score
                suggestion.explanations.append(
                    f"Personalised based on your trust score ({user_model.trust_score:.1%})"
                )

        async with engine._lock:
            engine.policy_suggestions.append(suggestion)
            if engine.config.mopd.enabled and pareto:
                engine.pareto_history.append({
                    'timestamp': suggestion.timestamp,
                    'pareto_front': [p.to_dict() for p in pareto],
                    'best_plan': best.to_dict() if best else None,
                    'context': ctx,
                    'user_id': user_id,
                })
            engine._stats_cache = None

        self.observability.counter('suggestions_generated')
        if engine.config.mopd.enabled and pareto:
            self.observability.counter('mopd_generations')
            self.observability.histogram('pareto_front_size', len(pareto))
            if best is not None:
                self.observability.gauge('best_carbon', best.carbon_impact)
                self.observability.gauge('best_helium', best.helium_impact)

        return suggestion

    @staticmethod
    def _validate_context(context: Dict[str, Any]) -> Dict[str, Any]:
        def _num(key: str, lo: float, hi: float) -> Optional[float]:
            if key not in context:
                return None
            v = context[key]
            if v is None:
                return None
            try:
                f = float(v)
            except (TypeError, ValueError):
                return None
            if f < lo or f > hi:
                return None
            return f

        return {
            'carbon_intensity': _num('carbon_intensity', 0, 10_000),
            'helium_scarcity': _num('helium_scarcity', 0, 1),
            'energy_price': _num('energy_price', 0, 10_000),
            'renewable_ratio': _num('renewable_ratio', 0, 1),
        }

    @staticmethod
    def _build_rationale(point: MOPDPoint, ctx: Dict[str, Any]) -> List[str]:
        rationale = []
        for action in point.actions:
            if action == 'reduce_carbon':
                ci = ctx.get('carbon_intensity')
                rationale.append(
                    f"High carbon intensity detected: {ci:.0f} gCO₂/kWh"
                    if ci is not None else "Reduce carbon-intensive operations"
                )
            elif action == 'conserve_helium':
                hs = ctx.get('helium_scarcity')
                rationale.append(
                    f"Helium scarcity is critical: {hs:.0%}"
                    if hs is not None else "Conserve helium reserves"
                )
            elif action == 'optimize_energy':
                ep = ctx.get('energy_price')
                rationale.append(
                    f"High energy prices: ${ep:.2f}/kWh"
                    if ep is not None else "Optimise energy consumption"
                )
            elif action == 'increase_renewable':
                rr = ctx.get('renewable_ratio')
                rationale.append(
                    f"Low renewable ratio: {rr:.0%}"
                    if rr is not None else "Increase renewable share"
                )
        return rationale

    @staticmethod
    def _build_impact(point: MOPDPoint) -> Dict[str, float]:
        impact: Dict[str, float] = {}
        if point.carbon_impact < 0.5:
            impact['carbon_reduction'] = (0.5 - point.carbon_impact) * 2.0
        if point.helium_impact < 0.5:
            impact['helium_savings'] = (0.5 - point.helium_impact) * 2.0
        if point.energy_impact < 0.5:
            impact['energy_savings'] = (0.5 - point.energy_impact) * 2.0
        if point.cost < 0.5:
            impact['cost_savings'] = (0.5 - point.cost) * 2.0
        return impact

    @staticmethod
    def _build_explanations(point: MOPDPoint, user_id: Optional[str]) -> List[str]:
        exps: List[str] = []
        if point.carbon_impact < 0.5:
            exps.append("Reducing carbon-intensive operations lowers environmental footprint.")
        if point.helium_impact < 0.5:
            exps.append("Conserving helium ensures future availability.")
        if point.energy_impact < 0.5:
            exps.append("Optimising energy consumption reduces cost and emissions.")
        if point.cost < 0.5:
            exps.append("This strategy is cost-effective.")
        if not exps:
            exps.append("This policy maintains current sustainability metrics.")
        if point.explanation:
            exps.append(f"Trade-off details: {point.explanation}")
        return exps


class SimulationService:
    """Monte-Carlo policy impact simulation."""

    def __init__(self, engine: "HumanAICoEvolutionEngine") -> None:
        self.engine = engine
        self.observability = engine.observability

    async def simulate(
        self,
        policy: Dict[str, Any],
        simulation_steps: Optional[int] = None,
        num_scenarios: Optional[int] = None,
        confidence_level: Optional[float] = None,
    ) -> SimulationResult:
        cfg = self.engine.config.simulation
        steps = simulation_steps or cfg.simulation_steps
        scenarios = num_scenarios or cfg.num_scenarios
        conf = confidence_level or cfg.confidence_level
        if not 0.8 <= conf <= 0.99:
            conf = cfg.confidence_level

        # z-score from confidence level (two-sided)
        z = self._z_score(conf)

        actions = list(policy.get('actions', []) or [])
        carbon_all: List[List[float]] = []
        helium_all: List[List[float]] = []
        energy_all: List[List[float]] = []
        scores: List[float] = []

        for scenario in range(scenarios):
            rng = np.random.default_rng(scenario * 12345 + 42)
            current_carbon = 400.0 + rng.normal(0, 20)
            current_helium = 0.5 + rng.normal(0, 0.05)
            current_energy = 0.5 + rng.normal(0, 0.05)
            current_renewable = 0.3 + rng.normal(0, 0.03)

            carbon_noise = rng.normal(0, cfg.carbon_noise_level, steps)
            helium_noise = rng.normal(0, cfg.helium_noise_level, steps)
            energy_noise = rng.normal(0, cfg.energy_noise_level, steps)

            carbon_traj: List[float] = []
            helium_traj: List[float] = []
            energy_traj: List[float] = []

            for step in range(steps):
                if cfg.enable_simulation_coupling:
                    renewable_increase = 0.01 if 'increase_renewable' in actions else 0.0
                    current_renewable += renewable_increase + rng.normal(0, 0.005)
                    current_renewable = max(0.1, min(0.9, current_renewable))

                    carbon_reduction_rate = 0.95 - 0.1 * current_renewable
                    if 'reduce_carbon' in actions:
                        current_carbon *= carbon_reduction_rate + carbon_noise[step] * 0.02
                    else:
                        current_carbon *= 1.0 + carbon_noise[step] * 0.02

                    if 'conserve_helium' in actions:
                        current_helium *= 0.97 + helium_noise[step] * 0.02
                    else:
                        current_helium *= 1.0 + helium_noise[step] * 0.02

                    if 'optimize_energy' in actions:
                        current_energy *= 0.98 + energy_noise[step] * 0.02
                    else:
                        current_energy *= 1.0 + energy_noise[step] * 0.02
                else:
                    if 'reduce_carbon' in actions:
                        current_carbon *= 0.95 + carbon_noise[step] * 0.02
                    if 'conserve_helium' in actions:
                        current_helium *= 0.97 + helium_noise[step] * 0.02
                    if 'optimize_energy' in actions:
                        current_energy *= 0.98 + energy_noise[step] * 0.02

                carbon_traj.append(float(current_carbon))
                helium_traj.append(float(current_helium))
                energy_traj.append(float(current_energy))

            carbon_score = 1.0 - (carbon_traj[-1] - 300.0) / 700.0
            score = (
                max(0.0, min(1.0, carbon_score)) * 0.4 +
                max(0.0, min(1.0, current_helium)) * 0.3 +
                max(0.0, min(1.0, 1.0 - current_energy)) * 0.3
            )
            carbon_all.append(carbon_traj)
            helium_all.append(helium_traj)
            energy_all.append(energy_traj)
            scores.append(float(score))

        def _ci(data: List[List[float]]) -> List[Tuple[float, float]]:
            arr = np.array(data, dtype=float)
            mean = arr.mean(axis=0)
            std = arr.std(axis=0)
            lo = mean - z * std
            hi = mean + z * std
            return [(float(a), float(b)) for a, b in zip(lo, hi)]

        carbon_ci = _ci(carbon_all)
        helium_ci = _ci(helium_all)
        energy_ci = _ci(energy_all)

        improvement_prob = sum(1 for s in scores if s > 0.5) / len(scores)
        significant_prob = sum(1 for s in scores if s > 0.7) / len(scores)
        target_prob = sum(1 for s in scores if s > 0.6) / len(scores)

        self.observability.counter('simulations_total')
        self.observability.gauge('last_sustainability_score', float(np.mean(scores)))

        return SimulationResult(
            carbon_trajectory=np.mean(carbon_all, axis=0).tolist(),
            helium_trajectory=np.mean(helium_all, axis=0).tolist(),
            energy_trajectory=np.mean(energy_all, axis=0).tolist(),
            sustainability_score=float(np.mean(scores)),
            confidence_intervals={
                'carbon': carbon_ci[0] if carbon_ci else (0.0, 0.0),
                'helium': helium_ci[0] if helium_ci else (0.0, 0.0),
                'energy': energy_ci[0] if energy_ci else (0.0, 0.0),
            },
            trajectory_confidence_intervals={
                'carbon': carbon_ci,
                'helium': helium_ci,
                'energy': energy_ci,
            },
            probabilities={
                'improvement': improvement_prob,
                'significant_improvement': significant_prob,
                'sustainability_target': target_prob,
            },
            scenario_metadata={
                'num_scenarios': scenarios,
                'confidence_level': conf,
                'z_score': z,
                'simulation_steps': steps,
                'coupling_enabled': cfg.enable_simulation_coupling,
            },
        )

    @staticmethod
    def _z_score(conf: float) -> float:
        # Approximate two-sided z-score using an erf-inverse
        try:
            from statistics import NormalDist
            return NormalDist().inv_cdf(1 - (1 - conf) / 2.0)
        except Exception:
            # Fallback table
            table = {0.80: 1.2816, 0.90: 1.6449, 0.95: 1.9600, 0.99: 2.5758}
            closest = min(table.keys(), key=lambda k: abs(k - conf))
            return table[closest]


class CollaborationService:
    """Multi-user collaborative decision-making."""

    def __init__(self, engine: "HumanAICoEvolutionEngine") -> None:
        self.engine = engine
        self.observability = engine.observability

    async def decide(
        self,
        users: List[str],
        options: List[Dict[str, Any]],
        require_consensus: bool = False,
        consensus_threshold: Optional[float] = None,
        voting_method: str = "range",
    ) -> CollaborativeDecision:
        engine = self.engine
        threshold = float(consensus_threshold or engine.config.default_consensus_threshold)

        async with engine._lock:
            user_votes: Dict[str, List[float]] = {}
            user_weights: Dict[str, float] = {}

            for user_id in users:
                user_model = engine.user_models.get(user_id)
                if user_model is None:
                    continue
                prefs = user_model.preferences
                weight = user_model.trust_score * (0.5 + 0.5 * user_model.engagement_level)
                user_weights[user_id] = weight
                votes: List[float] = []
                for option in options:
                    score = 0.0
                    if 'sustainability' in prefs:
                        score += float(option.get('sustainability', 0.5)) * prefs['sustainability']
                    if 'cost' in prefs:
                        score += float(option.get('cost', 0.5)) * prefs['cost']
                    if 'speed' in prefs:
                        score += float(option.get('speed', 0.5)) * prefs['speed']
                    if 'risk' in prefs:
                        score += float(option.get('risk', 0.5)) * (1.0 - prefs['risk'])
                    votes.append(float(score))
                if votes and max(votes) > 0:
                    mx = max(votes)
                    votes = [v / mx for v in votes]
                user_votes[user_id] = votes

            if not user_votes or not options:
                decision = CollaborativeDecision(
                    selected_option=options[0] if options else {},
                    scores=[1.0] if options else [],
                    participants=[],
                    consensus_reached=False,
                    consensus_score=0.0,
                    vote_distribution={},
                    consensus_threshold=None,
                )
                engine.collaborative_decisions.append(decision)
                return decision

            n_options = len(options)
            aggregated = [0.0] * n_options
            total_weight = 0.0
            for user_id, votes in user_votes.items():
                w = user_weights.get(user_id, 0.5)
                if voting_method == "quadratic":
                    votes = [v ** 2 if v > 0 else 0.0 for v in votes]
                for i, v in enumerate(votes):
                    aggregated[i] += v * w
                total_weight += w

            if total_weight > 0:
                aggregated = [a / total_weight for a in aggregated]
            if aggregated and max(aggregated) > 0:
                mx = max(aggregated)
                aggregated = [a / mx for a in aggregated]

            consensus_score = self._gini_consensus(aggregated)
            consensus_reached = (consensus_score >= threshold) if require_consensus else True

            best_idx = int(np.argmax(aggregated))
            vote_dist: Dict[str, Any] = {}
            for user_id, votes in user_votes.items():
                if votes:
                    vote_dist[user_id] = {
                        'preferred_option': int(np.argmax(votes)),
                        'scores': votes,
                        'weight': user_weights.get(user_id, 0.5),
                    }

            decision = CollaborativeDecision(
                selected_option=options[best_idx],
                scores=aggregated,
                participants=list(user_votes.keys()),
                consensus_reached=consensus_reached,
                consensus_score=float(consensus_score),
                vote_distribution=vote_dist,
                consensus_threshold=threshold if require_consensus else None,
            )

            if not consensus_reached:
                disagreeing = [
                    uid for uid, votes in user_votes.items()
                    if votes and int(np.argmax(votes)) != best_idx
                ]
                decision.disagreeing_users = disagreeing
                decision.disagreement_analysis = self._analyse_disagreement(
                    user_votes, options, disagreeing,
                )

            engine.collaborative_decisions.append(decision)
            engine._stats_cache = None

        self.observability.counter('collaborative_decisions_total')
        self.observability.gauge('consensus_score', float(consensus_score))
        return decision

    @staticmethod
    def _gini_consensus(scores: List[float]) -> float:
        if len(scores) <= 1:
            return 1.0
        s = sorted(scores)
        n = len(s)
        mean = float(np.mean(s))
        if mean <= 0:
            return 0.0
        num = sum((i + 1) * s[i] for i in range(n)) - (n * (n + 1) * mean / 2.0)
        den = n * (n - 1) * mean / 2.0
        gini = num / den if den != 0 else 0.0
        return max(0.0, min(1.0, 1.0 - gini))

    @staticmethod
    def _analyse_disagreement(
        user_votes: Dict[str, List[float]],
        options: List[Dict[str, Any]],
        disagreeing: List[str],
    ) -> Dict[str, Any]:
        if not disagreeing:
            return {'pattern': 'consensus'}
        # Simple diagnostic: which preference dominates disagreeing users
        preference_keys = ['sustainability', 'cost', 'speed', 'risk']
        diagnostic: Dict[str, int] = {k: 0 for k in preference_keys}
        for uid in disagreeing:
            votes = user_votes.get(uid, [])
            if not votes:
                continue
            best_opt = options[int(np.argmax(votes))] if options else {}
            for key in preference_keys:
                if key in best_opt:
                    diagnostic[key] += 1
        return {
            'pattern': 'preference_divergence',
            'disagreeing_count': len(disagreeing),
            'dominant_preferences': sorted(diagnostic.items(),
                                           key=lambda x: x[1], reverse=True),
        }


class ChaosService:
    """Non-destructive chaos testing with save/restore."""

    def __init__(self, engine: "HumanAICoEvolutionEngine",
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
                if fault_type == FaultType.SENTIMENT_MODEL_DOWN:
                    self._saved.setdefault('sentiment_pipeline',
                                            self.engine.sentiment_service.pipeline)
                    self.engine.sentiment_service.pipeline = None
                    self._injected.append(fault_type.value)
                    return {'status': 'injected'}
                if fault_type == FaultType.TELEMETRY_FAILURE:
                    self._saved.setdefault('telemetry', self.engine.telemetry)
                    self.engine.telemetry = None
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
                if fault_type == FaultType.CLUSTERING_FAILURE:
                    self._saved.setdefault('clustering_enabled',
                                            self.engine.config.clustering.enable_user_clustering)
                    self.engine.config.clustering.enable_user_clustering = False
                    self._injected.append(fault_type.value)
                    return {'status': 'injected'}
                if fault_type == FaultType.SIMULATION_FAILURE:
                    self._saved.setdefault('simulation_service',
                                            self.engine.simulation_service)
                    self.engine.simulation_service = None  # type: ignore
                    self._injected.append(fault_type.value)
                    return {'status': 'injected'}
                if fault_type == FaultType.USER_MODEL_CORRUPTION:
                    if self.engine.user_models:
                        uid = next(iter(self.engine.user_models))
                        um = self.engine.user_models[uid]
                        self._saved.setdefault(f'user:{uid}:trust', um.trust_score)
                        # Set to valid but degraded value (respects [0,1])
                        um.trust_score = 0.0
                        self._injected.append(fault_type.value)
                        return {'status': 'injected', 'user_id': uid}
                    return {'status': 'skipped', 'reason': 'no_users'}
                if fault_type == FaultType.GOVERNANCE_TIMEOUT:
                    self._saved.setdefault('governance_enabled',
                                            self.engine.governance.enabled)
                    self.engine.governance.enabled = True
                    self._saved.setdefault('governance_timeout',
                                            self.engine.governance.timeout_seconds)
                    self.engine.governance.timeout_seconds = 0.01
                    self._injected.append(fault_type.value)
                    return {'status': 'injected'}
            except Exception as e:
                logger.exception("Chaos injection failed: %s", e)
                return {'status': 'error', 'error': str(e)}
            return {'status': 'unknown_fault'}

    def restore(self) -> None:
        eng = self.engine
        for key, value in list(self._saved.items()):
            if key == 'sentiment_pipeline':
                eng.sentiment_service.pipeline = value
            elif key == 'telemetry':
                eng.telemetry = value
            elif key == 'persistence_save':
                eng.persistence.save = value  # type: ignore
            elif key == 'clustering_enabled':
                eng.config.clustering.enable_user_clustering = value
            elif key == 'simulation_service':
                eng.simulation_service = value
            elif key.startswith('user:') and key.endswith(':trust'):
                uid = key.split(':')[1]
                um = eng.user_models.get(uid)
                if um is not None:
                    um.trust_score = float(value)
            elif key == 'governance_enabled':
                eng.governance.enabled = value
            elif key == 'governance_timeout':
                eng.governance.timeout_seconds = float(value)
            self._saved.pop(key, None)
        self._injected.clear()

    async def run_test(
        self,
        user_id: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        if not self.enabled:
            return {'status': 'disabled'}

        report: Dict[str, Any] = {
            'faults': [],
            'results': {},
            'restored': False,
        }
        context = context or {}
        faults = [
            FaultType.SENTIMENT_MODEL_DOWN,
            FaultType.TELEMETRY_FAILURE,
            FaultType.PERSISTENCE_FAILURE,
            FaultType.CLUSTERING_FAILURE,
            FaultType.USER_MODEL_CORRUPTION,
            FaultType.GOVERNANCE_TIMEOUT,
        ]
        try:
            for ft in faults[:CHAOS_MAX_FAULTS_PER_RUN]:
                res = await self.inject(ft)
                report['faults'].append(ft.value)
                report['results'][ft.value] = res

            # Steady-state hypothesis: sentiment fallback works
            try:
                sent = await self.engine.sentiment_service.analyze_sentiment("great work")
                report['results']['sentiment_fallback'] = {
                    'success': bool(sent), 'score': sent.get('score', 0.0),
                }
            except Exception as e:
                report['results']['sentiment_fallback'] = {'error': str(e)}

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
# Main engine
# ============================================================================
class HumanAICoEvolutionEngine:
    """
    Human-AI co-evolution engine v5.0.0 with sub-services, lifecycle,
    governance, observability, chaos testing, and idempotency.
    """

    def __init__(self, config: Optional[CoEvolutionConfig] = None) -> None:
        self.config = config or CoEvolutionConfig()

        # Observability
        self.observability = ObservabilityHub()
        self.telemetry: Optional[TelemetryProtocol] = None  # populated externally

        # Governance
        self.governance = GovernanceLayer(
            enabled=self.config.approval.enabled,
            timeout_seconds=self.config.approval.timeout_seconds,
            auto_approve_on_timeout=self.config.approval.auto_approve_on_timeout,
            publish_fn=None,  # can be injected later
        )

        # Safety
        self.safety_monitor = SafetyMonitor()
        self._setup_invariants()

        # Persistence
        self.persistence = PersistenceService(
            self.config.persistence_path, observability=self.observability,
        )

        # Sentiment
        self.sentiment_service = SentimentService(
            model_name=self.config.sentiment_model_name,
            device=self.config.sentiment_model_device,
            observability=self.observability,
        )

        # State containers
        self.feedback_history: Deque[FeedbackEntry] = deque(
            maxlen=self.config.feedback_history_limit,
        )
        self.user_models: Dict[str, UserModel] = {}
        self.policy_suggestions: Deque[PolicySuggestion] = deque(
            maxlen=self.config.policy_suggestions_limit,
        )
        self.collaborative_decisions: Deque[CollaborativeDecision] = deque(
            maxlen=self.config.collaborative_decisions_limit,
        )
        self.behavior_history: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self.pareto_history: Deque[Dict[str, Any]] = deque(
            maxlen=self.config.pareto_history_limit,
        )
        self.consensus_builders: Dict[str, Dict[str, Any]] = {}

        # Idempotency
        self._idempotency = IdempotencyCache()

        # Dead-letter queue
        self._dead_letters: Deque[Dict[str, Any]] = deque(maxlen=MAX_DEAD_LETTERS)

        # Locks
        self._lock = asyncio.Lock()

        # Caches
        self._stats_cache: Optional[Dict[str, Any]] = None
        self._stats_cache_time: Optional[datetime] = None
        self._stats_cache_ttl = 30.0

        # Sub-services
        self.feedback_service = FeedbackService(self)
        self.user_service = UserModelService(self)
        self.policy_service = PolicyService(self)
        self.simulation_service = SimulationService(self)
        self.collaboration_service = CollaborationService(self)

        # Chaos
        self.chaos_service = ChaosService(self, self.config.chaos)

        # Lifecycle
        self._started = False
        self._shutting_down = False
        self._background_tasks: List[asyncio.Task] = []
        self._auto_save_task: Optional[asyncio.Task] = None
        self._chaos_task: Optional[asyncio.Task] = None

        logger.info("HumanAICoEvolutionEngine v%s initialised (not started)", ENGINE_VERSION)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def start(self) -> None:
        if self._started:
            return
        self._started = True

        await self._load_state()

        if self.config.persistence_auto_save_interval > 0:
            self._auto_save_task = asyncio.create_task(self._auto_save_loop())
            self._background_tasks.append(self._auto_save_task)

        if self.config.enable_chaos_testing and self.config.chaos.enabled:
            self._chaos_task = asyncio.create_task(self._chaos_loop())
            self._background_tasks.append(self._chaos_task)

        logger.info("HumanAICoEvolutionEngine started")

    async def wait_ready(self) -> None:
        if not self._started:
            await self.start()

    async def shutdown(self) -> None:
        logger.info("Shutting down HumanAICoEvolutionEngine")
        self._shutting_down = True
        self._started = False

        for t in self._background_tasks:
            t.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)
        self._background_tasks.clear()

        await self.user_service.shutdown()

        try:
            await self.save_state()
        except Exception as e:
            logger.error("Final save failed: %s", e)

        logger.info("Shutdown complete")

    async def _auto_save_loop(self) -> None:
        interval = max(1, self.config.persistence_auto_save_interval)
        while not self._shutting_down:
            try:
                await asyncio.sleep(interval)
                await self.save_state()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Auto-save error: %s", e)

    async def _chaos_loop(self) -> None:
        interval = max(60, self.config.chaos.interval_seconds)
        while not self._shutting_down:
            try:
                await asyncio.sleep(interval)
                await self.chaos_service.run_test()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Chaos loop error: %s", e)

    # ------------------------------------------------------------------
    # Public API (backward compatible)
    # ------------------------------------------------------------------
    async def record_feedback(
        self,
        user_id: str,
        policy_id: str,
        feedback: Dict[str, Any],
        idempotency_key: Optional[str] = None,
    ) -> FeedbackEntry:
        await self.wait_ready()
        return await self.feedback_service.record(
            user_id, policy_id, feedback, idempotency_key=idempotency_key,
        )

    async def generate_policy_suggestion(
        self,
        context: Dict[str, Any],
        user_id: Optional[str] = None,
        idempotency_key: Optional[str] = None,
    ) -> Dict[str, Any]:
        await self.wait_ready()
        suggestion = await self.policy_service.generate(
            context, user_id=user_id, idempotency_key=idempotency_key,
        )
        return suggestion.to_dict()

    async def simulate_policy_impact(
        self,
        policy: Dict[str, Any],
        simulation_steps: Optional[int] = None,
        num_scenarios: Optional[int] = None,
        confidence_level: Optional[float] = None,
    ) -> SimulationResult:
        await self.wait_ready()
        return await self.simulation_service.simulate(
            policy,
            simulation_steps=simulation_steps,
            num_scenarios=num_scenarios,
            confidence_level=confidence_level,
        )

    async def collaborative_decision(
        self,
        users: List[str],
        options: List[Dict[str, Any]],
        require_consensus: bool = False,
        consensus_threshold: Optional[float] = None,
        voting_method: str = "range",
    ) -> Dict[str, Any]:
        await self.wait_ready()
        decision = await self.collaboration_service.decide(
            users, options,
            require_consensus=require_consensus,
            consensus_threshold=consensus_threshold,
            voting_method=voting_method,
        )
        return decision.to_dict()

    async def predict_user_preferences(self, user_id: str, days: int = 30) -> Dict[str, Any]:
        await self.wait_ready()
        return await self.user_service.predict_preferences(user_id, days=days)

    def get_user_insights(self, user_id: str) -> Dict[str, Any]:
        user_model = self.user_models.get(user_id)
        if user_model is None:
            return {'status': 'user_not_found'}
        return {
            'user_id': user_id,
            'preferences': dict(user_model.preferences),
            'trust_score': user_model.trust_score,
            'feedback_count': user_model.feedback_count,
            'sentiment_score': user_model.sentiment_score,
            'engagement_level': user_model.engagement_level,
            'cluster_id': user_model.cluster_id,
            'last_active': user_model.last_active,
        }

    def get_sentiment_summary(self) -> Dict[str, Any]:
        if not self.feedback_history:
            return {'status': 'no_feedback'}
        scores: List[float] = []
        policy_scores: Dict[str, List[float]] = defaultdict(list)
        for fb in self.feedback_history:
            if fb.sentiment:
                s = float(fb.sentiment.get('score', 0.0))
                scores.append(s)
                if fb.policy_id:
                    policy_scores[fb.policy_id].append(s)
        if not scores:
            return {'status': 'no_sentiment_data'}
        policy_summary = {
            pid: {
                'average_sentiment': float(np.mean(v)),
                'sample_count': len(v),
                'positive_ratio': sum(1 for x in v if x > 0.2) / len(v),
                'negative_ratio': sum(1 for x in v if x < -0.2) / len(v),
            }
            for pid, v in policy_scores.items()
        }
        return {
            'average_sentiment': float(np.mean(scores)),
            'positive_ratio': sum(1 for s in scores if s > 0.2) / len(scores),
            'negative_ratio': sum(1 for s in scores if s < -0.2) / len(scores),
            'neutral_ratio': sum(1 for s in scores if -0.2 <= s <= 0.2) / len(scores),
            'sample_count': len(scores),
            'by_policy': policy_summary,
        }

    def get_consensus_analysis(self) -> Dict[str, Any]:
        if not self.collaborative_decisions:
            return {'status': 'no_decisions'}
        decisions = list(self.collaborative_decisions)
        n = len(decisions)
        consensus_rate = sum(1 for d in decisions if d.consensus_reached) / n
        avg_consensus = float(np.mean([d.consensus_score for d in decisions]))
        avg_participants = float(np.mean([len(d.participants) for d in decisions]))
        return {
            'total_decisions': n,
            'consensus_rate': consensus_rate,
            'average_consensus_score': avg_consensus,
            'average_participants': avg_participants,
        }

    def get_coevolution_stats(self) -> Dict[str, Any]:
        now = datetime.now(timezone.utc)
        if (self._stats_cache is not None and self._stats_cache_time is not None and
                (now - self._stats_cache_time).total_seconds() < self._stats_cache_ttl):
            return self._stats_cache
        if not self.user_models:
            stats: Dict[str, Any] = {
                'total_feedback': len(self.feedback_history),
                'unique_users': 0,
                'policy_suggestions': len(self.policy_suggestions),
                'collaborative_decisions': len(self.collaborative_decisions),
                'average_trust': 0.0,
                'average_sentiment': 0.0,
                'high_trust_users': 0,
                'mopd_stats': {
                    'enabled': self.config.mopd.enabled,
                    'pareto_fronts_generated': len(self.pareto_history),
                },
            }
        else:
            trusts = [m.trust_score for m in self.user_models.values()]
            sents = [m.sentiment_score for m in self.user_models.values()]
            stats = {
                'total_feedback': len(self.feedback_history),
                'unique_users': len(self.user_models),
                'policy_suggestions': len(self.policy_suggestions),
                'collaborative_decisions': len(self.collaborative_decisions),
                'average_trust': float(np.mean(trusts)),
                'average_sentiment': float(np.mean(sents)),
                'high_trust_users': sum(1 for t in trusts if t > 0.7),
                'mopd_stats': {
                    'enabled': self.config.mopd.enabled,
                    'pareto_fronts_generated': len(self.pareto_history),
                    'average_pareto_size': float(np.mean(
                        [len(p['pareto_front']) for p in self.pareto_history]
                    )) if self.pareto_history else 0.0,
                },
            }
        self._stats_cache = stats
        self._stats_cache_time = now
        return stats

    async def get_telemetry_export(self) -> str:
        return await self.observability.export_prometheus()

    async def get_health_status(self) -> Dict[str, Any]:
        stats = self.get_coevolution_stats()
        return {
            'status': 'healthy' if self._calculate_health_score() > 0.5 else 'degraded',
            'score': self._calculate_health_score(),
            'version': ENGINE_VERSION,
            'schema': STATE_SCHEMA_VERSION,
            'started': self._started,
            'shutting_down': self._shutting_down,
            'details': {
                'feedback_count': stats.get('total_feedback', 0),
                'user_count': stats.get('unique_users', 0),
                'suggestions_count': stats.get('policy_suggestions', 0),
                'decisions_count': stats.get('collaborative_decisions', 0),
                'avg_trust': stats.get('average_trust', 0.0),
                'avg_sentiment': stats.get('average_sentiment', 0.0),
                'mopd_enabled': self.config.mopd.enabled,
                'pareto_history_size': len(self.pareto_history),
                'temporal_safety_enabled': self.config.enable_temporal_safety,
                'human_approval_enabled': self.config.approval.enabled,
                'chaos_testing_enabled': self.config.enable_chaos_testing,
                'federated_enabled': self.config.federated.enabled,
                'dead_letters': len(self._dead_letters),
                'governance_pending': len(self.governance.pending),
            },
        }

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------
    async def save_state(self) -> bool:
        state = {
            'version': ENGINE_VERSION,
            'feedback_history': [f.to_dict() for f in self.feedback_history],
            'user_models': {uid: m.to_dict() for uid, m in self.user_models.items()},
            'policy_suggestions': [p.to_dict() for p in self.policy_suggestions],
            'collaborative_decisions': [d.to_dict() for d in self.collaborative_decisions],
            'behavior_history': {k: list(v) for k, v in self.behavior_history.items()},
            'pareto_history': list(self.pareto_history),
            'consensus_builders': _to_serializable(self.consensus_builders),
        }
        return await self.persistence.save(state)

    async def _load_state(self) -> bool:
        data = await self.persistence.load()
        if not data:
            return False
        try:
            self.feedback_history.clear()
            for e in data.get('feedback_history', []):
                try:
                    self.feedback_history.append(FeedbackEntry.from_dict(e))
                except Exception as ex:
                    logger.warning("Skipping invalid feedback entry: %s", ex)

            self.user_models.clear()
            for uid, um in (data.get('user_models') or {}).items():
                try:
                    self.user_models[uid] = UserModel.from_dict(um)
                except Exception as ex:
                    logger.warning("Skipping invalid user model %s: %s", uid, ex)

            self.policy_suggestions.clear()
            for p in data.get('policy_suggestions', []):
                try:
                    self.policy_suggestions.append(PolicySuggestion(**_to_serializable(p)))
                except Exception as ex:
                    logger.warning("Skipping invalid policy suggestion: %s", ex)

            self.collaborative_decisions.clear()
            for d in data.get('collaborative_decisions', []):
                try:
                    self.collaborative_decisions.append(CollaborativeDecision(**_to_serializable(d)))
                except Exception as ex:
                    logger.warning("Skipping invalid decision: %s", ex)

            self.behavior_history.clear()
            for uid, history in (data.get('behavior_history') or {}).items():
                self.behavior_history[uid] = list(history)

            self.pareto_history.clear()
            for entry in data.get('pareto_history', []):
                self.pareto_history.append(entry)

            self.consensus_builders = dict(data.get('consensus_builders') or {})
            logger.info("State loaded from %s", self.persistence.path)
            self.observability.counter('state_loads_total')
            return True
        except Exception as e:
            logger.error("Failed to parse loaded state: %s", e)
            return False

    async def delete_state(self) -> bool:
        return await self.persistence.delete()

    # ------------------------------------------------------------------
    # Chaos / governance public API
    # ------------------------------------------------------------------
    async def inject_fault(self, fault_type: str, **params: Any) -> Dict[str, Any]:
        try:
            ft = FaultType(fault_type)
        except ValueError:
            logger.warning("Unknown fault type: %s", fault_type)
            return {'status': 'unknown_fault'}
        return await self.chaos_service.inject(ft, **params)

    async def run_chaos_test(self, user_id: Optional[str] = None,
                             context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return await self.chaos_service.run_test(user_id=user_id, context=context)

    async def submit_approval_decision(self, request_id: str, approved: bool) -> bool:
        return await self.governance.submit_decision(request_id, approved)

    def get_approval_audit(self) -> List[Dict[str, Any]]:
        return self.governance.get_audit_log()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _setup_invariants(self) -> None:
        def _bounded(name: str, lo: float, hi: float) -> Callable[[Dict[str, Any]], bool]:
            return lambda s: lo <= float(s.get(name, 0.0)) <= hi

        def _nonneg(name: str) -> Callable[[Dict[str, Any]], bool]:
            return lambda s: float(s.get(name, 0.0)) >= 0.0

        self.safety_monitor.add('carbon_bounded', _bounded('carbon_impact', 0.0, 1.0),
                                'Carbon impact must be in [0, 1]')
        self.safety_monitor.add('helium_bounded', _bounded('helium_impact', 0.0, 1.0),
                                'Helium impact must be in [0, 1]')
        self.safety_monitor.add('energy_bounded', _bounded('energy_impact', 0.0, 1.0),
                                'Energy impact must be in [0, 1]')
        self.safety_monitor.add('cost_bounded', _bounded('cost', 0.0, 1.0),
                                'Cost must be in [0, 1]')
        self.safety_monitor.add('satisfaction_bounded', _bounded('user_satisfaction', 0.0, 1.0),
                                'User satisfaction must be in [0, 1]')
        self.safety_monitor.add('trust_nonneg', _nonneg('average_trust'),
                                'Average trust must be non-negative')

    def _calculate_health_score(self) -> float:
        if not self.user_models:
            return 0.5
        trusts = [m.trust_score for m in self.user_models.values()]
        engagements = [m.engagement_level for m in self.user_models.values()]
        return float(np.mean(trusts) * 0.6 + np.mean(engagements) * 0.4)

    def _dead_letter(self, topic: str, error: str) -> None:
        self._dead_letters.append({
            'topic': topic,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'error': error,
        })

    def get_dead_letters(self) -> List[Dict[str, Any]]:
        return list(self._dead_letters)

    async def get_prometheus_metrics(self) -> str:
        return await self.observability.export_prometheus()

    # ------------------------------------------------------------------
    # Federated (stub)
    # ------------------------------------------------------------------
    async def participate_in_federation(self) -> None:
        if not self.config.federated.enabled:
            return
        stats = self.get_coevolution_stats()
        logger.info("Federated sync stub: %s feedbacks", stats.get('total_feedback', 0))
        # Real implementation would POST to `config.federated.server_url`
        # with DP noise applied to numeric fields.


# ============================================================================
# Exports
# ============================================================================
__all__ = [
    # Engine
    "HumanAICoEvolutionEngine",
    "CoEvolutionConfig",
    # Sub-services
    "FeedbackService",
    "UserModelService",
    "PolicyService",
    "SimulationService",
    "CollaborationService",
    "SentimentService",
    "PersistenceService",
    # Infra
    "ObservabilityHub",
    "GovernanceLayer",
    "ApprovalRequest",
    "ApprovalResult",
    "ApprovalStatus",
    "ChaosService",
    "FaultType",
    "SafetyInvariant",
    "SafetyMonitor",
    "IdempotencyCache",
    # Data
    "MOPDPoint",
    "FeedbackEntry",
    "UserModel",
    "PolicySuggestion",
    "CollaborativeDecision",
    "SimulationResult",
    # Sub-configs
    "MOPDConfig",
    "FederatedConfig",
    "SimulationConfig",
    "TelemetryConfig",
    "ClusteringConfig",
    "ApprovalConfig",
    "ChaosConfig",
    # Utilities
    "scalarise_pareto",
    "filter_pareto",
    # Constants
    "ENGINE_VERSION",
    "STATE_SCHEMA_VERSION",
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

    async def main() -> None:
        cfg = CoEvolutionConfig()
        engine = HumanAICoEvolutionEngine(cfg)
        await engine.start()

        await engine.record_feedback(
            user_id="alice",
            policy_id="p1",
            feedback={"rating": 5, "comment": "Great improvement!",
                      "preferences": {"sustainability": 0.9, "cost": 0.3}},
            idempotency_key="fb-1",
        )
        await engine.record_feedback(
            user_id="bob",
            policy_id="p1",
            feedback={"rating": 3, "comment": "Slow but useful.",
                      "preferences": {"sustainability": 0.6, "cost": 0.8}},
            idempotency_key="fb-2",
        )

        suggestion = await engine.generate_policy_suggestion(
            context={"carbon_intensity": 500, "helium_scarcity": 0.7,
                     "energy_price": 0.15, "renewable_ratio": 0.25},
            user_id="alice",
            idempotency_key="sug-1",
        )
        print("Suggestion best plan:", json.dumps(suggestion.get('best_plan'), indent=2))

        sim = await engine.simulate_policy_impact(suggestion)
        print("Simulation sustainability:", sim.sustainability_score)

        decision = await engine.collaborative_decision(
            users=["alice", "bob"],
            options=[
                {"sustainability": 0.9, "cost": 0.5, "speed": 0.6, "risk": 0.2},
                {"sustainability": 0.5, "cost": 0.9, "speed": 0.8, "risk": 0.1},
            ],
        )
        print("Consensus:", decision['consensus_reached'], decision['consensus_score'])

        health = await engine.get_health_status()
        print("Health:", health['status'])

        report = await engine.run_chaos_test()
        print("Chaos:", report['restored'], report['results'].keys())

        metrics = await engine.get_prometheus_metrics()
        print(metrics[:300])

        await engine.shutdown()

    asyncio.run(main())
