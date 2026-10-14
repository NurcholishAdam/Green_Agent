#!/usr/bin/env python3
# sustainability/__init__.py
"""
Sustainability-Aware Model Compression and Pruning Module v8.0.0
================================================================

Single-file drop-in for the Green_Agent MoE system with MOPD, XAI, temporal
safety, human-in-the-loop, non-destructive chaos testing, and federated
learning config support.

ENHANCEMENTS OVER v3.2.0:
- Pydantic v1/v2 compatibility (with dataclass fallback).
- Canonical MOPD objective keys matching MOPDPoint fields.
- Modern torch imports (torch.ao.quantization) with fallback.
- Safe `torch.load(weights_only=True)`.
- Structured pruning extended to Linear layers.
- Real SVD: replaces Linear with two smaller Linears.
- Async SQLite via `asyncio.to_thread`.
- `GovernanceLayer` — real human-in-the-loop with queue + audit.
- `ChaosService` — non-destructive with full save/restore.
- Shared `scalarise_pareto` / `filter_pareto` — single source of truth.
- `ObservabilityHub` — Prometheus-ready metrics.
- Full lifecycle: `start()` / `wait_ready()` / `shutdown()`.
- Idempotency keys with LRU cache.
- Dead-letter queue for swarm/feedback failures.
- Bounded caches, timezone-aware datetimes.
- Skip already-compressed experts in re-compression loop.
- Process all matching experts in anomaly handling.
"""

from __future__ import annotations

import asyncio
import copy
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
from dataclasses import dataclass, field, asdict, fields as dataclass_fields
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import (
    Any, Awaitable, Callable, Deque, Dict, FrozenSet, List, Optional,
    Protocol, Sequence, Tuple, runtime_checkable,
)

import numpy as np

# ---------------------------------------------------------------------------
# Torch
# ---------------------------------------------------------------------------
try:
    import torch
    import torch.nn as nn
    import torch.nn.utils.prune as prune
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False
    torch = None
    nn = None
    prune = None

try:
    from torch.ao.quantization import quantize_dynamic
    QUANT_AVAILABLE = True
except ImportError:
    try:
        from torch.quantization import quantize_dynamic  # type: ignore
        QUANT_AVAILABLE = True
    except ImportError:
        quantize_dynamic = None  # type: ignore
        QUANT_AVAILABLE = False

try:
    from thop import profile as _thop_profile  # type: ignore
    THOP_AVAILABLE = True
except ImportError:
    _thop_profile = None
    THOP_AVAILABLE = False

# ---------------------------------------------------------------------------
# Optional Prometheus
# ---------------------------------------------------------------------------
try:
    from prometheus_client import (  # type: ignore
        Counter, Gauge, Histogram, CollectorRegistry,
        generate_latest, CONTENT_TYPE_LATEST,
    )
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

# ---------------------------------------------------------------------------
# Pydantic compatibility layer
# ---------------------------------------------------------------------------
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

logger = logging.getLogger(__name__)


# ============================================================================
# Named Constants
# ============================================================================
MODULE_VERSION = "8.0.0"
STATE_SCHEMA_VERSION = 8
DEFAULT_ENERGY_THRESHOLD = 5.0
DEFAULT_ACCURACY_TOLERANCE = 0.02
DEFAULT_ROLLBACK_THRESHOLD = 0.95
MAX_APPROVAL_AUDIT = 1_000
MAX_DEAD_LETTERS = 1_000
MAX_IDEMPOTENCY_CACHE = 4_096
MAX_APPROVAL_PENDING = 512
MAX_PARETO_CACHE = 512
MAX_EXPLANATION_CACHE = 512
APPROVAL_DEFAULT_TIMEOUT = 60.0
CHAOS_MAX_FAULTS_PER_RUN = 6
MIN_SVD_RANK = 1

MOPD_OBJECTIVE_NAMES: Tuple[str, ...] = (
    'accuracy', 'energy', 'carbon_savings_kg', 'material_index',
)
MOPD_HIGHER_IS_BETTER: FrozenSet[str] = frozenset({
    'accuracy', 'carbon_savings_kg',
})

_DEFAULT_HARDWARE_PROFILES: Dict[str, float] = {
    'default': 0.5e-12, 'gpu': 0.3e-12, 'cpu': 0.5e-12, 'tpu': 0.2e-12,
}
_DEFAULT_MOPD_WEIGHTS: Dict[str, float] = {
    'accuracy': 0.40, 'energy': 0.30,
    'carbon_savings_kg': 0.20, 'material_index': 0.10,
}
_DEFAULT_MATERIAL_MAP: Dict[str, float] = {
    'default': 0.5, 'gpu': 0.4, 'cpu': 0.3, 'tpu': 0.2,
}


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
    if TORCH_AVAILABLE and torch is not None and isinstance(obj, torch.Tensor):  # type: ignore
        return obj.detach().cpu().tolist()
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
# Protocols
# ============================================================================
@runtime_checkable
class TelemetryProtocol(Protocol):
    async def get_energy_per_inference(self, expert_id: str) -> Optional[float]: ...
    async def increment(self, metric: str, value: float = 1.0,
                        tags: Optional[Dict[str, str]] = None) -> None: ...
    async def gauge(self, metric: str, value: float,
                    tags: Optional[Dict[str, str]] = None) -> None: ...


@runtime_checkable
class CarbonManagerProtocol(Protocol):
    async def get_current_intensity(self) -> float: ...
    async def close(self) -> None: ...


@runtime_checkable
class AnomalyDetectorProtocol(Protocol):
    async def ingest(self, node_id: str, metrics: Dict[str, Any]) -> Optional[Any]: ...


@runtime_checkable
class MessageQueueProtocol(Protocol):
    async def publish(self, topic: str, payload: Any) -> None: ...


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
    CARBON_API_FAILURE = "carbon_api_failure"
    TELEMETRY_FAILURE = "telemetry_failure"
    HIGH_ENERGY = "high_energy"
    QUANTIZATION_FAILURE = "quantization_failure"
    SVD_SINGULARITY = "svd_singularity"
    STORAGE_CORRUPTION = "storage_corruption"
    COMPRESSOR_FAILURE = "compressor_failure"


# ============================================================================
# Stubs
# ============================================================================
class TelemetryCollectorStub:
    async def get_energy_per_inference(self, expert_id: str) -> Optional[float]:
        return None

    async def increment(self, metric: str, value: float = 1.0,
                        tags: Optional[Dict[str, str]] = None) -> None:
        pass

    async def gauge(self, metric: str, value: float,
                    tags: Optional[Dict[str, str]] = None) -> None:
        pass


class CarbonIntensityManagerStub:
    async def get_current_intensity(self) -> float:
        return 400.0

    async def close(self) -> None:
        pass


class AnomalyDetectorStub:
    async def ingest(self, node_id: str,
                     metrics: Dict[str, Any]) -> Optional[Any]:
        return None


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
# Safety
# ============================================================================
class SafetyInvariant:
    def __init__(self, name: str, predicate: Callable[[Dict[str, Any]], bool],
                 description: str, kind: str = "always") -> None:
        if kind not in ("always", "eventually"):
            raise ValueError(f"Unsupported kind: {kind}")
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
# Shared Pareto helpers
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
    if not points:
        return None
    names = list(objective_names or MOPD_OBJECTIVE_NAMES)
    hib = set(higher_is_better or MOPD_HIGHER_IS_BETTER)
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
    names = list(objective_names or MOPD_OBJECTIVE_NAMES)
    hib = set(higher_is_better or MOPD_HIGHER_IS_BETTER)
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
# Configurations
# ============================================================================
@dataclass
class MOPDConfig:
    enabled: bool = True
    objective_weights: Dict[str, float] = field(
        default_factory=lambda: dict(_DEFAULT_MOPD_WEIGHTS),
    )
    grid_resolution: int = 5
    enable_cost_benefit: bool = True
    enable_predictive: bool = True
    explain_decisions: bool = True

    def __post_init__(self) -> None:
        extra = set(self.objective_weights) - set(MOPD_OBJECTIVE_NAMES)
        missing = set(MOPD_OBJECTIVE_NAMES) - set(self.objective_weights)
        if extra:
            raise ValueError(f"Unknown MOPD weight keys: {sorted(extra)}")
        if missing:
            raise ValueError(f"Missing MOPD weight keys: {sorted(missing)}")
        total = sum(self.objective_weights.values())
        if total <= 0:
            raise ValueError("MOPD weights must sum to > 0")
        if abs(total - 1.0) > 1e-6:
            logger.warning("MOPD weights sum to %.4f; normalising.", total)
            for k in list(self.objective_weights):
                self.objective_weights[k] /= total


@dataclass
class FederatedConfig:
    enabled: bool = False
    server_url: Optional[str] = None
    sparsity_ratio: float = 0.1
    privacy_epsilon: float = 1.0
    privacy_delta: float = 1e-5
    privacy_clip_norm: float = 1.0
    sync_interval_seconds: int = 3600
    max_retries: int = 3


@dataclass
class ApprovalConfig:
    enabled: bool = False
    timeout_seconds: float = APPROVAL_DEFAULT_TIMEOUT
    auto_approve_on_timeout: bool = False
    require_for_accuracy_drop_above: float = 0.2


@dataclass
class ChaosConfig:
    enabled: bool = False
    interval_seconds: int = 3600
    auto_restore: bool = True


@dataclass
class SustainabilityConfig:
    # Compression triggers
    energy_threshold_joules: float = DEFAULT_ENERGY_THRESHOLD
    accuracy_drop_tolerance: float = DEFAULT_ACCURACY_TOLERANCE
    relative_accuracy_drop_tolerance: float = 0.03
    rollback_accuracy_threshold: float = DEFAULT_ROLLBACK_THRESHOLD
    # Energy model
    energy_per_mac: float = 0.5e-12
    max_energy_joules: float = 10.0
    carbon_savings_scale_kg: float = 0.1
    hardware_profiles: Dict[str, float] = field(
        default_factory=lambda: dict(_DEFAULT_HARDWARE_PROFILES),
    )
    hardware_profile: str = "default"
    material_index_map: Dict[str, float] = field(
        default_factory=lambda: dict(_DEFAULT_MATERIAL_MAP),
    )
    # Fitness
    fitness_accuracy_weight: float = 0.5
    fitness_energy_weight: float = 0.3
    fitness_carbon_weight: float = 0.15
    fitness_material_weight: float = 0.05
    compression_bonus: float = 0.05
    # Strategy
    pruning_sparsity: float = 0.3
    structured_pruning_enabled: bool = True
    hybrid_pruning_sparsity: float = 0.2
    svd_rank_factor: float = 0.5
    # Persistence
    compressed_model_dir: str = "./compressed_models"
    history_db_path: str = "./compression_history.db"
    # Triggers
    recompress_interval: int = 0
    anomaly_trigger_enabled: bool = True
    # Telemetry
    telemetry_prefix: str = "sustainability"
    # Retry/circuit breaker
    max_retries: int = 3
    retry_base_delay_ms: float = 100.0
    retry_max_delay_ms: float = 5000.0
    circuit_breaker_failure_threshold: int = 5
    circuit_breaker_recovery_timeout: float = 30.0
    # Feature flags
    enable_idempotency: bool = True
    enable_temporal_safety: bool = True
    enable_chaos_testing: bool = False
    # Sub-configs
    mopd: MOPDConfig = field(default_factory=MOPDConfig)
    federated: FederatedConfig = field(default_factory=FederatedConfig)
    approval: ApprovalConfig = field(default_factory=ApprovalConfig)
    chaos: ChaosConfig = field(default_factory=ChaosConfig)

    def __post_init__(self) -> None:
        if self.energy_threshold_joules < 0:
            raise ValueError("energy_threshold_joules must be >= 0")
        if not (0 <= self.accuracy_drop_tolerance <= 1):
            raise ValueError("accuracy_drop_tolerance in [0, 1]")
        if not (0 < self.svd_rank_factor <= 1):
            raise ValueError("svd_rank_factor in (0, 1]")
        if not (0 < self.rollback_accuracy_threshold <= 1):
            raise ValueError("rollback_accuracy_threshold in (0, 1]")
        if not self.hardware_profiles:
            raise ValueError("hardware_profiles must be non-empty")
        if self.hardware_profile not in self.hardware_profiles:
            raise ValueError(
                f"hardware_profile '{self.hardware_profile}' not in "
                f"{sorted(self.hardware_profiles.keys())}"
            )
        total = (self.fitness_accuracy_weight + self.fitness_energy_weight +
                 self.fitness_carbon_weight + self.fitness_material_weight)
        if total <= 0:
            raise ValueError("Fitness weights must sum to > 0")
        if abs(total - 1.0) > 1e-6:
            logger.warning("Fitness weights sum to %.4f; normalising.", total)
            self.fitness_accuracy_weight /= total
            self.fitness_energy_weight /= total
            self.fitness_carbon_weight /= total
            self.fitness_material_weight /= total

    def get_energy_coeff(self, profile: Optional[str] = None) -> float:
        profile = profile or self.hardware_profile
        return self.hardware_profiles.get(profile, self.energy_per_mac)

    def get_material_index(self, profile: Optional[str] = None) -> float:
        profile = profile or self.hardware_profile
        return self.material_index_map.get(profile, 0.5)

    def to_dict(self) -> Dict[str, Any]:
        return _to_serializable(self)


# ============================================================================
# Data models
# ============================================================================
@dataclass
class MOPDPoint:
    method: str
    accuracy: float
    energy: float
    carbon_savings_kg: float
    material_index: float
    scalarised_score: float = 0.0
    explanation: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            'method': self.method,
            'accuracy': float(self.accuracy),
            'energy': float(self.energy),
            'carbon_savings_kg': float(self.carbon_savings_kg),
            'material_index': float(self.material_index),
            'scalarised_score': float(self.scalarised_score),
            'explanation': self.explanation,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MOPDPoint":
        return cls(
            method=data['method'],
            accuracy=float(data['accuracy']),
            energy=float(data['energy']),
            carbon_savings_kg=float(data['carbon_savings_kg']),
            material_index=float(data['material_index']),
            scalarised_score=float(data.get('scalarised_score', 0.0)),
            explanation=str(data.get('explanation', '')),
        )

    def objective_vector(self) -> List[float]:
        vec: List[float] = []
        for name in MOPD_OBJECTIVE_NAMES:
            val = float(getattr(self, name, 0.0))
            if name in MOPD_HIGHER_IS_BETTER:
                val = -val
            vec.append(val)
        return vec


@dataclass
class SustainabilityAwareExpertProfile:
    expert_id: str
    model_path: Optional[str] = None
    node_id: Optional[str] = None
    compressed_flag: bool = False
    compression_method: Optional[str] = None
    energy_per_inference_full: float = float('inf')
    energy_per_inference_compressed: Optional[float] = None
    accuracy_full: float = 0.0
    accuracy_compressed: Optional[float] = None
    sustainability_fitness_score: float = 0.0
    carbon_savings_kg: float = 0.0
    material_index: float = 0.5
    last_compressed_at: Optional[datetime] = None
    compression_history: List[Dict[str, Any]] = field(default_factory=list)
    pareto_front: Deque[MOPDPoint] = field(default_factory=lambda: deque(maxlen=MAX_PARETO_CACHE))

    def update_material_index(self, config: SustainabilityConfig) -> None:
        self.material_index = config.get_material_index()


# ============================================================================
# Async SQLite history
# ============================================================================
class CompressionHistoryManager:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        os.makedirs(os.path.dirname(os.path.abspath(db_path)) or '.', exist_ok=True)
        self._lock = asyncio.Lock()
        self._init_db()

    def _init_db(self) -> None:
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS compression_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    expert_id TEXT NOT NULL,
                    method TEXT NOT NULL,
                    energy_before REAL,
                    energy_after REAL,
                    accuracy_before REAL,
                    accuracy_after REAL,
                    carbon_savings_kg REAL,
                    hardware_profile TEXT,
                    timestamp TEXT NOT NULL
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS pareto_fronts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    expert_id TEXT NOT NULL,
                    method TEXT NOT NULL,
                    accuracy REAL,
                    energy REAL,
                    carbon_savings_kg REAL,
                    material_index REAL,
                    scalarised_score REAL,
                    explanation TEXT,
                    timestamp TEXT NOT NULL
                )
            """)
            conn.commit()
        finally:
            conn.close()

    async def record(self, expert_id: str, method: str,
                     energy_before: float, energy_after: float,
                     accuracy_before: float, accuracy_after: float,
                     carbon_savings_kg: float, hardware_profile: str) -> None:
        ts = datetime.now(timezone.utc).isoformat()

        def _write() -> None:
            conn = sqlite3.connect(self.db_path)
            try:
                conn.execute(
                    "INSERT INTO compression_events "
                    "(expert_id, method, energy_before, energy_after, "
                    "accuracy_before, accuracy_after, carbon_savings_kg, "
                    "hardware_profile, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (expert_id, method, energy_before, energy_after,
                     accuracy_before, accuracy_after, carbon_savings_kg,
                     hardware_profile, ts),
                )
                conn.commit()
            finally:
                conn.close()

        async with self._lock:
            await asyncio.to_thread(_write)

    async def record_pareto_front(self, expert_id: str,
                                  pareto_front: Sequence[MOPDPoint]) -> None:
        if not pareto_front:
            return
        ts = datetime.now(timezone.utc).isoformat()

        def _write() -> None:
            conn = sqlite3.connect(self.db_path)
            try:
                for p in pareto_front:
                    conn.execute(
                        "INSERT INTO pareto_fronts "
                        "(expert_id, method, accuracy, energy, carbon_savings_kg, "
                        "material_index, scalarised_score, explanation, timestamp) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (expert_id, p.method, p.accuracy, p.energy,
                         p.carbon_savings_kg, p.material_index,
                         p.scalarised_score, p.explanation, ts),
                    )
                conn.commit()
            finally:
                conn.close()

        async with self._lock:
            await asyncio.to_thread(_write)

    async def get_history(self, expert_id: str, limit: int = 10) -> List[Dict[str, Any]]:
        def _read() -> List[Tuple[Any, ...]]:
            conn = sqlite3.connect(self.db_path)
            try:
                cur = conn.execute(
                    "SELECT method, energy_before, energy_after, accuracy_before, "
                    "accuracy_after, carbon_savings_kg, hardware_profile, timestamp "
                    "FROM compression_events WHERE expert_id = ? "
                    "ORDER BY timestamp DESC LIMIT ?",
                    (expert_id, limit),
                )
                return cur.fetchall()
            finally:
                conn.close()

        rows = await asyncio.to_thread(_read)
        return [{
            'method': r[0], 'energy_before': r[1], 'energy_after': r[2],
            'accuracy_before': r[3], 'accuracy_after': r[4],
            'carbon_savings_kg': r[5], 'hardware_profile': r[6], 'timestamp': r[7],
        } for r in rows]

    async def get_latest(self, expert_id: str) -> Optional[Dict[str, Any]]:
        history = await self.get_history(expert_id, limit=1)
        return history[0] if history else None

    async def get_pareto_front(self, expert_id: str, limit: int = 50) -> List[MOPDPoint]:
        def _read() -> List[Tuple[Any, ...]]:
            conn = sqlite3.connect(self.db_path)
            try:
                cur = conn.execute(
                    "SELECT method, accuracy, energy, carbon_savings_kg, "
                    "material_index, scalarised_score, explanation, timestamp "
                    "FROM pareto_fronts WHERE expert_id = ? "
                    "ORDER BY timestamp DESC LIMIT ?",
                    (expert_id, limit),
                )
                return cur.fetchall()
            finally:
                conn.close()

        rows = await asyncio.to_thread(_read)
        return [
            MOPDPoint(
                method=r[0], accuracy=float(r[1]), energy=float(r[2]),
                carbon_savings_kg=float(r[3]), material_index=float(r[4]),
                scalarised_score=float(r[5]), explanation=r[6],
            )
            for r in rows
        ]


# ============================================================================
# Storage
# ============================================================================
class CompressedModelStorage:
    def __init__(self, storage_dir: str) -> None:
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)

    def _model_path(self, expert_id: str, method: str) -> Path:
        safe_id = hashlib.md5(expert_id.encode()).hexdigest()[:8]
        return self.storage_dir / f"{expert_id}_{method}_{safe_id}.pt"

    def save(self, expert_id: str, method: str, model: Any,
             profile: SustainabilityAwareExpertProfile) -> None:
        if not TORCH_AVAILABLE:
            return
        path = self._model_path(expert_id, method)
        torch.save({  # type: ignore
            'model_state_dict': model.state_dict(),
            'method': method,
            'energy': profile.energy_per_inference_compressed,
            'accuracy': profile.accuracy_compressed,
            'timestamp': datetime.now(timezone.utc).isoformat(),
        }, path)

    def load(self, expert_id: str, method: str, model: Any) -> bool:
        if not TORCH_AVAILABLE:
            return False
        path = self._model_path(expert_id, method)
        if not path.exists():
            return False
        try:
            data = torch.load(path, map_location='cpu', weights_only=True)  # type: ignore
        except TypeError:
            data = torch.load(path, map_location='cpu')  # type: ignore
        model.load_state_dict(data['model_state_dict'])
        return True


# ============================================================================
# Chaos Service (non-destructive)
# ============================================================================
class ChaosService:
    def __init__(self, compressor: "SustainabilityCompressor",
                 config: ChaosConfig) -> None:
        self.compressor = compressor
        self.config = config
        self._saved: Dict[str, Any] = {}
        self._injected: List[str] = []
        self._lock = asyncio.Lock()

    async def inject(self, fault_type: FaultType, **params: Any) -> Dict[str, Any]:
        if not self.config.enabled:
            return {'status': 'disabled'}
        async with self._lock:
            try:
                if fault_type == FaultType.CARBON_API_FAILURE:
                    self._saved.setdefault('carbon_manager', self.compressor.carbon_manager)
                    self.compressor.carbon_manager = CarbonIntensityManagerStub()
                    self._injected.append(fault_type.value)
                    return {'status': 'injected'}

                if fault_type == FaultType.TELEMETRY_FAILURE:
                    self._saved.setdefault('telemetry', self.compressor.telemetry)
                    self.compressor.telemetry = TelemetryCollectorStub()
                    self._injected.append(fault_type.value)
                    return {'status': 'injected'}

                if fault_type == FaultType.HIGH_ENERGY:
                    self._saved.setdefault('energy_threshold',
                                            self.compressor.config.energy_threshold_joules)
                    self.compressor.config.energy_threshold_joules = 0.0
                    self._injected.append(fault_type.value)
                    return {'status': 'injected'}

                if fault_type == FaultType.QUANTIZATION_FAILURE:
                    self._saved.setdefault('quantize_fn',
                                            getattr(self.compressor, '_quantize_fn', None))
                    self.compressor._quantize_fn = None  # type: ignore
                    self._injected.append(fault_type.value)
                    return {'status': 'injected'}

                if fault_type == FaultType.SVD_SINGULARITY:
                    self._saved.setdefault('svd_rank_factor',
                                            self.compressor.config.svd_rank_factor)
                    self.compressor.config.svd_rank_factor = 0.999
                    self._injected.append(fault_type.value)
                    return {'status': 'injected'}

                if fault_type == FaultType.STORAGE_CORRUPTION:
                    self._saved.setdefault('storage', self.compressor.storage)
                    self.compressor.storage = None  # type: ignore
                    self._injected.append(fault_type.value)
                    return {'status': 'injected'}

                if fault_type == FaultType.COMPRESSOR_FAILURE:
                    self._saved.setdefault('accuracy_fn', self.compressor.accuracy_fn)

                    def _fail(model: Any, loader: Any) -> float:
                        raise RuntimeError("Simulated compressor failure")

                    self.compressor.accuracy_fn = _fail  # type: ignore
                    self._injected.append(fault_type.value)
                    return {'status': 'injected'}
            except Exception as e:
                logger.exception("Chaos injection failed: %s", e)
                return {'status': 'error', 'error': str(e)}
            return {'status': 'unknown_fault'}

    def restore(self) -> None:
        c = self.compressor
        if 'carbon_manager' in self._saved:
            c.carbon_manager = self._saved.pop('carbon_manager')
        if 'telemetry' in self._saved:
            c.telemetry = self._saved.pop('telemetry')
        if 'energy_threshold' in self._saved:
            c.config.energy_threshold_joules = self._saved.pop('energy_threshold')
        if 'quantize_fn' in self._saved:
            c._quantize_fn = self._saved.pop('quantize_fn')
        if 'svd_rank_factor' in self._saved:
            c.config.svd_rank_factor = self._saved.pop('svd_rank_factor')
        if 'storage' in self._saved:
            c.storage = self._saved.pop('storage')
        if 'accuracy_fn' in self._saved:
            c.accuracy_fn = self._saved.pop('accuracy_fn')
        self._injected.clear()

    async def run_test(self, val_loader: Any,
                       sample_input: Any) -> Dict[str, Any]:
        if not self.config.enabled:
            return {'status': 'disabled'}
        if val_loader is None or sample_input is None:
            return {'status': 'skipped',
                    'reason': 'no_val_loader_or_sample_input'}
        report: Dict[str, Any] = {'faults': [], 'results': {}, 'restored': False}
        faults = [
            FaultType.CARBON_API_FAILURE, FaultType.TELEMETRY_FAILURE,
            FaultType.HIGH_ENERGY, FaultType.QUANTIZATION_FAILURE,
            FaultType.SVD_SINGULARITY, FaultType.STORAGE_CORRUPTION,
        ]
        try:
            for ft in faults[:CHAOS_MAX_FAULTS_PER_RUN]:
                res = await self.inject(ft)
                report['faults'].append(ft.value)
                report['results'][ft.value] = res
            try:
                success = await self.compressor.evaluate_tradeoff_and_compress(
                    val_loader, sample_input,
                )
                report['results']['post_fault_compress'] = {
                    'success': bool(success),
                }
            except Exception as e:
                report['results']['post_fault_compress'] = {
                    'success': False, 'error': type(e).__name__,
                }
        finally:
            if self.config.auto_restore:
                self.restore()
                report['restored'] = True
        return report


# ============================================================================
# Compressor
# ============================================================================
class SustainabilityCompressor:
    def __init__(
        self,
        model: Any,
        profile: SustainabilityAwareExpertProfile,
        config: Optional[SustainabilityConfig] = None,
        telemetry: Optional[TelemetryProtocol] = None,
        carbon_manager: Optional[CarbonManagerProtocol] = None,
        history_manager: Optional[CompressionHistoryManager] = None,
        storage: Optional[CompressedModelStorage] = None,
        accuracy_fn: Optional[Callable[[Any, Any], float]] = None,
        governance: Optional[GovernanceLayer] = None,
        observability: Optional[ObservabilityHub] = None,
    ) -> None:
        if not TORCH_AVAILABLE:
            raise RuntimeError("PyTorch is required for SustainabilityCompressor")
        self.model = model
        self.profile = profile
        self.config = config or SustainabilityConfig()
        self.telemetry: TelemetryProtocol = telemetry or TelemetryCollectorStub()  # type: ignore
        self.carbon_manager: CarbonManagerProtocol = carbon_manager or CarbonIntensityManagerStub()  # type: ignore
        self.history_manager = history_manager
        self.storage = storage
        self.hardware_profile = self.config.hardware_profile
        self.accuracy_fn = accuracy_fn or self._default_accuracy_fn
        self.governance = governance or GovernanceLayer(enabled=False)
        self.observability = observability or ObservabilityHub()

        self._original_state_dict = copy.deepcopy(model.state_dict())
        self._quantize_fn: Optional[Callable[[Any], Any]] = None

        self._idempotency = IdempotencyCache()
        self._dead_letters: Deque[Dict[str, Any]] = deque(maxlen=MAX_DEAD_LETTERS)

        # Safety
        self.safety_monitor = SafetyMonitor()
        self._setup_invariants()

        # Chaos
        self.chaos_service: Optional[ChaosService] = (
            ChaosService(self, self.config.chaos)
            if self.config.enable_chaos_testing else None
        )

        if self.profile.material_index == 0.0:
            self.profile.update_material_index(self.config)

    # ------------------------------------------------------------------
    # Safety
    # ------------------------------------------------------------------
    def _setup_invariants(self) -> None:
        def _acc_drop_nonneg(state: Dict[str, Any]) -> bool:
            return float(state.get('accuracy_drop', 0.0)) >= -1e-6

        def _energy_saved_nonneg(state: Dict[str, Any]) -> bool:
            return float(state.get('energy_saved', 0.0)) >= -1e-6

        def _carbon_nonneg(state: Dict[str, Any]) -> bool:
            return float(state.get('carbon_savings_kg', 0.0)) >= -1e-6

        self.safety_monitor.add('acc_drop_nonneg', _acc_drop_nonneg,
                                'Accuracy drop must be non-negative')
        self.safety_monitor.add('energy_saved_nonneg', _energy_saved_nonneg,
                                'Energy saved must be non-negative')
        self.safety_monitor.add('carbon_nonneg', _carbon_nonneg,
                                'Carbon savings must be non-negative')

    # ------------------------------------------------------------------
    # Accuracy / energy
    # ------------------------------------------------------------------
    def _default_accuracy_fn(self, model: Any, val_loader: Any) -> float:
        model.eval()
        correct = 0
        total = 0
        with torch.no_grad():  # type: ignore
            for batch in val_loader:
                if isinstance(batch, (tuple, list)) and len(batch) >= 2:
                    inputs, labels = batch[0], batch[1]
                    outputs = model(inputs)
                    _, predicted = torch.max(outputs.data, 1)  # type: ignore
                    total += labels.size(0)
                    correct += (predicted == labels).sum().item()
        return correct / total if total > 0 else 0.0

    async def _estimate_energy_real(self, model: Any, sample_input: Any) -> float:
        try:
            energy = await self.telemetry.get_energy_per_inference(self.profile.expert_id)
            if energy is not None and math.isfinite(energy) and energy > 0:
                return float(energy)
        except Exception:
            pass
        return self._estimate_energy_flops(model, sample_input)

    def _estimate_energy_flops(self, model: Any, sample_input: Any) -> float:
        flops = 0
        if THOP_AVAILABLE and _thop_profile is not None:
            try:
                flops, _ = _thop_profile(model, inputs=(sample_input,), verbose=False)
            except Exception:
                flops = 0
        if not flops:
            for module in model.modules():
                if isinstance(module, nn.Linear):
                    flops += module.in_features * module.out_features * 2
                elif isinstance(module, nn.Conv2d):
                    k = module.kernel_size[0] * module.kernel_size[1]
                    flops += k * module.in_channels * module.out_channels * 2
        coeff = self.config.get_energy_coeff(self.hardware_profile)
        return float(flops) * coeff

    # ------------------------------------------------------------------
    # Compression methods
    # ------------------------------------------------------------------
    def _apply_structured_pruning(self, model: Any, sparsity: Optional[float] = None,
                                  dim: int = 0) -> Any:
        if sparsity is None:
            sparsity = self.config.pruning_sparsity
        pruned_any = False
        for module in model.modules():
            if isinstance(module, nn.Conv2d):
                prune.ln_structured(module, name='weight', amount=sparsity, n=2, dim=dim)
                prune.remove(module, 'weight')
                pruned_any = True
            elif isinstance(module, nn.Linear):
                prune.ln_structured(module, name='weight', amount=sparsity, n=2, dim=1)
                prune.remove(module, 'weight')
                pruned_any = True
        if not pruned_any:
            logger.debug("Structured pruning: no Conv2d or Linear layers found")
        return model

    def _apply_unstructured_pruning(self, model: Any,
                                    sparsity: Optional[float] = None) -> Any:
        if sparsity is None:
            sparsity = self.config.pruning_sparsity
        params: List[Tuple[Any, str]] = []
        for module in model.modules():
            if isinstance(module, (nn.Linear, nn.Conv2d)):
                params.append((module, 'weight'))
        if not params:
            return model
        prune.global_unstructured(params, pruning_method=prune.L1Unstructured,
                                  amount=sparsity)
        for module, _ in params:
            prune.remove(module, 'weight')
        return model

    def _apply_int8_quantization(self, model: Any) -> Any:
        fn = self._quantize_fn or quantize_dynamic
        if fn is None or not QUANT_AVAILABLE:
            raise RuntimeError("quantize_dynamic not available")
        return fn(model, {nn.Linear}, dtype=torch.qint8)  # type: ignore

    def _apply_hybrid(self, model: Any) -> Any:
        model = self._apply_unstructured_pruning(
            model, sparsity=self.config.hybrid_pruning_sparsity,
        )
        model = self._apply_int8_quantization(model)
        return model

    def _apply_svd(self, model: Any, rank_factor: Optional[float] = None) -> Any:
        """Replace each Linear with two smaller Linears (true SVD)."""
        if rank_factor is None:
            rank_factor = self.config.svd_rank_factor

        def replace(module: Any) -> None:
            for name, child in list(module.named_children()):
                if isinstance(child, nn.Linear):
                    in_f, out_f = child.in_features, child.out_features
                    k = max(MIN_SVD_RANK, int(min(in_f, out_f) * rank_factor))
                    if k >= min(in_f, out_f):
                        continue
                    try:
                        U, S, Vh = torch.linalg.svd(child.weight.data, full_matrices=False)  # type: ignore
                    except Exception as e:
                        logger.warning("SVD failed for layer %s: %s", name, e)
                        continue
                    sqrt_S = torch.sqrt(S[:k])  # type: ignore
                    A = U[:, :k] * sqrt_S.unsqueeze(0)
                    B = sqrt_S.unsqueeze(1) * Vh[:k, :]
                    new_l1 = nn.Linear(in_f, k, bias=False)
                    new_l2 = nn.Linear(k, out_f, bias=child.bias is not None)
                    new_l1.weight.data = B.contiguous()
                    new_l2.weight.data = A.contiguous()
                    if child.bias is not None:
                        new_l2.bias.data = child.bias.data.clone()
                    setattr(module, name, nn.Sequential(new_l1, new_l2))
                else:
                    replace(child)

        replace(model)
        return model

    # ------------------------------------------------------------------
    # Model helpers
    # ------------------------------------------------------------------
    def _copy_model(self) -> Any:
        return copy.deepcopy(self.model)

    def _restore_original(self) -> None:
        self.model.load_state_dict(self._original_state_dict)

    def _evaluate_accuracy(self, model: Any, val_loader: Any) -> float:
        try:
            return float(self.accuracy_fn(model, val_loader))
        except Exception as e:
            logger.warning("Accuracy evaluation failed: %s", e)
            return 0.0

    # ------------------------------------------------------------------
    # Pareto front generation
    # ------------------------------------------------------------------
    async def _generate_pareto_front(
        self,
        val_loader: Any,
        sample_input: Any,
        baseline_acc: float,
        baseline_energy: float,
    ) -> List[MOPDPoint]:
        methods: List[Tuple[str, Callable[..., Any]]] = [
            ('structured_pruning', self._apply_structured_pruning),
            ('unstructured_pruning', self._apply_unstructured_pruning),
            ('int8_quant', self._apply_int8_quantization),
            ('hybrid', self._apply_hybrid),
            ('svd', self._apply_svd),
        ]
        points: List[MOPDPoint] = []
        for name, fn in methods:
            model_copy = self._copy_model()
            try:
                if name == 'structured_pruning':
                    model_copy = self._apply_structured_pruning(
                        model_copy, self.config.pruning_sparsity,
                    )
                elif name == 'unstructured_pruning':
                    model_copy = self._apply_unstructured_pruning(
                        model_copy, self.config.pruning_sparsity,
                    )
                elif name == 'int8_quant':
                    model_copy = self._apply_int8_quantization(model_copy)
                elif name == 'hybrid':
                    model_copy = self._apply_hybrid(model_copy)
                elif name == 'svd':
                    model_copy = self._apply_svd(model_copy, self.config.svd_rank_factor)

                acc = self._evaluate_accuracy(model_copy, val_loader)
                energy = await self._estimate_energy_real(model_copy, sample_input)

                carbon_savings = 0.0
                try:
                    intensity = await self.carbon_manager.get_current_intensity()
                    energy_saved_j = max(0.0, baseline_energy - energy)
                    carbon_savings = (energy_saved_j / 3.6e6) * (float(intensity) / 1000.0)
                except Exception:
                    pass

                # Tolerance check (absolute or relative)
                absolute_drop = baseline_acc - acc
                relative_drop = absolute_drop / max(baseline_acc, 1e-9)
                tolerance = min(
                    self.config.accuracy_drop_tolerance,
                    self.config.relative_accuracy_drop_tolerance * baseline_acc,
                ) if baseline_acc > 0 else self.config.accuracy_drop_tolerance

                if absolute_drop <= tolerance or relative_drop <= self.config.relative_accuracy_drop_tolerance:
                    explanation = (
                        f"method={name}, acc={acc:.4f} "
                        f"(Δ={absolute_drop:+.4f}), energy={energy:.4f}J "
                        f"(Δ={baseline_energy - energy:+.4f}), "
                        f"carbon_saved={carbon_savings:.6f}kg"
                    )
                    points.append(MOPDPoint(
                        method=name,
                        accuracy=float(acc),
                        energy=float(energy),
                        carbon_savings_kg=float(carbon_savings),
                        material_index=float(self.profile.material_index),
                        explanation=explanation,
                    ))
            except Exception as e:
                logger.warning("Compression method %s failed: %s", name, e)
            finally:
                del model_copy

        if not points:
            return []
        return filter_pareto(points)

    # ------------------------------------------------------------------
    # Main compression
    # ------------------------------------------------------------------
    async def evaluate_tradeoff_and_compress(
        self,
        val_loader: Any,
        sample_input: Any,
        use_mopd: bool = True,
        idempotency_key: Optional[str] = None,
    ) -> bool:
        if self._idempotency.seen(idempotency_key):
            logger.info("Skipping duplicate compression request")
            return False

        self._restore_original()
        baseline_acc = self._evaluate_accuracy(self.model, val_loader)
        baseline_energy = await self._estimate_energy_real(self.model, sample_input)

        self.profile.accuracy_full = baseline_acc
        self.profile.energy_per_inference_full = baseline_energy

        if baseline_energy <= self.config.energy_threshold_joules:
            logger.info(
                "Expert %s energy (%.2f J) within threshold; skipping.",
                self.profile.expert_id, baseline_energy,
            )
            return False

        pareto_front = await self._generate_pareto_front(
            val_loader, sample_input, baseline_acc, baseline_energy,
        )
        if not pareto_front:
            logger.warning("No viable compression candidates for %s",
                           self.profile.expert_id)
            self._restore_original()
            return False

        self.profile.pareto_front = deque(pareto_front, maxlen=MAX_PARETO_CACHE)

        best: Optional[MOPDPoint] = None
        if use_mopd and self.config.mopd.enabled:
            best = scalarise_pareto(pareto_front,
                                     weights=self.config.mopd.objective_weights)
        else:
            best = min(pareto_front, key=lambda p: p.energy)

        if best is None:
            self._restore_original()
            return False

        accuracy_drop = baseline_acc - best.accuracy

        # Temporal safety
        if self.config.enable_temporal_safety:
            violations = self.safety_monitor.check({
                'accuracy_drop': accuracy_drop,
                'energy_saved': baseline_energy - best.energy,
                'carbon_savings_kg': best.carbon_savings_kg,
            })
            if violations:
                logger.warning("Safety violations: %s", violations)
                self._restore_original()
                return False

        # Human approval
        if (self.governance.enabled and
                accuracy_drop > self.config.approval.require_for_accuracy_drop_above):
            approval = await self.governance.request_approval({
                'action': 'approve_compression',
                'expert_id': self.profile.expert_id,
                'method': best.method,
                'accuracy_drop': accuracy_drop,
                'energy_savings': baseline_energy - best.energy,
                'explanation': best.explanation,
            })
            if approval.status not in (ApprovalStatus.APPROVED,
                                        ApprovalStatus.AUTO_APPROVED):
                logger.info("Compression not approved: %s", approval.status.value)
                self._restore_original()
                return False

        # Apply the chosen method
        method_name = best.method
        try:
            if method_name == 'structured_pruning':
                model_copy = self._apply_structured_pruning(
                    self._copy_model(), self.config.pruning_sparsity,
                )
            elif method_name == 'unstructured_pruning':
                model_copy = self._apply_unstructured_pruning(
                    self._copy_model(), self.config.pruning_sparsity,
                )
            elif method_name == 'int8_quant':
                model_copy = self._apply_int8_quantization(self._copy_model())
            elif method_name == 'hybrid':
                model_copy = self._apply_hybrid(self._copy_model())
            elif method_name == 'svd':
                model_copy = self._apply_svd(self._copy_model(),
                                              self.config.svd_rank_factor)
            else:
                logger.warning("Unknown method: %s", method_name)
                self._restore_original()
                return False
        except Exception as e:
            logger.error("Failed to apply %s: %s", method_name, e)
            self._restore_original()
            return False

        self.model.load_state_dict(model_copy.state_dict())
        del model_copy

        self.profile.compressed_flag = True
        self.profile.compression_method = method_name
        self.profile.accuracy_compressed = best.accuracy
        self.profile.energy_per_inference_compressed = best.energy
        self.profile.carbon_savings_kg = best.carbon_savings_kg
        self.profile.last_compressed_at = datetime.now(timezone.utc)

        if self.storage is not None:
            try:
                self.storage.save(self.profile.expert_id, method_name,
                                  self.model, self.profile)
            except Exception as e:
                logger.warning("Storage save failed: %s", e)

        if self.history_manager is not None:
            try:
                await self.history_manager.record(
                    self.profile.expert_id, method_name,
                    baseline_energy, best.energy,
                    baseline_acc, best.accuracy,
                    best.carbon_savings_kg, self.hardware_profile,
                )
                await self.history_manager.record_pareto_front(
                    self.profile.expert_id, pareto_front,
                )
            except Exception as e:
                logger.warning("History persistence failed: %s", e)

        # Telemetry
        try:
            await self.telemetry.increment(f"{self.config.telemetry_prefix}.compressions_total")
            await self.telemetry.gauge(f"{self.config.telemetry_prefix}.energy_saved_joules",
                                        baseline_energy - best.energy)
            await self.telemetry.gauge(f"{self.config.telemetry_prefix}.carbon_saved_kg",
                                        best.carbon_savings_kg)
        except Exception:
            pass

        # Observability
        self.observability.counter('compressions_total', method=method_name)
        self.observability.gauge('energy_saved_joules', baseline_energy - best.energy)
        self.observability.gauge('carbon_saved_kg', best.carbon_savings_kg)
        self.observability.histogram('pareto_front_size', len(pareto_front))

        logger.info(
            "Expert %s compressed with %s. Energy: %.4f → %.4f J, "
            "Accuracy: %.4f → %.4f, Carbon saved: %.6f kg",
            self.profile.expert_id, method_name, baseline_energy, best.energy,
            baseline_acc, best.accuracy, best.carbon_savings_kg,
        )
        return True

    # ------------------------------------------------------------------
    # Chaos API
    # ------------------------------------------------------------------
    async def inject_fault(self, fault_type: str, **params: Any) -> Dict[str, Any]:
        if self.chaos_service is None:
            return {'status': 'disabled'}
        try:
            ft = FaultType(fault_type)
        except ValueError:
            logger.warning("Unknown fault type: %s", fault_type)
            return {'status': 'unknown_fault'}
        return await self.chaos_service.inject(ft, **params)

    async def run_chaos_test(self, val_loader: Any = None,
                             sample_input: Any = None) -> Dict[str, Any]:
        if self.chaos_service is None:
            return {'status': 'disabled'}
        return await self.chaos_service.run_test(val_loader, sample_input)

    async def get_prometheus_metrics(self) -> str:
        return await self.observability.export_prometheus()

    def get_dead_letters(self) -> List[Dict[str, Any]]:
        return list(self._dead_letters)


# ============================================================================
# Fitness scorer
# ============================================================================
class SustainabilityFitnessScorer:
    def __init__(self, config: Optional[SustainabilityConfig] = None) -> None:
        self.config = config or SustainabilityConfig()

    def compute(self, profile: SustainabilityAwareExpertProfile) -> float:
        if profile.compressed_flag:
            acc = profile.accuracy_compressed or 0.0
            energy = profile.energy_per_inference_compressed or 0.0
        else:
            acc = profile.accuracy_full
            energy = profile.energy_per_inference_full

        # Prefer MOPD weights when available
        mopd_w = self.config.mopd.objective_weights if self.config.mopd.enabled else None
        if mopd_w:
            acc_w = mopd_w.get('accuracy', 0.4)
            energy_w = mopd_w.get('energy', 0.3)
            carbon_w = mopd_w.get('carbon_savings_kg', 0.2)
            material_w = mopd_w.get('material_index', 0.1)
        else:
            acc_w = self.config.fitness_accuracy_weight
            energy_w = self.config.fitness_energy_weight
            carbon_w = self.config.fitness_carbon_weight
            material_w = self.config.fitness_material_weight

        normalized_energy = (
            max(0.0, 1.0 - (energy / self.config.max_energy_joules))
            if math.isfinite(energy) else 0.0
        )
        carbon_score = min(1.0, profile.carbon_savings_kg / self.config.carbon_savings_scale_kg)
        material_score = 1.0 - profile.material_index
        fitness = (
            acc_w * acc +
            energy_w * normalized_energy +
            carbon_w * carbon_score +
            material_w * material_score
        )
        bonus = self.config.compression_bonus if profile.compressed_flag else 0.0
        profile.sustainability_fitness_score = fitness + bonus
        return profile.sustainability_fitness_score


# ============================================================================
# Sub-services for MLOpsPipelineExtension
# ============================================================================
class RegistrationService:
    def __init__(self, ext: "MLOpsPipelineExtension") -> None:
        self.ext = ext

    async def register(
        self,
        expert_id: str,
        model: Any,
        profile: SustainabilityAwareExpertProfile,
        val_loader: Any,
        idempotency_key: Optional[str] = None,
    ) -> bool:
        ext = self.ext
        if ext._idempotency.seen(idempotency_key):
            return False
        if expert_id in ext.pipeline.model_registry:
            logger.info("Expert %s already registered", expert_id)
            return False

        ext._ensure_pipeline()
        profile.update_material_index(ext.config)
        ext._full_models[expert_id] = _deepcopy_model(model)

        # Try restoring a compressed model
        if ext.storage is not None and ext.history_manager is not None:
            latest = await ext.history_manager.get_latest(expert_id)
            if latest:
                method = latest['method']
                try:
                    if ext.storage.load(expert_id, method, model):
                        async with ext._lock:
                            profile.compressed_flag = True
                            profile.compression_method = method
                            profile.accuracy_compressed = latest['accuracy_after']
                            profile.energy_per_inference_compressed = latest['energy_after']
                            profile.carbon_savings_kg = latest['carbon_savings_kg']
                            try:
                                profile.last_compressed_at = datetime.fromisoformat(
                                    latest['timestamp'],
                                )
                            except Exception:
                                profile.last_compressed_at = None
                            ext.pipeline.model_registry[expert_id] = model
                            ext.pipeline.profile_registry[expert_id] = profile
                            if profile.accuracy_compressed is not None:
                                ext._compressed_acc_cache[expert_id] = float(
                                    profile.accuracy_compressed,
                                )
                            pareto = await ext.history_manager.get_pareto_front(expert_id)
                            if pareto:
                                profile.pareto_front = deque(pareto, maxlen=MAX_PARETO_CACHE)
                                self._cache_pareto(expert_id, pareto)
                        logger.info("Restored compressed model for %s", expert_id)
                        return True
                except Exception as e:
                    logger.warning("Failed to restore %s: %s", expert_id, e)

        # Trigger compression if needed
        if profile.energy_per_inference_full > ext.config.energy_threshold_joules:
            await self._compress_initial(expert_id, model, profile, val_loader)
        else:
            logger.info("Expert %s within energy threshold", expert_id)
            ext.pipeline.model_registry[expert_id] = model
            ext.pipeline.profile_registry[expert_id] = profile
        return True

    async def _compress_initial(self, expert_id: str, model: Any,
                                profile: SustainabilityAwareExpertProfile,
                                val_loader: Any) -> None:
        ext = self.ext
        sample_input = ext._sample_input_for(val_loader)
        if sample_input is None:
            ext.pipeline.model_registry[expert_id] = model
            ext.pipeline.profile_registry[expert_id] = profile
            return
        compressor = ext._make_compressor(model, profile)
        try:
            success = await compressor.evaluate_tradeoff_and_compress(
                val_loader, sample_input,
            )
        except Exception as e:
            logger.exception("Compression failed for %s: %s", expert_id, e)
            ext.pipeline.model_registry[expert_id] = model
            ext.pipeline.profile_registry[expert_id] = profile
            return
        async with ext._lock:
            if success:
                ext.pipeline.model_registry[expert_id] = compressor.model
                ext.pipeline.profile_registry[expert_id] = profile
                if profile.accuracy_compressed is not None:
                    ext._compressed_acc_cache[expert_id] = float(profile.accuracy_compressed)
                self._cache_pareto(expert_id, profile.pareto_front)
            else:
                ext.pipeline.model_registry[expert_id] = model
                ext.pipeline.profile_registry[expert_id] = profile

    def _cache_pareto(self, expert_id: str, pareto: Sequence[MOPDPoint]) -> None:
        ext = self.ext
        if not pareto:
            return
        if len(ext._pareto_fronts) >= MAX_PARETO_CACHE:
            ext._pareto_fronts.pop(next(iter(ext._pareto_fronts)))
        ext._pareto_fronts[expert_id] = list(pareto)


class RecompressionService:
    def __init__(self, ext: "MLOpsPipelineExtension") -> None:
        self.ext = ext

    async def run_once(self) -> Dict[str, int]:
        ext = self.ext
        ext._ensure_pipeline()
        stats = {'attempted': 0, 'compressed': 0, 'skipped': 0, 'failed': 0}
        for expert_id, model in list(ext.pipeline.model_registry.items()):
            profile = ext.pipeline.profile_registry.get(expert_id)
            if profile is None or profile.compressed_flag:
                stats['skipped'] += 1
                continue
            val_loader = ext.pipeline.val_loaders.get(expert_id)
            if val_loader is None:
                stats['skipped'] += 1
                continue
            sample_input = ext._sample_input_for(val_loader)
            if sample_input is None:
                stats['skipped'] += 1
                continue
            stats['attempted'] += 1
            compressor = ext._make_compressor(model, profile)
            try:
                success = await compressor.evaluate_tradeoff_and_compress(
                    val_loader, sample_input,
                )
            except Exception as e:
                logger.exception("Recompression failed for %s: %s", expert_id, e)
                stats['failed'] += 1
                continue
            if success:
                async with ext._lock:
                    ext.pipeline.model_registry[expert_id] = compressor.model
                    ext.pipeline.profile_registry[expert_id] = profile
                    if profile.accuracy_compressed is not None:
                        ext._compressed_acc_cache[expert_id] = float(profile.accuracy_compressed)
                stats['compressed'] += 1
            else:
                stats['skipped'] += 1
        return stats


class AnomalyService:
    def __init__(self, ext: "MLOpsPipelineExtension") -> None:
        self.ext = ext

    async def on_anomaly(self, node_id: str,
                         metrics: Dict[str, Any]) -> Dict[str, int]:
        ext = self.ext
        if not ext.config.anomaly_trigger_enabled:
            return {'triggered': 0}
        ext._ensure_pipeline()
        stats = {'triggered': 0, 'compressed': 0, 'failed': 0}
        async with ext._lock:
            matching = [
                (eid, p) for eid, p in ext.pipeline.profile_registry.items()
                if p.node_id == node_id and not p.compressed_flag
            ]
        for expert_id, profile in matching:
            stats['triggered'] += 1
            model = ext.pipeline.model_registry.get(expert_id)
            if model is None:
                continue
            val_loader = ext.pipeline.val_loaders.get(expert_id)
            if val_loader is None:
                continue
            sample_input = ext._sample_input_for(val_loader)
            if sample_input is None:
                continue

            approval = await ext.governance.request_approval({
                'action': 'anomaly_triggered_compression',
                'expert_id': expert_id,
                'node_id': node_id,
                'metrics': metrics,
            })
            if approval.status not in (ApprovalStatus.APPROVED,
                                        ApprovalStatus.AUTO_APPROVED):
                continue

            compressor = ext._make_compressor(model, profile)
            try:
                success = await compressor.evaluate_tradeoff_and_compress(
                    val_loader, sample_input,
                )
            except Exception as e:
                logger.exception("Anomaly compression failed for %s: %s",
                                 expert_id, e)
                stats['failed'] += 1
                continue
            if success:
                async with ext._lock:
                    ext.pipeline.model_registry[expert_id] = compressor.model
                    ext.pipeline.profile_registry[expert_id] = profile
                    if profile.accuracy_compressed is not None:
                        ext._compressed_acc_cache[expert_id] = float(profile.accuracy_compressed)
                stats['compressed'] += 1
        return stats


class RollbackService:
    def __init__(self, ext: "MLOpsPipelineExtension") -> None:
        self.ext = ext

    async def monitor(self, expert_id: str, current_accuracy: float) -> bool:
        ext = self.ext
        compressed_acc = ext._compressed_acc_cache.get(expert_id)
        if compressed_acc is None or compressed_acc <= 0:
            return False
        if current_accuracy >= compressed_acc * ext.config.rollback_accuracy_threshold:
            return False
        full_model = ext._full_models.get(expert_id)
        if full_model is None:
            logger.error("No full-model snapshot for %s", expert_id)
            return False
        profile = ext.pipeline.profile_registry.get(expert_id)
        if profile is None or not profile.compressed_flag:
            return False
        async with ext._lock:
            ext.pipeline.model_registry[expert_id] = full_model
            profile.compressed_flag = False
            profile.accuracy_compressed = None
            profile.energy_per_inference_compressed = None
            profile.compression_method = None
            ext._compressed_acc_cache.pop(expert_id, None)
            ext._pareto_fronts.pop(expert_id, None)
        ext.observability.counter('rollbacks_total', expert=expert_id)
        logger.info("Rolled back %s to full model", expert_id)
        return True


# ============================================================================
# MLOpsPipelineExtension
# ============================================================================
def _deepcopy_model(model: Any) -> Any:
    if not TORCH_AVAILABLE or model is None:
        return model
    try:
        return copy.deepcopy(model)
    except Exception as e:
        logger.warning("Model deepcopy failed: %s", e)
        return model


class MLOpsPipelineExtension:
    def __init__(
        self,
        pipeline: Any,
        config: Optional[SustainabilityConfig] = None,
        telemetry: Optional[TelemetryProtocol] = None,
        carbon_manager: Optional[CarbonManagerProtocol] = None,
        anomaly_detector: Optional[AnomalyDetectorProtocol] = None,
        accuracy_fn: Optional[Callable[[Any, Any], float]] = None,
        publish_fn: Optional[Callable[[str, Any], Awaitable[None]]] = None,
        observability: Optional[ObservabilityHub] = None,
    ) -> None:
        self.pipeline = pipeline
        self.config = config or SustainabilityConfig()
        self.telemetry = telemetry or TelemetryCollectorStub()
        self.carbon_manager = carbon_manager or CarbonIntensityManagerStub()
        self.anomaly_detector = anomaly_detector or AnomalyDetectorStub()
        self.accuracy_fn = accuracy_fn

        self.history_manager = CompressionHistoryManager(self.config.history_db_path)
        self.storage = CompressedModelStorage(self.config.compressed_model_dir)
        self.observability = observability or ObservabilityHub()

        self.governance = GovernanceLayer(
            enabled=self.config.approval.enabled,
            timeout_seconds=self.config.approval.timeout_seconds,
            auto_approve_on_timeout=self.config.approval.auto_approve_on_timeout,
            publish_fn=publish_fn,
        )

        self._idempotency = IdempotencyCache()
        self._dead_letters: Deque[Dict[str, Any]] = deque(maxlen=MAX_DEAD_LETTERS)
        self._compressed_acc_cache: Dict[str, float] = {}
        self._pareto_fronts: Dict[str, List[MOPDPoint]] = {}
        self._full_models: Dict[str, Any] = {}
        self._sample_cache: Dict[int, Any] = {}
        self._lock = asyncio.Lock()

        # Lifecycle
        self._started = False
        self._shutting_down = False
        self._background_tasks: List[asyncio.Task] = []
        self._recompress_task: Optional[asyncio.Task] = None
        self._chaos_task: Optional[asyncio.Task] = None

        # Sub-services
        self.registration_service = RegistrationService(self)
        self.recompression_service = RecompressionService(self)
        self.anomaly_service = AnomalyService(self)
        self.rollback_service = RollbackService(self)

        self._ensure_pipeline()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        if self.config.recompress_interval > 0:
            self._recompress_task = asyncio.create_task(self._recompress_loop())
            self._background_tasks.append(self._recompress_task)
        if self.config.enable_chaos_testing and self.config.chaos.enabled:
            self._chaos_task = asyncio.create_task(self._chaos_loop())
            self._background_tasks.append(self._chaos_task)
        logger.info("MLOpsPipelineExtension started")

    async def wait_ready(self) -> None:
        if not self._started:
            await self.start()

    async def shutdown(self) -> None:
        self._shutting_down = True
        for t in self._background_tasks:
            t.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)
        self._background_tasks.clear()
        try:
            await self.carbon_manager.close()
        except Exception:
            pass
        logger.info("MLOpsPipelineExtension shutdown complete")

    async def _recompress_loop(self) -> None:
        interval = max(60, self.config.recompress_interval)
        while not self._shutting_down:
            try:
                await asyncio.sleep(interval)
                await self.recompression_service.run_once()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Recompress loop error: %s", e)

    async def _chaos_loop(self) -> None:
        interval = max(60, self.config.chaos.interval_seconds)
        while not self._shutting_down:
            try:
                await asyncio.sleep(interval)
                # Chaos runs against a compressor instance externally
            except asyncio.CancelledError:
                break

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    async def on_expert_registered(
        self,
        expert_id: str,
        model: Any,
        profile: SustainabilityAwareExpertProfile,
        val_loader: Any,
        idempotency_key: Optional[str] = None,
    ) -> None:
        await self.wait_ready()
        key = idempotency_key or f"register:{expert_id}"
        await self.registration_service.register(
            expert_id, model, profile, val_loader, idempotency_key=key,
        )

    async def on_anomaly_detected(self, node_id: str,
                                  metrics: Dict[str, Any]) -> None:
        await self.wait_ready()
        await self.anomaly_service.on_anomaly(node_id, metrics)

    async def monitor_rollback(self, expert_id: str,
                               current_accuracy: float) -> None:
        await self.rollback_service.monitor(expert_id, current_accuracy)

    async def start_recompress_loop(self) -> None:
        await self.start()

    async def stop_recompress_loop(self) -> None:
        if self._recompress_task is not None:
            self._recompress_task.cancel()
            try:
                await self._recompress_task
            except asyncio.CancelledError:
                pass
            self._recompress_task = None

    async def get_prometheus_metrics(self) -> str:
        return await self.observability.export_prometheus()

    async def health_check(self) -> Dict[str, Any]:
        self._ensure_pipeline()
        compressed = sum(
            1 for p in self.pipeline.profile_registry.values()
            if getattr(p, 'compressed_flag', False)
        )
        return {
            'status': 'healthy',
            'started': self._started,
            'shutting_down': self._shutting_down,
            'num_experts': len(self.pipeline.model_registry),
            'compressed_count': compressed,
            'pareto_fronts_stored': len(self._pareto_fronts),
            'full_model_snapshots': len(self._full_models),
            'governance_pending': len(self.governance.pending),
            'dead_letters': len(self._dead_letters),
        }

    def get_dead_letters(self) -> List[Dict[str, Any]]:
        return list(self._dead_letters)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _ensure_pipeline(self) -> None:
        if not hasattr(self.pipeline, 'model_registry'):
            raise AttributeError("Pipeline must expose 'model_registry'")
        if not hasattr(self.pipeline, 'profile_registry'):
            raise AttributeError("Pipeline must expose 'profile_registry'")
        if not hasattr(self.pipeline, 'val_loaders'):
            self.pipeline.val_loaders = {}

    def _sample_input_for(self, val_loader: Any) -> Any:
        if val_loader is None:
            return None
        loader_id = id(val_loader)
        if loader_id in self._sample_cache:
            return self._sample_cache[loader_id]
        try:
            for batch in val_loader:
                if isinstance(batch, (tuple, list)) and len(batch) >= 1:
                    self._sample_cache[loader_id] = batch[0]
                    return batch[0]
                self._sample_cache[loader_id] = batch
                return batch
        except Exception as e:
            logger.warning("Could not extract sample input: %s", e)
        return None

    def _make_compressor(self, model: Any,
                         profile: SustainabilityAwareExpertProfile) -> SustainabilityCompressor:
        return SustainabilityCompressor(
            model=model,
            profile=profile,
            config=self.config,
            telemetry=self.telemetry,
            carbon_manager=self.carbon_manager,
            history_manager=self.history_manager,
            storage=self.storage,
            accuracy_fn=self.accuracy_fn,
            governance=self.governance,
            observability=self.observability,
        )


# ============================================================================
# Router
# ============================================================================
class SustainabilityAwareRouter:
    def __init__(
        self,
        base_router: Any,
        scorer: Optional[SustainabilityFitnessScorer] = None,
        config: Optional[SustainabilityConfig] = None,
        default_required_accuracy: float = 0.90,
        observability: Optional[ObservabilityHub] = None,
    ) -> None:
        self.base_router = base_router
        self.config = config or SustainabilityConfig()
        self.scorer = scorer or SustainabilityFitnessScorer(self.config)
        self.default_required_accuracy = default_required_accuracy
        self.observability = observability or ObservabilityHub()

    def route(
        self,
        query: Any,
        required_accuracy: Optional[float] = None,
        use_mopd: Optional[bool] = None,
        objective_weights: Optional[Dict[str, float]] = None,
    ) -> Any:
        required = required_accuracy if required_accuracy is not None else self.default_required_accuracy
        mopd_enabled = self.config.mopd.enabled
        use_mopd_flag = mopd_enabled if use_mopd is None else bool(use_mopd)

        try:
            candidates = self.base_router.get_all_experts(query)
        except Exception as e:
            logger.error("Base router failed: %s", e)
            return self.base_router.route(query)

        valid: List[Tuple[str, Any]] = []
        for exp_id, profile in candidates:
            acc = profile.accuracy_compressed if profile.compressed_flag else profile.accuracy_full
            if acc is None:
                continue
            if acc >= required:
                valid.append((exp_id, profile))

        if not valid:
            self.observability.counter('routing_fallbacks_total')
            return self.base_router.route(query)

        if use_mopd_flag:
            best_id, best_profile = self._mopd_select(valid, objective_weights)
        else:
            best_id, best_profile = self._fitness_select(valid)

        if best_id is None or best_profile is None:
            self.observability.counter('routing_fallbacks_total')
            return self.base_router.route(query)

        self.observability.counter(
            'routing_decisions_total',
            mode='mopd' if use_mopd_flag else 'fitness',
        )
        try:
            if best_profile.compressed_flag:
                return self.base_router.load_compressed_model(best_id)
            return self.base_router.load_full_model(best_id)
        except Exception as e:
            logger.error("Model load failed for %s: %s", best_id, e)
            return self.base_router.route(query)

    def _mopd_select(self, valid: Sequence[Tuple[str, Any]],
                     weights_override: Optional[Dict[str, float]]
                     ) -> Tuple[Optional[str], Any]:
        weights = dict(
            weights_override if weights_override is not None
            else self.config.mopd.objective_weights
        )
        if not weights:
            weights = {name: 1.0 / len(MOPD_OBJECTIVE_NAMES)
                       for name in MOPD_OBJECTIVE_NAMES}
        best_id: Optional[str] = None
        best_profile: Any = None
        best_score = -float('inf')
        for exp_id, profile in valid:
            pareto = list(getattr(profile, 'pareto_front', []) or [])
            if not pareto:
                self.scorer.compute(profile)
                score = float(getattr(profile, 'sustainability_fitness_score', 0.0))
            else:
                chosen = scalarise_pareto(pareto, weights)
                score = float(getattr(chosen, 'scalarised_score', 0.0)) if chosen else 0.0
            if score > best_score:
                best_score = score
                best_id = exp_id
                best_profile = profile
        return best_id, best_profile

    def _fitness_select(self, valid: Sequence[Tuple[str, Any]]
                        ) -> Tuple[Optional[str], Any]:
        best_id: Optional[str] = None
        best_profile: Any = None
        best_score = -float('inf')
        for exp_id, profile in valid:
            self.scorer.compute(profile)
            score = float(getattr(profile, 'sustainability_fitness_score', 0.0))
            if score > best_score:
                best_score = score
                best_id = exp_id
                best_profile = profile
        return best_id, best_profile

    async def get_prometheus_metrics(self) -> str:
        return await self.observability.export_prometheus()


# ============================================================================
# Exports
# ============================================================================
__all__ = [
    # Config
    "MOPDConfig",
    "FederatedConfig",
    "ApprovalConfig",
    "ChaosConfig",
    "SustainabilityConfig",
    "SUSTAINABILITY_CONFIG",
    # Data
    "MOPDPoint",
    "SustainabilityAwareExpertProfile",
    # Core
    "SustainabilityCompressor",
    "SustainabilityFitnessScorer",
    "MLOpsPipelineExtension",
    "SustainabilityAwareRouter",
    # Persistence
    "CompressionHistoryManager",
    "CompressedModelStorage",
    # Sub-services
    "RegistrationService",
    "RecompressionService",
    "AnomalyService",
    "RollbackService",
    # Infra
    "ObservabilityHub",
    "GovernanceLayer",
    "ApprovalRequest",
    "ApprovalResult",
    "ApprovalStatus",
    "ChaosService",
    "FaultType",
    "SafetyMonitor",
    "SafetyInvariant",
    "IdempotencyCache",
    # Stubs
    "TelemetryCollectorStub",
    "CarbonIntensityManagerStub",
    "AnomalyDetectorStub",
    # Protocols
    "TelemetryProtocol",
    "CarbonManagerProtocol",
    "AnomalyDetectorProtocol",
    "MessageQueueProtocol",
    # Utilities
    "scalarise_pareto",
    "filter_pareto",
    # Constants
    "MODULE_VERSION",
    "STATE_SCHEMA_VERSION",
    "MOPD_OBJECTIVE_NAMES",
    "MOPD_HIGHER_IS_BETTER",
    # Pydantic flags
    "PYDANTIC_AVAILABLE",
    "PYDANTIC_V1",
    "PYDANTIC_V2",
]

SUSTAINABILITY_CONFIG = SustainabilityConfig()


# ============================================================================
# Example usage
# ============================================================================
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    if not TORCH_AVAILABLE:
        print("PyTorch not available; skipping example.")
    else:
        class _DummyModel(nn.Module):  # type: ignore
            def __init__(self) -> None:
                super().__init__()
                self.fc1 = nn.Linear(64, 128)
                self.fc2 = nn.Linear(128, 10)

            def forward(self, x: Any) -> Any:
                x = torch.relu(self.fc1(x))  # type: ignore
                return self.fc2(x)

        class _DummyLoader:
            def __init__(self) -> None:
                self._batches = [
                    (torch.randn(8, 64), torch.randint(0, 10, (8,)))  # type: ignore
                    for _ in range(5)
                ]

            def __iter__(self):
                return iter(self._batches)

        async def _demo() -> None:
            config = SustainabilityConfig(energy_threshold_joules=0.0)
            config.chaos.enabled = True
            config.enable_chaos_testing = True

            model = _DummyModel()
            loader = _DummyLoader()
            sample_input = next(iter(loader))[0]
            profile = SustainabilityAwareExpertProfile(expert_id="demo")

            compressor = SustainabilityCompressor(
                model=model,
                profile=profile,
                config=config,
                accuracy_fn=lambda m, l: 0.95,
                governance=GovernanceLayer(enabled=False),
                observability=ObservabilityHub(),
            )
            success = await compressor.evaluate_tradeoff_and_compress(
                loader, sample_input,
                idempotency_key=uuid.uuid4().hex,
            )
            print(f"Compression success: {success}")
            print(f"Method: {profile.compression_method}")
            print(f"Pareto front size: {len(profile.pareto_front)}")

            chaos = await compressor.run_chaos_test(loader, sample_input)
            print(f"Chaos restored: {chaos.get('restored')}")

            metrics = await compressor.get_prometheus_metrics()
            print(metrics[:300])

        asyncio.run(_demo())
