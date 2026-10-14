#!/usr/bin/env python3
# sustainability/pipeline.py
"""
Enhanced MLOps Pipeline Extension v8.0.0
========================================

Production-grade integration layer for sustainability-aware compression.

Highlights
----------
* Sub-service architecture: Registration, Recompression, Anomaly, Rollback,
  Routing, Chaos, and Governance are cleanly separated.
* Shared Pareto scalarisation helper — a single source of truth for MOPD
  decisions, eliminating drift between the compressor and the router.
* Real human-in-the-loop approval via `GovernanceLayer` (queue + audit).
* `ObservabilityHub` with Prometheus export.
* Non-destructive chaos testing with full save/restore.
* Full lifecycle: `start()` / `wait_ready()` / `shutdown()`.
* Idempotency keys with LRU cache.
* Canonical MOPD objective names matching `sustainability.config`.
* Bounded caches, consistent lock discipline, and rich health checks.

Backward compatibility
----------------------
`MLOpsPipelineExtension` and `SustainabilityAwareRouter` keep the same public
surface as v7.0.0 while delegating to the new sub-services internally.
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
from collections import OrderedDict, deque
from dataclasses import dataclass, field, fields as dataclass_fields
from datetime import datetime, timezone
from enum import Enum
from typing import (
    Any, Awaitable, Callable, Deque, Dict, FrozenSet, List, Optional,
    Protocol, Sequence, Tuple, runtime_checkable,
)

import numpy as np

try:
    import torch
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False
    torch = None  # type: ignore

logger = logging.getLogger(__name__)


# ============================================================================
# Best-effort imports from sibling modules (graceful fallback)
# ============================================================================
try:
    from .config import (  # type: ignore
        SUSTAINABILITY_CONFIG,
        SustainabilityConfig,
        STATE_SCHEMA_VERSION,
        MOPD_OBJECTIVE_NAMES as CONFIG_MOPD_OBJECTIVE_NAMES,
        MOPD_HIGHER_IS_BETTER as CONFIG_MOPD_HIGHER_IS_BETTER,
    )
    CONFIG_AVAILABLE = True
except ImportError:
    CONFIG_AVAILABLE = False
    SUSTAINABILITY_CONFIG = None  # type: ignore
    SustainabilityConfig = None  # type: ignore
    STATE_SCHEMA_VERSION = 7
    CONFIG_MOPD_OBJECTIVE_NAMES = (
        'accuracy', 'energy', 'carbon_savings_kg', 'material_index',
    )
    CONFIG_MOPD_HIGHER_IS_BETTER = frozenset({'accuracy', 'carbon_savings_kg'})

try:
    from .compressor import SustainabilityCompressor  # type: ignore
    COMPRESSOR_AVAILABLE = True
except ImportError:
    COMPRESSOR_AVAILABLE = False
    SustainabilityCompressor = None  # type: ignore

try:
    from .fitness_scorer import SustainabilityFitnessScorer  # type: ignore
    SCORER_AVAILABLE = True
except ImportError:
    SCORER_AVAILABLE = False

    class SustainabilityFitnessScorer:  # type: ignore
        def __init__(self, config: Any = None, telemetry: Any = None):
            self.config = config

        def compute(self, profile: Any) -> float:
            score = getattr(profile, 'sustainability_fitness_score', 0.0)
            return float(score)

try:
    from .history import CompressionHistoryManager  # type: ignore
    HISTORY_AVAILABLE = True
except ImportError:
    HISTORY_AVAILABLE = False

    class CompressionHistoryManager:  # type: ignore
        def __init__(self, db_path: str = ":memory:", retention_days: int = 365):
            self.db_path = db_path

        def get_latest(self, expert_id: str) -> Optional[Dict[str, Any]]:
            return None

        def get_history(self, expert_id: str, limit: int = 10) -> List[Dict[str, Any]]:
            return []

        def get_pareto_front(self, expert_id: str, limit: int = 50) -> List[Any]:
            return []

try:
    from .storage import CompressedModelStorage  # type: ignore
    STORAGE_AVAILABLE = True
except ImportError:
    STORAGE_AVAILABLE = False

    class CompressedModelStorage:  # type: ignore
        def __init__(self, storage_dir: str = "./compressed_models"):
            self.storage_dir = storage_dir

        def load(self, expert_id: str, method: str, model: Any) -> bool:
            return False

        def save(self, expert_id: str, method: str, model: Any, profile: Any) -> None:
            pass

try:
    from .profiles import (  # type: ignore
        SustainabilityAwareExpertProfile,
        MOPDPoint,
    )
    PROFILES_AVAILABLE = True
except ImportError:
    PROFILES_AVAILABLE = False

    @dataclass
    class MOPDPoint:  # type: ignore
        method: str
        accuracy: float
        energy: float
        carbon_savings_kg: float
        material_index: float
        scalarised_score: float = 0.0
        explanation: str = ""

        def objective_vector(self) -> List[float]:
            vec: List[float] = []
            for name in CONFIG_MOPD_OBJECTIVE_NAMES:
                val = float(getattr(self, name, 0.0))
                if name in CONFIG_MOPD_HIGHER_IS_BETTER:
                    val = -val
                vec.append(val)
            return vec

    @dataclass
    class SustainabilityAwareExpertProfile:  # type: ignore
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
        pareto_front: List[MOPDPoint] = field(default_factory=list)

        def update_material_index(self, hardware_profile: str) -> None:
            mapping = {'default': 0.5, 'gpu': 0.4, 'cpu': 0.3, 'tpu': 0.2}
            self.material_index = mapping.get(hardware_profile, 0.5)


# ============================================================================
# Named Constants
# ============================================================================
DEFAULT_ENERGY_THRESHOLD = 5.0
DEFAULT_ACCURACY_DROP_TOLERANCE = 0.02
DEFAULT_ROLLBACK_THRESHOLD = 0.95
MAX_APPROVAL_AUDIT = 1000
MAX_DEAD_LETTERS = 1000
MAX_PARETO_CACHE = 512
MAX_EXPLANATION_CACHE = 512
IDEMPOTENCY_CACHE_SIZE = 4096
CHAOS_MAX_FAULTS_PER_RUN = 6
APPROVAL_DEFAULT_TIMEOUT = 60.0


# ============================================================================
# Protocols
# ============================================================================
@runtime_checkable
class BaseRouterProtocol(Protocol):
    def get_all_experts(self, query: Any) -> List[Tuple[str, Any]]: ...
    def route(self, query: Any) -> Any: ...
    def load_compressed_model(self, expert_id: str) -> Any: ...
    def load_full_model(self, expert_id: str) -> Any: ...


@runtime_checkable
class TelemetryProtocol(Protocol):
    async def increment(self, metric: str, value: float = 1.0,
                        tags: Optional[Dict[str, str]] = None) -> None: ...
    async def gauge(self, metric: str, value: float,
                    tags: Optional[Dict[str, str]] = None) -> None: ...
    async def histogram(self, metric: str, value: float,
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
    async def publish(self, topic: str, payload: str) -> None: ...


@runtime_checkable
class EventBrokerProtocol(Protocol):
    def subscribe(self, event_type: str,
                  handler: Callable[[Any], Awaitable[None]]) -> None: ...
    def publish(self, topic: str, payload: Any) -> Any: ...


# ============================================================================
# Enums
# ============================================================================
class FaultType(str, Enum):
    HIGH_ENERGY = "high_energy"
    LOW_ACCURACY = "low_accuracy"
    TELEMETRY_FAILURE = "telemetry_failure"
    DB_WRITE_FAILURE = "db_write_failure"
    STORAGE_CORRUPTION = "storage_corruption"
    COMPRESSOR_FAILURE = "compressor_failure"


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
    """Shared Prometheus-ready metrics hub."""

    def __init__(self) -> None:
        self._counters: Dict[str, float] = {}
        self._gauges: Dict[str, float] = {}
        self._histograms: Dict[str, Deque[float]] = {}
        self._lock = asyncio.Lock()

    def _key(self, name: str, labels: Optional[Dict[str, str]]) -> str:
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
            self._histograms[key] = deque(maxlen=1000)
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
            'context': _safe_dict(self.context),
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


def _safe_dict(obj: Any) -> Any:
    """Recursively JSON-safe conversion."""
    if obj is None or isinstance(obj, (bool, int, float, str)):
        return obj
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {str(k): _safe_dict(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set, frozenset)):
        return [_safe_dict(v) for v in obj]
    if isinstance(obj, deque):
        return [_safe_dict(v) for v in obj]
    if hasattr(obj, 'to_dict') and callable(obj.to_dict):
        return _safe_dict(obj.to_dict())
    if hasattr(obj, '__dict__'):
        return _safe_dict(obj.__dict__)
    return str(obj)


class GovernanceLayer:
    """Human-in-the-loop approval with queue topic + audit + timeout."""

    def __init__(
        self,
        config: Any,
        publish_fn: Optional[Callable[[str, Any], Awaitable[None]]] = None,
    ) -> None:
        self.config = config
        self.publish_fn = publish_fn
        self.pending: Dict[str, ApprovalRequest] = {}
        self.audit: Deque[Dict[str, Any]] = deque(maxlen=MAX_APPROVAL_AUDIT)
        self._lock = asyncio.Lock()

    @property
    def enabled(self) -> bool:
        return bool(getattr(self.config, 'require_human_approval', False))

    @property
    def timeout_seconds(self) -> float:
        return float(getattr(self.config, 'approval_timeout_seconds',
                             APPROVAL_DEFAULT_TIMEOUT))

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

        timeout = timeout if timeout is not None else self.timeout_seconds
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
                await self.publish_fn(
                    'approval_requests',
                    json.dumps(req.to_dict(), default=str),
                )
            decision = await asyncio.wait_for(future, timeout=timeout)
            result = ApprovalResult(
                request_id=request_id,
                status=ApprovalStatus.APPROVED if decision else ApprovalStatus.DENIED,
                reason='human_decision',
                decided_at=datetime.now(timezone.utc),
            )
        except asyncio.TimeoutError:
            auto_approve = bool(getattr(self.config, 'approval_auto_approve_on_timeout', False))
            result = ApprovalResult(
                request_id=request_id,
                status=(ApprovalStatus.AUTO_APPROVED if auto_approve
                        else ApprovalStatus.TIMEOUT),
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
            'context': _safe_dict(context),
            'result': result.to_dict(),
        })

    def get_audit_log(self) -> List[Dict[str, Any]]:
        return list(self.audit)


# ============================================================================
# Shared Pareto scalarisation — single source of truth
# ============================================================================
def scalarise_pareto(
    points: Sequence[Any],
    weights: Dict[str, float],
    objective_names: Optional[Sequence[str]] = None,
    higher_is_better: Optional[FrozenSet[str]] = None,
) -> Optional[Any]:
    """
    Scalarise Pareto points using min-max normalisation.

    Uses the canonical lower-is-better objective vector, so this function
    is consistent with the compressor's internal scalariser.
    """
    if not points:
        return None
    names = list(objective_names or CONFIG_MOPD_OBJECTIVE_NAMES)
    hib = set(higher_is_better or CONFIG_MOPD_HIGHER_IS_BETTER)

    vectors: List[List[float]] = []
    for p in points:
        vec = _objective_vector(p, names, hib)
        vectors.append(vec)

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
            w = float(weights.get(name, 1.0 / len(names)))
            score += w * float(normalised[i])
        try:
            setattr(p, 'scalarised_score', score)
        except Exception:
            pass
        if score > best_score:
            best_score = score
            best = p
    return best


def _objective_vector(
    point: Any,
    names: Sequence[str],
    higher_is_better: set,
) -> List[float]:
    """Canonical lower-is-better vector for a plan-like object."""
    # Prefer the object's own method if available
    method = getattr(point, 'objective_vector', None)
    if callable(method):
        try:
            vec = method()
            if vec is not None and len(vec) == len(names):
                return [float(v) for v in vec]
        except Exception:
            pass

    vec: List[float] = []
    for name in names:
        val = float(getattr(point, name, 0.0))
        if name in higher_is_better:
            val = -val
        vec.append(val)
    return vec


# ============================================================================
# Temporal safety
# ============================================================================
class SafetyInvariant:
    def __init__(self, name: str, predicate: Callable[[Dict[str, Any]], bool],
                 description: str, kind: str = "always") -> None:
        self.name = name
        self.predicate = predicate
        self.description = description
        self.kind = kind


class SafetyMonitor:
    def __init__(self) -> None:
        self.invariants: List[SafetyInvariant] = []

    def add(self, name: str, predicate: Callable[[Dict[str, Any]], bool],
            description: str, kind: str = "always") -> None:
        self.invariants.append(SafetyInvariant(name, predicate, description, kind))

    def check(self, state: Dict[str, Any]) -> List[str]:
        violations: List[str] = []
        for inv in self.invariants:
            try:
                ok = bool(inv.predicate(state))
            except Exception as e:
                violations.append(f"{inv.name}: predicate error ({e})")
                continue
            if not ok:
                violations.append(f"{inv.name}: {inv.description}")
        return violations


# ============================================================================
# ChaosService (non-destructive)
# ============================================================================
class ChaosService:
    """
    Save/restore-based chaos injection. Every injected fault is reversible
    via `restore()`, and `run_chaos_test` guarantees restoration.
    """

    def __init__(self, extension: "MLOpsPipelineExtension", enabled: bool) -> None:
        self.extension = extension
        self.enabled = enabled
        self._saved: Dict[str, Any] = {}
        self._injected: List[str] = []
        self._lock = asyncio.Lock()

    async def inject(self, fault_type: FaultType, **params: Any) -> Dict[str, Any]:
        if not self.enabled:
            return {'status': 'disabled'}

        async with self._lock:
            try:
                if fault_type == FaultType.HIGH_ENERGY:
                    expert_id = params.get('expert_id')
                    if expert_id:
                        profile = self.extension._get_profile(expert_id)
                        if profile is not None:
                            self._saved[f'energy:{expert_id}'] = getattr(
                                profile, 'energy_per_inference_full', 0.0,
                            )
                            profile.energy_per_inference_full = 1e6
                            self._injected.append(fault_type.value)
                            return {'status': 'injected', 'expert_id': expert_id}
                    return {'status': 'skipped', 'reason': 'no_expert'}

                if fault_type == FaultType.LOW_ACCURACY:
                    self._saved['scorer'] = self.extension.scorer
                    self.extension.scorer = _LowAccuracyScorer()
                    self._injected.append(fault_type.value)
                    return {'status': 'injected'}

                if fault_type == FaultType.TELEMETRY_FAILURE:
                    self._saved['telemetry'] = self.extension.telemetry
                    self.extension.telemetry = None
                    self._injected.append(fault_type.value)
                    return {'status': 'injected'}

                if fault_type == FaultType.DB_WRITE_FAILURE:
                    self._saved['history_manager'] = self.extension.history_manager
                    self.extension.history_manager = None
                    self._injected.append(fault_type.value)
                    return {'status': 'injected'}

                if fault_type == FaultType.STORAGE_CORRUPTION:
                    self._saved['storage'] = self.extension.storage
                    self.extension.storage = None
                    self._injected.append(fault_type.value)
                    return {'status': 'injected'}

                if fault_type == FaultType.COMPRESSOR_FAILURE:
                    self._saved['compressor_disabled'] = True
                    self.extension._compressor_disabled = True
                    self._injected.append(fault_type.value)
                    return {'status': 'injected'}
            except Exception as e:
                logger.exception("Chaos injection failed: %s", e)
                return {'status': 'error', 'error': str(e)}
            return {'status': 'unknown_fault'}

    def restore(self) -> None:
        ext = self.extension
        for key, value in list(self._saved.items()):
            if key.startswith('energy:'):
                expert_id = key.split(':', 1)[1]
                profile = ext._get_profile(expert_id)
                if profile is not None:
                    profile.energy_per_inference_full = value
            elif key == 'scorer':
                ext.scorer = value
            elif key == 'telemetry':
                ext.telemetry = value
            elif key == 'history_manager':
                ext.history_manager = value
            elif key == 'storage':
                ext.storage = value
            elif key == 'compressor_disabled':
                ext._compressor_disabled = False
            self._saved.pop(key, None)
        self._injected.clear()

    async def run_chaos_test(
        self,
        expert_id: Optional[str] = None,
        val_loader: Any = None,
        sample_input: Any = None,
    ) -> Dict[str, Any]:
        if not self.enabled:
            return {'status': 'disabled'}

        report: Dict[str, Any] = {
            'faults': [],
            'results': {},
            'restored': False,
        }
        faults = [
            FaultType.HIGH_ENERGY,
            FaultType.LOW_ACCURACY,
            FaultType.TELEMETRY_FAILURE,
            FaultType.DB_WRITE_FAILURE,
            FaultType.STORAGE_CORRUPTION,
            FaultType.COMPRESSOR_FAILURE,
        ]
        try:
            for ft in faults[:CHAOS_MAX_FAULTS_PER_RUN]:
                res = await self.inject(ft, expert_id=expert_id)
                report['faults'].append(ft.value)
                report['results'][ft.value] = res

            # Steady-state hypothesis: health_check must not crash
            try:
                health = await self.extension.health_check()
                report['results']['health_check'] = {
                    'status': health.get('status', 'unknown'),
                }
            except Exception as e:
                report['results']['health_check'] = {'error': str(e)}
        finally:
            self.restore()
            report['restored'] = True
        return report


class _LowAccuracyScorer:
    def compute(self, profile: Any) -> float:
        try:
            profile.sustainability_fitness_score = 0.0
        except Exception:
            pass
        return 0.0


# ============================================================================
# Idempotency cache (LRU)
# ============================================================================
class _IdempotencyCache:
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


# ============================================================================
# Sub-services
# ============================================================================
class RegistrationService:
    """Handles expert registration and initial compression decisions."""

    def __init__(self, extension: "MLOpsPipelineExtension") -> None:
        self.ext = extension

    async def register(
        self,
        expert_id: str,
        model: Any,
        profile: Any,
        val_loader: Any,
        idempotency_key: Optional[str] = None,
    ) -> bool:
        ext = self.ext
        if ext._idempotency.seen(idempotency_key):
            logger.info("Duplicate registration for %s (idempotent)", expert_id)
            return False
        if expert_id in ext.pipeline.model_registry:
            logger.info("Expert %s already registered; skipping", expert_id)
            return False

        ext._ensure_pipeline()
        hw = getattr(ext.config, 'hardware_profile', 'default')
        try:
            profile.update_material_index(hw)
        except TypeError:
            # Older profile signature: pass hardware profile dict
            profile.update_material_index(getattr(ext.config, 'hardware_profiles', {}))

        # Snapshot original model for rollback
        ext._full_models[expert_id] = _deepcopy_model(model)

        # Attempt disk restore
        if ext.storage is not None and ext.history_manager is not None:
            restored = await self._try_restore(expert_id, model, profile)
            if restored:
                return True

        # Trigger compression if needed
        threshold = float(getattr(ext.config, 'energy_threshold_joules', DEFAULT_ENERGY_THRESHOLD))
        if profile.energy_per_inference_full > threshold:
            await self._compress_initial(expert_id, model, profile, val_loader)
        else:
            logger.info(
                "Expert %s energy (%.4f J) within threshold; no compression.",
                expert_id, profile.energy_per_inference_full,
            )
            ext.pipeline.model_registry[expert_id] = model
            ext.pipeline.profile_registry[expert_id] = profile
        return True

    async def _try_restore(self, expert_id: str, model: Any, profile: Any) -> bool:
        ext = self.ext
        try:
            latest = ext.history_manager.get_latest(expert_id)
        except Exception:
            latest = None
        if not latest:
            return False
        method = latest.get('method')
        if not method:
            return False
        try:
            loaded = ext.storage.load(expert_id, method, model)
        except Exception as e:
            logger.warning("Failed to load compressed model for %s: %s", expert_id, e)
            return False
        if not loaded:
            return False

        async with ext._lock:
            profile.compressed_flag = True
            profile.compression_method = method
            profile.accuracy_compressed = latest.get('accuracy_after')
            profile.energy_per_inference_compressed = latest.get('energy_after')
            profile.carbon_savings_kg = latest.get('carbon_savings_kg', 0.0)
            ts = latest.get('timestamp')
            if isinstance(ts, str):
                try:
                    profile.last_compressed_at = datetime.fromisoformat(ts)
                except ValueError:
                    profile.last_compressed_at = None
            ext.pipeline.model_registry[expert_id] = model
            ext.pipeline.profile_registry[expert_id] = profile
            if profile.accuracy_compressed is not None:
                ext._compressed_acc_cache[expert_id] = float(profile.accuracy_compressed)

            try:
                pareto = ext.history_manager.get_pareto_front(expert_id)
            except Exception:
                pareto = []
            if pareto:
                profile.pareto_front = list(pareto)
                self._cache_pareto(expert_id, pareto)

        logger.info("Restored compressed model for %s (method=%s)", expert_id, method)
        return True

    async def _compress_initial(
        self, expert_id: str, model: Any, profile: Any, val_loader: Any,
    ) -> None:
        ext = self.ext
        if ext._compressor_disabled:
            logger.warning("Compressor disabled; skipping %s", expert_id)
            ext.pipeline.model_registry[expert_id] = model
            ext.pipeline.profile_registry[expert_id] = profile
            return
        if not COMPRESSOR_AVAILABLE or SustainabilityCompressor is None:
            logger.warning("Compressor unavailable; skipping %s", expert_id)
            ext.pipeline.model_registry[expert_id] = model
            ext.pipeline.profile_registry[expert_id] = profile
            return

        sample_input = ext._sample_input_for(val_loader)
        if sample_input is None:
            logger.warning("No sample input for %s; skipping compression.", expert_id)
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
                self._cache_explanation(expert_id, profile)
                logger.info("Compressed expert %s", expert_id)
            else:
                ext.pipeline.model_registry[expert_id] = model
                ext.pipeline.profile_registry[expert_id] = profile
                logger.info("Expert %s left uncompressed", expert_id)

    def _cache_pareto(self, expert_id: str, pareto: Sequence[Any]) -> None:
        ext = self.ext
        if not pareto:
            return
        if len(ext._pareto_fronts) >= MAX_PARETO_CACHE:
            ext._pareto_fronts.pop(next(iter(ext._pareto_fronts)))
        ext._pareto_fronts[expert_id] = list(pareto)
        ext.observability.histogram('pareto_front_size', len(pareto))

    def _cache_explanation(self, expert_id: str, profile: Any) -> None:
        ext = self.ext
        explanation = _choose_explanation(profile)
        if explanation:
            if len(ext._explanations) >= MAX_EXPLANATION_CACHE:
                ext._explanations.pop(next(iter(ext._explanations)))
            ext._explanations[expert_id] = explanation


class RecompressionService:
    """Periodic re-compression of uncompressed experts."""

    def __init__(self, extension: "MLOpsPipelineExtension") -> None:
        self.ext = extension

    async def run_once(self) -> Dict[str, int]:
        ext = self.ext
        ext._ensure_pipeline()
        stats = {'attempted': 0, 'compressed': 0, 'skipped': 0, 'failed': 0}
        for expert_id, model in list(ext.pipeline.model_registry.items()):
            profile = ext.pipeline.profile_registry.get(expert_id)
            if profile is None:
                stats['skipped'] += 1
                continue
            if profile.compressed_flag:
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

            # Safety invariants
            if getattr(ext.config, 'enable_temporal_safety', True):
                violations = ext.safety_monitor.check({
                    'energy_full': profile.energy_per_inference_full,
                    'energy_threshold': getattr(ext.config, 'energy_threshold_joules',
                                                DEFAULT_ENERGY_THRESHOLD),
                })
                if violations:
                    logger.warning("Safety violations for %s: %s", expert_id, violations)
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
                    ext.registration_service._cache_pareto(expert_id, profile.pareto_front)
                    ext.registration_service._cache_explanation(expert_id, profile)
                stats['compressed'] += 1
            else:
                stats['skipped'] += 1
        return stats


class AnomalyService:
    """Handles anomaly-triggered compression for all matching experts."""

    def __init__(self, extension: "MLOpsPipelineExtension") -> None:
        self.ext = extension

    async def on_anomaly(self, node_id: str, metrics: Dict[str, Any]) -> Dict[str, int]:
        ext = self.ext
        if not getattr(ext.config, 'enable_anomaly_trigger', True):
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

            # Human approval gate for anomaly-triggered compression
            approval = await ext.governance.request_approval({
                'action': 'anomaly_triggered_compression',
                'expert_id': expert_id,
                'node_id': node_id,
                'metrics': metrics,
            })
            if approval.status not in (ApprovalStatus.APPROVED, ApprovalStatus.AUTO_APPROVED):
                logger.info(
                    "Anomaly compression denied for %s (status=%s)",
                    expert_id, approval.status.value,
                )
                continue

            compressor = ext._make_compressor(model, profile)
            try:
                success = await compressor.evaluate_tradeoff_and_compress(
                    val_loader, sample_input,
                )
            except Exception as e:
                logger.exception("Anomaly compression failed for %s: %s", expert_id, e)
                stats['failed'] += 1
                continue

            if success:
                async with ext._lock:
                    ext.pipeline.model_registry[expert_id] = compressor.model
                    ext.pipeline.profile_registry[expert_id] = profile
                    if profile.accuracy_compressed is not None:
                        ext._compressed_acc_cache[expert_id] = float(profile.accuracy_compressed)
                    ext.registration_service._cache_pareto(expert_id, profile.pareto_front)
                    ext.registration_service._cache_explanation(expert_id, profile)
                stats['compressed'] += 1
        return stats


class RollbackService:
    """Monitors accuracy and rolls back to the full model if degraded."""

    def __init__(self, extension: "MLOpsPipelineExtension") -> None:
        self.ext = extension

    async def monitor(self, expert_id: str, current_accuracy: float) -> bool:
        ext = self.ext
        compressed_acc = ext._compressed_acc_cache.get(expert_id)
        if compressed_acc is None or compressed_acc <= 0:
            return False

        threshold = float(getattr(ext.config, 'rollback_accuracy_threshold',
                                  DEFAULT_ROLLBACK_THRESHOLD))
        if current_accuracy >= compressed_acc * threshold:
            return False

        logger.warning(
            "Expert %s accuracy %.4f below rollback threshold %.4f",
            expert_id, current_accuracy, compressed_acc * threshold,
        )
        return await self._rollback(expert_id)

    async def _rollback(self, expert_id: str) -> bool:
        ext = self.ext
        full_model = ext._full_models.get(expert_id)
        if full_model is None:
            logger.error("No full-model snapshot for %s; cannot rollback", expert_id)
            return False

        profile = ext._get_profile(expert_id)
        if profile is None:
            return False
        if not profile.compressed_flag:
            return False

        async with ext._lock:
            ext.pipeline.model_registry[expert_id] = full_model
            profile.compressed_flag = False
            profile.accuracy_compressed = None
            profile.energy_per_inference_compressed = None
            profile.compression_method = None
            ext._compressed_acc_cache.pop(expert_id, None)
            ext._pareto_fronts.pop(expert_id, None)
            ext._explanations.pop(expert_id, None)
            ext.observability.counter('rollbacks_total', expert=expert_id)
        logger.info("Rolled back expert %s to full model", expert_id)
        return True


class RoutingService:
    """MOPD-aware routing with unified Pareto scalarisation."""

    def __init__(
        self,
        base_router: BaseRouterProtocol,
        scorer: Any,
        config: Any,
        default_required_accuracy: float = 0.90,
        observability: Optional[ObservabilityHub] = None,
    ) -> None:
        self.base_router = base_router
        self.scorer = scorer
        self.config = config
        self.default_required_accuracy = float(default_required_accuracy)
        self.observability = observability or ObservabilityHub()

    def route(
        self,
        query: Any,
        required_accuracy: Optional[float] = None,
        use_mopd: Optional[bool] = None,
        objective_weights: Optional[Dict[str, float]] = None,
    ) -> Any:
        required = (required_accuracy if required_accuracy is not None
                    else self.default_required_accuracy)
        mopd_cfg = getattr(self.config, 'mopd', None)
        mopd_enabled = bool(getattr(mopd_cfg, 'enabled', False))
        use_mopd_flag = mopd_enabled if use_mopd is None else bool(use_mopd)

        try:
            candidates = self.base_router.get_all_experts(query)
        except Exception as e:
            logger.error("Base router.get_all_experts failed: %s", e)
            return self.base_router.route(query)

        valid: List[Tuple[str, Any]] = []
        for exp_id, profile in candidates:
            acc = (profile.accuracy_compressed if profile.compressed_flag
                   else profile.accuracy_full)
            if acc is None:
                continue
            if acc >= required:
                valid.append((exp_id, profile))

        if not valid:
            self.observability.counter('routing_fallbacks_total')
            return self.base_router.route(query)

        if use_mopd_flag:
            best_id, best_profile, explanation = self._mopd_select(
                valid, objective_weights,
            )
        else:
            best_id, best_profile, explanation = self._fitness_select(valid)

        if best_profile is None or best_id is None:
            self.observability.counter('routing_fallbacks_total')
            return self.base_router.route(query)

        self.observability.counter('routing_decisions_total',
                                   mode='mopd' if use_mopd_flag else 'fitness')
        logger.info("Routing decision: %s", explanation)

        try:
            if best_profile.compressed_flag:
                return self.base_router.load_compressed_model(best_id)
            return self.base_router.load_full_model(best_id)
        except Exception as e:
            logger.exception("Model load failed for %s: %s", best_id, e)
            return self.base_router.route(query)

    def _mopd_select(
        self,
        valid: Sequence[Tuple[str, Any]],
        objective_weights: Optional[Dict[str, float]],
    ) -> Tuple[Optional[str], Any, str]:
        mopd_cfg = getattr(self.config, 'mopd', None)
        weights = dict(objective_weights
                       if objective_weights is not None
                       else getattr(mopd_cfg, 'objective_weights', {}))
        if not weights:
            weights = {name: 1.0 / len(CONFIG_MOPD_OBJECTIVE_NAMES)
                       for name in CONFIG_MOPD_OBJECTIVE_NAMES}

        best_id: Optional[str] = None
        best_profile: Any = None
        best_score = -float('inf')
        parts: List[str] = []
        for exp_id, profile in valid:
            pareto = getattr(profile, 'pareto_front', None) or []
            if not pareto:
                try:
                    self.scorer.compute(profile)
                except Exception:
                    pass
                score = float(getattr(profile, 'sustainability_fitness_score', 0.0))
                parts.append(f"{exp_id}=scalar({score:.4f})")
            else:
                chosen = scalarise_pareto(pareto, weights)
                score = float(getattr(chosen, 'scalarised_score', 0.0)) if chosen else 0.0
                parts.append(f"{exp_id}=pareto({score:.4f})")
            if score > best_score:
                best_score = score
                best_id = exp_id
                best_profile = profile

        explanation = (
            f"MOPD selected {best_id} with score {best_score:.4f}; "
            f"candidates: [{'; '.join(parts)}]"
        )
        return best_id, best_profile, explanation

    def _fitness_select(
        self,
        valid: Sequence[Tuple[str, Any]],
    ) -> Tuple[Optional[str], Any, str]:
        best_id: Optional[str] = None
        best_profile: Any = None
        best_score = -float('inf')
        parts: List[str] = []
        for exp_id, profile in valid:
            try:
                self.scorer.compute(profile)
            except Exception:
                pass
            score = float(getattr(profile, 'sustainability_fitness_score', 0.0))
            parts.append(f"{exp_id}=fitness({score:.4f})")
            if score > best_score:
                best_score = score
                best_id = exp_id
                best_profile = profile
        explanation = (
            f"Fitness selected {best_id} with score {best_score:.4f}; "
            f"candidates: [{'; '.join(parts)}]"
        )
        return best_id, best_profile, explanation


# ============================================================================
# Helpers
# ============================================================================
def _deepcopy_model(model: Any) -> Any:
    if not TORCH_AVAILABLE or model is None:
        return model
    import copy as _copy
    try:
        return _copy.deepcopy(model)
    except Exception as e:
        logger.warning("Model deepcopy failed: %s", e)
        return model


def _choose_explanation(profile: Any) -> Optional[str]:
    """Pick a representative explanation for XAI caching."""
    # Prefer the selected plan's explanation if the compressor stored it
    selected = getattr(profile, 'selected_explanation', None)
    if selected:
        return str(selected)
    pareto = getattr(profile, 'pareto_front', None) or []
    if not pareto:
        return None
    # Best scalarised point
    best_score = -float('inf')
    best_expl = None
    for p in pareto:
        s = float(getattr(p, 'scalarised_score', 0.0))
        e = getattr(p, 'explanation', None)
        if e and s >= best_score:
            best_score = s
            best_expl = e
    return best_expl


# ============================================================================
# Main extension
# ============================================================================
class MLOpsPipelineExtension:
    """
    Enhanced MLOps pipeline extension with sub-services, lifecycle,
    governance, observability, chaos testing, and idempotency.
    """

    def __init__(
        self,
        pipeline: Any,
        config: Optional[Any] = None,
        scorer: Optional[Any] = None,
        telemetry: Optional[TelemetryProtocol] = None,
        carbon_manager: Optional[CarbonManagerProtocol] = None,
        anomaly_detector: Optional[AnomalyDetectorProtocol] = None,
        accuracy_fn: Optional[Callable[[Any, Any], float]] = None,
        publish_fn: Optional[Callable[[str, Any], Awaitable[None]]] = None,
        observability: Optional[ObservabilityHub] = None,
    ) -> None:
        self.pipeline = pipeline
        self.config = config or SUSTAINABILITY_CONFIG
        if self.config is None:
            raise ValueError(
                "No config provided and SUSTAINABILITY_CONFIG unavailable. "
                "Pass `config=` explicitly."
            )

        # Externals
        self.telemetry = telemetry
        self.carbon_manager = carbon_manager
        self.anomaly_detector = anomaly_detector
        self.accuracy_fn = accuracy_fn
        self.scorer = scorer or SustainabilityFitnessScorer(
            self.config, telemetry=telemetry,
        )

        # Persistence
        self.history_manager: Any = None
        self.storage: Any = None
        if HISTORY_AVAILABLE:
            try:
                self.history_manager = CompressionHistoryManager(
                    db_path=getattr(self.config, 'history_db_path',
                                    "./compression_history.db"),
                    retention_days=int(getattr(
                        self.config, 'history_retention_days', 365,
                    )),
                )
            except Exception as e:
                logger.warning("History manager init failed: %s", e)
        if STORAGE_AVAILABLE:
            try:
                self.storage = CompressedModelStorage(
                    getattr(self.config, 'compressed_model_dir',
                            "./compressed_models"),
                )
            except Exception as e:
                logger.warning("Storage init failed: %s", e)

        # Observability
        self.observability = observability or ObservabilityHub()

        # Governance
        self.governance = GovernanceLayer(self.config, publish_fn=publish_fn)

        # Safety
        self.safety_monitor = SafetyMonitor()
        self._setup_invariants()

        # Chaos
        chaos_enabled = bool(getattr(self.config, 'enable_chaos_testing', False))
        self.chaos_service = ChaosService(self, enabled=chaos_enabled)

        # Caches (bounded)
        self._compressed_acc_cache: Dict[str, float] = {}
        self._pareto_fronts: Dict[str, List[Any]] = {}
        self._explanations: Dict[str, str] = {}
        self._full_models: Dict[str, Any] = {}
        self._idempotency = _IdempotencyCache()

        # Lifecycle state
        self._started = False
        self._shutting_down = False
        self._recompress_task: Optional[asyncio.Task] = None
        self._chaos_task: Optional[asyncio.Task] = None
        self._background_tasks: List[asyncio.Task] = []
        self._lock = asyncio.Lock()

        # Chaos flag
        self._compressor_disabled = False

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

        interval = int(getattr(self.config, 'recompress_interval_seconds', 0) or 0)
        enable_recompress = bool(getattr(self.config, 'enable_auto_recompress', True))
        if enable_recompress and interval > 0:
            self._recompress_task = asyncio.create_task(self._recompress_loop(interval))
            self._background_tasks.append(self._recompress_task)

        if bool(getattr(self.config, 'enable_chaos_testing', False)):
            chaos_interval = int(getattr(
                self.config, 'chaos_test_interval_seconds', 3600,
            ) or 3600)
            self._chaos_task = asyncio.create_task(self._chaos_loop(chaos_interval))
            self._background_tasks.append(self._chaos_task)

        logger.info("MLOpsPipelineExtension started")

    async def wait_ready(self) -> None:
        if not self._started:
            await self.start()

    async def shutdown(self) -> None:
        logger.info("Shutting down MLOpsPipelineExtension")
        self._shutting_down = True
        self._started = False

        for t in self._background_tasks:
            t.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)
        self._background_tasks.clear()
        self._recompress_task = None
        self._chaos_task = None

        if self.carbon_manager is not None:
            try:
                res = self.carbon_manager.close()
                if asyncio.iscoroutine(res):
                    await res
            except Exception as e:
                logger.warning("Carbon manager close failed: %s", e)

        logger.info("MLOpsPipelineExtension shutdown complete")

    async def _recompress_loop(self, interval: int) -> None:
        while not self._shutting_down:
            try:
                await asyncio.sleep(interval)
                await self.recompression_service.run_once()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Recompression loop error: %s", e)

    async def _chaos_loop(self, interval: int) -> None:
        while not self._shutting_down:
            try:
                await asyncio.sleep(interval)
                experts = list(self.pipeline.profile_registry.keys())
                expert_id = experts[0] if experts else None
                await self.chaos_service.run_chaos_test(expert_id=expert_id)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("Chaos loop error: %s", e)

    # ------------------------------------------------------------------
    # Public API — backward compatible
    # ------------------------------------------------------------------
    async def on_expert_registered(
        self,
        expert_id: str,
        model: Any,
        profile: Any,
        val_loader: Any,
        idempotency_key: Optional[str] = None,
    ) -> None:
        await self.wait_ready()
        key = idempotency_key or f"register:{expert_id}"
        await self.registration_service.register(
            expert_id, model, profile, val_loader, idempotency_key=key,
        )

    async def on_anomaly_detected(self, node_id: str, metrics: Dict[str, Any]) -> None:
        await self.wait_ready()
        await self.anomaly_service.on_anomaly(node_id, metrics)

    async def monitor_rollback(self, expert_id: str, current_accuracy: float) -> None:
        await self.rollback_service.monitor(expert_id, current_accuracy)

    async def start_recompress_loop(self) -> None:
        """Backward-compatible alias for the lifecycle start."""
        await self.start()

    async def stop_recompress_loop(self) -> None:
        """Backward-compatible alias for a soft stop of the recompression loop only."""
        if self._recompress_task is not None:
            self._recompress_task.cancel()
            try:
                await self._recompress_task
            except asyncio.CancelledError:
                pass
            self._recompress_task = None

    # ------------------------------------------------------------------
    # XAI / MOPD queries
    # ------------------------------------------------------------------
    async def get_pareto_front(self, expert_id: str) -> List[Any]:
        if not getattr(getattr(self.config, 'mopd', None), 'enabled', False):
            logger.debug("MOPD disabled; no Pareto front for %s", expert_id)
            return []
        if expert_id in self._pareto_fronts:
            return list(self._pareto_fronts[expert_id])
        if self.history_manager is not None:
            try:
                pareto = self.history_manager.get_pareto_front(expert_id)
            except Exception:
                pareto = []
            if pareto:
                self._pareto_fronts[expert_id] = list(pareto)
                return list(pareto)
        return []

    async def get_explanation(self, expert_id: str) -> Optional[str]:
        if expert_id in self._explanations:
            return self._explanations[expert_id]
        profile = self._get_profile(expert_id)
        if profile is not None:
            explanation = _choose_explanation(profile)
            if explanation:
                self._explanations[expert_id] = explanation
                return explanation
        pareto = await self.get_pareto_front(expert_id)
        if pareto:
            explanation = _choose_explanation(profile or _SimpleProfile(pareto))
            if explanation:
                self._explanations[expert_id] = explanation
                return explanation
        return None

    async def get_mopd_summary(self) -> Dict[str, Any]:
        mopd_cfg = getattr(self.config, 'mopd', None)
        if not getattr(mopd_cfg, 'enabled', False):
            return {"enabled": False}
        sizes = [len(f) for f in self._pareto_fronts.values()]
        return {
            "enabled": True,
            "objective_weights": dict(getattr(mopd_cfg, 'objective_weights', {})),
            "grid_resolution": int(getattr(mopd_cfg, 'grid_resolution', 0)),
            "total_experts_with_pareto_front": len(self._pareto_fronts),
            "average_pareto_size": float(np.mean(sizes)) if sizes else 0.0,
            "max_pareto_size": max(sizes) if sizes else 0,
            "explanations_cached": len(self._explanations),
        }

    # ------------------------------------------------------------------
    # Chaos API
    # ------------------------------------------------------------------
    async def inject_fault(self, fault_type: str, **params: Any) -> Dict[str, Any]:
        try:
            ft = FaultType(fault_type)
        except ValueError:
            logger.warning("Unknown fault type: %s", fault_type)
            return {'status': 'unknown_fault'}
        return await self.chaos_service.inject(ft, **params)

    async def run_chaos_test(
        self,
        expert_id: Optional[str] = None,
        val_loader: Any = None,
        sample_input: Any = None,
    ) -> Dict[str, Any]:
        return await self.chaos_service.run_chaos_test(
            expert_id=expert_id, val_loader=val_loader, sample_input=sample_input,
        )

    # ------------------------------------------------------------------
    # Observability / health
    # ------------------------------------------------------------------
    async def get_prometheus_metrics(self) -> str:
        return await self.observability.export_prometheus()

    async def health_check(self) -> Dict[str, Any]:
        self._ensure_pipeline()
        compressed = sum(
            1 for p in self.pipeline.profile_registry.values()
            if getattr(p, 'compressed_flag', False)
        )
        recompress_active = (
            self._recompress_task is not None and not self._recompress_task.done()
        )
        return {
            "status": "healthy",
            "started": self._started,
            "shutting_down": self._shutting_down,
            "recompress_task_active": recompress_active,
            "chaos_task_active": (
                self._chaos_task is not None and not self._chaos_task.done()
            ),
            "num_experts": len(self.pipeline.model_registry),
            "compressed_count": compressed,
            "config_version": str(getattr(self.config, 'version', 'unknown')),
            "state_schema_version": STATE_SCHEMA_VERSION,
            "mopd_enabled": bool(getattr(
                getattr(self.config, 'mopd', None), 'enabled', False,
            )),
            "pareto_fronts_stored": len(self._pareto_fronts),
            "xai_explanations_stored": len(self._explanations),
            "full_model_snapshots": len(self._full_models),
            "chaos_injected_count": len(self.chaos_service._injected),
            "governance_pending": len(self.governance.pending),
        }

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

    def _get_profile(self, expert_id: str) -> Optional[Any]:
        try:
            return self.pipeline.profile_registry.get(expert_id)
        except Exception:
            return None

    def _sample_input_for(self, val_loader: Any) -> Any:
        """Extract a single sample input from a loader (cached per loader)."""
        if val_loader is None:
            return None
        cache = getattr(self, '_sample_cache', None)
        if cache is None:
            cache = {}
            self._sample_cache = cache
        loader_id = id(val_loader)
        if loader_id in cache:
            return cache[loader_id]
        try:
            for batch in val_loader:
                if isinstance(batch, (tuple, list)) and len(batch) >= 1:
                    cache[loader_id] = batch[0]
                    return batch[0]
                cache[loader_id] = batch
                return batch
        except Exception as e:
            logger.warning("Could not extract sample input: %s", e)
        return None

    def _make_compressor(self, model: Any, profile: Any) -> Any:
        if not COMPRESSOR_AVAILABLE or SustainabilityCompressor is None:
            raise RuntimeError("SustainabilityCompressor unavailable")
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

    def _setup_invariants(self) -> None:
        def _positive_energy(state: Dict[str, Any]) -> bool:
            return float(state.get('energy_full', 0.0)) >= 0.0

        def _threshold_positive(state: Dict[str, Any]) -> bool:
            return float(state.get('energy_threshold', 0.0)) >= 0.0

        self.safety_monitor.add(
            "positive_energy", _positive_energy,
            "Full-model energy must be non-negative",
        )
        self.safety_monitor.add(
            "threshold_positive", _threshold_positive,
            "Energy threshold must be non-negative",
        )


# ============================================================================
# Helper profile wrapper (used only when a profile object is unavailable)
# ============================================================================
class _SimpleProfile:
    def __init__(self, pareto_front: Sequence[Any]):
        self.pareto_front = list(pareto_front)
        self.compressed_flag = True
        self.accuracy_full = 0.0
        self.accuracy_compressed = 0.0
        self.energy_per_inference_full = 0.0
        self.energy_per_inference_compressed = 0.0
        self.carbon_savings_kg = 0.0
        self.material_index = 0.5
        self.sustainability_fitness_score = 0.0


# ============================================================================
# SustainabilityAwareRouter (backward-compatible facade)
# ============================================================================
class SustainabilityAwareRouter:
    """
    Router facade. Delegates to `RoutingService` for MOPD-aware decisions.
    """

    def __init__(
        self,
        base_router: Any,
        scorer: Optional[Any] = None,
        default_required_accuracy: float = 0.90,
        config: Optional[Any] = None,
        observability: Optional[ObservabilityHub] = None,
    ) -> None:
        self.base_router = base_router
        self.config = config or SUSTAINABILITY_CONFIG
        if self.config is None:
            raise ValueError(
                "No config provided and SUSTAINABILITY_CONFIG unavailable."
            )
        self.scorer = scorer or SustainabilityFitnessScorer(self.config)
        self.observability = observability or ObservabilityHub()
        self.service = RoutingService(
            base_router=base_router,
            scorer=self.scorer,
            config=self.config,
            default_required_accuracy=default_required_accuracy,
            observability=self.observability,
        )

    def route(
        self,
        query: Any,
        required_accuracy: Optional[float] = None,
        use_mopd: Optional[bool] = None,
        objective_weights: Optional[Dict[str, float]] = None,
    ) -> Any:
        return self.service.route(
            query,
            required_accuracy=required_accuracy,
            use_mopd=use_mopd,
            objective_weights=objective_weights,
        )

    async def get_prometheus_metrics(self) -> str:
        return await self.observability.export_prometheus()


# ============================================================================
# Exports
# ============================================================================
__all__ = [
    # Main extension
    "MLOpsPipelineExtension",
    "SustainabilityAwareRouter",
    # Sub-services
    "RegistrationService",
    "RecompressionService",
    "AnomalyService",
    "RollbackService",
    "RoutingService",
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
    # Utilities
    "scalarise_pareto",
    # Constants
    "STATE_SCHEMA_VERSION",
    "CONFIG_MOPD_OBJECTIVE_NAMES",
    "CONFIG_MOPD_HIGHER_IS_BETTER",
]


# ============================================================================
# Example usage
# ============================================================================
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    class _DummyRouter:
        def __init__(self) -> None:
            self._profiles = {
                "e1": _SimpleProfile([]),
                "e2": _SimpleProfile([]),
            }
            for pid, p in self._profiles.items():
                p.accuracy_full = 0.95
                p.compressed_flag = False

        def get_all_experts(self, query: Any) -> List[Tuple[str, Any]]:
            return list(self._profiles.items())

        def route(self, query: Any) -> str:
            return "default_route"

        def load_compressed_model(self, expert_id: str) -> str:
            return f"compressed:{expert_id}"

        def load_full_model(self, expert_id: str) -> str:
            return f"full:{expert_id}"

    async def _demo() -> None:
        router = SustainabilityAwareRouter(base_router=_DummyRouter())
        result = router.route({"task": "demo"}, required_accuracy=0.9)
        print(f"Route result: {result}")
        metrics = await router.get_prometheus_metrics()
        print(metrics[:300])

    asyncio.run(_demo())
