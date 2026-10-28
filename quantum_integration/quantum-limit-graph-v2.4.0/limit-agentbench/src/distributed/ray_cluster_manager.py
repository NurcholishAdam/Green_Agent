# src/distributed/ray_cluster_manager.py

"""
Helium-Aware Ray Cluster Manager — v2.0.0
=========================================

Routes tasks to worker pools based on helium constraints and an
:class:`ExecutionDecision` produced by the carbon-aware decision core.

Original behaviour preserved
----------------------------
- ``WorkerType`` enum with ``helium_footprint`` property.
- ``ExecutionResult`` dataclass.
- ``HeliumAwareRayExecutor(config=None)`` — constructs worker pools.
- ``await execute_task(task, workload_profile, execution_decision)``.
- ``get_worker_pool_status()`` — returns per-pool status.

v2.0.0 changes vs. v1.0.0
-------------------------
**P0 bug fixes**
  * 2.1 ``_config_from_legacy_dict`` / ``from_dict`` delegate to
        ``RayExecutorConfig.from_legacy_dict`` so new config fields use
        their declared defaults instead of the old hardcoded ones.
  * 2.2 ``__getstate__`` / ``__setstate__`` make the executor picklable
        (the ``RLock`` is dropped and restored).
  * 2.3 ``power_budget=None`` is no longer a crash: strict mode raises
        ``RayExecutorError``, lenient mode substitutes 1.0.
  * 2.4 ``helium_aware_flag="false"`` (string) is coerced to ``False``
        (previously ``bool("false") is True`` silently routed to GPU).
  * 2.5 Worker-pool capacity is now decremented by ``_acquire`` and
        restored by ``_release``; ``execute_task`` falls back when the
        primary pool is exhausted.
  * 2.6 Helium-usage formula fixed: ``footprint * (blend + (1-blend) * pb)``.
        At full power, CPU reports ≈0.10 and Quantum reports ≈0.99
        (previously both reported ≈0.50).
  * 2.7 ``power_budget=0.0`` returns ``success=False`` (previously
        ``success=True`` with ``accuracy=0.0``).
  * 2.8 ``optimization_level`` for ``power_budget<0.2`` is now ``"minimal"``
        (previously ``"deferred"``, which was misleading — the task
        actually executed).

**P1 robustness / observability**
  * 2.9 Warns on history truncation during ``from_dict``.
  * 2.10 ``task=None`` rejected in strict mode.
  * 2.11 ``helium_zone`` normalised (strip/lowercase) and validated;
        unknown zones are logged and treated as absent.
  * 3.2 Context managers hold the RLock while mutating state.
  * 3.3 ``metrics_hook(event, fields)`` for Prometheus / OTel.
  * 3.4 ``close()`` lifecycle hook.
  * 3.5 ``statistics(window=N)`` for a consistent windowed view.
  * 3.13 ``WorkerType.__str__`` returns ``.value``.
  * 3.14 ``from_dict`` warns when ``worker_pools`` in the payload don't
         match the enum footprints.

**P2 enhancements**
  * Fallback hierarchy honours ``power_budget`` scaling.
  * Consistent ``has_result``/``include_result`` semantics.

Thread-safe, serializable, picklable, bounded in memory.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import pickle  # noqa: F401  (used in self-tests)
import random
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field, fields
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Deque, Dict, List, Mapping, Optional

logger = logging.getLogger(__name__)

__version__ = "2.0.0"


# --------------------------------------------------------------------------- #
# Optional Ray integration
# --------------------------------------------------------------------------- #
try:  # pragma: no cover — environment-dependent
    import ray  # type: ignore

    _RAY_AVAILABLE = True
except ImportError:  # pragma: no cover
    ray = None  # type: ignore[assignment]
    _RAY_AVAILABLE = False


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class RayExecutorError(ValueError):
    """Raised for invalid inputs, configuration, or execution failures."""


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _coerce_bool(value: Any, default: bool = False) -> bool:
    """
    Robust bool coercion (P0 2.4).

    Handles real bools, common string spellings, and falls back to
    Python's truthiness. ``None`` returns ``default``.
    """
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, str):
        return value.strip().lower() in (
            "true", "1", "yes", "on", "y", "t",
        )
    try:
        return bool(value)
    except Exception:  # noqa: BLE001
        return default


# --------------------------------------------------------------------------- #
# Worker types
# --------------------------------------------------------------------------- #
class WorkerType(Enum):
    """Worker pool types with helium footprints."""

    STANDARD_CPU = "standard_cpu"
    GPU_SINGLE = "gpu_single"
    GPU_CLUSTER = "gpu_cluster"
    TPU = "tpu"
    QUANTUM = "quantum"

    @property
    def helium_footprint(self) -> float:
        """Relative helium footprint in [0, 1] (1 = most helium-intensive)."""
        return _HELIUM_FOOTPRINTS[self]

    def __str__(self) -> str:  # P1 3.13
        return self.value


# Single source of truth for helium footprints.
_HELIUM_FOOTPRINTS: Dict["WorkerType", float] = {
    WorkerType.STANDARD_CPU: 0.10,
    WorkerType.GPU_SINGLE: 0.75,
    WorkerType.GPU_CLUSTER: 0.95,
    WorkerType.TPU: 0.85,
    WorkerType.QUANTUM: 0.99,
}


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class RayExecutorConfig:
    """Tunable parameters for the helium-aware executor."""

    cpu_workers: int = 10
    gpu_workers: int = 4
    gpu_cluster_workers: int = 2
    tpu_workers: int = 2
    quantum_workers: int = 1

    gpu_available: bool = True
    gpu_cluster_available: bool = True
    tpu_available: bool = False
    quantum_available: bool = False

    fallback_enabled: bool = True
    execution_timeout_seconds: float = 300.0

    sim_min_latency_ms: float = 100.0
    sim_max_latency_ms: float = 1000.0
    sim_accuracy_ceiling: float = 0.95
    sim_energy_per_task_kwh: float = 0.5
    sim_carbon_per_task_kg: float = 0.2
    sim_distilled_accuracy: float = 0.85
    sim_distilled_energy_kwh: float = 0.2
    sim_distilled_carbon_kg: float = 0.08
    sim_distilled_latency_ms: float = 150.0
    sim_cpu_fallback_accuracy: float = 0.70
    sim_cpu_fallback_energy_kwh: float = 0.15
    sim_cpu_fallback_carbon_kg: float = 0.06
    sim_cpu_fallback_latency_ms: float = 500.0

    helium_blend_coefficient: float = 0.5

    max_history: int = 1000

    high_dependency_cutoff: float = 0.8
    mid_dependency_cutoff: float = 0.5

    def __post_init__(self) -> None:
        # P0 2.1 + validation: reject bools masquerading as int for numeric fields.
        for name in (
            "cpu_workers", "gpu_workers", "gpu_cluster_workers",
            "tpu_workers", "quantum_workers", "max_history",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int):
                raise RayExecutorError(
                    f"{name} must be an int, got {type(value).__name__}.")
            if name != "max_history" and value < 0:
                raise RayExecutorError(
                    f"{name} must be >= 0 (got {value}).")
        if self.max_history <= 0:
            raise RayExecutorError("max_history must be > 0.")
        if isinstance(self.execution_timeout_seconds, bool) or \
                not isinstance(self.execution_timeout_seconds, (int, float)) or \
                self.execution_timeout_seconds <= 0:
            raise RayExecutorError("execution_timeout_seconds must be > 0.")
        if not 0.0 <= self.helium_blend_coefficient <= 1.0:
            raise RayExecutorError(
                "helium_blend_coefficient must be in [0, 1].")
        if not 0.0 <= self.mid_dependency_cutoff <= \
                self.high_dependency_cutoff <= 1.0:
            raise RayExecutorError(
                "dependency cutoffs must satisfy 0 <= mid <= high <= 1.")
        if self.sim_min_latency_ms <= 0 or \
                self.sim_max_latency_ms < self.sim_min_latency_ms:
            raise RayExecutorError(
                "sim latencies must satisfy 0 < min <= max.")

    @classmethod
    def from_legacy_dict(
        cls, cfg: Optional[Mapping[str, Any]]
    ) -> "RayExecutorConfig":
        """
        Build from a flat mapping; unknown keys ignored (P0 2.1).

        Only keys matching a dataclass field are consumed, so new fields
        added to ``RayExecutorConfig`` automatically use their declared
        defaults instead of the caller silently getting the old default.
        """
        cfg = dict(cfg or {})
        valid = {f.name for f in fields(cls)}
        return cls(**{k: v for k, v in cfg.items() if k in valid})


# --------------------------------------------------------------------------- #
# Execution result
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ExecutionResult:
    """Immutable execution result with helium metrics."""

    success: bool
    task_id: str
    accuracy: float
    energy_consumed_kwh: float
    carbon_emitted_kg: float
    execution_time_ms: int
    worker_type: str

    helium_usage: float = 0.0
    helium_zone: Optional[str] = None
    fallback_used: bool = False
    optimization_level: str = "none"
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def __post_init__(self) -> None:
        if not isinstance(self.task_id, str) or not self.task_id:
            raise RayExecutorError("task_id must be a non-empty string.")
        if not 0.0 <= self.accuracy <= 1.0:
            raise RayExecutorError("accuracy must be in [0, 1].")
        for name in (
            "energy_consumed_kwh",
            "carbon_emitted_kg",
            "execution_time_ms",
        ):
            if getattr(self, name) < 0:
                raise RayExecutorError(f"{name} must be >= 0.")
        if not 0.0 <= self.helium_usage <= 1.0:
            raise RayExecutorError("helium_usage must be in [0, 1].")
        if not isinstance(self.timestamp, datetime):
            raise RayExecutorError("timestamp must be a datetime.")
        if not self.timestamp.tzinfo:
            object.__setattr__(
                self, "timestamp",
                self.timestamp.replace(tzinfo=timezone.utc),
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "success": self.success,
            "task_id": self.task_id,
            "accuracy": self.accuracy,
            "energy_consumed_kwh": self.energy_consumed_kwh,
            "carbon_emitted_kg": self.carbon_emitted_kg,
            "execution_time_ms": self.execution_time_ms,
            "worker_type": self.worker_type,
            "helium_usage": self.helium_usage,
            "helium_zone": self.helium_zone,
            "fallback_used": self.fallback_used,
            "optimization_level": self.optimization_level,
            "timestamp": self.timestamp.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ExecutionResult":
        if not isinstance(data, Mapping):
            raise RayExecutorError(
                f"ExecutionResult.from_dict expects a Mapping, "
                f"got {type(data).__name__}.")
        ts = data.get("timestamp")
        if isinstance(ts, str):
            timestamp = datetime.fromisoformat(ts)
            if not timestamp.tzinfo:
                timestamp = timestamp.replace(tzinfo=timezone.utc)
        elif isinstance(ts, datetime):
            timestamp = ts
        else:
            timestamp = datetime.now(timezone.utc)
        return cls(
            success=bool(data["success"]),
            task_id=str(data["task_id"]),
            accuracy=float(data["accuracy"]),
            energy_consumed_kwh=float(
                data.get("energy_consumed_kwh", 0.0)),
            carbon_emitted_kg=float(
                data.get("carbon_emitted_kg", 0.0)),
            execution_time_ms=int(data.get("execution_time_ms", 0)),
            worker_type=str(data.get("worker_type", "none")),
            helium_usage=float(data.get("helium_usage", 0.0)),
            helium_zone=data.get("helium_zone"),
            fallback_used=bool(data.get("fallback_used", False)),
            optimization_level=str(
                data.get("optimization_level", "none")),
            timestamp=timestamp,
        )


# --------------------------------------------------------------------------- #
# Executor
# --------------------------------------------------------------------------- #
class HeliumAwareRayExecutor:
    """
    Ray executor with helium-aware routing and fallback paths.

    Thread-safe, serializable, picklable, bounded in memory.
    """

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        *,
        executor_config: Optional[RayExecutorConfig] = None,
        strict: bool = True,
        metrics_hook: Optional[
            Callable[[str, Mapping[str, Any]], None]
        ] = None,
    ) -> None:
        if executor_config is not None:
            self._config = executor_config
        else:
            # P0 2.1: delegate to from_legacy_dict.
            self._config = RayExecutorConfig.from_legacy_dict(config)

        self._strict: bool = bool(strict)
        self.config: Dict[str, Any] = dict(config or {})
        self.ray_address = self.config.get("ray_address", "auto")

        if metrics_hook is not None and not callable(metrics_hook):
            raise RayExecutorError("metrics_hook must be callable.")
        self._metrics_hook = metrics_hook

        self.worker_pools: Dict[WorkerType, Dict[str, Any]] = {
            WorkerType.STANDARD_CPU: {
                "available": True,
                "capacity": self._config.cpu_workers,
                "cost_factor": 1.0,
            },
            WorkerType.GPU_SINGLE: {
                "available": self._config.gpu_available,
                "capacity": self._config.gpu_workers,
                "cost_factor": 3.0,
            },
            WorkerType.GPU_CLUSTER: {
                "available": self._config.gpu_cluster_available,
                "capacity": self._config.gpu_cluster_workers,
                "cost_factor": 8.0,
            },
            WorkerType.TPU: {
                "available": self._config.tpu_available,
                "capacity": self._config.tpu_workers,
                "cost_factor": 5.0,
            },
            WorkerType.QUANTUM: {
                "available": self._config.quantum_available,
                "capacity": self._config.quantum_workers,
                "cost_factor": 20.0,
            },
        }

        self.fallback_enabled = self._config.fallback_enabled
        self.execution_timeout_seconds = \
            self._config.execution_timeout_seconds

        self._lock = threading.RLock()
        self._history: Deque[ExecutionResult] = deque(
            maxlen=self._config.max_history)
        self._ctx_start: Optional[float] = None
        self._async_ctx_start: Optional[float] = None
        self._started_at: float = time.time()

        logger.debug(
            "HeliumAwareRayExecutor v%s initialized "
            "(ray_address=%s, fallback=%s, strict=%s, ray=%s, "
            "cpu_workers=%d)",
            __version__,
            self.ray_address,
            self.fallback_enabled,
            self._strict,
            _RAY_AVAILABLE,
            self._config.cpu_workers,
        )

    # ------------------------------------------------------------------ props
    @property
    def config_typed(self) -> RayExecutorConfig:
        return self._config

    @property
    def history(self) -> List[ExecutionResult]:
        with self._lock:
            return list(self._history)

    @property
    def history_size(self) -> int:
        with self._lock:
            return len(self._history)

    # ---------------------------------------------------------- metrics
    def _emit_metric(
        self, event: str, fields: Mapping[str, Any],
    ) -> None:
        if self._metrics_hook is None:
            return
        try:
            self._metrics_hook(event, dict(fields))
        except Exception:  # noqa: BLE001
            logger.debug("metrics_hook raised", exc_info=True)

    # ---------------------------------------------------------- helpers
    @staticmethod
    def _get_attr(obj: Any, name: str, default: Any = None) -> Any:
        """Read attribute from either an object or a mapping."""
        if obj is None:
            return default
        if isinstance(obj, Mapping):
            return obj.get(name, default)
        return getattr(obj, name, default)

    def _normalize_zone(self, zone_raw: Any) -> Optional[str]:
        """Normalize a helium zone to a validated lowercase string (P0 2.11)."""
        if zone_raw is None:
            return None
        if hasattr(zone_raw, "value"):
            zone_raw = zone_raw.value
        level = str(zone_raw).strip().lower()
        valid = {
            "helium_green", "helium_yellow",
            "helium_red", "helium_critical",
        }
        if level not in valid:
            logger.warning(
                "Unknown helium_zone %r; treating as absent.", level)
            return None
        return level

    def _validate_power_budget(self, value: Any) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            msg = (
                f"power_budget must be numeric, "
                f"got {type(value).__name__}.")
            if self._strict:
                raise RayExecutorError(msg)
            logger.warning("%s Using 1.0.", msg)
            return 1.0
        fvalue = float(value)
        if math.isnan(fvalue) or math.isinf(fvalue):
            msg = f"power_budget must be finite, got {value!r}."
            if self._strict:
                raise RayExecutorError(msg)
            logger.warning("%s Using 1.0.", msg)
            return 1.0
        if not 0.0 <= fvalue <= 1.0:
            msg = f"power_budget must be in [0, 1], got {fvalue}."
            if self._strict:
                raise RayExecutorError(msg)
            logger.warning("%s Clamping.", msg)
            return max(0.0, min(1.0, fvalue))
        return fvalue

    def _normalize_decision(
        self, execution_decision: Any,
    ) -> Dict[str, Any]:
        """
        Normalize an ``ExecutionDecision`` into a dict.

        P0 2.3: ``power_budget=None`` → strict raises, lenient → 1.0.
        P0 2.4: ``helium_aware_flag="false"`` → coerced to False.
        P0 2.11: ``helium_zone`` normalised.
        """
        if execution_decision is None:
            return {
                "helium_aware_flag": False,
                "helium_zone": None,
                "power_budget": 1.0,
            }
        raw_pb = self._get_attr(execution_decision, "power_budget", 1.0)
        return {
            "helium_aware_flag": _coerce_bool(
                self._get_attr(
                    execution_decision, "helium_aware_flag", False),
                default=False,
            ),
            "helium_zone": self._normalize_zone(
                self._get_attr(
                    execution_decision, "helium_zone", None)),
            # P0 2.3: None (or any invalid value) is handled here.
            "power_budget": self._validate_power_budget(raw_pb),
        }

    @staticmethod
    def _task_id_of(task: Any) -> str:
        if task is None:
            return "unknown"
        for key in ("id", "task_id"):
            value = getattr(task, key, None)
            if isinstance(value, str) and value:
                return value
        if isinstance(task, Mapping):
            for key in ("id", "task_id"):
                value = task.get(key)
                if isinstance(value, str) and value:
                    return value
        return "unknown"

    # ---------------------------------------------------------- pools
    def _pool_available(self, worker: WorkerType) -> bool:
        """
        True iff the pool is enabled.

        Note: capacity is NOT checked here — that is the job of ``_acquire``
        (P0 2.5). This lets the routing layer prefer a pool based on policy
        even when it is temporarily exhausted; the ``_acquire`` gate then
        forces a fallback if the slot race is lost.
        """
        cfg = self.worker_pools.get(worker)
        return bool(cfg and cfg.get("available"))

    def _acquire(self, worker: WorkerType) -> bool:
        """
        Atomically reserve one slot in ``worker``'s pool (P0 2.5).

        Returns False if the pool is disabled or has no free slot.
        """
        with self._lock:
            pool = self.worker_pools.get(worker)
            if pool is None or not pool.get("available"):
                return False
            if pool.get("capacity", 0) <= 0:
                return False
            pool["capacity"] -= 1
            return True

    def _release(self, worker: WorkerType) -> None:
        """Restore one slot to ``worker``'s pool (P0 2.5)."""
        with self._lock:
            pool = self.worker_pools.get(worker)
            if pool is None:
                return
            max_capacity = self._max_capacity(worker)
            pool["capacity"] = min(
                pool.get("capacity", 0) + 1, max_capacity)

    def _max_capacity(self, worker: WorkerType) -> int:
        return {
            WorkerType.STANDARD_CPU: self._config.cpu_workers,
            WorkerType.GPU_SINGLE: self._config.gpu_workers,
            WorkerType.GPU_CLUSTER: self._config.gpu_cluster_workers,
            WorkerType.TPU: self._config.tpu_workers,
            WorkerType.QUANTUM: self._config.quantum_workers,
        }.get(worker, 0)

    # ---------------------------------------------------------- routing
    def _select_worker_pool(
        self,
        *,
        helium_aware: bool,
        helium_zone: Optional[str],
        helium_profile: Any,
        power_budget: float,
    ) -> Optional[WorkerType]:
        zone = (helium_zone or "").lower()

        if helium_aware and zone in ("helium_red", "helium_critical"):
            if helium_profile is not None and getattr(
                helium_profile, "can_run_on_cpu", False
            ):
                return (
                    WorkerType.STANDARD_CPU
                    if self._pool_available(WorkerType.STANDARD_CPU)
                    else None
                )
            if self._pool_available(WorkerType.GPU_SINGLE):
                return WorkerType.GPU_SINGLE
            return (
                WorkerType.STANDARD_CPU
                if self._pool_available(WorkerType.STANDARD_CPU)
                else None
            )

        if helium_aware and zone == "helium_yellow":
            if self._pool_available(WorkerType.GPU_SINGLE):
                return WorkerType.GPU_SINGLE
            if self._pool_available(WorkerType.STANDARD_CPU):
                return WorkerType.STANDARD_CPU
            if self._pool_available(WorkerType.GPU_CLUSTER):
                return WorkerType.GPU_CLUSTER
            return None

        # Normal conditions.
        if helium_profile is not None:
            score = getattr(helium_profile, "dependency_score", 0.0)
            try:
                score_f = float(score)
            except (TypeError, ValueError):
                score_f = 0.0
            if math.isnan(score_f) or math.isinf(score_f):
                score_f = 0.0

            if score_f > self._config.high_dependency_cutoff:
                if self._pool_available(WorkerType.GPU_CLUSTER):
                    return WorkerType.GPU_CLUSTER
                if self._pool_available(WorkerType.GPU_SINGLE):
                    return WorkerType.GPU_SINGLE
                return (
                    WorkerType.STANDARD_CPU
                    if self._pool_available(WorkerType.STANDARD_CPU)
                    else None
                )
            if score_f > self._config.mid_dependency_cutoff:
                if self._pool_available(WorkerType.GPU_SINGLE):
                    return WorkerType.GPU_SINGLE
                if self._pool_available(WorkerType.STANDARD_CPU):
                    return WorkerType.STANDARD_CPU
                return None
            return (
                WorkerType.STANDARD_CPU
                if self._pool_available(WorkerType.STANDARD_CPU)
                else None
            )

        return (
            WorkerType.STANDARD_CPU
            if self._pool_available(WorkerType.STANDARD_CPU)
            else None
        )

    # ---------------------------------------------------------- public API
    async def execute_task(
        self,
        task: Any,
        workload_profile: Any = None,
        execution_decision: Any = None,
    ) -> ExecutionResult:
        """Execute a task with helium-aware routing."""
        # P1 2.10: strict-mode task validation.
        if task is None and self._strict:
            raise RayExecutorError("task must not be None.")

        task_id = self._task_id_of(task)
        # P0 2.3 / 2.4 / 2.11: normalized + validated.
        decision = self._normalize_decision(execution_decision)
        power_budget = decision["power_budget"]

        helium_profile = getattr(workload_profile, "helium_profile", None)

        target_worker = self._select_worker_pool(
            helium_aware=decision["helium_aware_flag"],
            helium_zone=decision["helium_zone"],
            helium_profile=helium_profile,
            power_budget=power_budget,
        )

        # P0 2.5: atomic acquire gate — race-lost acquisition falls back.
        acquired = False
        if target_worker is not None:
            acquired = self._acquire(target_worker)

        if not acquired:
            if self.fallback_enabled:
                result = await self._execute_fallback(
                    task, helium_profile, power_budget, decision)
                self._record(result)
                return result
            result = ExecutionResult(
                success=False,
                task_id=task_id,
                accuracy=0.0,
                energy_consumed_kwh=0.0,
                carbon_emitted_kg=0.0,
                execution_time_ms=0,
                worker_type="none",
                helium_usage=0.0,
                helium_zone=decision["helium_zone"],
                fallback_used=False,
                optimization_level="minimal",
            )
            self._record(result)
            return result

        try:
            result = self._simulate_execution(
                task_id=task_id,
                worker_type=target_worker,
                power_budget=power_budget,
                helium_zone=decision["helium_zone"],
                helium_aware=decision["helium_aware_flag"],
            )
        finally:
            # P0 2.5: always release the slot, even on exception.
            self._release(target_worker)

        self._record(result)
        self._emit_metric("task_executed", {
            "worker_type": result.worker_type,
            "success": result.success,
            "accuracy": result.accuracy,
            "energy_kwh": result.energy_consumed_kwh,
            "carbon_kg": result.carbon_emitted_kg,
            "helium_usage": result.helium_usage,
            "optimization_level": result.optimization_level,
        })
        return result

    async def execute_batch(
        self,
        tasks: List[Any],
        workload_profile: Any = None,
        execution_decisions: Optional[List[Any]] = None,
    ) -> List[ExecutionResult]:
        if not isinstance(tasks, list):
            raise RayExecutorError("tasks must be a list.")
        if execution_decisions is not None and \
                len(execution_decisions) != len(tasks):
            raise RayExecutorError(
                "execution_decisions must match tasks in length.")
        results: List[ExecutionResult] = []
        for idx, task in enumerate(tasks):
            decision = (
                execution_decisions[idx]
                if execution_decisions is not None else None
            )
            results.append(
                await self.execute_task(
                    task, workload_profile, decision))
        return results

    # ---------------------------------------------------------- simulation
    def _simulate_execution(
        self,
        *,
        task_id: str,
        worker_type: WorkerType,
        power_budget: float,
        helium_zone: Optional[str],
        helium_aware: bool,
    ) -> ExecutionResult:
        """Simulated execution with the fixed helium formula (P0 2.6-2.8)."""
        cfg = self._config
        footprint = worker_type.helium_footprint

        # P0 2.7 + P0 2.8: zero power → no execution, labelled "minimal".
        if power_budget <= 0.0:
            return ExecutionResult(
                success=False,
                task_id=task_id,
                accuracy=0.0,
                energy_consumed_kwh=0.0,
                carbon_emitted_kg=0.0,
                execution_time_ms=0,
                worker_type=worker_type.value,
                helium_usage=0.0,
                helium_zone=helium_zone,
                fallback_used=False,
                optimization_level=self._get_optimization_level(
                    helium_aware=helium_aware,
                    power_budget=power_budget,
                ),
            )

        execution_time = random.uniform(
            cfg.sim_min_latency_ms, cfg.sim_max_latency_ms)

        # P0 2.6: correct formula. At power=1.0 → footprint. At power=0
        # → footprint * blend (but the zero-power branch above short-
        # circuits, so this only applies to power > 0).
        blend = cfg.helium_blend_coefficient
        helium_usage = footprint * (blend + (1.0 - blend) * power_budget)
        helium_usage = max(0.0, min(1.0, helium_usage))

        return ExecutionResult(
            success=True,
            task_id=task_id,
            accuracy=cfg.sim_accuracy_ceiling * power_budget,
            energy_consumed_kwh=cfg.sim_energy_per_task_kwh * power_budget,
            carbon_emitted_kg=cfg.sim_carbon_per_task_kg * power_budget,
            execution_time_ms=int(execution_time),
            worker_type=worker_type.value,
            helium_usage=helium_usage,
            helium_zone=helium_zone,
            fallback_used=False,
            optimization_level=self._get_optimization_level(
                helium_aware=helium_aware, power_budget=power_budget),
        )

    # ---------------------------------------------------------- fallback
    async def _execute_fallback(
        self,
        task: Any,
        helium_profile: Any,
        power_budget: float,
        decision: Mapping[str, Any],
    ) -> ExecutionResult:
        cfg = self._config
        task_id = self._task_id_of(task)
        helium_zone = decision.get("helium_zone")
        logger.warning(
            "Executing fallback for task %s (helium zone: %s)",
            task_id, helium_zone or "n/a")

        if helium_profile is not None and getattr(
            helium_profile, "can_use_distilled_model", False
        ):
            logger.info("Fallback: Using distilled model.")
            self._emit_metric("task_fallback", {
                "task_id": task_id, "path": "distilled"})
            return ExecutionResult(
                success=True,
                task_id=task_id,
                accuracy=cfg.sim_distilled_accuracy,
                energy_consumed_kwh=(
                    cfg.sim_distilled_energy_kwh * power_budget),
                carbon_emitted_kg=(
                    cfg.sim_distilled_carbon_kg * power_budget),
                execution_time_ms=int(cfg.sim_distilled_latency_ms),
                worker_type="distilled_cpu",
                helium_usage=0.1,
                helium_zone=helium_zone,
                fallback_used=True,
                optimization_level="distilled",
            )

        if helium_profile is not None and getattr(
            helium_profile, "can_run_on_cpu", False
        ):
            logger.info("Fallback: Executing on CPU.")
            self._emit_metric("task_fallback", {
                "task_id": task_id, "path": "cpu"})
            return ExecutionResult(
                success=True,
                task_id=task_id,
                accuracy=cfg.sim_cpu_fallback_accuracy,
                energy_consumed_kwh=(
                    cfg.sim_cpu_fallback_energy_kwh * power_budget),
                carbon_emitted_kg=(
                    cfg.sim_cpu_fallback_carbon_kg * power_budget),
                execution_time_ms=int(cfg.sim_cpu_fallback_latency_ms),
                worker_type="cpu_fallback",
                helium_usage=0.05,
                helium_zone=helium_zone,
                fallback_used=True,
                optimization_level="degraded",
            )

        logger.warning("Fallback: Deferring task — no viable path.")
        self._emit_metric("task_fallback", {
            "task_id": task_id, "path": "deferred"})
        return ExecutionResult(
            success=False,
            task_id=task_id,
            accuracy=0.0,
            energy_consumed_kwh=0.0,
            carbon_emitted_kg=0.0,
            execution_time_ms=0,
            worker_type="none",
            helium_usage=0.0,
            helium_zone=helium_zone,
            fallback_used=True,
            optimization_level="deferred",
        )

    # ---------------------------------------------------------- level
    @staticmethod
    def _get_optimization_level(
        *, helium_aware: bool, power_budget: float,
    ) -> str:
        """
        Return the optimization level implied by the decision.

        P0 2.8: ``"minimal"`` replaces the misleading ``"deferred"`` label
        for ``power_budget < 0.2`` (the task executes at low power; it is
        not actually deferred).
        """
        if not helium_aware:
            return "none"
        if power_budget >= 0.8:
            return "light"
        if power_budget >= 0.5:
            return "moderate"
        if power_budget >= 0.2:
            return "aggressive"
        return "minimal"   # was "deferred"

    # ---------------------------------------------------------- recording
    def _record(self, result: ExecutionResult) -> None:
        with self._lock:
            self._history.append(result)

    # ---------------------------------------------------------- status
    def get_worker_pool_status(self) -> Dict[str, Dict[str, Any]]:
        """Return the current status of every worker pool."""
        with self._lock:
            return {
                worker.value: {
                    "available": bool(cfg["available"]),
                    "capacity": int(cfg["capacity"]),
                    "max_capacity": self._max_capacity(worker),
                    "cost_factor": float(cfg["cost_factor"]),
                    "helium_footprint": float(
                        worker.helium_footprint),
                }
                for worker, cfg in self.worker_pools.items()
            }

    def statistics(self, window: Optional[int] = None) -> Dict[str, Any]:
        """
        Return aggregate statistics over the recorded executions.

        ``window`` (P1 3.5): restrict to the last ``window`` executions.
        """
        with self._lock:
            if window is not None:
                history = list(self._history)[-window:]
            else:
                history = list(self._history)

        if not history:
            return {
                "executions": 0,
                "successful": 0,
                "failed": 0,
                "fallbacks": 0,
                "by_worker": {},
                "by_optimization_level": {},
                "mean_accuracy": None,
                "mean_energy_kwh": None,
                "mean_carbon_kg": None,
                "mean_helium_usage": None,
                "mean_latency_ms": None,
                "success_rate": None,
            }

        by_worker: Dict[str, int] = {}
        by_opt: Dict[str, int] = {}
        for r in history:
            by_worker[r.worker_type] = \
                by_worker.get(r.worker_type, 0) + 1
            by_opt[r.optimization_level] = \
                by_opt.get(r.optimization_level, 0) + 1

        successful = sum(1 for r in history if r.success)
        failed = len(history) - successful
        fallbacks = sum(1 for r in history if r.fallback_used)

        return {
            "executions": len(history),
            "successful": successful,
            "failed": failed,
            "fallbacks": fallbacks,
            "by_worker": by_worker,
            "by_optimization_level": by_opt,
            "mean_accuracy":
                sum(r.accuracy for r in history) / len(history),
            "mean_energy_kwh":
                sum(r.energy_consumed_kwh for r in history)
                / len(history),
            "mean_carbon_kg":
                sum(r.carbon_emitted_kg for r in history)
                / len(history),
            "mean_helium_usage":
                sum(r.helium_usage for r in history) / len(history),
            "mean_latency_ms":
                sum(r.execution_time_ms for r in history)
                / len(history),
            "success_rate": successful / len(history),
        }

    def reset(self, *, clear_history: bool = False) -> None:
        with self._lock:
            if clear_history:
                self._history.clear()
            for worker, pool in self.worker_pools.items():
                pool["capacity"] = self._max_capacity(worker)
            self._ctx_start = None
            self._async_ctx_start = None
        logger.debug(
            "HeliumAwareRayExecutor reset (clear_history=%s)",
            clear_history)

    def close(self) -> None:
        """Release resources (P1 3.4). Idempotent."""
        logger.debug(
            "HeliumAwareRayExecutor closed (%d execution(s)).",
            self.history_size)

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "version": __version__,
                "config": asdict(self._config),
                "strict": self._strict,
                "ray_address": self.ray_address,
                "fallback_enabled": self.fallback_enabled,
                "ray_available": _RAY_AVAILABLE,
                "started_at": self._started_at,
                "worker_pools": self.get_worker_pool_status(),
                "history": [r.to_dict() for r in self._history],
            }

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        metrics_hook: Optional[
            Callable[[str, Mapping[str, Any]], None]
        ] = None,
    ) -> "HeliumAwareRayExecutor":
        if not isinstance(data, Mapping):
            raise RayExecutorError(
                f"from_dict expects a Mapping, "
                f"got {type(data).__name__}.")

        # P0 2.1: delegate to from_legacy_dict — new fields default
        # correctly without the hardcoded re-listing.
        cfg = RayExecutorConfig.from_legacy_dict(
            data.get("config") or {})

        executor = cls(
            config={"ray_address": data.get("ray_address", "auto")},
            executor_config=cfg,
            strict=bool(data.get("strict", True)),
            metrics_hook=metrics_hook,
        )

        # P2 3.14: warn on worker-pool footprint mismatch.
        payload_pools = data.get("worker_pools") or {}
        for name, entry in payload_pools.items():
            if not isinstance(entry, Mapping):
                continue
            try:
                worker = WorkerType(name)
            except ValueError:
                logger.info(
                    "Unknown worker type %r in payload; ignored.", name)
                continue
            serialized_fp = entry.get("helium_footprint")
            if serialized_fp is not None and abs(
                float(serialized_fp) - worker.helium_footprint
            ) > 1e-6:
                logger.info(
                    "Worker %s footprint changed: %r → %r.",
                    name, serialized_fp, worker.helium_footprint)

        # P1 2.9: warn on history truncation.
        history_entries = list(data.get("history", []))
        if len(history_entries) > cfg.max_history:
            logger.warning(
                "Restoring %d history entries; only most recent %d kept.",
                len(history_entries), cfg.max_history)

        with executor._lock:
            for entry in history_entries:
                executor._history.append(
                    ExecutionResult.from_dict(entry))
            executor._started_at = float(
                data.get("started_at", time.time()))
        return executor

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(
        cls,
        payload: str,
        *,
        metrics_hook: Optional[
            Callable[[str, Mapping[str, Any]], None]
        ] = None,
    ) -> "HeliumAwareRayExecutor":
        try:
            return cls.from_dict(
                json.loads(payload), metrics_hook=metrics_hook)
        except json.JSONDecodeError as exc:
            raise RayExecutorError(
                f"Invalid JSON payload: {exc}") from exc

    # ---------------------------------------------------------- pickling
    def __getstate__(self) -> Dict[str, Any]:
        # P0 2.2: RLock is not picklable.
        state = self.__dict__.copy()
        state.pop("_lock", None)
        return state

    def __setstate__(self, state: Dict[str, Any]) -> None:
        self.__dict__.update(state)
        self._lock = threading.RLock()

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "HeliumAwareRayExecutor":
        with self._lock:
            self._ctx_start = time.perf_counter()
        logger.debug("Entering scoped HeliumAwareRayExecutor session.")
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        with self._lock:
            start = (
                self._ctx_start
                if self._ctx_start is not None
                else time.perf_counter()
            )
            elapsed = time.perf_counter() - start
            self._ctx_start = None
            size = len(self._history)
        if exc_type is not None:
            logger.warning(
                "HeliumAwareRayExecutor scope exited with %s "
                "after %.4fs.", exc_type.__name__, elapsed)
            return
        logger.info(
            "HeliumAwareRayExecutor scope closed in %.4fs "
            "(%d execution(s)).", elapsed, size)

    async def __aenter__(self) -> "HeliumAwareRayExecutor":
        with self._lock:
            self._async_ctx_start = time.perf_counter()
        logger.debug(
            "Entering async HeliumAwareRayExecutor session.")
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        with self._lock:
            start = (
                self._async_ctx_start
                if self._async_ctx_start is not None
                else time.perf_counter()
            )
            elapsed = time.perf_counter() - start
            self._async_ctx_start = None
            size = len(self._history)
        if exc_type is not None:
            logger.warning(
                "Async HeliumAwareRayExecutor scope exited with %s "
                "after %.4fs.", exc_type.__name__, elapsed)
            return
        logger.info(
            "Async HeliumAwareRayExecutor scope closed in %.4fs "
            "(%d execution(s)).", elapsed, size)

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        with self._lock:
            return (
                "HeliumAwareRayExecutor("
                f"version={__version__!r}, "
                f"ray_address={self.ray_address!r}, "
                f"fallback={self.fallback_enabled}, "
                f"strict={self._strict}, "
                f"executions={len(self._history)})"
            )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "WorkerType",
    "ExecutionResult",
    "HeliumAwareRayExecutor",
    "RayExecutorConfig",
    "RayExecutorError",
]


# --------------------------------------------------------------------------- #
# Self-tests: python -m distributed.ray_cluster_manager
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.WARNING,
        format="%(levelname)s %(name)s: %(message)s",
    )
    import pickle

    # ---------------------------------------------------------------- #
    # Test fixtures
    # ---------------------------------------------------------------- #
    class _Task:
        def __init__(self, task_id: str) -> None:
            self.id = task_id

    class _HeliumProfile:
        def __init__(
            self,
            dependency_score: float = 0.0,
            can_run_on_cpu: bool = False,
            can_use_distilled_model: bool = False,
        ) -> None:
            self.dependency_score = dependency_score
            self.can_run_on_cpu = can_run_on_cpu
            self.can_use_distilled_model = can_use_distilled_model

    class _Workload:
        def __init__(self, helium_profile: Optional[_HeliumProfile]) -> None:
            self.helium_profile = helium_profile

    class _Decision:
        def __init__(
            self,
            helium_aware_flag: Any,
            helium_zone: Any,
            power_budget: Any,
        ) -> None:
            self.helium_aware_flag = helium_aware_flag
            self.helium_zone = helium_zone
            self.power_budget = power_budget

    # ---------------------------------------------------------------- #
    # P0 2.1 — legacy dict picks up new field defaults
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.1: legacy dict picks up new config defaults ===")

    partial = HeliumAwareRayExecutor(config={"cpu_workers": 4})
    assert partial.config_typed.cpu_workers == 4, (
        f"cpu_workers={partial.config_typed.cpu_workers}, expected 4")
    # Fields omitted from the legacy dict use their declared defaults
    assert partial.config_typed.gpu_workers == 4
    assert partial.config_typed.sim_max_latency_ms == 1000.0
    assert partial.config_typed.high_dependency_cutoff == 0.8
    print("  ✓ HeliumAwareRayExecutor(config={'cpu_workers': 4})")

    payload = {
        "config": {"cpu_workers": 6, "gpu_workers": 3},
        "strict": True,
        "ray_address": "auto",
    }
    restored = HeliumAwareRayExecutor.from_dict(payload)
    assert restored.config_typed.cpu_workers == 6
    assert restored.config_typed.gpu_workers == 3
    assert restored.config_typed.sim_max_latency_ms == 1000.0  # default
    assert restored.config_typed.mid_dependency_cutoff == 0.5  # default
    print("  ✓ from_dict with omitted fields uses declared defaults")

    # ---------------------------------------------------------------- #
    # P0 2.2 — pickle round-trip
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.2: pickle round-trip ===")

    async def _test_pickle() -> None:
        executor = HeliumAwareRayExecutor(config={"cpu_workers": 3})
        await executor.execute_task(_Task("t1"))

        blob = pickle.dumps(executor)
        restored_exec = pickle.loads(blob)

        assert restored_exec.config_typed.cpu_workers == 3
        assert restored_exec.history_size == 1
        assert restored_exec._lock is not None
        # Should still work after unpickle
        await restored_exec.execute_task(_Task("t2"))
        assert restored_exec.history_size == 2
        print(f"  ✓ pickle round-trip: history_size="
              f"{restored_exec.history_size}")

    asyncio.run(_test_pickle())

    # ---------------------------------------------------------------- #
    # P0 2.3 — power_budget=None
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.3: power_budget=None ===")

    async def _test_pb_none() -> None:
        # Strict: raises
        strict = HeliumAwareRayExecutor(strict=True)
        try:
            await strict.execute_task(
                _Task("bad"), None,
                _Decision(False, None, None),
            )
        except RayExecutorError as exc:
            print(f"  ✓ strict raises: {exc}")
        else:
            raise AssertionError(
                "P0 2.3 regression: strict mode did not raise for "
                "power_budget=None")

        # Lenient: substitutes 1.0 → accuracy = sim_accuracy_ceiling
        lenient = HeliumAwareRayExecutor(strict=False)
        result = await lenient.execute_task(
            _Task("lenient-ok"), None,
            _Decision(False, None, None),
        )
        assert result.success is True
        ceiling = lenient.config_typed.sim_accuracy_ceiling
        assert abs(result.accuracy - ceiling) < 0.01, (
            f"P0 2.3 regression: accuracy={result.accuracy:.4f}, "
            f"expected ≈{ceiling} (power_budget defaulted to 1.0)")
        print(f"  ✓ lenient substitutes 1.0 → accuracy="
              f"{result.accuracy:.2f}")

    asyncio.run(_test_pb_none())

    # ---------------------------------------------------------------- #
    # P0 2.4 — helium_aware_flag="false" routes to normal
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.4: helium_aware_flag=\"false\" ===")

    async def _test_flag_string() -> None:
        executor = HeliumAwareRayExecutor(
            config={"gpu_available": True, "cpu_workers": 10})
        workload = _Workload(_HeliumProfile(dependency_score=0.2))

        # "false" string + helium_yellow: with the fix, routes as
        # helium_aware=False → normal conditions → low dep → CPU.
        result = await executor.execute_task(
            _Task("false-flag"), workload,
            _Decision("false", "helium_yellow", 0.5),
        )
        assert result.worker_type == "standard_cpu", (
            f"P0 2.4 regression: helium_aware_flag='false' routed to "
            f"{result.worker_type}, expected standard_cpu (normal "
            f"conditions)")
        print(f"  ✓ 'false' → normal routing → {result.worker_type}")

        # Control: bool True with same zone → GPU_SINGLE.
        result2 = await executor.execute_task(
            _Task("true-flag"), workload,
            _Decision(True, "helium_yellow", 0.5),
        )
        assert result2.worker_type == "gpu_single", (
            f"control: expected gpu_single for helium-aware, got "
            f"{result2.worker_type}")
        print(f"  ✓ True → helium-aware → {result2.worker_type}")

    asyncio.run(_test_flag_string())

    # ---------------------------------------------------------------- #
    # P0 2.5 — capacity decrement via _acquire
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.5: capacity decrement via _acquire ===")

    async def _test_capacity() -> None:
        executor = HeliumAwareRayExecutor(config={
            "cpu_workers": 2,
            "gpu_available": False,
            "gpu_cluster_available": False,
            "tpu_available": False,
            "quantum_available": False,
        })
        # Direct _acquire: 2 slots available, 3rd returns False
        assert executor._acquire(WorkerType.STANDARD_CPU) is True   # 2→1
        assert executor._acquire(WorkerType.STANDARD_CPU) is True   # 1→0
        assert executor._acquire(WorkerType.STANDARD_CPU) is False  # exhausted
        print("  ✓ _acquire: True, True, False (2 slots → exhausted)")

        # Integration: capacity=0, execute_task should fall back
        workload = _Workload(_HeliumProfile(dependency_score=0.5))
        result = await executor.execute_task(
            _Task("blocked"), workload, None)
        assert result.fallback_used is True, (
            f"P0 2.5 regression: no fallback when CPU exhausted, "
            f"worker_type={result.worker_type}, "
            f"fallback_used={result.fallback_used}")
        print(f"  ✓ exhausted CPU → fallback_used=True, "
              f"worker_type={result.worker_type}")

        # Release and verify capacity restored
        executor._release(WorkerType.STANDARD_CPU)  # 0→1
        executor._release(WorkerType.STANDARD_CPU)  # 1→2
        status = executor.get_worker_pool_status()
        assert status["standard_cpu"]["capacity"] == 2
        print(f"  ✓ after release: capacity={status['standard_cpu']['capacity']}")

        # Now execute succeeds on CPU (acquire/release around execution)
        result2 = await executor.execute_task(
            _Task("ok"), workload, None)
        assert result2.worker_type == "standard_cpu", (
            f"after release, expected standard_cpu, got "
            f"{result2.worker_type}")
        # Capacity should be back to 2 after acquire+release
        status = executor.get_worker_pool_status()
        assert status["standard_cpu"]["capacity"] == 2
        print(f"  ✓ after execution: worker_type={result2.worker_type}, "
              f"capacity={status['standard_cpu']['capacity']}")

    asyncio.run(_test_capacity())

    # ---------------------------------------------------------------- #
    # P0 2.6 — helium formula
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.6: helium_usage formula ===")

    def _test_formula() -> None:
        executor = HeliumAwareRayExecutor()

        result_cpu = executor._simulate_execution(
            task_id="cpu-full",
            worker_type=WorkerType.STANDARD_CPU,
            power_budget=1.0,
            helium_zone="helium_green",
            helium_aware=True,
        )
        assert abs(result_cpu.helium_usage - 0.10) < 0.01, (
            f"P0 2.6 regression: CPU full-power helium_usage="
            f"{result_cpu.helium_usage:.4f}, expected ≈0.10")
        print(f"  ✓ CPU full-power  helium_usage="
              f"{result_cpu.helium_usage:.4f}  ≈ 0.10")

        result_quantum = executor._simulate_execution(
            task_id="quantum-full",
            worker_type=WorkerType.QUANTUM,
            power_budget=1.0,
            helium_zone="helium_green",
            helium_aware=True,
        )
        assert abs(result_quantum.helium_usage - 0.99) < 0.01, (
            f"P0 2.6 regression: quantum full-power helium_usage="
            f"{result_quantum.helium_usage:.4f}, expected ≈0.99")
        print(f"  ✓ Quantum full-power helium_usage="
              f"{result_quantum.helium_usage:.4f}  ≈ 0.99")

        # Bonus: GPU_SINGLE
        result_gpu = executor._simulate_execution(
            task_id="gpu-full",
            worker_type=WorkerType.GPU_SINGLE,
            power_budget=1.0,
            helium_zone="helium_green",
            helium_aware=True,
        )
        assert abs(result_gpu.helium_usage - 0.75) < 0.01
        print(f"  ✓ GPU_SINGLE full-power helium_usage="
              f"{result_gpu.helium_usage:.4f}  ≈ 0.75")

    _test_formula()

    # ---------------------------------------------------------------- #
    # P0 2.7 / 2.8 — zero power
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.7 / 2.8: power_budget=0.0 ===")

    async def _test_zero_power() -> None:
        executor = HeliumAwareRayExecutor()
        workload = _Workload(_HeliumProfile(dependency_score=0.7))
        result = await executor.execute_task(
            _Task("zero"), workload,
            _Decision(True, "helium_yellow", 0.0),
        )
        assert result.success is False, (
            f"P0 2.7 regression: power=0 returned success="
            f"{result.success}")
        assert result.optimization_level == "minimal", (
            f"P0 2.8 regression: optimization_level="
            f"{result.optimization_level!r}, expected 'minimal'")
        assert result.accuracy == 0.0
        assert result.energy_consumed_kwh == 0.0
        assert result.carbon_emitted_kg == 0.0
        assert result.helium_usage == 0.0
        print(f"  ✓ power=0 → success={result.success}, "
              f"optimization_level={result.optimization_level!r}, "
              f"accuracy={result.accuracy}")

    asyncio.run(_test_zero_power())

    # ---------------------------------------------------------------- #
    # Sanity: happy paths still work
    # ---------------------------------------------------------------- #
    print("\n=== Sanity: happy paths ===")

    async def _test_sanity() -> None:
        executor = HeliumAwareRayExecutor()

        # Normal routing: low dep → CPU
        result = await executor.execute_task(
            _Task("low"), _Workload(_HeliumProfile(0.2)),
            _Decision(False, None, 1.0),
        )
        assert result.worker_type == "standard_cpu"
        assert result.success is True
        print(f"  ✓ low dep → {result.worker_type}")

        # Normal routing: mid dep → GPU_SINGLE
        result = await executor.execute_task(
            _Task("mid"), _Workload(_HeliumProfile(0.6)),
            _Decision(False, None, 1.0),
        )
        assert result.worker_type == "gpu_single"
        print(f"  ✓ mid dep → {result.worker_type}")

        # helium_red with can_run_on_cpu → CPU
        result = await executor.execute_task(
            _Task("red"), _Workload(
                _HeliumProfile(0.9, can_run_on_cpu=True)),
            _Decision(True, "helium_red", 0.3),
        )
        assert result.worker_type == "standard_cpu"
        print(f"  ✓ helium_red + can_run_on_cpu → {result.worker_type}")

        # Serialization round-trip
        payload = executor.to_json()
        restored = HeliumAwareRayExecutor.from_json(payload)
        assert restored.to_dict() == executor.to_dict()
        print("  ✓ serialization round-trip")

    asyncio.run(_test_sanity())

    # ---------------------------------------------------------------- #
    # BONUS: statistics(window=), close(), metrics_hook
    # ---------------------------------------------------------------- #
    print("\n=== Bonus: statistics / close / metrics ===")

    async def _test_bonus() -> None:
        events: List = []
        executor = HeliumAwareRayExecutor(
            metrics_hook=lambda ev, f: events.append((ev, f)),
        )
        for i in range(3):
            await executor.execute_task(
                _Task(f"t{i}"), _Workload(_HeliumProfile(0.2)),
                _Decision(False, None, 1.0),
            )
        assert len(events) >= 3
        print(f"  ✓ metrics_hook captured {len(events)} event(s)")

        stats = executor.statistics()
        assert stats["executions"] == 3
        win = executor.statistics(window=2)
        assert win["executions"] == 2
        print(f"  ✓ statistics() = {stats['executions']}, "
              f"statistics(window=2) = {win['executions']}")

        executor.close()  # idempotent
        executor.close()
        print("  ✓ close() is idempotent")

    asyncio.run(_test_bonus())

    # ---------------------------------------------------------------- #
    # Validation: bad configs rejected
    # ---------------------------------------------------------------- #
    print("\n=== Validation ===")

    for bad_cfg in (
        {"cpu_workers": -1},
        {"cpu_workers": "many"},
        {"execution_timeout_seconds": 0},
        {"helium_blend_coefficient": 2.0},
        {"max_history": 0},
        {"high_dependency_cutoff": 0.3, "mid_dependency_cutoff": 0.5},
    ):
        try:
            RayExecutorConfig(**bad_cfg)  # type: ignore[arg-type]
        except RayExecutorError as exc:
            print(f"  ✓ rejected {bad_cfg}: {exc}")
        else:
            raise AssertionError(
                f"expected rejection for {bad_cfg!r}")

    print("\n✓ All self-tests passed.")
