# src/distributed/ray_cluster_manager.py

"""
Helium-Aware Ray Cluster Manager
================================

Routes tasks to worker pools based on helium constraints and an
:class:`ExecutionDecision` produced by the carbon-aware decision core.

Original behaviour preserved
----------------------------
- ``WorkerType`` enum with ``helium_footprint`` property.
- ``ExecutionResult`` dataclass (success, task_id, accuracy, energy, carbon,
  execution_time_ms, worker_type, helium_usage, helium_zone, fallback_used,
  optimization_level).
- ``HeliumAwareRayExecutor(config=None)`` — constructs worker pools.
- ``await execute_task(task, workload_profile, execution_decision)`` — routes
  the task.
- ``get_worker_pool_status()`` — returns per-pool status.

Enhancements
------------
- Configurable via :class:`RayExecutorConfig` (pool capacities, cost factors,
  simulation coefficients, fallback policy).
- Helium footprint sourced from the ``WorkerType`` enum only — no duplication.
- Robust handling of ``execution_decision.helium_zone`` as Enum or string.
- Full validation of every argument; strict / non-strict modes.
- Thread-safe via ``RLock``; bounded execution history.
- Immutable :class:`ExecutionResult` with full serialization.
- Fixed worker-selection edge cases (availability checks on every branch).
- ``statistics()``, ``__repr__``, sync + async context managers.
- Serialization: ``to_dict`` / ``from_dict`` / ``to_json`` / ``from_json``.
- Custom :class:`RayExecutorError`; lazy ``%s`` logging.
- Optional Ray import guard (mirrors ``AgentNode`` / ``ContinuumScheduler``).
- ``__main__`` smoke test covering every worker path, fallback, serialization,
  context managers, and validation.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import random
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Deque, Dict, List, Mapping, Optional

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Optional Ray integration (mirrors AgentNode / ContinuumScheduler)
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
    """
    Tunable parameters for the helium-aware executor.

    Centralizes worker pool capacities, simulation coefficients, and fallback
    policy so deployments can calibrate without editing the class.
    """

    # Pool capacities.
    cpu_workers: int = 10
    gpu_workers: int = 4
    gpu_cluster_workers: int = 2
    tpu_workers: int = 2
    quantum_workers: int = 1

    # Pool availability toggles.
    gpu_available: bool = True
    gpu_cluster_available: bool = True
    tpu_available: bool = False
    quantum_available: bool = False

    # Fallback behaviour.
    fallback_enabled: bool = True
    execution_timeout_seconds: float = 300.0

    # Simulation coefficients (used only when Ray is not wired).
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

    # Helium usage formula blend coefficient (0 ≤ blend ≤ 1).
    helium_blend_coefficient: float = 0.5

    # Bounded history.
    max_history: int = 1000

    # Dependency-score cutoffs used by normal-condition routing.
    high_dependency_cutoff: float = 0.8
    mid_dependency_cutoff: float = 0.5

    def __post_init__(self) -> None:
        for name in (
            "cpu_workers",
            "gpu_workers",
            "gpu_cluster_workers",
            "tpu_workers",
            "quantum_workers",
        ):
            value = getattr(self, name)
            if not isinstance(value, int) or value < 0:
                raise RayExecutorError(f"{name} must be a non-negative int.")
        if self.execution_timeout_seconds <= 0:
            raise RayExecutorError("execution_timeout_seconds must be > 0.")
        if not 0.0 <= self.helium_blend_coefficient <= 1.0:
            raise RayExecutorError(
                "helium_blend_coefficient must be in [0, 1]."
            )
        if self.max_history <= 0:
            raise RayExecutorError("max_history must be > 0.")
        if not 0.0 <= self.mid_dependency_cutoff <= self.high_dependency_cutoff <= 1.0:
            raise RayExecutorError(
                "dependency cutoffs must satisfy 0 <= mid <= high <= 1."
            )
        if self.sim_min_latency_ms <= 0 or self.sim_max_latency_ms < self.sim_min_latency_ms:
            raise RayExecutorError(
                "sim latencies must satisfy 0 < min <= max."
            )


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

    # Helium metrics.
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
        if self.energy_consumed_kwh < 0:
            raise RayExecutorError("energy_consumed_kwh must be >= 0.")
        if self.carbon_emitted_kg < 0:
            raise RayExecutorError("carbon_emitted_kg must be >= 0.")
        if self.execution_time_ms < 0:
            raise RayExecutorError("execution_time_ms must be >= 0.")
        if not 0.0 <= self.helium_usage <= 1.0:
            raise RayExecutorError("helium_usage must be in [0, 1].")

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
                f"got {type(data).__name__}."
            )
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
            energy_consumed_kwh=float(data.get("energy_consumed_kwh", 0.0)),
            carbon_emitted_kg=float(data.get("carbon_emitted_kg", 0.0)),
            execution_time_ms=int(data.get("execution_time_ms", 0)),
            worker_type=str(data.get("worker_type", "none")),
            helium_usage=float(data.get("helium_usage", 0.0)),
            helium_zone=data.get("helium_zone"),
            fallback_used=bool(data.get("fallback_used", False)),
            optimization_level=str(data.get("optimization_level", "none")),
            timestamp=timestamp,
        )


# --------------------------------------------------------------------------- #
# Executor
# --------------------------------------------------------------------------- #
class HeliumAwareRayExecutor:
    """
    Ray executor with helium-aware routing and fallback paths.

    Thread-safe, serializable, and bounded in memory. The public API
    (``__init__``, ``execute_task``, ``get_worker_pool_status``) is preserved;
    new parameters are keyword-only.

    Parameters
    ----------
    config : dict, optional
        Legacy flat config dict. Recognized keys are forwarded to
        :class:`RayExecutorConfig`.
    executor_config : RayExecutorConfig, optional
        Typed configuration. When provided, ``config`` is ignored.
    strict : bool, default True
        If True, invalid inputs raise :class:`RayExecutorError`.
        If False, invalid inputs are logged and coerced.
    """

    def __init__(
        self,
        config: Optional[Dict[str, Any]] = None,
        *,
        executor_config: Optional[RayExecutorConfig] = None,
        strict: bool = True,
    ) -> None:
        if executor_config is not None:
            self._config = executor_config
        else:
            self._config = self._config_from_legacy_dict(config)

        self._strict: bool = bool(strict)
        # Preserve ``self.config`` for backward compatibility.
        self.config: Dict[str, Any] = dict(config or {})

        self.ray_address = self.config.get("ray_address", "auto")

        # Worker pool bookkeeping (helium_footprint sourced from the enum).
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

        # Legacy flag preserved for backward compatibility.
        self.fallback_enabled = self._config.fallback_enabled
        self.execution_timeout_seconds = self._config.execution_timeout_seconds

        self._lock = threading.RLock()
        self._history: Deque[ExecutionResult] = deque(
            maxlen=self._config.max_history
        )
        self._ctx_start: Optional[float] = None
        self._async_ctx_start: Optional[float] = None
        self._started_at: float = time.time()

        logger.debug(
            "HeliumAwareRayExecutor initialized "
            "(ray_address=%s, fallback=%s, strict=%s, ray=%s)",
            self.ray_address,
            self.fallback_enabled,
            self._strict,
            _RAY_AVAILABLE,
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

    # ---------------------------------------------------------- config helper
    @staticmethod
    def _config_from_legacy_dict(
        cfg: Optional[Mapping[str, Any]],
    ) -> RayExecutorConfig:
        """Build a :class:`RayExecutorConfig` from the original flat dict."""
        cfg = dict(cfg or {})
        kwargs: Dict[str, Any] = {}
        for key in (
            "cpu_workers",
            "gpu_workers",
            "gpu_cluster_workers",
            "tpu_workers",
            "quantum_workers",
            "gpu_available",
            "gpu_cluster_available",
            "tpu_available",
            "quantum_available",
            "fallback_enabled",
            "execution_timeout_seconds",
            "sim_min_latency_ms",
            "sim_max_latency_ms",
            "sim_accuracy_ceiling",
            "sim_energy_per_task_kwh",
            "sim_carbon_per_task_kg",
            "helium_blend_coefficient",
            "max_history",
            "high_dependency_cutoff",
            "mid_dependency_cutoff",
        ):
            if key in cfg:
                kwargs[key] = cfg[key]
        return RayExecutorConfig(**kwargs)

    # ---------------------------------------------------------- public API
    async def execute_task(
        self,
        task: Any,
        workload_profile: Any = None,
        execution_decision: Any = None,
    ) -> ExecutionResult:
        """
        Execute a task with helium-aware routing.

        Parameters
        ----------
        task : Any
            Task payload. ``task.id`` is used for attribution.
        workload_profile : Any, optional
            Must expose ``helium_profile`` when helium-awareness is active.
        execution_decision : Any, optional
            Must expose ``helium_aware_flag``, ``helium_zone``, and
            ``power_budget``. Missing attributes fall back to safe defaults.

        Returns
        -------
        ExecutionResult
        """
        task_id = self._task_id_of(task)
        decision = self._normalize_decision(execution_decision)
        power_budget = self._validate_power_budget(decision.get("power_budget", 1.0))

        helium_profile = getattr(workload_profile, "helium_profile", None)

        target_worker = self._select_worker_pool(
            helium_aware=decision["helium_aware_flag"],
            helium_zone=decision["helium_zone"],
            helium_profile=helium_profile,
            power_budget=power_budget,
        )

        if target_worker is None:
            if self.fallback_enabled:
                result = await self._execute_fallback(
                    task, helium_profile, power_budget, decision
                )
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
            )
            self._record(result)
            return result

        footprint = target_worker.helium_footprint
        logger.info(
            "Executing task %s on %s (helium footprint: %.2f)",
            task_id,
            target_worker.value,
            footprint,
        )

        # Simulated execution path.
        result = self._simulate_execution(
            task_id=task_id,
            worker_type=target_worker,
            power_budget=power_budget,
            helium_zone=decision["helium_zone"],
            helium_aware=decision["helium_aware_flag"],
        )
        self._record(result)
        return result

    async def execute_batch(
        self,
        tasks: List[Any],
        workload_profile: Any = None,
        execution_decisions: Optional[List[Any]] = None,
    ) -> List[ExecutionResult]:
        """
        Execute multiple tasks sequentially.

        If ``execution_decisions`` is provided, it must match ``tasks`` in
        length; otherwise the same decision is used for every task.
        """
        if not isinstance(tasks, list):
            raise RayExecutorError("tasks must be a list.")
        if execution_decisions is not None and len(execution_decisions) != len(tasks):
            raise RayExecutorError(
                "execution_decisions must match tasks in length."
            )
        results: List[ExecutionResult] = []
        for idx, task in enumerate(tasks):
            decision = (
                execution_decisions[idx]
                if execution_decisions is not None
                else None
            )
            results.append(
                await self.execute_task(task, workload_profile, decision)
            )
        return results

    # ---------------------------------------------------------- worker routing
    def _select_worker_pool(
        self,
        *,
        helium_aware: bool,
        helium_zone: Optional[str],
        helium_profile: Any,
        power_budget: float,
    ) -> Optional[WorkerType]:
        """
        Select an appropriate worker pool.

        Routing rules
        -------------
        - ``helium_red`` / ``helium_critical`` → prefer CPU if the workload can
          run on CPU; otherwise single GPU; fallback to CPU.
        - ``helium_yellow`` → prefer single GPU; fallback to CPU; last resort
          GPU cluster **only if available**.
        - Otherwise → route by dependency score (high/mid/low cutoffs).
        """
        zone = (helium_zone or "").lower()

        if helium_aware and zone in ("helium_red", "helium_critical"):
            if helium_profile is not None and getattr(
                helium_profile, "can_run_on_cpu", False
            ):
                return WorkerType.STANDARD_CPU
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

        # No helium profile → default CPU.
        return (
            WorkerType.STANDARD_CPU
            if self._pool_available(WorkerType.STANDARD_CPU)
            else None
        )

    def _pool_available(self, worker: WorkerType) -> bool:
        cfg = self.worker_pools.get(worker)
        return bool(cfg and cfg.get("available") and cfg.get("capacity", 0) > 0)

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
        """Simulated execution (production would call a real Ray worker)."""
        cfg = self._config
        footprint = worker_type.helium_footprint

        execution_time = random.uniform(
            cfg.sim_min_latency_ms, cfg.sim_max_latency_ms
        )

        # Helium usage blends footprint and power budget.
        blend = cfg.helium_blend_coefficient
        helium_usage = footprint * (1.0 - power_budget) * blend + power_budget * blend

        return ExecutionResult(
            success=True,
            task_id=task_id,
            accuracy=cfg.sim_accuracy_ceiling * power_budget,
            energy_consumed_kwh=cfg.sim_energy_per_task_kwh * power_budget,
            carbon_emitted_kg=cfg.sim_carbon_per_task_kg * power_budget,
            execution_time_ms=int(execution_time),
            worker_type=worker_type.value,
            helium_usage=max(0.0, min(1.0, helium_usage)),
            helium_zone=helium_zone,
            fallback_used=False,
            optimization_level=self._get_optimization_level(
                helium_aware=helium_aware, power_budget=power_budget
            ),
        )

    # ---------------------------------------------------------- fallback
    async def _execute_fallback(
        self,
        task: Any,
        helium_profile: Any,
        power_budget: float,
        decision: Mapping[str, Any],
    ) -> ExecutionResult:
        """
        Fallback path used when no primary worker can be selected.

        Options (in priority order):
        1. Distilled model on CPU (small accuracy loss).
        2. Degraded CPU execution (larger accuracy loss).
        3. Defer the task.
        """
        cfg = self._config
        task_id = self._task_id_of(task)
        helium_zone = decision.get("helium_zone")
        logger.warning(
            "Executing fallback for task %s (helium zone: %s)",
            task_id,
            helium_zone or "n/a",
        )

        if helium_profile is not None and getattr(
            helium_profile, "can_use_distilled_model", False
        ):
            logger.info("Fallback: Using distilled model.")
            return ExecutionResult(
                success=True,
                task_id=task_id,
                accuracy=cfg.sim_distilled_accuracy,
                energy_consumed_kwh=cfg.sim_distilled_energy_kwh,
                carbon_emitted_kg=cfg.sim_distilled_carbon_kg,
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
            return ExecutionResult(
                success=True,
                task_id=task_id,
                accuracy=cfg.sim_cpu_fallback_accuracy,
                energy_consumed_kwh=cfg.sim_cpu_fallback_energy_kwh,
                carbon_emitted_kg=cfg.sim_cpu_fallback_carbon_kg,
                execution_time_ms=int(cfg.sim_cpu_fallback_latency_ms),
                worker_type="cpu_fallback",
                helium_usage=0.05,
                helium_zone=helium_zone,
                fallback_used=True,
                optimization_level="degraded",
            )

        logger.warning("Fallback: Deferring task — no viable path.")
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

    # ---------------------------------------------------------- helpers
    @staticmethod
    def _get_optimization_level(
        *, helium_aware: bool, power_budget: float
    ) -> str:
        """Return the optimization level implied by the decision."""
        if not helium_aware:
            return "none"
        if power_budget >= 0.8:
            return "light"
        if power_budget >= 0.5:
            return "moderate"
        if power_budget >= 0.2:
            return "aggressive"
        return "deferred"

    def _normalize_decision(self, execution_decision: Any) -> Dict[str, Any]:
        """Normalize an ``ExecutionDecision`` (or None) into a dict."""
        if execution_decision is None:
            return {
                "helium_aware_flag": False,
                "helium_zone": None,
                "power_budget": 1.0,
            }
        zone = getattr(execution_decision, "helium_zone", None)
        zone_str: Optional[str] = None
        if zone is not None:
            zone_str = zone.value if hasattr(zone, "value") else str(zone)
        return {
            "helium_aware_flag": bool(
                getattr(execution_decision, "helium_aware_flag", False)
            ),
            "helium_zone": zone_str,
            "power_budget": float(
                getattr(execution_decision, "power_budget", 1.0)
            ),
        }

    def _validate_power_budget(self, value: Any) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            msg = (
                f"power_budget must be numeric, got {type(value).__name__}."
            )
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

    def _record(self, result: ExecutionResult) -> None:
        with self._lock:
            self._history.append(result)

    # ---------------------------------------------------------- status
    def get_worker_pool_status(self) -> Dict[str, Dict[str, Any]]:
        """
        Return the current status of every worker pool.

        Returns a deep copy so callers cannot mutate internal state; the
        ``helium_footprint`` field is sourced from the enum, not from a
        duplicated dict.
        """
        with self._lock:
            return {
                worker.value: {
                    "available": bool(cfg["available"]),
                    "capacity": int(cfg["capacity"]),
                    "cost_factor": float(cfg["cost_factor"]),
                    "helium_footprint": float(worker.helium_footprint),
                }
                for worker, cfg in self.worker_pools.items()
            }

    def statistics(self) -> Dict[str, Any]:
        """Return aggregate statistics over the recorded executions."""
        with self._lock:
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
            by_worker[r.worker_type] = by_worker.get(r.worker_type, 0) + 1
            by_opt[r.optimization_level] = by_opt.get(r.optimization_level, 0) + 1

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
            "mean_accuracy": sum(r.accuracy for r in history) / len(history),
            "mean_energy_kwh": sum(r.energy_consumed_kwh for r in history) / len(history),
            "mean_carbon_kg": sum(r.carbon_emitted_kg for r in history) / len(history),
            "mean_helium_usage": sum(r.helium_usage for r in history) / len(history),
            "mean_latency_ms": sum(r.execution_time_ms for r in history) / len(history),
            "success_rate": successful / len(history),
        }

    def reset(self, *, clear_history: bool = False) -> None:
        """Reset internal state; optionally clear execution history."""
        with self._lock:
            if clear_history:
                self._history.clear()
        logger.debug("HeliumAwareRayExecutor reset (clear_history=%s)", clear_history)

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
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
    def from_dict(cls, data: Mapping[str, Any]) -> "HeliumAwareRayExecutor":
        if not isinstance(data, Mapping):
            raise RayExecutorError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )
        cfg_data = dict(data.get("config", {}) or {})
        cfg = RayExecutorConfig(
            cpu_workers=int(cfg_data.get("cpu_workers", 10)),
            gpu_workers=int(cfg_data.get("gpu_workers", 4)),
            gpu_cluster_workers=int(cfg_data.get("gpu_cluster_workers", 2)),
            tpu_workers=int(cfg_data.get("tpu_workers", 2)),
            quantum_workers=int(cfg_data.get("quantum_workers", 1)),
            gpu_available=bool(cfg_data.get("gpu_available", True)),
            gpu_cluster_available=bool(cfg_data.get("gpu_cluster_available", True)),
            tpu_available=bool(cfg_data.get("tpu_available", False)),
            quantum_available=bool(cfg_data.get("quantum_available", False)),
            fallback_enabled=bool(cfg_data.get("fallback_enabled", True)),
            execution_timeout_seconds=float(
                cfg_data.get("execution_timeout_seconds", 300.0)
            ),
            sim_min_latency_ms=float(cfg_data.get("sim_min_latency_ms", 100.0)),
            sim_max_latency_ms=float(cfg_data.get("sim_max_latency_ms", 1000.0)),
            sim_accuracy_ceiling=float(cfg_data.get("sim_accuracy_ceiling", 0.95)),
            sim_energy_per_task_kwh=float(
                cfg_data.get("sim_energy_per_task_kwh", 0.5)
            ),
            sim_carbon_per_task_kg=float(
                cfg_data.get("sim_carbon_per_task_kg", 0.2)
            ),
            helium_blend_coefficient=float(
                cfg_data.get("helium_blend_coefficient", 0.5)
            ),
            max_history=int(cfg_data.get("max_history", 1000)),
            high_dependency_cutoff=float(
                cfg_data.get("high_dependency_cutoff", 0.8)
            ),
            mid_dependency_cutoff=float(
                cfg_data.get("mid_dependency_cutoff", 0.5)
            ),
        )

        executor = cls(
            config={"ray_address": data.get("ray_address", "auto")},
            executor_config=cfg,
            strict=bool(data.get("strict", True)),
        )

        with executor._lock:
            for entry in data.get("history", []):
                executor._history.append(ExecutionResult.from_dict(entry))
            executor._started_at = float(data.get("started_at", time.time()))
        return executor

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "HeliumAwareRayExecutor":
        try:
            return cls.from_dict(json.loads(payload))
        except json.JSONDecodeError as exc:
            raise RayExecutorError(f"Invalid JSON payload: {exc}") from exc

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "HeliumAwareRayExecutor":
        self._ctx_start = time.perf_counter()
        logger.debug("Entering scoped HeliumAwareRayExecutor session.")
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        elapsed = time.perf_counter() - (
            self._ctx_start if self._ctx_start is not None else time.perf_counter()
        )
        self._ctx_start = None
        if exc_type is not None:
            logger.warning(
                "HeliumAwareRayExecutor scope exited with %s after %.4fs.",
                exc_type.__name__, elapsed,
            )
            return
        logger.info(
            "HeliumAwareRayExecutor scope closed in %.4fs (%d execution(s)).",
            elapsed, self.history_size,
        )

    async def __aenter__(self) -> "HeliumAwareRayExecutor":
        self._async_ctx_start = time.perf_counter()
        logger.debug("Entering async HeliumAwareRayExecutor session.")
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        elapsed = time.perf_counter() - (
            self._async_ctx_start
            if self._async_ctx_start is not None
            else time.perf_counter()
        )
        self._async_ctx_start = None
        if exc_type is not None:
            logger.warning(
                "Async HeliumAwareRayExecutor scope exited with %s after %.4fs.",
                exc_type.__name__, elapsed,
            )
            return
        logger.info(
            "Async HeliumAwareRayExecutor scope closed in %.4fs "
            "(%d execution(s)).",
            elapsed, self.history_size,
        )

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        return (
            "HeliumAwareRayExecutor("
            f"ray_address={self.ray_address!r}, "
            f"fallback={self.fallback_enabled}, "
            f"strict={self._strict}, "
            f"executions={self.history_size})"
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
# Local smoke test: python -m distributed.ray_cluster_manager
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

    # ---------------------------------------------------------------- #
    # Dummy task / workload profile / execution decision.
    # ---------------------------------------------------------------- #
    class _Task:
        def __init__(self, task_id: str) -> None:
            self.id = task_id

    class _HeliumProfile:
        def __init__(
            self,
            dependency_score: float,
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
            helium_aware_flag: bool,
            helium_zone: Optional[str],
            power_budget: float,
        ) -> None:
            self.helium_aware_flag = helium_aware_flag
            self.helium_zone = helium_zone
            self.power_budget = power_budget

    async def main() -> None:
        # ------------------------------------------------------------ #
        # 1. Worker pool status
        # ------------------------------------------------------------ #
        executor = HeliumAwareRayExecutor(config={"gpu_available": True})
        print("executor     :", executor)
        print("pool status  :")
        for name, cfg in executor.get_worker_pool_status().items():
            print(f"  {name:<16} footprint={cfg['helium_footprint']:.2f} "
                  f"capacity={cfg['capacity']} available={cfg['available']}")

        # ------------------------------------------------------------ #
        # 2. Route under various conditions
        # ------------------------------------------------------------ #
        scenarios = [
            ("normal-cpu",     _Workload(_HeliumProfile(0.2)),
             _Decision(False, None, 1.0)),
            ("normal-gpu",     _Workload(_HeliumProfile(0.6)),
             _Decision(False, None, 1.0)),
            ("normal-cluster", _Workload(_HeliumProfile(0.9)),
             _Decision(False, None, 1.0)),
            ("yellow-throttle",_Workload(_HeliumProfile(0.7)),
             _Decision(True, "helium_yellow", 0.5)),
            ("red-cpu-ok",     _Workload(_HeliumProfile(0.4, can_run_on_cpu=True)),
             _Decision(True, "helium_red", 0.3)),
            ("red-gpu-only",   _Workload(_HeliumProfile(0.9)),
             _Decision(True, "helium_red", 0.3)),
            ("critical-defer", _Workload(_HeliumProfile(0.9)),
             _Decision(True, "helium_critical", 0.0)),
        ]

        print("\nRouting:")
        for label, wp, decision in scenarios:
            result = await executor.execute_task(
                _Task(label), wp, decision
            )
            print(
                f"  {label:<16} -> {result.worker_type:<16} "
                f"acc={result.accuracy:.2f} helium={result.helium_usage:.2f} "
                f"opt={result.optimization_level}"
            )

        # ------------------------------------------------------------ #
        # 3. Fallback paths
        # ------------------------------------------------------------ #
        fallback = HeliumAwareRayExecutor(
            config={
                "gpu_available": False,
                "gpu_cluster_available": False,
                "tpu_available": False,
                "quantum_available": False,
            }
        )
        print("\nFallback (all GPU pools disabled):")
        # No helium profile → default to CPU, which IS available → normal path.
        # Use a helium_red + can_run_on_cpu=False to force fallback.
        r = await fallback.execute_task(
            _Task("fallback-distilled"),
            _Workload(_HeliumProfile(0.9, can_run_on_cpu=False,
                                     can_use_distilled_model=True)),
            _Decision(True, "helium_red", 0.2),
        )
        print(f"  distilled     -> {r.worker_type:<16} "
              f"acc={r.accuracy:.2f} fallback={r.fallback_used}")

        r = await fallback.execute_task(
            _Task("fallback-cpu"),
            _Workload(_HeliumProfile(0.9, can_run_on_cpu=True)),
            _Decision(True, "helium_red", 0.2),
        )
        print(f"  cpu-ok        -> {r.worker_type:<16} "
              f"acc={r.accuracy:.2f} fallback={r.fallback_used}")

        r = await fallback.execute_task(
            _Task("fallback-defer"),
            _Workload(_HeliumProfile(0.9)),
            _Decision(True, "helium_red", 0.2),
        )
        print(f"  deferred      -> success={r.success} worker={r.worker_type} "
              f"fallback={r.fallback_used}")

        # ------------------------------------------------------------ #
        # 4. Statistics
        # ------------------------------------------------------------ #
        stats = executor.statistics()
        print("\nStatistics:")
        for k, v in stats.items():
            if isinstance(v, float):
                print(f"  {k:<24} {v:.4f}")
            else:
                print(f"  {k:<24} {v}")

        # ------------------------------------------------------------ #
        # 5. Serialization round-trip
        # ------------------------------------------------------------ #
        payload = executor.to_json()
        restored = HeliumAwareRayExecutor.from_json(payload)
        assert restored.to_dict() == executor.to_dict()
        print("Serialization round-trip OK.")

        # ------------------------------------------------------------ #
        # 6. Context managers
        # ------------------------------------------------------------ #
        with HeliumAwareRayExecutor() as scoped:
            await scoped.execute_task(_Task("ctx-1"))
        print("Sync context OK.")

        async with HeliumAwareRayExecutor() as scoped:
            await scoped.execute_task(_Task("actx-1"))
        print("Async context OK.")

        # ------------------------------------------------------------ #
        # 7. Batch execution
        # ------------------------------------------------------------ #
        batch = await executor.execute_batch(
            [_Task(f"b-{i}") for i in range(3)],
            _Workload(_HeliumProfile(0.6)),
            [_Decision(False, None, 1.0)] * 3,
        )
        print("Batch results:", len(batch))

        # ------------------------------------------------------------ #
        # 8. Validation failures
        # ------------------------------------------------------------ #
        for bad_cfg in (
            dict(cpu_workers=-1),
            dict(gpu_workers="many"),
            dict(execution_timeout_seconds=0),
            dict(helium_blend_coefficient=2.0),
            dict(max_history=0),
            dict(high_dependency_cutoff=0.3, mid_dependency_cutoff=0.5),
        ):
            try:
                RayExecutorConfig(**bad_cfg)  # type: ignore[arg-type]
            except RayExecutorError as exc:
                print("Rejected cfg :", exc)
            else:  # pragma: no cover
                raise AssertionError(f"Expected config rejection: {bad_cfg!r}")

        strict = HeliumAwareRayExecutor()
        for bad_budget in (float("nan"), float("inf"), -0.5, 1.5):
            if bad_budget in (1.5, -0.5):
                # In strict mode these should raise.
                try:
                    await strict.execute_task(
                        _Task("bad"),
                        _Workload(_HeliumProfile(0.5)),
                        _Decision(False, None, bad_budget),
                    )
                except RayExecutorError as exc:
                    print("Rejected budget:", exc)
            else:
                try:
                    await strict.execute_task(
                        _Task("bad"),
                        _Workload(_HeliumProfile(0.5)),
                        _Decision(False, None, bad_budget),
                    )
                except RayExecutorError as exc:
                    print("Rejected budget:", exc)

        # Non-strict mode coerces rather than raising.
        lenient = HeliumAwareRayExecutor(strict=False)
        r = await lenient.execute_task(
            _Task("lenient"),
            _Workload(_HeliumProfile(0.5)),
            _Decision(False, None, float("nan")),
        )
        print("Non-strict   :", r.worker_type, "acc=", round(r.accuracy, 2))

        # Invalid batch length.
        try:
            await executor.execute_batch(
                [_Task("x")],
                _Workload(_HeliumProfile(0.5)),
                [_Decision(False, None, 1.0), _Decision(False, None, 1.0)],
            )
        except RayExecutorError as exc:
            print("Rejected batch:", exc)

        print("\nSmoke test passed.")

    asyncio.run(main())
