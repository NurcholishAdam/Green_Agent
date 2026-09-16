# src/distributed/carbon_aware_scheduler.py

"""
Carbon-Aware Scheduler for Ray
==============================

Schedules Ray tasks to nodes with lowest carbon intensity.

Location: src/distributed/carbon_aware_scheduler.py

Original behaviour preserved
----------------------------
- ``CarbonAwareScheduler(carbon_forecaster=None, update_interval_minutes=15)``.
- ``await update_node_carbon_info()`` — refresh node carbon landscape.
- ``await schedule_task(task, task_energy_estimate_kwh)`` — pick a node.
- ``await batch_schedule(tasks, task_energy_estimate_kwh)`` — pick many.
- ``await should_defer_task(task, defer_threshold_gco2kwh)`` — defer policy.
- ``get_statistics()`` / ``get_current_carbon_landscape()``.
- ``CarbonAwareRayCluster(cluster_manager, carbon_forecaster)``.
- ``create_carbon_aware_cluster(num_workers, carbon_forecaster)``.

Enhancements
------------
- Fixed ``carbon_saved_percent`` logical error + ``ZeroDivisionError`` guard.
- Fixed ``should_defer_task`` returning True when no nodes are known.
- Optional Ray import with graceful standalone fallback (``@_ray_remote`` no-op).
- Configurable via :class:`CarbonSchedulerConfig`.
- Thread-safe via ``RLock``; bounded history via ``deque(maxlen=...)``.
- UTC timestamps throughout (``datetime.now(timezone.utc)``).
- Full validation; strict / non-strict modes.
- Immutable :class:`SchedulingDecision` with full serialization.
- :class:`CarbonAwareRayCluster` works with or without Ray installed.
- ``create_carbon_aware_cluster`` uses lazy import + defensive fallback.
- Sync and async context managers; ``statistics()`` alias; ``__repr__``.
- Custom :class:`CarbonSchedulerError`; lazy ``%s`` logging.
- ``__main__`` smoke test exercises scheduling, defer policy, stats,
  serialization, and validation without requiring a Ray cluster.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Awaitable, Callable, Deque, Dict, List, Mapping, Optional, Tuple

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Optional Ray integration
# --------------------------------------------------------------------------- #
try:  # pragma: no cover — environment-dependent
    import ray  # type: ignore
    from ray.util.scheduling_strategies import (  # type: ignore
        PlacementGroupSchedulingStrategy,
    )

    _RAY_AVAILABLE = True

    def _ray_remote(cls):
        """Wrap ``cls`` with ``@ray.remote`` when Ray is available."""
        return ray.remote(cls)

except ImportError:  # pragma: no cover
    ray = None  # type: ignore[assignment]
    PlacementGroupSchedulingStrategy = None  # type: ignore[assignment]
    _RAY_AVAILABLE = False

    def _ray_remote(cls):
        """No-op fallback when Ray is not installed."""
        return cls


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class CarbonSchedulerError(ValueError):
    """Raised for invalid inputs, configuration, or scheduling failures."""


# --------------------------------------------------------------------------- #
# Enums
# --------------------------------------------------------------------------- #
class NodeCarbonMode(Enum):
    """Carbon intensity classification for nodes."""

    CLEAN = "clean"        # < 200 gCO2/kWh
    MODERATE = "moderate"  # 200–400 gCO2/kWh
    DIRTY = "dirty"        # > 400 gCO2/kWh


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class CarbonSchedulerConfig:
    """
    Tunable parameters for the carbon-aware scheduler.

    Centralizes thresholds, defaults, and buffer sizes so deployments can
    calibrate without editing the class.
    """

    # Carbon mode thresholds (gCO2/kWh).
    clean_threshold: float = 200.0
    moderate_threshold: float = 400.0

    # Default carbon intensity when no forecaster is available.
    default_carbon_intensity: float = 400.0

    # Default defer policy threshold (gCO2/kWh).
    default_defer_threshold: float = 500.0

    # Node refresh cadence.
    update_interval_minutes: int = 15

    # Bounded scheduling history.
    max_history: int = 10_000

    # Node info TTL — entries older than this are dropped on refresh.
    node_ttl_seconds: float = 3600.0

    def __post_init__(self) -> None:
        if self.clean_threshold <= 0:
            raise CarbonSchedulerError("clean_threshold must be > 0.")
        if self.moderate_threshold <= self.clean_threshold:
            raise CarbonSchedulerError(
                "moderate_threshold must be > clean_threshold."
            )
        if self.default_carbon_intensity < 0:
            raise CarbonSchedulerError(
                "default_carbon_intensity must be >= 0."
            )
        if self.default_defer_threshold <= 0:
            raise CarbonSchedulerError(
                "default_defer_threshold must be > 0."
            )
        if self.update_interval_minutes <= 0:
            raise CarbonSchedulerError(
                "update_interval_minutes must be > 0."
            )
        if self.max_history <= 0:
            raise CarbonSchedulerError("max_history must be > 0.")
        if self.node_ttl_seconds <= 0:
            raise CarbonSchedulerError("node_ttl_seconds must be > 0.")

    def classify(self, carbon_intensity: float) -> NodeCarbonMode:
        """Return the :class:`NodeCarbonMode` for ``carbon_intensity``."""
        if carbon_intensity < self.clean_threshold:
            return NodeCarbonMode.CLEAN
        if carbon_intensity < self.moderate_threshold:
            return NodeCarbonMode.MODERATE
        return NodeCarbonMode.DIRTY


# --------------------------------------------------------------------------- #
# Data containers
# --------------------------------------------------------------------------- #
@dataclass
class NodeInfo:
    """Information about a Ray node."""

    node_id: str
    region: str
    carbon_intensity: float             # gCO2/kWh
    carbon_mode: NodeCarbonMode
    available_cpus: int
    available_gpus: int
    current_load: float                 # 0–1
    last_updated: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def __post_init__(self) -> None:
        if not isinstance(self.node_id, str) or not self.node_id:
            raise CarbonSchedulerError("node_id must be a non-empty string.")
        if self.carbon_intensity < 0:
            raise CarbonSchedulerError("carbon_intensity must be >= 0.")
        if not 0.0 <= self.current_load <= 1.0:
            raise CarbonSchedulerError("current_load must be in [0, 1].")
        if not isinstance(self.last_updated, datetime):
            raise CarbonSchedulerError("last_updated must be a datetime.")
        if not self.last_updated.tzinfo:
            # Normalize naive timestamps to UTC.
            self.last_updated = self.last_updated.replace(tzinfo=timezone.utc)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "node_id": self.node_id,
            "region": self.region,
            "carbon_intensity": self.carbon_intensity,
            "carbon_mode": self.carbon_mode.value,
            "available_cpus": self.available_cpus,
            "available_gpus": self.available_gpus,
            "current_load": self.current_load,
            "last_updated": self.last_updated.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "NodeInfo":
        if not isinstance(data, Mapping):
            raise CarbonSchedulerError(
                f"NodeInfo.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        ts = data.get("last_updated")
        if isinstance(ts, str):
            timestamp = datetime.fromisoformat(ts)
            if not timestamp.tzinfo:
                timestamp = timestamp.replace(tzinfo=timezone.utc)
        elif isinstance(ts, datetime):
            timestamp = ts
        else:
            timestamp = datetime.now(timezone.utc)
        return cls(
            node_id=str(data["node_id"]),
            region=str(data.get("region", "unknown")),
            carbon_intensity=float(data["carbon_intensity"]),
            carbon_mode=NodeCarbonMode(str(data["carbon_mode"])),
            available_cpus=int(data.get("available_cpus", 0)),
            available_gpus=int(data.get("available_gpus", 0)),
            current_load=float(data.get("current_load", 0.0)),
            last_updated=timestamp,
        )


@dataclass(frozen=True)
class SchedulingDecision:
    """Immutable result of a scheduling decision."""

    node_id: str
    node_region: str
    carbon_intensity: float
    reason: str
    estimated_carbon_kgco2e: float
    baseline_carbon_kgco2e: float = 0.0
    carbon_saved_kgco2e: float = 0.0
    task_id: Optional[str] = None
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def __post_init__(self) -> None:
        if not isinstance(self.node_id, str) or not self.node_id:
            raise CarbonSchedulerError("node_id must be a non-empty string.")
        for name in (
            "carbon_intensity",
            "estimated_carbon_kgco2e",
            "baseline_carbon_kgco2e",
        ):
            value = getattr(self, name)
            if not isinstance(value, (int, float)) or math.isnan(value) or math.isinf(value):
                raise CarbonSchedulerError(f"{name} must be a finite number.")
        if self.carbon_intensity < 0:
            raise CarbonSchedulerError("carbon_intensity must be >= 0.")
        if self.estimated_carbon_kgco2e < 0:
            raise CarbonSchedulerError(
                "estimated_carbon_kgco2e must be >= 0."
            )
        if self.baseline_carbon_kgco2e < 0:
            raise CarbonSchedulerError(
                "baseline_carbon_kgco2e must be >= 0."
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            **asdict(self),
            "timestamp": self.timestamp.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "SchedulingDecision":
        if not isinstance(data, Mapping):
            raise CarbonSchedulerError(
                f"SchedulingDecision.from_dict expects a Mapping, "
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
            node_id=str(data["node_id"]),
            node_region=str(data.get("node_region", "unknown")),
            carbon_intensity=float(data["carbon_intensity"]),
            reason=str(data.get("reason", "")),
            estimated_carbon_kgco2e=float(data["estimated_carbon_kgco2e"]),
            baseline_carbon_kgco2e=float(data.get("baseline_carbon_kgco2e", 0.0)),
            carbon_saved_kgco2e=float(data.get("carbon_saved_kgco2e", 0.0)),
            task_id=data.get("task_id"),
            timestamp=timestamp,
        )


# --------------------------------------------------------------------------- #
# Scheduler
# --------------------------------------------------------------------------- #
@_ray_remote
class CarbonAwareScheduler:
    """
    Schedules tasks to Ray nodes based on carbon intensity.

    Strategy
    --------
    1. Prefer nodes with lowest carbon intensity.
    2. Within the same carbon mode, prefer least-loaded.
    3. If all nodes are DIRTY, defer tasks when possible.
    4. Track carbon savings vs. baseline (average-across-nodes) scheduling.

    The class is Ray-deployable but fully usable standalone. All original
    public methods are preserved; new parameters are keyword-only.
    """

    # ------------------------------------------------------------------ init
    def __init__(
        self,
        carbon_forecaster: Optional[Any] = None,
        update_interval_minutes: int = 15,
        *,
        config: Optional[CarbonSchedulerConfig] = None,
        strict: bool = True,
        node_provider: Optional[Callable[[], List[Mapping[str, Any]]]] = None,
    ) -> None:
        """
        Parameters
        ----------
        carbon_forecaster : Any, optional
            Object exposing ``get_current_intensity(region)`` (sync or async).
        update_interval_minutes : int
            Legacy positional; folded into :class:`CarbonSchedulerConfig`
            when ``config`` is not supplied.
        config : CarbonSchedulerConfig, optional
            Extended configuration. Takes precedence over
            ``update_interval_minutes``.
        strict : bool, default True
            If True, invalid inputs raise :class:`CarbonSchedulerError`.
        node_provider : callable, optional
            Zero-arg callable returning node metadata (``node_id``, ``region``,
            ``Resources``). Defaults to ``ray.nodes()`` when Ray is available.
        """
        if not isinstance(update_interval_minutes, int) or update_interval_minutes <= 0:
            raise CarbonSchedulerError(
                "update_interval_minutes must be a positive int."
            )

        if config is not None:
            self._config = config
        else:
            self._config = CarbonSchedulerConfig(
                update_interval_minutes=update_interval_minutes
            )

        self.carbon_forecaster = carbon_forecaster
        self._strict: bool = bool(strict)

        # Legacy attribute preserved for backward compatibility.
        self.update_interval = timedelta(
            minutes=self._config.update_interval_minutes
        )

        self._lock = threading.RLock()
        self.node_info: Dict[str, NodeInfo] = {}
        self.last_update: Optional[datetime] = None
        self._scheduling_history: Deque[SchedulingDecision] = deque(
            maxlen=self._config.max_history
        )

        # Counters used by ``get_statistics`` (kept numeric for backwards compat).
        self.total_tasks_scheduled: int = 0
        self.carbon_saved_kgco2e: float = 0.0
        # NEW: baseline emissions total — required for a correct savings
        # percentage. Previously mis-computed by the original code.
        self._baseline_kgco2e_total: float = 0.0

        # Optional provider hook.
        self._node_provider = node_provider

        self._ctx_start: Optional[float] = None
        self._async_ctx_start: Optional[float] = None

        logger.debug(
            "CarbonAwareScheduler initialized "
            "(update_interval=%dm, strict=%s, ray=%s)",
            self._config.update_interval_minutes,
            self._strict,
            _RAY_AVAILABLE,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> CarbonSchedulerConfig:
        return self._config

    @property
    def scheduling_history(self) -> List[SchedulingDecision]:
        with self._lock:
            return list(self._scheduling_history)

    @property
    def node_count(self) -> int:
        with self._lock:
            return len(self.node_info)

    # ---------------------------------------------------------- node refresh
    async def update_node_carbon_info(self) -> int:
        """
        Refresh the carbon-intensity landscape for all known nodes.

        Returns the number of node records updated.

        Node metadata comes from:
        1. ``self._node_provider()`` if supplied.
        2. ``ray.nodes()`` when Ray is available.
        3. An empty list otherwise (no nodes).
        """
        raw_nodes = self._fetch_raw_nodes()
        cfg = self._config
        now = datetime.now(timezone.utc)
        updated = 0

        for node in raw_nodes:
            if not isinstance(node, Mapping):
                continue
            if node.get("Alive") is False:
                continue

            node_id = node.get("NodeID") or node.get("node_id")
            if not isinstance(node_id, str) or not node_id:
                continue

            region = (
                node.get("Region")
                or node.get("region")
                or node.get("NodeManagerAddress")
                or "US-CA"
            )
            region = str(region)

            carbon_intensity = await self._safe_get_intensity(region)

            resources = node.get("Resources") or node.get("resources") or {}
            try:
                available_cpus = int(resources.get("CPU", 0))
                available_gpus = int(resources.get("GPU", 0))
            except (TypeError, ValueError):
                available_cpus = 0
                available_gpus = 0

            # Best-effort load estimate.
            current_load = self._estimate_load(node, available_cpus)

            info = NodeInfo(
                node_id=node_id,
                region=region,
                carbon_intensity=float(carbon_intensity),
                carbon_mode=cfg.classify(float(carbon_intensity)),
                available_cpus=available_cpus,
                available_gpus=available_gpus,
                current_load=current_load,
                last_updated=now,
            )

            with self._lock:
                self.node_info[node_id] = info
            updated += 1

        # Purge stale entries (past TTL).
        with self._lock:
            stale_ids = [
                nid for nid, info in self.node_info.items()
                if (now - info.last_updated).total_seconds()
                > cfg.node_ttl_seconds
            ]
            for nid in stale_ids:
                self.node_info.pop(nid, None)

        with self._lock:
            self.last_update = now

        logger.info(
            "Updated carbon info: %d node(s) refreshed, %d stale purged "
            "(total=%d)",
            updated,
            len(stale_ids),
            len(self.node_info),
        )
        return updated

    def _fetch_raw_nodes(self) -> List[Mapping[str, Any]]:
        """Return raw node metadata from the provider or Ray."""
        if self._node_provider is not None:
            try:
                result = self._node_provider()
                if asyncio.iscoroutine(result):
                    # Cannot await from a sync context; drain it via loop.
                    try:
                        loop = asyncio.get_event_loop()
                        result = loop.run_until_complete(result)
                    except RuntimeError:
                        logger.warning(
                            "node_provider returned a coroutine but no loop "
                            "is available; ignoring."
                        )
                        return []
                return [n for n in result if isinstance(n, Mapping)]
            except Exception as exc:
                logger.warning("node_provider failed: %s", exc)
                return []

        if _RAY_AVAILABLE and ray is not None:
            try:
                nodes = ray.nodes()
                return [n for n in nodes if isinstance(n, Mapping)]
            except Exception as exc:
                logger.warning("ray.nodes() failed: %s", exc)
                return []

        return []

    @staticmethod
    def _estimate_load(node: Mapping[str, Any], available_cpus: int) -> float:
        """Best-effort load estimate in [0, 1]."""
        used = node.get("UsedCPU") or node.get("used_cpu")
        if used is None:
            # No live utilization data; assume idle.
            return 0.0
        try:
            used_f = float(used)
        except (TypeError, ValueError):
            return 0.0
        total = available_cpus + used_f
        if total <= 0:
            return 0.0
        return max(0.0, min(1.0, used_f / total))

    async def _safe_get_intensity(self, region: str) -> float:
        """Fetch carbon intensity for ``region`` with sync/async support."""
        if self.carbon_forecaster is None:
            return self._config.default_carbon_intensity

        method = getattr(self.carbon_forecaster, "get_current_intensity", None)
        if not callable(method):
            return self._config.default_carbon_intensity

        try:
            result = method(region)
            if asyncio.iscoroutine(result):
                result = await result
        except Exception as exc:
            logger.warning(
                "Carbon forecaster failed for region %r: %s", region, exc
            )
            return self._config.default_carbon_intensity

        try:
            value = float(result)
        except (TypeError, ValueError):
            logger.warning(
                "Carbon forecaster returned non-numeric value %r; using "
                "default.", result,
            )
            return self._config.default_carbon_intensity

        if math.isnan(value) or math.isinf(value) or value < 0:
            logger.warning(
                "Carbon forecaster returned invalid value %r; using "
                "default.", value,
            )
            return self._config.default_carbon_intensity
        return value

    # ---------------------------------------------------------- scheduling
    async def schedule_task(
        self,
        task: Mapping[str, Any],
        task_energy_estimate_kwh: float = 0.001,
    ) -> SchedulingDecision:
        """
        Schedule a task to the optimal node based on carbon intensity.

        Parameters
        ----------
        task : Mapping
            Task payload. Recognized optional keys: ``task_id``,
            ``deferrable``, ``deadline``.
        task_energy_estimate_kwh : float
            Estimated energy consumption. Must be finite and >= 0.

        Returns
        -------
        SchedulingDecision
        """
        if not isinstance(task, Mapping):
            raise CarbonSchedulerError(
                f"task must be a Mapping, got {type(task).__name__}."
            )
        energy = self._validate_energy(task_energy_estimate_kwh)

        await self._maybe_refresh()

        with self._lock:
            available_nodes = [
                n for n in self.node_info.values()
                if n.available_cpus > 0
            ]

        if not available_nodes:
            raise CarbonSchedulerError(
                "No available nodes for scheduling."
            )

        # Prefer lowest intensity, tie-break on least loaded.
        sorted_nodes = sorted(
            available_nodes,
            key=lambda n: (n.carbon_intensity, n.current_load),
        )
        best_node = sorted_nodes[0]

        # Baseline = average carbon intensity across all available nodes.
        avg_intensity = (
            sum(n.carbon_intensity for n in available_nodes)
            / len(available_nodes)
        )
        baseline_kg = energy * avg_intensity / 1000.0
        actual_kg = energy * best_node.carbon_intensity / 1000.0
        saved_kg = max(0.0, baseline_kg - actual_kg)

        task_id = task.get("task_id")
        if task_id is not None and not isinstance(task_id, str):
            task_id = str(task_id)

        decision = SchedulingDecision(
            node_id=best_node.node_id,
            node_region=best_node.region,
            carbon_intensity=best_node.carbon_intensity,
            reason=(
                f"Lowest carbon intensity ({best_node.carbon_mode.value}, "
                f"{best_node.carbon_intensity:.1f} gCO2/kWh)"
            ),
            estimated_carbon_kgco2e=actual_kg,
            baseline_carbon_kgco2e=baseline_kg,
            carbon_saved_kgco2e=saved_kg,
            task_id=task_id,
        )

        with self._lock:
            self._scheduling_history.append(decision)
            self.total_tasks_scheduled += 1
            self.carbon_saved_kgco2e += saved_kg
            self._baseline_kgco2e_total += baseline_kg

        logger.info(
            "Scheduled %s to node %s (carbon=%.1f gCO2/kWh, saved=%.3f gCO2e)",
            task_id or "<unnamed>",
            best_node.node_id[:8],
            best_node.carbon_intensity,
            saved_kg * 1000.0,
        )
        return decision

    async def batch_schedule(
        self,
        tasks: List[Mapping[str, Any]],
        task_energy_estimate_kwh: float = 0.001,
    ) -> List[SchedulingDecision]:
        """Schedule multiple tasks sequentially (preserves original semantics)."""
        if not isinstance(tasks, list):
            raise CarbonSchedulerError("tasks must be a list.")
        energy = self._validate_energy(task_energy_estimate_kwh)

        decisions: List[SchedulingDecision] = []
        for task in tasks:
            decisions.append(
                await self.schedule_task(task, energy)
            )
        return decisions

    async def should_defer_task(
        self,
        task: Mapping[str, Any],
        defer_threshold_gco2kwh: Optional[float] = None,
    ) -> bool:
        """
        Return True if the task should be deferred to a cleaner time window.

        Rules
        -----
        - Refresh node info if empty.
        - If **no nodes are known**, defer is **not** recommended (returns
          ``False``). (Fixes an original bug where an empty node set made
          ``all([])`` evaluate to True and erroneously defer everything.)
        - If all nodes exceed ``defer_threshold_gco2kwh`` **and** the task is
          deferrable **and** has no deadline, return True.
        """
        if not isinstance(task, Mapping):
            raise CarbonSchedulerError(
                f"task must be a Mapping, got {type(task).__name__}."
            )

        threshold = defer_threshold_gco2kwh
        if threshold is None:
            threshold = self._config.default_defer_threshold
        if not isinstance(threshold, (int, float)) or math.isnan(threshold) or math.isinf(threshold):
            raise CarbonSchedulerError(
                "defer_threshold_gco2kwh must be a finite number."
            )
        if threshold < 0:
            raise CarbonSchedulerError(
                "defer_threshold_gco2kwh must be >= 0."
            )

        with self._lock:
            has_nodes = bool(self.node_info)
        if not has_nodes:
            await self.update_node_carbon_info()

        with self._lock:
            nodes = list(self.node_info.values())

        # Explicit empty check — the original `all([])` bug lives here.
        if not nodes:
            logger.debug("No node info available; not deferring.")
            return False

        all_dirty = all(
            node.carbon_intensity > threshold for node in nodes
        )
        is_deferrable = bool(task.get("deferrable", True))
        has_deadline = "deadline" in task

        if all_dirty and is_deferrable and not has_deadline:
            logger.info(
                "Task %s deferred — all nodes above %.0f gCO2/kWh.",
                task.get("task_id", "<unnamed>"),
                threshold,
            )
            return True
        return False

    # ---------------------------------------------------------- stats
    def get_statistics(self) -> Dict[str, Any]:
        """Return scheduler statistics (backward-compatible)."""
        with self._lock:
            history = list(self._scheduling_history)
            total_tasks = self.total_tasks_scheduled
            saved = self.carbon_saved_kgco2e
            baseline_total = self._baseline_kgco2e_total

        if not history:
            return {
                "total_tasks_scheduled": 0,
                "carbon_saved_kgco2e": 0.0,
                "carbon_saved_percent": 0.0,
                "avg_carbon_intensity": 0.0,
                "node_utilization": {},
            }

        # Correct savings percentage: savings / baseline, with a guard.
        if baseline_total > 0:
            saved_percent = (saved / baseline_total) * 100.0
        else:
            saved_percent = 0.0

        avg_intensity = (
            sum(d.carbon_intensity for d in history) / len(history)
        )

        return {
            "total_tasks_scheduled": total_tasks,
            "carbon_saved_kgco2e": saved,
            "carbon_saved_percent": saved_percent,
            "avg_carbon_intensity": avg_intensity,
            "node_utilization": self._get_node_utilization(),
            "baseline_carbon_kgco2e": baseline_total,
        }

    def statistics(self) -> Dict[str, Any]:
        """Alias for :meth:`get_statistics` (consistent with other modules)."""
        return self.get_statistics()

    def _get_node_utilization(self) -> Dict[str, int]:
        """Return the count of decisions per node."""
        with self._lock:
            history = list(self._scheduling_history)
        utilization: Dict[str, int] = {}
        for decision in history:
            utilization[decision.node_id] = (
                utilization.get(decision.node_id, 0) + 1
            )
        return utilization

    def get_current_carbon_landscape(self) -> Dict[str, Any]:
        """Return the current carbon-intensity landscape across all nodes."""
        with self._lock:
            nodes = list(self.node_info.values())

        if not nodes:
            return {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "total_nodes": 0,
                "nodes_by_carbon_mode": {"clean": 0, "moderate": 0, "dirty": 0},
                "nodes": {"clean": [], "moderate": [], "dirty": []},
                "avg_carbon_intensity": 0.0,
            }

        nodes_by_mode: Dict[str, List[Dict[str, Any]]] = {
            "clean": [], "moderate": [], "dirty": [],
        }
        for node in nodes:
            nodes_by_mode[node.carbon_mode.value].append(
                {
                    "node_id": node.node_id[:8],
                    "region": node.region,
                    "carbon_intensity": node.carbon_intensity,
                    "available_cpus": node.available_cpus,
                    "current_load": node.current_load,
                }
            )

        avg = sum(n.carbon_intensity for n in nodes) / len(nodes)
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "total_nodes": len(nodes),
            "nodes_by_carbon_mode": {
                mode: len(items) for mode, items in nodes_by_mode.items()
            },
            "nodes": nodes_by_mode,
            "avg_carbon_intensity": avg,
        }

    # ---------------------------------------------------------- lifecycle
    async def _maybe_refresh(self) -> None:
        """Refresh node info if the last update is older than the interval."""
        with self._lock:
            last = self.last_update
        if last is None or (
            datetime.now(timezone.utc) - last > self.update_interval
        ):
            await self.update_node_carbon_info()

    def reset(self, *, clear_history: bool = False) -> None:
        """Reset internal state; optionally clear scheduling history."""
        with self._lock:
            if clear_history:
                self._scheduling_history.clear()
            self.node_info.clear()
            self.last_update = None
            self.total_tasks_scheduled = 0
            self.carbon_saved_kgco2e = 0.0
            self._baseline_kgco2e_total = 0.0
        logger.debug("CarbonAwareScheduler reset (clear_history=%s)", clear_history)

    # ---------------------------------------------------------- validation
    def _validate_energy(self, value: Any) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise CarbonSchedulerError(
                f"task_energy_estimate_kwh must be numeric, "
                f"got {type(value).__name__}."
            )
        fvalue = float(value)
        if math.isnan(fvalue) or math.isinf(fvalue):
            raise CarbonSchedulerError(
                f"task_energy_estimate_kwh must be finite, got {value!r}."
            )
        if fvalue < 0:
            raise CarbonSchedulerError(
                f"task_energy_estimate_kwh must be >= 0, got {fvalue}."
            )
        return fvalue

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "config": asdict(self._config),
                "strict": self._strict,
                "ray_available": _RAY_AVAILABLE,
                "last_update": (
                    self.last_update.isoformat() if self.last_update else None
                ),
                "counters": {
                    "total_tasks_scheduled": self.total_tasks_scheduled,
                    "carbon_saved_kgco2e": self.carbon_saved_kgco2e,
                    "baseline_kgco2e_total": self._baseline_kgco2e_total,
                },
                "nodes": [n.to_dict() for n in self.node_info.values()],
                "history": [d.to_dict() for d in self._scheduling_history],
            }

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        carbon_forecaster: Optional[Any] = None,
    ) -> "CarbonAwareScheduler":
        if not isinstance(data, Mapping):
            raise CarbonSchedulerError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )
        cfg_data = dict(data.get("config", {}) or {})
        cfg = CarbonSchedulerConfig(
            clean_threshold=float(cfg_data.get("clean_threshold", 200.0)),
            moderate_threshold=float(cfg_data.get("moderate_threshold", 400.0)),
            default_carbon_intensity=float(
                cfg_data.get("default_carbon_intensity", 400.0)
            ),
            default_defer_threshold=float(
                cfg_data.get("default_defer_threshold", 500.0)
            ),
            update_interval_minutes=int(
                cfg_data.get("update_interval_minutes", 15)
            ),
            max_history=int(cfg_data.get("max_history", 10_000)),
            node_ttl_seconds=float(cfg_data.get("node_ttl_seconds", 3600.0)),
        )

        scheduler = cls(
            carbon_forecaster=carbon_forecaster,
            config=cfg,
            strict=bool(data.get("strict", True)),
        )

        with scheduler._lock:
            for n in data.get("nodes", []):
                info = NodeInfo.from_dict(n)
                scheduler.node_info[info.node_id] = info
            for d in data.get("history", []):
                scheduler._scheduling_history.append(
                    SchedulingDecision.from_dict(d)
                )
            counters = dict(data.get("counters", {}) or {})
            scheduler.total_tasks_scheduled = int(
                counters.get("total_tasks_scheduled", 0)
            )
            scheduler.carbon_saved_kgco2e = float(
                counters.get("carbon_saved_kgco2e", 0.0)
            )
            scheduler._baseline_kgco2e_total = float(
                counters.get("baseline_kgco2e_total", 0.0)
            )
            last = data.get("last_update")
            if isinstance(last, str):
                timestamp = datetime.fromisoformat(last)
                if not timestamp.tzinfo:
                    timestamp = timestamp.replace(tzinfo=timezone.utc)
                scheduler.last_update = timestamp
        return scheduler

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(
        cls,
        payload: str,
        *,
        carbon_forecaster: Optional[Any] = None,
    ) -> "CarbonAwareScheduler":
        try:
            return cls.from_dict(
                json.loads(payload), carbon_forecaster=carbon_forecaster
            )
        except json.JSONDecodeError as exc:
            raise CarbonSchedulerError(f"Invalid JSON payload: {exc}") from exc

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "CarbonAwareScheduler":
        self._ctx_start = time.perf_counter()
        logger.debug("Entering scoped CarbonAwareScheduler session.")
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        elapsed = time.perf_counter() - (
            self._ctx_start if self._ctx_start is not None else time.perf_counter()
        )
        self._ctx_start = None
        if exc_type is not None:
            logger.warning(
                "CarbonAwareScheduler scope exited with %s after %.4fs.",
                exc_type.__name__, elapsed,
            )
            return
        logger.info(
            "CarbonAwareScheduler scope closed in %.4fs (%d task(s)).",
            elapsed, self.total_tasks_scheduled,
        )

    async def __aenter__(self) -> "CarbonAwareScheduler":
        self._async_ctx_start = time.perf_counter()
        logger.debug("Entering async CarbonAwareScheduler session.")
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
                "Async CarbonAwareScheduler scope exited with %s after %.4fs.",
                exc_type.__name__, elapsed,
            )
            return
        logger.info(
            "Async CarbonAwareScheduler scope closed in %.4fs (%d task(s)).",
            elapsed, self.total_tasks_scheduled,
        )

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        with self._lock:
            return (
                "CarbonAwareScheduler("
                f"nodes={len(self.node_info)}, "
                f"tasks={self.total_tasks_scheduled}, "
                f"saved_kg={self.carbon_saved_kgco2e:.4f}, "
                f"strict={self._strict}, "
                f"ray={_RAY_AVAILABLE})"
            )


# --------------------------------------------------------------------------- #
# Cluster wrapper
# --------------------------------------------------------------------------- #
class CarbonAwareRayCluster:
    """
    Ray cluster with integrated carbon-aware scheduling.

    Wraps a :class:`CarbonAwareScheduler` and a cluster manager. Works with or
    without Ray installed:

    - When Ray is available, the scheduler is deployed as a Ray actor and
      invoked via ``ray.get(actor.method.remote(...))``.
    - Otherwise the scheduler runs in-process and calls are ``await``-ed
      directly.
    """

    def __init__(
        self,
        cluster_manager: Any,
        carbon_forecaster: Optional[Any] = None,
        *,
        scheduler: Optional[CarbonAwareScheduler] = None,
        strict: bool = True,
    ) -> None:
        self.cluster_manager = cluster_manager
        self._strict = bool(strict)

        if scheduler is not None:
            self._local_scheduler = scheduler
            self.scheduler_actor = scheduler
            self._scheduler_is_ray_actor = False
        elif _RAY_AVAILABLE and ray is not None:
            try:
                self.scheduler_actor = CarbonAwareScheduler.remote(  # type: ignore[attr-defined]
                    carbon_forecaster=carbon_forecaster
                )
                self._scheduler_is_ray_actor = True
                self._local_scheduler = None
            except Exception as exc:  # pragma: no cover — env-dependent
                logger.warning(
                    "Ray actor creation failed (%s); falling back to "
                    "in-process scheduler.", exc,
                )
                self._local_scheduler = CarbonAwareScheduler(
                    carbon_forecaster=carbon_forecaster, strict=strict
                )
                self.scheduler_actor = self._local_scheduler
                self._scheduler_is_ray_actor = False
        else:
            self._local_scheduler = CarbonAwareScheduler(
                carbon_forecaster=carbon_forecaster, strict=strict
            )
            self.scheduler_actor = self._local_scheduler
            self._scheduler_is_ray_actor = False

    # ---------------------------------------------------------- internal
    async def _call_scheduler(
        self, method_name: str, **kwargs: Any
    ) -> Any:
        """Invoke a scheduler method regardless of Ray availability."""
        if self._scheduler_is_ray_actor and _RAY_AVAILABLE and ray is not None:
            method = getattr(self.scheduler_actor, method_name)
            ref = method.remote(**kwargs)
            return await asyncio.to_thread(ray.get, ref)

        method = getattr(self.scheduler_actor, method_name)
        result = method(**kwargs)
        if asyncio.iscoroutine(result):
            return await result
        return result

    # ---------------------------------------------------------- public API
    async def schedule_and_execute(
        self,
        tasks: List[Mapping[str, Any]],
        agent_type: str = "retriever",
        task_energy_estimate_kwh: float = 0.001,
    ) -> List[Dict[str, Any]]:
        """
        Schedule tasks with carbon awareness and execute them via the cluster
        manager.

        The cluster manager must expose
        ``execute_distributed_tasks(tasks, agent_type)``. If it does not, an
        empty result list is returned and the decisions are still recorded.
        """
        if not isinstance(tasks, list):
            raise CarbonSchedulerError("tasks must be a list.")

        decisions = await self._call_scheduler(
            "batch_schedule",
            tasks=tasks,
            task_energy_estimate_kwh=task_energy_estimate_kwh,
        )

        exec_method = getattr(
            self.cluster_manager, "execute_distributed_tasks", None
        )
        if callable(exec_method):
            try:
                result = exec_method(tasks=tasks, agent_type=agent_type)
                if asyncio.iscoroutine(result):
                    result = await result
                results: List[Dict[str, Any]] = list(result or [])
            except Exception as exc:
                logger.exception("Cluster execution failed: %s", exc)
                if self._strict:
                    raise CarbonSchedulerError(
                        f"Cluster execution failed: {exc}"
                    ) from exc
                results = []
        else:
            logger.warning(
                "cluster_manager lacks execute_distributed_tasks; "
                "returning empty results."
            )
            results = []

        # Augment each result with its decision metadata (safe-zip).
        for result, decision in zip(results, decisions):
            if not isinstance(result, dict):
                continue
            result["carbon_scheduling"] = {
                "node_id": decision.node_id,
                "node_region": decision.node_region,
                "carbon_intensity": decision.carbon_intensity,
                "estimated_carbon_kgco2e": decision.estimated_carbon_kgco2e,
                "carbon_saved_kgco2e": decision.carbon_saved_kgco2e,
            }

        return results

    async def get_scheduler_stats(self) -> Dict[str, Any]:
        """Return scheduler statistics."""
        return await self._call_scheduler("get_statistics")

    async def get_carbon_landscape(self) -> Dict[str, Any]:
        """Return the current carbon landscape."""
        return await self._call_scheduler("get_current_carbon_landscape")

    async def update_node_info(self) -> int:
        """Trigger a node-info refresh on the underlying scheduler."""
        return await self._call_scheduler("update_node_carbon_info")

    # ---------------------------------------------------------- context mgr
    async def __aenter__(self) -> "CarbonAwareRayCluster":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        shutdown = getattr(self.cluster_manager, "shutdown", None)
        if callable(shutdown):
            try:
                result = shutdown()
                if asyncio.iscoroutine(result):
                    await result
            except Exception:
                logger.exception("Cluster shutdown failed.")

    def __repr__(self) -> str:
        return (
            "CarbonAwareRayCluster("
            f"scheduler_is_ray_actor={self._scheduler_is_ray_actor}, "
            f"strict={self._strict})"
        )


# --------------------------------------------------------------------------- #
# Convenience factory
# --------------------------------------------------------------------------- #
def create_carbon_aware_cluster(
    num_workers: int = 4,
    carbon_forecaster: Optional[Any] = None,
) -> CarbonAwareRayCluster:
    """
    Create a Ray cluster with carbon-aware scheduling.

    Tries to construct a real cluster manager via
    ``distributed.ray_cluster_manager.create_ray_cluster``. If that module is
    missing or fails, a minimal in-process stub manager is used so the
    returned :class:`CarbonAwareRayCluster` remains usable for tests and
    dry runs.
    """
    if not isinstance(num_workers, int) or num_workers <= 0:
        raise CarbonSchedulerError("num_workers must be a positive int.")

    manager = None
    try:
        from .ray_cluster_manager import create_ray_cluster  # type: ignore
        manager = create_ray_cluster(num_workers=num_workers)
    except Exception as exc:
        logger.debug(
            "Could not create real Ray cluster manager (%s); using stub.",
            exc,
        )
        manager = _StubClusterManager(num_workers=num_workers)

    return CarbonAwareRayCluster(
        cluster_manager=manager,
        carbon_forecaster=carbon_forecaster,
    )


class _StubClusterManager:
    """Minimal in-process stand-in for a Ray cluster manager."""

    def __init__(self, num_workers: int = 4) -> None:
        self.num_workers = num_workers

    async def execute_distributed_tasks(
        self, tasks: List[Mapping[str, Any]], agent_type: str = "retriever"
    ) -> List[Dict[str, Any]]:
        return [
            {
                "task_id": t.get("task_id"),
                "agent_type": agent_type,
                "status": "ok",
                "stub": True,
            }
            for t in tasks
        ]

    def shutdown(self) -> None:
        logger.debug("Stub cluster manager shutdown.")


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "NodeCarbonMode",
    "NodeInfo",
    "SchedulingDecision",
    "CarbonSchedulerConfig",
    "CarbonAwareScheduler",
    "CarbonAwareRayCluster",
    "create_carbon_aware_cluster",
    "CarbonSchedulerError",
]


# --------------------------------------------------------------------------- #
# Local smoke test: python -m distributed.carbon_aware_scheduler
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO, format="%(levelname)s %(name)s: %(message)s"
    )

    # ---- Fake carbon forecaster -------------------------------------- #
    _REGION_INTENSITY = {
        "eu-west": 80.0,     # very clean
        "us-west": 220.0,    # moderate
        "us-east": 450.0,    # dirty
    }

    class _FakeForecaster:
        async def get_current_intensity(self, region: str) -> float:
            await asyncio.sleep(0)
            return _REGION_INTENSITY.get(region, 300.0)

    # ---- Fake node provider ------------------------------------------ #
    def _node_provider() -> List[Dict[str, Any]]:
        return [
            {
                "NodeID": "node-eu-west-0001",
                "Alive": True,
                "Region": "eu-west",
                "Resources": {"CPU": 8, "GPU": 1},
            },
            {
                "NodeID": "node-us-west-0002",
                "Alive": True,
                "Region": "us-west",
                "Resources": {"CPU": 4, "GPU": 0},
            },
            {
                "NodeID": "node-us-east-0003",
                "Alive": True,
                "Region": "us-east",
                "Resources": {"CPU": 16, "GPU": 2},
            },
            {
                "NodeID": "node-dead-0004",
                "Alive": False,
                "Region": "us-east",
                "Resources": {"CPU": 4, "GPU": 0},
            },
        ]

    async def main() -> None:
        # ------------------------------------------------------------ #
        # 1. Happy-path scheduling
        # ------------------------------------------------------------ #
        scheduler = CarbonAwareScheduler(
            carbon_forecaster=_FakeForecaster(),
            config=CarbonSchedulerConfig(update_interval_minutes=15),
            node_provider=_node_provider,
        )

        refreshed = await scheduler.update_node_carbon_info()
        print(f"refreshed    : {refreshed} node(s)")
        print(f"node count   : {scheduler.node_count}")

        landscape = scheduler.get_current_carbon_landscape()
        print("landscape    :", landscape["nodes_by_carbon_mode"])
        print("avg carbon   :", round(landscape["avg_carbon_intensity"], 1))

        # Schedule a batch of tasks with rising energy cost.
        tasks = [
            {
                "task_id": f"t-{i}",
                "deferrable": True,
                "energy_kwh": 0.001 * (i + 1),
            }
            for i in range(5)
        ]
        decisions = await scheduler.batch_schedule(tasks, task_energy_estimate_kwh=0.002)
        for d in decisions:
            print(
                f"  {d.task_id:<8} -> {d.node_id[:16]:<16} "
                f"carbon={d.carbon_intensity:>6.1f}  "
                f"saved={d.carbon_saved_kgco2e * 1000:.4f} gCO2e"
            )

        # ------------------------------------------------------------ #
        # 2. Statistics (with the corrected savings formula)
        # ------------------------------------------------------------ #
        stats = scheduler.get_statistics()
        print("stats        :", {
            k: (round(v, 4) if isinstance(v, float) else v)
            for k, v in stats.items() if k != "node_utilization"
        })
        print("utilization  :", stats["node_utilization"])

        # ------------------------------------------------------------ #
        # 3. Defer policy (empty-node case + all-dirty case)
        # ------------------------------------------------------------ #
        empty = CarbonAwareScheduler(
            carbon_forecaster=_FakeForecaster(),
            node_provider=lambda: [],  # no nodes at all
        )
        print(
            "empty defer  :",
            await empty.should_defer_task({"task_id": "t-empty", "deferrable": True}),
        )

        dirty = CarbonAwareScheduler(
            carbon_forecaster=_FakeForecaster(),
            node_provider=lambda: [
                {
                    "NodeID": "n-dirty",
                    "Alive": True,
                    "Region": "us-east",
                    "Resources": {"CPU": 8, "GPU": 0},
                }
            ],
        )
        await dirty.update_node_carbon_info()
        print(
            "dirty defer  :",
            await dirty.should_defer_task(
                {"task_id": "t-dirty", "deferrable": True},
                defer_threshold_gco2kwh=300.0,
            ),
        )
        print(
            "deadline     :",
            await dirty.should_defer_task(
                {"task_id": "t-dirty2", "deferrable": True, "deadline": "soon"},
                defer_threshold_gco2kwh=300.0,
            ),
        )

        # ------------------------------------------------------------ #
        # 4. Serialization round-trip
        # ------------------------------------------------------------ #
        payload = scheduler.to_json()
        restored = CarbonAwareScheduler.from_json(
            payload, carbon_forecaster=_FakeForecaster()
        )
        assert restored.to_dict() == scheduler.to_dict()
        print("Serialization round-trip OK.")

        # ------------------------------------------------------------ #
        # 5. Context managers
        # ------------------------------------------------------------ #
        with CarbonAwareScheduler(node_provider=_node_provider) as scoped:
            await scoped.update_node_carbon_info()
            await scoped.schedule_task({"task_id": "ctx-t"}, 0.001)
        print("Sync context OK.")

        async with CarbonAwareScheduler(node_provider=_node_provider) as scoped:
            await scoped.update_node_carbon_info()
            await scoped.schedule_task({"task_id": "actx-t"}, 0.001)
        print("Async context OK.")

        # ------------------------------------------------------------ #
        # 6. Cluster wrapper (Ray-absent path uses the stub manager)
        # ------------------------------------------------------------ #
        cluster = create_carbon_aware_cluster(
            num_workers=2,
            carbon_forecaster=_FakeForecaster(),
        )
        # Replace the stub-provider scheduler with our fake node provider.
        cluster._local_scheduler._node_provider = _node_provider  # type: ignore[attr-defined]
        results = await cluster.schedule_and_execute(
            tasks=[{"task_id": "cl-1"}, {"task_id": "cl-2"}],
            agent_type="retriever",
            task_energy_estimate_kwh=0.001,
        )
        print("cluster exec :", len(results), "result(s)")
        if results:
            print("first result :", results[0])

        cluster_stats = await cluster.get_scheduler_stats()
        print("cluster stats:", {
            k: (round(v, 4) if isinstance(v, float) else v)
            for k, v in cluster_stats.items() if k != "node_utilization"
        })

        # ------------------------------------------------------------ #
        # 7. Validation failures
        # ------------------------------------------------------------ #
        for bad_energy in (float("nan"), float("inf"), -1.0, "not-a-number"):
            try:
                await scheduler.schedule_task({"task_id": "bad"}, bad_energy)  # type: ignore[arg-type]
            except CarbonSchedulerError as exc:
                print("Rejected energy :", exc)
            else:  # pragma: no cover
                raise AssertionError(f"Expected rejection for {bad_energy!r}")

        try:
            await scheduler.schedule_task("not-a-mapping", 0.001)  # type: ignore[arg-type]
        except CarbonSchedulerError as exc:
            print("Rejected task   :", exc)

        for bad_cfg in (
            dict(clean_threshold=0),
            dict(clean_threshold=300, moderate_threshold=200),
            dict(update_interval_minutes=0),
            dict(max_history=0),
        ):
            try:
                CarbonSchedulerConfig(**bad_cfg)  # type: ignore[arg-type]
            except CarbonSchedulerError as exc:
                print("Rejected config :", exc)
            else:  # pragma: no cover
                raise AssertionError(f"Expected config rejection: {bad_cfg!r}")

        print("\nSmoke test passed.")

    asyncio.run(main())
