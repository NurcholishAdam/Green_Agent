# src/distributed/carbon_aware_scheduler.py

"""
Carbon-Aware Scheduler for Ray — v2.0.0
=======================================

Schedules Ray tasks to nodes with lowest carbon intensity.

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

v2.0.0 changes vs. v1.0.0
-------------------------
**P0 bug fixes**
  * 2.1 ``CarbonAwareScheduler`` is a plain class — direct construction works
        in both Ray-present and Ray-absent environments. Use
        ``RemoteCarbonAwareScheduler`` for the actor handle.
  * 2.2 Async ``node_provider`` works. ``_fetch_raw_nodes`` is now async and
        awaits coroutine results.
  * 2.3 ``current_load`` is updated after each schedule (``available_cpus``
        is decremented by ``task_cpu_demand``, ``current_load`` follows).
  * 2.4 ``deferrable="false"`` (string) is now correctly coerced to False.
  * 2.5 ``from_dict`` delegates to ``CarbonSchedulerConfig.from_legacy_dict``
        so new config fields default correctly.

**P1 robustness / observability**
  * 2.6 ``update_node_carbon_info`` fetches region intensities in parallel
        (``asyncio.gather``) instead of serially per node.
  * 2.7 ``node_info`` is encapsulated; callers get dict snapshots via
        ``get_node_info`` / ``list_nodes``. A deprecated ``node_info``
        property returns a snapshot view.
  * 3.1 ``_maybe_refresh`` is single-flight via ``asyncio.Lock``.
  * 3.2 ``schedule_and_execute`` rejects result/decision length mismatch in
        strict mode.
  * 3.3 ``metrics_hook(event, fields)`` for Prometheus / OTel.
  * 3.4 ``close()`` lifecycle hook.
  * 3.5 Context managers hold the ``RLock`` while mutating context state.
  * 3.6 ``SchedulingReason`` enum + ``SchedulingDecision.reason_code``.
  * 3.7 ``get_statistics(window=N)``.
  * 3.8 ``should_defer_task`` parses ``task["deadline"]`` and refuses to
        defer when the deadline is imminent.
  * 3.9 ``schedule_task(..., require_gpus=N)`` filters nodes by available GPUs.

**P2 enhancements**
  * 3.12 ``_StubClusterManager`` accepts ``delay_s`` and ``fail_on`` for
        async / error testing.
  * 3.13 ``create_carbon_aware_cluster(..., node_provider=..., scheduler_config=...)``.
  * 3.14 ``NodeInfo`` is never returned live; always via ``to_dict()``.
  * 3.15 ``isinstance(True, int)`` is rejected for numeric config fields.
  * 3.16 ``SchedulingDecision.carbon_saved_kgco2e`` is validated.
  * 3.17 ``intensity_series(window_s)`` for dashboards.
  * 3.20 ``from_dict`` cross-checks the saved counter against the history sum.

Thread-safe, serializable, picklable, Ray-deployable.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field, fields
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import (
    Any, Callable, Deque, Dict, List, Mapping, Optional, Tuple,
)

logger = logging.getLogger(__name__)

__version__ = "2.0.0"


# --------------------------------------------------------------------------- #
# Optional Ray integration (P0 2.1 — two names)
# --------------------------------------------------------------------------- #
try:  # pragma: no cover — environment-dependent
    import ray  # type: ignore
    from ray.util.scheduling_strategies import (  # type: ignore  # noqa: F401
        PlacementGroupSchedulingStrategy,
    )

    _RAY_AVAILABLE = True
except ImportError:  # pragma: no cover
    ray = None  # type: ignore[assignment]
    PlacementGroupSchedulingStrategy = None  # type: ignore[assignment]
    _RAY_AVAILABLE = False


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


class SchedulingReason(str, Enum):
    """Stable machine-readable reason codes (P1 3.6)."""

    LOWEST_CARBON = "lowest_carbon"
    LEAST_LOADED_TIEBREAK = "least_loaded_tiebreak"
    ONLY_NODE_AVAILABLE = "only_node_available"


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _coerce_bool(value: Any, default: bool = True) -> bool:
    """
    Coerce ``value`` to bool (P0 2.4).

    Handles real bools, common string spellings (``"true"``, ``"false"``,
    ``"yes"``, ``"no"``, ``"1"``, ``"0"``, ``"on"``, ``"off"``), and falls
    back to Python's truthiness. ``None`` returns ``default``.
    """
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, str):
        return value.strip().lower() in (
            "true", "1", "yes", "yes", "on", "y", "t",
        )
    try:
        return bool(value)
    except Exception:  # noqa: BLE001
        return default


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class CarbonSchedulerConfig:
    """
    Tunable parameters for the carbon-aware scheduler.
    """

    clean_threshold: float = 200.0
    moderate_threshold: float = 400.0
    default_carbon_intensity: float = 400.0
    default_defer_threshold: float = 500.0
    update_interval_minutes: int = 15
    max_history: int = 10_000
    node_ttl_seconds: float = 3600.0

    # Imminent-deadline policy (P1 3.8).
    deadline_horizon_minutes: int = 30

    def __post_init__(self) -> None:
        # 3.15: reject bool masquerading as int for numeric fields
        for name in (
            "clean_threshold", "moderate_threshold",
            "default_carbon_intensity", "default_defer_threshold",
            "node_ttl_seconds",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise CarbonSchedulerError(
                    f"{name} must be numeric, got "
                    f"{type(value).__name__}."
                )

        if isinstance(self.update_interval_minutes, bool) or \
                not isinstance(self.update_interval_minutes, int):
            raise CarbonSchedulerError(
                "update_interval_minutes must be a positive int.")
        if isinstance(self.max_history, bool) or \
                not isinstance(self.max_history, int):
            raise CarbonSchedulerError("max_history must be > 0.")
        if isinstance(self.deadline_horizon_minutes, bool) or \
                not isinstance(self.deadline_horizon_minutes, int):
            raise CarbonSchedulerError(
                "deadline_horizon_minutes must be a positive int.")

        if self.clean_threshold <= 0:
            raise CarbonSchedulerError("clean_threshold must be > 0.")
        if self.moderate_threshold <= self.clean_threshold:
            raise CarbonSchedulerError(
                "moderate_threshold must be > clean_threshold.")
        if self.default_carbon_intensity < 0:
            raise CarbonSchedulerError(
                "default_carbon_intensity must be >= 0.")
        if self.default_defer_threshold <= 0:
            raise CarbonSchedulerError(
                "default_defer_threshold must be > 0.")
        if self.update_interval_minutes <= 0:
            raise CarbonSchedulerError(
                "update_interval_minutes must be > 0.")
        if self.max_history <= 0:
            raise CarbonSchedulerError("max_history must be > 0.")
        if self.node_ttl_seconds <= 0:
            raise CarbonSchedulerError("node_ttl_seconds must be > 0.")
        if self.deadline_horizon_minutes <= 0:
            raise CarbonSchedulerError(
                "deadline_horizon_minutes must be > 0.")

    def classify(self, carbon_intensity: float) -> NodeCarbonMode:
        if carbon_intensity < self.clean_threshold:
            return NodeCarbonMode.CLEAN
        if carbon_intensity < self.moderate_threshold:
            return NodeCarbonMode.MODERATE
        return NodeCarbonMode.DIRTY

    # ---------------------------------------------------------------- #
    @classmethod
    def from_legacy_dict(
        cls, cfg: Optional[Mapping[str, Any]]
    ) -> "CarbonSchedulerConfig":
        """Build from a flat mapping; unknown keys ignored (P0 2.5)."""
        cfg = dict(cfg or {})
        valid = {f.name for f in fields(cls)}
        return cls(**{k: v for k, v in cfg.items() if k in valid})


# --------------------------------------------------------------------------- #
# Data containers
# --------------------------------------------------------------------------- #
@dataclass
class NodeInfo:
    """Information about a Ray node. Internal — callers receive dicts."""

    node_id: str
    region: str
    carbon_intensity: float
    carbon_mode: NodeCarbonMode
    available_cpus: int
    available_gpus: int
    current_load: float
    last_updated: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def __post_init__(self) -> None:
        if not isinstance(self.node_id, str) or not self.node_id:
            raise CarbonSchedulerError(
                "node_id must be a non-empty string.")
        if not isinstance(self.carbon_intensity, (int, float)) or \
                math.isnan(self.carbon_intensity) or \
                math.isinf(self.carbon_intensity):
            raise CarbonSchedulerError(
                "carbon_intensity must be a finite number.")
        if self.carbon_intensity < 0:
            raise CarbonSchedulerError(
                "carbon_intensity must be >= 0.")
        if not 0.0 <= self.current_load <= 1.0:
            raise CarbonSchedulerError(
                "current_load must be in [0, 1].")
        if not isinstance(self.last_updated, datetime):
            raise CarbonSchedulerError(
                "last_updated must be a datetime.")
        if not self.last_updated.tzinfo:
            self.last_updated = self.last_updated.replace(
                tzinfo=timezone.utc)

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

        mode_raw = data.get("carbon_mode", NodeCarbonMode.DIRTY.value)
        try:
            mode = (
                mode_raw if isinstance(mode_raw, NodeCarbonMode)
                else NodeCarbonMode(str(mode_raw))
            )
        except ValueError:
            mode = NodeCarbonMode.DIRTY

        return cls(
            node_id=str(data["node_id"]),
            region=str(data.get("region", "unknown")),
            carbon_intensity=float(data["carbon_intensity"]),
            carbon_mode=mode,
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
    reason_code: str = ""                  # P1 3.6
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def __post_init__(self) -> None:
        if not isinstance(self.node_id, str) or not self.node_id:
            raise CarbonSchedulerError(
                "node_id must be a non-empty string.")
        # 3.16: now also validates carbon_saved_kgco2e
        for name in (
            "carbon_intensity",
            "estimated_carbon_kgco2e",
            "baseline_carbon_kgco2e",
            "carbon_saved_kgco2e",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or \
                    not isinstance(value, (int, float)):
                raise CarbonSchedulerError(
                    f"{name} must be numeric, got "
                    f"{type(value).__name__}."
                )
            if math.isnan(value) or math.isinf(value):
                raise CarbonSchedulerError(
                    f"{name} must be finite, got {value!r}.")
        if self.carbon_intensity < 0:
            raise CarbonSchedulerError(
                "carbon_intensity must be >= 0.")
        if self.estimated_carbon_kgco2e < 0:
            raise CarbonSchedulerError(
                "estimated_carbon_kgco2e must be >= 0.")
        if self.baseline_carbon_kgco2e < 0:
            raise CarbonSchedulerError(
                "baseline_carbon_kgco2e must be >= 0.")
        if self.carbon_saved_kgco2e < 0:
            raise CarbonSchedulerError(
                "carbon_saved_kgco2e must be >= 0.")
        if not isinstance(self.timestamp, datetime):
            raise CarbonSchedulerError(
                "timestamp must be a datetime.")
        if not self.timestamp.tzinfo:
            object.__setattr__(
                self, "timestamp",
                self.timestamp.replace(tzinfo=timezone.utc),
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
            estimated_carbon_kgco2e=float(
                data["estimated_carbon_kgco2e"]),
            baseline_carbon_kgco2e=float(
                data.get("baseline_carbon_kgco2e", 0.0)),
            carbon_saved_kgco2e=float(
                data.get("carbon_saved_kgco2e", 0.0)),
            task_id=data.get("task_id"),
            reason_code=str(data.get("reason_code", "")),
            timestamp=timestamp,
        )


# --------------------------------------------------------------------------- #
# Scheduler
# --------------------------------------------------------------------------- #
class CarbonAwareScheduler:
    """
    Schedules tasks to Ray nodes based on carbon intensity.

    Strategy
    --------
    1. Prefer nodes with lowest carbon intensity.
    2. Within the same carbon mode, prefer least-loaded.
    3. If all nodes are DIRTY, defer tasks when possible.
    4. Track carbon savings vs. baseline (average-across-nodes) scheduling.

    The class is a plain (non-Ray-decorated) class. Use
    :data:`RemoteCarbonAwareScheduler` when you need a Ray actor.
    """

    # ------------------------------------------------------------------ #
    def __init__(
        self,
        carbon_forecaster: Optional[Any] = None,
        update_interval_minutes: int = 15,
        *,
        config: Optional[CarbonSchedulerConfig] = None,
        strict: bool = True,
        node_provider: Optional[Callable[[], Any]] = None,
        metrics_hook: Optional[Callable[[str, Mapping[str, Any]], None]] = None,
    ) -> None:
        # 3.15: reject bool
        if isinstance(update_interval_minutes, bool) or \
                not isinstance(update_interval_minutes, int) or \
                update_interval_minutes <= 0:
            raise CarbonSchedulerError(
                "update_interval_minutes must be a positive int.")

        if config is not None:
            self._config = config
        else:
            self._config = CarbonSchedulerConfig(
                update_interval_minutes=update_interval_minutes)

        if metrics_hook is not None and not callable(metrics_hook):
            raise CarbonSchedulerError("metrics_hook must be callable.")

        self.carbon_forecaster = carbon_forecaster
        self._strict: bool = bool(strict)
        self._metrics_hook = metrics_hook

        # Legacy attribute preserved for backward compat.
        self.update_interval = timedelta(
            minutes=self._config.update_interval_minutes)

        self._lock = threading.RLock()
        self._node_info: Dict[str, NodeInfo] = {}   # P1 2.7: renamed
        self.last_update: Optional[datetime] = None
        self._scheduling_history: Deque[SchedulingDecision] = deque(
            maxlen=self._config.max_history)

        self.total_tasks_scheduled: int = 0
        self.carbon_saved_kgco2e: float = 0.0
        self._baseline_kgco2e_total: float = 0.0

        self._node_provider = node_provider

        # P1 3.1: single-flight refresh
        self._refresh_lock = asyncio.Lock()

        self._ctx_start: Optional[float] = None
        self._async_ctx_start: Optional[float] = None
        self._closed: bool = False

        logger.debug(
            "CarbonAwareScheduler v%s initialized "
            "(update_interval=%dm, strict=%s, ray=%s)",
            __version__,
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
            return len(self._node_info)

    @property
    def node_info(self) -> Dict[str, Dict[str, Any]]:
        """
        Deprecated snapshot view (P1 2.7).

        Returns a dict-of-dicts. Use ``get_node_info(node_id)`` or
        ``list_nodes()`` in new code.
        """
        with self._lock:
            return {nid: n.to_dict() for nid, n in self._node_info.items()}

    def get_node_info(self, node_id: str) -> Optional[Dict[str, Any]]:
        """Return a dict snapshot of one node, or ``None``."""
        self._validate_id("node_id", node_id)
        with self._lock:
            info = self._node_info.get(node_id)
        return None if info is None else info.to_dict()

    def list_nodes(self) -> List[Dict[str, Any]]:
        """Return dict snapshots of every node."""
        with self._lock:
            nodes = list(self._node_info.values())
        return [n.to_dict() for n in nodes]

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

    # ---------------------------------------------------------- node refresh
    async def update_node_carbon_info(self) -> int:
        """
        Refresh the carbon-intensity landscape for all known nodes.

        Returns the number of node records updated.
        """
        raw_nodes = await self._fetch_raw_nodes()
        cfg = self._config
        now = datetime.now(timezone.utc)

        # P1 2.6: prefetch intensities for distinct regions in parallel.
        regions: set = set()
        for node in raw_nodes:
            if not isinstance(node, Mapping):
                continue
            if node.get("Alive") is False:
                continue
            region = str(
                node.get("Region")
                or node.get("region")
                or node.get("NodeManagerAddress")
                or "US-CA"
            )
            regions.add(region)

        if regions:
            results = await asyncio.gather(
                *(self._safe_get_intensity(r) for r in regions),
                return_exceptions=False,
            )
            intensity_by_region = dict(zip(regions, results))
        else:
            intensity_by_region = {}

        updated = 0
        for node in raw_nodes:
            if not isinstance(node, Mapping):
                continue
            if node.get("Alive") is False:
                continue

            node_id = node.get("NodeID") or node.get("node_id")
            if not isinstance(node_id, str) or not node_id:
                continue

            region = str(
                node.get("Region")
                or node.get("region")
                or node.get("NodeManagerAddress")
                or "US-CA"
            )

            carbon_intensity = intensity_by_region.get(
                region, cfg.default_carbon_intensity)

            resources = node.get("Resources") or node.get(
                "resources") or {}
            try:
                available_cpus = int(resources.get("CPU", 0))
                available_gpus = int(resources.get("GPU", 0))
            except (TypeError, ValueError):
                available_cpus = 0
                available_gpus = 0

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
                self._node_info[node_id] = info
            updated += 1
            self._emit_metric("node_refreshed", {
                "node_id": node_id,
                "region": region,
                "intensity": carbon_intensity,
            })

        # Purge stale entries past TTL.
        with self._lock:
            stale_ids = [
                nid for nid, info in self._node_info.items()
                if (now - info.last_updated).total_seconds()
                > cfg.node_ttl_seconds
            ]
            for nid in stale_ids:
                self._node_info.pop(nid, None)

        with self._lock:
            self.last_update = now
            total = len(self._node_info)

        logger.info(
            "Updated carbon info: %d node(s) refreshed, "
            "%d stale purged (total=%d)",
            updated, len(stale_ids), total,
        )
        self._emit_metric("refresh_cycle", {
            "updated": updated,
            "purged": len(stale_ids),
            "total": total,
        })
        return updated

    async def _fetch_raw_nodes(self) -> List[Mapping[str, Any]]:
        """Return raw node metadata from the provider or Ray (P0 2.2)."""
        if self._node_provider is not None:
            try:
                result = self._node_provider()
                if asyncio.iscoroutine(result):
                    result = await result
                return [n for n in result if isinstance(n, Mapping)]
            except Exception as exc:  # noqa: BLE001
                logger.warning("node_provider failed: %s", exc)
                self._emit_metric(
                    "refresh_failed", {"source": "provider"})
                return []

        if _RAY_AVAILABLE and ray is not None:
            try:
                nodes = ray.nodes()
                return [n for n in nodes if isinstance(n, Mapping)]
            except Exception as exc:  # noqa: BLE001
                logger.warning("ray.nodes() failed: %s", exc)
                self._emit_metric(
                    "refresh_failed", {"source": "ray"})
                return []

        return []

    @staticmethod
    def _estimate_load(node: Mapping[str, Any], available_cpus: int) -> float:
        """Best-effort load estimate in [0, 1]."""
        used = node.get("UsedCPU") or node.get("used_cpu")
        if used is None:
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

        method = getattr(
            self.carbon_forecaster, "get_current_intensity", None)
        if not callable(method):
            return self._config.default_carbon_intensity

        try:
            result = method(region)
            if asyncio.iscoroutine(result):
                result = await result
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Carbon forecaster failed for region %r: %s",
                region, exc)
            return self._config.default_carbon_intensity

        try:
            value = float(result)
        except (TypeError, ValueError):
            logger.warning(
                "Carbon forecaster returned non-numeric value %r; "
                "using default.", result)
            return self._config.default_carbon_intensity

        if math.isnan(value) or math.isinf(value) or value < 0:
            logger.warning(
                "Carbon forecaster returned invalid value %r; "
                "using default.", value)
            return self._config.default_carbon_intensity
        return value

    # ---------------------------------------------------------- scheduling
    async def schedule_task(
        self,
        task: Mapping[str, Any],
        task_energy_estimate_kwh: float = 0.001,
        *,
        task_cpu_demand: int = 1,
        require_gpus: int = 0,
    ) -> SchedulingDecision:
        """
        Schedule a task to the optimal node based on carbon intensity.

        Parameters
        ----------
        task : Mapping
            Recognized optional keys: ``task_id``, ``deferrable``,
            ``deadline``.
        task_energy_estimate_kwh : float
            Estimated energy consumption. Must be finite and >= 0.
        task_cpu_demand : int, default 1
            CPUs to consume on the chosen node (P0 2.3). Decrements
            ``available_cpus`` and updates ``current_load``.
        require_gpus : int, default 0
            Minimum available GPUs on the chosen node (P1 3.9).
        """
        if not isinstance(task, Mapping):
            raise CarbonSchedulerError(
                f"task must be a Mapping, "
                f"got {type(task).__name__}.")
        energy = self._validate_energy(task_energy_estimate_kwh)
        if isinstance(task_cpu_demand, bool) or \
                not isinstance(task_cpu_demand, int) or \
                task_cpu_demand < 0:
            raise CarbonSchedulerError(
                "task_cpu_demand must be a non-negative int.")
        if isinstance(require_gpus, bool) or \
                not isinstance(require_gpus, int) or require_gpus < 0:
            raise CarbonSchedulerError(
                "require_gpus must be a non-negative int.")

        await self._maybe_refresh()

        with self._lock:
            available_nodes = [
                n for n in self._node_info.values()
                if n.available_cpus >= max(1, task_cpu_demand)
                and n.available_gpus >= require_gpus
            ]

        if not available_nodes:
            raise CarbonSchedulerError(
                "No available nodes for scheduling.")

        sorted_nodes = sorted(
            available_nodes,
            key=lambda n: (n.carbon_intensity, n.current_load),
        )
        best_node = sorted_nodes[0]

        # Reason classification.
        if len(sorted_nodes) == 1:
            reason_code = SchedulingReason.ONLY_NODE_AVAILABLE.value
        elif sorted_nodes[1].carbon_intensity > best_node.carbon_intensity:
            reason_code = SchedulingReason.LOWEST_CARBON.value
        else:
            reason_code = SchedulingReason.LEAST_LOADED_TIEBREAK.value

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
                f"Lowest carbon intensity "
                f"({best_node.carbon_mode.value}, "
                f"{best_node.carbon_intensity:.1f} gCO2/kWh)"
            ),
            estimated_carbon_kgco2e=actual_kg,
            baseline_carbon_kgco2e=baseline_kg,
            carbon_saved_kgco2e=saved_kg,
            task_id=task_id,
            reason_code=reason_code,
        )

        # P0 2.3: decrement the live node's capacity + update load.
        with self._lock:
            live = self._node_info.get(best_node.node_id)
            if live is not None:
                consumed = min(task_cpu_demand, live.available_cpus)
                if consumed > 0:
                    live.available_cpus -= consumed
                    denom = live.available_cpus + consumed
                    live.current_load = max(
                        0.0,
                        min(1.0, 1.0 - live.available_cpus / denom),
                    )
            self._scheduling_history.append(decision)
            self.total_tasks_scheduled += 1
            self.carbon_saved_kgco2e += saved_kg
            self._baseline_kgco2e_total += baseline_kg

        self._emit_metric("task_scheduled", {
            "node_id": best_node.node_id,
            "carbon_mode": best_node.carbon_mode.value,
            "intensity": best_node.carbon_intensity,
            "saved_kg": saved_kg,
            "reason_code": reason_code,
        })
        logger.info(
            "Scheduled %s to node %s "
            "(carbon=%.1f gCO2/kWh, saved=%.3f gCO2e)",
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
        """Schedule multiple tasks sequentially."""
        if not isinstance(tasks, list):
            raise CarbonSchedulerError("tasks must be a list.")
        energy = self._validate_energy(task_energy_estimate_kwh)

        decisions: List[SchedulingDecision] = []
        for task in tasks:
            decisions.append(
                await self.schedule_task(task, energy))
        return decisions

    async def should_defer_task(
        self,
        task: Mapping[str, Any],
        defer_threshold_gco2kwh: Optional[float] = None,
    ) -> bool:
        """
        Return True if the task should be deferred to a cleaner window.

        Rules
        -----
        - Refresh node info if empty.
        - If **no nodes are known**, do **not** defer (``all([]) is True``
          trap — fixed in v1.0.1).
        - If all nodes exceed the threshold **and** the task is deferrable
          **and** has no imminent deadline, return True (P1 3.8).
        """
        if not isinstance(task, Mapping):
            raise CarbonSchedulerError(
                f"task must be a Mapping, "
                f"got {type(task).__name__}.")

        threshold = defer_threshold_gco2kwh
        if threshold is None:
            threshold = self._config.default_defer_threshold
        if isinstance(threshold, bool) or \
                not isinstance(threshold, (int, float)) or \
                math.isnan(threshold) or math.isinf(threshold):
            raise CarbonSchedulerError(
                "defer_threshold_gco2kwh must be a finite number.")
        if threshold < 0:
            raise CarbonSchedulerError(
                "defer_threshold_gco2kwh must be >= 0.")

        with self._lock:
            has_nodes = bool(self._node_info)
        if not has_nodes:
            await self.update_node_carbon_info()

        with self._lock:
            nodes = list(self._node_info.values())

        # P0 2.2-style empty guard.
        if not nodes:
            logger.debug("No node info available; not deferring.")
            return False

        # P0 2.4: coerce strings like "false" correctly.
        is_deferrable = _coerce_bool(
            task.get("deferrable", True), default=True)

        # P1 3.8: parse the deadline; refuse to defer when imminent.
        has_imminent_deadline = self._has_imminent_deadline(
            task.get("deadline"))

        all_dirty = all(
            node.carbon_intensity > threshold for node in nodes)

        if all_dirty and is_deferrable and not has_imminent_deadline:
            self._emit_metric("task_deferred", {
                "task_id": task.get("task_id", "<unnamed>"),
                "threshold": threshold,
                "reason": "all_nodes_above_threshold",
            })
            logger.info(
                "Task %s deferred — all nodes above %.0f gCO2/kWh.",
                task.get("task_id", "<unnamed>"),
                threshold,
            )
            return True
        return False

    def _has_imminent_deadline(self, deadline_raw: Any) -> bool:
        """Return True if ``deadline_raw`` is present and falls within
        ``deadline_horizon_minutes`` from now (P1 3.8)."""
        if deadline_raw is None:
            return False
        if isinstance(deadline_raw, datetime):
            deadline = deadline_raw
            if not deadline.tzinfo:
                deadline = deadline.replace(tzinfo=timezone.utc)
        elif isinstance(deadline_raw, str):
            try:
                deadline = datetime.fromisoformat(
                    deadline_raw.strip().replace("Z", "+00:00"))
                if not deadline.tzinfo:
                    deadline = deadline.replace(tzinfo=timezone.utc)
            except ValueError:
                # Malformed deadline → do NOT defer (conservative).
                return True
        else:
            # Unknown type → treat as present but unparseable.
            return True

        horizon = timedelta(minutes=self._config.deadline_horizon_minutes)
        return (deadline - datetime.now(timezone.utc)) < horizon

    # ---------------------------------------------------------- stats
    def get_statistics(
        self, window: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Return scheduler statistics.

        ``window`` (P1 3.7): if provided, restrict derived metrics (mean
        intensity, savings) to the last ``window`` decisions, using the
        history slice for both counters and means.
        """
        with self._lock:
            if window is None:
                history = list(self._scheduling_history)
                total_tasks = self.total_tasks_scheduled
                saved = self.carbon_saved_kgco2e
                baseline_total = self._baseline_kgco2e_total
            else:
                history = list(self._scheduling_history)[-window:]
                total_tasks = len(history)
                saved = sum(d.carbon_saved_kgco2e for d in history)
                baseline_total = sum(
                    d.baseline_carbon_kgco2e for d in history)
            node_info_snapshot = {
                nid: info.to_dict()
                for nid, info in self._node_info.items()
            }

        if not history:
            return {
                "total_tasks_scheduled": total_tasks,
                "carbon_saved_kgco2e": saved,
                "carbon_saved_percent": 0.0,
                "avg_carbon_intensity": 0.0,
                "node_utilization": {},
                "baseline_carbon_kgco2e": baseline_total,
                "nodes_known": len(node_info_snapshot),
            }

        saved_percent = (
            (saved / baseline_total) * 100.0
            if baseline_total > 0 else 0.0
        )
        avg_intensity = (
            sum(d.carbon_intensity for d in history) / len(history))

        utilization: Dict[str, int] = {}
        for decision in history:
            utilization[decision.node_id] = (
                utilization.get(decision.node_id, 0) + 1)

        return {
            "total_tasks_scheduled": total_tasks,
            "carbon_saved_kgco2e": saved,
            "carbon_saved_percent": saved_percent,
            "avg_carbon_intensity": avg_intensity,
            "node_utilization": utilization,
            "baseline_carbon_kgco2e": baseline_total,
            "nodes_known": len(node_info_snapshot),
        }

    def statistics(self) -> Dict[str, Any]:
        """Alias for :meth:`get_statistics`."""
        return self.get_statistics()

    def intensity_series(
        self, *, window_s: Optional[float] = None,
    ) -> List[Tuple[float, float]]:
        """
        Return ``[(unix_ts, carbon_intensity), ...]`` from the decision
        history (P2 3.17). Useful for dashboards.
        """
        cutoff = (
            time.time() - window_s if window_s is not None else None)
        with self._lock:
            decisions = list(self._scheduling_history)
        out: List[Tuple[float, float]] = []
        for d in decisions:
            ts = d.timestamp.timestamp()
            if cutoff is not None and ts < cutoff:
                continue
            out.append((ts, d.carbon_intensity))
        return out

    def get_current_carbon_landscape(self) -> Dict[str, Any]:
        """Return the current carbon-intensity landscape across all nodes."""
        with self._lock:
            nodes = list(self._node_info.values())

        if not nodes:
            return {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "total_nodes": 0,
                "nodes_by_carbon_mode": {
                    "clean": 0, "moderate": 0, "dirty": 0},
                "nodes": {"clean": [], "moderate": [], "dirty": []},
                "avg_carbon_intensity": 0.0,
            }

        nodes_by_mode: Dict[str, List[Dict[str, Any]]] = {
            "clean": [], "moderate": [], "dirty": []}
        for node in nodes:
            nodes_by_mode[node.carbon_mode.value].append({
                "node_id": node.node_id[:8],
                "region": node.region,
                "carbon_intensity": node.carbon_intensity,
                "available_cpus": node.available_cpus,
                "current_load": node.current_load,
            })

        avg = sum(n.carbon_intensity for n in nodes) / len(nodes)
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "total_nodes": len(nodes),
            "nodes_by_carbon_mode": {
                mode: len(items)
                for mode, items in nodes_by_mode.items()},
            "nodes": nodes_by_mode,
            "avg_carbon_intensity": avg,
        }

    # ---------------------------------------------------------- lifecycle
    async def _maybe_refresh(self) -> None:
        """Single-flight refresh (P1 3.1)."""
        with self._lock:
            last = self.last_update
        if last is not None and (
            datetime.now(timezone.utc) - last <= self.update_interval
        ):
            return
        async with self._refresh_lock:
            # Re-check inside the lock
            with self._lock:
                last = self.last_update
            if last is not None and (
                datetime.now(timezone.utc) - last <= self.update_interval
            ):
                return
            await self.update_node_carbon_info()

    def close(self) -> None:
        """Release resources (P1 3.4). Idempotent."""
        self._closed = True
        logger.debug(
            "CarbonAwareScheduler closed (tasks=%d, saved=%.4f kg).",
            self.total_tasks_scheduled,
            self.carbon_saved_kgco2e,
        )

    def reset(self, *, clear_history: bool = False) -> None:
        """Reset internal state; optionally clear scheduling history."""
        with self._lock:
            if clear_history:
                self._scheduling_history.clear()
            self._node_info.clear()
            self.last_update = None
            self.total_tasks_scheduled = 0
            self.carbon_saved_kgco2e = 0.0
            self._baseline_kgco2e_total = 0.0
            self._ctx_start = None
            self._async_ctx_start = None
        logger.debug(
            "CarbonAwareScheduler reset (clear_history=%s)",
            clear_history)

    # ---------------------------------------------------------- validation
    @staticmethod
    def _validate_id(name: str, value: Any) -> None:
        if not isinstance(value, str) or not value:
            raise CarbonSchedulerError(
                f"{name} must be a non-empty string.")

    def _validate_energy(self, value: Any) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise CarbonSchedulerError(
                f"task_energy_estimate_kwh must be numeric, "
                f"got {type(value).__name__}.")
        fvalue = float(value)
        if math.isnan(fvalue) or math.isinf(fvalue):
            raise CarbonSchedulerError(
                f"task_energy_estimate_kwh must be finite, "
                f"got {value!r}.")
        if fvalue < 0:
            raise CarbonSchedulerError(
                f"task_energy_estimate_kwh must be >= 0, "
                f"got {fvalue}.")
        return fvalue

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "version": __version__,
                "config": asdict(self._config),
                "strict": self._strict,
                "ray_available": _RAY_AVAILABLE,
                "last_update": (
                    self.last_update.isoformat()
                    if self.last_update else None
                ),
                "counters": {
                    "total_tasks_scheduled": self.total_tasks_scheduled,
                    "carbon_saved_kgco2e": self.carbon_saved_kgco2e,
                    "baseline_kgco2e_total": self._baseline_kgco2e_total,
                },
                "nodes": [
                    n.to_dict() for n in self._node_info.values()],
                "history": [
                    d.to_dict() for d in self._scheduling_history],
            }

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        carbon_forecaster: Optional[Any] = None,
        metrics_hook: Optional[
            Callable[[str, Mapping[str, Any]], None]
        ] = None,
    ) -> "CarbonAwareScheduler":
        if not isinstance(data, Mapping):
            raise CarbonSchedulerError(
                f"from_dict expects a Mapping, "
                f"got {type(data).__name__}.")

        # P0 2.5: delegate to from_legacy_dict
        cfg = CarbonSchedulerConfig.from_legacy_dict(
            data.get("config") or {})

        scheduler = cls(
            carbon_forecaster=carbon_forecaster,
            config=cfg,
            strict=bool(data.get("strict", True)),
            metrics_hook=metrics_hook,
        )

        with scheduler._lock:
            for n in data.get("nodes", []):
                info = NodeInfo.from_dict(n)
                scheduler._node_info[info.node_id] = info
            for d in data.get("history", []):
                scheduler._scheduling_history.append(
                    SchedulingDecision.from_dict(d))
            counters = dict(data.get("counters", {}) or {})
            scheduler.total_tasks_scheduled = int(
                counters.get("total_tasks_scheduled", 0))
            scheduler.carbon_saved_kgco2e = float(
                counters.get("carbon_saved_kgco2e", 0.0))
            scheduler._baseline_kgco2e_total = float(
                counters.get("baseline_kgco2e_total", 0.0))
            last = data.get("last_update")
            if isinstance(last, str):
                timestamp = datetime.fromisoformat(last)
                if not timestamp.tzinfo:
                    timestamp = timestamp.replace(tzinfo=timezone.utc)
                scheduler.last_update = timestamp

        # 3.20: cross-check saved counter vs history sum
        if scheduler._scheduling_history:
            computed = sum(
                d.carbon_saved_kgco2e
                for d in scheduler._scheduling_history)
            if abs(computed - scheduler.carbon_saved_kgco2e) > 1e-6:
                logger.warning(
                    "Restored carbon_saved_kgco2e (%.6f) does not "
                    "match history sum (%.6f); using history sum.",
                    scheduler.carbon_saved_kgco2e, computed,
                )
                if scheduler._strict:
                    scheduler.carbon_saved_kgco2e = computed
        return scheduler

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(
        cls,
        payload: str,
        *,
        carbon_forecaster: Optional[Any] = None,
        metrics_hook: Optional[
            Callable[[str, Mapping[str, Any]], None]
        ] = None,
    ) -> "CarbonAwareScheduler":
        try:
            return cls.from_dict(
                json.loads(payload),
                carbon_forecaster=carbon_forecaster,
                metrics_hook=metrics_hook,
            )
        except json.JSONDecodeError as exc:
            raise CarbonSchedulerError(
                f"Invalid JSON payload: {exc}") from exc

    # ---------------------------------------------------------- context mgr
    def __enter__(self) -> "CarbonAwareScheduler":
        with self._lock:
            self._ctx_start = time.perf_counter()
        logger.debug("Entering scoped CarbonAwareScheduler session.")
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
            tasks = self.total_tasks_scheduled
        if exc_type is not None:
            logger.warning(
                "CarbonAwareScheduler scope exited with %s "
                "after %.4fs.", exc_type.__name__, elapsed)
            return
        logger.info(
            "CarbonAwareScheduler scope closed in %.4fs "
            "(%d task(s)).", elapsed, tasks)

    async def __aenter__(self) -> "CarbonAwareScheduler":
        with self._lock:
            self._async_ctx_start = time.perf_counter()
        logger.debug("Entering async CarbonAwareScheduler session.")
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
            tasks = self.total_tasks_scheduled
        if exc_type is not None:
            logger.warning(
                "Async CarbonAwareScheduler scope exited with %s "
                "after %.4fs.", exc_type.__name__, elapsed)
            return
        logger.info(
            "Async CarbonAwareScheduler scope closed in %.4fs "
            "(%d task(s)).", elapsed, tasks)

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        with self._lock:
            return (
                "CarbonAwareScheduler("
                f"version={__version__!r}, "
                f"nodes={len(self._node_info)}, "
                f"tasks={self.total_tasks_scheduled}, "
                f"saved_kg={self.carbon_saved_kgco2e:.4f}, "
                f"strict={self._strict}, "
                f"ray={_RAY_AVAILABLE})"
            )


# --------------------------------------------------------------------------- #
# Ray integration (P0 2.1 — two names)
# --------------------------------------------------------------------------- #
if _RAY_AVAILABLE:  # pragma: no cover — environment-dependent
    RemoteCarbonAwareScheduler = ray.remote(  # type: ignore[assignment]
        CarbonAwareScheduler)
else:
    RemoteCarbonAwareScheduler = CarbonAwareScheduler


# --------------------------------------------------------------------------- #
# Cluster wrapper
# --------------------------------------------------------------------------- #
class CarbonAwareRayCluster:
    """
    Ray cluster with integrated carbon-aware scheduling.

    Works with or without Ray installed:

    - When Ray is available, the scheduler is deployed as a Ray actor via
      ``RemoteCarbonAwareScheduler.remote(...)``.
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
            return

        if _RAY_AVAILABLE and ray is not None:
            try:
                self.scheduler_actor = RemoteCarbonAwareScheduler.remote(  # type: ignore[attr-defined]
                    carbon_forecaster=carbon_forecaster,
                )
                self._scheduler_is_ray_actor = True
                self._local_scheduler = None
                return
            except Exception as exc:  # pragma: no cover — env-dependent
                logger.warning(
                    "Ray actor creation failed (%s); falling back "
                    "to in-process scheduler.", exc)

        self._local_scheduler = CarbonAwareScheduler(
            carbon_forecaster=carbon_forecaster, strict=strict)
        self.scheduler_actor = self._local_scheduler
        self._scheduler_is_ray_actor = False

    # ---------------------------------------------------------- internal
    async def _call_scheduler(
        self, method_name: str, **kwargs: Any,
    ) -> Any:
        if self._scheduler_is_ray_actor and _RAY_AVAILABLE and \
                ray is not None:
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
        """Schedule with carbon awareness, then execute via cluster."""
        if not isinstance(tasks, list):
            raise CarbonSchedulerError("tasks must be a list.")

        decisions = await self._call_scheduler(
            "batch_schedule",
            tasks=tasks,
            task_energy_estimate_kwh=task_energy_estimate_kwh,
        )

        exec_method = getattr(
            self.cluster_manager,
            "execute_distributed_tasks", None)
        if callable(exec_method):
            try:
                result = exec_method(
                    tasks=tasks, agent_type=agent_type)
                if asyncio.iscoroutine(result):
                    result = await result
                results: List[Dict[str, Any]] = list(result or [])
            except Exception as exc:  # noqa: BLE001
                logger.exception("Cluster execution failed: %s", exc)
                if self._strict:
                    raise CarbonSchedulerError(
                        f"Cluster execution failed: {exc}") from exc
                results = []
        else:
            logger.warning(
                "cluster_manager lacks execute_distributed_tasks; "
                "returning empty results.")
            results = []

        # P1 3.2: strict length check.
        if len(results) != len(decisions):
            msg = (
                f"Cluster returned {len(results)} results for "
                f"{len(decisions)} scheduled tasks."
            )
            if self._strict:
                raise CarbonSchedulerError(msg)
            logger.warning(msg)

        for result, decision in zip(results, decisions):
            if not isinstance(result, dict):
                continue
            result["carbon_scheduling"] = {
                "node_id": decision.node_id,
                "node_region": decision.node_region,
                "carbon_intensity": decision.carbon_intensity,
                "estimated_carbon_kgco2e":
                    decision.estimated_carbon_kgco2e,
                "carbon_saved_kgco2e": decision.carbon_saved_kgco2e,
                "reason_code": decision.reason_code,
            }

        return results

    async def get_scheduler_stats(self) -> Dict[str, Any]:
        return await self._call_scheduler("get_statistics")

    async def get_carbon_landscape(self) -> Dict[str, Any]:
        return await self._call_scheduler(
            "get_current_carbon_landscape")

    async def update_node_info(self) -> int:
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
            except Exception:  # noqa: BLE001
                logger.exception("Cluster shutdown failed.")

    def __repr__(self) -> str:
        return (
            "CarbonAwareRayCluster("
            f"scheduler_is_ray_actor="
            f"{self._scheduler_is_ray_actor}, "
            f"strict={self._strict})"
        )


# --------------------------------------------------------------------------- #
# Convenience factory (P2 3.13)
# --------------------------------------------------------------------------- #
def create_carbon_aware_cluster(
    num_workers: int = 4,
    carbon_forecaster: Optional[Any] = None,
    *,
    node_provider: Optional[Callable[[], Any]] = None,
    scheduler_config: Optional[CarbonSchedulerConfig] = None,
    metrics_hook: Optional[
        Callable[[str, Mapping[str, Any]], None]
    ] = None,
) -> CarbonAwareRayCluster:
    """
    Create a Ray cluster with carbon-aware scheduling.

    If ``node_provider`` or ``scheduler_config`` is supplied, an in-process
    :class:`CarbonAwareScheduler` is constructed and injected — this
    bypasses the Ray actor path even when Ray is installed (useful for
    tests and dry runs).
    """
    if isinstance(num_workers, bool) or \
            not isinstance(num_workers, int) or num_workers <= 0:
        raise CarbonSchedulerError(
            "num_workers must be a positive int.")

    manager = None
    try:
        from .ray_cluster_manager import (  # type: ignore  # noqa: F401
            create_ray_cluster,
        )
        manager = create_ray_cluster(num_workers=num_workers)
    except Exception as exc:  # noqa: BLE001
        logger.debug(
            "Could not create real Ray cluster manager (%s); "
            "using stub.", exc)
        manager = _StubClusterManager(num_workers=num_workers)

    if node_provider is not None or scheduler_config is not None:
        scheduler = CarbonAwareScheduler(
            carbon_forecaster=carbon_forecaster,
            config=scheduler_config,
            node_provider=node_provider,
            metrics_hook=metrics_hook,
        )
        return CarbonAwareRayCluster(
            cluster_manager=manager,
            carbon_forecaster=carbon_forecaster,
            scheduler=scheduler,
        )

    return CarbonAwareRayCluster(
        cluster_manager=manager,
        carbon_forecaster=carbon_forecaster,
    )


class _StubClusterManager:
    """Minimal in-process stand-in for a Ray cluster manager."""

    def __init__(
        self,
        num_workers: int = 4,
        *,
        delay_s: float = 0.0,
        fail_on: Optional[str] = None,
    ) -> None:
        self.num_workers = num_workers
        self._delay_s = delay_s
        self._fail_on = fail_on

    async def execute_distributed_tasks(
        self,
        tasks: List[Mapping[str, Any]],
        agent_type: str = "retriever",
    ) -> List[Dict[str, Any]]:
        if self._delay_s > 0:
            await asyncio.sleep(self._delay_s)
        if self._fail_on is not None:
            for t in tasks:
                if t.get("task_id") == self._fail_on:
                    raise RuntimeError(
                        f"stub failure on {self._fail_on}")
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
    "SchedulingReason",
    "NodeInfo",
    "SchedulingDecision",
    "CarbonSchedulerConfig",
    "CarbonAwareScheduler",
    "RemoteCarbonAwareScheduler",
    "CarbonAwareRayCluster",
    "create_carbon_aware_cluster",
    "CarbonSchedulerError",
]


# --------------------------------------------------------------------------- #
# Self-tests: python -m distributed.carbon_aware_scheduler
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s: %(message)s",
    )

    # ---- Fakes --------------------------------------------------------- #
    _REGION_INTENSITY = {
        "eu-west": 80.0,
        "us-west": 220.0,
        "us-east": 450.0,
    }

    class _FakeForecaster:
        def __init__(self, async_mode: bool = True) -> None:
            self.call_count = 0
            self._async = async_mode

        def _lookup(self, region: str) -> float:
            self.call_count += 1
            return _REGION_INTENSITY.get(region, 300.0)

        async def get_current_intensity(self, region: str) -> float:
            if self._async:
                await asyncio.sleep(0)
            return self._lookup(region)

    def _node_provider_sync() -> List[Dict[str, Any]]:
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

    async def _node_provider_async() -> List[Dict[str, Any]]:
        await asyncio.sleep(0)
        return _node_provider_sync()

    async def main() -> None:
        print(f"=== carbon_aware_scheduler v{__version__} self-tests ===\n")

        # ------------------------------------------------------------- #
        # 1. Basic scheduling (happy path)
        # ------------------------------------------------------------- #
        print("=== 1. Basic scheduling ===")
        scheduler = CarbonAwareScheduler(
            carbon_forecaster=_FakeForecaster(),
            config=CarbonSchedulerConfig(update_interval_minutes=15),
            node_provider=_node_provider_sync,
        )
        refreshed = await scheduler.update_node_carbon_info()
        assert refreshed == 3, f"expected 3 alive nodes, got {refreshed}"
        print(f"  ✓ refreshed {refreshed} node(s); "
              f"node_count={scheduler.node_count}")

        decisions = await scheduler.batch_schedule(
            [{"task_id": f"t-{i}"} for i in range(3)],
            task_energy_estimate_kwh=0.002,
        )
        for d in decisions:
            print(f"    {d.task_id} -> {d.node_id[:16]} "
                  f"carbon={d.carbon_intensity:.1f} "
                  f"code={d.reason_code}")
        # First decision should pick the cleanest (eu-west)
        assert decisions[0].node_id == "node-eu-west-0001"
        assert decisions[0].reason_code == \
            SchedulingReason.LOWEST_CARBON.value
        print(f"  ✓ chose cleanest node; saved="
              f"{decisions[0].carbon_saved_kgco2e * 1000:.4f} gCO2e")

        # ------------------------------------------------------------- #
        # 2. P0 2.1: Ray decorator does not break construction
        # ------------------------------------------------------------- #
        print("\n=== 2. P0 2.1: two names — CarbonAwareScheduler + "
              "RemoteCarbonAwareScheduler ===")
        assert CarbonAwareScheduler is not None
        assert RemoteCarbonAwareScheduler is not None
        # Direct construction must work in any environment
        direct = CarbonAwareScheduler()
        assert direct.node_count == 0
        print(f"  ✓ CarbonAwareScheduler() constructs directly "
              f"(ray_available={_RAY_AVAILABLE})")
        print(f"  ✓ RemoteCarbonAwareScheduler available "
              f"({getattr(RemoteCarbonAwareScheduler, '__name__', 'ray actor')})")

        # ------------------------------------------------------------- #
        # 3. P0 2.2: async node_provider works
        # ------------------------------------------------------------- #
        print("\n=== 3. P0 2.2: async node_provider ===")
        async_sched = CarbonAwareScheduler(
            carbon_forecaster=_FakeForecaster(),
            node_provider=_node_provider_async,   # ← coroutine function
        )
        n = await async_sched.update_node_carbon_info()
        assert n == 3, (
            f"P0 2.2 regression: async provider returned {n} nodes "
            f"(expected 3)")
        print(f"  ✓ async provider refreshed {n} node(s)")

        # ------------------------------------------------------------- #
        # 4. P0 2.3: load decrements after schedule
        # ------------------------------------------------------------- #
        print("\n=== 4. P0 2.3: load decrement after schedule ===")

        def _equal_nodes() -> List[Dict[str, Any]]:
            # Two nodes with identical intensity and equal CPU capacity.
            return [
                {"NodeID": "eq-a", "Alive": True, "Region": "eu-west",
                 "Resources": {"CPU": 2, "GPU": 0}},
                {"NodeID": "eq-b", "Alive": True, "Region": "eu-west",
                 "Resources": {"CPU": 2, "GPU": 0}},
            ]

        eq = CarbonAwareScheduler(
            carbon_forecaster=_FakeForecaster(),
            node_provider=_equal_nodes,
        )
        await eq.update_node_carbon_info()
        a_before = eq.get_node_info("eq-a")
        b_before = eq.get_node_info("eq-b")
        assert a_before["available_cpus"] == 2
        assert b_before["available_cpus"] == 2
        assert a_before["current_load"] == 0.0
        assert b_before["current_load"] == 0.0

        d1 = await eq.schedule_task(
            {"task_id": "eq-1"}, 0.001, task_cpu_demand=1)
        a_after = eq.get_node_info("eq-a")
        b_after = eq.get_node_info("eq-b")
        # First task lands on whichever node sorted first — but its cpus
        # must decrease and load must rise.
        chosen = d1.node_id
        other = "eq-b" if chosen == "eq-a" else "eq-a"
        chosen_snap = eq.get_node_info(chosen)
        other_snap = eq.get_node_info(other)
        assert chosen_snap["available_cpus"] == 1, chosen_snap
        assert chosen_snap["current_load"] == 0.5, chosen_snap
        assert other_snap["available_cpus"] == 2, other_snap
        assert other_snap["current_load"] == 0.0, other_snap
        print(f"  ✓ after 1st task, {chosen}: cpus=1, load=0.5; "
              f"{other}: cpus=2, load=0.0")

        # Second task should go to the OTHER node (least loaded).
        d2 = await eq.schedule_task(
            {"task_id": "eq-2"}, 0.001, task_cpu_demand=1)
        assert d2.node_id != chosen, (
            f"P0 2.3 regression: second task went to same node "
            f"({chosen}) despite higher load")
        assert d2.reason_code == \
            SchedulingReason.LEAST_LOADED_TIEBREAK.value, d2.reason_code
        print(f"  ✓ second task went to {d2.node_id} (least loaded); "
              f"code={d2.reason_code}")

        # ------------------------------------------------------------- #
        # 5. P0 2.4: deferrable="false" (string) → False
        # ------------------------------------------------------------- #
        print("\n=== 5. P0 2.4: deferrable=\"false\" respected ===")
        dirty_provider = lambda: [  # noqa: E731
            {"NodeID": "n-dirty", "Alive": True, "Region": "us-east",
             "Resources": {"CPU": 8, "GPU": 0}},
        ]
        dirty = CarbonAwareScheduler(
            carbon_forecaster=_FakeForecaster(),
            node_provider=dirty_provider,
        )
        await dirty.update_node_carbon_info()

        # Deferrable=False (string) → should NOT defer
        result = await dirty.should_defer_task(
            {"task_id": "t", "deferrable": "false"},
            defer_threshold_gco2kwh=300.0,
        )
        assert result is False, (
            f"P0 2.4 regression: deferrable='false' deferred={result}")
        print("  ✓ deferrable=\"false\" → not deferred")

        # Deferrable=True (string) → SHOULD defer
        result = await dirty.should_defer_task(
            {"task_id": "t", "deferrable": "true"},
            defer_threshold_gco2kwh=300.0,
        )
        assert result is True, (
            f"P0 2.4 regression: deferrable='true' deferred={result}")
        print("  ✓ deferrable=\"true\" → deferred")

        # ------------------------------------------------------------- #
        # 6. P0 2.5: from_dict picks up new config defaults
        # ------------------------------------------------------------- #
        print("\n=== 6. P0 2.5: from_dict handles new fields ===")
        legacy = {
            "config": {
                "clean_threshold": 100.0,
                "moderate_threshold": 300.0,
                "update_interval_minutes": 5,
                # note: deadline_horizon_minutes omitted (new field)
            },
            "strict": True,
            "nodes": [],
            "history": [],
        }
        restored = CarbonAwareScheduler.from_dict(legacy)
        assert restored.config.clean_threshold == 100.0
        assert restored.config.moderate_threshold == 300.0
        assert restored.config.update_interval_minutes == 5
        assert restored.config.deadline_horizon_minutes == 30  # default
        print("  ✓ new fields default correctly on restore")

        # ------------------------------------------------------------- #
        # 7. P1 2.6: parallel region refresh
        # ------------------------------------------------------------- #
        print("\n=== 7. P1 2.6: parallel region refresh ===")

        class _SlowForecaster:
            def __init__(self, delay_s: float = 0.1) -> None:
                self._delay = delay_s

            async def get_current_intensity(self, region: str) -> float:
                await asyncio.sleep(self._delay)
                return _REGION_INTENSITY.get(region, 300.0)

        parallel = CarbonAwareScheduler(
            carbon_forecaster=_SlowForecaster(delay_s=0.1),
            node_provider=_node_provider_sync,   # 3 distinct regions
        )
        t0 = time.perf_counter()
        await parallel.update_node_carbon_info()
        elapsed = time.perf_counter() - t0
        # Sequential: 3 × 0.1s = 0.3s. Parallel: ~0.1s.
        assert elapsed < 0.2, (
            f"P1 2.6 regression: refresh took {elapsed:.3f}s "
            f"(expected <0.2s with parallel fetch)")
        print(f"  ✓ refreshed 3 regions in {elapsed:.3f}s "
              f"(sequential would be 0.3s)")

        # ------------------------------------------------------------- #
        # 8. P1 2.7: node_info is encapsulated
        # ------------------------------------------------------------- #
        print("\n=== 8. P1 2.7: node_info snapshots ===")
        snap = scheduler.get_node_info("node-eu-west-0001")
        assert snap is not None
        original_cpus = snap["available_cpus"]
        snap["available_cpus"] = 9999
        snap["carbon_intensity"] = 0.0
        # Registry-side node must be unchanged
        fresh = scheduler.get_node_info("node-eu-west-0001")
        assert fresh["available_cpus"] == original_cpus, (
            f"P1 2.7 regression: internal node_info was mutated "
            f"through snapshot")
        assert fresh["carbon_intensity"] > 0
        print(f"  ✓ snapshot mutation isolated; internal cpus={original_cpus}")

        # list_nodes returns snapshots too
        all_nodes = scheduler.list_nodes()
        assert all(isinstance(n, dict) for n in all_nodes)
        print(f"  ✓ list_nodes returned {len(all_nodes)} dict(s)")

        # ------------------------------------------------------------- #
        # 9. P1 3.1: single-flight refresh
        # ------------------------------------------------------------- #
        print("\n=== 9. P1 3.1: single-flight refresh ===")

        counting_forecaster = _FakeForecaster(async_mode=False)
        single_flight = CarbonAwareScheduler(
            carbon_forecaster=counting_forecaster,
            config=CarbonSchedulerConfig(update_interval_minutes=1),
            node_provider=_node_provider_sync,
        )
        # No initial refresh; 5 concurrent schedule_task calls each trigger
        # _maybe_refresh. Single-flight ensures only 1 refresh runs.
        tasks = [
            single_flight.schedule_task(
                {"task_id": f"sf-{i}"}, 0.001)
            for i in range(5)
        ]
        await asyncio.gather(*tasks)
        # 3 unique regions → 3 forecaster calls if single-flight worked;
        # 5×3 = 15 if not.
        assert counting_forecaster.call_count == 3, (
            f"P1 3.1 regression: forecaster called "
            f"{counting_forecaster.call_count} times (expected 3)")
        print(f"  ✓ 5 concurrent schedules → "
              f"{counting_forecaster.call_count} forecaster call(s)")

        # ------------------------------------------------------------- #
        # 10. P1 3.2: strict zip mismatch
        # ------------------------------------------------------------- #
        print("\n=== 10. P1 3.2: zip length mismatch (strict) ===")

        class _ShortClusterManager:
            async def execute_distributed_tasks(
                self, tasks, agent_type="retriever"):
                # Return 1 result for N tasks
                return [{"task_id": "only-one"}]

        # Strict → raises
        strict_cluster = CarbonAwareRayCluster(
            cluster_manager=_ShortClusterManager(),
            scheduler=CarbonAwareScheduler(
                carbon_forecaster=_FakeForecaster(),
                node_provider=_node_provider_sync,
            ),
            strict=True,
        )
        await strict_cluster.update_node_info()
        try:
            await strict_cluster.schedule_and_execute(
                tasks=[{"task_id": f"z-{i}"} for i in range(3)],
                task_energy_estimate_kwh=0.001,
            )
        except CarbonSchedulerError as exc:
            print(f"  ✓ strict raises: {exc}")
        else:
            raise AssertionError("P1 3.2 regression: expected raise")

        # Lenient → returns truncated list with warning
        lenient_cluster = CarbonAwareRayCluster(
            cluster_manager=_ShortClusterManager(),
            scheduler=CarbonAwareScheduler(
                carbon_forecaster=_FakeForecaster(),
                node_provider=_node_provider_sync,
            ),
            strict=False,
        )
        await lenient_cluster.update_node_info()
        results = await lenient_cluster.schedule_and_execute(
            tasks=[{"task_id": f"z-{i}"} for i in range(3)],
            task_energy_estimate_kwh=0.001,
        )
        assert len(results) == 1
        print(f"  ✓ lenient returns {len(results)} result(s) with warning")

        # ------------------------------------------------------------- #
        # 11. P1 3.3: metrics_hook
        # ------------------------------------------------------------- #
        print("\n=== 11. P1 3.3: metrics_hook ===")
        events: List[Tuple[str, Mapping[str, Any]]] = []
        m = CarbonAwareScheduler(
            carbon_forecaster=_FakeForecaster(),
            node_provider=_node_provider_sync,
            metrics_hook=lambda ev, f: events.append((ev, f)),
        )
        await m.update_node_carbon_info()
        await m.schedule_task({"task_id": "m-1"}, 0.001)
        names = {ev for ev, _ in events}
        assert "node_refreshed" in names
        assert "refresh_cycle" in names
        assert "task_scheduled" in names
        print(f"  ✓ captured events: {sorted(names)}")

        # ------------------------------------------------------------- #
        # 12. P1 3.4: close()
        # ------------------------------------------------------------- #
        print("\n=== 12. P1 3.4: close() ===")
        m.close()
        m.close()  # idempotent
        assert m._closed is True
        print("  ✓ close() is idempotent")

        # ------------------------------------------------------------- #
        # 13. P1 3.5: context-manager lock (no deadlock under threads)
        # ------------------------------------------------------------- #
        print("\n=== 13. P1 3.5: context managers locked ===")

        def _scope(reg: CarbonAwareScheduler) -> None:
            with reg:
                time.sleep(0.02)

        threaded = CarbonAwareScheduler()
        threads = [
            threading.Thread(target=_scope, args=(threaded,))
            for _ in range(4)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        print("  ✓ 4 concurrent scopes completed without deadlock")

        # ------------------------------------------------------------- #
        # 14. P1 3.6: SchedulingReason enum
        # ------------------------------------------------------------- #
        print("\n=== 14. P1 3.6: SchedulingReason enum ===")
        assert SchedulingReason.LOWEST_CARBON.value == "lowest_carbon"
        assert SchedulingReason.LEAST_LOADED_TIEBREAK.value == \
            "least_loaded_tiebreak"
        assert SchedulingReason.ONLY_NODE_AVAILABLE.value == \
            "only_node_available"
        # Decisions carry reason_code
        for d in scheduler.scheduling_history:
            assert d.reason_code, "decision missing reason_code"
        print(f"  ✓ enum values + decision.reason_code populated")

        # ------------------------------------------------------------- #
        # 15. P1 3.7: statistics(window=N)
        # ------------------------------------------------------------- #
        print("\n=== 15. P1 3.7: statistics(window=N) ===")
        all_stats = scheduler.get_statistics()
        win_stats = scheduler.get_statistics(window=2)
        print(f"  all     : {all_stats['total_tasks_scheduled']} tasks, "
              f"saved={all_stats['carbon_saved_kgco2e']:.6f}")
        print(f"  window=2: {win_stats['total_tasks_scheduled']} tasks, "
              f"saved={win_stats['carbon_saved_kgco2e']:.6f}")
        assert win_stats["total_tasks_scheduled"] == 2
        assert win_stats["carbon_saved_kgco2e"] <= \
            all_stats["carbon_saved_kgco2e"] + 1e-9

        # ------------------------------------------------------------- #
        # 16. P1 3.8: imminent deadline prevents defer
        # ------------------------------------------------------------- #
        print("\n=== 16. P1 3.8: imminent deadline prevents defer ===")

        async def _imminent() -> str:
            return (
                datetime.now(timezone.utc) + timedelta(minutes=5)
            ).isoformat()

        deadline_str = await _imminent()
        # All dirty, deferrable, but imminent deadline → NOT deferred
        should_defer = await dirty.should_defer_task(
            {"task_id": "d-1", "deferrable": True,
             "deadline": deadline_str},
            defer_threshold_gco2kwh=300.0,
        )
        assert should_defer is False, (
            f"P1 3.8 regression: imminent deadline deferred={should_defer}")
        print(f"  ✓ imminent deadline → not deferred "
              f"(deadline={deadline_str[:19]})")

        # Far deadline → deferred
        far_str = (
            datetime.now(timezone.utc) + timedelta(hours=4)
        ).isoformat()
        should_defer = await dirty.should_defer_task(
            {"task_id": "d-2", "deferrable": True, "deadline": far_str},
            defer_threshold_gco2kwh=300.0,
        )
        assert should_defer is True
        print("  ✓ far deadline → deferred")

        # ------------------------------------------------------------- #
        # 17. P1 3.9: require_gpus filter
        # ------------------------------------------------------------- #
        print("\n=== 17. P1 3.9: require_gpus filter ===")

        def _gpu_nodes() -> List[Dict[str, Any]]:
            return [
                {"NodeID": "cpu-only", "Alive": True, "Region": "eu-west",
                 "Resources": {"CPU": 8, "GPU": 0}},
                {"NodeID": "gpu-1", "Alive": True, "Region": "eu-west",
                 "Resources": {"CPU": 8, "GPU": 1}},
                {"NodeID": "gpu-2", "Alive": True, "Region": "eu-west",
                 "Resources": {"CPU": 8, "GPU": 2}},
            ]

        gpu_sched = CarbonAwareScheduler(
            carbon_forecaster=_FakeForecaster(),
            node_provider=_gpu_nodes,
        )
        await gpu_sched.update_node_carbon_info()

        # require_gpus=0 → any node OK
        d = await gpu_sched.schedule_task({"task_id": "gpu-0"}, 0.001)
        print(f"  ✓ require_gpus=0 → {d.node_id}")

        # require_gpus=2 → only gpu-2
        d = await gpu_sched.schedule_task(
            {"task_id": "gpu-2"}, 0.001, require_gpus=2)
        assert d.node_id == "gpu-2", (
            f"P1 3.9 regression: got {d.node_id}, expected gpu-2")
        print(f"  ✓ require_gpus=2 → {d.node_id} (only node with 2 GPUs)")

        # require_gpus=5 → no node qualifies → raises
        try:
            await gpu_sched.schedule_task(
                {"task_id": "gpu-5"}, 0.001, require_gpus=5)
        except CarbonSchedulerError as exc:
            print(f"  ✓ require_gpus=5 rejected: {exc}")
        else:
            raise AssertionError("Expected error for require_gpus=5")

        # ------------------------------------------------------------- #
        # 18. P2 3.12: Stub failure injection
        # ------------------------------------------------------------- #
        print("\n=== 18. P2 3.12: Stub failure injection ===")

        async def _stub_delay() -> None:
            stub = _StubClusterManager(delay_s=0.05)
            t0 = time.perf_counter()
            await stub.execute_distributed_tasks(
                [{"task_id": "s-1"}], "retriever")
            elapsed = time.perf_counter() - t0
            assert elapsed >= 0.04
            print(f"  ✓ delay_s honoured ({elapsed:.3f}s)")

        await _stub_delay()

        async def _stub_fail() -> None:
            stub = _StubClusterManager(fail_on="boom")
            try:
                await stub.execute_distributed_tasks(
                    [{"task_id": "boom"}], "retriever")
            except RuntimeError as exc:
                print(f"  ✓ fail_on triggered: {exc}")
            else:
                raise AssertionError("Expected stub failure")

        await _stub_fail()

        # ------------------------------------------------------------- #
        # 19. P2 3.13: factory node_provider
        # ------------------------------------------------------------- #
        print("\n=== 19. P2 3.13: factory node_provider ===")
        cluster = create_carbon_aware_cluster(
            num_workers=2,
            carbon_forecaster=_FakeForecaster(),
            node_provider=_node_provider_sync,
        )
        n = await cluster.update_node_info()
        assert n == 3, f"expected 3 nodes via factory, got {n}"
        results = await cluster.schedule_and_execute(
            tasks=[{"task_id": "f-1"}, {"task_id": "f-2"}],
            task_energy_estimate_kwh=0.001,
        )
        assert len(results) == 2
        assert "carbon_scheduling" in results[0]
        print(f"  ✓ factory injected node_provider; "
              f"{len(results)} result(s) with scheduling metadata")

        # ------------------------------------------------------------- #
        # 20. P2 3.17: intensity_series
        # ------------------------------------------------------------- #
        print("\n=== 20. P2 3.17: intensity_series ===")
        series = scheduler.intensity_series()
        assert len(series) == len(scheduler.scheduling_history)
        all_ts_ordered = all(
            series[i][0] <= series[i + 1][0]
            for i in range(len(series) - 1))
        assert all_ts_ordered, "series timestamps not monotonic"
        print(f"  ✓ {len(series)} point(s), timestamps monotonic")
        recent = scheduler.intensity_series(window_s=60.0)
        print(f"  ✓ window_s=60 returned {len(recent)} point(s)")

        # ------------------------------------------------------------- #
        # 21. Validation
        # ------------------------------------------------------------- #
        print("\n=== 21. Validation ===")
        # Energy
        for bad in (float("nan"), float("inf"), -1.0, "not-a-number"):
            try:
                await scheduler.schedule_task({"task_id": "bad"}, bad)
            except CarbonSchedulerError as exc:
                print(f"  ✓ rejected energy {bad!r}: {exc}")
            else:
                raise AssertionError(f"Expected energy rejection {bad!r}")

        # Task
        try:
            await scheduler.schedule_task("not-a-mapping", 0.001)
        except CarbonSchedulerError as exc:
            print(f"  ✓ rejected non-mapping task: {exc}")
        else:
            raise AssertionError("Expected task rejection")

        # Config: bool masquerading as int
        try:
            CarbonSchedulerConfig(update_interval_minutes=True)
        except CarbonSchedulerError as exc:
            print(f"  ✓ rejected bool-as-int: {exc}")
        else:
            raise AssertionError("Expected bool rejection")

        # Config: invalid thresholds
        for bad_cfg in (
            dict(clean_threshold=0),
            dict(clean_threshold=300, moderate_threshold=200),
            dict(update_interval_minutes=0),
            dict(max_history=0),
        ):
            try:
                CarbonSchedulerConfig(**bad_cfg)
            except CarbonSchedulerError as exc:
                print(f"  ✓ rejected config {bad_cfg}: {exc}")
            else:
                raise AssertionError(
                    f"Expected config rejection: {bad_cfg!r}")

        # Carbon saved validation (P2 3.16)
        try:
            SchedulingDecision(
                node_id="n", node_region="r",
                carbon_intensity=100.0, reason="test",
                estimated_carbon_kgco2e=0.1,
                carbon_saved_kgco2e=float("nan"),
            )
        except CarbonSchedulerError as exc:
            print(f"  ✓ rejected NaN carbon_saved: {exc}")
        else:
            raise AssertionError("Expected carbon_saved rejection")

        # ------------------------------------------------------------- #
        # 22. Serialization round-trip
        # ------------------------------------------------------------- #
        print("\n=== 22. Serialization round-trip ===")
        payload = scheduler.to_json()
        restored = CarbonAwareScheduler.from_json(
            payload, carbon_forecaster=_FakeForecaster())
        assert restored.to_dict() == scheduler.to_dict()
        print("  ✓ JSON round-trip OK")

        # ------------------------------------------------------------- #
        # 23. Sync + async context managers
        # ------------------------------------------------------------- #
        print("\n=== 23. Context managers ===")
        with CarbonAwareScheduler(
            node_provider=_node_provider_sync,
        ) as scoped:
            await scoped.update_node_carbon_info()
            await scoped.schedule_task({"task_id": "ctx-t"}, 0.001)
        print("  ✓ sync context OK")

        async with CarbonAwareScheduler(
            node_provider=_node_provider_sync,
        ) as scoped:
            await scoped.update_node_carbon_info()
            await scoped.schedule_task({"task_id": "actx-t"}, 0.001)
        print("  ✓ async context OK")

        print("\n✓ All self-tests passed.")

    asyncio.run(main())
