# src/dpq/carbon_intensity_monitor.py

"""
Carbon Intensity Monitor — v2.0.0
==================================

Real-time carbon intensity tracker with zone prediction.

v2.0.0 changes vs. v1.0.0
-------------------------
**P0 bug fixes**
  * 2.1 Callbacks now fire on the very first fetch *and* on zone changes.
        Previously ``_zone_changed()`` was checked against the value that
        had just been written, so callbacks were dead code.
  * 2.2 ``from_dict`` delegates to ``CarbonMonitorConfig.from_legacy_dict``
        and (when available) ``DPQConfig.from_legacy_dict`` so new fields
        default correctly.
  * 2.3 ``CarbonIntensityUpdate.from_dict`` normalises zone strings
        (case/whitespace) and falls back to ``CarbonZone.RED`` with a
        warning on unknown values instead of raising ``ValueError``.
  * 2.4 ``__getstate__`` / ``__setstate__`` make the monitor picklable.

**P1 robustness / observability**
  * 2.5 Configurable ``fallback_intensity_gco2_kwh`` (was hard-coded 400.0)
        and a ``from_fallback`` flag on ``CarbonIntensityUpdate`` so callers
        can tell fabricated values from real ones.
  * 3.1 Single-flight fetch: concurrent ``get_current_intensity`` calls for
        the same region share one in-flight task.
  * 3.2 Fetch statistics (``total_fetches``, ``total_failures``,
        ``consecutive_failures``, …) surfaced via ``statistics()``.
  * 3.3 ``metrics_hook(event, fields)`` for Prometheus / OTel.
  * 3.4 ``__aenter__`` starts the loop; ``__aexit__`` stops it.
  * 3.5 ``close()`` for sync callers.
  * 3.6 ``forecast_15min`` / ``forecast_60min`` are validated for
        NaN / Inf / < 0.
  * 3.7 Configurable ``monitor_regions`` for the background loop.
  * 3.8 LRU cache eviction (was FIFO).
  * 3.10 ``register_callback`` deduplicates.
  * 3.15 ``statistics()`` reads all state under the lock.

**P2 enhancements**
  * 3.9 Per-callback failure tracking.
  * 3.11 ``start(warm=True)`` warms the cache before returning.
  * 3.12 Unrecognised fetcher return types are logged.
  * 3.14 ``from_dict`` warns when the payload was built with a fetcher
         (which cannot be restored).

Thread-safe, serializable, picklable, bounded in memory.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import threading
import time
from collections import OrderedDict, deque
from dataclasses import asdict, dataclass, field, fields
from datetime import datetime, timezone
from typing import (
    Any, Callable, Deque, Dict, List, Mapping, Optional, Tuple,
)

from .models import CarbonZone, DPQConfig

logger = logging.getLogger(__name__)

__version__ = "2.0.0"


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class CarbonMonitorError(ValueError):
    """Raised for invalid inputs, configuration, or fetch failures."""


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _dpq_config_from_dict(data: Mapping[str, Any]) -> DPQConfig:
    """
    Reconstruct a ``DPQConfig`` from a payload (P0 2.2 for the nested config).

    Prefers a ``from_legacy_dict`` classmethod if one exists (the sibling
    modules provide one); otherwise introspects the dataclass fields;
    finally falls back to the v1.0.0 hardcoded keys.
    """
    from_legacy = getattr(DPQConfig, "from_legacy_dict", None)
    if callable(from_legacy):
        return from_legacy(dict(data or {}))
    try:
        valid = {f.name for f in fields(DPQConfig)}
        filtered = {k: v for k, v in (data or {}).items() if k in valid}
        return DPQConfig(**filtered)
    except Exception:  # noqa: BLE001 — defensive
        return DPQConfig(
            green_threshold=float(data.get("green_threshold", 50.0)),
            yellow_threshold=float(data.get("yellow_threshold", 200.0)),
            red_threshold=float(data.get("red_threshold", 400.0)),
        )


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class CarbonMonitorConfig:
    """Tunable parameters for the carbon intensity monitor."""

    update_interval_seconds: int = 900
    cache_ttl_seconds: int = 1800
    prediction_horizon_minutes: int = 60
    max_cache_entries: int = 1000
    max_callbacks: int = 32
    fetch_max_retries: int = 2
    fetch_base_backoff_seconds: float = 1.0
    fetch_timeout_seconds: float = 15.0
    loop_error_sleep_seconds: float = 60.0

    # P1 2.5: configurable fallback (was hard-coded 400.0).
    fallback_intensity_gco2_kwh: float = 400.0

    # P1 3.7: which regions the background loop refreshes.
    monitor_regions: Tuple[str, ...] = ("default",)

    def __post_init__(self) -> None:
        for name in (
            "update_interval_seconds",
            "cache_ttl_seconds",
            "prediction_horizon_minutes",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) \
                    or value <= 0:
                raise CarbonMonitorError(
                    f"{name} must be a positive int.")
        if isinstance(self.max_cache_entries, bool) or \
                not isinstance(self.max_cache_entries, int) or \
                self.max_cache_entries <= 0:
            raise CarbonMonitorError("max_cache_entries must be > 0.")
        if isinstance(self.max_callbacks, bool) or \
                not isinstance(self.max_callbacks, int) or \
                self.max_callbacks <= 0:
            raise CarbonMonitorError("max_callbacks must be > 0.")
        if isinstance(self.fetch_max_retries, bool) or \
                not isinstance(self.fetch_max_retries, int) or \
                self.fetch_max_retries < 0:
            raise CarbonMonitorError(
                "fetch_max_retries must be >= 0.")
        if self.fetch_base_backoff_seconds < 0:
            raise CarbonMonitorError(
                "fetch_base_backoff_seconds must be >= 0.")
        if self.fetch_timeout_seconds <= 0:
            raise CarbonMonitorError("fetch_timeout_seconds must be > 0.")
        if self.loop_error_sleep_seconds <= 0:
            raise CarbonMonitorError(
                "loop_error_sleep_seconds must be > 0.")
        if isinstance(self.fallback_intensity_gco2_kwh, bool) or \
                not isinstance(self.fallback_intensity_gco2_kwh,
                               (int, float)):
            raise CarbonMonitorError(
                "fallback_intensity_gco2_kwh must be numeric.")
        if self.fallback_intensity_gco2_kwh < 0:
            raise CarbonMonitorError(
                "fallback_intensity_gco2_kwh must be >= 0.")
        if not self.monitor_regions:
            raise CarbonMonitorError("monitor_regions must be non-empty.")
        for r in self.monitor_regions:
            if not isinstance(r, str) or not r:
                raise CarbonMonitorError(
                    "monitor_regions must contain non-empty strings.")

    @classmethod
    def from_legacy_dict(
        cls, cfg: Optional[Mapping[str, Any]]
    ) -> "CarbonMonitorConfig":
        """Build from a flat mapping; unknown keys ignored (P0 2.2)."""
        cfg = dict(cfg or {})
        valid = {f.name for f in fields(cls)}
        kwargs: Dict[str, Any] = {
            k: v for k, v in cfg.items() if k in valid}
        # JSON round-trips tuples through lists — restore the tuple.
        mr = kwargs.get("monitor_regions")
        if isinstance(mr, list):
            kwargs["monitor_regions"] = tuple(mr)
        return cls(**kwargs)


# --------------------------------------------------------------------------- #
# Update record
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class CarbonIntensityUpdate:
    """Immutable snapshot of a carbon intensity reading."""

    timestamp: datetime
    intensity_gco2_kwh: float
    zone: CarbonZone
    region: str
    forecast_15min: Optional[float] = None
    forecast_60min: Optional[float] = None
    # P1 2.5: caller can distinguish fabricated values from real ones.
    from_fallback: bool = False

    def __post_init__(self) -> None:
        # Primary intensity.
        if isinstance(self.intensity_gco2_kwh, bool) or \
                not isinstance(self.intensity_gco2_kwh, (int, float)):
            raise CarbonMonitorError(
                "intensity_gco2_kwh must be numeric.")
        if math.isnan(self.intensity_gco2_kwh) or math.isinf(
            self.intensity_gco2_kwh
        ):
            raise CarbonMonitorError(
                "intensity_gco2_kwh must be finite.")
        if self.intensity_gco2_kwh < 0:
            raise CarbonMonitorError(
                "intensity_gco2_kwh must be >= 0.")

        # P1 3.6: forecast validation.
        for name in ("forecast_15min", "forecast_60min"):
            value = getattr(self, name)
            if value is None:
                continue
            if isinstance(value, bool) or \
                    not isinstance(value, (int, float)):
                raise CarbonMonitorError(
                    f"{name} must be numeric or None.")
            if math.isnan(value) or math.isinf(value):
                raise CarbonMonitorError(f"{name} must be finite.")
            if value < 0:
                raise CarbonMonitorError(f"{name} must be >= 0.")

        # Zone must be the enum.
        if not isinstance(self.zone, CarbonZone):
            raise CarbonMonitorError(
                "zone must be a CarbonZone instance.")

        if not isinstance(self.region, str) or not self.region:
            raise CarbonMonitorError(
                "region must be a non-empty string.")

        if not isinstance(self.timestamp, datetime):
            raise CarbonMonitorError(
                "timestamp must be a datetime.")
        if not self.timestamp.tzinfo:
            object.__setattr__(
                self, "timestamp",
                self.timestamp.replace(tzinfo=timezone.utc),
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp.isoformat(),
            "intensity_gco2_kwh": self.intensity_gco2_kwh,
            "zone": self.zone.value,
            "region": self.region,
            "forecast_15min": self.forecast_15min,
            "forecast_60min": self.forecast_60min,
            "from_fallback": self.from_fallback,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "CarbonIntensityUpdate":
        if not isinstance(data, Mapping):
            raise CarbonMonitorError(
                f"CarbonIntensityUpdate.from_dict expects a Mapping, "
                f"got {type(data).__name__}.")

        ts = datetime.fromisoformat(str(data["timestamp"]))
        if not ts.tzinfo:
            ts = ts.replace(tzinfo=timezone.utc)

        # P0 2.3: robust zone deserialization.
        raw_zone = data["zone"]
        if isinstance(raw_zone, CarbonZone):
            zone = raw_zone
        else:
            normalized = str(raw_zone).strip().lower()
            try:
                zone = CarbonZone(normalized)
            except ValueError:
                logger.warning(
                    "Unknown carbon zone %r in serialized update; "
                    "defaulting to RED.", raw_zone)
                zone = CarbonZone.RED

        return cls(
            timestamp=ts,
            intensity_gco2_kwh=float(data["intensity_gco2_kwh"]),
            zone=zone,
            region=str(data.get("region", "default")),
            forecast_15min=data.get("forecast_15min"),
            forecast_60min=data.get("forecast_60min"),
            from_fallback=bool(data.get("from_fallback", False)),
        )


# --------------------------------------------------------------------------- #
# Monitor
# --------------------------------------------------------------------------- #
class CarbonIntensityMonitor:
    """
    Real-time carbon intensity tracker with zone prediction.

    Thread-safe, serializable, picklable, bounded in memory.
    """

    def __init__(
        self,
        carbon_forecaster_url: str,
        update_interval_seconds: int = 900,
        cache_ttl_seconds: int = 1800,
        prediction_horizon_minutes: int = 60,
        *,
        config: Optional[CarbonMonitorConfig] = None,
        dpq_config: Optional[DPQConfig] = None,
        strict: bool = True,
        fetcher: Optional[Callable[[str], Any]] = None,
        metrics_hook: Optional[
            Callable[[str, Mapping[str, Any]], None]
        ] = None,
    ) -> None:
        if not isinstance(carbon_forecaster_url, str):
            raise CarbonMonitorError(
                "carbon_forecaster_url must be a string.")

        if config is not None:
            self._config = config
        else:
            self._config = CarbonMonitorConfig(
                update_interval_seconds=update_interval_seconds,
                cache_ttl_seconds=cache_ttl_seconds,
                prediction_horizon_minutes=prediction_horizon_minutes,
            )

        self._dpq_config = dpq_config or DPQConfig()
        self._strict = bool(strict)

        if metrics_hook is not None and not callable(metrics_hook):
            raise CarbonMonitorError("metrics_hook must be callable.")
        self._metrics_hook = metrics_hook

        self.carbon_forecaster_url: str = carbon_forecaster_url
        # Legacy attributes preserved.
        self.update_interval: int = self._config.update_interval_seconds
        self.cache_ttl: int = self._config.cache_ttl_seconds
        self.prediction_horizon: int = \
            self._config.prediction_horizon_minutes

        self._lock = threading.RLock()
        # P1 3.8: OrderedDict gives us LRU semantics.
        self._cache: "OrderedDict[str, CarbonIntensityUpdate]" = \
            OrderedDict()
        self._callbacks: List[Callable[[CarbonIntensityUpdate], Any]] = []
        self._callback_stats: Dict[str, Dict[str, Any]] = {}
        self._running: bool = False
        self._tasks: List[asyncio.Task] = []
        self._ctx_start: Optional[float] = None
        self._fetcher = fetcher

        # P1 3.1: single-flight map.
        self._inflight: Dict[str, asyncio.Task] = {}

        # P1 3.2: fetch observability.
        self._fetch_stats: Dict[str, Any] = {
            "total_fetches": 0,
            "total_successes": 0,
            "total_failures": 0,
            "total_fallbacks": 0,
            "last_success_at": None,
            "last_failure_at": None,
            "consecutive_failures": 0,
        }

        logger.debug(
            "CarbonIntensityMonitor v%s initialized "
            "(url=%s, interval=%ds, cache_ttl=%ds, strict=%s, "
            "fallback=%s)",
            __version__,
            self.carbon_forecaster_url,
            self.update_interval,
            self.cache_ttl,
            self._strict,
            self._config.fallback_intensity_gco2_kwh,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> CarbonMonitorConfig:
        return self._config

    @property
    def running(self) -> bool:
        return self._running

    @property
    def cache_size(self) -> int:
        with self._lock:
            return len(self._cache)

    # ---------------------------------------------------------- metrics
    def _emit_metric(
        self, event: str, fields_: Mapping[str, Any],
    ) -> None:
        if self._metrics_hook is None:
            return
        try:
            self._metrics_hook(event, dict(fields_))
        except Exception:  # noqa: BLE001
            logger.debug("metrics_hook raised", exc_info=True)

    # ---------------------------------------------------------- lifecycle
    async def start(self, *, warm: bool = False) -> None:
        """
        Start the monitoring loop.

        ``warm=True`` (P2 3.11) performs an initial fetch for each configured
        region before returning so the cache is populated on return.
        """
        if self._running:
            return
        self._running = True
        self._tasks = [
            asyncio.create_task(
                self._monitoring_loop(), name="dpq:carbon")
        ]
        if warm:
            for region in self._config.monitor_regions:
                try:
                    await self.get_current_intensity(region)
                except Exception as exc:  # noqa: BLE001
                    logger.warning(
                        "Initial fetch for region %r failed during "
                        "start(warm=True): %s", region, exc)
        logger.info("CarbonIntensityMonitor started.")

    async def stop(self, *, timeout_seconds: float = 5.0) -> None:
        """Stop the monitoring loop."""
        if not self._running:
            return
        self._running = False
        for t in self._tasks:
            t.cancel()
        if self._tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._tasks, return_exceptions=True),
                    timeout=timeout_seconds,
                )
            except asyncio.TimeoutError:  # pragma: no cover
                logger.warning(
                    "Timed out waiting for carbon loop to stop.")
        self._tasks = []
        logger.info("CarbonIntensityMonitor stopped.")

    def close(self) -> None:
        """Best-effort sync close (P1 3.5). Idempotent."""
        if not self._running:
            return
        self._running = False
        for t in self._tasks:
            t.cancel()
        self._tasks = []
        logger.info("CarbonIntensityMonitor closed (sync).")

    # ---------------------------------------------------------- callbacks
    def register_callback(
        self, callback: Callable[[CarbonIntensityUpdate], Any]
    ) -> None:
        """Register a callback invoked on zone changes (P1 3.10 dedupes)."""
        if not callable(callback):
            raise CarbonMonitorError("callback must be callable.")
        with self._lock:
            if callback in self._callbacks:
                logger.debug(
                    "Callback %r already registered; skipping.",
                    getattr(callback, "__name__", callback))
                return
            if len(self._callbacks) >= self._config.max_callbacks:
                raise CarbonMonitorError(
                    f"max_callbacks={self._config.max_callbacks} "
                    f"reached.")
            self._callbacks.append(callback)
            self._callback_stats[
                getattr(callback, "__name__", repr(callback))
            ] = {
                "successes": 0,
                "failures": 0,
                "last_error": None,
            }
        logger.debug(
            "Registered carbon callback: %s",
            getattr(callback, "__name__", callback),
        )

    def unregister_callback(
        self, callback: Callable[[CarbonIntensityUpdate], Any]
    ) -> bool:
        """Remove a previously registered callback."""
        with self._lock:
            try:
                self._callbacks.remove(callback)
                self._callback_stats.pop(
                    getattr(callback, "__name__", repr(callback)),
                    None)
                return True
            except ValueError:
                return False

    async def _dispatch_callbacks(
        self, update: CarbonIntensityUpdate,
    ) -> None:
        with self._lock:
            callbacks = list(self._callbacks)
        for cb in callbacks:
            name = getattr(cb, "__name__", repr(cb))
            try:
                result = cb(update)
                if asyncio.iscoroutine(result):
                    await result
                with self._lock:
                    stats = self._callback_stats.get(name)
                    if stats is not None:
                        stats["successes"] += 1
            except Exception as exc:  # noqa: BLE001
                logger.error(
                    "Carbon callback %r failed: %s", name, exc)
                with self._lock:
                    stats = self._callback_stats.get(name)
                    if stats is not None:
                        stats["failures"] += 1
                        stats["last_error"] = str(exc)
                self._emit_metric("callback_failed", {
                    "callback": name,
                    "error": str(exc),
                })

    # ---------------------------------------------------------- public API
    async def get_current_intensity(
        self, region: str = "default"
    ) -> CarbonIntensityUpdate:
        """Return the current intensity for ``region`` (cached when fresh)."""
        if not isinstance(region, str) or not region:
            raise CarbonMonitorError(
                "region must be a non-empty string.")

        cache_key = f"{region}_current"

        # Fast path: fresh cache hit.
        with self._lock:
            cached = self._cache.get(cache_key)
            if cached is not None:
                age = (
                    datetime.now(timezone.utc) - cached.timestamp
                ).total_seconds()
                if age < self.cache_ttl:
                    # P1 3.8: refresh LRU position.
                    self._cache.move_to_end(cache_key)
                    return cached

        # P1 3.1: single-flight.
        loop = asyncio.get_running_loop()
        with self._lock:
            task = self._inflight.get(cache_key)
            if task is None:
                task = loop.create_task(
                    self._fetch_and_store(region, cache_key))
                task.add_done_callback(
                    lambda t, k=cache_key: self._clear_inflight(k, t))
                self._inflight[cache_key] = task

        return await task

    async def _fetch_and_store(
        self, region: str, cache_key: str,
    ) -> CarbonIntensityUpdate:
        """Fetch, write to cache, and fire callbacks on zone change."""
        # Capture prior state so a zone change is detectable.
        with self._lock:
            prior = self._cache.get(cache_key)

        update = await self._fetch_from_api(region)

        # Write to cache under the lock.
        with self._lock:
            self._cache[cache_key] = update
            self._cache.move_to_end(cache_key)
            while len(self._cache) > self._config.max_cache_entries:
                self._cache.popitem(last=False)

        # P0 2.1: correct zone-change detection.
        zone_changed = (prior is None) or (prior.zone != update.zone)
        if zone_changed:
            self._emit_metric("zone_changed", {
                "region": region,
                "old_zone": prior.zone.value if prior else None,
                "new_zone": update.zone.value,
                "intensity": update.intensity_gco2_kwh,
            })
            await self._dispatch_callbacks(update)

        return update

    def _clear_inflight(
        self, cache_key: str, task: asyncio.Task,
    ) -> None:
        """P1 3.1: clear the single-flight entry and suppress warnings."""
        with self._lock:
            if self._inflight.get(cache_key) is task:
                del self._inflight[cache_key]
        # Mark the task's exception retrieved to avoid the
        # "Task exception was never retrieved" warning.
        if not task.cancelled():
            try:
                task.exception()
            except Exception:  # noqa: BLE001
                pass

    async def _fetch_from_api(
        self, region: str,
    ) -> CarbonIntensityUpdate:
        """Fetch carbon intensity for ``region``."""
        if self._fetcher is None:
            # No fetcher → immediate fallback.
            return self._make_fallback(region)

        last_exc: Optional[BaseException] = None
        for attempt in range(1 + self._config.fetch_max_retries):
            try:
                result = self._fetcher(region)
                if asyncio.iscoroutine(result):
                    result = await asyncio.wait_for(
                        result,
                        timeout=self._config.fetch_timeout_seconds,
                    )
                update = self._coerce_fetch_result(result, region)
                self._record_fetch_success(update)
                return update
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                logger.warning(
                    "Fetch attempt %d/%d for region %r failed: %s",
                    attempt + 1,
                    self._config.fetch_max_retries + 1,
                    region,
                    exc,
                )
                self._emit_metric("fetch_failed", {
                    "region": region,
                    "attempt": attempt + 1,
                    "reason": type(exc).__name__,
                })
                if attempt < self._config.fetch_max_retries:
                    await asyncio.sleep(
                        self._config.fetch_base_backoff_seconds
                        * (2 ** attempt))

        self._record_fetch_failure()
        if self._strict:
            raise CarbonMonitorError(
                f"All fetch attempts failed for region '{region}': "
                f"{type(last_exc).__name__}: {last_exc}"
            ) from last_exc
        return self._make_fallback(region)

    def _coerce_fetch_result(
        self, result: Any, region: str,
    ) -> CarbonIntensityUpdate:
        """Convert an arbitrary fetcher result into an update."""
        if isinstance(result, CarbonIntensityUpdate):
            return result
        if isinstance(result, Mapping):
            return CarbonIntensityUpdate.from_dict(result)
        if isinstance(result, (int, float)) and not isinstance(result, bool):
            intensity = float(result)
            return CarbonIntensityUpdate(
                timestamp=datetime.now(timezone.utc),
                intensity_gco2_kwh=intensity,
                zone=self._dpq_config.classify(intensity),
                region=region,
            )
        # P2 3.12: unrecognised type is a failure, not a silent fallback.
        raise CarbonMonitorError(
            f"fetcher returned unrecognised type "
            f"{type(result).__name__} for region {region!r}")

    def _make_fallback(self, region: str) -> CarbonIntensityUpdate:
        """Build a fabricated update marked with ``from_fallback=True``."""
        intensity = self._config.fallback_intensity_gco2_kwh
        self._record_fetch_fallback(intensity)
        self._emit_metric("fetch_fallback", {
            "region": region,
            "intensity": intensity,
        })
        return CarbonIntensityUpdate(
            timestamp=datetime.now(timezone.utc),
            intensity_gco2_kwh=intensity,
            zone=self._dpq_config.classify(intensity),
            region=region,
            from_fallback=True,
        )

    # ---------------------------------------------------------- stats bookkeeping
    def _record_fetch_success(
        self, update: CarbonIntensityUpdate,
    ) -> None:
        now = datetime.now(timezone.utc)
        with self._lock:
            self._fetch_stats["total_fetches"] += 1
            self._fetch_stats["total_successes"] += 1
            self._fetch_stats["last_success_at"] = now
            self._fetch_stats["consecutive_failures"] = 0
        self._emit_metric("fetch_succeeded", {
            "region": update.region,
            "intensity": update.intensity_gco2_kwh,
            "zone": update.zone.value,
        })

    def _record_fetch_failure(self) -> None:
        now = datetime.now(timezone.utc)
        with self._lock:
            self._fetch_stats["total_fetches"] += 1
            self._fetch_stats["total_failures"] += 1
            self._fetch_stats["last_failure_at"] = now
            self._fetch_stats["consecutive_failures"] += 1

    def _record_fetch_fallback(self, intensity: float) -> None:
        with self._lock:
            self._fetch_stats["total_fallbacks"] += 1

    # ---------------------------------------------------------- loop
    async def _monitoring_loop(self) -> None:
        """Background loop for periodic updates (P1 3.7 configurable regions)."""
        while self._running:
            try:
                for region in self._config.monitor_regions:
                    try:
                        await self.get_current_intensity(region)
                    except Exception as exc:  # noqa: BLE001
                        logger.error(
                            "Monitor fetch for region %r failed: %s",
                            region, exc)
                await asyncio.sleep(self.update_interval)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                logger.error("Monitoring loop error: %s", exc)
                await asyncio.sleep(
                    self._config.loop_error_sleep_seconds)

    # ---------------------------------------------------------- stats
    def statistics(self) -> Dict[str, Any]:
        """Return aggregate statistics (P1 3.15: locked read)."""
        with self._lock:
            cache = list(self._cache.values())
            callbacks_count = len(self._callbacks)
            running = self._running
            fetch_stats = dict(self._fetch_stats)
            callback_stats = {
                k: dict(v) for k, v in self._callback_stats.items()
            }

        if not cache:
            return {
                "cache_size": 0,
                "mean_intensity": None,
                "by_zone": {},
                "callbacks": callbacks_count,
                "running": running,
                "fetch_stats": fetch_stats,
                "callback_stats": callback_stats,
            }

        by_zone: Dict[str, int] = {}
        fallback_count = 0
        for u in cache:
            by_zone[u.zone.value] = by_zone.get(u.zone.value, 0) + 1
            if u.from_fallback:
                fallback_count += 1

        return {
            "cache_size": len(cache),
            "mean_intensity":
                sum(u.intensity_gco2_kwh for u in cache) / len(cache),
            "by_zone": by_zone,
            "fallback_count": fallback_count,
            "callbacks": callbacks_count,
            "running": running,
            "fetch_stats": fetch_stats,
            "callback_stats": callback_stats,
        }

    def fetch_health(self) -> Dict[str, Any]:
        """Return fetch health (P1 3.2)."""
        with self._lock:
            return dict(self._fetch_stats)

    def reset(self, *, clear_cache: bool = False) -> None:
        """Reset internal state; optionally clear the cache."""
        with self._lock:
            if clear_cache:
                self._cache.clear()
            self._ctx_start = None
        logger.debug(
            "CarbonIntensityMonitor reset (clear_cache=%s)",
            clear_cache)

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "version": __version__,
                "carbon_forecaster_url": self.carbon_forecaster_url,
                "config": asdict(self._config),
                "dpq_config": asdict(self._dpq_config),
                "strict": self._strict,
                "running": self._running,
                "has_fetcher": self._fetcher is not None,
                "cache": [
                    u.to_dict() for u in self._cache.values()],
                "callbacks": len(self._callbacks),
            }

    @classmethod
    def from_dict(
        cls, data: Mapping[str, Any]
    ) -> "CarbonIntensityMonitor":
        if not isinstance(data, Mapping):
            raise CarbonMonitorError(
                f"from_dict expects a Mapping, "
                f"got {type(data).__name__}.")

        # P0 2.2: delegate to from_legacy_dict.
        cfg = CarbonMonitorConfig.from_legacy_dict(
            data.get("config") or {})
        dpq_cfg = _dpq_config_from_dict(
            data.get("dpq_config") or {})

        monitor = cls(
            carbon_forecaster_url=str(data["carbon_forecaster_url"]),
            config=cfg,
            dpq_config=dpq_cfg,
            strict=bool(data.get("strict", True)),
        )

        # P2 3.14: warn if the payload came from a fetcher-backed monitor.
        if data.get("has_fetcher", False):
            logger.warning(
                "Restoring a monitor that originally had a live "
                "fetcher; the fetcher cannot be serialized — "
                "inject one with `monitor._fetcher = ...` to enable "
                "live fetches.")

        with monitor._lock:
            for u in data.get("cache", []):
                update = CarbonIntensityUpdate.from_dict(u)
                key = f"{update.region}_current"
                monitor._cache[key] = update
            # Apply the same LRU bound that runtime enforces.
            while len(monitor._cache) > cfg.max_cache_entries:
                monitor._cache.popitem(last=False)
        return monitor

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(
        cls, payload: str
    ) -> "CarbonIntensityMonitor":
        try:
            return cls.from_dict(json.loads(payload))
        except json.JSONDecodeError as exc:
            raise CarbonMonitorError(
                f"Invalid JSON payload: {exc}") from exc

    # ---------------------------------------------------------- pickling
    def __getstate__(self) -> Dict[str, Any]:
        # P0 2.4: RLock, in-flight tasks, and async tasks are not
        # picklable. Drop them; the fetcher is also dropped because
        # closures are not picklable in general.
        state = self.__dict__.copy()
        state.pop("_lock", None)
        state.pop("_inflight", None)
        state.pop("_tasks", None)
        state.pop("_fetcher", None)
        state["_running"] = False
        return state

    def __setstate__(self, state: Dict[str, Any]) -> None:
        self.__dict__.update(state)
        self._lock = threading.RLock()
        self._inflight = {}
        self._tasks = []
        self._fetcher = None

    # ---------------------------------------------------------- context mgr
    async def __aenter__(self) -> "CarbonIntensityMonitor":
        # P1 3.4: symmetric with __aexit__ — start the loop.
        self._ctx_start = time.perf_counter()
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        try:
            await self.stop()
        finally:
            elapsed = time.perf_counter() - (
                self._ctx_start
                if self._ctx_start is not None
                else time.perf_counter()
            )
            self._ctx_start = None
            if exc_type is not None:
                logger.warning(
                    "CarbonMonitor scope exited with %s after %.4fs.",
                    exc_type.__name__, elapsed)

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        with self._lock:
            return (
                "CarbonIntensityMonitor("
                f"version={__version__!r}, "
                f"url={self.carbon_forecaster_url!r}, "
                f"cache={len(self._cache)}, "
                f"callbacks={len(self._callbacks)}, "
                f"running={self._running})"
            )


__all__ = [
    "CarbonIntensityMonitor",
    "CarbonIntensityUpdate",
    "CarbonMonitorConfig",
    "CarbonMonitorError",
]


# --------------------------------------------------------------------------- #
# Self-tests: python -m dpq.carbon_intensity_monitor
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    import pickle

    logging.basicConfig(
        level=logging.WARNING,
        format="%(levelname)s %(name)s: %(message)s",
    )

    # ---------------------------------------------------------------- #
    # P0 2.1 — callback fires on first fetch AND on zone change
    # ---------------------------------------------------------------- #
    print("=== P0 2.1: callbacks fire on first fetch and zone change ===")

    async def _test_callbacks() -> None:
        values = iter([40.0, 350.0, 45.0])

        async def sequential_fetcher(region: str) -> float:
            return next(values)

        monitor = CarbonIntensityMonitor(
            carbon_forecaster_url="http://test",
            fetcher=sequential_fetcher,
            config=CarbonMonitorConfig(cache_ttl_seconds=1),
        )

        fired: List[Tuple[str, str]] = []

        def callback(update: CarbonIntensityUpdate) -> None:
            fired.append((update.region, update.zone.value))

        monitor.register_callback(callback)

        # First fetch: prior is None → callback fires (GREEN)
        u1 = await monitor.get_current_intensity("eu-west")
        assert u1.zone == CarbonZone.GREEN, u1.zone
        assert len(fired) == 1, (
            f"P0 2.1 regression: callback did not fire on first "
            f"fetch (fired={fired})")
        assert fired[0] == ("eu-west", "green"), fired[0]
        print(f"  ✓ first fetch ({u1.zone.value}): "
              f"callback fired ({len(fired)})")

        # Force cache miss (simulate TTL expiry) without waiting.
        with monitor._lock:
            monitor._cache.pop("eu-west_current", None)

        # Second fetch: prior GREEN, new RED → callback fires
        u2 = await monitor.get_current_intensity("eu-west")
        assert u2.zone == CarbonZone.RED, u2.zone
        assert len(fired) == 2, (
            f"P0 2.1 regression: callback did not fire on zone change "
            f"(fired={fired})")
        assert fired[1] == ("eu-west", "red"), fired[1]
        print(f"  ✓ zone change ({u1.zone.value} → {u2.zone.value}): "
              f"callback fired ({len(fired)})")

        # Force cache miss again.
        with monitor._lock:
            monitor._cache.pop("eu-west_current", None)

        # Third fetch: prior RED, new GREEN → callback fires
        u3 = await monitor.get_current_intensity("eu-west")
        assert u3.zone == CarbonZone.GREEN, u3.zone
        assert len(fired) == 3, fired
        print(f"  ✓ zone change ({u2.zone.value} → {u3.zone.value}): "
              f"callback fired ({len(fired)})")

        # Force cache miss again.
        with monitor._lock:
            monitor._cache.pop("eu-west_current", None)

        # Fourth fetch: prior GREEN, new GREEN (45.0) → NO callback
        u4 = await monitor.get_current_intensity("eu-west")
        assert u4.zone == CarbonZone.GREEN, u4.zone
        assert len(fired) == 3, (
            f"P0 2.1 regression: callback fired on unchanged zone "
            f"(fired={fired})")
        print(f"  ✓ same zone ({u3.zone.value} → {u4.zone.value}): "
              f"callback NOT fired ({len(fired)})")

    asyncio.run(_test_callbacks())

    # ---------------------------------------------------------------- #
    # P0 2.2 — legacy payload with missing field uses default
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.2: legacy payload picks up defaults ===")

    legacy_payload = {
        "carbon_forecaster_url": "http://test",
        "config": {
            "update_interval_seconds": 300,
            # cache_ttl_seconds omitted → 1800 (default)
            # fallback_intensity_gco2_kwh omitted → 400.0 (default)
            # monitor_regions omitted → ("default",) (default)
        },
        "dpq_config": {},
        "strict": True,
        "cache": [],
    }
    restored = CarbonIntensityMonitor.from_dict(legacy_payload)
    assert restored.config.update_interval_seconds == 300, (
        f"P0 2.2 regression: update_interval_seconds="
        f"{restored.config.update_interval_seconds}")
    assert restored.config.cache_ttl_seconds == 1800, (
        f"P0 2.2 regression: cache_ttl_seconds="
        f"{restored.config.cache_ttl_seconds}, expected 1800 (default)")
    assert restored.config.fallback_intensity_gco2_kwh == 400.0, (
        f"P0 2.2 regression: fallback_intensity_gco2_kwh="
        f"{restored.config.fallback_intensity_gco2_kwh}")
    assert restored.config.monitor_regions == ("default",)
    print(f"  ✓ update_interval=300 (explicit), "
          f"cache_ttl={restored.config.cache_ttl_seconds} (default), "
          f"fallback={restored.config.fallback_intensity_gco2_kwh} (default)")

    # ---------------------------------------------------------------- #
    # P0 2.3 — from_dict with "GREEN" (uppercase) works
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.3: from_dict handles uppercase zone ===")

    uppercase_payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "intensity_gco2_kwh": 40.0,
        "zone": "GREEN",       # uppercase — must not crash
        "region": "eu-west",
    }
    update = CarbonIntensityUpdate.from_dict(uppercase_payload)
    assert update.zone == CarbonZone.GREEN, (
        f"P0 2.3 regression: zone={update.zone}")
    print(f"  ✓ 'GREEN' → {update.zone.value}")

    # Whitespace
    whitespace_payload = dict(uppercase_payload)
    whitespace_payload["zone"] = "  green  "
    update = CarbonIntensityUpdate.from_dict(whitespace_payload)
    assert update.zone == CarbonZone.GREEN, update.zone
    print(f"  ✓ '  green  ' → {update.zone.value}")

    # Unknown value → RED + warning
    unknown_payload = dict(uppercase_payload)
    unknown_payload["zone"] = "chartreuse"
    update = CarbonIntensityUpdate.from_dict(unknown_payload)
    assert update.zone == CarbonZone.RED, (
        f"P0 2.3 regression: unknown zone should default to RED, "
        f"got {update.zone}")
    print(f"  ✓ 'chartreuse' → {update.zone.value} (fallback)")

    # ---------------------------------------------------------------- #
    # P0 2.4 — pickle round-trip works
    # ---------------------------------------------------------------- #
    print("\n=== P0 2.4: pickle round-trip ===")

    async def _test_pickle() -> None:
        monitor = CarbonIntensityMonitor(
            carbon_forecaster_url="http://test",
            fetcher=lambda r: 150.0,
        )
        # Populate the cache to check state preservation.
        await monitor.get_current_intensity("eu-west")
        assert monitor.cache_size == 1

        blob = pickle.dumps(monitor)
        restored = pickle.loads(blob)

        assert restored.cache_size == 1, (
            f"P0 2.4 regression: cache_size={restored.cache_size}, "
            f"expected 1")
        assert restored._lock is not None
        assert restored._fetcher is None  # dropped by design
        # _inflight / _tasks cleared
        assert restored._inflight == {}
        assert restored._tasks == []
        # Monitor still works
        restored._fetcher = lambda r: 100.0
        u = await restored.get_current_intensity("us-east")
        assert u.intensity_gco2_kwh == 100.0
        print(f"  ✓ pickle round-trip: cache_size={restored.cache_size}, "
              f"post-restore fetch OK")

    asyncio.run(_test_pickle())

    # ---------------------------------------------------------------- #
    # P1 2.5 — failing fetcher produces from_fallback=True
    # ---------------------------------------------------------------- #
    print("\n=== P1 2.5: fallback path sets from_fallback ===")

    async def _test_fallback() -> None:
        async def always_fail(region: str) -> float:
            raise RuntimeError("simulated fetch failure")

        monitor = CarbonIntensityMonitor(
            carbon_forecaster_url="http://test",
            fetcher=always_fail,
            strict=False,
            config=CarbonMonitorConfig(
                fetch_max_retries=0,
                fetch_base_backoff_seconds=0.0,
                fallback_intensity_gco2_kwh=999.0,
            ),
        )
        update = await monitor.get_current_intensity("eu-west")
        assert update.from_fallback is True, (
            f"P1 2.5 regression: from_fallback={update.from_fallback}, "
            f"expected True")
        assert update.intensity_gco2_kwh == 999.0, (
            f"P1 2.5 regression: intensity="
            f"{update.intensity_gco2_kwh}, expected 999.0 (config)")
        print(f"  ✓ from_fallback=True, "
              f"intensity={update.intensity_gco2_kwh} "
              f"(config fallback)")

        # Also check fetch_health reflects the failure.
        health = monitor.fetch_health()
        assert health["total_failures"] == 1, health
        assert health["consecutive_failures"] == 1, health
        print(f"  ✓ fetch_health: failures="
              f"{health['total_failures']}, "
              f"consecutive={health['consecutive_failures']}")

    asyncio.run(_test_fallback())

    # ---------------------------------------------------------------- #
    # P1 3.1 — single-flight for concurrent get_current_intensity
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.1: single-flight for concurrent fetches ===")

    async def _test_single_flight() -> None:
        call_count = 0

        async def slow_fetcher(region: str) -> float:
            nonlocal call_count
            call_count += 1
            await asyncio.sleep(0.1)  # simulate latency
            return 200.0

        monitor = CarbonIntensityMonitor(
            carbon_forecaster_url="http://test",
            fetcher=slow_fetcher,
        )

        results = await asyncio.gather(
            monitor.get_current_intensity("eu-west"),
            monitor.get_current_intensity("eu-west"),
            monitor.get_current_intensity("eu-west"),
        )
        assert call_count == 1, (
            f"P1 3.1 regression: fetcher called {call_count} times "
            f"(expected 1 via single-flight)")
        assert results[0] is results[1] is results[2], (
            "all callers should receive the same update object")
        print(f"  ✓ 3 concurrent calls → 1 fetcher invocation")

        # A subsequent call after completion hits the cache.
        result = await monitor.get_current_intensity("eu-west")
        assert call_count == 1  # still 1
        assert result is results[0]
        print(f"  ✓ subsequent call served from cache "
              f"(fetcher calls still {call_count})")

    asyncio.run(_test_single_flight())

    # ---------------------------------------------------------------- #
    # P1 3.6 — forecast_15min=NaN raises CarbonMonitorError
    # ---------------------------------------------------------------- #
    print("\n=== P1 3.6: forecast validation ===")

    base = dict(
        timestamp=datetime.now(timezone.utc),
        intensity_gco2_kwh=100.0,
        zone=CarbonZone.GREEN,
        region="test",
    )

    try:
        CarbonIntensityUpdate(**base, forecast_15min=float("nan"))
    except CarbonMonitorError as exc:
        print(f"  ✓ NaN forecast_15min rejected: {exc}")
    else:
        raise AssertionError(
            "P1 3.6 regression: NaN forecast_15min was accepted")

    try:
        CarbonIntensityUpdate(**base, forecast_60min=float("inf"))
    except CarbonMonitorError as exc:
        print(f"  ✓ Inf forecast_60min rejected: {exc}")
    else:
        raise AssertionError(
            "P1 3.6 regression: Inf forecast_60min was accepted")

    try:
        CarbonIntensityUpdate(**base, forecast_15min=-10.0)
    except CarbonMonitorError as exc:
        print(f"  ✓ negative forecast_15min rejected: {exc}")
    else:
        raise AssertionError(
            "P1 3.6 regression: negative forecast_15min was accepted")

    # Valid forecasts still work.
    ok = CarbonIntensityUpdate(**base, forecast_15min=120.0,
                               forecast_60min=180.0)
    assert ok.forecast_15min == 120.0
    print("  ✓ valid forecasts accepted")

    # ---------------------------------------------------------------- #
    # Bonus: metrics_hook, close(), statistics shape
    # ---------------------------------------------------------------- #
    print("\n=== Bonus: metrics / close / statistics ===")

    async def _test_bonus() -> None:
        events: List[Tuple[str, Mapping[str, Any]]] = []
        monitor = CarbonIntensityMonitor(
            carbon_forecaster_url="http://test",
            fetcher=lambda r: 100.0,
            metrics_hook=lambda ev, f: events.append((ev, f)),
        )
        await monitor.get_current_intensity("eu-west")
        names = {ev for ev, _ in events}
        assert "fetch_succeeded" in names, names
        assert "zone_changed" in names, names
        print(f"  ✓ metrics_hook: {sorted(names)}")

        stats = monitor.statistics()
        assert "fetch_stats" in stats
        assert stats["fetch_stats"]["total_successes"] == 1
        print(f"  ✓ statistics: "
              f"successes={stats['fetch_stats']['total_successes']}")

        monitor.close()  # idempotent
        monitor.close()
        print("  ✓ close() is idempotent")

    asyncio.run(_test_bonus())

    # ---------------------------------------------------------------- #
    # Bonus: async context manager starts and stops
    # ---------------------------------------------------------------- #
    print("\n=== Bonus: async context manager ===")

    async def _test_cm() -> None:
        monitor = CarbonIntensityMonitor(
            carbon_forecaster_url="http://test",
            fetcher=lambda r: 100.0,
            config=CarbonMonitorConfig(
                update_interval_seconds=9999,  # won't fire during test
            ),
        )
        async with monitor:
            assert monitor.running is True
        assert monitor.running is False
        print("  ✓ __aenter__ starts, __aexit__ stops")

    asyncio.run(_test_cm())

    print("\n✓ All self-tests passed.")
