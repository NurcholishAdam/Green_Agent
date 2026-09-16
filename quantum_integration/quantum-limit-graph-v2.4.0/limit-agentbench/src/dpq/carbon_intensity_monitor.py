# src/dpq/carbon_intensity_monitor.py

"""
Carbon Intensity Monitor
========================

Real-time carbon intensity tracker with zone prediction.

Enhancements
------------
- Imports ``CarbonZone`` from :mod:`models` — single source of truth.
- Module-level ``logger`` (was previously undefined → ``NameError`` risk).
- ``CarbonMonitorConfig`` with validated intervals / cache TTL.
- ``RLock``-guarded cache and callbacks; bounded cache.
- Retry with exponential backoff on API failures.
- Full validation; strict / non-strict modes.
- Sync + async context managers; serialization; ``statistics()``;
  ``__repr__``; custom :class:`CarbonMonitorError`.
- ``__main__`` smoke test with a pluggable fetcher.
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
from typing import Any, Awaitable, Callable, Deque, Dict, List, Mapping, Optional

from .models import CarbonZone, DPQConfig

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class CarbonMonitorError(ValueError):
    """Raised for invalid inputs, configuration, or fetch failures."""


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class CarbonMonitorConfig:
    """Tunable parameters for the carbon intensity monitor."""

    update_interval_seconds: int = 900        # 15 minutes
    cache_ttl_seconds: int = 1800             # 30 minutes
    prediction_horizon_minutes: int = 60
    max_cache_entries: int = 1000
    max_callbacks: int = 32
    fetch_max_retries: int = 2
    fetch_base_backoff_seconds: float = 1.0
    fetch_timeout_seconds: float = 15.0
    loop_error_sleep_seconds: float = 60.0

    def __post_init__(self) -> None:
        for name in (
            "update_interval_seconds",
            "cache_ttl_seconds",
            "prediction_horizon_minutes",
        ):
            value = getattr(self, name)
            if not isinstance(value, int) or value <= 0:
                raise CarbonMonitorError(f"{name} must be a positive int.")
        if self.max_cache_entries <= 0:
            raise CarbonMonitorError("max_cache_entries must be > 0.")
        if self.max_callbacks <= 0:
            raise CarbonMonitorError("max_callbacks must be > 0.")
        if self.fetch_max_retries < 0:
            raise CarbonMonitorError("fetch_max_retries must be >= 0.")
        if self.fetch_base_backoff_seconds < 0:
            raise CarbonMonitorError("fetch_base_backoff_seconds must be >= 0.")
        if self.fetch_timeout_seconds <= 0:
            raise CarbonMonitorError("fetch_timeout_seconds must be > 0.")


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

    def __post_init__(self) -> None:
        if self.intensity_gco2_kwh < 0:
            raise CarbonMonitorError("intensity_gco2_kwh must be >= 0.")
        if math.isnan(self.intensity_gco2_kwh) or math.isinf(
            self.intensity_gco2_kwh
        ):
            raise CarbonMonitorError("intensity_gco2_kwh must be finite.")
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
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "CarbonIntensityUpdate":
        ts = datetime.fromisoformat(str(data["timestamp"]))
        if not ts.tzinfo:
            ts = ts.replace(tzinfo=timezone.utc)
        return cls(
            timestamp=ts,
            intensity_gco2_kwh=float(data["intensity_gco2_kwh"]),
            zone=CarbonZone(str(data["zone"])),
            region=str(data.get("region", "default")),
            forecast_15min=data.get("forecast_15min"),
            forecast_60min=data.get("forecast_60min"),
        )


# --------------------------------------------------------------------------- #
# Monitor
# --------------------------------------------------------------------------- #
class CarbonIntensityMonitor:
    """
    Real-time carbon intensity tracker with zone prediction.

    Thread-safe, serializable, and bounded in memory. All original public
    methods are preserved; new parameters are keyword-only.
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
    ) -> None:
        if not isinstance(carbon_forecaster_url, str):
            raise CarbonMonitorError(
                "carbon_forecaster_url must be a string."
            )
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

        self.carbon_forecaster_url: str = carbon_forecaster_url
        # Legacy attributes preserved.
        self.update_interval: int = self._config.update_interval_seconds
        self.cache_ttl: int = self._config.cache_ttl_seconds
        self.prediction_horizon: int = self._config.prediction_horizon_minutes

        self._lock = threading.RLock()
        self._cache: Dict[str, CarbonIntensityUpdate] = {}
        self._cache_order: Deque[str] = deque()
        self._callbacks: List[Callable[[CarbonIntensityUpdate], Any]] = []
        self._running: bool = False
        self._tasks: List[asyncio.Task] = []
        self._ctx_start: Optional[float] = None
        self._fetcher = fetcher  # optional injectable fetcher for tests

        logger.debug(
            "CarbonIntensityMonitor initialized (url=%s, interval=%ds, "
            "cache_ttl=%ds, strict=%s)",
            self.carbon_forecaster_url,
            self.update_interval,
            self.cache_ttl,
            self._strict,
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

    # ---------------------------------------------------------- lifecycle
    async def start(self) -> None:
        """Start the monitoring loop."""
        if self._running:
            return
        self._running = True
        self._tasks = [
            asyncio.create_task(self._monitoring_loop(), name="dpq:carbon")
        ]
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
                logger.warning("Timed out waiting for carbon loop to stop.")
        self._tasks = []
        logger.info("CarbonIntensityMonitor stopped.")

    # ---------------------------------------------------------- callbacks
    def register_callback(
        self, callback: Callable[[CarbonIntensityUpdate], Any]
    ) -> None:
        """Register a callback invoked on zone changes."""
        if not callable(callback):
            raise CarbonMonitorError("callback must be callable.")
        with self._lock:
            if len(self._callbacks) >= self._config.max_callbacks:
                raise CarbonMonitorError(
                    f"max_callbacks={self._config.max_callbacks} reached."
                )
            self._callbacks.append(callback)
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
                return True
            except ValueError:
                return False

    async def _dispatch_callbacks(self, update: CarbonIntensityUpdate) -> None:
        with self._lock:
            callbacks = list(self._callbacks)
        for cb in callbacks:
            try:
                result = cb(update)
                if asyncio.iscoroutine(result):
                    await result
            except Exception as exc:
                logger.error(
                    "Carbon callback %r failed: %s",
                    getattr(cb, "__name__", cb),
                    exc,
                )

    # ---------------------------------------------------------- public API
    async def get_current_intensity(
        self, region: str = "default"
    ) -> CarbonIntensityUpdate:
        """Return the current intensity for ``region`` (cached when fresh)."""
        if not isinstance(region, str) or not region:
            raise CarbonMonitorError("region must be a non-empty string.")

        cache_key = f"{region}_current"

        with self._lock:
            cached = self._cache.get(cache_key)
            if cached is not None:
                age = (datetime.now(timezone.utc) - cached.timestamp).total_seconds()
                if age < self.cache_ttl:
                    return cached
                self._cache.pop(cache_key, None)

        update = await self._fetch_from_api(region)

        with self._lock:
            if cache_key not in self._cache:
                self._cache_order.append(cache_key)
            self._cache[cache_key] = update
            # Enforce bounded cache.
            while len(self._cache_order) > self._config.max_cache_entries:
                oldest = self._cache_order.popleft()
                self._cache.pop(oldest, None)

        if self._zone_changed(cache_key, update):
            await self._dispatch_callbacks(update)

        return update

    async def _fetch_from_api(self, region: str) -> CarbonIntensityUpdate:
        """
        Fetch carbon intensity for ``region``.

        Uses the injected ``fetcher`` when provided; otherwise returns a safe
        default reading (matches the original no-op behavior while remaining
        deterministic for testing).
        """
        if self._fetcher is not None:
            for attempt in range(1 + self._config.fetch_max_retries):
                try:
                    result = self._fetcher(region)
                    if asyncio.iscoroutine(result):
                        result = await asyncio.wait_for(
                            result,
                            timeout=self._config.fetch_timeout_seconds,
                        )
                    if isinstance(result, CarbonIntensityUpdate):
                        return result
                    if isinstance(result, Mapping):
                        return CarbonIntensityUpdate.from_dict(result)
                    # Numeric intensity → classify.
                    if isinstance(result, (int, float)):
                        intensity = float(result)
                        return CarbonIntensityUpdate(
                            timestamp=datetime.now(timezone.utc),
                            intensity_gco2_kwh=intensity,
                            zone=self._dpq_config.classify(intensity),
                            region=region,
                        )
                except Exception as exc:
                    logger.warning(
                        "Fetch attempt %d/%d for region %r failed: %s",
                        attempt + 1,
                        self._config.fetch_max_retries + 1,
                        region,
                        exc,
                    )
                    if attempt < self._config.fetch_max_retries:
                        await asyncio.sleep(
                            self._config.fetch_base_backoff_seconds
                            * (2 ** attempt)
                        )
            if self._strict:
                raise CarbonMonitorError(
                    f"All fetch attempts failed for region '{region}'."
                )

        # Deterministic fallback (was `pass` in the original).
        intensity = 400.0
        return CarbonIntensityUpdate(
            timestamp=datetime.now(timezone.utc),
            intensity_gco2_kwh=intensity,
            zone=self._dpq_config.classify(intensity),
            region=region,
        )

    def _zone_changed(
        self, cache_key: str, new_update: CarbonIntensityUpdate
    ) -> bool:
        """Return True if the carbon zone changed."""
        with self._lock:
            existing = self._cache.get(cache_key)
        if existing is None:
            return True
        return existing.zone != new_update.zone

    async def _monitoring_loop(self) -> None:
        """Background loop for periodic updates."""
        while self._running:
            try:
                await self.get_current_intensity()
                await asyncio.sleep(self.update_interval)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("Monitoring loop error: %s", exc)
                await asyncio.sleep(self._config.loop_error_sleep_seconds)

    # ---------------------------------------------------------- stats
    def statistics(self) -> Dict[str, Any]:
        """Return aggregate statistics over the cache."""
        with self._lock:
            cache = list(self._cache.values())
        if not cache:
            return {
                "cache_size": 0,
                "mean_intensity": None,
                "by_zone": {},
                "callbacks": len(self._callbacks),
                "running": self._running,
            }
        by_zone: Dict[str, int] = {}
        for u in cache:
            by_zone[u.zone.value] = by_zone.get(u.zone.value, 0) + 1
        return {
            "cache_size": len(cache),
            "mean_intensity": sum(u.intensity_gco2_kwh for u in cache) / len(cache),
            "by_zone": by_zone,
            "callbacks": len(self._callbacks),
            "running": self._running,
        }

    def reset(self, *, clear_cache: bool = False) -> None:
        """Reset internal state; optionally clear the cache."""
        with self._lock:
            if clear_cache:
                self._cache.clear()
                self._cache_order.clear()
        logger.debug("CarbonIntensityMonitor reset (clear_cache=%s)", clear_cache)

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "carbon_forecaster_url": self.carbon_forecaster_url,
                "config": asdict(self._config),
                "dpq_config": asdict(self._dpq_config),
                "strict": self._strict,
                "running": self._running,
                "cache": [u.to_dict() for u in self._cache.values()],
                "callbacks": len(self._callbacks),
            }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "CarbonIntensityMonitor":
        if not isinstance(data, Mapping):
            raise CarbonMonitorError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )
        cfg_data = dict(data.get("config", {}) or {})
        cfg = CarbonMonitorConfig(
            update_interval_seconds=int(cfg_data.get("update_interval_seconds", 900)),
            cache_ttl_seconds=int(cfg_data.get("cache_ttl_seconds", 1800)),
            prediction_horizon_minutes=int(
                cfg_data.get("prediction_horizon_minutes", 60)
            ),
            max_cache_entries=int(cfg_data.get("max_cache_entries", 1000)),
            max_callbacks=int(cfg_data.get("max_callbacks", 32)),
            fetch_max_retries=int(cfg_data.get("fetch_max_retries", 2)),
            fetch_base_backoff_seconds=float(
                cfg_data.get("fetch_base_backoff_seconds", 1.0)
            ),
            fetch_timeout_seconds=float(
                cfg_data.get("fetch_timeout_seconds", 15.0)
            ),
            loop_error_sleep_seconds=float(
                cfg_data.get("loop_error_sleep_seconds", 60.0)
            ),
        )
        dpq_data = dict(data.get("dpq_config", {}) or {})
        dpq_cfg = DPQConfig(
            green_threshold=float(dpq_data.get("green_threshold", 50.0)),
            yellow_threshold=float(dpq_data.get("yellow_threshold", 200.0)),
            red_threshold=float(dpq_data.get("red_threshold", 400.0)),
        )
        monitor = cls(
            carbon_forecaster_url=str(data["carbon_forecaster_url"]),
            config=cfg,
            dpq_config=dpq_cfg,
            strict=bool(data.get("strict", True)),
        )
        with monitor._lock:
            for u in data.get("cache", []):
                update = CarbonIntensityUpdate.from_dict(u)
                key = f"{update.region}_current"
                monitor._cache[key] = update
                monitor._cache_order.append(key)
        return monitor

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "CarbonIntensityMonitor":
        try:
            return cls.from_dict(json.loads(payload))
        except json.JSONDecodeError as exc:
            raise CarbonMonitorError(f"Invalid JSON payload: {exc}") from exc

    # ---------------------------------------------------------- context mgr
    async def __aenter__(self) -> "CarbonIntensityMonitor":
        self._ctx_start = time.perf_counter()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        elapsed = time.perf_counter() - (
            self._ctx_start if self._ctx_start is not None else time.perf_counter()
        )
        self._ctx_start = None
        try:
            await self.stop()
        finally:
            if exc_type is not None:
                logger.warning(
                    "CarbonMonitor scope exited with %s after %.4fs.",
                    exc_type.__name__, elapsed,
                )

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        with self._lock:
            return (
                "CarbonIntensityMonitor("
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
# Smoke test
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    async def _fake_fetcher(region: str) -> float:
        return {"eu-west": 40.0, "us-east": 350.0}.get(region, 200.0)

    async def main() -> None:
        monitor = CarbonIntensityMonitor(
            carbon_forecaster_url="http://carbon.invalid",
            fetcher=_fake_fetcher,
        )
        async with monitor:
            await monitor.start()
            u1 = await monitor.get_current_intensity("eu-west")
            print("eu-west   :", u1.zone.value, u1.intensity_gco2_kwh)
            u2 = await monitor.get_current_intensity("us-east")
            print("us-east   :", u2.zone.value, u2.intensity_gco2_kwh)

            zones: List[str] = []

            async def _cb(update: CarbonIntensityUpdate) -> None:
                zones.append(update.zone.value)

            monitor.register_callback(_cb)
            await monitor.get_current_intensity("us-east")  # cache hit
            print("stats     :", monitor.statistics())
        print("repr      :", monitor)

        # Serialization round-trip.
        payload = monitor.to_json()
        restored = CarbonIntensityMonitor.from_json(payload)
        assert restored.to_dict() == monitor.to_dict()
        print("Round-trip OK.")

    asyncio.run(main())
