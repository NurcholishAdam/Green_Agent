# src/sustainability/carbon_forecast.py

"""
Carbon Forecast
===============

Moving-average and exponentially-weighted carbon intensity predictor.

Enhancements
------------
- ``ForecastMethod`` StrEnum — ``"mean"`` | ``"median"`` | ``"ewma"`` |
  ``"p95"``.
- ``CarbonForecastConfig`` — frozen, validated: window size, method,
  EWMA alpha, finite ceilings, bounded history.
- **Fixed O(n) list mutation** — ``collections.deque(maxlen=window_size)``
  replaces ``list`` + ``pop(0)``.
- **Full validation** of every ``carbon_intensity`` sample; strict /
  non-strict modes.
- **Thread safety** — ``RLock`` guards the history.
- **`reset()` / `statistics()` / `__repr__`**.
- Serialization: ``to_dict`` / ``from_dict`` / ``to_json``.
- Custom ``CarbonForecastError(ValueError)``, lazy ``%s`` logging, and a
  ``__main__`` smoke test.
"""

from __future__ import annotations

import json
import logging
import math
import statistics as _statistics
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Deque, Dict, List, Mapping, Optional, Sequence

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class CarbonForecastError(ValueError):
    """Raised for invalid carbon-forecast inputs or configuration."""


# --------------------------------------------------------------------------- #
# Forecast method
# --------------------------------------------------------------------------- #
class ForecastMethod(str, Enum):
    """Supported prediction strategies."""

    MEAN = "mean"
    MEDIAN = "median"
    EWMA = "ewma"
    P95 = "p95"

    @property
    def description(self) -> str:
        return {
            ForecastMethod.MEAN: "Arithmetic mean of the window",
            ForecastMethod.MEDIAN: "Median of the window",
            ForecastMethod.EWMA: "Exponentially weighted moving average",
            ForecastMethod.P95: "95th percentile of the window",
        }[self]


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class CarbonForecastConfig:
    """Tunable parameters for :class:`CarbonForecast`."""

    window_size: int = 24
    method: ForecastMethod = ForecastMethod.MEAN
    ewma_alpha: float = 0.3
    max_intensity: float = 1e6  # gCO2/kWh ceiling
    max_history: int = 10_000   # observation audit trail

    def __post_init__(self) -> None:
        if not isinstance(self.window_size, int) or self.window_size <= 0:
            raise CarbonForecastError("window_size must be a positive int.")
        if not isinstance(self.method, ForecastMethod):
            try:
                object.__setattr__(
                    self, "method", ForecastMethod(str(self.method)),
                )
            except ValueError as exc:
                raise CarbonForecastError(
                    f"unknown method {self.method!r}."
                ) from exc
        if not 0.0 < self.ewma_alpha <= 1.0:
            raise CarbonForecastError("ewma_alpha must be in (0, 1].")
        if self.max_intensity <= 0:
            raise CarbonForecastError("max_intensity must be > 0.")
        if self.max_history <= 0:
            raise CarbonForecastError("max_history must be > 0.")

    def to_dict(self) -> Dict[str, Any]:
        return {**asdict(self), "method": self.method.value}

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "CarbonForecastConfig":
        if not isinstance(data, Mapping):
            raise CarbonForecastError(
                "CarbonForecastConfig.from_dict expects a Mapping."
            )
        valid = set(cls.__dataclass_fields__.keys())
        kwargs: Dict[str, Any] = {}
        for k, v in data.items():
            if k not in valid:
                continue
            if k == "method" and isinstance(v, str):
                kwargs[k] = ForecastMethod(v)
            else:
                kwargs[k] = v
        return cls(**kwargs)


# --------------------------------------------------------------------------- #
# Forecast
# --------------------------------------------------------------------------- #
class CarbonForecast:
    """
    Moving-average / EWMA carbon intensity predictor.

    Thread-safe, serializable, and bounded in memory. The original public API
    (``update``, ``predict_next``, ``history``, ``window_size``) is preserved;
    new parameters are keyword-only.
    """

    def __init__(
        self,
        window_size: int = 24,
        *,
        config: Optional[CarbonForecastConfig] = None,
        strict: bool = True,
    ) -> None:
        if config is not None:
            self._config = config
        else:
            self._config = CarbonForecastConfig(window_size=int(window_size))
        self._strict = bool(strict)

        # Legacy attributes preserved.
        self.window_size: int = self._config.window_size

        self._lock = threading.RLock()
        # O(1) ring-buffer for the prediction window.
        self.history: Deque[float] = deque(maxlen=self._config.window_size)
        # Bounded audit trail of every observed sample.
        self._observations: Deque[Dict[str, Any]] = deque(
            maxlen=self._config.max_history
        )
        self._ewma: Optional[float] = None
        self._started_at: float = time.time()

        logger.debug(
            "CarbonForecast initialized "
            "(window=%d, method=%s, strict=%s)",
            self.window_size, self._config.method.value, self._strict,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> CarbonForecastConfig:
        return self._config

    @property
    def method(self) -> ForecastMethod:
        return self._config.method

    @property
    def sample_count(self) -> int:
        with self._lock:
            return len(self.history)

    # ---------------------------------------------------------- public API
    def update(self, carbon_intensity: float) -> None:
        """
        Append a new intensity observation.

        Parameters
        ----------
        carbon_intensity : float
            Observed intensity in gCO2/kWh. Must be finite and in
            ``[0, config.max_intensity]``.
        """
        value = self._validate_intensity(carbon_intensity)

        with self._lock:
            self.history.append(value)
            # Update the EWMA state.
            if self._ewma is None:
                self._ewma = value
            else:
                alpha = self._config.ewma_alpha
                self._ewma = alpha * value + (1.0 - alpha) * self._ewma
            self._observations.append({
                "value": value,
                "timestamp": time.time(),
            })

        logger.debug("Forecast updated: intensity=%.3f", value)

    def predict_next(self) -> float:
        """
        Return the predicted next carbon intensity.

        Returns ``0.0`` when the forecast has no observations yet.
        """
        with self._lock:
            if not self.history:
                return 0.0
            values = list(self.history)
            method = self._config.method
            ewma_state = self._ewma

        if method == ForecastMethod.MEAN:
            return sum(values) / len(values)
        if method == ForecastMethod.MEDIAN:
            return float(_statistics.median(values))
        if method == ForecastMethod.EWMA:
            return float(ewma_state) if ewma_state is not None else 0.0
        if method == ForecastMethod.P95:
            ordered = sorted(values)
            idx = max(0, min(len(ordered) - 1, int(round(0.95 * (len(ordered) - 1)))))
            return float(ordered[idx])
        return sum(values) / len(values)  # pragma: no cover — exhaustive

    def reset(self, *, clear_observations: bool = True) -> int:
        """Reset the forecast. Returns the number of observations removed."""
        with self._lock:
            removed = len(self._observations)
            self.history.clear()
            self._ewma = None
            if clear_observations:
                self._observations.clear()
            self._started_at = time.time()
        logger.debug("CarbonForecast reset (removed %d).", removed)
        return removed

    # ---------------------------------------------------------- validation
    def _validate_intensity(self, value: Any) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            msg = (
                f"carbon_intensity must be numeric, got "
                f"{type(value).__name__}."
            )
            if self._strict:
                raise CarbonForecastError(msg)
            logger.warning("%s Using 0.0.", msg)
            return 0.0
        fv = float(value)
        if math.isnan(fv) or math.isinf(fv):
            msg = f"carbon_intensity must be finite, got {value!r}."
            if self._strict:
                raise CarbonForecastError(msg)
            logger.warning("%s Using 0.0.", msg)
            return 0.0
        if fv < 0:
            msg = f"carbon_intensity must be >= 0, got {fv}."
            if self._strict:
                raise CarbonForecastError(msg)
            logger.warning("%s Using 0.0.", msg)
            return 0.0
        if fv > self._config.max_intensity:
            msg = (
                f"carbon_intensity must be <= {self._config.max_intensity}, "
                f"got {fv}."
            )
            if self._strict:
                raise CarbonForecastError(msg)
            logger.warning("%s Clamping.", msg)
            return self._config.max_intensity
        return fv

    # ---------------------------------------------------------- statistics
    def statistics(self) -> Dict[str, Any]:
        with self._lock:
            values = list(self.history)
            observations = len(self._observations)
            ewma_state = self._ewma
        if not values:
            return {
                "sample_count": 0,
                "method": self._config.method.value,
                "window_size": self.window_size,
                "predict_next": 0.0,
                "mean": 0.0,
                "median": 0.0,
                "min": 0.0,
                "max": 0.0,
                "ewma": None,
                "observations": observations,
                "uptime_seconds": time.time() - self._started_at,
            }
        return {
            "sample_count": len(values),
            "method": self._config.method.value,
            "window_size": self.window_size,
            "predict_next": self.predict_next(),
            "mean": sum(values) / len(values),
            "median": float(_statistics.median(values)),
            "min": min(values),
            "max": max(values),
            "ewma": ewma_state,
            "observations": observations,
            "uptime_seconds": time.time() - self._started_at,
        }

    # ---------------------------------------------------------- serialization
    def to_dict(self, *, include_observations: bool = False) -> Dict[str, Any]:
        with self._lock:
            payload: Dict[str, Any] = {
                "config": self._config.to_dict(),
                "strict": self._strict,
                "history": list(self.history),
                "ewma": self._ewma,
                "started_at": self._started_at,
            }
            if include_observations:
                payload["observations"] = list(self._observations)
        return payload

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "CarbonForecast":
        if not isinstance(data, Mapping):
            raise CarbonForecastError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )
        cfg = CarbonForecastConfig.from_dict(
            dict(data.get("config", {}) or {})
        )
        forecast = cls(config=cfg, strict=bool(data.get("strict", True)))
        with forecast._lock:
            for v in data.get("history", []):
                forecast.history.append(float(v))
            forecast._ewma = data.get("ewma")
            forecast._started_at = float(data.get("started_at", time.time()))
            for o in data.get("observations", []):
                if isinstance(o, Mapping):
                    forecast._observations.append(dict(o))
        return forecast

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "CarbonForecast":
        try:
            return cls.from_dict(json.loads(payload))
        except json.JSONDecodeError as exc:
            raise CarbonForecastError(f"Invalid JSON: {exc}") from exc

    # ----------------------------------------------------------------- dunder
    def __len__(self) -> int:
        return self.sample_count

    def __repr__(self) -> str:
        with self._lock:
            return (
                "CarbonForecast("
                f"window={self.window_size}, "
                f"method={self._config.method.value}, "
                f"samples={len(self.history)}, "
                f"predict_next={self.predict_next():.3f})"
            )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "CarbonForecast",
    "CarbonForecastConfig",
    "CarbonForecastError",
    "ForecastMethod",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m sustainability.carbon_forecast
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    # ---- All four methods ------------------------------------------ #
    for method in ForecastMethod:
        f = CarbonForecast(config=CarbonForecastConfig(method=method))
        for v in (100, 200, 300, 400, 500):
            f.update(v)
        print(f"{method.value:<8} predict_next={f.predict_next():.3f}")

    # ---- Bug fix: NaN / inf / negative ---------------------------- #
    f = CarbonForecast()
    for bad in (float("nan"), float("inf"), -1.0, "big", None):
        try:
            f.update(bad)
        except CarbonForecastError as exc:
            print("Rejected   :", exc)

    # ---- Non-strict coerces --------------------------------------- #
    lenient = CarbonForecast(strict=False)
    lenient.update(float("nan"))
    print("lenient    :", lenient.predict_next())

    # ---- Config validation ---------------------------------------- #
    for bad_cfg in (
        dict(window_size=0),
        dict(method="bogus"),
        dict(ewma_alpha=0.0),
        dict(ewma_alpha=1.5),
        dict(max_intensity=0),
        dict(max_history=0),
    ):
        try:
            CarbonForecastConfig(**bad_cfg)  # type: ignore[arg-type]
        except CarbonForecastError as exc:
            print("Rejected cfg:", exc)

    # ---- Serialization round-trip --------------------------------- #
    payload = f.to_json()
    restored = CarbonForecast.from_json(payload)
    assert restored.to_dict() == f.to_dict()
    print("Round-trip OK.")

    # ---- Statistics ----------------------------------------------- #
    print("statistics :", {
        k: v for k, v in f.statistics().items()
        if k != "uptime_seconds"
    })

    print("\nSmoke test passed.")
