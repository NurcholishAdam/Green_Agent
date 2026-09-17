# src/sustainability/temporal_shifter.py

"""
Temporal Shifter
================

Decides whether work should be deferred until the grid is cleaner, based on
a forecasted carbon intensity.

Enhancements
------------
- ``TemporalShifterConfig`` — frozen, validated: default threshold,
  hysteresis, bounded decision history.
- ``ShiftDecision`` — frozen, serializable record of a decision.
- **Full validation** of `predicted_carbon` and `threshold`.
- **Hysteresis** — a value oscillating around the threshold does not toggle
  the deferral every call.
- **Optional forecast provider** — the shifter can pull the prediction from
  a :class:`~sustainability.carbon_forecast.CarbonForecast` when no
  explicit value is supplied.
- **Bounded decision history** — every decision is recorded with a
  timestamp.
- **Thread safety** — `RLock` guards state and history.
- **Serialization / `statistics()` / `reset()` / `__repr__`.**
- Custom ``TemporalShifterError(ValueError)``, lazy ``%s`` logging, and a
  ``__main__`` smoke test.
"""

from __future__ import annotations

import json
import logging
import math
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Deque, Dict, List, Mapping, Optional

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class TemporalShifterError(ValueError):
    """Raised for invalid temporal-shifter inputs or configuration."""


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class TemporalShifterConfig:
    """Tunable parameters for :class:`TemporalShifter`."""

    default_threshold: float = 400.0
    clean_threshold: float = 350.0  # hysteresis: do not defer below this
    max_history: int = 1_000

    def __post_init__(self) -> None:
        for name in ("default_threshold", "clean_threshold"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise TemporalShifterError(f"{name} must be numeric.")
            fv = float(value)
            if math.isnan(fv) or math.isinf(fv) or fv < 0:
                raise TemporalShifterError(
                    f"{name} must be finite and >= 0."
                )
        if self.clean_threshold >= self.default_threshold:
            raise TemporalShifterError(
                "clean_threshold must be < default_threshold."
            )
        if self.max_history <= 0:
            raise TemporalShifterError("max_history must be > 0.")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "TemporalShifterConfig":
        if not isinstance(data, Mapping):
            raise TemporalShifterError(
                "TemporalShifterConfig.from_dict expects a Mapping."
            )
        valid = set(cls.__dataclass_fields__.keys())
        kwargs = {k: v for k, v in data.items() if k in valid}
        return cls(**kwargs)


# --------------------------------------------------------------------------- #
# Decision
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class ShiftDecision:
    """Immutable record of a shift decision."""

    should_delay: bool
    predicted_carbon: float
    threshold: float
    reason: str
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "should_delay": self.should_delay,
            "predicted_carbon": self.predicted_carbon,
            "threshold": self.threshold,
            "reason": self.reason,
            "timestamp": self.timestamp.isoformat(),
        }

    def __repr__(self) -> str:
        return (
            "ShiftDecision("
            f"should_delay={self.should_delay}, "
            f"predicted={self.predicted_carbon:.1f}, "
            f"threshold={self.threshold:.1f})"
        )


# --------------------------------------------------------------------------- #
# Shifter
# --------------------------------------------------------------------------- #
class TemporalShifter:
    """
    Decides whether work should be deferred until the grid is cleaner.

    The original API (``should_delay(predicted_carbon, threshold)``) is
    preserved; new parameters are keyword-only.
    """

    def __init__(
        self,
        *,
        config: Optional[TemporalShifterConfig] = None,
        forecast: Optional[Any] = None,
        strict: bool = True,
    ) -> None:
        self._config = config or TemporalShifterConfig()
        self._strict = bool(strict)

        # Optional forecast collaborator.
        if forecast is not None and not callable(
            getattr(forecast, "predict_next", None)
        ):
            raise TemporalShifterError(
                "forecast must expose a callable 'predict_next'."
            )
        self.forecast = forecast

        self._lock = threading.RLock()
        self._history: Deque[ShiftDecision] = deque(
            maxlen=self._config.max_history
        )
        self._was_delaying: bool = False
        self._started_at: float = time.time()

        logger.debug(
            "TemporalShifter initialized "
            "(threshold=%.1f, clean=%.1f, forecast=%s, strict=%s)",
            self._config.default_threshold,
            self._config.clean_threshold,
            forecast is not None,
            self._strict,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> TemporalShifterConfig:
        return self._config

    @property
    def decisions(self) -> List[ShiftDecision]:
        with self._lock:
            return list(self._history)

    # ---------------------------------------------------------- public API
    def should_delay(
        self,
        predicted_carbon: Optional[float] = None,
        threshold: Optional[float] = None,
    ) -> bool:
        """
        Decide whether work should be deferred.

        Parameters
        ----------
        predicted_carbon : float, optional
            Forecasted carbon intensity. When ``None``, the shifter uses its
            ``forecast`` collaborator's ``predict_next()`` if available;
            otherwise it returns ``False``.
        threshold : float, optional
            Defer when ``predicted_carbon`` exceeds this value. Defaults to
            ``config.default_threshold``.

        Returns
        -------
        bool
            True if the work should be deferred.
        """
        # ---- Resolve the prediction ---------------------------------
        if predicted_carbon is None:
            if self.forecast is not None:
                try:
                    predicted_carbon = float(self.forecast.predict_next())
                except Exception as exc:
                    if self._strict:
                        raise TemporalShifterError(
                            f"forecast.predict_next failed: {exc}"
                        ) from exc
                    logger.warning("forecast.predict_next failed: %s", exc)
                    predicted_carbon = 0.0
            else:
                predicted_carbon = 0.0

        value = self._validate_number("predicted_carbon", predicted_carbon)

        # ---- Resolve the threshold ----------------------------------
        t = (
            self._config.default_threshold
            if threshold is None else threshold
        )
        t = self._validate_number("threshold", t)

        # ---- Hysteresis ---------------------------------------------
        with self._lock:
            was_delaying = self._was_delaying
            if not was_delaying and value > t:
                should_delay = True
                reason = "predicted_above_threshold"
            elif was_delaying and value < self._config.clean_threshold:
                should_delay = False
                reason = "predicted_below_clean_threshold"
            else:
                should_delay = was_delaying
                reason = "hysteresis_hold"
            self._was_delaying = should_delay

            self._history.append(ShiftDecision(
                should_delay=should_delay,
                predicted_carbon=value,
                threshold=t,
                reason=reason,
            ))

        logger.debug(
            "Shift decision: predicted=%.1f threshold=%.1f -> %s (%s)",
            value, t, should_delay, reason,
        )
        return should_delay

    def reset(self, *, clear_history: bool = True) -> int:
        """Reset the shifter. Returns the number of decisions removed."""
        with self._lock:
            removed = len(self._history)
            self._was_delaying = False
            if clear_history:
                self._history.clear()
            self._started_at = time.time()
        logger.debug("TemporalShifter reset (removed %d).", removed)
        return removed

    # ---------------------------------------------------------- validation
    def _validate_number(self, name: str, value: Any) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            msg = f"{name} must be numeric, got {type(value).__name__}."
            if self._strict:
                raise TemporalShifterError(msg)
            logger.warning("%s Using 0.0.", msg)
            return 0.0
        fv = float(value)
        if math.isnan(fv) or math.isinf(fv) or fv < 0:
            msg = f"{name} must be finite and >= 0, got {value!r}."
            if self._strict:
                raise TemporalShifterError(msg)
            logger.warning("%s Using 0.0.", msg)
            return 0.0
        return fv

    # ---------------------------------------------------------- statistics
    def statistics(self) -> Dict[str, Any]:
        with self._lock:
            history = list(self._history)
            currently_delaying = self._was_delaying
        delays = sum(1 for d in history if d.should_delay)
        returns = len(history) - delays
        return {
            "decisions": len(history),
            "delays": delays,
            "releases": returns,
            "currently_delaying": currently_delaying,
            "config": self._config.to_dict(),
            "strict": self._strict,
            "uptime_seconds": time.time() - self._started_at,
        }

    # ---------------------------------------------------------- serialization
    def to_dict(self, *, include_history: bool = False) -> Dict[str, Any]:
        with self._lock:
            payload: Dict[str, Any] = {
                "config": self._config.to_dict(),
                "strict": self._strict,
                "was_delaying": self._was_delaying,
                "started_at": self._started_at,
            }
            if include_history:
                payload["history"] = [d.to_dict() for d in self._history]
        return payload

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        with self._lock:
            return (
                "TemporalShifter("
                f"threshold={self._config.default_threshold:.1f}, "
                f"currently_delaying={self._was_delaying}, "
                f"decisions={len(self._history)})"
            )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "ShiftDecision",
    "TemporalShifter",
    "TemporalShifterConfig",
    "TemporalShifterError",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m sustainability.temporal_shifter
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    shifter = TemporalShifter()
    print("repr       :", shifter)

    # ---- Enter deferral -------------------------------------------- #
    assert shifter.should_delay(predicted_carbon=500.0, threshold=400.0) is True
    print("defer      : yes (above threshold)")

    # ---- Hysteresis ------------------------------------------------ #
    assert shifter.should_delay(predicted_carbon=380.0, threshold=400.0) is True
    print("hysteresis : still deferring")

    # ---- Release --------------------------------------------------- #
    assert shifter.should_delay(predicted_carbon=300.0, threshold=400.0) is False
    print("release    : no (below clean threshold)")

    # ---- Forecast collaborator ------------------------------------ #
    class _Forecast:
        def predict_next(self) -> float:
            return 450.0

    forecast_shifter = TemporalShifter(forecast=_Forecast())
    assert forecast_shifter.should_delay() is True
    print("forecast   : OK")

    # ---- Bug fix: NaN / inf / negative ---------------------------- #
    for bad in (float("nan"), float("inf"), -1.0, "big"):
        try:
            shifter.should_delay(predicted_carbon=bad)
        except TemporalShifterError as exc:
            print("Rejected   :", exc)

    # ---- Config validation ---------------------------------------- #
    for bad_cfg in (
        dict(default_threshold=-1),
        dict(clean_threshold=500, default_threshold=400),
        dict(max_history=0),
    ):
        try:
            TemporalShifterConfig(**bad_cfg)  # type: ignore[arg-type]
        except TemporalShifterError as exc:
            print("Rejected cfg:", exc)

    # ---- Statistics ----------------------------------------------- #
    print("statistics :", {
        k: v for k, v in shifter.statistics().items()
        if k not in ("config", "uptime_seconds")
    })

    # ---- Serialization ------------------------------------------- #
    print("serialization:", json.dumps(shifter.to_dict(), default=str)[:80], "...")

    print("\nSmoke test passed.")
