# src/sustainability/carbon_forecast.py

"""
Carbon Forecast
===============

Moving-average, median, EWMA, and 95th-percentile carbon intensity predictor.

The forecast maintains a sliding window of recent observations (bounded by
``window_size``) plus a longer audit trail (bounded by ``max_history``).
Four prediction strategies are supported via :class:`ForecastMethod`.

Enhancements
------------
- ``ForecastMethod`` — ``StrEnum`` with a 3.8–3.10 shim, so ``str(method)``
  is ``"mean"`` (not ``"ForecastMethod.MEAN"``) on every supported Python.
- ``CarbonForecastConfig`` — frozen, ``slots``, ``__repr__``; every field
  type-checked (``bool`` rejected for ints, ``NaN``/``inf`` rejected for
  floats); no ``object.__setattr__`` hack — string input is coerced in
  ``from_dict`` only, with case-insensitive matching; ``from_dict(...,
  strict=True)`` rejects unknown keys.
- **``to_json(include_observations=True)`` now works** — the kwarg is
  routed to ``to_dict`` instead of being forwarded to ``json.dumps``.
- **Atomic ``statistics()``** — ``predict_next`` is computed from the
  snapshot taken under the lock, not by re-entering ``predict_next()``.
- **Lifecycle counters** — ``total_updates`` and ``dropped_observations``
  are decoupled from ``len(history)`` / ``len(_observations)``.
- **Non-strict coercion is marked** — a sample coerced to ``0.0`` (or
  clamped) carries ``"coerced": True`` and ``"raw_repr"`` in the audit
  trail so consumers can filter it out.
- ``predict_next_optional() -> Optional[float]`` — distinguishes "no data"
  from a genuine zero.
- ``update_batch(values)`` — validates all values first, then applies
  under one lock, so a failure mid-batch leaves the forecast untouched.
- ``from_dict`` validates restored ``history`` / ``ewma`` items (finite,
  in bounds) and wraps every parse error as :class:`CarbonForecastError`.
- ``to_json`` no longer uses ``default=str``.
- ``__eq__`` / ``__hash__`` on the config and forecast;
  ``__len__`` still returns window occupancy (backward-compatible).
- ``CarbonForecastStatistics`` TypedDict for ``statistics()``.
- Class docstring documents the "mutable legacy attributes" caveat and
  ``_started_at`` uptime semantics for restored instances.
- ``__dir__`` for clean REPL discoverability.

Compatibility
-------------
- ``CarbonForecast(window_size=24, *, config=None, strict=True)`` unchanged.
- ``update(value) -> None`` unchanged.
- ``predict_next() -> float`` unchanged (returns ``0.0`` when empty).
- ``len(forecast)`` still returns window occupancy.
- ``to_json`` signature is now ``to_json(*, include_observations=False,
  **kwargs)`` — the kwarg routes correctly (previously it crashed).
- ``CarbonForecastConfig(method="mean")`` now raises; use
  ``CarbonForecastConfig(method=ForecastMethod.MEAN)`` or
  ``CarbonForecastConfig.from_dict({"method": "mean"})``.
- ``CarbonForecast(window_size=24.9)`` now raises instead of silently
  truncating to ``24``.

Requires Python 3.8+. ``slots=True`` on dataclasses requires 3.10+;
drop it for older versions.
"""

from __future__ import annotations

import json
import logging
import math
import statistics as _statistics
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass
from enum import Enum
from typing import (
    Any, Deque, Dict, Iterable, List, Mapping, Optional, Tuple, TypedDict,
)

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# StrEnum compatibility shim
# --------------------------------------------------------------------------- #
try:  # pragma: no cover — Python 3.11+
    from enum import StrEnum  # type: ignore[attr-defined]
except ImportError:  # pragma: no cover — 3.10 and earlier
    class StrEnum(str, Enum):  # type: ignore[no-redef]
        """``str`` + ``Enum`` with ``__str__``/``__format__`` delegating to ``str``."""

        __str__ = str.__str__
        __format__ = str.__format__


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class CarbonForecastError(ValueError):
    """Raised for invalid carbon-forecast inputs or configuration."""


# --------------------------------------------------------------------------- #
# Typed statistics return
# --------------------------------------------------------------------------- #
class CarbonForecastStatistics(TypedDict):
    """Return shape of :meth:`CarbonForecast.statistics`."""

    sample_count: int
    total_updates: int
    dropped_observations: int
    window_size: int
    method: str
    predict_next: Optional[float]
    mean: float
    median: float
    min: float
    max: float
    ewma: Optional[float]
    observations: int
    uptime_seconds: float


# --------------------------------------------------------------------------- #
# Forecast method
# --------------------------------------------------------------------------- #
class ForecastMethod(StrEnum):
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
            ForecastMethod.P95: (
                "95th percentile of the window (nearest-rank)"
            ),
        }[self]


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class CarbonForecastConfig:
    """Tunable parameters for :class:`CarbonForecast`."""

    window_size: int = 24
    method: ForecastMethod = ForecastMethod.MEAN
    ewma_alpha: float = 0.3
    max_intensity: float = 1e6  # gCO2/kWh ceiling
    max_history: int = 10_000   # observation audit trail

    def __post_init__(self) -> None:
        # --- window_size --------------------------------------------- #
        if isinstance(self.window_size, bool) or not isinstance(
            self.window_size, int
        ):
            raise CarbonForecastError("window_size must be an int.")
        if self.window_size <= 0:
            raise CarbonForecastError("window_size must be > 0.")

        # --- method (no coercion; strings handled in from_dict) ------ #
        if not isinstance(self.method, ForecastMethod):
            raise CarbonForecastError(
                f"method must be a ForecastMethod, got "
                f"{type(self.method).__name__}."
            )

        # --- ewma_alpha ---------------------------------------------- #
        if isinstance(self.ewma_alpha, bool) or not isinstance(
            self.ewma_alpha, (int, float)
        ):
            raise CarbonForecastError("ewma_alpha must be numeric.")
        alpha = float(self.ewma_alpha)
        if not math.isfinite(alpha):
            raise CarbonForecastError("ewma_alpha must be finite.")
        if not 0.0 < alpha <= 1.0:
            raise CarbonForecastError("ewma_alpha must be in (0, 1].")

        # --- max_intensity ------------------------------------------- #
        if isinstance(self.max_intensity, bool) or not isinstance(
            self.max_intensity, (int, float)
        ):
            raise CarbonForecastError("max_intensity must be numeric.")
        mx = float(self.max_intensity)
        if not math.isfinite(mx) or mx <= 0:
            raise CarbonForecastError(
                "max_intensity must be finite and > 0."
            )

        # --- max_history --------------------------------------------- #
        if isinstance(self.max_history, bool) or not isinstance(
            self.max_history, int
        ):
            raise CarbonForecastError("max_history must be an int.")
        if self.max_history <= 0:
            raise CarbonForecastError("max_history must be > 0.")

    def to_dict(self) -> Dict[str, Any]:
        return {**asdict(self), "method": self.method.value}

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        strict: bool = True,
    ) -> "CarbonForecastConfig":
        if not isinstance(data, Mapping):
            raise CarbonForecastError(
                "CarbonForecastConfig.from_dict expects a Mapping."
            )
        valid = set(cls.__dataclass_fields__.keys())
        if strict:
            unknown = [k for k in data if k not in valid]
            if unknown:
                raise CarbonForecastError(
                    f"unknown CarbonForecastConfig keys: {unknown}"
                )
        try:
            kwargs: Dict[str, Any] = {}
            for k, v in data.items():
                if k not in valid:
                    continue
                if k == "method" and isinstance(v, str):
                    # Case-insensitive to accept "P95", "MEAN", "ewma", ...
                    try:
                        kwargs[k] = ForecastMethod(v.lower())
                    except ValueError as exc:
                        raise CarbonForecastError(
                            f"unknown method {v!r}"
                        ) from exc
                else:
                    kwargs[k] = v
            return cls(**kwargs)
        except CarbonForecastError:
            raise
        except (TypeError, ValueError) as exc:
            raise CarbonForecastError(
                f"invalid CarbonForecastConfig payload: {exc}"
            ) from exc

    def __repr__(self) -> str:
        return (
            "CarbonForecastConfig("
            f"window={self.window_size}, "
            f"method={self.method.value!r}, "
            f"alpha={self.ewma_alpha:.3g}, "
            f"max_intensity={self.max_intensity:.3g}, "
            f"max_history={self.max_history})"
        )


# --------------------------------------------------------------------------- #
# Forecast
# --------------------------------------------------------------------------- #
class CarbonForecast:
    """
    Moving-average / EWMA carbon intensity predictor.

    Thread-safe for all public methods. Direct mutation of ``history`` or
    ``window_size`` bypasses validation and locking — prefer the public
    API (``update``, ``update_batch``, ``reset``).

    ``_started_at`` lifecycle
    -------------------------
    Set at construction. ``reset()`` resets it to ``time.time()``.
    ``from_dict()`` restores the serialized value, so
    ``statistics()["uptime_seconds"]`` on a restored instance reports the
    **lifetime of the serialized object**, not the current instance. Call
    ``reset()`` after restoring if you want uptime relative to now.

    ``update()`` failure behaviour (non-strict mode)
    ------------------------------------------------
    Bad samples (non-numeric, ``NaN``, ``inf``, negative, or above
    ``max_intensity``) are coerced to ``0.0`` (or clamped) **and marked**
    in the audit trail with ``"coerced": True`` and ``"raw_repr"``. The
    ``history`` window and EWMA state receive the coerced value, so
    downstream statistics still see the sample — filter ``_observations``
    by ``coerced`` to exclude them.
    """

    def __init__(
        self,
        window_size: int = 24,
        *,
        config: Optional[CarbonForecastConfig] = None,
        strict: bool = True,
    ) -> None:
        if config is not None:
            if not isinstance(config, CarbonForecastConfig):
                raise CarbonForecastError(
                    f"config must be a CarbonForecastConfig, got "
                    f"{type(config).__name__}."
                )
            if window_size != 24:
                logger.warning(
                    "window_size=%r ignored; using "
                    "config.window_size=%d.",
                    window_size, config.window_size,
                )
            self._config = config
        else:
            # Let the config validate window_size (rejects floats, bools).
            self._config = CarbonForecastConfig(window_size=window_size)
        self._strict = bool(strict)

        # Legacy attribute preserved; see class docstring for the caveat.
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

        # Lifecycle counters (cumulative; not reset by reset()).
        self._total_updates: int = 0
        self._dropped_observations: int = 0

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
    def strict(self) -> bool:
        return self._strict

    @property
    def sample_count(self) -> int:
        """Number of samples currently in the prediction window."""
        with self._lock:
            return len(self.history)

    @property
    def total_updates(self) -> int:
        """Total number of ``update()`` calls over the object's lifetime."""
        with self._lock:
            return self._total_updates

    @property
    def observations(self) -> List[Dict[str, Any]]:
        """Snapshot of the audit trail (read-only copy)."""
        with self._lock:
            return [dict(o) for o in self._observations]

    # ---------------------------------------------------------- public API
    def update(self, carbon_intensity: float) -> None:
        """
        Append a new intensity observation.

        Parameters
        ----------
        carbon_intensity : float
            Observed intensity in gCO2/kWh. Must be finite and in
            ``[0, config.max_intensity]`` in strict mode. Non-strict mode
            coerces and marks the record.
        """
        value, coerced, raw_repr = self._validate_intensity(carbon_intensity)
        with self._lock:
            self._apply_update_locked(value, coerced, raw_repr)

    def update_batch(self, values: Iterable[float]) -> int:
        """
        Append multiple observations with one lock acquisition.

        All values are validated **before** any state is mutated, so a
        validation failure in strict mode leaves the forecast unchanged.
        Returns the number of samples accepted.
        """
        validated: List[Tuple[float, bool, Optional[str]]] = []
        for v in values:
            validated.append(self._validate_intensity(v))
        with self._lock:
            for value, coerced, raw_repr in validated:
                self._apply_update_locked(value, coerced, raw_repr)
        return len(validated)

    def predict_next(self) -> float:
        """
        Return the predicted next carbon intensity.

        Legacy behaviour: returns ``0.0`` when no observations have been
        recorded. Use :meth:`predict_next_optional` to distinguish "no
        data" from a genuine zero.
        """
        with self._lock:
            result = self._predict_next_locked(
                list(self.history), self._ewma
            )
        return 0.0 if result is None else result

    def predict_next_optional(self) -> Optional[float]:
        """
        Return the predicted next carbon intensity, or ``None`` when the
        forecast has no observations.
        """
        with self._lock:
            return self._predict_next_locked(list(self.history), self._ewma)

    def reset(self, *, clear_observations: bool = True) -> int:
        """
        Reset the forecast state.

        Returns the number of *buffered* audit-trail entries removed.
        Lifecycle counters (``total_updates``, ``dropped_observations``)
        are **not** reset.
        """
        with self._lock:
            removed = len(self._observations)
            self.history.clear()
            self._ewma = None
            if clear_observations:
                self._observations.clear()
            self._started_at = time.time()
        logger.debug("CarbonForecast reset (removed %d).", removed)
        return removed

    # ---------------------------------------------------------- internals
    def _apply_update_locked(
        self,
        value: float,
        coerced: bool,
        raw_repr: Optional[str],
    ) -> None:
        """Apply one update to the window, EWMA, and audit trail."""
        self.history.append(value)
        if self._ewma is None:
            self._ewma = value
        else:
            alpha = self._config.ewma_alpha
            self._ewma = alpha * value + (1.0 - alpha) * self._ewma

        obs: Dict[str, Any] = {
            "value": value,
            "timestamp": time.time(),
        }
        if coerced:
            obs["coerced"] = True
            if raw_repr is not None:
                obs["raw_repr"] = raw_repr

        self._total_updates += 1
        if len(self._observations) == self._config.max_history:
            self._dropped_observations += 1
        self._observations.append(obs)

    def _predict_next_locked(
        self, values: List[float], ewma: Optional[float]
    ) -> Optional[float]:
        """Compute the prediction from a snapshot. Caller holds the lock."""
        if not values:
            return None
        method = self._config.method
        if method == ForecastMethod.MEAN:
            return sum(values) / len(values)
        if method == ForecastMethod.MEDIAN:
            return float(_statistics.median(values))
        if method == ForecastMethod.EWMA:
            return float(ewma) if ewma is not None else None
        if method == ForecastMethod.P95:
            ordered = sorted(values)
            idx = max(
                0,
                min(
                    len(ordered) - 1,
                    int(round(0.95 * (len(ordered) - 1))),
                ),
            )
            return float(ordered[idx])
        # pragma: no cover — exhaustive over the enum
        return sum(values) / len(values)

    def _validate_intensity(
        self, value: Any
    ) -> Tuple[float, bool, Optional[str]]:
        """
        Return ``(validated_value, was_coerced, raw_repr)``.

        ``raw_repr`` is ``None`` when the value passed validation unchanged;
        otherwise it is ``repr(value)`` truncated to keep the audit trail
        JSON-safe.
        """
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            msg = (
                f"carbon_intensity must be numeric, got "
                f"{type(value).__name__}."
            )
            if self._strict:
                raise CarbonForecastError(msg)
            logger.warning("%s Coercing to 0.0.", msg)
            return 0.0, True, repr(value)
        fv = float(value)
        if math.isnan(fv) or math.isinf(fv):
            msg = f"carbon_intensity must be finite, got {value!r}."
            if self._strict:
                raise CarbonForecastError(msg)
            logger.warning("%s Coercing to 0.0.", msg)
            return 0.0, True, repr(value)
        if fv < 0:
            msg = f"carbon_intensity must be >= 0, got {fv}."
            if self._strict:
                raise CarbonForecastError(msg)
            logger.warning("%s Coercing to 0.0.", msg)
            return 0.0, True, repr(value)
        if fv > self._config.max_intensity:
            msg = (
                f"carbon_intensity must be <= {self._config.max_intensity}, "
                f"got {fv}."
            )
            if self._strict:
                raise CarbonForecastError(msg)
            logger.warning(
                "%s Clamping to %s.", msg, self._config.max_intensity,
            )
            return self._config.max_intensity, True, repr(value)
        return fv, False, None

    # ---------------------------------------------------------- statistics
    def statistics(self) -> CarbonForecastStatistics:
        """Return a JSON-safe, atomic snapshot of the forecast state."""
        with self._lock:
            values = list(self.history)
            obs_count = len(self._observations)
            ewma_state = self._ewma
            total = self._total_updates
            dropped = self._dropped_observations
            uptime = time.time() - self._started_at
            predict = self._predict_next_locked(values, ewma_state)
            if not values:
                return {
                    "sample_count": 0,
                    "total_updates": total,
                    "dropped_observations": dropped,
                    "window_size": self.window_size,
                    "method": self._config.method.value,
                    "predict_next": predict,
                    "mean": 0.0,
                    "median": 0.0,
                    "min": 0.0,
                    "max": 0.0,
                    "ewma": ewma_state,
                    "observations": obs_count,
                    "uptime_seconds": uptime,
                }
            return {
                "sample_count": len(values),
                "total_updates": total,
                "dropped_observations": dropped,
                "window_size": self.window_size,
                "method": self._config.method.value,
                "predict_next": predict,
                "mean": sum(values) / len(values),
                "median": float(_statistics.median(values)),
                "min": min(values),
                "max": max(values),
                "ewma": ewma_state,
                "observations": obs_count,
                "uptime_seconds": uptime,
            }

    # ---------------------------------------------------------- serialization
    def to_dict(
        self, *, include_observations: bool = False
    ) -> Dict[str, Any]:
        with self._lock:
            payload: Dict[str, Any] = {
                "config": self._config.to_dict(),
                "strict": self._strict,
                "history": list(self.history),
                "ewma": self._ewma,
                "started_at": self._started_at,
                "total_updates": self._total_updates,
                "dropped_observations": self._dropped_observations,
            }
            if include_observations:
                payload["observations"] = [
                    dict(o) for o in self._observations
                ]
        return payload

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "CarbonForecast":
        if not isinstance(data, Mapping):
            raise CarbonForecastError(
                f"from_dict expects a Mapping, got {type(data).__name__}."
            )
        try:
            cfg = CarbonForecastConfig.from_dict(
                dict(data.get("config") or {}), strict=True
            )
            forecast = cls(
                config=cfg, strict=bool(data.get("strict", True)),
            )
            with forecast._lock:
                for v in data.get("history", []) or []:
                    fv = float(v)
                    if (
                        not math.isfinite(fv)
                        or fv < 0.0
                        or fv > cfg.max_intensity
                    ):
                        raise CarbonForecastError(
                            f"invalid history value: {v!r}"
                        )
                    forecast.history.append(fv)

                ewma_raw = data.get("ewma")
                if ewma_raw is None:
                    forecast._ewma = None
                else:
                    fe = float(ewma_raw)
                    if (
                        not math.isfinite(fe)
                        or fe < 0.0
                        or fe > cfg.max_intensity
                    ):
                        raise CarbonForecastError(
                            f"invalid ewma value: {ewma_raw!r}"
                        )
                    forecast._ewma = fe

                if data.get("started_at") is not None:
                    forecast._started_at = float(data["started_at"])

                if "total_updates" in data:
                    forecast._total_updates = int(data["total_updates"])
                elif forecast.history:
                    forecast._total_updates = len(forecast.history)
                if "dropped_observations" in data:
                    forecast._dropped_observations = int(
                        data["dropped_observations"]
                    )

                for o in data.get("observations", []) or []:
                    if isinstance(o, Mapping):
                        forecast._observations.append(dict(o))

            return forecast
        except CarbonForecastError:
            raise
        except (TypeError, ValueError) as exc:
            raise CarbonForecastError(
                f"invalid CarbonForecast payload: {exc}"
            ) from exc

    def to_json(
        self, *, include_observations: bool = False, **kwargs: Any
    ) -> str:
        # Payload is primitive-only; no default= needed.
        return json.dumps(
            self.to_dict(include_observations=include_observations), **kwargs
        )

    @classmethod
    def from_json(cls, payload: str) -> "CarbonForecast":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise CarbonForecastError(f"Invalid JSON: {exc}") from exc
        return cls.from_dict(data)

    # ----------------------------------------------------------------- dunder
    def __len__(self) -> int:
        """Number of samples in the window (backward-compatible)."""
        return self.sample_count

    def __repr__(self) -> str:
        with self._lock:
            predict = self._predict_next_locked(
                list(self.history), self._ewma
            )
            predict_display = 0.0 if predict is None else predict
            return (
                "CarbonForecast("
                f"window={self.window_size}, "
                f"method={self._config.method.value}, "
                f"samples={len(self.history)}, "
                f"total={self._total_updates}, "
                f"predict_next={predict_display:.3f})"
            )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, CarbonForecast):
            return NotImplemented
        return self._config == other._config and self._strict == other._strict

    def __hash__(self) -> int:
        return hash((self._config, self._strict))


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "CarbonForecast",
    "CarbonForecastConfig",
    "CarbonForecastError",
    "CarbonForecastStatistics",
    "ForecastMethod",
]


def __dir__() -> List[str]:
    return sorted(__all__)


# --------------------------------------------------------------------------- #
# Smoke test: python -m sustainability.carbon_forecast
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    passed = 0
    failed = 0

    def _check(name: str, cond: bool) -> None:
        global passed, failed
        if cond:
            passed += 1
            print(f"  ✅ {name}")
        else:
            failed += 1
            print(f"  ❌ {name}")

    # ---- 1. All four methods --------------------------------------- #
    print("all methods:")
    expected = {
        "mean":   300.0,
        "median": 300.0,
        "ewma":   None,   # compute below
        "p95":    500.0,
    }
    for method in ForecastMethod:
        f = CarbonForecast(config=CarbonForecastConfig(method=method))
        for v in (100, 200, 300, 400, 500):
            f.update(v)
        p = f.predict_next()
        print(f"  {method.value:<7} predict_next={p:.3f}")
        if method == ForecastMethod.MEAN:
            _check("mean = 300", abs(p - 300.0) < 1e-9)
        elif method == ForecastMethod.MEDIAN:
            _check("median = 300", abs(p - 300.0) < 1e-9)
        elif method == ForecastMethod.EWMA:
            _check("ewma in range", 100.0 <= p <= 500.0)
        elif method == ForecastMethod.P95:
            _check("p95 = 500", abs(p - 500.0) < 1e-9)

    # ---- 2. Bad inputs rejected (strict) --------------------------- #
    print("strict rejection:")
    f = CarbonForecast()
    for bad in (float("nan"), float("inf"), -1.0, "big", None, True):
        try:
            f.update(bad)
            _check(f"reject {bad!r}", False)
        except CarbonForecastError:
            _check(f"reject {bad!r}", True)

    # ---- 3. Non-strict coerce + mark ------------------------------- #
    print("non-strict coercion:")
    lenient = CarbonForecast(strict=False)
    lenient.update(float("nan"))
    obs = lenient.observations
    _check("coerced marked", obs[-1].get("coerced") is True)
    _check("raw_repr stored", "raw_repr" in obs[-1])
    _check("coerced value is 0.0", obs[-1]["value"] == 0.0)
    _check("predict_next = 0.0", lenient.predict_next() == 0.0)

    # ---- 4. Config validation ------------------------------------- #
    print("config validation:")
    for bad_cfg in (
        dict(window_size=0),
        dict(window_size=True),
        dict(window_size=24.9),
        dict(method="bogus"),
        dict(method="mean"),          # must be ForecastMethod instance
        dict(ewma_alpha=0.0),
        dict(ewma_alpha=1.5),
        dict(ewma_alpha="0.5"),
        dict(max_intensity=0),
        dict(max_intensity="100"),
        dict(max_history=0),
        dict(max_history="100"),
    ):
        try:
            CarbonForecastConfig(**bad_cfg)  # type: ignore[arg-type]
            _check(f"reject cfg {list(bad_cfg)}", False)
        except CarbonForecastError:
            _check(f"reject cfg {list(bad_cfg)}", True)

    # ---- 5. from_dict: case-insensitive method -------------------- #
    print("from_dict method coercion:")
    cfg = CarbonForecastConfig.from_dict({"method": "P95"})
    _check("P95 accepted", cfg.method is ForecastMethod.P95)
    cfg = CarbonForecastConfig.from_dict({"method": "ewma"})
    _check("ewma accepted", cfg.method is ForecastMethod.EWMA)
    try:
        CarbonForecastConfig.from_dict({"method": "bogus"})
        _check("bogus rejected", False)
    except CarbonForecastError:
        _check("bogus rejected", True)

    # Unknown keys rejected by default
    try:
        CarbonForecastConfig.from_dict({"typo": 1})
        _check("unknown key rejected", False)
    except CarbonForecastError:
        _check("unknown key rejected", True)
    # ...but accepted with strict=False
    cfg = CarbonForecastConfig.from_dict({"typo": 1}, strict=False)
    _check("unknown key accepted non-strict",
           cfg.window_size == 24)

    # ---- 6. Round-trip with observations (bug fix) ---------------- #
    print("round-trip with observations:")
    f = CarbonForecast()
    for v in (100, 200, 300):
        f.update(v)
    payload = f.to_json(include_observations=True)
    restored = CarbonForecast.from_json(payload)
    _check("history preserved",
           restored.to_dict() == f.to_dict())
    _check("observations preserved",
           len(restored.observations) == len(f.observations))
    _check("predict matches",
           restored.predict_next() == f.predict_next())

    # ---- 7. Atomic statistics ------------------------------------- #
    print("statistics consistency:")
    for method in ForecastMethod:
        f = CarbonForecast(config=CarbonForecastConfig(method=method))
        for v in (100, 200, 300):
            f.update(v)
        stats = f.statistics()
        if method == ForecastMethod.MEAN:
            _check(
                "mean predict matches stats",
                abs(
                    (stats["predict_next"] or 0.0) - stats["mean"]
                ) < 1e-9,
            )
        if method == ForecastMethod.MEDIAN:
            _check(
                "median predict matches stats",
                abs(
                    (stats["predict_next"] or 0.0) - stats["median"]
                ) < 1e-9,
            )

    # ---- 8. predict_next_optional --------------------------------- #
    print("predict_next_optional:")
    empty = CarbonForecast()
    _check("empty returns None", empty.predict_next_optional() is None)
    _check("legacy returns 0.0", empty.predict_next() == 0.0)
    empty.update(0.0)
    _check("single zero returns 0.0",
           empty.predict_next_optional() == 0.0)

    # ---- 9. update_batch ------------------------------------------ #
    print("update_batch:")
    f = CarbonForecast()
    n = f.update_batch([100.0, 200.0, 300.0])
    _check("accepted 3", n == 3)
    _check("total_updates == 3", f.total_updates == 3)
    _check("history size == 3", f.sample_count == 3)
    # Strict failure mid-batch leaves state untouched.
    f2 = CarbonForecast()
    try:
        f2.update_batch([100.0, float("nan")])
        _check("batch raises", False)
    except CarbonForecastError:
        _check("batch raises", True)
        _check("batch rollback", f2.total_updates == 0)

    # ---- 10. StrEnum formatting ----------------------------------- #
    print("StrEnum formatting:")
    _check("str(MEAN) == 'mean'", str(ForecastMethod.MEAN) == "mean")
    _check("str(P95) == 'p95'", str(ForecastMethod.P95) == "p95")
    _check(
        "f-string",
        f"{ForecastMethod.EWMA}" == "ewma",
    )

    # ---- 11. __eq__ / __hash__ ------------------------------------ #
    print("eq/hash:")
    a = CarbonForecast(config=CarbonForecastConfig(window_size=4))
    b = CarbonForecast(config=CarbonForecastConfig(window_size=4))
    _check("equal configs", a == b)
    _check("hash equal", hash(a) == hash(b))
    c = CarbonForecast(config=CarbonForecastConfig(window_size=8))
    _check("different configs", a != c)

    # ---- 12. Counter invariant ------------------------------------ #
    print("counter invariant:")
    f = CarbonForecast(config=CarbonForecastConfig(max_history=3))
    for i in range(5):
        f.update(float(i))
    stats = f.statistics()
    _check("total_updates == 5", stats["total_updates"] == 5)
    _check("dropped_observations == 2",
           stats["dropped_observations"] == 2)
    _check(
        "invariant total == buffered + dropped",
        stats["total_updates"]
        == stats["observations"] + stats["dropped_observations"],
    )

    # ---- 13. from_dict wraps parse errors ------------------------- #
    print("from_dict error wrapping:")
    try:
        CarbonForecast.from_dict({"started_at": "not-a-number"})
        _check("started_at error wrapped", False)
    except CarbonForecastError:
        _check("started_at error wrapped", True)
    try:
        CarbonForecast.from_dict({"history": [float("nan")]})
        _check("nan history rejected", False)
    except CarbonForecastError:
        _check("nan history rejected", True)
    try:
        CarbonForecast.from_dict({"history": [float("inf")]})
        _check("inf history rejected", False)
    except CarbonForecastError:
        _check("inf history rejected", True)

    # ---- 14. len / total_updates / sample_count ------------------- #
    print("len semantics:")
    f = CarbonForecast(config=CarbonForecastConfig(window_size=4))
    for v in (10, 20, 30, 40, 50, 60):
        f.update(float(v))
    _check("len == window_size", len(f) == 4)
    _check("sample_count == window_size", f.sample_count == 4)
    _check("total_updates == 6", f.total_updates == 6)

    print()
    print(f"passed: {passed}  failed: {failed}")
    if failed:
        import sys
        sys.exit(1)
