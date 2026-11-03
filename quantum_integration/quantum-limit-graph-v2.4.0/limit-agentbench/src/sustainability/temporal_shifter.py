# src/sustainability/temporal_shifter.py

"""
Temporal Shifter
================

Decides whether work should be deferred until the grid is cleaner, based on
a forecasted carbon intensity.

Hysteresis anchoring
--------------------
The **enter** boundary is the caller-supplied ``threshold`` (or
``config.default_threshold`` when omitted). The **exit** boundary is
``config.clean_threshold``. A caller-supplied ``threshold`` must therefore
be ``>= config.clean_threshold``; otherwise the state machine becomes
unstable (a value in ``[threshold, clean_threshold)`` toggles the deferral
on every call). This is enforced in **both** strict and non-strict modes:
it is a data-integrity constraint, not a coercion.

Coerced samples do not toggle state
-----------------------------------
When the shifter cannot obtain a real prediction — no forecast
collaborator, the forecast returns ``None``, the forecast raises, or the
caller passes a non-numeric value in non-strict mode — it records the
decision with ``coerced=True`` and returns the current state unchanged.
A hardware fault must not *release* a deferral that was legitimately
engaged, and it must not *engage* one either. The audit trail marks the
skip with a distinct ``reason`` so downstream consumers can distinguish
"real reading" from "fallback".

Enhancements
------------
- ``TemporalShifterConfig`` — frozen, ``slots``, ``__repr__``; every field
  type-checked (``bool`` rejected for ``max_history``; ``NaN``/``inf``
  rejected for floats); ``from_dict(..., strict=True)`` rejects unknown
  keys and wraps parse errors.
- ``ShiftDecision`` — ``slots``; new fields ``release_threshold``,
  ``coerced``, ``raw_error``; ``__post_init__`` validation; ``from_dict``.
- ``ForecastProvider`` Protocol — ``isinstance`` check replaces the
  hand-rolled ``getattr`` loop. ``predict_next_optional`` is detected via
  duck-typing and used when present.
- **Hysteresis anchoring fix** — ``threshold < clean_threshold`` is
  rejected in both modes (was silently accepted with unstable behaviour).
- **Coerced decisions do not toggle state** — no more silent release on
  forecast failure or missing prediction.
- **``from_dict`` / ``from_json``** added to the shifter and
  ``ShiftDecision``.
- **``to_json(include_history=True)`` now works** — the kwarg is routed
  to ``to_dict``; ``default=str`` removed.
- **Lifecycle counters** — ``total_decisions``, ``total_delays``,
  ``total_releases``, ``dropped_decisions``; ``statistics()`` reports
  lifetime counts, not capped at ``len(history)``.
- **Atomic ``statistics()``** — every field read under the lock.
- ``should_delay_detailed() -> ShiftDecision`` and
  ``should_delay_batch(pairs) -> List[bool]``.
- **Distinct hold reasons** — ``hysteresis_hold_delaying`` vs
  ``hysteresis_hold_released`` instead of a single ``hysteresis_hold``.
- ``__eq__`` / ``__hash__`` on config and shifter; ``__dir__``.
- ``__main__`` smoke test uses explicit ``_check()`` helpers so
  ``python -O`` does not silently pass.

Compatibility
-------------
- ``TemporalShifter(*, config=None, forecast=None, strict=True)``
  unchanged.
- ``should_delay(predicted_carbon=None, threshold=None) -> bool``
  unchanged in return type; now records ``coerced=True`` on fallback
  (additive) and rejects ``threshold < clean_threshold`` (breaking).
- ``reset(*, clear_history=True) -> int`` unchanged.
- ``statistics()`` keys ``decisions`` / ``delays`` / ``releases`` now
  report **lifetime** counts; new keys ``buffered_decisions`` and
  ``dropped_decisions`` added.
- ``to_json`` signature is now ``to_json(*, include_history=False,
  **kwargs)`` — the kwarg routes correctly (previously it crashed).
- ``TemporalShifterConfig.from_dict(...)`` now rejects unknown keys by
  default; pass ``strict=False`` to opt out.
- ``TemporalShifter.from_dict(data, forecast=None)`` and
  ``from_json(payload, forecast=None)`` accept an optional forecast
  collaborator (the collaborator is not serialized).

Requires Python 3.8+. ``slots=True`` on dataclasses requires 3.10+;
drop it for older versions.
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
from typing import (
    Any, Deque, Dict, List, Mapping, Optional, Protocol, Sequence, Tuple,
    TypedDict, runtime_checkable,
)

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class TemporalShifterError(ValueError):
    """Raised for invalid temporal-shifter inputs or configuration."""


# --------------------------------------------------------------------------- #
# Typed statistics shape
# --------------------------------------------------------------------------- #
class TemporalShifterStatistics(TypedDict):
    """Return shape of :meth:`TemporalShifter.statistics`."""

    decisions: int
    delays: int
    releases: int
    buffered_decisions: int
    dropped_decisions: int
    currently_delaying: bool
    config: Dict[str, Any]
    strict: bool
    uptime_seconds: float


# --------------------------------------------------------------------------- #
# Forecast provider protocol
# --------------------------------------------------------------------------- #
@runtime_checkable
class ForecastProvider(Protocol):
    """
    Minimal interface the shifter requires from a forecast collaborator.

    ``CarbonForecast`` and any object exposing ``predict_next() -> float``
    satisfy this protocol. If the collaborator also exposes
    ``predict_next_optional() -> Optional[float]``, the shifter will
    prefer it — allowing a "no data" forecast to be distinguished from a
    genuine zero prediction.
    """

    def predict_next(self) -> float: ...


# --------------------------------------------------------------------------- #
# Numeric helpers
# --------------------------------------------------------------------------- #
def _require_finite_nonneg(name: str, value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TemporalShifterError(f"{name} must be numeric.")
    fv = float(value)
    if not math.isfinite(fv) or fv < 0:
        raise TemporalShifterError(
            f"{name} must be finite and >= 0, got {value!r}."
        )
    return fv


def _require_positive_int(name: str, value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TemporalShifterError(f"{name} must be an int.")
    if value <= 0:
        raise TemporalShifterError(f"{name} must be > 0.")
    return value


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class TemporalShifterConfig:
    """
    Tunable parameters for :class:`TemporalShifter`.

    ``clean_threshold`` is the **release** boundary; ``default_threshold``
    is the **enter** boundary and must be strictly greater than it. Caller
    overrides of ``threshold`` must also be ``>= clean_threshold`` (see
    the module docstring).
    """

    default_threshold: float = 400.0
    clean_threshold: float = 350.0
    max_history: int = 1_000

    def __post_init__(self) -> None:
        dt = _require_finite_nonneg(
            "default_threshold", self.default_threshold
        )
        ct = _require_finite_nonneg(
            "clean_threshold", self.clean_threshold
        )
        if ct >= dt:
            raise TemporalShifterError(
                "clean_threshold must be < default_threshold."
            )
        _require_positive_int("max_history", self.max_history)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        strict: bool = True,
    ) -> "TemporalShifterConfig":
        if not isinstance(data, Mapping):
            raise TemporalShifterError(
                "TemporalShifterConfig.from_dict expects a Mapping."
            )
        valid = set(cls.__dataclass_fields__.keys())
        if strict:
            unknown = [k for k in data if k not in valid]
            if unknown:
                raise TemporalShifterError(
                    f"unknown TemporalShifterConfig keys: {unknown}"
                )
        try:
            kwargs = {k: v for k, v in data.items() if k in valid}
            return cls(**kwargs)
        except TemporalShifterError:
            raise
        except (TypeError, ValueError) as exc:
            raise TemporalShifterError(
                f"invalid TemporalShifterConfig payload: {exc}"
            ) from exc

    def __repr__(self) -> str:
        return (
            "TemporalShifterConfig("
            f"default={self.default_threshold:.1f}, "
            f"clean={self.clean_threshold:.1f}, "
            f"max_history={self.max_history})"
        )


# --------------------------------------------------------------------------- #
# Decision
# --------------------------------------------------------------------------- #
@dataclass(frozen=True, slots=True)
class ShiftDecision:
    """
    Immutable record of one shift decision.

    ``coerced`` marks a decision recorded on a fallback path (no forecast,
    failed forecast, empty forecast, or a non-numeric input in non-strict
    mode). A coerced decision does **not** toggle the shifter's state —
    ``should_delay`` is the current state, unchanged.

    ``release_threshold`` records the hysteresis boundary in effect at
    decision time (``config.clean_threshold``). This makes the audit trail
    self-contained: a reader can infer why ``should_delay`` was ``False``
    without inspecting the config that produced the decision.

    ``raw_error`` carries the ``repr`` of the exception that caused a
    coerced skip on the ``forecast_failed_skip`` path. ``None`` on all
    other paths.
    """

    should_delay: bool
    predicted_carbon: float
    threshold: float
    reason: str
    timestamp: datetime = field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    release_threshold: Optional[float] = None
    coerced: bool = False
    raw_error: Optional[str] = None

    def __post_init__(self) -> None:
        if not isinstance(self.should_delay, bool):
            raise TemporalShifterError(
                "should_delay must be a bool."
            )
        for name in ("predicted_carbon", "threshold"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(
                value, (int, float)
            ):
                raise TemporalShifterError(f"{name} must be numeric.")
            fv = float(value)
            if not math.isfinite(fv) or fv < 0:
                raise TemporalShifterError(
                    f"{name} must be finite and >= 0, got {value!r}."
                )
        if not isinstance(self.reason, str) or not self.reason:
            raise TemporalShifterError(
                "reason must be a non-empty string."
            )
        if not isinstance(self.timestamp, datetime):
            raise TemporalShifterError(
                f"timestamp must be a datetime, got "
                f"{type(self.timestamp).__name__}."
            )
        if self.release_threshold is not None:
            if isinstance(self.release_threshold, bool) or not isinstance(
                self.release_threshold, (int, float)
            ):
                raise TemporalShifterError(
                    "release_threshold must be numeric or None."
                )
            fv = float(self.release_threshold)
            if not math.isfinite(fv) or fv < 0:
                raise TemporalShifterError(
                    "release_threshold must be finite and >= 0."
                )
        if not isinstance(self.coerced, bool):
            raise TemporalShifterError("coerced must be a bool.")
        if self.raw_error is not None and not isinstance(
            self.raw_error, str
        ):
            raise TemporalShifterError(
                "raw_error must be str or None."
            )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "should_delay": self.should_delay,
            "predicted_carbon": float(self.predicted_carbon),
            "threshold": float(self.threshold),
            "reason": self.reason,
            "timestamp": self.timestamp.isoformat(),
            "release_threshold": (
                None if self.release_threshold is None
                else float(self.release_threshold)
            ),
            "coerced": self.coerced,
            "raw_error": self.raw_error,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "ShiftDecision":
        if not isinstance(data, Mapping):
            raise TemporalShifterError(
                "ShiftDecision.from_dict expects a Mapping."
            )
        try:
            ts = data.get("timestamp")
            if ts is None:
                timestamp = datetime.now(timezone.utc)
            elif isinstance(ts, str):
                timestamp = datetime.fromisoformat(ts)
                if timestamp.tzinfo is None:
                    timestamp = timestamp.replace(tzinfo=timezone.utc)
            elif isinstance(ts, datetime):
                timestamp = ts
            else:
                raise TemporalShifterError(
                    f"timestamp must be str|datetime|None, "
                    f"got {type(ts).__name__}."
                )
            return cls(
                should_delay=bool(data.get("should_delay", False)),
                predicted_carbon=float(
                    data.get("predicted_carbon", 0.0)
                ),
                threshold=float(data.get("threshold", 0.0)),
                reason=str(data.get("reason", "unknown")),
                timestamp=timestamp,
                release_threshold=(
                    None if data.get("release_threshold") is None
                    else float(data["release_threshold"])
                ),
                coerced=bool(data.get("coerced", False)),
                raw_error=(
                    None if data.get("raw_error") is None
                    else str(data["raw_error"])
                ),
            )
        except TemporalShifterError:
            raise
        except (TypeError, ValueError) as exc:
            raise TemporalShifterError(
                f"invalid ShiftDecision payload: {exc}"
            ) from exc

    def __repr__(self) -> str:
        suffix = ""
        if self.coerced:
            suffix = f", coerced=True, reason={self.reason!r}"
        return (
            "ShiftDecision("
            f"should_delay={self.should_delay}, "
            f"predicted={self.predicted_carbon:.1f}, "
            f"threshold={self.threshold:.1f}{suffix})"
        )


# --------------------------------------------------------------------------- #
# Shifter
# --------------------------------------------------------------------------- #
class TemporalShifter:
    """
    Decides whether work should be deferred until the grid is cleaner.

    Thread-safe for all public methods. Direct mutation of ``forecast`` or
    ``_was_delaying`` bypasses locking — prefer the public API.

    ``_started_at`` lifecycle
    -------------------------
    Set at construction. :meth:`reset` resets it to ``time.time()``.
    :meth:`from_dict` restores the serialized value, so
    ``statistics()["uptime_seconds"]`` on a restored shifter reports the
    **lifetime of the serialized object**, not the current instance. Call
    :meth:`reset` after restoring if you want uptime relative to now.

    ``should_delay`` fallback semantics
    -----------------------------------
    If the shifter cannot obtain a real prediction — no forecast
    collaborator, empty forecast via ``predict_next_optional()``,
    ``predict_next()`` raises, or a non-numeric value in non-strict mode —
    it records a decision with ``coerced=True`` and returns the current
    state unchanged. See the module docstring.
    """

    def __init__(
        self,
        *,
        config: Optional[TemporalShifterConfig] = None,
        forecast: Optional[ForecastProvider] = None,
        strict: bool = True,
    ) -> None:
        if config is not None and not isinstance(
            config, TemporalShifterConfig
        ):
            raise TemporalShifterError(
                f"config must be a TemporalShifterConfig, got "
                f"{type(config).__name__}."
            )
        self._config = config or TemporalShifterConfig()
        self._strict = bool(strict)

        if forecast is not None and not isinstance(
            forecast, ForecastProvider
        ):
            raise TemporalShifterError(
                f"forecast must satisfy ForecastProvider (needs "
                f"predict_next), got {type(forecast).__name__}."
            )
        self.forecast = forecast

        self._lock = threading.RLock()
        self._history: Deque[ShiftDecision] = deque(
            maxlen=self._config.max_history
        )
        self._was_delaying: bool = False
        self._started_at: float = time.time()

        # Lifecycle counters (cumulative; not reset by reset()).
        self._total_decisions: int = 0
        self._total_delays: int = 0
        self._total_releases: int = 0
        self._dropped_decisions: int = 0

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
    def strict(self) -> bool:
        return self._strict

    @property
    def is_delaying(self) -> bool:
        """Current hysteresis state (True when work should be deferred)."""
        with self._lock:
            return self._was_delaying

    @property
    def decisions(self) -> List[ShiftDecision]:
        """Read-only snapshot of the decision history."""
        with self._lock:
            return list(self._history)

    @property
    def total_decisions(self) -> int:
        with self._lock:
            return self._total_decisions

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
            Forecasted carbon intensity. When ``None``, the shifter uses
            its ``forecast`` collaborator's ``predict_next_optional()`` or
            ``predict_next()`` if available; otherwise it records a
            coerced skip.
        threshold : float, optional
            **Enter** threshold. Defaults to ``config.default_threshold``.
            Must be ``>= config.clean_threshold`` (the release boundary).

        Returns
        -------
        bool
            True if the work should be deferred.
        """
        return self._decide(predicted_carbon, threshold).should_delay

    def should_delay_detailed(
        self,
        predicted_carbon: Optional[float] = None,
        threshold: Optional[float] = None,
    ) -> ShiftDecision:
        """Like :meth:`should_delay`, but returns the full record."""
        return self._decide(predicted_carbon, threshold)

    def should_delay_batch(
        self,
        pairs: Sequence[Tuple[Optional[float], Optional[float]]],
    ) -> List[bool]:
        """
        Apply a batch of ``(predicted_carbon, threshold)`` pairs.

        Sequential (each decision is stateful); validates format only.
        """
        if not isinstance(pairs, Sequence):
            raise TemporalShifterError("pairs must be a Sequence.")
        results: List[bool] = []
        for pair in pairs:
            if not (isinstance(pair, tuple) and len(pair) == 2):
                raise TemporalShifterError(
                    "each pair must be "
                    "(predicted_carbon, threshold)."
                )
            results.append(
                self._decide(pair[0], pair[1]).should_delay
            )
        return results

    def reset(self, *, clear_history: bool = True) -> int:
        """
        Reset the shifter. Returns buffered entries removed.

        Lifecycle counters are **not** reset.
        """
        with self._lock:
            removed = len(self._history)
            self._was_delaying = False
            if clear_history:
                self._history.clear()
            self._started_at = time.time()
        logger.debug("TemporalShifter reset (removed %d).", removed)
        return removed

    # ---------------------------------------------------------- internals
    def _decide(
        self,
        predicted_carbon: Optional[float],
        threshold: Optional[float],
    ) -> ShiftDecision:
        """Compute, record, and return one decision."""
        t = self._resolve_threshold(threshold)
        value, coerced, skip_reason, raw_error = self._resolve_prediction(
            predicted_carbon, t,
        )

        with self._lock:
            current = self._was_delaying
            if coerced:
                should_delay = current
                reason = skip_reason or "coerced_skip"
            else:
                if not current and value > t:
                    should_delay = True
                    reason = "predicted_above_threshold"
                elif current and value < self._config.clean_threshold:
                    should_delay = False
                    reason = "predicted_below_clean_threshold"
                elif current:
                    should_delay = True
                    reason = "hysteresis_hold_delaying"
                else:
                    should_delay = False
                    reason = "hysteresis_hold_released"
                self._was_delaying = should_delay

            decision = ShiftDecision(
                should_delay=should_delay,
                predicted_carbon=value,
                threshold=t,
                reason=reason,
                release_threshold=self._config.clean_threshold,
                coerced=coerced,
                raw_error=raw_error,
            )
            self._record_locked(decision)

        logger.debug(
            "Shift decision: predicted=%.1f threshold=%.1f -> %s "
            "(reason=%s, coerced=%s)",
            value, t, should_delay, reason, coerced,
        )
        return decision

    def _resolve_threshold(self, threshold: Optional[float]) -> float:
        """Validate the caller's threshold; reject unstable values."""
        if threshold is None:
            return self._config.default_threshold
        t = _require_finite_nonneg("threshold", threshold)
        if t < self._config.clean_threshold:
            raise TemporalShifterError(
                f"threshold={t} must be >= clean_threshold="
                f"{self._config.clean_threshold} (hysteresis is anchored "
                f"to config.clean_threshold; lower values cause the "
                f"state machine to toggle on every call)."
            )
        return t

    def _resolve_prediction(
        self,
        predicted_carbon: Optional[float],
        threshold: float,  # noqa: ARG002 — for future per-threshold logic
    ) -> Tuple[float, bool, Optional[str], Optional[str]]:
        """
        Resolve the prediction value.

        Returns ``(value, coerced, skip_reason, raw_error)``.
        """
        if predicted_carbon is not None:
            # Validate; coerce in non-strict, raise in strict.
            try:
                return (
                    self._validate_prediction(predicted_carbon),
                    False, None, None,
                )
            except TemporalShifterError:
                if self._strict:
                    raise
                # Non-strict coercion path — mark and skip.
                logger.warning(
                    "predicted_carbon=%r invalid in non-strict mode; "
                    "recording a coerced skip.",
                    predicted_carbon,
                )
                return (
                    0.0, True, "input_coerced_skip",
                    repr(predicted_carbon),
                )

        if self.forecast is None:
            logger.warning(
                "should_delay called with no prediction and no "
                "forecast collaborator; recording a coerced skip."
            )
            return 0.0, True, "no_forecast_skip", None

        # ---- Prefer predict_next_optional when available ---------- #
        optional = getattr(self.forecast, "predict_next_optional", None)
        if callable(optional):
            try:
                opt = optional()
            except Exception as exc:
                if self._strict:
                    raise TemporalShifterError(
                        f"forecast.predict_next_optional failed: {exc}"
                    ) from exc
                logger.exception(
                    "forecast.predict_next_optional failed: %s", exc,
                )
                return (
                    0.0, True, "forecast_failed_skip", repr(exc),
                )
            if opt is None:
                logger.debug(
                    "forecast.predict_next_optional returned None; "
                    "recording a coerced skip."
                )
                return 0.0, True, "forecast_empty_skip", None
            try:
                return self._validate_prediction(opt), False, None, None
            except TemporalShifterError as exc:
                if self._strict:
                    raise
                logger.exception(
                    "forecast.predict_next_optional returned invalid "
                    "value: %s", exc,
                )
                return (
                    0.0, True, "forecast_failed_skip", repr(exc),
                )

        # ---- Fall back to predict_next --------------------------- #
        try:
            raw = self.forecast.predict_next()
        except Exception as exc:
            if self._strict:
                raise TemporalShifterError(
                    f"forecast.predict_next failed: {exc}"
                ) from exc
            logger.exception(
                "forecast.predict_next failed: %s", exc,
            )
            return 0.0, True, "forecast_failed_skip", repr(exc)

        try:
            return self._validate_prediction(raw), False, None, None
        except TemporalShifterError:
            if self._strict:
                raise
            logger.warning(
                "forecast.predict_next returned invalid value %r; "
                "recording a coerced skip.", raw,
            )
            return (
                0.0, True, "forecast_failed_skip", repr(raw),
            )

    def _validate_prediction(self, value: Any) -> float:
        """Strict-validate a numeric prediction."""
        return _require_finite_nonneg("predicted_carbon", value)

    def _record_locked(self, decision: ShiftDecision) -> None:
        """Append + bump counters. Caller must hold ``self._lock``."""
        self._total_decisions += 1
        if decision.should_delay:
            self._total_delays += 1
        else:
            self._total_releases += 1
        if len(self._history) == self._config.max_history:
            self._dropped_decisions += 1
        self._history.append(decision)

    # ---------------------------------------------------------- statistics
    def statistics(self) -> TemporalShifterStatistics:
        """Return a JSON-safe, atomic snapshot of the shifter state."""
        with self._lock:
            return {
                "decisions": self._total_decisions,
                "delays": self._total_delays,
                "releases": self._total_releases,
                "buffered_decisions": len(self._history),
                "dropped_decisions": self._dropped_decisions,
                "currently_delaying": self._was_delaying,
                "config": self._config.to_dict(),
                "strict": self._strict,
                "uptime_seconds": time.time() - self._started_at,
            }

    # ---------------------------------------------------------- serialization
    def to_dict(
        self, *, include_history: bool = False
    ) -> Dict[str, Any]:
        with self._lock:
            payload: Dict[str, Any] = {
                "config": self._config.to_dict(),
                "strict": self._strict,
                "was_delaying": self._was_delaying,
                "started_at": self._started_at,
                "total_decisions": self._total_decisions,
                "total_delays": self._total_delays,
                "total_releases": self._total_releases,
                "dropped_decisions": self._dropped_decisions,
            }
            if include_history:
                payload["history"] = [
                    d.to_dict() for d in self._history
                ]
        return payload

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, Any],
        *,
        forecast: Optional[ForecastProvider] = None,
    ) -> "TemporalShifter":
        """
        Reconstruct a shifter from a ``to_dict()`` payload.

        ``forecast`` is optional; the collaborator is not serialized, so
        a restored shifter is only useful for further ``should_delay``
        calls when a fresh forecast collaborator (or an explicit
        ``predicted_carbon`` per call) is provided.
        """
        if not isinstance(data, Mapping):
            raise TemporalShifterError(
                f"from_dict expects a Mapping, got "
                f"{type(data).__name__}."
            )
        try:
            cfg = TemporalShifterConfig.from_dict(
                dict(data.get("config") or {}), strict=True,
            )
            shifter = cls(
                config=cfg,
                forecast=forecast,
                strict=bool(data.get("strict", True)),
            )
            with shifter._lock:
                if "history" not in data:
                    logger.info(
                        "from_dict: payload has no 'history' key; "
                        "restoring empty decision history."
                    )
                for entry in data.get("history", []) or []:
                    shifter._history.append(
                        ShiftDecision.from_dict(entry)
                    )

                shifter._was_delaying = bool(
                    data.get("was_delaying", False)
                )
                if data.get("started_at") is not None:
                    shifter._started_at = float(data["started_at"])

                shifter._total_decisions = int(
                    data.get("total_decisions", 0)
                )
                shifter._total_delays = int(
                    data.get("total_delays", 0)
                )
                shifter._total_releases = int(
                    data.get("total_releases", 0)
                )
                shifter._dropped_decisions = int(
                    data.get("dropped_decisions", 0)
                )
                # Derive counters if the payload lacked them.
                if (
                    "total_decisions" not in data
                    and shifter._history
                ):
                    shifter._total_decisions = len(shifter._history)
                    shifter._total_delays = sum(
                        1 for d in shifter._history if d.should_delay
                    )
                    shifter._total_releases = (
                        shifter._total_decisions
                        - shifter._total_delays
                    )
            return shifter
        except TemporalShifterError:
            raise
        except (TypeError, ValueError) as exc:
            raise TemporalShifterError(
                f"invalid TemporalShifter payload: {exc}"
            ) from exc

    def to_json(
        self, *, include_history: bool = False, **kwargs: Any
    ) -> str:
        # Payload is primitive-only; no default= needed.
        return json.dumps(
            self.to_dict(include_history=include_history), **kwargs
        )

    @classmethod
    def from_json(
        cls,
        payload: str,
        *,
        forecast: Optional[ForecastProvider] = None,
    ) -> "TemporalShifter":
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise TemporalShifterError(
                f"Invalid JSON: {exc}"
            ) from exc
        return cls.from_dict(data, forecast=forecast)

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        with self._lock:
            return (
                "TemporalShifter("
                f"threshold={self._config.default_threshold:.1f}, "
                f"currently_delaying={self._was_delaying}, "
                f"decisions={self._total_decisions})"
            )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, TemporalShifter):
            return NotImplemented
        return self._config == other._config and self._strict == other._strict

    def __hash__(self) -> int:
        return hash((self._config, self._strict))


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "ForecastProvider",
    "ShiftDecision",
    "TemporalShifter",
    "TemporalShifterConfig",
    "TemporalShifterError",
    "TemporalShifterStatistics",
]


def __dir__() -> List[str]:
    return sorted(__all__)


# --------------------------------------------------------------------------- #
# Smoke test: python -m sustainability.temporal_shifter
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    import sys

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

    # ---- 1. Basic hysteresis ------------------------------------- #
    print("basic hysteresis:")
    shifter = TemporalShifter()
    print("  repr:", shifter)
    _check(
        "enter above threshold",
        shifter.should_delay(predicted_carbon=500.0) is True,
    )
    _check(
        "hysteresis hold (delaying, between clean and dirty)",
        shifter.should_delay(predicted_carbon=380.0) is True,
    )
    _check(
        "exit below clean",
        shifter.should_delay(predicted_carbon=300.0) is False,
    )
    _check(
        "hysteresis hold (released, between clean and dirty)",
        shifter.should_delay(predicted_carbon=380.0) is False,
    )

    # ---- 2. Threshold anchoring ---------------------------------- #
    print("threshold anchoring:")
    s = TemporalShifter()
    try:
        s.should_delay(predicted_carbon=250.0, threshold=200.0)
        _check("threshold < clean_threshold rejected", False)
    except TemporalShifterError:
        _check("threshold < clean_threshold rejected", True)

    # At exactly clean_threshold
    s = TemporalShifter()
    _check(
        "threshold == clean_threshold accepted",
        s.should_delay(
            predicted_carbon=360.0, threshold=350.0,
        ) is True,
    )

    # ---- 3. Coerced skip preserves state ------------------------- #
    print("coerced skip preserves state:")
    # no forecast, no arg → skip
    s = TemporalShifter()
    _check(
        "no-forecast skip preserves state (not delaying)",
        s.should_delay() is False,
    )
    _check(
        "skip recorded with coerced=True",
        s.decisions[-1].coerced is True,
    )
    _check(
        "skip reason",
        s.decisions[-1].reason == "no_forecast_skip",
    )

    # Now enter, then trigger skip — state must remain delaying
    s = TemporalShifter()
    s.should_delay(predicted_carbon=500.0)
    _check("was delaying", s.is_delaying is True)
    _check("skip still returns True", s.should_delay() is True)
    _check(
        "coerced skip does not release",
        s.is_delaying is True,
    )

    # ---- 4. Forecast collaborator -------------------------------- #
    print("forecast collaborator:")

    class _OKForecast:
        def predict_next(self) -> float:
            return 450.0

    s = TemporalShifter(forecast=_OKForecast())
    _check(
        "forecast engaged",
        s.should_delay() is True,
    )

    class _EmptyForecast:
        def predict_next(self) -> float:
            return 0.0

        def predict_next_optional(self) -> Optional[float]:
            return None

    s = TemporalShifter(forecast=_EmptyForecast())
    _check(
        "empty forecast → coerced skip",
        s.should_delay() is False,
    )
    _check(
        "empty forecast reason",
        s.decisions[-1].reason == "forecast_empty_skip",
    )

    class _RaiseForecast:
        def predict_next(self) -> float:
            raise RuntimeError("sensor lost")

    s = TemporalShifter(forecast=_RaiseForecast(), strict=False)
    _check(
        "failed forecast → coerced skip (non-strict)",
        s.should_delay() is False,
    )
    _check(
        "failed forecast reason",
        s.decisions[-1].reason == "forecast_failed_skip",
    )
    _check(
        "raw_error recorded",
        s.decisions[-1].raw_error is not None,
    )

    # Strict mode: raise on forecast failure
    s = TemporalShifter(forecast=_RaiseForecast(), strict=True)
    try:
        s.should_delay()
        _check("strict forecast failure raises", False)
    except TemporalShifterError:
        _check("strict forecast failure raises", True)

    # ---- 5. Bad input rejected ----------------------------------- #
    print("bad input:")
    s = TemporalShifter()
    for bad in (float("nan"), float("inf"), -1.0, "big", None, True):
        try:
            s.should_delay(predicted_carbon=bad)
            _check(f"reject {bad!r}", False)
        except TemporalShifterError:
            _check(f"reject {bad!r}", True)

    # Non-strict coerces
    lenient = TemporalShifter(strict=False)
    _check(
        "non-strict bad input → coerced skip",
        lenient.should_delay(predicted_carbon=float("nan")) is False,
    )
    _check(
        "coerced skip reason",
        lenient.decisions[-1].reason == "input_coerced_skip",
    )

    # ---- 6. Config validation ------------------------------------ #
    print("config validation:")
    for bad_cfg in (
        dict(default_threshold=-1),
        dict(default_threshold=float("nan")),
        dict(clean_threshold=500, default_threshold=400),
        dict(max_history=0),
        dict(max_history=True),
        dict(max_history="100"),
    ):
        try:
            TemporalShifterConfig(**bad_cfg)  # type: ignore[arg-type]
            _check(f"reject cfg {list(bad_cfg)}", False)
        except TemporalShifterError:
            _check(f"reject cfg {list(bad_cfg)}", True)

    # Config.from_dict strict
    try:
        TemporalShifterConfig.from_dict({"typo": 1}, strict=True)
        _check("config unknown key rejected", False)
    except TemporalShifterError:
        _check("config unknown key rejected", True)
    cfg = TemporalShifterConfig.from_dict({"typo": 1}, strict=False)
    _check(
        "config unknown key accepted non-strict",
        cfg.default_threshold == 400.0,
    )

    # ---- 7. ForecastProvider protocol check ---------------------- #
    print("ForecastProvider protocol:")
    try:
        TemporalShifter(forecast=object())  # type: ignore[arg-type]
        _check("non-forecast rejected", False)
    except TemporalShifterError:
        _check("non-forecast rejected", True)

    # ---- 8. Lifecycle counters ----------------------------------- #
    print("counter invariant:")
    s = TemporalShifter(
        config=TemporalShifterConfig(max_history=3)
    )
    for v in (500.0, 380.0, 300.0, 380.0, 500.0):
        s.should_delay(predicted_carbon=v)
    stats = s.statistics()
    _check("total_decisions == 5", stats["decisions"] == 5)
    _check(
        "delays + releases == decisions",
        stats["delays"] + stats["releases"] == stats["decisions"],
    )
    _check(
        "invariant total == buffered + dropped",
        stats["decisions"]
        == stats["buffered_decisions"]
        + stats["dropped_decisions"],
    )

    # ---- 9. Round-trip ------------------------------------------- #
    print("round-trip:")
    class _F:
        def predict_next(self) -> float:
            return 450.0

    s = TemporalShifter(forecast=_F(), strict=True)
    s.should_delay(predicted_carbon=500.0)
    s.should_delay(predicted_carbon=300.0)
    payload = s.to_json(include_history=True)
    restored = TemporalShifter.from_json(payload)
    _check(
        "history preserved",
        len(restored.decisions) == len(s.decisions),
    )
    _check(
        "was_delaying preserved",
        restored.is_delaying == s.is_delaying,
    )
    _check(
        "counters preserved",
        restored.statistics()["decisions"]
        == s.statistics()["decisions"],
    )
    _check("eq works", restored == s)

    # include_history=False now works (was TypeError before)
    payload2 = s.to_json(include_history=False)
    _check(
        "include_history=False: no history key",
        "history" not in json.loads(payload2),
    )

    # ---- 10. ShiftDecision validation ---------------------------- #
    print("ShiftDecision validation:")
    for bad in (
        dict(should_delay="yes", predicted_carbon=100.0,
             threshold=400.0, reason="x"),
        dict(should_delay=True, predicted_carbon=float("nan"),
             threshold=400.0, reason="x"),
        dict(should_delay=True, predicted_carbon=100.0,
             threshold=-1.0, reason="x"),
        dict(should_delay=True, predicted_carbon=100.0,
             threshold=400.0, reason=""),
        dict(should_delay=True, predicted_carbon=100.0,
             threshold=400.0, reason="x",
             timestamp="not-a-datetime"),
    ):
        try:
            ShiftDecision(**bad)  # type: ignore[arg-type]
            _check(f"reject decision {list(bad)}", False)
        except TemporalShifterError:
            _check(f"reject decision {list(bad)}", True)

    # ---- 11. from_dict error wrapping ---------------------------- #
    print("from_dict error wrapping:")
    try:
        TemporalShifter.from_dict({"started_at": "not-a-number"})
        _check("bad started_at rejected", False)
    except TemporalShifterError:
        _check("bad started_at rejected", True)

    try:
        TemporalShifter.from_dict(
            {"history": [{"should_delay": "yes"}]}
        )
        _check("bad history entry rejected", False)
    except TemporalShifterError:
        _check("bad history entry rejected", True)

    # ---- 12. should_delay_detailed ------------------------------- #
    print("should_delay_detailed:")
    s = TemporalShifter()
    decision = s.should_delay_detailed(predicted_carbon=500.0)
    _check(
        "detailed returns ShiftDecision",
        isinstance(decision, ShiftDecision),
    )
    _check("detailed should_delay=True", decision.should_delay is True)
    _check(
        "detailed reason",
        decision.reason == "predicted_above_threshold",
    )
    _check(
        "release_threshold recorded",
        decision.release_threshold == 350.0,
    )

    # ---- 13. should_delay_batch --------------------------------- #
    print("should_delay_batch:")
    s = TemporalShifter()
    results = s.should_delay_batch([
        (500.0, None),
        (380.0, None),   # hysteresis hold
        (300.0, None),
    ])
    _check(
        "batch results match expectations",
        results == [True, True, False],
    )

    # ---- 14. Atomic statistics ---------------------------------- #
    print("statistics atomicity:")
    s = TemporalShifter()
    s.should_delay(predicted_carbon=500.0)
    stats = s.statistics()
    _check(
        "currently_delaying matches",
        stats["currently_delaying"] == s.is_delaying,
    )
    _check(
        "decisions matches total_decisions",
        stats["decisions"] == s.total_decisions,
    )

    # ---- 15. eq / hash ----------------------------------------- #
    print("eq/hash:")
    a = TemporalShifter(
        config=TemporalShifterConfig(default_threshold=500.0),
    )
    b = TemporalShifter(
        config=TemporalShifterConfig(default_threshold=500.0),
    )
    c = TemporalShifter(
        config=TemporalShifterConfig(default_threshold=600.0),
    )
    _check("equal configs", a == b)
    _check("hash equal", hash(a) == hash(b))
    _check("different configs", a != c)

    # ---- 16. dir ---------------------------------------------- #
    print("dir:")
    _check("__dir__ matches __all__", __dir__() == sorted(__all__))

    # ---- 17. Distinct hold reasons ---------------------------- #
    print("distinct hold reasons:")
    s = TemporalShifter()
    s.should_delay(predicted_carbon=500.0)  # enter
    s.should_delay(predicted_carbon=380.0)  # hold
    _check(
        "hysteresis hold (delaying) reason",
        s.decisions[-1].reason == "hysteresis_hold_delaying",
    )
    s.should_delay(predicted_carbon=300.0)  # release
    s.should_delay(predicted_carbon=380.0)  # hold
    _check(
        "hysteresis hold (released) reason",
        s.decisions[-1].reason == "hysteresis_hold_released",
    )

    print()
    print(f"passed: {passed}  failed: {failed}")
    if failed:
        sys.exit(1)
