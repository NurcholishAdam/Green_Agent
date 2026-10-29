# src/instrumentation/energy/base_meter.py

"""
Base Energy Meter
=================

Abstract base class for energy measurement backends.

Enhancements
------------
- :class:`Provenance` — closed string-enum vocabulary replacing free-form
  provenance strings.
- :class:`BaseMeterConfig` — frozen, fully validated (booleans, tuple
  types, numeric bounds); exposed as ``DEFAULT_CONFIG`` so meters share
  a single policy object by default.
- :class:`TelemetrySample` — frozen, validated; new fields
  ``sanitized`` (was any field coerced from a contract violation?) and
  ``correlation_id``; ``extras`` is now a read-only ``MappingProxyType``.
  ``from_dict`` / ``from_metrics`` raise :class:`InstrumentationError`
  on any parse failure and reject ``bool`` for numeric fields.
- :meth:`BaseEnergyMeter.measure` — context manager now:
    * preserves the body's exception when the meter *also* fails during
      exit (previously the meter's :class:`InstrumentationError`
      masked the real cause);
    * enforces ``min_measurement_seconds`` by marking short-window
      samples ``sanitized``;
    * records the meter's own measured elapsed (from
      :meth:`_mark_stopped`) on the sample when available;
    * forbids concurrent measurement on the same instance via a
      dedicated lock;
    * accepts ``correlation_id`` and propagates it to the sample.
- :meth:`BaseEnergyMeter._validate_metrics` continues to return a
  plain dict for backwards compatibility, and additionally sets
  ``self._last_sanitized = True`` when any field was coerced. This
  gives subclasses (and :meth:`measure`) a way to distinguish a clean
  reading from a fallback-filled one.
- ``last_sample`` initialized in ``__init__``; ``strict`` and the
  config's boolean fields are required to be actual ``bool`` values
  (no silent ``bool("yes")`` coercion).
- ``__repr__`` on config, sample, and meter.
- ``__main__`` smoke test with strict / lenient / body-exception /
  minimum-window regression cases.
"""

from __future__ import annotations

import json
import logging
import math
import threading
import time
from abc import ABC, abstractmethod
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from enum import Enum
from types import MappingProxyType
from typing import Any, Dict, Iterator, Mapping, Optional, Tuple

logger = logging.getLogger(__name__)

__all__ = [
    "BaseEnergyMeter",
    "BaseMeterConfig",
    "DEFAULT_CONFIG",
    "InstrumentationError",
    "Provenance",
    "TelemetrySample",
]


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class InstrumentationError(ValueError):
    """Raised for invalid meter inputs, configuration, or contract violations."""


# --------------------------------------------------------------------------- #
# Provenance
# --------------------------------------------------------------------------- #
class Provenance(str, Enum):
    """Closed vocabulary for the source of a measurement."""

    MEASURED = "measured"
    ESTIMATED = "estimated"
    SIMULATED = "simulated"

    def __str__(self) -> str:
        return self.value


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
_DEFAULT_REQUIRED_KEYS: Tuple[str, ...] = (
    "energy_joules",
    "power_watts",
    "provenance",
)
_DEFAULT_ALLOWED_PROVENANCE: Tuple[str, ...] = tuple(
    p.value for p in Provenance
)


@dataclass(frozen=True)
class BaseMeterConfig:
    """
    Contract-level configuration shared by every energy meter.

    Concrete meters may extend this or accept it as a base config.

    Semantics of ``enforce_contract`` and ``strict``:

    * ``enforce_contract=True`` (default) makes contract violations
      raise regardless of the meter's ``strict`` flag. This is the
      "never silently accept bad data" mode.
    * To get lenient behavior (log + coerce) the meter must be
      constructed with **both** ``strict=False`` **and**
      ``config=BaseMeterConfig(enforce_contract=False, ...)``.
    """

    # Enforce the ``stop()`` contract (energy_joules / power_watts /
    # provenance).
    enforce_contract: bool = True

    # Which keys the returned dict must contain (at minimum).
    required_keys: Tuple[str, ...] = _DEFAULT_REQUIRED_KEYS

    # Allowed values for ``provenance``.
    allowed_provenance: Tuple[str, ...] = _DEFAULT_ALLOWED_PROVENANCE

    # Reject negative energy/power readings.
    require_non_negative: bool = True

    # Samples whose measured elapsed time is shorter than this threshold
    # are marked ``sanitized`` — very short windows are usually noise.
    min_measurement_seconds: float = 0.0

    def __post_init__(self) -> None:
        if not isinstance(self.enforce_contract, bool):
            raise InstrumentationError("enforce_contract must be a bool")
        if not isinstance(self.require_non_negative, bool):
            raise InstrumentationError("require_non_negative must be a bool")

        if (
            not isinstance(self.required_keys, tuple)
            or not self.required_keys
        ):
            raise InstrumentationError(
                "required_keys must be a non-empty tuple"
            )
        if (
            not isinstance(self.allowed_provenance, tuple)
            or not self.allowed_provenance
        ):
            raise InstrumentationError(
                "allowed_provenance must be a non-empty tuple"
            )
        for name in ("required_keys", "allowed_provenance"):
            for value in getattr(self, name):
                if not isinstance(value, str) or not value:
                    raise InstrumentationError(
                        f"{name} entries must be non-empty strings"
                    )

        if isinstance(self.min_measurement_seconds, bool) or not isinstance(
            self.min_measurement_seconds, (int, float)
        ):
            raise InstrumentationError(
                "min_measurement_seconds must be a number"
            )
        if not math.isfinite(float(self.min_measurement_seconds)):
            raise InstrumentationError(
                "min_measurement_seconds must be finite"
            )
        if float(self.min_measurement_seconds) < 0:
            raise InstrumentationError(
                "min_measurement_seconds must be >= 0"
            )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


DEFAULT_CONFIG: BaseMeterConfig = BaseMeterConfig()


# --------------------------------------------------------------------------- #
# Telemetry sample
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class TelemetrySample:
    """
    Immutable, contract-validated reading from an energy meter.

    ``sanitized=True`` means at least one field was coerced from a
    contract violation (missing key, wrong type, negative, bad
    provenance) or the measurement window was shorter than
    ``BaseMeterConfig.min_measurement_seconds``. Downstream consumers
    that gate on the reading — e.g. a sustainability budget check —
    should treat sanitized samples as "unknown" rather than as
    measured data.

    ``extras`` is a read-only mapping; callers cannot mutate the sample
    after construction.
    """

    energy_joules: float
    power_watts: float
    provenance: Provenance
    source: Optional[str] = None
    elapsed_seconds: Optional[float] = None
    extras: Mapping[str, Any] = field(default_factory=dict)
    sanitized: bool = False
    correlation_id: Optional[str] = None

    def __post_init__(self) -> None:
        for name in ("energy_joules", "power_watts"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(
                value, (int, float)
            ):
                raise InstrumentationError(
                    f"{name} must be numeric, got "
                    f"{type(value).__name__}"
                )
            fv = float(value)
            if not math.isfinite(fv):
                raise InstrumentationError(
                    f"{name} must be finite, got {value!r}"
                )
            if fv < 0:
                raise InstrumentationError(
                    f"{name} must be >= 0, got {fv}"
                )
            object.__setattr__(self, name, fv)

        if not isinstance(self.provenance, Provenance):
            try:
                object.__setattr__(
                    self, "provenance", Provenance(self.provenance)
                )
            except (ValueError, TypeError) as exc:
                raise InstrumentationError(
                    f"provenance must be one of "
                    f"{[p.value for p in Provenance]}, got "
                    f"{self.provenance!r}"
                ) from exc

        if self.elapsed_seconds is not None:
            if isinstance(self.elapsed_seconds, bool) or not isinstance(
                self.elapsed_seconds, (int, float)
            ):
                raise InstrumentationError(
                    f"elapsed_seconds must be numeric, got "
                    f"{type(self.elapsed_seconds).__name__}"
                )
            e = float(self.elapsed_seconds)
            if not math.isfinite(e):
                raise InstrumentationError(
                    f"elapsed_seconds must be finite, got {e!r}"
                )
            if e < 0:
                raise InstrumentationError(
                    f"elapsed_seconds must be >= 0, got {e}"
                )
            object.__setattr__(self, "elapsed_seconds", e)

        if self.extras is None:
            object.__setattr__(
                self, "extras", MappingProxyType({})
            )
        elif not isinstance(self.extras, Mapping):
            raise InstrumentationError(
                f"extras must be a Mapping, got "
                f"{type(self.extras).__name__}"
            )
        else:
            object.__setattr__(
                self,
                "extras",
                MappingProxyType(dict(self.extras)),
            )

        if not isinstance(self.sanitized, bool):
            raise InstrumentationError("sanitized must be a bool")

        if self.correlation_id is not None:
            if (
                not isinstance(self.correlation_id, str)
                or not self.correlation_id
            ):
                raise InstrumentationError(
                    "correlation_id must be a non-empty string or None"
                )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "energy_joules": self.energy_joules,
            "power_watts": self.power_watts,
            "provenance": self.provenance.value,
            "source": self.source,
            "elapsed_seconds": self.elapsed_seconds,
            "extras": dict(self.extras),
            "sanitized": self.sanitized,
            "correlation_id": self.correlation_id,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "TelemetrySample":
        if not isinstance(data, Mapping):
            raise InstrumentationError(
                f"TelemetrySample.from_dict expects a Mapping, "
                f"got {type(data).__name__}"
            )

        def _numeric(key: str) -> float:
            if key not in data:
                raise InstrumentationError(f"missing field {key!r}")
            raw = data[key]
            if isinstance(raw, bool):
                raise InstrumentationError(
                    f"{key} must be numeric, not bool"
                )
            try:
                return float(raw)
            except (TypeError, ValueError) as exc:
                raise InstrumentationError(
                    f"invalid numeric field {key!r}: {exc}"
                ) from exc

        energy = _numeric("energy_joules")
        power = _numeric("power_watts")

        if "provenance" not in data:
            raise InstrumentationError("missing field 'provenance'")
        try:
            provenance = Provenance(data["provenance"])
        except (TypeError, ValueError) as exc:
            raise InstrumentationError(
                f"invalid provenance: {data['provenance']!r}"
            ) from exc

        extras_raw = data.get("extras", {})
        if not isinstance(extras_raw, Mapping):
            raise InstrumentationError(
                f"extras must be a Mapping, got "
                f"{type(extras_raw).__name__}"
            )

        return cls(
            energy_joules=energy,
            power_watts=power,
            provenance=provenance,
            source=data.get("source"),
            elapsed_seconds=data.get("elapsed_seconds"),
            extras=dict(extras_raw),
            sanitized=bool(data.get("sanitized", False)),
            correlation_id=data.get("correlation_id"),
        )

    @classmethod
    def from_metrics(
        cls,
        metrics: Mapping[str, Any],
        *,
        source: Optional[str] = None,
        elapsed_seconds: Optional[float] = None,
        sanitized: bool = False,
        correlation_id: Optional[str] = None,
    ) -> "TelemetrySample":
        """Build a sample from a raw (or validated) ``stop()`` dict."""
        if not isinstance(metrics, Mapping):
            raise InstrumentationError(
                f"metrics must be a Mapping, got "
                f"{type(metrics).__name__}"
            )

        def _numeric(key: str) -> float:
            if key not in metrics:
                raise InstrumentationError(f"missing field {key!r}")
            raw = metrics[key]
            if isinstance(raw, bool):
                raise InstrumentationError(
                    f"{key} must be numeric, not bool"
                )
            try:
                return float(raw)
            except (TypeError, ValueError) as exc:
                raise InstrumentationError(
                    f"invalid numeric field {key!r}: {exc}"
                ) from exc

        energy = _numeric("energy_joules")
        power = _numeric("power_watts")

        if "provenance" not in metrics:
            raise InstrumentationError("missing field 'provenance'")
        try:
            provenance = Provenance(metrics["provenance"])
        except (TypeError, ValueError) as exc:
            raise InstrumentationError(
                f"invalid provenance: {metrics['provenance']!r}"
            ) from exc

        core_keys = {"energy_joules", "power_watts", "provenance"}
        extras = {
            k: v for k, v in metrics.items() if k not in core_keys
        }

        return cls(
            energy_joules=energy,
            power_watts=power,
            provenance=provenance,
            source=source,
            elapsed_seconds=elapsed_seconds,
            extras=extras,
            sanitized=sanitized,
            correlation_id=correlation_id,
        )

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "TelemetrySample":
        try:
            return cls.from_dict(json.loads(payload))
        except json.JSONDecodeError as exc:
            raise InstrumentationError(
                f"Invalid JSON payload: {exc}"
            ) from exc

    def __repr__(self) -> str:
        return (
            "TelemetrySample("
            f"energy_joules={self.energy_joules:.4g}, "
            f"power_watts={self.power_watts:.4g}, "
            f"provenance={self.provenance.value!r}, "
            f"source={self.source!r}, "
            f"sanitized={self.sanitized})"
        )


# --------------------------------------------------------------------------- #
# Abstract base meter
# --------------------------------------------------------------------------- #
class BaseEnergyMeter(ABC):
    """
    Abstract base class for energy measurement backends.

    Subclasses implement :meth:`start` and :meth:`stop`. The ``stop``
    return contract is defined by :class:`BaseMeterConfig`.

    Subclasses **should return the raw metrics dict** from ``stop()``;
    :meth:`measure` performs contract validation once. Subclasses that
    want defense-in-depth may call :meth:`_validate_metrics` themselves
    before returning — the resulting sanitization is still recorded in
    ``self._last_sanitized`` and reflected on the emitted sample.

    Parameters
    ----------
    config : BaseMeterConfig, optional
        Contract policy. Defaults to ``BaseMeterConfig()``.
    strict : bool, default True
        If True, contract violations raise :class:`InstrumentationError`.
        If False, they are logged and coerced to safe defaults.
        Must be an actual ``bool``; ``1`` / ``"yes"`` are rejected.
    name : str, optional
        Human-readable meter name. Defaults to the subclass class name.
    """

    def __init__(
        self,
        *,
        config: Optional[BaseMeterConfig] = None,
        strict: bool = True,
        name: Optional[str] = None,
    ) -> None:
        if not isinstance(strict, bool):
            raise InstrumentationError("strict must be a bool")
        if config is not None and not isinstance(config, BaseMeterConfig):
            raise InstrumentationError(
                "config must be a BaseMeterConfig instance"
            )

        self._config = config if config is not None else BaseMeterConfig()
        self._strict: bool = strict
        self.name: str = name or type(self).__name__

        self._lock = threading.RLock()
        self._measure_lock = threading.Lock()
        self._started: bool = False
        self._start_monotonic: Optional[float] = None
        self._last_stop_elapsed: Optional[float] = None
        self._last_sanitized: bool = False

        # Declared here so callers can safely read it before the first
        # measure() call.
        self.last_sample: Optional[TelemetrySample] = None

        logger.debug(
            "BaseEnergyMeter '%s' initialized "
            "(strict=%s, enforce_contract=%s)",
            self.name,
            self._strict,
            self._config.enforce_contract,
        )

    # ------------------------------------------------------------------ props
    @property
    def config(self) -> BaseMeterConfig:
        return self._config

    @property
    def started(self) -> bool:
        with self._lock:
            return self._started

    @property
    def elapsed_seconds(self) -> Optional[float]:
        with self._lock:
            if self._start_monotonic is None:
                return None
            return time.monotonic() - self._start_monotonic

    # ---------------------------------------------------------- public API
    @abstractmethod
    def start(self) -> None:
        """
        Start measurement.

        Implementations must be idempotent under repeated calls and must
        acquire ``self._lock`` when mutating ``_started`` /
        ``_start_monotonic``. On failure, the meter should be left in a
        stopped state.
        """
        raise NotImplementedError

    @abstractmethod
    def stop(self) -> Dict[str, Any]:
        """
        Stop measurement and return metrics::

            {"energy_joules": float,
             "power_watts": float,
             "provenance": str}   # one of config.allowed_provenance

        The returned value may be either raw (in which case
        :meth:`measure` validates it) or pre-validated via
        :meth:`_validate_metrics`. When pre-validating, the resulting
        sanitization is automatically recorded.
        """
        raise NotImplementedError

    # ---------------------------------------------------------- helpers
    def _mark_started(self) -> None:
        """Record that measurement started (subclasses call this in ``start``)."""
        with self._lock:
            self._started = True
            self._start_monotonic = time.monotonic()
            self._last_stop_elapsed = None

    def _mark_stopped(self) -> float:
        """
        Record that measurement stopped, store the meter's own elapsed
        time, and return it. Subclasses call this in ``stop``.
        """
        with self._lock:
            elapsed = (
                time.monotonic() - self._start_monotonic
                if self._start_monotonic is not None
                else 0.0
            )
            self._started = False
            self._start_monotonic = None
            self._last_stop_elapsed = elapsed
            return elapsed

    def _validate_metrics(self, metrics: Any) -> Dict[str, Any]:
        """
        Validate a ``stop()`` result against the contract.

        Returns a JSON-safe dict with the required keys. Raises
        :class:`InstrumentationError` in strict mode on any violation;
        otherwise logs and coerces. When coercing, sets
        ``self._last_sanitized = True`` so :meth:`measure` can mark the
        emitted sample.
        """
        cfg = self._config

        if not isinstance(metrics, Mapping):
            msg = (
                f"meter '{self.name}' stop() returned "
                f"{type(metrics).__name__}; expected Mapping."
            )
            if self._strict or cfg.enforce_contract:
                raise InstrumentationError(msg)
            logger.warning("%s Returning zeros.", msg)
            self._last_sanitized = True
            return {
                "energy_joules": 0.0,
                "power_watts": 0.0,
                "provenance": Provenance.ESTIMATED.value,
            }

        missing = [k for k in cfg.required_keys if k not in metrics]
        if missing:
            msg = (
                f"meter '{self.name}' stop() missing required key(s): "
                f"{missing}."
            )
            if self._strict or cfg.enforce_contract:
                raise InstrumentationError(msg)
            logger.warning("%s Filling with defaults.", msg)
            self._last_sanitized = True

        out: Dict[str, Any] = dict(metrics)

        # Numeric validation for energy / power.
        for key in ("energy_joules", "power_watts"):
            value = out.get(key, 0.0)
            if isinstance(value, bool) or not isinstance(
                value, (int, float)
            ):
                msg = (
                    f"meter '{self.name}' field '{key}' must be "
                    f"numeric, got {type(value).__name__}."
                )
                if self._strict or cfg.enforce_contract:
                    raise InstrumentationError(msg)
                logger.warning("%s Using 0.0.", msg)
                out[key] = 0.0
                self._last_sanitized = True
                continue
            fv = float(value)
            if not math.isfinite(fv):
                msg = (
                    f"meter '{self.name}' field '{key}' must be finite."
                )
                if self._strict or cfg.enforce_contract:
                    raise InstrumentationError(msg)
                logger.warning("%s Using 0.0.", msg)
                out[key] = 0.0
                self._last_sanitized = True
                continue
            if cfg.require_non_negative and fv < 0:
                msg = (
                    f"meter '{self.name}' field '{key}' must be >= 0."
                )
                if self._strict or cfg.enforce_contract:
                    raise InstrumentationError(msg)
                logger.warning("%s Using 0.0.", msg)
                out[key] = 0.0
                self._last_sanitized = True
                continue
            out[key] = fv

        # Provenance validation.
        raw_prov = out.get(
            "provenance", Provenance.ESTIMATED.value,
        )
        try:
            prov_enum = Provenance(raw_prov)
        except (TypeError, ValueError):
            prov_enum = None

        if prov_enum is None or prov_enum.value not in cfg.allowed_provenance:
            msg = (
                f"meter '{self.name}' provenance {raw_prov!r} not in "
                f"{list(cfg.allowed_provenance)}."
            )
            if self._strict or cfg.enforce_contract:
                raise InstrumentationError(msg)
            logger.warning("%s Using 'estimated'.", msg)
            out["provenance"] = Provenance.ESTIMATED.value
            self._last_sanitized = True
        else:
            out["provenance"] = prov_enum.value

        return out

    # ---------------------------------------------------------- context manager
    @contextmanager
    def measure(
        self,
        *,
        correlation_id: Optional[str] = None,
    ) -> Iterator["BaseEnergyMeter"]:
        """
        Sync context manager that wires ``start`` / ``stop`` around a block.

        Example
        -------
        ::

            with meter.measure(correlation_id=req.request_id) as m:
                do_work()
            sample = m.last_sample
            if sample.sanitized:
                ...  # treat as unknown rather than measured

        Behavior
        --------
        * If the ``with`` body raises, ``stop()`` is still called for
          cleanup, but the **body's exception wins** — a meter failure
          during exit never masks the original cause.
        * If ``stop()`` fails while the body succeeded, the meter's
          exception propagates in strict mode; in lenient mode it is
          logged and ``last_sample`` may be ``None``.
        * If the measured window is shorter than
          ``config.min_measurement_seconds``, the emitted sample is
          marked ``sanitized=True`` (very short windows are noise).
        * The sample's ``elapsed_seconds`` is the meter's own measured
          elapsed (from :meth:`_mark_stopped`) when the subclass called
          it; otherwise it falls back to wall time.
        * Concurrent ``measure()`` calls on the same instance raise
          :class:`InstrumentationError` immediately.
        """
        if not self._measure_lock.acquire(blocking=False):
            raise InstrumentationError(
                f"meter '{self.name}' is already in a measure() context"
            )

        ctx_start = time.monotonic()
        try:
            self._last_sanitized = False
            self.last_sample = None
            self.start()

            body_exc: Optional[BaseException] = None
            try:
                yield self
            except BaseException as exc:  # noqa: BLE001
                body_exc = exc
                raise
            finally:
                try:
                    raw = self.stop()
                    validated = self._validate_metrics(raw)

                    # Prefer the meter's own measured elapsed.
                    elapsed = (
                        self._last_stop_elapsed
                        if self._last_stop_elapsed is not None
                        else time.monotonic() - ctx_start
                    )

                    if elapsed < self._config.min_measurement_seconds:
                        logger.warning(
                            "meter '%s' elapsed %.4fs below threshold "
                            "%.4fs; marking sample sanitized",
                            self.name,
                            elapsed,
                            self._config.min_measurement_seconds,
                        )
                        self._last_sanitized = True

                    self.last_sample = TelemetrySample.from_metrics(
                        validated,
                        source=self.name,
                        elapsed_seconds=elapsed,
                        sanitized=self._last_sanitized,
                        correlation_id=correlation_id,
                    )
                except Exception:  # noqa: BLE001
                    logger.exception(
                        "meter '%s' failed during context exit",
                        self.name,
                    )
                    # Only propagate if the body succeeded — otherwise
                    # the body's exception is already on its way out.
                    if body_exc is None and self._strict:
                        raise
        finally:
            self._measure_lock.release()

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}("
            f"name={self.name!r}, "
            f"started={self.started}, "
            f"strict={self._strict})"
        )


# --------------------------------------------------------------------------- #
# Smoke test: python -m instrumentation.energy.base_meter
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    class _FakeMeter(BaseEnergyMeter):
        """Reference implementation — returns raw metrics."""

        def __init__(
            self,
            *,
            energy_joules: float = 10.0,
            power_watts: float = 5.0,
            provenance: str = "measured",
            **kwargs: Any,
        ) -> None:
            super().__init__(**kwargs)
            self._energy = energy_joules
            self._power = power_watts
            self._provenance = provenance

        def start(self) -> None:
            self._mark_started()

        def stop(self) -> Dict[str, Any]:
            self._mark_stopped()
            return {
                "energy_joules": self._energy,
                "power_watts": self._power,
                "provenance": self._provenance,
            }

    class _BadMeter(BaseEnergyMeter):
        """Returns garbage — used to exercise contract enforcement."""

        def start(self) -> None:
            self._mark_started()

        def stop(self) -> Dict[str, Any]:
            self._mark_stopped()
            return {"energy_joules": -5.0}  # type: ignore[return-value]

    # ---- Happy path --------------------------------------------------- #
    meter = _FakeMeter()
    print("repr       :", meter)
    with meter.measure(correlation_id="req-42") as m:
        time.sleep(0.02)
    sample = meter.last_sample
    print("sample     :", sample)
    assert sample is not None
    assert sample.sanitized is False
    assert sample.correlation_id == "req-42"
    assert sample.provenance is Provenance.MEASURED
    assert sample.elapsed_seconds and sample.elapsed_seconds > 0
    assert meter.last_sample is not None  # still available on `m`

    # ---- Round-trip --------------------------------------------------- #
    payload = sample.to_json()
    restored = TelemetrySample.from_json(payload)
    assert restored.to_dict() == sample.to_dict()
    print("Round-trip OK.")

    # ---- Extras immutability ----------------------------------------- #
    try:
        sample.extras["x"] = 1  # type: ignore[index]
    except TypeError:
        print("Extras     : read-only (as designed)")
    else:
        raise AssertionError("extras should be read-only")

    # ---- Lenient contract violation → sanitized=True ----------------- #
    lenient = _BadMeter(
        strict=False,
        config=BaseMeterConfig(enforce_contract=False),
    )
    with lenient.measure() as m:
        pass
    assert m.last_sample is not None
    assert m.last_sample.sanitized is True
    assert m.last_sample.energy_joules == 0.0
    print("Lenient    :", m.last_sample, "(sanitized=True)")

    # ---- Strict contract violation → raises -------------------------- #
    strict = _BadMeter(strict=True)
    try:
        with strict.measure():
            pass
    except InstrumentationError as exc:
        print("Rejected   :", exc)
    else:
        raise AssertionError("strict mode should have raised")

    # ---- Body exception is preserved even when meter fails ----------- #
    strict2 = _BadMeter(strict=True)

    class _BodyError(RuntimeError):
        pass

    try:
        with strict2.measure():
            raise _BodyError("the real cause")
    except _BodyError as exc:
        print("Body wins  :", type(exc).__name__, "-", exc)
    except InstrumentationError:
        raise AssertionError(
            "meter failure masked the body's exception"
        )

    # ---- min_measurement_seconds → sanitized=True -------------------- #
    quick = _FakeMeter(
        config=BaseMeterConfig(min_measurement_seconds=10.0),
    )
    with quick.measure():
        pass  # far below 10s
    assert quick.last_sample is not None
    assert quick.last_sample.sanitized is True
    print("Too short  :", quick.last_sample, "(sanitized=True)")

    # ---- Concurrent measure() rejected ------------------------------- #
    shared = _FakeMeter()
    shared._measure_lock.acquire(blocking=False)
    try:
        try:
            with shared.measure():
                pass
        except InstrumentationError as exc:
            print("Concurrent :", exc)
        else:
            raise AssertionError(
                "concurrent measure() should have raised"
            )
    finally:
        shared._measure_lock.release()

    # ---- Provenance enum coercion ------------------------------------ #
    assert Provenance("measured") is Provenance.MEASURED
    try:
        Provenance("bogus")
    except ValueError:
        print("Enum       : bogus provenance rejected")

    # ---- Config validation ------------------------------------------- #
    for bad in (
        dict(required_keys=()),
        dict(allowed_provenance=()),
        dict(min_measurement_seconds=-1.0),
        dict(enforce_contract="yes"),  # type: ignore[dict-item]
        dict(require_non_negative=1),  # type: ignore[dict-item]
    ):
        try:
            BaseMeterConfig(**bad)  # type: ignore[arg-type]
        except InstrumentationError as exc:
            print("Rejected cfg:", exc)
        else:
            raise AssertionError(f"config should have rejected {bad}")

    # ---- from_dict rejects bool and bad types ------------------------ #
    try:
        TelemetrySample.from_dict({
            "energy_joules": True,
            "power_watts": 5.0,
            "provenance": "measured",
        })
    except InstrumentationError as exc:
        print("Bad dict    :", exc)
    else:
        raise AssertionError("from_dict should reject bool energy")

    print("\nSmoke test passed.")
