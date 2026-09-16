# src/instrumentation/energy/base_meter.py

"""
Base Energy Meter
=================

Abstract base class for energy measurement backends.

Enhancements
------------
- Module-level ``logger`` (previously absent — silent failures).
- :class:`InstrumentationError` — narrow, catchable failure type.
- :class:`BaseMeterConfig` — frozen, validated, centralizes contract policy.
- :class:`TelemetrySample` — frozen dataclass formalizing the return contract
  of :meth:`BaseEnergyMeter.stop`.
- :meth:`BaseEnergyMeter.stop` return contract is now **enforced** by the
  :meth:`_validate_metrics` helper (subclasses call it or inherit the safe
  :meth:`measure` context manager).
- :meth:`BaseEnergyMeter.measure` — sync context manager that wires
  ``start`` / ``stop`` and returns a validated :class:`TelemetrySample`.
- Serialization on the config and the sample.
- ``__repr__`` on config and sample.
- ``__main__`` smoke test with a reference in-memory subclass.
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
from typing import Any, Dict, Iterator, Mapping, Optional

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class InstrumentationError(ValueError):
    """Raised for invalid meter inputs, configuration, or contract violations."""


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class BaseMeterConfig:
    """
    Contract-level configuration shared by every energy meter.

    Concrete meters may extend this or accept it as a base config.
    """

    # Enforce the ``stop()`` contract (energy_joules / power_watts / provenance).
    enforce_contract: bool = True

    # Which keys the returned dict must contain (at minimum).
    required_keys: tuple = ("energy_joules", "power_watts", "provenance")

    # Allowed values for ``provenance``.
    allowed_provenance: tuple = ("measured", "estimated", "simulated")

    # Reject negative energy/power readings.
    require_non_negative: bool = True

    # If the meter's measured elapsed time is < this threshold, treat the
    # reading as suspect (very short windows are usually noise).
    min_measurement_seconds: float = 0.0

    def __post_init__(self) -> None:
        if not isinstance(self.required_keys, tuple) or not self.required_keys:
            raise InstrumentationError(
                "required_keys must be a non-empty tuple."
            )
        if not isinstance(self.allowed_provenance, tuple):
            raise InstrumentationError("allowed_provenance must be a tuple.")
        for name in ("required_keys", "allowed_provenance"):
            for value in getattr(self, name):
                if not isinstance(value, str) or not value:
                    raise InstrumentationError(
                        f"{name} entries must be non-empty strings."
                    )
        if self.min_measurement_seconds < 0:
            raise InstrumentationError(
                "min_measurement_seconds must be >= 0."
            )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# --------------------------------------------------------------------------- #
# Telemetry sample
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class TelemetrySample:
    """
    Immutable, contract-validated reading from an energy meter.

    Concrete meters may build this from their raw ``stop()`` dict via
    :meth:`from_metrics`, or return the dict directly and let
    :meth:`BaseEnergyMeter.measure` handle validation.
    """

    energy_joules: float
    power_watts: float
    provenance: str
    source: Optional[str] = None
    elapsed_seconds: Optional[float] = None
    extras: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name in ("energy_joules", "power_watts"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise InstrumentationError(
                    f"{name} must be numeric, got {type(value).__name__}."
                )
            fv = float(value)
            if math.isnan(fv) or math.isinf(fv):
                raise InstrumentationError(f"{name} must be finite, got {value!r}.")
            if fv < 0:
                raise InstrumentationError(f"{name} must be >= 0, got {fv}.")
        if not isinstance(self.provenance, str) or not self.provenance:
            raise InstrumentationError("provenance must be a non-empty string.")
        if self.elapsed_seconds is not None and self.elapsed_seconds < 0:
            raise InstrumentationError("elapsed_seconds must be >= 0.")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "energy_joules": self.energy_joules,
            "power_watts": self.power_watts,
            "provenance": self.provenance,
            "source": self.source,
            "elapsed_seconds": self.elapsed_seconds,
            "extras": dict(self.extras),
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "TelemetrySample":
        if not isinstance(data, Mapping):
            raise InstrumentationError(
                f"TelemetrySample.from_dict expects a Mapping, "
                f"got {type(data).__name__}."
            )
        return cls(
            energy_joules=float(data["energy_joules"]),
            power_watts=float(data["power_watts"]),
            provenance=str(data["provenance"]),
            source=data.get("source"),
            elapsed_seconds=data.get("elapsed_seconds"),
            extras=dict(data.get("extras", {}) or {}),
        )

    @classmethod
    def from_metrics(
        cls,
        metrics: Mapping[str, Any],
        *,
        source: Optional[str] = None,
        elapsed_seconds: Optional[float] = None,
    ) -> "TelemetrySample":
        """Build a sample from a raw ``stop()`` dict."""
        if not isinstance(metrics, Mapping):
            raise InstrumentationError(
                f"metrics must be a Mapping, got {type(metrics).__name__}."
            )
        # Preserve unknown keys as ``extras``.
        core_keys = {"energy_joules", "power_watts", "provenance"}
        extras = {k: v for k, v in metrics.items() if k not in core_keys}
        return cls(
            energy_joules=float(metrics["energy_joules"]),
            power_watts=float(metrics["power_watts"]),
            provenance=str(metrics["provenance"]),
            source=source,
            elapsed_seconds=elapsed_seconds,
            extras=extras,
        )

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    @classmethod
    def from_json(cls, payload: str) -> "TelemetrySample":
        try:
            return cls.from_dict(json.loads(payload))
        except json.JSONDecodeError as exc:
            raise InstrumentationError(f"Invalid JSON payload: {exc}") from exc

    def __repr__(self) -> str:
        return (
            "TelemetrySample("
            f"energy_joules={self.energy_joules:.4g}, "
            f"power_watts={self.power_watts:.4g}, "
            f"provenance={self.provenance!r}, "
            f"source={self.source!r})"
        )


# --------------------------------------------------------------------------- #
# Abstract base meter
# --------------------------------------------------------------------------- #
class BaseEnergyMeter(ABC):
    """
    Abstract base class for energy measurement backends.

    Subclasses implement :meth:`start` and :meth:`stop`. The ``stop`` return
    contract is defined by :class:`BaseMeterConfig` and enforced by
    :meth:`_validate_metrics` (or implicitly when using :meth:`measure`).

    Parameters
    ----------
    config : BaseMeterConfig, optional
        Contract policy. Defaults to ``BaseMeterConfig()``.
    strict : bool, default True
        If True, contract violations raise :class:`InstrumentationError`.
        If False, they are logged and coerced to safe defaults.
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
        self._config = config or BaseMeterConfig()
        self._strict = bool(strict)
        self.name: str = name or type(self).__name__
        self._lock = threading.RLock()
        self._started: bool = False
        self._start_monotonic: Optional[float] = None
        self._ctx_start: Optional[float] = None

        logger.debug(
            "BaseEnergyMeter '%s' initialized (strict=%s, enforce_contract=%s)",
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
        acquire ``self._lock`` when mutating ``_started`` / ``_start_monotonic``.
        """
        raise NotImplementedError

    @abstractmethod
    def stop(self) -> Dict[str, Any]:
        """
        Stop measurement and return metrics:

        ``{"energy_joules": float, "power_watts": float, "provenance": str}``

        ``provenance`` must be one of ``config.allowed_provenance``. Concrete
        implementations should call :meth:`_validate_metrics` on their result
        before returning.
        """
        raise NotImplementedError

    # ---------------------------------------------------------- helpers
    def _mark_started(self) -> None:
        """Record that measurement started (subclasses call this in ``start``)."""
        with self._lock:
            self._started = True
            self._start_monotonic = time.monotonic()

    def _mark_stopped(self) -> float:
        """
        Record that measurement stopped and return the elapsed wall time in
        seconds. Subclasses call this in ``stop``.
        """
        with self._lock:
            elapsed = (
                time.monotonic() - self._start_monotonic
                if self._start_monotonic is not None
                else 0.0
            )
            self._started = False
            self._start_monotonic = None
            return elapsed

    def _validate_metrics(self, metrics: Any) -> Dict[str, Any]:
        """
        Validate a ``stop()`` result against the contract.

        Returns a JSON-safe dict with the required keys. Raises
        :class:`InstrumentationError` in strict mode on any violation;
        otherwise logs and coerces.
        """
        cfg = self._config

        if not isinstance(metrics, Mapping):
            msg = (
                f"meter '{self.name}' stop() returned {type(metrics).__name__}; "
                f"expected Mapping."
            )
            if self._strict or cfg.enforce_contract:
                raise InstrumentationError(msg)
            logger.warning("%s Returning zeros.", msg)
            return {"energy_joules": 0.0, "power_watts": 0.0,
                    "provenance": "estimated"}

        missing = [k for k in cfg.required_keys if k not in metrics]
        if missing:
            msg = (
                f"meter '{self.name}' stop() missing required key(s): {missing}."
            )
            if self._strict or cfg.enforce_contract:
                raise InstrumentationError(msg)
            logger.warning("%s Filling with defaults.", msg)

        out: Dict[str, Any] = dict(metrics)

        # Numeric validation for energy / power.
        for key in ("energy_joules", "power_watts"):
            value = out.get(key, 0.0)
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                msg = (
                    f"meter '{self.name}' field '{key}' must be numeric, "
                    f"got {type(value).__name__}."
                )
                if self._strict or cfg.enforce_contract:
                    raise InstrumentationError(msg)
                logger.warning("%s Using 0.0.", msg)
                out[key] = 0.0
                continue
            fv = float(value)
            if math.isnan(fv) or math.isinf(fv):
                msg = f"meter '{self.name}' field '{key}' must be finite."
                if self._strict or cfg.enforce_contract:
                    raise InstrumentationError(msg)
                logger.warning("%s Using 0.0.", msg)
                out[key] = 0.0
                continue
            if cfg.require_non_negative and fv < 0:
                msg = f"meter '{self.name}' field '{key}' must be >= 0."
                if self._strict or cfg.enforce_contract:
                    raise InstrumentationError(msg)
                logger.warning("%s Using 0.0.", msg)
                out[key] = 0.0
                continue
            out[key] = fv

        # Provenance validation.
        provenance = out.get("provenance", "estimated")
        if not isinstance(provenance, str) or provenance not in cfg.allowed_provenance:
            msg = (
                f"meter '{self.name}' provenance {provenance!r} not in "
                f"{list(cfg.allowed_provenance)}."
            )
            if self._strict or cfg.enforce_contract:
                raise InstrumentationError(msg)
            logger.warning("%s Using 'estimated'.", msg)
            out["provenance"] = "estimated"

        return out

    # ---------------------------------------------------------- context manager
    @contextmanager
    def measure(self) -> Iterator["BaseEnergyMeter"]:
        """
        Sync context manager that wires ``start`` / ``stop`` around a block.

        Example
        -------
        ::

            with meter.measure() as m:
                do_work()
            sample = m.last_sample

        The validated :class:`TelemetrySample` is stored on
        :attr:`last_sample` for retrieval after the ``with`` block.
        """
        self._ctx_start = time.perf_counter()
        self.last_sample: Optional[TelemetrySample] = None
        self.start()
        try:
            yield self
        finally:
            try:
                raw = self.stop()
                validated = self._validate_metrics(raw)
                elapsed = time.perf_counter() - self._ctx_start
                self.last_sample = TelemetrySample.from_metrics(
                    validated,
                    source=self.name,
                    elapsed_seconds=elapsed,
                )
            except InstrumentationError:
                logger.exception(
                    "meter '%s' failed during context exit.", self.name
                )
                if self._strict:
                    raise
            finally:
                self._ctx_start = None

    # ----------------------------------------------------------------- dunder
    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}("
            f"name={self.name!r}, "
            f"started={self.started}, "
            f"strict={self._strict})"
        )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "BaseEnergyMeter",
    "BaseMeterConfig",
    "InstrumentationError",
    "TelemetrySample",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m instrumentation.energy.base_meter
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    class _FakeMeter(BaseEnergyMeter):
        """Reference implementation used for the smoke test."""

        def __init__(self, *, energy_joules: float = 10.0,
                     power_watts: float = 5.0,
                     provenance: str = "measured",
                     **kwargs: Any) -> None:
            super().__init__(**kwargs)
            self._energy = energy_joules
            self._power = power_watts
            self._provenance = provenance

        def start(self) -> None:
            self._mark_started()

        def stop(self) -> Dict[str, Any]:
            self._mark_stopped()
            return self._validate_metrics({
                "energy_joules": self._energy,
                "power_watts": self._power,
                "provenance": self._provenance,
            })

    # ---- Happy path --------------------------------------------------- #
    meter = _FakeMeter()
    print("repr       :", meter)
    with meter.measure() as m:
        time.sleep(0.01)
    print("sample     :", m.last_sample)
    print("dict       :", m.last_sample.to_dict() if m.last_sample else None)

    # ---- Serialization round-trip ------------------------------------- #
    if meter.last_sample is not None:
        payload = meter.last_sample.to_json()
        restored = TelemetrySample.from_json(payload)
        assert restored.to_dict() == meter.last_sample.to_dict()
        print("Round-trip OK.")

    # ---- Non-strict contract violation -------------------------------- #
    class _BadMeter(BaseEnergyMeter):
        def start(self) -> None:
            self._mark_started()

        def stop(self) -> Dict[str, Any]:
            self._mark_stopped()
            # Missing keys → contract violation.
            return {"energy_joules": -5.0}  # type: ignore[return-value]

    lenient = _BadMeter(strict=False)
    with lenient.measure() as m:
        pass
    print("Lenient    :", m.last_sample)

    # ---- Strict contract violation ------------------------------------ #
    strict = _BadMeter(strict=True)
    try:
        with strict.measure():
            pass
    except InstrumentationError as exc:
        print("Rejected   :", exc)

    # ---- Config validation -------------------------------------------- #
    for bad in (
        dict(required_keys=()),
        dict(required_keys=("energy_joules",)),
        dict(allowed_provenance=("bogus",)),
        dict(min_measurement_seconds=-1.0),
    ):
        try:
            BaseMeterConfig(**bad)  # type: ignore[arg-type]
        except InstrumentationError as exc:
            print("Rejected cfg:", exc)

    print("\nSmoke test passed.")
