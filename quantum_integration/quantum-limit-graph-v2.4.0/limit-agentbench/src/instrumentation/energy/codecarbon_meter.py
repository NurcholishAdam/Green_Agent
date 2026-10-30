# src/instrumentation/energy/codecarbon_meter.py

"""
CodeCarbon Energy Meter
=======================

Measures carbon emissions and energy consumption using CodeCarbon.

Enhancements
------------
- Optional ``codecarbon`` import guarded by ``_CODECARBON_AVAILABLE``.
- :class:`TrackingMode` — closed enum replacing the free-form string.
- :class:`CodeCarbonConfig` — frozen, fully validated (bools, types,
  numeric bounds, enum coercion for ``tracking_mode`` and
  ``default_provenance``); project/country fields are string-checked.
- :class:`Provenance` (re-used from ``base_meter``) for
  ``default_provenance``.
- :meth:`CodeCarbonMeter.stop`:
    * ``_mark_stopped()`` runs in a ``finally`` block so state is
      always consistent, whether or not ``tracker.stop()`` succeeds.
    * Every lenient fallback (tracker failure, unreadable energy,
      no-op tracker) sets ``self._last_sanitized = True`` so
      :class:`TelemetrySample` (when built via :meth:`measure`) reports
      ``sanitized=True`` — the sample is *explicitly* unknown rather
      than silently zero.
    * ``min_energy_joules`` is enforced: readings below the floor set
      ``_last_sanitized = True`` (or raise in strict mode).
- :meth:`CodeCarbonMeter._read_energy_joules` rejects ``bool`` from
  the tracker and flags the ``0.0`` fallback as sanitized.
- ``EmissionsTracker`` retry path preserves the original
  ``TypeError`` in the chained error message.
- ``__main__`` smoke test with a real missing-codecarbon test, an
  energy-floor test, and a sanitized-on-failure regression case.

Note on lenient mode
--------------------
To suppress contract violations the meter must be constructed with
**both** ``strict=False`` **and**
``base_config=BaseMeterConfig(enforce_contract=False)``. The
``enforce_contract`` flag is the base meter's absolute gate; the
``strict`` flag alone is not sufficient.

Note on ``carbon_kg``
---------------------
:class:`TelemetrySample` does not carry a first-class carbon field.
The carbon weight reported by CodeCarbon is preserved in
``sample.extras["carbon_kg"]``; the sustainability policy's
``expected_carbon_g`` consumer is expected to read it from there.
"""

from __future__ import annotations

import json
import logging
import math
import numbers
import threading
import time
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Dict, Mapping, Optional

from .base_meter import (
    BaseEnergyMeter,
    BaseMeterConfig,
    InstrumentationError,
    Provenance,
    TelemetrySample,
)

logger = logging.getLogger(__name__)

__all__ = [
    "CodeCarbonConfig",
    "CodeCarbonMeter",
    "CodeCarbonMeterError",
    "TrackingMode",
]


# --------------------------------------------------------------------------- #
# Optional dependency
# --------------------------------------------------------------------------- #
try:  # pragma: no cover — environment-dependent
    from codecarbon import EmissionsTracker  # type: ignore

    _CODECARBON_AVAILABLE = True
except ImportError:  # pragma: no cover
    EmissionsTracker = None  # type: ignore[assignment]
    _CODECARBON_AVAILABLE = False
    logger.debug("codecarbon not importable; CodeCarbonMeter disabled.")


# --------------------------------------------------------------------------- #
# Errors and enums
# --------------------------------------------------------------------------- #
class CodeCarbonMeterError(InstrumentationError):
    """Raised for invalid CodeCarbon meter inputs or runtime failures."""


class TrackingMode(str, Enum):
    """CodeCarbon's tracking mode."""

    MACHINE = "machine"
    PROCESS = "process"


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class CodeCarbonConfig:
    """Tunable parameters for the CodeCarbon meter."""

    measure_power_secs: float = 1.0
    tracking_mode: TrackingMode = TrackingMode.MACHINE
    output_dir: Optional[str] = None
    project_name: Optional[str] = None
    country_iso_code: Optional[str] = None
    min_energy_joules: float = 0.0
    default_provenance: Provenance = Provenance.ESTIMATED

    def __post_init__(self) -> None:
        # measure_power_secs
        if isinstance(self.measure_power_secs, bool) or not isinstance(
            self.measure_power_secs, (int, float)
        ):
            raise CodeCarbonMeterError(
                "measure_power_secs must be a number, not bool"
            )
        fv = float(self.measure_power_secs)
        if not math.isfinite(fv) or fv <= 0:
            raise CodeCarbonMeterError(
                "measure_power_secs must be finite and > 0"
            )
        object.__setattr__(self, "measure_power_secs", fv)

        # tracking_mode
        if not isinstance(self.tracking_mode, TrackingMode):
            try:
                object.__setattr__(
                    self,
                    "tracking_mode",
                    TrackingMode(self.tracking_mode),
                )
            except (ValueError, TypeError) as exc:
                raise CodeCarbonMeterError(
                    f"tracking_mode must be one of "
                    f"{[m.value for m in TrackingMode]}, got "
                    f"{self.tracking_mode!r}"
                ) from exc

        # Optional string fields
        for label in ("output_dir", "project_name", "country_iso_code"):
            value = getattr(self, label)
            if value is None:
                continue
            if not isinstance(value, str) or not value.strip():
                raise CodeCarbonMeterError(
                    f"{label} must be a non-empty string or None"
                )

        # min_energy_joules
        if isinstance(self.min_energy_joules, bool) or not isinstance(
            self.min_energy_joules, (int, float)
        ):
            raise CodeCarbonMeterError(
                "min_energy_joules must be a number, not bool"
            )
        mev = float(self.min_energy_joules)
        if not math.isfinite(mev) or mev < 0:
            raise CodeCarbonMeterError(
                "min_energy_joules must be finite and >= 0"
            )
        object.__setattr__(self, "min_energy_joules", mev)

        # default_provenance
        if not isinstance(self.default_provenance, Provenance):
            try:
                object.__setattr__(
                    self,
                    "default_provenance",
                    Provenance(self.default_provenance),
                )
            except (ValueError, TypeError) as exc:
                raise CodeCarbonMeterError(
                    f"default_provenance must be one of "
                    f"{[p.value for p in Provenance]}, got "
                    f"{self.default_provenance!r}"
                ) from exc

    def to_dict(self) -> Dict[str, Any]:
        return {
            "measure_power_secs": self.measure_power_secs,
            "tracking_mode": self.tracking_mode.value,
            "output_dir": self.output_dir,
            "project_name": self.project_name,
            "country_iso_code": self.country_iso_code,
            "min_energy_joules": self.min_energy_joules,
            "default_provenance": self.default_provenance.value,
        }


# --------------------------------------------------------------------------- #
# Meter
# --------------------------------------------------------------------------- #
class CodeCarbonMeter(BaseEnergyMeter):
    """
    Measures carbon and energy using CodeCarbon.

    See the module docstring for the lenient-mode contract and for
    where ``carbon_kg`` is exposed on the emitted sample.
    """

    def __init__(
        self,
        *,
        config: Optional[CodeCarbonConfig] = None,
        base_config: Optional[BaseMeterConfig] = None,
        strict: bool = True,
        tracker_factory: Optional[Any] = None,
    ) -> None:
        super().__init__(
            config=base_config,
            strict=strict,
            name="codecarbon",
        )
        self._cc_config: CodeCarbonConfig = config or CodeCarbonConfig()
        self._tracker_factory = tracker_factory
        self._inner_lock = threading.RLock()
        self._tracker: Any = None
        self._start_time: Optional[float] = None
        self._last_emissions_kg: Optional[float] = None

        if tracker_factory is None and not _CODECARBON_AVAILABLE:
            msg = (
                "codecarbon is not installed and no tracker_factory "
                "was provided."
            )
            if self._strict:
                raise CodeCarbonMeterError(msg)
            logger.warning(
                "%s CodeCarbonMeter will use a no-op tracker; "
                "emitted samples will be sanitized.", msg,
            )
            self._tracker = _NoopTracker()
        else:
            self._tracker = self._build_tracker()

        logger.debug(
            "CodeCarbonMeter initialized (tracker=%s, "
            "measure_power_secs=%.2fs, tracking_mode=%s, strict=%s)",
            type(self._tracker).__name__,
            self._cc_config.measure_power_secs,
            self._cc_config.tracking_mode.value,
            self._strict,
        )

    # ---------------------------------------------------------- tracker setup
    def _build_tracker(self) -> Any:
        if self._tracker_factory is not None:
            try:
                tracker = self._tracker_factory()
            except Exception as exc:
                raise CodeCarbonMeterError(
                    f"tracker_factory failed: {exc}"
                ) from exc
            if tracker is None:
                raise CodeCarbonMeterError(
                    "tracker_factory returned None"
                )
            return tracker

        kwargs: Dict[str, Any] = {
            "measure_power_secs": self._cc_config.measure_power_secs,
            "tracking_mode": self._cc_config.tracking_mode.value,
            "save_to_file": False,
        }
        if self._cc_config.output_dir is not None:
            kwargs["output_dir"] = self._cc_config.output_dir
        if self._cc_config.project_name is not None:
            kwargs["project_name"] = self._cc_config.project_name
        if self._cc_config.country_iso_code is not None:
            kwargs["country_iso_code"] = (
                self._cc_config.country_iso_code
            )

        try:
            return EmissionsTracker(**kwargs)  # type: ignore[misc]
        except TypeError as first_exc:
            # Older CodeCarbon accepts a narrower kwargs set.
            logger.debug(
                "EmissionsTracker rejected extended kwargs (%s); "
                "retrying with minimal set.", first_exc,
            )
            try:
                return EmissionsTracker(  # type: ignore[misc]
                    measure_power_secs=(
                        self._cc_config.measure_power_secs
                    ),
                )
            except Exception as second_exc:
                raise CodeCarbonMeterError(
                    f"Could not construct EmissionsTracker "
                    f"(extended kwargs failed with "
                    f"{first_exc!r}; minimal set failed with "
                    f"{second_exc!r})"
                ) from second_exc
        except Exception as exc:
            raise CodeCarbonMeterError(
                f"Could not construct EmissionsTracker: {exc}"
            ) from exc

    # ---------------------------------------------------------- lifecycle
    def start(self) -> None:
        with self._inner_lock:
            if self.started:
                logger.debug(
                    "CodeCarbonMeter.start() called while already "
                    "running."
                )
                return
            try:
                self._tracker.start()
            except Exception as exc:
                logger.exception("tracker.start() failed: %s", exc)
                if self._strict:
                    raise CodeCarbonMeterError(
                        f"tracker.start() failed: {exc}"
                    ) from exc
                return
            self._start_time = time.monotonic()
            self._mark_started()
        logger.debug("CodeCarbonMeter started.")

    def stop(self) -> Dict[str, Any]:
        """
        Stop CodeCarbon measurement and return metrics::

            {"energy_joules": float, "power_watts": float,
             "carbon_kg": float, "provenance": str}

        In lenient mode, the returned dict always satisfies the base
        contract; ``self._last_sanitized`` is ``True`` when any field
        was substituted from a failure (tracker error, unreadable
        energy, no-op tracker, or an energy reading below
        ``min_energy_joules``).
        """
        with self._inner_lock:
            tracker_error: Optional[Exception] = None
            emissions: Optional[float] = None

            try:
                emissions = self._tracker.stop()
            except Exception as exc:
                tracker_error = exc
                logger.exception("tracker.stop() failed: %s", exc)

            # Always clean up meter state, even if tracker.stop() failed.
            elapsed = self._mark_stopped()

            if tracker_error is not None:
                self._last_sanitized = True
                if self._strict:
                    raise CodeCarbonMeterError(
                        f"tracker.stop() failed: {tracker_error}"
                    ) from tracker_error

            # No-op tracker → sample is fabricated.
            if getattr(self._tracker, "simulated", False):
                self._last_sanitized = True

            energy_joules = self._read_energy_joules()
            power_watts = self._derive_power_watts(
                energy_joules=energy_joules,
                elapsed_seconds=elapsed,
            )

            carbon_kg = self._coerce_carbon(emissions)
            if carbon_kg is None:
                self._last_sanitized = True
                carbon_kg = 0.0

            self._last_emissions_kg = carbon_kg

            # Enforce the energy floor.
            floor = self._cc_config.min_energy_joules
            if floor > 0 and energy_joules < floor:
                msg = (
                    f"meter 'codecarbon' energy {energy_joules:.6g} J "
                    f"below floor {floor:.6g} J"
                )
                if self._strict:
                    raise CodeCarbonMeterError(msg)
                logger.warning("%s Marking sample sanitized.", msg)
                self._last_sanitized = True

            raw = {
                "energy_joules": energy_joules,
                "power_watts": power_watts,
                "carbon_kg": carbon_kg,
                "provenance": (
                    self._cc_config.default_provenance.value
                ),
                "source": "codecarbon",
                "elapsed_seconds": elapsed,
            }

        logger.debug(
            "CodeCarbonMeter stopped: energy=%.6g J, power=%.4g W, "
            "carbon=%.6g kg, elapsed=%.3fs, sanitized=%s",
            raw["energy_joules"],
            raw["power_watts"],
            raw["carbon_kg"],
            elapsed,
            self._last_sanitized,
        )

        return self._validate_metrics(raw)

    # ---------------------------------------------------------- extraction
    def _read_energy_joules(self) -> float:
        """
        Read cumulative energy from the tracker using the most public
        API available; fall back through several CodeCarbon
        representations. Any fallback that substitutes ``0.0`` for a
        tracker failure sets ``self._last_sanitized = True``.
        """
        tracker = self._tracker

        # 1. Public API: ``final_emissions_data.energy_consumed`` (kWh).
        final_data = getattr(tracker, "final_emissions_data", None)
        if final_data is not None:
            kwh = getattr(final_data, "energy_consumed", None)
            if self._is_valid_kwh(kwh):
                return float(kwh) * 3.6e6

        # 2. Legacy: ``_total_energy.kWh``.
        total_energy = getattr(tracker, "_total_energy", None)
        if total_energy is not None:
            kwh = getattr(total_energy, "kWh", None)
            if self._is_valid_kwh(kwh):
                return float(kwh) * 3.6e6

        # 3. Even older: ``_total_energy`` as a raw float (kWh).
        if self._is_valid_kwh(total_energy):
            return float(total_energy) * 3.6e6

        # 4. Nothing usable → flag and return zero.
        msg = (
            "could not read energy from the CodeCarbon tracker; "
            "using 0.0 J"
        )
        if self._strict:
            raise CodeCarbonMeterError(msg)
        logger.warning("%s (sample marked sanitized)", msg)
        self._last_sanitized = True
        return 0.0

    @staticmethod
    def _is_valid_kwh(value: Any) -> bool:
        if isinstance(value, bool) or not isinstance(
            value, numbers.Real
        ):
            return False
        try:
            fv = float(value)
        except (TypeError, ValueError, OverflowError):
            return False
        return math.isfinite(fv) and fv >= 0

    @staticmethod
    def _coerce_carbon(value: Any) -> Optional[float]:
        """Return a finite non-negative float, or ``None``."""
        if isinstance(value, bool) or not isinstance(
            value, numbers.Real
        ):
            return None
        try:
            fv = float(value)
        except (TypeError, ValueError, OverflowError):
            return None
        if not math.isfinite(fv) or fv < 0:
            return None
        return fv

    @staticmethod
    def _derive_power_watts(
        *,
        energy_joules: float,
        elapsed_seconds: float,
    ) -> float:
        if (
            elapsed_seconds <= 0
            or energy_joules <= 0
            or not math.isfinite(energy_joules)
            or not math.isfinite(elapsed_seconds)
        ):
            return 0.0
        return energy_joules / elapsed_seconds

    # ---------------------------------------------------------- props
    @property
    def cc_config(self) -> CodeCarbonConfig:
        return self._cc_config

    @property
    def last_emissions_kg(self) -> Optional[float]:
        return self._last_emissions_kg

    @property
    def last_sanitized(self) -> bool:
        """True when the most recent ``stop()`` substituted any field."""
        return self._last_sanitized

    @property
    def tracker(self) -> Any:
        return self._tracker

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "config": self._cc_config.to_dict(),
            "base_config": asdict(self._config),
            "strict": self._strict,
            "started": self.started,
            "codecarbon_available": _CODECARBON_AVAILABLE,
            "tracker_type": type(self._tracker).__name__,
            "last_sanitized": self._last_sanitized,
        }

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    def __repr__(self) -> str:
        return (
            "CodeCarbonMeter("
            f"tracker={type(self._tracker).__name__}, "
            f"measure_power_secs="
            f"{self._cc_config.measure_power_secs:.2f}, "
            f"tracking_mode={self._cc_config.tracking_mode.value!r}, "
            f"started={self.started}, "
            f"strict={self._strict}, "
            f"sanitized={self._last_sanitized})"
        )


# --------------------------------------------------------------------------- #
# No-op tracker
# --------------------------------------------------------------------------- #
class _NoopTracker:
    """Silent stand-in used when ``codecarbon`` is not installed."""

    simulated = True

    class _Energy:
        kWh: float = 0.0

    def __init__(self) -> None:
        self._total_energy = self._Energy()

    def start(self) -> None:  # pragma: no cover
        return None

    def stop(self) -> float:  # pragma: no cover
        return 0.0


# --------------------------------------------------------------------------- #
# Smoke test
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)
    import sys

    class _MockEnergy:
        def __init__(self, kwh: float) -> None:
            self.kWh = kwh

    class _MockTracker:
        def __init__(
            self, energy_kwh: float, emissions_kg: float,
        ) -> None:
            self._total_energy = _MockEnergy(energy_kwh)
            self.final_emissions_data = type(
                "_Final", (), {"energy_consumed": energy_kwh}
            )()
            self._emissions_kg = emissions_kg
            self._running = False

        def start(self) -> None:
            self._running = True

        def stop(self) -> float:
            self._running = False
            return self._emissions_kg

    def _factory() -> _MockTracker:
        return _MockTracker(
            energy_kwh=0.0025, emissions_kg=0.0011,
        )

    # Happy path
    meter = CodeCarbonMeter(tracker_factory=_factory)
    print("repr       :", meter)
    with meter.measure(correlation_id="req-1") as m:
        time.sleep(0.05)
    sample = m.last_sample
    print("sample     :", sample)
    assert sample is not None
    assert sample.sanitized is False
    assert sample.correlation_id == "req-1"
    assert sample.provenance is Provenance.ESTIMATED
    print("carbon_kg  :", sample.extras.get("carbon_kg"))

    # Round-trip
    payload = sample.to_json()
    assert TelemetrySample.from_json(payload).to_dict() == sample.to_dict()
    print("Round-trip OK.")

    # Direct start/stop still returns validated data
    meter2 = CodeCarbonMeter(tracker_factory=_factory)
    meter2.start()
    time.sleep(0.02)
    metrics = meter2.stop()
    assert "power_watts" in metrics
    assert "energy_joules" in metrics
    assert "provenance" in metrics
    print("legacy stop:", metrics)

    # Missing codecarbon — real test by patching the flag
    mod = sys.modules[__name__]
    saved = mod._CODECARBON_AVAILABLE
    try:
        mod._CODECARBON_AVAILABLE = False
        try:
            CodeCarbonMeter(strict=True)
        except CodeCarbonMeterError as exc:
            print("Strict miss:", exc)
        lenient = CodeCarbonMeter(strict=False)
        with lenient.measure():
            pass
        assert lenient.last_sample is not None
        assert lenient.last_sample.sanitized is True
        print("Lenient miss:", lenient.last_sample, "(sanitized=True)")
    finally:
        mod._CODECARBON_AVAILABLE = saved

    # Energy floor enforcement
    floor_meter = CodeCarbonMeter(
        config=CodeCarbonConfig(min_energy_joules=1.0),
        tracker_factory=_factory,
        strict=False,
        base_config=BaseMeterConfig(enforce_contract=False),
    )
    with floor_meter.measure():
        pass
    assert floor_meter.last_sample is not None
    assert floor_meter.last_sample.sanitized is True
    print("Floor       : sanitized=True")

    # Tracker failure → sanitized
    class _BrokenTracker:
        def start(self) -> None:
            pass

        def stop(self) -> float:
            raise RuntimeError("tracker crashed")

    lenient2 = CodeCarbonMeter(
        tracker_factory=lambda: _BrokenTracker(),
        strict=False,
        base_config=BaseMeterConfig(enforce_contract=False),
    )
    with lenient2.measure():
        pass
    assert lenient2.last_sample is not None
    assert lenient2.last_sample.sanitized is True
    print("Failed stop : sanitized=True")

    # Config validation
    for bad in (
        dict(measure_power_secs=0),
        dict(measure_power_secs=True),
        dict(tracking_mode="bogus"),
        dict(min_energy_joules=-1.0),
        dict(default_provenance="fabricated"),
        dict(project_name=42),
    ):
        try:
            CodeCarbonConfig(**bad)  # type: ignore[arg-type]
        except CodeCarbonMeterError as exc:
            print("Rejected cfg:", exc)
        else:
            raise AssertionError(f"config should have rejected {bad}")

    print("\nSmoke test passed.")
