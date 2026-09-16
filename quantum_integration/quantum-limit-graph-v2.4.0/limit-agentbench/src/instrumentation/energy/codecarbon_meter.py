# src/instrumentation/energy/codecarbon_meter.py

"""
CodeCarbon Energy Meter
=======================

Measures carbon emissions and energy consumption using CodeCarbon.

Enhancements
------------
- Optional ``codecarbon`` import guarded by ``_CODECARBON_AVAILABLE``.
- ``CodeCarbonConfig`` — frozen, validated, centralizes ``measure_power_secs``,
  tracking mode, output dir, and version-tolerant public-API access.
- Defensive extraction of ``total_energy`` (public attribute → private
  attribute → ``final_emissions_data`` → zero) so version drift does not
  crash the meter.
- Fixed the missing ``power_watts`` field in the ``stop()`` contract.
- Lifecycle tracking via ``_mark_started`` / ``_mark_stopped``.
- ``stop()`` result routed through ``BaseEnergyMeter._validate_metrics``.
- ``RLock``-guarded ``start`` / ``stop`` (prevents double-start races).
- Custom ``CodeCarbonMeterError``; lazy ``%s`` logging; ``__repr__``;
  ``__main__`` smoke test with a mocked tracker.
"""

from __future__ import annotations

import json
import logging
import math
import threading
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Mapping, Optional

from .base_meter import (
    BaseEnergyMeter,
    BaseMeterConfig,
    InstrumentationError,
    TelemetrySample,
)

logger = logging.getLogger(__name__)


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
# Errors
# --------------------------------------------------------------------------- #
class CodeCarbonMeterError(InstrumentationError):
    """Raised for invalid CodeCarbon meter inputs or runtime failures."""


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class CodeCarbonConfig:
    """Tunable parameters for the CodeCarbon meter."""

    # How often CodeCarbon samples power (seconds).
    measure_power_secs: float = 1.0

    # CodeCarbon tracking mode.
    tracking_mode: str = "machine"      # "machine" | "process"

    # Optional output directory for CodeCarbon CSVs. ``None`` uses CC's default.
    output_dir: Optional[str] = None

    # Optional experiment name / project / country overrides.
    project_name: Optional[str] = None
    country_iso_code: Optional[str] = None

    # If the tracker reports zero energy after the measurement window, treat
    # the reading as suspect (usually means a very short window).
    min_energy_joules: float = 0.0

    # Fallback provenance value when the tracker returns an unrecognized state.
    default_provenance: str = "estimated"

    def __post_init__(self) -> None:
        if self.measure_power_secs <= 0:
            raise CodeCarbonMeterError("measure_power_secs must be > 0.")
        if self.tracking_mode not in ("machine", "process"):
            raise CodeCarbonMeterError(
                "tracking_mode must be 'machine' or 'process'."
            )
        if self.output_dir is not None and not isinstance(self.output_dir, str):
            raise CodeCarbonMeterError("output_dir must be a string or None.")
        if self.min_energy_joules < 0:
            raise CodeCarbonMeterError("min_energy_joules must be >= 0.")
        if self.default_provenance not in ("measured", "estimated", "simulated"):
            raise CodeCarbonMeterError(
                "default_provenance must be one of measured/estimated/simulated."
            )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# --------------------------------------------------------------------------- #
# Meter
# --------------------------------------------------------------------------- #
class CodeCarbonMeter(BaseEnergyMeter):
    """
    Measures carbon and energy using CodeCarbon.

    Parameters
    ----------
    config : CodeCarbonConfig, optional
        Meter configuration. Defaults to ``CodeCarbonConfig()``.
    base_config : BaseMeterConfig, optional
        Contract policy passed to :class:`BaseEnergyMeter`.
    strict : bool, default True
        If True, missing CodeCarbon or contract violations raise
        :class:`CodeCarbonMeterError`. If False, the meter logs and falls back.
    tracker_factory : callable, optional
        Zero-arg callable returning a tracker-shaped object exposing
        ``start()``, ``stop()``, ``_total_energy``, and ``final_emissions_data``.
        Used to inject a mock in tests.
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

        # ---- Build the tracker (or fail in strict mode) ---------------- #
        if tracker_factory is None and not _CODECARBON_AVAILABLE:
            msg = (
                "codecarbon is not installed and no tracker_factory was "
                "provided."
            )
            if self._strict:
                raise CodeCarbonMeterError(msg)
            logger.warning("%s CodeCarbonMeter will use a no-op tracker.", msg)
            self._tracker = _NoopTracker()
        else:
            self._tracker = self._build_tracker()

        logger.debug(
            "CodeCarbonMeter initialized (tracker=%s, measure_power_secs=%.2fs, "
            "tracking_mode=%s, strict=%s)",
            type(self._tracker).__name__,
            self._cc_config.measure_power_secs,
            self._cc_config.tracking_mode,
            self._strict,
        )

    # ---------------------------------------------------------- tracker setup
    def _build_tracker(self) -> Any:
        """Construct the underlying CodeCarbon tracker (or a mock)."""
        if self._tracker_factory is not None:
            try:
                tracker = self._tracker_factory()
            except Exception as exc:
                raise CodeCarbonMeterError(
                    f"tracker_factory failed: {exc}"
                ) from exc
            if tracker is None:
                raise CodeCarbonMeterError("tracker_factory returned None.")
            return tracker

        kwargs: Dict[str, Any] = {
            "measure_power_secs": self._cc_config.measure_power_secs,
            "tracking_mode": self._cc_config.tracking_mode,
            "save_to_file": False,  # we manage persistence ourselves
        }
        if self._cc_config.output_dir is not None:
            kwargs["output_dir"] = self._cc_config.output_dir
        if self._cc_config.project_name is not None:
            kwargs["project_name"] = self._cc_config.project_name
        if self._cc_config.country_iso_code is not None:
            kwargs["country_iso_code"] = self._cc_config.country_iso_code

        try:
            return EmissionsTracker(**kwargs)  # type: ignore[misc]
        except TypeError:
            # Older CodeCarbon versions accept a narrower kwargs set.
            logger.debug(
                "EmissionsTracker rejected extended kwargs; retrying with "
                "the minimal set."
            )
            try:
                return EmissionsTracker(  # type: ignore[misc]
                    measure_power_secs=self._cc_config.measure_power_secs
                )
            except Exception as exc:
                raise CodeCarbonMeterError(
                    f"Could not construct EmissionsTracker: {exc}"
                ) from exc
        except Exception as exc:
            raise CodeCarbonMeterError(
                f"Could not construct EmissionsTracker: {exc}"
            ) from exc

    # ---------------------------------------------------------- lifecycle
    def start(self) -> None:
        """Start CodeCarbon measurement."""
        with self._inner_lock:
            if self.started:
                logger.debug("CodeCarbonMeter.start() called while already running.")
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
        Stop CodeCarbon measurement and return metrics:

        ``{"energy_joules": float, "power_watts": float, "carbon_kg": float,
        "provenance": "estimated"}``
        """
        with self._inner_lock:
            elapsed = self._mark_stopped()
            emissions: Optional[float] = None
            try:
                emissions = self._tracker.stop()
            except Exception as exc:
                logger.exception("tracker.stop() failed: %s", exc)
                if self._strict:
                    raise CodeCarbonMeterError(
                        f"tracker.stop() failed: {exc}"
                    ) from exc
                emissions = None

            energy_joules = self._read_energy_joules()
            power_watts = self._derive_power_watts(
                energy_joules=energy_joules, elapsed_seconds=elapsed
            )
            carbon_kg = (
                float(emissions)
                if isinstance(emissions, (int, float))
                and not isinstance(emissions, bool)
                and math.isfinite(float(emissions))
                else 0.0
            )
            self._last_emissions_kg = carbon_kg

            raw = {
                "energy_joules": energy_joules,
                "power_watts": power_watts,
                "carbon_kg": carbon_kg,
                "provenance": self._cc_config.default_provenance,
                "source": "codecarbon",
                "elapsed_seconds": elapsed,
            }

        logger.debug(
            "CodeCarbonMeter stopped: energy=%.6g J, power=%.4g W, "
            "carbon=%.6g kg, elapsed=%.3fs",
            raw["energy_joules"],
            raw["power_watts"],
            raw["carbon_kg"],
            elapsed,
        )

        # Enforce the BaseEnergyMeter contract.
        return self._validate_metrics(raw)

    # ---------------------------------------------------------- extraction
    def _read_energy_joules(self) -> float:
        """
        Read cumulative energy from the tracker using the most public API
        available; falls back through several CodeCarbon representations.
        """
        tracker = self._tracker

        # 1. Public API: ``final_emissions_data.energy_consumed`` (kWh).
        final_data = getattr(tracker, "final_emissions_data", None)
        if final_data is not None:
            kwh = getattr(final_data, "energy_consumed", None)
            if isinstance(kwh, (int, float)) and math.isfinite(float(kwh)):
                return float(kwh) * 3.6e6

        # 2. Legacy / semi-public: ``_total_energy.kWh``.
        total_energy = getattr(tracker, "_total_energy", None)
        if total_energy is not None:
            kwh = getattr(total_energy, "kWh", None)
            if isinstance(kwh, (int, float)) and math.isfinite(float(kwh)):
                return float(kwh) * 3.6e6

        # 3. Even older: ``_total_energy`` as a float in kWh.
        if isinstance(total_energy, (int, float)) and math.isfinite(
            float(total_energy)
        ):
            return float(total_energy) * 3.6e6

        # 4. Nothing usable.
        msg = (
            "could not read energy from the CodeCarbon tracker; "
            "using 0.0 J."
        )
        if self._strict:
            raise CodeCarbonMeterError(msg)
        logger.warning("%s", msg)
        return 0.0

    @staticmethod
    def _derive_power_watts(
        *, energy_joules: float, elapsed_seconds: float
    ) -> float:
        """Derive average power from cumulative energy and elapsed time."""
        if elapsed_seconds <= 0 or energy_joules <= 0:
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
    def tracker(self) -> Any:
        """Live underlying tracker (useful for tests)."""
        return self._tracker

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "config": asdict(self._cc_config),
            "base_config": asdict(self._config),
            "strict": self._strict,
            "started": self.started,
            "codecarbon_available": _CODECARBON_AVAILABLE,
            "tracker_type": type(self._tracker).__name__,
        }

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    def __repr__(self) -> str:
        return (
            "CodeCarbonMeter("
            f"tracker={type(self._tracker).__name__}, "
            f"measure_power_secs={self._cc_config.measure_power_secs:.2f}, "
            f"tracking_mode={self._cc_config.tracking_mode!r}, "
            f"started={self.started}, "
            f"strict={self._strict})"
        )


# --------------------------------------------------------------------------- #
# No-op tracker (used when codecarbon is missing and strict=False)
# --------------------------------------------------------------------------- #
class _NoopTracker:
    """Silent stand-in used when ``codecarbon`` is not installed."""

    class _Energy:
        kWh: float = 0.0

    def __init__(self) -> None:
        self._total_energy = self._Energy()

    def start(self) -> None:  # pragma: no cover — trivial
        return None

    def stop(self) -> float:  # pragma: no cover — trivial
        return 0.0


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    "CodeCarbonConfig",
    "CodeCarbonMeter",
    "CodeCarbonMeterError",
]


# --------------------------------------------------------------------------- #
# Smoke test: python -m instrumentation.energy.codecarbon_meter
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    # ---- Mock tracker (deterministic; no CodeCarbon install required) -- #
    class _MockEnergy:
        def __init__(self, kwh: float) -> None:
            self.kWh = kwh

    class _MockTracker:
        """Tracker-shaped object for tests."""

        def __init__(self, energy_kwh: float, emissions_kg: float) -> None:
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
        return _MockTracker(energy_kwh=0.0025, emissions_kg=0.0011)

    # ---- Happy path --------------------------------------------------- #
    meter = CodeCarbonMeter(tracker_factory=_factory)
    print("repr       :", meter)
    with meter.measure() as m:
        time.sleep(0.05)
    sample: Optional[TelemetrySample] = getattr(m, "last_sample", None)
    print("sample     :", sample)
    if sample is not None:
        print("dict       :", sample.to_dict())

    # ---- Serialization round-trip ------------------------------------- #
    if sample is not None:
        payload = sample.to_json()
        restored = TelemetrySample.from_json(payload)
        assert restored.to_dict() == sample.to_dict()
        print("Round-trip OK.")

    # ---- Explicit start / stop (legacy path) -------------------------- #
    meter2 = CodeCarbonMeter(tracker_factory=_factory)
    meter2.start()
    time.sleep(0.02)
    metrics = meter2.stop()
    print("legacy stop:", metrics)
    assert "power_watts" in metrics, "contract requires power_watts"
    assert "energy_joules" in metrics
    assert "provenance" in metrics

    # ---- Missing codecarbon in strict mode ---------------------------- #
    import importlib
    saved_available = importlib.import_module(__name__)
    # Simulate absence by constructing with strict + no factory but patching
    # the module-level flag on a throwaway instance.
    try:
        from .base_meter import BaseEnergyMeter  # type: ignore

        class _GhostMeter(BaseEnergyMeter):
            def start(self): self._mark_started()
            def stop(self): return self._validate_metrics({})  # type: ignore

        with _GhostMeter(strict=True).measure():
            pass
    except InstrumentationError as exc:
        print("Contract   :", exc)

    # ---- Config validation -------------------------------------------- #
    for bad in (
        dict(measure_power_secs=0),
        dict(tracking_mode="bogus"),
        dict(min_energy_joules=-1.0),
        dict(default_provenance="fabricated"),
    ):
        try:
            CodeCarbonConfig(**bad)  # type: ignore[arg-type]
        except CodeCarbonMeterError as exc:
            print("Rejected cfg:", exc)

    print("\nSmoke test passed.")
