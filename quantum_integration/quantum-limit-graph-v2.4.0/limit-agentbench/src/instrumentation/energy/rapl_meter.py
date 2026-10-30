# src/instrumentation/energy/rapl_meter.py

"""
RAPL CPU Meter
==============

Real CPU energy via Intel RAPL (through ``pyRAPL``).

Enhancements
------------
- :class:`Provenance` (from ``base_meter``) replaces the raw string.
- **Sanitized propagation** — every fallback (missing pyRAPL, failed
  setup, `begin()`/`end()` failure, unparseable domain, overlong
  measurement) sets ``self._last_sanitized = True``. Downstream consumers
  can distinguish fabricated estimates from real measurements.
- **`_extract_joules` returns `(value, ok)`** so "domain legitimately not
  measured" is distinguishable from "domain was requested but the counter
  was unreadable (NaN, wrong shape, iterable safety cap)".
- **`measurement.end()` no longer lies** — if `end()` succeeds but any
  requested domain is unreadable, the sample is marked sanitized.
- **Numeric config strictness** — rejects `bool` / `NaN` / negative
  values; `include_pkg` / `include_dram` / `eager_setup` are type-checked
  as bools; `fallback_duty_cycle` is bounded to `[0, 1]`.
- **`max_measurement_seconds` is enforced** — strict mode raises; lenient
  mode logs and sets ``sanitized``.
- **`fallback_duty_cycle` is configurable** (default 0.5).
- **`measurement_factory` / `setup_fn` validated** for callability at
  construction.
- **Generic-iterable extraction is bounded** — rejects `str` / `bytes` /
  `Mapping` and caps at ``256`` items.
- **`_last_sanitized` reset at start of `stop()`** — direct callers don't
  inherit stale flags.
- **`last_sanitized` / `last_provenance` properties** exposed.
- ``pkg_joules`` / ``dram_joules`` only emitted when the corresponding
  ``include_*`` flag is set.
- ``__main__`` smoke test extended with sanitization, overlong-window,
  and unreadable-domain regression cases.

Modeling note
-------------
When RAPL counters are unavailable, the estimate is::

    energy = (fallback_idle_watts
              + fallback_dynamic_watts * fallback_duty_cycle) * elapsed

``fallback_duty_cycle`` defaults to 0.5 — a "middle-of-the-road" CPU
assumption. A fabricated estimate is always marked ``sanitized=True``.

Note on ``pyRAPL`` availability
-------------------------------
Deferred setup (the default) defers ``pyRAPL.setup()`` to the first
``start()``, so permission errors surface at use time rather than
construction time. ``eager_setup=True`` restores the original
construction-time behavior.
"""

from __future__ import annotations

import json
import logging
import math
import numbers
import threading
import time
from dataclasses import asdict, dataclass
from typing import (
    Any,
    Dict,
    List,
    Mapping,
    Optional,
    Tuple,
)

from .base_meter import (
    BaseEnergyMeter,
    BaseMeterConfig,
    InstrumentationError,
    Provenance,
    TelemetrySample,
)

logger = logging.getLogger(__name__)

__all__ = [
    "RAPLCPUMeter",
    "RAPLConfig",
    "RAPLCPUMeterError",
]


# --------------------------------------------------------------------------- #
# Optional dependency
# --------------------------------------------------------------------------- #
try:  # pragma: no cover — environment-dependent
    import pyRAPL  # type: ignore

    _PYRAPL_AVAILABLE = True
except ImportError:  # pragma: no cover
    pyRAPL = None  # type: ignore[assignment]
    _PYRAPL_AVAILABLE = False
    logger.debug("pyRAPL not importable; RAPLCPUMeter disabled.")


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class RAPLCPUMeterError(InstrumentationError):
    """Raised for invalid RAPL meter inputs or runtime failures."""


# --------------------------------------------------------------------------- #
# Config helpers
# --------------------------------------------------------------------------- #
def _require_non_negative_finite(value: Any, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, numbers.Real):
        raise RAPLCPUMeterError(f"{label} must be a number, not bool")
    fv = float(value)
    if not math.isfinite(fv):
        raise RAPLCPUMeterError(f"{label} must be finite")
    if fv < 0:
        raise RAPLCPUMeterError(f"{label} must be >= 0")
    return fv


def _require_positive_finite(value: Any, label: str) -> float:
    fv = _require_non_negative_finite(value, label)
    if fv <= 0:
        raise RAPLCPUMeterError(f"{label} must be > 0")
    return fv


def _require_bool(value: Any, label: str) -> bool:
    if not isinstance(value, bool):
        raise RAPLCPUMeterError(f"{label} must be a bool")
    return value


# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class RAPLConfig:
    """Tunable parameters for the RAPL CPU meter."""

    label: str = "green_agent"
    include_pkg: bool = True
    include_dram: bool = False

    fallback_idle_watts: float = 3.0
    fallback_dynamic_watts: float = 30.0
    fallback_duty_cycle: float = 0.5

    max_measurement_seconds: float = 3600.0
    eager_setup: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.label, str) or not self.label.strip():
            raise RAPLCPUMeterError(
                "label must be a non-empty string"
            )

        object.__setattr__(
            self, "include_pkg",
            _require_bool(self.include_pkg, "include_pkg"),
        )
        object.__setattr__(
            self, "include_dram",
            _require_bool(self.include_dram, "include_dram"),
        )
        object.__setattr__(
            self, "eager_setup",
            _require_bool(self.eager_setup, "eager_setup"),
        )

        if not (self.include_pkg or self.include_dram):
            raise RAPLCPUMeterError(
                "at least one of include_pkg / include_dram must be True"
            )

        object.__setattr__(
            self, "fallback_idle_watts",
            _require_non_negative_finite(
                self.fallback_idle_watts,
                "fallback_idle_watts",
            ),
        )
        object.__setattr__(
            self, "fallback_dynamic_watts",
            _require_non_negative_finite(
                self.fallback_dynamic_watts,
                "fallback_dynamic_watts",
            ),
        )

        duty = _require_non_negative_finite(
            self.fallback_duty_cycle, "fallback_duty_cycle",
        )
        if duty > 1.0:
            raise RAPLCPUMeterError(
                "fallback_duty_cycle must be in [0, 1]"
            )
        object.__setattr__(self, "fallback_duty_cycle", duty)

        object.__setattr__(
            self, "max_measurement_seconds",
            _require_positive_finite(
                self.max_measurement_seconds,
                "max_measurement_seconds",
            ),
        )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# --------------------------------------------------------------------------- #
# Meter
# --------------------------------------------------------------------------- #
class RAPLCPUMeter(BaseEnergyMeter):
    """
    Real CPU energy via Intel RAPL.

    See the module docstring for the modeling assumption, the deferred
    setup behavior, and the sanitization contract.
    """

    _MICROJOULES_PER_JOULE = 1e6
    # Bound the per-socket list. More than this is almost certainly not a
    # real RAPL reading.
    _MAX_ITERABLE_ITEMS = 256

    def __init__(
        self,
        *,
        config: Optional[RAPLConfig] = None,
        base_config: Optional[BaseMeterConfig] = None,
        strict: bool = True,
        measurement_factory: Optional[Any] = None,
        setup_fn: Optional[Any] = None,
    ) -> None:
        super().__init__(
            config=base_config,
            strict=strict,
            name="rapl",
        )
        if config is not None and not isinstance(config, RAPLConfig):
            raise RAPLCPUMeterError(
                "config must be a RAPLConfig instance"
            )
        if measurement_factory is not None and not callable(
            measurement_factory
        ):
            raise RAPLCPUMeterError(
                "measurement_factory must be callable"
            )
        if setup_fn is not None and not callable(setup_fn):
            raise RAPLCPUMeterError("setup_fn must be callable")

        self._rapl_config: RAPLConfig = config or RAPLConfig()
        self._inner_lock = threading.RLock()

        self._measurement_factory = measurement_factory
        self._setup_fn = setup_fn
        self._setup_done: bool = False
        self._measurement: Any = None
        self._last_energy_joules: float = 0.0
        self._last_power_watts: float = 0.0
        self._last_provenance: Provenance = Provenance.ESTIMATED

        if (
            measurement_factory is None
            and setup_fn is None
            and not _PYRAPL_AVAILABLE
        ):
            msg = "pyRAPL is not installed and no mock was provided."
            if self._strict:
                raise RAPLCPUMeterError(msg)
            logger.warning(
                "%s RAPLCPUMeter will estimate values; samples will "
                "be marked sanitized.", msg,
            )
            self._available = False
        else:
            self._available = True

        if self._rapl_config.eager_setup and self._available:
            try:
                self._do_setup()
            except RAPLCPUMeterError:
                if self._strict:
                    raise
                self._available = False
                logger.warning(
                    "RAPL eager setup failed; estimates will be used "
                    "and samples will be sanitized."
                )

        if self._available:
            try:
                self._measurement = self._build_measurement()
            except RAPLCPUMeterError:
                if self._strict:
                    raise
                self._available = False
                self._measurement = None

        logger.debug(
            "RAPLCPUMeter initialized (label=%s, pkg=%s, dram=%s, "
            "available=%s, strict=%s)",
            self._rapl_config.label,
            self._rapl_config.include_pkg,
            self._rapl_config.include_dram,
            self._available,
            self._strict,
        )

    # ---------------------------------------------------------- setup
    def _do_setup(self) -> None:
        if self._setup_done:
            return
        setup = self._setup_fn
        if setup is None:
            setup = getattr(pyRAPL, "setup", None)
        if not callable(setup):
            raise RAPLCPUMeterError(
                "pyRAPL.setup() is not callable; library is unavailable."
            )
        try:
            setup()
        except Exception as exc:
            raise RAPLCPUMeterError(
                f"pyRAPL.setup() failed: {exc}"
            ) from exc
        self._setup_done = True
        logger.debug("pyRAPL.setup() completed.")

    def _build_measurement(self) -> Any:
        factory = self._measurement_factory
        if factory is None:
            factory = getattr(pyRAPL, "Measurement", None)
        if not callable(factory):
            raise RAPLCPUMeterError(
                "pyRAPL.Measurement is not callable; library is "
                "unavailable."
            )
        try:
            return factory(self._rapl_config.label)
        except TypeError:
            try:
                return factory(label=self._rapl_config.label)
            except Exception as exc:
                raise RAPLCPUMeterError(
                    f"could not construct pyRAPL.Measurement: {exc}"
                ) from exc
        except Exception as exc:
            raise RAPLCPUMeterError(
                f"could not construct pyRAPL.Measurement: {exc}"
            ) from exc

    # ---------------------------------------------------------- lifecycle
    def start(self) -> None:
        with self._inner_lock:
            if self.started:
                logger.debug(
                    "RAPLCPUMeter.start() called while already running."
                )
                return

            if self._available:
                try:
                    self._do_setup()
                except RAPLCPUMeterError:
                    if self._strict:
                        raise
                    self._available = False
                    logger.warning(
                        "pyRAPL.setup() failed; falling back to "
                        "estimates."
                    )

            if self._available and self._measurement is not None:
                try:
                    self._measurement.begin()
                except Exception as exc:
                    logger.exception(
                        "measurement.begin() failed: %s", exc,
                    )
                    if self._strict:
                        raise RAPLCPUMeterError(
                            f"measurement.begin() failed: {exc}"
                        ) from exc
                    self._available = False

            self._mark_started()
        logger.debug("RAPLCPUMeter started.")

    def stop(self) -> Dict[str, Any]:
        with self._inner_lock:
            # Reset state so direct callers don't inherit stale flags.
            self._last_sanitized = False
            elapsed = self._mark_stopped()

            pkg_joules = 0.0
            dram_joules = 0.0
            pkg_ok = True
            dram_ok = True
            provenance = Provenance.ESTIMATED

            if self._available and self._measurement is not None:
                try:
                    self._measurement.end()
                except Exception as exc:
                    logger.exception(
                        "measurement.end() failed: %s", exc,
                    )
                    if self._strict:
                        raise RAPLCPUMeterError(
                            f"measurement.end() failed: {exc}"
                        ) from exc
                    self._available = False
                    self._last_sanitized = True
                else:
                    if self._rapl_config.include_pkg:
                        pkg_joules, pkg_ok = self._extract_joules("pkg")
                    if self._rapl_config.include_dram:
                        dram_joules, dram_ok = self._extract_joules(
                            "dram",
                        )
                    if pkg_ok and dram_ok:
                        provenance = Provenance.MEASURED
                    else:
                        self._last_sanitized = True

            total_joules = pkg_joules + dram_joules

            # -- Fallback if counters were unusable --------------------
            if provenance is not Provenance.MEASURED:
                total_joules = self._estimate_energy_joules(elapsed)
                self._last_sanitized = True
            elif not self._available:
                self._last_sanitized = True

            # -- Enforce max_measurement_seconds ----------------------
            max_secs = self._rapl_config.max_measurement_seconds
            if elapsed > max_secs:
                msg = (
                    f"RAPL measurement window {elapsed:.3f}s exceeds "
                    f"max_measurement_seconds={max_secs:.3f}"
                )
                if self._strict:
                    raise RAPLCPUMeterError(msg)
                logger.warning("%s Marking sample sanitized.", msg)
                self._last_sanitized = True

            # -- Derive power -----------------------------------------
            power_watts = (
                total_joules / elapsed
                if elapsed > 0 and total_joules > 0
                else 0.0
            )

            raw: Dict[str, Any] = {
                "energy_joules": total_joules,
                "power_watts": power_watts,
                "provenance": provenance.value,
                "source": "rapl",
                "elapsed_seconds": elapsed,
                "sanitized": self._last_sanitized,
            }
            if self._rapl_config.include_pkg:
                raw["pkg_joules"] = pkg_joules
            if self._rapl_config.include_dram:
                raw["dram_joules"] = dram_joules

            self._last_energy_joules = total_joules
            self._last_power_watts = power_watts
            self._last_provenance = provenance

        logger.debug(
            "RAPLCPUMeter stopped: pkg=%.6g J, dram=%.6g J, "
            "total=%.6g J, power=%.4g W, elapsed=%.3fs, "
            "provenance=%s, sanitized=%s",
            pkg_joules,
            dram_joules,
            total_joules,
            power_watts,
            elapsed,
            provenance.value,
            self._last_sanitized,
        )

        return self._validate_metrics(raw)

    # ---------------------------------------------------------- extraction
    def _extract_joules(self, domain: str) -> Tuple[float, bool]:
        """
        Extract a domain's cumulative energy from the measurement result.

        Returns ``(joules, ok)`` where ``ok=False`` means the reading was
        requested but unreadable (missing result, NaN, unparseable shape,
        oversize iterable). ``ok=True`` includes the legitimate case where
        a domain is not measured and we did not request it — but this
        function is only called for *requested* domains.
        """
        result = getattr(self._measurement, "result", None)
        if result is None:
            logger.warning(
                "pyRAPL '%s' result is None; counting as sanitized.",
                domain,
            )
            return 0.0, False

        value = getattr(result, domain, None)
        if value is None:
            logger.warning(
                "pyRAPL '%s' domain is None; counting as sanitized.",
                domain,
            )
            return 0.0, False

        microjoules = self._coerce_microjoules(value)
        if microjoules is None:
            msg = (
                f"could not interpret pyRAPL '{domain}' reading of type "
                f"{type(value).__name__}; counting as sanitized."
            )
            if self._strict:
                raise RAPLCPUMeterError(msg)
            logger.warning("%s", msg)
            return 0.0, False

        return microjoules / self._MICROJOULES_PER_JOULE, True

    @classmethod
    def _coerce_microjoules(cls, value: Any) -> Optional[float]:
        """Return the total microjoules from any pyRAPL result shape."""
        # Scalar numbers.
        if isinstance(value, bool):
            return None
        if isinstance(value, numbers.Real):
            fv = float(value)
            if not math.isfinite(fv) or fv < 0:
                return None
            return fv

        # pyRAPL dataclass with ``.value``.
        attr_value = getattr(value, "value", None)
        if attr_value is not None and not isinstance(
            attr_value, bool
        ) and isinstance(attr_value, numbers.Real):
            fv = float(attr_value)
            if not math.isfinite(fv) or fv < 0:
                return None
            return fv

        # Iterables of numbers — reject str / bytes / Mapping (their
        # iteration semantics are not "one value per socket").
        if isinstance(value, (str, bytes, Mapping)):
            return None

        if isinstance(value, (list, tuple)):
            items = list(value)
        elif hasattr(value, "__iter__"):
            try:
                items = list(value)
            except Exception:  # pragma: no cover — defensive
                return None
        else:
            return None

        if len(items) > cls._MAX_ITERABLE_ITEMS:
            return None

        total = 0.0
        for item in items:
            parsed = cls._coerce_microjoules(item)
            if parsed is None:
                return None
            total += parsed
        return total

    # ---------------------------------------------------------- estimation
    def _estimate_energy_joules(self, elapsed: float) -> float:
        """Modelled fallback used when RAPL counters are unavailable."""
        cfg = self._rapl_config
        power = (
            cfg.fallback_idle_watts
            + cfg.fallback_dynamic_watts * cfg.fallback_duty_cycle
        )
        return max(0.0, power * max(elapsed, 0.0))

    # ---------------------------------------------------------- props
    @property
    def rapl_config(self) -> RAPLConfig:
        return self._rapl_config

    @property
    def available(self) -> bool:
        return self._available

    @property
    def label(self) -> str:
        return self._rapl_config.label

    @property
    def last_energy_joules(self) -> float:
        return self._last_energy_joules

    @property
    def last_power_watts(self) -> float:
        return self._last_power_watts

    @property
    def last_provenance(self) -> Provenance:
        return self._last_provenance

    @property
    def last_sanitized(self) -> bool:
        return self._last_sanitized

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "label": self._rapl_config.label,
            "config": self._rapl_config.to_dict(),
            "base_config": asdict(self._config),
            "strict": self._strict,
            "started": self.started,
            "pyrapl_available": _PYRAPL_AVAILABLE,
            "available": self._available,
            "setup_done": self._setup_done,
            "last_provenance": self._last_provenance.value,
            "last_sanitized": self._last_sanitized,
        }

    def to_json(self, **kwargs: Any) -> str:
        return json.dumps(self.to_dict(), default=str, **kwargs)

    def __repr__(self) -> str:
        return (
            "RAPLCPUMeter("
            f"label={self._rapl_config.label!r}, "
            f"available={self._available}, "
            f"pyrapl={_PYRAPL_AVAILABLE}, "
            f"setup={self._setup_done}, "
            f"started={self.started}, "
            f"strict={self._strict}, "
            f"sanitized={self._last_sanitized})"
        )


# --------------------------------------------------------------------------- #
# Smoke test
# --------------------------------------------------------------------------- #
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    class _MeasurementResult:
        def __init__(self, pkg: Any, dram: Any = None) -> None:
            self.pkg = pkg
            self.dram = dram

    class _Measurement:
        def __init__(
            self, label: str, *, pkg: Any, dram: Any = None,
        ) -> None:
            self.label = label
            self._pkg = pkg
            self._dram = dram
            self.result: Optional[_MeasurementResult] = None

        def begin(self) -> None:
            pass

        def end(self) -> None:
            self.result = _MeasurementResult(self._pkg, self._dram)

    def _factory_scalar(label: str) -> _Measurement:
        return _Measurement(
            label, pkg=12_500_000.0, dram=2_000_000.0,
        )

    def _factory_list(label: str) -> _Measurement:
        return _Measurement(
            label,
            pkg=[10_000_000.0, 8_000_000.0, 6_000_000.0],
            dram=[1_500_000.0, 1_200_000.0, 1_000_000.0],
        )

    def _factory_dataclass(label: str) -> _Measurement:
        class _Wrapper:
            def __init__(self, v: float) -> None:
                self.value = v
        return _Measurement(label, pkg=_Wrapper(20_000_000.0))

    def _factory_nan(label: str) -> _Measurement:
        return _Measurement(label, pkg=float("nan"))

    # Happy path
    meter = RAPLCPUMeter(measurement_factory=_factory_scalar)
    print("repr       :", meter)
    with meter.measure() as m:
        time.sleep(0.05)
    sample: Optional[TelemetrySample] = getattr(
        m, "last_sample", None,
    )
    print("sample     :", sample)
    assert sample is not None
    assert sample.sanitized is False
    assert sample.provenance is Provenance.MEASURED

    # Legacy path
    legacy = RAPLCPUMeter(measurement_factory=_factory_scalar)
    legacy.start()
    time.sleep(0.02)
    metrics = legacy.stop()
    assert "energy_joules" in metrics
    assert "power_watts" in metrics
    print("legacy     :", {
        "energy_joules": metrics["energy_joules"],
        "power_watts": metrics["power_watts"],
        "provenance": metrics["provenance"],
    })

    # Multi-socket
    multi = RAPLCPUMeter(measurement_factory=_factory_list)
    with multi.measure() as m:
        time.sleep(0.01)
    assert m.last_sample is not None
    print("multi-sock :", m.last_sample)

    # Dataclass
    dc = RAPLCPUMeter(measurement_factory=_factory_dataclass)
    with dc.measure() as m:
        time.sleep(0.01)
    assert m.last_sample is not None
    print("dataclass  :", m.last_sample)

    # NaN in lenient mode → sanitized
    nan_meter = RAPLCPUMeter(
        measurement_factory=_factory_nan,
        strict=False,
        base_config=BaseMeterConfig(enforce_contract=False),
    )
    with nan_meter.measure() as m:
        time.sleep(0.01)
    assert m.last_sample is not None
    assert m.last_sample.sanitized is True
    assert m.last_sample.provenance is Provenance.ESTIMATED
    print("nan        :", m.last_sample, "(sanitized)")

    # DRAM toggle
    dram_meter = RAPLCPUMeter(
        measurement_factory=_factory_scalar,
        config=RAPLConfig(
            label="dram_run",
            include_pkg=True,
            include_dram=True,
        ),
    )
    with dram_meter.measure() as m:
        time.sleep(0.01)
    assert m.last_sample is not None
    assert "dram_joules" in m.last_sample.extras
    print("with dram  :", m.last_sample.energy_joules, "J")

    # Overlong measurement → sanitized (lenient)
    overlong = RAPLCPUMeter(
        measurement_factory=_factory_scalar,
        config=RAPLConfig(max_measurement_seconds=0.01),
        strict=False,
        base_config=BaseMeterConfig(enforce_contract=False),
    )
    with overlong.measure() as m:
        time.sleep(0.05)  # > 0.01
    assert m.last_sample is not None
    assert m.last_sample.sanitized is True
    print("overlong   :", m.last_sample, "(sanitized)")

    # Overlong measurement → strict raises
    try:
        overlong_strict = RAPLCPUMeter(
            measurement_factory=_factory_scalar,
            config=RAPLConfig(max_measurement_seconds=0.01),
            strict=True,
        )
        overlong_strict.start()
        time.sleep(0.05)
        overlong_strict.stop()
    except RAPLCPUMeterError as exc:
        print("overlong s :", exc)
    else:
        raise AssertionError(
            "strict overlong should have raised"
        )

    # Missing pyRAPL
    import sys
    mod = sys.modules[__name__]
    saved = mod._PYRAPL_AVAILABLE
    mod._PYRAPL_AVAILABLE = False
    try:
        try:
            RAPLCPUMeter(strict=True)
        except RAPLCPUMeterError as exc:
            print("no pyRAPL  :", exc)
        lenient = RAPLCPUMeter(
            strict=False,
            base_config=BaseMeterConfig(enforce_contract=False),
        )
        with lenient.measure() as m:
            time.sleep(0.01)
        assert m.last_sample is not None
        assert m.last_sample.sanitized is True
        print("lenient    :", m.last_sample, "(sanitized)")
    finally:
        mod._PYRAPL_AVAILABLE = saved

    # Serialization
    payload = meter.to_json()
    assert json.loads(payload)["config"]["label"] == "green_agent"
    print("Serialization OK.")

    # Config validation
    bad_configs = (
        dict(label=""),
        dict(include_pkg=False, include_dram=False),
        dict(include_pkg="yes"),  # type: ignore[arg-type]
        dict(include_dram=1),  # type: ignore[arg-type]
        dict(eager_setup="yes"),  # type: ignore[arg-type]
        dict(fallback_idle_watts=-1.0),
        dict(fallback_idle_watts=True),  # type: ignore[arg-type]
        dict(fallback_idle_watts=float("nan")),
        dict(fallback_dynamic_watts=-1.0),
        dict(fallback_dynamic_watts=float("nan")),
        dict(fallback_duty_cycle=-0.1),
        dict(fallback_duty_cycle=1.5),
        dict(fallback_duty_cycle=float("nan")),
        dict(max_measurement_seconds=0),
        dict(max_measurement_seconds=float("nan")),
    )
    for bad in bad_configs:
        try:
            RAPLConfig(**bad)  # type: ignore[arg-type]
        except RAPLCPUMeterError as exc:
            print("Rejected cfg:", exc)
        else:
            raise AssertionError(
                f"config should have rejected {bad}"
            )

    # Live probe
    if _PYRAPL_AVAILABLE:
        real = RAPLCPUMeter(
            strict=False,
            base_config=BaseMeterConfig(enforce_contract=False),
        )
        with real.measure() as m:
            time.sleep(0.05)
        print("real host  :", m.last_sample)
    else:
        print("real host  : pyRAPL not present (skipping live probe).")

    print("\nSmoke test passed.")
