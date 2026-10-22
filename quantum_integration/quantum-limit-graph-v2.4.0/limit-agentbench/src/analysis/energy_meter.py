# src/analysis/telemetry/energy_meter.py
"""
Real energy measurement with graceful degradation (Enhanced, v2.0.0)
=====================================================================

Part of the analysis layer's Priority #1: reliable telemetry and provenance.

Original API preserved (all signatures, all side-effects):
    meter = EnergyMeter()
    meter.start()
    meter.stop()
    joules = meter.joules()            # float
    meter.reset()

Enhanced API:
    started = meter.start()            # bool: True iff this call owns the measurement
    record  = meter.stop()             # MeasurementRecord | None
    record  = meter.measure_detailed() # raises on misuse
    joules  = meter.measure()          # float (raises on misuse)
    meter.set_hardware_profile(...)    # context + fallback-power derivation
    meter.set_hitl_callback(cb)        # in-place reviewer for extreme measurements
    meter.get_statistics()             # per-agent + per-source + energy histogram
    dr = meter.to_decision_record(record=...)
    meter.is_running()
    meter.last_record()
    meter.reset_energy() / meter.reset_statistics()

Enhancements implemented (v2.0.0):
  1.  Robust codecarbon accessor  — works across codecarbon 1.x / 2.x
  2.  Hardware-aware fallback     — preset table drives default power draw
  3.  Precision-aware scaling     — quantisation changes fallback watts (opt-in)
  4.  PUE application             — opt-in; records raw + effective energy
  5.  Carbon capture              — operational kgCO2eq from codecarbon
  6.  Bounded state               — history / aggregates use fixed windows
  7.  Per-agent aggregation       — running counts / sums per agent_id
  8.  Structured XAI              — headline, rationale, confidence
  9.  Data-driven uncertainty     — source-aware ± J
 10.  Temporal verification       — duration bounds flag bad measurements
 11.  HITL                       — threshold flag + in-place reviewer callback
 12.  Lifecycle safety            — double-start / stop-without-start / re-entrant measure()
 13.  Thread safety               — RLock around all state
 14.  JSON-safe serialisation     — MeasurementRecord.to_dict(safe=True)
 15.  Provenance                  — measurement_id, run/task/agent/trace ids
 16.  Backward compatibility      — joules(), start(), stop(), measure(), reset()

Notes on codecarbon compatibility:
  - codecarbon's `EmissionsTracker.stop()` returns a `float` (kg CO2eq) in most
    versions. The *energy* is on `tracker.final_emissions_data.energy_consumed`.
  - This module reads BOTH sources; either firing is sufficient.
  - If you have already configured PUE in codecarbon, do NOT also set
    `apply_pue=True` here — that double-counts datacenter overhead.
"""

from __future__ import annotations

import logging
import math
import threading
import time
import uuid
from collections import Counter, deque
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple

__version__ = "2.0.0"

__all__ = [
    "EnergyMeter",
    "MeasurementRecord",
    "MeasurementSource",
    "Severity",
    "EnergyExplainer",
    "__version__",
]

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================

_UNSET: Any = object()  # sentinel for "argument not provided"


# =============================================================================
# Enums
# =============================================================================

class MeasurementSource(Enum):
    """Where a measurement came from."""
    CODECARBON = "codecarbon"
    HARDWARE = "hardware"      # RAPL / PMT / vendor SDK
    FALLBACK = "fallback"
    UNKNOWN = "unknown"


class Severity(Enum):
    """Severity levels aligned with the analysis layer."""
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"
    EMERGENCY = "emergency"


# =============================================================================
# Hardware power presets — used when fallback_power_watts is not overridden
# =============================================================================

_HARDWARE_POWER_PRESETS: Dict[str, float] = {
    # Low-power / edge
    "raspberry-pi": 5.0,
    "edge": 3.0,
    "jetson-nano": 5.0,
    "jetson-xavier": 20.0,
    "jetson-orin": 25.0,
    # Laptop / desktop
    "laptop": 15.0,
    "desktop": 65.0,
    # Server CPUs
    "xeon": 150.0,
    "epyc": 180.0,
    "cpu-server": 150.0,
    # GPUs
    "t4": 70.0,
    "l4": 72.0,
    "v100": 300.0,
    "a100": 400.0,
    "h100": 700.0,
    "rtx-3090": 350.0,
    "rtx-4090": 450.0,
    # Accelerators
    "tpu-v4": 200.0,
    # Conservative default
    "default": 5.0,
}

_GPU_PROFILE_KEYWORDS = (
    "gpu", "v100", "a100", "h100", "t4", "l4", "tpu",
    "rtx", "cuda", "rocm",
)


def _derive_power_from_profile(profile: Optional[str]) -> Optional[float]:
    """Return the preset wattage for `profile`, or None if unknown."""
    if not isinstance(profile, str) or not profile:
        return None
    key = profile.strip().lower()
    if key in _HARDWARE_POWER_PRESETS:
        return _HARDWARE_POWER_PRESETS[key]
    # Substring match — e.g. "nvidia-v100-16gb" -> v100
    for preset_key, watts in _HARDWARE_POWER_PRESETS.items():
        if preset_key != "default" and preset_key in key:
            return watts
    return None


# =============================================================================
# Precision energy scaling — relative indicators, not absolute claims
# =============================================================================

_PRECISION_ENERGY_SCALE: Dict[str, float] = {
    "fp64": 1.0,
    "fp32": 1.0,
    "tf32": 0.85,
    "fp16": 0.70,
    "bf16": 0.65,
    "int8": 0.40,
    "int4": 0.25,
    "quantum_distilled": 0.25,
}


def _precision_scale(precision: Optional[str]) -> float:
    if not isinstance(precision, str):
        return 1.0
    return _PRECISION_ENERGY_SCALE.get(precision.strip().lower(), 1.0)


# =============================================================================
# Numeric safety helpers
# =============================================================================

def _finite_nonneg(value: Any, default: float = 0.0) -> float:
    """Coerce to a finite, non-negative float, else `default`."""
    try:
        f = float(value)
    except (TypeError, ValueError, OverflowError):
        return default
    if math.isnan(f) or math.isinf(f) or f < 0:
        return default
    return f


def _sanitize_power(value: Any) -> Tuple[float, Optional[str]]:
    """Validate a power value; return (safe_value, violation_or_None)."""
    if isinstance(value, bool):
        return 0.0, "bool_not_allowed"
    try:
        f = float(value)
    except (TypeError, ValueError):
        return 0.0, "non_numeric"
    if math.isnan(f):
        return 0.0, "nan"
    if math.isinf(f):
        return 0.0, "inf"
    if f < 0:
        return 0.0, "negative"
    return f, None


def _json_safe(x: Any) -> Any:
    """Coerce to a JSON-serialisable primitive; non-primitives -> repr()."""
    if x is None or isinstance(x, (bool, int, str)):
        return x
    if isinstance(x, float):
        return x if math.isfinite(x) else None
    return repr(x)


# =============================================================================
# MeasurementRecord
# =============================================================================

@dataclass
class MeasurementRecord:
    """
    Rich result of an energy measurement.

    `energy_joules` is the *effective* value (after precision scaling and,
    when enabled, PUE). `energy_joules_raw` is the pre-scaling value — it is
    `None` when no scaling was applied so downstream can detect that cheaply.
    """
    energy_joules: float
    source: str
    duration_s: float

    # --- Raw (pre-PUE / pre-precision) ---
    energy_joules_raw: Optional[float] = None

    # --- Uncertainty ---
    uncertainty_joules: float = 0.0

    # --- Carbon ---
    carbon_operational_kg: Optional[float] = None
    carbon_contractual_kg: Optional[float] = None

    # --- Power attribution ---
    power_watts: Optional[float] = None            # observed (measured only)
    fallback_power_watts: Optional[float] = None   # assumed (fallback only)
    precision_scale_applied: float = 1.0
    pue_applied: float = 1.0

    # --- Trust flag ---
    simulated: bool = False

    # --- Temporal verification ---
    bounds_ok: bool = True
    bounds_violations: List[str] = field(default_factory=list)

    # --- Severity ---
    severity: str = Severity.INFO.value

    # --- Context ---
    precision: Optional[str] = None
    region: Optional[str] = None
    pue: Optional[float] = None
    hardware_profile: Optional[str] = None
    agent_id: Optional[str] = None
    run_id: Optional[str] = None
    task_id: Optional[str] = None
    trace_id: Optional[str] = None

    # --- Provenance ---
    measurement_id: str = field(default_factory=lambda: uuid.uuid4().hex)

    # --- XAI ---
    explanation: Optional[Dict[str, Any]] = None

    # --- Safety / HITL ---
    needs_review: bool = False
    review_reason: Optional[str] = None

    # --- Metadata ---
    at: datetime = field(default_factory=datetime.now)

    def to_joules(self) -> float:
        """Backward-compatible float view."""
        return self.energy_joules

    def to_dict(self, safe: bool = False) -> Dict[str, Any]:
        """
        Convert to dictionary.

        `safe=True` coerces non-primitive values and replaces NaN/Inf floats
        with `None`, guaranteeing `json.dumps(record.to_dict(safe=True))`
        succeeds.
        """
        out = asdict(self)
        out["at"] = self.at.isoformat()
        if not safe:
            return out

        for key, value in list(out.items()):
            if isinstance(value, float) and not math.isfinite(value):
                out[key] = None
            elif isinstance(value, dict):
                out[key] = {k: _json_safe(v) for k, v in value.items()}
            elif isinstance(value, list):
                out[key] = [_json_safe(v) for v in value]

        expl = out.get("explanation")
        if isinstance(expl, dict):
            if isinstance(expl.get("rationale"), list):
                expl["rationale"] = [str(r) for r in expl["rationale"]]
        return out


# =============================================================================
# XAI explainer
# =============================================================================

class EnergyExplainer:
    """Produce a headline + rationale for a MeasurementRecord."""

    @staticmethod
    def explain(record: MeasurementRecord) -> Dict[str, Any]:
        reasons: List[str] = []

        if record.source == MeasurementSource.CODECARBON.value:
            reasons.append(
                f"Measured by codecarbon: {record.energy_joules:.4f} J "
                f"over {record.duration_s:.3f}s."
            )
        elif record.source == MeasurementSource.HARDWARE.value:
            reasons.append(
                f"Measured by hardware counter: "
                f"{record.energy_joules:.4f} J over {record.duration_s:.3f}s."
            )
        else:
            reasons.append(
                f"Fallback estimate: {record.energy_joules:.4f} J "
                f"({record.fallback_power_watts} W × "
                f"{record.duration_s:.3f}s)."
            )
            reasons.append(
                "WARNING: fallback assumes constant power; actual "
                "consumption may differ by 10×–100× for GPU workloads."
            )
        if record.precision_scale_applied != 1.0:
            reasons.append(
                f"Precision scaling applied: ×{record.precision_scale_applied:.2f} "
                f"(relative indicator only — not an absolute claim)."
            )
        if record.pue_applied != 1.0:
            reasons.append(
                f"PUE overhead applied: ×{record.pue_applied:.3f}."
            )
        if record.power_watts is not None:
            reasons.append(f"Observed power: {record.power_watts:.2f} W.")
        if record.precision:
            reasons.append(f"Precision context: {record.precision}.")
        if record.region:
            reasons.append(f"Region: {record.region}.")
        if record.pue is not None:
            reasons.append(f"PUE (configured): {record.pue}.")
        if not record.bounds_ok:
            reasons.append(
                f"Temporal bounds violated: {record.bounds_violations}."
            )

        if record.source == MeasurementSource.CODECARBON.value:
            confidence = 0.95
        elif record.source == MeasurementSource.HARDWARE.value:
            confidence = 0.95
        else:
            confidence = 0.30

        return {
            "headline": (
                f"{record.energy_joules:.4f} J from {record.source} "
                f"(±{record.uncertainty_joules:.4f} J)"
            ),
            "rationale": reasons,
            "confidence": confidence,
            "simulated": record.simulated,
        }


# =============================================================================
# EnergyMeter
# =============================================================================

class EnergyMeter:
    """
    Enhanced energy meter with backward-compatible API.

    Original:
        meter = EnergyMeter()
        meter.start()
        meter.stop()
        joules = meter.joules()   # float

    Enhanced:
        started = meter.start()   # bool
        record  = meter.stop()    # MeasurementRecord | None
    """

    JOULES_PER_KWH = 3.6e6
    DEFAULT_FALLBACK_POWER_WATTS = 5.0
    REALISTIC_CPU_TDP_WATTS = 65.0
    REALISTIC_GPU_TDP_WATTS = 300.0

    MAX_PLAUSIBLE_DURATION_S = 86_400.0
    MIN_PLAUSIBLE_DURATION_S = 1e-6

    HITL_JOULES_THRESHOLD = 100_000.0

    def __init__(
        self,
        *,
        fallback_power_watts: Any = _UNSET,
        measure_power_secs: int = 1,
        agent_id: Optional[str] = None,
        hardware_profile: Optional[str] = None,
        region: Optional[str] = None,
        pue: Optional[float] = None,
        precision: Optional[str] = None,
        apply_pue: bool = False,
        apply_precision_scale: bool = False,
        features: Optional[Dict[str, bool]] = None,
        tracker_reinit_max_failures: int = 3,
        hitl_joules_threshold: Optional[float] = None,
        max_plausible_duration_s: Optional[float] = None,
        min_plausible_duration_s: Optional[float] = None,
        history_size: int = 1024,
        run_id: Optional[str] = None,
        task_id: Optional[str] = None,
        trace_id: Optional[str] = None,
    ) -> None:
        # --- Identity / context ---
        self.agent_id = agent_id
        self.hardware_profile = hardware_profile
        self.region = region
        self.pue = _finite_nonneg(pue, default=0.0) or None if pue is not None else None
        # (Above: keep None if caller didn't pass it, clamp invalid to None.)
        if pue is not None and self.pue is None:
            logger.warning("Invalid pue=%r; ignored", pue)
        self.precision = precision
        self.run_id = run_id
        self.task_id = task_id
        self.trace_id = trace_id

        # --- Behaviour toggles ---
        self.apply_pue = bool(apply_pue)
        self.apply_precision_scale = bool(apply_precision_scale)

        # --- Fallback power resolution ---
        if fallback_power_watts is _UNSET:
            derived = _derive_power_from_profile(hardware_profile)
            resolved = (
                derived if derived is not None
                else self.DEFAULT_FALLBACK_POWER_WATTS
            )
            self._fallback_power_explicitly_set = False
        else:
            resolved = fallback_power_watts
            self._fallback_power_explicitly_set = True

        power, violation = _sanitize_power(resolved)
        if violation:
            logger.warning(
                "Invalid fallback_power_watts=%r (%s); using %.1f W",
                resolved, violation, self.DEFAULT_FALLBACK_POWER_WATTS,
            )
            power = self.DEFAULT_FALLBACK_POWER_WATTS
        self.fallback_power_watts = power

        # Warn on GPU-looking profile + tiny fallback power
        if (
            self.fallback_power_watts < 20.0
            and isinstance(hardware_profile, str)
            and any(k in hardware_profile.lower() for k in _GPU_PROFILE_KEYWORDS)
        ):
            logger.warning(
                "hardware_profile=%r suggests GPU but fallback_power_watts="
                "%.1fW — this will underestimate energy by orders of "
                "magnitude. Use a known GPU profile or pass "
                "fallback_power_watts explicitly.",
                hardware_profile, self.fallback_power_watts,
            )

        # --- Thresholds ---
        self.hitl_joules_threshold = (
            float(hitl_joules_threshold)
            if hitl_joules_threshold is not None
            else self.HITL_JOULES_THRESHOLD
        )
        self.max_plausible_duration_s = (
            float(max_plausible_duration_s)
            if max_plausible_duration_s is not None
            else self.MAX_PLAUSIBLE_DURATION_S
        )
        self.min_plausible_duration_s = (
            float(min_plausible_duration_s)
            if min_plausible_duration_s is not None
            else self.MIN_PLAUSIBLE_DURATION_S
        )

        # --- Features ---
        self.features: Dict[str, bool] = {
            "provenance": True,
            "uncertainty": True,
            "xai": True,
            "hitl": True,
            "statistics": True,
            "tracker_reinit": True,
        }
        if features:
            self.features.update(features)

        # --- Tracker initialisation ---
        self.measure_power_secs = int(measure_power_secs)
        self.tracker_reinit_max_failures = int(tracker_reinit_max_failures)
        self._tracker: Any = None
        self._tracker_consecutive_failures: int = 0
        self._init_tracker()

        # --- Lifecycle / concurrency ---
        self._lock = threading.RLock()
        self._running = False
        self._start_time: Optional[float] = None
        self._energy_joules: float = 0.0

        # --- Statistics (bounded) ---
        self._measurement_count: int = 0
        self._total_joules: float = 0.0
        self._source_counts: Counter = Counter()
        self._per_agent: Dict[str, Dict[str, float]] = {}
        self._per_source: Dict[str, Dict[str, float]] = {}
        self._history: Deque[MeasurementRecord] = deque(maxlen=history_size)
        self._last_record: Optional[MeasurementRecord] = None
        self._hitl_count: int = 0
        self._bounds_violation_count: int = 0

        # --- HITL callback ---
        self._hitl_callback: Optional[
            Callable[[MeasurementRecord], Optional[bool]]
        ] = None

        logger.debug(
            "EnergyMeter initialised (fallback_power=%.1fW, agent_id=%s, "
            "hardware_profile=%s, apply_pue=%s, apply_precision_scale=%s)",
            self.fallback_power_watts, agent_id, hardware_profile,
            self.apply_pue, self.apply_precision_scale,
        )

    # ------------------------------------------------------------------
    # ORIGINAL PUBLIC API (backward-compatible)
    # ------------------------------------------------------------------

    def start(self) -> bool:
        """
        Begin an energy measurement.

        Returns True iff this call actually started a new measurement.
        Returns False (with a warning) if a measurement was already running.
        """
        with self._lock:
            if self._running:
                logger.warning(
                    "EnergyMeter.start() called while already running; "
                    "ignoring to preserve the in-flight measurement"
                )
                return False

            if (
                self._tracker is None
                and self.features.get("tracker_reinit", True)
                and self._tracker_consecutive_failures
                < self.tracker_reinit_max_failures
            ):
                self._try_reinit_tracker()

            self._start_time = time.time()

            if self._tracker is not None:
                try:
                    self._tracker.start()
                except Exception as e:
                    logger.warning(
                        "Tracker start failed (%s); using fallback", e
                    )
                    self._tracker = None
                    self._tracker_consecutive_failures += 1

            self._running = True
            return True

    def stop(self) -> Optional[MeasurementRecord]:
        """
        End the current measurement and compute joules.

        Returns the completed MeasurementRecord, or None if no measurement
        was in progress. The meter's `joules()` value is updated to the
        effective energy of the returned record.
        """
        with self._lock:
            if not self._running:
                logger.warning(
                    "EnergyMeter.stop() called without a matching start(); "
                    "returning None"
                )
                return None

            duration_s = (
                time.time() - self._start_time
                if self._start_time is not None else 0.0
            )
            # Clear the start timestamp to remove the footgun where
            # downstream code computes time.time() - _start_time.
            self._start_time = None
            self._running = False

            # --- Try codecarbon first (works across 1.x / 2.x) ---
            energy_kwh, emissions_kg = self._stop_tracker()

            if energy_kwh is not None:
                raw_joules = energy_kwh * self.JOULES_PER_KWH
                source = MeasurementSource.CODECARBON.value
                uncertainty_fraction = 0.05
                self._tracker_consecutive_failures = 0
            else:
                raw_joules = duration_s * self.fallback_power_watts
                source = MeasurementSource.FALLBACK.value
                uncertainty_fraction = 0.50

            record = self._build_record(
                raw_joules=raw_joules,
                source=source,
                duration_s=duration_s,
                uncertainty_fraction=uncertainty_fraction,
                emissions_kg=emissions_kg,
            )

            # --- Always update last-record + energy_joules ---
            self._last_record = record
            self._energy_joules = record.energy_joules

            # --- Statistics bookkeeping (opt-out) ---
            if self.features.get("statistics", True):
                self._record(record)

            logger.info(
                "energy.measured",
                extra={
                    "measurement_id": record.measurement_id,
                    "source": record.source,
                    "energy_joules": record.energy_joules,
                    "energy_joules_raw": record.energy_joules_raw,
                    "duration_s": record.duration_s,
                    "uncertainty_joules": record.uncertainty_joules,
                    "simulated": record.simulated,
                    "needs_review": record.needs_review,
                    "bounds_ok": record.bounds_ok,
                    "severity": record.severity,
                    "agent_id": record.agent_id,
                    "run_id": record.run_id,
                    "task_id": record.task_id,
                    "trace_id": record.trace_id,
                    "precision": record.precision,
                    "hardware_profile": record.hardware_profile,
                    "pue_applied": record.pue_applied,
                },
            )
            return record

    def joules(self) -> float:
        """Return the most recent measurement in joules (float)."""
        with self._lock:
            return self._energy_joules

    # ------------------------------------------------------------------
    # ENHANCED PUBLIC API
    # ------------------------------------------------------------------

    def measure(self) -> float:
        """
        One-shot measurement. Raises RuntimeError on misuse
        (already-running meter, start() failed).
        """
        if self._running:
            raise RuntimeError(
                "EnergyMeter.measure() called while a measurement is in "
                "progress; use stop() to collect the in-flight result"
            )
        if not self.start():
            raise RuntimeError("EnergyMeter.start() failed unexpectedly")
        record = self.stop()
        if record is None:
            raise RuntimeError("EnergyMeter.stop() produced no record")
        return record.energy_joules

    def measure_detailed(self) -> MeasurementRecord:
        """
        One-shot measurement returning the full MeasurementRecord.
        Raises RuntimeError on misuse.
        """
        if self._running:
            raise RuntimeError(
                "EnergyMeter.measure_detailed() called while a measurement "
                "is in progress; use stop() to collect the in-flight result"
            )
        if not self.start():
            raise RuntimeError("EnergyMeter.start() failed unexpectedly")
        record = self.stop()
        if record is None:
            raise RuntimeError("EnergyMeter.stop() produced no record")
        return record

    def is_running(self) -> bool:
        with self._lock:
            return self._running

    def last_record(self) -> Optional[MeasurementRecord]:
        with self._lock:
            return self._last_record

    def get_statistics(self) -> Dict[str, Any]:
        """Return cumulative measurement statistics (thread-safe)."""
        with self._lock:
            count = self._measurement_count
            mean = self._total_joules / count if count else 0.0

            energy_stats: Dict[str, float] = {
                "min": 0.0, "max": 0.0, "median": 0.0, "std": 0.0,
            }
            if self._history:
                energies = [r.energy_joules for r in self._history]
                sorted_e = sorted(energies)
                n = len(sorted_e)
                median = (
                    sorted_e[n // 2] if n % 2 == 1
                    else (sorted_e[n // 2 - 1] + sorted_e[n // 2]) / 2
                )
                if n > 1:
                    h_mean = sum(energies) / n
                    var = sum((e - h_mean) ** 2 for e in energies) / n
                    std = math.sqrt(var)
                else:
                    std = 0.0
                energy_stats = {
                    "min": sorted_e[0],
                    "max": sorted_e[-1],
                    "median": median,
                    "std": std,
                }

            return {
                "analyzer_version": __version__,
                "measurement_count": count,
                "total_joules": self._total_joules,
                "mean_joules": mean,
                "energy_stats": energy_stats,
                "source_counts": dict(self._source_counts),
                "per_agent": {a: dict(v) for a, v in self._per_agent.items()},
                "per_source": {
                    s: dict(v) for s, v in self._per_source.items()
                },
                "hitl_count": self._hitl_count,
                "bounds_violation_count": self._bounds_violation_count,
                "tracker_available": self._tracker is not None,
                "tracker_consecutive_failures":
                    self._tracker_consecutive_failures,
                "running": self._running,
                "fallback_power_watts": self.fallback_power_watts,
                "apply_pue": self.apply_pue,
                "apply_precision_scale": self.apply_precision_scale,
                "pue": self.pue,
                "precision": self.precision,
                "hardware_profile": self.hardware_profile,
                "agent_id": self.agent_id,
                "region": self.region,
                "run_id": self.run_id,
                "task_id": self.task_id,
            }

    def export(self) -> Dict[str, Any]:
        """Full serialisable export (bounded by `history_size`)."""
        with self._lock:
            return {
                "analyzer_version": __version__,
                "statistics": self.get_statistics(),
                "history": [
                    r.to_dict(safe=True) for r in self._history
                ],
            }

    def reset_energy(self) -> None:
        """Reset the current measurement state (energy, start, running flag)."""
        with self._lock:
            self._energy_joules = 0.0
            self._start_time = None
            self._running = False

    def reset_statistics(self) -> None:
        """Clear all cumulative statistics and history."""
        with self._lock:
            self._measurement_count = 0
            self._total_joules = 0.0
            self._source_counts.clear()
            self._per_agent.clear()
            self._per_source.clear()
            self._history.clear()
            self._last_record = None
            self._hitl_count = 0
            self._bounds_violation_count = 0

    def reset(self) -> None:
        """
        Deprecated: resets only measurement state (energy/start/running),
        matching v1.x semantics. Use `reset_energy()` and/or
        `reset_statistics()` explicitly to be unambiguous.
        """
        logger.warning(
            "EnergyMeter.reset() is deprecated; use reset_energy() or "
            "reset_statistics() explicitly"
        )
        self.reset_energy()

    def set_hardware_profile(
        self,
        *,
        hardware_profile: Optional[str] = None,
        region: Optional[str] = None,
        pue: Optional[float] = None,
        precision: Optional[str] = None,
        fallback_power_watts: Optional[float] = None,
    ) -> None:
        """Update hardware/region context used for future measurements."""
        with self._lock:
            if hardware_profile is not None:
                self.hardware_profile = hardware_profile
                if not self._fallback_power_explicitly_set:
                    derived = _derive_power_from_profile(hardware_profile)
                    if derived is not None:
                        self.fallback_power_watts = derived
            if region is not None:
                self.region = region
            if pue is not None:
                pue_f = _finite_nonneg(pue, default=-1.0)
                if pue_f > 0:
                    self.pue = pue_f
                else:
                    logger.warning("Invalid pue=%r; ignored", pue)
            if precision is not None:
                self.precision = precision
            if fallback_power_watts is not None:
                power, violation = _sanitize_power(fallback_power_watts)
                if violation:
                    logger.warning(
                        "Invalid fallback_power_watts=%r (%s); ignored",
                        fallback_power_watts, violation,
                    )
                else:
                    self.fallback_power_watts = power
                    self._fallback_power_explicitly_set = True

    def set_hitl_callback(
        self,
        callback: Callable[[MeasurementRecord], Optional[bool]],
    ) -> None:
        """
        Register a reviewer for extreme measurements.

        The callback receives the record *before* it is returned to the
        caller and may mutate it. Returning `False` appends
        `"[HITL: denied]"` to `review_reason`. Exceptions are logged and
        swallowed.
        """
        self._hitl_callback = callback

    def to_decision_record(
        self,
        *,
        record: Optional[MeasurementRecord] = None,
        run_id: Optional[str] = None,
        task_id: Optional[str] = None,
        policy_version: str = "",
    ) -> Optional[Any]:
        """
        Emit a DecisionRecord for a completed measurement.

        If `record` is None, uses `last_record()`. Refuses (returns None)
        if no measurement has been taken yet or one is in progress.
        Returns None if the DecisionRecord contract isn't importable.
        """
        if record is None:
            with self._lock:
                if self._running:
                    logger.warning(
                        "to_decision_record() called while a measurement is "
                        "in progress; returning None"
                    )
                    return None
                record = self._last_record
            if record is None:
                logger.debug(
                    "to_decision_record(): no completed measurement available"
                )
                return None

        try:
            from src.analysis import DecisionRecord  # type: ignore
        except Exception:
            try:
                from analysis import DecisionRecord  # type: ignore
            except Exception:
                logger.debug(
                    "DecisionRecord not importable; skipping emission"
                )
                return None

        # Enrich explanation with fields that are easy to miss downstream.
        explanation: Dict[str, Any] = dict(record.explanation or {})
        explanation.update({
            "energy_joules": record.energy_joules,
            "energy_joules_raw": record.energy_joules_raw,
            "duration_s": record.duration_s,
            "uncertainty_joules": record.uncertainty_joules,
            "precision_scale_applied": record.precision_scale_applied,
            "pue_applied": record.pue_applied,
            "bounds_ok": record.bounds_ok,
            "bounds_violations": list(record.bounds_violations),
            "needs_review": record.needs_review,
            "review_reason": record.review_reason,
            "severity": record.severity,
            "simulated": record.simulated,
        })

        return DecisionRecord(
            run_id=run_id or record.run_id or "",
            timestamp=record.at,
            task_id=task_id or record.task_id or "",
            selected_action=f"energy_measurement({record.source})",
            policy_version=policy_version,
            model_or_agent=record.agent_id or "",
            hardware_profile=record.hardware_profile or "",
            precision=record.precision,
            latency_ms=record.duration_s * 1000.0,
            energy_kwh=record.energy_joules / self.JOULES_PER_KWH,
            carbon_operational_kg=record.carbon_operational_kg or 0.0,
            explanation=explanation,
            provenance={
                "source": "energy_meter",
                "measurement_id": record.measurement_id,
                "measurement_source": record.source,
                "simulated": record.simulated,
                "uncertainty_joules": record.uncertainty_joules,
                "trace_id": record.trace_id,
                "analyzer_version": __version__,
            },
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _init_tracker(self) -> None:
        try:
            from codecarbon import EmissionsTracker  # type: ignore
            self._tracker = EmissionsTracker(
                measure_power_secs=self.measure_power_secs,
                log_level="error",
            )
            logger.debug("codecarbon EmissionsTracker initialised")
        except ImportError:
            logger.info(
                "codecarbon not installed; EnergyMeter will use fallback "
                "(%.1f W)", self.fallback_power_watts,
            )
            self._tracker = None
        except Exception as e:
            logger.warning(
                "codecarbon EmissionsTracker init failed (%s); using "
                "fallback", e,
            )
            self._tracker = None
            self._tracker_consecutive_failures += 1

    def _try_reinit_tracker(self) -> None:
        if self._tracker_consecutive_failures >= \
                self.tracker_reinit_max_failures:
            return
        try:
            from codecarbon import EmissionsTracker  # type: ignore
            self._tracker = EmissionsTracker(
                measure_power_secs=self.measure_power_secs,
                log_level="error",
            )
            logger.debug("codecarbon tracker reinitialised")
        except Exception as e:
            self._tracker = None
            self._tracker_consecutive_failures += 1
            logger.debug("codecarbon tracker reinit failed: %s", e)

    def _stop_tracker(self) -> Tuple[Optional[float], Optional[float]]:
        """
        Stop the tracker; return `(energy_kwh, emissions_kg)`.

        Reads across codecarbon 1.x and 2.x:
          - `final_emissions_data.energy_consumed` / `.emissions` (preferred)
          - `_total_energy` (older versions)
          - the `stop()` return value, which is usually a float
            (kg CO2eq) but may carry `.energy_consumed`
        Returns `(None, None)` if the tracker is unavailable or fails.
        """
        if self._tracker is None:
            return None, None

        try:
            stop_result = self._tracker.stop()
        except Exception as e:
            logger.warning("Tracker stop failed: %s", e)
            self._tracker_consecutive_failures += 1
            return None, None

        energy_kwh: Optional[float] = None
        emissions_kg: Optional[float] = None

        # 1) Preferred: final_emissions_data
        data = getattr(self._tracker, "final_emissions_data", None)
        if data is not None:
            e = getattr(data, "energy_consumed", None)
            if isinstance(e, (int, float)):
                ef = float(e)
                if math.isfinite(ef) and ef >= 0:
                    energy_kwh = ef
            c = getattr(data, "emissions", None)
            if isinstance(c, (int, float)):
                cf = float(c)
                if math.isfinite(cf) and cf >= 0:
                    emissions_kg = cf

        # 2) Older: _total_energy
        if energy_kwh is None:
            e = getattr(self._tracker, "_total_energy", None)
            if isinstance(e, (int, float)):
                ef = float(e)
                if math.isfinite(ef) and ef >= 0:
                    energy_kwh = ef

        # 3) Last resort: stop_result itself carries energy
        if energy_kwh is None and stop_result is not None:
            e = getattr(stop_result, "energy_consumed", None)
            if isinstance(e, (int, float)):
                ef = float(e)
                if math.isfinite(ef) and ef >= 0:
                    energy_kwh = ef

        # stop_result is usually a float (kg CO2eq)
        if emissions_kg is None and isinstance(stop_result, (int, float)):
            cf = float(stop_result)
            if math.isfinite(cf) and cf >= 0:
                emissions_kg = cf

        return energy_kwh, emissions_kg

    def _build_record(
        self,
        *,
        raw_joules: float,
        source: str,
        duration_s: float,
        uncertainty_fraction: float,
        emissions_kg: Optional[float],
    ) -> MeasurementRecord:
        simulated = source != MeasurementSource.CODECARBON.value
        violations: List[str] = []

        # --- Sanitise raw energy ---
        safe_raw = _finite_nonneg(raw_joules, default=0.0)
        if safe_raw != raw_joules:
            violations.append("raw_energy_sanitized")

        # --- Precision scaling (fallback only; only when opted in) ---
        precision_scale = 1.0
        if (
            simulated
            and self.apply_precision_scale
            and self.precision
        ):
            precision_scale = _precision_scale(self.precision)
        scaled_joules = safe_raw * precision_scale

        # --- PUE (when opted in; applies to measured and fallback) ---
        pue_multiplier = 1.0
        if self.apply_pue and self.pue is not None and self.pue > 0:
            pue_multiplier = self.pue
        effective = scaled_joules * pue_multiplier

        # --- Uncertainty ---
        uncertainty_joules = 0.0
        if self.features.get("uncertainty", True):
            uncertainty_joules = max(0.0, effective * uncertainty_fraction)

        # --- Duration bounds ---
        if duration_s < self.min_plausible_duration_s:
            violations.append(
                f"duration_below_min({duration_s:.6g}s < "
                f"{self.min_plausible_duration_s})"
            )
        elif duration_s > self.max_plausible_duration_s:
            violations.append(
                f"duration_above_max({duration_s:.3f}s > "
                f"{self.max_plausible_duration_s})"
            )
        bounds_ok = len(violations) == 0

        # --- HITL / severity ---
        needs_review = False
        review_reason: Optional[str] = None
        if (
            self.features.get("hitl", True)
            and effective >= self.hitl_joules_threshold
        ):
            needs_review = True
            review_reason = (
                f"energy {effective:.2f} J >= threshold "
                f"{self.hitl_joules_threshold:.2f} J "
                f"(source={source}, duration={duration_s:.2f}s, "
                f"precision={self.precision or 'unknown'})"
            )
        if not bounds_ok:
            needs_review = True
            extra = "bounds: " + "; ".join(violations)
            review_reason = (
                f"{review_reason} | {extra}" if review_reason else extra
            )

        if not bounds_ok:
            severity = Severity.CRITICAL.value
        elif needs_review:
            severity = Severity.WARNING.value
        else:
            severity = Severity.INFO.value

        # --- Power attribution ---
        power_watts: Optional[float] = None
        if not simulated and duration_s > 0:
            power_watts = effective / duration_s

        # --- Provenance (feature-gated) ---
        provenance_enabled = self.features.get("provenance", True)
        agent_id = self.agent_id if provenance_enabled else None
        run_id = self.run_id if provenance_enabled else None
        task_id = self.task_id if provenance_enabled else None
        trace_id = self.trace_id if provenance_enabled else None

        # --- Raw vs effective ---
        energy_raw_field: Optional[float] = None
        if precision_scale != 1.0 or pue_multiplier != 1.0:
            energy_raw_field = safe_raw

        record = MeasurementRecord(
            energy_joules=effective,
            energy_joules_raw=energy_raw_field,
            source=source,
            duration_s=duration_s,
            uncertainty_joules=uncertainty_joules,
            carbon_operational_kg=emissions_kg,
            power_watts=power_watts,
            fallback_power_watts=(
                self.fallback_power_watts if simulated else None
            ),
            precision_scale_applied=precision_scale,
            pue_applied=pue_multiplier,
            simulated=simulated,
            bounds_ok=bounds_ok,
            bounds_violations=violations,
            severity=severity,
            precision=self.precision,
            region=self.region,
            pue=self.pue,
            hardware_profile=self.hardware_profile,
            agent_id=agent_id,
            run_id=run_id,
            task_id=task_id,
            trace_id=trace_id,
            needs_review=needs_review,
            review_reason=review_reason,
        )

        # --- XAI ---
        if self.features.get("xai", True):
            record.explanation = EnergyExplainer.explain(record)

        # --- HITL callback ---
        if needs_review and self._hitl_callback is not None:
            try:
                approved = self._hitl_callback(record)
                if approved is False:
                    record.review_reason = (
                        (record.review_reason or "")
                        + " [HITL: denied]"
                    )
                    logger.info(
                        "HITL denied extreme measurement %s",
                        record.measurement_id,
                    )
            except Exception as e:
                logger.warning("HITL callback failed: %s", e)

        return record

    def _record(self, record: MeasurementRecord) -> None:
        """Update statistics from a completed measurement (under outer lock)."""
        self._measurement_count += 1
        self._total_joules += record.energy_joules
        self._source_counts[record.source] += 1
        self._history.append(record)

        if record.needs_review:
            self._hitl_count += 1
        if not record.bounds_ok:
            self._bounds_violation_count += 1

        agent = record.agent_id
        if isinstance(agent, str):
            agg = self._per_agent.setdefault(
                agent,
                {"count": 0.0, "sum_joules": 0.0, "sum_uncertainty": 0.0},
            )
            agg["count"] += 1
            agg["sum_joules"] += record.energy_joules
            agg["sum_uncertainty"] += record.uncertainty_joules

        src = record.source
        src_agg = self._per_source.setdefault(
            src, {"count": 0.0, "sum_joules": 0.0},
        )
        src_agg["count"] += 1
        src_agg["sum_joules"] += record.energy_joules


# =============================================================================
# Demo
# =============================================================================

if __name__ == "__main__":
    import json

    logging.basicConfig(level=logging.WARNING)

    # =================================================================
    # 1. Original behavior (backward compatible)
    # =================================================================
    print("=== 1. Original behavior ===")
    meter = EnergyMeter()
    meter.start()
    time.sleep(0.05)
    meter.stop()
    print(f"  joules(): {meter.joules():.4f}")

    # =================================================================
    # 2. Hardware preset auto-derives fallback power
    # =================================================================
    print("\n=== 2. Hardware preset (auto-derived power) ===")
    m_hw = EnergyMeter(hardware_profile="nvidia-v100-16gb", agent_id="worker-A")
    print(f"  fallback_power_watts: {m_hw.fallback_power_watts} W "
          f"(from profile 'nvidia-v100-16gb')")
    rec_hw = m_hw.measure_detailed()
    print(f"  energy:    {rec_hw.energy_joules:.2f} J")
    print(f"  source:    {rec_hw.source}")
    print(f"  severity:  {rec_hw.severity}")
    print(f"  bounds_ok: {rec_hw.bounds_ok}")

    # =================================================================
    # 3. codecarbon mock — proves the accessor works
    # =================================================================
    print("\n=== 3. codecarbon mock (measured path fires) ===")

    class _FakeEmissionsData:
        def __init__(self, energy_kwh, emissions_kg):
            self.energy_consumed = energy_kwh
            self.emissions = emissions_kg

    class _FakeTracker:
        """Mimics codecarbon 2.x: stop() returns a float."""
        def __init__(self, energy_kwh=0.005, emissions_kg=0.0004):
            self.final_emissions_data = _FakeEmissionsData(
                energy_kwh, emissions_kg,
            )
        def start(self): pass
        def stop(self): return 0.0004  # kg CO2eq

    m_cc = EnergyMeter(agent_id="mock-agent")
    m_cc._tracker = _FakeTracker(energy_kwh=0.005)  # 5 Wh = 18000 J
    m_cc.start()
    time.sleep(0.01)
    rec_cc = m_cc.stop()
    print(f"  source:                 {rec_cc.source}  (expected: codecarbon)")
    print(f"  energy:                 {rec_cc.energy_joules:.1f} J  "
          f"(expected ≈ 18000.0)")
    print(f"  carbon_operational_kg:  {rec_cc.carbon_operational_kg}")
    print(f"  simulated:              {rec_cc.simulated}")
    print(f"  uncertainty:            ±{rec_cc.uncertainty_joules:.2f} J")

    # =================================================================
    # 4. Lifecycle safety
    # =================================================================
    print("\n=== 4. Lifecycle safety ===")
    m_life = EnergyMeter()
    print(f"  stop() before start() -> {m_life.stop()}  (expected None)")
    print(f"  start() #1 -> {m_life.start()}   (expected True)")
    print(f"  start() #2 -> {m_life.start()}  (expected False)")
    time.sleep(0.01)
    print(f"  stop()     -> {type(m_life.stop()).__name__}  (expected MeasurementRecord)")
    print(f"  stop() #2  -> {m_life.stop()}  (expected None)")

    # =================================================================
    # 5. Re-entrancy raises
    # =================================================================
    print("\n=== 5. Re-entrancy (measure() during running measurement) ===")
    m_rent = EnergyMeter()
    m_rent.start()
    try:
        m_rent.measure()
        print("  ERROR: measure() should have raised")
    except RuntimeError as e:
        print(f"  RuntimeError raised as expected: {e}")
    m_rent.stop()

    # =================================================================
    # 6. PUE + precision scaling (opt-in)
    # =================================================================
    print("\n=== 6. PUE + precision scaling (opt-in) ===")
    m_pue = EnergyMeter(
        fallback_power_watts=100.0,
        pue=1.5,
        apply_pue=True,
        precision="int8",
        apply_precision_scale=True,
    )
    rec_pue = m_pue.measure_detailed()
    print(f"  raw energy (pre-scale):  {rec_pue.energy_joules_raw:.4f} J")
    print(f"  effective energy:        {rec_pue.energy_joules:.4f} J")
    print(f"  precision_scale_applied: {rec_pue.precision_scale_applied}")
    print(f"  pue_applied:             {rec_pue.pue_applied}")

    # =================================================================
    # 7. NaN / Inf handling
    # =================================================================
    print("\n=== 7. NaN / Inf handling ===")
    m_nan = EnergyMeter(fallback_power_watts=float("nan"))
    print(f"  NaN power sanitized to: {m_nan.fallback_power_watts} W")
    rec_nan = m_nan.measure_detailed()
    print(f"  energy:   {rec_nan.energy_joules}  (finite)")
    print(f"  bounds:   {rec_nan.bounds_ok}")

    # =================================================================
    # 8. HITL callback
    # =================================================================
    print("\n=== 8. HITL callback ===")
    m_hitl = EnergyMeter(
        fallback_power_watts=1e9,  # 1 GW — forces review
        hitl_joules_threshold=1.0,
    )
    m_hitl.set_hitl_callback(
        lambda r: r.rationale if False else False  # deny
    )
    rec_hitl = m_hitl.measure_detailed()
    print(f"  needs_review:  {rec_hitl.needs_review}")
    print(f"  review_reason: {rec_hitl.review_reason}")
    print(f"  severity:      {rec_hitl.severity}")

    # =================================================================
    # 9. Per-agent aggregation
    # =================================================================
    print("\n=== 9. Per-agent aggregation ===")
    m_multi = EnergyMeter()
    for agent in ("A", "B", "A", "C", "A"):
        m_multi.agent_id = agent
        m_multi.measure()
    stats = m_multi.get_statistics()
    print(f"  per_agent: {stats['per_agent']}")
    print(f"  per_source: {stats['per_source']}")

    # =================================================================
    # 10. JSON-safe serialization
    # =================================================================
    print("\n=== 10. JSON-safe serialization ===")
    weird = MeasurementRecord(
        energy_joules=float("nan"),
        source="fallback",
        duration_s=1.0,
        explanation={"headline": "x", "rationale": [object()]},
    )
    payload = json.dumps(weird.to_dict(safe=True))
    print(f"  json.dumps(to_dict(safe=True)): OK ({len(payload)} bytes)")

    # =================================================================
    # 11. DecisionRecord emission
    # =================================================================
    print("\n=== 11. DecisionRecord emission ===")
    dr = m_pue.to_decision_record(run_id="run-001", task_id="task-1")
    if dr:
        print(f"  selected_action: {dr.selected_action}")
        print(f"  energy_kwh:      {dr.energy_kwh:.8f}")
        print(f"  carbon_kg:       {dr.carbon_operational_kg}")
        print(f"  explanation keys: {sorted(dr.explanation.keys())}")
        print(f"  provenance:      {dr.provenance}")

    # =================================================================
    # 12. Full statistics dump
    # =================================================================
    print("\n=== 12. Statistics ===")
    print(json.dumps(
        m_multi.get_statistics(), indent=2, default=str,
    ))
