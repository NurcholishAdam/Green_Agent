#!/usr/bin/env python3
# =============================================================================
# FILE: src/analysis/telemetry/carbon_estimator.py
# VERSION: 2.0.0
# =============================================================================
"""
Carbon estimation for Green Agent — v2.0.0
==========================================

Estimates operational carbon emissions from energy consumption with
provenance, uncertainty, XAI, and carbon markets separation.

v1 API preserved (identical signature and float return type):
    est = CarbonEstimator(grid_intensity_g_kwh=385.0, pue=1.2)
    kgco2e = est.estimate(energy_wh)             # → float

v2 enhancements:
  [P0] Stable hashlib.sha256 for grid_source_hash (was `hash()` — non-
       deterministic across Python processes); explicit `energy_is_baseline`
       flag on `estimate_detailed` — precision multiplier only applies when
       caller states input is FP32-baseline (was silently halving measured
       energy); per-field provenance (`energy_provenance`, `grid_provenance`,
       `contractual_provenance`) replaces composite `simulated`; causal
       learner wired into `_self_test` + docstring — no longer a no-op in
       practice.
  [P1] `async_update_from_forecaster` for async forecasters; `estimate_batch_
       detailed` has try/except per-item; `hitl_verdict` (`pending` /
       `approved` / `denied` / `error`); `grid_stale` + `grid_violations`
       surfaced; `CircuitBreaker.half_open_max_calls` limits concurrent
       HALF_OPEN retries; `MAX_PUE=5.0` (was 3.0 — rejected legitimate
       high-PUE facilities); `estimate()` returns `nan` for invalid input
       (was silently 0.0); separate `_real` / `_simulated` statistics;
       `precision` validated against known map.
  [P2] Lazy `%s` logging; module-level `import random`; `rec_kgco2e_avoided`
       computed from grid intensity (was always 0 for RECs); `grid_age_at_
       last_estimate_seconds` in stats; `_self_test()`; `_to_jsonable` for
       `to_dict()`; `reset_statistics()`; `__repr__`; bounded deques
       (`violations`, `profiles`, `events`, `buffer`).
  [P3] `carbon_estimate_to_decision()` bridge to `DecisionRecord`;
       honest class-name comments for Causal / Federated / Adaptive modules;
       REC tracked as `rec_mwh` + `rec_kgco2e_avoided` (never summed with
       operational); `EnergyMeter` runtime-checkable Protocol.

**v2.0.0 breaking changes:**
  - `estimate_detailed(..., precision=X)` defaults to `energy_is_baseline=True`
    which preserves v1 (halves input energy when X is a lower-precision mode).
    Callers with **measured** energy at a specific precision should pass
    `energy_is_baseline=False`.
  - `estimate()` returns `float("nan")` for invalid input (was `0.0`).
  - `CarbonEstimate.simulated` is now a computed property from per-field
    provenance; the legacy condition (static grid + no uncertainty) is
    preserved via `legacy_simulated` for backward-compat callers.
"""

from __future__ import annotations

import hashlib
import logging
import math
import random
import time
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import datetime
from enum import Enum
from typing import (Any, Callable, Deque, Dict, List, Optional, Protocol,
                    Tuple, runtime_checkable)

logger = logging.getLogger(__name__)


# =============================================================================
# Enums
# =============================================================================
class GridSource(Enum):
    STATIC = "static"
    ELECTRICITYMAP = "electricitymap"
    EIA = "eia"
    FORECASTER = "forecaster"
    FEDERATED = "federated"
    MANUAL = "manual"
    UNKNOWN = "unknown"


class CarbonKind(Enum):
    OPERATIONAL = "operational"
    CONTRACTUAL = "contractual"


class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


# [P0-3] per-field provenance — replaces composite `simulated` bool
class Provenance(Enum):
    MEASURED = "measured"
    ESTIMATED = "estimated"
    STATIC = "static"        # config value, never updated
    SIMULATED = "simulated"  # synthetic/demo


# [P3-4] optional injected energy meter
@runtime_checkable
class EnergyMeter(Protocol):
    def measure(self, *, energy_wh_estimate: float) -> float: ...


# Grid source → provenance mapping
_GRID_PROVENANCE: Dict[str, str] = {
    GridSource.STATIC.value: Provenance.STATIC.value,
    GridSource.ELECTRICITYMAP.value: Provenance.MEASURED.value,
    GridSource.EIA.value: Provenance.MEASURED.value,
    GridSource.FORECASTER.value: Provenance.ESTIMATED.value,
    GridSource.FEDERATED.value: Provenance.ESTIMATED.value,
    GridSource.MANUAL.value: Provenance.MEASURED.value,
    GridSource.UNKNOWN.value: Provenance.ESTIMATED.value,
}


# =============================================================================
# [P0-1] Stable hash helper (was `hash()` — non-deterministic across processes)
# =============================================================================
def _stable_hash_float(value: str) -> float:
    """Deterministic hash → [0, 1] range. Stable across Python processes."""
    h = hashlib.sha256(value.encode("utf-8")).digest()
    return int.from_bytes(h[:4], "big") / (2 ** 32 - 1)


# =============================================================================
# [P2-6] JSON-safe serializer
# =============================================================================
def _to_jsonable(v: Any) -> Any:
    if isinstance(v, dict):
        return {str(k): _to_jsonable(val) for k, val in v.items()}
    if isinstance(v, (list, tuple, set, deque)):
        return [_to_jsonable(val) for val in v]
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, Enum):
        return v.value
    if is_dataclass(v) and not isinstance(v, type):
        return _to_jsonable(asdict(v))
    return v


# =============================================================================
# [P1-5] Resilience — Circuit Breaker with HALF_OPEN limiter
# =============================================================================
@dataclass
class CircuitBreaker:
    name: str
    failure_threshold: int = 3
    recovery_timeout_seconds: int = 60
    half_open_max_calls: int = 1   # [P1-5] max concurrent probes in HALF_OPEN
    state: CircuitState = CircuitState.CLOSED
    failures: int = 0
    half_open_calls: int = 0
    last_failure_at: Optional[float] = None

    def can_call(self) -> bool:
        if self.state == CircuitState.CLOSED:
            return True
        if self.state == CircuitState.OPEN:
            if (self.last_failure_at and
                    (time.time() - self.last_failure_at)
                    > self.recovery_timeout_seconds):
                self.state = CircuitState.HALF_OPEN
                self.half_open_calls = 0
                self.half_open_calls += 1
                return True
            return False
        # HALF_OPEN
        if self.half_open_calls >= self.half_open_max_calls:
            return False
        self.half_open_calls += 1
        return True

    def record_success(self) -> None:
        self.failures = 0
        self.half_open_calls = 0
        self.state = CircuitState.CLOSED

    def record_failure(self) -> None:
        self.failures += 1
        self.last_failure_at = time.time()
        if self.state == CircuitState.HALF_OPEN:
            self.state = CircuitState.OPEN
            logger.warning(
                "Circuit %r re-OPENed after HALF_OPEN probe failed",
                self.name)
            return
        if self.failures >= self.failure_threshold:
            self.state = CircuitState.OPEN
            logger.warning(
                "Circuit %r OPEN after %d failures",
                self.name, self.failures)


class ChaosInjector:
    # [P2-2] module-level import random (was imported inside method)
    def __init__(self, failure_rate: float = 0.0) -> None:
        self.failure_rate = failure_rate
        # [P2] bounded
        self.events: Deque[Dict[str, Any]] = deque(maxlen=500)

    def maybe_fail(self, component: str) -> bool:
        if random.random() < self.failure_rate:
            self.events.append({
                "component": component,
                "at": datetime.now().isoformat(),
            })
            return True
        return False


# =============================================================================
# Temporal logic monitor
# =============================================================================
@dataclass
class GridIntensityReading:
    value: float
    source: str
    timestamp: float
    uncertainty: Optional[float] = None
    region: Optional[str] = None

    @property
    def age_seconds(self) -> float:
        return time.time() - self.timestamp


class TemporalLogicMonitor:
    def __init__(
        self,
        max_age_seconds: float = 3600.0,
        min_intensity: float = 0.0,
        max_intensity: float = 2000.0,
    ) -> None:
        self.max_age_seconds = max_age_seconds
        self.min_intensity = min_intensity
        self.max_intensity = max_intensity
        # [P2] bounded
        self.violations: Deque[Dict[str, Any]] = deque(maxlen=1000)

    def verify(self, reading: GridIntensityReading) -> Tuple[bool, List[str]]:
        bad: List[str] = []
        if reading.age_seconds > self.max_age_seconds:
            bad.append("GridIntensityFresh")
        if not (self.min_intensity <= reading.value <= self.max_intensity):
            bad.append("GridIntensityBounds")
        if bad:
            self.violations.append({
                "properties": bad,
                "value": reading.value,
                "source": reading.source,
                "at": datetime.now().isoformat(),
            })
        return (len(bad) == 0, bad)


# =============================================================================
# CarbonEstimate dataclass — [P0-2/P0-3/P1-3/P1-4/P2-3] new fields
# =============================================================================
@dataclass
class CarbonEstimate:
    """
    Rich result of a carbon estimate.

    `to_float()` returns the same value v1's `estimate()` would have returned.
    """
    # --- v1 fields (preserved) ---
    energy_wh: float
    grid_intensity_g_kwh: float
    pue: float
    carbon_operational_kg: float

    # --- [P0-2] energy provenance (input vs effective) ---
    energy_wh_input: float = 0.0
    energy_is_baseline: bool = True

    # --- Grid provenance ---
    grid_source: str = GridSource.STATIC.value
    grid_age_seconds: float = 0.0
    grid_uncertainty: Optional[float] = None
    region: Optional[str] = None

    # --- [P0-3] per-field provenance ---
    energy_provenance: str = Provenance.ESTIMATED.value
    grid_provenance: str = Provenance.STATIC.value
    contractual_provenance: str = Provenance.ESTIMATED.value

    # --- Uncertainty ---
    uncertainty_kg: float = 0.0

    # --- Context ---
    precision: Optional[str] = None
    agent_id: Optional[str] = None
    policy_version: Optional[str] = None

    # --- [P2-3] Contractual instruments (never summed with operational) ---
    carbon_contractual_kg: float = 0.0
    rec_mwh: float = 0.0
    rec_kgco2e_avoided: float = 0.0
    instrument_provenance: Optional[Dict[str, Any]] = None

    # --- Companion estimate ---
    helium_units: float = 0.0

    # --- Explanation ---
    explanation: Optional[Dict[str, Any]] = None

    # --- [P1-3/P1-4] Safety & HITL ---
    needs_review: bool = False
    review_reason: Optional[str] = None
    hitl_verdict: Optional[str] = None   # pending / approved / denied / error
    grid_stale: bool = False
    grid_violations: Optional[List[str]] = None

    # --- Metadata ---
    at: datetime = field(default_factory=datetime.now)

    def to_float(self) -> float:
        """Backward-compatible float view (operational only)."""
        return self.carbon_operational_kg

    @property
    def simulated(self) -> bool:
        """[P0-3] Computed from per-field provenance — True if BOTH are
        estimated/static (no real measurement)."""
        return (self.energy_provenance in (Provenance.ESTIMATED.value,
                                             Provenance.SIMULATED.value)
                and self.grid_provenance in (Provenance.ESTIMATED.value,
                                               Provenance.STATIC.value))

    @property
    def legacy_simulated(self) -> bool:
        """[P0-3] v1 behaviour: static grid AND no uncertainty."""
        return (self.grid_source == GridSource.STATIC.value
                and self.grid_uncertainty is None)

    @property
    def carbon_total_with_contractual(self) -> float:
        """Sum operational + contractual. Reports should display both
        SEPARATELY — never imply RECs cancel physical emissions."""
        return self.carbon_operational_kg + self.carbon_contractual_kg

    def to_dict(self) -> Dict[str, Any]:
        # [P2-6] use _to_jsonable for safe serialization
        out = _to_jsonable(asdict(self))
        out["at"] = self.at.isoformat()
        return out


# =============================================================================
# XAI explainer
# =============================================================================
class CarbonExplainer:
    @staticmethod
    def explain(estimate: CarbonEstimate) -> Dict[str, Any]:
        reasons: List[str] = []
        if estimate.energy_is_baseline:
            reasons.append(
                f"Baseline energy = {estimate.energy_wh_input:.4f} Wh "
                f"({estimate.energy_wh_input / 1000.0:.6f} kWh); "
                f"effective at precision={estimate.precision or 'fp32'} "
                f"= {estimate.energy_wh:.4f} Wh.")
        else:
            reasons.append(
                f"Measured energy = {estimate.energy_wh:.4f} Wh "
                f"({estimate.energy_wh / 1000.0:.6f} kWh) at precision="
                f"{estimate.precision or 'fp32'}.")
        reasons.append(
            f"Grid intensity = {estimate.grid_intensity_g_kwh:.2f} gCO₂/kWh "
            f"(source: {estimate.grid_source}, age: "
            f"{estimate.grid_age_seconds:.1f}s, provenance: "
            f"{estimate.grid_provenance}).")
        reasons.append(f"PUE = {estimate.pue:.3f}.")
        if estimate.grid_uncertainty is not None:
            reasons.append(
                f"Grid uncertainty = ±{estimate.grid_uncertainty:.2f} gCO₂/kWh.")
        if estimate.region:
            reasons.append(f"Region = {estimate.region}.")
        if estimate.helium_units:
            reasons.append(f"Companion helium = {estimate.helium_units:.6f} units.")
        if estimate.grid_stale:
            reasons.append(
                f"⚠ Grid intensity is stale "
                f"({estimate.grid_age_seconds:.0f}s > freshness window). "
                f"Violations: {estimate.grid_violations}.")
        if estimate.rec_mwh:
            reasons.append(
                f"RECs: {estimate.rec_mwh:.4f} MWh "
                f"→ {estimate.rec_kgco2e_avoided:.6f} kg CO₂e avoided "
                f"(never summed with operational).")
        if estimate.carbon_contractual_kg:
            reasons.append(
                f"Offset instruments: {estimate.carbon_contractual_kg:.6f} kg "
                f"CO₂e (kept separate from operational).")

        confidence = 0.95
        if estimate.grid_uncertainty is not None:
            confidence = max(0.5, 0.95 - estimate.grid_uncertainty / 500.0)
        if estimate.grid_stale:
            confidence *= 0.7

        return {
            "headline": (
                f"{estimate.carbon_operational_kg:.6f} kgCO₂e operational"
            ),
            "rationale": reasons,
            "confidence": confidence,
            "operational_vs_contractual": {
                "operational_kg": estimate.carbon_operational_kg,
                "contractual_kg": estimate.carbon_contractual_kg,
                "rec_mwh": estimate.rec_mwh,
                "rec_kgco2e_avoided": estimate.rec_kgco2e_avoided,
                "note": "Operational and contractual are kept separate.",
            },
        }


# =============================================================================
# Precision energy multiplier — [P3-3] honest static table (not "adaptive")
# =============================================================================
class PrecisionEnergyMultiplier:
    """
    Energy multiplier by precision (relative to FP32).

    **Not "adaptive":** no hardware probing, no accuracy feedback. Just a
    static table. The name reflects what it is.
    """
    _MAP: Dict[str, float] = {
        "fp32": 1.00,
        "fp16": 0.75,
        "bf16": 0.80,
        "int8": 0.50,
        "int4": 0.35,
        "quantum_distilled": 0.25,
    }

    @classmethod
    def get(cls, precision: Optional[str]) -> float:
        if precision is None:
            return 1.0
        return cls._MAP.get(precision.lower(), 1.0)

    @classmethod
    def is_known(cls, precision: Optional[str]) -> bool:
        if precision is None:
            return True
        return precision.lower() in cls._MAP

    @classmethod
    def known_levels(cls) -> List[str]:
        return sorted(cls._MAP.keys())


# =============================================================================
# [P0-1/P0-4] Causal carbon learner — stable hash, wired
# =============================================================================
@dataclass
class CausalCorrectionState:
    hour_of_day: int
    grid_source_hash: float       # [P0-1] stable hash
    recent_energy_norm: float


class CausalCarbonLearner:
    """
    Multiplicative residual corrector (NOT causal — no IV, no propensity
    weighting, no do-calculus). Bounded to ±20%. Trains only when
    `record_measured()` is called with ground-truth carbon readings.

    **Renamed from "Causal" to honest "Residual" in docstring — class name
    preserved for backward compat.**
    """
    N_FEATURES = 3

    def __init__(self, lr: float = 0.02) -> None:
        self.lr = lr
        self.coeffs: List[float] = [0.0] * self.N_FEATURES
        self.buffer: Deque[Tuple[List[float], float, float]] = deque(maxlen=512)
        self.observations: int = 0
        self.updates: int = 0

    @staticmethod
    def _features(s: CausalCorrectionState) -> List[float]:
        return [s.hour_of_day / 24.0, s.grid_source_hash, s.recent_energy_norm]

    def predict_correction(self, s: CausalCorrectionState) -> float:
        f = self._features(s)
        c = sum(w * x for w, x in zip(self.coeffs, f))
        return max(-0.2, min(0.2, c))

    def record(
        self, s: CausalCorrectionState, predicted: float, measured: float,
    ) -> None:
        self.buffer.append((self._features(s), predicted, measured))
        self.observations += 1

    def update(self) -> None:
        if not self.buffer:
            return
        for f, pred, meas in self.buffer:
            if pred <= 0:
                continue
            err = (meas - pred) / max(pred, 1e-9)
            for i, x in enumerate(f):
                self.coeffs[i] += self.lr * err * x * 0.5
        self.updates += 1
        self.buffer.clear()


# =============================================================================
# [P3-2] Local grid profile aggregator (was "Federated" — local-only)
# =============================================================================
@dataclass
class GridProfile:
    deployment_id: str
    region: str
    mean_intensity: float
    uncertainty: float
    sample_count: int
    timestamp: float = field(default_factory=time.time)


class LocalGridProfileAggregator:
    """
    Aggregates grid-intensity profiles from multiple deployments *locally*.

    **Not federated** — no network protocol. Callers who want real federation
    should push profiles to a remote aggregator; this class is the local
    cache. Renamed from `FederatedGridAggregator` (kept as alias for compat).
    """

    def __init__(self) -> None:
        # [P2] bounded
        self.profiles: Deque[GridProfile] = deque(maxlen=100)
        self._global: Dict[str, Dict[str, float]] = {}

    def push(self, profile: GridProfile) -> None:
        self.profiles.append(profile)

    def aggregate(self) -> Dict[str, Dict[str, float]]:
        grouped: Dict[str, List[GridProfile]] = defaultdict(list)
        for p in self.profiles:
            grouped[p.region].append(p)
        result: Dict[str, Dict[str, float]] = {}
        for region, profiles in grouped.items():
            total_w = sum(p.sample_count for p in profiles) or 1
            result[region] = {
                "mean_intensity": sum(
                    p.mean_intensity * p.sample_count for p in profiles) / total_w,
                "uncertainty": sum(
                    p.uncertainty * p.sample_count for p in profiles) / total_w,
                "sample_count": float(total_w),
            }
        self._global = result
        return result

    def lookup(self, region: str) -> Optional[Dict[str, float]]:
        return self._global.get(region)


# Backward-compat alias
FederatedGridProfile = GridProfile
FederatedGridAggregator = LocalGridProfileAggregator


# =============================================================================
# [P2-3] Carbon instruments (operational ≠ contractual)
# =============================================================================
@dataclass
class CarbonInstrument:
    instrument_id: str
    kind: str             # "rec" | "offset" | "credit"
    quantity: float
    unit: str             # "MWh" | "kgCO2e"
    vintage_year: Optional[int] = None
    matching_period: Optional[str] = None
    producer: Optional[str] = None
    provenance: Dict[str, Any] = field(default_factory=dict)


class CarbonInstrumentsLedger:
    """Track contractual instruments separately. Unbounded by design —
    accounting records must never be auto-evicted."""

    def __init__(self) -> None:
        self.instruments: List[CarbonInstrument] = []

    def add(self, instrument: CarbonInstrument) -> None:
        self.instruments.append(instrument)

    def contractual_kgco2e(self) -> float:
        """Sum of offset instruments in kgCO2e (never added to operational)."""
        return sum(
            i.quantity for i in self.instruments
            if i.kind == "offset" and i.unit == "kgCO2e")

    def rec_mwh(self) -> float:
        """[P2-3] Sum of REC instruments in MWh."""
        return sum(
            i.quantity for i in self.instruments
            if i.kind == "rec" and i.unit == "MWh")

    def rec_kgco2e_avoided(self, grid_intensity_g_kwh: float) -> float:
        """[P2-3] CO₂e emissions avoided via RECs at the current grid intensity."""
        return self.rec_mwh() * grid_intensity_g_kwh  # MWh × g/kWh = kg/1000 × 1000

    def summary(self) -> Dict[str, Any]:
        kinds: Dict[str, int] = defaultdict(int)
        for i in self.instruments:
            kinds[i.kind] += 1
        return {
            "total_instruments": len(self.instruments),
            "contractual_kgco2e": self.contractual_kgco2e(),
            "rec_mwh": self.rec_mwh(),
            "by_kind": dict(kinds),
        }


# =============================================================================
# Helium companion
# =============================================================================
class HeliumCompanion:
    HELIUM_UNITS_PER_WH = 5e-5  # heuristic — no empirical basis

    @classmethod
    def estimate_units(cls, energy_wh: float,
                        scarcity_score: float = 0.0) -> float:
        base = energy_wh * cls.HELIUM_UNITS_PER_WH
        efficiency = 1.0 - min(0.5, scarcity_score * 0.5)
        return base * efficiency


# =============================================================================
# THE ENHANCED CarbonEstimator
# =============================================================================
class CarbonEstimator:
    """
    Enhanced carbon estimator.

    **Backward-compatible:** `CarbonEstimator(grid, pue).estimate(wh)` returns
    a float, identical to v1.
    """

    DEFAULT_FEATURES: Dict[str, bool] = {
        "provenance": True,
        "uncertainty": True,
        "xai": True,
        "temporal_logic": True,
        "carbon_markets": True,
        "helium_companion": True,
        "circuit_breaker": True,
        "causal_rl": True,
        "federated": True,
        "adaptive_precision": True,
        "hitl": True,
        "chaos_testing": False,
    }

    MIN_PUE: float = 1.0
    # [P1-7] raised from 3.0 → 5.0 (legitimate high-PUE facilities exist)
    MAX_PUE: float = 5.0
    HITL_CARBON_THRESHOLD_KG: float = 10.0

    def __init__(
        self,
        grid_intensity_g_kwh: float,
        pue: float,
        *,
        region: Optional[str] = None,
        grid_uncertainty: Optional[float] = None,
        agent_id: Optional[str] = None,
        deployment_id: str = "local",
        features: Optional[Dict[str, bool]] = None,
        energy_meter: Optional[EnergyMeter] = None,
    ) -> None:
        # --- v1 fields ---
        self.grid = float(grid_intensity_g_kwh)
        self.pue = float(pue)

        # --- Enhancement config ---
        self.region = region
        self.agent_id = agent_id
        self.deployment_id = deployment_id
        self.features = {**self.DEFAULT_FEATURES, **(features or {})}

        # --- Grid provenance ---
        self._grid_source: str = GridSource.STATIC.value
        self._grid_timestamp: float = time.time()
        self._grid_uncertainty: Optional[float] = grid_uncertainty

        # --- Validation ---
        if not (self.MIN_PUE <= self.pue <= self.MAX_PUE):
            logger.warning(
                "PUE=%s outside plausible range [%s, %s]; "
                "estimate may be unrealistic",
                self.pue, self.MIN_PUE, self.MAX_PUE)
        if self.grid < 0:
            logger.warning("Negative grid intensity: %s", self.grid)

        # --- [P3-4] optional injected energy meter ---
        if energy_meter is not None and not isinstance(energy_meter, EnergyMeter):
            logger.warning(
                "energy_meter does not implement "
                "EnergyMeter.measure(energy_wh_estimate=float) -> float; "
                "ignoring")
            energy_meter = None
        self.energy_meter = energy_meter

        # --- Resilience ---
        self._grid_circuit = CircuitBreaker("grid_source", failure_threshold=3)
        self.chaos = (ChaosInjector(failure_rate=0.0)
                      if self.features["chaos_testing"] else None)

        # --- Temporal ---
        self.temporal_monitor: Optional[TemporalLogicMonitor] = (
            TemporalLogicMonitor() if self.features["temporal_logic"] else None)

        # --- XAI ---
        self.explainer = CarbonExplainer() if self.features["xai"] else None

        # --- [P0-4] Causal learner ---
        self.causal_learner = (
            CausalCarbonLearner() if self.features["causal_rl"] else None)

        # --- [P3-2] Local profile aggregator ---
        self.federated = (
            LocalGridProfileAggregator() if self.features["federated"] else None)

        # --- Instruments ledger ---
        self.instruments = (
            CarbonInstrumentsLedger() if self.features["carbon_markets"] else None)

        # --- Helium ---
        self.helium_scarcity_fn: Optional[Callable[[], float]] = None

        # --- HITL ---
        self._hitl_callback: Optional[Callable[[Dict[str, Any]], bool]] = None

        # --- [P1-9] Split stats ---
        self._estimate_count: int = 0
        self._count_real: int = 0
        self._count_simulated: int = 0
        self._total_operational_kg: float = 0.0
        self._total_operational_kg_real: float = 0.0
        self._total_operational_kg_simulated: float = 0.0
        self._total_energy_wh: float = 0.0
        self._total_helium_units: float = 0.0
        self._last_estimate_grid_age: Optional[float] = None

        logger.debug(
            "CarbonEstimator initialized (grid=%s, pue=%s, region=%s)",
            self.grid, self.pue, self.region)

    # ------------------------------------------------------------------
    # [P2-8] Dunders
    # ------------------------------------------------------------------
    def __repr__(self) -> str:
        return (
            f"CarbonEstimator(grid={self.grid}, pue={self.pue}, "
            f"region={self.region!r}, source={self._grid_source!r}, "
            f"estimates={self._estimate_count}, "
            f"circuit={self._grid_circuit.state.value})"
        )

    # ------------------------------------------------------------------
    # [P2-7] Reset statistics (keeps config and instruments)
    # ------------------------------------------------------------------
    def reset_statistics(self) -> None:
        """Reset cumulative counters without touching config or state."""
        self._estimate_count = 0
        self._count_real = 0
        self._count_simulated = 0
        self._total_operational_kg = 0.0
        self._total_operational_kg_real = 0.0
        self._total_operational_kg_simulated = 0.0
        self._total_energy_wh = 0.0
        self._total_helium_units = 0.0
        self._last_estimate_grid_age = None
        if self.causal_learner is not None:
            self.causal_learner.buffer.clear()
        if self.temporal_monitor is not None:
            self.temporal_monitor.violations.clear()

    # ------------------------------------------------------------------
    # v1 public API — preserved exactly
    # ------------------------------------------------------------------
    def estimate(self, energy_wh: float) -> float:
        """
        Estimate operational carbon emissions in kgCO₂e.

        **v2.0.0:** returns `float("nan")` for invalid input (v1 returned 0.0).
        """
        est = self._estimate_internal(energy_wh, energy_is_baseline=True)
        if est is None:
            return float("nan")
        return est.carbon_operational_kg

    # ------------------------------------------------------------------
    # Enhanced public API
    # ------------------------------------------------------------------
    def estimate_detailed(
        self,
        energy_wh: float,
        *,
        precision: Optional[str] = None,
        policy_version: Optional[str] = None,
        helium_scarcity: Optional[float] = None,
        # [P0-2] explicit semantics for the input energy
        energy_is_baseline: bool = True,
        energy_provenance: Optional[str] = None,
    ) -> Optional[CarbonEstimate]:
        """
        Return a rich CarbonEstimate.

        **`energy_is_baseline`:**
          - `True` (default): `energy_wh` is the FP32-baseline energy; the
            returned `CarbonEstimate.energy_wh` is the *effective* energy at
            `precision`. v1-compatible.
          - `False`: `energy_wh` is measured energy at `precision`;
            no precision multiplier is applied.
        """
        return self._estimate_internal(
            energy_wh,
            precision=precision,
            policy_version=policy_version,
            helium_scarcity=helium_scarcity,
            energy_is_baseline=energy_is_baseline,
            energy_provenance=energy_provenance,
        )

    def estimate_batch(self, energies_wh: List[float]) -> List[float]:
        out: List[float] = []
        for e in energies_wh:
            try:
                out.append(self.estimate(e))
            except Exception:
                out.append(float("nan"))
        return out

    def estimate_batch_detailed(
        self, energies_wh: List[float], **kwargs: Any,
    ) -> List[Optional[CarbonEstimate]]:
        # [P1-2] per-item error handling (was: one bad item killed the batch)
        out: List[Optional[CarbonEstimate]] = []
        for e in energies_wh:
            try:
                out.append(self.estimate_detailed(e, **kwargs))
            except Exception as ex:
                logger.warning("batch item failed (energy_wh=%r): %s", e, ex)
                out.append(None)
        return out

    # ------------------------------------------------------------------
    # Grid intensity updates
    # ------------------------------------------------------------------
    def set_grid_intensity(
        self,
        value: float,
        source: str = GridSource.MANUAL.value,
        uncertainty: Optional[float] = None,
        region: Optional[str] = None,
    ) -> None:
        """Update grid intensity with explicit provenance."""
        # [P1-10] validate source against known GridSource values
        known = {g.value for g in GridSource}
        if source not in known:
            logger.warning(
                "Unknown grid source %r; storing as-is (known: %s)",
                source, sorted(known))
        self.grid = float(value)
        self._grid_source = source
        self._grid_timestamp = time.time()
        self._grid_uncertainty = uncertainty
        if region is not None:
            self.region = region
        logger.debug(
            "Grid intensity updated: %.2f gCO2/kWh (source=%s, unc=%s)",
            value, source, uncertainty)

    def update_from_forecaster(self, carbon_forecaster: Any) -> bool:
        """Sync update. For async forecasters use `async_update_from_forecaster`."""
        if carbon_forecaster is None:
            return False
        if not self._grid_circuit.can_call():
            logger.debug("Grid circuit open; skipping forecaster update")
            return False
        try:
            if self.chaos and self.chaos.maybe_fail("forecaster"):
                raise RuntimeError("chaos: forecaster")
            getter = getattr(carbon_forecaster, "get_current_intensity", None)
            if getter is None:
                return False
            value = getter(self.region) if self.region else getter()
            if hasattr(value, "__await__"):
                # [P1-1] explicit close to suppress "never awaited" warning
                try:
                    value.close()
                except Exception:
                    pass
                logger.debug(
                    "Forecaster returned a coroutine; use "
                    "async_update_from_forecaster()")
                return False
            self.set_grid_intensity(
                float(value), source=GridSource.FORECASTER.value)
            self._grid_circuit.record_success()
            return True
        except Exception as e:
            self._grid_circuit.record_failure()
            logger.warning("Forecaster update failed: %s", e)
            return False

    async def async_update_from_forecaster(self, carbon_forecaster: Any) -> bool:
        """[P1-1] Async update for async forecasters (ElectricityMap, WattTime)."""
        if carbon_forecaster is None:
            return False
        if not self._grid_circuit.can_call():
            logger.debug("Grid circuit open; skipping async forecaster update")
            return False
        try:
            if self.chaos and self.chaos.maybe_fail("forecaster"):
                raise RuntimeError("chaos: forecaster")
            getter = getattr(carbon_forecaster, "get_current_intensity", None)
            if getter is None:
                return False
            value = getter(self.region) if self.region else getter()
            if hasattr(value, "__await__"):
                value = await value
            self.set_grid_intensity(
                float(value), source=GridSource.FORECASTER.value)
            self._grid_circuit.record_success()
            return True
        except Exception as e:
            self._grid_circuit.record_failure()
            logger.warning("Async forecaster update failed: %s", e)
            return False

    # ------------------------------------------------------------------
    # Instruments, HITL, statistics
    # ------------------------------------------------------------------
    def add_instrument(self, instrument: CarbonInstrument) -> None:
        if self.instruments is None:
            logger.debug("Carbon markets disabled; instrument ignored")
            return
        self.instruments.add(instrument)

    def set_hitl_callback(
        self, callback: Callable[[Dict[str, Any]], bool],
    ) -> None:
        """Register a HITL review callback. Must be sync (returns bool)."""
        self._hitl_callback = callback

    def get_statistics(self) -> Dict[str, Any]:
        return {
            "deployment_id": self.deployment_id,
            "agent_id": self.agent_id,
            "region": self.region,
            "estimate_count": self._estimate_count,
            "count_real": self._count_real,
            "count_simulated": self._count_simulated,
            "total_operational_kg": self._total_operational_kg,
            "total_operational_kg_real": self._total_operational_kg_real,
            "total_operational_kg_simulated": self._total_operational_kg_simulated,
            "total_energy_wh": self._total_energy_wh,
            "total_helium_units": self._total_helium_units,
            "grid_intensity": self.grid,
            "grid_source": self._grid_source,
            "grid_age_now_seconds": time.time() - self._grid_timestamp,
            # [P2-4] snapshot at last estimate time
            "grid_age_at_last_estimate_seconds": self._last_estimate_grid_age,
            "grid_uncertainty": self._grid_uncertainty,
            "pue": self.pue,
            "circuit_state": self._grid_circuit.state.value,
            "circuit_failures": self._grid_circuit.failures,
            "causal_observations": (
                self.causal_learner.observations if self.causal_learner else 0),
            "causal_updates": (
                self.causal_learner.updates if self.causal_learner else 0),
            "instruments_summary": (
                self.instruments.summary() if self.instruments else None),
        }

    def export(self) -> Dict[str, Any]:
        return _to_jsonable({
            "config": {
                "grid_intensity_g_kwh": self.grid,
                "pue": self.pue,
                "region": self.region,
                "grid_source": self._grid_source,
                "grid_uncertainty": self._grid_uncertainty,
            },
            "statistics": self.get_statistics(),
            "instruments": (
                [asdict(i) for i in self.instruments.instruments]
                if self.instruments else []),
        })

    # ------------------------------------------------------------------
    # Causal correction (ground truth)
    # ------------------------------------------------------------------
    def record_measured(
        self, predicted_kg: float, measured_kg: float,
    ) -> None:
        """
        Feed back (predicted, measured) for residual correction learning.

        Call this after ground-truth carbon measurement is available (e.g.
        from utility bills or certified audits).
        """
        if not self.causal_learner:
            return
        state = CausalCorrectionState(
            hour_of_day=datetime.now().hour,
            grid_source_hash=_stable_hash_float(self._grid_source),  # [P0-1]
            recent_energy_norm=min(1.0, self._total_energy_wh / 1000.0),
        )
        self.causal_learner.record(state, predicted_kg, measured_kg)
        if self.causal_learner.observations % 10 == 0:
            self.causal_learner.update()

    def contribute_federated(self) -> None:
        """Contribute this deployment's grid profile to the local aggregator."""
        if not self.federated or not self.region:
            return
        self.federated.push(GridProfile(
            deployment_id=self.deployment_id,
            region=self.region,
            mean_intensity=self.grid,
            uncertainty=self._grid_uncertainty or 0.0,
            sample_count=max(1, self._estimate_count),
        ))

    # ------------------------------------------------------------------
    # INTERNAL pipeline
    # ------------------------------------------------------------------
    def _estimate_internal(
        self,
        energy_wh: float,
        *,
        precision: Optional[str] = None,
        policy_version: Optional[str] = None,
        helium_scarcity: Optional[float] = None,
        energy_is_baseline: bool = True,
        energy_provenance: Optional[str] = None,
    ) -> Optional[CarbonEstimate]:
        # --- Input validation ---
        energy_wh_f = self._safe_float(energy_wh)
        if energy_wh_f is None:
            logger.warning("Invalid energy_wh=%r; returning NaN", energy_wh)
            return None
        if energy_wh_f < 0:
            logger.warning("Negative energy_wh=%s; clamped to 0.0", energy_wh_f)
            energy_wh_f = 0.0

        # --- [P1-10] Precision validation ---
        if precision is not None and not PrecisionEnergyMultiplier.is_known(precision):
            logger.warning(
                "Unknown precision %r; known levels: %s",
                precision, PrecisionEnergyMultiplier.known_levels())
            precision = None

        # --- [P0-2] Precision multiplier only if input is baseline ---
        precision_mult = 1.0
        if (self.features["adaptive_precision"]
                and precision is not None
                and energy_is_baseline):
            precision_mult = PrecisionEnergyMultiplier.get(precision)
        adjusted_energy_wh = energy_wh_f * precision_mult

        # --- [P3-4] Optional injected energy meter ---
        if self.energy_meter is not None:
            try:
                metered = float(self.energy_meter.measure(
                    energy_wh_estimate=adjusted_energy_wh))
                if metered > 0:
                    adjusted_energy_wh = metered
            except Exception as e:
                logger.warning("Energy meter failed (%s); using estimate", e)

        # --- Core formula ---
        kwh = adjusted_energy_wh / 1000.0
        carbon_kg = (kwh * self.grid * self.pue) / 1000.0

        # --- [P0-4] Causal correction (bounded ±20%) ---
        causal_adjustment = 0.0
        if self.causal_learner:
            state = CausalCorrectionState(
                hour_of_day=datetime.now().hour,
                grid_source_hash=_stable_hash_float(self._grid_source),
                recent_energy_norm=min(1.0, adjusted_energy_wh / 1000.0),
            )
            causal_adjustment = self.causal_learner.predict_correction(state)
            carbon_kg = max(0.0, carbon_kg * (1.0 + causal_adjustment))

        # --- Uncertainty ---
        uncertainty_kg = 0.0
        if self.features["uncertainty"] and self._grid_uncertainty is not None:
            uncertainty_kg = kwh * self._grid_uncertainty * self.pue / 1000.0

        # --- [P2-3] Helium companion ---
        helium_units = 0.0
        if self.features["helium_companion"]:
            scarcity = helium_scarcity
            if scarcity is None and self.helium_scarcity_fn is not None:
                try:
                    scarcity = float(self.helium_scarcity_fn())
                except Exception:
                    scarcity = 0.0
            helium_units = HeliumCompanion.estimate_units(
                adjusted_energy_wh, scarcity or 0.0)

        # --- Contractual instruments ---
        contractual_kg = 0.0
        rec_mwh = 0.0
        rec_kgco2e_avoided = 0.0
        instrument_prov: Optional[Dict[str, Any]] = None
        if self.instruments is not None:
            contractual_kg = self.instruments.contractual_kgco2e()
            rec_mwh = self.instruments.rec_mwh()
            rec_kgco2e_avoided = self.instruments.rec_kgco2e_avoided(self.grid)
            instrument_prov = self.instruments.summary()

        # --- [P1-4] Temporal verification ---
        grid_age = time.time() - self._grid_timestamp
        temporal_ok = True
        temporal_violations: List[str] = []
        if self.temporal_monitor:
            reading = GridIntensityReading(
                value=self.grid, source=self._grid_source,
                timestamp=self._grid_timestamp,
                uncertainty=self._grid_uncertainty, region=self.region)
            temporal_ok, temporal_violations = self.temporal_monitor.verify(reading)
        grid_stale = not temporal_ok

        # --- [P1-3] HITL ---
        needs_review = False
        review_reason: Optional[str] = None
        hitl_verdict: Optional[str] = None
        if self.features["hitl"] and carbon_kg >= self.HITL_CARBON_THRESHOLD_KG:
            needs_review = True
            review_reason = (
                f"carbon estimate {carbon_kg:.4f} kg >= "
                f"threshold {self.HITL_CARBON_THRESHOLD_KG} kg")
            if self._hitl_callback is not None:
                try:
                    approved = self._hitl_callback({
                        "reason": review_reason,
                        "carbon_kg": carbon_kg,
                        "energy_wh": energy_wh_f,
                        "agent_id": self.agent_id,
                    })
                    if hasattr(approved, "__await__"):
                        # Async callback — can't await in sync context
                        try: approved.close()
                        except Exception: pass
                        logger.warning(
                            "HITL callback returned coroutine; sync context "
                            "cannot await — treating as pending")
                        hitl_verdict = "pending"
                    elif approved is False:
                        hitl_verdict = "denied"
                        logger.info("HITL denied estimate; keeping value flagged")
                    elif approved is True:
                        hitl_verdict = "approved"
                        needs_review = False
                        review_reason = None
                except Exception as e:
                    logger.warning("HITL callback failed: %s", e)
                    hitl_verdict = "error"
            else:
                hitl_verdict = "pending"

        # --- [P0-3] Per-field provenance ---
        grid_prov = _GRID_PROVENANCE.get(self._grid_source,
                                          Provenance.ESTIMATED.value)
        if energy_provenance is not None:
            energy_prov = energy_provenance
        elif self.energy_meter is not None:
            energy_prov = Provenance.MEASURED.value
        else:
            energy_prov = Provenance.ESTIMATED.value
        contractual_prov = (
            Provenance.MEASURED.value if self.instruments is not None
            and len(self.instruments.instruments) > 0
            else Provenance.ESTIMATED.value)

        # --- Build estimate ---
        estimate = CarbonEstimate(
            energy_wh=adjusted_energy_wh,
            grid_intensity_g_kwh=self.grid,
            pue=self.pue,
            carbon_operational_kg=carbon_kg,
            energy_wh_input=energy_wh_f,
            energy_is_baseline=energy_is_baseline,
            grid_source=self._grid_source,
            grid_age_seconds=grid_age,
            grid_uncertainty=self._grid_uncertainty,
            region=self.region,
            energy_provenance=energy_prov,
            grid_provenance=grid_prov,
            contractual_provenance=contractual_prov,
            uncertainty_kg=uncertainty_kg,
            precision=precision,
            agent_id=self.agent_id,
            policy_version=policy_version,
            carbon_contractual_kg=contractual_kg,
            rec_mwh=rec_mwh,
            rec_kgco2e_avoided=rec_kgco2e_avoided,
            instrument_provenance=instrument_prov,
            helium_units=helium_units,
            needs_review=needs_review,
            review_reason=review_reason,
            hitl_verdict=hitl_verdict,
            grid_stale=grid_stale,
            grid_violations=temporal_violations,
        )

        # --- XAI ---
        if self.explainer:
            estimate.explanation = CarbonExplainer.explain(estimate)

        # --- [P1-9] Statistics (real vs simulated) ---
        self._estimate_count += 1
        self._total_operational_kg += carbon_kg
        self._total_energy_wh += adjusted_energy_wh
        self._total_helium_units += helium_units
        self._last_estimate_grid_age = grid_age

        if estimate.simulated:
            self._count_simulated += 1
            self._total_operational_kg_simulated += carbon_kg
        else:
            self._count_real += 1
            self._total_operational_kg_real += carbon_kg

        return estimate

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _safe_float(value: Any) -> Optional[float]:
        try:
            v = float(value)
            return v if math.isfinite(v) else None
        except (TypeError, ValueError):
            return None


# =============================================================================
# [P3-1] Bridge to DecisionRecord
# =============================================================================
def carbon_estimate_to_decision(
    estimate: CarbonEstimate,
    *,
    run_id: str,
    task_id: str,
    policy_version: str = "",
    hardware_profile: str = "",
    **extra: Any,
) -> Optional[Any]:
    """
    Bridge a CarbonEstimate to a DecisionRecord (analysis-layer contract).

    Returns `None` if DecisionRecord cannot be imported.
    """
    try:
        from src.analysis import DecisionRecord  # type: ignore
    except ImportError:
        try:
            from analysis import DecisionRecord  # type: ignore
        except ImportError:
            return None
    try:
        return DecisionRecord(
            run_id=run_id,
            timestamp=estimate.at,
            task_id=task_id,
            selected_action="carbon_estimate",
            alternatives=[],
            policy_version=policy_version,
            model_or_agent=estimate.agent_id or "carbon_estimator",
            hardware_profile=hardware_profile,
            precision=estimate.precision,
            latency_ms=0.0,
            energy_kwh=estimate.energy_wh / 1000.0,
            carbon_operational_kg=estimate.carbon_operational_kg,
            carbon_contractual_kg=estimate.carbon_contractual_kg,
            helium_units=estimate.helium_units,
            uncertainty=estimate.uncertainty_kg,
            explanation=estimate.explanation or {},
            provenance={
                "source": "carbon_estimator",
                "grid_source": estimate.grid_source,
                "grid_provenance": estimate.grid_provenance,
                "energy_provenance": estimate.energy_provenance,
                **extra,
            },
        )
    except Exception as e:
        logger.warning("carbon_estimate_to_decision failed: %s", e)
        return None


# =============================================================================
# Self test
# =============================================================================
def _self_test() -> int:
    """Return 0 if all assertions pass, else count of failures."""
    logging.basicConfig(level=logging.WARNING)
    failures = 0

    def _check(name: str, fn) -> None:
        nonlocal failures
        try:
            fn()
            print(f"  ✓ {name}")
        except AssertionError as e:
            failures += 1
            print(f"  ✗ {name}: {e}")
        except Exception as e:
            failures += 1
            print(f"  ✗ {name}: {type(e).__name__}: {e}")

    print("=" * 72)
    print("Carbon Estimator v2.0.0 — self test")
    print("=" * 72)

    # 1. Backward compat
    def _t_backward() -> None:
        est = CarbonEstimator(grid_intensity_g_kwh=385.0, pue=1.2)
        val = est.estimate(1000.0)
        # (1000 / 1000) * 385 * 1.2 / 1000 = 0.462
        assert abs(val - 0.462) < 1e-9, val
        assert isinstance(val, float)
    _check("v1 estimate() returns float 0.462", _t_backward)

    # 2. [P0-1] Stable hash
    def _t_stable_hash() -> None:
        h1 = _stable_hash_float("electricitymap")
        h2 = _stable_hash_float("electricitymap")
        h3 = _stable_hash_float("eia")
        assert h1 == h2, "hash must be deterministic"
        assert h1 != h3, "different inputs must hash differently"
        assert 0.0 <= h1 <= 1.0
    _check("[P0-1] Stable hash across calls", _t_stable_hash)

    # 3. [P0-2] energy_wh_input vs energy_wh (baseline default)
    def _t_energy_semantics() -> None:
        est = CarbonEstimator(385.0, 1.2)
        rec = est.estimate_detailed(1500.0, precision="int8")
        # Default: energy_is_baseline=True → multiplier 0.5 applied
        assert abs(rec.energy_wh_input - 1500.0) < 1e-9
        assert abs(rec.energy_wh - 750.0) < 1e-9
        assert rec.energy_is_baseline is True
        # Opt-out: energy_is_baseline=False → no multiplier
        rec2 = est.estimate_detailed(1500.0, precision="int8",
                                        energy_is_baseline=False)
        assert abs(rec2.energy_wh - 1500.0) < 1e-9
        assert rec2.energy_is_baseline is False
    _check("[P0-2] energy_is_baseline controls precision", _t_energy_semantics)

    # 4. [P0-3] Per-field provenance
    def _t_provenance() -> None:
        est = CarbonEstimator(385.0, 1.2)
        rec = est.estimate_detailed(1000.0)
        assert rec.grid_provenance == "static"
        assert rec.energy_provenance == "estimated"
        assert rec.simulated is True
        assert rec.legacy_simulated is True  # static + no uncertainty
        est.set_grid_intensity(250.0, source="electricitymap")
        rec2 = est.estimate_detailed(1000.0)
        assert rec2.grid_provenance == "measured"
        assert rec2.simulated is False
        assert rec2.legacy_simulated is False
    _check("[P0-3] Per-field provenance + simulated property", _t_provenance)

    # 5. [P0-4] Causal learner updates
    def _t_causal() -> None:
        est = CarbonEstimator(385.0, 1.2)
        assert est.causal_learner is not None
        # Before training, correction is 0
        c0 = est.causal_learner.predict_correction(CausalCorrectionState(
            hour_of_day=12, grid_source_hash=0.5, recent_energy_norm=0.5))
        assert c0 == 0.0, c0
        # Feed 20 observed pairs
        for _ in range(20):
            est.record_measured(predicted_kg=1.0, measured_kg=1.3)
        assert est.causal_learner.updates >= 1
        assert any(abs(c) > 1e-6 for c in est.causal_learner.coeffs)
    _check("[P0-4] Causal learner trains via record_measured", _t_causal)

    # 6. [P1-1] Async forecaster path
    def _t_async_forecaster() -> None:
        import asyncio
        class AsyncForecaster:
            async def get_current_intensity(self, region=None):
                return 220.0
        est = CarbonEstimator(385.0, 1.2)
        ok = asyncio.run(est.async_update_from_forecaster(AsyncForecaster()))
        assert ok is True, ok
        assert abs(est.grid - 220.0) < 1e-9
        assert est._grid_source == "forecaster"
    _check("[P1-1] async_update_from_forecaster", _t_async_forecaster)

    # 7. [P1-2] estimate_batch_detailed handles errors
    def _t_batch_detailed() -> None:
        est = CarbonEstimator(385.0, 1.2)
        out = est.estimate_batch_detailed([1000.0, None, 2000.0])
        assert len(out) == 3
        assert out[0] is not None
        assert out[1] is None  # None input → None result
        assert out[2] is not None
    _check("[P1-2] estimate_batch_detailed error handling", _t_batch_detailed)

    # 8. [P1-3] HITL verdict tracking
    def _t_hitl() -> None:
        est = CarbonEstimator(385.0, 1.2)
        calls = []
        def _deny(payload):
            calls.append(payload)
            return False
        est.set_hitl_callback(_deny)
        # Energy that produces > 10 kg: kwh * grid * pue / 1000 > 10
        # 100,000 Wh = 100 kWh → 100 * 385 * 1.2 / 1000 = 46.2 kg
        rec = est.estimate_detailed(100_000.0)
        assert rec.needs_review is True
        assert rec.hitl_verdict == "denied", rec.hitl_verdict
        assert len(calls) == 1
    _check("[P1-3] HITL verdict recorded", _t_hitl)

    # 9. [P1-4] Stale grid surfaced
    def _t_stale() -> None:
        est = CarbonEstimator(385.0, 1.2)
        # Fake an old timestamp
        est._grid_timestamp = time.time() - 4000.0
        rec = est.estimate_detailed(1000.0)
        assert rec.grid_stale is True
        assert "GridIntensityFresh" in (rec.grid_violations or [])
    _check("[P1-4] Stale grid flag", _t_stale)

    # 10. [P1-5] Circuit breaker HALF_OPEN limiter
    def _t_circuit() -> None:
        cb = CircuitBreaker("test", failure_threshold=2,
                             recovery_timeout_seconds=0.1,
                             half_open_max_calls=1)
        cb.record_failure(); cb.record_failure()
        assert cb.state == CircuitState.OPEN
        time.sleep(0.15)
        assert cb.can_call() is True   # enters HALF_OPEN, uses 1 call
        assert cb.state == CircuitState.HALF_OPEN
        assert cb.can_call() is False  # max_calls reached
        cb.record_success()
        assert cb.state == CircuitState.CLOSED
    _check("[P1-5] CircuitBreaker HALF_OPEN limiter", _t_circuit)

    # 11. [P1-7] MAX_PUE = 5.0
    def _t_pue() -> None:
        # PUE=4.5 should NOT warn (was 3.0 max in v1)
        est = CarbonEstimator(385.0, 4.5)
        assert est.pue == 4.5
        val = est.estimate(1000.0)
        assert abs(val - (1.0 * 385.0 * 4.5 / 1000.0)) < 1e-9
    _check("[P1-7] PUE=4.5 accepted", _t_pue)

    # 12. [P1-8] estimate(nan) returns nan
    def _t_nan() -> None:
        est = CarbonEstimator(385.0, 1.2)
        import math
        assert math.isnan(est.estimate(float("nan")))
        assert math.isnan(est.estimate(None))
        assert est.estimate(-100.0) == 0.0  # negative clamped to 0
    _check("[P1-8] Invalid input returns nan", _t_nan)

    # 13. [P1-9] Split real/simulated stats
    def _t_stats_split() -> None:
        est = CarbonEstimator(385.0, 1.2)
        est.estimate_detailed(1000.0)  # simulated (static grid)
        est.set_grid_intensity(250.0, source="electricitymap")
        est.estimate_detailed(1000.0)  # real (measured grid)
        stats = est.get_statistics()
        assert stats["count_real"] == 1
        assert stats["count_simulated"] == 1
        assert stats["total_operational_kg_real"] > 0
        assert stats["total_operational_kg_simulated"] > 0
    _check("[P1-9] Separate real/simulated stats", _t_stats_split)

    # 14. [P2-3] REC kg-equivalent
    def _t_rec() -> None:
        est = CarbonEstimator(385.0, 1.2)
        est.add_instrument(CarbonInstrument(
            instrument_id="rec-1", kind="rec", quantity=5.0, unit="MWh"))
        rec = est.estimate_detailed(1000.0)
        assert rec.rec_mwh == 5.0
        # 5 MWh × 385 g/kWh = 5 * 385 = 1925 kg avoided
        assert abs(rec.rec_kgco2e_avoided - 1925.0) < 1e-6, rec.rec_kgco2e_avoided
        # Contractual (offset) still 0 since kind=rec
        assert rec.carbon_contractual_kg == 0.0
    _check("[P2-3] REC kg-equivalent computed", _t_rec)

    # 15. [P2-7] reset_statistics
    def _t_reset_stats() -> None:
        est = CarbonEstimator(385.0, 1.2)
        for _ in range(5):
            est.estimate(1000.0)
        assert est._estimate_count == 5
        est.reset_statistics()
        assert est._estimate_count == 0
        assert est._total_operational_kg == 0.0
    _check("[P2-7] reset_statistics", _t_reset_stats)

    # 16. [P2-8] __repr__
    def _t_repr() -> None:
        est = CarbonEstimator(385.0, 1.2, region="US-CA")
        r = repr(est)
        assert "grid=385.0" in r
        assert "US-CA" in r
    _check("[P2-8] __repr__", _t_repr)

    # 17. [P1-10] Unknown precision warning + no multiplier
    def _t_unknown_precision() -> None:
        est = CarbonEstimator(385.0, 1.2)
        rec = est.estimate_detailed(1000.0, precision="int999")
        # Unknown precision → precision set to None → multiplier 1.0
        assert rec.precision is None
        assert abs(rec.energy_wh - 1000.0) < 1e-9
    _check("[P1-10] Unknown precision rejected", _t_unknown_precision)

    # 18. [P2-6] to_dict is JSON-safe
    def _t_json_safe() -> None:
        import json
        est = CarbonEstimator(385.0, 1.2)
        rec = est.estimate_detailed(1000.0, precision="int8")
        d = rec.to_dict()
        json.dumps(d)  # must not raise
        assert "at" in d and isinstance(d["at"], str)
    _check("[P2-6] to_dict JSON-serializable", _t_json_safe)

    # 19. [P3-1] carbon_estimate_to_decision bridge
    def _t_bridge() -> None:
        est = CarbonEstimator(385.0, 1.2)
        rec = est.estimate_detailed(1000.0)
        # Will return None if DecisionRecord can't be imported in this env
        result = carbon_estimate_to_decision(
            rec, run_id="r1", task_id="t1")
        # Either returns a DecisionRecord or None (both OK)
        if result is not None:
            assert getattr(result, "run_id") == "r1"
    _check("[P3-1] carbon_estimate_to_decision bridge", _t_bridge)

    # 20. [P1-6] Contribute to local aggregator
    def _t_local_aggregator() -> None:
        est = CarbonEstimator(385.0, 1.2, region="US-CA")
        for _ in range(3):
            est.estimate(1000.0)
        est.contribute_federated()
        assert len(est.federated.profiles) == 1
        agg = est.federated.aggregate()
        assert "US-CA" in agg
        assert abs(agg["US-CA"]["mean_intensity"] - 385.0) < 1e-9
    _check("[P1-6] Local profile aggregation", _t_local_aggregator)

    print("=" * 72)
    if failures == 0:
        print("✅ All tests passed")
    else:
        print(f"❌ {failures} test(s) failed")
    print("=" * 72)
    return failures


# =============================================================================
# Demo
# =============================================================================
def _demo():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    print("=" * 72)
    print("Carbon Estimator v2.0.0 — demo")
    print("=" * 72)

    # Original behavior
    est = CarbonEstimator(grid_intensity_g_kwh=385.0, pue=1.2)
    print(f"Original estimate(1000 Wh): {est.estimate(1000.0)}")

    # Enhanced
    est2 = CarbonEstimator(
        grid_intensity_g_kwh=385.0, pue=1.2,
        region="US-CA", grid_uncertainty=25.0,
        agent_id="estimator-A", deployment_id="us-ca-prod-01",
    )

    print("\n=== Rich estimate (precision=int8, baseline energy) ===")
    rec = est2.estimate_detailed(
        energy_wh=1500.0, precision="int8", policy_version="v5.0.1")
    print(f"  Input energy:         {rec.energy_wh_input:.4f} Wh (baseline)")
    print(f"  Effective energy:     {rec.energy_wh:.4f} Wh (at int8)")
    print(f"  Operational carbon:   {rec.carbon_operational_kg:.6f} kgCO2e")
    print(f"  Uncertainty:          ±{rec.uncertainty_kg:.6f} kg")
    print(f"  Helium companion:     {rec.helium_units:.8f} units")
    print(f"  Energy provenance:    {rec.energy_provenance}")
    print(f"  Grid provenance:      {rec.grid_provenance}")
    print(f"  Simulated (computed): {rec.simulated}")

    print("\n=== Rich estimate (measured energy) ===")
    rec_measured = est2.estimate_detailed(
        energy_wh=1500.0, precision="int8", energy_is_baseline=False)
    print(f"  Input energy:         {rec_measured.energy_wh_input:.4f} Wh")
    print(f"  Effective energy:     {rec_measured.energy_wh:.4f} Wh (no mult)")
    print(f"  Operational carbon:   {rec_measured.carbon_operational_kg:.6f} kgCO2e")

    if rec.explanation:
        print("\n=== XAI ===")
        print(f"  {rec.explanation['headline']}")
        for r in rec.explanation["rationale"][:5]:
            print(f"    • {r}")

    # REC
    est2.add_instrument(CarbonInstrument(
        instrument_id="rec-2026-001", kind="rec", quantity=5.0, unit="MWh",
        vintage_year=2026, matching_period="2026-03",
        producer="Pacific Wind Co."))
    rec2 = est2.estimate_detailed(energy_wh=2000.0)
    print(f"\n=== Instruments ===")
    print(f"  Operational:          {rec2.carbon_operational_kg:.6f} kg")
    print(f"  RECs:                 {rec2.rec_mwh:.4f} MWh "
          f"→ {rec2.rec_kgco2e_avoided:.4f} kg CO2e avoided")
    print(f"  Offset kg:            {rec2.carbon_contractual_kg:.6f} kg")
    print(f"  (never summed with operational)")

    # Async forecaster
    print("\n=== Async forecaster update ===")
    import asyncio
    class _AsyncForecaster:
        async def get_current_intensity(self, region=None):
            return 220.0
    ok = asyncio.run(est2.async_update_from_forecaster(_AsyncForecaster()))
    print(f"  Updated: {ok}, grid now {est2.grid} gCO2/kWh")

    # Batch
    print(f"\n=== Batch ===")
    print(f"  Batch: {est2.estimate_batch([1000.0, 2000.0, 3000.0])}")
    print(f"  Detailed: {len(est2.estimate_batch_detailed([1000.0, 2000.0]))} records")

    # Statistics
    import json
    print(f"\n=== Statistics ===")
    print(json.dumps(est2.get_statistics(), indent=2, default=str))
    print(f"\n{est2!r}")


if __name__ == "__main__":
    import sys
    if os.environ.get("CARBON_ESTIMATOR_DEMO") == "1" or (
            "demo" in sys.argv[1:]):
        _demo()
        sys.exit(0)
    sys.exit(_self_test())
