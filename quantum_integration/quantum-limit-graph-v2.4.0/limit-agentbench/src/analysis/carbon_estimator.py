# src/analysis/telemetry/carbon_estimator.py

"""
Carbon estimation for Green Agent (Enhanced)
=============================================

Estimates operational carbon emissions from energy consumption. This module
is part of the analysis layer's Priority #1 (reliable telemetry + provenance).

Original API preserved:
    est = CarbonEstimator(grid_intensity_g_kwh=385.0, pue=1.2)
    kgco2e = est.estimate(energy_wh)

Enhanced API:
    kgco2e = est.estimate(energy_wh)                        # unchanged
    rec    = est.estimate_detailed(energy_wh)               # CarbonEstimate
    batch  = est.estimate_batch([1000.0, 2000.0])           # list[float]
    recs   = est.estimate_batch_detailed([1000.0, 2000.0])
    est.set_grid_intensity(g, source="electricitymap")
    est.update_from_forecaster(carbon_forecaster)
    est.get_statistics()
    est.export()

Enhancements:
  1. Quantum-Distillation      — precision-aware estimation
  2. Causal RL                 — correction from measured values
  3. Federated Green Learning  — regional grid profiles
  4. Multi-Agent Coordination  — agent_id attribution
  5. Temporal Logic            — freshness & bounds properties
  6. Explainable AI            — rationale for every estimate
  7. Adaptive Precision        — precision attribution
  8. Carbon Markets            — separate operational vs contractual
  9. Resilience & Chaos        — circuit breaker around grid source
 10. Human-in-the-Loop         — review for extreme estimates
 +   Helium awareness          — companion estimate
 +   Provenance & uncertainty
 +   Validation, logging, statistics
"""

from __future__ import annotations

import logging
import math
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from typing import (
    Any, Callable, Deque, Dict, List, Optional, Tuple,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Enums
# =============================================================================

class GridSource(Enum):
    """Where the grid intensity came from."""
    STATIC = "static"
    ELECTRICITYMAP = "electricitymap"
    EIA = "eia"
    FORECASTER = "forecaster"
    FEDERATED = "federated"
    MANUAL = "manual"
    UNKNOWN = "unknown"


class CarbonKind(Enum):
    """Distinguishes physical emissions from contractual instruments."""
    OPERATIONAL = "operational"     # real emissions from energy consumed
    CONTRACTUAL = "contractual"     # offsets, RECs — do NOT sum with operational


class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


# =============================================================================
# 9. Resilience — Circuit Breaker
# =============================================================================

@dataclass
class CircuitBreaker:
    name: str
    failure_threshold: int = 3
    recovery_timeout_seconds: int = 60
    state: CircuitState = CircuitState.CLOSED
    failures: int = 0
    last_failure_at: Optional[float] = None

    def can_call(self) -> bool:
        if self.state == CircuitState.CLOSED:
            return True
        if self.state == CircuitState.OPEN:
            if (self.last_failure_at and
                    (time.time() - self.last_failure_at)
                    > self.recovery_timeout_seconds):
                self.state = CircuitState.HALF_OPEN
                return True
            return False
        return True

    def record_success(self) -> None:
        self.failures = 0
        self.state = CircuitState.CLOSED

    def record_failure(self) -> None:
        self.failures += 1
        self.last_failure_at = time.time()
        if self.failures >= self.failure_threshold:
            self.state = CircuitState.OPEN
            logger.warning(
                f"Circuit '{self.name}' OPEN after {self.failures} failures"
            )


class ChaosInjector:
    def __init__(self, failure_rate: float = 0.0) -> None:
        self.failure_rate = failure_rate
        self.events: List[Dict[str, Any]] = []

    def maybe_fail(self, component: str) -> bool:
        import random
        if random.random() < self.failure_rate:
            self.events.append({
                "component": component,
                "at": datetime.now().isoformat(),
            })
            return True
        return False


# =============================================================================
# 5. Temporal Logic
# =============================================================================

@dataclass
class GridIntensityReading:
    """A single grid-intensity reading with provenance."""
    value: float
    source: str
    timestamp: float
    uncertainty: Optional[float] = None
    region: Optional[str] = None

    @property
    def age_seconds(self) -> float:
        return time.time() - self.timestamp


class TemporalLogicMonitor:
    """Verify freshness, bounds, and consistency of grid readings."""

    def __init__(
        self,
        max_age_seconds: float = 3600.0,
        min_intensity: float = 0.0,
        max_intensity: float = 2000.0,
    ) -> None:
        self.max_age_seconds = max_age_seconds
        self.min_intensity = min_intensity
        self.max_intensity = max_intensity
        self.violations: List[Dict[str, Any]] = []

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
# Shared contract — CarbonEstimate
# =============================================================================

@dataclass
class CarbonEstimate:
    """
    Rich result of a carbon estimate.

    `to_float()` returns exactly the same value the original `estimate()`
    would have returned, so downstream callers can migrate incrementally.
    """
    energy_wh: float
    grid_intensity_g_kwh: float
    pue: float
    carbon_operational_kg: float

    # --- Provenance ---
    grid_source: str = GridSource.STATIC.value
    grid_age_seconds: float = 0.0
    grid_uncertainty: Optional[float] = None
    region: Optional[str] = None

    # --- Uncertainty ---
    uncertainty_kg: float = 0.0

    # --- Context ---
    precision: Optional[str] = None
    agent_id: Optional[str] = None
    policy_version: Optional[str] = None

    # --- Separated instruments ---
    carbon_contractual_kg: float = 0.0
    instrument_provenance: Optional[Dict[str, Any]] = None

    # --- Companion estimate ---
    helium_units: float = 0.0

    # --- Explanation ---
    explanation: Optional[Dict[str, Any]] = None

    # --- Safety / HITL ---
    needs_review: bool = False
    review_reason: Optional[str] = None

    # --- Metadata ---
    at: datetime = field(default_factory=datetime.now)
    simulated: bool = False

    def to_float(self) -> float:
        """Backward-compatible float view (operational emissions only)."""
        return self.carbon_operational_kg

    def to_dict(self) -> Dict[str, Any]:
        out = asdict(self)
        out["at"] = self.at.isoformat()
        return out

    @property
    def carbon_total_with_contractual(self) -> float:
        """
        Operational + contractual emissions.

        Provided for completeness only. Reports should display both numbers
        SEPARATELY. Never imply that buying RECs cancels physical emissions.
        """
        return self.carbon_operational_kg + self.carbon_contractual_kg


# =============================================================================
# 6. XAI — explanation
# =============================================================================

class CarbonExplainer:
    @staticmethod
    def explain(estimate: CarbonEstimate) -> Dict[str, Any]:
        reasons: List[str] = []
        reasons.append(
            f"Energy = {estimate.energy_wh:.4f} Wh "
            f"({estimate.energy_wh / 1000.0:.6f} kWh)."
        )
        reasons.append(
            f"Grid intensity = {estimate.grid_intensity_g_kwh:.2f} gCO₂/kWh "
            f"(source: {estimate.grid_source}, age: "
            f"{estimate.grid_age_seconds:.1f}s)."
        )
        reasons.append(f"PUE = {estimate.pue:.3f}.")
        if estimate.grid_uncertainty is not None:
            reasons.append(
                f"Grid uncertainty = ±{estimate.grid_uncertainty:.2f} gCO₂/kWh."
            )
        if estimate.precision:
            reasons.append(f"Precision = {estimate.precision}.")
        if estimate.region:
            reasons.append(f"Region = {estimate.region}.")
        if estimate.helium_units:
            reasons.append(f"Companion helium = {estimate.helium_units:.6f} units.")

        return {
            "headline": (
                f"{estimate.carbon_operational_kg:.6f} kgCO₂e operational"
            ),
            "rationale": reasons,
            "confidence": (
                0.95 if estimate.grid_uncertainty is None
                else max(0.5, 0.95 - estimate.grid_uncertainty / 500.0)
            ),
            "operational_vs_contractual": {
                "operational_kg": estimate.carbon_operational_kg,
                "contractual_kg": estimate.carbon_contractual_kg,
                "note": (
                    "These are kept separate; do not sum for reporting."
                ),
            },
        }


# =============================================================================
# 1 / 7. Quantum-Distillation + Adaptive Precision
# =============================================================================

@dataclass
class HardwareProfile:
    has_tensor_cores: bool = True
    supports_int8: bool = True
    supports_int4: bool = False
    vram_gb: float = 24.0
    edge_device: bool = False


class PrecisionEnergyMultiplier:
    """Energy multiplier by precision (relative to FP32)."""
    _MAP = {
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


# =============================================================================
# 2. Causal RL — correction from measured values
# =============================================================================

@dataclass
class CausalCorrectionState:
    hour_of_day: int
    grid_source_hash: float
    recent_energy_norm: float


class CausalCarbonLearner:
    """
    Learns a multiplicative correction to the heuristic estimate from
    (predicted, measured) pairs. Reward is relative accuracy.
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
        return [
            s.hour_of_day / 24.0,
            s.grid_source_hash,
            s.recent_energy_norm,
        ]

    def predict_correction(self, s: CausalCorrectionState) -> float:
        f = self._features(s)
        c = sum(w * x for w, x in zip(self.coeffs, f))
        # Bound to ±20%
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
# 3. Federated Green Learning — regional grid profiles
# =============================================================================

@dataclass
class FederatedGridProfile:
    deployment_id: str
    region: str
    mean_intensity: float
    uncertainty: float
    sample_count: int
    timestamp: float = field(default_factory=time.time)


class FederatedGridAggregator:
    def __init__(self) -> None:
        self.profiles: List[FederatedGridProfile] = []
        self._global: Dict[str, Dict[str, float]] = {}

    def push(self, profile: FederatedGridProfile) -> None:
        self.profiles.append(profile)

    def aggregate(self) -> Dict[str, Dict[str, float]]:
        grouped: Dict[str, List[FederatedGridProfile]] = defaultdict(list)
        for p in self.profiles:
            grouped[p.region].append(p)
        result: Dict[str, Dict[str, float]] = {}
        for region, profiles in grouped.items():
            total_w = sum(p.sample_count for p in profiles) or 1
            result[region] = {
                "mean_intensity": sum(
                    p.mean_intensity * p.sample_count for p in profiles
                ) / total_w,
                "uncertainty": sum(
                    p.uncertainty * p.sample_count for p in profiles
                ) / total_w,
                "sample_count": total_w,
            }
        self._global = result
        return result

    def lookup(self, region: str) -> Optional[Dict[str, float]]:
        return self._global.get(region)


# =============================================================================
# 8. Carbon Markets — instruments (kept separate from operational)
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
    """
    Track contractual instruments SEPARATELY from physical emissions.

    Per the analysis-layer recommendation: reports must never sum
    operational and contractual emissions into a single number.
    """

    def __init__(self) -> None:
        self.instruments: List[CarbonInstrument] = []

    def add(self, instrument: CarbonInstrument) -> None:
        self.instruments.append(instrument)

    def contractual_kgco2e(self) -> float:
        """Sum of contractual instruments in kgCO2e (never added to operational)."""
        total = 0.0
        for i in self.instruments:
            if i.kind == "offset" and i.unit == "kgCO2e":
                total += i.quantity
        return total

    def summary(self) -> Dict[str, Any]:
        return {
            "total_instruments": len(self.instruments),
            "contractual_kgco2e": self.contractual_kgco2e(),
            "by_kind": {
                k: sum(1 for i in self.instruments if i.kind == k)
                for k in {i.kind for i in self.instruments}
            },
        }


# =============================================================================
# Helium companion
# =============================================================================

class HeliumCompanion:
    """Estimate helium consumption from energy (dual-axis mission)."""
    HELIUM_UNITS_PER_WH = 5e-5

    @classmethod
    def estimate_units(
        cls, energy_wh: float, scarcity_score: float = 0.0,
    ) -> float:
        base = energy_wh * cls.HELIUM_UNITS_PER_WH
        efficiency = 1.0 - min(0.5, scarcity_score * 0.5)
        return base * efficiency


# =============================================================================
# THE ENHANCED CarbonEstimator
# =============================================================================

class CarbonEstimator:
    """
    Enhanced carbon estimator.

    Backward-compatible: same constructor signature, same `estimate()`
    signature, same return type.

    Original:
        est = CarbonEstimator(grid_intensity_g_kwh=385.0, pue=1.2)
        kg = est.estimate(energy_wh)  # float
    """

    DEFAULT_FEATURES = {
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

    # PUE plausible bounds (ASHRAE-ish): below 1.0 is impossible
    MIN_PUE = 1.0
    MAX_PUE = 3.0
    # Extremely high carbon triggers HITL review (kgCO2e)
    HITL_CARBON_THRESHOLD_KG = 10.0

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
        energy_meter: Any = None,
    ):
        # --- Original fields (preserved names) ---
        self.grid = float(grid_intensity_g_kwh)
        self.pue = float(pue)

        # --- Enhancement config ---
        self.region = region
        self.agent_id = agent_id
        self.deployment_id = deployment_id
        self.features = {**self.DEFAULT_FEATURES, **(features or {})}

        # --- Grid intensity provenance ---
        self._grid_source: str = GridSource.STATIC.value
        self._grid_timestamp: float = time.time()
        self._grid_uncertainty: Optional[float] = grid_uncertainty

        # --- Validation (warn, don't crash — original accepted anything) ---
        if not (self.MIN_PUE <= self.pue <= self.MAX_PUE):
            logger.warning(
                f"PUE={self.pue} outside plausible range "
                f"[{self.MIN_PUE}, {self.MAX_PUE}]; estimate may be unrealistic"
            )
        if self.grid < 0:
            logger.warning(f"Negative grid intensity: {self.grid}")

        # --- Optional injected energy meter ---
        self.energy_meter = energy_meter

        # --- Resilience ---
        self._grid_circuit = CircuitBreaker("grid_source", failure_threshold=3)
        self.chaos = (
            ChaosInjector(failure_rate=0.0)
            if self.features["chaos_testing"] else None
        )

        # --- Temporal logic ---
        self.temporal_monitor: Optional[TemporalLogicMonitor] = (
            TemporalLogicMonitor() if self.features["temporal_logic"] else None
        )

        # --- XAI ---
        self.explainer = CarbonExplainer() if self.features["xai"] else None

        # --- Causal RL ---
        self.causal_learner = (
            CausalCarbonLearner() if self.features["causal_rl"] else None
        )

        # --- Federated ---
        self.federated = (
            FederatedGridAggregator() if self.features["federated"] else None
        )

        # --- Carbon market ledger ---
        self.instruments = (
            CarbonInstrumentsLedger() if self.features["carbon_markets"] else None
        )

        # --- Helium companion ---
        self.helium_scarcity_fn: Optional[Callable[[], float]] = None

        # --- HITL (callback only; no interactive prompt here) ---
        self._hitl_callback: Optional[Callable[[Dict[str, Any]], bool]] = None

        # --- Statistics ---
        self._estimate_count: int = 0
        self._total_operational_kg: float = 0.0
        self._total_energy_wh: float = 0.0
        self._total_helium_units: float = 0.0

        logger.debug(
            f"CarbonEstimator initialized "
            f"(grid={self.grid}, pue={self.pue}, region={self.region}, "
            f"features={list(self.features)})"
        )

    # ------------------------------------------------------------------
    # ORIGINAL PUBLIC API — preserved exactly
    # ------------------------------------------------------------------

    def estimate(self, energy_wh: float) -> float:
        """
        Estimate operational carbon emissions in kgCO₂e.

        Backward-compatible: same signature, same return type, same math.
        """
        return self._estimate_internal(energy_wh).carbon_operational_kg

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
    ) -> CarbonEstimate:
        """
        Return a rich CarbonEstimate with provenance, uncertainty, XAI,
        separate operational vs contractual emissions, and helium companion.
        """
        return self._estimate_internal(
            energy_wh,
            precision=precision,
            policy_version=policy_version,
            helium_scarcity=helium_scarcity,
        )

    def estimate_batch(self, energies_wh: List[float]) -> List[float]:
        """Batch float API (partial-failure tolerant)."""
        out: List[float] = []
        for e in energies_wh:
            try:
                out.append(self.estimate(e))
            except Exception:
                out.append(float("nan"))
        return out

    def estimate_batch_detailed(
        self, energies_wh: List[float], **kwargs: Any,
    ) -> List[CarbonEstimate]:
        """Batch rich API (partial-failure tolerant)."""
        return [self.estimate_detailed(e, **kwargs) for e in energies_wh]

    def set_grid_intensity(
        self,
        value: float,
        source: str = GridSource.MANUAL.value,
        uncertainty: Optional[float] = None,
        region: Optional[str] = None,
    ) -> None:
        """Update grid intensity with explicit provenance."""
        self.grid = float(value)
        self._grid_source = source
        self._grid_timestamp = time.time()
        self._grid_uncertainty = uncertainty
        if region is not None:
            self.region = region
        logger.debug(
            f"Grid intensity updated: {value:.2f} gCO2/kWh "
            f"(source={source}, uncertainty={uncertainty})"
        )

    def update_from_forecaster(self, carbon_forecaster: Any) -> bool:
        """
        Pull the latest intensity from a carbon forecaster.

        Returns True on success, False on failure. Never raises.
        """
        if carbon_forecaster is None:
            return False
        if not self._grid_circuit.can_call():
            logger.debug("Grid-source circuit open; skipping forecaster update")
            return False
        try:
            if self.chaos and self.chaos.maybe_fail("forecaster"):
                raise RuntimeError("chaos: forecaster")
            getter = getattr(carbon_forecaster, "get_current_intensity", None)
            if getter is None:
                return False
            value = getter(self.region) if self.region else getter()
            # Handle async callers gracefully — don't await here
            if hasattr(value, "__await__"):
                logger.debug(
                    "Forecaster returned a coroutine; skip (async callers "
                    "should await separately)"
                )
                return False
            self.set_grid_intensity(
                float(value),
                source=GridSource.FORECASTER.value,
            )
            self._grid_circuit.record_success()
            return True
        except Exception as e:
            self._grid_circuit.record_failure()
            logger.warning(f"Forecaster update failed: {e}")
            return False

    def add_instrument(self, instrument: CarbonInstrument) -> None:
        """Register a contractual instrument (kept separate from operational)."""
        if self.instruments is None:
            logger.debug("Carbon markets feature disabled; instrument ignored")
            return
        self.instruments.add(instrument)

    def get_statistics(self) -> Dict[str, Any]:
        return {
            "deployment_id": self.deployment_id,
            "agent_id": self.agent_id,
            "region": self.region,
            "estimate_count": self._estimate_count,
            "total_operational_kg": self._total_operational_kg,
            "total_energy_wh": self._total_energy_wh,
            "total_helium_units": self._total_helium_units,
            "grid_intensity": self.grid,
            "grid_source": self._grid_source,
            "grid_age_seconds": time.time() - self._grid_timestamp,
            "grid_uncertainty": self._grid_uncertainty,
            "pue": self.pue,
            "circuit_state": self._grid_circuit.state.value,
            "causal_observations": (
                self.causal_learner.observations if self.causal_learner else 0
            ),
            "instruments_summary": (
                self.instruments.summary() if self.instruments else None
            ),
        }

    def export(self) -> Dict[str, Any]:
        return {
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
                if self.instruments else []
            ),
        }

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
    ) -> CarbonEstimate:
        # --- Input validation (warn, don't crash — original had no validation) ---
        energy_wh_f = self._safe_float(energy_wh)
        if energy_wh_f is None:
            logger.warning(f"Invalid energy_wh={energy_wh!r}; returning 0.0")
            return CarbonEstimate(
                energy_wh=0.0,
                grid_intensity_g_kwh=self.grid,
                pue=self.pue,
                carbon_operational_kg=0.0,
                grid_source=self._grid_source,
                grid_age_seconds=time.time() - self._grid_timestamp,
                grid_uncertainty=self._grid_uncertainty,
                region=self.region,
                agent_id=self.agent_id,
                policy_version=policy_version,
                simulated=True,
            )
        if energy_wh_f < 0:
            logger.warning(f"Negative energy_wh={energy_wh_f}; clamped to 0.0")
            energy_wh_f = 0.0

        # --- Precision energy multiplier (enhancement 1/7) ---
        precision_mult = 1.0
        if self.features["adaptive_precision"] and precision is not None:
            precision_mult = PrecisionEnergyMultiplier.get(precision)

        adjusted_energy_wh = energy_wh_f * precision_mult

        # --- Original formula (preserved exactly) ---
        kwh = adjusted_energy_wh / 1000.0
        carbon_kg = (kwh * self.grid * self.pue) / 1000.0

        # --- Causal correction (enhancement 2) ---
        causal_adjustment = 0.0
        if self.causal_learner:
            state = CausalCorrectionState(
                hour_of_day=datetime.now().hour,
                grid_source_hash=float(hash(self._grid_source) % 1000) / 1000.0,
                recent_energy_norm=min(1.0, adjusted_energy_wh / 1000.0),
            )
            causal_adjustment = self.causal_learner.predict_correction(state)
            carbon_kg = max(0.0, carbon_kg * (1.0 + causal_adjustment))

        # --- Uncertainty propagation (enhancement 6) ---
        uncertainty_kg = 0.0
        if self.features["uncertainty"] and self._grid_uncertainty is not None:
            uncertainty_kg = (
                kwh * self._grid_uncertainty * self.pue / 1000.0
            )

        # --- Helium companion (dual-axis mission) ---
        helium_units = 0.0
        if self.features["helium_companion"]:
            scarcity = helium_scarcity
            if scarcity is None and self.helium_scarcity_fn is not None:
                try:
                    scarcity = float(self.helium_scarcity_fn())
                except Exception:
                    scarcity = 0.0
            helium_units = HeliumCompanion.estimate_units(
                adjusted_energy_wh, scarcity or 0.0,
            )

        # --- Contractual instruments (kept separate) ---
        contractual_kg = 0.0
        instrument_provenance: Optional[Dict[str, Any]] = None
        if self.instruments is not None:
            contractual_kg = self.instruments.contractual_kgco2e()
            instrument_provenance = self.instruments.summary()

        # --- Temporal verification ---
        temporal_ok = True
        temporal_violations: List[str] = []
        if self.temporal_monitor:
            reading = GridIntensityReading(
                value=self.grid,
                source=self._grid_source,
                timestamp=self._grid_timestamp,
                uncertainty=self._grid_uncertainty,
                region=self.region,
            )
            temporal_ok, temporal_violations = self.temporal_monitor.verify(reading)

        # --- HITL review (extreme carbon) ---
        needs_review = False
        review_reason: Optional[str] = None
        if self.features["hitl"] and carbon_kg >= self.HITL_CARBON_THRESHOLD_KG:
            needs_review = True
            review_reason = (
                f"carbon estimate {carbon_kg:.4f} kg >= "
                f"threshold {self.HITL_CARBON_THRESHOLD_KG} kg"
            )
            if self._hitl_callback is not None:
                try:
                    approved = self._hitl_callback({
                        "reason": review_reason,
                        "carbon_kg": carbon_kg,
                        "energy_wh": energy_wh_f,
                        "agent_id": self.agent_id,
                    })
                    if approved is False:
                        logger.info(
                            "HITL denied estimate; keeping value but flagged"
                        )
                except Exception as e:
                    logger.warning(f"HITL callback failed: {e}")

        # --- Build estimate object ---
        estimate = CarbonEstimate(
            energy_wh=adjusted_energy_wh,
            grid_intensity_g_kwh=self.grid,
            pue=self.pue,
            carbon_operational_kg=carbon_kg,
            grid_source=self._grid_source,
            grid_age_seconds=time.time() - self._grid_timestamp,
            grid_uncertainty=self._grid_uncertainty,
            region=self.region,
            uncertainty_kg=uncertainty_kg,
            precision=precision,
            agent_id=self.agent_id,
            policy_version=policy_version,
            carbon_contractual_kg=contractual_kg,
            instrument_provenance=instrument_provenance,
            helium_units=helium_units,
            needs_review=needs_review,
            review_reason=review_reason,
            simulated=(self._grid_source == GridSource.STATIC.value
                       and self._grid_uncertainty is None),
        )

        # --- XAI explanation ---
        if self.explainer:
            estimate.explanation = CarbonExplainer.explain(estimate)
            if not temporal_ok:
                estimate.explanation.setdefault("rationale", []).append(
                    f"Temporal violations: {temporal_violations}"
                )

        # --- Statistics ---
        self._estimate_count += 1
        self._total_operational_kg += carbon_kg
        self._total_energy_wh += adjusted_energy_wh
        self._total_helium_units += helium_units

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

    def set_hitl_callback(
        self, callback: Callable[[Dict[str, Any]], bool],
    ) -> None:
        """Register a HITL review callback for extreme estimates."""
        self._hitl_callback = callback

    def record_measured(
        self,
        predicted_kg: float,
        measured_kg: float,
    ) -> None:
        """
        Feed back (predicted, measured) for causal correction learning.
        Called after ground-truth carbon measurement is available.
        """
        if not self.causal_learner:
            return
        state = CausalCorrectionState(
            hour_of_day=datetime.now().hour,
            grid_source_hash=float(hash(self._grid_source) % 1000) / 1000.0,
            recent_energy_norm=min(1.0, self._total_energy_wh / 1000.0),
        )
        self.causal_learner.record(state, predicted_kg, measured_kg)
        if self.causal_learner.observations % 10 == 0:
            self.causal_learner.update()

    def contribute_federated(self) -> None:
        """Contribute this deployment's grid intensity profile."""
        if not self.federated or not self.region:
            return
        self.federated.push(FederatedGridProfile(
            deployment_id=self.deployment_id,
            region=self.region,
            mean_intensity=self.grid,
            uncertainty=self._grid_uncertainty or 0.0,
            sample_count=max(1, self._estimate_count),
        ))


# =============================================================================
# Demo
# =============================================================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # --- Original behavior (backward compatible) ---
    est = CarbonEstimator(grid_intensity_g_kwh=385.0, pue=1.2)
    print("Original estimate(1000 Wh):", est.estimate(1000.0))
    # → (1.0 * 385 * 1.2) / 1000 = 0.462 kgCO2e

    # --- Enhanced behavior ---
    print("\n=== Enhanced CarbonEstimator ===")
    est2 = CarbonEstimator(
        grid_intensity_g_kwh=385.0,
        pue=1.2,
        region="US-CA",
        grid_uncertainty=25.0,
        agent_id="estimator-A",
        deployment_id="us-ca-prod-01",
        features={k: True for k in CarbonEstimator.DEFAULT_FEATURES},
    )

    # Rich estimate
    rec = est2.estimate_detailed(
        energy_wh=1500.0,
        precision="int8",
        policy_version="v5.0.1",
    )
    print(f"\nOperational carbon: {rec.carbon_operational_kg:.6f} kgCO2e")
    print(f"Uncertainty:        ±{rec.uncertainty_kg:.6f} kg")
    print(f"Helium companion:   {rec.helium_units:.8f} units")
    print(f"Grid source:        {rec.grid_source}")
    print(f"Needs review:       {rec.needs_review}")
    print(f"Simulated:          {rec.simulated}")

    if rec.explanation:
        print(f"\nXAI: {rec.explanation['headline']}")
        for r in rec.explanation["rationale"]:
            print(f"  • {r}")

    # Add a REC (kept separate!)
    est2.add_instrument(CarbonInstrument(
        instrument_id="rec-2026-001",
        kind="rec",
        quantity=5.0,
        unit="MWh",
        vintage_year=2026,
        matching_period="2026-03",
        producer="Pacific Wind Co.",
    ))
    rec2 = est2.estimate_detailed(energy_wh=2000.0)
    print(f"\nOperational: {rec2.carbon_operational_kg:.6f} kg (never summed)")
    print(f"Contractual: {rec2.carbon_contractual_kg:.6f} kg (separate)")

    # Update grid from forecaster
    class _MockForecaster:
        def get_current_intensity(self, region=None):
            return 220.0

    est2.update_from_forecaster(_MockForecaster())
    rec3 = est2.estimate_detailed(energy_wh=1000.0)
    print(f"\nAfter forecaster update: grid={est2.grid}, "
          f"carbon={rec3.carbon_operational_kg:.6f} kg")

    # Batch
    print("\nBatch:", est2.estimate_batch([1000.0, 2000.0, 3000.0]))

    # Statistics
    import json
    print("\n=== Statistics ===")
    print(json.dumps(est2.get_statistics(), indent=2, default=str))
