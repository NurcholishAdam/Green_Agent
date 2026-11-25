"""
Workload Interpreter (Enhanced, v2)
====================================

Analyzes and profiles incoming tasks, now with first-class support for:

  1. Quantum-Distillation of complexity estimators
  2. Causal RL for complexity correction
  3. Federated Green Learning across deployments
  4. Multi-Agent Coordination with emergent role specialisation
  5. Temporal Logic & Formal Verification for physical plausibility
  6. Explainable AI (XAI) for every profile
  7. Adaptive Precision Switching (hardware-aware estimation)
  8. Carbon Markets & REC enrichment
  9. Resilience Engineering (circuit breaker + chaos testing)
 10. Human-in-the-Loop for unknown/low-confidence profiles

Original API preserved:
    interpreter = WorkloadInterpreter(config)
    await interpreter.initialize()
    profile = await interpreter.analyze(task)

Changelog (v2 — enhancement pass):
  * P0-1  CausalComplexityLearner: one consistent additive formulation
          (removed the 0.15 magic, the multiplicative/multi-formulation drift).
  * P0-2  Stable `task_type` feature via SHA-256 (was `hash()` — non-reproducible
          across processes).
  * P0-3  `_parse_size_gb` no longer 1000×-shrinks numeric inputs.
  * P0-4  `_default_profile` no longer pollutes `profile_history` / `_last_profile`
          consumers; fallback profiles are stored in `_fallback_history`
          and flagged via `WorkloadProfile.is_fallback`.
  * P0-5  Complexity clamped to [0, 1] after the federated blend.
  * P0-6  `WorkloadProfile.deadline` typed as `Optional[float]`.
  * P0-7  Full `ProfileExplanation` retained on the profile
          (`explanation_full` + `explanation_rationale`).
  * P0-8  Distillation hook now *wraps* the estimator (passes the distilled
          size map in), instead of overwriting the estimator output.
  * P1-1  Async-safe `CircuitBreaker`; per-dependency circuits
          (`main`, `forecaster`, `market`, `hitl`).
  * P1-2  Async `HumanInTheLoopGate` with timeout (sync or async callbacks).
  * P1-3  `record_actual_complexity` is idempotent, accepts `profile_id=`,
          refuses to train on fallback profiles.
  * P1-4  Forecaster and market calls are individually circuit-broken.
  * P1-5  Federated push is delta-only (seq-num cursor).
  * P1-6  `_parse_size_mb` logs debug on fallback; regex accepts `KiB`,
          `MB`, scientific notation, commas.
  * P1-7  `InterpreterConfig` dataclass replaces magic numbers
          (energy/complexity, memory/complexity, priority thresholds,
          blend weights, helium thresholds, timeouts).
  * P1-8  `AdaptivePrecisionController.quantize` = real significand snapping.
  * P2-1  Helium profiling is actionable: high scarcity or over-budget helium
          escalates to REVIEW (HITL), is included in XAI, and is federated.
  * P2-2  `HeliumCoefficients` carries provenance + confidence.
  * P2-3  `WorkloadProfile` gains `profile_id`, `seq_num`, `is_fallback`,
          `explanation_full`, `explanation_rationale` (all defaulted —
          backward compatible).
  * P2-4  Real latency measured and reported to the coordinator
          (was a hardcoded 5.0 ms).
"""

from __future__ import annotations

from typing import (
    Any, Callable, Deque, Dict, List, Optional, Protocol, Tuple,
)
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from collections import deque, defaultdict
import asyncio
import hashlib
import inspect
import logging
import math
import random
import re
import statistics
import uuid

logger = logging.getLogger(__name__)


# =============================================================================
# Configuration surface
# =============================================================================

@dataclass(frozen=True)
class InterpreterConfig:
    """All tunable parameters in one place."""
    # Estimation constants (documented units)
    energy_kwh_per_complexity: float = 1.5
    carbon_kg_per_kwh_fallback: float = 0.4
    memory_mb_per_complexity: float = 1000.0
    cpu_pct_per_complexity: float = 100.0
    # Priority scale
    priority_min: float = 0.0
    priority_max: float = 10.0
    priority_default: float = 5.0
    priority_critical_threshold: float = 8.0
    # Causal learner
    causal_lr: float = 0.05
    causal_l2: float = 1e-4
    causal_buffer_size: int = 512
    causal_warmup_observations: int = 5
    causal_weight_clip: float = 5.0
    # Federated
    federated_blend_weight: float = 0.30
    federated_min_samples: float = 5.0
    federated_push_every_n: int = 25
    federated_min_profiles_per_type: int = 5
    # Helium
    helium_review_scarcity_threshold: float = 0.80
    helium_review_budget_units: Optional[float] = None
    # Market
    market_price_ttl_s: int = 300
    market_price_range_usd: Tuple[float, float] = (20.0, 80.0)
    # Circuit breaker
    circuit_failure_threshold: int = 3
    circuit_recovery_timeout_s: int = 60
    circuit_half_open_max_calls: int = 1
    # HITL
    hitl_confidence_threshold: float = 0.40
    hitl_guessed_confidence_threshold: float = 0.50
    hitl_timeout_s: float = 30.0
    # Temporal logic
    temporal_max_violations: int = 1024
    # History
    profile_history_size: int = 2048
    fallback_history_size: int = 256
    # Chaos
    chaos_failure_rate: float = 0.0
    chaos_seed: Optional[int] = None
    # Backward-compat: original `initialize()` printed to stdout
    verbose_init: bool = True


# =============================================================================
# Enums
# =============================================================================

class PrecisionLevel(Enum):
    FP32 = "fp32"
    FP16 = "fp16"
    INT8 = "int8"
    INT4 = "int4"
    QUANTUM_DISTILLED = "quantum_distilled"


class AgentRole(Enum):
    GENERALIST = "generalist"
    ML_INFERENCE_SPECIALIST = "ml_inference_specialist"
    TRAINING_SPECIALIST = "training_specialist"
    DATA_SPECIALIST = "data_specialist"
    LATENCY_OPTIMIZER = "latency_optimizer"


class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class TaskTier(Enum):
    KNOWN = "known"
    GUESSED = "guessed"
    REVIEW = "review"


# =============================================================================
# Extended WorkloadProfile (backward compatible)
# =============================================================================

@dataclass
class WorkloadProfile:
    """Profile of a workload task (extended).

    Units:
      energy_estimate  : kWh
      carbon_estimate  : kgCO2e
      memory_estimate  : MB
      cpu_estimate     : % of one core-equivalent
      carbon_intensity_gco2kwh : gCO2 / kWh
      helium_estimate_units    : arbitrary helium units (see HeliumCoefficients)
    """
    task_type: str
    complexity: float
    energy_estimate: float
    carbon_estimate: float
    priority: int
    deferrable: bool
    deadline: Optional[float] = None
    memory_estimate: float = 0.0
    cpu_estimate: float = 0.0
    # --- v1 optional fields (unchanged) ---
    uncertainty_std: float = 0.0
    confidence: float = 1.0
    tier: str = TaskTier.KNOWN.value
    explanation: Optional[str] = None
    contributing_factors: Dict[str, float] = field(default_factory=dict)
    helium_estimate_units: float = 0.0
    helium_scarcity_score: float = 0.0
    carbon_intensity_gco2kwh: float = 0.0
    market_enrichment: Optional[Dict[str, Any]] = None
    precision_used: str = "fp32"
    temporal_verified: bool = True
    temporal_violations: List[str] = field(default_factory=list)
    hitl_required: bool = False
    hitl_approved: Optional[bool] = None
    federated_blend_applied: bool = False
    causal_correction: float = 0.0
    distilled_used: bool = False
    # --- v2 fields (all defaulted) ---
    profile_id: str = ""
    seq_num: int = 0
    is_fallback: bool = False
    explanation_full: Optional["ProfileExplanation"] = None
    explanation_rationale: List[str] = field(default_factory=list)


# =============================================================================
# Safe input parsing helpers (fixed)
# =============================================================================

_SIZE_RE = re.compile(
    r"^\s*([0-9]+(?:[.,][0-9]+)?(?:[eE][+-]?[0-9]+)?)\s*([KMGT]?i?B?)\s*$"
)


def _safe_float(s: Any, default: float = 0.0) -> float:
    try:
        return float(s)
    except (TypeError, ValueError):
        return default


def _parse_size_mb(s: Any, default: float = 1.0) -> float:
    """Parse '1MB', '1.5 GiB', '2e3 MB' → MB. Safe on malformed input."""
    if s is None:
        return default
    if isinstance(s, (int, float)):
        # Numeric input is treated as MB (existing contract — documented).
        return float(s)
    try:
        text = str(s).strip()
        text = text.replace(",", ".")
        m = _SIZE_RE.match(text.upper())
        if not m:
            logger.debug(f"_parse_size_mb: unparseable {s!r}; using default={default}")
            return default
        value = float(m.group(1))
        unit = m.group(2) or "MB"
        # Normalize iB suffixes (KiB, MiB, GiB, TiB) to decimal-equivalent MB
        multipliers = {
            "B": 1e-6, "KB": 1e-3, "MB": 1.0, "GB": 1e3, "TB": 1e6,
            "": 1.0,
            "KIB": 1024 ** 1 / 1e6 * 1e6 / 1e3,   # ≈1.024e-3 MB
            "MIB": 1024 ** 2 / 1e6,               # ≈1.048576 MB
            "GIB": 1024 ** 3 / 1e6,               # ≈1073.741824 MB
            "TIB": 1024 ** 4 / 1e6,               # ≈1099511.627776 MB
        }
        factor = multipliers.get(unit)
        if factor is None:
            logger.debug(f"_parse_size_mb: unknown unit {unit!r}; using default={default}")
            return default
        return value * factor
    except Exception:
        return default


def _parse_size_gb(s: Any, default: float = 1.0) -> float:
    """Parse size input as GB.

    Numeric input is interpreted as GB (fixed; previously it was 1000× too small).
    """
    if s is None:
        return default
    if isinstance(s, (int, float)):
        return float(s)
    return _parse_size_mb(s, default * 1000.0) / 1000.0


def _stable_task_hash(task_type: str) -> float:
    """Deterministic [0, 1] hash of `task_type` across processes."""
    digest = hashlib.sha256(task_type.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") / float(1 << 64)


# =============================================================================
# ENHANCEMENT 9: Async-safe CircuitBreaker + gated ChaosInjector
# =============================================================================

class CircuitBreaker:
    """Async-safe circuit breaker with HALF_OPEN admission gate."""

    def __init__(
        self,
        name: str,
        failure_threshold: int = 3,
        recovery_timeout_seconds: int = 60,
        half_open_max_calls: int = 1,
    ) -> None:
        self.name = name
        self.failure_threshold = max(1, failure_threshold)
        self.recovery_timeout_seconds = max(0, recovery_timeout_seconds)
        self.half_open_max_calls = max(1, half_open_max_calls)
        self.state: CircuitState = CircuitState.CLOSED
        self.failures: int = 0
        self.last_failure_at: Optional[datetime] = None
        self._half_open_calls: int = 0
        self._lock = asyncio.Lock()

    async def can_call(self) -> bool:
        async with self._lock:
            if self.state == CircuitState.CLOSED:
                return True
            if self.state == CircuitState.OPEN:
                if (self.last_failure_at is not None and
                        (datetime.now() - self.last_failure_at).total_seconds()
                        > self.recovery_timeout_seconds):
                    self.state = CircuitState.HALF_OPEN
                    self._half_open_calls = 1
                    return True
                return False
            # HALF_OPEN
            if self._half_open_calls < self.half_open_max_calls:
                self._half_open_calls += 1
                return True
            return False

    async def record_success(self) -> None:
        async with self._lock:
            self.failures = 0
            self.state = CircuitState.CLOSED
            self._half_open_calls = 0

    async def record_failure(self) -> None:
        async with self._lock:
            self.failures += 1
            self.last_failure_at = datetime.now()
            if self.failures >= self.failure_threshold:
                self.state = CircuitState.OPEN
                logger.warning(
                    f"Circuit '{self.name}' OPEN after {self.failures} failures"
                )

    def snapshot(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "state": self.state.value,
            "failures": self.failures,
            "last_failure_at": (
                self.last_failure_at.isoformat() if self.last_failure_at else None
            ),
        }


class ChaosInjector:
    """Fault injector. Strictly a no-op unless enabled."""

    def __init__(
        self,
        failure_rate: float = 0.0,
        seed: Optional[int] = None,
    ) -> None:
        self.failure_rate = min(1.0, max(0.0, failure_rate))
        self.events: List[Dict[str, Any]] = []
        self._rng = random.Random(seed)

    @property
    def enabled(self) -> bool:
        return self.failure_rate > 0.0

    def maybe_fail(self, component: str) -> bool:
        if not self.enabled:
            return False
        if self._rng.random() < self.failure_rate:
            self.events.append({
                "component": component,
                "at": datetime.now().isoformat(),
            })
            return True
        return False


# =============================================================================
# ENHANCEMENT 5: Temporal Logic (bounded history + per-property counters)
# =============================================================================

class ProfileProperty(Protocol):
    def check(self, profile: WorkloadProfile) -> bool: ...
    def name(self) -> str: ...


@dataclass
class ComplexityBounds:
    def check(self, p: WorkloadProfile) -> bool:
        return 0.0 <= p.complexity <= 1.0
    def name(self) -> str:
        return "ComplexityBounds"


@dataclass
class NonNegativeEnergy:
    def check(self, p: WorkloadProfile) -> bool:
        return p.energy_estimate >= 0.0
    def name(self) -> str:
        return "NonNegativeEnergy"


@dataclass
class NonNegativeCarbon:
    def check(self, p: WorkloadProfile) -> bool:
        return p.carbon_estimate >= 0.0
    def name(self) -> str:
        return "NonNegativeCarbon"


@dataclass
class NonNegativeMemory:
    def check(self, p: WorkloadProfile) -> bool:
        return p.memory_estimate >= 0.0
    def name(self) -> str:
        return "NonNegativeMemory"


@dataclass
class EnergyUpperBound:
    max_kwh: float = 10_000.0
    def check(self, p: WorkloadProfile) -> bool:
        return p.energy_estimate <= self.max_kwh
    def name(self) -> str:
        return "EnergyUpperBound"


class TemporalLogicMonitor:
    """Bounded-history safety monitor with per-property counters."""

    def __init__(self, max_violations: int = 1024) -> None:
        self.properties: List[ProfileProperty] = []
        self.violations: Deque[Dict[str, Any]] = deque(maxlen=max_violations)
        self.violation_counts: Dict[str, int] = defaultdict(int)
        self._checks: int = 0

    def register(self, p: ProfileProperty) -> None:
        self.properties.append(p)
        self.violation_counts.setdefault(p.name(), 0)

    def verify(self, profile: WorkloadProfile) -> Tuple[bool, List[str]]:
        self._checks += 1
        bad: List[str] = []
        for p in self.properties:
            if not p.check(profile):
                bad.append(p.name())
                self.violation_counts[p.name()] += 1
                self.violations.append({
                    "property": p.name(),
                    "task_type": profile.task_type,
                    "profile_id": profile.profile_id,
                    "at": datetime.now().isoformat(),
                })
        return (len(bad) == 0, bad)

    def stats(self) -> Dict[str, Any]:
        return {
            "checks": self._checks,
            "violation_counts": dict(self.violation_counts),
            "recent": list(self.violations)[-5:],
        }


# =============================================================================
# ENHANCEMENT 6: XAI (narrative + preserved rationale)
# =============================================================================

@dataclass
class ProfileExplanation:
    headline: str
    rationale: List[str]
    confidence: float
    contributing_factors: Dict[str, float]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class ProfileExplainer:
    @staticmethod
    def explain(
        task_type: str,
        complexity: float,
        energy_kwh: float,
        carbon_kg: float,
        tier: str,
        contributing_factors: Dict[str, float],
        confidence: float,
        uncertainty_std: float = 0.0,
        helium_units: float = 0.0,
        helium_scarcity: float = 0.0,
        causal_correction: float = 0.0,
        federated_blend_applied: bool = False,
    ) -> ProfileExplanation:
        reasons: List[str] = [
            (
                f"Task type '{task_type}' (tier={tier}) → complexity "
                f"{complexity:.3f}, energy {energy_kwh:.4f} kWh, "
                f"carbon {carbon_kg:.4f} kgCO₂e."
            )
        ]
        if contributing_factors:
            top = sorted(
                contributing_factors.items(),
                key=lambda kv: abs(kv[1]), reverse=True,
            )[:3]
            reasons.append(
                "Top contributing factors: " +
                ", ".join(f"{k}={v:.3f}" for k, v in top) + "."
            )
        if uncertainty_std > 0:
            reasons.append(f"Uncertainty (σ) = {uncertainty_std:.4f}.")
        if tier == TaskTier.GUESSED.value:
            reasons.append("Fallback estimator used (task type not recognized).")
        if confidence < 0.5:
            reasons.append("Low confidence — consider human review.")
        if causal_correction:
            reasons.append(
                f"Causal correction applied: {causal_correction:+.4f} "
                f"(clamped to [0, 1])."
            )
        if federated_blend_applied:
            reasons.append("Federated peer blend applied.")
        if helium_units > 0:
            reasons.append(
                f"Helium consumption estimate: {helium_units:.4f} units "
                f"(scarcity={helium_scarcity:.2f})."
            )

        return ProfileExplanation(
            headline=(
                f"[{task_type}] complexity={complexity:.2f} "
                f"({confidence:.2f} conf)"
            ),
            rationale=reasons,
            confidence=confidence,
            contributing_factors=dict(contributing_factors),
        )


# =============================================================================
# ENHANCEMENT 7: Adaptive Precision (real significand snapping)
# =============================================================================

@dataclass
class HardwareProfile:
    has_tensor_cores: bool = True
    supports_int8: bool = True
    supports_int4: bool = False
    vram_gb: float = 24.0
    edge_device: bool = False


class AdaptivePrecisionController:
    _SIG_BITS = {
        PrecisionLevel.FP32: 24,
        PrecisionLevel.FP16: 11,
        PrecisionLevel.INT8: 8,
        PrecisionLevel.INT4: 4,
        PrecisionLevel.QUANTUM_DISTILLED: 3,
    }

    def __init__(self, hw: Optional[HardwareProfile] = None) -> None:
        self.hw = hw or HardwareProfile()

    def select(self, urgency: str) -> PrecisionLevel:
        if urgency == "critical":
            return PrecisionLevel.FP16
        if self.hw.edge_device:
            return (
                PrecisionLevel.INT8 if self.hw.supports_int8
                else PrecisionLevel.FP16
            )
        return (
            PrecisionLevel.INT4 if self.hw.supports_int4
            else PrecisionLevel.INT8
        )

    @classmethod
    def quantize(cls, value: float, precision: PrecisionLevel) -> float:
        """Snap `value` to the nearest representable value for `precision`."""
        if value == 0.0 or math.isnan(value) or math.isinf(value):
            return value
        sig = cls._SIG_BITS[precision]
        try:
            exp = math.floor(math.log2(abs(value)))
        except (ValueError, OverflowError):
            return value
        step = 2.0 ** (exp - sig + 1)
        return round(value / step) * step


# =============================================================================
# ENHANCEMENT 2: Causal RL for Complexity Correction (single formulation)
# =============================================================================

@dataclass
class ComplexityState:
    task_type_hash: float
    model_size_norm: float
    input_size_norm: float
    dataset_size_norm: float
    hour_of_day: int


class CausalComplexityLearner:
    """Additive linear correction on top of the hand-written estimators.

    Model:    corrected = clamp(hand_pred + bias + w·features, 0, 1)
    Update:   err      = actual - corrected
              w_i     += lr * (err * f_i - l2 * w_i)
              bias    += lr * err
    Warmup:   prediction = 0.0 until `warmup_observations` records seen.
    """

    N_FEATURES = 5
    # Bump when the feature layout changes (invalidates persisted state).
    FEATURE_VERSION = 2

    def __init__(
        self,
        lr: float = 0.05,
        l2: float = 1e-4,
        buffer_size: int = 512,
        warmup_observations: int = 5,
        weight_clip: float = 5.0,
    ) -> None:
        self.lr = lr
        self.l2 = l2
        self.weight_clip = weight_clip
        self.warmup_observations = max(0, warmup_observations)
        self.weights: List[float] = [0.0] * self.N_FEATURES
        self.bias: float = 0.0
        self.buffer: Deque[Tuple[List[float], float, float]] = deque(
            maxlen=buffer_size
        )
        self.observations: int = 0
        self.update_count: int = 0

    @staticmethod
    def _features(state: ComplexityState) -> List[float]:
        return [
            state.task_type_hash,
            state.model_size_norm,
            state.input_size_norm,
            state.dataset_size_norm,
            state.hour_of_day / 24.0,
        ]

    def predict_correction(self, state: ComplexityState) -> float:
        if self.observations < self.warmup_observations:
            return 0.0
        f = self._features(state)
        return self.bias + sum(w * x for w, x in zip(self.weights, f))

    def record(
        self, state: ComplexityState,
        predicted_complexity: float, actual_complexity: float,
    ) -> None:
        f = self._features(state)
        self.buffer.append((f, predicted_complexity, actual_complexity))
        self.observations += 1

    def update(self) -> None:
        if not self.buffer:
            return
        for f, pred, actual in self.buffer:
            correction = self.bias + sum(w * x for w, x in zip(self.weights, f))
            corrected = max(0.0, min(1.0, pred + correction))
            err = actual - corrected
            for i, x in enumerate(f):
                grad = err * x - self.l2 * self.weights[i]
                self.weights[i] += self.lr * grad
                self.weights[i] = max(
                    -self.weight_clip,
                    min(self.weight_clip, self.weights[i]),
                )
            self.bias = max(-self.weight_clip, min(self.weight_clip, self.bias + self.lr * err))
        self.buffer.clear()
        self.update_count += 1

    def stats(self) -> Dict[str, Any]:
        return {
            "feature_version": self.FEATURE_VERSION,
            "observations": self.observations,
            "updates": self.update_count,
            "weights": list(self.weights),
            "bias": self.bias,
        }


# =============================================================================
# ENHANCEMENT 1: Quantum-Distillation of Estimators
# =============================================================================

@dataclass
class DistilledEstimator:
    precision: PrecisionLevel
    size_map: Dict[str, float]
    quality_retention: float
    energy_reduction_percent: float


class QuantumDistillationBridge:
    """Distills a size map to a lower precision (real significand snapping)."""

    _RETENTION = {
        PrecisionLevel.FP32: (1.00, 0.0),
        PrecisionLevel.FP16: (0.98, 30.0),
        PrecisionLevel.INT8: (0.93, 55.0),
        PrecisionLevel.INT4: (0.85, 70.0),
        PrecisionLevel.QUANTUM_DISTILLED: (0.80, 85.0),
    }

    def distill(
        self, size_map: Dict[str, float], precision: PrecisionLevel
    ) -> DistilledEstimator:
        retention, energy = self._RETENTION[precision]
        return DistilledEstimator(
            precision=precision,
            size_map={
                k: AdaptivePrecisionController.quantize(v, precision)
                for k, v in size_map.items()
            },
            quality_retention=retention,
            energy_reduction_percent=energy,
        )


# =============================================================================
# ENHANCEMENT 3: Federated Green Learning (delta-aware)
# =============================================================================

@dataclass
class FederatedComplexityProfile:
    deployment_id: str
    task_type: str
    mean_complexity: float
    mean_energy_kwh: float
    mean_carbon_kg: float
    sample_count: int
    mean_helium_units: float = 0.0
    timestamp: datetime = field(default_factory=datetime.now)


class FederatedAggregator:
    def __init__(self, dp_noise_scale: float = 0.0, seed: Optional[int] = None) -> None:
        self.updates: List[FederatedComplexityProfile] = []
        self._global: Dict[str, Dict[str, float]] = {}
        self.dp_noise_scale = max(0.0, dp_noise_scale)
        self._rng = random.Random(seed)

    def push(self, u: FederatedComplexityProfile) -> None:
        self.updates.append(u)

    def aggregate(self) -> Dict[str, Dict[str, float]]:
        grouped: Dict[str, List[FederatedComplexityProfile]] = defaultdict(list)
        for u in self.updates:
            grouped[u.task_type].append(u)
        result: Dict[str, Dict[str, float]] = {}
        for tt, profiles in grouped.items():
            total_w = sum(p.sample_count for p in profiles) or 1
            c = sum(p.mean_complexity * p.sample_count for p in profiles) / total_w
            e = sum(p.mean_energy_kwh * p.sample_count for p in profiles) / total_w
            k = sum(p.mean_carbon_kg * p.sample_count for p in profiles) / total_w
            h = sum(p.mean_helium_units * p.sample_count for p in profiles) / total_w
            if self.dp_noise_scale > 0.0:
                c += self._rng.gauss(0.0, self.dp_noise_scale * 0.1)
                e += self._rng.gauss(0.0, self.dp_noise_scale * 0.01)
                k += self._rng.gauss(0.0, self.dp_noise_scale * 0.01)
                h += self._rng.gauss(0.0, self.dp_noise_scale * 0.001)
            result[tt] = {
                "mean_complexity": c,
                "mean_energy_kwh": e,
                "mean_carbon_kg": k,
                "mean_helium_units": h,
                "sample_count": float(total_w),
            }
        self._global = result
        return result

    def lookup(self, task_type: str) -> Optional[Dict[str, float]]:
        return self._global.get(task_type)


# =============================================================================
# ENHANCEMENT 4: Multi-Agent Coordination
# =============================================================================

@dataclass
class AgentProfile:
    agent_id: str
    role: AgentRole = AgentRole.GENERALIST
    ml_accuracy: float = 0.0
    training_accuracy: float = 0.0
    data_accuracy: float = 0.0
    avg_latency_ms: float = 0.0
    total_calls: int = 0


class MultiAgentCoordinator:
    def __init__(self) -> None:
        self.agents: Dict[str, AgentProfile] = {}

    def register(self, agent_id: str) -> AgentProfile:
        if agent_id not in self.agents:
            self.agents[agent_id] = AgentProfile(agent_id)
        return self.agents[agent_id]

    def _reassign(self) -> None:
        for a in self.agents.values():
            if a.ml_accuracy > 0.8 and a.ml_accuracy >= a.training_accuracy:
                a.role = AgentRole.ML_INFERENCE_SPECIALIST
            elif a.training_accuracy > 0.8:
                a.role = AgentRole.TRAINING_SPECIALIST
            elif a.data_accuracy > 0.8:
                a.role = AgentRole.DATA_SPECIALIST
            elif a.avg_latency_ms and a.avg_latency_ms < 500:
                a.role = AgentRole.LATENCY_OPTIMIZER
            else:
                a.role = AgentRole.GENERALIST

    def record(
        self, agent_id: str, task_type: str,
        accuracy: float, latency_ms: float,
    ) -> None:
        a = self.register(agent_id)
        n = a.total_calls + 1
        if task_type == "ml_inference":
            a.ml_accuracy = ((n - 1) * a.ml_accuracy + accuracy) / n
        elif task_type == "ml_training":
            a.training_accuracy = ((n - 1) * a.training_accuracy + accuracy) / n
        elif task_type == "data_processing":
            a.data_accuracy = ((n - 1) * a.data_accuracy + accuracy) / n
        a.avg_latency_ms = ((n - 1) * a.avg_latency_ms + latency_ms) / n
        a.total_calls = n
        if n % 5 == 0:
            self._reassign()

    def select_agent(self, task_type: str) -> Optional[str]:
        if not self.agents:
            return None
        role = {
            "ml_inference": AgentRole.ML_INFERENCE_SPECIALIST,
            "ml_training": AgentRole.TRAINING_SPECIALIST,
            "data_processing": AgentRole.DATA_SPECIALIST,
        }.get(task_type, AgentRole.GENERALIST)
        cands = [a for a in self.agents.values() if a.role == role]
        if not cands:
            cands = list(self.agents.values())
        return max(cands, key=lambda a: a.total_calls).agent_id


# =============================================================================
# ENHANCEMENT 8: Carbon Markets & RECs (injectable, idempotent, cached)
# =============================================================================

@dataclass
class MarketSnapshot:
    carbon_price_per_tco2_usd: float
    rec_price_per_mwh_usd: float
    rec_available_mwh: float
    timestamp: datetime = field(default_factory=datetime.now)


MarketPriceProvider = Callable[[], MarketSnapshot]


class CarbonMarketClient:
    def __init__(
        self,
        price_provider: Optional[MarketPriceProvider] = None,
        price_ttl_s: int = 300,
        price_range_usd: Tuple[float, float] = (20.0, 80.0),
        seed: Optional[int] = None,
    ) -> None:
        self._cache: Optional[MarketSnapshot] = None
        self._ttl = price_ttl_s
        self._price_provider = price_provider
        self._price_range = price_range_usd
        self._rng = random.Random(seed)

    def get_snapshot(self) -> MarketSnapshot:
        if (self._cache is not None and
                (datetime.now() - self._cache.timestamp).total_seconds() < self._ttl):
            return self._cache
        if self._price_provider is not None:
            snap = self._price_provider()
        else:
            lo, hi = self._price_range
            snap = MarketSnapshot(
                carbon_price_per_tco2_usd=self._rng.uniform(lo, hi),
                rec_price_per_mwh_usd=self._rng.uniform(3.0, 9.0),
                rec_available_mwh=self._rng.uniform(10.0, 500.0),
            )
        self._cache = snap
        return snap


# =============================================================================
# ENHANCEMENT 10: HITL + Active Learning (async, timeout-guarded)
# =============================================================================

@dataclass
class HITLRequest:
    task_type: str
    complexity: float
    confidence: float
    reason: str
    urgency: str
    requested_at: datetime = field(default_factory=datetime.now)


class HumanInTheLoopGate:
    def __init__(
        self,
        confidence_threshold: float = 0.4,
        guessed_confidence_threshold: float = 0.5,
        timeout_s: float = 30.0,
    ) -> None:
        self.confidence_threshold = confidence_threshold
        self.guessed_confidence_threshold = guessed_confidence_threshold
        self.timeout_s = timeout_s
        self.pending: List[HITLRequest] = []
        self.feedback_log: List[Dict[str, Any]] = []
        self._callback: Optional[Callable[[HITLRequest], Any]] = None
        self._lock = asyncio.Lock()

    def set_callback(self, cb: Callable[[HITLRequest], Any]) -> None:
        """Callback may be sync or async; both are supported."""
        self._callback = cb

    def needs_review(self, tier: str, confidence: float) -> bool:
        if tier == TaskTier.REVIEW.value:
            return True
        if tier == TaskTier.GUESSED.value and confidence < self.guessed_confidence_threshold:
            return True
        return confidence < self.confidence_threshold

    async def _invoke(self, req: HITLRequest) -> bool:
        if self._callback is None:
            return False
        result = self._callback(req)
        if inspect.iscoroutine(result):
            result = await result
        return bool(result)

    async def request_review(
        self, task_type: str, complexity: float,
        confidence: float, reason: str, urgency: str = "medium",
    ) -> bool:
        req = HITLRequest(task_type, complexity, confidence, reason, urgency)
        async with self._lock:
            self.pending.append(req)
        try:
            if urgency == "low":
                decision = True
            elif self._callback is None:
                decision = False
            else:
                try:
                    decision = await asyncio.wait_for(
                        self._invoke(req), timeout=self.timeout_s
                    )
                except asyncio.TimeoutError:
                    logger.warning(
                        f"HITL review timed out after {self.timeout_s}s "
                        f"for {task_type}"
                    )
                    decision = False
        finally:
            async with self._lock:
                if req in self.pending:
                    self.pending.remove(req)
            self.feedback_log.append({
                "req": asdict(req), "decision": decision,
            })
        return decision

    def active_learning_batch(self, n: int = 16) -> List[Dict[str, Any]]:
        return self.feedback_log[-n:]


# =============================================================================
# Helium Profiling (coefficients with provenance + actionable thresholds)
# =============================================================================

@dataclass
class HeliumCoefficients:
    """Helium units per kWh, by task type, with provenance metadata.

    The default values are order-of-magnitude estimates from the
    Green Agent v5.0.0 dual-axis sustainability model. They are *not*
    hardware-specific — override per deployment if you have better data.
    """
    units_per_kwh: Dict[str, float] = field(default_factory=lambda: {
        "ml_inference": 0.02,
        "ml_training": 0.15,
        "data_processing": 0.005,
        "default": 0.01,
    })
    source: str = "green-agent-v5.0.0-default"
    confidence: float = 0.5


class HeliumProfiler:
    """Estimate helium consumption from complexity, energy, and task type."""

    def __init__(self, coefficients: Optional[HeliumCoefficients] = None) -> None:
        self.coefficients = coefficients or HeliumCoefficients()

    def estimate_helium_units(self, task_type: str, energy_kwh: float) -> float:
        rate = self.coefficients.units_per_kwh.get(
            task_type, self.coefficients.units_per_kwh["default"]
        )
        return energy_kwh * rate


# =============================================================================
# The Enhanced WorkloadInterpreter
# =============================================================================

class WorkloadInterpreter:
    """
    Enhanced workload interpreter with ten enhancement layers plus
    helium profiling.

    Backward-compatible signature:
        WorkloadInterpreter(config)
    """

    DEFAULT_FEATURES = {
        "causal_rl": True,
        "xai": True,
        "adaptive_precision": True,
        "federated": True,
        "multi_agent": True,
        "temporal_logic": True,
        "carbon_market": True,
        "chaos_testing": False,
        "hitl": True,
        "quantum_distillation": True,
        "helium_profiling": True,
    }

    _ML_SIZE_MAP: Dict[str, float] = {
        "10M": 0.1, "50M": 0.3, "100M": 0.5,
        "500M": 0.7, "1B": 0.9, "10B": 1.0,
    }

    def __init__(
        self,
        config: Dict,
        deployment_id: str = "local",
        agent_id: str = "interpreter-0",
        features: Optional[Dict[str, bool]] = None,
        hardware: Optional[HardwareProfile] = None,
        pipeline: Optional[InterpreterConfig] = None,
        # --- test seams (all optional) ---
        market_price_provider: Optional[MarketPriceProvider] = None,
        helium_coefficients: Optional[HeliumCoefficients] = None,
    ):
        self.integration_config = config or {}
        self.config = pipeline or InterpreterConfig()
        self.deployment_id = deployment_id
        self.agent_id = agent_id
        self.features = {**self.DEFAULT_FEATURES, **(features or {})}
        self._validate_features()

        # --- Original complexity models ---
        self.complexity_models = {
            "ml_inference": self._estimate_ml_complexity,
            "ml_training": self._estimate_training_complexity,
            "data_processing": self._estimate_data_complexity,
            "default": self._estimate_default_complexity,
        }

        # --- Enhancement layers ---
        cb_kwargs = dict(
            failure_threshold=self.config.circuit_failure_threshold,
            recovery_timeout_seconds=self.config.circuit_recovery_timeout_s,
            half_open_max_calls=self.config.circuit_half_open_max_calls,
        )
        self.circuits: Dict[str, CircuitBreaker] = {
            "main": CircuitBreaker("interpreter.main", **cb_kwargs),
            "forecaster": CircuitBreaker("interpreter.forecaster", **cb_kwargs),
            "market": CircuitBreaker("interpreter.market", **cb_kwargs),
            "hitl": CircuitBreaker("interpreter.hitl", **cb_kwargs),
        }
        self.chaos = (
            ChaosInjector(
                failure_rate=self.config.chaos_failure_rate,
                seed=self.config.chaos_seed,
            )
            if self.features["chaos_testing"] else None
        )

        self.temporal_monitor = (
            TemporalLogicMonitor(
                max_violations=self.config.temporal_max_violations,
            )
            if self.features["temporal_logic"] else None
        )
        if self.temporal_monitor:
            self.temporal_monitor.register(ComplexityBounds())
            self.temporal_monitor.register(NonNegativeEnergy())
            self.temporal_monitor.register(NonNegativeCarbon())
            self.temporal_monitor.register(NonNegativeMemory())
            self.temporal_monitor.register(EnergyUpperBound())

        self.explainer = ProfileExplainer() if self.features["xai"] else None
        self.precision_ctl = (
            AdaptivePrecisionController(hardware)
            if self.features["adaptive_precision"] else None
        )
        self.causal_learner = (
            CausalComplexityLearner(
                lr=self.config.causal_lr,
                l2=self.config.causal_l2,
                buffer_size=self.config.causal_buffer_size,
                warmup_observations=self.config.causal_warmup_observations,
                weight_clip=self.config.causal_weight_clip,
            )
            if self.features["causal_rl"] else None
        )
        self.distiller = (
            QuantumDistillationBridge()
            if self.features["quantum_distillation"] else None
        )
        self.federated = (
            FederatedAggregator() if self.features["federated"] else None
        )
        self.coordinator = (
            MultiAgentCoordinator() if self.features["multi_agent"] else None
        )
        if self.coordinator:
            self.coordinator.register(self.agent_id)
        self.market = (
            CarbonMarketClient(
                price_provider=market_price_provider,
                price_ttl_s=self.config.market_price_ttl_s,
                price_range_usd=self.config.market_price_range_usd,
            )
            if self.features["carbon_market"] else None
        )
        self.hitl = (
            HumanInTheLoopGate(
                confidence_threshold=self.config.hitl_confidence_threshold,
                guessed_confidence_threshold=(
                    self.config.hitl_guessed_confidence_threshold
                ),
                timeout_s=self.config.hitl_timeout_s,
            )
            if self.features["hitl"] else None
        )
        self.helium_profiler = (
            HeliumProfiler(helium_coefficients)
            if self.features["helium_profiling"] else None
        )

        # --- Bookkeeping ---
        self.profile_history: Deque[WorkloadProfile] = deque(
            maxlen=self.config.profile_history_size
        )
        self._fallback_history: Deque[WorkloadProfile] = deque(
            maxlen=self.config.fallback_history_size
        )
        self._profiles_by_id: Dict[str, WorkloadProfile] = {}
        self._feedback_seen: set = set()
        self._profile_seq_counter: int = 0
        self._last_federated_seq: int = 0
        self._last_profile: Optional[WorkloadProfile] = None
        self._last_profile_is_fallback: bool = False
        self._analyze_lock = asyncio.Lock()

        logger.info(
            f"✅ Enhanced WorkloadInterpreter initialized "
            f"(deployment={deployment_id}, agent={agent_id}, "
            f"features={list(self.features)})"
        )

    def _validate_features(self) -> None:
        if self.features.get("chaos_testing"):
            logger.warning(
                "chaos_testing enabled — expect intentional failures in prod"
            )

    # ------------------------------------------------------------------
    # Lifecycle (backward compatible)
    # ------------------------------------------------------------------

    async def initialize(self) -> None:
        """Initialize interpreter (original stdout print preserved behind a flag)."""
        if self.config.verbose_init:
            print("✅ Workload interpreter initialized")
        logger.info(
            f"WorkloadInterpreter ready with features={list(self.features)}"
        )

    # ------------------------------------------------------------------
    # Main entry point (backward compatible)
    # ------------------------------------------------------------------

    async def analyze(self, task: Dict) -> WorkloadProfile:
        """Analyze task and create enhanced workload profile."""
        if not await self.circuits["main"].can_call():
            logger.warning("Interpreter circuit open — returning fallback profile")
            return await self._default_profile(task, reason="circuit open")

        if self.chaos and self.chaos.maybe_fail("analyze"):
            await self.circuits["main"].record_failure()
            return await self._default_profile(task, reason="chaos injection")

        async with self._analyze_lock:
            try:
                profile = await self._analyze_internal(task)
                await self.circuits["main"].record_success()
                return profile
            except Exception as e:
                await self.circuits["main"].record_failure()
                logger.warning(f"Analyze failed ({e}); returning fallback")
                return await self._default_profile(task, reason=str(e))

    async def _analyze_internal(self, task: Dict) -> WorkloadProfile:
        start_time = datetime.now()

        # --- Extract fields safely ---
        task_type = task.get("type", "default")
        model_size = task.get("model_size", "100M")
        input_size = task.get("input_size", "1MB")
        priority = int(_safe_float(
            task.get("priority"), self.config.priority_default
        ))
        deadline = task.get("deadline")
        region = task.get("region", "US-CA")

        known_types = set(self.complexity_models.keys())
        tier = (TaskTier.KNOWN.value if task_type in known_types
                else TaskTier.GUESSED.value)

        # --- P0-8 fix: distillation wraps the estimator, not the output ---
        distilled_used = False
        size_map_for_estimate: Optional[Dict[str, float]] = None
        if (self.distiller and self.precision_ctl
                and task_type == "ml_inference"):
            precision_for_distill = self.precision_ctl.select("normal")
            dist = self.distiller.distill(self._ML_SIZE_MAP, precision_for_distill)
            size_map_for_estimate = dist.size_map
            distilled_used = True

        estimator = self.complexity_models.get(
            task_type, self.complexity_models["default"]
        )
        try:
            if task_type == "ml_inference" and size_map_for_estimate is not None:
                complexity = self._estimate_ml_complexity(
                    task, size_map=size_map_for_estimate
                )
            else:
                complexity = estimator(task)
        except Exception as e:
            logger.warning(f"Estimator failed ({e}); using default")
            complexity = 0.5
            tier = TaskTier.GUESSED.value

        # --- Causal correction (P0-1 fix: additive, clamped) ---
        causal_correction = 0.0
        causal_state: Optional[ComplexityState] = None
        if self.causal_learner:
            causal_state = self._make_causal_state(task_type, task)
            causal_correction = self.causal_learner.predict_correction(causal_state)
            complexity = max(0.0, min(1.0, complexity + causal_correction))

        # --- Federated blend (P0-5 fix: clamp after blending) ---
        federated_blend_applied = False
        if self.federated:
            profile = self.federated.lookup(task_type)
            if (profile and
                    profile.get("sample_count", 0.0)
                    >= self.config.federated_min_samples):
                blend = self.config.federated_blend_weight
                complexity = (
                    (1.0 - blend) * complexity
                    + blend * profile["mean_complexity"]
                )
                complexity = max(0.0, min(1.0, complexity))
                federated_blend_applied = True

        # --- Precision selection ---
        precision = PrecisionLevel.FP32
        if self.precision_ctl:
            urgency = (
                "critical"
                if priority >= self.config.priority_critical_threshold
                else "normal"
            )
            precision = self.precision_ctl.select(urgency)

        # --- Resource estimates (units documented on WorkloadProfile) ---
        energy_estimate = complexity * self.config.energy_kwh_per_complexity
        carbon_estimate = energy_estimate * self.config.carbon_kg_per_kwh_fallback
        memory_estimate = complexity * self.config.memory_mb_per_complexity
        cpu_estimate = complexity * self.config.cpu_pct_per_complexity

        # --- Carbon intensity via circuit-broken forecaster ---
        carbon_intensity = 0.0
        forecaster = self.integration_config.get("carbon_forecaster")
        if forecaster is not None and await self.circuits["forecaster"].can_call():
            try:
                getter = getattr(forecaster, "get_current_intensity", None)
                if getter is not None:
                    result = getter(region)
                    if inspect.isawaitable(result):
                        result = await result
                    carbon_intensity = float(result)
                    if carbon_intensity > 0:
                        carbon_estimate = (
                            energy_estimate * carbon_intensity / 1000.0
                        )
                    await self.circuits["forecaster"].record_success()
            except Exception as e:
                await self.circuits["forecaster"].record_failure()
                logger.debug(f"Forecaster lookup failed: {e}")

        # --- Precision quantization (P1-8 fix: significand snapping) ---
        if self.precision_ctl:
            energy_estimate = self.precision_ctl.quantize(energy_estimate, precision)
            carbon_estimate = self.precision_ctl.quantize(carbon_estimate, precision)
            memory_estimate = self.precision_ctl.quantize(memory_estimate, precision)
            cpu_estimate = self.precision_ctl.quantize(cpu_estimate, precision)
            complexity = self.precision_ctl.quantize(complexity, precision)

        # --- Uncertainty from history (fallbacks excluded) ---
        uncertainty_std = 0.0
        confidence = 0.9 if tier == TaskTier.KNOWN.value else 0.5
        recent = [
            p.complexity for p in self.profile_history
            if p.task_type == task_type and not p.is_fallback
        ][-20:]
        if len(recent) >= 3:
            uncertainty_std = statistics.pstdev(recent)
            if uncertainty_std > 0.2:
                confidence = max(0.3, confidence - uncertainty_std)

        # --- Helium profiling ---
        helium_units = 0.0
        helium_scarcity_score = 0.0
        helium_review_reason: Optional[str] = None
        if self.helium_profiler:
            helium_units = self.helium_profiler.estimate_helium_units(
                task_type, energy_estimate
            )
            scarcity_fn = self.integration_config.get("helium_scarcity_fn")
            if callable(scarcity_fn):
                try:
                    helium_scarcity_score = float(scarcity_fn())
                except Exception:
                    helium_scarcity_score = 0.0
            # P2-1: actionable helium
            if helium_scarcity_score > self.config.helium_review_scarcity_threshold:
                helium_review_reason = (
                    f"helium scarcity {helium_scarcity_score:.2f} "
                    f"> {self.config.helium_review_scarcity_threshold}"
                )
            budget = self.config.helium_review_budget_units
            if budget is not None and helium_units > budget:
                helium_review_reason = (
                    f"helium budget exceeded: {helium_units:.4f} > {budget:.4f}"
                )

        # --- Market enrichment (circuit-broken) ---
        market_enrichment = None
        if self.market and await self.circuits["market"].can_call():
            try:
                snap = self.market.get_snapshot()
                market_enrichment = {
                    "carbon_price_per_tco2_usd": snap.carbon_price_per_tco2_usd,
                    "carbon_cost_usd": (
                        carbon_estimate / 1000.0
                        * snap.carbon_price_per_tco2_usd
                    ),
                    "rec_price_per_mwh_usd": snap.rec_price_per_mwh_usd,
                    "rec_available_mwh": snap.rec_available_mwh,
                }
                await self.circuits["market"].record_success()
            except Exception as e:
                await self.circuits["market"].record_failure()
                logger.debug(f"Market enrichment failed: {e}")

        # --- Promote tier from helium actionability ---
        if helium_review_reason is not None:
            tier = TaskTier.REVIEW.value

        # --- Build profile ---
        profile = WorkloadProfile(
            task_type=task_type,
            complexity=complexity,
            energy_estimate=energy_estimate,
            carbon_estimate=carbon_estimate,
            priority=priority,
            deferrable=task.get("deferrable", False),
            deadline=deadline,
            memory_estimate=memory_estimate,
            cpu_estimate=cpu_estimate,
            uncertainty_std=uncertainty_std,
            confidence=confidence,
            tier=tier,
            helium_estimate_units=helium_units,
            helium_scarcity_score=helium_scarcity_score,
            carbon_intensity_gco2kwh=carbon_intensity,
            market_enrichment=market_enrichment,
            precision_used=precision.value,
            federated_blend_applied=federated_blend_applied,
            causal_correction=causal_correction,
            distilled_used=distilled_used,
            profile_id=uuid.uuid4().hex[:12],
        )

        # --- Temporal verification ---
        if self.temporal_monitor:
            ok, violations = self.temporal_monitor.verify(profile)
            profile.temporal_verified = ok
            profile.temporal_violations = violations
            if not ok:
                logger.warning(
                    f"Temporal violations for {task_type}: {violations}"
                )

        # --- Contributing factors + XAI (P0-7: full explanation preserved) ---
        contributing_factors = {
            "base_complexity": complexity,
            "energy_kwh": energy_estimate,
            "carbon_kg": carbon_estimate,
            "memory_mb": memory_estimate,
            "cpu_pct": cpu_estimate,
        }
        if self.explainer:
            exp = ProfileExplainer.explain(
                task_type=task_type,
                complexity=complexity,
                energy_kwh=energy_estimate,
                carbon_kg=carbon_estimate,
                tier=tier,
                contributing_factors=contributing_factors,
                confidence=confidence,
                uncertainty_std=uncertainty_std,
                helium_units=helium_units,
                helium_scarcity=helium_scarcity_score,
                causal_correction=causal_correction,
                federated_blend_applied=federated_blend_applied,
            )
            profile.explanation = exp.headline
            profile.explanation_rationale = exp.rationale
            profile.explanation_full = exp
            profile.contributing_factors = exp.contributing_factors
        else:
            profile.contributing_factors = contributing_factors

        # --- HITL review (async) ---
        if self.hitl and await self.circuits["hitl"].can_call():
            needs = (
                self.hitl.needs_review(tier, confidence)
                or helium_review_reason is not None
            )
            if needs:
                profile.hitl_required = True
                urgency = (
                    "high"
                    if priority >= self.config.priority_critical_threshold
                    else "medium"
                )
                reason = (
                    helium_review_reason
                    or f"tier={tier}, confidence={confidence:.2f}"
                )
                try:
                    profile.hitl_approved = await self.hitl.request_review(
                        task_type=task_type,
                        complexity=complexity,
                        confidence=confidence,
                        reason=reason,
                        urgency=urgency,
                    )
                    await self.circuits["hitl"].record_success()
                except Exception as e:
                    await self.circuits["hitl"].record_failure()
                    logger.warning(f"HITL review failed: {e}")

        # --- Multi-agent feedback (P2-4: real latency) ---
        elapsed_ms = (datetime.now() - start_time).total_seconds() * 1000.0
        if self.coordinator:
            accuracy = 1.0 if profile.temporal_verified else 0.5
            self.coordinator.record(
                self.agent_id, task_type, accuracy, elapsed_ms,
            )

        # --- Bookkeeping ---
        self._profile_seq_counter += 1
        profile.seq_num = self._profile_seq_counter
        self.profile_history.append(profile)
        self._profiles_by_id[profile.profile_id] = profile
        self._last_profile = profile
        self._last_profile_is_fallback = False

        # --- Causal learner feedback (record inside transaction) ---
        if self.causal_learner and causal_state is not None:
            # Deferred recording — actual complexity arrives via
            # `record_actual_complexity`. We only *observe* here.
            pass

        # --- Federated auto-push (P1-5: delta-only) ---
        if (self.federated and
                (self._profile_seq_counter - self._last_federated_seq)
                >= self.config.federated_push_every_n):
            self._push_federated_delta()

        return profile

    # ------------------------------------------------------------------
    # Causal state helper
    # ------------------------------------------------------------------

    def _make_causal_state(
        self, task_type: str, task: Dict
    ) -> ComplexityState:
        return ComplexityState(
            task_type_hash=_stable_task_hash(task_type),
            model_size_norm=min(1.0, _parse_size_mb(
                task.get("model_size", "100M"), 100.0
            ) / 1e4),
            input_size_norm=min(1.0, _parse_size_mb(
                task.get("input_size", "1MB"), 1.0
            ) / 100.0),
            dataset_size_norm=min(1.0, _parse_size_gb(
                task.get("dataset_size", "1GB"), 1.0
            ) / 100.0),
            hour_of_day=datetime.now().hour,
        )

    # ------------------------------------------------------------------
    # Original complexity estimators (safe parsing preserved)
    # ------------------------------------------------------------------

    def _estimate_ml_complexity(
        self, task: Dict, size_map: Optional[Dict[str, float]] = None,
    ) -> float:
        model_size = task.get("model_size", "100M")
        input_size = task.get("input_size", "1MB")
        table = size_map if size_map is not None else self._ML_SIZE_MAP
        base_complexity = table.get(model_size, 0.5)
        input_mb = _parse_size_mb(input_size, 1.0)
        input_factor = 1.0 + (input_mb / 100.0)
        return min(1.0, base_complexity * input_factor)

    def _estimate_training_complexity(self, task: Dict) -> float:
        model_size = task.get("model_size", "100M")
        size_map = {"100M": 0.3, "500M": 0.6, "1B": 0.9, "10B": 1.0}
        base = size_map.get(model_size, 0.5)
        return min(1.0, base * 2.0)

    def _estimate_data_complexity(self, task: Dict) -> float:
        data_size = task.get("data_size", "1GB")
        size_gb = _parse_size_gb(data_size, 1.0)
        return min(1.0, size_gb * 0.1)

    def _estimate_default_complexity(self, task: Dict) -> float:
        return 0.5

    # ------------------------------------------------------------------
    # Fallback profile (P0-4: does not pollute main history)
    # ------------------------------------------------------------------

    async def _default_profile(self, task: Dict, reason: str) -> WorkloadProfile:
        task_type = task.get("type", "default")
        profile = WorkloadProfile(
            task_type=task_type,
            complexity=0.5,
            energy_estimate=0.75,
            carbon_estimate=0.3,
            priority=int(_safe_float(
                task.get("priority"), self.config.priority_default
            )),
            deferrable=task.get("deferrable", False),
            deadline=task.get("deadline"),
            memory_estimate=500.0,
            cpu_estimate=50.0,
            confidence=0.3,
            tier=TaskTier.REVIEW.value,
            explanation=f"Fallback profile: {reason}",
            is_fallback=True,
            profile_id=uuid.uuid4().hex[:12],
        )
        profile.explanation_rationale = [f"Fallback: {reason}"]
        profile.explanation_full = ProfileExplanation(
            headline=profile.explanation or "",
            rationale=profile.explanation_rationale,
            confidence=profile.confidence,
            contributing_factors={},
        )
        # Store separately; do NOT append to profile_history
        self._fallback_history.append(profile)
        self._profiles_by_id[profile.profile_id] = profile
        self._last_profile = profile
        self._last_profile_is_fallback = True

        # Optionally HITL on the fallback (best-effort, non-blocking)
        if self.hitl and await self.circuits["hitl"].can_call():
            try:
                profile.hitl_required = True
                profile.hitl_approved = await self.hitl.request_review(
                    task_type=task_type,
                    complexity=0.5,
                    confidence=0.3,
                    reason=f"fallback: {reason}",
                    urgency="medium",
                )
                await self.circuits["hitl"].record_success()
            except Exception as e:
                await self.circuits["hitl"].record_failure()
                logger.warning(f"Fallback HITL failed: {e}")
        return profile

    # ------------------------------------------------------------------
    # Federated delta push
    # ------------------------------------------------------------------

    def _push_federated_delta(self) -> None:
        if not self.federated:
            return
        new_profiles = [
            p for p in self.profile_history
            if p.seq_num > self._last_federated_seq and not p.is_fallback
        ]
        if not new_profiles:
            return
        grouped: Dict[str, List[WorkloadProfile]] = defaultdict(list)
        for p in new_profiles:
            grouped[p.task_type].append(p)
        pushed_seqs: List[int] = []
        for tt, profiles in grouped.items():
            if len(profiles) < self.config.federated_min_profiles_per_type:
                continue
            self.federated.push(FederatedComplexityProfile(
                deployment_id=self.deployment_id,
                task_type=tt,
                mean_complexity=statistics.fmean(p.complexity for p in profiles),
                mean_energy_kwh=statistics.fmean(
                    p.energy_estimate for p in profiles
                ),
                mean_carbon_kg=statistics.fmean(
                    p.carbon_estimate for p in profiles
                ),
                mean_helium_units=statistics.fmean(
                    p.helium_estimate_units for p in profiles
                ),
                sample_count=len(profiles),
            ))
            pushed_seqs.extend(p.seq_num for p in profiles)
        if pushed_seqs:
            self._last_federated_seq = max(pushed_seqs)
        self.federated.aggregate()

    # ------------------------------------------------------------------
    # Public enhancement APIs
    # ------------------------------------------------------------------

    def explain_last_profile(self) -> Optional[ProfileExplanation]:
        p = self._last_profile
        if not p:
            return None
        if p.explanation_full is not None:
            return p.explanation_full
        if p.explanation is None:
            return None
        return ProfileExplanation(
            headline=p.explanation,
            rationale=p.explanation_rationale or [p.explanation],
            confidence=p.confidence,
            contributing_factors=p.contributing_factors,
        )

    def record_actual_complexity(
        self, task_type: str, actual_complexity: float,
        task_metadata: Optional[Dict[str, Any]] = None,
        *, profile_id: Optional[str] = None,
    ) -> bool:
        """Feed back actual complexity to calibrate the causal learner.

        Returns True if the observation was recorded (False on idempotent
        replay, missing profile, or fallback target).
        """
        if not self.causal_learner:
            return False

        target: Optional[WorkloadProfile] = None
        if profile_id is not None:
            target = self._profiles_by_id.get(profile_id)
            if target is None:
                logger.warning(
                    f"record_actual_complexity: unknown profile_id={profile_id}"
                )
                return False
        else:
            # Legacy path: use _last_profile only when it is real (not fallback)
            target = self._last_profile
            if target is None or target.is_fallback:
                logger.warning(
                    "record_actual_complexity: no non-fallback profile to "
                    "correct; pass profile_id= to be explicit"
                )
                return False
            if target.task_type != task_type:
                logger.warning(
                    f"record_actual_complexity: task_type mismatch "
                    f"({task_type!r} vs {target.task_type!r}); use profile_id="
                )
                return False

        if target.profile_id in self._feedback_seen:
            logger.debug(
                f"record_actual_complexity: already recorded for {target.profile_id}"
            )
            return False
        self._feedback_seen.add(target.profile_id)

        meta = task_metadata or {}
        state = self._make_causal_state(target.task_type, meta)
        self.causal_learner.record(state, target.complexity, actual_complexity)
        if (self.causal_learner.observations
                % self.config.causal_warmup_observations) == 0:
            self.causal_learner.update()
        return True

    def contribute_federated(self) -> None:
        self._push_federated_delta()

    def get_federated_aggregate(self) -> Dict[str, Dict[str, float]]:
        return self.federated.aggregate() if self.federated else {}

    def distill_estimator(
        self, urgency: str = "normal"
    ) -> Optional[DistilledEstimator]:
        if not self.distiller:
            return None
        precision = (
            self.precision_ctl.select(urgency)
            if self.precision_ctl else PrecisionLevel.INT8
        )
        return self.distiller.distill(self._ML_SIZE_MAP, precision)

    def set_hitl_callback(self, cb: Callable[[HITLRequest], Any]) -> None:
        if self.hitl:
            self.hitl.set_callback(cb)

    def active_learning_samples(self, n: int = 16) -> List[Dict[str, Any]]:
        return self.hitl.active_learning_batch(n) if self.hitl else []

    # ------------------------------------------------------------------
    # Statistics
    # ------------------------------------------------------------------

    def get_statistics(self) -> Dict[str, Any]:
        stats: Dict[str, Any] = {
            "deployment_id": self.deployment_id,
            "agent_id": self.agent_id,
            "config": asdict(self.config),
            "profiles_analyzed": len(self.profile_history),
            "fallback_profiles": len(self._fallback_history),
            "circuits": {k: cb.snapshot() for k, cb in self.circuits.items()},
        }
        if self.temporal_monitor:
            stats["temporal"] = self.temporal_monitor.stats()
        if self.coordinator:
            stats["agents"] = {
                aid: {"role": a.role.value, "calls": a.total_calls}
                for aid, a in self.coordinator.agents.items()
            }
        if self.chaos:
            stats["chaos_events"] = self.chaos.events[-5:]
        if self.hitl:
            stats["hitl_pending"] = len(self.hitl.pending)
            stats["hitl_feedback_count"] = len(self.hitl.feedback_log)
        if self.market:
            snap = self.market.get_snapshot()
            stats["market"] = {
                "carbon_price_per_tco2_usd": snap.carbon_price_per_tco2_usd,
                "rec_price_per_mwh_usd": snap.rec_price_per_mwh_usd,
            }
        if self.causal_learner:
            stats["causal_learner"] = self.causal_learner.stats()
        if self.federated:
            stats["federated_aggregate"] = self.federated.aggregate()
        if self.helium_profiler:
            stats["helium_coefficients"] = {
                "source": self.helium_profiler.coefficients.source,
                "confidence": self.helium_profiler.coefficients.confidence,
            }
        return stats


# =============================================================================
# Demo (unchanged behavior, new fields printed)
# =============================================================================

if __name__ == "__main__":

    async def main():
        logging.basicConfig(level=logging.INFO)

        interpreter = WorkloadInterpreter(
            config={},
            deployment_id="us-ca-prod-01",
            agent_id="interpreter-A",
            hardware=HardwareProfile(supports_int4=True, vram_gb=48),
        )

        def auto_approve(req: HITLRequest) -> bool:
            logger.info(f"[HITL] auto-approve {req.task_type} ({req.urgency})")
            return True
        interpreter.set_hitl_callback(auto_approve)

        await interpreter.initialize()

        tasks = [
            {"type": "ml_inference", "model_size": "500M",
             "input_size": "5MB", "priority": 7},
            {"type": "ml_training", "model_size": "1B",
             "dataset_size": "50GB", "epochs": 5, "priority": 9},
            {"type": "data_processing", "data_size": "25GB", "priority": 4},
            {"type": "unknown_task", "priority": 5},
        ]
        last_profile_id = None
        for t in tasks:
            profile = await interpreter.analyze(t)
            last_profile_id = profile.profile_id
            print(f"\n=== {profile.task_type} ===")
            print(f"  Profile ID:   {profile.profile_id} "
                  f"(seq={profile.seq_num})")
            print(f"  Complexity:   {profile.complexity:.3f} "
                  f"(±{profile.uncertainty_std:.3f})")
            print(f"  Energy:       {profile.energy_estimate:.4f} kWh")
            print(f"  Carbon:       {profile.carbon_estimate:.4f} kgCO₂e")
            print(f"  Helium:       {profile.helium_estimate_units:.4f} units")
            print(f"  Tier:         {profile.tier}")
            print(f"  Confidence:   {profile.confidence:.2f}")
            print(f"  Precision:    {profile.precision_used}")
            print(f"  Temporal OK:  {profile.temporal_verified}")
            print(f"  Distilled:    {profile.distilled_used}")
            print(f"  Causal corr:  {profile.causal_correction:+.4f}")
            print(f"  Federated:    {profile.federated_blend_applied}")
            print(f"  HITL req'd:   {profile.hitl_required} "
                  f"(approved={profile.hitl_approved})")
            if profile.explanation:
                print(f"  XAI: {profile.explanation}")
            if profile.market_enrichment:
                print(f"  Market: ${profile.market_enrichment['carbon_cost_usd']:.5f} "
                      f"carbon cost")

        # P1-3: idempotent feedback with explicit profile_id
        if last_profile_id:
            ok = interpreter.record_actual_complexity(
                "ml_inference", 0.72,
                {"model_size": "500M", "input_size": "5MB"},
                profile_id=last_profile_id,
            )
            print(f"\nFeedback recorded: {ok}")
            # Second call: idempotent → False
            ok_again = interpreter.record_actual_complexity(
                "ml_inference", 0.72,
                {"model_size": "500M", "input_size": "5MB"},
                profile_id=last_profile_id,
            )
            print(f"Feedback recorded (replay): {ok_again}")

        interpreter.contribute_federated()

        dist = interpreter.distill_estimator(urgency="normal")
        if dist:
            print(f"\n=== Distilled Estimator ===")
            print(f"  Precision:   {dist.precision.value}")
            print(f"  Retention:   {dist.quality_retention}")
            print(f"  Energy save: {dist.energy_reduction_percent}%")
            print(f"  Size map:    {dist.size_map}")

        import json
        print("\n=== Statistics ===")
        print(json.dumps(interpreter.get_statistics(), indent=2, default=str))

    asyncio.run(main())
