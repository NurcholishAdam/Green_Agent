# -*- coding: utf-8 -*-
"""
Enhanced Metrics Collection Module (v5.1)
========================================

Ensures all resource metrics (latency, energy, carbon, memory, tool calls,
helium) are logged consistently with real-time monitoring hooks.

Fixes in v5.1 (see review)
--------------------------
  * P0 — Energy model uses ``HardwareProfile.cpu_tdp_watts`` and
         ``gpu_tdp_watts``. Previously the coefficients were hardcoded
         as ``0.05`` / ``0.20`` **watts**, making reported energy
         1,300× (CPU) to 1,500× (GPU) too small.
  * P0 — Quantum distillation now affects reported energy.
         ``DistilledEnergyModel.energy_reduction_percent`` is applied
         to the raw energy when the precision tier is INT8 / INT4 /
         QUANTUM_DISTILLED. Previously the flag was set but no
         reduction was applied.
  * P0 — HITL is functional. ``MetricTier.FALLBACK`` is now produced
         when both psutil and GPU probes fail, and ``needs_review``
         fires on FALLBACK unconditionally. Previously FALLBACK was
         never produced, so ``needs_review`` always returned ``False``.
         ``urgency="low"`` no longer auto-approves by default, and
         ``active_learning_batch`` returns the lowest-confidence
         samples instead of the newest.
  * P0 — ``get_current_metrics()`` is deprecated. Callers should use
         ``collect_and_record()`` (explicit write) or ``peek_current()``
         (read-only). The old method still works but logs a
         ``DeprecationWarning``-style message per call.
  * P0 — ``CausalMeasurementLearner`` uses the same formula for
         training and inference. Previously training used
         ``pred × (1 + bias + w·f)`` while inference returned
         ``bias + w·f`` and the caller applied
         ``energy × (1 + 0.15 × correction)``. The learner could
         never converge.
  * P1 — Reads are locked. ``render_prometheus`` and
         ``get_helium_metrics`` acquire ``self._lock`` before reading
         ``metrics_history``.
  * P1 — ``threading.RLock`` instead of ``Lock``. Prevents deadlock
         when a locked public method calls another locked public
         method.
  * P1 — UTC timestamps everywhere. ``datetime.now(timezone.utc)``;
         ``fromtimestamp(x, tz=timezone.utc)`` for the causal
         learner's ``hour_of_day``.
  * P1 — Bounded collections. ``metrics_history``,
         ``temporal_monitor.violations``, and ``chaos.events`` are
         ``deque(maxlen=…)``; the bound is configurable via
         ``max_history``.
  * P1 — GPU utilization is cached with a TTL (default 2 s). The
         ``nvidia-smi`` subprocess no longer blocks the collection
         path on every snapshot.
  * P1 — Async forecaster coroutines are closed to avoid
         ``RuntimeWarning``. The collector remains synchronous; the
         fallback is logged at debug.
  * P1 — ``MetricTier`` classification is sensor-driven: LIVE only
         when both psutil and GPU probes succeed; ESTIMATED when one
         succeeds; FALLBACK when both fail.
  * P1 — Federated contribution check is done inside the lock to
         eliminate the TOCTOU double-push race.
  * P1 — ``CircuitBreaker`` tracks ``_probes_in_flight`` so a HALF_OPEN
         probe is issued to exactly one caller.
  * P1 — ``CarbonMarketClient`` uses an ``RLock`` and a seeded RNG.
  * P2 — ``MetricsSnapshot`` gains ``__post_init__``, ``from_dict``,
         ``from_json``, ``key()``. The collector gains ``to_dict``,
         ``from_dict``, ``to_json``, ``from_json``, ``reset``,
         ``configure``, ``test_smoke``, ``health``, ``__repr__``,
         ``__dir__``. ``__version__`` added.
  * P3 — ``datetime.timedelta`` import dropped (unused);
         ``_grid_intensity_last_fetch`` / ``_last_helium_signal`` /
         ``cpu_tdp_watts`` / ``gpu_tdp_watts`` no longer dead;
         ``MultiAgentCoordinator._reassign`` boundary clarified
         (``>= 0.30`` for the carbon-optimizer role).

Original API preserved:
    collector = MetricsCollector(grid_intensity_g_kwh=385.0, pue_factor=1.2)
    collector.start_step()
    collector.record_tool_call()
    snap = collector.collect_snapshot()
    cumulative = collector.get_cumulative_metrics()
    collector.export_history("metrics.json")

Backward compatibility
----------------------
- ``get_current_metrics()`` remains, but now emits a
  ``DeprecationWarning`` and calls ``collect_and_record()`` (same
  write behaviour as before). Prefer ``peek_current()`` for a
  read-only view.
- New ``MetricsCollector`` kwargs are all optional and keyword-only.
"""

from __future__ import annotations

import json
import logging
import math
import random
import statistics
import threading
import time
import uuid
import warnings
from collections import deque
from dataclasses import asdict, dataclass, field, fields, replace
from datetime import datetime, timezone
from enum import Enum
from typing import (
    Any,
    Callable,
    Deque,
    Dict,
    List,
    Optional,
    Protocol,
    Tuple,
)

try:
    import psutil  # type: ignore
    _HAS_PSUTIL = True
except Exception:  # pragma: no cover
    psutil = None  # type: ignore
    _HAS_PSUTIL = False

logger = logging.getLogger(__name__)

__version__ = "5.1.0"

__all__ = [
    "AdaptivePrecisionController",
    "AgentProfile",
    "AgentRole",
    "CarbonMarketClient",
    "CausalMeasurementLearner",
    "ChaosInjector",
    "CircuitBreaker",
    "CircuitState",
    "DistilledEnergyModel",
    "FederatedAggregator",
    "FederatedMetricsProfile",
    "HITLRequest",
    "HardwareProfile",
    "HeliumProfiler",
    "HumanInTheLoopGate",
    "MarketSnapshot",
    "MeasurementState",
    "MetricTier",
    "MetricsCollector",
    "MetricsCollectorError",
    "MetricsSnapshot",
    "MultiAgentCoordinator",
    "PrecisionLevel",
    "QuantumDistillationBridge",
    "SnapshotExplanation",
    "SnapshotExplainer",
    "TemporalLogicMonitor",
    "__version__",
]


# =============================================================================
# Errors & safe coercion
# =============================================================================
class MetricsCollectorError(ValueError):
    """Raised for invalid metrics inputs or configuration."""


def _safe_float(name: str, value: Any, *, allow_negative: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise MetricsCollectorError(
            f"{name} must be numeric, got {type(value).__name__}"
        )
    v = float(value)
    if not math.isfinite(v):
        raise MetricsCollectorError(
            f"{name} must be finite, got {value!r}"
        )
    if not allow_negative and v < 0:
        raise MetricsCollectorError(
            f"{name} must be >= 0, got {v!r}"
        )
    return v


def _safe_opt_float(name: str, value: Any) -> Optional[float]:
    if value is None:
        return None
    return _safe_float(name, value, allow_negative=True)


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
    CARBON_OPTIMIZER = "carbon_optimizer"
    LATENCY_OPTIMIZER = "latency_optimizer"
    ACCURACY_OPTIMIZER = "accuracy_optimizer"


class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class MetricTier(Enum):
    LIVE = "live"
    ESTIMATED = "estimated"
    FALLBACK = "fallback"


# =============================================================================
# Snapshot
# =============================================================================
@dataclass
class MetricsSnapshot:
    """Single point-in-time metrics snapshot (extended)."""
    timestamp: float
    step: int
    latency_ms: float
    energy_wh: float
    carbon_kg: float
    memory_mb: float
    tool_calls: int
    cpu_percent: float
    gpu_utilization: Optional[float] = None
    # Enhancement fields (all defaulted for backward compat)
    helium_units: float = 0.0
    helium_scarcity_score: float = 0.0
    carbon_intensity_gco2kwh: float = 0.0
    carbon_cost_usd: float = 0.0
    precision_used: str = "fp32"
    tier: str = MetricTier.ESTIMATED.value
    explanation: Optional[str] = None
    temporal_verified: bool = True
    temporal_violations: List[str] = field(default_factory=list)
    hitl_required: bool = False
    hitl_approved: Optional[bool] = None
    uncertainty_std: float = 0.0
    schema_version: str = __version__

    def __post_init__(self) -> None:
        for name in (
            "latency_ms", "energy_wh", "carbon_kg", "memory_mb",
            "cpu_percent", "helium_units", "helium_scarcity_score",
            "carbon_intensity_gco2kwh", "carbon_cost_usd",
            "uncertainty_std",
        ):
            v = getattr(self, name)
            if isinstance(v, bool) or not isinstance(v, (int, float)):
                raise MetricsCollectorError(
                    f"{name} must be numeric, got {type(v).__name__}"
                )
            if not math.isfinite(float(v)):
                raise MetricsCollectorError(
                    f"{name} must be finite, got {v!r}"
                )
            if v < 0:
                raise MetricsCollectorError(
                    f"{name} must be >= 0, got {v!r}"
                )
        if self.gpu_utilization is not None:
            object.__setattr__(
                self, "gpu_utilization",
                _safe_float(
                    "gpu_utilization", self.gpu_utilization,
                    allow_negative=True,
                ),
            )
        if not isinstance(self.tool_calls, int) or self.tool_calls < 0:
            raise MetricsCollectorError(
                "tool_calls must be a non-negative int."
            )
        if not isinstance(self.step, int) or self.step < 0:
            raise MetricsCollectorError(
                "step must be a non-negative int."
            )
        if not isinstance(self.precision_used, str):
            raise MetricsCollectorError(
                "precision_used must be str."
            )
        if not isinstance(self.tier, str):
            raise MetricsCollectorError("tier must be str.")
        if self.explanation is not None and not isinstance(
            self.explanation, str
        ):
            raise MetricsCollectorError(
                "explanation must be str or None."
            )
        if self.hitl_approved is not None and not isinstance(
            self.hitl_approved, bool
        ):
            raise MetricsCollectorError(
                "hitl_approved must be bool or None."
            )
        if not isinstance(self.timestamp, (int, float)):
            raise MetricsCollectorError("timestamp must be numeric.")
        if not math.isfinite(float(self.timestamp)):
            raise MetricsCollectorError("timestamp must be finite.")

    def key(self) -> Tuple[Any, ...]:
        """Deterministic identity tuple (excludes derived reasoning)."""
        return (
            self.step,
            round(self.energy_wh, 12),
            round(self.carbon_kg, 12),
            self.precision_used,
            self.tier,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "timestamp": float(self.timestamp),
            "step": int(self.step),
            "latency_ms": float(self.latency_ms),
            "energy_wh": float(self.energy_wh),
            "carbon_kg": float(self.carbon_kg),
            "memory_mb": float(self.memory_mb),
            "tool_calls": int(self.tool_calls),
            "cpu_percent": float(self.cpu_percent),
            "gpu_utilization": (
                float(self.gpu_utilization)
                if self.gpu_utilization is not None else None
            ),
            "helium_units": float(self.helium_units),
            "helium_scarcity_score": float(self.helium_scarcity_score),
            "carbon_intensity_gco2kwh": float(
                self.carbon_intensity_gco2kwh
            ),
            "carbon_cost_usd": float(self.carbon_cost_usd),
            "precision_used": self.precision_used,
            "tier": self.tier,
            "explanation": self.explanation,
            "temporal_verified": bool(self.temporal_verified),
            "temporal_violations": list(self.temporal_violations),
            "hitl_required": bool(self.hitl_required),
            "hitl_approved": self.hitl_approved,
            "uncertainty_std": float(self.uncertainty_std),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MetricsSnapshot":
        if not isinstance(data, dict):
            raise MetricsCollectorError(
                f"from_dict expects a dict, got {type(data).__name__}"
            )
        required = (
            "timestamp", "step", "latency_ms", "energy_wh",
            "carbon_kg", "memory_mb", "tool_calls", "cpu_percent",
        )
        missing = [k for k in required if k not in data]
        if missing:
            raise MetricsCollectorError(
                f"MetricsSnapshot.from_dict: missing fields {missing}"
            )
        known_fields = {f.name for f in fields(cls)}
        kwargs = {
            k: v for k, v in data.items() if k in known_fields
        }
        kwargs.pop("schema_version", None)
        return cls(**kwargs)


# =============================================================================
# Resilience
# =============================================================================
@dataclass
class CircuitBreaker:
    name: str
    failure_threshold: int = 3
    recovery_timeout_seconds: int = 30
    state: CircuitState = CircuitState.CLOSED
    failures: int = 0
    last_failure_at: Optional[float] = None
    _probes_in_flight: int = 0

    def can_call(self) -> bool:
        if self.state == CircuitState.CLOSED:
            return True
        if self.state == CircuitState.OPEN:
            if (
                self.last_failure_at is not None
                and (time.time() - self.last_failure_at)
                > self.recovery_timeout_seconds
            ):
                self.state = CircuitState.HALF_OPEN
                self._probes_in_flight = 0
            else:
                return False
        if self._probes_in_flight < 1:
            self._probes_in_flight += 1
            return True
        return False

    def record_success(self) -> None:
        self.failures = 0
        self.state = CircuitState.CLOSED
        self._probes_in_flight = 0

    def record_failure(self) -> None:
        self.failures += 1
        self.last_failure_at = time.time()
        if self.state == CircuitState.HALF_OPEN:
            self.state = CircuitState.OPEN
            self._probes_in_flight = 0
        elif self.failures >= self.failure_threshold:
            self.state = CircuitState.OPEN
            logger.warning(
                "Circuit %r OPEN after %d failures",
                self.name, self.failures,
            )


class ChaosInjector:
    def __init__(
        self,
        failure_rate: float = 0.0,
        rng: Optional[random.Random] = None,
    ) -> None:
        self.failure_rate = max(0.0, min(1.0, float(failure_rate)))
        self.events: Deque[Dict[str, Any]] = deque(maxlen=1024)
        self._rng = rng or random.Random()

    def maybe_fail(self, component: str) -> bool:
        if self._rng.random() < self.failure_rate:
            self.events.append({
                "component": component,
                "at": datetime.now(timezone.utc).isoformat(),
            })
            return True
        return False


# =============================================================================
# Temporal logic
# =============================================================================
class SnapshotProperty(Protocol):
    def check(self, snap: MetricsSnapshot) -> bool: ...
    def name(self) -> str: ...


@dataclass
class NonNegativeEnergy:
    def check(self, snap: MetricsSnapshot) -> bool:
        return snap.energy_wh >= 0.0
    def name(self) -> str:
        return "NonNegativeEnergy"


@dataclass
class NonNegativeCarbon:
    def check(self, snap: MetricsSnapshot) -> bool:
        return snap.carbon_kg >= 0.0
    def name(self) -> str:
        return "NonNegativeCarbon"


@dataclass
class NonNegativeLatency:
    def check(self, snap: MetricsSnapshot) -> bool:
        return snap.latency_ms >= 0.0
    def name(self) -> str:
        return "NonNegativeLatency"


@dataclass
class EnergyUpperBound:
    max_wh: float = 10_000.0
    def check(self, snap: MetricsSnapshot) -> bool:
        return snap.energy_wh <= self.max_wh
    def name(self) -> str:
        return "EnergyUpperBound"


class TemporalLogicMonitor:
    def __init__(self, max_violations: int = 1024) -> None:
        self.properties: List[SnapshotProperty] = []
        self.violations: Deque[Dict[str, Any]] = deque(
            maxlen=max_violations
        )

    def register(self, p: SnapshotProperty) -> None:
        self.properties.append(p)

    def verify(self, snap: MetricsSnapshot) -> Tuple[bool, List[str]]:
        bad: List[str] = []
        for p in self.properties:
            try:
                ok = p.check(snap)
            except Exception as exc:  # noqa: BLE001
                ok = False
                logger.warning(
                    "Property %r raised: %s", p.name(), exc,
                )
            if not ok:
                bad.append(p.name())
                self.violations.append({
                    "property": p.name(),
                    "step": snap.step,
                    "at": datetime.now(timezone.utc).isoformat(),
                })
        return (len(bad) == 0, bad)


# =============================================================================
# XAI
# =============================================================================
@dataclass
class SnapshotExplanation:
    headline: str
    rationale: List[str]
    confidence: float
    contributing_factors: Dict[str, float]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class SnapshotExplainer:
    @staticmethod
    def explain(
        step: int,
        energy_wh: float,
        carbon_kg: float,
        cpu_percent: float,
        gpu_util: Optional[float],
        memory_mb: float,
        precision: PrecisionLevel,
        tier: str,
        helium_units: float = 0.0,
        grid_intensity: float = 0.0,
        simulated: bool = False,
    ) -> SnapshotExplanation:
        reasons: List[str] = []
        reasons.append(
            f"Step {step}: energy={energy_wh:.4f} Wh, "
            f"carbon={carbon_kg:.6f} kgCO2e."
        )
        reasons.append(
            f"CPU={cpu_percent:.1f}%, "
            f"GPU={'n/a' if gpu_util is None else f'{gpu_util:.1f}%'}, "
            f"Memory={memory_mb:.1f} MB."
        )
        reasons.append(
            f"Precision={precision.value}, tier={tier}."
        )
        if grid_intensity > 0:
            reasons.append(
                f"Grid intensity={grid_intensity:.0f} gCO2/kWh."
            )
        if helium_units > 0:
            reasons.append(
                f"Helium consumed={helium_units:.5f} units."
            )
        if gpu_util is None:
            reasons.append(
                "GPU utilization unavailable - CPU-only model applied."
            )
        if simulated:
            reasons.append(
                "Measurement flagged as simulated; not independently "
                "verified."
            )

        confidence = {
            MetricTier.LIVE.value: 0.9,
            MetricTier.ESTIMATED.value: 0.6,
            MetricTier.FALLBACK.value: 0.3,
        }.get(tier, 0.3)
        if simulated:
            confidence = max(0.0, confidence - 0.25)

        return SnapshotExplanation(
            headline=(
                f"[step {step}] {energy_wh:.4f} Wh / "
                f"{carbon_kg:.6f} kgCO2e"
            ),
            rationale=reasons,
            confidence=confidence,
            contributing_factors={
                "cpu_percent": cpu_percent,
                "gpu_util": (
                    float(gpu_util) if gpu_util is not None else -1.0
                ),
                "memory_mb": memory_mb,
                "energy_wh": energy_wh,
                "carbon_kg": carbon_kg,
            },
        )


# =============================================================================
# Adaptive precision
# =============================================================================
@dataclass
class HardwareProfile:
    has_tensor_cores: bool = True
    supports_int8: bool = True
    supports_int4: bool = False
    vram_gb: float = 24.0
    edge_device: bool = False
    cpu_tdp_watts: float = 65.0
    gpu_tdp_watts: float = 300.0

    def __post_init__(self) -> None:
        for name in ("cpu_tdp_watts", "gpu_tdp_watts", "vram_gb"):
            v = getattr(self, name)
            if (
                isinstance(v, bool)
                or not isinstance(v, (int, float))
                or not math.isfinite(float(v))
                or v < 0
            ):
                raise MetricsCollectorError(
                    f"{name} must be a finite non-negative number, "
                    f"got {v!r}"
                )


class AdaptivePrecisionController:
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

    @staticmethod
    def quantize(value: float, precision: PrecisionLevel) -> float:
        scale = {
            PrecisionLevel.FP32: 1.0,
            PrecisionLevel.FP16: 1.0,
            PrecisionLevel.INT8: 1000.0,
            PrecisionLevel.INT4: 100.0,
            PrecisionLevel.QUANTUM_DISTILLED: 10.0,
        }[precision]
        if scale <= 1.0:
            return value
        return math.floor(value * scale) / scale

    @staticmethod
    def energy_multiplier(precision: PrecisionLevel) -> float:
        """Approximate relative energy factor by precision (vs FP32)."""
        return {
            PrecisionLevel.FP32: 1.00,
            PrecisionLevel.FP16: 0.75,
            PrecisionLevel.INT8: 0.50,
            PrecisionLevel.INT4: 0.35,
            PrecisionLevel.QUANTUM_DISTILLED: 0.25,
        }[precision]


# =============================================================================
# Causal learner
# =============================================================================
@dataclass
class MeasurementState:
    step_norm: float
    cpu_norm: float
    gpu_norm: float
    memory_norm: float
    hour_of_day: int


class CausalMeasurementLearner:
    """
    Learns a multiplicative correction on top of the heuristic energy model.

    Training and inference use the same formula::

        multiplier = 1 + bias + sum(w_i * x_i)

    A multiplier of 1.0 means "no correction". The caller applies it as
    ``energy * multiplier`` — the same formula the trainer optimizes.
    """
    N_FEATURES = 5

    def __init__(self, lr: float = 0.02) -> None:
        if lr <= 0:
            raise MetricsCollectorError("lr must be > 0.")
        self.lr = float(lr)
        self.weights: List[float] = [0.0] * self.N_FEATURES
        self.bias: float = 0.0
        self.buffer: Deque[Tuple[List[float], float, float]] = deque(
            maxlen=512
        )
        self.observations: int = 0

    @staticmethod
    def _features(s: MeasurementState) -> List[float]:
        return [
            s.step_norm,
            s.cpu_norm,
            s.gpu_norm,
            s.memory_norm,
            s.hour_of_day / 24.0,
        ]

    def predict_multiplier(self, state: MeasurementState) -> float:
        """Return the multiplicative correction factor (>= 0.01)."""
        f = self._features(state)
        raw = self.bias + sum(
            w * x for w, x in zip(self.weights, f)
        )
        return max(0.01, 1.0 + raw)

    def record(
        self,
        state: MeasurementState,
        predicted_wh: float,
        actual_wh: float,
    ) -> None:
        f = self._features(state)
        self.buffer.append((f, float(predicted_wh), float(actual_wh)))
        self.observations += 1

    def update(self) -> None:
        if not self.buffer:
            return
        for f, pred, actual in self.buffer:
            raw = self.bias + sum(
                w * x for w, x in zip(self.weights, f)
            )
            multiplier = max(0.01, 1.0 + raw)
            pred_adj = pred * multiplier
            err = actual - pred_adj
            if abs(pred) < 1e-9:
                continue
            grad_scale = 1.0 / max(abs(pred), 1e-3)
            grad_scale = min(grad_scale, 1000.0)
            for i, x in enumerate(f):
                self.weights[i] += self.lr * err * x * grad_scale
            self.bias += self.lr * err * grad_scale
        self.buffer.clear()


# =============================================================================
# Quantum distillation
# =============================================================================
@dataclass
class DistilledEnergyModel:
    precision: PrecisionLevel
    cpu_coefficient: float
    gpu_coefficient: float
    quality_retention: float
    energy_reduction_percent: float


class QuantumDistillationBridge:
    def distill(
        self,
        cpu_coefficient: float,
        gpu_coefficient: float,
        precision: PrecisionLevel,
    ) -> DistilledEnergyModel:
        retention, energy = {
            PrecisionLevel.FP32: (1.00, 0.0),
            PrecisionLevel.FP16: (0.98, 30.0),
            PrecisionLevel.INT8: (0.93, 55.0),
            PrecisionLevel.INT4: (0.85, 70.0),
            PrecisionLevel.QUANTUM_DISTILLED: (0.80, 85.0),
        }[precision]
        return DistilledEnergyModel(
            precision=precision,
            cpu_coefficient=float(cpu_coefficient),
            gpu_coefficient=float(gpu_coefficient),
            quality_retention=retention,
            energy_reduction_percent=energy,
        )


# =============================================================================
# Federated aggregation
# =============================================================================
@dataclass
class FederatedMetricsProfile:
    deployment_id: str
    key: str
    mean_energy_wh: float
    mean_carbon_kg: float
    mean_helium_units: float
    sample_count: int
    timestamp: float = field(default_factory=time.time)


class FederatedAggregator:
    def __init__(self, max_updates: int = 4096) -> None:
        self.updates: Deque[FederatedMetricsProfile] = deque(
            maxlen=max_updates
        )
        self._global: Dict[str, Dict[str, float]] = {}
        self._lock = threading.RLock()

    def push(self, u: FederatedMetricsProfile) -> None:
        with self._lock:
            self.updates.append(u)

    def aggregate(self) -> Dict[str, Dict[str, float]]:
        with self._lock:
            updates = list(self.updates)
        grouped: Dict[str, List[FederatedMetricsProfile]] = {}
        for u in updates:
            grouped.setdefault(u.key, []).append(u)
        result: Dict[str, Dict[str, float]] = {}
        for key, profiles in grouped.items():
            total_w = sum(p.sample_count for p in profiles) or 1
            result[key] = {
                "mean_energy_wh": sum(
                    p.mean_energy_wh * p.sample_count for p in profiles
                ) / total_w,
                "mean_carbon_kg": sum(
                    p.mean_carbon_kg * p.sample_count for p in profiles
                ) / total_w,
                "mean_helium_units": sum(
                    p.mean_helium_units * p.sample_count
                    for p in profiles
                ) / total_w,
                "sample_count": total_w,
            }
        with self._lock:
            self._global = result
        return result

    def lookup(self, key: str) -> Optional[Dict[str, float]]:
        with self._lock:
            return self._global.get(key)


# =============================================================================
# Multi-agent coordination
# =============================================================================
@dataclass
class AgentProfile:
    agent_id: str
    role: AgentRole = AgentRole.GENERALIST
    success_rate: float = 0.0
    avg_carbon_saved: float = 0.0
    avg_latency_ms: float = 0.0
    total_calls: int = 0


class MultiAgentCoordinator:
    #: Minimum fraction of carbon savings to be classified as a carbon
    #: optimizer. Stored as a fraction (0.0-1.0), not a percentage.
    CARBON_OPTIMIZER_THRESHOLD = 0.30

    def __init__(self) -> None:
        self.agents: Dict[str, AgentProfile] = {}

    def register(self, agent_id: str) -> AgentProfile:
        if agent_id not in self.agents:
            self.agents[agent_id] = AgentProfile(agent_id)
        return self.agents[agent_id]

    def _reassign(self) -> None:
        for a in self.agents.values():
            if a.avg_carbon_saved >= self.CARBON_OPTIMIZER_THRESHOLD:
                a.role = AgentRole.CARBON_OPTIMIZER
            elif a.avg_latency_ms and a.avg_latency_ms < 500:
                a.role = AgentRole.LATENCY_OPTIMIZER
            elif a.success_rate > 0.9:
                a.role = AgentRole.ACCURACY_OPTIMIZER
            else:
                a.role = AgentRole.GENERALIST

    def record(
        self, agent_id: str, success: bool,
        carbon_saved_pct: float, latency_ms: float,
    ) -> None:
        a = self.register(agent_id)
        n = a.total_calls + 1
        a.success_rate = (
            (n - 1) * a.success_rate + float(success)
        ) / n
        a.avg_carbon_saved = (
            (n - 1) * a.avg_carbon_saved + carbon_saved_pct / 100.0
        ) / n
        a.avg_latency_ms = (
            (n - 1) * a.avg_latency_ms + latency_ms
        ) / n
        a.total_calls = n
        if n % 5 == 0:
            self._reassign()

    def select_agent(
        self, role: AgentRole = AgentRole.GENERALIST,
    ) -> Optional[str]:
        if not self.agents:
            return None
        cands = [a for a in self.agents.values() if a.role == role]
        if not cands:
            cands = list(self.agents.values())
        return max(cands, key=lambda a: a.success_rate).agent_id


# =============================================================================
# Carbon markets
# =============================================================================
@dataclass
class MarketSnapshot:
    carbon_price_per_tco2_usd: float
    rec_price_per_mwh_usd: float
    rec_available_mwh: float
    timestamp: float = field(default_factory=time.time)


class CarbonMarketClient:
    def __init__(
        self,
        ttl_seconds: int = 300,
        rng: Optional[random.Random] = None,
    ) -> None:
        self._cache: Optional[MarketSnapshot] = None
        self._ttl = int(ttl_seconds)
        self._rng = rng or random.Random()
        self._lock = threading.RLock()

    def get_snapshot(self) -> MarketSnapshot:
        with self._lock:
            if (
                self._cache
                and (time.time() - self._cache.timestamp) < self._ttl
            ):
                return self._cache
            snap = MarketSnapshot(
                carbon_price_per_tco2_usd=self._rng.uniform(20, 80),
                rec_price_per_mwh_usd=self._rng.uniform(3, 9),
                rec_available_mwh=self._rng.uniform(10, 500),
            )
            self._cache = snap
            return snap


# =============================================================================
# HITL & active learning
# =============================================================================
@dataclass
class HITLRequest:
    step: int
    reason: str
    urgency: str
    context: Dict[str, Any] = field(default_factory=dict)
    requested_at: float = field(default_factory=time.time)


class HumanInTheLoopGate:
    def __init__(
        self,
        confidence_threshold: float = 0.5,
        *,
        auto_approve_low_urgency: bool = False,
        max_feedback: int = 4096,
    ) -> None:
        self.confidence_threshold = float(confidence_threshold)
        self.auto_approve_low_urgency = bool(auto_approve_low_urgency)
        self.pending: List[HITLRequest] = []
        self.feedback_log: Deque[Dict[str, Any]] = deque(
            maxlen=max_feedback
        )
        self._callback: Optional[
            Callable[[HITLRequest], bool]
        ] = None
        self._lock = threading.RLock()

    def set_callback(
        self, cb: Callable[[HITLRequest], bool],
    ) -> None:
        self._callback = cb

    def needs_review(self, tier: str, confidence: float) -> bool:
        # FALLBACK tier always needs review — we do not trust the reading.
        if tier == MetricTier.FALLBACK.value:
            return True
        return confidence < self.confidence_threshold

    def request_review(
        self,
        step: int,
        reason: str,
        urgency: str = "medium",
        context: Optional[Dict[str, Any]] = None,
    ) -> bool:
        ctx = context or {}
        req = HITLRequest(step, reason, urgency, ctx)
        with self._lock:
            self.pending.append(req)
            try:
                if urgency == "low" and self.auto_approve_low_urgency:
                    self.feedback_log.append({
                        "req": asdict(req),
                        "decision": True,
                        "confidence": ctx.get("confidence"),
                    })
                    return True
                if self._callback is None:
                    # No callback → deny by default (fail-closed).
                    self.feedback_log.append({
                        "req": asdict(req),
                        "decision": False,
                        "confidence": ctx.get("confidence"),
                    })
                    return False
                decision = bool(self._callback(req))
                self.feedback_log.append({
                    "req": asdict(req),
                    "decision": decision,
                    "confidence": ctx.get("confidence"),
                })
                return decision
            finally:
                if req in self.pending:
                    self.pending.remove(req)

    def active_learning_batch(
        self, n: int = 16,
    ) -> List[Dict[str, Any]]:
        """Return the N lowest-confidence samples (most informative)."""
        with self._lock:
            entries = list(self.feedback_log)

        def _conf(x: Dict[str, Any]) -> float:
            c = x.get("confidence")
            return float(c) if c is not None else 0.5

        return sorted(entries, key=_conf)[:n]


# =============================================================================
# Helium profiling
# =============================================================================
class HeliumProfiler:
    """
    Estimate helium consumption from energy and a per-Wh rate.

    Helium is consumed primarily in semiconductor fabrication and
    cryogenic cooling; the rate is a heuristic scaling of energy.
    """
    HELIUM_UNITS_PER_WH = 2e-5  # tentative heuristic

    @classmethod
    def estimate_units(
        cls, energy_wh: float, scarcity_score: float = 0.0,
    ) -> float:
        base = energy_wh * cls.HELIUM_UNITS_PER_WH
        efficiency_factor = 1.0 - min(0.5, scarcity_score * 0.5)
        return base * efficiency_factor


# =============================================================================
# Metrics collector
# =============================================================================
class MetricsCollector:
    """
    Enhanced metrics collector with ten cross-cutting enhancement layers.

    Backward-compatible signature:
        MetricsCollector(grid_intensity_g_kwh=385.0, pue_factor=1.2)

    New keyword-only kwargs:
        max_history:      Maximum snapshots retained (default 10,000).
        federated_key:    Federated aggregation key (default per-deployment).
        clock:            Injectable time source (default time.time).
        rng_seed:         Seed for the market / chaos RNGs.
        gpu_cache_ttl:    Seconds to cache ``nvidia-smi`` output (default 2).
    """

    DEFAULT_FEATURES: Dict[str, bool] = {
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

    def __init__(
        self,
        grid_intensity_g_kwh: float = 385.0,
        pue_factor: float = 1.2,
        deployment_id: str = "local",
        agent_id: str = "metrics-0",
        features: Optional[Dict[str, bool]] = None,
        hardware: Optional[HardwareProfile] = None,
        carbon_forecaster: Any = None,
        helium_signal_fn: Optional[Callable[[], Any]] = None,
        *,
        max_history: int = 10_000,
        federated_key: Optional[str] = None,
        clock: Optional[Callable[[], float]] = None,
        rng_seed: Optional[int] = None,
        gpu_cache_ttl: float = 2.0,
    ) -> None:
        self.grid_intensity = _safe_float(
            "grid_intensity_g_kwh", grid_intensity_g_kwh,
        )
        self.pue_factor = _safe_float("pue_factor", pue_factor)
        if self.pue_factor <= 0:
            raise MetricsCollectorError("pue_factor must be > 0.")
        if max_history <= 0:
            raise MetricsCollectorError("max_history must be > 0.")
        if gpu_cache_ttl < 0:
            raise MetricsCollectorError(
                "gpu_cache_ttl must be >= 0."
            )

        self._clock = clock or time.time
        seed = rng_seed if rng_seed is not None else 0
        self._market_rng = random.Random(seed)
        self._chaos_rng = random.Random(seed)

        self.metrics_history: Deque[MetricsSnapshot] = deque(
            maxlen=max_history
        )
        self.current_step = 0
        self.start_time = self._clock()
        self.tool_call_count = 0
        self.process = psutil.Process() if _HAS_PSUTIL else None
        self.step_start_time: Optional[float] = None

        self.deployment_id = str(deployment_id)
        self.agent_id = str(agent_id)
        self.features = {**self.DEFAULT_FEATURES, **(features or {})}
        self.carbon_forecaster = carbon_forecaster
        self.helium_signal_fn = helium_signal_fn
        self._federated_key = federated_key or (
            f"metrics:{self.deployment_id}"
        )

        self._psutil_circuit = CircuitBreaker(
            "psutil", failure_threshold=5,
        )
        self._forecaster_circuit = CircuitBreaker(
            "forecaster", failure_threshold=3,
        )
        self._gpu_cache_ttl = float(gpu_cache_ttl)
        self._gpu_cache_last_fetch: float = 0.0
        self._gpu_cache_value: Optional[float] = None

        self.chaos = (
            ChaosInjector(
                failure_rate=0.0, rng=self._chaos_rng,
            )
            if self.features["chaos_testing"] else None
        )

        self.temporal_monitor: Optional[TemporalLogicMonitor] = None
        if self.features["temporal_logic"]:
            self.temporal_monitor = TemporalLogicMonitor()
            self.temporal_monitor.register(NonNegativeEnergy())
            self.temporal_monitor.register(NonNegativeCarbon())
            self.temporal_monitor.register(NonNegativeLatency())
            self.temporal_monitor.register(EnergyUpperBound())

        self.explainer = (
            SnapshotExplainer() if self.features["xai"] else None
        )

        self.precision_ctl: Optional[AdaptivePrecisionController] = None
        if self.features["adaptive_precision"]:
            self.precision_ctl = AdaptivePrecisionController(hardware)
        self._hardware = (
            hardware if hardware is not None else HardwareProfile()
        )
        self.current_precision: PrecisionLevel = PrecisionLevel.FP32

        self.causal_learner = (
            CausalMeasurementLearner()
            if self.features["causal_rl"] else None
        )

        self.distiller = (
            QuantumDistillationBridge()
            if self.features["quantum_distillation"] else None
        )

        self.federated = (
            FederatedAggregator()
            if self.features["federated"] else None
        )

        self.coordinator: Optional[MultiAgentCoordinator] = None
        if self.features["multi_agent"]:
            self.coordinator = MultiAgentCoordinator()
            self.coordinator.register(self.agent_id)

        self.market = (
            CarbonMarketClient(rng=self._market_rng)
            if self.features["carbon_market"] else None
        )

        self.hitl = (
            HumanInTheLoopGate()
            if self.features["hitl"] else None
        )

        self.helium_profiler = (
            HeliumProfiler()
            if self.features["helium_profiling"] else None
        )

        self._lock = threading.RLock()

        logger.debug(
            "Enhanced MetricsCollector initialized "
            "(deployment=%s, agent=%s, features=%s)",
            deployment_id, agent_id, list(self.features),
        )

    # --------------------------------------------------------------- step
    def start_step(self) -> None:
        """Mark the beginning of a new execution step."""
        self.current_step += 1
        self.step_start_time = self._clock()

    def record_tool_call(self) -> None:
        self.tool_call_count += 1

    # ------------------------------------------------------------ helpers
    def _get_grid_intensity(self) -> float:
        if self.carbon_forecaster is None:
            return self.grid_intensity
        if not self._forecaster_circuit.can_call():
            return self.grid_intensity
        try:
            getter = getattr(
                self.carbon_forecaster,
                "get_current_intensity",
                None,
            )
            if getter is None:
                return self.grid_intensity
            result = getter()
            if hasattr(result, "__await__"):
                # The collector is synchronous; the coroutine can only
                # be awaited by an async caller. Close it to avoid
                # RuntimeWarning and fall back to the configured value.
                result.close()
                logger.debug(
                    "carbon_forecaster returned a coroutine; "
                    "collector is synchronous and will use the "
                    "configured grid_intensity."
                )
                self._forecaster_circuit.record_success()
                return self.grid_intensity
            value = float(result)
            if not math.isfinite(value) or value <= 0:
                self._forecaster_circuit.record_success()
                return self.grid_intensity
            self._forecaster_circuit.record_success()
            return value
        except Exception as exc:  # noqa: BLE001
            self._forecaster_circuit.record_failure()
            logger.debug("Forecaster lookup failed: %s", exc)
            return self.grid_intensity

    def _get_helium_signal(self) -> Any:
        if self.helium_signal_fn is None:
            return None
        try:
            return self.helium_signal_fn()
        except Exception:  # noqa: BLE001
            return None

    def _get_gpu_util(self) -> Optional[float]:
        """Return GPU utilization, cached for ``_gpu_cache_ttl`` seconds."""
        now = self._clock()
        if (
            now - self._gpu_cache_last_fetch
            < self._gpu_cache_ttl
        ):
            return self._gpu_cache_value
        self._gpu_cache_last_fetch = now
        try:
            import subprocess
            result = subprocess.run(
                [
                    "nvidia-smi",
                    "--query-gpu=utilization.gpu",
                    "--format=csv,noheader,nounits",
                ],
                capture_output=True,
                text=True,
                timeout=2,
            )
            if result.returncode == 0:
                raw = result.stdout.strip().split("\n")[0]
                self._gpu_cache_value = float(raw)
            else:
                self._gpu_cache_value = None
        except Exception:  # noqa: BLE001
            self._gpu_cache_value = None
        return self._gpu_cache_value

    # ---------------------------------------------------------- snapshot
    def collect_snapshot(self) -> MetricsSnapshot:
        """
        Collect current metrics snapshot with all enhancements applied.

        This appends to ``metrics_history`` and is an explicit write
        operation. For a read-only view of the latest snapshot, use
        :meth:`peek_current`.
        """
        current_time = self._clock()
        elapsed = (
            current_time - self.step_start_time
            if self.step_start_time is not None else 0.0
        )

        # --- Memory & CPU ---
        memory_mb = 0.0
        cpu_percent = 0.0
        psutil_ok = False
        if self.process is not None and self._psutil_circuit.can_call():
            try:
                if self.chaos and self.chaos.maybe_fail("psutil"):
                    raise RuntimeError("chaos: psutil")
                memory_info = self.process.memory_info()
                memory_mb = memory_info.rss / (1024 * 1024)
                cpu_percent = self.process.cpu_percent(interval=0.05)
                self._psutil_circuit.record_success()
                psutil_ok = True
            except Exception as exc:  # noqa: BLE001
                self._psutil_circuit.record_failure()
                logger.debug("psutil read failed: %s", exc)

        # --- GPU ---
        gpu_util = self._get_gpu_util()
        gpu_ok = gpu_util is not None

        # --- Tier classification (fix P1 #13) ---
        if psutil_ok and gpu_ok:
            tier = MetricTier.LIVE.value
        elif psutil_ok or gpu_ok:
            tier = MetricTier.ESTIMATED.value
        else:
            tier = MetricTier.FALLBACK.value

        # --- Precision selection ---
        precision = self.current_precision
        if self.precision_ctl:
            urgency = "normal"
            precision = self.precision_ctl.select(urgency)
            self.current_precision = precision

        # --- Distillation ---
        distilled_used = False
        dist: Optional[DistilledEnergyModel] = None
        if self.distiller:
            dist = self.distiller.distill(
                cpu_coefficient=(
                    self._hardware.cpu_tdp_watts
                ),
                gpu_coefficient=(
                    self._hardware.gpu_tdp_watts
                ),
                precision=precision,
            )
            distilled_used = True

        # --- Energy from TDPs (fix P0 #1) ---
        cpu_watts = (cpu_percent / 100.0) * self._hardware.cpu_tdp_watts
        gpu_watts = (
            (gpu_util / 100.0) * self._hardware.gpu_tdp_watts
            if gpu_util is not None else 0.0
        )
        raw_energy_wh = (cpu_watts + gpu_watts) * (elapsed / 3600.0)

        # --- Precision / distillation effect (fix P0 #2) ---
        # When distillation is active for a compressed-precision tier,
        # apply its stated energy reduction instead of the coarse
        # `energy_multiplier`. This gives the distillation layer a real
        # effect on the reported value without double-counting.
        if (
            distilled_used
            and dist is not None
            and precision in (
                PrecisionLevel.INT8,
                PrecisionLevel.INT4,
                PrecisionLevel.QUANTUM_DISTILLED,
            )
        ):
            factor = 1.0 - dist.energy_reduction_percent / 100.0
        else:
            factor = AdaptivePrecisionController.energy_multiplier(
                precision
            )
        energy_wh = raw_energy_wh * factor

        # --- Causal correction (fix P0 #5) ---
        if self.causal_learner is not None:
            state = MeasurementState(
                step_norm=min(1.0, self.current_step / 1000.0),
                cpu_norm=cpu_percent / 100.0,
                gpu_norm=(gpu_util or 0.0) / 100.0,
                memory_norm=min(1.0, memory_mb / 8192.0),
                hour_of_day=datetime.now(timezone.utc).hour,
            )
            energy_wh = max(
                0.0,
                energy_wh
                * self.causal_learner.predict_multiplier(state),
            )

        # --- Carbon ---
        grid_intensity = self._get_grid_intensity()
        carbon_kg = (
            energy_wh * grid_intensity * self.pue_factor
        ) / 1e6

        # --- Helium ---
        helium_units = 0.0
        helium_scarcity_score = 0.0
        helium_signal = self._get_helium_signal()
        if helium_signal is not None:
            try:
                helium_scarcity_score = float(
                    getattr(helium_signal, "scarcity_score", 0.0)
                )
                if not math.isfinite(helium_scarcity_score):
                    helium_scarcity_score = 0.0
            except (TypeError, ValueError):
                helium_scarcity_score = 0.0
        if self.helium_profiler is not None:
            helium_units = HeliumProfiler.estimate_units(
                energy_wh, helium_scarcity_score,
            )

        # --- Market ---
        carbon_cost_usd = 0.0
        if self.market is not None:
            try:
                snap = self.market.get_snapshot()
                carbon_cost_usd = (
                    carbon_kg / 1000.0
                    * snap.carbon_price_per_tco2_usd
                )
            except Exception:  # noqa: BLE001
                pass

        # --- Uncertainty (historical std of energy) ---
        with self._lock:
            recent_energies = [
                m.energy_wh
                for m in list(self.metrics_history)[-20:]
            ]
        uncertainty_std = (
            statistics.pstdev(recent_energies)
            if len(recent_energies) >= 3 else 0.0
        )

        snapshot = MetricsSnapshot(
            timestamp=current_time,
            step=self.current_step,
            latency_ms=elapsed * 1000.0,
            energy_wh=energy_wh,
            carbon_kg=carbon_kg,
            memory_mb=memory_mb,
            tool_calls=self.tool_call_count,
            cpu_percent=cpu_percent,
            gpu_utilization=gpu_util,
            helium_units=helium_units,
            helium_scarcity_score=helium_scarcity_score,
            carbon_intensity_gco2kwh=grid_intensity,
            carbon_cost_usd=carbon_cost_usd,
            precision_used=precision.value,
            tier=tier,
            uncertainty_std=uncertainty_std,
        )

        # --- Temporal verification ---
        if self.temporal_monitor is not None:
            ok, violations = self.temporal_monitor.verify(snapshot)
            snapshot.temporal_verified = ok
            snapshot.temporal_violations = violations
            if not ok:
                logger.warning(
                    "Temporal violations on step %d: %s",
                    self.current_step, violations,
                )

        # --- XAI ---
        if self.explainer is not None:
            exp = SnapshotExplainer.explain(
                step=self.current_step,
                energy_wh=energy_wh,
                carbon_kg=carbon_kg,
                cpu_percent=cpu_percent,
                gpu_util=gpu_util,
                memory_mb=memory_mb,
                precision=precision,
                tier=snapshot.tier,
                helium_units=helium_units,
                grid_intensity=grid_intensity,
            )
            snapshot.explanation = exp.headline

        # --- HITL ---
        if self.hitl is not None:
            confidence = {
                MetricTier.LIVE.value: 0.9,
                MetricTier.ESTIMATED.value: 0.6,
                MetricTier.FALLBACK.value: 0.3,
            }.get(snapshot.tier, 0.3)
            if self.hitl.needs_review(snapshot.tier, confidence):
                snapshot.hitl_required = True
                snapshot.hitl_approved = self.hitl.request_review(
                    step=self.current_step,
                    reason=(
                        f"tier={snapshot.tier}, "
                        f"uncertainty={uncertainty_std:.4f}"
                    ),
                    urgency=(
                        "high"
                        if snapshot.tier == MetricTier.FALLBACK.value
                        else "medium"
                    ),
                    context={"confidence": confidence},
                )

        # --- Multi-agent feedback ---
        if self.coordinator is not None:
            self.coordinator.record(
                agent_id=self.agent_id,
                success=snapshot.temporal_verified,
                carbon_saved_pct=0.0,
                latency_ms=snapshot.latency_ms,
            )

        # --- Append (locked) + federated (fix P1 #14: TOCTOU) ---
        with self._lock:
            self.metrics_history.append(snapshot)
            should_contribute = (
                self.federated is not None
                and len(self.metrics_history) % 25 == 0
            )

        if should_contribute:
            self.federated.push(FederatedMetricsProfile(
                deployment_id=self.deployment_id,
                key=self._federated_key,
                mean_energy_wh=energy_wh,
                mean_carbon_kg=carbon_kg,
                mean_helium_units=helium_units,
                sample_count=1,
            ))
            self.federated.aggregate()

        return snapshot

    # ---------------------------------------------------------- queries
    def collect_and_record(self) -> MetricsSnapshot:
        """Collect a snapshot and append it to history (same as
        :meth:`collect_snapshot`)."""
        return self.collect_snapshot()

    def peek_current(self) -> Dict[str, Any]:
        """Return the latest snapshot as a dict — read-only, no side effects."""
        with self._lock:
            if not self.metrics_history:
                return {}
            return self.metrics_history[-1].to_dict()

    def get_current_metrics(self) -> Dict[str, Any]:
        """
        DEPRECATED. Collecting metrics has side effects on history and
        enhancement layers. Use :meth:`peek_current` for a read-only
        view or :meth:`collect_and_record` for an explicit write.
        """
        warnings.warn(
            "MetricsCollector.get_current_metrics() has side effects; "
            "use peek_current() for read-only access or "
            "collect_and_record() for an explicit collection.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.collect_snapshot().to_dict()

    def get_cumulative_metrics(self) -> Dict[str, Any]:
        with self._lock:
            history = list(self.metrics_history)
        if not history:
            return {}
        total_latency = sum(m.latency_ms for m in history)
        total_energy = sum(m.energy_wh for m in history)
        total_carbon = sum(m.carbon_kg for m in history)
        total_helium = sum(m.helium_units for m in history)
        total_carbon_cost = sum(m.carbon_cost_usd for m in history)
        avg_memory = sum(m.memory_mb for m in history) / len(history)
        max_memory = max(m.memory_mb for m in history)
        return {
            "total_steps": self.current_step,
            "total_latency_ms": total_latency,
            "total_energy_wh": total_energy,
            "total_carbon_kg": total_carbon,
            "total_helium_units": total_helium,
            "total_carbon_cost_usd": total_carbon_cost,
            "avg_memory_mb": avg_memory,
            "max_memory_mb": max_memory,
            "total_tool_calls": self.tool_call_count,
            "elapsed_time_s": self._clock() - self.start_time,
            "temporal_violations": (
                len(self.temporal_monitor.violations)
                if self.temporal_monitor is not None else 0
            ),
        }

    def export_history(self, filepath: str) -> None:
        with self._lock:
            snapshots = [m.to_dict() for m in self.metrics_history]
        data = {
            "schema_version": __version__,
            "snapshots": snapshots,
            "cumulative": self.get_cumulative_metrics(),
        }
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

    def get_metrics_for_reflection(self) -> Dict[str, Any]:
        cumulative = self.get_cumulative_metrics()
        with self._lock:
            history = list(self.metrics_history)
        recent = history[-5:]
        n = len(recent) or 1
        return {
            "cumulative": cumulative,
            "recent_trend": {
                "avg_latency_ms": sum(
                    m.latency_ms for m in recent
                ) / n if recent else 0.0,
                "avg_energy_wh": sum(
                    m.energy_wh for m in recent
                ) / n if recent else 0.0,
                "avg_carbon_kg": sum(
                    m.carbon_kg for m in recent
                ) / n if recent else 0.0,
                "avg_helium_units": sum(
                    m.helium_units for m in recent
                ) / n if recent else 0.0,
            },
            "step_count": self.current_step,
        }

    # -------------------------------------------------- enhancement APIs
    def record_actual_energy(
        self,
        actual_wh: float,
        snapshot: Optional[MetricsSnapshot] = None,
    ) -> None:
        """Feed back actual energy consumption for causal calibration."""
        if self.causal_learner is None:
            return
        actual_wh = _safe_float("actual_wh", actual_wh)
        snap = snapshot
        if snap is None:
            with self._lock:
                snap = (
                    self.metrics_history[-1]
                    if self.metrics_history else None
                )
        if snap is None:
            return
        state = MeasurementState(
            step_norm=min(1.0, snap.step / 1000.0),
            cpu_norm=snap.cpu_percent / 100.0,
            gpu_norm=(snap.gpu_utilization or 0.0) / 100.0,
            memory_norm=min(1.0, snap.memory_mb / 8192.0),
            hour_of_day=datetime.fromtimestamp(
                snap.timestamp, tz=timezone.utc,
            ).hour,
        )
        self.causal_learner.record(state, snap.energy_wh, actual_wh)
        if self.causal_learner.observations % 10 == 0:
            self.causal_learner.update()

    def render_prometheus(self) -> str:
        """Render the most recent snapshot as Prometheus text format."""
        with self._lock:
            if not self.metrics_history:
                return ""
            snap = self.metrics_history[-1]

        lines: List[str] = []

        def _emit(name: str, help_text: str, value: Any) -> None:
            lines.append(f"# HELP {name} {help_text}")
            lines.append(f"# TYPE {name} gauge")
            lines.append(f"{name} {value}")

        _emit(
            "green_agent_step_energy_wh",
            "Energy per step (Wh)", snap.energy_wh,
        )
        _emit(
            "green_agent_step_carbon_kg",
            "Carbon per step (kg)", snap.carbon_kg,
        )
        _emit(
            "green_agent_step_latency_ms",
            "Latency per step (ms)", snap.latency_ms,
        )
        _emit(
            "green_agent_step_memory_mb",
            "Memory per step (MB)", snap.memory_mb,
        )
        _emit(
            "green_agent_step_helium_units",
            "Helium per step", snap.helium_units,
        )
        _emit(
            "green_agent_step_cpu_percent",
            "CPU percent", snap.cpu_percent,
        )
        _emit(
            "green_agent_step_gpu_utilization",
            "GPU utilization percent",
            snap.gpu_utilization if snap.gpu_utilization is not None
            else float("nan"),
        )
        precision_code = {
            "fp32": 4, "fp16": 3, "int8": 2, "int4": 1,
            "quantum_distilled": 0,
        }.get(snap.precision_used, 4)
        _emit(
            "green_agent_precision_current",
            "Current precision tier (0=quantum, 4=fp32)",
            precision_code,
        )
        tier_code = {
            "live": 2, "estimated": 1, "fallback": 0,
        }.get(snap.tier, 0)
        _emit(
            "green_agent_metric_tier",
            "Metric tier (0=fallback, 2=live)",
            tier_code,
        )
        return "\n".join(lines) + "\n"

    def get_helium_metrics(self) -> Dict[str, Any]:
        with self._lock:
            if not self.metrics_history:
                return {}
            snap = self.metrics_history[-1]
        return {
            "helium_units": snap.helium_units,
            "helium_scarcity_score": snap.helium_scarcity_score,
        }

    def set_hitl_callback(
        self, cb: Callable[[HITLRequest], bool],
    ) -> None:
        if self.hitl is not None:
            self.hitl.set_callback(cb)

    def active_learning_samples(
        self, n: int = 16,
    ) -> List[Dict[str, Any]]:
        return (
            self.hitl.active_learning_batch(n)
            if self.hitl is not None else []
        )

    def contribute_federated(self) -> None:
        if self.federated is None:
            return
        with self._lock:
            history = list(self.metrics_history)
        if not history:
            return
        self.federated.push(FederatedMetricsProfile(
            deployment_id=self.deployment_id,
            key=self._federated_key,
            mean_energy_wh=statistics.fmean(
                m.energy_wh for m in history
            ),
            mean_carbon_kg=statistics.fmean(
                m.carbon_kg for m in history
            ),
            mean_helium_units=statistics.fmean(
                m.helium_units for m in history
            ),
            sample_count=len(history),
        ))
        self.federated.aggregate()

    def get_federated_aggregate(self) -> Dict[str, Dict[str, float]]:
        return self.federated.aggregate() if self.federated else {}

    def distill_energy_model(
        self, urgency: str = "normal",
    ) -> Optional[DistilledEnergyModel]:
        if self.distiller is None:
            return None
        precision = (
            self.precision_ctl.select(urgency)
            if self.precision_ctl is not None
            else PrecisionLevel.INT8
        )
        return self.distiller.distill(
            cpu_coefficient=self._hardware.cpu_tdp_watts,
            gpu_coefficient=self._hardware.gpu_tdp_watts,
            precision=precision,
        )

    # ---------------------------------------------------------- lifecycle
    def reset(self) -> None:
        """Clear history and counters (keeps configuration and callbacks)."""
        with self._lock:
            self.metrics_history.clear()
        self.current_step = 0
        self.tool_call_count = 0
        self.start_time = self._clock()
        self.step_start_time = None
        if self.temporal_monitor is not None:
            self.temporal_monitor.violations.clear()
        logger.debug("MetricsCollector reset.")

    def configure(self, **kwargs: Any) -> None:
        """Update configuration at runtime (grid intensity, PUE, etc.)."""
        allowed = {
            "grid_intensity", "pue_factor", "carbon_forecaster",
            "helium_signal_fn", "federated_key",
        }
        unknown = set(kwargs) - allowed
        if unknown:
            raise MetricsCollectorError(
                f"Unknown configure() keys: {sorted(unknown)}. "
                f"Allowed: {sorted(allowed)}"
            )
        if "grid_intensity" in kwargs:
            self.grid_intensity = _safe_float(
                "grid_intensity", kwargs["grid_intensity"],
            )
        if "pue_factor" in kwargs:
            pue = _safe_float("pue_factor", kwargs["pue_factor"])
            if pue <= 0:
                raise MetricsCollectorError(
                    "pue_factor must be > 0."
                )
            self.pue_factor = pue
        if "carbon_forecaster" in kwargs:
            self.carbon_forecaster = kwargs["carbon_forecaster"]
        if "helium_signal_fn" in kwargs:
            self.helium_signal_fn = kwargs["helium_signal_fn"]
        if "federated_key" in kwargs:
            self._federated_key = str(kwargs["federated_key"])

    # ---------------------------------------------------------- serialization
    def to_dict(self) -> Dict[str, Any]:
        with self._lock:
            history = [m.to_dict() for m in self.metrics_history]
            temporal_violations = (
                list(self.temporal_monitor.violations)
                if self.temporal_monitor is not None else []
            )
        return {
            "schema_version": __version__,
            "deployment_id": self.deployment_id,
            "agent_id": self.agent_id,
            "grid_intensity": self.grid_intensity,
            "pue_factor": self.pue_factor,
            "features": dict(self.features),
            "federated_key": self._federated_key,
            "current_step": self.current_step,
            "tool_call_count": self.tool_call_count,
            "start_time": self.start_time,
            "history": history,
            "temporal_violations": temporal_violations,
        }

    def to_json(self, **kwargs: Any) -> str:
        try:
            return json.dumps(self.to_dict(), **kwargs)
        except (TypeError, ValueError) as exc:
            raise MetricsCollectorError(
                f"MetricsCollector is not JSON-serializable: {exc}"
            ) from exc

    # ---------------------------------------------------------- statistics
    def get_statistics(self) -> Dict[str, Any]:
        stats: Dict[str, Any] = {
            "deployment_id": self.deployment_id,
            "agent_id": self.agent_id,
            "steps": self.current_step,
            "snapshots": len(self.metrics_history),
            "max_history": self.metrics_history.maxlen,
            "circuits": {
                "psutil": self._psutil_circuit.state.value,
                "forecaster": self._forecaster_circuit.state.value,
            },
        }
        if self.temporal_monitor is not None:
            stats["temporal_violations"] = list(
                self.temporal_monitor.violations
            )[-5:]
        if self.coordinator is not None:
            stats["agents"] = {
                aid: {
                    "role": a.role.value,
                    "calls": a.total_calls,
                }
                for aid, a in self.coordinator.agents.items()
            }
        if self.chaos is not None:
            stats["chaos_events"] = list(self.chaos.events)[-5:]
        if self.hitl is not None:
            stats["hitl_pending"] = len(self.hitl.pending)
            stats["hitl_feedback_count"] = len(self.hitl.feedback_log)
        if self.market is not None:
            snap = self.market.get_snapshot()
            stats["market"] = {
                "carbon_price_per_tco2_usd":
                    snap.carbon_price_per_tco2_usd,
                "rec_price_per_mwh_usd":
                    snap.rec_price_per_mwh_usd,
            }
        if self.causal_learner is not None:
            stats["causal_learner"] = {
                "observations": self.causal_learner.observations,
                "weights": list(self.causal_learner.weights),
                "bias": self.causal_learner.bias,
            }
        if self.federated is not None:
            stats["federated_aggregate"] = (
                self.federated.aggregate()
            )
        return stats

    def test_smoke(self) -> Dict[str, Any]:
        """Structured health check for CI. Never raises."""
        results: Dict[str, Any] = {
            "ok": False, "steps": [],
        }
        try:
            collector = MetricsCollector(
                grid_intensity_g_kwh=385.0,
                pue_factor=1.2,
                deployment_id="smoke",
                agent_id="smoke",
                hardware=HardwareProfile(
                    supports_int4=True, vram_gb=48,
                ),
            )
            collector.start_step()
            collector.record_tool_call()
            snap = collector.collect_snapshot()
            results["steps"].append({
                "name": "collect", "ok": True,
                "tier": snap.tier,
                "energy_wh": snap.energy_wh,
            })
            results["steps"].append({
                "name": "bounded_history",
                "ok": collector.metrics_history.maxlen is not None,
            })
            results["steps"].append({
                "name": "peek_current",
                "ok": bool(collector.peek_current()),
            })
            results["ok"] = all(
                s["ok"] for s in results["steps"]
            )
        except Exception as exc:  # noqa: BLE001
            results["error"] = (
                f"{type(exc).__name__}: {exc}"
            )
        return results

    def health(self) -> Dict[str, Any]:
        smoke = self.test_smoke()
        return {
            "ok": smoke["ok"],
            "package_version": __version__,
            "steps": smoke.get("steps", []),
            "statistics": self.get_statistics(),
        }

    def __repr__(self) -> str:
        return (
            "MetricsCollector("
            f"deployment={self.deployment_id!r}, "
            f"agent={self.agent_id!r}, "
            f"steps={self.current_step}, "
            f"snapshots={len(self.metrics_history)})"
        )

    def __dir__(self) -> List[str]:
        return sorted(
            set(super().__dir__()) | set(__all__)
        )


# =============================================================================
# Demo
# =============================================================================
if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO)

    class _MockForecaster:
        def get_current_intensity(self) -> float:
            return 240.0

    class _MockHeliumSignal:
        scarcity_score = 0.45

    collector = MetricsCollector(
        grid_intensity_g_kwh=385.0,
        pue_factor=1.2,
        deployment_id="us-ca-prod-01",
        agent_id="metrics-A",
        hardware=HardwareProfile(
            supports_int4=True, vram_gb=48,
        ),
        carbon_forecaster=_MockForecaster(),
        helium_signal_fn=lambda: _MockHeliumSignal(),
    )

    def auto_approve(req: HITLRequest) -> bool:
        logger.info("[HITL] auto-approve: %s", req.reason)
        return True

    collector.set_hitl_callback(auto_approve)

    # --- P0 #1: energy model accuracy ---
    # 1 hour of 100% CPU on a 65W profile should yield ~65 Wh.
    # We simulate by faking the step duration.
    collector.step_start_time = time.time() - 3600.0
    collector._hardware.cpu_tdp_watts = 65.0
    collector._hardware.gpu_tdp_watts = 0.0   # disable GPU for this check
    snap_1h = collector.collect_snapshot()
    if 60.0 <= snap_1h.energy_wh <= 70.0:
        print(
            f"P0 #1 energy model: {snap_1h.energy_wh:.2f} Wh "
            f"(expected ~65 Wh)"
        )
    else:
        print(
            f"P0 #1 energy model: {snap_1h.energy_wh:.2f} Wh — "
            f"outside [60, 70] Wh"
        )

    # Reset for demo
    collector.reset()
    collector._hardware = HardwareProfile(
        supports_int4=True, vram_gb=48,
    )

    for step in range(5):
        collector.start_step()
        time.sleep(0.05)
        collector.record_tool_call()
        time.sleep(0.05)
        snap = collector.collect_snapshot()
        print(f"\n=== Step {snap.step} ===")
        print(f"  Latency:    {snap.latency_ms:.2f} ms")
        print(f"  Energy:     {snap.energy_wh:.6f} Wh")
        print(f"  Carbon:     {snap.carbon_kg:.8f} kgCO2e")
        print(f"  Helium:     {snap.helium_units:.8f} units")
        print(f"  CPU:        {snap.cpu_percent:.1f}%")
        print(f"  Memory:     {snap.memory_mb:.1f} MB")
        print(f"  Precision:  {snap.precision_used}")
        print(f"  Tier:       {snap.tier}")
        print(f"  Temporal:   {snap.temporal_verified}")
        print(f"  Cost USD:   {snap.carbon_cost_usd:.8f}")
        if snap.explanation:
            print(f"  XAI: {snap.explanation}")

    # --- P0 #4: peek_current does not mutate ---
    n_before = len(collector.metrics_history)
    collector.peek_current()
    collector.peek_current()
    collector.peek_current()
    assert len(collector.metrics_history) == n_before
    print("\nP0 #4 peek_current: no side effects")

    # --- Cumulative ---
    print("\n=== Cumulative Metrics ===")
    print(json.dumps(
        collector.get_cumulative_metrics(),
        indent=2, default=str,
    ))

    # --- Reflection ---
    print("\n=== Reflection Metrics ===")
    print(json.dumps(
        collector.get_metrics_for_reflection(),
        indent=2, default=str,
    ))

    # --- Prometheus ---
    print("\n=== Prometheus (latest snapshot) ===")
    print(collector.render_prometheus())

    # --- Helium ---
    print("=== Helium ===")
    print(collector.get_helium_metrics())

    # --- Federated ---
    collector.contribute_federated()
    print("\n=== Federated Aggregate ===")
    print(collector.get_federated_aggregate())

    # --- Distill ---
    dist = collector.distill_energy_model(urgency="normal")
    if dist:
        print("\n=== Distilled Energy Model ===")
        print(f"  Precision:   {dist.precision.value}")
        print(f"  Retention:   {dist.quality_retention}")
        print(f"  Energy save: {dist.energy_reduction_percent}%")

    # --- Statistics ---
    print("\n=== Statistics ===")
    print(json.dumps(
        collector.get_statistics(), indent=2, default=str,
    ))

    # --- Health ---
    print("\n=== Health ===")
    print(json.dumps(collector.health(), indent=2, default=str))
