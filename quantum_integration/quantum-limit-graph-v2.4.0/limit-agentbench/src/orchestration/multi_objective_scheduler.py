"""
Multi-Objective Scheduler (Enhanced, v2)
=========================================

Optimizes task scheduling across competing objectives:
  - Carbon intensity (minimize)                 [gross or net-after-credits]
  - Energy consumption (minimize)
  - Cost (minimize)
  - Performance/latency (minimize, priority-weighted)
  - Helium consumption (minimize)               ← first-class objective
  - Carbon market cost (minimize)               ← first-class objective

First-class support for the ten Green Agent enhancement layers:
  1. Quantum-Distillation of the estimator
  2. Causal RL for adaptive objective weights
  3. Federated Green Learning across deployments
  4. Multi-Agent Coordination with emergent role specialisation
  5. Temporal Logic & Formal Verification for scheduling safety
  6. Explainable AI (XAI) for every scheduling decision
  7. Adaptive Precision Switching as a scheduling dimension
  8. Carbon Markets & REC enrichment
  9. Resilience Engineering (circuit breaker + chaos testing)
 10. Human-in-the-Loop for critical deferrals + active learning

v2 fixes:
  * Stable (SHA-256) feature hashing → reproducible RL across restarts
  * Reward-weighted regression with per-bin baseline → CausalWeightLearner
    actually learns
  * Net vs gross carbon tracked separately (market arbitrage is transparent)
  * Pluggable objectives via ObjectiveSpec registry
  * Timeouts on every external call; schedule-level budget + fallback
  * Configurable chaos injection
  * Async HITL with timeout + policy default
  * Metrics + audit-log hooks (drop-in replaceable)
  * Bounded histories; health() endpoint; timezone-safe datetimes

Location: src/orchestration/multi_objective_scheduler.py
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import math
import random
import statistics
import time
from collections import deque, defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import (
    Any, Callable, Deque, Dict, List, Optional, Protocol, Tuple, Literal,
)

try:
    import numpy as np  # type: ignore
    _HAS_NUMPY = True
except Exception:  # pragma: no cover
    np = None  # type: ignore
    _HAS_NUMPY = False

logger = logging.getLogger(__name__)


# =============================================================================
# Helpers
# =============================================================================

def _utcnow() -> datetime:
    """Timezone-aware 'now'. Use everywhere internally."""
    return datetime.now(timezone.utc)


def _ensure_aware(dt: datetime) -> datetime:
    """Normalize naive datetimes to UTC to avoid comparison errors."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def stable_hash(s: str) -> float:
    """
    Deterministic, process-independent hash in [0, 1).
    Fixes the PYTHONHASHSEED-nondeterminism of built-in hash() for strings.
    """
    h = hashlib.sha256(s.encode("utf-8")).digest()
    return int.from_bytes(h[:8], "big") / 2**64


# =============================================================================
# Enums
# =============================================================================

class ExecutionMode(Enum):
    IMMEDIATE = "immediate"
    DEFERRED = "deferred"
    REALLOCATED = "reallocated"
    GREEN_ROUTED = "green_routed"
    PRECISION_THROTTLED = "precision_throttled"
    MARKET_ARBITRAGED = "market_arbitraged"


class PrecisionLevel(Enum):
    FP32 = "fp32"
    FP16 = "fp16"
    INT8 = "int8"
    INT4 = "int4"
    QUANTUM_DISTILLED = "quantum_distilled"


class AgentRole(Enum):
    GENERALIST = "generalist"
    SHORT_HORIZON_SPECIALIST = "short_horizon_specialist"
    LONG_HORIZON_SPECIALIST = "long_horizon_specialist"
    CROSS_REGION_SPECIALIST = "cross_region_specialist"
    MARKET_SPECIALIST = "market_specialist"


class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


Direction = Literal["min", "max"]


# =============================================================================
# Config
# =============================================================================

@dataclass
class SchedulerConfig:
    """All tunable knobs in one place."""
    electricity_cost_per_kwh: Dict[str, float] = field(default_factory=lambda: {
        "US-CA": 0.20, "US-NY": 0.18, "EU-DE": 0.25, "EU-FR": 0.22,
    })

    # Timeouts (seconds)
    forecaster_timeout_s: float = 2.0
    profiler_timeout_s: float = 3.0
    cluster_timeout_s: float = 1.0
    hitl_timeout_s: float = 60.0
    schedule_timeout_s: float = 5.0

    # History bounds
    history_maxlen: int = 10_000
    hitl_feedback_maxlen: int = 1_000
    temporal_violation_maxlen: int = 500
    chaos_event_maxlen: int = 100
    audit_maxlen: int = 10_000

    # Resilience
    circuit_failure_threshold: int = 3
    circuit_recovery_timeout_s: int = 30
    chaos_failure_rate: float = 0.0

    # Market
    market_ttl_s: int = 300

    # Causal learner
    causal_lr: float = 0.02
    causal_baseline_decay: float = 0.95
    causal_update_every: int = 10
    causal_buffer_maxlen: int = 512

    # Federated
    federated_min_samples: int = 5
    federated_push_every: int = 5

    # Risk
    risk_aversion_kappa: float = 0.0  # 0 disables risk-adjusted scoring

    # HITL policy
    hitl_deferral_threshold_hours: float = 24.0
    hitl_uncertainty_threshold: float = 0.6
    hitl_market_cost_threshold_usd: float = 5.0
    hitl_first_n_in_region: int = 3
    hitl_default_on_timeout: bool = False


# =============================================================================
# Objectives registry
# =============================================================================

@dataclass
class ObjectiveSpec:
    """One scored objective, pluggable."""
    name: str                     # weight key, e.g. "alpha"
    extract: Callable[["SchedulingOption"], float]
    direction: Direction
    default_weight: float = 0.0
    transform: Optional[Callable[[float, Dict[str, Any]], float]] = None


def default_objectives() -> List[ObjectiveSpec]:
    return [
        # Net carbon is the correct signal once market credits are in play.
        ObjectiveSpec(
            name="alpha",
            extract=lambda o: (o.carbon_kgco2e_net
                               if o.carbon_kgco2e_net is not None
                               else o.carbon_kgco2e),
            direction="min", default_weight=0.40,
        ),
        ObjectiveSpec(
            name="beta",
            extract=lambda o: o.energy_kwh,
            direction="min", default_weight=0.25,
        ),
        ObjectiveSpec(
            name="gamma",
            extract=lambda o: o.cost_usd,
            direction="min", default_weight=0.10,
        ),
        ObjectiveSpec(
            name="delta",
            extract=lambda o: o.latency_hours,
            direction="min", default_weight=0.10,
            # Latency is softened by urgency; low priority → still ok to delay.
            transform=lambda nv, task: nv * (1.0 - float(task.get("priority", 0.5))),
        ),
        ObjectiveSpec(
            name="epsilon",
            extract=lambda o: o.helium_units,
            direction="min", default_weight=0.05,
        ),
        ObjectiveSpec(
            name="zeta",
            extract=lambda o: o.carbon_market_cost_usd,
            direction="min", default_weight=0.10,
        ),
    ]


# =============================================================================
# Dataclasses
# =============================================================================

@dataclass
class SchedulingOption:
    mode: ExecutionMode
    node_id: str
    region: str
    start_time: datetime
    carbon_kgco2e: float
    energy_kwh: float
    cost_usd: float
    latency_hours: float
    score: float

    # Gross vs net carbon: net = gross − purchased credits.
    carbon_kgco2e_net: Optional[float] = None

    helium_units: float = 0.0
    helium_scarcity_score: float = 0.0
    carbon_market_cost_usd: float = 0.0
    precision: str = "fp32"
    confidence: float = 1.0
    uncertainty_std: float = 0.0
    temporal_verified: bool = True
    temporal_violations: List[str] = field(default_factory=list)
    explanation: Optional[str] = None
    distilled_used: bool = False
    causally_adjusted: bool = False
    hitl_required: bool = False
    hitl_approved: Optional[bool] = None
    federated_blend_applied: bool = False

    def __post_init__(self) -> None:
        if self.carbon_kgco2e_net is None:
            self.carbon_kgco2e_net = self.carbon_kgco2e
        self.start_time = _ensure_aware(self.start_time)


@dataclass
class SchedulingDecision:
    task_id: str
    chosen_option: SchedulingOption
    alternatives: List[SchedulingOption]
    reasoning: str
    pareto_efficient: bool

    explanation: Optional[Dict[str, Any]] = None
    temporal_verified: bool = True
    temporal_violations: List[str] = field(default_factory=list)
    market_enrichment: Optional[Dict[str, Any]] = None
    hitl_required: bool = False
    hitl_approved: Optional[bool] = None
    adaptive_weights_used: Optional[Dict[str, float]] = None
    causal_policy_used: bool = False
    federated_blend_applied: bool = False
    circuits: Dict[str, str] = field(default_factory=dict)
    degraded: bool = False
    degradation_reason: Optional[str] = None
    at: datetime = field(default_factory=_utcnow)


# =============================================================================
# Resilience — Circuit Breaker + Chaos Injector
# =============================================================================

@dataclass
class CircuitBreaker:
    name: str
    failure_threshold: int = 3
    recovery_timeout_s: int = 30
    state: CircuitState = CircuitState.CLOSED
    failures: int = 0
    last_failure_at: Optional[datetime] = None

    def can_call(self) -> bool:
        if self.state == CircuitState.CLOSED:
            return True
        if self.state == CircuitState.OPEN:
            if (self.last_failure_at and
                    (_utcnow() - self.last_failure_at).total_seconds()
                    > self.recovery_timeout_s):
                self.state = CircuitState.HALF_OPEN
                return True
            return False
        return True

    def record_success(self) -> None:
        self.failures = 0
        self.state = CircuitState.CLOSED

    def record_failure(self) -> None:
        self.failures += 1
        self.last_failure_at = _utcnow()
        if self.failures >= self.failure_threshold:
            self.state = CircuitState.OPEN
            logger.warning(f"Circuit '{self.name}' OPEN after {self.failures} failures")


class ChaosInjector:
    """Injects controlled faults for resilience testing."""
    def __init__(self, failure_rate: float = 0.0, maxlen: int = 100) -> None:
        self.failure_rate = failure_rate
        self.events: Deque[Dict[str, Any]] = deque(maxlen=maxlen)

    def maybe_fail(self, component: str) -> bool:
        if self.failure_rate > 0 and random.random() < self.failure_rate:
            self.events.append({
                "component": component, "at": _utcnow().isoformat(),
            })
            return True
        return False


# =============================================================================
# ENHANCEMENT 5 — Temporal Logic
# =============================================================================

class SchedulingProperty(Protocol):
    def check(self, option: SchedulingOption, task: Dict[str, Any]) -> bool: ...
    def name(self) -> str: ...


@dataclass
class DeadlineRespected:
    def check(self, option: SchedulingOption, task: Dict[str, Any]) -> bool:
        deadline = task.get("deadline")
        if deadline is None:
            return True
        deadline = _ensure_aware(deadline)
        completion = _ensure_aware(option.start_time) + timedelta(hours=option.latency_hours)
        return completion <= deadline

    def name(self) -> str:
        return "DeadlineRespected"


@dataclass
class EnergyBudgetNotExceeded:
    default_max_kwh: float = 100.0
    def check(self, option: SchedulingOption, task: Dict[str, Any]) -> bool:
        cap = float(task.get("energy_budget_kwh", self.default_max_kwh))
        return option.energy_kwh <= cap

    def name(self) -> str:
        return "EnergyBudgetNotExceeded"


@dataclass
class CarbonBudgetNotExceeded:
    default_max_kg: float = 5.0
    def check(self, option: SchedulingOption, task: Dict[str, Any]) -> bool:
        cap = float(task.get("carbon_budget_kg", self.default_max_kg))
        net = option.carbon_kgco2e_net if option.carbon_kgco2e_net is not None else option.carbon_kgco2e
        return net <= cap

    def name(self) -> str:
        return "CarbonBudgetNotExceeded"


@dataclass
class NonNegativeMetrics:
    def check(self, option: SchedulingOption, task: Dict[str, Any]) -> bool:
        return (
            option.carbon_kgco2e >= 0
            and option.energy_kwh >= 0
            and option.cost_usd >= 0
            and option.latency_hours >= 0
            and option.helium_units >= 0
            and option.carbon_market_cost_usd >= 0
        )

    def name(self) -> str:
        return "NonNegativeMetrics"


class TemporalLogicMonitor:
    def __init__(self, maxlen: int = 500) -> None:
        self.properties: List[SchedulingProperty] = []
        self.violations: Deque[Dict[str, Any]] = deque(maxlen=maxlen)

    def register(self, p: SchedulingProperty) -> None:
        self.properties.append(p)

    def verify(self, option: SchedulingOption,
               task: Dict[str, Any]) -> Tuple[bool, List[str]]:
        bad: List[str] = []
        for p in self.properties:
            if not p.check(option, task):
                bad.append(p.name())
                self.violations.append({
                    "property": p.name(),
                    "mode": option.mode.value,
                    "at": _utcnow().isoformat(),
                })
        return (len(bad) == 0, bad)


# =============================================================================
# ENHANCEMENT 6 — XAI
# =============================================================================

@dataclass
class SchedulingExplanation:
    headline: str
    rationale: List[str]
    confidence: float
    contributing_factors: Dict[str, float]
    counterfactual: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class SchedulingExplainer:
    @staticmethod
    def explain(
        chosen: SchedulingOption,
        alternatives: List[SchedulingOption],
        task: Dict[str, Any],
        weights: Dict[str, float],
        market_snapshot: Optional[Dict[str, Any]] = None,
    ) -> SchedulingExplanation:
        reasons: List[str] = []
        reasons.append(
            f"Chose {chosen.mode.value} on {chosen.region} "
            f"(score={chosen.score:.4f})."
        )

        gross = chosen.carbon_kgco2e
        net = chosen.carbon_kgco2e_net if chosen.carbon_kgco2e_net is not None else gross
        if abs(gross - net) > 1e-9:
            reasons.append(
                f"Gross carbon {gross:.4f} kgCO₂e offset to net {net:.4f} kgCO₂e "
                f"via market credits (${chosen.carbon_market_cost_usd:.4f})."
            )

        # Compare to the runner-up (the real counterfactual).
        if alternatives:
            runner_up = alternatives[0]
            c_saved = runner_up.carbon_kgco2e - chosen.carbon_kgco2e
            if abs(c_saved) > 1e-9:
                reasons.append(
                    f"Carbon {'saved' if c_saved > 0 else 'cost'} vs runner-up: "
                    f"{abs(c_saved):.4f} kgCO₂e."
                )
            cost_delta = runner_up.cost_usd - chosen.cost_usd
            if abs(cost_delta) > 1e-9:
                reasons.append(
                    f"Cost {'saved' if cost_delta > 0 else 'cost'} vs runner-up: "
                    f"${abs(cost_delta):.4f}."
                )

        if chosen.latency_hours > 0:
            reasons.append(f"Deferral/transfer delay: {chosen.latency_hours:.2f}h.")
        if chosen.precision != "fp32":
            reasons.append(f"Precision: {chosen.precision}.")
        if chosen.helium_units > 0:
            reasons.append(f"Helium: {chosen.helium_units:.5f} units.")
        if not chosen.temporal_verified:
            reasons.append(f"Temporal violations: {chosen.temporal_violations}.")
        reasons.append(
            f"Active weights: "
            + ", ".join(f"{k}={v:.2f}" for k, v in weights.items())
        )
        if market_snapshot:
            cp = market_snapshot.get("carbon_price_per_tco2_usd")
            if cp:
                reasons.append(f"Carbon price = ${cp:.2f}/tCO₂.")

        counterfactual = "N/A"
        if alternatives:
            r = alternatives[0]
            counterfactual = (
                f"Runner-up {r.mode.value} would emit {r.carbon_kgco2e:.4f} kgCO₂e "
                f"at {r.cost_usd:.4f} USD / {r.latency_hours:.2f}h."
            )

        return SchedulingExplanation(
            headline=f"[{chosen.mode.value}] score={chosen.score:.4f} "
                     f"carbon(net)={net:.4f} kg",
            rationale=reasons,
            confidence=chosen.confidence,
            contributing_factors={
                "carbon_gross_kgco2e": gross,
                "carbon_net_kgco2e": net,
                "energy_kwh": chosen.energy_kwh,
                "cost_usd": chosen.cost_usd,
                "latency_hours": chosen.latency_hours,
                "helium_units": chosen.helium_units,
                "carbon_market_cost_usd": chosen.carbon_market_cost_usd,
            },
            counterfactual=counterfactual,
        )


# =============================================================================
# ENHANCEMENT 7 — Adaptive Precision
# =============================================================================

class AdaptivePrecisionController:
    PRECISION_ENERGY_MULT = {
        PrecisionLevel.FP32: 1.00,
        PrecisionLevel.FP16: 0.75,
        PrecisionLevel.INT8: 0.50,
        PrecisionLevel.INT4: 0.35,
        PrecisionLevel.QUANTUM_DISTILLED: 0.25,
    }

    def __init__(self, hardware: Optional[Dict[str, Any]] = None) -> None:
        hw = hardware or {}
        self.has_tensor_cores = hw.get("has_tensor_cores", True)
        self.supports_int8 = hw.get("supports_int8", True)
        self.supports_int4 = hw.get("supports_int4", False)
        self.edge_device = hw.get("edge_device", False)

    def select(self, urgency: str) -> PrecisionLevel:
        """Full precision for critical; lowest viable for normal."""
        if urgency == "critical":
            return (PrecisionLevel.FP16 if self.has_tensor_cores
                    else PrecisionLevel.FP32)
        if self.edge_device:
            return (PrecisionLevel.INT8 if self.supports_int8
                    else PrecisionLevel.FP16)
        return (PrecisionLevel.INT4 if self.supports_int4
                else PrecisionLevel.INT8)

    def baseline_precision(self, urgency: str = "critical") -> PrecisionLevel:
        """The un-throttled baseline used by IMMEDIATE execution."""
        return self.select(urgency)

    def energy_multiplier(self, precision: PrecisionLevel) -> float:
        return self.PRECISION_ENERGY_MULT[precision]


# =============================================================================
# ENHANCEMENT 2 — Causal RL (v2: reward-weighted regression w/ baseline)
# =============================================================================

@dataclass
class WeightState:
    region_hash: float
    hour_of_day: int
    priority: float
    recent_carbon_norm: float
    task_type_hash: float


class CausalWeightLearner:
    """
    Learns a contextual adjustment over base objective weights.

    v2 — proper reward-weighted regression:
      * Stable SHA-256 hashing (in the caller; see _build_weight_state).
      * Per-bin baseline (advantage) — prevents the always-positive-drift bug.
      * Update uses `weights_used` and per-feature error: advantage-weighted
        gradient toward observed weights.
    """

    N_FEATURES = 5

    def __init__(
        self,
        base_weights: Dict[str, float],
        lr: float = 0.02,
        baseline_decay: float = 0.95,
        buffer_maxlen: int = 512,
    ) -> None:
        self.base = dict(base_weights)
        self.keys: Tuple[str, ...] = tuple(base_weights.keys())
        self.n_weights = len(self.keys)
        self.lr = lr
        self.baseline_decay = baseline_decay

        # Per-weight linear coefficients over features
        self.weight_coeffs: List[List[float]] = [
            [0.0] * self.N_FEATURES for _ in range(self.n_weights)
        ]

        # Per-context-bin baseline (advantage estimation)
        self.baselines: Dict[int, float] = defaultdict(float)
        self.baseline_counts: Dict[int, int] = defaultdict(int)

        # Buffered observations: (features, weights_used, reward, bin)
        self.buffer: Deque[Tuple[List[float], Dict[str, float], float, int]] = \
            deque(maxlen=buffer_maxlen)
        self.observations: int = 0
        self.updates: int = 0
        self.last_advantage_mean: float = 0.0

    # --- Feature / bin helpers ------------------------------------------------

    @staticmethod
    def _features(s: WeightState) -> List[float]:
        return [
            s.region_hash,
            s.hour_of_day / 24.0,
            s.priority,
            min(1.0, max(0.0, s.recent_carbon_norm)),  # built-in min/max
            s.task_type_hash,
        ]

    def _bin(self, f: List[float]) -> int:
        return int(f[0] * 10)  # 10 region bins

    def _baseline_for(self, f: List[float]) -> float:
        return self.baselines.get(self._bin(f), 0.0)

    def _update_baseline(self, f: List[float], reward: float) -> None:
        b = self._bin(f)
        old = self.baselines.get(b, 0.0)
        self.baselines[b] = (self.baseline_decay * old
                             + (1.0 - self.baseline_decay) * reward)
        self.baseline_counts[b] += 1

    # --- Prediction -----------------------------------------------------------

    def _raw_predict(self, f: List[float]) -> List[float]:
        return [
            self.base[k] + sum(c * x for c, x in zip(coeffs, f))
            for k, coeffs in zip(self.keys, self.weight_coeffs)
        ]

    def predict_weights(self, s: WeightState) -> Dict[str, float]:
        f = self._features(s)
        raw = self._raw_predict(f)
        adj = {k: max(0.01, r) for k, r in zip(self.keys, raw)}
        total = sum(adj.values()) or 1.0
        return {k: v / total for k, v in adj.items()}

    # --- Learning -------------------------------------------------------------

    def record(
        self,
        s: WeightState,
        weights_used: Dict[str, float],
        reward: float,
    ) -> None:
        f = self._features(s)
        self.buffer.append((f, dict(weights_used), reward, self._bin(f)))
        self.observations += 1

    def update(self) -> None:
        """
        Reward-weighted regression step.

        For each buffered (f, weights_used, reward):
            advantage = reward − baseline(bin(f))
            For each weight k:
                coeffs[k] += lr · advantage · (target_k − predict_k(f)) · f
        Where target_k = weights_used[k].

        Interpretation: when reward > baseline, pull predictions toward
        the weights that produced it; when reward < baseline, push away.
        """
        if not self.buffer:
            return

        advantages: List[float] = []
        for f, weights_used, reward, _b in self.buffer:
            adv = reward - self._baseline_for(f)
            advantages.append(adv)
            self._update_baseline(f, reward)

            current = self._raw_predict(f)
            for i, k in enumerate(self.keys):
                target = weights_used.get(k, 1.0 / self.n_weights)
                error = target - current[i]
                for j, x in enumerate(f):
                    self.weight_coeffs[i][j] += self.lr * adv * error * x

        self.last_advantage_mean = statistics.fmean(advantages) if advantages else 0.0
        self.updates += 1
        self.buffer.clear()


# =============================================================================
# ENHANCEMENT 1 — Quantum Distillation
# =============================================================================

@dataclass
class DistilledEstimatorModel:
    precision: PrecisionLevel
    energy_coefficient: float
    carbon_coefficient: float
    quality_retention: float
    energy_reduction_percent: float


class QuantumDistillationBridge:
    def distill(
        self,
        energy_coefficient: float,
        carbon_coefficient: float,
        precision: PrecisionLevel,
    ) -> DistilledEstimatorModel:
        scale, retention, energy = {
            PrecisionLevel.FP32: (1.0, 1.00, 0.0),
            PrecisionLevel.FP16: (1.0, 0.98, 30.0),
            PrecisionLevel.INT8: (100.0, 0.93, 55.0),
            PrecisionLevel.INT4: (10.0, 0.85, 70.0),
            PrecisionLevel.QUANTUM_DISTILLED: (5.0, 0.80, 85.0),
        }[precision]

        def qz(v: float) -> float:
            return math.floor(v * scale) / scale if scale > 1.0 else v

        return DistilledEstimatorModel(
            precision=precision,
            energy_coefficient=qz(energy_coefficient),
            carbon_coefficient=qz(carbon_coefficient),
            quality_retention=retention,
            energy_reduction_percent=energy,
        )


# =============================================================================
# ENHANCEMENT 3 — Federated Learning
# =============================================================================

@dataclass
class FederatedSchedulingProfile:
    deployment_id: str
    key: str
    mode: str
    mean_carbon_kg: float
    mean_savings_pct: float
    sample_count: int
    granularity: str = "aggregate"  # "aggregate" | "single"
    timestamp: datetime = field(default_factory=_utcnow)


class FederatedAggregator:
    def __init__(self) -> None:
        self.updates: List[FederatedSchedulingProfile] = []
        self._global: Dict[str, Dict[str, float]] = {}

    def push(self, u: FederatedSchedulingProfile) -> None:
        self.updates.append(u)

    def aggregate(self) -> Dict[str, Dict[str, float]]:
        # Only aggregated profiles participate in weighted means;
        # singles are still logged for traceability but don't skew weights.
        grouped: Dict[str, List[FederatedSchedulingProfile]] = defaultdict(list)
        for u in self.updates:
            if u.granularity == "aggregate":
                grouped[u.key].append(u)
        result: Dict[str, Dict[str, float]] = {}
        for key, profiles in grouped.items():
            total_w = sum(p.sample_count for p in profiles) or 1
            result[key] = {
                "mean_carbon_kg": sum(
                    p.mean_carbon_kg * p.sample_count for p in profiles
                ) / total_w,
                "mean_savings_pct": sum(
                    p.mean_savings_pct * p.sample_count for p in profiles
                ) / total_w,
                "sample_count": total_w,
            }
        self._global = result
        return result

    def lookup(self, key: str) -> Optional[Dict[str, float]]:
        return self._global.get(key)


# =============================================================================
# ENHANCEMENT 4 — Multi-Agent Coordination
# =============================================================================

@dataclass
class AgentProfile:
    agent_id: str
    role: AgentRole = AgentRole.GENERALIST
    success_rate: float = 0.0
    avg_carbon_saved: float = 0.0
    avg_latency_hours: float = 0.0
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
            if a.avg_carbon_saved > 0.2 and a.avg_latency_hours > 6.0:
                a.role = AgentRole.LONG_HORIZON_SPECIALIST
            elif a.avg_carbon_saved > 0.2 and a.avg_latency_hours <= 6.0:
                a.role = AgentRole.SHORT_HORIZON_SPECIALIST
            elif a.avg_carbon_saved > 0.1:
                a.role = AgentRole.CROSS_REGION_SPECIALIST
            elif a.success_rate > 0.9:
                a.role = AgentRole.MARKET_SPECIALIST
            else:
                a.role = AgentRole.GENERALIST

    def record(
        self,
        agent_id: str,
        success: bool,
        carbon_saved_pct: float,
        latency_hours: float,
    ) -> None:
        a = self.register(agent_id)
        n = a.total_calls + 1
        a.success_rate = ((n - 1) * a.success_rate + float(success)) / n
        a.avg_carbon_saved = (
            (n - 1) * a.avg_carbon_saved + carbon_saved_pct / 100.0
        ) / n
        a.avg_latency_hours = (
            (n - 1) * a.avg_latency_hours + latency_hours
        ) / n
        a.total_calls = n
        if n % 5 == 0:
            self._reassign()

    def select_agent(
        self, prefer: AgentRole = AgentRole.GENERALIST
    ) -> Optional[str]:
        if not self.agents:
            return None
        cands = [a for a in self.agents.values() if a.role == prefer]
        if not cands:
            cands = list(self.agents.values())
        return max(cands, key=lambda a: a.success_rate).agent_id


# =============================================================================
# ENHANCEMENT 8 — Carbon Markets (with pluggable backend)
# =============================================================================

@dataclass
class MarketSnapshot:
    carbon_price_per_tco2_usd: float
    rec_price_per_mwh_usd: float
    rec_available_mwh: float
    timestamp: datetime = field(default_factory=_utcnow)


class MarketBackend(Protocol):
    def fetch(self) -> MarketSnapshot: ...


class MockMarketBackend:
    def fetch(self) -> MarketSnapshot:
        return MarketSnapshot(
            carbon_price_per_tco2_usd=random.uniform(20, 80),
            rec_price_per_mwh_usd=random.uniform(3, 9),
            rec_available_mwh=random.uniform(10, 500),
        )


class CarbonMarketClient:
    def __init__(self, backend: Optional[MarketBackend] = None,
                 ttl_s: int = 300) -> None:
        self._backend = backend or MockMarketBackend()
        self._ttl = ttl_s
        self._cache: Optional[MarketSnapshot] = None

    def get_snapshot(self) -> MarketSnapshot:
        if self._cache and (
            (_utcnow() - _ensure_aware(self._cache.timestamp)).total_seconds()
            < self._ttl
        ):
            return self._cache
        snap = self._backend.fetch()
        self._cache = snap
        return snap


# =============================================================================
# ENHANCEMENT 10 — HITL (async-supported)
# =============================================================================

@dataclass
class HITLRequest:
    task_id: str
    mode: str
    reason: str
    urgency: str
    context: Dict[str, Any] = field(default_factory=dict)
    requested_at: datetime = field(default_factory=_utcnow)


class HumanInTheLoopGate:
    def __init__(
        self,
        deferral_threshold_hours: float = 24.0,
        uncertainty_threshold: float = 0.6,
        market_cost_threshold_usd: float = 5.0,
        first_n_in_region: int = 3,
        timeout_s: float = 60.0,
        default_on_timeout: bool = False,
        feedback_maxlen: int = 1_000,
    ) -> None:
        self.deferral_threshold_hours = deferral_threshold_hours
        self.uncertainty_threshold = uncertainty_threshold
        self.market_cost_threshold_usd = market_cost_threshold_usd
        self.first_n_in_region = first_n_in_region
        self.timeout_s = timeout_s
        self.default_on_timeout = default_on_timeout

        self.pending: List[HITLRequest] = []
        self.feedback_log: Deque[Dict[str, Any]] = deque(maxlen=feedback_maxlen)
        self._callback: Optional[Callable[[HITLRequest], bool]] = None
        self._async_callback: Optional[Callable[[HITLRequest], Any]] = None
        self._region_counts: Dict[str, int] = defaultdict(int)

    def set_callback(self, cb: Callable[[HITLRequest], bool]) -> None:
        self._callback = cb

    def set_async_callback(
        self, cb: Callable[[HITLRequest], Any]
    ) -> None:
        """cb(req) must return an awaitable resolving to bool."""
        self._async_callback = cb

    def needs_review(
        self, option: SchedulingOption, task: Dict[str, Any]
    ) -> bool:
        if option.latency_hours > self.deferral_threshold_hours:
            return True
        if (task.get("deadline_critical")
                and option.mode in (ExecutionMode.GREEN_ROUTED,
                                    ExecutionMode.MARKET_ARBITRAGED)):
            return True
        if option.mode == ExecutionMode.MARKET_ARBITRAGED:
            return True
        if option.uncertainty_std > self.uncertainty_threshold:
            return True
        if option.carbon_market_cost_usd > self.hitl_market_cost_threshold_usd:
            return True
        if self._region_counts[option.region] < self.first_n_in_region:
            return True
        return False

    def _record_decision(self, req: HITLRequest, decision: bool,
                         auto: bool = False) -> None:
        self.feedback_log.append({
            "req": asdict(req),
            "decision": decision,
            "auto": auto,
        })

    async def request_review_async(
        self,
        task_id: str,
        mode: str,
        reason: str,
        urgency: str = "medium",
        context: Optional[Dict[str, Any]] = None,
    ) -> bool:
        req = HITLRequest(task_id, mode, reason, urgency, context or {})
        self.pending.append(req)
        try:
            if urgency == "low":
                # Policy auto-approve — documented and auditable.
                self._record_decision(req, True, auto=True)
                return True

            if self._async_callback is not None:
                decision = await asyncio.wait_for(
                    self._async_callback(req), timeout=self.timeout_s,
                )
            elif self._callback is not None:
                decision = self._callback(req)
            else:
                decision = self.default_on_timeout
        except asyncio.TimeoutError:
            logger.warning(f"HITL timed out for {task_id}; using default")
            decision = self.default_on_timeout
        except Exception as e:
            logger.error(f"HITL callback failed: {e}")
            decision = False
        finally:
            if req in self.pending:
                self.pending.remove(req)

        self._record_decision(req, bool(decision))
        self._region_counts[context.get("region", "") if context else ""] += 1
        return bool(decision)

    def active_learning_batch(self, n: int = 16) -> List[Dict[str, Any]]:
        return list(self.feedback_log)[-n:]


# =============================================================================
# Helium Profiler
# =============================================================================

class HeliumProfiler:
    """
    Estimate helium consumption from energy and scarcity.

    Scarcity → *higher* effective units (the resource is harder to procure,
    so its shadow price/cost per unit is higher). Earlier behaviour was
    inverted.
    """
    HELIUM_UNITS_PER_KWH = 0.05
    SCARCITY_UPLIFT_MAX = 0.5  # up to +50%

    @classmethod
    def estimate_units(
        cls, energy_kwh: float, scarcity_score: float = 0.0
    ) -> float:
        base = energy_kwh * cls.HELIUM_UNITS_PER_KWH
        uplift = 1.0 + min(cls.SCARCITY_UPLIFT_MAX, max(0.0, scarcity_score) * 0.5)
        return base * uplift


# =============================================================================
# Observability: Metrics + Audit
# =============================================================================

class MetricsRecorder:
    """Swap with a Prometheus/OTel bridge in production."""

    def __init__(self) -> None:
        self.counters: Dict[str, float] = defaultdict(float)
        self.histograms: Dict[str, List[float]] = defaultdict(list)

    @staticmethod
    def _labels(labels: Dict[str, Any]) -> str:
        if not labels:
            return ""
        return "{" + ",".join(f'{k}="{v}"' for k, v in sorted(labels.items())) + "}"

    def inc(self, name: str, value: float = 1.0, **labels: Any) -> None:
        self.counters[f"{name}{self._labels(labels)}"] += value

    def observe(self, name: str, value: float, **labels: Any) -> None:
        self.histograms[f"{name}{self._labels(labels)}"].append(value)

    def snapshot(self) -> Dict[str, Any]:
        return {
            "counters": dict(self.counters),
            "histograms": {
                k: {
                    "count": len(v),
                    "mean": statistics.fmean(v) if v else 0.0,
                }
                for k, v in self.histograms.items() if v
            },
        }


class AuditLog:
    """Append-only audit log (compliance, market spends)."""

    def __init__(
        self,
        sink: Optional[Callable[[Dict[str, Any]], None]] = None,
        maxlen: int = 10_000,
    ) -> None:
        self.records: Deque[Dict[str, Any]] = deque(maxlen=maxlen)
        self._sink = sink

    def record(self, entry: Dict[str, Any]) -> None:
        full = {**entry, "ts": _utcnow().isoformat()}
        self.records.append(full)
        if self._sink:
            try:
                self._sink(full)
            except Exception as e:  # never let audit block scheduling
                logger.error(f"Audit sink failed: {e}")


# =============================================================================
# The Enhanced MultiObjectiveScheduler
# =============================================================================

class MultiObjectiveScheduler:
    """
    Enhanced scheduler with all ten cross-cutting enhancement layers.

    Backward-compatible signature:
        MultiObjectiveScheduler(
            carbon_forecaster, task_profiler, ray_cluster,
            alpha=0.5, beta=0.3, gamma=0.1, delta=0.1,
        )
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

    def __init__(
        self,
        carbon_forecaster,
        task_profiler,
        ray_cluster,
        alpha: float = 0.5,
        beta: float = 0.3,
        gamma: float = 0.1,
        delta: float = 0.1,
        epsilon: float = 0.02,     # helium
        zeta: float = 0.02,        # carbon market cost
        deployment_id: str = "local",
        agent_id: str = "scheduler-0",
        features: Optional[Dict[str, bool]] = None,
        hardware: Optional[Dict[str, Any]] = None,
        config: Optional[SchedulerConfig] = None,
        objectives: Optional[List[ObjectiveSpec]] = None,
        metrics: Optional[MetricsRecorder] = None,
        audit_log: Optional[AuditLog] = None,
    ):
        self.carbon_forecaster = carbon_forecaster
        self.task_profiler = task_profiler
        self.ray_cluster = ray_cluster

        self.config = config or SchedulerConfig()
        self.electricity_cost_per_kwh = dict(self.config.electricity_cost_per_kwh)

        self.deployment_id = deployment_id
        self.agent_id = agent_id
        self.features = {**self.DEFAULT_FEATURES, **(features or {})}

        # Objectives registry (pluggable)
        self.objectives: List[ObjectiveSpec] = (
            objectives or default_objectives()
        )

        # Base weights per objective name
        self.base_weights: Dict[str, float] = {
            "alpha": alpha, "beta": beta, "gamma": gamma, "delta": delta,
            "epsilon": epsilon, "zeta": zeta,
        }
        # Ensure every registered objective has a weight
        for spec in self.objectives:
            self.base_weights.setdefault(spec.name, spec.default_weight)

        # Bounded history
        self.scheduling_history: Deque[SchedulingDecision] = deque(
            maxlen=self.config.history_maxlen
        )

        # Resilience
        self._forecaster_circuit = CircuitBreaker(
            "forecaster", self.config.circuit_failure_threshold,
            self.config.circuit_recovery_timeout_s,
        )
        self._profiler_circuit = CircuitBreaker(
            "profiler", self.config.circuit_failure_threshold,
            self.config.circuit_recovery_timeout_s,
        )
        self._cluster_circuit = CircuitBreaker(
            "cluster", self.config.circuit_failure_threshold,
            self.config.circuit_recovery_timeout_s,
        )
        self.chaos = (
            ChaosInjector(
                failure_rate=self.config.chaos_failure_rate,
                maxlen=self.config.chaos_event_maxlen,
            )
            if self.features["chaos_testing"] else None
        )

        # Temporal logic
        self.temporal_monitor: Optional[TemporalLogicMonitor] = (
            TemporalLogicMonitor(
                maxlen=self.config.temporal_violation_maxlen,
            )
            if self.features["temporal_logic"] else None
        )
        if self.temporal_monitor:
            self.temporal_monitor.register(DeadlineRespected())
            self.temporal_monitor.register(EnergyBudgetNotExceeded())
            self.temporal_monitor.register(CarbonBudgetNotExceeded())
            self.temporal_monitor.register(NonNegativeMetrics())

        # XAI
        self.explainer = SchedulingExplainer() if self.features["xai"] else None

        # Precision
        self.precision_ctl = (
            AdaptivePrecisionController(hardware)
            if self.features["adaptive_precision"] else None
        )

        # Causal RL
        self.causal_learner = (
            CausalWeightLearner(
                base_weights=self.base_weights,
                lr=self.config.causal_lr,
                baseline_decay=self.config.causal_baseline_decay,
                buffer_maxlen=self.config.causal_buffer_maxlen,
            )
            if self.features["causal_rl"] else None
        )

        # Distillation
        self.distiller = (
            QuantumDistillationBridge()
            if self.features["quantum_distillation"] else None
        )

        # Federated
        self.federated = (
            FederatedAggregator() if self.features["federated"] else None
        )

        # Multi-agent
        self.coordinator = (
            MultiAgentCoordinator() if self.features["multi_agent"] else None
        )
        if self.coordinator:
            self.coordinator.register(self.agent_id)

        # Market
        self.market = (
            CarbonMarketClient(ttl_s=self.config.market_ttl_s)
            if self.features["carbon_market"] else None
        )

        # HITL
        self.hitl = (
            HumanInTheLoopGate(
                deferral_threshold_hours=self.config.hitl_deferral_threshold_hours,
                uncertainty_threshold=self.config.hitl_uncertainty_threshold,
                market_cost_threshold_usd=self.config.hitl_market_cost_threshold_usd,
                first_n_in_region=self.config.hitl_first_n_in_region,
                timeout_s=self.config.hitl_timeout_s,
                default_on_timeout=self.config.hitl_default_on_timeout,
                feedback_maxlen=self.config.hitl_feedback_maxlen,
            )
            if self.features["hitl"] else None
        )

        # Helium
        self.helium_profiler = (
            HeliumProfiler() if self.features["helium_profiling"] else None
        )

        # Observability
        self.metrics = metrics or MetricsRecorder()
        self.audit_log = audit_log or AuditLog(
            maxlen=self.config.audit_maxlen
        )

        # Per-region decision counters (for first-N HITL trigger)
        self._region_decision_counts: Dict[str, int] = defaultdict(int)

        logger.info(
            f"Enhanced Multi-objective scheduler initialized "
            f"(deployment={deployment_id}, agent={agent_id}, "
            f"base_weights={self.base_weights}, "
            f"objectives={[o.name for o in self.objectives]}, "
            f"features={list(self.features)})"
        )

    # ------------------------------------------------------------------
    # Main entry point (with overall timeout + fallback)
    # ------------------------------------------------------------------

    async def schedule(
        self,
        task: Dict[str, Any],
        consider_deferral: bool = True,
        consider_reallocation: bool = True,
    ) -> SchedulingDecision:
        try:
            return await asyncio.wait_for(
                self._schedule_inner(
                    task, consider_deferral, consider_reallocation,
                ),
                timeout=self.config.schedule_timeout_s,
            )
        except asyncio.TimeoutError:
            logger.error(
                f"Scheduling timed out for task {task.get('task_id')}; "
                "falling back to IMMEDIATE"
            )
            self.metrics.inc("scheduler_degraded_total", reason="timeout")
            return await self._fallback_decision(task, "timeout")
        except Exception as e:
            logger.exception(f"Scheduling error: {e}; falling back")
            self.metrics.inc("scheduler_degraded_total", reason="error")
            return await self._fallback_decision(task, f"error:{type(e).__name__}")

    async def _schedule_inner(
        self,
        task: Dict[str, Any],
        consider_deferral: bool,
        consider_reallocation: bool,
    ) -> SchedulingDecision:
        t0 = time.perf_counter()
        task_id = task.get("task_id", "unknown")

        # --- Generate options ---
        options = await self._generate_options(
            task=task,
            consider_deferral=consider_deferral,
            consider_reallocation=consider_reallocation,
        )
        if not options:
            raise ValueError("No feasible scheduling options found")

        # --- Adaptive weights ---
        causal_policy_used = False
        if self.causal_learner:
            state = self._build_weight_state(task)
            active_weights = self.causal_learner.predict_weights(state)
            causal_policy_used = True
        else:
            active_weights = self._normalized_base_weights()

        # --- Scoring ---
        scored_options = self._calculate_scores(
            task, options, active_weights=active_weights,
        )

        # --- Temporal verification ---
        if self.temporal_monitor:
            for opt in scored_options:
                ok, violations = self.temporal_monitor.verify(opt, task)
                opt.temporal_verified = ok
                opt.temporal_violations = violations
            valid = [o for o in scored_options if o.temporal_verified]
            if valid:
                scored_options = valid

        scored_options.sort(key=lambda opt: opt.score)
        best_option = scored_options[0]

        # --- Federated hint ---
        federated_blend_applied = False
        if self.federated:
            key = f"scheduling:{best_option.region}:{best_option.mode.value}"
            profile = self.federated.lookup(key)
            if profile and profile.get("sample_count", 0) >= \
                    self.config.federated_min_samples:
                federated_blend_applied = True
                best_option.federated_blend_applied = True

        # --- HITL ---
        if self.hitl and self.hitl.needs_review(best_option, task):
            best_option.hitl_required = True
            approved = await self.hitl.request_review_async(
                task_id=task_id,
                mode=best_option.mode.value,
                reason=(
                    f"latency={best_option.latency_hours:.1f}h, "
                    f"mode={best_option.mode.value}, "
                    f"uncertainty={best_option.uncertainty_std:.2f}, "
                    f"market_cost=${best_option.carbon_market_cost_usd:.2f}"
                ),
                urgency="high" if task.get("deadline_critical") else "medium",
                context={
                    "latency_hours": best_option.latency_hours,
                    "region": best_option.region,
                },
            )
            best_option.hitl_approved = approved
            if approved is False:
                immediate = next(
                    (o for o in scored_options
                     if o.mode == ExecutionMode.IMMEDIATE), None,
                )
                if immediate:
                    best_option = immediate

        # --- Pareto ---
        pareto_efficient = self._is_pareto_efficient(
            best_option, scored_options[1:]
        )

        # --- Reasoning ---
        reasoning = self._generate_reasoning(best_option, scored_options)

        # --- XAI ---
        explanation_dict = None
        if self.explainer:
            market_snapshot = None
            if self.market:
                snap = self.market.get_snapshot()
                market_snapshot = {
                    "carbon_price_per_tco2_usd": snap.carbon_price_per_tco2_usd,
                    "rec_available_mwh": snap.rec_available_mwh,
                }
            exp = self.explainer.explain(
                chosen=best_option,
                alternatives=scored_options[1:],
                task=task,
                weights=active_weights,
                market_snapshot=market_snapshot,
            )
            best_option.explanation = exp.headline
            explanation_dict = exp.to_dict()

        # --- Market enrichment ---
        market_enrichment = None
        if self.market:
            snap = self.market.get_snapshot()
            net = best_option.carbon_kgco2e_net or 0.0
            market_enrichment = {
                "carbon_price_per_tco2_usd": snap.carbon_price_per_tco2_usd,
                "carbon_market_cost_usd": net / 1000.0 * snap.carbon_price_per_tco2_usd,
                "rec_price_per_mwh_usd": snap.rec_price_per_mwh_usd,
                "rec_available_mwh": snap.rec_available_mwh,
            }

        # --- Decision ---
        decision = SchedulingDecision(
            task_id=task_id,
            chosen_option=best_option,
            alternatives=scored_options[1:5],
            reasoning=reasoning,
            pareto_efficient=pareto_efficient,
            explanation=explanation_dict,
            temporal_verified=best_option.temporal_verified,
            temporal_violations=best_option.temporal_violations,
            market_enrichment=market_enrichment,
            hitl_required=best_option.hitl_required,
            hitl_approved=best_option.hitl_approved,
            adaptive_weights_used=active_weights,
            causal_policy_used=causal_policy_used,
            federated_blend_applied=federated_blend_applied,
            circuits=self._circuit_states(),
        )
        self.scheduling_history.append(decision)
        self._region_decision_counts[best_option.region] += 1

        # --- Metrics + audit ---
        self._record_metrics(task, best_option, active_weights, t0)
        self._audit_decision(task, decision)

        # --- Causal feedback ---
        if self.causal_learner:
            reward = self._compute_reward(best_option, task)
            self.causal_learner.record(
                self._build_weight_state(task),
                active_weights,
                reward,
            )
            if self.causal_learner.observations % \
                    self.config.causal_update_every == 0:
                self.causal_learner.update()

        # --- Multi-agent feedback ---
        if self.coordinator:
            saved_pct = self._carbon_saved_pct(best_option, scored_options[1:])
            self.coordinator.record(
                agent_id=self.agent_id,
                success=best_option.temporal_verified,
                carbon_saved_pct=saved_pct,
                latency_hours=best_option.latency_hours,
            )

        # --- Federated contribution ---
        if self.federated and len(self.scheduling_history) % \
                self.config.federated_push_every == 0:
            self._push_federated_profile(best_option)

        logger.info(
            f"Scheduled {task_id} as {best_option.mode.value} "
            f"(score={best_option.score:.3f}, "
            f"carbon_net={best_option.carbon_kgco2e_net:.4f} kgCO₂e)"
        )
        return decision

    # ------------------------------------------------------------------
    # Reward function
    # ------------------------------------------------------------------

    def _compute_reward(
        self, chosen: SchedulingOption, task: Dict[str, Any],
    ) -> float:
        """
        Reward the *decision*, not the estimator.

        Baseline is the worst (gross) carbon among the current decision's
        alternatives; saving vs that baseline dominates. Latency is penalized
        when the task is urgent.
        """
        alternatives = list(self.scheduling_history[-1].alternatives) \
            if self.scheduling_history else []
        baseline_carbon = max(
            [a.carbon_kgco2e for a in alternatives] + [chosen.carbon_kgco2e, 1e-9]
        )
        net = chosen.carbon_kgco2e_net or 0.0
        carbon_savings = max(0.0, baseline_carbon - net) / baseline_carbon

        # Latency penalty scaled by urgency
        priority = float(task.get("priority", 0.5))
        lat_pen = min(1.0, chosen.latency_hours / 24.0) * priority

        # Uncertainty penalty
        unc_pen = min(1.0, chosen.uncertainty_std)

        reward = 0.6 * carbon_savings + 0.3 * (1.0 - lat_pen) + 0.1 * (1.0 - unc_pen)
        return max(0.0, min(1.0, reward))

    def _carbon_saved_pct(
        self, chosen: SchedulingOption,
        alternatives: List[SchedulingOption],
    ) -> float:
        if not alternatives:
            return 0.0
        alt_carbon = statistics.fmean(
            (a.carbon_kgco2e_net or a.carbon_kgco2e) for a in alternatives
        )
        if alt_carbon <= 0:
            return 0.0
        chosen_carbon = chosen.carbon_kgco2e_net or chosen.carbon_kgco2e
        return max(0.0, (alt_carbon - chosen_carbon) / alt_carbon * 100.0)

    # ------------------------------------------------------------------
    # Weight-state helper (stable hashing)
    # ------------------------------------------------------------------

    def _build_weight_state(self, task: Dict[str, Any]) -> WeightState:
        region = task.get("region", "US-CA")
        recent_carbon = 0.0
        if self.scheduling_history:
            recent = list(self.scheduling_history)[-10:]
            recent_carbon = statistics.fmean(
                (d.chosen_option.carbon_kgco2e_net or 0.0) for d in recent
            )
        return WeightState(
            region_hash=stable_hash(region),
            hour_of_day=_utcnow().hour,
            priority=float(task.get("priority", 0.5)),
            recent_carbon_norm=min(1.0, recent_carbon * 100.0),
            task_type_hash=stable_hash(str(task.get("task_type", ""))),
        )

    def _normalized_base_weights(self) -> Dict[str, float]:
        total = sum(self.base_weights.values()) or 1.0
        return {k: v / total for k, v in self.base_weights.items()}

    # ------------------------------------------------------------------
    # Fallback decision
    # ------------------------------------------------------------------

    async def _fallback_decision(
        self, task: Dict[str, Any], reason: str,
    ) -> SchedulingDecision:
        region = task.get("region", "US-CA")
        est = self._default_estimate(task, 400.0)
        opt = SchedulingOption(
            mode=ExecutionMode.IMMEDIATE,
            node_id="current_node",
            region=region,
            start_time=_utcnow(),
            carbon_kgco2e=est.expected_carbon_kgco2e,
            energy_kwh=est.expected_energy_kwh,
            cost_usd=est.expected_energy_kwh * self.electricity_cost_per_kwh.get(region, 0.2),
            latency_hours=0.0,
            score=float("inf"),
            precision=PrecisionLevel.FP32.value,
            confidence=0.1,
            uncertainty_std=1.0,
        )
        decision = SchedulingDecision(
            task_id=task.get("task_id", "unknown"),
            chosen_option=opt,
            alternatives=[],
            reasoning=f"Fallback IMMEDIATE due to {reason}",
            pareto_efficient=False,
            degraded=True,
            degradation_reason=reason,
            circuits=self._circuit_states(),
        )
        self.scheduling_history.append(decision)
        return decision

    # ------------------------------------------------------------------
    # Option generation
    # ------------------------------------------------------------------

    async def _generate_options(
        self,
        task: Dict[str, Any],
        consider_deferral: bool,
        consider_reallocation: bool,
    ) -> List[SchedulingOption]:
        options: List[SchedulingOption] = []
        current_region = task.get("region", "US-CA")
        current_intensity = await self._safe_get_intensity(current_region)

        options.append(await self._create_immediate_option(
            task=task, region=current_region, carbon_intensity=current_intensity,
        ))

        if consider_deferral and task.get("deferrable", True):
            deadline = task.get("deadline")
            if deadline:
                deferred = await self._create_deferred_option(
                    task=task, region=current_region, deadline=deadline,
                )
                if deferred:
                    options.append(deferred)

        if consider_reallocation:
            try:
                cluster_info = await self._safe_get_cluster_info()
                for node in cluster_info.get("nodes", [])[:3]:
                    if node.get("node_id") != "current_node":
                        options.append(await self._create_reallocated_option(
                            task=task, node=node, region=current_region,
                            carbon_intensity=current_intensity,
                        ))
            except Exception as e:
                logger.debug(f"Reallocation options unavailable: {e}")

        if consider_reallocation:
            alternative_regions = [
                r for r in ["US-CA", "US-NY", "EU-DE", "EU-FR"]
                if r != current_region
            ][:2]

            async def _mk(region: str):
                return await self._create_green_routed_option(
                    task=task, target_region=region,
                )

            green_results = await asyncio.gather(
                *[_mk(r) for r in alternative_regions],
                return_exceptions=True,
            )
            for r in green_results:
                if isinstance(r, SchedulingOption):
                    options.append(r)

        if self.precision_ctl:
            throttled = await self._create_precision_throttled_option(
                task=task, region=current_region,
                carbon_intensity=current_intensity,
            )
            if throttled:
                options.append(throttled)

        if self.market:
            arbitraged = await self._create_market_arbitraged_option(
                task=task, region=current_region,
                carbon_intensity=current_intensity,
            )
            if arbitraged:
                options.append(arbitraged)

        return options

    # ------------------------------------------------------------------
    # Circuit-safe wrappers (with timeouts)
    # ------------------------------------------------------------------

    async def _safe_get_intensity(self, region: str) -> float:
        if not self._forecaster_circuit.can_call():
            logger.debug("Forecaster circuit open — using default 400.0")
            return 400.0
        try:
            if self.chaos and self.chaos.maybe_fail("forecaster"):
                raise RuntimeError("chaos: forecaster")
            v = await asyncio.wait_for(
                self.carbon_forecaster.get_current_intensity(region),
                timeout=self.config.forecaster_timeout_s,
            )
            self._forecaster_circuit.record_success()
            return float(v)
        except Exception as e:
            self._forecaster_circuit.record_failure()
            logger.warning(f"Intensity fetch failed for {region}: {e}; using 400.0")
            return 400.0

    async def _safe_get_cluster_info(self) -> Dict[str, Any]:
        if not self._cluster_circuit.can_call():
            return {"nodes": []}
        try:
            if self.chaos and self.chaos.maybe_fail("cluster"):
                raise RuntimeError("chaos: cluster")
            info = await asyncio.wait_for(
                self._maybe_await(self.ray_cluster.get_cluster_info()),
                timeout=self.config.cluster_timeout_s,
            )
            self._cluster_circuit.record_success()
            return info or {"nodes": []}
        except Exception as e:
            self._cluster_circuit.record_failure()
            logger.warning(f"Cluster info fetch failed: {e}")
            return {"nodes": []}

    @staticmethod
    async def _maybe_await(v: Any) -> Any:
        """Support both sync and async dependency methods."""
        if asyncio.iscoroutine(v) or isinstance(v, asyncio.Future):
            return await v
        return v

    async def _safe_estimate_energy(
        self, task: Dict[str, Any], carbon_intensity: float,
    ) -> Any:
        if not self._profiler_circuit.can_call():
            return self._default_estimate(task, carbon_intensity)
        try:
            if self.chaos and self.chaos.maybe_fail("profiler"):
                raise RuntimeError("chaos: profiler")
            est = await asyncio.wait_for(
                self.task_profiler.estimate_energy(
                    task=task, carbon_intensity=carbon_intensity,
                ),
                timeout=self.config.profiler_timeout_s,
            )
            self._profiler_circuit.record_success()
            return est
        except Exception as e:
            self._profiler_circuit.record_failure()
            logger.warning(f"Energy estimate failed: {e}; using default")
            return self._default_estimate(task, carbon_intensity)

    def _default_estimate(
        self, task: Dict[str, Any], carbon_intensity: float,
    ) -> Any:
        class _FallbackEstimate:
            def __init__(self, energy: float, carbon: float, conf: float):
                self.expected_energy_kwh = energy
                self.expected_carbon_kgco2e = carbon
                self.confidence = conf
                self.uncertainty_std = 1.0 - conf
        energy = max(0.001, task.get("dataset_size", 1000) / 1e6)
        carbon = energy * carbon_intensity / 1000.0
        return _FallbackEstimate(energy, carbon, 0.1)

    # ------------------------------------------------------------------
    # Option constructors
    # ------------------------------------------------------------------

    def _common_option_fields(
        self, carbon_gross: float, energy: float,
        region: str, precision: PrecisionLevel, confidence: float,
    ) -> Dict[str, Any]:
        helium_units = 0.0
        scarcity = 0.0
        if self.helium_profiler:
            scarcity = float(self._last_helium_signal or 0.0) if hasattr(self, "_last_helium_signal") else 0.0
            helium_units = HeliumProfiler.estimate_units(energy, scarcity)
        market_cost = 0.0
        if self.market:
            snap = self.market.get_snapshot()
            market_cost = carbon_gross / 1000.0 * snap.carbon_price_per_tco2_usd
        return {
            "helium_units": helium_units,
            "helium_scarcity_score": scarcity,
            "carbon_market_cost_usd": market_cost,
            "precision": precision.value,
            "confidence": confidence,
        }

    async def _create_immediate_option(
        self, task: Dict[str, Any], region: str, carbon_intensity: float,
    ) -> SchedulingOption:
        est = await self._safe_estimate_energy(task, carbon_intensity)
        # IMMEDIATE = full-precision baseline (do NOT auto-throttle).
        precision = PrecisionLevel.FP32
        if self.precision_ctl and task.get("deadline_critical"):
            precision = self.precision_ctl.baseline_precision("critical")
        energy_mult = (self.precision_ctl.energy_multiplier(precision)
                       if self.precision_ctl else 1.0)
        energy = est.expected_energy_kwh * energy_mult
        carbon = energy * carbon_intensity / 1000.0
        cost_usd = energy * self.electricity_cost_per_kwh.get(region, 0.20)
        extra = self._common_option_fields(
            carbon, energy, region, precision,
            getattr(est, "confidence", 0.5),
        )
        return SchedulingOption(
            mode=ExecutionMode.IMMEDIATE,
            node_id="current_node",
            region=region,
            start_time=_utcnow(),
            carbon_kgco2e=carbon,
            energy_kwh=energy,
            cost_usd=cost_usd,
            latency_hours=0.0,
            score=0.0,
            uncertainty_std=getattr(est, "uncertainty_std", 0.0),
            **extra,
        )

    async def _create_deferred_option(
        self, task: Dict[str, Any], region: str, deadline: datetime,
    ) -> Optional[SchedulingOption]:
        try:
            duration_hours = task.get("estimated_duration_hours", 1.0)
            if self.chaos and self.chaos.maybe_fail("deferred_window"):
                raise RuntimeError("chaos: deferred_window")
            window = await asyncio.wait_for(
                self.carbon_forecaster.find_optimal_execution_window(
                    duration_hours=duration_hours,
                    deadline=_ensure_aware(deadline),
                ),
                timeout=self.config.forecaster_timeout_s,
            )
            est = await self._safe_estimate_energy(task, window.avg_intensity)
            cost_usd = est.expected_energy_kwh * self.electricity_cost_per_kwh.get(region, 0.20)
            latency = (window.start_time - _utcnow()).total_seconds() / 3600.0
            extra = self._common_option_fields(
                est.expected_carbon_kgco2e, est.expected_energy_kwh,
                region, PrecisionLevel.FP32,
                getattr(est, "confidence", 0.5),
            )
            return SchedulingOption(
                mode=ExecutionMode.DEFERRED,
                node_id="current_node",
                region=region,
                start_time=window.start_time,
                carbon_kgco2e=est.expected_carbon_kgco2e,
                energy_kwh=est.expected_energy_kwh,
                cost_usd=cost_usd,
                latency_hours=max(0.0, latency),
                score=0.0,
                uncertainty_std=getattr(est, "uncertainty_std", 0.0),
                **extra,
            )
        except Exception as e:
            logger.warning(f"Could not create deferred option: {e}")
            return None

    async def _create_reallocated_option(
        self, task: Dict[str, Any], node: Dict[str, Any],
        region: str, carbon_intensity: float,
    ) -> SchedulingOption:
        est = await self._safe_estimate_energy(task, carbon_intensity)
        node_load = float(node.get("current_load", 0.5))
        energy = est.expected_energy_kwh * (1.0 + node_load * 0.1)
        carbon = energy * carbon_intensity / 1000.0
        cost_usd = energy * self.electricity_cost_per_kwh.get(region, 0.20)
        extra = self._common_option_fields(
            carbon, energy, region, PrecisionLevel.FP32,
            getattr(est, "confidence", 0.5),
        )
        return SchedulingOption(
            mode=ExecutionMode.REALLOCATED,
            node_id=str(node["node_id"]),
            region=region,
            start_time=_utcnow(),
            carbon_kgco2e=carbon,
            energy_kwh=energy,
            cost_usd=cost_usd,
            latency_hours=0.1,
            score=0.0,
            uncertainty_std=getattr(est, "uncertainty_std", 0.0),
            **extra,
        )

    async def _create_green_routed_option(
        self, task: Dict[str, Any], target_region: str,
    ) -> Optional[SchedulingOption]:
        try:
            target_intensity = await self._safe_get_intensity(target_region)
            est = await self._safe_estimate_energy(task, target_intensity)
            cost_usd = (est.expected_energy_kwh
                        * self.electricity_cost_per_kwh.get(target_region, 0.20)
                        * 1.05)  # data transfer overhead
            extra = self._common_option_fields(
                est.expected_carbon_kgco2e, est.expected_energy_kwh,
                target_region, PrecisionLevel.FP32,
                getattr(est, "confidence", 0.5),
            )
            return SchedulingOption(
                mode=ExecutionMode.GREEN_ROUTED,
                node_id=f"{target_region}_node",
                region=target_region,
                start_time=_utcnow(),
                carbon_kgco2e=est.expected_carbon_kgco2e,
                energy_kwh=est.expected_energy_kwh,
                cost_usd=cost_usd,
                latency_hours=0.2,
                score=0.0,
                uncertainty_std=getattr(est, "uncertainty_std", 0.0),
                **extra,
            )
        except Exception as e:
            logger.warning(f"Could not create green routed option: {e}")
            return None

    async def _create_precision_throttled_option(
        self, task: Dict[str, Any], region: str, carbon_intensity: float,
    ) -> Optional[SchedulingOption]:
        if not self.precision_ctl:
            return None
        try:
            precision = self.precision_ctl.select("normal")
            energy_mult = self.precision_ctl.energy_multiplier(precision)
            est = await self._safe_estimate_energy(task, carbon_intensity)
            energy = est.expected_energy_kwh * energy_mult
            carbon = energy * carbon_intensity / 1000.0
            cost_usd = energy * self.electricity_cost_per_kwh.get(region, 0.20)
            extra = self._common_option_fields(
                carbon, energy, region, precision,
                getattr(est, "confidence", 0.5) * 0.9,
            )
            return SchedulingOption(
                mode=ExecutionMode.PRECISION_THROTTLED,
                node_id="current_node",
                region=region,
                start_time=_utcnow(),
                carbon_kgco2e=carbon,
                energy_kwh=energy,
                cost_usd=cost_usd,
                latency_hours=0.05,
                score=0.0,
                uncertainty_std=getattr(est, "uncertainty_std", 0.0) + 0.05,
                **extra,
            )
        except Exception as e:
            logger.debug(f"Precision-throttled option failed: {e}")
            return None

    async def _create_market_arbitraged_option(
        self, task: Dict[str, Any], region: str, carbon_intensity: float,
    ) -> Optional[SchedulingOption]:
        """
        Run now, but buy credits to neutralize emissions.
        Reports gross carbon honestly; net is zero after credits.
        """
        if not self.market:
            return None
        try:
            est = await self._safe_estimate_energy(task, carbon_intensity)
            snap = self.market.get_snapshot()
            gross = est.expected_carbon_kgco2e
            credit_cost = gross / 1000.0 * snap.carbon_price_per_tco2_usd
            elec_cost = est.expected_energy_kwh * self.electricity_cost_per_kwh.get(region, 0.20)
            cost_usd = elec_cost + credit_cost

            helium_units = 0.0
            scarcity = float(self._last_helium_signal or 0.0) if hasattr(self, "_last_helium_signal") else 0.0
            if self.helium_profiler:
                helium_units = HeliumProfiler.estimate_units(
                    est.expected_energy_kwh, scarcity,
                )
            return SchedulingOption(
                mode=ExecutionMode.MARKET_ARBITRAGED,
                node_id="current_node",
                region=region,
                start_time=_utcnow(),
                carbon_kgco2e=gross,            # honest gross
                carbon_kgco2e_net=0.0,          # net after credits
                energy_kwh=est.expected_energy_kwh,
                cost_usd=cost_usd,
                latency_hours=0.0,
                score=0.0,
                helium_units=helium_units,
                helium_scarcity_score=scarcity,
                carbon_market_cost_usd=credit_cost,
                precision=PrecisionLevel.FP32.value,
                confidence=getattr(est, "confidence", 0.5),
                uncertainty_std=getattr(est, "uncertainty_std", 0.0),
            )
        except Exception as e:
            logger.debug(f"Market-arbitraged option failed: {e}")
            return None

    # ------------------------------------------------------------------
    # Scoring (pluggable over objectives)
    # ------------------------------------------------------------------

    def _calculate_scores(
        self,
        task: Dict[str, Any],
        options: List[SchedulingOption],
        active_weights: Optional[Dict[str, float]] = None,
    ) -> List[SchedulingOption]:
        weights = active_weights or self._normalized_base_weights()
        total = sum(weights.values()) or 1.0
        weights = {k: v / total for k, v in weights.items()}

        # Compute per-objective maxima once (used for min-max normalisation)
        per_max: Dict[str, float] = {}
        for spec in self.objectives:
            vals = [max(0.0, spec.extract(o)) for o in options]
            m = max(vals) if vals else 1.0
            per_max[spec.name] = m if m > 0 else 1.0

        kappa = self.config.risk_aversion_kappa

        for option in options:
            score = 0.0
            for spec in self.objectives:
                w = weights.get(spec.name, 0.0)
                if w == 0.0:
                    continue
                v = spec.extract(option)
                nv = max(0.0, v) / per_max[spec.name]
                if spec.direction == "max":
                    nv = 1.0 - nv
                if spec.transform:
                    nv = spec.transform(nv, task)
                score += w * nv
            # Risk-adjusted component
            if kappa > 0:
                score += kappa * option.uncertainty_std
            option.score = score
        return options

    # ------------------------------------------------------------------
    # Pareto (N-D over objectives)
    # ------------------------------------------------------------------

    def _is_pareto_efficient(
        self, option: SchedulingOption, alternatives: List[SchedulingOption],
    ) -> bool:
        opt_vec = [(s.extract(option) if s.direction == "min"
                    else -s.extract(option)) for s in self.objectives]
        for alt in alternatives:
            alt_vec = [(s.extract(alt) if s.direction == "min"
                        else -s.extract(alt)) for s in self.objectives]
            dominated = True
            strict = False
            for a, o in zip(alt_vec, opt_vec):
                if a > o:
                    dominated = False
                    break
                if a < o:
                    strict = True
            if dominated and strict:
                return False
        return True

    # ------------------------------------------------------------------
    # Reasoning
    # ------------------------------------------------------------------

    def _generate_reasoning(
        self, chosen: SchedulingOption, alternatives: List[SchedulingOption],
    ) -> str:
        immediate = next(
            (o for o in alternatives if o.mode == ExecutionMode.IMMEDIATE), None,
        )
        if immediate and chosen != immediate:
            c_saved = immediate.carbon_kgco2e - chosen.carbon_kgco2e
            c_pct = (c_saved / immediate.carbon_kgco2e * 100
                     if immediate.carbon_kgco2e > 0 else 0.0)
            parts = [
                f"Chose {chosen.mode.value} execution "
                f"saving {c_saved:.4f} kgCO₂e ({c_pct:.1f}%) vs immediate."
            ]
            if chosen.latency_hours > 0:
                parts.append(f"Delay: {chosen.latency_hours:.1f}h.")
            if chosen.region != immediate.region:
                parts.append(f"Routed to {chosen.region}.")
            if chosen.precision != immediate.precision:
                parts.append(f"Precision: {chosen.precision}.")
            if chosen.carbon_kgco2e_net != chosen.carbon_kgco2e:
                parts.append(
                    f"Net carbon {chosen.carbon_kgco2e_net:.4f} kg after credits."
                )
            return " ".join(parts)
        return f"Immediate execution optimal (score: {chosen.score:.3f})"

    # ------------------------------------------------------------------
    # Metrics + audit
    # ------------------------------------------------------------------

    def _record_metrics(
        self, task: Dict[str, Any], chosen: SchedulingOption,
        weights: Dict[str, float], t0: float,
    ) -> None:
        self.metrics.inc(
            "scheduler_decisions_total",
            mode=chosen.mode.value, region=chosen.region,
        )
        self.metrics.observe(
            "scheduler_score", chosen.score, mode=chosen.mode.value,
        )
        self.metrics.observe(
            "scheduler_carbon_net_kgco2e", chosen.carbon_kgco2e_net or 0.0,
        )
        self.metrics.observe(
            "scheduler_latency_s", time.perf_counter() - t0,
        )
        self.metrics.observe(
            "scheduler_precision", 0.0, precision=chosen.precision,
        )
        if chosen.hitl_required:
            self.metrics.inc("scheduler_hitl_required_total")
        self.metrics.inc(
            "scheduler_circuit_state", name="forecaster",
            state=self._forecaster_circuit.state.value,
        )

    def _audit_decision(
        self, task: Dict[str, Any], decision: SchedulingDecision,
    ) -> None:
        # Audit every decision; special attention to financial commits.
        entry: Dict[str, Any] = {
            "event": "schedule",
            "task_id": decision.task_id,
            "mode": decision.chosen_option.mode.value,
            "region": decision.chosen_option.region,
            "carbon_gross_kg": decision.chosen_option.carbon_kgco2e,
            "carbon_net_kg": decision.chosen_option.carbon_kgco2e_net,
            "energy_kwh": decision.chosen_option.energy_kwh,
            "cost_usd": decision.chosen_option.cost_usd,
            "market_cost_usd": decision.chosen_option.carbon_market_cost_usd,
            "hitl_required": decision.hitl_required,
            "hitl_approved": decision.hitl_approved,
            "deployment_id": self.deployment_id,
            "agent_id": self.agent_id,
        }
        if decision.chosen_option.mode == ExecutionMode.MARKET_ARBITRAGED:
            entry["event"] = "market_arbitrage"
        self.audit_log.record(entry)

    # ------------------------------------------------------------------
    # Federated contribution
    # ------------------------------------------------------------------

    def _push_federated_profile(self, option: SchedulingOption) -> None:
        if not self.federated:
            return
        self.federated.push(FederatedSchedulingProfile(
            deployment_id=self.deployment_id,
            key=f"scheduling:{option.region}:{option.mode.value}",
            mode=option.mode.value,
            mean_carbon_kg=option.carbon_kgco2e,
            mean_savings_pct=0.0,
            sample_count=1,
            granularity="single",
        ))
        self.federated.aggregate()

    def contribute_federated(self) -> None:
        if not self.federated:
            return
        grouped: Dict[str, List[SchedulingOption]] = defaultdict(list)
        for d in self.scheduling_history:
            key = f"scheduling:{d.chosen_option.region}:{d.chosen_option.mode.value}"
            grouped[key].append(d.chosen_option)
        for key, opts in grouped.items():
            if len(opts) < 3:
                continue
            self.federated.push(FederatedSchedulingProfile(
                deployment_id=self.deployment_id,
                key=key,
                mode=opts[0].mode.value,
                mean_carbon_kg=statistics.fmean(o.carbon_kgco2e for o in opts),
                mean_savings_pct=0.0,
                sample_count=len(opts),
                granularity="aggregate",
            ))
        self.federated.aggregate()

    def get_federated_aggregate(self) -> Dict[str, Dict[str, float]]:
        return self.federated.aggregate() if self.federated else {}

    # ------------------------------------------------------------------
    # Public APIs
    # ------------------------------------------------------------------

    def _circuit_states(self) -> Dict[str, str]:
        return {
            "forecaster": self._forecaster_circuit.state.value,
            "profiler": self._profiler_circuit.state.value,
            "cluster": self._cluster_circuit.state.value,
        }

    def set_hitl_callback(self, cb: Callable[[HITLRequest], bool]) -> None:
        if self.hitl:
            self.hitl.set_callback(cb)

    def set_hitl_async_callback(self, cb: Callable[[HITLRequest], Any]) -> None:
        if self.hitl:
            self.hitl.set_async_callback(cb)

    def active_learning_samples(self, n: int = 16) -> List[Dict[str, Any]]:
        return self.hitl.active_learning_batch(n) if self.hitl else []

    def explain_last_decision(self) -> Optional[Dict[str, Any]]:
        if not self.scheduling_history:
            return None
        return self.scheduling_history[-1].explanation

    def distill_estimator(
        self, urgency: str = "normal",
    ) -> Optional[DistilledEstimatorModel]:
        if not self.distiller:
            return None
        precision = (
            self.precision_ctl.select(urgency)
            if self.precision_ctl else PrecisionLevel.INT8
        )
        return self.distiller.distill(
            energy_coefficient=0.5,
            carbon_coefficient=0.4,
            precision=precision,
        )

    def record_actual_outcome(
        self,
        task_id: str,
        actual_carbon_kg: float,
        actual_energy_kwh: float,
        counterfactual_carbon_kg: Optional[float] = None,
    ) -> None:
        """
        Feed back actual outcomes. Reward measures how much we saved vs the
        counterfactual (defaults to the largest alternative in the decision),
        NOT estimator accuracy.
        """
        if not self.causal_learner:
            return
        match = next(
            (d for d in reversed(self.scheduling_history)
             if d.task_id == task_id),
            None,
        )
        if match is None:
            return
        if counterfactual_carbon_kg is None:
            counterfactual_carbon_kg = max(
                [a.carbon_kgco2e for a in match.alternatives]
                + [match.chosen_option.carbon_kgco2e, 1e-9]
            )
        reward = max(0.0, min(
            1.0,
            (counterfactual_carbon_kg - actual_carbon_kg)
            / max(counterfactual_carbon_kg, 1e-9),
        ))
        state = WeightState(
            region_hash=stable_hash(match.chosen_option.region),
            hour_of_day=_utcnow().hour,
            priority=0.5,
            recent_carbon_norm=min(1.0, actual_carbon_kg * 100.0),
            task_type_hash=0.5,
        )
        self.causal_learner.record(
            state,
            match.adaptive_weights_used or self._normalized_base_weights(),
            reward,
        )

    # ------------------------------------------------------------------
    # Health check
    # ------------------------------------------------------------------

    def health(self) -> Dict[str, Any]:
        """For k8s readiness/liveness probes."""
        circuits = self._circuit_states()
        ready = all(s != CircuitState.OPEN.value for s in circuits.values())
        return {
            "ready": ready,
            "circuits": circuits,
            "features": self.features,
            "deployment_id": self.deployment_id,
            "agent_id": self.agent_id,
            "decisions_served": len(self.scheduling_history),
            "degraded": sum(
                1 for d in self.scheduling_history if d.degraded
            ),
        }

    # ------------------------------------------------------------------
    # Statistics
    # ------------------------------------------------------------------

    def get_statistics(self) -> Dict[str, Any]:
        if not self.scheduling_history:
            return {"num_decisions": 0}

        mode_counts: Dict[str, int] = {}
        total_gross = 0.0
        total_net = 0.0
        total_energy = 0.0
        total_helium = 0.0
        total_market_cost = 0.0
        pareto_count = 0
        degraded_count = 0

        for d in self.scheduling_history:
            opt = d.chosen_option
            mode_counts[opt.mode.value] = mode_counts.get(opt.mode.value, 0) + 1
            total_gross += opt.carbon_kgco2e
            total_net += (opt.carbon_kgco2e_net or opt.carbon_kgco2e)
            total_energy += opt.energy_kwh
            total_helium += opt.helium_units
            total_market_cost += opt.carbon_market_cost_usd
            if d.pareto_efficient:
                pareto_count += 1
            if d.degraded:
                degraded_count += 1

        n = len(self.scheduling_history)
        stats: Dict[str, Any] = {
            "num_decisions": n,
            "mode_distribution": mode_counts,
            "total_carbon_kgco2e_gross": total_gross,
            "total_carbon_kgco2e_net": total_net,
            "total_energy_kwh": total_energy,
            "total_helium_units": total_helium,
            "total_carbon_market_cost_usd": total_market_cost,
            "avg_carbon_gross_per_task": total_gross / n,
            "avg_carbon_net_per_task": total_net / n,
            "pareto_efficient_rate": pareto_count / n,
            "degraded_decisions": degraded_count,
            "circuits": self._circuit_states(),
        }
        if self.temporal_monitor:
            stats["recent_temporal_violations"] = list(
                self.temporal_monitor.violations
            )[-5:]
        if self.coordinator:
            stats["agents"] = {
                aid: {"role": a.role.value, "calls": a.total_calls}
                for aid, a in self.coordinator.agents.items()
            }
        if self.chaos:
            stats["recent_chaos_events"] = list(self.chaos.events)[-5:]
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
            stats["causal_learner"] = {
                "observations": self.causal_learner.observations,
                "updates": self.causal_learner.updates,
                "last_advantage_mean": self.causal_learner.last_advantage_mean,
            }
        if self.federated:
            stats["federated_aggregate"] = self.federated.aggregate()
        stats["metrics_snapshot"] = self.metrics.snapshot()
        return stats


# =============================================================================
# Demo
# =============================================================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    class _StubForecaster:
        async def get_current_intensity(self, region: str = "US-CA") -> float:
            base = {"US-CA": 250, "US-NY": 300, "EU-DE": 350, "EU-FR": 80}
            return float(base.get(region, 400))

        async def find_optimal_execution_window(self, duration_hours, deadline):
            class W:
                start_time = _utcnow() + timedelta(hours=6)
                avg_intensity = 150.0
            return W()

    class _StubProfiler:
        async def estimate_energy(self, task, carbon_intensity=0.0):
            class E:
                expected_energy_kwh = 0.05
                expected_carbon_kgco2e = 0.05 * carbon_intensity / 1000.0
                confidence = 0.8
                uncertainty_std = 0.1
            return E()

    class _StubCluster:
        def get_cluster_info(self):
            return {"nodes": [
                {"node_id": "node-1", "current_load": 0.4},
                {"node_id": "node-2", "current_load": 0.7},
            ]}

    async def main():
        scheduler = MultiObjectiveScheduler(
            carbon_forecaster=_StubForecaster(),
            task_profiler=_StubProfiler(),
            ray_cluster=_StubCluster(),
            alpha=0.5, beta=0.3, gamma=0.1, delta=0.1,
            epsilon=0.02, zeta=0.02,
            deployment_id="us-ca-prod-01",
            agent_id="scheduler-A",
            hardware={"supports_int4": True, "vram_gb": 48},
        )

        async def auto_approve(req: HITLRequest) -> bool:
            logger.info(f"[HITL] auto-approve: {req.reason}")
            return True
        scheduler.set_hitl_async_callback(auto_approve)

        task = {
            "task_id": "bert_sentiment",
            "model_name": "bert-base-uncased",
            "dataset_size": 10_000,
            "num_epochs": 3,
            "batch_size": 32,
            "hardware": "V100",
            "region": "US-CA",
            "priority": 0.7,
            "deferrable": True,
            "deadline": _utcnow() + timedelta(hours=48),
        }

        decision = await scheduler.schedule(task)
        opt = decision.chosen_option

        print("\n=== Scheduling Decision ===")
        print(f"Task:          {decision.task_id}")
        print(f"Chosen mode:   {opt.mode.value}")
        print(f"Region:        {opt.region}")
        print(f"Carbon (gross):{opt.carbon_kgco2e:.4f} kgCO₂e")
        print(f"Carbon (net):  {opt.carbon_kgco2e_net:.4f} kgCO₂e")
        print(f"Energy:        {opt.energy_kwh:.4f} kWh")
        print(f"Helium:        {opt.helium_units:.5f} units")
        print(f"Market cost:   ${opt.carbon_market_cost_usd:.4f}")
        print(f"Cost:          ${opt.cost_usd:.4f}")
        print(f"Latency:       {opt.latency_hours:.2f}h")
        print(f"Precision:     {opt.precision}")
        print(f"Temporal OK:   {decision.temporal_verified}")
        print(f"Pareto eff:    {decision.pareto_efficient}")
        print(f"HITL:          {decision.hitl_required} (approved={decision.hitl_approved})")
        print(f"Circuits:      {decision.circuits}")
        print(f"Reasoning:     {decision.reasoning}")

        if decision.explanation:
            print("\n=== XAI ===")
            print(f"  {decision.explanation['headline']}")
            for line in decision.explanation["rationale"]:
                print(f"    • {line}")
            print(f"  Counterfactual: {decision.explanation['counterfactual']}")

        if decision.market_enrichment:
            print("\n=== Market ===")
            print(f"  Carbon cost:   ${decision.market_enrichment['carbon_market_cost_usd']:.4f}")
            print(f"  REC available: {decision.market_enrichment['rec_available_mwh']:.1f} MWh")

        scheduler.record_actual_outcome(
            task_id=decision.task_id,
            actual_carbon_kg=opt.carbon_kgco2e * 0.9,
            actual_energy_kwh=opt.energy_kwh * 0.9,
        )

        scheduler.contribute_federated()

        dist = scheduler.distill_estimator(urgency="normal")
        if dist:
            print("\n=== Distilled Estimator ===")
            print(f"  Precision:   {dist.precision.value}")
            print(f"  Retention:   {dist.quality_retention}")
            print(f"  Energy save: {dist.energy_reduction_percent}%")

        print("\n=== Health ===")
        print(scheduler.health())

        import json
        print("\n=== Statistics ===")
        print(json.dumps(scheduler.get_statistics(), indent=2, default=str))

    asyncio.run(main())
