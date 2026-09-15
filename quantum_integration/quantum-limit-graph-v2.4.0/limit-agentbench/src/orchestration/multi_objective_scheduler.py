"""
Multi-Objective Scheduler (Enhanced)
=====================================

Optimizes task scheduling across competing objectives:
- Carbon intensity (minimize)
- Energy consumption (minimize)
- Cost (minimize)
- Performance/latency (maximize)
- Helium consumption (minimize)          ← NEW dual-axis
- Carbon market cost (minimize)          ← NEW

Now with first-class support for the ten Green Agent enhancement layers:

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

Location: src/orchestration/multi_objective_scheduler.py
"""

from __future__ import annotations

import asyncio
import logging
import math
import random
import statistics
import time
from collections import deque, defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum
from typing import (
    Any, Callable, Deque, Dict, List, Optional, Protocol, Tuple,
)

try:
    import numpy as np  # type: ignore
    _HAS_NUMPY = True
except Exception:  # pragma: no cover
    np = None  # type: ignore
    _HAS_NUMPY = False

logger = logging.getLogger(__name__)


# =============================================================================
# Original enums & data classes (backward compatible, extended)
# =============================================================================

class ExecutionMode(Enum):
    """Execution modes for tasks (extended)."""
    IMMEDIATE = "immediate"
    DEFERRED = "deferred"
    REALLOCATED = "reallocated"
    GREEN_ROUTED = "green_routed"
    PRECISION_THROTTLED = "precision_throttled"     # NEW
    MARKET_ARBITRAGED = "market_arbitraged"          # NEW


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


@dataclass
class SchedulingOption:
    """A scheduling option with associated costs (extended)."""
    mode: ExecutionMode
    node_id: str
    region: str
    start_time: datetime
    carbon_kgco2e: float
    energy_kwh: float
    cost_usd: float
    latency_hours: float
    score: float
    # --- New optional fields (all defaulted) ---
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


@dataclass
class SchedulingDecision:
    """Final scheduling decision (extended)."""
    task_id: str
    chosen_option: SchedulingOption
    alternatives: List[SchedulingOption]
    reasoning: str
    pareto_efficient: bool
    # --- New optional fields ---
    explanation: Optional[Dict[str, Any]] = None
    temporal_verified: bool = True
    temporal_violations: List[str] = field(default_factory=list)
    market_enrichment: Optional[Dict[str, Any]] = None
    hitl_required: bool = False
    hitl_approved: Optional[bool] = None
    adaptive_weights_used: Optional[Dict[str, float]] = None
    causal_policy_used: bool = False
    federated_blend_applied: bool = False
    circuit_state: str = "closed"
    at: datetime = field(default_factory=datetime.now)


# =============================================================================
# ENHANCEMENT 9: Resilience — Circuit Breaker + Chaos Injector
# =============================================================================

@dataclass
class CircuitBreaker:
    name: str
    failure_threshold: int = 3
    recovery_timeout_seconds: int = 30
    state: CircuitState = CircuitState.CLOSED
    failures: int = 0
    last_failure_at: Optional[datetime] = None

    def can_call(self) -> bool:
        if self.state == CircuitState.CLOSED:
            return True
        if self.state == CircuitState.OPEN:
            if (self.last_failure_at and
                    (datetime.now() - self.last_failure_at).total_seconds()
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
        self.last_failure_at = datetime.now()
        if self.failures >= self.failure_threshold:
            self.state = CircuitState.OPEN
            logger.warning(f"Circuit '{self.name}' OPEN after {self.failures} failures")


class ChaosInjector:
    """Injects controlled faults for resilience testing."""
    def __init__(self, failure_rate: float = 0.0) -> None:
        self.failure_rate = failure_rate
        self.events: List[Dict[str, Any]] = []

    def maybe_fail(self, component: str) -> bool:
        if random.random() < self.failure_rate:
            self.events.append({
                "component": component,
                "at": datetime.now().isoformat(),
            })
            return True
        return False


# =============================================================================
# ENHANCEMENT 5: Temporal Logic & Formal Verification
# =============================================================================

class SchedulingProperty(Protocol):
    def check(self, option: SchedulingOption, task: Dict[str, Any]) -> bool: ...
    def name(self) -> str: ...


@dataclass
class DeadlineRespected:
    """G(option.start_time + latency <= task.deadline)."""
    def check(self, option: SchedulingOption, task: Dict[str, Any]) -> bool:
        deadline = task.get("deadline")
        if deadline is None:
            return True
        # Option's projected completion time must be <= deadline
        completion = option.start_time + timedelta(hours=option.latency_hours)
        return completion <= deadline

    def name(self) -> str:
        return "DeadlineRespected"


@dataclass
class EnergyBudgetNotExceeded:
    max_kwh: float = 100.0
    def check(self, option: SchedulingOption, task: Dict[str, Any]) -> bool:
        return option.energy_kwh <= self.max_kwh

    def name(self) -> str:
        return "EnergyBudgetNotExceeded"


@dataclass
class CarbonBudgetNotExceeded:
    max_kg: float = 5.0
    def check(self, option: SchedulingOption, task: Dict[str, Any]) -> bool:
        return option.carbon_kgco2e <= self.max_kg

    def name(self) -> str:
        return "CarbonBudgetNotExceeded"


@dataclass
class NonNegativeMetrics:
    def check(self, option: SchedulingOption, task: Dict[str, Any]) -> bool:
        return (
            option.carbon_kgco2e >= 0 and option.energy_kwh >= 0
            and option.cost_usd >= 0 and option.latency_hours >= 0
        )

    def name(self) -> str:
        return "NonNegativeMetrics"


class TemporalLogicMonitor:
    def __init__(self) -> None:
        self.properties: List[SchedulingProperty] = []
        self.violations: List[Dict[str, Any]] = []

    def register(self, p: SchedulingProperty) -> None:
        self.properties.append(p)

    def verify(
        self, option: SchedulingOption, task: Dict[str, Any]
    ) -> Tuple[bool, List[str]]:
        bad: List[str] = []
        for p in self.properties:
            if not p.check(option, task):
                bad.append(p.name())
                self.violations.append({
                    "property": p.name(),
                    "mode": option.mode.value,
                    "at": datetime.now().isoformat(),
                })
        return (len(bad) == 0, bad)


# =============================================================================
# ENHANCEMENT 6: Explainable AI (XAI)
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
        # Compare against immediate if present
        immediate = next(
            (a for a in alternatives if a.mode == ExecutionMode.IMMEDIATE),
            None,
        )
        if immediate and immediate is not chosen:
            carbon_savings = immediate.carbon_kgco2e - chosen.carbon_kgco2e
            pct = (
                carbon_savings / immediate.carbon_kgco2e * 100
                if immediate.carbon_kgco2e > 0 else 0.0
            )
            reasons.append(
                f"Carbon savings vs immediate: {carbon_savings:.4f} kgCO₂e "
                f"({pct:.1f}%)."
            )
        if chosen.latency_hours > 0:
            reasons.append(f"Deferral/transfer delay: {chosen.latency_hours:.2f}h.")
        if chosen.precision != "fp32":
            reasons.append(f"Precision switched to {chosen.precision}.")
        if chosen.helium_units > 0:
            reasons.append(f"Helium consumed: {chosen.helium_units:.5f} units.")
        if not chosen.temporal_verified:
            reasons.append(f"Temporal violations: {chosen.temporal_violations}.")
        reasons.append(
            f"Active objective weights: α={weights['alpha']:.2f}, "
            f"β={weights['beta']:.2f}, γ={weights['gamma']:.2f}, "
            f"δ={weights['delta']:.2f}."
        )
        if market_snapshot:
            cp = market_snapshot.get("carbon_price_per_tco2_usd")
            if cp:
                reasons.append(f"Carbon market price = ${cp:.2f}/tCO₂.")

        # Counterfactual
        counterfactual = "N/A"
        if immediate and immediate is not chosen:
            counterfactual = (
                f"If run immediately: {immediate.carbon_kgco2e:.4f} kgCO₂e "
                f"at {immediate.latency_hours:.2f}h latency."
            )

        return SchedulingExplanation(
            headline=f"[{chosen.mode.value}] score={chosen.score:.4f} "
                     f"carbon={chosen.carbon_kgco2e:.4f} kg",
            rationale=reasons,
            confidence=chosen.confidence,
            contributing_factors={
                "carbon_kgco2e": chosen.carbon_kgco2e,
                "energy_kwh": chosen.energy_kwh,
                "cost_usd": chosen.cost_usd,
                "latency_hours": chosen.latency_hours,
                "helium_units": chosen.helium_units,
            },
            counterfactual=counterfactual,
        )


# =============================================================================
# ENHANCEMENT 7: Adaptive Precision Switching
# =============================================================================

class AdaptivePrecisionController:
    """
    Precision selector for scheduling. Determines which precision level
    is appropriate for a given task + urgency.
    """
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
        if urgency == "critical":
            return PrecisionLevel.FP16
        if self.edge_device:
            return (PrecisionLevel.INT8 if self.supports_int8
                    else PrecisionLevel.FP16)
        return (PrecisionLevel.INT4 if self.supports_int4
                else PrecisionLevel.INT8)

    def energy_multiplier(self, precision: PrecisionLevel) -> float:
        return self.PRECISION_ENERGY_MULT[precision]


# =============================================================================
# ENHANCEMENT 2: Causal RL for adaptive objective weights
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
    Learns a contextual adjustment on top of the base weights
    (α, β, γ, δ) using historical scheduling outcomes.

    Features: [region_hash, hour/24, priority, carbon_norm, task_type_hash]
    Output: 4 deltas applied to base weights (then normalized).
    """
    N_FEATURES = 5
    N_WEIGHTS = 4

    def __init__(self, base_weights: Dict[str, float], lr: float = 0.02) -> None:
        self.base = dict(base_weights)
        self.lr = lr
        # Per-weight linear model over features
        self.weight_coeffs: List[List[float]] = [
            [0.0] * self.N_FEATURES for _ in range(self.N_WEIGHTS)
        ]
        self.buffer: Deque[Tuple[List[float], Dict[str, float], float]] = \
            deque(maxlen=512)
        self.observations: int = 0
        self.updates: int = 0

    @staticmethod
    def _features(s: WeightState) -> List[float]:
        return [
            s.region_hash,
            s.hour_of_day / 24.0,
            s.priority,
            s.min(1.0, max(0.0, s.recent_carbon_norm)),
            s.task_type_hash,
        ]

    def predict_weights(self, s: WeightState) -> Dict[str, float]:
        f = self._features(s)
        deltas = [
            sum(c * x for c, x in zip(coeffs, f))
            for coeffs in self.weight_coeffs
        ]
        keys = ("alpha", "beta", "gamma", "delta")
        adj = {
            k: max(0.01, self.base[k] + deltas[i])
            for i, k in enumerate(keys)
        }
        total = sum(adj.values()) or 1.0
        # Normalize to sum ~= 1.0
        return {k: v / total for k, v in adj.items()}

    def record(
        self,
        s: WeightState,
        weights_used: Dict[str, float],
        reward: float,
    ) -> None:
        self.buffer.append((self._features(s), dict(weights_used), reward))
        self.observations += 1

    def update(self) -> None:
        if not self.buffer:
            return
        keys = ("alpha", "beta", "gamma", "delta")
        for f, weights_used, reward in self.buffer:
            # Simple policy gradient: nudge coefficients toward reward
            for i, k in enumerate(keys):
                # Reward magnitude scaled by feature value
                for j, x in enumerate(f):
                    self.weight_coeffs[i][j] += self.lr * reward * x * 0.1
        self.updates += 1
        self.buffer.clear()


# Extend WeightState with a helper for safety
def _weight_state_min(self, a: float, b: float) -> float:
    return a if a < b else b


WeightState.min = _weight_state_min  # type: ignore[attr-defined]


# =============================================================================
# ENHANCEMENT 1: Quantum-Distillation of the estimator
# =============================================================================

@dataclass
class DistilledEstimatorModel:
    precision: PrecisionLevel
    energy_coefficient: float
    carbon_coefficient: float
    quality_retention: float
    energy_reduction_percent: float


class QuantumDistillationBridge:
    """Distills the scheduling estimator coefficients."""

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
# ENHANCEMENT 3: Federated Green Learning
# =============================================================================

@dataclass
class FederatedSchedulingProfile:
    deployment_id: str
    key: str  # e.g. "scheduling:US-CA"
    mode: str
    mean_carbon_kg: float
    mean_savings_pct: float
    sample_count: int
    timestamp: datetime = field(default_factory=datetime.now)


class FederatedAggregator:
    def __init__(self) -> None:
        self.updates: List[FederatedSchedulingProfile] = []
        self._global: Dict[str, Dict[str, float]] = {}

    def push(self, u: FederatedSchedulingProfile) -> None:
        self.updates.append(u)

    def aggregate(self) -> Dict[str, Dict[str, float]]:
        grouped: Dict[str, List[FederatedSchedulingProfile]] = defaultdict(list)
        for u in self.updates:
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
# ENHANCEMENT 4: Multi-Agent Coordination
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
# ENHANCEMENT 8: Carbon Markets & RECs
# =============================================================================

@dataclass
class MarketSnapshot:
    carbon_price_per_tco2_usd: float
    rec_price_per_mwh_usd: float
    rec_available_mwh: float
    timestamp: datetime = field(default_factory=datetime.now)


class CarbonMarketClient:
    def __init__(self) -> None:
        self._cache: Optional[MarketSnapshot] = None
        self._ttl = 300

    def get_snapshot(self) -> MarketSnapshot:
        if self._cache and (
            (datetime.now() - self._cache.timestamp).total_seconds() < self._ttl
        ):
            return self._cache
        snap = MarketSnapshot(
            carbon_price_per_tco2_usd=random.uniform(20, 80),
            rec_price_per_mwh_usd=random.uniform(3, 9),
            rec_available_mwh=random.uniform(10, 500),
        )
        self._cache = snap
        return snap


# =============================================================================
# ENHANCEMENT 10: HITL + Active Learning
# =============================================================================

@dataclass
class HITLRequest:
    task_id: str
    mode: str
    reason: str
    urgency: str
    context: Dict[str, Any] = field(default_factory=dict)
    requested_at: datetime = field(default_factory=datetime.now)


class HumanInTheLoopGate:
    def __init__(self, deferral_threshold_hours: float = 24.0) -> None:
        self.deferral_threshold_hours = deferral_threshold_hours
        self.pending: List[HITLRequest] = []
        self.feedback_log: List[Dict[str, Any]] = []
        self._callback: Optional[Callable[[HITLRequest], bool]] = None

    def set_callback(self, cb: Callable[[HITLRequest], bool]) -> None:
        self._callback = cb

    def needs_review(
        self, option: SchedulingOption, task: Dict[str, Any]
    ) -> bool:
        # Long deferral → review
        if option.latency_hours > self.deferral_threshold_hours:
            return True
        # Deadline-critical cross-region → review
        if (task.get("deadline_critical")
                and option.mode == ExecutionMode.GREEN_ROUTED):
            return True
        return False

    def request_review(
        self,
        task_id: str,
        mode: str,
        reason: str,
        urgency: str = "medium",
        context: Optional[Dict[str, Any]] = None,
    ) -> bool:
        req = HITLRequest(task_id, mode, reason, urgency, context or {})
        self.pending.append(req)
        if urgency == "low":
            self.pending.remove(req)
            self.feedback_log.append({"req": asdict(req), "decision": True})
            return True
        if self._callback is None:
            self.pending.remove(req)
            self.feedback_log.append({"req": asdict(req), "decision": False})
            return False
        decision = self._callback(req)
        self.pending.remove(req)
        self.feedback_log.append({"req": asdict(req), "decision": decision})
        return decision

    def active_learning_batch(self, n: int = 16) -> List[Dict[str, Any]]:
        return self.feedback_log[-n:]


# =============================================================================
# Helium Profiling
# =============================================================================

class HeliumProfiler:
    """Estimate helium consumption from energy and precision."""
    HELIUM_UNITS_PER_KWH = 0.05

    @classmethod
    def estimate_units(
        cls, energy_kwh: float, scarcity_score: float = 0.0
    ) -> float:
        base = energy_kwh * cls.HELIUM_UNITS_PER_KWH
        efficiency = 1.0 - min(0.5, scarcity_score * 0.5)
        return base * efficiency


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
        deployment_id: str = "local",
        agent_id: str = "scheduler-0",
        features: Optional[Dict[str, bool]] = None,
        hardware: Optional[Dict[str, Any]] = None,
    ):
        # --- Original state ---
        self.carbon_forecaster = carbon_forecaster
        self.task_profiler = task_profiler
        self.ray_cluster = ray_cluster
        self.alpha = alpha
        self.beta = beta
        self.gamma = gamma
        self.delta = delta
        self.electricity_cost_per_kwh = {
            "US-CA": 0.20, "US-NY": 0.18, "EU-DE": 0.25, "EU-FR": 0.22,
        }
        self.scheduling_history: List[SchedulingDecision] = []

        # --- Enhancement config ---
        self.deployment_id = deployment_id
        self.agent_id = agent_id
        self.features = {**self.DEFAULT_FEATURES, **(features or {})}

        # --- Resilience ---
        self._forecaster_circuit = CircuitBreaker("forecaster", failure_threshold=3)
        self._profiler_circuit = CircuitBreaker("profiler", failure_threshold=3)
        self._cluster_circuit = CircuitBreaker("cluster", failure_threshold=3)
        self.chaos = (
            ChaosInjector(failure_rate=0.0)
            if self.features["chaos_testing"] else None
        )

        # --- Temporal logic ---
        self.temporal_monitor: Optional[TemporalLogicMonitor] = (
            TemporalLogicMonitor() if self.features["temporal_logic"] else None
        )
        if self.temporal_monitor:
            self.temporal_monitor.register(DeadlineRespected())
            self.temporal_monitor.register(EnergyBudgetNotExceeded())
            self.temporal_monitor.register(CarbonBudgetNotExceeded())
            self.temporal_monitor.register(NonNegativeMetrics())

        # --- XAI ---
        self.explainer = SchedulingExplainer() if self.features["xai"] else None

        # --- Adaptive precision ---
        self.precision_ctl = (
            AdaptivePrecisionController(hardware)
            if self.features["adaptive_precision"] else None
        )

        # --- Causal RL ---
        self.causal_learner = (
            CausalWeightLearner(
                base_weights={
                    "alpha": alpha, "beta": beta,
                    "gamma": gamma, "delta": delta,
                },
                lr=0.02,
            )
            if self.features["causal_rl"] else None
        )

        # --- Quantum distillation ---
        self.distiller = (
            QuantumDistillationBridge()
            if self.features["quantum_distillation"] else None
        )

        # --- Federated ---
        self.federated = (
            FederatedAggregator() if self.features["federated"] else None
        )

        # --- Multi-agent ---
        self.coordinator = (
            MultiAgentCoordinator() if self.features["multi_agent"] else None
        )
        if self.coordinator:
            self.coordinator.register(self.agent_id)

        # --- Carbon markets ---
        self.market = (
            CarbonMarketClient() if self.features["carbon_market"] else None
        )

        # --- HITL ---
        self.hitl = HumanInTheLoopGate() if self.features["hitl"] else None

        # --- Helium ---
        self.helium_profiler = (
            HeliumProfiler() if self.features["helium_profiling"] else None
        )
        self._last_helium_signal: Any = None

        logger.info(
            f"Enhanced Multi-objective scheduler initialized "
            f"(deployment={deployment_id}, agent={agent_id}, "
            f"α={alpha}, β={beta}, γ={gamma}, δ={delta}, "
            f"features={list(self.features)})"
        )

    # ------------------------------------------------------------------
    # Main scheduling entry point (backward compatible)
    # ------------------------------------------------------------------

    async def schedule(
        self,
        task: Dict[str, Any],
        consider_deferral: bool = True,
        consider_reallocation: bool = True,
    ) -> SchedulingDecision:
        """
        Find optimal scheduling for task. Backward-compatible signature.
        """
        # --- Generate options ---
        options = await self._generate_options(
            task=task,
            consider_deferral=consider_deferral,
            consider_reallocation=consider_reallocation,
        )

        if not options:
            raise ValueError("No feasible scheduling options found")

        # --- Adaptive objective weights (causal RL) ---
        active_weights = {
            "alpha": self.alpha, "beta": self.beta,
            "gamma": self.gamma, "delta": self.delta,
        }
        causal_policy_used = False
        if self.causal_learner:
            state = self._build_weight_state(task)
            active_weights = self.causal_learner.predict_weights(state)
            causal_policy_used = True

        # --- Score options with active weights ---
        scored_options = self._calculate_scores(
            task, options, active_weights=active_weights,
        )

        # --- Temporal verification per option ---
        if self.temporal_monitor:
            for opt in scored_options:
                ok, violations = self.temporal_monitor.verify(opt, task)
                opt.temporal_verified = ok
                opt.temporal_violations = violations
            # Filter out options that violate hard temporal properties
            valid = [o for o in scored_options if o.temporal_verified]
            if valid:
                scored_options = valid

        # --- Sort by score (lower is better) ---
        scored_options.sort(key=lambda opt: opt.score)

        # --- Select best option ---
        best_option = scored_options[0]

        # --- Federated blend hint ---
        federated_blend_applied = False
        if self.federated:
            key = f"scheduling:{best_option.region}:{best_option.mode.value}"
            profile = self.federated.lookup(key)
            if profile and profile.get("sample_count", 0) >= 5:
                federated_blend_applied = True
                best_option.federated_blend_applied = True

        # --- HITL for critical decisions ---
        if self.hitl and self.hitl.needs_review(best_option, task):
            best_option.hitl_required = True
            approved = self.hitl.request_review(
                task_id=task.get("task_id", "unknown"),
                mode=best_option.mode.value,
                reason=f"long deferral ({best_option.latency_hours:.1f}h) "
                       f"or deadline-critical cross-region routing",
                urgency="high" if task.get("deadline_critical") else "medium",
                context={"latency_hours": best_option.latency_hours},
            )
            best_option.hitl_approved = approved
            if approved is False:
                # Downgrade to IMMEDIATE
                immediate = next(
                    (o for o in scored_options
                     if o.mode == ExecutionMode.IMMEDIATE), None
                )
                if immediate:
                    best_option = immediate

        # --- Pareto efficiency ---
        pareto_efficient = self._is_pareto_efficient(
            best_option, scored_options[1:]
        )

        # --- Reasoning (backward compatible) ---
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
            exp = SchedulingExplainer.explain(
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
            market_enrichment = {
                "carbon_price_per_tco2_usd": snap.carbon_price_per_tco2_usd,
                "carbon_market_cost_usd": (
                    best_option.carbon_kgco2e / 1000.0 * snap.carbon_price_per_tco2_usd
                ),
                "rec_price_per_mwh_usd": snap.rec_price_per_mwh_usd,
                "rec_available_mwh": snap.rec_available_mwh,
            }

        # --- Build decision ---
        decision = SchedulingDecision(
            task_id=task.get("task_id", "unknown"),
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
            circuit_state=self._forecaster_circuit.state.value,
        )
        self.scheduling_history.append(decision)

        # --- Causal feedback ---
        if self.causal_learner:
            reward = 1.0 if best_option.temporal_verified else 0.3
            if self.scheduling_history:
                recent = self.scheduling_history[-1]
                if recent.chosen_option.carbon_kgco2e > 0:
                    reward *= min(
                        2.0,
                        1.0 + (1.0 - best_option.carbon_kgco2e
                               / max(recent.chosen_option.carbon_kgco2e, 1e-6)),
                    )
            self.causal_learner.record(
                self._build_weight_state(task),
                active_weights,
                reward,
            )
            if self.causal_learner.observations % 10 == 0:
                self.causal_learner.update()

        # --- Multi-agent feedback ---
        if self.coordinator:
            self.coordinator.record(
                agent_id=self.agent_id,
                success=best_option.temporal_verified,
                carbon_saved_pct=(
                    best_option.explanation and 5.0 or 0.0
                ),
                latency_hours=best_option.latency_hours,
            )

        # --- Federated contribution (every 5 decisions) ---
        if self.federated and len(self.scheduling_history) % 5 == 0:
            self._push_federated_profile(best_option)

        logger.info(
            f"Scheduled task {task.get('task_id')} as "
            f"{best_option.mode.value} "
            f"(score: {best_option.score:.3f}, "
            f"carbon: {best_option.carbon_kgco2e:.4f} kgCO2e)"
        )
        return decision

    # ------------------------------------------------------------------
    # Weight-state helper
    # ------------------------------------------------------------------

    def _build_weight_state(self, task: Dict[str, Any]) -> WeightState:
        region = task.get("region", "US-CA")
        recent_carbon = 0.0
        if self.scheduling_history:
            recent_carbon = statistics.fmean(
                d.chosen_option.carbon_kgco2e
                for d in self.scheduling_history[-10:]
            )
        return WeightState(
            region_hash=float(hash(region) % 1000) / 1000.0,
            hour_of_day=datetime.now().hour,
            priority=float(task.get("priority", 0.5)),
            recent_carbon_norm=min(1.0, recent_carbon * 100.0),
            task_type_hash=float(hash(task.get("task_type", "")) % 1000) / 1000.0,
        )

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

        # --- Get current intensity with circuit breaker ---
        current_region = task.get("region", "US-CA")
        current_intensity = await self._safe_get_intensity(current_region)

        # --- Immediate ---
        immediate_option = await self._create_immediate_option(
            task=task, region=current_region,
            carbon_intensity=current_intensity,
        )
        options.append(immediate_option)

        # --- Deferred ---
        if consider_deferral and task.get("deferrable", True):
            deadline = task.get("deadline")
            if deadline:
                deferred_option = await self._create_deferred_option(
                    task=task, region=current_region, deadline=deadline,
                )
                if deferred_option:
                    options.append(deferred_option)

        # --- Reallocated ---
        if consider_reallocation:
            try:
                cluster_info = await self._safe_get_cluster_info()
                available_nodes = cluster_info.get("nodes", [])
                for node in available_nodes[:3]:
                    if node.get("node_id") != immediate_option.node_id:
                        realloc = await self._create_reallocated_option(
                            task=task, node=node, region=current_region,
                            carbon_intensity=current_intensity,
                        )
                        options.append(realloc)
            except Exception as e:
                logger.debug(f"Reallocation options unavailable: {e}")

        # --- Green routed ---
        if consider_reallocation:
            alternative_regions = ["US-CA", "US-NY", "EU-DE", "EU-FR"]
            alternative_regions = [
                r for r in alternative_regions if r != current_region
            ]
            for region in alternative_regions[:2]:
                green_option = await self._create_green_routed_option(
                    task=task, target_region=region,
                )
                if green_option:
                    options.append(green_option)

        # --- NEW: Precision-throttled option ---
        if self.precision_ctl:
            throttled = await self._create_precision_throttled_option(
                task=task, region=current_region,
                carbon_intensity=current_intensity,
            )
            if throttled:
                options.append(throttled)

        # --- NEW: Market-arbitraged option ---
        if self.market:
            arbitraged = await self._create_market_arbitraged_option(
                task=task, region=current_region,
                carbon_intensity=current_intensity,
            )
            if arbitraged:
                options.append(arbitraged)

        return options

    # ------------------------------------------------------------------
    # Circuit breaker wrappers
    # ------------------------------------------------------------------

    async def _safe_get_intensity(self, region: str) -> float:
        """Fetch intensity with circuit breaker + fallback."""
        if not self._forecaster_circuit.can_call():
            logger.debug("Forecaster circuit open — using default 400.0")
            return 400.0
        try:
            if self.chaos and self.chaos.maybe_fail("forecaster"):
                raise RuntimeError("chaos: forecaster")
            v = await self.carbon_forecaster.get_current_intensity(region)
            self._forecaster_circuit.record_success()
            return float(v)
        except Exception as e:
            self._forecaster_circuit.record_failure()
            logger.warning(f"Intensity fetch failed: {e}; using 400.0")
            return 400.0

    async def _safe_get_cluster_info(self) -> Dict[str, Any]:
        if not self._cluster_circuit.can_call():
            return {"nodes": []}
        try:
            if self.chaos and self.chaos.maybe_fail("cluster"):
                raise RuntimeError("chaos: cluster")
            info = self.ray_cluster.get_cluster_info()
            self._cluster_circuit.record_success()
            return info or {"nodes": []}
        except Exception as e:
            self._cluster_circuit.record_failure()
            logger.warning(f"Cluster info fetch failed: {e}")
            return {"nodes": []}

    async def _safe_estimate_energy(
        self, task: Dict[str, Any], carbon_intensity: float
    ) -> Any:
        if not self._profiler_circuit.can_call():
            return self._default_estimate(task, carbon_intensity)
        try:
            if self.chaos and self.chaos.maybe_fail("profiler"):
                raise RuntimeError("chaos: profiler")
            est = await self.task_profiler.estimate_energy(
                task=task, carbon_intensity=carbon_intensity,
            )
            self._profiler_circuit.record_success()
            return est
        except Exception as e:
            self._profiler_circuit.record_failure()
            logger.warning(f"Energy estimate failed: {e}; using default")
            return self._default_estimate(task, carbon_intensity)

    def _default_estimate(self, task: Dict[str, Any], carbon_intensity: float) -> Any:
        """Fallback estimate when profiler fails."""
        class _FallbackEstimate:
            def __init__(self, energy: float, carbon: float, conf: float):
                self.expected_energy_kwh = energy
                self.expected_carbon_kgco2e = carbon
                self.confidence = conf
        energy = max(0.001, task.get("dataset_size", 1000) / 1e6)
        carbon = energy * carbon_intensity / 1000.0
        return _FallbackEstimate(energy, carbon, 0.3)

    # ------------------------------------------------------------------
    # Option constructors (original + extended)
    # ------------------------------------------------------------------

    async def _create_immediate_option(
        self, task: Dict[str, Any], region: str, carbon_intensity: float,
    ) -> SchedulingOption:
        estimate = await self._safe_estimate_energy(task, carbon_intensity)
        electricity_cost = self.electricity_cost_per_kwh.get(region, 0.20)
        cost_usd = estimate.expected_energy_kwh * electricity_cost

        # Precision integration
        precision = PrecisionLevel.FP32
        energy_mult = 1.0
        if self.precision_ctl:
            urgency = "critical" if task.get("deadline_critical") else "normal"
            precision = self.precision_ctl.select(urgency)
            energy_mult = self.precision_ctl.energy_multiplier(precision)

        adjusted_energy = estimate.expected_energy_kwh * energy_mult
        adjusted_carbon = adjusted_energy * carbon_intensity / 1000.0

        # Helium
        helium_units = 0.0
        if self.helium_profiler:
            helium_units = HeliumProfiler.estimate_units(adjusted_energy)

        # Market cost
        market_cost = 0.0
        if self.market:
            snap = self.market.get_snapshot()
            market_cost = (
                adjusted_carbon / 1000.0 * snap.carbon_price_per_tco2_usd
            )

        return SchedulingOption(
            mode=ExecutionMode.IMMEDIATE,
            node_id="current_node",
            region=region,
            start_time=datetime.now(),
            carbon_kgco2e=adjusted_carbon,
            energy_kwh=adjusted_energy,
            cost_usd=cost_usd,
            latency_hours=0.0,
            score=0.0,
            helium_units=helium_units,
            carbon_market_cost_usd=market_cost,
            precision=precision.value,
            confidence=getattr(estimate, "confidence", 0.5),
        )

    async def _create_deferred_option(
        self, task: Dict[str, Any], region: str, deadline: datetime,
    ) -> Optional[SchedulingOption]:
        try:
            duration_hours = task.get("estimated_duration_hours", 1.0)
            if self.chaos and self.chaos.maybe_fail("deferred_window"):
                raise RuntimeError("chaos: deferred_window")
            optimal_window = await self.carbon_forecaster.find_optimal_execution_window(
                duration_hours=duration_hours, deadline=deadline,
            )
            estimate = await self._safe_estimate_energy(
                task, optimal_window.avg_intensity,
            )
            electricity_cost = self.electricity_cost_per_kwh.get(region, 0.20)
            cost_usd = estimate.expected_energy_kwh * electricity_cost
            latency_hours = (
                optimal_window.start_time - datetime.now()
            ).total_seconds() / 3600

            # Helium + market
            helium_units = 0.0
            if self.helium_profiler:
                helium_units = HeliumProfiler.estimate_units(
                    estimate.expected_energy_kwh
                )
            market_cost = 0.0
            if self.market:
                snap = self.market.get_snapshot()
                market_cost = (
                    estimate.expected_carbon_kgco2e / 1000.0
                    * snap.carbon_price_per_tco2_usd
                )

            return SchedulingOption(
                mode=ExecutionMode.DEFERRED,
                node_id="current_node",
                region=region,
                start_time=optimal_window.start_time,
                carbon_kgco2e=estimate.expected_carbon_kgco2e,
                energy_kwh=estimate.expected_energy_kwh,
                cost_usd=cost_usd,
                latency_hours=latency_hours,
                score=0.0,
                helium_units=helium_units,
                carbon_market_cost_usd=market_cost,
                precision="fp32",
                confidence=getattr(estimate, "confidence", 0.5),
            )
        except Exception as e:
            logger.warning(f"Could not create deferred option: {e}")
            return None

    async def _create_reallocated_option(
        self, task: Dict[str, Any], node: Dict[str, Any],
        region: str, carbon_intensity: float,
    ) -> SchedulingOption:
        estimate = await self._safe_estimate_energy(task, carbon_intensity)
        node_load = node.get("current_load", 0.5)
        adjusted_energy = estimate.expected_energy_kwh * (1.0 + node_load * 0.1)
        adjusted_carbon = adjusted_energy * carbon_intensity / 1000.0
        electricity_cost = self.electricity_cost_per_kwh.get(region, 0.20)
        cost_usd = adjusted_energy * electricity_cost

        helium_units = 0.0
        if self.helium_profiler:
            helium_units = HeliumProfiler.estimate_units(adjusted_energy)
        market_cost = 0.0
        if self.market:
            snap = self.market.get_snapshot()
            market_cost = (
                adjusted_carbon / 1000.0 * snap.carbon_price_per_tco2_usd
            )

        return SchedulingOption(
            mode=ExecutionMode.REALLOCATED,
            node_id=node["node_id"],
            region=region,
            start_time=datetime.now(),
            carbon_kgco2e=adjusted_carbon,
            energy_kwh=adjusted_energy,
            cost_usd=cost_usd,
            latency_hours=0.1,
            score=0.0,
            helium_units=helium_units,
            carbon_market_cost_usd=market_cost,
            precision="fp32",
            confidence=getattr(estimate, "confidence", 0.5),
        )

    async def _create_green_routed_option(
        self, task: Dict[str, Any], target_region: str,
    ) -> Optional[SchedulingOption]:
        try:
            target_intensity = await self._safe_get_intensity(target_region)
            estimate = await self._safe_estimate_energy(task, target_intensity)
            electricity_cost = self.electricity_cost_per_kwh.get(
                target_region, 0.20,
            )
            cost_usd = estimate.expected_energy_kwh * electricity_cost
            cost_usd *= 1.05  # data transfer overhead

            helium_units = 0.0
            if self.helium_profiler:
                helium_units = HeliumProfiler.estimate_units(
                    estimate.expected_energy_kwh
                )
            market_cost = 0.0
            if self.market:
                snap = self.market.get_snapshot()
                market_cost = (
                    estimate.expected_carbon_kgco2e / 1000.0
                    * snap.carbon_price_per_tco2_usd
                )

            return SchedulingOption(
                mode=ExecutionMode.GREEN_ROUTED,
                node_id=f"{target_region}_node",
                region=target_region,
                start_time=datetime.now(),
                carbon_kgco2e=estimate.expected_carbon_kgco2e,
                energy_kwh=estimate.expected_energy_kwh,
                cost_usd=cost_usd,
                latency_hours=0.2,
                score=0.0,
                helium_units=helium_units,
                carbon_market_cost_usd=market_cost,
                precision="fp32",
                confidence=getattr(estimate, "confidence", 0.5),
            )
        except Exception as e:
            logger.warning(f"Could not create green routed option: {e}")
            return None

    async def _create_precision_throttled_option(
        self, task: Dict[str, Any], region: str, carbon_intensity: float,
    ) -> Optional[SchedulingOption]:
        """NEW: precision-throttled option using a lower-precision execution."""
        if not self.precision_ctl:
            return None
        try:
            # Always use the lowest viable precision for this option
            precision = self.precision_ctl.select("normal")
            energy_mult = self.precision_ctl.energy_multiplier(precision)

            estimate = await self._safe_estimate_energy(task, carbon_intensity)
            adjusted_energy = estimate.expected_energy_kwh * energy_mult
            adjusted_carbon = adjusted_energy * carbon_intensity / 1000.0
            electricity_cost = self.electricity_cost_per_kwh.get(region, 0.20)
            cost_usd = adjusted_energy * electricity_cost

            helium_units = 0.0
            if self.helium_profiler:
                helium_units = HeliumProfiler.estimate_units(adjusted_energy)
            market_cost = 0.0
            if self.market:
                snap = self.market.get_snapshot()
                market_cost = (
                    adjusted_carbon / 1000.0 * snap.carbon_price_per_tco2_usd
                )

            return SchedulingOption(
                mode=ExecutionMode.PRECISION_THROTTLED,
                node_id="current_node",
                region=region,
                start_time=datetime.now(),
                carbon_kgco2e=adjusted_carbon,
                energy_kwh=adjusted_energy,
                cost_usd=cost_usd,
                latency_hours=0.05,
                score=0.0,
                helium_units=helium_units,
                carbon_market_cost_usd=market_cost,
                precision=precision.value,
                confidence=getattr(estimate, "confidence", 0.5) * 0.9,
            )
        except Exception as e:
            logger.debug(f"Precision-throttled option failed: {e}")
            return None

    async def _create_market_arbitraged_option(
        self, task: Dict[str, Any], region: str, carbon_intensity: float,
    ) -> Optional[SchedulingOption]:
        """
        NEW: market-arbitraged option — run now but buy carbon credits to
        offset emissions.
        """
        if not self.market:
            return None
        try:
            estimate = await self._safe_estimate_energy(task, carbon_intensity)
            snap = self.market.get_snapshot()
            carbon_cost = (
                estimate.expected_carbon_kgco2e / 1000.0
                * snap.carbon_price_per_tco2_usd
            )
            electricity_cost = self.electricity_cost_per_kwh.get(region, 0.20)
            cost_usd = (
                estimate.expected_energy_kwh * electricity_cost + carbon_cost
            )

            helium_units = 0.0
            if self.helium_profiler:
                helium_units = HeliumProfiler.estimate_units(
                    estimate.expected_energy_kwh
                )

            return SchedulingOption(
                mode=ExecutionMode.MARKET_ARBITRAGED,
                node_id="current_node",
                region=region,
                start_time=datetime.now(),
                carbon_kgco2e=0.0,  # net-zero after credits
                energy_kwh=estimate.expected_energy_kwh,
                cost_usd=cost_usd,
                latency_hours=0.0,
                score=0.0,
                helium_units=helium_units,
                carbon_market_cost_usd=carbon_cost,
                precision="fp32",
                confidence=getattr(estimate, "confidence", 0.5),
            )
        except Exception as e:
            logger.debug(f"Market-arbitraged option failed: {e}")
            return None

    # ------------------------------------------------------------------
    # Scoring
    # ------------------------------------------------------------------

    def _calculate_scores(
        self,
        task: Dict[str, Any],
        options: List[SchedulingOption],
        active_weights: Optional[Dict[str, float]] = None,
    ) -> List[SchedulingOption]:
        """Calculate multi-objective scores using active (possibly learned) weights."""
        weights = active_weights or {
            "alpha": self.alpha, "beta": self.beta,
            "gamma": self.gamma, "delta": self.delta,
        }
        priority = float(task.get("priority", 0.5))

        carbon_values = [opt.carbon_kgco2e for opt in options] or [1.0]
        energy_values = [opt.energy_kwh for opt in options] or [1.0]
        cost_values = [opt.cost_usd for opt in options] or [1.0]
        latency_values = [opt.latency_hours for opt in options] or [1.0]
        helium_values = [opt.helium_units for opt in options] or [0.0]

        max_carbon = max(carbon_values) or 1.0
        max_energy = max(energy_values) or 1.0
        max_cost = max(cost_values) or 1.0
        max_latency = max(latency_values) or 1.0
        max_helium = max(helium_values) or 1.0

        for option in options:
            n_carbon = option.carbon_kgco2e / max_carbon if max_carbon > 0 else 0
            n_energy = option.energy_kwh / max_energy if max_energy > 0 else 0
            n_cost = option.cost_usd / max_cost if max_cost > 0 else 0
            n_latency = option.latency_hours / max_latency if max_latency > 0 else 0
            n_helium = option.helium_units / max_helium if max_helium > 0 else 0

            option.score = (
                weights["alpha"] * n_carbon
                + weights["beta"] * n_energy
                + weights["gamma"] * n_cost
                + weights["delta"] * n_latency * (1.0 - priority)
                + 0.05 * n_helium  # helium always contributes
            )
        return options

    # ------------------------------------------------------------------
    # Pareto efficiency (unchanged)
    # ------------------------------------------------------------------

    def _is_pareto_efficient(
        self, option: SchedulingOption, alternatives: List[SchedulingOption],
    ) -> bool:
        for alt in alternatives:
            if (alt.carbon_kgco2e <= option.carbon_kgco2e
                    and alt.energy_kwh <= option.energy_kwh
                    and alt.cost_usd <= option.cost_usd
                    and alt.latency_hours <= option.latency_hours
                    and (alt.carbon_kgco2e < option.carbon_kgco2e
                         or alt.energy_kwh < option.energy_kwh
                         or alt.cost_usd < option.cost_usd
                         or alt.latency_hours < option.latency_hours)):
                return False
        return True

    # ------------------------------------------------------------------
    # Reasoning (backward compatible)
    # ------------------------------------------------------------------

    def _generate_reasoning(
        self, chosen: SchedulingOption, alternatives: List[SchedulingOption],
    ) -> str:
        immediate = next(
            (opt for opt in alternatives if opt.mode == ExecutionMode.IMMEDIATE),
            None,
        )
        if immediate and chosen != immediate:
            carbon_savings = immediate.carbon_kgco2e - chosen.carbon_kgco2e
            carbon_savings_pct = (
                carbon_savings / immediate.carbon_kgco2e * 100
                if immediate.carbon_kgco2e > 0 else 0
            )
            reasoning = (
                f"Chose {chosen.mode.value} execution "
                f"saving {carbon_savings:.4f} kgCO2e "
                f"({carbon_savings_pct:.1f}%) vs immediate execution. "
            )
            if chosen.latency_hours > 0:
                reasoning += f"Delay: {chosen.latency_hours:.1f} hours. "
            if chosen.region != immediate.region:
                reasoning += f"Routed to {chosen.region} (cleaner grid). "
            if chosen.precision != "fp32":
                reasoning += f"Precision: {chosen.precision}. "
            return reasoning
        return f"Immediate execution optimal (score: {chosen.score:.3f})"

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
        ))
        self.federated.aggregate()

    def contribute_federated(self) -> None:
        """Aggregate all local decisions into a federated profile."""
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
            ))
        self.federated.aggregate()

    def get_federated_aggregate(self) -> Dict[str, Dict[str, float]]:
        return self.federated.aggregate() if self.federated else {}

    # ------------------------------------------------------------------
    # Public enhancement APIs
    # ------------------------------------------------------------------

    def set_hitl_callback(self, cb: Callable[[HITLRequest], bool]) -> None:
        if self.hitl:
            self.hitl.set_callback(cb)

    def active_learning_samples(self, n: int = 16) -> List[Dict[str, Any]]:
        return self.hitl.active_learning_batch(n) if self.hitl else []

    def explain_last_decision(self) -> Optional[Dict[str, Any]]:
        if not self.scheduling_history:
            return None
        return self.scheduling_history[-1].explanation

    def distill_estimator(
        self, urgency: str = "normal"
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
    ) -> None:
        """
        Feed back actual outcomes for causal weight learning.
        Called after a scheduled task completes.
        """
        if not self.causal_learner:
            return
        # Find the matching decision
        match = next(
            (d for d in reversed(self.scheduling_history)
             if d.task_id == task_id),
            None,
        )
        if match is None:
            return
        predicted = match.chosen_option.carbon_kgco2e
        actual = max(actual_carbon_kg, 1e-9)
        # Relative accuracy reward
        reward = max(0.0, 1.0 - abs(predicted - actual) / actual)
        # Rebuild a plausible state (best-effort)
        state = WeightState(
            region_hash=float(hash(match.chosen_option.region) % 1000) / 1000.0,
            hour_of_day=datetime.now().hour,
            priority=0.5,
            recent_carbon_norm=min(1.0, actual * 100.0),
            task_type_hash=0.5,
        )
        self.causal_learner.record(
            state, match.adaptive_weights_used or {
                "alpha": self.alpha, "beta": self.beta,
                "gamma": self.gamma, "delta": self.delta,
            }, reward,
        )

    # ------------------------------------------------------------------
    # Statistics (backward compatible + extended)
    # ------------------------------------------------------------------

    def get_statistics(self) -> Dict[str, Any]:
        """Get scheduling statistics (backward compatible, extended)."""
        if not self.scheduling_history:
            return {"num_decisions": 0}

        mode_counts: Dict[str, int] = {}
        total_carbon = 0.0
        total_energy = 0.0
        total_helium = 0.0
        pareto_count = 0

        for d in self.scheduling_history:
            mode = d.chosen_option.mode.value
            mode_counts[mode] = mode_counts.get(mode, 0) + 1
            total_carbon += d.chosen_option.carbon_kgco2e
            total_energy += d.chosen_option.energy_kwh
            total_helium += d.chosen_option.helium_units
            if d.pareto_efficient:
                pareto_count += 1

        stats: Dict[str, Any] = {
            "num_decisions": len(self.scheduling_history),
            "mode_distribution": mode_counts,
            "total_carbon_kgco2e": total_carbon,
            "total_energy_kwh": total_energy,
            "total_helium_units": total_helium,
            "avg_carbon_per_task": total_carbon / len(self.scheduling_history),
            "pareto_efficient_rate": pareto_count / len(self.scheduling_history),
            "circuits": {
                "forecaster": self._forecaster_circuit.state.value,
                "profiler": self._profiler_circuit.state.value,
                "cluster": self._cluster_circuit.state.value,
            },
        }
        if self.temporal_monitor:
            stats["temporal_violations"] = self.temporal_monitor.violations[-5:]
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
            stats["causal_learner"] = {
                "observations": self.causal_learner.observations,
                "updates": self.causal_learner.updates,
            }
        if self.federated:
            stats["federated_aggregate"] = self.federated.aggregate()
        return stats


# =============================================================================
# Demo
# =============================================================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # --- Stub dependencies for demo ---
    class _StubForecaster:
        async def get_current_intensity(self, region: str = "US-CA") -> float:
            base = {"US-CA": 250, "US-NY": 300, "EU-DE": 350, "EU-FR": 80}
            return float(base.get(region, 400))
        async def find_optimal_execution_window(self, duration_hours, deadline):
            class W:
                start_time = datetime.now() + timedelta(hours=6)
                avg_intensity = 150.0
            return W()

    class _StubProfiler:
        async def estimate_energy(self, task, carbon_intensity=0.0):
            class E:
                expected_energy_kwh = 0.05
                expected_carbon_kgco2e = 0.05 * carbon_intensity / 1000.0
                confidence = 0.8
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
            deployment_id="us-ca-prod-01",
            agent_id="scheduler-A",
            hardware={"supports_int4": True, "vram_gb": 48},
        )

        def auto_approve(req: HITLRequest) -> bool:
            logger.info(f"[HITL] auto-approve: {req.reason}")
            return True
        scheduler.set_hitl_callback(auto_approve)

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
            "deadline": datetime.now() + timedelta(hours=48),
        }

        decision = await scheduler.schedule(task)

        print(f"\n=== Scheduling Decision ===")
        print(f"Task:        {decision.task_id}")
        print(f"Chosen mode: {decision.chosen_option.mode.value}")
        print(f"Region:      {decision.chosen_option.region}")
        print(f"Carbon:      {decision.chosen_option.carbon_kgco2e:.4f} kgCO₂e")
        print(f"Energy:      {decision.chosen_option.energy_kwh:.4f} kWh")
        print(f"Helium:      {decision.chosen_option.helium_units:.5f} units")
        print(f"Cost:        ${decision.chosen_option.cost_usd:.4f}")
        print(f"Latency:     {decision.chosen_option.latency_hours:.2f}h")
        print(f"Precision:   {decision.chosen_option.precision}")
        print(f"Temporal OK: {decision.temporal_verified}")
        print(f"Pareto eff:  {decision.pareto_efficient}")
        print(f"HITL:        {decision.hitl_required} (approved={decision.hitl_approved})")
        print(f"Circuit:     {decision.circuit_state}")
        print(f"Reasoning:   {decision.reasoning}")

        if decision.explanation:
            print(f"\n=== XAI ===")
            print(f"  {decision.explanation['headline']}")
            for line in decision.explanation['rationale']:
                print(f"    • {line}")
            print(f"  Counterfactual: {decision.explanation['counterfactual']}")

        if decision.market_enrichment:
            print(f"\n=== Market ===")
            print(f"  Carbon cost: ${decision.market_enrichment['carbon_market_cost_usd']:.4f}")
            print(f"  REC available: {decision.market_enrichment['rec_available_mwh']:.1f} MWh")

        # Feed back actual outcome
        scheduler.record_actual_outcome(
            task_id=decision.task_id,
            actual_carbon_kg=decision.chosen_option.carbon_kgco2e * 0.9,
            actual_energy_kwh=decision.chosen_option.energy_kwh * 0.9,
        )

        # Federated
        scheduler.contribute_federated()

        # Distill estimator
        dist = scheduler.distill_estimator(urgency="normal")
        if dist:
            print(f"\n=== Distilled Estimator ===")
            print(f"  Precision:   {dist.precision.value}")
            print(f"  Retention:   {dist.quality_retention}")
            print(f"  Energy save: {dist.energy_reduction_percent}%")

        # Statistics
        import json
        print(f"\n=== Statistics ===")
        print(json.dumps(scheduler.get_statistics(), indent=2, default=str))

    asyncio.run(main())
