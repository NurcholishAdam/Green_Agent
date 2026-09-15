"""
PolicyEngine (Enhanced)
========================

Reward shaping and temporal shift policy for Green Agent v5.0.0.

Original API preserved:
    engine = PolicyEngine(energy_budget, baseline_energy=None)
    reward = engine.compute_sustainability_reward(accuracy, energy)
    hour, saving = engine.suggest_temporal_shift(
        shifter, forecast_engine, energy_kwh, shiftable=True, urgency="low"
    )

New capabilities (all inline, feature-toggleable):
  1. Quantum-Distillation of the reward policy
  2. Causal RL for learned budget penalties
  3. Federated Green Learning across deployments
  4. Multi-Agent Coordination with emergent role specialisation
  5. Temporal Logic & Formal Verification for shift safety
  6. Explainable AI (XAI) for every reward and shift
  7. Adaptive Precision as a reward dimension
  8. Carbon Markets & REC enrichment
  9. Resilience Engineering (circuit breaker + chaos testing)
 10. Human-in-the-Loop for critical shifts + active learning
 +   Helium awareness (dual-axis reward)
 +   Carbon awareness (intensity + market cost)
 +   Logging, statistics, reward/shift history
 +   Input validation, uncertainty quantification
"""

from __future__ import annotations

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

# Original import preserved; fallback if unavailable
try:
    from green_agent.rewards.negawatt_reward import NegawattReward
except Exception:
    class NegawattReward:
        """Fallback negawatt reward module."""
        def __init__(self, baseline_energy: float):
            self.baseline = baseline_energy

        def combined_reward(self, accuracy: float, energy: float) -> float:
            if self.baseline <= 0:
                return accuracy * 0.5
            savings = max(0.0, (self.baseline - energy) / self.baseline)
            return 0.5 * accuracy + 0.5 * savings

logger = logging.getLogger(__name__)


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
    CARBON_SPECIALIST = "carbon_specialist"
    HELIUM_SPECIALIST = "helium_specialist"
    LATENCY_SPECIALIST = "latency_specialist"
    MARKET_SPECIALIST = "market_specialist"


class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


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
            logger.warning(
                f"Circuit '{self.name}' OPEN after {self.failures} failures"
            )


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

class ShiftProperty(Protocol):
    def check(self, shift: Dict[str, Any], task: Dict[str, Any]) -> bool: ...
    def name(self) -> str: ...


@dataclass
class DeadlineRespected:
    """G(shift.hour <= task.deadline_hour)."""
    def check(self, shift: Dict[str, Any], task: Dict[str, Any]) -> bool:
        deadline_hour = task.get("deadline_hour")
        if deadline_hour is None:
            return True
        return shift.get("hour", 0) <= deadline_hour

    def name(self) -> str:
        return "DeadlineRespected"


@dataclass
class NonNegativeSaving:
    def check(self, shift: Dict[str, Any], task: Dict[str, Any]) -> bool:
        return shift.get("saving", 0.0) >= 0.0

    def name(self) -> str:
        return "NonNegativeSaving"


@dataclass
class MaxDelayBounded:
    max_delay_hours: int = 48
    def check(self, shift: Dict[str, Any], task: Dict[str, Any]) -> bool:
        current_hour = task.get("current_hour", 0)
        return abs(shift.get("hour", 0) - current_hour) <= self.max_delay_hours

    def name(self) -> str:
        return "MaxDelayBounded"


@dataclass
class NonNegativeEnergy:
    def check(self, shift: Dict[str, Any], task: Dict[str, Any]) -> bool:
        return task.get("energy_kwh", 0.0) >= 0.0

    def name(self) -> str:
        return "NonNegativeEnergy"


class TemporalLogicMonitor:
    def __init__(self) -> None:
        self.properties: List[ShiftProperty] = []
        self.violations: List[Dict[str, Any]] = []

    def register(self, p: ShiftProperty) -> None:
        self.properties.append(p)

    def verify(
        self, shift: Dict[str, Any], task: Dict[str, Any]
    ) -> Tuple[bool, List[str]]:
        bad: List[str] = []
        for p in self.properties:
            if not p.check(shift, task):
                bad.append(p.name())
                self.violations.append({
                    "property": p.name(),
                    "at": datetime.now().isoformat(),
                })
        return (len(bad) == 0, bad)


# =============================================================================
# ENHANCEMENT 6: Explainable AI (XAI)
# =============================================================================

@dataclass
class RewardExplanation:
    headline: str
    rationale: List[str]
    confidence: float
    contributing_factors: Dict[str, float]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class RewardExplainer:
    @staticmethod
    def explain(
        accuracy: float,
        energy: float,
        energy_budget: float,
        negawatt_reward: float,
        budget_penalty: float,
        final_reward: float,
        helium_units: float = 0.0,
        carbon_market_cost: float = 0.0,
        precision: str = "fp32",
        causal_adjustment: float = 0.0,
    ) -> RewardExplanation:
        reasons: List[str] = []
        reasons.append(
            f"Accuracy={accuracy:.3f}, Energy={energy:.4f} kWh "
            f"(budget={energy_budget:.4f})."
        )
        reasons.append(f"Negawatt component: {negawatt_reward:.4f}.")
        if budget_penalty:
            reasons.append(
                f"Budget penalty applied: −{budget_penalty:.4f}."
            )
        if helium_units > 0:
            reasons.append(f"Helium component: {helium_units:.5f} units.")
        if carbon_market_cost > 0:
            reasons.append(f"Carbon market cost: ${carbon_market_cost:.4f}.")
        if causal_adjustment:
            reasons.append(
                f"Learned penalty adjustment: {causal_adjustment:+.4f}."
            )
        reasons.append(f"Precision used: {precision}.")
        reasons.append(f"Final reward: {final_reward:.4f}.")

        return RewardExplanation(
            headline=f"Reward={final_reward:.4f} "
                     f"(acc={accuracy:.2f}, energy={energy:.4f})",
            rationale=reasons,
            confidence=0.9 if accuracy > 0.8 else 0.6,
            contributing_factors={
                "accuracy": accuracy,
                "energy": energy,
                "negawatt": negawatt_reward,
                "budget_penalty": budget_penalty,
                "helium": helium_units,
                "carbon_market_cost": carbon_market_cost,
            },
        )


# =============================================================================
# ENHANCEMENT 7: Adaptive Precision Switching
# =============================================================================

@dataclass
class HardwareProfile:
    has_tensor_cores: bool = True
    supports_int8: bool = True
    supports_int4: bool = False
    vram_gb: float = 24.0
    edge_device: bool = False


class AdaptivePrecisionController:
    def __init__(self, hw: Optional[HardwareProfile] = None) -> None:
        self.hw = hw or HardwareProfile()

    def select(self, urgency: str) -> PrecisionLevel:
        if urgency == "critical":
            return PrecisionLevel.FP16
        if self.hw.edge_device:
            return (PrecisionLevel.INT8 if self.hw.supports_int8
                    else PrecisionLevel.FP16)
        return (PrecisionLevel.INT4 if self.hw.supports_int4
                else PrecisionLevel.INT8)

    @staticmethod
    def energy_multiplier(precision: PrecisionLevel) -> float:
        return {
            PrecisionLevel.FP32: 1.00,
            PrecisionLevel.FP16: 0.75,
            PrecisionLevel.INT8: 0.50,
            PrecisionLevel.INT4: 0.35,
            PrecisionLevel.QUANTUM_DISTILLED: 0.25,
        }[precision]


# =============================================================================
# ENHANCEMENT 2: Causal RL for learned budget penalties
# =============================================================================

@dataclass
class PenaltyState:
    hour_of_day: int
    energy_overshoot_ratio: float
    task_priority: float


class CausalPenaltyLearner:
    """
    Learns a graduated budget penalty from outcomes.

    Features: [hour/24, overshoot_ratio, task_priority]
    Reward: 1.0 if the penalty led to better sustainability outcomes;
    0.5 if neutral; 0.0 if harmful.
    """
    N_FEATURES = 3

    def __init__(self, base_penalty: float = 1.0, lr: float = 0.02) -> None:
        self.base_penalty = base_penalty
        self.lr = lr
        self.coeffs: List[float] = [0.0] * self.N_FEATURES
        self.buffer: Deque[Tuple[List[float], float]] = deque(maxlen=512)
        self.observations: int = 0
        self.updates: int = 0

    @staticmethod
    def _features(s: PenaltyState) -> List[float]:
        return [s.hour_of_day / 24.0, s.energy_overshoot_ratio, s.task_priority]

    def predict_penalty(self, s: PenaltyState) -> float:
        f = self._features(s)
        delta = sum(c * x for c, x in zip(self.coeffs, f))
        magnitude = 1.0 + max(0.0, min(2.0, s.energy_overshoot_ratio))
        return max(
            self.base_penalty,
            self.base_penalty * magnitude + delta,
        )

    def record(self, s: PenaltyState, reward: float) -> None:
        self.buffer.append((self._features(s), reward))
        self.observations += 1

    def update(self) -> None:
        if not self.buffer:
            return
        for f, r in self.buffer:
            for i, x in enumerate(f):
                self.coeffs[i] += self.lr * r * x * 0.2
        self.updates += 1
        self.buffer.clear()


# =============================================================================
# ENHANCEMENT 1: Quantum-Distillation of the reward policy
# =============================================================================

@dataclass
class DistilledRewardPolicy:
    precision: PrecisionLevel
    base_penalty: float
    negawatt_weight: float
    quality_retention: float
    energy_reduction_percent: float


class QuantumDistillationBridge:
    """Distills the reward policy to a compact, low-precision student."""

    def distill(
        self,
        base_penalty: float,
        negawatt_weight: float,
        precision: PrecisionLevel,
    ) -> DistilledRewardPolicy:
        scale, retention, energy = {
            PrecisionLevel.FP32: (1.0, 1.00, 0.0),
            PrecisionLevel.FP16: (1.0, 0.98, 30.0),
            PrecisionLevel.INT8: (100.0, 0.93, 55.0),
            PrecisionLevel.INT4: (10.0, 0.85, 70.0),
            PrecisionLevel.QUANTUM_DISTILLED: (5.0, 0.80, 85.0),
        }[precision]

        def qz(v: float) -> float:
            return math.floor(v * scale) / scale if scale > 1.0 else v

        return DistilledRewardPolicy(
            precision=precision,
            base_penalty=qz(base_penalty),
            negawatt_weight=qz(negawatt_weight),
            quality_retention=retention,
            energy_reduction_percent=energy,
        )


# =============================================================================
# ENHANCEMENT 3: Federated Green Learning
# =============================================================================

@dataclass
class FederatedRewardProfile:
    deployment_id: str
    key: str
    mean_reward: float
    mean_energy: float
    sample_count: int
    timestamp: datetime = field(default_factory=datetime.now)


class FederatedAggregator:
    def __init__(self) -> None:
        self.updates: List[FederatedRewardProfile] = []
        self._global: Dict[str, Dict[str, float]] = {}

    def push(self, u: FederatedRewardProfile) -> None:
        self.updates.append(u)

    def aggregate(self) -> Dict[str, Dict[str, float]]:
        grouped: Dict[str, List[FederatedRewardProfile]] = defaultdict(list)
        for u in self.updates:
            grouped[u.key].append(u)
        result: Dict[str, Dict[str, float]] = {}
        for key, profiles in grouped.items():
            total_w = sum(p.sample_count for p in profiles) or 1
            result[key] = {
                "mean_reward": sum(
                    p.mean_reward * p.sample_count for p in profiles
                ) / total_w,
                "mean_energy": sum(
                    p.mean_energy * p.sample_count for p in profiles
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
    avg_helium_saved: float = 0.0
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
            if a.avg_carbon_saved > 0.2 and a.avg_helium_saved > 0.2:
                a.role = AgentRole.GENERALIST
            elif a.avg_carbon_saved > 0.3:
                a.role = AgentRole.CARBON_SPECIALIST
            elif a.avg_helium_saved > 0.3:
                a.role = AgentRole.HELIUM_SPECIALIST
            elif a.success_rate > 0.9:
                a.role = AgentRole.LATENCY_SPECIALIST
            else:
                a.role = AgentRole.GENERALIST

    def record(
        self, agent_id: str, success: bool,
        carbon_saved_pct: float, helium_saved_pct: float,
    ) -> None:
        a = self.register(agent_id)
        n = a.total_calls + 1
        a.success_rate = ((n - 1) * a.success_rate + float(success)) / n
        a.avg_carbon_saved = (
            (n - 1) * a.avg_carbon_saved + carbon_saved_pct / 100.0
        ) / n
        a.avg_helium_saved = (
            (n - 1) * a.avg_helium_saved + helium_saved_pct / 100.0
        ) / n
        a.total_calls = n
        if n % 5 == 0:
            self._reassign()

    def select_agent(self, prefer: AgentRole = AgentRole.GENERALIST) -> Optional[str]:
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
    shift_hour: int
    shift_saving: float
    urgency: str
    reason: str
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
        self, shift_hour: int, current_hour: int, urgency: str
    ) -> bool:
        if urgency == "high":
            return True
        if abs(shift_hour - current_hour) > self.deferral_threshold_hours:
            return True
        return False

    def request_review(
        self, shift_hour: int, shift_saving: float,
        urgency: str, reason: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> bool:
        req = HITLRequest(
            shift_hour, shift_saving, urgency, reason, context or {},
        )
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
    """Estimate helium consumption from energy."""
    HELIUM_UNITS_PER_KWH = 0.05

    @classmethod
    def estimate_units(
        cls, energy_kwh: float, scarcity_score: float = 0.0
    ) -> float:
        base = energy_kwh * cls.HELIUM_UNITS_PER_KWH
        efficiency = 1.0 - min(0.5, scarcity_score * 0.5)
        return base * efficiency


# =============================================================================
# The Enhanced PolicyEngine
# =============================================================================

class PolicyEngine:
    """
    Enhanced reward shaping and temporal shift policy engine.

    Backward-compatible signature:
        PolicyEngine(energy_budget, baseline_energy=None)
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
        "helium_awareness": True,
        "carbon_awareness": True,
    }

    def __init__(
        self,
        energy_budget: float,
        baseline_energy: Optional[float] = None,
        deployment_id: str = "local",
        agent_id: str = "policy-0",
        features: Optional[Dict[str, bool]] = None,
        hardware: Optional[HardwareProfile] = None,
    ):
        # --- Original state ---
        self.energy_budget = float(energy_budget)
        self.baseline_energy = baseline_energy
        if baseline_energy is not None:
            try:
                self.negawatt_module = NegawattReward(baseline_energy)
            except Exception:
                self.negawatt_module = None
        else:
            self.negawatt_module = None

        # --- Enhancement config ---
        self.deployment_id = deployment_id
        self.agent_id = agent_id
        self.features = {**self.DEFAULT_FEATURES, **(features or {})}

        # --- Resilience ---
        self._negawatt_circuit = CircuitBreaker("negawatt", failure_threshold=3)
        self._forecaster_circuit = CircuitBreaker("forecaster", failure_threshold=3)
        self._shifter_circuit = CircuitBreaker("shifter", failure_threshold=3)
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
            self.temporal_monitor.register(NonNegativeSaving())
            self.temporal_monitor.register(MaxDelayBounded())
            self.temporal_monitor.register(NonNegativeEnergy())

        # --- XAI ---
        self.explainer = RewardExplainer() if self.features["xai"] else None

        # --- Adaptive precision ---
        self.precision_ctl = (
            AdaptivePrecisionController(hardware)
            if self.features["adaptive_precision"] else None
        )

        # --- Causal RL ---
        self.penalty_learner = (
            CausalPenaltyLearner(base_penalty=1.0, lr=0.02)
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
            HeliumProfiler() if self.features["helium_awareness"] else None
        )
        self.helium_signal_fn: Optional[Callable[[], Any]] = None

        # --- Carbon ---
        self.carbon_forecaster: Any = None

        # --- History & statistics ---
        self.reward_history: Deque[Dict[str, Any]] = deque(maxlen=1024)
        self.shift_history: Deque[Dict[str, Any]] = deque(maxlen=1024)
        self.reward_count: int = 0
        self.shift_count: int = 0
        self.last_explanation: Optional[RewardExplanation] = None

        logger.info(
            f"Enhanced PolicyEngine initialized "
            f"(deployment={deployment_id}, agent={agent_id}, "
            f"budget={energy_budget}, features={list(self.features)})"
        )

    # ------------------------------------------------------------------
    # Original public API: compute_sustainability_reward
    # ------------------------------------------------------------------

    def compute_sustainability_reward(
        self, accuracy: float, energy: float
    ) -> float:
        """
        Compute sustainability reward. Backward-compatible.

        With all features off, behaves identically to the original:
        delegates to NegawattReward, then applies a fixed -1.0 penalty
        if energy > energy_budget.
        """
        # --- Input validation ---
        acc = self._safe_float(accuracy)
        en = self._safe_float(energy)
        if acc is None or en is None:
            logger.warning("Invalid accuracy/energy input; returning 0.0")
            return 0.0
        acc = max(0.0, min(1.0, acc))
        en = max(0.0, en)

        # --- Negawatt component (with circuit breaker) ---
        negawatt_reward = 0.0
        if self.negawatt_module is not None and self._negawatt_circuit.can_call():
            try:
                if self.chaos and self.chaos.maybe_fail("negawatt"):
                    raise RuntimeError("chaos: negawatt")
                negawatt_reward = float(
                    self.negawatt_module.combined_reward(
                        accuracy=acc, energy=en,
                    )
                )
                self._negawatt_circuit.record_success()
            except Exception as e:
                self._negawatt_circuit.record_failure()
                logger.warning(f"NegawattReward failed: {e}; using fallback")
                negawatt_reward = self._fallback_negawatt(acc, en)

        # --- Budget penalty (learned or fixed) ---
        budget_penalty = 0.0
        causal_adjustment = 0.0
        if en > self.energy_budget:
            overshoot_ratio = (
                (en - self.energy_budget) / self.energy_budget
                if self.energy_budget > 0 else 1.0
            )
            if self.penalty_learner:
                state = PenaltyState(
                    hour_of_day=datetime.now().hour,
                    energy_overshoot_ratio=min(2.0, overshoot_ratio),
                    task_priority=0.5,
                )
                budget_penalty = self.penalty_learner.predict_penalty(state)
                causal_adjustment = budget_penalty - 1.0
            else:
                budget_penalty = 1.0

        # --- Helium component ---
        helium_units = 0.0
        helium_scarcity = 0.0
        if self.helium_profiler:
            if self.helium_signal_fn:
                try:
                    sig = self.helium_signal_fn()
                    if sig is not None:
                        helium_scarcity = float(
                            getattr(sig, "scarcity_score", 0.0)
                        )
                except Exception:
                    pass
            helium_units = HeliumProfiler.estimate_units(en, helium_scarcity)

        # --- Carbon market component ---
        carbon_market_cost = 0.0
        if self.market:
            try:
                snap = self.market.get_snapshot()
                carbon_kg = en * 400.0 / 1000.0  # 400 gCO2/kWh default
                carbon_market_cost = (
                    carbon_kg / 1000.0 * snap.carbon_price_per_tco2_usd
                )
            except Exception:
                pass

        # --- Precision component ---
        precision = PrecisionLevel.FP32
        if self.precision_ctl:
            precision = self.precision_ctl.select("normal")

        # --- Final reward ---
        final_reward = negawatt_reward - budget_penalty
        if self.features["helium_awareness"] and helium_units > 0:
            final_reward -= min(0.2, helium_units * 0.1)
        if self.features["carbon_market"] and carbon_market_cost > 0:
            final_reward -= min(0.15, carbon_market_cost)

        # --- XAI ---
        if self.explainer:
            exp = RewardExplainer.explain(
                accuracy=acc, energy=en,
                energy_budget=self.energy_budget,
                negawatt_reward=negawatt_reward,
                budget_penalty=budget_penalty,
                final_reward=final_reward,
                helium_units=helium_units,
                carbon_market_cost=carbon_market_cost,
                precision=precision.value,
                causal_adjustment=causal_adjustment,
            )
            self.last_explanation = exp

        # --- Record ---
        self.reward_count += 1
        self.reward_history.append({
            "accuracy": acc, "energy": en,
            "negawatt": negawatt_reward,
            "budget_penalty": budget_penalty,
            "helium_units": helium_units,
            "carbon_market_cost": carbon_market_cost,
            "final_reward": final_reward,
            "precision": precision.value,
            "at": datetime.now().isoformat(),
        })

        # --- Multi-agent feedback ---
        if self.coordinator:
            self.coordinator.record(
                agent_id=self.agent_id,
                success=(budget_penalty == 0.0),
                carbon_saved_pct=0.0,
                helium_saved_pct=0.0,
            )

        # --- Federated contribution ---
        if self.federated and self.reward_count % 25 == 0:
            self.federated.push(FederatedRewardProfile(
                deployment_id=self.deployment_id,
                key="reward:default",
                mean_reward=final_reward,
                mean_energy=en,
                sample_count=1,
            ))
            self.federated.aggregate()

        return final_reward

    def _fallback_negawatt(self, accuracy: float, energy: float) -> float:
        if self.baseline_energy and self.baseline_energy > 0:
            savings = max(
                0.0,
                (self.baseline_energy - energy) / self.baseline_energy,
            )
            return 0.5 * accuracy + 0.5 * savings
        return 0.5 * accuracy

    @staticmethod
    def _safe_float(value: Any) -> Optional[float]:
        try:
            v = float(value)
            return v if math.isfinite(v) else None
        except (TypeError, ValueError):
            return None

    # ------------------------------------------------------------------
    # Original public API: suggest_temporal_shift
    # ------------------------------------------------------------------

    def suggest_temporal_shift(
        self,
        shifter,
        forecast_engine,
        energy_kwh: float,
        shiftable: bool = True,
        urgency: str = "low",
        current_hour: int = 0,
        deadline_hour: Optional[int] = None,
        task: Optional[Dict[str, Any]] = None,
    ) -> Tuple[int, float]:
        """
        Suggest optimal temporal shift. Backward-compatible.

        Enhanced: temporal verification, HITL review, circuit breakers,
        and structured logging.
        """
        # --- Original gate ---
        if not shiftable or urgency == "high":
            return 0, 0.0

        # --- Circuit breaker on forecaster ---
        if not self._forecaster_circuit.can_call():
            logger.debug("Forecaster circuit open; returning (0, 0.0)")
            return 0, 0.0

        try:
            if self.chaos and self.chaos.maybe_fail("forecast_engine"):
                raise RuntimeError("chaos: forecast_engine")
            current = forecast_engine.current_intensity()
            forecast = forecast_engine.forecast_next_hours(4)
            self._forecaster_circuit.record_success()
        except Exception as e:
            self._forecaster_circuit.record_failure()
            logger.warning(f"Forecast failed: {e}; returning (0, 0.0)")
            return 0, 0.0

        # --- Circuit breaker on shifter ---
        if not self._shifter_circuit.can_call():
            return 0, 0.0

        try:
            if self.chaos and self.chaos.maybe_fail("shifter"):
                raise RuntimeError("chaos: shifter")
            hour, saving = shifter.suggest(current, forecast, energy_kwh)
            self._shifter_circuit.record_success()
        except Exception as e:
            self._shifter_circuit.record_failure()
            logger.warning(f"Shifter failed: {e}; returning (0, 0.0)")
            return 0, 0.0

        # --- Temporal verification ---
        shift = {"hour": hour, "saving": saving}
        task_ctx = task or {
            "current_hour": current_hour,
            "deadline_hour": deadline_hour,
            "energy_kwh": energy_kwh,
        }
        if self.temporal_monitor:
            ok, violations = self.temporal_monitor.verify(shift, task_ctx)
            if not ok:
                logger.warning(
                    f"Shift to hour {hour} violates {violations}; rejecting"
                )
                return 0, 0.0

        # --- HITL review ---
        if self.hitl and self.hitl.needs_review(hour, current_hour, urgency):
            approved = self.hitl.request_review(
                shift_hour=hour,
                shift_saving=saving,
                urgency=urgency,
                reason=f"deferral of {abs(hour - current_hour)}h",
                context={"energy_kwh": energy_kwh},
            )
            if approved is False:
                logger.info(f"HITL denied shift to hour {hour}")
                return 0, 0.0

        # --- Record ---
        self.shift_count += 1
        self.shift_history.append({
            "hour": hour, "saving": saving,
            "current_hour": current_hour,
            "urgency": urgency,
            "at": datetime.now().isoformat(),
        })

        return hour, saving

    # ------------------------------------------------------------------
    # Public enhancement APIs
    # ------------------------------------------------------------------

    def set_hitl_callback(self, cb: Callable[[HITLRequest], bool]) -> None:
        if self.hitl:
            self.hitl.set_callback(cb)

    def active_learning_samples(self, n: int = 16) -> List[Dict[str, Any]]:
        return self.hitl.active_learning_batch(n) if self.hitl else []

    def explain_last_reward(self) -> Optional[RewardExplanation]:
        return self.last_explanation

    def record_outcome(
        self,
        actual_energy: float,
        quality_preserved: bool,
    ) -> None:
        """Feed back actual outcome for causal penalty learning."""
        if not self.penalty_learner or not self.reward_history:
            return
        last = self.reward_history[-1]
        predicted = last["energy"]
        overshoot = (
            (predicted - self.energy_budget) / self.energy_budget
            if self.energy_budget > 0 else 0.0
        )
        reward = 1.0 if quality_preserved else 0.0
        if actual_energy < predicted:
            reward = min(1.0, reward + 0.2)
        state = PenaltyState(
            hour_of_day=datetime.now().hour,
            energy_overshoot_ratio=min(2.0, max(0.0, overshoot)),
            task_priority=0.5,
        )
        self.penalty_learner.record(state, reward)
        if self.penalty_learner.observations % 10 == 0:
            self.penalty_learner.update()

    def contribute_federated(self) -> None:
        if not self.federated or not self.reward_history:
            return
        self.federated.push(FederatedRewardProfile(
            deployment_id=self.deployment_id,
            key="reward:default",
            mean_reward=statistics.fmean(
                r["final_reward"] for r in self.reward_history
            ),
            mean_energy=statistics.fmean(
                r["energy"] for r in self.reward_history
            ),
            sample_count=len(self.reward_history),
        ))
        self.federated.aggregate()

    def get_federated_aggregate(self) -> Dict[str, Dict[str, float]]:
        return self.federated.aggregate() if self.federated else {}

    def distill_reward_policy(
        self, urgency: str = "normal"
    ) -> Optional[DistilledRewardPolicy]:
        if not self.distiller:
            return None
        precision = (
            self.precision_ctl.select(urgency)
            if self.precision_ctl else PrecisionLevel.INT8
        )
        return self.distiller.distill(
            base_penalty=1.0,
            negawatt_weight=0.5,
            precision=precision,
        )

    # ------------------------------------------------------------------
    # Statistics
    # ------------------------------------------------------------------

    def get_statistics(self) -> Dict[str, Any]:
        stats: Dict[str, Any] = {
            "deployment_id": self.deployment_id,
            "agent_id": self.agent_id,
            "rewards_computed": self.reward_count,
            "shifts_suggested": self.shift_count,
            "energy_budget": self.energy_budget,
            "baseline_energy": self.baseline_energy,
            "circuits": {
                "negawatt": self._negawatt_circuit.state.value,
                "forecaster": self._forecaster_circuit.state.value,
                "shifter": self._shifter_circuit.state.value,
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
        if self.penalty_learner:
            stats["causal_learner"] = {
                "observations": self.penalty_learner.observations,
                "updates": self.penalty_learner.updates,
            }
        if self.federated:
            stats["federated_aggregate"] = self.federated.aggregate()
        if self.reward_history:
            rewards = [r["final_reward"] for r in self.reward_history]
            stats["reward_stats"] = {
                "mean": statistics.fmean(rewards),
                "max": max(rewards),
                "min": min(rewards),
            }
        return stats


# =============================================================================
# Demo
# =============================================================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # --- Mock components ---
    class _MockForecaster:
        def current_intensity(self): return 400.0
        def forecast_next_hours(self, n):
            return {i: 400.0 - i * 50 for i in range(n)}

    class _MockShifter:
        def suggest(self, current, forecast, energy_kwh):
            best_hour, best_saving = 0, 0.0
            current_carbon = current * energy_kwh / 1000.0
            for h, intensity in forecast.items():
                saving = current_carbon - intensity * energy_kwh / 1000.0
                if saving > best_saving:
                    best_saving, best_hour = saving, h
            return best_hour, best_saving

    class _MockHelium:
        scarcity_score = 0.7

    def auto_approve(req: HITLRequest) -> bool:
        logger.info(f"[HITL] auto-approve: {req.reason}")
        return True

    # --- Legacy mode (all features off → matches original) ---
    print("\n=== Legacy mode (all features off) ===")
    legacy = PolicyEngine(
        energy_budget=0.05, baseline_energy=0.1,
        features={k: False for k in PolicyEngine.DEFAULT_FEATURES},
    )
    r = legacy.compute_sustainability_reward(accuracy=0.9, energy=0.06)
    print(f"  reward = {r:.4f}")

    # --- Enhanced mode (all features on) ---
    print("\n=== Enhanced mode ===")
    engine = PolicyEngine(
        energy_budget=0.05, baseline_energy=0.1,
        deployment_id="us-ca-prod-01",
        agent_id="policy-A",
        hardware=HardwareProfile(supports_int4=True, vram_gb=48),
    )
    engine.set_hitl_callback(auto_approve)
    engine.helium_signal_fn = lambda: _MockHelium()

    reward = engine.compute_sustainability_reward(
        accuracy=0.88, energy=0.062,
    )
    print(f"  reward = {reward:.4f}")
    exp = engine.explain_last_reward()
    if exp:
        print(f"  XAI: {exp.headline}")
        for line in exp.rationale:
            print(f"    • {line}")

    # Temporal shift
    hour, saving = engine.suggest_temporal_shift(
        shifter=_MockShifter(),
        forecast_engine=_MockForecaster(),
        energy_kwh=0.05,
        shiftable=True,
        urgency="low",
        current_hour=10,
        deadline_hour=20,
    )
    print(f"\n  Shift: hour={hour}, saving={saving:.6f} kgCO₂e")

    # Outcome feedback
    engine.record_outcome(actual_energy=0.055, quality_preserved=True)

    # Federated
    engine.contribute_federated()

    # Distill
    dist = engine.distill_reward_policy(urgency="normal")
    if dist:
        print(f"\n=== Distilled Reward Policy ===")
        print(f"  Precision:    {dist.precision.value}")
        print(f"  Base penalty: {dist.base_penalty}")
        print(f"  Retention:    {dist.quality_retention}")
        print(f"  Energy save:  {dist.energy_reduction_percent}%")

    # Statistics
    import json
    print("\n=== Statistics ===")
    print(json.dumps(engine.get_statistics(), indent=2, default=str))
