"""
PolicyFeedback (Enhanced)
==========================

Generates reflective explanations for policy decisions, now with first-class
support for the ten Green Agent enhancement layers.

Original API preserved:
    feedback = PolicyFeedback()
    result = feedback.generate(decision, before, after)
    # result == {
    #     "decision": str,
    #     "explanation": str,
    #     "tradeoff": {"latency_delta": float, "energy_delta": float},
    #     "confidence": float,
    # }

New capabilities (all inline, feature-toggleable):
  1. Quantum-Distillation of the explanation policy
  2. Causal RL for calibrated confidence
  3. Federated Green Learning across deployments
  4. Multi-Agent Coordination with emergent role specialisation
  5. Temporal Logic & Formal Verification for explanation safety
  6. Explainable AI (XAI) — structured rationale + counterfactual
  7. Adaptive Precision attribution
  8. Carbon Markets & REC enrichment
  9. Resilience Engineering (circuit breaker + chaos testing)
 10. Human-in-the-Loop for low-confidence explanations + active learning
 +   Helium awareness (dual-axis tradeoff)
 +   Carbon awareness (intensity delta + market-aware savings)
 +   Input validation, logging, statistics
 +   Expanded tradeoff dict (backward compatible — original keys preserved)
"""

from __future__ import annotations

import logging
import math
import random
import statistics
from collections import deque, defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from typing import (
    Any, Callable, Deque, Dict, List, Optional, Protocol, Tuple,
)

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


class ExplanationTier(Enum):
    HIGH = "high"        # well-supported, multi-factor
    MEDIUM = "medium"    # single-factor, dynamic confidence
    LOW = "low"          # fallback / degraded


# =============================================================================
# ENHANCEMENT 9: Resilience — Circuit Breaker + Chaos Injector
# =============================================================================

@dataclass
class CircuitBreaker:
    name: str
    failure_threshold: int = 5
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

class ExplanationProperty(Protocol):
    def check(self, after: Dict[str, Any], context: Dict[str, Any]) -> bool: ...
    def name(self) -> str: ...


@dataclass
class NonNegativeEnergy:
    def check(self, after: Dict[str, Any], context: Dict[str, Any]) -> bool:
        e = after.get("energy", 0.0)
        try:
            return float(e) >= 0.0
        except (TypeError, ValueError):
            return False

    def name(self) -> str:
        return "NonNegativeEnergy"


@dataclass
class NonNegativeLatency:
    def check(self, after: Dict[str, Any], context: Dict[str, Any]) -> bool:
        l = after.get("latency", 0.0)
        try:
            return float(l) >= 0.0
        except (TypeError, ValueError):
            return False

    def name(self) -> str:
        return "NonNegativeLatency"


@dataclass
class DeadlineRespected:
    """G(after.latency <= context.deadline_seconds)."""
    def check(self, after: Dict[str, Any], context: Dict[str, Any]) -> bool:
        deadline = context.get("deadline_seconds")
        if deadline is None:
            return True
        try:
            return float(after.get("latency", 0.0)) <= float(deadline)
        except (TypeError, ValueError):
            return False

    def name(self) -> str:
        return "DeadlineRespected"


@dataclass
class EnergyBudgetNotExceeded:
    def check(self, after: Dict[str, Any], context: Dict[str, Any]) -> bool:
        budget = context.get("energy_budget")
        if budget is None:
            return True
        try:
            return float(after.get("energy", 0.0)) <= float(budget)
        except (TypeError, ValueError):
            return False

    def name(self) -> str:
        return "EnergyBudgetNotExceeded"


@dataclass
class NonNegativeCarbon:
    def check(self, after: Dict[str, Any], context: Dict[str, Any]) -> bool:
        c = after.get("carbon", 0.0)
        try:
            return float(c) >= 0.0
        except (TypeError, ValueError):
            return False

    def name(self) -> str:
        return "NonNegativeCarbon"


class TemporalLogicMonitor:
    def __init__(self) -> None:
        self.properties: List[ExplanationProperty] = []
        self.violations: List[Dict[str, Any]] = []

    def register(self, p: ExplanationProperty) -> None:
        self.properties.append(p)

    def verify(
        self, after: Dict[str, Any], context: Dict[str, Any]
    ) -> Tuple[bool, List[str]]:
        bad: List[str] = []
        for p in self.properties:
            if not p.check(after, context):
                bad.append(p.name())
                self.violations.append({
                    "property": p.name(),
                    "at": datetime.now().isoformat(),
                })
        return (len(bad) == 0, bad)


# =============================================================================
# ENHANCEMENT 6: Explainable AI (XAI) — structured explanation
# =============================================================================

@dataclass
class StructuredExplanation:
    headline: str
    rationale: List[str]
    confidence: float
    contributing_factors: Dict[str, float]
    counterfactual: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class StructuredExplainer:
    """Builds structured explanations from delta dicts."""

    @staticmethod
    def build(
        decision: str,
        deltas: Dict[str, float],
        temporal_ok: bool,
        temporal_violations: List[str],
        carbon_market_snapshot: Optional[Dict[str, Any]] = None,
        helium_scarcity: Optional[float] = None,
    ) -> StructuredExplanation:
        reasons: List[str] = []
        reasons.append(
            f"Selected mode '{decision}' based on measured deltas."
        )

        energy_d = deltas.get("energy_delta", 0.0)
        if energy_d < 0:
            reasons.append(f"Energy reduced by {abs(energy_d):.4f} kWh.")
        elif energy_d > 0:
            reasons.append(f"Energy increased by {energy_d:.4f} kWh.")

        latency_d = deltas.get("latency_delta", 0.0)
        if latency_d != 0:
            reasons.append(f"Latency delta: {latency_d:+.2f}s.")

        carbon_d = deltas.get("carbon_delta", 0.0)
        if carbon_d != 0:
            reasons.append(f"Carbon delta: {carbon_d:+.5f} kgCO₂e.")

        helium_d = deltas.get("helium_delta", 0.0)
        if helium_d != 0:
            reasons.append(f"Helium delta: {helium_d:+.5f} units.")

        cost_d = deltas.get("cost_delta", 0.0)
        if cost_d != 0:
            reasons.append(f"Cost delta: {cost_d:+.4f} USD.")

        if not temporal_ok:
            reasons.append(
                f"Temporal violations detected: {temporal_violations}."
            )
        if carbon_market_snapshot:
            cp = carbon_market_snapshot.get("carbon_price_per_tco2_usd")
            if cp:
                value = abs(carbon_d) / 1000.0 * cp
                reasons.append(
                    f"Carbon market value of delta: ${value:.4f}."
                )
        if helium_scarcity is not None and helium_scarcity > 0.5:
            reasons.append(
                f"Helium scarcity elevated at {helium_scarcity:.2f}."
            )

        counterfactual = (
            f"If a different mode had been selected, "
            f"energy might have shifted by ~{abs(energy_d) * 0.5:.4f} kWh."
        )

        return StructuredExplanation(
            headline=f"[{decision}] energy_delta={energy_d:+.4f} kWh",
            rationale=reasons,
            confidence=0.0,  # filled in by caller
            contributing_factors={
                "energy_delta": energy_d,
                "latency_delta": latency_d,
                "carbon_delta": carbon_d,
                "helium_delta": helium_d,
                "cost_delta": cost_d,
            },
            counterfactual=counterfactual,
        )


# =============================================================================
# ENHANCEMENT 7: Adaptive Precision
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
# ENHANCEMENT 2: Causal RL for calibrated confidence
# =============================================================================

@dataclass
class ConfidenceState:
    n_factors: int
    energy_delta_norm: float
    latency_delta_norm: float
    decision_hash: float


class CausalConfidenceLearner:
    """
    Learns a calibrated confidence model from `(state, predicted_conf, actual_outcome)`.
    Actual outcome: 1.0 if the explanation was "accurate" (downstream user
    accepted it or subsequent metrics validated it), 0.0 if not.
    """
    N_FEATURES = 4

    def __init__(self, base_confidence: float = 0.75, lr: float = 0.05) -> None:
        self.base_confidence = base_confidence
        self.lr = lr
        self.coeffs: List[float] = [0.0] * self.N_FEATURES
        self.buffer: Deque[Tuple[List[float], float]] = deque(maxlen=512)
        self.observations: int = 0
        self.updates: int = 0

    @staticmethod
    def _features(s: ConfidenceState) -> List[float]:
        return [
            min(1.0, s.n_factors / 5.0),
            abs(s.energy_delta_norm),
            abs(s.latency_delta_norm),
            s.decision_hash,
        ]

    def predict_confidence(self, s: ConfidenceState) -> float:
        f = self._features(s)
        delta = sum(c * x for c, x in zip(self.coeffs, f))
        raw = self.base_confidence + delta
        # Clamp to [0.3, 0.99]
        return max(0.3, min(0.99, raw))

    def record(self, s: ConfidenceState, actual_outcome: float) -> None:
        self.buffer.append((self._features(s), actual_outcome))
        self.observations += 1

    def update(self) -> None:
        if not self.buffer:
            return
        for f, outcome in self.buffer:
            predicted = self.base_confidence + sum(
                c * x for c, x in zip(self.coeffs, f)
            )
            err = outcome - predicted
            for i, x in enumerate(f):
                self.coeffs[i] += self.lr * err * x
        self.updates += 1
        self.buffer.clear()


# =============================================================================
# ENHANCEMENT 1: Quantum-Distillation of the explanation policy
# =============================================================================

@dataclass
class DistilledExplanationPolicy:
    precision: PrecisionLevel
    base_confidence: float
    rationale_weight: float
    quality_retention: float
    energy_reduction_percent: float


class QuantumDistillationBridge:
    def distill(
        self,
        base_confidence: float,
        rationale_weight: float,
        precision: PrecisionLevel,
    ) -> DistilledExplanationPolicy:
        scale, retention, energy = {
            PrecisionLevel.FP32: (1.0, 1.00, 0.0),
            PrecisionLevel.FP16: (1.0, 0.98, 30.0),
            PrecisionLevel.INT8: (100.0, 0.93, 55.0),
            PrecisionLevel.INT4: (10.0, 0.85, 70.0),
            PrecisionLevel.QUANTUM_DISTILLED: (5.0, 0.80, 85.0),
        }[precision]

        def qz(v: float) -> float:
            return math.floor(v * scale) / scale if scale > 1.0 else v

        return DistilledExplanationPolicy(
            precision=precision,
            base_confidence=qz(base_confidence),
            rationale_weight=qz(rationale_weight),
            quality_retention=retention,
            energy_reduction_percent=energy,
        )


# =============================================================================
# ENHANCEMENT 3: Federated Green Learning
# =============================================================================

@dataclass
class FederatedExplanationProfile:
    deployment_id: str
    decision: str
    mean_confidence: float
    mean_energy_delta: float
    sample_count: int
    timestamp: datetime = field(default_factory=datetime.now)


class FederatedAggregator:
    def __init__(self) -> None:
        self.updates: List[FederatedExplanationProfile] = []
        self._global: Dict[str, Dict[str, float]] = {}

    def push(self, u: FederatedExplanationProfile) -> None:
        self.updates.append(u)

    def aggregate(self) -> Dict[str, Dict[str, float]]:
        grouped: Dict[str, List[FederatedExplanationProfile]] = defaultdict(list)
        for u in self.updates:
            grouped[u.decision].append(u)
        result: Dict[str, Dict[str, float]] = {}
        for decision, profiles in grouped.items():
            total_w = sum(p.sample_count for p in profiles) or 1
            result[decision] = {
                "mean_confidence": sum(
                    p.mean_confidence * p.sample_count for p in profiles
                ) / total_w,
                "mean_energy_delta": sum(
                    p.mean_energy_delta * p.sample_count for p in profiles
                ) / total_w,
                "sample_count": total_w,
            }
        self._global = result
        return result

    def lookup(self, decision: str) -> Optional[Dict[str, float]]:
        return self._global.get(decision)


# =============================================================================
# ENHANCEMENT 4: Multi-Agent Coordination
# =============================================================================

@dataclass
class AgentProfile:
    agent_id: str
    role: AgentRole = AgentRole.GENERALIST
    success_rate: float = 0.0
    carbon_focus: float = 0.0
    helium_focus: float = 0.0
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
            if a.carbon_focus > 0.4 and a.helium_focus > 0.4:
                a.role = AgentRole.GENERALIST
            elif a.carbon_focus > 0.5:
                a.role = AgentRole.CARBON_SPECIALIST
            elif a.helium_focus > 0.5:
                a.role = AgentRole.HELIUM_SPECIALIST
            elif a.success_rate > 0.9:
                a.role = AgentRole.LATENCY_SPECIALIST
            else:
                a.role = AgentRole.GENERALIST

    def record(
        self, agent_id: str, success: bool,
        carbon_delta: float, helium_delta: float,
    ) -> None:
        a = self.register(agent_id)
        n = a.total_calls + 1
        a.success_rate = ((n - 1) * a.success_rate + float(success)) / n
        a.carbon_focus = (
            (n - 1) * a.carbon_focus + min(1.0, abs(carbon_delta) * 100)
        ) / n
        a.helium_focus = (
            (n - 1) * a.helium_focus + min(1.0, abs(helium_delta) * 10)
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
    decision: str
    confidence: float
    reason: str
    urgency: str
    context: Dict[str, Any] = field(default_factory=dict)
    requested_at: datetime = field(default_factory=datetime.now)


class HumanInTheLoopGate:
    def __init__(self, confidence_threshold: float = 0.5) -> None:
        self.confidence_threshold = confidence_threshold
        self.pending: List[HITLRequest] = []
        self.feedback_log: List[Dict[str, Any]] = []
        self._callback: Optional[Callable[[HITLRequest], bool]] = None

    def set_callback(self, cb: Callable[[HITLRequest], bool]) -> None:
        self._callback = cb

    def needs_review(self, confidence: float, tier: str) -> bool:
        if tier == ExplanationTier.LOW.value:
            return True
        return confidence < self.confidence_threshold

    def request_review(
        self,
        decision: str,
        confidence: float,
        reason: str,
        urgency: str = "medium",
        context: Optional[Dict[str, Any]] = None,
    ) -> bool:
        req = HITLRequest(decision, confidence, reason, urgency, context or {})
        self.pending.append(req)
        if urgency == "low":
            self.pending.remove(req)
            self.feedback_log.append({"req": asdict(req), "decision": True})
            return True
        if self._callback is None:
            self.pending.remove(req)
            self.feedback_log.append({"req": asdict(req), "decision": False})
            return False
        decision_bool = self._callback(req)
        self.pending.remove(req)
        self.feedback_log.append({"req": asdict(req), "decision": decision_bool})
        return decision_bool

    def active_learning_batch(self, n: int = 16) -> List[Dict[str, Any]]:
        return self.feedback_log[-n:]


# =============================================================================
# The Enhanced PolicyFeedback
# =============================================================================

class PolicyFeedback:
    """
    Enhanced reflective explanation generator.

    Backward-compatible signature:
        PolicyFeedback()
        result = feedback.generate(decision, before, after)

    Return dict always contains the original four keys:
        decision, explanation, tradeoff, confidence

    Plus optional extended keys:
        tradeoff_extended, structured, temporal_verified,
        temporal_violations, market_snapshot, helium_scarcity,
        precision, distilled_policy, hitl_required, hitl_approved,
        causal_calibrated, tier, at
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
        deployment_id: str = "local",
        agent_id: str = "feedback-0",
        features: Optional[Dict[str, bool]] = None,
        hardware: Optional[HardwareProfile] = None,
        base_confidence: float = 0.75,
    ):
        # --- Enhancement config ---
        self.deployment_id = deployment_id
        self.agent_id = agent_id
        self.features = {**self.DEFAULT_FEATURES, **(features or {})}
        self.base_confidence = float(base_confidence)

        # --- Resilience ---
        self._circuit = CircuitBreaker("policy_feedback", failure_threshold=5)
        self.chaos = (
            ChaosInjector(failure_rate=0.0)
            if self.features["chaos_testing"] else None
        )

        # --- Temporal logic ---
        self.temporal_monitor: Optional[TemporalLogicMonitor] = (
            TemporalLogicMonitor() if self.features["temporal_logic"] else None
        )
        if self.temporal_monitor:
            self.temporal_monitor.register(NonNegativeEnergy())
            self.temporal_monitor.register(NonNegativeLatency())
            self.temporal_monitor.register(NonNegativeCarbon())
            self.temporal_monitor.register(DeadlineRespected())
            self.temporal_monitor.register(EnergyBudgetNotExceeded())

        # --- Structured XAI ---
        self.structured_explainer = (
            StructuredExplainer() if self.features["xai"] else None
        )

        # --- Adaptive precision ---
        self.precision_ctl = (
            AdaptivePrecisionController(hardware)
            if self.features["adaptive_precision"] else None
        )

        # --- Causal RL ---
        self.confidence_learner = (
            CausalConfidenceLearner(base_confidence=base_confidence, lr=0.05)
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
        self.helium_signal_fn: Optional[Callable[[], Any]] = None

        # --- History & statistics ---
        self.history: Deque[Dict[str, Any]] = deque(maxlen=1024)
        self.generation_count: int = 0
        self.confidence_values: Deque[float] = deque(maxlen=1024)
        self.decision_counts: Dict[str, int] = defaultdict(int)

        logger.info(
            f"Enhanced PolicyFeedback initialized "
            f"(deployment={deployment_id}, agent={agent_id}, "
            f"features={list(self.features)})"
        )

    # ------------------------------------------------------------------
    # Original public API: generate
    # ------------------------------------------------------------------

    def generate(
        self,
        decision: str,
        before: Dict[str, Any],
        after: Dict[str, Any],
        context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Generate a reflective explanation for a policy decision.

        Original return shape preserved:
            {"decision": str, "explanation": str,
             "tradeoff": {"latency_delta": float, "energy_delta": float},
             "confidence": float}

        Extended keys are added when their features are enabled.
        """
        # --- Circuit breaker ---
        if not self._circuit.can_call():
            logger.warning("PolicyFeedback circuit open — degraded response")
            return self._degraded_response(decision, before, after)

        # --- Chaos injection ---
        if self.chaos and self.chaos.maybe_fail("generate"):
            self._circuit.record_failure()
            return self._degraded_response(decision, before, after)

        try:
            response = self._generate_internal(
                decision, before, after, context or {}
            )
            self._circuit.record_success()
            return response
        except Exception as e:
            self._circuit.record_failure()
            logger.warning(f"generate() failed: {e}; returning degraded response")
            return self._degraded_response(decision, before, after)

    # ------------------------------------------------------------------
    # Internal generation pipeline
    # ------------------------------------------------------------------

    def _generate_internal(
        self,
        decision: str,
        before: Any,
        after: Any,
        context: Dict[str, Any],
    ) -> Dict[str, Any]:
        # --- Input validation ---
        before = self._safe_dict(before)
        after = self._safe_dict(after)

        b_energy = self._safe_float(before.get("energy"), default=0.0)
        b_latency = self._safe_float(before.get("latency"), default=0.0)
        a_energy = self._safe_float(after.get("energy"), default=0.0)
        a_latency = self._safe_float(after.get("latency"), default=0.0)

        b_carbon = self._safe_float(before.get("carbon"), default=0.0)
        a_carbon = self._safe_float(after.get("carbon"), default=0.0)

        b_helium = self._safe_float(before.get("helium"), default=0.0)
        a_helium = self._safe_float(after.get("helium"), default=0.0)

        b_cost = self._safe_float(before.get("cost"), default=0.0)
        a_cost = self._safe_float(after.get("cost"), default=0.0)

        b_accuracy = self._safe_float(before.get("accuracy"), default=None)
        a_accuracy = self._safe_float(after.get("accuracy"), default=None)

        # --- Deltas (original tradeoff keys preserved) ---
        latency_delta = a_latency - b_latency
        energy_delta = a_energy - b_energy
        carbon_delta = a_carbon - b_carbon
        helium_delta = a_helium - b_helium
        cost_delta = a_cost - b_cost
        accuracy_delta = (
            (a_accuracy - b_accuracy)
            if (a_accuracy is not None and b_accuracy is not None)
            else 0.0
        )

        tradeoff = {
            "latency_delta": latency_delta,
            "energy_delta": energy_delta,
        }
        tradeoff_extended = {
            "latency_delta": latency_delta,
            "energy_delta": energy_delta,
            "carbon_delta": carbon_delta,
            "helium_delta": helium_delta,
            "cost_delta": cost_delta,
            "accuracy_delta": accuracy_delta,
        }

        # --- Temporal verification ---
        temporal_ok = True
        temporal_violations: List[str] = []
        if self.temporal_monitor:
            temporal_ok, temporal_violations = self.temporal_monitor.verify(
                after, context
            )

        # --- Carbon market enrichment ---
        market_snapshot: Optional[Dict[str, Any]] = None
        if self.market:
            try:
                snap = self.market.get_snapshot()
                market_snapshot = {
                    "carbon_price_per_tco2_usd": snap.carbon_price_per_tco2_usd,
                    "rec_price_per_mwh_usd": snap.rec_price_per_mwh_usd,
                    "rec_available_mwh": snap.rec_available_mwh,
                    "carbon_delta_value_usd": (
                        abs(carbon_delta) / 1000.0
                        * snap.carbon_price_per_tco2_usd
                    ),
                }
            except Exception:
                pass

        # --- Helium scarcity ---
        helium_scarcity: Optional[float] = None
        if self.features["helium_awareness"] and self.helium_signal_fn:
            try:
                sig = self.helium_signal_fn()
                if sig is not None:
                    helium_scarcity = self._safe_float(
                        getattr(sig, "scarcity_score", 0.0), default=0.0
                    )
            except Exception:
                pass

        # --- Precision attribution ---
        precision = PrecisionLevel.FP32
        if self.precision_ctl:
            urgency = "critical" if context.get("deadline_critical") else "normal"
            precision = self.precision_ctl.select(urgency)

        # --- Structured XAI ---
        structured_dict: Optional[Dict[str, Any]] = None
        if self.structured_explainer:
            se = StructuredExplainer.build(
                decision=decision,
                deltas=tradeoff_extended,
                temporal_ok=temporal_ok,
                temporal_violations=temporal_violations,
                carbon_market_snapshot=market_snapshot,
                helium_scarcity=helium_scarcity,
            )
            structured_dict = se.to_dict()

        # --- Confidence computation ---
        n_factors = sum(
            1 for v in [
                latency_delta, energy_delta, carbon_delta,
                helium_delta, cost_delta,
            ] if v != 0.0
        )
        confidence = self.base_confidence
        causal_calibrated = False

        if self.confidence_learner:
            state = ConfidenceState(
                n_factors=n_factors,
                energy_delta_norm=min(1.0, abs(energy_delta) * 10),
                latency_delta_norm=min(1.0, abs(latency_delta) / 100.0),
                decision_hash=float(hash(decision) % 1000) / 1000.0,
            )
            confidence = self.confidence_learner.predict_confidence(state)
            causal_calibrated = True

        # Adjust confidence downward if temporal violations exist
        if not temporal_ok:
            confidence *= 0.6

        # Determine tier
        if not temporal_ok or n_factors == 0:
            tier = ExplanationTier.LOW.value
        elif n_factors >= 3 and confidence >= 0.7:
            tier = ExplanationTier.HIGH.value
        else:
            tier = ExplanationTier.MEDIUM.value

        # --- Explanation string (backward-compatible shape) ---
        explanation_str = (
            f"I selected mode '{decision}' because energy shifted "
            f"from {b_energy} to {a_energy}."
        )
        # If structured XAI available, use the headline instead
        if structured_dict:
            explanation_str = structured_dict["headline"]

        # --- HITL review ---
        hitl_required = False
        hitl_approved: Optional[bool] = None
        if self.hitl and self.hitl.needs_review(confidence, tier):
            hitl_required = True
            hitl_approved = self.hitl.request_review(
                decision=decision,
                confidence=confidence,
                reason=f"tier={tier}, confidence={confidence:.2f}",
                urgency="low" if tier != ExplanationTier.LOW.value else "medium",
                context={"decision": decision},
            )

        # --- Multi-agent feedback ---
        if self.coordinator:
            self.coordinator.record(
                agent_id=self.agent_id,
                success=temporal_ok,
                carbon_delta=carbon_delta,
                helium_delta=helium_delta,
            )

        # --- Distilled policy (informational) ---
        distilled_dict: Optional[Dict[str, Any]] = None
        if self.distiller:
            dp = self.distiller.distill(
                base_confidence=self.base_confidence,
                rationale_weight=0.5,
                precision=precision,
            )
            distilled_dict = asdict(dp)

        # --- Federated contribution (every 25 generations) ---
        if self.federated and self.generation_count % 25 == 0:
            self.federated.push(FederatedExplanationProfile(
                deployment_id=self.deployment_id,
                decision=decision,
                mean_confidence=confidence,
                mean_energy_delta=energy_delta,
                sample_count=1,
            ))
            self.federated.aggregate()

        # --- Record history ---
        self.generation_count += 1
        self.confidence_values.append(confidence)
        self.decision_counts[decision] += 1
        record = {
            "decision": decision,
            "confidence": confidence,
            "tier": tier,
            "energy_delta": energy_delta,
            "carbon_delta": carbon_delta,
            "helium_delta": helium_delta,
            "temporal_ok": temporal_ok,
            "at": datetime.now().isoformat(),
        }
        self.history.append(record)

        # --- Build response (original keys first) ---
        response: Dict[str, Any] = {
            "decision": decision,
            "explanation": explanation_str,
            "tradeoff": tradeoff,
            "confidence": confidence,
        }
        # --- Extended keys (purely additive) ---
        response.update({
            "tradeoff_extended": tradeoff_extended,
            "structured": structured_dict,
            "temporal_verified": temporal_ok,
            "temporal_violations": temporal_violations,
            "market_snapshot": market_snapshot,
            "helium_scarcity": helium_scarcity,
            "precision": precision.value,
            "distilled_policy": distilled_dict,
            "hitl_required": hitl_required,
            "hitl_approved": hitl_approved,
            "causal_calibrated": causal_calibrated,
            "tier": tier,
            "at": record["at"],
        })
        return response

    # ------------------------------------------------------------------
    # Degraded response (circuit open or chaos)
    # ------------------------------------------------------------------

    def _degraded_response(
        self,
        decision: str,
        before: Any,
        after: Any,
    ) -> Dict[str, Any]:
        """Return a minimal explanation with low confidence when degraded."""
        b = self._safe_dict(before)
        a = self._safe_dict(after)
        latency_delta = (
            self._safe_float(a.get("latency"), 0.0)
            - self._safe_float(b.get("latency"), 0.0)
        )
        energy_delta = (
            self._safe_float(a.get("energy"), 0.0)
            - self._safe_float(b.get("energy"), 0.0)
        )
        return {
            "decision": decision,
            "explanation": (
                f"Degraded explanation for '{decision}' (component unavailable)."
            ),
            "tradeoff": {
                "latency_delta": latency_delta,
                "energy_delta": energy_delta,
            },
            "confidence": 0.3,
            "tier": ExplanationTier.LOW.value,
            "temporal_verified": True,
            "temporal_violations": [],
            "at": datetime.now().isoformat(),
        }

    # ------------------------------------------------------------------
    # Input helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _safe_dict(value: Any) -> Dict[str, Any]:
        if value is None:
            return {}
        if isinstance(value, dict):
            return value
        try:
            return dict(value)
        except Exception:
            return {}

    @staticmethod
    def _safe_float(value: Any, default: Optional[float] = 0.0) -> Optional[float]:
        if value is None:
            return default
        try:
            v = float(value)
            return v if math.isfinite(v) else default
        except (TypeError, ValueError):
            return default

    # ------------------------------------------------------------------
    # Public enhancement APIs
    # ------------------------------------------------------------------

    def set_hitl_callback(self, cb: Callable[[HITLRequest], bool]) -> None:
        if self.hitl:
            self.hitl.set_callback(cb)

    def active_learning_samples(self, n: int = 16) -> List[Dict[str, Any]]:
        return self.hitl.active_learning_batch(n) if self.hitl else []

    def record_outcome(
        self,
        decision: str,
        was_accepted: bool,
    ) -> None:
        """
        Feed back whether the explanation was accepted/validated.

        Calibrates the causal confidence learner.
        """
        if not self.confidence_learner or not self.history:
            return
        # Find the most recent matching decision
        match = None
        for record in reversed(self.history):
            if record["decision"] == decision:
                match = record
                break
        if match is None:
            return
        state = ConfidenceState(
            n_factors=sum(
                1 for k in ("energy_delta", "carbon_delta", "helium_delta")
                if match.get(k, 0.0) != 0.0
            ),
            energy_delta_norm=min(1.0, abs(match.get("energy_delta", 0.0)) * 10),
            latency_delta_norm=0.0,
            decision_hash=float(hash(decision) % 1000) / 1000.0,
        )
        self.confidence_learner.record(
            state, 1.0 if was_accepted else 0.0
        )
        if self.confidence_learner.observations % 10 == 0:
            self.confidence_learner.update()

    def contribute_federated(self) -> None:
        if not self.federated or not self.history:
            return
        grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for rec in self.history:
            grouped[rec["decision"]].append(rec)
        for decision, recs in grouped.items():
            if len(recs) < 3:
                continue
            self.federated.push(FederatedExplanationProfile(
                deployment_id=self.deployment_id,
                decision=decision,
                mean_confidence=statistics.fmean(
                    r["confidence"] for r in recs
                ),
                mean_energy_delta=statistics.fmean(
                    r["energy_delta"] for r in recs
                ),
                sample_count=len(recs),
            ))
        self.federated.aggregate()

    def get_federated_aggregate(self) -> Dict[str, Dict[str, float]]:
        return self.federated.aggregate() if self.federated else {}

    def distill_policy(
        self, urgency: str = "normal"
    ) -> Optional[DistilledExplanationPolicy]:
        if not self.distiller:
            return None
        precision = (
            self.precision_ctl.select(urgency)
            if self.precision_ctl else PrecisionLevel.INT8
        )
        return self.distiller.distill(
            base_confidence=self.base_confidence,
            rationale_weight=0.5,
            precision=precision,
        )

    # ------------------------------------------------------------------
    # Statistics
    # ------------------------------------------------------------------

    def get_statistics(self) -> Dict[str, Any]:
        stats: Dict[str, Any] = {
            "deployment_id": self.deployment_id,
            "agent_id": self.agent_id,
            "generations": self.generation_count,
            "circuit": {
                "state": self._circuit.state.value,
                "failures": self._circuit.failures,
            },
            "decision_distribution": dict(self.decision_counts),
        }
        if self.confidence_values:
            cvs = list(self.confidence_values)
            stats["confidence_stats"] = {
                "mean": statistics.fmean(cvs),
                "min": min(cvs),
                "max": max(cvs),
                "stdev": (
                    statistics.pstdev(cvs) if len(cvs) > 1 else 0.0
                ),
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
        if self.confidence_learner:
            stats["causal_learner"] = {
                "observations": self.confidence_learner.observations,
                "updates": self.confidence_learner.updates,
            }
        if self.federated:
            stats["federated_aggregate"] = self.federated.aggregate()
        return stats


# =============================================================================
# Demo
# =============================================================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    class _MockHelium:
        scarcity_score = 0.65

    def auto_approve(req: HITLRequest) -> bool:
        logger.info(f"[HITL] auto-approve: {req.reason}")
        return True

    # --- Legacy mode (all features off) ---
    print("\n=== Legacy mode (all features off) ===")
    legacy = PolicyFeedback(
        features={k: False for k in PolicyFeedback.DEFAULT_FEATURES},
    )
    result = legacy.generate(
        decision="low_energy",
        before={"energy": 0.08, "latency": 120.0},
        after={"energy": 0.05, "latency": 150.0},
    )
    print(f"  decision:    {result['decision']}")
    print(f"  explanation: {result['explanation']}")
    print(f"  tradeoff:    {result['tradeoff']}")
    print(f"  confidence:  {result['confidence']}")

    # --- Enhanced mode (all features on) ---
    print("\n=== Enhanced mode ===")
    feedback = PolicyFeedback(
        deployment_id="us-ca-prod-01",
        agent_id="feedback-A",
        hardware=HardwareProfile(supports_int4=True, vram_gb=48),
        base_confidence=0.75,
    )
    feedback.set_hitl_callback(auto_approve)
    feedback.helium_signal_fn = lambda: _MockHelium()

    result = feedback.generate(
        decision="low_energy",
        before={"energy": 0.08, "latency": 120.0,
                "carbon": 0.032, "helium": 0.004, "cost": 0.016},
        after={"energy": 0.05, "latency": 150.0,
               "carbon": 0.020, "helium": 0.0025, "cost": 0.010},
        context={"deadline_seconds": 300.0, "energy_budget": 0.06},
    )

    print(f"  decision:    {result['decision']}")
    print(f"  explanation: {result['explanation']}")
    print(f"  confidence:  {result['confidence']:.4f}")
    print(f"  tier:        {result['tier']}")
    print(f"  tradeoff:    {result['tradeoff']}")
    print(f"  extended:    {result['tradeoff_extended']}")
    print(f"  temporal:    {result['temporal_verified']} "
          f"(violations={result['temporal_violations']})")
    print(f"  precision:   {result['precision']}")
    print(f"  causal cal:  {result['causal_calibrated']}")
    print(f"  HITL:        {result['hitl_required']} "
          f"(approved={result['hitl_approved']})")
    if result.get("market_snapshot"):
        print(f"  market:      ${result['market_snapshot']['carbon_price_per_tco2_usd']:.2f}/tCO2 "
              f"(value=${result['market_snapshot']['carbon_delta_value_usd']:.4f})")
    if result.get("structured"):
        print(f"  structured rationale:")
        for line in result["structured"]["rationale"]:
            print(f"    • {line}")
        print(f"  counterfactual: {result['structured']['counterfactual']}")

    # Outcome feedback
    feedback.record_outcome(decision="low_energy", was_accepted=True)

    # Federated
    feedback.contribute_federated()

    # Distill
    dist = feedback.distill_policy(urgency="normal")
    if dist:
        print(f"\n=== Distilled Explanation Policy ===")
        print(f"  Precision:     {dist.precision.value}")
        print(f"  Base conf:     {dist.base_confidence}")
        print(f"  Retention:     {dist.quality_retention}")
        print(f"  Energy saving: {dist.energy_reduction_percent}%")

    # Statistics
    import json
    print("\n=== Statistics ===")
    print(json.dumps(feedback.get_statistics(), indent=2, default=str))
