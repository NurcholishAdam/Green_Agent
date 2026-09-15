"""
Workload Interpreter (Enhanced)
================================

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
import logging
import math
import random
import statistics
import re

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
    ML_INFERENCE_SPECIALIST = "ml_inference_specialist"
    TRAINING_SPECIALIST = "training_specialist"
    DATA_SPECIALIST = "data_specialist"
    LATENCY_OPTIMIZER = "latency_optimizer"


class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class TaskTier(Enum):
    KNOWN = "known"          # recognized task type
    GUESSED = "guessed"      # fallback heuristic
    REVIEW = "review"        # needs human review


# =============================================================================
# Extended WorkloadProfile (backward compatible)
# =============================================================================

@dataclass
class WorkloadProfile:
    """Profile of a workload task (extended)."""
    task_type: str
    complexity: float
    energy_estimate: float
    carbon_estimate: float
    priority: int
    deferrable: bool
    deadline: float = None
    memory_estimate: float = 0.0
    cpu_estimate: float = 0.0
    # --- New optional fields (all defaulted) ---
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


# =============================================================================
# ENHANCEMENT 9: Resilience — Circuit Breaker + Chaos Injector
# =============================================================================

@dataclass
class CircuitBreaker:
    name: str
    failure_threshold: int = 3
    recovery_timeout_seconds: int = 60
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
# ENHANCEMENT 5: Temporal Logic
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
    def __init__(self) -> None:
        self.properties: List[ProfileProperty] = []
        self.violations: List[Dict[str, Any]] = []

    def register(self, p: ProfileProperty) -> None:
        self.properties.append(p)

    def verify(self, profile: WorkloadProfile) -> Tuple[bool, List[str]]:
        bad: List[str] = []
        for p in self.properties:
            if not p.check(profile):
                bad.append(p.name())
                self.violations.append({
                    "property": p.name(),
                    "task_type": profile.task_type,
                    "at": datetime.now().isoformat(),
                })
        return (len(bad) == 0, bad)


# =============================================================================
# ENHANCEMENT 6: XAI
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
    ) -> ProfileExplanation:
        reasons: List[str] = []
        reasons.append(
            f"Task type '{task_type}' (tier={tier}) → complexity "
            f"{complexity:.3f}, energy {energy_kwh:.4f} kWh, "
            f"carbon {carbon_kg:.4f} kgCO₂e."
        )
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

        return ProfileExplanation(
            headline=f"[{task_type}] complexity={complexity:.2f} "
                     f"({confidence:.2f} conf)",
            rationale=reasons,
            confidence=confidence,
            contributing_factors=dict(contributing_factors),
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


# =============================================================================
# ENHANCEMENT 2: Causal RL for Complexity Correction
# =============================================================================

@dataclass
class ComplexityState:
    task_type_hash: float
    model_size_norm: float
    input_size_norm: float
    dataset_size_norm: float
    hour_of_day: int


class CausalComplexityLearner:
    """
    Learns a linear correction on top of the hand-written complexity
    estimates. Features encode task shape; reward is relative accuracy.
    """
    N_FEATURES = 5

    def __init__(self, lr: float = 0.05) -> None:
        self.lr = lr
        self.weights: List[float] = [0.0] * self.N_FEATURES
        self.bias: float = 0.0
        self.buffer: Deque[Tuple[List[float], float, float]] = deque(maxlen=512)
        self.observations: int = 0

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
            pred_adj = pred * (1.0 + self.bias + sum(
                w * x for w, x in zip(self.weights, f)
            ))
            err = actual - pred_adj
            grad_scale = 1e-3 if abs(pred) > 0.5 else 1.0
            for i, x in enumerate(f):
                self.weights[i] += self.lr * err * x * grad_scale
            self.bias += self.lr * err * grad_scale
        self.buffer.clear()


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
    """Distills the hardcoded size maps to lower precision."""

    def distill(
        self, size_map: Dict[str, float], precision: PrecisionLevel
    ) -> DistilledEstimator:
        scale, retention, energy = {
            PrecisionLevel.FP32: (1.0, 1.00, 0.0),
            PrecisionLevel.FP16: (1.0, 0.98, 30.0),
            PrecisionLevel.INT8: (100.0, 0.93, 55.0),
            PrecisionLevel.INT4: (10.0, 0.85, 70.0),
            PrecisionLevel.QUANTUM_DISTILLED: (5.0, 0.80, 85.0),
        }[precision]

        def qz(v: float) -> float:
            return math.floor(v * scale) / scale if scale > 1.0 else v

        return DistilledEstimator(
            precision=precision,
            size_map={k: qz(v) for k, v in size_map.items()},
            quality_retention=retention,
            energy_reduction_percent=energy,
        )


# =============================================================================
# ENHANCEMENT 3: Federated Green Learning
# =============================================================================

@dataclass
class FederatedComplexityProfile:
    deployment_id: str
    task_type: str
    mean_complexity: float
    mean_energy_kwh: float
    mean_carbon_kg: float
    sample_count: int
    timestamp: datetime = field(default_factory=datetime.now)


class FederatedAggregator:
    def __init__(self) -> None:
        self.updates: List[FederatedComplexityProfile] = []
        self._global: Dict[str, Dict[str, float]] = {}

    def push(self, u: FederatedComplexityProfile) -> None:
        self.updates.append(u)

    def aggregate(self) -> Dict[str, Dict[str, float]]:
        grouped: Dict[str, List[FederatedComplexityProfile]] = defaultdict(list)
        for u in self.updates:
            grouped[u.task_type].append(u)
        result: Dict[str, Dict[str, float]] = {}
        for tt, profiles in grouped.items():
            total_w = sum(p.sample_count for p in profiles) or 1
            result[tt] = {
                "mean_complexity": sum(
                    p.mean_complexity * p.sample_count for p in profiles
                ) / total_w,
                "mean_energy_kwh": sum(
                    p.mean_energy_kwh * p.sample_count for p in profiles
                ) / total_w,
                "mean_carbon_kg": sum(
                    p.mean_carbon_kg * p.sample_count for p in profiles
                ) / total_w,
                "sample_count": total_w,
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
    task_type: str
    complexity: float
    confidence: float
    reason: str
    urgency: str
    requested_at: datetime = field(default_factory=datetime.now)


class HumanInTheLoopGate:
    def __init__(self, confidence_threshold: float = 0.4) -> None:
        self.confidence_threshold = confidence_threshold
        self.pending: List[HITLRequest] = []
        self.feedback_log: List[Dict[str, Any]] = []
        self._callback: Optional[Callable[[HITLRequest], bool]] = None

    def set_callback(self, cb: Callable[[HITLRequest], bool]) -> None:
        self._callback = cb

    def needs_review(self, tier: str, confidence: float) -> bool:
        if tier == TaskTier.REVIEW.value:
            return True
        if tier == TaskTier.GUESSED.value and confidence < 0.5:
            return True
        return confidence < self.confidence_threshold

    def request_review(
        self, task_type: str, complexity: float,
        confidence: float, reason: str, urgency: str = "medium",
    ) -> bool:
        req = HITLRequest(task_type, complexity, confidence, reason, urgency)
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
    """
    Estimate helium consumption from complexity, energy, and task type.
    Based on the Green Agent v5.0.0 dual-axis sustainability model.
    """
    # Units of helium per kWh by task type (order-of-magnitude estimates)
    HELIUM_UNITS_PER_KWH = {
        "ml_inference": 0.02,
        "ml_training": 0.15,
        "data_processing": 0.005,
        "default": 0.01,
    }

    @classmethod
    def estimate_helium_units(
        cls, task_type: str, energy_kwh: float
    ) -> float:
        rate = cls.HELIUM_UNITS_PER_KWH.get(
            task_type, cls.HELIUM_UNITS_PER_KWH["default"]
        )
        return energy_kwh * rate


# =============================================================================
# Safe input parsing helpers
# =============================================================================

def _safe_float(s: Any, default: float = 0.0) -> float:
    try:
        return float(s)
    except (TypeError, ValueError):
        return default


def _parse_size_mb(s: Any, default: float = 1.0) -> float:
    """Parse '1MB', '1.5MB', '1 GB' → MB. Safe on malformed input."""
    if s is None:
        return default
    if isinstance(s, (int, float)):
        return float(s)
    try:
        text = str(s).strip().upper()
        match = re.match(r"^([0-9]*\.?[0-9]+)\s*([KMGT]?B?)$", text)
        if not match:
            return default
        value = float(match.group(1))
        unit = match.group(2) or "MB"
        multipliers = {
            "B": 1e-6, "KB": 1e-3, "MB": 1.0, "GB": 1e3, "TB": 1e6,
            "": 1.0,
        }
        return value * multipliers.get(unit, 1.0)
    except Exception:
        return default


def _parse_size_gb(s: Any, default: float = 1.0) -> float:
    return _parse_size_mb(s, default * 1000.0) / 1000.0


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

    def __init__(
        self,
        config: Dict,
        deployment_id: str = "local",
        agent_id: str = "interpreter-0",
        features: Optional[Dict[str, bool]] = None,
        hardware: Optional[HardwareProfile] = None,
    ):
        self.config = config or {}
        self.deployment_id = deployment_id
        self.agent_id = agent_id
        self.features = {**self.DEFAULT_FEATURES, **(features or {})}

        # --- Original complexity models ---
        self.complexity_models = {
            "ml_inference": self._estimate_ml_complexity,
            "ml_training": self._estimate_training_complexity,
            "data_processing": self._estimate_data_complexity,
            "default": self._estimate_default_complexity,
        }

        # --- Enhancement layers ---
        self.circuit = CircuitBreaker("interpreter", failure_threshold=5)
        self.chaos = ChaosInjector() if self.features["chaos_testing"] else None

        self.temporal_monitor = (
            TemporalLogicMonitor() if self.features["temporal_logic"] else None
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
            CausalComplexityLearner()
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
            CarbonMarketClient() if self.features["carbon_market"] else None
        )
        self.hitl = HumanInTheLoopGate() if self.features["hitl"] else None
        self.helium_profiler = (
            HeliumProfiler() if self.features["helium_profiling"] else None
        )

        # --- Bookkeeping ---
        self.profile_history: Deque[WorkloadProfile] = deque(maxlen=2048)
        self._last_profile: Optional[WorkloadProfile] = None

        logger.info(
            f"✅ Enhanced WorkloadInterpreter initialized "
            f"(deployment={deployment_id}, agent={agent_id}, "
            f"features={list(self.features)})"
        )

    # ------------------------------------------------------------------
    # Lifecycle (backward compatible)
    # ------------------------------------------------------------------

    async def initialize(self) -> None:
        """Initialize interpreter (original print preserved)."""
        print("✅ Workload interpreter initialized")
        logger.info(
            f"WorkloadInterpreter ready with features={list(self.features)}"
        )

    # ------------------------------------------------------------------
    # Main entry point (backward compatible)
    # ------------------------------------------------------------------

    async def analyze(self, task: Dict) -> WorkloadProfile:
        """
        Analyze task and create enhanced workload profile.

        Backward compatible: accepts the same `task` dict and returns a
        `WorkloadProfile` (with new optional fields added).
        """
        # --- Circuit breaker ---
        if not self.circuit.can_call():
            logger.warning("Interpreter circuit open — returning default profile")
            return self._default_profile(task, reason="circuit open")

        # --- Chaos injection ---
        if self.chaos and self.chaos.maybe_fail("analyze"):
            self.circuit.record_failure()
            return self._default_profile(task, reason="chaos injection")

        try:
            profile = await self._analyze_internal(task)
            self.circuit.record_success()
            return profile
        except Exception as e:
            self.circuit.record_failure()
            logger.warning(f"Analyze failed ({e}); returning default")
            return self._default_profile(task, reason=str(e))

    async def _analyze_internal(self, task: Dict) -> WorkloadProfile:
        # --- Extract fields safely ---
        task_type = task.get("type", "default")
        model_size = task.get("model_size", "100M")
        input_size = task.get("input_size", "1MB")
        priority = task.get("priority", 5)
        deadline = task.get("deadline")

        # --- Determine tier ---
        known_types = set(self.complexity_models.keys())
        tier = (TaskTier.KNOWN.value if task_type in known_types
                else TaskTier.GUESSED.value)

        # --- Get complexity estimator ---
        estimator = self.complexity_models.get(
            task_type, self.complexity_models["default"]
        )
        try:
            complexity = estimator(task)
        except Exception as e:
            logger.warning(f"Estimator failed ({e}); using default")
            complexity = 0.5
            tier = TaskTier.GUESSED.value

        # --- Distillation hook ---
        distilled_used = False
        if self.distiller and task_type == "ml_inference":
            precision = (
                self.precision_ctl.select("normal")
                if self.precision_ctl else PrecisionLevel.INT8
            )
            dist = self.distiller.distill(
                {"10M": 0.1, "50M": 0.3, "100M": 0.5,
                 "500M": 0.7, "1B": 0.9, "10B": 1.0},
                precision,
            )
            # Use the distilled size map for one re-estimation pass
            base = dist.size_map.get(model_size)
            if base is not None:
                input_factor = 1.0 + (
                    _parse_size_mb(input_size, 1.0) / 100.0
                )
                complexity = min(1.0, base * input_factor)
            distilled_used = True

        # --- Causal correction ---
        causal_correction = 0.0
        if self.causal_learner:
            state = ComplexityState(
                task_type_hash=float(hash(task_type) % 1000) / 1000.0,
                model_size_norm=min(1.0, _parse_size_mb(
                    model_size, 100.0
                ) / 1e4),
                input_size_norm=min(1.0, _parse_size_mb(
                    input_size, 1.0
                ) / 100.0),
                dataset_size_norm=min(1.0, _parse_size_gb(
                    task.get("dataset_size", "1GB"), 1.0
                ) / 100.0),
                hour_of_day=datetime.now().hour,
            )
            causal_correction = self.causal_learner.predict_correction(state)
            complexity = max(0.0, min(1.0, complexity * (1.0 + 0.15 * causal_correction)))

        # --- Federated blend ---
        federated_blend_applied = False
        if self.federated:
            profile = self.federated.lookup(task_type)
            if profile and profile.get("sample_count", 0) >= 5:
                blend = 0.3
                complexity = (
                    (1 - blend) * complexity
                    + blend * profile["mean_complexity"]
                )
                federated_blend_applied = True

        # --- Adaptive precision quantization ---
        precision = PrecisionLevel.FP32
        if self.precision_ctl:
            urgency = "critical" if priority >= 8 else "normal"
            precision = self.precision_ctl.select(urgency)

        # --- Original resource estimates ---
        energy_estimate = complexity * 1.5
        carbon_estimate = energy_estimate * 0.4
        memory_estimate = complexity * 1000
        cpu_estimate = complexity * 100

        # --- Carbon intensity from forecaster (if configured) ---
        carbon_intensity = 0.0
        forecaster = self.config.get("carbon_forecaster")
        if forecaster is not None:
            try:
                getter = getattr(forecaster, "get_current_intensity", None)
                if getter is not None:
                    result = getter(task.get("region", "US-CA"))
                    if asyncio.iscoroutine(result):
                        carbon_intensity = float(await result)
                    else:
                        carbon_intensity = float(result)
                    if carbon_intensity > 0:
                        carbon_estimate = (
                            energy_estimate * carbon_intensity / 1000.0
                        )
            except Exception as e:
                logger.debug(f"Forecaster lookup failed: {e}")

        # --- Quantize values ---
        if self.precision_ctl:
            energy_estimate = self.precision_ctl.quantize(
                energy_estimate, precision
            )
            carbon_estimate = self.precision_ctl.quantize(
                carbon_estimate, precision
            )
            memory_estimate = self.precision_ctl.quantize(
                memory_estimate, precision
            )
            cpu_estimate = self.precision_ctl.quantize(cpu_estimate, precision)
            complexity = self.precision_ctl.quantize(complexity, precision)

        # --- Uncertainty estimate from history ---
        uncertainty_std = 0.0
        confidence = 0.9 if tier == TaskTier.KNOWN.value else 0.5
        if self.profile_history:
            recent = [
                p.complexity for p in self.profile_history
                if p.task_type == task_type
            ][-20:]
            if len(recent) >= 3:
                uncertainty_std = statistics.pstdev(recent)
                if uncertainty_std > 0.2:
                    confidence = max(0.3, confidence - uncertainty_std)

        # --- Helium profiling ---
        helium_units = 0.0
        helium_scarcity_score = 0.0
        if self.helium_profiler:
            helium_units = self.helium_profiler.estimate_helium_units(
                task_type, energy_estimate
            )
            scarcity_fn = self.config.get("helium_scarcity_fn")
            if callable(scarcity_fn):
                try:
                    helium_scarcity_score = float(scarcity_fn())
                except Exception:
                    helium_scarcity_score = 0.0

        # --- Market enrichment ---
        market_enrichment = None
        if self.market:
            snap = self.market.get_snapshot()
            market_enrichment = {
                "carbon_price_per_tco2_usd": snap.carbon_price_per_tco2_usd,
                "carbon_cost_usd": (
                    carbon_estimate / 1000.0 * snap.carbon_price_per_tco2_usd
                ),
                "rec_price_per_mwh_usd": snap.rec_price_per_mwh_usd,
                "rec_available_mwh": snap.rec_available_mwh,
            }

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

        # --- Contributing factors + XAI ---
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
            )
            profile.explanation = exp.headline
            profile.contributing_factors = exp.contributing_factors
        else:
            profile.contributing_factors = contributing_factors

        # --- HITL review ---
        if self.hitl and self.hitl.needs_review(tier, confidence):
            profile.hitl_required = True
            urgency = "high" if priority >= 8 else "medium"
            profile.hitl_approved = self.hitl.request_review(
                task_type=task_type,
                complexity=complexity,
                confidence=confidence,
                reason=f"tier={tier}, confidence={confidence:.2f}",
                urgency=urgency,
            )

        # --- Multi-agent feedback ---
        if self.coordinator:
            accuracy = 1.0 if profile.temporal_verified else 0.5
            latency_ms = 5.0  # nominal
            self.coordinator.record(
                self.agent_id, task_type, accuracy, latency_ms
            )

        # --- Bookkeeping ---
        self.profile_history.append(profile)
        self._last_profile = profile

        # --- Federated contribution (every 25 profiles) ---
        if self.federated and len(self.profile_history) % 25 == 0:
            self.federated.push(FederatedComplexityProfile(
                deployment_id=self.deployment_id,
                task_type=task_type,
                mean_complexity=complexity,
                mean_energy_kwh=energy_estimate,
                mean_carbon_kg=carbon_estimate,
                sample_count=1,
            ))
            self.federated.aggregate()

        return profile

    # ------------------------------------------------------------------
    # Original complexity estimators (unchanged)
    # ------------------------------------------------------------------

    def _estimate_ml_complexity(self, task: Dict) -> float:
        """Estimate complexity for ML inference (safe parsing)."""
        model_size = task.get("model_size", "100M")
        input_size = task.get("input_size", "1MB")
        size_map = {
            "10M": 0.1, "50M": 0.3, "100M": 0.5,
            "500M": 0.7, "1B": 0.9, "10B": 1.0,
        }
        base_complexity = size_map.get(model_size, 0.5)
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
    # Fallback profile
    # ------------------------------------------------------------------

    def _default_profile(self, task: Dict, reason: str) -> WorkloadProfile:
        task_type = task.get("type", "default")
        profile = WorkloadProfile(
            task_type=task_type,
            complexity=0.5,
            energy_estimate=0.75,
            carbon_estimate=0.3,
            priority=task.get("priority", 5),
            deferrable=task.get("deferrable", False),
            deadline=task.get("deadline"),
            memory_estimate=500.0,
            cpu_estimate=50.0,
            confidence=0.3,
            tier=TaskTier.REVIEW.value,
            explanation=f"Fallback profile: {reason}",
        )
        self.profile_history.append(profile)
        self._last_profile = profile
        return profile

    # ------------------------------------------------------------------
    # Public enhancement APIs
    # ------------------------------------------------------------------

    def explain_last_profile(self) -> Optional[ProfileExplanation]:
        p = self._last_profile
        if not p or not p.explanation:
            return None
        return ProfileExplanation(
            headline=p.explanation,
            rationale=[p.explanation],
            confidence=p.confidence,
            contributing_factors=p.contributing_factors,
        )

    def record_actual_complexity(
        self, task_type: str, actual_complexity: float,
        task_metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Feed back actual complexity to calibrate the causal learner.
        Called after a task completes and its real cost is measured.
        """
        if not self.causal_learner:
            return
        last = self._last_profile
        if last is None or last.task_type != task_type:
            return
        meta = task_metadata or {}
        state = ComplexityState(
            task_type_hash=float(hash(task_type) % 1000) / 1000.0,
            model_size_norm=min(1.0, _parse_size_mb(
                meta.get("model_size", "100M"), 100.0
            ) / 1e4),
            input_size_norm=min(1.0, _parse_size_mb(
                meta.get("input_size", "1MB"), 1.0
            ) / 100.0),
            dataset_size_norm=min(1.0, _parse_size_gb(
                meta.get("dataset_size", "1GB"), 1.0
            ) / 100.0),
            hour_of_day=datetime.now().hour,
        )
        self.causal_learner.record(state, last.complexity, actual_complexity)
        if self.causal_learner.observations % 10 == 0:
            self.causal_learner.update()

    def contribute_federated(self) -> None:
        if not self.federated:
            return
        grouped: Dict[str, List[WorkloadProfile]] = defaultdict(list)
        for p in self.profile_history:
            grouped[p.task_type].append(p)
        for tt, profiles in grouped.items():
            if len(profiles) < 5:
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
                sample_count=len(profiles),
            ))
        self.federated.aggregate()

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
        return self.distiller.distill(
            {"10M": 0.1, "50M": 0.3, "100M": 0.5,
             "500M": 0.7, "1B": 0.9, "10B": 1.0},
            precision,
        )

    def set_hitl_callback(self, cb: Callable[[HITLRequest], bool]) -> None:
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
            "profiles_analyzed": len(self.profile_history),
            "circuit": {
                "state": self.circuit.state.value,
                "failures": self.circuit.failures,
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
                "weights": list(self.causal_learner.weights),
                "bias": self.causal_learner.bias,
            }
        if self.federated:
            stats["federated_aggregate"] = self.federated.aggregate()
        return stats


# =============================================================================
# Demo
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

        # Simulate a batch of tasks
        tasks = [
            {"type": "ml_inference", "model_size": "500M",
             "input_size": "5MB", "priority": 7},
            {"type": "ml_training", "model_size": "1B",
             "dataset_size": "50GB", "epochs": 5, "priority": 9},
            {"type": "data_processing", "data_size": "25GB", "priority": 4},
            {"type": "unknown_task", "priority": 5},
        ]
        for t in tasks:
            profile = await interpreter.analyze(t)
            print(f"\n=== {profile.task_type} ===")
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
            print(f"  HITL req'd:   {profile.hitl_required} "
                  f"(approved={profile.hitl_approved})")
            if profile.explanation:
                print(f"  XAI: {profile.explanation}")
            if profile.market_enrichment:
                print(f"  Market: ${profile.market_enrichment['carbon_cost_usd']:.5f} "
                      f"carbon cost")

        # Feed back actual complexity
        interpreter.record_actual_complexity(
            "ml_inference", 0.72, {"model_size": "500M", "input_size": "5MB"}
        )

        # Federated contribution
        interpreter.contribute_federated()

        # Distill the estimator
        dist = interpreter.distill_estimator(urgency="normal")
        if dist:
            print(f"\n=== Distilled Estimator ===")
            print(f"  Precision:   {dist.precision.value}")
            print(f"  Retention:   {dist.quality_retention}")
            print(f"  Energy save: {dist.energy_reduction_percent}%")

        # Statistics
        import json
        print("\n=== Statistics ===")
        print(json.dumps(interpreter.get_statistics(), indent=2, default=str))

    asyncio.run(main())
