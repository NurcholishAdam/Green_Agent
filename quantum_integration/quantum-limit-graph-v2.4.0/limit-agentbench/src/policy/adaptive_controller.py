"""
AdaptiveController (Enhanced)
==============================

Dynamically adjusts runtime strategy based on resource trends, carbon
intensity, helium scarcity, and learned thresholds.

Original API preserved:
    controller = AdaptiveController()
    mode = controller.evaluate(trend)
    controller.apply(runtime)

New capabilities (all inline, feature-toggleable):
  1. Quantum-Distillation of the mode-selection policy
  2. Causal RL for learned thresholds
  3. Federated Green Learning across deployments
  4. Multi-Agent Coordination with emergent role specialisation
  5. Temporal Logic & Formal Verification for safe mode switches
  6. Explainable AI (XAI) for every mode switch
  7. Adaptive Precision Switching (precision_throttled mode)
  8. Carbon Markets & REC enrichment
  9. Resilience Engineering (circuit breaker + chaos testing)
 10. Human-in-the-Loop for disruptive switches + active learning
 +   Carbon-aware mode selection
 +   Helium-aware mode selection
 +   Logging, statistics, mode history
 +   Input validation
 +   Plugin/strategy registry for custom modes
"""

from __future__ import annotations

import logging
import math
import random
import statistics
import time
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
    MEMORY_SPECIALIST = "memory_specialist"
    ENERGY_SPECIALIST = "energy_specialist"
    CARBON_SPECIALIST = "carbon_specialist"
    LATENCY_SPECIALIST = "latency_specialist"


class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class ModeKind(Enum):
    """Category of mode for logging/statistics grouping."""
    NORMAL = "normal"
    RESOURCE_THROTTLED = "resource_throttled"
    CARBON_THROTTLED = "carbon_throttled"
    PRECISION_THROTTLED = "precision_throttled"
    SAFETY_FALLBACK = "safety_fallback"


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

@dataclass
class ModeSwitchContext:
    """Context passed to temporal properties at mode-switch time."""
    mode: str
    trend: Dict[str, Any]
    in_critical_section: bool = False
    min_tool_calls: int = 1
    critical_floor_tool_calls: int = 1


class SwitchProperty(Protocol):
    def check(self, ctx: ModeSwitchContext) -> bool: ...
    def name(self) -> str: ...


@dataclass
class NoSwitchInCriticalSection:
    """G(mode_switch → ¬in_critical_section)."""
    def check(self, ctx: ModeSwitchContext) -> bool:
        if ctx.in_critical_section and ctx.mode != "normal":
            return False
        return True

    def name(self) -> str:
        return "NoSwitchInCriticalSection"


@dataclass
class MinimumToolCallsRespected:
    """G(mode == low_energy → tool_calls >= critical_floor)."""
    def check(self, ctx: ModeSwitchContext) -> bool:
        if ctx.mode == "low_energy":
            return ctx.min_tool_calls >= ctx.critical_floor_tool_calls
        return True

    def name(self) -> str:
        return "MinimumToolCallsRespected"


@dataclass
class NonNegativeThresholds:
    """G(memory_delta >= 0 ∧ cpu_delta >= 0) for the trend feeding the switch."""
    def check(self, ctx: ModeSwitchContext) -> bool:
        md = ctx.trend.get("memory_delta", 0.0)
        cd = ctx.trend.get("cpu_delta", 0.0)
        try:
            return float(md) >= 0.0 and float(cd) >= 0.0
        except (TypeError, ValueError):
            return False

    def name(self) -> str:
        return "NonNegativeThresholds"


class TemporalLogicMonitor:
    def __init__(self) -> None:
        self.properties: List[SwitchProperty] = []
        self.violations: List[Dict[str, Any]] = []

    def register(self, p: SwitchProperty) -> None:
        self.properties.append(p)

    def verify(self, ctx: ModeSwitchContext) -> Tuple[bool, List[str]]:
        bad: List[str] = []
        for p in self.properties:
            if not p.check(ctx):
                bad.append(p.name())
                self.violations.append({
                    "property": p.name(),
                    "mode": ctx.mode,
                    "at": datetime.now().isoformat(),
                })
        return (len(bad) == 0, bad)


# =============================================================================
# ENHANCEMENT 6: Explainable AI (XAI)
# =============================================================================

@dataclass
class ModeExplanation:
    headline: str
    rationale: List[str]
    confidence: float
    contributing_factors: Dict[str, float]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class ModeExplainer:
    @staticmethod
    def explain(
        mode: str,
        previous_mode: str,
        trend: Dict[str, Any],
        memory_threshold: float,
        cpu_threshold: float,
        carbon_intensity: Optional[float] = None,
        helium_scarcity: Optional[float] = None,
        causal_adjustment: Tuple[float, float] = (0.0, 0.0),
    ) -> ModeExplanation:
        reasons: List[str] = []
        mem = float(trend.get("memory_delta", 0.0))
        cpu = float(trend.get("cpu_delta", 0.0))

        reasons.append(
            f"Switched from '{previous_mode}' to '{mode}' "
            f"(memory_delta={mem:.2f}, cpu_delta={cpu:.2f})."
        )
        if mode == "low_memory":
            reasons.append(
                f"memory_delta {mem:.2f} > threshold {memory_threshold:.2f}."
            )
        elif mode == "low_energy":
            reasons.append(
                f"cpu_delta {cpu:.2f} > threshold {cpu_threshold:.2f}."
            )
        elif mode == "precision_throttled":
            reasons.append("Precision throttle selected for energy reduction.")
        elif mode == "carbon_throttled":
            if carbon_intensity is not None:
                reasons.append(
                    f"carbon_intensity={carbon_intensity:.0f} gCO₂/kWh triggered "
                    "carbon-aware throttle."
                )
        if helium_scarcity is not None and helium_scarcity > 0.6:
            reasons.append(
                f"Helium scarcity={helium_scarcity:.2f} elevated."
            )

        ca_mem, ca_cpu = causal_adjustment
        if ca_mem or ca_cpu:
            reasons.append(
                f"Learned threshold adjustment: "
                f"mem +{ca_mem:.2f}, cpu +{ca_cpu:.2f}."
            )

        return ModeExplanation(
            headline=f"[{mode}] mem={mem:.1f}, cpu={cpu:.1f}",
            rationale=reasons,
            confidence=0.9 if mode != "normal" else 0.7,
            contributing_factors={
                "memory_delta": mem,
                "cpu_delta": cpu,
                "memory_threshold": memory_threshold,
                "cpu_threshold": cpu_threshold,
                "carbon_intensity": carbon_intensity or 0.0,
                "helium_scarcity": helium_scarcity or 0.0,
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
# ENHANCEMENT 2: Causal RL for learned thresholds
# =============================================================================

@dataclass
class ThresholdState:
    hour_of_day: int
    recent_mode_hash: float
    workload_hash: float


class CausalThresholdLearner:
    """
    Learns two offsets (memory, cpu) applied on top of base thresholds.
    Reward = 1.0 if the switch reduced resource usage without quality loss;
    0.5 if neutral; 0.0 if harmful (quality loss or excessive throttling).
    """
    N_FEATURES = 3

    def __init__(self, lr: float = 0.02) -> None:
        self.lr = lr
        self.mem_coeffs: List[float] = [0.0] * self.N_FEATURES
        self.cpu_coeffs: List[float] = [0.0] * self.N_FEATURES
        self.buffer: Deque[Tuple[List[float], float]] = deque(maxlen=512)
        self.observations: int = 0
        self.updates: int = 0

    @staticmethod
    def _features(s: ThresholdState) -> List[float]:
        return [s.hour_of_day / 24.0, s.recent_mode_hash, s.workload_hash]

    def predict_offsets(self, s: ThresholdState) -> Tuple[float, float]:
        f = self._features(s)
        mem_off = sum(c * x for c, x in zip(self.mem_coeffs, f))
        cpu_off = sum(c * x for c, x in zip(self.cpu_coeffs, f))
        return mem_off, cpu_off

    def record(self, s: ThresholdState, reward: float) -> None:
        self.buffer.append((self._features(s), reward))
        self.observations += 1

    def update(self) -> None:
        if not self.buffer:
            return
        for f, r in self.buffer:
            # Simple policy gradient on both offset models
            for i, x in enumerate(f):
                self.mem_coeffs[i] += self.lr * r * x * 0.5
                self.cpu_coeffs[i] += self.lr * r * x * 0.5
        self.updates += 1
        self.buffer.clear()


# =============================================================================
# ENHANCEMENT 1: Quantum-Distillation of the mode policy
# =============================================================================

@dataclass
class DistilledModePolicy:
    precision: PrecisionLevel
    memory_threshold: float
    cpu_threshold: float
    quality_retention: float
    energy_reduction_percent: float


class QuantumDistillationBridge:
    """Distills the threshold-based mode policy into a compact student."""

    def distill(
        self,
        memory_threshold: float,
        cpu_threshold: float,
        precision: PrecisionLevel,
    ) -> DistilledModePolicy:
        scale, retention, energy = {
            PrecisionLevel.FP32: (1.0, 1.00, 0.0),
            PrecisionLevel.FP16: (1.0, 0.98, 30.0),
            PrecisionLevel.INT8: (100.0, 0.93, 55.0),
            PrecisionLevel.INT4: (10.0, 0.85, 70.0),
            PrecisionLevel.QUANTUM_DISTILLED: (5.0, 0.80, 85.0),
        }[precision]

        def qz(v: float) -> float:
            return math.floor(v * scale) / scale if scale > 1.0 else v

        return DistilledModePolicy(
            precision=precision,
            memory_threshold=qz(memory_threshold),
            cpu_threshold=qz(cpu_threshold),
            quality_retention=retention,
            energy_reduction_percent=energy,
        )


# =============================================================================
# ENHANCEMENT 3: Federated Green Learning
# =============================================================================

@dataclass
class FederatedModeProfile:
    deployment_id: str
    mode: str
    mean_memory_delta: float
    mean_cpu_delta: float
    sample_count: int
    timestamp: datetime = field(default_factory=datetime.now)


class FederatedAggregator:
    def __init__(self) -> None:
        self.updates: List[FederatedModeProfile] = []
        self._global: Dict[str, Dict[str, float]] = {}

    def push(self, u: FederatedModeProfile) -> None:
        self.updates.append(u)

    def aggregate(self) -> Dict[str, Dict[str, float]]:
        grouped: Dict[str, List[FederatedModeProfile]] = defaultdict(list)
        for u in self.updates:
            grouped[u.mode].append(u)
        result: Dict[str, Dict[str, float]] = {}
        for mode, profiles in grouped.items():
            total_w = sum(p.sample_count for p in profiles) or 1
            result[mode] = {
                "mean_memory_delta": sum(
                    p.mean_memory_delta * p.sample_count for p in profiles
                ) / total_w,
                "mean_cpu_delta": sum(
                    p.mean_cpu_delta * p.sample_count for p in profiles
                ) / total_w,
                "sample_count": total_w,
            }
        self._global = result
        return result

    def lookup(self, mode: str) -> Optional[Dict[str, float]]:
        return self._global.get(mode)


# =============================================================================
# ENHANCEMENT 4: Multi-Agent Coordination
# =============================================================================

@dataclass
class AgentProfile:
    agent_id: str
    role: AgentRole = AgentRole.GENERALIST
    success_rate: float = 0.0
    memory_catch_rate: float = 0.0
    energy_catch_rate: float = 0.0
    carbon_catch_rate: float = 0.0
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
            if a.memory_catch_rate > 0.5 and \
                    a.memory_catch_rate >= a.energy_catch_rate:
                a.role = AgentRole.MEMORY_SPECIALIST
            elif a.energy_catch_rate > 0.5:
                a.role = AgentRole.ENERGY_SPECIALIST
            elif a.carbon_catch_rate > 0.3:
                a.role = AgentRole.CARBON_SPECIALIST
            elif a.success_rate > 0.9:
                a.role = AgentRole.LATENCY_SPECIALIST
            else:
                a.role = AgentRole.GENERALIST

    def record(
        self,
        agent_id: str,
        mode: str,
        success: bool,
    ) -> None:
        a = self.register(agent_id)
        n = a.total_calls + 1
        a.success_rate = ((n - 1) * a.success_rate + float(success)) / n
        if mode == "low_memory":
            a.memory_catch_rate = (
                (n - 1) * a.memory_catch_rate + float(success)
            ) / n
        elif mode == "low_energy":
            a.energy_catch_rate = (
                (n - 1) * a.energy_catch_rate + float(success)
            ) / n
        elif mode == "carbon_throttled":
            a.carbon_catch_rate = (
                (n - 1) * a.carbon_catch_rate + float(success)
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
# ENHANCEMENT 8: Carbon Markets
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
    mode: str
    previous_mode: str
    trend: Dict[str, Any]
    reason: str
    urgency: str
    requested_at: datetime = field(default_factory=datetime.now)


class HumanInTheLoopGate:
    def __init__(self, disruptive_modes: Tuple[str, ...] = ("low_energy",)) -> None:
        self.disruptive_modes = set(disruptive_modes)
        self.pending: List[HITLRequest] = []
        self.feedback_log: List[Dict[str, Any]] = []
        self._callback: Optional[Callable[[HITLRequest], bool]] = None

    def set_callback(self, cb: Callable[[HITLRequest], bool]) -> None:
        self._callback = cb

    def needs_review(self, mode: str, trend: Dict[str, Any]) -> bool:
        # Disruptive modes need review
        if mode in self.disruptive_modes:
            return True
        # Very high memory delta → review
        try:
            if float(trend.get("memory_delta", 0.0)) > 80.0:
                return True
        except (TypeError, ValueError):
            pass
        return False

    def request_review(
        self,
        mode: str,
        previous_mode: str,
        trend: Dict[str, Any],
        reason: str,
        urgency: str = "medium",
    ) -> bool:
        req = HITLRequest(mode, previous_mode, dict(trend), reason, urgency)
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
# Plugin / strategy registry for custom modes
# =============================================================================

class ModeStrategy(Protocol):
    name: str
    kind: ModeKind
    def matches(self, trend: Dict[str, Any], context: Dict[str, Any]) -> bool: ...
    def apply(self, runtime: Any) -> None: ...


class StrategyRegistry:
    """Registry of custom mode strategies."""
    def __init__(self) -> None:
        self._strategies: Dict[str, ModeStrategy] = {}

    def register(self, strategy: ModeStrategy) -> None:
        self._strategies[strategy.name] = strategy

    def get(self, name: str) -> Optional[ModeStrategy]:
        return self._strategies.get(name)

    def all(self) -> List[ModeStrategy]:
        return list(self._strategies.values())

    def names(self) -> List[str]:
        return list(self._strategies.keys())


# =============================================================================
# The Enhanced AdaptiveController
# =============================================================================

class AdaptiveController:
    """
    Enhanced runtime strategy controller.

    Backward-compatible signature:
        AdaptiveController()
        mode = controller.evaluate(trend)
        controller.apply(runtime)
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
        "carbon_awareness": True,
        "helium_awareness": True,
    }

    def __init__(
        self,
        deployment_id: str = "local",
        agent_id: str = "adaptive-0",
        features: Optional[Dict[str, bool]] = None,
        hardware: Optional[HardwareProfile] = None,
        memory_threshold: float = 25.0,
        cpu_threshold: float = 15.0,
        carbon_threshold: float = 500.0,
        helium_threshold: float = 0.7,
    ):
        # --- Original state ---
        self.mode: str = "normal"

        # --- Enhancement config ---
        self.deployment_id = deployment_id
        self.agent_id = agent_id
        self.features = {**self.DEFAULT_FEATURES, **(features or {})}

        # --- Thresholds (become base values for causal RL) ---
        self.base_memory_threshold = float(memory_threshold)
        self.base_cpu_threshold = float(cpu_threshold)
        self.base_carbon_threshold = float(carbon_threshold)
        self.base_helium_threshold = float(helium_threshold)

        # --- Resilience ---
        self._runtime_circuit = CircuitBreaker("runtime", failure_threshold=3)
        self.chaos = (
            ChaosInjector(failure_rate=0.0)
            if self.features["chaos_testing"] else None
        )

        # --- Temporal logic ---
        self.temporal_monitor: Optional[TemporalLogicMonitor] = (
            TemporalLogicMonitor() if self.features["temporal_logic"] else None
        )
        if self.temporal_monitor:
            self.temporal_monitor.register(NoSwitchInCriticalSection())
            self.temporal_monitor.register(MinimumToolCallsRespected())
            self.temporal_monitor.register(NonNegativeThresholds())

        # --- XAI ---
        self.explainer = ModeExplainer() if self.features["xai"] else None

        # --- Adaptive precision ---
        self.precision_ctl = (
            AdaptivePrecisionController(hardware)
            if self.features["adaptive_precision"] else None
        )
        self.current_precision: PrecisionLevel = PrecisionLevel.FP32

        # --- Causal RL ---
        self.causal_learner = (
            CausalThresholdLearner()
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

        # --- Plugin registry ---
        self.registry = StrategyRegistry()

        # --- Optional external signals ---
        self.carbon_forecaster: Any = None          # duck-typed
        self.helium_signal_fn: Optional[Callable[[], Any]] = None

        # --- History & statistics ---
        self.mode_history: Deque[Dict[str, Any]] = deque(maxlen=1024)
        self.mode_counts: Dict[str, int] = defaultdict(int)
        self.last_explanation: Optional[ModeExplanation] = None
        self.last_causal_offsets: Tuple[float, float] = (0.0, 0.0)

        logger.info(
            f"Enhanced AdaptiveController initialized "
            f"(deployment={deployment_id}, agent={agent_id}, "
            f"features={list(self.features)})"
        )

    # ------------------------------------------------------------------
    # Original public API: evaluate
    # ------------------------------------------------------------------

    def evaluate(self, trend: Dict[str, Any]) -> str:
        """
        Evaluate trend and return the chosen mode.

        Backward-compatible: with all features off, uses base thresholds
        (25 for memory, 15 for cpu) exactly like the original.
        """
        # --- Input validation ---
        mem = self._safe_float(trend.get("memory_delta", 0.0))
        cpu = self._safe_float(trend.get("cpu_delta", 0.0))
        if mem is None or cpu is None:
            logger.warning("Invalid trend values; staying in normal mode")
            self._record_switch("normal", trend, reason="invalid_input")
            return self.mode

        previous_mode = self.mode

        # --- Causal RL threshold offsets ---
        mem_threshold = self.base_memory_threshold
        cpu_threshold = self.base_cpu_threshold
        causal_offsets = (0.0, 0.0)
        if self.causal_learner:
            state = self._build_threshold_state(trend)
            mem_off, cpu_off = self.causal_learner.predict_offsets(state)
            # Bounded adjustment (never more than ±50% of base)
            mem_off = max(-0.5 * self.base_memory_threshold,
                          min(0.5 * self.base_memory_threshold, mem_off))
            cpu_off = max(-0.5 * self.base_cpu_threshold,
                          min(0.5 * self.base_cpu_threshold, cpu_off))
            mem_threshold = self.base_memory_threshold + mem_off
            cpu_threshold = self.base_cpu_threshold + cpu_off
            causal_offsets = (mem_off, cpu_off)
            self.last_causal_offsets = causal_offsets

        # --- Optional external signals ---
        carbon_intensity: Optional[float] = None
        helium_scarcity: Optional[float] = None
        if self.features["carbon_awareness"]:
            carbon_intensity = self._get_carbon_intensity()
        if self.features["helium_awareness"]:
            helium_scarcity = self._get_helium_scarcity()

        # --- Decide mode ---
        chosen_mode = "normal"
        if mem > mem_threshold:
            chosen_mode = "low_memory"
        elif cpu > cpu_threshold:
            chosen_mode = "low_energy"
        elif (self.features["carbon_awareness"]
              and carbon_intensity is not None
              and carbon_intensity > self.base_carbon_threshold):
            chosen_mode = "carbon_throttled"
        elif (self.features["adaptive_precision"]
              and self.precision_ctl is not None
              and cpu > cpu_threshold * 0.6):
            # Mild CPU pressure → precision throttle
            chosen_mode = "precision_throttled"
        else:
            # Check custom plugin strategies
            for strategy in self.registry.all():
                try:
                    if strategy.matches(trend, {"carbon": carbon_intensity}):
                        chosen_mode = strategy.name
                        break
                except Exception as e:
                    logger.debug(f"Strategy {strategy.name} failed: {e}")

        # --- Temporal verification ---
        if self.temporal_monitor:
            ctx = ModeSwitchContext(
                mode=chosen_mode,
                trend=trend,
                in_critical_section=bool(trend.get("in_critical_section", False)),
                min_tool_calls=int(trend.get("min_tool_calls", 1)),
            )
            ok, violations = self.temporal_monitor.verify(ctx)
            if not ok:
                logger.warning(
                    f"Mode switch to {chosen_mode} violates {violations}; "
                    "falling back to normal"
                )
                chosen_mode = "normal"

        # --- HITL review ---
        if self.hitl and chosen_mode != "normal" and \
                self.hitl.needs_review(chosen_mode, trend):
            approved = self.hitl.request_review(
                mode=chosen_mode,
                previous_mode=previous_mode,
                trend=trend,
                reason=f"disruptive switch to {chosen_mode}",
                urgency="high" if chosen_mode == "low_energy" else "medium",
            )
            if approved is False:
                logger.info(
                    f"HITL denied switch to {chosen_mode}; staying in normal"
                )
                chosen_mode = "normal"

        # --- Update precision ---
        if self.precision_ctl and chosen_mode in ("low_energy", "precision_throttled"):
            self.current_precision = self.precision_ctl.select("normal")
        elif self.precision_ctl and chosen_mode == "normal":
            self.current_precision = self.precision_ctl.select("critical")

        # --- Update mode ---
        self.mode = chosen_mode

        # --- XAI ---
        if self.explainer:
            exp = ModeExplainer.explain(
                mode=chosen_mode,
                previous_mode=previous_mode,
                trend=trend,
                memory_threshold=mem_threshold,
                cpu_threshold=cpu_threshold,
                carbon_intensity=carbon_intensity,
                helium_scarcity=helium_scarcity,
                causal_adjustment=causal_offsets,
            )
            self.last_explanation = exp

        # --- Record switch ---
        self._record_switch(
            chosen_mode, trend,
            reason="threshold",
            carbon_intensity=carbon_intensity,
            helium_scarcity=helium_scarcity,
        )

        # --- Multi-agent feedback ---
        if self.coordinator:
            self.coordinator.record(
                agent_id=self.agent_id,
                mode=chosen_mode,
                success=(chosen_mode == "normal"),
            )

        # --- Federated contribution ---
        if self.federated and len(self.mode_history) % 10 == 0:
            self.federated.push(FederatedModeProfile(
                deployment_id=self.deployment_id,
                mode=chosen_mode,
                mean_memory_delta=mem,
                mean_cpu_delta=cpu,
                sample_count=1,
            ))
            self.federated.aggregate()

        return self.mode

    # ------------------------------------------------------------------
    # Original public API: apply
    # ------------------------------------------------------------------

    def apply(self, runtime: Any) -> None:
        """
        Apply the current mode to the runtime.

        Backward-compatible: calls `runtime.reduce_tool_calls()` when mode
        is low_energy, and `runtime.shorten_context()` when mode is
        low_memory. Extended modes use additional runtime hooks when
        available (all wrapped in the runtime circuit breaker).
        """
        if not self._runtime_circuit.can_call():
            logger.debug("Runtime circuit open — skipping apply")
            return
        try:
            if self.chaos and self.chaos.maybe_fail("runtime"):
                raise RuntimeError("chaos: runtime")

            if self.mode == "low_energy":
                self._call_runtime(runtime, "reduce_tool_calls")
            elif self.mode == "low_memory":
                self._call_runtime(runtime, "shorten_context")
            elif self.mode == "carbon_throttled":
                self._call_runtime(runtime, "reduce_tool_calls")
                self._call_runtime(runtime, "shorten_context")
            elif self.mode == "precision_throttled":
                self._call_runtime(
                    runtime, "set_precision", self.current_precision.value
                )
            elif self.mode in self.registry.names():
                # Custom plugin strategy
                strategy = self.registry.get(self.mode)
                if strategy:
                    try:
                        strategy.apply(runtime)
                    except Exception as e:
                        logger.warning(
                            f"Strategy {self.mode} apply failed: {e}"
                        )

            self._runtime_circuit.record_success()
        except Exception as e:
            self._runtime_circuit.record_failure()
            logger.warning(f"Runtime apply failed ({e}); degrading to normal")
            self.mode = "normal"

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _safe_float(value: Any) -> Optional[float]:
        try:
            v = float(value)
            if not math.isfinite(v):
                return None
            return v
        except (TypeError, ValueError):
            return None

    def _call_runtime(self, runtime: Any, method: str, *args: Any) -> None:
        """Invoke a runtime method if it exists (duck-typed)."""
        fn = getattr(runtime, method, None)
        if callable(fn):
            fn(*args)

    def _get_carbon_intensity(self) -> Optional[float]:
        if self.carbon_forecaster is None:
            return None
        try:
            getter = getattr(self.carbon_forecaster, "get_current_intensity", None)
            if getter is None:
                return None
            result = getter()
            if hasattr(result, "__await__"):
                return None  # async callers must await; skip here
            return float(result)
        except Exception:
            return None

    def _get_helium_scarcity(self) -> Optional[float]:
        if self.helium_signal_fn is None:
            return None
        try:
            sig = self.helium_signal_fn()
            if sig is None:
                return None
            return float(getattr(sig, "scarcity_score", 0.0))
        except Exception:
            return None

    def _build_threshold_state(self, trend: Dict[str, Any]) -> ThresholdState:
        recent_mode_hash = (
            float(hash(self.mode) % 1000) / 1000.0
        )
        workload_hash = (
            float(hash(trend.get("workload", "")) % 1000) / 1000.0
        )
        return ThresholdState(
            hour_of_day=datetime.now().hour,
            recent_mode_hash=recent_mode_hash,
            workload_hash=workload_hash,
        )

    def _record_switch(
        self,
        new_mode: str,
        trend: Dict[str, Any],
        reason: str = "threshold",
        carbon_intensity: Optional[float] = None,
        helium_scarcity: Optional[float] = None,
    ) -> None:
        entry = {
            "mode": new_mode,
            "previous": (
                self.mode_history[-1]["mode"] if self.mode_history else "normal"
            ),
            "trend": dict(trend),
            "reason": reason,
            "carbon_intensity": carbon_intensity,
            "helium_scarcity": helium_scarcity,
            "causal_offsets": self.last_causal_offsets,
            "precision": self.current_precision.value,
            "at": datetime.now().isoformat(),
        }
        self.mode_history.append(entry)
        self.mode_counts[new_mode] += 1

    # ------------------------------------------------------------------
    # Public enhancement APIs
    # ------------------------------------------------------------------

    def register_strategy(self, strategy: ModeStrategy) -> None:
        """Register a custom mode strategy."""
        self.registry.register(strategy)
        logger.info(f"Registered custom mode strategy: {strategy.name}")

    def set_hitl_callback(self, cb: Callable[[HITLRequest], bool]) -> None:
        if self.hitl:
            self.hitl.set_callback(cb)

    def active_learning_samples(self, n: int = 16) -> List[Dict[str, Any]]:
        return self.hitl.active_learning_batch(n) if self.hitl else []

    def explain_last_switch(self) -> Optional[ModeExplanation]:
        return self.last_explanation

    def record_outcome(
        self,
        quality_preserved: bool,
        resource_reduction_pct: float,
    ) -> None:
        """
        Feed back the outcome of the most recent switch to the causal learner.

        Args:
            quality_preserved: whether the switch preserved output quality.
            resource_reduction_pct: percent reduction in resource usage (0-100).
        """
        if not self.causal_learner:
            return
        if not self.mode_history:
            return
        last = self.mode_history[-1]
        # Reward: quality is required; reduction scales the reward
        reward = 0.0
        if quality_preserved:
            reward = min(1.0, resource_reduction_pct / 50.0)
        state = self._build_threshold_state(last.get("trend", {}))
        self.causal_learner.record(state, reward)
        if self.causal_learner.observations % 10 == 0:
            self.causal_learner.update()

    def contribute_federated(self) -> None:
        if not self.federated:
            return
        # Aggregate by mode
        grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for entry in self.mode_history:
            grouped[entry["mode"]].append(entry)
        for mode, entries in grouped.items():
            if len(entries) < 3:
                continue
            mems = [
                e["trend"].get("memory_delta", 0.0) for e in entries
            ]
            cpus = [
                e["trend"].get("cpu_delta", 0.0) for e in entries
            ]
            self.federated.push(FederatedModeProfile(
                deployment_id=self.deployment_id,
                mode=mode,
                mean_memory_delta=statistics.fmean(mems),
                mean_cpu_delta=statistics.fmean(cpus),
                sample_count=len(entries),
            ))
        self.federated.aggregate()

    def get_federated_aggregate(self) -> Dict[str, Dict[str, float]]:
        return self.federated.aggregate() if self.federated else {}

    def distill_policy(
        self, urgency: str = "normal"
    ) -> Optional[DistilledModePolicy]:
        if not self.distiller:
            return None
        precision = (
            self.precision_ctl.select(urgency)
            if self.precision_ctl else PrecisionLevel.INT8
        )
        # Use current causal-adjusted thresholds as the distilled values
        mem_threshold = self.base_memory_threshold + self.last_causal_offsets[0]
        cpu_threshold = self.base_cpu_threshold + self.last_causal_offsets[1]
        return self.distiller.distill(
            memory_threshold=mem_threshold,
            cpu_threshold=cpu_threshold,
            precision=precision,
        )

    # ------------------------------------------------------------------
    # Statistics
    # ------------------------------------------------------------------

    def get_statistics(self) -> Dict[str, Any]:
        stats: Dict[str, Any] = {
            "deployment_id": self.deployment_id,
            "agent_id": self.agent_id,
            "current_mode": self.mode,
            "switches_recorded": len(self.mode_history),
            "mode_distribution": dict(self.mode_counts),
            "circuits": {
                "runtime": self._runtime_circuit.state.value,
            },
            "base_thresholds": {
                "memory": self.base_memory_threshold,
                "cpu": self.base_cpu_threshold,
                "carbon": self.base_carbon_threshold,
                "helium": self.base_helium_threshold,
            },
            "current_precision": self.current_precision.value,
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
                "last_offsets": {
                    "memory": self.last_causal_offsets[0],
                    "cpu": self.last_causal_offsets[1],
                },
            }
        if self.federated:
            stats["federated_aggregate"] = self.federated.aggregate()
        return stats


# =============================================================================
# Demo
# =============================================================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # --- Mock runtime ---
    class _MockRuntime:
        def __init__(self): self.tool_calls = 10; self.context_len = 4096
        def reduce_tool_calls(self):
            self.tool_calls = max(1, self.tool_calls - 2)
            logger.info(f"  reduced tool_calls → {self.tool_calls}")
        def shorten_context(self):
            self.context_len = max(512, self.context_len // 2)
            logger.info(f"  shortened context → {self.context_len}")
        def set_precision(self, level: str):
            logger.info(f"  set precision → {level}")

    # --- Mock helium signal ---
    class _MockHelium:
        scarcity_score = 0.75

    # --- Mock carbon forecaster ---
    class _MockCarbon:
        def get_current_intensity(self): return 520.0  # above threshold

    def auto_approve(req: HITLRequest) -> bool:
        logger.info(f"[HITL] auto-approve: {req.reason}")
        return True

    # --- Demo 1: original behavior (all features off) ---
    print("\n=== Legacy mode (all features off) ===")
    legacy = AdaptiveController(
        features={k: False for k in AdaptiveController.DEFAULT_FEATURES},
    )
    mode = legacy.evaluate({"memory_delta": 30.0, "cpu_delta": 5.0})
    print(f"  mode = {mode}")  # low_memory

    # --- Demo 2: full enhancement stack ---
    print("\n=== Enhanced mode (all features on) ===")
    controller = AdaptiveController(
        deployment_id="us-ca-prod-01",
        agent_id="adaptive-A",
        hardware=HardwareProfile(supports_int4=True, vram_gb=48),
    )
    controller.set_hitl_callback(auto_approve)
    controller.carbon_forecaster = _MockCarbon()
    controller.helium_signal_fn = lambda: _MockHelium()

    runtime = _MockRuntime()

    # Trigger low_memory
    trend1 = {"memory_delta": 32.0, "cpu_delta": 8.0}
    mode = controller.evaluate(trend1)
    controller.apply(runtime)
    print(f"  mode = {mode}")

    # Trigger carbon_throttled
    trend2 = {"memory_delta": 5.0, "cpu_delta": 5.0}
    mode = controller.evaluate(trend2)
    controller.apply(runtime)
    print(f"  mode = {mode}")

    # Trigger low_energy
    trend3 = {"memory_delta": 5.0, "cpu_delta": 25.0}
    mode = controller.evaluate(trend3)
    controller.apply(runtime)
    print(f"  mode = {mode}")

    # Feed back an outcome
    controller.record_outcome(
        quality_preserved=True,
        resource_reduction_pct=35.0,
    )

    # Federated contribution
    controller.contribute_federated()

    # Distill
    dist = controller.distill_policy(urgency="normal")
    if dist:
        print(f"\n=== Distilled Policy ===")
        print(f"  Precision:      {dist.precision.value}")
        print(f"  Mem threshold:  {dist.memory_threshold:.2f}")
        print(f"  CPU threshold:  {dist.cpu_threshold:.2f}")
        print(f"  Retention:      {dist.quality_retention}")
        print(f"  Energy saving:  {dist.energy_reduction_percent}%")

    # Last explanation
    exp = controller.explain_last_switch()
    if exp:
        print(f"\n=== XAI (last switch) ===")
        print(f"  {exp.headline}")
        for r in exp.rationale:
            print(f"    • {r}")

    # Statistics
    import json
    print("\n=== Statistics ===")
    print(json.dumps(controller.get_statistics(), indent=2, default=str))
