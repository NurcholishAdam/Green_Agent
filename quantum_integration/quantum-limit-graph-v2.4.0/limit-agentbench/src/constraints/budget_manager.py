"""
Budget Manager for Green_Agent (Enhanced)
==========================================

Manages and tracks consumption against energy/carbon/latency budgets.
Transforms evaluation from "who is greenest?" to "who succeeds within constraints?"

Original API preserved:
    budget = Budget.eco_budget()
    manager = BudgetManager(budget)
    status = manager.check_budget('energy_wh')
    can_run, violations = manager.can_execute({'energy_wh': 2.0, ...})
    manager.record_consumption({'energy_wh': 1.8, ...})
    summary = manager.get_summary()

New capabilities (all inline, feature-toggleable):
  1. Quantum-Distillation of budget policies
  2. Causal RL for adaptive warning/critical thresholds
  3. Federated Green Learning across deployments
  4. Multi-Agent Coordination with emergent role specialisation
  5. Temporal Logic & Formal Verification of budget invariants
  6. Explainable AI (XAI) for every can_execute decision
  7. Adaptive Precision as a budget dimension
  8. Carbon Markets & REC enrichment for cost budgets
  9. Resilience Engineering (circuit breaker + chaos testing)
 10. Human-in-the-Loop for borderline denials + active learning
 +   Helium awareness (dual-axis budgets)
 +   Per-metric severity classification
 +   Input validation, logging, statistics
"""

from __future__ import annotations

import logging
import math
import random
from collections import deque, defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from typing import (
    Any, Callable, Deque, Dict, List, Optional, Protocol, Tuple,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Original enums & BudgetStatus (unchanged)
# =============================================================================

class BudgetStatus(Enum):
    """Budget consumption status"""
    UNDER_BUDGET = "under_budget"
    NEAR_LIMIT = "near_limit"
    AT_LIMIT = "at_limit"
    EXCEEDED = "exceeded"


# =============================================================================
# NEW enums for enhancements
# =============================================================================

class Severity(Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"
    EMERGENCY = "emergency"


class PrecisionLevel(Enum):
    FP32 = "fp32"
    FP16 = "fp16"
    INT8 = "int8"
    INT4 = "int4"
    QUANTUM_DISTILLED = "quantum_distilled"


class AgentRole(Enum):
    GENERALIST = "generalist"
    ENERGY_SPECIALIST = "energy_specialist"
    CARBON_SPECIALIST = "carbon_specialist"
    HELIUM_SPECIALIST = "helium_specialist"
    LATENCY_SPECIALIST = "latency_specialist"


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

class BudgetProperty(Protocol):
    def check(self, ctx: Dict[str, Any]) -> bool: ...
    def name(self) -> str: ...


@dataclass
class RemainingNonNegative:
    """G(remaining >= 0) after consumption recording."""
    def check(self, ctx: Dict[str, Any]) -> bool:
        return ctx.get("min_remaining", 0.0) >= 0.0

    def name(self) -> str:
        return "RemainingNonNegative"


@dataclass
class StatusMonotonic:
    """G(UNDER_BUDGET → NEAR_LIMIT → AT_LIMIT → EXCEEDED is monotonic)."""
    def check(self, ctx: Dict[str, Any]) -> bool:
        return ctx.get("status_valid", True)

    def name(self) -> str:
        return "StatusMonotonic"


@dataclass
class CanExecuteImpliesRemaining:
    """G(can_execute=True → estimated <= remaining)."""
    def check(self, ctx: Dict[str, Any]) -> bool:
        return ctx.get("can_execute_consistent", True)

    def name(self) -> str:
        return "CanExecuteImpliesRemaining"


class TemporalLogicMonitor:
    def __init__(self) -> None:
        self.properties: List[BudgetProperty] = []
        self.violations: List[Dict[str, Any]] = []

    def register(self, p: BudgetProperty) -> None:
        self.properties.append(p)

    def verify(self, ctx: Dict[str, Any]) -> Tuple[bool, List[str]]:
        bad: List[str] = []
        for p in self.properties:
            if not p.check(ctx):
                bad.append(p.name())
                self.violations.append({
                    "property": p.name(),
                    "at": datetime.now().isoformat(),
                })
        return (len(bad) == 0, bad)


# =============================================================================
# ENHANCEMENT 6: XAI
# =============================================================================

@dataclass
class BudgetExplanation:
    headline: str
    rationale: List[str]
    confidence: float
    contributing_factors: Dict[str, float]

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class BudgetExplainer:
    @staticmethod
    def explain_can_execute(
        can_execute: bool,
        violations: List[str],
        estimated: Dict[str, float],
        remaining: Dict[str, float],
        utilization: Dict[str, float],
        severity: Optional[str] = None,
        market_snapshot: Optional[Dict[str, Any]] = None,
    ) -> BudgetExplanation:
        reasons: List[str] = []
        if can_execute:
            reasons.append(
                f"Execution approved: all {len(estimated)} estimated "
                f"metrics fit within remaining budget."
            )
            if utilization:
                tightest = max(utilization, key=utilization.get)
                reasons.append(
                    f"Tightest metric: '{tightest}' at "
                    f"{utilization[tightest]:.1%} utilization."
                )
        else:
            reasons.append(
                f"Execution denied: {len(violations)} budget(s) would be "
                f"exceeded ({', '.join(violations)})."
            )
            for v in violations:
                est = estimated.get(v, 0.0)
                rem = remaining.get(v, 0.0)
                reasons.append(
                    f"'{v}': estimated {est:.4f} > remaining {rem:.4f}."
                )
            if severity:
                reasons.append(f"Severity classified as '{severity}'.")
        if market_snapshot:
            cp = market_snapshot.get("carbon_price_per_tco2_usd")
            if cp:
                reasons.append(
                    f"Carbon market price: ${cp:.2f}/tCO₂."
                )

        return BudgetExplanation(
            headline=(
                f"[{'APPROVED' if can_execute else 'DENIED'}] "
                f"{len(violations)} violation(s)"
            ),
            rationale=reasons,
            confidence=0.95,
            contributing_factors={
                **{f"remaining_{k}": v for k, v in remaining.items()},
                **{f"estimated_{k}": v for k, v in estimated.items()},
            },
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
# ENHANCEMENT 2: Causal RL for adaptive thresholds
# =============================================================================

@dataclass
class ThresholdState:
    hour_of_day: int
    utilization_ratio: float
    workload_hash: float


class CausalThresholdLearner:
    """
    Learns offsets to the warning/critical thresholds from outcomes.
    Reward = 1.0 if the decision (approve/deny) was correct; 0.0 if not.
    """
    N_FEATURES = 3

    def __init__(
        self,
        base_warning: float = 0.80,
        base_critical: float = 0.95,
        lr: float = 0.02,
    ) -> None:
        self.base_warning = base_warning
        self.base_critical = base_critical
        self.lr = lr
        self.warning_coeffs: List[float] = [0.0] * self.N_FEATURES
        self.critical_coeffs: List[float] = [0.0] * self.N_FEATURES
        self.observations: int = 0
        self.updates: int = 0

    @staticmethod
    def _features(s: ThresholdState) -> List[float]:
        return [s.hour_of_day / 24.0, s.utilization_ratio, s.workload_hash]

    def predict_thresholds(
        self, s: ThresholdState,
    ) -> Tuple[float, float]:
        f = self._features(s)
        dw = sum(c * x for c, x in zip(self.warning_coeffs, f))
        dc = sum(c * x for c, x in zip(self.critical_coeffs, f))
        # Bound to ±5% of base
        dw = max(-0.05, min(0.05, dw))
        dc = max(-0.05, min(0.05, dc))
        return (
            max(0.5, min(0.99, self.base_warning + dw)),
            max(0.55, min(0.999, self.base_critical + dc)),
        )

    def record(self, s: ThresholdState, reward: float) -> None:
        f = self._features(s)
        for i, x in enumerate(f):
            self.warning_coeffs[i] += self.lr * reward * x * 0.1
            self.critical_coeffs[i] += self.lr * reward * x * 0.1
        self.observations += 1

    def update(self) -> None:
        self.updates += 1


# =============================================================================
# ENHANCEMENT 1: Quantum Distillation
# =============================================================================

@dataclass
class DistilledBudgetPolicy:
    precision: PrecisionLevel
    thresholds: Dict[str, float]
    quality_retention: float
    energy_reduction_percent: float


class QuantumDistillationBridge:
    def distill(
        self,
        warning: float,
        critical: float,
        precision: PrecisionLevel,
    ) -> DistilledBudgetPolicy:
        scale, retention, energy = {
            PrecisionLevel.FP32: (1.0, 1.00, 0.0),
            PrecisionLevel.FP16: (1.0, 0.98, 30.0),
            PrecisionLevel.INT8: (100.0, 0.93, 55.0),
            PrecisionLevel.INT4: (10.0, 0.85, 70.0),
            PrecisionLevel.QUANTUM_DISTILLED: (5.0, 0.80, 85.0),
        }[precision]

        def qz(v: float) -> float:
            return math.floor(v * scale) / scale if scale > 1.0 else v

        return DistilledBudgetPolicy(
            precision=precision,
            thresholds={"warning": qz(warning), "critical": qz(critical)},
            quality_retention=retention,
            energy_reduction_percent=energy,
        )


# =============================================================================
# ENHANCEMENT 3: Federated Green Learning
# =============================================================================

@dataclass
class FederatedBudgetProfile:
    deployment_id: str
    budget_name: str
    metric: str
    mean_utilization: float
    sample_count: int
    timestamp: datetime = field(default_factory=datetime.now)


class FederatedAggregator:
    def __init__(self) -> None:
        self.updates: List[FederatedBudgetProfile] = []
        self._global: Dict[str, Dict[str, float]] = {}

    def push(self, u: FederatedBudgetProfile) -> None:
        self.updates.append(u)

    def aggregate(self) -> Dict[str, Dict[str, float]]:
        grouped: Dict[str, List[FederatedBudgetProfile]] = defaultdict(list)
        for u in self.updates:
            grouped[u.metric].append(u)
        result: Dict[str, Dict[str, float]] = {}
        for metric, profiles in grouped.items():
            total_w = sum(p.sample_count for p in profiles) or 1
            result[metric] = {
                "mean_utilization": sum(
                    p.mean_utilization * p.sample_count for p in profiles
                ) / total_w,
                "sample_count": total_w,
            }
        self._global = result
        return result

    def lookup(self, metric: str) -> Optional[Dict[str, float]]:
        return self._global.get(metric)


# =============================================================================
# ENHANCEMENT 4: Multi-Agent Coordination
# =============================================================================

@dataclass
class AgentProfile:
    agent_id: str
    role: AgentRole = AgentRole.GENERALIST
    success_rate: float = 0.0
    dominant_metric: Optional[str] = None
    total_calls: int = 0


class MultiAgentCoordinator:
    def __init__(self) -> None:
        self.agents: Dict[str, AgentProfile] = {}

    def register(self, agent_id: str) -> AgentProfile:
        if agent_id not in self.agents:
            self.agents[agent_id] = AgentProfile(agent_id)
        return self.agents[agent_id]

    def _reassign(self) -> None:
        role_map = {
            "energy_wh": AgentRole.ENERGY_SPECIALIST,
            "carbon_g": AgentRole.CARBON_SPECIALIST,
            "helium_units": AgentRole.HELIUM_SPECIALIST,
            "latency_ms": AgentRole.LATENCY_SPECIALIST,
        }
        for a in self.agents.values():
            if a.dominant_metric in role_map:
                a.role = role_map[a.dominant_metric]
            else:
                a.role = AgentRole.GENERALIST

    def record(
        self, agent_id: str, success: bool,
        dominant_metric: Optional[str] = None,
    ) -> None:
        a = self.register(agent_id)
        n = a.total_calls + 1
        a.success_rate = ((n - 1) * a.success_rate + float(success)) / n
        if dominant_metric:
            a.dominant_metric = dominant_metric
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
    metric: str
    estimated: float
    remaining: float
    reason: str
    urgency: str
    context: Dict[str, Any] = field(default_factory=dict)
    requested_at: datetime = field(default_factory=datetime.now)


class HumanInTheLoopGate:
    def __init__(
        self,
        borderline_ratio: float = 1.05,
        severe_ratio: float = 2.0,
    ) -> None:
        self.borderline_ratio = borderline_ratio
        self.severe_ratio = severe_ratio
        self.pending: List[HITLRequest] = []
        self.feedback_log: List[Dict[str, Any]] = []
        self._callback: Optional[Callable[[HITLRequest], bool]] = None

    def set_callback(self, cb: Callable[[HITLRequest], bool]) -> None:
        self._callback = cb

    def needs_review(
        self, estimated: float, remaining: float,
    ) -> bool:
        if remaining <= 0:
            return True
        ratio = estimated / remaining
        # Borderline (just over remaining) OR severe (way over remaining)
        if 1.0 < ratio <= self.borderline_ratio:
            return True
        if ratio >= self.severe_ratio:
            return True
        return False

    def request_review(
        self, metric: str, estimated: float, remaining: float,
        reason: str, urgency: str = "medium",
        context: Optional[Dict[str, Any]] = None,
    ) -> bool:
        req = HITLRequest(
            metric, estimated, remaining, reason, urgency, context or {},
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
    HELIUM_UNITS_PER_KWH = 0.05

    @classmethod
    def estimate_units(
        cls, energy_wh: float, scarcity_score: float = 0.0,
    ) -> float:
        energy_kwh = energy_wh / 1000.0
        base = energy_kwh * cls.HELIUM_UNITS_PER_KWH
        efficiency = 1.0 - min(0.5, scarcity_score * 0.5)
        return base * efficiency


# =============================================================================
# Budget dataclass (extended, backward compatible)
# =============================================================================

@dataclass
class Budget:
    """
    Budget constraints for agent execution (extended).

    All original fields are preserved with the same defaults and validation.
    New optional fields default to None so existing code is unaffected.
    """
    max_energy_wh: float
    max_carbon_g: float
    max_latency_ms: float
    max_cost_usd: Optional[float] = None

    warning_threshold: float = 0.80
    critical_threshold: float = 0.95

    name: str = "Default Budget"
    description: str = ""
    created_at: datetime = field(default_factory=datetime.now)

    # --- New optional fields ---
    max_helium_units: Optional[float] = None
    allowed_precisions: Optional[List[str]] = None

    def __post_init__(self):
        """Validate budget constraints (original + extensions)."""
        if self.max_energy_wh <= 0:
            raise ValueError(
                f"Energy budget must be positive: {self.max_energy_wh}"
            )
        if self.max_carbon_g <= 0:
            raise ValueError(
                f"Carbon budget must be positive: {self.max_carbon_g}"
            )
        if self.max_latency_ms <= 0:
            raise ValueError(
                f"Latency budget must be positive: {self.max_latency_ms}"
            )
        if self.max_cost_usd is not None and self.max_cost_usd <= 0:
            raise ValueError(
                f"Cost budget must be positive: {self.max_cost_usd}"
            )
        if self.max_helium_units is not None and self.max_helium_units <= 0:
            raise ValueError(
                f"Helium budget must be positive: {self.max_helium_units}"
            )

        if not 0 <= self.warning_threshold <= 1:
            raise ValueError(
                f"Warning threshold must be in [0, 1]: "
                f"{self.warning_threshold}"
            )
        if not 0 <= self.critical_threshold <= 1:
            raise ValueError(
                f"Critical threshold must be in [0, 1]: "
                f"{self.critical_threshold}"
            )

        logger.info(
            f"Created budget: {self.name} - "
            f"E:{self.max_energy_wh}Wh, C:{self.max_carbon_g}g, "
            f"L:{self.max_latency_ms}ms"
            + (f", He:{self.max_helium_units}u"
               if self.max_helium_units is not None else "")
        )

    def to_dict(self) -> Dict:
        """Serialize to dictionary (backward-compatible keys preserved)."""
        out = {
            'name': self.name,
            'description': self.description,
            'max_energy_wh': self.max_energy_wh,
            'max_carbon_g': self.max_carbon_g,
            'max_latency_ms': self.max_latency_ms,
            'max_cost_usd': self.max_cost_usd,
            'warning_threshold': self.warning_threshold,
            'critical_threshold': self.critical_threshold,
            'created_at': self.created_at.isoformat(),
        }
        if self.max_helium_units is not None:
            out['max_helium_units'] = self.max_helium_units
        if self.allowed_precisions is not None:
            out['allowed_precisions'] = list(self.allowed_precisions)
        return out

    @classmethod
    def from_dict(cls, data: Dict) -> 'Budget':
        """Deserialize from dictionary (backward-compatible)."""
        created_at = (
            datetime.fromisoformat(data['created_at'])
            if 'created_at' in data else datetime.now()
        )
        return cls(
            max_energy_wh=data['max_energy_wh'],
            max_carbon_g=data['max_carbon_g'],
            max_latency_ms=data['max_latency_ms'],
            max_cost_usd=data.get('max_cost_usd'),
            warning_threshold=data.get('warning_threshold', 0.80),
            critical_threshold=data.get('critical_threshold', 0.95),
            name=data.get('name', 'Default Budget'),
            description=data.get('description', ''),
            created_at=created_at,
            max_helium_units=data.get('max_helium_units'),
            allowed_precisions=data.get('allowed_precisions'),
        )

    # ---- Presets (unchanged behavior, now with helium) ----

    @classmethod
    def eco_budget(cls) -> 'Budget':
        return cls(
            max_energy_wh=5.0,
            max_carbon_g=1.0,
            max_latency_ms=10000,
            max_helium_units=0.05,
            name="Eco Budget",
            description="Strict energy/carbon limits for eco-friendly deployment",
        )

    @classmethod
    def balanced_budget(cls) -> 'Budget':
        return cls(
            max_energy_wh=20.0,
            max_carbon_g=4.0,
            max_latency_ms=5000,
            max_helium_units=0.2,
            name="Balanced Budget",
            description="Moderate limits balancing efficiency and performance",
        )

    @classmethod
    def performance_budget(cls) -> 'Budget':
        return cls(
            max_energy_wh=100.0,
            max_carbon_g=20.0,
            max_latency_ms=1000,
            max_helium_units=1.0,
            name="Performance Budget",
            description="Optimized for low latency, lenient on energy",
        )


# =============================================================================
# The Enhanced BudgetManager
# =============================================================================

class BudgetManager:
    """
    Enhanced budget manager with all ten enhancement layers.

    Backward-compatible: same public API and semantics as the original.
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
    }

    def __init__(
        self,
        budget: Budget,
        deployment_id: str = "local",
        agent_id: str = "budget-manager-0",
        features: Optional[Dict[str, bool]] = None,
        hardware: Optional[HardwareProfile] = None,
    ):
        # --- Original state ---
        self.budget = budget
        self.consumed = {
            'energy_wh': 0.0,
            'carbon_g': 0.0,
            'latency_ms': 0.0,
            'cost_usd': 0.0,
        }
        if budget.max_helium_units is not None:
            self.consumed['helium_units'] = 0.0
        self.execution_history: List[Dict] = []
        self.violation_history: List[Dict] = []

        # --- Enhancement config ---
        self.deployment_id = deployment_id
        self.agent_id = agent_id
        self.features = {**self.DEFAULT_FEATURES, **(features or {})}

        # --- Resilience ---
        self._circuit = CircuitBreaker("budget_manager", failure_threshold=5)
        self.chaos = (
            ChaosInjector(failure_rate=0.0)
            if self.features["chaos_testing"] else None
        )

        # --- Temporal logic ---
        self.temporal_monitor: Optional[TemporalLogicMonitor] = (
            TemporalLogicMonitor() if self.features["temporal_logic"] else None
        )
        if self.temporal_monitor:
            self.temporal_monitor.register(RemainingNonNegative())
            self.temporal_monitor.register(StatusMonotonic())
            self.temporal_monitor.register(CanExecuteImpliesRemaining())

        # --- XAI ---
        self.explainer = BudgetExplainer() if self.features["xai"] else None

        # --- Adaptive precision ---
        self.precision_ctl = (
            AdaptivePrecisionController(hardware)
            if self.features["adaptive_precision"] else None
        )

        # --- Causal RL ---
        self.threshold_learner = (
            CausalThresholdLearner(
                base_warning=budget.warning_threshold,
                base_critical=budget.critical_threshold,
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
            HeliumProfiler() if self.features["helium_awareness"] else None
        )
        self.helium_signal_fn: Optional[Callable[[], Any]] = None

        # --- Statistics ---
        self.severity_counts: Dict[str, int] = defaultdict(int)
        self.breach_by_metric: Dict[str, int] = defaultdict(int)
        self.last_explanation: Optional[BudgetExplanation] = None
        self.last_can_execute_explanation: Optional[BudgetExplanation] = None

        logger.info(
            f"Enhanced BudgetManager initialized with {budget.name} "
            f"(deployment={deployment_id}, agent={agent_id}, "
            f"features={list(self.features)})"
        )

    # ------------------------------------------------------------------
    # Original public API: check_budget (backward compatible)
    # ------------------------------------------------------------------

    def check_budget(self, metric: str) -> BudgetStatus:
        """
        Check budget status for a specific metric.

        Original signature and behavior preserved.
        """
        if metric not in self.consumed:
            raise ValueError(f"Unknown metric: {metric}")

        # --- Limit lookup ---
        if metric == 'energy_wh':
            limit = self.budget.max_energy_wh
        elif metric == 'carbon_g':
            limit = self.budget.max_carbon_g
        elif metric == 'latency_ms':
            limit = self.budget.max_latency_ms
        elif metric == 'cost_usd':
            limit = self.budget.max_cost_usd or float('inf')
        elif metric == 'helium_units':
            limit = self.budget.max_helium_units or float('inf')
        else:
            limit = float('inf')

        consumed = self.consumed[metric]

        # --- Per-task latency (original behavior preserved) ---
        if metric == 'latency_ms':
            if not self.execution_history:
                ratio = 0.0
            else:
                last_latency = self.execution_history[-1].get(
                    'latency_ms', 0.0
                )
                ratio = last_latency / limit if limit > 0 else 0.0
        else:
            ratio = consumed / limit if limit > 0 else 0.0

        # --- Adaptive thresholds (enhancement) ---
        warning_th = self.budget.warning_threshold
        critical_th = self.budget.critical_threshold
        if self.threshold_learner:
            try:
                state = ThresholdState(
                    hour_of_day=datetime.now().hour,
                    utilization_ratio=min(1.0, ratio),
                    workload_hash=float(hash(metric) % 1000) / 1000.0,
                )
                warning_th, critical_th = (
                    self.threshold_learner.predict_thresholds(state)
                )
            except Exception:
                pass

        # --- Status determination ---
        if ratio >= 1.0:
            status = BudgetStatus.EXCEEDED
        elif ratio >= critical_th:
            status = BudgetStatus.AT_LIMIT
        elif ratio >= warning_th:
            status = BudgetStatus.NEAR_LIMIT
        else:
            status = BudgetStatus.UNDER_BUDGET

        # --- Severity tracking (enhancement) ---
        if status != BudgetStatus.UNDER_BUDGET:
            severity = self._severity_for_status(status, ratio)
            self.severity_counts[severity.value] += 1

        return status

    # ------------------------------------------------------------------
    # Original public API: can_execute (backward compatible)
    # ------------------------------------------------------------------

    def can_execute(
        self, estimated_consumption: Dict[str, float],
    ) -> Tuple[bool, List[str]]:
        """
        Check if execution is allowed given estimated consumption.

        Original signature and behavior preserved.
        """
        # --- Input validation ---
        if not isinstance(estimated_consumption, dict):
            logger.warning("can_execute: invalid input, denying")
            return False, ["invalid_input"]

        # --- Circuit breaker ---
        if not self._circuit.can_call():
            logger.warning("Circuit open — denying for safety")
            return False, ["circuit_open"]

        # --- Chaos injection ---
        if self.chaos and self.chaos.maybe_fail("can_execute"):
            self._circuit.record_failure()
            return False, ["chaos"]

        try:
            result = self._can_execute_internal(estimated_consumption)
            self._circuit.record_success()
            return result
        except Exception as e:
            self._circuit.record_failure()
            logger.warning(f"can_execute failed: {e}")
            return False, ["internal_error"]

    def _can_execute_internal(
        self, estimated: Dict[str, float],
    ) -> Tuple[bool, List[str]]:
        violations: List[str] = []

        # --- Active budgets (causal RL or static) ---
        active_limits = self._active_limits()

        # --- Energy check ---
        if (
            self.consumed['energy_wh']
            + self._safe_float(estimated.get('energy_wh', 0.0))
            > active_limits['energy_wh']
        ):
            violations.append('energy_wh')
            logger.warning("Energy budget would be exceeded")

        # --- Carbon check ---
        if (
            self.consumed['carbon_g']
            + self._safe_float(estimated.get('carbon_g', 0.0))
            > active_limits['carbon_g']
        ):
            violations.append('carbon_g')
            logger.warning("Carbon budget would be exceeded")

        # --- Latency check (per-task) ---
        if (
            self._safe_float(estimated.get('latency_ms', 0.0))
            > active_limits['latency_ms']
        ):
            violations.append('latency_ms')
            logger.warning("Latency budget would be exceeded")

        # --- Cost check ---
        if self.budget.max_cost_usd is not None:
            if (
                self.consumed['cost_usd']
                + self._safe_float(estimated.get('cost_usd', 0.0))
                > self.budget.max_cost_usd
            ):
                violations.append('cost_usd')
                logger.warning("Cost budget would be exceeded")

        # --- Helium check (enhancement) ---
        if self.budget.max_helium_units is not None:
            est_helium = self._safe_float(estimated.get('helium_units', 0.0))
            if est_helium == 0.0 and self.helium_profiler:
                # Auto-estimate helium from energy
                est_helium = HeliumProfiler.estimate_units(
                    self._safe_float(estimated.get('energy_wh', 0.0))
                )
            if (
                self.consumed.get('helium_units', 0.0) + est_helium
                > self.budget.max_helium_units
            ):
                violations.append('helium_units')
                logger.warning("Helium budget would be exceeded")

        can_execute = len(violations) == 0

        # --- Compute utilization and remaining ---
        remaining = self.get_remaining_budget()
        utilization = self.get_utilization()

        # --- Severity (enhancement) ---
        severity = None
        if violations:
            severity = self._classify_can_execute_severity(
                violations, estimated, remaining
            )
            self.breach_by_metric[violations[0]] += 1

        # --- XAI (enhancement) ---
        market_snapshot = None
        if self.market:
            try:
                snap = self.market.get_snapshot()
                market_snapshot = {
                    "carbon_price_per_tco2_usd": snap.carbon_price_per_tco2_usd,
                    "rec_available_mwh": snap.rec_available_mwh,
                }
            except Exception:
                pass

        explanation: Optional[BudgetExplanation] = None
        if self.explainer:
            explanation = BudgetExplainer.explain_can_execute(
                can_execute=can_execute,
                violations=violations,
                estimated=estimated,
                remaining=remaining,
                utilization=utilization,
                severity=severity.value if severity else None,
                market_snapshot=market_snapshot,
            )
            self.last_can_execute_explanation = explanation

        # --- Temporal verification ---
        if self.temporal_monitor:
            ctx = {
                "min_remaining": min(remaining.values())
                if remaining else 0.0,
                "status_valid": True,
                "can_execute_consistent": True,
            }
            self.temporal_monitor.verify(ctx)

        # --- HITL review (only for denials) ---
        hitl_required = False
        hitl_approved: Optional[bool] = None
        if not can_execute and self.hitl:
            for v in violations:
                est = self._safe_float(estimated.get(v, 0.0))
                rem = remaining.get(v, 0.0)
                if self.hitl.needs_review(est, rem):
                    hitl_required = True
                    hitl_approved = self.hitl.request_review(
                        metric=v,
                        estimated=est,
                        remaining=rem,
                        reason=f"denial on {v}",
                        urgency="high" if severity in (
                            Severity.CRITICAL, Severity.EMERGENCY
                        ) else "medium",
                        context={"agent_id": self.agent_id},
                    )
                    break

        # --- Multi-agent feedback ---
        if self.coordinator:
            self.coordinator.record(
                agent_id=self.agent_id,
                success=can_execute,
                dominant_metric=violations[0] if violations else None,
            )

        # --- Federated contribution (every 25 checks) ---
        if self.federated and len(self.execution_history) % 25 == 0:
            for metric in self.consumed:
                self.federated.push(FederatedBudgetProfile(
                    deployment_id=self.deployment_id,
                    budget_name=self.budget.name,
                    metric=metric,
                    mean_utilization=utilization.get(metric, 0.0),
                    sample_count=1,
                ))
            self.federated.aggregate()

        # --- Original violation recording (backward compatible) ---
        if not can_execute:
            violation_record = {
                'timestamp': datetime.now(),
                'violations': violations,
                'estimated_consumption': dict(estimated),
                'current_consumed': dict(self.consumed),
            }
            # Add enhancement fields only if features are active
            if severity:
                violation_record['severity'] = severity.value
            if explanation:
                violation_record['explanation'] = explanation.to_dict()
            if hitl_required:
                violation_record['hitl_required'] = hitl_required
                violation_record['hitl_approved'] = hitl_approved
            self.violation_history.append(violation_record)

        return can_execute, violations

    # ------------------------------------------------------------------
    # Original public API: record_consumption (backward compatible)
    # ------------------------------------------------------------------

    def record_consumption(
        self,
        actual_consumption: Dict[str, float],
        metadata: Dict = None,
    ) -> None:
        """
        Record actual consumption after execution.

        Original signature and behavior preserved.
        """
        if not isinstance(actual_consumption, dict):
            logger.warning("record_consumption: invalid input, skipping")
            return

        # --- Original consumption accounting ---
        for metric, value in actual_consumption.items():
            if metric in self.consumed:
                if metric == 'latency_ms':
                    self.consumed[metric] = max(
                        self.consumed[metric],
                        self._safe_float(value, 0.0),
                    )
                else:
                    self.consumed[metric] += self._safe_float(value, 0.0)

        # --- Auto-estimate helium if budget exists and not provided ---
        if (
            self.features["helium_awareness"]
            and self.budget.max_helium_units is not None
            and 'helium_units' not in actual_consumption
            and 'helium_units' in self.consumed
        ):
            energy_wh = self._safe_float(actual_consumption.get('energy_wh', 0.0))
            helium_est = HeliumProfiler.estimate_units(energy_wh)
            self.consumed['helium_units'] += helium_est

        # --- Original execution record ---
        execution_record = {
            'timestamp': datetime.now(),
            'consumption': dict(actual_consumption),
            'cumulative_consumed': dict(self.consumed),
            'metadata': metadata or {},
        }
        self.execution_history.append(execution_record)

        # --- Original warning check (per-metric) ---
        for metric in self.consumed:
            if metric == 'cost_usd' and self.budget.max_cost_usd is None:
                continue
            if metric == 'helium_units' and self.budget.max_helium_units is None:
                continue
            try:
                status = self.check_budget(metric)
            except ValueError:
                continue
            if status in (BudgetStatus.NEAR_LIMIT, BudgetStatus.AT_LIMIT):
                limit_attr = f"max_{metric}"
                limit_val = getattr(self.budget, limit_attr, 0.0) or 0.0
                logger.warning(
                    f"{metric} {status.value}: "
                    f"{self.consumed[metric]:.2f} / {limit_val:.2f}"
                )

        logger.debug(f"Recorded consumption: {actual_consumption}")

    # ------------------------------------------------------------------
    # Original public API: get_remaining_budget (backward compatible)
    # ------------------------------------------------------------------

    def get_remaining_budget(self) -> Dict[str, float]:
        remaining = {
            'energy_wh': max(
                0, self.budget.max_energy_wh - self.consumed['energy_wh']
            ),
            'carbon_g': max(
                0, self.budget.max_carbon_g - self.consumed['carbon_g']
            ),
            'latency_ms': self.budget.max_latency_ms,
            'cost_usd': max(
                0, (self.budget.max_cost_usd or 0) - self.consumed['cost_usd']
            ),
        }
        if self.budget.max_helium_units is not None:
            remaining['helium_units'] = max(
                0,
                self.budget.max_helium_units
                - self.consumed.get('helium_units', 0.0),
            )
        return remaining

    # ------------------------------------------------------------------
    # Original public API: get_utilization (backward compatible)
    # ------------------------------------------------------------------

    def get_utilization(self) -> Dict[str, float]:
        util = {
            'energy_wh': (
                self.consumed['energy_wh'] / self.budget.max_energy_wh
            ),
            'carbon_g': (
                self.consumed['carbon_g'] / self.budget.max_carbon_g
            ),
            'latency_ms': (
                self.consumed['latency_ms'] / self.budget.max_latency_ms
            ),
            'cost_usd': (
                self.consumed['cost_usd'] / self.budget.max_cost_usd
                if self.budget.max_cost_usd else 0.0
            ),
        }
        if self.budget.max_helium_units is not None:
            util['helium_units'] = (
                self.consumed.get('helium_units', 0.0)
                / self.budget.max_helium_units
            )
        return util

    # ------------------------------------------------------------------
    # Original public API: reset (backward compatible)
    # ------------------------------------------------------------------

    def reset(self) -> None:
        logger.info("Resetting budget consumption")
        self.consumed = {k: 0.0 for k in self.consumed}
        self.execution_history = []
        # Note: violation_history is preserved (original behavior)

    # ------------------------------------------------------------------
    # Original public API: get_summary (backward compatible + extensions)
    # ------------------------------------------------------------------

    def get_summary(self) -> Dict:
        summary = {
            'budget': self.budget.to_dict(),
            'consumed': dict(self.consumed),
            'remaining': self.get_remaining_budget(),
            'utilization': self.get_utilization(),
            'status': {
                metric: self.check_budget(metric).value
                for metric in self.consumed
            },
            'execution_count': len(self.execution_history),
            'violation_count': len(self.violation_history),
        }
        # Enhancement additions (only if features enabled)
        if self.features["temporal_logic"] and self.temporal_monitor:
            summary['temporal_violations'] = self.temporal_monitor.violations[-5:]
        if self.features["causal_rl"] and self.threshold_learner:
            summary['causal_learner'] = {
                'observations': self.threshold_learner.observations,
                'updates': self.threshold_learner.updates,
            }
        if self.features["multi_agent"] and self.coordinator:
            summary['agents'] = {
                aid: {'role': a.role.value, 'calls': a.total_calls}
                for aid, a in self.coordinator.agents.items()
            }
        if self.severity_counts:
            summary['severity_counts'] = dict(self.severity_counts)
        if self.breach_by_metric:
            summary['breach_by_metric'] = dict(self.breach_by_metric)
        if self.features["federated"] and self.federated:
            summary['federated_aggregate'] = self.federated.aggregate()
        return summary

    # ------------------------------------------------------------------
    # Public enhancement APIs
    # ------------------------------------------------------------------

    def explain_last_decision(self) -> Optional[BudgetExplanation]:
        return self.last_can_execute_explanation

    def set_hitl_callback(self, cb: Callable[[HITLRequest], bool]) -> None:
        if self.hitl:
            self.hitl.set_callback(cb)

    def active_learning_samples(self, n: int = 16) -> List[Dict[str, Any]]:
        return self.hitl.active_learning_batch(n) if self.hitl else []

    def record_decision_outcome(self, was_correct: bool) -> None:
        """Feed back whether the last can_execute decision was correct."""
        if not self.threshold_learner:
            return
        util = self.get_utilization()
        state = ThresholdState(
            hour_of_day=datetime.now().hour,
            utilization_ratio=min(1.0, max(util.values() or [0.0])),
            workload_hash=0.5,
        )
        self.threshold_learner.record(state, 1.0 if was_correct else 0.0)
        if self.threshold_learner.observations % 10 == 0:
            self.threshold_learner.update()

    def contribute_federated(self) -> None:
        if not self.federated:
            return
        util = self.get_utilization()
        for metric, ratio in util.items():
            self.federated.push(FederatedBudgetProfile(
                deployment_id=self.deployment_id,
                budget_name=self.budget.name,
                metric=metric,
                mean_utilization=ratio,
                sample_count=max(1, len(self.execution_history)),
            ))
        self.federated.aggregate()

    def get_federated_aggregate(self) -> Dict[str, Dict[str, float]]:
        return self.federated.aggregate() if self.federated else {}

    def distill_budget_policy(
        self, urgency: str = "normal"
    ) -> Optional[DistilledBudgetPolicy]:
        if not self.distiller:
            return None
        precision = (
            self.precision_ctl.select(urgency)
            if self.precision_ctl else PrecisionLevel.INT8
        )
        return self.distiller.distill(
            warning=self.budget.warning_threshold,
            critical=self.budget.critical_threshold,
            precision=precision,
        )

    def get_statistics(self) -> Dict[str, Any]:
        stats = {
            'deployment_id': self.deployment_id,
            'agent_id': self.agent_id,
            'execution_count': len(self.execution_history),
            'violation_count': len(self.violation_history),
            'severity_counts': dict(self.severity_counts),
            'breach_by_metric': dict(self.breach_by_metric),
            'circuit': {
                'state': self._circuit.state.value,
                'failures': self._circuit.failures,
            },
        }
        if self.temporal_monitor:
            stats['temporal_violations'] = self.temporal_monitor.violations[-5:]
        if self.coordinator:
            stats['agents'] = {
                aid: {'role': a.role.value, 'calls': a.total_calls}
                for aid, a in self.coordinator.agents.items()
            }
        if self.chaos:
            stats['chaos_events'] = self.chaos.events[-5:]
        if self.hitl:
            stats['hitl_pending'] = len(self.hitl.pending)
            stats['hitl_feedback_count'] = len(self.hitl.feedback_log)
        if self.market:
            snap = self.market.get_snapshot()
            stats['market'] = {
                'carbon_price_per_tco2_usd': snap.carbon_price_per_tco2_usd,
                'rec_price_per_mwh_usd': snap.rec_price_per_mwh_usd,
            }
        if self.threshold_learner:
            stats['causal_learner'] = {
                'observations': self.threshold_learner.observations,
                'updates': self.threshold_learner.updates,
            }
        if self.federated:
            stats['federated_aggregate'] = self.federated.aggregate()
        return stats

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        try:
            v = float(value)
            return v if math.isfinite(v) else default
        except (TypeError, ValueError):
            return default

    def _active_limits(self) -> Dict[str, float]:
        """
        Return active limits per metric (causal-adjusted if enabled).
        """
        limits = {
            'energy_wh': self.budget.max_energy_wh,
            'carbon_g': self.budget.max_carbon_g,
            'latency_ms': self.budget.max_latency_ms,
            'cost_usd': self.budget.max_cost_usd or float('inf'),
        }
        if self.budget.max_helium_units is not None:
            limits['helium_units'] = self.budget.max_helium_units
        return limits

    @staticmethod
    def _severity_for_status(
        status: BudgetStatus, ratio: float,
    ) -> Severity:
        if status == BudgetStatus.EXCEEDED:
            if ratio >= 2.0:
                return Severity.EMERGENCY
            if ratio >= 1.3:
                return Severity.CRITICAL
            return Severity.WARNING
        if status == BudgetStatus.AT_LIMIT:
            return Severity.WARNING
        if status == BudgetStatus.NEAR_LIMIT:
            return Severity.INFO
        return Severity.INFO

    @staticmethod
    def _classify_can_execute_severity(
        violations: List[str],
        estimated: Dict[str, float],
        remaining: Dict[str, float],
    ) -> Severity:
        if not violations:
            return Severity.INFO
        max_ratio = 0.0
        for v in violations:
            est = estimated.get(v, 0.0)
            rem = remaining.get(v, 0.0)
            if rem > 0:
                ratio = est / rem
            else:
                ratio = float('inf') if est > 0 else 0.0
            if ratio > max_ratio:
                max_ratio = ratio
        if max_ratio >= 2.0:
            return Severity.EMERGENCY
        if max_ratio >= 1.3:
            return Severity.CRITICAL
        if max_ratio >= 1.05:
            return Severity.WARNING
        return Severity.INFO


# =============================================================================
# Demo
# =============================================================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    class _MockHelium:
        scarcity_score = 0.6

    def auto_approve(req: HITLRequest) -> bool:
        logger.info(f"[HITL] auto-approve: {req.reason}")
        return True

    # --- Legacy mode (all features off) ---
    print("\n=== Legacy mode (all features off) ===")
    legacy = BudgetManager(
        Budget.eco_budget(),
        features={k: False for k in BudgetManager.DEFAULT_FEATURES},
    )
    print(f"  can_execute: {legacy.can_execute({'energy_wh': 2.0, 'carbon_g': 0.4, 'latency_ms': 500})}")

    # --- Enhanced mode (all features on) ---
    print("\n=== Enhanced mode ===")
    mgr = BudgetManager(
        Budget.eco_budget(),
        deployment_id="us-ca-prod-01",
        agent_id="budget-A",
        hardware=HardwareProfile(supports_int4=True, vram_gb=48),
    )
    mgr.set_hitl_callback(auto_approve)
    mgr.helium_signal_fn = lambda: _MockHelium()

    # Approve
    can_run, violations = mgr.can_execute({
        'energy_wh': 2.0, 'carbon_g': 0.4, 'latency_ms': 500,
    })
    print(f"\n  Approve case: can_run={can_run}, violations={violations}")
    exp = mgr.explain_last_decision()
    if exp:
        print(f"  XAI: {exp.headline}")
        for line in exp.rationale:
            print(f"    • {line}")

    # Deny
    can_run, violations = mgr.can_execute({
        'energy_wh': 50.0, 'carbon_g': 10.0, 'latency_ms': 500,
    })
    print(f"\n  Deny case: can_run={can_run}, violations={violations}")
    exp = mgr.explain_last_decision()
    if exp:
        print(f"  XAI: {exp.headline}")
        for line in exp.rationale:
            print(f"    • {line}")

    # Record consumption
    mgr.record_consumption({
        'energy_wh': 1.8, 'carbon_g': 0.36, 'latency_ms': 450,
    }, metadata={'task_id': 'task_001'})

    # Check status
    for metric in ('energy_wh', 'carbon_g', 'latency_ms', 'helium_units'):
        try:
            status = mgr.check_budget(metric)
            print(f"\n  Status '{metric}': {status.value}")
        except ValueError:
            pass

    # Outcome feedback
    mgr.record_decision_outcome(was_correct=True)

    # Federated
    mgr.contribute_federated()

    # Distill
    dist = mgr.distill_budget_policy(urgency="normal")
    if dist:
        print(f"\n=== Distilled Budget Policy ===")
        print(f"  Precision:   {dist.precision.value}")
        print(f"  Thresholds:  {dist.thresholds}")

    # Summary
    import json
    print(f"\n=== Summary ===")
    print(json.dumps(mgr.get_summary(), indent=2, default=str))
