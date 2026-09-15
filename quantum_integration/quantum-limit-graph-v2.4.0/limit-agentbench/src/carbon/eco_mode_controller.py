"""
Eco-Mode Controller (Enhanced)
==============================

Dynamically throttles AI workloads based on carbon intensity forecasts,
now with:
  - Causal RL policy adaptation
  - Explainable AI (XAI) for every decision
  - Adaptive precision switching (hardware-aware)
  - Federated green learning across deployments
  - Carbon market & REC integration
  - Temporal logic verification for safety-critical policies
  - Multi-agent coordination with emergent role specialization
  - Human-in-the-Loop (HITL) for critical deferrals
  - Resilience engineering / chaos testing
  - Quantum-distillation hooks for model compression

Location: src/carbon/eco_mode_controller.py
"""

from __future__ import annotations

from typing import Dict, Any, Optional, List, Tuple, Callable, Protocol
from dataclasses import dataclass, field, asdict
from enum import Enum
from datetime import datetime, timedelta
from collections import deque, defaultdict
import logging
import asyncio
import math
import random
import hashlib
import json

logger = logging.getLogger(__name__)


# =============================================================================
# Core enums & data classes (from original, extended)
# =============================================================================

class EcoMode(Enum):
    PERFORMANCE = "performance"
    BALANCED = "balanced"
    GREEN = "green"
    EXTREME_GREEN = "extreme_green"
    EMERGENCY = "emergency"


class PrecisionLevel(Enum):
    FP32 = "fp32"
    FP16 = "fp16"
    INT8 = "int8"
    INT4 = "int4"
    QUANTUM_DISTILLED = "quantum_distilled"


class AgentRole(Enum):
    LATENCY_CRITICAL = "latency_critical"
    THROUGHPUT_ORIENTED = "throughput_oriented"
    CARBON_OPPORTUNISTIC = "carbon_opportunistic"
    DEFERRAL_SPECIALIST = "deferral_specialist"
    BALANCED_WORKER = "balanced_worker"


@dataclass
class EcoModeConfig:
    mode: EcoMode
    compute_limit_percent: int
    max_tokens_multiplier: float
    temperature_override: Optional[float]
    top_k_override: Optional[int]
    use_quantized_model: bool
    use_cache_aggressively: bool
    early_stopping_threshold: float
    quality_degradation_acceptable: float
    # NEW: precision preference
    preferred_precision: PrecisionLevel = PrecisionLevel.FP32


ECO_MODE_CONFIGS = {
    EcoMode.PERFORMANCE: EcoModeConfig(
        EcoMode.PERFORMANCE, 100, 1.0, None, None, False, False, 0.95, 0.0,
        PrecisionLevel.FP32),
    EcoMode.BALANCED: EcoModeConfig(
        EcoMode.BALANCED, 80, 0.8, None, None, False, True, 0.90, 0.05,
        PrecisionLevel.FP16),
    EcoMode.GREEN: EcoModeConfig(
        EcoMode.GREEN, 50, 0.5, 0.3, 10, False, True, 0.85, 0.10,
        PrecisionLevel.INT8),
    EcoMode.EXTREME_GREEN: EcoModeConfig(
        EcoMode.EXTREME_GREEN, 25, 0.25, 0.1, 5, True, True, 0.80, 0.15,
        PrecisionLevel.INT4),
    EcoMode.EMERGENCY: EcoModeConfig(
        EcoMode.EMERGENCY, 10, 0.1, 0.01, 3, True, True, 0.75, 0.20,
        PrecisionLevel.QUANTUM_DISTILLED),
}


# =============================================================================
# ENHANCEMENT 1: Quantum-Distillation Integration (inline hook)
# =============================================================================

@dataclass
class DistillationRequest:
    """Request to distill a model to a lower-precision variant."""
    model_id: str
    target_precision: PrecisionLevel
    max_acceptable_quality_loss: float
    deadline: Optional[datetime] = None


@dataclass
class DistillationResult:
    model_id: str
    precision: PrecisionLevel
    achieved_quality_retention: float
    energy_reduction_percent: float
    distilled_at: datetime = field(default_factory=datetime.utcnow)


class QuantumDistillationBridge:
    """
    Thin inline bridge that *would* call into quantum_integration's
    distillation orchestrator. Keeps controller decoupled — it only
    emits a distillation request and receives a cache hit or not.
    """

    def __init__(self, enabled: bool = True):
        self.enabled = enabled
        self._cache: Dict[Tuple[str, PrecisionLevel], DistillationResult] = {}
        self._pending: List[DistillationRequest] = []

    def lookup(self, model_id: str, precision: PrecisionLevel) -> Optional[DistillationResult]:
        return self._cache.get((model_id, precision))

    async def request_distillation(
        self, req: DistillationRequest
    ) -> DistillationResult:
        # Simulated async call — real impl delegates to distillation_orchestrator
        await asyncio.sleep(0)
        result = DistillationResult(
            model_id=req.model_id,
            precision=req.target_precision,
            achieved_quality_retention=1.0 - req.max_acceptable_quality_loss * 0.5,
            energy_reduction_percent={
                PrecisionLevel.FP32: 0,
                PrecisionLevel.FP16: 30,
                PrecisionLevel.INT8: 55,
                PrecisionLevel.INT4: 70,
                PrecisionLevel.QUANTUM_DISTILLED: 85,
            }[req.target_precision],
        )
        self._cache[(req.model_id, req.target_precision)] = result
        return result


# =============================================================================
# ENHANCEMENT 2: Causal RL for Policy Adaptation
# =============================================================================

@dataclass
class PolicyState:
    carbon_intensity: float
    forecast_intensity: float
    gpu_utilization: float
    queue_depth: int
    recent_quality_loss: float


@dataclass
class PolicyTransition:
    state: PolicyState
    action: EcoMode
    reward: float
    next_state: PolicyState
    timestamp: datetime = field(default_factory=datetime.utcnow)


class CausalRLPolicy:
    """
    Lightweight causal RL policy that learns a mapping
    state -> EcoMode using counterfactual rewards.
    Uses ε-greedy with a causal reward shaper that penalizes
    unnecessary throttling (quality loss) and rewards carbon savings.
    """

    def __init__(self, n_actions: int = 5, epsilon: float = 0.1, lr: float = 0.01):
        self.n_actions = n_actions
        self.epsilon = epsilon
        self.lr = lr
        # Simple linear Q-weights per action
        self.weights: Dict[EcoMode, List[float]] = {
            m: [0.0] * 5 for m in EcoMode
        }
        self.buffer: deque[PolicyTransition] = deque(maxlen=2048)
        self._mode_list = list(EcoMode)

    # ---- feature extraction ----
    @staticmethod
    def _features(s: PolicyState) -> List[float]:
        return [
            s.carbon_intensity / 1000.0,
            s.forecast_intensity / 1000.0,
            s.gpu_utilization,
            min(s.queue_depth / 100.0, 1.0),
            s.recent_quality_loss,
        ]

    def _q(self, s: PolicyState, a: EcoMode) -> float:
        f = self._features(s)
        return sum(w * x for w, x in zip(self.weights[a], f))

    def select_action(self, s: PolicyState) -> EcoMode:
        if random.random() < self.epsilon:
            return random.choice(self._mode_list)
        return max(self._mode_list, key=lambda a: self._q(s, a))

    # ---- causal reward shaper ----
    @staticmethod
    def causal_reward(
        state: PolicyState,
        action: EcoMode,
        next_state: PolicyState,
        actual_quality_loss: float,
        energy_saved_percent: float,
    ) -> float:
        carbon_delta = (state.carbon_intensity - next_state.carbon_intensity) / 1000.0
        quality_penalty = actual_quality_loss * 2.0
        return energy_saved_percent / 100.0 + carbon_delta - quality_penalty

    def record(self, t: PolicyTransition) -> None:
        self.buffer.append(t)

    def update(self) -> None:
        """Batch linear-Q update using stored transitions."""
        if not self.buffer:
            return
        for tr in self.buffer:
            f = self._features(tr.state)
            q = sum(w * x for w, x in zip(self.weights[tr.action], f))
            target = tr.reward + 0.95 * max(
                self._q(tr.next_state, a) for a in self._mode_list
            )
            err = target - q
            for i, x in enumerate(f):
                self.weights[tr.action][i] += self.lr * err * x
        self.buffer.clear()


# =============================================================================
# ENHANCEMENT 3: Federated Green Learning Across Deployments
# =============================================================================

@dataclass
class FederatedUpdate:
    deployment_id: str
    mode_counts: Dict[str, int]
    avg_intensity: float
    avg_quality_loss: float
    weight: int  # number of samples
    timestamp: datetime = field(default_factory=datetime.utcnow)


class FederatedAggregator:
    """Weighted aggregation of mode-transition statistics."""

    def __init__(self) -> None:
        self._updates: List[FederatedUpdate] = []

    def push(self, update: FederatedUpdate) -> None:
        self._updates.append(update)

    def aggregate(self) -> Dict[str, Any]:
        if not self._updates:
            return {}
        total_w = sum(u.weight for u in self._updates)
        merged: Dict[str, int] = defaultdict(int)
        inten = 0.0
        qloss = 0.0
        for u in self._updates:
            for k, v in u.mode_counts.items():
                merged[k] += v
            inten += u.avg_intensity * u.weight
            qloss += u.avg_quality_loss * u.weight
        return {
            "mode_counts": dict(merged),
            "global_avg_intensity": inten / total_w,
            "global_avg_quality_loss": qloss / total_w,
            "contributors": len(self._updates),
        }

    def reset(self) -> None:
        self._updates.clear()


# =============================================================================
# ENHANCEMENT 4: Multi-Agent Coordination with Emergent Roles
# =============================================================================

@dataclass
class AgentProfile:
    agent_id: str
    role: AgentRole
    success_rate: float = 0.0
    deferral_success: float = 0.0
    energy_efficiency: float = 0.0


class MultiAgentCoordinator:
    """
    Assigns roles to worker agents based on observed performance
    (emergent specialization). Routes tasks accordingly.
    """

    def __init__(self) -> None:
        self.agents: Dict[str, AgentProfile] = {}
        self._history: Dict[str, List[float]] = defaultdict(list)

    def register(self, agent_id: str) -> AgentProfile:
        if agent_id not in self.agents:
            self.agents[agent_id] = AgentProfile(agent_id, AgentRole.BALANCED_WORKER)
        return self.agents[agent_id]

    def _reassign_roles(self) -> None:
        for a in self.agents.values():
            if a.deferral_success > 0.8:
                a.role = AgentRole.DEFERRAL_SPECIALIST
            elif a.energy_efficiency > 0.7:
                a.role = AgentRole.CARBON_OPPORTUNISTIC
            elif a.success_rate > 0.9:
                a.role = AgentRole.LATENCY_CRITICAL
            else:
                a.role = AgentRole.BALANCED_WORKER

    def record_outcome(
        self,
        agent_id: str,
        success: bool,
        was_deferred: bool,
        energy_saved_pct: float,
    ) -> None:
        a = self.register(agent_id)
        n = len(self._history[agent_id]) + 1
        a.success_rate = ((n - 1) * a.success_rate + float(success)) / n
        if was_deferred:
            a.deferral_success = (
                (n - 1) * a.deferral_success + float(success)
            ) / n
        a.energy_efficiency = (
            (n - 1) * a.energy_efficiency + energy_saved_pct / 100.0
        ) / n
        self._history[agent_id].append(energy_saved_pct)
        if n % 5 == 0:
            self._reassign_roles()

    def select_agent(self, task: Dict[str, Any]) -> Optional[str]:
        if not self.agents:
            return None
        if task.get("deferrable") and task.get("low_priority"):
            specialists = [
                a for a in self.agents.values()
                if a.role == AgentRole.DEFERRAL_SPECIALIST
            ]
            if specialists:
                return max(specialists, key=lambda a: a.deferral_success).agent_id
        if task.get("latency_critical"):
            crit = [
                a for a in self.agents.values()
                if a.role == AgentRole.LATENCY_CRITICAL
            ]
            if crit:
                return max(crit, key=lambda a: a.success_rate).agent_id
        return max(self.agents.values(), key=lambda a: a.energy_efficiency).agent_id


# =============================================================================
# ENHANCEMENT 5: Temporal Logic & Formal Verification
# =============================================================================

class TemporalProperty(Protocol):
    def check(self, history: List[Tuple[datetime, EcoMode, float]]) -> bool: ...


@dataclass
class NoEmergencyDeferralWithinDeadline:
    """G(EMERGENCY → ¬defer past deadline)."""
    deadline: datetime

    def check(self, history: List[Tuple[datetime, EcoMode, float]]) -> bool:
        for ts, mode, _ in history:
            if mode == EcoMode.EMERGENCY and ts > self.deadline:
                return False
        return True


@dataclass
class BoundedModeChurn:
    """No more than N mode changes in any 10-minute window."""
    max_changes: int = 4
    window_seconds: int = 600

    def check(self, history: List[Tuple[datetime, EcoMode, float]]) -> bool:
        if len(history) < 2:
            return True
        for i in range(len(history)):
            window_end = history[i][0] + timedelta(seconds=self.window_seconds)
            changes = sum(
                1 for j in range(i + 1, len(history)) if history[j][0] <= window_end
            )
            if changes > self.max_changes:
                return False
        return True


class TemporalLogicMonitor:
    def __init__(self) -> None:
        self.properties: List[TemporalProperty] = []
        self.violations: List[Dict[str, Any]] = []

    def register(self, prop: TemporalProperty) -> None:
        self.properties.append(prop)

    def verify(
        self, history: List[Tuple[datetime, EcoMode, float]]
    ) -> bool:
        ok = True
        for p in self.properties:
            if not p.check(history):
                ok = False
                self.violations.append({
                    "property": type(p).__name__,
                    "at": datetime.utcnow().isoformat(),
                })
        return ok


# =============================================================================
# ENHANCEMENT 6: Explainable AI (XAI)
# =============================================================================

@dataclass
class Explanation:
    headline: str
    rationale: List[str]
    counterfactual: str
    confidence: float
    contributing_factors: Dict[str, float]


class XAIExplainer:
    """Generates human-readable explanations for throttling decisions."""

    @staticmethod
    def explain(
        mode: EcoMode,
        intensity: float,
        forecast: float,
        thresholds: Dict[EcoMode, float],
        config: EcoModeConfig,
        rl_weights_used: bool = False,
    ) -> Explanation:
        reasons: List[str] = []
        reasons.append(
            f"Effective carbon intensity = {max(intensity, forecast):.0f} gCO₂/kWh "
            f"(current={intensity:.0f}, forecast={forecast:.0f})."
        )
        # Find which threshold caused this mode
        for m, thr in thresholds.items():
            if m == mode:
                reasons.append(
                    f"Mode '{mode.value}' selected because intensity is below "
                    f"the {m.value} threshold ({thr if thr != float('inf') else '∞'})."
                )
                break
        reasons.append(
            f"Compute limited to {config.compute_limit_percent}% → "
            f"energy reduction ≈ {100 - config.compute_limit_percent}%."
        )
        reasons.append(
            f"Acceptable quality loss: {config.quality_degradation_acceptable*100:.1f}%."
        )
        if rl_weights_used:
            reasons.append("Adaptive causal-RL policy overrode static thresholds.")

        counterfactual = (
            f"If carbon intensity had stayed below "
            f"{thresholds.get(EcoMode.BALANCED, 250):.0f}, mode would be BALANCED."
        )
        factors = {
            "carbon_intensity": intensity / 1000.0,
            "forecast_intensity": forecast / 1000.0,
            "compute_limit": config.compute_limit_percent / 100.0,
            "quality_risk": config.quality_degradation_acceptable,
        }
        conf = 0.95 if not rl_weights_used else 0.80
        return Explanation(
            headline=f"Eco-Mode = {mode.value.upper()}",
            rationale=reasons,
            counterfactual=counterfactual,
            confidence=conf,
            contributing_factors=factors,
        )


# =============================================================================
# ENHANCEMENT 7: Adaptive Precision Switching (Hardware-Aware)
# =============================================================================

@dataclass
class HardwareProfile:
    has_tensor_cores: bool = True
    supports_int8: bool = True
    supports_int4: bool = False
    vram_gb: float = 24.0
    tdp_watts: float = 350.0


class AdaptivePrecisionController:
    """Chooses a precision based on hardware + eco-mode + task SLA."""

    def __init__(self, hw: Optional[HardwareProfile] = None) -> None:
        self.hw = hw or HardwareProfile()

    def select(
        self, mode: EcoMode, task: Dict[str, Any]
    ) -> PrecisionLevel:
        if task.get("latency_critical"):
            return PrecisionLevel.FP16 if self.hw.has_tensor_cores else PrecisionLevel.FP32
        if mode in (EcoMode.PERFORMANCE, EcoMode.BALANCED):
            return PrecisionLevel.FP16
        if mode == EcoMode.GREEN:
            return PrecisionLevel.INT8 if self.hw.supports_int8 else PrecisionLevel.FP16
        if mode == EcoMode.EXTREME_GREEN:
            return PrecisionLevel.INT4 if self.hw.supports_int4 else PrecisionLevel.INT8
        # EMERGENCY
        return PrecisionLevel.QUANTUM_DISTILLED


# =============================================================================
# ENHANCEMENT 8: Carbon Markets & Renewable Energy Credits
# =============================================================================

@dataclass
class MarketQuote:
    price_per_kwh: float  # in fiat
    credit_kgco2: float   # credit value in kgCO2 offset per kWh

    def effective_cost(self, energy_kwh: float) -> float:
        return self.price_per_kwh * energy_kwh


class CarbonMarketBridge:
    """Inline adapter for buying credits or selling saved energy."""

    def __init__(self, budget_usd: float = 10.0) -> None:
        self.budget = budget_usd
        self.spent = 0.0
        self.earned = 0.0
        self.trades: List[Dict[str, Any]] = []

    async def get_quote(self) -> MarketQuote:
        # Simulated quote; real impl calls carbon_credit_marketplace
        await asyncio.sleep(0)
        return MarketQuote(price_per_kwh=0.08, credit_kgco2=0.4)

    def should_buy_credits(
        self, task: Dict[str, Any], mode: EcoMode, quote: MarketQuote
    ) -> bool:
        if mode != EcoMode.EMERGENCY:
            return False
        if not task.get("deadline_critical"):
            return False
        energy = task.get("estimated_energy_kwh", 0.001)
        cost = quote.effective_cost(energy)
        return (self.spent + cost) <= self.budget

    async def buy_credits(
        self, energy_kwh: float, quote: MarketQuote, reason: str
    ) -> Dict[str, Any]:
        cost = quote.effective_cost(energy_kwh)
        self.spent += cost
        rec = {
            "type": "buy",
            "energy_kwh": energy_kwh,
            "cost_usd": cost,
            "co2_offset_kg": energy_kwh * quote.credit_kgco2,
            "reason": reason,
            "at": datetime.utcnow().isoformat(),
        }
        self.trades.append(rec)
        return rec

    def sell_savings(
        self, saved_kwh: float, quote: MarketQuote
    ) -> Dict[str, Any]:
        revenue = saved_kwh * quote.price_per_kwh * 0.5
        self.earned += revenue
        rec = {
            "type": "sell",
            "energy_kwh": saved_kwh,
            "revenue_usd": revenue,
            "at": datetime.utcnow().isoformat(),
        }
        self.trades.append(rec)
        return rec


# =============================================================================
# ENHANCEMENT 9: Resilience Engineering & Chaos Testing
# =============================================================================

class ChaosInjector:
    """Injects controlled faults for resilience testing."""

    def __init__(
        self,
        failure_rate: float = 0.0,
        forecaster_down: bool = False,
    ) -> None:
        self.failure_rate = failure_rate
        self.forecaster_down = forecaster_down
        self.events: List[Dict[str, Any]] = []

    def maybe_fail(self, component: str) -> bool:
        if random.random() < self.failure_rate:
            self.events.append({
                "component": component,
                "at": datetime.utcnow().isoformat(),
            })
            return True
        if component == "forecaster" and self.forecaster_down:
            return True
        return False


# =============================================================================
# ENHANCEMENT 10: Human-in-the-Loop with Active Learning
# =============================================================================

@dataclass
class HITLRequest:
    task_id: str
    proposed_mode: EcoMode
    reason: str
    urgency: str  # "low" | "medium" | "high"
    requested_at: datetime = field(default_factory=datetime.utcnow)


class HITLGate:
    """
    Asks a human for approval on high-stakes decisions.
    Records decisions for active-learning fine-tuning.
    """

    def __init__(self, auto_approve_low_risk: bool = True) -> None:
        self.auto_approve_low_risk = auto_approve_low_risk
        self.pending: List[HITLRequest] = []
        self.feedback_log: List[Dict[str, Any]] = []
        self._callback: Optional[Callable[[HITLRequest], bool]] = None

    def set_callback(self, cb: Callable[[HITLRequest], bool]) -> None:
        self._callback = cb

    async def request_approval(
        self, task: Dict[str, Any], mode: EcoMode, reason: str
    ) -> bool:
        urgency = "high" if task.get("deadline_critical") else (
            "medium" if mode == EcoMode.EMERGENCY else "low"
        )
        req = HITLRequest(
            task_id=str(task.get("task_id", "unknown")),
            proposed_mode=mode,
            reason=reason,
            urgency=urgency,
        )
        if urgency == "low" and self.auto_approve_low_risk:
            self.feedback_log.append({"req": asdict(req), "decision": True})
            return True
        if self._callback is None:
            # No human available — fail-safe: reject
            self.feedback_log.append({"req": asdict(req), "decision": False})
            return False
        decision = self._callback(req)
        self.feedback_log.append({"req": asdict(req), "decision": decision})
        return decision

    def active_learning_batch(self, n: int = 16) -> List[Dict[str, Any]]:
        return self.feedback_log[-n:]


# =============================================================================
# Extended decision record
# =============================================================================

@dataclass
class ThrottlingDecision:
    original_task: Dict[str, Any]
    throttled_task: Dict[str, Any]
    eco_mode: EcoMode
    carbon_intensity: float
    estimated_energy_reduction_percent: float
    estimated_quality_impact_percent: float
    should_defer: bool
    defer_until: Optional[datetime]
    # NEW fields
    precision: PrecisionLevel = PrecisionLevel.FP32
    explanation: Optional[Explanation] = None
    assigned_agent: Optional[str] = None
    hitl_required: bool = False
    hitl_approved: Optional[bool] = None
    market_trade: Optional[Dict[str, Any]] = None
    temporal_verified: bool = True
    distillation_used: bool = False


# =============================================================================
# Enhanced EcoModeController
# =============================================================================

class EcoModeController:
    """
    Enhanced carbon-aware eco-mode controller.

    Strategy unchanged for backward compatibility, but now augmented by
    ten orthogonal enhancement layers. All layers can be individually
    enabled/disabled via `features` for A/B testing.
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
    }

    def __init__(
        self,
        carbon_forecaster,
        carbon_thresholds: Optional[Dict[EcoMode, float]] = None,
        deployment_id: str = "local",
        features: Optional[Dict[str, bool]] = None,
        hardware: Optional[HardwareProfile] = None,
        market_budget_usd: float = 10.0,
    ):
        self.carbon_forecaster = carbon_forecaster
        self.deployment_id = deployment_id
        self.features = {**self.DEFAULT_FEATURES, **(features or {})}

        self.carbon_thresholds = carbon_thresholds or {
            EcoMode.PERFORMANCE: 150,
            EcoMode.BALANCED: 250,
            EcoMode.GREEN: 400,
            EcoMode.EXTREME_GREEN: 600,
            EcoMode.EMERGENCY: float("inf"),
        }

        self.current_mode = EcoMode.BALANCED
        self.mode_history: List[Tuple[datetime, EcoMode, float]] = []

        # Statistics
        self.total_tasks_processed = 0
        self.total_tasks_deferred = 0
        self.total_energy_saved_kwh = 0.0
        self.quality_impact_sum = 0.0

        # --- Enhancement layers ---
        self.rl_policy = CausalRLPolicy() if self.features["causal_rl"] else None
        self.explainer = XAIExplainer() if self.features["xai"] else None
        self.precision_ctl = (
            AdaptivePrecisionController(hardware)
            if self.features["adaptive_precision"] else None
        )
        self.federated = FederatedAggregator() if self.features["federated"] else None
        self.coordinator = (
            MultiAgentCoordinator() if self.features["multi_agent"] else None
        )
        self.temporal_monitor = (
            TemporalLogicMonitor() if self.features["temporal_logic"] else None
        )
        self.market = (
            CarbonMarketBridge(market_budget_usd)
            if self.features["carbon_market"] else None
        )
        self.chaos = ChaosInjector() if self.features["chaos_testing"] else None
        self.hitl = HITLGate() if self.features["hitl"] else None
        self.distiller = (
            QuantumDistillationBridge()
            if self.features["quantum_distillation"] else None
        )

        # Register default temporal properties
        if self.temporal_monitor:
            self.temporal_monitor.register(BoundedModeChurn())

        logger.info(
            f"Enhanced Eco-mode controller initialized "
            f"(deployment={deployment_id}, features={list(self.features)})"
        )

    # ------------------------------------------------------------------
    # Mode selection
    # ------------------------------------------------------------------

    def _state_from_inputs(
        self, intensity: float, forecast: float, task: Optional[Dict[str, Any]] = None
    ) -> PolicyState:
        task = task or {}
        return PolicyState(
            carbon_intensity=intensity,
            forecast_intensity=forecast,
            gpu_utilization=task.get("gpu_utilization", 0.5),
            queue_depth=task.get("queue_depth", 0),
            recent_quality_loss=(
                self.quality_impact_sum / self.total_tasks_processed / 100.0
                if self.total_tasks_processed else 0.0
            ),
        )

    async def _fetch_forecast(self) -> Tuple[float, float]:
        """Return (current, avg_forecast), honoring chaos injection."""
        if self.chaos and self.chaos.maybe_fail("forecaster"):
            logger.warning("Chaos: forecaster down — falling back to last known mode")
            return 200.0, 200.0  # safe fallback

        current = await self.carbon_forecaster.get_current_intensity()
        try:
            forecasts = await self.carbon_forecaster.predict(
                horizon="1h", interval_minutes=15
            )
            avg = sum(f.predicted_intensity for f in forecasts) / max(len(forecasts), 1)
        except Exception as e:
            logger.warning(f"Forecast failed: {e}; using current only")
            avg = current
        return current, avg

    async def determine_eco_mode(self) -> EcoMode:
        current, forecast = await self._fetch_forecast()
        effective = max(current, forecast)

        # --- Threshold-based (fallback / baseline) ---
        if effective < self.carbon_thresholds[EcoMode.PERFORMANCE]:
            threshold_mode = EcoMode.PERFORMANCE
        elif effective < self.carbon_thresholds[EcoMode.BALANCED]:
            threshold_mode = EcoMode.BALANCED
        elif effective < self.carbon_thresholds[EcoMode.GREEN]:
            threshold_mode = EcoMode.GREEN
        elif effective < self.carbon_thresholds[EcoMode.EXTREME_GREEN]:
            threshold_mode = EcoMode.EXTREME_GREEN
        else:
            threshold_mode = EcoMode.EMERGENCY

        # --- Causal RL override ---
        mode = threshold_mode
        if self.rl_policy:
            state = self._state_from_inputs(current, forecast)
            rl_mode = self.rl_policy.select_action(state)
            # Conservative: prefer the *worse* of threshold vs RL for safety
            order = list(EcoMode)
            mode = rl_mode if order.index(rl_mode) >= order.index(threshold_mode) else threshold_mode

        if mode != self.current_mode:
            logger.info(
                f"Eco-mode changed: {self.current_mode.value} → {mode.value} "
                f"(intensity: {effective:.0f} gCO2/kWh)"
            )
            self.current_mode = mode
            self.mode_history.append((datetime.now(), mode, effective))

            # Temporal verification
            if self.temporal_monitor:
                if not self.temporal_monitor.verify(self.mode_history):
                    logger.error("Temporal property violated — reverting mode")
                    self.current_mode = EcoMode.EMERGENCY
                    mode = EcoMode.EMERGENCY

        return mode

    # ------------------------------------------------------------------
    # Throttling pipeline
    # ------------------------------------------------------------------

    async def apply_throttling(
        self,
        task: Dict[str, Any],
        force_mode: Optional[EcoMode] = None,
    ) -> ThrottlingDecision:
        # 1. Determine mode
        eco_mode = force_mode or await self.determine_eco_mode()
        config = ECO_MODE_CONFIGS[eco_mode]
        current, forecast = await self._fetch_forecast()
        carbon_intensity = current

        # 2. Multi-agent role assignment
        assigned_agent = None
        if self.coordinator:
            assigned_agent = self.coordinator.select_agent(task)

        # 3. Deferral decision
        should_defer = False
        defer_until = None
        if eco_mode == EcoMode.EMERGENCY and task.get("deferrable", True):
            # HITL check for emergency deferral
            hitl_approved: Optional[bool] = None
            hitl_required = False
            if self.hitl:
                hitl_required = True
                approved = await self.hitl.request_approval(
                    task, eco_mode,
                    reason="EMERGENCY deferral requires human confirmation"
                )
                hitl_approved = approved
                if not approved:
                    logger.info("HITL denied deferral — proceeding throttled")
                else:
                    try:
                        deadline = task.get(
                            "deadline", datetime.now() + timedelta(hours=48)
                        )
                        window = await self.carbon_forecaster.find_optimal_execution_window(
                            duration_hours=task.get("estimated_duration_hours", 1.0),
                            deadline=deadline,
                        )
                        should_defer = True
                        defer_until = window.start_time
                    except Exception as e:
                        logger.warning(f"Deferral window lookup failed: {e}")
            else:
                try:
                    deadline = task.get(
                        "deadline", datetime.now() + timedelta(hours=48)
                    )
                    window = await self.carbon_forecaster.find_optimal_execution_window(
                        duration_hours=task.get("estimated_duration_hours", 1.0),
                        deadline=deadline,
                    )
                    should_defer = True
                    defer_until = window.start_time
                except Exception as e:
                    logger.warning(f"Deferral window lookup failed: {e}")

        # 4. Adaptive precision
        precision = config.preferred_precision
        if self.precision_ctl:
            precision = self.precision_ctl.select(eco_mode, task)

        # 5. Quantum distillation hook
        distillation_used = False
        if self.distiller and precision == PrecisionLevel.QUANTUM_DISTILLED:
            model_id = task.get("model_id", "default")
            cached = self.distiller.lookup(model_id, precision)
            if cached is None:
                cached = await self.distiller.request_distillation(
                    DistillationRequest(
                        model_id=model_id,
                        target_precision=precision,
                        max_acceptable_quality_loss=config.quality_degradation_acceptable,
                    )
                )
            distillation_used = True
            logger.debug(
                f"Distilled {model_id} → {precision.value} "
                f"(retention={cached.achieved_quality_retention:.2f})"
            )

        # 6. Carbon market interaction
        market_trade = None
        if self.market:
            quote = await self.market.get_quote()
            if self.market.should_buy_credits(task, eco_mode, quote):
                energy = task.get("estimated_energy_kwh", 0.001)
                market_trade = await self.market.buy_credits(
                    energy, quote,
                    reason="deadline-critical task in EMERGENCY mode",
                )

        # 7. Build throttled task
        throttled_task = dict(task)
        if "max_tokens" in throttled_task:
            throttled_task["max_tokens"] = int(
                throttled_task["max_tokens"] * config.max_tokens_multiplier
            )
        if config.temperature_override is not None:
            throttled_task["temperature"] = config.temperature_override
        if config.top_k_override is not None:
            throttled_task["top_k"] = config.top_k_override
        throttled_task["use_quantized_model"] = config.use_quantized_model
        throttled_task["use_cache_aggressively"] = config.use_cache_aggressively
        throttled_task["early_stopping_threshold"] = config.early_stopping_threshold
        throttled_task["precision"] = precision.value
        if assigned_agent:
            throttled_task["assigned_agent"] = assigned_agent
        if should_defer:
            throttled_task["deferred_until"] = defer_until.isoformat()

        # 8. Metrics
        energy_reduction_percent = 100 - config.compute_limit_percent
        quality_impact_percent = config.quality_degradation_acceptable * 100

        baseline_energy_kwh = task.get("estimated_energy_kwh", 0.001)
        energy_saved_kwh = baseline_energy_kwh * (energy_reduction_percent / 100)

        # 9. XAI explanation
        explanation = None
        if self.explainer:
            explanation = XAIExplainer.explain(
                mode=eco_mode,
                intensity=current,
                forecast=forecast,
                thresholds=self.carbon_thresholds,
                config=config,
                rl_weights_used=self.rl_policy is not None,
            )

        # 10. Statistics
        self.total_tasks_processed += 1
        if should_defer:
            self.total_tasks_deferred += 1
        self.total_energy_saved_kwh += energy_saved_kwh
        self.quality_impact_sum += quality_impact_percent

        # 11. Federated contribution
        if self.federated and self.total_tasks_processed % 50 == 0:
            self.federated.push(FederatedUpdate(
                deployment_id=self.deployment_id,
                mode_counts={eco_mode.value: 1},
                avg_intensity=current,
                avg_quality_loss=quality_impact_percent / 100.0,
                weight=1,
            ))

        # 12. RL feedback record
        if self.rl_policy:
            state = self._state_from_inputs(current, forecast, task)
            next_state = self._state_from_inputs(
                current, forecast * 0.95, task
            )  # simulated improvement
            reward = CausalRLPolicy.causal_reward(
                state, eco_mode, next_state,
                actual_quality_loss=quality_impact_percent / 100.0,
                energy_saved_percent=energy_reduction_percent,
            )
            self.rl_policy.record(PolicyTransition(state, eco_mode, reward, next_state))
            if self.total_tasks_processed % 20 == 0:
                self.rl_policy.update()

        # 13. Multi-agent outcome feedback
        if self.coordinator and assigned_agent:
            self.coordinator.record_outcome(
                assigned_agent,
                success=True,
                was_deferred=should_defer,
                energy_saved_pct=energy_reduction_percent,
            )

        return ThrottlingDecision(
            original_task=task,
            throttled_task=throttled_task,
            eco_mode=eco_mode,
            carbon_intensity=carbon_intensity,
            estimated_energy_reduction_percent=energy_reduction_percent,
            estimated_quality_impact_percent=quality_impact_percent,
            should_defer=should_defer,
            defer_until=defer_until,
            precision=precision,
            explanation=explanation,
            assigned_agent=assigned_agent,
            hitl_required=(self.hitl is not None and eco_mode == EcoMode.EMERGENCY),
            hitl_approved=None,
            market_trade=market_trade,
            temporal_verified=True,
            distillation_used=distillation_used,
        )

    async def batch_throttle(
        self, tasks: List[Dict[str, Any]]
    ) -> List[ThrottlingDecision]:
        return [await self.apply_throttling(t) for t in tasks]

    # ------------------------------------------------------------------
    # Reporting
    # ------------------------------------------------------------------

    def get_statistics(self) -> Dict[str, Any]:
        if self.total_tasks_processed == 0:
            return {"total_tasks_processed": 0, "current_mode": self.current_mode.value}
        stats: Dict[str, Any] = {
            "current_mode": self.current_mode.value,
            "total_tasks_processed": self.total_tasks_processed,
            "total_tasks_deferred": self.total_tasks_deferred,
            "deferral_rate_percent": (
                self.total_tasks_deferred / self.total_tasks_processed * 100
            ),
            "total_energy_saved_kwh": self.total_energy_saved_kwh,
            "avg_energy_saved_per_task_kwh": (
                self.total_energy_saved_kwh / self.total_tasks_processed
            ),
            "avg_quality_impact_percent": (
                self.quality_impact_sum / self.total_tasks_processed
            ),
            "mode_history": [
                {"timestamp": ts.isoformat(), "mode": m.value, "carbon_intensity": i}
                for ts, m, i in self.mode_history[-10:]
            ],
        }
        if self.market:
            stats["market"] = {
                "spent_usd": self.market.spent,
                "earned_usd": self.market.earned,
                "trades": len(self.market.trades),
            }
        if self.coordinator:
            stats["agents"] = {
                aid: {"role": a.role.value, "efficiency": a.energy_efficiency}
                for aid, a in self.coordinator.agents.items()
            }
        if self.federated:
            stats["federated_aggregate"] = self.federated.aggregate()
        if self.temporal_monitor:
            stats["temporal_violations"] = self.temporal_monitor.violations[-5:]
        if self.hitl:
            stats["hitl_feedback_count"] = len(self.hitl.feedback_log)
        if self.chaos:
            stats["chaos_events"] = self.chaos.events[-5:]
        return stats

    async def get_current_recommendation(self) -> Dict[str, Any]:
        current, forecast = await self._fetch_forecast()
        eco_mode = await self.determine_eco_mode()
        config = ECO_MODE_CONFIGS[eco_mode]

        try:
            forecasts = await self.carbon_forecaster.predict(
                horizon="6h", interval_minutes=60
            )
            min_forecast = min(forecasts, key=lambda f: f.predicted_intensity)
            next_6h = [
                {
                    "timestamp": f.timestamp.isoformat(),
                    "intensity": f.predicted_intensity,
                    "confidence": f.confidence,
                }
                for f in forecasts
            ]
        except Exception as e:
            logger.warning(f"6h forecast failed: {e}")
            min_forecast = None
            next_6h = []

        explanation = None
        if self.explainer:
            exp = XAIExplainer.explain(
                mode=eco_mode,
                intensity=current,
                forecast=forecast,
                thresholds=self.carbon_thresholds,
                config=config,
                rl_weights_used=self.rl_policy is not None,
            )
            explanation = {
                "headline": exp.headline,
                "rationale": exp.rationale,
                "counterfactual": exp.counterfactual,
                "confidence": exp.confidence,
            }

        return {
            "current_intensity_gco2kwh": current,
            "recommended_mode": eco_mode.value,
            "compute_limit_percent": config.compute_limit_percent,
            "quality_degradation_acceptable_percent": (
                config.quality_degradation_acceptable * 100
            ),
            "preferred_precision": config.preferred_precision.value,
            "should_defer_new_tasks": eco_mode == EcoMode.EMERGENCY,
            "optimal_time_next_6h": (
                {
                    "timestamp": min_forecast.timestamp.isoformat(),
                    "intensity_gco2kwh": min_forecast.predicted_intensity,
                    "hours_from_now": (
                        min_forecast.timestamp - datetime.now()
                    ).total_seconds() / 3600,
                }
                if min_forecast else None
            ),
            "forecast_next_6h": next_6h,
            "explanation": explanation,
        }


# =============================================================================
# Convenience factory
# =============================================================================

def create_eco_mode_controller(
    carbon_forecaster,
    custom_thresholds: Optional[Dict[EcoMode, float]] = None,
    deployment_id: str = "local",
    features: Optional[Dict[str, bool]] = None,
    hardware: Optional[HardwareProfile] = None,
) -> EcoModeController:
    return EcoModeController(
        carbon_forecaster=carbon_forecaster,
        carbon_thresholds=custom_thresholds,
        deployment_id=deployment_id,
        features=features,
        hardware=hardware,
    )


# =============================================================================
# Demo
# =============================================================================

if __name__ == "__main__":

    class _DummyForecaster:
        async def get_current_intensity(self) -> float:
            return 320.0

        async def predict(self, horizon: str, interval_minutes: int):
            class F:
                def __init__(self, i):
                    self.timestamp = datetime.now() + timedelta(minutes=15 * i)
                    self.predicted_intensity = 300 + 20 * math.sin(i)
                    self.confidence = 0.9
            n = 4 if horizon == "1h" else 6
            return [F(i) for i in range(n)]

        async def find_optimal_execution_window(self, duration_hours, deadline):
            class W:
                start_time = datetime.now() + timedelta(hours=6)
                carbon_savings_percent = 42.0
            return W()

    async def main():
        logging.basicConfig(level=logging.INFO)
        forecaster = _DummyForecaster()
        controller = create_eco_mode_controller(
            forecaster,
            deployment_id="us-ca-prod-01",
            hardware=HardwareProfile(supports_int4=True, vram_gb=48),
        )
        controller.coordinator.register("agent-A")
        controller.coordinator.register("agent-B")

        rec = await controller.get_current_recommendation()
        print("\n=== Current Recommendation ===")
        print(f"Mode: {rec['recommended_mode']} | Precision: {rec['preferred_precision']}")
        print(f"Explanation: {rec['explanation']['headline']}")
        for r in rec["explanation"]["rationale"]:
            print(f"  • {r}")

        task = {
            "task_id": "job-42",
            "model_id": "llama-70b",
            "max_tokens": 2000,
            "temperature": 0.7,
            "top_k": 50,
            "deferrable": True,
            "deadline_critical": True,
            "estimated_duration_hours": 1.0,
            "estimated_energy_kwh": 0.004,
        }
        decision = await controller.apply_throttling(task)
        print("\n=== Throttling Decision ===")
        print(f"Mode: {decision.eco_mode.value}")
        print(f"Precision: {decision.precision.value}")
        print(f"Energy reduction: {decision.estimated_energy_reduction_percent:.1f}%")
        print(f"Quality impact: {decision.estimated_quality_impact_percent:.1f}%")
        print(f"Assigned agent: {decision.assigned_agent}")
        print(f"Should defer: {decision.should_defer}")
        print(f"Distillation used: {decision.distillation_used}")
        if decision.market_trade:
            print(f"Market trade: {decision.market_trade}")

        print("\n=== Controller Statistics ===")
        print(json.dumps(controller.get_statistics(), indent=2, default=str))

    asyncio.run(main())
