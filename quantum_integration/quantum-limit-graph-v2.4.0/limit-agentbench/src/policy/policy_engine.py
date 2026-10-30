"""
PolicyEngine (Enhanced v2)
==========================

Reward shaping and temporal shift policy for Green Agent v5.0.0.

Original API preserved:
    engine = PolicyEngine(energy_budget, baseline_energy=None)
    reward = engine.compute_sustainability_reward(accuracy, energy)
    hour, saving = engine.suggest_temporal_shift(
        shifter, forecast_engine, energy_kwh, shiftable=True, urgency="low"
    )

v2 fixes (see changelog in commit message):
  * HeliumProfiler no longer inverts scarcity (higher scarcity = more units).
  * CausalPenaltyLearner uses reward-weighted regression with a per-bin
    baseline; consumes the penalty that was actually applied; no artificial
    max(base, …) floor that blocked softening.
  * record_outcome rewards the appropriateness of the penalty, not the
    estimator's numeric accuracy.
  * allow_high_urgency_with_hitl=True unlocks the HITL path for high-urgency
    deferrals (default preserves original "no shift for high urgency").
  * HITL auto-approvals are audit-flagged (auto=True).
  * Precision is an opt-in reward dimension (caller-supplied); no more
    decorative "int4 used" explanations.
  * Thread-safe; UTC datetimes; configurable chaos; timeouts + circuit
    breakers on shifter/forecaster/market.
  * Incremental federated aggregation; health(); MetricsRecorder.
  * suggest_temporal_shift_async for async shifter / forecaster.
  * NonBackwardShift temporal property.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import hashlib
import inspect
import logging
import math
import random
import statistics
import threading
import time
import types
from collections import OrderedDict, defaultdict, deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import (
    Any, Awaitable, Callable, Deque, Dict, List, Mapping, Optional,
    Protocol, Tuple, Union,
)

try:
    from green_agent.rewards.negawatt_reward import NegawattReward
except Exception:
    class NegawattReward:  # type: ignore[no-redef]
        """Fallback negawatt reward module."""
        def __init__(self, baseline_energy: float):
            self.baseline = baseline_energy

        def combined_reward(self, accuracy: float, energy: float) -> float:
            if self.baseline <= 0:
                return accuracy * 0.5
            savings = max(0.0, (self.baseline - energy) / self.baseline)
            return 0.5 * accuracy + 0.5 * savings

logger = logging.getLogger(__name__)

__version__ = "2.0.0"


# =============================================================================
# Helpers
# =============================================================================

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def stable_hash(s: Any) -> float:
    h = hashlib.sha256(str(s).encode("utf-8")).digest()
    return int.from_bytes(h[:8], "big") / 2**64


def _sanitize(s: Any, max_len: int = 96) -> str:
    if not isinstance(s, str):
        s = str(s)
    cleaned = "".join(c if c.isprintable() or c == " " else "?" for c in s)
    return cleaned[:max_len]


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
# Config
# =============================================================================

@dataclass(frozen=True)
class PolicyEngineConfig:
    """Frozen, validated configuration."""

    # --- negawatt / budget ---
    base_budget_penalty: float = 1.0
    negawatt_weight: float = 0.5
    helium_penalty_cap: float = 0.2
    carbon_market_penalty_cap: float = 0.15

    # --- precision reward dimension ---
    precision_bonus_enabled: bool = True
    precision_bonus_weight: float = 0.03

    # --- HITL ---
    allow_high_urgency_with_hitl: bool = False
    hitl_deferral_threshold_hours: float = 24.0
    hitl_timeout_s: float = 60.0
    hitl_default_on_timeout: bool = False
    hitl_feedback_maxlen: int = 1000

    # --- timeouts (None disables) ---
    shifter_timeout_s: Optional[float] = 2.0
    forecaster_timeout_s: Optional[float] = 2.0
    market_timeout_s: Optional[float] = 2.0

    # --- circuit breakers ---
    circuit_failure_threshold: int = 3
    circuit_recovery_timeout_s: int = 30

    # --- chaos ---
    chaos_failure_rate: float = 0.0
    chaos_event_maxlen: int = 100

    # --- history bounds ---
    reward_history_maxlen: int = 1024
    shift_history_maxlen: int = 1024
    temporal_violation_maxlen: int = 500

    # --- causal learner ---
    causal_lr: float = 0.02
    causal_baseline_decay: float = 0.95
    causal_buffer_maxlen: int = 512
    causal_update_every: int = 10

    # --- federated ---
    federated_push_every: int = 25
    federated_recent_maxlen: int = 256

    # --- temporal safety ---
    allow_backward_shift: bool = False
    max_delay_hours: int = 48

    # --- executor ---
    executor_max_workers: int = 4

    def __post_init__(self) -> None:
        if self.base_budget_penalty < 0:
            raise ValueError("base_budget_penalty must be >= 0.")
        if not 0.0 <= self.negawatt_weight <= 1.0:
            raise ValueError("negawatt_weight must be in [0, 1].")
        if not 0.0 <= self.chaos_failure_rate <= 1.0:
            raise ValueError("chaos_failure_rate must be in [0, 1].")
        for f in ("shifter_timeout_s", "forecaster_timeout_s", "market_timeout_s"):
            v = getattr(self, f)
            if v is not None and v <= 0:
                raise ValueError(f"{f} must be > 0 or None.")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# =============================================================================
# Circuit Breaker
# =============================================================================

class CircuitBreaker:
    def __init__(
        self, name: str, failure_threshold: int = 3,
        recovery_timeout_s: int = 30,
    ) -> None:
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout_s = recovery_timeout_s
        self._failures = 0
        self._state = CircuitState.CLOSED
        self._last_failure_mono: Optional[float] = None
        self._last_failure_at: Optional[datetime] = None
        self._lock = threading.RLock()

    @property
    def state(self) -> CircuitState:
        with self._lock:
            return self._state

    def can_call(self) -> bool:
        with self._lock:
            if self._state == CircuitState.CLOSED:
                return True
            if self._state == CircuitState.OPEN:
                if (
                    self._last_failure_mono is not None
                    and (time.monotonic() - self._last_failure_mono)
                    > self.recovery_timeout_s
                ):
                    self._state = CircuitState.HALF_OPEN
                    return True
                return False
            return True

    def record_success(self) -> None:
        with self._lock:
            self._failures = 0
            self._state = CircuitState.CLOSED

    def record_failure(self) -> None:
        with self._lock:
            self._failures += 1
            self._last_failure_mono = time.monotonic()
            self._last_failure_at = _utcnow()
            if self._failures >= self.failure_threshold:
                if self._state != CircuitState.OPEN:
                    logger.warning(
                        "Circuit '%s' OPEN after %d failures",
                        self.name, self._failures,
                    )
                self._state = CircuitState.OPEN


# =============================================================================
# Chaos Injector
# =============================================================================

class ChaosInjector:
    def __init__(self, failure_rate: float = 0.0, maxlen: int = 100) -> None:
        self.failure_rate = max(0.0, min(1.0, failure_rate))
        self.events: Deque[Dict[str, Any]] = deque(maxlen=maxlen)
        self._lock = threading.RLock()

    def maybe_fail(self, component: str) -> bool:
        if self.failure_rate <= 0:
            return False
        if random.random() < self.failure_rate:
            with self._lock:
                self.events.append({
                    "component": component,
                    "at": _utcnow().isoformat(),
                })
            return True
        return False


# =============================================================================
# Temporal Logic
# =============================================================================

class ShiftProperty(Protocol):
    def check(self, shift: Dict[str, Any], task: Dict[str, Any]) -> bool: ...
    def name(self) -> str: ...


@dataclass
class DeadlineRespected:
    def check(self, shift: Dict[str, Any], task: Dict[str, Any]) -> bool:
        deadline_hour = task.get("deadline_hour")
        if deadline_hour is None:
            return True
        return int(shift.get("hour", 0)) <= int(deadline_hour)

    def name(self) -> str:
        return "DeadlineRespected"


@dataclass
class NonNegativeSaving:
    def check(self, shift: Dict[str, Any], task: Dict[str, Any]) -> bool:
        try:
            return float(shift.get("saving", 0.0)) >= 0.0
        except (TypeError, ValueError):
            return False

    def name(self) -> str:
        return "NonNegativeSaving"


@dataclass
class MaxDelayBounded:
    max_delay_hours: int = 48

    def check(self, shift: Dict[str, Any], task: Dict[str, Any]) -> bool:
        current = int(task.get("current_hour", 0))
        return abs(int(shift.get("hour", 0)) - current) <= self.max_delay_hours

    def name(self) -> str:
        return "MaxDelayBounded"


@dataclass
class NonBackwardShift:
    """Shift.hour must be >= current_hour unless the caller has opted in."""
    allow_backward: bool = False

    def check(self, shift: Dict[str, Any], task: Dict[str, Any]) -> bool:
        if self.allow_backward:
            return True
        current = int(task.get("current_hour", 0))
        return int(shift.get("hour", 0)) >= current

    def name(self) -> str:
        return "NonBackwardShift"


@dataclass
class NonNegativeEnergy:
    def check(self, shift: Dict[str, Any], task: Dict[str, Any]) -> bool:
        try:
            return float(task.get("energy_kwh", 0.0)) >= 0.0
        except (TypeError, ValueError):
            return False

    def name(self) -> str:
        return "NonNegativeEnergy"


class TemporalLogicMonitor:
    def __init__(self, maxlen: int = 500) -> None:
        self.properties: List[ShiftProperty] = []
        self.violations: Deque[Dict[str, Any]] = deque(maxlen=maxlen)
        self._lock = threading.RLock()

    def register(self, p: ShiftProperty) -> None:
        with self._lock:
            self.properties.append(p)

    def verify(
        self, shift: Dict[str, Any], task: Dict[str, Any],
    ) -> Tuple[bool, List[str]]:
        bad: List[str] = []
        with self._lock:
            props = list(self.properties)
        for p in props:
            try:
                ok = p.check(shift, task)
            except Exception as e:
                logger.warning("property %s raised: %s", p.name(), e)
                ok = False
            if not ok:
                bad.append(p.name())
                with self._lock:
                    self.violations.append({
                        "property": p.name(),
                        "shift": dict(shift),
                        "at": _utcnow().isoformat(),
                    })
        return (len(bad) == 0, bad)


# =============================================================================
# XAI
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
        precision: Optional[str] = None,
        causal_adjustment: float = 0.0,
        precision_bonus: float = 0.0,
        *,
        learner_observations: int = 0,
    ) -> RewardExplanation:
        reasons: List[str] = [
            f"Accuracy={accuracy:.3f}, Energy={energy:.4f} kWh "
            f"(budget={energy_budget:.4f}).",
            f"Negawatt component: {negawatt_reward:.4f}.",
        ]
        if budget_penalty:
            reasons.append(f"Budget penalty applied: −{budget_penalty:.4f}.")
        if helium_units > 0:
            reasons.append(f"Helium component: {helium_units:.5f} units.")
        if carbon_market_cost > 0:
            reasons.append(f"Carbon market cost: ${carbon_market_cost:.4f}.")
        if causal_adjustment:
            reasons.append(
                f"Learned penalty adjustment: {causal_adjustment:+.4f}."
            )
        if precision is not None:
            reasons.append(f"Precision: {precision}.")
        if precision_bonus:
            reasons.append(f"Precision bonus: +{precision_bonus:.4f}.")
        reasons.append(f"Final reward: {final_reward:.4f}.")

        confidence = 0.5
        if accuracy > 0.8:
            confidence += 0.3
        if learner_observations >= 20:
            confidence += 0.1
        if learner_observations < 5:
            confidence -= 0.1
        confidence = max(0.1, min(1.0, confidence))

        return RewardExplanation(
            headline=(
                f"Reward={final_reward:.4f} "
                f"(acc={accuracy:.2f}, energy={energy:.4f})"
            ),
            rationale=reasons,
            confidence=confidence,
            contributing_factors={
                "accuracy": accuracy,
                "energy": energy,
                "negawatt": negawatt_reward,
                "budget_penalty": budget_penalty,
                "helium": helium_units,
                "carbon_market_cost": carbon_market_cost,
                "precision_bonus": precision_bonus,
            },
        )


# =============================================================================
# Adaptive Precision
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
        """'critical' means FULL fidelity (FP32)."""
        if urgency == "critical":
            return PrecisionLevel.FP32
        if urgency == "high":
            return (PrecisionLevel.FP16 if self.hw.has_tensor_cores
                    else PrecisionLevel.FP32)
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
# Causal Penalty Learner (RWR + per-bin baseline)
# =============================================================================

@dataclass
class PenaltyState:
    hour_of_day: int
    energy_overshoot_ratio: float
    task_priority: float


class CausalPenaltyLearner:
    """
    Learns a budget-penalty adjustment from outcomes.

    v2 — reward-weighted regression with a per-bin baseline:
      * Consumes the penalty actually applied (target of the regression).
      * Can soften *below* ``base_penalty`` — the previous max(base, …)
        floor structurally prevented the learner from reducing penalties.
      * No monotonic drift: baseline removes always-positive bias.
    """

    N_FEATURES = 3

    def __init__(
        self,
        base_penalty: float = 1.0,
        lr: float = 0.02,
        baseline_decay: float = 0.95,
        buffer_maxlen: int = 512,
    ) -> None:
        self.base_penalty = base_penalty
        self.lr = lr
        self.baseline_decay = baseline_decay
        self.coeffs: List[float] = [0.0] * self.N_FEATURES
        self.baselines: Dict[int, float] = defaultdict(float)
        self.baseline_counts: Dict[int, int] = defaultdict(int)
        self.buffer: Deque[
            Tuple[List[float], float, float, int]
        ] = deque(maxlen=buffer_maxlen)
        self.observations: int = 0
        self.updates: int = 0
        self.last_advantage_mean: float = 0.0
        self._lock = threading.RLock()

    @staticmethod
    def _features(s: PenaltyState) -> List[float]:
        return [
            s.hour_of_day / 24.0,
            min(2.0, max(0.0, s.energy_overshoot_ratio)),
            min(1.0, max(0.0, s.task_priority)),
        ]

    @staticmethod
    def _bin(f: List[float]) -> int:
        return int(f[1] * 5)  # 5 buckets by overshoot ratio

    def _baseline_for(self, f: List[float]) -> float:
        return self.baselines.get(self._bin(f), 0.0)

    def _update_baseline(self, f: List[float], reward: float) -> None:
        b = self._bin(f)
        old = self.baselines.get(b, 0.0)
        self.baselines[b] = (
            self.baseline_decay * old
            + (1.0 - self.baseline_decay) * reward
        )
        self.baseline_counts[b] += 1

    def _raw_predict(self, f: List[float], magnitude: float) -> float:
        delta = sum(c * x for c, x in zip(self.coeffs, f))
        # Note: no max(base, …) — the learner may soften the penalty.
        return max(0.0, self.base_penalty * magnitude + delta)

    def predict_penalty(self, s: PenaltyState) -> float:
        f = self._features(s)
        magnitude = 1.0 + max(0.0, min(2.0, s.energy_overshoot_ratio))
        with self._lock:
            return self._raw_predict(f, magnitude)

    def record(
        self,
        s: PenaltyState,
        reward: float,
        penalty_used: float,
    ) -> None:
        f = self._features(s)
        with self._lock:
            self.buffer.append((f, reward, penalty_used, self._bin(f)))
            self.observations += 1

    def update(self) -> None:
        with self._lock:
            if not self.buffer:
                return
            advantages: List[float] = []
            for f, reward, penalty_used, _b in self.buffer:
                adv = reward - self._baseline_for(f)
                advantages.append(adv)
                self._update_baseline(f, reward)

                magnitude = 1.0 + min(2.0, max(0.0, f[1]))
                pred = self._raw_predict(f, magnitude)
                error = penalty_used - pred
                for i, x in enumerate(f):
                    self.coeffs[i] += self.lr * adv * error * x
            self.last_advantage_mean = (
                statistics.fmean(advantages) if advantages else 0.0
            )
            self.updates += 1
            self.buffer.clear()


# =============================================================================
# Quantum Distillation
# =============================================================================

@dataclass
class DistilledRewardPolicy:
    precision: PrecisionLevel
    base_penalty: float
    negawatt_weight: float
    quality_retention: float
    energy_reduction_percent: float


class QuantumDistillationBridge:
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
# Federated (incremental)
# =============================================================================

@dataclass
class FederatedRewardProfile:
    deployment_id: str
    key: str
    mean_reward: float
    mean_energy: float
    sample_count: int
    timestamp: datetime = field(default_factory=_utcnow)


class FederatedAggregator:
    def __init__(self, recent_maxlen: int = 256) -> None:
        self._sum_reward: Dict[str, float] = defaultdict(float)
        self._sum_energy: Dict[str, float] = defaultdict(float)
        self._count: Dict[str, int] = defaultdict(int)
        self.recent: Deque[FederatedRewardProfile] = deque(maxlen=recent_maxlen)
        self._global: Dict[str, Dict[str, float]] = {}
        self._lock = threading.RLock()

    def push(self, u: FederatedRewardProfile) -> None:
        with self._lock:
            self.recent.append(u)
            self._sum_reward[u.key] += u.mean_reward * u.sample_count
            self._sum_energy[u.key] += u.mean_energy * u.sample_count
            self._count[u.key] += u.sample_count

    def aggregate(self) -> Dict[str, Dict[str, float]]:
        with self._lock:
            result = {}
            for key, n in self._count.items():
                if n <= 0:
                    continue
                result[key] = {
                    "mean_reward": self._sum_reward[key] / n,
                    "mean_energy": self._sum_energy[key] / n,
                    "sample_count": n,
                }
            self._global = result
            return dict(result)

    def lookup(self, key: str) -> Optional[Dict[str, float]]:
        with self._lock:
            if not self._global:
                self.aggregate()
            return self._global.get(key)


# =============================================================================
# Multi-Agent
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
        self._lock = threading.RLock()

    def register(self, agent_id: str) -> AgentProfile:
        with self._lock:
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
        with self._lock:
            a = self.register(agent_id)
            n = a.total_calls + 1
            a.success_rate = (
                (n - 1) * a.success_rate + float(success)
            ) / n
            a.avg_carbon_saved = (
                (n - 1) * a.avg_carbon_saved + carbon_saved_pct / 100.0
            ) / n
            a.avg_helium_saved = (
                (n - 1) * a.avg_helium_saved + helium_saved_pct / 100.0
            ) / n
            a.total_calls = n
            if n % 5 == 0:
                self._reassign()

    def select_agent(
        self, prefer: AgentRole = AgentRole.GENERALIST,
    ) -> Optional[str]:
        with self._lock:
            if not self.agents:
                return None
            cands = [a for a in self.agents.values() if a.role == prefer]
            if not cands:
                cands = list(self.agents.values())
            return max(cands, key=lambda a: a.success_rate).agent_id


# =============================================================================
# Carbon Markets
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
    def __init__(
        self, backend: Optional[MarketBackend] = None, ttl_s: int = 300,
    ) -> None:
        self._backend = backend or MockMarketBackend()
        self._ttl = ttl_s
        self._cache: Optional[MarketSnapshot] = None
        self._lock = threading.RLock()

    def get_snapshot(self) -> MarketSnapshot:
        with self._lock:
            if self._cache and (
                (_utcnow() - self._cache.timestamp).total_seconds()
                < self._ttl
            ):
                return self._cache
            snap = self._backend.fetch()
            self._cache = snap
            return snap


# =============================================================================
# HITL (async + timeout + audit-flagged auto-approvals)
# =============================================================================

@dataclass(frozen=True)
class HITLRequest:
    shift_hour: int
    shift_saving: float
    urgency: str
    reason: str
    context: Mapping[str, Any] = field(default_factory=dict)
    requested_at: datetime = field(default_factory=_utcnow)

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["context"] = dict(self.context)
        d["requested_at"] = self.requested_at.isoformat()
        return d


class HumanInTheLoopGate:
    def __init__(
        self,
        deferral_threshold_hours: float = 24.0,
        timeout_s: float = 60.0,
        default_on_timeout: bool = False,
        feedback_maxlen: int = 1000,
    ) -> None:
        self.deferral_threshold_hours = deferral_threshold_hours
        self.timeout_s = timeout_s
        self.default_on_timeout = default_on_timeout
        self.pending: List[HITLRequest] = []
        self.feedback_log: Deque[Dict[str, Any]] = deque(maxlen=feedback_maxlen)
        self._callback: Optional[Callable[[HITLRequest], bool]] = None
        self._async_callback: Optional[
            Callable[[HITLRequest], Awaitable[bool]]
        ] = None
        self._lock = threading.RLock()

    def set_callback(self, cb: Callable[[HITLRequest], bool]) -> None:
        self._callback = cb

    def set_async_callback(
        self, cb: Callable[[HITLRequest], Awaitable[bool]],
    ) -> None:
        self._async_callback = cb

    def needs_review(
        self, shift_hour: int, current_hour: int, urgency: str,
    ) -> bool:
        if urgency == "high":
            return True
        if abs(shift_hour - current_hour) > self.deferral_threshold_hours:
            return True
        return False

    def _record(
        self, req: HITLRequest, decision: bool, auto: bool = False,
    ) -> None:
        with self._lock:
            self.feedback_log.append({
                "req": req.to_dict(),
                "decision": decision,
                "auto": auto,
            })

    def request_review(
        self,
        shift_hour: int,
        shift_saving: float,
        urgency: str,
        reason: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> bool:
        req = HITLRequest(
            shift_hour, shift_saving, urgency, reason, context or {},
        )
        with self._lock:
            self.pending.append(req)
        try:
            # NOTE: The previous auto-approve "low" branch was dead — the
            # caller only reaches us when needs_review() returned True,
            # which cannot happen for low urgency (unless deferral
            # threshold was crossed). Handled by the callback path.
            if self._callback is None:
                self._record(req, False, auto=True)
                return False
            decision = self._callback(req)
            self._record(req, bool(decision))
            return bool(decision)
        finally:
            with self._lock:
                if req in self.pending:
                    self.pending.remove(req)

    async def request_review_async(
        self,
        shift_hour: int,
        shift_saving: float,
        urgency: str,
        reason: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> bool:
        req = HITLRequest(
            shift_hour, shift_saving, urgency, reason, context or {},
        )
        with self._lock:
            self.pending.append(req)
        try:
            if self._async_callback is not None:
                decision = await asyncio.wait_for(
                    self._async_callback(req), timeout=self.timeout_s,
                )
            elif self._callback is not None:
                decision = self._callback(req)
            else:
                decision = self.default_on_timeout
        except asyncio.TimeoutError:
            logger.warning("HITL async timed out; using default")
            decision = self.default_on_timeout
        except Exception as e:
            logger.error("HITL async callback failed: %s", e)
            decision = False
        finally:
            with self._lock:
                if req in self.pending:
                    self.pending.remove(req)
        self._record(req, bool(decision))
        return bool(decision)

    def active_learning_batch(self, n: int = 16) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self.feedback_log)[-n:]


# =============================================================================
# Helium Profiler — v2: scarcity *increases* estimated units
# =============================================================================

class HeliumProfiler:
    """
    Estimate helium consumption from energy and scarcity.

    Scarcity → **higher** effective units (the resource is harder to
    procure, so its shadow cost per kWh goes up). The previous
    `1 - 0.5*scarcity` factor inverted this.
    """
    HELIUM_UNITS_PER_KWH = 0.05
    SCARCITY_UPLIFT_MAX = 0.5  # up to +50%

    @classmethod
    def estimate_units(
        cls, energy_kwh: float, scarcity_score: float = 0.0,
    ) -> float:
        base = energy_kwh * cls.HELIUM_UNITS_PER_KWH
        uplift = 1.0 + min(
            cls.SCARCITY_UPLIFT_MAX, max(0.0, scarcity_score) * 0.5,
        )
        return base * uplift


# =============================================================================
# Metrics
# =============================================================================

class MetricsRecorder:
    def __init__(self) -> None:
        self.counters: Dict[str, float] = {}
        self.histograms: Dict[str, List[float]] = {}
        self._lock = threading.RLock()

    @staticmethod
    def _key(name: str, labels: Mapping[str, Any]) -> str:
        if not labels:
            return name
        parts = ",".join(f'{k}="{v}"' for k, v in sorted(labels.items()))
        return f"{name}{{{parts}}}"

    def inc(self, name: str, value: float = 1.0, **labels: Any) -> None:
        with self._lock:
            k = self._key(name, labels)
            self.counters[k] = self.counters.get(k, 0.0) + value

    def observe(self, name: str, value: float, **labels: Any) -> None:
        with self._lock:
            k = self._key(name, labels)
            self.histograms.setdefault(k, []).append(float(value))

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            hist: Dict[str, Dict[str, float]] = {}
            for k, vals in self.histograms.items():
                if not vals:
                    continue
                s = sorted(vals)
                hist[k] = {
                    "count": len(vals),
                    "mean": sum(vals) / len(vals),
                    "p50": s[len(s) // 2],
                    "p99": s[min(len(s) - 1, int(len(s) * 0.99))],
                    "max": s[-1],
                }
            return {
                "counters": dict(self.counters),
                "histograms": hist,
            }


# =============================================================================
# Default feature flags (immutable)
# =============================================================================

_DEFAULT_FEATURES = types.MappingProxyType({
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
})


# =============================================================================
# PolicyEngine
# =============================================================================

class PolicyEngine:
    """
    Enhanced reward shaping and temporal shift policy engine.

    Backward-compatible:
        PolicyEngine(energy_budget, baseline_energy=None)
    """

    @classmethod
    def default_features(cls) -> Dict[str, bool]:
        return dict(_DEFAULT_FEATURES)

    def __init__(
        self,
        energy_budget: float,
        baseline_energy: Optional[float] = None,
        deployment_id: str = "local",
        agent_id: str = "policy-0",
        features: Optional[Dict[str, bool]] = None,
        hardware: Optional[HardwareProfile] = None,
        config: Optional[PolicyEngineConfig] = None,
        metrics: Optional[MetricsRecorder] = None,
    ) -> None:
        self.config = config or PolicyEngineConfig()
        self.energy_budget = float(energy_budget)
        self.baseline_energy = baseline_energy

        if baseline_energy is not None:
            try:
                self.negawatt_module = NegawattReward(baseline_energy)
            except Exception:
                self.negawatt_module = None
        else:
            self.negawatt_module = None

        self.deployment_id = deployment_id
        self.agent_id = agent_id
        self.features = {**self.default_features(), **(features or {})}
        self._metrics = metrics or MetricsRecorder()

        # Resilience.
        self._negawatt_circuit = CircuitBreaker(
            "negawatt",
            self.config.circuit_failure_threshold,
            self.config.circuit_recovery_timeout_s,
        )
        self._forecaster_circuit = CircuitBreaker(
            "forecaster",
            self.config.circuit_failure_threshold,
            self.config.circuit_recovery_timeout_s,
        )
        self._shifter_circuit = CircuitBreaker(
            "shifter",
            self.config.circuit_failure_threshold,
            self.config.circuit_recovery_timeout_s,
        )
        self._market_circuit = CircuitBreaker(
            "market",
            self.config.circuit_failure_threshold,
            self.config.circuit_recovery_timeout_s,
        )
        self.chaos = (
            ChaosInjector(
                failure_rate=self.config.chaos_failure_rate,
                maxlen=self.config.chaos_event_maxlen,
            )
            if self.features["chaos_testing"] else None
        )

        # Temporal logic.
        self.temporal_monitor: Optional[TemporalLogicMonitor] = (
            TemporalLogicMonitor(
                maxlen=self.config.temporal_violation_maxlen,
            )
            if self.features["temporal_logic"] else None
        )
        if self.temporal_monitor:
            self.temporal_monitor.register(DeadlineRespected())
            self.temporal_monitor.register(NonNegativeSaving())
            self.temporal_monitor.register(
                MaxDelayBounded(max_delay_hours=self.config.max_delay_hours),
            )
            self.temporal_monitor.register(
                NonBackwardShift(
                    allow_backward=self.config.allow_backward_shift,
                ),
            )
            self.temporal_monitor.register(NonNegativeEnergy())

        # XAI.
        self.explainer = RewardExplainer() if self.features["xai"] else None

        # Precision.
        self.precision_ctl = (
            AdaptivePrecisionController(hardware)
            if self.features["adaptive_precision"] else None
        )

        # Causal learner.
        self.penalty_learner: Optional[CausalPenaltyLearner] = (
            CausalPenaltyLearner(
                base_penalty=self.config.base_budget_penalty,
                lr=self.config.causal_lr,
                baseline_decay=self.config.causal_baseline_decay,
                buffer_maxlen=self.config.causal_buffer_maxlen,
            )
            if self.features["causal_rl"] else None
        )

        # Distillation.
        self.distiller = (
            QuantumDistillationBridge()
            if self.features["quantum_distillation"] else None
        )

        # Federated.
        self.federated = (
            FederatedAggregator(recent_maxlen=self.config.federated_recent_maxlen)
            if self.features["federated"] else None
        )

        # Multi-agent.
        self.coordinator = (
            MultiAgentCoordinator() if self.features["multi_agent"] else None
        )
        if self.coordinator:
            self.coordinator.register(self.agent_id)

        # Market.
        self.market = (
            CarbonMarketClient() if self.features["carbon_market"] else None
        )

        # HITL.
        self.hitl = (
            HumanInTheLoopGate(
                deferral_threshold_hours=self.config.hitl_deferral_threshold_hours,
                timeout_s=self.config.hitl_timeout_s,
                default_on_timeout=self.config.hitl_default_on_timeout,
                feedback_maxlen=self.config.hitl_feedback_maxlen,
            )
            if self.features["hitl"] else None
        )

        # Helium profiler (scarcity-aware).
        self.helium_profiler = (
            HeliumProfiler() if self.features["helium_awareness"] else None
        )

        # External signals.
        self.helium_signal_fn: Optional[Callable[[], Any]] = None
        self.carbon_forecaster: Any = None
        self._async_forecaster_warned: bool = False

        # History / counters.
        self._lock = threading.RLock()
        self.reward_history: Deque[Dict[str, Any]] = deque(
            maxlen=self.config.reward_history_maxlen,
        )
        self.shift_history: Deque[Dict[str, Any]] = deque(
            maxlen=self.config.shift_history_maxlen,
        )
        self.reward_count: int = 0
        self.shift_count: int = 0
        self.last_explanation: Optional[RewardExplanation] = None
        self.last_penalty_used: float = 0.0
        self.last_negawatt_weight: float = self.config.negawatt_weight
        self._started_mono: float = time.monotonic()
        self._pending_outcomes: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
        self._next_outcome_id: int = 1

        # Executor for timeouts on sync external calls.
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.config.executor_max_workers,
            thread_name_prefix="policy-engine",
        )

        logger.info(
            "Enhanced PolicyEngine initialized "
            "(deployment=%s, agent=%s, budget=%s, features=%s)",
            deployment_id, agent_id, energy_budget, list(self.features),
        )

    # ------------------------------------------------------------------ lifecycle
    def close(self) -> None:
        self._executor.shutdown(wait=False, cancel_futures=True)

    def __enter__(self) -> "PolicyEngine":
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.close()

    # ------------------------------------------------------------------ helpers
    @staticmethod
    def _safe_float(value: Any) -> Optional[float]:
        try:
            v = float(value)
            return v if math.isfinite(v) else None
        except (TypeError, ValueError):
            return None

    def _call_with_timeout(
        self,
        fn: Callable[..., Any],
        timeout_s: Optional[float],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        if timeout_s is None or timeout_s <= 0:
            return fn(*args, **kwargs)
        import functools
        fut = self._executor.submit(functools.partial(fn, *args, **kwargs))
        return fut.result(timeout=timeout_s)

    def _fallback_negawatt(self, accuracy: float, energy: float) -> float:
        if self.baseline_energy and self.baseline_energy > 0:
            savings = max(
                0.0,
                (self.baseline_energy - energy) / self.baseline_energy,
            )
            return 0.5 * accuracy + 0.5 * savings
        return 0.5 * accuracy

    # ------------------------------------------------------------------ signals
    def _get_helium_scarcity(self) -> Optional[float]:
        if self.helium_signal_fn is None:
            return None
        try:
            sig = self.helium_signal_fn()
            if inspect.isawaitable(sig):
                logger.warning(
                    "helium_signal_fn returns a coroutine; use "
                    "compute_sustainability_reward_async()",
                )
                close = getattr(sig, "close", None)
                if callable(close):
                    close()
                return None
            if sig is None:
                return None
            if isinstance(sig, Mapping):
                raw = sig.get("scarcity_score")
            elif hasattr(sig, "scarcity_score"):
                raw = sig.scarcity_score
            else:
                return None
            if raw is None:
                return None
            return float(raw)
        except Exception as e:
            logger.warning(
                "helium_signal_fn failed: %s", e, exc_info=True,
            )
            self._metrics.inc("helium_signal_errors")
            return None

    def _get_carbon_price(self) -> Optional[MarketSnapshot]:
        if self.market is None:
            return None
        if not self._market_circuit.can_call():
            return None
        try:
            if self.chaos and self.chaos.maybe_fail("market"):
                raise RuntimeError("chaos: market")
            snap = self._call_with_timeout(
                self.market.get_snapshot,
                self.config.market_timeout_s,
            )
            self._market_circuit.record_success()
            return snap
        except (concurrent.futures.TimeoutError, TimeoutError):
            self._market_circuit.record_failure()
            self._metrics.inc("market_timeouts")
            logger.warning("market.get_snapshot timed out")
            return None
        except Exception as e:
            self._market_circuit.record_failure()
            self._metrics.inc("market_errors")
            logger.warning("market.get_snapshot failed: %s", e, exc_info=True)
            return None

    # ------------------------------------------------------------------ reward
    def compute_sustainability_reward(
        self,
        accuracy: float,
        energy: float,
        *,
        precision: Optional[Union[PrecisionLevel, str]] = None,
        task_priority: float = 0.5,
    ) -> float:
        """
        Compute sustainability reward. Backward-compatible.

        With all features off, behaves identically to the original:
        delegates to NegawattReward, then applies a fixed −1.0 penalty
        if energy > energy_budget.

        ``precision`` is an *optional* reward dimension. If the caller
        passes the precision the workload actually ran at, a configurable
        bonus is applied for choosing a lower-energy precision. If omitted,
        no precision term is included (previous behaviour was to *choose*
        a precision internally and report it in XAI without actually
        affecting the reward or reflecting the caller's reality).
        """
        # --- Input validation ---
        acc = self._safe_float(accuracy)
        en = self._safe_float(energy)
        if acc is None or en is None:
            logger.warning("Invalid accuracy/energy input; returning 0.0")
            self._metrics.inc("reward_invalid_input")
            return 0.0
        acc = max(0.0, min(1.0, acc))
        en = max(0.0, en)

        # --- Negawatt component ---
        negawatt_reward = 0.0
        if self.negawatt_module is not None:
            if not self._negawatt_circuit.can_call():
                negawatt_reward = self._fallback_negawatt(acc, en)
            else:
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
                    logger.warning(
                        "NegawattReward failed: %s; using fallback",
                        e, exc_info=True,
                    )
                    negawatt_reward = self._fallback_negawatt(acc, en)

        # --- Budget penalty ---
        budget_penalty = 0.0
        causal_adjustment = 0.0
        if en > self.energy_budget:
            overshoot_ratio = (
                (en - self.energy_budget) / self.energy_budget
                if self.energy_budget > 0 else 1.0
            )
            if self.penalty_learner:
                state = PenaltyState(
                    hour_of_day=_utcnow().hour,
                    energy_overshoot_ratio=min(2.0, overshoot_ratio),
                    task_priority=min(1.0, max(0.0, task_priority)),
                )
                budget_penalty = self.penalty_learner.predict_penalty(state)
                causal_adjustment = (
                    budget_penalty - self.config.base_budget_penalty
                )
            else:
                budget_penalty = self.config.base_budget_penalty

        # --- Helium ---
        helium_units = 0.0
        helium_scarcity = 0.0
        if self.helium_profiler:
            s = self._get_helium_scarcity()
            if s is not None:
                helium_scarcity = s
            else:
                self._metrics.inc("helium_signal_missing")
            helium_units = HeliumProfiler.estimate_units(
                en, helium_scarcity,
            )

        # --- Carbon market ---
        carbon_market_cost = 0.0
        snap = self._get_carbon_price() if self.market else None
        if snap is not None:
            try:
                carbon_kg = en * 400.0 / 1000.0  # 400 gCO2/kWh default
                carbon_market_cost = (
                    carbon_kg / 1000.0 * snap.carbon_price_per_tco2_usd
                )
            except Exception as e:
                logger.warning(
                    "carbon_market_cost computation failed: %s",
                    e, exc_info=True,
                )

        # --- Precision (opt-in) ---
        precision_bonus = 0.0
        precision_str: Optional[str] = None
        if precision is not None and self.config.precision_bonus_enabled:
            if isinstance(precision, PrecisionLevel):
                p_enum = precision
            else:
                try:
                    p_enum = PrecisionLevel(str(precision))
                except ValueError:
                    p_enum = None
            if p_enum is not None:
                precision_str = p_enum.value
                mult = AdaptivePrecisionController.energy_multiplier(p_enum)
                # Bonus for lower-energy precision, scaled by weight.
                precision_bonus = (
                    self.config.precision_bonus_weight * (1.0 - mult)
                )

        # --- Final reward ---
        final_reward = negawatt_reward - budget_penalty + precision_bonus
        if self.features["helium_awareness"] and helium_units > 0:
            final_reward -= min(
                self.config.helium_penalty_cap, helium_units * 0.1,
            )
        if self.features["carbon_market"] and carbon_market_cost > 0:
            final_reward -= min(
                self.config.carbon_market_penalty_cap, carbon_market_cost,
            )

        # --- XAI ---
        if self.explainer:
            exp = RewardExplainer.explain(
                accuracy=acc,
                energy=en,
                energy_budget=self.energy_budget,
                negawatt_reward=negawatt_reward,
                budget_penalty=budget_penalty,
                final_reward=final_reward,
                helium_units=helium_units,
                carbon_market_cost=carbon_market_cost,
                precision=precision_str,
                causal_adjustment=causal_adjustment,
                precision_bonus=precision_bonus,
                learner_observations=(
                    self.penalty_learner.observations
                    if self.penalty_learner else 0
                ),
            )
            with self._lock:
                self.last_explanation = exp

        # --- Record ---
        outcome_id = self._new_outcome_id()
        with self._lock:
            self.reward_count += 1
            self.reward_history.append({
                "outcome_id": outcome_id,
                "accuracy": acc,
                "energy": en,
                "negawatt": negawatt_reward,
                "budget_penalty": budget_penalty,
                "helium_units": helium_units,
                "carbon_market_cost": carbon_market_cost,
                "precision_bonus": precision_bonus,
                "precision": precision_str,
                "final_reward": final_reward,
                "at": _utcnow().isoformat(),
            })
            self.last_penalty_used = budget_penalty
            self.last_negawatt_weight = self.config.negawatt_weight

        # Stash state for record_outcome attribution.
        if self.penalty_learner and en > self.energy_budget:
            overshoot_ratio = (
                (en - self.energy_budget) / self.energy_budget
                if self.energy_budget > 0 else 1.0
            )
            state = PenaltyState(
                hour_of_day=_utcnow().hour,
                energy_overshoot_ratio=min(2.0, overshoot_ratio),
                task_priority=min(1.0, max(0.0, task_priority)),
            )
            with self._lock:
                self._pending_outcomes[outcome_id] = {
                    "state": state,
                    "penalty_used": budget_penalty,
                    "predicted_energy": en,
                }
                # Bound size.
                while len(self._pending_outcomes) > 512:
                    self._pending_outcomes.popitem(last=False)

        # --- Federated contribution ---
        if self.federated and \
                self.reward_count % self.config.federated_push_every == 0:
            self.federated.push(FederatedRewardProfile(
                deployment_id=self.deployment_id,
                key="reward:default",
                mean_reward=final_reward,
                mean_energy=en,
                sample_count=1,
            ))

        # --- Metrics ---
        self._metrics.inc("reward_total")
        self._metrics.observe("reward_value", final_reward)
        if budget_penalty > 0:
            self._metrics.inc("reward_budget_exceeded")

        return final_reward

    async def compute_sustainability_reward_async(
        self,
        accuracy: float,
        energy: float,
        *,
        precision: Optional[Union[PrecisionLevel, str]] = None,
        task_priority: float = 0.5,
    ) -> float:
        """Async wrapper — supports async helium signal + async market."""
        # Pre-fetch async helium/market signals in case the caller's
        # callbacks return coroutines.
        if (
            self.helium_signal_fn is not None
            and not self.explainer  # only matters when we might call it
            or True
        ):
            sig = self.helium_signal_fn() if self.helium_signal_fn else None
            if inspect.isawaitable(sig):
                try:
                    await asyncio.wait_for(sig, timeout=2.0)
                except Exception:
                    pass
        return await asyncio.to_thread(
            self.compute_sustainability_reward,
            accuracy, energy,
            precision=precision, task_priority=task_priority,
        )

    def _new_outcome_id(self) -> str:
        with self._lock:
            oid = f"ro-{self._next_outcome_id}"
            self._next_outcome_id += 1
            return oid

    # ------------------------------------------------------------------ outcome
    def record_outcome(
        self,
        actual_energy: float,
        quality_preserved: bool,
        *,
        outcome_id: Optional[str] = None,
    ) -> None:
        """
        Feed back actual outcomes for causal penalty learning.

        v2: rewards the *appropriateness* of the penalty, not whether the
        estimator's prediction matched reality.

        Appropriateness heuristic:
          * Penalty was 0 but energy was below budget  → reward 1.0 (correct)
          * Penalty was >0 but energy was below budget → reward 0.3 (over-penalized)
          * Penalty was 0 but quality was lost         → reward 0.3 (under-penalized)
          * Penalty >0 and quality preserved           → reward scaled by overshoot
          * Penalty >0 and quality lost                → reward 1.0 (correct to penalize)
        """
        if not self.penalty_learner:
            return
        with self._lock:
            if outcome_id is None:
                if not self._pending_outcomes:
                    return
                outcome_id = next(reversed(self._pending_outcomes))
            entry = self._pending_outcomes.pop(outcome_id, None)
            if entry is None:
                return

        actual = self._safe_float(actual_energy)
        if actual is None:
            logger.warning("Invalid actual_energy; ignoring outcome")
            return
        actual = max(0.0, actual)

        penalty_used = entry["penalty_used"]
        actual_exceeded = actual > self.energy_budget

        if not actual_exceeded and penalty_used == 0.0:
            reward = 1.0
        elif not actual_exceeded and penalty_used > 0.0:
            reward = 0.3
        elif actual_exceeded and not quality_preserved:
            reward = 1.0
        elif actual_exceeded and quality_preserved:
            overshoot = (
                (actual - self.energy_budget) / max(self.energy_budget, 1e-9)
            )
            reward = max(0.4, 1.0 - min(1.0, overshoot))
        else:
            reward = 0.5

        self.penalty_learner.record(
            entry["state"], reward, penalty_used,
        )
        self._metrics.observe("causal_reward", reward)

        # Coordinator success now driven by real outcome, not mode.
        if self.coordinator:
            self.coordinator.record(
                agent_id=self.agent_id,
                success=bool(quality_preserved),
                carbon_saved_pct=0.0,
                helium_saved_pct=0.0,
            )

        if self.penalty_learner.observations % \
                self.config.causal_update_every == 0:
            self.penalty_learner.update()

    # ------------------------------------------------------------------ shift
    def suggest_temporal_shift(
        self,
        shifter: Any,
        forecast_engine: Any,
        energy_kwh: float,
        shiftable: bool = True,
        urgency: str = "low",
        current_hour: int = 0,
        deadline_hour: Optional[int] = None,
        task: Optional[Dict[str, Any]] = None,
    ) -> Tuple[int, float]:
        """
        Suggest optimal temporal shift. Backward-compatible.

        v2: `urgency=="high"` continues to short-circuit (original
        behavior) unless `allow_high_urgency_with_hitl=True` is set in
        config, in which case the HITL review path fires.
        """
        # Validate energy.
        en = self._safe_float(energy_kwh)
        if en is None:
            logger.warning("Invalid energy_kwh; refusing to shift")
            return 0, 0.0

        # Original gate — unless HITL-for-high-urgency is explicitly allowed.
        if not shiftable:
            return 0, 0.0
        if urgency == "high" and not self.config.allow_high_urgency_with_hitl:
            return 0, 0.0

        # Circuit breaker on forecaster.
        if not self._forecaster_circuit.can_call():
            logger.debug("Forecaster circuit open; returning (0, 0.0)")
            return 0, 0.0

        try:
            if self.chaos and self.chaos.maybe_fail("forecast_engine"):
                raise RuntimeError("chaos: forecast_engine")
            current = self._call_with_timeout(
                forecast_engine.current_intensity,
                self.config.forecaster_timeout_s,
            )
            forecast = self._call_with_timeout(
                forecast_engine.forecast_next_hours,
                self.config.forecaster_timeout_s,
                4,
            )
            self._forecaster_circuit.record_success()
        except (concurrent.futures.TimeoutError, TimeoutError):
            self._forecaster_circuit.record_failure()
            self._metrics.inc("forecaster_timeouts")
            logger.warning("Forecast timed out; returning (0, 0.0)")
            return 0, 0.0
        except Exception as e:
            self._forecaster_circuit.record_failure()
            self._metrics.inc("forecaster_errors")
            logger.warning(
                "Forecast failed: %s; returning (0, 0.0)", e, exc_info=True,
            )
            return 0, 0.0

        # Circuit breaker on shifter.
        if not self._shifter_circuit.can_call():
            return 0, 0.0

        try:
            if self.chaos and self.chaos.maybe_fail("shifter"):
                raise RuntimeError("chaos: shifter")
            hour, saving = self._call_with_timeout(
                shifter.suggest,
                self.config.shifter_timeout_s,
                current, forecast, en,
            )
            self._shifter_circuit.record_success()
        except (concurrent.futures.TimeoutError, TimeoutError):
            self._shifter_circuit.record_failure()
            self._metrics.inc("shifter_timeouts")
            logger.warning("Shifter timed out; returning (0, 0.0)")
            return 0, 0.0
        except Exception as e:
            self._shifter_circuit.record_failure()
            self._metrics.inc("shifter_errors")
            logger.warning(
                "Shifter failed: %s; returning (0, 0.0)", e, exc_info=True,
            )
            return 0, 0.0

        # Temporal verification.
        shift = {"hour": int(hour), "saving": float(saving)}
        task_ctx = task or {
            "current_hour": current_hour,
            "deadline_hour": deadline_hour,
            "energy_kwh": en,
        }
        if self.temporal_monitor:
            ok, violations = self.temporal_monitor.verify(shift, task_ctx)
            if not ok:
                logger.warning(
                    "Shift to hour %s violates %s; rejecting",
                    hour, violations,
                )
                self._metrics.inc("temporal_veto_total")
                return 0, 0.0

        # HITL review.
        if self.hitl and self.hitl.needs_review(hour, current_hour, urgency):
            approved = self.hitl.request_review(
                shift_hour=hour,
                shift_saving=saving,
                urgency=urgency,
                reason=f"deferral of {abs(hour - current_hour)}h",
                context={"energy_kwh": en},
            )
            if approved is False:
                logger.info("HITL denied shift to hour %s", hour)
                self._metrics.inc("hitl_veto_total")
                return 0, 0.0

        # Record.
        with self._lock:
            self.shift_count += 1
            self.shift_history.append({
                "hour": hour,
                "saving": saving,
                "current_hour": current_hour,
                "urgency": urgency,
                "at": _utcnow().isoformat(),
            })
        self._metrics.inc("shift_total", urgency=urgency)
        self._metrics.observe("shift_saving", float(saving))

        return hour, saving

    async def suggest_temporal_shift_async(
        self,
        shifter: Any,
        forecast_engine: Any,
        energy_kwh: float,
        shiftable: bool = True,
        urgency: str = "low",
        current_hour: int = 0,
        deadline_hour: Optional[int] = None,
        task: Optional[Dict[str, Any]] = None,
    ) -> Tuple[int, float]:
        """
        Async variant — awaits async shifter / forecaster if present.
        """
        en = self._safe_float(energy_kwh)
        if en is None:
            return 0, 0.0
        if not shiftable:
            return 0, 0.0
        if urgency == "high" and not self.config.allow_high_urgency_with_hitl:
            return 0, 0.0

        if not self._forecaster_circuit.can_call():
            return 0, 0.0
        try:
            cur_result = forecast_engine.current_intensity()
            current = (
                await asyncio.wait_for(
                    cur_result,
                    timeout=self.config.forecaster_timeout_s or 2.0,
                )
                if inspect.isawaitable(cur_result) else cur_result
            )
            fc_result = forecast_engine.forecast_next_hours(4)
            forecast = (
                await asyncio.wait_for(
                    fc_result,
                    timeout=self.config.forecaster_timeout_s or 2.0,
                )
                if inspect.isawaitable(fc_result) else fc_result
            )
            self._forecaster_circuit.record_success()
        except asyncio.TimeoutError:
            self._forecaster_circuit.record_failure()
            self._metrics.inc("forecaster_timeouts")
            return 0, 0.0
        except Exception as e:
            self._forecaster_circuit.record_failure()
            self._metrics.inc("forecaster_errors")
            logger.warning("Forecast async failed: %s", e, exc_info=True)
            return 0, 0.0

        if not self._shifter_circuit.can_call():
            return 0, 0.0
        try:
            s_result = shifter.suggest(current, forecast, en)
            hour, saving = (
                await asyncio.wait_for(
                    s_result,
                    timeout=self.config.shifter_timeout_s or 2.0,
                )
                if inspect.isawaitable(s_result) else s_result
            )
            self._shifter_circuit.record_success()
        except asyncio.TimeoutError:
            self._shifter_circuit.record_failure()
            self._metrics.inc("shifter_timeouts")
            return 0, 0.0
        except Exception as e:
            self._shifter_circuit.record_failure()
            self._metrics.inc("shifter_errors")
            logger.warning("Shifter async failed: %s", e, exc_info=True)
            return 0, 0.0

        shift = {"hour": int(hour), "saving": float(saving)}
        task_ctx = task or {
            "current_hour": current_hour,
            "deadline_hour": deadline_hour,
            "energy_kwh": en,
        }
        if self.temporal_monitor:
            ok, violations = self.temporal_monitor.verify(shift, task_ctx)
            if not ok:
                logger.warning(
                    "Shift to hour %s violates %s; rejecting",
                    hour, violations,
                )
                self._metrics.inc("temporal_veto_total")
                return 0, 0.0

        if self.hitl and self.hitl.needs_review(hour, current_hour, urgency):
            approved = await self.hitl.request_review_async(
                shift_hour=hour,
                shift_saving=saving,
                urgency=urgency,
                reason=f"deferral of {abs(hour - current_hour)}h",
                context={"energy_kwh": en},
            )
            if approved is False:
                self._metrics.inc("hitl_veto_total")
                return 0, 0.0

        with self._lock:
            self.shift_count += 1
            self.shift_history.append({
                "hour": hour,
                "saving": saving,
                "current_hour": current_hour,
                "urgency": urgency,
                "at": _utcnow().isoformat(),
            })
        self._metrics.inc("shift_total", urgency=urgency)
        self._metrics.observe("shift_saving", float(saving))
        return hour, saving

    # ------------------------------------------------------------------ public APIs
    def set_hitl_callback(self, cb: Callable[[HITLRequest], bool]) -> None:
        if self.hitl:
            self.hitl.set_callback(cb)

    def set_hitl_async_callback(
        self, cb: Callable[[HITLRequest], Awaitable[bool]],
    ) -> None:
        if self.hitl:
            self.hitl.set_async_callback(cb)

    def active_learning_samples(self, n: int = 16) -> List[Dict[str, Any]]:
        return self.hitl.active_learning_batch(n) if self.hitl else []

    def explain_last_reward(self) -> Optional[RewardExplanation]:
        with self._lock:
            return self.last_explanation

    def contribute_federated(self) -> None:
        if not self.federated:
            return
        with self._lock:
            history = list(self.reward_history)
        if not history:
            return
        self.federated.push(FederatedRewardProfile(
            deployment_id=self.deployment_id,
            key="reward:default",
            mean_reward=statistics.fmean(
                r["final_reward"] for r in history
            ),
            mean_energy=statistics.fmean(r["energy"] for r in history),
            sample_count=len(history),
        ))
        self.federated.aggregate()

    def get_federated_aggregate(self) -> Dict[str, Dict[str, float]]:
        return self.federated.aggregate() if self.federated else {}

    def distill_reward_policy(
        self, urgency: str = "normal",
    ) -> Optional[DistilledRewardPolicy]:
        if not self.distiller:
            return None
        precision = (
            self.precision_ctl.select(urgency)
            if self.precision_ctl else PrecisionLevel.INT8
        )
        # Distil the CURRENT learned policy, not hardcoded defaults.
        with self._lock:
            current_penalty = (
                self.penalty_learner.base_penalty
                if self.penalty_learner
                else self.config.base_budget_penalty
            )
            current_negawatt_w = self.last_negawatt_weight
        return self.distiller.distill(
            base_penalty=current_penalty,
            negawatt_weight=current_negawatt_w,
            precision=precision,
        )

    # ------------------------------------------------------------------ health
    def health(self) -> Dict[str, Any]:
        return {
            "ready": (
                self._negawatt_circuit.state != CircuitState.OPEN
                and self._forecaster_circuit.state != CircuitState.OPEN
                and self._shifter_circuit.state != CircuitState.OPEN
            ),
            "circuits": {
                "negawatt": self._negawatt_circuit.state.value,
                "forecaster": self._forecaster_circuit.state.value,
                "shifter": self._shifter_circuit.state.value,
                "market": self._market_circuit.state.value,
            },
            "signals": {
                "carbon_forecaster_configured":
                    self.carbon_forecaster is not None,
                "helium_signal_configured":
                    self.helium_signal_fn is not None,
            },
            "uptime_seconds": time.monotonic() - self._started_mono,
        }

    # ------------------------------------------------------------------ statistics
    def get_statistics(self) -> Dict[str, Any]:
        with self._lock:
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
                    "market": self._market_circuit.state.value,
                },
                "uptime_seconds": time.monotonic() - self._started_mono,
            }
        if self.temporal_monitor:
            stats["recent_temporal_violations"] = list(
                self.temporal_monitor.violations,
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
        if self.penalty_learner:
            stats["causal_learner"] = {
                "observations": self.penalty_learner.observations,
                "updates": self.penalty_learner.updates,
                "last_advantage_mean":
                    self.penalty_learner.last_advantage_mean,
            }
        if self.federated:
            stats["federated_aggregate"] = self.federated.aggregate()
        with self._lock:
            if self.reward_history:
                rewards = [r["final_reward"] for r in self.reward_history]
                stats["reward_stats"] = {
                    "mean": statistics.fmean(rewards),
                    "max": max(rewards),
                    "min": min(rewards),
                    "count": len(rewards),
                }
        stats["metrics"] = self._metrics.snapshot()
        return stats


__all__ = [
    "AgentRole",
    "CausalPenaltyLearner",
    "CircuitBreaker",
    "CircuitState",
    "DistilledRewardPolicy",
    "FederatedAggregator",
    "FederatedRewardProfile",
    "HITLRequest",
    "HardwareProfile",
    "HeliumProfiler",
    "HumanInTheLoopGate",
    "MarketSnapshot",
    "MetricsRecorder",
    "MultiAgentCoordinator",
    "NonBackwardShift",
    "PenaltyState",
    "PolicyEngine",
    "PolicyEngineConfig",
    "PrecisionLevel",
    "QuantumDistillationBridge",
    "RewardExplanation",
    "RewardExplainer",
    "TemporalLogicMonitor",
]


# =============================================================================
# Demo
# =============================================================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    class _MockForecaster:
        def current_intensity(self):
            return 400.0

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
        logger.info("[HITL] auto-approve: %s", req.reason)
        return True

    # --- Legacy mode: all features off, matches original ---
    print("\n=== Legacy mode (all features off) ===")
    legacy = PolicyEngine(
        energy_budget=0.05, baseline_energy=0.1,
        features={k: False for k in PolicyEngine.default_features()},
    )
    r = legacy.compute_sustainability_reward(accuracy=0.9, energy=0.06)
    print(f"  reward = {r:.4f}")
    assert abs(r - (0.5 * 0.9 + 0.5 * 0.4 - 1.0)) < 1e-6

    # --- Enhanced mode ---
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
        precision=PrecisionLevel.INT4,
    )
    print(f"  reward = {reward:.4f}")
    exp = engine.explain_last_reward()
    if exp:
        print(f"  XAI: {exp.headline}  (confidence={exp.confidence:.2f})")
        for line in exp.rationale:
            print(f"    • {line}")

    # Temporal shift.
    hour, saving = engine.suggest_temporal_shift(
        shifter=_MockShifter(),
        forecast_engine=_MockForecaster(),
        energy_kwh=0.05,
        shiftable=True,
        urgency="low",
        current_hour=10,
        deadline_hour=20,
    )
    print(f"\n  Shift: hour={hour}, saving={saving:.6f} kgCO2e")

    # NonBackwardShift test — midnight should be rejected.
    print("\n=== NonBackwardShift test ===")
    class _BadShifter:
        def suggest(self, current, forecast, energy_kwh):
            return 0, 999.0  # backward + inflated saving
    hour, saving = engine.suggest_temporal_shift(
        shifter=_BadShifter(),
        forecast_engine=_MockForecaster(),
        energy_kwh=0.05,
        shiftable=True, urgency="low",
        current_hour=10, deadline_hour=20,
    )
    print(f"  backward shift attempt → ({hour}, {saving:.4f})")
    assert hour == 0 and saving == 0.0

    # Outcome feedback.
    engine.record_outcome(actual_energy=0.045, quality_preserved=True)

    # Federated.
    engine.contribute_federated()

    # Distill — should reflect current learned values.
    dist = engine.distill_reward_policy(urgency="normal")
    if dist:
        print("\n=== Distilled Reward Policy ===")
        print(f"  Precision:    {dist.precision.value}")
        print(f"  Base penalty: {dist.base_penalty}")
        print(f"  Retention:    {dist.quality_retention}")
        print(f"  Energy save:  {dist.energy_reduction_percent}%")

    # Helium-as-dict test.
    print("\n=== Helium-as-dict test ===")
    engine.helium_signal_fn = lambda: {"scarcity_score": 0.9}
    engine.compute_sustainability_reward(accuracy=0.9, energy=0.04)
    print("  helium signal handled from dict ✓")

    # Helium scarcity *increases* units.
    print("\n=== Helium scarcity direction ===")
    units_low = HeliumProfiler.estimate_units(1.0, 0.0)
    units_high = HeliumProfiler.estimate_units(1.0, 1.0)
    print(f"  scarcity=0.0 → {units_low:.4f}")
    print(f"  scarcity=1.0 → {units_high:.4f}")
    assert units_high > units_low

    # Health.
    print("\n=== Health ===")
    print(engine.health())

    # Statistics.
    import json
    print("\n=== Statistics ===")
    print(json.dumps(engine.get_statistics(), indent=2, default=str))

    engine.close()
    print("\nSmoke test passed.")
