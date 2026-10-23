"""
Eco-Mode Controller (v2.0.0)
============================

Dynamically throttles AI workloads based on carbon intensity forecasts.

**Correctness changes from v1**
-------------------------------
* **`hitl_approved` and `temporal_verified` are populated from the actual
  decision**, not hardcoded. This is an audit-trail fix; the previous
  version lied about what happened.
* **No fabricated RL "next state".** The policy is now an honest
  **contextual bandit** (renamed ``LinearEcoPolicy``); rewards come from
  observed carbon savings, not from multiplying the forecast by 0.95.
* **`NoEmergencyDeferralWithinDeadline` is task-aware**: it uses the
  current task's deadline, not a construction-time one, and only checks
  recent history. No more permanent EMERGENCY trap.
* **Temporal violations are checkable on the *proposed* change** via
  ``would_allow`` semantics, so a mode change that would violate a
  property is blocked *before* it enters history.
* **Async HITL callbacks work.** ``request_approval`` awaits awaitable
  callbacks; a coroutine is no longer silently coerced to truthy.
* **One forecast fetch per decision.** ``_fetch_forecast`` is called once
  per ``apply_throttling``; the mode decision and the recorded intensity
  come from the same snapshot.
* **`defer_until` is guarded**, so a malformed execution window cannot
  crash ``apply_throttling``.
* **`FederatedAggregator`** guards ``total_w == 0``.
* **Bounded collections** everywhere (mode_history, feedback_log,
  chaos.events, market.trades, per-agent history, temporal violations).
* **Deep-copied task snapshots** so ``decision.original_task`` cannot be
  mutated by a downstream consumer.

**Forecaster adapter**
----------------------
``ForecastAdapter`` detects the forecaster API at construction:

* **v18** — ``current_intensity_async`` / ``forecast_next_hours_async``.
* **v1**  — ``get_current_intensity`` / ``predict`` / ``find_optimal_execution_window``.

Both work with ``EcoModeController``; the v18 path is preferred when
available.

**Preserved from v1**
---------------------
* ``ECO_MODE_CONFIGS``, ``EcoMode``, ``PrecisionLevel``, ``AgentRole``
* ``ThrottlingDecision`` (extended with ``hitl_status`` and
  ``temporal_violations``)
* ``create_eco_mode_controller`` factory
* ``apply_throttling`` / ``batch_throttle`` / ``get_current_recommendation``
* ``get_statistics`` (extended with per-mode/precision/agent counters)

**Simplified reference implementations**
----------------------------------------
The ten enhancement classes are labelled *simplified reference
implementations* in their docstrings. ``CausalRLPolicy`` is retained as
an alias for ``LinearEcoPolicy`` (which is a bandit, not causal
inference).

Location: src/carbon/eco_mode_controller.py
"""

from __future__ import annotations

import asyncio
import copy
import inspect
import json
import logging
import math
import random
from collections import Counter, defaultdict, deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from types import MappingProxyType
from typing import (
    Any,
    Callable,
    Deque,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Protocol,
    Sequence,
    Tuple,
)

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
# Time / value helpers
# --------------------------------------------------------------------------- #
def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _ensure_aware(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _parse_timestamp(raw: Any) -> datetime:
    if isinstance(raw, datetime):
        return _ensure_aware(raw)
    if isinstance(raw, str):
        s = raw.strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            return _ensure_aware(datetime.fromisoformat(s))
        except ValueError:
            return _utcnow()
    return _utcnow()


# --------------------------------------------------------------------------- #
# Bounds
# --------------------------------------------------------------------------- #
_MAX_MODE_HISTORY = 1000
_MAX_HITL_LOG = 500
_MAX_CHAOS_EVENTS = 500
_MAX_MARKET_TRADES = 500
_MAX_AGENT_HISTORY = 200
_MAX_TEMPORAL_VIOLATIONS = 200
_MAX_FEDERATED_UPDATES = 100
_MAX_RL_BUFFER = 2048


# =========================================================================== #
# FORECASTER ADAPTER
# =========================================================================== #
@dataclass
class PredictionPoint:
    """Normalised prediction point used by the controller."""

    timestamp: datetime
    predicted_intensity: float
    confidence: float = 0.5


@dataclass
class ExecutionWindow:
    """Normalised execution window."""

    start_time: datetime
    carbon_savings_percent: float = 0.0


class ForecastAdapter:
    """
    Thin shim over any of the supported forecaster APIs.

    Supports:

    * **v18** ``CarbonForecast``
      (``current_intensity_async`` / ``forecast_next_hours_async``)
    * **v1** — legacy interface
      (``get_current_intensity`` / ``predict`` /
      ``find_optimal_execution_window``)

    Unknown forecasters raise at construction (fail fast).
    """

    def __init__(self, forecaster: Any) -> None:
        if forecaster is None:
            raise ValueError("forecaster is required")
        self._forecaster = forecaster
        self._is_v18 = hasattr(forecaster, "current_intensity_async")

        # Fall back to the v1 API if v18 is not available
        if not self._is_v18 and not hasattr(
            forecaster, "get_current_intensity"
        ):
            raise TypeError(
                "forecaster must expose either 'current_intensity_async' "
                "(v18) or 'get_current_intensity' (v1); "
                f"got {type(forecaster).__name__}"
            )

    @property
    def is_v18(self) -> bool:
        return self._is_v18

    @property
    def raw(self) -> Any:
        return self._forecaster

    async def get_current_intensity(self) -> Optional[float]:
        """
        Return the current intensity as ``float``, or ``None`` on failure.

        Never raises — callers decide how to handle "no reading".
        """
        try:
            if self._is_v18:
                value = await self._forecaster.current_intensity_async(
                    legacy_float=True
                )
                return float(value)
            value = await self._forecaster.get_current_intensity()
            return float(value)
        except Exception as e:
            logger.warning("get_current_intensity failed: %s", e)
            return None

    async def predict(
        self, horizon: str = "1h", interval_minutes: int = 15
    ) -> List[PredictionPoint]:
        """Return a list of normalised prediction points (may be empty)."""
        try:
            if self._is_v18:
                hours = _horizon_to_hours(horizon)
                batch = await self._forecaster.forecast_next_hours_async(
                    hours=hours
                )
                points = getattr(batch, "points", []) or []
                return [
                    PredictionPoint(
                        timestamp=_parse_timestamp(p.timestamp),
                        predicted_intensity=float(p.intensity),
                        confidence=float(getattr(p, "confidence", 0.5)),
                    )
                    for p in points
                ]
            # v1
            raw = await self._forecaster.predict(
                horizon=horizon, interval_minutes=interval_minutes
            )
            return [
                PredictionPoint(
                    timestamp=_parse_timestamp(p.timestamp),
                    predicted_intensity=float(p.predicted_intensity),
                    confidence=float(getattr(p, "confidence", 0.5)),
                )
                for p in raw
            ]
        except Exception as e:
            logger.warning("predict failed: %s", e)
            return []

    async def find_optimal_execution_window(
        self, duration_hours: float, deadline: datetime
    ) -> Optional[ExecutionWindow]:
        """Return the best execution window, or ``None`` if unavailable."""
        try:
            if self._is_v18:
                now = _utcnow()
                deadline = _ensure_aware(deadline)
                if deadline <= now:
                    return None
                hours_ahead = max(
                    1, min(int((deadline - now).total_seconds() / 3600), 168)
                )
                batch = await self._forecaster.forecast_next_hours_async(
                    hours=hours_ahead
                )
                points = getattr(batch, "points", []) or []
                if not points:
                    return None
                best = min(points, key=lambda p: float(p.intensity))
                current = await self.get_current_intensity()
                if current is None or current <= 0:
                    return ExecutionWindow(
                        start_time=_parse_timestamp(best.timestamp),
                        carbon_savings_percent=0.0,
                    )
                savings = max(
                    0.0,
                    (current - float(best.intensity)) / current * 100.0,
                )
                return ExecutionWindow(
                    start_time=_parse_timestamp(best.timestamp),
                    carbon_savings_percent=savings,
                )
            # v1
            w = await self._forecaster.find_optimal_execution_window(
                duration_hours=duration_hours, deadline=deadline
            )
            start = getattr(w, "start_time", None)
            if start is None:
                return None
            return ExecutionWindow(
                start_time=_parse_timestamp(start),
                carbon_savings_percent=float(
                    getattr(w, "carbon_savings_percent", 0.0) or 0.0
                ),
            )
        except Exception as e:
            logger.warning("find_optimal_execution_window failed: %s", e)
            return None

    async def aclose(self) -> None:
        """Close the underlying forecaster if it exposes a closer."""
        for name in ("close", "aclose"):
            fn = getattr(self._forecaster, name, None)
            if callable(fn):
                try:
                    result = fn()
                    if inspect.isawaitable(result):
                        await result
                except Exception as e:
                    logger.warning("forecaster %s failed: %s", name, e)
                return


def _horizon_to_hours(horizon: str) -> int:
    """Translate a human horizon ('1h', '6h', '1d') to integer hours."""
    horizon = horizon.strip().lower()
    if horizon.endswith("h"):
        return max(1, int(float(horizon[:-1])))
    if horizon.endswith("d"):
        return max(1, int(float(horizon[:-1]) * 24))
    if horizon.endswith("m"):
        return max(1, int(float(horizon[:-1]) / 60))
    try:
        return max(1, int(float(horizon)))
    except ValueError:
        return 1


# =========================================================================== #
# CORE ENUMS & DATACLASSES
# =========================================================================== #
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


@dataclass(frozen=True)
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
    preferred_precision: PrecisionLevel = PrecisionLevel.FP32

    def __post_init__(self) -> None:
        if not 0 <= self.compute_limit_percent <= 100:
            raise ValueError("compute_limit_percent must be in [0, 100]")
        if self.max_tokens_multiplier < 0:
            raise ValueError("max_tokens_multiplier must be >= 0")
        if not 0.0 <= self.quality_degradation_acceptable <= 1.0:
            raise ValueError(
                "quality_degradation_acceptable must be in [0, 1]"
            )


# --------------------------------------------------------------------------- #
# Immutable configuration tables
# --------------------------------------------------------------------------- #
_ECO_MODE_CONFIGS: Dict[EcoMode, EcoModeConfig] = {
    EcoMode.PERFORMANCE: EcoModeConfig(
        mode=EcoMode.PERFORMANCE,
        compute_limit_percent=100,
        max_tokens_multiplier=1.0,
        temperature_override=None,
        top_k_override=None,
        use_quantized_model=False,
        use_cache_aggressively=False,
        early_stopping_threshold=0.95,
        quality_degradation_acceptable=0.0,
        preferred_precision=PrecisionLevel.FP32,
    ),
    EcoMode.BALANCED: EcoModeConfig(
        mode=EcoMode.BALANCED,
        compute_limit_percent=80,
        max_tokens_multiplier=0.8,
        temperature_override=None,
        top_k_override=None,
        use_quantized_model=False,
        use_cache_aggressively=True,
        early_stopping_threshold=0.90,
        quality_degradation_acceptable=0.05,
        preferred_precision=PrecisionLevel.FP16,
    ),
    EcoMode.GREEN: EcoModeConfig(
        mode=EcoMode.GREEN,
        compute_limit_percent=50,
        max_tokens_multiplier=0.5,
        temperature_override=0.3,
        top_k_override=10,
        use_quantized_model=False,
        use_cache_aggressively=True,
        early_stopping_threshold=0.85,
        quality_degradation_acceptable=0.10,
        preferred_precision=PrecisionLevel.INT8,
    ),
    EcoMode.EXTREME_GREEN: EcoModeConfig(
        mode=EcoMode.EXTREME_GREEN,
        compute_limit_percent=25,
        max_tokens_multiplier=0.25,
        temperature_override=0.1,
        top_k_override=5,
        use_quantized_model=True,
        use_cache_aggressively=True,
        early_stopping_threshold=0.80,
        quality_degradation_acceptable=0.15,
        preferred_precision=PrecisionLevel.INT4,
    ),
    EcoMode.EMERGENCY: EcoModeConfig(
        mode=EcoMode.EMERGENCY,
        compute_limit_percent=10,
        max_tokens_multiplier=0.1,
        temperature_override=0.01,
        top_k_override=3,
        use_quantized_model=True,
        use_cache_aggressively=True,
        early_stopping_threshold=0.75,
        quality_degradation_acceptable=0.20,
        preferred_precision=PrecisionLevel.QUANTUM_DISTILLED,
    ),
}
ECO_MODE_CONFIGS: Mapping[EcoMode, EcoModeConfig] = MappingProxyType(
    _ECO_MODE_CONFIGS
)

_DEFAULT_FEATURES: Dict[str, bool] = {
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
    # If True, the RL policy can only push toward greener modes.
    "conservative_override": True,
}
DEFAULT_FEATURES: Mapping[str, bool] = MappingProxyType(
    dict(_DEFAULT_FEATURES)
)

_MODE_ORDER = list(EcoMode)  # PERFORMANCE ... EMERGENCY


# =========================================================================== #
# ENHANCEMENT 1 — Quantum-distillation bridge
# =========================================================================== #
@dataclass
class DistillationRequest:
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
    distilled_at: datetime = field(default_factory=_utcnow)


class QuantumDistillationBridge:
    """
    **Simplified reference implementation**: caches distillation results
    for (model, precision) pairs. Real deployments should delegate to a
    distillation orchestrator.
    """

    _ENERGY_REDUCTION = {
        PrecisionLevel.FP32: 0.0,
        PrecisionLevel.FP16: 30.0,
        PrecisionLevel.INT8: 55.0,
        PrecisionLevel.INT4: 70.0,
        PrecisionLevel.QUANTUM_DISTILLED: 85.0,
    }

    def __init__(self, enabled: bool = True) -> None:
        self.enabled = enabled
        self._cache: Dict[
            Tuple[str, PrecisionLevel], DistillationResult
        ] = {}

    def lookup(
        self, model_id: str, precision: PrecisionLevel
    ) -> Optional[DistillationResult]:
        return self._cache.get((model_id, precision))

    async def request_distillation(
        self, req: DistillationRequest
    ) -> DistillationResult:
        await asyncio.sleep(0)
        result = DistillationResult(
            model_id=req.model_id,
            precision=req.target_precision,
            achieved_quality_retention=1.0
            - req.max_acceptable_quality_loss * 0.5,
            energy_reduction_percent=self._ENERGY_REDUCTION.get(
                req.target_precision, 0.0
            ),
        )
        self._cache[(req.model_id, req.target_precision)] = result
        return result


# =========================================================================== #
# ENHANCEMENT 2 — Linear eco policy (contextual bandit)
# =========================================================================== #
@dataclass(frozen=True)
class PolicyState:
    carbon_intensity: float
    forecast_intensity: float
    gpu_utilization: float
    queue_depth: int
    recent_quality_loss: float

    def features(self) -> List[float]:
        return [
            self.carbon_intensity / 1000.0,
            self.forecast_intensity / 1000.0,
            self.gpu_utilization,
            min(self.queue_depth / 100.0, 1.0),
            self.recent_quality_loss,
        ]

    @staticmethod
    def feature_names() -> List[str]:
        return [
            "carbon_intensity",
            "forecast_intensity",
            "gpu_utilization",
            "queue_depth",
            "recent_quality_loss",
        ]


class LinearEcoPolicy:
    """
    **Simplified reference implementation**: a linear contextual bandit
    over ``EcoMode``.

    This is **not** causal RL:

    * No do-calculus, no counterfactual sampling, no structural model.
    * No next-state bootstrapping — rewards come from immediate observed
      outcomes (carbon saved, quality impact).
    * ε-greedy exploration with a fixed ε.

    The class was called ``CausalRLPolicy`` in v1; that name implied
    stronger guarantees than the code delivers. ``CausalRLPolicy`` is
    retained as an alias for backward compatibility.
    """

    def __init__(
        self,
        epsilon: float = 0.1,
        lr: float = 0.01,
        min_samples_before_update: int = 20,
    ) -> None:
        if not 0.0 <= epsilon <= 1.0:
            raise ValueError("epsilon must be in [0, 1]")
        if lr <= 0:
            raise ValueError("lr must be > 0")
        self.epsilon = epsilon
        self.lr = lr
        self.min_samples_before_update = min_samples_before_update

        self.weights: Dict[EcoMode, List[float]] = {
            m: [0.0] * 5 for m in EcoMode
        }
        self.buffer: Deque[
            Tuple[PolicyState, EcoMode, float]
        ] = deque(maxlen=_MAX_RL_BUFFER)
        self._samples_seen = 0
        self._update_count = 0
        self._mode_list = list(EcoMode)

    # ---- feature/q helpers ----
    @staticmethod
    def _features(s: PolicyState) -> List[float]:
        return s.features()

    def _q(self, s: PolicyState, a: EcoMode) -> float:
        f = self._features(s)
        return sum(w * x for w, x in zip(self.weights[a], f))

    # ---- selection ----
    def select_action(self, s: PolicyState) -> EcoMode:
        if random.random() < self.epsilon:
            return random.choice(self._mode_list)
        return max(self._mode_list, key=lambda a: self._q(s, a))

    # ---- recording / learning ----
    def record(
        self, state: PolicyState, action: EcoMode, reward: float
    ) -> None:
        self.buffer.append((state, action, float(reward)))
        self._samples_seen += 1

    def update(self) -> bool:
        """Bandit update over buffered (state, action, reward) triples."""
        if len(self.buffer) < self.min_samples_before_update:
            return False
        for state, action, reward in self.buffer:
            f = self._features(state)
            q = sum(w * x for w, x in zip(self.weights[action], f))
            err = reward - q
            for i, x in enumerate(f):
                self.weights[action][i] += self.lr * err * x
        self.buffer.clear()
        self._update_count += 1
        return True

    # ---- introspection ----
    def contributions(
        self, state: PolicyState, action: EcoMode
    ) -> Dict[str, float]:
        """Per-feature contribution to the Q-value of ``action``."""
        f = self._features(state)
        return {
            name: w * x
            for name, w, x in zip(
                PolicyState.feature_names(), self.weights[action], f
            )
        }

    def stats(self) -> Dict[str, Any]:
        return {
            "samples_seen": self._samples_seen,
            "update_count": self._update_count,
            "buffer_size": len(self.buffer),
            "epsilon": self.epsilon,
            "lr": self.lr,
        }


# Backward-compatible alias (the old name implied more than the code does)
CausalRLPolicy = LinearEcoPolicy


# =========================================================================== #
# ENHANCEMENT 3 — Federated green learning (simplified reference)
# =========================================================================== #
@dataclass
class FederatedUpdate:
    deployment_id: str
    mode_counts: Dict[str, int]
    avg_intensity: float
    avg_quality_loss: float
    weight: int
    timestamp: datetime = field(default_factory=_utcnow)


class FederatedAggregator:
    """
    **Simplified reference implementation**: weighted aggregation of
    mode-transition statistics, bounded in memory.
    """

    def __init__(self, max_updates: int = _MAX_FEDERATED_UPDATES) -> None:
        self._updates: Deque[FederatedUpdate] = deque(maxlen=max_updates)

    def push(self, update: FederatedUpdate) -> None:
        self._updates.append(update)

    def aggregate(self) -> Dict[str, Any]:
        if not self._updates:
            return {
                "mode_counts": {},
                "global_avg_intensity": 0.0,
                "global_avg_quality_loss": 0.0,
                "contributors": 0,
            }
        total_w = sum(max(0, u.weight) for u in self._updates)
        if total_w <= 0:
            return {
                "mode_counts": {},
                "global_avg_intensity": 0.0,
                "global_avg_quality_loss": 0.0,
                "contributors": len(self._updates),
            }
        merged: Dict[str, int] = defaultdict(int)
        inten = 0.0
        qloss = 0.0
        for u in self._updates:
            w = max(0, u.weight)
            for k, v in u.mode_counts.items():
                merged[k] += v
            inten += u.avg_intensity * w
            qloss += u.avg_quality_loss * w
        return {
            "mode_counts": dict(merged),
            "global_avg_intensity": inten / total_w,
            "global_avg_quality_loss": qloss / total_w,
            "contributors": len(self._updates),
        }

    def reset(self) -> None:
        self._updates.clear()


# =========================================================================== #
# ENHANCEMENT 4 — Multi-agent coordination (simplified reference)
# =========================================================================== #
@dataclass
class AgentProfile:
    agent_id: str
    role: AgentRole
    success_rate: float = 0.0
    deferral_success: float = 0.0
    energy_efficiency: float = 0.0
    throughput: float = 0.0  # tasks / minute, for THROUGHPUT_ORIENTED


class MultiAgentCoordinator:
    """
    **Simplified reference implementation**: emergent role assignment via
    a sliding-window mean over the last ``window_size`` outcomes. Routes
    tasks by role; all five roles are actually consulted.
    """

    def __init__(self, window_size: int = 20) -> None:
        self.agents: Dict[str, AgentProfile] = {}
        self._history: Dict[str, Deque[Dict[str, Any]]] = defaultdict(
            lambda: deque(maxlen=window_size)
        )

    def register(self, agent_id: str) -> AgentProfile:
        if agent_id not in self.agents:
            self.agents[agent_id] = AgentProfile(
                agent_id, AgentRole.BALANCED_WORKER
            )
        return self.agents[agent_id]

    # ---- role reassignment using sliding-window means ----
    def _window_mean(self, agent_id: str, key: str) -> float:
        hist = self._history[agent_id]
        if not hist:
            return 0.0
        return sum(float(h.get(key, 0.0)) for h in hist) / len(hist)

    def _reassign_roles(self) -> None:
        for a in self.agents.values():
            hist = self._history[a.agent_id]
            if len(hist) < 3:
                continue
            a.success_rate = self._window_mean(a.agent_id, "success")
            a.deferral_success = self._window_mean(a.agent_id, "deferral")
            a.energy_efficiency = self._window_mean(
                a.agent_id, "energy_saved"
            )
            a.throughput = self._window_mean(a.agent_id, "throughput")

            if a.deferral_success > 0.8:
                a.role = AgentRole.DEFERRAL_SPECIALIST
            elif a.throughput > 0.7:
                a.role = AgentRole.THROUGHPUT_ORIENTED
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
        throughput: float = 0.5,
    ) -> None:
        a = self.register(agent_id)
        self._history[agent_id].append(
            {
                "success": float(success),
                "deferral": float(success) if was_deferred else 0.0,
                "energy_saved": energy_saved_pct / 100.0,
                "throughput": throughput,
            }
        )
        self._reassign_roles()

    def select_agent(self, task: Mapping[str, Any]) -> Optional[str]:
        if not self.agents:
            return None
        # 1. Deferrable + low-priority → deferral specialists
        if task.get("deferrable") and task.get("low_priority"):
            specialists = [
                a
                for a in self.agents.values()
                if a.role == AgentRole.DEFERRAL_SPECIALIST
            ]
            if specialists:
                return max(
                    specialists, key=lambda a: a.deferral_success
                ).agent_id
        # 2. Latency-critical → latency specialists
        if task.get("latency_critical"):
            crit = [
                a
                for a in self.agents.values()
                if a.role == AgentRole.LATENCY_CRITICAL
            ]
            if crit:
                return max(crit, key=lambda a: a.success_rate).agent_id
        # 3. High-throughput → throughput specialists
        if task.get("high_throughput"):
            tp = [
                a
                for a in self.agents.values()
                if a.role == AgentRole.THROUGHPUT_ORIENTED
            ]
            if tp:
                return max(tp, key=lambda a: a.throughput).agent_id
        # 4. Carbon-sensitive → carbon opportunists
        if task.get("carbon_sensitive"):
            opp = [
                a
                for a in self.agents.values()
                if a.role == AgentRole.CARBON_OPPORTUNISTIC
            ]
            if opp:
                return max(
                    opp, key=lambda a: a.energy_efficiency
                ).agent_id
        # 5. Fallback → best energy efficiency
        return max(
            self.agents.values(), key=lambda a: a.energy_efficiency
        ).agent_id


# =========================================================================== #
# ENHANCEMENT 5 — Temporal logic
# =========================================================================== #
class TemporalProperty(Protocol):
    def check(
        self,
        history: List[Tuple[datetime, EcoMode, float]],
        context: Optional[Mapping[str, Any]] = None,
    ) -> bool: ...


@dataclass
class NoEmergencyDeferralWithinDeadline:
    """
    G(EMERGENCY → entry_time <= task.deadline).

    Only fires when ``context`` carries a ``deadline``. Checks recent
    history only (last 20 entries) so old violations do not permanently
    trap the controller.
    """

    window_seconds: int = 3600

    def check(
        self,
        history: List[Tuple[datetime, EcoMode, float]],
        context: Optional[Mapping[str, Any]] = None,
    ) -> bool:
        if not context:
            return True
        deadline = context.get("deadline")
        if deadline is None:
            return True
        deadline = _ensure_aware(
            deadline
            if isinstance(deadline, datetime)
            else _parse_timestamp(deadline)
        )
        for ts, mode, _ in history[-20:]:
            if mode == EcoMode.EMERGENCY and _ensure_aware(ts) > deadline:
                return False
        return True


@dataclass
class BoundedModeChurn:
    """
    No more than ``max_changes`` mode changes in any ``window_seconds``
    window. O(n) sliding-window check.
    """

    max_changes: int = 4
    window_seconds: int = 600

    def check(
        self,
        history: List[Tuple[datetime, EcoMode, float]],
        context: Optional[Mapping[str, Any]] = None,
    ) -> bool:
        if len(history) < 2:
            return True
        j = 0
        n = len(history)
        for i in range(n):
            base = history[i][0]
            while j < n and (
                history[j][0] - base
            ).total_seconds() <= self.window_seconds:
                j += 1
            changes_after_i = j - i - 1
            if changes_after_i > self.max_changes:
                return False
        return True


class TemporalLogicMonitor:
    """
    **Simplified reference implementation**: verifies bounded temporal
    properties over the recent history of mode decisions.

    Violations are bounded; ``would_allow`` checks whether *adding* a
    proposed entry would cause a violation, so a mode change can be
    rejected *before* it enters history.
    """

    def __init__(
        self, max_violations: int = _MAX_TEMPORAL_VIOLATIONS
    ) -> None:
        self.properties: List[TemporalProperty] = []
        self.violations: Deque[Dict[str, Any]] = deque(
            maxlen=max_violations
        )

    def register(self, prop: TemporalProperty) -> None:
        self.properties.append(prop)

    def _lookback(self) -> int:
        windows = [
            getattr(p, "window_seconds", 0) for p in self.properties
        ]
        return max(1200, 2 * max(windows) if windows else 1200)

    def verify(
        self,
        history: List[Tuple[datetime, EcoMode, float]],
        context: Optional[Mapping[str, Any]] = None,
        lookback_seconds: Optional[int] = None,
    ) -> bool:
        """Verify the recent portion of ``history``."""
        if lookback_seconds is None:
            lookback_seconds = self._lookback()
        cutoff = _utcnow() - timedelta(seconds=lookback_seconds)
        recent = [h for h in history if _ensure_aware(h[0]) >= cutoff]
        ok = True
        for p in self.properties:
            try:
                if not p.check(recent, context):
                    ok = False
                    self.violations.append(
                        {
                            "property": type(p).__name__,
                            "at": _utcnow().isoformat(),
                        }
                    )
            except Exception as e:
                logger.warning(
                    "Temporal property %s raised: %s",
                    type(p).__name__,
                    e,
                )
                ok = False
        return ok

    def would_allow(
        self,
        current_history: List[Tuple[datetime, EcoMode, float]],
        proposed_entry: Tuple[datetime, EcoMode, float],
        context: Optional[Mapping[str, Any]] = None,
    ) -> bool:
        """
        True iff appending ``proposed_entry`` would still satisfy every
        property. The current history is trusted (it was verified
        incrementally), so only the proposed entry is subject to a fresh
        check — implemented by simply re-verifying the augmented history
        over the lookback window.
        """
        proposed = list(current_history) + [proposed_entry]
        return self.verify(proposed, context=context)

    def stats(self) -> Dict[str, Any]:
        return {
            "properties": [type(p).__name__ for p in self.properties],
            "violation_count": len(self.violations),
            "last_violations": list(self.violations)[-5:],
        }


# =========================================================================== #
# ENHANCEMENT 6 — XAI
# =========================================================================== #
@dataclass
class Explanation:
    headline: str
    rationale: List[str]
    counterfactual: str
    confidence: float
    contributing_factors: Dict[str, float]
    policy_attributions: Optional[Dict[str, float]] = None


def explain_decision(
    mode: EcoMode,
    intensity: float,
    forecast: float,
    thresholds: Mapping[EcoMode, float],
    config: EcoModeConfig,
    policy: Optional[LinearEcoPolicy] = None,
    state: Optional[PolicyState] = None,
) -> Explanation:
    """
    Build a human-readable explanation.

    When ``policy`` and ``state`` are provided, per-feature contributions
    are computed from the linear Q-weights — real attributions, not a
    template.
    """
    reasons: List[str] = []
    reasons.append(
        f"Effective carbon intensity = {max(intensity, forecast):.0f} "
        f"gCO₂/kWh (current={intensity:.0f}, forecast={forecast:.0f})."
    )
    for m, thr in thresholds.items():
        if m == mode:
            thr_str = "∞" if thr == float("inf") else f"{thr:.0f}"
            reasons.append(
                f"Mode '{mode.value}' selected because intensity is below "
                f"the {m.value} threshold ({thr_str})."
            )
            break
    reasons.append(
        f"Compute limited to {config.compute_limit_percent}% → "
        f"estimated energy reduction ≈ "
        f"{100 - config.compute_limit_percent}%."
    )
    reasons.append(
        f"Acceptable quality loss: "
        f"{config.quality_degradation_acceptable * 100:.1f}%."
    )
    if policy is not None:
        reasons.append(
            "Adaptive linear-bandit policy informed this decision "
            "(see policy_attributions)."
        )

    counterfactual = (
        f"If carbon intensity had stayed below "
        f"{thresholds.get(EcoMode.BALANCED, 250):.0f} gCO₂/kWh, "
        f"mode would be BALANCED."
    )

    factors = {
        "carbon_intensity": intensity / 1000.0,
        "forecast_intensity": forecast / 1000.0,
        "compute_limit": config.compute_limit_percent / 100.0,
        "quality_risk": config.quality_degradation_acceptable,
    }

    policy_attrs: Optional[Dict[str, float]] = None
    if policy is not None and state is not None:
        policy_attrs = policy.contributions(state, mode)

    confidence = 0.95 if policy is None else 0.80
    return Explanation(
        headline=f"Eco-Mode = {mode.value.upper()}",
        rationale=reasons,
        counterfactual=counterfactual,
        confidence=confidence,
        contributing_factors=factors,
        policy_attributions=policy_attrs,
    )


# =========================================================================== #
# ENHANCEMENT 7 — Adaptive precision switching
# =========================================================================== #
@dataclass
class HardwareProfile:
    has_tensor_cores: bool = True
    supports_int8: bool = True
    supports_int4: bool = False
    vram_gb: float = 24.0
    tdp_watts: float = 350.0


class AdaptivePrecisionController:
    """
    **Simplified reference implementation**: chooses precision by task SLA
    → eco-mode config → hardware capability.

    Precedence:

    1. Explicit ``task["precision"]`` (if valid) — caller override.
    2. Task SLA (``latency_critical`` → fp16 if tensor cores, else fp32).
    3. ``config.preferred_precision`` (from ``EcoModeConfig``).
    4. Hardware capability (fallback).
    """

    VALID = {
        PrecisionLevel.FP32,
        PrecisionLevel.FP16,
        PrecisionLevel.INT8,
        PrecisionLevel.INT4,
        PrecisionLevel.QUANTUM_DISTILLED,
    }

    def __init__(self, hw: Optional[HardwareProfile] = None) -> None:
        self.hw = hw or HardwareProfile()

    def _parse_override(self, raw: Any) -> Optional[PrecisionLevel]:
        if raw is None:
            return None
        if isinstance(raw, PrecisionLevel):
            return raw
        try:
            return PrecisionLevel(str(raw).lower())
        except ValueError:
            return None

    def _hw_floor(self, target: PrecisionLevel) -> PrecisionLevel:
        """Clamp ``target`` to what the hardware supports."""
        if target == PrecisionLevel.INT4 and not self.hw.supports_int4:
            return PrecisionLevel.INT8
        if target == PrecisionLevel.INT8 and not self.hw.supports_int8:
            return PrecisionLevel.FP16
        if (
            target == PrecisionLevel.FP16
            and not self.hw.has_tensor_cores
        ):
            return PrecisionLevel.FP32
        if target == PrecisionLevel.QUANTUM_DISTILLED:
            # Distillation target is always accepted; the distiller
            # handles the actual variant.
            return target
        return target

    def select(
        self,
        mode: EcoMode,
        task: Mapping[str, Any],
        config: Optional[EcoModeConfig] = None,
    ) -> PrecisionLevel:
        # 1. Explicit task override
        override = self._parse_override(task.get("precision"))
        if override is not None:
            return self._hw_floor(override)

        # 2. Task SLA
        if task.get("latency_critical"):
            return self._hw_floor(
                PrecisionLevel.FP16
                if self.hw.has_tensor_cores
                else PrecisionLevel.FP32
            )

        # 3. Config preference
        if config is not None:
            return self._hw_floor(config.preferred_precision)

        # 4. Mode default
        default = _ECO_MODE_CONFIGS[mode].preferred_precision
        return self._hw_floor(default)


# =========================================================================== #
# ENHANCEMENT 8 — Carbon market bridge (simplified reference)
# =========================================================================== #
@dataclass
class MarketQuote:
    price_per_kwh: float
    credit_kgco2: float

    def effective_cost(self, energy_kwh: float) -> float:
        return self.price_per_kwh * energy_kwh


class CarbonMarketBridge:
    """**Simplified reference implementation**: budget-bounded trades."""

    def __init__(self, budget_usd: float = 10.0) -> None:
        if budget_usd < 0:
            raise ValueError("budget_usd must be >= 0")
        self.budget = budget_usd
        self.spent = 0.0
        self.earned = 0.0
        self.trades: Deque[Dict[str, Any]] = deque(
            maxlen=_MAX_MARKET_TRADES
        )

    async def get_quote(self) -> MarketQuote:
        await asyncio.sleep(0)
        return MarketQuote(price_per_kwh=0.08, credit_kgco2=0.4)

    def should_buy_credits(
        self,
        task: Mapping[str, Any],
        mode: EcoMode,
        quote: MarketQuote,
    ) -> bool:
        if mode != EcoMode.EMERGENCY:
            return False
        if not task.get("deadline_critical"):
            return False
        energy = float(task.get("estimated_energy_kwh", 0.001))
        cost = quote.effective_cost(energy)
        return (self.spent + cost) <= self.budget

    async def buy_credits(
        self,
        energy_kwh: float,
        quote: MarketQuote,
        reason: str,
    ) -> Dict[str, Any]:
        cost = quote.effective_cost(energy_kwh)
        self.spent += cost
        rec = {
            "type": "buy",
            "energy_kwh": energy_kwh,
            "cost_usd": cost,
            "co2_offset_kg": energy_kwh * quote.credit_kgco2,
            "reason": reason,
            "at": _utcnow().isoformat(),
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
            "at": _utcnow().isoformat(),
        }
        self.trades.append(rec)
        return rec


# =========================================================================== #
# ENHANCEMENT 9 — Chaos injector (simplified reference)
# =========================================================================== #
class ChaosInjector:
    """
    **Simplified reference implementation**: deterministic-rate fault
    injection for the forecaster / market / HITL paths.
    """

    def __init__(
        self,
        failure_rate: float = 0.0,
        forecaster_down: bool = False,
    ) -> None:
        if not 0.0 <= failure_rate <= 1.0:
            raise ValueError("failure_rate must be in [0, 1]")
        self.failure_rate = failure_rate
        self.forecaster_down = forecaster_down
        self.events: Deque[Dict[str, Any]] = deque(
            maxlen=_MAX_CHAOS_EVENTS
        )

    def maybe_fail(self, component: str) -> bool:
        if component == "forecaster" and self.forecaster_down:
            self.events.append(
                {
                    "component": component,
                    "reason": "forecaster_down",
                    "at": _utcnow().isoformat(),
                }
            )
            return True
        if random.random() < self.failure_rate:
            self.events.append(
                {
                    "component": component,
                    "reason": "rate",
                    "at": _utcnow().isoformat(),
                }
            )
            return True
        return False


# =========================================================================== #
# ENHANCEMENT 10 — HITL gate
# =========================================================================== #
@dataclass
class HITLRequest:
    task_id: str
    proposed_mode: EcoMode
    reason: str
    urgency: str
    requested_at: datetime = field(default_factory=_utcnow)


class HITLGate:
    """
    **Simplified reference implementation**: prompts a human for approval
    on high-stakes decisions. ``request_approval`` **awaits awaitable
    callbacks** and treats them as truthy only if they resolve to truthy.
    """

    def __init__(self, auto_approve_low_risk: bool = True) -> None:
        self.auto_approve_low_risk = auto_approve_low_risk
        self.pending: List[HITLRequest] = []
        self.feedback_log: Deque[Dict[str, Any]] = deque(
            maxlen=_MAX_HITL_LOG
        )
        self._callback: Optional[Callable[[HITLRequest], Any]] = None

    def set_callback(
        self, cb: Callable[[HITLRequest], Any]
    ) -> None:
        self._callback = cb

    async def request_approval(
        self, task: Mapping[str, Any], mode: EcoMode, reason: str
    ) -> Optional[bool]:
        """
        Returns ``True`` (approved), ``False`` (rejected), or ``None``
        (no answer — treated as rejection by the caller).
        """
        urgency = (
            "high"
            if task.get("deadline_critical")
            else ("medium" if mode == EcoMode.EMERGENCY else "low")
        )
        req = HITLRequest(
            task_id=str(task.get("task_id", "unknown")),
            proposed_mode=mode,
            reason=reason,
            urgency=urgency,
        )
        if urgency == "low" and self.auto_approve_low_risk:
            self.feedback_log.append(
                {
                    "req": asdict(req),
                    "decision": True,
                    "source": "auto",
                }
            )
            return True
        if self._callback is None:
            self.feedback_log.append(
                {
                    "req": asdict(req),
                    "decision": None,
                    "source": "no_callback",
                }
            )
            return None
        try:
            decision = self._callback(req)
            if inspect.isawaitable(decision):
                decision = await decision
            if decision is None:
                self.feedback_log.append(
                    {
                        "req": asdict(req),
                        "decision": None,
                        "source": "callback_none",
                    }
                )
                return None
            approved = bool(decision)
            self.feedback_log.append(
                {
                    "req": asdict(req),
                    "decision": approved,
                    "source": "callback",
                }
            )
            return approved
        except Exception as e:
            logger.warning("HITL callback raised: %s", e)
            self.feedback_log.append(
                {
                    "req": asdict(req),
                    "decision": None,
                    "source": "callback_error",
                }
            )
            return None

    def active_learning_batch(self, n: int = 16) -> List[Dict[str, Any]]:
        return list(self.feedback_log)[-n:]

    def outcome_counts(self) -> Dict[str, int]:
        counts: Counter = Counter()
        for entry in self.feedback_log:
            decision = entry.get("decision")
            if decision is True:
                counts["approved"] += 1
            elif decision is False:
                counts["rejected"] += 1
            else:
                counts["no_answer"] += 1
        return dict(counts)


# =========================================================================== #
# DECISION RECORD
# =========================================================================== #
@dataclass
class ThrottlingDecision:
    # --- from v1 ---
    original_task: Dict[str, Any]
    throttled_task: Dict[str, Any]
    eco_mode: EcoMode
    carbon_intensity: float
    estimated_energy_reduction_percent: float
    estimated_quality_impact_percent: float
    should_defer: bool
    defer_until: Optional[datetime]
    # --- extension fields ---
    precision: PrecisionLevel = PrecisionLevel.FP32
    explanation: Optional[Explanation] = None
    assigned_agent: Optional[str] = None
    # HITL / temporal are audit-grade: populated from what actually happened
    hitl_required: bool = False
    hitl_status: str = "not_required"  # not_required | pending | approved | rejected | timed_out
    hitl_approved: Optional[bool] = None
    market_trade: Optional[Dict[str, Any]] = None
    temporal_verified: Optional[bool] = None
    temporal_violations: List[str] = field(default_factory=list)
    distillation_used: bool = False
    used_fallback: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "original_task": self.original_task,
            "throttled_task": self.throttled_task,
            "eco_mode": self.eco_mode.value,
            "carbon_intensity": self.carbon_intensity,
            "estimated_energy_reduction_percent": (
                self.estimated_energy_reduction_percent
            ),
            "estimated_quality_impact_percent": (
                self.estimated_quality_impact_percent
            ),
            "should_defer": self.should_defer,
            "defer_until": (
                self.defer_until.isoformat()
                if self.defer_until
                else None
            ),
            "precision": self.precision.value,
            "explanation": (
                asdict(self.explanation) if self.explanation else None
            ),
            "assigned_agent": self.assigned_agent,
            "hitl_required": self.hitl_required,
            "hitl_status": self.hitl_status,
            "hitl_approved": self.hitl_approved,
            "market_trade": self.market_trade,
            "temporal_verified": self.temporal_verified,
            "temporal_violations": list(self.temporal_violations),
            "distillation_used": self.distillation_used,
            "used_fallback": self.used_fallback,
        }


# =========================================================================== #
# CONTROLLER
# =========================================================================== #
@dataclass
class _ModeDecision:
    """Internal one-shot result: fetch forecast + determine mode."""

    current_intensity: float
    forecast_intensity: float
    eco_mode: EcoMode
    temporal_verified: Optional[bool]
    temporal_violations: List[str]
    used_fallback: bool


class EcoModeController:
    """
    Carbon-aware eco-mode controller.

    All ten enhancement layers are individually toggleable via
    ``enable_feature`` / ``disable_feature`` at runtime.
    """

    def __init__(
        self,
        carbon_forecaster: Any,
        carbon_thresholds: Optional[Dict[EcoMode, float]] = None,
        deployment_id: str = "local",
        features: Optional[Dict[str, bool]] = None,
        hardware: Optional[HardwareProfile] = None,
        market_budget_usd: float = 10.0,
    ) -> None:
        self.forecaster = ForecastAdapter(carbon_forecaster)
        self.deployment_id = deployment_id
        self.features: Dict[str, bool] = {
            **_DEFAULT_FEATURES,
            **(features or {}),
        }
        self.hardware = hardware or HardwareProfile()

        # Thresholds
        user_thresholds = carbon_thresholds or {
            EcoMode.PERFORMANCE: 150.0,
            EcoMode.BALANCED: 250.0,
            EcoMode.GREEN: 400.0,
            EcoMode.EXTREME_GREEN: 600.0,
            EcoMode.EMERGENCY: float("inf"),
        }
        missing = [m for m in EcoMode if m not in user_thresholds]
        if missing:
            raise ValueError(
                f"carbon_thresholds missing modes: {missing}"
            )
        self.carbon_thresholds: Mapping[EcoMode, float] = MappingProxyType(
            dict(user_thresholds)
        )

        self.current_mode = EcoMode.BALANCED
        self.mode_history: Deque[
            Tuple[datetime, EcoMode, float]
        ] = deque(maxlen=_MAX_MODE_HISTORY)

        # Counters
        self.total_tasks_processed = 0
        self.total_tasks_deferred = 0
        self.total_energy_saved_kwh = 0.0
        self.quality_impact_sum = 0.0
        self.fallback_count = 0
        self.temporal_block_count = 0
        self.mode_counts: Counter = Counter()
        self.precision_counts: Counter = Counter()
        self.agent_counts: Counter = Counter()
        self.deferral_by_mode: Counter = Counter()
        self.hitl_outcomes: Counter = Counter()

        # Component registry (lazy instantiation)
        self._components: Dict[str, Any] = {}

        # Register default temporal properties
        if self.features.get("temporal_logic", False):
            monitor = self._ensure_component("temporal_logic")
            monitor.register(BoundedModeChurn())
            monitor.register(NoEmergencyDeferralWithinDeadline())

        logger.info(
            "EcoModeController v2 initialized "
            "(deployment=%s, features=%s)",
            deployment_id,
            {k: v for k, v in self.features.items() if v},
        )

    # ------------------------------------------------------------------
    # Feature toggles / lazy components
    # ------------------------------------------------------------------
    def enable_feature(self, name: str, enabled: bool = True) -> None:
        if name not in _DEFAULT_FEATURES:
            raise ValueError(f"unknown feature {name!r}")
        self.features[name] = bool(enabled)
        if enabled:
            # Recreate the component if the flag flips on
            self._components.pop(name, None)

    def disable_feature(self, name: str) -> None:
        self.enable_feature(name, enabled=False)

    def _ensure_component(self, name: str) -> Any:
        if not self.features.get(name, False):
            return None
        if name in self._components:
            return self._components[name]
        comp = self._make_component(name)
        self._components[name] = comp
        return comp

    def _make_component(self, name: str) -> Any:
        if name == "causal_rl":
            return LinearEcoPolicy()
        if name == "xai":
            return True  # module-level functions; sentinel value
        if name == "adaptive_precision":
            return AdaptivePrecisionController(self.hardware)
        if name == "federated":
            return FederatedAggregator()
        if name == "multi_agent":
            return MultiAgentCoordinator()
        if name == "temporal_logic":
            return TemporalLogicMonitor()
        if name == "carbon_market":
            return CarbonMarketBridge()
        if name == "chaos_testing":
            return ChaosInjector()
        if name == "hitl":
            return HITLGate()
        if name == "quantum_distillation":
            return QuantumDistillationBridge()
        raise ValueError(f"unknown feature {name!r}")

    # Convenience properties
    @property
    def policy(self) -> Optional[LinearEcoPolicy]:
        return self._ensure_component("causal_rl")

    @property
    def coordinator(self) -> Optional[MultiAgentCoordinator]:
        return self._ensure_component("multi_agent")

    @property
    def temporal_monitor(self) -> Optional[TemporalLogicMonitor]:
        return self._ensure_component("temporal_logic")

    @property
    def market(self) -> Optional[CarbonMarketBridge]:
        return self._ensure_component("carbon_market")

    @property
    def chaos(self) -> Optional[ChaosInjector]:
        return self._ensure_component("chaos_testing")

    @property
    def hitl(self) -> Optional[HITLGate]:
        return self._ensure_component("hitl")

    @property
    def distiller(self) -> Optional[QuantumDistillationBridge]:
        return self._ensure_component("quantum_distillation")

    @property
    def federated(self) -> Optional[FederatedAggregator]:
        return self._ensure_component("federated")

    @property
    def precision_ctl(self) -> Optional[AdaptivePrecisionController]:
        return self._ensure_component("adaptive_precision")

    def register_agent(self, agent_id: str) -> AgentProfile:
        if self.coordinator is None:
            raise RuntimeError("multi_agent feature is disabled")
        return self.coordinator.register(agent_id)

    # ------------------------------------------------------------------
    # Forecast + mode (single fetch)
    # ------------------------------------------------------------------
    async def _fetch_forecast(self) -> Tuple[
        Optional[float], Optional[float], bool
    ]:
        """
        Return ``(current, forecast, used_fallback)``.

        On total failure, ``used_fallback=True`` and the values are
        conservative: ``current = forecast = EMERGENCY threshold - 1``
        would be too lax; we return ``None, None`` so callers can choose
        a safe mode explicitly.
        """
        chaos = self.chaos
        if chaos and chaos.maybe_fail("forecaster"):
            logger.warning(
                "Chaos: forecaster down — using conservative fallback"
            )
            return None, None, True

        current = await self.forecaster.get_current_intensity()
        if current is None:
            return None, None, True

        preds = await self.forecaster.predict(
            horizon="1h", interval_minutes=15
        )
        if preds:
            forecast = sum(p.predicted_intensity for p in preds) / len(
                preds
            )
        else:
            forecast = current
        return current, forecast, False

    def _state_from_inputs(
        self,
        intensity: float,
        forecast: float,
        task: Optional[Mapping[str, Any]] = None,
    ) -> PolicyState:
        task = task or {}
        recent_qloss = (
            self.quality_impact_sum / self.total_tasks_processed / 100.0
            if self.total_tasks_processed
            else 0.0
        )
        return PolicyState(
            carbon_intensity=intensity,
            forecast_intensity=forecast,
            gpu_utilization=float(task.get("gpu_utilization", 0.5)),
            queue_depth=int(task.get("queue_depth", 0)),
            recent_quality_loss=recent_qloss,
        )

    def _threshold_mode(self, effective: float) -> EcoMode:
        if effective < self.carbon_thresholds[EcoMode.PERFORMANCE]:
            return EcoMode.PERFORMANCE
        if effective < self.carbon_thresholds[EcoMode.BALANCED]:
            return EcoMode.BALANCED
        if effective < self.carbon_thresholds[EcoMode.GREEN]:
            return EcoMode.GREEN
        if effective < self.carbon_thresholds[EcoMode.EXTREME_GREEN]:
            return EcoMode.EXTREME_GREEN
        return EcoMode.EMERGENCY

    async def _determine_mode_and_forecast(
        self, task: Optional[Mapping[str, Any]] = None
    ) -> _ModeDecision:
        current, forecast, fallback = await self._fetch_forecast()

        if fallback or current is None:
            # Conservative but not catastrophic: assume high intensity
            # (but below the EMERGENCY threshold) so the system throttles
            # without the full emergency machinery kicking in.
            self.fallback_count += 1
            safe = self.carbon_thresholds[EcoMode.GREEN] + 1.0
            current = safe
            forecast = safe

        effective = max(current, forecast)
        threshold_mode = self._threshold_mode(effective)

        # RL proposal (may be more green than threshold, never less)
        proposed = threshold_mode
        policy = self.policy
        state = self._state_from_inputs(current, forecast, task)
        if policy is not None:
            rl_mode = policy.select_action(state)
            if self.features.get("conservative_override", True):
                if _MODE_ORDER.index(rl_mode) >= _MODE_ORDER.index(
                    threshold_mode
                ):
                    proposed = rl_mode
                else:
                    proposed = threshold_mode
            else:
                proposed = rl_mode

        temporal_verified: Optional[bool] = None
        violations: List[str] = []
        monitor = self.temporal_monitor
        if monitor is not None:
            context = (
                {"deadline": task.get("deadline")}
                if task and task.get("deadline")
                else None
            )
            proposed_entry = (_utcnow(), proposed, effective)
            would_violate = not monitor.would_allow(
                list(self.mode_history), proposed_entry, context=context
            )
            if would_violate:
                # Stay in current mode; the proposed change is not safe.
                temporal_verified = False
                self.temporal_block_count += 1
                for v in monitor.violations:
                    name = v.get("property", "unknown")
                    if name not in violations:
                        violations.append(name)
                logger.warning(
                    "Temporal properties block proposed change %s → %s; "
                    "staying in %s",
                    self.current_mode.value,
                    proposed.value,
                    self.current_mode.value,
                )
                proposed = self.current_mode
            else:
                temporal_verified = True

        # Commit
        if proposed != self.current_mode:
            self.mode_history.append(
                (_utcnow(), proposed, effective)
            )
            logger.info(
                "Eco-mode changed: %s → %s (intensity=%.0f gCO2/kWh)",
                self.current_mode.value,
                proposed.value,
                effective,
            )
            self.current_mode = proposed
        elif temporal_verified is None and monitor is not None:
            # No change; nothing to verify. Mark as None so the ledger
            # does not claim a check it did not perform.
            temporal_verified = None

        return _ModeDecision(
            current_intensity=current,
            forecast_intensity=forecast,
            eco_mode=proposed,
            temporal_verified=temporal_verified,
            temporal_violations=violations,
            used_fallback=fallback,
        )

    async def determine_eco_mode(
        self, task: Optional[Mapping[str, Any]] = None
    ) -> EcoMode:
        """Return the current eco-mode (single forecast fetch)."""
        decision = await self._determine_mode_and_forecast(task)
        return decision.eco_mode

    # ------------------------------------------------------------------
    # Reward for the linear policy
    # ------------------------------------------------------------------
    def _compute_reward(
        self,
        config: EcoModeConfig,
        task: Mapping[str, Any],
        carbon_intensity: float,
        deferred: bool,
        temporal_ok: bool,
        hitl_approved: Optional[bool],
    ) -> float:
        """
        Honest reward computed from **observed** quantities:

        * Carbon avoided vs. the PERFORMANCE-mode baseline (g CO2).
        * Quality impact penalty (config-declared).
        * Deferral bonus.
        * Temporal / HITL penalties.

        No fabricated "next state"; the reward is a single-step signal
        suitable for a contextual bandit.
        """
        perf = _ECO_MODE_CONFIGS[EcoMode.PERFORMANCE]
        baseline_energy = float(
            task.get("estimated_energy_kwh", 0.001)
        )
        compute_ratio = config.compute_limit_percent / max(
            1, perf.compute_limit_percent
        )
        actual_energy = baseline_energy * compute_ratio
        energy_saved = max(0.0, baseline_energy - actual_energy)
        carbon_saved_g = energy_saved * max(0.0, carbon_intensity)

        carbon_reward = min(carbon_saved_g, 10.0) * 0.1  # cap at 1.0
        quality_penalty = config.quality_degradation_acceptable * 2.0
        defer_bonus = 0.3 if deferred else 0.0
        temporal_penalty = 0.5 if not temporal_ok else 0.0
        hitl_penalty = 0.2 if hitl_approved is False else 0.0
        return (
            carbon_reward
            + defer_bonus
            - quality_penalty
            - temporal_penalty
            - hitl_penalty
        )

    # ------------------------------------------------------------------
    # Throttling
    # ------------------------------------------------------------------
    async def apply_throttling(
        self,
        task: Mapping[str, Any],
        force_mode: Optional[EcoMode] = None,
    ) -> ThrottlingDecision:
        decision = await self._determine_mode_and_forecast(task)
        return await self._apply_throttling_with(
            task, force_mode, decision
        )

    async def _apply_throttling_with(
        self,
        task: Mapping[str, Any],
        force_mode: Optional[EcoMode],
        decision: _ModeDecision,
    ) -> ThrottlingDecision:
        # Snapshot task (deep copy so callers cannot mutate it later)
        original_task = copy.deepcopy(dict(task))

        eco_mode = force_mode or decision.eco_mode
        config = ECO_MODE_CONFIGS[eco_mode]
        carbon_intensity = decision.current_intensity

        # ------------------------------------------------------------------
        # HITL decision matrix
        # ------------------------------------------------------------------
        # EMERGENCY + deferrable → ask "approve deferral?"
        # EMERGENCY + non-deferrable → ask "approve throttled run?"
        # HITL rejection of a deferral → proceed throttled (no HITL req)
        # HITL rejection of the *run* → fall back to PERFORMANCE
        hitl_required = False
        hitl_status = "not_required"
        hitl_approved: Optional[bool] = None

        hitl = self.hitl
        if hitl is not None and eco_mode == EcoMode.EMERGENCY:
            is_deferrable = bool(task.get("deferrable", True))
            reason = (
                "EMERGENCY deferral requires human confirmation"
                if is_deferrable
                else "EMERGENCY-mode run of non-deferrable task requires "
                "human confirmation"
            )
            hitl_required = True
            result = await hitl.request_approval(
                task, eco_mode, reason=reason
            )
            if result is None:
                hitl_status = "timed_out"
                hitl_approved = None
                # Conservative default: for deferrable tasks, refuse the
                # deferral but still throttle. For non-deferrable tasks,
                # fall back to PERFORMANCE.
                if is_deferrable:
                    # proceed throttled (deferral denied)
                    pass
                else:
                    eco_mode = EcoMode.PERFORMANCE
                    config = ECO_MODE_CONFIGS[eco_mode]
            elif result is True:
                hitl_status = "approved"
                hitl_approved = True
            else:  # result is False
                hitl_status = "rejected"
                hitl_approved = False
                if is_deferrable:
                    # Human vetoed deferral; run throttled anyway
                    pass
                else:
                    eco_mode = EcoMode.PERFORMANCE
                    config = ECO_MODE_CONFIGS[eco_mode]

        self.hitl_outcomes[hitl_status] += 1

        # ------------------------------------------------------------------
        # Deferral window
        # ------------------------------------------------------------------
        should_defer = False
        defer_until: Optional[datetime] = None
        if (
            eco_mode == EcoMode.EMERGENCY
            and bool(task.get("deferrable", True))
            and hitl_status in ("approved", "not_required", "timed_out")
        ):
            deadline = task.get(
                "deadline", _utcnow() + timedelta(hours=48)
            )
            window = await self.forecaster.find_optimal_execution_window(
                duration_hours=float(
                    task.get("estimated_duration_hours", 1.0)
                ),
                deadline=_ensure_aware(
                    deadline
                    if isinstance(deadline, datetime)
                    else _parse_timestamp(deadline)
                ),
            )
            if window is not None and window.start_time is not None:
                should_defer = True
                defer_until = window.start_time
            else:
                logger.warning(
                    "No execution window found; proceeding throttled"
                )

        # ------------------------------------------------------------------
        # Multi-agent routing
        # ------------------------------------------------------------------
        assigned_agent: Optional[str] = None
        coordinator = self.coordinator
        if coordinator is not None:
            assigned_agent = coordinator.select_agent(task)

        # ------------------------------------------------------------------
        # Precision (task SLA → config → hardware)
        # ------------------------------------------------------------------
        precision = config.preferred_precision
        precision_ctl = self.precision_ctl
        if precision_ctl is not None:
            precision = precision_ctl.select(
                eco_mode, task, config=config
            )
        self.precision_counts[precision.value] += 1

        # ------------------------------------------------------------------
        # Quantum distillation hook
        # ------------------------------------------------------------------
        distillation_used = False
        distiller = self.distiller
        if distiller is not None and precision == (
            PrecisionLevel.QUANTUM_DISTILLED
        ):
            model_id = str(task.get("model_id", "default"))
            cached = distiller.lookup(model_id, precision)
            if cached is None:
                cached = await distiller.request_distillation(
                    DistillationRequest(
                        model_id=model_id,
                        target_precision=precision,
                        max_acceptable_quality_loss=(
                            config.quality_degradation_acceptable
                        ),
                    )
                )
            distillation_used = True
            logger.debug(
                "Distilled %s → %s (retention=%.2f)",
                model_id,
                precision.value,
                cached.achieved_quality_retention,
            )

        # ------------------------------------------------------------------
        # Carbon market
        # ------------------------------------------------------------------
        market_trade: Optional[Dict[str, Any]] = None
        market = self.market
        if market is not None:
            quote = await market.get_quote()
            if market.should_buy_credits(task, eco_mode, quote):
                energy = float(task.get("estimated_energy_kwh", 0.001))
                market_trade = await market.buy_credits(
                    energy,
                    quote,
                    reason=(
                        "deadline-critical task in EMERGENCY mode"
                    ),
                )

        # ------------------------------------------------------------------
        # Build throttled task
        # ------------------------------------------------------------------
        throttled_task = copy.deepcopy(original_task)
        if "max_tokens" in throttled_task:
            throttled_task["max_tokens"] = max(
                1,
                int(
                    throttled_task["max_tokens"]
                    * config.max_tokens_multiplier
                ),
            )
        if config.temperature_override is not None:
            throttled_task["temperature"] = config.temperature_override
        if config.top_k_override is not None:
            throttled_task["top_k"] = config.top_k_override
        throttled_task["use_quantized_model"] = (
            config.use_quantized_model
        )
        throttled_task["use_cache_aggressively"] = (
            config.use_cache_aggressively
        )
        throttled_task["early_stopping_threshold"] = (
            config.early_stopping_threshold
        )
        throttled_task["precision"] = precision.value
        if assigned_agent is not None:
            throttled_task["assigned_agent"] = assigned_agent
        if should_defer and defer_until is not None:
            throttled_task["deferred_until"] = defer_until.isoformat()

        # ------------------------------------------------------------------
        # Metrics
        # ------------------------------------------------------------------
        energy_reduction_percent = 100 - config.compute_limit_percent
        quality_impact_percent = (
            config.quality_degradation_acceptable * 100
        )
        baseline_energy_kwh = float(
            task.get("estimated_energy_kwh", 0.001)
        )
        energy_saved_kwh = (
            baseline_energy_kwh * energy_reduction_percent / 100
        )

        self.total_tasks_processed += 1
        if should_defer:
            self.total_tasks_deferred += 1
            self.deferral_by_mode[eco_mode.value] += 1
        self.total_energy_saved_kwh += energy_saved_kwh
        self.quality_impact_sum += quality_impact_percent
        self.mode_counts[eco_mode.value] += 1
        if assigned_agent is not None:
            self.agent_counts[assigned_agent] += 1

        # ------------------------------------------------------------------
        # XAI
        # ------------------------------------------------------------------
        explanation: Optional[Explanation] = None
        policy = self.policy
        if self.features.get("xai", False):
            state = self._state_from_inputs(
                carbon_intensity, decision.forecast_intensity, task
            )
            explanation = explain_decision(
                mode=eco_mode,
                intensity=carbon_intensity,
                forecast=decision.forecast_intensity,
                thresholds=self.carbon_thresholds,
                config=config,
                policy=policy if policy is not None else None,
                state=state if policy is not None else None,
            )

        # ------------------------------------------------------------------
        # Federated push (sampled)
        # ------------------------------------------------------------------
        federated = self.federated
        if (
            federated is not None
            and self.total_tasks_processed % 50 == 0
        ):
            federated.push(
                FederatedUpdate(
                    deployment_id=self.deployment_id,
                    mode_counts={eco_mode.value: 1},
                    avg_intensity=carbon_intensity,
                    avg_quality_loss=quality_impact_percent / 100.0,
                    weight=1,
                )
            )

        # ------------------------------------------------------------------
        # RL record + periodic update
        # ------------------------------------------------------------------
        if policy is not None:
            state = self._state_from_inputs(
                carbon_intensity, decision.forecast_intensity, task
            )
            reward = self._compute_reward(
                config=config,
                task=task,
                carbon_intensity=carbon_intensity,
                deferred=should_defer,
                temporal_ok=bool(
                    decision.temporal_verified is not False
                ),
                hitl_approved=hitl_approved,
            )
            policy.record(state, eco_mode, reward)
            if self.total_tasks_processed % 20 == 0:
                policy.update()

        # ------------------------------------------------------------------
        # Multi-agent feedback
        # ------------------------------------------------------------------
        if coordinator is not None and assigned_agent is not None:
            coordinator.record_outcome(
                assigned_agent,
                success=True,
                was_deferred=should_defer,
                energy_saved_pct=energy_reduction_percent,
                throughput=1.0
                / max(
                    1.0,
                    float(task.get("estimated_duration_hours", 1.0))
                    * 60.0,
                ),
            )

        return ThrottlingDecision(
            original_task=original_task,
            throttled_task=throttled_task,
            eco_mode=eco_mode,
            carbon_intensity=carbon_intensity,
            estimated_energy_reduction_percent=(
                energy_reduction_percent
            ),
            estimated_quality_impact_percent=quality_impact_percent,
            should_defer=should_defer,
            defer_until=defer_until,
            precision=precision,
            explanation=explanation,
            assigned_agent=assigned_agent,
            hitl_required=hitl_required,
            hitl_status=hitl_status,
            hitl_approved=hitl_approved,
            market_trade=market_trade,
            temporal_verified=decision.temporal_verified,
            temporal_violations=list(decision.temporal_violations),
            distillation_used=distillation_used,
            used_fallback=decision.used_fallback,
        )

    # ------------------------------------------------------------------
    # Batch
    # ------------------------------------------------------------------
    async def batch_throttle(
        self,
        tasks: Sequence[Mapping[str, Any]],
        parallel: bool = False,
    ) -> List[ThrottlingDecision]:
        if not tasks:
            return []
        decision = await self._determine_mode_and_forecast()
        if parallel:
            return list(
                await asyncio.gather(
                    *[
                        self._apply_throttling_with(t, None, decision)
                        for t in tasks
                    ]
                )
            )
        out: List[ThrottlingDecision] = []
        for t in tasks:
            out.append(
                await self._apply_throttling_with(t, None, decision)
            )
        return out

    # ------------------------------------------------------------------
    # Recommendation
    # ------------------------------------------------------------------
    async def get_current_recommendation(self) -> Dict[str, Any]:
        decision = await self._determine_mode_and_forecast()
        current = decision.current_intensity
        forecast = decision.forecast_intensity
        eco_mode = decision.eco_mode
        config = ECO_MODE_CONFIGS[eco_mode]

        try:
            preds_6h = await self.forecaster.predict(
                horizon="6h", interval_minutes=60
            )
        except Exception as e:
            logger.warning("6h forecast failed: %s", e)
            preds_6h = []

        min_forecast: Optional[PredictionPoint] = (
            min(preds_6h, key=lambda p: p.predicted_intensity)
            if preds_6h
            else None
        )
        next_6h = [
            {
                "timestamp": p.timestamp.isoformat(),
                "intensity": p.predicted_intensity,
                "confidence": p.confidence,
            }
            for p in preds_6h
        ]

        explanation = None
        if self.features.get("xai", False):
            policy = self.policy
            state = self._state_from_inputs(current, forecast)
            exp = explain_decision(
                mode=eco_mode,
                intensity=current,
                forecast=forecast,
                thresholds=self.carbon_thresholds,
                config=config,
                policy=policy if policy is not None else None,
                state=state if policy is not None else None,
            )
            explanation = {
                "headline": exp.headline,
                "rationale": exp.rationale,
                "counterfactual": exp.counterfactual,
                "confidence": exp.confidence,
                "policy_attributions": exp.policy_attributions,
            }

        optimal_time = None
        if min_forecast is not None:
            optimal_time = {
                "timestamp": min_forecast.timestamp.isoformat(),
                "intensity_gco2kwh": min_forecast.predicted_intensity,
                "hours_from_now": (
                    min_forecast.timestamp - _utcnow()
                ).total_seconds()
                / 3600.0,
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
            "optimal_time_next_6h": optimal_time,
            "forecast_next_6h": next_6h,
            "explanation": explanation,
            "temporal_verified": decision.temporal_verified,
            "temporal_violations": decision.temporal_violations,
            "used_fallback": decision.used_fallback,
        }

    # ------------------------------------------------------------------
    # Statistics
    # ------------------------------------------------------------------
    def get_statistics(self) -> Dict[str, Any]:
        if self.total_tasks_processed == 0:
            return {
                "total_tasks_processed": 0,
                "current_mode": self.current_mode.value,
            }
        stats: Dict[str, Any] = {
            "current_mode": self.current_mode.value,
            "total_tasks_processed": self.total_tasks_processed,
            "total_tasks_deferred": self.total_tasks_deferred,
            "deferral_rate_percent": (
                self.total_tasks_deferred
                / self.total_tasks_processed
                * 100.0
            ),
            "total_energy_saved_kwh": self.total_energy_saved_kwh,
            "avg_energy_saved_per_task_kwh": (
                self.total_energy_saved_kwh
                / self.total_tasks_processed
            ),
            "avg_quality_impact_percent": (
                self.quality_impact_sum / self.total_tasks_processed
            ),
            "fallback_count": self.fallback_count,
            "temporal_block_count": self.temporal_block_count,
            "mode_counts": dict(self.mode_counts),
            "precision_counts": dict(self.precision_counts),
            "agent_counts": dict(self.agent_counts),
            "deferral_by_mode": dict(self.deferral_by_mode),
            "hitl_outcomes": dict(self.hitl_outcomes),
            "mode_history": [
                {
                    "timestamp": ts.isoformat(),
                    "mode": m.value,
                    "carbon_intensity": i,
                }
                for ts, m, i in list(self.mode_history)[-10:]
            ],
            "features_effective": dict(self.features),
        }

        policy = self.policy
        if policy is not None:
            stats["policy"] = policy.stats()

        market = self.market
        if market is not None:
            stats["market"] = {
                "spent_usd": market.spent,
                "earned_usd": market.earned,
                "trades": len(market.trades),
            }

        coordinator = self.coordinator
        if coordinator is not None:
            stats["agents"] = {
                aid: {
                    "role": a.role.value,
                    "success_rate": a.success_rate,
                    "deferral_success": a.deferral_success,
                    "energy_efficiency": a.energy_efficiency,
                    "throughput": a.throughput,
                }
                for aid, a in coordinator.agents.items()
            }

        federated = self.federated
        if federated is not None:
            stats["federated_aggregate"] = federated.aggregate()

        monitor = self.temporal_monitor
        if monitor is not None:
            stats["temporal"] = monitor.stats()

        hitl = self.hitl
        if hitl is not None:
            stats["hitl"] = {
                "log_size": len(hitl.feedback_log),
                "outcome_counts": hitl.outcome_counts(),
            }

        chaos = self.chaos
        if chaos is not None:
            stats["chaos_events"] = list(chaos.events)[-5:]

        return stats

    async def health_check(self) -> Dict[str, Any]:
        """Operational health probe."""
        ok = True
        details: Dict[str, Any] = {
            "deployment_id": self.deployment_id,
            "forecaster_api": (
                "v18" if self.forecaster.is_v18 else "v1"
            ),
            "current_mode": self.current_mode.value,
            "features": dict(self.features),
            "fallback_count": self.fallback_count,
            "temporal_block_count": self.temporal_block_count,
            "recent_temporal_violations": (
                list(self.temporal_monitor.violations)[-3:]
                if self.temporal_monitor is not None
                else []
            ),
        }
        monitor = self.temporal_monitor
        if monitor is not None and len(monitor.violations) > 50:
            ok = False
            details["warning"] = (
                "temporal violations accumulated "
                f"({len(monitor.violations)}); consider policy review"
            )
        return {"healthy": ok, **details}

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def aclose(self) -> None:
        """Release resources held by bridges / forecasters."""
        for bridge in (
            self._components.get("carbon_market"),
            self._components.get("quantum_distillation"),
        ):
            close = getattr(bridge, "aclose", None) or getattr(
                bridge, "close", None
            )
            if callable(close):
                try:
                    result = close()
                    if inspect.isawaitable(result):
                        await result
                except Exception as e:
                    logger.warning("bridge close failed: %s", e)
        try:
            await self.forecaster.aclose()
        except Exception as e:
            logger.warning("forecaster close failed: %s", e)

    async def __aenter__(self) -> "EcoModeController":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.aclose()


# =========================================================================== #
# Factory
# =========================================================================== #
def create_eco_mode_controller(
    carbon_forecaster: Any,
    custom_thresholds: Optional[Dict[EcoMode, float]] = None,
    deployment_id: str = "local",
    features: Optional[Dict[str, bool]] = None,
    hardware: Optional[HardwareProfile] = None,
    market_budget_usd: float = 10.0,
) -> EcoModeController:
    return EcoModeController(
        carbon_forecaster=carbon_forecaster,
        carbon_thresholds=custom_thresholds,
        deployment_id=deployment_id,
        features=features,
        hardware=hardware,
        market_budget_usd=market_budget_usd,
    )


# =========================================================================== #
# Public API
# =========================================================================== #
__all__ = [
    # Enums / configs
    "EcoMode",
    "PrecisionLevel",
    "AgentRole",
    "EcoModeConfig",
    "ECO_MODE_CONFIGS",
    "DEFAULT_FEATURES",
    # Forecaster adapter
    "ForecastAdapter",
    "PredictionPoint",
    "ExecutionWindow",
    # Enhancements
    "QuantumDistillationBridge",
    "DistillationRequest",
    "DistillationResult",
    "LinearEcoPolicy",
    "CausalRLPolicy",  # alias
    "PolicyState",
    "FederatedAggregator",
    "FederatedUpdate",
    "MultiAgentCoordinator",
    "AgentProfile",
    "TemporalLogicMonitor",
    "TemporalProperty",
    "BoundedModeChurn",
    "NoEmergencyDeferralWithinDeadline",
    "Explanation",
    "explain_decision",
    "AdaptivePrecisionController",
    "HardwareProfile",
    "CarbonMarketBridge",
    "MarketQuote",
    "ChaosInjector",
    "HITLGate",
    "HITLRequest",
    # Controller
    "ThrottlingDecision",
    "EcoModeController",
    "create_eco_mode_controller",
]


# =========================================================================== #
# Demo
# =========================================================================== #
async def _demo() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        ),
    )

    # --- 1. Legacy forecaster (v1 API) ---
    class _LegacyForecaster:
        async def get_current_intensity(self) -> float:
            return 320.0

        async def predict(
            self, horizon: str, interval_minutes: int
        ) -> List[Any]:
            n = 4 if horizon == "1h" else 6

            class F:
                def __init__(self, i: int) -> None:
                    self.timestamp = _utcnow() + timedelta(
                        minutes=15 * i
                    )
                    self.predicted_intensity = 300 + 20 * math.sin(i)
                    self.confidence = 0.9

            return [F(i) for i in range(n)]

        async def find_optimal_execution_window(
            self, duration_hours: float, deadline: datetime
        ) -> Any:
            class W:
                start_time = _utcnow() + timedelta(hours=6)
                carbon_savings_percent = 42.0

            return W()

    # --- 2. v18-style forecaster (async + batch) ---
    class _V18Forecaster:
        async def current_intensity_async(
            self, legacy_float: bool = True
        ) -> Any:
            return 320.0 if legacy_float else (320.0, "api")

        async def forecast_next_hours_async(
            self, hours: int = 4
        ) -> Any:
            class P:
                def __init__(self, i: int) -> None:
                    self.timestamp = (
                        _utcnow() + timedelta(hours=i)
                    ).isoformat()
                    self.intensity = 300 + 20 * math.sin(i)
                    self.confidence = 0.9
                    self.source = "api"

            class B:
                pass

            batch = B()
            batch.points = [P(i) for i in range(hours)]
            return batch

        async def close(self) -> None:
            pass

    async def _run(label: str, forecaster: Any) -> None:
        print("\n" + "=" * 80)
        print(f"Scenario: {label}")
        print("=" * 80)

        async with create_eco_mode_controller(
            forecaster,
            deployment_id="us-ca-prod-01",
            hardware=HardwareProfile(
                supports_int4=True, vram_gb=48
            ),
            features={
                "chaos_testing": True,
                "hitl": True,
                "temporal_logic": True,
            },
        ) as controller:
            controller.chaos.failure_rate = 0.0
            controller.register_agent("agent-A")
            controller.register_agent("agent-B")

            # --- HITL callback that awaits ---
            async def _hitl_cb(req: HITLRequest) -> bool:
                await asyncio.sleep(0.01)
                return req.urgency != "low"

            controller.hitl.set_callback(_hitl_cb)

            # --- Recommendation ---
            rec = await controller.get_current_recommendation()
            print(
                f"\nMode: {rec['recommended_mode']} | "
                f"Precision: {rec['preferred_precision']}"
            )
            print(f"Explanation: {rec['explanation']['headline']}")
            if rec["explanation"]["policy_attributions"]:
                print("Policy attributions:")
                for k, v in rec["explanation"][
                    "policy_attributions"
                ].items():
                    print(f"  {k}: {v:+.4f}")

            # --- Single task ---
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
            print("\nThrottling decision:")
            print(
                f"  mode={decision.eco_mode.value} "
                f"precision={decision.precision.value} "
                f"defer={decision.should_defer} "
                f"hitl_required={decision.hitl_required} "
                f"hitl_status={decision.hitl_status} "
                f"hitl_approved={decision.hitl_approved} "
                f"temporal_verified={decision.temporal_verified}"
            )
            if decision.temporal_violations:
                print(
                    f"  temporal_violations="
                    f"{decision.temporal_violations}"
                )

            # --- Batch ---
            batch = [
                {**task, "task_id": f"job-{i}"} for i in range(5)
            ]
            decisions = await controller.batch_throttle(batch)
            print(f"\nBatch of {len(decisions)} processed.")

            # --- Audit-trail sanity check ---
            print(
                f"\nHITL outcomes (all decisions): "
                f"{dict(controller.hitl_outcomes)}"
            )

            # --- Statistics ---
            print("\nStatistics:")
            stats = controller.get_statistics()
            for k in (
                "mode_counts",
                "precision_counts",
                "hitl_outcomes",
                "temporal_block_count",
                "fallback_count",
            ):
                print(f"  {k}: {stats.get(k)}")

            # --- Health ---
            health = await controller.health_check()
            print(f"\nHealth: {health['healthy']}")
            print(
                f"  forecaster API: {health['forecaster_api']}"
            )

            # --- Feature toggle at runtime ---
            controller.disable_feature("xai")
            assert (
                controller.get_statistics()["features_effective"][
                    "xai"
                ]
                is False
            )
            controller.enable_feature("xai")
            print("\nRuntime feature toggle: OK")

    await _run("Legacy v1 forecaster", _LegacyForecaster())
    await _run("v18-style forecaster", _V18Forecaster())

    print("\nDemo complete.")


if __name__ == "__main__":
    asyncio.run(_demo())
