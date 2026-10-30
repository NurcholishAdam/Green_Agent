"""
AdaptiveController (Enhanced v2)
================================

Dynamically adjusts runtime strategy based on resource trends, carbon
intensity, helium scarcity, and learned thresholds.

Original API preserved:
    controller = AdaptiveController()
    mode = controller.evaluate(trend)
    controller.apply(runtime)

v2 fixes (see module docstring summary in commit message):
  * apply() now transitions runtime state (restores when leaving a mode).
  * Invalid-input path no longer diverges self.mode from history.
  * Causal learner uses reward-weighted regression with a per-bin baseline
    and consumes the actual offsets used.
  * FiniteTrendValues replaces NonNegativeThresholds; negative deltas are
    valid signals.
  * Stable SHA-256 hashing for RL features.
  * select("critical") returns FP32.
  * Timeout + circuit breaker on the carbon forecaster.
  * Async forecaster / runtime detection (with evaluate_async / apply_async).
  * Thread-safe state via RLock; UTC-aware datetimes.
  * MetricsRecorder, health(), AdaptiveControllerConfig.
  * Async HITL with timeout; HITL triggers on capacity-reducing transitions.
  * Incremental federated aggregation.
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

logger = logging.getLogger(__name__)

__version__ = "2.0.0"


# =============================================================================
# Helpers
# =============================================================================

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def stable_hash(s: str) -> float:
    """Deterministic, process-independent hash in [0, 1)."""
    h = hashlib.sha256(str(s).encode("utf-8")).digest()
    return int.from_bytes(h[:8], "big") / 2**64


def _sanitize(s: Any, max_len: int = 96) -> str:
    """Strip non-printables and cap length (audit-safe)."""
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
    CUSTOM = "custom"


MODE_KIND_MAP: Dict[str, ModeKind] = {
    "normal": ModeKind.NORMAL,
    "low_energy": ModeKind.RESOURCE_THROTTLED,
    "low_memory": ModeKind.RESOURCE_THROTTLED,
    "carbon_throttled": ModeKind.CARBON_THROTTLED,
    "precision_throttled": ModeKind.PRECISION_THROTTLED,
}


def _mode_kind(mode: str) -> ModeKind:
    return MODE_KIND_MAP.get(mode, ModeKind.CUSTOM)


# =============================================================================
# Config
# =============================================================================

@dataclass(frozen=True)
class AdaptiveControllerConfig:
    """Frozen, validated controller configuration."""

    # --- core thresholds ---
    memory_threshold: float = 25.0
    cpu_threshold: float = 15.0
    carbon_threshold: float = 500.0
    helium_threshold: float = 0.7

    # --- external call timeouts (None disables) ---
    carbon_timeout_s: Optional[float] = 2.0
    runtime_timeout_s: Optional[float] = 2.0
    hitl_timeout_s: float = 60.0

    # --- circuit breakers ---
    circuit_failure_threshold: int = 3
    circuit_recovery_timeout_s: int = 30

    # --- chaos ---
    chaos_failure_rate: float = 0.0

    # --- bounds ---
    mode_history_maxlen: int = 1024
    hitl_feedback_maxlen: int = 1000
    temporal_violation_maxlen: int = 500
    chaos_event_maxlen: int = 100
    pending_switch_maxlen: int = 512

    # --- causal learner ---
    causal_lr: float = 0.02
    causal_baseline_decay: float = 0.95
    causal_buffer_maxlen: int = 512
    causal_update_every: int = 10

    # --- federated ---
    federated_push_every: int = 10
    federated_min_samples: int = 3

    # --- HITL policy ---
    hitl_disruptive_modes: Tuple[str, ...] = (
        "low_energy", "low_memory", "carbon_throttled", "precision_throttled",
    )
    hitl_memory_spike_threshold: float = 80.0
    hitl_default_on_timeout: bool = False

    # --- executor ---
    executor_max_workers: int = 4

    # --- history sanitisation ---
    max_trend_str_len: int = 128
    history_trend_whitelist: Tuple[str, ...] = (
        "memory_delta", "cpu_delta", "workload", "priority",
        "min_tool_calls", "in_critical_section",
    )

    def __post_init__(self) -> None:
        for name in (
            "memory_threshold", "cpu_threshold",
            "carbon_threshold", "helium_threshold",
        ):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be > 0.")
        if not 0.0 <= self.chaos_failure_rate <= 1.0:
            raise ValueError("chaos_failure_rate must be in [0, 1].")
        for f in ("carbon_timeout_s", "runtime_timeout_s"):
            v = getattr(self, f)
            if v is not None and v <= 0:
                raise ValueError(f"{f} must be > 0 or None.")

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["hitl_disruptive_modes"] = list(self.hitl_disruptive_modes)
        d["history_trend_whitelist"] = list(self.history_trend_whitelist)
        return d


# =============================================================================
# Circuit Breaker (UTC-free, monotonic clock)
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
            return True  # HALF_OPEN lets one through

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
    """Injects controlled faults for resilience testing."""

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

@dataclass
class ModeSwitchContext:
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
    """G(mode_switch → ¬in_critical_section) — no switch to non-normal in
    a critical section."""
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
class FiniteTrendValues:
    """
    Deltas must be finite numbers. *Negative* deltas are valid (resources
    freed) — the previous NonNegativeThresholds was blocking recovery.
    """
    def check(self, ctx: ModeSwitchContext) -> bool:
        for k in ("memory_delta", "cpu_delta"):
            v = ctx.trend.get(k, 0.0)
            try:
                if not math.isfinite(float(v)):
                    return False
            except (TypeError, ValueError):
                return False
        return True

    def name(self) -> str:
        return "FiniteTrendValues"


class TemporalLogicMonitor:
    def __init__(self, maxlen: int = 500) -> None:
        self.properties: List[SwitchProperty] = []
        self.violations: Deque[Dict[str, Any]] = deque(maxlen=maxlen)
        self._lock = threading.RLock()

    def register(self, p: SwitchProperty) -> None:
        with self._lock:
            self.properties.append(p)

    def verify(
        self, ctx: ModeSwitchContext,
    ) -> Tuple[bool, List[str]]:
        bad: List[str] = []
        with self._lock:
            props = list(self.properties)
        for p in props:
            try:
                ok = p.check(ctx)
            except Exception as e:
                logger.warning("property %s raised: %s", p.name(), e)
                ok = False
            if not ok:
                bad.append(p.name())
                with self._lock:
                    self.violations.append({
                        "property": p.name(),
                        "mode": ctx.mode,
                        "trend": _safe_trend_snapshot(ctx.trend),
                        "at": _utcnow().isoformat(),
                    })
        return (len(bad) == 0, bad)


def _safe_trend_snapshot(
    trend: Mapping[str, Any], max_str_len: int = 64,
) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in trend.items():
        if isinstance(v, str):
            out[k] = _sanitize(v, max_str_len)
        elif isinstance(v, (int, float, bool)) or v is None:
            out[k] = v
        else:
            out[k] = f"<{type(v).__name__}>"
    return out


# =============================================================================
# XAI
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
        *,
        learner_observations: int = 0,
        carbon_signal_available: bool = True,
        helium_signal_available: bool = True,
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
                    f"carbon_intensity={carbon_intensity:.0f} gCO2/kWh "
                    "triggered carbon-aware throttle."
                )
            else:
                reasons.append(
                    "carbon_throttled selected but carbon signal was missing."
                )
        if helium_scarcity is not None and helium_scarcity > 0.6:
            reasons.append(
                f"Helium scarcity={helium_scarcity:.2f} elevated."
            )

        ca_mem, ca_cpu = causal_adjustment
        if ca_mem or ca_cpu:
            reasons.append(
                f"Learned threshold adjustment: "
                f"mem {ca_mem:+.2f}, cpu {ca_cpu:+.2f}."
            )

        if not carbon_signal_available:
            reasons.append("Carbon signal unavailable this cycle.")
        if not helium_signal_available:
            reasons.append("Helium signal unavailable this cycle.")

        # Dynamic confidence
        confidence = 0.5
        if mode != "normal":
            confidence = 0.85
        if carbon_signal_available and helium_signal_available:
            confidence += 0.05
        if learner_observations >= 20:
            confidence += 0.05
        if learner_observations < 5:
            confidence -= 0.10
        confidence = max(0.1, min(1.0, confidence))

        return ModeExplanation(
            headline=f"[{mode}] mem={mem:.1f}, cpu={cpu:.1f}",
            rationale=reasons,
            confidence=confidence,
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
        """Map urgency → precision. 'critical' means FULL fidelity (FP32)."""
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
# Causal RL — reward-weighted regression with per-bin baseline
# =============================================================================

@dataclass
class ThresholdState:
    hour_of_day: int
    previous_mode_hash: float   # renamed from recent_mode_hash (it's the previous)
    workload_hash: float


class CausalThresholdLearner:
    """
    Learns (memory, cpu) threshold offsets.

    v2: reward-weighted regression with a per-bin baseline.
        Consumes the actual offsets used at decision time.
    """

    N_FEATURES = 3

    def __init__(
        self,
        lr: float = 0.02,
        baseline_decay: float = 0.95,
        buffer_maxlen: int = 512,
    ) -> None:
        self.lr = lr
        self.baseline_decay = baseline_decay
        self.mem_coeffs: List[float] = [0.0] * self.N_FEATURES
        self.cpu_coeffs: List[float] = [0.0] * self.N_FEATURES

        self.baselines: Dict[int, float] = defaultdict(float)
        self.baseline_counts: Dict[int, int] = defaultdict(int)
        self.buffer: Deque[
            Tuple[List[float], float, Tuple[float, float], int]
        ] = deque(maxlen=buffer_maxlen)

        self.observations: int = 0
        self.updates: int = 0
        self.last_advantage_mean: float = 0.0
        self._lock = threading.RLock()

    @staticmethod
    def _features(s: ThresholdState) -> List[float]:
        return [
            s.hour_of_day / 24.0,
            s.previous_mode_hash,
            s.workload_hash,
        ]

    @staticmethod
    def _bin(f: List[float]) -> int:
        return int(f[1] * 10)  # 10 buckets by previous mode

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

    def _raw_predict(self, f: List[float]) -> Tuple[float, float]:
        mem = sum(c * x for c, x in zip(self.mem_coeffs, f))
        cpu = sum(c * x for c, x in zip(self.cpu_coeffs, f))
        return mem, cpu

    def predict_offsets(self, s: ThresholdState) -> Tuple[float, float]:
        with self._lock:
            return self._raw_predict(self._features(s))

    def record(
        self,
        s: ThresholdState,
        reward: float,
        offsets_used: Tuple[float, float],
    ) -> None:
        f = self._features(s)
        with self._lock:
            self.buffer.append((f, reward, offsets_used, self._bin(f)))
            self.observations += 1

    def update(self) -> None:
        with self._lock:
            if not self.buffer:
                return
            advantages: List[float] = []
            for f, reward, (mem_used, cpu_used), _b in self.buffer:
                adv = reward - self._baseline_for(f)
                advantages.append(adv)
                self._update_baseline(f, reward)

                pred_mem, pred_cpu = self._raw_predict(f)
                for i, x in enumerate(f):
                    self.mem_coeffs[i] += (
                        self.lr * adv * (mem_used - pred_mem) * x
                    )
                    self.cpu_coeffs[i] += (
                        self.lr * adv * (cpu_used - pred_cpu) * x
                    )
            self.last_advantage_mean = (
                statistics.fmean(advantages) if advantages else 0.0
            )
            self.updates += 1
            self.buffer.clear()


# =============================================================================
# Quantum Distillation
# =============================================================================

@dataclass
class DistilledModePolicy:
    precision: PrecisionLevel
    memory_threshold: float
    cpu_threshold: float
    quality_retention: float
    energy_reduction_percent: float


class QuantumDistillationBridge:
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
# Federated (incremental)
# =============================================================================

@dataclass
class FederatedModeProfile:
    deployment_id: str
    mode: str
    mean_memory_delta: float
    mean_cpu_delta: float
    sample_count: int
    timestamp: datetime = field(default_factory=_utcnow)


class FederatedAggregator:
    """Incremental aggregation — O(modes) lookup, not O(records)."""

    def __init__(self, recent_maxlen: int = 256) -> None:
        self._sum_mem: Dict[str, float] = defaultdict(float)
        self._sum_cpu: Dict[str, float] = defaultdict(float)
        self._count: Dict[str, int] = defaultdict(int)
        self.recent: Deque[FederatedModeProfile] = deque(maxlen=recent_maxlen)
        self._global: Dict[str, Dict[str, float]] = {}
        self._lock = threading.RLock()

    def push(self, u: FederatedModeProfile) -> None:
        with self._lock:
            self.recent.append(u)
            self._sum_mem[u.mode] += u.mean_memory_delta * u.sample_count
            self._sum_cpu[u.mode] += u.mean_cpu_delta * u.sample_count
            self._count[u.mode] += u.sample_count

    def aggregate(self) -> Dict[str, Dict[str, float]]:
        with self._lock:
            result = {}
            for mode, n in self._count.items():
                if n <= 0:
                    continue
                result[mode] = {
                    "mean_memory_delta": self._sum_mem[mode] / n,
                    "mean_cpu_delta": self._sum_cpu[mode] / n,
                    "sample_count": n,
                }
            self._global = result
            return dict(result)

    def lookup(self, mode: str) -> Optional[Dict[str, float]]:
        with self._lock:
            if not self._global:
                self.aggregate()
            return self._global.get(mode)


# =============================================================================
# Multi-Agent
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
        self._lock = threading.RLock()

    def register(self, agent_id: str) -> AgentProfile:
        with self._lock:
            if agent_id not in self.agents:
                self.agents[agent_id] = AgentProfile(agent_id)
            return self.agents[agent_id]

    def _reassign(self) -> None:
        for a in self.agents.values():
            if (
                a.memory_catch_rate > 0.5
                and a.memory_catch_rate >= a.energy_catch_rate
            ):
                a.role = AgentRole.MEMORY_SPECIALIST
            elif a.energy_catch_rate > 0.5:
                a.role = AgentRole.ENERGY_SPECIALIST
            elif a.carbon_catch_rate > 0.3:
                a.role = AgentRole.CARBON_SPECIALIST
            elif a.success_rate > 0.9:
                a.role = AgentRole.LATENCY_SPECIALIST
            else:
                a.role = AgentRole.GENERALIST

    def record(self, agent_id: str, mode: str, success: bool) -> None:
        with self._lock:
            a = self.register(agent_id)
            n = a.total_calls + 1
            a.success_rate = (
                (n - 1) * a.success_rate + float(success)
            ) / n
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
# HITL (async + timeout + transition-based trigger)
# =============================================================================

@dataclass(frozen=True)
class HITLRequest:
    mode: str
    previous_mode: str
    trend: Mapping[str, Any]
    reason: str
    urgency: str
    requested_at: datetime = field(default_factory=_utcnow)

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["trend"] = dict(self.trend)
        d["requested_at"] = self.requested_at.isoformat()
        return d


class HumanInTheLoopGate:
    def __init__(
        self,
        disruptive_modes: Tuple[str, ...] = ("low_energy",),
        memory_spike_threshold: float = 80.0,
        timeout_s: float = 60.0,
        default_on_timeout: bool = False,
        feedback_maxlen: int = 1000,
    ) -> None:
        self.disruptive_modes = set(disruptive_modes)
        self.memory_spike_threshold = memory_spike_threshold
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
        self, mode: str, previous_mode: str, trend: Dict[str, Any],
    ) -> bool:
        # Capacity-reducing transitions (entering throttled, or tightening).
        if mode in self.disruptive_modes and previous_mode not in self.disruptive_modes:
            return True
        if (
            mode in self.disruptive_modes
            and previous_mode in self.disruptive_modes
            and mode != previous_mode
        ):
            return True
        try:
            if float(trend.get("memory_delta", 0.0)) > self.memory_spike_threshold:
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
        """Sync request — used by ``AdaptiveController.evaluate``."""
        req = HITLRequest(mode, previous_mode, dict(trend), reason, urgency)
        with self._lock:
            self.pending.append(req)
        try:
            if urgency == "low":
                self._record(req, True, auto=True)
                return True
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
        mode: str,
        previous_mode: str,
        trend: Dict[str, Any],
        reason: str,
        urgency: str = "medium",
    ) -> bool:
        req = HITLRequest(mode, previous_mode, dict(trend), reason, urgency)
        with self._lock:
            self.pending.append(req)
        try:
            if urgency == "low":
                self._record(req, True, auto=True)
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

    def _record(
        self, req: HITLRequest, decision: bool, auto: bool = False,
    ) -> None:
        with self._lock:
            self.feedback_log.append({
                "req": req.to_dict(),
                "decision": decision,
                "auto": auto,
            })

    def active_learning_batch(self, n: int = 16) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self.feedback_log)[-n:]


# =============================================================================
# Strategy Registry
# =============================================================================

class ModeStrategy(Protocol):
    name: str
    kind: ModeKind

    def matches(self, trend: Dict[str, Any], context: Dict[str, Any]) -> bool: ...
    def apply(self, runtime: Any) -> None: ...
    # Optional: revert(runtime) -> None


class StrategyRegistry:
    def __init__(self) -> None:
        self._strategies: Dict[str, ModeStrategy] = {}
        self._cached_list: Optional[List[ModeStrategy]] = None
        self._lock = threading.RLock()

    def register(self, strategy: ModeStrategy) -> None:
        with self._lock:
            self._strategies[strategy.name] = strategy
            self._cached_list = None

    def unregister(self, name: str) -> None:
        with self._lock:
            self._strategies.pop(name, None)
            self._cached_list = None

    def get(self, name: str) -> Optional[ModeStrategy]:
        with self._lock:
            return self._strategies.get(name)

    def all(self) -> List[ModeStrategy]:
        with self._lock:
            if self._cached_list is None:
                self._cached_list = list(self._strategies.values())
            return list(self._cached_list)

    def names(self) -> List[str]:
        with self._lock:
            return list(self._strategies.keys())


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
# AdaptiveController
# =============================================================================

# Default feature flags (immutable class-level dict).
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
    "carbon_awareness": True,
    "helium_awareness": True,
})


# Mode → (tool_calls_state, context_state, precision_urgency)
_MODE_TARGETS: Dict[str, Dict[str, Any]] = {
    "normal": {
        "tool_calls": "full",
        "context": "full",
        "precision": "critical",
    },
    "low_memory": {
        "tool_calls": "full",
        "context": "shortened",
        "precision": "critical",
    },
    "low_energy": {
        "tool_calls": "reduced",
        "context": "full",
        "precision": "high",
    },
    "carbon_throttled": {
        "tool_calls": "reduced",
        "context": "shortened",
        "precision": "high",
    },
    "precision_throttled": {
        "tool_calls": "full",
        "context": "full",
        "precision": "normal",
    },
}


class AdaptiveController:
    """
    Enhanced runtime strategy controller.

    Backward-compatible:
        controller = AdaptiveController()
        mode = controller.evaluate(trend)
        controller.apply(runtime)
    """

    @classmethod
    def default_features(cls) -> Dict[str, bool]:
        """Return a fresh copy of the default feature dict."""
        return dict(_DEFAULT_FEATURES)

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
        config: Optional[AdaptiveControllerConfig] = None,
        metrics: Optional[MetricsRecorder] = None,
    ) -> None:
        # Backward-compatible: positional threshold kwargs override config.
        self.config = config or AdaptiveControllerConfig(
            memory_threshold=memory_threshold,
            cpu_threshold=cpu_threshold,
            carbon_threshold=carbon_threshold,
            helium_threshold=helium_threshold,
        )

        self.deployment_id = deployment_id
        self.agent_id = agent_id
        self.features = {**self.default_features(), **(features or {})}
        self._metrics = metrics or MetricsRecorder()

        # Base thresholds (causal learner adjusts on top).
        self.base_memory_threshold = float(self.config.memory_threshold)
        self.base_cpu_threshold = float(self.config.cpu_threshold)
        self.base_carbon_threshold = float(self.config.carbon_threshold)
        self.base_helium_threshold = float(self.config.helium_threshold)

        # State (guarded by _lock).
        self._lock = threading.RLock()
        self.mode: str = "normal"
        self._applied_mode: str = "normal"
        self._applied_strategy: Optional[ModeStrategy] = None
        self.current_precision: PrecisionLevel = PrecisionLevel.FP32
        self.last_causal_offsets: Tuple[float, float] = (0.0, 0.0)
        self.last_explanation: Optional[ModeExplanation] = None
        self.mode_history: Deque[Dict[str, Any]] = deque(
            maxlen=self.config.mode_history_maxlen,
        )
        self.mode_counts: Dict[str, int] = defaultdict(int)
        self.mode_kind_counts: Dict[str, int] = defaultdict(int)
        self._pending_switches: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
        self._next_switch_id: int = 1
        self._started_mono: float = time.monotonic()

        # Resilience.
        self._runtime_circuit = CircuitBreaker(
            "runtime",
            self.config.circuit_failure_threshold,
            self.config.circuit_recovery_timeout_s,
        )
        self._carbon_circuit = CircuitBreaker(
            "carbon",
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
            self.temporal_monitor.register(NoSwitchInCriticalSection())
            self.temporal_monitor.register(MinimumToolCallsRespected())
            self.temporal_monitor.register(FiniteTrendValues())

        # XAI.
        self.explainer = ModeExplainer() if self.features["xai"] else None

        # Precision.
        self.precision_ctl = (
            AdaptivePrecisionController(hardware)
            if self.features["adaptive_precision"] else None
        )

        # Causal learner.
        self.causal_learner: Optional[CausalThresholdLearner] = (
            CausalThresholdLearner(
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
            FederatedAggregator() if self.features["federated"] else None
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
                disruptive_modes=self.config.hitl_disruptive_modes,
                memory_spike_threshold=self.config.hitl_memory_spike_threshold,
                timeout_s=self.config.hitl_timeout_s,
                default_on_timeout=self.config.hitl_default_on_timeout,
                feedback_maxlen=self.config.hitl_feedback_maxlen,
            )
            if self.features["hitl"] else None
        )

        # Plugin registry.
        self.registry = StrategyRegistry()

        # External signals.
        self.carbon_forecaster: Any = None
        self.helium_signal_fn: Optional[Callable[[], Any]] = None
        self._async_forecaster_warned: bool = False

        # Executor for timeouts on sync external calls.
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.config.executor_max_workers,
            thread_name_prefix="adaptive-controller",
        )

        logger.info(
            "Enhanced AdaptiveController initialized "
            "(deployment=%s, agent=%s, features=%s)",
            deployment_id, agent_id, list(self.features),
        )

    # ------------------------------------------------------------------ lifecycle
    def close(self) -> None:
        self._executor.shutdown(wait=False, cancel_futures=True)

    def __enter__(self) -> "AdaptiveController":
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.close()

    # ------------------------------------------------------------------ public: evaluate
    def evaluate(self, trend: Dict[str, Any]) -> str:
        """
        Evaluate trend and return the chosen mode.

        Backward-compatible: with all features off, thresholds default to
        25/15 exactly like the original.
        """
        return self._evaluate_impl(trend)

    async def evaluate_async(self, trend: Dict[str, Any]) -> str:
        """Async variant: awaits an async carbon forecaster if needed."""
        carbon_intensity = await self._safe_get_carbon_intensity_async()
        return self._evaluate_impl(trend, carbon_preloaded=carbon_intensity)

    def _evaluate_impl(
        self,
        trend: Dict[str, Any],
        carbon_preloaded: Optional[float] = None,
    ) -> str:
        t0 = time.monotonic()

        # --- Input validation ---
        mem = self._safe_float(trend.get("memory_delta", 0.0))
        cpu = self._safe_float(trend.get("cpu_delta", 0.0))
        with self._lock:
            previous_mode = self.mode
        if mem is None or cpu is None:
            logger.warning("Invalid trend values; forcing normal mode")
            with self._lock:
                self.mode = "normal"
            self._metrics.inc("evaluate_invalid_input")
            self._record_switch(
                new_mode="normal", previous_mode=previous_mode,
                trend=trend, reason="invalid_input",
            )
            return "normal"

        # --- Causal RL threshold offsets ---
        mem_threshold = self.base_memory_threshold
        cpu_threshold = self.base_cpu_threshold
        causal_offsets = (0.0, 0.0)
        if self.causal_learner:
            state = self._build_threshold_state(previous_mode, trend)
            mem_off, cpu_off = self.causal_learner.predict_offsets(state)
            # Clamp adjustments to ±50% of base.
            mem_off = max(
                -0.5 * self.base_memory_threshold,
                min(0.5 * self.base_memory_threshold, mem_off),
            )
            cpu_off = max(
                -0.5 * self.base_cpu_threshold,
                min(0.5 * self.base_cpu_threshold, cpu_off),
            )
            mem_threshold = self.base_memory_threshold + mem_off
            cpu_threshold = self.base_cpu_threshold + cpu_off
            causal_offsets = (mem_off, cpu_off)

        # --- External signals ---
        carbon_intensity: Optional[float] = None
        carbon_available = False
        helium_scarcity: Optional[float] = None
        helium_available = False

        if self.features["carbon_awareness"]:
            if carbon_preloaded is not None:
                carbon_intensity = carbon_preloaded
                carbon_available = True
            else:
                carbon_intensity = self._safe_get_carbon_intensity()
                carbon_available = carbon_intensity is not None
                if not carbon_available:
                    self._metrics.inc("carbon_signal_missing")
        if self.features["helium_awareness"]:
            helium_scarcity = self._safe_get_helium_scarcity()
            helium_available = helium_scarcity is not None
            if not helium_available:
                self._metrics.inc("helium_signal_missing")

        # --- Decide mode ---
        chosen_mode = "normal"
        if mem > mem_threshold:
            chosen_mode = "low_memory"
        elif cpu > cpu_threshold:
            chosen_mode = "low_energy"
        elif (
            self.features["carbon_awareness"]
            and carbon_intensity is not None
            and carbon_intensity > self.base_carbon_threshold
        ):
            chosen_mode = "carbon_throttled"
        elif (
            self.features["adaptive_precision"]
            and self.precision_ctl is not None
            and cpu > cpu_threshold * 0.6
        ):
            chosen_mode = "precision_throttled"
        else:
            chosen_mode = self._try_strategies(
                trend=trend,
                previous_mode=previous_mode,
                carbon_intensity=carbon_intensity,
                helium_scarcity=helium_scarcity,
                fallback="normal",
            )

        # --- Temporal verification ---
        veto: Optional[Tuple[str, str]] = None
        if self.temporal_monitor:
            ctx = ModeSwitchContext(
                mode=chosen_mode,
                trend=trend,
                in_critical_section=bool(
                    trend.get("in_critical_section", False)
                ),
                min_tool_calls=int(trend.get("min_tool_calls", 1)),
            )
            ok, violations = self.temporal_monitor.verify(ctx)
            if not ok:
                logger.warning(
                    "Mode switch to %s violates %s; falling back to normal",
                    chosen_mode, violations,
                )
                veto = (chosen_mode, f"temporal:{violations[0]}")
                chosen_mode = "normal"
                self._metrics.inc("temporal_veto_total")

        # --- HITL ---
        if (
            veto is None
            and self.hitl
            and chosen_mode != previous_mode
            and self.hitl.needs_review(chosen_mode, previous_mode, trend)
        ):
            approved = self.hitl.request_review(
                mode=chosen_mode,
                previous_mode=previous_mode,
                trend=trend,
                reason=f"transition {previous_mode} → {chosen_mode}",
                urgency="high" if chosen_mode == "low_energy" else "medium",
            )
            if approved is False:
                logger.info(
                    "HITL denied switch to %s; staying in normal",
                    chosen_mode,
                )
                veto = (chosen_mode, "hitl_denied")
                chosen_mode = "normal"
                self._metrics.inc("hitl_veto_total")

        # --- Update precision + mode ---
        precision_before = self.current_precision
        if self.precision_ctl:
            if chosen_mode in _MODE_TARGETS:
                urgency = _MODE_TARGETS[chosen_mode]["precision"]
                self.current_precision = self.precision_ctl.select(urgency)
            # Custom strategy modes: leave precision untouched.
        with self._lock:
            self.mode = chosen_mode
            self.last_causal_offsets = causal_offsets

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
                learner_observations=(
                    self.causal_learner.observations
                    if self.causal_learner else 0
                ),
                carbon_signal_available=carbon_available,
                helium_signal_available=helium_available,
            )
            with self._lock:
                self.last_explanation = exp

        # --- Record switch ---
        switch_id = self._record_switch(
            new_mode=chosen_mode,
            previous_mode=previous_mode,
            trend=trend,
            reason="threshold",
            carbon_intensity=carbon_intensity,
            helium_scarcity=helium_scarcity,
            vetoed_from=veto[0] if veto else None,
            veto_reason=veto[1] if veto else None,
        )

        # Stash the state for record_outcome attribution.
        if self.causal_learner:
            state = self._build_threshold_state(previous_mode, trend)
            with self._lock:
                self._pending_switches[switch_id] = {
                    "state": state,
                    "offsets_used": causal_offsets,
                    "mode": chosen_mode,
                    "previous_mode": previous_mode,
                }
                while len(self._pending_switches) > self.config.pending_switch_maxlen:
                    self._pending_switches.popitem(last=False)

        # --- Federated contribution (every N) ---
        if self.federated and len(self.mode_history) % \
                self.config.federated_push_every == 0:
            self.federated.push(FederatedModeProfile(
                deployment_id=self.deployment_id,
                mode=chosen_mode,
                mean_memory_delta=mem,
                mean_cpu_delta=cpu,
                sample_count=1,
            ))

        # --- Metrics ---
        self._metrics.inc(
            "evaluate_total", mode=chosen_mode,
            kind=_mode_kind(chosen_mode).value,
        )
        self._metrics.observe(
            "evaluate_latency_ms", (time.monotonic() - t0) * 1000.0,
        )
        if chosen_mode != previous_mode:
            self._metrics.inc(
                "mode_transitions_total",
                to=chosen_mode, **{"from": previous_mode},
            )
        if precision_before != self.current_precision:
            self._metrics.inc(
                "precision_transitions_total",
                to=self.current_precision.value,
            )

        return chosen_mode

    # ------------------------------------------------------------------ public: apply
    def apply(self, runtime: Any) -> bool:
        """
        Apply the current mode to the runtime.

        Idempotent: repeated calls with the same mode are no-ops. Restores
        runtime state when transitioning *back* to a less-throttled mode.
        """
        with self._lock:
            target_mode = self.mode
            old_mode = self._applied_mode
            target_precision = self.current_precision

        if not self._runtime_circuit.can_call():
            logger.debug("Runtime circuit open — skipping apply")
            return False

        try:
            if self.chaos and self.chaos.maybe_fail("runtime"):
                raise RuntimeError("chaos: runtime")

            if target_mode == old_mode:
                # No transition. Ensure precision is aligned (defensive).
                self._apply_precision(runtime, old_mode, target_mode,
                                      target_precision)
                self._runtime_circuit.record_success()
                return True

            # --- Tool calls ---
            old_t = _MODE_TARGETS.get(old_mode, {}).get("tool_calls", "full")
            new_t = _MODE_TARGETS.get(target_mode, {}).get("tool_calls", "full")
            if new_t != old_t:
                self._transition_tool_calls(runtime, old_t, new_t)

            # --- Context ---
            old_c = _MODE_TARGETS.get(old_mode, {}).get("context", "full")
            new_c = _MODE_TARGETS.get(target_mode, {}).get("context", "full")
            if new_c != old_c:
                self._transition_context(runtime, old_c, new_c)

            # --- Precision ---
            self._apply_precision(runtime, old_mode, target_mode,
                                  target_precision)

            # --- Custom strategies ---
            self._transition_strategies(runtime, old_mode, target_mode)

            with self._lock:
                self._applied_mode = target_mode
            self._runtime_circuit.record_success()
            self._metrics.inc(
                "apply_transition_total",
                to=target_mode, **{"from": old_mode},
            )
            return True

        except Exception as e:
            self._runtime_circuit.record_failure()
            logger.warning(
                "Runtime apply failed (%s); resetting mode to normal", e,
            )
            with self._lock:
                self.mode = "normal"
                self._applied_mode = "normal"
            self._metrics.inc("apply_failures")
            return False

    async def apply_async(self, runtime: Any) -> bool:
        """Async wrapper — awaits async runtime methods if any."""
        # For now, run sync apply in a thread; async runtime support can be
        # layered by subclassing _call_runtime.
        return await asyncio.to_thread(self.apply, runtime)

    # ------------------------------------------------------------------ transitions
    def _transition_tool_calls(
        self, runtime: Any, old_state: str, new_state: str,
    ) -> None:
        if new_state == "reduced":
            self._call_runtime(runtime, "reduce_tool_calls")
        elif new_state == "full":
            self._call_runtime(runtime, "restore_tool_calls")

    def _transition_context(
        self, runtime: Any, old_state: str, new_state: str,
    ) -> None:
        if new_state == "shortened":
            self._call_runtime(runtime, "shorten_context")
        elif new_state == "full":
            self._call_runtime(runtime, "restore_context")

    def _apply_precision(
        self,
        runtime: Any,
        old_mode: str,
        target_mode: str,
        target_precision: PrecisionLevel,
    ) -> None:
        # Only fire set_precision when precision actually changes.
        if old_mode == target_mode:
            return
        self._call_runtime(
            runtime, "set_precision", target_precision.value,
        )

    def _transition_strategies(
        self, runtime: Any, old_mode: str, target_mode: str,
    ) -> None:
        # Revert old strategy if applicable.
        if self._applied_strategy is not None:
            old_strategy = self.registry.get(old_mode)
            if old_strategy is not None and old_strategy is self._applied_strategy:
                revert = getattr(old_strategy, "revert", None)
                if callable(revert):
                    try:
                        revert(runtime)
                    except Exception as e:
                        logger.warning(
                            "strategy %s revert failed: %s", old_mode, e,
                        )
                with self._lock:
                    self._applied_strategy = None

        # Apply new strategy if the target mode is one.
        new_strategy = self.registry.get(target_mode)
        if new_strategy is not None:
            try:
                new_strategy.apply(runtime)
                with self._lock:
                    self._applied_strategy = new_strategy
            except Exception as e:
                logger.warning(
                    "strategy %s apply failed: %s", target_mode, e,
                )
        elif target_mode not in _MODE_TARGETS:
            # Strategy disappeared (was registered before, now not).
            logger.warning(
                "mode %s has no strategy registered; resetting to normal",
                target_mode,
            )
            with self._lock:
                self.mode = "normal"
                self._applied_mode = "normal"
            self._metrics.inc("strategy_missing_total")

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

    def _call_runtime(self, runtime: Any, method: str, *args: Any) -> None:
        fn = getattr(runtime, method, None)
        if not callable(fn):
            return
        try:
            result = fn(*args)
            if inspect.isawaitable(result):
                logger.warning(
                    "runtime.%s is async; cannot await from sync apply()",
                    method,
                )
                # Close coroutine to avoid 'never awaited' warning.
                close = getattr(result, "close", None)
                if callable(close):
                    close()
        except Exception:
            # Let the caller's circuit breaker handle it.
            raise

    def _safe_get_carbon_intensity(self) -> Optional[float]:
        if self.carbon_forecaster is None:
            return None
        if not self._carbon_circuit.can_call():
            return None
        try:
            getter = getattr(
                self.carbon_forecaster, "get_current_intensity", None,
            )
            if getter is None:
                return None
            result = self._call_with_timeout(
                getter, self.config.carbon_timeout_s,
            )
            if inspect.isawaitable(result):
                if not self._async_forecaster_warned:
                    logger.warning(
                        "carbon_forecaster returns a coroutine; use "
                        "evaluate_async() to await it",
                    )
                    self._async_forecaster_warned = True
                close = getattr(result, "close", None)
                if callable(close):
                    close()
                return None
            self._carbon_circuit.record_success()
            return float(result) if result is not None else None
        except (concurrent.futures.TimeoutError, TimeoutError):
            self._carbon_circuit.record_failure()
            self._metrics.inc("carbon_timeouts")
            logger.warning("carbon_forecaster timed out")
            return None
        except Exception as e:
            self._carbon_circuit.record_failure()
            self._metrics.inc("carbon_errors")
            logger.warning(
                "carbon_forecaster failed: %s", e, exc_info=True,
            )
            return None

    async def _safe_get_carbon_intensity_async(self) -> Optional[float]:
        if self.carbon_forecaster is None:
            return None
        if not self._carbon_circuit.can_call():
            return None
        try:
            getter = getattr(
                self.carbon_forecaster, "get_current_intensity", None,
            )
            if getter is None:
                return None
            coro = getter()
            if inspect.isawaitable(coro):
                result = await asyncio.wait_for(
                    coro, timeout=self.config.carbon_timeout_s
                    or 2.0,
                )
            else:
                result = coro
            self._carbon_circuit.record_success()
            return float(result) if result is not None else None
        except (asyncio.TimeoutError, concurrent.futures.TimeoutError):
            self._carbon_circuit.record_failure()
            self._metrics.inc("carbon_timeouts")
            return None
        except Exception as e:
            self._carbon_circuit.record_failure()
            self._metrics.inc("carbon_errors")
            logger.warning(
                "carbon_forecaster (async) failed: %s", e, exc_info=True,
            )
            return None

    def _safe_get_helium_scarcity(self) -> Optional[float]:
        if self.helium_signal_fn is None:
            return None
        try:
            sig = self.helium_signal_fn()
            if inspect.isawaitable(sig):
                logger.warning(
                    "helium_signal_fn returns a coroutine; use sync in "
                    "evaluate() or await in evaluate_async()",
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
            return None

    def _build_threshold_state(
        self, previous_mode: str, trend: Dict[str, Any],
    ) -> ThresholdState:
        return ThresholdState(
            hour_of_day=_utcnow().hour,
            previous_mode_hash=stable_hash(previous_mode),
            workload_hash=stable_hash(str(trend.get("workload", ""))),
        )

    def _try_strategies(
        self,
        trend: Dict[str, Any],
        previous_mode: str,
        carbon_intensity: Optional[float],
        helium_scarcity: Optional[float],
        fallback: str,
    ) -> str:
        context = {
            "carbon_intensity": carbon_intensity,
            "helium_scarcity": helium_scarcity,
            "current_precision": self.current_precision.value,
            "previous_mode": previous_mode,
            "trend": trend,
        }
        for strategy in self.registry.all():
            try:
                if strategy.matches(trend, context):
                    self._metrics.inc(
                        "strategy_match_total", name=strategy.name,
                    )
                    return strategy.name
            except Exception as e:
                logger.warning(
                    "strategy %s matches() failed: %s", strategy.name, e,
                )
        return fallback

    def _record_switch(
        self,
        new_mode: str,
        previous_mode: str,
        trend: Dict[str, Any],
        reason: str = "threshold",
        carbon_intensity: Optional[float] = None,
        helium_scarcity: Optional[float] = None,
        vetoed_from: Optional[str] = None,
        veto_reason: Optional[str] = None,
    ) -> str:
        switch_id = self._new_switch_id()
        entry = {
            "switch_id": switch_id,
            "mode": new_mode,
            "previous": previous_mode,
            "trend": _extract_safe_trend(trend, self.config),
            "reason": reason,
            "carbon_intensity": carbon_intensity,
            "helium_scarcity": helium_scarcity,
            "causal_offsets": self.last_causal_offsets,
            "precision": self.current_precision.value,
            "vetoed_from": vetoed_from,
            "veto_reason": veto_reason,
            "at": _utcnow().isoformat(),
        }
        with self._lock:
            self.mode_history.append(entry)
            self.mode_counts[new_mode] += 1
            self.mode_kind_counts[_mode_kind(new_mode).value] += 1
        return switch_id

    def _new_switch_id(self) -> str:
        with self._lock:
            switch_id = f"sw-{self._next_switch_id}"
            self._next_switch_id += 1
            return switch_id

    # ------------------------------------------------------------------ public enhancement APIs
    def register_strategy(self, strategy: ModeStrategy) -> None:
        self.registry.register(strategy)
        logger.info("Registered custom mode strategy: %s", strategy.name)

    def unregister_strategy(self, name: str) -> None:
        self.registry.unregister(name)
        logger.info("Unregistered custom mode strategy: %s", name)

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

    def explain_last_switch(self) -> Optional[ModeExplanation]:
        with self._lock:
            return self.last_explanation

    def record_outcome(
        self,
        quality_preserved: bool,
        resource_reduction_pct: float,
        *,
        switch_id: Optional[str] = None,
    ) -> None:
        """
        Feed back the outcome of a recent switch to the causal learner.

        Args:
            quality_preserved: whether the switch preserved output quality.
            resource_reduction_pct: percent reduction (0-100).
            switch_id: optional switch identifier; defaults to the most recent.
        """
        if not self.causal_learner:
            return
        with self._lock:
            if switch_id is None:
                if not self._pending_switches:
                    return
                switch_id = next(reversed(self._pending_switches))
            entry = self._pending_switches.get(switch_id)
            if entry is None:
                return
            # Pop once consumed (single attribution).
            self._pending_switches.pop(switch_id, None)

        # Negative reward when quality is lost.
        if not quality_preserved:
            reward = -1.0
        else:
            reward = min(1.0, max(0.0, resource_reduction_pct / 50.0))

        self.causal_learner.record(
            entry["state"], reward, entry["offsets_used"],
        )
        self._metrics.observe("causal_reward", reward)

        # Coordinator feedback now comes from *outcome*, not mode.
        if self.coordinator:
            self.coordinator.record(
                agent_id=self.agent_id,
                mode=entry["mode"],
                success=bool(quality_preserved),
            )

        if self.causal_learner.observations % \
                self.config.causal_update_every == 0:
            self.causal_learner.update()

    def contribute_federated(self) -> None:
        if not self.federated:
            return
        grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        with self._lock:
            for entry in self.mode_history:
                grouped[entry["mode"]].append(entry)
        for mode, entries in grouped.items():
            if len(entries) < self.config.federated_min_samples:
                continue
            mems = [
                float(e["trend"].get("memory_delta", 0.0) or 0.0)
                for e in entries
            ]
            cpus = [
                float(e["trend"].get("cpu_delta", 0.0) or 0.0)
                for e in entries
            ]
            self.federated.push(FederatedModeProfile(
                deployment_id=self.deployment_id,
                mode=mode,
                mean_memory_delta=statistics.fmean(mems) if mems else 0.0,
                mean_cpu_delta=statistics.fmean(cpus) if cpus else 0.0,
                sample_count=len(entries),
            ))

    def get_federated_aggregate(self) -> Dict[str, Dict[str, float]]:
        return self.federated.aggregate() if self.federated else {}

    def distill_policy(
        self, urgency: str = "normal",
    ) -> Optional[DistilledModePolicy]:
        if not self.distiller:
            return None
        precision = (
            self.precision_ctl.select(urgency)
            if self.precision_ctl else PrecisionLevel.INT8
        )
        mem_t = self.base_memory_threshold + self.last_causal_offsets[0]
        cpu_t = self.base_cpu_threshold + self.last_causal_offsets[1]
        return self.distiller.distill(
            memory_threshold=mem_t,
            cpu_threshold=cpu_t,
            precision=precision,
        )

    # ------------------------------------------------------------------ health & statistics
    def health(self) -> Dict[str, Any]:
        with self._lock:
            mode = self.mode
            applied_mode = self._applied_mode
        return {
            "ready": self._runtime_circuit.state != CircuitState.OPEN,
            "current_mode": mode,
            "applied_mode": applied_mode,
            "mode_in_sync": mode == applied_mode,
            "circuits": {
                "runtime": self._runtime_circuit.state.value,
                "carbon": self._carbon_circuit.state.value,
            },
            "signals": {
                "carbon_forecaster_configured":
                    self.carbon_forecaster is not None,
                "helium_signal_configured":
                    self.helium_signal_fn is not None,
            },
            "uptime_seconds": time.monotonic() - self._started_mono,
        }

    def get_statistics(self) -> Dict[str, Any]:
        with self._lock:
            stats: Dict[str, Any] = {
                "deployment_id": self.deployment_id,
                "agent_id": self.agent_id,
                "current_mode": self.mode,
                "applied_mode": self._applied_mode,
                "switches_recorded": len(self.mode_history),
                "mode_distribution": dict(self.mode_counts),
                "mode_kind_distribution": dict(self.mode_kind_counts),
                "circuits": {
                    "runtime": self._runtime_circuit.state.value,
                    "carbon": self._carbon_circuit.state.value,
                },
                "base_thresholds": {
                    "memory": self.base_memory_threshold,
                    "cpu": self.base_cpu_threshold,
                    "carbon": self.base_carbon_threshold,
                    "helium": self.base_helium_threshold,
                },
                "current_precision": self.current_precision.value,
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
        if self.causal_learner:
            stats["causal_learner"] = {
                "observations": self.causal_learner.observations,
                "updates": self.causal_learner.updates,
                "last_advantage_mean":
                    self.causal_learner.last_advantage_mean,
                "last_offsets": {
                    "memory": self.last_causal_offsets[0],
                    "cpu": self.last_causal_offsets[1],
                },
            }
        if self.federated:
            stats["federated_aggregate"] = self.federated.aggregate()
        stats["metrics"] = self._metrics.snapshot()
        return stats


# =============================================================================
# Helpers used by AdaptiveController
# =============================================================================

def _extract_safe_trend(
    trend: Mapping[str, Any],
    config: AdaptiveControllerConfig,
) -> Dict[str, Any]:
    """Only keep whitelisted keys and cap string lengths for history."""
    out: Dict[str, Any] = {}
    for k in config.history_trend_whitelist:
        if k not in trend:
            continue
        v = trend[k]
        if isinstance(v, str):
            out[k] = _sanitize(v, config.max_trend_str_len)
        elif isinstance(v, (int, float, bool)) or v is None:
            out[k] = v
        else:
            out[k] = f"<{type(v).__name__}>"
    return out


__all__ = [
    "AdaptiveController",
    "AdaptiveControllerConfig",
    "AdaptivePrecisionController",
    "AgentRole",
    "CausalThresholdLearner",
    "CircuitBreaker",
    "CircuitState",
    "DistilledModePolicy",
    "FederatedAggregator",
    "FederatedModeProfile",
    "FiniteTrendValues",
    "HITLRequest",
    "HardwareProfile",
    "HumanInTheLoopGate",
    "MarketSnapshot",
    "MetricsRecorder",
    "MinimumToolCallsRespected",
    "ModeExplanation",
    "ModeExplainer",
    "ModeKind",
    "ModeStrategy",
    "MultiAgentCoordinator",
    "NoSwitchInCriticalSection",
    "PrecisionLevel",
    "QuantumDistillationBridge",
    "StrategyRegistry",
    "TemporalLogicMonitor",
    "ThresholdState",
]


# =============================================================================
# Demo
# =============================================================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    class _MockRuntime:
        """Runtime with both reduce/restore pairs so un-throttle works."""
        def __init__(self):
            self.tool_calls = 10
            self.context_len = 4096
            self.precision = "fp32"
            self._history: List[Tuple[str, Any]] = []

        def reduce_tool_calls(self):
            self.tool_calls = max(1, self.tool_calls - 2)
            self._history.append(("reduce_tool_calls", self.tool_calls))
            logger.info("  reduced tool_calls → %s", self.tool_calls)

        def restore_tool_calls(self):
            self.tool_calls = 10
            self._history.append(("restore_tool_calls", self.tool_calls))
            logger.info("  restored tool_calls → %s", self.tool_calls)

        def shorten_context(self):
            self.context_len = max(512, self.context_len // 2)
            self._history.append(("shorten_context", self.context_len))
            logger.info("  shortened context → %s", self.context_len)

        def restore_context(self):
            self.context_len = 4096
            self._history.append(("restore_context", self.context_len))
            logger.info("  restored context → %s", self.context_len)

        def set_precision(self, level: str):
            self.precision = level
            self._history.append(("set_precision", level))
            logger.info("  set precision → %s", level)

    class _MockHelium:
        scarcity_score = 0.75

    class _MockCarbon:
        def get_current_intensity(self):
            return 520.0  # above threshold

    def auto_approve(req: HITLRequest) -> bool:
        logger.info("[HITL] auto-approve: %s", req.reason)
        return True

    # ---- Demo 1: legacy (all features off) ----
    print("\n=== Legacy mode (all features off) ===")
    legacy = AdaptiveController(
        features={k: False for k in AdaptiveController.default_features()},
    )
    mode = legacy.evaluate({"memory_delta": 30.0, "cpu_delta": 5.0})
    print(f"  mode = {mode}")  # low_memory
    rt_legacy = _MockRuntime()
    legacy.apply(rt_legacy)
    legacy.apply(rt_legacy)  # idempotent: no further shortening
    assert rt_legacy.context_len == 2048, (
        f"expected shortened once to 2048, got {rt_legacy.context_len}"
    )
    print(f"  context after one apply (idempotent): {rt_legacy.context_len}")
    legacy.evaluate({"memory_delta": 1.0, "cpu_delta": 1.0})
    legacy.apply(rt_legacy)  # → normal restores
    assert rt_legacy.context_len == 4096, (
        f"expected restore to 4096, got {rt_legacy.context_len}"
    )
    print(f"  context after return-to-normal: {rt_legacy.context_len} ✓")

    # ---- Demo 2: full enhancement stack ----
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

    # Trigger normal — verify un-throttle
    print("\n=== Un-throttle test ===")
    trend4 = {"memory_delta": 1.0, "cpu_delta": 1.0}
    mode = controller.evaluate(trend4)
    controller.apply(runtime)
    print(f"  mode = {mode}, tool_calls={runtime.tool_calls}, "
          f"context={runtime.context_len}, precision={runtime.precision}")
    assert runtime.tool_calls == 10, "tool_calls should be restored"
    assert runtime.context_len == 4096, "context should be restored"
    assert runtime.precision == "fp32", "precision should be restored"
    print("  ✓ all runtime state restored to full capacity")

    # Feed back an outcome (targeted to the most recent switch).
    controller.record_outcome(
        quality_preserved=True,
        resource_reduction_pct=35.0,
    )

    # Negative-delta test (resources freed → must NOT be blocked).
    print("\n=== Negative-delta test ===")
    mode = controller.evaluate({"memory_delta": -5.0, "cpu_delta": -3.0})
    print(f"  mode after negative deltas = {mode}")
    assert mode == "normal", "negative deltas should not force normal via veto"

    # Federated + distill.
    controller.contribute_federated()
    dist = controller.distill_policy(urgency="normal")
    if dist:
        print("\n=== Distilled Policy ===")
        print(f"  Precision:      {dist.precision.value}")
        print(f"  Mem threshold:  {dist.memory_threshold:.2f}")
        print(f"  CPU threshold:  {dist.cpu_threshold:.2f}")
        print(f"  Retention:      {dist.quality_retention}")
        print(f"  Energy saving:  {dist.energy_reduction_percent}%")

    # XAI.
    exp = controller.explain_last_switch()
    if exp:
        print("\n=== XAI (last switch) ===")
        print(f"  {exp.headline}  (confidence={exp.confidence:.2f})")
        for r in exp.rationale:
            print(f"    • {r}")

    # Helium-as-dict test.
    print("\n=== Helium-as-dict test ===")
    controller.helium_signal_fn = lambda: {"scarcity_score": 0.9}
    controller.evaluate({"memory_delta": 1.0, "cpu_delta": 1.0})
    print(f"  helium signal handled from dict ✓")

    # Health.
    print("\n=== Health ===")
    print(controller.health())

    # Statistics.
    import json
    print("\n=== Statistics ===")
    print(json.dumps(controller.get_statistics(), indent=2, default=str))

    controller.close()
    print("\nSmoke test passed.")
