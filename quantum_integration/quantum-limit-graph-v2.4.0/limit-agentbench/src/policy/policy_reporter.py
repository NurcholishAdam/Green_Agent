"""
Policy reporting and alerting (Enhanced v3)
============================================

Generates alerts from policy metrics with first-class support for the
ten Green Agent enhancement layers.

Original API preserved:
    reporter = PolicyReporter(policy)
    result = reporter.report(metrics)
    # result["policy_alerts"] is a list of strings

v3 fixes (see changelog in commit message):
  * Deduplication is cross-call (was dead code within a single report).
  * Rate limiting uses a rolling 60s monotonic window (was
    len(alert_history), which fired forever after 60 reports).
  * Critical-section policy *filters* alerts; the temporal monitor is a
    passive observer (was: self-amplifying alert storm).
  * Causal learner uses reward-weighted regression with a per-bin
    baseline and consumes the threshold actually used.
  * Legacy energy gate uses the BASE threshold; the adjusted threshold
    only affects the structured alert.
  * AlertCategory.PRECISION is now wired via metrics["precision"].
  * report_count is incremented AFTER input validation.
  * Missing metrics are distinct from zero (`missing_inputs`).
  * Degraded paths emit the full happy-path key set.
  * StructuredAlert is frozen; structured_alerts_last is a tuple.
  * HITL receives the top-alert's category/value/threshold + full payload.
  * Per-category severity ratios; configurable chaos; timeouts on market
    and helium signals; incremental federated aggregation.
  * Thread-safe; UTC datetimes; stable SHA-256 hashing.
  * PolicyReporterConfig, MetricsRecorder, health(), close(),
    async HITL, __version__, __all__.
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
    Protocol, Tuple,
)

logger = logging.getLogger(__name__)

__version__ = "3.0.0"


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

class Severity(Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"
    EMERGENCY = "emergency"


class AlertCategory(Enum):
    ENERGY = "energy"
    CARBON = "carbon"
    HELIUM = "helium"
    LATENCY = "latency"
    COST = "cost"
    PRECISION = "precision"
    TEMPORAL = "temporal"


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
# Config
# =============================================================================

@dataclass(frozen=True)
class SeverityRatios:
    """Overshoot ratios (value / threshold) that define each severity."""
    warning: float = 1.05
    critical: float = 1.30
    emergency: float = 2.00


@dataclass(frozen=True)
class PolicyReporterConfig:
    # Thresholds.
    energy_threshold_pct: float = 80.0
    carbon_threshold_kg: float = 1.0
    helium_threshold_units: float = 0.5
    latency_threshold_s: float = 300.0
    cost_threshold_usd: float = 1.0

    # Deduplication.
    dedup_window_s: float = 30.0
    dedup_max_entries: int = 4096

    # Rate limiting.
    max_alert_rate_per_minute: int = 60
    rate_window_max_entries: int = 4096

    # Critical-section severity policy.
    critical_section_passes: Tuple[str, ...] = ("critical", "emergency")

    # Severity.
    default_severity: SeverityRatios = field(default_factory=SeverityRatios)
    severity_by_category: Mapping[str, SeverityRatios] = field(
        default_factory=dict,
    )

    # Signal timeouts (None disables).
    market_timeout_s: Optional[float] = 2.0
    helium_timeout_s: Optional[float] = 2.0
    hitl_timeout_s: float = 60.0
    hitl_default_on_timeout: bool = False

    # Circuit breakers.
    circuit_failure_threshold: int = 5
    circuit_recovery_timeout_s: int = 30

    # Chaos.
    chaos_failure_rate: float = 0.0
    chaos_event_maxlen: int = 100

    # History bounds.
    alert_history_maxlen: int = 1024
    temporal_violation_maxlen: int = 500
    hitl_feedback_maxlen: int = 1000
    pending_alert_maxlen: int = 512

    # Federated.
    federated_push_every: int = 25
    federated_recent_maxlen: int = 256

    # Causal learner.
    causal_lr: float = 0.02
    causal_max_adjustment_ratio: float = 0.20
    causal_baseline_decay: float = 0.95
    causal_buffer_maxlen: int = 512
    causal_update_every: int = 10

    # Executor.
    executor_max_workers: int = 4

    def __post_init__(self) -> None:
        if self.dedup_window_s < 0:
            raise ValueError("dedup_window_s must be >= 0.")
        if self.max_alert_rate_per_minute <= 0:
            raise ValueError("max_alert_rate_per_minute must be > 0.")
        if not 0.0 <= self.chaos_failure_rate <= 1.0:
            raise ValueError("chaos_failure_rate must be in [0, 1].")
        for f in ("market_timeout_s", "helium_timeout_s"):
            v = getattr(self, f)
            if v is not None and v <= 0:
                raise ValueError(f"{f} must be > 0 or None.")
        if not isinstance(self.severity_by_category, types.MappingProxyType):
            object.__setattr__(
                self, "severity_by_category",
                types.MappingProxyType(dict(self.severity_by_category)),
            )

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["severity_by_category"] = {
            k: asdict(v) for k, v in self.severity_by_category.items()
        }
        d["critical_section_passes"] = list(self.critical_section_passes)
        return d


# =============================================================================
# Circuit Breaker
# =============================================================================

class CircuitBreaker:
    def __init__(
        self, name: str, failure_threshold: int = 5,
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

    @property
    def failures(self) -> int:
        with self._lock:
            return self._failures

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
# Rate Limiter (rolling window)
# =============================================================================

class _RateLimiter:
    def __init__(self, max_per_minute: int, max_entries: int = 4096) -> None:
        self.max_per_minute = max_per_minute
        self._timestamps: Deque[float] = deque(maxlen=max_entries)
        self._lock = threading.RLock()

    def try_acquire(self) -> bool:
        with self._lock:
            now = time.monotonic()
            cutoff = now - 60.0
            while self._timestamps and self._timestamps[0] < cutoff:
                self._timestamps.popleft()
            if len(self._timestamps) >= self.max_per_minute:
                return False
            self._timestamps.append(now)
            return True

    def count_last_minute(self) -> int:
        with self._lock:
            cutoff = time.monotonic() - 60.0
            while self._timestamps and self._timestamps[0] < cutoff:
                self._timestamps.popleft()
            return len(self._timestamps)


# =============================================================================
# Critical-Section Guard
# =============================================================================

class _CriticalSectionGuard:
    def __init__(self, pass_through_severities: Tuple[str, ...]) -> None:
        self._pass = set(pass_through_severities)

    def passes(self, severity: str, in_critical_section: bool) -> bool:
        if not in_critical_section:
            return True
        return severity in self._pass


# =============================================================================
# Temporal Logic (passive observer)
# =============================================================================

class AlertProperty(Protocol):
    def check(self, context: Dict[str, Any]) -> bool: ...
    def name(self) -> str: ...


@dataclass
class MaxAlertRate:
    """Passive observer — records violations but never emits alerts."""
    max_per_minute: int = 60

    def check(self, context: Dict[str, Any]) -> bool:
        return context.get("alerts_last_minute", 0) <= self.max_per_minute

    def name(self) -> str:
        return "MaxAlertRate"


@dataclass
class NoAlertDuringCriticalSection:
    """Passive observer — the critical-section guard already filters."""
    def check(self, context: Dict[str, Any]) -> bool:
        if not context.get("in_critical_section"):
            return True
        return context.get("filtered_alert_count", 0) == 0

    def name(self) -> str:
        return "NoAlertDuringCriticalSection"


class TemporalLogicMonitor:
    """Passive observer — never emits alerts. Records violations for stats."""

    def __init__(self, maxlen: int = 500) -> None:
        self.properties: List[AlertProperty] = []
        self.violations: Deque[Dict[str, Any]] = deque(maxlen=maxlen)
        self._lock = threading.RLock()

    def register(self, p: AlertProperty) -> None:
        with self._lock:
            self.properties.append(p)

    def verify(
        self, context: Dict[str, Any],
    ) -> Tuple[bool, List[str]]:
        bad: List[str] = []
        with self._lock:
            props = list(self.properties)
        for p in props:
            try:
                ok = p.check(context)
            except Exception as e:
                logger.warning("property %s raised: %s", p.name(), e)
                ok = False
            if not ok:
                bad.append(p.name())
                with self._lock:
                    self.violations.append({
                        "property": p.name(),
                        "at": _utcnow().isoformat(),
                    })
        return (len(bad) == 0, bad)


# =============================================================================
# Structured XAI
# =============================================================================

@dataclass(frozen=True)
class StructuredAlert:
    """Rich, immutable structured alert."""
    category: str
    severity: str
    headline: str
    rationale: Tuple[str, ...]
    confidence: float
    contributing_factors: Mapping[str, float]
    alert_id: Optional[str] = None
    value: Optional[float] = None
    threshold: Optional[float] = None
    at: str = field(default_factory=lambda: _utcnow().isoformat())

    def __post_init__(self) -> None:
        object.__setattr__(self, "rationale", tuple(self.rationale))
        if not isinstance(self.contributing_factors, types.MappingProxyType):
            object.__setattr__(
                self, "contributing_factors",
                types.MappingProxyType(dict(self.contributing_factors)),
            )

    def __str__(self) -> str:
        return f"[{self.severity.upper()}] {self.headline}"

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["rationale"] = list(self.rationale)
        d["contributing_factors"] = dict(self.contributing_factors)
        return d


class AlertExplainer:
    @staticmethod
    def build(
        category: AlertCategory,
        severity: Severity,
        value: Optional[float],
        threshold: Optional[float],
        extra_factors: Optional[Dict[str, float]] = None,
        market_snapshot: Optional[Dict[str, Any]] = None,
        alert_id: Optional[str] = None,
    ) -> StructuredAlert:
        reasons: List[str] = []
        factors: Dict[str, float] = {}

        if value is not None and threshold is not None:
            reasons.append(
                f"{category.value} metric = {value:.4f}, "
                f"threshold = {threshold:.4f}."
            )
            overshoot = value - threshold
            if threshold > 0 and overshoot > 0:
                pct = (overshoot / threshold) * 100.0
                reasons.append(f"Overshoot: {overshoot:.4f} ({pct:.1f}%).")
                factors["overshoot"] = overshoot
            factors["value"] = value
            factors["threshold"] = threshold
            headline = (
                f"{category.value} alert: {value:.3f} "
                f"vs threshold {threshold:.3f}"
            )
        elif value is not None:
            reasons.append(f"{category.value} metric = {value:.4f}.")
            factors["value"] = value
            headline = f"{category.value} alert: {value:.3f}"
        else:
            reasons.append(f"{category.value} alert (no value provided).")
            headline = f"{category.value} alert"

        if market_snapshot:
            cp = market_snapshot.get("carbon_price_per_tco2_usd")
            if cp:
                reasons.append(f"Carbon market price = ${cp:.2f}/tCO2.")
                factors["carbon_price"] = float(cp)

        if extra_factors:
            for k, v in extra_factors.items():
                try:
                    factors[k] = float(v)
                except (TypeError, ValueError):
                    continue

        base_conf = 0.5
        factor_boost = min(0.4, 0.08 * max(0, len(factors) - 2))
        confidence = min(0.99, base_conf + factor_boost)

        return StructuredAlert(
            category=category.value,
            severity=severity.value,
            headline=headline,
            rationale=tuple(reasons),
            confidence=confidence,
            contributing_factors=factors,
            alert_id=alert_id,
            value=value,
            threshold=threshold,
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
        """'critical' → FULL fidelity (FP32)."""
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
# Causal Threshold Learner (RWR + per-bin baseline)
# =============================================================================

@dataclass(frozen=True)
class ThresholdState:
    hour_of_day: int
    recent_alert_count: int
    workload_hash: float


class CausalThresholdLearner:
    """
    Learns a bounded offset to the base threshold from outcome feedback.

    v3 — reward-weighted regression:
      * Consumes the offset actually applied (regression target).
      * Per-bin baseline removes monotone drift (reward is 0/1).
      * No artificial base floor — the offset can legitimately soften the
        threshold if the base was too tight for a given context.
    """

    N_FEATURES = 3

    def __init__(
        self,
        base_threshold: float,
        lr: float = 0.02,
        max_adjustment_ratio: float = 0.20,
        baseline_decay: float = 0.95,
        buffer_maxlen: int = 512,
    ) -> None:
        self.base_threshold = float(base_threshold)
        self.lr = lr
        self.max_adjustment_ratio = max_adjustment_ratio
        self.baseline_decay = baseline_decay
        self.coeffs: List[float] = [0.0] * self.N_FEATURES
        self.baselines: Dict[int, float] = defaultdict(float)
        self.buffer: Deque[
            Tuple[List[float], float, float, int]
        ] = deque(maxlen=buffer_maxlen)
        self.observations: int = 0
        self.updates: int = 0
        self.last_advantage_mean: float = 0.0
        self._lock = threading.RLock()

    @staticmethod
    def _features(s: ThresholdState) -> List[float]:
        return [
            s.hour_of_day / 24.0,
            min(1.0, s.recent_alert_count / 50.0),
            s.workload_hash,
        ]

    @staticmethod
    def _bin(f: List[float]) -> int:
        return int(f[1] * 5)  # 5 buckets by recent alert load

    def _baseline_for(self, f: List[float]) -> float:
        return self.baselines.get(self._bin(f), 0.0)

    def _update_baseline(self, f: List[float], reward: float) -> None:
        b = self._bin(f)
        old = self.baselines.get(b, 0.0)
        self.baselines[b] = (
            self.baseline_decay * old
            + (1.0 - self.baseline_decay) * reward
        )

    def _raw_predict(self, f: List[float]) -> float:
        return sum(c * x for c, x in zip(self.coeffs, f))

    def predict_threshold(self, s: ThresholdState) -> float:
        f = self._features(s)
        with self._lock:
            offset = self._raw_predict(f)
        bound = self.max_adjustment_ratio * self.base_threshold
        offset = max(-bound, min(bound, offset))
        return max(0.01 * self.base_threshold, self.base_threshold + offset)

    def record(
        self,
        s: ThresholdState,
        reward: float,
        threshold_used: float,
    ) -> None:
        f = self._features(s)
        with self._lock:
            self.buffer.append((f, reward, threshold_used, self._bin(f)))
            self.observations += 1

    def update(self) -> None:
        with self._lock:
            if not self.buffer:
                return
            advantages: List[float] = []
            for f, reward, threshold_used, _b in self.buffer:
                adv = reward - self._baseline_for(f)
                advantages.append(adv)
                self._update_baseline(f, reward)

                pred_offset = self._raw_predict(f)
                used_offset = threshold_used - self.base_threshold
                error = used_offset - pred_offset
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
class DistilledAlertPolicy:
    precision: PrecisionLevel
    base_threshold: float
    quality_retention: float
    energy_reduction_percent: float


class QuantumDistillationBridge:
    def distill(
        self,
        base_threshold: float,
        precision: PrecisionLevel,
    ) -> DistilledAlertPolicy:
        scale, retention, energy = {
            PrecisionLevel.FP32: (1.0, 1.00, 0.0),
            PrecisionLevel.FP16: (1.0, 0.98, 30.0),
            PrecisionLevel.INT8: (100.0, 0.93, 55.0),
            PrecisionLevel.INT4: (10.0, 0.85, 70.0),
            PrecisionLevel.QUANTUM_DISTILLED: (5.0, 0.80, 85.0),
        }[precision]

        def qz(v: float) -> float:
            return math.floor(v * scale) / scale if scale > 1.0 else v

        return DistilledAlertPolicy(
            precision=precision,
            base_threshold=qz(base_threshold),
            quality_retention=retention,
            energy_reduction_percent=energy,
        )


# =============================================================================
# Federated (incremental)
# =============================================================================

@dataclass(frozen=True)
class FederatedAlertProfile:
    deployment_id: str
    category: str
    severity: str
    mean_value: float
    sample_count: int
    granularity: str = "aggregate"  # "aggregate" | "single"
    timestamp: datetime = field(default_factory=_utcnow)


class FederatedAggregator:
    def __init__(self, recent_maxlen: int = 256) -> None:
        self._sum_value: Dict[str, float] = defaultdict(float)
        self._count: Dict[str, int] = defaultdict(int)
        self.recent: Deque[FederatedAlertProfile] = deque(maxlen=recent_maxlen)
        self._global: Dict[str, Dict[str, float]] = {}
        self._lock = threading.RLock()

    def push(self, u: FederatedAlertProfile) -> None:
        with self._lock:
            self.recent.append(u)
            if u.granularity != "aggregate":
                return
            self._sum_value[u.category] += u.mean_value * u.sample_count
            self._count[u.category] += u.sample_count

    def aggregate(self) -> Dict[str, Dict[str, float]]:
        with self._lock:
            result: Dict[str, Dict[str, float]] = {}
            for category, n in self._count.items():
                if n <= 0:
                    continue
                result[category] = {
                    "mean_value": self._sum_value[category] / n,
                    "sample_count": n,
                }
            self._global = result
            return dict(result)

    def lookup(self, category: str) -> Optional[Dict[str, float]]:
        with self._lock:
            if not self._global:
                self.aggregate()
            return self._global.get(category)


# =============================================================================
# Multi-Agent
# =============================================================================

@dataclass
class AgentProfile:
    agent_id: str
    role: AgentRole = AgentRole.GENERALIST
    success_rate: float = 0.0
    energy_focus: float = 0.0
    carbon_focus: float = 0.0
    helium_focus: float = 0.0
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
            if a.energy_focus > 0.5:
                a.role = AgentRole.ENERGY_SPECIALIST
            elif a.carbon_focus > 0.5:
                a.role = AgentRole.CARBON_SPECIALIST
            elif a.helium_focus > 0.4:
                a.role = AgentRole.HELIUM_SPECIALIST
            elif a.success_rate > 0.9:
                a.role = AgentRole.LATENCY_SPECIALIST
            else:
                a.role = AgentRole.GENERALIST

    def record(
        self, agent_id: str, category: str, success: bool,
    ) -> None:
        with self._lock:
            a = self.register(agent_id)
            n = a.total_calls + 1
            a.success_rate = (
                (n - 1) * a.success_rate + float(success)
            ) / n
            if category == AlertCategory.ENERGY.value:
                a.energy_focus = ((n - 1) * a.energy_focus + 1.0) / n
            elif category == AlertCategory.CARBON.value:
                a.carbon_focus = ((n - 1) * a.carbon_focus + 1.0) / n
            elif category == AlertCategory.HELIUM.value:
                a.helium_focus = ((n - 1) * a.helium_focus + 1.0) / n
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

@dataclass(frozen=True)
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
# HITL
# =============================================================================

@dataclass(frozen=True)
class HITLRequest:
    category: str
    severity: str
    value: Optional[float]
    threshold: Optional[float]
    reason: str
    urgency: str
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
        timeout_s: float = 60.0,
        default_on_timeout: bool = False,
        feedback_maxlen: int = 1000,
    ) -> None:
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
        if not callable(cb):
            raise TypeError("callback must be callable")
        self._callback = cb

    def set_async_callback(
        self, cb: Callable[[HITLRequest], Awaitable[bool]],
    ) -> None:
        if not callable(cb):
            raise TypeError("callback must be callable")
        self._async_callback = cb

    def needs_review(self, severity: str) -> bool:
        return severity in (Severity.CRITICAL.value, Severity.EMERGENCY.value)

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
        category: str,
        severity: str,
        value: Optional[float],
        threshold: Optional[float],
        reason: str,
        urgency: str = "medium",
        context: Optional[Dict[str, Any]] = None,
    ) -> bool:
        req = HITLRequest(
            category, severity, value, threshold, reason, urgency,
            context or {},
        )
        with self._lock:
            self.pending.append(req)
        try:
            if self._callback is None:
                self._record(req, False, auto=True)
                return False
            decision = self._callback(req)
            self._record(req, bool(decision))
            return bool(decision)
        except Exception as e:
            logger.error("HITL callback failed: %s", e, exc_info=True)
            self._record(req, False)
            return False
        finally:
            with self._lock:
                if req in self.pending:
                    self.pending.remove(req)

    async def request_review_async(
        self,
        category: str,
        severity: str,
        value: Optional[float],
        threshold: Optional[float],
        reason: str,
        urgency: str = "medium",
        context: Optional[Dict[str, Any]] = None,
    ) -> bool:
        req = HITLRequest(
            category, severity, value, threshold, reason, urgency,
            context or {},
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
            logger.error(
                "HITL async callback failed: %s", e, exc_info=True,
            )
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
# Default features (immutable)
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
    "multi_category": True,
    "deduplication": True,
})


# =============================================================================
# PolicyReporter
# =============================================================================

class PolicyReporter:
    """
    Enhanced policy reporter with all ten enhancement layers.

    Backward-compatible signature:
        PolicyReporter(policy)
        result = reporter.report(metrics)
    """

    @classmethod
    def default_features(cls) -> Dict[str, bool]:
        return dict(_DEFAULT_FEATURES)

    def __init__(
        self,
        policy: Dict,
        deployment_id: str = "local",
        agent_id: str = "reporter-0",
        features: Optional[Dict[str, bool]] = None,
        hardware: Optional[HardwareProfile] = None,
        dedup_window_seconds: Optional[float] = None,
        config: Optional[PolicyReporterConfig] = None,
        metrics: Optional[MetricsRecorder] = None,
    ):
        # --- Parse the policy dict (backward-compatible) ---
        policy_safe = policy if isinstance(policy, dict) else {}
        reporting = policy_safe.get("reporting", {})
        if not isinstance(reporting, dict):
            reporting = {}

        energy_thr = float(reporting.get(
            "alert_threshold_energy_pct",
            (config.energy_threshold_pct if config else 80.0),
        ))
        carbon_thr = float(reporting.get(
            "alert_threshold_carbon_kg",
            (config.carbon_threshold_kg if config else 1.0),
        ))
        helium_thr = float(reporting.get(
            "alert_threshold_helium_units",
            (config.helium_threshold_units if config else 0.5),
        ))
        latency_thr = float(reporting.get(
            "alert_threshold_latency_seconds",
            (config.latency_threshold_s if config else 300.0),
        ))
        cost_thr = float(reporting.get(
            "alert_threshold_cost_usd",
            (config.cost_threshold_usd if config else 1.0),
        ))
        dedup_win = float(
            dedup_window_seconds
            if dedup_window_seconds is not None
            else (config.dedup_window_s if config else 30.0)
        )

        # Construct effective config (policy dict overrides config values).
        if config is not None:
            self.config = PolicyReporterConfig(
                energy_threshold_pct=energy_thr,
                carbon_threshold_kg=carbon_thr,
                helium_threshold_units=helium_thr,
                latency_threshold_s=latency_thr,
                cost_threshold_usd=cost_thr,
                dedup_window_s=dedup_win,
                dedup_max_entries=config.dedup_max_entries,
                max_alert_rate_per_minute=config.max_alert_rate_per_minute,
                rate_window_max_entries=config.rate_window_max_entries,
                critical_section_passes=config.critical_section_passes,
                default_severity=config.default_severity,
                severity_by_category=dict(config.severity_by_category),
                market_timeout_s=config.market_timeout_s,
                helium_timeout_s=config.helium_timeout_s,
                hitl_timeout_s=config.hitl_timeout_s,
                hitl_default_on_timeout=config.hitl_default_on_timeout,
                circuit_failure_threshold=config.circuit_failure_threshold,
                circuit_recovery_timeout_s=config.circuit_recovery_timeout_s,
                chaos_failure_rate=config.chaos_failure_rate,
                chaos_event_maxlen=config.chaos_event_maxlen,
                alert_history_maxlen=config.alert_history_maxlen,
                temporal_violation_maxlen=config.temporal_violation_maxlen,
                hitl_feedback_maxlen=config.hitl_feedback_maxlen,
                pending_alert_maxlen=config.pending_alert_maxlen,
                federated_push_every=config.federated_push_every,
                federated_recent_maxlen=config.federated_recent_maxlen,
                causal_lr=config.causal_lr,
                causal_max_adjustment_ratio=config.causal_max_adjustment_ratio,
                causal_baseline_decay=config.causal_baseline_decay,
                causal_buffer_maxlen=config.causal_buffer_maxlen,
                causal_update_every=config.causal_update_every,
                executor_max_workers=config.executor_max_workers,
            )
        else:
            self.config = PolicyReporterConfig(
                energy_threshold_pct=energy_thr,
                carbon_threshold_kg=carbon_thr,
                helium_threshold_units=helium_thr,
                latency_threshold_s=latency_thr,
                cost_threshold_usd=cost_thr,
                dedup_window_s=dedup_win,
            )

        # --- Original state (base thresholds) ---
        self.threshold = energy_thr
        self.carbon_threshold = carbon_thr
        self.helium_threshold = helium_thr
        self.latency_threshold_seconds = latency_thr
        self.cost_threshold_usd = cost_thr
        self.dedup_window_seconds = dedup_win

        # --- Enhancement config ---
        self.deployment_id = deployment_id
        self.agent_id = agent_id
        self.features = {**self.default_features(), **(features or {})}
        self._metrics = metrics or MetricsRecorder()

        # --- Thread safety ---
        self._lock = threading.RLock()

        # --- Resilience ---
        self._circuit = CircuitBreaker(
            "policy_reporter",
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

        # --- Temporal logic (passive observer) ---
        self.temporal_monitor: Optional[TemporalLogicMonitor] = (
            TemporalLogicMonitor(
                maxlen=self.config.temporal_violation_maxlen,
            )
            if self.features["temporal_logic"] else None
        )
        if self.temporal_monitor:
            self.temporal_monitor.register(
                MaxAlertRate(self.config.max_alert_rate_per_minute),
            )
            self.temporal_monitor.register(NoAlertDuringCriticalSection())

        # --- Operational gates ---
        self._rate_limiter = _RateLimiter(
            max_per_minute=self.config.max_alert_rate_per_minute,
            max_entries=self.config.rate_window_max_entries,
        )
        self._critical_guard = _CriticalSectionGuard(
            pass_through_severities=self.config.critical_section_passes,
        )

        # --- XAI ---
        self.explainer = AlertExplainer() if self.features["xai"] else None

        # --- Adaptive precision ---
        self.precision_ctl = (
            AdaptivePrecisionController(hardware)
            if self.features["adaptive_precision"] else None
        )

        # --- Causal RL ---
        self.threshold_learner: Optional[CausalThresholdLearner] = (
            CausalThresholdLearner(
                base_threshold=float(self.threshold),
                lr=self.config.causal_lr,
                max_adjustment_ratio=self.config.causal_max_adjustment_ratio,
                baseline_decay=self.config.causal_baseline_decay,
                buffer_maxlen=self.config.causal_buffer_maxlen,
            )
            if self.features["causal_rl"] else None
        )

        # --- Distillation ---
        self.distiller = (
            QuantumDistillationBridge()
            if self.features["quantum_distillation"] else None
        )

        # --- Federated ---
        self.federated = (
            FederatedAggregator(
                recent_maxlen=self.config.federated_recent_maxlen,
            )
            if self.features["federated"] else None
        )

        # --- Multi-agent ---
        self.coordinator = (
            MultiAgentCoordinator() if self.features["multi_agent"] else None
        )
        if self.coordinator:
            self.coordinator.register(self.agent_id)

        # --- Market ---
        self.market = (
            CarbonMarketClient() if self.features["carbon_market"] else None
        )

        # --- HITL ---
        self.hitl = (
            HumanInTheLoopGate(
                timeout_s=self.config.hitl_timeout_s,
                default_on_timeout=self.config.hitl_default_on_timeout,
                feedback_maxlen=self.config.hitl_feedback_maxlen,
            )
            if self.features["hitl"] else None
        )

        # --- Helium signal (caller-supplied) ---
        self.helium_signal_fn: Optional[Callable[[], Any]] = None

        # --- Dedupe state ---
        self._recent_alerts: Deque[Tuple[Tuple[str, str], float]] = deque(
            maxlen=self.config.dedup_max_entries,
        )

        # --- Alert-ID bookkeeping ---
        self._next_alert_id: int = 1
        self._pending_alert_states: "OrderedDict[str, Dict[str, Any]]" = \
            OrderedDict()

        # --- History & stats ---
        self.alert_history: Deque[Dict[str, Any]] = deque(
            maxlen=self.config.alert_history_maxlen,
        )
        self.alert_counts: Dict[str, int] = defaultdict(int)
        self.severity_counts: Dict[str, int] = defaultdict(int)
        self.report_count: int = 0
        self._started_mono: float = time.monotonic()
        self.structured_alerts_last: Tuple[StructuredAlert, ...] = ()

        # --- Executor for timeouts ---
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.config.executor_max_workers,
            thread_name_prefix="policy-reporter",
        )

        logger.info(
            "Enhanced PolicyReporter initialized "
            "(deployment=%s, agent=%s, threshold=%s, features=%s)",
            deployment_id, agent_id, self.threshold, list(self.features),
        )

    # ------------------------------------------------------------------ lifecycle
    def close(self) -> None:
        self._executor.shutdown(wait=False, cancel_futures=True)

    def __enter__(self) -> "PolicyReporter":
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.close()

    # ------------------------------------------------------------------ helpers
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

    @staticmethod
    def _safe_float(
        value: Any, default: Optional[float] = None,
    ) -> Optional[float]:
        if value is None:
            return default
        try:
            v = float(value)
            return v if math.isfinite(v) else default
        except (TypeError, ValueError):
            return default

    def _severity_for(
        self, category: str, value: float, threshold: float,
    ) -> Severity:
        # §1.6: threshold <= 0 with a positive value is 100%+ overshoot.
        if threshold <= 0:
            return Severity.CRITICAL if value > 0 else Severity.INFO
        ratio = value / threshold
        sr = self.config.severity_by_category.get(
            category, self.config.default_severity,
        )
        if ratio >= sr.emergency:
            return Severity.EMERGENCY
        if ratio >= sr.critical:
            return Severity.CRITICAL
        if ratio >= sr.warning:
            return Severity.WARNING
        return Severity.INFO

    @staticmethod
    def _top_severity(severities: List[str]) -> Optional[str]:
        if not severities:
            return None
        order = {
            Severity.INFO.value: 0,
            Severity.WARNING.value: 1,
            Severity.CRITICAL.value: 2,
            Severity.EMERGENCY.value: 3,
        }
        return max(severities, key=lambda s: order.get(s, -1))

    def _build_threshold_state(
        self, metrics: Mapping[str, Any],
    ) -> ThresholdState:
        workload_id = metrics.get("workload_id", "default")
        with self._lock:
            recent_count = len(self.alert_history)
        return ThresholdState(
            hour_of_day=_utcnow().hour,
            recent_alert_count=recent_count,
            workload_hash=stable_hash(str(workload_id)),
        )

    def _new_alert_id(self) -> str:
        with self._lock:
            aid = f"alert-{self._next_alert_id}"
            self._next_alert_id += 1
            return aid

    # ------------------------------------------------------------------ signals
    def _get_market_snapshot(self) -> Optional[Dict[str, Any]]:
        if self.market is None:
            return None
        try:
            snap = self._call_with_timeout(
                self.market.get_snapshot, self.config.market_timeout_s,
            )
            return {
                "carbon_price_per_tco2_usd": snap.carbon_price_per_tco2_usd,
                "rec_price_per_mwh_usd": snap.rec_price_per_mwh_usd,
            }
        except (concurrent.futures.TimeoutError, TimeoutError):
            self._metrics.inc("market_timeouts")
            logger.warning("market.get_snapshot timed out")
            return None
        except Exception as e:
            self._metrics.inc("market_errors")
            logger.warning(
                "market.get_snapshot failed: %s", e, exc_info=True,
            )
            return None

    def _get_helium_scarcity(self) -> Optional[float]:
        if self.helium_signal_fn is None:
            return None
        try:
            sig = self._call_with_timeout(
                self.helium_signal_fn, self.config.helium_timeout_s,
            )
            if inspect.isawaitable(sig):
                logger.warning(
                    "helium_signal_fn returned a coroutine; use a sync "
                    "callback or provide pre-computed scarcity",
                )
                close = getattr(sig, "close", None)
                if callable(close):
                    close()
                self._metrics.inc("helium_signal_errors")
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
        except (concurrent.futures.TimeoutError, TimeoutError):
            self._metrics.inc("helium_signal_timeouts")
            logger.warning("helium signal timed out")
            return None
        except Exception as e:
            self._metrics.inc("helium_signal_errors")
            logger.warning(
                "helium_signal_fn failed: %s", e, exc_info=True,
            )
            return None

    # ------------------------------------------------------------------ dedupe / rate
    def _is_duplicate(self, category: str, severity: str) -> bool:
        now = time.monotonic()
        with self._lock:
            while (
                self._recent_alerts
                and (now - self._recent_alerts[0][1])
                > self.config.dedup_window_s
            ):
                self._recent_alerts.popleft()
            key = (category, severity)
            for k, _ in self._recent_alerts:
                if k == key:
                    return True
            return False

    def _commit_emit(self, category: str, severity: str) -> None:
        with self._lock:
            self._recent_alerts.append(((category, severity), time.monotonic()))

    def _passes_critical_section(
        self, severity: str, in_critical_section: bool,
    ) -> bool:
        return self._critical_guard.passes(severity, in_critical_section)

    # ------------------------------------------------------------------ public: report
    def report(self, metrics: Dict) -> Dict:
        """
        Original signature preserved.

        Original behavior:
            if metrics["energy_pct"] >= self.threshold:
                alerts.append("Energy budget nearing limit")
            metrics["policy_alerts"] = alerts
        """
        # --- Defensive input ---
        if metrics is None:
            metrics = {}
        elif not isinstance(metrics, dict):
            try:
                metrics = dict(metrics)
            except Exception:
                metrics = {}

        # §2.5: increment after validation.
        with self._lock:
            self.report_count += 1
            report_idx = self.report_count

        # --- Legacy alert path (uses BASE threshold) ---
        alerts: List[str] = []
        energy_pct = self._safe_float(metrics.get("energy_pct"), default=None)
        if energy_pct is not None and energy_pct >= float(self.threshold):
            alerts.append("Energy budget nearing limit")

        # --- Circuit breaker ---
        if not self._circuit.can_call():
            logger.warning(
                "PolicyReporter circuit open — degraded reporting",
            )
            self._metrics.inc("report_degraded", reason="circuit_open")
            return self._degraded_response(
                metrics, alerts, reason="circuit_open",
            )

        # --- Chaos ---
        if self.chaos and self.chaos.maybe_fail("report"):
            self._circuit.record_failure()
            self._metrics.inc("report_degraded", reason="chaos")
            return self._degraded_response(
                metrics, alerts, reason="chaos",
            )

        # --- Compute threshold ---
        state = self._build_threshold_state(metrics)
        active_threshold = float(self.threshold)
        causal_adjustment = 0.0
        if self.threshold_learner is not None:
            try:
                active_threshold = self.threshold_learner.predict_threshold(
                    state,
                )
                causal_adjustment = active_threshold - float(self.threshold)
            except Exception as e:
                logger.warning(
                    "causal threshold predict failed: %s", e, exc_info=True,
                )
                active_threshold = float(self.threshold)

        # --- Structured alerts ---
        try:
            structured_alerts = self._generate_structured_alerts(
                metrics, energy_pct, active_threshold, state,
            )
        except Exception as e:
            self._circuit.record_failure()
            logger.warning(
                "structured alert generation failed: %s", e, exc_info=True,
            )
            self._metrics.inc("report_degraded", reason="generation_error")
            return self._degraded_response(
                metrics, alerts, reason=f"error:{type(e).__name__}",
            )
        self._circuit.record_success()

        # --- Append structured alert strings ---
        for sa in structured_alerts:
            alerts.append(str(sa))

        # --- Compute severity summary ---
        severities = [sa.severity for sa in structured_alerts]
        top_severity = self._top_severity(severities)
        top_alert = next(
            (sa for sa in structured_alerts if sa.severity == top_severity),
            None,
        )

        # --- Multi-agent feedback ---
        if self.coordinator:
            for sa in structured_alerts:
                self.coordinator.record(
                    agent_id=self.agent_id,
                    category=sa.category,
                    success=True,
                )

        # --- HITL ---
        hitl_required = False
        hitl_approved: Optional[bool] = None
        if (
            self.hitl
            and top_severity is not None
            and self.hitl.needs_review(top_severity)
        ):
            hitl_required = True
            hitl_approved = self.hitl.request_review(
                category=(
                    top_alert.category if top_alert
                    else ",".join(sa.category for sa in structured_alerts)
                ),
                severity=top_severity,
                value=top_alert.value if top_alert else None,
                threshold=top_alert.threshold if top_alert else None,
                reason=f"top severity={top_severity}",
                urgency=(
                    "high" if top_severity == Severity.EMERGENCY.value
                    else "medium"
                ),
                context={
                    "top_alert": (
                        top_alert.to_dict() if top_alert else None
                    ),
                    "structured_alerts": [
                        sa.to_dict() for sa in structured_alerts
                    ],
                },
            )

        # --- Federated (every N reports) ---
        if self.federated and \
                report_idx % self.config.federated_push_every == 0:
            for sa in structured_alerts:
                self.federated.push(FederatedAlertProfile(
                    deployment_id=self.deployment_id,
                    category=sa.category,
                    severity=sa.severity,
                    mean_value=sa.value or 0.0,
                    sample_count=1,
                    granularity="single",
                ))

        # --- Response keys (full shape) ---
        alert_ids = [sa.alert_id for sa in structured_alerts if sa.alert_id]
        metrics["policy_alerts"] = alerts
        metrics["policy_alerts_structured"] = [
            sa.to_dict() for sa in structured_alerts
        ]
        metrics["policy_alert_severity"] = (
            top_severity or Severity.INFO.value
        )
        metrics["policy_alert_threshold_used"] = active_threshold
        metrics["policy_alert_causal_adjustment"] = causal_adjustment
        metrics["policy_hitl_required"] = hitl_required
        metrics["policy_hitl_approved"] = hitl_approved
        metrics["policy_alert_ids"] = alert_ids
        metrics["policy_alerts_missing_inputs"] = self._missing_inputs(metrics)
        metrics["policy_reporter_status"] = "ok"
        metrics["policy_reporter_degraded"] = False
        metrics["policy_reporter_degradation_reason"] = None

        # --- History ---
        with self._lock:
            for sa in structured_alerts:
                self.alert_counts[sa.category] += 1
                self.severity_counts[sa.severity] += 1
            self.alert_history.append({
                "n_alerts": len(alerts),
                "n_structured": len(structured_alerts),
                "categories": [sa.category for sa in structured_alerts],
                "severities": severities,
                "top_severity": top_severity,
                "alert_ids": alert_ids,
                "at": _utcnow().isoformat(),
            })
            self.structured_alerts_last = tuple(structured_alerts)

        # --- Metrics ---
        self._metrics.inc("report_total", tier=top_severity or "none")

        return metrics

    def report_copy(self, metrics: Dict) -> Dict:
        """Like report(), but returns a fresh dict — never mutates the input."""
        return self.report(dict(metrics) if metrics else {})

    # ------------------------------------------------------------------ degraded
    def _degraded_response(
        self,
        metrics: Dict,
        alerts: List[str],
        reason: str,
    ) -> Dict:
        """Degraded response with the full happy-path key set."""
        metrics["policy_alerts"] = alerts
        metrics["policy_alerts_structured"] = []
        metrics["policy_alert_severity"] = Severity.INFO.value
        metrics["policy_alert_threshold_used"] = float(self.threshold)
        metrics["policy_alert_causal_adjustment"] = 0.0
        metrics["policy_hitl_required"] = False
        metrics["policy_hitl_approved"] = None
        metrics["policy_alert_ids"] = []
        metrics["policy_alerts_missing_inputs"] = self._missing_inputs(metrics)
        metrics["policy_reporter_status"] = reason
        metrics["policy_reporter_degraded"] = True
        metrics["policy_reporter_degradation_reason"] = reason
        return metrics

    # ------------------------------------------------------------------ structured alerts
    def _generate_structured_alerts(
        self,
        metrics: Dict[str, Any],
        energy_pct: Optional[float],
        active_energy_threshold: float,
        threshold_state: ThresholdState,
    ) -> List[StructuredAlert]:
        out: List[StructuredAlert] = []

        # --- Pre-compute breach eligibility (lazy signal fetch, §2.11) ---
        carbon_kg = self._safe_float(metrics.get("carbon_kg"), default=None)
        helium_units = self._safe_float(
            metrics.get("helium_units"), default=None,
        )
        latency_s = self._safe_float(
            metrics.get("latency_seconds"), default=None,
        )
        cost_usd = self._safe_float(metrics.get("cost_usd"), default=None)
        precision_str = metrics.get("precision")
        precision_enum: Optional[PrecisionLevel] = None
        if precision_str is not None:
            try:
                precision_enum = (
                    precision_str if isinstance(precision_str, PrecisionLevel)
                    else PrecisionLevel(str(precision_str))
                )
            except ValueError:
                precision_enum = None

        energy_breach = (
            energy_pct is not None
            and energy_pct >= active_energy_threshold
        )
        carbon_breach = (
            self.features["carbon_awareness"]
            and carbon_kg is not None
            and carbon_kg >= float(self.carbon_threshold)
        )
        helium_breach = (
            self.features["helium_awareness"]
            and helium_units is not None
            and helium_units >= float(self.helium_threshold)
        )
        multi_cat = self.features["multi_category"]
        latency_breach = (
            multi_cat
            and latency_s is not None
            and latency_s >= float(self.latency_threshold_seconds)
        )
        cost_breach = (
            multi_cat
            and cost_usd is not None
            and cost_usd >= float(self.cost_threshold_usd)
        )
        precision_breach = (
            self.features["adaptive_precision"]
            and precision_enum is not None
            and precision_enum != PrecisionLevel.FP32
        )

        in_critical_section = bool(
            metrics.get("in_critical_section", False)
        )

        # Lazy market fetch: only when an alert will consume it.
        market_snapshot: Optional[Dict[str, Any]] = None
        if energy_breach or carbon_breach or cost_breach:
            market_snapshot = self._get_market_snapshot()

        helium_scarcity: Optional[float] = None
        if helium_breach:
            helium_scarcity = self._get_helium_scarcity()

        # --- Emit helper (dedupe + rate + critical-section gates) ---
        def emit(sa: StructuredAlert) -> None:
            if self.features["deduplication"] and self._is_duplicate(
                sa.category, sa.severity,
            ):
                self._metrics.inc(
                    "alerts_suppressed", reason="dedup",
                    category=sa.category,
                )
                return
            if not self._rate_limiter.try_acquire():
                self._metrics.inc(
                    "alerts_suppressed", reason="rate_limit",
                    category=sa.category,
                )
                return
            if not self._passes_critical_section(
                sa.severity, in_critical_section,
            ):
                self._metrics.inc(
                    "alerts_suppressed", reason="critical_section",
                    category=sa.category,
                )
                return
            if self.features["deduplication"]:
                self._commit_emit(sa.category, sa.severity)
            out.append(sa)
            self._metrics.inc(
                "alerts_emitted", category=sa.category,
                severity=sa.severity,
            )
            if self.threshold_learner is not None:
                with self._lock:
                    self._pending_alert_states[sa.alert_id or ""] = {
                        "state": threshold_state,
                        "threshold_used": active_energy_threshold,
                    }
                    while (
                        len(self._pending_alert_states)
                        > self.config.pending_alert_maxlen
                    ):
                        self._pending_alert_states.popitem(last=False)

        # --- Energy ---
        if energy_breach and energy_pct is not None:
            severity = self._severity_for(
                AlertCategory.ENERGY.value,
                energy_pct,
                active_energy_threshold,
            )
            extra: Dict[str, float] = {}
            if precision_enum is not None:
                extra["precision_weight"] = (
                    AdaptivePrecisionController.energy_multiplier(
                        precision_enum,
                    )
                )
            emit(AlertExplainer.build(
                category=AlertCategory.ENERGY,
                severity=severity,
                value=energy_pct,
                threshold=active_energy_threshold,
                extra_factors=extra,
                market_snapshot=market_snapshot,
                alert_id=self._new_alert_id(),
            ))

        # --- Carbon ---
        if carbon_breach and carbon_kg is not None:
            severity = self._severity_for(
                AlertCategory.CARBON.value, carbon_kg,
                float(self.carbon_threshold),
            )
            emit(AlertExplainer.build(
                category=AlertCategory.CARBON,
                severity=severity,
                value=carbon_kg,
                threshold=float(self.carbon_threshold),
                market_snapshot=market_snapshot,
                alert_id=self._new_alert_id(),
            ))

        # --- Helium ---
        if helium_breach and helium_units is not None:
            severity = self._severity_for(
                AlertCategory.HELIUM.value, helium_units,
                float(self.helium_threshold),
            )
            extra: Dict[str, float] = {}
            if helium_scarcity is not None:
                extra["helium_scarcity"] = float(helium_scarcity)
            emit(AlertExplainer.build(
                category=AlertCategory.HELIUM,
                severity=severity,
                value=helium_units,
                threshold=float(self.helium_threshold),
                extra_factors=extra,
                alert_id=self._new_alert_id(),
            ))

        # --- Latency ---
        if latency_breach and latency_s is not None:
            severity = self._severity_for(
                AlertCategory.LATENCY.value, latency_s,
                float(self.latency_threshold_seconds),
            )
            emit(AlertExplainer.build(
                category=AlertCategory.LATENCY,
                severity=severity,
                value=latency_s,
                threshold=float(self.latency_threshold_seconds),
                alert_id=self._new_alert_id(),
            ))

        # --- Cost ---
        if cost_breach and cost_usd is not None:
            severity = self._severity_for(
                AlertCategory.COST.value, cost_usd,
                float(self.cost_threshold_usd),
            )
            emit(AlertExplainer.build(
                category=AlertCategory.COST,
                severity=severity,
                value=cost_usd,
                threshold=float(self.cost_threshold_usd),
                market_snapshot=market_snapshot,
                alert_id=self._new_alert_id(),
            ))

        # --- Precision (informational, §2.1) ---
        if precision_breach and precision_enum is not None:
            multiplier = AdaptivePrecisionController.energy_multiplier(
                precision_enum,
            )
            emit(AlertExplainer.build(
                category=AlertCategory.PRECISION,
                severity=Severity.INFO,
                value=multiplier,
                threshold=1.0,  # FP32 baseline
                extra_factors={
                    "precision_level_index":
                        float(list(PrecisionLevel).index(precision_enum)),
                },
                alert_id=self._new_alert_id(),
            ))

        # --- Temporal monitor (passive observation only) ---
        if self.temporal_monitor:
            ctx = {
                "alerts_last_minute": self._rate_limiter.count_last_minute(),
                "in_critical_section": in_critical_section,
                "filtered_alert_count": 0,  # we filter upstream
            }
            ok, violations = self.temporal_monitor.verify(ctx)
            if not ok:
                self._metrics.inc(
                    "temporal_violations_total",
                    properties=",".join(violations),
                )
            # Violations are recorded but never emitted as alerts —
            # emitting an alert about an alert storm is self-amplifying.

        return out

    def _missing_inputs(self, metrics: Mapping[str, Any]) -> List[str]:
        """Which known metric keys are absent or non-numeric?"""
        missing: List[str] = []
        for key in (
            "energy_pct", "carbon_kg", "helium_units",
            "latency_seconds", "cost_usd",
        ):
            if self._safe_float(metrics.get(key), default=None) is None:
                missing.append(key)
        return missing

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

    def record_outcome(
        self,
        was_false_positive: bool,
        *,
        alert_id: Optional[str] = None,
    ) -> bool:
        """
        Feed back whether an alert was a false positive.

        Returns True if learning happened, False otherwise. The target
        alert is either ``alert_id`` (from ``result["policy_alert_ids"]``)
        or the most recently emitted alert if unspecified.
        """
        if not self.threshold_learner:
            return False

        with self._lock:
            if alert_id is None:
                if not self._pending_alert_states:
                    logger.warning(
                        "record_outcome: no pending alert to learn from",
                    )
                    self._metrics.inc("outcome_unmatched")
                    return False
                alert_id = next(reversed(self._pending_alert_states))
            entry = self._pending_alert_states.pop(alert_id, None)

        if entry is None:
            logger.warning(
                "record_outcome: unknown alert_id=%s", alert_id,
            )
            self._metrics.inc("outcome_unmatched")
            return False

        reward = 0.0 if was_false_positive else 1.0
        self.threshold_learner.record(
            entry["state"], reward, entry["threshold_used"],
        )
        self._metrics.observe("outcome_reward", reward)
        if self.threshold_learner.observations % \
                self.config.causal_update_every == 0:
            self.threshold_learner.update()
        return True

    def contribute_federated(self) -> None:
        if not self.federated:
            return
        with self._lock:
            snapshot = list(self.alert_history)
        category_values: Dict[str, List[float]] = defaultdict(list)
        category_severity: Dict[str, List[str]] = defaultdict(list)
        for rec in snapshot:
            for cat, sev in zip(
                rec.get("categories", []), rec.get("severities", []),
            ):
                category_values[cat].append(1.0)  # frequency
                category_severity[cat].append(sev)
        for cat, values in category_values.items():
            if len(values) < 3:
                continue
            self.federated.push(FederatedAlertProfile(
                deployment_id=self.deployment_id,
                category=cat,
                severity=self._top_severity(category_severity[cat])
                or Severity.INFO.value,
                mean_value=float(len(values)),
                sample_count=len(values),
                granularity="aggregate",
            ))

    def get_federated_aggregate(self) -> Dict[str, Dict[str, float]]:
        return self.federated.aggregate() if self.federated else {}

    def distill_alert_policy(
        self, urgency: str = "normal",
    ) -> Optional[DistilledAlertPolicy]:
        if not self.distiller:
            return None
        precision = (
            self.precision_ctl.select(urgency)
            if self.precision_ctl else PrecisionLevel.INT8
        )
        return self.distiller.distill(
            base_threshold=float(self.threshold),
            precision=precision,
        )

    def explain_last_alerts(self) -> List[StructuredAlert]:
        with self._lock:
            return list(self.structured_alerts_last)

    # ------------------------------------------------------------------ health
    def health(self) -> Dict[str, Any]:
        return {
            "ready": self._circuit.state != CircuitState.OPEN,
            "circuit": self._circuit.state.value,
            "reports_emitted": self.report_count,
            "current_rate_per_minute": self._rate_limiter.count_last_minute(),
            "uptime_seconds": time.monotonic() - self._started_mono,
            "signals": {
                "market": self.market is not None,
                "helium": self.helium_signal_fn is not None,
            },
        }

    # ------------------------------------------------------------------ statistics
    def get_statistics(self) -> Dict[str, Any]:
        with self._lock:
            stats: Dict[str, Any] = {
                "deployment_id": self.deployment_id,
                "agent_id": self.agent_id,
                "report_count": self.report_count,
                "alert_category_counts": dict(self.alert_counts),
                "alert_severity_counts": dict(self.severity_counts),
                "base_threshold": float(self.threshold),
                "active_threshold": (
                    self.threshold_learner.predict_threshold(
                        self._build_threshold_state({})
                    ) if self.threshold_learner else float(self.threshold)
                ),
                "circuit": {
                    "state": self._circuit.state.value,
                    "failures": self._circuit.failures,
                },
                "rate_limiter": {
                    "max_per_minute": self.config.max_alert_rate_per_minute,
                    "current_per_minute":
                        self._rate_limiter.count_last_minute(),
                },
                "dedup_recent_count": len(self._recent_alerts),
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
        if self.threshold_learner:
            stats["causal_learner"] = {
                "observations": self.threshold_learner.observations,
                "updates": self.threshold_learner.updates,
                "last_advantage_mean":
                    self.threshold_learner.last_advantage_mean,
            }
        if self.federated:
            stats["federated_aggregate"] = self.federated.aggregate()
        stats["metrics"] = self._metrics.snapshot()
        return stats


__all__ = [
    "AdaptivePrecisionController",
    "AgentRole",
    "AlertCategory",
    "AlertExplainer",
    "CausalThresholdLearner",
    "CircuitBreaker",
    "CircuitState",
    "DistilledAlertPolicy",
    "FederatedAggregator",
    "FederatedAlertProfile",
    "HITLRequest",
    "HardwareProfile",
    "HumanInTheLoopGate",
    "MarketSnapshot",
    "MetricsRecorder",
    "MultiAgentCoordinator",
    "PolicyReporter",
    "PolicyReporterConfig",
    "PrecisionLevel",
    "QuantumDistillationBridge",
    "Severity",
    "SeverityRatios",
    "StructuredAlert",
    "TemporalLogicMonitor",
    "ThresholdState",
]


# =============================================================================
# Demo
# =============================================================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    class _MockHelium:
        scarcity_score = 0.7

    def auto_approve(req: HITLRequest) -> bool:
        logger.info("[HITL] auto-approve: %s", req.reason)
        return True

    # --- Legacy mode ---
    print("\n=== Legacy mode (all features off) ===")
    legacy = PolicyReporter(
        policy={"reporting": {"alert_threshold_energy_pct": 80}},
        features={k: False for k in PolicyReporter.default_features()},
    )
    result = legacy.report({"energy_pct": 85})
    print(f"  policy_alerts: {result['policy_alerts']}")
    legacy.close()

    # --- Enhanced mode ---
    print("\n=== Enhanced mode (all features on) ===")
    reporter = PolicyReporter(
        policy={"reporting": {
            "alert_threshold_energy_pct": 80,
            "alert_threshold_carbon_kg": 0.5,
            "alert_threshold_helium_units": 0.3,
            "alert_threshold_latency_seconds": 200.0,
            "alert_threshold_cost_usd": 0.5,
        }},
        deployment_id="us-ca-prod-01",
        agent_id="reporter-A",
        hardware=HardwareProfile(supports_int4=True, vram_gb=48),
        dedup_window_seconds=2.0,
    )
    reporter.set_hitl_callback(auto_approve)
    reporter.helium_signal_fn = lambda: _MockHelium()

    m1 = {
        "energy_pct": 92,
        "carbon_kg": 0.7,
        "helium_units": 0.4,
        "latency_seconds": 250.0,
        "cost_usd": 0.6,
        "precision": "int4",
        "workload_id": "training",
    }
    r1 = reporter.report(m1)
    print("\n  --- Report 1 (multi-category) ---")
    for a in r1["policy_alerts"]:
        print(f"    • {a}")
    print(f"  severity:      {r1['policy_alert_severity']}")
    print(f"  threshold:     {r1['policy_alert_threshold_used']:.2f}")
    print(f"  causal adj:    {r1['policy_alert_causal_adjustment']:+.4f}")
    print(f"  alert_ids:     {r1['policy_alert_ids']}")
    print(f"  missing:       {r1['policy_alerts_missing_inputs']}")
    print(f"  HITL:          {r1['policy_hitl_required']} "
          f"(approved={r1['policy_hitl_approved']})")
    for sa in r1["policy_alerts_structured"]:
        print(f"    [{sa['severity']}] {sa['category']}: {sa['headline']}")

    # Dedupe test (same metrics within window).
    print("\n  --- Report 2 (dedupe — expect 0 structured alerts) ---")
    r2 = reporter.report(dict(m1))
    print(f"  structured alerts: {len(r2['policy_alerts_structured'])}")

    import time as _t
    _t.sleep(2.1)
    print("\n  --- Report 3 (after dedupe window — expect re-emit) ---")
    r3 = reporter.report(dict(m1))
    print(f"  structured alerts: {len(r3['policy_alerts_structured'])}")

    # Missing-input test.
    print("\n  --- Report 4 (missing inputs) ---")
    r4 = reporter.report({"carbon_kg": 0.9})
    print(f"  missing: {r4['policy_alerts_missing_inputs']}")
    print(f"  alerts:  {r4['policy_alerts']}")

    # Critical-section test.
    print("\n  --- Report 5 (critical section: only critical+ pass) ---")
    r5 = reporter.report({
        "energy_pct": 92,
        "carbon_kg": 2.0,
        "in_critical_section": True,
    })
    print(f"  emitted categories: "
          f"{[sa['category'] for sa in r5['policy_alerts_structured']]}")

    # Rate limiter test.
    print("\n  --- Rate limiter test (rapid fire) ---")
    before = reporter._metrics.counters.get(
        'alerts_suppressed{category="energy",reason="rate_limit"}', 0,
    )
    for _ in range(150):
        reporter.report({"energy_pct": 95})
    after = reporter._metrics.counters.get(
        'alerts_suppressed{category="energy",reason="rate_limit"}', 0,
    )
    print(f"  rate-limit suppressions: {after - before}")

    # Outcome feedback (targeted).
    if r1["policy_alert_ids"]:
        ok = reporter.record_outcome(
            was_false_positive=False,
            alert_id=r1["policy_alert_ids"][0],
        )
        print(f"\n  record_outcome(alert_id) -> {ok}")

    # Federated.
    reporter.contribute_federated()
    print(f"\n  federated: {reporter.get_federated_aggregate()}")

    # Distill.
    dist = reporter.distill_alert_policy(urgency="normal")
    if dist:
        print(f"\n=== Distilled Alert Policy ===")
        print(f"  Precision:      {dist.precision.value}")
        print(f"  Base threshold: {dist.base_threshold}")
        print(f"  Retention:      {dist.quality_retention}")

    # Degraded path (chaos).
    print("\n=== Degraded path (chaos) ===")
    reporter.chaos = ChaosInjector(failure_rate=1.0)
    rd = reporter.report({"energy_pct": 92})
    print(f"  degraded:  {rd['policy_reporter_degraded']}")
    print(f"  reason:    {rd['policy_reporter_degradation_reason']}")
    required = [
        "policy_alert_severity", "policy_alert_threshold_used",
        "policy_alert_ids", "policy_hitl_required",
    ]
    print(f"  all keys:  {all(k in rd for k in required)}")

    # Health.
    print("\n=== Health ===")
    print(reporter.health())

    # Statistics.
    import json
    print("\n=== Statistics ===")
    print(json.dumps(reporter.get_statistics(), indent=2, default=str))

    reporter.close()
    print("\nSmoke test passed.")
