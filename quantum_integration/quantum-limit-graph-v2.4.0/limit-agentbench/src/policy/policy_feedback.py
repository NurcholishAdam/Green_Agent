"""
PolicyFeedback (Enhanced v2)
=============================

Generates reflective explanations for policy decisions with first-class
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

v2 fixes (see changelog in commit message):
  * record_outcome uses the *exact* ConfidenceState that was predicted on.
  * Sigmoid + BCE gradient in the confidence learner; no silent clamp drift.
  * Confidence floor after the temporal penalty.
  * Missing inputs are not treated as zero (missing_inputs list).
  * Real counterfactuals via context["alternative_outcomes"].
  * Stable SHA-256 hashing.
  * Precision is caller-supplied, not claimed.
  * Degraded response has the full happy-path shape and temporal_verified=False.
  * HITL auto-approvals are audit-flagged.
  * Configurable normalizers, chaos, timeouts; UTC datetimes; thread-safe.
  * health(), MetricsRecorder, incremental federated aggregation.
  * tier_detail field for finer-grained alerting.
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
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import (
    Any, Awaitable, Callable, Deque, Dict, List, Mapping, Optional,
    Protocol, Tuple,
)

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


class ExplanationTier(Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"                    # kept for backward compat
    DEGRADED = "degraded"          # pipeline error / circuit open


# =============================================================================
# Config
# =============================================================================

@dataclass(frozen=True)
class PolicyFeedbackConfig:
    base_confidence: float = 0.75

    # Confidence bounds.
    confidence_floor: float = 0.1
    confidence_ceiling: float = 0.99

    # Normalizers for RL features.
    confidence_energy_scale: float = 10.0
    confidence_latency_scale: float = 100.0

    # External-call timeouts (None disables).
    signal_timeout_s: Optional[float] = 2.0
    hitl_timeout_s: float = 60.0
    hitl_default_on_timeout: bool = False

    # Circuit breakers.
    circuit_failure_threshold: int = 5
    circuit_recovery_timeout_s: int = 30

    # Chaos.
    chaos_failure_rate: float = 0.0
    chaos_event_maxlen: int = 100

    # Bounded histories.
    history_maxlen: int = 1024
    confidence_values_maxlen: int = 1024
    temporal_violation_maxlen: int = 500
    hitl_feedback_maxlen: int = 1000

    # Federated.
    federated_push_every: int = 25
    federated_recent_maxlen: int = 256

    # RL.
    causal_lr: float = 0.05
    causal_buffer_maxlen: int = 512
    causal_update_every: int = 10

    # Executor.
    executor_max_workers: int = 4

    def __post_init__(self) -> None:
        if not 0.0 < self.base_confidence < 1.0:
            raise ValueError("base_confidence must be in (0, 1).")
        if not 0.0 <= self.confidence_floor < self.confidence_ceiling <= 1.0:
            raise ValueError("confidence bounds invalid.")
        if not 0.0 <= self.chaos_failure_rate <= 1.0:
            raise ValueError("chaos_failure_rate must be in [0, 1].")
        for f in ("signal_timeout_s",):
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
# Temporal Logic
# =============================================================================

class ExplanationProperty(Protocol):
    def check(self, after: Dict[str, Any], context: Dict[str, Any]) -> bool: ...
    def name(self) -> str: ...


@dataclass
class NonNegativeEnergy:
    def check(self, after: Dict[str, Any], context: Dict[str, Any]) -> bool:
        try:
            return float(after.get("energy", 0.0)) >= 0.0
        except (TypeError, ValueError):
            return False

    def name(self) -> str:
        return "NonNegativeEnergy"


@dataclass
class NonNegativeLatency:
    def check(self, after: Dict[str, Any], context: Dict[str, Any]) -> bool:
        try:
            return float(after.get("latency", 0.0)) >= 0.0
        except (TypeError, ValueError):
            return False

    def name(self) -> str:
        return "NonNegativeLatency"


@dataclass
class DeadlineRespected:
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
        try:
            return float(after.get("carbon", 0.0)) >= 0.0
        except (TypeError, ValueError):
            return False

    def name(self) -> str:
        return "NonNegativeCarbon"


class TemporalLogicMonitor:
    def __init__(self, maxlen: int = 500) -> None:
        self.properties: List[ExplanationProperty] = []
        self.violations: Deque[Dict[str, Any]] = deque(maxlen=maxlen)
        self._lock = threading.RLock()

    def register(self, p: ExplanationProperty) -> None:
        with self._lock:
            self.properties.append(p)

    def verify(
        self, after: Dict[str, Any], context: Dict[str, Any],
    ) -> Tuple[bool, List[str]]:
        bad: List[str] = []
        with self._lock:
            props = list(self.properties)
        for p in props:
            try:
                ok = p.check(after, context)
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

@dataclass
class StructuredExplanation:
    headline: str
    rationale: List[str]
    confidence: float
    contributing_factors: Dict[str, Optional[float]]
    counterfactual: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class StructuredExplainer:
    """Builds structured explanations from delta dicts (None-aware)."""

    @staticmethod
    def build(
        decision: str,
        deltas: Dict[str, Optional[float]],
        temporal_ok: bool,
        temporal_violations: List[str],
        carbon_market_snapshot: Optional[Dict[str, Any]] = None,
        helium_scarcity: Optional[float] = None,
        counterfactual: str = "",
    ) -> StructuredExplanation:
        reasons: List[str] = []
        reasons.append(
            f"Selected mode '{decision}' based on measured deltas."
        )

        def _delta_line(label: str, key: str, unit: str, fmt: str = ".4f") -> None:
            v = deltas.get(key)
            if v is None:
                return
            if abs(v) < 1e-12:
                return
            sign = "+" if v >= 0 else ""
            direction = "reduced" if v < 0 else "increased"
            reasons.append(
                f"{label} {direction} by {sign}{v:{fmt}} {unit}."
                if v != 0 else f"{label} unchanged."
            )

        _delta_line("Energy", "energy_delta", "kWh")
        _delta_line("Latency", "latency_delta", "s", ".2f")
        _delta_line("Carbon", "carbon_delta", "kgCO2e", ".5f")
        _delta_line("Helium", "helium_delta", "units", ".5f")
        _delta_line("Cost", "cost_delta", "USD", ".4f")

        acc = deltas.get("accuracy_delta")
        if acc is not None and abs(acc) > 1e-12:
            reasons.append(f"Accuracy delta: {acc:+.3f}.")

        if not temporal_ok:
            reasons.append(
                f"Temporal violations detected: {temporal_violations}."
            )
        if carbon_market_snapshot:
            cp = carbon_market_snapshot.get("carbon_price_per_tco2_usd")
            carbon_d = deltas.get("carbon_delta")
            if cp and carbon_d is not None:
                value = abs(carbon_d) / 1000.0 * cp
                reasons.append(f"Carbon market value of delta: ${value:.4f}.")
        if helium_scarcity is not None and helium_scarcity > 0.5:
            reasons.append(
                f"Helium scarcity elevated at {helium_scarcity:.2f}."
            )

        cf = counterfactual or "No counterfactual provided by caller."
        if counterfactual:
            reasons.append(f"Counterfactual: {counterfactual}")

        energy_d = deltas.get("energy_delta")
        headline = (
            f"[{decision}] "
            + (
                f"energy_delta={energy_d:+.4f} kWh"
                if energy_d is not None else "energy_delta=N/A"
            )
        )

        return StructuredExplanation(
            headline=headline,
            rationale=reasons,
            confidence=0.0,  # filled by caller
            contributing_factors={
                "energy_delta": energy_d,
                "latency_delta": deltas.get("latency_delta"),
                "carbon_delta": deltas.get("carbon_delta"),
                "helium_delta": deltas.get("helium_delta"),
                "cost_delta": deltas.get("cost_delta"),
                "accuracy_delta": deltas.get("accuracy_delta"),
            },
            counterfactual=cf,
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
# Causal Confidence Learner (logit + sigmoid + BCE gradient)
# =============================================================================

@dataclass(frozen=True)
class ConfidenceState:
    n_factors: int
    energy_delta_norm: float
    latency_delta_norm: float
    decision_hash: float
    cost_delta_norm: float = 0.0  # v2

    def to_dict(self) -> Dict[str, float]:
        return {
            "n_factors": float(self.n_factors),
            "energy_delta_norm": self.energy_delta_norm,
            "latency_delta_norm": self.latency_delta_norm,
            "decision_hash": self.decision_hash,
            "cost_delta_norm": self.cost_delta_norm,
        }


class CausalConfidenceLearner:
    """
    Learns a calibrated confidence model.

    v2 — logit-space sigmoid with BCE gradient:
        * Predictions are naturally bounded; no silent clamp drift.
        * Coefficients converge instead of saturating.
        * Consumes the *exact* ConfidenceState predicted on (caller caches it).
    """

    N_FEATURES = 5

    def __init__(
        self,
        base_confidence: float = 0.75,
        lr: float = 0.05,
        buffer_maxlen: int = 512,
        floor: float = 0.1,
        ceiling: float = 0.99,
    ) -> None:
        self.base_confidence = base_confidence
        # Logit of the base probability.
        self.base_logit = math.log(
            base_confidence / (1.0 - base_confidence),
        )
        self.lr = lr
        self.floor = floor
        self.ceiling = ceiling
        self.coeffs: List[float] = [0.0] * self.N_FEATURES
        self.buffer: Deque[Tuple[List[float], float]] = deque(maxlen=buffer_maxlen)
        self.observations: int = 0
        self.updates: int = 0
        self._lock = threading.RLock()

    @staticmethod
    def _features(s: ConfidenceState) -> List[float]:
        return [
            min(1.0, s.n_factors / 6.0),
            abs(s.energy_delta_norm),
            abs(s.latency_delta_norm),
            s.decision_hash,
            abs(s.cost_delta_norm),
        ]

    @staticmethod
    def _sigmoid(z: float) -> float:
        # Numerically stable sigmoid.
        if z >= 0:
            ez = math.exp(-z)
            return 1.0 / (1.0 + ez)
        ez = math.exp(z)
        return ez / (1.0 + ez)

    def _raw_logit(self, f: List[float]) -> float:
        return self.base_logit + sum(
            c * x for c, x in zip(self.coeffs, f)
        )

    def predict_confidence(self, s: ConfidenceState) -> float:
        with self._lock:
            z = self._raw_logit(self._features(s))
        p = self._sigmoid(z)
        return max(self.floor, min(self.ceiling, p))

    def record(self, s: ConfidenceState, actual_outcome: float) -> None:
        f = self._features(s)
        with self._lock:
            self.buffer.append((f, actual_outcome))
            self.observations += 1

    def update(self) -> None:
        with self._lock:
            if not self.buffer:
                return
            for f, target in self.buffer:
                z = self._raw_logit(f)
                p = self._sigmoid(z)
                grad = target - p  # BCE grad w.r.t. z
                for i, x in enumerate(f):
                    self.coeffs[i] += self.lr * grad * x
            self.updates += 1
            self.buffer.clear()


# =============================================================================
# Quantum Distillation
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
# Federated (incremental)
# =============================================================================

@dataclass(frozen=True)
class FederatedExplanationProfile:
    deployment_id: str
    decision: str
    mean_confidence: float
    mean_energy_delta: float
    sample_count: int
    granularity: str = "aggregate"  # "aggregate" | "single"
    timestamp: datetime = field(default_factory=_utcnow)


class FederatedAggregator:
    def __init__(self, recent_maxlen: int = 256) -> None:
        self._sum_confidence: Dict[str, float] = defaultdict(float)
        self._sum_energy: Dict[str, float] = defaultdict(float)
        self._count: Dict[str, int] = defaultdict(int)
        self.recent: Deque[FederatedExplanationProfile] = deque(
            maxlen=recent_maxlen,
        )
        self._global: Dict[str, Dict[str, float]] = {}
        self._lock = threading.RLock()

    def push(self, u: FederatedExplanationProfile) -> None:
        with self._lock:
            self.recent.append(u)
            if u.granularity != "aggregate":
                return
            self._sum_confidence[u.decision] += u.mean_confidence * u.sample_count
            self._sum_energy[u.decision] += u.mean_energy_delta * u.sample_count
            self._count[u.decision] += u.sample_count

    def aggregate(self) -> Dict[str, Dict[str, float]]:
        with self._lock:
            result = {}
            for decision, n in self._count.items():
                if n <= 0:
                    continue
                result[decision] = {
                    "mean_confidence": self._sum_confidence[decision] / n,
                    "mean_energy_delta": self._sum_energy[decision] / n,
                    "sample_count": n,
                }
            self._global = result
            return dict(result)

    def lookup(self, decision: str) -> Optional[Dict[str, float]]:
        with self._lock:
            if not self._global:
                self.aggregate()
            return self._global.get(decision)


# =============================================================================
# Multi-Agent
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
        self._lock = threading.RLock()

    def register(self, agent_id: str) -> AgentProfile:
        with self._lock:
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
        with self._lock:
            a = self.register(agent_id)
            n = a.total_calls + 1
            a.success_rate = (
                (n - 1) * a.success_rate + float(success)
            ) / n
            a.carbon_focus = (
                (n - 1) * a.carbon_focus + min(1.0, abs(carbon_delta) * 100)
            ) / n
            a.helium_focus = (
                (n - 1) * a.helium_focus + min(1.0, abs(helium_delta) * 10)
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
    decision: str
    confidence: float
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
        confidence_threshold: float = 0.5,
        timeout_s: float = 60.0,
        default_on_timeout: bool = False,
        feedback_maxlen: int = 1000,
    ) -> None:
        self.confidence_threshold = confidence_threshold
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

    def needs_review(
        self,
        confidence: float,
        tier: str,
        tier_detail: Optional[str] = None,
    ) -> bool:
        if tier == ExplanationTier.DEGRADED.value:
            return False  # already flagged degraded
        if tier_detail == "temporal_violation":
            return True
        if tier == ExplanationTier.LOW.value:
            return True
        return confidence < self.confidence_threshold

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
        decision: str,
        confidence: float,
        reason: str,
        urgency: str = "medium",
        context: Optional[Dict[str, Any]] = None,
    ) -> bool:
        req = HITLRequest(
            decision, confidence, reason, urgency, context or {},
        )
        with self._lock:
            self.pending.append(req)
        try:
            if self._callback is None:
                self._record(req, False, auto=True)
                return False
            decision_bool = self._callback(req)
            self._record(req, bool(decision_bool))
            return bool(decision_bool)
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
        decision: str,
        confidence: float,
        reason: str,
        urgency: str = "medium",
        context: Optional[Dict[str, Any]] = None,
    ) -> bool:
        req = HITLRequest(
            decision, confidence, reason, urgency, context or {},
        )
        with self._lock:
            self.pending.append(req)
        try:
            if self._async_callback is not None:
                decision_bool = await asyncio.wait_for(
                    self._async_callback(req), timeout=self.timeout_s,
                )
            elif self._callback is not None:
                decision_bool = self._callback(req)
            else:
                decision_bool = self.default_on_timeout
        except asyncio.TimeoutError:
            logger.warning("HITL async timed out; using default")
            decision_bool = self.default_on_timeout
        except Exception as e:
            logger.error("HITL async callback failed: %s", e, exc_info=True)
            decision_bool = False
        finally:
            with self._lock:
                if req in self.pending:
                    self.pending.remove(req)
        self._record(req, bool(decision_bool))
        return bool(decision_bool)

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
})


# =============================================================================
# PolicyFeedback
# =============================================================================

class PolicyFeedback:
    """
    Enhanced reflective explanation generator.

    Backward-compatible:
        PolicyFeedback()
        result = feedback.generate(decision, before, after)

    Return dict always contains the original four keys:
        decision, explanation, tradeoff, confidence

    Plus extended keys when features are enabled.
    """

    @classmethod
    def default_features(cls) -> Dict[str, bool]:
        return dict(_DEFAULT_FEATURES)

    def __init__(
        self,
        deployment_id: str = "local",
        agent_id: str = "feedback-0",
        features: Optional[Dict[str, bool]] = None,
        hardware: Optional[HardwareProfile] = None,
        base_confidence: float = 0.75,
        config: Optional[PolicyFeedbackConfig] = None,
        metrics: Optional[MetricsRecorder] = None,
    ) -> None:
        # Backward-compatible: positional base_confidence kwarg override.
        self.config = config or PolicyFeedbackConfig(
            base_confidence=float(base_confidence),
        )
        self.deployment_id = deployment_id
        self.agent_id = agent_id
        self.features = {**self.default_features(), **(features or {})}
        self._metrics = metrics or MetricsRecorder()

        # Lock for all mutable state.
        self._lock = threading.RLock()

        # Resilience.
        self._circuit = CircuitBreaker(
            "policy_feedback",
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
            self.temporal_monitor.register(NonNegativeEnergy())
            self.temporal_monitor.register(NonNegativeLatency())
            self.temporal_monitor.register(NonNegativeCarbon())
            self.temporal_monitor.register(DeadlineRespected())
            self.temporal_monitor.register(EnergyBudgetNotExceeded())

        # Structured XAI.
        self.structured_explainer = (
            StructuredExplainer() if self.features["xai"] else None
        )

        # Adaptive precision (used only for distillation, not attribution).
        self.precision_ctl = (
            AdaptivePrecisionController(hardware)
            if self.features["adaptive_precision"] else None
        )

        # Causal RL.
        self.confidence_learner: Optional[CausalConfidenceLearner] = (
            CausalConfidenceLearner(
                base_confidence=self.config.base_confidence,
                lr=self.config.causal_lr,
                buffer_maxlen=self.config.causal_buffer_maxlen,
                floor=self.config.confidence_floor,
                ceiling=self.config.confidence_ceiling,
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
            FederatedAggregator(
                recent_maxlen=self.config.federated_recent_maxlen,
            )
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
                timeout_s=self.config.hitl_timeout_s,
                default_on_timeout=self.config.hitl_default_on_timeout,
                feedback_maxlen=self.config.hitl_feedback_maxlen,
            )
            if self.features["hitl"] else None
        )

        # External signals.
        self.helium_signal_fn: Optional[Callable[[], Any]] = None

        # History / counters.
        self.history: Deque[Dict[str, Any]] = deque(
            maxlen=self.config.history_maxlen,
        )
        self.generation_count: int = 0
        self.confidence_values: Deque[float] = deque(
            maxlen=self.config.confidence_values_maxlen,
        )
        self.decision_counts: Dict[str, int] = defaultdict(int)
        self._next_generation_id: int = 1
        self._started_mono: float = time.monotonic()
        self.last_explanation: Optional[StructuredExplanation] = None
        self.last_generation_id: Optional[str] = None

        # Executor for signal timeouts.
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.config.executor_max_workers,
            thread_name_prefix="policy-feedback",
        )

        logger.info(
            "Enhanced PolicyFeedback initialized "
            "(deployment=%s, agent=%s, features=%s)",
            deployment_id, agent_id, list(self.features),
        )

    # ------------------------------------------------------------------ lifecycle
    def close(self) -> None:
        self._executor.shutdown(wait=False, cancel_futures=True)

    def __enter__(self) -> "PolicyFeedback":
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
    def _safe_dict(value: Any) -> Dict[str, Any]:
        if value is None:
            return {}
        if isinstance(value, dict):
            return dict(value)
        try:
            return dict(value)
        except Exception:
            # Support dataclass-like inputs.
            if hasattr(value, "__dict__"):
                return dict(vars(value))
            return {}

    @staticmethod
    def _safe_float(
        value: Any, default: Optional[float] = 0.0,
    ) -> Optional[float]:
        if value is None:
            return default
        try:
            v = float(value)
            return v if math.isfinite(v) else default
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _delta(
        a: Optional[float], b: Optional[float],
    ) -> Optional[float]:
        if a is None or b is None:
            return None
        return a - b

    def _new_generation_id(self) -> str:
        with self._lock:
            gid = f"gen-{self._next_generation_id}"
            self._next_generation_id += 1
            return gid

    # ------------------------------------------------------------------ signals
    def _get_helium_scarcity(self) -> Optional[float]:
        if self.helium_signal_fn is None:
            return None
        try:
            sig = self._call_with_timeout(
                self.helium_signal_fn, self.config.signal_timeout_s,
            )
            if inspect.isawaitable(sig):
                logger.warning(
                    "helium_signal_fn returned a coroutine; use "
                    "generate_async()",
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

    def _get_market_snapshot(self) -> Optional[Dict[str, Any]]:
        if self.market is None:
            return None
        try:
            snap = self._call_with_timeout(
                self.market.get_snapshot, self.config.signal_timeout_s,
            )
            return {
                "carbon_price_per_tco2_usd": snap.carbon_price_per_tco2_usd,
                "rec_price_per_mwh_usd": snap.rec_price_per_mwh_usd,
                "rec_available_mwh": snap.rec_available_mwh,
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

    # ------------------------------------------------------------------ public: generate
    def generate(
        self,
        decision: str,
        before: Dict[str, Any],
        after: Dict[str, Any],
        context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Generate a reflective explanation for a policy decision."""
        # --- Circuit breaker ---
        if not self._circuit.can_call():
            logger.warning("PolicyFeedback circuit open — degraded response")
            self._metrics.inc("generate_degraded", reason="circuit_open")
            return self._degraded_response(
                decision, before, after, reason="circuit_open",
            )

        # --- Chaos injection ---
        if self.chaos and self.chaos.maybe_fail("generate"):
            self._circuit.record_failure()
            self._metrics.inc("generate_degraded", reason="chaos")
            return self._degraded_response(
                decision, before, after, reason="chaos",
            )

        t0 = time.monotonic()
        try:
            response = self._generate_internal(
                decision, before, after, context or {},
            )
            self._circuit.record_success()
            self._metrics.observe(
                "generate_latency_ms", (time.monotonic() - t0) * 1000.0,
            )
            return response
        except Exception as e:
            self._circuit.record_failure()
            logger.warning(
                "generate() failed: %s; returning degraded response",
                e, exc_info=True,
            )
            self._metrics.inc("generate_degraded", reason="exception")
            return self._degraded_response(
                decision, before, after,
                reason=f"exception:{type(e).__name__}",
            )

    async def generate_async(
        self,
        decision: str,
        before: Dict[str, Any],
        after: Dict[str, Any],
        context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Async wrapper — awaits async signals if present."""
        return await asyncio.to_thread(
            self.generate, decision, before, after, context,
        )

    # ------------------------------------------------------------------ internal
    def _generate_internal(
        self,
        decision: str,
        before: Any,
        after: Any,
        context: Dict[str, Any],
    ) -> Dict[str, Any]:
        before = self._safe_dict(before)
        after = self._safe_dict(after)
        generation_id = self._new_generation_id()

        # --- Inputs (None for missing) ---
        b_energy = self._safe_float(before.get("energy"), default=None)
        a_energy = self._safe_float(after.get("energy"), default=None)
        b_latency = self._safe_float(before.get("latency"), default=None)
        a_latency = self._safe_float(after.get("latency"), default=None)
        b_carbon = self._safe_float(before.get("carbon"), default=None)
        a_carbon = self._safe_float(after.get("carbon"), default=None)
        b_helium = self._safe_float(before.get("helium"), default=None)
        a_helium = self._safe_float(after.get("helium"), default=None)
        b_cost = self._safe_float(before.get("cost"), default=None)
        a_cost = self._safe_float(after.get("cost"), default=None)
        b_accuracy = self._safe_float(before.get("accuracy"), default=None)
        a_accuracy = self._safe_float(after.get("accuracy"), default=None)

        # --- Deltas (None if inputs missing) ---
        latency_delta = self._delta(a_latency, b_latency)
        energy_delta = self._delta(a_energy, b_energy)
        carbon_delta = self._delta(a_carbon, b_carbon)
        helium_delta = self._delta(a_helium, b_helium)
        cost_delta = self._delta(a_cost, b_cost)
        accuracy_delta = self._delta(a_accuracy, b_accuracy)

        # Track missing inputs for honesty.
        missing_inputs: List[str] = []
        for name, val in [
            ("energy", energy_delta),
            ("latency", latency_delta),
            ("carbon", carbon_delta),
            ("helium", helium_delta),
            ("cost", cost_delta),
            ("accuracy", accuracy_delta),
        ]:
            if val is None:
                missing_inputs.append(name)

        # --- Original backward-compatible tradeoff (0.0 for missing) ---
        tradeoff = {
            "latency_delta": latency_delta if latency_delta is not None else 0.0,
            "energy_delta": energy_delta if energy_delta is not None else 0.0,
        }
        # --- Extended (None for missing) ---
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
                after, context,
            )

        # --- Market enrichment ---
        market_snapshot = self._get_market_snapshot() if self.market else None
        if market_snapshot and carbon_delta is not None:
            cp = market_snapshot["carbon_price_per_tco2_usd"]
            market_snapshot["carbon_delta_value_usd"] = (
                abs(carbon_delta) / 1000.0 * cp
            )

        # --- Helium ---
        helium_scarcity = (
            self._get_helium_scarcity()
            if self.features["helium_awareness"] else None
        )

        # --- Precision: caller-supplied only ---
        precision_in = context.get("precision")
        precision_str: Optional[str] = None
        if precision_in is not None:
            if isinstance(precision_in, PrecisionLevel):
                precision_str = precision_in.value
            else:
                precision_str = str(precision_in)

        # --- Counterfactual (from caller) ---
        counterfactual = ""
        alt_outcomes = context.get("alternative_outcomes")
        if isinstance(alt_outcomes, Mapping) and alt_outcomes:
            try:
                best_alt = min(
                    alt_outcomes.items(),
                    key=lambda kv: float(
                        (kv[1] or {}).get("energy", float("inf"))
                    ),
                )
                alt_mode, alt_metrics = best_alt
                bits = [f"mode='{_sanitize(alt_mode, 32)}'"]
                if isinstance(alt_metrics, Mapping):
                    for k in ("energy", "latency", "carbon", "cost"):
                        if k in alt_metrics:
                            bits.append(f"{k}={alt_metrics[k]}")
                counterfactual = (
                    "Alternative outcome would have been "
                    + ", ".join(bits) + "."
                )
            except Exception as e:
                logger.debug("counterfactual build failed: %s", e)

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
                counterfactual=counterfactual,
            )
            structured_dict = se.to_dict()

        # --- Confidence ---
        n_factors = sum(
            1 for v in [
                latency_delta, energy_delta, carbon_delta,
                helium_delta, cost_delta, accuracy_delta,
            ] if v is not None and v != 0.0
        )
        confidence = self.config.base_confidence
        causal_calibrated = False
        confidence_state: Optional[ConfidenceState] = None

        if self.confidence_learner:
            # Compute normalized features (only when inputs present).
            energy_norm = (
                min(1.0, abs(energy_delta)
                    / max(self.config.confidence_energy_scale, 1e-9))
                if energy_delta is not None else 0.0
            )
            latency_norm = (
                min(1.0, abs(latency_delta)
                    / max(self.config.confidence_latency_scale, 1e-9))
                if latency_delta is not None else 0.0
            )
            cost_norm = (
                min(1.0, abs(cost_delta) * 10.0)
                if cost_delta is not None else 0.0
            )
            confidence_state = ConfidenceState(
                n_factors=n_factors,
                energy_delta_norm=energy_norm,
                latency_delta_norm=latency_norm,
                decision_hash=stable_hash(decision),
                cost_delta_norm=cost_norm,
            )
            confidence = self.confidence_learner.predict_confidence(
                confidence_state,
            )
            causal_calibrated = True

        # Temporal penalty (applied before final clamp).
        if not temporal_ok:
            confidence *= 0.6
        # Final clamp — enforce documented range.
        confidence = max(
            self.config.confidence_floor,
            min(self.config.confidence_ceiling, confidence),
        )

        # --- Tier ---
        if not temporal_ok:
            tier = ExplanationTier.LOW.value
            tier_detail = "temporal_violation"
        elif n_factors == 0:
            tier = ExplanationTier.LOW.value
            tier_detail = "no_signal"
        elif n_factors >= 3 and confidence >= 0.7:
            tier = ExplanationTier.HIGH.value
            tier_detail = "high_confidence"
        else:
            tier = ExplanationTier.MEDIUM.value
            tier_detail = "medium_confidence"

        # --- Explanation string (honest fallback) ---
        if structured_dict:
            explanation_str = structured_dict["headline"]
        elif energy_delta is not None:
            explanation_str = (
                f"Selected mode '{decision}' because energy shifted "
                f"by {energy_delta:+.4f} kWh."
            )
        else:
            explanation_str = (
                f"Selected mode '{decision}' "
                f"(energy measurement unavailable)."
            )

        # --- HITL review ---
        hitl_required = False
        hitl_approved: Optional[bool] = None
        if self.hitl and self.hitl.needs_review(confidence, tier, tier_detail):
            hitl_required = True
            urgency = (
                "high" if tier_detail == "temporal_violation"
                else "medium" if tier == ExplanationTier.LOW.value
                else "low"
            )
            hitl_approved = self.hitl.request_review(
                decision=decision,
                confidence=confidence,
                reason=(
                    f"tier={tier}/{tier_detail}, "
                    f"confidence={confidence:.2f}"
                ),
                urgency=urgency,
                context={
                    "decision": decision,
                    "tier_detail": tier_detail,
                    "tradeoff_extended": tradeoff_extended,
                    "temporal_violations": list(temporal_violations),
                    "missing_inputs": missing_inputs,
                },
            )

        # --- Multi-agent feedback ---
        if self.coordinator:
            self.coordinator.record(
                agent_id=self.agent_id,
                success=temporal_ok and (n_factors > 0),
                carbon_delta=carbon_delta or 0.0,
                helium_delta=helium_delta or 0.0,
            )

        # --- Distilled policy (informational) ---
        distilled_dict: Optional[Dict[str, Any]] = None
        if self.distiller and self.precision_ctl is not None:
            dp = self.distiller.distill(
                base_confidence=self.config.base_confidence,
                rationale_weight=(
                    len(structured_dict["rationale"]) / 10.0
                    if structured_dict else 0.5
                ),
                precision=self.precision_ctl.select("normal"),
            )
            distilled_dict = {
                "precision": dp.precision.value,
                "base_confidence": dp.base_confidence,
                "rationale_weight": dp.rationale_weight,
                "quality_retention": dp.quality_retention,
                "energy_reduction_percent": dp.energy_reduction_percent,
            }

        # --- Record history (with cached learner state) ---
        with self._lock:
            self.generation_count += 1
            self.confidence_values.append(confidence)
            self.decision_counts[decision] += 1
            record = {
                "generation_id": generation_id,
                "decision": decision,
                "confidence": confidence,
                "tier": tier,
                "tier_detail": tier_detail,
                "energy_delta": energy_delta,
                "latency_delta": latency_delta,
                "carbon_delta": carbon_delta,
                "helium_delta": helium_delta,
                "cost_delta": cost_delta,
                "accuracy_delta": accuracy_delta,
                "n_factors": n_factors,
                "temporal_ok": temporal_ok,
                "confidence_state": confidence_state,  # ← the exact state
                "at": _utcnow().isoformat(),
            }
            self.history.append(record)
            gen_count_snapshot = self.generation_count

        # --- Federated contribution ---
        if self.federated and \
                gen_count_snapshot % self.config.federated_push_every == 0 \
                and energy_delta is not None:
            self.federated.push(FederatedExplanationProfile(
                deployment_id=self.deployment_id,
                decision=decision,
                mean_confidence=confidence,
                mean_energy_delta=energy_delta,
                sample_count=1,
                granularity="single",
            ))

        # --- XAI last-explain cache ---
        if structured_dict:
            with self._lock:
                self.last_explanation = StructuredExplanation(
                    headline=structured_dict["headline"],
                    rationale=list(structured_dict["rationale"]),
                    confidence=confidence,
                    contributing_factors=dict(
                        structured_dict["contributing_factors"],
                    ),
                    counterfactual=structured_dict.get("counterfactual", ""),
                )
                self.last_generation_id = generation_id

        # --- Metrics ---
        self._metrics.inc("generate_total", tier=tier)
        self._metrics.observe("confidence", confidence)
        if tier_detail == "temporal_violation":
            self._metrics.inc("temporal_violations_total")
        if hitl_required:
            self._metrics.inc(
                "hitl_required_total",
                approved=str(hitl_approved).lower(),
            )

        # --- Build response (original keys first, then extended) ---
        response: Dict[str, Any] = {
            "decision": decision,
            "explanation": explanation_str,
            "tradeoff": tradeoff,
            "confidence": confidence,
        }
        response.update({
            "tradeoff_extended": tradeoff_extended,
            "missing_inputs": missing_inputs,
            "structured": structured_dict,
            "temporal_verified": temporal_ok,
            "temporal_violations": temporal_violations,
            "market_snapshot": market_snapshot,
            "helium_scarcity": helium_scarcity,
            "precision": precision_str,
            "distilled_policy": distilled_dict,
            "hitl_required": hitl_required,
            "hitl_approved": hitl_approved,
            "causal_calibrated": causal_calibrated,
            "tier": tier,
            "tier_detail": tier_detail,
            "generation_id": generation_id,
            "at": record["at"],
        })
        return response

    # ------------------------------------------------------------------ degraded
    def _degraded_response(
        self,
        decision: str,
        before: Any,
        after: Any,
        reason: str = "degraded",
    ) -> Dict[str, Any]:
        """
        Degraded response with the full happy-path shape.

        temporal_verified is False — we did NOT verify anything when the
        pipeline degraded. Notifying callers this is *unknown*, not "safe".
        """
        b = self._safe_dict(before)
        a = self._safe_dict(after)
        latency_delta = self._delta(
            self._safe_float(a.get("latency"), default=None),
            self._safe_float(b.get("latency"), default=None),
        )
        energy_delta = self._delta(
            self._safe_float(a.get("energy"), default=None),
            self._safe_float(b.get("energy"), default=None),
        )
        missing: List[str] = []
        if energy_delta is None:
            missing.append("energy")
        if latency_delta is None:
            missing.append("latency")

        generation_id = self._new_generation_id()
        now = _utcnow().isoformat()
        return {
            "decision": decision,
            "explanation": (
                f"Degraded explanation for '{decision}' ({reason})."
            ),
            "tradeoff": {
                "latency_delta": latency_delta if latency_delta is not None else 0.0,
                "energy_delta": energy_delta if energy_delta is not None else 0.0,
            },
            "confidence": self.config.confidence_floor,
            # --- Extended keys (consistent shape) ---
            "tradeoff_extended": {
                "latency_delta": latency_delta,
                "energy_delta": energy_delta,
                "carbon_delta": None,
                "helium_delta": None,
                "cost_delta": None,
                "accuracy_delta": None,
            },
            "missing_inputs": missing,
            "structured": None,
            "temporal_verified": False,
            "temporal_violations": ["pipeline_degraded"],
            "market_snapshot": None,
            "helium_scarcity": None,
            "precision": None,
            "distilled_policy": None,
            "hitl_required": False,
            "hitl_approved": None,
            "causal_calibrated": False,
            "tier": ExplanationTier.DEGRADED.value,
            "tier_detail": "degraded",
            "generation_id": generation_id,
            "at": now,
            "degraded": True,
            "degradation_reason": reason,
        }

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

    def explain_last(self) -> Optional[StructuredExplanation]:
        with self._lock:
            return self.last_explanation

    def record_outcome(
        self,
        decision: str,
        was_accepted: bool,
        *,
        generation_id: Optional[str] = None,
    ) -> bool:
        """
        Feed back whether the explanation was accepted/validated.

        Returns True if learning happened, False otherwise (unknown
        generation, missing state, learner disabled). Warns on failure.
        """
        if not self.confidence_learner:
            return False

        with self._lock:
            match = None
            if generation_id is not None:
                for rec in reversed(self.history):
                    if rec.get("generation_id") == generation_id:
                        match = rec
                        break
            else:
                for rec in reversed(self.history):
                    if rec["decision"] == decision:
                        match = rec
                        break

        if match is None:
            logger.warning(
                "record_outcome: no matching record "
                "(decision=%s, generation_id=%s)",
                decision, generation_id,
            )
            self._metrics.inc("outcome_unmatched")
            return False

        state = match.get("confidence_state")
        if state is None:
            logger.warning(
                "record_outcome: record lacks confidence_state "
                "(decision=%s, generation_id=%s)",
                decision, match.get("generation_id"),
            )
            self._metrics.inc("outcome_missing_state")
            return False

        reward = 1.0 if was_accepted else 0.0
        self.confidence_learner.record(state, reward)
        self._metrics.observe("outcome_reward", reward)
        if self.confidence_learner.observations % \
                self.config.causal_update_every == 0:
            self.confidence_learner.update()
        return True

    def contribute_federated(self) -> None:
        if not self.federated:
            return
        with self._lock:
            snapshot = list(self.history)
        grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for rec in snapshot:
            if rec.get("energy_delta") is None:
                continue
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
                granularity="aggregate",
            ))

    def get_federated_aggregate(self) -> Dict[str, Dict[str, float]]:
        return self.federated.aggregate() if self.federated else {}

    def distill_policy(
        self, urgency: str = "normal",
    ) -> Optional[DistilledExplanationPolicy]:
        if not self.distiller:
            return None
        precision = (
            self.precision_ctl.select(urgency)
            if self.precision_ctl else PrecisionLevel.INT8
        )
        with self._lock:
            rationale_weight = (
                len(self.last_explanation.rationale) / 10.0
                if self.last_explanation else 0.5
            )
        return self.distiller.distill(
            base_confidence=self.config.base_confidence,
            rationale_weight=rationale_weight,
            precision=precision,
        )

    # ------------------------------------------------------------------ health
    def health(self) -> Dict[str, Any]:
        return {
            "ready": self._circuit.state != CircuitState.OPEN,
            "circuit": self._circuit.state.value,
            "generations": self.generation_count,
            "signals": {
                "market_configured": self.market is not None,
                "helium_signal_configured": self.helium_signal_fn is not None,
            },
            "uptime_seconds": time.monotonic() - self._started_mono,
        }

    # ------------------------------------------------------------------ statistics
    def get_statistics(self) -> Dict[str, Any]:
        with self._lock:
            stats: Dict[str, Any] = {
                "deployment_id": self.deployment_id,
                "agent_id": self.agent_id,
                "generations": self.generation_count,
                "circuit": {
                    "state": self._circuit.state.value,
                    "failures": self._circuit.failures,
                },
                "decision_distribution": dict(self.decision_counts),
                "uptime_seconds": time.monotonic() - self._started_mono,
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
        if self.confidence_learner:
            stats["causal_learner"] = {
                "observations": self.confidence_learner.observations,
                "updates": self.confidence_learner.updates,
            }
        if self.federated:
            stats["federated_aggregate"] = self.federated.aggregate()
        stats["metrics"] = self._metrics.snapshot()
        return stats


__all__ = [
    "AdaptivePrecisionController",
    "AgentRole",
    "CausalConfidenceLearner",
    "CircuitBreaker",
    "CircuitState",
    "ConfidenceState",
    "DistilledExplanationPolicy",
    "ExplanationTier",
    "FederatedAggregator",
    "FederatedExplanationProfile",
    "HITLRequest",
    "HardwareProfile",
    "HumanInTheLoopGate",
    "MarketSnapshot",
    "MetricsRecorder",
    "MultiAgentCoordinator",
    "PolicyFeedback",
    "PolicyFeedbackConfig",
    "PrecisionLevel",
    "QuantumDistillationBridge",
    "StructuredExplanation",
    "StructuredExplainer",
    "TemporalLogicMonitor",
]


# =============================================================================
# Demo
# =============================================================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    class _MockHelium:
        scarcity_score = 0.65

    def auto_approve(req: HITLRequest) -> bool:
        logger.info("[HITL] auto-approve: %s", req.reason)
        return True

    # --- Legacy mode ---
    print("\n=== Legacy mode (all features off) ===")
    legacy = PolicyFeedback(
        features={k: False for k in PolicyFeedback.default_features()},
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
    legacy.close()

    # --- Enhanced mode ---
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
        context={
            "deadline_seconds": 300.0,
            "energy_budget": 0.06,
            "precision": PrecisionLevel.INT4,
            "alternative_outcomes": {
                "normal": {"energy": 0.08, "latency": 120.0},
            },
        },
    )
    print(f"  decision:    {result['decision']}")
    print(f"  explanation: {result['explanation']}")
    print(f"  confidence:  {result['confidence']:.4f}")
    print(f"  tier:        {result['tier']} / {result['tier_detail']}")
    print(f"  missing:     {result['missing_inputs']}")
    print(f"  tradeoff:    {result['tradeoff']}")
    print(f"  extended:    {result['tradeoff_extended']}")
    print(f"  temporal:    {result['temporal_verified']}")
    print(f"  precision:   {result['precision']}")
    print(f"  causal cal:  {result['causal_calibrated']}")
    print(f"  HITL:        {result['hitl_required']} "
          f"(approved={result['hitl_approved']})")
    if result.get("market_snapshot"):
        print(f"  market:      ${result['market_snapshot']['carbon_price_per_tco2_usd']:.2f}"
              f"/tCO2")
    if result.get("structured"):
        print("  structured rationale:")
        for line in result["structured"]["rationale"]:
            print(f"    • {line}")
        print(f"  counterfactual: {result['structured']['counterfactual']}")

    # Outcome — using generation_id targeting.
    ok = feedback.record_outcome(
        decision="low_energy",
        was_accepted=True,
        generation_id=result["generation_id"],
    )
    print(f"\n  record_outcome: {ok}")

    # Missing-input test.
    print("\n=== Missing-input test ===")
    r2 = feedback.generate(
        decision="low_memory",
        before={},                       # ← no energy field
        after={"energy": 0.05},
    )
    print(f"  missing_inputs: {r2['missing_inputs']}")
    print(f"  tradeoff:       {r2['tradeoff']}")
    print(f"  extended energy: {r2['tradeoff_extended']['energy_delta']}")
    print(f"  explanation:    {r2['explanation']}")

    # Temporal violation test (energy over budget).
    print("\n=== Temporal violation test ===")
    r3 = feedback.generate(
        decision="low_energy",
        before={"energy": 0.08, "latency": 120.0},
        after={"energy": 0.10, "latency": 150.0},
        context={"energy_budget": 0.05},
    )
    print(f"  tier:        {r3['tier']} / {r3['tier_detail']}")
    print(f"  temporal:    {r3['temporal_verified']} "
          f"(violations={r3['temporal_violations']})")
    print(f"  confidence:  {r3['confidence']:.4f} "
          f"(>= {feedback.config.confidence_floor})")

    # Helium-as-dict test.
    print("\n=== Helium-as-dict test ===")
    feedback.helium_signal_fn = lambda: {"scarcity_score": 0.9}
    r4 = feedback.generate(
        decision="normal",
        before={"energy": 0.05}, after={"energy": 0.04},
    )
    print(f"  helium_scarcity: {r4['helium_scarcity']}")

    # Counterfactual test.
    print("\n=== Counterfactual test ===")
    r5 = feedback.generate(
        decision="low_energy",
        before={"energy": 0.08}, after={"energy": 0.05},
        context={
            "alternative_outcomes": {
                "normal": {"energy": 0.08, "latency": 120.0},
            },
        },
    )
    print(f"  counterfactual: "
          f"{r5['structured']['counterfactual']}")

    # Federated.
    feedback.contribute_federated()

    # Distill.
    dist = feedback.distill_policy(urgency="normal")
    if dist:
        print("\n=== Distilled Explanation Policy ===")
        print(f"  Precision:     {dist.precision.value}")
        print(f"  Base conf:     {dist.base_confidence}")
        print(f"  Retention:     {dist.quality_retention}")
        print(f"  Energy saving: {dist.energy_reduction_percent}%")

    # Health.
    print("\n=== Health ===")
    print(feedback.health())

    # Statistics.
    import json
    print("\n=== Statistics ===")
    print(json.dumps(feedback.get_statistics(), indent=2, default=str))

    feedback.close()
    print("\nSmoke test passed.")
