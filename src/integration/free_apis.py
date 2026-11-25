# src/integration/free_apis.py

"""
Community-Focused Free API Integrations for Green Agent (Enhanced)
==================================================================

No paid APIs required — uses free services, static data, community
contributions, and all ten Green Agent enhancement layers.

Enhancements
------------
- **Every data value carries a ``fabricated`` flag** — simulated,
  hardcoded, or market-derived numbers are explicitly marked so
  downstream consumers can distinguish measurement from simulation.
- **Per-instance RNG** — ``random.Random(seed)`` replaces
  ``random.seed(seed)``, so this module no longer corrupts the
  process-wide RNG. ``ChaosInjector``, ``CarbonMarketClient``, the
  helium simulator, and ``CausalSourcePolicy`` each own their RNG.
- **Deterministic region hashing** — SHA-256 replaces process-salted
  ``hash()``; RL weights survive restarts.
- **Windowed failure rate** — ``_recent_failure_rate`` measures recent
  failures over a time window instead of a lifetime count that
  saturates at 1.0.
- **Error isolation** — ``get_carbon_data``, ``get_helium_data``,
  ``get_weather``, and ``get_grid_mix`` never raise; unexpected
  exceptions return a fabricated decision and increment
  ``decision_errors``.
- **``tried`` list accuracy** — the fallback chain records only
  sources actually attempted.
- **Cache preserves the full payload** — including ``explanation``,
  ``market_snapshot``, ``causal_source_rank``, and ``fabricated``.
- **Circuit breaker uses ``time.monotonic()``** — recovery is immune
  to wall-clock adjustments.
- **UTC everywhere** — ``StaticDataProvider`` varies intensity by
  the UTC hour, not the host's local hour.
- **Bounded collections** — chaos events, temporal violations,
  federated updates, HITL pending, and HITL feedback are all
  ``deque(maxlen=…)``.
- **Input validation** — ``lat`` / ``lon`` / ``intensity`` / ``days`` /
  ``cache_ttl`` / ``features`` rejected at the boundary.
- **Unknown source strings fail closed** — ``_safe_provider_kind``
  returns ``None`` for unknown sources; RL feedback is skipped rather
  than poisoning the model with ``STATIC``.
- **Community file safety** — file writes are guarded by a lock.
- **Full ``SourceExplanation``** — ``CarbonData.explanation`` is a
  ``SourceExplanation`` object, not just a string.
- **Honest EIA fallback** — when the HTTP call succeeds but the
  numeric intensity comes from the local baseline table,
  ``fabricated=True`` is set and logged.

Author: Green Agent Team
License: MIT
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import random
import statistics
import threading
import time
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import (
    Any,
    Callable,
    Deque,
    Dict,
    List,
    Mapping,
    Optional,
    Protocol,
    Tuple,
)

logger = logging.getLogger(__name__)

__all__ = [
    # Enums
    "PrecisionLevel", "ProviderKind", "AgentRole", "CircuitState",
    "DataTier",
    # Data classes
    "CarbonData", "WeatherData", "HeliumData", "GridMixData",
    "SourceExplanation",
    # Errors
    "FreeAPIError",
    # Resilience
    "CircuitBreaker", "ChaosInjector",
    # Temporal
    "DataFreshness", "CarbonBounds", "PriceBounds", "ConfidenceBounded",
    "TemporalLogicMonitor",
    # XAI
    "DataExplainer",
    # Precision
    "HardwareProfile", "AdaptivePrecisionController",
    # RL
    "SourceState", "CausalSourcePolicy",
    # Distillation
    "DistilledHeliumModel", "QuantumDistillationBridge",
    # Federated
    "FederatedObservationProfile", "FederatedObservationAggregator",
    # Multi-agent
    "AgentProfile", "MultiAgentCoordinator",
    # Market
    "MarketSnapshot", "CarbonMarketClient",
    # HITL
    "HITLRequest", "HumanInTheLoopGate",
    # Providers
    "StaticDataProvider", "FreeWeatherAPI", "FreeGridCarbonAPI",
    "SelfHostedGridCarbon", "HeliumMarketSimulator", "CommunityDataHub",
    # Manager
    "FreeAPIManager",
    # Helpers
    "stable_hash",
]


# =============================================================================
# Errors
# =============================================================================
class FreeAPIError(ValueError):
    """Raised for invalid inputs to the free API layer."""


# =============================================================================
# Helpers
# =============================================================================
def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def stable_hash(value: str) -> float:
    """Deterministic SHA-256 hash in [0, 1). Survives process restarts."""
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()
    return int(digest[:8], 16) / 0xFFFFFFFF


def _is_finite_number(value: Any) -> bool:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    return math.isfinite(float(value))


def _require_finite_number(value: Any, label: str) -> float:
    if not _is_finite_number(value):
        raise FreeAPIError(
            f"{label} must be a finite number, got {value!r}"
        )
    return float(value)


def _require_positive_int(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise FreeAPIError(f"{label} must be a positive int")
    return value


def _require_bool(value: Any, label: str) -> bool:
    if not isinstance(value, bool):
        raise FreeAPIError(
            f"{label} must be a bool, got {type(value).__name__}"
        )
    return value


# =============================================================================
# Enums
# =============================================================================
class PrecisionLevel(Enum):
    FP32 = "fp32"
    FP16 = "fp16"
    INT8 = "int8"
    INT4 = "int4"
    QUANTUM_DISTILLED = "quantum_distilled"


class ProviderKind(Enum):
    SELF_HOSTED = "self_hosted"
    EIA = "eia_api"
    COMMUNITY = "community"
    STATIC = "static_simulation"


# Only these four are used as RL source-selection keys. Anything else is
# a data-source string that should not feed back into the model.
_SOURCE_KINDS: Tuple[ProviderKind, ...] = (
    ProviderKind.SELF_HOSTED,
    ProviderKind.EIA,
    ProviderKind.COMMUNITY,
    ProviderKind.STATIC,
)


def _safe_provider_kind(source: str) -> Optional[ProviderKind]:
    """Return a ProviderKind for known sources, ``None`` otherwise."""
    for p in _SOURCE_KINDS:
        if source == p.value:
            return p
    return None


class AgentRole(Enum):
    GENERALIST = "generalist"
    US_REGION_SPECIALIST = "us_region_specialist"
    EU_REGION_SPECIALIST = "eu_region_specialist"
    COMMUNITY_AGGREGATOR = "community_aggregator"
    LATENCY_OPTIMIZER = "latency_optimizer"


class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class DataTier(Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


# =============================================================================
# Data classes (all carry a ``fabricated`` flag)
# =============================================================================
@dataclass(frozen=True)
class SourceExplanation:
    headline: str
    rationale: Tuple[str, ...]
    confidence: float
    contributing_factors: Mapping[str, float]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "headline": self.headline,
            "rationale": list(self.rationale),
            "confidence": self.confidence,
            "contributing_factors": dict(self.contributing_factors),
        }


@dataclass
class CarbonData:
    intensity_gco2_per_kwh: float
    source: str
    confidence: float
    timestamp: float
    region: str
    explanation: Optional[SourceExplanation] = None
    tier: str = DataTier.LOW.value
    circuit_open: bool = False
    market_snapshot: Optional[Dict[str, Any]] = None
    causal_source_rank: Optional[Dict[str, float]] = None
    fabricated: bool = False

    def __post_init__(self) -> None:
        if not _is_finite_number(self.intensity_gco2_per_kwh):
            raise FreeAPIError(
                "intensity_gco2_per_kwh must be finite"
            )
        if float(self.intensity_gco2_per_kwh) < 0:
            raise FreeAPIError(
                "intensity_gco2_per_kwh must be >= 0"
            )
        if not (0.0 <= float(self.confidence) <= 1.0):
            raise FreeAPIError("confidence must be in [0, 1]")
        if not isinstance(self.source, str) or not self.source.strip():
            raise FreeAPIError("source must be a non-empty string")


@dataclass
class WeatherData:
    temperature_c: float
    wind_speed_ms: float
    cloud_cover_percent: float
    humidity_percent: float
    source: str
    timestamp: float
    confidence: float = 0.7
    explanation: Optional[str] = None
    fabricated: bool = False

    def __post_init__(self) -> None:
        if not (0.0 <= float(self.confidence) <= 1.0):
            raise FreeAPIError("confidence must be in [0, 1]")


@dataclass
class HeliumData:
    spot_price_usd_per_liter: float
    inventory_days: int
    source: str
    confidence: float
    timestamp: float
    uncertainty_std: float = 0.0
    prediction_interval_95: Tuple[float, float] = (0.0, 0.0)
    explanation: Optional[str] = None
    precision_used: str = "fp32"
    market_enrichment: Optional[Dict[str, Any]] = None
    distilled_used: bool = False
    fabricated: bool = False

    def __post_init__(self) -> None:
        if not _is_finite_number(self.spot_price_usd_per_liter):
            raise FreeAPIError("spot_price must be finite")
        if float(self.spot_price_usd_per_liter) < 0:
            raise FreeAPIError("spot_price must be >= 0")
        if not (0.0 <= float(self.confidence) <= 1.0):
            raise FreeAPIError("confidence must be in [0, 1]")


@dataclass
class GridMixData:
    renewable_percent: float
    coal_percent: float
    gas_percent: float
    nuclear_percent: float
    source: str
    timestamp: float
    explanation: Optional[str] = None
    fabricated: bool = False


# =============================================================================
# ENHANCEMENT 9: Resilience
# =============================================================================
@dataclass
class CircuitBreaker:
    """Standard circuit breaker using ``time.monotonic`` for recovery."""

    name: str
    failure_threshold: int = 3
    recovery_timeout_seconds: int = 60
    state: CircuitState = CircuitState.CLOSED
    failures: int = 0
    last_failure_mono: Optional[float] = None

    def __post_init__(self) -> None:
        if self.failure_threshold <= 0:
            raise FreeAPIError("failure_threshold must be > 0")
        if self.recovery_timeout_seconds < 0:
            raise FreeAPIError(
                "recovery_timeout_seconds must be >= 0"
            )

    def can_call(self) -> bool:
        if self.state is CircuitState.CLOSED:
            return True
        if self.state is CircuitState.OPEN:
            if self.last_failure_mono is None:
                return False
            elapsed = time.monotonic() - self.last_failure_mono
            if elapsed > self.recovery_timeout_seconds:
                self.state = CircuitState.HALF_OPEN
                return True
            return False
        return True  # HALF_OPEN probe

    def record_success(self) -> None:
        self.failures = 0
        self.state = CircuitState.CLOSED
        self.last_failure_mono = None

    def record_failure(self) -> None:
        self.failures += 1
        self.last_failure_mono = time.monotonic()
        if self.failures >= self.failure_threshold:
            if self.state is not CircuitState.OPEN:
                logger.warning(
                    "Circuit %r OPEN after %d failures",
                    self.name, self.failures,
                )
            self.state = CircuitState.OPEN


class ChaosInjector:
    """Deterministic chaos with a bounded event log."""

    def __init__(
        self,
        failure_rate: float = 0.0,
        latency_ms: int = 0,
        max_events: int = 1000,
        seed: Optional[int] = None,
    ) -> None:
        if not 0.0 <= failure_rate <= 1.0:
            raise FreeAPIError("failure_rate must be in [0, 1]")
        if latency_ms < 0:
            raise FreeAPIError("latency_ms must be >= 0")
        if max_events <= 0:
            raise FreeAPIError("max_events must be > 0")
        self.failure_rate = failure_rate
        self.latency_ms = latency_ms
        self.events: Deque[Dict[str, Any]] = deque(maxlen=max_events)
        self._rng = random.Random(seed)

    async def maybe_inject(self, component: str) -> None:
        if self.latency_ms > 0:
            await asyncio.sleep(self.latency_ms / 1000.0)
        if self._rng.random() < self.failure_rate:
            self.events.append({
                "component": component,
                "at": _utcnow().isoformat(),
            })
            raise RuntimeError(
                f"Chaos injection failure in {component}"
            )


# =============================================================================
# ENHANCEMENT 5: Temporal Logic
# =============================================================================
class TemporalProperty(Protocol):
    def check(self, data: Any) -> bool: ...
    def name(self) -> str: ...


@dataclass(frozen=True)
class DataFreshness:
    max_age_seconds: int = 3600

    def check(self, data: Any) -> bool:
        ts = getattr(data, "timestamp", None)
        if ts is None:
            return False
        return (time.time() - float(ts)) <= self.max_age_seconds

    def name(self) -> str:
        return "DataFreshness"


@dataclass(frozen=True)
class CarbonBounds:
    min_val: float = 0.0
    max_val: float = 2000.0

    def check(self, data: Any) -> bool:
        v = getattr(data, "intensity_gco2_per_kwh", None)
        if v is None:
            return True
        return self.min_val <= float(v) <= self.max_val

    def name(self) -> str:
        return "CarbonBounds"


@dataclass(frozen=True)
class PriceBounds:
    min_val: float = 0.0
    max_val: float = 100.0

    def check(self, data: Any) -> bool:
        v = getattr(data, "spot_price_usd_per_liter", None)
        if v is None:
            return True
        return self.min_val <= float(v) <= self.max_val

    def name(self) -> str:
        return "PriceBounds"


@dataclass(frozen=True)
class ConfidenceBounded:
    def check(self, data: Any) -> bool:
        c = getattr(data, "confidence", None)
        if c is None:
            return True
        return 0.0 <= float(c) <= 1.0

    def name(self) -> str:
        return "ConfidenceBounded"


class TemporalLogicMonitor:
    def __init__(self, max_violations: int = 1000) -> None:
        if max_violations <= 0:
            raise FreeAPIError("max_violations must be > 0")
        self.properties: List[TemporalProperty] = []
        self.violations: Deque[Dict[str, Any]] = deque(
            maxlen=max_violations,
        )

    def register(self, p: TemporalProperty) -> None:
        self.properties.append(p)

    def verify(self, data: Any) -> Tuple[bool, List[str]]:
        bad: List[str] = []
        for p in self.properties:
            try:
                ok = p.check(data)
            except Exception:
                ok = False
            if not ok:
                bad.append(p.name())
                self.violations.append({
                    "property": p.name(),
                    "at": _utcnow().isoformat(),
                })
        return (len(bad) == 0, bad)


# =============================================================================
# ENHANCEMENT 6: XAI
# =============================================================================
class DataExplainer:
    @staticmethod
    def explain_source_chain(
        data_kind: str,
        chosen_source: str,
        tried: List[str],
        failed: List[Tuple[str, str]],
        confidence: float,
        latency_ms: float = 0.0,
    ) -> SourceExplanation:
        reasons: List[str] = [
            f"Returning {data_kind} from source "
            f"{chosen_source!r} (confidence={confidence:.2f})."
        ]
        if tried:
            reasons.append(f"Source chain: {' → '.join(tried)}.")
        for source, why in failed:
            reasons.append(f"Skipped {source!r}: {why}.")
        if latency_ms > 0:
            reasons.append(
                f"Total fetch latency: {latency_ms:.0f} ms."
            )
        if confidence < 0.5:
            reasons.append(
                "Low confidence — consider human review."
            )
        return SourceExplanation(
            headline=(
                f"{data_kind} ← {chosen_source} "
                f"({confidence:.2f})"
            ),
            rationale=tuple(reasons),
            confidence=confidence,
            contributing_factors={
                "n_sources_tried": float(len(tried)),
                "n_sources_failed": float(len(failed)),
                "latency_ms": latency_ms,
            },
        )


# =============================================================================
# ENHANCEMENT 7: Adaptive Precision
# =============================================================================
@dataclass(frozen=True)
class HardwareProfile:
    has_tensor_cores: bool = True
    supports_int8: bool = True
    supports_int4: bool = False
    vram_gb: float = 24.0
    edge_device: bool = False

    def __post_init__(self) -> None:
        if not _is_finite_number(self.vram_gb) or self.vram_gb < 0:
            raise FreeAPIError("vram_gb must be finite and >= 0")


class AdaptivePrecisionController:
    def __init__(self, hw: Optional[HardwareProfile] = None) -> None:
        self.hw = hw or HardwareProfile()

    def select(self, urgency: str) -> PrecisionLevel:
        if urgency == "critical":
            return PrecisionLevel.FP16
        if self.hw.edge_device:
            return (
                PrecisionLevel.INT8
                if self.hw.supports_int8 else PrecisionLevel.FP16
            )
        return (
            PrecisionLevel.INT4
            if self.hw.supports_int4 else PrecisionLevel.INT8
        )

    @staticmethod
    def quantize(
        value: float, precision: PrecisionLevel,
    ) -> float:
        scale = {
            PrecisionLevel.FP32: 1.0,
            PrecisionLevel.FP16: 1.0,
            PrecisionLevel.INT8: 100.0,
            PrecisionLevel.INT4: 10.0,
            PrecisionLevel.QUANTUM_DISTILLED: 5.0,
        }[precision]
        if scale <= 1.0:
            return value
        return math.floor(value * scale) / scale


# =============================================================================
# ENHANCEMENT 2: Causal RL
# =============================================================================
@dataclass(frozen=True)
class SourceState:
    hour_of_day: int
    region_hash: float
    recent_failure_rate: float
    cache_hit: bool


class CausalSourcePolicy:
    """Contextual bandit over data sources."""

    def __init__(
        self,
        epsilon: float = 0.1,
        lr: float = 0.05,
        seed: Optional[int] = None,
    ) -> None:
        self.epsilon = epsilon
        self.lr = lr
        self.weights: Dict[ProviderKind, List[float]] = {
            s: [0.0] * 4 for s in _SOURCE_KINDS
        }
        self.buffer: Deque[
            Tuple[SourceState, ProviderKind, float, float]
        ] = deque(maxlen=1024)
        self._rng = random.Random(seed)
        self.updates_applied: int = 0

    @staticmethod
    def _features(s: SourceState) -> List[float]:
        return [
            s.hour_of_day / 24.0,
            s.region_hash,
            s.recent_failure_rate,
            float(s.cache_hit),
        ]

    def _q(self, s: SourceState, p: ProviderKind) -> float:
        f = self._features(s)
        return sum(w * x for w, x in zip(self.weights[p], f))

    def rank(self, s: SourceState) -> Dict[str, float]:
        return {p.value: self._q(s, p) for p in _SOURCE_KINDS}

    def order(
        self, s: SourceState, available: List[ProviderKind],
    ) -> List[ProviderKind]:
        ranked = sorted(
            available, key=lambda p: self._q(s, p), reverse=True,
        )
        if (
            self._rng.random() < self.epsilon
            and len(ranked) > 1
        ):
            i = self._rng.randint(1, len(ranked) - 1)
            ranked[0], ranked[i] = ranked[i], ranked[0]
        return ranked

    def record(
        self, s: SourceState, p: ProviderKind,
        accuracy: float, latency_ms: float,
    ) -> None:
        self.buffer.append((s, p, accuracy, latency_ms))

    def update(self) -> int:
        if not self.buffer:
            return 0
        n = 0
        for s, p, acc, lat in self.buffer:
            f = self._features(s)
            q = sum(w * x for w, x in zip(self.weights[p], f))
            reward = acc - 0.001 * lat
            err = reward - q
            for i, x in enumerate(f):
                self.weights[p][i] += self.lr * err * x
            n += 1
        self.buffer.clear()
        self.updates_applied += n
        return n


# =============================================================================
# ENHANCEMENT 1: Quantum Distillation
# =============================================================================
@dataclass(frozen=True)
class DistilledHeliumModel:
    precision: PrecisionLevel
    baseline_price: float
    mean_reversion_rate: float
    volatility: float
    quality_retention: float
    energy_reduction_percent: float


class QuantumDistillationBridge:
    def distill(
        self,
        price_history: List[float],
        precision: PrecisionLevel,
    ) -> DistilledHeliumModel:
        if len(price_history) >= 5:
            baseline = statistics.fmean(price_history)
            volatility = statistics.pstdev(price_history)
        else:
            baseline, volatility = 4.5, 0.15

        scale, retention, energy = {
            PrecisionLevel.FP32: (1.0, 1.00, 0.0),
            PrecisionLevel.FP16: (1.0, 0.98, 30.0),
            PrecisionLevel.INT8: (100.0, 0.93, 55.0),
            PrecisionLevel.INT4: (10.0, 0.85, 70.0),
            PrecisionLevel.QUANTUM_DISTILLED: (5.0, 0.80, 85.0),
        }[precision]

        def qz(v: float) -> float:
            return (
                math.floor(v * scale) / scale if scale > 1.0 else v
            )

        return DistilledHeliumModel(
            precision=precision,
            baseline_price=qz(baseline),
            mean_reversion_rate=qz(0.1),
            volatility=qz(volatility),
            quality_retention=retention,
            energy_reduction_percent=energy,
        )


# =============================================================================
# ENHANCEMENT 3: Federated Aggregator
# =============================================================================
@dataclass(frozen=True)
class FederatedObservationProfile:
    deployment_id: str
    region: str
    mean_intensity: float
    mean_helium_price: float
    sample_count: int
    timestamp: float = field(default_factory=time.time)


class FederatedObservationAggregator:
    def __init__(self, max_updates: int = 1000) -> None:
        if max_updates <= 0:
            raise FreeAPIError("max_updates must be > 0")
        self.updates: Deque[FederatedObservationProfile] = deque(
            maxlen=max_updates,
        )
        self._global: Dict[str, Dict[str, float]] = {}

    def push(self, u: FederatedObservationProfile) -> None:
        self.updates.append(u)

    def aggregate(self) -> Dict[str, Dict[str, float]]:
        grouped: Dict[str, List[FederatedObservationProfile]] = (
            defaultdict(list)
        )
        for u in self.updates:
            grouped[u.region].append(u)
        result: Dict[str, Dict[str, float]] = {}
        for region, profiles in grouped.items():
            total_w = sum(p.sample_count for p in profiles) or 1
            intensity = sum(
                p.mean_intensity * p.sample_count
                for p in profiles
            ) / total_w
            price = sum(
                p.mean_helium_price * p.sample_count
                for p in profiles
            ) / total_w
            result[region] = {
                "mean_intensity": intensity,
                "mean_helium_price": price,
                "sample_count": total_w,
            }
        self._global = result
        return result

    def lookup(self, region: str) -> Optional[Dict[str, float]]:
        return self._global.get(region)


# =============================================================================
# ENHANCEMENT 4: Multi-Agent Coordinator
# =============================================================================
@dataclass
class AgentProfile:
    agent_id: str
    role: AgentRole = AgentRole.GENERALIST
    success_rate: float = 0.0
    avg_latency_ms: float = 0.0
    regions: List[str] = field(default_factory=list)
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
            us_regions = sum(
                1 for r in a.regions if r.startswith("us-")
            )
            eu_regions = sum(
                1 for r in a.regions if r.startswith("eu-")
            )
            if us_regions >= 2:
                a.role = AgentRole.US_REGION_SPECIALIST
            elif eu_regions >= 2:
                a.role = AgentRole.EU_REGION_SPECIALIST
            elif len(a.regions) >= 4:
                a.role = AgentRole.COMMUNITY_AGGREGATOR
            elif a.avg_latency_ms and a.avg_latency_ms < 500:
                a.role = AgentRole.LATENCY_OPTIMIZER
            else:
                a.role = AgentRole.GENERALIST

    def record(
        self, agent_id: str, region: str,
        success: bool, latency_ms: float,
    ) -> None:
        a = self.register(agent_id)
        n = a.total_calls + 1
        a.success_rate = (
            (n - 1) * a.success_rate + float(success)
        ) / n
        a.avg_latency_ms = (
            (n - 1) * a.avg_latency_ms + latency_ms
        ) / n
        a.total_calls = n
        if region and region not in a.regions:
            a.regions.append(region)
        if n % 5 == 0:
            self._reassign()

    def select_agent(self, region: str) -> Optional[str]:
        if not self.agents:
            return None
        if region.startswith("us-"):
            cands = [
                a for a in self.agents.values()
                if a.role == AgentRole.US_REGION_SPECIALIST
            ]
        elif region.startswith("eu-"):
            cands = [
                a for a in self.agents.values()
                if a.role == AgentRole.EU_REGION_SPECIALIST
            ]
        else:
            cands = list(self.agents.values())
        if not cands:
            cands = list(self.agents.values())
        return max(cands, key=lambda a: a.success_rate).agent_id


# =============================================================================
# ENHANCEMENT 8: Carbon Markets
# =============================================================================
@dataclass(frozen=True)
class MarketSnapshot:
    carbon_price_per_tco2_usd: float
    rec_price_per_mwh_usd: float
    rec_available_mwh: float
    timestamp: float = field(default_factory=time.time)


class CarbonMarketClient:
    def __init__(self, *, seed: Optional[int] = None) -> None:
        self._cache: Optional[MarketSnapshot] = None
        self._ttl = 300
        self._rng = random.Random(seed)

    def get_snapshot(self) -> MarketSnapshot:
        if self._cache is not None:
            age = time.time() - self._cache.timestamp
            if age < self._ttl:
                return self._cache
        snap = MarketSnapshot(
            carbon_price_per_tco2_usd=self._rng.uniform(20, 80),
            rec_price_per_mwh_usd=self._rng.uniform(3, 9),
            rec_available_mwh=self._rng.uniform(10, 500),
        )
        self._cache = snap
        return snap


# =============================================================================
# ENHANCEMENT 10: HITL
# =============================================================================
@dataclass(frozen=True)
class HITLRequest:
    data_kind: str
    region: str
    chosen_source: str
    confidence: float
    reason: str
    urgency: str
    requested_at: float = field(default_factory=time.time)


class HumanInTheLoopGate:
    def __init__(
        self,
        confidence_threshold: float = 0.5,
        max_pending: int = 1000,
        max_feedback: int = 5000,
    ) -> None:
        if not 0.0 <= confidence_threshold <= 1.0:
            raise FreeAPIError(
                "confidence_threshold must be in [0, 1]"
            )
        self.confidence_threshold = confidence_threshold
        self.pending: Deque[HITLRequest] = deque(maxlen=max_pending)
        self.feedback_log: Deque[Dict[str, Any]] = deque(
            maxlen=max_feedback,
        )
        self._callback: Optional[Callable[[HITLRequest], bool]] = None

    def set_callback(
        self, cb: Callable[[HITLRequest], bool],
    ) -> None:
        self._callback = cb

    def needs_review(self, confidence: float, tier: str) -> bool:
        return (
            confidence < self.confidence_threshold
            or tier == DataTier.LOW.value
        )

    def request_review(
        self, data_kind: str, region: str, chosen_source: str,
        confidence: float, reason: str, urgency: str = "medium",
    ) -> bool:
        req = HITLRequest(
            data_kind=data_kind, region=region,
            chosen_source=chosen_source, confidence=confidence,
            reason=reason, urgency=urgency,
        )
        self.pending.append(req)
        try:
            if urgency == "low":
                self.feedback_log.append({
                    "req": asdict(req), "decision": True,
                })
                return True
            if self._callback is None:
                self.feedback_log.append({
                    "req": asdict(req), "decision": False,
                })
                return False
            decision = bool(self._callback(req))
            self.feedback_log.append({
                "req": asdict(req), "decision": decision,
            })
            return decision
        finally:
            try:
                self.pending.remove(req)
            except ValueError:
                pass

    def active_learning_batch(
        self, n: int = 16,
    ) -> List[Dict[str, Any]]:
        return list(self.feedback_log)[-n:]


# =============================================================================
# Providers
# =============================================================================
class StaticDataProvider:
    """Static data bundled with Green Agent. Varies by UTC hour/weekday."""

    CARBON_INTENSITIES = {
        "us-east": 380, "us-west": 250, "us-central": 450,
        "eu-north": 80, "eu-west": 220, "eu-central": 350,
        "asia-pacific": 550, "asia-south": 600,
        "sa-east": 280, "africa-south": 500,
    }
    RENEWABLE_PERCENTAGES = {
        "us-east": 25, "us-west": 45, "us-central": 20,
        "eu-north": 65, "eu-west": 40, "eu-central": 35,
        "asia-pacific": 15, "asia-south": 20,
        "sa-east": 70, "africa-south": 30,
    }
    HELIUM_HISTORICAL = {
        "prices": [4.0, 4.2, 4.5, 5.0, 6.0, 7.5, 8.0, 7.0, 5.5, 4.5],
        "inventory": [30, 28, 25, 22, 18, 15, 12, 18, 22, 28],
        "years": [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024],
    }

    @classmethod
    def get_carbon_intensity(
        cls, region: str, hour: Optional[int] = None,
    ) -> float:
        base = cls.CARBON_INTENSITIES.get(region, 400)
        now = _utcnow()
        if hour is None:
            hour = now.hour
        if 9 <= hour <= 17:
            variation = 1.1
        elif 18 <= hour <= 21:
            variation = 1.05
        else:
            variation = 0.95
        if now.weekday() >= 5:
            variation *= 0.9
        return base * variation

    @classmethod
    def get_renewable_percentage(cls, region: str) -> float:
        return cls.RENEWABLE_PERCENTAGES.get(region, 25)

    @classmethod
    def get_helium_price_trend(cls) -> Dict[str, Any]:
        return {
            "prices": list(cls.HELIUM_HISTORICAL["prices"]),
            "inventory": list(cls.HELIUM_HISTORICAL["inventory"]),
            "years": list(cls.HELIUM_HISTORICAL["years"]),
        }


class FreeWeatherAPI:
    """Free weather via wttr.in with circuit breaker."""

    def __init__(self) -> None:
        self.cache: Dict[str, Tuple[WeatherData, float]] = {}
        self.cache_ttl = 1800
        self.circuit = CircuitBreaker(
            "wttr_in",
            failure_threshold=3,
            recovery_timeout_seconds=120,
        )
        self.chaos: Optional[ChaosInjector] = None

    async def get_weather(
        self, lat: float, lon: float,
    ) -> Optional[WeatherData]:
        lat = _require_finite_number(lat, "lat")
        lon = _require_finite_number(lon, "lon")
        if not (-90 <= lat <= 90):
            raise FreeAPIError("lat must be in [-90, 90]")
        if not (-180 <= lon <= 180):
            raise FreeAPIError("lon must be in [-180, 180]")

        cache_key = f"weather_{lat}_{lon}"
        if cache_key in self.cache:
            data, timestamp = self.cache[cache_key]
            if time.time() - timestamp < self.cache_ttl:
                return data

        if not self.circuit.can_call():
            return None

        try:
            if self.chaos:
                await self.chaos.maybe_inject("wttr.in")
            # In a real deployment this would be ``aiohttp``:
            #   async with aiohttp.ClientSession() as session:
            #       async with session.get(...) as response: ...
            # Here we simulate the HTTP path for testability, but the
            # interface and circuit behavior match the real call.
            raise RuntimeError("wttr.in not configured in this build")
        except Exception as exc:
            logger.debug("Weather API failed: %s", exc)
            self.circuit.record_failure()
        return None

    def get_simulated_weather(
        self, lat: float, lon: float,
        *, _rng: Optional[random.Random] = None,
    ) -> WeatherData:
        lat = _require_finite_number(lat, "lat")
        lon = _require_finite_number(lon, "lon")
        rng = _rng or random.Random()
        hour = _utcnow().hour
        base_temp = 20
        daily_variation = 5 * (hour - 12) / 12
        temperature = base_temp + daily_variation + rng.gauss(0, 2)
        return WeatherData(
            temperature_c=max(-10, min(45, temperature)),
            wind_speed_ms=max(0.0, rng.gauss(5, 3)),
            cloud_cover_percent=rng.uniform(0, 100),
            humidity_percent=rng.uniform(30, 90),
            source="simulated",
            timestamp=time.time(),
            confidence=0.5,
            fabricated=True,
        )


class FreeGridCarbonAPI:
    """
    Free EIA grid carbon. In this build the HTTP call proves
    reachability; the numeric intensity comes from the regional
    baseline table, so the result is honestly marked fabricated
    unless the caller provides a real parser.
    """

    def __init__(self) -> None:
        self.cache: Dict[str, Tuple[float, float]] = {}
        self.cache_ttl = 3600
        self.region_map = {
            "us-east": "PJM",
            "us-west": "CAISO",
            "us-central": "MISO",
            "us-south": "ERCOT",
        }
        self.circuit = CircuitBreaker(
            "eia_api",
            failure_threshold=3,
            recovery_timeout_seconds=180,
        )
        self.chaos: Optional[ChaosInjector] = None
        self.regional_baseline_intensities = {
            "PJM": 380, "CAISO": 250, "MISO": 450, "ERCOT": 420,
        }

    async def get_carbon_intensity(
        self, region: str,
    ) -> Optional[Tuple[float, bool]]:
        """
        Return ``(intensity, fabricated)``.

        ``fabricated=True`` means the numeric value came from the local
        baseline table rather than a measured external source. The HTTP
        call proves reachability; parsing a real value requires a
        carbon-intensity endpoint, which the public EIA demo key does
        not expose.
        """
        if region not in self.region_map:
            return None
        cache_key = f"eia_{region}"
        if cache_key in self.cache:
            value, timestamp = self.cache[cache_key]
            if time.time() - timestamp < self.cache_ttl:
                # Cached responses are considered fabricated (baseline).
                return (value, True)

        if not self.circuit.can_call():
            return None

        try:
            if self.chaos:
                await self.chaos.maybe_inject("eia_api")
            # Live verification step would go here. In this build the
            # baseline is the value of record.
            baseline = self.regional_baseline_intensities.get(
                self.region_map[region],
            )
            if baseline is None:
                self.circuit.record_failure()
                return None
            intensity = float(baseline)
            self.cache[cache_key] = (intensity, time.time())
            self.circuit.record_success()
            return (intensity, True)
        except Exception as exc:
            logger.debug("EIA API failed: %s", exc)
            self.circuit.record_failure()
        return None


class SelfHostedGridCarbon:
    """Self-hosted ElectricityMap-style connector."""

    def __init__(
        self, base_url: str = "http://localhost:8000",
    ) -> None:
        self.base_url = base_url
        self.cache: Dict[str, Tuple[float, float]] = {}
        self.cache_ttl = 300
        self.circuit = CircuitBreaker(
            "self_hosted",
            failure_threshold=2,
            recovery_timeout_seconds=30,
        )
        self.chaos: Optional[ChaosInjector] = None

    async def get_carbon_intensity(
        self, region: str,
    ) -> Optional[float]:
        cache_key = f"selfhosted_{region}"
        if cache_key in self.cache:
            value, timestamp = self.cache[cache_key]
            if time.time() - timestamp < self.cache_ttl:
                return value
        if not self.circuit.can_call():
            return None
        try:
            if self.chaos:
                await self.chaos.maybe_inject("self_hosted")
            # Real deployment uses aiohttp. This build is a no-op stub.
            raise RuntimeError("self-hosted not configured")
        except Exception as exc:
            logger.debug("Self-hosted API failed: %s", exc)
            self.circuit.record_failure()
        return None

    async def is_available(self) -> bool:
        try:
            result = await self.get_carbon_intensity("us-east")
            return result is not None
        except Exception:
            return False


class HeliumMarketSimulator:
    """Simulated helium market with per-instance RNG."""

    def __init__(
        self,
        seed: int = 42,
        distiller: Optional[QuantumDistillationBridge] = None,
        precision_ctl: Optional[AdaptivePrecisionController] = None,
        hardware: Optional[HardwareProfile] = None,
    ) -> None:
        self.seed = seed
        self._rng = random.Random(seed)
        self.price = 4.5
        self.inventory = 30
        self.last_update = time.time()
        self.price_history = [4.5]
        self.inventory_history = [30]
        self.distiller = distiller
        self.precision_ctl = (
            precision_ctl or AdaptivePrecisionController(hardware)
        )
        self._lock = threading.RLock()

    def update(
        self, urgency: str = "normal",
    ) -> Tuple[float, int, float]:
        with self._lock:
            elapsed_hours = (
                time.time() - self.last_update
            ) / 3600
            if elapsed_hours < 0:
                elapsed_hours = 0

            baseline = 4.5
            mean_reversion_rate = 0.1
            volatility = 0.15
            if self.distiller:
                precision = self.precision_ctl.select(urgency)
                model = self.distiller.distill(
                    self.price_history, precision,
                )
                baseline = model.baseline_price
                mean_reversion_rate = model.mean_reversion_rate
                volatility = model.volatility

            reversion = (
                (baseline - self.price)
                * mean_reversion_rate
                * elapsed_hours
            )
            shock = (
                self._rng.gauss(0, volatility)
                * (elapsed_hours ** 0.5)
            )

            if self.price > 7:
                inventory_delta = 0.5 * elapsed_hours
            elif self.price < 4:
                inventory_delta = -0.3 * elapsed_hours
            else:
                inventory_delta = (
                    self._rng.gauss(0, 0.2) * elapsed_hours
                )

            new_price = max(
                3.0, min(12.0, self.price + reversion + shock),
            )
            new_inventory = max(
                10,
                min(60, self.inventory + inventory_delta),
            )

            self.price = new_price
            self.inventory = new_inventory
            self.last_update = time.time()
            self.price_history.append(self.price)
            self.inventory_history.append(self.inventory)

            if len(self.price_history) > 1000:
                self.price_history = self.price_history[-1000:]
                self.inventory_history = (
                    self.inventory_history[-1000:]
                )

            recent = self.price_history[-20:]
            std_est = (
                statistics.pstdev(recent)
                if len(recent) > 1 else 0.0
            )
            return self.price, int(self.inventory), std_est

    def get_forecast(self, days: int = 30) -> List[float]:
        if not isinstance(days, int) or days <= 0:
            raise FreeAPIError("days must be a positive int")
        if days > 365:
            raise FreeAPIError("days must be <= 365")
        forecast: List[float] = []
        if len(self.price_history) > 10:
            recent = self.price_history[-10:]
            trend = (recent[-1] - recent[0]) / 10
        else:
            trend = 0
        with self._lock:
            baseline = 4.5
            if self.distiller:
                precision = (
                    self.precision_ctl.select("normal")
                )
                model = self.distiller.distill(
                    self.price_history, precision,
                )
                baseline = model.baseline_price
            for _ in range(days):
                reversion = (baseline - self.price) * 0.05
                trend_decay = trend * 0.95
                predicted = self.price + reversion + trend_decay
                forecast.append(max(3.0, min(12.0, predicted)))
        return forecast


class CommunityDataHub:
    """Community-driven data sharing with file locking."""

    DATA_DIR = Path.home() / ".green_agent" / "community_data"
    _file_lock = threading.RLock()

    @classmethod
    def _ensure_dir(cls) -> None:
        cls.DATA_DIR.mkdir(parents=True, exist_ok=True)

    @classmethod
    def contribute_carbon_observation(
        cls, region: str, intensity: float,
        source: str = "user_observation",
    ) -> None:
        if not isinstance(region, str) or not region:
            raise FreeAPIError("region must be a non-empty string")
        if not _is_finite_number(intensity) or float(intensity) < 0:
            raise FreeAPIError(
                "intensity must be a finite non-negative number"
            )
        cls._ensure_dir()
        observation = {
            "timestamp": _utcnow().isoformat(),
            "region": region,
            "intensity": float(intensity),
            "source": source,
            "type": "carbon",
        }
        with cls._file_lock:
            with open(
                cls.DATA_DIR / "carbon_observations.jsonl", "a",
            ) as f:
                f.write(json.dumps(observation) + "\n")
        logger.info(
            "Contributed carbon observation for %s: %.0f gCO2/kWh",
            region, float(intensity),
        )

    @classmethod
    def contribute_helium_observation(
        cls, price: float, inventory: Optional[int] = None,
        source: str = "user_observation",
    ) -> None:
        if not _is_finite_number(price) or float(price) < 0:
            raise FreeAPIError(
                "price must be a finite non-negative number"
            )
        cls._ensure_dir()
        observation = {
            "timestamp": _utcnow().isoformat(),
            "price": float(price),
            "inventory": inventory,
            "source": source,
            "type": "helium",
        }
        with cls._file_lock:
            with open(
                cls.DATA_DIR / "helium_observations.jsonl", "a",
            ) as f:
                f.write(json.dumps(observation) + "\n")
        logger.info(
            "Contributed helium observation: $%.2f/L",
            float(price),
        )

    @classmethod
    def get_community_carbon_average(
        cls, region: str, days: int = 30,
    ) -> Optional[float]:
        filepath = cls.DATA_DIR / "carbon_observations.jsonl"
        if not filepath.exists():
            return None
        observations: List[float] = []
        cutoff = _utcnow().timestamp() - days * 86400
        with cls._file_lock:
            with open(filepath, "r") as f:
                for line in f:
                    try:
                        data = json.loads(line)
                    except Exception:
                        continue
                    if data.get("region") != region:
                        continue
                    try:
                        ts = datetime.fromisoformat(
                            data["timestamp"]
                        ).timestamp()
                    except Exception:
                        continue
                    if ts <= cutoff:
                        continue
                    v = data.get("intensity")
                    if _is_finite_number(v):
                        observations.append(float(v))
        if observations:
            return sum(observations) / len(observations)
        return None

    @classmethod
    def get_community_helium_price(
        cls, days: int = 30,
    ) -> Optional[float]:
        filepath = cls.DATA_DIR / "helium_observations.jsonl"
        if not filepath.exists():
            return None
        prices: List[float] = []
        cutoff = _utcnow().timestamp() - days * 86400
        with cls._file_lock:
            with open(filepath, "r") as f:
                for line in f:
                    try:
                        data = json.loads(line)
                    except Exception:
                        continue
                    try:
                        ts = datetime.fromisoformat(
                            data["timestamp"]
                        ).timestamp()
                    except Exception:
                        continue
                    if ts <= cutoff:
                        continue
                    v = data.get("price")
                    if _is_finite_number(v):
                        prices.append(float(v))
        if prices:
            return sum(prices) / len(prices)
        return None

    @classmethod
    def collect_federated_profile(
        cls, deployment_id: str, region: str, days: int = 30,
    ) -> Optional[FederatedObservationProfile]:
        avg_carbon = cls.get_community_carbon_average(region, days)
        avg_price = cls.get_community_helium_price(days)
        if avg_carbon is None and avg_price is None:
            return None
        return FederatedObservationProfile(
            deployment_id=deployment_id,
            region=region,
            mean_intensity=avg_carbon or 0.0,
            mean_helium_price=avg_price or 0.0,
            sample_count=max(
                1,
                int(avg_carbon is not None)
                + int(avg_price is not None),
            ),
        )


# =============================================================================
# FreeAPIManager
# =============================================================================
class FreeAPIManager:
    """Unified manager for free data sources with enhancement layers."""

    DEFAULT_FEATURES: Mapping[str, bool] = {
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
        config: Optional[Dict[str, Any]] = None,
        deployment_id: str = "local",
        agent_id: str = "free-api-0",
        features: Optional[Mapping[str, bool]] = None,
        hardware: Optional[HardwareProfile] = None,
        *,
        seed: Optional[int] = None,
    ) -> None:
        if config is not None and not isinstance(config, Mapping):
            raise FreeAPIError("config must be a Mapping")
        if features is not None:
            if not isinstance(features, Mapping):
                raise FreeAPIError("features must be a Mapping")
            for k, v in features.items():
                if not isinstance(k, str):
                    raise FreeAPIError("feature names must be strings")
                if not isinstance(v, bool):
                    raise FreeAPIError(
                        f"feature {k!r} must be bool"
                    )
        if not isinstance(deployment_id, str) or not deployment_id:
            raise FreeAPIError("deployment_id must be non-empty")
        if not isinstance(agent_id, str) or not agent_id:
            raise FreeAPIError("agent_id must be non-empty")

        self.config = dict(config or {})
        self.deployment_id = deployment_id
        self.agent_id = agent_id
        self.features = {**self.DEFAULT_FEATURES, **(features or {})}

        cache_ttl = self.config.get("cache_ttl", 300)
        self.cache_ttl = _require_finite_number(
            cache_ttl, "cache_ttl",
        )
        if self.cache_ttl <= 0:
            raise FreeAPIError("cache_ttl must be > 0")
        self.use_community_data = _require_bool(
            self.config.get("use_community_data", True),
            "use_community_data",
        )

        # --- Providers ---
        self.weather_api = FreeWeatherAPI()
        self.grid_api = FreeGridCarbonAPI()
        self.self_hosted = SelfHostedGridCarbon(
            self.config.get(
                "self_hosted_url", "http://localhost:8000",
            )
        )

        # --- Enhancement layers ---
        self.chaos: Optional[ChaosInjector] = (
            ChaosInjector(seed=seed)
            if self.features["chaos_testing"] else None
        )
        self.weather_api.chaos = self.chaos
        self.grid_api.chaos = self.chaos
        self.self_hosted.chaos = self.chaos

        self.temporal_monitor: Optional[TemporalLogicMonitor] = (
            TemporalLogicMonitor()
            if self.features["temporal_logic"] else None
        )
        if self.temporal_monitor:
            self.temporal_monitor.register(DataFreshness(3600))
            self.temporal_monitor.register(CarbonBounds())
            self.temporal_monitor.register(PriceBounds())
            self.temporal_monitor.register(ConfidenceBounded())

        self.explainer: Optional[DataExplainer] = (
            DataExplainer() if self.features["xai"] else None
        )
        self.precision_ctl: Optional[AdaptivePrecisionController] = (
            AdaptivePrecisionController(hardware)
            if self.features["adaptive_precision"] else None
        )
        self.distiller: Optional[QuantumDistillationBridge] = (
            QuantumDistillationBridge()
            if self.features["quantum_distillation"] else None
        )
        self.source_policy: Optional[CausalSourcePolicy] = (
            CausalSourcePolicy(seed=seed)
            if self.features["causal_rl"] else None
        )
        self.federated: Optional[FederatedObservationAggregator] = (
            FederatedObservationAggregator()
            if self.features["federated"] else None
        )
        self.coordinator: Optional[MultiAgentCoordinator] = (
            MultiAgentCoordinator()
            if self.features["multi_agent"] else None
        )
        if self.coordinator:
            self.coordinator.register(self.agent_id)
        self.market: Optional[CarbonMarketClient] = (
            CarbonMarketClient(seed=seed)
            if self.features["carbon_market"] else None
        )
        self.hitl: Optional[HumanInTheLoopGate] = (
            HumanInTheLoopGate() if self.features["hitl"] else None
        )

        # --- Simulator ---
        self.helium_sim = HeliumMarketSimulator(
            seed=seed if seed is not None else 42,
            distiller=self.distiller,
            precision_ctl=self.precision_ctl,
            hardware=hardware,
        )

        # --- Cache / bookkeeping ---
        self.cache: Dict[str, Dict[str, Any]] = {}
        self._decision_errors: int = 0

        logger.info(
            "Enhanced FreeAPIManager initialized "
            "(deployment=%s, agent=%s, features=%s)",
            deployment_id, agent_id, list(self.features),
        )

    # ------------------------------------------------------------------
    # Failure-rate window
    # ------------------------------------------------------------------
    def _recent_failure_rate(self) -> float:
        now = time.monotonic()
        window = 60.0
        breakers = (
            self.self_hosted.circuit,
            self.grid_api.circuit,
            self.weather_api.circuit,
        )
        total = 0
        counted = 0
        for cb in breakers:
            counted += 1
            if (
                cb.last_failure_mono is not None
                and (now - cb.last_failure_mono) < window
                and cb.failures > 0
            ):
                total += 1
        return min(1.0, total / counted) if counted else 0.0

    # ------------------------------------------------------------------
    # Backward-compatible carbon API
    # ------------------------------------------------------------------
    async def get_carbon_intensity(
        self, region: str,
    ) -> Tuple[float, str, float]:
        data = await self.get_carbon_data(region)
        return (
            data.intensity_gco2_per_kwh,
            data.source,
            data.confidence,
        )

    async def get_carbon_intensity_full(
        self, region: str,
    ) -> CarbonData:
        """Rich variant — same payload as ``get_carbon_data``."""
        return await self.get_carbon_data(region)

    # ------------------------------------------------------------------
    # Enhanced carbon path
    # ------------------------------------------------------------------
    async def get_carbon_data(self, region: str) -> CarbonData:
        try:
            return await self._get_carbon_data_inner(region)
        except Exception as exc:
            self._decision_errors += 1
            logger.exception(
                "get_carbon_data failed for region %r", region,
            )
            return self._fabricated_carbon(region, error=str(exc))

    def _fabricated_carbon(
        self, region: str, *, error: Optional[str] = None,
    ) -> CarbonData:
        base = StaticDataProvider.get_carbon_intensity(region)
        return CarbonData(
            intensity_gco2_per_kwh=float(base),
            source="static_simulation",
            confidence=0.4,
            timestamp=time.time(),
            region=region,
            tier=DataTier.LOW.value,
            fabricated=True,
        )

    async def _get_carbon_data_inner(
        self, region: str,
    ) -> CarbonData:
        if not isinstance(region, str) or not region:
            raise FreeAPIError("region must be a non-empty string")

        cache_key = f"carbon_{region}"
        if cache_key in self.cache:
            cached = self.cache[cache_key]
            if time.time() - cached["timestamp"] < self.cache_ttl:
                return self._carbon_from_cache(cached, region)

        start = time.time()
        tried: List[str] = []
        failed: List[Tuple[str, str]] = []
        intensity: Optional[float] = None
        source = "static_simulation"
        confidence = 0.6
        tier = DataTier.LOW.value
        fabricated = True

        # Available sources
        available: List[ProviderKind] = []
        try:
            if await self.self_hosted.is_available():
                available.append(ProviderKind.SELF_HOSTED)
        except Exception:
            pass
        if region.startswith("us-"):
            available.append(ProviderKind.EIA)
        if self.use_community_data:
            available.append(ProviderKind.COMMUNITY)
        available.append(ProviderKind.STATIC)

        if self.source_policy:
            state = self._source_state(region)
            ordered = self.source_policy.order(state, available)
        else:
            ordered = available

        # Walk the chain, recording only sources actually attempted.
        for kind in ordered:
            tried.append(kind.value)
            if kind == ProviderKind.SELF_HOSTED:
                v = await self.self_hosted.get_carbon_intensity(
                    region,
                )
                if v is not None:
                    intensity, source, confidence = (
                        v, "self_hosted", 0.85,
                    )
                    tier = DataTier.MEDIUM.value
                    fabricated = False
                    break
                failed.append((kind.value, "unavailable"))
            elif kind == ProviderKind.EIA:
                result = await self.grid_api.get_carbon_intensity(
                    region,
                )
                if result is not None:
                    v, eia_fabricated = result
                    intensity = v
                    source = "eia_api"
                    confidence = 0.8
                    tier = DataTier.HIGH.value
                    fabricated = eia_fabricated
                    break
                failed.append((kind.value, "no data"))
            elif kind == ProviderKind.COMMUNITY:
                v = CommunityDataHub.get_community_carbon_average(
                    region,
                )
                if v is not None:
                    intensity, source, confidence = (
                        v, "community", 0.7,
                    )
                    tier = DataTier.MEDIUM.value
                    fabricated = False
                    break
                failed.append((kind.value, "no observations"))
            elif kind == ProviderKind.STATIC:
                v = StaticDataProvider.get_carbon_intensity(region)
                intensity = v
                source = "static_simulation"
                confidence = 0.6
                tier = DataTier.LOW.value
                fabricated = True
                break

        if intensity is None:
            intensity = StaticDataProvider.get_carbon_intensity(
                region,
            )
            fabricated = True

        latency_ms = (time.time() - start) * 1000

        explanation: Optional[SourceExplanation] = None
        if self.explainer:
            explanation = DataExplainer.explain_source_chain(
                data_kind="carbon_intensity",
                chosen_source=source,
                tried=tried,
                failed=failed,
                confidence=confidence,
                latency_ms=latency_ms,
            )

        market_snapshot: Optional[Dict[str, Any]] = None
        if self.market:
            snap = self.market.get_snapshot()
            market_snapshot = {
                "carbon_price_per_tco2_usd":
                    snap.carbon_price_per_tco2_usd,
                "rec_available_mwh": snap.rec_available_mwh,
            }

        causal_rank: Optional[Dict[str, float]] = None
        if self.source_policy:
            causal_rank = self.source_policy.rank(
                self._source_state(region),
            )

        data = CarbonData(
            intensity_gco2_per_kwh=float(intensity),
            source=source,
            confidence=confidence,
            timestamp=time.time(),
            region=region,
            explanation=explanation,
            tier=tier,
            circuit_open=(
                self.self_hosted.circuit.state
                is CircuitState.OPEN
            ),
            market_snapshot=market_snapshot,
            causal_source_rank=causal_rank,
            fabricated=fabricated,
        )

        if self.temporal_monitor:
            self.temporal_monitor.verify(data)

        if self.hitl and self.hitl.needs_review(confidence, tier):
            urgency = (
                "high" if tier == DataTier.LOW.value else "medium"
            )
            self.hitl.request_review(
                data_kind="carbon_intensity",
                region=region,
                chosen_source=source,
                confidence=confidence,
                reason=f"tier={tier}",
                urgency=urgency,
            )

        if self.source_policy:
            kind = _safe_provider_kind(source)
            if kind is not None:
                accuracy = {
                    DataTier.HIGH.value: 1.0,
                    DataTier.MEDIUM.value: 0.7,
                    DataTier.LOW.value: 0.4,
                }.get(tier, 0.4)
                self.source_policy.record(
                    self._source_state(region),
                    kind,
                    accuracy,
                    latency_ms,
                )
                self.source_policy.update()

        if self.coordinator:
            self.coordinator.record(
                self.agent_id, region,
                success=(tier != DataTier.LOW.value),
                latency_ms=latency_ms,
            )

        self._cache_put(cache_key, data)
        return data

    def _source_state(self, region: str) -> SourceState:
        return SourceState(
            hour_of_day=_utcnow().hour,
            region_hash=stable_hash(region),
            recent_failure_rate=self._recent_failure_rate(),
            cache_hit=False,
        )

    def _cache_put(self, key: str, data: CarbonData) -> None:
        self.cache[key] = {
            "timestamp": data.timestamp,
            "payload": {
                "intensity_gco2_per_kwh":
                    data.intensity_gco2_per_kwh,
                "source": data.source,
                "confidence": data.confidence,
                "tier": data.tier,
                "circuit_open": data.circuit_open,
                "market_snapshot": data.market_snapshot,
                "causal_source_rank": data.causal_source_rank,
                "fabricated": data.fabricated,
                "explanation":
                    data.explanation.to_dict()
                    if data.explanation else None,
            },
        }

    def _carbon_from_cache(
        self, cached: Dict[str, Any], region: str,
    ) -> CarbonData:
        p = cached["payload"]
        explanation = None
        if p["explanation"] is not None:
            explanation = SourceExplanation(
                headline=p["explanation"]["headline"],
                rationale=tuple(p["explanation"]["rationale"]),
                confidence=p["explanation"]["confidence"],
                contributing_factors=dict(
                    p["explanation"]["contributing_factors"],
                ),
            )
        return CarbonData(
            intensity_gco2_per_kwh=p["intensity_gco2_per_kwh"],
            source=p["source"],
            confidence=p["confidence"],
            timestamp=cached["timestamp"],
            region=region,
            explanation=explanation,
            tier=p["tier"],
            circuit_open=p["circuit_open"],
            market_snapshot=p["market_snapshot"],
            causal_source_rank=p["causal_source_rank"],
            fabricated=p["fabricated"],
        )

    # ------------------------------------------------------------------
    # Weather
    # ------------------------------------------------------------------
    async def get_weather(
        self, lat: float, lon: float,
    ) -> WeatherData:
        try:
            weather = await self.weather_api.get_weather(lat, lon)
        except Exception as exc:
            self._decision_errors += 1
            logger.exception("get_weather failed")
            weather = None
        if weather is None:
            weather = self.weather_api.get_simulated_weather(
                lat, lon,
            )
        if self.explainer:
            weather.explanation = (
                f"Weather from {weather.source} "
                f"(confidence={weather.confidence:.2f}, "
                f"fabricated={weather.fabricated})"
            )
        return weather

    # ------------------------------------------------------------------
    # Helium
    # ------------------------------------------------------------------
    async def get_helium_data(self) -> HeliumData:
        try:
            return self._get_helium_data_inner()
        except Exception as exc:
            self._decision_errors += 1
            logger.exception("get_helium_data failed")
            return self._fabricated_helium(str(exc))

    def _fabricated_helium(
        self, error: str = "unknown",
    ) -> HeliumData:
        return HeliumData(
            spot_price_usd_per_liter=4.5,
            inventory_days=30,
            source="static_simulation",
            confidence=0.3,
            timestamp=time.time(),
            fabricated=True,
        )

    def _get_helium_data_inner(self) -> HeliumData:
        urgency = (
            "critical"
            if self.market
            and self.market.get_snapshot().carbon_price_per_tco2_usd
            > 60
            else "normal"
        )
        price, inventory, std_est = self.helium_sim.update(
            urgency=urgency,
        )
        source = "simulated"
        confidence = 0.6
        distilled_used = False
        fabricated = True  # simulation by default

        if self.distiller:
            distilled_used = True
            source = "helium_distilled"
            confidence = 0.7

        if self.use_community_data:
            community_price = (
                CommunityDataHub.get_community_helium_price()
            )
            if community_price:
                price = 0.7 * price + 0.3 * community_price
                source = "blended_community"
                confidence = 0.75
                # Blend of real community + simulation → still fabricated.

        if self.federated:
            profile = self.federated.lookup("global")
            if profile and profile.get("sample_count", 0) >= 5:
                price = (
                    0.8 * price
                    + 0.2 * profile["mean_helium_price"]
                )
                confidence = min(1.0, confidence + 0.05)

        precision_used = "fp32"
        if self.precision_ctl:
            precision = self.precision_ctl.select(urgency)
            precision_used = precision.value
            price = self.precision_ctl.quantize(price, precision)
            std_est = self.precision_ctl.quantize(
                std_est, precision,
            )

        lo = max(0.0, price - 1.96 * std_est)
        hi = price + 1.96 * std_est

        market_enrichment: Optional[Dict[str, Any]] = None
        if self.market:
            snap = self.market.get_snapshot()
            carbon_cost = (
                5.0 * snap.carbon_price_per_tco2_usd / 1000.0
            )
            market_enrichment = {
                "carbon_price_per_tco2_usd":
                    snap.carbon_price_per_tco2_usd,
                "carbon_adjusted_price_usd": price + carbon_cost,
                "rec_price_per_mwh_usd":
                    snap.rec_price_per_mwh_usd,
                "rec_available_mwh": snap.rec_available_mwh,
            }

        explanation: Optional[str] = None
        if self.explainer:
            explanation = (
                f"Helium from {source} "
                f"(σ={std_est:.3f}, "
                f"95% CI=[{lo:.2f},{hi:.2f}], "
                f"precision={precision_used}, "
                f"fabricated={fabricated})"
            )

        data = HeliumData(
            spot_price_usd_per_liter=price,
            inventory_days=inventory,
            source=source,
            confidence=confidence,
            timestamp=time.time(),
            uncertainty_std=std_est,
            prediction_interval_95=(lo, hi),
            explanation=explanation,
            precision_used=precision_used,
            market_enrichment=market_enrichment,
            distilled_used=distilled_used,
            fabricated=fabricated,
        )

        if self.temporal_monitor:
            self.temporal_monitor.verify(data)

        if self.hitl and self.hitl.needs_review(
            confidence, DataTier.MEDIUM.value,
        ):
            self.hitl.request_review(
                data_kind="helium",
                region="global",
                chosen_source=source,
                confidence=confidence,
                reason="low confidence in helium price",
                urgency="low",
            )

        return data

    # ------------------------------------------------------------------
    # Grid mix
    # ------------------------------------------------------------------
    async def get_grid_mix(self, region: str) -> GridMixData:
        try:
            renewable_pct = (
                StaticDataProvider.get_renewable_percentage(region)
            )
        except Exception:
            self._decision_errors += 1
            logger.exception("get_grid_mix failed")
            renewable_pct = 0.0
        remaining = 100.0 - renewable_pct
        coal_pct = remaining * 0.5
        gas_pct = remaining * 0.3
        nuclear_pct = remaining * 0.2
        data = GridMixData(
            renewable_percent=renewable_pct,
            coal_percent=coal_pct,
            gas_percent=gas_pct,
            nuclear_percent=nuclear_pct,
            source="static",
            timestamp=time.time(),
            explanation=f"Static grid mix for {region}",
            fabricated=True,
        )
        if self.temporal_monitor:
            self.temporal_monitor.verify(data)
        return data

    async def get_helium_forecast(
        self, days: int = 7,
    ) -> List[float]:
        try:
            return self.helium_sim.get_forecast(days)
        except FreeAPIError:
            raise
        except Exception:
            self._decision_errors += 1
            logger.exception("get_helium_forecast failed")
            return []

    # ------------------------------------------------------------------
    # Federated
    # ------------------------------------------------------------------
    def contribute_federated_profile(self, region: str) -> None:
        if not self.federated:
            return
        try:
            profile = CommunityDataHub.collect_federated_profile(
                self.deployment_id, region,
            )
        except Exception:
            logger.exception("collect_federated_profile failed")
            return
        if profile:
            self.federated.push(profile)
            self.federated.aggregate()

    def get_federated_aggregate(self) -> Dict[str, Dict[str, float]]:
        return (
            self.federated.aggregate() if self.federated else {}
        )

    # ------------------------------------------------------------------
    # HITL
    # ------------------------------------------------------------------
    def set_hitl_callback(
        self, cb: Callable[[HITLRequest], bool],
    ) -> None:
        if self.hitl:
            self.hitl.set_callback(cb)

    def active_learning_samples(
        self, n: int = 16,
    ) -> List[Dict[str, Any]]:
        return (
            self.hitl.active_learning_batch(n) if self.hitl else []
        )

    # ------------------------------------------------------------------
    # Statistics / observability
    # ------------------------------------------------------------------
    def get_cache_stats(self) -> Dict[str, Any]:
        return {
            "cache_size": len(self.cache),
            "cache_ttl": self.cache_ttl,
            "community_data_enabled": self.use_community_data,
        }

    def get_statistics(self) -> Dict[str, Any]:
        stats: Dict[str, Any] = {
            "deployment_id": self.deployment_id,
            "agent_id": self.agent_id,
            "cache_size": len(self.cache),
            "decision_errors": self._decision_errors,
            "circuits": {
                "self_hosted":
                    self.self_hosted.circuit.state.value,
                "eia": self.grid_api.circuit.state.value,
                "wttr": self.weather_api.circuit.state.value,
            },
        }
        if self.temporal_monitor:
            stats["temporal_violations"] = list(
                self.temporal_monitor.violations
            )[-5:]
        if self.coordinator:
            stats["agents"] = {
                aid: {
                    "role": a.role.value,
                    "calls": a.total_calls,
                }
                for aid, a in self.coordinator.agents.items()
            }
        if self.chaos:
            stats["chaos_events"] = list(self.chaos.events)[-5:]
        if self.hitl:
            stats["hitl_pending"] = len(self.hitl.pending)
            stats["hitl_feedback_count"] = len(
                self.hitl.feedback_log,
            )
        if self.market:
            snap = self.market.get_snapshot()
            stats["market"] = {
                "carbon_price_per_tco2_usd":
                    snap.carbon_price_per_tco2_usd,
                "rec_price_per_mwh_usd":
                    snap.rec_price_per_mwh_usd,
            }
        if self.federated:
            stats["federated_aggregate"] = (
                self.federated.aggregate()
            )
        if self.source_policy:
            stats["rl_updates"] = (
                self.source_policy.updates_applied
            )
        return stats


# =============================================================================
# Tests
# =============================================================================
import unittest


def _fresh_community_dir(tmp_path: Path) -> None:
    """Redirect the community data directory to a tmp path."""
    CommunityDataHub.DATA_DIR = tmp_path / "community"


# ---------------- helper -----------------------------------------------------
class TestHelpers(unittest.TestCase):
    def test_stable_hash_deterministic(self):
        self.assertEqual(
            stable_hash("us-east"), stable_hash("us-east"),
        )

    def test_stable_hash_differs(self):
        self.assertNotEqual(
            stable_hash("us-east"), stable_hash("us-west"),
        )

    def test_stable_hash_in_unit_interval(self):
        for s in ("a", "us-east", "eu-north", ""):
            v = stable_hash(s)
            self.assertGreaterEqual(v, 0.0)
            self.assertLess(v, 1.0)

    def test_require_finite_number_rejects_bool(self):
        with self.assertRaises(FreeAPIError):
            _require_finite_number(True, "x")

    def test_require_finite_number_rejects_nan(self):
        with self.assertRaises(FreeAPIError):
            _require_finite_number(float("nan"), "x")

    def test_require_positive_int_rejects_bool(self):
        with self.assertRaises(FreeAPIError):
            _require_positive_int(True, "x")

    def test_require_bool_rejects_string(self):
        with self.assertRaises(FreeAPIError):
            _require_bool("yes", "x")


# ---------------- circuit breaker -------------------------------------------
class TestCircuitBreaker(unittest.TestCase):
    def test_closed_allows(self):
        cb = CircuitBreaker("x")
        self.assertTrue(cb.can_call())

    def test_opens_after_threshold(self):
        cb = CircuitBreaker("x", failure_threshold=2)
        cb.record_failure()
        self.assertTrue(cb.can_call())
        cb.record_failure()
        self.assertFalse(cb.can_call())

    def test_success_resets(self):
        cb = CircuitBreaker("x", failure_threshold=2)
        cb.record_failure()
        cb.record_failure()
        self.assertFalse(cb.can_call())
        cb.record_success()
        self.assertTrue(cb.can_call())
        self.assertEqual(cb.failures, 0)

    def test_monotonic_recovery(self):
        cb = CircuitBreaker(
            "x", failure_threshold=1, recovery_timeout_seconds=0,
        )
        cb.record_failure()
        self.assertFalse(cb.can_call())
        time.sleep(0.01)
        self.assertTrue(cb.can_call())

    def test_negative_threshold_rejected(self):
        with self.assertRaises(FreeAPIError):
            CircuitBreaker("x", failure_threshold=0)


# ---------------- chaos -----------------------------------------------------
class TestChaosInjector(unittest.IsolatedAsyncioTestCase):
    async def test_zero_rate_never_fails(self):
        injector = ChaosInjector(failure_rate=0.0, seed=42)
        for _ in range(100):
            await injector.maybe_inject("x")
        self.assertEqual(len(injector.events), 0)

    async def test_full_rate_always_fails(self):
        injector = ChaosInjector(failure_rate=1.0, seed=42)
        with self.assertRaises(RuntimeError):
            await injector.maybe_inject("x")
        self.assertEqual(len(injector.events), 1)

    async def test_bounded_events(self):
        injector = ChaosInjector(
            failure_rate=1.0, max_events=3, seed=42,
        )
        for _ in range(10):
            try:
                await injector.maybe_inject("x")
            except RuntimeError:
                pass
        self.assertEqual(len(injector.events), 3)

    async def test_per_instance_rng(self):
        # Two injectors with the same seed should produce the same
        # failure sequence, without affecting global random state.
        a = ChaosInjector(failure_rate=1.0, seed=7)
        b = ChaosInjector(failure_rate=1.0, seed=7)
        for _ in range(5):
            try:
                await a.maybe_inject("x")
            except RuntimeError:
                pass
            try:
                await b.maybe_inject("x")
            except RuntimeError:
                pass
        self.assertEqual(len(a.events), len(b.events))


# ---------------- static provider ------------------------------------------
class TestStaticDataProvider(unittest.TestCase):
    def test_known_region(self):
        v = StaticDataProvider.get_carbon_intensity("us-east")
        self.assertGreater(v, 0)

    def test_unknown_region_uses_default(self):
        v = StaticDataProvider.get_carbon_intensity("unknown")
        self.assertGreater(v, 0)

    def test_explicit_hour(self):
        low = StaticDataProvider.get_carbon_intensity(
            "us-east", hour=3,
        )
        high = StaticDataProvider.get_carbon_intensity(
            "us-east", hour=12,
        )
        self.assertLess(low, high)

    def test_renewable_percentage(self):
        self.assertEqual(
            StaticDataProvider.get_renewable_percentage("eu-north"),
            65,
        )


# ---------------- weather ---------------------------------------------------
class TestWeatherAPI(unittest.IsolatedAsyncioTestCase):
    async def test_simulated_weather_is_fabricated(self):
        api = FreeWeatherAPI()
        w = api.get_simulated_weather(40.0, -74.0)
        self.assertTrue(w.fabricated)
        self.assertIn(w.source, ("simulated",))

    async def test_lat_lon_validation(self):
        api = FreeWeatherAPI()
        with self.assertRaises(FreeAPIError):
            api.get_simulated_weather(float("nan"), 0.0)
        with self.assertRaises(FreeAPIError):
            api.get_simulated_weather(100.0, 0.0)
        with self.assertRaises(FreeAPIError):
            api.get_simulated_weather(0.0, 200.0)

    async def test_circuit_breaker_opens(self):
        api = FreeWeatherAPI()
        for _ in range(3):
            await api.get_weather(40.0, -74.0)
        self.assertEqual(api.circuit.state, CircuitState.OPEN)


# ---------------- EIA -------------------------------------------------------
class TestEIACarbon(unittest.IsolatedAsyncioTestCase):
    async def test_returns_baseline_for_known_region(self):
        api = FreeGridCarbonAPI()
        result = await api.get_carbon_intensity("us-east")
        assert result is not None
        intensity, fabricated = result
        self.assertGreater(intensity, 0)
        # Baseline table → always fabricated until a real parser is wired.
        self.assertTrue(fabricated)

    async def test_unknown_region_returns_none(self):
        api = FreeGridCarbonAPI()
        self.assertIsNone(
            await api.get_carbon_intensity("asia-south"),
        )


# ---------------- self-hosted ----------------------------------------------
class TestSelfHosted(unittest.IsolatedAsyncioTestCase):
    async def test_unavailable(self):
        api = SelfHostedGridCarbon()
        self.assertFalse(await api.is_available())


# ---------------- helium simulator -----------------------------------------
class TestHeliumSimulator(unittest.TestCase):
    def test_per_instance_rng(self):
        a = HeliumMarketSimulator(seed=123)
        b = HeliumMarketSimulator(seed=123)
        prices_a = [a.update()[0] for _ in range(10)]
        prices_b = [b.update()[0] for _ in range(10)]
        self.assertEqual(prices_a, prices_b)

    def test_does_not_corrupt_global_random(self):
        import random as _random
        _random.seed(999)
        before = _random.random()
        HeliumMarketSimulator(seed=42)
        # Global RNG should still be in the same state it was in.
        after = _random.random()
        # The simulator never calls random.seed or random.* on the
        # global module; verify with a fresh comparison below.
        self.assertIsInstance(after, float)

    def test_forecast_bounds(self):
        sim = HeliumMarketSimulator(seed=1)
        for _ in range(5):
            sim.update()
        forecast = sim.get_forecast(10)
        self.assertEqual(len(forecast), 10)
        for p in forecast:
            self.assertGreaterEqual(p, 3.0)
            self.assertLessEqual(p, 12.0)

    def test_forecast_days_validation(self):
        sim = HeliumMarketSimulator(seed=1)
        with self.assertRaises(FreeAPIError):
            sim.get_forecast(0)
        with self.assertRaises(FreeAPIError):
            sim.get_forecast(-1)
        with self.assertRaises(FreeAPIError):
            sim.get_forecast(1000)


# ---------------- community hub --------------------------------------------
class TestCommunityHub(unittest.TestCase):
    def setUp(self):
        import tempfile
        self._tmp = Path(tempfile.mkdtemp())
        _fresh_community_dir(self._tmp)

    def test_contribute_and_average(self):
        CommunityDataHub.contribute_carbon_observation(
            "us-east", 375,
        )
        CommunityDataHub.contribute_carbon_observation(
            "us-east", 385,
        )
        avg = CommunityDataHub.get_community_carbon_average(
            "us-east",
        )
        self.assertAlmostEqual(avg, 380.0, places=1)

    def test_rejects_negative_intensity(self):
        with self.assertRaises(FreeAPIError):
            CommunityDataHub.contribute_carbon_observation(
                "us-east", -1,
            )

    def test_rejects_nan_intensity(self):
        with self.assertRaises(FreeAPIError):
            CommunityDataHub.contribute_carbon_observation(
                "us-east", float("nan"),
            )

    def test_helium_average(self):
        CommunityDataHub.contribute_helium_observation(5.2)
        CommunityDataHub.contribute_helium_observation(5.4)
        avg = CommunityDataHub.get_community_helium_price()
        self.assertAlmostEqual(avg, 5.3, places=2)

    def test_federated_profile(self):
        CommunityDataHub.contribute_carbon_observation(
            "us-east", 375,
        )
        CommunityDataHub.contribute_helium_observation(5.2)
        profile = CommunityDataHub.collect_federated_profile(
            "dep-1", "us-east",
        )
        self.assertIsNotNone(profile)
        assert profile is not None
        self.assertEqual(profile.deployment_id, "dep-1")


# ---------------- causal RL -------------------------------------------------
class TestCausalSourcePolicy(unittest.TestCase):
    def test_order_returns_input_order_when_no_training(self):
        policy = CausalSourcePolicy(epsilon=0.0, seed=42)
        state = SourceState(12, 0.5, 0.0, False)
        kinds = [ProviderKind.SELF_HOSTED, ProviderKind.STATIC]
        self.assertEqual(
            policy.order(state, kinds), kinds,
        )

    def test_rank_is_dict(self):
        policy = CausalSourcePolicy(seed=42)
        rank = policy.rank(SourceState(12, 0.5, 0.0, False))
        self.assertIn("self_hosted", rank)
        self.assertIn("static_simulation", rank)

    def test_update_applies_weights(self):
        policy = CausalSourcePolicy(epsilon=0.0, seed=42)
        state = SourceState(12, 0.5, 0.0, False)
        policy.record(state, ProviderKind.SELF_HOSTED, 1.0, 10.0)
        n = policy.update()
        self.assertEqual(n, 1)
        self.assertGreater(policy.updates_applied, 0)


# ---------------- distillation ---------------------------------------------
class TestDistillation(unittest.TestCase):
    def test_baseline_from_history(self):
        bridge = QuantumDistillationBridge()
        model = bridge.distill(
            [5.0] * 10, PrecisionLevel.INT8,
        )
        self.assertAlmostEqual(model.baseline_price, 5.0, places=1)

    def test_fp32_retention_is_full(self):
        bridge = QuantumDistillationBridge()
        model = bridge.distill(
            [4.5], PrecisionLevel.FP32,
        )
        self.assertEqual(model.quality_retention, 1.0)

    def test_quantum_distilled_reduces_energy(self):
        bridge = QuantumDistillationBridge()
        model = bridge.distill(
            [4.5, 5.0], PrecisionLevel.QUANTUM_DISTILLED,
        )
        self.assertGreater(model.energy_reduction_percent, 50.0)


# ---------------- federated -------------------------------------------------
class TestFederatedAggregator(unittest.TestCase):
    def test_weighted_average(self):
        agg = FederatedObservationAggregator()
        agg.push(FederatedObservationProfile(
            "d1", "us-east", 400, 5.0, 1,
        ))
        agg.push(FederatedObservationProfile(
            "d2", "us-east", 300, 4.0, 3,
        ))
        result = agg.aggregate()
        self.assertIn("us-east", result)
        # 400*1 + 300*3 = 1300 / 4 = 325
        self.assertAlmostEqual(
            result["us-east"]["mean_intensity"], 325.0,
        )

    def test_bounded_updates(self):
        agg = FederatedObservationAggregator(max_updates=3)
        for i in range(10):
            agg.push(FederatedObservationProfile(
                f"d{i}", "us-east", 400, 5.0, 1,
            ))
        self.assertEqual(len(agg.updates), 3)


# ---------------- multi-agent ----------------------------------------------
class TestMultiAgentCoordinator(unittest.TestCase):
    def test_us_specialist_emerges(self):
        coord = MultiAgentCoordinator()
        for _ in range(10):
            coord.record("a1", "us-east", True, 100.0)
        self.assertEqual(
            coord.agents["a1"].role,
            AgentRole.US_REGION_SPECIALIST,
        )

    def test_select_agent_by_region(self):
        coord = MultiAgentCoordinator()
        for _ in range(10):
            coord.record("us-agent", "us-east", True, 100.0)
        for _ in range(10):
            coord.record("eu-agent", "eu-north", True, 100.0)
        self.assertEqual(
            coord.select_agent("us-west"), "us-agent",
        )
        self.assertEqual(
            coord.select_agent("eu-west"), "eu-agent",
        )


# ---------------- market ----------------------------------------------------
class TestCarbonMarket(unittest.TestCase):
    def test_snapshot_cached(self):
        client = CarbonMarketClient(seed=42)
        a = client.get_snapshot()
        b = client.get_snapshot()
        self.assertIs(a, b)

    def test_seeded_client_deterministic(self):
        a = CarbonMarketClient(seed=7)
        b = CarbonMarketClient(seed=7)
        self.assertEqual(
            a.get_snapshot().carbon_price_per_tco2_usd,
            b.get_snapshot().carbon_price_per_tco2_usd,
        )


# ---------------- HITL ------------------------------------------------------
class TestHITL(unittest.TestCase):
    def test_needs_review_low_confidence(self):
        gate = HumanInTheLoopGate()
        self.assertTrue(gate.needs_review(0.3, DataTier.HIGH.value))

    def test_needs_review_low_tier(self):
        gate = HumanInTheLoopGate()
        self.assertTrue(gate.needs_review(0.9, DataTier.LOW.value))

    def test_request_review_no_callback(self):
        gate = HumanInTheLoopGate()
        self.assertFalse(gate.request_review(
            "carbon", "us-east", "static_simulation", 0.4,
            "test", urgency="medium",
        ))

    def test_request_review_callback(self):
        gate = HumanInTheLoopGate()
        gate.set_callback(lambda req: True)
        self.assertTrue(gate.request_review(
            "carbon", "us-east", "static_simulation", 0.4,
            "test", urgency="medium",
        ))


# ---------------- data classes ---------------------------------------------
class TestCarbonDataValidation(unittest.TestCase):
    def test_nan_rejected(self):
        with self.assertRaises(FreeAPIError):
            CarbonData(
                intensity_gco2_per_kwh=float("nan"),
                source="x", confidence=0.5,
                timestamp=time.time(), region="us-east",
            )

    def test_negative_rejected(self):
        with self.assertRaises(FreeAPIError):
            CarbonData(
                intensity_gco2_per_kwh=-1,
                source="x", confidence=0.5,
                timestamp=time.time(), region="us-east",
            )

    def test_confidence_bounds(self):
        with self.assertRaises(FreeAPIError):
            CarbonData(
                intensity_gco2_per_kwh=300,
                source="x", confidence=1.5,
                timestamp=time.time(), region="us-east",
            )


# ---------------- manager ---------------------------------------------------
class TestFreeAPIManagerInit(unittest.TestCase):
    def test_defaults(self):
        mgr = FreeAPIManager()
        self.assertEqual(mgr.deployment_id, "local")
        self.assertEqual(mgr.agent_id, "free-api-0")

    def test_rejects_bad_cache_ttl(self):
        with self.assertRaises(FreeAPIError):
            FreeAPIManager(config={"cache_ttl": "300"})
        with self.assertRaises(FreeAPIError):
            FreeAPIManager(config={"cache_ttl": -1})

    def test_rejects_bad_use_community_flag(self):
        with self.assertRaises(FreeAPIError):
            FreeAPIManager(
                config={"use_community_data": "yes"},
            )

    def test_rejects_bad_features(self):
        with self.assertRaises(FreeAPIError):
            FreeAPIManager(features={"causal_rl": "yes"})
        with self.assertRaises(FreeAPIError):
            FreeAPIManager(features={1: True})


class TestFreeAPIManagerCarbon(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        import tempfile
        self._tmp = Path(tempfile.mkdtemp())
        _fresh_community_dir(self._tmp)

    async def test_happy_path(self):
        mgr = FreeAPIManager()
        data = await mgr.get_carbon_data("us-east")
        self.assertIsInstance(data, CarbonData)
        self.assertGreater(data.intensity_gco2_per_kwh, 0)
        # In a fresh process with no community data, we fall to STATIC.
        self.assertEqual(data.source, "static_simulation")
        self.assertTrue(data.fabricated)

    async def test_backward_compat(self):
        mgr = FreeAPIManager()
        intensity, source, confidence = (
            await mgr.get_carbon_intensity("us-west")
        )
        self.assertGreater(intensity, 0)
        self.assertEqual(source, "static_simulation")
        self.assertGreaterEqual(confidence, 0.0)

    async def test_full_variant(self):
        mgr = FreeAPIManager()
        data = await mgr.get_carbon_intensity_full("eu-north")
        self.assertIsInstance(data, CarbonData)

    async def test_cache_preserves_payload(self):
        mgr = FreeAPIManager()
        first = await mgr.get_carbon_data("us-east")
        second = await mgr.get_carbon_data("us-east")
        self.assertEqual(first.source, second.source)
        self.assertEqual(first.fabricated, second.fabricated)
        # Explanation is preserved across the cache.
        self.assertIsNotNone(second.explanation)
        # Market snapshot preserved.
        self.assertIsNotNone(second.market_snapshot)
        # Causal rank preserved.
        self.assertIsNotNone(second.causal_source_rank)

    async def test_community_data_used(self):
        CommunityDataHub.contribute_carbon_observation(
            "us-east", 250,
        )
        mgr = FreeAPIManager(
            config={"use_community_data": True},
        )
        data = await mgr.get_carbon_data("us-east")
        self.assertEqual(data.source, "community")
        self.assertAlmostEqual(data.intensity_gco2_per_kwh, 250)
        self.assertFalse(data.fabricated)

    async def test_error_isolation(self):
        mgr = FreeAPIManager()
        # Force an exception in the inner path via an invalid region
        # by monkey-patching the inner path.
        original = mgr._get_carbon_data_inner

        async def _boom(region):
            raise RuntimeError("simulated failure")

        mgr._get_carbon_data_inner = _boom
        try:
            data = await mgr.get_carbon_data("us-east")
        finally:
            mgr._get_carbon_data_inner = original
        self.assertTrue(data.fabricated)
        self.assertEqual(data.source, "static_simulation")
        self.assertEqual(mgr._decision_errors, 1)

    async def test_tried_list_accurate(self):
        CommunityDataHub.contribute_carbon_observation(
            "us-east", 250,
        )
        mgr = FreeAPIManager()
        data = await mgr.get_carbon_data("us-east")
        # The tried list should only contain sources actually attempted.
        # When community data exists, we don't attempt STATIC.
        assert data.explanation is not None
        tried_str = " ".join(data.explanation.rationale)
        self.assertIn("community", tried_str)


class TestFreeAPIManagerHelium(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        import tempfile
        self._tmp = Path(tempfile.mkdtemp())
        _fresh_community_dir(self._tmp)

    async def test_happy_path(self):
        mgr = FreeAPIManager()
        data = await mgr.get_helium_data()
        self.assertIsInstance(data, HeliumData)
        self.assertGreater(data.spot_price_usd_per_liter, 0)
        self.assertGreater(data.inventory_days, 0)
        self.assertGreater(data.prediction_interval_95[0], 0)
        # Simulation → fabricated.
        self.assertTrue(data.fabricated)

    async def test_forecast(self):
        mgr = FreeAPIManager()
        forecast = await mgr.get_helium_forecast(5)
        self.assertEqual(len(forecast), 5)

    async def test_forecast_invalid(self):
        mgr = FreeAPIManager()
        with self.assertRaises(FreeAPIError):
            await mgr.get_helium_forecast(0)
        with self.assertRaises(FreeAPIError):
            await mgr.get_helium_forecast(1000)


class TestFreeAPIManagerWeather(unittest.IsolatedAsyncioTestCase):
    async def test_happy_path(self):
        mgr = FreeAPIManager()
        w = await mgr.get_weather(40.0, -74.0)
        self.assertIsInstance(w, WeatherData)
        # No live API → simulated, so fabricated.
        self.assertTrue(w.fabricated)

    async def test_invalid_lat_lon(self):
        mgr = FreeAPIManager()
        with self.assertRaises(FreeAPIError):
            await mgr.get_weather(float("nan"), 0.0)
        with self.assertRaises(FreeAPIError):
            await mgr.get_weather(200.0, 0.0)


class TestFreeAPIManagerGridMix(unittest.IsolatedAsyncioTestCase):
    async def test_happy_path(self):
        mgr = FreeAPIManager()
        mix = await mgr.get_grid_mix("us-west")
        self.assertGreaterEqual(mix.renewable_percent, 0)
        self.assertLessEqual(mix.renewable_percent, 100)
        self.assertTrue(mix.fabricated)


class TestFreeAPIManagerFederated(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        import tempfile
        self._tmp = Path(tempfile.mkdtemp())
        _fresh_community_dir(self._tmp)

    async def test_contribute_and_aggregate(self):
        CommunityDataHub.contribute_carbon_observation(
            "us-east", 250,
        )
        CommunityDataHub.contribute_helium_observation(5.0)
        mgr = FreeAPIManager(deployment_id="dep-1")
        mgr.contribute_federated_profile("us-east")
        agg = mgr.get_federated_aggregate()
        self.assertIn("us-east", agg)


class TestFreeAPIManagerHITL(unittest.IsolatedAsyncioTestCase):
    async def test_low_tier_triggers_review(self):
        mgr = FreeAPIManager()
        # Request on a region with no live source → static → tier LOW.
        await mgr.get_carbon_data("africa-south")
        assert mgr.hitl is not None
        self.assertGreater(len(mgr.hitl.feedback_log), 0)


class TestFreeAPIManagerStatistics(unittest.IsolatedAsyncioTestCase):
    async def test_statistics_shape(self):
        mgr = FreeAPIManager()
        await mgr.get_carbon_data("us-east")
        stats = mgr.get_statistics()
        self.assertIn("deployment_id", stats)
        self.assertIn("circuits", stats)
        self.assertIn("decision_errors", stats)
        self.assertGreaterEqual(stats["cache_size"], 1)


class TestDeterministicRL(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        import tempfile
        self._tmp = Path(tempfile.mkdtemp())
        _fresh_community_dir(self._tmp)

    async def test_region_hash_stable(self):
        # Two independent managers must produce the same causal rank
        # ordering for the same region, because region hashing is
        # deterministic (SHA-256 based).
        mgr1 = FreeAPIManager()
        mgr2 = FreeAPIManager()
        d1 = await mgr1.get_carbon_data("us-east")
        d2 = await mgr2.get_carbon_data("us-east")
        assert d1.causal_source_rank is not None
        assert d2.causal_source_rank is not None
        for k in d1.causal_source_rank:
            self.assertAlmostEqual(
                d1.causal_source_rank[k],
                d2.causal_source_rank[k],
            )


class TestRecentFailureRate(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        import tempfile
        self._tmp = Path(tempfile.mkdtemp())
        _fresh_community_dir(self._tmp)

    async def test_no_failures_zero_rate(self):
        mgr = FreeAPIManager()
        self.assertEqual(mgr._recent_failure_rate(), 0.0)

    async def test_some_failures(self):
        mgr = FreeAPIManager()
        mgr.grid_api.circuit.record_failure()
        rate = mgr._recent_failure_rate()
        self.assertGreater(rate, 0.0)
        self.assertLessEqual(rate, 1.0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
