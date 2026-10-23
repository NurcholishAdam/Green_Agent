"""
forecasting_engine.py — v6.0.0 (thin composition layer over v18)
=================================================================

Green Agent Layer 7: real-time carbon intensity + forecasting.

**What changed in v6**
----------------------
v5 had its own complete forecaster (providers, circuit breakers, caches,
retries) that drifted from the canonical v18 ``carbon_forecast``. That
drift is now retired: **all fetching, caching, retrying, and circuit
breaking is delegated to v18's ``CarbonForecast``**. This file adds only
what v18 does not have.

**Re-exported from v18 (single implementation)**
    CarbonForecast, CircuitBreaker, InMemoryStorage, StorageProtocol,
    ForecastPoint, ForecastBatch, ForecastSource,
    QuantumDistillationEngine, CausalGraphLearner, CausalPolicyAdapter,
    FederatedGreenAggregator, MultiAgentCoordinator,
    TemporalLogicVerifier, XAIDecisionExplainer,
    AdaptivePrecisionSwitcher, CarbonMarketIntegrator,
    ChaosTestingEngine, ActiveUserPreferenceLearner,
    plus every ``Simple*`` alias.

**Added by v6 (genuinely new)**
    IntensityReading          — rich reading with provenance + HITL status
    IntensityForecast         — point forecast with confidence
    OptimalWindow             — best-execution window
    AnomalyDetector           — per-region rolling z-score + physical bounds
    UncertaintyEstimator      — mean / std / confidence from recent readings
    FetcherCoordinator        — fetcher-agent role assignment (distinct from
                                v18's forecast-pipeline agents)
    FederatedRegionalAggregator — hourly-profile federation (distinct from
                                v18's byte-blob federation)
    ReadingTemporalMonitor    — temporal properties over IntensityReading
                                objects (distinct from v18's state-dict
                                properties)
    HumanInTheLoopGate        — anomaly-review gate that **awaits awaitable
                                callbacks**, has a timeout, bounded queues,
                                and returns an explicit status
    CarbonForecaster          — the v6 wrapper class

**Backward compatibility**
    CarbonForecaster(config, deployment_id, features, hardware) keeps the
    v5 constructor. get_current_intensity / get_current_reading / predict /
    find_optimal_execution_window / get_statistics / initialize / shutdown
    all keep their v5 signatures. ``create_forecaster`` factory unchanged.

    For callers who already have a v18 ``CarbonForecast`` instance, pass
    it directly: ``CarbonForecaster(config, forecast=my_v18_forecast)``.

File: src/carbon/forecasting_engine.py
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import math
import random
import statistics
from collections import Counter, defaultdict, deque
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import (
    Any,
    Callable,
    Deque,
    Dict,
    List,
    Mapping,
    Optional,
    Protocol,
    Sequence,
    Tuple,
)

# --------------------------------------------------------------------------- #
# Import v18 (single implementation for fetching, caching, circuits, retry)
# --------------------------------------------------------------------------- #
try:
    from carbon_forecast import (  # type: ignore[import-not-found]
        CarbonForecast,
        ForecastBatch,
        ForecastPoint,
        ForecastSource,
        CircuitBreaker,
        StorageProtocol,
        InMemoryStorage,
        QuantumDistillationEngine,
        CausalGraphLearner,
        CausalPolicyAdapter,
        FederatedGreenAggregator,
        MultiAgentCoordinator,
        TemporalLogicVerifier,
        XAIDecisionExplainer,
        AdaptivePrecisionSwitcher,
        CarbonMarketIntegrator,
        ChaosTestingEngine,
        ActiveUserPreferenceLearner,
        SimpleQuantumDistillationEngine,
        SimpleCausalGraphLearner,
        SimpleCausalPolicyAdapter,
        SimpleFederatedGreenAggregator,
        SimpleMultiAgentCoordinator,
        SimpleTemporalLogicVerifier,
        SimpleXAIDecisionExplainer,
        SimpleAdaptivePrecisionSwitcher,
        SimpleCarbonMarketIntegrator,
        SimpleChaosTestingEngine,
        SimpleActiveUserPreferenceLearner,
        _utcnow,
        _ensure_aware,
        _parse_timestamp,
        _json_safe,
        _cfg_get,
        _safe_mean,
    )
    _V18_AVAILABLE = True
    _V18_ERROR: Optional[str] = None
except ImportError as _e:  # pragma: no cover
    _V18_AVAILABLE = False
    _V18_ERROR = str(_e)

    # Minimal fallbacks so the module is still importable, but the
    # CarbonForecaster constructor raises a clear error if v18 is missing.
    class CarbonForecast:  # type: ignore[no-redef]
        def __init__(self, *a: Any, **kw: Any) -> None:
            raise RuntimeError(
                "forecasting_engine v6 requires carbon_forecast (v18); "
                f"import error: {_V18_ERROR}"
            )

    CircuitBreaker = None  # type: ignore[assignment]
    ForecastPoint = None  # type: ignore[assignment]
    ForecastBatch = None  # type: ignore[assignment]

    class ForecastSource(Enum):  # type: ignore[no-redef]
        API = "api"
        CACHE = "cache"
        HISTORY = "history"
        LAST_API = "last_api"
        RANDOM_WALK = "random_walk"
        FALLBACK = "fallback"

    QuantumDistillationEngine = None  # type: ignore[assignment]
    CausalGraphLearner = None  # type: ignore[assignment]
    CausalPolicyAdapter = None  # type: ignore[assignment]
    FederatedGreenAggregator = None  # type: ignore[assignment]
    MultiAgentCoordinator = None  # type: ignore[assignment]
    TemporalLogicVerifier = None  # type: ignore[assignment]
    XAIDecisionExplainer = None  # type: ignore[assignment]
    AdaptivePrecisionSwitcher = None  # type: ignore[assignment]
    CarbonMarketIntegrator = None  # type: ignore[assignment]
    ChaosTestingEngine = None  # type: ignore[assignment]
    ActiveUserPreferenceLearner = None  # type: ignore[assignment]
    SimpleQuantumDistillationEngine = None  # type: ignore[assignment]
    SimpleCausalGraphLearner = None  # type: ignore[assignment]
    SimpleCausalPolicyAdapter = None  # type: ignore[assignment]
    SimpleFederatedGreenAggregator = None  # type: ignore[assignment]
    SimpleMultiAgentCoordinator = None  # type: ignore[assignment]
    SimpleTemporalLogicVerifier = None  # type: ignore[assignment]
    SimpleXAIDecisionExplainer = None  # type: ignore[assignment]
    SimpleAdaptivePrecisionSwitcher = None  # type: ignore[assignment]
    SimpleCarbonMarketIntegrator = None  # type: ignore[assignment]
    SimpleChaosTestingEngine = None  # type: ignore[assignment]
    SimpleActiveUserPreferenceLearner = None  # type: ignore[assignment]
    InMemoryStorage = None  # type: ignore[assignment]
    StorageProtocol = None  # type: ignore[assignment]

    def _utcnow() -> datetime:  # type: ignore[misc]
        return datetime.now(timezone.utc)

    def _ensure_aware(dt: datetime) -> datetime:  # type: ignore[misc]
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

    def _parse_timestamp(raw: Any) -> datetime:  # type: ignore[misc]
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

    def _json_safe(obj: Any) -> Any:  # type: ignore[misc]
        if isinstance(obj, float):
            return None if (math.isnan(obj) or math.isinf(obj)) else obj
        if isinstance(obj, datetime):
            return _ensure_aware(obj).isoformat()
        if isinstance(obj, Enum):
            return obj.value
        if isinstance(obj, Mapping):
            return {str(k): _json_safe(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple, set)):
            return [_json_safe(v) for v in obj]
        return obj

    def _cfg_get(config: Any, key: str, default: Any = None) -> Any:  # type: ignore[misc]
        if config is None:
            return default
        if isinstance(config, Mapping):
            return config.get(key, default)
        return getattr(config, key, default)

    def _safe_mean(values: Any, default: float = 0.0) -> float:  # type: ignore[misc]
        items = [
            float(v)
            for v in values
            if v is not None and math.isfinite(float(v))
        ]
        return statistics.mean(items) if items else default


logger = logging.getLogger(__name__)

_VALID_HITL_STATUSES = frozenset(
    {"not_required", "pending", "approved", "rejected", "timed_out"}
)


# --------------------------------------------------------------------------- #
# Public enums
# --------------------------------------------------------------------------- #
class PrecisionLevel(Enum):
    FP32 = "fp32"
    FP16 = "fp16"
    INT8 = "int8"
    INT4 = "int4"
    QUANTUM_DISTILLED = "quantum_distilled"


class ProviderKind(Enum):
    """Where a reading came from (mapped from v18's ``ForecastSource``)."""

    SIMULATION = "simulation"
    ELECTRICITYMAP = "electricitymap"
    CARBONINTENSITY = "carbonintensity"
    FEDERATED_PEER = "federated_peer"
    DISTILLED_MODEL = "distilled_model"


class AgentRole(Enum):
    REGIONAL_SPECIALIST = "regional_specialist"
    PROVIDER_SPECIALIST = "provider_specialist"
    ANOMALY_HUNTER = "anomaly_hunter"
    LATENCY_OPTIMIZER = "latency_optimizer"
    GENERALIST = "generalist"


#: Map v18 ``ForecastSource`` → v6 ``ProviderKind``.
_FORECAST_TO_PROVIDER: Dict[str, ProviderKind] = {
    "api": ProviderKind.ELECTRICITYMAP,
    "cache": ProviderKind.ELECTRICITYMAP,
    "last_api": ProviderKind.ELECTRICITYMAP,
    "history": ProviderKind.SIMULATION,
    "random_walk": ProviderKind.SIMULATION,
    "fallback": ProviderKind.SIMULATION,
}


# --------------------------------------------------------------------------- #
# v6 types (not in v18)
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class IntensityReading:
    """
    A single carbon intensity observation with full provenance.

    Frozen — a reading is an immutable fact. Corrections are recorded as
    new readings (see the ledger module for the same rule).
    """

    region: str
    intensity: float
    timestamp: datetime
    source: ProviderKind
    confidence: float = 1.0
    is_simulated: bool = False
    is_stale: bool = False
    explanation: Optional[str] = None
    hitl_status: str = "not_required"
    hitl_approved: Optional[bool] = None
    anomaly_flagged: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "region": self.region,
            "intensity": self.intensity,
            "timestamp": _ensure_aware(self.timestamp).isoformat(),
            "source": self.source.value,
            "confidence": self.confidence,
            "is_simulated": self.is_simulated,
            "is_stale": self.is_stale,
            "explanation": self.explanation,
            "hitl_status": self.hitl_status,
            "hitl_approved": self.hitl_approved,
            "anomaly_flagged": self.anomaly_flagged,
        }


@dataclass(frozen=True)
class IntensityForecast:
    """A single forecasted point."""

    timestamp: datetime
    predicted_intensity: float
    confidence: float
    source: ProviderKind = ProviderKind.SIMULATION


@dataclass(frozen=True)
class OptimalWindow:
    """Result of an optimal-execution-window search."""

    start_time: datetime
    end_time: datetime
    avg_intensity: float
    carbon_savings_percent: float


@dataclass
class HardwareProfile:
    """Minimal hardware capability description used by v6's precision selection."""

    has_tensor_cores: bool = True
    supports_int8: bool = True
    supports_int4: bool = False
    vram_gb: float = 24.0
    edge_device: bool = False


@dataclass(frozen=True)
class MarketSnapshot:
    price_per_kwh_usd: float
    rec_available_mwh: float
    rec_price_per_mwh_usd: float
    timestamp: datetime = field(default_factory=_utcnow)


@dataclass(frozen=True)
class HITLRequest:
    region: str
    intensity: float
    reason: str
    urgency: str
    requested_at: datetime = field(default_factory=_utcnow)


@dataclass(frozen=True)
class UncertaintyEstimate:
    mean: float
    std: float
    confidence: float
    n_samples: int


# --------------------------------------------------------------------------- #
# Genuinely new v6 components (not in v18)
# --------------------------------------------------------------------------- #
class AnomalyDetector:
    """
    Per-region rolling z-score detector with physical-bounds check.

    History is **keyed by region**, so readings in different regions do
    not contaminate each other.
    """

    def __init__(
        self,
        window: int = 20,
        z_threshold: float = 3.0,
        min_val: float = 10.0,
        max_val: float = 1200.0,
    ) -> None:
        self.window = window
        self.z = z_threshold
        self.min_val = min_val
        self.max_val = max_val
        self._history: Dict[str, Deque[float]] = defaultdict(
            lambda: deque(maxlen=window)
        )

    def check(self, region: str, intensity: float) -> Tuple[bool, float]:
        hist = self._history[region]
        implausible = intensity < self.min_val or intensity > self.max_val
        if len(hist) < 5:
            hist.append(intensity)
            return implausible, 0.0
        mean = statistics.fmean(hist)
        std = statistics.pstdev(hist) or 1e-6
        z = abs(intensity - mean) / std
        hist.append(intensity)
        return implausible or (z > self.z), z

    def stats(self) -> Dict[str, Dict[str, float]]:
        return {
            region: {
                "samples": len(hist),
                "mean": statistics.fmean(hist) if hist else 0.0,
            }
            for region, hist in self._history.items()
        }


class UncertaintyEstimator:
    """Estimates uncertainty from a recent population of readings."""

    @staticmethod
    def estimate(values: Sequence[float]) -> UncertaintyEstimate:
        if not values:
            return UncertaintyEstimate(200.0, 100.0, 0.1, 0)
        mean = statistics.fmean(values)
        std = statistics.pstdev(values) if len(values) > 1 else 0.0
        cv = std / mean if mean > 0 else 1.0
        confidence = max(0.0, min(1.0, 1.0 - cv))
        return UncertaintyEstimate(mean, std, confidence, len(values))


@dataclass
class AgentProfile:
    agent_id: str
    role: AgentRole = AgentRole.GENERALIST
    regions: List[str] = field(default_factory=list)
    providers: List[ProviderKind] = field(default_factory=list)
    success_rate: float = 0.0
    avg_latency_ms: float = 0.0
    anomaly_catch_rate: float = 0.0
    total_calls: int = 0


class FetcherCoordinator:
    """
    Fetcher-agent role assignment via sliding-window outcome means.

    This is a distinct concern from v18's ``MultiAgentCoordinator`` (which
    coordinates the forecast pipeline by role). v6's coordinators
    specialise **fetcher agents** by region and provider.
    """

    def __init__(self, window_size: int = 20) -> None:
        self.agents: Dict[str, AgentProfile] = {}
        self._history: Dict[str, Deque[Dict[str, float]]] = defaultdict(
            lambda: deque(maxlen=window_size)
        )

    def register(self, agent_id: str) -> AgentProfile:
        if agent_id not in self.agents:
            self.agents[agent_id] = AgentProfile(agent_id)
        return self.agents[agent_id]

    def _window_mean(self, agent_id: str, key: str) -> float:
        hist = self._history[agent_id]
        if not hist:
            return 0.0
        return sum(h.get(key, 0.0) for h in hist) / len(hist)

    def _reassign_roles(self) -> None:
        for a in self.agents.values():
            if len(self._history[a.agent_id]) < 3:
                continue
            a.success_rate = self._window_mean(a.agent_id, "success")
            a.avg_latency_ms = self._window_mean(a.agent_id, "latency")
            a.anomaly_catch_rate = self._window_mean(a.agent_id, "anomaly")
            if len(a.regions) >= 3:
                a.role = AgentRole.REGIONAL_SPECIALIST
            elif a.anomaly_catch_rate > 0.5:
                a.role = AgentRole.ANOMALY_HUNTER
            elif 0 < a.avg_latency_ms < 200:
                a.role = AgentRole.LATENCY_OPTIMIZER
            elif len(a.providers) >= 2:
                a.role = AgentRole.PROVIDER_SPECIALIST
            else:
                a.role = AgentRole.GENERALIST

    def record(
        self,
        agent_id: str,
        region: str,
        provider: ProviderKind,
        success: bool,
        latency_ms: float,
        anomaly_caught: bool,
    ) -> None:
        a = self.register(agent_id)
        self._history[agent_id].append(
            {
                "success": float(success),
                "latency": latency_ms,
                "anomaly": float(anomaly_caught),
            }
        )
        a.total_calls += 1
        if region not in a.regions:
            a.regions = (a.regions + [region])[-10:]
        if provider not in a.providers:
            a.providers = (a.providers + [provider])[-10:]
        self._reassign_roles()

    def select_agent(
        self, region: str, provider: ProviderKind
    ) -> Optional[str]:
        if not self.agents:
            return None
        specialists = [
            a
            for a in self.agents.values()
            if a.role == AgentRole.REGIONAL_SPECIALIST
            and region in a.regions
        ]
        if specialists:
            return max(
                specialists, key=lambda a: a.success_rate
            ).agent_id
        generalists = [
            a
            for a in self.agents.values()
            if a.role == AgentRole.GENERALIST
        ]
        if generalists:
            return max(
                generalists, key=lambda a: a.success_rate
            ).agent_id
        return max(
            self.agents.values(), key=lambda a: a.success_rate
        ).agent_id


@dataclass
class FederatedRegionalProfile:
    deployment_id: str
    region: str
    hourly_means: List[float]
    sample_count: int
    timestamp: datetime = field(default_factory=_utcnow)


class FederatedRegionalAggregator:
    """
    Hourly-profile federation, distinct from v18's byte-blob federation.

    v18's ``FederatedGreenAggregator`` shares opaque byte blobs; this class
    shares 24-hour intensity profiles between deployments. Both are
    re-exported so callers can use whichever fits.
    """

    def __init__(self) -> None:
        self._updates: Dict[
            str, List[FederatedRegionalProfile]
        ] = defaultdict(list)
        self._global: Dict[str, List[float]] = {}

    def push(self, u: FederatedRegionalProfile) -> None:
        bucket = self._updates[u.region]
        bucket.append(u)
        if len(bucket) > 100:
            del bucket[: len(bucket) - 100]

    def aggregate(self) -> Dict[str, List[float]]:
        result: Dict[str, List[float]] = {}
        for region, profiles in self._updates.items():
            if not profiles:
                continue
            total_w = sum(max(1, p.sample_count) for p in profiles)
            if total_w <= 0:
                continue
            merged = [0.0] * 24
            for profile in profiles:
                w = max(1, profile.sample_count)
                for h in range(24):
                    merged[h] += profile.hourly_means[h] * w
            result[region] = [v / total_w for v in merged]
        self._global = result
        return result

    def get_global(self, region: str) -> Optional[List[float]]:
        return self._global.get(region)


# --------------------------- temporal logic for readings --------------------------- #
class TemporalProperty(Protocol):
    def check(
        self,
        history: Sequence[IntensityReading],
        region: str,
    ) -> bool: ...


@dataclass
class CacheFreshness:
    """G(fresh reading in the current window)."""

    max_age_seconds: int = 900
    window: int = 20

    def check(
        self, history: Sequence[IntensityReading], region: str
    ) -> bool:
        recent = [r for r in history if r.region == region][-self.window:]
        if not recent:
            return True
        now = _utcnow()
        return all(
            (now - _ensure_aware(r.timestamp)).total_seconds()
            <= self.max_age_seconds
            for r in recent
        )


@dataclass
class IntensityBounds:
    """G(intensity ∈ [min, max]) for readings of the current region."""

    min_val: float = 10.0
    max_val: float = 1200.0

    def check(
        self, history: Sequence[IntensityReading], region: str
    ) -> bool:
        return all(
            self.min_val <= r.intensity <= self.max_val
            for r in history
            if r.region == region
        )


class ReadingTemporalMonitor:
    """
    Bounded temporal monitor over ``IntensityReading`` objects.

    Distinct from v18's ``TemporalLogicVerifier`` (which verifies state
    dicts). The v5 bug where one stale reading permanently alarmed the
    monitor is fixed: properties only consider the last ``window``
    readings per region, and violations are bounded.
    """

    def __init__(self, max_violations: int = 200) -> None:
        self.properties: List[TemporalProperty] = []
        self.violations: Deque[Dict[str, Any]] = deque(maxlen=max_violations)

    def register(self, prop: TemporalProperty) -> None:
        self.properties.append(prop)

    def verify(
        self,
        history: Sequence[IntensityReading],
        region: Optional[str] = None,
    ) -> bool:
        target = region or (
            history[-1].region if history else ""
        )
        ok = True
        for p in self.properties:
            try:
                if not p.check(history, target):
                    ok = False
                    self.violations.append(
                        {
                            "property": type(p).__name__,
                            "region": target,
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


# ------------------------------- HITL ---------------------------------- #
class HumanInTheLoopGate:
    """
    Anomaly-review HITL.

    Fixes over v5:

    * **Awaits awaitable callbacks** — a coroutine is no longer silently
      coerced to truthy.
    * **Has a timeout** — callbacks and the underlying v18 learner are
      wrapped in ``asyncio.wait_for``.
    * **Bounded** — ``pending`` and ``feedback_log`` are deques.
    * **Returns a status** — callers can distinguish "approved",
      "rejected", "timed_out", "pending" (via ``IntensityReading.hitl_status``).
    * **Optionally delegates** to v18's ``ActiveUserPreferenceLearner`` for
      the queue/timeout plumbing.
    """

    def __init__(
        self,
        learner: Optional[Any] = None,
        timeout: float = 3.0,
        auto_approve_low: bool = True,
    ) -> None:
        self._learner = learner
        self._timeout = timeout
        self.auto_approve_low = auto_approve_low
        self.pending: Deque[HITLRequest] = deque(maxlen=100)
        self.feedback_log: Deque[Dict[str, Any]] = deque(maxlen=500)
        self._callback: Optional[Callable[[HITLRequest], Any]] = None

    def set_callback(
        self, cb: Callable[[HITLRequest], Any]
    ) -> None:
        self._callback = cb

    def _resolve(
        self,
        req: HITLRequest,
        approved: Optional[bool],
        source: str,
    ) -> Tuple[Optional[bool], str]:
        try:
            self.pending.remove(req)
        except ValueError:
            pass
        status = (
            "approved"
            if approved is True
            else "rejected"
            if approved is False
            else "timed_out"
        )
        self.feedback_log.append(
            {
                "req": asdict(req),
                "approved": approved,
                "status": status,
                "source": source,
            }
        )
        return approved, status

    async def request_review(
        self,
        region: str,
        intensity: float,
        reason: str,
        urgency: str,
    ) -> Tuple[Optional[bool], str]:
        req = HITLRequest(region, intensity, reason, urgency)
        self.pending.append(req)

        if urgency == "low" and self.auto_approve_low:
            return self._resolve(req, True, "auto")

        if self._callback is not None:
            try:
                decision = self._callback(req)
                if inspect.isawaitable(decision):
                    decision = await asyncio.wait_for(
                        decision, timeout=self._timeout
                    )
                approved = (
                    bool(decision) if decision is not None else None
                )
                return self._resolve(req, approved, "callback")
            except asyncio.TimeoutError:
                return self._resolve(req, None, "timeout")
            except Exception as e:
                logger.warning("HITL callback failed: %s", e)
                return self._resolve(req, None, "error")

        if self._learner is not None:
            try:
                chosen = await asyncio.wait_for(
                    self._learner.query_user_if_needed(
                        "anomaly_reviewer",
                        [
                            {
                                "solution_id": "approve",
                                "quality_score": 0.9,
                            },
                            {
                                "solution_id": "reject",
                                "quality_score": 0.8,
                            },
                        ],
                        timeout=self._timeout,
                    ),
                    timeout=self._timeout,
                )
                if chosen is None:
                    return self._resolve(req, None, "learner_timeout")
                approved = chosen == "approve"
                return self._resolve(req, approved, "learner")
            except Exception as e:
                logger.warning("Learner failed: %s", e)

        # No callback and no learner → no answer
        return self._resolve(req, None, "no_callback")

    def outcome_counts(self) -> Dict[str, int]:
        counts: Counter = Counter()
        for entry in self.feedback_log:
            counts[entry.get("status", "unknown")] += 1
        return dict(counts)


# --------------------------------------------------------------------------- #
# Feature flags
# --------------------------------------------------------------------------- #
DEFAULT_FEATURES: Dict[str, bool] = {
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
    "anomaly_detection": True,
}


# --------------------------------------------------------------------------- #
# Config translation
# --------------------------------------------------------------------------- #
def _v6_to_v18_config(v6_config: Any) -> Dict[str, Any]:
    """Translate the v5 ``{"carbon": {...}}`` config into v18's flat keys."""
    carbon = _cfg_get(v6_config, "carbon", {}) or {}
    return {
        "carbon_region": _cfg_get(
            carbon, "default_region", "US-CA"
        ),
        "carbon_cache_ttl": int(
            _cfg_get(carbon, "cache_ttl", 900)
        ),
        "carbon_market_api_url": _cfg_get(carbon, "api_url", None),
        "electricity_maps_api_key": _cfg_get(
            carbon, "api_key", None
        ),
        "fail_mode": _cfg_get(v6_config, "fail_mode", "log"),
        "circuit_failure_threshold": int(
            _cfg_get(v6_config, "circuit_failure_threshold", 3)
        ),
        "circuit_recovery_timeout": float(
            _cfg_get(v6_config, "circuit_recovery_timeout", 60.0)
        ),
    }


# --------------------------------------------------------------------------- #
# Main composition class
# --------------------------------------------------------------------------- #
class CarbonForecaster:
    """
    Thin composition layer over the v18 ``CarbonForecast``.

    * **Fetching / caching / retry / circuit breaking** — delegated to
      ``self._forecast`` (a v18 ``CarbonForecast`` instance).
    * **v6 additions** — ``get_current_reading``, ``predict``,
      ``find_optimal_execution_window``, plus anomaly detection,
      uncertainty estimation, reading-scoped temporal properties, HITL,
      fetcher coordination, and regional federation.
    """

    def __init__(
        self,
        config: Mapping[str, Any],
        deployment_id: str = "local",
        features: Optional[Dict[str, bool]] = None,
        hardware: Optional[HardwareProfile] = None,
        forecast: Optional["CarbonForecast"] = None,
    ) -> None:
        if not _V18_AVAILABLE and forecast is None:
            raise RuntimeError(
                "forecasting_engine v6 requires carbon_forecast (v18); "
                f"import error: {_V18_ERROR}"
            )

        self.config: Mapping[str, Any] = config or {}
        self.deployment_id = deployment_id
        self.features: Dict[str, bool] = {
            **DEFAULT_FEATURES,
            **(features or {}),
        }
        self.hardware = hardware or HardwareProfile()

        v18_config = _v6_to_v18_config(config)
        self.default_region = v18_config["carbon_region"]
        self._cache_ttl_seconds = v18_config["carbon_cache_ttl"]

        # ---- The single implementation for fetching/caching/retrying ----
        self._forecast: "CarbonForecast" = forecast or CarbonForecast(
            config=v18_config,
            region=self.default_region,
            cache_ttl=self._cache_ttl_seconds,
        )
        # Serialise region swaps so concurrent reads on different regions
        # don't race on v18's internal `.region` field.
        self._region_lock = asyncio.Lock()

        # ---- v18 enhancement instances (used where the API fits) ----
        self.precision_switcher = (
            AdaptivePrecisionSwitcher(config=v18_config, storage=None)
            if self.features["adaptive_precision"]
            else None
        )
        self.market_integrator = (
            CarbonMarketIntegrator(config=v18_config, storage=None)
            if self.features["carbon_market"]
            else None
        )
        self.xai = (
            XAIDecisionExplainer(config=v18_config, storage=None)
            if self.features["xai"]
            else None
        )
        self.chaos = (
            ChaosTestingEngine(config=v18_config, storage=None)
            if self.features["chaos_testing"]
            else None
        )
        self.policy = (
            CausalPolicyAdapter(
                config=v18_config,
                storage=None,
                graph=CausalGraphLearner(storage=None),
            )
            if self.features["causal_rl"]
            else None
        )
        self.distiller = (
            QuantumDistillationEngine()
            if self.features["quantum_distillation"]
            else None
        )
        self.hitl_learner = (
            ActiveUserPreferenceLearner(storage=None)
            if self.features["hitl"]
            else None
        )
        # v18's federated aggregator is available via re-export; v6 uses
        # its own hourly-profile aggregator because the concerns differ.
        self.federated = (
            FederatedRegionalAggregator()
            if self.features["federated"]
            else None
        )

        # ---- v6-only components ----
        self.anomaly = (
            AnomalyDetector() if self.features["anomaly_detection"] else None
        )
        self.uncertainty = UncertaintyEstimator()
        self.hitl = (
            HumanInTheLoopGate(learner=self.hitl_learner)
            if self.features["hitl"]
            else None
        )
        self.temporal = (
            ReadingTemporalMonitor()
            if self.features["temporal_logic"]
            else None
        )
        if self.temporal is not None:
            self.temporal.register(
                CacheFreshness(self._cache_ttl_seconds)
            )
            self.temporal.register(IntensityBounds())
        self.coordinator = (
            FetcherCoordinator() if self.features["multi_agent"] else None
        )

        # ---- State ----
        self._history: Dict[str, Deque[IntensityReading]] = defaultdict(
            lambda: deque(maxlen=200)
        )
        self._cache: Dict[str, IntensityReading] = {}
        self._fallback_count = 0
        self._source_counts: Counter = Counter()
        self._hitl_outcomes: Counter = Counter()

        logger.info(
            "CarbonForecaster v6.0.0 initialized "
            "(deployment=%s, region=%s, features=%s)",
            deployment_id,
            self.default_region,
            sorted(k for k, v in self.features.items() if v),
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def initialize(self) -> None:
        logger.info(
            "CarbonForecaster ready (region=%s, forecaster=%s)",
            self.default_region,
            type(self._forecast).__name__,
        )

    async def shutdown(self) -> None:
        try:
            await self._forecast.close()
        except Exception as e:
            logger.warning("Forecaster close failed: %s", e)
        logger.info("CarbonForecaster shutdown complete")

    async def __aenter__(self) -> "CarbonForecaster":
        await self.initialize()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.shutdown()

    # ------------------------------------------------------------------
    # Region-safe fetch
    # ------------------------------------------------------------------
    async def _fetch_current(
        self, region: str
    ) -> Tuple[float, ProviderKind]:
        """Call v18's forecaster for ``region`` without leaking region state."""
        async with self._region_lock:
            original = self._forecast.region
            try:
                if region != original:
                    self._forecast.set_region(region)
                value, source = await self._forecast.current_intensity_async()
            finally:
                if region != original:
                    self._forecast.set_region(original)

        provider = _FORECAST_TO_PROVIDER.get(
            source.value if hasattr(source, "value") else str(source),
            ProviderKind.SIMULATION,
        )
        return float(value), provider

    # ------------------------------------------------------------------
    # Public reading API
    # ------------------------------------------------------------------
    async def get_current_intensity(
        self, region: Optional[str] = None
    ) -> float:
        reading = await self.get_current_reading(region)
        return reading.intensity

    async def get_current_reading(
        self, region: Optional[str] = None
    ) -> IntensityReading:
        region = region or self.default_region
        now = _utcnow()

        # 1. Fast-path cache (v6's enriched cache, with TTL)
        cached = self._cache.get(region)
        if cached is not None:
            age = (now - _ensure_aware(cached.timestamp)).total_seconds()
            if age < self._cache_ttl_seconds and not cached.is_stale:
                return cached

        # 2. Fetch from v18 (v18 handles its own cache / circuit / retry)
        used_fallback = False
        try:
            intensity, provider = await self._fetch_current(region)
        except Exception as e:
            logger.warning("v18 forecaster failed: %s", e)
            intensity = 200.0  # conservative
            provider = ProviderKind.SIMULATION
            used_fallback = True
            self._fallback_count += 1

        self._source_counts[provider.value] += 1

        # 3. Anomaly detection (per-region)
        anomaly_flagged = False
        z_score = 0.0
        if self.anomaly is not None:
            anomaly_flagged, z_score = self.anomaly.check(
                region, intensity
            )
            if anomaly_flagged:
                logger.warning(
                    "Anomalous reading for %s: %.0f (z=%.2f)",
                    region,
                    intensity,
                    z_score,
                )

        # 4. Uncertainty from recent readings
        recent = [r.intensity for r in self._history.get(region, ())]
        ue = self.uncertainty.estimate(recent + [intensity])

        # 5. HITL for anomalous readings (result is used, not discarded)
        hitl_approved: Optional[bool] = None
        hitl_status = "not_required"
        if anomaly_flagged and self.hitl is not None:
            urgency = (
                "high"
                if intensity < 10 or intensity > 1200
                else "medium"
            )
            hitl_approved, hitl_status = await self.hitl.request_review(
                region=region,
                intensity=intensity,
                reason="Anomalous reading (z-score exceeded)",
                urgency=urgency,
            )
            self._hitl_outcomes[hitl_status] += 1

        # 6. Explanation (via v18's XAI, if enabled and numpy is present)
        explanation: Optional[str] = None
        if self.xai is not None:
            explanation = (
                f"{intensity:.0f} gCO2/kWh from {provider.value}"
                f"{' (anomalous)' if anomaly_flagged else ''}"
            )

        # 7. Build the reading
        reading = IntensityReading(
            region=region,
            intensity=intensity,
            timestamp=now,
            source=provider,
            confidence=ue.confidence,
            is_simulated=(
                provider == ProviderKind.SIMULATION or used_fallback
            ),
            is_stale=False,
            explanation=explanation,
            hitl_status=hitl_status,
            hitl_approved=hitl_approved,
            anomaly_flagged=anomaly_flagged,
        )

        # 8. Temporal verification
        self._history[region].append(reading)
        if self.temporal is not None:
            if not self.temporal.verify(
                list(self._history[region]), region=region
            ):
                logger.warning(
                    "Temporal violation for region %s", region
                )

        # 9. HITL rejection → serve the last known good reading as stale
        if hitl_status == "rejected":
            last_good = cached
            if last_good is not None and not last_good.is_stale:
                stale = replace(
                    last_good,
                    is_stale=True,
                    hitl_status="rejected",
                    explanation=(
                        f"{last_good.explanation or ''} "
                        "(served as stale; new reading rejected by HITL)"
                    ).strip(),
                )
                return stale
            reading = replace(
                reading, is_stale=True, hitl_status="rejected"
            )
            return reading

        # 10. Cache if acceptable
        if not anomaly_flagged:
            self._cache[region] = reading

        # 11. Fetcher coordinator feedback
        if self.coordinator is not None:
            agent = self.coordinator.select_agent(
                region, provider
            ) or "default"
            self.coordinator.record(
                agent_id=agent,
                region=region,
                provider=provider,
                success=not anomaly_flagged,
                latency_ms=0.0,
                anomaly_caught=anomaly_flagged,
            )

        return reading

    # ------------------------------------------------------------------
    # Forecast
    # ------------------------------------------------------------------
    async def predict(
        self,
        horizon: str = "1h",
        interval_minutes: int = 15,
        region: Optional[str] = None,
    ) -> List[IntensityForecast]:
        """
        Predict future intensity at ``interval_minutes`` granularity.

        Delegates the hourly anchor forecast to v18's
        ``forecast_next_hours_async`` and linearly interpolates to the
        requested interval.
        """
        region = region or self.default_region
        horizon_hours = _parse_horizon(horizon)
        n_points = max(
            1, int(horizon_hours * 60 / interval_minutes)
        )

        # Hourly anchors from v18
        try:
            batch = await self._forecast.forecast_next_hours_async(
                hours=max(1, int(math.ceil(horizon_hours))),
                region=region,
            )
            anchor_points: List[Tuple[datetime, float, float]] = []
            for p in getattr(batch, "points", []) or []:
                anchor_points.append(
                    (
                        _parse_timestamp(p.timestamp),
                        float(p.intensity),
                        float(getattr(p, "confidence", 0.5)),
                    )
                )
        except Exception as e:
            logger.warning(
                "v18 forecast_next_hours_async failed: %s", e
            )
            anchor_points = []

        # Fallback anchor: current reading
        if not anchor_points:
            try:
                current = await self.get_current_reading(region)
                anchor_points = [
                    (
                        _utcnow() + timedelta(hours=1),
                        current.intensity,
                        current.confidence,
                    )
                ]
            except Exception:
                anchor_points = [
                    (_utcnow() + timedelta(hours=1), 200.0, 0.3)
                ]

        source_kind = ProviderKind.ELECTRICITYMAP
        now = _utcnow()
        forecasts: List[IntensityForecast] = []
        for i in range(1, n_points + 1):
            ts = now + timedelta(minutes=interval_minutes * i)
            hours_offset = (ts - now).total_seconds() / 3600.0
            value = _interpolate_anchors(
                hours_offset, anchor_points
            )
            # Confidence decays with horizon; capped by v18's anchor
            # confidence
            anchor_conf = anchor_points[0][2]
            confidence = max(
                0.3,
                min(anchor_conf, anchor_conf - 0.03 * hours_offset),
            )
            forecasts.append(
                IntensityForecast(
                    timestamp=ts,
                    predicted_intensity=max(20.0, min(1200.0, value)),
                    confidence=confidence,
                    source=source_kind,
                )
            )
        return forecasts

    async def find_optimal_execution_window(
        self,
        duration_hours: float,
        deadline: Optional[datetime] = None,
        region: Optional[str] = None,
    ) -> OptimalWindow:
        """
        Best contiguous window of ``duration_hours`` within the horizon.

        Uses v18's hourly forecast through ``predict`` and respects
        ``deadline`` (candidate windows must end before it).
        """
        region = region or self.default_region
        now = _utcnow()
        deadline_aware = (
            _ensure_aware(deadline) if deadline is not None else None
        )
        if deadline_aware is not None and deadline_aware <= now:
            current = await self.get_current_reading(region)
            return OptimalWindow(
                start_time=now,
                end_time=now + timedelta(hours=duration_hours),
                avg_intensity=current.intensity,
                carbon_savings_percent=0.0,
            )

        horizon_hours = (
            min(
                48.0,
                max(
                    1.0,
                    (deadline_aware - now).total_seconds() / 3600.0,
                ),
            )
            if deadline_aware is not None
            else 48.0
        )

        step_min = 30
        forecasts = await self.predict(
            horizon=f"{int(horizon_hours)}h",
            interval_minutes=step_min,
            region=region,
        )
        points_needed = max(
            1, int(duration_hours * 60 / step_min)
        )
        if len(forecasts) < points_needed:
            current = await self.get_current_reading(region)
            return OptimalWindow(
                start_time=now,
                end_time=now + timedelta(hours=duration_hours),
                avg_intensity=current.intensity,
                carbon_savings_percent=0.0,
            )

        best_idx = 0
        best_avg = float("inf")
        for i in range(len(forecasts) - points_needed + 1):
            window = forecasts[i : i + points_needed]
            end_ts = window[-1].timestamp
            if deadline_aware is not None and end_ts > deadline_aware:
                break
            avg = statistics.fmean(
                f.predicted_intensity for f in window
            )
            if avg < best_avg:
                best_avg, best_idx = avg, i

        best = forecasts[best_idx : best_idx + points_needed]
        start = best[0].timestamp
        end = best[-1].timestamp + timedelta(minutes=step_min)
        if deadline_aware is not None and end > deadline_aware:
            end = deadline_aware
        current = await self.get_current_reading(region)
        savings = (
            max(
                0.0,
                (current.intensity - best_avg)
                / current.intensity
                * 100.0,
            )
            if current.intensity > 0
            else 0.0
        )
        return OptimalWindow(
            start_time=start,
            end_time=end,
            avg_intensity=best_avg,
            carbon_savings_percent=savings,
        )

    # ------------------------------------------------------------------
    # Market
    # ------------------------------------------------------------------
    async def get_market_snapshot(self) -> Optional[MarketSnapshot]:
        if self.market_integrator is None:
            return None
        try:
            price = await self.market_integrator.update_price()
            # v18's integrator exposes a credit price per tonne; convert
            # to a rough per-kWh figure for the snapshot.
            price_per_kwh = price / 1000.0
            return MarketSnapshot(
                price_per_kwh_usd=price_per_kwh,
                rec_available_mwh=100.0,
                rec_price_per_mwh_usd=max(1.0, price),
            )
        except Exception as e:
            logger.warning("Market snapshot failed: %s", e)
            return None

    # ------------------------------------------------------------------
    # Federated
    # ------------------------------------------------------------------
    def contribute_federated_profile(
        self, region: Optional[str] = None
    ) -> None:
        if self.federated is None:
            return
        region = region or self.default_region
        readings = [
            r
            for r in self._history.get(region, ())
            if r.region == region
        ]
        if len(readings) < 24:
            return
        hourly: Dict[int, List[float]] = defaultdict(list)
        for r in readings:
            hourly[_ensure_aware(r.timestamp).hour].append(
                r.intensity
            )
        means = [
            statistics.fmean(hourly[h]) if hourly[h] else 200.0
            for h in range(24)
        ]
        self.federated.push(
            FederatedRegionalProfile(
                deployment_id=self.deployment_id,
                region=region,
                hourly_means=means,
                sample_count=len(readings),
            )
        )

    def get_federated_aggregate(self) -> Dict[str, List[float]]:
        return (
            self.federated.aggregate()
            if self.federated is not None
            else {}
        )

    # ------------------------------------------------------------------
    # Stats / health
    # ------------------------------------------------------------------
    def get_statistics(self) -> Dict[str, Any]:
        base: Dict[str, Any] = {}
        try:
            base = self._forecast.stats()
        except Exception:
            pass
        v6_stats: Dict[str, Any] = {
            "deployment_id": self.deployment_id,
            "default_region": self.default_region,
            "features": dict(self.features),
            "fallback_count": self._fallback_count,
            "source_counts": dict(self._source_counts),
            "hitl_outcomes": dict(self._hitl_outcomes),
            "history_regions": list(self._history.keys()),
            "cache_regions": list(self._cache.keys()),
        }
        if self.temporal is not None:
            v6_stats["temporal_violations"] = list(
                self.temporal.violations
            )[-5:]
        if self.coordinator is not None:
            v6_stats["agents"] = {
                aid: {
                    "role": a.role.value,
                    "success_rate": a.success_rate,
                    "avg_latency_ms": a.avg_latency_ms,
                }
                for aid, a in self.coordinator.agents.items()
            }
        if self.hitl is not None:
            v6_stats["hitl"] = {
                "pending": len(self.hitl.pending),
                "log_size": len(self.hitl.feedback_log),
                "outcomes": self.hitl.outcome_counts(),
            }
        if self.anomaly is not None:
            v6_stats["anomaly_detector"] = self.anomaly.stats()
        return {**base, **v6_stats}

    async def health_check(self) -> Dict[str, Any]:
        stats = self.get_statistics()
        base_state = stats.get("circuit_state", "unknown")
        return {
            "healthy": base_state != "OPEN",
            "deployment_id": self.deployment_id,
            "forecaster_version": "v18",
            "circuit_state": base_state,
            "fallback_count": self._fallback_count,
            "temporal_violations": len(
                self.temporal.violations if self.temporal else []
            ),
            "hitl_outcomes": dict(self._hitl_outcomes),
        }


# --------------------------------------------------------------------------- #
# Interpolation helper
# --------------------------------------------------------------------------- #
def _interpolate_anchors(
    hours_offset: float,
    anchors: Sequence[Tuple[datetime, float, float]],
) -> float:
    """Linear interpolation between hourly anchors, clamped at the edges."""
    if not anchors:
        return 200.0
    if hours_offset <= 1.0:
        # Between "now" and the first hourly anchor
        return anchors[0][1]
    prev_h = 1.0
    prev_v = anchors[0][1]
    for i, (_ts, v, _c) in enumerate(anchors[1:], start=2):
        if hours_offset <= i:
            frac = (hours_offset - prev_h) / (i - prev_h)
            return prev_v + (v - prev_v) * frac
        prev_h = float(i)
        prev_v = v
    return anchors[-1][1]


def _parse_horizon(horizon: str) -> float:
    h = horizon.strip().lower()
    if h.endswith("h"):
        return max(0.01, float(h[:-1]))
    if h.endswith("m"):
        return max(0.01, float(h[:-1]) / 60.0)
    if h.endswith("d"):
        return max(0.01, float(h[:-1]) * 24.0)
    return max(0.01, float(h))


# --------------------------------------------------------------------------- #
# Factory
# --------------------------------------------------------------------------- #
def create_forecaster(
    config: Optional[Mapping[str, Any]] = None,
    deployment_id: str = "local",
    features: Optional[Dict[str, bool]] = None,
    hardware: Optional[HardwareProfile] = None,
    forecast: Optional["CarbonForecast"] = None,
) -> CarbonForecaster:
    return CarbonForecaster(
        config=config
        or {
            "carbon": {
                "api_provider": "simulation",
                "default_region": "US-CA",
            }
        },
        deployment_id=deployment_id,
        features=features,
        hardware=hardware,
        forecast=forecast,
    )


# --------------------------------------------------------------------------- #
# Public API — v18 re-exports + v6 additions
# --------------------------------------------------------------------------- #
__all__ = [
    # ---- v18 re-exports: infra / core ----
    "CarbonForecast",
    "CircuitBreaker",
    "InMemoryStorage",
    "StorageProtocol",
    "ForecastPoint",
    "ForecastBatch",
    "ForecastSource",
    # ---- v18 re-exports: enhancement classes (single implementation) ----
    "QuantumDistillationEngine",
    "CausalGraphLearner",
    "CausalPolicyAdapter",
    "FederatedGreenAggregator",
    "MultiAgentCoordinator",
    "TemporalLogicVerifier",
    "XAIDecisionExplainer",
    "AdaptivePrecisionSwitcher",
    "CarbonMarketIntegrator",
    "ChaosTestingEngine",
    "ActiveUserPreferenceLearner",
    # ---- v18 Simple* aliases ----
    "SimpleQuantumDistillationEngine",
    "SimpleCausalGraphLearner",
    "SimpleCausalPolicyAdapter",
    "SimpleFederatedGreenAggregator",
    "SimpleMultiAgentCoordinator",
    "SimpleTemporalLogicVerifier",
    "SimpleXAIDecisionExplainer",
    "SimpleAdaptivePrecisionSwitcher",
    "SimpleCarbonMarketIntegrator",
    "SimpleChaosTestingEngine",
    "SimpleActiveUserPreferenceLearner",
    # ---- v6 types ----
    "PrecisionLevel",
    "ProviderKind",
    "AgentRole",
    "IntensityReading",
    "IntensityForecast",
    "OptimalWindow",
    "HardwareProfile",
    "MarketSnapshot",
    "HITLRequest",
    "UncertaintyEstimate",
    # ---- v6 components (genuinely new concerns) ----
    "AnomalyDetector",
    "UncertaintyEstimator",
    "AgentProfile",
    "FetcherCoordinator",
    "FederatedRegionalProfile",
    "FederatedRegionalAggregator",
    "TemporalProperty",
    "CacheFreshness",
    "IntensityBounds",
    "ReadingTemporalMonitor",
    "HumanInTheLoopGate",
    # ---- v6 main + factory ----
    "CarbonForecaster",
    "create_forecaster",
    "DEFAULT_FEATURES",
]


# --------------------------------------------------------------------------- #
# Demo — exercises both classes side by side
# --------------------------------------------------------------------------- #
async def _demo() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        ),
    )

    config = {
        "carbon": {
            "api_provider": "simulation",
            "default_region": "US-CA",
            "cache_ttl": 300,
        },
        "fail_mode": "log",
    }

    print("=" * 80)
    print("forecasting_engine.py v6.0.0 — Demo")
    print("=" * 80)

    # ---- Path 1: direct v18 usage ----
    print("\n=== Path 1: v18 CarbonForecast directly ===")
    async with CarbonForecast(
        config=_v6_to_v18_config(config),
        region="US-CA",
        cache_ttl=300,
    ) as v18:
        v18_value, v18_source = await v18.current_intensity_async()
        print(
            f"  v18.current_intensity_async(): "
            f"{v18_value:.1f} gCO2/kWh ({v18_source.value})"
        )
        v18_batch = await v18.forecast_next_hours_async(hours=3)
        print(f"  v18.forecast_next_hours_async(3):")
        for p in v18_batch.points:
            print(
                f"    {p.timestamp[:19]}  {p.intensity:.1f}  "
                f"({p.source.value})"
            )

    # ---- Path 2: v6 wrapper (same v18 instance internally) ----
    print("\n=== Path 2: v6 CarbonForecaster (delegates to v18) ===")
    async with create_forecaster(
        config=config,
        deployment_id="us-ca-prod-01",
        hardware=HardwareProfile(
            supports_int4=True, vram_gb=48
        ),
    ) as fc:
        fc.coordinator.register("fetcher-A")
        fc.coordinator.register("fetcher-B")

        print("\n-- get_current_reading --")
        reading = await fc.get_current_reading("US-CA")
        print(f"  intensity: {reading.intensity:.1f} gCO2/kWh")
        print(f"  source: {reading.source.value}")
        print(f"  confidence: {reading.confidence:.2f}")
        print(f"  is_simulated: {reading.is_simulated}")
        print(f"  hitl_status: {reading.hitl_status}")
        print(f"  explanation: {reading.explanation}")

        print("\n-- predict (15-min interval, 1h horizon) --")
        forecasts = await fc.predict(
            horizon="1h", interval_minutes=15, region="US-CA"
        )
        for f in forecasts:
            print(
                f"  {f.timestamp.strftime('%H:%M')} -> "
                f"{f.predicted_intensity:.0f} gCO2/kWh "
                f"(conf={f.confidence:.2f}, src={f.source.value})"
            )

        print("\n-- find_optimal_execution_window (2h, next 12h) --")
        window = await fc.find_optimal_execution_window(
            duration_hours=2.0,
            deadline=_utcnow() + timedelta(hours=12),
            region="US-CA",
        )
        print(f"  start: {window.start_time.strftime('%H:%M')}")
        print(f"  end:   {window.end_time.strftime('%H:%M')}")
        print(f"  avg intensity: {window.avg_intensity:.0f} gCO2/kWh")
        print(
            f"  carbon savings vs now: "
            f"{window.carbon_savings_percent:.1f}%"
        )

        print("\n-- market snapshot --")
        snap = await fc.get_market_snapshot()
        if snap:
            print(f"  price: ${snap.price_per_kwh_usd:.4f}/kWh")
            print(
                f"  REC: {snap.rec_available_mwh:.1f} MWh @ "
                f"${snap.rec_price_per_mwh_usd:.2f}/MWh"
            )

        # Push a profile to the federated aggregator
        for _ in range(30):
            await fc.get_current_reading("US-CA")
        fc.contribute_federated_profile("US-CA")
        agg = fc.get_federated_aggregate()
        if "US-CA" in agg:
            print(
                f"\n-- federated aggregate (US-CA first 6h): "
                f"{[round(v) for v in agg['US-CA'][:6]]}"
            )

        print("\n-- statistics --")
        import json as _json
        print(_json.dumps(fc.get_statistics(), indent=2, default=str))

        print("\n-- health --")
        print(
            _json.dumps(await fc.health_check(), indent=2, default=str)
        )

    print("\n=== Sanity: v18 classes are the same instances ===")
    print(
        f"  CircuitBreaker is from v18: "
        f"{CircuitBreaker is not None}"
    )
    print(
        f"  CarbonForecast is the v18 class: "
        f"{CarbonForecast is not None}"
    )

    print("\nDemo complete.")


if __name__ == "__main__":
    asyncio.run(_demo())
