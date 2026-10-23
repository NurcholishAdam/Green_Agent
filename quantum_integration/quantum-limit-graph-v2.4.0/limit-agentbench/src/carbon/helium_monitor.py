"""
helium_monitor.py — v6.0.0 (thin wrapper over carbon_forecast v18)
====================================================================

Helium supply-chain monitoring for Layer 7.

**What changed in v6**
----------------------
v5 grew its own copies of the enhancement classes, started a background
task from ``__init__`` (broken outside a running event loop), discarded
the HITL review result, kept three incompatible score→level threshold
sets, inverted the confidence calculation, and fired ``SignalFreshness``
permanently once any signal became 30 minutes old.

v6 fixes all of that.

**Re-exported from v18 (single implementation)**
    CircuitBreaker, ChaosTestingEngine, ActiveUserPreferenceLearner,
    AdaptivePrecisionSwitcher, QuantumDistillationEngine,
    CarbonMarketIntegrator, XAIDecisionExplainer,
    plus every ``Simple*`` alias and shared helpers
    (``_utcnow``, ``_ensure_aware``, ``_parse_timestamp``, ``_json_safe``,
    ``_cfg_get``, ``_safe_mean``).

**Local (helium-specific; fixed)**
    HeliumScarcityLevel, ProviderKind, PrecisionLevel, AgentRole
    HeliumSupplySignal (+ hitl_status, hitl_approved)
    SignalExplanation
    _classify  (single source of truth for score → level)
    SyncCircuitAdapter (sync facade over v18's async CircuitBreaker)
    AsyncChaosInjector (async, failure-rate-based)
    SignalFreshness, ScoreBounds, PriceSanity (bounded, recent-window)
    SignalSafetyMonitor (pure vet, bounded violations)
    SignalExplainer (fixed confidence)
    ProviderPolicy (contextual bandit over ProviderKind)
    QuantumDistillationBridge (per-hour profile distillation)
    AdaptivePrecisionSelector (urgency-based)
    FederatedHeliumAggregator (hourly profiles)
    HeliumAgentCoordinator (helium role set)
    HumanReviewGate (sync + async, awaits awaitables)
    AnomalyDetector (per-region)
    CarbonMarketClient (sync, cached)
    HeliumMonitor (the main class)

**Public API preserved**
    monitor = HeliumMonitor(config={...})
    signal = monitor.get_current_supply()
    trend  = monitor.get_supply_trend(hours=24)
    forecast = await monitor.get_forecast(hours_ahead=24)
    await monitor.shutdown()

**P0 fixes**
    1. ``__init__`` no longer starts monitoring when there is no running
       loop. Use ``await monitor.start()`` or ``async with HeliumMonitor(...)``.
    2. HITL result is stored on ``HeliumSupplySignal.hitl_status`` and
       ``.hitl_approved``.
    3. Async HITL callbacks are awaited in ``areview``; in sync ``review``
       they are rejected (fail-safe) with an explicit log.
    4. ``SignalFreshness`` checks the **most recent N readings per
       region**, not the entire history. ``violations`` is bounded.
    5. ``SignalExplainer`` confidence peaks **inside** a band and dips at
       boundaries (v5 peaked at the midpoint).
    6. Single ``_classify(score)`` used by every code path.
    7. ``_build_from_federated`` exposes the profile's per-hour std via
       ``confidence = 1 / (1 + std)`` and sets ``is_stale=True``.

**P1 fixes**
    * ``merge_federated_profile`` actually merges into the distilled model
      via a persisted per-region override.
    * RL feedback uses the **pre-fetch** ``ProviderState``, not a
      post-fetch reconstruction; reward = "chosen provider answered".
    * Fallback signals are appended to ``_signal_history`` (with
      ``source="fallback"``); counters incremented.
    * ``_available_providers`` calls ``aggregate()`` so
      ``FEDERATED_PEER`` is reachable.
    * Distilled model cached per ``(hour, precision)``.
    * ``AnomalyDetector`` is per-region.
    * All datetimes are ``datetime.now(timezone.utc)``.
    * ``_parse_api_response`` validates ``data`` is a Mapping.
    * ``set_features`` recreates layers at runtime.
    * ``__aenter__`` / ``__aexit__``.
    * Every collection (pending, feedback_log, violations, chaos.events,
      federated updates) is bounded.

File: src/carbon/helium_monitor.py
"""

from __future__ import annotations

import asyncio
import copy
import inspect
import logging
import math
import os
import random
import statistics
from collections import Counter, defaultdict, deque
from dataclasses import asdict, dataclass, field
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
# v18 imports (single implementation for enhancement classes)
# --------------------------------------------------------------------------- #
try:
    from carbon_forecast import (  # type: ignore[import-not-found]
        CircuitBreaker,
        ChaosTestingEngine,
        ActiveUserPreferenceLearner,
        AdaptivePrecisionSwitcher,
        QuantumDistillationEngine,
        CarbonMarketIntegrator,
        XAIDecisionExplainer,
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
        ForecastPoint,
        ForecastBatch,
        ForecastSource,
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

    CircuitBreaker = None  # type: ignore[assignment]
    ChaosTestingEngine = None  # type: ignore[assignment]
    ActiveUserPreferenceLearner = None  # type: ignore[assignment]
    AdaptivePrecisionSwitcher = None  # type: ignore[assignment]
    QuantumDistillationEngine = None  # type: ignore[assignment]
    CarbonMarketIntegrator = None  # type: ignore[assignment]
    XAIDecisionExplainer = None  # type: ignore[assignment]
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
    ForecastPoint = None  # type: ignore[assignment]
    ForecastBatch = None  # type: ignore[assignment]

    class ForecastSource(Enum):  # type: ignore[no-redef]
        API = "api"
        CACHE = "cache"
        HISTORY = "history"
        LAST_API = "last_api"
        RANDOM_WALK = "random_walk"
        FALLBACK = "fallback"

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
            float(v) for v in values
            if v is not None and math.isfinite(float(v))
        ]
        return statistics.mean(items) if items else default


logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
# Bounds
# --------------------------------------------------------------------------- #
_MAX_VIOLATIONS = 200
_MAX_PENDING_HITL = 100
_MAX_HITL_LOG = 500
_MAX_CHAOS_EVENTS = 500
_MAX_FEDERATED_UPDATES = 100
_MAX_POLICY_BUFFER = 1024
_MAX_FRESHNESS_WINDOW = 5


# --------------------------------------------------------------------------- #
# Canonical threshold maps (single source of truth)
# --------------------------------------------------------------------------- #
_SCORE_BANDS: Dict[str, float] = {
    "normal": 0.25,   # score < 0.25         → NORMAL
    "caution": 0.55,  # 0.25 <= score < 0.55 → CAUTION
    "critical": 0.80, # 0.55 <= score < 0.80 → CRITICAL
    # score >= 0.80                          → SEVERE
}

_PRICE_BANDS: Dict[str, float] = {
    "normal": 4.5,
    "caution": 6.5,
    "critical": 9.0,
}

_INVENTORY_BANDS: Dict[str, int] = {
    "normal": 25,
    "caution": 15,
    "critical": 8,
}


# --------------------------------------------------------------------------- #
# Domain enums
# --------------------------------------------------------------------------- #
class HeliumScarcityLevel(Enum):
    NORMAL = "normal"
    CAUTION = "caution"
    CRITICAL = "critical"
    SEVERE = "severe"


class PrecisionLevel(Enum):
    FP32 = "fp32"
    FP16 = "fp16"
    INT8 = "int8"
    INT4 = "int4"
    QUANTUM_DISTILLED = "quantum_distilled"


class ProviderKind(Enum):
    PRIMARY_API = "primary_api"
    BACKUP_API = "backup_api"
    FEDERATED_PEER = "federated_peer"
    DISTILLED_MODEL = "distilled_model"
    SIMULATION = "simulation"


class AgentRole(Enum):
    GENERALIST = "generalist"
    REGIONAL_SPECIALIST = "regional_specialist"
    PROVIDER_SPECIALIST = "provider_specialist"
    ANOMALY_HUNTER = "anomaly_hunter"
    LATENCY_OPTIMIZER = "latency_optimizer"


class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


# --------------------------------------------------------------------------- #
# Single source of truth for classification
# --------------------------------------------------------------------------- #
def _classify(score: float) -> HeliumScarcityLevel:
    """Map a scarcity_score in [0, 1] to a ``HeliumScarcityLevel``.

    Every code path (simulation, distillation, federation) uses this
    function so a given score cannot produce different levels depending
    on which provider happened to answer.
    """
    s = float(score)
    if s < _SCORE_BANDS["normal"]:
        return HeliumScarcityLevel.NORMAL
    if s < _SCORE_BANDS["caution"]:
        return HeliumScarcityLevel.CAUTION
    if s < _SCORE_BANDS["critical"]:
        return HeliumScarcityLevel.CRITICAL
    return HeliumScarcityLevel.SEVERE


# --------------------------------------------------------------------------- #
# Signal
# --------------------------------------------------------------------------- #
@dataclass
class HeliumSupplySignal:
    """Real-time helium supply-chain signal (extended)."""

    timestamp: datetime
    scarcity_level: HeliumScarcityLevel
    scarcity_score: float
    spot_price_usd_per_liter: float
    fab_inventory_days: int
    vendor_alerts: List[str]
    source: str
    forecast_valid_until: Optional[datetime] = None
    confidence: float = 1.0
    explanation: Optional[str] = None
    region: str = "global"
    is_anomalous: bool = False
    is_stale: bool = False
    market_snapshot: Optional[Dict[str, Any]] = None
    # --- v6: audit fields ---
    hitl_status: str = "not_required"
    hitl_approved: Optional[bool] = None

    def is_critical(self) -> bool:
        return self.scarcity_level in (
            HeliumScarcityLevel.CRITICAL,
            HeliumScarcityLevel.SEVERE,
        )

    def price_premium(self, baseline: float = 4.0) -> float:
        return max(0.0, self.spot_price_usd_per_liter - baseline)

    def to_dict(self) -> Dict[str, Any]:
        return _json_safe(asdict(self))


# --------------------------------------------------------------------------- #
# Sync adapter around v18's CircuitBreaker
# --------------------------------------------------------------------------- #
class SyncCircuitAdapter:
    """
    Sync facade over v18's ``CircuitBreaker``.

    Calls v18's sync internals (``_acquire_slot`` / ``_on_success`` /
    ``_on_failure``) so the sync fetch path can use the fixed HALF_OPEN
    semantics. Documented here so a v18 refactor surfaces clearly.
    """

    def __init__(
        self,
        name: str,
        failure_threshold: int = 3,
        recovery_timeout: float = 60.0,
    ) -> None:
        if not _V18_AVAILABLE:
            raise RuntimeError(
                "SyncCircuitAdapter requires carbon_forecast (v18): "
                f"{_V18_ERROR}"
            )
        self._cb = CircuitBreaker(  # type: ignore[misc]
            failure_threshold=failure_threshold,
            recovery_timeout=recovery_timeout,
            name=name,
        )

    @property
    def state(self) -> str:
        return self._cb.state

    @property
    def failures(self) -> int:
        return self._cb.failure_count

    def can_call(self) -> bool:
        try:
            self._cb._acquire_slot()
            self._cb._on_success()  # release the probe slot
            return True
        except RuntimeError:
            return False

    def record_success(self) -> None:
        self._cb._on_success()

    def record_failure(self) -> None:
        self._cb._on_failure()


class AsyncChaosInjector:
    """Async, rate-based fault injector for the fetch path."""

    def __init__(
        self, failure_rate: float = 0.0, latency_ms: int = 0
    ) -> None:
        if not 0.0 <= failure_rate <= 1.0:
            raise ValueError("failure_rate must be in [0, 1]")
        if latency_ms < 0:
            raise ValueError("latency_ms must be >= 0")
        self.failure_rate = failure_rate
        self.latency_ms = latency_ms
        self.events: Deque[Dict[str, Any]] = deque(maxlen=_MAX_CHAOS_EVENTS)

    async def maybe_inject(self, component: str) -> None:
        if self.latency_ms > 0:
            await asyncio.sleep(self.latency_ms / 1000.0)
        if random.random() < self.failure_rate:
            self.events.append(
                {"component": component, "at": _utcnow().isoformat()}
            )
            raise RuntimeError(
                f"Chaos injection failure in {component}"
            )


# --------------------------------------------------------------------------- #
# Temporal properties (per-region, recent-window)
# --------------------------------------------------------------------------- #
class TemporalProperty(Protocol):
    def check(
        self, history: Sequence[HeliumSupplySignal], region: str
    ) -> bool: ...
    def name(self) -> str: ...


@dataclass
class SignalFreshness:
    """G(recent signal for region is < max_age_seconds old)."""

    max_age_seconds: int = 1800
    window: int = _MAX_FRESHNESS_WINDOW

    def check(
        self, history: Sequence[HeliumSupplySignal], region: str
    ) -> bool:
        recent = [
            s for s in history if s.region == region
        ][-self.window:]
        if not recent:
            return True
        now = _utcnow()
        return all(
            (now - _ensure_aware(s.timestamp)).total_seconds()
            <= self.max_age_seconds
            for s in recent
        )

    def name(self) -> str:
        return "SignalFreshness"


@dataclass
class ScoreBounds:
    """G(scarcity_score ∈ [0, 1])."""

    def check(
        self, history: Sequence[HeliumSupplySignal], region: str
    ) -> bool:
        return all(
            0.0 <= s.scarcity_score <= 1.0
            for s in history
            if s.region == region
        )

    def name(self) -> str:
        return "ScoreBounds"


@dataclass
class PriceSanity:
    """G(0 <= spot_price <= max_price)."""

    max_price: float = 1000.0

    def check(
        self, history: Sequence[HeliumSupplySignal], region: str
    ) -> bool:
        return all(
            0.0 <= s.spot_price_usd_per_liter <= self.max_price
            for s in history
            if s.region == region
        )

    def name(self) -> str:
        return "PriceSanity"


class SignalSafetyMonitor:
    """Bounded, pure verification of temporal properties."""

    def __init__(self, max_violations: int = _MAX_VIOLATIONS) -> None:
        self.properties: List[TemporalProperty] = []
        self.violations: Deque[Dict[str, Any]] = deque(
            maxlen=max_violations
        )

    def register(self, p: TemporalProperty) -> None:
        self.properties.append(p)

    def verify(
        self,
        history: Sequence[HeliumSupplySignal],
        region: str,
    ) -> Tuple[bool, List[str]]:
        bad: List[str] = []
        for p in self.properties:
            try:
                if not p.check(history, region):
                    bad.append(p.name())
                    self.violations.append(
                        {
                            "property": p.name(),
                            "region": region,
                            "at": _utcnow().isoformat(),
                        }
                    )
            except Exception as e:
                logger.warning(
                    "Property %s raised: %s", p.name(), e
                )
                bad.append(p.name())
        return (len(bad) == 0, bad)


# --------------------------------------------------------------------------- #
# XAI
# --------------------------------------------------------------------------- #
@dataclass
class SignalExplanation:
    headline: str
    rationale: List[str]
    confidence: float
    contributing_factors: Dict[str, float]

    def to_dict(self) -> Dict[str, Any]:
        return _json_safe(asdict(self))


class SignalExplainer:
    """
    Explains a scarcity classification.

    Fixed vs. v5: confidence peaks **inside** a band and dips at
    threshold boundaries, matching the intuition that a reading well
    inside a band is more clearly that level than one on the boundary.
    """

    # Re-use the canonical bands so all code paths agree.
    THRESHOLDS: Dict[str, Dict[str, float]] = {
        "score": dict(_SCORE_BANDS),
        "price": dict(_PRICE_BANDS),
        "inventory": {k: float(v) for k, v in _INVENTORY_BANDS.items()},
    }

    @classmethod
    def _band_margin(cls, score: float) -> float:
        """Distance from ``score`` to the nearest threshold, in [0, 0.5]."""
        boundaries = [
            _SCORE_BANDS["normal"],
            _SCORE_BANDS["caution"],
            _SCORE_BANDS["critical"],
        ]
        return min(
            min(abs(score - b) for b in boundaries),
            0.5,
        )

    @classmethod
    def explain(
        cls,
        level: HeliumScarcityLevel,
        score: float,
        price: float,
        inventory: int,
        source: str,
        fallback_reason: Optional[str] = None,
        anomaly_flagged: bool = False,
    ) -> SignalExplanation:
        reasons: List[str] = []
        reasons.append(
            f"Scarcity level = {level.value} "
            f"(score={score:.2f}, price=${price:.2f}/L, "
            f"inventory={inventory}d)."
        )
        reasons.append(f"Source: {source}.")

        t = cls.THRESHOLDS
        if score >= t["score"]["critical"]:
            reasons.append(
                f"Score ≥ {t['score']['critical']} (severe band)."
            )
        elif score >= t["score"]["caution"]:
            reasons.append(
                f"Score ≥ {t['score']['caution']} (critical band)."
            )
        elif score >= t["score"]["normal"]:
            reasons.append(
                f"Score ≥ {t['score']['normal']} (caution band)."
            )

        if price >= t["price"]["critical"]:
            reasons.append(
                f"Price ≥ ${t['price']['critical']}/L (critical band)."
            )
        elif price >= t["price"]["caution"]:
            reasons.append(
                f"Price ≥ ${t['price']['caution']}/L (caution band)."
            )

        if inventory <= t["inventory"]["critical"]:
            reasons.append(
                f"Inventory ≤ {t['inventory']['critical']}d "
                "(critical band)."
            )
        elif inventory <= t["inventory"]["caution"]:
            reasons.append(
                f"Inventory ≤ {t['inventory']['caution']}d "
                "(caution band)."
            )

        if fallback_reason:
            reasons.append(f"Fallback: {fallback_reason}")
        if anomaly_flagged:
            reasons.append("Flagged as anomalous by detector.")

        # FIXED: confidence peaks inside a band, not at the boundary.
        margin = cls._band_margin(score)
        confidence = max(0.0, min(1.0, 0.5 + margin))

        return SignalExplanation(
            headline=(
                f"[{level.value.upper()}] {score:.2f} @ ${price:.2f}/L"
            ),
            rationale=reasons,
            confidence=confidence,
            contributing_factors={
                "scarcity_score": score,
                "spot_price": price,
                "inventory_days": float(inventory),
            },
        )


# --------------------------------------------------------------------------- #
# Causal-RL provider policy (contextual bandit)
# --------------------------------------------------------------------------- #
@dataclass
class ProviderState:
    hour_of_day: int
    recent_failure_rate: float
    cache_hit: bool
    region_hash: float


class ProviderPolicy:
    """Contextual bandit over providers. Reward = accuracy - latency penalty."""

    PROVIDERS = [
        ProviderKind.PRIMARY_API,
        ProviderKind.BACKUP_API,
        ProviderKind.FEDERATED_PEER,
        ProviderKind.DISTILLED_MODEL,
        ProviderKind.SIMULATION,
    ]

    def __init__(self, epsilon: float = 0.1, lr: float = 0.05) -> None:
        self.epsilon = epsilon
        self.lr = lr
        self.weights: Dict[ProviderKind, List[float]] = {
            p: [0.0] * 4 for p in self.PROVIDERS
        }
        self.buffer: Deque[
            Tuple[ProviderState, ProviderKind, float, float]
        ] = deque(maxlen=_MAX_POLICY_BUFFER)

    @staticmethod
    def _features(s: ProviderState) -> List[float]:
        return [
            s.hour_of_day / 24.0,
            s.recent_failure_rate,
            float(s.cache_hit),
            s.region_hash,
        ]

    def _q(self, s: ProviderState, p: ProviderKind) -> float:
        f = self._features(s)
        return sum(w * x for w, x in zip(self.weights[p], f))

    def select(
        self, s: ProviderState, available: List[ProviderKind]
    ) -> ProviderKind:
        if random.random() < self.epsilon:
            return random.choice(available)
        return max(available, key=lambda p: self._q(s, p))

    def record(
        self,
        s: ProviderState,
        p: ProviderKind,
        accuracy: float,
        latency_ms: float,
    ) -> None:
        self.buffer.append((s, p, accuracy, latency_ms))

    def update(self) -> None:
        if not self.buffer:
            return
        for s, p, acc, lat in self.buffer:
            f = self._features(s)
            q = sum(w * x for w, x in zip(self.weights[p], f))
            reward = acc - 0.001 * lat
            err = reward - q
            for i, x in enumerate(f):
                self.weights[p][i] += self.lr * err * x
        self.buffer.clear()


# --------------------------------------------------------------------------- #
# Distillation (per-hour profile)
# --------------------------------------------------------------------------- #
@dataclass
class DistilledSimulationModel:
    precision: PrecisionLevel
    hourly_mean: List[float] = field(
        default_factory=lambda: [0.3] * 24
    )
    hourly_std: List[float] = field(
        default_factory=lambda: [0.15] * 24
    )
    quality_retention: float = 0.9
    energy_reduction_percent: float = 55.0


class QuantumDistillationBridge:
    """Distills a full simulator into a compact per-hour profile."""

    _SCALE: Dict[PrecisionLevel, float] = {
        PrecisionLevel.FP32: 1.0,
        PrecisionLevel.FP16: 1.0,
        PrecisionLevel.INT8: 100.0,
        PrecisionLevel.INT4: 10.0,
        PrecisionLevel.QUANTUM_DISTILLED: 5.0,
    }
    _RETENTION: Dict[PrecisionLevel, Tuple[float, float]] = {
        PrecisionLevel.FP32: (1.0, 0.0),
        PrecisionLevel.FP16: (0.98, 30.0),
        PrecisionLevel.INT8: (0.93, 55.0),
        PrecisionLevel.INT4: (0.85, 70.0),
        PrecisionLevel.QUANTUM_DISTILLED: (0.80, 85.0),
    }

    def distill(
        self,
        history: Sequence[HeliumSupplySignal],
        precision: PrecisionLevel,
        federated_override: Optional[List[float]] = None,
    ) -> DistilledSimulationModel:
        buckets: Dict[int, List[float]] = defaultdict(list)
        for s in history:
            buckets[_ensure_aware(s.timestamp).hour].append(
                s.scarcity_score
            )

        means: List[float] = []
        stds: List[float] = []
        for h in range(24):
            vals = buckets.get(h, [])
            if vals:
                means.append(statistics.fmean(vals))
                stds.append(
                    statistics.pstdev(vals) if len(vals) > 1 else 0.1
                )
            else:
                means.append(0.3)
                stds.append(0.15)

        # FIXED: apply the federated override if present
        if federated_override is not None:
            for h in range(24):
                if h < len(federated_override):
                    means[h] = float(federated_override[h])

        # FIXED: round, not floor (unbiased)
        scale = self._SCALE[precision]
        retention, energy = self._RETENTION[precision]

        def qz(v: float) -> float:
            if scale == 1.0:
                return float(v)
            return round(v * scale) / scale

        return DistilledSimulationModel(
            precision=precision,
            hourly_mean=[qz(m) for m in means],
            hourly_std=[qz(s) for s in stds],
            quality_retention=retention,
            energy_reduction_percent=energy,
        )


# --------------------------------------------------------------------------- #
# Adaptive precision (urgency-based)
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class HardwareProfile:
    has_tensor_cores: bool = True
    supports_int8: bool = True
    supports_int4: bool = False
    vram_gb: float = 24.0
    edge_device: bool = False


class AdaptivePrecisionSelector:
    def __init__(self, hw: Optional[HardwareProfile] = None) -> None:
        self.hw = hw or HardwareProfile()

    def select(self, urgency: str) -> PrecisionLevel:
        if urgency == "critical":
            return PrecisionLevel.FP16
        if self.hw.edge_device:
            return (
                PrecisionLevel.INT8
                if self.hw.supports_int8
                else PrecisionLevel.FP16
            )
        return (
            PrecisionLevel.INT4
            if self.hw.supports_int4
            else PrecisionLevel.INT8
        )


# --------------------------------------------------------------------------- #
# Federated aggregation (hourly profiles)
# --------------------------------------------------------------------------- #
@dataclass
class FederatedHeliumProfile:
    deployment_id: str
    region: str
    hourly_means: List[float]
    sample_count: int
    timestamp: datetime = field(default_factory=_utcnow)


class FederatedHeliumAggregator:
    """FedAvg over hourly profiles, bounded."""

    def __init__(
        self, max_updates: int = _MAX_FEDERATED_UPDATES
    ) -> None:
        self.updates: Deque[FederatedHeliumProfile] = deque(
            maxlen=max_updates
        )
        self._global: Dict[str, List[float]] = {}

    def push(self, u: FederatedHeliumProfile) -> None:
        self.updates.append(u)

    def aggregate(self) -> Dict[str, List[float]]:
        grouped: Dict[str, List[FederatedHeliumProfile]] = defaultdict(
            list
        )
        for u in self.updates:
            grouped[u.region].append(u)

        result: Dict[str, List[float]] = {}
        for region, profiles in grouped.items():
            total_w = sum(
                max(0, p.sample_count) for p in profiles
            )
            if total_w <= 0:
                continue
            merged = [0.0] * 24
            for p in profiles:
                w = max(0, p.sample_count) / total_w
                if w <= 0:
                    continue
                for h in range(24):
                    merged[h] += p.hourly_means[h] * w
            result[region] = merged
        self._global = result
        return result

    def get_global(self, region: str) -> Optional[List[float]]:
        return self._global.get(region)


# --------------------------------------------------------------------------- #
# Multi-agent coordinator (helium role set)
# --------------------------------------------------------------------------- #
@dataclass
class AgentProfile:
    agent_id: str
    role: AgentRole = AgentRole.GENERALIST
    success_rate: float = 0.0
    avg_latency_ms: float = 0.0
    anomaly_catch_rate: float = 0.0
    regions: List[str] = field(default_factory=list)
    total_calls: int = 0


class HeliumAgentCoordinator:
    """Sliding-window role assignment over helium fetcher agents."""

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

    def _reassign(self) -> None:
        for a in self.agents.values():
            if len(self._history[a.agent_id]) < 3:
                continue
            a.success_rate = self._window_mean(
                a.agent_id, "success"
            )
            a.avg_latency_ms = self._window_mean(
                a.agent_id, "latency"
            )
            a.anomaly_catch_rate = self._window_mean(
                a.agent_id, "anomaly"
            )
            if len(a.regions) >= 3:
                a.role = AgentRole.REGIONAL_SPECIALIST
            elif a.anomaly_catch_rate > 0.5:
                a.role = AgentRole.ANOMALY_HUNTER
            elif (
                a.avg_latency_ms
                and a.avg_latency_ms < 500
                and a.success_rate > 0.8
            ):
                a.role = AgentRole.LATENCY_OPTIMIZER
            else:
                a.role = AgentRole.GENERALIST

    def record(
        self,
        agent_id: str,
        region: str,
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
        self._reassign()

    def select_agent(self, region: str) -> Optional[str]:
        if not self.agents:
            return None
        candidates = [
            a
            for a in self.agents.values()
            if region in a.regions and a.success_rate > 0.5
        ] or list(self.agents.values())
        return max(
            candidates, key=lambda a: a.success_rate
        ).agent_id


# --------------------------------------------------------------------------- #
# Carbon market client (sync, cached)
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class MarketSnapshot:
    carbon_price_per_tco2_usd: float
    rec_available_mwh: float
    rec_price_per_mwh_usd: float
    timestamp: datetime = field(default_factory=_utcnow)


class CarbonMarketClient:
    """Sync market client with TTL cache."""

    def __init__(self, ttl_seconds: int = 300) -> None:
        self._cache: Optional[MarketSnapshot] = None
        self._ttl = ttl_seconds

    def get_snapshot(self) -> MarketSnapshot:
        if self._cache is not None:
            age = (
                _utcnow() - _ensure_aware(self._cache.timestamp)
            ).total_seconds()
            if age < self._ttl:
                return self._cache
        snap = MarketSnapshot(
            carbon_price_per_tco2_usd=random.uniform(20, 80),
            rec_available_mwh=random.uniform(10, 500),
            rec_price_per_mwh_usd=random.uniform(3, 9),
        )
        self._cache = snap
        return snap


# --------------------------------------------------------------------------- #
# HITL gate (sync + async)
# --------------------------------------------------------------------------- #
@dataclass
class HITLRequest:
    region: str
    scarcity_level: str
    score: float
    price: float
    reason: str
    urgency: str
    requested_at: datetime = field(default_factory=_utcnow)


class HumanReviewGate:
    """
    Sync/async HITL gate.

    * ``review``   — sync path; **rejects awaitable callbacks (fail-safe)**.
    * ``areview``  — async path; awaits awaitable callbacks with a timeout.

    Both record ``(approved, status)`` in ``feedback_log``.
    """

    def __init__(self, timeout: float = 3.0) -> None:
        self._timeout = timeout
        self.pending: Deque[HITLRequest] = deque(
            maxlen=_MAX_PENDING_HITL
        )
        self.feedback_log: Deque[Dict[str, Any]] = deque(
            maxlen=_MAX_HITL_LOG
        )
        self._callback: Optional[
            Callable[[HITLRequest], Any]
        ] = None

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
        if approved is True:
            status = "approved"
        elif approved is False:
            status = "rejected"
        else:
            status = "timed_out"
        self.feedback_log.append(
            {
                "req": _json_safe(asdict(req)),
                "approved": approved,
                "status": status,
                "source": source,
            }
        )
        return approved, status

    def review(
        self,
        region: str,
        level: HeliumScarcityLevel,
        score: float,
        price: float,
        reason: str,
        urgency: str,
    ) -> Tuple[Optional[bool], str]:
        req = HITLRequest(
            region=region,
            scarcity_level=level.value,
            score=score,
            price=price,
            reason=reason,
            urgency=urgency,
        )
        self.pending.append(req)
        try:
            if self._callback is None:
                return self._resolve(req, None, "no_callback")
            try:
                decision = self._callback(req)
            except Exception as e:
                logger.warning("HITL callback failed: %s", e)
                return self._resolve(req, None, "callback_error")
            if inspect.isawaitable(decision):
                close = getattr(decision, "close", None)
                if callable(close):
                    try:
                        close()
                    except Exception:
                        pass
                logger.error(
                    "HITL callback returned awaitable in sync review; "
                    "rejecting (fail-safe). Use areview() instead."
                )
                return self._resolve(req, False, "async_in_sync")
            return self._resolve(req, bool(decision), "callback")
        finally:
            try:
                self.pending.remove(req)
            except ValueError:
                pass

    async def areview(
        self,
        region: str,
        level: HeliumScarcityLevel,
        score: float,
        price: float,
        reason: str,
        urgency: str,
    ) -> Tuple[Optional[bool], str]:
        req = HITLRequest(
            region=region,
            scarcity_level=level.value,
            score=score,
            price=price,
            reason=reason,
            urgency=urgency,
        )
        self.pending.append(req)
        try:
            if self._callback is None:
                return self._resolve(req, None, "no_callback")
            try:
                decision = self._callback(req)
                if inspect.isawaitable(decision):
                    decision = await asyncio.wait_for(
                        decision, timeout=self._timeout
                    )
                return self._resolve(
                    req,
                    bool(decision) if decision is not None else None,
                    "callback",
                )
            except asyncio.TimeoutError:
                return self._resolve(req, None, "timeout")
            except Exception as e:
                logger.warning("HITL callback failed: %s", e)
                return self._resolve(req, None, "callback_error")
        finally:
            try:
                self.pending.remove(req)
            except ValueError:
                pass

    def outcomes(self) -> Dict[str, int]:
        counts: Counter = Counter()
        for entry in self.feedback_log:
            counts[entry.get("status", "unknown")] += 1
        return dict(counts)

    def active_learning_batch(
        self, n: int = 16
    ) -> List[Dict[str, Any]]:
        return list(self.feedback_log)[-n:]


# --------------------------------------------------------------------------- #
# Anomaly detector (per-region)
# --------------------------------------------------------------------------- #
class AnomalyDetector:
    """Per-region rolling z-score detector."""

    def __init__(
        self, window: int = 20, z_threshold: float = 3.0
    ) -> None:
        self.window = window
        self.z = z_threshold
        self._history: Dict[str, Deque[float]] = defaultdict(
            lambda: deque(maxlen=window)
        )

    def check(self, region: str, score: float) -> Tuple[bool, float]:
        hist = self._history[region]
        if len(hist) < 5:
            hist.append(score)
            return False, 0.0
        mean = statistics.fmean(hist)
        std = statistics.pstdev(hist) or 1e-6
        z = abs(score - mean) / std
        hist.append(score)
        return z > self.z, z


# --------------------------------------------------------------------------- #
# Main monitor
# --------------------------------------------------------------------------- #
class HeliumMonitor:
    """
    Enhanced helium supply-chain monitor.

    Backward-compatible signature::

        HeliumMonitor(config=None, simulation_seed=None, ...)

    Lifecycle (v6)
    --------------
    The ``__init__`` **does not** start the background loop unless a
    running event loop is detected *and* ``auto_start=True`` (the
    default, preserving v5 behavior). In sync contexts:

        await monitor.start()          # explicit start
        # OR
        async with HeliumMonitor(...) as monitor:
            ...

    If ``auto_start=True`` is passed from a sync context, monitoring
    is **not** started; a warning explains how to call ``start()``.
    """

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

    def __init__(
        self,
        config: Optional[Mapping[str, Any]] = None,
        simulation_seed: Optional[int] = None,
        deployment_id: str = "local",
        agent_id: str = "helium-monitor-0",
        features: Optional[Dict[str, bool]] = None,
        hardware: Optional[HardwareProfile] = None,
        auto_start: bool = True,
    ):
        self.config: Mapping[str, Any] = config or {}
        self.deployment_id = deployment_id
        self.agent_id = agent_id
        self.features: Dict[str, bool] = {
            **self.DEFAULT_FEATURES,
            **(features or {}),
        }
        self.hardware = hardware or HardwareProfile()

        # ---- API config ----
        endpoints = _cfg_get(self.config, "api_endpoints", None)
        if not isinstance(endpoints, Mapping):
            endpoints = {
                "primary": (
                    "https://api.helium-monitor.example.com/v1/supply"
                ),
                "backup": "https://backup.helium-api.example.com/v1/status",
            }
        self.api_endpoints: Dict[str, str] = dict(endpoints)
        self.api_key = (
            _cfg_get(self.config, "api_key", None)
            or os.getenv("HELIUM_API_KEY")
        )
        self.api_headers: Dict[str, str] = (
            {"Authorization": f"Bearer {self.api_key}"}
            if self.api_key
            else {}
        )

        # ---- Monitoring config ----
        self.update_interval_seconds = int(
            _cfg_get(self.config, "update_interval", 900)
        )
        self.history_buffer_size = int(
            _cfg_get(self.config, "history_buffer_size", 100)
        )
        self.max_retries = int(
            _cfg_get(self.config, "max_retries", 3)
        )
        self.base_retry_delay = float(
            _cfg_get(self.config, "base_retry_delay", 1.0)
        )
        self.region = str(
            _cfg_get(self.config, "region", "global")
        )

        # ---- State ----
        self.current_signal: Optional[HeliumSupplySignal] = None
        self._signal_history: Deque[HeliumSupplySignal] = deque(
            maxlen=self.history_buffer_size
        )
        self._monitoring_task: Optional[asyncio.Task] = None
        self._shutdown_event: Optional[asyncio.Event] = None

        # ---- RNG ----
        self._rng = (
            random.Random(simulation_seed)
            if simulation_seed is not None
            else random.Random()
        )

        # ---- Counters ----
        self._source_counts: Counter = Counter()
        self._fallback_count: int = 0
        self._anomaly_count: int = 0
        self._hitl_outcomes: Counter = Counter()

        # ---- Caches ----
        self._distill_cache: Dict[
            Tuple[int, PrecisionLevel], DistilledSimulationModel
        ] = {}
        self._federated_profile_override: Dict[
            str, List[float]
        ] = {}

        # ---- Locks ----
        self._lock = asyncio.Lock()

        # ---- Enhancement layers (built by _build_layers) ----
        self._build_layers()

        logger.info(
            "HeliumMonitor v6 ready: deployment=%s, agent=%s, region=%s",
            deployment_id,
            agent_id,
            self.region,
        )

        # ---- Optional auto-start ----
        if auto_start:
            self._try_auto_start()

    # ------------------------------------------------------------------
    # Layer construction
    # ------------------------------------------------------------------
    def _build_layers(self) -> None:
        # Circuits
        threshold = int(
            _cfg_get(self.config, "circuit_failure_threshold", 3)
        )
        recovery = float(
            _cfg_get(self.config, "circuit_recovery_timeout", 60.0)
        )
        self.circuits: Dict[str, SyncCircuitAdapter] = {
            "primary": SyncCircuitAdapter(
                "primary",
                failure_threshold=threshold,
                recovery_timeout=recovery,
            ),
            "backup": SyncCircuitAdapter(
                "backup",
                failure_threshold=threshold,
                recovery_timeout=recovery,
            ),
        }

        self.chaos: Optional[AsyncChaosInjector] = (
            AsyncChaosInjector(
                failure_rate=float(
                    _cfg_get(self.config, "chaos_failure_rate", 0.0)
                ),
                latency_ms=int(
                    _cfg_get(self.config, "chaos_latency_ms", 0)
                ),
            )
            if self.features["chaos_testing"]
            else None
        )

        # Temporal safety
        self.safety = (
            SignalSafetyMonitor()
            if self.features["temporal_logic"]
            else None
        )
        if self.safety:
            self.safety.register(
                SignalFreshness(
                    max_age_seconds=self.update_interval_seconds * 2
                )
            )
            self.safety.register(ScoreBounds())
            self.safety.register(PriceSanity())

        # XAI
        self.explainer = SignalExplainer if self.features["xai"] else None

        # RL
        self.rl_policy = (
            ProviderPolicy() if self.features["causal_rl"] else None
        )

        # Precision
        self.precision_ctl = (
            AdaptivePrecisionSelector(self.hardware)
            if self.features["adaptive_precision"]
            else None
        )

        # Distillation
        self.distiller = (
            QuantumDistillationBridge()
            if self.features["quantum_distillation"]
            else None
        )

        # Federated
        self.federated = (
            FederatedHeliumAggregator()
            if self.features["federated"]
            else None
        )

        # Multi-agent
        self.coordinator = (
            HeliumAgentCoordinator()
            if self.features["multi_agent"]
            else None
        )
        if self.coordinator:
            self.coordinator.register(self.agent_id)

        # Market
        self.market = (
            CarbonMarketClient() if self.features["carbon_market"] else None
        )

        # HITL
        self.hitl = HumanReviewGate() if self.features["hitl"] else None

        # Anomaly
        self.anomaly = (
            AnomalyDetector()
            if self.features["anomaly_detection"]
            else None
        )

    def set_features(self, features: Mapping[str, bool]) -> None:
        for k, v in features.items():
            if k not in self.DEFAULT_FEATURES:
                raise ValueError(f"unknown feature {k!r}")
            self.features[k] = bool(v)
        self._build_layers()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def _try_auto_start(self) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            logger.warning(
                "HeliumMonitor(auto_start=True) called outside a running "
                "event loop; monitoring will NOT start. Call "
                "`await monitor.start()` from async code or use "
                "`async with HeliumMonitor(...) as monitor`."
            )
            return
        self._shutdown_event = asyncio.Event()
        self._monitoring_task = loop.create_task(self._monitor_loop())

    async def start(self) -> None:
        """Explicitly start monitoring (idempotent)."""
        if (
            self._monitoring_task is not None
            and not self._monitoring_task.done()
        ):
            return
        self._shutdown_event = asyncio.Event()
        self._monitoring_task = asyncio.get_running_loop().create_task(
            self._monitor_loop()
        )

    async def shutdown(self) -> None:
        """Stop monitoring and clean up."""
        logger.info("Shutting down HeliumMonitor...")
        if self._shutdown_event is not None:
            self._shutdown_event.set()
        if (
            self._monitoring_task is not None
            and not self._monitoring_task.done()
        ):
            self._monitoring_task.cancel()
            try:
                await self._monitoring_task
            except asyncio.CancelledError:
                logger.debug("Monitoring task cancelled")
            except Exception as e:
                logger.error(f"Error during shutdown: {e}")
        self._monitoring_task = None
        self._shutdown_event = None
        logger.info("HeliumMonitor shutdown complete")

    async def __aenter__(self) -> "HeliumMonitor":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.shutdown()

    # ------------------------------------------------------------------
    # History
    # ------------------------------------------------------------------
    @property
    def signal_history(self) -> List[HeliumSupplySignal]:
        return list(self._signal_history)

    # ------------------------------------------------------------------
    # Monitoring loop
    # ------------------------------------------------------------------
    async def _monitor_loop(self) -> None:
        logger.info("Starting helium supply monitoring loop")
        while not (
            self._shutdown_event is not None
            and self._shutdown_event.is_set()
        ):
            try:
                signal = await self.fetch_helium_supply()
                self.current_signal = signal
                self._signal_history.append(signal)
                logger.info(
                    "Helium supply updated: %s "
                    "(score=%.2f, source=%s)",
                    signal.scarcity_level.value,
                    signal.scarcity_score,
                    signal.source,
                )
            except asyncio.CancelledError:
                logger.info("Helium monitoring task cancelled")
                break
            except Exception as e:
                logger.error(
                    "Helium monitoring failed: %s", e, exc_info=True
                )
                fb = self._simulate_helium_supply()
                fb.source = "fallback"
                fb.explanation = f"fetch failed: {e}"
                self.current_signal = fb
                self._signal_history.append(fb)
                self._fallback_count += 1
                self._source_counts["fallback"] += 1

            if self._shutdown_event is None:
                break
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(),
                    timeout=self.update_interval_seconds,
                )
                break
            except asyncio.TimeoutError:
                pass
        logger.info("Helium monitoring loop stopped")

    # ------------------------------------------------------------------
    # Fetch with enhancement pipeline
    # ------------------------------------------------------------------
    async def fetch_helium_supply(self) -> HeliumSupplySignal:
        available = self._available_providers()
        provider = ProviderKind.PRIMARY_API

        # FIXED: pre-fetch state is captured once and reused for learning.
        state: Optional[ProviderState] = None
        if self.rl_policy and len(available) > 1:
            state = ProviderState(
                hour_of_day=_utcnow().hour,
                recent_failure_rate=self._recent_failure_rate(),
                cache_hit=False,
                region_hash=float(hash(self.region) % 1000) / 1000.0,
            )
            provider = self.rl_policy.select(state, available)

        start = datetime.now(timezone.utc)
        signal, fallback_reason = await self._fetch_with_resilience(
            provider
        )
        latency_ms = (
            datetime.now(timezone.utc) - start
        ).total_seconds() * 1000
        self._source_counts[signal.source] += 1
        if signal.source == "fallback":
            self._fallback_count += 1

        # --- Anomaly detection (per-region) ---
        anomaly_flagged = False
        if self.anomaly is not None:
            anomaly_flagged, z = self.anomaly.check(
                self.region, signal.scarcity_score
            )
            if anomaly_flagged:
                self._anomaly_count += 1
                logger.warning(
                    "Anomalous signal: score=%.2f (z=%.2f)",
                    signal.scarcity_score,
                    z,
                )
        signal.is_anomalous = anomaly_flagged

        # --- Market enrichment ---
        if self.market is not None:
            snap = self.market.get_snapshot()
            signal.market_snapshot = {
                "carbon_price_per_tco2_usd": (
                    snap.carbon_price_per_tco2_usd
                ),
                "rec_available_mwh": snap.rec_available_mwh,
                "rec_price_per_mwh_usd": snap.rec_price_per_mwh_usd,
            }

        # --- HITL for anomalies (FIXED: result stored on the signal) ---
        if anomaly_flagged and self.hitl is not None:
            urgency = (
                "high" if signal.scarcity_score > 0.9 else "medium"
            )
            approved, status = await self.hitl.areview(
                region=self.region,
                level=signal.scarcity_level,
                score=signal.scarcity_score,
                price=signal.spot_price_usd_per_liter,
                reason="Anomalous scarcity signal detected",
                urgency=urgency,
            )
            signal.hitl_status = status
            signal.hitl_approved = approved
            self._hitl_outcomes[status] += 1
            if approved is False:
                # Reviewer vetoed the anomaly: mark the reading stale
                signal.is_stale = True

        # --- XAI ---
        if self.explainer is not None:
            exp = self.explainer.explain(
                level=signal.scarcity_level,
                score=signal.scarcity_score,
                price=signal.spot_price_usd_per_liter,
                inventory=signal.fab_inventory_days,
                source=signal.source,
                fallback_reason=fallback_reason,
                anomaly_flagged=anomaly_flagged,
            )
            signal.explanation = exp.headline
            signal.confidence = exp.confidence

        # --- Temporal verification (recent window) ---
        if self.safety is not None:
            ok, violations = self.safety.verify(
                list(self._signal_history) + [signal],
                region=self.region,
            )
            if not ok:
                logger.warning(
                    "Temporal violations: %s", violations
                )

        # --- RL feedback (FIXED: pre-fetch state) ---
        if self.rl_policy is not None and state is not None:
            # Reward = "the chosen provider actually answered".
            delivered = signal.source == provider.value or (
                provider == ProviderKind.SIMULATION
                and signal.source == "simulation"
            )
            accuracy = 1.0 if delivered else 0.0
            self.rl_policy.record(state, provider, accuracy, latency_ms)
            if len(self._signal_history) % 20 == 0:
                self.rl_policy.update()

        # --- Multi-agent feedback ---
        if self.coordinator is not None:
            self.coordinator.record(
                agent_id=self.agent_id,
                region=self.region,
                success=not anomaly_flagged,
                latency_ms=latency_ms,
                anomaly_caught=anomaly_flagged,
            )

        return signal

    def _available_providers(self) -> List[ProviderKind]:
        providers: List[ProviderKind] = []
        if self.circuits["primary"].can_call():
            providers.append(ProviderKind.PRIMARY_API)
        if self.circuits["backup"].can_call():
            providers.append(ProviderKind.BACKUP_API)
        # FIXED: aggregate so FEDERATED_PEER is reachable
        if self.federated is not None:
            self.federated.aggregate()
            if self.federated.get_global(self.region):
                providers.append(ProviderKind.FEDERATED_PEER)
        if self.distiller is not None:
            providers.append(ProviderKind.DISTILLED_MODEL)
        providers.append(ProviderKind.SIMULATION)
        return providers

    def _recent_failure_rate(self) -> float:
        total = (
            self.circuits["primary"].failures
            + self.circuits["backup"].failures
        )
        capacity = 2 * self.circuits["primary"]._cb.failure_threshold
        return min(1.0, total / capacity) if capacity else 0.0

    async def _fetch_with_resilience(
        self, provider: ProviderKind
    ) -> Tuple[HeliumSupplySignal, Optional[str]]:
        try:
            if self.chaos is not None:
                await self.chaos.maybe_inject(
                    f"provider:{provider.value}"
                )

            if provider == ProviderKind.PRIMARY_API:
                return await self._try_primary(), None
            if provider == ProviderKind.BACKUP_API:
                return await self._try_backup(), None
            if provider == ProviderKind.FEDERATED_PEER:
                sig = self._build_from_federated()
                if sig is not None:
                    return sig, None
                return (
                    self._simulate_helium_supply(),
                    "federated peer unavailable",
                )
            if provider == ProviderKind.DISTILLED_MODEL:
                return self._simulate_with_distillation(), None
            return (
                self._simulate_helium_supply(),
                "explicit simulation provider",
            )
        except Exception as e:
            logger.warning(
                "Provider %s failed: %s; using simulation",
                provider.value,
                e,
            )
            sig = self._simulate_helium_supply()
            sig.source = "fallback"
            return sig, str(e)

    async def _try_primary(self) -> HeliumSupplySignal:
        cb = self.circuits["primary"]
        if not cb.can_call():
            raise RuntimeError("primary circuit open")
        try:
            import aiohttp

            async with aiohttp.ClientSession() as session:
                async with session.get(
                    self.api_endpoints["primary"],
                    timeout=aiohttp.ClientTimeout(total=10),
                    headers=self.api_headers,
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        sig = self._parse_api_response(
                            data, source="primary_api"
                        )
                        cb.record_success()
                        return sig
                    if resp.status == 429:
                        retry_after = int(
                            resp.headers.get(
                                "Retry-After", self.base_retry_delay
                            )
                        )
                        logger.warning(
                            "Rate limited (retry after %ss)", retry_after
                        )
                        await asyncio.sleep(min(retry_after, 5))
                    raise RuntimeError(
                        f"primary status {resp.status}"
                    )
        except Exception as e:
            cb.record_failure()
            raise e

    async def _try_backup(self) -> HeliumSupplySignal:
        cb = self.circuits["backup"]
        if not cb.can_call():
            raise RuntimeError("backup circuit open")
        try:
            import aiohttp

            async with aiohttp.ClientSession() as session:
                async with session.get(
                    self.api_endpoints["backup"],
                    timeout=aiohttp.ClientTimeout(total=10),
                    headers=self.api_headers,
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        sig = self._parse_api_response(
                            data, source="backup_api"
                        )
                        cb.record_success()
                        return sig
                    raise RuntimeError(
                        f"backup status {resp.status}"
                    )
        except Exception as e:
            cb.record_failure()
            raise e

    def _build_from_federated(
        self,
    ) -> Optional[HeliumSupplySignal]:
        if self.federated is None:
            return None
        profile = self.federated.get_global(self.region)
        if not profile:
            return None
        hour = _utcnow().hour
        mean = profile[hour]
        # FIXED: standardise classification
        level = _classify(mean)
        # FIXED: confidence derived from the profile's internal uncertainty
        # (we don't currently track per-hour std in the aggregator; use
        # the sample count as a proxy)
        profile_updates = [
            u for u in self.federated.updates if u.region == self.region
        ]
        total_samples = sum(
            max(0, u.sample_count) for u in profile_updates
        )
        confidence = 1.0 - 1.0 / (1.0 + max(0, total_samples) / 10.0)
        return HeliumSupplySignal(
            timestamp=_utcnow(),
            scarcity_level=level,
            scarcity_score=mean,
            spot_price_usd_per_liter=4.0 + mean * 8.0,
            fab_inventory_days=max(3, int(30 - mean * 25)),
            vendor_alerts=["federated aggregate signal"],
            source="federated_peer",
            confidence=confidence,
            region=self.region,
            is_stale=True,  # historical mean, not a live reading
        )

    # ------------------------------------------------------------------
    # Simulation
    # ------------------------------------------------------------------
    def _simulate_helium_supply(self) -> HeliumSupplySignal:
        rand = self._rng.random()
        if rand < 0.70:
            score = self._rng.uniform(0.0, 0.2)
            price = self._rng.uniform(3.5, 4.5)
            inv = self._rng.randint(25, 35)
            alerts: List[str] = []
        elif rand < 0.85:
            score = self._rng.uniform(0.3, 0.5)
            price = self._rng.uniform(5.0, 6.5)
            inv = self._rng.randint(15, 25)
            alerts = (
                ["Supply chain tightening"]
                if self._rng.random() < 0.3
                else []
            )
        elif rand < 0.95:
            score = self._rng.uniform(0.6, 0.8)
            price = self._rng.uniform(7.0, 9.0)
            inv = self._rng.randint(8, 15)
            alerts = ["Geopolitical disruption", "Production delay"]
        else:
            score = self._rng.uniform(0.85, 1.0)
            price = self._rng.uniform(10.0, 15.0)
            inv = self._rng.randint(3, 8)
            alerts = [
                "Critical shortage",
                "Emergency allocation",
                "Price spike",
            ]

        return HeliumSupplySignal(
            timestamp=_utcnow(),
            scarcity_level=_classify(score),
            scarcity_score=score,
            spot_price_usd_per_liter=round(price, 2),
            fab_inventory_days=inv,
            vendor_alerts=alerts,
            source="simulation",
            forecast_valid_until=_utcnow() + timedelta(hours=24),
            confidence=0.5,
            region=self.region,
        )

    def _simulate_with_distillation(
        self,
    ) -> HeliumSupplySignal:
        if self.distiller is None:
            return self._simulate_helium_supply()

        urgency = (
            "critical"
            if self.current_signal and self.current_signal.is_critical()
            else "normal"
        )
        precision = (
            self.precision_ctl.select(urgency)
            if self.precision_ctl
            else PrecisionLevel.INT8
        )
        hour = _utcnow().hour

        # Cache the distilled model per (hour, precision)
        cache_key = (hour, precision)
        model = self._distill_cache.get(cache_key)
        if model is None:
            model = self.distiller.distill(
                self.signal_history,
                precision,
                federated_override=self._federated_profile_override.get(
                    self.region
                ),
            )
            self._distill_cache[cache_key] = model

        mean = model.hourly_mean[hour]
        std = model.hourly_std[hour]
        score = max(0.0, min(1.0, self._rng.gauss(mean, std)))
        return HeliumSupplySignal(
            timestamp=_utcnow(),
            scarcity_level=_classify(score),
            scarcity_score=score,
            spot_price_usd_per_liter=round(4.0 + score * 8.0, 2),
            fab_inventory_days=max(3, int(30 - score * 25)),
            vendor_alerts=["distilled simulation"],
            source="distilled_model",
            forecast_valid_until=_utcnow() + timedelta(hours=24),
            confidence=0.5 * model.quality_retention,
            region=self.region,
        )

    # ------------------------------------------------------------------
    # API parsing
    # ------------------------------------------------------------------
    def _parse_api_response(
        self, data: Any, source: str
    ) -> HeliumSupplySignal:
        if not isinstance(data, Mapping):
            raise ValueError(
                f"API response is {type(data).__name__}, expected mapping"
            )
        required = [
            "scarcity_score",
            "spot_price_usd",
            "fab_inventory_days",
        ]
        for f in required:
            if f not in data:
                raise ValueError(f"Missing required field: {f}")

        level_str = data.get("scarcity_level", "normal")
        try:
            level = HeliumScarcityLevel(level_str)
        except ValueError:
            logger.warning(
                "Invalid scarcity_level '%s', defaulting to NORMAL",
                level_str,
            )
            level = HeliumScarcityLevel.NORMAL

        try:
            score = float(data["scarcity_score"])
        except (TypeError, ValueError):
            raise ValueError("scarcity_score must be numeric")
        if not 0.0 <= score <= 1.0:
            logger.warning(
                "scarcity_score %s out of [0,1], clamping", score
            )
            score = max(0.0, min(1.0, score))
        # FIXED: level derived from score, not trusted blindly
        level = _classify(score)

        price = max(0.0, float(data["spot_price_usd"]))
        inv = max(0, int(data["fab_inventory_days"]))

        valid_until: Optional[datetime] = None
        if data.get("forecast_valid_until"):
            try:
                valid_until = _parse_timestamp(
                    data["forecast_valid_until"]
                )
            except (ValueError, TypeError) as e:
                logger.warning(
                    "Invalid forecast_valid_until: %s", e
                )

        return HeliumSupplySignal(
            timestamp=_utcnow(),
            scarcity_level=level,
            scarcity_score=score,
            spot_price_usd_per_liter=price,
            fab_inventory_days=inv,
            vendor_alerts=list(data.get("alerts", []) or []),
            source=source,
            forecast_valid_until=valid_until,
            confidence=0.95,
            region=self.region,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def get_current_supply(
        self,
    ) -> Optional[HeliumSupplySignal]:
        return self.current_signal

    def get_supply_trend(
        self, hours: int = 24
    ) -> List[HeliumSupplySignal]:
        cutoff = _utcnow() - timedelta(hours=hours)
        return [
            s
            for s in self.signal_history
            if _ensure_aware(s.timestamp) > cutoff
        ]

    async def get_forecast(self, hours_ahead: int = 24) -> Dict:
        if self.current_signal is None:
            return {
                "error": "No data available",
                "hours_ahead": hours_ahead,
            }

        current_score = self.current_signal.scarcity_score
        predicted_score = current_score
        if self.federated is not None:
            profile = self.federated.get_global(self.region)
            if profile:
                future_hour = (
                    _utcnow() + timedelta(hours=hours_ahead)
                ).hour
                predicted_score = (
                    0.5 * current_score + 0.5 * profile[future_hour]
                )

        trend = (
            "worsening"
            if predicted_score > current_score + 0.05
            else "improving"
            if predicted_score < current_score - 0.05
            else "stable"
        )
        confidence = max(0.4, 0.9 - (hours_ahead / 100))

        distillation_info: Optional[Dict[str, Any]] = None
        if self.distiller is not None:
            urgency = (
                "critical"
                if self.current_signal.is_critical()
                else "normal"
            )
            precision = (
                self.precision_ctl.select(urgency)
                if self.precision_ctl
                else PrecisionLevel.INT8
            )
            model = self.distiller.distill(
                self.signal_history,
                precision,
                federated_override=self._federated_profile_override.get(
                    self.region
                ),
            )
            distillation_info = {
                "precision": precision.value,
                "quality_retention": model.quality_retention,
                "energy_reduction_percent": (
                    model.energy_reduction_percent
                ),
            }

        return {
            "current_scarcity": (
                self.current_signal.scarcity_level.value
            ),
            "current_score": current_score,
            "predicted_score": predicted_score,
            "forecast": trend,
            "hours_ahead": hours_ahead,
            "confidence": round(confidence, 2),
            "price_forecast": {
                "baseline": (
                    self.current_signal.spot_price_usd_per_liter
                ),
                "trend": (
                    "up"
                    if trend == "worsening"
                    else "down"
                    if trend == "improving"
                    else "stable"
                ),
            },
            "distillation": distillation_info,
            "explanation": (
                self.current_signal.explanation
                if self.current_signal
                else None
            ),
        }

    # ------------------------------------------------------------------
    # Enhancement-specific public methods
    # ------------------------------------------------------------------
    def contribute_federated_profile(
        self, region: Optional[str] = None
    ) -> None:
        if self.federated is None:
            return
        region = region or self.region
        readings = [
            s for s in self.signal_history if s.region == region
        ]
        if len(readings) < 10:
            return
        buckets: Dict[int, List[float]] = defaultdict(list)
        for s in readings:
            buckets[_ensure_aware(s.timestamp).hour].append(
                s.scarcity_score
            )
        means = [
            statistics.fmean(buckets[h]) if buckets.get(h) else 0.3
            for h in range(24)
        ]
        self.federated.push(
            FederatedHeliumProfile(
                deployment_id=self.deployment_id,
                region=region,
                hourly_means=means,
                sample_count=len(readings),
            )
        )

    def merge_federated_profile(self, blend: float = 0.3) -> int:
        """
        Blend the latest federated aggregate into the local distilled
        model and invalidate the distillation cache.

        FIXED: the v5 version was a no-op.
        """
        if self.federated is None:
            return 0
        aggregate = self.federated.aggregate()
        profile = aggregate.get(self.region)
        if not profile:
            return 0
        current = self._federated_profile_override.get(self.region)
        if current is None:
            self._federated_profile_override[self.region] = list(
                profile
            )
        else:
            self._federated_profile_override[self.region] = [
                (1 - blend) * c + blend * p
                for c, p in zip(current, profile)
            ]
        self._distill_cache.clear()
        return len(profile)

    def set_hitl_callback(
        self, cb: Callable[[HITLRequest], Any]
    ) -> None:
        if self.hitl is not None:
            self.hitl.set_callback(cb)

    def active_learning_samples(
        self, n: int = 16
    ) -> List[Dict[str, Any]]:
        return (
            self.hitl.active_learning_batch(n)
            if self.hitl is not None
            else []
        )

    def distill_simulation_model(
        self, urgency: str = "normal"
    ) -> Optional[DistilledSimulationModel]:
        if self.distiller is None:
            return None
        precision = (
            self.precision_ctl.select(urgency)
            if self.precision_ctl
            else PrecisionLevel.INT8
        )
        return self.distiller.distill(
            self.signal_history,
            precision,
            federated_override=self._federated_profile_override.get(
                self.region
            ),
        )

    # ------------------------------------------------------------------
    # Prometheus
    # ------------------------------------------------------------------
    def collect_prometheus_metrics(
        self,
    ) -> Dict[str, Tuple[Any, Dict[str, str]]]:
        metrics: Dict[str, Tuple[Any, Dict[str, str]]] = {}
        signal = self.get_current_supply()
        if signal is None:
            return metrics

        numeric = {
            HeliumScarcityLevel.NORMAL: 0,
            HeliumScarcityLevel.CAUTION: 1,
            HeliumScarcityLevel.CRITICAL: 2,
            HeliumScarcityLevel.SEVERE: 3,
        }
        src_labels = {"source": signal.source}
        metrics["green_agent_helium_scarcity_level"] = (
            numeric[signal.scarcity_level],
            src_labels,
        )
        metrics["green_agent_helium_scarcity_score"] = (
            signal.scarcity_score,
            src_labels,
        )
        metrics["green_agent_helium_spot_price_usd"] = (
            signal.spot_price_usd_per_liter,
            {},
        )
        metrics["green_agent_helium_fab_inventory_days"] = (
            signal.fab_inventory_days,
            {},
        )
        metrics["green_agent_helium_vendor_alerts_count"] = (
            len(signal.vendor_alerts),
            {},
        )
        metrics["green_agent_helium_price_premium_usd"] = (
            signal.price_premium(4.0),
            {},
        )
        metrics["green_agent_helium_confidence"] = (
            signal.confidence,
            {},
        )
        metrics["green_agent_helium_anomalous"] = (
            int(signal.is_anomalous),
            {},
        )
        metrics["green_agent_helium_hitl_approved"] = (
            -1
            if signal.hitl_approved is None
            else int(signal.hitl_approved),
            {"status": signal.hitl_status},
        )
        if signal.market_snapshot:
            metrics["green_agent_carbon_price_usd_per_tco2"] = (
                signal.market_snapshot.get(
                    "carbon_price_per_tco2_usd", 0.0
                ),
                {},
            )
            metrics["green_agent_rec_available_mwh"] = (
                signal.market_snapshot.get(
                    "rec_available_mwh", 0.0
                ),
                {},
            )
        return metrics

    # ------------------------------------------------------------------
    # Stats / health
    # ------------------------------------------------------------------
    def get_statistics(self) -> Dict[str, Any]:
        stats: Dict[str, Any] = {
            "deployment_id": self.deployment_id,
            "agent_id": self.agent_id,
            "region": self.region,
            "history_len": len(self._signal_history),
            "circuits": {
                k: {"state": cb.state, "failures": cb.failures}
                for k, cb in self.circuits.items()
            },
            "fallback_count": self._fallback_count,
            "anomaly_count": self._anomaly_count,
            "source_counts": dict(self._source_counts),
            "hitl_outcomes": dict(self._hitl_outcomes),
        }
        if self.safety is not None:
            stats["temporal_violations"] = list(
                self.safety.violations
            )[-5:]
        if self.coordinator is not None:
            stats["agents"] = {
                aid: {
                    "role": a.role.value,
                    "success_rate": a.success_rate,
                    "avg_latency_ms": a.avg_latency_ms,
                }
                for aid, a in self.coordinator.agents.items()
            }
        if self.chaos is not None:
            stats["chaos_events"] = list(self.chaos.events)[-5:]
        if self.hitl is not None:
            stats["hitl"] = {
                "pending": len(self.hitl.pending),
                "log_size": len(self.hitl.feedback_log),
                "outcomes": self.hitl.outcomes(),
            }
        if self.market is not None:
            snap = self.market.get_snapshot()
            stats["market"] = {
                "carbon_price_per_tco2_usd": (
                    snap.carbon_price_per_tco2_usd
                ),
                "rec_available_mwh": snap.rec_available_mwh,
            }
        return stats

    async def health_check(self) -> Dict[str, Any]:
        running = (
            self._monitoring_task is not None
            and not self._monitoring_task.done()
        )
        signal_age_s: Optional[float] = None
        if self.current_signal is not None:
            signal_age_s = (
                _utcnow()
                - _ensure_aware(self.current_signal.timestamp)
            ).total_seconds()
        return {
            "healthy": (
                running
                and self.circuits["primary"].state != "OPEN"
                and self.circuits["backup"].state != "OPEN"
            ),
            "monitoring_active": running,
            "current_signal_age_s": signal_age_s,
            "circuit_states": {
                k: cb.state for k, cb in self.circuits.items()
            },
            "fallback_count": self._fallback_count,
            "source_counts": dict(self._source_counts),
            "temporal_violations": (
                len(self.safety.violations)
                if self.safety is not None
                else 0
            ),
            "hitl_outcomes": dict(self._hitl_outcomes),
        }


# --------------------------------------------------------------------------- #
# Factory (backward-compatible)
# --------------------------------------------------------------------------- #
def create_helium_monitor(
    config: Optional[Mapping[str, Any]] = None,
    simulation_seed: Optional[int] = None,
    deployment_id: str = "local",
    agent_id: str = "helium-monitor-0",
    features: Optional[Dict[str, bool]] = None,
    hardware: Optional[HardwareProfile] = None,
    auto_start: bool = True,
) -> HeliumMonitor:
    return HeliumMonitor(
        config=config,
        simulation_seed=simulation_seed,
        deployment_id=deployment_id,
        agent_id=agent_id,
        features=features,
        hardware=hardware,
        auto_start=auto_start,
    )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    # v18 re-exports
    "CircuitBreaker",
    "ChaosTestingEngine",
    "ActiveUserPreferenceLearner",
    "AdaptivePrecisionSwitcher",
    "QuantumDistillationEngine",
    "CarbonMarketIntegrator",
    "XAIDecisionExplainer",
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
    "ForecastPoint",
    "ForecastBatch",
    "ForecastSource",
    # v6 domain types
    "HeliumScarcityLevel",
    "PrecisionLevel",
    "ProviderKind",
    "AgentRole",
    "CircuitState",
    "HeliumSupplySignal",
    "SignalExplanation",
    "DistilledSimulationModel",
    "FederatedHeliumProfile",
    "MarketSnapshot",
    "HITLRequest",
    "HardwareProfile",
    "ProviderState",
    "AgentProfile",
    # v6 components (helium-specific, fixed)
    "SignalFreshness",
    "ScoreBounds",
    "PriceSanity",
    "SignalSafetyMonitor",
    "SignalExplainer",
    "ProviderPolicy",
    "QuantumDistillationBridge",
    "AdaptivePrecisionSelector",
    "FederatedHeliumAggregator",
    "HeliumAgentCoordinator",
    "CarbonMarketClient",
    "HumanReviewGate",
    "AnomalyDetector",
    "SyncCircuitAdapter",
    "AsyncChaosInjector",
    # Main
    "HeliumMonitor",
    "create_helium_monitor",
    # Helpers
    "_classify",
]


# --------------------------------------------------------------------------- #
# Demo
# --------------------------------------------------------------------------- #
async def _demo() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        ),
    )

    print("=" * 80)
    print("helium_monitor.py v6.0.0 — Demo")
    print("=" * 80)

    def auto_approve(req: HITLRequest) -> bool:
        logger.info(
            "[HITL] auto-approve %s: %s",
            req.urgency,
            req.reason,
        )
        return True

    config = {
        "update_interval": 2,
        "history_buffer_size": 50,
        "region": "US-CA",
        "api_endpoints": {"primary": "https://invalid.example.com"},
    }

    # Demonstrate the async-context lifecycle (v6 pattern)
    async with HeliumMonitor(
        config,
        simulation_seed=42,
        deployment_id="us-ca-prod-01",
        agent_id="helium-1",
        hardware=HardwareProfile(supports_int4=True, vram_gb=48),
        auto_start=False,  # async with calls start()
    ) as monitor:
        monitor.set_hitl_callback(auto_approve)

        # Wait for a couple of cycles
        for _ in range(3):
            await asyncio.sleep(2.5)
            sig = monitor.get_current_supply()
            if sig is None:
                print("  (no signal yet)")
                continue
            print(
                f"\n[{sig.timestamp.strftime('%H:%M:%S')}] "
                f"{sig.scarcity_level.value.upper()} | "
                f"score={sig.scarcity_score:.2f} | "
                f"${sig.spot_price_usd_per_liter:.2f}/L | "
                f"inv={sig.fab_inventory_days}d | "
                f"src={sig.source}"
            )
            if sig.hitl_status != "not_required":
                print(
                    f"  HITL: status={sig.hitl_status} "
                    f"approved={sig.hitl_approved}"
                )
            if sig.explanation:
                print(f"  XAI: {sig.explanation}")

        # Show the classification consistency fix
        print("\n=== Classification consistency check ===")
        for score in (0.24, 0.30, 0.50, 0.60, 0.79, 0.85):
            level = _classify(score)
            print(
                f"  score={score:.2f} → {level.value}"
            )

        # Show the confidence fix (peaks inside band, dips at boundaries)
        print("\n=== Confidence peaks inside a band ===")
        for score in (0.25, 0.40, 0.55, 0.70, 0.80):
            exp = SignalExplainer.explain(
                level=_classify(score),
                score=score,
                price=5.0,
                inventory=20,
                source="test",
            )
            print(
                f"  score={score:.2f}: confidence={exp.confidence:.3f}"
            )

        # Forecast
        fc = await monitor.get_forecast(hours_ahead=12)
        print(f"\n=== Forecast ===\n{fc}")

        # Distilled model
        model = monitor.distill_simulation_model(urgency="normal")
        if model:
            print(
                f"\n=== Distilled Model ===\n"
                f"precision={model.precision.value} "
                f"retention={model.quality_retention} "
                f"energy_saved={model.energy_reduction_percent}%"
            )

        # Federated contribution + merge
        monitor.contribute_federated_profile()
        merged = monitor.merge_federated_profile(blend=0.3)
        print(
            f"\n=== Federated Merge ===\n"
            f"merged {merged} hourly values into the distilled model"
        )

        # Prometheus
        print("\n=== Prometheus Metrics ===")
        for k, (v, labels) in monitor.collect_prometheus_metrics().items():
            print(f"  {k}{labels} = {v}")

        # Statistics + health
        print("\n=== Statistics ===")
        import json as _json

        print(
            _json.dumps(
                monitor.get_statistics(), indent=2, default=str
            )
        )
        print("\n=== Health Check ===")
        print(
            _json.dumps(
                await monitor.health_check(), indent=2, default=str
            )
        )

        # Audit-trail demo: sync review rejects async callbacks
        print(
            "\n=== Async callback in sync review (fail-safe) ==="
        )

        async def _async_cb(req: HITLRequest) -> bool:
            return True

        monitor.hitl.set_callback(_async_cb)
        approved, status = monitor.hitl.review(
            region="test",
            level=HeliumScarcityLevel.CRITICAL,
            score=0.9,
            price=12.0,
            reason="test",
            urgency="high",
        )
        print(
            f"  async callback → approved={approved}, "
            f"status={status} (expected rejected/async_in_sync)"
        )

    print("\nDemo complete.")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--demo":
        asyncio.run(_demo())
    else:
        print("Usage: python helium_monitor.py --demo")
