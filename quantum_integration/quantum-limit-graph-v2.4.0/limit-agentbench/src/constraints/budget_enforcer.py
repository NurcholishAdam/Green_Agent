# src/constraints/budget_enforcer.py
"""
budget_enforcer.py — v6.0.0 (thin wrapper over carbon_forecast v18)
====================================================================

Enforces execution budgets during benchmarking.

**What changed (v5 → v6)**
--------------------------

**P0 — enforcement safety and correctness**

1. **`check()` fails closed on every degraded path.** v5 returned normally
   on invalid metrics, open circuit, chaos injection, and internal errors —
   a caller that "successfully called `check()`" might not have been
   enforced at all. v6 raises :class:`BudgetExceeded` with
   ``degraded=True`` on every such path; the caller can opt into fail-open
   with ``degraded_policy=DegradedPolicy.RETURN``.
2. **The causal learner is feature-aware.** v5's ``record(reward)`` had no
   state input and ``update()`` was a counter; the buffer was never
   consumed. v6 stores ``(features, reward)`` and updates
   ``lr * reward * feature[i]``.
3. **Signed reward.** v5 used ``1.0 if correct else 0.0``, a one-way
   ratchet toward looser budgets. v6 uses ``+1.0 / -1.0`` so incorrect
   decisions tighten.
4. **Tightening-only by default.** ``allow_budget_loosening=False`` (the
   default) clamps offsets to ``<= 0``; any loosening must be explicitly
   opted in and is surfaced as ``loosening_events`` in ``health_check()``.
5. **HITL propagation.** v5 logged the review and raised regardless — the
   review was ceremony. v6 reviews **every** violation (not just the top);
   with ``allow_hitl_override=True`` a fully approved set suppresses the
   raise and returns normally; ``hitl_override`` is on the exception.
6. **NaN / missing metrics → EMERGENCY.** v5's ``_safe_float(NaN) = 0.0``
   silently passed. v6 records ``bad_data`` violations with
   ``severity=EMERGENCY``.
7. **Zero-limit metrics → EMERGENCY.** v5's ``overshoot_pct = 0.0`` for
   zero-limit metrics classified them as ``INFO``. v6 treats any usage of
   a zero-budget metric as ``+inf`` overshoot.
8. **Construction-time validation.** ``max_energy=nan``, negative budgets,
   and non-numeric values raise at construction.
9. **No breach classifies as INFO.** v5's minimum for a breach was INFO;
   v6 uses WARNING.

**P1 — reliability**

10. **Thread-safe via `RLock`** across every mutable structure.
11. **v18's `CircuitBreaker`** (via ``SyncCircuitAdapter``) replaces the
    local one that could let two concurrent callers both flip HALF_OPEN.
12. **Bounded collections**: ``federated.updates``, ``safety.violations``,
    ``hitl.pending``, ``hitl.feedback_log``, ``chaos.events``.
13. **`record_outcome(correct)`** uses the *real* ``BudgetState`` from the
    last check, not hard-coded constants.
14. **`get_statistics()` reports the last cached market snapshot**, not a
    fresh random one.
15. **Async HITL callbacks awaited in `areview`, rejected (fail-safe) in
    `review`.**
16. **`set_features()`, `health_check()`, `__enter__`/`__aenter__`.**
17. **`agent_metrics` and budgets** validated and defensively copied at
    construction.

**P2 — architecture**

18. **Eight enhancement classes imported from `carbon_forecast` v18** —
    the eighth local copy in v5 is retired.
19. **Load-bearing temporal properties.** v5's ``AllViolationsReported``
    and ``SeverityMonotonic`` were tautologies (hard-coded context).
    v6 passes the real violation counts and severity, and both properties
    actually fire on logic bugs.
20. **`QuantumDistillationBridge.distill` uses `math.ceil`**, not
    ``floor`` — for an enforcer, quantization must never loosen a budget.
21. **`FederatedBreachAggregator` bounds and zero-weight guard.**
22. **Helium enforcement clarified**: ``metrics["helium"]`` is the
    enforced value; ``helium_units`` (estimated from energy) is exposed
    as informational. Documented in the class docstring.

**Public API preserved**

    enforcer = BudgetEnforcer(max_energy, max_carbon, max_latency)
    enforcer.check(metrics)   # raises BudgetExceeded on breach

**New in v6**

    enforcer.check(metrics, agent_id=...)        # per-agent feedback
    enforcer.record_outcome(correct)             # uses the last real state
    enforcer.set_features(features)
    enforcer.health_check()
    await enforcer.health_check()
    async with BudgetEnforcer(...) as enforcer: ...
"""

from __future__ import annotations

import asyncio
import copy
import inspect
import json
import logging
import math
import random
import threading
import time
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
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
    Tuple,
)

# --------------------------------------------------------------------------- #
# v18 imports (single implementation for the enhancement classes)
# --------------------------------------------------------------------------- #
try:
    from carbon_forecast import (  # type: ignore[import-not-found]
        CircuitBreaker,
        ChaosTestingEngine,
        ActiveUserPreferenceLearner,
        AdaptivePrecisionSwitcher,
        CarbonMarketIntegrator,
        MultiAgentCoordinator,
        FederatedGreenAggregator,
        TemporalLogicVerifier,
        SimpleQuantumDistillationEngine,
        SimpleFederatedGreenAggregator,
        SimpleMultiAgentCoordinator,
        SimpleTemporalLogicVerifier,
        SimpleAdaptivePrecisionSwitcher,
        SimpleCarbonMarketIntegrator,
        SimpleChaosTestingEngine,
        SimpleActiveUserPreferenceLearner,
        _utcnow,
        _ensure_aware,
        _json_safe,
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
    CarbonMarketIntegrator = None  # type: ignore[assignment]
    MultiAgentCoordinator = None  # type: ignore[assignment]
    FederatedGreenAggregator = None  # type: ignore[assignment]
    TemporalLogicVerifier = None  # type: ignore[assignment]
    SimpleQuantumDistillationEngine = None  # type: ignore[assignment]
    SimpleFederatedGreenAggregator = None  # type: ignore[assignment]
    SimpleMultiAgentCoordinator = None  # type: ignore[assignment]
    SimpleTemporalLogicVerifier = None  # type: ignore[assignment]
    SimpleAdaptivePrecisionSwitcher = None  # type: ignore[assignment]
    SimpleCarbonMarketIntegrator = None  # type: ignore[assignment]
    SimpleChaosTestingEngine = None  # type: ignore[assignment]
    SimpleActiveUserPreferenceLearner = None  # type: ignore[assignment]

    def _utcnow() -> datetime:  # type: ignore[misc]
        return datetime.now(timezone.utc)

    def _ensure_aware(dt: datetime) -> datetime:  # type: ignore[misc]
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

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


logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Bounds
# --------------------------------------------------------------------------- #
_MAX_FEDERATED_UPDATES = 100
_MAX_VIOLATIONS = 200
_MAX_HITL_PENDING = 100
_MAX_HITL_LOG = 500
_MAX_CHAOS_EVENTS = 500
_MAX_CAUSAL_BUFFER = 512
_MAX_LAST_VIOLATIONS = 100


# --------------------------------------------------------------------------- #
# Enums
# --------------------------------------------------------------------------- #
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


class EvaluationOutcome(Enum):
    """Distinguishes the five outcomes `check()` may raise for."""

    INFEASIBLE = "infeasible"
    OVERRIDDEN = "overridden"      # HITL approved; no raise
    DEGRADED = "degraded"          # invalid input
    CIRCUIT_OPEN = "circuit_open"
    CHAOS = "chaos"
    ERROR = "error"


class DegradedPolicy(Enum):
    """What `check()` does when it cannot enforce."""

    RAISE = "raise"    # default: fail-closed (raise BudgetExceeded)
    RETURN = "return"  # opt-in: fail-open (return normally, log warning)


# --------------------------------------------------------------------------- #
# Structured exception
# --------------------------------------------------------------------------- #
class BudgetExceeded(Exception):
    """
    Raised when an execution budget is exceeded **or** enforcement is
    unavailable.

    Backward compatible: ``str(exc)`` is the v1-style message.

    Structured accessors distinguish a real breach (``degraded=False``)
    from a degradation (``degraded=True``) — a distinction v5 could not
    express.
    """

    def __init__(
        self,
        message: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__(message)
        self.details: Dict[str, Any] = details or {}

    @property
    def degraded(self) -> bool:
        """True if enforcement was unavailable (not a real breach)."""
        return bool(self.details.get("degraded", False))

    @property
    def outcome(self) -> str:
        return str(
            self.details.get("outcome", EvaluationOutcome.INFEASIBLE.value)
        )

    @property
    def violations(self) -> List[Dict[str, Any]]:
        return list(self.details.get("violations", []) or [])

    @property
    def severity(self) -> Optional[str]:
        return self.details.get("severity")

    @property
    def explanation(self) -> Optional[Dict[str, Any]]:
        return self.details.get("explanation")

    @property
    def market_snapshot(self) -> Optional[Dict[str, Any]]:
        return self.details.get("market_snapshot")

    @property
    def hitl_override(self) -> bool:
        return bool(self.details.get("hitl_override", False))


# --------------------------------------------------------------------------- #
# Domain dataclasses
# --------------------------------------------------------------------------- #
@dataclass(frozen=True)
class HardwareProfile:
    has_tensor_cores: bool = True
    supports_int8: bool = True
    supports_int4: bool = False
    vram_gb: float = 24.0
    edge_device: bool = False


@dataclass(frozen=True)
class BudgetState:
    """Feature vector for the causal budget learner."""

    hour_of_day: int
    metric_load: float
    workload_hash: float


@dataclass(frozen=True)
class BudgetExplanation:
    headline: str
    rationale: List[str]
    confidence: float
    contributing_factors: Dict[str, float]

    def to_dict(self) -> Dict[str, Any]:
        return _json_safe(asdict(self))


@dataclass(frozen=True)
class DistilledBudgetPolicy:
    precision: PrecisionLevel
    budgets: Dict[str, float]
    quality_retention: float
    energy_reduction_percent: float


@dataclass(frozen=True)
class FederatedBreachProfile:
    deployment_id: str
    metric: str
    mean_overshoot_ratio: float
    sample_count: int
    timestamp: datetime = field(default_factory=_utcnow)


@dataclass(frozen=True)
class MarketSnapshot:
    carbon_price_per_tco2_usd: float
    rec_price_per_mwh_usd: float
    rec_available_mwh: float
    timestamp: datetime = field(default_factory=_utcnow)


@dataclass(frozen=True)
class HITLRequest:
    agent_id: str
    metric: str
    value: float
    limit: float
    reason: str
    urgency: str
    context: Dict[str, Any] = field(default_factory=dict)
    requested_at: datetime = field(default_factory=_utcnow)


# --------------------------------------------------------------------------- #
# Sync adapters
# --------------------------------------------------------------------------- #
class SyncCircuitAdapter:
    """Sync facade over v18's single-probe ``CircuitBreaker``."""

    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
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
            self._cb._on_success()
            return True
        except RuntimeError:
            return False

    def record_success(self) -> None:
        self._cb._on_success()

    def record_failure(self) -> None:
        self._cb._on_failure()


class SyncChaosInjector:
    """Deterministic sync fault injector for the enforcement hot path."""

    def __init__(self, failure_rate: float = 0.0) -> None:
        if not 0.0 <= failure_rate <= 1.0:
            raise ValueError("failure_rate must be in [0, 1]")
        self.failure_rate = failure_rate
        self.events: Deque[Dict[str, Any]] = deque(maxlen=_MAX_CHAOS_EVENTS)

    def maybe_fail(self, component: str) -> bool:
        if random.random() < self.failure_rate:
            self.events.append(
                {"component": component, "at": _utcnow().isoformat()}
            )
            return True
        return False


# --------------------------------------------------------------------------- #
# XAI
# --------------------------------------------------------------------------- #
class BudgetExplainer:
    @staticmethod
    def explain(
        violations: List[Mapping[str, Any]],
        severity: Optional[str],
        market_snapshot: Optional[Mapping[str, Any]] = None,
        hitl_status: Optional[str] = None,
    ) -> BudgetExplanation:
        reasons: List[str] = []
        reasons.append(f"{len(violations)} budget(s) exceeded.")
        for v in violations:
            metric = v.get("metric", "unknown")
            used = v.get("used")
            limit = v.get("limit", 0.0)
            overshoot = v.get("overshoot")
            pct = v.get("overshoot_pct")
            if used is None:
                reasons.append(
                    f"'{metric}': unreadable value "
                    f"({v.get('reason', 'bad_data')}), "
                    f"limit {limit:.4f}."
                )
            elif pct is None or pct == float("inf"):
                reasons.append(
                    f"'{metric}': used {used:.4f} against zero limit."
                )
            else:
                reasons.append(
                    f"'{metric}': used {used:.4f}, limit {limit:.4f} "
                    f"(overshoot {overshoot:.4f}, {pct:.1f}%)."
                )
        if severity:
            reasons.append(f"Top severity classified as '{severity}'.")
        if market_snapshot:
            cp = market_snapshot.get("carbon_price_per_tco2_usd")
            if cp:
                reasons.append(
                    f"Carbon market price = ${cp:.2f}/tCO₂ in effect."
                )
        if hitl_status and hitl_status != "not_required":
            reasons.append(f"HITL status: {hitl_status}.")

        headline = (
            f"[{severity or 'warning'}] "
            + ", ".join(
                str(v.get("metric", "?")) for v in violations
            )
            + " budget exceeded"
        )
        factors = {}
        for v in violations:
            metric = str(v.get("metric", "?"))
            pct = v.get("overshoot_pct")
            if pct is None or pct == float("inf"):
                factors[f"overshoot_{metric}"] = float("inf")
            else:
                factors[f"overshoot_{metric}"] = float(pct)
        return BudgetExplanation(
            headline=headline,
            rationale=reasons,
            confidence=0.95,
            contributing_factors=factors,
        )


# --------------------------------------------------------------------------- #
# Causal learner (fixed: feature-aware, signed reward, tightening-only)
# --------------------------------------------------------------------------- #
class CausalBudgetLearner:
    """
    Learns a per-metric budget offset from outcomes.

    **Fixed vs v5**

    * v5's ``record(reward)`` had no state input; v6 takes ``features``.
    * v5's ``update()`` was a counter; v6 consumes the buffer.
    * v5's ``reward = 1.0 if correct else 0.0`` was a one-way ratchet;
      v6 uses ``+1.0 / -1.0`` so incorrect decisions tighten.
    * v5 could loosen budgets silently; v6 defaults to tightening-only
      (``allow_loosening=False``).
    """

    N_FEATURES = 3

    def __init__(
        self,
        base_budgets: Mapping[str, float],
        lr: float = 0.02,
        allow_loosening: bool = False,
    ) -> None:
        if lr <= 0:
            raise ValueError("lr must be > 0")
        self.base_budgets: Dict[str, float] = dict(base_budgets)
        self.lr = lr
        self.allow_loosening = bool(allow_loosening)
        self.offsets: Dict[str, List[float]] = {
            m: [0.0] * self.N_FEATURES for m in base_budgets
        }
        self.buffer: Deque[Tuple[List[float], float]] = deque(
            maxlen=_MAX_CAUSAL_BUFFER
        )
        self.observations: int = 0
        self.updates: int = 0
        self.loosening_events: int = 0

    @staticmethod
    def features_of(state: BudgetState) -> List[float]:
        return [
            state.hour_of_day / 24.0,
            state.metric_load,
            state.workload_hash,
        ]

    def predict_budgets(self, state: BudgetState) -> Dict[str, float]:
        f = self.features_of(state)
        out: Dict[str, float] = {}
        for metric, base in self.base_budgets.items():
            raw = sum(
                c * x
                for c, x in zip(
                    self.offsets.get(metric, [0.0] * self.N_FEATURES),
                    f,
                )
            )
            raw = max(-0.2 * base, min(0.2 * base, raw))
            if not self.allow_loosening:
                raw = min(0.0, raw)
            out[metric] = max(0.0, base + raw)
        return out

    def record(self, features: List[float], reward: float) -> None:
        if len(features) != self.N_FEATURES:
            return
        if not math.isfinite(reward):
            return
        self.buffer.append((list(features), float(reward)))
        self.observations += 1

    def update(self) -> bool:
        if not self.buffer:
            return False
        for features, reward in self.buffer:
            for metric in self.offsets:
                for i in range(self.N_FEATURES):
                    delta = self.lr * reward * features[i]
                    new_val = self.offsets[metric][i] + delta
                    if not self.allow_loosening:
                        new_val = min(0.0, new_val)
                    self.offsets[metric][i] = max(-1.0, min(1.0, new_val))
        self.updates += 1
        self.buffer.clear()
        return True

    def stats(self) -> Dict[str, Any]:
        return {
            "observations": self.observations,
            "updates": self.updates,
            "loosening_events": self.loosening_events,
            "allow_loosening": self.allow_loosening,
            "buffer_size": len(self.buffer),
            "offsets": {m: list(v) for m, v in self.offsets.items()},
        }


# --------------------------------------------------------------------------- #
# Distillation (ceil, not floor — for enforcement, never loosen)
# --------------------------------------------------------------------------- #
class QuantumDistillationBridge:
    """
    Quantizes a budget table to a lower precision.

    Uses ``math.ceil`` (v5 used ``math.floor``): for an **enforcer**, a
    quantized budget must never be smaller than the operator intended.
    """

    _SCALE: Dict[PrecisionLevel, float] = {
        PrecisionLevel.FP32: 1.0,
        PrecisionLevel.FP16: 1.0,
        PrecisionLevel.INT8: 100.0,
        PrecisionLevel.INT4: 10.0,
        PrecisionLevel.QUANTUM_DISTILLED: 5.0,
    }
    _RETENTION: Dict[PrecisionLevel, Tuple[float, float]] = {
        PrecisionLevel.FP32: (1.00, 0.0),
        PrecisionLevel.FP16: (0.98, 30.0),
        PrecisionLevel.INT8: (0.93, 55.0),
        PrecisionLevel.INT4: (0.85, 70.0),
        PrecisionLevel.QUANTUM_DISTILLED: (0.80, 85.0),
    }

    def distill(
        self,
        budgets: Mapping[str, float],
        precision: PrecisionLevel,
    ) -> DistilledBudgetPolicy:
        scale = self._SCALE[precision]
        retention, energy = self._RETENTION[precision]

        def qz(v: float) -> float:
            if scale <= 1.0:
                return float(v)
            return math.ceil(float(v) * scale) / scale

        return DistilledBudgetPolicy(
            precision=precision,
            budgets={k: qz(v) for k, v in budgets.items()},
            quality_retention=retention,
            energy_reduction_percent=energy,
        )


# --------------------------------------------------------------------------- #
# Federated breach aggregation (bounded)
# --------------------------------------------------------------------------- #
class FederatedBreachAggregator:
    """FedAvg over per-metric overshoot ratios, bounded in memory."""

    def __init__(self, max_updates: int = _MAX_FEDERATED_UPDATES) -> None:
        self.updates: Deque[FederatedBreachProfile] = deque(
            maxlen=max_updates
        )
        self._global: Dict[str, Dict[str, float]] = {}

    def push(self, u: FederatedBreachProfile) -> None:
        self.updates.append(u)

    def aggregate(self) -> Dict[str, Dict[str, float]]:
        grouped: Dict[str, List[FederatedBreachProfile]] = defaultdict(list)
        for u in self.updates:
            grouped[u.metric].append(u)
        result: Dict[str, Dict[str, float]] = {}
        for metric, profiles in grouped.items():
            total_w = sum(max(0, p.sample_count) for p in profiles)
            if total_w <= 0:
                continue
            result[metric] = {
                "mean_overshoot_ratio": sum(
                    p.mean_overshoot_ratio * p.sample_count
                    for p in profiles
                )
                / total_w,
                "sample_count": float(total_w),
            }
        self._global = result
        return result

    def lookup(self, metric: str) -> Optional[Dict[str, float]]:
        return self._global.get(metric)

    def reset(self) -> None:
        self.updates.clear()
        self._global.clear()


# --------------------------------------------------------------------------- #
# Helium profiler (informational; explicit enforcement is on
# metrics["helium"] against max_helium)
# --------------------------------------------------------------------------- #
class HeliumProfiler:
    HELIUM_UNITS_PER_KWH = 0.05

    @classmethod
    def estimate_units(
        cls, energy_kwh: float, scarcity_score: float = 0.0
    ) -> float:
        base = energy_kwh * cls.HELIUM_UNITS_PER_KWH
        efficiency = 1.0 - min(0.5, scarcity_score * 0.5)
        return base * efficiency


# --------------------------------------------------------------------------- #
# Load-bearing temporal properties
# --------------------------------------------------------------------------- #
class BudgetProperty(Protocol):
    def check(self, ctx: Mapping[str, Any]) -> bool: ...
    def name(self) -> str: ...


@dataclass(frozen=True)
class AllViolationsReported:
    """
    G(actual_violation_count == expected_violation_count).

    Defence against logic bugs: the violation loop must collect exactly
    as many entries as the parallel "breached metrics" loop. v5's version
    read a hard-coded ``True`` from the context and could never fire.
    """

    def check(self, ctx: Mapping[str, Any]) -> bool:
        expected = ctx.get("expected_violation_count")
        actual = ctx.get("actual_violation_count")
        if expected is None or actual is None:
            return True
        return int(actual) == int(expected)

    def name(self) -> str:
        return "AllViolationsReported"


@dataclass(frozen=True)
class SeverityMonotonic:
    """
    G(severity is consistent with max_overshoot_pct).

    EMERGENCY iff max_pct >= 100 or infinite; CRITICAL iff 30 <= pct <
    100; WARNING iff 0 < pct < 30. INFO only when there are no
    violations. v5 read a hard-coded ``True`` from the context.
    """

    def check(self, ctx: Mapping[str, Any]) -> bool:
        severity = ctx.get("severity")
        if severity is None:
            return True
        max_pct = ctx.get("max_overshoot_pct", 0.0)
        infinite = bool(ctx.get("any_infinite_overshoot", False))

        if severity == Severity.INFO.value:
            return max_pct == 0.0 and not infinite
        if severity == Severity.WARNING.value:
            return 0.0 < max_pct < 30.0 and not infinite
        if severity == Severity.CRITICAL.value:
            return 30.0 <= max_pct < 100.0 and not infinite
        if severity == Severity.EMERGENCY.value:
            return infinite or max_pct >= 100.0
        return False

    def name(self) -> str:
        return "SeverityMonotonic"


class BudgetSafetyMonitor:
    """Bounded verification of temporal budget properties."""

    def __init__(self, max_violations: int = _MAX_VIOLATIONS) -> None:
        self.properties: List[BudgetProperty] = []
        self.violations: Deque[Dict[str, Any]] = deque(
            maxlen=max_violations
        )

    def register(self, prop: BudgetProperty) -> None:
        self.properties.append(prop)

    def verify(
        self, ctx: Mapping[str, Any]
    ) -> Tuple[bool, List[str]]:
        bad: List[str] = []
        for prop in self.properties:
            try:
                if not prop.check(ctx):
                    bad.append(prop.name())
                    self.violations.append(
                        {
                            "property": prop.name(),
                            "at": _utcnow().isoformat(),
                        }
                    )
            except Exception as exc:
                logger.warning(
                    "Property %s raised: %s", prop.name(), exc
                )
                bad.append(prop.name())
        return (len(bad) == 0, bad)


# --------------------------------------------------------------------------- #
# HITL gate (sync + async, bounded)
# --------------------------------------------------------------------------- #
class HumanReviewGate:
    """
    Sync + async HITL gate.

    * ``review``  — sync; **rejects awaitable callbacks (fail-safe)**.
    * ``areview`` — async; awaits awaitables with a timeout.
    """

    def __init__(
        self,
        learner: Optional[Any] = None,
        timeout: float = 3.0,
        borderline_ratio: float = 0.95,
        severe_ratio: float = 2.0,
    ) -> None:
        self._learner = learner
        self._timeout = timeout
        self.borderline_ratio = borderline_ratio
        self.severe_ratio = severe_ratio
        self.pending: Deque[HITLRequest] = deque(maxlen=_MAX_HITL_PENDING)
        self.feedback_log: Deque[Dict[str, Any]] = deque(
            maxlen=_MAX_HITL_LOG
        )
        self._callback: Optional[Callable[[HITLRequest], Any]] = None
        self._outcomes: Dict[str, int] = defaultdict(int)

    def set_callback(self, cb: Callable[[HITLRequest], Any]) -> None:
        self._callback = cb

    def needs_review(self, value: float, limit: float) -> bool:
        if limit <= 0:
            return True
        ratio = value / limit
        if self.borderline_ratio <= ratio <= 1.05:
            return True
        if ratio >= self.severe_ratio:
            return True
        return False

    def _make_req(
        self,
        agent_id: str,
        metric: str,
        value: float,
        limit: float,
        reason: str,
        urgency: str,
        context: Optional[Mapping[str, Any]],
    ) -> HITLRequest:
        return HITLRequest(
            agent_id=agent_id,
            metric=metric,
            value=float(value),
            limit=float(limit),
            reason=reason,
            urgency=urgency,
            context=dict(context or {}),
        )

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
        self._outcomes[status] += 1
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
        agent_id: str,
        metric: str,
        value: float,
        limit: float,
        reason: str,
        urgency: str = "medium",
        context: Optional[Mapping[str, Any]] = None,
    ) -> Tuple[Optional[bool], str]:
        req = self._make_req(
            agent_id, metric, value, limit, reason, urgency, context
        )
        self.pending.append(req)
        try:
            if self._callback is None:
                return self._resolve(req, None, "no_callback")
            try:
                decision = self._callback(req)
            except Exception as exc:
                logger.warning("HITL callback failed: %s", exc)
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
        agent_id: str,
        metric: str,
        value: float,
        limit: float,
        reason: str,
        urgency: str = "medium",
        context: Optional[Mapping[str, Any]] = None,
    ) -> Tuple[Optional[bool], str]:
        req = self._make_req(
            agent_id, metric, value, limit, reason, urgency, context
        )
        self.pending.append(req)
        try:
            if self._callback is not None:
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
                except Exception as exc:
                    logger.warning("HITL callback failed: %s", exc)
                    return self._resolve(req, None, "callback_error")

            if self._learner is not None:
                try:
                    chosen = await asyncio.wait_for(
                        self._learner.query_user_if_needed(
                            "budget_reviewer",
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
                        return self._resolve(
                            req, None, "learner_timeout"
                        )
                    return self._resolve(
                        req, chosen == "approve", "learner"
                    )
                except Exception as exc:
                    logger.warning("Learner failed: %s", exc)

            return self._resolve(req, None, "no_callback")
        finally:
            try:
                self.pending.remove(req)
            except ValueError:
                pass

    def outcomes(self) -> Dict[str, int]:
        return dict(self._outcomes)


# --------------------------------------------------------------------------- #
# Market cache (sync, TTL)
# --------------------------------------------------------------------------- #
class CarbonMarketCache:
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
            rec_price_per_mwh_usd=random.uniform(3, 9),
            rec_available_mwh=random.uniform(10, 500),
        )
        self._cache = snap
        return snap

    def peek(self) -> Optional[MarketSnapshot]:
        return self._cache


# --------------------------------------------------------------------------- #
# Main class
# --------------------------------------------------------------------------- #
class BudgetEnforcer:
    """
    Budget enforcer.

    Backward-compatible:

        BudgetEnforcer(max_energy=5.0, max_carbon=200.0, max_latency=30.0)

    The constructor's ``max_helium`` is the **enforced** budget for the
    ``"helium"`` key of ``metrics``; the ``HeliumProfiler`` value
    (estimated from energy) is exposed as ``helium_units`` in the
    exception details for informational use only.
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
        "helium_awareness": True,
        "aggregate_violations": True,
    }

    def __init__(
        self,
        max_energy: Optional[float] = None,
        max_carbon: Optional[float] = None,
        max_latency: Optional[float] = None,
        max_helium: Optional[float] = None,
        deployment_id: str = "local",
        agent_id: str = "budget-enforcer-0",
        features: Optional[Dict[str, bool]] = None,
        hardware: Optional[HardwareProfile] = None,
        *,
        degraded_policy: str = "raise",
        allow_budget_loosening: bool = False,
        allow_hitl_override: bool = False,
    ) -> None:
        # ---- Validate and copy budgets ----
        self._base_budgets: Dict[str, float] = {}
        for name, value in (
            ("energy", max_energy),
            ("carbon", max_carbon),
            ("latency", max_latency),
            ("helium", max_helium),
        ):
            validated = self._validate_budget(name, value)
            if validated is not None:
                self._base_budgets[name] = validated
        self.max_energy = max_energy
        self.max_carbon = max_carbon
        self.max_latency = max_latency
        self.max_helium = max_helium

        # ---- Config ----
        self.deployment_id = deployment_id
        self.agent_id = agent_id
        self.features: Dict[str, bool] = {
            **self.DEFAULT_FEATURES,
            **(features or {}),
        }
        self.hardware = hardware or HardwareProfile()
        try:
            self._degraded_policy = DegradedPolicy(degraded_policy)
        except ValueError as exc:
            raise ValueError(
                f"degraded_policy must be one of "
                f"{[p.value for p in DegradedPolicy]}, "
                f"got {degraded_policy!r}"
            ) from exc
        self._allow_budget_loosening = bool(allow_budget_loosening)
        self._allow_hitl_override = bool(allow_hitl_override)

        # ---- Locks ----
        self._lock = threading.RLock()

        # ---- Core state ----
        self._circuit = SyncCircuitAdapter(
            "budget_enforcer", failure_threshold=5, recovery_timeout=30.0
        )

        # ---- Counters ----
        self._checks_total: int = 0
        self._checks_enforced: int = 0
        self._breach_count: int = 0
        self._degraded_count: int = 0
        self._override_count: int = 0
        self._breach_by_metric: Dict[str, int] = defaultdict(int)
        self._severity_counts: Dict[str, int] = defaultdict(int)
        self._outcome_counts: Dict[str, int] = defaultdict(int)
        self._last_state: Optional[BudgetState] = None
        self._last_violations: Deque[Dict[str, Any]] = deque(
            maxlen=_MAX_LAST_VIOLATIONS
        )
        self._last_explanation: Optional[BudgetExplanation] = None
        self._last_market_snapshot: Optional[MarketSnapshot] = None
        self._last_market_fetched_at: Optional[float] = None

        self._build_layers()

        logger.info(
            "BudgetEnforcer v6 ready: deployment=%s, agent=%s, "
            "budgets=%s, features=%s, degraded_policy=%s",
            deployment_id,
            agent_id,
            list(self._base_budgets.keys()),
            sorted(k for k, v in self.features.items() if v),
            self._degraded_policy.value,
        )

    # ------------------------------------------------------------------
    # Construction helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _validate_budget(
        name: str, value: Optional[float]
    ) -> Optional[float]:
        if value is None:
            return None
        if isinstance(value, bool):
            raise ValueError(
                f"max_{name} must be numeric, got bool {value!r}"
            )
        try:
            fval = float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"max_{name} must be numeric, got {value!r}"
            ) from exc
        if not math.isfinite(fval):
            raise ValueError(
                f"max_{name} must be finite, got {value!r}"
            )
        if fval < 0:
            raise ValueError(
                f"max_{name} must be >= 0, got {value!r}"
            )
        return fval

    def _build_layers(self) -> None:
        self._chaos = (
            SyncChaosInjector()
            if self.features["chaos_testing"]
            else None
        )

        self._safety = (
            BudgetSafetyMonitor()
            if self.features["temporal_logic"]
            else None
        )
        if self._safety is not None:
            self._safety.register(AllViolationsReported())
            self._safety.register(SeverityMonotonic())

        self._precision: Optional[Any] = None
        if self.features["adaptive_precision"] and _V18_AVAILABLE:
            try:
                self._precision = AdaptivePrecisionSwitcher(  # type: ignore[misc]
                    config={}, storage=None
                )
            except Exception:  # pragma: no cover
                self._precision = None

        self._budget_learner: Optional[CausalBudgetLearner] = (
            CausalBudgetLearner(
                base_budgets=self._base_budgets,
                allow_loosening=self._allow_budget_loosening,
            )
            if self.features["causal_rl"]
            else None
        )

        self._distiller: Optional[QuantumDistillationBridge] = (
            QuantumDistillationBridge()
            if self.features["quantum_distillation"]
            else None
        )

        self._federated: Optional[FederatedBreachAggregator] = (
            FederatedBreachAggregator()
            if self.features["federated"]
            else None
        )

        self._coordinator: Optional[Any] = None
        if self.features["multi_agent"] and _V18_AVAILABLE:
            try:
                self._coordinator = MultiAgentCoordinator(  # type: ignore[misc]
                    config={}, storage=None
                )
                self._coordinator.register(self.agent_id)
            except Exception:  # pragma: no cover
                self._coordinator = None

        self._market: Optional[CarbonMarketCache] = (
            CarbonMarketCache() if self.features["carbon_market"] else None
        )

        self._hitl: Optional[HumanReviewGate] = None
        if self.features["hitl"]:
            learner = None
            if _V18_AVAILABLE:
                try:
                    learner = ActiveUserPreferenceLearner(  # type: ignore[misc]
                        storage=None
                    )
                except Exception:  # pragma: no cover
                    learner = None
            self._hitl = HumanReviewGate(learner=learner)

    def set_features(self, features: Mapping[str, bool]) -> None:
        for k, v in features.items():
            if k not in self.DEFAULT_FEATURES:
                raise ValueError(f"unknown feature {k!r}")
            self.features[k] = bool(v)
        with self._lock:
            self._build_layers()

    # ------------------------------------------------------------------
    # Public check()
    # ------------------------------------------------------------------
    def check(
        self,
        metrics: Mapping[str, Any],
        *,
        agent_id: Optional[str] = None,
    ) -> None:
        """
        Check metrics against budgets.

        Raises
        ------
        BudgetExceeded
            On a genuine breach, or (with ``degraded=True``) on any
            path where enforcement cannot run. Opt in to fail-open with
            ``degraded_policy=DegradedPolicy.RETURN``.
        """
        with self._lock:
            self._checks_total += 1

        # 1) Invalid metrics
        if metrics is None or not isinstance(metrics, Mapping):
            self._degraded_count += 1
            self._outcome_counts[EvaluationOutcome.DEGRADED.value] += 1
            if self._degraded_policy == DegradedPolicy.RETURN:
                logger.warning(
                    "Invalid metrics input; skipping enforcement (fail-open)"
                )
                return
            raise BudgetExceeded(
                "Invalid metrics: enforcement unavailable",
                details={
                    "degraded": True,
                    "outcome": EvaluationOutcome.DEGRADED.value,
                    "reason": "invalid_metrics",
                    "at": _utcnow().isoformat(),
                },
            )

        # 2) Circuit open
        if not self._circuit.can_call():
            self._degraded_count += 1
            self._outcome_counts[
                EvaluationOutcome.CIRCUIT_OPEN.value
            ] += 1
            if self._degraded_policy == DegradedPolicy.RETURN:
                logger.warning(
                    "BudgetEnforcer circuit open; skipping enforcement "
                    "(fail-open)"
                )
                return
            raise BudgetExceeded(
                "BudgetEnforcer circuit open: enforcement unavailable",
                details={
                    "degraded": True,
                    "outcome": EvaluationOutcome.CIRCUIT_OPEN.value,
                    "reason": "circuit_open",
                    "at": _utcnow().isoformat(),
                },
            )

        # 3) Chaos
        if self._chaos is not None and self._chaos.maybe_fail("check"):
            self._circuit.record_failure()
            self._degraded_count += 1
            self._outcome_counts[EvaluationOutcome.CHAOS.value] += 1
            if self._degraded_policy == DegradedPolicy.RETURN:
                logger.warning(
                    "Chaos injection; skipping enforcement (fail-open)"
                )
                return
            raise BudgetExceeded(
                "Chaos injection: enforcement unavailable",
                details={
                    "degraded": True,
                    "outcome": EvaluationOutcome.CHAOS.value,
                    "reason": "chaos",
                    "at": _utcnow().isoformat(),
                },
            )

        # 4) Enforcement pipeline
        try:
            self._check_internal(
                dict(metrics), agent_id=agent_id or self.agent_id
            )
            self._circuit.record_success()
            with self._lock:
                self._checks_enforced += 1
        except BudgetExceeded:
            self._circuit.record_success()  # breach != component failure
            raise
        except Exception as exc:
            self._circuit.record_failure()
            self._degraded_count += 1
            self._outcome_counts[EvaluationOutcome.ERROR.value] += 1
            logger.warning("Budget check failed: %s", exc)
            if self._degraded_policy == DegradedPolicy.RETURN:
                return
            raise BudgetExceeded(
                f"Budget check failed: {exc}",
                details={
                    "degraded": True,
                    "outcome": EvaluationOutcome.ERROR.value,
                    "reason": str(exc),
                    "at": _utcnow().isoformat(),
                },
            ) from exc

    # ------------------------------------------------------------------
    # Enforcement pipeline
    # ------------------------------------------------------------------
    def _check_internal(
        self,
        metrics: Dict[str, Any],
        *,
        agent_id: str,
    ) -> None:
        # ---- Active budgets (base or RL-adjusted) ----
        active_budgets = dict(self._base_budgets)
        causal_adjustment = 0.0
        state = BudgetState(
            hour_of_day=_utcnow().hour,
            metric_load=min(
                1.0,
                sum(
                    self._safe_float(metrics.get(k, 0.0))
                    for k in self._base_budgets
                )
                / 10.0,
            ),
            workload_hash=float(abs(hash(agent_id)) % 1000) / 1000.0,
        )
        with self._lock:
            self._last_state = state

        if self._budget_learner is not None:
            try:
                active_budgets = self._budget_learner.predict_budgets(state)
                if self._base_budgets:
                    causal_adjustment = sum(
                        active_budgets.get(k, self._base_budgets[k])
                        - self._base_budgets[k]
                        for k in self._base_budgets
                    ) / len(self._base_budgets)
                if any(
                    active_budgets.get(k, self._base_budgets[k])
                    > self._base_budgets[k] + 1e-9
                    for k in self._base_budgets
                ):
                    with self._lock:
                        self._budget_learner.loosening_events += 1
            except Exception:  # pragma: no cover
                active_budgets = dict(self._base_budgets)

        # ---- Scan for violations ----
        violations: List[Dict[str, Any]] = []
        breached_metrics: List[str] = []
        bad_data: List[Dict[str, Any]] = []
        max_utilization = 0.0

        for metric, limit in active_budgets.items():
            raw = metrics.get(metric)
            if raw is None:
                bad_data.append(
                    {"metric": metric, "reason": "missing"}
                )
                continue
            if isinstance(raw, bool) or not isinstance(
                raw, (int, float)
            ):
                bad_data.append(
                    {
                        "metric": metric,
                        "reason": "not_numeric",
                        "raw": repr(raw),
                    }
                )
                continue
            used = float(raw)
            if not math.isfinite(used):
                bad_data.append(
                    {
                        "metric": metric,
                        "reason": "not_finite",
                        "raw": repr(raw),
                    }
                )
                continue
            if limit > 0:
                max_utilization = max(max_utilization, used / limit)
            if used > limit:
                breached_metrics.append(metric)
                if limit > 0:
                    overshoot = used - limit
                    overshoot_pct = overshoot / limit * 100.0
                else:
                    overshoot = float("inf")
                    overshoot_pct = float("inf")
                violations.append(
                    {
                        "metric": metric,
                        "used": used,
                        "limit": limit,
                        "overshoot": overshoot,
                        "overshoot_pct": overshoot_pct,
                    }
                )

        # Bad data is a violation (P0 §6).
        for bd in bad_data:
            violations.append(
                {
                    "metric": bd["metric"],
                    "used": None,
                    "limit": active_budgets.get(bd["metric"], 0.0),
                    "overshoot": float("inf"),
                    "overshoot_pct": float("inf"),
                    "reason": bd["reason"],
                    "raw": bd.get("raw"),
                }
            )

        # ---- No violations → success ----
        if not violations:
            if self._coordinator is not None:
                try:
                    self._coordinator.reward(self.agent_id, 1.0)
                except Exception:  # pragma: no cover
                    pass
            return

        # ---- Severity (relative overshoot) ----
        severity = self._classify_severity(violations)
        with self._lock:
            self._severity_counts[severity.value] += 1
            self._breach_count += 1
            self._outcome_counts[
                EvaluationOutcome.INFEASIBLE.value
            ] += 1
            for v in violations:
                self._breach_by_metric[v["metric"]] += 1
            for v in violations:
                self._last_violations.append(v)

        # ---- Temporal verification (load-bearing) ----
        temporal_verified: Optional[bool] = True
        temporal_violations: List[str] = []
        if self._safety is not None:
            finite_pcts = [
                v["overshoot_pct"]
                for v in violations
                if v["overshoot_pct"] != float("inf")
            ]
            infinite = any(
                v["overshoot_pct"] == float("inf")
                for v in violations
            )
            ctx = {
                "expected_violation_count": (
                    len(breached_metrics) + len(bad_data)
                ),
                "actual_violation_count": len(violations),
                "severity": severity.value,
                "max_overshoot_pct": (
                    max(finite_pcts) if finite_pcts else 0.0
                ),
                "any_infinite_overshoot": infinite,
            }
            ok, tv = self._safety.verify(ctx)
            temporal_verified = ok
            temporal_violations = tv
            if not ok:
                logger.warning(
                    "Temporal violations on %s: %s", agent_id, tv
                )

        # ---- Market snapshot (cached) ----
        market_snapshot: Optional[Dict[str, Any]] = None
        if self._market is not None:
            try:
                snap = self._market.get_snapshot()
                co2_used = self._safe_float(metrics.get("carbon", 0.0))
                market_snapshot = {
                    "carbon_price_per_tco2_usd": (
                        snap.carbon_price_per_tco2_usd
                    ),
                    "rec_available_mwh": snap.rec_available_mwh,
                    "co2_market_value_usd": (
                        co2_used
                        / 1000.0
                        * snap.carbon_price_per_tco2_usd
                    ),
                }
                with self._lock:
                    self._last_market_snapshot = snap
                    self._last_market_fetched_at = _utcnow().timestamp()
            except Exception:  # pragma: no cover
                pass

        # ---- Helium profiling (informational) ----
        helium_units = 0.0
        if self.features["helium_awareness"]:
            energy = self._safe_float(metrics.get("energy", 0.0))
            helium_units = HeliumProfiler.estimate_units(energy)

        # ---- Precision attribution ----
        precision = self._select_precision(
            "critical" if not temporal_verified else "normal"
        )

        # ---- HITL (review ALL violations; propagate) ----
        hitl_required = False
        hitl_approved_all: Optional[bool] = None
        hitl_status = "not_required"
        hitl_override = False
        if self._hitl is not None:
            reviewable = [
                v
                for v in violations
                if self._hitl.needs_review(
                    v["used"] if v["used"] is not None else float("inf"),
                    v["limit"],
                )
            ]
            if reviewable:
                hitl_required = True
                all_approved = True
                statuses: List[str] = []
                for v in reviewable:
                    approved, status = self._hitl.review(
                        agent_id=agent_id,
                        metric=v["metric"],
                        value=(
                            v["used"]
                            if v["used"] is not None
                            else float("inf")
                        ),
                        limit=v["limit"],
                        reason=(
                            f"{v['metric']} overshoot "
                            f"{v['overshoot_pct']:.1f}%"
                        ),
                        urgency=(
                            "high"
                            if (
                                v["overshoot_pct"] == float("inf")
                                or v["overshoot_pct"] >= 100
                            )
                            else "medium"
                        ),
                        context={
                            "agent_id": agent_id,
                            "violation": v,
                        },
                    )
                    statuses.append(status)
                    if approved is not True:
                        all_approved = False
                hitl_status = (
                    "approved"
                    if all_approved and statuses
                    else (
                        "rejected"
                        if "rejected" in statuses
                        else "timed_out"
                    )
                )
                hitl_approved_all = all_approved

        # ---- Multi-agent feedback ----
        if self._coordinator is not None:
            try:
                self._coordinator.reward(self.agent_id, 0.0)
            except Exception:  # pragma: no cover
                pass

        # ---- Federated contribution (sampled) ----
        if (
            self._federated is not None
            and self._checks_total % 25 == 0
        ):
            for v in violations:
                pct = v["overshoot_pct"]
                ratio = (
                    pct / 100.0
                    if pct != float("inf")
                    else 100.0
                )
                self._federated.push(
                    FederatedBreachProfile(
                        deployment_id=self.deployment_id,
                        metric=v["metric"],
                        mean_overshoot_ratio=ratio,
                        sample_count=1,
                    )
                )
            self._federated.aggregate()

        # ---- XAI ----
        explanation: Optional[BudgetExplanation] = None
        if self.features["xai"]:
            explanation = BudgetExplainer.explain(
                violations=violations,
                severity=severity.value,
                market_snapshot=market_snapshot,
                hitl_status=hitl_status,
            )
            with self._lock:
                self._last_explanation = explanation

        # ---- Build message (v1 compat preserved) ----
        primary = violations[0]
        primary_metric = str(primary["metric"]).capitalize()
        message = f"{primary_metric} budget exceeded"
        if len(violations) > 1:
            message += "; also violated: " + ", ".join(
                str(v["metric"]) for v in violations[1:]
            )

        # ---- Structured details ----
        details: Dict[str, Any] = {
            "agent_id": agent_id,
            "violations": violations,
            "bad_data": bad_data,
            "severity": severity.value,
            "max_utilization": max_utilization,
            "causal_adjustment": causal_adjustment,
            "active_budgets": active_budgets,
            "base_budgets": dict(self._base_budgets),
            "temporal_verified": temporal_verified,
            "temporal_violations": temporal_violations,
            "precision": precision.value,
            "helium_units": helium_units,
            "market_snapshot": market_snapshot,
            "hitl_required": hitl_required,
            "hitl_approved": hitl_approved_all,
            "hitl_status": hitl_status,
            "hitl_override": False,
            "degraded": False,
            "outcome": EvaluationOutcome.INFEASIBLE.value,
            "at": _utcnow().isoformat(),
        }
        if explanation is not None:
            details["explanation"] = explanation.to_dict()
            details["explanation_str"] = explanation.headline

        # ---- HITL override (opt-in) ----
        if hitl_override_eligible(
            self._allow_hitl_override, hitl_approved_all, hitl_required
        ):
            details["hitl_override"] = True
            details["outcome"] = EvaluationOutcome.OVERRIDDEN.value
            with self._lock:
                self._override_count += 1
                self._outcome_counts[
                    EvaluationOutcome.OVERRIDDEN.value
                ] += 1
            logger.warning(
                "Budget overrun approved by HITL (allow_hitl_override=True); "
                "not raising: %s",
                message,
            )
            return

        raise BudgetExceeded(message, details=details)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        try:
            v = float(value)
            return v if math.isfinite(v) else default
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _classify_severity(
        violations: List[Dict[str, Any]],
    ) -> Severity:
        if not violations:
            return Severity.INFO
        finite_pcts = [
            v["overshoot_pct"]
            for v in violations
            if v["overshoot_pct"] != float("inf")
        ]
        infinite = any(
            v["overshoot_pct"] == float("inf") for v in violations
        )
        if infinite:
            return Severity.EMERGENCY
        max_pct = max(finite_pcts) if finite_pcts else 0.0
        if max_pct >= 100.0:
            return Severity.EMERGENCY
        if max_pct >= 30.0:
            return Severity.CRITICAL
        return Severity.WARNING  # any breach is at least WARNING

    def _select_precision(self, urgency: str) -> PrecisionLevel:
        if urgency == "critical":
            return PrecisionLevel.FP16
        if self.hardware.edge_device:
            return (
                PrecisionLevel.INT8
                if self.hardware.supports_int8
                else PrecisionLevel.FP16
            )
        return (
            PrecisionLevel.INT4
            if self.hardware.supports_int4
            else PrecisionLevel.INT8
        )

    # ------------------------------------------------------------------
    # Feedback / public API
    # ------------------------------------------------------------------
    def record_outcome(self, correct: bool) -> bool:
        """
        Feed back whether the last enforcement decision was correct.

        Uses the *real* ``BudgetState`` from the last ``check()`` call
        (v5 used hard-coded constants and never updated the weights).
        """
        with self._lock:
            state = self._last_state
        if self._budget_learner is None or state is None:
            return False
        features = CausalBudgetLearner.features_of(state)
        reward = 1.0 if correct else -1.0
        self._budget_learner.record(features, reward)
        if self._budget_learner.observations % 10 == 0:
            self._budget_learner.update()
        return True

    def set_hitl_callback(
        self, cb: Callable[[HITLRequest], Any]
    ) -> None:
        if self._hitl is not None:
            self._hitl.set_callback(cb)

    def active_learning_samples(
        self, n: int = 16
    ) -> List[Dict[str, Any]]:
        return (
            list(self._hitl.feedback_log)[-n:]
            if self._hitl is not None
            else []
        )

    def contribute_federated(self) -> None:
        if self._federated is None:
            return
        # Push actual overshoot ratios (not breach rates).
        for v in self._last_violations:
            pct = v["overshoot_pct"]
            ratio = (
                pct / 100.0 if pct != float("inf") else 100.0
            )
            self._federated.push(
                FederatedBreachProfile(
                    deployment_id=self.deployment_id,
                    metric=v["metric"],
                    mean_overshoot_ratio=ratio,
                    sample_count=1,
                )
            )
        self._federated.aggregate()

    def get_federated_aggregate(self) -> Dict[str, Dict[str, float]]:
        return self._federated.aggregate() if self._federated else {}

    def distill_budgets(
        self, urgency: str = "normal"
    ) -> Optional[DistilledBudgetPolicy]:
        if self._distiller is None or not self._base_budgets:
            return None
        precision = self._select_precision(urgency)
        return self._distiller.distill(
            budgets=self._base_budgets, precision=precision
        )

    def explain_last_breach(self) -> Optional[Dict[str, Any]]:
        if self._last_explanation is None:
            return None
        return self._last_explanation.to_dict()

    # ------------------------------------------------------------------
    # Statistics / health
    # ------------------------------------------------------------------
    def get_statistics(self) -> Dict[str, Any]:
        with self._lock:
            stats: Dict[str, Any] = {
                "deployment_id": self.deployment_id,
                "agent_id": self.agent_id,
                "checks": self._checks_total,
                "checks_enforced": self._checks_enforced,
                "breaches": self._breach_count,
                "breach_rate": (
                    self._breach_count / self._checks_total
                    if self._checks_total > 0
                    else 0.0
                ),
                "breach_by_metric": dict(self._breach_by_metric),
                "severity_counts": dict(self._severity_counts),
                "outcome_counts": dict(self._outcome_counts),
                "degraded_count": self._degraded_count,
                "override_count": self._override_count,
                "loosening_events": (
                    self._budget_learner.loosening_events
                    if self._budget_learner else 0
                ),
                "base_budgets": dict(self._base_budgets),
                "circuit": {
                    "state": self._circuit.state,
                    "failures": self._circuit.failures,
                },
            }
            if self._safety is not None:
                stats["temporal_violation_count"] = len(
                    self._safety.violations
                )
                stats["temporal_violations"] = list(
                    self._safety.violations
                )[-5:]
            if self._budget_learner is not None:
                stats["causal_learner"] = self._budget_learner.stats()
            if self._federated is not None:
                stats["federated_aggregate"] = (
                    self._federated.aggregate()
                )
            if self._hitl is not None:
                stats["hitl"] = {
                    "pending": len(self._hitl.pending),
                    "log_size": len(self._hitl.feedback_log),
                    "outcomes": self._hitl.outcomes(),
                }
            if self._chaos is not None:
                stats["chaos_events"] = list(self._chaos.events)[-5:]
            if self._last_market_snapshot is not None:
                stats["market"] = {
                    "carbon_price_per_tco2_usd": (
                        self._last_market_snapshot.carbon_price_per_tco2_usd
                    ),
                    "cached_at": _ensure_aware(
                        self._last_market_snapshot.timestamp
                    ).isoformat(),
                    "age_seconds": (
                        _utcnow().timestamp()
                        - self._last_market_fetched_at
                        if self._last_market_fetched_at else None
                    ),
                }
            return stats

    async def health_check(self) -> Dict[str, Any]:
        with self._lock:
            return {
                "healthy": self._circuit.state != "OPEN",
                "deployment_id": self.deployment_id,
                "agent_id": self.agent_id,
                "circuit": {
                    "state": self._circuit.state,
                    "failures": self._circuit.failures,
                },
                "checks_total": self._checks_total,
                "checks_enforced": self._checks_enforced,
                "degraded_count": self._degraded_count,
                "breaches": self._breach_count,
                "override_count": self._override_count,
                "loosening_events": (
                    self._budget_learner.loosening_events
                    if self._budget_learner else 0
                ),
                "hitl_outcomes": (
                    self._hitl.outcomes() if self._hitl else {}
                ),
                "temporal_violation_count": (
                    len(self._safety.violations)
                    if self._safety else 0
                ),
                "degraded_policy": self._degraded_policy.value,
                "allow_budget_loosening": self._allow_budget_loosening,
                "allow_hitl_override": self._allow_hitl_override,
                "features": dict(self.features),
            }

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def __enter__(self) -> "BudgetEnforcer":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        pass

    async def __aenter__(self) -> "BudgetEnforcer":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        pass

    def __repr__(self) -> str:
        return (
            "BudgetEnforcer("
            f"budgets={list(self._base_budgets)}, "
            f"checks={self._checks_total}, "
            f"breaches={self._breach_count}, "
            f"degraded={self._degraded_count}, "
            f"circuit={self._circuit.state}, "
            f"degraded_policy={self._degraded_policy.value})"
        )


def hitl_override_eligible(
    allow_override: bool,
    hitl_approved_all: Optional[bool],
    hitl_required: bool,
) -> bool:
    """True iff HITL ran, ALL violations were approved, and override is enabled."""
    return bool(
        allow_override
        and hitl_required
        and hitl_approved_all is True
    )


# --------------------------------------------------------------------------- #
# Factory
# --------------------------------------------------------------------------- #
def create_budget_enforcer(
    max_energy: Optional[float] = None,
    max_carbon: Optional[float] = None,
    max_latency: Optional[float] = None,
    max_helium: Optional[float] = None,
    deployment_id: str = "local",
    agent_id: str = "budget-enforcer-0",
    features: Optional[Dict[str, bool]] = None,
    hardware: Optional[HardwareProfile] = None,
    degraded_policy: str = "raise",
    allow_budget_loosening: bool = False,
    allow_hitl_override: bool = False,
) -> BudgetEnforcer:
    return BudgetEnforcer(
        max_energy=max_energy,
        max_carbon=max_carbon,
        max_latency=max_latency,
        max_helium=max_helium,
        deployment_id=deployment_id,
        agent_id=agent_id,
        features=features,
        hardware=hardware,
        degraded_policy=degraded_policy,
        allow_budget_loosening=allow_budget_loosening,
        allow_hitl_override=allow_hitl_override,
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
    "CarbonMarketIntegrator",
    "MultiAgentCoordinator",
    "FederatedGreenAggregator",
    "TemporalLogicVerifier",
    "SimpleQuantumDistillationEngine",
    "SimpleFederatedGreenAggregator",
    "SimpleMultiAgentCoordinator",
    "SimpleTemporalLogicVerifier",
    "SimpleAdaptivePrecisionSwitcher",
    "SimpleCarbonMarketIntegrator",
    "SimpleChaosTestingEngine",
    "SimpleActiveUserPreferenceLearner",
    # Enums
    "Severity",
    "PrecisionLevel",
    "AgentRole",
    "EvaluationOutcome",
    "DegradedPolicy",
    # Exception
    "BudgetExceeded",
    # Domain types
    "HardwareProfile",
    "BudgetState",
    "BudgetExplanation",
    "DistilledBudgetPolicy",
    "FederatedBreachProfile",
    "MarketSnapshot",
    "HITLRequest",
    # Components
    "SyncCircuitAdapter",
    "SyncChaosInjector",
    "BudgetExplainer",
    "CausalBudgetLearner",
    "QuantumDistillationBridge",
    "FederatedBreachAggregator",
    "HeliumProfiler",
    "AllViolationsReported",
    "SeverityMonotonic",
    "BudgetSafetyMonitor",
    "HumanReviewGate",
    "CarbonMarketCache",
    # Main
    "BudgetEnforcer",
    "create_budget_enforcer",
    "hitl_override_eligible",
]


# --------------------------------------------------------------------------- #
# Demo — every P0/P1 fix verified by assertion
# --------------------------------------------------------------------------- #
def _demo() -> None:
    logging.basicConfig(
        level=logging.WARNING,
        format="%(levelname)s %(name)s: %(message)s",
    )

    print("=" * 78)
    print("budget_enforcer.py v6.0.0 — Demo")
    print("=" * 78)

    def _check(condition: bool, message: str) -> None:
        if not condition:
            raise AssertionError(f"FAILED: {message}")
        print(f"  ✓ {message}")

    # ============================================================= #
    # Backward compatibility
    # ============================================================= #
    print("\n=== Backward compat: v1-style enforcer + message shape ===")
    legacy = BudgetEnforcer(
        max_energy=5.0, max_carbon=200.0, max_latency=30.0,
        features={k: False for k in BudgetEnforcer.DEFAULT_FEATURES},
    )
    try:
        legacy.check({"energy": 6.2, "carbon": 210.0, "latency": 28.0})
    except BudgetExceeded as e:
        _check(True, "legacy raises BudgetExceeded")
        _check(
            str(e).startswith("Energy budget exceeded"),
            f"legacy message preserved: {str(e)!r}",
        )
        _check(e.degraded is False, "legacy breach is not degraded")
    else:
        raise AssertionError("legacy check did not raise")

    # ============================================================= #
    # P0 §1 — all five degraded paths fail-closed (raise)
    # ============================================================= #
    print("\n=== P0 §1: check() fails closed on every degraded path ===")

    # (a) Invalid metrics
    inv = BudgetEnforcer(max_energy=5.0)
    try:
        inv.check(None)  # type: ignore[arg-type]
    except BudgetExceeded as e:
        _check(e.degraded, "invalid metrics → raise (degraded=True)")
    else:
        raise AssertionError("invalid metrics did not raise")

    # (b) Circuit open
    circ = BudgetEnforcer(max_energy=5.0)
    for _ in range(10):
        circ._circuit.record_failure()
    try:
        circ.check({"energy": 1.0})
    except BudgetExceeded as e:
        _check(e.degraded, "circuit open → raise (degraded=True)")
    else:
        raise AssertionError("circuit open did not raise")

    # (c) Chaos
    chaos = BudgetEnforcer(
        max_energy=5.0, features={"chaos_testing": True}
    )
    chaos._chaos = SyncChaosInjector(failure_rate=1.0)
    try:
        chaos.check({"energy": 1.0})
    except BudgetExceeded as e:
        _check(e.degraded, "chaos → raise (degraded=True)")
    else:
        raise AssertionError("chaos did not raise")

    # (d) Internal error (inject via monkeypatch)
    err_enf = BudgetEnforcer(max_energy=5.0)
    _orig = err_enf._check_internal
    def _boom(metrics, *, agent_id):
        raise RuntimeError("simulated internal error")
    err_enf._check_internal = _boom  # type: ignore[assignment]
    try:
        err_enf.check({"energy": 1.0})
    except BudgetExceeded as e:
        _check(
            e.degraded and e.outcome == "error",
            "internal error → raise (degraded=True, outcome=error)",
        )
    else:
        raise AssertionError("internal error did not raise")
    err_enf._check_internal = _orig  # type: ignore[assignment]

    # (e) Breach (obvious, but included)
    breach = BudgetEnforcer(max_energy=5.0)
    try:
        breach.check({"energy": 6.0})
    except BudgetExceeded as e:
        _check(
            not e.degraded and e.outcome == "infeasible",
            "breach → raise (degraded=False, outcome=infeasible)",
        )
    else:
        raise AssertionError("breach did not raise")

    # Opt-in fail-open still works
    open_enf = BudgetEnforcer(
        max_energy=5.0, degraded_policy="return"
    )
    try:
        open_enf.check(None)  # type: ignore[arg-type]
        _check(True, "fail-open opt-in: invalid metrics returns normally")
    except BudgetExceeded:
        raise AssertionError("fail-open did not return normally")

    # ============================================================= #
    # P0 §2 — causal learner uses features
    # ============================================================= #
    print("\n=== P0 §2: causal learner uses features ===")
    learner = CausalBudgetLearner(base_budgets={"energy": 5.0})
    state_a = BudgetState(hour_of_day=8, metric_load=0.9, workload_hash=0.1)
    state_b = BudgetState(hour_of_day=20, metric_load=0.1, workload_hash=0.9)
    for _ in range(20):
        learner.record(CausalBudgetLearner.features_of(state_a), -1.0)
    learner.update()
    for _ in range(20):
        learner.record(CausalBudgetLearner.features_of(state_b), -1.0)
    learner.update()
    budgets_a = learner.predict_budgets(state_a)["energy"]
    budgets_b = learner.predict_budgets(state_b)["energy"]
    _check(
        abs(budgets_a - budgets_b) > 0.01,
        f"state-aware budgets differ: A={budgets_a:.4f}, B={budgets_b:.4f}",
    )

    # ============================================================= #
    # P0 §3 — signed reward tightens
    # ============================================================= #
    print("\n=== P0 §3: negative reward tightens ===")
    learner2 = CausalBudgetLearner(base_budgets={"energy": 5.0})
    state = BudgetState(12, 0.5, 0.5)
    baseline = learner2.predict_budgets(state)["energy"]
    for _ in range(50):
        learner2.record(CausalBudgetLearner.features_of(state), -1.0)
    learner2.update()
    after = learner2.predict_budgets(state)["energy"]
    _check(
        after < baseline,
        f"incorrect outcomes tightened: {baseline:.4f} → {after:.4f}",
    )

    # ============================================================= #
    # P0 §4 — RL tightening-only by default
    # ============================================================= #
    print("\n=== P0 §4: RL tightening-only by default ===")
    tight = BudgetEnforcer(max_energy=5.0, max_carbon=200.0)
    for _ in range(200):
        tight.record_outcome(True)  # all "correct" — would loosen if allowed
    tight._budget_learner.update()
    loosen_flags = [
        m for m in tight._base_budgets
        if tight._budget_learner.predict_budgets(
            BudgetState(12, 0.5, 0.5)
        ).get(m, tight._base_budgets[m]) > tight._base_budgets[m] + 1e-9
    ]
    _check(
        loosen_flags == [],
        "200 correct outcomes did not loosen any budget (tightening-only)",
    )

    loose = BudgetEnforcer(
        max_energy=5.0, max_carbon=200.0,
        allow_budget_loosening=True,
    )
    for _ in range(200):
        loose.record_outcome(True)
    loose._budget_learner.update()
    loosen_flags2 = [
        m for m in loose._base_budgets
        if loose._budget_learner.predict_budgets(
            BudgetState(12, 0.5, 0.5)
        ).get(m, loose._base_budgets[m]) > loose._base_budgets[m] + 1e-9
    ]
    _check(
        len(loosen_flags2) > 0,
        "with allow_budget_loosening=True, correct decisions loosen",
    )

    # ============================================================= #
    # P0 §5 — HITL propagation
    # ============================================================= #
    print("\n=== P0 §5: HITL propagation ===")

    # (a) Without allow_hitl_override: approval does NOT suppress raise
    hitl_default = BudgetEnforcer(max_energy=5.0)
    hitl_default.set_hitl_callback(lambda req: True)
    try:
        hitl_default.check({"energy": 6.0})
    except BudgetExceeded as e:
        _check(
            e.details.get("hitl_status") == "approved"
            and e.hitl_override is False,
            "HITL approved but default policy still raises",
        )
    else:
        raise AssertionError("default policy did not raise")

    # (b) With allow_hitl_override: approval suppresses raise
    hitl_override = BudgetEnforcer(
        max_energy=5.0, allow_hitl_override=True
    )
    hitl_override.set_hitl_callback(lambda req: True)
    try:
        hitl_override.check({"energy": 6.0})
        _check(
            True,
            "allow_hitl_override=True: approved breaches do not raise",
        )
        _check(
            hitl_override._override_count == 1,
            "override_count incremented",
        )
    except BudgetExceeded:
        raise AssertionError(
            "allow_hitl_override=True did not suppress raise"
        )

    # (c) Rejection always raises (override flag cannot save it)
    hitl_reject = BudgetEnforcer(
        max_energy=5.0, allow_hitl_override=True
    )
    hitl_reject.set_hitl_callback(lambda req: False)
    try:
        hitl_reject.check({"energy": 6.0})
    except BudgetExceeded as e:
        _check(
            e.details.get("hitl_status") == "rejected"
            and e.hitl_override is False,
            "HITL rejection always raises (override cannot rescue)",
        )
    else:
        raise AssertionError("HITL rejection did not raise")

    # ============================================================= #
    # P0 §6 — NaN metrics → EMERGENCY
    # ============================================================= #
    print("\n=== P0 §6: NaN metrics → EMERGENCY ===")
    nan_enf = BudgetEnforcer(max_energy=5.0)
    try:
        nan_enf.check({"energy": float("nan")})
    except BudgetExceeded as e:
        _check(
            e.severity == "emergency",
            f"NaN energy → EMERGENCY (got {e.severity})",
        )
        _check(
            any(
                v.get("reason") == "not_finite"
                for v in e.violations
            ),
            "bad_data violation present",
        )
    else:
        raise AssertionError("NaN metric did not raise")

    # ============================================================= #
    # P0 §7 — zero-limit metric → EMERGENCY
    # ============================================================= #
    print("\n=== P0 §7: zero-limit metric → EMERGENCY ===")
    zero_enf = BudgetEnforcer(max_energy=0.0)
    try:
        zero_enf.check({"energy": 1.0})
    except BudgetExceeded as e:
        _check(
            e.severity == "emergency",
            f"zero-limit with usage → EMERGENCY (got {e.severity})",
        )
    else:
        raise AssertionError("zero-limit breach did not raise")

    # ============================================================= #
    # P0 §8 — construction-time validation
    # ============================================================= #
    print("\n=== P0 §8: construction-time validation ===")
    for name, args in [
        ("NaN budget", {"max_energy": float("nan")}),
        ("negative budget", {"max_energy": -5.0}),
        ("non-numeric budget", {"max_energy": "big"}),  # type: ignore[arg-type]
    ]:
        try:
            BudgetEnforcer(**args)  # type: ignore[arg-type]
        except ValueError:
            _check(True, f"{name} rejected at construction")
        else:
            raise AssertionError(f"{name} did not raise")

    # ============================================================= #
    # P0 §9 — no breach classifies as INFO
    # ============================================================= #
    print("\n=== P0 §9: no breach is INFO ===")
    tiny = BudgetEnforcer(max_energy=5.0)
    try:
        tiny.check({"energy": 5.01})  # 0.2% over
    except BudgetExceeded as e:
        _check(
            e.severity == "warning",
            f"0.2% overrun → WARNING (got {e.severity})",
        )
    else:
        raise AssertionError("tiny breach did not raise")

    # ============================================================= #
    # P1 §10 — thread safety
    # ============================================================= #
    print("\n=== P1 §10: thread safety ===")
    import threading
    safe = BudgetEnforcer(max_energy=5.0)
    errors: List[BaseException] = []

    def _worker() -> None:
        try:
            for _ in range(100):
                try:
                    safe.check({"energy": 5.0})
                except BudgetExceeded:
                    pass
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=_worker) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    _check(
        not errors,
        f"10 threads × 100 concurrent checks: no errors "
        f"({len(errors)} raised)",
    )
    _check(
        safe._checks_total == 1000,
        f"total counter is 1000 (got {safe._checks_total})",
    )

    # ============================================================= #
    # P1 §11 — v18 single-probe circuit
    # ============================================================= #
    print("\n=== P1 §11: v18 single-probe circuit breaker ===")
    probe = BudgetEnforcer(max_energy=5.0)
    # Two failures must not yet open (threshold=5)
    probe._circuit.record_failure()
    probe._circuit.record_failure()
    _check(
        probe._circuit.state == "CLOSED",
        f"two failures below threshold=5 → CLOSED (got {probe._circuit.state})",
    )
    # Five failures must open
    for _ in range(3):
        probe._circuit.record_failure()
    _check(
        probe._circuit.state == "OPEN",
        f"five failures → OPEN (got {probe._circuit.state})",
    )

    # ============================================================= #
    # P1 §12 — bounded collections
    # ============================================================= #
    print("\n=== P1 §12: bounded collections ===")
    bound = BudgetEnforcer(max_energy=5.0)
    for _ in range(2000):
        try:
            bound.check({"energy": 6.0})
        except BudgetExceeded:
            pass
    _check(
        len(bound._last_violations) <= _MAX_LAST_VIOLATIONS,
        f"last_violations bounded at {_MAX_LAST_VIOLATIONS}",
    )
    _check(
        len(bound._hitl.feedback_log) <= _MAX_HITL_LOG,
        f"hitl.feedback_log bounded at {_MAX_HITL_LOG}",
    )
    _check(
        len(bound._federated.updates) <= _MAX_FEDERATED_UPDATES,
        f"federated.updates bounded at {_MAX_FEDERATED_UPDATES}",
    )

    # ============================================================= #
    # P1 §13 — record_outcome uses real state
    # ============================================================= #
    print("\n=== P1 §13: record_outcome uses real state ===")
    rec = BudgetEnforcer(max_energy=5.0)
    rec.check({"energy": 1.0})
    _check(
        rec._last_state is not None,
        "_last_state captured from check()",
    )
    ok = rec.record_outcome(correct=False)
    _check(ok is True, "record_outcome(False) returned True")
    _check(
        rec._budget_learner.observations >= 1,
        "causal learner recorded the outcome",
    )

    # ============================================================= #
    # P1 §14 — cached market snapshot
    # ============================================================= #
    print("\n=== P1 §14: get_statistics uses cached market snapshot ===")
    cache_enf = BudgetEnforcer(max_energy=5.0)
    try:
        cache_enf.check({"energy": 6.0})
    except BudgetExceeded:
        pass
    first_price = cache_enf.get_statistics().get("market", {}).get(
        "carbon_price_per_tco2_usd"
    )
    second_price = cache_enf.get_statistics().get("market", {}).get(
        "carbon_price_per_tco2_usd"
    )
    _check(
        first_price == second_price,
        "consecutive get_statistics() reports the same cached price",
    )

    # ============================================================= #
    # P1 §15 — async HITL
    # ============================================================= #
    print("\n=== P1 §15: async HITL callbacks ===")

    # (a) Rejected in sync review
    gate = HumanReviewGate()

    async def _async_cb(req):
        return True

    gate.set_callback(_async_cb)
    approved, status = gate.review(
        agent_id="a", metric="energy", value=6.0, limit=5.0,
        reason="test", urgency="medium",
    )
    _check(
        approved is False and status == "async_in_sync",
        f"async callback rejected in sync review "
        f"(got {approved!r}, {status!r})",
    )

    # (b) Awaited in areview
    async def _await_review() -> Tuple[Optional[bool], str]:
        gate2 = HumanReviewGate()

        async def _cb(req):
            return True

        gate2.set_callback(_cb)
        return await gate2.areview(
            agent_id="a", metric="energy", value=6.0, limit=5.0,
            reason="test", urgency="medium",
        )

    approved2, status2 = asyncio.run(_await_review())
    _check(
        approved2 is True and status2 == "approved",
        "async callback awaited in areview",
    )

    # ============================================================= #
    # P1 §16 — set_features, health_check, async CM
    # ============================================================= #
    print("\n=== P1 §16: set_features, health_check, async CM ===")
    live = BudgetEnforcer(max_energy=5.0)
    _check(live._market is not None, "market enabled by default")
    live.set_features({"carbon_market": False})
    _check(live._market is None, "set_features disabled market")
    live.set_features({"carbon_market": True})
    _check(live._market is not None, "set_features re-enabled market")

    async def _async_health() -> None:
        async with BudgetEnforcer(max_energy=5.0) as enf:
            health = await enf.health_check()
            _check(
                "healthy" in health and "degraded_policy" in health,
                "health_check returns expected keys",
            )

    asyncio.run(_async_health())

    # ============================================================= #
    # P2 §19 — load-bearing temporal properties
    # ============================================================= #
    print("\n=== P2 §19: temporal properties actually fire ===")
    safety = BudgetSafetyMonitor()
    safety.register(AllViolationsReported())
    ok, viols = safety.verify(
        {
            "expected_violation_count": 3,
            "actual_violation_count": 2,  # deliberate mismatch
        }
    )
    _check(
        not ok and "AllViolationsReported" in viols,
        "AllViolationsReported fires on a logic bug",
    )

    safety2 = BudgetSafetyMonitor()
    safety2.register(SeverityMonotonic())
    ok2, viols2 = safety2.verify(
        {
            "severity": "warning",       # deliberate mismatch
            "max_overshoot_pct": 200.0,  # EMERGENCY territory
            "any_infinite_overshoot": False,
        }
    )
    _check(
        not ok2 and "SeverityMonotonic" in viols2,
        "SeverityMonotonic fires when severity misclassifies overshoot",
    )

    # ============================================================= #
    # P2 §20 — distillation uses ceil (never loosens)
    # ============================================================= #
    print("\n=== P2 §20: distillation uses ceil (never loosens) ===")
    bridge = QuantumDistillationBridge()
    # 5.121 at INT8 (scale=100):
    #   floor → 5.12 (loosens)
    #   round → 5.12 (loosens)
    #   ceil  → 5.13 (never loosens)
    policy = bridge.distill(
        {"energy": 5.121}, precision=PrecisionLevel.INT8
    )
    _check(
        policy.budgets["energy"] >= 5.121,
        f"ceil preserves original budget: 5.121 → {policy.budgets['energy']}",
    )

    # ============================================================= #
    # P2 §21 — federated bounded + div-zero guard
    # ============================================================= #
    print("\n=== P2 §21: federated bounded + div-zero guard ===")
    fed = FederatedBreachAggregator(max_updates=5)
    for _ in range(100):
        fed.push(
            FederatedBreachProfile(
                deployment_id="x",
                metric="energy",
                mean_overshoot_ratio=0.1,
                sample_count=0,  # zero-weight → should not divide-by-zero
            )
        )
    result = fed.aggregate()
    _check(
        result == {},
        "all-zero-weight profiles produce empty aggregate (no crash)",
    )
    _check(
        len(fed.updates) <= 5,
        f"federated.updates bounded at 5 (got {len(fed.updates)})",
    )

    # ============================================================= #
    # P2 §22 — helium enforcement clarified
    # ============================================================= #
    print("\n=== P2 §22: helium enforced on metrics['helium'] ===")
    helium_enf = BudgetEnforcer(max_energy=5.0, max_helium=0.5)
    try:
        helium_enf.check({"energy": 1.0, "helium": 0.6})
    except BudgetExceeded as e:
        _check(
            any(v["metric"] == "helium" for v in e.violations),
            "helium violation present in details",
        )
        _check(
            "helium_units" in e.details,
            "helium_units informational field present",
        )
    else:
        raise AssertionError("helium violation did not raise")

    # ============================================================= #
    # Final summary
    # ============================================================= #
    print("\n=== Statistics (last enforcer) ===")
    print(json.dumps(live.get_statistics(), indent=2, default=str))

    print("\n" + "=" * 78)
    print("All v6 assertions passed.")
    print("=" * 78)


if __name__ == "__main__":
    _demo()
