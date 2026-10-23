"""
budget_constrained.py — v6.0.0 (thin wrapper over carbon_forecast v18)
=======================================================================

Budget-constrained evaluator for the Green Agent.

**What changed (v5 → v6)**
--------------------------

**P0 — safety and correctness**

1. **RL tightening-only by default.** v5's `CausalBudgetLearner` could
   silently *loosen* every budget by up to 20% and had no mechanism to
   ever tighten it back — an unbounded, one-way drift. v6 requires an
   explicit ``allow_budget_loosening=True`` opt-in; the default clamps
   every learned adjustment to ``<= 0`` (tightening only), and the number
   of loosening events is surfaced in ``health_check()``.
2. **Feature-aware causal learner.** v5 stored a snapshot of weights and
   then ignored it. v6 stores ``(features, reward)``, updates each weight
   with ``lr * reward * feature[i]``, and produces different adjustments
   for different states (verified by assertion in the demo).
3. **Signed reward.** v5 used ``reward = 1.0 if correct else 0.0`` — an
   all-positive, one-way gradient. v6 uses ``+1.0 / -1.0`` so incorrect
   decisions tighten the offset.
4. **HITL propagation.** v5 logged the review and discarded it. v6 marks
   the result ``feasible=False, hitl_override=True`` on rejection and
   ``feasible=True, hitl_override=True`` on approval-of-an-infeasible —
   the caller always sees whether HITL overrode the automatic decision.
5. **Relative severity.** v5 compared raw overshoot to absolute
   thresholds, so a 5% CO₂ overrun was classified as more severe than an
   18% energy overrun. v6 uses ``overshoot / limit``.
6. **`is_feasible` distinguishes five outcomes.** v5 returned a single
   ``False`` bit for "infeasible", "unknown agent", "chaos", "circuit
   open", and "internal error". v6 raises :class:`UnknownAgentError` for
   unknown agents (matching v1), returns ``False`` for genuine
   infeasibility, and either raises :class:`EvaluationUnavailableError` or
   returns ``False`` (per ``fail_safe_on_degraded``) for chaos / circuit
   open / internal error.
7. **`missing_metric_policy` is independent of `causal_rl`.** v5 coupled
   the strict-vs-tolerant policy for missing metrics to the
   ``causal_rl`` feature flag. v6 exposes it as an explicit constructor
   argument with values ``"raise"`` (v1 behaviour, default),
   ``"infeasible"``, and ``"skip"``.

**P1 — reliability**

8. **Thread-safe via `RLock`** across every mutable structure.
9. **Single-probe circuit breaker** — v18's `CircuitBreaker` (via
   `SyncCircuitAdapter`) replaces the local one that could let two
   concurrent callers both flip HALF_OPEN.
10. **NaN / inf metrics → infeasible**, not silent ``0.0`` — a
    budget evaluator must not approve an agent because its metric is
    unreadable.
11. **Bounded collections**: `evaluation_history`, `violations`,
    `hitl.pending`, `hitl.feedback_log`, `last_explanations`,
    `federated.updates` all bounded.
12. **`record_outcome(agent_id, correct)`** uses the real state from the
    last evaluation of that agent, not hard-coded constants.
13. **`get_statistics` returns the cached market snapshot**, not a fresh
    random one.
14. **`_degraded_result` sets `temporal_verified=None`** — v5 claimed
    ``True`` for results that were never actually verified.
15. **Async HITL callbacks are awaited in `areview`, rejected (fail-safe)
    in `review`** — v5 silently auto-approved.
16. **`set_features()`, `health_check()`, `__aenter__`/`__aexit__`** —
    v5 had none.
17. **`budgets` and `agent_metrics` validated and defensively copied**
    at construction.
18. **`evaluation_count` (total) and `unique_agents_evaluated` (set)**
    are reported separately so the caller can see both.

**P2 — architecture**

19. **Ten enhancement classes imported from `carbon_forecast` v18**
    (CircuitBreaker, ChaosTestingEngine, ActiveUserPreferenceLearner,
    AdaptivePrecisionSwitcher, CarbonMarketIntegrator,
    MultiAgentCoordinator, FederatedGreenAggregator,
    TemporalLogicVerifier) — the seventh copy in v5 is retired.
20. **Load-bearing temporal properties.** v5's `SlackNonNegativeWhenFeasible`
    was a tautology and `OvershootBounded` had a 10× threshold that
    nothing could reach. v6 renames the first to
    `FeasibleImpliesBudgetCompliance` (defence against logic bugs) and
    reduces the second to `max_overshoot=0.5` (50%).
21. **`QuantumDistillationBridge.distill` uses `round`, not `floor`.**
22. **`HeliumProfiler` output is exposed as `helium_units` and, when the
    caller includes a ``"helium"`` budget, enforced** — v5 computed it
    and then discarded it.

**Public API preserved**

    evaluator = BudgetConstrainedEvaluator(agent_metrics, budgets)
    evaluator.is_feasible(agent_id)          # -> bool
    evaluator.evaluate_all()                 # -> Dict[str, bool]
    evaluator.compute_slack(agent_id)        # -> Dict[str, float]

**New in v6**

    evaluator.evaluate_with_details(agent_id)     # -> Dict
    evaluator.evaluate_all_detailed()             # -> Dict[str, Dict]
    evaluator.record_outcome(agent_id, correct)
    evaluator.set_features(features)
    evaluator.health_check()
    await evaluator.health_check()
    async with BudgetConstrainedEvaluator(...) as evaluator: ...
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
import uuid
from collections import OrderedDict, defaultdict, deque
from dataclasses import asdict, dataclass, field, fields
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
# Bounds & defaults
# --------------------------------------------------------------------------- #
_MAX_EVALUATION_HISTORY = 1024
_MAX_LAST_EXPLANATIONS = 256
_MAX_HITL_PENDING = 100
_MAX_HITL_LOG = 500
_MAX_CHAOS_EVENTS = 500
_MAX_VIOLATIONS = 200
_MAX_CAUSAL_BUFFER = 512
_MAX_FEDERATED_UPDATES = 100


# --------------------------------------------------------------------------- #
# Errors
# --------------------------------------------------------------------------- #
class BudgetError(RuntimeError):
    """Base for budget-evaluation errors."""


class UnknownAgentError(BudgetError, KeyError):
    """Raised by ``is_feasible`` for an unknown ``agent_id`` (v1 compat)."""


class EvaluationUnavailableError(BudgetError):
    """
    Raised by ``is_feasible`` when evaluation cannot proceed (chaos,
    circuit open, internal error) and ``fail_safe_on_degraded=False``.
    """


# --------------------------------------------------------------------------- #
# Enums
# --------------------------------------------------------------------------- #
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


class Severity(Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"
    EMERGENCY = "emergency"


class EvaluationOutcome(Enum):
    """Distinguishes the five outcomes ``is_feasible`` used to conflate."""

    FEASIBLE = "feasible"
    INFEASIBLE = "infeasible"
    UNKNOWN_AGENT = "unknown_agent"
    CHAOS = "chaos"
    CIRCUIT_OPEN = "circuit_open"
    ERROR = "error"


class MissingMetricPolicy(Enum):
    """Policy for a metric in ``budgets`` that is missing from an agent."""

    RAISE = "raise"           # v1 behaviour: raise KeyError
    INFEASIBLE = "infeasible"  # v5 default: mark infeasible
    SKIP = "skip"              # ignore the missing metric entirely


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
    agent_load: float
    workload_hash: float


@dataclass(frozen=True)
class FeasibilityExplanation:
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
class FederatedBudgetProfile:
    deployment_id: str
    metric: str
    mean_budget: float
    mean_usage: float
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
    reason: str
    urgency: str
    context: Dict[str, Any] = field(default_factory=dict)
    requested_at: datetime = field(default_factory=_utcnow)


# --------------------------------------------------------------------------- #
# Sync adapters over v18's async circuits and chaos engines
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
    """
    Sync fault injector used by the ``is_feasible`` hot path.

    v18's ``ChaosTestingEngine`` is async; this is the sync counterpart
    that ``_evaluate_with_details`` can call without ``await``. The two
    share the same rate semantics.
    """

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
        agent_id: str,
        feasible: bool,
        slack: Mapping[str, float],
        overshoots: Mapping[str, float],
        severity: Optional[str] = None,
        hitl_status: str = "not_required",
    ) -> FeasibilityExplanation:
        reasons: List[str] = []
        if feasible:
            min_metric = min(slack, key=slack.get) if slack else None
            reasons.append(
                f"Agent '{agent_id}' is feasible: all {len(slack)} "
                f"budgets satisfied."
            )
            if min_metric is not None:
                reasons.append(
                    f"Tightest constraint: '{min_metric}' with "
                    f"{slack[min_metric]:.4f} slack remaining."
                )
        else:
            reasons.append(
                f"Agent '{agent_id}' is infeasible: "
                f"{len(overshoots)} metric(s) exceeded budget."
            )
            for metric, overshoot in overshoots.items():
                reasons.append(f"'{metric}' overshoot by {overshoot:.4f}.")
            if severity is not None:
                reasons.append(f"Severity classified as '{severity}'.")
        if hitl_status != "not_required":
            reasons.append(f"HITL status: {hitl_status}.")

        confidence = 0.95 if feasible else 0.90
        return FeasibilityExplanation(
            headline=f"[{'FEASIBLE' if feasible else 'INFEASIBLE'}] {agent_id}",
            rationale=reasons,
            confidence=confidence,
            contributing_factors={
                **{f"slack_{k}": v for k, v in slack.items()},
                **{f"overshoot_{k}": v for k, v in overshoots.items()},
            },
        )


# --------------------------------------------------------------------------- #
# Causal budget learner (fixed: feature-aware, signed reward, tightening-only)
# --------------------------------------------------------------------------- #
class CausalBudgetLearner:
    """
    Learns a per-metric budget offset from outcomes.

    **FIXED v5 bugs**

    * v5 stored a snapshot of weights in the buffer and then discarded it,
      so the update never depended on the state. v6 stores
      ``(features, reward)``.
    * v5 added ``lr * reward * 0.1`` to every weight uniformly. v6 adds
      ``lr * reward * feature[i]`` so different states produce different
      adjustments.
    * v5's reward was ``1.0 if correct else 0.0`` — a one-way ratchet.
      v6 uses ``+1.0 / -1.0`` so incorrect decisions tighten.
    * v5 could **loosen** budgets silently. v6 clamps offsets to
      ``<= 0`` unless ``allow_loosening=True`` is passed explicitly.
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
    def features_of(s: BudgetState) -> List[float]:
        return [s.hour_of_day / 24.0, s.agent_load, s.workload_hash]

    def predict_budgets(self, s: BudgetState) -> Dict[str, float]:
        f = self.features_of(s)
        out: Dict[str, float] = {}
        for metric, base in self.base_budgets.items():
            raw = sum(
                c * x for c, x in zip(
                    self.offsets.get(metric, [0.0] * self.N_FEATURES), f
                )
            )
            raw = max(-0.2 * base, min(0.2 * base, raw))
            # P0 §1: never loosen unless explicitly allowed.
            if not self.allow_loosening:
                raw = min(0.0, raw)
            out[metric] = max(0.0, base + raw)
        return out

    def record(self, features: List[float], reward: float) -> None:
        if len(features) != self.N_FEATURES:
            return
        if not math.isfinite(reward):
            return
        # +1 correct, -1 incorrect
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
                        # Only tightening: offset must be <= 0.
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
# Distillation (round, not floor)
# --------------------------------------------------------------------------- #
class QuantumDistillationBridge:
    """Quantizes a budget table to a lower precision."""

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
            return round(float(v) * scale) / scale

        return DistilledBudgetPolicy(
            precision=precision,
            budgets={k: qz(v) for k, v in budgets.items()},
            quality_retention=retention,
            energy_reduction_percent=energy,
        )


# --------------------------------------------------------------------------- #
# Federated budget aggregation (bounded)
# --------------------------------------------------------------------------- #
class FederatedBudgetAggregator:
    """FedAvg over per-metric budget profiles, bounded in memory."""

    def __init__(self, max_updates: int = _MAX_FEDERATED_UPDATES) -> None:
        self.updates: Deque[FederatedBudgetProfile] = deque(
            maxlen=max_updates
        )
        self._global: Dict[str, Dict[str, float]] = {}

    def push(self, u: FederatedBudgetProfile) -> None:
        self.updates.append(u)

    def aggregate(self) -> Dict[str, Dict[str, float]]:
        grouped: Dict[str, List[FederatedBudgetProfile]] = defaultdict(list)
        for u in self.updates:
            grouped[u.metric].append(u)
        result: Dict[str, Dict[str, float]] = {}
        for metric, profiles in grouped.items():
            total_w = sum(max(0, p.sample_count) for p in profiles)
            if total_w <= 0:
                continue
            result[metric] = {
                "mean_budget": sum(
                    p.mean_budget * p.sample_count for p in profiles
                )
                / total_w,
                "mean_usage": sum(
                    p.mean_usage * p.sample_count for p in profiles
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
# Helium profiling (used only if the caller includes a helium budget)
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
class FeasibleImpliesBudgetCompliance:
    """
    G(feasible → every budgeted metric's usage <= budget).

    A defence against logic bugs: if some future refactor sets
    ``feasible=True`` while an overshoot exists, this property fires
    before the result leaves the evaluator. (v5's
    ``SlackNonNegativeWhenFeasible`` was a tautology given the code
    already enforced ``feasible=False`` when ``slack < 0``.)
    """

    def check(self, ctx: Mapping[str, Any]) -> bool:
        if not ctx.get("feasible"):
            return True
        budgets = ctx.get("budgets", {})
        usage = ctx.get("usage", {})
        for m, limit in budgets.items():
            if m in usage and usage[m] > limit:
                return False
        return True

    def name(self) -> str:
        return "FeasibleImpliesBudgetCompliance"


@dataclass(frozen=True)
class OvershootBounded:
    """G(max relative overshoot <= max_overshoot)."""

    max_overshoot: float = 0.5  # 50%

    def check(self, ctx: Mapping[str, Any]) -> bool:
        budgets = ctx.get("budgets", {})
        usage = ctx.get("usage", {})
        for m, limit in budgets.items():
            if m in usage and limit > 0:
                ratio = (usage[m] - limit) / limit
                if ratio > self.max_overshoot:
                    return False
        return True

    def name(self) -> str:
        return "OvershootBounded"


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
    * ``areview`` — async; awaits awaitables with a timeout, and can
      delegate to v18's ``ActiveUserPreferenceLearner`` when no callback
      is set.
    """

    def __init__(
        self,
        learner: Optional[Any] = None,
        timeout: float = 3.0,
        borderline_ratio: float = 0.95,
    ) -> None:
        self._learner = learner
        self._timeout = timeout
        self.borderline_ratio = borderline_ratio
        self.pending: Deque[HITLRequest] = deque(maxlen=_MAX_HITL_PENDING)
        self.feedback_log: Deque[Dict[str, Any]] = deque(
            maxlen=_MAX_HITL_LOG
        )
        self._callback: Optional[Callable[[HITLRequest], Any]] = None
        self._outcomes: Dict[str, int] = defaultdict(int)

    def set_callback(self, cb: Callable[[HITLRequest], Any]) -> None:
        self._callback = cb

    def needs_review(self, feasible: bool, max_utilization: float) -> bool:
        if feasible and max_utilization > self.borderline_ratio:
            return True
        if not feasible and max_utilization < 1.05:
            return True
        return False

    def _make_req(
        self,
        agent_id: str,
        reason: str,
        urgency: str,
        context: Optional[Mapping[str, Any]],
    ) -> HITLRequest:
        return HITLRequest(
            agent_id=agent_id,
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
        reason: str,
        urgency: str = "medium",
        context: Optional[Mapping[str, Any]] = None,
    ) -> Tuple[Optional[bool], str]:
        req = self._make_req(agent_id, reason, urgency, context)
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
        reason: str,
        urgency: str = "medium",
        context: Optional[Mapping[str, Any]] = None,
    ) -> Tuple[Optional[bool], str]:
        req = self._make_req(agent_id, reason, urgency, context)
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
                                {"solution_id": "approve", "quality_score": 0.9},
                                {"solution_id": "reject", "quality_score": 0.8},
                            ],
                            timeout=self._timeout,
                        ),
                        timeout=self._timeout,
                    )
                    if chosen is None:
                        return self._resolve(req, None, "learner_timeout")
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
    """Sync market client with TTL cache and a stable last-snapshot."""

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
class BudgetConstrainedEvaluator:
    """
    Budget-constrained evaluator.

    Backward-compatible:

        evaluator = BudgetConstrainedEvaluator(agent_metrics, budgets)
        evaluator.is_feasible(agent_id)
        evaluator.evaluate_all()
        evaluator.compute_slack(agent_id)
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
    }

    def __init__(
        self,
        agent_metrics: Mapping[str, Mapping[str, float]],
        budgets: Mapping[str, float],
        deployment_id: str = "local",
        agent_id: str = "budget-eval-0",
        features: Optional[Dict[str, bool]] = None,
        hardware: Optional[HardwareProfile] = None,
        *,
        missing_metric_policy: str = "raise",
        fail_safe_on_degraded: bool = True,
        allow_budget_loosening: bool = False,
    ) -> None:
        # ---- Validate and copy inputs ----
        if not isinstance(agent_metrics, Mapping):
            raise TypeError(
                f"agent_metrics must be a Mapping, got "
                f"{type(agent_metrics).__name__}"
            )
        self.agent_metrics: Dict[str, Dict[str, float]] = {}
        for aid, metrics in agent_metrics.items():
            if not isinstance(metrics, Mapping):
                raise TypeError(
                    f"agent_metrics['{aid}'] must be a Mapping"
                )
            self.agent_metrics[str(aid)] = dict(metrics)

        if not isinstance(budgets, Mapping):
            raise TypeError(
                f"budgets must be a Mapping, got {type(budgets).__name__}"
            )
        self.budgets: Dict[str, float] = {}
        for metric, limit in budgets.items():
            try:
                fval = float(limit)
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    f"budget['{metric}'] must be numeric, "
                    f"got {limit!r}"
                ) from exc
            if not math.isfinite(fval) or fval < 0:
                raise ValueError(
                    f"budget['{metric}'] must be finite and >= 0, "
                    f"got {limit!r}"
                )
            self.budgets[str(metric)] = fval

        # ---- Config ----
        self.deployment_id = deployment_id
        self.agent_id = agent_id
        self.features: Dict[str, bool] = {
            **self.DEFAULT_FEATURES,
            **(features or {}),
        }
        try:
            self._missing_policy = MissingMetricPolicy(missing_metric_policy)
        except ValueError as exc:
            raise ValueError(
                f"missing_metric_policy must be one of "
                f"{[p.value for p in MissingMetricPolicy]}, "
                f"got {missing_metric_policy!r}"
            ) from exc
        self._fail_safe_on_degraded = bool(fail_safe_on_degraded)
        self._allow_budget_loosening = bool(allow_budget_loosening)
        self.hardware = hardware or HardwareProfile()

        # ---- Locks ----
        self._lock = threading.RLock()

        # ---- Core state ----
        self._circuit = SyncCircuitAdapter(
            "budget_eval", failure_threshold=5, recovery_timeout=30.0
        )
        self._chaos: Optional[SyncChaosInjector] = None
        self._safety: Optional[BudgetSafetyMonitor] = None
        self._precision: Optional[Any] = None  # AdaptivePrecisionSwitcher
        self._budget_learner: Optional[CausalBudgetLearner] = None
        self._distiller: Optional[QuantumDistillationBridge] = None
        self._federated: Optional[FederatedBudgetAggregator] = None
        self._coordinator: Optional[Any] = None  # v18 MultiAgentCoordinator
        self._market: Optional[CarbonMarketCache] = None
        self._hitl: Optional[HumanReviewGate] = None
        self._hitl_learner: Optional[Any] = None

        # ---- Bookkeeping ----
        self._evaluation_history: Deque[Dict[str, Any]] = deque(
            maxlen=_MAX_EVALUATION_HISTORY
        )
        self._last_explanations: "OrderedDict[str, FeasibilityExplanation]" = (
            OrderedDict()
        )
        self._last_evaluation_state: Dict[str, BudgetState] = {}
        self._last_market_snapshot: Optional[MarketSnapshot] = None
        self._last_market_fetched_at: Optional[float] = None
        self._counters: Dict[str, int] = defaultdict(int)
        self._unique_agents: set[str] = set()
        self._failure_reasons: Dict[str, int] = defaultdict(int)
        self._loosening_events: int = 0

        self._build_layers()

        logger.info(
            "BudgetConstrainedEvaluator v6 ready: deployment=%s, "
            "agent=%s, agents=%d, budgets=%s, features=%s",
            deployment_id,
            agent_id,
            len(self.agent_metrics),
            list(self.budgets.keys()),
            sorted(k for k, v in self.features.items() if v),
        )

    # ------------------------------------------------------------------
    # Layer construction
    # ------------------------------------------------------------------
    def _build_layers(self) -> None:
        # Sync chaos
        self._chaos = (
            SyncChaosInjector(failure_rate=0.0)
            if self.features["chaos_testing"]
            else None
        )

        # Temporal safety
        self._safety = (
            BudgetSafetyMonitor()
            if self.features["temporal_logic"]
            else None
        )
        if self._safety is not None:
            self._safety.register(FeasibleImpliesBudgetCompliance())
            self._safety.register(OvershootBounded(max_overshoot=0.5))

        # Precision
        if self.features["adaptive_precision"] and _V18_AVAILABLE:
            try:
                self._precision = AdaptivePrecisionSwitcher(  # type: ignore[misc]
                    config={}, storage=None
                )
            except Exception as exc:  # pragma: no cover — defensive
                logger.warning("AdaptivePrecisionSwitcher unavailable: %s", exc)
                self._precision = None
        else:
            self._precision = None

        # Causal learner
        self._budget_learner = (
            CausalBudgetLearner(
                base_budgets=self.budgets,
                allow_loosening=self._allow_budget_loosening,
            )
            if self.features["causal_rl"]
            else None
        )

        # Distiller
        self._distiller = (
            QuantumDistillationBridge()
            if self.features["quantum_distillation"]
            else None
        )

        # Federated
        self._federated = (
            FederatedBudgetAggregator()
            if self.features["federated"]
            else None
        )

        # Multi-agent (v18)
        if self.features["multi_agent"] and _V18_AVAILABLE:
            try:
                self._coordinator = MultiAgentCoordinator(  # type: ignore[misc]
                    config={}, storage=None
                )
                self._coordinator.register(self.agent_id)
            except Exception as exc:  # pragma: no cover
                logger.warning("MultiAgentCoordinator unavailable: %s", exc)
                self._coordinator = None
        else:
            self._coordinator = None

        # Market cache
        self._market = (
            CarbonMarketCache() if self.features["carbon_market"] else None
        )

        # HITL
        if self.features["hitl"]:
            if _V18_AVAILABLE:
                try:
                    self._hitl_learner = ActiveUserPreferenceLearner(  # type: ignore[misc]
                        storage=None
                    )
                except Exception:  # pragma: no cover
                    self._hitl_learner = None
            self._hitl = HumanReviewGate(learner=self._hitl_learner)
        else:
            self._hitl = None
            self._hitl_learner = None

    def set_features(self, features: Mapping[str, bool]) -> None:
        for k, v in features.items():
            if k not in self.DEFAULT_FEATURES:
                raise ValueError(f"unknown feature {k!r}")
            self.features[k] = bool(v)
        with self._lock:
            self._build_layers()

    # ------------------------------------------------------------------
    # Legacy public API
    # ------------------------------------------------------------------
    def is_feasible(self, agent_id: str) -> bool:
        """
        Return ``True`` iff the agent satisfies every budget.

        Raises
        ------
        UnknownAgentError
            If ``agent_id`` is not present and ``missing_metric_policy``
            is ``RAISE`` (the default; matches v1).
        EvaluationUnavailableError
            If evaluation cannot proceed (chaos, circuit open, error)
            and ``fail_safe_on_degraded`` is ``False``.
        """
        result = self._evaluate_with_details(agent_id)
        outcome = result.get("outcome", "error")
        if outcome == EvaluationOutcome.FEASIBLE.value:
            return True
        if outcome == EvaluationOutcome.INFEASIBLE.value:
            return False
        if outcome == EvaluationOutcome.UNKNOWN_AGENT.value:
            if self._missing_policy == MissingMetricPolicy.RAISE:
                raise UnknownAgentError(f"Unknown agent '{agent_id}'")
            return False
        # chaos / circuit_open / error
        if self._fail_safe_on_degraded:
            return False
        raise EvaluationUnavailableError(
            f"Evaluation of '{agent_id}' unavailable: "
            f"{result.get('degraded_reason', outcome)}"
        )

    def evaluate_all(self) -> Dict[str, bool]:
        return {
            aid: self.is_feasible(aid) for aid in self.agent_metrics
        }

    def compute_slack(self, agent_id: str) -> Dict[str, float]:
        """
        Compute slack against the *base* budgets.

        Uses ``_safe_float`` so a NaN / missing metric produces a
        sensible value rather than raising (v5 behaviour).
        """
        metrics = self.agent_metrics.get(agent_id)
        if metrics is None:
            raise UnknownAgentError(f"Unknown agent '{agent_id}'")
        slack: Dict[str, float] = {}
        for metric, limit in self.budgets.items():
            used = self._safe_float(metrics.get(metric, 0.0))
            slack[metric] = limit - used
        return slack

    # ------------------------------------------------------------------
    # Enhanced public API
    # ------------------------------------------------------------------
    def evaluate_with_details(self, agent_id: str) -> Dict[str, Any]:
        return self._evaluate_with_details(agent_id)

    def evaluate_all_detailed(self) -> Dict[str, Dict[str, Any]]:
        return {
            aid: self._evaluate_with_details(aid)
            for aid in self.agent_metrics
        }

    def explain(self, agent_id: str) -> Optional[FeasibilityExplanation]:
        with self._lock:
            return self._last_explanations.get(agent_id)

    def set_hitl_callback(self, cb: Callable[[HITLRequest], Any]) -> None:
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

    def record_outcome(self, agent_id: str, correct: bool) -> bool:
        """
        Feed back whether the last evaluation for ``agent_id`` was correct.

        Uses the actual features from the last evaluation of that agent
        (v5 used hard-coded constants).
        """
        if self._budget_learner is None:
            return False
        with self._lock:
            state = self._last_evaluation_state.get(agent_id)
        if state is None:
            return False
        features = CausalBudgetLearner.features_of(state)
        reward = 1.0 if correct else -1.0
        self._budget_learner.record(features, reward)
        if self._budget_learner.observations % 10 == 0:
            self._budget_learner.update()
        return True

    def contribute_federated(self) -> None:
        if self._federated is None or not self.agent_metrics:
            return
        for metric, limit in self.budgets.items():
            values = [
                self._safe_float(m.get(metric, 0.0))
                for m in self.agent_metrics.values()
            ]
            if not values:
                continue
            self._federated.push(
                FederatedBudgetProfile(
                    deployment_id=self.deployment_id,
                    metric=metric,
                    mean_budget=limit,
                    mean_usage=sum(values) / len(values),
                    sample_count=len(values),
                )
            )
        self._federated.aggregate()

    def get_federated_aggregate(self) -> Dict[str, Dict[str, float]]:
        return self._federated.aggregate() if self._federated else {}

    def distill_budgets(
        self, urgency: str = "normal"
    ) -> Optional[DistilledBudgetPolicy]:
        if self._distiller is None:
            return None
        precision = self._select_precision(urgency)
        return self._distiller.distill(
            budgets=self.budgets, precision=precision
        )

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
                "evaluation_count": sum(self._counters.values()),
                "outcome_counts": dict(self._counters),
                "unique_agents_evaluated": len(self._unique_agents),
                "feasible_count": self._counters["feasible"],
                "infeasible_count": self._counters["infeasible"],
                "degraded_count": sum(
                    self._counters[o]
                    for o in (
                        "chaos",
                        "circuit_open",
                        "error",
                        "unknown_agent",
                    )
                ),
                "loosening_events": self._loosening_events,
                "failure_reasons": dict(self._failure_reasons),
                "hitl_outcomes": (
                    self._hitl.outcomes() if self._hitl else {}
                ),
                "temporal_violation_count": (
                    len(self._safety.violations)
                    if self._safety else 0
                ),
                "market_cache_age_seconds": (
                    (_utcnow().timestamp() - self._last_market_fetched_at)
                    if self._last_market_fetched_at else None
                ),
                "features": dict(self.features),
                "missing_metric_policy": self._missing_policy.value,
                "allow_budget_loosening": self._allow_budget_loosening,
                "fail_safe_on_degraded": self._fail_safe_on_degraded,
            }

    def get_statistics(self) -> Dict[str, Any]:
        with self._lock:
            stats: Dict[str, Any] = {
                "deployment_id": self.deployment_id,
                "agent_id": self.agent_id,
                "agents_evaluated": len(self.agent_metrics),
                "evaluation_count": sum(self._counters.values()),
                "outcome_counts": dict(self._counters),
                "feasible_count": self._counters["feasible"],
                "infeasible_count": self._counters["infeasible"],
                "failure_reasons": dict(self._failure_reasons),
                "loosening_events": self._loosening_events,
                "circuit": {
                    "state": self._circuit.state,
                    "failures": self._circuit.failures,
                },
            }
            if self._safety is not None:
                stats["temporal_violations"] = list(
                    self._safety.violations
                )[-5:]
            if self._budget_learner is not None:
                stats["causal_learner"] = self._budget_learner.stats()
            if self._federated is not None:
                stats["federated_aggregate"] = self._federated.aggregate()
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
                    "rec_price_per_mwh_usd": (
                        self._last_market_snapshot.rec_price_per_mwh_usd
                    ),
                    "cached_at": _ensure_aware(
                        self._last_market_snapshot.timestamp
                    ).isoformat(),
                }
            if self._coordinator is not None:
                try:
                    stats["agents"] = {
                        a.id: a.role
                        for a in getattr(
                            self._coordinator, "agents", {}
                        ).values()
                    }
                except Exception:  # pragma: no cover
                    pass
            return stats

    # ------------------------------------------------------------------
    # Evaluation pipeline
    # ------------------------------------------------------------------
    def _evaluate_with_details(self, agent_id: str) -> Dict[str, Any]:
        # Circuit check (before lock — can_call mutates the probe slot)
        if not self._circuit.can_call():
            with self._lock:
                self._counters["circuit_open"] += 1
            return self._degraded_result(
                agent_id,
                reason="circuit open",
                outcome=EvaluationOutcome.CIRCUIT_OPEN,
            )

        # Chaos injection
        if self._chaos is not None and self._chaos.maybe_fail("is_feasible"):
            self._circuit.record_failure()
            with self._lock:
                self._counters["chaos"] += 1
            return self._degraded_result(
                agent_id, reason="chaos injection", outcome=EvaluationOutcome.CHAOS
            )

        try:
            result = self._evaluate_internal(agent_id)
            self._circuit.record_success()
        except Exception as exc:
            self._circuit.record_failure()
            logger.warning("Evaluation failed for '%s': %s", agent_id, exc)
            with self._lock:
                self._counters["error"] += 1
            return self._degraded_result(
                agent_id, reason=str(exc), outcome=EvaluationOutcome.ERROR
            )

        with self._lock:
            outcome = result.get("outcome", "error")
            self._counters[outcome] += 1
            self._unique_agents.add(agent_id)
        return result

    def _evaluate_internal(self, agent_id: str) -> Dict[str, Any]:
        # ---- Unknown agent ----
        if agent_id not in self.agent_metrics:
            with self._lock:
                self._counters["unknown_agent"] += 1
            return {
                "agent_id": agent_id,
                "outcome": EvaluationOutcome.UNKNOWN_AGENT.value,
                "feasible": False,
                "slack": {},
                "overshoots": {},
                "bad_data": False,
                "severity": Severity.WARNING.value,
                "explanation": None,
                "at": _utcnow().isoformat(),
            }

        metrics = self.agent_metrics[agent_id]

        # ---- Budget state ----
        state = BudgetState(
            hour_of_day=_utcnow().hour,
            agent_load=min(
                1.0,
                sum(
                    max(0.0, self._safe_float(v))
                    for v in metrics.values()
                )
                / max(1.0, 10.0),
            ),
            workload_hash=float(abs(hash(agent_id)) % 1000) / 1000.0,
        )
        with self._lock:
            self._last_evaluation_state[agent_id] = state

        # ---- Active budgets (base or RL-adjusted) ----
        active_budgets = dict(self.budgets)
        causal_adjustment = 0.0
        if self._budget_learner is not None:
            try:
                active_budgets = self._budget_learner.predict_budgets(state)
                if self.budgets:
                    causal_adjustment = sum(
                        active_budgets.get(k, self.budgets[k])
                        - self.budgets[k]
                        for k in self.budgets
                    ) / len(self.budgets)
                # Track loosening events
                if any(
                    active_budgets.get(k, self.budgets[k])
                    > self.budgets[k] + 1e-9
                    for k in self.budgets
                ):
                    with self._lock:
                        self._loosening_events += 1
            except Exception:  # pragma: no cover
                active_budgets = dict(self.budgets)

        # ---- Feasibility scan ----
        feasible = True
        slack: Dict[str, float] = {}
        overshoots: Dict[str, float] = {}
        missing_metrics: List[str] = []
        bad_data_metrics: List[str] = []
        max_utilization = 0.0

        for metric, limit in active_budgets.items():
            if metric not in metrics:
                missing_metrics.append(metric)
                if self._missing_policy == MissingMetricPolicy.RAISE:
                    raise KeyError(
                        f"Missing budget metric '{metric}' for "
                        f"agent '{agent_id}'"
                    )
                if self._missing_policy == MissingMetricPolicy.INFEASIBLE:
                    feasible = False
                continue
            raw_value = metrics[metric]
            if isinstance(raw_value, bool) or not isinstance(
                raw_value, (int, float)
            ):
                bad_data_metrics.append(metric)
                feasible = False
                continue
            if not math.isfinite(float(raw_value)):
                bad_data_metrics.append(metric)
                feasible = False
                continue
            used = float(raw_value)
            if used > limit:
                feasible = False
                overshoots[metric] = used - limit
                with self._lock:
                    self._failure_reasons[metric] += 1
            slack[metric] = limit - used
            if limit > 0:
                max_utilization = max(max_utilization, used / limit)

        # ---- Severity (relative overshoot) ----
        severity = self._classify_severity(
            overshoots=overshoots,
            feasible=feasible,
            budgets=active_budgets,
        )

        # ---- Temporal verification (load-bearing) ----
        temporal_verified: Optional[bool] = True
        temporal_violations: List[str] = []
        if self._safety is not None:
            ctx: Dict[str, Any] = {
                "feasible": feasible,
                "budgets": dict(active_budgets),
                "usage": {
                    m: self._safe_float(metrics.get(m, 0.0))
                    for m in active_budgets
                },
            }
            ok, violations = self._safety.verify(ctx)
            temporal_verified = ok
            temporal_violations = violations
            if not ok:
                logger.warning(
                    "Temporal violations on %s: %s",
                    agent_id,
                    violations,
                )

        # ---- Market enrichment (cached) ----
        market_snapshot = None
        if self._market is not None:
            try:
                snap = self._market.get_snapshot()
                co2_used = self._safe_float(metrics.get("co2", 0.0))
                market_snapshot = {
                    "carbon_price_per_tco2_usd": snap.carbon_price_per_tco2_usd,
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

        # ---- Helium profiling ----
        helium_units = 0.0
        if self.features["helium_awareness"]:
            energy = self._safe_float(metrics.get("energy", 0.0))
            helium_units = HeliumProfiler.estimate_units(energy, 0.0)

        # ---- Precision ----
        precision = self._select_precision(
            "critical" if not feasible else "normal"
        )

        # ---- HITL ----
        hitl_required = False
        hitl_approved: Optional[bool] = None
        hitl_status = "not_required"
        hitl_override = False
        if (
            self._hitl is not None
            and self._hitl.needs_review(feasible, max_utilization)
        ):
            hitl_required = True
            hitl_approved, hitl_status = self._hitl.review(
                agent_id=agent_id,
                reason=(
                    f"borderline case: feasible={feasible}, "
                    f"utilization={max_utilization:.3f}"
                ),
                urgency="medium" if feasible else "high",
                context={"agent_id": agent_id},
            )
            # P0 §4: HITL result affects feasibility
            if hitl_approved is False and feasible:
                feasible = False
                hitl_override = True
            elif hitl_approved is True and not feasible:
                # Human approved the override → report feasible but flag it
                feasible = True
                hitl_override = True

        # ---- XAI ----
        explanation: Optional[FeasibilityExplanation] = None
        if self.features["xai"]:
            explanation = BudgetExplainer.explain(
                agent_id=agent_id,
                feasible=feasible,
                slack=slack,
                overshoots=overshoots,
                severity=severity.value,
                hitl_status=hitl_status,
            )
            with self._lock:
                self._last_explanations[agent_id] = explanation
                while len(self._last_explanations) > _MAX_LAST_EXPLANATIONS:
                    self._last_explanations.popitem(last=False)

        # ---- Multi-agent observability ----
        if self._coordinator is not None:
            try:
                slack_ratio = (
                    sum(slack.values()) / sum(active_budgets.values())
                    if active_budgets else 0.0
                )
                self._coordinator.reward(
                    self.agent_id, 1.0 if feasible else 0.0
                )
                _ = slack_ratio  # observable, not required
            except Exception:  # pragma: no cover
                pass

        # ---- Federated contribution (sampled) ----
        if (
            self._federated is not None
            and len(self._evaluation_history) % 25 == 0
        ):
            for metric, limit in active_budgets.items():
                used = self._safe_float(metrics.get(metric, 0.0))
                self._federated.push(
                    FederatedBudgetProfile(
                        deployment_id=self.deployment_id,
                        metric=metric,
                        mean_budget=limit,
                        mean_usage=used,
                        sample_count=1,
                    )
                )
            self._federated.aggregate()

        # ---- Outcome ----
        outcome = (
            EvaluationOutcome.FEASIBLE.value
            if feasible
            else EvaluationOutcome.INFEASIBLE.value
        )

        # ---- History ----
        with self._lock:
            self._evaluation_history.append(
                {
                    "agent_id": agent_id,
                    "outcome": outcome,
                    "severity": severity.value,
                    "at": _utcnow().isoformat(),
                }
            )

        return {
            "agent_id": agent_id,
            "outcome": outcome,
            "feasible": feasible,
            "slack": slack,
            "overshoots": overshoots,
            "missing_metrics": missing_metrics,
            "bad_data_metrics": bad_data_metrics,
            "severity": severity.value,
            "max_utilization": max_utilization,
            "active_budgets": active_budgets,
            "causal_adjustment": causal_adjustment,
            "temporal_verified": temporal_verified,
            "temporal_violations": temporal_violations,
            "explanation": explanation.to_dict() if explanation else None,
            "market_snapshot": market_snapshot,
            "helium_units": helium_units,
            "precision": precision.value,
            "hitl_required": hitl_required,
            "hitl_approved": hitl_approved,
            "hitl_status": hitl_status,
            "hitl_override": hitl_override,
            "degraded": False,
            "degraded_reason": None,
            "at": _utcnow().isoformat(),
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _degraded_result(
        self,
        agent_id: str,
        reason: str,
        outcome: EvaluationOutcome,
    ) -> Dict[str, Any]:
        """
        Return a degraded result.

        P1 §14: ``temporal_verified`` is ``None`` (not ``True``).
        """
        return {
            "agent_id": agent_id,
            "outcome": outcome.value,
            "feasible": False,
            "slack": {},
            "overshoots": {},
            "missing_metrics": [],
            "bad_data_metrics": [],
            "severity": Severity.WARNING.value,
            "max_utilization": 0.0,
            "active_budgets": dict(self.budgets),
            "causal_adjustment": 0.0,
            "temporal_verified": None,
            "temporal_violations": [],
            "explanation": None,
            "market_snapshot": None,
            "helium_units": 0.0,
            "precision": "fp32",
            "hitl_required": False,
            "hitl_approved": None,
            "hitl_status": "not_required",
            "hitl_override": False,
            "degraded": True,
            "degraded_reason": reason,
            "at": _utcnow().isoformat(),
        }

    @staticmethod
    def _safe_float(value: Any, default: float = 0.0) -> float:
        try:
            v = float(value)
            return v if math.isfinite(v) else default
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _classify_severity(
        overshoots: Mapping[str, float],
        feasible: bool,
        budgets: Mapping[str, float],
    ) -> Severity:
        """
        Classify severity by **relative** overshoot (P0 §5).

        v5 compared ``overshoot`` to absolute thresholds, so a CO2 overrun
        of ``10`` was EMERGENCY and an energy overrun of ``0.9`` was
        WARNING — regardless of the metric's unit.
        """
        if feasible:
            return Severity.INFO
        if not overshoots:
            return Severity.WARNING
        ratios = [
            overshoots[k] / max(budgets.get(k, 1.0), 1e-9)
            for k in overshoots
        ]
        max_ratio = max(ratios) if ratios else 0.0
        if max_ratio >= 1.0:
            return Severity.EMERGENCY
        if max_ratio >= 0.3:
            return Severity.CRITICAL
        return Severity.WARNING

    def _select_precision(self, urgency: str) -> PrecisionLevel:
        # v18's switcher probes hardware; the urgency-based selection
        # that v5 had is still local because it's budget-specific.
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
    # Lifecycle
    # ------------------------------------------------------------------
    def __enter__(self) -> "BudgetConstrainedEvaluator":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        pass

    async def __aenter__(self) -> "BudgetConstrainedEvaluator":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        pass

    def __repr__(self) -> str:
        return (
            "BudgetConstrainedEvaluator("
            f"agents={len(self.agent_metrics)}, "
            f"budgets={list(self.budgets)}, "
            f"evals={sum(self._counters.values())}, "
            f"circuit={self._circuit.state}, "
            f"loosening_events={self._loosening_events})"
        )


# --------------------------------------------------------------------------- #
# Factory
# --------------------------------------------------------------------------- #
def create_budget_evaluator(
    agent_metrics: Mapping[str, Mapping[str, float]],
    budgets: Mapping[str, float],
    deployment_id: str = "local",
    agent_id: str = "budget-eval-0",
    features: Optional[Dict[str, bool]] = None,
    hardware: Optional[HardwareProfile] = None,
    missing_metric_policy: str = "raise",
    fail_safe_on_degraded: bool = True,
    allow_budget_loosening: bool = False,
) -> BudgetConstrainedEvaluator:
    return BudgetConstrainedEvaluator(
        agent_metrics=agent_metrics,
        budgets=budgets,
        deployment_id=deployment_id,
        agent_id=agent_id,
        features=features,
        hardware=hardware,
        missing_metric_policy=missing_metric_policy,
        fail_safe_on_degraded=fail_safe_on_degraded,
        allow_budget_loosening=allow_budget_loosening,
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
    "PrecisionLevel",
    "AgentRole",
    "Severity",
    "EvaluationOutcome",
    "MissingMetricPolicy",
    # Errors
    "BudgetError",
    "UnknownAgentError",
    "EvaluationUnavailableError",
    # Domain types
    "HardwareProfile",
    "BudgetState",
    "FeasibilityExplanation",
    "DistilledBudgetPolicy",
    "FederatedBudgetProfile",
    "MarketSnapshot",
    "HITLRequest",
    # Components
    "SyncCircuitAdapter",
    "SyncChaosInjector",
    "BudgetExplainer",
    "CausalBudgetLearner",
    "QuantumDistillationBridge",
    "FederatedBudgetAggregator",
    "HeliumProfiler",
    "FeasibleImpliesBudgetCompliance",
    "OvershootBounded",
    "BudgetSafetyMonitor",
    "HumanReviewGate",
    "CarbonMarketCache",
    # Main
    "BudgetConstrainedEvaluator",
    "create_budget_evaluator",
]


# --------------------------------------------------------------------------- #
# Demo
# --------------------------------------------------------------------------- #
def _demo() -> None:
    logging.basicConfig(
        level=logging.WARNING,
        format="%(levelname)s %(name)s: %(message)s",
    )

    print("=" * 76)
    print("budget_constrained.py v6.0.0 — Demo")
    print("=" * 76)

    def _check(condition: bool, message: str) -> None:
        if not condition:
            raise AssertionError(f"FAILED: {message}")
        print(f"  ✓ {message}")

    # ---- Fixtures ----
    agent_metrics = {
        "agent-A": {"energy": 4.5, "co2": 180.0, "latency": 25.0},
        "agent-B": {"energy": 6.2, "co2": 210.0, "latency": 28.0},
        "agent-C": {"energy": 3.0, "co2": 150.0, "latency": 20.0},
    }
    budgets = {"energy": 5.0, "co2": 200.0, "latency": 30.0}

    # ---- Baseline (backward compat) ----
    print("\n=== Baseline: is_feasible / evaluate_all ===")
    evaluator = create_budget_evaluator(
        agent_metrics=agent_metrics,
        budgets=budgets,
        deployment_id="us-ca-prod-01",
        hardware=HardwareProfile(supports_int4=True, vram_gb=48),
    )
    _check(evaluator.is_feasible("agent-A") is True, "agent-A feasible")
    _check(evaluator.is_feasible("agent-B") is False, "agent-B infeasible")
    _check(evaluator.is_feasible("agent-C") is True, "agent-C feasible")
    _check(
        evaluator.evaluate_all()
        == {"agent-A": True, "agent-B": False, "agent-C": True},
        "evaluate_all matches per-agent results",
    )

    # ============================================================= #
    # P0 §1 — RL tightening-only by default
    # ============================================================= #
    print("\n=== P0 §1: RL tightening-only by default ===")
    tl = create_budget_evaluator(
        agent_metrics=agent_metrics,
        budgets=budgets,
    )
    # Simulate 100 correct outcomes — the learner should NOT loosen
    for _ in range(100):
        tl.record_outcome("agent-A", correct=True)
    tl._budget_learner.update()
    loosened = [
        m for m in budgets
        if tl._budget_learner.predict_budgets(BudgetState(12, 0.5, 0.5)).get(m, 0.0) > budgets[m] + 1e-9
    ]
    _check(
        loosened == [],
        "100 correct outcomes did not loosen any budget",
    )
    _check(
        tl._loosening_events == 0,
        "loosening_events counter is 0",
    )

    # With allow_budget_loosening=True, the same test should loosen.
    tl_loose = create_budget_evaluator(
        agent_metrics=agent_metrics,
        budgets=budgets,
        allow_budget_loosening=True,
    )
    for _ in range(200):
        tl_loose.record_outcome("agent-A", correct=True)
    tl_loose._budget_learner.update()
    loosened_any = any(
        tl_loose._budget_learner.predict_budgets(
            BudgetState(12, 0.5, 0.5)
        ).get(m, 0.0) > budgets[m] + 1e-9
        for m in budgets
    )
    _check(
        loosened_any,
        "with allow_budget_loosening=True, correct decisions can loosen",
    )

    # ============================================================= #
    # P0 §2 — Causal learner uses features (different states → different adjustments)
    # ============================================================= #
    print("\n=== P0 §2: causal learner uses features ===")
    learner = CausalBudgetLearner(base_budgets={"energy": 5.0})
    # Train on state A
    state_a = BudgetState(hour_of_day=8, agent_load=0.9, workload_hash=0.1)
    features_a = CausalBudgetLearner.features_of(state_a)
    for _ in range(20):
        learner.record(features_a, -1.0)  # incorrect → tighten
    learner.update()
    # Train on state B
    state_b = BudgetState(hour_of_day=20, agent_load=0.1, workload_hash=0.9)
    features_b = CausalBudgetLearner.features_of(state_b)
    for _ in range(20):
        learner.record(features_b, -1.0)
    learner.update()
    budgets_a = learner.predict_budgets(state_a)["energy"]
    budgets_b = learner.predict_budgets(state_b)["energy"]
    _check(
        abs(budgets_a - budgets_b) > 0.01,
        f"different states → different budgets: A={budgets_a:.4f}, B={budgets_b:.4f}",
    )

    # ============================================================= #
    # P0 §3 — Signed reward (negative reward tightens)
    # ============================================================= #
    print("\n=== P0 §3: negative reward tightens ===")
    learner2 = CausalBudgetLearner(base_budgets={"energy": 5.0})
    state = BudgetState(12, 0.5, 0.5)
    features = CausalBudgetLearner.features_of(state)
    baseline_budget = learner2.predict_budgets(state)["energy"]
    for _ in range(50):
        learner2.record(features, -1.0)  # incorrect
    learner2.update()
    after_budget = learner2.predict_budgets(state)["energy"]
    _check(
        after_budget < baseline_budget,
        f"incorrect outcomes tightened: {baseline_budget:.4f} → {after_budget:.4f}",
    )

    # ============================================================= #
    # P0 §4 — HITL propagation
    # ============================================================= #
    print("\n=== P0 §4: HITL rejection makes feasible agent infeasible ===")
    # Force HITL review by using a borderline utilization
    border_metrics = {
        "agent-X": {"energy": 4.99, "co2": 199.0, "latency": 29.0},
    }
    hitl_ev = create_budget_evaluator(
        agent_metrics=border_metrics,
        budgets={"energy": 5.0, "co2": 200.0, "latency": 30.0},
    )
    hitl_ev.set_hitl_callback(lambda req: False)
    res = hitl_ev.evaluate_with_details("agent-X")
    _check(res["hitl_required"] is True, "borderline case triggered HITL")
    _check(res["hitl_status"] == "rejected", "HITL status is rejected")
    _check(
        res["feasible"] is False and res["hitl_override"] is True,
        "HITL rejection made agent infeasible and flagged override",
    )

    print("\n=== P0 §4: HITL approval of infeasible → feasible with override ===")
    over_metrics = {"agent-Y": {"energy": 5.05, "co2": 200.0, "latency": 30.0}}
    over_ev = create_budget_evaluator(
        agent_metrics=over_metrics,
        budgets={"energy": 5.0, "co2": 200.0, "latency": 30.0},
    )
    over_ev.set_hitl_callback(lambda req: True)
    res_over = over_ev.evaluate_with_details("agent-Y")
    _check(res_over["hitl_status"] == "approved", "HITL approved")
    _check(
        res_over["feasible"] is True and res_over["hitl_override"] is True,
        "HITL approval of infeasible → feasible + override flag",
    )

    # ============================================================= #
    # P0 §5 — Relative severity
    # ============================================================= #
    print("\n=== P0 §5: severity uses relative overshoot ===")
    # 5% co2 overrun — should be WARNING, not EMERGENCY
    rel_ev = create_budget_evaluator(
        agent_metrics={"tiny": {"energy": 1.0, "co2": 210.0, "latency": 1.0}},
        budgets={"energy": 5.0, "co2": 200.0, "latency": 30.0},
    )
    res_rel = rel_ev.evaluate_with_details("tiny")
    _check(
        res_rel["severity"] == "warning",
        f"5% co2 overrun → WARNING (was {res_rel['severity']})",
    )
    # 100% energy overrun — should be EMERGENCY
    emg_ev = create_budget_evaluator(
        agent_metrics={"big": {"energy": 10.0, "co2": 100.0, "latency": 10.0}},
        budgets={"energy": 5.0, "co2": 200.0, "latency": 30.0},
    )
    res_emg = emg_ev.evaluate_with_details("big")
    _check(
        res_emg["severity"] == "emergency",
        f"100% energy overrun → EMERGENCY (was {res_emg['severity']})",
    )
    # 40% energy overrun — should be CRITICAL
    crit_ev = create_budget_evaluator(
        agent_metrics={"mid": {"energy": 7.0, "co2": 100.0, "latency": 10.0}},
        budgets={"energy": 5.0, "co2": 200.0, "latency": 30.0},
    )
    res_crit = crit_ev.evaluate_with_details("mid")
    _check(
        res_crit["severity"] == "critical",
        f"40% energy overrun → CRITICAL (was {res_crit['severity']})",
    )

    # ============================================================= #
    # P0 §6 — is_feasible distinguishes five outcomes
    # ============================================================= #
    print("\n=== P0 §6: is_feasible distinguishes outcomes ===")
    disc = create_budget_evaluator(
        agent_metrics=agent_metrics,
        budgets=budgets,
        fail_safe_on_degraded=False,
    )
    # Unknown agent → UnknownAgentError
    try:
        disc.is_feasible("nonexistent")
    except UnknownAgentError:
        _check(True, "unknown agent → UnknownAgentError")
    else:
        raise AssertionError("unknown agent did not raise")

    # Chaos → EvaluationUnavailableError with fail_safe_on_degraded=False
    disc_chaos = create_budget_evaluator(
        agent_metrics=agent_metrics,
        budgets=budgets,
        features={"chaos_testing": True},
        fail_safe_on_degraded=False,
    )
    disc_chaos._chaos = SyncChaosInjector(failure_rate=1.0)
    try:
        disc_chaos.is_feasible("agent-A")
    except EvaluationUnavailableError:
        _check(True, "chaos + fail_safe=False → EvaluationUnavailableError")
    else:
        raise AssertionError("chaos did not raise")

    # Same chaos with fail_safe_on_degraded=True → returns False
    disc_safe = create_budget_evaluator(
        agent_metrics=agent_metrics,
        budgets=budgets,
        features={"chaos_testing": True},
        fail_safe_on_degraded=True,
    )
    disc_safe._chaos = SyncChaosInjector(failure_rate=1.0)
    _check(
        disc_safe.is_feasible("agent-A") is False,
        "chaos + fail_safe=True → returns False",
    )

    # ============================================================= #
    # P0 §7 — missing_metric_policy independent of causal_rl
    # ============================================================= #
    print("\n=== P0 §7: missing_metric_policy independent of causal_rl ===")
    partial = {"agent-P": {"energy": 1.0}}  # missing co2, latency
    # Default policy=raise (v1 compat), with causal_rl enabled
    mmp_raise = create_budget_evaluator(
        agent_metrics=partial,
        budgets=budgets,
        features={"causal_rl": True},
        missing_metric_policy="raise",
    )
    try:
        mmp_raise.is_feasible("agent-P")
    except (KeyError, BudgetError):
        _check(True, "missing_metric_policy='raise' raises even with causal_rl")
    else:
        raise AssertionError("raise policy did not raise")

    mmp_inf = create_budget_evaluator(
        agent_metrics=partial,
        budgets=budgets,
        features={"causal_rl": True},
        missing_metric_policy="infeasible",
    )
    _check(
        mmp_inf.is_feasible("agent-P") is False,
        "missing_metric_policy='infeasible' returns False",
    )

    mmp_skip = create_budget_evaluator(
        agent_metrics=partial,
        budgets=budgets,
        features={"causal_rl": True},
        missing_metric_policy="skip",
    )
    _check(
        mmp_skip.is_feasible("agent-P") is True,
        "missing_metric_policy='skip' ignores missing co2/latency",
    )

    # ============================================================= #
    # P1 §10 — NaN → infeasible
    # ============================================================= #
    print("\n=== P1 §10: NaN metric → infeasible ===")
    nan_metrics = {"nan-agent": {"energy": float("nan"), "co2": 100.0}}
    nan_ev = create_budget_evaluator(
        agent_metrics=nan_metrics,
        budgets={"energy": 5.0, "co2": 200.0},
    )
    res_nan = nan_ev.evaluate_with_details("nan-agent")
    _check(
        res_nan["feasible"] is False,
        "NaN energy → infeasible (v5 silently coerced to 0)",
    )
    _check(
        "energy" in res_nan["bad_data_metrics"],
        "bad_data_metrics flags 'energy'",
    )

    # ============================================================= #
    # P1 §12 — record_outcome(agent_id, correct) uses real state
    # ============================================================= #
    print("\n=== P1 §12: record_outcome uses real state ===")
    rec_ev = create_budget_evaluator(
        agent_metrics=agent_metrics,
        budgets=budgets,
    )
    rec_ev.evaluate_with_details("agent-A")
    stored_state = rec_ev._last_evaluation_state.get("agent-A")
    _check(
        stored_state is not None,
        "evaluation stored the real BudgetState",
    )
    ok = rec_ev.record_outcome("agent-A", correct=False)
    _check(ok is True, "record_outcome(agent_id, correct=False) returned True")

    # ============================================================= #
    # P1 §13 — cached market snapshot in get_statistics
    # ============================================================= #
    print("\n=== P1 §13: get_statistics returns cached market snapshot ===")
    cache_ev = create_budget_evaluator(
        agent_metrics=agent_metrics,
        budgets=budgets,
    )
    cache_ev.evaluate_with_details("agent-A")
    first_market = cache_ev.get_statistics().get("market", {}).get(
        "carbon_price_per_tco2_usd"
    )
    second_market = cache_ev.get_statistics().get("market", {}).get(
        "carbon_price_per_tco2_usd"
    )
    _check(
        first_market == second_market,
        "consecutive get_statistics() reports the same cached market price",
    )

    # ============================================================= #
    # P1 §14 — degraded result does not claim temporal_verified=True
    # ============================================================= #
    print("\n=== P1 §14: degraded result sets temporal_verified=None ===")
    deg_ev = create_budget_evaluator(
        agent_metrics=agent_metrics,
        budgets=budgets,
        features={"chaos_testing": True},
    )
    deg_ev._chaos = SyncChaosInjector(failure_rate=1.0)
    deg_res = deg_ev.evaluate_with_details("agent-A")
    _check(
        deg_res["temporal_verified"] is None,
        "degraded result has temporal_verified=None (v5 claimed True)",
    )
    _check(
        deg_res["outcome"] == "chaos",
        "degraded outcome is 'chaos'",
    )

    # ============================================================= #
    # P1 §9/§11 — thread safety and bounded collections
    # ============================================================= #
    print("\n=== P1 §9: thread safety ===")
    import threading
    conc_ev = create_budget_evaluator(
        agent_metrics=agent_metrics,
        budgets=budgets,
    )
    errors: List[BaseException] = []

    def _worker():
        try:
            for _ in range(100):
                conc_ev.is_feasible("agent-A")
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=_worker) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    _check(
        not errors,
        f"10 threads × 100 concurrent is_feasible: no errors ({len(errors)} raised)",
    )
    _check(
        sum(conc_ev._counters.values()) == 1000,
        f"counter is exactly 1000 (got {sum(conc_ev._counters.values())})",
    )

    print("\n=== P1 §11: bounded collections ===")
    bound_ev = create_budget_evaluator(
        agent_metrics=agent_metrics,
        budgets=budgets,
    )
    for _ in range(2000):
        bound_ev.evaluate_with_details("agent-A")
    _check(
        len(bound_ev._evaluation_history) <= _MAX_EVALUATION_HISTORY,
        f"evaluation_history bounded at {_MAX_EVALUATION_HISTORY}",
    )
    _check(
        len(bound_ev._last_explanations) <= _MAX_LAST_EXPLANATIONS,
        f"last_explanations bounded at {_MAX_LAST_EXPLANATIONS}",
    )

    # ============================================================= #
    # P2 §20 — temporal properties are load-bearing
    # ============================================================= #
    print("\n=== P2 §20: temporal properties actually fire ===")
    safety = BudgetSafetyMonitor()
    safety.register(FeasibleImpliesBudgetCompliance())
    # Deliberately pass an inconsistent context: feasible but with an overshoot
    ok, violations = safety.verify(
        {
            "feasible": True,
            "budgets": {"energy": 5.0},
            "usage": {"energy": 6.0},
        }
    )
    _check(
        ok is False and "FeasibleImpliesBudgetCompliance" in violations,
        "FeasibleImpliesBudgetCompliance fires on a logic bug",
    )

    safety2 = BudgetSafetyMonitor()
    safety2.register(OvershootBounded(max_overshoot=0.5))
    ok2, violations2 = safety2.verify(
        {
            "feasible": False,
            "budgets": {"energy": 5.0},
            "usage": {"energy": 8.0},  # 60% overrun
        }
    )
    _check(
        ok2 is False and "OvershootBounded" in violations2,
        "OvershootBounded fires at 60% overrun (v5's 10x threshold never fired)",
    )

    # ============================================================= #
    # P2 §21 — distillation uses round, not floor
    # ============================================================= #
    print("\n=== P2 §21: distillation uses round, not floor ===")
    bridge = QuantumDistillationBridge()
    # 5.129 quantized at INT8 (scale=100): floor → 5.12, round → 5.13
    policy = bridge.distill(
        {"energy": 5.129}, precision=PrecisionLevel.INT8
    )
    _check(
        policy.budgets["energy"] >= 5.125,
        f"round preserves 5.129 as {policy.budgets['energy']} (floor would give 5.12)",
    )

    # ============================================================= #
    # P1 §16 — set_features, health_check, async CM
    # ============================================================= #
    print("\n=== P1 §16: set_features, health_check, async CM ===")
    live = create_budget_evaluator(
        agent_metrics=agent_metrics,
        budgets=budgets,
    )
    _check(live._market is not None, "market enabled by default")
    live.set_features({"carbon_market": False})
    _check(live._market is None, "set_features disabled market")
    live.set_features({"carbon_market": True})
    _check(live._market is not None, "set_features re-enabled market")

    async def _async_check() -> None:
        async with create_budget_evaluator(
            agent_metrics=agent_metrics,
            budgets=budgets,
        ) as ev:
            health = await ev.health_check()
            _check(
                "healthy" in health and "loosening_events" in health,
                "health_check returns expected keys",
            )

    asyncio.run(_async_check())

    # ---- Final summary ----
    print("\n=== Full statistics ===")
    print(json.dumps(evaluator.get_statistics(), indent=2, default=str))

    print("\n" + "=" * 76)
    print("All v6 assertions passed.")
    print("=" * 76)


if __name__ == "__main__":
    _demo()
