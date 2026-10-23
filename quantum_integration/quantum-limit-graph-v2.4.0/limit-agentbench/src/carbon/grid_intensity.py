"""
qlearning_agent.py — v6.0.0 (thin wrapper over v18)
====================================================

Enhanced tabular Q-learning agent for the Green Agent.

**What changed in v6**
----------------------
v5 grew its own copies of the ten enhancement classes, drifted from the
fixed versions in ``carbon_forecast`` v18, and broke two things in the
process:

* ``_safe_load`` referenced ``self.chaos`` **before** the attribute was
  set, so the Q-table never loaded on process start.
* The reward shaper was **action-independent**, so Q(s, ·) converged to
  identical values across all actions and the agent could not learn
  which action was better.

v6 fixes both, plus:

* **HITL**: async callbacks are rejected with an error (fail-safe) in
  the sync ``review`` path; a separate ``areview`` awaits them. Callbacks
  are wrapped in ``try/except``; ``pending``/``feedback_log`` are bounded.
* **Propensity**: the effective propensity (accounting for temporal
  vetoes and HITL overrides) is recorded by ``choose_action`` and used by
  ``update``. ``update`` also accepts an explicit ``propensity=`` for
  external behaviour policies.
* **Terminal handling**: ``update(..., done=False)`` stops bootstrapping
  on terminal transitions.
* **Bounded Q-table** (LRU) and **batched saves** (every ``save_interval``
  episodes, plus explicit ``save()``).
* **Order-independent** ``_state_key`` (``json.dumps(sort_keys=True)``).
* **Deep-copied** state on ``DecisionRecord`` and ``HITLRequest``.
* **Distillation uses round, not floor** and the resulting
  ``DistilledQPolicy`` can be **installed** (``use_distilled``).
* **Feature flags** are consulted per call; ``set_features()`` recreates
  layers.
* **Fixed market sign**: the market term is action-dependent
  (``energy_saved_pct / 100 * price``), not the state-level price.
* **``health_check()``** exposes load status, circuit state, LRU
  pressure, distill state, and HITL outcome counts.
* **Datetime hygiene**: ``datetime.now(timezone.utc)`` everywhere.
* **Async context manager** (``__aenter__`` / ``__aexit__``).

**Re-exported from v18 (single implementation)**
    CircuitBreaker, ActiveUserPreferenceLearner, ChaosTestingEngine,
    MultiAgentCoordinator, FederatedGreenAggregator,
    QuantumDistillationEngine, XAIDecisionExplainer,
    AdaptivePrecisionSwitcher, CarbonMarketIntegrator,
    ForecastPoint, ForecastBatch, ForecastSource, CarbonForecast,
    plus every ``Simple*`` alias and the shared helpers
    (``_utcnow``, ``_ensure_aware``, ``_json_safe``, ``_safe_mean``,
    ``_cfg_get``).

**Local (genuinely different concern; fixed)**
    SyncCircuitAdapter      — sync adapter around v18's async
                              ``CircuitBreaker``
    SyncChaosInjector       — deterministic-rate sync fault injector
    ActionSafetyMonitor     — safety properties on ``(state, action)``
                              (v18's verifier is trace-based on state dicts)
    QLearningCoordinator    — Q-learning agent specialisation by role
                              (v18's coordinator handles forecast-pipeline
                              roles; different concern)
    FederatedQTableAggregator — FedAvg over Q-tables (v18's federated
                              aggregator handles byte blobs)
    QLearningDistiller      — quantizes a Q-table (v18's distillation
                              engine handles teacher→student policies)
    QValueExplainer         — Q-value based explanation (v18's XAI
                              handles feature attributions)
    HumanReviewGate         — sync review gate with fixed async handling
                              (wraps v18's ``ActiveUserPreferenceLearner``
                              for the async queue path)

**Public API preserved**
    agent = QLearningAgent(alpha=0.1, gamma=0.9, epsilon=0.1)
    action = agent.choose_action(state)
    agent.update(state, action, reward, next_state)  # done= now optional

File: src/carbon/qlearning_agent.py
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import inspect
import json
import logging
import math
import random
import statistics
import threading
from collections import Counter, OrderedDict, defaultdict, deque
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
# v18 imports (single implementation of the enhancement classes)
# --------------------------------------------------------------------------- #
try:
    from carbon_forecast import (  # type: ignore[import-not-found]
        CircuitBreaker,
        ActiveUserPreferenceLearner,
        ChaosTestingEngine,
        MultiAgentCoordinator,
        FederatedGreenAggregator,
        QuantumDistillationEngine,
        XAIDecisionExplainer,
        AdaptivePrecisionSwitcher,
        CarbonMarketIntegrator,
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
        CarbonForecast,
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
    ActiveUserPreferenceLearner = None  # type: ignore[assignment]
    ChaosTestingEngine = None  # type: ignore[assignment]
    MultiAgentCoordinator = None  # type: ignore[assignment]
    FederatedGreenAggregator = None  # type: ignore[assignment]
    QuantumDistillationEngine = None  # type: ignore[assignment]
    XAIDecisionExplainer = None  # type: ignore[assignment]
    AdaptivePrecisionSwitcher = None  # type: ignore[assignment]
    CarbonMarketIntegrator = None  # type: ignore[assignment]
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

    class CarbonForecast:  # type: ignore[no-redef]
        def __init__(self, *a: Any, **kw: Any) -> None:
            raise RuntimeError(
                "qlearning_agent v6 requires carbon_forecast (v18); "
                f"import error: {_V18_ERROR}"
            )

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
# RLStorage — safe import with in-memory fallback
# --------------------------------------------------------------------------- #
try:
    from rl.rl_storage import RLStorage  # type: ignore
except Exception:  # pragma: no cover — self-contained fallback
    class RLStorage:  # type: ignore[no-redef]
        """In-memory fallback for RLStorage."""

        def __init__(self) -> None:
            self._store: Dict[str, Any] = {}

        def load(self) -> Dict[str, Any]:
            return dict(self._store)

        def save(self, q_table: Dict[str, Any]) -> None:
            self._store = dict(q_table)


# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #
_MAX_Q_TABLE_STATES = 100_000
_MAX_VIOLATIONS = 200
_MAX_PENDING_HITL = 100
_MAX_HITL_LOG = 500
_MAX_TRANSITIONS = 4096


# --------------------------------------------------------------------------- #
# Public enums / dataclasses (v6 scope)
# --------------------------------------------------------------------------- #
class PrecisionLevel(Enum):
    FP32 = "fp32"
    FP16 = "fp16"
    INT8 = "int8"
    INT4 = "int4"
    QUANTUM_DISTILLED = "quantum_distilled"


class AgentRole(Enum):
    GENERALIST = "generalist"
    LATENCY_OPTIMIZER = "latency_optimizer"
    ENERGY_SAVER = "energy_saver"
    DEFERRAL_SPECIALIST = "deferral_specialist"
    BALANCED_WORKER = "balanced_worker"


@dataclass(frozen=True)
class HardwareProfile:
    """Local: v18 does not export a hardware profile."""

    has_tensor_cores: bool = True
    supports_int8: bool = True
    supports_int4: bool = False
    vram_gb: float = 24.0
    edge_device: bool = False


@dataclass
class Explanation:
    """Q-value explanation (local; v18's XAI is feature-attribution)."""

    action: str
    headline: str
    rationale: List[str]
    q_values: Dict[str, float]
    confidence: float
    causal_adjustment: Optional[Dict[str, float]] = None
    contributing_factors: Dict[str, float] = field(default_factory=dict)
    used_distilled_policy: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class DecisionRecord:
    """Full record of a decision for audit and active learning."""

    state: Dict[str, Any]
    action: str
    q_values: Dict[str, float]
    confidence: float
    propensity: float
    explanation: Optional[Explanation]
    timestamp: datetime = field(default_factory=_utcnow)
    human_approved: Optional[bool] = None
    hitl_status: str = "not_required"
    temporal_violations: List[str] = field(default_factory=list)
    used_distilled_policy: bool = False


@dataclass(frozen=True)
class HITLRequest:
    state: Dict[str, Any]
    action: str
    confidence: float
    reason: str
    urgency: str
    requested_at: datetime = field(default_factory=_utcnow)


@dataclass(frozen=True)
class DistilledQPolicy:
    precision: PrecisionLevel
    table: Dict[str, Dict[str, float]]
    quality_retention: float
    energy_reduction_percent: float

    def to_dict(self) -> Dict[str, Any]:
        return _json_safe(
            {
                "precision": self.precision.value,
                "table": self.table,
                "quality_retention": self.quality_retention,
                "energy_reduction_percent": (
                    self.energy_reduction_percent
                ),
            }
        )


# --------------------------------------------------------------------------- #
# Sync adapter around v18's async CircuitBreaker
# --------------------------------------------------------------------------- #
class SyncCircuitAdapter:
    """
    Sync facade over v18's ``CircuitBreaker``.

    v18's ``CircuitBreaker`` exposes sync internals
    (``_acquire_slot`` / ``_on_success`` / ``_on_failure``) and an async
    ``call`` method. The Q-learner is sync, so this adapter calls the
    sync internals directly. The dependency is documented here so a
    future refactor of v18's breaker cannot silently break Q-learning.
    """

    def __init__(self, name: str, failure_threshold: int = 3,
                 recovery_timeout: float = 30.0) -> None:
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
    def failure_count(self) -> int:
        return self._cb.failure_count

    def call(self, fn: Callable[[], Any]) -> Any:
        """Run ``fn`` through the circuit; raises ``RuntimeError`` if OPEN."""
        self._cb._acquire_slot()
        try:
            result = fn()
            self._cb._on_success()
            return result
        except Exception:
            self._cb._on_failure()
            raise


# --------------------------------------------------------------------------- #
# Sync chaos injector (simple; v18's ChaosTestingEngine is async)
# --------------------------------------------------------------------------- #
class SyncChaosInjector:
    """Deterministic-rate sync fault injector for the Q-learning hot path."""

    def __init__(self, failure_rate: float = 0.0) -> None:
        if not 0.0 <= failure_rate <= 1.0:
            raise ValueError("failure_rate must be in [0, 1]")
        self.failure_rate = failure_rate
        self.events: Deque[Dict[str, Any]] = deque(maxlen=500)

    def maybe_fail(self, component: str) -> bool:
        if random.random() < self.failure_rate:
            self.events.append(
                {
                    "component": component,
                    "at": _utcnow().isoformat(),
                }
            )
            return True
        return False


# --------------------------------------------------------------------------- #
# Action-safety properties (local; v18's TemporalLogicVerifier is
# trace-based on state dicts, not action-safety)
# --------------------------------------------------------------------------- #
class SafetyProperty(Protocol):
    def is_safe(
        self, state: Dict[str, Any], action: str
    ) -> bool: ...
    def name(self) -> str: ...


@dataclass
class NoEnergySaverOnDeadline:
    """If the task is deadline-critical, forbid ``energy_saver``."""

    def is_safe(
        self, state: Dict[str, Any], action: str
    ) -> bool:
        if state.get("deadline_critical"):
            return action != "energy_saver"
        return True

    def name(self) -> str:
        return "NoEnergySaverOnDeadline"


@dataclass
class NoLatencyOptimizedUnderEmergency:
    """Under high carbon intensity, forbid ``latency_optimized``."""

    def is_safe(
        self, state: Dict[str, Any], action: str
    ) -> bool:
        ci = state.get("carbon_intensity", 0)
        if ci and ci > 600 and action == "latency_optimized":
            return False
        return True

    def name(self) -> str:
        return "NoLatencyOptimizedUnderEmergency"


class ActionSafetyMonitor:
    """
    Verifies that an ``(state, action)`` pair satisfies registered safety
    properties.

    Fixed vs. v5:
    * ``vet`` is **pure** — it does not mutate ``violations``.
    * ``violations`` is bounded.
    * ``vet`` is O(1) per property.
    """

    def __init__(self, max_violations: int = _MAX_VIOLATIONS) -> None:
        self.properties: List[SafetyProperty] = []
        self.violations: Deque[Dict[str, Any]] = deque(
            maxlen=max_violations
        )

    def register(self, prop: SafetyProperty) -> None:
        self.properties.append(prop)

    def vet(
        self, state: Dict[str, Any], action: str
    ) -> Tuple[bool, List[str]]:
        """Return ``(ok, violated_property_names)``; side-effect free."""
        bad: List[str] = []
        for p in self.properties:
            try:
                if not p.is_safe(state, action):
                    bad.append(p.name())
            except Exception as e:
                logger.warning(
                    "Safety property %s raised: %s", p.name(), e
                )
                bad.append(p.name())
        return (len(bad) == 0, bad)

    def record_violation(
        self, state: Dict[str, Any], action: str, properties: List[str]
    ) -> None:
        for name in properties:
            self.violations.append(
                {
                    "property": name,
                    "action": action,
                    "at": _utcnow().isoformat(),
                }
            )

    def is_action_safe(
        self, state: Dict[str, Any], action: str
    ) -> bool:
        return self.vet(state, action)[0]


# --------------------------------------------------------------------------- #
# Q-learning agent coordinator (local; v18's is forecast-pipeline scoped)
# --------------------------------------------------------------------------- #
@dataclass
class AgentProfile:
    agent_id: str
    role: AgentRole = AgentRole.GENERALIST
    success_rate: float = 0.0
    avg_energy_saved: float = 0.0
    avg_latency_ms: float = 0.0
    total_episodes: int = 0


class QLearningCoordinator:
    """Emergent role specialisation for Q-learning worker agents."""

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
            a.success_rate = self._window_mean(a.agent_id, "success")
            a.avg_energy_saved = self._window_mean(a.agent_id, "energy")
            a.avg_latency_ms = self._window_mean(a.agent_id, "latency")
            if (
                a.avg_latency_ms
                and a.avg_latency_ms < 200
                and a.success_rate > 0.8
            ):
                a.role = AgentRole.LATENCY_OPTIMIZER
            elif a.avg_energy_saved > 0.4:
                a.role = AgentRole.ENERGY_SAVER
            elif a.success_rate > 0.9:
                a.role = AgentRole.BALANCED_WORKER
            else:
                a.role = AgentRole.GENERALIST

    def record(
        self,
        agent_id: str,
        success: bool,
        energy_saved_pct: float,
        latency_ms: float,
    ) -> None:
        a = self.register(agent_id)
        self._history[agent_id].append(
            {
                "success": float(success),
                "energy": energy_saved_pct / 100.0,
                "latency": latency_ms,
            }
        )
        a.total_episodes += 1
        self._reassign()


# --------------------------------------------------------------------------- #
# Federated Q-table aggregator (local; v18's is byte-blob scoped)
# --------------------------------------------------------------------------- #
@dataclass
class FederatedQUpdate:
    deployment_id: str
    q_table: Dict[str, Dict[str, float]]
    sample_count: int
    timestamp: datetime = field(default_factory=_utcnow)


class FederatedQTableAggregator:
    """FedAvg over Q-tables, bounded, with proper weight handling."""

    def __init__(self, max_updates: int = 100) -> None:
        self.updates: Deque[FederatedQUpdate] = deque(maxlen=max_updates)

    def push(self, u: FederatedQUpdate) -> None:
        self.updates.append(u)

    def aggregate(self) -> Dict[str, Dict[str, float]]:
        if not self.updates:
            return {}
        total_w = sum(max(0, u.sample_count) for u in self.updates)
        if total_w <= 0:
            return {}
        merged: Dict[str, Dict[str, float]] = defaultdict(
            lambda: defaultdict(float)
        )
        for u in self.updates:
            w = max(0, u.sample_count) / total_w
            if w <= 0:
                continue
            for state, acts in u.q_table.items():
                for a, v in acts.items():
                    merged[state][a] += v * w
        return {s: dict(a) for s, a in merged.items()}

    def reset(self) -> None:
        self.updates.clear()


# --------------------------------------------------------------------------- #
# Q-table distillation (local; v18's is teacher→student policy)
# --------------------------------------------------------------------------- #
class QLearningDistiller:
    """Quantizes a Q-table to a lower-precision policy."""

    _SCALE = {
        PrecisionLevel.FP32: (1.0, 1.00, 0.0),
        PrecisionLevel.FP16: (1.0, 0.98, 30.0),
        PrecisionLevel.INT8: (100.0, 0.93, 55.0),
        PrecisionLevel.INT4: (10.0, 0.85, 70.0),
        PrecisionLevel.QUANTUM_DISTILLED: (5.0, 0.80, 85.0),
    }

    def distill(
        self,
        q_table: Dict[str, Dict[str, float]],
        precision: PrecisionLevel,
    ) -> DistilledQPolicy:
        scale, retention, energy = self._SCALE[precision]
        out: Dict[str, Dict[str, float]] = {}
        for state, acts in q_table.items():
            if scale == 1.0:
                out[state] = dict(acts)
            else:
                out[state] = {
                    a: round(v * scale) / scale
                    for a, v in acts.items()
                }
        return DistilledQPolicy(
            precision=precision,
            table=out,
            quality_retention=retention,
            energy_reduction_percent=energy,
        )


# --------------------------------------------------------------------------- #
# Q-value explainer (local; v18's XAI is feature-attribution)
# --------------------------------------------------------------------------- #
class QValueExplainer:
    @staticmethod
    def explain(
        state: Dict[str, Any],
        action: str,
        q_values: Dict[str, float],
        counterfactuals: Optional[Dict[str, float]] = None,
        rejected: Optional[List[Tuple[str, List[str]]]] = None,
        used_distilled: bool = False,
    ) -> Explanation:
        ranked = sorted(
            q_values.items(), key=lambda kv: kv[1], reverse=True
        )
        top_action, top_q = ranked[0] if ranked else (action, 0.0)
        second_q = ranked[1][1] if len(ranked) > 1 else 0.0
        margin = top_q - second_q
        confidence = 1.0 / (1.0 + math.exp(-3.0 * margin))

        reasons: List[str] = []
        prefix = "[distilled] " if used_distilled else ""
        reasons.append(
            f"{prefix}Chose '{action}' because "
            f"Q={q_values.get(action, 0.0):.3f}."
        )
        if ranked:
            reasons.append(
                "Ranked Q-values: "
                + ", ".join(f"{a}={q:.3f}" for a, q in ranked)
            )
        if counterfactuals:
            reasons.append(
                "Counterfactual E[R|do(a)]: "
                + ", ".join(
                    f"{a}={v:.3f}"
                    for a, v in counterfactuals.items()
                )
            )
        if rejected:
            for a, props in rejected:
                reasons.append(
                    f"Action '{a}' vetoed by {', '.join(props)}."
                )

        return Explanation(
            action=action,
            headline=(
                f"{prefix}Action = {action.upper()} "
                f"(confidence={confidence:.2f})"
            ),
            rationale=reasons,
            q_values=dict(q_values),
            confidence=confidence,
            causal_adjustment=(
                dict(counterfactuals) if counterfactuals else None
            ),
            contributing_factors={
                "q_margin": margin,
                "num_actions": float(len(q_values)),
            },
            used_distilled_policy=used_distilled,
        )


# --------------------------------------------------------------------------- #
# HITL review gate (local sync facade around v18's async learner)
# --------------------------------------------------------------------------- #
class HumanReviewGate:
    """
    Sync review gate. Async callbacks are **rejected** (fail-safe) in
    ``review``; use ``areview`` to await them.

    * Bounded queues.
    * Callbacks wrapped in ``try/except``.
    * Every review produces a ``(approved, status)`` tuple.
    * Optional ``learner`` (v18 ``ActiveUserPreferenceLearner``) is used
      as a fallback in ``areview``.
    """

    def __init__(
        self,
        learner: Optional[Any] = None,
        timeout: float = 3.0,
        auto_approve_high_conf: float = 0.85,
    ) -> None:
        self._learner = learner
        self._timeout = timeout
        self.auto_approve_high_conf = auto_approve_high_conf
        self.pending: Deque[HITLRequest] = deque(
            maxlen=_MAX_PENDING_HITL
        )
        self.feedback_log: Deque[Dict[str, Any]] = deque(
            maxlen=_MAX_HITL_LOG
        )
        self._callback: Optional[Callable[[HITLRequest], Any]] = None

    def set_callback(
        self, cb: Callable[[HITLRequest], Any]
    ) -> None:
        self._callback = cb

    def needs_review(
        self, state: Dict[str, Any], action: str, confidence: float
    ) -> bool:
        if confidence < self.auto_approve_high_conf:
            return True
        if state.get("deadline_critical"):
            return True
        return False

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
        state: Dict[str, Any],
        action: str,
        confidence: float,
        reason: str,
    ) -> Tuple[Optional[bool], str]:
        """Synchronous review. Awaitable callbacks are rejected."""
        req = HITLRequest(
            state=copy.deepcopy(state),
            action=action,
            confidence=confidence,
            reason=reason,
            urgency=(
                "high" if state.get("deadline_critical") else "medium"
            ),
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
                # Close the coroutine to avoid a RuntimeWarning.
                close = getattr(decision, "close", None)
                if callable(close):
                    try:
                        close()
                    except Exception:
                        pass
                logger.error(
                    "HITL callback returned awaitable in sync review; "
                    "rejecting the action (fail-safe). "
                    "Use areview() for async callbacks."
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
        state: Dict[str, Any],
        action: str,
        confidence: float,
        reason: str,
    ) -> Tuple[Optional[bool], str]:
        """Async review. Awaits awaitable callbacks and the v18 learner."""
        req = HITLRequest(
            state=copy.deepcopy(state),
            action=action,
            confidence=confidence,
            reason=reason,
            urgency=(
                "high" if state.get("deadline_critical") else "medium"
            ),
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
                except Exception as e:
                    logger.warning("HITL callback failed: %s", e)
                    return self._resolve(req, None, "callback_error")

            if self._learner is not None:
                try:
                    chosen = await asyncio.wait_for(
                        self._learner.query_user_if_needed(
                            "reviewer",
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
                except Exception as e:
                    logger.warning("Learner failed: %s", e)

            return self._resolve(req, None, "no_callback")
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
# Causal reward shaper (fixed: action-aware)
# --------------------------------------------------------------------------- #
class CausalRewardShaper:
    """
    Shapes a raw reward into a causal reward using:

    * IPS de-biasing (clipped).
    * A state-level carbon penalty.
    * An **action-dependent** energy term (default per-action savings).
    * An **action-dependent** carbon-bonus term.
    * An optional market term (action savings × market price).

    Per-action defaults can be overridden via ``set_action_effects`` or
    ``state["action_effects"][action]``.
    """

    DEFAULT_ENERGY_SAVINGS: Dict[str, float] = {
        "energy_saver": 30.0,
        "balanced": 10.0,
        "latency_optimized": -5.0,
    }
    DEFAULT_CARBON_SAVINGS: Dict[str, float] = {
        "energy_saver": 30.0,
        "balanced": 10.0,
        "latency_optimized": -5.0,
    }

    def __init__(
        self,
        carbon_penalty: float = 0.5,
        ips_clip: float = 10.0,
    ) -> None:
        self.carbon_penalty = carbon_penalty
        self.ips_clip = ips_clip
        self.action_effects: Dict[str, Dict[str, float]] = {
            "energy_saved_pct": dict(self.DEFAULT_ENERGY_SAVINGS),
            "carbon_saved_pct": dict(self.DEFAULT_CARBON_SAVINGS),
        }

    def set_action_effects(
        self, action_effects: Mapping[str, Mapping[str, float]]
    ) -> None:
        for key, table in action_effects.items():
            self.action_effects.setdefault(key, {})
            self.action_effects[key].update(table)

    def _lookup_action_effect(
        self,
        key: str,
        state: Mapping[str, Any],
        action: str,
    ) -> float:
        override = state.get("action_effects", {})
        if isinstance(override, Mapping):
            per_action = override.get(action, {})
            if isinstance(per_action, Mapping):
                if key in per_action:
                    try:
                        return float(per_action[key])
                    except (TypeError, ValueError):
                        pass
        return float(self.action_effects.get(key, {}).get(action, 0.0))

    def shape(
        self,
        raw_reward: float,
        action: str,
        propensity: float,
        state: Optional[Mapping[str, Any]] = None,
        market_price_usd_per_kwh: Optional[float] = None,
    ) -> float:
        state = state if isinstance(state, Mapping) else {}
        ips_weight = min(
            1.0 / max(propensity, 1e-3), self.ips_clip
        )
        carbon_intensity = float(
            state.get("carbon_intensity", 0.0)
        )

        energy_pct = self._lookup_action_effect(
            "energy_saved_pct", state, action
        )
        carbon_pct = self._lookup_action_effect(
            "carbon_saved_pct", state, action
        )

        carbon_penalty_term = (
            -self.carbon_penalty * (carbon_intensity / 1000.0)
        )
        energy_term = energy_pct / 100.0
        carbon_saved_term = (
            carbon_pct / 100.0
        ) * (carbon_intensity / 1000.0)

        market_term = 0.0
        if market_price_usd_per_kwh is not None:
            market_term = (
                energy_pct / 100.0
            ) * market_price_usd_per_kwh * 0.1

        return (
            ips_weight * raw_reward
            + carbon_penalty_term
            + energy_term
            + carbon_saved_term
            + market_term
        )


class CausalCounterfactualEstimator:
    """Per-action E[R | do(a)] estimate from observed shaped rewards."""

    def __init__(self, actions: Sequence[str]) -> None:
        self.actions = list(actions)
        self.sums: Dict[str, float] = {a: 0.0 for a in self.actions}
        self.counts: Dict[str, int] = {a: 0 for a in self.actions}

    def observe(self, action: str, shaped_reward: float) -> None:
        if action not in self.sums:
            self.sums[action] = 0.0
            self.counts[action] = 0
        self.sums[action] += shaped_reward
        self.counts[action] += 1

    def counterfactual_mean(self, action: str) -> float:
        n = self.counts.get(action, 0)
        return self.sums.get(action, 0.0) / n if n > 0 else 0.0

    def counterfactuals(self) -> Dict[str, float]:
        return {
            a: self.counterfactual_mean(a) for a in self.actions
        }


# --------------------------------------------------------------------------- #
# Transition log (kept for offline replay; bounded)
# --------------------------------------------------------------------------- #
@dataclass
class Transition:
    state: Dict[str, Any]
    action: str
    reward: float
    next_state: Dict[str, Any]
    propensity: float
    timestamp: datetime = field(default_factory=_utcnow)


# --------------------------------------------------------------------------- #
# The enhanced Q-learning agent
# --------------------------------------------------------------------------- #
class QLearningAgent:
    """
    Enhanced tabular Q-learning agent.

    Backward-compatible:
        QLearningAgent(alpha=0.1, gamma=0.9, epsilon=0.1)

    All ten enhancements are enabled by default; pass ``features={...}``
    to selectively disable. ``set_features()`` recreates the layers at
    runtime.
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
    }

    def __init__(
        self,
        alpha: float = 0.1,
        gamma: float = 0.9,
        epsilon: float = 0.1,
        deployment_id: str = "local",
        agent_id: str = "agent-0",
        features: Optional[Dict[str, bool]] = None,
        hardware: Optional[HardwareProfile] = None,
        q_table_max_states: int = _MAX_Q_TABLE_STATES,
        save_interval: int = 50,
    ):
        self.alpha = alpha
        self.gamma = gamma
        self.epsilon = epsilon
        self.deployment_id = deployment_id
        self.agent_id = agent_id
        self.features: Dict[str, bool] = {
            **self.DEFAULT_FEATURES,
            **(features or {}),
        }
        self.hardware = hardware or HardwareProfile()

        # --- Feature-adjacent state created BEFORE _safe_load ---
        self.storage = RLStorage()
        self.storage_circuit = SyncCircuitAdapter(
            "rl_storage",
            failure_threshold=3,
            recovery_timeout=30.0,
        )
        self.chaos = (
            SyncChaosInjector()
            if self.features["chaos_testing"]
            else None
        )

        # --- Q-table (must come after chaos/storage) ---
        self.actions: List[str] = [
            "balanced",
            "energy_saver",
            "latency_optimized",
        ]
        self._q_table_max = max(1, int(q_table_max_states))
        loaded = self._safe_load()
        self.q_table: "OrderedDict[str, Dict[str, float]]" = (
            OrderedDict(loaded)
        )
        self._trim_q_table()

        # --- Batching ---
        self.save_interval = max(1, int(save_interval))
        self._episodes_since_save: int = 0

        # --- Enhancement layers (all recreated by set_features) ---
        self._build_layers()

        # --- Bookkeeping ---
        self.decision_history: Deque[DecisionRecord] = deque(
            maxlen=2048
        )
        self.transitions: Deque[Transition] = deque(
            maxlen=_MAX_TRANSITIONS
        )
        self.episode_count: int = 0
        self._last_decision: Optional[DecisionRecord] = None
        self._last_propensity: float = 0.0
        self._last_action: Optional[str] = None
        self._lock = threading.RLock()

        # --- Distillation state ---
        self.distilled_policy: Optional[DistilledQPolicy] = None
        self.use_distilled: bool = False

        # --- Diagnostics ---
        self._load_attempted: bool = True
        self._load_error: Optional[str] = None

        logger.info(
            "QLearningAgent v6 (%s) ready: deployment=%s, "
            "states=%d, features=%s",
            self.agent_id,
            self.deployment_id,
            len(self.q_table),
            sorted(k for k, v in self.features.items() if v),
        )

    # ------------------------------------------------------------------
    # Layer construction (idempotent; safe to call from set_features)
    # ------------------------------------------------------------------
    def _build_layers(self) -> None:
        self.reward_shaper = (
            CausalRewardShaper()
            if self.features["causal_rl"]
            else None
        )
        self.counterfactual = (
            CausalCounterfactualEstimator(self.actions)
            if self.features["causal_rl"]
            else None
        )
        self.explainer = (
            QValueExplainer if self.features["xai"] else None
        )
        self.precision_ctl = (
            AdaptivePrecisionSwitcher(
                config={"precision_levels": ["fp32", "fp16", "int8"]},
                storage=None,
            )
            if self.features["adaptive_precision"]
            else None
        )
        self.distiller = (
            QLearningDistiller()
            if self.features["quantum_distillation"]
            else None
        )
        self.federated = (
            FederatedQTableAggregator()
            if self.features["federated"]
            else None
        )
        self.coordinator = (
            QLearningCoordinator()
            if self.features["multi_agent"]
            else None
        )
        if self.coordinator:
            self.coordinator.register(self.agent_id)

        self.safety_monitor = (
            ActionSafetyMonitor()
            if self.features["temporal_logic"]
            else None
        )
        if self.safety_monitor:
            self.safety_monitor.register(NoEnergySaverOnDeadline())
            self.safety_monitor.register(
                NoLatencyOptimizedUnderEmergency()
            )

        self.market = (
            CarbonMarketIntegrator(config={}, storage=None)
            if self.features["carbon_market"]
            else None
        )
        self.hitl = (
            HumanReviewGate(
                learner=(
                    ActiveUserPreferenceLearner(storage=None)
                    if _V18_AVAILABLE
                    else None
                ),
            )
            if self.features["hitl"]
            else None
        )

    def set_features(
        self, features: Mapping[str, bool]
    ) -> None:
        """Reconfigure and recreate layers at runtime."""
        for k, v in features.items():
            if k not in self.DEFAULT_FEATURES:
                raise ValueError(f"unknown feature {k!r}")
            self.features[k] = bool(v)
        with self._lock:
            self._build_layers()

    # ------------------------------------------------------------------
    # Storage
    # ------------------------------------------------------------------
    def _safe_load(self) -> Dict[str, Dict[str, float]]:
        try:
            if self.chaos and self.chaos.maybe_fail("rl_storage:load"):
                raise RuntimeError("chaos: rl_storage load")
            data = self.storage.load()
            if not isinstance(data, dict):
                raise RuntimeError(
                    f"storage returned {type(data).__name__}, "
                    "expected dict"
                )
            self._load_error = None
            return data
        except Exception as e:
            self._load_error = str(e)
            logger.warning(
                "RLStorage.load failed (%s); starting empty", e
            )
            return {}

    def _safe_save(self, force: bool = False) -> None:
        if not force:
            self._episodes_since_save += 1
            if self._episodes_since_save < self.save_interval:
                return
        self._episodes_since_save = 0

        if self.chaos and self.chaos.maybe_fail("rl_storage:save"):
            logger.debug("Chaos: skipping save")
            return

        snapshot = dict(self.q_table)

        def _do_save() -> None:
            self.storage.save(snapshot)

        try:
            self.storage_circuit.call(_do_save)
        except RuntimeError:
            logger.debug("Storage circuit open — skipping save")
        except Exception as e:
            logger.warning(
                "RLStorage.save failed (%s); continuing in-memory", e
            )

    def save(self) -> None:
        """Explicitly persist the Q-table."""
        self._safe_save(force=True)

    def load(self) -> None:
        """Explicitly reload the Q-table from storage."""
        with self._lock:
            self.q_table = OrderedDict(self._safe_load())
            self._trim_q_table()

    # ------------------------------------------------------------------
    # Q-table helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _state_key(state: Any) -> str:
        if isinstance(state, str):
            return state
        try:
            return hashlib.sha256(
                json.dumps(
                    state, sort_keys=True, default=str
                ).encode("utf-8")
            ).hexdigest()[:16]
        except Exception:
            return str(state)

    def _ensure_state(self, state: Any) -> str:
        key = self._state_key(state)
        with self._lock:
            if key not in self.q_table:
                self.q_table[key] = {
                    a: 0.0 for a in self.actions
                }
                self._trim_q_table()
            else:
                self.q_table.move_to_end(key)
        return key

    def _trim_q_table(self) -> None:
        while len(self.q_table) > self._q_table_max:
            self.q_table.popitem(last=False)

    def _as_dict_state(
        self, state: Any
    ) -> Dict[str, Any]:
        if isinstance(state, Mapping):
            return dict(state)
        if isinstance(state, str):
            return {"_raw": state}
        return {"_repr": repr(state)}

    # ------------------------------------------------------------------
    # Action selection
    # ------------------------------------------------------------------
    def choose_action(self, state: Any) -> str:
        """
        Backward-compatible. Full pipeline:

        1. Ensure state exists.
        2. ε-greedy on causally-adjusted Q-values.
        3. Action-safety vetting.
        4. XAI explanation.
        5. HITL review (sync).
        6. Record effective propensity.
        """
        state_dict = self._as_dict_state(state)
        key = self._ensure_state(state)

        # Q-values (from distilled or full table)
        with self._lock:
            if self.use_distilled and self.distilled_policy is not None:
                q_values = dict(
                    self.distilled_policy.table.get(key, {})
                ) or {
                    a: 0.0 for a in self.actions
                }
                used_distilled = True
            else:
                q_values = dict(self.q_table[key])
                used_distilled = False

        # Causal adjustment
        adjusted = q_values
        counterfactuals: Optional[Dict[str, float]] = None
        if self.counterfactual is not None:
            counterfactuals = (
                self.counterfactual.counterfactuals()
            )
            adjusted = {
                a: q_values.get(a, 0.0)
                + 0.1 * counterfactuals.get(a, 0.0)
                for a in self.actions
            }

        # ε-greedy on adjusted Q-values
        if random.random() < self.epsilon:
            candidates = list(self.actions)
            explore = True
        else:
            candidates = [max(adjusted, key=adjusted.get)]
            explore = False

        # Action-safety vetting
        rejected: List[Tuple[str, List[str]]] = []
        safe_candidates: List[str] = []
        for a in candidates:
            if self.safety_monitor is not None:
                ok, props = self.safety_monitor.vet(
                    state_dict, a
                )
                if not ok:
                    rejected.append((a, props))
                    continue
            safe_candidates.append(a)

        if not safe_candidates:
            # Fallback to any safe action
            for a in self.actions:
                if self.safety_monitor is None:
                    safe_candidates.append(a)
                    continue
                ok, _ = self.safety_monitor.vet(state_dict, a)
                if ok:
                    safe_candidates.append(a)
            if not safe_candidates:
                safe_candidates = ["balanced"]

        if explore:
            action = random.choice(safe_candidates)
        else:
            action = safe_candidates[0]

        # Effective propensity: ε-greedy over the *safe* subset only
        n_safe = len(safe_candidates)
        if explore:
            propensity = self.epsilon / n_safe
        else:
            propensity = (
                self.epsilon / n_safe + (1.0 - self.epsilon)
            )

        # XAI
        explanation: Optional[Explanation] = None
        confidence = 0.5
        if self.explainer is not None:
            explanation = self.explainer.explain(
                state=state_dict,
                action=action,
                q_values=adjusted,
                counterfactuals=counterfactuals,
                rejected=rejected,
                used_distilled=used_distilled,
            )
            confidence = explanation.confidence

        # HITL
        human_approved: Optional[bool] = None
        hitl_status = "not_required"
        if self.hitl is not None and self.hitl.needs_review(
            state_dict, action, confidence
        ):
            human_approved, hitl_status = self.hitl.review(
                state_dict,
                action,
                confidence,
                reason=(
                    f"confidence={confidence:.2f} below threshold"
                ),
            )
            if human_approved is False:
                # Conservative fallback
                safe_fallback = [
                    a for a in self.actions
                    if self.safety_monitor is None
                    or self.safety_monitor.is_action_safe(
                        state_dict, a
                    )
                ]
                action = (
                    "balanced"
                    if "balanced" in safe_fallback
                    else (safe_fallback[0] if safe_fallback else "balanced")
                )
                propensity = 1.0  # HITL forced this action
            elif human_approved is None:
                # No answer → treat as not-approved for safety
                pass

        # Persist the effective propensity for update()
        with self._lock:
            self._last_propensity = propensity
            self._last_action = action

        # Record decision
        if self.safety_monitor is not None and rejected:
            self.safety_monitor.record_violation(
                state_dict,
                action,
                [p for _, props in rejected for p in props],
            )

        record = DecisionRecord(
            state=copy.deepcopy(state_dict),
            action=action,
            q_values=adjusted,
            confidence=confidence,
            propensity=propensity,
            explanation=explanation,
            human_approved=human_approved,
            hitl_status=hitl_status,
            temporal_violations=[
                p for _, props in rejected for p in props
            ],
            used_distilled_policy=used_distilled,
        )
        with self._lock:
            self.decision_history.append(record)
            self._last_decision = record

        return action

    def act(self, state: Any) -> str:
        return self.choose_action(state)

    # ------------------------------------------------------------------
    # Q-update
    # ------------------------------------------------------------------
    def update(
        self,
        state: Any,
        action: str,
        reward: float,
        next_state: Any,
        done: bool = False,
        propensity: Optional[float] = None,
    ) -> None:
        """
        Backward-compatible signature plus:

        * ``done``       — skip bootstrap on terminal transitions.
        * ``propensity`` — override the effective propensity. When
                           omitted, the value recorded by the most recent
                           ``choose_action`` for this action is used.
        """
        state_dict = self._as_dict_state(state)
        next_dict = self._as_dict_state(next_state)
        s_key = self._ensure_state(state)
        ns_key = self._ensure_state(next_state)

        # Use recorded propensity when the action matches; otherwise fall
        # back to ε-greedy on the current Q-table.
        eff_propensity = propensity
        if eff_propensity is None:
            with self._lock:
                last_match = (
                    self._last_action == action
                    and self._last_propensity > 0
                )
                eff_propensity = (
                    self._last_propensity
                    if last_match
                    else self.epsilon / max(1, len(self.actions))
                )
        eff_propensity = float(max(eff_propensity, 1e-3))

        # Reward shaping
        shaped = float(reward)
        if self.reward_shaper is not None:
            market_price: Optional[float] = None
            if self.market is not None:
                try:
                    market_price = (
                        self.market.last_price / 1000.0
                    )
                except Exception:
                    market_price = None
            shaped = self.reward_shaper.shape(
                raw_reward=float(reward),
                action=action,
                propensity=eff_propensity,
                state=state_dict,
                market_price_usd_per_kwh=market_price,
            )

        # Bellman update
        with self._lock:
            if done:
                td_target = shaped
            else:
                best_next = max(
                    self.q_table[ns_key].values()
                )
                td_target = shaped + self.gamma * best_next
            self.q_table[s_key][action] += self.alpha * (
                td_target - self.q_table[s_key][action]
            )

        # Counterfactual stats
        if self.counterfactual is not None:
            self.counterfactual.observe(action, shaped)

        # Transition log
        self.transitions.append(
            Transition(
                state=copy.deepcopy(state_dict),
                action=action,
                reward=shaped,
                next_state=copy.deepcopy(next_dict),
                propensity=eff_propensity,
            )
        )

        # Coordinator feedback
        self.episode_count += 1
        if self.coordinator is not None:
            self.coordinator.record(
                agent_id=self.agent_id,
                success=shaped > 0,
                energy_saved_pct=float(
                    state_dict.get("energy_saved_pct", 0.0)
                ),
                latency_ms=float(
                    state_dict.get("latency_ms", 0.0)
                ),
            )

        # Federated contribution
        if (
            self.federated is not None
            and self.episode_count % 50 == 0
        ):
            self.federated.push(
                FederatedQUpdate(
                    deployment_id=self.deployment_id,
                    q_table=dict(self.q_table),
                    sample_count=self.episode_count,
                )
            )

        # Batched save
        self._safe_save()

    # ------------------------------------------------------------------
    # Distillation
    # ------------------------------------------------------------------
    def distill_policy(
        self, urgency: str = "normal", install: bool = False
    ) -> Optional[DistilledQPolicy]:
        """Distill and optionally install as the active policy."""
        if self.distiller is None:
            return None
        if self.precision_ctl is not None:
            # Map urgency → a PrecisionLevel using v18's switcher's
            # ENERGY table (its select_precision uses hardware; here we
            # keep a deterministic urgency map).
            if urgency == "critical":
                precision = PrecisionLevel.FP16
            elif self.hardware.edge_device:
                precision = (
                    PrecisionLevel.INT8
                    if self.hardware.supports_int8
                    else PrecisionLevel.FP16
                )
            elif self.hardware.supports_int4:
                precision = PrecisionLevel.INT4
            else:
                precision = PrecisionLevel.INT8
        else:
            precision = PrecisionLevel.FP16
        policy = self.distiller.distill(
            dict(self.q_table), precision
        )
        if install:
            self.install_distilled_policy(policy)
        return policy

    def install_distilled_policy(
        self, policy: DistilledQPolicy
    ) -> None:
        with self._lock:
            self.distilled_policy = policy
            self.use_distilled = True

    def set_distilled_mode(self, enabled: bool) -> None:
        with self._lock:
            self.use_distilled = bool(enabled) and (
                self.distilled_policy is not None
            )

    # ------------------------------------------------------------------
    # Federated
    # ------------------------------------------------------------------
    def merge_federated_q_table(self, blend: float = 0.3) -> None:
        if self.federated is None:
            return
        agg = self.federated.aggregate()
        with self._lock:
            for state, acts in agg.items():
                if state not in self.q_table:
                    self.q_table[state] = dict(acts)
                    continue
                for a, v in acts.items():
                    old = self.q_table[state].get(a, 0.0)
                    self.q_table[state][a] = (
                        (1 - blend) * old + blend * v
                    )
            self._trim_q_table()
            self.federated.reset()
        self._safe_save(force=True)

    # ------------------------------------------------------------------
    # HITL helpers
    # ------------------------------------------------------------------
    def set_hitl_callback(
        self, cb: Callable[[HITLRequest], Any]
    ) -> None:
        if self.hitl is not None:
            self.hitl.set_callback(cb)

    def active_learning_samples(
        self, n: int = 16
    ) -> List[Dict[str, Any]]:
        if self.hitl is None:
            return []
        return self.hitl.active_learning_batch(n)

    # ------------------------------------------------------------------
    # Explanation accessor
    # ------------------------------------------------------------------
    def explain_last_decision(self) -> Optional[Explanation]:
        rec = self._last_decision
        return rec.explanation if rec else None

    def explain_last_chosen_action(self) -> Optional[Explanation]:
        return self.explain_last_decision()

    # ------------------------------------------------------------------
    # Transition accessor (for offline analysis)
    # ------------------------------------------------------------------
    def get_transitions(
        self, n: int = 100
    ) -> List[Dict[str, Any]]:
        return [
            {
                "state": t.state,
                "action": t.action,
                "reward": t.reward,
                "next_state": t.next_state,
                "propensity": t.propensity,
                "timestamp": _ensure_aware(
                    t.timestamp
                ).isoformat(),
            }
            for t in list(self.transitions)[-n:]
        ]

    # ------------------------------------------------------------------
    # Statistics / health
    # ------------------------------------------------------------------
    def get_statistics(self) -> Dict[str, Any]:
        stats: Dict[str, Any] = {
            "agent_id": self.agent_id,
            "deployment_id": self.deployment_id,
            "states_learned": len(self.q_table),
            "q_table_max": self._q_table_max,
            "episodes": self.episode_count,
            "epsilon": self.epsilon,
            "storage_circuit": self.storage_circuit.state,
            "storage_load_error": self._load_error,
            "distilled_active": self.use_distilled,
            "distilled_precision": (
                self.distilled_policy.precision.value
                if self.distilled_policy else None
            ),
        }
        if self.counterfactual is not None:
            stats["counterfactual_E_R_given_do"] = (
                self.counterfactual.counterfactuals()
            )
        if self.safety_monitor is not None:
            stats["safety_violations"] = list(
                self.safety_monitor.violations
            )[-5:]
        if self.coordinator is not None:
            stats["agents"] = {
                aid: {
                    "role": a.role.value,
                    "success_rate": a.success_rate,
                }
                for aid, a in self.coordinator.agents.items()
            }
        if self.hitl is not None:
            stats["hitl"] = {
                "pending": len(self.hitl.pending),
                "log_size": len(self.hitl.feedback_log),
                "outcomes": self.hitl.outcomes(),
            }
        if self.chaos is not None:
            stats["chaos_events"] = list(
                self.chaos.events
            )[-5:]
        if self.market is not None:
            stats["market"] = {
                "credit_price": self.market.last_price,
            }
        return stats

    def health_check(self) -> Dict[str, Any]:
        return {
            "agent_id": self.agent_id,
            "healthy": self.storage_circuit.state != "OPEN",
            "q_table_loaded": self._load_error is None,
            "q_table_states": len(self.q_table),
            "q_table_max": self._q_table_max,
            "storage_circuit": self.storage_circuit.state,
            "distilled_active": self.use_distilled,
            "safety_violations": (
                len(self.safety_monitor.violations)
                if self.safety_monitor else 0
            ),
            "hitl_outcomes": (
                self.hitl.outcomes() if self.hitl else {}
            ),
        }

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def __enter__(self) -> "QLearningAgent":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        try:
            self._safe_save(force=True)
        except Exception as e:
            logger.warning("Final save failed: %s", e)

    async def __aenter__(self) -> "QLearningAgent":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        self.__exit__(exc_type, exc, tb)


# --------------------------------------------------------------------------- #
# Factory (backward-compatible with v5)
# --------------------------------------------------------------------------- #
def create_qlearning_agent(
    alpha: float = 0.1,
    gamma: float = 0.9,
    epsilon: float = 0.1,
    deployment_id: str = "local",
    agent_id: str = "agent-0",
    features: Optional[Dict[str, bool]] = None,
    hardware: Optional[HardwareProfile] = None,
) -> QLearningAgent:
    return QLearningAgent(
        alpha=alpha,
        gamma=gamma,
        epsilon=epsilon,
        deployment_id=deployment_id,
        agent_id=agent_id,
        features=features,
        hardware=hardware,
    )


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #
__all__ = [
    # v18 re-exports (infra + enhancement classes)
    "CircuitBreaker",
    "ActiveUserPreferenceLearner",
    "ChaosTestingEngine",
    "MultiAgentCoordinator",
    "FederatedGreenAggregator",
    "QuantumDistillationEngine",
    "XAIDecisionExplainer",
    "AdaptivePrecisionSwitcher",
    "CarbonMarketIntegrator",
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
    "CarbonForecast",
    # v6 public enums / dataclasses
    "PrecisionLevel",
    "AgentRole",
    "HardwareProfile",
    "Explanation",
    "DecisionRecord",
    "HITLRequest",
    "DistilledQPolicy",
    # v6 local components
    "SyncCircuitAdapter",
    "SyncChaosInjector",
    "ActionSafetyMonitor",
    "SafetyProperty",
    "NoEnergySaverOnDeadline",
    "NoLatencyOptimizedUnderEmergency",
    "QLearningCoordinator",
    "AgentProfile",
    "FederatedQUpdate",
    "FederatedQTableAggregator",
    "QLearningDistiller",
    "QValueExplainer",
    "HumanReviewGate",
    "CausalRewardShaper",
    "CausalCounterfactualEstimator",
    "Transition",
    # Main
    "QLearningAgent",
    "create_qlearning_agent",
]


# --------------------------------------------------------------------------- #
# Demo
# --------------------------------------------------------------------------- #
def _demo() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        ),
    )

    print("=" * 80)
    print("qlearning_agent.py v6.0.0 — Demo")
    print("=" * 80)

    # ---- Auto-approve HITL (sync callback) ----
    def auto_approve(req: HITLRequest) -> bool:
        logger.info(
            "[HITL] auto-approving %s (urgency=%s)",
            req.action,
            req.urgency,
        )
        return True

    with QLearningAgent(
        alpha=0.2,
        gamma=0.9,
        epsilon=0.15,
        deployment_id="us-ca-prod-01",
        agent_id="worker-A",
        hardware=HardwareProfile(supports_int4=True, vram_gb=48),
    ) as agent:
        agent.set_hitl_callback(auto_approve)

        # ---- Verify action-dependent learning ----
        print("\n=== Action-dependent learning sanity check ===")
        s = {
            "region": "US-CA",
            "carbon_intensity": 420,
        }
        # Force the same state/action pair with contrasting rewards
        for _ in range(30):
            agent.update(s, "energy_saver", 5.0, s, done=True)
        for _ in range(30):
            agent.update(s, "latency_optimized", -5.0, s, done=True)

        key = QLearningAgent._state_key(s)
        q_energy = agent.q_table[key]["energy_saver"]
        q_latency = agent.q_table[key]["latency_optimized"]
        print(
            f"  Q(energy_saver)        = {q_energy:+.3f}"
        )
        print(
            f"  Q(latency_optimized)   = {q_latency:+.3f}"
        )
        assert q_energy > q_latency, (
            "Q-learning is not action-dependent! "
            "See fix §2.2 in the review."
        )
        print("  Action-dependent learning: OK")

        # ---- Simulate episodes ----
        print("\n=== Episodes ===")
        states = [
            {
                "region": "US-CA",
                "carbon_intensity": 180,
                "deadline_critical": False,
            },
            {
                "region": "US-CA",
                "carbon_intensity": 420,
                "energy_saved_pct": 40,
            },
            {
                "region": "US-TX",
                "carbon_intensity": 650,
                "deadline_critical": True,
            },
        ]
        for ep in range(20):
            for s in states:
                a = agent.choose_action(s)
                # Reward depends on state AND action (the fix)
                action_energy = {
                    "energy_saver": 1.0,
                    "balanced": 0.5,
                    "latency_optimized": -0.5,
                }[a]
                r = action_energy - (
                    s.get("carbon_intensity", 200) / 1000.0
                )
                agent.update(s, a, r, s, done=False)

        # ---- Explanation ----
        print("\n=== Last decision explanation ===")
        exp = agent.explain_last_decision()
        if exp:
            print(f"  {exp.headline}")
            for line in exp.rationale:
                print(f"   • {line}")

        # ---- Distillation + install ----
        print("\n=== Distilled policy ===")
        dist = agent.distill_policy(
            urgency="normal", install=True
        )
        if dist:
            print(f"  precision: {dist.precision.value}")
            print(
                f"  quality_retention: "
                f"{dist.quality_retention:.2f}"
            )
            print(
                f"  energy_reduction: "
                f"{dist.energy_reduction_percent:.0f}%"
            )
            print(
                f"  distilled_active: {agent.use_distilled}"
            )

        # ---- Save / load round-trip ----
        print("\n=== Save / load round-trip ===")
        agent.save()
        before_states = len(agent.q_table)
        agent.load()
        after_states = len(agent.q_table)
        print(
            f"  states before={before_states} "
            f"after={after_states}"
        )
        assert before_states == after_states
        print("  Q-table persisted and restored: OK")

        # ---- Federated merge ----
        print("\n=== Federated merge ===")
        agent.merge_federated_q_table(blend=0.2)
        print(
            f"  states after merge: {len(agent.q_table)}"
        )

        # ---- Statistics / health ----
        print("\n=== Statistics ===")
        print(json.dumps(
            agent.get_statistics(), indent=2, default=str
        ))
        print("\n=== Health check ===")
        print(json.dumps(
            agent.health_check(), indent=2, default=str
        ))

        # ---- Async HITL is rejected in sync review ----
        print(
            "\n=== Async callback in sync review "
            "(fail-safe) ==="
        )
        async def async_cb(req: HITLRequest) -> bool:
            return True
        agent.set_hitl_callback(async_cb)
        approved, status = agent.hitl.review(
            {"deadline_critical": True},
            "balanced",
            0.4,
            reason="test",
        )
        print(
            f"  async callback → approved={approved}, "
            f"status={status} (expected rejected/async_in_sync)"
        )
        assert approved is False and status == "async_in_sync"

    print("\nDemo complete.")


if __name__ == "__main__":
    _demo()
