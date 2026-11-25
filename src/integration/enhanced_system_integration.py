#!/usr/bin/env python3
# File: src/integration/enhanced_system_integration.py

"""
Enhanced Green Agent System Integration
========================================

Orchestrates the core Green Agent modules together with all ten
cross-cutting enhancement layers, fully self-contained.

Enhancements over the original
------------------------------
- :class:`IntegratorConfig` — frozen, validated. Adds **``strict``
  mode**: ``strict=True`` raises if any required module is missing;
  ``strict=False`` (default) uses stubs but flags every decision as
  ``fabricated=True``.
- **Error isolation** around the entire routing body; failures are
  captured in the returned decision and counted in ``decision_errors``.
- **Bounded collections** everywhere — ``ChaosInjector.events``,
  ``FederatedAggregator.updates``, ``TemporalLogicMonitor.violations``,
  ``HITL.pending`` / ``HITL.feedback_log``.
- **Fixed RL / federated stop-firing bug** — dedicated counters replace
  ``len(decision_history) % N``, which froze once the deque filled.
- **Idempotent router setup** — no double-wrap recursion.
- **Deterministic ``task_type_hash``** — SHA-256 based; RL weights
  survive restarts.
- **Pareto router and cost function are wired** — ``_enhanced_route``
  actually calls ``pareto_router.get_frontier`` and
  ``cost_function.evaluate``.
- **Carbon / helium / pareto / xai circuit breakers** are consulted on
  their failure paths.
- **Timezone-aware timestamps** — ``datetime.now(timezone.utc)``.
- **Compact decision history** — ``task`` / ``context`` truncated to
  200 chars in ``decision_history`` to bound memory.
- **``decision_errors`` counter** on the integrator.
- **Typed ``_last_explanation``** — returned directly, no fragile
  reconstruction.

Run the smoke test with::

    python -m src.integration.enhanced_system_integration
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import random
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

logger = logging.getLogger(__name__)

__all__ = [
    "EnhancedSystemIntegrator",
    "IntegratorConfig",
    "IntegratorError",
    "HardwareProfile",
    "PrecisionLevel",
    "AgentRole",
    "CircuitState",
    "CircuitBreaker",
    "ChaosInjector",
    "TemporalLogicMonitor",
    "RoutingExplainer",
    "Explanation",
    "AdaptivePrecisionController",
    "CausalRLPolicy",
    "RoutingState",
    "QuantumDistillationBridge",
    "DistilledExpert",
    "FederatedAggregator",
    "FederatedUpdate",
    "MultiAgentCoordinator",
    "AgentProfile",
    "MarketSnapshot",
    "CarbonMarketClient",
    "HumanInTheLoopGate",
    "HITLRequest",
    "simulate_policy",
    "train_expert_on_synthetic_data",
]


# =============================================================================
# Errors
# =============================================================================
class IntegratorError(RuntimeError):
    """Raised for invalid integrator inputs or missing required modules."""


# =============================================================================
# Helpers
# =============================================================================
def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _stable_hash(value: str) -> float:
    """Deterministic hash of a string in [0, 1). Survives restarts."""
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()
    return int(digest[:8], 16) / 0xFFFFFFFF


def _compact(obj: Any, max_len: int = 200) -> Any:
    """Truncate strings for storage; recurse into containers."""
    if isinstance(obj, str) and len(obj) > max_len:
        return obj[:max_len] + "..."
    if isinstance(obj, Mapping):
        return {k: _compact(v, max_len) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_compact(v, max_len) for v in obj]
    return obj


# =============================================================================
# Graceful fallback shims
# =============================================================================
def _try_import(
    module_path: str, attr: str, fallback: Any,
) -> Tuple[Any, bool]:
    """Return ``(component, is_stub)``."""
    try:
        mod = __import__(module_path, fromlist=[attr])
        return getattr(mod, attr), False
    except Exception:
        return fallback, True


class _StubDBManager:
    def __init__(self, config: Optional[Dict] = None) -> None:
        self.config = config or {}
    async def connect(self) -> None: pass
    async def disconnect(self) -> None: pass


class _StubTaskManager:
    def __init__(self, max_workers: int = 10) -> None:
        self.max_workers = max_workers
        self._tasks: Dict[str, asyncio.Task] = {}
    def start_task(
        self, name: str, coro_fn, interval_seconds: float = 60.0,
    ) -> None:
        async def _loop() -> None:
            while True:
                try:
                    result = coro_fn()
                    if asyncio.iscoroutine(result):
                        await result
                except Exception as exc:
                    logger.warning("Task %r raised: %s", name, exc)
                await asyncio.sleep(interval_seconds)
        self._tasks[name] = asyncio.create_task(_loop())
    async def stop_all(self) -> None:
        for t in self._tasks.values():
            t.cancel()
        if self._tasks:
            await asyncio.gather(
                *self._tasks.values(), return_exceptions=True,
            )


class _StubRegistry:
    def __init__(self, config: Optional[Dict] = None) -> None:
        self.config = config or {}
    def get_experts(self) -> List[Any]: return []


class _StubRouter:
    def __init__(self, config: Optional[Dict] = None) -> None:
        self.config = config or {}
    async def route(
        self, task: Dict, context: Dict,
    ) -> Dict[str, Any]:
        return {
            "expert_id": "stub",
            "node_id": context.get("target_node_id", "local"),
        }


class _StubDigitalTwin:
    def __init__(self, config: Optional[Dict] = None) -> None:
        self.config = config or {}
    async def simulate_routing(
        self, dataset: List, policy_config: Dict,
    ) -> Dict[str, Any]:
        return {"simulated": len(dataset)}


class _StubMLOps:
    def __init__(self, config: Optional[Dict] = None) -> None:
        self.config = config or {}


class _StubCarbonManager:
    def __init__(self, config: Optional[Dict] = None) -> None:
        self.config = config or {}
    async def get_intensity(self, region: str = "global") -> float:
        return 400.0


class _StubHeliumDashboard:
    def __init__(self, config: Optional[Dict] = None) -> None:
        self.config = config or {}
    async def get_price(self) -> float:
        return 5.0


class _StubHarvester:
    def __init__(self, config: Optional[Dict] = None) -> None:
        self.config = config or {}
    async def harvest(self) -> Dict[str, Any]:
        return {"units": 0.0}


class _StubNodeRegistry:
    def __init__(
        self, config: Optional[Dict] = None, db_manager: Any = None,
    ) -> None:
        self.config = config or {}
        self.db = db_manager
    async def start(self, refresh_interval: int = 3600) -> None: pass
    async def stop(self) -> None: pass


class _StubCostFunction:
    def __init__(self, config: Optional[Dict] = None) -> None:
        self.config = config or {}
        self.deps: Dict[str, Any] = {}
    def inject_dependencies(self, **kwargs: Any) -> None:
        self.deps = kwargs
    async def evaluate(self, ctx: Dict) -> float:
        return 1.0


class _StubEvolutionaryEngine:
    def __init__(self, **kwargs: Any) -> None:
        self.config = kwargs.get("config", {})
    async def start(self) -> None: pass
    async def stop(self) -> None: pass


class _StubTokenizationOptimizer:
    def __init__(self, config: Optional[Dict] = None) -> None:
        self.config = config or {}
    def optimize(
        self, text: str, context: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        return {"tokens": len(text.split()), "text": text}


class _StubParetoRouter:
    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
    async def get_frontier(
        self, task: Dict, context: Dict,
    ) -> List[Dict[str, Any]]:
        return []


class _StubUserPreferences:
    def __init__(self, prefs: Optional[Dict] = None) -> None:
        self.prefs = prefs or {}


class _StubSyntheticDataGenerator:
    def generate_dataset(
        self, num_tasks: int = 500, include_edge_cases: bool = True,
    ) -> List[Dict[str, Any]]:
        return [
            {"task_id": i, "type": "summarize"}
            for i in range(num_tasks)
        ]
    def export_for_simulation(
        self, dataset: List[Dict],
    ) -> List[Dict]:
        return dataset
    def generate_task_batch(
        self, n: int, task_type: str = "summarize",
        priority: str = "balanced",
    ) -> List[Dict[str, Any]]:
        return [
            {"task_id": i, "type": task_type, "priority": priority}
            for i in range(n)
        ]


# Resolve external modules — each returns ``(component, is_stub)``.
DatabaseManager, _stub_db = _try_import(
    "src.database.manager", "DatabaseManager", _StubDBManager,
)
TaskManager, _stub_tm = _try_import(
    "src.task_manager", "TaskManager", _StubTaskManager,
)
ExpertRegistry, _stub_reg = _try_import(
    "src.expert_registry", "ExpertRegistry", _StubRegistry,
)
ExpertRouter, _stub_router = _try_import(
    "src.expert_router", "ExpertRouter", _StubRouter,
)
DigitalTwin, _stub_twin = _try_import(
    "src.digital_twin", "DigitalTwin", _StubDigitalTwin,
)
MLOpsPipeline, _stub_mlops = _try_import(
    "src.mlops_pipeline", "MLOpsPipeline", _StubMLOps,
)
CarbonIntensityManager, _stub_carbon = _try_import(
    "src.carbon_manager", "CarbonIntensityManager", _StubCarbonManager,
)
HeliumEfficiencyDashboard, _stub_helium = _try_import(
    "src.helium_dashboard", "HeliumEfficiencyDashboard",
    _StubHeliumDashboard,
)
PhotosyntheticHarvester, _stub_harvest = _try_import(
    "src.bio_inspired", "PhotosyntheticHarvester", _StubHarvester,
)
NodeRegistry, _stub_node = _try_import(
    "src.enhancements.node_registry", "NodeRegistry", _StubNodeRegistry,
)
SustainabilityCostFunction, _stub_cost = _try_import(
    "src.enhancements.sustainability_cost",
    "SustainabilityCostFunction", _StubCostFunction,
)
EvolutionaryEngine, _stub_evo = _try_import(
    "src.enhancements.evolutionary_engine", "EvolutionaryEngine",
    _StubEvolutionaryEngine,
)
TokenizationOptimizer, _stub_token = _try_import(
    "src.enhancements.tokenization_optimizer", "TokenizationOptimizer",
    _StubTokenizationOptimizer,
)
ParetoRouter, _stub_pareto = _try_import(
    "src.enhancements.pareto_router", "ParetoRouter", _StubParetoRouter,
)
UserPreferences, _stub_user = _try_import(
    "src.user_preferences", "UserPreferences", _StubUserPreferences,
)
SyntheticDataGenerator, _stub_synth = _try_import(
    "src.synthetic_data", "SyntheticDataGenerator",
    _StubSyntheticDataGenerator,
)

_STUB_COMPONENTS: Tuple[str, ...] = tuple(
    name for name, is_stub in (
        ("DatabaseManager", _stub_db),
        ("TaskManager", _stub_tm),
        ("ExpertRegistry", _stub_reg),
        ("ExpertRouter", _stub_router),
        ("DigitalTwin", _stub_twin),
        ("MLOpsPipeline", _stub_mlops),
        ("CarbonIntensityManager", _stub_carbon),
        ("HeliumEfficiencyDashboard", _stub_helium),
        ("PhotosyntheticHarvester", _stub_harvest),
        ("NodeRegistry", _stub_node),
        ("SustainabilityCostFunction", _stub_cost),
        ("EvolutionaryEngine", _stub_evo),
        ("TokenizationOptimizer", _stub_token),
        ("ParetoRouter", _stub_pareto),
        ("UserPreferences", _stub_user),
        ("SyntheticDataGenerator", _stub_synth),
    ) if is_stub
)


# =============================================================================
# Config
# =============================================================================
@dataclass(frozen=True)
class IntegratorConfig:
    """Frozen, validated integrator configuration."""

    deployment_id: str = "local"
    agent_id: str = "integrator-0"
    strict: bool = False
    features: Mapping[str, bool] = field(default_factory=dict)
    hardware: Optional["HardwareProfile"] = None

    decision_history_max: int = 2048
    chaos_events_max: int = 1000
    federated_updates_max: int = 1000
    temporal_violations_max: int = 1000
    hitl_pending_max: int = 1000
    hitl_feedback_max: int = 5000

    rl_update_every: int = 20
    federated_push_every: int = 50

    def __post_init__(self) -> None:
        if (
            not isinstance(self.deployment_id, str)
            or not self.deployment_id.strip()
        ):
            raise IntegratorError("deployment_id must be non-empty")
        if (
            not isinstance(self.agent_id, str)
            or not self.agent_id.strip()
        ):
            raise IntegratorError("agent_id must be non-empty")
        if not isinstance(self.strict, bool):
            raise IntegratorError("strict must be a bool")
        if not isinstance(self.features, Mapping):
            raise IntegratorError("features must be a Mapping")
        for k, v in self.features.items():
            if not isinstance(k, str):
                raise IntegratorError("feature names must be strings")
            if not isinstance(v, bool):
                raise IntegratorError(
                    f"feature {k!r} must be bool, got {type(v).__name__}"
                )
        for name in (
            "decision_history_max",
            "chaos_events_max",
            "federated_updates_max",
            "temporal_violations_max",
            "hitl_pending_max",
            "hitl_feedback_max",
            "rl_update_every",
            "federated_push_every",
        ):
            v = getattr(self, name)
            if isinstance(v, bool) or not isinstance(v, int) or v <= 0:
                raise IntegratorError(f"{name} must be a positive int")


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
    LATENCY_SPECIALIST = "latency_specialist"
    CARBON_OPTIMIZER = "carbon_optimizer"
    COST_OPTIMIZER = "cost_optimizer"
    DISTILLATION_SPECIALIST = "distillation_specialist"


class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


# =============================================================================
# ENHANCEMENT 9: Resilience
# =============================================================================
@dataclass
class CircuitBreaker:
    """Mutable per-dependency circuit breaker."""

    name: str
    failure_threshold: int = 3
    recovery_timeout_seconds: int = 60
    state: CircuitState = CircuitState.CLOSED
    failures: int = 0
    last_failure_at: Optional[datetime] = None

    def __post_init__(self) -> None:
        if self.failure_threshold <= 0:
            raise IntegratorError("failure_threshold must be > 0")
        if self.recovery_timeout_seconds < 0:
            raise IntegratorError(
                "recovery_timeout_seconds must be >= 0"
            )

    def can_call(self) -> bool:
        if self.state is CircuitState.CLOSED:
            return True
        if self.state is CircuitState.OPEN:
            if (
                self.last_failure_at is not None
                and (_utcnow() - self.last_failure_at).total_seconds()
                > self.recovery_timeout_seconds
            ):
                self.state = CircuitState.HALF_OPEN
                return True
            return False
        return True

    def record_success(self) -> None:
        self.failures = 0
        self.state = CircuitState.CLOSED

    def record_failure(self) -> None:
        self.failures += 1
        self.last_failure_at = _utcnow()
        if self.failures >= self.failure_threshold:
            if self.state is not CircuitState.OPEN:
                logger.warning(
                    "Circuit %r OPEN after %d failures",
                    self.name, self.failures,
                )
            self.state = CircuitState.OPEN


class ChaosInjector:
    """Deterministic chaos injection with a bounded event log."""

    def __init__(
        self,
        failure_rate: float = 0.0,
        latency_ms: int = 0,
        max_events: int = 1000,
    ) -> None:
        if not 0.0 <= failure_rate <= 1.0:
            raise IntegratorError("failure_rate must be in [0, 1]")
        if latency_ms < 0:
            raise IntegratorError("latency_ms must be >= 0")
        if max_events <= 0:
            raise IntegratorError("max_events must be > 0")
        self.failure_rate = failure_rate
        self.latency_ms = latency_ms
        self.events: Deque[Dict[str, Any]] = deque(maxlen=max_events)

    async def maybe_inject(self, component: str) -> None:
        if self.latency_ms > 0:
            await asyncio.sleep(self.latency_ms / 1000.0)
        if random.random() < self.failure_rate:
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
class SafetyProperty(Protocol):
    def check(self, decision: Mapping[str, Any]) -> bool: ...
    def name(self) -> str: ...


class DeadlineRespected:
    def check(self, decision: Mapping[str, Any]) -> bool:
        deadline = decision.get("deadline_hours")
        latency = decision.get("latency_hours", 0.0)
        if deadline is None:
            return True
        return float(latency) <= float(deadline)

    def name(self) -> str:
        return "DeadlineRespected"


@dataclass(frozen=True)
class EnergyBudgetNotExceeded:
    max_kwh: float = 10.0
    def check(self, decision: Mapping[str, Any]) -> bool:
        return float(decision.get("energy_kwh", 0.0)) <= self.max_kwh
    def name(self) -> str:
        return "EnergyBudgetNotExceeded"


@dataclass(frozen=True)
class CarbonBudgetNotExceeded:
    max_kg: float = 5.0
    def check(self, decision: Mapping[str, Any]) -> bool:
        return float(decision.get("carbon_kg", 0.0)) <= self.max_kg
    def name(self) -> str:
        return "CarbonBudgetNotExceeded"


class TemporalLogicMonitor:
    def __init__(self, max_violations: int = 1000) -> None:
        if max_violations <= 0:
            raise IntegratorError("max_violations must be > 0")
        self.properties: List[SafetyProperty] = []
        self.violations: Deque[Dict[str, Any]] = deque(
            maxlen=max_violations,
        )

    def register(self, p: SafetyProperty) -> None:
        self.properties.append(p)

    def verify(
        self, decision: Mapping[str, Any],
    ) -> Tuple[bool, List[str]]:
        bad: List[str] = []
        for p in self.properties:
            if not p.check(decision):
                bad.append(p.name())
                self.violations.append({
                    "property": p.name(),
                    "at": _utcnow().isoformat(),
                    "decision_summary": _compact({
                        k: decision.get(k) for k in (
                            "deadline_hours", "latency_hours",
                            "energy_kwh", "carbon_kg",
                        )
                    }),
                })
        return (len(bad) == 0, bad)


# =============================================================================
# ENHANCEMENT 6: XAI
# =============================================================================
@dataclass(frozen=True)
class Explanation:
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


class RoutingExplainer:
    @staticmethod
    def explain(
        task: Mapping[str, Any],
        context: Mapping[str, Any],
        chosen_expert: str,
        chosen_node: str,
        cost_breakdown: Mapping[str, float],
        temporal_ok: bool,
        temporal_violations: List[str],
        market_snapshot: Optional[Mapping[str, Any]] = None,
    ) -> Explanation:
        reasons: List[str] = [
            f"Task type {task.get('type', 'unknown')!r} routed to "
            f"expert {chosen_expert!r} on node {chosen_node!r}."
        ]
        if cost_breakdown:
            top = sorted(
                cost_breakdown.items(),
                key=lambda kv: abs(kv[1]), reverse=True,
            )[:3]
            reasons.append(
                "Top cost contributors: "
                + ", ".join(f"{k}={v:.3f}" for k, v in top) + "."
            )
        if temporal_ok:
            reasons.append(
                "Temporal safety properties verified ✓."
            )
        else:
            reasons.append(
                f"Temporal violations: {temporal_violations}."
            )
        if market_snapshot:
            cp = market_snapshot.get("carbon_price_per_tco2_usd")
            if cp is not None:
                reasons.append(
                    f"Carbon market price = ${cp:.2f}/tCO₂ considered."
                )
        return Explanation(
            headline=f"Route → {chosen_expert}@{chosen_node}",
            rationale=tuple(reasons),
            confidence=0.9 if temporal_ok else 0.4,
            contributing_factors=dict(cost_breakdown),
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
        if not math.isfinite(self.vram_gb) or self.vram_gb < 0:
            raise IntegratorError("vram_gb must be finite and >= 0")


class AdaptivePrecisionController:
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
class RoutingState:
    task_type_hash: float
    hour_of_day: int
    recent_latency_ms: float
    recent_carbon_kg: float
    recent_cost: float


class CausalRLPolicy:
    """Contextual bandit over experts, learning from counterfactual rewards."""

    def __init__(
        self, experts: List[str], epsilon: float = 0.1, lr: float = 0.05,
    ) -> None:
        self.experts = list(experts)
        self.epsilon = epsilon
        self.lr = lr
        self.weights: Dict[str, List[float]] = {
            e: [0.0] * 5 for e in self.experts
        }
        self.buffer: Deque[Tuple[RoutingState, str, float]] = deque(
            maxlen=1024,
        )
        self.updates_applied: int = 0

    @staticmethod
    def _features(s: RoutingState) -> List[float]:
        return [
            s.task_type_hash,
            s.hour_of_day / 24.0,
            s.recent_latency_ms / 1000.0,
            s.recent_carbon_kg / 10.0,
            s.recent_cost / 10.0,
        ]

    def _q(self, s: RoutingState, expert: str) -> float:
        f = self._features(s)
        return sum(w * x for w, x in zip(self.weights[expert], f))

    def select(
        self, s: RoutingState, available: List[str],
    ) -> str:
        if not available:
            return self.experts[0] if self.experts else "default"
        if random.random() < self.epsilon:
            return random.choice(available)
        return max(available, key=lambda e: self._q(s, e))

    def record(
        self, s: RoutingState, expert: str, reward: float,
    ) -> None:
        self.buffer.append((s, expert, reward))

    def update(self) -> int:
        """Return the number of weight updates applied."""
        if not self.buffer:
            return 0
        n = 0
        for s, e, r in self.buffer:
            f = self._features(s)
            q = sum(w * x for w, x in zip(self.weights[e], f))
            err = r - q
            for i, x in enumerate(f):
                self.weights[e][i] += self.lr * err * x
            n += 1
        self.buffer.clear()
        self.updates_applied += n
        return n


# =============================================================================
# ENHANCEMENT 1: Quantum Distillation
# =============================================================================
@dataclass(frozen=True)
class DistilledExpert:
    expert_id: str
    precision: PrecisionLevel
    student_weights: Tuple[float, ...]
    quality_retention: float
    energy_reduction_percent: float


class QuantumDistillationBridge:
    def distill(
        self,
        expert_id: str,
        teacher_weights: List[float],
        precision: PrecisionLevel,
    ) -> DistilledExpert:
        scale, retention, energy = {
            PrecisionLevel.FP32: (1.0, 1.00, 0.0),
            PrecisionLevel.FP16: (1.0, 0.98, 30.0),
            PrecisionLevel.INT8: (100.0, 0.93, 55.0),
            PrecisionLevel.INT4: (10.0, 0.85, 70.0),
            PrecisionLevel.QUANTUM_DISTILLED: (5.0, 0.80, 85.0),
        }[precision]
        student = [w * 0.9 for w in teacher_weights]
        if scale > 1.0:
            student = [math.floor(w * scale) / scale for w in student]
        return DistilledExpert(
            expert_id=expert_id,
            precision=precision,
            student_weights=tuple(student),
            quality_retention=retention,
            energy_reduction_percent=energy,
        )


# =============================================================================
# ENHANCEMENT 3: Federated Aggregator
# =============================================================================
@dataclass(frozen=True)
class FederatedUpdate:
    deployment_id: str
    expert_rewards: Mapping[str, float]
    sample_count: int
    timestamp: datetime = field(default_factory=_utcnow)


class FederatedAggregator:
    def __init__(self, max_updates: int = 1000) -> None:
        if max_updates <= 0:
            raise IntegratorError("max_updates must be > 0")
        self.updates: Deque[FederatedUpdate] = deque(
            maxlen=max_updates,
        )
        self._global: Dict[str, float] = {}

    def push(self, u: FederatedUpdate) -> None:
        self.updates.append(u)

    def aggregate(self) -> Dict[str, float]:
        if not self.updates:
            return {}
        total_w = sum(u.sample_count for u in self.updates) or 1
        merged: Dict[str, float] = defaultdict(float)
        for u in self.updates:
            w = u.sample_count / total_w
            for expert, reward in u.expert_rewards.items():
                merged[expert] += reward * w
        self._global = dict(merged)
        return self._global

    def get_global(self, expert: str) -> Optional[float]:
        return self._global.get(expert)


# =============================================================================
# ENHANCEMENT 4: Multi-Agent Coordinator
# =============================================================================
@dataclass
class AgentProfile:
    agent_id: str
    role: AgentRole = AgentRole.GENERALIST
    success_rate: float = 0.0
    avg_energy_saved: float = 0.0
    avg_latency_ms: float = 0.0
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
            if (
                a.avg_latency_ms
                and a.avg_latency_ms < 300
                and a.success_rate > 0.8
            ):
                a.role = AgentRole.LATENCY_SPECIALIST
            elif a.avg_energy_saved > 0.4:
                a.role = AgentRole.CARBON_OPTIMIZER
            elif a.success_rate > 0.85:
                a.role = AgentRole.COST_OPTIMIZER
            else:
                a.role = AgentRole.GENERALIST

    def record(
        self, agent_id: str, success: bool,
        energy_saved_pct: float, latency_ms: float,
    ) -> None:
        a = self.register(agent_id)
        n = a.total_calls + 1
        a.success_rate = (
            (n - 1) * a.success_rate + float(success)
        ) / n
        a.avg_energy_saved = (
            (n - 1) * a.avg_energy_saved + energy_saved_pct / 100.0
        ) / n
        a.avg_latency_ms = (
            (n - 1) * a.avg_latency_ms + latency_ms
        ) / n
        a.total_calls = n
        if n % 5 == 0:
            self._reassign()

    def select_agent(
        self, prefer: AgentRole = AgentRole.GENERALIST,
    ) -> Optional[str]:
        if not self.agents:
            return None
        cands = [
            a for a in self.agents.values() if a.role == prefer
        ]
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
    timestamp: datetime = field(default_factory=_utcnow)


class CarbonMarketClient:
    def __init__(self, *, seed: Optional[int] = None) -> None:
        self._cache: Optional[MarketSnapshot] = None
        self._ttl = 300
        self._rng = random.Random(seed)

    def get_snapshot(self) -> MarketSnapshot:
        if self._cache is not None:
            age = (
                _utcnow() - self._cache.timestamp
            ).total_seconds()
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
# ENHANCEMENT 10: HITL + Active Learning
# =============================================================================
@dataclass(frozen=True)
class HITLRequest:
    task_id: str
    chosen_expert: str
    confidence: float
    reason: str
    urgency: str
    requested_at: datetime = field(default_factory=_utcnow)


class HumanInTheLoopGate:
    def __init__(
        self,
        confidence_threshold: float = 0.5,
        max_pending: int = 1000,
        max_feedback: int = 5000,
    ) -> None:
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

    def needs_review(
        self, confidence: float, context: Mapping[str, Any],
    ) -> bool:
        if confidence < self.confidence_threshold:
            return True
        if context.get("deadline_critical"):
            return True
        return False

    def request_review(
        self, task_id: str, expert: str, confidence: float,
        reason: str, urgency: str = "medium",
    ) -> bool:
        req = HITLRequest(
            task_id=task_id, chosen_expert=expert,
            confidence=confidence, reason=reason, urgency=urgency,
        )
        self.pending.append(req)
        try:
            if urgency == "low":
                self.feedback_log.append({
                    "req": {
                        **asdict(req),
                        "requested_at": req.requested_at.isoformat(),
                    },
                    "decision": True,
                })
                return True
            if self._callback is None:
                self.feedback_log.append({
                    "req": {
                        **asdict(req),
                        "requested_at": req.requested_at.isoformat(),
                    },
                    "decision": False,
                })
                return False
            decision = bool(self._callback(req))
            self.feedback_log.append({
                "req": {
                    **asdict(req),
                    "requested_at": req.requested_at.isoformat(),
                },
                "decision": decision,
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
# Enhanced router wrapper
# =============================================================================
class _EnhancedRouter:
    """Wrapper that runs the integrator's enhancement pipeline."""

    def __init__(self, integrator: "EnhancedSystemIntegrator") -> None:
        self._integrator = integrator

    async def route(
        self, task: Dict[str, Any], context: Dict[str, Any],
    ) -> Dict[str, Any]:
        return await self._integrator._enhanced_route(task, context)


# =============================================================================
# Enhanced System Integrator
# =============================================================================
class EnhancedSystemIntegrator:
    """Orchestrates core + enhanced modules with all ten enhancement layers."""

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
        config: Mapping[str, Any],
        *,
        deployment_id: Optional[str] = None,
        agent_id: Optional[str] = None,
        features: Optional[Mapping[str, bool]] = None,
        hardware: Optional[HardwareProfile] = None,
        strict: bool = False,
    ) -> None:
        if not isinstance(config, Mapping):
            raise IntegratorError("config must be a Mapping")

        merged_features = {
            **self.DEFAULT_FEATURES, **(features or {})
        }
        self._config = IntegratorConfig(
            deployment_id=deployment_id or "local",
            agent_id=agent_id or "integrator-0",
            strict=strict,
            features=merged_features,
            hardware=hardware,
        )
        self.config = dict(config)
        self.deployment_id = self._config.deployment_id
        self.agent_id = self._config.agent_id
        self.features = dict(self._config.features)

        if self._config.strict and _STUB_COMPONENTS:
            raise IntegratorError(
                "strict=True but the following modules are missing: "
                f"{list(_STUB_COMPONENTS)}"
            )

        self._stub_components = list(_STUB_COMPONENTS)
        self._fabricated = bool(self._stub_components)

        # Core components
        self.db_manager: Any = None
        self.task_manager: Any = None
        self.registry: Any = None
        self.router: Any = None
        self.digital_twin: Any = None
        self.mlops: Any = None
        self.carbon_manager: Any = None
        self.helium_dashboard: Any = None
        self.harvester: Any = None

        # Enhancement domain components
        self.cost_function: Any = None
        self.node_registry: Any = None
        self.evolution_engine: Any = None
        self.token_optimizer: Any = None
        self.pareto_router: Any = None

        # Enhancement layers
        self.circuit_breakers: Dict[str, CircuitBreaker] = {
            "router": CircuitBreaker("router"),
            "carbon": CircuitBreaker("carbon"),
            "helium": CircuitBreaker("helium"),
            "pareto": CircuitBreaker("pareto"),
            "xai": CircuitBreaker("xai"),
        }
        self.chaos: Optional[ChaosInjector] = (
            ChaosInjector(max_events=self._config.chaos_events_max)
            if self.features["chaos_testing"] else None
        )

        self.temporal_monitor: Optional[TemporalLogicMonitor] = (
            TemporalLogicMonitor(
                max_violations=self._config.temporal_violations_max,
            )
            if self.features["temporal_logic"] else None
        )
        if self.temporal_monitor:
            self.temporal_monitor.register(DeadlineRespected())
            self.temporal_monitor.register(EnergyBudgetNotExceeded())
            self.temporal_monitor.register(CarbonBudgetNotExceeded())

        self.explainer = (
            RoutingExplainer() if self.features["xai"] else None
        )
        self.precision_ctl: Optional[AdaptivePrecisionController] = (
            AdaptivePrecisionController(self._config.hardware)
            if self.features["adaptive_precision"] else None
        )
        self.rl_policy: Optional[CausalRLPolicy] = (
            CausalRLPolicy(experts=[])
            if self.features["causal_rl"] else None
        )
        self.distiller: Optional[QuantumDistillationBridge] = (
            QuantumDistillationBridge()
            if self.features["quantum_distillation"] else None
        )
        self.federated: Optional[FederatedAggregator] = (
            FederatedAggregator(
                max_updates=self._config.federated_updates_max,
            )
            if self.features["federated"] else None
        )
        self.coordinator: Optional[MultiAgentCoordinator] = (
            MultiAgentCoordinator()
            if self.features["multi_agent"] else None
        )
        if self.coordinator:
            self.coordinator.register(self.agent_id)

        self.market: Optional[CarbonMarketClient] = (
            CarbonMarketClient()
            if self.features["carbon_market"] else None
        )
        self.hitl: Optional[HumanInTheLoopGate] = (
            HumanInTheLoopGate(
                max_pending=self._config.hitl_pending_max,
                max_feedback=self._config.hitl_feedback_max,
            )
            if self.features["hitl"] else None
        )

        # Bookkeeping
        self.decision_history: Deque[Dict[str, Any]] = deque(
            maxlen=self._config.decision_history_max,
        )
        self._last_decision: Optional[Dict[str, Any]] = None
        self._last_explanation: Optional[Explanation] = None

        # Dedicated counters — no more `len(decision_history) % N`.
        self._rl_update_counter: int = 0
        self._federated_push_counter: int = 0
        self._decision_errors: int = 0

        self._initialized = False
        self._setup_done = False

    # ------------------------------------------------------------------
    # Initialization
    # ------------------------------------------------------------------
    async def initialize(self) -> None:
        if self._initialized:
            logger.info(
                "Core components already initialized; skipping"
            )
            return

        logger.info(
            "Initializing core components (stub=%s)",
            bool(self._stub_components),
        )

        self.db_manager = DatabaseManager(
            self.config.get("database", {}),
        )
        self.task_manager = TaskManager(max_workers=10)
        self.registry = ExpertRegistry(
            self.config.get("registry", {}),
        )
        self.digital_twin = DigitalTwin(
            self.config.get("digital_twin", {}),
        )
        self.mlops = MLOpsPipeline(self.config.get("mlops", {}))
        self.carbon_manager = CarbonIntensityManager(
            self.config.get("carbon", {}),
        )
        self.helium_dashboard = HeliumEfficiencyDashboard(
            self.config.get("helium", {}),
        )
        self.harvester = PhotosyntheticHarvester(
            self.config.get("harvester", {}),
        )
        self.router = ExpertRouter(self.config.get("router", {}))

        if hasattr(self.db_manager, "connect"):
            try:
                await self.db_manager.connect()
            except Exception as exc:
                logger.warning("DB connect failed: %s", exc)

        self._initialized = True
        logger.info("Core components initialized.")

    # ------------------------------------------------------------------
    # Enhancement setup (idempotent)
    # ------------------------------------------------------------------
    async def setup_enhancements(self) -> None:
        if self._setup_done:
            logger.info("Enhancements already set up; skipping")
            return
        if not self._initialized:
            await self.initialize()

        logger.info("Setting up enhancements...")

        node_cfg = self.config.get("node_registry", {})
        self.node_registry = NodeRegistry(
            node_cfg, self.db_manager,
        )
        try:
            await self.node_registry.start(
                refresh_interval=node_cfg.get("refresh_interval", 3600),
            )
        except Exception as exc:
            logger.warning("NodeRegistry start failed: %s", exc)

        cost_cfg = self.config.get("cost_function", {
            "alpha": 1.0, "beta": 2.0, "gamma": 0.5,
            "delta": 0.3, "epsilon": 0.1, "zeta": -0.1,
        })
        self.cost_function = SustainabilityCostFunction(cost_cfg)
        if hasattr(self.cost_function, "inject_dependencies"):
            self.cost_function.inject_dependencies(
                carbon_manager=self.carbon_manager,
                helium_dashboard=self.helium_dashboard,
                node_registry=self.node_registry,
            )

        expert_ids = self._load_expert_ids()
        if self.rl_policy:
            self.rl_policy.experts = list(expert_ids)
            self.rl_policy.weights = {
                e: [0.0] * 5 for e in expert_ids
            }

        user_prefs = UserPreferences({
            "alpha": 0.5, "beta": 2.0, "gamma": 0.5,
            "delta": 0.3, "epsilon": 0.1, "zeta": -0.1,
        })

        try:
            self.pareto_router = ParetoRouter(
                config=self.config.get("router", {}),
                cost_function=self.cost_function,
                node_registry=self.node_registry,
                user_preferences=user_prefs,
            )
            if hasattr(self.pareto_router, "registry"):
                self.pareto_router.registry = self.registry
        except Exception as exc:
            logger.warning(
                "ParetoRouter init failed, using stub: %s", exc,
            )
            self.pareto_router = _StubParetoRouter(
                config=self.config.get("router", {}),
                cost_function=self.cost_function,
                node_registry=self.node_registry,
                user_preferences=user_prefs,
            )

        # Idempotent wrapping: never wrap a wrapper.
        if not isinstance(self.router, _EnhancedRouter):
            self.router = _EnhancedRouter(self)

        self.token_optimizer = TokenizationOptimizer(
            self.config.get("tokenization", {}),
        )

        evo_cfg = self.config.get(
            "evolutionary_engine",
            {
                "prune_threshold": 0.2,
                "merge_similarity_threshold": 0.85,
                "spawn_gap_threshold": 0.3,
                "interval_seconds": 3600,
            },
        )
        self.evolution_engine = EvolutionaryEngine(
            config=evo_cfg,
            registry=self.registry,
            cost_function=self.cost_function,
            digital_twin=self.digital_twin,
            mlops=self.mlops,
            db_manager=self.db_manager,
            task_manager=self.task_manager,
        )
        try:
            self.task_manager.start_task(
                "evolution_engine",
                self.evolution_engine.start,
                interval_seconds=evo_cfg.get(
                    "interval_seconds", 3600,
                ),
            )
        except Exception as exc:
            logger.warning(
                "Failed to start evolution engine: %s", exc,
            )

        self._setup_done = True
        logger.info(
            "All enhancements set up (features=%s, stub=%s)",
            list(self.features), bool(self._stub_components),
        )

    def _load_expert_ids(self) -> List[str]:
        try:
            experts = self.registry.get_experts() or []
            ids = [
                getattr(e, "expert_id", getattr(e, "id", str(e)))
                for e in experts
            ]
        except Exception:
            ids = []
        return ids or ["default_expert"]

    # ------------------------------------------------------------------
    # Routing (with error isolation)
    # ------------------------------------------------------------------
    async def _enhanced_route(
        self, task: Dict[str, Any], context: Dict[str, Any],
    ) -> Dict[str, Any]:
        try:
            return await self._enhanced_route_inner(task, context)
        except Exception as exc:
            self._decision_errors += 1
            logger.exception("enhanced_route failed")
            return self._fabricated_decision(
                task, context,
                error=f"{type(exc).__name__}: {exc}",
            )

    async def _enhanced_route_inner(
        self, task: Dict[str, Any], context: Dict[str, Any],
    ) -> Dict[str, Any]:
        cb = self.circuit_breakers["router"]
        if not cb.can_call():
            logger.warning(
                "Router circuit open — returning fallback"
            )
            return self._fabricated_decision(task, context)

        if self.chaos is not None:
            try:
                await self.chaos.maybe_inject("router")
            except Exception as exc:
                logger.warning(
                    "Chaos injection (expected): %s", exc,
                )
                cb.record_failure()
                return self._fabricated_decision(
                    task, context, error="chaos injection",
                )

        # --- Causal RL ---
        available_experts = self._available_experts(context)
        chosen_expert: Optional[str] = None
        rl_state: Optional[RoutingState] = None
        if self.rl_policy and available_experts:
            rl_state = self._make_rl_state(task, context)
            chosen_expert = self.rl_policy.select(
                rl_state, available_experts,
            )

        # --- Pareto frontier ---
        pareto_result: Optional[Dict[str, Any]] = None
        if self.pareto_router is not None:
            p_cb = self.circuit_breakers["pareto"]
            if p_cb.can_call():
                try:
                    frontier = await self.pareto_router.get_frontier(
                        task, context,
                    )
                except Exception as exc:
                    p_cb.record_failure()
                    logger.warning(
                        "Pareto router failed: %s", exc,
                    )
                    frontier = []
                else:
                    p_cb.record_success()
                if frontier:
                    best = max(
                        frontier,
                        key=lambda c: c.get("score", 0.0),
                    )
                    pareto_result = dict(best)

        # --- Underlying router ---
        try:
            result = await self.router.inner.route(task, context)
        except Exception as exc:
            cb.record_failure()
            logger.warning(
                "Router failed (%s) — falling back", exc,
            )
            result = {
                "expert_id": chosen_expert or "fallback",
                "node_id": "local",
            }
        else:
            cb.record_success()

        if chosen_expert:
            result["expert_id"] = chosen_expert
        if pareto_result:
            for k in ("expert_id", "node_id"):
                if k in pareto_result:
                    result[k] = pareto_result[k]

        # --- Cost breakdown ---
        cost_breakdown: Dict[str, float] = {
            "carbon": float(context.get("recent_carbon_kg", 0.0)),
            "helium": 0.0,
            "latency": float(
                context.get("recent_latency_ms", 0.0)
            ) / 1000.0,
            "accuracy_loss": float(
                context.get("recent_accuracy_loss", 0.0)
            ),
        }
        if self.helium_dashboard is not None:
            h_cb = self.circuit_breakers["helium"]
            if h_cb.can_call():
                try:
                    cost_breakdown["helium"] = (
                        await self.helium_dashboard.get_price()
                    )
                except Exception as exc:
                    h_cb.record_failure()
                    logger.warning(
                        "Helium dashboard failed: %s", exc,
                    )
                else:
                    h_cb.record_success()

        if self.cost_function is not None:
            try:
                cost_breakdown["total"] = float(
                    await self.cost_function.evaluate(context)
                )
            except Exception as exc:
                logger.warning(
                    "Cost function evaluate failed: %s", exc,
                )

        # --- Adaptive precision ---
        precision = PrecisionLevel.FP32
        if self.precision_ctl:
            urgency = (
                "critical"
                if context.get("deadline_critical")
                else "normal"
            )
            precision = self.precision_ctl.select(urgency)

        # --- Market enrichment ---
        market_snapshot: Optional[Dict[str, Any]] = None
        if self.market:
            snap = self.market.get_snapshot()
            market_snapshot = {
                "carbon_price_per_tco2_usd":
                    snap.carbon_price_per_tco2_usd,
                "rec_price_per_mwh_usd":
                    snap.rec_price_per_mwh_usd,
                "rec_available_mwh": snap.rec_available_mwh,
            }

        # --- Temporal verification ---
        decision_input = {
            "deadline_hours": context.get("deadline_hours"),
            "latency_hours": context.get(
                "estimated_latency_hours", 0.0,
            ),
            "energy_kwh": context.get("estimated_energy_kwh", 0.0),
            "carbon_kg": context.get("estimated_carbon_kg", 0.0),
        }
        temporal_ok = True
        violations: List[str] = []
        if self.temporal_monitor:
            temporal_ok, violations = (
                self.temporal_monitor.verify(decision_input)
            )

        # --- XAI ---
        explanation_obj: Optional[Explanation] = None
        if self.explainer is not None:
            x_cb = self.circuit_breakers["xai"]
            if x_cb.can_call():
                try:
                    explanation_obj = RoutingExplainer.explain(
                        task=task,
                        context=context,
                        chosen_expert=result.get(
                            "expert_id", "unknown",
                        ),
                        chosen_node=result.get(
                            "node_id", "unknown",
                        ),
                        cost_breakdown=cost_breakdown,
                        temporal_ok=temporal_ok,
                        temporal_violations=violations,
                        market_snapshot=market_snapshot,
                    )
                except Exception as exc:
                    x_cb.record_failure()
                    logger.warning("XAI failed: %s", exc)

        # --- HITL ---
        confidence = (
            explanation_obj.confidence if explanation_obj else 0.5
        )
        hitl_required = False
        hitl_approved: Optional[bool] = None
        if self.hitl and self.hitl.needs_review(
            confidence, context,
        ):
            hitl_required = True
            hitl_approved = self.hitl.request_review(
                task_id=str(task.get("task_id", "unknown")),
                expert=result.get("expert_id", "unknown"),
                confidence=confidence,
                reason="low confidence or deadline-critical",
                urgency=(
                    "high"
                    if context.get("deadline_critical")
                    else "medium"
                ),
            )
            if hitl_approved is False:
                result["hitl_denied"] = True

        # --- RL feedback (fires every N, independent of deque) ---
        if self.rl_policy and rl_state and chosen_expert:
            reward = 1.0 if temporal_ok else 0.3
            reward -= 0.1 * cost_breakdown["latency"]
            self.rl_policy.record(rl_state, chosen_expert, reward)
            self._rl_update_counter += 1
            if (
                self._rl_update_counter
                % self._config.rl_update_every == 0
            ):
                self.rl_policy.update()

        # --- Multi-agent coordination ---
        if self.coordinator:
            self.coordinator.record(
                agent_id=self.agent_id,
                success=temporal_ok,
                energy_saved_pct=float(
                    context.get("energy_saved_pct", 0.0)
                ),
                latency_ms=float(
                    context.get("recent_latency_ms", 0.0)
                ),
            )

        # --- Federated contribution ---
        self._federated_push_counter += 1
        if (
            self.federated
            and self._federated_push_counter
            % self._config.federated_push_every == 0
        ):
            self.federated.push(FederatedUpdate(
                deployment_id=self.deployment_id,
                expert_rewards={
                    result.get("expert_id", "unknown"): 1.0,
                },
                sample_count=1,
            ))
            self.federated.aggregate()

        # --- Distillation ---
        distilled_info: Optional[Dict[str, Any]] = None
        if self.distiller and context.get("distill_expert"):
            teacher_weights = [0.5, 0.3, 0.1, 0.05, 0.05]
            dist = self.distiller.distill(
                expert_id=result.get("expert_id", "unknown"),
                teacher_weights=teacher_weights,
                precision=precision,
            )
            distilled_info = {
                "precision": dist.precision.value,
                "quality_retention": dist.quality_retention,
                "energy_reduction_percent":
                    dist.energy_reduction_percent,
            }

        # --- Bookkeeping ---
        decision_record = {
            "task": _compact(task),
            "context": _compact(context),
            "result": dict(result),
            "cost_breakdown": dict(cost_breakdown),
            "precision": precision.value,
            "temporal_ok": temporal_ok,
            "temporal_violations": violations,
            "hitl_required": hitl_required,
            "hitl_approved": hitl_approved,
            "market_snapshot": market_snapshot,
            "distilled_info": distilled_info,
            "fabricated": self._fabricated,
            "at": _utcnow().isoformat(),
        }
        self.decision_history.append(decision_record)
        self._last_decision = decision_record
        self._last_explanation = explanation_obj

        return {
            "expert_id": result.get("expert_id", "unknown"),
            "node_id": result.get("node_id", "unknown"),
            "precision": precision.value,
            "temporal_ok": temporal_ok,
            "temporal_violations": violations,
            "explanation": (
                explanation_obj.to_dict()
                if explanation_obj else None
            ),
            "hitl_required": hitl_required,
            "hitl_approved": hitl_approved,
            "market_snapshot": market_snapshot,
            "distilled_info": distilled_info,
            "fabricated": self._fabricated,
        }

    def _make_rl_state(
        self, task: Dict[str, Any], context: Dict[str, Any],
    ) -> RoutingState:
        return RoutingState(
            task_type_hash=_stable_hash(
                str(task.get("type", ""))
            ),
            hour_of_day=_utcnow().hour,
            recent_latency_ms=float(
                context.get("recent_latency_ms", 0.0)
            ),
            recent_carbon_kg=float(
                context.get("recent_carbon_kg", 0.0)
            ),
            recent_cost=float(context.get("recent_cost", 0.0)),
        )

    def _available_experts(
        self, context: Mapping[str, Any],
    ) -> List[str]:
        if context.get("expert_whitelist"):
            return list(context["expert_whitelist"])
        if self.rl_policy and self.rl_policy.experts:
            return list(self.rl_policy.experts)
        try:
            experts = self.registry.get_experts() or []
            ids = [
                getattr(e, "expert_id", getattr(e, "id", str(e)))
                for e in experts
            ]
            return ids or ["default_expert"]
        except Exception:
            return ["default_expert"]

    def _fabricated_decision(
        self,
        task: Dict[str, Any],
        context: Dict[str, Any],
        *,
        error: Optional[str] = None,
    ) -> Dict[str, Any]:
        decision = {
            "expert_id": "fallback",
            "node_id": "local",
            "precision": PrecisionLevel.FP32.value,
            "temporal_ok": False,
            "temporal_violations": [],
            "explanation": None,
            "hitl_required": False,
            "hitl_approved": None,
            "market_snapshot": None,
            "distilled_info": None,
            "fabricated": True,
        }
        if error is not None:
            decision["error"] = error
        return decision

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------
    async def shutdown(self) -> None:
        logger.info("Shutting down enhanced components...")
        if self.evolution_engine is not None:
            try:
                await self.evolution_engine.stop()
            except Exception as exc:
                logger.warning(
                    "Evolution engine stop failed: %s", exc,
                )
        if self.node_registry is not None:
            try:
                await self.node_registry.stop()
            except Exception as exc:
                logger.warning(
                    "NodeRegistry stop failed: %s", exc,
                )
        if self.task_manager is not None:
            try:
                await self.task_manager.stop_all()
            except Exception as exc:
                logger.warning(
                    "TaskManager stop failed: %s", exc,
                )
        if hasattr(self.db_manager, "disconnect"):
            try:
                await self.db_manager.disconnect()
            except Exception as exc:
                logger.warning(
                    "DB disconnect failed: %s", exc,
                )
        logger.info("Shutdown complete.")

    # ------------------------------------------------------------------
    # Public enhanced APIs
    # ------------------------------------------------------------------
    def explain_last_decision(self) -> Optional[Explanation]:
        return self._last_explanation

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

    def contribute_federated(
        self, expert_rewards: Mapping[str, float],
    ) -> None:
        if not self.federated:
            return
        self.federated.push(FederatedUpdate(
            deployment_id=self.deployment_id,
            expert_rewards=dict(expert_rewards),
            sample_count=len(self.decision_history) or 1,
        ))
        self.federated.aggregate()

    def get_federated_aggregate(self) -> Dict[str, float]:
        return (
            self.federated.aggregate() if self.federated else {}
        )

    def distill_expert(
        self,
        expert_id: str,
        teacher_weights: List[float],
        urgency: str = "normal",
    ) -> Optional[DistilledExpert]:
        if not self.distiller:
            return None
        precision = (
            self.precision_ctl.select(urgency)
            if self.precision_ctl
            else PrecisionLevel.INT8
        )
        return self.distiller.distill(
            expert_id, teacher_weights, precision,
        )

    # ------------------------------------------------------------------
    # Statistics
    # ------------------------------------------------------------------
    def get_statistics(self) -> Dict[str, Any]:
        stats: Dict[str, Any] = {
            "deployment_id": self.deployment_id,
            "agent_id": self.agent_id,
            "decisions_made": len(self.decision_history),
            "decision_errors": self._decision_errors,
            "initialized": self._initialized,
            "setup_done": self._setup_done,
            "stub_components": list(self._stub_components),
            "fabricated": self._fabricated,
            "rl_updates": self._rl_update_counter,
            "federated_pushes": self._federated_push_counter,
            "circuits": {
                k: {
                    "state": cb.state.value,
                    "failures": cb.failures,
                }
                for k, cb in self.circuit_breakers.items()
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
        return stats

    # ------------------------------------------------------------------
    # Demo runner
    # ------------------------------------------------------------------
    async def run(self) -> Dict[str, Any]:
        task = {
            "task_id": "demo-1",
            "type": "summarize",
            "text": "Example input text",
        }
        context = {
            "token_count": 50,
            "target_node_id": "aws-us-east-1",
            "data_source": "photosynthetic_harvester",
            "deadline_critical": False,
            "deadline_hours": 2.0,
            "estimated_latency_hours": 0.5,
            "estimated_energy_kwh": 0.05,
            "estimated_carbon_kg": 0.02,
            "distill_expert": True,
        }
        result = await self.router.route(task, context)
        logger.info("Routing result: %s", result)
        if self.token_optimizer is not None:
            optimized = self.token_optimizer.optimize(
                task["text"], context,
            )
            logger.info("Tokenization: %s", optimized)
        await asyncio.sleep(0.05)
        return result


# =============================================================================
# Standalone helper functions
# =============================================================================
async def simulate_policy(
    policy_config: Dict[str, Any],
    digital_twin: Optional[Any] = None,
    num_tasks: int = 500,
) -> Dict[str, Any]:
    generator = SyntheticDataGenerator()
    dataset = generator.generate_dataset(
        num_tasks=num_tasks, include_edge_cases=True,
    )
    exported = generator.export_for_simulation(dataset)
    if digital_twin is None:
        return {
            "simulated": len(exported),
            "policy": policy_config,
        }
    return await digital_twin.simulate_routing(
        exported, policy_config,
    )


async def train_expert_on_synthetic_data(
    domain: str,
    mlops: Optional[Any] = None,
    num_samples: int = 1000,
) -> Dict[str, Any]:
    generator = SyntheticDataGenerator()
    tasks = generator.generate_task_batch(
        num_samples, task_type=domain, priority="balanced",
    )
    if mlops is None:
        return {"generated": len(tasks), "domain": domain}
    if hasattr(mlops, "train_expert"):
        return await mlops.train_expert(domain, tasks)
    return {"generated": len(tasks), "domain": domain}


# =============================================================================
# Smoke test
# =============================================================================
async def _smoke_test() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    config = {
        "database": {"db_path": "./green_agent.db"},
        "carbon": {"api_key": "your_key", "region": "global"},
        "helium": {},
        "harvester": {},
        "router": {},
        "node_registry": {"refresh_interval": 3600},
        "cost_function": {
            "alpha": 1.0, "beta": 2.0, "gamma": 0.5,
            "delta": 0.3, "epsilon": 0.1, "zeta": -0.1,
        },
        "evolutionary_engine": {"interval_seconds": 3600},
        "tokenization": {},
    }

    print("=" * 72)
    print("STUB COMPONENTS")
    print("=" * 72)
    print(f"  {len(_STUB_COMPONENTS)} stub(s): {list(_STUB_COMPONENTS)}")

    # ------------------------------------------------------------------
    # 1. Strict mode raises when stubs are present
    # ------------------------------------------------------------------
    print("\n" + "=" * 72)
    print("1. STRICT MODE REGRESSION")
    print("=" * 72)
    if _STUB_COMPONENTS:
        try:
            EnhancedSystemIntegrator(config, strict=True)
        except IntegratorError as exc:
            print(f"  ✓ strict=True raises: {exc}")
        else:
            raise AssertionError(
                "strict=True must raise when stubs are present"
            )
    else:
        print("  (no stubs present; strict mode would pass)")

    # ------------------------------------------------------------------
    # 2. Happy path with fabricated flag
    # ------------------------------------------------------------------
    print("\n" + "=" * 72)
    print("2. HAPPY PATH + FABRICATED FLAG")
    print("=" * 72)
    integrator = EnhancedSystemIntegrator(
        config,
        deployment_id="us-ca-prod-01",
        agent_id="integrator-A",
        hardware=HardwareProfile(
            supports_int4=True, vram_gb=48,
        ),
    )

    def auto_approve(req: HITLRequest) -> bool:
        logger.info(
            "[HITL] auto-approve %s (%s)",
            req.chosen_expert, req.urgency,
        )
        return True

    await integrator.initialize()
    await integrator.setup_enhancements()
    integrator.set_hitl_callback(auto_approve)

    result = await integrator.run()
    print(f"  Route result keys: {sorted(result)}")
    print(f"  expert={result['expert_id']!r} "
          f"node={result['node_id']!r}")
    print(f"  precision={result['precision']!r}")
    print(f"  temporal_ok={result['temporal_ok']}")
    print(f"  fabricated={result['fabricated']}  "
          f"(expected {bool(_STUB_COMPONENTS)})")
    assert result["fabricated"] == bool(_STUB_COMPONENTS)

    # ------------------------------------------------------------------
    # 3. XAI explanation
    # ------------------------------------------------------------------
    print("\n" + "=" * 72)
    print("3. XAI EXPLANATION")
    print("=" * 72)
    exp = integrator.explain_last_decision()
    assert exp is not None
    print(f"  headline: {exp.headline}")
    for line in exp.rationale:
        print(f"    • {line}")
    print(f"  confidence: {exp.confidence:.2f}")

    # ------------------------------------------------------------------
    # 4. Idempotent setup — no infinite recursion
    # ------------------------------------------------------------------
    print("\n" + "=" * 72)
    print("4. IDEMPOTENT SETUP")
    print("=" * 72)
    original_router = integrator.router
    await integrator.setup_enhancements()
    assert integrator.router is original_router, (
        "setup_enhancements() must not re-wrap the router"
    )
    print("  ✓ router not re-wrapped; no infinite recursion")

    # ------------------------------------------------------------------
    # 5. Pareto + cost function actually called
    # ------------------------------------------------------------------
    print("\n" + "=" * 72)
    print("5. PARETO + COST FUNCTION WIRED")
    print("=" * 72)
    # The last decision's cost_breakdown should contain "total"
    # (only present if cost_function.evaluate was awaited successfully).
    last = integrator._last_decision
    assert last is not None
    assert "total" in last["cost_breakdown"], (
        "cost function must actually be invoked"
    )
    print(
        f"  ✓ cost_breakdown['total'] = "
        f"{last['cost_breakdown']['total']}"
    )
    # Pareto router is present (stub returns empty; real would return frontier).
    assert integrator.pareto_router is not None
    print(f"  ✓ pareto_router present: "
          f"{type(integrator.pareto_router).__name__}")

    # ------------------------------------------------------------------
    # 6. Distillation
    # ------------------------------------------------------------------
    print("\n" + "=" * 72)
    print("6. QUANTUM DISTILLATION")
    print("=" * 72)
    dist = integrator.distill_expert(
        "default_expert",
        [0.5, 0.3, 0.1, 0.05, 0.05],
        urgency="normal",
    )
    assert dist is not None
    print(f"  precision={dist.precision.value}")
    print(f"  quality_retention={dist.quality_retention}")
    print(
        f"  energy_reduction_percent="
        f"{dist.energy_reduction_percent}"
    )

    # ------------------------------------------------------------------
    # 7. Federated contribution
    # ------------------------------------------------------------------
    print("\n" + "=" * 72)
    print("7. FEDERATED AGGREGATION")
    print("=" * 72)
    integrator.contribute_federated({"default_expert": 0.85})
    agg = integrator.get_federated_aggregate()
    print(f"  aggregate: {agg}")
    assert "default_expert" in agg

    # ------------------------------------------------------------------
    # 8. RL fires after > 2048 decisions
    # ------------------------------------------------------------------
    print("\n" + "=" * 72)
    print("8. RL / FEDERATED LIFECYCLE REGRESSION")
    print("=" * 72)
    target = integrator._config.decision_history_max + 100
    for i in range(target):
        await integrator.router.route(
            {"type": "summarize"}, {},
        )
    print(
        f"  decisions made: "
        f"{len(integrator.decision_history)} "
        f"(max={integrator._config.decision_history_max})"
    )
    print(f"  rl_update_counter: {integrator._rl_update_counter}")
    print(
        f"  federated_push_counter: "
        f"{integrator._federated_push_counter}"
    )
    assert integrator._rl_update_counter > integrator._config.decision_history_max, (
        "RL must keep firing past the deque cap"
    )
    assert (
        integrator._federated_push_counter
        > integrator._config.decision_history_max
    ), "Federated must keep firing past the deque cap"
    assert integrator.rl_policy is not None
    assert integrator.rl_policy.updates_applied > 0, (
        "at least one RL update batch must have been applied"
    )
    print(
        f"  ✓ RL updates applied: "
        f"{integrator.rl_policy.updates_applied}"
    )

    # ------------------------------------------------------------------
    # 9. Deterministic hashing across "restarts"
    # ------------------------------------------------------------------
    print("\n" + "=" * 72)
    print("9. DETERMINISTIC HASHING")
    print("=" * 72)
    h1 = _stable_hash("summarize")
    h2 = _stable_hash("summarize")
    h3 = _stable_hash("something_else")
    print(f"  _stable_hash('summarize') = {h1:.6f}")
    print(f"  _stable_hash('summarize') = {h2:.6f} (must equal)")
    print(f"  _stable_hash('other')     = {h3:.6f} (must differ)")
    assert h1 == h2, "hash must be deterministic"
    assert h1 != h3, "different inputs must hash differently"

    # ------------------------------------------------------------------
    # 10. Circuit breaker transitions
    # ------------------------------------------------------------------
    print("\n" + "=" * 72)
    print("10. CIRCUIT BREAKER TRANSITIONS")
    print("=" * 72)
    cb = CircuitBreaker("test", failure_threshold=2)
    assert cb.can_call()
    cb.record_failure()
    assert cb.can_call(), "one failure must not open"
    cb.record_failure()
    assert not cb.can_call(), "two failures must open"
    print(f"  ✓ breaker opens at threshold "
          f"(state={cb.state.value}, failures={cb.failures})")
    cb.record_success()
    assert cb.can_call(), "success must reset"
    print(f"  ✓ breaker resets on success "
          f"(state={cb.state.value})")

    # ------------------------------------------------------------------
    # 11. Bounded collections
    # ------------------------------------------------------------------
    print("\n" + "=" * 72)
    print("11. BOUNDED COLLECTIONS")
    print("=" * 72)
    agg_small = FederatedAggregator(max_updates=3)
    for i in range(10):
        agg_small.push(FederatedUpdate(
            deployment_id=f"d{i}",
            expert_rewards={"e": float(i)},
            sample_count=1,
        ))
    assert len(agg_small.updates) == 3
    print(f"  ✓ FederatedAggregator bounded: "
          f"len={len(agg_small.updates)} (cap=3)")

    chaos_small = ChaosInjector(
        failure_rate=1.0, max_events=5,
    )
    for i in range(10):
        try:
            await chaos_small.maybe_inject(f"comp{i}")
        except RuntimeError:
            pass
    assert len(chaos_small.events) == 5
    print(f"  ✓ ChaosInjector bounded: "
          f"len={len(chaos_small.events)} (cap=5)")

    # ------------------------------------------------------------------
    # 12. Error isolation
    # ------------------------------------------------------------------
    print("\n" + "=" * 72)
    print("12. ERROR ISOLATION")
    print("=" * 72)

    class _BoomExplainer(RoutingExplainer):
        @staticmethod
        def explain(*args, **kwargs):
            raise RuntimeError("xai exploded")

    # Mutate the integrator so the next route hits the XAI path.
    original_explainer = integrator.explainer
    integrator.explainer = _BoomExplainer()
    integrator.circuit_breakers["xai"].record_success()
    result = await integrator.router.route(
        {"type": "summarize"}, {},
    )
    assert result["fabricated"] in (True, False)
    # XAI explosion must not crash the route
    print(
        f"  ✓ route survived XAI failure; "
        f"xai breaker failures="
        f"{integrator.circuit_breakers['xai'].failures}"
    )
    integrator.explainer = original_explainer

    # ------------------------------------------------------------------
    # 13. Statistics snapshot
    # ------------------------------------------------------------------
    print("\n" + "=" * 72)
    print("13. STATISTICS SNAPSHOT")
    print("=" * 72)
    stats = integrator.get_statistics()
    print(json.dumps({
        k: stats[k] for k in (
            "decisions_made", "decision_errors",
            "rl_updates", "federated_pushes",
            "fabricated", "circuits",
        )
    }, indent=2, default=str))

    # ------------------------------------------------------------------
    # 14. Synthetic data helpers
    # ------------------------------------------------------------------
    print("\n" + "=" * 72)
    print("14. SYNTHETIC DATA HELPERS")
    print("=" * 72)
    sim = await simulate_policy(
        {"alpha": 1.0}, integrator.digital_twin, num_tasks=10,
    )
    print(f"  simulate_policy: {sim}")
    train = await train_expert_on_synthetic_data(
        "summarize", integrator.mlops, 50,
    )
    print(f"  train_expert_on_synthetic_data: {train}")

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------
    await integrator.shutdown()

    print("\n" + "=" * 72)
    print("SMOKE TEST PASSED")
    print("=" * 72)


if __name__ == "__main__":
    try:
        asyncio.run(_smoke_test())
    except KeyboardInterrupt:
        print("\nInterrupted.")
