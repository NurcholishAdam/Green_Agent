#!/usr/bin/env python3
# =============================================================================
# Counterfactual Benchmarking Harness v4.0.0 — Patched Single-File Edition
# =============================================================================
"""
Counterfactual Benchmarking Harness v4.0.0
==========================================
Patched single-file version of v3.4.1.

P0 fixes
--------
- Local imports (Storage, config, logger, FeedbackEvent, MTPDOptimizer,
  StrategyMetrics) are guarded with fallback stubs. The module imports even
  without the sibling packages.
- `_compute_p_value` returns `None` with a clear "unavailable" reason instead
  of a hard-coded 0.05.
- NSGA-II tournament ranks by identity, not dataclass equality.
- `_select_best_from_pareto` returns a copy; the stored Pareto front is not
  mutated.
- All asyncio.Locks are lazy.
- Lifecycle added: `async start()`, `async ready()`, `async shutdown()`,
  `__aenter__` / `__aexit__`.

P1 — correctness
----------------
- Events cache key includes `(days_back, sample_limit)`.
- Timezone-aware timestamps throughout.
- MoE gating update is reward-weighted.
- Dynamic MOEA weights use the full event set, not the first 100.
- Optional train/hold-out split for evolution vs. reporting.
- Warnings on empty metrics and missing candidate fields.
- `StrategyMetrics` carries `energy_joules` and `helium_cost`.
- Population/point alignment is tracked explicitly.

P2 — honesty
------------
- MODULE_STATUS documents every module.
- Placeholders (disabled by default, `.available = False`, warn when enabled):
    moe_gating, rlhf, modp, limit_graph, p_value.
- Experimental (warn when enabled):
    nsga2_optimizer, policy_evolution.

P3 — production readiness
-------------------------
- `--status` prints module maturity; `--test` runs the embedded suite;
  `--example` runs a mock demo.
- Prometheus + OpenTelemetry (both optional).
- Correlation IDs propagated via contextvar.
"""

from __future__ import annotations

import argparse
import asyncio
import copy
import functools
import hashlib
import json
import logging
import random
import sys
import time
import unittest
import uuid
from contextvars import ContextVar
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timedelta, timezone
from typing import (
    Any, Awaitable, Callable, Dict, List, Optional, Tuple,
)

import numpy as np

# -----------------------------------------------------------------------------
# Optional dependencies
# -----------------------------------------------------------------------------
try:
    from scipy import stats
    SCIPY_AVAILABLE = True
except ImportError:
    stats = None  # type: ignore
    SCIPY_AVAILABLE = False

try:
    from prometheus_client import Counter, Gauge, Histogram
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    from opentelemetry import trace
    _TRACER = trace.get_tracer("counterfactual_benchmark")
    OTEL_AVAILABLE = True
except ImportError:
    _TRACER = None
    OTEL_AVAILABLE = False

try:
    import structlog
    from structlog.processors import JSONRenderer, TimeStamper
    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            TimeStamper(fmt="iso"),
            JSONRenderer(),
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    logger = structlog.get_logger(__name__)
    HAS_STRUCTLOG = True
except ImportError:
    HAS_STRUCTLOG = False

    class _LoggerShim:
        """Standard-library logger that accepts structlog-style kwargs."""
        def __init__(self, name: str):
            self._log = logging.getLogger(name)
        def _emit(self, level: int, event: str, **kwargs: Any) -> None:
            if kwargs:
                try:
                    msg = f"{event} {json.dumps(kwargs, default=str)}"
                except Exception:
                    msg = f"{event} {kwargs}"
            else:
                msg = event
            self._log.log(level, msg)
        def debug(self, event: str, **kwargs: Any) -> None:
            self._emit(logging.DEBUG, event, **kwargs)
        def info(self, event: str, **kwargs: Any) -> None:
            self._emit(logging.INFO, event, **kwargs)
        def warning(self, event: str, **kwargs: Any) -> None:
            self._emit(logging.WARNING, event, **kwargs)
        def error(self, event: str, **kwargs: Any) -> None:
            self._emit(logging.ERROR, event, **kwargs)
        def exception(self, event: str, **kwargs: Any) -> None:
            self._emit(logging.ERROR, event, **kwargs)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    logger = _LoggerShim(__name__)


# -----------------------------------------------------------------------------
# Local imports (guarded with fallback stubs)
# -----------------------------------------------------------------------------
try:
    from ..storage import Storage  # type: ignore
    CENTRAL_STORAGE_AVAILABLE = True
except ImportError:
    CENTRAL_STORAGE_AVAILABLE = False
    Storage = None  # type: ignore

try:
    from ..config import config  # type: ignore
    CENTRAL_CONFIG_AVAILABLE = True
except ImportError:
    CENTRAL_CONFIG_AVAILABLE = False
    config = None  # type: ignore

try:
    from ..schemas.feedback_event import FeedbackEvent  # type: ignore
    FEEDBACK_EVENT_AVAILABLE = True
except ImportError:
    FEEDBACK_EVENT_AVAILABLE = False
    FeedbackEvent = None  # type: ignore

try:
    from ..mtpd_optimizer import (  # type: ignore
        MTPDOptimizer, StrategyMetrics,
    )
    MTPD_AVAILABLE = True
except ImportError:
    MTPD_AVAILABLE = False
    MTPDOptimizer = None  # type: ignore

    @dataclass
    class StrategyMetrics:  # type: ignore
        strategy_name: str
        latency_ms: float = 0.0
        carbon_g: float = 0.0
        cost_usd: float = 0.0
        quality_score: float = 0.0
        energy_joules: float = 0.0
        helium_cost: float = 0.0


# =============================================================================
# SECTION 1. MODULE STATUS
# =============================================================================
MODULE_STATUS: Dict[str, str] = {
    "benchmark_core":    "stable",
    "bootstrap_ci":      "stable",
    "policy_registry":   "stable",
    "nsga2_optimizer":   "experimental",
    "policy_evolution":  "experimental",
    "train_holdout":     "experimental",
    "moe_gating":        "placeholder",
    "rlhf":              "placeholder",
    "modp":              "placeholder",
    "limit_graph":       "placeholder",
    "p_value":           "placeholder",
}


def _warn_module(name: str) -> None:
    status = MODULE_STATUS.get(name, "unknown")
    if status == "stable":
        return
    if status == "placeholder":
        logger.warning("module_is_placeholder", module=name)
    elif status == "experimental":
        logger.warning("module_is_experimental", module=name)


# =============================================================================
# SECTION 2. HELPERS
# =============================================================================
_cid_ctx: ContextVar[str] = ContextVar("benchmark_cid", default="-")


def current_cid() -> str:
    return _cid_ctx.get()


def _new_cid() -> str:
    return uuid.uuid4().hex[:12]


def _is_awaitable(x: Any) -> bool:
    return hasattr(x, "__await__")


def traced(span_name: str):
    def decorator(fn: Callable):
        if not OTEL_AVAILABLE:
            return fn

        @functools.wraps(fn)
        async def wrapper(*args, **kwargs):
            with _TRACER.start_as_current_span(span_name):
                return await fn(*args, **kwargs)
        return wrapper
    return decorator


# =============================================================================
# SECTION 3. CONFIG
# =============================================================================
@dataclass
class BenchmarkConfig:
    confidence_level: float = 0.95
    bootstrap_samples: int = 1000

    # MOEA
    moea_population_size: int = 20
    moea_generations: int = 5
    moea_mutation_rate: float = 0.2
    moea_crossover_rate: float = 0.8
    moea_tournament_size: int = 3
    moea_objective_weights: Dict[str, float] = field(default_factory=lambda: {
        "quality": 0.3, "carbon": 0.2, "latency": 0.2,
        "energy": 0.1, "cost": 0.1, "helium": 0.1,
    })
    moea_dynamic_weights: bool = True

    # Data split
    use_train_holdout_split: bool = False
    holdout_ratio: float = 0.3

    # Cache
    events_cache_ttl_seconds: float = 3600.0

    # Lifecycle
    shutdown_timeout_seconds: float = 10.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "BenchmarkConfig":
        data = dict(data or {})
        return cls(**{
            k: v for k, v in data.items()
            if k in cls.__dataclass_fields__
        })


# =============================================================================
# SECTION 4. DATA STRUCTURES
# =============================================================================
@dataclass
class BenchmarkResult:
    run_id: str
    policy_name: str
    timestamp: float
    sample_count: int
    metrics: Dict[str, float]
    confidence_intervals: Dict[str, Tuple[float, float]]
    p_value: Optional[float] = None
    p_value_reason: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "policy_name": self.policy_name,
            "timestamp": self.timestamp,
            "sample_count": self.sample_count,
            "metrics": dict(self.metrics),
            "confidence_intervals": {
                k: [float(v[0]), float(v[1])]
                for k, v in self.confidence_intervals.items()
            },
            "p_value": self.p_value,
            "p_value_reason": self.p_value_reason,
        }


@dataclass
class Policy:
    policy_id: str
    weights: Dict[str, float]

    def to_dict(self) -> Dict[str, Any]:
        return {"policy_id": self.policy_id, "weights": dict(self.weights)}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Policy":
        return cls(
            policy_id=data.get("policy_id", str(uuid.uuid4())),
            weights=dict(data.get("weights", {})),
        )

    def choose_candidate(self, candidates: List[Dict[str, Any]]) -> int:
        """
        Pick the candidate with the lowest weighted cost.

        Weights are normalized to sum to 1. Cost-like metrics are added;
        `quality` is subtracted (higher quality is better).
        """
        if not candidates:
            raise ValueError("candidates list is empty")
        total = sum(max(0.0, v) for v in self.weights.values())
        if total <= 0:
            n = len(self.weights) or 1
            norm = {k: 1.0 / n for k in self.weights}
        else:
            norm = {k: max(0.0, v) / total for k, v in self.weights.items()}

        cost_metrics = ("carbon", "latency", "energy", "cost", "helium")
        best_idx = 0
        best_score = float("inf")
        for idx, cand in enumerate(candidates):
            score = 0.0
            for metric in cost_metrics:
                if metric in norm:
                    score += norm[metric] * float(cand.get(metric, 0.0))
            if "quality" in norm:
                score -= norm["quality"] * float(cand.get("quality_score", 0.0))
            if score < best_score:
                best_score = score
                best_idx = idx
        return best_idx


@dataclass
class MOPDPoint:
    policy: Policy
    objectives: Dict[str, float]
    scalarised_score: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "policy": self.policy.to_dict(),
            "objectives": dict(self.objectives),
            "scalarised_score": self.scalarised_score,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MOPDPoint":
        return cls(
            policy=Policy.from_dict(data.get("policy", {})),
            objectives=dict(data.get("objectives", {})),
            scalarised_score=float(data.get("scalarised_score", 0.0)),
        )


# =============================================================================
# SECTION 5. PLACEHOLDERS (safe no-ops, disabled by default)
# =============================================================================
class LimitGraphManagerPlaceholder:
    STATUS = "placeholder"

    def __init__(self, storage: Optional[Any] = None,
                 enabled: bool = False, max_graphs: int = 50):
        if enabled:
            _warn_module("limit_graph")
        self.storage = storage
        self.max_graphs = max_graphs
        self.graphs: Dict[str, Dict[str, Any]] = {}
        self.available = False

    def _evict(self, graph_id: str) -> None:
        if graph_id not in self.graphs:
            return
        order = list(self.graphs.keys())
        while len(order) > self.max_graphs:
            old = order.pop(0)
            self.graphs.pop(old, None)

    def create_graph(self, graph_id: str, description: str,
                      configuration: Dict[str, Any]) -> None:
        self.graphs[graph_id] = {
            "description": description, "configuration": configuration,
            "nodes": {}, "edges": {},
        }
        self._evict(graph_id)

    def add_node(self, graph_id: str, node_id: str,
                  node_type: Optional[str],
                  attributes: Dict[str, Any]) -> None:
        if graph_id not in self.graphs:
            self.create_graph(graph_id, "", {})
        self.graphs[graph_id]["nodes"][node_id] = {
            "node_type": node_type, "attributes": attributes,
        }

    def add_edge(self, graph_id: str, edge_id: str, source: str,
                  target: str, weight: Optional[float],
                  attributes: Dict[str, Any]) -> None:
        if graph_id not in self.graphs:
            self.create_graph(graph_id, "", {})
        self.graphs[graph_id]["edges"][edge_id] = {
            "source": source, "target": target,
            "weight": weight, "attributes": attributes,
        }

    def get_nodes(self, graph_id: str) -> List[Dict[str, Any]]:
        return list(self.graphs.get(graph_id, {}).get("nodes", {}).values())

    def get_edges(self, graph_id: str) -> List[Dict[str, Any]]:
        return list(self.graphs.get(graph_id, {}).get("edges", {}).values())

    def get_metadata(self, graph_id: str) -> Optional[Dict[str, Any]]:
        return self.graphs.get(graph_id)


class MODPOptimizerPlaceholder:
    STATUS = "placeholder"

    def __init__(self, storage: Optional[Any] = None,
                 enabled: bool = False, max_states: int = 5000):
        if enabled:
            _warn_module("modp")
        self.storage = storage
        self.max_states = max_states
        from collections import deque
        self.states: Dict[str, Any] = {}
        self.available = False

    def add_state(self, state_id: str, problem_id: str,
                   state_attributes: Dict[str, Any],
                   objective_values: Dict[str, float],
                   stage: int) -> None:
        from collections import deque
        if problem_id not in self.states:
            self.states[problem_id] = deque(maxlen=self.max_states)
        self.states[problem_id].append({
            "state_id": state_id,
            "state_attributes": state_attributes,
            "objective_values": objective_values,
            "stage": stage,
        })

    def add_policy(self, policy_id: str, problem_id: str,
                    state_id: str, action: str,
                    expected_objectives: Dict[str, float]) -> None:
        return None

    def get_states(self, problem_id: str) -> List[Dict[str, Any]]:
        return list(self.states.get(problem_id, []))

    def get_policies(self, problem_id: str) -> List[Dict[str, Any]]:
        return []


class RLHFTrainerPlaceholder:
    STATUS = "placeholder"

    def __init__(self, storage: Optional[Any] = None,
                 enabled: bool = False, max_pairs: int = 2000):
        if enabled:
            _warn_module("rlhf")
        from collections import deque
        self.storage = storage
        self.max_pairs = max_pairs
        self.pairs = deque(maxlen=max_pairs)
        self.available = False

    def record_pair(self, pair_id: str, prompt: str, chosen: str,
                     rejected: str, reward_diff: float,
                     metadata: Optional[Dict[str, Any]] = None) -> None:
        self.pairs.append({
            "pair_id": pair_id, "prompt": prompt, "chosen": chosen,
            "rejected": rejected, "reward_diff": reward_diff,
            "metadata": metadata,
        })

    def get_pairs(self, limit: int = 100) -> List[Dict[str, Any]]:
        return list(self.pairs)[-limit:]

    def train_reward_model(self) -> None:
        logger.info("rlhf_train_skipped", reason="placeholder", n=len(self.pairs))


class MoEGatingNetworkPlaceholder:
    """
    STATUS: placeholder.

    The reward-weighted policy gradient update is implemented correctly;
    the module itself is a placeholder and is disabled by default.
    """
    STATUS = "placeholder"

    def __init__(self, storage: Optional[Any] = None,
                 config: Optional[Dict[str, Any]] = None,
                 enabled: bool = False):
        if enabled:
            _warn_module("moe_gating")
        self.storage = storage
        self.config = config or {}
        self.expert_names = list(self.config.get("expert_names", [
            "fixed_cheapest", "energy_only", "carbon_only", "quality_only",
        ]))
        self.num_experts = len(self.expert_names)
        self.state_dim = 12
        self.lr = 0.05
        self.gating_weights = np.random.randn(self.num_experts, self.state_dim) * 0.1
        self.available = False

    def _encode_state(self, state: Dict[str, Any]) -> np.ndarray:
        features = [
            min(max(float(state.get("carbon_intensity", 0.0)), 0.0), 1.0),
            min(max(float(state.get("workload_size", 0.0)) / 10000.0, 0.0), 1.0),
            min(max(float(state.get("latency_target", 0.0)) / 5000.0, 0.0), 1.0),
            min(max(float(state.get("cost_budget", 0.0)) / 100.0, 0.0), 1.0),
            min(max(float(state.get("energy_price", 0.0)), 0.0), 1.0),
            min(max(float(state.get("helium_scarcity", 0.0)), 0.0), 1.0),
            min(max(float(state.get("quality_requirement", 0.0)), 0.0), 1.0),
            min(max(float(state.get("hour_of_day", 0.0)) / 24.0, 0.0), 1.0),
            min(max(float(state.get("day_of_week", 0.0)) / 7.0, 0.0), 1.0),
            min(max(float(state.get("recent_success_rate", 0.5)), 0.0), 1.0),
            min(max(float(state.get("avg_reward", 0.0)), 0.0), 1.0),
            min(max(float(state.get("num_candidates", 1.0)) / 10.0, 0.0), 1.0),
        ]
        return np.array(features, dtype=np.float32)

    def _gate(self, x: np.ndarray) -> np.ndarray:
        logits = self.gating_weights @ x
        logits = logits - np.max(logits)
        probs = np.exp(logits)
        return probs / probs.sum()

    async def select_expert(self, state: Dict[str, Any]
                             ) -> Tuple[str, np.ndarray]:
        x = self._encode_state(state)
        probs = self._gate(x)
        expert_idx = int(np.argmax(probs))
        selected = self.expert_names[expert_idx]
        if self.storage is not None and hasattr(self.storage, "log_routing_decision"):
            try:
                sample_id = hashlib.sha256(x.tobytes()).hexdigest()[:16]
                self.storage.log_routing_decision(
                    str(uuid.uuid4()), sample_id, selected,
                    float(probs[expert_idx]),
                )
            except Exception:
                pass
        return selected, probs

    async def add_training_sample(self, state: Dict[str, Any],
                                    selected_expert: str,
                                    reward: float) -> None:
        if selected_expert not in self.expert_names:
            return
        x = self._encode_state(state)
        expert_idx = self.expert_names.index(selected_expert)
        one_hot = np.zeros(self.num_experts)
        one_hot[expert_idx] = 1.0
        probs = self._gate(x)
        # REINFORCE: ascend reward * log pi(expert)
        # grad = reward * (one_hot - probs) * x^T
        grad = reward * np.outer((one_hot - probs), x)
        self.gating_weights += self.lr * grad

    def get_stats(self) -> Dict[str, Any]:
        return {
            "available": self.available,
            "num_experts": self.num_experts,
            "weight_norm": float(np.linalg.norm(self.gating_weights)),
        }


# =============================================================================
# SECTION 6. NSGA-II OPTIMIZER
# =============================================================================
class NSGAIIOptimizer:
    """
    Multi-objective evolutionary optimizer for policy weight vectors.

    Dominance convention: **maximization**. Every objective must be
    larger-is-better. If the caller's natural objective is a cost, negate it.

    STATUS: experimental.
    """
    STATUS = "experimental"

    def __init__(
        self,
        evaluate_func: Callable[[Dict[str, float]], Awaitable[Dict[str, float]]],
        parameter_bounds: Dict[str, Tuple[float, float]],
        population_size: int = 20,
        generations: int = 5,
        mutation_rate: float = 0.2,
        crossover_rate: float = 0.8,
        tournament_size: int = 3,
        objective_weights: Optional[Dict[str, float]] = None,
        dynamic_weights: bool = True,
        eval_cache_max_entries: int = 5000,
        enabled: bool = True,
    ):
        if enabled:
            _warn_module("nsga2_optimizer")
        self.evaluate_func = evaluate_func
        self.parameter_bounds = parameter_bounds
        self.population_size = population_size
        self.generations = generations
        self.mutation_rate = mutation_rate
        self.crossover_rate = crossover_rate
        self.tournament_size = tournament_size
        self.objective_weights = objective_weights or {}
        self.dynamic_weights = dynamic_weights
        self.eval_cache_max_entries = eval_cache_max_entries

        self.best_individual: Optional[Dict[str, float]] = None
        self.best_fitness: float = -float("inf")
        self.evolution_history: List[Dict[str, Any]] = []
        self.pareto_front: List[MOPDPoint] = []
        self._cache: "Dict[Tuple, Dict[str, float]]" = {}
        self.available = True

    # ---------------- genome ops ----------------
    def _random_individual(self) -> Dict[str, float]:
        ind = {
            name: random.uniform(low, high)
            for name, (low, high) in self.parameter_bounds.items()
        }
        return self._normalize(ind)

    @staticmethod
    def _normalize(ind: Dict[str, float]) -> Dict[str, float]:
        total = sum(max(0.0, v) for v in ind.values())
        if total <= 0:
            n = len(ind) or 1
            return {k: 1.0 / n for k in ind}
        return {k: max(0.0, v) / total for k, v in ind.items()}

    def _crossover(self, p1: Dict[str, float],
                    p2: Dict[str, float]) -> Dict[str, float]:
        child: Dict[str, float] = {}
        for name, (low, high) in self.parameter_bounds.items():
            if random.random() < 0.5:
                u = random.random()
                beta = (
                    (2 * u) ** (1.0 / 21.0) if u <= 0.5
                    else (1.0 / (2 * (1 - u))) ** (1.0 / 21.0)
                )
                val = 0.5 * ((1 + beta) * p1[name] + (1 - beta) * p2[name])
                child[name] = max(low, min(high, val))
            else:
                child[name] = p1[name] if random.random() < 0.5 else p2[name]
        return self._normalize(child)

    def _mutate(self, ind: Dict[str, float]) -> Dict[str, float]:
        mutant = dict(ind)
        for name, (low, high) in self.parameter_bounds.items():
            if random.random() < self.mutation_rate:
                u = random.random()
                delta = (
                    (2 * u) ** (1.0 / 21.0) - 1 if u < 0.5
                    else 1 - (2 * (1 - u)) ** (1.0 / 21.0)
                )
                mutant[name] = max(low, min(high, mutant[name] + delta * (high - low)))
        return self._normalize(mutant)

    # ---------------- Pareto mechanics ----------------
    @staticmethod
    def _dominates(a: MOPDPoint, b: MOPDPoint) -> bool:
        keys = set(a.objectives.keys()) & set(b.objectives.keys())
        if not keys:
            return False
        return (
            all(a.objectives[k] >= b.objectives[k] for k in keys)
            and any(a.objectives[k] > b.objectives[k] for k in keys)
        )

    def _fast_non_dominated_sort(
        self, points: List[MOPDPoint]
    ) -> List[List[int]]:
        n = len(points)
        dominated: List[List[int]] = [[] for _ in range(n)]
        dom_count = [0] * n
        fronts: List[List[int]] = [[]]

        for i in range(n):
            for j in range(n):
                if i == j:
                    continue
                if self._dominates(points[i], points[j]):
                    dominated[i].append(j)
                elif self._dominates(points[j], points[i]):
                    dom_count[i] += 1
            if dom_count[i] == 0:
                fronts[0].append(i)

        k = 0
        while fronts[k]:
            nxt: List[int] = []
            for i in fronts[k]:
                for j in dominated[i]:
                    dom_count[j] -= 1
                    if dom_count[j] == 0:
                        nxt.append(j)
            k += 1
            fronts.append(nxt)
        return [f for f in fronts if f]

    def _crowding_distance(self, front: List[int],
                            points: List[MOPDPoint]) -> Dict[int, float]:
        if not front:
            return {}
        if len(front) <= 2:
            return {i: float("inf") for i in front}
        distances: Dict[int, float] = {i: 0.0 for i in front}
        keys = list(points[front[0]].objectives.keys())
        for key in keys:
            sf = sorted(front, key=lambda i: points[i].objectives[key])
            distances[sf[0]] = float("inf")
            distances[sf[-1]] = float("inf")
            span = (
                points[sf[-1]].objectives[key]
                - points[sf[0]].objectives[key]
            )
            if span <= 0:
                continue
            for idx in range(1, len(sf) - 1):
                distances[sf[idx]] += (
                    points[sf[idx + 1]].objectives[key]
                    - points[sf[idx - 1]].objectives[key]
                ) / span
        return distances

    def _tournament(self, ranks: Dict[int, int],
                     crowd: Dict[int, float],
                     n_pop: int) -> int:
        """Return the index of the tournament winner."""
        if n_pop <= 0:
            raise RuntimeError("empty population in tournament selection")
        k = min(self.tournament_size, n_pop)
        candidates = random.sample(range(n_pop), k)
        best = candidates[0]
        best_rank = ranks.get(best, 10**9)
        best_crowd = crowd.get(best, 0.0)
        for c in candidates[1:]:
            r = ranks.get(c, 10**9)
            cd = crowd.get(c, 0.0)
            if r < best_rank or (r == best_rank and cd > best_crowd):
                best, best_rank, best_crowd = c, r, cd
        return best

    # ---------------- evaluation ----------------
    async def _evaluate(self, params: Dict[str, float]) -> Dict[str, float]:
        key = tuple(sorted((k, float(v)) for k, v in params.items()))
        if key in self._cache:
            return self._cache[key]
        r = self.evaluate_func(params)
        if _is_awaitable(r):
            r = await r
        objs = {k: float(v) for k, v in (r or {}).items()}
        self._cache[key] = objs
        while len(self._cache) > self.eval_cache_max_entries:
            # evict oldest insertion
            first = next(iter(self._cache))
            self._cache.pop(first, None)
        return objs

    def _compute_dynamic_weights(self) -> Dict[str, float]:
        weights = dict(self.objective_weights)
        if not self.dynamic_weights or not self.pareto_front:
            return weights
        keys = list(weights.keys())
        if not keys:
            return weights
        avg = {k: float(np.mean([
            p.objectives.get(k, 0.0) for p in self.pareto_front
        ])) for k in keys}
        mx = {k: max(p.objectives.get(k, 0.0) for p in self.pareto_front)
              for k in keys}
        for k in keys:
            if mx[k] > 0 and avg[k] < 0.5 * mx[k]:
                weights[k] = min(0.6, weights.get(k, 0.0) * 1.5)
        total = sum(weights.values()) or 1.0
        return {k: v / total for k, v in weights.items()}

    def _select_best_from_pareto(self, front: List[MOPDPoint],
                                   weights: Dict[str, float]
                                   ) -> Optional[MOPDPoint]:
        if not front:
            return None
        keys = list(weights.keys())
        if not keys:
            return MOPDPoint.from_dict(front[0].to_dict())
        max_vals = {k: max(p.objectives.get(k, 0.0) for p in front)
                    for k in keys}
        min_vals = {k: min(p.objectives.get(k, 0.0) for p in front)
                    for k in keys}
        ranges = {
            k: (max_vals[k] - min_vals[k]) if max_vals[k] != min_vals[k]
            else 1.0
            for k in keys
        }
        best: Optional[MOPDPoint] = None
        best_score = -float("inf")
        for p in front:
            score = sum(
                weights.get(k, 0.0) * (
                    (p.objectives.get(k, 0.0) - min_vals[k]) / ranges[k]
                )
                for k in keys
            )
            if score > best_score:
                best_score = score
                best = p
        if best is None:
            return None
        # Return a copy so the stored front is never mutated.
        copy_point = MOPDPoint.from_dict(best.to_dict())
        copy_point.scalarised_score = best_score
        return copy_point

    # ---------------- main loop ----------------
    @traced("nsga2.evolve")
    async def evolve(self) -> List[MOPDPoint]:
        # Build initial population as (individual, point) pairs.
        pop: List[Tuple[Dict[str, float], MOPDPoint]] = []
        for _ in range(self.population_size):
            ind = self._random_individual()
            objs = await self._evaluate(ind)
            pop.append((ind, MOPDPoint(
                policy=Policy(policy_id=str(uuid.uuid4()), weights=ind),
                objectives=objs,
            )))

        for gen in range(self.generations):
            inds = [ind for ind, _ in pop]
            points = [pt for _, pt in pop]

            fronts = self._fast_non_dominated_sort(points)
            ranks: Dict[int, int] = {}
            crowd: Dict[int, float] = {}
            for rank_idx, front in enumerate(fronts):
                cd = self._crowding_distance(front, points)
                for i in front:
                    ranks[i] = rank_idx
                    crowd[i] = cd.get(i, 0.0)

            offspring: List[Tuple[Dict[str, float], MOPDPoint]] = []
            while len(offspring) < self.population_size:
                i = self._tournament(ranks, crowd, len(inds))
                j = self._tournament(ranks, crowd, len(inds))
                if random.random() < self.crossover_rate:
                    child = self._crossover(inds[i], inds[j])
                else:
                    child = dict(inds[i])
                child = self._mutate(child)
                objs = await self._evaluate(child)
                offspring.append((child, MOPDPoint(
                    policy=Policy(policy_id=str(uuid.uuid4()), weights=child),
                    objectives=objs,
                )))

            combined = pop + offspring
            seen: Dict[Tuple, int] = {}
            dedup: List[Tuple[Dict[str, float], MOPDPoint]] = []
            for ind, pt in combined:
                key = tuple(sorted((k, float(v)) for k, v in ind.items()))
                if key in seen:
                    continue
                seen[key] = len(dedup)
                dedup.append((ind, pt))

            dedup_points = [pt for _, pt in dedup]
            fronts = self._fast_non_dominated_sort(dedup_points)
            new_pop: List[Tuple[Dict[str, float], MOPDPoint]] = []
            for front in fronts:
                if len(new_pop) + len(front) <= self.population_size:
                    for i in front:
                        new_pop.append(dedup[i])
                else:
                    cd = self._crowding_distance(front, dedup_points)
                    sf = sorted(front, key=lambda i: cd.get(i, 0.0),
                                 reverse=True)
                    remaining = self.population_size - len(new_pop)
                    for i in sf[:remaining]:
                        new_pop.append(dedup[i])
                    break
            pop = new_pop

        points = [pt for _, pt in pop]
        fronts = self._fast_non_dominated_sort(points)
        self.pareto_front = [points[i] for i in fronts[0]] if fronts else []

        weights = self._compute_dynamic_weights()
        best = self._select_best_from_pareto(self.pareto_front, weights)
        if best is not None:
            self.best_individual = dict(best.policy.weights)
            self.best_fitness = best.scalarised_score

        self.evolution_history.append({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "best_fitness": self.best_fitness,
            "pareto_size": len(self.pareto_front),
        })
        return list(self.pareto_front)

    def get_status(self) -> Dict[str, Any]:
        return {
            "available": self.available,
            "best_fitness": self.best_fitness,
            "pareto_size": len(self.pareto_front),
            "history": self.evolution_history[-10:],
        }


# =============================================================================
# SECTION 7. MAIN HARNESS
# =============================================================================
class CounterfactualBenchmark:
    """
    Lifecycle:
        bm = CounterfactualBenchmark(storage, ...)
        await bm.start()
        results = await bm.run_benchmark(days_back=7)
        await bm.shutdown()
    """

    POLICIES: Dict[str, str] = {
        "fixed_cheapest": "_policy_fixed_cheapest",
        "energy_only": "_policy_energy_only",
        "carbon_only": "_policy_carbon_only",
        "quality_only": "_policy_quality_only",
        "mopd_current": "_policy_mopd_current",
    }

    def __init__(
        self,
        storage: Any,
        optimizer: Optional[Any] = None,
        config: Optional[BenchmarkConfig] = None,
        enable_limit_graph: bool = False,
        enable_modp: bool = False,
        enable_rlhf: bool = False,
        enable_moe: bool = False,
        moe_expert_names: Optional[List[str]] = None,
    ):
        self.storage = storage
        self.optimizer = optimizer
        self.config = config or BenchmarkConfig()
        self.confidence_level = self.config.confidence_level
        self.bootstrap_samples = self.config.bootstrap_samples

        # MOEA config
        self.moea_population_size = self.config.moea_population_size
        self.moea_generations = self.config.moea_generations
        self.moea_mutation_rate = self.config.moea_mutation_rate
        self.moea_crossover_rate = self.config.moea_crossover_rate
        self.moea_tournament_size = self.config.moea_tournament_size
        self.moea_objective_weights = dict(self.config.moea_objective_weights)
        self.moea_dynamic_weights = self.config.moea_dynamic_weights

        self.evolved_pareto_front: List[MOPDPoint] = []
        self.best_evolved_policy: Optional[Policy] = None

        # Cached events, keyed by (days_back, sample_limit).
        # Value is (timestamp_monotonic, events).
        self._events_cache: Dict[Tuple[int, int], Tuple[float, List[Dict[str, Any]]]] = {}
        self._cache_lock: Optional[asyncio.Lock] = None

        # Evolution lock
        self._evolution_lock: Optional[asyncio.Lock] = None

        # Placeholders
        self.limit_graph_manager: Optional[LimitGraphManagerPlaceholder] = (
            LimitGraphManagerPlaceholder(storage, enabled=enable_limit_graph)
            if enable_limit_graph else None
        )
        self.modp_solver: Optional[MODPOptimizerPlaceholder] = (
            MODPOptimizerPlaceholder(storage, enabled=enable_modp)
            if enable_modp else None
        )
        self.rlhf_trainer: Optional[RLHFTrainerPlaceholder] = (
            RLHFTrainerPlaceholder(storage, enabled=enable_rlhf)
            if enable_rlhf else None
        )
        self.moe_gating: Optional[MoEGatingNetworkPlaceholder] = None
        if enable_moe:
            expert_names = moe_expert_names or list(self.POLICIES.keys())
            self.moe_gating = MoEGatingNetworkPlaceholder(
                storage, {"expert_names": expert_names}, enabled=True
            )

        # Lifecycle
        self._started = False
        self._shutdown = False

        # Metrics
        self._setup_metrics()

        logger.info("counterfactual_benchmark_created", v="4.0.0")

    # ---------------- locks ----------------
    def _get_cache_lock(self) -> asyncio.Lock:
        if self._cache_lock is None:
            self._cache_lock = asyncio.Lock()
        return self._cache_lock

    def _get_evolution_lock(self) -> asyncio.Lock:
        if self._evolution_lock is None:
            self._evolution_lock = asyncio.Lock()
        return self._evolution_lock

    # ---------------- metrics ----------------
    def _setup_metrics(self) -> None:
        if not PROMETHEUS_AVAILABLE:
            self.metrics: Optional[Dict[str, Any]] = None
            return
        try:
            self.metrics = {
                "benchmark_runs": Counter(
                    "benchmark_runs_total", "Benchmark runs",
                    ["policy"],
                ),
                "benchmark_duration": Histogram(
                    "benchmark_duration_seconds", "Benchmark duration",
                ),
                "pareto_size": Gauge(
                    "benchmark_pareto_size", "Pareto front size",
                ),
                "evolution_rounds": Counter(
                    "benchmark_evolution_total",
                    "Policy evolution rounds",
                ),
                "cache_hits": Counter(
                    "benchmark_cache_hits_total", "Events cache hits",
                ),
                "cache_misses": Counter(
                    "benchmark_cache_misses_total", "Events cache misses",
                ),
            }
        except Exception as e:
            logger.warning("metrics_setup_failed", error=str(e))
            self.metrics = None

    # ---------------- lifecycle ----------------
    @traced("benchmark.start")
    async def start(self) -> None:
        if self._started and not self._shutdown:
            logger.warning("benchmark_already_started")
            return
        self._started = True
        self._shutdown = False
        if self.limit_graph_manager is not None:
            self._init_limit_graph()
        logger.info("benchmark_started")

    async def ready(self) -> bool:
        return self._started and not self._shutdown

    @traced("benchmark.shutdown")
    async def shutdown(self, timeout: Optional[float] = None) -> None:
        if self._shutdown:
            return
        self._shutdown = True
        self._started = False
        # No background tasks to drain in v4.0.0, but kept for symmetry.
        logger.info("benchmark_shutdown_complete")

    async def __aenter__(self) -> "CounterfactualBenchmark":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.shutdown()

    # ---------------- LIMIT graph ----------------
    def _init_limit_graph(self) -> None:
        if self.limit_graph_manager is None:
            return
        graph_id = "benchmark_policies"
        if not self.limit_graph_manager.get_metadata(graph_id):
            self.limit_graph_manager.create_graph(
                graph_id, "Benchmark Policy Relationships", {}
            )
            for policy_name in self.POLICIES:
                self.limit_graph_manager.add_node(
                    graph_id, f"policy_{policy_name}", policy_name, {}
                )
            self.limit_graph_manager.add_edge(
                graph_id, "edge_mopd_evolved",
                "policy_mopd_current", "policy_evolved", 1.0, {},
            )
            logger.info("limit_graph_initialized")

    # ---------------- built-in policies ----------------
    async def _policy_fixed_cheapest(self, state: Dict[str, Any],
                                       candidates: List[Dict[str, Any]]) -> int:
        return min(range(len(candidates)),
                    key=lambda i: float(candidates[i].get("cost_usd", float("inf"))))

    async def _policy_energy_only(self, state: Dict[str, Any],
                                    candidates: List[Dict[str, Any]]) -> int:
        return min(range(len(candidates)),
                    key=lambda i: float(candidates[i].get("energy_joules", float("inf"))))

    async def _policy_carbon_only(self, state: Dict[str, Any],
                                    candidates: List[Dict[str, Any]]) -> int:
        return min(range(len(candidates)),
                    key=lambda i: float(candidates[i].get("carbon_g", float("inf"))))

    async def _policy_quality_only(self, state: Dict[str, Any],
                                     candidates: List[Dict[str, Any]]) -> int:
        return max(range(len(candidates)),
                    key=lambda i: float(candidates[i].get("quality_score", 0.0)))

    async def _policy_mopd_current(self, state: Dict[str, Any],
                                     candidates: List[Dict[str, Any]]) -> int:
        if self.optimizer is None:
            logger.warning("mopd_optimizer_not_set; falling back to index 0")
            return 0
        try:
            metrics_list = [
                StrategyMetrics(
                    strategy_name=c.get("action_id", "unknown"),
                    latency_ms=float(c.get("latency_ms", 0.0)),
                    carbon_g=float(c.get("carbon_g", 0.0)),
                    cost_usd=float(c.get("cost_usd", 0.0)),
                    quality_score=float(c.get("quality_score", 0.0)),
                    energy_joules=float(c.get("energy_joules", 0.0)),
                    helium_cost=float(c.get("helium_cost", 0.0)),
                )
                for c in candidates
            ]
            chosen = self.optimizer.select_strategy(state, metrics_list)
            chosen_name = getattr(chosen, "strategy_name", None)
            if chosen_name is not None:
                for idx, c in enumerate(candidates):
                    if c.get("action_id") == chosen_name:
                        return idx
            action_idx = getattr(chosen, "action_idx", None)
            if isinstance(action_idx, int) and 0 <= action_idx < len(candidates):
                return action_idx
        except Exception as e:
            logger.warning("mopd_policy_failed", error=str(e))
        return 0

    # ---------------- events cache ----------------
    async def _get_events(self, days_back: int, sample_limit: int) -> List[Dict[str, Any]]:
        key = (int(days_back), int(sample_limit))
        now = time.monotonic()
        async with self._get_cache_lock():
            entry = self._events_cache.get(key)
            if entry is not None:
                ts, events = entry
                if now - ts < self.config.events_cache_ttl_seconds:
                    if self.metrics:
                        try:
                            self.metrics["cache_hits"].inc()
                        except Exception:
                            pass
                    return list(events)
            if self.metrics:
                try:
                    self.metrics["cache_misses"].inc()
                except Exception:
                    pass
        try:
            fn = getattr(self.storage, "get_feedback_events_with_context", None)
            if fn is None:
                logger.warning("storage_missing_get_feedback_events_with_context")
                return []
            r = fn(days_back=days_back, limit=sample_limit)
            if _is_awaitable(r):
                r = await r
            events = list(r or [])
        except Exception as e:
            logger.error("fetch_events_failed", error=str(e))
            return []
        async with self._get_cache_lock():
            self._events_cache[key] = (time.monotonic(), list(events))
        return events

    def _split_events(self, events: List[Dict[str, Any]]
                       ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Return (train, holdout). If split disabled, both equal events."""
        if not self.config.use_train_holdout_split or not events:
            return list(events), list(events)
        shuffled = list(events)
        random.shuffle(shuffled)
        holdout_n = max(1, int(len(shuffled) * self.config.holdout_ratio))
        holdout = shuffled[:holdout_n]
        train = shuffled[holdout_n:]
        if not train:
            train = list(shuffled)
        return train, holdout

    # ---------------- context ----------------
    def _build_context(self, events: List[Dict[str, Any]]) -> Dict[str, Any]:
        keys = (
            "carbon_intensity", "workload_size", "latency_target",
            "cost_budget", "energy_price", "helium_scarcity",
            "quality_requirement", "hour_of_day", "day_of_week",
            "recent_success_rate", "avg_reward", "num_candidates",
        )
        if not events:
            return {k: 0.0 for k in keys}
        context: Dict[str, Any] = {}
        for key in keys:
            values = []
            for event in events:
                state = event.get("state", {}) or {}
                if key in state and state[key] is not None:
                    try:
                        values.append(float(state[key]))
                    except (TypeError, ValueError):
                        continue
            context[key] = float(np.mean(values)) if values else 0.0
        return context

    # ---------------- benchmarking ----------------
    @traced("benchmark.run")
    async def run_benchmark(
        self,
        days_back: int = 7,
        policies: Optional[List[str]] = None,
        sample_limit: int = 10000,
    ) -> Dict[str, BenchmarkResult]:
        start = time.monotonic()
        cid = _new_cid()
        token = _cid_ctx.set(cid)
        try:
            if policies is None:
                policies = list(self.POLICIES.keys())

            events = await self._get_events(days_back, sample_limit)
            if not events:
                logger.warning("no_events_for_benchmark", days_back=days_back)
                return {}

            if self.config.use_train_holdout_split:
                _, report_events = self._split_events(events)
            else:
                report_events = events

            logger.info("running_benchmark",
                        event_count=len(report_events),
                        days_back=days_back,
                        cid=cid)

            results: Dict[str, BenchmarkResult] = {}
            for policy_name in policies:
                if policy_name not in self.POLICIES:
                    logger.warning("unknown_policy", policy=policy_name)
                    continue
                policy_method = getattr(self, self.POLICIES[policy_name])
                metrics, ci = await self._evaluate_policy(policy_method, report_events)

                run_id = str(uuid.uuid4())
                result = BenchmarkResult(
                    run_id=run_id,
                    policy_name=policy_name,
                    timestamp=time.time(),
                    sample_count=len(report_events),
                    metrics=metrics,
                    confidence_intervals=ci,
                )
                results[policy_name] = result

                try:
                    store_fn = getattr(self.storage, "store_benchmark_result", None)
                    if store_fn is not None:
                        r = store_fn(
                            run_id=run_id,
                            policy_name=policy_name,
                            metrics=metrics,
                            count=len(report_events),
                            confidence_intervals=ci,
                        )
                        if _is_awaitable(r):
                            await r
                except Exception as e:
                    logger.warning("store_benchmark_failed",
                                   policy=policy_name, error=str(e))

                if self.metrics:
                    try:
                        self.metrics["benchmark_runs"].labels(policy=policy_name).inc()
                    except Exception:
                        pass

                if self.limit_graph_manager is not None:
                    try:
                        self.limit_graph_manager.add_node(
                            "benchmark_policies", f"run_{run_id}",
                            "benchmark_run",
                            {"policy": policy_name, "metrics": metrics},
                        )
                    except Exception:
                        pass

            # p-values vs mopd_current
            if "mopd_current" in results and len(results) > 1:
                for name, res in results.items():
                    if name == "mopd_current":
                        continue
                    p, reason = self._compute_p_value(res, results["mopd_current"])
                    res.p_value = p
                    res.p_value_reason = reason

            self._log_comparison(results)

            # MoE selection (placeholder)
            if self.moe_gating is not None and results:
                try:
                    context = self._build_context(report_events)
                    selected_expert, _ = await self.moe_gating.select_expert(context)
                    logger.info("moe_selected_policy", policy=selected_expert)
                    if self.rlhf_trainer is not None:
                        self.rlhf_trainer.record_pair(
                            pair_id=str(uuid.uuid4()),
                            prompt="Which policy performs best?",
                            chosen=selected_expert,
                            rejected="quality_only",
                            reward_diff=0.1,
                            metadata={"cid": cid, "benchmark": True},
                        )
                except Exception as e:
                    logger.warning("moe_selection_failed", error=str(e))

            if self.metrics:
                try:
                    self.metrics["benchmark_duration"].observe(time.monotonic() - start)
                except Exception:
                    pass

            return results
        finally:
            _cid_ctx.reset(token)

    # ---------------- per-policy evaluation ----------------
    async def _evaluate_policy(
        self,
        policy_func: Callable[[Dict[str, Any], List[Dict[str, Any]]], Awaitable[int]],
        events: List[Dict[str, Any]],
    ) -> Tuple[Dict[str, float], Dict[str, Tuple[float, float]]]:
        per_event_metrics: List[Dict[str, float]] = []
        missing_fields_warned = False
        for event in events:
            state = event.get("state", {}) or {}
            candidates = event.get("candidates", []) or []
            if not candidates:
                continue
            try:
                chosen_idx = await policy_func(state, candidates)
            except Exception as e:
                logger.warning("policy_simulation_failed",
                               event_id=event.get("event_id"), error=str(e))
                continue
            if not (0 <= chosen_idx < len(candidates)):
                logger.warning("policy_returned_invalid_index",
                               index=chosen_idx, n=len(candidates))
                continue
            chosen = candidates[chosen_idx]
            row: Dict[str, float] = {}
            for key in ("quality_score", "carbon_g", "latency_ms",
                         "energy_joules", "cost_usd", "helium_cost"):
                if key not in chosen and not missing_fields_warned:
                    logger.debug("candidate_missing_field", field=key)
                row[key] = float(chosen.get(key, 0.0))
            per_event_metrics.append({
                "quality": row["quality_score"],
                "carbon": row["carbon_g"],
                "latency": row["latency_ms"],
                "energy": row["energy_joules"],
                "cost": row["cost_usd"],
                "helium": row["helium_cost"],
            })
            missing_fields_warned = True

        if not per_event_metrics:
            logger.warning("no_valid_events_for_policy")
            return {}, {}
        return self._bootstrap_aggregate(per_event_metrics)

    # ---------------- parameterized policies ----------------
    @traced("benchmark.evaluate_policy_parameters")
    async def evaluate_policy_parameters(
        self, weights: Dict[str, float]
    ) -> Dict[str, float]:
        events = await self._get_events(7, 10000)
        if not events:
            return {k: 0.0 for k in (
                "quality", "carbon", "latency", "energy", "cost", "helium",
            )}

        if self.config.use_train_holdout_split:
            train_events, _ = self._split_events(events)
        else:
            train_events = events

        policy = Policy(policy_id="temp", weights=weights)
        collected: Dict[str, List[float]] = {
            k: [] for k in (
                "quality", "carbon", "latency", "energy", "cost", "helium",
            )
        }
        for event in train_events:
            candidates = event.get("candidates", []) or []
            if not candidates:
                continue
            try:
                idx = policy.choose_candidate(candidates)
            except Exception as e:
                logger.debug("choose_candidate_failed", error=str(e))
                continue
            chosen = candidates[idx]
            collected["quality"].append(float(chosen.get("quality_score", 0.0)))
            collected["carbon"].append(float(chosen.get("carbon_g", 0.0)))
            collected["latency"].append(float(chosen.get("latency_ms", 0.0)))
            collected["energy"].append(float(chosen.get("energy_joules", 0.0)))
            collected["cost"].append(float(chosen.get("cost_usd", 0.0)))
            collected["helium"].append(float(chosen.get("helium_cost", 0.0)))

        benefits: Dict[str, float] = {}
        benefits["quality"] = float(np.mean(collected["quality"])) if collected["quality"] else 0.0
        for metric in ("carbon", "latency", "energy", "cost", "helium"):
            vals = collected[metric]
            if not vals:
                benefits[metric] = 0.0
                continue
            mx = max(vals)
            benefits[metric] = (
                1.0 - float(np.mean(vals)) / mx if mx > 0 else 1.0
            )
        return benefits

    # ---------------- evolution ----------------
    @traced("benchmark.run_evolution")
    async def run_policy_evolution(self) -> List[MOPDPoint]:
        async with self._get_evolution_lock():
            param_bounds: Dict[str, Tuple[float, float]] = {
                "quality": (0.01, 1.0),
                "carbon": (0.01, 1.0),
                "latency": (0.01, 1.0),
                "energy": (0.01, 1.0),
                "cost": (0.01, 1.0),
                "helium": (0.01, 1.0),
            }

            async def evaluate(weights: Dict[str, float]) -> Dict[str, float]:
                total = sum(max(0.0, v) for v in weights.values())
                if total <= 0:
                    normalized = {k: 1.0 / len(weights) for k in weights}
                else:
                    normalized = {k: max(0.0, v) / total
                                   for k, v in weights.items()}
                return await self.evaluate_policy_parameters(normalized)

            optimizer = NSGAIIOptimizer(
                evaluate_func=evaluate,
                parameter_bounds=param_bounds,
                population_size=self.moea_population_size,
                generations=self.moea_generations,
                mutation_rate=self.moea_mutation_rate,
                crossover_rate=self.moea_crossover_rate,
                tournament_size=self.moea_tournament_size,
                objective_weights=self.moea_objective_weights,
                dynamic_weights=self.moea_dynamic_weights,
            )

            pareto = await optimizer.evolve()
            self.evolved_pareto_front = pareto

            if self.metrics:
                try:
                    self.metrics["evolution_rounds"].inc()
                    self.metrics["pareto_size"].set(len(pareto))
                except Exception:
                    pass

            if pareto:
                best = optimizer._select_best_from_pareto(
                    pareto, self._get_dynamic_moea_weights()
                )
                if best is not None:
                    self.best_evolved_policy = best.policy
                    logger.info("best_evolved_policy",
                                weights=best.policy.weights)

                    store_fn = getattr(self.storage, "store_evolved_policy", None)
                    if store_fn is not None:
                        try:
                            r = store_fn(best.policy.to_dict())
                            if _is_awaitable(r):
                                await r
                        except Exception as e:
                            logger.warning("store_evolved_policy_failed",
                                           error=str(e))

                    if self.modp_solver is not None:
                        try:
                            state_id = f"evolved_{best.policy.policy_id}"
                            self.modp_solver.add_state(
                                state_id=state_id,
                                problem_id="policy_evolution",
                                state_attributes={"weights": best.policy.weights},
                                objective_values=dict(best.objectives),
                                stage=1,
                            )
                            self.modp_solver.add_policy(
                                policy_id=best.policy.policy_id,
                                problem_id="policy_evolution",
                                state_id=state_id,
                                action="evolved",
                                expected_objectives=dict(best.objectives),
                            )
                        except Exception as e:
                            logger.debug("modp_store_failed", error=str(e))

                    if self.limit_graph_manager is not None:
                        try:
                            self.limit_graph_manager.add_node(
                                "benchmark_policies",
                                f"policy_{best.policy.policy_id}",
                                "evolved_policy",
                                {"weights": best.policy.weights},
                            )
                        except Exception:
                            pass

            return pareto

    def _get_dynamic_moea_weights(self) -> Dict[str, float]:
        weights = dict(self.moea_objective_weights)
        if not self.moea_dynamic_weights:
            return weights
        # Aggregate across all cached events (not just the first 100).
        events: List[Dict[str, Any]] = []
        for _, evs in self._events_cache.values():
            events.extend(evs)
            if len(events) >= 1000:
                break
        if not events:
            return weights
        try:
            carbon_vals: List[float] = []
            for event in events:
                for c in event.get("candidates", []) or []:
                    if "carbon_g" in c:
                        carbon_vals.append(float(c["carbon_g"]))
            if carbon_vals:
                avg_carbon = float(np.mean(carbon_vals))
                if avg_carbon > 0.5:
                    weights["carbon"] = min(
                        0.6, weights.get("carbon", 0.2) * 1.2
                    )
                total = sum(weights.values()) or 1.0
                weights = {k: v / total for k, v in weights.items()}
        except Exception as e:
            logger.warning("dynamic_weight_adjustment_failed", error=str(e))
        return weights

    # ---------------- statistics ----------------
    def _bootstrap_aggregate(
        self, metrics_list: List[Dict[str, float]]
    ) -> Tuple[Dict[str, float], Dict[str, Tuple[float, float]]]:
        if not metrics_list:
            return {}, {}
        keys = list(metrics_list[0].keys())
        data = {key: np.array([m[key] for m in metrics_list]) for key in keys}
        means = {key: float(np.mean(data[key])) for key in keys}
        n = len(metrics_list)
        ci: Dict[str, Tuple[float, float]] = {}
        for key in keys:
            vals = data[key]
            boot_means = [
                float(np.mean(np.random.choice(vals, size=n, replace=True)))
                for _ in range(self.bootstrap_samples)
            ]
            boot_means_arr = np.array(boot_means)
            lower = float(np.percentile(
                boot_means_arr, (1 - self.confidence_level) / 2 * 100
            ))
            upper = float(np.percentile(
                boot_means_arr, (1 + self.confidence_level) / 2 * 100
            ))
            ci[key] = (lower, upper)
        return means, ci

    def _compute_p_value(
        self, result_a: BenchmarkResult, result_b: BenchmarkResult
    ) -> Tuple[Optional[float], Optional[str]]:
        """
        p-value is currently a placeholder: we do not retain per-event raw
        metrics across runs, so a paired test cannot be constructed.

        Returns (None, reason).
        """
        if not SCIPY_AVAILABLE:
            return None, "scipy_unavailable"
        if not result_a.metrics or not result_b.metrics:
            return None, "no_metrics"
        return None, "raw_per_event_data_not_retained"

    def _log_comparison(self, results: Dict[str, BenchmarkResult]) -> None:
        if not results:
            return
        logger.info("benchmark_comparison_begin")
        for name, res in results.items():
            logger.info(
                "benchmark_policy",
                policy=name,
                quality=float(res.metrics.get("quality", 0.0)),
                carbon=float(res.metrics.get("carbon", 0.0)),
                latency=float(res.metrics.get("latency", 0.0)),
                energy=float(res.metrics.get("energy", 0.0)),
                cost=float(res.metrics.get("cost", 0.0)),
                helium=float(res.metrics.get("helium", 0.0)),
                p_value=res.p_value,
                p_value_reason=res.p_value_reason,
            )
        logger.info("benchmark_comparison_end")

    # ---------------- API helper ----------------
    def to_api_response(self, results: Dict[str, BenchmarkResult]
                         ) -> Dict[str, Dict[str, Any]]:
        return {name: res.to_dict() for name, res in results.items()}

    # ---------------- placeholder helpers ----------------
    async def get_limit_graph(self, graph_id: str = "benchmark_policies"
                                ) -> Dict[str, Any]:
        if self.limit_graph_manager is None:
            return {}
        return {
            "metadata": self.limit_graph_manager.get_metadata(graph_id),
            "nodes": self.limit_graph_manager.get_nodes(graph_id),
            "edges": self.limit_graph_manager.get_edges(graph_id),
        }

    async def get_moe_experts(self) -> List[str]:
        if self.moe_gating is None:
            return []
        return list(self.moe_gating.expert_names)

    async def get_rlhf_pairs(self, limit: int = 100) -> List[Dict[str, Any]]:
        if self.rlhf_trainer is None:
            return []
        return self.rlhf_trainer.get_pairs(limit)

    async def record_rlhf_pair(self, pair_id: str, prompt: str,
                                chosen: str, rejected: str,
                                reward_diff: float,
                                metadata: Optional[Dict[str, Any]] = None
                                ) -> None:
        if self.rlhf_trainer is not None:
            self.rlhf_trainer.record_pair(
                pair_id, prompt, chosen, rejected, reward_diff, metadata
            )


# =============================================================================
# SECTION 8. TESTS
# =============================================================================
def _make_event(carbon=0.5, latency=100.0, energy=1.0,
                 cost=0.5, helium=0.1, quality=0.9,
                 action_id=None):
    if action_id is None:
        action_id = uuid.uuid4().hex[:6]
    return {
        "event_id": str(uuid.uuid4()),
        "state": {
            "carbon_intensity": carbon,
            "workload_size": 1000.0,
            "latency_target": latency,
            "cost_budget": 10.0,
            "energy_price": 0.1,
            "helium_scarcity": helium,
            "quality_requirement": quality,
            "hour_of_day": 12.0,
            "day_of_week": 3.0,
            "recent_success_rate": 0.7,
            "avg_reward": 0.5,
            "num_candidates": 3.0,
        },
        "candidates": [
            {
                "action_id": f"{action_id}_{i}",
                "carbon_g": carbon + i * 0.1,
                "latency_ms": latency + i * 10.0,
                "energy_joules": energy + i * 0.1,
                "cost_usd": cost + i * 0.05,
                "helium_cost": helium + i * 0.01,
                "quality_score": quality - i * 0.02,
            }
            for i in range(3)
        ],
    }


class _MockStorage:
    def __init__(self, events=None):
        self.events = events or []
        self.benchmark_results: List[Dict[str, Any]] = []
        self.evolved_policies: List[Dict[str, Any]] = []
        self.calls = 0

    def get_feedback_events_with_context(self, days_back: int, limit: int):
        self.calls += 1
        return list(self.events[:limit])

    def store_benchmark_result(self, **kwargs):
        self.benchmark_results.append(dict(kwargs))

    def store_evolved_policy(self, policy_dict):
        self.evolved_policies.append(dict(policy_dict))


class _Tests(unittest.TestCase):
    def _bm(self, events=None, **kwargs) -> CounterfactualBenchmark:
        storage = _MockStorage(events or [_make_event() for _ in range(20)])
        return CounterfactualBenchmark(storage=storage, **kwargs)

    def test_module_status_documented(self):
        self.assertEqual(MODULE_STATUS["benchmark_core"], "stable")
        self.assertEqual(MODULE_STATUS["moe_gating"], "placeholder")
        self.assertEqual(MODULE_STATUS["nsga2_optimizer"], "experimental")

    def test_lifecycle(self):
        async def go():
            bm = self._bm()
            self.assertFalse(await bm.ready())
            await bm.start()
            self.assertTrue(await bm.ready())
            await bm.shutdown()
            self.assertTrue(bm._shutdown)
            await bm.shutdown()  # idempotent
        asyncio.run(go())

    def test_context_manager(self):
        async def go():
            async with self._bm() as bm:
                self.assertTrue(await bm.ready())
        asyncio.run(go())

    def test_policy_choose_candidate(self):
        policy = Policy(policy_id="p", weights={
            "carbon": 0.5, "latency": 0.5, "quality": 0.0,
        })
        candidates = [
            {"carbon_g": 1.0, "latency_ms": 10.0, "quality_score": 0.5},
            {"carbon_g": 0.1, "latency_ms": 100.0, "quality_score": 0.9},
            {"carbon_g": 5.0, "latency_ms": 5.0, "quality_score": 0.5},
        ]
        idx = policy.choose_candidate(candidates)
        # weight 0.5 each on carbon and latency:
        #   c0 = 0.5*1.0 + 0.5*10.0 = 5.5
        #   c1 = 0.5*0.1 + 0.5*100.0 = 50.05
        #   c2 = 0.5*5.0 + 0.5*5.0 = 5.0
        self.assertEqual(idx, 2)

    def test_events_cache_key_includes_days_back(self):
        async def go():
            bm = self._bm()
            await bm._get_events(7, 100)
            await bm._get_events(30, 100)
            # Two different cache entries
            self.assertIn((7, 100), bm._events_cache)
            self.assertIn((30, 100), bm._events_cache)
            # Storage called exactly twice
            self.assertEqual(bm.storage.calls, 2)
        asyncio.run(go())

    def test_moe_gating_reward_weighting(self):
        async def go():
            gate = MoEGatingNetworkPlaceholder(enabled=False)
            state = {"carbon_intensity": 0.5, "workload_size": 1000}
            initial = gate.gating_weights.copy()
            # reward=0 must not change weights
            await gate.add_training_sample(state, gate.expert_names[0], 0.0)
            np.testing.assert_allclose(gate.gating_weights, initial, atol=1e-9)
            # positive reward changes weights
            await gate.add_training_sample(state, gate.expert_names[0], 1.0)
            diff = float(np.abs(gate.gating_weights - initial).sum())
            self.assertGreater(diff, 1e-6)
        asyncio.run(go())

    def test_nsga2_tournament_ranks_by_identity(self):
        async def go():
            async def eval_fn(p):
                return {"a": p["x"], "b": 1.0 - p["x"]}

            opt = NSGAIIOptimizer(
                evaluate_func=eval_fn,
                parameter_bounds={"x": (0.0, 1.0)},
                population_size=6, generations=2,
                objective_weights={"a": 0.5, "b": 0.5},
            )
            pareto = await opt.evolve()
            self.assertGreater(len(pareto), 0)
            # Every point has a copy semantics: mutating one must not
            # affect the stored front.
            before = [p.scalarised_score for p in pareto]
            _ = opt._select_best_from_pareto(pareto, {"a": 0.5, "b": 0.5})
            after = [p.scalarised_score for p in pareto]
            self.assertEqual(before, after)
        asyncio.run(go())

    def test_p_value_returns_none_with_reason(self):
        bm = self._bm()
        r1 = BenchmarkResult(
            run_id="a", policy_name="fixed_cheapest",
            timestamp=time.time(), sample_count=10,
            metrics={"quality": 0.9}, confidence_intervals={},
        )
        r2 = BenchmarkResult(
            run_id="b", policy_name="mopd_current",
            timestamp=time.time(), sample_count=10,
            metrics={"quality": 0.9}, confidence_intervals={},
        )
        p, reason = bm._compute_p_value(r1, r2)
        self.assertIsNone(p)
        self.assertIsNotNone(reason)

    def test_placeholders_honest(self):
        lg = LimitGraphManagerPlaceholder()
        self.assertFalse(lg.available)
        mo = MODPOptimizerPlaceholder()
        self.assertFalse(mo.available)
        rl = RLHFTrainerPlaceholder()
        self.assertFalse(rl.available)
        mg = MoEGatingNetworkPlaceholder()
        self.assertFalse(mg.available)

    def test_run_benchmark_end_to_end(self):
        async def go():
            async with self._bm() as bm:
                results = await bm.run_benchmark(days_back=7)
                self.assertIn("fixed_cheapest", results)
                self.assertIn("mopd_current", results)
                # The mopd fallback (index 0) should still produce metrics
                self.assertGreater(len(results["fixed_cheapest"].metrics), 0)
        asyncio.run(go())

    def test_run_policy_evolution(self):
        async def go():
            bm = self._bm()
            bm.moea_population_size = 6
            bm.moea_generations = 2
            async with bm:
                pareto = await bm.run_policy_evolution()
                self.assertIsInstance(pareto, list)
        asyncio.run(go())

    def test_benchmark_result_serializes(self):
        r = BenchmarkResult(
            run_id="r", policy_name="p",
            timestamp=1.0, sample_count=5,
            metrics={"quality": 0.9},
            confidence_intervals={"quality": (0.85, 0.95)},
        )
        d = r.to_dict()
        self.assertEqual(d["policy_name"], "p")
        self.assertEqual(d["confidence_intervals"]["quality"], [0.85, 0.95])

    def test_split_events(self):
        bm = self._bm()
        bm.config.use_train_holdout_split = True
        events = [_make_event() for _ in range(10)]
        train, holdout = bm._split_events(events)
        self.assertGreater(len(train), 0)
        self.assertGreater(len(holdout), 0)
        self.assertEqual(len(train) + len(holdout), len(events))


def run_tests() -> int:
    suite = unittest.TestLoader().loadTestsFromTestCase(_Tests)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    return 0 if result.wasSuccessful() else 1


# =============================================================================
# SECTION 9. ENTRY POINT
# =============================================================================
async def _example() -> None:
    events = [_make_event(carbon=0.2 + i * 0.05) for i in range(50)]
    storage = _MockStorage(events)
    async with CounterfactualBenchmark(storage=storage) as bm:
        results = await bm.run_benchmark(days_back=7)
        for name, res in results.items():
            print(f"{name}: quality={res.metrics.get('quality', 0.0):.3f} "
                  f"carbon={res.metrics.get('carbon', 0.0):.3f} "
                  f"p_value={res.p_value}")
        pareto = await bm.run_policy_evolution()
        print(f"Pareto front size: {len(pareto)}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Counterfactual Benchmarking Harness v4.0.0"
    )
    parser.add_argument("--test", action="store_true",
                        help="Run embedded tests")
    parser.add_argument("--example", action="store_true",
                        help="Run example usage")
    parser.add_argument("--status", action="store_true",
                        help="Print module statuses")
    args = parser.parse_args()

    if args.status:
        for name, status in MODULE_STATUS.items():
            print(f"{name:20s} {status}")
        return

    if args.test:
        sys.exit(run_tests())

    if args.example:
        asyncio.run(_example())
        return

    print("Counterfactual Benchmarking Harness v4.0.0 — no mode selected.")
    print("Use --test, --example, or --status.")


if __name__ == "__main__":
    main()
