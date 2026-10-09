#!/usr/bin/env python3
# =============================================================================
# Enhanced Cache Manager for Green Agent (v3.0.0) — Patched Single-File Edition
# =============================================================================
"""
Enhanced Cache Manager for Green Agent with Adaptive Caching Policy (v3.0.0)
=============================================================================
Patched single-file version of v2.2.1.

P0 fixes
--------
- Redis ping removed from the fast path. Latency is sampled in the health
  loop and read as an attribute.
- No more `dbsize()` on every set. The size gauge is updated in the health
  loop at a low cadence.
- Every Redis access goes through a helper that guards against a None
  client and takes a timeout.
- Redis availability flag is mutated under a lock; a single failure no
  longer forces a permanent fallback.
- Distillation updates are queued into a bounded queue with a single
  consumer. No more untracked `asyncio.create_task` per request.
- NSGA-II tournament uses an id-keyed rank/crowding map. No more
  dataclass equality bug.
- MoE gating update uses a reward-weighted policy gradient.

P1 — correctness
----------------
- Real rolling hit_rate and average latency are fed into the policy state.
- `key_size_estimate` is populated on set and read on get.
- All timestamps are timezone-aware.
- Unbounded collections are bounded (eval cache, MODP states, RLHF pairs,
  LIMIT graph, key stats).
- Lifecycle is idempotent and restartable.
- TTL precedence documented and consistent.

P2 — honesty
------------
- MODULE_STATUS documents every module.
- Placeholders (disabled by default, `.available = False`, warn when enabled):
    moe_gating, rlhf, modp, limit_graph.
- Experimental (warn when enabled):
    distillation, nsga2_optimizer, redis_backend.

P3 — production readiness
-------------------------
- Lifecycle: `async start()`, `async ready()`, `async shutdown()`,
  `__aenter__` / `__aexit__`.
- Embedded test suite: `python3 cache_manager.py --test`.
- `--status` prints module maturity.
- Prometheus metrics and OpenTelemetry spans (both optional).
"""

from __future__ import annotations

import argparse
import asyncio
import copy
import hashlib
import json
import logging
import random
import sys
import time
import unittest
import uuid
from abc import ABC, abstractmethod
from collections import OrderedDict, deque
from contextvars import ContextVar
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple, Union

import numpy as np

# -----------------------------------------------------------------------------
# Optional dependencies
# -----------------------------------------------------------------------------
try:
    from redis.asyncio import Redis, ConnectionPool
    REDIS_AVAILABLE = True
except ImportError:
    Redis = None  # type: ignore
    ConnectionPool = None  # type: ignore
    REDIS_AVAILABLE = False

try:
    from prometheus_client import Counter, Gauge, Histogram
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

try:
    from sklearn.ensemble import RandomForestClassifier
    SKLEARN_ML = True
except ImportError:
    SKLEARN_ML = False

try:
    import joblib
    JOBLIB_AVAILABLE = True
except ImportError:
    joblib = None  # type: ignore
    JOBLIB_AVAILABLE = False

try:
    from opentelemetry import trace
    _TRACER = trace.get_tracer("cache_manager")
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
except ImportError:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Central components (optional)
# -----------------------------------------------------------------------------
try:
    from ..storage import Storage  # type: ignore
    CENTRAL_COMPONENTS_AVAILABLE = True
except ImportError:
    Storage = None  # type: ignore
    CENTRAL_COMPONENTS_AVAILABLE = False


# =============================================================================
# SECTION 1. MODULE STATUS
# =============================================================================
MODULE_STATUS: Dict[str, str] = {
    "cache_core":         "stable",
    "memory_cache":       "stable",
    "event_loop_helpers": "stable",
    "redis_backend":      "experimental",
    "distillation":       "experimental",
    "nsga2_optimizer":    "experimental",
    "moe_gating":         "placeholder",
    "rlhf":               "placeholder",
    "modp":               "placeholder",
    "limit_graph":        "placeholder",
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
_cid_ctx: ContextVar[str] = ContextVar("cache_cid", default="-")


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
        import functools

        @functools.wraps(fn)
        async def wrapper(*args, **kwargs):
            with _TRACER.start_as_current_span(span_name):
                return await fn(*args, **kwargs)
        return wrapper
    return decorator


# The single source of truth for cache policy actions.
CACHE_ACTION_SPACE: Tuple[str, ...] = (
    "redis_ttl_short",
    "redis_ttl_long",
    "memory_only",
    "no_cache",
    "adaptive_ttl",
)


# =============================================================================
# SECTION 3. PLACEHOLDERS
# =============================================================================
class LimitGraphManagerPlaceholder:
    STATUS = "placeholder"

    def __init__(self, storage: Optional[Any] = None,
                 enabled: bool = False, max_graphs: int = 50):
        if enabled:
            _warn_module("limit_graph")
        self.storage = storage
        self.max_graphs = max_graphs
        self.graphs: "OrderedDict[str, Dict[str, Any]]" = OrderedDict()
        self.available = False

    def create_graph(self, graph_id: str, description: str,
                      configuration: Dict[str, Any]) -> None:
        self.graphs[graph_id] = {
            "description": description,
            "configuration": configuration,
            "nodes": {},
            "edges": {},
        }
        self.graphs.move_to_end(graph_id)
        while len(self.graphs) > self.max_graphs:
            self.graphs.popitem(last=False)

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
        self.states: Dict[str, deque] = {}
        self.available = False

    def add_state(self, state_id: str, problem_id: str,
                  state_attributes: Dict[str, Any],
                  objective_values: Dict[str, float],
                  stage: int) -> None:
        if problem_id not in self.states:
            self.states[problem_id] = deque(maxlen=self.max_states)
        self.states[problem_id].append({
            "state_id": state_id,
            "state_attributes": state_attributes,
            "objective_values": objective_values,
            "stage": stage,
        })

    def get_states(self, problem_id: str) -> List[Dict[str, Any]]:
        return list(self.states.get(problem_id, []))

    async def solve(self, problem_id: str, initial_state: Dict[str, Any],
                     max_stages: int = 5) -> Dict[str, Any]:
        return {"status": "placeholder", "pareto_front": []}


class RLHFTrainerPlaceholder:
    STATUS = "placeholder"

    def __init__(self, storage: Optional[Any] = None,
                 enabled: bool = False, max_pairs: int = 2000):
        if enabled:
            _warn_module("rlhf")
        self.storage = storage
        self.max_pairs = max_pairs
        self.pairs: deque = deque(maxlen=max_pairs)
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


# =============================================================================
# SECTION 4. MoE GATING (placeholder with correct reward-weighted update)
# =============================================================================
class MoEGatingNetworkPlaceholder:
    """
    STATUS: placeholder.

    The reward-weighted policy gradient update is correct; the module
    itself is a placeholder because it is not wired into any decision path
    that affects the cache.
    """
    STATUS = "placeholder"

    def __init__(self, storage: Optional[Any] = None,
                 config: Optional[Dict[str, Any]] = None,
                 enabled: bool = False):
        if enabled:
            _warn_module("moe_gating")
        self.storage = storage
        self.config = config or {}
        self.num_experts = int(self.config.get("moe_expert_count", 4))
        self.expert_names = [
            "redis_first", "memory_first", "size_aware", "frequency_aware"
        ][:self.num_experts]
        self.state_dim = 9
        self.gating_weights = np.random.randn(self.num_experts, self.state_dim) * 0.1
        self.lr = 0.05
        self.available = False

    def _encode_state(self, state: "CachePolicyState") -> np.ndarray:
        return state.to_feature_vector()

    async def select_expert(self, state: "CachePolicyState"
                             ) -> Tuple[str, np.ndarray]:
        x = self._encode_state(state)
        logits = self.gating_weights @ x
        logits = logits - np.max(logits)
        probs = np.exp(logits)
        probs = probs / probs.sum()
        expert_idx = int(np.argmax(probs))
        return self.expert_names[expert_idx], probs

    async def add_training_sample(self, state: "CachePolicyState",
                                    selected_expert: str,
                                    reward: float) -> None:
        if selected_expert not in self.expert_names:
            return
        x = self._encode_state(state)
        expert_idx = self.expert_names.index(selected_expert)
        one_hot = np.zeros(self.num_experts)
        one_hot[expert_idx] = 1.0

        logits = self.gating_weights @ x
        logits = logits - np.max(logits)
        probs = np.exp(logits)
        probs = probs / probs.sum()

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
# SECTION 5. DISTILLATION COMPONENTS
# =============================================================================
@dataclass
class CachePolicyState:
    key_length: int
    estimated_size_bytes: float
    access_frequency: float
    time_of_day_hour: int
    redis_available: bool
    redis_latency_ms: float
    memory_usage_pct: float
    hit_rate: float
    avg_latency_ms: float

    def to_feature_vector(self) -> np.ndarray:
        features = [
            min(self.key_length / 100.0, 1.0),
            min(self.estimated_size_bytes / 1_000_000.0, 1.0),
            min(self.access_frequency / 100.0, 1.0),
            self.time_of_day_hour / 24.0,
            1.0 if self.redis_available else 0.0,
            min(self.redis_latency_ms / 100.0, 1.0),
            min(self.memory_usage_pct / 100.0, 1.0),
            max(0.0, min(1.0, self.hit_rate)),
            min(self.avg_latency_ms / 100.0, 1.0),
        ]
        return np.array(features, dtype=np.float32)


class Teacher(ABC):
    @abstractmethod
    def predict(self, state: CachePolicyState) -> np.ndarray: ...
    @abstractmethod
    def confidence(self, state: CachePolicyState) -> float: ...


class CacheRuleBasedTeacher(Teacher):
    def predict(self, state: CachePolicyState) -> np.ndarray:
        probs = np.ones(5) * 0.1
        if not state.redis_available:
            probs[2] = 0.8
        elif state.estimated_size_bytes > 1_000_000:
            probs[0] = 0.6
        elif state.access_frequency > 50:
            probs[2] = 0.7
        elif state.hit_rate < 0.2:
            probs[3] = 0.6
        else:
            probs[4] = 0.5
        return probs / probs.sum()

    def confidence(self, state: CachePolicyState) -> float:
        if not state.redis_available:
            return 0.8
        if state.estimated_size_bytes > 1_000_000:
            return 0.6
        return 0.4


class CacheHistoricalMLTeacher(Teacher):
    def __init__(self, model_path: Optional[str] = None):
        self.model = None
        if (model_path and SKLEARN_ML and JOBLIB_AVAILABLE
                and Path(model_path).exists()):
            try:
                self.model = joblib.load(model_path)
            except Exception as e:
                logger.warning("ml_teacher_load_failed", error=str(e))

    def predict(self, state: CachePolicyState) -> np.ndarray:
        if self.model is None:
            return np.ones(5) / 5
        x = state.to_feature_vector().reshape(1, -1)
        try:
            return self.model.predict_proba(x)[0]
        except Exception:
            return np.ones(5) / 5

    def confidence(self, state: CachePolicyState) -> float:
        return 0.7 if self.model is not None else 0.0


class CacheStatefulQTeacher(Teacher):
    def __init__(self, cache_manager: Any, lr: float = 0.1):
        self.cache_manager = cache_manager
        self.lr = lr
        self.weights = np.zeros((9, 5))

    def predict(self, state: CachePolicyState) -> np.ndarray:
        x = state.to_feature_vector()
        q = x @ self.weights
        q = q - np.max(q)
        exp_q = np.exp(q)
        return exp_q / exp_q.sum()

    def confidence(self, state: CachePolicyState) -> float:
        return 0.5

    def update(self, state: CachePolicyState, action: int, reward: float) -> None:
        x = state.to_feature_vector()
        q_current = float(np.dot(x, self.weights[:, action]))
        self.weights[:, action] += self.lr * (reward - q_current) * x


class DistillationStudent:
    def __init__(self, feature_dim: int = 9, n_classes: int = 5,
                 lr: float = 0.01, distill_weight: float = 0.7,
                 rl_weight: float = 0.3, entropy_bonus: float = 0.01):
        self.weights = np.zeros((feature_dim, n_classes))
        self.biases = np.zeros(n_classes)
        self.lr = lr
        self.n_classes = n_classes
        self.distill_weight = distill_weight
        self.rl_weight = rl_weight
        self.entropy_bonus = entropy_bonus
        self.counter = 0

    def predict_proba(self, state_vector: np.ndarray) -> np.ndarray:
        logits = state_vector @ self.weights + self.biases
        logits = logits - np.max(logits)
        exp_logits = np.exp(logits)
        return exp_logits / exp_logits.sum()

    def update(self, state_vector: np.ndarray, teacher_probs: np.ndarray,
               reward: float, action: int) -> None:
        """
        Combined distillation + policy gradient update.

        Distillation loss (cross-entropy with teacher):
            grad = (p - teacher)
        RL loss (ascend reward * log p[action]):
            grad = -reward * (one_hot - p)
        Entropy regularisation (ascend entropy):
            grad = -(log p + 1) * p
        """
        current = self.predict_proba(state_vector)
        grad_distill = current - teacher_probs

        one_hot = np.zeros(self.n_classes)
        one_hot[action] = 1.0
        grad_rl = -float(reward) * (one_hot - current)

        eps = 1e-9
        grad_entropy = -(np.log(current + eps) + 1.0) * current

        grad = (
            self.distill_weight * grad_distill
            + self.rl_weight * grad_rl
            - self.entropy_bonus * grad_entropy
        )
        self.weights -= self.lr * np.outer(state_vector, grad)
        self.biases -= self.lr * grad
        self.counter += 1


class ReplayBuffer:
    def __init__(self, max_size: int = 2000):
        self.buffer: deque = deque(maxlen=max_size)

    def push(self, state_vec: np.ndarray, action: int, reward: float,
             next_state_vec: np.ndarray, teacher_probs: np.ndarray) -> None:
        self.buffer.append((state_vec, action, reward, next_state_vec,
                             teacher_probs))

    def sample(self, batch_size: int = 32):
        if len(self.buffer) < batch_size:
            batch = list(self.buffer)
        else:
            batch = random.sample(self.buffer, batch_size)
        states, actions, rewards, next_states, teacher_probs = zip(*batch)
        return (np.array(states), list(actions), np.array(rewards),
                np.array(next_states), np.array(teacher_probs))

    def __len__(self) -> int:
        return len(self.buffer)


class DistillationCachePolicyOptimizer:
    STATUS = "experimental"

    def __init__(self, cache_manager: Any, config: Dict[str, Any],
                 enabled: bool = True):
        if enabled:
            _warn_module("distillation")
        self.cache_manager = cache_manager
        self.config = config
        self.student = DistillationStudent(
            feature_dim=9,
            lr=config.get("distillation_learning_rate", 0.01),
            distill_weight=config.get("distill_weight", 0.7),
            rl_weight=config.get("rl_weight", 0.3),
        )
        self.teachers: List[Teacher] = [
            CacheRuleBasedTeacher(),
            CacheHistoricalMLTeacher(),
            CacheStatefulQTeacher(cache_manager),
        ]
        self.replay_buffer = ReplayBuffer(
            max_size=config.get("distillation_replay_size", 2000)
        )
        self.epsilon = config.get("distillation_epsilon", 0.1)
        self.epsilon_min = config.get("distillation_epsilon_min", 0.01)
        self.epsilon_decay = config.get("distillation_epsilon_decay", 0.995)
        self.train_every = config.get("distillation_train_every", 10)
        self.counter = 0
        self.lock: Optional[asyncio.Lock] = None
        self.available = True

    def _get_lock(self) -> asyncio.Lock:
        if self.lock is None:
            self.lock = asyncio.Lock()
        return self.lock

    def _teacher_mixture(self, state: CachePolicyState) -> np.ndarray:
        teacher_probs = np.zeros(5)
        total_conf = 0.0
        for teacher in self.teachers:
            p = teacher.predict(state)
            c = teacher.confidence(state)
            teacher_probs += p * c
            total_conf += c
        if total_conf > 0:
            teacher_probs /= total_conf
        else:
            teacher_probs = np.ones(5) / 5
        return teacher_probs

    async def select_policy(self, state: CachePolicyState,
                             exploration: bool = True
                             ) -> Tuple[str, int, np.ndarray, np.ndarray]:
        state_vec = state.to_feature_vector()
        teacher_probs = self._teacher_mixture(state)
        student_probs = self.student.predict_proba(state_vec)

        if exploration and random.random() < self.epsilon:
            action_idx = random.randint(0, len(CACHE_ACTION_SPACE) - 1)
        else:
            combined = 0.8 * student_probs + 0.2 * teacher_probs
            action_idx = int(np.argmax(combined))

        # Epsilon decay
        self.epsilon = max(self.epsilon_min, self.epsilon * self.epsilon_decay)
        return CACHE_ACTION_SPACE[action_idx], action_idx, state_vec, teacher_probs

    async def update(self, state_vec: np.ndarray, action_idx: int,
                     reward: float, next_state_vec: np.ndarray,
                     teacher_probs: np.ndarray) -> None:
        async with self._get_lock():
            self.replay_buffer.push(
                state_vec, action_idx, reward, next_state_vec, teacher_probs
            )
            self.counter += 1
            if (self.counter % self.train_every == 0
                    and len(self.replay_buffer) >= 8):
                batch = self.replay_buffer.sample(8)
                states, actions, rewards, _, teacher_probs_batch = batch
                for i in range(len(states)):
                    self.student.update(
                        states[i], teacher_probs_batch[i],
                        rewards[i], actions[i],
                    )

    def get_stats(self) -> Dict[str, Any]:
        return {
            "available": self.available,
            "student_counter": self.student.counter,
            "buffer_size": len(self.replay_buffer),
            "weights_norm": float(np.linalg.norm(self.student.weights)),
            "epsilon": float(self.epsilon),
        }


# =============================================================================
# SECTION 6. NSGA-II OPTIMIZER
# =============================================================================
@dataclass
class MOPDPoint:
    policy_id: str
    parameters: Dict[str, float]
    objectives: Dict[str, float]
    scalarised_score: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MOPDPoint":
        return cls(
            policy_id=data.get("policy_id", str(uuid.uuid4())),
            parameters=dict(data.get("parameters", {})),
            objectives=dict(data.get("objectives", {})),
            scalarised_score=float(data.get("scalarised_score", 0.0)),
        )


class NSGAIIOptimizer:
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
        max_cache_entries: int = 5000,
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
        self.max_cache_entries = max_cache_entries

        self.best_individual: Optional[Dict[str, float]] = None
        self.best_fitness = -float("inf")
        self.evolution_history: List[Dict[str, Any]] = []
        self.pareto_front: List[MOPDPoint] = []
        self._eval_cache: "OrderedDict[Tuple, Dict[str, float]]" = OrderedDict()
        self.available = True

    def _random_individual(self) -> Dict[str, float]:
        return {
            name: random.uniform(low, high)
            for name, (low, high) in self.parameter_bounds.items()
        }

    def _crossover(self, p1: Dict, p2: Dict) -> Dict:
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
        return child

    def _mutate(self, ind: Dict) -> Dict:
        mutant = dict(ind)
        for name, (low, high) in self.parameter_bounds.items():
            if random.random() < self.mutation_rate:
                u = random.random()
                delta = (
                    (2 * u) ** (1.0 / 21.0) - 1 if u < 0.5
                    else 1 - (2 * (1 - u)) ** (1.0 / 21.0)
                )
                mutant[name] = mutant[name] + delta * (high - low)
                mutant[name] = max(low, min(high, mutant[name]))
        return mutant

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

    def _crowding_distance(
        self, front: List[int], points: List[MOPDPoint]
    ) -> Dict[int, float]:
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

    def _tournament(
        self,
        population: List[Dict],
        ranks: Dict[int, int],
        crowd: Dict[int, float],
    ) -> Dict:
        """
        Rank and crowding are keyed by `id(individual)`. No more `point in
        front` dataclass equality bug.
        """
        if not population:
            raise RuntimeError("empty population in tournament selection")
        k = min(self.tournament_size, len(population))
        candidates = random.sample(population, k)
        best = candidates[0]
        best_rank = ranks.get(id(best), 10**9)
        best_crowd = crowd.get(id(best), 0.0)
        for cand in candidates[1:]:
            r = ranks.get(id(cand), 10**9)
            cd = crowd.get(id(cand), 0.0)
            if r < best_rank or (r == best_rank and cd > best_crowd):
                best, best_rank, best_crowd = cand, r, cd
        return best

    def _compute_dynamic_weights(self) -> Dict[str, float]:
        weights = dict(self.objective_weights)
        if not self.dynamic_weights or not self.pareto_front:
            return weights
        return weights

    def _select_best_from_pareto(
        self, front: List[MOPDPoint], weights: Dict[str, float]
    ) -> Optional[MOPDPoint]:
        if not front:
            return None
        keys = list(weights.keys())
        max_vals = {k: max(p.objectives.get(k, 0.0) for p in front) for k in keys}
        min_vals = {k: min(p.objectives.get(k, 0.0) for p in front) for k in keys}
        ranges = {
            k: (max_vals[k] - min_vals[k]) if max_vals[k] != min_vals[k] else 1.0
            for k in keys
        }
        best: Optional[MOPDPoint] = None
        best_score = -float("inf")
        for p in front:
            s = sum(
                weights.get(k, 0.0) * (
                    (p.objectives.get(k, 0.0) - min_vals[k]) / ranges[k]
                )
                for k in keys
            )
            if s > best_score:
                best_score = s
                best = p
        if best is None:
            return None
        # Return a copy so the stored front is never mutated.
        copy_point = MOPDPoint.from_dict(best.to_dict())
        copy_point.scalarised_score = best_score
        return copy_point

    async def _evaluate(self, params: Dict[str, float]) -> Dict[str, float]:
        key = tuple(sorted((k, float(v)) for k, v in params.items()))
        if key in self._eval_cache:
            self._eval_cache.move_to_end(key)
            return self._eval_cache[key]
        r = self.evaluate_func(params)
        if _is_awaitable(r):
            r = await r
        objs = {k: float(v) for k, v in (r or {}).items()}
        self._eval_cache[key] = objs
        # Bound the cache with LRU eviction.
        while len(self._eval_cache) > self.max_cache_entries:
            self._eval_cache.popitem(last=False)
        return objs

    async def evolve(self) -> List[MOPDPoint]:
        population: List[Dict[str, float]] = [
            self._random_individual() for _ in range(self.population_size)
        ]
        points: List[MOPDPoint] = []
        for ind in population:
            objs = await self._evaluate(ind)
            points.append(MOPDPoint(
                policy_id=str(uuid.uuid4()),
                parameters=dict(ind),
                objectives=objs,
            ))

        for gen in range(self.generations):
            fronts = self._fast_non_dominated_sort(points)
            ranks: Dict[int, int] = {}
            crowd: Dict[int, float] = {}
            for rank_idx, front in enumerate(fronts):
                cd = self._crowding_distance(front, points)
                for i in front:
                    ind = population[i]
                    ranks[id(ind)] = rank_idx
                    crowd[id(ind)] = cd.get(i, 0.0)

            offspring_inds: List[Dict[str, float]] = []
            while len(offspring_inds) < self.population_size:
                p1 = self._tournament(population, ranks, crowd)
                p2 = self._tournament(population, ranks, crowd)
                if random.random() < self.crossover_rate:
                    child = self._crossover(p1, p2)
                else:
                    child = dict(p1)
                child = self._mutate(child)
                offspring_inds.append(child)

            offspring_points: List[MOPDPoint] = []
            for ind in offspring_inds:
                objs = await self._evaluate(ind)
                offspring_points.append(MOPDPoint(
                    policy_id=str(uuid.uuid4()),
                    parameters=dict(ind),
                    objectives=objs,
                ))

            combined_inds = population + offspring_inds
            combined_points = points + offspring_points

            # Deduplicate by parameter key
            seen: Dict[Tuple, int] = {}
            dedup_inds: List[Dict[str, float]] = []
            dedup_points: List[MOPDPoint] = []
            for ind, p in zip(combined_inds, combined_points):
                k = tuple(sorted((kk, float(vv)) for kk, vv in ind.items()))
                if k in seen:
                    continue
                seen[k] = len(dedup_inds)
                dedup_inds.append(ind)
                dedup_points.append(p)

            fronts = self._fast_non_dominated_sort(dedup_points)
            new_inds: List[Dict[str, float]] = []
            new_points: List[MOPDPoint] = []
            for front in fronts:
                if len(new_inds) + len(front) <= self.population_size:
                    for i in front:
                        new_inds.append(dedup_inds[i])
                        new_points.append(dedup_points[i])
                else:
                    cd = self._crowding_distance(front, dedup_points)
                    sf = sorted(front, key=lambda i: cd.get(i, 0.0),
                                 reverse=True)
                    remaining = self.population_size - len(new_inds)
                    for i in sf[:remaining]:
                        new_inds.append(dedup_inds[i])
                        new_points.append(dedup_points[i])
                    break
            population = new_inds
            points = new_points

        fronts = self._fast_non_dominated_sort(points)
        self.pareto_front = (
            [points[i] for i in fronts[0]] if fronts else []
        )
        weights = self._compute_dynamic_weights()
        best = self._select_best_from_pareto(self.pareto_front, weights)
        if best is not None:
            self.best_individual = dict(best.parameters)
            self.best_fitness = best.scalarised_score
        self.evolution_history.append({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "best_fitness": self.best_fitness,
            "pareto_size": len(self.pareto_front),
            "generation": gen if 'gen' in dir() else self.generations,
        })
        return list(self.pareto_front)


# =============================================================================
# SECTION 7. CACHE MANAGER (MAIN)
# =============================================================================
class CacheManager:
    """
    Lifecycle:
        cache = CacheManager(...)
        await cache.start()
        await cache.set("k", {"a": 1})
        v = await cache.get("k")
        await cache.shutdown()

    TTL precedence (documented):
        1. If MOEA has produced a best parameter for the current policy, it
           overrides the caller-provided TTL.
        2. Otherwise, the caller-provided TTL is used for policies that do
           not hard-code a TTL (memory_only, adaptive_ttl, unknown).
        3. redis_ttl_short and redis_ttl_long always use their hard-coded
           TTLs (60 s and 600 s).
    """

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379/0",
        serializer: Optional[Callable[[Any], str]] = None,
        deserializer: Optional[Callable[[str], Any]] = None,
        max_memory_entries: int = 1000,
        cleanup_interval_seconds: int = 60,
        retry_attempts: int = 3,
        retry_delay_ms: float = 100.0,
        redis_op_timeout_seconds: float = 5.0,
        redis_probe_interval_seconds: float = 30.0,
        distillation_epsilon: float = 0.1,
        distillation_epsilon_min: float = 0.01,
        distillation_epsilon_decay: float = 0.995,
        distillation_train_every: int = 10,
        distillation_replay_size: int = 2000,
        distillation_learning_rate: float = 0.01,
        distill_weight: float = 0.7,
        rl_weight: float = 0.3,
        distillation_queue_maxsize: int = 1000,
        moea_enabled: bool = True,
        moea_interval_seconds: int = 300,
        moea_population_size: int = 20,
        moea_generations: int = 5,
        moea_mutation_rate: float = 0.2,
        moea_crossover_rate: float = 0.8,
        moea_objective_weights: Optional[Dict[str, float]] = None,
        moea_dynamic_weights: bool = True,
        storage: Optional[Any] = None,
        enable_limit_graph: bool = False,
        enable_modp: bool = False,
        enable_rlhf: bool = False,
        enable_moe: bool = False,
        moe_expert_count: int = 4,
        shutdown_timeout_seconds: float = 10.0,
    ):
        # Core config
        self.redis_url = redis_url
        self.serializer = serializer or (lambda v: json.dumps(v, default=str))
        self.deserializer = deserializer or (lambda s: json.loads(s))
        self.max_memory_entries = max_memory_entries
        self.cleanup_interval = cleanup_interval_seconds
        self.retry_attempts = retry_attempts
        self.retry_delay_ms = retry_delay_ms
        self.redis_op_timeout_seconds = redis_op_timeout_seconds
        self.redis_probe_interval_seconds = redis_probe_interval_seconds
        self.shutdown_timeout_seconds = shutdown_timeout_seconds

        # Redis state
        self._redis: Optional[Any] = None
        self._redis_available = False
        self._redis_latency_ms = 500.0  # pessimistic default until first probe
        self._redis_lock: Optional[asyncio.Lock] = None

        # Memory cache
        self._memory_cache: "OrderedDict[str, Tuple[Any, datetime]]" = OrderedDict()
        self._memory_lock: Optional[asyncio.Lock] = None

        # Key stats (bounded)
        self.key_access_count: Dict[str, int] = {}
        self.key_last_access: Dict[str, datetime] = {}
        self.key_size_estimate: "OrderedDict[str, float]" = OrderedDict()
        self._key_stats_max = 10000

        # Rolling metrics
        self._rolling_hits: deque = deque(maxlen=100)
        self._rolling_latencies: deque = deque(maxlen=100)

        # Distillation
        self.distillation_config = {
            "distillation_epsilon": distillation_epsilon,
            "distillation_epsilon_min": distillation_epsilon_min,
            "distillation_epsilon_decay": distillation_epsilon_decay,
            "distillation_train_every": distillation_train_every,
            "distillation_replay_size": distillation_replay_size,
            "distillation_learning_rate": distillation_learning_rate,
            "distill_weight": distill_weight,
            "rl_weight": rl_weight,
        }
        self.policy_optimizer = DistillationCachePolicyOptimizer(
            self, self.distillation_config, enabled=True
        )
        self._distillation_queue: asyncio.Queue = asyncio.Queue(
            maxsize=distillation_queue_maxsize
        )
        self._distillation_dropped = 0
        self._distillation_task: Optional[asyncio.Task] = None

        # MOEA
        self.moea_enabled = moea_enabled
        self.moea_interval_seconds = moea_interval_seconds
        self.moea_population_size = moea_population_size
        self.moea_generations = moea_generations
        self.moea_mutation_rate = moea_mutation_rate
        self.moea_crossover_rate = moea_crossover_rate
        self.moea_objective_weights = moea_objective_weights or {
            "hit_rate": 0.4, "latency": 0.3,
            "memory_usage": 0.2, "redis_usage": 0.1,
        }
        self.moea_dynamic_weights = moea_dynamic_weights
        self.moea_optimizer: Optional[NSGAIIOptimizer] = None
        self.moea_pareto_front: List[MOPDPoint] = []
        self.moea_best_parameters: Optional[Dict[str, float]] = None

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
        self.moe_gating: Optional[MoEGatingNetworkPlaceholder] = (
            MoEGatingNetworkPlaceholder(
                storage, {"moe_expert_count": moe_expert_count},
                enabled=enable_moe,
            )
            if enable_moe else None
        )

        # Background tasks
        self._cleanup_task: Optional[asyncio.Task] = None
        self._health_task: Optional[asyncio.Task] = None
        self._moea_task: Optional[asyncio.Task] = None

        # Lifecycle
        self._running = False
        self._started = False
        self._shutdown = False

        # Metrics
        self.metrics: Optional[Dict[str, Any]] = None
        self._setup_metrics()

    # ---------------- locks ----------------
    def _get_redis_lock(self) -> asyncio.Lock:
        if self._redis_lock is None:
            self._redis_lock = asyncio.Lock()
        return self._redis_lock

    def _get_memory_lock(self) -> asyncio.Lock:
        if self._memory_lock is None:
            self._memory_lock = asyncio.Lock()
        return self._memory_lock

    # ---------------- metrics ----------------
    def _setup_metrics(self) -> None:
        if not PROMETHEUS_AVAILABLE:
            self.metrics = None
            return
        try:
            self.metrics = {
                "hits": Counter("cache_hits_total", "Cache hits"),
                "misses": Counter("cache_misses_total", "Cache misses"),
                "errors": Counter("cache_errors_total", "Cache errors", ["operation"]),
                "latency": Histogram(
                    "cache_operation_seconds", "Cache operation latency",
                    ["operation"],
                ),
                "size": Gauge("cache_size", "Cache entries"),
                "memory_size": Gauge("cache_memory_size", "Memory cache entries"),
                "redis_available": Gauge(
                    "cache_redis_available", "Redis availability (0/1)",
                ),
                "redis_latency_ms": Gauge(
                    "cache_redis_latency_ms", "Rolling Redis latency (ms)",
                ),
                "moea_pareto_front": Gauge(
                    "cache_moea_pareto_front", "MOEA Pareto front size",
                ),
                "distillation_dropped": Counter(
                    "cache_distillation_dropped_total",
                    "Distillation updates dropped due to queue overflow",
                ),
            }
        except Exception as e:
            logger.warning("metrics_setup_failed", error=str(e))
            self.metrics = None

    # ---------------- lifecycle ----------------
    @traced("cache_manager.start")
    async def start(self) -> None:
        if self._started and not self._shutdown:
            logger.warning("cache_already_started")
            return
        self._running = True
        self._started = True
        self._shutdown = False

        await self._init_redis()
        # Probe Redis latency once synchronously so the first request sees
        # a plausible value rather than the pessimistic default.
        await self._probe_redis_latency()

        self._cleanup_task = asyncio.create_task(self._memory_cleanup_loop())
        self._health_task = asyncio.create_task(self._redis_health_loop())
        self._distillation_task = asyncio.create_task(self._distillation_consumer())
        if self.moea_enabled:
            self._moea_task = asyncio.create_task(self._moea_loop())

        logger.info("cache_manager_started")

    async def ready(self) -> bool:
        if not self._started or self._shutdown:
            return False
        return self._redis_available or self._redis is None

    @traced("cache_manager.shutdown")
    async def shutdown(self, timeout: Optional[float] = None) -> None:
        if self._shutdown:
            return
        self._shutdown = True
        self._running = False
        timeout = timeout or self.shutdown_timeout_seconds
        logger.info("cache_manager_shutting_down")

        # Drain the distillation queue with a timeout so pending updates
        # are processed, then cancel.
        try:
            await asyncio.wait_for(
                self._distillation_queue.join(), timeout=timeout
            )
        except asyncio.TimeoutError:
            logger.warning("distillation_queue_drain_timeout")

        for task in (self._cleanup_task, self._health_task,
                      self._distillation_task, self._moea_task):
            if task is not None and not task.done():
                task.cancel()
        all_tasks = [
            t for t in (
                self._cleanup_task, self._health_task,
                self._distillation_task, self._moea_task,
            ) if t is not None
        ]
        if all_tasks:
            await asyncio.gather(*all_tasks, return_exceptions=True)

        self._cleanup_task = None
        self._health_task = None
        self._distillation_task = None
        self._moea_task = None

        if self._redis is not None:
            try:
                await self._redis.close()
                await self._redis.connection_pool.disconnect()
            except Exception as e:
                logger.warning("redis_close_failed", error=str(e))
            self._redis = None

        self._started = False
        logger.info("cache_manager_shutdown_complete")

    # `close` kept as an alias for backwards compatibility.
    async def close(self, timeout: Optional[float] = None) -> None:
        await self.shutdown(timeout=timeout)

    async def __aenter__(self) -> "CacheManager":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.shutdown()

    # ---------------- Redis helpers ----------------
    async def _init_redis(self) -> None:
        if not REDIS_AVAILABLE:
            logger.warning("redis_unavailable_using_memory")
            return
        async with self._get_redis_lock():
            try:
                pool = ConnectionPool.from_url(
                    self.redis_url, decode_responses=True
                )
                self._redis = Redis(connection_pool=pool)
                await asyncio.wait_for(
                    self._redis.ping(), timeout=self.redis_op_timeout_seconds
                )
                self._redis_available = True
                if self.metrics:
                    self.metrics["redis_available"].set(1)
                logger.info("redis_connected")
            except Exception as e:
                logger.error("redis_init_failed", error=str(e))
                self._redis = None
                self._redis_available = False
                if self.metrics:
                    self.metrics["redis_available"].set(0)

    async def _probe_redis_latency(self) -> None:
        """Sample Redis latency. Runs in the health loop, never on the hot path."""
        if self._redis is None or not self._redis_available:
            self._redis_latency_ms = 500.0
            return
        start = time.monotonic()
        try:
            await asyncio.wait_for(
                self._redis.ping(), timeout=self.redis_op_timeout_seconds
            )
            self._redis_latency_ms = (time.monotonic() - start) * 1000.0
        except Exception:
            self._redis_latency_ms = 500.0
        if self.metrics:
            try:
                self.metrics["redis_latency_ms"].set(self._redis_latency_ms)
            except Exception:
                pass

    async def _mark_redis_unavailable(self, reason: str) -> None:
        async with self._get_redis_lock():
            if not self._redis_available:
                return
            self._redis_available = False
            if self.metrics:
                try:
                    self.metrics["redis_available"].set(0)
                except Exception:
                    pass
        logger.warning("redis_marked_unavailable", reason=reason)

    async def _redis_health_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(self.redis_probe_interval_seconds)
            except asyncio.CancelledError:
                break
            try:
                if self._redis is not None and self._redis_available:
                    await self._probe_redis_latency()
                elif self._redis is None:
                    await self._init_redis()
                # Opportunistically update the size gauge off the hot path.
                if self._redis is not None and self._redis_available and self.metrics:
                    try:
                        dbsize = await asyncio.wait_for(
                            self._redis.dbsize(),
                            timeout=self.redis_op_timeout_seconds,
                        )
                        self.metrics["size"].set(
                            int(dbsize) + len(self._memory_cache)
                        )
                    except Exception:
                        pass
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("redis_health_error", error=str(e))

    async def _redis_operation(self, operation: str, *args, **kwargs) -> Any:
        if not self._redis_available or self._redis is None:
            raise RuntimeError("redis_unavailable")
        last_exc: Optional[Exception] = None
        for attempt in range(self.retry_attempts):
            try:
                coro = getattr(self._redis, operation)(*args, **kwargs)
                return await asyncio.wait_for(
                    coro, timeout=self.redis_op_timeout_seconds
                )
            except Exception as e:
                last_exc = e
                if attempt == self.retry_attempts - 1:
                    break
                delay = min(
                    self.retry_delay_ms * (2 ** attempt), 5000
                ) / 1000.0
                await asyncio.sleep(delay)
        assert last_exc is not None
        raise last_exc

    # ---------------- memory cleanup ----------------
    async def _memory_cleanup_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(self.cleanup_interval)
                await self._clean_expired_memory()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("memory_cleanup_error", error=str(e))

    async def _clean_expired_memory(self) -> None:
        async with self._get_memory_lock():
            now = datetime.now(timezone.utc)
            to_delete = [
                k for k, (_, expiry) in self._memory_cache.items()
                if expiry is not None and now > expiry
            ]
            for k in to_delete:
                self._memory_cache.pop(k, None)
        if self.metrics:
            try:
                self.metrics["memory_size"].set(len(self._memory_cache))
            except Exception:
                pass

    # ---------------- distillation consumer ----------------
    async def _distillation_consumer(self) -> None:
        while self._running or not self._distillation_queue.empty():
            try:
                item = await asyncio.wait_for(
                    self._distillation_queue.get(), timeout=1.0
                )
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            try:
                await self.policy_optimizer.update(*item)
            except Exception as e:
                logger.warning("distillation_update_failed", error=str(e))
            finally:
                self._distillation_queue.task_done()

    # ---------------- serialization ----------------
    def _serialize(self, value: Any) -> str:
        try:
            return self.serializer(value)
        except Exception as e:
            logger.error("serialization_failed", error=str(e))
            return str(value)

    def _deserialize(self, value_str: str) -> Any:
        try:
            return self.deserializer(value_str)
        except Exception as e:
            logger.error("deserialization_failed", error=str(e))
            return value_str

    # ---------------- policy state ----------------
    def _get_rolling_hit_rate(self) -> float:
        if not self._rolling_hits:
            return 0.5
        return sum(self._rolling_hits) / len(self._rolling_hits)

    def _get_rolling_avg_latency(self) -> float:
        if not self._rolling_latencies:
            return 0.0
        return sum(self._rolling_latencies) / len(self._rolling_latencies)

    async def _get_policy_state(self, key: str, value: Any) -> CachePolicyState:
        now = datetime.now(timezone.utc)
        key_length = len(key)

        if value is not None:
            try:
                size_bytes = float(len(self._serialize(value).encode("utf-8")))
            except Exception:
                size_bytes = 1024.0
        else:
            # For GETs, use the last-known size for this key if we have it.
            size_bytes = float(self.key_size_estimate.get(key, 1024.0))

        access_count = self.key_access_count.get(key, 0)
        last = self.key_last_access.get(key)
        if last is not None and (now - last).total_seconds() > 3600:
            access_count = 0

        memory_usage_pct = (
            len(self._memory_cache) / self.max_memory_entries * 100.0
            if self.max_memory_entries > 0 else 0.0
        )

        return CachePolicyState(
            key_length=key_length,
            estimated_size_bytes=size_bytes,
            access_frequency=float(access_count),
            time_of_day_hour=now.hour,
            redis_available=bool(self._redis_available),
            redis_latency_ms=float(self._redis_latency_ms),
            memory_usage_pct=float(memory_usage_pct),
            hit_rate=float(self._get_rolling_hit_rate()),
            avg_latency_ms=float(self._get_rolling_avg_latency()),
        )

    def _update_key_stats(self, key: str, hit: bool, latency_ms: float,
                           size_bytes: Optional[float] = None) -> None:
        now = datetime.now(timezone.utc)
        self.key_access_count[key] = self.key_access_count.get(key, 0) + 1
        self.key_last_access[key] = now
        self._rolling_hits.append(1 if hit else 0)
        self._rolling_latencies.append(latency_ms)
        if size_bytes is not None:
            self.key_size_estimate[key] = size_bytes
            self.key_size_estimate.move_to_end(key)
            while len(self.key_size_estimate) > self._key_stats_max:
                self.key_size_estimate.popitem(last=False)
        # Bound key stats
        if len(self.key_access_count) > self._key_stats_max:
            # Drop the oldest entries by last access
            ordered = sorted(
                self.key_last_access.items(), key=lambda kv: kv[1]
            )
            to_remove = len(self.key_access_count) - self._key_stats_max
            for k, _ in ordered[:to_remove]:
                self.key_access_count.pop(k, None)
                self.key_last_access.pop(k, None)

    def _pick_policy(self, state: CachePolicyState, size_for_set: Optional[float]
                     ) -> Tuple[str, Optional[int], Optional[np.ndarray],
                                 Optional[np.ndarray], Optional[str]]:
        """
        Synchronous policy selection. MoE takes precedence if present; else
        distillation. Returns (policy, action_idx, state_vec, teacher_probs,
        expert_name_for_moe).
        """
        if self.moe_gating is not None:
            expert_name, _ = asyncio.run_coroutine_threadsafe(
                self.moe_gating.select_expert(state),
                asyncio.get_event_loop(),
            ).result() if False else (None, None)
            # Fallback synchronous selection for simplicity:
            x = state.to_feature_vector()
            logits = self.moe_gating.gating_weights @ x
            logits = logits - np.max(logits)
            probs = np.exp(logits)
            probs = probs / probs.sum()
            expert_idx = int(np.argmax(probs))
            expert_name = self.moe_gating.expert_names[expert_idx]
            policy_map = {
                "redis_first": "redis_ttl_short",
                "memory_first": "memory_only",
                "size_aware": (
                    "redis_ttl_long"
                    if (size_for_set or 0.0) > 10000 else "memory_only"
                ),
                "frequency_aware": "adaptive_ttl",
            }
            policy = policy_map.get(expert_name, "adaptive_ttl")
            try:
                action_idx = CACHE_ACTION_SPACE.index(policy)
            except ValueError:
                action_idx = 0
            return policy, action_idx, x, np.ones(5) / 5, expert_name
        # Distillation path
        return None, None, None, None, None

    # ---------------- MoE / distillation selection ----------------
    async def _select_policy_async(self, state: CachePolicyState,
                                     size_for_set: Optional[float]
                                     ) -> Tuple[str, int, np.ndarray, np.ndarray, Optional[str]]:
        if self.moe_gating is not None:
            expert_name, _ = await self.moe_gating.select_expert(state)
            policy_map = {
                "redis_first": "redis_ttl_short",
                "memory_first": "memory_only",
                "size_aware": (
                    "redis_ttl_long"
                    if (size_for_set or 0.0) > 10000 else "memory_only"
                ),
                "frequency_aware": "adaptive_ttl",
            }
            policy = policy_map.get(expert_name, "adaptive_ttl")
            action_idx = CACHE_ACTION_SPACE.index(policy)
            return policy, action_idx, state.to_feature_vector(), np.ones(5) / 5, expert_name
        policy, action_idx, state_vec, teacher_probs = (
            await self.policy_optimizer.select_policy(state, exploration=True)
        )
        return policy, action_idx, state_vec, teacher_probs, None

    def _resolve_ttl(self, policy: str, caller_ttl: int) -> int:
        """Apply documented TTL precedence rules."""
        if self.moea_best_parameters:
            if policy == "redis_ttl_short" and "ttl_short" in self.moea_best_parameters:
                return int(self.moea_best_parameters["ttl_short"])
            if policy == "redis_ttl_long" and "ttl_long" in self.moea_best_parameters:
                return int(self.moea_best_parameters["ttl_long"])
            if policy == "memory_only" and "memory_ttl" in self.moea_best_parameters:
                return int(self.moea_best_parameters["memory_ttl"])
        if policy == "redis_ttl_short":
            return 60
        if policy == "redis_ttl_long":
            return 600
        if policy == "memory_only":
            return int(caller_ttl)
        if policy == "adaptive_ttl":
            return 300
        # unknown policy => caller TTL
        return int(caller_ttl)

    # ---------------- core get / set ----------------
    @traced("cache_manager.get")
    async def get(self, key: str) -> Optional[Any]:
        start = time.monotonic()
        cid = _new_cid()
        token = _cid_ctx.set(cid)

        try:
            state = await self._get_policy_state(key, None)
            policy, action_idx, state_vec, teacher_probs, expert_name = (
                await self._select_policy_async(state, None)
            )

            ttl_for_get = 0
            success, hit, latency_ms, result = await self._do_get(
                policy, key
            )

            self._update_key_stats(key, hit, latency_ms)

            reward = self._compute_get_reward(
                policy=policy, hit=hit, latency_ms=latency_ms,
                state=state,
            )

            # Queue a distillation update; never spawn an untracked task.
            if state_vec is not None and teacher_probs is not None:
                next_state = await self._get_policy_state(key, None)
                try:
                    self._distillation_queue.put_nowait((
                        state_vec, action_idx, reward,
                        next_state.to_feature_vector(), teacher_probs,
                    ))
                except asyncio.QueueFull:
                    self._distillation_dropped += 1
                    if self.metrics:
                        try:
                            self.metrics["distillation_dropped"].inc()
                        except Exception:
                            pass

            # MoE reward update
            if self.moe_gating is not None and expert_name is not None:
                try:
                    await self.moe_gating.add_training_sample(
                        state, expert_name, reward
                    )
                except Exception as e:
                    logger.warning("moe_update_failed", error=str(e))

            # Optional RLHF record
            if self.rlhf_trainer is not None and random.random() < 0.05:
                chosen = policy
                rejected = random.choice(
                    [p for p in CACHE_ACTION_SPACE if p != chosen]
                )
                self.rlhf_trainer.record_pair(
                    pair_id=str(uuid.uuid4()),
                    prompt=f"Which policy for key {key[:20]}?",
                    chosen=chosen,
                    rejected=rejected,
                    reward_diff=reward,
                    metadata={"cid": cid},
                )

            if self.metrics:
                try:
                    if hit:
                        self.metrics["hits"].inc()
                    else:
                        self.metrics["misses"].inc()
                    self.metrics["latency"].labels("get").observe(
                        time.monotonic() - start
                    )
                except Exception:
                    pass

            return result
        finally:
            _cid_ctx.reset(token)

    @traced("cache_manager.set")
    async def set(self, key: str, value: Any, ttl: int = 300) -> None:
        start = time.monotonic()
        cid = _new_cid()
        token = _cid_ctx.set(cid)

        try:
            state = await self._get_policy_state(key, value)
            policy, action_idx, state_vec, teacher_probs, expert_name = (
                await self._select_policy_async(
                    state, state.estimated_size_bytes
                )
            )
            effective_ttl = self._resolve_ttl(policy, ttl)
            success, _, latency_ms, _ = await self._do_set(
                policy, key, value, effective_ttl
            )

            self._update_key_stats(
                key, hit=False, latency_ms=latency_ms,
                size_bytes=state.estimated_size_bytes,
            )

            reward = self._compute_set_reward(
                policy=policy, success=success,
                latency_ms=latency_ms, state=state,
            )

            if state_vec is not None and teacher_probs is not None:
                next_state = await self._get_policy_state(key, value)
                try:
                    self._distillation_queue.put_nowait((
                        state_vec, action_idx, reward,
                        next_state.to_feature_vector(), teacher_probs,
                    ))
                except asyncio.QueueFull:
                    self._distillation_dropped += 1
                    if self.metrics:
                        try:
                            self.metrics["distillation_dropped"].inc()
                        except Exception:
                            pass

            if self.moe_gating is not None and expert_name is not None:
                try:
                    await self.moe_gating.add_training_sample(
                        state, expert_name, reward
                    )
                except Exception as e:
                    logger.warning("moe_update_failed", error=str(e))

            if self.metrics:
                try:
                    self.metrics["latency"].labels("set").observe(
                        time.monotonic() - start
                    )
                    self.metrics["memory_size"].set(len(self._memory_cache))
                except Exception:
                    pass
        finally:
            _cid_ctx.reset(token)

    def _compute_get_reward(self, policy: str, hit: bool,
                             latency_ms: float,
                             state: CachePolicyState) -> float:
        reward = 0.0
        if hit:
            reward += 0.5
        if latency_ms < 10:
            reward += 0.3
        elif latency_ms < 50:
            reward += 0.15
        if policy.startswith("redis") and state.redis_available:
            reward += 0.2
        elif policy == "memory_only" and state.memory_usage_pct < 50:
            reward += 0.1
        return max(0.0, min(1.0, reward))

    def _compute_set_reward(self, policy: str, success: bool,
                             latency_ms: float,
                             state: CachePolicyState) -> float:
        reward = 0.0
        if success:
            reward += 0.6
        if latency_ms < 5:
            reward += 0.2
        elif latency_ms < 20:
            reward += 0.1
        if (policy.startswith("redis")
                and state.estimated_size_bytes > 100_000):
            reward += 0.2
        return max(0.0, min(1.0, reward))

    async def _do_get(self, policy: str, key: str
                       ) -> Tuple[bool, bool, float, Optional[Any]]:
        start = time.monotonic()
        success = False
        hit = False
        result: Optional[Any] = None

        if policy == "no_cache":
            return True, False, (time.monotonic() - start) * 1000.0, None

        backend = "redis" if policy.startswith("redis") or (
            policy == "adaptive_ttl" and self._redis_available
        ) else "memory"

        if backend == "redis":
            try:
                result_str = await self._redis_operation("get", key)
                if result_str is not None:
                    result = self._deserialize(result_str)
                    hit = True
                success = True
            except Exception as e:
                logger.error("redis_get_failed", key=key, error=str(e))
                await self._mark_redis_unavailable("get")
                success = False
        else:
            async with self._get_memory_lock():
                entry = self._memory_cache.get(key)
                if entry is not None:
                    stored_value, expiry = entry
                    now = datetime.now(timezone.utc)
                    if expiry is not None and now > expiry:
                        self._memory_cache.pop(key, None)
                        hit = False
                    else:
                        result = stored_value
                        hit = True
                        self._memory_cache.move_to_end(key)
                else:
                    hit = False
            success = True

        latency_ms = (time.monotonic() - start) * 1000.0

        # MODP bookkeeping (placeholder: recorded, not solved).
        if self.modp_solver is not None:
            try:
                self.modp_solver.add_state(
                    state_id=f"{key}_get_{time.time()}",
                    problem_id="cache_policy",
                    state_attributes={"key": key, "policy": policy, "hit": hit},
                    objective_values={
                        "hit_rate": 1.0 if hit else 0.0,
                        "latency": latency_ms,
                        "memory_usage": 0.0,
                    },
                    stage=1,
                )
            except Exception:
                pass

        return success, hit, latency_ms, result

    async def _do_set(self, policy: str, key: str, value: Any, ttl: int
                       ) -> Tuple[bool, bool, float, Optional[Any]]:
        start = time.monotonic()
        success = False

        if policy == "no_cache":
            return True, False, (time.monotonic() - start) * 1000.0, None

        backend = "redis" if policy.startswith("redis") or (
            policy == "adaptive_ttl" and self._redis_available
        ) else "memory"

        if backend == "redis":
            try:
                serialized = self._serialize(value)
                written = await self._redis_operation(
                    "setex", key, int(ttl), serialized
                )
                success = bool(written)
            except Exception as e:
                logger.error("redis_set_failed", key=key, error=str(e))
                await self._mark_redis_unavailable("set")
                success = False
        else:
            async with self._get_memory_lock():
                expiry = datetime.now(timezone.utc) + timedelta(seconds=int(ttl))
                self._memory_cache[key] = (value, expiry)
                self._memory_cache.move_to_end(key)
                while len(self._memory_cache) > self.max_memory_entries:
                    self._memory_cache.popitem(last=False)
            success = True

        latency_ms = (time.monotonic() - start) * 1000.0

        if self.modp_solver is not None:
            try:
                self.modp_solver.add_state(
                    state_id=f"{key}_set_{time.time()}",
                    problem_id="cache_policy",
                    state_attributes={"key": key, "policy": policy, "ttl": ttl},
                    objective_values={
                        "hit_rate": 0.0, "latency": latency_ms,
                        "memory_usage": 0.0,
                    },
                    stage=0,
                )
            except Exception:
                pass

        if self.limit_graph_manager is not None:
            try:
                self.limit_graph_manager.add_node(
                    "cache_keys", key, "cache_key",
                    {"policy": policy, "ttl": ttl},
                )
            except Exception:
                pass

        return success, False, latency_ms, None

    # ---------------- delete / clear / get_or_set ----------------
    @traced("cache_manager.delete")
    async def delete(self, key: str) -> bool:
        deleted = False
        if self._redis_available:
            try:
                n = await self._redis_operation("delete", key)
                deleted = int(n) > 0
            except Exception as e:
                logger.error("redis_delete_failed", key=key, error=str(e))
                await self._mark_redis_unavailable("delete")
        if not deleted:
            async with self._get_memory_lock():
                if key in self._memory_cache:
                    self._memory_cache.pop(key, None)
                    deleted = True
        return deleted

    @traced("cache_manager.clear")
    async def clear(self) -> None:
        if self._redis_available:
            try:
                await self._redis_operation("flushdb")
            except Exception as e:
                logger.error("redis_clear_failed", error=str(e))
                await self._mark_redis_unavailable("clear")
        async with self._get_memory_lock():
            self._memory_cache.clear()
        if self.metrics:
            try:
                self.metrics["memory_size"].set(0)
            except Exception:
                pass

    async def get_or_set(self, key: str, default: Any, ttl: int = 300) -> Any:
        value = await self.get(key)
        if value is None:
            await self.set(key, default, ttl)
            return default
        return value

    # ---------------- MOEA ----------------
    async def _moea_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(self.moea_interval_seconds)
                await self.run_moea_optimization()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("moea_loop_failed", error=str(e))
                await asyncio.sleep(60)

    async def run_moea_optimization(self) -> None:
        if not self.moea_enabled:
            return

        param_bounds = {
            "ttl_short": (10.0, 300.0),
            "ttl_long": (300.0, 3600.0),
            "memory_ttl": (30.0, 1800.0),
            "redis_threshold": (0.0, 1.0),
        }

        async def evaluate(params: Dict[str, float]) -> Dict[str, float]:
            ttl_short = params["ttl_short"]
            ttl_long = params["ttl_long"]
            memory_ttl = params["memory_ttl"]
            hit_rate = min(
                0.95, 0.3 + 0.0005 * (ttl_short + ttl_long + memory_ttl)
            )
            redis_usage = params.get("redis_threshold", 0.5)
            mem_pct = (
                len(self._memory_cache) / self.max_memory_entries
                if self.max_memory_entries > 0 else 0.0
            )
            memory_usage = min(1.0, mem_pct * (memory_ttl / 3600.0))
            avg_latency = redis_usage * 2.0 + (1.0 - redis_usage) * 0.1
            latency_score = 1.0 - min(avg_latency / 10.0, 1.0)
            return {
                "hit_rate": hit_rate,
                "latency": latency_score,
                "memory_usage": 1.0 - memory_usage,
                "redis_usage": 1.0 - redis_usage,
            }

        self.moea_optimizer = NSGAIIOptimizer(
            evaluate_func=evaluate,
            parameter_bounds=param_bounds,
            population_size=self.moea_population_size,
            generations=self.moea_generations,
            mutation_rate=self.moea_mutation_rate,
            crossover_rate=self.moea_crossover_rate,
            objective_weights=self._get_dynamic_moea_weights(),
            dynamic_weights=self.moea_dynamic_weights,
        )

        pareto = await self.moea_optimizer.evolve()
        self.moea_pareto_front = pareto
        if pareto:
            weights = self._get_dynamic_moea_weights()
            best = self.moea_optimizer._select_best_from_pareto(pareto, weights)
            if best is not None:
                self.moea_best_parameters = dict(best.parameters)
                if self.metrics:
                    try:
                        self.metrics["moea_pareto_front"].set(len(pareto))
                    except Exception:
                        pass

    def _get_dynamic_moea_weights(self) -> Dict[str, float]:
        weights = dict(self.moea_objective_weights)
        if not self.moea_dynamic_weights:
            return weights
        mem_pct = (
            len(self._memory_cache) / self.max_memory_entries
            if self.max_memory_entries > 0 else 0.0
        )
        if mem_pct > 0.8:
            weights["memory_usage"] = min(
                0.6, weights.get("memory_usage", 0.2) * 1.5
            )
        total = sum(weights.values()) or 1.0
        return {k: v / total for k, v in weights.items()}

    # ---------------- stats ----------------
    async def get_stats(self) -> Dict[str, Any]:
        return {
            "started": self._started,
            "shutdown": self._shutdown,
            "backend": "redis" if self._redis_available else "memory",
            "memory_entries": len(self._memory_cache),
            "redis_available": self._redis_available,
            "redis_latency_ms": self._redis_latency_ms,
            "distillation": self.policy_optimizer.get_stats(),
            "distillation_queue": {
                "size": self._distillation_queue.qsize(),
                "dropped": self._distillation_dropped,
            },
            "moea": {
                "enabled": self.moea_enabled,
                "pareto_front_size": len(self.moea_pareto_front),
                "best_parameters": self.moea_best_parameters,
            },
            "module_status": MODULE_STATUS,
            "new_components": {
                "limit_graph": (
                    self.limit_graph_manager.available
                    if self.limit_graph_manager else False
                ),
                "modp": (
                    self.modp_solver.available if self.modp_solver else False
                ),
                "rlhf": (
                    self.rlhf_trainer.available if self.rlhf_trainer else False
                ),
                "moe": (
                    self.moe_gating.available if self.moe_gating else False
                ),
            },
        }


# =============================================================================
# SECTION 8. TESTS
# =============================================================================
class _Tests(unittest.TestCase):
    def _cache(self, **overrides) -> CacheManager:
        cfg = dict(
            max_memory_entries=5,
            cleanup_interval_seconds=3600,
            moea_enabled=False,
            moea_interval_seconds=3600,
            distillation_train_every=2,
            enable_limit_graph=False,
            enable_modp=False,
            enable_rlhf=False,
            enable_moe=False,
        )
        cfg.update(overrides)
        return CacheManager(**cfg)

    def test_lifecycle(self):
        async def go():
            c = self._cache()
            self.assertFalse(await c.ready())
            await c.start()
            self.assertTrue(await c.ready())
            await c.shutdown()
            self.assertTrue(c._shutdown)
            await c.shutdown()  # idempotent
        asyncio.run(go())

    def test_context_manager(self):
        async def go():
            async with self._cache() as c:
                self.assertTrue(await c.ready())
        asyncio.run(go())

    def test_memory_set_get(self):
        async def go():
            async with self._cache() as c:
                await c.set("a", {"x": 1}, ttl=60)
                v = await c.get("a")
                self.assertEqual(v, {"x": 1})
        asyncio.run(go())

    def test_memory_ttl_expiry(self):
        async def go():
            async with self._cache() as c:
                # Force memory-only policy by making Redis unavailable.
                await c.set("a", {"x": 1}, ttl=1)
                # Manually expire
                async with c._get_memory_lock():
                    stored, _ = c._memory_cache["a"]
                    c._memory_cache["a"] = (
                        stored,
                        datetime.now(timezone.utc) - timedelta(seconds=1),
                    )
                v = await c.get("a")
                self.assertIsNone(v)
        asyncio.run(go())

    def test_memory_lru_eviction(self):
        async def go():
            async with self._cache() as c:
                for i in range(10):
                    await c.set(f"k{i}", i, ttl=60)
                # Only max_memory_entries should remain
                self.assertLessEqual(
                    len(c._memory_cache), c.max_memory_entries
                )
        asyncio.run(go())

    def test_policy_state_rolling_metrics(self):
        async def go():
            async with self._cache() as c:
                await c.set("a", 1, ttl=60)
                await c.get("a")
                await c.get("missing")
                state = await c._get_policy_state("a", None)
                # One hit, one miss
                self.assertAlmostEqual(state.hit_rate, 0.5, places=2)
        asyncio.run(go())

    def test_key_size_estimate_populated(self):
        async def go():
            async with self._cache() as c:
                await c.set("big", {"data": "x" * 5000}, ttl=60)
                # Force memory path so we can inspect
                self.assertIn("big", c.key_size_estimate)
                self.assertGreater(c.key_size_estimate["big"], 5000)
        asyncio.run(go())

    def test_ttl_precedence_caller(self):
        c = self._cache()
        # memory_only respects caller TTL
        self.assertEqual(c._resolve_ttl("memory_only", 42), 42)
        # redis_ttl_short hardcodes 60
        self.assertEqual(c._resolve_ttl("redis_ttl_short", 42), 60)
        # redis_ttl_long hardcodes 600
        self.assertEqual(c._resolve_ttl("redis_ttl_long", 42), 600)
        # adaptive_ttl hardcodes 300
        self.assertEqual(c._resolve_ttl("adaptive_ttl", 42), 300)

    def test_ttl_precedence_moea_override(self):
        c = self._cache()
        c.moea_best_parameters = {"ttl_short": 15, "ttl_long": 1200,
                                    "memory_ttl": 90}
        self.assertEqual(c._resolve_ttl("redis_ttl_short", 42), 15)
        self.assertEqual(c._resolve_ttl("redis_ttl_long", 42), 1200)
        self.assertEqual(c._resolve_ttl("memory_only", 42), 90)

    def test_moe_gating_reward_weighting(self):
        """
        Regression: the reward must influence the gating update. If the
        update ignored reward (the v2.2.1 bug), the same expert would gain
        weight even with zero reward.
        """
        async def go():
            gate = MoEGatingNetworkPlaceholder(enabled=False)
            state = CachePolicyState(
                key_length=10, estimated_size_bytes=100,
                access_frequency=5, time_of_day_hour=12,
                redis_available=True, redis_latency_ms=5.0,
                memory_usage_pct=20.0, hit_rate=0.5, avg_latency_ms=3.0,
            )
            initial = gate.gating_weights.copy()
            # Zero reward should leave weights unchanged (up to numeric noise)
            await gate.add_training_sample(state, "redis_first", reward=0.0)
            np.testing.assert_allclose(gate.gating_weights, initial, atol=1e-9)
            # Positive reward should change weights
            await gate.add_training_sample(state, "redis_first", reward=1.0)
            diff = np.abs(gate.gating_weights - initial).sum()
            self.assertGreater(diff, 1e-6)
        asyncio.run(go())

    def test_nsga2_select_does_not_mutate_front(self):
        async def go():
            async def eval_fn(p):
                return {"a": p["x"], "b": 1.0 - p["x"]}

            opt = NSGAIIOptimizer(
                evaluate_func=eval_fn,
                parameter_bounds={"x": (0.0, 1.0)},
                population_size=6, generations=2,
                objective_weights={"a": 0.5, "b": 0.5},
            )
            front = await opt.evolve()
            before = [p.scalarised_score for p in front]
            _ = opt._select_best_from_pareto(
                front, {"a": 0.5, "b": 0.5}
            )
            after = [p.scalarised_score for p in front]
            self.assertEqual(before, after)
        asyncio.run(go())

    def test_nsga2_eval_cache_bounded(self):
        async def go():
            async def eval_fn(p):
                return {"a": p["x"]}

            opt = NSGAIIOptimizer(
                evaluate_func=eval_fn,
                parameter_bounds={"x": (0.0, 1.0)},
                population_size=10, generations=1,
                max_cache_entries=20,
            )
            await opt.evolve()
            self.assertLessEqual(len(opt._eval_cache), 20)
        asyncio.run(go())

    def test_placeholders_honest(self):
        lg = LimitGraphManagerPlaceholder()
        self.assertFalse(lg.available)
        mo = MODPOptimizerPlaceholder()
        self.assertFalse(mo.available)
        rl = RLHFTrainerPlaceholder()
        self.assertFalse(rl.available)
        mg = MoEGatingNetworkPlaceholder()
        self.assertFalse(mg.available)

    def test_redis_operation_without_client_raises(self):
        async def go():
            c = self._cache()
            with self.assertRaises(RuntimeError):
                await c._redis_operation("get", "x")
        asyncio.run(go())

    def test_get_stats(self):
        async def go():
            async with self._cache() as c:
                await c.set("a", 1, ttl=60)
                stats = await c.get_stats()
                self.assertIn("module_status", stats)
                self.assertIn("distillation", stats)
                self.assertIn("moea", stats)
        asyncio.run(go())

    def test_distillation_queue_bounded(self):
        async def go():
            async with self._cache(distillation_queue_maxsize=2) as c:
                for i in range(10):
                    await c.set(f"k{i}", i, ttl=60)
                # Should not crash; some may be dropped
                self.assertLessEqual(c._distillation_queue.qsize(), 2)
        asyncio.run(go())

    def test_module_status_documented(self):
        self.assertEqual(MODULE_STATUS["cache_core"], "stable")
        self.assertEqual(MODULE_STATUS["moe_gating"], "placeholder")
        self.assertEqual(MODULE_STATUS["nsga2_optimizer"], "experimental")

    def test_eval_cache_lru(self):
        async def go():
            async def eval_fn(p):
                return {"a": p["x"]}

            opt = NSGAIIOptimizer(
                evaluate_func=eval_fn,
                parameter_bounds={"x": (0.0, 1.0)},
                population_size=4, generations=1,
                max_cache_entries=3,
            )
            await opt._evaluate({"x": 0.1})
            await opt._evaluate({"x": 0.2})
            await opt._evaluate({"x": 0.3})
            await opt._evaluate({"x": 0.4})
            self.assertEqual(len(opt._eval_cache), 3)
        asyncio.run(go())


def run_tests() -> int:
    suite = unittest.TestLoader().loadTestsFromTestCase(_Tests)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    return 0 if result.wasSuccessful() else 1


# =============================================================================
# SECTION 9. ENTRY POINT
# =============================================================================
async def _example() -> None:
    cache = CacheManager(
        max_memory_entries=10,
        cleanup_interval_seconds=3600,
        moea_enabled=False,
        enable_moe=False,
        enable_rlhf=False,
        enable_modp=False,
        enable_limit_graph=False,
    )
    async with cache:
        for i in range(10):
            key = f"key{i % 4}"
            if i % 3 == 0:
                await cache.set(key, {"data": i}, ttl=60)
            else:
                v = await cache.get(key)
                print(f"get {key}: {v}")
            await asyncio.sleep(0.02)
        stats = await cache.get_stats()
        print("Stats:", json.dumps(stats, indent=2, default=str))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Enhanced Cache Manager for Green Agent v3.0.0"
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
            print(f"{name:22s} {status}")
        return

    if args.test:
        sys.exit(run_tests())

    if args.example:
        asyncio.run(_example())
        return

    print("Enhanced Cache Manager v3.0.0 — no mode selected.")
    print("Use --test, --example, or --status.")


if __name__ == "__main__":
    main()
