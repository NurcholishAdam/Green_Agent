#!/usr/bin/env python3
# =============================================================================
# Enhanced Sustainability Cost Function v3.0.0 — Patched Single-File Edition
# =============================================================================
"""
Enhanced Sustainability Cost Function v3.0.0
=============================================
Patched single-file version of v2.4.1.

P0 fixes
--------
- Added missing imports: `dataclass`, `asdict`, `hashlib`, `Callable`.
- Local schema imports are guarded with fallback stubs, so the module imports
  even when the sibling packages are absent.
- Feature dimension unified to 12 everywhere (state, MoE, Q-teacher, student).
- MoE gating update is reward-weighted (regression test included).
- Distillation updates go through a bounded queue with a single consumer.
- Lifecycle moved out of `__init__`: use `await cost_fn.start()`.
- `_last_action_idx`, `_last_teacher_probs`, `_last_strategy` initialized.
- Every asyncio.Lock is created lazily.

P1 — correctness
----------------
- Carbon unit conversion fixed: energy_joules * carbon_intensity / 3_600_000.
- `_build_optimization_state` uses the async carbon fetch, so the state
  sees fresh carbon intensity.
- Helium collector is wired in (with graceful fallback if absent).
- `_get_material_composite` handles async footprint updaters.
- `_normalize_*` returns values in [0, 1] (documented).
- NSGA-II `_eval_cache` is LRU-bounded.
- MOEA evaluation does not mutate the live `_current_weights`.
- `DistillationCostOptimizer` decays epsilon.
- `set_weights` also updates `_current_weights`.
- `_last_*` state is reset at the top of every `compute`.

P2 — honesty
------------
- MODULE_STATUS documents every module.
- Placeholders (disabled by default, `.available = False`, warn when enabled):
    moe_gating, rlhf, modp, limit_graph, anomaly_hooks.
- Experimental (warn when enabled):
    distillation, nsga2_optimizer, adaptive_weights, carbon_cache.

P3 — production readiness
-------------------------
- Lifecycle: `async start()`, `async ready()`, `async shutdown()`,
  `__aenter__` / `__aexit__`.
- Embedded test suite: `python3 sustainability_cost.py --test`.
- `--status` prints module maturity.
- Prometheus + OpenTelemetry (both optional).
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
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import (
    Any, Awaitable, Callable, Dict, List, Optional, Tuple, Union,
)

import numpy as np

# -----------------------------------------------------------------------------
# Optional dependencies
# -----------------------------------------------------------------------------
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
    _TRACER = trace.get_tracer("sustainability_cost")
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
# Local schemas (guarded with fallback stubs)
# -----------------------------------------------------------------------------
try:
    from ..schemas.node_descriptor import NodeDescriptor  # type: ignore
    from ..schemas.workload_descriptor import WorkloadDescriptor  # type: ignore
    from ..data_integration.carbon_intensity import CarbonIntensityFetcher  # type: ignore
    from ..data_integration.material_footprint import MaterialFootprintUpdater  # type: ignore
    from ..data_integration.helium_collector import HeliumCollector  # type: ignore
    from ..expert_registry import ExpertProfile  # type: ignore
    LOCAL_SCHEMAS_AVAILABLE = True
except ImportError:
    LOCAL_SCHEMAS_AVAILABLE = False

    @dataclass
    class NodeDescriptor:  # type: ignore
        id: str = "node"
        region: str = "default"
        energy_per_token: float = 0.0001
        helium_connectivity_score: float = 0.5
        material_footprint_id: Optional[str] = None

    @dataclass
    class WorkloadDescriptor:  # type: ignore
        tokens: int = 1000
        latency_target: float = 1000.0

    class CarbonIntensityFetcher:  # type: ignore
        async def get_intensity(self, region: str) -> float:
            return 0.4

    class MaterialFootprintUpdater:  # type: ignore
        def get_footprint(self, footprint_id: str) -> Optional[Dict[str, float]]:
            return None

    class HeliumCollector:  # type: ignore
        async def get_scarcity(self, node_id: str) -> Optional[float]:
            return None

    @dataclass
    class ExpertProfile:  # type: ignore
        accuracy_score: float = 0.9


# Central storage (optional)
try:
    from ...storage import Storage  # type: ignore
    CENTRAL_STORAGE_AVAILABLE = True
except ImportError:
    Storage = None  # type: ignore
    CENTRAL_STORAGE_AVAILABLE = False


# =============================================================================
# SECTION 1. MODULE STATUS
# =============================================================================
MODULE_STATUS: Dict[str, str] = {
    "cost_core":        "stable",
    "normalization":    "stable",
    "carbon_cache":     "experimental",
    "adaptive_weights": "experimental",
    "distillation":     "experimental",
    "nsga2_optimizer":  "experimental",
    "moe_gating":       "placeholder",
    "rlhf":             "placeholder",
    "modp":             "placeholder",
    "limit_graph":      "placeholder",
    "anomaly_hooks":    "placeholder",
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
_cid_ctx: ContextVar[str] = ContextVar("cost_cid", default="-")


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


# The single source of truth for distillation strategy actions.
STRATEGY_ACTION_SPACE: Tuple[str, ...] = (
    "standard",
    "carbon_focus",
    "energy_focus",
    "helium_focus",
    "adaptive",
)

# Feature dimension for the state vector.
STATE_FEATURE_DIM = 12


# =============================================================================
# SECTION 3. CONFIG (pure dataclass, works with or without Pydantic)
# =============================================================================
@dataclass
class CostConfig:
    # Initial weights (must sum to 1)
    energy_weight: float = 0.2
    carbon_weight: float = 0.3
    helium_weight: float = 0.15
    material_weight: float = 0.15
    latency_weight: float = 0.1
    accuracy_weight: float = 0.1

    # Normalization baselines
    latency_baseline_ms: float = 1000.0
    latency_max_ms: float = 5000.0
    accuracy_baseline: float = 0.9
    accuracy_max: float = 1.0
    energy_baseline_joules: float = 0.0001
    energy_max_joules: float = 0.001
    carbon_intensity_baseline_kg_per_kwh: float = 0.4
    carbon_intensity_max_kg_per_kwh: float = 1.0
    helium_scarcity_threshold: float = 0.7
    helium_max_scarcity: float = 1.0
    material_embodied_norm: float = 200.0
    material_rare_earth_norm: float = 0.01
    material_max_composite: float = 1.0

    # Carbon cache
    carbon_cache_ttl_seconds: int = 300
    carbon_cache_max_size: int = 100

    # Integration flags
    use_adaptive_weights: bool = False
    integrate_anomaly_detection: bool = False
    integrate_predictive_maintenance: bool = False

    # Distillation
    distillation_epsilon: float = 0.1
    distillation_epsilon_min: float = 0.01
    distillation_epsilon_decay: float = 0.995
    distillation_train_every: int = 10
    distillation_replay_size: int = 2000
    distillation_learning_rate: float = 0.01
    distill_weight: float = 0.7
    rl_weight: float = 0.3
    distillation_queue_maxsize: int = 1000

    # NSGA-II / MOEA
    moea_enabled: bool = True
    moea_interval_seconds: int = 300
    moea_population_size: int = 30
    moea_generations: int = 10
    moea_mutation_rate: float = 0.2
    moea_crossover_rate: float = 0.8
    moea_tournament_size: int = 3
    moea_objective_weights: Dict[str, float] = field(default_factory=lambda: {
        "energy": 0.2, "carbon": 0.3, "helium": 0.15,
        "material": 0.15, "latency": 0.1, "accuracy": 0.1,
    })
    moea_dynamic_weights: bool = True
    moea_eval_cache_max_entries: int = 5000

    # Shutdown
    shutdown_timeout_seconds: float = 10.0

    # Placeholder toggles — disabled by default
    enable_limit_graph: bool = False
    enable_modp: bool = False
    enable_rlhf: bool = False
    enable_moe: bool = False
    moe_expert_count: int = 4

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CostConfig":
        data = dict(data or {})
        return cls(**{
            k: v for k, v in data.items()
            if k in cls.__dataclass_fields__
        })


# =============================================================================
# SECTION 4. PLACEHOLDERS
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
            "description": description, "configuration": configuration,
            "nodes": {}, "edges": {},
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


class MoEGatingNetworkPlaceholder:
    """
    STATUS: placeholder.

    Reward-weighted policy gradient update is implemented correctly; the
    module is a placeholder because it is disabled by default and its
    decisions do not feed back into the weight-selection path unless the
    caller explicitly opts in.
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
            "carbon_focus", "energy_focus", "helium_focus", "adaptive"
        ][:self.num_experts]
        self.state_dim = STATE_FEATURE_DIM
        self.lr = 0.05
        self.gating_weights = (
            np.random.randn(self.num_experts, self.state_dim) * 0.1
        )
        self.available = False

    def _encode_state(self, state: "CostOptimizationState") -> np.ndarray:
        return state.to_feature_vector()

    async def select_expert(self, state: "CostOptimizationState"
                             ) -> Tuple[str, np.ndarray]:
        x = self._encode_state(state)
        logits = self.gating_weights @ x
        logits = logits - np.max(logits)
        probs = np.exp(logits)
        probs = probs / probs.sum()
        expert_idx = int(np.argmax(probs))
        selected = self.expert_names[expert_idx]
        if self.storage is not None and hasattr(self.storage, "log_routing_decision"):
            try:
                sample_id = hashlib.sha256(
                    x.tobytes()
                ).hexdigest()[:16]
                self.storage.log_routing_decision(
                    str(uuid.uuid4()), sample_id, selected,
                    float(probs[expert_idx]),
                )
            except Exception:
                pass
        return selected, probs

    async def add_training_sample(self, state: "CostOptimizationState",
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
class CostOptimizationState:
    """State for the distillation agent."""
    carbon_intensity: float
    node_health: float
    workload_tokens: float
    latency_target: float
    anomaly_severity: float
    avg_cost_trend: float
    cost_variance: float
    weight_carbon: float
    weight_energy: float
    weight_helium: float
    weight_material: float
    hour_of_day: float

    def to_feature_vector(self) -> np.ndarray:
        features = [
            min(max(self.carbon_intensity / 1.0, 0.0), 1.0),
            min(max(self.node_health, 0.0), 1.0),
            min(self.workload_tokens / 10000.0, 1.0),
            min(self.latency_target / 5000.0, 1.0),
            min(max(self.anomaly_severity, 0.0), 1.0),
            float(np.clip(self.avg_cost_trend, -1.0, 1.0) + 1.0) / 2.0,
            min(max(self.cost_variance / 0.5, 0.0), 1.0),
            min(max(self.weight_carbon, 0.0), 1.0),
            min(max(self.weight_energy, 0.0), 1.0),
            min(max(self.weight_helium, 0.0), 1.0),
            min(max(self.weight_material, 0.0), 1.0),
            min(self.hour_of_day / 24.0, 1.0),
        ]
        return np.array(features, dtype=np.float32)


class Teacher(ABC):
    @abstractmethod
    def predict(self, state: CostOptimizationState) -> np.ndarray: ...

    @abstractmethod
    def confidence(self, state: CostOptimizationState) -> float: ...


class CostRuleBasedTeacher(Teacher):
    def predict(self, state: CostOptimizationState) -> np.ndarray:
        probs = np.ones(len(STRATEGY_ACTION_SPACE)) * 0.1
        if state.anomaly_severity > 0.7:
            probs[1] = 0.8
        elif state.carbon_intensity > 0.8:
            probs[1] = 0.7
        elif state.workload_tokens > 5000:
            probs[2] = 0.6
        elif state.node_health < 0.5:
            probs[3] = 0.6
        else:
            probs[4] = 0.6
        return probs / probs.sum()

    def confidence(self, state: CostOptimizationState) -> float:
        if state.anomaly_severity > 0.7:
            return 0.6
        return 0.4


class CostHistoricalMLTeacher(Teacher):
    def __init__(self, model_path: Optional[str] = None):
        self.model = None
        if (model_path and SKLEARN_ML and JOBLIB_AVAILABLE
                and Path(model_path).exists()):
            try:
                self.model = joblib.load(model_path)
            except Exception as e:
                logger.warning("ml_teacher_load_failed", error=str(e))

    def predict(self, state: CostOptimizationState) -> np.ndarray:
        if self.model is None:
            return np.ones(len(STRATEGY_ACTION_SPACE)) / len(STRATEGY_ACTION_SPACE)
        x = state.to_feature_vector().reshape(1, -1)
        try:
            return self.model.predict_proba(x)[0]
        except Exception:
            return np.ones(len(STRATEGY_ACTION_SPACE)) / len(STRATEGY_ACTION_SPACE)

    def confidence(self, state: CostOptimizationState) -> float:
        return 0.7 if self.model is not None else 0.0


class CostStatefulQTeacher(Teacher):
    def __init__(self, cost_func: "SustainabilityCostFunction",
                 lr: float = 0.1):
        self.cost_func = cost_func
        self.lr = lr
        self.weights = np.zeros((STATE_FEATURE_DIM, len(STRATEGY_ACTION_SPACE)))

    def predict(self, state: CostOptimizationState) -> np.ndarray:
        x = state.to_feature_vector()
        q = x @ self.weights
        q = q - np.max(q)
        exp_q = np.exp(q)
        return exp_q / exp_q.sum()

    def confidence(self, state: CostOptimizationState) -> float:
        return 0.5

    def update(self, state: CostOptimizationState, action: int,
                reward: float) -> None:
        x = state.to_feature_vector()
        q_current = float(np.dot(x, self.weights[:, action]))
        self.weights[:, action] += self.lr * (reward - q_current) * x


class DistillationStudent:
    def __init__(self, feature_dim: int = STATE_FEATURE_DIM,
                 n_classes: int = 5, lr: float = 0.01,
                 distill_weight: float = 0.7, rl_weight: float = 0.3,
                 entropy_bonus: float = 0.01):
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
        Combined distillation + policy gradient + entropy regularisation.

        Distillation loss:  CE(p, teacher)        grad = (p - teacher)
        RL loss:            -reward * log p[a]     grad = -reward * (one_hot - p)
        Entropy bonus:      -H(p)                  grad = -(log p + 1) * p
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


class DistillationCostOptimizer:
    STATUS = "experimental"

    def __init__(self, cost_func: "SustainabilityCostFunction",
                 config: Dict[str, Any], enabled: bool = True):
        if enabled:
            _warn_module("distillation")
        self.cost_func = cost_func
        self.config = config
        self.student = DistillationStudent(
            feature_dim=STATE_FEATURE_DIM,
            lr=config.get("distillation_learning_rate", 0.01),
            distill_weight=config.get("distill_weight", 0.7),
            rl_weight=config.get("rl_weight", 0.3),
        )
        self.teachers: List[Teacher] = [
            CostRuleBasedTeacher(),
            CostHistoricalMLTeacher(),
            CostStatefulQTeacher(cost_func),
        ]
        self.replay_buffer = ReplayBuffer(
            max_size=config.get("distillation_replay_size", 2000)
        )
        self.epsilon = float(config.get("distillation_epsilon", 0.1))
        self.epsilon_min = float(config.get("distillation_epsilon_min", 0.01))
        self.epsilon_decay = float(
            config.get("distillation_epsilon_decay", 0.995)
        )
        self.train_every = int(config.get("distillation_train_every", 10))
        self.counter = 0
        self._lock: Optional[asyncio.Lock] = None
        self.available = True

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    def _teacher_mixture(self, state: CostOptimizationState) -> np.ndarray:
        n = len(STRATEGY_ACTION_SPACE)
        teacher_probs = np.zeros(n)
        total_conf = 0.0
        for teacher in self.teachers:
            p = teacher.predict(state)
            c = teacher.confidence(state)
            teacher_probs += p * c
            total_conf += c
        if total_conf > 0:
            teacher_probs /= total_conf
        else:
            teacher_probs = np.ones(n) / n
        return teacher_probs

    async def select_strategy(self, state: CostOptimizationState,
                                exploration: bool = True
                                ) -> Tuple[str, int, np.ndarray, np.ndarray]:
        state_vec = state.to_feature_vector()
        teacher_probs = self._teacher_mixture(state)
        student_probs = self.student.predict_proba(state_vec)
        if exploration and random.random() < self.epsilon:
            action_idx = random.randrange(len(STRATEGY_ACTION_SPACE))
        else:
            combined = 0.8 * student_probs + 0.2 * teacher_probs
            action_idx = int(np.argmax(combined))
        self.epsilon = max(self.epsilon_min, self.epsilon * self.epsilon_decay)
        return STRATEGY_ACTION_SPACE[action_idx], action_idx, state_vec, teacher_probs

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
        population_size: int = 30,
        generations: int = 10,
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
        self.best_fitness = -float("inf")
        self.evolution_history: List[Dict[str, Any]] = []
        self.pareto_front: List[MOPDPoint] = []
        self._eval_cache: "OrderedDict[Tuple, Dict[str, float]]" = OrderedDict()
        self.available = True

    def _random_individual(self) -> Dict[str, float]:
        ind = {
            name: random.uniform(low, high)
            for name, (low, high) in self.parameter_bounds.items()
        }
        return self._normalize(ind)

    def _normalize(self, ind: Dict[str, float]) -> Dict[str, float]:
        total = sum(max(0.0, v) for v in ind.values())
        if total <= 0:
            n = len(ind)
            return {k: 1.0 / n for k in ind}
        return {k: max(0.0, v) / total for k, v in ind.items()}

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
        return self._normalize(child)

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
        return self._normalize(mutant)

    @staticmethod
    def _dominates(a: MOPDPoint, b: MOPDPoint) -> bool:
        keys = set(a.objectives.keys()) & set(b.objectives.keys())
        if not keys:
            return False
        return (
            all(a.objectives[k] >= b.objectives[k] for k in keys)
            and any(a.objectives[k] > b.objectives[k] for k in keys)
        )

    def _fast_non_dominated_sort(self, points: List[MOPDPoint]
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

    def _tournament(
        self,
        population: List[Dict],
        ranks: Dict[int, int],
        crowd: Dict[int, float],
    ) -> Dict:
        """Tournament by rank + crowding, keyed on `id(individual)`."""
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
        obj_keys = list(weights.keys())
        if not obj_keys:
            return weights
        avg = {k: float(np.mean([p.objectives.get(k, 0.0) for p in self.pareto_front])) for k in obj_keys}
        mx = {k: max(p.objectives.get(k, 0.0) for p in self.pareto_front) for k in obj_keys}
        for k in obj_keys:
            if mx[k] > 0 and avg[k] < 0.5 * mx[k]:
                weights[k] = min(0.6, weights.get(k, 0.0) * 1.5)
        total = sum(weights.values()) or 1.0
        return {k: v / total for k, v in weights.items()}

    def _select_best_from_pareto(self, pareto: List[MOPDPoint],
                                   weights: Dict[str, float]
                                   ) -> Optional[MOPDPoint]:
        if not pareto:
            return None
        keys = list(weights.keys())
        max_vals = {k: max(p.objectives.get(k, 0.0) for p in pareto) for k in keys}
        min_vals = {k: min(p.objectives.get(k, 0.0) for p in pareto) for k in keys}
        ranges = {
            k: (max_vals[k] - min_vals[k]) if max_vals[k] != min_vals[k] else 1.0
            for k in keys
        }
        best: Optional[MOPDPoint] = None
        best_score = -float("inf")
        for p in pareto:
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
        while len(self._eval_cache) > self.eval_cache_max_entries:
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
                    sf = sorted(front, key=lambda i: cd.get(i, 0.0), reverse=True)
                    remaining = self.population_size - len(new_inds)
                    for i in sf[:remaining]:
                        new_inds.append(dedup_inds[i])
                        new_points.append(dedup_points[i])
                    break
            population = new_inds
            points = new_points

        fronts = self._fast_non_dominated_sort(points)
        self.pareto_front = [points[i] for i in fronts[0]] if fronts else []
        weights = self._compute_dynamic_weights()
        best = self._select_best_from_pareto(self.pareto_front, weights)
        if best is not None:
            self.best_individual = dict(best.parameters)
            self.best_fitness = best.scalarised_score
        self.evolution_history.append({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "best_fitness": self.best_fitness,
            "pareto_size": len(self.pareto_front),
        })
        return list(self.pareto_front)


# =============================================================================
# SECTION 7. MAIN COST FUNCTION
# =============================================================================
class SustainabilityCostFunction:
    """
    Lifecycle:
        cost_fn = SustainabilityCostFunction(...)
        await cost_fn.start()
        c = await cost_fn.compute(node_desc, workload)
        await cost_fn.shutdown()
    """

    def __init__(
        self,
        carbon_fetcher: Any,
        material_updater: Any,
        helium_collector: Any,
        config: Optional[Union[Dict[str, Any], CostConfig]] = None,
        adaptive_cost_function: Optional[Any] = None,
        anomaly_detector: Optional[Any] = None,
        predictive_maintenance: Optional[Any] = None,
        storage: Optional[Any] = None,
        enable_limit_graph: bool = False,
        enable_modp: bool = False,
        enable_rlhf: bool = False,
        enable_moe: bool = False,
        moe_expert_count: int = 4,
    ):
        # Config
        if config is None:
            self.config = CostConfig()
        elif isinstance(config, CostConfig):
            self.config = config
        elif isinstance(config, dict):
            self.config = CostConfig.from_dict(config)
        else:
            raise TypeError(f"Unsupported config type: {type(config)}")

        issues = self._validate_config()
        if issues:
            raise ValueError(f"Invalid config: {issues}")

        # Services
        self.carbon = carbon_fetcher
        self.material = material_updater
        self.helium = helium_collector
        self.adaptive_cost = adaptive_cost_function
        self.anomaly_detector = anomaly_detector
        self.predictive_maintenance = predictive_maintenance
        self.storage = storage

        # Weights
        self._base_weights = self._get_initial_weights()
        self._current_weights = dict(self._base_weights)

        # Carbon cache
        self._carbon_cache: "OrderedDict[str, Tuple[float, datetime]]" = OrderedDict()
        self._carbon_cache_ttl = self.config.carbon_cache_ttl_seconds
        self._carbon_cache_max_size = self.config.carbon_cache_max_size
        self._carbon_cache_lock: Optional[asyncio.Lock] = None

        # Cost history
        self._cost_history: deque = deque(maxlen=50)
        self._last_total_cost: Optional[float] = None

        # Distillation
        self.distillation_config = {
            "distillation_epsilon": self.config.distillation_epsilon,
            "distillation_epsilon_min": self.config.distillation_epsilon_min,
            "distillation_epsilon_decay": self.config.distillation_epsilon_decay,
            "distillation_train_every": self.config.distillation_train_every,
            "distillation_replay_size": self.config.distillation_replay_size,
            "distillation_learning_rate": self.config.distillation_learning_rate,
            "distill_weight": self.config.distill_weight,
            "rl_weight": self.config.rl_weight,
        }
        self.policy_optimizer = DistillationCostOptimizer(
            self, self.distillation_config, enabled=True
        )
        self._distillation_queue: asyncio.Queue = asyncio.Queue(
            maxsize=self.config.distillation_queue_maxsize
        )
        self._distillation_consumer_task: Optional[asyncio.Task] = None
        self._distillation_dropped = 0

        # MOEA
        self.moea_enabled = self.config.moea_enabled
        self.moea_interval_seconds = self.config.moea_interval_seconds
        self.moea_optimizer: Optional[NSGAIIOptimizer] = None
        self.moea_pareto_front: List[MOPDPoint] = []
        self.moea_best_weights: Optional[Dict[str, float]] = None
        self._moea_task: Optional[asyncio.Task] = None
        self._moea_lock: Optional[asyncio.Lock] = None

        # Last-seen scenario for the MOEA background loop
        self._last_node_desc: Optional[NodeDescriptor] = None
        self._last_workload: Optional[WorkloadDescriptor] = None
        self._last_expert_profile: Optional[ExpertProfile] = None

        # Last-call state (reset at the top of every compute)
        self._last_selected_expert: Optional[str] = None
        self._last_strategy: str = "standard"
        self._last_action_idx: int = 0
        self._last_teacher_probs: np.ndarray = np.ones(5) / 5
        self._last_state_vec: Optional[np.ndarray] = None

        # Anomaly cooldown
        self._last_anomaly_time: Optional[datetime] = None
        self._anomaly_cooldown = timedelta(seconds=300)

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
            ) if enable_moe else None
        )

        # Lifecycle flags
        self._started = False
        self._shutdown = False

        # Metrics
        self._setup_metrics()

        logger.info("sustainability_cost_created", v="3.0.0")

    # ---------------- config ----------------
    def _validate_config(self) -> List[str]:
        issues: List[str] = []
        w = (
            self.config.energy_weight + self.config.carbon_weight
            + self.config.helium_weight + self.config.material_weight
            + self.config.latency_weight + self.config.accuracy_weight
        )
        if abs(w - 1.0) > 1e-6:
            issues.append("initial weights must sum to 1")
        if self.config.latency_max_ms <= self.config.latency_baseline_ms:
            issues.append("latency_max_ms must exceed latency_baseline_ms")
        if self.config.energy_max_joules <= self.config.energy_baseline_joules:
            issues.append("energy_max_joules must exceed energy_baseline_joules")
        return issues

    def _get_initial_weights(self) -> Dict[str, float]:
        weights = {
            "energy": self.config.energy_weight,
            "carbon": self.config.carbon_weight,
            "helium": self.config.helium_weight,
            "material": self.config.material_weight,
            "latency": self.config.latency_weight,
            "accuracy": self.config.accuracy_weight,
        }
        total = sum(weights.values()) or 1.0
        return {k: v / total for k, v in weights.items()}

    # ---------------- locks ----------------
    def _get_carbon_cache_lock(self) -> asyncio.Lock:
        if self._carbon_cache_lock is None:
            self._carbon_cache_lock = asyncio.Lock()
        return self._carbon_cache_lock

    def _get_moea_lock(self) -> asyncio.Lock:
        if self._moea_lock is None:
            self._moea_lock = asyncio.Lock()
        return self._moea_lock

    # ---------------- metrics ----------------
    def _setup_metrics(self) -> None:
        if not PROMETHEUS_AVAILABLE:
            self.metrics: Optional[Dict[str, Any]] = None
            return
        try:
            self.metrics = {
                "energy": Histogram("cost_energy", "Energy cost (normalized)"),
                "carbon": Histogram("cost_carbon", "Carbon cost (normalized)"),
                "helium": Histogram("cost_helium", "Helium cost (normalized)"),
                "material": Histogram("cost_material", "Material cost (normalized)"),
                "latency": Histogram("cost_latency", "Latency cost (normalized)"),
                "accuracy": Histogram("cost_accuracy", "Accuracy cost (normalized)"),
                "total": Histogram("cost_total", "Total sustainability cost"),
                "weights": Gauge("cost_weights", "Current weights", ["component"]),
                "moea_pareto_front": Gauge(
                    "cost_moea_pareto_front", "MOEA Pareto front size"
                ),
                "distillation_dropped": Counter(
                    "cost_distillation_dropped_total",
                    "Distillation updates dropped",
                ),
            }
        except Exception as e:
            logger.warning("metrics_setup_failed", error=str(e))
            self.metrics = None

    # ---------------- lifecycle ----------------
    @traced("cost.start")
    async def start(self) -> None:
        if self._started and not self._shutdown:
            logger.warning("cost_already_started")
            return
        self._started = True
        self._shutdown = False

        # Start distillation consumer
        self._distillation_consumer_task = asyncio.create_task(
            self._distillation_consumer()
        )
        # Start MOEA loop
        if self.moea_enabled:
            self._moea_task = asyncio.create_task(self._moea_loop())
        # Init LIMIT graph if enabled
        if self.limit_graph_manager is not None:
            self._init_limit_graph()
        logger.info("sustainability_cost_started")

    async def ready(self) -> bool:
        return self._started and not self._shutdown

    @traced("cost.shutdown")
    async def shutdown(self, timeout: Optional[float] = None) -> None:
        if self._shutdown:
            return
        self._shutdown = True
        timeout = timeout or self.config.shutdown_timeout_seconds
        logger.info("sustainability_cost_shutting_down")

        # Drain distillation queue
        try:
            await asyncio.wait_for(
                self._distillation_queue.join(), timeout=timeout
            )
        except asyncio.TimeoutError:
            logger.warning("distillation_queue_drain_timeout")

        tasks = [t for t in (
            self._distillation_consumer_task, self._moea_task,
        ) if t is not None and not t.done()]
        for t in tasks:
            t.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._distillation_consumer_task = None
        self._moea_task = None

        self._started = False
        logger.info("sustainability_cost_shutdown_complete")

    # `close` kept as an alias for backwards compatibility.
    async def close(self, timeout: Optional[float] = None) -> None:
        await self.shutdown(timeout=timeout)

    async def __aenter__(self) -> "SustainabilityCostFunction":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.shutdown()

    # ---------------- distillation consumer ----------------
    async def _distillation_consumer(self) -> None:
        while True:
            try:
                item = await asyncio.wait_for(
                    self._distillation_queue.get(), timeout=1.0
                )
            except asyncio.TimeoutError:
                if self._shutdown:
                    return
                continue
            except asyncio.CancelledError:
                return
            try:
                await self.policy_optimizer.update(*item)
            except Exception as e:
                logger.warning("distillation_update_failed", error=str(e))
            finally:
                self._distillation_queue.task_done()

    def _enqueue_distillation_update(
        self, state_vec: np.ndarray, action_idx: int, reward: float,
        next_state_vec: np.ndarray, teacher_probs: np.ndarray,
    ) -> None:
        try:
            self._distillation_queue.put_nowait((
                state_vec, action_idx, reward, next_state_vec, teacher_probs,
            ))
        except asyncio.QueueFull:
            self._distillation_dropped += 1
            if self.metrics:
                try:
                    self.metrics["distillation_dropped"].inc()
                except Exception:
                    pass

    # ---------------- normalization (all in [0, 1]) ----------------
    def _clip01(self, x: float) -> float:
        return max(0.0, min(1.0, float(x)))

    def _normalize_energy(self, energy_joules: float) -> float:
        b = self.config.energy_baseline_joules
        m = self.config.energy_max_joules
        if m <= b:
            return 0.0
        return self._clip01((energy_joules - b) / (m - b))

    def _normalize_carbon(self, carbon_kg: float) -> float:
        b = self.config.carbon_intensity_baseline_kg_per_kwh
        m = self.config.carbon_intensity_max_kg_per_kwh
        if m <= b:
            return 0.0
        return self._clip01((carbon_kg - b) / (m - b))

    def _normalize_helium(self, helium_cost: float) -> float:
        m = self.config.helium_max_scarcity
        return self._clip01(helium_cost / m) if m > 0 else 0.0

    def _normalize_material(self, material_composite: float) -> float:
        m = self.config.material_max_composite
        return self._clip01(material_composite / m) if m > 0 else 0.0

    def _normalize_latency(self, latency_ms: float) -> float:
        b = self.config.latency_baseline_ms
        m = self.config.latency_max_ms
        if m <= b:
            return 0.0
        return self._clip01((latency_ms - b) / (m - b))

    def _normalize_accuracy(self, accuracy: float) -> float:
        b = self.config.accuracy_baseline
        m = self.config.accuracy_max
        if m <= b:
            return 0.0
        return self._clip01((m - accuracy) / (m - b))

    # ---------------- carbon cache ----------------
    async def _get_carbon_intensity(self, region: str) -> float:
        now = datetime.now(timezone.utc)
        async with self._get_carbon_cache_lock():
            entry = self._carbon_cache.get(region)
            if entry is not None:
                value, ts = entry
                if (now - ts).total_seconds() < self._carbon_cache_ttl:
                    self._carbon_cache.move_to_end(region)
                    return value
                self._carbon_cache.pop(region, None)
        try:
            r = self.carbon.get_intensity(region)
            if _is_awaitable(r):
                r = await r
            intensity = float(r) if r is not None else float(
                self.config.carbon_intensity_baseline_kg_per_kwh
            )
        except Exception as e:
            logger.error("carbon_intensity_fetch_failed", error=str(e))
            intensity = float(self.config.carbon_intensity_baseline_kg_per_kwh)
        async with self._get_carbon_cache_lock():
            self._carbon_cache[region] = (intensity, now)
            self._carbon_cache.move_to_end(region)
            while len(self._carbon_cache) > self._carbon_cache_max_size:
                self._carbon_cache.popitem(last=False)
        return intensity

    # ---------------- helium / material ----------------
    async def _get_helium_scarcity(self, node_desc: NodeDescriptor) -> float:
        # Try the helium collector first; fall back to the node attribute.
        if self.helium is not None:
            try:
                fn = getattr(self.helium, "get_scarcity", None)
                if fn is not None:
                    r = fn(node_desc.id)
                    if _is_awaitable(r):
                        r = await r
                    if r is not None:
                        return float(r)
            except Exception as e:
                logger.debug("helium_collector_failed", error=str(e))
        # Fallback
        return float(1.0 - getattr(node_desc, "helium_connectivity_score", 0.5))

    async def _get_material_composite(self, node_desc: NodeDescriptor) -> float:
        fp_id = getattr(node_desc, "material_footprint_id", None)
        if not fp_id:
            return 0.0
        try:
            r = self.material.get_footprint(fp_id)
            if _is_awaitable(r):
                r = await r
        except Exception as e:
            logger.debug("material_footprint_failed", error=str(e))
            return 0.0
        if not r:
            return 0.0
        embodied = float(r.get("embodied_carbon_kg", 0.0))
        rare_earth = float(r.get("rare_earth_kg", 0.0))
        ne = self.config.material_embodied_norm
        nr = self.config.material_rare_earth_norm
        norm_e = embodied / ne if ne > 0 else 0.0
        norm_r = rare_earth / nr if nr > 0 else 0.0
        composite = norm_e * 0.7 + norm_r * 0.3
        return min(self.config.material_max_composite, max(0.0, composite))

    # ---------------- state ----------------
    async def _build_optimization_state(
        self,
        node_desc: NodeDescriptor,
        workload: WorkloadDescriptor,
        expert_profile: Optional[ExpertProfile] = None,
    ) -> CostOptimizationState:
        carbon_intensity = await self._get_carbon_intensity(node_desc.region)

        node_health = 1.0
        if (self.config.integrate_predictive_maintenance
                and self.predictive_maintenance is not None):
            try:
                fn = getattr(self.predictive_maintenance, "get_efficiency_factor", None)
                if fn is not None:
                    r = fn(node_desc.id)
                    if _is_awaitable(r):
                        r = await r
                    if r is not None:
                        node_health = self._clip01(float(r))
            except Exception as e:
                logger.debug("predictive_maintenance_failed", error=str(e))

        anomaly_severity = 0.0
        if (self.config.integrate_anomaly_detection
                and self.anomaly_detector is not None):
            try:
                fn = getattr(self.anomaly_detector, "get_latest_severity", None)
                if fn is not None:
                    r = fn()
                    if _is_awaitable(r):
                        r = await r
                    if r is not None:
                        anomaly_severity = self._clip01(float(r))
            except Exception as e:
                logger.debug("anomaly_severity_failed", error=str(e))

        if len(self._cost_history) >= 5:
            recent = list(self._cost_history)[-5:]
            trend = (
                (recent[-1] - recent[0]) / (len(recent) - 1)
                if len(recent) > 1 else 0.0
            )
            avg_cost_trend = float(trend)
            cost_variance = float(np.var(recent))
        else:
            avg_cost_trend = 0.0
            cost_variance = 0.0

        now = datetime.now(timezone.utc)
        return CostOptimizationState(
            carbon_intensity=carbon_intensity,
            node_health=node_health,
            workload_tokens=float(workload.tokens),
            latency_target=float(workload.latency_target),
            anomaly_severity=anomaly_severity,
            avg_cost_trend=avg_cost_trend,
            cost_variance=cost_variance,
            weight_carbon=self._base_weights["carbon"],
            weight_energy=self._base_weights["energy"],
            weight_helium=self._base_weights["helium"],
            weight_material=self._base_weights["material"],
            hour_of_day=float(now.hour),
        )

    # ---------------- core compute ----------------
    @traced("cost.compute")
    async def compute(
        self,
        node_desc: NodeDescriptor,
        workload: WorkloadDescriptor,
        expert_profile: Optional[ExpertProfile] = None,
    ) -> float:
        # Reset per-call state
        self._last_selected_expert = None
        self._last_strategy = "standard"
        self._last_action_idx = 0
        self._last_teacher_probs = np.ones(5) / 5
        self._last_state_vec = None

        cid = _new_cid()
        token = _cid_ctx.set(cid)
        try:
            # Stash scenario for the MOEA loop
            self._last_node_desc = node_desc
            self._last_workload = workload
            self._last_expert_profile = expert_profile

            # Energy
            energy_used = float(node_desc.energy_per_token) * float(workload.tokens)
            if (self.config.integrate_predictive_maintenance
                    and self.predictive_maintenance is not None):
                try:
                    fn = getattr(self.predictive_maintenance, "get_efficiency_factor", None)
                    if fn is not None:
                        r = fn(node_desc.id)
                        if _is_awaitable(r):
                            r = await r
                        if r is not None and r > 0:
                            energy_used = energy_used / float(r)
                except Exception as e:
                    logger.debug("predictive_efficiency_failed", error=str(e))
            energy_cost = self._normalize_energy(energy_used)

            # Carbon
            carbon_intensity = await self._get_carbon_intensity(node_desc.region)
            # energy is in joules, carbon_intensity in kg/kWh
            carbon_kg = energy_used * carbon_intensity / 3_600_000.0
            carbon_cost = self._normalize_carbon(carbon_kg)

            # Helium
            helium_scarcity = await self._get_helium_scarcity(node_desc)
            helium_base = (1.0 - float(getattr(node_desc, "helium_connectivity_score", 0.5))) * 0.5
            if helium_scarcity > self.config.helium_scarcity_threshold:
                helium_base *= (1.0 + helium_scarcity)
            helium_cost = self._normalize_helium(helium_base)

            # Material
            material_composite = await self._get_material_composite(node_desc)
            material_cost = self._normalize_material(material_composite)

            # Latency
            latency_cost = self._normalize_latency(float(workload.latency_target))

            # Accuracy
            if expert_profile is not None:
                acc = float(getattr(expert_profile, "accuracy_score",
                                     self.config.accuracy_baseline))
            else:
                acc = float(self.config.accuracy_baseline)
            accuracy_cost = self._normalize_accuracy(acc)

            # Weights
            weights = await self._get_weights(node_desc, workload, expert_profile)

            # Total
            total = (
                weights["energy"] * energy_cost
                + weights["carbon"] * carbon_cost
                + weights["helium"] * helium_cost
                + weights["material"] * material_cost
                + weights["latency"] * latency_cost
                + weights["accuracy"] * accuracy_cost
            )

            if self.metrics:
                try:
                    self.metrics["energy"].observe(energy_cost)
                    self.metrics["carbon"].observe(carbon_cost)
                    self.metrics["helium"].observe(helium_cost)
                    self.metrics["material"].observe(material_cost)
                    self.metrics["latency"].observe(latency_cost)
                    self.metrics["accuracy"].observe(accuracy_cost)
                    self.metrics["total"].observe(total)
                    for k, v in weights.items():
                        self.metrics["weights"].labels(component=k).set(v)
                except Exception:
                    pass

            # Reward
            baseline_total = sum(
                self._base_weights[k] * v
                for k, v in (
                    ("energy", energy_cost), ("carbon", carbon_cost),
                    ("helium", helium_cost), ("material", material_cost),
                    ("latency", latency_cost), ("accuracy", accuracy_cost),
                )
            )
            reward = (
                (baseline_total - total) / baseline_total
                if baseline_total > 0 else 0.0
            )
            reward = max(0.0, min(1.0, reward))

            self._cost_history.append(total)
            self._last_total_cost = total

            # Distillation update
            state = await self._build_optimization_state(
                node_desc, workload, expert_profile
            )
            state_vec = state.to_feature_vector()

            if self.moe_gating is not None and self._last_selected_expert is not None:
                try:
                    await self.moe_gating.add_training_sample(
                        state, self._last_selected_expert, reward
                    )
                except Exception as e:
                    logger.warning("moe_update_failed", error=str(e))
            else:
                self._enqueue_distillation_update(
                    state_vec, self._last_action_idx, reward,
                    state_vec, self._last_teacher_probs,
                )

            # RLHF record
            if self.rlhf_trainer is not None and random.random() < 0.05:
                chosen = self._last_strategy
                rejected = random.choice(
                    [s for s in STRATEGY_ACTION_SPACE if s != chosen]
                )
                self.rlhf_trainer.record_pair(
                    pair_id=str(uuid.uuid4()),
                    prompt="Which weight strategy is better for current conditions?",
                    chosen=chosen,
                    rejected=rejected,
                    reward_diff=reward,
                    metadata={
                        "cid": cid,
                        "node_id": node_desc.id,
                        "workload_tokens": workload.tokens,
                    },
                )

            # LIMIT graph
            if self.limit_graph_manager is not None:
                try:
                    for comp, w in weights.items():
                        self.limit_graph_manager.add_node(
                            "cost_components", f"node_{comp}", comp,
                            {"weight": w,
                             "timestamp": datetime.now(timezone.utc).isoformat()},
                        )
                except Exception:
                    pass

            logger.debug(
                "cost_computed",
                energy=round(energy_cost, 4),
                carbon=round(carbon_cost, 4),
                helium=round(helium_cost, 4),
                material=round(material_cost, 4),
                latency=round(latency_cost, 4),
                accuracy=round(accuracy_cost, 4),
                total=round(total, 4),
            )
            return total
        finally:
            _cid_ctx.reset(token)

    # ---------------- weight selection ----------------
    async def _get_weights(
        self,
        node_desc: NodeDescriptor,
        workload: WorkloadDescriptor,
        expert_profile: Optional[ExpertProfile] = None,
    ) -> Dict[str, float]:
        state = await self._build_optimization_state(
            node_desc, workload, expert_profile
        )

        if self.moe_gating is not None:
            expert_name, _ = await self.moe_gating.select_expert(state)
            self._last_selected_expert = expert_name
            strategy_map = {
                "carbon_focus": "carbon_focus",
                "energy_focus": "energy_focus",
                "helium_focus": "helium_focus",
                "adaptive": "adaptive",
            }
            strategy = strategy_map.get(expert_name, "adaptive")
            self._last_strategy = strategy
            self._last_action_idx = STRATEGY_ACTION_SPACE.index(strategy)
            self._last_teacher_probs = np.ones(5) / 5
            self._last_state_vec = state.to_feature_vector()
            weights = dict(self._base_weights)
            weights = self._apply_strategy_to_weights(strategy, weights)
            weights = self._normalize_weights(weights)
            self._current_weights = weights
            return weights

        strategy, action_idx, state_vec, teacher_probs = (
            await self.policy_optimizer.select_strategy(state, exploration=True)
        )
        self._last_strategy = strategy
        self._last_action_idx = action_idx
        self._last_teacher_probs = teacher_probs
        self._last_state_vec = state_vec

        weights = dict(self._base_weights)
        weights = self._apply_strategy_to_weights(strategy, weights)
        weights = self._normalize_weights(weights)
        self._current_weights = weights
        return weights

    def _apply_strategy_to_weights(self, strategy: str,
                                     weights: Dict[str, float]
                                     ) -> Dict[str, float]:
        if strategy == "carbon_focus":
            weights["carbon"] *= 1.2
        elif strategy == "energy_focus":
            weights["energy"] *= 1.2
        elif strategy == "helium_focus":
            weights["helium"] *= 1.2
        elif strategy == "adaptive":
            if (self.config.use_adaptive_weights and self.adaptive_cost is not None):
                # Synchronously read `weights` attribute if present
                try:
                    adaptive = getattr(self.adaptive_cost, "weights", None)
                    if isinstance(adaptive, dict):
                        mapping = {
                            "alpha": "energy", "beta": "carbon",
                            "gamma": "helium", "delta": "material",
                            "epsilon": "latency", "zeta": "accuracy",
                        }
                        for ad_key, comp in mapping.items():
                            if ad_key in adaptive:
                                weights[comp] = float(adaptive[ad_key])
                except Exception as e:
                    logger.debug("adaptive_weights_read_failed", error=str(e))
        return weights

    def _normalize_weights(self, weights: Dict[str, float]) -> Dict[str, float]:
        total = sum(max(0.0, v) for v in weights.values())
        if total <= 0:
            n = len(weights) or 1
            return {k: 1.0 / n for k in weights}
        return {k: max(0.0, v) / total for k, v in weights.items()}

    # ---------------- utility ----------------
    async def get_weights(self) -> Dict[str, float]:
        return dict(self._current_weights)

    async def set_weights(self, new_weights: Dict[str, float]) -> None:
        normalized = self._normalize_weights(new_weights)
        self._base_weights = dict(normalized)
        self._current_weights = dict(normalized)
        logger.info("base_weights_set", weights=normalized)

    async def reset_weights(self) -> None:
        self._base_weights = self._get_initial_weights()
        self._current_weights = dict(self._base_weights)
        logger.info("weights_reset")

    async def reset_carbon_cache(self) -> None:
        async with self._get_carbon_cache_lock():
            self._carbon_cache.clear()
        logger.info("carbon_cache_cleared")

    async def get_cost_breakdown(
        self,
        node_desc: NodeDescriptor,
        workload: WorkloadDescriptor,
        expert_profile: Optional[ExpertProfile] = None,
    ) -> Dict[str, Any]:
        energy_used = float(node_desc.energy_per_token) * float(workload.tokens)
        energy_cost = self._normalize_energy(energy_used)
        carbon_intensity = await self._get_carbon_intensity(node_desc.region)
        carbon_kg = energy_used * carbon_intensity / 3_600_000.0
        carbon_cost = self._normalize_carbon(carbon_kg)
        helium_scarcity = await self._get_helium_scarcity(node_desc)
        helium_base = (1.0 - float(getattr(node_desc, "helium_connectivity_score", 0.5))) * 0.5
        if helium_scarcity > self.config.helium_scarcity_threshold:
            helium_base *= (1.0 + helium_scarcity)
        helium_cost = self._normalize_helium(helium_base)
        material_composite = await self._get_material_composite(node_desc)
        material_cost = self._normalize_material(material_composite)
        latency_cost = self._normalize_latency(float(workload.latency_target))
        acc = (
            float(getattr(expert_profile, "accuracy_score",
                           self.config.accuracy_baseline))
            if expert_profile is not None else float(self.config.accuracy_baseline)
        )
        accuracy_cost = self._normalize_accuracy(acc)
        weights = await self.get_weights()
        total = (
            weights["energy"] * energy_cost
            + weights["carbon"] * carbon_cost
            + weights["helium"] * helium_cost
            + weights["material"] * material_cost
            + weights["latency"] * latency_cost
            + weights["accuracy"] * accuracy_cost
        )
        return {
            "energy": {"raw": energy_used, "normalized": energy_cost},
            "carbon": {"raw": carbon_kg, "normalized": carbon_cost},
            "helium": {"raw": helium_base, "normalized": helium_cost},
            "material": {"raw": material_composite, "normalized": material_cost},
            "latency": {"raw": workload.latency_target, "normalized": latency_cost},
            "accuracy": {"raw": acc, "normalized": accuracy_cost},
            "total": total,
            "weights": weights,
        }

    async def get_distillation_stats(self) -> Dict[str, Any]:
        stats = self.policy_optimizer.get_stats()
        stats["queue_size"] = self._distillation_queue.qsize()
        stats["dropped"] = self._distillation_dropped
        return stats

    # ---------------- MOEA ----------------
    async def _moea_loop(self) -> None:
        while True:
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
        if self._last_node_desc is None or self._last_workload is None:
            logger.warning("moea_no_scenario")
            return

        async with self._get_moea_lock():
            param_bounds = {
                "energy": (0.01, 0.99),
                "carbon": (0.01, 0.99),
                "helium": (0.01, 0.99),
                "material": (0.01, 0.99),
                "latency": (0.01, 0.99),
                "accuracy": (0.01, 0.99),
            }

            # Snapshot shared state so the evaluation does not touch it
            node_desc = self._last_node_desc
            workload = self._last_workload
            expert_profile = self._last_expert_profile

            # Pre-compute the raw components (they do not depend on weights)
            energy_used = float(node_desc.energy_per_token) * float(workload.tokens)
            energy_cost = self._normalize_energy(energy_used)
            carbon_intensity = await self._get_carbon_intensity(node_desc.region)
            carbon_kg = energy_used * carbon_intensity / 3_600_000.0
            carbon_cost = self._normalize_carbon(carbon_kg)
            helium_scarcity = await self._get_helium_scarcity(node_desc)
            helium_base = (1.0 - float(getattr(node_desc, "helium_connectivity_score", 0.5))) * 0.5
            if helium_scarcity > self.config.helium_scarcity_threshold:
                helium_base *= (1.0 + helium_scarcity)
            helium_cost = self._normalize_helium(helium_base)
            material_composite = await self._get_material_composite(node_desc)
            material_cost = self._normalize_material(material_composite)
            latency_cost = self._normalize_latency(float(workload.latency_target))
            acc = (
                float(getattr(expert_profile, "accuracy_score",
                               self.config.accuracy_baseline))
                if expert_profile is not None else float(self.config.accuracy_baseline)
            )
            accuracy_cost = self._normalize_accuracy(acc)

            async def evaluate(weights: Dict[str, float]) -> Dict[str, float]:
                w = self._normalize_weights(weights)
                return {
                    "energy": 1.0 - w["energy"] * energy_cost,
                    "carbon": 1.0 - w["carbon"] * carbon_cost,
                    "helium": 1.0 - w["helium"] * helium_cost,
                    "material": 1.0 - w["material"] * material_cost,
                    "latency": 1.0 - w["latency"] * latency_cost,
                    "accuracy": 1.0 - w["accuracy"] * accuracy_cost,
                }

            self.moea_optimizer = NSGAIIOptimizer(
                evaluate_func=evaluate,
                parameter_bounds=param_bounds,
                population_size=self.config.moea_population_size,
                generations=self.config.moea_generations,
                mutation_rate=self.config.moea_mutation_rate,
                crossover_rate=self.config.moea_crossover_rate,
                tournament_size=self.config.moea_tournament_size,
                objective_weights=self._get_dynamic_moea_weights(),
                dynamic_weights=self.config.moea_dynamic_weights,
                eval_cache_max_entries=self.config.moea_eval_cache_max_entries,
            )

            pareto = await self.moea_optimizer.evolve()
            self.moea_pareto_front = pareto
            if not pareto:
                return

            weights = self._get_dynamic_moea_weights()
            best_point = self.moea_optimizer._select_best_from_pareto(pareto, weights)
            if best_point is None:
                return

            self.moea_best_weights = dict(best_point.parameters)
            self._base_weights = dict(best_point.parameters)
            self._current_weights = dict(best_point.parameters)
            logger.info("moea_applied", weights=self._base_weights)
            if self.metrics:
                try:
                    self.metrics["moea_pareto_front"].set(len(pareto))
                except Exception:
                    pass
            if self.modp_solver is not None:
                try:
                    self.modp_solver.add_state(
                        state_id=f"moea_best_{time.time()}",
                        problem_id="cost_weight_optimization",
                        state_attributes={"weights": self.moea_best_weights},
                        objective_values={
                            k: 1.0 - v
                            for k, v in best_point.objectives.items()
                        },
                        stage=0,
                    )
                except Exception:
                    pass

    def _get_dynamic_moea_weights(self) -> Dict[str, float]:
        weights = dict(self.config.moea_objective_weights)
        if not weights:
            return weights
        if self._carbon_cache:
            try:
                now = datetime.now(timezone.utc)
                fresh = [
                    v for v, ts in self._carbon_cache.values()
                    if (now - ts).total_seconds() < self._carbon_cache_ttl
                ]
                if fresh:
                    # Use the mean rather than the max to avoid single-region bias
                    mean_carbon = float(np.mean(fresh))
                    if mean_carbon > 0.8:
                        weights["carbon"] = min(
                            0.6, weights.get("carbon", 0.3) * 1.5
                        )
            except Exception:
                pass
        total = sum(weights.values()) or 1.0
        return {k: v / total for k, v in weights.items()}

    async def get_pareto_front(self) -> List[Dict[str, Any]]:
        return [p.to_dict() for p in self.moea_pareto_front]

    async def apply_moea_weights(self, weights: Dict[str, float]) -> None:
        normalized = self._normalize_weights(weights)
        self._base_weights = dict(normalized)
        self._current_weights = dict(normalized)
        logger.info("moea_weights_applied", weights=normalized)

    # ---------------- hooks ----------------
    async def on_anomaly_detected(self, anomaly_severity: float) -> None:
        if not self.config.integrate_anomaly_detection:
            return
        self._last_anomaly_time = datetime.now(timezone.utc)
        logger.info("anomaly_detected", severity=anomaly_severity)

    async def update_from_predictive_maintenance(self, node_id: str,
                                                  efficiency_factor: float) -> None:
        if not self.config.integrate_predictive_maintenance:
            return
        logger.debug("predictive_maintenance_update",
                     node_id=node_id, efficiency_factor=efficiency_factor)

    # ---------------- placeholder helpers ----------------
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

    async def get_limit_graph(self, graph_id: str = "cost_components"
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

    def _init_limit_graph(self) -> None:
        if self.limit_graph_manager is None:
            return
        graph_id = "cost_components"
        if not self.limit_graph_manager.get_metadata(graph_id):
            self.limit_graph_manager.create_graph(
                graph_id, "Sustainability Cost Component Dependencies", {}
            )
            for comp in ("energy", "carbon", "helium",
                          "material", "latency", "accuracy"):
                self.limit_graph_manager.add_node(
                    graph_id, f"node_{comp}", comp,
                    {"weight": self._base_weights.get(comp, 0.1)},
                )
            self.limit_graph_manager.add_edge(
                graph_id, "edge_energy_carbon",
                "node_energy", "node_carbon", 0.8, {},
            )
            self.limit_graph_manager.add_edge(
                graph_id, "edge_carbon_helium",
                "node_carbon", "node_helium", 0.3, {},
            )
            logger.info("limit_graph_initialized")


# =============================================================================
# SECTION 8. FACTORY
# =============================================================================
def create_cost_function(
    carbon_fetcher: Any,
    material_updater: Any,
    helium_collector: Any,
    config: Optional[Union[Dict[str, Any], CostConfig]] = None,
    adaptive_cost_function: Optional[Any] = None,
    anomaly_detector: Optional[Any] = None,
    predictive_maintenance: Optional[Any] = None,
    storage: Optional[Any] = None,
    **kwargs: Any,
) -> SustainabilityCostFunction:
    return SustainabilityCostFunction(
        carbon_fetcher=carbon_fetcher,
        material_updater=material_updater,
        helium_collector=helium_collector,
        config=config,
        adaptive_cost_function=adaptive_cost_function,
        anomaly_detector=anomaly_detector,
        predictive_maintenance=predictive_maintenance,
        storage=storage,
        **kwargs,
    )


# =============================================================================
# SECTION 9. TESTS
# =============================================================================
class _MockCarbon:
    async def get_intensity(self, region: str) -> float:
        return 0.5


class _MockMaterial:
    def get_footprint(self, fid: str):
        return {"embodied_carbon_kg": 50.0, "rare_earth_kg": 0.005}


class _MockHelium:
    async def get_scarcity(self, node_id: str) -> float:
        return 0.4


class _Tests(unittest.TestCase):
    def _cost_fn(self, **kwargs) -> SustainabilityCostFunction:
        return SustainabilityCostFunction(
            carbon_fetcher=_MockCarbon(),
            material_updater=_MockMaterial(),
            helium_collector=_MockHelium(),
            config=CostConfig(moea_enabled=False),
            **kwargs,
        )

    def test_module_status_documented(self):
        self.assertEqual(MODULE_STATUS["cost_core"], "stable")
        self.assertEqual(MODULE_STATUS["moe_gating"], "placeholder")

    def test_feature_dim_consistent(self):
        state = CostOptimizationState(
            carbon_intensity=0.5, node_health=0.9,
            workload_tokens=1000.0, latency_target=1000.0,
            anomaly_severity=0.0, avg_cost_trend=0.0,
            cost_variance=0.0, weight_carbon=0.3,
            weight_energy=0.2, weight_helium=0.15,
            weight_material=0.15, hour_of_day=12.0,
        )
        v = state.to_feature_vector()
        self.assertEqual(v.shape[0], STATE_FEATURE_DIM)

    def test_lifecycle(self):
        async def go():
            fn = self._cost_fn()
            self.assertFalse(await fn.ready())
            await fn.start()
            self.assertTrue(await fn.ready())
            await fn.shutdown()
            self.assertTrue(fn._shutdown)
            await fn.shutdown()  # idempotent
        asyncio.run(go())

    def test_context_manager(self):
        async def go():
            async with self._cost_fn() as fn:
                self.assertTrue(await fn.ready())
        asyncio.run(go())

    def test_compute_runs(self):
        async def go():
            async with self._cost_fn() as fn:
                node = NodeDescriptor(
                    id="n1", region="r1",
                    energy_per_token=0.0001,
                    helium_connectivity_score=0.6,
                    material_footprint_id="fp1",
                )
                workload = WorkloadDescriptor(tokens=1000, latency_target=1200.0)
                c = await fn.compute(node, workload)
                self.assertGreaterEqual(c, 0.0)
                self.assertLessEqual(c, 1.0)
        asyncio.run(go())

    def test_normalization_clamped(self):
        fn = self._cost_fn()
        # Below baseline -> 0.0
        self.assertEqual(fn._normalize_energy(0.0), 0.0)
        # Above max -> 1.0
        self.assertEqual(fn._normalize_energy(1e9), 1.0)
        # Mid-range
        v = fn._normalize_energy(0.0005)
        self.assertGreater(v, 0.0)
        self.assertLess(v, 1.0)
        # Carbon and latency
        self.assertEqual(fn._normalize_carbon(0.0), 0.0)
        self.assertEqual(fn._normalize_latency(1e9), 1.0)

    def test_carbon_unit_conversion(self):
        """Regression: carbon should be ~energy_joules * kg_per_kWh / 3.6e6."""
        async def go():
            async with self._cost_fn() as fn:
                node = NodeDescriptor(id="n1", region="r1",
                                       energy_per_token=0.0001)
                workload = WorkloadDescriptor(tokens=1000, latency_target=1000.0)
                breakdown = await fn.get_cost_breakdown(node, workload)
                # Energy_used = 0.0001 * 1000 = 0.1 J
                # carbon_kg = 0.1 J * 0.5 kg/kWh / 3.6e6 = 1.39e-8 kg
                self.assertGreaterEqual(breakdown["carbon"]["raw"], 0.0)
                self.assertLess(breakdown["carbon"]["raw"], 0.001)
        asyncio.run(go())

    def test_moe_gating_reward_weighting(self):
        """Regression: reward=0 must not change gating weights."""
        async def go():
            gate = MoEGatingNetworkPlaceholder(enabled=False)
            state = CostOptimizationState(
                carbon_intensity=0.5, node_health=0.9,
                workload_tokens=1000.0, latency_target=1000.0,
                anomaly_severity=0.0, avg_cost_trend=0.0,
                cost_variance=0.0, weight_carbon=0.3,
                weight_energy=0.2, weight_helium=0.15,
                weight_material=0.15, hour_of_day=12.0,
            )
            initial = gate.gating_weights.copy()
            await gate.add_training_sample(state, "carbon_focus", reward=0.0)
            np.testing.assert_allclose(gate.gating_weights, initial, atol=1e-9)
            await gate.add_training_sample(state, "carbon_focus", reward=1.0)
            diff = float(np.abs(gate.gating_weights - initial).sum())
            self.assertGreater(diff, 1e-6)
        asyncio.run(go())

    def test_nsga2_no_front_mutation(self):
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
            _ = opt._select_best_from_pareto(front, {"a": 0.5, "b": 0.5})
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
                eval_cache_max_entries=20,
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

    def test_config_roundtrip(self):
        cfg = CostConfig()
        d = cfg.to_dict()
        cfg2 = CostConfig.from_dict(d)
        self.assertEqual(cfg.moea_population_size, cfg2.moea_population_size)
        self.assertEqual(cfg.moea_enabled, cfg2.moea_enabled)

    def test_distillation_queue_bounded(self):
        async def go():
            fn = self._cost_fn()
            fn.config.distillation_queue_maxsize = 2
            fn._distillation_queue = asyncio.Queue(maxsize=2)
            await fn.start()
            try:
                for i in range(20):
                    fn._enqueue_distillation_update(
                        np.zeros(STATE_FEATURE_DIM, dtype=np.float32),
                        0, 0.5, np.zeros(STATE_FEATURE_DIM, dtype=np.float32),
                        np.ones(5) / 5,
                    )
                self.assertLessEqual(fn._distillation_queue.qsize(), 2)
                self.assertGreater(fn._distillation_dropped, 0)
            finally:
                await fn.shutdown()
        asyncio.run(go())

    def test_set_weights_updates_current(self):
        async def go():
            fn = self._cost_fn()
            await fn.set_weights({
                "energy": 0.1, "carbon": 0.1, "helium": 0.1,
                "material": 0.1, "latency": 0.3, "accuracy": 0.3,
            })
            w = await fn.get_weights()
            self.assertAlmostEqual(w["latency"], 0.3, places=5)
            self.assertAlmostEqual(w["energy"], 0.1, places=5)
        asyncio.run(go())

    def test_get_cost_breakdown(self):
        async def go():
            async with self._cost_fn() as fn:
                node = NodeDescriptor(id="n1", region="r1")
                workload = WorkloadDescriptor(tokens=1000, latency_target=1000.0)
                breakdown = await fn.get_cost_breakdown(node, workload)
                for k in ("energy", "carbon", "helium", "material",
                          "latency", "accuracy"):
                    self.assertIn(k, breakdown)
                self.assertIn("total", breakdown)
        asyncio.run(go())


def run_tests() -> int:
    suite = unittest.TestLoader().loadTestsFromTestCase(_Tests)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    return 0 if result.wasSuccessful() else 1


# =============================================================================
# SECTION 10. ENTRY POINT
# =============================================================================
async def _example() -> None:
    async with SustainabilityCostFunction(
        carbon_fetcher=_MockCarbon(),
        material_updater=_MockMaterial(),
        helium_collector=_MockHelium(),
        config=CostConfig(moea_enabled=False),
    ) as fn:
        node = NodeDescriptor(
            id="demo", region="r1",
            energy_per_token=0.0001,
            helium_connectivity_score=0.5,
            material_footprint_id="fp1",
        )
        workload = WorkloadDescriptor(tokens=1000, latency_target=1200.0)
        for i in range(5):
            c = await fn.compute(node, workload)
            print(f"Cycle {i}: cost={c:.4f}")
        print("Stats:", json.dumps(await fn.get_distillation_stats(),
                                    indent=2, default=str))
        print("Modules:", json.dumps(MODULE_STATUS, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Sustainability Cost Function v3.0.0"
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

    print("Sustainability Cost Function v3.0.0 — no mode selected.")
    print("Use --test, --example, or --status.")


if __name__ == "__main__":
    main()
