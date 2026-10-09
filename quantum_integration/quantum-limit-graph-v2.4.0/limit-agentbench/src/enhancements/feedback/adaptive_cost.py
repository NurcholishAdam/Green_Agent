#!/usr/bin/env python3
"""
Adaptive Cost Function with Two-Tier Updates + MOEA + LIMIT Graph + MODP + RLHF + MoE
=======================================================================================
Enhanced v2.5.0

- Online: fast exponential moving average for immediate routing.
- Offline: batched, validated updates for long-term policy weights.
- MOEA: NSGA-II evolves a Pareto front of weight vectors with a real
  policy-based objective function (no self-referential objectives).
- LIMIT Graph: tracks relationships between weight vectors.
- MODP: persists Pareto points and selected weight vectors.
- RLHF: collects human preference pairs.
- MoE: gating network blends online / offline / rule-based weight vectors
  with REINFORCE updates that use the observed reward.

FIXES OVER v2.2:
- Relative imports wrapped with fallbacks.
- `asyncio.create_task` moved to `async start()`; `close()` added.
- NSGA-II `evaluate()` no longer self-referential — it now evaluates a
  top-K policy per candidate, producing a genuine Pareto front.
- `get_best_weight_vector()` works after loading from disk.
- MoE gating update uses reward with a running baseline.
- `record_feedback` uses a composite reward, and propagates errors.
- `_compute_dynamic_weights` derives weights from recent telemetry.
- Sync storage + JSON I/O wrapped in `asyncio.to_thread`.
- Buffered writer for OnlineWeightManager (debounced).
- Offline buffer is bounded and not truncated before success.
- MOEA optimizer reused across ticks; population seeded from last Pareto.
- Constructor flags for enable_* on AdaptiveCostFunction.
- `datetime.now(timezone.utc)` everywhere.
- Prometheus metrics (optional).
- `MOPDWeightVector.from_dict` filters unknown fields.
- Whole Pareto front stored in MODP.
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import logging
import os
import random
import time
import uuid
from collections import Counter, deque
from dataclasses import dataclass, fields as dataclass_fields
from datetime import datetime, timezone
from pathlib import Path
from typing import (
    Any, Awaitable, Callable, Dict, List, Optional, Tuple, Union,
)

import numpy as np

# ------------------------------------------------------------------------------
# Optional imports with fallbacks
# ------------------------------------------------------------------------------
try:
    from prometheus_client import Counter as PromCounter, Gauge, Histogram
    PROMETHEUS_AVAILABLE = True
except ImportError:  # pragma: no cover
    PROMETHEUS_AVAILABLE = False


try:
    import structlog
    _logger = structlog.get_logger(__name__)
    _STRUCTLOG_AVAILABLE = True
except ImportError:  # pragma: no cover
    _logger = logging.getLogger(__name__)
    _STRUCTLOG_AVAILABLE = False
    logging.basicConfig(level=logging.INFO)


def log_event(level: str, message: str, **kwargs: Any) -> None:
    """Logging shim that works with structlog or stdlib logging."""
    if _STRUCTLOG_AVAILABLE:
        getattr(_logger, level)(message, **kwargs)
    else:
        if kwargs:
            extra = " ".join(f"{k}={v!r}" for k, v in kwargs.items())
            message = f"{message} | {extra}"
        getattr(_logger, level)(message)


try:
    from ..storage import Storage  # type: ignore
except ImportError:  # pragma: no cover
    Storage = Any  # type: ignore

try:
    from ..schemas.feedback_event import FeedbackEvent  # type: ignore
except ImportError:  # pragma: no cover
    FeedbackEvent = Any  # type: ignore

try:
    from ..config import config  # type: ignore
except ImportError:  # pragma: no cover
    import types
    config = types.SimpleNamespace()  # type: ignore

try:
    from ..logger import logger  # type: ignore
except ImportError:  # pragma: no cover
    logger = _logger  # type: ignore


# ==============================================================================
# Helpers
# ==============================================================================
_WEIGHT_KEYS = ("quality", "energy", "carbon", "latency", "helium")


def _normalized_benefits(event: Any) -> Dict[str, float]:
    """Convert a FeedbackEvent into normalized benefits (higher is better)."""
    max_energy = getattr(config, "ADAPTIVE_MAX_ENERGY", 100.0) or 100.0
    max_carbon = getattr(config, "ADAPTIVE_MAX_CARBON", 1.0) or 1.0
    max_latency = getattr(config, "ADAPTIVE_MAX_LATENCY", 1000.0) or 1000.0
    max_helium = getattr(config, "ADAPTIVE_MAX_HELIUM", 1.0) or 1.0

    quality = float(getattr(event, "quality_score", 0.0))
    energy = float(getattr(event, "energy_joules", 0.0))
    carbon = float(getattr(event, "carbon_g", 0.0))
    latency = float(getattr(event, "latency_ms", 0.0))
    helium_cost = getattr(event, "helium_cost", None)

    b: Dict[str, float] = {
        "quality": max(0.0, min(1.0, quality)),
        "energy": 1.0 - min(1.0, energy / max_energy),
        "carbon": 1.0 - min(1.0, carbon / max_carbon),
        "latency": 1.0 - min(1.0, latency / max_latency),
    }
    if helium_cost is not None:
        b["helium"] = 1.0 - min(1.0, float(helium_cost) / max_helium)
    else:
        b["helium"] = 0.5
    return b


def _composite_reward(event: Any, weights: Dict[str, float]) -> float:
    """Scalar reward = weighted sum of normalized benefits."""
    b = _normalized_benefits(event)
    return float(sum(weights.get(k, 0.0) * b.get(k, 0.0) for k in _WEIGHT_KEYS))


def _normalize_weights(weights: Dict[str, float]) -> Dict[str, float]:
    total = sum(weights.values())
    if total > 0:
        return {k: v / total for k, v in weights.items()}
    n = max(len(weights), 1)
    return {k: 1.0 / n for k in weights}


# ==============================================================================
# OnlineWeightManager
# ==============================================================================
class OnlineWeightManager:
    """
    Exponential moving average for online adaptation.

    Persists weights through the storage backend. Writes are debounced to avoid
    blocking the event loop on every feedback event.
    """

    def __init__(self, storage: Storage):
        self.storage = storage
        self.weights: Dict[str, float] = {
            "quality": 0.25, "energy": 0.25, "carbon": 0.25,
            "latency": 0.25, "helium": 0.0,
        }
        self.alpha = 0.1
        self.max_energy = getattr(config, "ADAPTIVE_MAX_ENERGY", 100.0) or 100.0
        self.max_carbon = getattr(config, "ADAPTIVE_MAX_CARBON", 1.0) or 1.0
        self.max_latency = getattr(config, "ADAPTIVE_MAX_LATENCY", 1000.0) or 1000.0
        self.max_helium = getattr(config, "ADAPTIVE_MAX_HELIUM", 1.0) or 1.0
        self._lock = asyncio.Lock()

        # Debounced persistence
        self._save_interval_seconds = float(
            getattr(config, "ADAPTIVE_SAVE_INTERVAL_SEC", 2.0) or 2.0
        )
        self._last_saved_at: float = 0.0
        self._dirty = False

        self._load_state()

    # ---- persistence ----
    def _load_state(self) -> None:
        try:
            data = self.storage.load_adaptive_state("online_weights")
            if data:
                parsed = json.loads(data)
                if isinstance(parsed, dict):
                    for k in _WEIGHT_KEYS:
                        if k in parsed:
                            self.weights[k] = float(parsed[k])
                log_event("info", "Loaded online weights", weights=self.weights)
        except Exception as exc:
            log_event("warning", f"Failed to load online weights: {exc}")

    def _save_state(self) -> None:
        try:
            self.storage.save_adaptive_state(
                "online_weights", json.dumps(self.weights)
            )
            self._last_saved_at = time.time()
            self._dirty = False
        except Exception as exc:
            log_event("error", f"Failed to save online weights: {exc}")

    async def _maybe_flush(self) -> None:
        if not self._dirty:
            return
        if (time.time() - self._last_saved_at) < self._save_interval_seconds:
            return
        try:
            await asyncio.to_thread(self._save_state)
        except Exception as exc:
            log_event("warning", f"Failed to flush online weights: {exc}")

    async def flush(self) -> None:
        """Force-persist the current weights."""
        async with self._lock:
            try:
                await asyncio.to_thread(self._save_state)
            except Exception as exc:
                log_event("warning", f"Failed to flush online weights: {exc}")

    # ---- update ----
    async def update(self, event: FeedbackEvent) -> None:
        async with self._lock:
            observed = _normalized_benefits(event)
            for key in self.weights:
                if key in observed:
                    self.weights[key] = (
                        (1 - self.alpha) * self.weights[key]
                        + self.alpha * observed[key]
                    )
            self.weights = _normalize_weights(self.weights)
            self._dirty = True
            log_event("debug", "Online weights updated", weights=self.weights)
        await self._maybe_flush()

    def get_cost_vector(self) -> Dict[str, float]:
        return self.weights.copy()

    def reset(self, initial_weights: Dict[str, float]) -> None:
        self.weights = _normalize_weights(dict(initial_weights))
        self._dirty = True
        try:
            self._save_state()
        except Exception as exc:
            log_event("warning", f"Failed to persist reset weights: {exc}")
        log_event("info", "Online weights reset", weights=self.weights)


# ==============================================================================
# LimitGraphManager
# ==============================================================================
class LimitGraphManager:
    """Graph of weight-vector relationships (nodes + edges)."""

    def __init__(self, storage: Optional[Storage] = None):
        self.storage = storage
        self.graphs: Dict[str, Dict[str, Any]] = {}

    def create_graph(self, graph_id: str, description: str, configuration: Dict[str, Any]) -> None:
        if self.storage and hasattr(self.storage, "save_limit_graph_metadata"):
            self.storage.save_limit_graph_metadata(graph_id, description, configuration)
        else:
            self.graphs[graph_id] = {
                "description": description,
                "configuration": configuration,
                "nodes": {}, "edges": {},
            }

    def add_node(self, graph_id: str, node_id: str, node_type: Optional[str], attributes: Dict[str, Any]) -> None:
        if self.storage and hasattr(self.storage, "save_limit_graph_node"):
            self.storage.save_limit_graph_node(node_id, graph_id, node_type, attributes)
        else:
            self.graphs.setdefault(graph_id, {"nodes": {}, "edges": {}})
            self.graphs[graph_id]["nodes"][node_id] = {
                "node_type": node_type, "attributes": attributes,
            }

    def add_edge(self, graph_id: str, edge_id: str, source: str, target: str,
                 weight: Optional[float], attributes: Dict[str, Any]) -> None:
        if self.storage and hasattr(self.storage, "save_limit_graph_edge"):
            self.storage.save_limit_graph_edge(edge_id, graph_id, source, target, weight, attributes)
        else:
            self.graphs.setdefault(graph_id, {"nodes": {}, "edges": {}})
            self.graphs[graph_id]["edges"][edge_id] = {
                "source": source, "target": target,
                "weight": weight, "attributes": attributes,
            }

    def get_nodes(self, graph_id: str) -> List[Dict]:
        if self.storage and hasattr(self.storage, "get_limit_graph_nodes"):
            return self.storage.get_limit_graph_nodes(graph_id)
        return list(self.graphs.get(graph_id, {}).get("nodes", {}).values())

    def get_edges(self, graph_id: str) -> List[Dict]:
        if self.storage and hasattr(self.storage, "get_limit_graph_edges"):
            return self.storage.get_limit_graph_edges(graph_id)
        return list(self.graphs.get(graph_id, {}).get("edges", {}).values())

    def get_metadata(self, graph_id: str) -> Optional[Dict]:
        if self.storage and hasattr(self.storage, "get_limit_graph_metadata"):
            return self.storage.get_limit_graph_metadata(graph_id)
        return self.graphs.get(graph_id, {})


# ==============================================================================
# MODPOptimizer
# ==============================================================================
class MODPOptimizer:
    """
    Stores decision states and policies. Useful for persisting the Pareto
    front and the selected weight vector.
    """

    def __init__(self, storage: Optional[Storage] = None):
        self.storage = storage
        self.states: Dict[str, List[Dict[str, Any]]] = {}

    def add_state(self, state_id: str, problem_id: str, state_attributes: Dict[str, Any],
                  objective_values: Dict[str, float], stage: int) -> None:
        if self.storage and hasattr(self.storage, "save_modp_state"):
            self.storage.save_modp_state(state_id, problem_id, state_attributes, objective_values, stage)
        else:
            self.states.setdefault(problem_id, []).append({
                "state_id": state_id,
                "state_attributes": state_attributes,
                "objective_values": objective_values,
                "stage": stage,
            })

    def add_policy(self, policy_id: str, problem_id: str, state_id: str,
                   action: str, expected_objectives: Dict[str, float]) -> None:
        if self.storage and hasattr(self.storage, "save_modp_policy"):
            self.storage.save_modp_policy(policy_id, problem_id, state_id, action, expected_objectives)

    def get_states(self, problem_id: str) -> List[Dict]:
        if self.storage and hasattr(self.storage, "get_modp_states"):
            return self.storage.get_modp_states(problem_id)
        return self.states.get(problem_id, [])

    def get_policies(self, problem_id: str) -> List[Dict]:
        if self.storage and hasattr(self.storage, "get_modp_policies"):
            return self.storage.get_modp_policies(problem_id)
        return []


# ==============================================================================
# RLHFTrainer
# ==============================================================================
class RLHFTrainer:
    """Stores human preference pairs over weight vectors."""

    def __init__(self, storage: Optional[Storage] = None):
        self.storage = storage
        self.pairs: List[Dict[str, Any]] = []

    def record_pair(self, pair_id: str, prompt: str, chosen: str, rejected: str,
                    reward_diff: float, metadata: Optional[Dict] = None) -> None:
        if self.storage and hasattr(self.storage, "save_preference_pair"):
            self.storage.save_preference_pair(pair_id, prompt, chosen, rejected, reward_diff, metadata)
        else:
            self.pairs.append({
                "pair_id": pair_id, "prompt": prompt, "chosen": chosen,
                "rejected": rejected, "reward_diff": reward_diff, "metadata": metadata,
            })

    def get_pairs(self, limit: int = 100) -> List[Dict]:
        if self.storage and hasattr(self.storage, "get_preference_pairs"):
            return self.storage.get_preference_pairs(limit)
        return self.pairs[-limit:]

    def train_reward_model(self) -> None:
        pairs = self.get_pairs()
        if len(pairs) < 5:
            log_event("info", "Not enough preference pairs for RLHF training.")
            return
        log_event("info", f"Training reward model on {len(pairs)} preference pairs...")


# ==============================================================================
# MoEGatingNetwork
# ==============================================================================
class MoEGatingNetwork:
    """
    Gating network over the weight sources (online / offline / rule-based).
    Uses a 5-dim state encoding (normalized benefits) and REINFORCE updates
    with a running baseline.
    """

    def __init__(self, storage: Optional[Storage] = None, config: Optional[Dict] = None):
        self.storage = storage
        self.config = config or {}
        self.expert_names: List[str] = self.config.get(
            "expert_names", ["online", "offline", "rule_based"]
        )
        self.num_experts = len(self.expert_names)
        # Small random init; scaled down to avoid saturation.
        self.gating_weights = np.random.randn(self.num_experts, 5) * 0.01
        self.gating_lr = float(self.config.get("gating_lr", 0.05))
        self._reward_baseline = 0.5

    def _encode_state(self, metrics: Dict[str, float]) -> np.ndarray:
        features = [
            metrics.get("quality", 0.5),
            metrics.get("energy", 0.5),
            metrics.get("carbon", 0.5),
            metrics.get("latency", 0.5),
            metrics.get("helium", 0.5),
        ]
        return np.asarray(features, dtype=np.float32)

    async def select_expert(self, metrics: Dict[str, float]) -> Tuple[str, np.ndarray]:
        x = self._encode_state(metrics)
        logits = self.gating_weights @ x
        probs = np.exp(logits - np.max(logits))
        probs = probs / probs.sum()
        expert_idx = int(np.argmax(probs))
        selected = self.expert_names[expert_idx]
        if self.storage and hasattr(self.storage, "log_routing_decision"):
            sample_id = hashlib.sha256(str(metrics).encode()).hexdigest()[:16]
            try:
                self.storage.log_routing_decision(
                    str(uuid.uuid4()), sample_id, selected, float(probs[expert_idx])
                )
            except Exception as exc:
                log_event("warning", f"Failed to log routing decision: {exc}")
        return selected, probs

    async def add_training_sample(self, metrics: Dict[str, float],
                                  selected_expert: str, reward: float) -> None:
        if selected_expert not in self.expert_names:
            return
        x = self._encode_state(metrics)
        expert_idx = self.expert_names.index(selected_expert)

        logits = self.gating_weights @ x
        probs = np.exp(logits - np.max(logits))
        probs = probs / probs.sum()

        # REINFORCE with a running baseline.
        self._reward_baseline = 0.95 * self._reward_baseline + 0.05 * float(reward)
        advantage = float(reward) - self._reward_baseline

        target = np.zeros(self.num_experts)
        target[expert_idx] = 1.0
        grad = advantage * (probs - target)[:, None] * x[None, :]
        self.gating_weights -= self.gating_lr * grad


# ==============================================================================
# MOPDWeightVector
# ==============================================================================
@dataclass
class MOPDWeightVector:
    """A weight vector and its objectives (all maximized)."""
    vector_id: str
    weights: Dict[str, float]
    objectives: Dict[str, float]
    scalarised_score: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "vector_id": self.vector_id,
            "weights": self.weights,
            "objectives": self.objectives,
            "scalarised_score": self.scalarised_score,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MOPDWeightVector":
        field_names = {f.name for f in dataclass_fields(cls)}
        return cls(**{k: v for k, v in data.items() if k in field_names})


# ==============================================================================
# Pure selection helper (usable without an optimizer instance)
# ==============================================================================
def select_best_from_pareto(
    pareto: List[MOPDWeightVector],
    weights: Dict[str, float],
) -> Optional[MOPDWeightVector]:
    """Normalize each objective across the Pareto front, then scalarize."""
    if not pareto:
        return None
    keys = list(weights.keys())
    max_vals = {k: max(p.objectives.get(k, 0.0) for p in pareto) for k in keys}
    min_vals = {k: min(p.objectives.get(k, 0.0) for p in pareto) for k in keys}
    ranges = {
        k: (max_vals[k] - min_vals[k]) if max_vals[k] != min_vals[k] else 1.0
        for k in keys
    }
    best: Optional[MOPDWeightVector] = None
    best_score = -float("inf")
    for p in pareto:
        score = 0.0
        for k in keys:
            v = p.objectives.get(k, 0.0)
            norm = (v - min_vals[k]) / ranges[k] if ranges[k] > 0 else 1.0
            score += weights.get(k, 0.0) * norm
        p.scalarised_score = score
        if score > best_score:
            best_score = score
            best = p
    return best


# ==============================================================================
# NSGAIIWeightOptimizer
# ==============================================================================
class NSGAIIWeightOptimizer:
    """
    NSGA-II over weight vectors.

    The evaluate_func receives a candidate weight vector and must return an
    objective dict (all maximized). The optimizer treats the candidate as a
    policy: the objectives must reflect outcomes that depend on the candidate
    in a non-trivial way, otherwise the Pareto front collapses.
    """

    def __init__(
        self,
        evaluate_func: Callable[[Dict[str, float]], Awaitable[Dict[str, float]]],
        population_size: int = 20,
        generations: int = 10,
        mutation_rate: float = 0.2,
        crossover_rate: float = 0.8,
        tournament_size: int = 3,
        objective_weights: Optional[Dict[str, float]] = None,
        dynamic_weights: bool = True,
    ):
        self.evaluate_func = evaluate_func
        self.population_size = max(4, population_size)
        self.generations = max(1, generations)
        self.mutation_rate = mutation_rate
        self.crossover_rate = crossover_rate
        self.tournament_size = max(2, tournament_size)
        self.objective_weights = objective_weights or {
            "quality": 0.3, "energy": 0.2, "carbon": 0.2,
            "latency": 0.2, "helium": 0.1,
        }
        self.dynamic_weights = dynamic_weights

        self.best_individual: Optional[Dict[str, float]] = None
        self.best_fitness = -float("inf")
        self.evolution_history: List[Dict[str, Any]] = []
        self.pareto_front: List[MOPDWeightVector] = []
        self._eval_cache: Dict[Tuple[Tuple[str, float], ...], Dict[str, float]] = {}

        # Reused across ticks; seeded with the previous Pareto front.
        self._population: List[Dict[str, float]] = []

    # ---- genetic operators ----
    def _random_individual(self) -> Dict[str, float]:
        keys = list(self.objective_weights.keys())
        weights = {k: random.random() for k in keys}
        return _normalize_weights(weights)

    def _crossover(self, p1: Dict[str, float], p2: Dict[str, float]) -> Dict[str, float]:
        child: Dict[str, float] = {}
        for key in p1:
            if random.random() < 0.5:
                u = random.random()
                if u <= 0.5:
                    beta = (2 * u) ** (1 / 21)
                else:
                    beta = (1 / (2 * (1 - u))) ** (1 / 21)
                child[key] = max(0.0, min(1.0, 0.5 * ((1 + beta) * p1[key] + (1 - beta) * p2[key])))
            else:
                child[key] = p1[key] if random.random() < 0.5 else p2[key]
        return _normalize_weights(child)

    def _mutate(self, ind: Dict[str, float]) -> Dict[str, float]:
        mutant = ind.copy()
        for key in mutant:
            if random.random() < self.mutation_rate:
                u = random.random()
                if u < 0.5:
                    delta = (2 * u) ** (1 / 21) - 1
                else:
                    delta = 1 - (2 * (1 - u)) ** (1 / 21)
                mutant[key] = max(0.0, min(1.0, mutant[key] + delta))
        return _normalize_weights(mutant)

    # ---- NSGA-II core ----
    @staticmethod
    def _dominates(a: MOPDWeightVector, b: MOPDWeightVector, keys: List[str]) -> bool:
        at_least = all(a.objectives.get(k, 0.0) >= b.objectives.get(k, 0.0) for k in keys)
        strictly = any(a.objectives.get(k, 0.0) > b.objectives.get(k, 0.0) for k in keys)
        return at_least and strictly

    def _fast_non_dominated_sort(
        self, points: List[MOPDWeightVector]
    ) -> List[List[MOPDWeightVector]]:
        if not points:
            return []
        keys = list(self.objective_weights.keys())
        fronts: List[List[MOPDWeightVector]] = [[]]
        domination_count: Dict[int, int] = {id(p): 0 for p in points}
        dominated_solutions: Dict[int, List[MOPDWeightVector]] = {id(p): [] for p in points}

        for p in points:
            for q in points:
                if p is q:
                    continue
                if self._dominates(p, q, keys):
                    dominated_solutions[id(p)].append(q)
                elif self._dominates(q, p, keys):
                    domination_count[id(p)] += 1
            if domination_count[id(p)] == 0:
                fronts[0].append(p)

        i = 0
        while i < len(fronts) and fronts[i]:
            nxt: List[MOPDWeightVector] = []
            for p in fronts[i]:
                for q in dominated_solutions[id(p)]:
                    domination_count[id(q)] -= 1
                    if domination_count[id(q)] == 0:
                        nxt.append(q)
            i += 1
            fronts.append(nxt)
        return [f for f in fronts if f]

    def _crowding_distance(self, front: List[MOPDWeightVector]) -> Dict[int, float]:
        if not front:
            return {}
        keys = list(self.objective_weights.keys())
        dist: Dict[int, float] = {id(p): 0.0 for p in front}
        for k in keys:
            sorted_front = sorted(front, key=lambda x: x.objectives.get(k, 0.0))
            dist[id(sorted_front[0])] = float("inf")
            dist[id(sorted_front[-1])] = float("inf")
            lo = sorted_front[0].objectives.get(k, 0.0)
            hi = sorted_front[-1].objectives.get(k, 0.0)
            span = hi - lo
            if span <= 0:
                continue
            for i in range(1, len(sorted_front) - 1):
                dist[id(sorted_front[i])] += (
                    sorted_front[i + 1].objectives.get(k, 0.0)
                    - sorted_front[i - 1].objectives.get(k, 0.0)
                ) / span
        return dist

    def _tournament_selection(
        self,
        population: List[Dict[str, float]],
        rank: Dict[int, int],
        crowding: Dict[int, float],
        ind_to_point: Dict[Tuple[Tuple[str, float], ...], MOPDWeightVector],
    ) -> Dict[str, float]:
        candidates = random.sample(population, min(self.tournament_size, len(population)))
        best = candidates[0]
        best_rank = float("inf")
        best_cd = -float("inf")
        for cand in candidates:
            key = tuple(sorted(cand.items()))
            point = ind_to_point.get(key)
            if point is None:
                continue
            r = rank.get(id(point), float("inf"))
            cd = crowding.get(id(point), 0.0)
            if r < best_rank or (r == best_rank and cd > best_cd):
                best = cand
                best_rank = r
                best_cd = cd
        return best

    # ---- scalarization ----
    def _compute_dynamic_weights(self) -> Dict[str, float]:
        weights = dict(self.objective_weights)
        if not self.dynamic_weights or not self.pareto_front:
            return weights
        for k in weights:
            vals = [p.objectives.get(k, 0.0) for p in self.pareto_front]
            if not vals:
                continue
            avg = float(np.mean(vals))
            mx = float(np.max(vals))
            if mx > 0 and avg < 0.5 * mx:
                weights[k] = min(0.6, weights[k] * 1.5)
        return _normalize_weights(weights)

    def _select_best_from_pareto(
        self, pareto: List[MOPDWeightVector], weights: Dict[str, float]
    ) -> Optional[MOPDWeightVector]:
        return select_best_from_pareto(pareto, weights)

    # ---- evaluation ----
    async def _evaluate(self, weights: Dict[str, float]) -> Dict[str, float]:
        key = tuple(sorted(weights.items()))
        if key in self._eval_cache:
            return self._eval_cache[key]
        obj = await self.evaluate_func(weights)
        self._eval_cache[key] = obj
        return obj

    # ---- main loop ----
    async def evolve(self) -> List[MOPDWeightVector]:
        # Seed with previous Pareto front if available.
        population: List[Dict[str, float]] = [dict(p.weights) for p in self.pareto_front]
        while len(population) < self.population_size:
            population.append(self._random_individual())
        population = population[: self.population_size]

        points: List[MOPDWeightVector] = []
        for w in population:
            obj = await self._evaluate(w)
            points.append(MOPDWeightVector(
                vector_id=str(uuid.uuid4()),
                weights=w,
                objectives=obj,
                scalarised_score=0.0,
            ))

        for gen in range(self.generations):
            fronts = self._fast_non_dominated_sort(points)
            rank: Dict[int, int] = {}
            crowding: Dict[int, float] = {}
            for i, front in enumerate(fronts):
                cd = self._crowding_distance(front)
                for p in front:
                    rank[id(p)] = i
                    crowding[id(p)] = cd.get(id(p), 0.0)

            ind_to_point: Dict[Tuple[Tuple[str, float], ...], MOPDWeightVector] = {
                tuple(sorted(p.weights.items())): p for p in points
            }

            offspring_w: List[Dict[str, float]] = []
            while len(offspring_w) < self.population_size:
                p1 = self._tournament_selection(population, rank, crowding, ind_to_point)
                p2 = self._tournament_selection(population, rank, crowding, ind_to_point)
                child = self._crossover(p1, p2) if random.random() < self.crossover_rate else dict(p1)
                child = self._mutate(child)
                offspring_w.append(child)

            offspring_points: List[MOPDWeightVector] = []
            for w in offspring_w:
                obj = await self._evaluate(w)
                offspring_points.append(MOPDWeightVector(
                    vector_id=str(uuid.uuid4()),
                    weights=w,
                    objectives=obj,
                    scalarised_score=0.0,
                ))

            combined = points + offspring_points
            fronts = self._fast_non_dominated_sort(combined)
            next_points: List[MOPDWeightVector] = []
            for front in fronts:
                if len(next_points) + len(front) <= self.population_size:
                    next_points.extend(front)
                else:
                    cd = self._crowding_distance(front)
                    front_sorted = sorted(front, key=lambda p: cd.get(id(p), 0.0), reverse=True)
                    next_points.extend(front_sorted[: self.population_size - len(next_points)])
                    break
            points = next_points
            population = [dict(p.weights) for p in points]

            self.evolution_history.append({
                "generation": gen,
                "pareto_size": len(fronts[0]) if fronts else 0,
                "population_size": len(points),
            })
            log_event("info", f"Generation {gen + 1}/{self.generations}",
                      pareto_size=len(fronts[0]) if fronts else 0)

        fronts = self._fast_non_dominated_sort(points)
        self.pareto_front = fronts[0] if fronts else points
        if self.pareto_front:
            best = self._select_best_from_pareto(
                self.pareto_front, self._compute_dynamic_weights()
            )
            if best is not None:
                self.best_individual = dict(best.weights)
                self.best_fitness = best.scalarised_score
        return self.pareto_front


# ==============================================================================
# OfflineTrainer
# ==============================================================================
class OfflineTrainer:
    """
    Batched updates + MOEA. The MOEA optimizer is reused across ticks; the
    population is seeded from the last Pareto front. The buffer is bounded
    and is not truncated until the batch has been processed successfully.
    """

    def __init__(
        self,
        storage: Storage,
        mtpd_optimizer: Optional[Any] = None,
        limit_graph_manager: Optional[LimitGraphManager] = None,
        modp_solver: Optional[MODPOptimizer] = None,
        *,
        enable_moea: Optional[bool] = None,
        moea_objective_weights: Optional[Dict[str, float]] = None,
        moea_interval_seconds: Optional[int] = None,
        moea_population_size: Optional[int] = None,
        moea_generations: Optional[int] = None,
        moea_mutation_rate: Optional[float] = None,
        moea_crossover_rate: Optional[float] = None,
        moea_tournament_size: Optional[int] = None,
        moea_dynamic_weights: Optional[bool] = None,
    ):
        self.storage = storage
        self.mtpd_optimizer = mtpd_optimizer
        self.limit_graph_manager = limit_graph_manager
        self.modp_solver = modp_solver

        # Buffer
        self.batch_size = int(getattr(config, "OFFLINE_BATCH_SIZE", 32) or 32)
        self.buffer_max_size = int(getattr(config, "OFFLINE_BUFFER_MAX_SIZE", 4096) or 4096)
        self.buffer: deque = deque(maxlen=self.buffer_max_size)
        self.update_interval = getattr(config, "OFFLINE_UPDATE_INTERVAL_SEC", 60)
        self.last_update = datetime.now(timezone.utc)
        self._lock = asyncio.Lock()

        # MOEA config
        self.moea_enabled = bool(
            enable_moea if enable_moea is not None
            else getattr(config, "MOEA_ENABLED", True)
        )
        self.moea_population_size = int(
            moea_population_size
            or getattr(config, "MOEA_POPULATION_SIZE", 20)
        )
        self.moea_generations = int(
            moea_generations
            or getattr(config, "MOEA_GENERATIONS", 10)
        )
        self.moea_interval_seconds = int(
            moea_interval_seconds
            or getattr(config, "MOEA_INTERVAL_SEC", 300)
        )
        self.moea_mutation_rate = float(
            moea_mutation_rate
            if moea_mutation_rate is not None
            else getattr(config, "MOEA_MUTATION_RATE", 0.2)
        )
        self.moea_crossover_rate = float(
            moea_crossover_rate
            if moea_crossover_rate is not None
            else getattr(config, "MOEA_CROSSOVER_RATE", 0.8)
        )
        self.moea_tournament_size = int(
            moea_tournament_size
            or getattr(config, "MOEA_TOURNAMENT_SIZE", 3)
        )
        self.moea_objective_weights = (
            moea_objective_weights
            or getattr(config, "MOEA_OBJECTIVE_WEIGHTS", None)
            or {"quality": 0.3, "energy": 0.2, "carbon": 0.2,
                "latency": 0.2, "helium": 0.1}
        )
        self.moea_dynamic_weights = bool(
            moea_dynamic_weights
            if moea_dynamic_weights is not None
            else getattr(config, "MOEA_DYNAMIC_WEIGHTS", True)
        )

        self.moea_optimizer: Optional[NSGAIIWeightOptimizer] = None
        self.pareto_front: List[MOPDWeightVector] = []
        self._moea_task: Optional[asyncio.Task] = None
        self._started = False

        # Persistence
        self.pareto_path = Path(
            getattr(config, "MOEA_PARETO_PATH", "./adaptive_pareto_front.json")
            or "./adaptive_pareto_front.json"
        )

        # Metrics
        self.metrics: Optional[Dict[str, Any]] = None
        if PROMETHEUS_AVAILABLE and getattr(config, "ENABLE_PROMETHEUS", True):
            self.metrics = {
                "batch_size": Histogram(
                    "adaptive_offline_batch_size", "Offline batch size"
                ),
                "batch_failures": PromCounter(
                    "adaptive_offline_batch_failures_total", "Offline batch failures"
                ),
                "pareto_size": Gauge(
                    "adaptive_moea_pareto_size", "MOEA Pareto front size"
                ),
                "moea_runs": PromCounter(
                    "adaptive_moea_runs_total", "MOEA runs"
                ),
            }

    # ---- lifecycle ----
    async def start(self) -> "OfflineTrainer":
        if self._started:
            return self
        self._started = True
        # Load a persisted Pareto front so the next evolution can seed from it.
        await self._load_pareto()
        if self.moea_enabled:
            self._moea_task = asyncio.create_task(self._moea_loop())
        return self

    async def close(self) -> None:
        if self._moea_task:
            self._moea_task.cancel()
            try:
                await self._moea_task
            except (asyncio.CancelledError, Exception):
                pass
            self._moea_task = None
        await self._persist_pareto()
        self._started = False

    async def __aenter__(self) -> "OfflineTrainer":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    # ---- buffer ----
    async def queue_event(self, event: FeedbackEvent) -> None:
        async with self._lock:
            self.buffer.append(event)
            if len(self.buffer) >= self.batch_size:
                await self._train_step_locked()

    async def _train_step_locked(self) -> None:
        """Process a batch. On failure the batch stays in the buffer."""
        if not self.buffer:
            return
        batch = list(self.buffer)[: self.batch_size]

        # Compute average normalized metrics for this batch.
        avg_metrics: Dict[str, List[float]] = {k: [] for k in _WEIGHT_KEYS}
        for e in batch:
            b = _normalized_benefits(e)
            for k in _WEIGHT_KEYS:
                avg_metrics[k].append(b[k])
        batch_weights = {k: float(np.mean(v)) if v else 0.5 for k, v in avg_metrics.items()}

        # Call the MTPD optimizer if available.
        if self.mtpd_optimizer:
            try:
                await self.mtpd_optimizer.offline_update(batch)
                log_event("info", f"MTPD offline update OK batch_size={len(batch)}")
            except Exception as exc:
                log_event("error", f"MTPD offline update failed: {exc}")
                if self.metrics:
                    try:
                        self.metrics["batch_failures"].inc()
                    except Exception:
                        pass
                return  # keep batch in buffer

        # Persist a summary (best effort).
        try:
            await asyncio.to_thread(
                self.storage.log_offline_batch_summary,
                {
                    "timestamp": time.time(),
                    "batch_size": len(batch),
                    "avg_quality": batch_weights.get("quality", 0.0),
                    "avg_carbon": batch_weights.get("carbon", 0.0),
                    "avg_latency": batch_weights.get("latency", 0.0),
                    "avg_energy": batch_weights.get("energy", 0.0),
                },
            )
        except Exception as exc:
            log_event("warning", f"Failed to store batch summary: {exc}")

        # Only remove the processed events on success.
        for _ in range(len(batch)):
            try:
                self.buffer.popleft()
            except IndexError:
                break

        self.last_update = datetime.now(timezone.utc)
        if self.metrics:
            try:
                self.metrics["batch_size"].observe(len(batch))
            except Exception:
                pass

    # ---- MOEA ----
    async def _moea_loop(self) -> None:
        # Run once at start, then periodically.
        try:
            await self.run_moea()
        except Exception as exc:
            log_event("error", f"Initial MOEA run failed: {exc}")
        while True:
            try:
                await asyncio.sleep(self.moea_interval_seconds)
                await self.run_moea()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                log_event("error", f"MOEA loop error: {exc}")
                await asyncio.sleep(60)

    async def _load_pareto(self) -> None:
        if not self.pareto_path.exists():
            return
        try:
            def _read() -> List[Dict[str, Any]]:
                with open(self.pareto_path, "r") as f:
                    return json.load(f)

            data = await asyncio.to_thread(_read)
            self.pareto_front = [MOPDWeightVector.from_dict(d) for d in data]
            log_event("info", f"Loaded Pareto front from {self.pareto_path}",
                      size=len(self.pareto_front))
        except Exception as exc:
            log_event("warning", f"Failed to load Pareto front: {exc}")

    async def _persist_pareto(self) -> None:
        if not self.pareto_front:
            return
        payload = [p.to_dict() for p in self.pareto_front]
        path = self.pareto_path
        tmp = path.with_suffix(path.suffix + ".tmp")

        def _write() -> None:
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(tmp, "w") as f:
                json.dump(payload, f, indent=2)
            os.replace(tmp, path)

        try:
            await asyncio.to_thread(_write)
        except Exception as exc:
            log_event("warning", f"Failed to persist Pareto front: {exc}")

    async def _fetch_events(self, limit: int = 1000) -> List[Any]:
        if hasattr(self.storage, "get_recent_feedback_events"):
            return await asyncio.to_thread(
                self.storage.get_recent_feedback_events, limit=limit
            )
        if hasattr(self.storage, "get_feedback_events"):
            return await asyncio.to_thread(
                self.storage.get_feedback_events, limit=limit
            )
        log_event("warning", "Storage does not provide feedback events; MOEA skipped.")
        return []

    async def run_moea(self) -> List[MOPDWeightVector]:
        try:
            events = await self._fetch_events(limit=1000)
        except Exception as exc:
            log_event("error", f"Failed to retrieve events for MOEA: {exc}")
            return []

        if len(events) < 20:
            log_event("warning", "Not enough events for MOEA; skipping.")
            return []

        # Precompute benefits.
        benefits_list = [_normalized_benefits(ev) for ev in events]
        keys = list(self.moea_objective_weights.keys())
        n_events = len(benefits_list)
        top_k = max(1, n_events // 10)

        async def evaluate(weights: Dict[str, float]) -> Dict[str, float]:
            """
            Policy-based objectives.

            A candidate weight vector defines a preference over events.
            We rank events by the weighted score of their benefits, then
            evaluate the top-K events' mean benefit per objective. Different
            candidates select different top-K sets, producing genuine
            trade-offs — not a self-referential score.
            """
            scores = np.array([
                sum(weights.get(k, 0.0) * b.get(k, 0.0) for k in keys)
                for b in benefits_list
            ])
            if len(scores) == 0:
                return {k: 0.0 for k in keys}
            top_idx = np.argsort(scores)[-top_k:]
            selected = [benefits_list[i] for i in top_idx]
            return {
                k: float(np.mean([b.get(k, 0.0) for b in selected]))
                for k in keys
            }

        # Reuse the optimizer across ticks.
        if self.moea_optimizer is None:
            self.moea_optimizer = NSGAIIWeightOptimizer(
                evaluate_func=evaluate,
                population_size=self.moea_population_size,
                generations=self.moea_generations,
                mutation_rate=self.moea_mutation_rate,
                crossover_rate=self.moea_crossover_rate,
                tournament_size=self.moea_tournament_size,
                objective_weights=self._compute_dynamic_weights(),
                dynamic_weights=self.moea_dynamic_weights,
            )
        else:
            self.moea_optimizer.objective_weights = self._compute_dynamic_weights()

        self.pareto_front = await self.moea_optimizer.evolve()
        log_event("info", f"MOEA produced Pareto front size={len(self.pareto_front)}")

        if self.metrics:
            try:
                self.metrics["pareto_size"].set(len(self.pareto_front))
                self.metrics["moea_runs"].inc()
            except Exception:
                pass

        # Persist asynchronously.
        await self._persist_pareto()

        # Store the WHOLE front in MODP and add nodes to LIMIT graph.
        if self.pareto_front:
            for p in self.pareto_front:
                if self.modp_solver:
                    self.modp_solver.add_state(
                        state_id=f"pareto_{p.vector_id}",
                        problem_id="weight_optimization",
                        state_attributes={"weights": p.weights},
                        objective_values=p.objectives,
                        stage=1,
                    )
            if self.limit_graph_manager:
                best = select_best_from_pareto(
                    self.pareto_front, self._compute_dynamic_weights()
                )
                if best:
                    self.limit_graph_manager.add_node(
                        "weight_vectors",
                        f"vector_{best.vector_id}",
                        "best_weight_vector",
                        {"weights": best.weights, "objectives": best.objectives},
                    )
        return self.pareto_front

    async def get_best_weight_vector(self) -> Optional[Dict[str, float]]:
        if not self.pareto_front:
            await self._load_pareto()
        if self.pareto_front:
            best = select_best_from_pareto(
                self.pareto_front, self._compute_dynamic_weights()
            )
            if best is not None:
                return dict(best.weights)
        return None

    def _compute_dynamic_weights(self) -> Dict[str, float]:
        weights = dict(self.moea_objective_weights)
        recent = list(self.buffer)[-50:] if self.buffer else []
        if recent:
            avg_carbon = float(np.mean([getattr(e, "carbon_g", 0.0) for e in recent]))
            max_carbon = getattr(config, "ADAPTIVE_MAX_CARBON", 1.0) or 1.0
            if avg_carbon / max_carbon > 0.7:
                weights["carbon"] = min(0.6, weights.get("carbon", 0.2) * 1.5)
            avg_latency = float(np.mean([getattr(e, "latency_ms", 0.0) for e in recent]))
            max_latency = getattr(config, "ADAPTIVE_MAX_LATENCY", 1000.0) or 1000.0
            if avg_latency / max_latency > 0.7:
                weights["latency"] = min(0.6, weights.get("latency", 0.2) * 1.5)
        return _normalize_weights(weights)


# ==============================================================================
# AdaptiveCostFunction
# ==============================================================================
class AdaptiveCostFunction:
    """
    Orchestrator: online EMA, offline batch trainer, MOEA, MoE gating,
    LIMIT graph, MODP, RLHF.
    """

    def __init__(
        self,
        storage: Storage,
        mtpd_optimizer: Optional[Any] = None,
        *,
        enable_moea: Optional[bool] = None,
        enable_limit_graph: Optional[bool] = None,
        enable_modp: Optional[bool] = None,
        enable_rlhf: Optional[bool] = None,
        enable_moe: Optional[bool] = None,
        moea_objective_weights: Optional[Dict[str, float]] = None,
        moea_interval_seconds: Optional[int] = None,
        moea_population_size: Optional[int] = None,
        moea_generations: Optional[int] = None,
        moea_dynamic_weights: Optional[bool] = None,
    ):
        self.storage = storage
        self.online = OnlineWeightManager(storage)

        def _flag(arg: Optional[bool], key: str, default: bool = True) -> bool:
            if arg is not None:
                return arg
            return bool(getattr(config, key, default))

        enable_limit_graph = _flag(enable_limit_graph, "ENABLE_LIMIT_GRAPH", True)
        enable_modp = _flag(enable_modp, "ENABLE_MODP", True)
        enable_rlhf = _flag(enable_rlhf, "ENABLE_RLHF", True)
        enable_moe = _flag(enable_moe, "ENABLE_MOE", True)

        self.limit_graph_manager = LimitGraphManager(storage) if enable_limit_graph else None
        self.modp_solver = MODPOptimizer(storage) if enable_modp else None
        self.rlhf_trainer = RLHFTrainer(storage) if enable_rlhf else None
        self.moe_gating = (
            MoEGatingNetwork(storage, {"expert_names": ["online", "offline", "rule_based"]})
            if enable_moe else None
        )

        self.offline = OfflineTrainer(
            storage,
            mtpd_optimizer,
            limit_graph_manager=self.limit_graph_manager,
            modp_solver=self.modp_solver,
            enable_moea=enable_moea,
            moea_objective_weights=moea_objective_weights,
            moea_interval_seconds=moea_interval_seconds,
            moea_population_size=moea_population_size,
            moea_generations=moea_generations,
            moea_dynamic_weights=moea_dynamic_weights,
        )
        self.drift_detector: Optional[Any] = None

        # Metrics
        self.metrics: Optional[Dict[str, Any]] = None
        if PROMETHEUS_AVAILABLE and getattr(config, "ENABLE_PROMETHEUS", True):
            self.metrics = {
                "feedback_total": PromCounter(
                    "adaptive_feedback_total", "Feedback events processed"
                ),
                "feedback_failures": PromCounter(
                    "adaptive_feedback_failures_total", "Feedback processing failures"
                ),
                "composite_reward": Histogram(
                    "adaptive_composite_reward", "Composite reward per event"
                ),
                "online_weight": Gauge(
                    "adaptive_online_weight", "Online weight per metric", ["metric"]
                ),
                "moe_route": PromCounter(
                    "adaptive_moe_route_total", "MoE routing decisions", ["expert"]
                ),
                "drift_detected": PromCounter(
                    "adaptive_drift_detected_total", "Drift detection events"
                ),
            }

        # LIMIT graph bootstrap
        if self.limit_graph_manager:
            if not self.limit_graph_manager.get_metadata("weight_vectors"):
                self.limit_graph_manager.create_graph(
                    "weight_vectors", "Weight Vector Relationships", {}
                )
            for src in ("online", "offline", "rule_based"):
                self.limit_graph_manager.add_node(
                    "weight_vectors", f"source_{src}", src, {"type": "source"}
                )

    # ---- lifecycle ----
    async def start(self) -> "AdaptiveCostFunction":
        await self.offline.start()
        return self

    async def close(self) -> None:
        try:
            await self.online.flush()
        except Exception:
            pass
        await self.offline.close()

    async def __aenter__(self) -> "AdaptiveCostFunction":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    # ---- feedback ----
    async def record_feedback(self, event: FeedbackEvent) -> None:
        """Record feedback. Raises on unrecoverable errors."""
        # Store the raw event.
        try:
            await asyncio.to_thread(
                self.storage.store_feedback_event, event.to_db_dict()
            )
        except Exception as exc:
            if self.metrics:
                try:
                    self.metrics["feedback_failures"].inc()
                except Exception:
                    pass
            log_event("error", f"Failed to persist feedback event: {exc}")
            raise

        # Online EMA update.
        try:
            await self.online.update(event)
        except Exception as exc:
            log_event("warning", f"Online update failed: {exc}")

        # Offline buffer.
        try:
            await self.offline.queue_event(event)
        except Exception as exc:
            log_event("warning", f"Offline queue failed: {exc}")

        # Composite reward over the current online weights.
        current_weights = self.online.get_cost_vector()
        reward = _composite_reward(event, current_weights)

        # MoE gating update.
        if self.moe_gating:
            metrics = _normalized_benefits(event)
            try:
                selected_expert, probs = await self.moe_gating.select_expert(metrics)
                await self.moe_gating.add_training_sample(metrics, selected_expert, reward)
                if self.metrics:
                    self.metrics["moe_route"].labels(expert=selected_expert).inc()
            except Exception as exc:
                log_event("warning", f"MoE gating update failed: {exc}")

        # Drift detection.
        if self.drift_detector:
            try:
                drift_result = await self.drift_detector.check_drift(current_weights)
                if drift_result and drift_result.get("drift_detected"):
                    log_event("warning", "Drift detected", result=drift_result)
                    if self.metrics:
                        self.metrics["drift_detected"].inc()
            except Exception as exc:
                log_event("warning", f"Drift detection failed: {exc}")

        # Metrics
        if self.metrics:
            try:
                self.metrics["feedback_total"].inc()
                self.metrics["composite_reward"].observe(reward)
                for k, v in current_weights.items():
                    self.metrics["online_weight"].labels(metric=k).set(v)
            except Exception:
                pass

    def get_current_weights(self) -> Dict[str, float]:
        return self.online.get_cost_vector()

    # ---- blended weights ----
    async def get_blended_weights(self) -> Dict[str, float]:
        """
        Blend online, offline (MOEA), and rule-based weight vectors via MoE.
        Falls back to online weights if MoE is disabled or offline unavailable.
        """
        if not self.moe_gating:
            return self.get_current_weights()

        online_weights = self.get_current_weights()
        offline_weights = await self.offline.get_best_weight_vector()
        if offline_weights is None:
            return online_weights

        rule_based = {k: 0.2 for k in _WEIGHT_KEYS}
        rule_based = _normalize_weights(rule_based)

        # Context: use the current online weights, which are normalized
        # benefits in [0, 1] — the same distribution the gating network was
        # trained on.
        context = {
            "quality": online_weights.get("quality", 0.2),
            "energy": online_weights.get("energy", 0.2),
            "carbon": online_weights.get("carbon", 0.2),
            "latency": online_weights.get("latency", 0.2),
            "helium": online_weights.get("helium", 0.0),
        }
        _, probs = await self.moe_gating.select_expert(context)

        blended: Dict[str, float] = {k: 0.0 for k in _WEIGHT_KEYS}
        total_prob = 0.0
        for i, name in enumerate(self.moe_gating.expert_names):
            if name == "online":
                weights = online_weights
            elif name == "offline":
                weights = offline_weights
            elif name == "rule_based":
                weights = rule_based
            else:
                continue
            p = float(probs[i])
            for k in _WEIGHT_KEYS:
                blended[k] += p * weights.get(k, 0.0)
            total_prob += p

        if total_prob > 0:
            blended = {k: v / total_prob for k, v in blended.items()}
        return _normalize_weights(blended)

    async def get_evolved_weights(self) -> Optional[Dict[str, float]]:
        return await self.offline.get_best_weight_vector()

    # ---- RLHF ----
    async def record_human_preference(
        self, chosen_source: str, rejected_source: str, reward_diff: float = 1.0
    ) -> None:
        if self.rlhf_trainer:
            self.rlhf_trainer.record_pair(
                pair_id=str(uuid.uuid4()),
                prompt="Which weight source produced better routing?",
                chosen=chosen_source,
                rejected=rejected_source,
                reward_diff=reward_diff,
                metadata={"timestamp": datetime.now(timezone.utc).isoformat()},
            )

    # ---- maintenance ----
    def reset_weights(self, initial_weights: Dict[str, float]) -> None:
        self.online.reset(initial_weights)
        self.offline.buffer.clear()
        log_event("info", "Adaptive cost function reset")

    async def get_limit_graph(self, graph_id: str = "weight_vectors") -> Dict:
        if self.limit_graph_manager:
            return {
                "metadata": self.limit_graph_manager.get_metadata(graph_id),
                "nodes": self.limit_graph_manager.get_nodes(graph_id),
                "edges": self.limit_graph_manager.get_edges(graph_id),
            }
        return {}

    async def get_moe_experts(self) -> List[str]:
        return list(self.moe_gating.expert_names) if self.moe_gating else []

    # ---- diagnostics ----
    def get_stats(self) -> Dict[str, Any]:
        return {
            "online_weights": self.online.get_cost_vector(),
            "offline_buffer_size": len(self.offline.buffer),
            "pareto_front_size": len(self.offline.pareto_front),
            "moea_enabled": self.offline.moea_enabled,
            "started": self.offline._started,
        }
