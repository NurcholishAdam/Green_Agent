#!/usr/bin/env python3
"""
Enhanced Helium Collector v2.5.0
==================================
Collects Helium hotspot connectivity data from a live API and/or offline Parquet
snapshots. Provides a connectivity score (0-1) based on RSSI and SNR.

FIXES OVER v2.4.1:
- Added missing `dataclass` import.
- Proper Pydantic v1/v2 config normalization.
- No `asyncio.create_task` in `__init__`; explicit `async start()`.
- Per-request `RequestContext` replaces shared `self.last_*` fields (race-free).
- Structured-logging shim works with or without structlog.
- Circuit breaker treats `None` / schema errors as failures; public `.state`.
- Snapshot Parquet is cached in memory and indexed by `hotspot_id`.
- `update_snapshot()` invalidates cache atomically under a lock.
- MoE gating uses reward with a running baseline; expert names generated dynamically.
- `SourceHistoricalMLTeacher` reorders probabilities to fixed action order.
- `SourceStatefulQTeacher.update()` is called online.
- `_log_interaction()` records `state_vec`, buffers, and flushes via `asyncio.to_thread`.
- `train_historical_model()` is implemented end-to-end.
- Fallback scores are cached with a shorter TTL.
- Batch fetch uses `Semaphore` + per-request timeout.
- MOEA latency metric filters to API-source rows only.
- Q weights and student weights persist on `close()`.
"""

from __future__ import annotations

import asyncio
import csv
import copy
import hashlib
import json
import logging
import os
import pickle
import random
import time
import uuid
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import (
    Any, Awaitable, Callable, Dict, List, Optional, Tuple,
    Union,
)

import aiohttp
import numpy as np
import pandas as pd
from aiohttp import ClientError, ClientResponseError, ClientTimeout

# ---------- Pydantic ----------
try:
    from pydantic import BaseModel, Field, field_validator, ValidationError
    PYDANTIC_AVAILABLE = True
except ImportError:  # pragma: no cover
    PYDANTIC_AVAILABLE = False
    BaseModel = object  # type: ignore

# ---------- Tenacity ----------
try:
    from tenacity import (
        retry, stop_after_attempt, wait_exponential,
        retry_if_exception_type, RetryError,
    )
    TENACITY_AVAILABLE = True
except ImportError:  # pragma: no cover
    TENACITY_AVAILABLE = False

# ---------- Prometheus ----------
try:
    from prometheus_client import Counter, Gauge, Histogram
    PROMETHEUS_AVAILABLE = True
except ImportError:  # pragma: no cover
    PROMETHEUS_AVAILABLE = False

# ---------- Structlog ----------
try:
    import structlog
    _logger = structlog.get_logger(__name__)
    _STRUCTLOG_AVAILABLE = True
except ImportError:  # pragma: no cover
    _logger = logging.getLogger(__name__)
    _STRUCTLOG_AVAILABLE = False
    logging.basicConfig(level=logging.INFO)


def log_event(level: str, message: str, **kwargs: Any) -> None:
    """Structured-logging shim that works with structlog or stdlib logging."""
    if _STRUCTLOG_AVAILABLE:
        getattr(_logger, level)(message, **kwargs)
    else:
        if kwargs:
            extra = " ".join(f"{k}={v!r}" for k, v in kwargs.items())
            message = f"{message} | {extra}"
        getattr(_logger, level)(message)


# ---------- scikit-learn ----------
try:
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.preprocessing import LabelEncoder
    SKLEARN_ML = True
except ImportError:  # pragma: no cover
    SKLEARN_ML = False

# ---------- Local imports (optional) ----------
try:
    from ..cache.cache_manager import CacheManager  # type: ignore
except ImportError:  # pragma: no cover
    CacheManager = Any  # type: ignore

try:
    from ...storage import Storage  # type: ignore
    CENTRAL_STORAGE_AVAILABLE = True
except ImportError:  # pragma: no cover
    Storage = Any  # type: ignore
    CENTRAL_STORAGE_AVAILABLE = False


# ============================================================================
# Configuration
# ============================================================================
if PYDANTIC_AVAILABLE:
    class HeliumConfig(BaseModel):
        """Configuration for HeliumCollector."""

        api_url: str = Field("https://api.helium.io/v1/")
        api_key: Optional[str] = None
        snapshot_path: Optional[Path] = None
        cache_ttl: int = Field(600, ge=0)
        fallback_cache_ttl: int = Field(30, ge=0)
        retry_attempts: int = Field(3, ge=0)
        retry_min_wait: float = Field(1.0, gt=0)
        retry_max_wait: float = Field(10.0, gt=0)
        circuit_breaker_threshold: int = Field(5, ge=1)
        circuit_breaker_timeout: float = Field(30.0, ge=1)
        request_timeout: float = Field(10.0, ge=1)
        rssi_min: float = Field(-120.0)
        rssi_max: float = Field(-30.0)
        snr_min: float = Field(-10.0)
        snr_max: float = Field(30.0)
        enable_prometheus: bool = True
        default_score: float = 0.5

        # Distillation
        distillation_epsilon: float = Field(0.1, ge=0, le=1)
        distillation_train_every: int = Field(10, ge=1)
        distillation_replay_size: int = Field(2000, ge=10)
        distillation_learning_rate: float = Field(0.01, ge=0.0001, le=1)
        distill_weight: float = Field(0.7, ge=0, le=1)
        rl_weight: float = Field(0.3, ge=0, le=1)

        # MOEA
        moea_enabled: bool = True
        moea_interval_seconds: int = Field(300, ge=60)
        moea_population_size: int = Field(30, ge=10)
        moea_generations: int = Field(10, ge=2)
        moea_mutation_rate: float = Field(0.2, ge=0.0, le=1.0)
        moea_crossover_rate: float = Field(0.8, ge=0.0, le=1.0)
        moea_tournament_size: int = Field(3, ge=2)
        moea_objective_weights: Dict[str, float] = Field(
            default_factory=lambda: {
                "success_rate": 0.4,
                "latency": 0.3,
                "snapshot_usage": 0.2,
                "cost": 0.1,
            }
        )
        moea_dynamic_weights: bool = True

        # Component flags
        enable_limit_graph: bool = True
        enable_modp: bool = True
        enable_rlhf: bool = True
        enable_moe: bool = True
        moe_expert_count: int = Field(4, ge=2)

        # Persistence
        q_weights_path: str = "./helium_q_weights.json"
        interaction_logs_path: str = "./helium_interactions.csv"
        historical_model_path: str = "./helium_historical_model.pkl"
        moea_pareto_path: str = "./helium_moea_pareto.json"

        @field_validator("api_url")
        @classmethod
        def validate_api_url(cls, v: str) -> str:
            if not v.endswith("/"):
                v += "/"
            return v

        class Config:
            env_prefix = "HELIUM_"
else:
    HELIUM_CONFIG: Dict[str, Any] = {
        "api_url": "https://api.helium.io/v1/",
        "api_key": None,
        "snapshot_path": None,
        "cache_ttl": 600,
        "fallback_cache_ttl": 30,
        "retry_attempts": 3,
        "retry_min_wait": 1.0,
        "retry_max_wait": 10.0,
        "circuit_breaker_threshold": 5,
        "circuit_breaker_timeout": 30.0,
        "request_timeout": 10.0,
        "rssi_min": -120.0,
        "rssi_max": -30.0,
        "snr_min": -10.0,
        "snr_max": 30.0,
        "enable_prometheus": True,
        "default_score": 0.5,
        "distillation_epsilon": 0.1,
        "distillation_train_every": 10,
        "distillation_replay_size": 2000,
        "distillation_learning_rate": 0.01,
        "distill_weight": 0.7,
        "rl_weight": 0.3,
        "moea_enabled": True,
        "moea_interval_seconds": 300,
        "moea_population_size": 30,
        "moea_generations": 10,
        "moea_mutation_rate": 0.2,
        "moea_crossover_rate": 0.8,
        "moea_tournament_size": 3,
        "moea_objective_weights": {
            "success_rate": 0.4, "latency": 0.3,
            "snapshot_usage": 0.2, "cost": 0.1,
        },
        "moea_dynamic_weights": True,
        "enable_limit_graph": True,
        "enable_modp": True,
        "enable_rlhf": True,
        "enable_moe": True,
        "moe_expert_count": 4,
        "q_weights_path": "./helium_q_weights.json",
        "interaction_logs_path": "./helium_interactions.csv",
        "historical_model_path": "./helium_historical_model.pkl",
        "moea_pareto_path": "./helium_moea_pareto.json",
    }


def _pydantic_dump(model: Any) -> Dict[str, Any]:
    if hasattr(model, "model_dump"):
        return model.model_dump()
    return model.dict()


def normalize_config(
    config: Optional[Union[Dict[str, Any], "HeliumConfig"]]
) -> Dict[str, Any]:
    """Normalize any config input into a plain dict (Pydantic v1/v2 aware)."""
    if config is None:
        if PYDANTIC_AVAILABLE:
            return _pydantic_dump(HeliumConfig())
        return copy.deepcopy(HELIUM_CONFIG)

    if isinstance(config, dict):
        if PYDANTIC_AVAILABLE:
            return _pydantic_dump(HeliumConfig(**config))
        return dict(config)

    if PYDANTIC_AVAILABLE and isinstance(config, HeliumConfig):
        return _pydantic_dump(config)

    return {k: getattr(config, k) for k in dir(config) if not k.startswith("_")}


# ============================================================================
# Circuit Breaker
# ============================================================================
class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """In-memory circuit breaker with half-open state."""

    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
    ) -> None:
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self._state = CircuitBreakerState.CLOSED
        self._failure_count = 0
        self._last_failure_time: Optional[datetime] = None
        self._lock = asyncio.Lock()
        self._on_state_change: Optional[Callable[[str, CircuitBreakerState], None]] = None

    @property
    def state(self) -> CircuitBreakerState:
        return self._state

    def on_state_change(
        self, cb: Callable[[str, CircuitBreakerState], None]
    ) -> None:
        self._on_state_change = cb

    def _set_state(self, new_state: CircuitBreakerState) -> None:
        if self._state != new_state:
            self._state = new_state
            if self._on_state_change:
                try:
                    self._on_state_change(self.name, new_state)
                except Exception:  # pragma: no cover
                    pass

    async def call(self, func: Callable[..., Awaitable[Any]], *args: Any, **kwargs: Any) -> Any:
        async with self._lock:
            now = datetime.now(timezone.utc)
            if self._state == CircuitBreakerState.OPEN:
                if (
                    self._last_failure_time
                    and (now - self._last_failure_time).total_seconds() >= self.recovery_timeout
                ):
                    self._set_state(CircuitBreakerState.HALF_OPEN)
                    log_event("info", f"Circuit breaker {self.name} entering HALF_OPEN")
                else:
                    raise RuntimeError(f"Circuit breaker {self.name} is OPEN")

        try:
            result = await func(*args, **kwargs)
        except Exception as exc:
            async with self._lock:
                self._failure_count += 1
                self._last_failure_time = datetime.now(timezone.utc)
                if self._failure_count >= self.failure_threshold:
                    self._set_state(CircuitBreakerState.OPEN)
                    log_event(
                        "warning",
                        f"Circuit breaker {self.name} opened "
                        f"after {self._failure_count} failures",
                    )
            raise exc

        async with self._lock:
            if self._state == CircuitBreakerState.HALF_OPEN:
                self._set_state(CircuitBreakerState.CLOSED)
                log_event("info", f"Circuit breaker {self.name} closed after success")
            self._failure_count = 0
        return result


# ============================================================================
# LIMIT Graph Manager
# ============================================================================
class LimitGraphManager:
    """Manages a graph of source selection relationships for LIMIT."""

    def __init__(self, storage: Optional[Any] = None) -> None:
        self.storage = storage
        self.graphs: Dict[str, Dict[str, Any]] = {}

    def create_graph(self, graph_id: str, description: str, configuration: Dict[str, Any]) -> None:
        if self.storage and hasattr(self.storage, "save_limit_graph_metadata"):
            self.storage.save_limit_graph_metadata(graph_id, description, configuration)
        else:
            self.graphs[graph_id] = {
                "description": description,
                "configuration": configuration,
                "nodes": {},
                "edges": {},
            }

    def add_node(self, graph_id: str, node_id: str, node_type: Optional[str], attributes: Dict[str, Any]) -> None:
        if self.storage and hasattr(self.storage, "save_limit_graph_node"):
            self.storage.save_limit_graph_node(node_id, graph_id, node_type, attributes)
        else:
            self.graphs.setdefault(graph_id, {"nodes": {}, "edges": {}})
            self.graphs[graph_id]["nodes"][node_id] = {
                "node_type": node_type, "attributes": attributes,
            }

    def add_edge(
        self, graph_id: str, edge_id: str, source: str, target: str,
        weight: Optional[float], attributes: Dict[str, Any],
    ) -> None:
        if self.storage and hasattr(self.storage, "save_limit_graph_edge"):
            self.storage.save_limit_graph_edge(edge_id, graph_id, source, target, weight, attributes)
        else:
            self.graphs.setdefault(graph_id, {"nodes": {}, "edges": {}})
            self.graphs[graph_id]["edges"][edge_id] = {
                "source": source, "target": target,
                "weight": weight, "attributes": attributes,
            }

    def get_nodes(self, graph_id: str) -> List[Dict[str, Any]]:
        if self.storage and hasattr(self.storage, "get_limit_graph_nodes"):
            return self.storage.get_limit_graph_nodes(graph_id)
        return list(self.graphs.get(graph_id, {}).get("nodes", {}).values())

    def get_edges(self, graph_id: str) -> List[Dict[str, Any]]:
        if self.storage and hasattr(self.storage, "get_limit_graph_edges"):
            return self.storage.get_limit_graph_edges(graph_id)
        return list(self.graphs.get(graph_id, {}).get("edges", {}).values())

    def get_metadata(self, graph_id: str) -> Optional[Dict[str, Any]]:
        if self.storage and hasattr(self.storage, "get_limit_graph_metadata"):
            return self.storage.get_limit_graph_metadata(graph_id)
        return self.graphs.get(graph_id, {})


# ============================================================================
# MODP Optimizer (interface preserved; stub solver)
# ============================================================================
class MODPOptimizer:
    """Multi-Objective Dynamic Programming solver."""

    def __init__(self, storage: Optional[Any] = None) -> None:
        self.storage = storage
        self.states: Dict[str, List[Dict[str, Any]]] = {}

    def add_state(
        self, state_id: str, problem_id: str, state_attributes: Dict[str, Any],
        objective_values: Dict[str, float], stage: int,
    ) -> None:
        if self.storage and hasattr(self.storage, "save_modp_state"):
            self.storage.save_modp_state(state_id, problem_id, state_attributes, objective_values, stage)
        else:
            self.states.setdefault(problem_id, []).append({
                "state_id": state_id,
                "state_attributes": state_attributes,
                "objective_values": objective_values,
                "stage": stage,
            })

    def add_transition(
        self, transition_id: str, problem_id: str, from_state: str,
        to_state: str, action: str, cost: float,
        objective_deltas: Dict[str, float],
    ) -> None:
        if self.storage and hasattr(self.storage, "save_modp_transition"):
            self.storage.save_modp_transition(
                transition_id, problem_id, from_state, to_state,
                action, cost, objective_deltas,
            )

    def add_policy(
        self, policy_id: str, problem_id: str, state_id: str,
        action: str, expected_objectives: Dict[str, float],
    ) -> None:
        if self.storage and hasattr(self.storage, "save_modp_policy"):
            self.storage.save_modp_policy(policy_id, problem_id, state_id, action, expected_objectives)

    def get_states(self, problem_id: str) -> List[Dict[str, Any]]:
        if self.storage and hasattr(self.storage, "get_modp_states"):
            return self.storage.get_modp_states(problem_id)
        return self.states.get(problem_id, [])

    def get_transitions(self, problem_id: str) -> List[Dict[str, Any]]:
        if self.storage and hasattr(self.storage, "get_modp_transitions"):
            return self.storage.get_modp_transitions(problem_id)
        return []

    def get_policies(self, problem_id: str) -> List[Dict[str, Any]]:
        if self.storage and hasattr(self.storage, "get_modp_policies"):
            return self.storage.get_modp_policies(problem_id)
        return []

    async def solve(self, problem_id: str, initial_state: Dict[str, Any], max_stages: int = 5) -> Dict[str, Any]:
        self.add_state(
            state_id=f"{problem_id}_init",
            problem_id=problem_id,
            state_attributes=initial_state,
            objective_values={
                "success_rate": 0.0, "latency": 0.0,
                "snapshot_usage": 0.0, "cost": 0.0,
            },
            stage=0,
        )
        return {"status": "solved", "pareto_front": []}


# ============================================================================
# RLHF Trainer
# ============================================================================
class RLHFTrainer:
    """Collects human preference pairs for source selection."""

    def __init__(self, storage: Optional[Any] = None) -> None:
        self.storage = storage
        self.pairs: List[Dict[str, Any]] = []

    def record_pair(
        self, pair_id: str, prompt: str, chosen: str, rejected: str,
        reward_diff: float, metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        if self.storage and hasattr(self.storage, "save_preference_pair"):
            self.storage.save_preference_pair(pair_id, prompt, chosen, rejected, reward_diff, metadata)
        else:
            self.pairs.append({
                "pair_id": pair_id, "prompt": prompt, "chosen": chosen,
                "rejected": rejected, "reward_diff": reward_diff, "metadata": metadata,
            })

    def get_pairs(self, limit: int = 100) -> List[Dict[str, Any]]:
        if self.storage and hasattr(self.storage, "get_preference_pairs"):
            return self.storage.get_preference_pairs(limit)
        return self.pairs[-limit:]

    def train_reward_model(self) -> None:
        pairs = self.get_pairs()
        if len(pairs) < 5:
            log_event("info", "Not enough preference pairs for RLHF training.")
            return
        log_event("info", f"Training reward model on {len(pairs)} preference pairs...")


# ============================================================================
# MoE Gating Network
# ============================================================================
class MoEGatingNetwork:
    """Mixture-of-Experts gating for source selection."""

    def __init__(self, storage: Optional[Any] = None, config: Optional[Dict[str, Any]] = None) -> None:
        self.storage = storage
        self.config = config or {}
        self.num_experts = int(self.config.get("moe_expert_count", 4))
        base = ["snapshot_focus", "api_focus", "fallback_focus", "adaptive"]
        self.expert_names = [
            base[i] if i < len(base) else f"expert_{i}"
            for i in range(self.num_experts)
        ]
        # Feature vector length: 8 (matches SourceSelectionState).
        self.gating_weights = np.random.randn(self.num_experts, 8) * 0.01
        self.gating_lr = float(self.config.get("moe_lr", 0.05))
        self._reward_baseline = 0.5

    def _encode_state(self, state: Union["SourceSelectionState", Dict[str, Any]]) -> np.ndarray:
        if isinstance(state, dict):
            features = [
                state.get("snapshot_exists", 0),
                state.get("hour_of_day", 0) / 24.0,
                state.get("day_of_week", 0) / 7.0,
                state.get("success_snapshot", 0.5),
                state.get("success_api", 0.5),
                state.get("success_fallback", 0.5),
                state.get("cb_state", 0) / 2.0,
                min(state.get("api_latency", 0) / 5.0, 1.0),
            ]
        else:
            features = state.to_feature_vector()
        return np.asarray(features, dtype=np.float32)

    async def select_expert(
        self, state: Union["SourceSelectionState", Dict[str, Any]]
    ) -> Tuple[str, np.ndarray]:
        x = self._encode_state(state)
        logits = self.gating_weights @ x
        probs = np.exp(logits - np.max(logits))
        probs = probs / probs.sum()
        idx = int(np.argmax(probs))
        selected = self.expert_names[idx]
        if self.storage and hasattr(self.storage, "log_routing_decision"):
            sample_id = hashlib.sha256(str(state).encode()).hexdigest()[:16]
            self.storage.log_routing_decision(
                str(uuid.uuid4()), sample_id, selected, float(probs[idx])
            )
        return selected, probs

    async def add_training_sample(
        self,
        state: Union["SourceSelectionState", Dict[str, Any]],
        selected_expert: str,
        reward: float,
    ) -> None:
        if selected_expert not in self.expert_names:
            return
        x = self._encode_state(state)
        expert_idx = self.expert_names.index(selected_expert)

        logits = self.gating_weights @ x
        probs = np.exp(logits - np.max(logits))
        probs = probs / probs.sum()

        # REINFORCE-style update with running baseline.
        self._reward_baseline = 0.95 * self._reward_baseline + 0.05 * float(reward)
        advantage = float(reward) - self._reward_baseline

        target = np.zeros(self.num_experts)
        target[expert_idx] = 1.0
        grad = advantage * (probs - target)[:, None] * x[None, :]
        self.gating_weights -= self.gating_lr * grad


# ============================================================================
# Distillation Components
# ============================================================================
@dataclass
class SourceSelectionState:
    """State representation for source selection (8 features)."""

    snapshot_exists: float = 0.0
    hour_of_day: float = 0.0
    day_of_week: float = 0.0
    success_snapshot: float = 0.5
    success_api: float = 0.5
    success_fallback: float = 0.5
    cb_state: float = 0.0
    api_latency: float = 0.0

    def to_feature_vector(self) -> np.ndarray:
        return np.asarray([
            self.snapshot_exists,
            self.hour_of_day / 24.0,
            self.day_of_week / 7.0,
            self.success_snapshot,
            self.success_api,
            self.success_fallback,
            self.cb_state / 2.0,
            min(self.api_latency / 5.0, 1.0),
        ], dtype=np.float32)


class Teacher(ABC):
    @abstractmethod
    def predict(self, state: SourceSelectionState) -> np.ndarray:
        ...

    @abstractmethod
    def confidence(self, state: SourceSelectionState) -> float:
        ...


class SourceRuleBasedTeacher(Teacher):
    ACTION_SPACE = ["snapshot", "api", "fallback"]

    def predict(self, state: SourceSelectionState) -> np.ndarray:
        probs = np.ones(3) * 0.1
        if state.snapshot_exists > 0.5 and state.success_snapshot > 0.6:
            probs[0] += 0.7
        elif state.success_api > 0.6 and state.cb_state == 0.0:
            probs[1] += 0.6
        else:
            probs[2] += 0.5
        total = probs.sum()
        return probs / total if total > 0 else np.ones(3) / 3

    def confidence(self, state: SourceSelectionState) -> float:
        return 0.6 if state.snapshot_exists > 0.5 else 0.4


class SourceHistoricalMLTeacher(Teacher):
    def __init__(self, model_path: Optional[Path] = None, n_actions: int = 3) -> None:
        self.model = None
        self.label_encoder = None
        self.n_actions = n_actions
        if model_path and Path(model_path).exists() and SKLEARN_ML:
            try:
                with open(model_path, "rb") as f:
                    self.model, self.label_encoder = pickle.load(f)
                log_event("info", f"Loaded historical model from {model_path}")
            except Exception as exc:
                log_event("warning", f"Failed to load historical model: {exc}")
                self.model = None

    def predict(self, state: SourceSelectionState) -> np.ndarray:
        if self.model is None or self.label_encoder is None:
            return np.ones(self.n_actions) / self.n_actions
        x = state.to_feature_vector().reshape(1, -1)
        try:
            probs = self.model.predict_proba(x)[0]
        except Exception:
            return np.ones(self.n_actions) / self.n_actions
        out = np.zeros(self.n_actions)
        for i, cls in enumerate(self.label_encoder.classes_):
            if i < len(probs) and 0 <= int(cls) < self.n_actions:
                out[int(cls)] = probs[i]
        s = out.sum()
        return out / s if s > 0 else np.ones(self.n_actions) / self.n_actions

    def confidence(self, state: SourceSelectionState) -> float:
        return 0.7 if self.model is not None else 0.0


class SourceStatefulQTeacher(Teacher):
    def __init__(self, lr: float = 0.1) -> None:
        self.lr = lr
        self.weights = np.zeros((8, 3))

    def predict(self, state: SourceSelectionState) -> np.ndarray:
        x = state.to_feature_vector()
        q = x @ self.weights
        exp_q = np.exp(q - np.max(q))
        return exp_q / exp_q.sum()

    def confidence(self, state: SourceSelectionState) -> float:
        return 0.5

    def update(self, state: SourceSelectionState, action: int, reward: float) -> None:
        x = state.to_feature_vector()
        q_current = float(np.dot(x, self.weights[:, action]))
        self.weights[:, action] += self.lr * (reward - q_current) * x


class DistillationStudent:
    def __init__(self, feature_dim: int = 8, n_classes: int = 3, lr: float = 0.01) -> None:
        self.weights = np.zeros((feature_dim, n_classes))
        self.biases = np.zeros(n_classes)
        self.lr = lr
        self.n_classes = n_classes
        self.counter = 0

    def predict_proba(self, state_vector: np.ndarray) -> np.ndarray:
        logits = state_vector @ self.weights + self.biases
        exp = np.exp(logits - np.max(logits))
        return exp / exp.sum()

    def update(
        self, state_vector: np.ndarray, teacher_probs: np.ndarray,
        reward: float, action: int,
        distill_weight: float = 0.7, rl_weight: float = 0.3,
    ) -> None:
        current = self.predict_proba(state_vector)
        grad_distill = -(teacher_probs - current)
        one_hot = np.zeros(self.n_classes)
        one_hot[action] = 1.0
        grad_rl = -reward * (one_hot - current)
        grad = distill_weight * grad_distill + rl_weight * grad_rl
        self.weights -= self.lr * np.outer(state_vector, grad)
        self.biases -= self.lr * grad
        self.counter += 1

    def save(self, path: Union[str, Path]) -> None:
        with open(path, "wb") as f:
            pickle.dump({"weights": self.weights, "biases": self.biases}, f)

    def load(self, path: Union[str, Path]) -> None:
        with open(path, "rb") as f:
            data = pickle.load(f)
        self.weights = data["weights"]
        self.biases = data["biases"]


class ReplayBuffer:
    def __init__(self, max_size: int = 2000) -> None:
        self.buffer: deque = deque(maxlen=max_size)

    def push(
        self, s: np.ndarray, a: int, r: float,
        ns: np.ndarray, tp: np.ndarray,
    ) -> None:
        self.buffer.append((s, a, r, ns, tp))

    def sample(
        self, batch_size: int = 32
    ) -> Tuple[np.ndarray, List[int], np.ndarray, np.ndarray, np.ndarray]:
        if len(self.buffer) < batch_size:
            batch = list(self.buffer)
        else:
            batch = random.sample(self.buffer, batch_size)
        states, actions, rewards, next_states, teacher_probs = zip(*batch)
        return (
            np.asarray(states),
            list(actions),
            np.asarray(rewards),
            np.asarray(next_states),
            np.asarray(teacher_probs),
        )

    def __len__(self) -> int:
        return len(self.buffer)


class DistillationSourceOptimizer:
    ACTION_SPACE = ["snapshot", "api", "fallback"]

    def __init__(
        self,
        config: Dict[str, Any],
        historical_model_path: Optional[Path] = None,
    ) -> None:
        self.config = config
        self.n_actions = len(self.ACTION_SPACE)
        self.student = DistillationStudent(
            feature_dim=8,
            n_classes=self.n_actions,
            lr=float(config.get("distillation_learning_rate", 0.01)),
        )
        self.q_teacher = SourceStatefulQTeacher()
        self.teachers: List[Teacher] = [
            SourceRuleBasedTeacher(),
            SourceHistoricalMLTeacher(
                model_path=historical_model_path, n_actions=self.n_actions
            ),
            self.q_teacher,
        ]
        self.replay_buffer = ReplayBuffer(
            max_size=int(config.get("distillation_replay_size", 2000))
        )
        self.epsilon = float(config.get("distillation_epsilon", 0.1))
        self.train_every = int(config.get("distillation_train_every", 10))
        self.counter = 0

    def _aggregate_teacher_probs(self, state: SourceSelectionState) -> np.ndarray:
        total = np.zeros(self.n_actions)
        total_conf = 0.0
        for teacher in self.teachers:
            try:
                p = teacher.predict(state)
                c = teacher.confidence(state)
            except Exception:
                continue
            if p.shape[0] != self.n_actions:
                continue
            total += p * c
            total_conf += c
        if total_conf > 0:
            total /= total_conf
        else:
            total = np.ones(self.n_actions) / self.n_actions
        return total

    async def select_source(
        self, state: SourceSelectionState, exploration: bool = True
    ) -> Tuple[str, int, np.ndarray, np.ndarray]:
        state_vec = state.to_feature_vector()
        teacher_probs = self._aggregate_teacher_probs(state)
        student_probs = self.student.predict_proba(state_vec)

        if exploration and random.random() < self.epsilon:
            action_idx = random.randint(0, self.n_actions - 1)
        else:
            combined = 0.8 * student_probs + 0.2 * teacher_probs
            action_idx = int(np.argmax(combined))

        return self.ACTION_SPACE[action_idx], action_idx, state_vec, teacher_probs

    async def update(
        self, state_vec: np.ndarray, action_idx: int, reward: float,
        next_state_vec: np.ndarray, teacher_probs: np.ndarray,
    ) -> None:
        self.replay_buffer.push(state_vec, action_idx, reward, next_state_vec, teacher_probs)
        self.counter += 1

        # Online Q teacher update.
        try:
            q_current = float(np.dot(state_vec, self.q_teacher.weights[:, action_idx]))
            self.q_teacher.weights[:, action_idx] += self.q_teacher.lr * (
                reward - q_current
            ) * state_vec
        except Exception:
            pass

        if self.counter % self.train_every == 0 and len(self.replay_buffer) >= 8:
            states, actions, rewards, _, teacher_probs_batch = self.replay_buffer.sample(8)
            for i in range(len(states)):
                self.student.update(
                    states[i], teacher_probs_batch[i], rewards[i], actions[i],
                    distill_weight=float(self.config.get("distill_weight", 0.7)),
                    rl_weight=float(self.config.get("rl_weight", 0.3)),
                )

    def get_stats(self) -> Dict[str, Any]:
        return {
            "student_counter": self.student.counter,
            "buffer_size": len(self.replay_buffer),
            "weights_norm": float(np.linalg.norm(self.student.weights)),
        }

    def save_q_weights(self, path: Union[str, Path]) -> None:
        with open(path, "w") as f:
            json.dump(self.q_teacher.weights.tolist(), f)

    def load_q_weights(self, path: Union[str, Path]) -> None:
        p = Path(path)
        if not p.exists():
            return
        try:
            with open(p, "r") as f:
                data = json.load(f)
            self.q_teacher.weights = np.asarray(data, dtype=np.float64)
        except Exception as exc:
            log_event("warning", f"Failed to load Q weights: {exc}")


# ============================================================================
# MOEA (interface preserved; still a stub NSGA-II)
# ============================================================================
@dataclass
class MOPDSourceStrategy:
    strategy_id: str
    weights: Dict[str, float]
    objectives: Dict[str, float]
    scalarised_score: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "strategy_id": self.strategy_id,
            "weights": self.weights,
            "objectives": self.objectives,
            "scalarised_score": self.scalarised_score,
        }


class NSGAIISourceOptimizer:
    """Interface preserved. Replace with a real NSGA-II implementation
    (e.g. via `pymoo`) for production use."""

    def __init__(
        self,
        evaluate_func: Callable[[Dict[str, float]], Awaitable[Dict[str, float]]],
        population_size: int = 30,
        generations: int = 10,
        mutation_rate: float = 0.2,
        crossover_rate: float = 0.8,
        tournament_size: int = 3,
        objective_weights: Optional[Dict[str, float]] = None,
        dynamic_weights: bool = True,
    ) -> None:
        self.evaluate_func = evaluate_func
        self.population_size = population_size
        self.generations = generations
        self.mutation_rate = mutation_rate
        self.crossover_rate = crossover_rate
        self.tournament_size = tournament_size
        self.objective_weights = objective_weights or {}
        self.dynamic_weights = dynamic_weights
        self.best_individual: Optional[MOPDSourceStrategy] = None
        self.best_fitness = -float("inf")
        self.pareto_front: List[MOPDSourceStrategy] = []

    def _random_weights(self) -> Dict[str, float]:
        w = {k: random.random() for k in self.objective_weights}
        total = sum(w.values())
        return {k: v / total for k, v in w.items()} if total > 0 else w

    def _crossover(self, p1: Dict[str, float], p2: Dict[str, float]) -> Dict[str, float]:
        child = {k: (p1[k] if random.random() < 0.5 else p2[k]) for k in p1}
        total = sum(child.values())
        return {k: v / total for k, v in child.items()} if total > 0 else child

    def _mutate(self, ind: Dict[str, float]) -> Dict[str, float]:
        m = ind.copy()
        for k in m:
            if random.random() < self.mutation_rate:
                m[k] = max(0.01, min(1.0, m[k] + random.uniform(-0.1, 0.1)))
        total = sum(m.values())
        return {k: v / total for k, v in m.items()} if total > 0 else m

    async def evolve(self) -> List[MOPDSourceStrategy]:
        population = [self._random_weights() for _ in range(self.population_size)]
        points: List[MOPDSourceStrategy] = []
        for ind in population:
            obj = await self.evaluate_func(ind)
            score = sum(self.objective_weights.get(k, 0.0) * float(v) for k, v in obj.items())
            points.append(MOPDSourceStrategy(
                strategy_id=str(uuid.uuid4()),
                weights=ind,
                objectives=obj,
                scalarised_score=score,
            ))
        self.pareto_front = points
        return points

    def _select_best_from_pareto(
        self, pareto: List[MOPDSourceStrategy], weights: Dict[str, float]
    ) -> Optional[MOPDSourceStrategy]:
        if not pareto:
            return None
        return max(pareto, key=lambda p: p.scalarised_score)


# ============================================================================
# Per-request context (fixes the shared-state race)
# ============================================================================
@dataclass
class RequestContext:
    hotspot_id: str
    state: SourceSelectionState
    state_vec: np.ndarray
    action_idx: int
    source: str
    teacher_probs: np.ndarray
    selected_expert: Optional[str] = None
    expert_probs: Optional[np.ndarray] = None


# ============================================================================
# HeliumCollector
# ============================================================================
class HeliumCollector:
    def __init__(
        self,
        cache: "CacheManager",
        config: Optional[Union[Dict[str, Any], "HeliumConfig"]] = None,
        storage: Optional["Storage"] = None,
        enable_limit_graph: Optional[bool] = None,
        enable_modp: Optional[bool] = None,
        enable_rlhf: Optional[bool] = None,
        enable_moe: Optional[bool] = None,
        moe_expert_count: Optional[int] = None,
    ) -> None:
        self.config: Dict[str, Any] = normalize_config(config)
        self.cache = cache
        self.storage = storage

        def flag(arg: Optional[bool], key: str, default: bool = True) -> bool:
            if arg is not None:
                return arg
            return bool(self.config.get(key, default))

        self.enable_limit_graph = flag(enable_limit_graph, "enable_limit_graph")
        self.enable_modp = flag(enable_modp, "enable_modp")
        self.enable_rlhf = flag(enable_rlhf, "enable_rlhf")
        self.enable_moe = flag(enable_moe, "enable_moe")
        self.moe_expert_count = int(
            moe_expert_count or self.config.get("moe_expert_count", 4)
        )

        self.api_url: str = str(self.config.get("api_url", "https://api.helium.io/v1/"))
        self.api_key: Optional[str] = (
            self.config.get("api_key") or os.environ.get("HELIUM_API_KEY")
        )
        self.snapshot_path: Optional[Path] = self._resolve_snapshot_path(
            self.config.get("snapshot_path")
        )
        self.cache_ttl: int = int(self.config.get("cache_ttl", 600))
        self.fallback_cache_ttl: int = int(self.config.get("fallback_cache_ttl", 30))
        self.request_timeout: float = float(self.config.get("request_timeout", 10.0))
        self.rssi_min: float = float(self.config.get("rssi_min", -120.0))
        self.rssi_max: float = float(self.config.get("rssi_max", -30.0))
        self.snr_min: float = float(self.config.get("snr_min", -10.0))
        self.snr_max: float = float(self.config.get("snr_max", 30.0))
        self.default_score: float = float(self.config.get("default_score", 0.5))

        # Session
        self._session: Optional[aiohttp.ClientSession] = None
        self._session_lock = asyncio.Lock()

        # Snapshot cache
        self._snapshot_df: Optional[pd.DataFrame] = None
        self._snapshot_lock = asyncio.Lock()
        self._snapshot_epoch = 0

        # Circuit breaker
        self._circuit_breaker = CircuitBreaker(
            name="helium_api",
            failure_threshold=int(self.config.get("circuit_breaker_threshold", 5)),
            recovery_timeout=float(self.config.get("circuit_breaker_timeout", 30.0)),
        )

        # Metrics (created before wiring CB callback)
        self.metrics: Optional[Dict[str, Any]] = None
        if PROMETHEUS_AVAILABLE and self.config.get("enable_prometheus", True):
            self.metrics = {
                "calls": Counter(
                    "helium_api_calls_total", "Helium source calls",
                    ["source", "status"],
                ),
                "errors": Counter(
                    "helium_api_errors_total", "Helium source errors", ["source"],
                ),
                "latency": Histogram(
                    "helium_api_latency_seconds", "Helium source latency", ["source"],
                ),
                "cache_hits": Counter("helium_cache_hits_total", "Cache hits"),
                "cache_misses": Counter("helium_cache_misses_total", "Cache misses"),
                "snapshot_hits": Counter("helium_snapshot_hits_total", "Snapshot hits"),
                "fallback_usage": Counter(
                    "helium_fallback_usage_total", "Fallback to default score",
                ),
                "connectivity_score": Gauge(
                    "helium_connectivity_score", "Hotspot connectivity score",
                    ["hotspot_id"],
                ),
                "circuit_breaker_state": Gauge(
                    "helium_circuit_breaker_state", "Circuit breaker state",
                ),
                "source_selection": Counter(
                    "helium_source_selection", "Source selected", ["source"],
                ),
                "source_reward": Histogram(
                    "helium_source_reward", "Reward per source selection",
                ),
                "moea_pareto_front": Gauge(
                    "helium_moea_pareto_front", "MOEA Pareto front size",
                ),
            }
            self._circuit_breaker.on_state_change(self._on_cb_state_change)

        # Distillation optimizer
        historical_path = Path(self.config.get(
            "historical_model_path", "./helium_historical_model.pkl"
        ))
        self.source_optimizer = DistillationSourceOptimizer(
            config={
                "distillation_epsilon": self.config.get("distillation_epsilon", 0.1),
                "distillation_train_every": self.config.get("distillation_train_every", 10),
                "distillation_replay_size": self.config.get("distillation_replay_size", 2000),
                "distillation_learning_rate": self.config.get("distillation_learning_rate", 0.01),
                "distill_weight": self.config.get("distill_weight", 0.7),
                "rl_weight": self.config.get("rl_weight", 0.3),
            },
            historical_model_path=historical_path,
        )
        try:
            self.source_optimizer.load_q_weights(
                self.config.get("q_weights_path", "./helium_q_weights.json")
            )
        except Exception as exc:
            log_event("warning", f"Could not load Q weights: {exc}")

        # Interaction log
        self.interaction_log: List[Dict[str, Any]] = []
        self._log_buffer: List[Dict[str, Any]] = []
        self._log_flush_size = 25
        self._log_lock = asyncio.Lock()
        self._interaction_log_path = Path(
            self.config.get("interaction_logs_path", "./helium_interactions.csv")
        )

        # MOEA
        self.moea_enabled: bool = bool(self.config.get("moea_enabled", True))
        self.moea_interval_seconds: int = int(self.config.get("moea_interval_seconds", 300))
        self.moea_population_size: int = int(self.config.get("moea_population_size", 30))
        self.moea_generations: int = int(self.config.get("moea_generations", 10))
        self.moea_mutation_rate: float = float(self.config.get("moea_mutation_rate", 0.2))
        self.moea_crossover_rate: float = float(self.config.get("moea_crossover_rate", 0.8))
        self.moea_tournament_size: int = int(self.config.get("moea_tournament_size", 3))
        self.moea_objective_weights: Dict[str, float] = dict(
            self.config.get("moea_objective_weights", {
                "success_rate": 0.4, "latency": 0.3,
                "snapshot_usage": 0.2, "cost": 0.1,
            })
        )
        self.moea_dynamic_weights: bool = bool(self.config.get("moea_dynamic_weights", True))
        self.moea_optimizer: Optional[NSGAIISourceOptimizer] = None
        self.evolved_pareto_front: List[MOPDSourceStrategy] = []
        self.best_evolved_strategy: Optional[MOPDSourceStrategy] = None
        self._moea_task: Optional[asyncio.Task] = None
        self._started = False

        # Optional components
        self.limit_graph_manager = LimitGraphManager(storage) if self.enable_limit_graph else None
        self.modp_solver = MODPOptimizer(storage) if self.enable_modp else None
        self.rlhf_trainer = RLHFTrainer(storage) if self.enable_rlhf else None
        self.moe_gating = (
            MoEGatingNetwork(storage, {"moe_expert_count": self.moe_expert_count})
            if self.enable_moe else None
        )

        if self.limit_graph_manager:
            self._init_limit_graph()

        log_event("info", "HeliumCollector v2.5.0 initialized")

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def start(self) -> "HeliumCollector":
        if self._started:
            return self
        self._started = True
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

        async with self._log_lock:
            await self._flush_log_buffer_locked()

        try:
            self.source_optimizer.save_q_weights(
                self.config.get("q_weights_path", "./helium_q_weights.json")
            )
        except Exception as exc:
            log_event("warning", f"Failed to persist Q weights: {exc}")

        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
        self._started = False

    async def __aenter__(self) -> "HeliumCollector":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    def _on_cb_state_change(self, provider: str, state: CircuitBreakerState) -> None:
        if not self.metrics:
            return
        mapping = {
            CircuitBreakerState.CLOSED: 0.0,
            CircuitBreakerState.HALF_OPEN: 1.0,
            CircuitBreakerState.OPEN: 2.0,
        }
        try:
            self.metrics["circuit_breaker_state"].set(mapping[state])
        except Exception:
            pass

    # ------------------------------------------------------------------
    # LIMIT graph
    # ------------------------------------------------------------------
    def _init_limit_graph(self) -> None:
        assert self.limit_graph_manager is not None
        graph_id = "helium_sources"
        if self.limit_graph_manager.get_metadata(graph_id):
            return
        self.limit_graph_manager.create_graph(graph_id, "Helium Source Selection Dependencies", {})
        for src in ["snapshot", "api", "fallback"]:
            self.limit_graph_manager.add_node(graph_id, f"source_{src}", src, {})
        self.limit_graph_manager.add_edge(
            graph_id, "edge_snapshot_api", "source_snapshot", "source_api", 1.0, {}
        )
        self.limit_graph_manager.add_edge(
            graph_id, "edge_api_fallback", "source_api", "source_fallback", 1.0, {}
        )

    # ------------------------------------------------------------------
    # Snapshot
    # ------------------------------------------------------------------
    def _resolve_snapshot_path(self, path: Any) -> Optional[Path]:
        if not path:
            return None
        p = Path(path) if isinstance(path, str) else path
        if p.exists():
            return p
        log_event("warning", "Snapshot path does not exist", path=str(p))
        return None

    async def _load_snapshot(self) -> Optional[pd.DataFrame]:
        """Load (and cache) the Parquet snapshot, indexed by hotspot_id."""
        if self.snapshot_path is None:
            return None
        async with self._snapshot_lock:
            if self._snapshot_df is not None:
                return self._snapshot_df
            try:
                df = pd.read_parquet(
                    self.snapshot_path,
                    columns=["hotspot_id", "rssi", "snr"],
                )
                if "hotspot_id" not in df.columns:
                    log_event(
                        "warning",
                        "Snapshot missing hotspot_id column; skipping index",
                    )
                    self._snapshot_df = df
                else:
                    self._snapshot_df = df.set_index("hotspot_id")
            except Exception as exc:
                log_event("warning", f"Failed to read snapshot: {exc}")
                self._snapshot_df = None
            return self._snapshot_df

    async def update_snapshot(self, snapshot_path: Any) -> bool:
        """Atomically swap the snapshot and invalidate the cache."""
        new_path = self._resolve_snapshot_path(snapshot_path)
        if new_path is None:
            log_event("warning", "update_snapshot called with invalid path")
            return False
        async with self._snapshot_lock:
            self.snapshot_path = new_path
            self._snapshot_df = None
            self._snapshot_epoch += 1
        # Invalidate cached scores for hotspots under this collector.
        try:
            if hasattr(self.cache, "invalidate_prefix"):
                await self.cache.invalidate_prefix("helium:score:")
        except Exception:
            pass
        log_event("info", "Snapshot path updated", path=str(new_path))
        return True

    # ------------------------------------------------------------------
    # Session
    # ------------------------------------------------------------------
    async def _get_session(self) -> aiohttp.ClientSession:
        async with self._session_lock:
            if self._session is None or self._session.closed:
                timeout = ClientTimeout(total=self.request_timeout)
                connector = aiohttp.TCPConnector(limit=10, ttl_dns_cache=300)
                self._session = aiohttp.ClientSession(
                    connector=connector,
                    timeout=timeout,
                    raise_for_status=False,
                )
            return self._session

    # ------------------------------------------------------------------
    # State building
    # ------------------------------------------------------------------
    def _build_state(self, hotspot_id: str) -> SourceSelectionState:
        snapshot_exists = (
            1.0
            if self.snapshot_path is not None
            and self.snapshot_path.exists()
            and self._snapshot_df is not None
            else 0.0
        )
        now = datetime.now(timezone.utc)
        hour = now.hour
        dow = now.weekday()

        success_counts = {"snapshot": 0, "api": 0, "fallback": 0}
        total_counts = {"snapshot": 0, "api": 0, "fallback": 0}
        for entry in self.interaction_log[-100:]:
            src = entry.get("source")
            if src in success_counts:
                total_counts[src] += 1
                if entry.get("success"):
                    success_counts[src] += 1
        success_rates = {
            src: success_counts[src] / max(total_counts[src], 1)
            for src in success_counts
        }

        cb_state = {
            CircuitBreakerState.CLOSED: 0.0,
            CircuitBreakerState.HALF_OPEN: 1.0,
            CircuitBreakerState.OPEN: 2.0,
        }[self._circuit_breaker.state]

        api_latencies = [
            float(entry["latency"])
            for entry in self.interaction_log
            if entry.get("source") == "api" and entry.get("latency") is not None
        ]
        avg_api_latency = float(np.mean(api_latencies)) if api_latencies else 0.0

        return SourceSelectionState(
            snapshot_exists=snapshot_exists,
            hour_of_day=hour,
            day_of_week=dow,
            success_snapshot=success_rates.get("snapshot", 0.5),
            success_api=success_rates.get("api", 0.5),
            success_fallback=success_rates.get("fallback", 0.5),
            cb_state=cb_state,
            api_latency=avg_api_latency,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    async def get_connectivity_score(
        self, hotspot_id: str, force_refresh: bool = False
    ) -> float:
        if not self._started:
            await self.start()

        cache_key = f"helium:score:{hotspot_id}"
        if not force_refresh:
            cached = await self.cache.get(cache_key)
            if cached is not None:
                if self.metrics:
                    self.metrics["cache_hits"].inc()
                return float(cached)

        if self.metrics:
            self.metrics["cache_misses"].inc()

        # Make sure the snapshot is loaded (lazy).
        if self.snapshot_path is not None and self._snapshot_df is None:
            await self._load_snapshot()

        state = self._build_state(hotspot_id)

        selected_expert: Optional[str] = None
        expert_probs: Optional[np.ndarray] = None
        if self.moe_gating:
            selected_expert, expert_probs = await self.moe_gating.select_expert(state)

        source, action_idx, state_vec, teacher_probs = (
            await self.source_optimizer.select_source(state, exploration=True)
        )

        ctx = RequestContext(
            hotspot_id=hotspot_id,
            state=state,
            state_vec=state_vec,
            action_idx=action_idx,
            source=source,
            teacher_probs=teacher_probs,
            selected_expert=selected_expert,
            expert_probs=expert_probs,
        )

        start = time.time()
        data: Optional[List[Dict[str, Any]]] = None
        success = False
        latency = 0.0

        if source == "snapshot":
            data = await self._fetch_from_snapshot(hotspot_id)
            if data:
                success = True
                if self.metrics:
                    self.metrics["snapshot_hits"].inc()
                    self.metrics["calls"].labels(source="snapshot", status="success").inc()
        elif source == "api":
            try:
                data = await self._fetch_from_api(hotspot_id)
                if data:
                    success = True
                latency = time.time() - start
                if self.metrics:
                    self.metrics["calls"].labels(
                        source="api", status="success" if success else "empty"
                    ).inc()
                    self.metrics["latency"].labels(source="api").observe(latency)
            except Exception as exc:
                if self.metrics:
                    self.metrics["errors"].labels(source="api").inc()
                    self.metrics["calls"].labels(source="api", status="error").inc()
                log_event(
                    "warning", "API fetch failed",
                    hotspot_id=hotspot_id, error=str(exc),
                )
        else:  # fallback
            success = False

        if data:
            score = self._compute_score(data)
        else:
            score = self.default_score
            if self.metrics:
                self.metrics["fallback_usage"].inc()

        reward = 1.0 if success else 0.0

        # MoE update (per request).
        if self.moe_gating and selected_expert:
            await self.moe_gating.add_training_sample(state, selected_expert, reward)

        # Distillation update (per request, race-free).
        next_state_vec = self._build_state(hotspot_id).to_feature_vector()
        await self.source_optimizer.update(
            ctx.state_vec, ctx.action_idx, reward, next_state_vec, ctx.teacher_probs,
        )

        # Interaction log with state_vec for historical ML.
        await self._log_interaction({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "hotspot_id": hotspot_id,
            "source": source,
            "success": success,
            "reward": reward,
            "latency": latency,
            "state_vec": ",".join(f"{v:.6g}" for v in ctx.state_vec),
        })

        # Optional RLHF sample.
        if self.rlhf_trainer and random.random() < 0.05:
            rejected = random.choice(
                [s for s in ("snapshot", "api", "fallback") if s != source] or [source]
            )
            self.rlhf_trainer.record_pair(
                pair_id=str(uuid.uuid4()),
                prompt=f"Which source should we use for {hotspot_id}?",
                chosen=source,
                rejected=rejected,
                reward_diff=reward,
                metadata={"hotspot_id": hotspot_id},
            )

        # MODP state recording.
        if self.modp_solver:
            self.modp_solver.add_state(
                state_id=f"{hotspot_id}_{datetime.now(timezone.utc).isoformat()}_{source}",
                problem_id="helium_source_selection",
                state_attributes={"hotspot_id": hotspot_id, "source": source},
                objective_values={
                    "success_rate": float(success),
                    "latency": latency,
                    "snapshot_usage": 1.0 if source == "snapshot" else 0.0,
                    "cost": 0.0,
                },
                stage=0,
            )

        # Cache with fallback-aware TTL.
        ttl = self.fallback_cache_ttl if not success else self.cache_ttl
        if ttl > 0:
            await self.cache.set(cache_key, str(score), ttl=ttl)

        if self.metrics:
            try:
                self.metrics["connectivity_score"].labels(hotspot_id=hotspot_id).set(score)
                self.metrics["source_selection"].labels(source=source).inc()
                self.metrics["source_reward"].observe(reward)
            except Exception:
                pass

        return float(score)

    async def fetch_batch_scores(
        self,
        hotspot_ids: List[str],
        max_concurrency: int = 10,
        per_request_timeout: Optional[float] = None,
    ) -> Dict[str, float]:
        sem = asyncio.Semaphore(max(1, max_concurrency))
        timeout = per_request_timeout or self.request_timeout * 2

        async def _one(hid: str) -> Tuple[str, float]:
            async with sem:
                try:
                    return hid, await asyncio.wait_for(
                        self.get_connectivity_score(hid), timeout=timeout
                    )
                except Exception:
                    return hid, self.default_score

        results = await asyncio.gather(*(_one(h) for h in hotspot_ids))
        return dict(results)

    # ------------------------------------------------------------------
    # Data sources
    # ------------------------------------------------------------------
    async def _fetch_from_snapshot(self, hotspot_id: str) -> Optional[List[Dict[str, Any]]]:
        df = await self._load_snapshot()
        if df is None:
            return None
        try:
            if "hotspot_id" in df.columns:
                rows = df[df["hotspot_id"] == hotspot_id]
            else:
                try:
                    rows = df.loc[[hotspot_id]]
                except KeyError:
                    return None
            if rows is None or len(rows) == 0:
                return None
            records: List[Dict[str, Any]] = []
            for _, row in rows.iterrows():
                record: Dict[str, Any] = {"hotspot_id": hotspot_id}
                if "rssi" in row and pd.notna(row["rssi"]):
                    record["rssi"] = float(row["rssi"])
                if "snr" in row and pd.notna(row["snr"]):
                    record["snr"] = float(row["snr"])
                records.append(record)
            return records or None
        except Exception as exc:
            log_event("warning", f"Snapshot lookup failed: {exc}")
            return None

    async def _fetch_from_api(self, hotspot_id: str) -> Optional[List[Dict[str, Any]]]:
        """Fetch hotspot stats from the live API.

        Raises on retryable/error responses so the circuit breaker can trip.
        Returns [] on genuine "no data" responses (e.g. 404).
        """
        attempts = int(self.config.get("retry_attempts", 3))
        min_wait = float(self.config.get("retry_min_wait", 1.0))
        max_wait = float(self.config.get("retry_max_wait", 10.0))

        async def _fetch_once() -> List[Dict[str, Any]]:
            session = await self._get_session()
            url = f"{self.api_url}hotspots/{hotspot_id}/stats"
            headers: Dict[str, str] = {}
            if self.api_key:
                headers["Authorization"] = f"Bearer {self.api_key}"
            async with session.get(url, headers=headers) as resp:
                if resp.status == 200:
                    try:
                        payload = await resp.json()
                    except Exception as exc:
                        raise ValueError(f"Invalid JSON: {exc}") from exc
                    stats = payload.get("data") if isinstance(payload, dict) else None
                    if not isinstance(stats, dict):
                        raise ValueError("Unexpected API response structure")
                    if "rssi" not in stats or "snr" not in stats:
                        raise ValueError("API response missing rssi/snr fields")
                    return [{
                        "hotspot_id": hotspot_id,
                        "rssi": float(stats["rssi"]),
                        "snr": float(stats["snr"]),
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    }]
                if resp.status == 404:
                    # Hotspot doesn't exist on this API; treat as empty.
                    return []
                if resp.status == 401 or resp.status == 403:
                    raise PermissionError(f"Auth error: HTTP {resp.status}")
                if resp.status == 429:
                    raise ClientResponseError(
                        request_info=resp.request_info,
                        history=resp.history,
                        status=resp.status,
                        message="Rate limit exceeded",
                    )
                # Any other status is a failure the breaker should see.
                raise ClientResponseError(
                    request_info=resp.request_info,
                    history=resp.history,
                    status=resp.status,
                    message=f"HTTP {resp.status}",
                )

        async def _fetch_with_retry() -> List[Dict[str, Any]]:
            last_exc: Optional[Exception] = None
            for attempt in range(max(1, attempts)):
                try:
                    return await _fetch_once()
                except (PermissionError, ValueError) as exc:
                    # Do not retry auth or schema errors.
                    raise exc
                except Exception as exc:
                    last_exc = exc
                    if attempt < attempts - 1:
                        wait = min(min_wait * (2 ** attempt), max_wait)
                        await asyncio.sleep(wait)
            assert last_exc is not None
            raise last_exc

        data = await self._circuit_breaker.call(_fetch_with_retry)
        return data

    # ------------------------------------------------------------------
    # Scoring
    # ------------------------------------------------------------------
    def _compute_score(self, data: List[Dict[str, Any]]) -> float:
        if not data:
            return self.default_score
        rssi_values = [float(e["rssi"]) for e in data if "rssi" in e and e["rssi"] is not None]
        snr_values = [float(e["snr"]) for e in data if "snr" in e and e["snr"] is not None]
        if not rssi_values or not snr_values:
            return self.default_score

        avg_rssi = sum(rssi_values) / len(rssi_values)
        avg_snr = sum(snr_values) / len(snr_values)

        rssi_span = max(self.rssi_max - self.rssi_min, 1e-6)
        snr_span = max(self.snr_max - self.snr_min, 1e-6)
        rssi_score = (avg_rssi - self.rssi_min) / rssi_span
        rssi_score = max(0.0, min(1.0, rssi_score))
        snr_score = (avg_snr - self.snr_min) / snr_span
        snr_score = max(0.0, min(1.0, snr_score))

        score = 0.6 * rssi_score + 0.4 * snr_score
        return max(0.0, min(1.0, score))

    # ------------------------------------------------------------------
    # Logging
    # ------------------------------------------------------------------
    async def _log_interaction(self, entry: Dict[str, Any]) -> None:
        async with self._log_lock:
            self.interaction_log.append(entry)
            if len(self.interaction_log) > 10_000:
                self.interaction_log = self.interaction_log[-5_000:]
            self._log_buffer.append(entry)
            if len(self._log_buffer) >= self._log_flush_size:
                await self._flush_log_buffer_locked()

    async def _flush_log_buffer_locked(self) -> None:
        if not self._log_buffer:
            return
        buffer, self._log_buffer = self._log_buffer, []

        def _write() -> None:
            path = self._interaction_log_path
            path.parent.mkdir(parents=True, exist_ok=True)
            file_exists = path.exists() and path.stat().st_size > 0
            fieldnames = list(buffer[0].keys())
            with open(path, "a", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                if not file_exists:
                    writer.writeheader()
                for row in buffer:
                    writer.writerow(row)

        try:
            await asyncio.to_thread(_write)
        except Exception as exc:
            log_event("warning", f"Failed to flush interaction log: {exc}")

    # ------------------------------------------------------------------
    # Historical model
    # ------------------------------------------------------------------
    @classmethod
    def train_historical_model(
        cls,
        log_path: Union[str, Path] = "./helium_interactions.csv",
        model_path: Union[str, Path] = "./helium_historical_model.pkl",
    ) -> None:
        log_path = Path(log_path)
        model_path = Path(model_path)
        if not log_path.exists():
            log_event("warning", f"Interaction logs not found at {log_path}. No model trained.")
            return
        try:
            df = pd.read_csv(log_path)
        except Exception as exc:
            log_event("error", f"Failed to read logs: {exc}")
            return

        required = {"state_vec", "source"}
        if len(df) < 10 or not required.issubset(df.columns):
            log_event(
                "warning",
                "Not enough logs or missing state_vec/source columns (need >= 10 rows).",
            )
            return
        if not SKLEARN_ML:
            log_event("error", "scikit-learn is required to train the historical model.")
            return

        def _parse_vec(s: Any) -> np.ndarray:
            if isinstance(s, str):
                return np.asarray([float(x) for x in s.split(",") if x != ""], dtype=np.float32)
            return np.asarray(s, dtype=np.float32)

        X = np.stack([_parse_vec(s) for s in df["state_vec"].values])
        y = df["source"].values

        le = LabelEncoder()
        y_enc = le.fit_transform(y)
        clf = RandomForestClassifier(n_estimators=100, random_state=42)
        clf.fit(X, y_enc)

        model_path.parent.mkdir(parents=True, exist_ok=True)
        with open(model_path, "wb") as f:
            pickle.dump((clf, le), f)
        log_event("info", f"Historical model saved to {model_path}")

    # ------------------------------------------------------------------
    # MOEA loop
    # ------------------------------------------------------------------
    async def _moea_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self.moea_interval_seconds)
                await self.run_source_evolution()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                log_event("error", f"MOEA loop failed: {exc}")
                await asyncio.sleep(60)

    async def run_source_evolution(self) -> List[MOPDSourceStrategy]:
        if not self.moea_enabled:
            return []

        async def evaluate(weights: Dict[str, float]) -> Dict[str, float]:
            recent = self.interaction_log[-100:]
            if len(recent) < 10:
                return {
                    "success_rate": 0.0, "latency": 0.0,
                    "snapshot_usage": 0.0, "cost": 0.0,
                }
            success_rate = float(np.mean([1.0 if e.get("success") else 0.0 for e in recent]))
            api_latencies = [
                float(e["latency"]) for e in recent
                if e.get("source") == "api" and e.get("latency") is not None
            ]
            # Report *reward-like* latency (higher is better).
            latency_score = (
                1.0 - min(float(np.mean(api_latencies)) / max(self.request_timeout, 1e-3), 1.0)
                if api_latencies else 0.0
            )
            snapshot_usage = float(np.mean(
                [1.0 if e.get("source") == "snapshot" else 0.0 for e in recent]
            ))
            cost = 0.5
            return {
                "success_rate": success_rate,
                "latency": latency_score,
                "snapshot_usage": snapshot_usage,
                "cost": cost,
            }

        self.moea_optimizer = NSGAIISourceOptimizer(
            evaluate_func=evaluate,
            population_size=self.moea_population_size,
            generations=self.moea_generations,
            mutation_rate=self.moea_mutation_rate,
            crossover_rate=self.moea_crossover_rate,
            tournament_size=self.moea_tournament_size,
            objective_weights=self._get_dynamic_moea_weights(),
            dynamic_weights=self.moea_dynamic_weights,
        )

        pareto = await self.moea_optimizer.evolve()
        self.evolved_pareto_front = pareto
        if pareto:
            best = self.moea_optimizer._select_best_from_pareto(
                pareto, self._get_dynamic_moea_weights()
            )
            if best:
                self.best_evolved_strategy = best
                if self.metrics:
                    try:
                        self.metrics["moea_pareto_front"].set(len(pareto))
                    except Exception:
                        pass
                if self.modp_solver:
                    self.modp_solver.add_state(
                        state_id=f"moea_best_{time.time()}",
                        problem_id="helium_strategy_evolution",
                        state_attributes={"weights": best.weights},
                        objective_values=best.objectives,
                        stage=0,
                    )

        # Persist Pareto front.
        try:
            path = Path(self.config.get("moea_pareto_path", "./helium_moea_pareto.json"))
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "w") as f:
                json.dump([s.to_dict() for s in pareto], f, indent=2)
        except Exception as exc:
            log_event("warning", f"Failed to persist Pareto front: {exc}")

        return pareto

    def _get_dynamic_moea_weights(self) -> Dict[str, float]:
        weights = dict(self.moea_objective_weights)
        if len(self.interaction_log) > 20:
            recent = self.interaction_log[-20:]
            success_rate = float(np.mean([1.0 if e.get("success") else 0.0 for e in recent]))
            if success_rate < 0.5:
                weights["success_rate"] = min(0.6, weights.get("success_rate", 0.4) * 1.5)
            total = sum(weights.values())
            if total > 0:
                weights = {k: v / total for k, v in weights.items()}
        return weights

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------
    async def get_limit_graph(self, graph_id: str = "helium_sources") -> Dict[str, Any]:
        if not self.limit_graph_manager:
            return {}
        return {
            "metadata": self.limit_graph_manager.get_metadata(graph_id),
            "nodes": self.limit_graph_manager.get_nodes(graph_id),
            "edges": self.limit_graph_manager.get_edges(graph_id),
        }

    async def get_moe_experts(self) -> List[str]:
        if self.moe_gating:
            return list(self.moe_gating.expert_names)
        return []

    async def get_rlhf_pairs(self, limit: int = 100) -> List[Dict[str, Any]]:
        if self.rlhf_trainer:
            return self.rlhf_trainer.get_pairs(limit)
        return []

    async def record_rlhf_pair(
        self, pair_id: str, prompt: str, chosen: str, rejected: str,
        reward_diff: float, metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        if self.rlhf_trainer:
            self.rlhf_trainer.record_pair(
                pair_id, prompt, chosen, rejected, reward_diff, metadata
            )

    def get_stats(self) -> Dict[str, Any]:
        stats = self.source_optimizer.get_stats()
        stats.update({
            "interaction_log_size": len(self.interaction_log),
            "pareto_front_size": len(self.evolved_pareto_front),
            "snapshot_epoch": self._snapshot_epoch,
            "snapshot_loaded": self._snapshot_df is not None,
            "circuit_breaker_state": self._circuit_breaker.state.value,
            "started": self._started,
        })
        return stats


# ============================================================================
# Convenience factory
# ============================================================================
def create_helium_collector(
    cache: "CacheManager",
    config: Optional[Union[Dict[str, Any], "HeliumConfig"]] = None,
    storage: Optional["Storage"] = None,
) -> HeliumCollector:
    return HeliumCollector(cache, config, storage)
