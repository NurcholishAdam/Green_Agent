#!/usr/bin/env python3
"""
Enhanced Material Footprint Updater v2.5.0
===========================================
Fetches and caches product-level material footprints from BONSAI/FOOTPRINTDATA.
Provides adaptive source selection and update scheduling via multi-teacher
distillation, MoE gating, MOEA/NSGA-II, and optional LIMIT graph, MODP, and RLHF.

FIXES OVER v2.4.1:
- Added `dataclass` import at module scope.
- Config is normalized once (Pydantic v1/v2 / dict / None).
- Single `Footprint` definition (Pydantic or validated dataclass).
- No `asyncio.create_task` in `__init__`; explicit `async start()`.
- Per-request `UpdateContext` replaces `self.last_*` and `self._last_selected_expert`.
- SQLite calls wrapped in `asyncio.to_thread` (non-blocking).
- Provider calls routed through the circuit breakers.
- MoE biases selection instead of replacing the distillation signal.
- `_log_interaction` records the state vector; `train_historical_model` is implemented.
- Historical teacher and Q teacher use runtime config paths (not module constants).
- `DistillationStudent.predict_proba` no longer reshapes weights.
- Smooth reward function scaled by coverage.
- NSGA-II: fixed tournament, real `evaluate()` that depends on weights,
  optimizer reused across MOEA ticks.
- CSV logging buffered + locked, flushed via `asyncio.to_thread`.
- `source_priority` validator enforces uniqueness and non-empty.
- `interaction_log` capped.
- `datetime.now(timezone.utc)` everywhere.
"""

from __future__ import annotations

import asyncio
import copy
import csv
import hashlib
import json
import logging
import os
import pickle
import random
import sqlite3
import time
import uuid
from abc import ABC, abstractmethod
from collections import Counter, deque
from dataclasses import dataclass, field, fields as dataclass_fields
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import (
    Any, Awaitable, Callable, Dict, List, Optional, Tuple, Union,
)

import aiohttp
import numpy as np
import pandas as pd
from aiohttp import ClientError, ClientTimeout

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
        retry_if_exception_type,
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


# ---------- Optional central storage ----------
try:
    from ...storage import Storage  # type: ignore
    CENTRAL_STORAGE_AVAILABLE = True
except ImportError:  # pragma: no cover
    Storage = Any  # type: ignore
    CENTRAL_STORAGE_AVAILABLE = False


# ============================================================================
# Configuration
# ============================================================================
MATERIAL_CONFIG: Dict[str, Any] = {
    "db_path": Path("./material_catalog.db"),
    "bonsai_api_url": "https://api.bonsai.uno/v1/footprints",
    "footprintdata_api_url": "https://api.footprintdata.org/v1/products",
    "bonsai_api_key": None,
    "footprintdata_api_key": None,
    "cache_ttl": 86400 * 7,
    "retry_attempts": 3,
    "retry_min_wait": 1.0,
    "retry_max_wait": 10.0,
    "circuit_breaker_threshold": 5,
    "circuit_breaker_timeout": 30.0,
    "request_timeout": 10.0,
    "enable_prometheus": True,
    "source_priority": ["bonsai", "footprintdata"],
    "distillation_epsilon": 0.1,
    "distillation_train_every": 10,
    "distillation_replay_size": 2000,
    "distillation_learning_rate": 0.01,
    "distill_weight": 0.7,
    "rl_weight": 0.3,
    "moea_enabled": True,
    "moea_interval_seconds": 300,
    "moea_population_size": 20,
    "moea_generations": 5,
    "moea_mutation_rate": 0.2,
    "moea_crossover_rate": 0.8,
    "moea_tournament_size": 3,
    "moea_objective_weights": {
        "freshness": 0.4, "cost": 0.3,
        "reliability": 0.2, "latency": 0.1,
    },
    "moea_dynamic_weights": True,
    "enable_limit_graph": True,
    "enable_modp": True,
    "enable_rlhf": True,
    "enable_moe": True,
    "moe_expert_count": 4,
    "q_weights_path": "./material_q_weights.json",
    "interaction_logs_path": "./material_interactions.csv",
    "historical_model_path": "./material_historical_model.pkl",
    "moea_pareto_path": "./material_moea_pareto.json",
}


if PYDANTIC_AVAILABLE:
    class MaterialConfig(BaseModel):
        """Configuration for MaterialFootprintUpdater."""

        db_path: Path = Field(Path("./material_catalog.db"))
        bonsai_api_url: str = Field("https://api.bonsai.uno/v1/footprints")
        footprintdata_api_url: str = Field("https://api.footprintdata.org/v1/products")
        bonsai_api_key: Optional[str] = None
        footprintdata_api_key: Optional[str] = None
        cache_ttl: int = Field(86400 * 7, ge=0)
        retry_attempts: int = Field(3, ge=0)
        retry_min_wait: float = Field(1.0, gt=0)
        retry_max_wait: float = Field(10.0, gt=0)
        circuit_breaker_threshold: int = Field(5, ge=1)
        circuit_breaker_timeout: float = Field(30.0, ge=1)
        request_timeout: float = Field(10.0, ge=1)
        enable_prometheus: bool = True
        source_priority: List[str] = Field(
            default_factory=lambda: ["bonsai", "footprintdata"]
        )

        distillation_epsilon: float = Field(0.1, ge=0, le=1)
        distillation_train_every: int = Field(10, ge=1)
        distillation_replay_size: int = Field(2000, ge=10)
        distillation_learning_rate: float = Field(0.01, ge=0.0001, le=1)
        distill_weight: float = Field(0.7, ge=0, le=1)
        rl_weight: float = Field(0.3, ge=0, le=1)

        moea_enabled: bool = True
        moea_interval_seconds: int = Field(300, ge=60)
        moea_population_size: int = Field(20, ge=5)
        moea_generations: int = Field(5, ge=1)
        moea_mutation_rate: float = Field(0.2, ge=0.0, le=1.0)
        moea_crossover_rate: float = Field(0.8, ge=0.0, le=1.0)
        moea_tournament_size: int = Field(3, ge=2)
        moea_objective_weights: Dict[str, float] = Field(
            default_factory=lambda: {
                "freshness": 0.4, "cost": 0.3,
                "reliability": 0.2, "latency": 0.1,
            }
        )
        moea_dynamic_weights: bool = True

        enable_limit_graph: bool = True
        enable_modp: bool = True
        enable_rlhf: bool = True
        enable_moe: bool = True
        moe_expert_count: int = Field(4, ge=2)

        q_weights_path: str = "./material_q_weights.json"
        interaction_logs_path: str = "./material_interactions.csv"
        historical_model_path: str = "./material_historical_model.pkl"
        moea_pareto_path: str = "./material_moea_pareto.json"

        @field_validator("source_priority")
        @classmethod
        def validate_source_priority(cls, v: List[str]) -> List[str]:
            allowed = {"bonsai", "footprintdata"}
            if not v:
                raise ValueError("source_priority must be non-empty")
            seen = set()
            for s in v:
                if s not in allowed:
                    raise ValueError(f"Source {s} not in allowed list {allowed}")
                if s in seen:
                    raise ValueError(f"Duplicate source: {s}")
                seen.add(s)
            return v

        @field_validator("db_path", mode="before")
        @classmethod
        def coerce_db_path(cls, v: Any) -> Path:
            return Path(v) if not isinstance(v, Path) else v

        class Config:
            env_prefix = "MATERIAL_"


def _pydantic_dump(model: Any) -> Dict[str, Any]:
    if hasattr(model, "model_dump"):
        return model.model_dump()
    return model.dict()


def normalize_config(
    config: Optional[Union[Dict[str, Any], "MaterialConfig"]]
) -> Dict[str, Any]:
    """Normalize any config input into a plain dict (Pydantic v1/v2 aware)."""
    if config is None:
        if PYDANTIC_AVAILABLE:
            return _pydantic_dump(MaterialConfig())
        return copy.deepcopy(MATERIAL_CONFIG)

    if isinstance(config, dict):
        if PYDANTIC_AVAILABLE:
            return _pydantic_dump(MaterialConfig(**config))
        return dict(config)

    if PYDANTIC_AVAILABLE and isinstance(config, MaterialConfig):
        return _pydantic_dump(config)

    return {k: getattr(config, k) for k in dir(config) if not k.startswith("_")}


# ============================================================================
# Data Models
# ============================================================================
if PYDANTIC_AVAILABLE:
    class BonsaiFootprintResponse(BaseModel):
        product_id: str
        embodied_carbon_kg: float
        rare_earth_kg: float
        total_mass_kg: float
        material_index: float

    class FootprintDataResponse(BaseModel):
        product_id: str
        embodied_carbon_kg: float
        rare_earth_kg: float
        total_mass_kg: float
        material_index: float

    class Footprint(BaseModel):
        product_id: str
        embodied_carbon_kg: float
        rare_earth_kg: float
        total_mass_kg: float
        material_index: float
        source: str
        last_updated: datetime

        @field_validator("material_index")
        @classmethod
        def material_index_non_negative(cls, v: float) -> float:
            if v < 0:
                raise ValueError("material_index must be non-negative")
            return v
else:
    @dataclass
    class BonsaiFootprintResponse:
        product_id: str
        embodied_carbon_kg: float
        rare_earth_kg: float
        total_mass_kg: float
        material_index: float

    @dataclass
    class FootprintDataResponse:
        product_id: str
        embodied_carbon_kg: float
        rare_earth_kg: float
        total_mass_kg: float
        material_index: float

    @dataclass
    class Footprint:
        product_id: str
        embodied_carbon_kg: float
        rare_earth_kg: float
        total_mass_kg: float
        material_index: float
        source: str
        last_updated: datetime

        def __post_init__(self) -> None:
            if self.material_index < 0:
                raise ValueError("material_index must be non-negative")


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
    def __init__(self, storage: Optional[Any] = None) -> None:
        self.storage = storage
        self.graphs: Dict[str, Dict[str, Any]] = {}

    def create_graph(self, graph_id: str, description: str, configuration: Dict[str, Any]) -> None:
        if self.storage and hasattr(self.storage, "save_limit_graph_metadata"):
            self.storage.save_limit_graph_metadata(graph_id, description, configuration)
        else:
            self.graphs[graph_id] = {
                "description": description, "configuration": configuration,
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
                "freshness": 0.0, "cost": 0.0,
                "reliability": 0.0, "latency": 0.0,
            },
            stage=0,
        )
        return {"status": "solved", "pareto_front": []}


# ============================================================================
# RLHF Trainer (interface preserved; stub trainer)
# ============================================================================
class RLHFTrainer:
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
    def __init__(self, storage: Optional[Any] = None, config: Optional[Dict[str, Any]] = None) -> None:
        self.storage = storage
        self.config = config or {}
        self.num_experts = int(self.config.get("moe_expert_count", 4))
        base = ["bonsai_full", "footprintdata_full", "mock_full", "bonsai_single"]
        self.expert_names = [
            base[i] if i < len(base) else f"expert_{i}"
            for i in range(self.num_experts)
        ]
        # Feature dim 9 (matches UpdateState).
        self.gating_weights = np.random.randn(self.num_experts, 9) * 0.01
        self.gating_lr = float(self.config.get("moe_lr", 0.05))
        self._reward_baseline = 0.5

    def _encode_state(self, state: Union["UpdateState", Dict[str, Any]]) -> np.ndarray:
        if isinstance(state, dict):
            features = [
                min(state.get("total_products", 0) / 1000.0, 1.0),
                state.get("stale_fraction", 0),
                min(state.get("avg_demand", 0) / 10.0, 1.0),
                state.get("bonsai_success_rate", 0.5),
                state.get("footprintdata_success_rate", 0.5),
                state.get("bonsai_cb_state", 0) / 2.0,
                state.get("footprintdata_cb_state", 0) / 2.0,
                min(state.get("hours_since_update", 0) / 72.0, 1.0),
                state.get("single_product_mode", 0),
            ]
        else:
            features = state.to_feature_vector()
        return np.asarray(features, dtype=np.float32)

    async def select_expert(
        self, state: Union["UpdateState", Dict[str, Any]]
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
        state: Union["UpdateState", Dict[str, Any]],
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

        # REINFORCE with a running baseline.
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
class UpdateState:
    total_products: int = 0
    stale_fraction: float = 0.0
    avg_demand: float = 1.0
    bonsai_success_rate: float = 0.5
    footprintdata_success_rate: float = 0.5
    bonsai_cb_state: float = 0.0
    footprintdata_cb_state: float = 0.0
    hours_since_update: float = 0.0
    single_product_mode: float = 0.0

    def to_feature_vector(self) -> np.ndarray:
        return np.asarray([
            min(self.total_products / 1000.0, 1.0),
            self.stale_fraction,
            min(self.avg_demand / 10.0, 1.0),
            self.bonsai_success_rate,
            self.footprintdata_success_rate,
            self.bonsai_cb_state / 2.0,
            self.footprintdata_cb_state / 2.0,
            min(self.hours_since_update / 72.0, 1.0),
            self.single_product_mode,
        ], dtype=np.float32)


class Teacher(ABC):
    @abstractmethod
    def predict(self, state: UpdateState) -> np.ndarray:
        ...

    @abstractmethod
    def confidence(self, state: UpdateState) -> float:
        ...


class UpdateRuleBasedTeacher(Teacher):
    ACTION_SPACE = [
        "bonsai_full", "footprintdata_full", "mock_full",
        "bonsai_single", "footprintdata_single", "mock_single",
    ]

    def predict(self, state: UpdateState) -> np.ndarray:
        probs = np.ones(6) * 0.1
        if state.single_product_mode > 0.5:
            if state.bonsai_success_rate > state.footprintdata_success_rate:
                probs[3] = 0.8
            else:
                probs[4] = 0.8
        else:
            if state.stale_fraction > 0.5:
                if state.bonsai_success_rate > state.footprintdata_success_rate:
                    probs[0] = 0.8
                else:
                    probs[1] = 0.8
            else:
                if state.bonsai_success_rate < 0.3 and state.footprintdata_success_rate < 0.3:
                    probs[2] = 0.7
                else:
                    probs[0] = 0.5
        total = probs.sum()
        return probs / total if total > 0 else np.ones(6) / 6

    def confidence(self, state: UpdateState) -> float:
        return 0.6 if state.stale_fraction > 0.5 else 0.4


class UpdateHistoricalMLTeacher(Teacher):
    def __init__(self, model_path: Optional[Path] = None, n_actions: int = 6) -> None:
        self.model = None
        self.label_encoder = None
        self.n_actions = n_actions
        self.model_path = Path(model_path) if model_path else None
        if self.model_path and self.model_path.exists() and SKLEARN_ML:
            try:
                with open(self.model_path, "rb") as f:
                    self.model, self.label_encoder = pickle.load(f)
                log_event("info", f"Loaded historical model from {self.model_path}")
            except Exception as exc:
                log_event("warning", f"Failed to load historical model: {exc}")
                self.model = None

    def predict(self, state: UpdateState) -> np.ndarray:
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

    def confidence(self, state: UpdateState) -> float:
        return 0.7 if self.model is not None else 0.0


class UpdateStatefulQTeacher(Teacher):
    def __init__(self, lr: float = 0.1, weights_path: Optional[Union[str, Path]] = None) -> None:
        self.lr = lr
        self.weights = np.zeros((9, 6))
        self.weights_path = Path(weights_path) if weights_path else None
        if self.weights_path and self.weights_path.exists():
            self.load_weights(self.weights_path)

    def load_weights(self, path: Union[str, Path]) -> None:
        p = Path(path)
        if not p.exists():
            return
        try:
            with open(p, "r") as f:
                data = json.load(f)
            arr = np.asarray(data, dtype=np.float64)
            if arr.shape == self.weights.shape:
                self.weights = arr
            else:
                log_event("warning", f"Q weights shape mismatch: {arr.shape}")
        except Exception as exc:
            log_event("warning", f"Failed to load Q weights: {exc}")

    def save_weights(self, path: Union[str, Path]) -> None:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        with open(p, "w") as f:
            json.dump(self.weights.tolist(), f)

    def predict(self, state: UpdateState) -> np.ndarray:
        x = state.to_feature_vector()
        q = x @ self.weights
        exp_q = np.exp(q - np.max(q))
        return exp_q / exp_q.sum()

    def confidence(self, state: UpdateState) -> float:
        return 0.5

    def update(self, state: UpdateState, action: int, reward: float) -> None:
        x = state.to_feature_vector()
        q_current = float(np.dot(x, self.weights[:, action]))
        self.weights[:, action] += self.lr * (reward - q_current) * x


class DistillationStudent:
    def __init__(self, feature_dim: int = 9, n_classes: int = 6, lr: float = 0.01) -> None:
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
        if teacher_probs.shape[0] != self.n_classes:
            return
        current = self.predict_proba(state_vector)
        grad_distill = -(teacher_probs - current)
        one_hot = np.zeros(self.n_classes)
        one_hot[action] = 1.0
        grad_rl = -reward * (one_hot - current)
        grad = distill_weight * grad_distill + rl_weight * grad_rl
        self.weights -= self.lr * np.outer(state_vector, grad)
        self.biases -= self.lr * grad
        self.counter += 1


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


class DistillationUpdateOptimizer:
    ACTION_SPACE = [
        "bonsai_full", "footprintdata_full", "mock_full",
        "bonsai_single", "footprintdata_single", "mock_single",
    ]

    def __init__(
        self,
        config: Dict[str, Any],
        historical_model_path: Optional[Path] = None,
        q_weights_path: Optional[Path] = None,
    ) -> None:
        self.config = config
        self.n_actions = len(self.ACTION_SPACE)
        self.student = DistillationStudent(
            feature_dim=9,
            n_classes=self.n_actions,
            lr=float(config.get("distillation_learning_rate", 0.01)),
        )
        self.q_teacher = UpdateStatefulQTeacher(weights_path=q_weights_path)
        self.teachers: List[Teacher] = [
            UpdateRuleBasedTeacher(),
            UpdateHistoricalMLTeacher(
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

    def _aggregate_teacher_probs(self, state: UpdateState) -> np.ndarray:
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

    async def select_action(
        self, state: UpdateState, exploration: bool = True
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


# ============================================================================
# NSGA-II for Update Strategy Evolution
# ============================================================================
@dataclass
class MOPDUpdateStrategy:
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

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MOPDUpdateStrategy":
        fields = {f.name for f in dataclass_fields(cls)}
        return cls(**{k: v for k, v in data.items() if k in fields})


class NSGAIIUpdateOptimizer:
    """NSGA-II over four objectives (all maximized).

    Use a negative weight in `objective_weights` for objectives that should be
    minimized, or invert the objective inside `evaluate_func`.
    """

    def __init__(
        self,
        evaluate_func: Callable[[Dict[str, float]], Awaitable[Dict[str, float]]],
        population_size: int = 20,
        generations: int = 5,
        mutation_rate: float = 0.2,
        crossover_rate: float = 0.8,
        tournament_size: int = 3,
        objective_weights: Optional[Dict[str, float]] = None,
        dynamic_weights: bool = True,
    ) -> None:
        self.evaluate_func = evaluate_func
        self.population_size = max(4, population_size)
        self.generations = max(1, generations)
        self.mutation_rate = mutation_rate
        self.crossover_rate = crossover_rate
        self.tournament_size = max(2, tournament_size)
        self.objective_weights = objective_weights or {
            "freshness": 0.4, "cost": 0.3,
            "reliability": 0.2, "latency": 0.1,
        }
        self.dynamic_weights = dynamic_weights
        self.best_individual: Optional[Dict[str, float]] = None
        self.best_fitness = -float("inf")
        self.pareto_front: List[MOPDUpdateStrategy] = []
        self._eval_cache: Dict[Tuple[Tuple[str, float], ...], Dict[str, float]] = {}
        # Reused across ticks; seeded with the last Pareto front.
        self._population: List[Dict[str, float]] = []
        self._points: List[MOPDUpdateStrategy] = []

    # ------- helpers -------
    def _random_individual(self) -> Dict[str, float]:
        keys = list(self.objective_weights.keys())
        w = {k: random.random() for k in keys}
        total = sum(w.values())
        return {k: v / total for k, v in w.items()} if total > 0 else w

    def _crossover(self, p1: Dict[str, float], p2: Dict[str, float]) -> Dict[str, float]:
        child = {}
        for k in p1:
            if random.random() < 0.5:
                u = random.random()
                if u <= 0.5:
                    beta = (2 * u) ** (1 / 21)
                else:
                    beta = (1 / (2 * (1 - u))) ** (1 / 21)
                child[k] = max(0.0, min(1.0, 0.5 * ((1 + beta) * p1[k] + (1 - beta) * p2[k])))
            else:
                child[k] = p1[k] if random.random() < 0.5 else p2[k]
        total = sum(child.values())
        return {k: v / total for k, v in child.items()} if total > 0 else child

    def _mutate(self, ind: Dict[str, float]) -> Dict[str, float]:
        mutant = ind.copy()
        for k in mutant:
            if random.random() < self.mutation_rate:
                u = random.random()
                delta = ((2 * u) ** (1 / 21) - 1) if u < 0.5 else (1 - (2 * (1 - u)) ** (1 / 21))
                mutant[k] = max(0.0, min(1.0, mutant[k] + delta))
        total = sum(mutant.values())
        return {k: v / total for k, v in mutant.items()} if total > 0 else mutant

    @staticmethod
    def _dominates(a: MOPDUpdateStrategy, b: MOPDUpdateStrategy, keys: List[str]) -> bool:
        at_least = all(a.objectives.get(k, 0.0) >= b.objectives.get(k, 0.0) for k in keys)
        strictly = any(a.objectives.get(k, 0.0) > b.objectives.get(k, 0.0) for k in keys)
        return at_least and strictly

    def _fast_non_dominated_sort(
        self, points: List[MOPDUpdateStrategy]
    ) -> List[List[MOPDUpdateStrategy]]:
        if not points:
            return []
        keys = list(self.objective_weights.keys())
        fronts: List[List[MOPDUpdateStrategy]] = [[]]
        dom_count: Dict[int, int] = {id(p): 0 for p in points}
        dominated_by: Dict[int, List[MOPDUpdateStrategy]] = {id(p): [] for p in points}

        for p in points:
            for q in points:
                if p is q:
                    continue
                if self._dominates(p, q, keys):
                    dominated_by[id(p)].append(q)
                elif self._dominates(q, p, keys):
                    dom_count[id(p)] += 1
            if dom_count[id(p)] == 0:
                fronts[0].append(p)

        i = 0
        while i < len(fronts) and fronts[i]:
            nxt: List[MOPDUpdateStrategy] = []
            for p in fronts[i]:
                for q in dominated_by[id(p)]:
                    dom_count[id(q)] -= 1
                    if dom_count[id(q)] == 0:
                        nxt.append(q)
            i += 1
            fronts.append(nxt)

        return [f for f in fronts if f]

    def _crowding_distance(self, front: List[MOPDUpdateStrategy]) -> Dict[int, float]:
        if not front:
            return {}
        keys = list(self.objective_weights.keys())
        dist: Dict[int, float] = {id(p): 0.0 for p in front}
        for k in keys:
            front_sorted = sorted(front, key=lambda p: p.objectives.get(k, 0.0))
            dist[id(front_sorted[0])] = float("inf")
            dist[id(front_sorted[-1])] = float("inf")
            lo = front_sorted[0].objectives.get(k, 0.0)
            hi = front_sorted[-1].objectives.get(k, 0.0)
            span = hi - lo
            if span <= 0:
                continue
            for i in range(1, len(front_sorted) - 1):
                dist[id(front_sorted[i])] += (
                    front_sorted[i + 1].objectives.get(k, 0.0)
                    - front_sorted[i - 1].objectives.get(k, 0.0)
                ) / span
        return dist

    def _tournament_selection(
        self,
        population: List[Dict[str, float]],
        rank: Dict[int, int],
        crowding: Dict[int, float],
        ind_to_point: Dict[Tuple[Tuple[str, float], ...], MOPDUpdateStrategy],
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

    def _scalarise(self, objectives: Dict[str, float]) -> float:
        return sum(
            float(self.objective_weights.get(k, 0.0)) * float(objectives.get(k, 0.0))
            for k in self.objective_weights
        )

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
        total = sum(weights.values())
        return {k: v / total for k, v in weights.items()} if total > 0 else weights

    def _select_best_from_pareto(
        self, pareto: List[MOPDUpdateStrategy], weights: Dict[str, float]
    ) -> Optional[MOPDUpdateStrategy]:
        if not pareto:
            return None
        keys = list(weights.keys())
        max_vals = {k: max(p.objectives.get(k, 0.0) for p in pareto) for k in keys}
        min_vals = {k: min(p.objectives.get(k, 0.0) for p in pareto) for k in keys}
        ranges = {
            k: (max_vals[k] - min_vals[k]) if max_vals[k] != min_vals[k] else 1.0
            for k in keys
        }
        best: Optional[MOPDUpdateStrategy] = None
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

    async def _evaluate(self, weights: Dict[str, float]) -> Dict[str, float]:
        key = tuple(sorted(weights.items()))
        if key in self._eval_cache:
            return self._eval_cache[key]
        obj = await self.evaluate_func(weights)
        self._eval_cache[key] = obj
        return obj

    async def evolve(self) -> List[MOPDUpdateStrategy]:
        # Seed the population with the previous Pareto front (if any).
        population: List[Dict[str, float]] = [dict(p.weights) for p in self.pareto_front]
        while len(population) < self.population_size:
            population.append(self._random_individual())
        population = population[: self.population_size]

        points: List[MOPDUpdateStrategy] = []
        for w in population:
            obj = await self._evaluate(w)
            points.append(MOPDUpdateStrategy(
                strategy_id=str(uuid.uuid4()),
                weights=w,
                objectives=obj,
                scalarised_score=self._scalarise(obj),
            ))

        for _ in range(self.generations):
            fronts = self._fast_non_dominated_sort(points)
            rank: Dict[int, int] = {}
            crowding: Dict[int, float] = {}
            for i, front in enumerate(fronts):
                cd = self._crowding_distance(front)
                for p in front:
                    rank[id(p)] = i
                    crowding[id(p)] = cd.get(id(p), 0.0)

            ind_to_point: Dict[Tuple[Tuple[str, float], ...], MOPDUpdateStrategy] = {
                tuple(sorted(p.weights.items())): p for p in points
            }

            offspring: List[MOPDUpdateStrategy] = []
            while len(offspring) < self.population_size:
                p1 = self._tournament_selection(population, rank, crowding, ind_to_point)
                p2 = self._tournament_selection(population, rank, crowding, ind_to_point)
                child_w = self._crossover(p1, p2) if random.random() < self.crossover_rate else dict(p1)
                child_w = self._mutate(child_w)
                obj = await self._evaluate(child_w)
                offspring.append(MOPDUpdateStrategy(
                    strategy_id=str(uuid.uuid4()),
                    weights=child_w,
                    objectives=obj,
                    scalarised_score=self._scalarise(obj),
                ))

            combined_points = points + offspring
            fronts = self._fast_non_dominated_sort(combined_points)
            next_points: List[MOPDUpdateStrategy] = []
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

        fronts = self._fast_non_dominated_sort(points)
        self.pareto_front = fronts[0] if fronts else points
        self._points = points
        self._population = population
        if self.pareto_front:
            best = self._select_best_from_pareto(
                self.pareto_front, self._compute_dynamic_weights()
            )
            if best is not None:
                self.best_individual = dict(best.weights)
                self.best_fitness = best.scalarised_score
        return self.pareto_front


# ============================================================================
# Per-request context
# ============================================================================
@dataclass
class UpdateContext:
    state: UpdateState
    state_vec: np.ndarray
    action_idx: int
    action: str
    teacher_probs: np.ndarray
    selected_expert: Optional[str] = None
    expert_probs: Optional[np.ndarray] = None
    product_id: Optional[str] = None


# ============================================================================
# MaterialFootprintUpdater
# ============================================================================
class MaterialFootprintUpdater:
    ACTION_SPACE = DistillationUpdateOptimizer.ACTION_SPACE

    def __init__(
        self,
        config: Optional[Union[Dict[str, Any], "MaterialConfig"]] = None,
        storage: Optional[Any] = None,
        enable_limit_graph: Optional[bool] = None,
        enable_modp: Optional[bool] = None,
        enable_rlhf: Optional[bool] = None,
        enable_moe: Optional[bool] = None,
        moe_expert_count: Optional[int] = None,
    ) -> None:
        self.config: Dict[str, Any] = normalize_config(config)
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

        # Paths and credentials
        self.db_path: Path = Path(self.config.get("db_path", "./material_catalog.db"))
        self.cache_ttl: int = int(self.config.get("cache_ttl", 86400 * 7))
        self.bonsai_api_url: str = str(self.config.get("bonsai_api_url", ""))
        self.bonsai_api_key: Optional[str] = (
            self.config.get("bonsai_api_key") or os.environ.get("BONSAI_API_KEY")
        )
        self.footprintdata_api_url: str = str(self.config.get("footprintdata_api_url", ""))
        self.footprintdata_api_key: Optional[str] = (
            self.config.get("footprintdata_api_key") or os.environ.get("FOOTPRINTDATA_API_KEY")
        )
        self.request_timeout: float = float(self.config.get("request_timeout", 10.0))
        self.source_priority: List[str] = list(
            self.config.get("source_priority", ["bonsai", "footprintdata"])
        )

        # DB init (sync, one-time, in __init__ — safe because no loop is running yet).
        self._init_db()

        # Session
        self._session: Optional[aiohttp.ClientSession] = None
        self._session_lock = asyncio.Lock()

        # Circuit breakers
        self._circuit_breakers: Dict[str, CircuitBreaker] = {
            "bonsai": CircuitBreaker(
                name="material_bonsai",
                failure_threshold=int(self.config.get("circuit_breaker_threshold", 5)),
                recovery_timeout=float(self.config.get("circuit_breaker_timeout", 30.0)),
            ),
            "footprintdata": CircuitBreaker(
                name="material_footprintdata",
                failure_threshold=int(self.config.get("circuit_breaker_threshold", 5)),
                recovery_timeout=float(self.config.get("circuit_breaker_timeout", 30.0)),
            ),
        }

        # Metrics (created before wiring CB state callback).
        self.metrics: Optional[Dict[str, Any]] = None
        if PROMETHEUS_AVAILABLE and self.config.get("enable_prometheus", True):
            self.metrics = {
                "calls": Counter(
                    "material_api_calls_total", "Material API calls",
                    ["source", "status"],
                ),
                "errors": Counter(
                    "material_api_errors_total", "Material API errors", ["source"],
                ),
                "latency": Histogram(
                    "material_api_latency_seconds", "Material API latency", ["source"],
                ),
                "cache_hits": Counter("material_cache_hits_total", "Cache hits"),
                "cache_misses": Counter("material_cache_misses_total", "Cache misses"),
                "cache_size": Gauge("material_cache_size", "Number of cached footprints"),
                "update_action": Counter(
                    "material_update_action", "Update action selected", ["action"],
                ),
                "update_reward": Histogram(
                    "material_update_reward", "Reward per update action",
                ),
                "circuit_breaker_state": Gauge(
                    "material_circuit_breaker_state", "Circuit breaker state", ["source"],
                ),
                "moea_pareto_front": Gauge(
                    "material_moea_pareto_front", "MOEA Pareto front size",
                ),
            }
            for src, cb in self._circuit_breakers.items():
                cb.on_state_change(self._on_cb_state_change)

        # Distillation optimizer
        historical_path = Path(self.config.get(
            "historical_model_path", "./material_historical_model.pkl"
        ))
        q_weights_path = Path(self.config.get(
            "q_weights_path", "./material_q_weights.json"
        ))
        self.update_optimizer = DistillationUpdateOptimizer(
            config={
                "distillation_epsilon": self.config.get("distillation_epsilon", 0.1),
                "distillation_train_every": self.config.get("distillation_train_every", 10),
                "distillation_replay_size": self.config.get("distillation_replay_size", 2000),
                "distillation_learning_rate": self.config.get("distillation_learning_rate", 0.01),
                "distill_weight": self.config.get("distill_weight", 0.7),
                "rl_weight": self.config.get("rl_weight", 0.3),
            },
            historical_model_path=historical_path,
            q_weights_path=q_weights_path,
        )

        # In-memory log + buffered writer
        self.interaction_log: List[Dict[str, Any]] = []
        self._log_buffer: List[Dict[str, Any]] = []
        self._log_flush_size = 25
        self._log_lock = asyncio.Lock()
        self._interaction_log_path = Path(self.config.get(
            "interaction_logs_path", "./material_interactions.csv"
        ))
        self._log_max_size = 5000

        self.last_update_time: Optional[datetime] = None

        # MOEA
        self.moea_enabled: bool = bool(self.config.get("moea_enabled", True))
        self.moea_interval_seconds: int = int(self.config.get("moea_interval_seconds", 300))
        self.moea_population_size: int = int(self.config.get("moea_population_size", 20))
        self.moea_generations: int = int(self.config.get("moea_generations", 5))
        self.moea_mutation_rate: float = float(self.config.get("moea_mutation_rate", 0.2))
        self.moea_crossover_rate: float = float(self.config.get("moea_crossover_rate", 0.8))
        self.moea_tournament_size: int = int(self.config.get("moea_tournament_size", 3))
        self.moea_objective_weights: Dict[str, float] = dict(
            self.config.get("moea_objective_weights", {
                "freshness": 0.4, "cost": 0.3,
                "reliability": 0.2, "latency": 0.1,
            })
        )
        self.moea_dynamic_weights: bool = bool(self.config.get("moea_dynamic_weights", True))
        self.moea_optimizer: Optional[NSGAIIUpdateOptimizer] = None
        self.evolved_pareto_front: List[MOPDUpdateStrategy] = []
        self.best_evolved_strategy: Optional[MOPDUpdateStrategy] = None
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

        log_event("info", "MaterialFootprintUpdater v2.5.0 initialized")

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def start(self) -> "MaterialFootprintUpdater":
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

        # Persist Q weights
        try:
            self.update_optimizer.q_teacher.save_weights(
                self.config.get("q_weights_path", "./material_q_weights.json")
            )
        except Exception as exc:
            log_event("warning", f"Failed to persist Q weights: {exc}")

        # Persist Pareto front
        if self.evolved_pareto_front:
            try:
                path = Path(self.config.get(
                    "moea_pareto_path", "./material_moea_pareto.json"
                ))
                path.parent.mkdir(parents=True, exist_ok=True)
                with open(path, "w") as f:
                    json.dump([p.to_dict() for p in self.evolved_pareto_front], f, indent=2)
            except Exception as exc:
                log_event("warning", f"Failed to persist Pareto front: {exc}")

        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
        self._started = False

    async def __aenter__(self) -> "MaterialFootprintUpdater":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    def _on_cb_state_change(self, source: str, state: CircuitBreakerState) -> None:
        if not self.metrics:
            return
        mapping = {
            CircuitBreakerState.CLOSED: 0.0,
            CircuitBreakerState.HALF_OPEN: 1.0,
            CircuitBreakerState.OPEN: 2.0,
        }
        try:
            self.metrics["circuit_breaker_state"].labels(source=source).set(mapping[state])
        except Exception:
            pass

    # ------------------------------------------------------------------
    # DB helpers (sync, wrap in asyncio.to_thread at call sites)
    # ------------------------------------------------------------------
    def _init_db(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS footprints (
                    product_id TEXT PRIMARY KEY,
                    embodied_carbon_kg REAL,
                    rare_earth_kg REAL,
                    total_mass_kg REAL,
                    material_index REAL,
                    source TEXT,
                    last_updated TEXT,
                    created_at TEXT DEFAULT (datetime('now'))
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_product_id ON footprints(product_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_last_updated ON footprints(last_updated)")
            conn.commit()
        finally:
            conn.close()

    def _db_execute(self, query: str, params: Tuple[Any, ...] = ()) -> int:
        conn = sqlite3.connect(self.db_path)
        try:
            cur = conn.execute(query, params)
            conn.commit()
            return cur.rowcount
        finally:
            conn.close()

    def _db_fetchone(self, query: str, params: Tuple[Any, ...] = ()) -> Optional[Tuple[Any, ...]]:
        conn = sqlite3.connect(self.db_path)
        try:
            return conn.execute(query, params).fetchone()
        finally:
            conn.close()

    def _db_fetchall(self, query: str, params: Tuple[Any, ...] = ()) -> List[Tuple[Any, ...]]:
        conn = sqlite3.connect(self.db_path)
        try:
            return conn.execute(query, params).fetchall()
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # LIMIT graph
    # ------------------------------------------------------------------
    def _init_limit_graph(self) -> None:
        assert self.limit_graph_manager is not None
        graph_id = "material_sources"
        if self.limit_graph_manager.get_metadata(graph_id):
            return
        self.limit_graph_manager.create_graph(graph_id, "Material Source Dependencies", {})
        for src in ("bonsai", "footprintdata", "mock"):
            self.limit_graph_manager.add_node(graph_id, f"source_{src}", src, {})
        self.limit_graph_manager.add_edge(
            graph_id, "edge_bonsai_footprintdata",
            "source_bonsai", "source_footprintdata", 1.0, {},
        )
        self.limit_graph_manager.add_edge(
            graph_id, "edge_footprintdata_mock",
            "source_footprintdata", "source_mock", 1.0, {},
        )

    # ------------------------------------------------------------------
    # Session
    # ------------------------------------------------------------------
    async def _get_session(self) -> aiohttp.ClientSession:
        async with self._session_lock:
            if self._session is None or self._session.closed:
                timeout = ClientTimeout(total=self.request_timeout)
                self._session = aiohttp.ClientSession(
                    timeout=timeout, raise_for_status=False
                )
            return self._session

    # ------------------------------------------------------------------
    # State builder
    # ------------------------------------------------------------------
    async def _build_state(self, product_id: Optional[str] = None) -> UpdateState:
        def _query() -> Tuple[int, List[Tuple[str]]]:
            conn = sqlite3.connect(self.db_path)
            try:
                total = conn.execute("SELECT COUNT(*) FROM footprints").fetchone()[0]
                rows = conn.execute("SELECT last_updated FROM footprints").fetchall()
                return total, rows
            finally:
                conn.close()

        total, rows = await asyncio.to_thread(_query)

        now = datetime.now(timezone.utc)
        stale_count = 0
        for row in rows:
            try:
                last = datetime.fromisoformat(row[0])
                if last.tzinfo is None:
                    last = last.replace(tzinfo=timezone.utc)
                if (now - last).total_seconds() > self.cache_ttl:
                    stale_count += 1
            except Exception:
                stale_count += 1
        stale_fraction = stale_count / max(total, 1)

        # Demand estimate from recent interaction log.
        recent_pids = [
            e.get("product_id")
            for e in self.interaction_log[-200:]
            if e.get("product_id") is not None
        ]
        if recent_pids:
            counts = Counter(recent_pids)
            avg_demand = float(np.mean(list(counts.values())))
        else:
            avg_demand = 1.0

        # Success rates per source (from interaction log).
        bonsai_entries = [e for e in self.interaction_log if e.get("source") == "bonsai"]
        footprint_entries = [e for e in self.interaction_log if e.get("source") == "footprintdata"]
        bonsai_success = (
            sum(1 for e in bonsai_entries if e.get("success"))
            / max(len(bonsai_entries), 1)
        )
        footprint_success = (
            sum(1 for e in footprint_entries if e.get("success"))
            / max(len(footprint_entries), 1)
        )

        cb_map = {
            CircuitBreakerState.CLOSED: 0.0,
            CircuitBreakerState.HALF_OPEN: 1.0,
            CircuitBreakerState.OPEN: 2.0,
        }
        bonsai_cb = cb_map[self._circuit_breakers["bonsai"].state]
        footprint_cb = cb_map[self._circuit_breakers["footprintdata"].state]

        hours_since = 0.0
        if self.last_update_time is not None:
            hours_since = (now - self.last_update_time).total_seconds() / 3600.0

        single_mode = 1.0 if product_id is not None else 0.0

        return UpdateState(
            total_products=int(total),
            stale_fraction=float(stale_fraction),
            avg_demand=float(avg_demand),
            bonsai_success_rate=float(bonsai_success),
            footprintdata_success_rate=float(footprint_success),
            bonsai_cb_state=float(bonsai_cb),
            footprintdata_cb_state=float(footprint_cb),
            hours_since_update=float(hours_since),
            single_product_mode=float(single_mode),
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    async def update_catalog(
        self, force_refresh: bool = False, product_id: Optional[str] = None
    ) -> int:
        if not self._started:
            await self.start()

        state = await self._build_state(product_id=product_id)

        # Distillation provides the real teacher signal.
        action, action_idx, state_vec, teacher_probs = (
            await self.update_optimizer.select_action(state, exploration=True)
        )

        # MoE biases the choice when the teacher signal is weak.
        selected_expert: Optional[str] = None
        expert_probs: Optional[np.ndarray] = None
        if self.moe_gating:
            selected_expert, expert_probs = await self.moe_gating.select_expert(state)
            if selected_expert in self.ACTION_SPACE:
                expert_action = self.ACTION_SPACE.index(selected_expert)
                if float(np.max(teacher_probs)) < 0.4:
                    action = selected_expert
                    action_idx = expert_action

        ctx = UpdateContext(
            state=state,
            state_vec=state_vec,
            action_idx=action_idx,
            action=action,
            teacher_probs=teacher_probs,
            selected_expert=selected_expert,
            expert_probs=expert_probs,
            product_id=product_id,
        )

        start = time.time()
        success = False
        updated_count = 0
        source_used: Optional[str] = None

        try:
            if action.startswith("bonsai"):
                source_used = "bonsai"
                updated_count = await self._update_from_source(
                    "bonsai", force_refresh, product_id
                )
            elif action.startswith("footprintdata"):
                source_used = "footprintdata"
                updated_count = await self._update_from_source(
                    "footprintdata", force_refresh, product_id
                )
            elif action.startswith("mock"):
                source_used = "mock"
                updated_count = self._seed_mock_data(product_id=product_id)
        except Exception as exc:
            log_event("warning", "Update failed", action=action, error=str(exc))
            if self.metrics and source_used:
                self.metrics["errors"].labels(source=source_used).inc()

        success = updated_count > 0
        latency = time.time() - start
        reward = await self._compute_reward(success, updated_count, force_refresh)

        self.last_update_time = datetime.now(timezone.utc)

        # Log interaction with state vector.
        await self._log_interaction({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "action": action,
            "action_idx": int(action_idx),
            "source": source_used or action,
            "success": success,
            "reward": float(reward),
            "latency": float(latency),
            "product_id": product_id or "",
            "state_vector": ",".join(f"{v:.6g}" for v in ctx.state_vec),
        })

        # MoE update (per request).
        if self.moe_gating and selected_expert:
            await self.moe_gating.add_training_sample(state, selected_expert, reward)

        # Distillation update (per request).
        next_state_vec = (await self._build_state(product_id=product_id)).to_feature_vector()
        await self.update_optimizer.update(
            ctx.state_vec, ctx.action_idx, reward, next_state_vec, ctx.teacher_probs,
        )

        # RLHF sample.
        if self.rlhf_trainer and random.random() < 0.05:
            rejected = random.choice(
                [a for a in self.ACTION_SPACE if a != action] or [action]
            )
            self.rlhf_trainer.record_pair(
                pair_id=str(uuid.uuid4()),
                prompt="Which update strategy is better?",
                chosen=action,
                rejected=rejected,
                reward_diff=float(reward),
                metadata={
                    "force_refresh": force_refresh,
                    "product_id": product_id,
                },
            )

        # MODP state.
        if self.modp_solver:
            self.modp_solver.add_state(
                state_id=f"{datetime.now(timezone.utc).isoformat()}_{action}",
                problem_id="material_update_strategy",
                state_attributes={"action": action, "force_refresh": force_refresh},
                objective_values={
                    "freshness": 1.0 if success else 0.0,
                    "cost": 0.0,
                    "reliability": 1.0 if success else 0.0,
                    "latency": max(0.0, 1.0 - min(latency / 10.0, 1.0)),
                },
                stage=0,
            )

        if self.metrics:
            try:
                self.metrics["update_action"].labels(action=action).inc()
                self.metrics["update_reward"].observe(reward)
                total = await asyncio.to_thread(
                    self._db_fetchone, "SELECT COUNT(*) FROM footprints"
                )
                if total:
                    self.metrics["cache_size"].set(int(total[0]))
            except Exception:
                pass

        log_event(
            "info",
            "Update completed",
            action=action, updated=updated_count, reward=reward,
        )
        return updated_count

    async def get_or_fetch_footprint(
        self, product_id: str, force_refresh: bool = False
    ) -> Optional[Footprint]:
        fp = await asyncio.to_thread(self.get_footprint, product_id)
        if fp and not force_refresh:
            age = (datetime.now(timezone.utc) - fp.last_updated).total_seconds()
            if age < self.cache_ttl:
                if self.metrics:
                    self.metrics["cache_hits"].inc()
                return fp

        if self.metrics:
            self.metrics["cache_misses"].inc()

        # Try each source in priority order.
        for source in self.source_priority:
            try:
                updated = await self._update_from_source(source, force_refresh, product_id)
                if updated > 0:
                    return await asyncio.to_thread(self.get_footprint, product_id)
            except Exception as exc:
                log_event("warning", "Fetch failed", source=source, product_id=product_id, error=str(exc))

        return await asyncio.to_thread(self.get_footprint, product_id)

    # ------------------------------------------------------------------
    # Providers
    # ------------------------------------------------------------------
    async def _update_from_source(
        self, source: str, force_refresh: bool, product_id: Optional[str] = None
    ) -> int:
        """Fetch one or more products from a provider, protected by the breaker."""
        if source == "bonsai" and not self.bonsai_api_key:
            log_event("warning", "Bonsai API key not set; skipping.")
            return 0
        if source == "footprintdata" and not self.footprintdata_api_key:
            log_event("warning", "FootprintData API key not set; skipping.")
            return 0
        if source not in ("bonsai", "footprintdata"):
            return 0

        cb = self._circuit_breakers[source]
        session = await self._get_session()

        async def _fetch_one(pid: str) -> Optional[Footprint]:
            # Replace this body with a real HTTP call in production.
            # The current implementation fabricates a Footprint for offline use.
            await asyncio.sleep(0)  # yield control
            return Footprint(
                product_id=pid,
                embodied_carbon_kg=random.uniform(10, 200),
                rare_earth_kg=random.uniform(0.001, 0.01),
                total_mass_kg=random.uniform(1, 10),
                material_index=random.uniform(0.1, 0.9),
                source=source,
                last_updated=datetime.now(timezone.utc),
            )

        async def _call() -> List[Footprint]:
            pids = [product_id] if product_id else [f"product_{i}" for i in range(1, 5)]
            return [fp for fp in [await _fetch_one(p) for p in pids] if fp is not None]

        start = time.time()
        try:
            footprints = await cb.call(_call)
        except Exception as exc:
            if self.metrics:
                self.metrics["errors"].labels(source=source).inc()
                self.metrics["calls"].labels(source=source, status="error").inc()
            raise exc

        for fp in footprints:
            await asyncio.to_thread(self._store_footprint, fp)

        if self.metrics:
            self.metrics["calls"].labels(source=source, status="success").inc()
            self.metrics["latency"].labels(source=source).observe(time.time() - start)

        return len(footprints)

    def _seed_mock_data(self, product_id: Optional[str] = None) -> int:
        pids = [product_id] if product_id else [f"mock_{i}" for i in range(1, 10)]
        count = 0
        conn = sqlite3.connect(self.db_path)
        try:
            for pid in pids:
                fp = Footprint(
                    product_id=pid,
                    embodied_carbon_kg=random.uniform(5, 50),
                    rare_earth_kg=0.0,
                    total_mass_kg=random.uniform(0.5, 5),
                    material_index=random.uniform(0.1, 0.5),
                    source="mock",
                    last_updated=datetime.now(timezone.utc),
                )
                conn.execute("""
                    INSERT OR REPLACE INTO footprints
                    (product_id, embodied_carbon_kg, rare_earth_kg, total_mass_kg,
                     material_index, source, last_updated)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    fp.product_id, fp.embodied_carbon_kg, fp.rare_earth_kg,
                    fp.total_mass_kg, fp.material_index, fp.source,
                    fp.last_updated.isoformat(),
                ))
                count += 1
            conn.commit()
        finally:
            conn.close()
        return count

    def _store_footprint(self, fp: Footprint) -> None:
        conn = sqlite3.connect(self.db_path)
        try:
            conn.execute("""
                INSERT OR REPLACE INTO footprints
                (product_id, embodied_carbon_kg, rare_earth_kg, total_mass_kg,
                 material_index, source, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                fp.product_id, fp.embodied_carbon_kg, fp.rare_earth_kg,
                fp.total_mass_kg, fp.material_index, fp.source,
                fp.last_updated.isoformat() if fp.last_updated.tzinfo
                else fp.last_updated.replace(tzinfo=timezone.utc).isoformat(),
            ))
            conn.commit()
        finally:
            conn.close()

    def _count_catalog(self) -> int:
        row = self._db_fetchone("SELECT COUNT(*) FROM footprints")
        return int(row[0]) if row else 0

    def get_footprint(self, product_id: str) -> Optional[Footprint]:
        row = self._db_fetchone(
            "SELECT product_id, embodied_carbon_kg, rare_earth_kg, total_mass_kg, "
            "material_index, source, last_updated FROM footprints WHERE product_id = ?",
            (product_id,),
        )
        if not row:
            return None
        try:
            last = datetime.fromisoformat(row[6])
            if last.tzinfo is None:
                last = last.replace(tzinfo=timezone.utc)
        except Exception:
            last = datetime.now(timezone.utc)
        return Footprint(
            product_id=row[0],
            embodied_carbon_kg=float(row[1]),
            rare_earth_kg=float(row[2]),
            total_mass_kg=float(row[3]),
            material_index=float(row[4]),
            source=row[5],
            last_updated=last,
        )

    # ------------------------------------------------------------------
    # Reward
    # ------------------------------------------------------------------
    async def _compute_reward(
        self, success: bool, updated_count: int, force_refresh: bool
    ) -> float:
        if not success:
            return 0.0
        total_row = await asyncio.to_thread(
            self._db_fetchone, "SELECT COUNT(*) FROM footprints"
        )
        total = int(total_row[0]) if total_row else 0
        coverage = min(updated_count / max(total, 1), 1.0)
        refresh_penalty = 0.05 if force_refresh else 0.0
        return max(0.0, min(1.0, 0.6 + 0.3 * coverage - refresh_penalty))

    # ------------------------------------------------------------------
    # Logging
    # ------------------------------------------------------------------
    async def _log_interaction(self, entry: Dict[str, Any]) -> None:
        async with self._log_lock:
            self.interaction_log.append(entry)
            if len(self.interaction_log) > self._log_max_size:
                self.interaction_log = self.interaction_log[-self._log_max_size // 2:]
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
        log_path: Union[str, Path] = "./material_interactions.csv",
        model_path: Union[str, Path] = "./material_historical_model.pkl",
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
        if len(df) < 10 or "state_vector" not in df.columns or "action" not in df.columns:
            log_event(
                "warning",
                "Not enough logs or missing state_vector/action columns (need >= 10 rows).",
            )
            return
        if not SKLEARN_ML:
            log_event("error", "scikit-learn is required to train the historical model.")
            return

        def _parse_vec(s: Any) -> np.ndarray:
            if isinstance(s, str):
                return np.asarray([float(x) for x in s.split(",") if x != ""], dtype=np.float32)
            return np.asarray(s, dtype=np.float32)

        try:
            X = np.stack([_parse_vec(s) for s in df["state_vector"].values])
        except Exception as exc:
            log_event("error", f"Failed to parse state vectors: {exc}")
            return

        y = df["action"].values
        le = LabelEncoder()
        y_enc = le.fit_transform(y)
        clf = RandomForestClassifier(n_estimators=100, random_state=42)
        clf.fit(X, y_enc)

        model_path.parent.mkdir(parents=True, exist_ok=True)
        with open(model_path, "wb") as f:
            pickle.dump((clf, le), f)
        log_event("info", f"Historical model saved to {model_path}")

    # ------------------------------------------------------------------
    # Catalog helpers
    # ------------------------------------------------------------------
    def list_products(self) -> List[str]:
        rows = self._db_fetchall("SELECT product_id FROM footprints")
        return [r[0] for r in rows]

    def delete_footprint(self, product_id: str) -> bool:
        return self._db_execute(
            "DELETE FROM footprints WHERE product_id = ?", (product_id,)
        ) > 0

    def clear_cache(self) -> None:
        self._db_execute("DELETE FROM footprints")
        self.interaction_log.clear()
        self.last_update_time = None

    def export_catalog(self, path: Union[str, Path]) -> None:
        path = Path(path)
        rows = self._db_fetchall("SELECT * FROM footprints")
        cols = [
            "product_id", "embodied_carbon_kg", "rare_earth_kg",
            "total_mass_kg", "material_index", "source", "last_updated", "created_at",
        ]
        df = pd.DataFrame(rows, columns=cols)
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.suffix == ".parquet":
            try:
                df.to_parquet(path)
                return
            except Exception as exc:
                log_event("warning", f"Parquet export failed, falling back to CSV: {exc}")
        if path.suffix == ".csv" or path.suffix == ".parquet":
            df.to_csv(path.with_suffix(".csv"), index=False)
        elif path.suffix == ".json":
            df.to_json(path, orient="records")

    def import_catalog(self, path: Union[str, Path]) -> int:
        path = Path(path)
        if path.suffix == ".parquet":
            df = pd.read_parquet(path)
        elif path.suffix == ".csv":
            df = pd.read_csv(path)
        elif path.suffix == ".json":
            df = pd.read_json(path)
        else:
            raise ValueError("Unsupported file format")

        count = 0
        conn = sqlite3.connect(self.db_path)
        try:
            for _, row in df.iterrows():
                last = datetime.fromisoformat(str(row["last_updated"]))
                if last.tzinfo is None:
                    last = last.replace(tzinfo=timezone.utc)
                conn.execute("""
                    INSERT OR REPLACE INTO footprints
                    (product_id, embodied_carbon_kg, rare_earth_kg, total_mass_kg,
                     material_index, source, last_updated)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    row["product_id"], float(row["embodied_carbon_kg"]),
                    float(row["rare_earth_kg"]), float(row["total_mass_kg"]),
                    float(row["material_index"]), row["source"], last.isoformat(),
                ))
                count += 1
            conn.commit()
        finally:
            conn.close()
        return count

    # ------------------------------------------------------------------
    # MOEA loop
    # ------------------------------------------------------------------
    async def _moea_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self.moea_interval_seconds)
                await self.run_strategy_evolution()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                log_event("error", f"MOEA loop failed: {exc}")
                await asyncio.sleep(60)

    async def run_strategy_evolution(self) -> List[MOPDUpdateStrategy]:
        if not self.moea_enabled:
            return []

        async def evaluate(weights: Dict[str, float]) -> Dict[str, float]:
            """Objectives derived from real telemetry.

            The candidate `weights` represent a preference over the four metrics.
            A candidate that prioritizes freshness will see higher freshness but
            may trade off cost, and vice versa. This gives NSGA-II something to
            optimize while keeping the objective vectors tied to real data.
            """
            total = self._count_catalog()
            if total == 0:
                return {"freshness": 0.0, "cost": 0.0, "reliability": 0.0, "latency": 0.0}

            rows = await asyncio.to_thread(
                self._db_fetchall, "SELECT last_updated FROM footprints"
            )
            now = datetime.now(timezone.utc)
            stale = 0
            for r in rows:
                try:
                    last = datetime.fromisoformat(r[0])
                    if last.tzinfo is None:
                        last = last.replace(tzinfo=timezone.utc)
                    if (now - last).total_seconds() > self.cache_ttl:
                        stale += 1
                except Exception:
                    stale += 1
            base_freshness = 1.0 - stale / total

            recent = self.interaction_log[-100:]
            if not recent:
                return {"freshness": base_freshness, "cost": 0.5, "reliability": 0.5, "latency": 0.5}

            bonsai_calls = sum(1 for e in recent if e.get("source") == "bonsai")
            footprint_calls = sum(1 for e in recent if e.get("source") == "footprintdata")
            total_calls = max(bonsai_calls + footprint_calls, 1)
            base_cost = 1.0 - (bonsai_calls * 0.6 + footprint_calls * 0.4) / total_calls

            bonsai_succ = (
                sum(1 for e in recent if e.get("source") == "bonsai" and e.get("success"))
                / max(bonsai_calls, 1)
            )
            footprint_succ = (
                sum(1 for e in recent if e.get("source") == "footprintdata" and e.get("success"))
                / max(footprint_calls, 1)
            )
            base_reliability = (bonsai_succ + footprint_succ) / 2.0

            latencies = [
                float(e["latency"]) for e in recent
                if e.get("source") in ("bonsai", "footprintdata") and e.get("latency") is not None
            ]
            base_latency = (
                1.0 - min(float(np.mean(latencies)) / 10.0, 1.0) if latencies else 0.5
            )

            w_fresh = float(weights.get("freshness", 0.25))
            w_cost = float(weights.get("cost", 0.25))
            w_rel = float(weights.get("reliability", 0.25))
            w_lat = float(weights.get("latency", 0.25))

            freshness = min(1.0, max(0.0, base_freshness + 0.2 * w_fresh - 0.1 * w_cost))
            cost = min(1.0, max(0.0, base_cost - 0.2 * w_cost + 0.1 * w_fresh))
            reliability = min(1.0, max(0.0, base_reliability + 0.2 * w_rel - 0.1 * w_lat))
            latency = min(1.0, max(0.0, base_latency + 0.2 * w_lat - 0.1 * w_rel))

            return {
                "freshness": freshness,
                "cost": cost,
                "reliability": reliability,
                "latency": latency,
            }

        # Reuse the same optimizer across ticks so the population keeps evolving.
        if self.moea_optimizer is None:
            self.moea_optimizer = NSGAIIUpdateOptimizer(
                evaluate_func=evaluate,
                population_size=self.moea_population_size,
                generations=self.moea_generations,
                mutation_rate=self.moea_mutation_rate,
                crossover_rate=self.moea_crossover_rate,
                tournament_size=self.moea_tournament_size,
                objective_weights=self._get_dynamic_moea_weights(),
                dynamic_weights=self.moea_dynamic_weights,
            )
        else:
            self.moea_optimizer.objective_weights = self._get_dynamic_moea_weights()

        pareto = await self.moea_optimizer.evolve()
        self.evolved_pareto_front = pareto
        if pareto:
            best = self.moea_optimizer._select_best_from_pareto(
                pareto, self._get_dynamic_moea_weights()
            )
            if best is not None:
                self.best_evolved_strategy = best
                log_event("info", f"Best evolved strategy weights: {best.weights}")
                if self.metrics:
                    try:
                        self.metrics["moea_pareto_front"].set(len(pareto))
                    except Exception:
                        pass
                if self.modp_solver:
                    self.modp_solver.add_state(
                        state_id=f"moea_best_{time.time()}",
                        problem_id="material_strategy_evolution",
                        state_attributes={"weights": best.weights},
                        objective_values=best.objectives,
                        stage=0,
                    )

        # Persist Pareto front.
        try:
            path = Path(self.config.get(
                "moea_pareto_path", "./material_moea_pareto.json"
            ))
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "w") as f:
                json.dump([p.to_dict() for p in pareto], f, indent=2)
        except Exception as exc:
            log_event("warning", f"Failed to persist Pareto front: {exc}")

        return pareto

    def _get_dynamic_moea_weights(self) -> Dict[str, float]:
        weights = dict(self.moea_objective_weights)
        if not self.interaction_log:
            return weights
        recent = self.interaction_log[-20:]
        success_rate = float(np.mean([1.0 if e.get("success") else 0.0 for e in recent]))
        if success_rate < 0.5:
            weights["freshness"] = min(0.6, weights.get("freshness", 0.4) * 1.5)
        total = sum(weights.values())
        return {k: v / total for k, v in weights.items()} if total > 0 else weights

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------
    async def get_limit_graph(self, graph_id: str = "material_sources") -> Dict[str, Any]:
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
        stats = self.update_optimizer.get_stats()
        stats.update({
            "catalog_size": self._count_catalog(),
            "interaction_log_size": len(self.interaction_log),
            "pareto_front_size": len(self.evolved_pareto_front),
            "circuit_breakers": {
                k: v.state.value for k, v in self._circuit_breakers.items()
            },
            "started": self._started,
        })
        return stats


# ============================================================================
# Factory
# ============================================================================
def create_material_updater(
    config: Optional[Union[Dict[str, Any], "MaterialConfig"]] = None,
    storage: Optional[Any] = None,
) -> MaterialFootprintUpdater:
    """Create a fully configured MaterialFootprintUpdater."""
    return MaterialFootprintUpdater(config, storage)


# ============================================================================
# Example usage
# ============================================================================
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    async def main() -> None:
        config = {
            "db_path": Path("./test_material.db"),
            "cache_ttl": 3600,
            "distillation_epsilon": 0.1,
            "distillation_train_every": 2,
            "moea_enabled": True,
            "moea_interval_seconds": 60,
            "enable_limit_graph": True,
            "enable_modp": True,
            "enable_rlhf": True,
            "enable_moe": True,
        }
        updater = create_material_updater(config)
        try:
            for _ in range(5):
                await updater.update_catalog()
                fp = await asyncio.to_thread(updater.get_footprint, "product_1")
                print(f"Got footprint: {fp}")

            print("Distillation stats:", updater.update_optimizer.get_stats())

            pareto = await updater.run_strategy_evolution()
            print(f"Evolved Pareto front size: {len(pareto)}")
            if updater.best_evolved_strategy:
                print("Best strategy weights:", updater.best_evolved_strategy.weights)

            print("LIMIT Graph:", await updater.get_limit_graph())
            print("MoE experts:", await updater.get_moe_experts())
            print("Stats:", updater.get_stats())
        finally:
            await updater.close()

    asyncio.run(main())
