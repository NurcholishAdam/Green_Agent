#!/usr/bin/env python3
"""
Enhanced Carbon Intensity Fetcher v2.5.0
========================================
Refactored for correctness, async-safety, and testability.

FIXES OVER v2.4.1:
- Added missing `dataclass` import.
- Proper Pydantic v1/v2 config normalization.
- No `asyncio.create_task` in `__init__`; explicit `async start()`.
- Per-request context instead of shared `self.last_*` fields (race-free).
- Circuit breaker now treats `None` provider responses as failures.
- Structured logging wrapper works with or without structlog.
- Config flags on the constructor are now Optional[bool] and respected.
- MoE gating update uses reward with a running baseline.
- Historical ML teacher accepts `model_path` and `n_actions`.
- CSV interaction log buffered with a thread-safe flush.
- Fallback values cached with a shorter TTL (or not at all).
- `np.fromstring` replaced with safe parsing.
- Public `CircuitBreaker.state` property.
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
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import (
    Any, Awaitable, Callable, Dict, List, Optional, Protocol,
    Tuple, Type, Union,
)

import numpy as np
import pandas as pd
import aiohttp
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
        retry_if_exception_type, before_sleep_log, RetryError,
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
    class CarbonIntensityConfig(BaseModel):
        """Configuration for CarbonIntensityFetcher."""

        providers: List[str] = Field(
            default_factory=lambda: ["climate_trace", "os_climate", "electricity_maps"]
        )
        climate_trace_api_key: Optional[str] = None
        os_climate_api_key: Optional[str] = None
        electricity_maps_api_key: Optional[str] = None
        region_averages: Dict[str, float] = Field(
            default_factory=lambda: {
                "us-east": 0.41, "us-west": 0.34, "eu-west": 0.27,
                "eu-north": 0.21, "asia-east": 0.49, "asia-southeast": 0.47,
                "global": 0.40,
            }
        )
        cache_ttl: int = Field(3600, ge=0)
        fallback_cache_ttl: int = Field(300, ge=0)
        retry_attempts: int = Field(3, ge=0)
        retry_min_wait: float = Field(1.0, gt=0)
        retry_max_wait: float = Field(10.0, gt=0)
        circuit_breaker_threshold: int = Field(5, ge=1)
        circuit_breaker_timeout: float = Field(30.0, ge=1)
        request_timeout: float = Field(10.0, ge=1)
        enable_prometheus: bool = True

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
                "success_rate": 0.4, "latency": 0.3,
                "cache_efficiency": 0.2, "cost": 0.1,
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
        q_weights_path: str = "./carbon_q_weights.json"
        interaction_logs_path: str = "./carbon_interactions.csv"
        historical_model_path: str = "./carbon_historical_model.pkl"
        moea_pareto_path: str = "./carbon_moea_pareto.json"

        @field_validator("providers")
        @classmethod
        def validate_providers(cls, v: List[str]) -> List[str]:
            allowed = {"climate_trace", "os_climate", "electricity_maps"}
            for p in v:
                if p not in allowed:
                    raise ValueError(f"Provider {p} not in allowed list {allowed}")
            return v

        class Config:
            env_prefix = "CARBON_"
else:
    CARBON_CONFIG: Dict[str, Any] = {
        "providers": ["climate_trace", "os_climate", "electricity_maps"],
        "climate_trace_api_key": None,
        "os_climate_api_key": None,
        "electricity_maps_api_key": None,
        "region_averages": {
            "us-east": 0.41, "us-west": 0.34, "eu-west": 0.27,
            "eu-north": 0.21, "asia-east": 0.49, "asia-southeast": 0.47,
            "global": 0.40,
        },
        "cache_ttl": 3600,
        "fallback_cache_ttl": 300,
        "retry_attempts": 3,
        "retry_min_wait": 1.0,
        "retry_max_wait": 10.0,
        "circuit_breaker_threshold": 5,
        "circuit_breaker_timeout": 30.0,
        "request_timeout": 10.0,
        "enable_prometheus": True,
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
            "cache_efficiency": 0.2, "cost": 0.1,
        },
        "moea_dynamic_weights": True,
        "enable_limit_graph": True,
        "enable_modp": True,
        "enable_rlhf": True,
        "enable_moe": True,
        "moe_expert_count": 4,
        "q_weights_path": "./carbon_q_weights.json",
        "interaction_logs_path": "./carbon_interactions.csv",
        "historical_model_path": "./carbon_historical_model.pkl",
        "moea_pareto_path": "./carbon_moea_pareto.json",
    }


def normalize_config(
    config: Optional[Union[Dict[str, Any], "CarbonIntensityConfig"]]
) -> Dict[str, Any]:
    """Normalize any config input to a plain dict (Pydantic v1/v2 aware)."""
    if config is None:
        if PYDANTIC_AVAILABLE:
            model = CarbonIntensityConfig()
            return _pydantic_dump(model)
        return copy.deepcopy(CARBON_CONFIG)

    if isinstance(config, dict):
        if PYDANTIC_AVAILABLE:
            return _pydantic_dump(CarbonIntensityConfig(**config))
        return dict(config)

    if PYDANTIC_AVAILABLE and isinstance(config, CarbonIntensityConfig):
        return _pydantic_dump(config)

    # Fallback: treat as a plain object with attributes.
    return {k: getattr(config, k) for k in dir(config) if not k.startswith("_")}


def _pydantic_dump(model: Any) -> Dict[str, Any]:
    if hasattr(model, "model_dump"):
        return model.model_dump()
    return model.dict()


# ============================================================================
# Circuit Breaker
# ============================================================================
class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
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
# Providers (interfaces preserved; still stubs — replace with real clients)
# ============================================================================
class BaseProvider:
    """Abstract provider interface."""

    name: str = "base"

    async def fetch(
        self,
        session: aiohttp.ClientSession,
        region: str,
        timestamp: datetime,
    ) -> Optional[float]:
        raise NotImplementedError


class ClimateTraceProvider(BaseProvider):
    name = "climate_trace"

    def __init__(self, api_key: Optional[str] = None) -> None:
        self.api_key = api_key or os.environ.get("CLIMATE_TRACE_API_KEY")

    async def fetch(self, session, region, timestamp):
        if not self.api_key:
            return None
        # TODO: replace with real HTTP call; for now return None to be honest.
        return None


class OSClimateProvider(BaseProvider):
    name = "os_climate"

    def __init__(self, api_key: Optional[str] = None) -> None:
        self.api_key = api_key or os.environ.get("OS_CLIMATE_API_KEY")

    async def fetch(self, session, region, timestamp):
        if not self.api_key:
            return None
        return None


class ElectricityMapsProvider(BaseProvider):
    name = "electricity_maps"

    def __init__(self, api_key: Optional[str] = None) -> None:
        self.api_key = api_key or os.environ.get("ELECTRICITY_MAPS_API_KEY")

    async def fetch(self, session, region, timestamp):
        if not self.api_key:
            return None
        return None


# ============================================================================
# LIMIT Graph / MODP / RLHF (interfaces preserved; stubs)
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
                "description": description,
                "configuration": configuration,
                "nodes": {},
                "edges": {},
            }

    def add_node(self, graph_id: str, node_id: str, node_type: str, attributes: Dict[str, Any]) -> None:
        if self.storage and hasattr(self.storage, "save_limit_graph_node"):
            self.storage.save_limit_graph_node(node_id, graph_id, node_type, attributes)
        else:
            self.graphs.setdefault(graph_id, {"nodes": {}, "edges": {}})
            self.graphs[graph_id]["nodes"][node_id] = {
                "node_type": node_type, "attributes": attributes,
            }

    def add_edge(
        self, graph_id: str, edge_id: str, source: str, target: str,
        weight: float, attributes: Dict[str, Any],
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

    def get_metadata(self, graph_id: str) -> Dict[str, Any]:
        if self.storage and hasattr(self.storage, "get_limit_graph_metadata"):
            return self.storage.get_limit_graph_metadata(graph_id)
        return self.graphs.get(graph_id, {})


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
                "success_rate": 0.0, "latency": 0.0,
                "cache_efficiency": 0.0, "cost": 0.0,
            },
            stage=0,
        )
        return {"status": "solved", "pareto_front": []}


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
# MoE Gating
# ============================================================================
class MoEGatingNetwork:
    def __init__(self, storage: Optional[Any] = None, config: Optional[Dict[str, Any]] = None) -> None:
        self.storage = storage
        self.config = config or {}
        self.num_experts = int(self.config.get("moe_expert_count", 4))
        defaults = ["success_focus", "latency_focus", "cache_focus", "cost_focus"]
        self.expert_names = [
            defaults[i] if i < len(defaults) else f"expert_{i}"
            for i in range(self.num_experts)
        ]
        self.gating_weights = np.random.randn(self.num_experts, 18) * 0.01
        self.gating_lr = float(self.config.get("moe_lr", 0.05))
        self._reward_baseline = 0.5

    def _encode_state(self, state: "ProviderSelectionState") -> np.ndarray:
        if isinstance(state, dict):
            features = [
                state.get("region_us_east", 0), state.get("region_us_west", 0),
                state.get("region_eu_west", 0), state.get("region_eu_north", 0),
                state.get("region_asia_east", 0), state.get("region_asia_southeast", 0),
                state.get("region_global", 0),
                state.get("hour_of_day", 0) / 24.0,
                state.get("day_of_week", 0) / 7.0,
                state.get("success_climate_trace", 0.5),
                state.get("success_os_climate", 0.5),
                state.get("success_electricity_maps", 0.5),
                state.get("cb_climate_trace", 0) / 2.0,
                state.get("cb_os_climate", 0) / 2.0,
                state.get("cb_electricity_maps", 0) / 2.0,
                state.get("avail_climate_trace", 1.0),
                state.get("avail_os_climate", 1.0),
                state.get("avail_electricity_maps", 1.0),
            ]
            return np.asarray(features, dtype=np.float32)
        return state.to_feature_vector()

    async def select_expert(self, state: "ProviderSelectionState") -> Tuple[str, np.ndarray]:
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
        self, state: "ProviderSelectionState", selected_expert: str, reward: float
    ) -> None:
        if selected_expert not in self.expert_names:
            return
        x = self._encode_state(state)
        expert_idx = self.expert_names.index(selected_expert)

        logits = self.gating_weights @ x
        probs = np.exp(logits - np.max(logits))
        probs = probs / probs.sum()

        # Running baseline; REINFORCE-style update using advantage.
        self._reward_baseline = 0.95 * self._reward_baseline + 0.05 * reward
        advantage = float(reward) - self._reward_baseline

        target = np.zeros(self.num_experts)
        target[expert_idx] = 1.0
        grad = advantage * (probs - target)[:, None] * x[None, :]
        self.gating_weights -= self.gating_lr * grad


# ============================================================================
# Distillation
# ============================================================================
@dataclass
class ProviderSelectionState:
    region_us_east: float = 0.0
    region_us_west: float = 0.0
    region_eu_west: float = 0.0
    region_eu_north: float = 0.0
    region_asia_east: float = 0.0
    region_asia_southeast: float = 0.0
    region_global: float = 0.0
    hour_of_day: float = 0.0
    day_of_week: float = 0.0
    success_climate_trace: float = 0.5
    success_os_climate: float = 0.5
    success_electricity_maps: float = 0.5
    cb_climate_trace: float = 0.0
    cb_os_climate: float = 0.0
    cb_electricity_maps: float = 0.0
    avail_climate_trace: float = 1.0
    avail_os_climate: float = 1.0
    avail_electricity_maps: float = 1.0

    def to_feature_vector(self) -> np.ndarray:
        return np.asarray([
            self.region_us_east, self.region_us_west, self.region_eu_west,
            self.region_eu_north, self.region_asia_east, self.region_asia_southeast,
            self.region_global, self.hour_of_day / 24.0, self.day_of_week / 7.0,
            self.success_climate_trace, self.success_os_climate, self.success_electricity_maps,
            self.cb_climate_trace / 2.0, self.cb_os_climate / 2.0, self.cb_electricity_maps / 2.0,
            self.avail_climate_trace, self.avail_os_climate, self.avail_electricity_maps,
        ], dtype=np.float32)


class Teacher(ABC):
    @abstractmethod
    def predict(self, state: ProviderSelectionState) -> np.ndarray:
        ...

    @abstractmethod
    def confidence(self, state: ProviderSelectionState) -> float:
        ...


class ProviderRuleBasedTeacher(Teacher):
    def __init__(self, available_providers: List[str]) -> None:
        self.providers = available_providers

    def _avail_succ_cb(self, state: ProviderSelectionState, provider: str) -> Tuple[float, float, float]:
        if provider == "climate_trace":
            return state.avail_climate_trace, state.success_climate_trace, state.cb_climate_trace
        if provider == "os_climate":
            return state.avail_os_climate, state.success_os_climate, state.cb_os_climate
        return state.avail_electricity_maps, state.success_electricity_maps, state.cb_electricity_maps

    def predict(self, state: ProviderSelectionState) -> np.ndarray:
        probs = np.ones(len(self.providers)) * 0.1
        for i, p in enumerate(self.providers):
            avail, succ, cb = self._avail_succ_cb(state, p)
            if avail > 0.5 and succ > 0.6:
                probs[i] += 0.5
            if cb == 0.0:
                probs[i] += 0.2
        total = probs.sum()
        return probs / total if total > 0 else np.ones(len(self.providers)) / len(self.providers)

    def confidence(self, state: ProviderSelectionState) -> float:
        return 0.6 if state.avail_climate_trace > 0.5 else 0.4


class ProviderHistoricalMLTeacher(Teacher):
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

    def predict(self, state: ProviderSelectionState) -> np.ndarray:
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

    def confidence(self, state: ProviderSelectionState) -> float:
        return 0.7 if self.model is not None else 0.0


class ProviderStatefulQTeacher(Teacher):
    def __init__(self, available_providers: List[str], lr: float = 0.1) -> None:
        self.providers = available_providers
        self.lr = lr
        self.weights = np.zeros((18, len(self.providers)))

    def predict(self, state: ProviderSelectionState) -> np.ndarray:
        x = state.to_feature_vector()
        q = x @ self.weights
        exp_q = np.exp(q - np.max(q))
        return exp_q / exp_q.sum()

    def confidence(self, state: ProviderSelectionState) -> float:
        return 0.5

    def update(self, state: ProviderSelectionState, action: int, reward: float) -> None:
        x = state.to_feature_vector()
        q_current = float(np.dot(x, self.weights[:, action]))
        self.weights[:, action] += self.lr * (reward - q_current) * x


class DistillationStudent:
    def __init__(self, feature_dim: int = 18, n_classes: int = 3, lr: float = 0.01) -> None:
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

    def sample(self, batch_size: int = 32) -> Tuple[np.ndarray, List[int], np.ndarray, np.ndarray, np.ndarray]:
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


class DistillationProviderOptimizer:
    def __init__(
        self,
        available_providers: List[str],
        config: Dict[str, Any],
        historical_model_path: Optional[Path] = None,
    ) -> None:
        self.providers = available_providers
        self.n_actions = len(available_providers)
        self.config = config
        self.student = DistillationStudent(
            feature_dim=18,
            n_classes=self.n_actions,
            lr=float(config.get("distillation_learning_rate", 0.01)),
        )
        self.q_teacher = ProviderStatefulQTeacher(available_providers)
        self.teachers: List[Teacher] = [
            ProviderRuleBasedTeacher(available_providers),
            ProviderHistoricalMLTeacher(
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

    def _aggregate_teacher_probs(self, state: ProviderSelectionState) -> np.ndarray:
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

    async def select_provider(
        self, state: ProviderSelectionState, exploration: bool = True
    ) -> Tuple[str, int, np.ndarray, np.ndarray]:
        state_vec = state.to_feature_vector()
        teacher_probs = self._aggregate_teacher_probs(state)
        student_probs = self.student.predict_proba(state_vec)

        if exploration and random.random() < self.epsilon:
            action_idx = random.randint(0, self.n_actions - 1)
        else:
            combined = 0.8 * student_probs + 0.2 * teacher_probs
            action_idx = int(np.argmax(combined))

        return self.providers[action_idx], action_idx, state_vec, teacher_probs

    async def update(
        self, state_vec: np.ndarray, action_idx: int, reward: float,
        next_state_vec: np.ndarray, teacher_probs: np.ndarray,
    ) -> None:
        self.replay_buffer.push(state_vec, action_idx, reward, next_state_vec, teacher_probs)
        self.counter += 1

        # Always update the Q teacher online.
        # Reconstruct a lightweight state for the Q-teacher by treating it as a vector.
        # The Q-teacher's update expects a ProviderSelectionState; we bypass it
        # by updating its weights directly with the feature vector.
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
# MOEA (interface preserved; still a stub)
# ============================================================================
@dataclass
class MOPDProviderStrategy:
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


class NSGAIIProviderOptimizer:
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
        self.best_individual: Optional[MOPDProviderStrategy] = None
        self.best_fitness = -float("inf")
        self.pareto_front: List[MOPDProviderStrategy] = []

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

    async def evolve(self) -> List[MOPDProviderStrategy]:
        population = [self._random_weights() for _ in range(self.population_size)]
        points: List[MOPDProviderStrategy] = []
        for ind in population:
            obj = await self.evaluate_func(ind)
            score = sum(self.objective_weights.get(k, 0.0) * v for k, v in obj.items())
            points.append(MOPDProviderStrategy(
                strategy_id=str(uuid.uuid4()),
                weights=ind,
                objectives=obj,
                scalarised_score=score,
            ))
        self.pareto_front = points
        return points


# ============================================================================
# Per-request context (fixes the shared-state race)
# ============================================================================
@dataclass
class RequestContext:
    region: str
    timestamp: datetime
    state: ProviderSelectionState
    state_vec: np.ndarray
    action_idx: int
    provider: str
    teacher_probs: np.ndarray
    selected_expert: Optional[str] = None
    expert_probs: Optional[np.ndarray] = None


# ============================================================================
# CarbonIntensityFetcher
# ============================================================================
class CarbonIntensityFetcher:
    def __init__(
        self,
        cache: "CacheManager",
        config: Optional[Union[Dict[str, Any], "CarbonIntensityConfig"]] = None,
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

        # Resolve flags: explicit arg > config > default
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

        self.provider_order: List[str] = list(
            self.config.get("providers", ["climate_trace", "os_climate", "electricity_maps"])
        )
        self.region_averages: Dict[str, float] = dict(self.config.get("region_averages", {}))
        self.cache_ttl: int = int(self.config.get("cache_ttl", 3600))
        self.fallback_cache_ttl: int = int(self.config.get("fallback_cache_ttl", 300))
        self.request_timeout: float = float(self.config.get("request_timeout", 10.0))

        self._providers: Dict[str, BaseProvider] = {
            "climate_trace": ClimateTraceProvider(self.config.get("climate_trace_api_key")),
            "os_climate": OSClimateProvider(self.config.get("os_climate_api_key")),
            "electricity_maps": ElectricityMapsProvider(self.config.get("electricity_maps_api_key")),
        }

        self._circuit_breakers: Dict[str, CircuitBreaker] = {
            p: CircuitBreaker(
                name=f"carbon_{p}",
                failure_threshold=int(self.config.get("circuit_breaker_threshold", 5)),
                recovery_timeout=float(self.config.get("circuit_breaker_timeout", 30.0)),
            )
            for p in self.provider_order
        }

        self._session: Optional[aiohttp.ClientSession] = None
        self._session_lock = asyncio.Lock()
        self._log_lock = asyncio.Lock()

        self.metrics: Optional[Dict[str, Any]] = None
        if PROMETHEUS_AVAILABLE and self.config.get("enable_prometheus", True):
            self.metrics = {
                "calls": Counter(
                    "carbon_api_calls_total", "Carbon API calls",
                    ["provider", "status"],
                ),
                "errors": Counter(
                    "carbon_api_errors_total", "Carbon API errors", ["provider"],
                ),
                "latency": Histogram(
                    "carbon_api_latency_seconds", "Carbon API latency", ["provider"],
                ),
                "cache_hits": Counter("carbon_cache_hits_total", "Cache hits"),
                "cache_misses": Counter("carbon_cache_misses_total", "Cache misses"),
                "circuit_breaker_state": Gauge(
                    "carbon_circuit_breaker_state", "Circuit breaker state", ["provider"],
                ),
                "fallback_usage": Counter(
                    "carbon_fallback_usage_total", "Fallback to region average",
                ),
                "moea_pareto_front": Gauge(
                    "carbon_moea_pareto_front", "MOEA Pareto front size",
                ),
            }
            # Wire circuit-breaker state changes to Prometheus.
            for p, cb in self._circuit_breakers.items():
                cb.on_state_change(self._on_cb_state_change)

        # Distillation optimizer (pass historical model path).
        historical_path = Path(self.config.get(
            "historical_model_path", "./carbon_historical_model.pkl"
        ))
        self.provider_optimizer = DistillationProviderOptimizer(
            available_providers=self.provider_order,
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
        # Load persisted Q weights if present.
        try:
            self.provider_optimizer.load_q_weights(
                self.config.get("q_weights_path", "./carbon_q_weights.json")
            )
        except Exception as exc:
            log_event("warning", f"Could not load Q weights: {exc}")

        # Interaction log
        self.interaction_log: List[Dict[str, Any]] = []
        self._log_buffer: List[Dict[str, Any]] = []
        self._log_flush_size = 25
        self._interaction_log_path = Path(
            self.config.get("interaction_logs_path", "./carbon_interactions.csv")
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
                "cache_efficiency": 0.2, "cost": 0.1,
            })
        )
        self.moea_dynamic_weights: bool = bool(self.config.get("moea_dynamic_weights", True))
        self.moea_optimizer: Optional[NSGAIIProviderOptimizer] = None
        self.evolved_pareto_front: List[MOPDProviderStrategy] = []
        self.best_evolved_strategy: Optional[MOPDProviderStrategy] = None
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

        log_event("info", "CarbonIntensityFetcher v2.5.0 initialized")

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def start(self) -> "CarbonIntensityFetcher":
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

        # Flush logs and persist state
        async with self._log_lock:
            await self._flush_log_buffer_locked()
        try:
            self.provider_optimizer.save_q_weights(
                self.config.get("q_weights_path", "./carbon_q_weights.json")
            )
        except Exception as exc:
            log_event("warning", f"Failed to persist Q weights: {exc}")

        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
        self._started = False

    async def __aenter__(self) -> "CarbonIntensityFetcher":
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
            self.metrics["circuit_breaker_state"].labels(provider=provider).set(mapping[state])
        except Exception:
            pass

    # ------------------------------------------------------------------
    # LIMIT graph
    # ------------------------------------------------------------------
    def _init_limit_graph(self) -> None:
        assert self.limit_graph_manager is not None
        graph_id = "carbon_providers"
        if self.limit_graph_manager.get_metadata(graph_id):
            return
        self.limit_graph_manager.create_graph(graph_id, "Carbon Provider Dependencies", {})
        for prov in self.provider_order:
            self.limit_graph_manager.add_node(
                graph_id, f"provider_{prov}", "provider",
                {"api_key_set": bool(self._providers[prov].api_key)},
            )
        for region, avg in self.region_averages.items():
            self.limit_graph_manager.add_node(
                graph_id, f"region_{region}", "region", {"average": avg},
            )
        for prov in self.provider_order:
            for region in self.region_averages:
                self.limit_graph_manager.add_edge(
                    graph_id, f"edge_{prov}_{region}",
                    f"provider_{prov}", f"region_{region}", 1.0, {},
                )

    # ------------------------------------------------------------------
    # Session
    # ------------------------------------------------------------------
    async def _get_session(self) -> aiohttp.ClientSession:
        async with self._session_lock:
            if self._session is None or self._session.closed:
                timeout = ClientTimeout(total=self.request_timeout)
                self._session = aiohttp.ClientSession(timeout=timeout)
            return self._session

    # ------------------------------------------------------------------
    # State building
    # ------------------------------------------------------------------
    def _build_state(self, region: str, timestamp: datetime) -> ProviderSelectionState:
        regions = [
            "us-east", "us-west", "eu-west", "eu-north",
            "asia-east", "asia-southeast", "global",
        ]
        region_onehot = [1.0 if region == r else 0.0 for r in regions]

        success_counts = {p: 0 for p in self.provider_order}
        total_counts = {p: 0 for p in self.provider_order}
        for entry in self.interaction_log[-100:]:
            p = entry.get("provider")
            if p in success_counts:
                total_counts[p] += 1
                if entry.get("success"):
                    success_counts[p] += 1
        success_rates = {
            p: success_counts[p] / max(total_counts[p], 1)
            for p in self.provider_order
        }

        cb_states: Dict[str, float] = {}
        for p in self.provider_order:
            cb = self._circuit_breakers[p]
            cb_states[p] = {
                CircuitBreakerState.CLOSED: 0.0,
                CircuitBreakerState.HALF_OPEN: 1.0,
                CircuitBreakerState.OPEN: 2.0,
            }[cb.state]

        avail = {
            p: 1.0 if self._providers[p].api_key else 0.0
            for p in self.provider_order
        }

        return ProviderSelectionState(
            region_us_east=region_onehot[0],
            region_us_west=region_onehot[1],
            region_eu_west=region_onehot[2],
            region_eu_north=region_onehot[3],
            region_asia_east=region_onehot[4],
            region_asia_southeast=region_onehot[5],
            region_global=region_onehot[6],
            hour_of_day=timestamp.hour,
            day_of_week=timestamp.weekday(),
            success_climate_trace=success_rates.get("climate_trace", 0.5),
            success_os_climate=success_rates.get("os_climate", 0.5),
            success_electricity_maps=success_rates.get("electricity_maps", 0.5),
            cb_climate_trace=cb_states.get("climate_trace", 0.0),
            cb_os_climate=cb_states.get("os_climate", 0.0),
            cb_electricity_maps=cb_states.get("electricity_maps", 0.0),
            avail_climate_trace=avail.get("climate_trace", 1.0),
            avail_os_climate=avail.get("os_climate", 1.0),
            avail_electricity_maps=avail.get("electricity_maps", 1.0),
        )

    # ------------------------------------------------------------------
    # Fetching
    # ------------------------------------------------------------------
    async def _fetch_from_provider(
        self,
        provider: str,
        region: str,
        timestamp: datetime,
    ) -> float:
        """Fetch from a single provider. Raises on failure (incl. None)."""
        cb = self._circuit_breakers[provider]
        provider_obj = self._providers[provider]
        session = await self._get_session()
        attempts = int(self.config.get("retry_attempts", 3))
        min_wait = float(self.config.get("retry_min_wait", 1.0))
        max_wait = float(self.config.get("retry_max_wait", 10.0))

        async def _attempt() -> float:
            last_exc: Optional[Exception] = None
            for attempt in range(max(1, attempts)):
                try:
                    value = await provider_obj.fetch(session, region, timestamp)
                    if value is None:
                        raise ValueError(
                            f"Provider {provider} returned no data for {region}"
                        )
                    return float(value)
                except Exception as exc:
                    last_exc = exc
                    if attempt < attempts - 1:
                        wait = min(min_wait * (2 ** attempt), max_wait)
                        await asyncio.sleep(wait)
            assert last_exc is not None
            raise last_exc

        return await cb.call(_attempt)

    async def get_intensity(
        self,
        region: str,
        timestamp: Optional[datetime] = None,
        force_refresh: bool = False,
    ) -> float:
        if not self._started:
            await self.start()

        if timestamp is None:
            timestamp = datetime.now(timezone.utc)
        cache_hour = timestamp.replace(minute=0, second=0, microsecond=0)
        cache_key = f"carbon:{region}:{cache_hour.isoformat()}"

        if not force_refresh:
            cached = await self.cache.get(cache_key)
            if cached is not None:
                if self.metrics:
                    self.metrics["cache_hits"].inc()
                return float(cached)

        if self.metrics:
            self.metrics["cache_misses"].inc()

        state = self._build_state(region, timestamp)

        # MoE expert selection (per-request)
        selected_expert: Optional[str] = None
        expert_probs: Optional[np.ndarray] = None
        if self.moe_gating:
            selected_expert, expert_probs = await self.moe_gating.select_expert(state)

        # Distillation-based provider selection
        provider, action_idx, state_vec, teacher_probs = (
            await self.provider_optimizer.select_provider(state, exploration=True)
        )

        ctx = RequestContext(
            region=region,
            timestamp=timestamp,
            state=state,
            state_vec=state_vec,
            action_idx=action_idx,
            provider=provider,
            teacher_probs=teacher_probs,
            selected_expert=selected_expert,
            expert_probs=expert_probs,
        )

        start = time.time()
        intensity: Optional[float] = None
        success = False
        is_fallback = False

        try:
            intensity = await self._fetch_from_provider(provider, region, timestamp)
            success = True
            if self.metrics:
                self.metrics["calls"].labels(provider=provider, status="success").inc()
                self.metrics["latency"].labels(provider=provider).observe(
                    time.time() - start
                )
        except Exception as exc:
            if self.metrics:
                self.metrics["errors"].labels(provider=provider).inc()
                self.metrics["calls"].labels(provider=provider, status="error").inc()
            log_event(
                "warning", "Provider failed",
                provider=provider, region=region, error=str(exc),
            )

        if intensity is None:
            intensity = self._get_region_average(region)
            is_fallback = True
            if self.metrics:
                self.metrics["fallback_usage"].inc()

        reward = 0.0 if is_fallback else 1.0

        # Train MoE gating with reward.
        if self.moe_gating and selected_expert:
            await self.moe_gating.add_training_sample(state, selected_expert, reward)

        # Distillation update using per-request context.
        next_state_vec = self._build_state(region, timestamp).to_feature_vector()
        await self.provider_optimizer.update(
            ctx.state_vec, ctx.action_idx, reward, next_state_vec, ctx.teacher_probs,
        )

        # Log interaction (async, buffered).
        await self._log_interaction({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "region": region,
            "provider": provider,
            "success": success,
            "fallback": is_fallback,
            "reward": reward,
            "state_vec": ",".join(f"{v:.6g}" for v in ctx.state_vec),
        })

        # Optional RLHF preference sample.
        if self.rlhf_trainer and random.random() < 0.05:
            rejected = random.choice(
                [p for p in self.provider_order if p != provider] or [provider]
            )
            self.rlhf_trainer.record_pair(
                pair_id=str(uuid.uuid4()),
                prompt=f"Which provider should we use for {region}?",
                chosen=provider,
                rejected=rejected,
                reward_diff=reward,
                metadata={"region": region, "timestamp": timestamp.isoformat()},
            )

        # MODP state recording.
        if self.modp_solver:
            self.modp_solver.add_state(
                state_id=f"{region}_{timestamp.isoformat()}_{provider}",
                problem_id="carbon_provider_selection",
                state_attributes={"region": region, "provider": provider},
                objective_values={
                    "success_rate": float(success),
                    "latency": time.time() - start,
                    "cache_efficiency": 0.0,
                    "cost": 0.0,
                },
                stage=0,
            )

        # Cache with fallback-aware TTL.
        ttl = self.fallback_cache_ttl if is_fallback else self.cache_ttl
        if ttl > 0:
            await self.cache.set(cache_key, str(intensity), ttl=ttl)

        return float(intensity)

    # ------------------------------------------------------------------
    # Logging
    # ------------------------------------------------------------------
    async def _log_interaction(self, entry: Dict[str, Any]) -> None:
        async with self._log_lock:
            self.interaction_log.append(entry)
            if len(self.interaction_log) > 10_000:
                # Bound memory; keep the most recent half.
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
    # Historical model training
    # ------------------------------------------------------------------
    @classmethod
    def train_historical_model(
        cls,
        log_path: Union[str, Path] = "./carbon_interactions.csv",
        model_path: Union[str, Path] = "./carbon_historical_model.pkl",
    ) -> None:
        log_path = Path(log_path)
        model_path = Path(model_path)
        if not log_path.exists():
            log_event("warning", "No logs found")
            return
        try:
            df = pd.read_csv(log_path)
        except Exception as exc:
            log_event("error", f"Failed to read logs: {exc}")
            return

        if len(df) < 10 or "state_vec" not in df.columns or "provider" not in df.columns:
            log_event("warning", "Insufficient logs or missing columns")
            return

        if not SKLEARN_ML:
            log_event("error", "scikit-learn is required to train the historical model")
            return

        def _parse_vec(s: Any) -> np.ndarray:
            if isinstance(s, str):
                return np.asarray([float(x) for x in s.split(",") if x != ""], dtype=np.float32)
            return np.asarray(s, dtype=np.float32)

        X = np.stack([_parse_vec(s) for s in df["state_vec"].values])
        y = df["provider"].values

        le = LabelEncoder()
        y_enc = le.fit_transform(y)
        clf = RandomForestClassifier(n_estimators=100, random_state=42)
        clf.fit(X, y_enc)

        model_path.parent.mkdir(parents=True, exist_ok=True)
        with open(model_path, "wb") as f:
            pickle.dump((clf, le), f)
        log_event("info", f"Historical model saved to {model_path}")

    # ------------------------------------------------------------------
    # Region fallback
    # ------------------------------------------------------------------
    def _get_region_average(self, region: str) -> float:
        return float(
            self.region_averages.get(
                region, self.region_averages.get("global", 0.40)
            )
        )

    # ------------------------------------------------------------------
    # Batch helpers
    # ------------------------------------------------------------------
    async def get_intensity_batch(
        self,
        regions: List[str],
        timestamp: Optional[datetime] = None,
        max_concurrency: int = 10,
    ) -> Dict[str, float]:
        sem = asyncio.Semaphore(max(1, max_concurrency))

        async def _one(r: str) -> Tuple[str, float]:
            async with sem:
                try:
                    return r, await self.get_intensity(r, timestamp)
                except Exception:
                    return r, self._get_region_average(r)

        results = await asyncio.gather(*(_one(r) for r in regions))
        return dict(results)

    async def get_historical_intensity(
        self,
        region: str,
        start: datetime,
        end: datetime,
        step_hours: int = 1,
        max_concurrency: int = 10,
    ) -> Dict[datetime, float]:
        sem = asyncio.Semaphore(max(1, max_concurrency))
        current = start.replace(minute=0, second=0, microsecond=0)
        timestamps: List[datetime] = []
        while current <= end:
            timestamps.append(current)
            current += timedelta(hours=step_hours)

        async def _one(ts: datetime) -> Tuple[datetime, float]:
            async with sem:
                try:
                    return ts, await self.get_intensity(region, ts)
                except Exception:
                    return ts, self._get_region_average(region)

        results = await asyncio.gather(*(_one(ts) for ts in timestamps))
        return dict(results)

    # ------------------------------------------------------------------
    # MOEA loop
    # ------------------------------------------------------------------
    async def _moea_loop(self) -> None:
        while True:
            try:
                await asyncio.sleep(self.moea_interval_seconds)
                await self.run_provider_evolution()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                log_event("error", f"MOEA loop failed: {exc}")
                await asyncio.sleep(60)

    async def run_provider_evolution(self) -> List[MOPDProviderStrategy]:
        if not self.moea_enabled:
            return []

        async def evaluate(weights: Dict[str, float]) -> Dict[str, float]:
            # TODO: replace with real metrics derived from interaction_log
            # and per-provider statistics. The current form is a placeholder.
            return {
                "success_rate": random.uniform(0.5, 1.0),
                "latency": random.uniform(0.2, 0.8),
                "cache_efficiency": random.uniform(0.5, 1.0),
                "cost": random.uniform(0.1, 0.9),
            }

        self.moea_optimizer = NSGAIIProviderOptimizer(
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
            self.best_evolved_strategy = max(
                pareto, key=lambda s: s.scalarised_score
            )
            if self.metrics:
                try:
                    self.metrics["moea_pareto_front"].set(len(pareto))
                except Exception:
                    pass

        # Persist Pareto front.
        try:
            path = Path(self.config.get("moea_pareto_path", "./carbon_moea_pareto.json"))
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "w") as f:
                json.dump([s.to_dict() for s in pareto], f, indent=2)
        except Exception as exc:
            log_event("warning", f"Failed to persist Pareto front: {exc}")

        return pareto

    def _get_dynamic_moea_weights(self) -> Dict[str, float]:
        weights = dict(self.moea_objective_weights)
        if self.interaction_log:
            recent = self.interaction_log[-20:]
            success_rate = float(np.mean([1.0 if e.get("success") else 0.0 for e in recent]))
            if success_rate < 0.5:
                weights["success_rate"] = min(0.6, weights.get("success_rate", 0.4) * 1.5)
            total = sum(weights.values())
            if total > 0:
                weights = {k: v / total for k, v in weights.items()}
        return weights

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------
    def get_stats(self) -> Dict[str, Any]:
        stats = self.provider_optimizer.get_stats()
        stats.update({
            "interaction_log_size": len(self.interaction_log),
            "pareto_front_size": len(self.evolved_pareto_front),
            "started": self._started,
        })
        return stats


# ============================================================================
# Factory
# ============================================================================
def create_carbon_fetcher(
    cache: "CacheManager",
    config: Optional[Union[Dict[str, Any], "CarbonIntensityConfig"]] = None,
    storage: Optional["Storage"] = None,
) -> CarbonIntensityFetcher:
    return CarbonIntensityFetcher(cache, config, storage)
