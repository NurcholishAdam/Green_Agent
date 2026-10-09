#!/usr/bin/env python3
"""
Enhanced Helium Synthetic Generator v2.5.0
===========================================
Generates synthetic Helium Proof-of-Coverage (PoC) traces with adaptive parameter
selection via multi-teacher distillation, MoE gating, MOEA/NSGA-II, and optional
LIMIT graph, MODP, and RLHF components.

FIXES OVER v2.4.1:
- Added missing `dataclass` import.
- Proper Pydantic v1/v2 config normalization.
- No `asyncio.create_task` in `__init__`; explicit `async start()`.
- `generate_trace()` refuses to run inside a live event loop (no silent breakage).
- Per-request `GenerationContext` replaces shared `self.last_*` fields.
- Per-instance RNG (no global `random.seed` / `np.random.seed`).
- Diurnal factor is applied to per-hour event counts.
- Hotspots are clustered around drawn cluster centers.
- Edge cases are marked as anomalies; `target_anomaly_rate` drives anomaly probability.
- Validation compares against fixed reference distributions, not fitted sample stats.
- Reward is a smooth, monotone function of validation metrics.
- `state_vector` is logged as CSV string; `train_historical_model` parses it.
- Historical teacher reorders probabilities to fixed action order.
- MoE biases source selection instead of replacing the distillation signal.
- MoE gating uses reward with a running baseline.
- CSV logging is buffered and flushed under an `asyncio.Lock` via `asyncio.to_thread`.
- Real (if simple) NSGA-II: non-dominated sorting, crowding distance, tournament.
- `load_config_from_json` fully rebuilds internal state.
- Q weights persisted on `close()`.
"""

from __future__ import annotations

import asyncio
import copy
import csv
import hashlib
import json
import logging
import math
import os
import pickle
import random
import time
import uuid
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass, field, fields as dataclass_fields
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import (
    Any, Awaitable, Callable, Dict, List, Optional, Tuple, Union,
)

import numpy as np
import pandas as pd

# ---------- Pydantic ----------
try:
    from pydantic import BaseModel, Field, field_validator, ValidationError
    PYDANTIC_AVAILABLE = True
except ImportError:  # pragma: no cover
    PYDANTIC_AVAILABLE = False
    BaseModel = object  # type: ignore

# ---------- SciPy ----------
try:
    from scipy import stats
    SCIPY_AVAILABLE = True
except ImportError:  # pragma: no cover
    SCIPY_AVAILABLE = False

# ---------- scikit-learn ----------
try:
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.preprocessing import LabelEncoder
    SKLEARN_ML = True
except ImportError:  # pragma: no cover
    SKLEARN_ML = False

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
if PYDANTIC_AVAILABLE:
    class HeliumSyntheticConfig(BaseModel):
        """Configuration for synthetic trace generation."""

        version: str = "2.5.0"
        seed: int = Field(42)
        num_hotspots: int = Field(100, ge=1)
        num_gateways: int = Field(5, ge=1)
        duration_hours: float = Field(24.0, ge=1)
        base_events_per_hour: float = Field(10.0, gt=0)
        rssi_mean_urban: float = Field(-70.0)
        rssi_std_urban: float = Field(10.0)
        rssi_mean_rural: float = Field(-80.0)
        rssi_std_rural: float = Field(15.0)
        snr_mean: float = Field(12.0)
        snr_std: float = Field(3.0)
        num_clusters: int = Field(3, ge=1)
        cluster_spread: float = Field(0.2)
        path_loss_exponent: float = Field(2.0, ge=1.0)
        reference_distance_km: float = Field(1.0, gt=0)
        shadowing_std: float = Field(3.0, ge=0)
        diurnal_amplitude: float = Field(0.3, ge=0, le=1)
        diurnal_peak_hour: int = Field(14, ge=0, le=23)
        burst_probability: float = Field(0.1, ge=0, le=1)
        burst_multiplier: float = Field(5.0, ge=1)
        edge_case_rate: float = Field(0.0, ge=0, le=1)
        export_format: str = Field("parquet")
        validation_alpha: float = Field(0.05, ge=0, le=1)

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
                "quality": 0.4, "diversity": 0.3,
                "edge_coverage": 0.2, "time_efficiency": 0.1,
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
        q_weights_path: str = "./synth_q_weights.json"
        generation_logs_path: str = "./synth_generation_logs.csv"
        historical_model_path: str = "./synth_historical_model.pkl"
        moea_pareto_path: str = "./synth_moea_pareto.json"

        @field_validator("export_format")
        @classmethod
        def validate_export_format(cls, v: str) -> str:
            if v not in ("parquet", "csv", "json"):
                raise ValueError("export_format must be 'parquet', 'csv', or 'json'")
            return v

        class Config:
            env_prefix = "HELIUM_SYNTH_"
else:
    HELIUM_SYNTH_CONFIG: Dict[str, Any] = {
        "version": "2.5.0",
        "seed": 42,
        "num_hotspots": 100,
        "num_gateways": 5,
        "duration_hours": 24.0,
        "base_events_per_hour": 10.0,
        "rssi_mean_urban": -70.0,
        "rssi_std_urban": 10.0,
        "rssi_mean_rural": -80.0,
        "rssi_std_rural": 15.0,
        "snr_mean": 12.0,
        "snr_std": 3.0,
        "num_clusters": 3,
        "cluster_spread": 0.2,
        "path_loss_exponent": 2.0,
        "reference_distance_km": 1.0,
        "shadowing_std": 3.0,
        "diurnal_amplitude": 0.3,
        "diurnal_peak_hour": 14,
        "burst_probability": 0.1,
        "burst_multiplier": 5.0,
        "edge_case_rate": 0.0,
        "export_format": "parquet",
        "validation_alpha": 0.05,
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
            "quality": 0.4, "diversity": 0.3,
            "edge_coverage": 0.2, "time_efficiency": 0.1,
        },
        "moea_dynamic_weights": True,
        "enable_limit_graph": True,
        "enable_modp": True,
        "enable_rlhf": True,
        "enable_moe": True,
        "moe_expert_count": 4,
        "q_weights_path": "./synth_q_weights.json",
        "generation_logs_path": "./synth_generation_logs.csv",
        "historical_model_path": "./synth_historical_model.pkl",
        "moea_pareto_path": "./synth_moea_pareto.json",
    }


def _pydantic_dump(model: Any) -> Dict[str, Any]:
    if hasattr(model, "model_dump"):
        return model.model_dump()
    return model.dict()


def normalize_config(
    config: Optional[Union[Dict[str, Any], "HeliumSyntheticConfig"]]
) -> Dict[str, Any]:
    """Normalize any config input into a plain dict (Pydantic v1/v2 aware)."""
    if config is None:
        if PYDANTIC_AVAILABLE:
            return _pydantic_dump(HeliumSyntheticConfig())
        return copy.deepcopy(HELIUM_SYNTH_CONFIG)

    if isinstance(config, dict):
        if PYDANTIC_AVAILABLE:
            return _pydantic_dump(HeliumSyntheticConfig(**config))
        return dict(config)

    if PYDANTIC_AVAILABLE and isinstance(config, HeliumSyntheticConfig):
        return _pydantic_dump(config)

    return {k: getattr(config, k) for k in dir(config) if not k.startswith("_")}


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
                "quality": 0.0, "diversity": 0.0,
                "edge_coverage": 0.0, "time_efficiency": 0.0,
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
    """Mixture-of-Experts gating over generation strategies."""

    def __init__(self, storage: Optional[Any] = None, config: Optional[Dict[str, Any]] = None) -> None:
        self.storage = storage
        self.config = config or {}
        self.num_experts = int(self.config.get("moe_expert_count", 4))
        base = ["realistic", "diverse", "edge_case_heavy", "balanced"]
        self.expert_names = [
            base[i] if i < len(base) else f"expert_{i}"
            for i in range(self.num_experts)
        ]
        # Feature dimension: 10 (matches GenerationState).
        self.gating_weights = np.random.randn(self.num_experts, 10) * 0.01
        self.gating_lr = float(self.config.get("moe_lr", 0.05))
        self._reward_baseline = 0.5

    def _encode_state(self, state: Union["GenerationState", Dict[str, Any]]) -> np.ndarray:
        if isinstance(state, dict):
            features = [
                state.get("target_ks_stat", 0),
                state.get("target_anomaly_rate", 0),
                state.get("target_diversity", 0),
                state.get("last_rssi_ks_p", 0.5),
                state.get("last_snr_ks_p", 0.5),
                state.get("last_uplink_chisq_p", 0.5),
                state.get("last_diurnal_p", 0.5),
                state.get("avg_quality_score", 0.5),
                min(state.get("num_traces_generated", 0) / 100.0, 1.0),
                min(state.get("hours_since_last", 0) / 24.0, 1.0),
            ]
        else:
            features = state.to_feature_vector()
        return np.asarray(features, dtype=np.float32)

    async def select_expert(
        self, state: Union["GenerationState", Dict[str, Any]]
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
        state: Union["GenerationState", Dict[str, Any]],
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

        # REINFORCE-style update using a running baseline.
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
class GenerationState:
    target_ks_stat: float = 0.0
    target_anomaly_rate: float = 0.02
    target_diversity: float = 0.8
    last_rssi_ks_p: float = 0.5
    last_snr_ks_p: float = 0.5
    last_uplink_chisq_p: float = 0.5
    last_diurnal_p: float = 0.5
    avg_quality_score: float = 0.5
    num_traces_generated: int = 0
    hours_since_last: float = 0.0

    def to_feature_vector(self) -> np.ndarray:
        return np.asarray([
            self.target_ks_stat,
            self.target_anomaly_rate,
            self.target_diversity,
            self.last_rssi_ks_p,
            self.last_snr_ks_p,
            self.last_uplink_chisq_p,
            self.last_diurnal_p,
            self.avg_quality_score,
            min(self.num_traces_generated / 100.0, 1.0),
            min(self.hours_since_last / 24.0, 1.0),
        ], dtype=np.float32)


class Teacher(ABC):
    @abstractmethod
    def predict(self, state: GenerationState) -> np.ndarray:
        ...

    @abstractmethod
    def confidence(self, state: GenerationState) -> float:
        ...


class StrategyRuleBasedTeacher(Teacher):
    STRATEGIES = ["realistic", "diverse", "edge_case_heavy", "balanced", "custom"]

    def predict(self, state: GenerationState) -> np.ndarray:
        probs = np.ones(5) * 0.1
        if state.last_rssi_ks_p < 0.05 or state.last_snr_ks_p < 0.05:
            probs[0] = 0.8
        elif state.last_diurnal_p < 0.05:
            probs[1] = 0.7
        elif state.target_anomaly_rate > 0.2 and state.last_uplink_chisq_p < 0.05:
            probs[2] = 0.7
        else:
            probs[3] = 0.6
        total = probs.sum()
        return probs / total if total > 0 else np.ones(5) / 5

    def confidence(self, state: GenerationState) -> float:
        return 0.6 if state.last_rssi_ks_p < 0.05 else 0.4


class StrategyHistoricalMLTeacher(Teacher):
    def __init__(self, model_path: Optional[Path] = None, n_actions: int = 5) -> None:
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

    def predict(self, state: GenerationState) -> np.ndarray:
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

    def confidence(self, state: GenerationState) -> float:
        return 0.7 if self.model is not None else 0.0


class StrategyStatefulQTeacher(Teacher):
    def __init__(self, lr: float = 0.1, weights_path: Optional[Union[str, Path]] = None) -> None:
        self.lr = lr
        self.weights = np.zeros((10, 5))
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

    def predict(self, state: GenerationState) -> np.ndarray:
        x = state.to_feature_vector()
        q = x @ self.weights
        exp_q = np.exp(q - np.max(q))
        return exp_q / exp_q.sum()

    def confidence(self, state: GenerationState) -> float:
        return 0.5

    def update(self, state: GenerationState, action: int, reward: float) -> None:
        x = state.to_feature_vector()
        q_current = float(np.dot(x, self.weights[:, action]))
        self.weights[:, action] += self.lr * (reward - q_current) * x


class DistillationStudent:
    def __init__(self, feature_dim: int = 10, n_classes: int = 5, lr: float = 0.01) -> None:
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
        self, state_vec: np.ndarray, action: int, reward: float,
        next_state_vec: np.ndarray, teacher_probs: np.ndarray,
    ) -> None:
        self.buffer.append((state_vec, action, reward, next_state_vec, teacher_probs))

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


class DistillationGeneratorOptimizer:
    STRATEGIES = ["realistic", "diverse", "edge_case_heavy", "balanced", "custom"]

    def __init__(
        self,
        config: Dict[str, Any],
        historical_model_path: Optional[Path] = None,
        q_weights_path: Optional[Path] = None,
    ) -> None:
        self.config = config
        self.n_actions = len(self.STRATEGIES)
        self.student = DistillationStudent(
            feature_dim=10,
            n_classes=self.n_actions,
            lr=float(config.get("distillation_learning_rate", 0.01)),
        )
        self.q_teacher = StrategyStatefulQTeacher(weights_path=q_weights_path)
        self.teachers: List[Teacher] = [
            StrategyRuleBasedTeacher(),
            StrategyHistoricalMLTeacher(
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

    def _aggregate_teacher_probs(self, state: GenerationState) -> np.ndarray:
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

    async def select_strategy(
        self, state: GenerationState, exploration: bool = True
    ) -> Tuple[str, int, np.ndarray, np.ndarray]:
        state_vec = state.to_feature_vector()
        teacher_probs = self._aggregate_teacher_probs(state)
        student_probs = self.student.predict_proba(state_vec)

        if exploration and random.random() < self.epsilon:
            action_idx = random.randint(0, self.n_actions - 1)
        else:
            combined = 0.8 * student_probs + 0.2 * teacher_probs
            action_idx = int(np.argmax(combined))

        return self.STRATEGIES[action_idx], action_idx, state_vec, teacher_probs

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
# NSGA-II for strategy evolution
# ============================================================================
@dataclass
class MOPDGenerationStrategy:
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
    def from_dict(cls, data: Dict[str, Any]) -> "MOPDGenerationStrategy":
        fields = {f.name for f in dataclass_fields(cls)}
        return cls(**{k: v for k, v in data.items() if k in fields})


class NSGAIIGeneratorOptimizer:
    """Simple but correct NSGA-II.

    All objectives are maximized. Use an explicit `-1` weight in
    `objective_weights` for objectives you want to minimize (or invert the
    objective before returning it from `evaluate_func`)."""

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
    ) -> None:
        self.evaluate_func = evaluate_func
        self.population_size = max(4, population_size)
        self.generations = max(1, generations)
        self.mutation_rate = mutation_rate
        self.crossover_rate = crossover_rate
        self.tournament_size = max(2, tournament_size)
        self.objective_weights = objective_weights or {
            "quality": 0.4, "diversity": 0.3,
            "edge_coverage": 0.2, "time_efficiency": 0.1,
        }
        self.dynamic_weights = dynamic_weights
        self.best_individual: Optional[MOPDGenerationStrategy] = None
        self.best_fitness = -float("inf")
        self.pareto_front: List[MOPDGenerationStrategy] = []
        self._eval_cache: Dict[Tuple[Tuple[str, float], ...], Dict[str, float]] = {}

    # ------- population helpers -------
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

    # ------- NSGA-II core -------
    @staticmethod
    def _dominates(a: MOPDGenerationStrategy, b: MOPDGenerationStrategy, keys: List[str]) -> bool:
        at_least = all(a.objectives.get(k, 0.0) >= b.objectives.get(k, 0.0) for k in keys)
        strictly = any(a.objectives.get(k, 0.0) > b.objectives.get(k, 0.0) for k in keys)
        return at_least and strictly

    def _fast_non_dominated_sort(
        self, points: List[MOPDGenerationStrategy]
    ) -> List[List[MOPDGenerationStrategy]]:
        if not points:
            return []
        keys = list(self.objective_weights.keys())
        fronts: List[List[MOPDGenerationStrategy]] = [[]]
        dom_count: Dict[int, int] = {id(p): 0 for p in points}
        dominated_by: Dict[int, List[MOPDGenerationStrategy]] = {id(p): [] for p in points}

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
        while fronts[i]:
            nxt: List[MOPDGenerationStrategy] = []
            for p in fronts[i]:
                for q in dominated_by[id(p)]:
                    dom_count[id(q)] -= 1
                    if dom_count[id(q)] == 0:
                        nxt.append(q)
            i += 1
            fronts.append(nxt)

        return [f for f in fronts if f]

    def _crowding_distance(self, front: List[MOPDGenerationStrategy]) -> Dict[int, float]:
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
        population: List[MOPDGenerationStrategy],
        rank: Dict[int, int],
        crowding: Dict[int, float],
    ) -> MOPDGenerationStrategy:
        candidates = random.sample(population, min(self.tournament_size, len(population)))
        return min(candidates, key=lambda p: (rank[id(p)], -crowding.get(id(p), 0.0)))

    def _select_best_from_pareto(
        self, pareto: List[MOPDGenerationStrategy], weights: Dict[str, float]
    ) -> Optional[MOPDGenerationStrategy]:
        if not pareto:
            return None
        return max(pareto, key=lambda p: p.scalarised_score)

    def _scalarise(self, objectives: Dict[str, float]) -> float:
        score = 0.0
        for k, w in self.objective_weights.items():
            score += w * float(objectives.get(k, 0.0))
        return score

    async def _evaluate_weights(self, weights: Dict[str, float]) -> Dict[str, float]:
        key = tuple(sorted(weights.items()))
        if key in self._eval_cache:
            return self._eval_cache[key]
        obj = await self.evaluate_func(weights)
        self._eval_cache[key] = obj
        return obj

    async def evolve(self) -> List[MOPDGenerationStrategy]:
        # Initial population
        population: List[MOPDGenerationStrategy] = []
        for _ in range(self.population_size):
            w = self._random_individual()
            obj = await self._evaluate_weights(w)
            population.append(MOPDGenerationStrategy(
                strategy_id=str(uuid.uuid4()),
                weights=w,
                objectives=obj,
                scalarised_score=self._scalarise(obj),
            ))

        for _ in range(self.generations):
            # Rank
            fronts = self._fast_non_dominated_sort(population)
            rank: Dict[int, int] = {}
            crowding: Dict[int, float] = {}
            for i, front in enumerate(fronts):
                cd = self._crowding_distance(front)
                for p in front:
                    rank[id(p)] = i
                    crowding[id(p)] = cd.get(id(p), 0.0)

            # Offspring
            offspring: List[MOPDGenerationStrategy] = []
            while len(offspring) < self.population_size:
                p1 = self._tournament_selection(population, rank, crowding)
                p2 = self._tournament_selection(population, rank, crowding)
                if random.random() < self.crossover_rate:
                    child_w = self._crossover(p1.weights, p2.weights)
                else:
                    child_w = dict(p1.weights)
                child_w = self._mutate(child_w)
                obj = await self._evaluate_weights(child_w)
                offspring.append(MOPDGenerationStrategy(
                    strategy_id=str(uuid.uuid4()),
                    weights=child_w,
                    objectives=obj,
                    scalarised_score=self._scalarise(obj),
                ))

            # Combine + truncate
            combined = population + offspring
            fronts = self._fast_non_dominated_sort(combined)
            next_pop: List[MOPDGenerationStrategy] = []
            for front in fronts:
                if len(next_pop) + len(front) <= self.population_size:
                    next_pop.extend(front)
                else:
                    cd = self._crowding_distance(front)
                    front_sorted = sorted(front, key=lambda p: cd.get(id(p), 0.0), reverse=True)
                    next_pop.extend(front_sorted[: self.population_size - len(next_pop)])
                    break
            population = next_pop

        # Final Pareto front
        fronts = self._fast_non_dominated_sort(population)
        pareto = fronts[0] if fronts else population
        self.pareto_front = pareto
        if pareto:
            self.best_individual = max(pareto, key=lambda p: p.scalarised_score)
            self.best_fitness = self.best_individual.scalarised_score
        return pareto


# ============================================================================
# Per-request context
# ============================================================================
@dataclass
class GenerationContext:
    state: GenerationState
    state_vec: np.ndarray
    action_idx: int
    strategy: str
    teacher_probs: np.ndarray
    selected_expert: Optional[str] = None
    expert_probs: Optional[np.ndarray] = None


# ============================================================================
# HeliumSyntheticGenerator
# ============================================================================
class HeliumSyntheticGenerator:
    STRATEGIES = ["realistic", "diverse", "edge_case_heavy", "balanced", "custom"]

    def __init__(
        self,
        config: Optional[Union[Dict[str, Any], "HeliumSyntheticConfig"]] = None,
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

        # Per-instance RNG (never touches global random state).
        seed = int(self.config.get("seed", 42))
        self._rng = random.Random(seed)
        self._np_rng = np.random.default_rng(seed)

        self._extract_params()

        # Distillation optimizer
        historical_path = Path(self.config.get(
            "historical_model_path", "./synth_historical_model.pkl"
        ))
        q_weights_path = Path(self.config.get(
            "q_weights_path", "./synth_q_weights.json"
        ))
        self.strategy_optimizer = DistillationGeneratorOptimizer(
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

        # Generation logs + buffer
        self.generation_logs: List[Dict[str, Any]] = []
        self._log_buffer: List[Dict[str, Any]] = []
        self._log_flush_size = 25
        self._log_lock = asyncio.Lock()
        self._log_path = Path(self.config.get(
            "generation_logs_path", "./synth_generation_logs.csv"
        ))

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
                "quality": 0.4, "diversity": 0.3,
                "edge_coverage": 0.2, "time_efficiency": 0.1,
            })
        )
        self.moea_dynamic_weights: bool = bool(self.config.get("moea_dynamic_weights", True))
        self.moea_optimizer: Optional[NSGAIIGeneratorOptimizer] = None
        self.evolved_pareto_front: List[MOPDGenerationStrategy] = []
        self.best_evolved_strategy: Optional[MOPDGenerationStrategy] = None
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

        log_event("info", "HeliumSyntheticGenerator v2.5.0 initialized")

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def start(self) -> "HeliumSyntheticGenerator":
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
            self.strategy_optimizer.q_teacher.save_weights(
                self.config.get("q_weights_path", "./synth_q_weights.json")
            )
        except Exception as exc:
            log_event("warning", f"Failed to persist Q weights: {exc}")

        # Persist Pareto front
        if self.evolved_pareto_front:
            try:
                path = Path(self.config.get(
                    "moea_pareto_path", "./synth_moea_pareto.json"
                ))
                path.parent.mkdir(parents=True, exist_ok=True)
                with open(path, "w") as f:
                    json.dump([p.to_dict() for p in self.evolved_pareto_front], f, indent=2)
            except Exception as exc:
                log_event("warning", f"Failed to persist Pareto front: {exc}")

        self._started = False

    async def __aenter__(self) -> "HeliumSyntheticGenerator":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    # ------------------------------------------------------------------
    # LIMIT graph
    # ------------------------------------------------------------------
    def _init_limit_graph(self) -> None:
        assert self.limit_graph_manager is not None
        graph_id = "generation_strategies"
        if self.limit_graph_manager.get_metadata(graph_id):
            return
        self.limit_graph_manager.create_graph(graph_id, "Generation Strategy Dependencies", {})
        for strat in self.STRATEGIES:
            self.limit_graph_manager.add_node(graph_id, f"strategy_{strat}", strat, {})
        for param in ("num_hotspots", "duration_hours", "base_events_per_hour"):
            self.limit_graph_manager.add_node(graph_id, f"param_{param}", param, {})
        for strat in self.STRATEGIES:
            for param in ("num_hotspots", "duration_hours", "base_events_per_hour"):
                self.limit_graph_manager.add_edge(
                    graph_id, f"edge_{strat}_{param}",
                    f"strategy_{strat}", f"param_{param}", 1.0, {},
                )

    # ------------------------------------------------------------------
    # Config helpers
    # ------------------------------------------------------------------
    def _get_config(self, key: str, default: Any = None) -> Any:
        return self.config.get(key, default)

    def _get_config_dict(self) -> Dict[str, Any]:
        return copy.deepcopy(self.config)

    def _extract_params(self) -> None:
        self.num_hotspots = int(self._get_config("num_hotspots", 100))
        self.num_gateways = int(self._get_config("num_gateways", 5))
        self.duration_hours = float(self._get_config("duration_hours", 24.0))
        self.base_events_per_hour = float(self._get_config("base_events_per_hour", 10.0))
        self.rssi_mean_urban = float(self._get_config("rssi_mean_urban", -70.0))
        self.rssi_std_urban = float(self._get_config("rssi_std_urban", 10.0))
        self.rssi_mean_rural = float(self._get_config("rssi_mean_rural", -80.0))
        self.rssi_std_rural = float(self._get_config("rssi_std_rural", 15.0))
        self.snr_mean = float(self._get_config("snr_mean", 12.0))
        self.snr_std = float(self._get_config("snr_std", 3.0))
        self.num_clusters = int(self._get_config("num_clusters", 3))
        self.cluster_spread = float(self._get_config("cluster_spread", 0.2))
        self.path_loss_exponent = float(self._get_config("path_loss_exponent", 2.0))
        self.reference_distance_km = float(self._get_config("reference_distance_km", 1.0))
        self.shadowing_std = float(self._get_config("shadowing_std", 3.0))
        self.diurnal_amplitude = float(self._get_config("diurnal_amplitude", 0.3))
        self.diurnal_peak_hour = int(self._get_config("diurnal_peak_hour", 14))
        self.burst_probability = float(self._get_config("burst_probability", 0.1))
        self.burst_multiplier = float(self._get_config("burst_multiplier", 5.0))
        self.edge_case_rate = float(self._get_config("edge_case_rate", 0.0))
        self.export_format = str(self._get_config("export_format", "parquet"))
        self.validation_alpha = float(self._get_config("validation_alpha", 0.05))

    def load_config_from_json(self, path: Union[str, Path]) -> None:
        """Load config and rebuild internal state (RNG, optimizer, MoE)."""
        with open(path, "r") as f:
            raw = json.load(f)
        self.config = normalize_config(raw)
        seed = int(self.config.get("seed", 42))
        self._rng = random.Random(seed)
        self._np_rng = np.random.default_rng(seed)
        self._extract_params()
        # Rebuild distillation optimizer with new config.
        historical_path = Path(self.config.get(
            "historical_model_path", "./synth_historical_model.pkl"
        ))
        q_weights_path = Path(self.config.get(
            "q_weights_path", "./synth_q_weights.json"
        ))
        self.strategy_optimizer = DistillationGeneratorOptimizer(
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
        # Rebuild MoE gating with the new expert count.
        if self.moe_gating is not None:
            self.moe_gating = MoEGatingNetwork(
                self.storage, {"moe_expert_count": int(self.config.get("moe_expert_count", 4))}
            )
        log_event("info", f"Reloaded config from {path}")

    def save_config_to_json(self, path: Union[str, Path]) -> None:
        with open(path, "w") as f:
            json.dump(self._get_config_dict(), f, indent=2)

    # ------------------------------------------------------------------
    # State builder
    # ------------------------------------------------------------------
    def _build_state(self, user_objectives: Optional[Dict[str, Any]] = None) -> GenerationState:
        user_objectives = user_objectives or {}
        target_ks = float(user_objectives.get("target_ks", 0.05))
        target_anomaly = float(user_objectives.get("target_anomaly_rate", 0.02))
        target_diversity = float(user_objectives.get("target_diversity", 0.8))

        if self.generation_logs:
            last_log = self.generation_logs[-1]
            val = last_log.get("validation", {}) or {}
            rssi_ks_p = float(val.get("rssi_ks_test", {}).get("p_value", 0.5))
            snr_ks_p = float(val.get("snr_ks_test", {}).get("p_value", 0.5))
            uplink_p = float(val.get("uplink_chisquare", {}).get("p_value", 0.5))
            diurnal_p = float(val.get("diurnal_binomial", {}).get("p_value", 0.5))
            rewards = [float(lg.get("reward", 0.0)) for lg in self.generation_logs[-20:]]
            avg_quality = float(np.mean(rewards)) if rewards else 0.5
            try:
                last_ts = datetime.fromisoformat(last_log["timestamp"])
                if last_ts.tzinfo is None:
                    last_ts = last_ts.replace(tzinfo=timezone.utc)
                hours_since = (datetime.now(timezone.utc) - last_ts).total_seconds() / 3600.0
            except Exception:
                hours_since = 0.0
        else:
            rssi_ks_p = 0.5
            snr_ks_p = 0.5
            uplink_p = 0.5
            diurnal_p = 0.5
            avg_quality = 0.5
            hours_since = 0.0

        return GenerationState(
            target_ks_stat=target_ks,
            target_anomaly_rate=target_anomaly,
            target_diversity=target_diversity,
            last_rssi_ks_p=rssi_ks_p,
            last_snr_ks_p=snr_ks_p,
            last_uplink_chisq_p=uplink_p,
            last_diurnal_p=diurnal_p,
            avg_quality_score=avg_quality,
            num_traces_generated=len(self.generation_logs),
            hours_since_last=hours_since,
        )

    # ------------------------------------------------------------------
    # Strategy application
    # ------------------------------------------------------------------
    def _apply_strategy(
        self, strategy: str, user_objectives: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        cfg = copy.deepcopy(self.config)
        user_objectives = user_objectives or {}

        if strategy == "realistic":
            cfg["edge_case_rate"] = 0.02
            cfg["diurnal_amplitude"] = 0.3
            cfg["diurnal_peak_hour"] = 14
            cfg["burst_probability"] = 0.05
        elif strategy == "diverse":
            cfg["num_clusters"] = max(5, int(cfg.get("num_clusters", 3)))
            cfg["cluster_spread"] = 0.4
            cfg["num_hotspots"] = int(cfg.get("num_hotspots", 100) * 1.5)
        elif strategy == "edge_case_heavy":
            cfg["edge_case_rate"] = 0.3
            cfg["burst_probability"] = 0.3
            cfg["burst_multiplier"] = 10.0
        elif strategy == "balanced":
            cfg["edge_case_rate"] = 0.05
            cfg["diurnal_amplitude"] = 0.2
            cfg["burst_probability"] = 0.1
            cfg["num_clusters"] = 3
            cfg["cluster_spread"] = 0.2
        elif strategy == "custom":
            for k, v in user_objectives.items():
                if k in cfg:
                    cfg[k] = v
                else:
                    log_event("warning", f"Unknown custom key ignored: {k}")

        cfg["num_hotspots"] = max(1, int(cfg["num_hotspots"]))
        cfg["num_gateways"] = max(1, int(cfg["num_gateways"]))
        cfg["num_clusters"] = max(1, int(cfg["num_clusters"]))
        cfg["seed"] = int(cfg.get("seed", 42)) + len(self.generation_logs) * 7
        return cfg

    # ------------------------------------------------------------------
    # Trace generation
    # ------------------------------------------------------------------
    def _generate_trace_for_config(
        self, cfg: Dict[str, Any], start_time: Optional[datetime] = None
    ) -> pd.DataFrame:
        """Pure generator: no global state, no new generator instances."""
        rng = random.Random(int(cfg.get("seed", 42)))
        np_rng = np.random.default_rng(int(cfg.get("seed", 42)))

        num_hotspots = int(cfg["num_hotspots"])
        num_gateways = int(cfg["num_gateways"])
        duration_hours = float(cfg["duration_hours"])
        base_events_per_hour = float(cfg["base_events_per_hour"])
        num_clusters = int(cfg["num_clusters"])
        cluster_spread = float(cfg["cluster_spread"])
        path_loss_exponent = float(cfg["path_loss_exponent"])
        reference_distance_km = float(cfg["reference_distance_km"])
        shadowing_std = float(cfg["shadowing_std"])
        diurnal_amplitude = float(cfg["diurnal_amplitude"])
        diurnal_peak_hour = int(cfg["diurnal_peak_hour"])
        burst_probability = float(cfg["burst_probability"])
        burst_multiplier = float(cfg["burst_multiplier"])
        edge_case_rate = float(cfg["edge_case_rate"])
        target_anomaly_rate = float(cfg.get("target_anomaly_rate", 0.01))

        if start_time is None:
            start_time = datetime.now(timezone.utc) - timedelta(hours=duration_hours)

        # Cluster centers.
        centers = [
            (rng.uniform(-0.5, 0.5), rng.uniform(-0.5, 0.5))
            for _ in range(num_clusters)
        ]
        hotspot_positions: Dict[int, Tuple[float, float]] = {}
        for i in range(num_hotspots):
            cx, cy = rng.choice(centers)
            hotspot_positions[i] = (
                cx + float(np_rng.normal(0, cluster_spread)),
                cy + float(np_rng.normal(0, cluster_spread)),
            )

        gateway_positions = [
            (rng.uniform(-0.5, 0.5), rng.uniform(-0.5, 0.5))
            for _ in range(num_gateways)
        ]

        timestamps: List[datetime] = []
        hotspot_ids: List[str] = []
        gateway_ids: List[str] = []
        rssi_values: List[float] = []
        snr_values: List[float] = []
        anomaly_flags: List[int] = []

        num_hours = max(1, int(math.ceil(duration_hours)))

        for hour_idx in range(num_hours):
            hour_start = start_time + timedelta(hours=hour_idx)
            # Diurnal factor is now actually applied to event counts.
            diurnal_factor = 1.0 + diurnal_amplitude * math.cos(
                2 * math.pi * (hour_start.hour - diurnal_peak_hour) / 24.0
            )
            if rng.random() < burst_probability:
                diurnal_factor *= burst_multiplier
            events_this_hour = int(
                base_events_per_hour * num_hotspots * diurnal_factor
            )

            for _ in range(events_this_hour):
                # Uniform within the hour.
                offset = timedelta(seconds=rng.uniform(0, 3600))
                event_time = hour_start + offset
                hotspot_id = rng.randint(0, num_hotspots - 1)
                gateway_id = rng.randint(0, num_gateways - 1)

                hx, hy = hotspot_positions[hotspot_id]
                gx, gy = gateway_positions[gateway_id]
                dx, dy = hx - gx, hy - gy
                distance_km = math.sqrt(dx * dx + dy * dy) * 100.0

                path_loss = path_loss_exponent * 10 * math.log10(
                    max(distance_km, 1e-6) / reference_distance_km
                )
                shadowing = float(np_rng.normal(0, shadowing_std))
                rssi = self.rssi_mean_urban - path_loss + shadowing
                snr = float(np_rng.normal(self.snr_mean, self.snr_std))

                is_edge = False
                if rng.random() < edge_case_rate:
                    is_edge = True
                    rssi = rng.choice([-200.0, -10.0, 0.0])
                    snr = rng.choice([-50.0, 50.0])

                # Anomaly: driven by target rate; edge cases are anomalies.
                anomaly = 1 if (is_edge or rng.random() < target_anomaly_rate) else 0

                timestamps.append(event_time)
                hotspot_ids.append(f"hotspot_{hotspot_id}")
                gateway_ids.append(f"gateway_{gateway_id}")
                rssi_values.append(float(rssi))
                snr_values.append(float(snr))
                anomaly_flags.append(anomaly)

        df = pd.DataFrame({
            "timestamp": timestamps,
            "hotspot_id": hotspot_ids,
            "gateway_id": gateway_ids,
            "rssi": rssi_values,
            "snr": snr_values,
            "anomaly": anomaly_flags,
        })
        return df

    # ------------------------------------------------------------------
    # Public generation API
    # ------------------------------------------------------------------
    async def generate_trace_async(
        self,
        num_hotspots: Optional[int] = None,
        duration_hours: Optional[float] = None,
        base_events_per_hour: Optional[float] = None,
        user_objectives: Optional[Dict[str, Any]] = None,
        start_time: Optional[datetime] = None,
        **kwargs: Any,
    ) -> pd.DataFrame:
        if not self._started:
            await self.start()

        state = self._build_state(user_objectives)

        # Distillation provides the actual teacher signal, always.
        strategy, action_idx, state_vec, teacher_probs = (
            await self.strategy_optimizer.select_strategy(state, exploration=True)
        )

        selected_expert: Optional[str] = None
        expert_probs: Optional[np.ndarray] = None
        if self.moe_gating:
            selected_expert, expert_probs = await self.moe_gating.select_expert(state)
            # MoE can bias the choice, but the teacher probs stay informative.
            if selected_expert in self.STRATEGIES:
                expert_action = self.STRATEGIES.index(selected_expert)
                # Light bias: prefer expert action if student is uncertain.
                if float(np.max(teacher_probs)) < 0.4:
                    strategy = selected_expert
                    action_idx = expert_action

        ctx = GenerationContext(
            state=state,
            state_vec=state_vec,
            action_idx=action_idx,
            strategy=strategy,
            teacher_probs=teacher_probs,
            selected_expert=selected_expert,
            expert_probs=expert_probs,
        )

        cfg = self._apply_strategy(strategy, user_objectives)
        if num_hotspots is not None:
            cfg["num_hotspots"] = max(1, int(num_hotspots))
        if duration_hours is not None:
            cfg["duration_hours"] = float(duration_hours)
        if base_events_per_hour is not None:
            cfg["base_events_per_hour"] = float(base_events_per_hour)
        for k, v in kwargs.items():
            if k in cfg:
                cfg[k] = v

        df = self._generate_trace_for_config(cfg, start_time=start_time)
        validation_results = self.validate_trace(df, cfg=cfg)
        reward = self._compute_reward(validation_results, user_objectives, df)

        await self._log_generation(ctx, reward, validation_results)

        # Update MoE gating (per request, race-free).
        if self.moe_gating and ctx.selected_expert:
            await self.moe_gating.add_training_sample(ctx.state, ctx.selected_expert, reward)

        # Update distillation student + Q teacher (per request).
        next_state_vec = self._build_state(user_objectives).to_feature_vector()
        await self.strategy_optimizer.update(
            ctx.state_vec, ctx.action_idx, reward, next_state_vec, ctx.teacher_probs,
        )

        # Optional RLHF sample.
        if self.rlhf_trainer and random.random() < 0.05:
            rejected = random.choice(
                [s for s in self.STRATEGIES if s != strategy] or [strategy]
            )
            self.rlhf_trainer.record_pair(
                pair_id=str(uuid.uuid4()),
                prompt="Which generation strategy is better?",
                chosen=strategy,
                rejected=rejected,
                reward_diff=reward,
                metadata={
                    "num_hotspots": cfg.get("num_hotspots"),
                    "duration_hours": cfg.get("duration_hours"),
                },
            )

        # MODP state.
        if self.modp_solver:
            self.modp_solver.add_state(
                state_id=f"{datetime.now(timezone.utc).isoformat()}_{strategy}",
                problem_id="generation_strategy_selection",
                state_attributes={
                    "strategy": strategy,
                    "num_hotspots": cfg.get("num_hotspots"),
                    "duration_hours": cfg.get("duration_hours"),
                },
                objective_values={
                    "quality": reward, "diversity": 0.5,
                    "edge_coverage": 0.3, "time_efficiency": 0.5,
                },
                stage=0,
            )

        df.attrs["version"] = "2.5.0"
        df.attrs["strategy"] = strategy
        df.attrs["reward"] = reward
        df.attrs["parameters"] = cfg
        return df

    def generate_trace(self, *args: Any, **kwargs: Any) -> pd.DataFrame:
        """Synchronous convenience wrapper.

        Refuses to run inside a live event loop; use `await generate_trace_async(...)`
        from async code instead.
        """
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(self.generate_trace_async(*args, **kwargs))
        raise RuntimeError(
            "generate_trace() was called from a running event loop. "
            "Use `await generate_trace_async(...)` instead."
        )

    async def generate_multiple_traces_async(
        self, n: int, *args: Any, **kwargs: Any
    ) -> List[pd.DataFrame]:
        return [await self.generate_trace_async(*args, **kwargs) for _ in range(n)]

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------
    def validate_trace(
        self, df: pd.DataFrame, cfg: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Validate against fixed reference distributions.

        Unlike the previous version, this does not compare the sample to its own
        fitted normal (which is a tautology). It compares against the target
        distribution implied by the generator's config.
        """
        results: Dict[str, Any] = {}
        if not SCIPY_AVAILABLE or len(df) < 10:
            return results

        cfg = cfg or {}
        rssi_vals = df["rssi"].values.astype(float)
        snr_vals = df["snr"].values.astype(float)

        # RSSI: KS against the target normal.
        rssi_mu = float(cfg.get("rssi_mean_urban", self.rssi_mean_urban))
        rssi_sigma = max(float(cfg.get("rssi_std_urban", self.rssi_std_urban)), 1e-6)
        ks_stat, p_value = stats.kstest(
            rssi_vals, "norm", args=(rssi_mu, rssi_sigma)
        )
        results["rssi_ks_test"] = {"p_value": float(p_value), "statistic": float(ks_stat)}

        # SNR: KS against the target normal.
        snr_mu = float(cfg.get("snr_mean", self.snr_mean))
        snr_sigma = max(float(cfg.get("snr_std", self.snr_std)), 1e-6)
        ks_stat, p_value = stats.kstest(
            snr_vals, "norm", args=(snr_mu, snr_sigma)
        )
        results["snr_ks_test"] = {"p_value": float(p_value), "statistic": float(ks_stat)}

        # Uplink chi-square: hour-of-day distribution vs. expected diurnal.
        if "timestamp" in df.columns:
            hours = pd.to_datetime(df["timestamp"]).dt.hour.values
            observed = np.bincount(hours, minlength=24).astype(float)
            peak = int(cfg.get("diurnal_peak_hour", self.diurnal_peak_hour))
            amp = float(cfg.get("diurnal_amplitude", self.diurnal_amplitude))
            expected = np.array([
                max(1e-6, 1.0 + amp * math.cos(2 * math.pi * (h - peak) / 24.0))
                for h in range(24)
            ])
            expected = expected / expected.sum() * observed.sum()
            # Only run if we have enough expected counts per bin.
            if np.all(expected > 5):
                try:
                    chi2, p = stats.chisquare(observed, expected)
                    results["uplink_chisquare"] = {
                        "p_value": float(p), "statistic": float(chi2)
                    }
                except Exception:
                    results["uplink_chisquare"] = {"p_value": 0.5, "statistic": 0.0}

            # Diurnal binomial: peak hours vs. off-peak.
            peak_hours = {(peak - 2) % 24, (peak - 1) % 24, peak % 24,
                          (peak + 1) % 24, (peak + 2) % 24}
            in_peak = np.isin(hours, list(peak_hours)).sum()
            total = len(hours)
            expected_frac = len(peak_hours) / 24.0
            if total > 0:
                try:
                    p = stats.binomtest(int(in_peak), total, expected_frac).pvalue
                    results["diurnal_binomial"] = {
                        "p_value": float(p),
                        "statistic": float(in_peak / total),
                    }
                except Exception:
                    results["diurnal_binomial"] = {"p_value": 0.5, "statistic": 0.0}

        return results

    def _compute_reward(
        self,
        validation_results: Dict[str, Any],
        user_objectives: Optional[Dict[str, Any]] = None,
        df: Optional[pd.DataFrame] = None,
    ) -> float:
        """Smooth, bounded, monotone reward.

        Components:
        - quality: mean of the KS/binomial p-values, clipped to [0, 1].
        - anomaly: exp(-|actual - target| / floor) for a smooth gradient.
        """
        p_values = []
        for key in ("rssi_ks_test", "snr_ks_test", "uplink_chisquare", "diurnal_binomial"):
            if key in validation_results:
                p_values.append(float(validation_results[key].get("p_value", 0.5)))
        quality_score = float(np.mean(p_values)) if p_values else 0.5
        quality_score = max(0.0, min(1.0, quality_score))

        target_anomaly = float((user_objectives or {}).get("target_anomaly_rate", 0.0))
        if df is not None and "anomaly" in df.columns and len(df) > 0:
            actual_anomaly = float(df["anomaly"].mean())
            floor = max(target_anomaly, 0.01)
            anomaly_score = math.exp(-abs(actual_anomaly - target_anomaly) / floor)
        else:
            anomaly_score = 0.5

        reward = 0.6 * quality_score + 0.4 * anomaly_score
        return float(max(0.0, min(1.0, reward)))

    # ------------------------------------------------------------------
    # Logging
    # ------------------------------------------------------------------
    async def _log_generation(
        self,
        ctx: GenerationContext,
        reward: float,
        validation_results: Dict[str, Any],
    ) -> None:
        entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "strategy": ctx.strategy,
            "action_idx": int(ctx.action_idx),
            "selected_expert": ctx.selected_expert or "",
            "reward": float(reward),
            "validation_json": json.dumps(validation_results, default=str),
            # Comma-separated string: training expects a string.
            "state_vector": ",".join(f"{v:.6g}" for v in ctx.state_vec),
        }
        async with self._log_lock:
            self.generation_logs.append({**entry, "validation": validation_results})
            if len(self.generation_logs) > 10_000:
                self.generation_logs = self.generation_logs[-5_000:]
            self._log_buffer.append(entry)
            if len(self._log_buffer) >= self._log_flush_size:
                await self._flush_log_buffer_locked()

    async def _flush_log_buffer_locked(self) -> None:
        if not self._log_buffer:
            return
        buffer, self._log_buffer = self._log_buffer, []

        def _write() -> None:
            path = self._log_path
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
            log_event("warning", f"Failed to flush generation log: {exc}")

    # ------------------------------------------------------------------
    # Historical model
    # ------------------------------------------------------------------
    @classmethod
    def train_historical_model(
        cls,
        log_path: Union[str, Path] = "./synth_generation_logs.csv",
        model_path: Union[str, Path] = "./synth_historical_model.pkl",
    ) -> None:
        log_path = Path(log_path)
        model_path = Path(model_path)
        if not log_path.exists():
            log_event("warning", f"Generation logs not found at {log_path}. No model trained.")
            return
        try:
            df_logs = pd.read_csv(log_path)
        except Exception as exc:
            log_event("error", f"Failed to read logs: {exc}")
            return
        if len(df_logs) < 10 or "state_vector" not in df_logs.columns:
            log_event("warning", "Not enough logs or missing state_vector column.")
            return
        if not SKLEARN_ML:
            log_event("error", "scikit-learn required.")
            return

        def _parse_vec(s: Any) -> np.ndarray:
            if isinstance(s, str):
                return np.asarray(
                    [float(x) for x in s.split(",") if x != ""], dtype=np.float32
                )
            return np.asarray(s, dtype=np.float32)

        try:
            X = np.stack([_parse_vec(s) for s in df_logs["state_vector"].values])
        except Exception as exc:
            log_event("error", f"Failed to parse state vectors: {exc}")
            return

        y = df_logs["strategy"].values
        le = LabelEncoder()
        y_enc = le.fit_transform(y)
        clf = RandomForestClassifier(n_estimators=100, random_state=42)
        clf.fit(X, y_enc)

        model_path.parent.mkdir(parents=True, exist_ok=True)
        with open(model_path, "wb") as f:
            pickle.dump((clf, le), f)
        log_event("info", f"Historical model saved to {model_path}")

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------
    def save_trace(self, df: pd.DataFrame, path: Union[str, Path]) -> None:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        if self.export_format == "parquet":
            try:
                df.to_parquet(path)
                return
            except Exception as exc:
                log_event(
                    "warning",
                    f"Parquet export failed, falling back to CSV: {exc}",
                )
        if self.export_format == "csv":
            df.to_csv(path, index=False)
        elif self.export_format == "json":
            df.to_json(path, orient="records")

    def export_with_metadata(self, df: pd.DataFrame, path: Union[str, Path]) -> None:
        path = Path(path)
        self.save_trace(df, path)
        meta_path = path.with_suffix(".meta.json")
        meta = {
            "version": self._get_config("version", "2.5.0"),
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "parameters": self._get_config_dict(),
        }
        with open(meta_path, "w") as f:
            json.dump(meta, f, indent=2)

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

    async def run_strategy_evolution(self) -> List[MOPDGenerationStrategy]:
        if not self.moea_enabled:
            return []

        async def evaluate(weights: Dict[str, float]) -> Dict[str, float]:
            """Objectives derived from real generation logs.

            All objectives are treated as maximized.
            """
            recent = self.generation_logs[-100:]
            if len(recent) < 5:
                return {
                    "quality": 0.0, "diversity": 0.0,
                    "edge_coverage": 0.0, "time_efficiency": 0.0,
                }
            rewards = [float(lg.get("reward", 0.0)) for lg in recent]
            quality = float(np.mean(rewards))

            # Diversity: fraction of distinct strategies used recently.
            strategies_used = {lg.get("strategy") for lg in recent if lg.get("strategy")}
            diversity = len(strategies_used) / max(len(self.STRATEGIES), 1)

            # Edge coverage: fraction of traces using edge_case_heavy.
            edge_coverage = sum(
                1 for lg in recent if lg.get("strategy") == "edge_case_heavy"
            ) / len(recent)

            # Time efficiency: inverse of average interval between generations.
            timestamps = []
            for lg in recent:
                try:
                    ts = datetime.fromisoformat(lg["timestamp"])
                    if ts.tzinfo is None:
                        ts = ts.replace(tzinfo=timezone.utc)
                    timestamps.append(ts)
                except Exception:
                    continue
            if len(timestamps) >= 2:
                deltas = [
                    (timestamps[i + 1] - timestamps[i]).total_seconds()
                    for i in range(len(timestamps) - 1)
                ]
                avg_delta = max(float(np.mean(deltas)), 1.0)
                # Reward faster generation; cap at 1.0.
                time_efficiency = min(60.0 / avg_delta, 1.0)
            else:
                time_efficiency = 0.5

            return {
                "quality": quality,
                "diversity": diversity,
                "edge_coverage": edge_coverage,
                "time_efficiency": time_efficiency,
            }

        self.moea_optimizer = NSGAIIGeneratorOptimizer(
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
                log_event("info", f"Best evolved strategy weights: {best.weights}")

        # Persist Pareto front.
        try:
            path = Path(self.config.get(
                "moea_pareto_path", "./synth_moea_pareto.json"
            ))
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "w") as f:
                json.dump([p.to_dict() for p in pareto], f, indent=2)
        except Exception as exc:
            log_event("warning", f"Failed to persist Pareto front: {exc}")

        return pareto

    def _get_dynamic_moea_weights(self) -> Dict[str, float]:
        weights = dict(self.moea_objective_weights)
        if len(self.generation_logs) > 10:
            recent_rewards = [
                float(lg.get("reward", 0.0)) for lg in self.generation_logs[-10:]
            ]
            avg_reward = float(np.mean(recent_rewards)) if recent_rewards else 0.0
            if avg_reward < 0.4:
                weights["quality"] = min(0.6, weights.get("quality", 0.4) * 1.5)
            total = sum(weights.values())
            if total > 0:
                weights = {k: v / total for k, v in weights.items()}
        return weights

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------
    async def get_evolved_pareto_front(self) -> List[Dict[str, Any]]:
        return [p.to_dict() for p in self.evolved_pareto_front]

    async def get_limit_graph(self, graph_id: str = "generation_strategies") -> Dict[str, Any]:
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
        stats = self.strategy_optimizer.get_stats()
        stats.update({
            "generation_log_size": len(self.generation_logs),
            "pareto_front_size": len(self.evolved_pareto_front),
            "started": self._started,
        })
        return stats


# ============================================================================
# Factory
# ============================================================================
def create_helium_synthetic_generator(
    config: Optional[Union[Dict[str, Any], "HeliumSyntheticConfig"]] = None,
    storage: Optional[Any] = None,
) -> HeliumSyntheticGenerator:
    return HeliumSyntheticGenerator(config, storage)


# ============================================================================
# Example usage
# ============================================================================
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    async def demo() -> None:
        config = {
            "num_hotspots": 50,
            "duration_hours": 6,
            "base_events_per_hour": 5,
            "distillation_epsilon": 0.2,
            "distillation_train_every": 2,
            "moea_enabled": True,
            "moea_interval_seconds": 60,
            "enable_limit_graph": True,
            "enable_modp": True,
            "enable_rlhf": True,
            "enable_moe": True,
        }
        gen = HeliumSyntheticGenerator(config)
        try:
            df = await gen.generate_trace_async(
                user_objectives={"target_anomaly_rate": 0.1}
            )
            print(f"Generated {len(df)} events, strategy used: {df.attrs.get('strategy')}")

            df2 = await gen.generate_trace_async()
            print(f"Second trace strategy: {df2.attrs.get('strategy')}")

            if SCIPY_AVAILABLE:
                results = gen.validate_trace(df)
                print("Validation results:", results)

            pareto = await gen.run_strategy_evolution()
            print(f"Evolved Pareto front size: {len(pareto)}")
            if gen.best_evolved_strategy:
                print("Best strategy weights:", gen.best_evolved_strategy.weights)

            print("Distillation stats:", gen.strategy_optimizer.get_stats())
            print("LIMIT Graph:", await gen.get_limit_graph())
            print("MoE experts:", await gen.get_moe_experts())
        finally:
            await gen.close()

    asyncio.run(demo())
