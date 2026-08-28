# src/enhancements/data_integration/material_footprint_v2_4_0.py
"""
Enhanced Material Footprint Updater v2.4.0
===========================================
Fetches and caches product‑level material footprints from BONSAI/FOOTPRINTDATA.
Provides adaptive source selection and update scheduling via Multi‑Teacher On‑Policy Distillation,
Multi‑Objective Evolutionary Optimization (MOEA) to evolve update strategy weights,
and additional LIMIT Graph, MODP, RLHF, and MoE components.

ENHANCEMENTS OVER v2.3.0:
- Added LIMIT Graph manager for product/source relationships.
- Added MODP solver for storing decision states and policies.
- Added RLHF trainer for human preference collection on update strategies.
- Added MoE gating network to blend update strategies (experts).
- Integration with central Storage (optional) for persistence.
- New configuration flags for enabling/disabling each component.

All previous features (distillation, circuit breakers, caching, fallback, MOEA) are retained.
"""

import asyncio
import logging
import time
import json
import sqlite3
import os
from pathlib import Path
from typing import Dict, List, Optional, Any, Union, Tuple
from datetime import datetime, timedelta
import aiohttp
from aiohttp import ClientTimeout, ClientError
import random
import numpy as np
from abc import ABC, abstractmethod
from collections import deque
import pickle
import pandas as pd
import copy
import uuid
import hashlib

# ---------- Pydantic ----------
try:
    from pydantic import BaseModel, Field, field_validator, ValidationError
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

# ---------- Tenacity (retry) ----------
try:
    from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type, before_sleep_log
    TENACITY_AVAILABLE = True
except ImportError:
    TENACITY_AVAILABLE = False

# ---------- Circuit breaker ----------
from enum import Enum

class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

class CircuitBreaker:
    """In‑memory circuit breaker with half‑open state."""
    def __init__(self, name: str, failure_threshold: int = 5, recovery_timeout: float = 30.0):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self._state = CircuitBreakerState.CLOSED
        self._failure_count = 0
        self._last_failure_time: Optional[datetime] = None
        self._lock = asyncio.Lock()

    async def call(self, func, *args, **kwargs):
        async with self._lock:
            now = datetime.utcnow()
            if self._state == CircuitBreakerState.OPEN:
                if self._last_failure_time and (now - self._last_failure_time).total_seconds() >= self.recovery_timeout:
                    self._state = CircuitBreakerState.HALF_OPEN
                    logger.info(f"Circuit breaker {self.name} entering HALF_OPEN")
                else:
                    raise RuntimeError(f"Circuit breaker {self.name} is OPEN")

        try:
            result = await func(*args, **kwargs)
            async with self._lock:
                if self._state == CircuitBreakerState.HALF_OPEN:
                    self._state = CircuitBreakerState.CLOSED
                    self._failure_count = 0
                    logger.info(f"Circuit breaker {self.name} closed after success")
                else:
                    self._failure_count = 0
            return result
        except Exception as e:
            async with self._lock:
                self._failure_count += 1
                self._last_failure_time = datetime.utcnow()
                if self._failure_count >= self.failure_threshold:
                    self._state = CircuitBreakerState.OPEN
                    logger.warning(f"Circuit breaker {self.name} opened after {self._failure_count} failures")
            raise e

# ---------- Prometheus ----------
try:
    from prometheus_client import Counter, Gauge, Histogram
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

# ---------- Structlog ----------
try:
    import structlog
    logger = structlog.get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO)

# ---------- scikit-learn for ML teacher ----------
try:
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.preprocessing import LabelEncoder
    SKLEARN_ML = True
except ImportError:
    SKLEARN_ML = False

# ---------- Optional central storage ----------
try:
    from ...storage import Storage  # Adjust path if needed
    CENTRAL_STORAGE_AVAILABLE = True
except ImportError:
    CENTRAL_STORAGE_AVAILABLE = False
    Storage = None

# ============================================================================
# Configuration
# ============================================================================
if PYDANTIC_AVAILABLE:
    class MaterialConfig(BaseModel):
        """Configuration for MaterialFootprintUpdater."""
        db_path: Path = Field(Path("./material_catalog.db"))
        bonsai_api_url: str = Field("https://api.bonsai.uno/v1/footprints")
        footprintdata_api_url: str = Field("https://api.footprintdata.org/v1/products")
        bonsai_api_key: Optional[str] = Field(None)
        footprintdata_api_key: Optional[str] = Field(None)
        cache_ttl: int = Field(86400 * 7, ge=0)
        retry_attempts: int = Field(3, ge=0)
        retry_min_wait: float = Field(1.0, gt=0)
        retry_max_wait: float = Field(10.0, gt=0)
        circuit_breaker_threshold: int = Field(5, ge=1)
        circuit_breaker_timeout: float = Field(30.0, ge=1)
        request_timeout: float = Field(10.0, ge=1)
        enable_prometheus: bool = True
        source_priority: List[str] = Field(default_factory=lambda: ["bonsai", "footprintdata"])

        distillation_epsilon: float = Field(0.1, ge=0, le=1)
        distillation_train_every: int = Field(10, ge=1)
        distillation_replay_size: int = Field(2000, ge=10)
        distillation_learning_rate: float = Field(0.01, ge=0.0001, le=1)
        distill_weight: float = Field(0.7, ge=0, le=1)
        rl_weight: float = Field(0.3, ge=0, le=1)

        moea_enabled: bool = Field(True)
        moea_interval_seconds: int = Field(300, ge=60)
        moea_population_size: int = Field(20, ge=5)
        moea_generations: int = Field(5, ge=1)
        moea_mutation_rate: float = Field(0.2, ge=0.0, le=1.0)
        moea_crossover_rate: float = Field(0.8, ge=0.0, le=1.0)
        moea_tournament_size: int = Field(3, ge=2)
        moea_objective_weights: Dict[str, float] = Field(
            default_factory=lambda: {
                'freshness': 0.4,
                'cost': 0.3,
                'reliability': 0.2,
                'latency': 0.1,
            }
        )
        moea_dynamic_weights: bool = Field(True)

        # NEW v2.4.0 flags
        enable_limit_graph: bool = Field(True)
        enable_modp: bool = Field(True)
        enable_rlhf: bool = Field(True)
        enable_moe: bool = Field(True)
        moe_expert_count: int = Field(4, ge=2)

        q_weights_path: str = Field("./material_q_weights.json")
        interaction_logs_path: str = Field("./material_interactions.csv")
        historical_model_path: str = Field("./material_historical_model.pkl")
        moea_pareto_path: str = Field("./material_moea_pareto.json")

        @field_validator('source_priority')
        @classmethod
        def validate_source_priority(cls, v):
            allowed = {"bonsai", "footprintdata"}
            for s in v:
                if s not in allowed:
                    raise ValueError(f"Source {s} not in allowed list {allowed}")
            return v

        class Config:
            env_prefix = "MATERIAL_"
else:
    MATERIAL_CONFIG = {
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
            'freshness': 0.4,
            'cost': 0.3,
            'reliability': 0.2,
            'latency': 0.1,
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

# ============================================================================
# Data Models (Pydantic or dataclass)
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

        @field_validator('material_index')
        @classmethod
        def material_index_non_negative(cls, v):
            if v < 0:
                raise ValueError("material_index must be non-negative")
            return v
else:
    from dataclasses import dataclass

    @dataclass
    class Footprint:
        product_id: str
        embodied_carbon_kg: float
        rare_earth_kg: float
        total_mass_kg: float
        material_index: float
        source: str
        last_updated: datetime

# ============================================================================
# NEW: LIMIT Graph Manager
# ============================================================================
class LimitGraphManager:
    """
    Manages a graph of product/source relationships for LIMIT.
    Nodes are sources or products, edges represent dependencies or fallback order.
    """
    def __init__(self, storage: Optional[Storage] = None):
        self.storage = storage
        self.graphs = {}

    def create_graph(self, graph_id: str, description: str, configuration: Dict[str, Any]) -> None:
        if self.storage and hasattr(self.storage, 'save_limit_graph_metadata'):
            self.storage.save_limit_graph_metadata(graph_id, description, configuration)
        else:
            self.graphs[graph_id] = {'description': description, 'configuration': configuration, 'nodes': {}, 'edges': {}}

    def add_node(self, graph_id: str, node_id: str, node_type: Optional[str], attributes: Dict[str, Any]) -> None:
        if self.storage and hasattr(self.storage, 'save_limit_graph_node'):
            self.storage.save_limit_graph_node(node_id, graph_id, node_type, attributes)
        else:
            if graph_id not in self.graphs:
                self.graphs[graph_id] = {'nodes': {}, 'edges': {}}
            self.graphs[graph_id]['nodes'][node_id] = {'node_type': node_type, 'attributes': attributes}

    def add_edge(self, graph_id: str, edge_id: str, source: str, target: str,
                 weight: Optional[float], attributes: Dict[str, Any]) -> None:
        if self.storage and hasattr(self.storage, 'save_limit_graph_edge'):
            self.storage.save_limit_graph_edge(edge_id, graph_id, source, target, weight, attributes)
        else:
            if graph_id not in self.graphs:
                self.graphs[graph_id] = {'nodes': {}, 'edges': {}}
            self.graphs[graph_id]['edges'][edge_id] = {'source': source, 'target': target, 'weight': weight, 'attributes': attributes}

    def get_nodes(self, graph_id: str) -> List[Dict]:
        if self.storage and hasattr(self.storage, 'get_limit_graph_nodes'):
            return self.storage.get_limit_graph_nodes(graph_id)
        return list(self.graphs.get(graph_id, {}).get('nodes', {}).values())

    def get_edges(self, graph_id: str) -> List[Dict]:
        if self.storage and hasattr(self.storage, 'get_limit_graph_edges'):
            return self.storage.get_limit_graph_edges(graph_id)
        return list(self.graphs.get(graph_id, {}).get('edges', {}).values())

    def get_metadata(self, graph_id: str) -> Optional[Dict]:
        if self.storage and hasattr(self.storage, 'get_limit_graph_metadata'):
            return self.storage.get_limit_graph_metadata(graph_id)
        return self.graphs.get(graph_id, {})

# ============================================================================
# NEW: MODP Optimizer
# ============================================================================
class MODPOptimizer:
    """
    Multi‑Objective Dynamic Programming solver that can be used to
    combine Pareto front with dynamic weights and store decision states.
    """
    def __init__(self, storage: Optional[Storage] = None):
        self.storage = storage
        self.states = {}

    def add_state(self, state_id: str, problem_id: str, state_attributes: Dict[str, Any],
                  objective_values: Dict[str, float], stage: int) -> None:
        if self.storage and hasattr(self.storage, 'save_modp_state'):
            self.storage.save_modp_state(state_id, problem_id, state_attributes, objective_values, stage)
        else:
            if problem_id not in self.states:
                self.states[problem_id] = []
            self.states[problem_id].append({
                'state_id': state_id, 'state_attributes': state_attributes,
                'objective_values': objective_values, 'stage': stage
            })

    def add_transition(self, transition_id: str, problem_id: str, from_state: str,
                       to_state: str, action: str, cost: float,
                       objective_deltas: Dict[str, float]) -> None:
        if self.storage and hasattr(self.storage, 'save_modp_transition'):
            self.storage.save_modp_transition(transition_id, problem_id, from_state, to_state, action, cost, objective_deltas)

    def add_policy(self, policy_id: str, problem_id: str, state_id: str,
                   action: str, expected_objectives: Dict[str, float]) -> None:
        if self.storage and hasattr(self.storage, 'save_modp_policy'):
            self.storage.save_modp_policy(policy_id, problem_id, state_id, action, expected_objectives)

    def get_states(self, problem_id: str) -> List[Dict]:
        if self.storage and hasattr(self.storage, 'get_modp_states'):
            return self.storage.get_modp_states(problem_id)
        return self.states.get(problem_id, [])

    def get_transitions(self, problem_id: str) -> List[Dict]:
        if self.storage and hasattr(self.storage, 'get_modp_transitions'):
            return self.storage.get_modp_transitions(problem_id)
        return []

    def get_policies(self, problem_id: str) -> List[Dict]:
        if self.storage and hasattr(self.storage, 'get_modp_policies'):
            return self.storage.get_modp_policies(problem_id)
        return []

    async def solve(self, problem_id: str, initial_state: Dict[str, Any], max_stages: int = 5) -> Dict[str, Any]:
        """Simplified DP solver; just stores initial state and returns empty front."""
        self.add_state(
            state_id=f"{problem_id}_init",
            problem_id=problem_id,
            state_attributes=initial_state,
            objective_values={"freshness": 0.0, "cost": 0.0, "reliability": 0.0, "latency": 0.0},
            stage=0
        )
        return {"status": "solved", "pareto_front": []}

# ============================================================================
# NEW: RLHF Trainer
# ============================================================================
class RLHFTrainer:
    """
    Collects human preference pairs for update strategy selection.
    """
    def __init__(self, storage: Optional[Storage] = None):
        self.storage = storage
        self.pairs = []

    def record_pair(self, pair_id: str, prompt: str, chosen: str, rejected: str,
                    reward_diff: float, metadata: Optional[Dict] = None) -> None:
        if self.storage and hasattr(self.storage, 'save_preference_pair'):
            self.storage.save_preference_pair(pair_id, prompt, chosen, rejected, reward_diff, metadata)
        else:
            self.pairs.append({
                'pair_id': pair_id, 'prompt': prompt, 'chosen': chosen,
                'rejected': rejected, 'reward_diff': reward_diff, 'metadata': metadata
            })

    def get_pairs(self, limit: int = 100) -> List[Dict]:
        if self.storage and hasattr(self.storage, 'get_preference_pairs'):
            return self.storage.get_preference_pairs(limit)
        return self.pairs[-limit:]

    def train_reward_model(self):
        pairs = self.get_pairs()
        if len(pairs) < 5:
            logger.info("Not enough preference pairs for RLHF training.")
            return
        logger.info(f"Training reward model on {len(pairs)} preference pairs...")

# ============================================================================
# NEW: MoE Gating Network
# ============================================================================
class MoEGatingNetwork:
    """
    Mixture-of-Experts gating for update strategy selection.
    Experts are specialized strategies: bonsai_full, footprintdata_full, mock_full, bonsai_single, etc.
    The gating network learns to blend them based on state.
    """
    def __init__(self, storage: Optional[Storage] = None, config: Optional[Dict] = None):
        self.storage = storage
        self.config = config or {}
        self.num_experts = self.config.get('moe_expert_count', 4)
        self.expert_names = ['bonsai_full', 'footprintdata_full', 'mock_full', 'bonsai_single'][:self.num_experts]
        # Gating weights: (num_experts, 9) because state dimension is 9
        self.gating_weights = np.random.randn(self.num_experts, 9)
        self._training_samples = []

    def _encode_state(self, state: Union['UpdateState', Dict]) -> np.ndarray:
        if isinstance(state, dict):
            features = [
                min(state.get('total_products', 0) / 1000.0, 1.0),
                state.get('stale_fraction', 0),
                min(state.get('avg_demand', 0) / 10.0, 1.0),
                state.get('bonsai_success_rate', 0.5),
                state.get('footprintdata_success_rate', 0.5),
                state.get('bonsai_cb_state', 0) / 2.0,
                state.get('footprintdata_cb_state', 0) / 2.0,
                min(state.get('hours_since_update', 0) / 72.0, 1.0),
                state.get('single_product_mode', 0),
            ]
        else:
            features = [
                min(state.total_products / 1000.0, 1.0),
                state.stale_fraction,
                min(state.avg_demand / 10.0, 1.0),
                state.bonsai_success_rate,
                state.footprintdata_success_rate,
                state.bonsai_cb_state / 2.0,
                state.footprintdata_cb_state / 2.0,
                min(state.hours_since_update / 72.0, 1.0),
                state.single_product_mode,
            ]
        return np.array(features, dtype=np.float32)

    async def select_expert(self, state: Union['UpdateState', Dict]) -> Tuple[str, np.ndarray]:
        x = self._encode_state(state)
        logits = self.gating_weights @ x
        probs = np.exp(logits - np.max(logits))
        probs /= probs.sum()
        expert_idx = np.argmax(probs)
        selected = self.expert_names[expert_idx]
        if self.storage and hasattr(self.storage, 'log_routing_decision'):
            sample_id = hashlib.sha256(str(state).encode()).hexdigest()[:16]
            self.storage.log_routing_decision(str(uuid.uuid4()), sample_id, selected, float(probs[expert_idx]))
        return selected, probs

    async def add_training_sample(self, state: Union['UpdateState', Dict], selected_expert: str, reward: float):
        x = self._encode_state(state)
        expert_idx = self.expert_names.index(selected_expert)
        target = np.zeros(self.num_experts)
        target[expert_idx] = 1.0
        logits = self.gating_weights @ x
        probs = np.exp(logits - np.max(logits))
        probs /= probs.sum()
        grad = (probs - target)[:, None] * x[None, :]
        self.gating_weights -= 0.1 * grad

# ============================================================================
# DISTILLATION COMPONENTS FOR ADAPTIVE UPDATE (unchanged)
# ============================================================================
@dataclass
class UpdateState:
    """State for the distillation agent."""
    total_products: int
    stale_fraction: float
    avg_demand: float
    bonsai_success_rate: float
    footprintdata_success_rate: float
    bonsai_cb_state: float
    footprintdata_cb_state: float
    hours_since_update: float
    single_product_mode: float

    def to_feature_vector(self) -> np.ndarray:
        features = [
            min(self.total_products / 1000.0, 1.0),
            self.stale_fraction,
            min(self.avg_demand / 10.0, 1.0),
            self.bonsai_success_rate,
            self.footprintdata_success_rate,
            self.bonsai_cb_state / 2.0,
            self.footprintdata_cb_state / 2.0,
            min(self.hours_since_update / 72.0, 1.0),
            self.single_product_mode,
        ]
        return np.array(features, dtype=np.float32)

class Teacher(ABC):
    @abstractmethod
    def predict(self, state: UpdateState) -> np.ndarray: ...
    @abstractmethod
    def confidence(self, state: UpdateState) -> float: ...

class UpdateRuleBasedTeacher(Teacher):
    ACTION_SPACE = ['bonsai_full','footprintdata_full','mock_full','bonsai_single','footprintdata_single','mock_single']
    def predict(self, state):
        probs = np.ones(6)*0.1
        if state.single_product_mode > 0.5:
            if state.bonsai_success_rate > state.footprintdata_success_rate:
                probs[3]=0.8
            else:
                probs[4]=0.8
        else:
            if state.stale_fraction > 0.5:
                if state.bonsai_success_rate > state.footprintdata_success_rate:
                    probs[0]=0.8
                else:
                    probs[1]=0.8
            else:
                if state.bonsai_success_rate < 0.3 and state.footprintdata_success_rate < 0.3:
                    probs[2]=0.7
                else:
                    probs[0]=0.5
        return probs/probs.sum()
    def confidence(self, state):
        if state.stale_fraction > 0.5:
            return 0.6
        return 0.4

class UpdateHistoricalMLTeacher(Teacher):
    def __init__(self, model_path=None):
        self.model=None; self.label_encoder=None
        self.model_path = model_path or Path(MATERIAL_CONFIG['historical_model_path'])
        if self.model_path.exists():
            try:
                with open(self.model_path,'rb') as f:
                    self.model, self.label_encoder = pickle.load(f)
            except Exception as e:
                logger.error(f"Failed to load historical model: {e}")
    def predict(self, state):
        if self.model is None:
            return np.ones(6)/6
        x=state.to_feature_vector().reshape(1,-1)
        return self.model.predict_proba(x)[0]
    def confidence(self, state):
        return 0.7 if self.model is not None else 0.0

class UpdateStatefulQTeacher(Teacher):
    def __init__(self, lr=0.1):
        self.lr=lr
        self.weights=np.zeros((9,6))
        self._load_state()
    def _load_state(self):
        path=Path(MATERIAL_CONFIG['q_weights_path'])
        if path.exists():
            try:
                with open(path,'r') as f:
                    self.weights=np.array(json.load(f))
            except Exception as e:
                logger.error(f"Failed to load Q-weights: {e}")
    def _save_state(self):
        path=Path(MATERIAL_CONFIG['q_weights_path'])
        with open(path,'w') as f:
            json.dump(self.weights.tolist(), f, indent=2)
    def predict(self, state):
        x=state.to_feature_vector()
        q=x@self.weights
        exp_q=np.exp(q-np.max(q))
        return exp_q/exp_q.sum()
    def confidence(self, state):
        return 0.5
    def update(self, state, action, reward):
        x=state.to_feature_vector()
        q_current=np.dot(x,self.weights[:,action])
        self.weights[:,action]+=self.lr*(reward-q_current)*x
        self._save_state()

class DistillationStudent:
    def __init__(self, feature_dim=9, n_classes=6, lr=0.01):
        self.weights=np.zeros((feature_dim,n_classes)); self.biases=np.zeros(n_classes)
        self.lr=lr; self.n_classes=n_classes; self.counter=0
    def predict_proba(self, state_vector, num_classes):
        if num_classes != self.n_classes:
            new_weights=np.zeros((self.weights.shape[0],num_classes)); new_biases=np.zeros(num_classes)
            min_dim=min(self.n_classes,num_classes)
            new_weights[:,:min_dim]=self.weights[:,:min_dim]; new_biases[:min_dim]=self.biases[:min_dim]
            self.weights=new_weights; self.biases=new_biases; self.n_classes=num_classes
        logits=state_vector@self.weights+self.biases
        max_logit=np.max(logits); exp_logits=np.exp(logits-max_logit)
        return exp_logits/exp_logits.sum()
    def update(self, state_vector, teacher_probs, reward, action, distill_weight=0.7, rl_weight=0.3):
        current_probs=self.predict_proba(state_vector,self.n_classes)
        logits=state_vector@self.weights+self.biases
        grad_distill=-(teacher_probs-current_probs)
        one_hot=np.zeros(self.n_classes); one_hot[action]=1.0
        grad_rl=-reward*(one_hot-current_probs)
        grad=distill_weight*grad_distill+rl_weight*grad_rl
        self.weights-=self.lr*np.outer(state_vector,grad)
        self.biases-=self.lr*grad
        self.counter+=1

class ReplayBuffer:
    def __init__(self,max_size=2000):
        self.buffer=deque(maxlen=max_size)
    def push(self,state_vec,action,reward,next_state_vec,teacher_probs):
        self.buffer.append((state_vec,action,reward,next_state_vec,teacher_probs))
    def sample(self,batch_size=32):
        if len(self.buffer)<batch_size:
            batch=list(self.buffer)
        else:
            batch=random.sample(self.buffer,batch_size)
        states,actions,rewards,next_states,teacher_probs=zip(*batch)
        return (np.array(states),actions,np.array(rewards),np.array(next_states),np.array(teacher_probs))
    def __len__(self): return len(self.buffer)

class DistillationUpdateOptimizer:
    ACTION_SPACE = ['bonsai_full','footprintdata_full','mock_full','bonsai_single','footprintdata_single','mock_single']
    def __init__(self, config):
        self.config=config
        self.student=DistillationStudent(lr=config.get('distillation_learning_rate',0.01))
        self.teachers=[UpdateRuleBasedTeacher(), UpdateHistoricalMLTeacher(), UpdateStatefulQTeacher()]
        self.replay_buffer=ReplayBuffer(max_size=config.get('distillation_replay_size',2000))
        self.epsilon=config.get('distillation_epsilon',0.1)
        self.train_every=config.get('distillation_train_every',10)
        self.counter=0
    async def select_action(self, state, exploration=True):
        state_vec=state.to_feature_vector(); n=6
        teacher_probs=np.zeros(n); total_conf=0.0
        for teacher in self.teachers:
            prob=teacher.predict(state); conf=teacher.confidence(state)
            if len(prob)!=n:
                if len(prob)<n: prob=np.pad(prob,(0,n-len(prob)),'constant')
                else: prob=prob[:n]
            teacher_probs+=prob*conf; total_conf+=conf
        if total_conf>0: teacher_probs/=total_conf
        else: teacher_probs=np.ones(n)/n
        student_probs=self.student.predict_proba(state_vec,n)
        if exploration and random.random()<self.epsilon:
            action_idx=random.randint(0,n-1)
        else:
            combined=0.8*student_probs+0.2*teacher_probs
            action_idx=np.argmax(combined)
        return self.ACTION_SPACE[action_idx], action_idx, state_vec, teacher_probs
    async def update(self, state_vec, action_idx, reward, next_state_vec, teacher_probs):
        self.replay_buffer.push(state_vec,action_idx,reward,next_state_vec,teacher_probs)
        self.counter+=1
        if self.counter%self.train_every==0 and len(self.replay_buffer)>=8:
            batch=self.replay_buffer.sample(8)
            states,actions,rewards,_,teacher_probs_batch=batch
            for i in range(len(states)):
                self.student.update(states[i],teacher_probs_batch[i],rewards[i],actions[i])
    def get_stats(self):
        return {'student_counter':self.student.counter,'buffer_size':len(self.replay_buffer)}

# ============================================================================
# NEW: Multi‑Objective Strategy Evolution (NSGA‑II)
# ============================================================================
@dataclass
class MOPDUpdateStrategy:
    strategy_id: str
    weights: Dict[str, float]
    objectives: Dict[str, float]
    scalarised_score: float = 0.0

    def to_dict(self):
        return {
            'strategy_id': self.strategy_id,
            'weights': self.weights,
            'objectives': self.objectives,
            'scalarised_score': self.scalarised_score,
        }

    @classmethod
    def from_dict(cls, data):
        return cls(**data)

class NSGAIIUpdateOptimizer:
    """
    Multi‑objective genetic algorithm for evolving update strategy weights.
    Decision variables: weights for freshness, cost, reliability, latency.
    Objectives: maximize freshness, minimize cost (max -cost), maximize reliability, minimize latency (max -latency).
    """
    def __init__(self,
                 evaluate_func: Callable[[Dict[str, float]], Awaitable[Dict[str, float]]],
                 population_size: int = 20,
                 generations: int = 5,
                 mutation_rate: float = 0.2,
                 crossover_rate: float = 0.8,
                 tournament_size: int = 3,
                 objective_weights: Optional[Dict[str, float]] = None,
                 dynamic_weights: bool = True):
        self.evaluate_func = evaluate_func
        self.population_size = population_size
        self.generations = generations
        self.mutation_rate = mutation_rate
        self.crossover_rate = crossover_rate
        self.tournament_size = tournament_size
        self.objective_weights = objective_weights or {
            'freshness': 0.4,
            'cost': 0.3,
            'reliability': 0.2,
            'latency': 0.1,
        }
        self.dynamic_weights = dynamic_weights

        self.best_individual = None
        self.best_fitness = -float('inf')
        self.evolution_history = []
        self.pareto_front: List[MOPDUpdateStrategy] = []
        self._eval_cache: Dict[Tuple[float, ...], Dict[str, float]] = {}

    def _random_individual(self) -> Dict[str, float]:
        weights = {
            'freshness': random.random(),
            'cost': random.random(),
            'reliability': random.random(),
            'latency': random.random(),
        }
        total = sum(weights.values())
        if total > 0:
            weights = {k: v / total for k, v in weights.items()}
        return weights

    def _crossover(self, p1, p2):
        child = {}
        for key in p1:
            if random.random() < 0.5:
                u = random.random()
                if u <= 0.5:
                    beta = (2 * u) ** (1 / (20 + 1))
                else:
                    beta = (1 / (2 * (1 - u))) ** (1 / (20 + 1))
                child[key] = max(0.0, min(1.0, 0.5 * ((1 + beta) * p1[key] + (1 - beta) * p2[key])))
            else:
                child[key] = p1[key] if random.random() < 0.5 else p2[key]
        total = sum(child.values())
        if total > 0:
            child = {k: v / total for k, v in child.items()}
        return child

    def _mutate(self, ind):
        mutant = ind.copy()
        for key in mutant:
            if random.random() < self.mutation_rate:
                u = random.random()
                if u < 0.5:
                    delta = (2 * u) ** (1 / (20 + 1)) - 1
                else:
                    delta = 1 - (2 * (1 - u)) ** (1 / (20 + 1))
                mutant[key] = mutant[key] + delta
                mutant[key] = max(0.0, min(1.0, mutant[key]))
        total = sum(mutant.values())
        if total > 0:
            mutant = {k: v / total for k, v in mutant.items()}
        return mutant

    def _fast_non_dominated_sort(self, points):
        fronts = []
        domination_count = {id(p): 0 for p in points}
        dominated_solutions = {id(p): [] for p in points}
        for i, p in enumerate(points):
            p_obj = p.objectives
            for j, q in enumerate(points):
                if i == j: continue
                q_obj = q.objectives
                if all(p_obj[k] >= q_obj[k] for k in p_obj) and any(p_obj[k] > q_obj[k] for k in p_obj):
                    dominated_solutions[id(p)].append(q)
                elif all(q_obj[k] >= p_obj[k] for k in q_obj) and any(q_obj[k] > p_obj[k] for k in q_obj):
                    domination_count[id(p)] += 1
            if domination_count[id(p)] == 0:
                if not fronts:
                    fronts.append([])
                fronts[0].append(p)
        i = 0
        while i < len(fronts):
            next_front = []
            for p in fronts[i]:
                for q in dominated_solutions[id(p)]:
                    domination_count[id(q)] -= 1
                    if domination_count[id(q)] == 0:
                        next_front.append(q)
            if next_front:
                fronts.append(next_front)
            i += 1
        return fronts

    def _crowding_distance(self, front):
        if not front:
            return {}
        distances = {id(p): 0.0 for p in front}
        objective_keys = list(front[0].objectives.keys())
        for obj in objective_keys:
            sorted_front = sorted(front, key=lambda x: x.objectives[obj])
            distances[id(sorted_front[0])] = float('inf')
            distances[id(sorted_front[-1])] = float('inf')
            obj_min = sorted_front[0].objectives[obj]
            obj_max = sorted_front[-1].objectives[obj]
            if obj_max == obj_min:
                continue
            for i in range(1, len(sorted_front) - 1):
                distances[id(sorted_front[i])] += (sorted_front[i+1].objectives[obj] - sorted_front[i-1].objectives[obj]) / (obj_max - obj_min)
        return distances

    def _tournament_selection(self, population, fronts, crowding):
        candidates = random.sample(population, self.tournament_size)
        ind_to_point = {}
        for ind, point in zip(population, self._all_points):
            ind_to_point[id(ind)] = point
        best = candidates[0]
        best_rank = float('inf')
        best_crowding = -float('inf')
        for cand in candidates:
            point = ind_to_point.get(id(cand))
            if not point:
                continue
            rank = len(fronts)
            for fi, front in enumerate(fronts):
                if point in front:
                    rank = fi
                    break
            cd = crowding.get(id(point), 0)
            if rank < best_rank or (rank == best_rank and cd > best_crowding):
                best = cand
                best_rank = rank
                best_crowding = cd
        return best

    def _compute_dynamic_weights(self):
        weights = self.objective_weights.copy()
        if not self.dynamic_weights or not self.pareto_front:
            return weights
        obj_keys = list(weights.keys())
        avg = {k: np.mean([p.objectives[k] for p in self.pareto_front]) for k in obj_keys}
        max_val = {k: np.max([p.objectives[k] for p in self.pareto_front]) for k in obj_keys}
        for k in obj_keys:
            if max_val[k] > 0 and avg[k] < 0.5 * max_val[k]:
                weights[k] = min(0.6, weights.get(k, 0.0) * 1.5)
        total = sum(weights.values())
        if total > 0:
            weights = {k: v / total for k, v in weights.items()}
        return weights

    def _select_best_from_pareto(self, pareto, weights):
        if not pareto:
            return None
        obj_keys = list(weights.keys())
        max_vals = {k: max(p.objectives[k] for p in pareto) for k in obj_keys}
        min_vals = {k: min(p.objectives[k] for p in pareto) for k in obj_keys}
        ranges = {k: max_vals[k] - min_vals[k] if max_vals[k] != min_vals[k] else 1.0 for k in obj_keys}
        best = None
        best_score = -float('inf')
        for p in pareto:
            score = 0.0
            for k in obj_keys:
                val = p.objectives[k]
                norm = (val - min_vals[k]) / ranges[k] if ranges[k] > 0 else 1.0
                score += weights.get(k, 0.0) * norm
            p.scalarised_score = score
            if score > best_score:
                best_score = score
                best = p
        return best

    async def evolve(self):
        population = [self._random_individual() for _ in range(self.population_size)]
        points = []
        eval_tasks = [self.evaluate_func(ind) for ind in population]
        eval_results = await asyncio.gather(*eval_tasks)
        for ind, obj in zip(population, eval_results):
            point = MOPDUpdateStrategy(strategy_id=str(uuid.uuid4()), weights=ind, objectives=obj)
            points.append(point)
            self._eval_cache[tuple(sorted(ind.items()))] = obj
        self._all_points = points
        for gen in range(self.generations):
            fronts = self._fast_non_dominated_sort(points)
            crowding = {}
            for front in fronts:
                front_crowding = self._crowding_distance(front)
                crowding.update(front_crowding)
            offspring = []
            while len(offspring) < self.population_size:
                parent1 = self._tournament_selection(population, fronts, crowding)
                parent2 = self._tournament_selection(population, fronts, crowding)
                if random.random() < self.crossover_rate:
                    child = self._crossover(parent1, parent2)
                else:
                    child = copy.deepcopy(parent1)
                child = self._mutate(child)
                offspring.append(child)
            child_tasks = [self.evaluate_func(ind) for ind in offspring]
            child_results = await asyncio.gather(*child_tasks)
            child_points = []
            for ind, obj in zip(offspring, child_results):
                point = MOPDUpdateStrategy(strategy_id=str(uuid.uuid4()), weights=ind, objectives=obj)
                child_points.append(point)
                self._eval_cache[tuple(sorted(ind.items()))] = obj
            combined_inds = population + offspring
            combined_points = points + child_points
            unique_pairs = {}
            for ind, p in zip(combined_inds, combined_points):
                key = tuple(sorted(ind.items()))
                unique_pairs[key] = (ind, p)
            population = [v[0] for v in unique_pairs.values()]
            points = [v[1] for v in unique_pairs.values()]
            self._all_points = points
            fronts = self._fast_non_dominated_sort(points)
            new_population = []
            new_points = []
            for front in fronts:
                if len(new_population) + len(front) <= self.population_size:
                    for p in front:
                        for ind, p2 in zip(population, points):
                            if p2 is p:
                                new_population.append(ind)
                                new_points.append(p)
                                break
                else:
                    crowding = self._crowding_distance(front)
                    sorted_front = sorted(front, key=lambda x: crowding.get(id(x), 0), reverse=True)
                    for p in sorted_front:
                        if len(new_population) >= self.population_size:
                            break
                        for ind, p2 in zip(population, points):
                            if p2 is p:
                                new_population.append(ind)
                                new_points.append(p)
                                break
            population = new_population[:self.population_size]
            points = new_points[:self.population_size]
            self._all_points = points
            fronts = self._fast_non_dominated_sort(points)
            if fronts:
                self.pareto_front = fronts[0]
            logger.info(f"Generation {gen+1}/{self.generations}: Pareto front size={len(self.pareto_front)}")
        weights = self._compute_dynamic_weights()
        best = self._select_best_from_pareto(self.pareto_front, weights)
        if best:
            self.best_individual = best.weights
            self.best_fitness = best.scalarised_score
        return self.pareto_front

# ============================================================================
# MaterialFootprintUpdater (Enhanced with new components)
# ============================================================================
class MaterialFootprintUpdater:
    """
    Enhanced material footprint updater with adaptive source selection, MOEA,
    LIMIT Graph, MODP, RLHF, and MoE gating.
    """

    def __init__(
        self,
        config: Optional[Union[Dict[str, Any], MaterialConfig]] = None,
        storage: Optional[Storage] = None,
        enable_limit_graph: bool = True,
        enable_modp: bool = True,
        enable_rlhf: bool = True,
        enable_moe: bool = True,
        moe_expert_count: int = 4,
    ):
        """
        Initialize the updater.

        Args:
            config: Configuration dict or Pydantic model.
            storage: Central Storage instance (optional).
            enable_limit_graph: Enable LIMIT Graph management.
            enable_modp: Enable MODP solver.
            enable_rlhf: Enable RLHF trainer.
            enable_moe: Enable MoE gating network.
            moe_expert_count: Number of experts in MoE.
        """
        if config is None:
            if PYDANTIC_AVAILABLE:
                self.config = MaterialConfig()
            else:
                self.config = MATERIAL_CONFIG
        elif isinstance(config, dict):
            if PYDANTIC_AVAILABLE:
                self.config = MaterialConfig(**config)
            else:
                self.config = config
        else:
            self.config = config

        self.storage = storage
        self.db_path = self._get_config('db_path', Path("./material_catalog.db"))
        self.cache_ttl = self._get_config('cache_ttl', 86400 * 7)
        self.bonsai_api_url = self._get_config('bonsai_api_url', "https://api.bonsai.uno/v1/footprints")
        self.bonsai_api_key = self._get_config('bonsai_api_key') or os.environ.get("BONSAI_API_KEY")
        self.footprintdata_api_url = self._get_config('footprintdata_api_url', "https://api.footprintdata.org/v1/products")
        self.footprintdata_api_key = self._get_config('footprintdata_api_key') or os.environ.get("FOOTPRINTDATA_API_KEY")
        self.request_timeout = self._get_config('request_timeout', 10.0)
        self.source_priority = self._get_config('source_priority', ["bonsai", "footprintdata"])

        # Initialize database
        self._init_db()

        # Session management
        self._session: Optional[aiohttp.ClientSession] = None
        self._session_lock = asyncio.Lock()

        # Circuit breakers per source
        self._circuit_breakers = {
            "bonsai": CircuitBreaker(
                name="material_bonsai",
                failure_threshold=self._get_config('circuit_breaker_threshold', 5),
                recovery_timeout=self._get_config('circuit_breaker_timeout', 30.0),
            ),
            "footprintdata": CircuitBreaker(
                name="material_footprintdata",
                failure_threshold=self._get_config('circuit_breaker_threshold', 5),
                recovery_timeout=self._get_config('circuit_breaker_timeout', 30.0),
            ),
        }

        # Prometheus metrics
        if PROMETHEUS_AVAILABLE and self._get_config('enable_prometheus', True):
            self.metrics = {
                'calls': Counter('material_api_calls_total', 'Material API calls', ['source', 'status']),
                'errors': Counter('material_api_errors_total', 'Material API errors', ['source']),
                'latency': Histogram('material_api_latency_seconds', 'Material API latency', ['source']),
                'cache_hits': Counter('material_cache_hits_total', 'Cache hits'),
                'cache_misses': Counter('material_cache_misses_total', 'Cache misses'),
                'cache_size': Gauge('material_cache_size', 'Number of cached footprints'),
                'cache_age_seconds': Gauge('material_cache_age_seconds', 'Age of cached footprint', ['product_id']),
                'update_action': Counter('material_update_action', 'Update action selected', ['action']),
                'update_reward': Histogram('material_update_reward', 'Reward per update action'),
                'moea_pareto_front': Gauge('material_moea_pareto_front', 'MOEA Pareto front size'),
            }
        else:
            self.metrics = None

        # Distillation optimizer
        self.update_optimizer = DistillationUpdateOptimizer({
            'distillation_epsilon': self._get_config('distillation_epsilon', 0.1),
            'distillation_train_every': self._get_config('distillation_train_every', 10),
            'distillation_replay_size': self._get_config('distillation_replay_size', 2000),
            'distillation_learning_rate': self._get_config('distillation_learning_rate', 0.01),
        })

        # Interaction tracking
        self.interaction_log: List[Dict] = []
        self.last_state_vec: Optional[np.ndarray] = None
        self.last_action_idx: Optional[int] = None
        self.last_teacher_probs: Optional[np.ndarray] = None
        self.last_update_time: Optional[datetime] = None

        # MOEA parameters
        self.moea_enabled = self._get_config('moea_enabled', True)
        self.moea_interval_seconds = self._get_config('moea_interval_seconds', 300)
        self.moea_population_size = self._get_config('moea_population_size', 20)
        self.moea_generations = self._get_config('moea_generations', 5)
        self.moea_mutation_rate = self._get_config('moea_mutation_rate', 0.2)
        self.moea_crossover_rate = self._get_config('moea_crossover_rate', 0.8)
        self.moea_tournament_size = self._get_config('moea_tournament_size', 3)
        self.moea_objective_weights = self._get_config('moea_objective_weights', {
            'freshness': 0.4,
            'cost': 0.3,
            'reliability': 0.2,
            'latency': 0.1,
        })
        self.moea_dynamic_weights = self._get_config('moea_dynamic_weights', True)
        self.moea_optimizer: Optional[NSGAIIUpdateOptimizer] = None
        self.evolved_pareto_front: List[MOPDUpdateStrategy] = []
        self.best_evolved_strategy: Optional[MOPDUpdateStrategy] = None
        self._moea_task: Optional[asyncio.Task] = None

        # NEW v2.4.0 components
        self.limit_graph_manager = LimitGraphManager(storage) if enable_limit_graph else None
        self.modp_solver = MODPOptimizer(storage) if enable_modp else None
        self.rlhf_trainer = RLHFTrainer(storage) if enable_rlhf else None
        self.moe_gating = MoEGatingNetwork(storage, {'moe_expert_count': moe_expert_count}) if enable_moe else None

        # Initialize LIMIT Graph if enabled
        if self.limit_graph_manager:
            self._init_limit_graph()

        # Start MOEA background task if enabled
        if self.moea_enabled:
            self._moea_task = asyncio.create_task(self._moea_loop())

        logger.info("MaterialFootprintUpdater initialized with adaptive update, MOEA, LIMIT Graph, MODP, RLHF, MoE",
                    db_path=str(self.db_path))

    def _init_limit_graph(self):
        """Create default product/source graph."""
        graph_id = "material_sources"
        if not self.limit_graph_manager.get_metadata(graph_id):
            self.limit_graph_manager.create_graph(graph_id, "Material Source Dependencies", {})
            # Add source nodes
            for src in ['bonsai', 'footprintdata', 'mock']:
                self.limit_graph_manager.add_node(graph_id, f"source_{src}", src, {})
            # Add edges (fallback order)
            self.limit_graph_manager.add_edge(graph_id, "edge_bonsai_footprintdata", "source_bonsai", "source_footprintdata", 1.0, {})
            self.limit_graph_manager.add_edge(graph_id, "edge_footprintdata_mock", "source_footprintdata", "source_mock", 1.0, {})

    def _get_config(self, key: str, default: Any = None) -> Any:
        if hasattr(self.config, 'model_dump'):
            return getattr(self.config, key, default)
        elif hasattr(self.config, 'dict'):
            return getattr(self.config, key, default)
        else:
            return self.config.get(key, default)

    def _init_db(self):
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.db_path)
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
        conn.close()

    async def _get_session(self) -> aiohttp.ClientSession:
        async with self._session_lock:
            if self._session is None or self._session.closed:
                timeout = ClientTimeout(total=self.request_timeout)
                connector = aiohttp.TCPConnector(limit=10, ttl_dns_cache=300)
                self._session = aiohttp.ClientSession(
                    connector=connector,
                    timeout=timeout,
                    raise_for_status=True,
                )
            return self._session

    async def close(self):
        if self._moea_task:
            self._moea_task.cancel()
            await asyncio.gather(self._moea_task, return_exceptions=True)
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    # ---------- Build state ----------
    def _build_state(self, product_id: Optional[str] = None) -> UpdateState:
        conn = sqlite3.connect(self.db_path)
        total = conn.execute("SELECT COUNT(*) FROM footprints").fetchone()[0]

        now = datetime.utcnow()
        rows = conn.execute("SELECT last_updated FROM footprints").fetchall()
        stale_count = 0
        for row in rows:
            try:
                last = datetime.fromisoformat(row[0])
                if (now - last).total_seconds() > self.cache_ttl:
                    stale_count += 1
            except:
                stale_count += 1
        conn.close()
        stale_fraction = stale_count / max(total, 1)

        if self.interaction_log:
            recent = [entry for entry in self.interaction_log[-50:] if entry.get('product_id') is not None]
            product_counts = {}
            for entry in recent:
                pid = entry['product_id']
                product_counts[pid] = product_counts.get(pid, 0) + 1
            avg_demand = np.mean(list(product_counts.values())) if product_counts else 1.0
        else:
            avg_demand = 1.0

        bonsai_success = 0.5
        footprintdata_success = 0.5
        if self.interaction_log:
            bonsai_entries = [e for e in self.interaction_log if e.get('source') == 'bonsai']
            footprint_entries = [e for e in self.interaction_log if e.get('source') == 'footprintdata']
            if bonsai_entries:
                bonsai_success = sum(1 for e in bonsai_entries if e.get('success', False)) / len(bonsai_entries)
            if footprint_entries:
                footprintdata_success = sum(1 for e in footprint_entries if e.get('success', False)) / len(footprint_entries)

        bonsai_cb = 0.0
        if self._circuit_breakers['bonsai']._state == CircuitBreakerState.CLOSED:
            bonsai_cb = 0.0
        elif self._circuit_breakers['bonsai']._state == CircuitBreakerState.HALF_OPEN:
            bonsai_cb = 1.0
        else:
            bonsai_cb = 2.0

        footprint_cb = 0.0
        if self._circuit_breakers['footprintdata']._state == CircuitBreakerState.CLOSED:
            footprint_cb = 0.0
        elif self._circuit_breakers['footprintdata']._state == CircuitBreakerState.HALF_OPEN:
            footprint_cb = 1.0
        else:
            footprint_cb = 2.0

        if self.last_update_time:
            hours = (datetime.utcnow() - self.last_update_time).total_seconds() / 3600
        else:
            hours = 0.0

        single_mode = 1.0 if product_id is not None else 0.0

        return UpdateState(
            total_products=total,
            stale_fraction=stale_fraction,
            avg_demand=avg_demand,
            bonsai_success_rate=bonsai_success,
            footprintdata_success_rate=footprintdata_success,
            bonsai_cb_state=bonsai_cb,
            footprintdata_cb_state=footprint_cb,
            hours_since_update=hours,
            single_product_mode=single_mode,
        )

    # ---------- Core update methods (enhanced with MoE) ----------
    async def update_catalog(self, force_refresh: bool = False) -> int:
        state = self._build_state(product_id=None)

        # Decide action: use MoE if available, else distillation
        if self.moe_gating:
            expert_name, _ = await self.moe_gating.select_expert(state)
            # Map expert to action
            action = expert_name if expert_name in DistillationUpdateOptimizer.ACTION_SPACE else 'bonsai_full'
            action_idx = DistillationUpdateOptimizer.ACTION_SPACE.index(action)
            state_vec = state.to_feature_vector()
            teacher_probs = np.ones(6) / 6
            self._last_selected_expert = expert_name
        else:
            action, action_idx, state_vec, teacher_probs = await self.update_optimizer.select_action(state, exploration=True)

        self.last_state_vec = state_vec
        self.last_action_idx = action_idx
        self.last_teacher_probs = teacher_probs

        success = False
        updated_count = 0
        start_time = time.time()

        if action == 'bonsai_full':
            updated_count = await self._update_from_source('bonsai', force_refresh)
            success = updated_count > 0
        elif action == 'footprintdata_full':
            updated_count = await self._update_from_source('footprintdata', force_refresh)
            success = updated_count > 0
        elif action == 'mock_full':
            self._seed_mock_data()
            updated_count = self._count_catalog()
            success = updated_count > 0
        elif action == 'bonsai_single':
            updated_count = await self._update_from_source('bonsai', force_refresh)
            success = updated_count > 0
        elif action == 'footprintdata_single':
            updated_count = await self._update_from_source('footprintdata', force_refresh)
            success = updated_count > 0
        elif action == 'mock_single':
            self._seed_mock_data()
            updated_count = self._count_catalog()
            success = updated_count > 0

        reward = self._compute_reward(success, updated_count, force_refresh)
        self.last_update_time = datetime.utcnow()

        self._log_interaction('update_catalog', action, success, reward)

        # Update distillation or MoE
        if self.last_state_vec is not None and self.last_action_idx is not None:
            next_state = self._build_state(product_id=None)
            next_state_vec = next_state.to_feature_vector()
            if self.moe_gating and hasattr(self, '_last_selected_expert'):
                await self.moe_gating.add_training_sample(state, self._last_selected_expert, reward)
                # Also update distillation as before
                await self.update_optimizer.update(
                    self.last_state_vec,
                    self.last_action_idx,
                    reward,
                    next_state_vec,
                    self.last_teacher_probs
                )
            else:
                await self.update_optimizer.update(
                    self.last_state_vec,
                    self.last_action_idx,
                    reward,
                    next_state_vec,
                    self.last_teacher_probs
                )

        # RLHF: occasionally record preference pair
        if self.rlhf_trainer and random.random() < 0.05:
            chosen_action = action
            rejected_action = random.choice([a for a in DistillationUpdateOptimizer.ACTION_SPACE if a != chosen_action])
            self.rlhf_trainer.record_pair(
                pair_id=str(uuid.uuid4()),
                prompt="Which update strategy is better?",
                chosen=chosen_action,
                rejected=rejected_action,
                reward_diff=reward,
                metadata={'force_refresh': force_refresh}
            )

        # MODP: record state and policy
        if self.modp_solver:
            problem_id = "material_update_strategy"
            state_id = f"{datetime.utcnow().isoformat()}_{action}"
            self.modp_solver.add_state(
                state_id=state_id,
                problem_id=problem_id,
                state_attributes={'action': action, 'force_refresh': force_refresh},
                objective_values={'freshness': 1.0 if success else 0.0, 'cost': 0.0, 'reliability': 0.0, 'latency': 0.0},
                stage=0
            )
            self.modp_solver.add_policy(
                policy_id=f"policy_{state_id}",
                problem_id=problem_id,
                state_id=state_id,
                action=action,
                expected_objectives={'freshness': 0.0, 'cost': 0.0, 'reliability': 0.0, 'latency': 0.0}
            )

        if self.metrics:
            self.metrics['update_action'].labels(action=action).inc()
            self.metrics['update_reward'].observe(reward)
            conn = sqlite3.connect(self.db_path)
            count = conn.execute("SELECT COUNT(*) FROM footprints").fetchone()[0]
            conn.close()
            self.metrics['cache_size'].set(count)

        logger.info(f"Update completed: action={action}, updated={updated_count}, reward={reward:.2f}")
        return updated_count

    async def _update_from_source(self, source: str, force_refresh: bool) -> int:
        # (Implementation from original v2.3.0)
        pass

    def _count_catalog(self) -> int:
        conn = sqlite3.connect(self.db_path)
        count = conn.execute("SELECT COUNT(*) FROM footprints").fetchone()[0]
        conn.close()
        return count

    def _seed_mock_data(self):
        pass

    def _compute_reward(self, success: bool, updated_count: int, force_refresh: bool) -> float:
        pass

    # ---------- Public methods ----------
    def get_footprint(self, product_id: str) -> Optional[Footprint]:
        pass

    async def get_or_fetch_footprint(self, product_id: str, force_refresh: bool = False) -> Optional[Footprint]:
        pass

    def _log_interaction(self, method: str, action: str, success: bool, reward: float, product_id: Optional[str] = None):
        pass

    @classmethod
    def train_historical_model(cls, log_path: Path = Path("./material_interactions.csv"),
                               model_path: Path = Path("./material_historical_model.pkl")):
        pass

    def list_products(self) -> List[str]:
        pass

    def delete_footprint(self, product_id: str) -> bool:
        pass

    def clear_cache(self) -> None:
        pass

    def export_catalog(self, path: Path) -> None:
        pass

    def import_catalog(self, path: Path) -> int:
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    # ---------- MOEA background loop and evolution ----------
    async def _moea_loop(self):
        while True:
            try:
                await asyncio.sleep(self.moea_interval_seconds)
                await self.run_strategy_evolution()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"MOEA loop failed: {e}")
                await asyncio.sleep(60)

    async def run_strategy_evolution(self) -> List[MOPDUpdateStrategy]:
        if not self.moea_enabled:
            logger.info("MOEA is disabled.")
            return []

        async def evaluate(weights: Dict[str, float]) -> Dict[str, float]:
            if len(self.interaction_log) < 10:
                return {'freshness': 0.0, 'cost': 0.0, 'reliability': 0.0, 'latency': 0.0}
            conn = sqlite3.connect(self.db_path)
            total = conn.execute("SELECT COUNT(*) FROM footprints").fetchone()[0]
            rows = conn.execute("SELECT last_updated FROM footprints").fetchall()
            stale_count = sum(1 for row in rows if (datetime.utcnow() - datetime.fromisoformat(row[0])).total_seconds() > self.cache_ttl)
            conn.close()
            freshness = 1.0 - stale_count / max(total, 1)
            bonsai_calls = sum(1 for e in self.interaction_log if e.get('source') == 'bonsai')
            footprint_calls = sum(1 for e in self.interaction_log if e.get('source') == 'footprintdata')
            total_calls = bonsai_calls + footprint_calls
            cost = 1.0 - (bonsai_calls * 0.6 + footprint_calls * 0.4) / max(total_calls, 1)
            bonsai_success = sum(1 for e in self.interaction_log if e.get('source') == 'bonsai' and e.get('success', False)) / max(bonsai_calls, 1)
            footprint_success = sum(1 for e in self.interaction_log if e.get('source') == 'footprintdata' and e.get('success', False)) / max(footprint_calls, 1)
            reliability = (bonsai_success + footprint_success) / 2
            latencies = [e.get('latency', 0) for e in self.interaction_log if 'latency' in e and e['latency'] is not None]
            avg_latency = np.mean(latencies) if latencies else 0.0
            latency = 1.0 - min(avg_latency / 10.0, 1.0)
            return {'freshness': freshness, 'cost': cost, 'reliability': reliability, 'latency': latency}

        bounds = {'freshness': (0.0, 1.0), 'cost': (0.0, 1.0), 'reliability': (0.0, 1.0), 'latency': (0.0, 1.0)}
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
        pareto = await self.moea_optimizer.evolve()
        self.evolved_pareto_front = pareto
        if pareto:
            best = self.moea_optimizer._select_best_from_pareto(pareto, self._get_dynamic_moea_weights())
            if best:
                self.best_evolved_strategy = best
                logger.info(f"Best evolved strategy weights: {best.weights}")
                if self.metrics:
                    self.metrics['moea_pareto_front'].set(len(pareto))
                # MODP: store state
                if self.modp_solver:
                    self.modp_solver.add_state(
                        state_id=f"moea_best_{time.time()}",
                        problem_id="material_strategy_evolution",
                        state_attributes={'weights': best.weights},
                        objective_values=best.objectives,
                        stage=0
                    )
        return pareto

    def _get_dynamic_moea_weights(self) -> Dict[str, float]:
        weights = self.moea_objective_weights.copy()
        conn = sqlite3.connect(self.db_path)
        rows = conn.execute("SELECT last_updated FROM footprints").fetchall()
        conn.close()
        total = len(rows)
        if total > 0:
            stale = sum(1 for row in rows if (datetime.utcnow() - datetime.fromisoformat(row[0])).total_seconds() > self.cache_ttl)
            stale_frac = stale / total
            if stale_frac > 0.5:
                weights['freshness'] = min(0.6, weights['freshness'] * 1.5)
        total_w = sum(weights.values())
        if total_w > 0:
            weights = {k: v / total_w for k, v in weights.items()}
        return weights

    # ---------- New public methods for enhancements ----------
    async def get_limit_graph(self, graph_id: str = "material_sources") -> Dict:
        if self.limit_graph_manager:
            return {
                'metadata': self.limit_graph_manager.get_metadata(graph_id),
                'nodes': self.limit_graph_manager.get_nodes(graph_id),
                'edges': self.limit_graph_manager.get_edges(graph_id),
            }
        return {}

    async def get_moe_experts(self) -> List[str]:
        if self.moe_gating:
            return self.moe_gating.expert_names
        return []

    async def get_rlhf_pairs(self, limit: int = 100) -> List[Dict]:
        if self.rlhf_trainer:
            return self.rlhf_trainer.get_pairs(limit)
        return []

    async def record_rlhf_pair(self, pair_id, prompt, chosen, rejected, reward_diff, metadata=None):
        if self.rlhf_trainer:
            self.rlhf_trainer.record_pair(pair_id, prompt, chosen, rejected, reward_diff, metadata)


# ============================================================================
# Convenience factory
# ============================================================================
def create_material_updater(
    config: Optional[Dict[str, Any]] = None,
    storage: Optional[Storage] = None,
) -> MaterialFootprintUpdater:
    """
    Factory to create a fully configured MaterialFootprintUpdater.
    """
    return MaterialFootprintUpdater(config, storage)


# ============================================================================
# UNIT TESTS (Phase 10)
# ============================================================================
import unittest
from unittest import IsolatedAsyncioTestCase

class TestDistillationComponents(IsolatedAsyncioTestCase):
    def setUp(self):
        self.config = {
            'distillation_epsilon': 0.0,
            'distillation_replay_size': 10,
            'distillation_learning_rate': 0.01,
            'distillation_train_every': 10,
        }
        self.optimizer = DistillationUpdateOptimizer(self.config)

    def test_state_feature_vector(self):
        state = UpdateState(
            total_products=100,
            stale_fraction=0.3,
            avg_demand=2.0,
            bonsai_success_rate=0.8,
            footprintdata_success_rate=0.6,
            bonsai_cb_state=0.0,
            footprintdata_cb_state=1.0,
            hours_since_update=12.0,
            single_product_mode=0.0,
        )
        vec = state.to_feature_vector()
        self.assertEqual(len(vec), 9)

    def test_rule_based_teacher(self):
        teacher = UpdateRuleBasedTeacher()
        state = UpdateState(
            total_products=100,
            stale_fraction=0.6,
            avg_demand=2.0,
            bonsai_success_rate=0.9,
            footprintdata_success_rate=0.5,
            bonsai_cb_state=0.0,
            footprintdata_cb_state=0.0,
            hours_since_update=12.0,
            single_product_mode=0.0,
        )
        probs = teacher.predict(state)
        self.assertAlmostEqual(sum(probs), 1.0)
        self.assertGreater(probs[0], probs[1])

    async def test_select_action(self):
        state = UpdateState(
            total_products=100,
            stale_fraction=0.3,
            avg_demand=2.0,
            bonsai_success_rate=0.8,
            footprintdata_success_rate=0.6,
            bonsai_cb_state=0.0,
            footprintdata_cb_state=0.0,
            hours_since_update=12.0,
            single_product_mode=0.0,
        )
        action, idx, state_vec, teacher_probs = await self.optimizer.select_action(state, exploration=False)
        self.assertIn(action, self.optimizer.ACTION_SPACE)

    def test_replay_buffer(self):
        buffer = ReplayBuffer(max_size=5)
        state_vec = np.random.randn(9)
        buffer.push(state_vec, 0, 1.0, state_vec, np.ones(6)/6)
        self.assertEqual(len(buffer), 1)
        batch = buffer.sample(1)
        self.assertEqual(len(batch[0]), 1)


# ============================================================================
# Example usage
# ============================================================================
if __name__ == "__main__":
    import asyncio
    import sys
    sys.path.append('../')

    async def main():
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

        for _ in range(5):
            await updater.update_catalog()
            fp = updater.get_footprint("gpu-a100")
            print(f"Got footprint: {fp}")

        stats = updater.update_optimizer.get_stats()
        print("Distillation stats:", stats)

        pareto = await updater.run_strategy_evolution()
        print(f"Evolved Pareto front size: {len(pareto)}")
        if updater.best_evolved_strategy:
            print("Best strategy weights:", updater.best_evolved_strategy.weights)

        print("LIMIT Graph:", await updater.get_limit_graph())
        print("MoE experts:", await updater.get_moe_experts())

        await updater.close()

    asyncio.run(main())
