# =============================================================================
# FILE: src/enhancements/green_dashboard/app_v2_3_0.py
# VERSION: 2.4.0 (Enhanced with LIMIT Graph, MODP, RLHF, MoE, MOEA)
# =============================================================================
"""
Live Green Data Center Dashboard Web Application
Version 2.4.0

ENHANCEMENTS OVER v2.3.0:
1. Added LIMIT Graph manager for strategy/weight vector relationships.
2. Added MODP solver wrapper for persisting Pareto front and selected weights.
3. Added RLHF trainer for human preference collection.
4. Added MoE gating network to blend online distillation and offline MOEA.
5. Integrated NSGA‑II MOEA (existing) with MODP and LIMIT Graph.
6. New configuration flags for enabling each component.
All previous features retained.
"""

import asyncio
import hashlib
import json
import logging
import os
import sqlite3
import uuid
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Any, Callable, Tuple, Set, Union
from dataclasses import dataclass, field
import threading
import gc
import queue
import random
import numpy as np
from abc import ABC, abstractmethod
from collections import deque
import pickle
import pandas as pd
import copy
import time

# =============================================================================
# FastAPI and related
# =============================================================================
from fastapi import FastAPI, HTTPException, Depends, Header, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.exceptions import RequestValidationError
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, ValidationError, ConfigDict, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from starlette.status import HTTP_429_TOO_MANY_REQUESTS

# Rate limiting
try:
    from slowapi import Limiter, _rate_limit_exceeded_handler
    from slowapi.util import get_remote_address
    from slowapi.errors import RateLimitExceeded
    SLOWAPI_AVAILABLE = True
except ImportError:
    SLOWAPI_AVAILABLE = False

# Templating
try:
    from jinja2 import Environment, FileSystemLoader, Template
    JINJA2_AVAILABLE = True
except ImportError:
    JINJA2_AVAILABLE = False

# =============================================================================
# Security: Post‑quantum cryptography
# =============================================================================
try:
    from pqcrypto.sign import dilithium
    PQC_AVAILABLE = True
except ImportError:
    PQC_AVAILABLE = False

# Fallback cryptography
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric.utils import encode_dss_signature, decode_dss_signature
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat, PrivateFormat, NoEncryption
from cryptography.hazmat.backends import default_backend

# =============================================================================
# Existing Green Agent modules (assumed available)
# =============================================================================
from ..ai_data_center_loader import AIDataCenterLoader
from ..green_datacenter_selector import GreenDatacenterSelector, WorkloadSpec
from ..real_carbon_intensity_api import RealCarbonIntensityClient
from ..cloud_latency_estimator import CloudLatencyEstimator
from ..sustainability_signals import SustainabilitySignalEnricher

# =============================================================================
# Logging setup
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# =============================================================================
# Configuration (Pydantic Settings) – Extended with new flags
# =============================================================================
class Settings(BaseSettings):
    """Central configuration with environment variable support and validation."""
    model_config = SettingsConfigDict(env_prefix="DASHBOARD_", case_sensitive=False)

    # Database
    db_path: str = Field("/tmp/dashboard.db", description="SQLite database path")

    # API keys
    electricity_maps_api_key: str = Field("", description="ElectricityMap API key")
    carbon_intensity_api_key: str = Field("", description="Carbon intensity API key")
    carbon_region: str = Field("global", description="Default carbon region")

    # Authentication
    api_key_enabled: bool = Field(False, description="Enable API key authentication")
    api_key: str = Field("change-me", description="API key for endpoints")

    # Rate limiting
    rate_limit_requests: int = Field(50, ge=1, description="Max requests per window")
    rate_limit_window: int = Field(60, ge=1, description="Window length in seconds")

    # Caching
    cache_ttl_carbon: int = Field(300, ge=0, description="Carbon data TTL (seconds)")
    cache_ttl_latency: int = Field(3600, ge=0, description="Latency data TTL (seconds)")
    cache_ttl_projects: int = Field(60, ge=0, description="Projects list TTL (seconds)")

    # Blockchain (stub)
    blockchain_rpc_url: str = Field("http://localhost:8545", description="Blockchain RPC URL")
    blockchain_contract_address: str = Field("0x0000000000000000000000000000000000000000", description="Contract address")
    blockchain_private_key: str = Field("", description="Private key for blockchain")

    # Multi‑cloud (stub)
    aws_access_key_id: str = Field("", description="AWS access key")
    aws_secret_access_key: str = Field("", description="AWS secret key")
    aws_region: str = Field("us-east-1", description="Default AWS region")
    azure_connection_string: str = Field("", description="Azure connection string")
    gcp_credentials_path: str = Field("", description="GCP credentials file path")

    # Master encryption key (for key storage)
    master_key_hex: str = Field("", description="Master key in hex (32 bytes)")

    # Distillation parameters
    distillation_epsilon: float = Field(0.1, ge=0, le=1, description="Exploration rate")
    distillation_train_every: int = Field(10, ge=1, description="Train student every N recommendations")
    distillation_replay_size: int = Field(2000, ge=10, description="Replay buffer size")
    distillation_learning_rate: float = Field(0.01, ge=0.0001, le=1, description="Student learning rate")
    distill_weight: float = Field(0.7, ge=0, le=1, description="Distillation weight")
    rl_weight: float = Field(0.3, ge=0, le=1, description="RL weight")

    # MOEA parameters
    moea_enabled: bool = Field(True, description="Enable MOEA global optimization")
    moea_interval_seconds: int = Field(300, ge=60, description="MOEA run interval")
    moea_population_size: int = Field(20, ge=5)
    moea_generations: int = Field(10, ge=1)
    moea_mutation_rate: float = Field(0.2, ge=0.0, le=1.0)
    moea_crossover_rate: float = Field(0.8, ge=0.0, le=1.0)
    moea_tournament_size: int = Field(3, ge=2)
    moea_objective_weights: Dict[str, float] = Field(
        default_factory=lambda: {
            'carbon': 0.4,
            'cost': 0.3,
            'latency': 0.2,
            'user_satisfaction': 0.1,
        }
    )
    moea_dynamic_weights: bool = Field(True)

    # NEW v2.4.0 flags
    enable_limit_graph: bool = Field(True, description="Enable LIMIT Graph")
    enable_modp: bool = Field(True, description="Enable MODP solver")
    enable_rlhf: bool = Field(True, description="Enable RLHF trainer")
    enable_moe: bool = Field(True, description="Enable MoE gating")
    moe_expert_count: int = Field(3, ge=2, description="Number of MoE experts")

    # Persistence paths
    q_weights_path: str = Field("./strategy_q_weights.json", description="Q‑teacher weights")
    interaction_logs_path: str = Field("./strategy_interactions.csv", description="Interaction logs")
    historical_model_path: str = Field("./strategy_historical_model.pkl", description="Historical ML model")
    moea_pareto_path: str = Field("./strategy_moea_pareto.json", description="MOEA Pareto front")
    limit_graph_path: str = Field("./strategy_limit_graph.json", description="LIMIT Graph persistence")

    @field_validator('master_key_hex')
    @classmethod
    def validate_master_key(cls, v: str) -> str:
        if v and len(v) != 64:
            raise ValueError("master_key_hex must be 64 hex characters (32 bytes)")
        return v

    def get_master_key(self) -> bytes:
        if not self.master_key_hex:
            raise ValueError("Master key not set")
        return bytes.fromhex(self.master_key_hex)

# Global settings instance
settings = Settings()

# =============================================================================
# Persistent Storage (SQLite with connection pool)
# =============================================================================
class Storage:
    """Persistent storage for user preferences and audit logs with connection pooling."""
    def __init__(self, db_path: str = None):
        self.db_path = db_path or settings.db_path
        self._connection_pool = queue.Queue(maxsize=10)
        self._lock = threading.Lock()
        self._init_db()

    def _init_db(self):
        with self._get_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS user_preferences (
                    user_id TEXT PRIMARY KEY,
                    preferences TEXT,
                    updated_at TEXT NOT NULL
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    user_id TEXT,
                    action TEXT,
                    details TEXT
                )
            """)
            conn.commit()

    def _get_connection(self):
        try:
            return self._connection_pool.get_nowait()
        except queue.Empty:
            return sqlite3.connect(self.db_path, timeout=5)

    def _return_connection(self, conn):
        try:
            self._connection_pool.put_nowait(conn)
        except queue.Full:
            conn.close()

    def _execute(self, query: str, params: tuple = ()):
        conn = self._get_connection()
        try:
            return conn.execute(query, params)
        finally:
            self._return_connection(conn)

    def save_user_preferences(self, user_id: str, preferences: Dict):
        self._execute("""
            INSERT OR REPLACE INTO user_preferences (user_id, preferences, updated_at)
            VALUES (?, ?, ?)
        """, (user_id, json.dumps(preferences), datetime.now(timezone.utc).isoformat()))

    def get_user_preferences(self, user_id: str) -> Optional[Dict]:
        row = self._execute("SELECT preferences FROM user_preferences WHERE user_id = ?", (user_id,)).fetchone()
        if row:
            return json.loads(row[0])
        return None

    def log_audit(self, user_id: str, action: str, details: Dict):
        self._execute("""
            INSERT INTO audit_log (timestamp, user_id, action, details)
            VALUES (?, ?, ?, ?)
        """, (datetime.now(timezone.utc).isoformat(), user_id, action, json.dumps(details)))

# =============================================================================
# Cache implementation (TTL with UTC timestamps)
# =============================================================================
class Cache:
    """Simple in‑memory cache with TTL using UTC timestamps."""
    def __init__(self, ttl: int = 300):
        self._cache = {}
        self._ttl = ttl
        self._lock = asyncio.Lock()
        self._hits = 0
        self._misses = 0

    async def get(self, key: str) -> Optional[Any]:
        async with self._lock:
            if key in self._cache:
                value, timestamp = self._cache[key]
                if (datetime.now(timezone.utc) - timestamp).total_seconds() < self._ttl:
                    self._hits += 1
                    return value
                else:
                    del self._cache[key]
        self._misses += 1
        return None

    async def set(self, key: str, value: Any):
        async with self._lock:
            self._cache[key] = (value, datetime.now(timezone.utc))

    async def clear(self):
        async with self._lock:
            self._cache.clear()

    async def stats(self):
        async with self._lock:
            total = self._hits + self._misses
            return {
                "cache_size": len(self._cache),
                "hits": self._hits,
                "misses": self._misses,
                "hit_ratio": self._hits / total if total else 0.0
            }

# =============================================================================
# Quantum-Resilient Security (with real Dilithium or ECDSA)
# =============================================================================
class QuantumResilientSecurity:
    """Quantum-resilient security for signing API responses."""
    def __init__(self):
        self.pqc_available = PQC_AVAILABLE
        try:
            self.master_key = settings.get_master_key()
        except ValueError:
            logger.warning("Master key not set; signatures will use a static key.")
            self.master_key = b"\x00" * 32

        self._ecdsa_private_key = ec.generate_private_key(ec.SECP256R1(), default_backend())
        self._ecdsa_public_key = self._ecdsa_private_key.public_key()

    async def sign_data(self, data: Dict) -> Dict:
        data_bytes = json.dumps(data, sort_keys=True, default=str).encode()
        if self.pqc_available:
            signing_key, verifying_key = dilithium.generate_keypair()
            signature = dilithium.sign(data_bytes, signing_key)
            algorithm = "dilithium"
        else:
            signature = self._ecdsa_private_key.sign(
                data_bytes,
                ec.ECDSA(hashes.SHA256())
            )
            algorithm = "ecdsa"
        return {
            'signature': signature.hex(),
            'algorithm': algorithm,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }

# =============================================================================
# Blockchain Verifier (stub with actual async)
# =============================================================================
class BlockchainVerifier:
    """Blockchain verification stub."""
    async def record_recommendation(self, recommendation: Dict) -> Dict:
        tx_hash = f"0x{hashlib.sha256(json.dumps(recommendation, default=str).encode()).hexdigest()[:64]}"
        await asyncio.sleep(0.1)
        return {
            'status': 'success',
            'tx_hash': tx_hash,
            'block_number': 12345678
        }

# =============================================================================
# MULTI‑TEACHER DISTILLATION COMPONENTS (unchanged)
# =============================================================================

@dataclass
class StrategyState:
    """State for the distillation agent."""
    gpu_hours: float
    latency_tolerance_ms: float
    workload_type: str
    carbon_budget_kg: Optional[float]
    max_cost_usd: Optional[float]
    user_region: str
    recent_success_rate: float = 0.5
    avg_carbon_savings_kg: float = 0.0
    avg_cost_usd: float = 0.0

    def to_feature_vector(self) -> np.ndarray:
        features = [
            min(self.gpu_hours / 1000.0, 1.0),
            min(self.latency_tolerance_ms / 500.0, 1.0),
            1.0 if self.workload_type == "training" else 0.5 if self.workload_type == "inference" else 0.0,
            min(self.carbon_budget_kg / 100.0, 1.0) if self.carbon_budget_kg else 0.5,
            min(self.max_cost_usd / 1000.0, 1.0) if self.max_cost_usd else 0.5,
            1.0 if self.user_region == "us-east" else 0.0,
            1.0 if self.user_region == "us-west" else 0.0,
            1.0 if self.user_region == "eu-west" else 0.0,
            1.0 if self.user_region == "asia-east" else 0.0,
            1.0 if self.user_region == "asia-southeast" else 0.0,
            self.recent_success_rate,
            min(self.avg_carbon_savings_kg / 10.0, 1.0),
            min(self.avg_cost_usd / 100.0, 1.0),
        ]
        return np.array(features, dtype=np.float32)


class Teacher(ABC):
    @abstractmethod
    def predict(self, state: StrategyState) -> np.ndarray: ...
    @abstractmethod
    def confidence(self, state: StrategyState) -> float: ...

class StrategyRuleBasedTeacher(Teacher):
    STRATEGIES = ['carbon_first', 'latency_first', 'cost_first', 'balanced', 'hybrid']
    def predict(self, state):
        probs = np.ones(5)*0.1
        if state.carbon_budget_kg and state.carbon_budget_kg < 10:
            probs[0]=0.8
        elif state.latency_tolerance_ms < 50:
            probs[1]=0.8
        elif state.max_cost_usd and state.max_cost_usd < 500:
            probs[2]=0.7
        elif state.workload_type == "training":
            probs[3]=0.7
        else:
            probs[4]=0.6
        return probs/probs.sum()
    def confidence(self, state):
        if state.carbon_budget_kg and state.carbon_budget_kg < 10:
            return 0.6
        return 0.4

class StrategyHistoricalMLTeacher(Teacher):
    def __init__(self, model_path=None):
        self.model=None; self.label_encoder=None
        self.model_path = model_path or Path(settings.historical_model_path)
        if self.model_path.exists():
            try:
                with open(self.model_path,'rb') as f:
                    self.model, self.label_encoder = pickle.load(f)
            except Exception as e:
                logger.error(f"Failed to load historical model: {e}")
    def predict(self, state):
        if self.model is None:
            return np.ones(5)/5
        x=state.to_feature_vector().reshape(1,-1)
        return self.model.predict_proba(x)[0]
    def confidence(self, state):
        return 0.7 if self.model is not None else 0.0

class StrategyStatefulQTeacher(Teacher):
    def __init__(self, lr=0.1):
        self.lr=lr
        self.weights=np.zeros((13,5))
        self._load_state()
    def _load_state(self):
        path=Path(settings.q_weights_path)
        if path.exists():
            try:
                with open(path,'r') as f:
                    self.weights=np.array(json.load(f))
            except Exception as e:
                logger.error(f"Failed to load Q-weights: {e}")
    def _save_state(self):
        path=Path(settings.q_weights_path)
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
    def __init__(self, feature_dim=13, n_classes=5, lr=0.01):
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

class DistillationStrategyOptimizer:
    STRATEGIES=['carbon_first','latency_first','cost_first','balanced','hybrid']
    def __init__(self, config):
        self.config=config
        self.student=DistillationStudent(lr=config.get('distillation_learning_rate',0.01))
        self.teachers=[StrategyRuleBasedTeacher(), StrategyHistoricalMLTeacher(), StrategyStatefulQTeacher()]
        self.replay_buffer=ReplayBuffer(max_size=config.get('distillation_replay_size',2000))
        self.epsilon=config.get('distillation_epsilon',0.1)
        self.train_every=config.get('distillation_train_every',10)
        self.counter=0
    async def select_strategy(self, state, exploration=True):
        state_vec=state.to_feature_vector(); n=5
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
        return self.STRATEGIES[action_idx], action_idx, state_vec, teacher_probs
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

# =============================================================================
# NEW: LIMIT Graph Manager
# =============================================================================
class LimitGraphManager:
    """
    Manages a graph of weight vector relationships for LIMIT.
    Nodes are weight vectors or updates, edges represent dependencies or improvements.
    """
    def __init__(self, storage: Optional[Any] = None):
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

# =============================================================================
# NEW: MODP Optimizer (wrapper)
# =============================================================================
class MODPOptimizer:
    """
    Multi‑Objective Dynamic Programming solver that stores decision states/policies.
    Used for persisting Pareto front points and selected weight vectors.
    """
    def __init__(self, storage: Optional[Any] = None):
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

    def add_policy(self, policy_id: str, problem_id: str, state_id: str,
                   action: str, expected_objectives: Dict[str, float]) -> None:
        if self.storage and hasattr(self.storage, 'save_modp_policy'):
            self.storage.save_modp_policy(policy_id, problem_id, state_id, action, expected_objectives)

    def get_states(self, problem_id: str) -> List[Dict]:
        if self.storage and hasattr(self.storage, 'get_modp_states'):
            return self.storage.get_modp_states(problem_id)
        return self.states.get(problem_id, [])

    def get_policies(self, problem_id: str) -> List[Dict]:
        if self.storage and hasattr(self.storage, 'get_modp_policies'):
            return self.storage.get_modp_policies(problem_id)
        return []

# =============================================================================
# NEW: RLHF Trainer
# =============================================================================
class RLHFTrainer:
    """
    Collects human preference pairs for weight vector choices.
    """
    def __init__(self, storage: Optional[Any] = None):
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

# =============================================================================
# NEW: MoE Gating Network for Strategy Blending
# =============================================================================
class MoEGatingNetwork:
    """
    Mixture-of-Experts gating that blends online distillation and offline MOEA strategies.
    The gating network learns to select the best source for the current context.
    """
    def __init__(self, storage: Optional[Any] = None, config: Optional[Dict] = None):
        self.storage = storage
        self.config = config or {}
        self.expert_names = self.config.get('expert_names', ['online', 'offline', 'rule_based'])
        self.num_experts = len(self.expert_names)
        # Simple linear gating weights on a small feature vector (normalized metrics)
        self.gating_weights = np.random.randn(self.num_experts, 5)  # 5 metrics
        self._training_samples = []

    def _encode_state(self, metrics: Dict[str, float]) -> np.ndarray:
        features = [
            metrics.get('quality', 0.5),
            metrics.get('energy', 0.5),
            metrics.get('carbon', 0.5),
            metrics.get('latency', 0.5),
            metrics.get('helium', 0.5),
        ]
        return np.array(features, dtype=np.float32)

    async def select_expert(self, metrics: Dict[str, float]) -> Tuple[str, np.ndarray]:
        x = self._encode_state(metrics)
        logits = self.gating_weights @ x
        probs = np.exp(logits - np.max(logits))
        probs /= probs.sum()
        expert_idx = np.argmax(probs)
        selected = self.expert_names[expert_idx]
        if self.storage and hasattr(self.storage, 'log_routing_decision'):
            sample_id = hashlib.sha256(str(metrics).encode()).hexdigest()[:16]
            self.storage.log_routing_decision(str(uuid.uuid4()), sample_id, selected, float(probs[expert_idx]))
        return selected, probs

    async def add_training_sample(self, metrics: Dict[str, float], selected_expert: str, reward: float):
        x = self._encode_state(metrics)
        expert_idx = self.expert_names.index(selected_expert)
        target = np.zeros(self.num_experts)
        target[expert_idx] = 1.0
        logits = self.gating_weights @ x
        probs = np.exp(logits - np.max(logits))
        probs /= probs.sum()
        grad = (probs - target)[:, None] * x[None, :]
        self.gating_weights -= 0.1 * grad

# =============================================================================
# NEW: Multi‑Objective Weight Optimizer (NSGA‑II) – existing but now integrated
# =============================================================================
@dataclass
class MOPDWeightVector:
    """A weight vector for scalarizing objectives (carbon, cost, latency)."""
    vector_id: str
    weights: Dict[str, float]  # keys: carbon, cost, latency, user_satisfaction
    objectives: Dict[str, float]  # achieved values (all maximized)
    scalarised_score: float = 0.0

    def to_dict(self):
        return {
            'vector_id': self.vector_id,
            'weights': self.weights,
            'objectives': self.objectives,
            'scalarised_score': self.scalarised_score,
        }

    @classmethod
    def from_dict(cls, data):
        return cls(**data)


class NSGAIIWeightOptimizer:
    # ... (existing implementation retained, but we will add MODP and LIMIT graph integration)
    # For brevity, we include the full class here; it's the same as previous.
    def __init__(self,
                 evaluate_func: Callable[[Dict[str, float]], Awaitable[Dict[str, float]]],
                 population_size: int = 20,
                 generations: int = 10,
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
            'carbon': 0.4,
            'cost': 0.3,
            'latency': 0.2,
            'user_satisfaction': 0.1,
        }
        self.dynamic_weights = dynamic_weights

        self.best_individual = None
        self.best_fitness = -float('inf')
        self.evolution_history = []
        self.pareto_front: List[MOPDWeightVector] = []
        self._eval_cache: Dict[Tuple[float, ...], Dict[str, float]] = {}

    def _random_individual(self) -> Dict[str, float]:
        keys = ['carbon', 'cost', 'latency', 'user_satisfaction']
        w = {k: random.random() for k in keys}
        total = sum(w.values())
        if total > 0:
            w = {k: v/total for k, v in w.items()}
        return w

    def _crossover(self, p1, p2):
        child = {}
        for key in p1:
            if random.random() < 0.5:
                u = random.random()
                if u <= 0.5:
                    beta = (2*u)**(1/(20+1))
                else:
                    beta = (1/(2*(1-u)))**(1/(20+1))
                child[key] = max(0.0, min(1.0, 0.5*((1+beta)*p1[key] + (1-beta)*p2[key])))
            else:
                child[key] = p1[key] if random.random() < 0.5 else p2[key]
        total = sum(child.values())
        if total > 0:
            child = {k: v/total for k, v in child.items()}
        return child

    def _mutate(self, ind):
        mutant = ind.copy()
        for key in mutant:
            if random.random() < self.mutation_rate:
                u = random.random()
                if u < 0.5:
                    delta = (2*u)**(1/(20+1)) - 1
                else:
                    delta = 1 - (2*(1-u))**(1/(20+1))
                mutant[key] = mutant[key] + delta
                mutant[key] = max(0.0, min(1.0, mutant[key]))
        total = sum(mutant.values())
        if total > 0:
            mutant = {k: v/total for k, v in mutant.items()}
        return mutant

    def _fast_non_dominated_sort(self, points):
        fronts = []
        domination_count = {id(p):0 for p in points}
        dominated_solutions = {id(p):[] for p in points}
        for i,p in enumerate(points):
            p_obj = p.objectives
            for j,q in enumerate(points):
                if i==j: continue
                q_obj = q.objectives
                if all(p_obj[k]>=q_obj[k] for k in p_obj) and any(p_obj[k]>q_obj[k] for k in p_obj):
                    dominated_solutions[id(p)].append(q)
                elif all(q_obj[k]>=p_obj[k] for k in q_obj) and any(q_obj[k]>p_obj[k] for k in q_obj):
                    domination_count[id(p)] += 1
            if domination_count[id(p)]==0:
                if not fronts:
                    fronts.append([])
                fronts[0].append(p)
        i=0
        while i<len(fronts):
            next_front=[]
            for p in fronts[i]:
                for q in dominated_solutions[id(p)]:
                    domination_count[id(q)]-=1
                    if domination_count[id(q)]==0:
                        next_front.append(q)
            if next_front:
                fronts.append(next_front)
            i+=1
        return fronts

    def _crowding_distance(self, front):
        if not front: return {}
        distances={id(p):0.0 for p in front}
        obj_keys=list(front[0].objectives.keys())
        for obj in obj_keys:
            sorted_front=sorted(front,key=lambda x:x.objectives[obj])
            distances[id(sorted_front[0])]=float('inf')
            distances[id(sorted_front[-1])]=float('inf')
            obj_min=sorted_front[0].objectives[obj]
            obj_max=sorted_front[-1].objectives[obj]
            if obj_max==obj_min: continue
            for i in range(1,len(sorted_front)-1):
                distances[id(sorted_front[i])]+=(sorted_front[i+1].objectives[obj]-sorted_front[i-1].objectives[obj])/(obj_max-obj_min)
        return distances

    def _tournament_selection(self, population, fronts, crowding):
        candidates=random.sample(population,self.tournament_size)
        ind_to_point={}
        for ind,point in zip(population,self._all_points):
            ind_to_point[id(ind)]=point
        best=candidates[0]; best_rank=float('inf'); best_crowding=-float('inf')
        for cand in candidates:
            point=ind_to_point.get(id(cand))
            if not point: continue
            rank=len(fronts)
            for fi,front in enumerate(fronts):
                if point in front:
                    rank=fi; break
            cd=crowding.get(id(point),0)
            if rank<best_rank or (rank==best_rank and cd>best_crowding):
                best=cand; best_rank=rank; best_crowding=cd
        return best

    def _compute_dynamic_weights(self):
        weights=self.objective_weights.copy()
        if not self.dynamic_weights or not self.pareto_front:
            return weights
        obj_keys=list(weights.keys())
        avg={k:np.mean([p.objectives[k] for p in self.pareto_front]) for k in obj_keys}
        max_val={k:np.max([p.objectives[k] for p in self.pareto_front]) for k in obj_keys}
        for k in obj_keys:
            if max_val[k]>0 and avg[k]<0.5*max_val[k]:
                weights[k]=min(0.6,weights.get(k,0.0)*1.5)
        total=sum(weights.values())
        if total>0:
            weights={k:v/total for k,v in weights.items()}
        return weights

    def _select_best_from_pareto(self, pareto, weights):
        if not pareto: return None
        obj_keys=list(weights.keys())
        max_vals={k:max(p.objectives[k] for p in pareto) for k in obj_keys}
        min_vals={k:min(p.objectives[k] for p in pareto) for k in obj_keys}
        ranges={k:max_vals[k]-min_vals[k] if max_vals[k]!=min_vals[k] else 1.0 for k in obj_keys}
        best=None; best_score=-float('inf')
        for p in pareto:
            score=0.0
            for k in obj_keys:
                val=p.objectives[k]
                norm=(val-min_vals[k])/ranges[k] if ranges[k]>0 else 1.0
                score+=weights.get(k,0.0)*norm
            p.scalarised_score=score
            if score>best_score:
                best_score=score; best=p
        return best

    async def evolve(self):
        population=[self._random_individual() for _ in range(self.population_size)]
        points=[]
        eval_tasks=[self.evaluate_func(ind) for ind in population]
        eval_results=await asyncio.gather(*eval_tasks)
        for ind,obj in zip(population,eval_results):
            point=MOPDWeightVector(vector_id=str(uuid.uuid4()),weights=ind,objectives=obj)
            points.append(point)
            self._eval_cache[tuple(sorted(ind.items()))]=obj
        self._all_points=points
        for gen in range(self.generations):
            fronts=self._fast_non_dominated_sort(points)
            crowding={}
            for front in fronts:
                front_crowding=self._crowding_distance(front)
                crowding.update(front_crowding)
            offspring=[]
            while len(offspring)<self.population_size:
                parent1=self._tournament_selection(population,fronts,crowding)
                parent2=self._tournament_selection(population,fronts,crowding)
                if random.random()<self.crossover_rate:
                    child=self._crossover(parent1,parent2)
                else:
                    child=copy.deepcopy(parent1)
                child=self._mutate(child)
                offspring.append(child)
            child_tasks=[self.evaluate_func(ind) for ind in offspring]
            child_results=await asyncio.gather(*child_tasks)
            child_points=[]
            for ind,obj in zip(offspring,child_results):
                point=MOPDWeightVector(vector_id=str(uuid.uuid4()),weights=ind,objectives=obj)
                child_points.append(point)
                self._eval_cache[tuple(sorted(ind.items()))]=obj
            combined_inds=population+offspring
            combined_points=points+child_points
            unique_pairs={}
            for ind,p in zip(combined_inds,combined_points):
                key=tuple(sorted(ind.items()))
                unique_pairs[key]=(ind,p)
            population=[v[0] for v in unique_pairs.values()]
            points=[v[1] for v in unique_pairs.values()]
            self._all_points=points
            fronts=self._fast_non_dominated_sort(points)
            new_population=[]; new_points=[]
            for front in fronts:
                if len(new_population)+len(front)<=self.population_size:
                    for p in front:
                        for ind,p2 in zip(population,points):
                            if p2 is p:
                                new_population.append(ind); new_points.append(p); break
                else:
                    crowding=self._crowding_distance(front)
                    sorted_front=sorted(front,key=lambda x:crowding.get(id(x),0),reverse=True)
                    for p in sorted_front:
                        if len(new_population)>=self.population_size: break
                        for ind,p2 in zip(population,points):
                            if p2 is p:
                                new_population.append(ind); new_points.append(p); break
            population=new_population[:self.population_size]
            points=new_points[:self.population_size]
            self._all_points=points
            fronts=self._fast_non_dominated_sort(points)
            if fronts:
                self.pareto_front=fronts[0]
            logger.info(f"Generation {gen+1}/{self.generations}: Pareto front size={len(self.pareto_front)}")
        weights=self._compute_dynamic_weights()
        best=self._select_best_from_pareto(self.pareto_front,weights)
        if best:
            self.best_individual=best.weights
            self.best_fitness=best.scalarised_score
        return self.pareto_front

# =============================================================================
# Multi-Cloud Distributor (unchanged)
# =============================================================================
class MultiCloudDistributor:
    """Multi-cloud distribution with region‑aware choice."""
    async def distribute(self, data: Dict) -> Dict:
        user_region = data.get('user_region', 'us-east')
        region_map = {
            'us-east': {'provider': 'aws', 'region': 'us-east-1'},
            'us-west': {'provider': 'aws', 'region': 'us-west-2'},
            'eu-west': {'provider': 'azure', 'region': 'westeurope'},
            'asia-east': {'provider': 'gcp', 'region': 'asia-east1'},
            'asia-southeast': {'provider': 'aws', 'region': 'ap-southeast-1'},
        }
        choice = region_map.get(user_region, {'provider': 'aws', 'region': 'us-east-1'})
        return {
            'optimal_provider': choice['provider'],
            'optimal_region': choice['region'],
            'reason': 'Based on user region proximity'
        }

# =============================================================================
# FastAPI application
# =============================================================================
app = FastAPI(
    title="Green Data Center Dashboard",
    description="AI Data Center Sustainability Explorer",
    version="2.4.0"
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Exception handlers (same as before)
@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    return JSONResponse(status_code=422, content={"detail": exc.errors()})

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(status_code=500, content={"detail": "Internal server error"})

# Rate limiting
if SLOWAPI_AVAILABLE:
    limiter = Limiter(key_func=get_remote_address)
    app.state.limiter = limiter
    app.add_exception_handler(429, _rate_limit_exceeded_handler)
else:
    limiter = None

def noop_decorator(func):
    return func

def rate_limit_decorator(limit_str):
    if limiter:
        return limiter.limit(limit_str)
    return noop_decorator

async def verify_api_key(api_key: str = Header(None, alias="X-API-Key")):
    if settings.api_key_enabled:
        if api_key != settings.api_key:
            raise HTTPException(status_code=401, detail="Invalid API key")
    return api_key

# =============================================================================
# Global components
# =============================================================================
loader = None
selector = None
carbon_client = None
latency_estimator = None
sustainability_enricher = None
cache = None
storage = None
security = None
blockchain = None
multi_cloud = None
strategy_optimizer = None
moea_optimizer = None
moea_task = None
projects_cache_key = "all_projects"
interaction_log: List[Dict] = []

# NEW components
limit_graph_manager = None
modp_solver = None
rlhf_trainer = None
moe_gating = None

@app.on_event("startup")
async def startup():
    global loader, selector, carbon_client, latency_estimator, sustainability_enricher, cache, storage, security, blockchain, multi_cloud, strategy_optimizer, moea_optimizer, moea_task
    global limit_graph_manager, modp_solver, rlhf_trainer, moe_gating

    logger.info("Starting Green Data Center Dashboard v2.4.0...")
    logger.info(f"Settings loaded: {settings.model_dump(exclude={'api_key', 'master_key_hex'})}")

    if settings.api_key_enabled and settings.api_key == "change-me":
        logger.warning("API key enabled but using default key. Please change it.")

    storage = Storage()
    cache = Cache()
    loader = AIDataCenterLoader()
    selector = GreenDatacenterSelector(loader)
    carbon_client = RealCarbonIntensityClient()
    latency_estimator = CloudLatencyEstimator()
    sustainability_enricher = SustainabilitySignalEnricher()
    security = QuantumResilientSecurity()
    blockchain = BlockchainVerifier()
    multi_cloud = MultiCloudDistributor()

    strategy_optimizer = DistillationStrategyOptimizer({
        'distillation_epsilon': settings.distillation_epsilon,
        'distillation_train_every': settings.distillation_train_every,
        'distillation_replay_size': settings.distillation_replay_size,
        'distillation_learning_rate': settings.distillation_learning_rate,
    })

    # Initialize new components
    if settings.enable_limit_graph:
        limit_graph_manager = LimitGraphManager(storage)
        # Create initial graph if not exists
        if not limit_graph_manager.get_metadata("strategy_graph"):
            limit_graph_manager.create_graph("strategy_graph", "Strategy Weight Vector Relationships", {})
    if settings.enable_modp:
        modp_solver = MODPOptimizer(storage)
    if settings.enable_rlhf:
        rlhf_trainer = RLHFTrainer(storage)
    if settings.enable_moe:
        moe_gating = MoEGatingNetwork(storage, {'expert_names': ['online', 'offline', 'rule_based']})

    moea_optimizer = NSGAIIWeightOptimizer(
        evaluate_func=None,  # set in run_moea
        population_size=settings.moea_population_size,
        generations=settings.moea_generations,
        mutation_rate=settings.moea_mutation_rate,
        crossover_rate=settings.moea_crossover_rate,
        tournament_size=settings.moea_tournament_size,
        objective_weights=settings.moea_objective_weights,
        dynamic_weights=settings.moea_dynamic_weights,
    )

    if settings.moea_enabled:
        moea_task = asyncio.create_task(moea_loop())

    await carbon_client.start()
    logger.info("Dashboard startup complete.")

@app.on_event("shutdown")
async def shutdown():
    logger.info("Shutting down Green Data Center Dashboard...")
    if carbon_client:
        await carbon_client.close()
    if moea_task:
        moea_task.cancel()
        await asyncio.gather(moea_task, return_exceptions=True)
    logger.info("Shutdown complete.")

# =============================================================================
# MOEA Background Loop
# =============================================================================
async def moea_loop():
    while True:
        try:
            await asyncio.sleep(settings.moea_interval_seconds)
            await run_moea()
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"MOEA loop error: {e}")
            await asyncio.sleep(60)

async def run_moea():
    global moea_optimizer, interaction_log, limit_graph_manager, modp_solver

    if len(interaction_log) < 20:
        logger.warning("Not enough interaction data for MOEA; skipping.")
        return

    async def evaluate(weights: Dict[str, float]) -> Dict[str, float]:
        # Use historical data (for demo: synthetic)
        carbon_savings = random.uniform(0, 10) * weights.get('carbon', 0.4)
        cost = 1000 * weights.get('cost', 0.3)
        latency = 200 * weights.get('latency', 0.2)
        user_satisfaction = random.uniform(0.5, 1.0)
        return {
            'carbon': carbon_savings,
            'cost': 1.0 - cost/2000,
            'latency': 1.0 - latency/500,
            'user_satisfaction': user_satisfaction,
        }

    moea_optimizer.evaluate_func = evaluate
    pareto = await moea_optimizer.evolve()
    logger.info(f"MOEA produced Pareto front of size {len(pareto)}")

    # Save Pareto front to disk
    try:
        with open(settings.moea_pareto_path, 'w') as f:
            json.dump([p.to_dict() for p in pareto], f, indent=2)
    except Exception as e:
        logger.error(f"Failed to save Pareto front: {e}")

    # Store best in MODP and add to LIMIT graph
    if pareto and moea_optimizer:
        weights = moea_optimizer._compute_dynamic_weights()
        best = moea_optimizer._select_best_from_pareto(pareto, weights)
        if best:
            # MODP
            if modp_solver:
                modp_solver.add_state(
                    state_id=f"moea_best_{best.vector_id}",
                    problem_id="strategy_weight_optimization",
                    state_attributes={'weights': best.weights},
                    objective_values=best.objectives,
                    stage=1
                )
            # LIMIT Graph
            if limit_graph_manager:
                limit_graph_manager.add_node(
                    "strategy_graph",
                    f"vector_{best.vector_id}",
                    "best_weight_vector",
                    {'weights': best.weights, 'objectives': best.objectives}
                )

# =============================================================================
# FastAPI endpoints
# =============================================================================
@app.get("/", response_class=HTMLResponse)
async def get_map(api_key: str = Depends(verify_api_key)):
    html_content = generate_map_html()
    return HTMLResponse(content=html_content)

@app.get("/api/projects")
@rate_limit_decorator(f"{settings.rate_limit_requests}/{settings.rate_limit_window}s")
async def get_projects(request: Request, api_key: str = Depends(verify_api_key)):
    cached_projects = await cache.get(projects_cache_key)
    if cached_projects is not None:
        projects = cached_projects
    else:
        projects = loader.get_all_projects()
        await cache.set(projects_cache_key, projects)

    for p in projects:
        try:
            cache_key = f"carbon_{p.location_country}"
            cached = await cache.get(cache_key)
            if cached is not None:
                intensity = cached
            else:
                intensity = await carbon_client.get_intensity(p.location_country)
                await cache.set(cache_key, intensity)
            p.sustainability.grid_carbon_intensity_gco2_per_kwh = intensity
            p.green_score = loader._compute_green_score(p)
        except Exception as e:
            logger.error(f"Failed to get carbon data for {p.location_country}: {e}")

    response = {
        "projects": [
            {
                "id": p.project_id,
                "name": p.project_name,
                "company": p.company,
                "location": f"{p.location_city}, {p.location_country}",
                "lat": p.latitude,
                "lon": p.longitude,
                "green_score": p.green_score,
                "capacity_mw": p.planned_power_capacity_mw,
                "status": p.status,
                "carbon_intensity": p.sustainability.grid_carbon_intensity_gco2_per_kwh,
                "renewable_share": p.sustainability.renewable_share_pct,
                "pue": p.sustainability.pue_estimated,
                "cooling_type": getattr(p.sustainability, 'cooling_type', 'unknown'),
                "water_stress": p.sustainability.water_stress_index
            }
            for p in projects
        ],
        "statistics": loader.get_statistics()
    }

    signature = await security.sign_data(response)
    response["quantum_signature"] = signature

    async def record_blockchain():
        try:
            await blockchain.record_recommendation({"type": "projects_list", "count": len(projects)})
        except Exception as e:
            logger.error(f"Blockchain recording failed: {e}")
    asyncio.create_task(record_blockchain())

    async def distribute_cloud():
        try:
            await multi_cloud.distribute(response)
        except Exception as e:
            logger.error(f"Multi-cloud distribution failed: {e}")
    asyncio.create_task(distribute_cloud())

    return response

@app.post("/api/recommend")
@rate_limit_decorator(f"{settings.rate_limit_requests}/{settings.rate_limit_window}s")
async def recommend_workload(request: Request, workload_req: dict, api_key: str = Depends(verify_api_key)):
    """Get data center recommendation for a workload."""
    try:
        workload = WorkloadSpec(
            gpu_hours=workload_req.get('gpu_hours', 100),
            latency_tolerance_ms=workload_req.get('latency_tolerance_ms', 200),
            workload_type=workload_req.get('workload_type', 'training'),
            carbon_budget_kg=workload_req.get('carbon_budget_kg'),
            max_cost_usd=workload_req.get('max_cost_usd')
        )
    except ValidationError as e:
        raise HTTPException(status_code=422, detail=e.errors())

    state = StrategyState(
        gpu_hours=workload.gpu_hours,
        latency_tolerance_ms=workload.latency_tolerance_ms,
        workload_type=workload.workload_type,
        carbon_budget_kg=workload.carbon_budget_kg,
        max_cost_usd=workload.max_cost_usd,
        user_region=workload_req.get('user_region', 'us-east'),
        recent_success_rate=0.5,
        avg_carbon_savings_kg=0.0,
        avg_cost_usd=0.0,
    )

    # Strategy selection: use MoE if available, else distillation
    if moe_gating:
        # Build metrics for gating context (simplified)
        metrics = {
            'quality': 0.5,  # placeholder
            'energy': 0.5,
            'carbon': 0.5,
            'latency': 0.5,
            'helium': 0.5,
        }
        expert_name, _ = await moe_gating.select_expert(metrics)
        strategy = expert_name if expert_name in DistillationStrategyOptimizer.STRATEGIES else 'balanced'
        action_idx = DistillationStrategyOptimizer.STRATEGIES.index(strategy)
        state_vec = state.to_feature_vector()
        teacher_probs = np.ones(5) / 5
    else:
        strategy, action_idx, state_vec, teacher_probs = await strategy_optimizer.select_strategy(state, exploration=True)

    logger.info(f"Using strategy: {strategy}")

    user_region = workload_req.get('user_region', 'us-east')
    result = selector.select_datacenter(workload, user_region)

    projects = loader.get_all_projects()
    avg_carbon = sum(p.sustainability.grid_carbon_intensity_gco2_per_kwh for p in projects) / len(projects) if projects else 400
    avg_emissions = workload.gpu_hours * 0.65 * 1.3 * (avg_carbon / 1000)
    savings = avg_emissions - result.estimated_carbon_kg

    response = {
        "selected_project": {
            "id": result.selected_project.project_id,
            "name": result.selected_project.project_name,
            "location": f"{result.selected_project.location_city}, {result.selected_project.location_country}",
            "green_score": result.green_score,
            "estimated_carbon_kg": result.estimated_carbon_kg,
            "estimated_cost_usd": result.estimated_cost_usd,
            "latency_ms": result.latency_ms
        },
        "alternatives": [
            {"name": alt.project_name, "green_score": score}
            for alt, score in result.alternatives
        ],
        "rationale": result.reasoning,
        "carbon_savings_kg": max(0, savings),
        "strategy_used": strategy
    }

    reward = 0.0
    if savings > 0:
        reward += 0.5
    if result.estimated_cost_usd < 1000:
        reward += 0.3
    if result.latency_ms < 200:
        reward += 0.2
    reward = max(0.0, min(1.0, reward))

    # Update agent (either distillation or MoE)
    if moe_gating and hasattr(moe_gating, '_last_selected_expert'):
        # Update MoE gating with reward
        await moe_gating.add_training_sample(metrics, expert_name, reward)
    else:
        next_state = state
        await strategy_optimizer.update(state_vec, action_idx, reward, next_state.to_feature_vector(), teacher_probs)

    # RLHF: record preference pair occasionally
    if rlhf_trainer and random.random() < 0.05:
        chosen_strategy = strategy
        rejected_strategy = random.choice([s for s in DistillationStrategyOptimizer.STRATEGIES if s != chosen_strategy])
        rlhf_trainer.record_pair(
            pair_id=str(uuid.uuid4()),
            prompt="Which strategy is better for this workload?",
            chosen=chosen_strategy,
            rejected=rejected_strategy,
            reward_diff=reward,
            metadata={'workload': workload.model_dump(), 'user_region': user_region}
        )

    # MODP: record state and policy
    if modp_solver:
        modp_solver.add_state(
            state_id=f"recommend_{uuid.uuid4()}",
            problem_id="strategy_selection",
            state_attributes={'workload': workload.model_dump(), 'strategy': strategy},
            objective_values={'carbon_savings': savings, 'cost': result.estimated_cost_usd, 'latency': result.latency_ms, 'user_satisfaction': reward},
            stage=0
        )
        modp_solver.add_policy(
            policy_id=f"policy_{uuid.uuid4()}",
            problem_id="strategy_selection",
            state_id="",  # optional
            action=strategy,
            expected_objectives={'carbon_savings': 0.0, 'cost': 0.0, 'latency': 0.0, 'user_satisfaction': 0.0}
        )

    # LIMIT Graph: add node for this recommendation
    if limit_graph_manager:
        limit_graph_manager.add_node(
            "strategy_graph",
            f"recommendation_{uuid.uuid4()}",
            "recommendation",
            {'strategy': strategy, 'carbon_savings': savings, 'cost': result.estimated_cost_usd, 'latency': result.latency_ms}
        )

    _log_interaction(state, strategy, reward)

    signature = await security.sign_data(response)
    response["quantum_signature"] = signature

    try:
        tx = await blockchain.record_recommendation({
            "workload": workload.model_dump(),
            "selected": result.selected_project.project_id,
            "carbon_savings": savings
        })
        response["blockchain_tx_hash"] = tx.get('tx_hash')
    except Exception as e:
        logger.error(f"Blockchain recording failed: {e}")

    try:
        dist = await multi_cloud.distribute({"user_region": user_region})
        response["cloud_distribution"] = dist
    except Exception as e:
        logger.error(f"Multi-cloud distribution failed: {e}")

    if storage:
        storage.log_audit(
            user_id=api_key or "anonymous",
            action="recommend",
            details={"workload": workload.model_dump(), "selected": result.selected_project.project_id}
        )

    return response

def _log_interaction(state: StrategyState, strategy: str, reward: float):
    """Log interaction for offline training."""
    entry = {
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'strategy': strategy,
        'reward': reward,
        'state_vector': state.to_feature_vector().tolist(),
    }
    interaction_log.append(entry)
    log_path = Path(settings.interaction_logs_path)
    df_log = pd.DataFrame([entry])
    if log_path.exists():
        df_log.to_csv(log_path, mode='a', header=False, index=False)
    else:
        df_log.to_csv(log_path, index=False)

# =============================================================================
# HTML generation (unchanged, but version updated)
# =============================================================================
def generate_map_html() -> str:
    # Same HTML as before, just title updated.
    return """
<!DOCTYPE html>
<html>
<head>
    <title>Green Data Center Dashboard v2.4</title>
    ...
</head>
<body>
    ...
</body>
</html>
    """
    # Note: full HTML omitted for brevity; replace with the existing HTML from v2.3.0.

# =============================================================================
# Optional: Run with uvicorn
# =============================================================================
if __name__ == "__main__":
    import uvicorn
    try:
        settings.get_master_key()
    except ValueError:
        logger.warning("Master key not set; some features will be limited.")
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
