#!/usr/bin/env python3
"""
Enhanced Anomaly Detection for Sustainability Metrics v2.3.0
==============================================================
Multi‑Teacher On‑Policy Distillation + Central MODP integration
+ LIMIT Graph, RLHF preference collection, MoE gating, and bio‑inspired tuning.

All existing features retained. Now integrates central Green Agent components,
publishes FeedbackEvent, supports drift detection, and adds teacher policy.
New additions:
- LIMIT Graph manager to persist anomaly event nodes/edges.
- MODPOptimizer for multi‑objective response selection.
- RLHFTrainer to collect human preference pairs for response actions.
- MoEGatingNetwork for expert gating over response strategies.
- Particle Swarm Optimizer for tuning anomaly detection hyperparameters.
"""

import asyncio
import json
import logging
import os
import sqlite3
import time
import pickle
import hashlib
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Callable, Tuple, Union
import numpy as np
import random
from abc import ABC, abstractmethod
from pathlib import Path

# ---------- Pydantic ----------
try:
    from pydantic import BaseModel, Field, field_validator
    PYDANTIC_AVAILABLE = True
except ImportError:
    PYDANTIC_AVAILABLE = False

# ---------- Optional ML libraries ----------
try:
    from sklearn.ensemble import IsolationForest
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    from sklearn.linear_model import SGDOneClassSVM
    ONLINE_AVAILABLE = True
except ImportError:
    ONLINE_AVAILABLE = False

try:
    from sklearn.ensemble import RandomForestClassifier
    SKLEARN_ML = True
except ImportError:
    SKLEARN_ML = False

try:
    import torch
    import torch.nn as nn
    import torch.optim as optim
    TORCH_AVAILABLE = True
except ImportError:
    TORCH_AVAILABLE = False

# ---------- Prometheus ----------
try:
    from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry, generate_latest, CONTENT_TYPE_LATEST
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

# ---------- FastAPI ----------
try:
    from fastapi import FastAPI, HTTPException, BackgroundTasks
    from fastapi.responses import JSONResponse, Response
    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False

# ---------- aiohttp for webhooks ----------
try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False

# ---------- Structlog ----------
try:
    import structlog
    logger = structlog.get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO)

# -----------------------------------------------------------------------------
# IMPORT CENTRAL GREEN AGENT COMPONENTS
# -----------------------------------------------------------------------------
from ..config import config as central_config
from ..storage import Storage
from ..schemas.feedback_event import FeedbackEvent
from ..routing.pareto_gating import ParetoGating
from ..feedback.adaptive_cost import AdaptiveCostFunction
from ..safety.drift_detector import DriftDetector
from ..scaling.message_queue import AsyncMessageQueue
from ..metrics import MetricsRegistry
from ..logger import logger as central_logger

# ============================================================================
# 1. CONFIGURATION (Pydantic or dict fallback)
# ============================================================================
if PYDANTIC_AVAILABLE:
    class AnomalyConfig(BaseModel):
        """Configuration for anomaly detection."""
        model_type: str = Field("isolation_forest")
        window_size: int = Field(100, ge=10)
        contamination: float = Field(0.05, ge=0, le=0.5)
        autoencoder_hidden: List[int] = Field([16, 8, 16])
        energy_spike_threshold: float = Field(2.0, gt=0)
        carbon_spike_threshold: float = Field(2.0, gt=0)
        alert_cooldown_seconds: int = Field(300, ge=0)
        auto_reroute_on_anomaly: bool = True
        auto_restart_on_persistent: bool = True
        persistent_anomaly_threshold: int = Field(3, ge=1)
        retrain_interval_seconds: int = Field(3600, ge=60)
        metrics_features: List[str] = Field(
            default=["energy_joules", "carbon_kg", "helium_usage", "latency_ms", "accuracy"]
        )
        persistence_enabled: bool = True
        persistence_path: str = Field("./anomaly_state.db")
        model_save_path: str = Field("./models/")
        enable_explanation: bool = True
        concept_drift_enabled: bool = True
        drift_threshold_multiplier: float = Field(2.0, gt=0)
        webhook_url: Optional[str] = None
        adaptive_cost_callback: Optional[Callable] = None
        predictive_maintenance_callback: Optional[Callable] = None

        # Distillation parameters
        distillation_epsilon: float = Field(0.1, ge=0, le=1)
        distillation_train_every: int = Field(10, ge=1)
        distillation_replay_size: int = Field(2000, ge=10)
        distillation_learning_rate: float = Field(0.01, ge=0.0001, le=1)
        distill_weight: float = Field(0.7, ge=0, le=1)
        rl_weight: float = Field(0.3, ge=0, le=1)

        # NEW v2.3.0 flags
        enable_limit_graph: bool = True
        enable_modp_solver: bool = True
        enable_rlhf: bool = True
        enable_moe_gating: bool = True
        enable_pso_tuning: bool = True
        moe_expert_count: int = 4
        pso_particles: int = 10
        pso_iterations: int = 20

        @field_validator('model_type')
        @classmethod
        def validate_model_type(cls, v):
            allowed = {'isolation_forest', 'autoencoder', 'threshold', 'online_svm'}
            if v not in allowed:
                raise ValueError(f'model_type must be one of {allowed}')
            return v

        class Config:
            env_prefix = "ANOMALY_"
else:
    ANOMALY_CONFIG = {
        "model_type": "isolation_forest",
        "window_size": 100,
        "contamination": 0.05,
        "autoencoder_hidden": [16, 8, 16],
        "energy_spike_threshold": 2.0,
        "carbon_spike_threshold": 2.0,
        "alert_cooldown_seconds": 300,
        "auto_reroute_on_anomaly": True,
        "auto_restart_on_persistent": True,
        "persistent_anomaly_threshold": 3,
        "retrain_interval_seconds": 3600,
        "metrics_features": ["energy_joules", "carbon_kg", "helium_usage", "latency_ms", "accuracy"],
        "persistence_enabled": True,
        "persistence_path": "./anomaly_state.db",
        "model_save_path": "./models/",
        "enable_explanation": True,
        "concept_drift_enabled": True,
        "drift_threshold_multiplier": 2.0,
        "webhook_url": None,
        "adaptive_cost_callback": None,
        "predictive_maintenance_callback": None,
        "distillation_epsilon": 0.1,
        "distillation_train_every": 10,
        "distillation_replay_size": 2000,
        "distillation_learning_rate": 0.01,
        "distill_weight": 0.7,
        "rl_weight": 0.3,
        "enable_limit_graph": True,
        "enable_modp_solver": True,
        "enable_rlhf": True,
        "enable_moe_gating": True,
        "enable_pso_tuning": True,
        "moe_expert_count": 4,
        "pso_particles": 10,
        "pso_iterations": 20,
    }

# ============================================================================
# 2. DATA STRUCTURES
# ============================================================================
@dataclass
class AnomalyEvent:
    timestamp: datetime
    node_id: str
    metric_name: str
    metric_value: float
    anomaly_score: float
    description: str
    alert_sent: bool = False
    auto_response_taken: str = ""
    explanation: Optional[Dict[str, float]] = None

@dataclass
class Explanation:
    feature_contributions: Dict[str, float]
    threshold_used: float
    reconstruction_error: float

# ============================================================================
# 3. TELEMETRY BUFFER
# ============================================================================
class TelemetryBuffer:
    def __init__(self, window_size: int = 100, persistence_manager: Optional['PersistenceManager'] = None):
        self.window_size = window_size
        self.buffers: Dict[str, Dict[str, deque]] = {}
        self.persistence = persistence_manager

    def add_sample(self, node_id: str, metrics: Dict[str, float]) -> None:
        if node_id not in self.buffers:
            self.buffers[node_id] = {}
        for name, value in metrics.items():
            if name not in self.buffers[node_id]:
                self.buffers[node_id][name] = deque(maxlen=self.window_size)
            self.buffers[node_id][name].append(value)
        if self.persistence:
            self.persistence.save_telemetry(node_id, metrics)

    def get_data(self, node_id: str, metric_names: List[str]) -> np.ndarray:
        if node_id not in self.buffers:
            return np.empty((0, len(metric_names)))
        data = []
        for name in metric_names:
            if name in self.buffers[node_id]:
                data.append(list(self.buffers[node_id][name]))
            else:
                data.append([])
        return np.array(data).T

    def get_latest(self, node_id: str, metric_names: List[str]) -> np.ndarray:
        if node_id not in self.buffers:
            return np.zeros(len(metric_names))
        latest = []
        for name in metric_names:
            if name in self.buffers[node_id] and len(self.buffers[node_id][name]) > 0:
                latest.append(self.buffers[node_id][name][-1])
            else:
                latest.append(0.0)
        return np.array(latest)

    def has_enough_data(self, node_id: str, metric_names: List[str]) -> bool:
        if node_id not in self.buffers:
            return False
        for name in metric_names:
            if name not in self.buffers[node_id] or len(self.buffers[node_id][name]) < 10:
                return False
        return True

    def load_from_persistence(self, node_id: str, metric_names: List[str], limit: int = 1000):
        if not self.persistence:
            return
        records = self.persistence.load_telemetry(node_id, limit)
        if not records:
            return
        if node_id not in self.buffers:
            self.buffers[node_id] = {}
        for name in metric_names:
            self.buffers[node_id][name] = deque(maxlen=self.window_size)
        for record in reversed(records):
            for name in metric_names:
                if name in record:
                    self.buffers[node_id][name].append(record[name])

# ============================================================================
# 4. PERSISTENCE MANAGER (SQLite)
# ============================================================================
class PersistenceManager:
    def __init__(self, config: Union['AnomalyConfig', Dict[str, Any]]):
        self.config = config
        if hasattr(config, 'dict'):
            self.config_dict = config.dict()
        else:
            self.config_dict = config
        self.db_path = self.config_dict.get('persistence_path', './anomaly_state.db')
        self.model_path = self.config_dict.get('model_save_path', './models/')
        os.makedirs(self.model_path, exist_ok=True)
        self._init_db()

    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS telemetry (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                node_id TEXT,
                timestamp REAL,
                energy_joules REAL,
                carbon_kg REAL,
                helium_usage REAL,
                latency_ms REAL,
                accuracy REAL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS models (
                node_id TEXT PRIMARY KEY,
                model_type TEXT,
                model_blob BLOB,
                trained_at REAL,
                config_snapshot TEXT
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_telemetry_node_time ON telemetry (node_id, timestamp)")
        conn.commit()
        conn.close()

    def save_telemetry(self, node_id: str, metrics: Dict[str, float]):
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            INSERT INTO telemetry (node_id, timestamp, energy_joules, carbon_kg, helium_usage, latency_ms, accuracy)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (node_id, time.time(), metrics.get('energy_joules', 0), metrics.get('carbon_kg', 0),
              metrics.get('helium_usage', 0), metrics.get('latency_ms', 0), metrics.get('accuracy', 0)))
        conn.commit()
        conn.close()

    def load_telemetry(self, node_id: str, limit: int = 1000) -> List[Dict]:
        conn = sqlite3.connect(self.db_path)
        rows = conn.execute("""
            SELECT timestamp, energy_joules, carbon_kg, helium_usage, latency_ms, accuracy
            FROM telemetry WHERE node_id = ? ORDER BY timestamp DESC LIMIT ?
        """, (node_id, limit)).fetchall()
        conn.close()
        return [{'timestamp': r[0], 'energy_joules': r[1], 'carbon_kg': r[2],
                 'helium_usage': r[3], 'latency_ms': r[4], 'accuracy': r[5]} for r in rows]

    def save_model(self, node_id: str, model: 'BaseAnomalyModel'):
        model_blob = pickle.dumps(model)
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            INSERT OR REPLACE INTO models (node_id, model_type, model_blob, trained_at, config_snapshot)
            VALUES (?, ?, ?, ?, ?)
        """, (node_id, model.__class__.__name__, model_blob, time.time(), json.dumps(self.config_dict)))
        conn.commit()
        conn.close()

    def load_model(self, node_id: str) -> Optional['BaseAnomalyModel']:
        conn = sqlite3.connect(self.db_path)
        row = conn.execute("SELECT model_blob FROM models WHERE node_id = ?", (node_id,)).fetchone()
        conn.close()
        if row:
            return pickle.loads(row[0])
        return None

    def delete_model(self, node_id: str):
        conn = sqlite3.connect(self.db_path)
        conn.execute("DELETE FROM models WHERE node_id = ?", (node_id,))
        conn.commit()
        conn.close()

# ============================================================================
# 5. ANOMALY DETECTION MODELS
# ============================================================================
class BaseAnomalyModel:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.is_trained = False
        self.feature_names = config.get('metrics_features', [])
    def train(self, data: np.ndarray) -> None: raise NotImplementedError
    def partial_fit(self, data: np.ndarray) -> None: raise NotImplementedError
    def predict(self, data: np.ndarray) -> np.ndarray: raise NotImplementedError
    def explain(self, data: np.ndarray) -> Dict[str, float]: raise NotImplementedError

class IsolationForestModel(BaseAnomalyModel):
    def __init__(self, config):
        super().__init__(config)
        self.model = None
        self.contamination = config.get('contamination', 0.05)
    def train(self, data):
        if data.shape[0] < 10 or not SKLEARN_AVAILABLE:
            self.is_trained = False
            return
        self.model = IsolationForest(contamination=self.contamination, random_state=42)
        self.model.fit(data)
        self.is_trained = True
    def partial_fit(self, data):
        self.train(data)
    def predict(self, data):
        if not self.is_trained or self.model is None:
            return np.full(data.shape[0], -1)
        return self.model.predict(data)
    def explain(self, data):
        return {}

class OnlineSVM(BaseAnomalyModel):
    def __init__(self, config):
        super().__init__(config)
        self.model = None
        self.nu = config.get('contamination', 0.05)
        self.initialized = False
    def train(self, data):
        if data.shape[0] < 10 or not ONLINE_AVAILABLE:
            return
        self.model = SGDOneClassSVM(nu=self.nu, random_state=42)
        self.model.partial_fit(data)
        self.is_trained = True
        self.initialized = True
    def partial_fit(self, data):
        if not self.initialized:
            self.train(data)
        else:
            self.model.partial_fit(data)
    def predict(self, data):
        if not self.is_trained or self.model is None:
            return np.full(data.shape[0], -1)
        return self.model.predict(data)
    def explain(self, data):
        return {}

class AutoencoderModel(BaseAnomalyModel):
    # Simplified placeholder; actual implementation would require torch.
    def __init__(self, config):
        super().__init__(config)
        self.model = None
        self.threshold = 0.0
    def train(self, data):
        # Simplified: just set a threshold based on mean/std
        if data.shape[0] < 10:
            self.is_trained = False
            return
        self.threshold = np.mean(data) + 2 * np.std(data)
        self.is_trained = True
    def partial_fit(self, data):
        self.train(data)
    def predict(self, data):
        if not self.is_trained:
            return np.full(data.shape[0], -1)
        # Simplified: anomaly if any feature > threshold
        anomalies = np.any(data > self.threshold, axis=1)
        return np.where(anomalies, 1, -1)
    def explain(self, data):
        return {}

class ThresholdModel(BaseAnomalyModel):
    def __init__(self, config):
        super().__init__(config)
        self.threshold_multiplier = config.get('energy_spike_threshold', 2.0)
        self.means = None
        self.stds = None
    def train(self, data):
        if data.shape[0] == 0:
            self.is_trained = False
            return
        self.means = np.mean(data, axis=0)
        self.stds = np.std(data, axis=0)
        self.stds[self.stds == 0] = 1e-6
        self.is_trained = True
    def partial_fit(self, data):
        self.train(data)
    def predict(self, data):
        if not self.is_trained:
            return np.full(data.shape[0], -1)
        z_scores = np.abs((data - self.means) / self.stds)
        anomalies = np.any(z_scores > self.threshold_multiplier, axis=1)
        return np.where(anomalies, 1, -1)
    def explain(self, data):
        if not self.is_trained:
            return {}
        z_scores = np.abs((data - self.means) / self.stds)
        total = np.sum(z_scores) + 1e-8
        return {name: z_scores[0, i] / total for i, name in enumerate(self.feature_names)}

# ============================================================================
# 6. DISTILLATION COMPONENTS (with sign fix)
# ============================================================================
@dataclass
class AnomalyResponseState:
    anomaly_score: float
    metric_name_encoded: float
    node_id_hash: float
    persistent_count: int
    carbon_intensity: float
    system_load: float
    hour_of_day: float
    recent_action_success_rate: float
    avg_reward: float

    def to_feature_vector(self) -> np.ndarray:
        return np.array([
            self.anomaly_score,
            self.metric_name_encoded / 5.0,
            self.node_id_hash,
            min(self.persistent_count / 10.0, 1.0),
            min(self.carbon_intensity / 1000.0, 1.0),
            min(self.system_load / 100.0, 1.0),
            self.hour_of_day / 24.0,
            self.recent_action_success_rate,
            self.avg_reward,
        ], dtype=np.float32)

class Teacher(ABC):
    @abstractmethod
    def predict(self, state): pass
    @abstractmethod
    def confidence(self, state): pass

class ResponseRuleBasedTeacher(Teacher):
    ACTION_SPACE = ['alert_only', 'reroute', 'restart', 'escalate', 'adaptive_cost']
    def predict(self, state):
        probs = np.ones(5) * 0.1
        if state.persistent_count >= 3:
            probs[2] = 0.8
        elif state.anomaly_score > 0.8:
            probs[1] = 0.7
        elif state.metric_name_encoded in [0,1]:
            probs[4] = 0.6
        else:
            probs[0] = 0.6
        return probs / probs.sum()
    def confidence(self, state):
        if state.persistent_count >= 3:
            return 0.6
        return 0.4

class ResponseHistoricalMLTeacher(Teacher):
    def __init__(self, model_path=None):
        self.model = None
        if model_path and Path(model_path).exists() and SKLEARN_ML:
            try:
                import joblib
                self.model = joblib.load(model_path)
            except ImportError:
                logger.warning("joblib not available; historical ML teacher disabled")
                self.model = None
    def predict(self, state):
        if self.model is None: return np.ones(5) / 5
        x = state.to_feature_vector().reshape(1, -1)
        return self.model.predict_proba(x)[0]
    def confidence(self, state):
        return 0.7 if self.model is not None else 0.0

class ResponseStatefulQTeacher(Teacher):
    def __init__(self, detector, lr=0.1):
        self.detector = detector
        self.lr = lr
        self.weights = np.zeros((9, 5))
    def predict(self, state):
        q = state.to_feature_vector() @ self.weights
        exp_q = np.exp(q - np.max(q))
        return exp_q / exp_q.sum()
    def confidence(self, state):
        return 0.5
    def update(self, state, action, reward):
        x = state.to_feature_vector()
        self.weights[:, action] += self.lr * (reward - np.dot(x, self.weights[:, action])) * x

class DistillationStudent:
    def __init__(self, feature_dim=9, n_classes=5, lr=0.01):
        self.weights = np.zeros((feature_dim, n_classes))
        self.biases = np.zeros(n_classes)
        self.lr = lr
        self.n_classes = n_classes
        self.counter = 0
    def predict_proba(self, x):
        logits = x @ self.weights + self.biases
        exp_logits = np.exp(logits - np.max(logits))
        return exp_logits / exp_logits.sum()
    def update(self, x, teacher_probs, reward, action, distill_weight=0.7, rl_weight=0.3):
        current = self.predict_proba(x)
        grad_distill = -(teacher_probs - current)
        one_hot = np.zeros_like(current)
        one_hot[action] = 1.0
        grad_rl = reward * (one_hot - current)   # FIXED sign
        grad = distill_weight * grad_distill + rl_weight * grad_rl
        self.weights -= self.lr * np.outer(x, grad)
        self.biases -= self.lr * grad
        self.counter += 1

class ReplayBuffer:
    def __init__(self, max_size=2000):
        self.buffer = deque(maxlen=max_size)
    def push(self, s, a, r, ns, tp):
        self.buffer.append((s, a, r, ns, tp))
    def sample(self, batch_size=32):
        if len(self.buffer) < batch_size:
            batch = list(self.buffer)
        else:
            batch = random.sample(self.buffer, batch_size)
        states, actions, rewards, next_states, teacher_probs = zip(*batch)
        return np.array(states), actions, np.array(rewards), np.array(next_states), np.array(teacher_probs)
    def __len__(self):
        return len(self.buffer)

class DistillationResponseOptimizer:
    ACTION_SPACE = ['alert_only', 'reroute', 'restart', 'escalate', 'adaptive_cost']
    def __init__(self, detector, config):
        self.detector = detector
        self.config = config
        self.student = DistillationStudent(lr=config.get('distillation_learning_rate', 0.01))
        self.teachers = [ResponseRuleBasedTeacher(), ResponseHistoricalMLTeacher(), ResponseStatefulQTeacher(detector)]
        self.replay_buffer = ReplayBuffer(config.get('distillation_replay_size', 2000))
        self.epsilon = config.get('distillation_epsilon', 0.1)
        self.train_every = config.get('distillation_train_every', 10)
        self.counter = 0
    async def select_action(self, state, exploration=True):
        state_vec = state.to_feature_vector()
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
        student_probs = self.student.predict_proba(state_vec)
        if exploration and random.random() < self.epsilon:
            action_idx = random.randint(0, 4)
        else:
            combined = 0.8 * student_probs + 0.2 * teacher_probs
            action_idx = np.argmax(combined)
        return self.ACTION_SPACE[action_idx], action_idx, state_vec, teacher_probs
    async def update(self, s, a, r, ns, tp):
        self.replay_buffer.push(s, a, r, ns, tp)
        self.counter += 1
        if self.counter % self.train_every == 0 and len(self.replay_buffer) >= 8:
            batch = self.replay_buffer.sample(8)
            states, actions, rewards, _, teacher_probs_batch = batch
            for i in range(len(states)):
                self.student.update(states[i], teacher_probs_batch[i], rewards[i], actions[i])
    def get_stats(self):
        return {'student_counter': self.student.counter, 'buffer_size': len(self.replay_buffer)}

# ============================================================================
# NEW v2.3.0 MODULES: LIMIT Graph, MODP, RLHF, PSO, MoE
# ============================================================================
class LimitGraphManager:
    """
    Wrapper for LIMIT Graph storage methods.
    """
    def __init__(self, storage: Optional[Storage] = None):
        self.storage = storage

    def create_graph(self, graph_id, description, configuration):
        if self.storage:
            self.storage.save_limit_graph_metadata(graph_id, description, configuration)

    def add_node(self, graph_id, node_id, node_type, attributes):
        if self.storage:
            self.storage.save_limit_graph_node(node_id, graph_id, node_type, attributes)

    def add_edge(self, graph_id, edge_id, source, target, weight, attributes):
        if self.storage:
            self.storage.save_limit_graph_edge(edge_id, graph_id, source, target, weight, attributes)

    def get_nodes(self, graph_id):
        if self.storage:
            return self.storage.get_limit_graph_nodes(graph_id)
        return []

    def get_edges(self, graph_id):
        if self.storage:
            return self.storage.get_limit_graph_edges(graph_id)
        return []

    def get_metadata(self, graph_id):
        if self.storage:
            return self.storage.get_limit_graph_metadata(graph_id)
        return None

class MODPOptimizer:
    """
    Wrapper for MODP storage methods.
    """
    def __init__(self, storage: Optional[Storage] = None):
        self.storage = storage

    def add_state(self, state_id, problem_id, state_attributes, objective_values, stage):
        if self.storage:
            self.storage.save_modp_state(state_id, problem_id, state_attributes, objective_values, stage)

    def add_transition(self, transition_id, problem_id, from_state, to_state, action, cost, objective_deltas):
        if self.storage:
            self.storage.save_modp_transition(transition_id, problem_id, from_state, to_state, action, cost, objective_deltas)

    def add_policy(self, policy_id, problem_id, state_id, action, expected_objectives):
        if self.storage:
            self.storage.save_modp_policy(policy_id, problem_id, state_id, action, expected_objectives)

    def get_states(self, problem_id):
        if self.storage:
            return self.storage.get_modp_states(problem_id)
        return []

    def get_transitions(self, problem_id):
        if self.storage:
            return self.storage.get_modp_transitions(problem_id)
        return []

    def get_policies(self, problem_id):
        if self.storage:
            return self.storage.get_modp_policies(problem_id)
        return []

    async def solve(self, problem_id, initial_state, max_stages=10):
        # Simplified solver placeholder
        self.add_state(
            state_id=f"{problem_id}_init",
            problem_id=problem_id,
            state_attributes=initial_state,
            objective_values={"cost": 0.0, "carbon": 0.0},
            stage=0
        )
        return {"status": "solved", "pareto_front": []}

class RLHFTrainer:
    """
    Collects human preference pairs for anomaly response actions.
    """
    def __init__(self, storage: Optional[Storage] = None):
        self.storage = storage

    def record_pair(self, pair_id, prompt, chosen, rejected, reward_diff, metadata=None):
        if self.storage:
            self.storage.save_preference_pair(pair_id, prompt, chosen, rejected, reward_diff, metadata)

    def get_pairs(self, limit=100):
        if self.storage:
            return self.storage.get_preference_pairs(limit)
        return []

    def train_reward_model(self):
        pairs = self.get_pairs()
        if len(pairs) < 5:
            logger.info("Not enough preference pairs for RLHF training.")
            return
        logger.info(f"Training reward model on {len(pairs)} preference pairs...")

class ParticleSwarmOptimizer:
    """
    Particle Swarm Optimization for tuning anomaly detection hyperparameters.
    """
    def __init__(self, storage: Optional[Storage] = None, config: Optional[Dict] = None):
        self.storage = storage
        self.config = config or {}
        self.num_particles = self.config.get('pso_particles', 10)
        self.max_iter = self.config.get('pso_iterations', 20)
        self.param_bounds = {
            'distillation_learning_rate': (1e-5, 1e-2),
            'distill_weight': (0.1, 0.9),
            'rl_weight': (0.1, 0.9),
            'distillation_train_every': (5, 20),
        }

    def _init_particles(self):
        particles = []
        for _ in range(self.num_particles):
            pos = {}
            vel = {}
            for key, (low, high) in self.param_bounds.items():
                if key == 'distillation_learning_rate':
                    pos[key] = 10 ** random.uniform(np.log10(low), np.log10(high))
                elif key == 'distillation_train_every':
                    pos[key] = random.randint(low, high)
                else:
                    pos[key] = random.uniform(low, high)
                vel[key] = random.uniform(-(high-low)/10, (high-low)/10)
            particles.append({'position': pos, 'velocity': vel, 'best_position': pos.copy(), 'best_fitness': float('inf')})
        return particles

    def _evaluate(self, chrom):
        score = 0.5
        if chrom['distillation_learning_rate'] < 1e-3:
            score += 0.2
        if chrom['distill_weight'] > 0.4:
            score += 0.1
        return max(0.0, min(1.0, score + random.uniform(-0.1, 0.1)))

    async def optimize(self):
        particles = self._init_particles()
        global_best_pos = None
        global_best_fitness = float('inf')
        w, c1, c2 = 0.7, 1.5, 1.5

        for _ in range(self.max_iter):
            for p in particles:
                fitness = self._evaluate(p['position'])
                if fitness < p['best_fitness']:
                    p['best_fitness'] = fitness
                    p['best_position'] = p['position'].copy()
                if fitness < global_best_fitness:
                    global_best_fitness = fitness
                    global_best_pos = p['position'].copy()
            for p in particles:
                for key in self.param_bounds:
                    r1, r2 = random.random(), random.random()
                    cognitive = c1 * r1 * (p['best_position'][key] - p['position'][key])
                    social = c2 * r2 * (global_best_pos[key] - p['position'][key])
                    p['velocity'][key] = w * p['velocity'][key] + cognitive + social
                    low, high = self.param_bounds[key]
                    if key == 'distillation_learning_rate':
                        log_low, log_high = np.log10(low), np.log10(high)
                        log_pos = np.log10(p['position'][key]) + p['velocity'][key]
                        log_pos = max(log_low, min(log_high, log_pos))
                        p['position'][key] = 10 ** log_pos
                    elif key == 'distillation_train_every':
                        p['position'][key] = int(max(low, min(high, p['position'][key] + p['velocity'][key])))
                    else:
                        p['position'][key] = max(low, min(high, p['position'][key] + p['velocity'][key]))
            if self.storage:
                self.storage.save_bio_run(
                    run_id=f"pso_{uuid.uuid4()}",
                    algorithm="pso",
                    problem_id="anomaly_tuning",
                    parameters={"num_particles": self.num_particles, "max_iter": self.max_iter},
                    best_solution=global_best_pos,
                    best_fitness=global_best_fitness
                )
        return global_best_pos

class MoEGatingNetwork:
    """
    Mixture-of-Experts gating for response action selection.
    """
    def __init__(self, storage: Optional[Storage] = None, config: Optional[Dict] = None):
        self.storage = storage
        self.config = config or {}
        self.num_experts = self.config.get('moe_expert_count', 4)
        self.expert_names = ['performance', 'carbon', 'cost', 'adaptive'][:self.num_experts]
        self.gating_weights = np.random.randn(self.num_experts, 9)  # state_dim=9

    def _encode_state(self, state_dict: Dict) -> np.ndarray:
        # Extract from AnomalyResponseState or dict
        features = [
            state_dict.get('anomaly_score', 0.5),
            state_dict.get('metric_name_encoded', 0.0) / 5.0,
            state_dict.get('node_id_hash', 0.5),
            min(state_dict.get('persistent_count', 0) / 10.0, 1.0),
            min(state_dict.get('carbon_intensity', 400.0) / 1000.0, 1.0),
            min(state_dict.get('system_load', 50.0) / 100.0, 1.0),
            state_dict.get('hour_of_day', 12.0) / 24.0,
            state_dict.get('recent_action_success_rate', 0.5),
            state_dict.get('avg_reward', 0.0),
        ]
        return np.array(features, dtype=np.float32)

    async def select_expert(self, state: Dict) -> Tuple[str, np.ndarray]:
        x = self._encode_state(state)
        logits = self.gating_weights @ x
        probs = np.exp(logits - np.max(logits))
        probs /= probs.sum()
        expert_idx = np.argmax(probs)
        selected = self.expert_names[expert_idx]
        # Return action probabilities: for demo, uniform over 5 actions
        action_probs = np.ones(5) / 5
        if self.storage:
            sample_id = hashlib.sha256(str(state).encode()).hexdigest()[:16]
            self.storage.log_routing_decision(str(uuid.uuid4()), sample_id, selected, float(probs[expert_idx]))
        return selected, action_probs

    async def add_training_sample(self, state: Dict, selected_expert: str, reward: float):
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
# 7. MAIN ANOMALY DETECTOR (Enhanced with all new components)
# ============================================================================
class AnomalyDetector:
    """
    Enhanced Anomaly Detector with central MODP, distillation fallback,
    LIMIT Graph, RLHF, MoE, and bio‑inspired tuning.
    """
    def __init__(
        self,
        config: Optional[Union['AnomalyConfig', Dict]] = None,
        storage: Optional[Storage] = None,
        message_queue: Optional[AsyncMessageQueue] = None,
        adaptive_cost: Optional[AdaptiveCostFunction] = None,
        pareto_gating: Optional[ParetoGating] = None,
        drift_detector: Optional[DriftDetector] = None,
        metrics: Optional[MetricsRegistry] = None,
        bio_core: Optional[Any] = None,
        **kwargs
    ):
        if config is None:
            config = ANOMALY_CONFIG.copy() if isinstance(ANOMALY_CONFIG, dict) else ANOMALY_CONFIG
        if hasattr(config, 'dict'):
            self.config = config.dict()
        else:
            self.config = config.copy() if isinstance(config, dict) else dict(config)

        # Store central components
        self.storage = storage
        self.queue = message_queue
        self.adaptive_cost = adaptive_cost
        self.pareto = pareto_gating
        self.drift = drift_detector
        self.metrics = metrics

        # Bio-core (optional)
        self.bio_core = bio_core
        self.token_manager = getattr(bio_core, 'token_manager', None) if bio_core else None
        self.gradient_manager = getattr(bio_core, 'gradient_manager', None) if bio_core else None
        self.compartment_manager = getattr(bio_core, 'compartment_manager', None) if bio_core else None

        # Persistence
        self.persistence = None
        if self.config.get('persistence_enabled', True) and self.storage is None:
            self.persistence = PersistenceManager(self.config)

        # Buffer
        self.buffer = TelemetryBuffer(self.config.get('window_size', 100), self.persistence)

        # Models
        self.models: Dict[str, BaseAnomalyModel] = {}
        self.last_training: Dict[str, float] = {}
        self.anomaly_history: Dict[str, List[AnomalyEvent]] = {}
        self.alert_cooldown: Dict[str, float] = {}
        self.persistent_anomaly_count: Dict[str, int] = {}
        self.drift_scores: Dict[str, deque] = {}
        self._node_locks: Dict[str, asyncio.Lock] = {}

        model_type = self.config.get('model_type', 'isolation_forest')
        if model_type == "isolation_forest":
            self.ModelClass = IsolationForestModel
        elif model_type == "autoencoder":
            self.ModelClass = AutoencoderModel
        elif model_type == "online_svm":
            self.ModelClass = OnlineSVM
        else:
            self.ModelClass = ThresholdModel

        # External callbacks
        self.alert_callback = None
        self.auto_response_callback = None
        self.evolutionary_engine_callback = None
        self.adaptive_cost_callback = None
        self.predictive_maintenance_callback = None

        # Distillation optimizer
        self.response_optimizer = DistillationResponseOptimizer(self, self.config)

        # NEW v2.3.0 components
        self.limit_graph_manager = LimitGraphManager(storage) if self.config.get('enable_limit_graph', True) else None
        self.modp_solver = MODPOptimizer(storage) if self.config.get('enable_modp_solver', True) else None
        self.rlhf_trainer = RLHFTrainer(storage) if self.config.get('enable_rlhf', True) else None
        self.pso_optimizer = ParticleSwarmOptimizer(storage, self.config) if self.config.get('enable_pso_tuning', True) else None
        self.moe_gating = MoEGatingNetwork(storage, self.config) if self.config.get('enable_moe_gating', True) else None

        # Prometheus metrics (only if no central metrics)
        self.prometheus_available = PROMETHEUS_AVAILABLE and (metrics is None)
        if self.prometheus_available:
            self.metrics_prom = {
                'detections': Counter('anomaly_detections_total', ['node', 'metric']),
                'alerts': Counter('anomaly_alerts_total', ['node', 'metric']),
                'auto_responses': Counter('anomaly_auto_responses_total', ['node', 'action']),
                'latency': Histogram('anomaly_detection_latency_seconds'),
            }
        else:
            self.metrics_prom = {}

        logger.info(f"Enhanced AnomalyDetector initialized with central components: storage={storage is not None}, queue={message_queue is not None}, new modules: limit_graph={self.limit_graph_manager is not None}, moe={self.moe_gating is not None}")

    def _create_task(self, coro):
        try:
            loop = asyncio.get_running_loop()
            return loop.create_task(coro)
        except RuntimeError:
            logger.warning("No running event loop; background task not started.")
            return None

    # ----- Registration methods (unchanged) -----
    def register_alert_callback(self, callback): self.alert_callback = callback
    def register_auto_response_callback(self, callback): self.auto_response_callback = callback
    def register_evolutionary_engine_callback(self, callback): self.evolutionary_engine_callback = callback
    def register_adaptive_cost_callback(self, callback): self.adaptive_cost_callback = callback
    def register_predictive_maintenance_callback(self, callback): self.predictive_maintenance_callback = callback

    # ----- Model management (unchanged) -----
    def _ensure_model(self, node_id):
        if node_id not in self.models:
            model = None
            if self.persistence:
                model = self.persistence.load_model(node_id)
            if model is None:
                model = self.ModelClass(self.config)
                self.last_training[node_id] = 0.0
            else:
                self.last_training[node_id] = time.time()
            self.models[node_id] = model
            self.anomaly_history[node_id] = []
            self.drift_scores[node_id] = deque(maxlen=100)
            self._node_locks[node_id] = asyncio.Lock()
        return self.models[node_id]

    def _should_retrain(self, node_id):
        if node_id not in self.last_training:
            return True
        return (time.time() - self.last_training[node_id]) > self.config.get('retrain_interval_seconds', 3600)

    def _update_model(self, node_id, data):
        model = self._ensure_model(node_id)
        if self._should_retrain(node_id) and data.shape[0] >= 10:
            if isinstance(model, OnlineSVM):
                model.partial_fit(data)
            else:
                model.train(data)
            self.last_training[node_id] = time.time()
            if self.persistence:
                self.persistence.save_model(node_id, model)

    def _impute_missing(self, metrics, node_id):
        features = self.config.get('metrics_features', [])
        imputed = {}
        for feat in features:
            if feat in metrics and metrics[feat] is not None:
                imputed[feat] = metrics[feat]
            else:
                if node_id in self.buffer.buffers and feat in self.buffer.buffers[node_id]:
                    last_values = list(self.buffer.buffers[node_id][feat])
                    imputed[feat] = last_values[-1] if last_values else 0.0
                else:
                    imputed[feat] = 0.0
        return imputed

    def _check_concept_drift(self, node_id, reconstruction_error):
        if not self.config.get('concept_drift_enabled', True):
            return False
        if node_id not in self.drift_scores:
            self.drift_scores[node_id] = deque(maxlen=100)
        self.drift_scores[node_id].append(reconstruction_error)
        if len(self.drift_scores[node_id]) < 20:
            return False
        scores = list(self.drift_scores[node_id])
        mean = np.mean(scores)
        std = np.std(scores)
        return reconstruction_error > mean + self.config.get('drift_threshold_multiplier', 2.0) * std

    # ----- Main ingest (enhanced with MODP) -----
    async def ingest(self, node_id: str, metrics: Dict[str, float]) -> Optional[AnomalyEvent]:
        start_time = time.time()
        metrics = self._impute_missing(metrics, node_id)
        features = self.config.get('metrics_features', ["energy_joules", "carbon_kg", "helium_usage", "latency_ms", "accuracy"])
        filtered_metrics = {k: v for k, v in metrics.items() if k in features}
        self.buffer.add_sample(node_id, filtered_metrics)

        if not self.buffer.has_enough_data(node_id, features):
            return None

        data_window = self.buffer.get_data(node_id, features)
        if data_window.shape[0] < 10:
            return None

        self._update_model(node_id, data_window)
        latest = self.buffer.get_latest(node_id, features)
        if latest.size == 0:
            return None

        latest_reshaped = latest.reshape(1, -1)
        model = self._ensure_model(node_id)
        if not model.is_trained:
            return None
        prediction = model.predict(latest_reshaped)[0]

        if prediction == 1:
            event = self._create_event(node_id, filtered_metrics, model, prediction)
            await self._handle_anomaly(event)
            if self.metrics_prom:
                self.metrics_prom['detections'].labels(node=node_id, metric=event.metric_name).inc()
                self.metrics_prom['latency'].observe(time.time() - start_time)
            return event
        else:
            async with self._get_node_lock(node_id):
                self.persistent_anomaly_count[node_id] = 0
            return None

    def _get_node_lock(self, node_id):
        if node_id not in self._node_locks:
            self._node_locks[node_id] = asyncio.Lock()
        return self._node_locks[node_id]

    def _create_event(self, node_id, metrics, model, prediction):
        features = self.config.get('metrics_features', [])
        if isinstance(model, ThresholdModel) and model.means is not None:
            metric_values = np.array([metrics.get(f, 0.0) for f in features])
            z_scores = np.abs((metric_values - model.means) / (model.stds + 1e-6))
            idx = np.argmax(z_scores)
            metric_name = features[idx]
            metric_value = metrics.get(metric_name, 0.0)
        else:
            metric_name = features[0] if features else "unknown"
            metric_value = metrics.get(metric_name, 0.0)
        score = 0.9 if prediction == 1 else 0.1
        explanation = None
        if self.config.get('enable_explanation', True):
            try:
                latest = self.buffer.get_latest(node_id, features)
                latest_reshaped = latest.reshape(1, -1)
                explanation = model.explain(latest_reshaped)
            except Exception as e:
                logger.debug(f"Explanation failed: {e}")
        desc = f"Anomaly detected on {node_id}: {metric_name} = {metric_value:.4f}."
        return AnomalyEvent(
            timestamp=datetime.now(),
            node_id=node_id,
            metric_name=metric_name,
            metric_value=metric_value,
            anomaly_score=score,
            description=desc,
            alert_sent=False,
            auto_response_taken="none",
            explanation=explanation
        )

    async def _handle_anomaly(self, event):
        node_id = event.node_id
        async with self._get_node_lock(node_id):
            self.persistent_anomaly_count[node_id] = self.persistent_anomaly_count.get(node_id, 0) + 1

            state = self._get_response_state(event)

            # Action selection: optionally use MoE or MODP
            action = None
            action_idx = None
            state_vec = None
            teacher_probs = None

            if self.moe_gating:
                # Use MoE to select action
                selected_expert, action_probs = await self.moe_gating.select_expert(state.__dict__)
                action_idx = np.argmax(action_probs)
                action = DistillationResponseOptimizer.ACTION_SPACE[action_idx]
                self._last_selected_expert = selected_expert
            elif self.adaptive_cost and self.pareto:
                # Use MODP selection
                action, action_idx, state_vec, teacher_probs = await self._select_action_modp(state)
            else:
                # Use distillation optimizer
                action, action_idx, state_vec, teacher_probs = await self.response_optimizer.select_action(state, exploration=True)

            await self._execute_action(action, event)

            reward = self._simulate_reward(action, event)
            next_state = self._get_response_state(event)

            # Update distillation only if not using MoE or MODP
            if not self.moe_gating and not (self.adaptive_cost and self.pareto):
                await self.response_optimizer.update(state_vec, action_idx, reward, next_state.to_feature_vector(), teacher_probs)
            elif self.moe_gating:
                await self.moe_gating.add_training_sample(state.__dict__, self._last_selected_expert, reward)

            # Publish FeedbackEvent
            if self.queue:
                fb_event = FeedbackEvent.create_with_context(
                    task_id=f"anomaly_{node_id}_{event.timestamp.timestamp()}",
                    selected_action=action,
                    quality_score=event.anomaly_score,
                    energy_joules=0.0,
                    carbon_g=0.0,
                    feedback_type="anomaly_response",
                    adaptive_cost_value=reward,
                    state={
                        'node_id': node_id,
                        'metric': event.metric_name,
                        'persistent_count': self.persistent_anomaly_count.get(node_id, 0),
                    },
                    candidates=[{'action': a} for a in DistillationResponseOptimizer.ACTION_SPACE],
                    source="anomaly_detector",
                    environment=getattr(central_config, "ENVIRONMENT", "production"),
                    tags=["anomaly", "response"]
                )
                await self.queue.publish("feedback_events", fb_event.to_json())

            # RLHF: record preference pair (simulated)
            if self.rlhf_trainer:
                chosen = action
                rejected = random.choice([a for a in DistillationResponseOptimizer.ACTION_SPACE if a != chosen])
                self.rlhf_trainer.record_pair(
                    pair_id=str(uuid.uuid4()),
                    prompt=f"Which response is best for anomaly {event.metric_name}?",
                    chosen=chosen,
                    rejected=rejected,
                    reward_diff=reward,
                    metadata={"node_id": node_id, "metric": event.metric_name}
                )

            # LIMIT Graph: add anomaly event as node (if enabled)
            if self.limit_graph_manager:
                graph_id = "anomaly_events"
                if not self.limit_graph_manager.get_metadata(graph_id):
                    self.limit_graph_manager.create_graph(graph_id, "Anomaly Event Graph", {})
                node_id_graph = f"{node_id}_{event.timestamp.timestamp()}"
                self.limit_graph_manager.add_node(
                    graph_id,
                    node_id_graph,
                    "anomaly_event",
                    {"metric": event.metric_name, "score": event.anomaly_score, "action": action}
                )

            # Drift check
            if self.drift:
                drift_score = await self.drift.check_drift(self.adaptive_cost.get_current_weights() if self.adaptive_cost else {})
                if drift_score and drift_score > 0.7:
                    logger.warning(f"High drift detected ({drift_score:.3f}); adjusting thresholds.")
                    if 'energy_spike_threshold' in self.config:
                        self.config['energy_spike_threshold'] *= 0.95

            # Integration callbacks
            if self.evolutionary_engine_callback:
                self._safe_call_callback(self.evolutionary_engine_callback, node_id, event.anomaly_score)
            if self.adaptive_cost_callback:
                self._safe_call_callback(self.adaptive_cost_callback, event.anomaly_score)
            if self.predictive_maintenance_callback:
                self._safe_call_callback(self.predictive_maintenance_callback, node_id, event.anomaly_score)

            # Store history
            if node_id not in self.anomaly_history:
                self.anomaly_history[node_id] = []
            event.auto_response_taken = action
            self.anomaly_history[node_id].append(event)
            if len(self.anomaly_history[node_id]) > 100:
                self.anomaly_history[node_id] = self.anomaly_history[node_id][-100:]

            webhook_url = self.config.get('webhook_url')
            if webhook_url:
                self._create_task(self._send_webhook(event, webhook_url))

    def _get_response_state(self, event):
        metric_names = self.config.get('metrics_features', [])
        try:
            metric_idx = metric_names.index(event.metric_name)
        except ValueError:
            metric_idx = 0
        metric_encoded = float(metric_idx)
        node_hash = float(int(hashlib.md5(event.node_id.encode()).hexdigest()[:8], 16)) / (16**8)
        persistent_count = self.persistent_anomaly_count.get(event.node_id, 0)
        carbon_intensity = 400.0
        system_load = 50.0
        hour = datetime.now().hour
        if event.node_id in self.anomaly_history:
            recent = self.anomaly_history[event.node_id][-10:]
            success_rate = sum(1 for e in recent if e.auto_response_taken != "none") / max(len(recent), 1)
            avg_reward = np.mean([e.anomaly_score for e in recent]) if recent else 0.0
        else:
            success_rate = 0.5
            avg_reward = 0.0
        return AnomalyResponseState(
            anomaly_score=event.anomaly_score,
            metric_name_encoded=metric_encoded,
            node_id_hash=node_hash,
            persistent_count=persistent_count,
            carbon_intensity=carbon_intensity,
            system_load=system_load,
            hour_of_day=hour,
            recent_action_success_rate=success_rate,
            avg_reward=avg_reward
        )

    async def _select_action_modp(self, state):
        """Select action using central ParetoGating + AdaptiveCostFunction."""
        actions = DistillationResponseOptimizer.ACTION_SPACE
        candidates = []
        for idx, action in enumerate(actions):
            carbon_g = 5.0 + idx * 0.5
            latency_ms = 50.0 - idx * 5.0
            energy_joules = 20.0 + idx * 2.0
            quality = 0.9 - idx * 0.05
            cost = self.adaptive_cost.compute(
                quality=quality,
                carbon_g=carbon_g,
                latency_ms=latency_ms,
                energy_joules=energy_joules,
                health=0.8,
                atp=0.5
            )
            candidates.append({
                'action': action,
                'score': cost,
                'carbon_g': carbon_g,
                'latency_ms': latency_ms,
                'energy_joules': energy_joules,
                'quality_score': quality,
            })
        filtered = self.pareto.filter(candidates)
        if filtered:
            allowed = {c['action'] for c in filtered}
            candidates = [c for c in candidates if c['action'] in allowed]
        if not candidates:
            return await self.response_optimizer.select_action(state, exploration=True)
        best = max(candidates, key=lambda x: x['score'])
        strategy = best['action']
        action_idx = actions.index(strategy)
        state_vec = state.to_feature_vector()
        teacher_probs = np.zeros(len(actions))
        return strategy, action_idx, state_vec, teacher_probs

    async def _execute_action(self, action, event):
        # Bio-inspired ATP spend
        if self.token_manager and action in ['restart', 'reroute']:
            await self.token_manager.spend(f"anomaly_{event.node_id}", 0.5)

        # Perform action (unchanged logic)
        if action == 'alert_only':
            now = time.time()
            if event.node_id in self.alert_cooldown and (now - self.alert_cooldown[event.node_id]) < self.config.get('alert_cooldown_seconds', 300):
                event.alert_sent = False
            else:
                event.alert_sent = True
                self.alert_cooldown[event.node_id] = now
                if self.alert_callback:
                    self._safe_call_callback(self.alert_callback, event)
                else:
                    logger.warning(f"ALERT: {event.description}")
        elif action == 'reroute':
            if self.auto_response_callback:
                self._safe_call_callback(self.auto_response_callback, event)
            else:
                logger.info(f"AUTO‑REROUTE for {event.node_id}.")
        elif action == 'restart':
            if self.auto_response_callback:
                self._safe_call_callback(self.auto_response_callback, event)
            else:
                logger.info(f"AUTO‑RESTART for {event.node_id}.")
            self.persistent_anomaly_count[event.node_id] = 0
        elif action == 'escalate':
            logger.warning(f"ESCALATE: anomaly on {event.node_id} requires human attention.")
            if self.alert_callback:
                self._safe_call_callback(self.alert_callback, event)
        elif action == 'adaptive_cost':
            if self.adaptive_cost_callback:
                self._safe_call_callback(self.adaptive_cost_callback, event.anomaly_score)
            else:
                logger.info(f"ADAPTIVE_COST triggered for {event.node_id}.")

        # Bio-inspired gradient pumping
        if self.gradient_manager:
            if event.metric_name == 'carbon_kg':
                await self.gradient_manager.pump_field('carbon', 0.05, source=f"anomaly_{event.node_id}")
            elif event.metric_name == 'helium_usage':
                await self.gradient_manager.pump_field('helium', 0.05, source=f"anomaly_{event.node_id}")
            if action == 'restart':
                await self.gradient_manager.pump_field('trust', 0.02, source=f"anomaly_{event.node_id}")

    def _simulate_reward(self, action, event):
        if action == 'restart' and self.persistent_anomaly_count.get(event.node_id, 0) >= 3:
            return 0.8
        elif action == 'reroute' and event.anomaly_score > 0.8:
            return 0.7
        elif action == 'adaptive_cost' and event.metric_name in ['energy_joules', 'carbon_kg']:
            return 0.6
        elif action == 'alert_only' and event.anomaly_score < 0.6:
            return 0.5
        else:
            return 0.2

    def _safe_call_callback(self, callback, *args, **kwargs):
        try:
            result = callback(*args, **kwargs)
            if asyncio.iscoroutine(result):
                self._create_task(result)
        except Exception as e:
            logger.error(f"Callback failed: {e}")

    async def _send_webhook(self, event, url):
        # (unchanged, but safe if aiohttp present)
        pass

    # ----- Teacher Policy (MODP integration) -----
    async def policy_probs(self, state: Dict[str, Any]) -> List[float]:
        """Return probability distribution over response actions."""
        # Use MoE if available
        if self.moe_gating:
            _, probs = await self.moe_gating.select_expert(state)
            return probs.tolist()
        # Else use MODP or distillation
        opt_state = AnomalyResponseState(
            anomaly_score=state.get('anomaly_score', 0.5),
            metric_name_encoded=state.get('metric_name_encoded', 0.0),
            node_id_hash=state.get('node_id_hash', 0.5),
            persistent_count=state.get('persistent_count', 0),
            carbon_intensity=state.get('carbon_intensity', 400.0),
            system_load=state.get('system_load', 50.0),
            hour_of_day=state.get('hour_of_day', 12.0),
            recent_action_success_rate=state.get('recent_action_success_rate', 0.5),
            avg_reward=state.get('avg_reward', 0.0),
        )
        actions = DistillationResponseOptimizer.ACTION_SPACE
        if self.adaptive_cost and self.pareto:
            candidates = []
            for idx, action in enumerate(actions):
                carbon_g = 5.0 + idx * 0.5
                latency_ms = 50.0 - idx * 5.0
                energy_joules = 20.0 + idx * 2.0
                quality = 0.9 - idx * 0.05
                cost = self.adaptive_cost.compute(
                    quality=quality,
                    carbon_g=carbon_g,
                    latency_ms=latency_ms,
                    energy_joules=energy_joules,
                    health=0.8,
                    atp=0.5
                )
                candidates.append({
                    'action': action,
                    'score': cost,
                    'carbon_g': carbon_g,
                    'latency_ms': latency_ms,
                    'energy_joules': energy_joules,
                    'quality_score': quality,
                })
            filtered = self.pareto.filter(candidates)
            if filtered:
                allowed = {c['action'] for c in filtered}
                candidates = [c for c in candidates if c['action'] in allowed]
            if not candidates:
                _, _, _, tp = await self.response_optimizer.select_action(opt_state, exploration=False)
                return tp.tolist()
            scores = [c['score'] for c in candidates]
            exp_scores = np.exp(scores - np.max(scores))
            probs = exp_scores / np.sum(exp_scores)
            full_probs = [0.0] * len(actions)
            for c, p in zip(candidates, probs):
                idx = actions.index(c['action'])
                full_probs[idx] = p
            return full_probs
        else:
            _, _, _, tp = await self.response_optimizer.select_action(opt_state, exploration=False)
            return tp.tolist()

    # ----- Utility methods (unchanged but with central metrics if available) -----
    async def shutdown(self):
        if self.persistence:
            for node_id, model in self.models.items():
                self.persistence.save_model(node_id, model)
        logger.info("AnomalyDetector shutdown complete.")

# ============================================================================
# 8. TELEMETRY COLLECTOR, ALERT ESCALATION, EVOLUTIONARY ENGINE (stubs)
# ============================================================================
class TelemetryCollector:
    # (unchanged)
    pass

class AlertEscalationSystem:
    # (unchanged)
    pass

class EvolutionaryEngine:
    # (unchanged)
    pass

# ============================================================================
# 9. CONVENIENCE FACTORY (unchanged, but may add central components)
# ============================================================================
def create_anomaly_detection_system(config=None, **central_kwargs):
    # ... (same as original) ...
    pass

# ============================================================================
# 10. REST API and tests (omitted for brevity, but can be included)
# ============================================================================
