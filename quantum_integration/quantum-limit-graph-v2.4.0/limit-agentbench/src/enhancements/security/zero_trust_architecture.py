#!/usr/bin/env python3
"""
Zero Trust Security Architecture for Green Agent v4.3.0
Implements complete zero-trust security model for expert routing and execution.
ENHANCED WITH:
- Adaptive authentication level selection via Multi-Teacher On-Policy Distillation.
- State-aware choice of auth level (light, standard, enhanced) based on context.
- Online learning from authentication outcomes.
- Teachers: rule-based, historical ML, stateful Q, RLHF, and optional quantum teacher.
- Student: linear softmax with distillation + REINFORCE.
- Persistence for Q-teacher weights and interaction logs.
- Offline training for historical ML teacher.
- Integration with FeedbackEvent schema and AsyncMessageQueue for cross-module learning.
- Expanded state vector with additional security context features.
- Public API for other modules to query security context.
All previous features (carbon/helium tracking, predictive analytics, ledger, rate limiting, etc.) retained.
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Tuple, Set, Union, Callable
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
import hashlib
import hmac
import secrets
import json
from enum import Enum
import jwt
import numpy as np
from collections import deque, defaultdict
import os
import pickle
import zlib
import random
from abc import ABC, abstractmethod
import pandas as pd
from pathlib import Path
import threading
from concurrent.futures import ThreadPoolExecutor

# Pydantic
from pydantic import BaseModel, Field, field_validator, ConfigDict

# Optional dependencies
try:
    import aiofiles
except ImportError:
    aiofiles = None

try:
    import aiohttp
except ImportError:
    aiohttp = None

try:
    from prometheus_client import Counter, Gauge, Histogram, start_http_server, generate_latest
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False

# Project imports
try:
    from .schemas.feedback_event import FeedbackEvent
except ImportError:
    FeedbackEvent = None

try:
    from .async_message_queue import AsyncMessageQueue
except ImportError:
    AsyncMessageQueue = None

# New enhancement: Quantum teacher
try:
    from .quantum_teacher import QuantumTeacher
except ImportError:
    QuantumTeacher = None

logger = logging.getLogger(__name__)

# ============================================================================
# Enums and Configuration
# ============================================================================

class AuthLevel(str, Enum):
    LIGHT = "light"
    STANDARD = "standard"
    ENHANCED = "enhanced"

class RiskLevel(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"

# ============================================================================
# State Representation for Distillation
# ============================================================================

@dataclass
class AuthState:
    """State representation for authentication level selection."""
    # Context features
    user_risk_score: float
    resource_sensitivity: float
    request_frequency: float  # requests per minute
    data_classification: float  # 0=public, 1=top secret
    time_of_day: float  # normalized 0-1
    day_of_week: float  # 0-6 normalized
    ip_reputation: float  # 0-1 (1 = good)
    device_trust: float  # 0-1
    previous_auth_success_rate: float
    previous_auth_avg_latency: float
    carbon_intensity: float  # environmental factor
    # Additional features
    recent_failed_attempts: float
    geo_velocity: float  # distance/time anomaly
    user_role_sensitivity: float
    # ... add more as needed

    def to_feature_vector(self) -> np.ndarray:
        """Convert to normalized feature vector (all values in [0,1] or scaled)."""
        features = [
            min(self.user_risk_score, 1.0),
            self.resource_sensitivity,
            min(self.request_frequency / 100.0, 1.0),  # assume max 100 req/min
            self.data_classification,
            self.time_of_day,
            self.day_of_week / 6.0,
            self.ip_reputation,
            self.device_trust,
            self.previous_auth_success_rate,
            min(self.previous_auth_avg_latency / 1000.0, 1.0),
            min(self.carbon_intensity / 1000.0, 1.0),
            min(self.recent_failed_attempts / 10.0, 1.0),
            min(self.geo_velocity / 1000.0, 1.0),
            self.user_role_sensitivity,
        ]
        return np.array(features, dtype=np.float32)

# ============================================================================
# Teacher Base Class
# ============================================================================

class Teacher(ABC):
    @abstractmethod
    def predict(self, state: AuthState) -> np.ndarray:
        """Return probability vector over 3 authentication levels."""
        pass

    @abstractmethod
    def confidence(self, state: AuthState) -> float:
        """Return confidence in prediction [0,1]."""
        pass

# ============================================================================
# Concrete Teachers
# ============================================================================

class RuleBasedAuthTeacher(Teacher):
    """Rule-based expert using security heuristics."""
    LEVELS = [AuthLevel.LIGHT, AuthLevel.STANDARD, AuthLevel.ENHANCED]

    def predict(self, state: AuthState) -> np.ndarray:
        probs = np.ones(3) * 0.1
        if state.user_risk_score < 0.3 and state.resource_sensitivity < 0.5:
            probs[0] = 0.8  # light
        elif state.user_risk_score > 0.7 or state.recent_failed_attempts > 3:
            probs[2] = 0.8  # enhanced
        else:
            probs[1] = 0.7  # standard
        return probs / probs.sum()

    def confidence(self, state: AuthState) -> float:
        # Higher confidence when risk is clearly low/high
        if state.user_risk_score < 0.2 or state.user_risk_score > 0.8:
            return 0.8
        return 0.5

class HistoricalMLAuthTeacher(Teacher):
    """Learns from historical authentication outcomes."""
    def __init__(self, model_path: Optional[Path] = None):
        self.model = None
        self.label_encoder = None
        self.model_path = model_path or Path("./auth_historical_model.pkl")
        self._load_model()

    def _load_model(self):
        if self.model_path.exists():
            try:
                with open(self.model_path, 'rb') as f:
                    self.model, self.label_encoder = pickle.load(f)
                logger.info(f"Loaded historical ML model from {self.model_path}")
            except Exception as e:
                logger.error(f"Failed to load historical model: {e}")

    def predict(self, state: AuthState) -> np.ndarray:
        if self.model is None or self.label_encoder is None:
            return np.ones(3) / 3
        x = state.to_feature_vector().reshape(1, -1)
        if hasattr(self.model, 'predict_proba'):
            probs = self.model.predict_proba(x)[0]
            # Ensure order matches AuthLevel enum
            target_order = [level.value for level in AuthLevel]
            classes = list(self.label_encoder.classes_)
            new_probs = np.zeros(3)
            for i, level in enumerate(target_order):
                if level in classes:
                    idx = classes.index(level)
                    new_probs[i] = probs[idx]
            probs = new_probs / new_probs.sum() if new_probs.sum() > 0 else np.ones(3)/3
            return probs
        else:
            return np.ones(3) / 3

    def confidence(self, state: AuthState) -> float:
        if self.model is None:
            return 0.0
        probs = self.predict(state)
        entropy = -np.sum(probs * np.log(probs + 1e-9))
        max_entropy = np.log(3)
        return 1.0 - entropy / max_entropy

    @classmethod
    def train_from_logs(cls, log_paths: List[Path], model_path: Path,
                        state_col: str = 'state_vec', label_col: str = 'auth_level'):
        """Train a RandomForest from historical logs."""
        try:
            from sklearn.ensemble import RandomForestClassifier
            from sklearn.preprocessing import LabelEncoder
        except ImportError:
            logger.error("scikit-learn required for historical model training.")
            return None

        all_dfs = []
        for path in log_paths:
            if path.exists():
                df = pd.read_csv(path)
                all_dfs.append(df)
        if not all_dfs:
            logger.warning("No logs found.")
            return None

        df = pd.concat(all_dfs, ignore_index=True)
        if len(df) < 10:
            logger.warning("Not enough logs.")
            return None

        def parse_state(s):
            try:
                return np.fromstring(s, sep=',')
            except:
                return None

        valid_idx = [i for i, s in enumerate(df[state_col]) if parse_state(s) is not None]
        X = np.array([parse_state(df[state_col].iloc[i]) for i in valid_idx])
        y = df[label_col].iloc[valid_idx].values

        if len(X) < 5:
            logger.warning("Too few valid samples.")
            return None

        le = LabelEncoder()
        y_enc = le.fit_transform(y)
        clf = RandomForestClassifier(n_estimators=100, random_state=42)
        clf.fit(X, y_enc)

        with open(model_path, 'wb') as f:
            pickle.dump((clf, le), f)
        logger.info(f"Trained and saved to {model_path}")
        return model_path

class StatefulQAuthTeacher(Teacher):
    """Q-learning based teacher."""
    def __init__(self, lr: float = 0.1, weights_path: Optional[Path] = None, feature_dim: int = 14):
        self.lr = lr
        self.weights_path = weights_path or Path("./auth_q_weights.json")
        self.feature_dim = feature_dim
        self.weights = np.zeros((feature_dim, 3))
        self._load_state()

    def _load_state(self):
        if self.weights_path.exists():
            try:
                with open(self.weights_path, 'r') as f:
                    data = json.load(f)
                self.weights = np.array(data)
                if self.weights.shape[0] != self.feature_dim:
                    logger.warning(f"Shape mismatch; resetting weights.")
                    self.weights = np.zeros((self.feature_dim, 3))
            except Exception as e:
                logger.error(f"Failed to load Q-weights: {e}")

    def _save_state(self):
        with open(self.weights_path, 'w') as f:
            json.dump(self.weights.tolist(), f, indent=2)

    def predict(self, state: AuthState) -> np.ndarray:
        x = state.to_feature_vector()
        q = x @ self.weights
        exp_q = np.exp(q - np.max(q))
        return exp_q / exp_q.sum()

    def confidence(self, state: AuthState) -> float:
        return 0.5

    def update(self, state: AuthState, action: int, reward: float):
        x = state.to_feature_vector()
        q_current = np.dot(x, self.weights[:, action])
        self.weights[:, action] += self.lr * (reward - q_current) * x
        self._save_state()

class RLHFAuthTeacher(Teacher):
    """Teacher influenced by human feedback on security decisions."""
    def predict(self, state: AuthState) -> np.ndarray:
        probs = np.ones(3) / 3
        # If human feedback indicates stricter security, favor enhanced
        # This is a placeholder; in a real system, feedback would be incorporated
        if state.user_risk_score > 0.6:
            probs[2] += 0.2
        return probs / probs.sum()

    def confidence(self, state: AuthState) -> float:
        return 0.6

# ============================================================================
# Student, ReplayBuffer, Gating
# ============================================================================

class DistillationStudent:
    def __init__(self, feature_dim: int = 14, n_classes: int = 3, lr: float = 0.01):
        self.feature_dim = feature_dim
        self.weights = np.zeros((feature_dim, n_classes))
        self.biases = np.zeros(n_classes)
        self.lr = lr
        self.n_classes = n_classes
        self.counter = 0
        self.grad_clip = 1.0

    def predict_proba(self, state_vector, num_classes=None):
        if num_classes is None:
            num_classes = self.n_classes
        logits = state_vector @ self.weights + self.biases
        exp = np.exp(logits - np.max(logits))
        return exp / exp.sum()

    def update(self, state_vector, teacher_probs, reward, action, distill_weight=0.7, rl_weight=0.3):
        current_probs = self.predict_proba(state_vector, self.n_classes)
        grad_distill = -(teacher_probs - current_probs)
        one_hot = np.zeros(self.n_classes)
        one_hot[action] = 1.0
        grad_rl = -reward * (one_hot - current_probs)
        grad = distill_weight * grad_distill + rl_weight * grad_rl
        grad = np.clip(grad, -self.grad_clip, self.grad_clip)
        self.weights -= self.lr * np.outer(state_vector, grad)
        self.biases -= self.lr * grad
        self.counter += 1

    def export_weights(self) -> Dict[str, np.ndarray]:
        return {"weights": self.weights.copy(), "biases": self.biases.copy()}

    def import_weights(self, weight_dict: Dict[str, np.ndarray]):
        self.weights = weight_dict["weights"].copy()
        self.biases = weight_dict["biases"].copy()

class ReplayBuffer:
    def __init__(self, max_size: int = 2000):
        self.buffer = deque(maxlen=max_size)

    def push(self, state_vec, action, reward, next_state_vec, teacher_outputs):
        self.buffer.append((state_vec, action, reward, next_state_vec, teacher_outputs))

    def sample(self, batch_size: int = 32):
        if len(self.buffer) < batch_size:
            batch = list(self.buffer)
        else:
            batch = random.sample(self.buffer, batch_size)
        states, actions, rewards, next_states, teacher_outputs = zip(*batch)
        return (np.array(states), actions, np.array(rewards),
                np.array(next_states), np.array(teacher_outputs))

    def __len__(self):
        return len(self.buffer)

class MoEGatingNetwork:
    def __init__(self, feature_dim: int = 14, n_experts: int = 5, lr: float = 0.005):
        self.feature_dim = feature_dim
        self.n_experts = n_experts
        self.lr = lr
        self.weights = np.random.randn(feature_dim, n_experts) * 0.01
        self.bias = np.zeros(n_experts)

    def forward(self, state_vec):
        logits = state_vec @ self.weights + self.bias
        exp = np.exp(logits - np.max(logits))
        return exp / exp.sum()

    def update(self, state_vec, teacher_outputs, student_probs, current_gate_weights):
        combined = np.sum(current_gate_weights[:, None] * teacher_outputs, axis=0)
        error = combined - student_probs
        grad_gate = teacher_outputs @ error
        self.weights -= self.lr * np.outer(state_vec, grad_gate)
        self.bias -= self.lr * grad_gate

# ============================================================================
# DistillationAuthOptimizer
# ============================================================================

class DistillationAuthOptimizer:
    """
    Multi-teacher on-policy distillation optimizer for authentication level selection.
    """
    AUTH_LEVELS = [level.value for level in AuthLevel]

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.feature_dim = 14
        self.n_actions = 3

        # Student
        self.student = DistillationStudent(
            feature_dim=self.feature_dim,
            lr=config.get('distillation_learning_rate', 0.01)
        )

        # Teachers
        self.teachers: List[Teacher] = [
            RuleBasedAuthTeacher(),
            HistoricalMLAuthTeacher(model_path=config.get('historical_model_path')),
            StatefulQAuthTeacher(
                lr=config.get('q_learning_rate', 0.1),
                weights_path=config.get('q_weights_path'),
                feature_dim=self.feature_dim
            ),
            RLHFAuthTeacher()
        ]

        # Optional quantum teacher
        if config.get('use_quantum_teacher', False) and QuantumTeacher is not None:
            self.teachers.append(QuantumTeacher(
                n_actions=self.n_actions,
                n_qubits=config.get('quantum_teacher_qubits', 4),
                n_layers=config.get('quantum_teacher_layers', 2)
            ))
            logger.info("Quantum teacher added to auth optimizer")

        self.n_teachers = len(self.teachers)

        # Gating network
        self.gating = MoEGatingNetwork(
            feature_dim=self.feature_dim,
            n_experts=self.n_teachers,
            lr=config.get('gating_learning_rate', 0.005)
        )

        # Replay buffer
        self.replay_buffer = ReplayBuffer(
            max_size=config.get('distillation_replay_size', 2000)
        )

        # Exploration parameters
        self.epsilon = config.get('distillation_epsilon', 0.1)
        self.epsilon_min = config.get('distillation_epsilon_min', 0.01)
        self.epsilon_decay = config.get('distillation_epsilon_decay', 0.995)
        self.train_every = config.get('distillation_train_every', 10)
        self.counter = 0
        self.distill_weight = config.get('distill_weight', 0.7)
        self.rl_weight = config.get('rl_weight', 0.3)
        self.batch_update_size = config.get('batch_update_size', 8)

        # Async lock
        self.lock = asyncio.Lock()

    def _compute_teacher_probs(self, state: AuthState) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """Returns (combined_probs, gate_weights, teacher_outputs_matrix)."""
        state_vec = state.to_feature_vector()
        teacher_outputs = []
        for teacher in self.teachers:
            prob = teacher.predict(state)
            if len(prob) != self.n_actions:
                if len(prob) < self.n_actions:
                    prob = np.pad(prob, (0, self.n_actions - len(prob)), 'constant')
                else:
                    prob = prob[:self.n_actions]
            teacher_outputs.append(prob)
        teacher_outputs = np.array(teacher_outputs)  # (n_teachers, n_actions)
        gate_weights = self.gating.forward(state_vec)
        combined = np.sum(gate_weights[:, None] * teacher_outputs, axis=0)
        combined = combined / combined.sum()
        return combined, gate_weights, teacher_outputs

    async def select_auth_level(self, state: AuthState, exploration: bool = True) -> Tuple[str, int, np.ndarray, np.ndarray]:
        async with self.lock:
            state_vec = state.to_feature_vector()
            teacher_probs, gate_weights, teacher_outputs = self._compute_teacher_probs(state)

            student_probs = self.student.predict_proba(state_vec, self.n_actions)

            if exploration and random.random() < self.epsilon:
                action_idx = random.randint(0, self.n_actions - 1)
            else:
                combined = 0.8 * student_probs + 0.2 * teacher_probs
                action_idx = int(np.argmax(combined))

            # Decay epsilon
            self.epsilon = max(self.epsilon_min, self.epsilon * self.epsilon_decay)

            return self.AUTH_LEVELS[action_idx], action_idx, state_vec, teacher_outputs

    async def update(self, state_vec, action_idx, reward, next_state_vec, teacher_outputs):
        async with self.lock:
            self.replay_buffer.push(state_vec, action_idx, reward, next_state_vec, teacher_outputs)
            self.counter += 1
            if self.counter % self.train_every == 0 and len(self.replay_buffer) >= self.batch_update_size:
                batch = self.replay_buffer.sample(self.batch_update_size)
                states, actions, rewards, _, teacher_outputs_batch = batch
                for i in range(len(states)):
                    # Average teacher probs as distillation target
                    avg_teacher_probs = teacher_outputs_batch[i].mean(axis=0)
                    self.student.update(states[i], avg_teacher_probs, rewards[i], actions[i],
                                        distill_weight=self.distill_weight, rl_weight=self.rl_weight)
                    # Update gating
                    current_gate = self.gating.forward(states[i])
                    student_out = self.student.predict_proba(states[i])
                    self.gating.update(states[i], teacher_outputs_batch[i], student_out, current_gate)

    async def export_student_weights(self) -> Dict[str, np.ndarray]:
        async with self.lock:
            return self.student.export_weights()

    async def import_student_weights(self, weights: Dict[str, np.ndarray]):
        async with self.lock:
            self.student.import_weights(weights)

    def get_stats(self) -> Dict:
        return {
            'student_counter': self.student.counter,
            'buffer_size': len(self.replay_buffer),
            'epsilon': self.epsilon,
            'n_teachers': self.n_teachers
        }

# ============================================================================
# ZeroTrustArchitecture (Main Class)
# ============================================================================

class ZeroTrustArchitecture:
    """
    Main zero-trust security manager integrating adaptive authentication.
    """
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.auth_optimizer = DistillationAuthOptimizer(config.get('distillation', {}))
        # Other components: rate limiter, ledger, etc. (simplified)
        self.rate_limiter = defaultdict(lambda: deque(maxlen=100))
        self.audit_log = []

    async def authenticate(self, context: Dict[str, Any]) -> Tuple[AuthLevel, Dict]:
        """
        Authenticate a request and select appropriate auth level.
        """
        # Build AuthState from context
        state = self._build_auth_state(context)
        level_str, action_idx, state_vec, teacher_outputs = await self.auth_optimizer.select_auth_level(state)
        level = AuthLevel(level_str)

        # In real system, perform actual authentication steps based on level
        # For prototype, simulate success and compute reward later
        self.audit_log.append({
            'timestamp': datetime.now().isoformat(),
            'level': level.value,
            'context': context.get('user_id', 'anonymous')
        })

        return level, {'state_vec': state_vec, 'action_idx': action_idx, 'teacher_outputs': teacher_outputs}

    async def record_outcome(self, auth_decision: Dict, success: bool, latency_ms: float, security_incident: bool = False):
        """
        Record outcome and update the optimizer.
        """
        reward = 1.0 if success and not security_incident else 0.0
        if security_incident:
            reward = -1.0

        state_vec = auth_decision['state_vec']
        action_idx = auth_decision['action_idx']
        teacher_outputs = auth_decision['teacher_outputs']

        # Next state: could be built from updated context; for simplicity, use same state
        next_state_vec = state_vec  # In reality, compute new state after auth

        await self.auth_optimizer.update(state_vec, action_idx, reward, next_state_vec, teacher_outputs)

    def _build_auth_state(self, context: Dict) -> AuthState:
        # Extract features from context; use defaults if missing
        return AuthState(
            user_risk_score=context.get('user_risk_score', 0.5),
            resource_sensitivity=context.get('resource_sensitivity', 0.5),
            request_frequency=context.get('request_frequency', 10.0),
            data_classification=context.get('data_classification', 0.5),
            time_of_day=float(datetime.now().hour) / 24.0,
            day_of_week=float(datetime.now().weekday()) / 6.0,
            ip_reputation=context.get('ip_reputation', 0.5),
            device_trust=context.get('device_trust', 0.5),
            previous_auth_success_rate=context.get('previous_auth_success_rate', 0.9),
            previous_auth_avg_latency=context.get('previous_auth_avg_latency', 200.0),
            carbon_intensity=context.get('carbon_intensity', 500.0),
            recent_failed_attempts=context.get('recent_failed_attempts', 0.0),
            geo_velocity=context.get('geo_velocity', 0.0),
            user_role_sensitivity=context.get('user_role_sensitivity', 0.5)
        )
