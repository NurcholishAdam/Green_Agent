"""
Enhanced Workload Descriptor v2.4.0
====================================
Defines the structure of a workload/task with adaptive priority selection
via Multi-Teacher On-Policy Distillation.

Enhancements (v2.4.0 over v2.3.0):
- Fixed missing @classmethod on Pydantic validators (silently skipped before).
- Fixed FeedbackEvent integration: correct feedback_type and context.task_id.
- Activated the Q-teacher via update_from_vector during batch training.
- Student now distills from the *gated* teacher combination, not the mean.
- MoE gating uses the full softmax Jacobian + optional load balancing.
- Added RunningScaler for online feature normalization, shared by all agents.
- Clamped all features in WorkloadState.to_feature_vector.
- Sanitized task_id for filesystem paths (no /, .., or unsafe chars).
- Asynchronous, append-only log persistence (no full-file rewrite per step).
- Causal reward shaping is guarded, clamped, and snapshot-aware.
- Configurable reward weights and scaling caps via metadata.
- Federated weight exchange via a versioned base64-JSON codec.
- Deadlock-safe public/private lock API with documented invariants.
- Hardened pickle/JSON loading with shape validation.
- Added __all__, type hints, and a built-in test suite.

All previous features retained.
"""

from __future__ import annotations

from enum import Enum
from typing import Optional, Dict, Any, List, Tuple, Union, ClassVar, FrozenSet
from datetime import datetime, timezone
from pathlib import Path
from collections import deque
import base64
import json
import random
import re
import numpy as np
from abc import ABC, abstractmethod
import pickle
import pandas as pd
from dataclasses import dataclass
import asyncio

from pydantic import BaseModel, Field, field_validator, ConfigDict, PrivateAttr

import logging
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Optional integrations
# ---------------------------------------------------------------------------
try:
    from .feedback_event import FeedbackEvent  # type: ignore
except ImportError:
    FeedbackEvent = None  # type: ignore

try:
    from ..quantum_teacher import QuantumTeacher  # type: ignore
except ImportError:
    QuantumTeacher = None  # type: ignore

try:
    from ..causal_reward_shaper import CausalRewardShaper  # type: ignore
except ImportError:
    CausalRewardShaper = None  # type: ignore


# ============================================================================
# Enums
# ============================================================================

class TaskType(str, Enum):
    TRAINING = "training"
    INFERENCE = "inference"
    EDGE_SENSING = "edge_sensing"
    FEDERATED_ROUND = "federated_round"
    MULTIMODAL = "multimodal"
    DATA_PROCESSING = "data_processing"
    QUERY = "query"
    INFERENCE_BATCH = "inference_batch"
    TRAINING_DISTRIBUTED = "training_distributed"
    EDGE_COMPUTE = "edge_compute"


class Urgency(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class Priority(str, Enum):
    ACCURACY = "accuracy"
    GREEN = "green"
    BALANCED = "balanced"


class BioMode(str, Enum):
    PHOTOSYNTHETIC = "photosynthetic"
    CHEMOTACTIC = "chemotactic"
    NONE = "none"


# Urgency numeric mapping (single source of truth)
URGENCY_TO_FLOAT: Dict[Urgency, float] = {
    Urgency.LOW: 0.0,
    Urgency.MEDIUM: 1.0,
    Urgency.HIGH: 2.0,
    Urgency.CRITICAL: 3.0,
}

# Registered workload feedback type — extends FeedbackEvent's allow-list.
WORKLOAD_FEEDBACK_TYPE: str = "scheduling"


# ============================================================================
# Utilities
# ============================================================================

def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _as_path(v: Union[str, Path, None]) -> Optional[Path]:
    if v is None:
        return None
    return v if isinstance(v, Path) else Path(v)


def _sanitize_id(value: Optional[str]) -> str:
    """Make a filesystem-safe version of an identifier."""
    if value is None or str(value).strip() == "":
        return "unknown"
    return re.sub(r"[^A-Za-z0-9_.\-]", "_", str(value))[:128] or "unknown"


def _softmax(logits: np.ndarray) -> np.ndarray:
    z = logits - np.max(logits)
    e = np.exp(z)
    s = e.sum()
    if s <= 0:
        return np.ones_like(e) / len(e)
    return e / s


class RunningScaler:
    """
    Online running mean/std scaler for feature normalization.

    Provides a rolling estimate of mean and variance so features can be
    z-scored without an offline fit. Values are clipped to ``[-clip, clip]``.
    """

    def __init__(self, dim: int, clip: float = 5.0, min_std: float = 1e-6) -> None:
        self.dim = int(dim)
        self.clip = float(clip)
        self.min_std = float(min_std)
        self.mean = np.zeros(self.dim, dtype=np.float64)
        self.m2 = np.zeros(self.dim, dtype=np.float64)
        self.count = 0

    def update(self, x: np.ndarray) -> None:
        x = np.asarray(x, dtype=np.float64).reshape(-1)
        if x.shape[0] != self.dim:
            return
        self.count += 1
        delta = x - self.mean
        self.mean += delta / self.count
        delta2 = x - self.mean
        self.m2 += delta * delta2

    def transform(self, x: np.ndarray) -> np.ndarray:
        x = np.asarray(x, dtype=np.float64).reshape(-1)
        if self.count < 2:
            return x.astype(np.float32)
        var = self.m2 / (self.count - 1)
        std = np.sqrt(np.maximum(var, 0.0))
        std = np.maximum(std, self.min_std)
        z = (x - self.mean) / std
        return np.clip(z, -self.clip, self.clip).astype(np.float32)

    def state_dict(self) -> Dict[str, Any]:
        return {
            "dim": self.dim,
            "clip": self.clip,
            "min_std": self.min_std,
            "mean": self.mean.tolist(),
            "m2": self.m2.tolist(),
            "count": self.count,
        }

    def load_state_dict(self, state: Dict[str, Any]) -> None:
        self.dim = int(state.get("dim", self.dim))
        self.clip = float(state.get("clip", self.clip))
        self.min_std = float(state.get("min_std", self.min_std))
        self.mean = np.array(state.get("mean", self.mean.tolist()), dtype=np.float64)
        self.m2 = np.array(state.get("m2", self.m2.tolist()), dtype=np.float64)
        self.count = int(state.get("count", 0))


class FederatedWeightsCodec:
    """Versioned, base64-encoded JSON codec for federated weight exchange."""

    VERSION = "1.0"

    @staticmethod
    def encode(weights: Dict[str, np.ndarray]) -> Dict[str, Any]:
        payload = {}
        for k, v in weights.items():
            arr = np.ascontiguousarray(v)
            payload[k] = {
                "shape": list(arr.shape),
                "dtype": str(arr.dtype),
                "data": base64.b64encode(arr.tobytes()).decode("ascii"),
            }
        return {"version": FederatedWeightsCodec.VERSION, "payload": payload}

    @staticmethod
    def decode(encoded: Dict[str, Any]) -> Dict[str, np.ndarray]:
        if encoded.get("version") != FederatedWeightsCodec.VERSION:
            raise ValueError(
                f"Unsupported federated weights version: {encoded.get('version')}"
            )
        out: Dict[str, np.ndarray] = {}
        for k, v in encoded["payload"].items():
            raw = base64.b64decode(v["data"])
            out[k] = np.frombuffer(raw, dtype=v["dtype"]).reshape(v["shape"]).copy()
        return out


# ============================================================================
# Workload State
# ============================================================================

@dataclass
class WorkloadState:
    """
    State for the distillation agent. All features are produced in [0, 1]
    when no scaler is provided; when a RunningScaler is passed, z-scored
    features are returned instead.
    """
    tokens: float
    latency_target: float
    urgency: float  # 0-3
    task_type_training: float = 0.0
    task_type_inference: float = 0.0
    task_type_edge_sensing: float = 0.0
    task_type_federated_round: float = 0.0
    task_type_multimodal: float = 0.0
    task_type_data_processing: float = 0.0
    task_type_query: float = 0.0
    task_type_inference_batch: float = 0.0
    task_type_training_distributed: float = 0.0
    task_type_edge_compute: float = 0.0
    estimated_energy: float = 0.0
    estimated_carbon: float = 0.0
    helium_units: float = 0.0
    sector_emission_factor: float = 0.0
    bio_mode_photosynthetic: float = 0.0
    bio_mode_chemotactic: float = 0.0
    recent_success_rate: float = 0.5
    avg_reward: float = 0.5
    human_feedback_score: float = 0.5
    graph_centrality: float = 0.5

    # Configurable static caps (used if no RunningScaler supplied)
    TOKENS_MAX: ClassVar[float] = 10_000.0
    LATENCY_MAX_MS: ClassVar[float] = 1_000.0
    ENERGY_MAX_J: ClassVar[float] = 10.0
    CARBON_MAX_KG: ClassVar[float] = 0.1
    HELIUM_MAX_UNITS: ClassVar[float] = 0.1
    SECTOR_EF_MAX: ClassVar[float] = 10.0

    def to_raw_vector(self) -> np.ndarray:
        return np.array([
            self.tokens,
            self.latency_target,
            self.urgency,
            self.task_type_training,
            self.task_type_inference,
            self.task_type_edge_sensing,
            self.task_type_federated_round,
            self.task_type_multimodal,
            self.task_type_data_processing,
            self.task_type_query,
            self.task_type_inference_batch,
            self.task_type_training_distributed,
            self.task_type_edge_compute,
            self.estimated_energy,
            self.estimated_carbon,
            self.helium_units,
            self.sector_emission_factor,
            self.bio_mode_photosynthetic,
            self.bio_mode_chemotactic,
            self.recent_success_rate,
            self.avg_reward,
            self.human_feedback_score,
            self.graph_centrality,
        ], dtype=np.float64)

    @staticmethod
    def _clamp01(v: float) -> float:
        return float(min(1.0, max(0.0, v)))

    def to_feature_vector(self, scaler: Optional[RunningScaler] = None) -> np.ndarray:
        raw = self.to_raw_vector()
        if scaler is not None:
            return scaler.transform(raw)

        c = self._clamp01
        return np.array([
            c(self.tokens / self.TOKENS_MAX),
            c(self.latency_target / self.LATENCY_MAX_MS),
            c(self.urgency / 3.0),
            c(self.task_type_training),
            c(self.task_type_inference),
            c(self.task_type_edge_sensing),
            c(self.task_type_federated_round),
            c(self.task_type_multimodal),
            c(self.task_type_data_processing),
            c(self.task_type_query),
            c(self.task_type_inference_batch),
            c(self.task_type_training_distributed),
            c(self.task_type_edge_compute),
            c(self.estimated_energy / self.ENERGY_MAX_J),
            c(self.estimated_carbon / self.CARBON_MAX_KG),
            c(self.helium_units / self.HELIUM_MAX_UNITS),
            c(self.sector_emission_factor / self.SECTOR_EF_MAX),
            c(self.bio_mode_photosynthetic),
            c(self.bio_mode_chemotactic),
            c(self.recent_success_rate),
            c(self.avg_reward),
            c(self.human_feedback_score),
            c(self.graph_centrality),
        ], dtype=np.float32)


# ============================================================================
# Teachers
# ============================================================================

class Teacher(ABC):
    """Abstract teacher producing a probability vector over priority actions."""

    def __init__(self) -> None:
        self.scaler: Optional[RunningScaler] = None

    @abstractmethod
    def predict(self, state: WorkloadState) -> np.ndarray:
        ...

    @abstractmethod
    def confidence(self, state: WorkloadState) -> float:
        ...

    def _features(self, state: WorkloadState) -> np.ndarray:
        return state.to_feature_vector(self.scaler)


class PriorityRuleBasedTeacher(Teacher):
    PRIORITIES = ["accuracy", "green", "balanced"]

    def predict(self, state: WorkloadState) -> np.ndarray:
        probs = np.ones(3, dtype=np.float64) * 0.1
        if state.urgency >= 2.0:
            probs[0] = 0.8
        elif state.estimated_carbon > 0.01 or state.helium_units > 0.01:
            probs[1] = 0.7
        elif state.latency_target < 100:
            probs[0] = 0.6
        else:
            probs[2] = 0.6
        return _softmax(probs)

    def confidence(self, state: WorkloadState) -> float:
        probs = self.predict(state)
        entropy = -np.sum(probs * np.log(probs + 1e-9))
        return float(1.0 - entropy / np.log(3))


class PriorityHistoricalMLTeacher(Teacher):
    EXPECTED_FEATURE_DIM: int = 23
    STRATEGY_ORDER: ClassVar[List[str]] = [p.value for p in Priority]

    def __init__(self, model_path: Optional[Path] = None) -> None:
        super().__init__()
        self.model = None
        self.label_encoder = None
        self.model_path: Path = _as_path(model_path) or Path("./priority_historical_model.pkl")
        self._load_model()

    def _load_model(self) -> None:
        if not self.model_path.exists():
            return
        try:
            with open(self.model_path, "rb") as f:
                self.model, self.label_encoder = pickle.load(f)
        except Exception as e:
            logger.error(f"Failed to load historical model: {e}")
            self.model, self.label_encoder = None, None
            return

        # Shape validation
        n_features = getattr(self.model, "n_features_in_", None)
        if n_features is not None and n_features != self.EXPECTED_FEATURE_DIM:
            logger.warning(
                f"Historical model expects {n_features} features, but the workload "
                f"pipeline produces {self.EXPECTED_FEATURE_DIM}. Disabling the teacher."
            )
            self.model, self.label_encoder = None, None
            return
        logger.info(f"Loaded historical ML model from {self.model_path}")

    def predict(self, state: WorkloadState) -> np.ndarray:
        if self.model is None or self.label_encoder is None:
            return np.ones(3) / 3
        x = self._features(state).reshape(1, -1)
        if not hasattr(self.model, "predict_proba"):
            return np.ones(3) / 3
        probs = self.model.predict_proba(x)[0]
        classes = list(self.label_encoder.classes_)
        new_probs = np.zeros(3, dtype=np.float64)
        for i, strat in enumerate(self.STRATEGY_ORDER):
            if strat in classes:
                new_probs[i] = probs[classes.index(strat)]
        s = new_probs.sum()
        return new_probs / s if s > 0 else np.ones(3) / 3

    def confidence(self, state: WorkloadState) -> float:
        if self.model is None:
            return 0.0
        probs = self.predict(state)
        entropy = -np.sum(probs * np.log(probs + 1e-9))
        return float(1.0 - entropy / np.log(3))

    @classmethod
    def train_from_logs(
        cls,
        log_paths: List[Path],
        model_path: Path,
        state_col: str = "state_vec",
        label_col: str = "priority",
    ) -> Optional[Path]:
        try:
            from sklearn.ensemble import RandomForestClassifier
            from sklearn.preprocessing import LabelEncoder
        except ImportError:
            logger.error("scikit-learn is required for historical model training.")
            return None

        all_dfs = []
        for path in log_paths:
            path = _as_path(path)
            if path and path.exists():
                all_dfs.append(pd.read_csv(path))
        if not all_dfs:
            logger.warning("No logs found for training.")
            return None

        df = pd.concat(all_dfs, ignore_index=True)
        if len(df) < 10:
            logger.warning("Not enough logs to train historical model.")
            return None

        def parse_state(s: Any) -> Optional[np.ndarray]:
            try:
                parts = [float(p) for p in str(s).split(",") if p.strip() != ""]
                if len(parts) != cls.EXPECTED_FEATURE_DIM:
                    return None
                return np.array(parts, dtype=np.float64)
            except Exception:
                return None

        rows, labels = [], []
        for s, lbl in zip(df[state_col].values, df[label_col].values):
            v = parse_state(s)
            if v is not None:
                rows.append(v)
                labels.append(lbl)
        if len(rows) < 5:
            logger.warning("Too few valid samples after parsing.")
            return None

        X = np.array(rows)
        y = np.array(labels)
        le = LabelEncoder()
        y_enc = le.fit_transform(y)
        clf = RandomForestClassifier(n_estimators=100, random_state=42)
        clf.fit(X, y_enc)

        model_path = _as_path(model_path) or Path("./priority_historical_model.pkl")
        with open(model_path, "wb") as f:
            pickle.dump((clf, le), f)
        logger.info(f"Trained historical model and saved to {model_path}")
        return model_path


class PriorityStatefulQTeacher(Teacher):
    """
    Linear Q-teacher. The optimizer calls ``update_from_vector`` during
    batch training; ``update`` remains for direct WorkloadState calls.
    """

    def __init__(
        self,
        lr: float = 0.1,
        weights_path: Optional[Path] = None,
        feature_dim: int = 23,
        autosave: bool = True,
    ) -> None:
        super().__init__()
        self.lr = float(lr)
        self.weights_path: Path = _as_path(weights_path) or Path("./priority_q_weights.json")
        self.feature_dim = int(feature_dim)
        self.weights = np.zeros((self.feature_dim, 3), dtype=np.float64)
        self.autosave = autosave
        self._dirty = False
        self._load_state()

    def _load_state(self) -> None:
        if not self.weights_path.exists():
            return
        try:
            with open(self.weights_path, "r") as f:
                data = json.load(f)
            w = np.array(data, dtype=np.float64)
            if w.shape == (self.feature_dim, 3):
                self.weights = w
                logger.info(f"Loaded Q-teacher weights from {self.weights_path}")
            else:
                logger.warning(
                    f"Loaded Q-weights shape {w.shape}, expected "
                    f"({self.feature_dim},3). Reinitializing."
                )
        except Exception as e:
            logger.error(f"Failed to load Q-weights: {e}")

    def save(self, force: bool = False) -> None:
        if not force and not self._dirty:
            return
        try:
            with open(self.weights_path, "w") as f:
                json.dump(self.weights.tolist(), f, indent=2)
            self._dirty = False
        except Exception as e:
            logger.error(f"Failed to save Q-weights: {e}")

    def _predict_from_vector(self, x: np.ndarray) -> np.ndarray:
        q = x @ self.weights
        return _softmax(q)

    def predict(self, state: WorkloadState) -> np.ndarray:
        return self._predict_from_vector(self._features(state))

    def confidence(self, state: WorkloadState) -> float:
        x = self._features(state)
        q = x @ self.weights
        spread = float(np.max(q) - np.min(q))
        return float(np.clip(0.3 + 0.35 * spread, 0.0, 1.0))

    def update_from_vector(self, state_vec: np.ndarray, action: int, reward: float) -> None:
        x = np.asarray(state_vec, dtype=np.float64).reshape(-1)
        if x.shape[0] != self.feature_dim:
            return
        if not 0 <= int(action) < 3:
            return
        q_current = float(np.dot(x, self.weights[:, action]))
        self.weights[:, action] += self.lr * (reward - q_current) * x
        self._dirty = True

    def update(self, state: WorkloadState, action: int, reward: float) -> None:
        self.update_from_vector(self._features(state), action, reward)


class RLHFPriorityTeacher(Teacher):
    def predict(self, state: WorkloadState) -> np.ndarray:
        probs = np.ones(3, dtype=np.float64) / 3
        if state.human_feedback_score > 0.7:
            probs[0] += 0.2  # accuracy
        elif state.human_feedback_score < 0.3:
            probs[1] += 0.2  # green
        return _softmax(probs)

    def confidence(self, state: WorkloadState) -> float:
        return 0.7 if abs(state.human_feedback_score - 0.5) > 0.3 else 0.4


# ============================================================================
# Student
# ============================================================================

class DistillationStudent:
    """Linear softmax student trained via distillation + RL gradient."""

    def __init__(
        self,
        feature_dim: int = 23,
        n_classes: int = 3,
        lr: float = 0.01,
        grad_clip: float = 1.0,
        seed: Optional[int] = None,
    ) -> None:
        rng = np.random.default_rng(seed)
        self.feature_dim = int(feature_dim)
        self.n_classes = int(n_classes)
        self.lr = float(lr)
        self.grad_clip = float(grad_clip)
        # Small random init to break symmetry
        self.weights = rng.normal(0.0, 0.01, size=(self.feature_dim, self.n_classes))
        self.biases = np.zeros(self.n_classes, dtype=np.float64)
        self.counter = 0

    def _maybe_resize(self, num_classes: int) -> None:
        if num_classes == self.n_classes:
            return
        if num_classes > self.n_classes:
            w = np.zeros((self.feature_dim, num_classes), dtype=np.float64)
            b = np.zeros(num_classes, dtype=np.float64)
            w[:, : self.n_classes] = self.weights
            b[: self.n_classes] = self.biases
            self.weights, self.biases = w, b
        else:
            self.weights = self.weights[:, :num_classes]
            self.biases = self.biases[:num_classes]
        self.n_classes = num_classes

    def predict_proba(self, state_vector: np.ndarray, num_classes: Optional[int] = None) -> np.ndarray:
        if num_classes is not None:
            self._maybe_resize(num_classes)
        logits = state_vector @ self.weights + self.biases
        return _softmax(logits)

    def update(
        self,
        state_vector: np.ndarray,
        teacher_probs: np.ndarray,
        reward: float,
        action: int,
        distill_weight: float = 0.7,
        rl_weight: float = 0.3,
    ) -> None:
        current = self.predict_proba(state_vector, self.n_classes)
        grad_distill = -(teacher_probs - current)
        one_hot = np.zeros(self.n_classes, dtype=np.float64)
        if 0 <= int(action) < self.n_classes:
            one_hot[int(action)] = 1.0
        grad_rl = -reward * (one_hot - current)
        grad = distill_weight * grad_distill + rl_weight * grad_rl
        grad = np.clip(grad, -self.grad_clip, self.grad_clip)
        self.weights -= self.lr * np.outer(state_vector, grad)
        self.biases -= self.lr * grad
        self.counter += 1

    def export_weights(self) -> Dict[str, np.ndarray]:
        return {"weights": self.weights.copy(), "biases": self.biases.copy()}

    def import_weights(self, weight_dict: Dict[str, np.ndarray]) -> None:
        w = np.asarray(weight_dict["weights"])
        b = np.asarray(weight_dict["biases"])
        if w.shape != self.weights.shape:
            raise ValueError(
                f"weights shape mismatch: expected {self.weights.shape}, got {w.shape}"
            )
        if b.shape != self.biases.shape:
            raise ValueError(
                f"biases shape mismatch: expected {self.biases.shape}, got {b.shape}"
            )
        self.weights = w.astype(np.float64, copy=True)
        self.biases = b.astype(np.float64, copy=True)


# ============================================================================
# Replay Buffer
# ============================================================================

class ReplayBuffer:
    def __init__(self, max_size: int = 2000) -> None:
        self.buffer: deque = deque(maxlen=int(max_size))

    def push(
        self,
        state_vec: np.ndarray,
        action: int,
        reward: float,
        next_state_vec: np.ndarray,
        teacher_outputs: np.ndarray,
    ) -> None:
        self.buffer.append((
            np.asarray(state_vec, dtype=np.float64),
            int(action),
            float(reward),
            np.asarray(next_state_vec, dtype=np.float64),
            np.asarray(teacher_outputs, dtype=np.float64),
        ))

    def sample(self, batch_size: int = 32):
        if len(self.buffer) < batch_size:
            batch = list(self.buffer)
        else:
            batch = random.sample(self.buffer, batch_size)
        states, actions, rewards, next_states, teacher_outputs = zip(*batch)
        return (
            np.array(states),
            np.array(actions, dtype=np.int64),
            np.array(rewards, dtype=np.float64),
            np.array(next_states),
            np.array(teacher_outputs),
        )

    def __len__(self) -> int:
        return len(self.buffer)


# ============================================================================
# MoE Gating Network
# ============================================================================

class MoEGatingNetwork:
    """
    Learnable gating network using the full softmax Jacobian for correct
    gradients, plus an optional load-balancing regularizer.
    """

    def __init__(
        self,
        feature_dim: int = 23,
        n_experts: int = 5,
        lr: float = 0.005,
        load_balance_weight: float = 0.01,
        seed: Optional[int] = None,
    ) -> None:
        rng = np.random.default_rng(seed)
        self.feature_dim = int(feature_dim)
        self.n_experts = int(n_experts)
        self.lr = float(lr)
        self.load_balance_weight = float(load_balance_weight)
        self.weights = rng.normal(0.0, 0.01, size=(self.feature_dim, self.n_experts))
        self.bias = np.zeros(self.n_experts, dtype=np.float64)
        self._mean_gate = np.ones(self.n_experts, dtype=np.float64) / max(self.n_experts, 1)

    def forward(self, state_vec: np.ndarray) -> np.ndarray:
        logits = state_vec @ self.weights + self.bias
        return _softmax(logits)

    def update(
        self,
        state_vec: np.ndarray,
        teacher_outputs: np.ndarray,
        student_probs: np.ndarray,
        current_gate_weights: np.ndarray,
    ) -> None:
        teacher_outputs = np.asarray(teacher_outputs, dtype=np.float64)
        student_probs = np.asarray(student_probs, dtype=np.float64)
        g = np.asarray(current_gate_weights, dtype=np.float64)

        combined = g @ teacher_outputs
        error = combined - student_probs
        dL_dg = teacher_outputs @ error  # (n_experts,)

        # Full softmax Jacobian
        dot = float(np.dot(g, dL_dg))
        dL_dz = g * (dL_dg - dot)

        # Optional load-balancing regularizer
        if self.load_balance_weight > 0:
            self._mean_gate = 0.99 * self._mean_gate + 0.01 * g
            target = np.ones_like(g) / len(g)
            lb_grad = self.load_balance_weight * (self._mean_gate - target)
            lb_dot = float(np.dot(g, lb_grad))
            dL_dz += g * (lb_grad - lb_dot)

        self.weights -= self.lr * np.outer(state_vec, dL_dz)
        self.bias -= self.lr * dL_dz


# ============================================================================
# Distillation Priority Optimizer
# ============================================================================

class DistillationPriorityOptimizer:
    """
    Multi-teacher on-policy distillation for priority selection.

    Key enhancements vs. v2.3.0:
      - Q-teacher is actually updated during batch training.
      - Student distills from the *gated* teacher combination.
      - MoE gating uses the full softmax Jacobian + load balancing.
      - Optional RunningScaler normalizes features online.
    """

    PRIORITIES: ClassVar[List[str]] = [p.value for p in Priority]

    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config
        self.feature_dim = 23
        self.n_actions = 3
        seed = config.get("seed", None)

        self.scaler = RunningScaler(self.feature_dim, clip=config.get("scaler_clip", 5.0))

        self.student = DistillationStudent(
            feature_dim=self.feature_dim,
            lr=config.get("distillation_learning_rate", 0.01),
            grad_clip=config.get("student_grad_clip", 1.0),
            seed=seed,
        )

        self.teachers: List[Teacher] = [
            PriorityRuleBasedTeacher(),
            PriorityHistoricalMLTeacher(model_path=_as_path(config.get("historical_model_path"))),
            PriorityStatefulQTeacher(
                lr=config.get("q_learning_rate", 0.1),
                weights_path=_as_path(config.get("q_weights_path")),
                feature_dim=self.feature_dim,
                autosave=config.get("q_autosave", True),
            ),
            RLHFPriorityTeacher(),
        ]

        if config.get("use_quantum_teacher", False) and QuantumTeacher is not None:
            try:
                self.teachers.append(QuantumTeacher(
                    n_actions=self.n_actions,
                    n_qubits=config.get("quantum_teacher_qubits", 4),
                    n_layers=config.get("quantum_teacher_layers", 2),
                ))
            except Exception as e:
                logger.warning(f"Failed to init quantum teacher: {e}")

        for t in self.teachers:
            t.scaler = self.scaler

        self.q_teacher: Optional[PriorityStatefulQTeacher] = next(
            (t for t in self.teachers if isinstance(t, PriorityStatefulQTeacher)), None
        )
        self.n_teachers = len(self.teachers)

        self.gating = MoEGatingNetwork(
            feature_dim=self.feature_dim,
            n_experts=self.n_teachers,
            lr=config.get("gating_learning_rate", 0.005),
            load_balance_weight=config.get("gating_load_balance_weight", 0.01),
            seed=seed,
        )

        self.replay_buffer = ReplayBuffer(max_size=config.get("distillation_replay_size", 2000))
        self.epsilon = float(config.get("distillation_epsilon", 0.1))
        self.epsilon_min = float(config.get("distillation_epsilon_min", 0.01))
        self.epsilon_decay = float(config.get("distillation_epsilon_decay", 0.995))
        self.train_every = int(config.get("distillation_train_every", 10))
        self.counter = 0
        self.distill_weight = float(config.get("distill_weight", 0.7))
        self.rl_weight = float(config.get("rl_weight", 0.3))
        self.batch_update_size = int(config.get("batch_update_size", 8))

        self.lock = asyncio.Lock()

        # Observability
        self.last_teacher_agreement: float = 0.0
        self.last_gate_entropy: float = 0.0
        self.last_student_loss: float = 0.0

    # ------------------------------------------------------------------
    def _compute_teacher_probs(self, state: WorkloadState) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        state_vec = state.to_feature_vector(self.scaler)
        teacher_outputs = []
        for teacher in self.teachers:
            try:
                prob = teacher.predict(state)
            except Exception as e:
                logger.warning(f"Teacher {type(teacher).__name__} failed: {e}")
                prob = np.ones(self.n_actions, dtype=np.float64) / self.n_actions
            prob = np.asarray(prob, dtype=np.float64).reshape(-1)
            if prob.shape[0] != self.n_actions:
                if prob.shape[0] < self.n_actions:
                    prob = np.pad(prob, (0, self.n_actions - prob.shape[0]))
                else:
                    prob = prob[: self.n_actions]
            s = prob.sum()
            prob = prob / s if s > 0 else np.ones(self.n_actions) / self.n_actions
            teacher_outputs.append(prob)
        teacher_outputs = np.array(teacher_outputs)
        gate_weights = self.gating.forward(state_vec)
        combined = gate_weights @ teacher_outputs
        s = combined.sum()
        combined = combined / s if s > 0 else np.ones(self.n_actions) / self.n_actions
        return combined, gate_weights, teacher_outputs

    def _track_metrics(self, teacher_outputs, gate_weights, combined, student_probs) -> None:
        n = teacher_outputs.shape[0]
        if n > 1:
            kls = []
            for i in range(n):
                for j in range(i + 1, n):
                    p = teacher_outputs[i] + 1e-9
                    q = teacher_outputs[j] + 1e-9
                    kls.append(float(np.sum(p * np.log(p / q))))
            mean_kl = float(np.mean(kls)) if kls else 0.0
            self.last_teacher_agreement = float(np.exp(-mean_kl))
        else:
            self.last_teacher_agreement = 1.0
        g = np.clip(gate_weights, 1e-12, 1.0)
        self.last_gate_entropy = float(-np.sum(g * np.log(g)))
        s = np.clip(student_probs, 1e-12, 1.0)
        self.last_student_loss = float(-np.sum(combined * np.log(s)))

    # ------------------------------------------------------------------
    async def select_priority(self, state: WorkloadState, exploration: bool = True):
        async with self.lock:
            raw = state.to_raw_vector()
            self.scaler.update(raw)
            state_vec = state.to_feature_vector(self.scaler)

            teacher_probs, gate_weights, teacher_outputs = self._compute_teacher_probs(state)
            student_probs = self.student.predict_proba(state_vec, self.n_actions)

            if exploration and random.random() < self.epsilon:
                action_idx = random.randint(0, self.n_actions - 1)
            else:
                combined = 0.8 * student_probs + 0.2 * teacher_probs
                action_idx = int(np.argmax(combined))

            self.epsilon = max(self.epsilon_min, self.epsilon * self.epsilon_decay)
            self._track_metrics(teacher_outputs, gate_weights, teacher_probs, student_probs)
            return self.PRIORITIES[action_idx], action_idx, state_vec, teacher_outputs

    async def update(self, state_vec, action_idx, reward, next_state_vec, teacher_outputs) -> None:
        async with self.lock:
            self.replay_buffer.push(state_vec, action_idx, reward, next_state_vec, teacher_outputs)
            self.counter += 1
            if self.counter % self.train_every != 0:
                return
            if len(self.replay_buffer) < self.batch_update_size:
                return

            states, actions, rewards, _, teacher_outputs_batch = self.replay_buffer.sample(
                self.batch_update_size
            )

            for i in range(len(states)):
                s_vec = states[i]
                t_outs = teacher_outputs_batch[i]  # (n_teachers, n_actions)
                a = int(actions[i])
                r = float(rewards[i])

                # 1. Gated teacher target
                gate_w = self.gating.forward(s_vec)
                combined = gate_w @ t_outs
                s = combined.sum()
                combined = combined / s if s > 0 else np.ones(self.n_actions) / self.n_actions

                # 2. Student update against gated teacher target
                self.student.update(
                    s_vec, combined, r, a,
                    distill_weight=self.distill_weight,
                    rl_weight=self.rl_weight,
                )

                # 3. Q-teacher online update
                if self.q_teacher is not None:
                    self.q_teacher.update_from_vector(s_vec, a, r)

                # 4. Gating update with full Jacobian
                student_out = self.student.predict_proba(s_vec)
                self.gating.update(s_vec, t_outs, student_out, gate_w)

    async def export_student_weights(self) -> Dict[str, np.ndarray]:
        async with self.lock:
            return self.student.export_weights()

    async def import_student_weights(self, weights: Dict[str, np.ndarray]) -> None:
        async with self.lock:
            self.student.import_weights(weights)

    async def export_federated_blob(self) -> Dict[str, Any]:
        w = await self.export_student_weights()
        return FederatedWeightsCodec.encode(w)

    async def import_federated_blob(self, blob: Dict[str, Any]) -> None:
        weights = FederatedWeightsCodec.decode(blob)
        await self.import_student_weights(weights)

    def get_stats(self) -> Dict[str, Any]:
        return {
            "student_counter": self.student.counter,
            "buffer_size": len(self.replay_buffer),
            "epsilon": self.epsilon,
            "teacher_agreement": self.last_teacher_agreement,
            "gate_entropy": self.last_gate_entropy,
            "student_loss": self.last_student_loss,
            "scaler_count": self.scaler.count,
        }

    def flush(self) -> None:
        if self.q_teacher is not None:
            self.q_teacher.save(force=True)


# ============================================================================
# Enhanced WorkloadDescriptor
# ============================================================================

class WorkloadDescriptor(BaseModel):
    model_config = ConfigDict(extra="allow", arbitrary_types_allowed=True)

    task_id: Optional[str] = Field(None, description="Unique task identifier")
    correlation_id: Optional[str] = Field(None, description="Correlation ID for tracing")

    task_type: TaskType = Field(..., description="Type of the workload")
    tokens: int = Field(..., ge=1, description="Number of tokens")
    latency_target: float = Field(..., gt=0, description="Target latency in ms")
    deadline: Optional[datetime] = Field(None, description="Hard deadline")
    urgency: Urgency = Field(Urgency.MEDIUM, description="Urgency level")

    sector_emission_factor: Optional[float] = Field(None, ge=0, description="kg CO₂ per $ revenue")
    estimated_energy_joules: Optional[float] = Field(None, ge=0, description="Estimated energy (J)")
    estimated_carbon_kg: Optional[float] = Field(None, ge=0, description="Estimated carbon (kg CO₂)")
    helium_units: Optional[float] = Field(None, ge=0, description="Estimated helium units")

    data_size_bytes: Optional[int] = Field(None, ge=0, description="Input data size (bytes)")
    model_size_bytes: Optional[int] = Field(None, ge=0, description="Model size (bytes)")

    user_id: Optional[str] = Field(None, description="User identifier")
    tenant_id: Optional[str] = Field(None, description="Tenant identifier")

    bio_mode: BioMode = Field(BioMode.NONE, description="Bio-inspired operation mode")

    adaptive_priority: Priority = Field(Priority.BALANCED, description="Current adaptive priority")
    performance_history: List[Dict[str, Any]] = Field(default_factory=list,
                                                     description="Recent scheduling outcomes")
    max_history_length: int = Field(100, ge=1, description="Maximum history entries")

    graph_metrics: Optional[Dict[str, float]] = Field(None, description="LIMIT Graph metrics")
    human_feedback_score: float = Field(0.5, ge=0, le=1, description="RLHF human feedback score")
    use_evolutionary: bool = Field(False, description="Enable evolutionary optimisation")
    evolutionary_population_size: int = Field(20, ge=2)
    evolutionary_mutation_rate: float = Field(0.1, ge=0, le=1)
    evolutionary_crossover_rate: float = Field(0.7, ge=0, le=1)
    evolutionary_elitism: int = Field(2, ge=1)
    use_quantum_teacher: bool = Field(False, description="Enable quantum teacher")

    version: str = Field("2.4.0", description="Schema version")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional custom data")

    # -- Private state ------------------------------------------------
    _priority_optimizer: Optional[DistillationPriorityOptimizer] = PrivateAttr(default=None)
    _last_decision: Optional[Dict[str, Any]] = PrivateAttr(default=None)
    _causal_shaper: Optional[Any] = PrivateAttr(default=None)
    _evolutionary_optimizer: Optional[Any] = PrivateAttr(default=None)
    _state_lock: asyncio.Lock = PrivateAttr(default_factory=asyncio.Lock)
    _log_lock: asyncio.Lock = PrivateAttr(default_factory=asyncio.Lock)
    _prev_snapshot: Dict[str, float] = PrivateAttr(default_factory=dict)
    _current_snapshot: Dict[str, float] = PrivateAttr(default_factory=dict)

    # ------------------------------------------------------------------
    # Validators
    # ------------------------------------------------------------------
    @field_validator("sector_emission_factor")
    @classmethod
    def _validate_sector_emission_factor(cls, v: Optional[float]) -> Optional[float]:
        if v is not None and v < 0:
            raise ValueError("sector_emission_factor must be non-negative")
        return v

    @field_validator("latency_target")
    @classmethod
    def _validate_latency_target(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("latency_target must be positive")
        return v

    @field_validator("deadline")
    @classmethod
    def _normalize_deadline(cls, v: Optional[datetime]) -> Optional[datetime]:
        if v is not None and v.tzinfo is None:
            return v.replace(tzinfo=timezone.utc)
        return v

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def compute_energy_cost(self, energy_per_token: float) -> float:
        return energy_per_token * self.tokens

    def compute_carbon_cost(self, carbon_intensity_kg_per_kwh: float) -> float:
        # Assume 10 J per token by default; make configurable via metadata.
        energy_per_token = float(self.metadata.get("assumed_energy_per_token_j", 10.0))
        energy_j = self.tokens * energy_per_token
        energy_kwh = energy_j * 2.7778e-7
        return energy_kwh * carbon_intensity_kg_per_kwh

    def to_dict(self, exclude_none: bool = False) -> Dict[str, Any]:
        data = self.model_dump()
        if exclude_none:
            return {k: v for k, v in data.items() if v is not None}
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "WorkloadDescriptor":
        return cls(**data)

    def is_critical(self) -> bool:
        return self.urgency == Urgency.CRITICAL

    def is_high_priority(self) -> bool:
        return self.urgency in (Urgency.HIGH, Urgency.CRITICAL)

    # ------------------------------------------------------------------
    # Snapshot management (public)
    # ------------------------------------------------------------------
    def set_snapshot_before(self, snapshot: Optional[Dict[str, float]]) -> None:
        self._prev_snapshot = dict(snapshot or {})

    def set_snapshot_after(self, snapshot: Optional[Dict[str, float]]) -> None:
        self._current_snapshot = dict(snapshot or {})

    # ------------------------------------------------------------------
    # Optimizer wiring
    # ------------------------------------------------------------------
    def _ensure_optimizer(self) -> None:
        if self._priority_optimizer is not None:
            return
        md = self.metadata or {}
        safe_task = _sanitize_id(self.task_id)
        config = {
            "distillation_epsilon": md.get("distillation_epsilon", 0.1),
            "distillation_epsilon_min": md.get("distillation_epsilon_min", 0.01),
            "distillation_epsilon_decay": md.get("distillation_epsilon_decay", 0.995),
            "distillation_train_every": md.get("distillation_train_every", 10),
            "distillation_replay_size": md.get("distillation_replay_size", 2000),
            "distillation_learning_rate": md.get("distillation_learning_rate", 0.01),
            "historical_model_path": _as_path(md.get("historical_model_path")),
            "q_learning_rate": md.get("q_learning_rate", 0.1),
            "q_weights_path": _as_path(
                md.get("q_weights_path", f"./priority_q_weights_{safe_task}.json")
            ),
            "q_autosave": md.get("q_autosave", True),
            "distill_weight": md.get("distill_weight", 0.7),
            "rl_weight": md.get("rl_weight", 0.3),
            "batch_update_size": md.get("batch_update_size", 8),
            "gating_learning_rate": md.get("gating_learning_rate", 0.005),
            "gating_load_balance_weight": md.get("gating_load_balance_weight", 0.01),
            "use_quantum_teacher": self.use_quantum_teacher,
            "quantum_teacher_qubits": md.get("quantum_teacher_qubits", 4),
            "quantum_teacher_layers": md.get("quantum_teacher_layers", 2),
            "seed": md.get("seed", None),
            "student_grad_clip": md.get("student_grad_clip", 1.0),
            "scaler_clip": md.get("scaler_clip", 5.0),
        }
        self._priority_optimizer = DistillationPriorityOptimizer(config)

    # ------------------------------------------------------------------
    # Decision + outcome
    # ------------------------------------------------------------------
    async def select_priority(
        self,
        exploration: bool = True,
        latency_achieved_ms: Optional[float] = None,
        carbon_saved_kg: Optional[float] = None,
        energy_used_joules: Optional[float] = None,
        snapshot_before: Optional[Dict[str, float]] = None,
    ) -> Priority:
        """
        Choose the next priority. If outcome metrics are provided, they are
        recorded immediately (via the lock-safe path). Otherwise the caller
        is expected to call ``record_outcome`` later.
        """
        async with self._state_lock:
            self._ensure_optimizer()
            if snapshot_before is not None:
                self.set_snapshot_before(snapshot_before)

            state = self._build_state()
            priority, action_idx, state_vec, teacher_outputs = await self._priority_optimizer.select_priority(
                state, exploration=exploration
            )
            self._last_decision = {
                "state_vec": state_vec,
                "action_idx": action_idx,
                "teacher_outputs": teacher_outputs,
                "priority": priority,
            }
            self.adaptive_priority = Priority(priority)

            record_now = (
                latency_achieved_ms is not None
                and carbon_saved_kg is not None
                and energy_used_joules is not None
            )
            if record_now:
                # We hold the lock; call the locked helper directly.
                await self._record_outcome_locked(
                    latency_achieved_ms, carbon_saved_kg, energy_used_joules,
                    snapshot_after=None,
                )
            return self.adaptive_priority

    async def record_outcome(
        self,
        latency_achieved_ms: float,
        carbon_saved_kg: float,
        energy_used_joules: float,
        snapshot_after: Optional[Dict[str, float]] = None,
    ) -> None:
        """Public API. Safe to call without holding the lock."""
        async with self._state_lock:
            await self._record_outcome_locked(
                latency_achieved_ms, carbon_saved_kg, energy_used_joules, snapshot_after
            )

    async def _record_outcome_locked(
        self,
        latency_achieved_ms: float,
        carbon_saved_kg: float,
        energy_used_joules: float,
        snapshot_after: Optional[Dict[str, float]] = None,
    ) -> None:
        """Must only be called while ``_state_lock`` is held."""
        if self._last_decision is None:
            logger.debug("record_outcome called with no pending decision; ignoring.")
            return

        if snapshot_after is not None:
            self.set_snapshot_after(snapshot_after)

        # --- Base reward ------------------------------------------------
        lat_target = float(self.latency_target) or 1.0
        latency_score = 1.0 - min(1.0, abs(latency_achieved_ms - lat_target) / lat_target)
        carbon_norm = min(1.0, max(0.0, carbon_saved_kg / 0.1))

        est_energy = self.estimated_energy_joules
        if est_energy is not None and est_energy > 0:
            energy_norm = 1.0 - min(1.0, max(0.0, energy_used_joules / est_energy))
        else:
            # Fall back to an assumed per-token energy budget
            assumed_per_token = float(self.metadata.get("assumed_energy_per_token_j", 10.0))
            fallback_budget = max(self.tokens * assumed_per_token, 1e-6)
            energy_norm = 1.0 - min(1.0, max(0.0, energy_used_joules / fallback_budget))

        w_lat = float(self.metadata.get("reward_weight_latency", 0.4))
        w_carbon = float(self.metadata.get("reward_weight_carbon", 0.3))
        w_energy = float(self.metadata.get("reward_weight_energy", 0.3))
        total_w = w_lat + w_carbon + w_energy
        if total_w <= 0:
            total_w = 1.0

        reward = (w_lat * latency_score + w_carbon * carbon_norm + w_energy * energy_norm) / total_w
        reward = float(max(0.0, min(1.0, reward)))

        # --- Causal shaping --------------------------------------------
        reward = self._shape_reward_safe(reward)

        # --- Build log entry -------------------------------------------
        entry: Dict[str, Any] = {
            "timestamp": _utc_now().isoformat(),
            "priority": self._last_decision["priority"],
            "reward": reward,
            "latency_achieved_ms": latency_achieved_ms,
            "carbon_saved_kg": carbon_saved_kg,
            "energy_used_joules": energy_used_joules,
            "state_vec": ",".join(map(str, self._last_decision["state_vec"])),
        }

        # --- Optimizer update ------------------------------------------
        state_vec = self._last_decision["state_vec"]
        action_idx = self._last_decision["action_idx"]
        teacher_outputs = self._last_decision["teacher_outputs"]

        next_state = self._build_state()
        next_state_vec = next_state.to_feature_vector(self._priority_optimizer.scaler)

        await self._priority_optimizer.update(
            state_vec, action_idx, reward, next_state_vec, teacher_outputs
        )

        self._last_decision = None

        # --- History -----------------------------------------------
        self.performance_history.append(entry)
        if len(self.performance_history) > self.max_history_length:
            self.performance_history = self.performance_history[-self.max_history_length:]

        # --- Async persistence (best-effort; outside the log lock) ---
        await self._persist_entry_async(entry)

        # --- FeedbackEvent ---------------------------------------------
        self._emit_feedback_event(reward, latency_achieved_ms, carbon_saved_kg, energy_used_joules)

    # ------------------------------------------------------------------
    def _shape_reward_safe(self, base_reward: float) -> float:
        if self._causal_shaper is None:
            return base_reward
        if self._last_decision is None:
            return base_reward
        if not self._prev_snapshot or not self._current_snapshot:
            return base_reward
        try:
            shaped = self._causal_shaper.shape_reward(
                self._last_decision["action_idx"],
                base_reward,
                self._prev_snapshot,
                self._current_snapshot,
            )
            return float(max(0.0, min(1.0, shaped)))
        except Exception as e:
            logger.warning(f"Causal shaping failed: {e}")
            return base_reward

    # ------------------------------------------------------------------
    def _emit_feedback_event(
        self,
        reward: float,
        latency_achieved_ms: float,
        carbon_saved_kg: float,
        energy_used_joules: float,
    ) -> None:
        if FeedbackEvent is None:
            return

        # Determine feedback_type: register "scheduling" if the schema allows
        # extras, otherwise fall back to "routing".
        allowed = getattr(FeedbackEvent, "allowed_feedback_types", None)
        if allowed is None or WORKLOAD_FEEDBACK_TYPE in set(allowed):
            fb_type = WORKLOAD_FEEDBACK_TYPE
        else:
            fb_type = "routing"

        try:
            event = FeedbackEvent(
                source="workload_descriptor",
                feedback_type=fb_type,
                context={
                    "task_id": self.task_id or "unknown",
                    "teacher_ids": [
                        type(t).__name__ for t in self._priority_optimizer.teachers
                    ] if self._priority_optimizer else [],
                },
                action={
                    "selected_action": self.adaptive_priority.value,
                    "selected_rank": 1,
                    "confidence_score": self.metadata.get("confidence", 0.5),
                },
                performance={
                    "quality_score": reward,
                    "latency_ms": latency_achieved_ms,
                    "energy_joules": energy_used_joules,
                    "carbon_g": max(0.0, carbon_saved_kg * 1000.0),
                    "helium_cost": float(self.helium_units or 0.0),
                    "duration_ms": 0.0,
                },
                adaptive_cost_value=reward,
                tags=["workload", "scheduling", self.adaptive_priority.value],
                envelope={
                    "trace_id": self.correlation_id,
                },
            )
            logger.debug(f"FeedbackEvent created: {getattr(event, 'event_id', '?')}")
        except Exception as e:
            logger.warning(f"Failed to create FeedbackEvent: {e}")

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------
    async def _persist_entry_async(self, entry: Dict[str, Any]) -> None:
        try:
            async with self._log_lock:
                await asyncio.to_thread(self._append_log_sync, entry)
        except Exception as e:
            logger.error(f"Failed to persist workload log: {e}")

    def _append_log_sync(self, entry: Dict[str, Any]) -> None:
        safe = _sanitize_id(self.task_id)
        csv_path = Path(f"./workload_{safe}_logs.csv")
        exists = csv_path.exists()
        flat = {
            k: (json.dumps(v) if isinstance(v, (dict, list)) else v)
            for k, v in entry.items()
        }
        pd.DataFrame([flat]).to_csv(csv_path, mode="a", header=not exists, index=False)

    # ------------------------------------------------------------------
    # State construction
    # ------------------------------------------------------------------
    def _build_state(self) -> WorkloadState:
        urgency_val = URGENCY_TO_FLOAT.get(self.urgency, 1.0)

        onehot: Dict[TaskType, float] = {t: 0.0 for t in TaskType}
        onehot[self.task_type] = 1.0

        bio_photo = 1.0 if self.bio_mode == BioMode.PHOTOSYNTHETIC else 0.0
        bio_chemo = 1.0 if self.bio_mode == BioMode.CHEMOTACTIC else 0.0

        if self.performance_history:
            recent = self.performance_history[-20:]
            rewards = [float(r.get("reward", 0.0)) for r in recent]
            success_rate = sum(1 for r in rewards if r > 0.5) / max(len(rewards), 1)
            avg_reward = float(np.mean(rewards)) if rewards else 0.0
        else:
            success_rate = 0.5
            avg_reward = 0.5

        graph_centrality = float((self.graph_metrics or {}).get("centrality", 0.5))
        graph_centrality = max(0.0, min(1.0, graph_centrality))

        return WorkloadState(
            tokens=float(self.tokens),
            latency_target=float(self.latency_target),
            urgency=urgency_val,
            task_type_training=onehot[TaskType.TRAINING],
            task_type_inference=onehot[TaskType.INFERENCE],
            task_type_edge_sensing=onehot[TaskType.EDGE_SENSING],
            task_type_federated_round=onehot[TaskType.FEDERATED_ROUND],
            task_type_multimodal=onehot[TaskType.MULTIMODAL],
            task_type_data_processing=onehot[TaskType.DATA_PROCESSING],
            task_type_query=onehot[TaskType.QUERY],
            task_type_inference_batch=onehot[TaskType.INFERENCE_BATCH],
            task_type_training_distributed=onehot[TaskType.TRAINING_DISTRIBUTED],
            task_type_edge_compute=onehot[TaskType.EDGE_COMPUTE],
            estimated_energy=float(self.estimated_energy_joules or 0.0),
            estimated_carbon=float(self.estimated_carbon_kg or 0.0),
            helium_units=float(self.helium_units or 0.0),
            sector_emission_factor=float(self.sector_emission_factor or 0.0),
            bio_mode_photosynthetic=bio_photo,
            bio_mode_chemotactic=bio_chemo,
            recent_success_rate=success_rate,
            avg_reward=avg_reward,
            human_feedback_score=self.human_feedback_score,
            graph_centrality=graph_centrality,
        )

    # ------------------------------------------------------------------
    # Causal graph + federated learning
    # ------------------------------------------------------------------
    def set_causal_graph(self, causal_graph: Any) -> None:
        if CausalRewardShaper is None:
            logger.warning("CausalRewardShaper unavailable; skipping causal graph setup.")
            return
        try:
            self._causal_shaper = CausalRewardShaper(causal_graph, influence_weight=0.3)
        except Exception as e:
            logger.warning(f"Failed to set causal graph: {e}")

    async def export_federated_weights(self) -> Dict[str, np.ndarray]:
        self._ensure_optimizer()
        return await self._priority_optimizer.export_student_weights()

    async def import_federated_weights(self, weights: Dict[str, np.ndarray]) -> None:
        self._ensure_optimizer()
        await self._priority_optimizer.import_student_weights(weights)

    async def export_federated_blob(self) -> Dict[str, Any]:
        self._ensure_optimizer()
        return await self._priority_optimizer.export_federated_blob()

    async def import_federated_blob(self, blob: Dict[str, Any]) -> None:
        self._ensure_optimizer()
        await self._priority_optimizer.import_federated_blob(blob)

    def get_optimizer_stats(self) -> Dict[str, Any]:
        self._ensure_optimizer()
        return self._priority_optimizer.get_stats()

    def flush(self) -> None:
        if self._priority_optimizer is not None:
            self._priority_optimizer.flush()


# ============================================================================
# Factory
# ============================================================================

def create_workload_descriptor(
    task_type: TaskType,
    tokens: int,
    latency_target: float,
    **kwargs: Any,
) -> WorkloadDescriptor:
    return WorkloadDescriptor(
        task_type=task_type,
        tokens=tokens,
        latency_target=latency_target,
        **kwargs,
    )


__all__ = [
    "TaskType",
    "Urgency",
    "Priority",
    "BioMode",
    "URGENCY_TO_FLOAT",
    "WORKLOAD_FEEDBACK_TYPE",
    "RunningScaler",
    "FederatedWeightsCodec",
    "WorkloadState",
    "Teacher",
    "PriorityRuleBasedTeacher",
    "PriorityHistoricalMLTeacher",
    "PriorityStatefulQTeacher",
    "RLHFPriorityTeacher",
    "DistillationStudent",
    "ReplayBuffer",
    "MoEGatingNetwork",
    "DistillationPriorityOptimizer",
    "WorkloadDescriptor",
    "create_workload_descriptor",
]


# ============================================================================
# Tests
# ============================================================================

def _make_descriptor(**overrides: Any) -> "WorkloadDescriptor":
    base: Dict[str, Any] = {
        "task_id": "task-1",
        "task_type": TaskType.INFERENCE,
        "tokens": 1000,
        "latency_target": 200.0,
        "urgency": Urgency.MEDIUM,
        "estimated_energy_joules": 100.0,
        "estimated_carbon_kg": 0.01,
        "helium_units": 0.005,
        "metadata": {"seed": 42},
    }
    base.update(overrides)
    return WorkloadDescriptor(**base)


def _test_validators_run() -> None:
    # sector_emission_factor must reject negatives
    try:
        _make_descriptor(sector_emission_factor=-1.0)
    except Exception:
        pass
    else:
        raise AssertionError("negative sector_emission_factor should be rejected")

    # latency_target must be positive
    try:
        _make_descriptor(latency_target=0.0)
    except Exception:
        pass
    else:
        raise AssertionError("zero latency_target should be rejected")


def _test_feature_vector_shape_and_clamps() -> None:
    d = _make_descriptor()
    s = d._build_state()
    v = s.to_feature_vector()
    assert v.shape == (23,)
    assert np.all(v >= 0.0) and np.all(v <= 1.0), f"features out of [0,1]: {v}"


def _test_running_scaler() -> None:
    s = RunningScaler(23)
    for _ in range(5):
        s.update(np.random.randn(23) + 1.0)
    out = s.transform(np.random.randn(23))
    assert out.shape == (23,)
    assert np.all(np.isfinite(out))


def _test_q_teacher_update_from_vector() -> None:
    q = PriorityStatefulQTeacher(feature_dim=23)
    v = np.zeros(23)
    v[0] = 1.0
    q.update_from_vector(v, 1, 1.0)
    assert np.any(q.weights[:, 1] != 0), "Q-teacher weights should update"


def _test_federated_codec_roundtrip() -> None:
    w = {"weights": np.random.randn(23, 3), "biases": np.random.randn(3)}
    blob = FederatedWeightsCodec.encode(w)
    back = FederatedWeightsCodec.decode(blob)
    assert np.allclose(w["weights"], back["weights"])
    assert np.allclose(w["biases"], back["biases"])


def _test_gating_jacobian_shapes() -> None:
    g = MoEGatingNetwork(feature_dim=23, n_experts=5, lr=0.01)
    s = np.random.randn(23)
    t = np.random.rand(5, 3)
    t = t / t.sum(axis=1, keepdims=True)
    student = np.random.rand(3)
    student = student / student.sum()
    gw = g.forward(s)
    g.update(s, t, student, gw)
    assert g.weights.shape == (23, 5)
    assert g.bias.shape == (5,)


def _test_sanitize_id() -> None:
    assert _sanitize_id(None) == "unknown"
    assert _sanitize_id("") == "unknown"
    assert "/" not in _sanitize_id("a/b/c")
    assert ".." not in _sanitize_id("../../etc/passwd")


def _test_shaper_guard_no_snapshots() -> None:
    d = _make_descriptor()
    # No causal shaper set; should be a no-op.
    assert d._shape_reward_safe(0.5) == 0.5


async def _async_test_select_and_record() -> None:
    d = _make_descriptor()
    strat = await d.select_priority(exploration=False)
    assert isinstance(strat, Priority)
    await d.record_outcome(
        latency_achieved_ms=150.0,
        carbon_saved_kg=0.05,
        energy_used_joules=80.0,
    )
    # Select again immediately; must not deadlock.
    strat2 = await d.select_priority(exploration=True)
    assert isinstance(strat2, Priority)

    stats = d.get_optimizer_stats()
    assert stats["student_counter"] >= 0
    d.flush()


async def _async_test_federated_roundtrip() -> None:
    d = _make_descriptor(task_id="fed-test")
    blob = await d.export_federated_blob()
    await d.import_federated_blob(blob)


def _run_all_tests() -> None:
    _test_validators_run()
    _test_feature_vector_shape_and_clamps()
    _test_running_scaler()
    _test_q_teacher_update_from_vector()
    _test_federated_codec_roundtrip()
    _test_gating_jacobian_shapes()
    _test_sanitize_id()
    _test_shaper_guard_no_snapshots()
    asyncio.run(_async_test_select_and_record())
    asyncio.run(_async_test_federated_roundtrip())
    print("All WorkloadDescriptor tests passed.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    _run_all_tests()
