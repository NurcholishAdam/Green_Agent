"""
Enhanced Node Descriptor v2.4.0
================================
Defines the structure of a compute node with adaptive routing strategy selection
via Multi-Teacher On-Policy Distillation.

Enhancements (v2.4.0 over v2.3.0):
- Fixed carbon-intensity unit mismatch (unified kg CO2/kWh everywhere).
- Eliminated deadlock between select_routing_strategy and record_outcome.
- Fixed falsy-value fallback bug in _build_state (0.0 no longer replaced).
- Q-teacher is now actually updated during training.
- Student distillation target now uses the gated (MoE) teacher combination.
- MoE gating update now uses the full softmax Jacobian + load balancing.
- Added RunningScaler for online feature normalization.
- Improved causal reward shaping with safety guards and snapshot tracking.
- Asynchronous, append-only, sanitized log persistence.
- Federated learning hardening: versioned base64-JSON codec + shape validation.
- Configurable reward weights and scaling caps.
- Sanitized file paths (no unsafe characters from node id).
- Added unit tests at the bottom of the file.

All previous features retained.
"""

from __future__ import annotations

from enum import Enum
from typing import Optional, Dict, Any, List, Tuple, Union
from datetime import datetime
from collections import deque
from pathlib import Path
import base64
import json
import random
import re
import numpy as np
from abc import ABC, abstractmethod
import pickle
import pandas as pd
from dataclasses import dataclass, asdict
import asyncio
import logging

from pydantic import BaseModel, Field, field_validator, ConfigDict, PrivateAttr

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
try:
    from ..logger import logger  # type: ignore
except ImportError:
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

try:
    from ..evolutionary_optimizer import EvolutionaryOptimizer  # type: ignore
except ImportError:
    EvolutionaryOptimizer = None  # type: ignore


# ============================================================================
# Enums
# ============================================================================

class NodeType(str, Enum):
    EDGE = "edge"
    HOTSPOT = "hotspot"
    CLOUD = "cloud"
    LAB = "lab"
    ON_PREM = "on_prem"
    VM = "vm"
    CONTAINER = "container"


class CoolingType(str, Enum):
    AIR = "air"
    LIQUID = "liquid"
    CRYOGENIC = "cryogenic"
    NONE = "none"


class MaintenanceStatus(str, Enum):
    OPERATIONAL = "operational"
    DEGRADED = "degraded"
    MAINTENANCE = "maintenance"
    OFFLINE = "offline"


class RoutingStrategy(str, Enum):
    CARBON_FIRST = "carbon_first"
    LATENCY_FIRST = "latency_first"
    COST_FIRST = "cost_first"
    BALANCED = "balanced"
    ADAPTIVE = "adaptive"


# ============================================================================
# Utilities
# ============================================================================

def _as_path(v: Union[str, Path, None]) -> Optional[Path]:
    """Coerce a string or Path into a Path (or None)."""
    if v is None:
        return None
    return v if isinstance(v, Path) else Path(v)


def _sanitize_id(node_id: str) -> str:
    """Make a filesystem-safe version of a node id."""
    return re.sub(r"[^A-Za-z0-9_.\-]", "_", str(node_id))[:128] or "node"


def _softmax(logits: np.ndarray) -> np.ndarray:
    """Numerically stable softmax."""
    z = logits - np.max(logits)
    e = np.exp(z)
    s = e.sum()
    if s <= 0:
        return np.ones_like(e) / len(e)
    return e / s


class RunningScaler:
    """
    Online running mean/std scaler for feature normalization.

    Provides a rolling estimate of mean and variance so that features can be
    normalized without a separate offline fit. Values are clipped to
    ``[-clip, clip]`` after z-scoring to bound the input distribution.
    """

    def __init__(self, dim: int, clip: float = 5.0, min_std: float = 1e-6):
        self.dim = int(dim)
        self.clip = float(clip)
        self.min_std = float(min_std)
        self.mean = np.zeros(self.dim, dtype=np.float64)
        self.m2 = np.zeros(self.dim, dtype=np.float64)  # sum of squared diffs
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


# ============================================================================
# State
# ============================================================================

@dataclass
class NodeState:
    """State for the distillation agent (carbon intensity in kg CO2/kWh)."""
    carbon_intensity: float          # kg CO2 / kWh
    renewable_fraction: float
    helium_connectivity: float
    uptime: float
    efficiency_score: float
    health_score: float
    cost_per_hour: float             # USD / hour
    energy_per_token: float          # J / token
    recent_success_rate: float
    avg_reward: float
    human_feedback_score: float = 0.5

    # Configurable static scaling caps (used if no RunningScaler provided)
    CARBON_MAX_KG_PER_KWH: float = 1.0
    COST_MAX_USD_PER_HOUR: float = 20.0
    ENERGY_MAX_J_PER_TOKEN: float = 1e-3

    def to_raw_vector(self) -> np.ndarray:
        """Return the raw (unscaled) 11-dim vector."""
        return np.array([
            self.carbon_intensity,
            self.renewable_fraction,
            self.helium_connectivity,
            self.uptime,
            self.efficiency_score,
            self.health_score,
            self.cost_per_hour,
            self.energy_per_token,
            self.recent_success_rate,
            self.avg_reward,
            self.human_feedback_score,
        ], dtype=np.float64)

    def to_feature_vector(self, scaler: Optional[RunningScaler] = None) -> np.ndarray:
        """
        Return the 11-dim numeric feature vector.

        If ``scaler`` is provided, features are z-scored (clipped). Otherwise
        a static min-max style scaling is used with configured caps.
        """
        raw = self.to_raw_vector()
        if scaler is not None:
            return scaler.transform(raw)

        v = np.empty(11, dtype=np.float32)
        v[0] = min(max(self.carbon_intensity / self.CARBON_MAX_KG_PER_KWH, 0.0), 1.0)
        v[1] = min(max(self.renewable_fraction, 0.0), 1.0)
        v[2] = min(max(self.helium_connectivity, 0.0), 1.0)
        v[3] = min(max(self.uptime, 0.0), 1.0)
        v[4] = min(max(self.efficiency_score, 0.0), 1.0)
        v[5] = min(max(self.health_score, 0.0), 1.0)
        v[6] = min(max(self.cost_per_hour / self.COST_MAX_USD_PER_HOUR, 0.0), 1.0)
        v[7] = min(max(self.energy_per_token / self.ENERGY_MAX_J_PER_TOKEN, 0.0), 1.0)
        v[8] = min(max(self.recent_success_rate, 0.0), 1.0)
        v[9] = min(max(self.avg_reward, 0.0), 1.0)
        v[10] = min(max(self.human_feedback_score, 0.0), 1.0)
        return v


# ============================================================================
# Teachers
# ============================================================================

class Teacher(ABC):
    """Abstract teacher. Each teacher produces a probability vector over actions."""

    def __init__(self) -> None:
        self.scaler: Optional[RunningScaler] = None

    @abstractmethod
    def predict(self, state: NodeState) -> np.ndarray:
        """Return probability vector over 5 strategies."""
        ...

    @abstractmethod
    def confidence(self, state: NodeState) -> float:
        """Return confidence in prediction [0,1]."""
        ...

    # ------------------------------------------------------------------
    def _features(self, state: NodeState) -> np.ndarray:
        return state.to_feature_vector(self.scaler)


class RoutingRuleBasedTeacher(Teacher):
    """
    Rule-based expert using the original heuristics, fixed for kg CO2/kWh.
    Thresholds:
        carbon_intensity > 0.5 kg/kWh  -> carbon_first
        health_score     < 0.6         -> latency_first
        cost_per_hour    > 5.0 USD/h   -> cost_first
        renewable_fraction > 0.7       -> balanced
        else                            -> adaptive default
    """

    STRATEGIES = ['carbon_first', 'latency_first', 'cost_first', 'balanced', 'adaptive']

    def __init__(self) -> None:
        super().__init__()

    def predict(self, state: NodeState) -> np.ndarray:
        probs = np.ones(5, dtype=np.float64) * 0.05
        if state.carbon_intensity > 0.5:
            probs[0] += 0.5
        if state.health_score < 0.6:
            probs[1] += 0.3
        if state.cost_per_hour > 5.0:
            probs[2] += 0.3
        if state.renewable_fraction > 0.7:
            probs[3] += 0.3
        probs[4] += 0.2
        return _softmax(probs)

    def confidence(self, state: NodeState) -> float:
        # Confidence increases when a strong rule fires.
        fired = 0
        fired += int(state.carbon_intensity > 0.5)
        fired += int(state.health_score < 0.6)
        fired += int(state.cost_per_hour > 5.0)
        fired += int(state.renewable_fraction > 0.7)
        return min(1.0, 0.4 + 0.15 * fired)


class RoutingHistoricalMLTeacher(Teacher):
    STRATEGY_ORDER = [s.value for s in RoutingStrategy]

    def __init__(self, model_path: Optional[Path] = None) -> None:
        super().__init__()
        self.model = None
        self.label_encoder = None
        self.model_path: Path = _as_path(model_path) or Path("./routing_historical_model.pkl")
        self._load_model()

    def _load_model(self) -> None:
        if self.model_path.exists():
            try:
                with open(self.model_path, 'rb') as f:
                    self.model, self.label_encoder = pickle.load(f)
                logger.info(f"Loaded historical ML model from {self.model_path}")
            except Exception as e:
                logger.error(f"Failed to load historical model: {e}")
                self.model, self.label_encoder = None, None

    def predict(self, state: NodeState) -> np.ndarray:
        if self.model is None or self.label_encoder is None:
            return np.ones(5) / 5
        x = self._features(state).reshape(1, -1)
        if not hasattr(self.model, "predict_proba"):
            return np.ones(5) / 5
        probs = self.model.predict_proba(x)[0]
        classes = list(self.label_encoder.classes_)
        idx_map = {cls: i for i, cls in enumerate(classes)}
        new_probs = np.zeros(5, dtype=np.float64)
        for i, strat in enumerate(self.STRATEGY_ORDER):
            if strat in idx_map:
                new_probs[i] = probs[idx_map[strat]]
        s = new_probs.sum()
        return new_probs / s if s > 0 else np.ones(5) / 5

    def confidence(self, state: NodeState) -> float:
        if self.model is None:
            return 0.0
        probs = self.predict(state)
        entropy = -np.sum(probs * np.log(probs + 1e-9))
        return float(1.0 - entropy / np.log(5))

    @classmethod
    def train_from_logs(
        cls,
        log_paths: List[Path],
        model_path: Path,
        state_col: str = "state_vec",
        label_col: str = "strategy",
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

        def parse_state(s: str) -> Optional[np.ndarray]:
            try:
                parts = [float(p) for p in str(s).split(",") if p.strip() != ""]
                return np.array(parts, dtype=np.float64) if parts else None
            except Exception:
                return None

        rows, labels = [], []
        for s, lbl in zip(df[state_col].values, df[label_col].values):
            v = parse_state(s)
            if v is not None and v.shape[0] == 11:
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

        model_path = _as_path(model_path) or Path("./routing_historical_model.pkl")
        with open(model_path, "wb") as f:
            pickle.dump((clf, le), f)
        logger.info(f"Trained historical model and saved to {model_path}")
        return model_path


class RoutingStatefulQTeacher(Teacher):
    """
    Linear Q-teacher. Now exposes an ``update_from_vector`` method that the
    optimizer calls with (feature_vector, action, reward).
    """

    def __init__(
        self,
        lr: float = 0.1,
        weights_path: Optional[Path] = None,
        feature_dim: int = 11,
        autosave: bool = True,
    ) -> None:
        super().__init__()
        self.lr = float(lr)
        self.weights_path: Path = _as_path(weights_path) or Path("./routing_q_weights.json")
        self.feature_dim = int(feature_dim)
        self.weights = np.zeros((self.feature_dim, 5), dtype=np.float64)
        self.autosave = autosave
        self._dirty = False
        self._load_state()

    def _load_state(self) -> None:
        if self.weights_path.exists():
            try:
                with open(self.weights_path, "r") as f:
                    data = json.load(f)
                w = np.array(data, dtype=np.float64)
                if w.shape == (self.feature_dim, 5):
                    self.weights = w
                    logger.info(f"Loaded Q-teacher weights from {self.weights_path}")
                else:
                    logger.warning(
                        f"Loaded Q-weights shape {w.shape}, expected "
                        f"({self.feature_dim},5). Reinitializing."
                    )
            except Exception as e:
                logger.error(f"Failed to load Q-weights: {e}")

    def save(self) -> None:
        if not self._dirty and self.autosave:
            return
        try:
            with open(self.weights_path, "w") as f:
                json.dump(self.weights.tolist(), f, indent=2)
            self._dirty = False
        except Exception as e:
            logger.error(f"Failed to save Q-weights: {e}")

    def predict(self, state: NodeState) -> np.ndarray:
        x = self._features(state)
        return self._predict_from_vector(x)

    def _predict_from_vector(self, x: np.ndarray) -> np.ndarray:
        q = x @ self.weights
        return _softmax(q)

    def confidence(self, state: NodeState) -> float:
        x = self._features(state)
        q = x @ self.weights
        spread = float(np.max(q) - np.min(q))
        # Map [0, 2] spread to [0.3, 1.0]
        return float(np.clip(0.3 + 0.35 * spread, 0.0, 1.0))

    # ------------------------------------------------------------------
    def update_from_vector(self, state_vec: np.ndarray, action: int, reward: float) -> None:
        """Q-learning style update using a scaled feature vector."""
        x = np.asarray(state_vec, dtype=np.float64).reshape(-1)
        if x.shape[0] != self.feature_dim:
            return
        if not 0 <= int(action) < 5:
            return
        q_current = float(np.dot(x, self.weights[:, action]))
        self.weights[:, action] += self.lr * (reward - q_current) * x
        self._dirty = True


class RLHFTeacher(Teacher):
    """Teacher influenced by human feedback."""

    def __init__(self) -> None:
        super().__init__()

    def predict(self, state: NodeState) -> np.ndarray:
        probs = np.ones(5, dtype=np.float64) / 5
        hf = state.human_feedback_score
        if hf > 0.7:
            probs[0] += 0.1  # carbon first
            probs[1] += 0.1  # latency first
        elif hf < 0.3:
            probs[2] += 0.1  # cost first
        return _softmax(probs)

    def confidence(self, state: NodeState) -> float:
        return 0.7 if abs(state.human_feedback_score - 0.5) > 0.3 else 0.4


# ============================================================================
# Student
# ============================================================================

class DistillationStudent:
    """Linear softmax student trained via distillation + RL gradient."""

    def __init__(self, feature_dim: int = 11, n_classes: int = 5, lr: float = 0.01,
                 grad_clip: float = 1.0, seed: Optional[int] = None) -> None:
        rng = np.random.default_rng(seed)
        self.feature_dim = int(feature_dim)
        self.n_classes = int(n_classes)
        self.lr = float(lr)
        self.grad_clip = float(grad_clip)
        # Small random init to break symmetry (avoids all-zero student stuck)
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
        """teacher_outputs: matrix (n_teachers, n_actions)"""
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
            list(actions),
            np.array(rewards),
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
    Learnable gating network that mixes teacher outputs.

    Uses the full softmax Jacobian for correct gradients, plus an optional
    load-balancing regularizer to avoid teacher collapse.
    """

    def __init__(
        self,
        feature_dim: int = 11,
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
        # Running estimate of mean gate weight for load balancing
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
        """
        Update gate parameters using the full softmax Jacobian.

        teacher_outputs:     (n_experts, n_actions)
        student_probs:       (n_actions,)
        current_gate_weights:(n_experts,)
        """
        teacher_outputs = np.asarray(teacher_outputs, dtype=np.float64)
        student_probs = np.asarray(student_probs, dtype=np.float64)
        g = np.asarray(current_gate_weights, dtype=np.float64)

        combined = g @ teacher_outputs                  # (n_actions,)
        error = combined - student_probs                # (n_actions,)
        dL_dg = teacher_outputs @ error                 # (n_experts,)

        # Softmax Jacobian: dL/dz_k = g_k * (dL/dg_k - <g, dL/dg>)
        dot = float(np.dot(g, dL_dg))
        dL_dz = g * (dL_dg - dot)

        # Load-balancing regularizer (encourage uniform expert usage)
        if self.load_balance_weight > 0:
            self._mean_gate = 0.99 * self._mean_gate + 0.01 * g
            target = np.ones_like(g) / len(g)
            lb_grad = self.load_balance_weight * (self._mean_gate - target)
            # Backprop through softmax approximately
            lb_dot = float(np.dot(g, lb_grad))
            dL_dz += g * (lb_grad - lb_dot)

        # Gradient step
        self.weights -= self.lr * np.outer(state_vec, dL_dz)
        self.bias -= self.lr * dL_dz


# ============================================================================
# Federated Weights Codec
# ============================================================================

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
            arr = np.frombuffer(raw, dtype=v["dtype"]).reshape(v["shape"]).copy()
            out[k] = arr
        return out


# ============================================================================
# Distillation Routing Optimizer
# ============================================================================

class DistillationRoutingOptimizer:
    """
    Multi-teacher on-policy distillation agent for routing strategy selection.

    Key enhancements vs. v2.3.0:
      - Student distills from the *gated* teacher combination, not the mean.
      - Q-teacher is actually updated during batch training.
      - MoE gating uses the full softmax Jacobian + load balancing.
      - Optional RunningScaler normalizes features online.
      - All hyperparameters are config-driven.
    """

    STRATEGIES = [s.value for s in RoutingStrategy]

    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config
        self.n_actions = 5
        self.feature_dim = 11
        seed = config.get("seed", None)

        self.scaler = RunningScaler(self.feature_dim, clip=config.get("scaler_clip", 5.0))

        # Student
        self.student = DistillationStudent(
            feature_dim=self.feature_dim,
            lr=config.get("distillation_learning_rate", 0.01),
            grad_clip=config.get("student_grad_clip", 1.0),
            seed=seed,
        )

        # Teachers
        self.teachers: List[Teacher] = [
            RoutingRuleBasedTeacher(),
            RoutingHistoricalMLTeacher(model_path=_as_path(config.get("historical_model_path"))),
            RoutingStatefulQTeacher(
                lr=config.get("q_learning_rate", 0.1),
                weights_path=_as_path(config.get("q_weights_path")),
                feature_dim=self.feature_dim,
                autosave=config.get("q_autosave", True),
            ),
            RLHFTeacher(),
        ]

        # Optional quantum teacher
        if config.get("use_quantum_teacher", False) and QuantumTeacher is not None:
            try:
                self.teachers.append(QuantumTeacher(
                    n_actions=self.n_actions,
                    n_qubits=config.get("quantum_teacher_qubits", 4),
                    n_layers=config.get("quantum_teacher_layers", 2),
                ))
            except Exception as e:
                logger.warning(f"Failed to init quantum teacher: {e}")

        # Wire scaler into every teacher
        for t in self.teachers:
            t.scaler = self.scaler

        # Keep a direct reference to the Q teacher so we can update it in training
        self.q_teacher: Optional[RoutingStatefulQTeacher] = next(
            (t for t in self.teachers if isinstance(t, RoutingStatefulQTeacher)), None
        )

        self.n_teachers = len(self.teachers)

        # Gating
        self.gating = MoEGatingNetwork(
            feature_dim=self.feature_dim,
            n_experts=self.n_teachers,
            lr=config.get("gating_learning_rate", 0.005),
            load_balance_weight=config.get("gating_load_balance_weight", 0.01),
            seed=seed,
        )

        # Buffers and training control
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

        # Observability metrics
        self.last_teacher_agreement: float = 0.0
        self.last_gate_entropy: float = 0.0
        self.last_student_loss: float = 0.0

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    def _compute_teacher_probs(
        self, state: NodeState
    ) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """Return (combined_probs, gate_weights, teacher_outputs_matrix)."""
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

        teacher_outputs = np.array(teacher_outputs)  # (n_teachers, n_actions)
        gate_weights = self.gating.forward(state_vec)
        combined = gate_weights @ teacher_outputs
        s = combined.sum()
        combined = combined / s if s > 0 else np.ones(self.n_actions) / self.n_actions
        return combined, gate_weights, teacher_outputs

    def _track_metrics(self, teacher_outputs: np.ndarray, gate_weights: np.ndarray,
                       combined: np.ndarray, student_probs: np.ndarray) -> None:
        # Teacher agreement: mean pairwise KL -> convert to similarity
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
    # Public API
    # ------------------------------------------------------------------
    async def select_strategy(
        self, state: NodeState, exploration: bool = True
    ) -> Tuple[str, int, np.ndarray, np.ndarray]:
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

            return self.STRATEGIES[action_idx], action_idx, state_vec, teacher_outputs

    async def update(
        self,
        state_vec: np.ndarray,
        action_idx: int,
        reward: float,
        next_state_vec: np.ndarray,
        teacher_outputs: np.ndarray,
    ) -> None:
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
                a = actions[i]
                r = float(rewards[i])

                # 1. Compute gated teacher target
                gate_w = self.gating.forward(s_vec)
                combined = gate_w @ t_outs
                s = combined.sum()
                if s <= 0:
                    combined = np.ones(self.n_actions) / self.n_actions
                else:
                    combined = combined / s

                # 2. Student update against gated teacher target
                self.student.update(
                    s_vec, combined, r, a,
                    distill_weight=self.distill_weight,
                    rl_weight=self.rl_weight,
                )

                # 3. Q-teacher online update
                if self.q_teacher is not None:
                    self.q_teacher.update_from_vector(s_vec, a, r)

                # 4. Gating update using full softmax Jacobian
                student_out = self.student.predict_proba(s_vec)
                self.gating.update(s_vec, t_outs, student_out, gate_w)

    async def export_student_weights(self) -> Dict[str, np.ndarray]:
        async with self.lock:
            return self.student.export_weights()

    async def import_student_weights(self, weights: Dict[str, np.ndarray]) -> None:
        async with self.lock:
            self.student.import_weights(weights)

    async def export_federated_blob(self) -> Dict[str, Any]:
        """Encode student weights as a versioned, base64-JSON blob."""
        w = await self.export_student_weights()
        return FederatedWeightsCodec.encode(w)

    async def import_federated_blob(self, blob: Dict[str, Any]) -> None:
        weights = FederatedWeightsCodec.decode(blob)
        await self.import_student_weights(weights)

    def get_stats(self) -> Dict[str, Any]:
        return {
            "student_counter": self.student.counter,
            "buffer_size": len(self.replay_buffer),
            "n_teachers": self.n_teachers,
            "epsilon": self.epsilon,
            "teacher_agreement": self.last_teacher_agreement,
            "gate_entropy": self.last_gate_entropy,
            "student_loss": self.last_student_loss,
            "scaler_count": self.scaler.count,
        }

    def flush(self) -> None:
        """Synchronously persist any dirty teacher state (e.g., Q-teacher)."""
        if self.q_teacher is not None:
            self.q_teacher.save()


# ============================================================================
# Enhanced NodeDescriptor
# ============================================================================

class NodeDescriptor(BaseModel):
    """Descriptor for a compute node with adaptive routing strategy selection."""
    model_config = ConfigDict(extra="allow", arbitrary_types_allowed=True)

    id: str = Field(..., description="Unique node identifier")
    type: NodeType = Field(..., description="Type of the node")
    region: str = Field(..., description="Geographic region")
    location_lat: Optional[float] = Field(None, description="Latitude of the node")
    location_lon: Optional[float] = Field(None, description="Longitude of the node")
    availability_zone: Optional[str] = Field(None, description="Cloud availability zone")

    region_carbon_intensity: float = Field(..., ge=0, description="kg CO2/kWh")
    carbon_intensity_source: Optional[str] = Field(None, description="Source of carbon intensity data")
    energy_per_token: float = Field(..., gt=0, description="Joules per token")
    helium_connectivity_score: float = Field(0.5, ge=0, le=1, description="Connectivity score for helium-based nodes")
    material_footprint_id: Optional[str] = Field(None, description="Reference to material footprint catalog")
    renewable_fraction: float = Field(0.0, ge=0, le=1, description="Fraction of energy from renewables")
    efficiency_score: Optional[float] = Field(None, ge=0, le=1, description="Overall sustainability efficiency score")

    flops: Optional[float] = Field(None, gt=0, description="Estimated FLOPs per second")
    memory_gb: Optional[float] = Field(None, gt=0, description="Available memory in GB")
    storage_gb: Optional[float] = Field(None, gt=0, description="Available storage in GB")

    cooling_type: CoolingType = Field(CoolingType.AIR, description="Cooling system type")
    network_latency_ms: Optional[float] = Field(None, gt=0, description="Average network latency in milliseconds")

    uptime: float = Field(1.0, ge=0, le=1, description="Fraction of time the node is available")
    cost_per_hour_usd: Optional[float] = Field(None, ge=0, description="Cost per hour in USD")
    maintenance_status: MaintenanceStatus = Field(MaintenanceStatus.OPERATIONAL, description="Maintenance status")
    last_updated: datetime = Field(default_factory=datetime.utcnow, description="Timestamp of last update")

    hardware_model: Optional[str] = Field(None, description="Hardware model identifier")
    manufacturer: Optional[str] = Field(None, description="Hardware manufacturer")

    routing_strategy: RoutingStrategy = Field(RoutingStrategy.BALANCED, description="Current routing strategy")
    performance_history: List[Dict[str, Any]] = Field(default_factory=list, description="Recent routing outcomes (max 100 entries)")
    max_history_length: int = Field(100, ge=1, description="Maximum number of history entries to keep")

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
    _routing_optimizer: Optional[DistillationRoutingOptimizer] = PrivateAttr(default=None)
    _last_decision: Optional[Dict[str, Any]] = PrivateAttr(default=None)
    _causal_shaper: Optional[Any] = PrivateAttr(default=None)
    _evolutionary_optimizer: Optional[Any] = PrivateAttr(default=None)
    _state_lock: asyncio.Lock = PrivateAttr(default_factory=asyncio.Lock)
    _prev_snapshot: Dict[str, float] = PrivateAttr(default_factory=dict)
    _current_snapshot: Dict[str, float] = PrivateAttr(default_factory=dict)
    _log_lock: asyncio.Lock = PrivateAttr(default_factory=asyncio.Lock)

    # ------------------------------------------------------------------
    # Validators
    # ------------------------------------------------------------------
    @field_validator("region_carbon_intensity")
    @classmethod
    def _validate_carbon(cls, v: float) -> float:
        if v < 0:
            raise ValueError("region_carbon_intensity must be non-negative")
        return v

    @field_validator("energy_per_token")
    @classmethod
    def _validate_energy(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("energy_per_token must be positive")
        return v

    @field_validator("location_lat")
    @classmethod
    def _validate_lat(cls, v: Optional[float]) -> Optional[float]:
        if v is not None and not (-90 <= v <= 90):
            raise ValueError("latitude must be between -90 and 90")
        return v

    @field_validator("location_lon")
    @classmethod
    def _validate_lon(cls, v: Optional[float]) -> Optional[float]:
        if v is not None and not (-180 <= v <= 180):
            raise ValueError("longitude must be between -180 and 180")
        return v

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def compute_energy_cost(self, tokens: int) -> float:
        return self.energy_per_token * tokens

    def compute_carbon_cost(self, energy_joules: float) -> float:
        energy_kwh = energy_joules * 2.7778e-7
        return energy_kwh * self.region_carbon_intensity

    def get_health_score(self) -> float:
        base = self.uptime
        if self.maintenance_status == MaintenanceStatus.OPERATIONAL:
            base *= 1.0
        elif self.maintenance_status == MaintenanceStatus.DEGRADED:
            base *= 0.7
        elif self.maintenance_status == MaintenanceStatus.MAINTENANCE:
            base *= 0.3
        else:
            base *= 0.0
        if self.efficiency_score is not None:
            base *= self.efficiency_score
        return float(max(0.0, min(1.0, base)))

    def is_available(self) -> bool:
        return self.maintenance_status in (MaintenanceStatus.OPERATIONAL, MaintenanceStatus.DEGRADED)

    def to_dict(self, exclude_none: bool = False) -> Dict[str, Any]:
        data = self.model_dump()
        if exclude_none:
            return {k: v for k, v in data.items() if v is not None}
        return data

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "NodeDescriptor":
        return cls(**data)

    # ------------------------------------------------------------------
    # Optimizer wiring
    # ------------------------------------------------------------------
    def _ensure_optimizer(self) -> None:
        if self._routing_optimizer is not None:
            return

        md = self.metadata or {}
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
                md.get("q_weights_path", f"./routing_q_weights_{_sanitize_id(self.id)}.json")
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
        self._routing_optimizer = DistillationRoutingOptimizer(config)

        # Evolutionary optimizer (optional)
        if self.use_evolutionary and self._evolutionary_optimizer is None:
            if EvolutionaryOptimizer is None:
                logger.warning("EvolutionaryOptimizer not available; disabling evolutionary blending.")
                self.use_evolutionary = False
            else:
                try:
                    self._evolutionary_optimizer = EvolutionaryOptimizer(
                        n_actions=5,
                        population_size=self.evolutionary_population_size,
                        mutation_rate=self.evolutionary_mutation_rate,
                        crossover_rate=self.evolutionary_crossover_rate,
                        elitism=self.evolutionary_elitism,
                    )
                except Exception as e:
                    logger.warning(f"EvolutionaryOptimizer init failed: {e}")
                    self.use_evolutionary = False

    # ------------------------------------------------------------------
    # Snapshot management
    # ------------------------------------------------------------------
    def set_snapshot_before(self, snapshot: Dict[str, float]) -> None:
        self._prev_snapshot = dict(snapshot or {})

    def set_snapshot_after(self, snapshot: Dict[str, float]) -> None:
        self._current_snapshot = dict(snapshot or {})

    # ------------------------------------------------------------------
    # Decision + outcome
    # ------------------------------------------------------------------
    async def select_routing_strategy(
        self,
        exploration: bool = True,
        snapshot_before: Optional[Dict[str, float]] = None,
    ) -> RoutingStrategy:
        """
        Select the next routing strategy. Caller is responsible for invoking
        ``record_outcome`` after the action completes.
        """
        async with self._state_lock:
            self._ensure_optimizer()
            if snapshot_before is not None:
                self._prev_snapshot = dict(snapshot_before)

            state = self._build_state()
            strategy, action_idx, state_vec, teacher_outputs = await self._routing_optimizer.select_strategy(
                state, exploration=exploration
            )

            # Optional evolutionary blending
            if self.use_evolutionary and self._evolutionary_optimizer is not None:
                try:
                    evo_probs = np.asarray(self._evolutionary_optimizer.get_probs(), dtype=np.float64)
                    if evo_probs.shape[0] == len(self._routing_optimizer.STRATEGIES):
                        s = evo_probs.sum()
                        evo_probs = evo_probs / s if s > 0 else evo_probs
                        combined = 0.7 * teacher_outputs.mean(axis=0) + 0.3 * evo_probs
                        action_idx = int(np.argmax(combined))
                        strategy = self._routing_optimizer.STRATEGIES[action_idx]
                except Exception as e:
                    logger.warning(f"Evolutionary blending failed: {e}")

            self._last_decision = {
                "state_vec": state_vec,
                "action_idx": action_idx,
                "teacher_outputs": teacher_outputs,
                "strategy": strategy,
            }
            self.routing_strategy = RoutingStrategy(strategy)
            return self.routing_strategy

    async def record_outcome(
        self,
        carbon_saved_kg: float,
        latency_ms: float,
        cost_usd: float,
        snapshot_after: Optional[Dict[str, float]] = None,
    ) -> None:
        """
        Record the outcome of the last decision and trigger an optimizer update.
        Must be called *after* ``select_routing_strategy`` (outside its lock).
        """
        async with self._state_lock:
            if self._last_decision is None:
                logger.debug("record_outcome called with no pending decision; ignoring.")
                return

            if snapshot_after is not None:
                self._current_snapshot = dict(snapshot_after)

            # --- Base reward ------------------------------------------------
            carbon_norm = min(1.0, max(0.0, carbon_saved_kg / 0.1))
            latency_norm = 1.0 - min(1.0, max(0.0, latency_ms / 500.0))
            cost_norm = 1.0 - min(1.0, max(0.0, cost_usd / 10.0))

            w_carbon = float(self.metadata.get("reward_weight_carbon", 0.5))
            w_latency = float(self.metadata.get("reward_weight_latency", 0.3))
            w_cost = float(self.metadata.get("reward_weight_cost", 0.2))
            total_w = w_carbon + w_latency + w_cost
            if total_w <= 0:
                total_w = 1.0

            reward = (w_carbon * carbon_norm + w_latency * latency_norm + w_cost * cost_norm) / total_w
            reward = float(max(0.0, min(1.0, reward)))

            # --- Optional causal shaping -----------------------------------
            reward = self._shape_reward_safe(reward)

            # --- Record entry ----------------------------------------------
            entry: Dict[str, Any] = {
                "timestamp": datetime.utcnow().isoformat(),
                "strategy": self._last_decision["strategy"],
                "reward": reward,
                "carbon_saved_kg": carbon_saved_kg,
                "latency_ms": latency_ms,
                "cost_usd": cost_usd,
                "state_vec": ",".join(map(str, self._last_decision["state_vec"])),
            }
            self.performance_history.append(entry)
            if len(self.performance_history) > self.max_history_length:
                self.performance_history = self.performance_history[-self.max_history_length:]

            # --- Optimizer update ------------------------------------------
            state_vec = self._last_decision["state_vec"]
            action_idx = self._last_decision["action_idx"]
            teacher_outputs = self._last_decision["teacher_outputs"]

            next_state = self._build_state()
            next_state_vec = next_state.to_feature_vector(self._routing_optimizer.scaler)

            await self._routing_optimizer.update(
                state_vec, action_idx, reward, next_state_vec, teacher_outputs
            )

            self._last_decision = None

        # --- Async persistence (outside lock, best-effort) ---------------
        await self._persist_entry_async(entry)

        # --- FeedbackEvent emission --------------------------------------
        if FeedbackEvent is not None:
            try:
                event = FeedbackEvent(
                    source="node_descriptor",
                    feedback_type="routing",
                    task_id=str(self.id),
                    context={"node_id": self.id},
                    action={
                        "selected_action": entry["strategy"],
                        "selected_rank": 1,
                        "confidence_score": self.metadata.get("confidence", 0.5),
                    },
                    performance={
                        "quality_score": reward,
                        "latency_ms": latency_ms,
                        "energy_joules": 0,
                        "carbon_g": carbon_saved_kg * 1000,
                        "helium_cost": 0,
                        "duration_ms": 0,
                    },
                    adaptive_cost_value=reward,
                    tags=["node", "routing", entry["strategy"]],
                )
                logger.debug(f"FeedbackEvent created: {getattr(event, 'event_id', '?')}")
            except Exception as e:
                logger.warning(f"Failed to create FeedbackEvent: {e}")

    # ------------------------------------------------------------------
    # Causal shaping helper
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
    # Async log persistence (append-only)
    # ------------------------------------------------------------------
    async def _persist_entry_async(self, entry: Dict[str, Any]) -> None:
        try:
            async with self._log_lock:
                await asyncio.to_thread(self._append_log_sync, entry)
        except Exception as e:
            logger.error(f"Failed to persist routing log: {e}")

    def _append_log_sync(self, entry: Dict[str, Any]) -> None:
        safe_id = _sanitize_id(self.id)
        csv_path = Path(f"./node_{safe_id}_routing_logs.csv")
        exists = csv_path.exists()
        # Flatten list/dict values to JSON strings for CSV safety
        flat = {
            k: (json.dumps(v) if isinstance(v, (dict, list)) else v)
            for k, v in entry.items()
        }
        pd.DataFrame([flat]).to_csv(
            csv_path, mode="a", header=not exists, index=False
        )

    # ------------------------------------------------------------------
    # State construction
    # ------------------------------------------------------------------
    def _build_state(self) -> NodeState:
        if self.performance_history:
            recent = self.performance_history[-20:]
            rewards = [float(r.get("reward", 0.0)) for r in recent]
            success_rate = sum(1 for r in rewards if r > 0.5) / max(len(rewards), 1)
            avg_reward = float(np.mean(rewards)) if rewards else 0.0
        else:
            success_rate = 0.5
            avg_reward = 0.5

        graph_health = float((self.graph_metrics or {}).get("centrality", 0.5))
        graph_health = max(0.0, min(1.0, graph_health))
        health_score = max(0.0, min(1.0, self.get_health_score() * (0.8 + 0.2 * graph_health)))

        efficiency = self.efficiency_score if self.efficiency_score is not None else 0.5
        cost = self.cost_per_hour_usd if self.cost_per_hour_usd is not None else 1.0

        return NodeState(
            carbon_intensity=self.region_carbon_intensity,
            renewable_fraction=self.renewable_fraction,
            helium_connectivity=self.helium_connectivity_score,
            uptime=self.uptime,
            efficiency_score=efficiency,
            health_score=health_score,
            cost_per_hour=cost,
            energy_per_token=self.energy_per_token,
            recent_success_rate=success_rate,
            avg_reward=avg_reward,
            human_feedback_score=self.human_feedback_score,
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
        return await self._routing_optimizer.export_student_weights()

    async def import_federated_weights(self, weights: Dict[str, np.ndarray]) -> None:
        self._ensure_optimizer()
        await self._routing_optimizer.import_student_weights(weights)

    async def export_federated_blob(self) -> Dict[str, Any]:
        self._ensure_optimizer()
        return await self._routing_optimizer.export_federated_blob()

    async def import_federated_blob(self, blob: Dict[str, Any]) -> None:
        self._ensure_optimizer()
        await self._routing_optimizer.import_federated_blob(blob)

    # ------------------------------------------------------------------
    def get_optimizer_stats(self) -> Dict[str, Any]:
        self._ensure_optimizer()
        return self._routing_optimizer.get_stats()

    def flush(self) -> None:
        if self._routing_optimizer is not None:
            self._routing_optimizer.flush()


# ============================================================================
# Convenience factory
# ============================================================================

def create_node_descriptor(
    id: str,
    node_type: NodeType,
    region: str,
    region_carbon_intensity: float,
    energy_per_token: float,
    **kwargs: Any,
) -> NodeDescriptor:
    """Factory function to create a NodeDescriptor with sensible defaults."""
    return NodeDescriptor(
        id=id,
        type=node_type,
        region=region,
        region_carbon_intensity=region_carbon_intensity,
        energy_per_token=energy_per_token,
        **kwargs,
    )


__all__ = [
    "NodeType",
    "CoolingType",
    "MaintenanceStatus",
    "RoutingStrategy",
    "RunningScaler",
    "NodeState",
    "Teacher",
    "RoutingRuleBasedTeacher",
    "RoutingHistoricalMLTeacher",
    "RoutingStatefulQTeacher",
    "RLHFTeacher",
    "DistillationStudent",
    "ReplayBuffer",
    "MoEGatingNetwork",
    "FederatedWeightsCodec",
    "DistillationRoutingOptimizer",
    "NodeDescriptor",
    "create_node_descriptor",
]


# ============================================================================
# Tests
# ============================================================================

def _test_running_scaler() -> None:
    s = RunningScaler(3)
    for x in [[1, 2, 3], [2, 3, 4], [3, 4, 5], [4, 5, 6]]:
        s.update(np.array(x, dtype=np.float64))
    out = s.transform(np.array([1, 2, 3], dtype=np.float64))
    assert out.shape == (3,)
    assert np.all(np.isfinite(out))


def _test_carbon_scaling() -> None:
    st = NodeState(
        carbon_intensity=0.4, renewable_fraction=0.5, helium_connectivity=0.5,
        uptime=1.0, efficiency_score=0.8, health_score=0.9,
        cost_per_hour=2.0, energy_per_token=1e-4,
        recent_success_rate=0.5, avg_reward=0.5,
    )
    v = st.to_feature_vector()
    # Previously this collapsed to ~0.0004 due to unit mismatch.
    assert v[0] > 0.1, f"carbon scaling regressed: {v[0]}"


def _test_rule_teacher_threshold() -> None:
    t = RoutingRuleBasedTeacher()
    high_carbon = NodeState(
        carbon_intensity=0.6, renewable_fraction=0.2, helium_connectivity=0.5,
        uptime=1.0, efficiency_score=0.7, health_score=0.9,
        cost_per_hour=2.0, energy_per_token=1e-4,
        recent_success_rate=0.5, avg_reward=0.5,
    )
    p = t.predict(high_carbon)
    assert p[0] == max(p), f"carbon_first should dominate: {p}"


def _test_falsy_fallback_fix() -> None:
    node = NodeDescriptor(
        id="falsy_test",
        type=NodeType.EDGE,
        region="us-west",
        region_carbon_intensity=0.3,
        energy_per_token=1e-4,
        cost_per_hour_usd=0.0,
        efficiency_score=0.0,
    )
    state = node._build_state()
    assert state.cost_per_hour == 0.0
    assert state.efficiency_score == 0.0


def _test_federated_codec_roundtrip() -> None:
    w = {"weights": np.random.randn(11, 5), "biases": np.random.randn(5)}
    blob = FederatedWeightsCodec.encode(w)
    back = FederatedWeightsCodec.decode(blob)
    assert np.allclose(w["weights"], back["weights"])
    assert np.allclose(w["biases"], back["biases"])


def _test_gating_jacobian_shapes() -> None:
    g = MoEGatingNetwork(feature_dim=11, n_experts=4, lr=0.01)
    s = np.random.randn(11)
    t_outs = np.random.rand(4, 5)
    t_outs = t_outs / t_outs.sum(axis=1, keepdims=True)
    student = np.random.rand(5)
    student = student / student.sum()
    gw = g.forward(s)
    g.update(s, t_outs, student, gw)
    assert g.weights.shape == (11, 4)
    assert g.bias.shape == (4,)


async def _async_test_node_descriptor() -> None:
    node = NodeDescriptor(
        id="test/with unsafe chars!",
        type=NodeType.EDGE,
        region="us-west",
        region_carbon_intensity=0.4,
        energy_per_token=1e-4,
        cost_per_hour_usd=2.0,
        metadata={"seed": 42},
    )
    strat = await node.select_routing_strategy(exploration=False)
    assert strat in RoutingStrategy
    await node.record_outcome(carbon_saved_kg=0.05, latency_ms=100.0, cost_usd=1.0)

    # No deadlock: select again immediately
    strat2 = await node.select_routing_strategy(exploration=True)
    assert strat2 in RoutingStrategy

    # Federated blob round-trip
    blob = await node.export_federated_blob()
    await node.import_federated_blob(blob)

    stats = node.get_optimizer_stats()
    assert stats["n_teachers"] >= 4
    node.flush()


def _run_all_tests() -> None:
    _test_running_scaler()
    _test_carbon_scaling()
    _test_rule_teacher_threshold()
    _test_falsy_fallback_fix()
    _test_federated_codec_roundtrip()
    _test_gating_jacobian_shapes()
    asyncio.run(_async_test_node_descriptor())
    print("All tests passed.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    _run_all_tests()
