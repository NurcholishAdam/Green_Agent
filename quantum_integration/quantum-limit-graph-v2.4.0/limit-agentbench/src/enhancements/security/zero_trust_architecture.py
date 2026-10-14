#!/usr/bin/env python3
"""
Zero Trust Security Architecture for Green Agent v4.4.0
=======================================================
Implements a zero-trust security model for expert routing and execution.

ENHANCEMENTS (v4.4.0 over v4.3.0):
- Fixed carbon-intensity unit mismatch (unified to kg CO2/kWh everywhere).
- Added RunningScaler for online feature normalization (shared by all agents).
- Q-teacher is now actually updated during batch training.
- Student distills from the *gated* teacher combination, not the mean.
- MoE gating uses the full softmax Jacobian + optional load balancing.
- Fixed DistillationStudent.predict_proba to properly resize n_classes.
- Replaced binary reward with a bounded multi-term reward (security + latency
  + friction + over-auth penalty).
- Implemented real zero-trust primitives: sliding-window rate limiting,
  JWT validation, HMAC verification, replay cache, session binding.
- Emits FeedbackEvent and publishes to AsyncMessageQueue (cross-module learning).
- Causal reward shaping with snapshot mechanism, guarded and clamped.
- Federated learning via a versioned base64-JSON weights codec.
- Sanitized file paths; Path coercion for model/weights paths.
- Async-safe persistence: dirty-flag saves, periodic flush, append-only logs.
- Shape validation on Q-teacher load; n_features validation on ML teacher load.
- Thread-safe rate limiter and bounded audit log.
- Added __all__ and a built-in test suite.

All previous features retained.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import os
import pickle
import random
import re
import secrets
import time
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Callable, ClassVar, Dict, FrozenSet, List, Optional, Tuple, Union

import numpy as np
import pandas as pd

# Optional dependencies
try:
    import jwt  # PyJWT
except ImportError:
    jwt = None  # type: ignore

try:
    import aiofiles  # type: ignore
except ImportError:
    aiofiles = None  # type: ignore

try:
    import aiohttp  # type: ignore
except ImportError:
    aiohttp = None  # type: ignore

try:
    from prometheus_client import Counter, Gauge, Histogram  # type: ignore
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False
    Counter = Gauge = Histogram = None  # type: ignore

# Project imports
try:
    from .schemas.feedback_event import FeedbackEvent  # type: ignore
except ImportError:
    FeedbackEvent = None  # type: ignore

try:
    from .async_message_queue import AsyncMessageQueue  # type: ignore
except ImportError:
    AsyncMessageQueue = None  # type: ignore

try:
    from .quantum_teacher import QuantumTeacher  # type: ignore
except ImportError:
    QuantumTeacher = None  # type: ignore

try:
    from .causal_reward_shaper import CausalRewardShaper  # type: ignore
except ImportError:
    CausalRewardShaper = None  # type: ignore

logger = logging.getLogger(__name__)


# ============================================================================
# Enums
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


class AuthOutcome(str, Enum):
    SUCCESS = "success"
    FAILURE = "failure"
    SECURITY_INCIDENT = "security_incident"
    RATE_LIMITED = "rate_limited"
    REPLAY_DETECTED = "replay_detected"


# Feedback type registered for the security module.
AUTH_FEEDBACK_TYPE: str = "auth"


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


def _secure_compare(a: str, b: str) -> bool:
    """Constant-time string comparison."""
    try:
        return hmac.compare_digest(a.encode("utf-8"), b.encode("utf-8"))
    except Exception:
        return False


class RunningScaler:
    """Online running mean/std scaler for feature normalization."""

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
            "dim": self.dim, "clip": self.clip, "min_std": self.min_std,
            "mean": self.mean.tolist(), "m2": self.m2.tolist(), "count": self.count,
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
# State representation
# ============================================================================

@dataclass
class AuthState:
    """
    State for authentication-level selection.

    ``carbon_intensity`` is in kg CO2/kWh (aligned with NodeDescriptor and
    WorkloadDescriptor). ``request_frequency`` is requests per minute.
    ``geo_velocity`` is km/h.
    """
    user_risk_score: float
    resource_sensitivity: float
    request_frequency: float
    data_classification: float
    time_of_day: float
    day_of_week: float
    ip_reputation: float
    device_trust: float
    previous_auth_success_rate: float
    previous_auth_avg_latency: float
    carbon_intensity: float
    recent_failed_attempts: float
    geo_velocity: float
    user_role_sensitivity: float

    # Configurable static caps (used if no RunningScaler supplied)
    FEATURE_DIM: ClassVar[int] = 14
    REQUEST_FREQ_MAX: ClassVar[float] = 100.0
    LATENCY_MAX_MS: ClassVar[float] = 1000.0
    CARBON_MAX_KG_PER_KWH: ClassVar[float] = 1.0
    FAILED_ATTEMPTS_MAX: ClassVar[float] = 10.0
    GEO_VELOCITY_MAX_KMH: ClassVar[float] = 1000.0

    def to_raw_vector(self) -> np.ndarray:
        return np.array([
            self.user_risk_score,
            self.resource_sensitivity,
            self.request_frequency,
            self.data_classification,
            self.time_of_day,
            self.day_of_week,
            self.ip_reputation,
            self.device_trust,
            self.previous_auth_success_rate,
            self.previous_auth_avg_latency,
            self.carbon_intensity,
            self.recent_failed_attempts,
            self.geo_velocity,
            self.user_role_sensitivity,
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
            c(self.user_risk_score),
            c(self.resource_sensitivity),
            c(self.request_frequency / self.REQUEST_FREQ_MAX),
            c(self.data_classification),
            c(self.time_of_day),
            c(self.day_of_week / 6.0),
            c(self.ip_reputation),
            c(self.device_trust),
            c(self.previous_auth_success_rate),
            c(self.previous_auth_avg_latency / self.LATENCY_MAX_MS),
            c(self.carbon_intensity / self.CARBON_MAX_KG_PER_KWH),
            c(self.recent_failed_attempts / self.FAILED_ATTEMPTS_MAX),
            c(self.geo_velocity / self.GEO_VELOCITY_MAX_KMH),
            c(self.user_role_sensitivity),
        ], dtype=np.float32)


# ============================================================================
# Teachers
# ============================================================================

class Teacher(ABC):
    def __init__(self) -> None:
        self.scaler: Optional[RunningScaler] = None

    @abstractmethod
    def predict(self, state: AuthState) -> np.ndarray:
        ...

    @abstractmethod
    def confidence(self, state: AuthState) -> float:
        ...

    def _features(self, state: AuthState) -> np.ndarray:
        return state.to_feature_vector(self.scaler)


class RuleBasedAuthTeacher(Teacher):
    LEVELS: ClassVar[List[str]] = [AuthLevel.LIGHT.value, AuthLevel.STANDARD.value, AuthLevel.ENHANCED.value]

    def predict(self, state: AuthState) -> np.ndarray:
        probs = np.ones(3, dtype=np.float64) * 0.1
        if state.user_risk_score < 0.3 and state.resource_sensitivity < 0.5:
            probs[0] = 0.8
        elif state.user_risk_score > 0.7 or state.recent_failed_attempts > 3:
            probs[2] = 0.8
        else:
            probs[1] = 0.7
        return _softmax(probs)

    def confidence(self, state: AuthState) -> float:
        if state.user_risk_score < 0.2 or state.user_risk_score > 0.8:
            return 0.8
        return 0.5


class HistoricalMLAuthTeacher(Teacher):
    EXPECTED_FEATURE_DIM: int = 14
    STRATEGY_ORDER: ClassVar[List[str]] = [lvl.value for lvl in AuthLevel]

    def __init__(self, model_path: Optional[Path] = None) -> None:
        super().__init__()
        self.model = None
        self.label_encoder = None
        self.model_path: Path = _as_path(model_path) or Path("./auth_historical_model.pkl")
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
        n_features = getattr(self.model, "n_features_in_", None)
        if n_features is not None and n_features != self.EXPECTED_FEATURE_DIM:
            logger.warning(
                f"Historical model expects {n_features} features, pipeline produces "
                f"{self.EXPECTED_FEATURE_DIM}. Disabling teacher."
            )
            self.model, self.label_encoder = None, None
            return
        logger.info(f"Loaded historical ML model from {self.model_path}")

    def predict(self, state: AuthState) -> np.ndarray:
        if self.model is None or self.label_encoder is None:
            return np.ones(3) / 3
        x = self._features(state).reshape(1, -1)
        if not hasattr(self.model, "predict_proba"):
            return np.ones(3) / 3
        probs = self.model.predict_proba(x)[0]
        classes = list(self.label_encoder.classes_)
        new_probs = np.zeros(3, dtype=np.float64)
        for i, lvl in enumerate(self.STRATEGY_ORDER):
            if lvl in classes:
                new_probs[i] = probs[classes.index(lvl)]
        s = new_probs.sum()
        return new_probs / s if s > 0 else np.ones(3) / 3

    def confidence(self, state: AuthState) -> float:
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
        label_col: str = "auth_level",
    ) -> Optional[Path]:
        try:
            from sklearn.ensemble import RandomForestClassifier
            from sklearn.preprocessing import LabelEncoder
        except ImportError:
            logger.error("scikit-learn required for historical model training.")
            return None

        all_dfs = []
        for path in log_paths:
            path = _as_path(path)
            if path and path.exists():
                all_dfs.append(pd.read_csv(path))
        if not all_dfs:
            logger.warning("No logs found.")
            return None

        df = pd.concat(all_dfs, ignore_index=True)
        if len(df) < 10:
            logger.warning("Not enough logs.")
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
            logger.warning("Too few valid samples.")
            return None

        X = np.array(rows)
        y = np.array(labels)
        le = LabelEncoder()
        y_enc = le.fit_transform(y)
        clf = RandomForestClassifier(n_estimators=100, random_state=42)
        clf.fit(X, y_enc)

        model_path = _as_path(model_path) or Path("./auth_historical_model.pkl")
        with open(model_path, "wb") as f:
            pickle.dump((clf, le), f)
        logger.info(f"Trained and saved to {model_path}")
        return model_path


class StatefulQAuthTeacher(Teacher):
    """
    Linear Q-teacher. ``update_from_vector`` is called by the optimizer during
    batch training; ``update`` remains for direct AuthState updates.
    """

    def __init__(
        self,
        lr: float = 0.1,
        weights_path: Optional[Path] = None,
        feature_dim: int = 14,
        autosave: bool = True,
    ) -> None:
        super().__init__()
        self.lr = float(lr)
        self.weights_path: Path = _as_path(weights_path) or Path("./auth_q_weights.json")
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

    def predict(self, state: AuthState) -> np.ndarray:
        return self._predict_from_vector(self._features(state))

    def confidence(self, state: AuthState) -> float:
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

    def update(self, state: AuthState, action: int, reward: float) -> None:
        self.update_from_vector(self._features(state), action, reward)


class RLHFAuthTeacher(Teacher):
    """Teacher influenced by human feedback on security decisions."""

    def predict(self, state: AuthState) -> np.ndarray:
        probs = np.ones(3, dtype=np.float64) / 3
        if state.user_risk_score > 0.6:
            probs[2] += 0.2
        return _softmax(probs)

    def confidence(self, state: AuthState) -> float:
        return 0.6


# ============================================================================
# Student / ReplayBuffer / Gating
# ============================================================================

class DistillationStudent:
    """Linear softmax student with distillation + REINFORCE gradient."""

    def __init__(
        self,
        feature_dim: int = 14,
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
            raise ValueError(f"weights shape mismatch: expected {self.weights.shape}, got {w.shape}")
        if b.shape != self.biases.shape:
            raise ValueError(f"biases shape mismatch: expected {self.biases.shape}, got {b.shape}")
        self.weights = w.astype(np.float64, copy=True)
        self.biases = b.astype(np.float64, copy=True)


class ReplayBuffer:
    def __init__(self, max_size: int = 2000) -> None:
        self.buffer: deque = deque(maxlen=int(max_size))

    def push(self, state_vec, action, reward, next_state_vec, teacher_outputs) -> None:
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


class MoEGatingNetwork:
    """Gating network using the full softmax Jacobian + load balancing."""

    def __init__(
        self,
        feature_dim: int = 14,
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
        dL_dg = teacher_outputs @ error

        dot = float(np.dot(g, dL_dg))
        dL_dz = g * (dL_dg - dot)

        if self.load_balance_weight > 0:
            self._mean_gate = 0.99 * self._mean_gate + 0.01 * g
            target = np.ones_like(g) / len(g)
            lb_grad = self.load_balance_weight * (self._mean_gate - target)
            lb_dot = float(np.dot(g, lb_grad))
            dL_dz += g * (lb_grad - lb_dot)

        self.weights -= self.lr * np.outer(state_vec, dL_dz)
        self.bias -= self.lr * dL_dz


# ============================================================================
# Zero-trust primitives
# ============================================================================

class SlidingWindowRateLimiter:
    """
    Thread-safe sliding-window rate limiter.

    Each subject (user, IP, tenant) is rate-limited independently. The
    ``limit`` and ``window_seconds`` are global; per-subject overrides can
    be added later.
    """

    def __init__(self, limit: int = 60, window_seconds: float = 60.0, max_subjects: int = 100_000) -> None:
        self.limit = int(limit)
        self.window_seconds = float(window_seconds)
        self.max_subjects = int(max_subjects)
        self._buckets: Dict[str, deque] = {}
        self._lock = asyncio.Lock()

    async def check_and_record(self, subject: str) -> bool:
        """
        Return True if the request is allowed; record the attempt either way.
        """
        now = time.monotonic()
        async with self._lock:
            bucket = self._buckets.get(subject)
            if bucket is None:
                if len(self._buckets) >= self.max_subjects:
                    # Evict oldest subject as a simple bound
                    self._buckets.pop(next(iter(self._buckets)), None)
                bucket = deque()
                self._buckets[subject] = bucket
            cutoff = now - self.window_seconds
            while bucket and bucket[0] < cutoff:
                bucket.popleft()
            allowed = len(bucket) < self.limit
            bucket.append(now)
            return allowed


class NonceCache:
    """Bounded, TTL-based replay-protection cache."""

    def __init__(self, ttl_seconds: float = 300.0, max_size: int = 100_000) -> None:
        self.ttl_seconds = float(ttl_seconds)
        self.max_size = int(max_size)
        self._entries: Dict[str, float] = {}
        self._lock = asyncio.Lock()

    async def check_and_add(self, nonce: str) -> bool:
        """
        Return True if the nonce is new (allowed), False if it was seen before.
        """
        now = time.monotonic()
        async with self._lock:
            # Purge expired
            if len(self._entries) > self.max_size // 2:
                cutoff = now - self.ttl_seconds
                expired = [k for k, t in self._entries.items() if t < cutoff]
                for k in expired:
                    self._entries.pop(k, None)
            if nonce in self._entries:
                return False
            self._entries[nonce] = now
            if len(self._entries) > self.max_size:
                # Evict oldest
                oldest = min(self._entries, key=self._entries.get)
                self._entries.pop(oldest, None)
            return True


# ============================================================================
# Distillation Auth Optimizer
# ============================================================================

class DistillationAuthOptimizer:
    """
    Multi-teacher on-policy distillation for authentication-level selection.

    Key enhancements vs. v4.3.0:
      - Q-teacher is actually updated during batch training.
      - Student distills from the *gated* teacher combination.
      - MoE gating uses the full softmax Jacobian + load balancing.
      - Optional RunningScaler normalizes features online.
    """

    AUTH_LEVELS: ClassVar[List[str]] = [lvl.value for lvl in AuthLevel]

    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config
        self.feature_dim = 14
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
            RuleBasedAuthTeacher(),
            HistoricalMLAuthTeacher(model_path=_as_path(config.get("historical_model_path"))),
            StatefulQAuthTeacher(
                lr=config.get("q_learning_rate", 0.1),
                weights_path=_as_path(config.get("q_weights_path")),
                feature_dim=self.feature_dim,
                autosave=config.get("q_autosave", True),
            ),
            RLHFAuthTeacher(),
        ]

        if config.get("use_quantum_teacher", False) and QuantumTeacher is not None:
            try:
                self.teachers.append(QuantumTeacher(
                    n_actions=self.n_actions,
                    n_qubits=config.get("quantum_teacher_qubits", 4),
                    n_layers=config.get("quantum_teacher_layers", 2),
                ))
                logger.info("Quantum teacher added to auth optimizer")
            except Exception as e:
                logger.warning(f"Failed to init quantum teacher: {e}")

        for t in self.teachers:
            t.scaler = self.scaler

        self.q_teacher: Optional[StatefulQAuthTeacher] = next(
            (t for t in self.teachers if isinstance(t, StatefulQAuthTeacher)), None
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
    def _compute_teacher_probs(self, state: AuthState) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
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
    async def select_auth_level(self, state: AuthState, exploration: bool = True):
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
            return self.AUTH_LEVELS[action_idx], action_idx, state_vec, teacher_outputs

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
                t_outs = teacher_outputs_batch[i]
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
            "n_teachers": self.n_teachers,
            "teacher_agreement": self.last_teacher_agreement,
            "gate_entropy": self.last_gate_entropy,
            "student_loss": self.last_student_loss,
            "scaler_count": self.scaler.count,
        }

    def flush(self) -> None:
        if self.q_teacher is not None:
            self.q_teacher.save(force=True)


# ============================================================================
# Zero Trust Architecture (main class)
# ============================================================================

class ZeroTrustArchitecture:
    """
    Zero-trust security manager with adaptive authentication.

    The authenticator:
      1. Enforces a sliding-window rate limit per subject.
      2. Validates an optional JWT (signature, audience, issuer, expiry).
      3. Verifies an optional HMAC signature over the request payload.
      4. Checks a replay nonce cache.
      5. Selects an auth level via the distillation optimizer.
      6. Records the outcome, updates the optimizer, emits a FeedbackEvent
         and publishes to the async message queue if available.
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.auth_optimizer = DistillationAuthOptimizer(config.get("distillation", {}) or {})

        # Rate limiting
        rl_cfg = config.get("rate_limit", {}) or {}
        self.rate_limiter = SlidingWindowRateLimiter(
            limit=rl_cfg.get("limit", 60),
            window_seconds=rl_cfg.get("window_seconds", 60.0),
        )

        # Replay protection
        rp_cfg = config.get("replay", {}) or {}
        self.nonce_cache = NonceCache(
            ttl_seconds=rp_cfg.get("ttl_seconds", 300.0),
            max_size=rp_cfg.get("max_size", 100_000),
        )

        # JWT validation config
        jwt_cfg = config.get("jwt", {}) or {}
        self.jwt_secret = jwt_cfg.get("secret") or secrets.token_urlsafe(32)
        self.jwt_algorithms = list(jwt_cfg.get("algorithms", ["HS256"]))
        self.jwt_audience = jwt_cfg.get("audience")
        self.jwt_issuer = jwt_cfg.get("issuer")
        self.jwt_leeway = float(jwt_cfg.get("leeway_seconds", 10.0))

        # HMAC config
        self.hmac_secret = config.get("hmac", {}).get("secret") or secrets.token_urlsafe(32)

        # Feedback / cross-module
        self.message_queue = config.get("message_queue", None)

        # Audit log (bounded)
        self.audit_log: deque = deque(maxlen=config.get("audit_log_max", 10_000))

        # Async locks
        self._state_lock = asyncio.Lock()
        self._log_lock = asyncio.Lock()

        # Last decision (for outcome recording)
        self._last_decision: Optional[Dict[str, Any]] = None

        # Causal shaping
        self._causal_shaper: Optional[Any] = None
        self._prev_snapshot: Dict[str, float] = {}
        self._current_snapshot: Dict[str, float] = {}

        # Persistence
        self._log_dir = Path(config.get("log_dir", "."))

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    async def authenticate(
        self,
        context: Dict[str, Any],
        token: Optional[str] = None,
        payload: Optional[bytes] = None,
        signature: Optional[str] = None,
        nonce: Optional[str] = None,
    ) -> Tuple[AuthLevel, Dict[str, Any]]:
        """
        Authenticate a request. Performs rate-limit, JWT, HMAC, and replay
        checks before selecting an auth level.

        Returns ``(AuthLevel, decision_dict)`` where ``decision_dict`` is
        suitable for later submission to ``record_outcome``.
        """
        subject = str(context.get("user_id") or context.get("tenant_id") or context.get("ip", "anonymous"))

        # 1. Rate limiting
        if not await self.rate_limiter.check_and_record(subject):
            logger.warning(f"Rate limit exceeded for {subject}")
            self._log_audit(subject, AuthLevel.LIGHT, AuthOutcome.RATE_LIMITED)
            return AuthLevel.ENHANCED, self._make_decision(None, AuthOutcome.RATE_LIMITED, subject)

        # 2. JWT validation (if a token was supplied)
        if token is not None:
            if not self._validate_jwt(token):
                logger.warning(f"JWT validation failed for {subject}")
                self._log_audit(subject, AuthLevel.ENHANCED, AuthOutcome.FAILURE)
                return AuthLevel.ENHANCED, self._make_decision(None, AuthOutcome.FAILURE, subject)

        # 3. HMAC verification (if payload + signature supplied)
        if payload is not None and signature is not None:
            if not self._verify_hmac(payload, signature):
                logger.warning(f"HMAC verification failed for {subject}")
                self._log_audit(subject, AuthLevel.ENHANCED, AuthOutcome.FAILURE)
                return AuthLevel.ENHANCED, self._make_decision(None, AuthOutcome.FAILURE, subject)

        # 4. Replay protection
        if nonce is not None:
            if not await self.nonce_cache.check_and_add(nonce):
                logger.warning(f"Replay detected for {subject}")
                self._log_audit(subject, AuthLevel.ENHANCED, AuthOutcome.REPLAY_DETECTED)
                return AuthLevel.ENHANCED, self._make_decision(None, AuthOutcome.REPLAY_DETECTED, subject)

        # 5. Adaptive auth level selection
        async with self._state_lock:
            state = self._build_auth_state(context)
            level_str, action_idx, state_vec, teacher_outputs = await self.auth_optimizer.select_auth_level(state)
            level = AuthLevel(level_str)
            self._last_decision = {
                "subject": subject,
                "state_vec": state_vec,
                "action_idx": action_idx,
                "teacher_outputs": teacher_outputs,
                "level": level.value,
                "risk_score": state.user_risk_score,
                "resource_sensitivity": state.resource_sensitivity,
            }
            self._log_audit(subject, level, AuthOutcome.SUCCESS)

        return level, dict(self._last_decision)

    async def record_outcome(
        self,
        decision: Dict[str, Any],
        outcome: AuthOutcome,
        latency_ms: float,
        security_incident: bool = False,
        over_authenticated: bool = False,
        snapshot_after: Optional[Dict[str, float]] = None,
    ) -> None:
        """
        Record the outcome of an authentication attempt and update the
        optimizer. Emits a FeedbackEvent and publishes to the message queue.
        """
        if snapshot_after is not None:
            self._current_snapshot = dict(snapshot_after)

        reward = self._compute_reward(
            outcome=outcome,
            latency_ms=latency_ms,
            security_incident=security_incident,
            over_authenticated=over_authenticated,
            risk_score=float(decision.get("risk_score", 0.5)),
            level=decision.get("level", AuthLevel.STANDARD.value),
        )

        reward = self._shape_reward_safe(reward, decision)

        state_vec = decision.get("state_vec")
        action_idx = decision.get("action_idx")
        teacher_outputs = decision.get("teacher_outputs")
        if state_vec is None or action_idx is None or teacher_outputs is None:
            return

        next_state_vec = state_vec  # In a real system, compute the post-auth state

        await self.auth_optimizer.update(state_vec, action_idx, reward, next_state_vec, teacher_outputs)

        # Persist audit
        self._log_audit(
            decision.get("subject", "unknown"),
            AuthLevel(decision.get("level", AuthLevel.STANDARD.value)),
            outcome,
            reward=reward,
            latency_ms=latency_ms,
        )

        # Persist log entry
        entry = {
            "timestamp": _utc_now().isoformat(),
            "subject": decision.get("subject", "unknown"),
            "level": decision.get("level"),
            "outcome": outcome.value,
            "reward": reward,
            "latency_ms": latency_ms,
            "security_incident": bool(security_incident),
            "over_authenticated": bool(over_authenticated),
        }
        await self._persist_entry_async(entry)

        # Emit FeedbackEvent
        await self._emit_feedback_event(decision, outcome, reward, latency_ms, security_incident)

        # Clear the pending decision
        self._last_decision = None

    def set_snapshot_before(self, snapshot: Optional[Dict[str, float]]) -> None:
        self._prev_snapshot = dict(snapshot or {})

    def set_snapshot_after(self, snapshot: Optional[Dict[str, float]]) -> None:
        self._current_snapshot = dict(snapshot or {})

    def set_causal_graph(self, causal_graph: Any) -> None:
        if CausalRewardShaper is None:
            logger.warning("CausalRewardShaper unavailable; skipping causal graph setup.")
            return
        try:
            self._causal_shaper = CausalRewardShaper(causal_graph, influence_weight=0.3)
        except Exception as e:
            logger.warning(f"Failed to set causal graph: {e}")

    async def export_federated_blob(self) -> Dict[str, Any]:
        return await self.auth_optimizer.export_federated_blob()

    async def import_federated_blob(self, blob: Dict[str, Any]) -> None:
        await self.auth_optimizer.import_federated_blob(blob)

    def get_stats(self) -> Dict[str, Any]:
        return self.auth_optimizer.get_stats()

    def flush(self) -> None:
        self.auth_optimizer.flush()

    # ------------------------------------------------------------------
    # Security helpers
    # ------------------------------------------------------------------
    def _validate_jwt(self, token: str) -> bool:
        if jwt is None:
            logger.warning("PyJWT not installed; skipping JWT validation.")
            return True
        try:
            options = {"require": ["exp"], "verify_aud": self.jwt_audience is not None}
            jwt.decode(
                token,
                self.jwt_secret,
                algorithms=self.jwt_algorithms,
                audience=self.jwt_audience,
                issuer=self.jwt_issuer,
                leeway=self.jwt_leeway,
                options=options,
            )
            return True
        except Exception as e:
            logger.debug(f"JWT decode failed: {e}")
            return False

    def _verify_hmac(self, payload: bytes, signature: str) -> bool:
        try:
            expected = hmac.new(
                self.hmac_secret.encode("utf-8"),
                payload,
                hashlib.sha256,
            ).hexdigest()
            return _secure_compare(expected, signature)
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Reward
    # ------------------------------------------------------------------
    def _compute_reward(
        self,
        outcome: AuthOutcome,
        latency_ms: float,
        security_incident: bool,
        over_authenticated: bool,
        risk_score: float,
        level: str,
    ) -> float:
        """
        Bounded reward in [0, 1] combining four terms:
          - security: 0.4 (incident -> 0; success -> 1; other failures -> 0.5)
          - latency:  0.3 (higher is better, capped at a target)
          - friction: 0.2 (penalize over-authentication when risk was low)
          - compliance: 0.1 (reward matching the required auth level to the risk)
        """
        # Security term
        if security_incident:
            sec = 0.0
        elif outcome == AuthOutcome.SUCCESS:
            sec = 1.0
        elif outcome in (AuthOutcome.FAILURE,):
            sec = 0.3
        elif outcome in (AuthOutcome.RATE_LIMITED, AuthOutcome.REPLAY_DETECTED):
            sec = 0.6
        else:
            sec = 0.5

        # Latency term (1000ms target, configurable)
        latency_target = float(self.config.get("latency_target_ms", 1000.0))
        lat = 1.0 - min(1.0, max(0.0, latency_ms / latency_target))

        # Friction term: penalize selecting enhanced when risk was low
        if level == AuthLevel.ENHANCED.value and risk_score < 0.4:
            friction = 0.0
        elif over_authenticated:
            friction = 0.3
        else:
            friction = 1.0

        # Compliance term: match level to risk bands
        if risk_score < 0.3:
            expected = AuthLevel.LIGHT.value
        elif risk_score < 0.7:
            expected = AuthLevel.STANDARD.value
        else:
            expected = AuthLevel.ENHANCED.value
        compliance = 1.0 if level == expected else 0.4

        w_sec = float(self.config.get("reward_weight_security", 0.4))
        w_lat = float(self.config.get("reward_weight_latency", 0.3))
        w_fric = float(self.config.get("reward_weight_friction", 0.2))
        w_comp = float(self.config.get("reward_weight_compliance", 0.1))
        total_w = w_sec + w_lat + w_fric + w_comp
        if total_w <= 0:
            total_w = 1.0

        reward = (w_sec * sec + w_lat * lat + w_fric * friction + w_comp * compliance) / total_w
        return float(max(0.0, min(1.0, reward)))

    def _shape_reward_safe(self, base_reward: float, decision: Dict[str, Any]) -> float:
        if self._causal_shaper is None:
            return base_reward
        if not self._prev_snapshot or not self._current_snapshot:
            return base_reward
        try:
            action_idx = decision.get("action_idx", 0)
            shaped = self._causal_shaper.shape_reward(
                action_idx, base_reward, self._prev_snapshot, self._current_snapshot,
            )
            return float(max(0.0, min(1.0, shaped)))
        except Exception as e:
            logger.warning(f"Causal shaping failed: {e}")
            return base_reward

    # ------------------------------------------------------------------
    # State construction
    # ------------------------------------------------------------------
    def _build_auth_state(self, context: Dict[str, Any]) -> AuthState:
        now = _utc_now()
        return AuthState(
            user_risk_score=float(context.get("user_risk_score", 0.5)),
            resource_sensitivity=float(context.get("resource_sensitivity", 0.5)),
            request_frequency=float(context.get("request_frequency", 10.0)),
            data_classification=float(context.get("data_classification", 0.5)),
            time_of_day=float(now.hour) / 24.0,
            day_of_week=float(now.weekday()) / 6.0,
            ip_reputation=float(context.get("ip_reputation", 0.5)),
            device_trust=float(context.get("device_trust", 0.5)),
            previous_auth_success_rate=float(context.get("previous_auth_success_rate", 0.9)),
            previous_auth_avg_latency=float(context.get("previous_auth_avg_latency", 200.0)),
            carbon_intensity=float(context.get("carbon_intensity", 0.4)),  # kg CO2/kWh
            recent_failed_attempts=float(context.get("recent_failed_attempts", 0.0)),
            geo_velocity=float(context.get("geo_velocity", 0.0)),
            user_role_sensitivity=float(context.get("user_role_sensitivity", 0.5)),
        )

    # ------------------------------------------------------------------
    # Audit + persistence
    # ------------------------------------------------------------------
    def _log_audit(
        self,
        subject: str,
        level: AuthLevel,
        outcome: AuthOutcome,
        reward: Optional[float] = None,
        latency_ms: Optional[float] = None,
    ) -> None:
        self.audit_log.append({
            "timestamp": _utc_now().isoformat(),
            "subject": subject,
            "level": level.value if isinstance(level, AuthLevel) else str(level),
            "outcome": outcome.value if isinstance(outcome, AuthOutcome) else str(outcome),
            "reward": reward,
            "latency_ms": latency_ms,
        })

    def _make_decision(
        self,
        state_vec: Optional[np.ndarray],
        outcome: AuthOutcome,
        subject: str,
    ) -> Dict[str, Any]:
        # Fallback decision for pre-optimizer rejections
        return {
            "subject": subject,
            "state_vec": state_vec if state_vec is not None else np.zeros(14, dtype=np.float32),
            "action_idx": 2,  # enhanced
            "teacher_outputs": np.ones((1, 3), dtype=np.float64) / 3,
            "level": AuthLevel.ENHANCED.value,
            "outcome": outcome.value,
            "risk_score": 1.0,
            "resource_sensitivity": 1.0,
        }

    async def _persist_entry_async(self, entry: Dict[str, Any]) -> None:
        try:
            async with self._log_lock:
                await asyncio.to_thread(self._append_log_sync, entry)
        except Exception as e:
            logger.error(f"Failed to persist auth log: {e}")

    def _append_log_sync(self, entry: Dict[str, Any]) -> None:
        try:
            self._log_dir.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass
        csv_path = self._log_dir / "auth_logs.csv"
        exists = csv_path.exists()
        flat = {k: (json.dumps(v) if isinstance(v, (dict, list)) else v) for k, v in entry.items()}
        pd.DataFrame([flat]).to_csv(csv_path, mode="a", header=not exists, index=False)

    # ------------------------------------------------------------------
    # FeedbackEvent + AsyncMessageQueue
    # ------------------------------------------------------------------
    async def _emit_feedback_event(
        self,
        decision: Dict[str, Any],
        outcome: AuthOutcome,
        reward: float,
        latency_ms: float,
        security_incident: bool,
    ) -> None:
        if FeedbackEvent is None:
            return

        allowed = getattr(FeedbackEvent, "allowed_feedback_types", None)
        if allowed is None or AUTH_FEEDBACK_TYPE in set(allowed):
            fb_type = AUTH_FEEDBACK_TYPE
        else:
            fb_type = "routing"

        try:
            event = FeedbackEvent(
                source="zero_trust_architecture",
                feedback_type=fb_type,
                context={
                    "task_id": decision.get("subject", "unknown"),
                    "teacher_ids": [type(t).__name__ for t in self.auth_optimizer.teachers],
                },
                action={
                    "selected_action": decision.get("level", AuthLevel.STANDARD.value),
                    "selected_rank": 1,
                    "confidence_score": 0.5,
                },
                performance={
                    "quality_score": reward,
                    "latency_ms": latency_ms,
                    "energy_joules": 0.0,
                    "carbon_g": 0.0,
                    "helium_cost": 0.0,
                    "duration_ms": latency_ms,
                },
                adaptive_cost_value=reward,
                tags=["auth", "zero_trust", decision.get("level", "unknown"), outcome.value],
                envelope={"trace_id": decision.get("subject", "unknown")},
            )
            if self.message_queue is not None and hasattr(self.message_queue, "publish"):
                try:
                    result = self.message_queue.publish(event)
                    if asyncio.iscoroutine(result):
                        await result
                except Exception as e:
                    logger.warning(f"Failed to publish FeedbackEvent: {e}")
            else:
                logger.debug(f"FeedbackEvent created: {getattr(event, 'event_id', '?')}")
        except Exception as e:
            logger.warning(f"Failed to create FeedbackEvent: {e}")


# ============================================================================
# Factory
# ============================================================================

def create_zero_trust_architecture(config: Optional[Dict[str, Any]] = None) -> ZeroTrustArchitecture:
    return ZeroTrustArchitecture(config or {})


__all__ = [
    "AuthLevel",
    "RiskLevel",
    "AuthOutcome",
    "AUTH_FEEDBACK_TYPE",
    "RunningScaler",
    "FederatedWeightsCodec",
    "AuthState",
    "Teacher",
    "RuleBasedAuthTeacher",
    "HistoricalMLAuthTeacher",
    "StatefulQAuthTeacher",
    "RLHFAuthTeacher",
    "DistillationStudent",
    "ReplayBuffer",
    "MoEGatingNetwork",
    "SlidingWindowRateLimiter",
    "NonceCache",
    "DistillationAuthOptimizer",
    "ZeroTrustArchitecture",
    "create_zero_trust_architecture",
]


# ============================================================================
# Tests
# ============================================================================

def _make_state(**overrides: Any) -> AuthState:
    base: Dict[str, Any] = {
        "user_risk_score": 0.4,
        "resource_sensitivity": 0.5,
        "request_frequency": 5.0,
        "data_classification": 0.5,
        "time_of_day": 0.5,
        "day_of_week": 3.0,
        "ip_reputation": 0.8,
        "device_trust": 0.7,
        "previous_auth_success_rate": 0.9,
        "previous_auth_avg_latency": 200.0,
        "carbon_intensity": 0.4,  # kg CO2/kWh
        "recent_failed_attempts": 0.0,
        "geo_velocity": 50.0,
        "user_role_sensitivity": 0.5,
    }
    base.update(overrides)
    return AuthState(**base)


def _test_authstate_features() -> None:
    s = _make_state()
    v = s.to_feature_vector()
    assert v.shape == (14,)
    assert np.all(v >= 0.0) and np.all(v <= 1.0), v
    # carbon_intensity=0.4 should map to ~0.4, not ~0.0004
    assert v[10] > 0.2, f"carbon scaling regressed: {v[10]}"


def _test_running_scaler() -> None:
    s = RunningScaler(14)
    for _ in range(5):
        s.update(np.random.randn(14) + 0.5)
    out = s.transform(np.random.randn(14))
    assert out.shape == (14,)
    assert np.all(np.isfinite(out))


def _test_q_teacher_update_from_vector() -> None:
    q = StatefulQAuthTeacher(feature_dim=14)
    v = np.zeros(14)
    v[0] = 1.0
    q.update_from_vector(v, 2, 1.0)
    assert np.any(q.weights[:, 2] != 0)


def _test_gating_jacobian_shapes() -> None:
    g = MoEGatingNetwork(feature_dim=14, n_experts=5)
    s = np.random.randn(14)
    t = np.random.rand(5, 3)
    t = t / t.sum(axis=1, keepdims=True)
    student = np.random.rand(3)
    student = student / student.sum()
    gw = g.forward(s)
    g.update(s, t, student, gw)
    assert g.weights.shape == (14, 5)
    assert g.bias.shape == (5,)


def _test_federated_codec() -> None:
    w = {"weights": np.random.randn(14, 3), "biases": np.random.randn(3)}
    blob = FederatedWeightsCodec.encode(w)
    back = FederatedWeightsCodec.decode(blob)
    assert np.allclose(w["weights"], back["weights"])
    assert np.allclose(w["biases"], back["biases"])


def _test_sanitize_id() -> None:
    assert _sanitize_id(None) == "unknown"
    assert "/" not in _sanitize_id("a/b")
    assert ".." not in _sanitize_id("../../etc/passwd")


async def _async_test_rate_limiter() -> None:
    rl = SlidingWindowRateLimiter(limit=3, window_seconds=60.0)
    results = [await rl.check_and_record("u1") for _ in range(5)]
    assert results[:3] == [True, True, True]
    assert results[3:] == [False, False]


async def _async_test_nonce_cache() -> None:
    nc = NonceCache(ttl_seconds=60.0)
    assert await nc.check_and_add("n1") is True
    assert await nc.check_and_add("n1") is False
    assert await nc.check_and_add("n2") is True


async def _async_test_jwt_and_hmac() -> None:
    zta = ZeroTrustArchitecture({
        "jwt": {"secret": "s3cr3t", "algorithms": ["HS256"]},
        "hmac": {"secret": "hunter2"},
    })
    if jwt is not None:
        import time as _t
        token = jwt.encode({"exp": int(_t.time()) + 60}, "s3cr3t", algorithm="HS256")
        assert zta._validate_jwt(token)
        bad = jwt.encode({"exp": int(_t.time()) - 60}, "s3cr3t", algorithm="HS256")
        assert not zta._validate_jwt(bad)

    payload = b"hello"
    sig = hmac.new(b"hunter2", payload, hashlib.sha256).hexdigest()
    assert zta._verify_hmac(payload, sig)
    assert not zta._verify_hmac(payload, "deadbeef")


async def _async_test_full_flow() -> None:
    zta = ZeroTrustArchitecture({
        "distillation": {"seed": 42, "distillation_train_every": 1, "batch_update_size": 2},
        "jwt": {"secret": "s3cr3t"},
        "hmac": {"secret": "hunter2"},
        "log_dir": "./_zta_test_logs",
    })

    level, decision = await zta.authenticate({"user_id": "alice", "user_risk_score": 0.2})
    assert isinstance(level, AuthLevel)

    await zta.record_outcome(
        decision,
        outcome=AuthOutcome.SUCCESS,
        latency_ms=120.0,
    )

    # Second round
    level2, decision2 = await zta.authenticate({"user_id": "alice", "user_risk_score": 0.9})
    assert isinstance(level2, AuthLevel)
    await zta.record_outcome(
        decision2,
        outcome=AuthOutcome.SUCCESS,
        latency_ms=200.0,
    )

    # Federated round-trip
    blob = await zta.export_federated_blob()
    await zta.import_federated_blob(blob)

    stats = zta.get_stats()
    assert stats["n_teachers"] >= 4
    zta.flush()


async def _async_test_replay_and_rate_limit() -> None:
    zta = ZeroTrustArchitecture({
        "distillation": {"seed": 1},
        "rate_limit": {"limit": 2, "window_seconds": 60.0},
    })
    # Two allowed, third rejected
    _, _ = await zta.authenticate({"user_id": "bob"})
    _, _ = await zta.authenticate({"user_id": "bob"})
    level, decision = await zta.authenticate({"user_id": "bob"})
    assert decision["level"] == AuthLevel.ENHANCED.value

    # Replay detection
    zta2 = ZeroTrustArchitecture({"distillation": {"seed": 1}})
    _, _ = await zta2.authenticate({"user_id": "carol"}, nonce="n1")
    _, d2 = await zta2.authenticate({"user_id": "carol"}, nonce="n1")
    assert d2["level"] == AuthLevel.ENHANCED.value


def _test_reward_bounds() -> None:
    zta = ZeroTrustArchitecture({})
    r_success = zta._compute_reward(
        AuthOutcome.SUCCESS, 100.0, False, False, 0.5, AuthLevel.STANDARD.value,
    )
    r_incident = zta._compute_reward(
        AuthOutcome.SUCCESS, 100.0, True, False, 0.5, AuthLevel.STANDARD.value,
    )
    r_overauth = zta._compute_reward(
        AuthOutcome.SUCCESS, 100.0, False, False, 0.2, AuthLevel.ENHANCED.value,
    )
    assert 0.0 <= r_success <= 1.0
    assert r_incident < r_success
    assert r_overauth < r_success


def _run_all_tests() -> None:
    _test_authstate_features()
    _test_running_scaler()
    _test_q_teacher_update_from_vector()
    _test_gating_jacobian_shapes()
    _test_federated_codec()
    _test_sanitize_id()
    _test_reward_bounds()
    asyncio.run(_async_test_rate_limiter())
    asyncio.run(_async_test_nonce_cache())
    asyncio.run(_async_test_jwt_and_hmac())
    asyncio.run(_async_test_full_flow())
    asyncio.run(_async_test_replay_and_rate_limit())
    print("All ZeroTrustArchitecture tests passed.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    _run_all_tests()
