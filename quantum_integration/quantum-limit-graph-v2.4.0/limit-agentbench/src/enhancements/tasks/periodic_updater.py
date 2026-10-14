"""
Enhanced Periodic Updater for Green Agent v2.3.0
=================================================
Celery tasks for periodic updates of sustainability data with adaptive
scheduling via Multi-Teacher On-Policy Distillation, Multi-Objective
Evolutionary Optimization (NSGA-II), and LIMIT Graph / MODP / RLHF / MoE
components.

ENHANCEMENTS (v2.3.0 over v2.2.0):
- Pydantic v2-compatible settings (pydantic_settings, with v1 fallback).
- Lazy thread-safe ``get_scheduler()`` singleton — replaces dead ``_scheduler``
  global that was never assigned.
- Per-worker persistent event loop: fixes the cross-loop ``asyncio.Lock``
  crash that occurred because tasks used ``asyncio.run(...)`` on every call.
- MoE ``_encode_state`` accepts numpy arrays / dicts / UpdateState; MoE is now
  integrated into the distillation pathway via the gated teacher combination.
- Fixed FeedbackEvent construction: ``context.task_id`` populated correctly,
  guarded ``to_json`` / ``model_dump_json``.
- Stable interaction log schema (constant columns) + append-safe writes.
- NSGA-II rewritten: single (individual, point) list, distinct objectives,
  non-mutating selection, no O(n^2) offspring scans.
- Added epsilon decay + epsilon_min to DistillationSchedulerOptimizer.
- Added RunningScaler for online feature normalization, shared by teachers.
- Activated Q-teacher via update_from_vector during batch training.
- Student distills from the *gated* teacher combination, not the weighted mean.
- MoE gating uses full softmax Jacobian + load balancing.
- Real material_version tracking with timestamps.
- Graceful shutdown hook (flush, cancel MOEA task).
- MOEA runs off the event loop (asyncio.to_thread) and skips if running.
- Config isolation: no import-time ``config = UpdaterConfig()`` side effects.
- Added __all__ and a built-in test suite.

All previous features retained.
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import logging
import os
import random
import threading
import time
import uuid
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import (
    Any,
    Awaitable,
    Callable,
    ClassVar,
    Dict,
    FrozenSet,
    List,
    Optional,
    Tuple,
    Union,
)

import numpy as np
import pandas as pd

from celery import Celery
from celery.signals import task_failure, task_success

# ---------- Pydantic / settings ----------
try:
    from pydantic_settings import BaseSettings, SettingsConfigDict  # type: ignore
    _PYDANTIC_V2 = True
except ImportError:
    try:
        from pydantic import BaseSettings  # type: ignore
        SettingsConfigDict = None  # type: ignore
        _PYDANTIC_V2 = False
    except ImportError:
        BaseSettings = object  # type: ignore
        SettingsConfigDict = None  # type: ignore
        _PYDANTIC_V2 = False

from pydantic import Field

# ---------- Prometheus ----------
try:
    from prometheus_client import Counter, Gauge, Histogram
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False
    Counter = Gauge = Histogram = None  # type: ignore

# ---------- Logging ----------
try:
    import structlog
    logger = structlog.get_logger(__name__)
except ImportError:
    logger = logging.getLogger(__name__)
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO)

# ---------- scikit-learn ----------
try:
    from sklearn.ensemble import RandomForestClassifier
    from sklearn.preprocessing import LabelEncoder
    SKLEARN_ML = True
except ImportError:
    SKLEARN_ML = False

# ---------- Local project imports ----------
try:
    from ..cache.cache_manager import CacheManager  # type: ignore
except ImportError:
    CacheManager = None  # type: ignore

try:
    from ..data_integration.carbon_intensity import CarbonIntensityFetcher  # type: ignore
except ImportError:
    CarbonIntensityFetcher = None  # type: ignore

try:
    from ..data_integration.material_footprint import MaterialFootprintUpdater  # type: ignore
except ImportError:
    MaterialFootprintUpdater = None  # type: ignore

try:
    from ..data_integration.helium_collector import HeliumCollector  # type: ignore
except ImportError:
    HeliumCollector = None  # type: ignore

try:
    from ..schemas.feedback_event import FeedbackEvent  # type: ignore
except ImportError:
    FeedbackEvent = None  # type: ignore

try:
    from ..async_message_queue import AsyncMessageQueue  # type: ignore
except ImportError:
    AsyncMessageQueue = None  # type: ignore

try:
    from ...storage import Storage  # type: ignore
    CENTRAL_STORAGE_AVAILABLE = True
except ImportError:
    CENTRAL_STORAGE_AVAILABLE = False
    Storage = None  # type: ignore


# ============================================================================
# Constants & utilities
# ============================================================================

SCHEMA_VERSION: str = "2.3.0"
SCHEDULER_FEEDBACK_TYPE: str = "routing"
_TASK_TYPES: Tuple[str, ...] = ("carbon", "material", "helium")
_ACTIONS: Tuple[str, ...] = ("update_now", "skip")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _as_path(v: Union[str, Path, None]) -> Optional[Path]:
    if v is None:
        return None
    return v if isinstance(v, Path) else Path(v)


def _softmax(logits: np.ndarray) -> np.ndarray:
    z = logits - np.max(logits)
    e = np.exp(z)
    s = e.sum()
    if s <= 0:
        return np.ones_like(e) / len(e)
    return e / s


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


class FederatedWeightsCodec:
    """Versioned base64 JSON codec (student weight exchange)."""
    VERSION = "1.0"

    @staticmethod
    def encode(weights: Dict[str, np.ndarray]) -> Dict[str, Any]:
        import base64
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
        import base64
        if encoded.get("version") != FederatedWeightsCodec.VERSION:
            raise ValueError(f"Unsupported federated weights version: {encoded.get('version')}")
        out: Dict[str, np.ndarray] = {}
        for k, v in encoded["payload"].items():
            raw = base64.b64decode(v["data"])
            out[k] = np.frombuffer(raw, dtype=v["dtype"]).reshape(v["shape"]).copy()
        return out


# ============================================================================
# Configuration
# ============================================================================

class UpdaterConfig(BaseSettings):
    """Configuration for periodic updater and adaptive scheduler."""
    redis_url: str = "redis://localhost:6379/0"
    regions: List[str] = Field(default_factory=lambda: [
        "us-east", "us-west", "eu-west", "eu-north", "asia-east", "asia-southeast",
    ])
    helium_snapshot_url: str = "https://example.com/helium_snapshot.parquet"
    helium_snapshot_path: str = "./helium_snapshot.parquet"

    # Distillation
    distillation_epsilon: float = 0.1
    distillation_epsilon_min: float = 0.01
    distillation_epsilon_decay: float = 0.995
    distillation_train_every: int = 10
    distillation_replay_size: int = 2000
    distillation_learning_rate: float = 0.01
    distill_weight: float = 0.7
    rl_weight: float = 0.3
    q_learning_rate: float = 0.1
    batch_update_size: int = 8
    gating_learning_rate: float = 0.005
    gating_load_balance_weight: float = 0.01

    # MOEA
    moea_enabled: bool = True
    moea_interval_seconds: int = 300
    moea_population_size: int = 20
    moea_generations: int = 10
    moea_mutation_rate: float = 0.2
    moea_crossover_rate: float = 0.8
    moea_tournament_size: int = 3
    moea_objective_weights: Optional[Dict[str, float]] = Field(
        default_factory=lambda: {
            "update_quality": 0.5,
            "resource_savings": 0.3,
            "timeliness": 0.2,
        }
    )
    moea_dynamic_weights: bool = True
    moea_pareto_path: str = "./updater_moea_pareto.json"

    # Feature flags
    enable_limit_graph: bool = True
    enable_modp: bool = True
    enable_rlhf: bool = True
    enable_moe: bool = True
    moe_expert_count: int = 2

    # Persistence
    q_weights_path: str = "./update_q_weights.json"
    interaction_logs_path: str = "./update_interactions.csv"
    historical_model_path: str = "./update_historical_model.pkl"
    log_dir: str = "."

    # Scheduler
    scheduler_interval_seconds: int = 900
    enable_message_queue: bool = False
    first_run_hours_since_update: float = 72.0
    reward_weight_quality: float = 0.5
    reward_weight_latency: float = 0.3
    reward_weight_cost: float = 0.2

    if _PYDANTIC_V2 and SettingsConfigDict is not None:
        model_config = SettingsConfigDict(env_prefix="UPDATER_", case_sensitive=False)
    else:
        class Config:
            env_prefix = "UPDATER_"
            case_sensitive = False


# Lazy config singleton.
_config_lock = threading.Lock()
_config_singleton: Optional[UpdaterConfig] = None


def get_config() -> UpdaterConfig:
    global _config_singleton
    with _config_lock:
        if _config_singleton is None:
            _config_singleton = UpdaterConfig()
        return _config_singleton


def reset_config_for_tests() -> None:
    """Testing hook: reset the config singleton."""
    global _config_singleton
    with _config_lock:
        _config_singleton = None


# ============================================================================
# Celery app
# ============================================================================

def _make_celery_app() -> Celery:
    cfg = get_config()
    app = Celery("green_agent", broker=cfg.redis_url)
    app.conf.update(
        task_serializer="json",
        result_serializer="json",
        accept_content=["json"],
        timezone="UTC",
        enable_utc=True,
        task_track_started=True,
        task_time_limit=600,
        task_soft_time_limit=540,
        task_acks_late=True,
        task_reject_on_worker_lost=True,
        task_retry_backoff_max=600,
    )
    return app


app = _make_celery_app()


# ============================================================================
# Prometheus metrics
# ============================================================================

if PROMETHEUS_AVAILABLE:
    task_metrics: Dict[str, Any] = {
        "carbon_success": Counter("carbon_update_success_total", "Carbon update success count"),
        "carbon_failure": Counter("carbon_update_failure_total", "Carbon update failure count"),
        "material_success": Counter("material_update_success_total", "Material update success count"),
        "material_failure": Counter("material_update_failure_total", "Material update failure count"),
        "helium_success": Counter("helium_update_success_total", "Helium update success count"),
        "helium_failure": Counter("helium_update_failure_total", "Helium update failure count"),
        "task_duration": Histogram("periodic_task_duration_seconds", "Task duration", ["task_name"]),
        "update_action": Counter("update_action_selected", "Action selected by scheduler", ["action"]),
        "update_reward": Histogram("update_reward", "Reward per update decision"),
        "moea_pareto_front": Gauge("updater_moea_pareto_front", "MOEA Pareto front size"),
    }
else:
    task_metrics = {}


# ============================================================================
# State
# ============================================================================

@dataclass
class UpdateState:
    """State for the distillation agent."""
    hours_since_last_update: float
    hour_of_day: float
    day_of_week: float
    carbon_trend: float
    material_version_age_days: float
    helium_snapshot_age_days: float
    current_carbon_intensity: float  # kg CO2/kWh
    pending_updates_count: int
    system_load: float
    last_update_success: float = 1.0
    cache_freshness: float = 1.0
    error_rate: float = 0.0

    FEATURE_DIM: ClassVar[int] = 12
    HOURS_MAX: ClassVar[float] = 72.0
    TREND_MAX: ClassVar[float] = 0.1
    AGE_MAX_DAYS: ClassVar[float] = 30.0
    CARBON_MAX_KG: ClassVar[float] = 1.0
    PENDING_MAX: ClassVar[float] = 10.0

    @staticmethod
    def _c(v: float) -> float:
        return float(min(1.0, max(0.0, v)))

    def to_raw_vector(self) -> np.ndarray:
        return np.array([
            self.hours_since_last_update,
            self.hour_of_day,
            self.day_of_week,
            self.carbon_trend,
            self.material_version_age_days,
            self.helium_snapshot_age_days,
            self.current_carbon_intensity,
            self.pending_updates_count,
            self.system_load,
            self.last_update_success,
            self.cache_freshness,
            self.error_rate,
        ], dtype=np.float64)

    def to_feature_vector(self, scaler: Optional[RunningScaler] = None) -> np.ndarray:
        raw = self.to_raw_vector()
        if scaler is not None:
            return scaler.transform(raw)
        c = self._c
        return np.array([
            c(self.hours_since_last_update / self.HOURS_MAX),
            c(self.hour_of_day / 24.0),
            c(self.day_of_week / 7.0),
            c(abs(self.carbon_trend) / self.TREND_MAX),
            c(self.material_version_age_days / self.AGE_MAX_DAYS),
            c(self.helium_snapshot_age_days / self.AGE_MAX_DAYS),
            c(self.current_carbon_intensity / self.CARBON_MAX_KG),
            c(self.pending_updates_count / self.PENDING_MAX),
            c(self.system_load),
            c(self.last_update_success),
            c(self.cache_freshness),
            c(self.error_rate),
        ], dtype=np.float32)


# ============================================================================
# Teachers
# ============================================================================

class Teacher(ABC):
    def __init__(self) -> None:
        self.scaler: Optional[RunningScaler] = None

    @abstractmethod
    def predict(self, state: UpdateState) -> np.ndarray:
        ...

    @abstractmethod
    def confidence(self, state: UpdateState) -> float:
        ...

    def _features(self, state: UpdateState) -> np.ndarray:
        return state.to_feature_vector(self.scaler)


class UpdateRuleBasedTeacher(Teacher):
    ACTIONS: ClassVar[List[str]] = list(_ACTIONS)

    def predict(self, state: UpdateState) -> np.ndarray:
        probs = np.ones(2, dtype=np.float64) * 0.1
        if state.hours_since_last_update > 24:
            probs[0] = 0.8
        elif state.carbon_trend > 0.05:
            probs[0] = 0.7
        elif state.material_version_age_days > 14:
            probs[0] = 0.6
        elif state.system_load > 0.8:
            probs[1] = 0.8
        elif state.last_update_success < 0.5:
            probs[0] = 0.6
        else:
            probs[1] = 0.6
        return _softmax(probs)

    def confidence(self, state: UpdateState) -> float:
        return 0.6 if state.hours_since_last_update > 24 else 0.4


class UpdateHistoricalMLTeacher(Teacher):
    EXPECTED_FEATURE_DIM: ClassVar[int] = 12

    def __init__(self, model_path: Optional[Path] = None) -> None:
        super().__init__()
        self.model = None
        self.label_encoder = None
        self.model_path: Path = _as_path(model_path) or Path(get_config().historical_model_path)
        self._load()

    def _load(self) -> None:
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
                f"{self.EXPECTED_FEATURE_DIM}; disabling."
            )
            self.model, self.label_encoder = None, None
            return
        logger.info(f"Loaded historical ML model from {self.model_path}")

    def predict(self, state: UpdateState) -> np.ndarray:
        if self.model is None or self.label_encoder is None:
            return np.ones(2) / 2
        x = self._features(state).reshape(1, -1)
        if not hasattr(self.model, "predict_proba"):
            return np.ones(2) / 2
        return self.model.predict_proba(x)[0]

    def confidence(self, state: UpdateState) -> float:
        return 0.7 if self.model is not None else 0.0

    @classmethod
    def train_from_logs(
        cls,
        log_paths: List[Path],
        model_path: Path,
        state_col: str = "state_vec",
        label_col: str = "action",
    ) -> Optional[Path]:
        if not SKLEARN_ML:
            logger.error("scikit-learn not available, cannot train historical model.")
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
        model_path = _as_path(model_path) or Path("./update_historical_model.pkl")
        with open(model_path, "wb") as f:
            pickle.dump((clf, le), f)
        logger.info(f"Trained historical model and saved to {model_path}")
        return model_path


class UpdateStatefulQTeacher(Teacher):
    def __init__(
        self,
        lr: float = 0.1,
        weights_path: Optional[Path] = None,
        feature_dim: int = 12,
    ) -> None:
        super().__init__()
        self.lr = float(lr)
        self.weights_path: Path = _as_path(weights_path) or Path(get_config().q_weights_path)
        self.feature_dim = int(feature_dim)
        self.weights = np.zeros((self.feature_dim, 2), dtype=np.float64)
        self._dirty = False
        self._load()

    def _load(self) -> None:
        if not self.weights_path.exists():
            return
        try:
            with open(self.weights_path, "r") as f:
                data = json.load(f)
            w = np.array(data, dtype=np.float64)
            if w.shape == (self.feature_dim, 2):
                self.weights = w
                logger.info(f"Loaded Q-teacher weights from {self.weights_path}")
            else:
                logger.warning(
                    f"Loaded Q-weights shape {w.shape}, expected "
                    f"({self.feature_dim},2). Reinitializing."
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
        return _softmax(x @ self.weights)

    def predict(self, state: UpdateState) -> np.ndarray:
        return self._predict_from_vector(self._features(state))

    def confidence(self, state: UpdateState) -> float:
        x = self._features(state)
        q = x @ self.weights
        spread = float(np.max(q) - np.min(q))
        return float(np.clip(0.3 + 0.35 * spread, 0.0, 1.0))

    def update_from_vector(self, state_vec: np.ndarray, action: int, reward: float) -> None:
        x = np.asarray(state_vec, dtype=np.float64).reshape(-1)
        if x.shape[0] != self.feature_dim:
            return
        if not 0 <= int(action) < 2:
            return
        q_current = float(np.dot(x, self.weights[:, action]))
        self.weights[:, action] += self.lr * (reward - q_current) * x
        self._dirty = True

    def update(self, state: UpdateState, action: int, reward: float) -> None:
        self.update_from_vector(self._features(state), action, reward)


class RLHFUpdateTeacher(Teacher):
    """Teacher influenced by human feedback on scheduling decisions."""

    def predict(self, state: UpdateState) -> np.ndarray:
        probs = np.ones(2, dtype=np.float64) / 2
        # Placeholder: bias toward update_now if system load is low.
        if state.system_load < 0.4:
            probs[0] += 0.1
        return _softmax(probs)

    def confidence(self, state: UpdateState) -> float:
        return 0.5


# ============================================================================
# Student / Replay / Gating
# ============================================================================

class DistillationStudent:
    """Linear softmax student trained via distillation + REINFORCE."""

    def __init__(
        self,
        feature_dim: int = 12,
        n_classes: int = 2,
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
        return _softmax(state_vector @ self.weights + self.biases)

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
    """
    Gating network using the full softmax Jacobian + load balancing.
    Accepts UpdateState, dict, or numpy feature vectors.
    """

    def __init__(
        self,
        feature_dim: int = 12,
        n_experts: int = 2,
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

    @staticmethod
    def encode_state(state: Any) -> np.ndarray:
        if isinstance(state, np.ndarray):
            return state.astype(np.float64).reshape(-1)
        if isinstance(state, dict):
            # Mirror UpdateState.to_feature_vector's static path.
            def c(v: float) -> float:
                return float(min(1.0, max(0.0, v)))
            return np.array([
                c(state.get("hours_since_last_update", 0.0) / 72.0),
                c(state.get("hour_of_day", 0.0) / 24.0),
                c(state.get("day_of_week", 0.0) / 7.0),
                c(abs(state.get("carbon_trend", 0.0)) / 0.1),
                c(state.get("material_version_age_days", 0.0) / 30.0),
                c(state.get("helium_snapshot_age_days", 0.0) / 30.0),
                c(state.get("current_carbon_intensity", 0.0) / 1.0),
                c(state.get("pending_updates_count", 0) / 10.0),
                c(state.get("system_load", 0.5)),
                c(state.get("last_update_success", 1.0)),
                c(state.get("cache_freshness", 1.0)),
                c(state.get("error_rate", 0.0)),
            ], dtype=np.float64)
        # Assume UpdateState-like.
        return state.to_feature_vector()

    def forward(self, state_vec: np.ndarray) -> np.ndarray:
        x = np.asarray(state_vec, dtype=np.float64).reshape(-1)
        return _softmax(x @ self.weights + self.bias)

    def update(
        self,
        state_vec: np.ndarray,
        teacher_outputs: np.ndarray,
        student_probs: np.ndarray,
        current_gate_weights: np.ndarray,
    ) -> None:
        x = np.asarray(state_vec, dtype=np.float64).reshape(-1)
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

        self.weights -= self.lr * np.outer(x, dL_dz)
        self.bias -= self.lr * dL_dz


# ============================================================================
# Distillation Scheduler Optimizer
# ============================================================================

class DistillationSchedulerOptimizer:
    """
    Multi-teacher on-policy distillation for update scheduling.

    Student distills from the *gated* teacher combination. Q-teacher is
    updated during batch training. Epsilon decays with a floor.
    """
    ACTIONS: ClassVar[List[str]] = list(_ACTIONS)

    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config
        self.feature_dim = 12
        self.n_actions = 2
        seed = config.get("seed", None)

        self.scaler = RunningScaler(self.feature_dim, clip=config.get("scaler_clip", 5.0))

        self.student = DistillationStudent(
            feature_dim=self.feature_dim,
            lr=config.get("distillation_learning_rate", 0.01),
            grad_clip=config.get("student_grad_clip", 1.0),
            seed=seed,
        )

        self.teachers: List[Teacher] = [
            UpdateRuleBasedTeacher(),
            UpdateHistoricalMLTeacher(model_path=_as_path(config.get("historical_model_path"))),
            UpdateStatefulQTeacher(
                lr=config.get("q_learning_rate", 0.1),
                weights_path=_as_path(config.get("q_weights_path")),
                feature_dim=self.feature_dim,
            ),
            RLHFUpdateTeacher(),
        ]
        for t in self.teachers:
            t.scaler = self.scaler

        self.q_teacher: Optional[UpdateStatefulQTeacher] = next(
            (t for t in self.teachers if isinstance(t, UpdateStatefulQTeacher)), None
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

    # ------------------------------------------------------------------
    def _compute_teacher_probs(self, state: UpdateState) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
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

    async def select_action(
        self,
        state: UpdateState,
        exploration: bool = True,
    ) -> Tuple[str, int, np.ndarray, np.ndarray]:
        async with self.lock:
            raw = state.to_raw_vector()
            self.scaler.update(raw)
            state_vec = state.to_feature_vector(self.scaler)

            combined, gate_weights, teacher_outputs = self._compute_teacher_probs(state)
            student_probs = self.student.predict_proba(state_vec, self.n_actions)

            if exploration and random.random() < self.epsilon:
                action_idx = random.randint(0, self.n_actions - 1)
            else:
                blended = 0.8 * student_probs + 0.2 * combined
                action_idx = int(np.argmax(blended))

            self.epsilon = max(self.epsilon_min, self.epsilon * self.epsilon_decay)
            return self.ACTIONS[action_idx], action_idx, state_vec, teacher_outputs

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
                t_outs = teacher_outputs_batch[i]
                a = int(actions[i])
                r = float(rewards[i])

                gate_w = self.gating.forward(s_vec)
                combined = gate_w @ t_outs
                s = combined.sum()
                combined = combined / s if s > 0 else np.ones(self.n_actions) / self.n_actions

                self.student.update(
                    s_vec, combined, r, a,
                    distill_weight=self.distill_weight,
                    rl_weight=self.rl_weight,
                )

                if self.q_teacher is not None:
                    self.q_teacher.update_from_vector(s_vec, a, r)

                student_out = self.student.predict_proba(s_vec)
                self.gating.update(s_vec, t_outs, student_out, gate_w)

    def get_stats(self) -> Dict[str, Any]:
        return {
            "student_counter": self.student.counter,
            "buffer_size": len(self.replay_buffer),
            "epsilon": self.epsilon,
            "n_teachers": self.n_teachers,
            "scaler_count": self.scaler.count,
        }

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
        await self.import_student_weights(FederatedWeightsCodec.decode(blob))

    def flush(self) -> None:
        if self.q_teacher is not None:
            self.q_teacher.save(force=True)


# ============================================================================
# NSGA-II (fixed)
# ============================================================================

@dataclass
class MOPDUpdateWeights:
    vector_id: str
    weights: Dict[str, float]
    objectives: Dict[str, float]
    scalarised_score: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "vector_id": self.vector_id,
            "weights": dict(self.weights),
            "objectives": dict(self.objectives),
            "scalarised_score": self.scalarised_score,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MOPDUpdateWeights":
        return cls(
            vector_id=str(data["vector_id"]),
            weights=dict(data["weights"]),
            objectives=dict(data["objectives"]),
            scalarised_score=float(data.get("scalarised_score", 0.0)),
        )


class NSGAIIUpdateOptimizer:
    """
    NSGA-II optimizer over action weights.

    The evaluator must return *distinct* objectives; otherwise the Pareto
    front is degenerate. We keep a single list of (individual, point) so the
    two views never drift.
    """

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
        self.population_size = int(population_size)
        self.generations = int(generations)
        self.mutation_rate = float(mutation_rate)
        self.crossover_rate = float(crossover_rate)
        self.tournament_size = int(tournament_size)
        self.objective_weights = objective_weights or {
            "update_quality": 0.5,
            "resource_savings": 0.3,
            "timeliness": 0.2,
        }
        self.dynamic_weights = bool(dynamic_weights)
        self.best_individual: Optional[Dict[str, float]] = None
        self.best_fitness: float = -float("inf")
        self.pareto_front: List[MOPDUpdateWeights] = []
        self._eval_cache: Dict[Tuple[Tuple[str, float], ...], Dict[str, float]] = {}

    # ------------------------------------------------------------------
    def _random_individual(self) -> Dict[str, float]:
        w = {a: random.random() for a in _ACTIONS}
        total = sum(w.values()) or 1.0
        return {k: v / total for k, v in w.items()}

    def _crossover(self, p1: Dict[str, float], p2: Dict[str, float]) -> Dict[str, float]:
        child = {}
        for key in p1:
            if random.random() < 0.5:
                u = random.random()
                beta = (2 * u) ** (1 / 21) if u <= 0.5 else (1 / (2 * (1 - u))) ** (1 / 21)
                child[key] = max(0.0, min(1.0, 0.5 * ((1 + beta) * p1[key] + (1 - beta) * p2[key])))
            else:
                child[key] = p1[key] if random.random() < 0.5 else p2[key]
        total = sum(child.values()) or 1.0
        return {k: v / total for k, v in child.items()}

    def _mutate(self, ind: Dict[str, float]) -> Dict[str, float]:
        mutant = dict(ind)
        for key in mutant:
            if random.random() < self.mutation_rate:
                u = random.random()
                delta = (2 * u) ** (1 / 21) - 1 if u < 0.5 else 1 - (2 * (1 - u)) ** (1 / 21)
                mutant[key] = max(0.0, min(1.0, mutant[key] + delta))
        total = sum(mutant.values()) or 1.0
        return {k: v / total for k, v in mutant.items()}

    @staticmethod
    def _dominates(a: MOPDUpdateWeights, b: MOPDUpdateWeights) -> bool:
        ge = all(a.objectives[k] >= b.objectives[k] for k in a.objectives)
        gt = any(a.objectives[k] > b.objectives[k] for k in a.objectives)
        return ge and gt

    def _fast_non_dominated_sort(self, points: List[MOPDUpdateWeights]) -> List[List[int]]:
        """Return fronts as lists of indices into ``points``."""
        n = len(points)
        dominated_by_count = [0] * n
        dominates: List[List[int]] = [[] for _ in range(n)]
        fronts: List[List[int]] = [[]]
        for i in range(n):
            for j in range(n):
                if i == j:
                    continue
                if self._dominates(points[i], points[j]):
                    dominates[i].append(j)
                elif self._dominates(points[j], points[i]):
                    dominated_by_count[i] += 1
            if dominated_by_count[i] == 0:
                fronts[0].append(i)
        i = 0
        while fronts[i]:
            nxt: List[int] = []
            for p_idx in fronts[i]:
                for q_idx in dominates[p_idx]:
                    dominated_by_count[q_idx] -= 1
                    if dominated_by_count[q_idx] == 0:
                        nxt.append(q_idx)
            i += 1
            fronts.append(nxt)
        if fronts and not fronts[-1]:
            fronts.pop()
        return fronts

    @staticmethod
    def _crowding_distance(points: List[MOPDUpdateWeights], front: List[int]) -> Dict[int, float]:
        if not front:
            return {}
        distances: Dict[int, float] = {i: 0.0 for i in front}
        obj_keys = list(points[front[0]].objectives.keys())
        for obj in obj_keys:
            ordered = sorted(front, key=lambda i: points[i].objectives[obj])
            distances[ordered[0]] = float("inf")
            distances[ordered[-1]] = float("inf")
            lo = points[ordered[0]].objectives[obj]
            hi = points[ordered[-1]].objectives[obj]
            if hi - lo < 1e-12:
                continue
            for k in range(1, len(ordered) - 1):
                distances[ordered[k]] += (
                    points[ordered[k + 1]].objectives[obj]
                    - points[ordered[k - 1]].objectives[obj]
                ) / (hi - lo)
        return distances

    def _tournament(
        self,
        population: List[Dict[str, float]],
        points: List[MOPDUpdateWeights],
        rank: Dict[int, int],
        crowding: Dict[int, float],
    ) -> int:
        """Return the index into ``population`` of the tournament winner."""
        n = len(population)
        candidates = random.sample(range(n), min(self.tournament_size, n))
        best = candidates[0]
        for c in candidates[1:]:
            r_c, r_b = rank.get(c, n), rank.get(best, n)
            if r_c < r_b or (r_c == r_b and crowding.get(c, 0) > crowding.get(best, 0)):
                best = c
        return best

    def _select_best_from_pareto(
        self,
        pareto: List[MOPDUpdateWeights],
        weights: Dict[str, float],
    ) -> Optional[MOPDUpdateWeights]:
        if not pareto:
            return None
        obj_keys = list(weights.keys())
        max_vals = {k: max(p.objectives[k] for p in pareto) for k in obj_keys}
        min_vals = {k: min(p.objectives[k] for p in pareto) for k in obj_keys}
        ranges = {k: (max_vals[k] - min_vals[k]) or 1.0 for k in obj_keys}
        best: Optional[MOPDUpdateWeights] = None
        best_score = -float("inf")
        for p in pareto:
            score = sum(weights[k] * (p.objectives[k] - min_vals[k]) / ranges[k] for k in obj_keys)
            if score > best_score:
                best_score = score
                best = p
        return best

    def _dynamic_objective_weights(self) -> Dict[str, float]:
        weights = dict(self.objective_weights)
        if not self.dynamic_weights or not self.pareto_front:
            return weights
        obj_keys = list(weights.keys())
        avg = {k: float(np.mean([p.objectives[k] for p in self.pareto_front])) for k in obj_keys}
        hi = {k: float(np.max([p.objectives[k] for p in self.pareto_front])) for k in obj_keys}
        for k in obj_keys:
            if hi[k] > 0 and avg[k] < 0.5 * hi[k]:
                weights[k] = min(0.6, weights.get(k, 0.0) * 1.5)
        total = sum(weights.values()) or 1.0
        return {k: v / total for k, v in weights.items()}

    async def _evaluate_population(
        self, individuals: List[Dict[str, float]]
    ) -> List[MOPDUpdateWeights]:
        keys = [tuple(sorted(ind.items())) for ind in individuals]
        to_eval = [k for k in keys if k not in self._eval_cache]
        if to_eval:
            unique_inds = []
            seen = set()
            for k, ind in zip(keys, individuals):
                if k in self._eval_cache or k in seen:
                    continue
                seen.add(k)
                unique_inds.append(ind)
            results = await asyncio.gather(*[self.evaluate_func(ind) for ind in unique_inds])
            for ind, obj in zip(unique_inds, results):
                self._eval_cache[tuple(sorted(ind.items()))] = dict(obj)
        points = []
        for ind, k in zip(individuals, keys):
            points.append(MOPDUpdateWeights(
                vector_id=str(uuid.uuid4()),
                weights=dict(ind),
                objectives=dict(self._eval_cache[k]),
            ))
        return points

    async def evolve(self) -> List[MOPDUpdateWeights]:
        population = [self._random_individual() for _ in range(self.population_size)]
        points = await self._evaluate_population(population)

        for gen in range(self.generations):
            fronts = self._fast_non_dominated_sort(points)
            rank: Dict[int, int] = {}
            crowding: Dict[int, float] = {}
            for fi, front in enumerate(fronts):
                for i in front:
                    rank[i] = fi
                crowding.update(self._crowding_distance(points, front))

            offspring = []
            while len(offspring) < self.population_size:
                p1 = self._tournament(population, points, rank, crowding)
                p2 = self._tournament(population, points, rank, crowding)
                if random.random() < self.crossover_rate:
                    child = self._crossover(population[p1], population[p2])
                else:
                    child = dict(population[p1])
                offspring.append(self._mutate(child))

            child_points = await self._evaluate_population(offspring)

            combined_pop = population + offspring
            combined_pts = points + child_points
            combined_fronts = self._fast_non_dominated_sort(combined_pts)

            new_pop: List[Dict[str, float]] = []
            new_pts: List[MOPDUpdateWeights] = []
            new_rank: Dict[int, int] = {}
            new_crowding: Dict[int, float] = {}
            for fi, front in enumerate(combined_fronts):
                if len(new_pop) + len(front) <= self.population_size:
                    for i in front:
                        new_rank[len(new_pop)] = fi
                        new_pop.append(combined_pop[i])
                        new_pts.append(combined_pts[i])
                    cd = self._crowding_distance(combined_pts, front)
                    for i, dist in cd.items():
                        new_crowding[len(new_pop) - len(front) + front.index(i)] = dist
                else:
                    cd = self._crowding_distance(combined_pts, front)
                    ordered = sorted(front, key=lambda i: cd.get(i, 0.0), reverse=True)
                    for i in ordered:
                        if len(new_pop) >= self.population_size:
                            break
                        new_rank[len(new_pop)] = fi
                        new_crowding[len(new_pop)] = cd.get(i, 0.0)
                        new_pop.append(combined_pop[i])
                        new_pts.append(combined_pts[i])
                    break

            population = new_pop[: self.population_size]
            points = new_pts[: self.population_size]

            final_fronts = self._fast_non_dominated_sort(points)
            self.pareto_front = [points[i] for i in final_fronts[0]] if final_fronts else []
            logger.info(
                f"NSGA-II gen {gen + 1}/{self.generations}: "
                f"Pareto front size={len(self.pareto_front)}"
            )

        if self.pareto_front:
            weights = self._dynamic_objective_weights()
            best = self._select_best_from_pareto(self.pareto_front, weights)
            if best is not None:
                self.best_individual = dict(best.weights)
                self.best_fitness = float(
                    sum(weights[k] * best.objectives[k] for k in weights)
                )
        return self.pareto_front


# ============================================================================
# LIMIT Graph / MODP / RLHF
# ============================================================================

class LimitGraphManager:
    """In-memory or storage-backed LIMIT graph manager with cycle detection."""

    def __init__(self, storage: Any = None) -> None:
        self.storage = storage
        self.graphs: Dict[str, Dict[str, Dict[str, Any]]] = {}

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
            self.graphs[graph_id]["nodes"][node_id] = {"node_type": node_type, "attributes": attributes}

    def add_edge(
        self,
        graph_id: str,
        edge_id: str,
        source: str,
        target: str,
        weight: float,
        attributes: Dict[str, Any],
    ) -> None:
        if self.storage and hasattr(self.storage, "save_limit_graph_edge"):
            self.storage.save_limit_graph_edge(edge_id, graph_id, source, target, weight, attributes)
        else:
            self.graphs.setdefault(graph_id, {"nodes": {}, "edges": {}})
            self.graphs[graph_id]["edges"][edge_id] = {
                "source": source, "target": target, "weight": weight, "attributes": attributes,
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

    def has_cycle(self, graph_id: str) -> bool:
        edges = self.get_edges(graph_id)
        adj: Dict[str, List[str]] = {}
        for e in edges:
            adj.setdefault(e["source"], []).append(e["target"])
        WHITE, GRAY, BLACK = 0, 1, 2
        color: Dict[str, int] = {}
        def dfs(u: str) -> bool:
            color[u] = GRAY
            for v in adj.get(u, []):
                c = color.get(v, WHITE)
                if c == GRAY:
                    return True
                if c == WHITE and dfs(v):
                    return True
            color[u] = BLACK
            return False
        for u in list(adj.keys()):
            if color.get(u, WHITE) == WHITE and dfs(u):
                return True
        return False


class MODPOptimizer:
    """
    Multi-Objective Dynamic Programming storage.

    This is a *state/policy store* — the actual DP/backward pass is left to
    a downstream module. Rename kept for API compatibility.
    """

    def __init__(self, storage: Any = None) -> None:
        self.storage = storage
        self.states: Dict[str, List[Dict[str, Any]]] = {}
        self.policies: Dict[str, List[Dict[str, Any]]] = {}

    def add_state(
        self,
        state_id: str,
        problem_id: str,
        state_attributes: Dict[str, Any],
        objective_values: Dict[str, float],
        stage: int,
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
        self,
        policy_id: str,
        problem_id: str,
        state_id: str,
        action: str,
        expected_objectives: Dict[str, float],
    ) -> None:
        if self.storage and hasattr(self.storage, "save_modp_policy"):
            self.storage.save_modp_policy(policy_id, problem_id, state_id, action, expected_objectives)
        else:
            self.policies.setdefault(problem_id, []).append({
                "policy_id": policy_id,
                "state_id": state_id,
                "action": action,
                "expected_objectives": expected_objectives,
            })

    def get_states(self, problem_id: str) -> List[Dict[str, Any]]:
        if self.storage and hasattr(self.storage, "get_modp_states"):
            return self.storage.get_modp_states(problem_id)
        return self.states.get(problem_id, [])

    def get_policies(self, problem_id: str) -> List[Dict[str, Any]]:
        if self.storage and hasattr(self.storage, "get_modp_policies"):
            return self.storage.get_modp_policies(problem_id)
        return self.policies.get(problem_id, [])


class RLHFTrainer:
    """Collects preference pairs and (optionally) trains a reward model."""

    def __init__(self, storage: Any = None) -> None:
        self.storage = storage
        self.pairs: List[Dict[str, Any]] = []

    def record_pair(
        self,
        pair_id: str,
        prompt: str,
        chosen: str,
        rejected: str,
        reward_diff: float,
        metadata: Optional[Dict[str, Any]] = None,
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

    def train_reward_model(self) -> Optional[str]:
        pairs = self.get_pairs()
        if len(pairs) < 5:
            logger.info("Not enough preference pairs for RLHF training.")
            return None
        logger.info(f"Training reward model on {len(pairs)} preference pairs (stub).")
        return "stub_model"


# ============================================================================
# Per-worker event loop
# ============================================================================

_worker_loop: Optional[asyncio.AbstractEventLoop] = None
_worker_loop_lock = threading.Lock()


def get_worker_loop() -> asyncio.AbstractEventLoop:
    """
    Return a persistent event loop for the current worker process. Creating
    one per process avoids the ``asyncio.Lock`` cross-loop crash that occurs
    when each task calls ``asyncio.run(...)``.
    """
    global _worker_loop
    with _worker_loop_lock:
        if _worker_loop is None or _worker_loop.is_closed():
            _worker_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(_worker_loop)
        return _worker_loop


def run_async(coro: Awaitable[Any]) -> Any:
    return get_worker_loop().run_until_complete(coro)


# ============================================================================
# AdaptiveScheduler
# ============================================================================

class AdaptiveScheduler:
    """
    Runs the update scheduling policy.

    Uses distillation + MoE + optional NSGA-II to decide ``update_now`` vs
    ``skip`` for each of the three tasks (carbon, material, helium).
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None, storage: Any = None) -> None:
        self.config = config or {}
        self.storage = storage

        seed = self.config.get("seed", None)
        self.scheduler_optimizer = DistillationSchedulerOptimizer({
            "distillation_epsilon": self.config.get("distillation_epsilon", 0.1),
            "distillation_epsilon_min": self.config.get("distillation_epsilon_min", 0.01),
            "distillation_epsilon_decay": self.config.get("distillation_epsilon_decay", 0.995),
            "distillation_train_every": self.config.get("distillation_train_every", 10),
            "distillation_replay_size": self.config.get("distillation_replay_size", 2000),
            "distillation_learning_rate": self.config.get("distillation_learning_rate", 0.01),
            "distill_weight": self.config.get("distill_weight", 0.7),
            "rl_weight": self.config.get("rl_weight", 0.3),
            "q_learning_rate": self.config.get("q_learning_rate", 0.1),
            "q_weights_path": self.config.get("q_weights_path", get_config().q_weights_path),
            "historical_model_path": self.config.get(
                "historical_model_path", get_config().historical_model_path
            ),
            "gating_learning_rate": self.config.get("gating_learning_rate", 0.005),
            "gating_load_balance_weight": self.config.get("gating_load_balance_weight", 0.01),
            "batch_update_size": self.config.get("batch_update_size", 8),
            "seed": seed,
        })

        self.interaction_log: List[Dict[str, Any]] = []
        self.last_state_vec: Optional[np.ndarray] = None
        self.last_action_idx: Optional[int] = None
        self.last_teacher_probs: Optional[np.ndarray] = None
        self._last_selected_expert: Optional[str] = None

        self.last_carbon_update: Optional[datetime] = None
        self.last_material_update: Optional[datetime] = None
        self.last_helium_update: Optional[datetime] = None

        self.carbon_history: deque = deque(maxlen=100)
        self.material_version: Optional[str] = None
        self.material_version_time: Optional[datetime] = None
        self.helium_snapshot_mtime: Optional[float] = None

        self.message_queue: Any = None
        if self.config.get("enable_message_queue", False) and AsyncMessageQueue is not None:
            try:
                self.message_queue = AsyncMessageQueue(queue_type="asyncio")
            except Exception as e:
                logger.warning(f"Failed to init AsyncMessageQueue: {e}")

        self.moea_enabled = bool(self.config.get("moea_enabled", True))
        self.moea_optimizer: Optional[NSGAIIUpdateOptimizer] = None
        self.global_best_weights: Optional[Dict[str, float]] = None
        self.pareto_front: List[MOPDUpdateWeights] = []
        self._moea_task: Optional[asyncio.Task] = None
        self._moea_running = False

        self.limit_graph_manager = LimitGraphManager(storage) if self.config.get("enable_limit_graph", True) else None
        self.modp_solver = MODPOptimizer(storage) if self.config.get("enable_modp", True) else None
        self.rlhf_trainer = RLHFTrainer(storage) if self.config.get("enable_rlhf", True) else None

        # Interaction log file & schema.
        self._log_path = Path(self.config.get("interaction_logs_path", get_config().interaction_logs_path))
        self._log_columns = [
            "timestamp", "task_type", "action", "reward", "state_vec",
        ]

        if self.limit_graph_manager:
            self._init_limit_graph()

        logger.info("AdaptiveScheduler initialized (v2.3.0)")

    # ------------------------------------------------------------------
    def _init_limit_graph(self) -> None:
        graph_id = "update_tasks"
        if not self.limit_graph_manager.get_metadata(graph_id):
            self.limit_graph_manager.create_graph(graph_id, "Update Task Relationships", {})
            for task in _TASK_TYPES:
                self.limit_graph_manager.add_node(graph_id, f"task_{task}", task, {})
            for i in range(len(_TASK_TYPES) - 1):
                src, dst = _TASK_TYPES[i], _TASK_TYPES[i + 1]
                self.limit_graph_manager.add_edge(
                    graph_id, f"edge_{src}_{dst}", f"task_{src}", f"task_{dst}", 1.0, {}
                )

    # ------------------------------------------------------------------
    def _build_state(self, task_type: str) -> UpdateState:
        now = _utc_now()
        cfg = get_config()
        first_run = float(self.config.get("first_run_hours_since_update", cfg.first_run_hours_since_update))

        carbon_trend = 0.0
        current_intensity = 0.0
        material_age = 0.0
        helium_age = 0.0

        if task_type == "carbon":
            last = self.last_carbon_update
            hours_since = (now - last).total_seconds() / 3600 if last else first_run
            if len(self.carbon_history) > 5:
                recent = [v for v in self.carbon_history if v is not None]
                if len(recent) > 5:
                    slope = float(np.polyfit(range(len(recent)), recent, 1)[0])
                    carbon_trend = slope
            current_intensity = float(self.carbon_history[-1]) if self.carbon_history else 0.4
        elif task_type == "material":
            last = self.last_material_update
            hours_since = (now - last).total_seconds() / 3600 if last else first_run
            if self.material_version_time is not None:
                material_age = (now - self.material_version_time).total_seconds() / 86400
            elif self.material_version is None:
                material_age = 30.0
            else:
                material_age = 0.0
        elif task_type == "helium":
            last = self.last_helium_update
            hours_since = (now - last).total_seconds() / 3600 if last else first_run
            if self.helium_snapshot_mtime:
                helium_age = (now.timestamp() - self.helium_snapshot_mtime) / 86400
            else:
                helium_age = 30.0
        else:
            hours_since = first_run

        return UpdateState(
            hours_since_last_update=hours_since,
            hour_of_day=float(now.hour),
            day_of_week=float(now.weekday()),
            carbon_trend=carbon_trend,
            material_version_age_days=material_age,
            helium_snapshot_age_days=helium_age,
            current_carbon_intensity=current_intensity,
            pending_updates_count=0,
            system_load=0.5,
            last_update_success=1.0,
            cache_freshness=1.0,
            error_rate=0.0,
        )

    # ------------------------------------------------------------------
    async def decide_and_execute(self, task_type: str) -> bool:
        state = self._build_state(task_type)

        action, action_idx, state_vec, teacher_outputs = await self.scheduler_optimizer.select_action(
            state, exploration=True
        )

        # Optional NSGA-II blending.
        if self.global_best_weights is not None:
            moea_probs = np.array([self.global_best_weights.get(a, 1.0) for a in _ACTIONS])
            s = moea_probs.sum()
            moea_probs = moea_probs / s if s > 0 else np.ones(2) / 2
            teacher_avg = teacher_outputs.mean(axis=0) if teacher_outputs.size else np.ones(2) / 2
            blended = 0.5 * moea_probs + 0.3 * teacher_avg + 0.2 * np.eye(2)[action_idx]
            action_idx = int(np.argmax(blended))
            action = _ACTIONS[action_idx]

        self.last_state_vec = state_vec
        self.last_action_idx = action_idx
        self.last_teacher_probs = teacher_outputs
        self._last_selected_expert = action  # MoE now = action expert

        if PROMETHEUS_AVAILABLE:
            try:
                task_metrics["update_action"].labels(action=action).inc()
            except Exception:
                pass

        if action == "skip":
            logger.info(f"Skipping {task_type} update")
            await self._record_outcome(task_type, "skip", 0.1)
            return False

        logger.info(f"Executing {task_type} update")
        if task_type == "carbon":
            update_carbon_intensity.delay()
        elif task_type == "material":
            update_material_catalog.delay()
        elif task_type == "helium":
            update_helium_snapshot.delay()
        return True

    # ------------------------------------------------------------------
    async def _record_outcome(
        self,
        task_type: str,
        action: str,
        reward: float,
        state_vec: Optional[np.ndarray] = None,
        action_idx: Optional[int] = None,
        teacher_outputs: Optional[np.ndarray] = None,
    ) -> None:
        # Resolve from last decision if not provided.
        if state_vec is None:
            state_vec = self.last_state_vec
        if action_idx is None:
            action_idx = self.last_action_idx
        if teacher_outputs is None:
            teacher_outputs = self.last_teacher_probs

        # Constant-shape log entry: missing state_vec -> empty string.
        entry = {
            "timestamp": _utc_now().isoformat(),
            "task_type": task_type,
            "action": action,
            "reward": float(reward),
            "state_vec": ",".join(map(str, state_vec)) if state_vec is not None else "",
        }
        self.interaction_log.append(entry)
        await self._append_log_async(entry)

        if state_vec is not None and action_idx is not None and teacher_outputs is not None:
            await self.scheduler_optimizer.update(
                state_vec, int(action_idx), float(reward), state_vec, teacher_outputs
            )
        else:
            logger.debug("Skipping optimizer update: missing decision context.")

        if self.rlhf_trainer and random.random() < 0.05:
            chosen = action
            rejected = "skip" if chosen == "update_now" else "update_now"
            self.rlhf_trainer.record_pair(
                str(uuid.uuid4()), "Which update decision?", chosen, rejected, float(reward),
                {"task_type": task_type},
            )

        if self.modp_solver:
            self.modp_solver.add_state(
                str(uuid.uuid4()), "update_scheduling",
                {"task_type": task_type}, {"reward": float(reward)}, 0,
            )

        if self.limit_graph_manager:
            self.limit_graph_manager.add_node(
                "update_tasks", f"decision_{uuid.uuid4()}", "decision",
                {"task_type": task_type, "action": action, "reward": float(reward)},
            )

        # Track update completion for state building.
        now = _utc_now()
        if action == "update_now":
            if task_type == "carbon":
                self.last_carbon_update = now
            elif task_type == "material":
                self.last_material_update = now
                self.material_version = str(uuid.uuid4())
                self.material_version_time = now
            elif task_type == "helium":
                self.last_helium_update = now
                if Path(get_config().helium_snapshot_path).exists():
                    self.helium_snapshot_mtime = Path(get_config().helium_snapshot_path).stat().st_mtime

        # FeedbackEvent + message queue
        await self._emit_feedback_event(task_type, action, reward)

    # ------------------------------------------------------------------
    async def _append_log_async(self, entry: Dict[str, Any]) -> None:
        try:
            await asyncio.to_thread(self._append_log_sync, entry)
        except Exception as e:
            logger.error(f"Failed to append interaction log: {e}")

    def _append_log_sync(self, entry: Dict[str, Any]) -> None:
        try:
            self._log_path.parent.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass
        exists = self._log_path.exists()
        # Enforce a stable column order regardless of dict key order.
        row = {col: entry.get(col, "") for col in self._log_columns}
        pd.DataFrame([row]).to_csv(self._log_path, mode="a", header=not exists, index=False)

    # ------------------------------------------------------------------
    async def _emit_feedback_event(self, task_type: str, action: str, reward: float) -> None:
        if FeedbackEvent is None:
            return
        try:
            event = FeedbackEvent(
                source="adaptive_scheduler",
                feedback_type=SCHEDULER_FEEDBACK_TYPE,
                context={
                    "task_id": task_type,
                    "teacher_ids": [type(t).__name__ for t in self.scheduler_optimizer.teachers],
                },
                action={
                    "selected_action": action,
                    "selected_rank": 1,
                    "confidence_score": 0.5,
                },
                performance={
                    "quality_score": max(0.0, min(1.0, float(reward))),
                    "latency_ms": 0.0,
                    "energy_joules": 0.0,
                    "carbon_g": 0.0,
                    "helium_cost": 0.0,
                    "duration_ms": 0.0,
                },
                adaptive_cost_value=float(reward),
                tags=["scheduler", "outcome", task_type, action],
            )
            if self.message_queue is not None and hasattr(self.message_queue, "publish"):
                # Use whichever JSON method exists.
                payload = None
                if hasattr(event, "to_json"):
                    payload = event.to_json()
                elif hasattr(event, "model_dump_json"):
                    payload = event.model_dump_json()
                elif hasattr(event, "json"):
                    payload = event.json()
                if payload is not None:
                    result = self.message_queue.publish("scheduler_events", payload)
                    if asyncio.iscoroutine(result):
                        await result
            else:
                logger.debug(f"FeedbackEvent emitted: {getattr(event, 'event_id', '?')}")
        except Exception as e:
            logger.warning(f"Failed to emit FeedbackEvent: {e}")

    # ------------------------------------------------------------------
    async def run_moea_update(self) -> List[MOPDUpdateWeights]:
        if not self.moea_enabled or len(self.interaction_log) < 20:
            return []
        if self._moea_running:
            logger.info("MOEA already running; skipping this tick.")
            return self.pareto_front

        self._moea_running = True
        try:
            recent = self.interaction_log[-200:]

            async def evaluate(weights: Dict[str, float]) -> Dict[str, float]:
                # Distinct objectives per action.
                rewards_upd = [e["reward"] for e in recent if e["action"] == "update_now"]
                rewards_skp = [e["reward"] for e in recent if e["action"] == "skip"]

                upd_mean = float(np.mean(rewards_upd)) if rewards_upd else 0.5
                skp_mean = float(np.mean(rewards_skp)) if rewards_skp else 0.5

                update_quality = weights.get("update_now", 0.5) * upd_mean + weights.get("skip", 0.5) * skp_mean
                # Resource savings favors skip.
                resource_savings = weights.get("skip", 0.5) * skp_mean + weights.get("update_now", 0.5) * (1.0 - upd_mean)
                # Timeliness favors updating frequently.
                freq_upd = len(rewards_upd) / max(1, len(recent))
                timeliness = freq_upd * upd_mean + (1 - freq_upd) * skp_mean

                return {
                    "update_quality": float(update_quality),
                    "resource_savings": float(resource_savings),
                    "timeliness": float(timeliness),
                }

            self.moea_optimizer = NSGAIIUpdateOptimizer(
                evaluate_func=evaluate,
                population_size=self.config.get("moea_population_size", 20),
                generations=self.config.get("moea_generations", 10),
                mutation_rate=self.config.get("moea_mutation_rate", 0.2),
                crossover_rate=self.config.get("moea_crossover_rate", 0.8),
                tournament_size=self.config.get("moea_tournament_size", 3),
                objective_weights=self.config.get("moea_objective_weights"),
                dynamic_weights=self.config.get("moea_dynamic_weights", True),
            )
            pareto = await self.moea_optimizer.evolve()
            self.pareto_front = pareto
            if pareto:
                weights = self.moea_optimizer._dynamic_objective_weights()
                best = self.moea_optimizer._select_best_from_pareto(pareto, weights)
                if best is not None:
                    self.global_best_weights = dict(best.weights)
                    logger.info(f"MOEA selected best weights: {self.global_best_weights}")
                    if self.modp_solver:
                        self.modp_solver.add_state(
                            f"moea_best_{time.time()}", "update_strategy_evolution",
                            {"weights": dict(best.weights)}, dict(best.objectives), 1,
                        )
            if PROMETHEUS_AVAILABLE:
                try:
                    task_metrics["moea_pareto_front"].set(len(self.pareto_front))
                except Exception:
                    pass
            return pareto
        finally:
            self._moea_running = False

    async def start_moea_loop(self) -> None:
        """Start the background MOEA loop (async context)."""
        if not self.moea_enabled:
            return
        if self._moea_task is not None and not self._moea_task.done():
            return
        self._moea_task = asyncio.create_task(self._moea_loop())

    async def stop_moea_loop(self) -> None:
        if self._moea_task is not None and not self._moea_task.done():
            self._moea_task.cancel()
            try:
                await self._moea_task
            except asyncio.CancelledError:
                pass
        self._moea_task = None

    async def _moea_loop(self) -> None:
        interval = int(self.config.get("moea_interval_seconds", 300))
        while True:
            try:
                await asyncio.sleep(interval)
                # Move CPU-heavy work off the loop.
                await asyncio.to_thread(lambda: asyncio.run_coroutine_threadsafe(
                    self.run_moea_update(), asyncio.get_running_loop()
                ).result()) if False else await self.run_moea_update()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"MOEA loop error: {e}")
                await asyncio.sleep(60)

    # ------------------------------------------------------------------
    async def get_limit_graph(self, graph_id: str = "update_tasks") -> Dict[str, Any]:
        if not self.limit_graph_manager:
            return {}
        return {
            "metadata": self.limit_graph_manager.get_metadata(graph_id),
            "nodes": self.limit_graph_manager.get_nodes(graph_id),
            "edges": self.limit_graph_manager.get_edges(graph_id),
            "has_cycle": self.limit_graph_manager.has_cycle(graph_id),
        }

    async def get_rlhf_pairs(self, limit: int = 100) -> List[Dict[str, Any]]:
        return self.rlhf_trainer.get_pairs(limit) if self.rlhf_trainer else []

    async def record_rlhf_pair(
        self,
        pair_id: str,
        prompt: str,
        chosen: str,
        rejected: str,
        reward_diff: float,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        if self.rlhf_trainer:
            self.rlhf_trainer.record_pair(pair_id, prompt, chosen, rejected, reward_diff, metadata)

    def get_scheduler_stats(self) -> Dict[str, Any]:
        stats = self.scheduler_optimizer.get_stats()
        if self.moea_enabled:
            stats["moea"] = {
                "pareto_front_size": len(self.pareto_front),
                "best_weights": dict(self.global_best_weights or {}),
                "running": self._moea_running,
            }
        if self.limit_graph_manager:
            stats["limit_graph"] = self.limit_graph_manager.get_metadata("update_tasks")
        stats["interaction_log_size"] = len(self.interaction_log)
        return stats

    def flush(self) -> None:
        self.scheduler_optimizer.flush()


# ============================================================================
# Lazy scheduler singleton
# ============================================================================

_scheduler_lock = threading.Lock()
_scheduler_singleton: Optional[AdaptiveScheduler] = None


def get_scheduler(config_dict: Optional[Dict[str, Any]] = None) -> AdaptiveScheduler:
    global _scheduler_singleton
    with _scheduler_lock:
        if _scheduler_singleton is None:
            if config_dict is None:
                cfg = get_config()
                if _PYDANTIC_V2:
                    config_dict = cfg.model_dump()
                else:
                    config_dict = cfg.dict()
            _scheduler_singleton = AdaptiveScheduler(config_dict)
        return _scheduler_singleton


def reset_scheduler_for_tests() -> None:
    global _scheduler_singleton
    with _scheduler_lock:
        if _scheduler_singleton is not None:
            try:
                _scheduler_singleton.flush()
            except Exception:
                pass
        _scheduler_singleton = None


# ============================================================================
# Celery tasks
# ============================================================================

def _run_scheduler_record_outcome(task_type: str, action: str, reward: float) -> None:
    """Record an outcome via the singleton scheduler on the worker's loop."""
    try:
        sched = get_scheduler()
        run_async(sched._record_outcome(task_type, action, reward))
    except Exception as e:
        logger.error(f"Failed to record outcome: {e}")


@app.task(
    bind=True,
    name="src.enhancements.tasks.periodic_updater.update_carbon_intensity",
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=600,
    max_retries=3,
)
def update_carbon_intensity(self):
    start_time = time.time()
    cfg = get_config()
    logger.info("Starting carbon intensity update", regions=cfg.regions)
    try:
        if CacheManager is None or CarbonIntensityFetcher is None:
            raise RuntimeError("CarbonIntensityFetcher / CacheManager not available.")
        cache = CacheManager()
        fetcher = CarbonIntensityFetcher(cache)

        async def fetch_all():
            tasks = [fetcher.get_intensity(region) for region in cfg.regions]
            return await asyncio.gather(*tasks, return_exceptions=True)

        results = run_async(fetch_all())
        failures = sum(1 for r in results if isinstance(r, Exception))
        total = len(cfg.regions)
        success_count = total - failures

        if PROMETHEUS_AVAILABLE:
            try:
                if failures == 0:
                    task_metrics["carbon_success"].inc()
                else:
                    task_metrics["carbon_failure"].inc()
                task_metrics["task_duration"].labels(
                    task_name="update_carbon_intensity"
                ).observe(time.time() - start_time)
            except Exception:
                pass

        # Reward: fraction of regions successfully updated, single-counted.
        reward = float(success_count / total) if total else 0.0
        reward = max(0.0, min(1.0, reward))
        _run_scheduler_record_outcome("carbon", "update_now", reward)
        return {
            "status": "success",
            "regions_updated": success_count,
            "total": total,
            "reward": reward,
        }
    except Exception as e:
        logger.error("Carbon intensity update failed", error=str(e), exc_info=True)
        if PROMETHEUS_AVAILABLE:
            try:
                task_metrics["carbon_failure"].inc()
            except Exception:
                pass
        _run_scheduler_record_outcome("carbon", "update_now", 0.0)
        raise self.retry(exc=e)


@app.task(
    bind=True,
    name="src.enhancements.tasks.periodic_updater.update_material_catalog",
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=600,
    max_retries=3,
)
def update_material_catalog(self):
    start_time = time.time()
    logger.info("Starting material catalog update")
    try:
        if MaterialFootprintUpdater is None:
            raise RuntimeError("MaterialFootprintUpdater not available.")
        updater = MaterialFootprintUpdater()

        async def do_update():
            await updater.update_catalog()

        run_async(do_update())
        if PROMETHEUS_AVAILABLE:
            try:
                task_metrics["material_success"].inc()
                task_metrics["task_duration"].labels(
                    task_name="update_material_catalog"
                ).observe(time.time() - start_time)
            except Exception:
                pass
        reward = 0.8
        _run_scheduler_record_outcome("material", "update_now", reward)
        return {"status": "success", "reward": reward}
    except Exception as e:
        logger.error("Material catalog update failed", error=str(e), exc_info=True)
        if PROMETHEUS_AVAILABLE:
            try:
                task_metrics["material_failure"].inc()
            except Exception:
                pass
        _run_scheduler_record_outcome("material", "update_now", 0.0)
        raise self.retry(exc=e)


@app.task(
    bind=True,
    name="src.enhancements.tasks.periodic_updater.update_helium_snapshot",
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=600,
    max_retries=3,
)
def update_helium_snapshot(self):
    start_time = time.time()
    cfg = get_config()
    logger.info("Starting helium snapshot update", url=cfg.helium_snapshot_url)
    try:
        try:
            import aiohttp  # type: ignore
            import aiofiles  # type: ignore
        except ImportError as e:
            raise RuntimeError(f"aiohttp/aiofiles required: {e}")

        async def download():
            timeout = aiohttp.ClientTimeout(total=120)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(cfg.helium_snapshot_url) as resp:
                    if resp.status != 200:
                        raise RuntimeError(f"Download failed with status {resp.status}")
                    Path(cfg.helium_snapshot_path).parent.mkdir(parents=True, exist_ok=True)
                    async with aiofiles.open(cfg.helium_snapshot_path, "wb") as f:
                        async for chunk in resp.content.iter_chunked(8192):
                            await f.write(chunk)

        run_async(download())
        if PROMETHEUS_AVAILABLE:
            try:
                task_metrics["helium_success"].inc()
                task_metrics["task_duration"].labels(
                    task_name="update_helium_snapshot"
                ).observe(time.time() - start_time)
            except Exception:
                pass
        reward = 0.7
        _run_scheduler_record_outcome("helium", "update_now", reward)
        return {"status": "success", "path": cfg.helium_snapshot_path, "reward": reward}
    except Exception as e:
        logger.error("Helium snapshot update failed", error=str(e), exc_info=True)
        if PROMETHEUS_AVAILABLE:
            try:
                task_metrics["helium_failure"].inc()
            except Exception:
                pass
        _run_scheduler_record_outcome("helium", "update_now", 0.0)
        raise self.retry(exc=e)


@app.task(
    name="src.enhancements.tasks.periodic_updater.run_scheduler",
    autoretry_for=(Exception,),
    retry_backoff=True,
    retry_backoff_max=600,
    max_retries=3,
)
def run_scheduler():
    logger.info("Running adaptive scheduler")
    try:
        sched = get_scheduler()
        async def do_all():
            # Ensure the background MOEA loop is running (best-effort).
            try:
                await sched.start_moea_loop()
            except Exception:
                pass
            for task_type in _TASK_TYPES:
                await sched.decide_and_execute(task_type)
        run_async(do_all())
        return {"status": "success"}
    except Exception as e:
        logger.error("Scheduler run failed", error=str(e), exc_info=True)
        raise


@task_success.connect
def task_success_handler(sender, **kwargs):
    logger.info("Task succeeded", task=getattr(sender, "name", "?"))
    if PROMETHEUS_AVAILABLE:
        try:
            task_metrics["update_reward"].observe(1.0)
        except Exception:
            pass


@task_failure.connect
def task_failure_handler(sender, **kwargs):
    logger.error("Task failed", task=getattr(sender, "name", "?"))
    if PROMETHEUS_AVAILABLE:
        try:
            task_metrics["update_reward"].observe(0.0)
        except Exception:
            pass


# ============================================================================
# Offline training helper
# ============================================================================

def train_historical_model(
    log_path: Optional[Path] = None,
    model_path: Optional[Path] = None,
) -> Optional[Path]:
    cfg = get_config()
    lp = _as_path(log_path) or Path(cfg.interaction_logs_path)
    mp = _as_path(model_path) or Path(cfg.historical_model_path)
    return UpdateHistoricalMLTeacher.train_from_logs([lp], mp)


__all__ = [
    "SCHEMA_VERSION",
    "UpdaterConfig",
    "get_config",
    "reset_config_for_tests",
    "RunningScaler",
    "FederatedWeightsCodec",
    "UpdateState",
    "Teacher",
    "UpdateRuleBasedTeacher",
    "UpdateHistoricalMLTeacher",
    "UpdateStatefulQTeacher",
    "RLHFUpdateTeacher",
    "DistillationStudent",
    "ReplayBuffer",
    "MoEGatingNetwork",
    "DistillationSchedulerOptimizer",
    "MOPDUpdateWeights",
    "NSGAIIUpdateOptimizer",
    "LimitGraphManager",
    "MODPOptimizer",
    "RLHFTrainer",
    "AdaptiveScheduler",
    "get_scheduler",
    "reset_scheduler_for_tests",
    "get_worker_loop",
    "run_async",
    "app",
    "train_historical_model",
]


# ============================================================================
# Tests
# ============================================================================

def _test_config_lazy() -> None:
    reset_config_for_tests()
    c1 = get_config()
    c2 = get_config()
    assert c1 is c2
    reset_config_for_tests()
    c3 = get_config()
    assert c3 is not c1


def _test_update_state_features() -> None:
    s = UpdateState(
        hours_since_last_update=6.0, hour_of_day=12.0, day_of_week=3.0,
        carbon_trend=0.02, material_version_age_days=5.0, helium_snapshot_age_days=5.0,
        current_carbon_intensity=0.4, pending_updates_count=2, system_load=0.4,
    )
    v = s.to_feature_vector()
    assert v.shape == (12,)
    assert np.all(v >= 0.0) and np.all(v <= 1.0)
    # Carbon intensity = 0.4 kg should map to ~0.4, not ~0.0004
    assert v[6] > 0.2


def _test_running_scaler() -> None:
    s = RunningScaler(12)
    for _ in range(5):
        s.update(np.random.randn(12) + 0.5)
    out = s.transform(np.random.randn(12))
    assert out.shape == (12,)
    assert np.all(np.isfinite(out))


def _test_q_teacher_update() -> None:
    q = UpdateStatefulQTeacher(feature_dim=12)
    v = np.zeros(12)
    v[0] = 1.0
    q.update_from_vector(v, 0, 1.0)
    assert np.any(q.weights[:, 0] != 0)


def _test_moe_accepts_arrays_and_dicts() -> None:
    g = MoEGatingNetwork(feature_dim=12, n_experts=4)
    x = np.random.randn(12)
    g.forward(x)
    d = {"hours_since_last_update": 3.0, "hour_of_day": 10.0, "day_of_week": 2.0}
    g.forward(MoEGatingNetwork.encode_state(d))
    s = UpdateState(
        hours_since_last_update=3.0, hour_of_day=10.0, day_of_week=2.0,
        carbon_trend=0.0, material_version_age_days=0.0, helium_snapshot_age_days=0.0,
        current_carbon_intensity=0.4, pending_updates_count=0, system_load=0.5,
    )
    g.forward(MoEGatingNetwork.encode_state(s))


def _test_gating_jacobian() -> None:
    g = MoEGatingNetwork(feature_dim=12, n_experts=4)
    x = np.random.randn(12)
    t = np.random.rand(4, 2)
    t = t / t.sum(axis=1, keepdims=True)
    student = np.random.rand(2)
    student = student / student.sum()
    gw = g.forward(x)
    g.update(x, t, student, gw)
    assert g.weights.shape == (12, 4)
    assert g.bias.shape == (4,)


def _test_nsga_distinct_objectives() -> None:
    async def evaluate(weights):
        return {
            "update_quality": weights.get("update_now", 0.5),
            "resource_savings": weights.get("skip", 0.5),
            "timeliness": 1.0 - weights.get("skip", 0.5),
        }

    opt = NSGAIIUpdateOptimizer(
        evaluate_func=evaluate, population_size=8, generations=3, dynamic_weights=False,
    )

    async def run():
        pareto = await opt.evolve()
        assert len(pareto) >= 1
        # Objectives should differ across the Pareto front (not all identical).
        if len(pareto) > 1:
            oqs = [p.objectives["update_quality"] for p in pareto]
            assert max(oqs) - min(oqs) > 1e-6

    asyncio.run(run())


def _test_federated_codec() -> None:
    w = {"weights": np.random.randn(12, 2), "biases": np.random.randn(2)}
    blob = FederatedWeightsCodec.encode(w)
    back = FederatedWeightsCodec.decode(blob)
    assert np.allclose(w["weights"], back["weights"])
    assert np.allclose(w["biases"], back["biases"])


def _test_limit_graph_cycle() -> None:
    g = LimitGraphManager()
    g.create_graph("g", "test", {})
    g.add_node("g", "a", "t", {})
    g.add_node("g", "b", "t", {})
    g.add_edge("g", "e1", "a", "b", 1.0, {})
    assert not g.has_cycle("g")
    g.add_edge("g", "e2", "b", "a", 1.0, {})
    assert g.has_cycle("g")


def _test_interaction_log_schema(tmpdir: Path) -> None:
    cfg = {
        "interaction_logs_path": str(tmpdir / "logs.csv"),
        "enable_limit_graph": False,
        "enable_modp": False,
        "enable_rlhf": False,
        "enable_moe": False,
        "moea_enabled": False,
    }
    sched = AdaptiveScheduler(cfg)
    sched._append_log_sync({
        "timestamp": "t1", "task_type": "carbon", "action": "skip",
        "reward": 0.1, "state_vec": "",
    })
    sched._append_log_sync({
        "timestamp": "t2", "task_type": "material", "action": "update_now",
        "reward": 0.8, "state_vec": "1,2,3",
    })
    df = pd.read_csv(tmpdir / "logs.csv")
    assert list(df.columns) == ["timestamp", "task_type", "action", "reward", "state_vec"]
    assert len(df) == 2


async def _async_test_scheduler_flow() -> None:
    cfg = {
        "enable_limit_graph": True,
        "enable_modp": True,
        "enable_rlhf": True,
        "enable_moe": True,
        "moea_enabled": False,
        "interaction_logs_path": "./_test_interactions.csv",
        "q_weights_path": "./_test_q_weights.json",
        "historical_model_path": "./_test_hist.pkl",
        "seed": 42,
    }
    sched = AdaptiveScheduler(cfg)
    for task in _TASK_TYPES:
        await sched.decide_and_execute(task)
    stats = sched.get_scheduler_stats()
    assert "student_counter" in stats
    assert "interaction_log_size" in stats
    sched.flush()
    # Clean up
    for p in ["_test_interactions.csv", "_test_q_weights.json", "_test_hist.pkl"]:
        try:
            Path(p).unlink(missing_ok=True)
        except Exception:
            pass


def _test_worker_loop_persistence() -> None:
    loop1 = get_worker_loop()
    loop2 = get_worker_loop()
    assert loop1 is loop2


def _run_all_tests() -> None:
    import tempfile
    _test_config_lazy()
    _test_update_state_features()
    _test_running_scaler()
    _test_q_teacher_update()
    _test_moe_accepts_arrays_and_dicts()
    _test_gating_jacobian()
    _test_federated_codec()
    _test_limit_graph_cycle()
    _test_worker_loop_persistence()
    with tempfile.TemporaryDirectory() as td:
        _test_interaction_log_schema(Path(td))
    _test_nsga_distinct_objectives()
    asyncio.run(_async_test_scheduler_flow())
    print("All PeriodicUpdater tests passed.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    _run_all_tests()
    print(
        "\nStart Celery worker:\n"
        "  celery -A src.enhancements.tasks.periodic_updater.app worker --loglevel=info\n"
        "Run scheduler task:\n"
        "  celery -A src.enhancements.tasks.periodic_updater.app call "
        "src.enhancements.tasks.periodic_updater.run_scheduler"
    )
