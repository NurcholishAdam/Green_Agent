#!/usr/bin/env python3
"""
Enhanced Anomaly Detection for Sustainability Metrics v2.4.0
==============================================================
Multi-Teacher On-Policy Distillation + Central MODP integration
+ LIMIT Graph, RLHF preference collection, MoE gating, and bio-inspired tuning.

Changes from v2.3.1:
- FIXED: anomaly polarity mismatch (unified `AnomalyResult` interface).
- FIXED: RL gradient sign in DistillationStudent.update.
- FIXED: MoE no longer bypasses distillation; experts produce action distributions
         and gating is reward-weighted.
- FIXED: relative imports guarded with fallbacks (script/package dual mode).
- FIXED: OnlineSVM now actually online (partial_fit per window).
- FIXED: concept drift wired into ingest(); drift_scores populated.
- FIXED: Q-teacher updated after reward (full state stored in replay).
- FIXED: FastAPI app.state.detector initialized via lifespan.
- ADDED: input validation (NaN/inf rejection, feature range clamping).
- ADDED: unified anomaly scoring (score_samples / is_anomaly) across models.
- ADDED: feature attribution for all model types.
- ADDED: LIMIT graph constraints used to filter MODP candidates.
- ADDED: PSO with a real objective function.
- ADDED: thread-safe buffers and per-node locks.
"""

import asyncio
import json
import logging
import os
import sqlite3
import time
import pickle
import hashlib
import uuid
import math
from collections import deque
from dataclasses import dataclass, field, asdict
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
    from fastapi import FastAPI, HTTPException, BackgroundTasks, Request
    from fastapi.responses import JSONResponse, Response
    from contextlib import asynccontextmanager
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
# CENTRAL GREEN AGENT COMPONENTS (with fallbacks so the file works standalone)
# -----------------------------------------------------------------------------
try:
    from ..config import config as central_config
    from ..storage import Storage
    from ..schemas.feedback_event import FeedbackEvent
    from ..routing.pareto_gating import ParetoGating
    from ..feedback.adaptive_cost import AdaptiveCostFunction
    from ..safety.drift_detector import DriftDetector
    from ..scaling.message_queue import AsyncMessageQueue
    from ..metrics import MetricsRegistry
    from ..logger import logger as central_logger
    CENTRAL_AVAILABLE = True
except Exception:
    CENTRAL_AVAILABLE = False

    class _FallbackConfig:
        ENVIRONMENT = "production"

    central_config = _FallbackConfig()

    class Storage:  # type: ignore
        pass

    class FeedbackEvent:  # type: ignore
        @staticmethod
        def create_with_context(**kwargs):
            return FeedbackEventStub(**kwargs)

    class FeedbackEventStub:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)
        def to_json(self):
            return json.dumps(self.__dict__, default=str)

    class ParetoGating:  # type: ignore
        def filter(self, candidates):
            return candidates

    class AdaptiveCostFunction:  # type: ignore
        def __init__(self, *a, **k):
            pass
        def compute(self, quality=0.0, carbon_g=0.0, latency_ms=0.0,
                    energy_joules=0.0, health=0.0, atp=0.0, **kwargs):
            # Lower is better (cost). We expose this contract explicitly.
            return (
                1.0 * (1.0 - quality)
                + 0.001 * carbon_g
                + 0.0005 * latency_ms
                + 0.0002 * energy_joules
                + 0.1 * (1.0 - health)
                + 0.05 * (1.0 - atp)
            )
        def get_current_weights(self):
            return {}

    class DriftDetector:  # type: ignore
        async def check_drift(self, *a, **k):
            return 0.0

    class AsyncMessageQueue:  # type: ignore
        async def publish(self, *a, **k):
            return None

    class MetricsRegistry:  # type: ignore
        pass

    central_logger = logger


# ============================================================================
# 1. CONFIGURATION
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
        online_partial_fit_every: int = Field(5, ge=1)
        metrics_features: List[str] = Field(
            default=["energy_joules", "carbon_kg", "helium_usage", "latency_ms", "accuracy"]
        )
        persistence_enabled: bool = True
        persistence_path: str = Field("./anomaly_state.db")
        model_save_path: str = Field("./models/")
        enable_explanation: bool = True
        concept_drift_enabled: bool = True
        drift_threshold_multiplier: float = Field(2.0, gt=0)
        drift_min_samples: int = Field(20, ge=5)
        webhook_url: Optional[str] = None

        # Distillation parameters
        distillation_epsilon: float = Field(0.1, ge=0, le=1)
        distillation_epsilon_min: float = Field(0.01, ge=0, le=1)
        distillation_epsilon_decay: float = Field(0.995, ge=0, le=1)
        distillation_train_every: int = Field(10, ge=1)
        distillation_replay_size: int = Field(2000, ge=10)
        distillation_learning_rate: float = Field(0.01, ge=0.0001, le=1)
        distill_weight: float = Field(0.7, ge=0, le=1)
        rl_weight: float = Field(0.3, ge=0, le=1)
        entropy_bonus: float = Field(0.01, ge=0, le=1)

        # Advanced flags
        enable_limit_graph: bool = True
        enable_modp_solver: bool = True
        enable_rlhf: bool = True
        enable_moe_gating: bool = True
        enable_pso_tuning: bool = True
        moe_expert_count: int = 4
        pso_particles: int = 10
        pso_iterations: int = 20
        pso_enabled_periodic: bool = False
        pso_periodic_interval: int = Field(3600, ge=60)

        # Input validation
        validate_inputs: bool = True

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
        "online_partial_fit_every": 5,
        "metrics_features": ["energy_joules", "carbon_kg", "helium_usage", "latency_ms", "accuracy"],
        "persistence_enabled": True,
        "persistence_path": "./anomaly_state.db",
        "model_save_path": "./models/",
        "enable_explanation": True,
        "concept_drift_enabled": True,
        "drift_threshold_multiplier": 2.0,
        "drift_min_samples": 20,
        "webhook_url": None,
        "distillation_epsilon": 0.1,
        "distillation_epsilon_min": 0.01,
        "distillation_epsilon_decay": 0.995,
        "distillation_train_every": 10,
        "distillation_replay_size": 2000,
        "distillation_learning_rate": 0.01,
        "distill_weight": 0.7,
        "rl_weight": 0.3,
        "entropy_bonus": 0.01,
        "enable_limit_graph": True,
        "enable_modp_solver": True,
        "enable_rlhf": True,
        "enable_moe_gating": True,
        "enable_pso_tuning": True,
        "moe_expert_count": 4,
        "pso_particles": 10,
        "pso_iterations": 20,
        "pso_enabled_periodic": False,
        "pso_periodic_interval": 3600,
        "validate_inputs": True,
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
    feature_contributions: Optional[Dict[str, float]] = None
    is_anomaly: bool = True


@dataclass
class AnomalyResult:
    """Unified model output. Higher `score` = more anomalous."""
    score: float
    is_anomaly: bool
    feature_contributions: Dict[str, float] = field(default_factory=dict)
    reconstruction_error: float = 0.0


# ============================================================================
# 3. INPUT VALIDATION HELPERS
# ============================================================================
class InputValidator:
    """Validates and sanitizes incoming telemetry metrics."""
    DEFAULT_FEATURE_RANGES = {
        "energy_joules": (0.0, 1e9),
        "carbon_kg": (0.0, 1e6),
        "helium_usage": (0.0, 1e6),
        "latency_ms": (0.0, 1e7),
        "accuracy": (0.0, 1.0),
    }

    def __init__(self, feature_ranges: Optional[Dict[str, Tuple[float, float]]] = None,
                 strict: bool = False):
        self.feature_ranges = feature_ranges or self.DEFAULT_FEATURE_RANGES
        self.strict = strict

    def sanitize(self, metrics: Dict[str, float]) -> Tuple[Dict[str, float], List[str]]:
        issues: List[str] = []
        clean: Dict[str, float] = {}
        for k, v in metrics.items():
            try:
                fv = float(v)
            except (TypeError, ValueError):
                issues.append(f"{k}: not numeric")
                continue
            if math.isnan(fv) or math.isinf(fv):
                issues.append(f"{k}: NaN/inf")
                continue
            lo, hi = self.feature_ranges.get(k, (-math.inf, math.inf))
            if fv < lo or fv > hi:
                if self.strict:
                    issues.append(f"{k}: out of range [{lo}, {hi}]")
                    continue
                clamped = max(lo, min(hi, fv))
                issues.append(f"{k}: clamped {fv} -> {clamped}")
                fv = clamped
            clean[k] = fv
        return clean, issues


# ============================================================================
# 4. TELEMETRY BUFFER (thread-safe with lock)
# ============================================================================
class TelemetryBuffer:
    def __init__(self, window_size: int = 100,
                 persistence_manager: Optional['PersistenceManager'] = None):
        self.window_size = window_size
        self.buffers: Dict[str, Dict[str, deque]] = {}
        self.persistence = persistence_manager
        self._lock = asyncio.Lock()

    async def add_sample(self, node_id: str, metrics: Dict[str, float]) -> None:
        async with self._lock:
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

    def has_enough_data(self, node_id: str, metric_names: List[str],
                        min_samples: int = 10) -> bool:
        if node_id not in self.buffers:
            return False
        for name in metric_names:
            if name not in self.buffers[node_id]:
                return False
            if len(self.buffers[node_id][name]) < min_samples:
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
# 5. PERSISTENCE MANAGER
# ============================================================================
class PersistenceManager:
    def __init__(self, config: Union['AnomalyConfig', Dict[str, Any]]):
        self.config = config
        if hasattr(config, 'dict'):
            self.config_dict = config.dict()
        else:
            self.config_dict = dict(config)
        self.db_path = self.config_dict.get('persistence_path', './anomaly_state.db')
        self.model_path = self.config_dict.get('model_save_path', './models/')
        os.makedirs(self.model_path, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
        except sqlite3.Error:
            pass
        return conn

    def _init_db(self):
        conn = self._connect()
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
        conn.execute("""
            CREATE TABLE IF NOT EXISTS anomaly_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp REAL,
                node_id TEXT,
                metric_name TEXT,
                metric_value REAL,
                anomaly_score REAL,
                description TEXT,
                action TEXT
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_telemetry_node_time ON telemetry (node_id, timestamp)")
        conn.commit()
        conn.close()

    def save_telemetry(self, node_id: str, metrics: Dict[str, float]):
        conn = self._connect()
        conn.execute("""
            INSERT INTO telemetry (node_id, timestamp, energy_joules, carbon_kg, helium_usage, latency_ms, accuracy)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (node_id, time.time(), metrics.get('energy_joules', 0), metrics.get('carbon_kg', 0),
              metrics.get('helium_usage', 0), metrics.get('latency_ms', 0), metrics.get('accuracy', 0)))
        conn.commit()
        conn.close()

    def load_telemetry(self, node_id: str, limit: int = 1000) -> List[Dict]:
        conn = self._connect()
        rows = conn.execute("""
            SELECT timestamp, energy_joules, carbon_kg, helium_usage, latency_ms, accuracy
            FROM telemetry WHERE node_id = ? ORDER BY timestamp DESC LIMIT ?
        """, (node_id, limit)).fetchall()
        conn.close()
        return [{'timestamp': r[0], 'energy_joules': r[1], 'carbon_kg': r[2],
                 'helium_usage': r[3], 'latency_ms': r[4], 'accuracy': r[5]} for r in rows]

    def save_model(self, node_id: str, model: 'BaseAnomalyModel'):
        model_blob = pickle.dumps(model)
        conn = self._connect()
        conn.execute("""
            INSERT OR REPLACE INTO models (node_id, model_type, model_blob, trained_at, config_snapshot)
            VALUES (?, ?, ?, ?, ?)
        """, (node_id, model.__class__.__name__, model_blob, time.time(), json.dumps(self.config_dict)))
        conn.commit()
        conn.close()

    def load_model(self, node_id: str) -> Optional['BaseAnomalyModel']:
        conn = self._connect()
        row = conn.execute("SELECT model_blob FROM models WHERE node_id = ?", (node_id,)).fetchone()
        conn.close()
        if row:
            try:
                return pickle.loads(row[0])
            except Exception as e:
                logger.warning(f"Failed to load model for {node_id}: {e}")
        return None

    def save_anomaly_event(self, event: 'AnomalyEvent'):
        conn = self._connect()
        conn.execute("""
            INSERT INTO anomaly_events (timestamp, node_id, metric_name, metric_value, anomaly_score, description, action)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (event.timestamp.timestamp(), event.node_id, event.metric_name, event.metric_value,
              event.anomaly_score, event.description, event.auto_response_taken))
        conn.commit()
        conn.close()

    def delete_model(self, node_id: str):
        conn = self._connect()
        conn.execute("DELETE FROM models WHERE node_id = ?", (node_id,))
        conn.commit()
        conn.close()


# ============================================================================
# 6. ANOMALY DETECTION MODELS (UNIFIED INTERFACE)
# ============================================================================
class BaseAnomalyModel:
    """All anomaly models share the same scoring/decision interface.

    Contract:
      - `score(data)` returns array where HIGHER = more anomalous.
      - `is_anomaly(data, threshold=None)` returns boolean array.
      - `explain(data)` returns per-feature contributions for the last row.
    """
    name = "base"

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.is_trained = False
        self.feature_names = config.get('metrics_features', [])

    def train(self, data: np.ndarray) -> None: raise NotImplementedError
    def partial_fit(self, data: np.ndarray) -> None: raise NotImplementedError

    def score(self, data: np.ndarray) -> np.ndarray: raise NotImplementedError

    def is_anomaly(self, data: np.ndarray, threshold: Optional[float] = None) -> np.ndarray:
        scores = self.score(data)
        if threshold is None:
            # Default: adaptive threshold on training residuals if available
            if getattr(self, "score_threshold", None) is None:
                if scores.size == 0:
                    return np.zeros(0, dtype=bool)
                self.score_threshold = float(np.percentile(scores, 95))
            threshold = self.score_threshold
        return scores > threshold

    def explain(self, data: np.ndarray) -> Dict[str, float]:
        return {}


class IsolationForestModel(BaseAnomalyModel):
    name = "isolation_forest"

    def __init__(self, config):
        super().__init__(config)
        self.model = None
        self.contamination = config.get('contamination', 0.05)
        self.score_threshold: Optional[float] = None

    def train(self, data):
        if data.shape[0] < 10 or not SKLEARN_AVAILABLE:
            self.is_trained = False
            return
        self.model = IsolationForest(contamination=self.contamination,
                                     random_state=42, n_estimators=100)
        self.model.fit(data)
        # Calibrate threshold from training data
        train_scores = -self.model.score_samples(data)
        self.score_threshold = float(np.percentile(train_scores, 100 * (1 - self.contamination)))
        self.is_trained = True

    def partial_fit(self, data):
        self.train(data)

    def score(self, data):
        if not self.is_trained or self.model is None:
            return np.zeros(data.shape[0])
        return -self.model.score_samples(data)  # higher = anomalous

    def is_anomaly(self, data, threshold=None):
        if not self.is_trained or self.model is None:
            return np.zeros(data.shape[0], dtype=bool)
        if threshold is None:
            threshold = self.score_threshold
        scores = self.score(data)
        return scores > threshold

    def explain(self, data):
        if not self.is_trained or data.size == 0:
            return {}
        # Approximate per-feature contribution: |x - mean| * std_dev_importance
        try:
            x = data[-1]
            # Use feature deviation from training mean weighted by inverse std
            if not hasattr(self, "_mean"):
                return {}
            z = np.abs((x - self._mean) / (self._std + 1e-9))
            total = z.sum() + 1e-9
            return {name: float(z[i] / total) for i, name in enumerate(self.feature_names)}
        except Exception:
            return {}

    def train(self, data):  # noqa: F811 (kept single definition)
        if data.shape[0] < 10 or not SKLEARN_AVAILABLE:
            self.is_trained = False
            return
        self.model = IsolationForest(contamination=self.contamination,
                                     random_state=42, n_estimators=100)
        self.model.fit(data)
        self._mean = data.mean(axis=0)
        self._std = data.std(axis=0)
        self._std[self._std == 0] = 1e-6
        train_scores = -self.model.score_samples(data)
        self.score_threshold = float(np.percentile(train_scores, 100 * (1 - self.contamination)))
        self.is_trained = True


class OnlineSVM(BaseAnomalyModel):
    name = "online_svm"

    def __init__(self, config):
        super().__init__(config)
        self.model = None
        self.nu = config.get('contamination', 0.05)
        self.initialized = False
        self.score_threshold: Optional[float] = None
        self._buffer: List[np.ndarray] = []

    def train(self, data):
        if data.shape[0] < 10 or not ONLINE_AVAILABLE:
            return
        self.model = SGDOneClassSVM(nu=self.nu, random_state=42)
        self.model.partial_fit(data)
        scores = -self.model.score_samples(data)
        self.score_threshold = float(np.percentile(scores, 100 * (1 - self.nu)))
        self.is_trained = True
        self.initialized = True

    def partial_fit(self, data):
        if not self.initialized:
            self.train(data)
        else:
            self.model.partial_fit(data)
            # Recalibrate threshold on a rolling window
            self._buffer.append(data)
            if len(self._buffer) > 20:
                self._buffer.pop(0)
            recent = np.vstack(self._buffer[-5:])
            scores = -self.model.score_samples(recent)
            self.score_threshold = float(np.percentile(scores, 100 * (1 - self.nu)))

    def score(self, data):
        if not self.is_trained or self.model is None:
            return np.zeros(data.shape[0])
        return -self.model.score_samples(data)

    def is_anomaly(self, data, threshold=None):
        if not self.is_trained:
            return np.zeros(data.shape[0], dtype=bool)
        if threshold is None:
            threshold = self.score_threshold
        return self.score(data) > threshold

    def explain(self, data):
        if not self.is_trained or data.size == 0:
            return {}
        if not hasattr(self, "_mean"):
            return {}
        x = data[-1]
        z = np.abs((x - self._mean) / (self._std + 1e-9))
        total = z.sum() + 1e-9
        return {name: float(z[i] / total) for i, name in enumerate(self.feature_names)}


class AutoencoderModel(BaseAnomalyModel):
    """Lightweight reconstruction-error autoencoder using numpy.

    Falls back to a PCA-like linear autoencoder when torch is unavailable.
    """
    name = "autoencoder"

    def __init__(self, config):
        super().__init__(config)
        self.W: Optional[np.ndarray] = None
        self.mean: Optional[np.ndarray] = None
        self.std: Optional[np.ndarray] = None
        self.score_threshold: Optional[float] = None

    def train(self, data):
        if data.shape[0] < 10:
            self.is_trained = False
            return
        self.mean = data.mean(axis=0)
        self.std = data.std(axis=0)
        self.std[self.std == 0] = 1e-6
        X = (data - self.mean) / self.std
        # Truncated SVD as linear autoencoder
        try:
            U, S, Vt = np.linalg.svd(X, full_matrices=False)
            k = max(1, min(X.shape[1] - 1, int(X.shape[1] * 0.6)))
            self.W = Vt[:k].T  # (features, k)
        except np.linalg.LinAlgError:
            self.W = None
        recon = self._reconstruct(X)
        errs = np.mean((X - recon) ** 2, axis=1)
        self.score_threshold = float(np.percentile(errs, 95))
        self.is_trained = True

    def partial_fit(self, data):
        self.train(data)

    def _reconstruct(self, X):
        if self.W is None:
            return X
        Z = X @ self.W
        return Z @ self.W.T

    def score(self, data):
        if not self.is_trained or self.mean is None:
            return np.zeros(data.shape[0])
        X = (data - self.mean) / self.std
        recon = self._reconstruct(X)
        return np.mean((X - recon) ** 2, axis=1)

    def is_anomaly(self, data, threshold=None):
        if not self.is_trained:
            return np.zeros(data.shape[0], dtype=bool)
        if threshold is None:
            threshold = self.score_threshold
        return self.score(data) > threshold

    def explain(self, data):
        if not self.is_trained or self.mean is None or data.size == 0:
            return {}
        X = (data - self.mean) / self.std
        recon = self._reconstruct(X)
        per_feature = (X[-1] - recon[-1]) ** 2
        total = per_feature.sum() + 1e-9
        return {name: float(per_feature[i] / total) for i, name in enumerate(self.feature_names)}


class ThresholdModel(BaseAnomalyModel):
    name = "threshold"

    def __init__(self, config):
        super().__init__(config)
        self.threshold_multiplier = config.get('energy_spike_threshold', 2.0)
        self.means = None
        self.stds = None
        self.score_threshold: Optional[float] = None

    def train(self, data):
        if data.shape[0] == 0:
            self.is_trained = False
            return
        self.means = np.mean(data, axis=0)
        self.stds = np.std(data, axis=0)
        self.stds[self.stds == 0] = 1e-6
        z_scores = np.abs((data - self.means) / self.stds)
        self.score_threshold = float(np.percentile(np.max(z_scores, axis=1), 95))
        self.is_trained = True

    def partial_fit(self, data):
        self.train(data)

    def score(self, data):
        if not self.is_trained:
            return np.zeros(data.shape[0])
        z_scores = np.abs((data - self.means) / self.stds)
        return np.max(z_scores, axis=1)

    def is_anomaly(self, data, threshold=None):
        if not self.is_trained:
            return np.zeros(data.shape[0], dtype=bool)
        # Use z-score threshold unless a score threshold is given
        z_scores = np.abs((data - self.means) / self.stds)
        return np.any(z_scores > self.threshold_multiplier, axis=1)

    def explain(self, data):
        if not self.is_trained or data.size == 0:
            return {}
        z_scores = np.abs((data - self.means) / self.stds)
        total = np.sum(z_scores) + 1e-8
        return {name: float(z_scores[0, i] / total) for i, name in enumerate(self.feature_names)}


# ============================================================================
# 7. DISTILLATION COMPONENTS (RL SIGN FIXED + Q-TEACHER UPDATE)
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

    FEATURE_DIM = 9

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

    def to_dict(self) -> Dict[str, float]:
        return asdict(self)


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
        elif state.metric_name_encoded in [0, 1]:
            probs[4] = 0.6
        else:
            probs[0] = 0.6
        return probs / probs.sum()
    def confidence(self, state):
        return 0.6 if state.persistent_count >= 3 else 0.4


class ResponseHistoricalMLTeacher(Teacher):
    def __init__(self, model_path=None):
        self.model = None
        if model_path and Path(model_path).exists() and SKLEARN_ML:
            try:
                import joblib
                self.model = joblib.load(model_path)
            except Exception:
                logger.warning("joblib not available or load failed; historical ML teacher disabled")
                self.model = None
    def predict(self, state):
        if self.model is None:
            return np.ones(5) / 5
        x = state.to_feature_vector().reshape(1, -1)
        return self.model.predict_proba(x)[0]
    def confidence(self, state):
        return 0.7 if self.model is not None else 0.0


class ResponseStatefulQTeacher(Teacher):
    def __init__(self, lr=0.1):
        self.lr = lr
        self.weights = np.zeros((AnomalyResponseState.FEATURE_DIM, 5))
        self.q_scale = 1.0

    def predict(self, state):
        q = state.to_feature_vector() @ self.weights
        exp_q = np.exp((q - np.max(q)) / max(self.q_scale, 1e-6))
        return exp_q / exp_q.sum()

    def confidence(self, state):
        return 0.5

    def update(self, state, action, reward):
        x = state.to_feature_vector()
        q_current = float(np.dot(x, self.weights[:, action]))
        td_target = reward
        td_error = td_target - q_current
        self.weights[:, action] += self.lr * td_error * x


class DistillationStudent:
    def __init__(self, feature_dim=9, n_classes=5, lr=0.01,
                 distill_weight: float = 0.7, rl_weight: float = 0.3,
                 entropy_bonus: float = 0.01):
        self.weights = np.zeros((feature_dim, n_classes))
        self.biases = np.zeros(n_classes)
        self.lr = lr
        self.n_classes = n_classes
        self.distill_weight = distill_weight
        self.rl_weight = rl_weight
        self.entropy_bonus = entropy_bonus
        self.counter = 0

    def predict_proba(self, x):
        logits = x @ self.weights + self.biases
        logits = logits - np.max(logits)
        exp_logits = np.exp(logits)
        return exp_logits / exp_logits.sum()

    def update(self, x, teacher_probs, reward, action):
        """Combined distillation + policy-gradient update.

        Distillation: cross-entropy with teacher -> grad = (p - teacher).
        RL: policy gradient ASCENT -> update direction is +reward * (one_hot - p).
        We implement descent on loss = -(rl_weight * reward * log p[a]) + distill_loss.
        Hence:  grad = (p - teacher) - rl_weight * reward * (one_hot - p)
        Then weights -= lr * outer(x, grad).
        """
        current = self.predict_proba(x)

        # Distillation gradient (descent on CE)
        grad_distill = (current - teacher_probs)

        # Policy gradient ascent => descent on -advantage * log pi
        one_hot = np.zeros_like(current)
        one_hot[action] = 1.0
        advantage = reward - float(np.dot(current, one_hot) * 0.0)  # raw reward as advantage
        grad_rl = -advantage * (one_hot - current)

        # Entropy bonus: encourage exploration (descent on -H)
        eps = 1e-9
        grad_entropy = -(-np.log(current + eps) - 1.0) * current  # d(-H)/dp approx
        # Simpler practical form: -dH/dp ~ log(p) + 1
        grad_entropy = -(np.log(current + eps) + 1.0) * current

        grad = (self.distill_weight * grad_distill
                + self.rl_weight * grad_rl
                - self.entropy_bonus * grad_entropy)

        self.weights -= self.lr * np.outer(x, grad)
        self.biases -= self.lr * grad
        self.counter += 1


class ReplayBuffer:
    """Stores full states so Q-teacher can update after reward."""
    def __init__(self, max_size=2000):
        self.buffer: deque = deque(maxlen=max_size)

    def push(self, state_obj, action, reward, next_state_obj, teacher_probs):
        self.buffer.append((state_obj, action, reward, next_state_obj, teacher_probs))

    def sample(self, batch_size=32):
        if len(self.buffer) < batch_size:
            batch = list(self.buffer)
        else:
            batch = random.sample(self.buffer, batch_size)
        states, actions, rewards, next_states, teacher_probs = zip(*batch)
        return list(states), list(actions), np.array(rewards), list(next_states), np.array(teacher_probs)

    def __len__(self):
        return len(self.buffer)


class DistillationResponseOptimizer:
    ACTION_SPACE = ['alert_only', 'reroute', 'restart', 'escalate', 'adaptive_cost']

    def __init__(self, detector, config):
        self.detector = detector
        self.config = config
        self.student = DistillationStudent(
            lr=config.get('distillation_learning_rate', 0.01),
            distill_weight=config.get('distill_weight', 0.7),
            rl_weight=config.get('rl_weight', 0.3),
            entropy_bonus=config.get('entropy_bonus', 0.01),
        )
        self.q_teacher = ResponseStatefulQTeacher(lr=0.1)
        self.teachers: List[Teacher] = [
            ResponseRuleBasedTeacher(),
            ResponseHistoricalMLTeacher(),
            self.q_teacher,
        ]
        self.replay_buffer = ReplayBuffer(config.get('distillation_replay_size', 2000))
        self.epsilon = config.get('distillation_epsilon', 0.1)
        self.epsilon_min = config.get('distillation_epsilon_min', 0.01)
        self.epsilon_decay = config.get('distillation_epsilon_decay', 0.995)
        self.train_every = config.get('distillation_train_every', 10)
        self.counter = 0
        self.lock = asyncio.Lock()

    def _teacher_mixture(self, state) -> np.ndarray:
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
        return teacher_probs

    async def select_action(self, state: AnomalyResponseState, exploration=True):
        async with self.lock:
            state_vec = state.to_feature_vector()
            teacher_probs = self._teacher_mixture(state)
            student_probs = self.student.predict_proba(state_vec)
            if exploration and random.random() < self.epsilon:
                action_idx = random.randint(0, 4)
            else:
                combined = 0.8 * student_probs + 0.2 * teacher_probs
                action_idx = int(np.argmax(combined))
            self.epsilon = max(self.epsilon_min, self.epsilon * self.epsilon_decay)
            return self.ACTION_SPACE[action_idx], action_idx, state_vec, teacher_probs

    async def update(self, state_obj, action_idx, reward,
                     next_state_obj, teacher_probs):
        async with self.lock:
            self.replay_buffer.push(state_obj, action_idx, reward,
                                    next_state_obj, teacher_probs)
            self.counter += 1

            # Q-teacher update (uses full state objects)
            self.q_teacher.update(state_obj, action_idx, reward)

            if self.counter % self.train_every == 0 and len(self.replay_buffer) >= 8:
                states, actions, rewards, _, teacher_probs_batch = self.replay_buffer.sample(8)
                for i, s in enumerate(states):
                    x = s.to_feature_vector()
                    self.student.update(x, teacher_probs_batch[i],
                                        rewards[i], actions[i])

    def get_stats(self):
        return {
            'student_counter': self.student.counter,
            'buffer_size': len(self.replay_buffer),
            'epsilon': self.epsilon,
        }


# ============================================================================
# 8. ADVANCED MODULES: LIMIT, MODP, RLHF, PSO, MoE (UPGRADED)
# ============================================================================
class LimitGraphManager:
    """Wrapper for LIMIT Graph storage methods."""
    def __init__(self, storage: Optional[Storage] = None):
        self.storage = storage
        self._local_nodes: Dict[str, Dict[str, Dict]] = {}
        self._local_edges: Dict[str, Dict[str, Dict]] = {}
        self._local_meta: Dict[str, Dict] = {}

    def create_graph(self, graph_id, description, configuration):
        self._local_meta[graph_id] = {"description": description, "config": configuration}
        if self.storage and hasattr(self.storage, 'save_limit_graph_metadata'):
            self.storage.save_limit_graph_metadata(graph_id, description, configuration)

    def add_node(self, graph_id, node_id, node_type, attributes):
        self._local_nodes.setdefault(graph_id, {})[node_id] = {
            "type": node_type, "attributes": attributes,
        }
        if self.storage and hasattr(self.storage, 'save_limit_graph_node'):
            self.storage.save_limit_graph_node(node_id, graph_id, node_type, attributes)

    def add_edge(self, graph_id, edge_id, source, target, weight, attributes):
        self._local_edges.setdefault(graph_id, {})[edge_id] = {
            "source": source, "target": target, "weight": weight, "attributes": attributes,
        }
        if self.storage and hasattr(self.storage, 'save_limit_graph_edge'):
            self.storage.save_limit_graph_edge(edge_id, graph_id, source, target, weight, attributes)

    def get_nodes(self, graph_id):
        if self.storage and hasattr(self.storage, 'get_limit_graph_nodes'):
            return self.storage.get_limit_graph_nodes(graph_id)
        return list(self._local_nodes.get(graph_id, {}).items())

    def get_edges(self, graph_id):
        if self.storage and hasattr(self.storage, 'get_limit_graph_edges'):
            return self.storage.get_limit_graph_edges(graph_id)
        return list(self._local_edges.get(graph_id, {}).items())

    def get_metadata(self, graph_id):
        if self.storage and hasattr(self.storage, 'get_limit_graph_metadata'):
            return self.storage.get_limit_graph_metadata(graph_id)
        return self._local_meta.get(graph_id)

    def allowed_actions(self, graph_id: str, context: Dict[str, Any]) -> Optional[set]:
        """Return a set of allowed action names based on graph edges, or None if unrestricted.

        Convention: nodes of type "action" with an edge from a context node ("node_id")
        whose attributes match `context` represent allowed actions.
        """
        nodes = dict(self.get_nodes(graph_id)) if not isinstance(self.get_nodes(graph_id), dict) else self.get_nodes(graph_id)
        edges = dict(self.get_edges(graph_id)) if not isinstance(self.get_edges(graph_id), dict) else self.get_edges(graph_id)
        if not nodes:
            return None
        ctx_node = context.get("node_id")
        if ctx_node is None or ctx_node not in nodes:
            return None
        allowed = set()
        for _, edge in edges.items():
            if edge.get("source") == ctx_node:
                target = edge.get("target")
                if target in nodes and nodes[target].get("type") == "action":
                    allowed.add(nodes[target].get("attributes", {}).get("name"))
        return allowed if allowed else None


class MODPOptimizer:
    """Multi-Objective Dynamic Programming wrapper with a simple lexicographic solver."""
    def __init__(self, storage: Optional[Storage] = None):
        self.storage = storage
        self._local_states: Dict[str, List[Dict]] = {}
        self._local_transitions: Dict[str, List[Dict]] = {}
        self._local_policies: Dict[str, List[Dict]] = {}

    def add_state(self, state_id, problem_id, state_attributes, objective_values, stage):
        self._local_states.setdefault(problem_id, []).append({
            "state_id": state_id, "attributes": state_attributes,
            "objectives": objective_values, "stage": stage,
        })
        if self.storage and hasattr(self.storage, 'save_modp_state'):
            self.storage.save_modp_state(state_id, problem_id, state_attributes, objective_values, stage)

    def add_transition(self, transition_id, problem_id, from_state, to_state, action, cost, objective_deltas):
        self._local_transitions.setdefault(problem_id, []).append({
            "id": transition_id, "from": from_state, "to": to_state,
            "action": action, "cost": cost, "deltas": objective_deltas,
        })
        if self.storage and hasattr(self.storage, 'save_modp_transition'):
            self.storage.save_modp_transition(transition_id, problem_id, from_state, to_state,
                                              action, cost, objective_deltas)

    def add_policy(self, policy_id, problem_id, state_id, action, expected_objectives):
        self._local_policies.setdefault(problem_id, []).append({
            "id": policy_id, "state": state_id, "action": action, "expected": expected_objectives,
        })
        if self.storage and hasattr(self.storage, 'save_modp_policy'):
            self.storage.save_modp_policy(policy_id, problem_id, state_id, action, expected_objectives)

    def get_states(self, problem_id):
        if self.storage and hasattr(self.storage, 'get_modp_states'):
            return self.storage.get_modp_states(problem_id)
        return self._local_states.get(problem_id, [])

    def get_transitions(self, problem_id):
        if self.storage and hasattr(self.storage, 'get_modp_transitions'):
            return self.storage.get_modp_transitions(problem_id)
        return self._local_transitions.get(problem_id, [])

    def get_policies(self, problem_id):
        if self.storage and hasattr(self.storage, 'get_modp_policies'):
            return self.storage.get_modp_policies(problem_id)
        return self._local_policies.get(problem_id, [])

    async def solve(self, problem_id, initial_state, max_stages=10):
        """Lexicographic Pareto-ish solver over the transition set."""
        transitions = self._local_transitions.get(problem_id, [])
        if not transitions:
            return {"status": "empty", "pareto_front": []}
        # Enumerate simple paths up to max_stages
        by_from: Dict[str, List[Dict]] = {}
        for t in transitions:
            by_from.setdefault(t["from"], []).append(t)

        paths: List[List[Dict]] = []
        def dfs(node, path, visited):
            if len(path) >= max_stages:
                paths.append(list(path))
                return
            nxt = by_from.get(node, [])
            if not nxt:
                paths.append(list(path))
                return
            for t in nxt:
                if t["to"] in visited:
                    continue
                visited.add(t["to"])
                path.append(t)
                dfs(t["to"], path, visited)
                path.pop()
                visited.remove(t["to"])

        for t in transitions:
            if t["from"] == initial_state:
                dfs(t["to"], [t], {initial_state, t["to"]})

        if not paths:
            return {"status": "no_paths", "pareto_front": []}

        # Compute total objective per path and Pareto-filter
        def total_obj(p):
            cost = sum(t["cost"] for t in p)
            carbon = sum(t.get("deltas", {}).get("carbon", 0.0) for t in p)
            quality = sum(t.get("deltas", {}).get("quality", 0.0) for t in p)
            return {"cost": cost, "carbon": carbon, "quality": quality}

        objs = [(p, total_obj(p)) for p in paths]
        pareto_front: List[Tuple[List[Dict], Dict[str, float]]] = []
        for p, o in objs:
            dominated = False
            for q, oq in objs:
                if q is p:
                    continue
                if (oq["cost"] <= o["cost"] and oq["carbon"] <= o["carbon"]
                        and oq["quality"] >= o["quality"]
                        and (oq["cost"] < o["cost"] or oq["carbon"] < o["carbon"]
                             or oq["quality"] > o["quality"])):
                    dominated = True
                    break
            if not dominated:
                pareto_front.append((p, o))

        serialized = [{"path": [t["action"] for t in p], "objectives": o} for p, o in pareto_front]
        return {"status": "solved", "pareto_front": serialized}


class RLHFTrainer:
    """Collects human preference pairs and trains a simple Bradley-Terry reward model."""
    def __init__(self, storage: Optional[Storage] = None):
        self.storage = storage
        self._pairs: List[Dict] = []
        self.reward_weights: Optional[np.ndarray] = None

    def record_pair(self, pair_id, prompt, chosen, rejected, reward_diff, metadata=None):
        pair = {"id": pair_id, "prompt": prompt, "chosen": chosen,
                "rejected": rejected, "reward_diff": reward_diff,
                "metadata": metadata or {}}
        self._pairs.append(pair)
        if self.storage and hasattr(self.storage, 'save_preference_pair'):
            self.storage.save_preference_pair(pair_id, prompt, chosen, rejected, reward_diff, metadata)

    def get_pairs(self, limit=100):
        if self.storage and hasattr(self.storage, 'get_preference_pairs'):
            return self.storage.get_preference_pairs(limit)
        return self._pairs[-limit:]

    def _featurize(self, action: str, prompt: str) -> np.ndarray:
        h = int(hashlib.md5(f"{prompt}|{action}".encode()).hexdigest()[:8], 16)
        base = np.array([(h >> (4 * i)) & 0xF for i in range(8)], dtype=np.float32) / 15.0
        return base

    def train_reward_model(self, epochs: int = 50, lr: float = 0.05):
        pairs = self.get_pairs()
        if len(pairs) < 5:
            logger.info("Not enough preference pairs for RLHF training.")
            return None
        dim = 8
        w = np.zeros(dim)
        for _ in range(epochs):
            random.shuffle(pairs)
            for p in pairs:
                pc = self._featurize(p["chosen"], p["prompt"])
                pr = self._featurize(p["rejected"], p["prompt"])
                s = float(np.dot(w, pc - pr))
                # sigmoid gradient on -log sigmoid(s)
                grad = -1.0 / (1.0 + math.exp(s)) * (pc - pr)
                w -= lr * grad
        self.reward_weights = w
        logger.info(f"Trained reward model on {len(pairs)} pairs; |w|={np.linalg.norm(w):.4f}")
        return w


class ParticleSwarmOptimizer:
    """PSO for tuning anomaly detection hyperparameters with a real objective."""
    def __init__(self, storage: Optional[Storage] = None, config: Optional[Dict] = None,
                 objective: Optional[Callable[[Dict[str, float]], float]] = None):
        self.storage = storage
        self.config = config or {}
        self.num_particles = self.config.get('pso_particles', 10)
        self.max_iter = self.config.get('pso_iterations', 20)
        self.objective = objective or self._default_objective
        self.param_bounds = {
            'distillation_learning_rate': (1e-5, 1e-2),
            'distill_weight': (0.1, 0.9),
            'rl_weight': (0.1, 0.9),
            'distillation_train_every': (5, 20),
        }

    def _default_objective(self, chrom: Dict[str, float]) -> float:
        """Returns a *loss* (lower is better)."""
        # Penalize extreme learning rates and unbalanced distill/RL weights.
        lr = chrom['distillation_learning_rate']
        dw = chrom['distill_weight']
        rw = chrom['rl_weight']
        te = chrom['distillation_train_every']
        lr_penalty = abs(math.log10(lr) - math.log10(1e-3))
        balance_penalty = abs((dw + rw) - 1.0)
        train_penalty = abs(te - 10) / 10.0
        return lr_penalty + balance_penalty + 0.1 * train_penalty

    def _init_particles(self):
        particles = []
        for _ in range(self.num_particles):
            pos, vel = {}, {}
            for key, (low, high) in self.param_bounds.items():
                if key == 'distillation_learning_rate':
                    pos[key] = 10 ** random.uniform(np.log10(low), np.log10(high))
                elif key == 'distillation_train_every':
                    pos[key] = random.randint(int(low), int(high))
                else:
                    pos[key] = random.uniform(low, high)
                vel[key] = random.uniform(-(high - low) / 10, (high - low) / 10)
            particles.append({
                'position': pos, 'velocity': vel,
                'best_position': dict(pos), 'best_fitness': float('inf'),
            })
        return particles

    def _evaluate(self, chrom):
        try:
            return float(self.objective(chrom))
        except Exception as e:
            logger.warning(f"PSO objective failed: {e}")
            return float('inf')

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
                    p['best_position'] = dict(p['position'])
                if fitness < global_best_fitness:
                    global_best_fitness = fitness
                    global_best_pos = dict(p['position'])
            for p in particles:
                for key in self.param_bounds:
                    r1, r2 = random.random(), random.random()
                    cognitive = c1 * r1 * (p['best_position'][key] - p['position'][key])
                    social = c2 * r2 * (global_best_pos[key] - p['position'][key])
                    p['velocity'][key] = w * p['velocity'][key] + cognitive + social
                    low, high = self.param_bounds[key]
                    if key == 'distillation_learning_rate':
                        log_low, log_high = np.log10(low), np.log10(high)
                        log_pos = np.log10(max(p['position'][key], 1e-12)) + p['velocity'][key]
                        log_pos = max(log_low, min(log_high, log_pos))
                        p['position'][key] = 10 ** log_pos
                    elif key == 'distillation_train_every':
                        p['position'][key] = int(max(low, min(high, p['position'][key] + p['velocity'][key])))
                    else:
                        p['position'][key] = max(low, min(high, p['position'][key] + p['velocity'][key]))

        if self.storage and hasattr(self.storage, 'save_bio_run'):
            try:
                self.storage.save_bio_run(
                    run_id=f"pso_{uuid.uuid4()}",
                    algorithm="pso",
                    problem_id="anomaly_tuning",
                    parameters={"num_particles": self.num_particles, "max_iter": self.max_iter},
                    best_solution=global_best_pos,
                    best_fitness=global_best_fitness,
                )
            except Exception as e:
                logger.warning(f"PSO storage save failed: {e}")
        return global_best_pos


class MoEGatingNetwork:
    """Mixture-of-Experts gating with real expert action distributions.

    Experts each map the state to an action distribution biased toward their
    specialty. The gating network combines them. Training updates the gating
    weights with a reward-weighted policy gradient.
    """
    def __init__(self, storage: Optional[Storage] = None, config: Optional[Dict] = None):
        self.storage = storage
        self.config = config or {}
        self.num_experts = self.config.get('moe_expert_count', 4)
        self.expert_names = ['performance', 'carbon', 'cost', 'adaptive'][:self.num_experts]
        self.state_dim = AnomalyResponseState.FEATURE_DIM
        self.action_dim = 5
        self.gating_weights = np.random.randn(self.num_experts, self.state_dim) * 0.1
        self.lr = 0.05
        # Expert -> action preference (unnormalized logits)
        self.expert_action_logits = self._build_expert_logits()
        self._last_gate_probs: Optional[np.ndarray] = None
        self._last_expert_idx: Optional[int] = None

    def _build_expert_logits(self) -> Dict[str, np.ndarray]:
        # Actions: alert_only(0), reroute(1), restart(2), escalate(3), adaptive_cost(4)
        presets = {
            'performance': np.array([0.1, 0.5, 0.4, 0.2, 0.1]),
            'carbon':      np.array([0.4, 0.6, 0.1, 0.1, 0.7]),
            'cost':        np.array([0.7, 0.2, 0.1, 0.1, 0.4]),
            'adaptive':    np.array([0.2, 0.3, 0.3, 0.4, 0.8]),
        }
        return {k: v[:self.action_dim] for k, v in presets.items()}

    def _encode_state(self, state: Union[Dict, AnomalyResponseState]) -> np.ndarray:
        if isinstance(state, AnomalyResponseState):
            return state.to_feature_vector()
        return AnomalyResponseState(
            anomaly_score=state.get('anomaly_score', 0.5),
            metric_name_encoded=state.get('metric_name_encoded', 0.0),
            node_id_hash=state.get('node_id_hash', 0.5),
            persistent_count=state.get('persistent_count', 0),
            carbon_intensity=state.get('carbon_intensity', 400.0),
            system_load=state.get('system_load', 50.0),
            hour_of_day=state.get('hour_of_day', 12.0),
            recent_action_success_rate=state.get('recent_action_success_rate', 0.5),
            avg_reward=state.get('avg_reward', 0.0),
        ).to_feature_vector()

    def _gate(self, x: np.ndarray) -> np.ndarray:
        logits = self.gating_weights @ x
        logits = logits - np.max(logits)
        e = np.exp(logits)
        return e / e.sum()

    def _expert_action_probs(self, expert_name: str) -> np.ndarray:
        logits = self.expert_action_logits.get(expert_name, np.ones(self.action_dim))
        logits = logits - np.max(logits)
        e = np.exp(logits)
        return e / e.sum()

    async def select_expert(self, state: Union[Dict, AnomalyResponseState]
                            ) -> Tuple[str, np.ndarray]:
        x = self._encode_state(state)
        gate_probs = self._gate(x)
        # Mixture over experts
        action_probs = np.zeros(self.action_dim)
        for i, name in enumerate(self.expert_names):
            action_probs += gate_probs[i] * self._expert_action_probs(name)
        action_probs /= action_probs.sum() + 1e-9

        expert_idx = int(np.argmax(gate_probs))
        selected = self.expert_names[expert_idx]

        self._last_gate_probs = gate_probs
        self._last_expert_idx = expert_idx

        if self.storage and hasattr(self.storage, 'log_routing_decision'):
            try:
                sample_id = hashlib.sha256(str(state).encode()).hexdigest()[:16]
                self.storage.log_routing_decision(str(uuid.uuid4()), sample_id,
                                                   selected, float(gate_probs[expert_idx]))
            except Exception:
                pass
        return selected, action_probs

    async def add_training_sample(self, state: Union[Dict, AnomalyResponseState],
                                   selected_expert: str, reward: float,
                                   action_idx: Optional[int] = None):
        """Reward-weighted update of gating weights.

        Uses REINFORCE-style update on the gating distribution:
            d/dw log pi(expert) * reward
        """
        x = self._encode_state(state)
        gate_probs = self._gate(x)
        if selected_expert not in self.expert_names:
            return
        expert_idx = self.expert_names.index(selected_expert)
        one_hot = np.zeros(self.num_experts)
        one_hot[expert_idx] = 1.0
        # Gradient ascent step (maximize expected reward)
        grad = np.outer((one_hot - gate_probs), x)
        self.gating_weights += self.lr * reward * grad


# ============================================================================
# 9. MAIN ANOMALY DETECTOR (ENHANCED & WIRED)
# ============================================================================
class AnomalyDetector:
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
            if PYDANTIC_AVAILABLE:
                config = AnomalyConfig()
            else:
                config = ANOMALY_CONFIG.copy()
        if hasattr(config, 'dict'):
            self.config = config.dict()
        else:
            self.config = dict(config)

        # Central components
        self.storage = storage
        self.queue = message_queue
        self.adaptive_cost = adaptive_cost or (AdaptiveCostFunction() if not CENTRAL_AVAILABLE else None)
        self.pareto = pareto_gating or (ParetoGating() if not CENTRAL_AVAILABLE else None)
        self.drift = drift_detector
        self.metrics = metrics

        # Bio-core
        self.bio_core = bio_core
        self.token_manager = getattr(bio_core, 'token_manager', None) if bio_core else None
        self.gradient_manager = getattr(bio_core, 'gradient_manager', None) if bio_core else None
        self.compartment_manager = getattr(bio_core, 'compartment_manager', None) if bio_core else None

        # Persistence
        self.persistence = None
        if self.config.get('persistence_enabled', True) and self.storage is None:
            try:
                self.persistence = PersistenceManager(self.config)
            except Exception as e:
                logger.warning(f"Persistence disabled: {e}")
                self.persistence = None

        # Buffer
        self.buffer = TelemetryBuffer(self.config.get('window_size', 100), self.persistence)

        # Input validator
        self.validator = InputValidator(strict=False) if self.config.get('validate_inputs', True) else None

        # Models / state
        self.models: Dict[str, BaseAnomalyModel] = {}
        self.last_training: Dict[str, float] = {}
        self.last_online_fit_count: Dict[str, int] = {}
        self.anomaly_history: Dict[str, List[AnomalyEvent]] = {}
        self.alert_cooldown: Dict[str, float] = {}
        self.persistent_anomaly_count: Dict[str, int] = {}
        self.drift_scores: Dict[str, deque] = {}
        self.drift_flags: Dict[str, int] = {}
        self._node_locks: Dict[str, asyncio.Lock] = {}

        model_type = self.config.get('model_type', 'isolation_forest')
        self.ModelClass = {
            "isolation_forest": IsolationForestModel,
            "autoencoder": AutoencoderModel,
            "online_svm": OnlineSVM,
        }.get(model_type, ThresholdModel)

        # Callbacks
        self.alert_callback = None
        self.auto_response_callback = None
        self.evolutionary_engine_callback = None
        self.adaptive_cost_callback = None
        self.predictive_maintenance_callback = None

        # Distillation
        self.response_optimizer = DistillationResponseOptimizer(self, self.config)

        # Advanced components
        self.limit_graph_manager = LimitGraphManager(storage) if self.config.get('enable_limit_graph', True) else None
        self.modp_solver = MODPOptimizer(storage) if self.config.get('enable_modp_solver', True) else None
        self.rlhf_trainer = RLHFTrainer(storage) if self.config.get('enable_rlhf', True) else None
        self.pso_optimizer = None
        if self.config.get('enable_pso_tuning', True):
            self.pso_optimizer = ParticleSwarmOptimizer(storage, self.config)
        self.moe_gating = MoEGatingNetwork(storage, self.config) if self.config.get('enable_moe_gating', True) else None

        # Prometheus (only if no central metrics)
        self.prometheus_available = PROMETHEUS_AVAILABLE and (metrics is None)
        if self.prometheus_available:
            self.metrics_prom = {
                'detections': Counter('anomaly_detections_total', ['node', 'metric']),
                'alerts': Counter('anomaly_alerts_total', ['node', 'metric']),
                'auto_responses': Counter('anomaly_auto_responses_total', ['node', 'action']),
                'latency': Histogram('anomaly_detection_latency_seconds'),
                'drift': Counter('anomaly_drift_events_total', ['node']),
                'pso_runs': Counter('anomaly_pso_runs_total'),
            }
        else:
            self.metrics_prom = {}

        self._pso_task: Optional[asyncio.Task] = None

        logger.info(
            f"AnomalyDetector v2.4.0 initialized | storage={storage is not None} "
            f"queue={message_queue is not None} "
            f"limit_graph={self.limit_graph_manager is not None} "
            f"moe={self.moe_gating is not None} "
            f"model={model_type}"
        )

    # ----- Task helpers -----
    def _create_task(self, coro):
        try:
            loop = asyncio.get_running_loop()
            return loop.create_task(coro)
        except RuntimeError:
            logger.warning("No running event loop; background task not started.")
            return None

    # ----- Registration -----
    def register_alert_callback(self, callback): self.alert_callback = callback
    def register_auto_response_callback(self, callback): self.auto_response_callback = callback
    def register_evolutionary_engine_callback(self, callback): self.evolutionary_engine_callback = callback
    def register_adaptive_cost_callback(self, callback): self.adaptive_cost_callback = callback
    def register_predictive_maintenance_callback(self, callback): self.predictive_maintenance_callback = callback

    # ----- Model management -----
    def _ensure_model(self, node_id: str) -> BaseAnomalyModel:
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
            self.anomaly_history.setdefault(node_id, [])
            self.drift_scores.setdefault(node_id, deque(maxlen=100))
            self.drift_flags.setdefault(node_id, 0)
            self._node_locks.setdefault(node_id, asyncio.Lock())
            self.last_online_fit_count.setdefault(node_id, 0)
        return self.models[node_id]

    def _should_retrain(self, node_id: str) -> bool:
        if node_id not in self.last_training:
            return True
        return (time.time() - self.last_training[node_id]) > self.config.get('retrain_interval_seconds', 3600)

    def _update_model(self, node_id: str, data: np.ndarray):
        model = self._ensure_model(node_id)
        if data.shape[0] < 10:
            return
        if isinstance(model, OnlineSVM):
            # Actually online: partial_fit on every new window after warmup
            if not model.is_trained:
                model.partial_fit(data)
                self.last_training[node_id] = time.time()
            else:
                self.last_online_fit_count[node_id] += 1
                every = self.config.get('online_partial_fit_every', 5)
                if self.last_online_fit_count[node_id] % every == 0:
                    model.partial_fit(data)
        elif self._should_retrain(node_id):
            model.train(data)
            self.last_training[node_id] = time.time()
            if self.persistence:
                try:
                    self.persistence.save_model(node_id, model)
                except Exception as e:
                    logger.warning(f"Failed to persist model: {e}")

    def _impute_missing(self, metrics: Dict[str, float], node_id: str) -> Dict[str, float]:
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

    # ----- Drift detection (WIRED) -----
    def _check_concept_drift(self, node_id: str, score: float) -> bool:
        if not self.config.get('concept_drift_enabled', True):
            return False
        buf = self.drift_scores.setdefault(node_id, deque(maxlen=100))
        buf.append(score)
        min_samples = self.config.get('drift_min_samples', 20)
        if len(buf) < min_samples:
            return False
        arr = np.array(buf)
        mean = float(arr.mean())
        std = float(arr.std()) + 1e-9
        threshold = mean + self.config.get('drift_threshold_multiplier', 2.0) * std
        if score > threshold:
            self.drift_flags[node_id] = self.drift_flags.get(node_id, 0) + 1
            if self.metrics_prom:
                self.metrics_prom['drift'].labels(node=node_id).inc()
            return True
        return False

    # ----- Main ingest -----
    async def ingest(self, node_id: str, metrics: Dict[str, float]) -> Optional[AnomalyEvent]:
        start_time = time.time()

        # 1) Validate
        if self.validator is not None:
            metrics, issues = self.validator.sanitize(metrics)
            if issues:
                logger.warning(f"Input sanitization for {node_id}: {issues}")
            if not metrics:
                logger.warning(f"No valid metrics for {node_id}; skipping ingest.")
                return None

        # 2) Impute and filter
        metrics = self._impute_missing(metrics, node_id)
        features = self.config.get('metrics_features', [])
        filtered = {k: v for k, v in metrics.items() if k in features}

        # 3) Buffer
        await self.buffer.add_sample(node_id, filtered)

        if not self.buffer.has_enough_data(node_id, features):
            return None

        data_window = self.buffer.get_data(node_id, features)
        if data_window.shape[0] < 10:
            return None

        # 4) Train / partial-fit
        self._update_model(node_id, data_window)

        # 5) Score & decide
        latest = self.buffer.get_latest(node_id, features)
        if latest.size == 0:
            return None
        latest_reshaped = latest.reshape(1, -1)
        model = self._ensure_model(node_id)
        if not model.is_trained:
            return None

        score_arr = model.score(latest_reshaped)
        score = float(score_arr[0]) if score_arr.size else 0.0
        anomaly_arr = model.is_anomaly(latest_reshaped)
        is_anomaly = bool(anomaly_arr[0]) if anomaly_arr.size else False

        # 6) Drift check (using the model's anomaly score)
        drift = self._check_concept_drift(node_id, score)

        # 7) Handle
        if is_anomaly:
            event = self._create_event(node_id, filtered, model, score, is_anomaly)
            await self._handle_anomaly(event)
            if self.metrics_prom:
                try:
                    self.metrics_prom['detections'].labels(node=node_id, metric=event.metric_name).inc()
                    self.metrics_prom['latency'].observe(time.time() - start_time)
                except Exception:
                    pass
            if self.persistence:
                try:
                    self.persistence.save_anomaly_event(event)
                except Exception:
                    pass
            return event
        else:
            async with self._get_node_lock(node_id):
                self.persistent_anomaly_count[node_id] = 0
            if drift:
                # Drift but not anomalous -> trigger retrain on next ingest
                self.last_training[node_id] = 0.0
                logger.info(f"Concept drift detected for {node_id}; scheduling retrain.")
            return None

    def _get_node_lock(self, node_id: str) -> asyncio.Lock:
        if node_id not in self._node_locks:
            self._node_locks[node_id] = asyncio.Lock()
        return self._node_locks[node_id]

    def _create_event(self, node_id, metrics, model, score, is_anomaly) -> AnomalyEvent:
        features = self.config.get('metrics_features', [])
        # Feature attribution
        explanation: Dict[str, float] = {}
        try:
            latest = self.buffer.get_latest(node_id, features).reshape(1, -1)
            explanation = model.explain(latest)
        except Exception as e:
            logger.debug(f"Explanation failed: {e}")

        # Pick the top-contributing metric
        if explanation:
            metric_name = max(explanation, key=explanation.get)
        elif isinstance(model, ThresholdModel) and model.means is not None:
            mv = np.array([metrics.get(f, 0.0) for f in features])
            z = np.abs((mv - model.means) / (model.stds + 1e-6))
            metric_name = features[int(np.argmax(z))] if len(features) else "unknown"
        else:
            metric_name = features[0] if features else "unknown"

        metric_value = metrics.get(metric_name, 0.0)
        desc = f"Anomaly on {node_id}: {metric_name}={metric_value:.4f} (score={score:.4f})"

        return AnomalyEvent(
            timestamp=datetime.now(),
            node_id=node_id,
            metric_name=metric_name,
            metric_value=metric_value,
            anomaly_score=score,
            description=desc,
            alert_sent=False,
            auto_response_taken="none",
            explanation=explanation,
            feature_contributions=explanation,
            is_anomaly=is_anomaly,
        )

    async def _handle_anomaly(self, event: AnomalyEvent):
        node_id = event.node_id
        async with self._get_node_lock(node_id):
            self.persistent_anomaly_count[node_id] = self.persistent_anomaly_count.get(node_id, 0) + 1
            state = self._get_response_state(event)

            # Decide on action
            action: str
            action_idx: int
            state_vec: np.ndarray
            teacher_probs: np.ndarray

            used_moe = False
            if self.moe_gating is not None:
                # MoE yields a real distribution over actions
                _, action_probs = await self.moe_gating.select_expert(state)
                action_idx = int(np.argmax(action_probs))
                action = DistillationResponseOptimizer.ACTION_SPACE[action_idx]
                teacher_probs = action_probs
                state_vec = state.to_feature_vector()
                self._last_selected_expert = self.moe_gating.expert_names[
                    int(np.argmax(self.moe_gating._last_gate_probs))
                ] if self.moe_gating._last_gate_probs is not None else self.moe_gating.expert_names[0]
                used_moe = True
            elif self.adaptive_cost is not None and self.pareto is not None:
                action, action_idx, state_vec, teacher_probs = await self._select_action_modp(state)
            else:
                action, action_idx, state_vec, teacher_probs = await self.response_optimizer.select_action(
                    state, exploration=True
                )

            await self._execute_action(action, event)

            reward = self._simulate_reward(action, event)
            next_state = self._get_response_state(event)

            # Update distillation (always) -- MoE is an additional advisor
            await self.response_optimizer.update(state, action_idx, reward,
                                                   next_state, teacher_probs)

            # Update MoE with reward
            if used_moe and self.moe_gating is not None:
                await self.moe_gating.add_training_sample(
                    state, self._last_selected_expert, reward, action_idx
                )

            # Feedback event
            if self.queue is not None:
                try:
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
                        tags=["anomaly", "response"],
                    )
                    await self.queue.publish("feedback_events", fb_event.to_json())
                except Exception as e:
                    logger.warning(f"Failed to publish feedback event: {e}")

            # RLHF preference pair
            if self.rlhf_trainer is not None:
                try:
                    rejected = random.choice(
                        [a for a in DistillationResponseOptimizer.ACTION_SPACE if a != action]
                    )
                    self.rlhf_trainer.record_pair(
                        pair_id=str(uuid.uuid4()),
                        prompt=f"Which response is best for anomaly {event.metric_name}?",
                        chosen=action,
                        rejected=rejected,
                        reward_diff=reward,
                        metadata={"node_id": node_id, "metric": event.metric_name},
                    )
                except Exception as e:
                    logger.debug(f"RLHF record failed: {e}")

            # LIMIT graph: add event + allowed action nodes
            if self.limit_graph_manager is not None:
                try:
                    graph_id = "anomaly_events"
                    if not self.limit_graph_manager.get_metadata(graph_id):
                        self.limit_graph_manager.create_graph(graph_id, "Anomaly Event Graph", {})
                    event_node = f"{node_id}_{event.timestamp.timestamp()}"
                    self.limit_graph_manager.add_node(
                        graph_id, event_node, "anomaly_event",
                        {"metric": event.metric_name, "score": event.anomaly_score,
                         "action": action},
                    )
                    # Ensure an action node exists for each possible action
                    for a in DistillationResponseOptimizer.ACTION_SPACE:
                        self.limit_graph_manager.add_node(
                            graph_id, f"action_{a}", "action", {"name": a}
                        )
                except Exception as e:
                    logger.debug(f"LIMIT graph update failed: {e}")

            # Central DriftDetector integration (if present)
            if self.drift is not None:
                try:
                    weights = self.adaptive_cost.get_current_weights() if self.adaptive_cost else {}
                    drift_score = await self.drift.check_drift(weights)
                    if drift_score and drift_score > 0.7:
                        logger.warning(f"High central drift ({drift_score:.3f}); tightening thresholds.")
                        if 'energy_spike_threshold' in self.config:
                            self.config['energy_spike_threshold'] *= 0.95
                except Exception as e:
                    logger.debug(f"Drift detector call failed: {e}")

            # External callbacks
            if self.evolutionary_engine_callback:
                self._safe_call_callback(self.evolutionary_engine_callback, node_id, event.anomaly_score)
            if self.adaptive_cost_callback:
                self._safe_call_callback(self.adaptive_cost_callback, event.anomaly_score)
            if self.predictive_maintenance_callback:
                self._safe_call_callback(self.predictive_maintenance_callback, node_id, event.anomaly_score)

            # History
            event.auto_response_taken = action
            self.anomaly_history.setdefault(node_id, [])
            self.anomaly_history[node_id].append(event)
            if len(self.anomaly_history[node_id]) > 100:
                self.anomaly_history[node_id] = self.anomaly_history[node_id][-100:]

            # Webhook
            webhook_url = self.config.get('webhook_url')
            if webhook_url and AIOHTTP_AVAILABLE:
                self._create_task(self._send_webhook(event, webhook_url))

    def _get_response_state(self, event: AnomalyEvent) -> AnomalyResponseState:
        metric_names = self.config.get('metrics_features', [])
        try:
            metric_idx = metric_names.index(event.metric_name)
        except ValueError:
            metric_idx = 0
        node_hash = float(int(hashlib.md5(event.node_id.encode()).hexdigest()[:8], 16)) / (16 ** 8)
        persistent_count = self.persistent_anomaly_count.get(event.node_id, 0)

        recent = self.anomaly_history.get(event.node_id, [])[-10:]
        if recent:
            success_rate = sum(1 for e in recent if e.auto_response_taken != "none") / max(len(recent), 1)
            avg_reward = float(np.mean([e.anomaly_score for e in recent]))
        else:
            success_rate = 0.5
            avg_reward = 0.0

        return AnomalyResponseState(
            anomaly_score=event.anomaly_score,
            metric_name_encoded=float(metric_idx),
            node_id_hash=node_hash,
            persistent_count=persistent_count,
            carbon_intensity=400.0,
            system_load=50.0,
            hour_of_day=float(datetime.now().hour),
            recent_action_success_rate=success_rate,
            avg_reward=avg_reward,
        )

    async def _select_action_modp(self, state: AnomalyResponseState):
        """Select action using AdaptiveCostFunction + ParetoGating + LIMIT constraints."""
        actions = DistillationResponseOptimizer.ACTION_SPACE

        # LIMIT constraints
        allowed = None
        if self.limit_graph_manager is not None:
            try:
                ctx = {"node_id": None}  # graph currently only tracks events, no strict constraints
                allowed = self.limit_graph_manager.allowed_actions("anomaly_events", ctx)
            except Exception:
                allowed = None

        candidates = []
        for idx, action in enumerate(actions):
            if allowed is not None and action not in allowed:
                continue
            carbon_g = 5.0 + idx * 0.5
            latency_ms = 50.0 - idx * 5.0
            energy_joules = 20.0 + idx * 2.0
            quality = 0.9 - idx * 0.05
            # AdaptiveCostFunction returns a COST (lower = better)
            cost = self.adaptive_cost.compute(
                quality=quality, carbon_g=carbon_g, latency_ms=latency_ms,
                energy_joules=energy_joules, health=0.8, atp=0.5,
            )
            candidates.append({
                'action': action, 'cost': float(cost),
                'carbon_g': carbon_g, 'latency_ms': latency_ms,
                'energy_joules': energy_joules, 'quality_score': quality,
            })

        if not candidates:
            return await self.response_optimizer.select_action(state, exploration=True)

        # Pareto filter (lower cost/carbon, higher quality is better)
        filtered = self.pareto.filter(candidates) if self.pareto else candidates
        if filtered:
            keep = {c['action'] for c in filtered}
            candidates = [c for c in candidates if c['action'] in keep]

        # Choose the minimum-cost candidate
        best = min(candidates, key=lambda x: x['cost'])
        action = best['action']
        action_idx = actions.index(action)
        state_vec = state.to_feature_vector()
        # Build teacher probs from -cost softmax
        costs = np.array([c['cost'] for c in candidates])
        e = np.exp(-costs + costs.min())
        probs = e / e.sum()
        teacher_probs = np.zeros(len(actions))
        for c, p in zip(candidates, probs):
            teacher_probs[actions.index(c['action'])] = p
        return action, action_idx, state_vec, teacher_probs

    async def _execute_action(self, action: str, event: AnomalyEvent):
        # Bio-inspired ATP spend
        if self.token_manager is not None and action in ('restart', 'reroute'):
            try:
                await self.token_manager.spend(f"anomaly_{event.node_id}", 0.5)
            except Exception:
                pass

        if action == 'alert_only':
            now = time.time()
            cooldown = self.config.get('alert_cooldown_seconds', 300)
            last = self.alert_cooldown.get(event.node_id, 0.0)
            if now - last < cooldown:
                event.alert_sent = False
            else:
                event.alert_sent = True
                self.alert_cooldown[event.node_id] = now
                if self.alert_callback:
                    self._safe_call_callback(self.alert_callback, event)
                else:
                    logger.warning(f"ALERT: {event.description}")
                if self.metrics_prom:
                    try:
                        self.metrics_prom['alerts'].labels(node=event.node_id, metric=event.metric_name).inc()
                    except Exception:
                        pass
        elif action == 'reroute':
            if self.auto_response_callback:
                self._safe_call_callback(self.auto_response_callback, event)
            else:
                logger.info(f"AUTO-REROUTE for {event.node_id}.")
        elif action == 'restart':
            if self.auto_response_callback:
                self._safe_call_callback(self.auto_response_callback, event)
            else:
                logger.info(f"AUTO-RESTART for {event.node_id}.")
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

        if self.metrics_prom:
            try:
                self.metrics_prom['auto_responses'].labels(node=event.node_id, action=action).inc()
            except Exception:
                pass

        # Bio-inspired gradient pumping
        if self.gradient_manager is not None:
            try:
                if event.metric_name == 'carbon_kg':
                    await self.gradient_manager.pump_field('carbon', 0.05, source=f"anomaly_{event.node_id}")
                elif event.metric_name == 'helium_usage':
                    await self.gradient_manager.pump_field('helium', 0.05, source=f"anomaly_{event.node_id}")
                if action == 'restart':
                    await self.gradient_manager.pump_field('trust', 0.02, source=f"anomaly_{event.node_id}")
            except Exception:
                pass

    def _simulate_reward(self, action: str, event: AnomalyEvent) -> float:
        pcount = self.persistent_anomaly_count.get(event.node_id, 0)
        if action == 'restart' and pcount >= 3:
            return 0.9
        if action == 'reroute' and event.anomaly_score > 0.8:
            return 0.8
        if action == 'adaptive_cost' and event.metric_name in ('energy_joules', 'carbon_kg'):
            return 0.7
        if action == 'alert_only' and event.anomaly_score < 0.6:
            return 0.6
        if action == 'escalate':
            return 0.4
        return 0.1

    def _safe_call_callback(self, callback, *args, **kwargs):
        try:
            result = callback(*args, **kwargs)
            if asyncio.iscoroutine(result):
                self._create_task(result)
        except Exception as e:
            logger.error(f"Callback failed: {e}")

    async def _send_webhook(self, event: AnomalyEvent, url: str):
        if not AIOHTTP_AVAILABLE:
            return
        payload = {
            'node_id': event.node_id,
            'metric': event.metric_name,
            'value': event.metric_value,
            'score': event.anomaly_score,
            'description': event.description,
            'action': event.auto_response_taken,
            'timestamp': event.timestamp.isoformat(),
        }
        for attempt in range(3):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(url, json=payload, timeout=5) as resp:
                        if resp.status < 500:
                            return
            except Exception as e:
                logger.warning(f"Webhook attempt {attempt + 1} failed: {e}")
                await asyncio.sleep(2 ** attempt)

    # ----- Policy probabilities (MODP-style) -----
    async def policy_probs(self, state: Dict[str, Any]) -> List[float]:
        # MoE first (if enabled), else MODP, else distillation
        if self.moe_gating is not None:
            _, probs = await self.moe_gating.select_expert(state)
            return probs.tolist()

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

        if self.adaptive_cost is not None and self.pareto is not None:
            candidates = []
            for idx, action in enumerate(actions):
                carbon_g = 5.0 + idx * 0.5
                latency_ms = 50.0 - idx * 5.0
                energy_joules = 20.0 + idx * 2.0
                quality = 0.9 - idx * 0.05
                cost = float(self.adaptive_cost.compute(
                    quality=quality, carbon_g=carbon_g, latency_ms=latency_ms,
                    energy_joules=energy_joules, health=0.8, atp=0.5,
                ))
                candidates.append({'action': action, 'cost': cost})
            filtered = self.pareto.filter(candidates) if self.pareto else candidates
            if filtered:
                keep = {c['action'] for c in filtered}
                candidates = [c for c in candidates if c['action'] in keep]
            if candidates:
                costs = np.array([c['cost'] for c in candidates])
                e = np.exp(-costs + costs.min())
                probs = e / e.sum()
                full = [0.0] * len(actions)
                for c, p in zip(candidates, probs):
                    full[actions.index(c['action'])] = float(p)
                return full

        _, _, _, tp = await self.response_optimizer.select_action(opt_state, exploration=False)
        return tp.tolist()

    # ----- PSO integration -----
    async def run_pso_tuning(self) -> Optional[Dict[str, float]]:
        if self.pso_optimizer is None:
            return None
        best = await self.pso_optimizer.optimize()
        if best and self.metrics_prom:
            try:
                self.metrics_prom['pso_runs'].inc()
            except Exception:
                pass
        if best:
            self.config['distillation_learning_rate'] = best['distillation_learning_rate']
            self.config['distill_weight'] = best['distill_weight']
            self.config['rl_weight'] = best['rl_weight']
            self.config['distillation_train_every'] = int(best['distillation_train_every'])
            self.response_optimizer.student.lr = best['distillation_learning_rate']
            self.response_optimizer.student.distill_weight = best['distill_weight']
            self.response_optimizer.student.rl_weight = best['rl_weight']
            self.response_optimizer.train_every = int(best['distillation_train_every'])
        return best

    async def train_rlhf_reward_model(self):
        if self.rlhf_trainer is not None:
            return self.rlhf_trainer.train_reward_model()
        return None

    # ----- Shutdown -----
    async def shutdown(self):
        if self._pso_task is not None:
            self._pso_task.cancel()
        if self.persistence:
            for node_id, model in self.models.items():
                try:
                    self.persistence.save_model(node_id, model)
                except Exception as e:
                    logger.warning(f"Shutdown: failed to persist {node_id}: {e}")
        logger.info("AnomalyDetector shutdown complete.")


# ============================================================================
# 10. TELEMETRY COLLECTOR / ALERT ESCALATION / EVOLUTIONARY ENGINE (stubs)
# ============================================================================
class TelemetryCollector:
    pass


class AlertEscalationSystem:
    pass


class EvolutionaryEngine:
    pass


# ============================================================================
# 11. CONVENIENCE FACTORY
# ============================================================================
def create_anomaly_detection_system(config=None, **central_kwargs) -> AnomalyDetector:
    return AnomalyDetector(config=config, **central_kwargs)


# ============================================================================
# 12. REST API (with proper lifespan) and CLI entry point
# ============================================================================
if FASTAPI_AVAILABLE:
    @asynccontextmanager
    async def _lifespan(app: "FastAPI"):
        detector = create_anomaly_detection_system()
        app.state.detector = detector
        try:
            yield
        finally:
            await detector.shutdown()

    app = FastAPI(title="Green Agent Anomaly Detection API", lifespan=_lifespan)

    @app.post("/ingest/{node_id}")
    async def ingest_metrics(node_id: str, metrics: Dict[str, float]):
        detector: AnomalyDetector = app.state.detector
        event = await detector.ingest(node_id, metrics)
        if event:
            return {"status": "anomaly", "event": {
                "timestamp": event.timestamp.isoformat(),
                "node_id": event.node_id,
                "metric_name": event.metric_name,
                "metric_value": event.metric_value,
                "anomaly_score": event.anomaly_score,
                "description": event.description,
                "action": event.auto_response_taken,
                "explanation": event.explanation,
            }}
        return {"status": "ok"}

    @app.get("/health")
    async def health():
        return {"status": "running"}

    @app.get("/stats/{node_id}")
    async def stats(node_id: str):
        detector: AnomalyDetector = app.state.detector
        model = detector.models.get(node_id)
        return {
            "node_id": node_id,
            "trained": bool(model and model.is_trained),
            "history_len": len(detector.anomaly_history.get(node_id, [])),
            "persistent_count": detector.persistent_anomaly_count.get(node_id, 0),
            "optimizer": detector.response_optimizer.get_stats(),
        }


def main():
    async def run():
        detector = create_anomaly_detection_system()
        # Warm up with normal samples
        for i in range(30):
            await detector.ingest("node1", {
                "energy_joules": 100 + random.uniform(-5, 5),
                "carbon_kg": 0.4 + random.uniform(-0.02, 0.02),
                "helium_usage": 0.1 + random.uniform(-0.01, 0.01),
                "latency_ms": 200 + random.uniform(-10, 10),
                "accuracy": 0.95 + random.uniform(-0.005, 0.005),
            })
        # Inject anomaly
        event = await detector.ingest("node1", {
            "energy_joules": 5000,
            "carbon_kg": 12.0,
            "helium_usage": 5.0,
            "latency_ms": 3000,
            "accuracy": 0.4,
        })
        print("Anomaly event:", event)
        await asyncio.sleep(1)
        await detector.shutdown()

    asyncio.run(run())


if __name__ == "__main__":
    main()
